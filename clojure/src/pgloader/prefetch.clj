(ns pgloader.prefetch
  (:import [org.postgresql PGConnection]
           [org.postgresql.copy CopyManager]
           [org.postgresql.util PSQLException]
           [java.sql Connection]
           [java.util.concurrent LinkedBlockingQueue
            BlockingQueue]
           [java.util.concurrent.atomic AtomicBoolean AtomicLong]
           [java.nio.charset StandardCharsets])
  (:require [pgloader.batch :as batch]
            [pgloader.copy :as copy]
            [pgloader.log :as plog]
            [pgloader.source.protocol :refer [Source read-rows]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)

(defrecord CopyPipeline
  [^BlockingQueue queue
   ^long batch-rows
   ^long batch-bytes
   ^AtomicBoolean done
   ^AtomicLong error-count])

(defn make-pipeline
  "Create a new CopyPipeline with default settings."
  ([] (make-pipeline copy/*batch-rows* copy/*batch-size*))
  ([batch-rows batch-bytes]
   (->CopyPipeline (LinkedBlockingQueue.
                     (int copy/*prefetch-queue-capacity*))
                   batch-rows batch-bytes
                   (AtomicBoolean. false)
                   (AtomicLong. 0))))

(defn- reader-loop
  [source table-spec ^CopyPipeline pipeline cast-specs max-rows max-bytes start row-filter-fn]
  (loop [batch (batch/make-batch max-rows max-bytes)
         rows  (read-rows source table-spec)]
    (if-let [row (first rows)]
          (let [row      (if row-filter-fn (row-filter-fn row) row)
                row-bytes (copy/format-row-bytes row cast-specs)]
            (if (batch/batch-full? batch)
          (do (.put ^BlockingQueue (.queue pipeline) batch)
              (recur (batch/batch-add-row! (batch/make-batch max-rows max-bytes)
                                            row-bytes)
                     (rest rows)))
          (recur (batch/batch-add-row! batch row-bytes)
                 (rest rows))))
      (let [elapsed (- (System/nanoTime) start)]
        (.put ^BlockingQueue (.queue pipeline) batch)
        (.put ^BlockingQueue (.queue pipeline) :end-of-data)
        (.set ^AtomicBoolean (.done pipeline) true)
        elapsed))))

(defn reader-task
  "Virtual thread task that reads rows from source and fills batches.
   Pushes full batches onto the pipeline queue.
   Returns total nanoseconds spent reading."
  [source table-spec ^CopyPipeline pipeline cast-specs & [row-filter-fn]]
  (let [max-rows  (.batch-rows pipeline)
        max-bytes (.batch-bytes pipeline)
        start (System/nanoTime)]
    (try
      (reader-loop source table-spec pipeline cast-specs max-rows max-bytes start row-filter-fn)
      (catch Exception e
        (.set ^AtomicBoolean (.done pipeline) true)
        (throw e)))))

(defn- send-batch-or-retry!
  "Send a single batch, or handle errors and return updated counters.
   Returns {:status :ok :rows-ok ... :errors ... :ws-nanos ... :bytes ... :reject-paths ...}
   for success/retry, or throws for non-retryable errors."
  [^PGConnection pg-conn table-spec ^String copy-sql-str
   b rows-ok errors ws-nanos bytes reject-paths]
  (let [batch-start (System/nanoTime)]
    (try
      (let [{:keys [rows]} (batch/send-batch! pg-conn b copy-sql-str)]
        (.commit ^Connection pg-conn)
        {:status :ok
         :rows-ok (+ rows-ok rows)
         :errors errors
         :ws-nanos (+ ws-nanos (- (System/nanoTime) batch-start))
         :bytes (+ bytes (:byte-count b))
         :reject-paths reject-paths})
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)
              cause (:cause data)]
          (log/error (str "COPY init failed: "
                          (if cause (.getMessage ^Throwable cause) "unknown"))))
        (throw e))
      (catch PSQLException e
        (.rollback ^Connection pg-conn)
        (log/info "Entering error recovery.")
        (let [retry-result (batch/retry-batch! b table-spec e pg-conn)]
          {:status :retry
           :rows-ok (+ rows-ok (:rows-ok retry-result 0))
           :errors (+ errors (:errors retry-result))
           :ws-nanos (+ ws-nanos (- (System/nanoTime) batch-start))
           :bytes (+ bytes (:byte-count b))
           :reject-paths (or (:reject-paths retry-result) reject-paths)}))
      (catch Exception e
        (try (.rollback ^Connection pg-conn) (catch Exception _))
        (log/error e (str "Non-retryable error for " (:target-table table-spec)))
        (throw e)))))

(defn writer-task
  "Virtual thread task that drains batches from the pipeline queue
   and sends them to PostgreSQL via CopyManager.
   Each batch gets its own transaction. On data errors, retry-batch!
   handles per-row recovery with independent sub-batch commits.
   Returns {:rows-ok n :rows-bad n :ws-nanos n :bytes n :reject-paths {...}}."
  [^PGConnection pg-conn table-spec ^CopyPipeline pipeline]
  (let [copy-sql-str (copy/copy-sql table-spec)
        start        (System/nanoTime)]
    (loop [rows-ok   (long 0)
           errors    (long 0)
           ws-nanos  (long 0)
           bytes     (long 0)
           reject-paths nil]
      (let [item (.take ^BlockingQueue (.queue pipeline))]
        (if (= :end-of-data item)
          {:rows-ok rows-ok
           :rows-bad errors
           :ws-nanos (- (System/nanoTime) start)
           :bytes bytes
           :reject-paths reject-paths}
          (let [^batch/Batch b item
                result (send-batch-or-retry!
                        pg-conn table-spec copy-sql-str
                        b rows-ok errors ws-nanos bytes reject-paths)]
            (recur (long (:rows-ok result)) (long (:errors result))
                   (long (:ws-nanos result)) (long (:bytes result))
                   (:reject-paths result))))))))

(defn copy-table!
  "Orchestrate the full copy of a single table.
   Spawns reader and writer virtual threads.
   Returns {:rows-ok n :rows-bad n :rs-nanos n :ws-nanos n :bytes n :reject-paths {...}}."
  [source table-spec ^PGConnection pg-conn cast-specs & [row-filter-fn]]
  (let [table-name (:target-table table-spec)
        ^CopyPipeline pipeline (make-pipeline)
        exc-holder (atom nil)
        rs-nanos-holder (atom 0)
        reader (Thread/startVirtualThread
                 (fn []
                   (log/debug (str "Reader started for " table-name))
                   (try
                     (reset! rs-nanos-holder
                       (reader-task source table-spec pipeline cast-specs row-filter-fn))
                     (log/debug (str "Reader for " table-name " is done in "
                                     (pgloader.log/fmt-duration @rs-nanos-holder)))
                     (catch Exception e
                       (reset! exc-holder e)
                       (.set ^AtomicBoolean (.done pipeline) true)
                       (.offer ^BlockingQueue (.queue pipeline) :end-of-data)))))]
    (log/debug (str "Writer started for " table-name))
    (let [{:keys [rows-ok rows-bad ws-nanos bytes reject-paths] :as writer-result}
          (try
            (writer-task pg-conn table-spec pipeline)
            (catch Exception e
              (reset! exc-holder e)
              {:rows-ok 0 :rows-bad 0 :ws-nanos 0 :bytes 0 :reject-paths nil}))]
      (log/debug (str "Writer for " table-name " is done in "
                      (pgloader.log/fmt-duration ws-nanos)))
      (try (.join reader) (catch InterruptedException _))
      (when-let [e @exc-holder]
        (throw e))
      {:rows-ok rows-ok
       :rows-bad rows-bad
       :rs-nanos @rs-nanos-holder
       :ws-nanos ws-nanos
       :bytes bytes
       :reject-paths reject-paths})))
