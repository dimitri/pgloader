(ns pgloader.batch
  (:import [org.postgresql PGConnection]
           [org.postgresql.copy CopyManager CopyIn]
           [org.postgresql.util PSQLException ServerErrorMessage]
           [java.sql Connection]
           [java.nio.charset StandardCharsets])
  (:require [pgloader.copy :as copy]
            [pgloader.reject :as reject]
            [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)

(defrecord Batch
           [start-time
            row-count
            byte-count
            rows
            max-rows
            max-bytes])

(defn make-batch
  "Create a new Batch with randomized row capacity (0.7x-1.3x of base-rows)."
  ([] (make-batch copy/*batch-rows* copy/*batch-size*))
  ([base-rows max-bytes]
   (let [rand-rows (long (* base-rows (+ 0.7 (rand 0.6))))
         row-array (make-array (Class/forName "[B") rand-rows)]
     (->Batch (System/nanoTime) 0 0 row-array rand-rows max-bytes))))

(defn batch-full?
  "Check if a batch has reached capacity limits."
  [b]
  (or (>= (:row-count b) (:max-rows b))
      (>= (:byte-count b) (:max-bytes b))))

(defn batch-add-row!
  "Add a pre-formatted row byte array to a batch. Returns updated batch."
  [b ^bytes row-bytes]
  (aset ^objects (:rows b) (:row-count b) row-bytes)
  (assoc b
         :row-count (inc (:row-count b))
         :byte-count (+ (:byte-count b) (alength row-bytes))))

(defn- send-rows!
  "Send rows [start, end) from a batch via CopyManager.
   Wraps copyIn() separately so that COPY init failures are distinguishable
   from data errors. On init failure throws ex-info with :type :copy-init-error.
   On data failure throws PSQLException."
  [^PGConnection pg-conn batch start end ^String copy-sql-str]
  (let [cm (.getCopyAPI pg-conn)
        ^CopyIn ci (try
                     (.copyIn cm copy-sql-str)
                     (catch PSQLException e
                       (throw (ex-info "COPY init failed"
                                       {:type :copy-init-error :cause e}))))]
    (try
      (loop [i start]
        (when (< i end)
          (let [^bytes row (aget ^objects (:rows batch) i)]
            (when row (.writeToCopy ci row 0 (alength row))))
          (recur (inc i))))
      (.endCopy ci)
      {:status :ok :rows (- end start)}
      (catch PSQLException e
        (try (.cancelCopy ci) (catch Exception _))
        (throw e)))))

(defn send-batch!
  "Send the full batch via CopyManager."
  [^PGConnection pg-conn batch ^String copy-sql-str]
  (send-rows! pg-conn batch 0 (:row-count batch) copy-sql-str))

(defn parse-copy-line
  "Extract 0-indexed failing line number from PostgreSQL CONTEXT string.
   CONTEXT is in getWhere(), not getMessage().
   Returns int or nil."
  [e]
  (when (instance? PSQLException e)
    (when-let [^ServerErrorMessage server-msg (.getServerErrorMessage ^PSQLException e)]
      (let [ctx (or
                 (try (.getWhere server-msg) (catch Exception _ nil))
                 (.getMessage server-msg))]
        (when-let [m (re-find #"COPY [^,]+, [^ ]+ (\d+)" ctx)]
          (- (Integer/parseInt (last m)) 1))))))

(defn write-reject-row!
  "Write a single bad row to reject files.
   Returns {:reject-data string :reject-log string} or nil."
  [batch idx table-spec exception]
  (let [^bytes row (aget ^objects (:rows batch) idx)]
    (when row
      (reject/write-reject! row
                            {:target-schema (:target-schema table-spec)
                             :target-table  (:target-table table-spec)}
                            exception
                            copy/*root-dir*))))

(defn- bisect-batch!
  "Binary search over batch[start..end) to find and reject all bad rows.
   Each good sub-range is committed independently so no good rows are lost.
   At the base case (one row) the row is tried individually and, if it fails,
   written to the reject files.
   Complexity: O(k · log N) COPY round-trips for k bad rows in a range of N.
   Returns {:errors n :rows-ok n :reject-paths p}."
  [batch table-spec ^PGConnection pg-conn ^String copy-sql start end reject-paths]
  (let [cnt (- end start)]
    (if (<= cnt 1)
      ;; Base case: try the single row to obtain the exact exception for the
      ;; reject log, then reject it if it still fails.
      (do
        (log/debug (str "bisect: trying 1 row at position " start))
        (try
          (send-rows! pg-conn batch start end copy-sql)
          (.commit ^Connection pg-conn)
          {:errors 0 :rows-ok 1 :reject-paths reject-paths}
          (catch PSQLException e
            (.rollback ^Connection pg-conn)
            (log/warn (str "Row permanently rejected (bisect) for "
                           (:target-table table-spec)))
            (let [paths (write-reject-row! batch start table-spec e)]
              {:errors 1 :rows-ok 0 :reject-paths (or paths reject-paths)}))))
      ;; Recursive case: split and try each half independently.
      (let [mid          (+ start (quot cnt 2))
            _            (log/debug (str "bisect: trying " (- mid start)
                                         " rows [" start ", " mid ")"))
            first-result (try
                           (send-rows! pg-conn batch start mid copy-sql)
                           (.commit ^Connection pg-conn)
                           {:errors 0 :rows-ok (- mid start) :reject-paths reject-paths}
                           (catch PSQLException _e
                             (.rollback ^Connection pg-conn)
                             (bisect-batch! batch table-spec pg-conn copy-sql
                                            start mid reject-paths)))
            _            (log/debug (str "bisect: trying " (- end mid)
                                         " rows [" mid ", " end ")"))
            second-result (try
                            (send-rows! pg-conn batch mid end copy-sql)
                            (.commit ^Connection pg-conn)
                            {:errors 0 :rows-ok (- end mid)
                             :reject-paths (:reject-paths first-result)}
                            (catch PSQLException _e
                              (.rollback ^Connection pg-conn)
                              (bisect-batch! batch table-spec pg-conn copy-sql
                                             mid end (:reject-paths first-result))))]
        {:errors      (+ (:errors first-result) (:errors second-result))
         :rows-ok     (+ (:rows-ok first-result) (:rows-ok second-result))
         :reject-paths (:reject-paths second-result)}))))

(defn- retry-loop
  "Retry helper: attempts to send a sub-batch [pos, total-rows).
   When PostgreSQL supplies a COPY line number in the error CONTEXT, skips
   directly to that row and commits all preceding good rows in one shot.
   When no line number is available (e.g. FK violations), delegates to
   bisect-batch! which finds bad rows in O(k·log N) round-trips.
   Each sub-batch is independently committed.
   Returns {:errors n :rows-ok n :reject-paths {...}}."
  [batch table-spec ^PGConnection pg-conn
   ^String copy-sql pos errors rows-ok total-rows reject-paths]
  (loop [pos pos errors errors rows-ok rows-ok reject-paths reject-paths]
    (if (>= pos total-rows)
      {:errors errors :rows-ok rows-ok :reject-paths reject-paths}
      (let [result
            (try
              (send-rows! pg-conn batch pos total-rows copy-sql)
              (.commit ^Connection pg-conn)
              :ok
              (catch PSQLException e
                (.rollback ^Connection pg-conn)
                {:error e :pos pos}))]
        (if (= :ok result)
          {:errors errors :rows-ok (+ rows-ok (- total-rows pos))
           :reject-paths reject-paths}
          (let [e        (:error result)
                err-line (parse-copy-line e)]
            (if (nil? err-line)
              ;; No COPY line number in the error (e.g. FK violation).
              ;; Use binary search to locate and reject bad rows.
              (let [bisect (bisect-batch! batch table-spec pg-conn copy-sql
                                          pos total-rows reject-paths)]
                {:errors      (+ errors (:errors bisect))
                 :rows-ok     (+ rows-ok (:rows-ok bisect))
                 :reject-paths (:reject-paths bisect)})
              ;; We have a line number — skip directly to the bad row.
              (let [bad-idx (if (and (< (+ pos err-line) total-rows)
                                     (>= (+ pos err-line) pos))
                              (+ pos err-line)
                              pos)]
                (if (< pos bad-idx)
                  ;; Commit good rows before the bad row, then reject it.
                  (let [sub-result
                        (try
                          (send-rows! pg-conn batch pos bad-idx copy-sql)
                          (.commit ^Connection pg-conn)
                          :ok
                          (catch PSQLException inner-e
                            (.rollback ^Connection pg-conn)
                            ;; Sub-batch itself failed — bisect it.
                            (bisect-batch! batch table-spec pg-conn copy-sql
                                           pos bad-idx reject-paths)))]
                    (if (= :ok sub-result)
                      (let [sub-rows (- bad-idx pos)
                            paths    (write-reject-row! batch bad-idx table-spec e)]
                        (log/warn (str "Row permanently rejected for "
                                       (:target-table table-spec)))
                        (recur (inc bad-idx) (inc errors) (+ rows-ok sub-rows)
                               (or paths reject-paths)))
                      ;; sub-result is a bisect result map
                      (recur (inc bad-idx)
                             (+ errors (:errors sub-result))
                             (+ rows-ok (:rows-ok sub-result))
                             (:reject-paths sub-result))))
                  ;; Bad row is right at pos — reject it and continue.
                  (let [paths (write-reject-row! batch bad-idx table-spec e)]
                    (log/warn (str "Row permanently rejected for "
                                   (:target-table table-spec)))
                    (recur (inc bad-idx) (inc errors) rows-ok
                           (or paths reject-paths))))))))))))

(defn retry-batch!
  "Entry point for error recovery. Called after the outer batch has been
   rolled back. The connection is in autoCommit=false with no active
   transaction. Each sub-batch in the retry gets its own commit.
   Returns {:errors n :rows-ok n :reject-paths {:reject-data string :reject-log string}}."
  [batch table-spec ^PSQLException first-exception ^PGConnection pg-conn]
  (log/info "Entering error recovery.")
  (retry-loop batch table-spec pg-conn
              (copy/copy-sql table-spec)
              0 0 0 (:row-count batch) nil))
