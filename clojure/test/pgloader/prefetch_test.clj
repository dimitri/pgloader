(ns pgloader.prefetch-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.batch :as batch]
            [pgloader.copy :as copy]
            [pgloader.prefetch :as prefetch])
  (:import [java.lang.reflect InvocationHandler Proxy]
           [java.nio.charset StandardCharsets]
           [java.sql Connection]
           [java.util.concurrent.atomic AtomicBoolean]
           [org.postgresql PGConnection]
           [org.postgresql.util PSQLException PSQLState]))

(defn- pg-connection
  ([rolled-back?] (pg-connection rolled-back? nil))
  ([rolled-back? rollback-error]
   (Proxy/newProxyInstance
    (.getClassLoader PGConnection)
    (into-array Class [PGConnection Connection])
    (reify InvocationHandler
      (invoke [_ _ method _]
        (when (= "rollback" (.getName method))
          (reset! rolled-back? true)
          (when rollback-error
            (throw rollback-error))))))))

(deftest writer-finishes-when-reader-fails-with-a-full-queue
  (binding [copy/*prefetch-queue-capacity* 1]
    (let [pipeline   (prefetch/make-pipeline 10 1024)
          test-batch (batch/batch-add-row!
                      (batch/make-batch 10 1024)
                      (.getBytes "1\n" StandardCharsets/UTF_8))
          writer     (atom nil)]
      (.put (.queue pipeline) test-batch)
      (is (zero? (.remainingCapacity (.queue pipeline))))
      (.set ^AtomicBoolean (.done pipeline) true)
      (with-redefs [copy/copy-sql (constantly "COPY public.items FROM STDIN")
                    batch/send-batch! (fn [& _] {:rows 1})]
        (reset! writer
                (future
                  (prefetch/writer-task
                   (pg-connection (atom false))
                   {:target-schema "public" :target-table "items"}
                   pipeline)))
        (try
          (is (= 1 (:rows-ok (deref @writer 500 ::timed-out))))
          (finally
            (future-cancel @writer)))))))

(deftest on-error-stop-does-not-retry-a-failed-copy-batch
  (let [rolled-back? (atom false)
        retried?     (atom false)
        test-batch   (batch/batch-add-row!
                      (batch/make-batch 10 1024)
                      (.getBytes "1\n" StandardCharsets/UTF_8))
        error        (PSQLException. "bad value" PSQLState/DATA_ERROR)]
    (with-redefs [batch/send-batch! (fn [& _] (throw error))
                  batch/retry-batch! (fn [& _]
                                       (reset! retried? true)
                                       {:rows-ok 0 :errors 1})]
      (binding [copy/*on-error-stop* true]
        (is (thrown-with-msg?
             PSQLException #"bad value"
             (#'prefetch/send-batch-or-retry!
              (pg-connection rolled-back?)
              {:target-schema "public" :target-table "items"}
              "COPY public.items FROM STDIN"
              test-batch 0 0 0 0 nil)))))
    (testing "the active transaction is rolled back before propagation"
      (is @rolled-back?))
    (testing "strict mode never enters row rejection"
      (is (false? @retried?)))))

(deftest resume-mode-still-retries-a-failed-copy-batch
  (let [rolled-back? (atom false)
        retried?     (atom false)
        test-batch   (batch/batch-add-row!
                      (batch/make-batch 10 1024)
                      (.getBytes "1\n" StandardCharsets/UTF_8))
        error        (PSQLException. "bad value" PSQLState/DATA_ERROR)]
    (with-redefs [batch/send-batch! (fn [& _] (throw error))
                  batch/retry-batch! (fn [& _]
                                       (reset! retried? true)
                                       {:rows-ok 1 :errors 1})]
      (binding [copy/*on-error-stop* false]
        (is (= {:status :retry
                :rows-ok 1
                :errors 1
                :bytes 2
                :reject-paths nil}
               (select-keys
                (#'prefetch/send-batch-or-retry!
                 (pg-connection rolled-back?)
                 {:target-schema "public" :target-table "items"}
                 "COPY public.items FROM STDIN"
                 test-batch 0 0 0 0 nil)
                [:status :rows-ok :errors :bytes :reject-paths])))))
    (is @rolled-back?)
    (is @retried?)))

(deftest rollback-failure-preserves-the-copy-error-and-aborts
  (let [rolled-back?  (atom false)
        retried?      (atom false)
        test-batch    (batch/batch-add-row!
                       (batch/make-batch 10 1024)
                       (.getBytes "1\n" StandardCharsets/UTF_8))
        copy-error    (PSQLException. "bad value" PSQLState/DATA_ERROR)
        rollback-error (java.sql.SQLException. "rollback failed")]
    (with-redefs [batch/send-batch! (fn [& _] (throw copy-error))
                  batch/retry-batch! (fn [& _]
                                       (reset! retried? true)
                                       {:rows-ok 0 :errors 1})]
      (binding [copy/*on-error-stop* false]
        (let [thrown (try
                       (#'prefetch/send-batch-or-retry!
                        (pg-connection rolled-back? rollback-error)
                        {:target-schema "public" :target-table "items"}
                        "COPY public.items FROM STDIN"
                        test-batch 0 0 0 0 nil)
                       nil
                       (catch PSQLException e e))]
          (is (identical? copy-error thrown))
          (is (= [rollback-error] (vec (.getSuppressed thrown)))))))
    (is @rolled-back?)
    (is (false? @retried?))))
