(ns pgloader.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.copy :as copy]
            [pgloader.core :as core])
  (:import [java.util.concurrent CompletableFuture Executors ExecutorService]))

(deftest await-table-futures-propagates-the-first-strict-failure
  (let [failure (ex-info "copy failed" {:table "items"})
        failed  (CompletableFuture/failedFuture failure)
        complete (CompletableFuture/completedFuture :ok)]
    (testing "strict mode unwraps and propagates a worker failure"
      (let [^ExecutorService index-executor (Executors/newSingleThreadExecutor)]
        (.submit index-executor ^Runnable (fn [] nil))
        (binding [copy/*on-error-stop* true]
          (let [thrown (try
                         (#'core/await-table-futures! [failed complete] index-executor)
                         nil
                         (catch Exception e e))]
            (is (identical? failure thrown))
            (is (.isShutdown index-executor))
            (is (.isTerminated index-executor))))))
    (testing "resume mode still waits for failures without aborting the load"
      (binding [copy/*on-error-stop* false]
        (is (nil? (#'core/await-table-futures! [failed complete] nil)))))))
