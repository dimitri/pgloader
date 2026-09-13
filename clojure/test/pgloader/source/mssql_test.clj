(ns pgloader.source.mssql-test
  (:require [clojure.test :refer [deftest is]]
            [pgloader.source.mssql :as mssql]
            [pgloader.source.protocol :as source])
  (:import [java.lang.reflect InvocationHandler Proxy]
           [java.sql Connection PreparedStatement ResultSet
            ResultSetMetaData SQLException]))

(defn- interface-proxy
  [interface invoke-fn]
  (Proxy/newProxyInstance
   (.getClassLoader interface)
   (into-array Class [interface])
   (reify InvocationHandler
     (invoke [_ _ method args]
       (invoke-fn (.getName method) args)))))

(defn- failing-source
  [failure-point]
  (let [advance-count (atom 0)
        metadata      (interface-proxy
                       ResultSetMetaData
                       (fn [method _]
                         (case method
                           "getColumnCount" (int 1)
                           nil)))
        result-set    (interface-proxy
                       ResultSet
                       (fn [method _]
                         (case method
                           "next" (if (= failure-point :row-advance)
                                    (throw (SQLException. "row advance failed"))
                                    (= 1 (swap! advance-count inc)))
                           "getMetaData" metadata
                           "getString" (throw (SQLException. "column read failed"))
                           nil)))
        statement     (interface-proxy
                       PreparedStatement
                       (fn [method _]
                         (case method
                           "executeQuery" result-set
                           nil)))
        connection    (interface-proxy
                       Connection
                       (fn [method _]
                         (case method
                           "prepareStatement" statement
                           nil)))]
    (mssql/->MSSQLSource connection "fixture")))

(def table-spec
  {:table-name "items"
   :schema "dbo"
   :columns [{:column-name "value"}]})

(deftest row-advance-errors-propagate
  (is (thrown-with-msg?
       SQLException #"row advance failed"
       (doall (source/read-rows (failing-source :row-advance) table-spec)))))

(deftest column-read-errors-propagate
  (is (thrown-with-msg?
       SQLException #"column read failed"
       (doall (source/read-rows (failing-source :column-read) table-spec)))))
