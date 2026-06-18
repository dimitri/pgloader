(ns pgloader.init.mssql
  (:import [java.sql DriverManager])
  (:require [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defn- split-batches
  [sql-text]
  (remove str/blank? (str/split sql-text #"(?im)^\s*GO\s*$")))

(defn -main
  [& args]
  (let [sql-file (or (first args) "/init.sql")
        host     (or (System/getenv "MSSQL_HOST") "mssql")
        port     (or (System/getenv "MSSQL_PORT") "1433")
        user     (or (System/getenv "MSSQL_USER") "sa")
        pass     (or (System/getenv "SA_PASSWORD") "pgloaderTest1!")
        url      (str "jdbc:sqlserver://" host ":" port ";databaseName=master;encrypt=false;loginTimeout=5")]
    (println "Connecting to MSSQL at" host ":" port)
    (with-open [conn (DriverManager/getConnection url user pass)]
      (doseq [batch (split-batches (slurp sql-file))]
        (let [sql (str/trim batch)]
          (println "> " (first (str/split-lines sql)))
          (with-open [stmt (.createStatement conn)]
            (.execute stmt sql))))
      (println "MSSQL init complete."))))
