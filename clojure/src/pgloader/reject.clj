(ns pgloader.reject
  (:import [java.io File FileOutputStream FileWriter]
           [java.util.concurrent.locks ReentrantLock])
  (:require [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defn reject-dat-path
  "Return the reject data file path for a given table"
  [^String root-dir ^String schema ^String table]
  (.getPath (File. root-dir (str schema "." table ".reject.dat"))))

(defn reject-log-path
  "Return the reject log file path for a given table"
  [^String root-dir ^String schema ^String table]
  (.getPath (File. root-dir (str schema "." table ".reject.log"))))

(defonce locks (atom {}))

(defn get-table-lock
  "Get or create a reentrant lock for a given table reject file.
   swap! is used as a compare-and-swap so both the check and the insert
   are inside the update function: all concurrent callers converge on the
   same lock instance regardless of how many threads race here."
  [^String schema ^String table]
  (let [key (str schema "." table)]
    (get (swap! locks (fn [m]
                        (if (contains? m key)
                          m
                          (assoc m key (ReentrantLock.)))))
         key)))

(defn write-reject!
  "Write a bad row to reject files.
   row-bytes - the raw COPY TEXT formatted row bytes
   table-spec - map with :target-schema and :target-table
   exception - the PSQLException or error message string
   root-dir - base directory for reject files
   Returns {:reject-data string :reject-log string}."
  [^bytes row-bytes {:keys [target-schema target-table]}
   exception ^String root-dir]
  (let [^String dat-path (reject-dat-path root-dir target-schema target-table)
        ^String log-path (reject-log-path root-dir target-schema target-table)
        lock (get-table-lock target-schema target-table)
        msg (if (instance? Exception exception)
              (.getMessage ^Exception exception)
              (str exception))]
    (.lock ^ReentrantLock lock)
    (try
      (with-open [w (FileOutputStream. dat-path true)]
        (.write w row-bytes)
        (.write w (int 10)))
      (with-open [w (FileWriter. log-path true)]
        (.write w (str msg "\n")))
      (finally
        (.unlock ^ReentrantLock lock)))
    {:reject-data dat-path
     :reject-log log-path}))
