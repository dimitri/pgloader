(ns pgloader.source.dbf
  (:require [pgloader.source.protocol :refer [Source]]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [com.linuxense.javadbf DBFReader DBFDataType]
           [java.io FileInputStream]
           [java.nio.charset Charset StandardCharsets]))

(set! *warn-on-reflection* true)

(def ^:private charset-aliases
  "Normalise common encoding aliases that may not be registered in the JVM."
  {"cp950"  "Big5"
   "CP950"  "Big5"
   "cp932"  "windows-31j"
   "CP932"  "windows-31j"})

(defn- charset-for ^Charset [^String encoding]
  (Charset/forName (get charset-aliases encoding encoding)))

(defn- dbf-type->pg [^DBFDataType t decimal-count]
  (cond
    (= t DBFDataType/CHARACTER) "text"
    (= t DBFDataType/VARCHAR) "text"
    (= t DBFDataType/NUMERIC) (if (pos? (long decimal-count)) "numeric" "bigint")
    (= t DBFDataType/FLOATING_POINT) "double precision"
    (= t DBFDataType/DOUBLE) "double precision"
    (= t DBFDataType/LONG) "integer"
    (= t DBFDataType/AUTOINCREMENT) "bigserial"
    (= t DBFDataType/DATE) "date"
    (= t DBFDataType/TIMESTAMP) "timestamptz"
    (= t DBFDataType/TIMESTAMP_DBASE7) "timestamptz"
    (= t DBFDataType/LOGICAL) "boolean"
    (= t DBFDataType/MEMO) "text"
    (= t DBFDataType/BINARY) "bytea"
    (= t DBFDataType/VARBINARY) "bytea"
    (= t DBFDataType/BLOB) "bytea"
    (= t DBFDataType/CURRENCY) "numeric"
    :else "text"))

(defn- field->col [^com.linuxense.javadbf.DBFField field]
  {:column-name (str/lower-case (.getName field))
   :column-type (dbf-type->pg (.getType field) (.getDecimalCount field))
   :is-nullable true
   :column-default nil
   :extra nil})

(defrecord DBFSource [^String filepath ^Charset encoding]

  Source
  (source-name [_] (str "dbf:" filepath))

  (catalog [_]
    (with-open [reader (DBFReader. (FileInputStream. filepath))]
      (.setCharactersetName reader (.name encoding))
      (let [table-name (-> filepath
                           (str/split #"/")
                           last
                           (str/replace #"(?i)\.dbf$" "")
                           str/lower-case)
            n (.getFieldCount reader)
            cols (mapv #(field->col (.getField reader %)) (range n))]
        [{:table-name table-name
          :schema "public"
          :columns cols
          :primary-key []
          :indexes []
          :fkeys []}])))

  (partition-source [_ _ _ _] nil)

  (close! [_])

  (read-query [_ _sql]
    (throw (UnsupportedOperationException. "read-query not supported for DBF source")))

  (read-rows [_ _table-spec]
    (let [reader (DBFReader. (FileInputStream. filepath))]
      (.setCharactersetName reader (.name encoding))
      (let [n (.getFieldCount reader)]
        ((fn thisfn []
           (when-let [record (.nextRecord reader)]
             (lazy-seq
              (cons (mapv (fn [i]
                            (let [v (aget record i)]
                              (when (some? v) (str v))))
                          (range n))
                    (thisfn))))))))))

(defn create-source [{:keys [path encoding]}]
  (->DBFSource path
               (or (when encoding (charset-for encoding))
                   StandardCharsets/ISO_8859_1)))
