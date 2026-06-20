(ns pgloader.source.copy
  (:require [pgloader.source.protocol :refer [Source]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [java.nio.charset Charset StandardCharsets]))

(set! *warn-on-reflection* true)

(defrecord CopySource
  [^String filepath
   ^Charset encoding
   delimiter
   null-str
   column-names]

  Source
  (source-name [_] (str "copy:" filepath))

  (catalog [_]
    (log/info (str "Reading COPY source: " filepath))
    (let [table-name (-> filepath
                         (str/split #"/")
                         last
                         (str/replace #"\.\w+$" ""))]
      [{:table-name table-name
        :schema "public"
        :columns (if (seq column-names)
                   (mapv (fn [n] {:column-name n
                                  :column-type "text"
                                  :is-nullable true
                                  :extra nil})
                         column-names)
                   [{:column-name "data"
                     :column-type "text"
                     :is-nullable true
                     :extra nil}])
        :primary-key []
        :indexes []
        :fkeys []}]))

  (partition-source [_ _ _ _] nil)

  (close! [_])

  (read-query [_ _sql]
    (throw (UnsupportedOperationException. "read-query not supported for COPY source")))

  (read-rows [_ _table-spec]
    (log/info (str "Reading COPY file: " filepath))
    (let [rdr (io/reader filepath :encoding (.name encoding))]
      (map (fn [line]
             (mapv (fn [v] (if (= v null-str) nil v))
                   (str/split line (re-pattern (java.util.regex.Pattern/quote
                                                 (str delimiter))))))
           (line-seq rdr)))))

(defn create-source
  [{:keys [path columns delimiter nullif encoding]}]
  (->CopySource path
                (or (when encoding (Charset/forName encoding))
                    StandardCharsets/UTF_8)
                (or (when delimiter (char delimiter)) \tab)
                (or nullif "\\N")
                (mapv name (or columns []))))
