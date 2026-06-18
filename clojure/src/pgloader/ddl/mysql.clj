(ns pgloader.ddl.mysql
  (:require [pgloader.ddl.common :as ddl]
            [pgloader.cast :as cast]
            [hugsql.core :as hugsql]
            [clojure.string :as str])
  (:import [java.sql Connection]))

(set! *warn-on-reflection* true)

(hugsql/def-db-fns "pgloader/ddl/mysql.sql")

(defn fetch-columns
  "Fetch column metadata for all tables in a MySQL catalog.
   Returns list of {:table-name :column-name :column-type ...}"
  [^Connection conn]
  (let [tables (tables conn)]
    (mapcat
      (fn [t]
        (let [table-name (:table-name t)
              schema (:table-schema t)
              cols (try
                     (columns conn {:schema schema :table table-name})
                     (catch Exception _ []))]
          (map #(assoc % :table-name table-name :table-schema schema) cols)))
      tables)))

(defn generate-ddl
  "Generate full DDL for a MySQL source.
   Returns {:create-sql [...] :index-sql [...] :drop-sql [...]}"
  [^Connection conn {:keys [schema target-schema table-filter]
                      :or {target-schema "public"}}]
  (let [all-columns (fetch-columns conn)
        groups (group-by :table-name all-columns)
        ;; apply cast transforms to column definitions
        transformed (fn [table-name cols]
                      (mapv #(cast/apply-cast %) cols))
        create-sqls (mapv (fn [[table-name cols]]
                            (ddl/create-table-sql
                              target-schema table-name
                              (cast/apply-cast cols)))
                          groups)
        drop-sqls (mapv (fn [[table-name _]]
                          (ddl/drop-table-if-exists-sql target-schema table-name))
                        groups)
        index-sqls (mapv (fn [[table-name cols]]
                           (ddl/create-index-sql target-schema table-name cols))
                         groups)]
    {:create-sql create-sqls
     :index-sql  index-sqls
     :drop-sql   drop-sqls
     :tables     (keys groups)}))
