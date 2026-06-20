(ns pgloader.copy
  (:import [java.nio.charset StandardCharsets])
  (:require [pgloader.cast :as cast]
            [clojure.string :as str]))

(set! *warn-on-reflection* true)

(def ^:dynamic *batch-rows*
  "Target rows per COPY batch (actual is randomized 0.7x-1.3x)"
  25000)

(def ^:dynamic *batch-size*
  "Maximum memory size allowed for a single batch in bytes"
  (* 20 1024 1024))

(def ^:dynamic *prefetch-queue-capacity*
  "Number of batches in the prefetch queue"
  4)

(def ^:dynamic *on-error-stop*
  "If true, abort on first error instead of continuing"
  false)

(def ^:dynamic *dry-run*
  "If true, check connections only — no catalog fetch, no DDL, no COPY"
  false)

(def ^:dynamic *root-dir*
  "Root directory for reject files and logs"
  "/tmp/pgloader/")

(defn- quote-id
  "Quote a PostgreSQL identifier."
  [^String s]
  (str "\"" (str/replace s "\"" "\"\"") "\""))

(defn escape-copy-text
  "Escape a string value for PostgreSQL COPY TEXT format."
  [^String v]
  (let [sb (StringBuilder. (count v))]
    (dotimes [i (count v)]
      (let [ch (.charAt v i)]
        (case ch
          \\          (.append sb "\\\\")
          \tab        (.append sb "\\t")
          \newline    (.append sb "\\n")
          \return     (.append sb "\\r")
          \backspace  (.append sb "\\b")
          \formfeed   (.append sb "\\f")
          (.append sb ch))))
    (.toString sb)))

(defn format-row
  "Format a row vector into a COPY TEXT line string.
   row - vector of String-or-nil values (may be shorter or longer than cast-specs)
   cast-specs - vector of cast keywords (or nil) per column
   Missing columns are output as \\N (NULL). Extra columns are ignored."
  [row cast-specs]
  (let [sb (StringBuilder.)
        n (count cast-specs)]
    (dotimes [i n]
      (when (pos? i)
        (.append sb \tab))
      (if-let [raw (nth row i nil)]
        (let [cast-fn (nth cast-specs i nil)
              val (if cast-fn (cast/apply-cast cast-fn raw) raw)]
          (.append sb (escape-copy-text val)))
        (.append sb "\\N")))
    (.append sb \newline)
    (.toString sb)))

(defn format-row-bytes
  "Format a row and return as UTF-8 byte array"
  [row cast-specs]
  (.getBytes ^String (format-row row cast-specs) StandardCharsets/UTF_8))

(defn copy-sql
  "Generate COPY FROM STDIN SQL for a table spec.
   Expects keys :target-schema, :target-table, :columns."
  [{:keys [target-schema target-table columns]}]
  (format "COPY %s.%s (%s) FROM STDIN WITH (FORMAT TEXT)"
          (quote-id target-schema)
          (quote-id target-table)
          (str/join ", "
                    (map (comp quote-id
                               #(or (:column-name %) (:name %)))
                         columns))))
