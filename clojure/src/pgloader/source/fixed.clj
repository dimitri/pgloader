(ns pgloader.source.fixed
  "Fixed-width file source.

   Source map keys:
     :path            Absolute path to the .dat file (after fixed:// prefix stripped).
     :inline-data     String of inline data (when FROM inline).
     :filename-pattern Regex string for FILENAME MATCHING mode.
                       Resolved at runtime against :archive-dir.
     :archive-dir     java.io.File — temp dir of the expanded archive.
     :fields          Vec of field specs produced by the grammar:
                        {:name string :start int :length int
                         :trim-right bool :null-if-blanks bool}
     :encoding        Charset name string (default ISO-8859-1).
     :fixed-header    When true the first line of the file defines column names
                      and their start positions; no explicit field list needed."
  (:require [pgloader.source.protocol :refer [Source]]
            [pgloader.utils.archive    :as archive]
            [clojure.string            :as str]
            [clojure.java.io           :as io]
            [clojure.tools.logging     :as log])
  (:import [java.io BufferedReader InputStreamReader File]
           [java.nio.charset Charset StandardCharsets]))

(set! *warn-on-reflection* true)

;; ── Field extraction ─────────────────────────────────────────────────────────

(defn- extract-field
  "Extract one fixed-width field from a line string."
  [^String line {:keys [^long start ^long length trim-right null-if-blanks]}]
  (let [line-len (.length line)
        from     (min start line-len)
        to       (min (+ start length) line-len)
        raw      (.substring line from to)
        ;; Right-pad to declared length so trim semantics are consistent
        raw      (if (< (count raw) length)
                   (str raw (apply str (repeat (- length (count raw)) \space)))
                   raw)
        val      (if trim-right (str/trimr raw) raw)
        val      (if (and null-if-blanks (str/blank? val)) nil val)]
    val))

(defn- parse-line
  "Extract all fields from line according to field specs. Returns a vector of strings (or nil)."
  [^String line fields]
  (mapv #(extract-field line %) fields))

;; ── Fixed-header inference ────────────────────────────────────────────────────

(defn- infer-fields-from-header
  "Read the first line of a fixed-width file and infer column specs.
   Column boundaries are the start positions of each whitespace-delimited token
   in the header line; widths are the distances to the next boundary (or EOL)."
  [^String header-line]
  (let [n (.length header-line)]
    (loop [i (long 0) fields []]
      (if (>= i n)
        fields
        (if (= \space (.charAt header-line i))
          (recur (inc i) fields)
          ;; Start of a token — find its end
          (let [j (loop [j (long (inc i))]
                    (if (or (>= j n) (= \space (.charAt header-line j)))
                      j
                      (recur (inc j))))
                col-name (.substring header-line i j)
                ;; Width extends to the start of the next token (or EOL)
                next-tok (loop [k (long j)]
                           (if (>= k n)
                             n
                             (if (not= \space (.charAt header-line k))
                               k
                               (recur (inc k)))))]
            (recur (long next-tok)
                   (conj fields {:name          (str/lower-case col-name)
                                 :start         i
                                 :length        (- next-tok i)
                                 :trim-right    true
                                 :null-if-blanks false}))))))))

;; ── Line reader ───────────────────────────────────────────────────────────────

(defn- open-reader ^BufferedReader [source encoding]
  (let [^Charset cs (if encoding
                      (Charset/forName ^String encoding)
                      StandardCharsets/ISO_8859_1)]
    (if-let [data (:inline-data source)]
      (BufferedReader. (InputStreamReader. (java.io.ByteArrayInputStream. (.getBytes ^String data cs)) cs))
      (BufferedReader. (InputStreamReader. (io/input-stream (File. ^String (:path source))) cs)))))

(defn- read-rows-from-reader
  "Read fixed-width rows from rdr lazily, closing the reader when exhausted."
  [^BufferedReader rdr fields fixed-header?]
  (let [eff-fields (if fixed-header?
                     (when-let [header (.readLine rdr)]
                       (infer-fields-from-header header))
                     fields)]
    (when eff-fields
      ((fn row-seq []
         (if-let [line (.readLine rdr)]
           (lazy-seq (cons (parse-line line eff-fields) (row-seq)))
           (do (.close rdr) nil)))))))

;; ── Catalog helper ────────────────────────────────────────────────────────────

(defn- fields->columns [fields]
  (mapv (fn [{:keys [name]}]
          {:column-name    name
           :column-type    "text"
           :is-nullable    true
           :column-default nil
           :extra          nil})
        fields))

(defn- table-name-from-path [^String path]
  (-> path
      (str/split #"/")
      last
      (str/replace #"(?i)\.(dat|txt|csv|fix|fixed)$" "")
      str/lower-case))

;; ── Source records ────────────────────────────────────────────────────────────

(defrecord FixedFileSource [source fields encoding fixed-header? select-cols]
  Source

  (source-name [_]
    (cond
      (:inline-data source)      "fixed:inline"
      (:filename-pattern source) (str "fixed:matching:" (:filename-pattern source))
      :else                      (str "fixed:" (:path source))))

  (catalog [_]
    ;; For FILENAME MATCHING mode: scan the archive dir for matching files.
    ;; For all modes: one table whose columns are derived from field specs
    ;; (or inferred from the header on first read).
    (let [resolved-files
          (if (:filename-pattern source)
            (archive/matching-files (:archive-dir source) (:filename-pattern source))
            nil)
          tname (or (when-let [^java.io.File f (first resolved-files)]
                      (table-name-from-path (.getName f)))
                    (some-> (:path source) table-name-from-path)
                    "fixed")
          ;; Peek at the first line if fixed-header? to get column names
          eff-fields
          (if fixed-header?
            (let [first-file (or (first resolved-files)
                                 (when (:path source) (File. ^String (:path source))))]
              (when first-file
                (with-open [rdr (BufferedReader.
                                 (InputStreamReader.
                                  (io/input-stream first-file)
                                  (if encoding
                                    (Charset/forName ^String encoding)
                                    StandardCharsets/ISO_8859_1)))]
                  (some-> (.readLine rdr) infer-fields-from-header))))
            fields)
          all-cols (fields->columns (or eff-fields []))
          ;; If select-columns was specified (post-INTO column list), project to that subset.
          final-cols (if (seq select-cols)
                       (filterv #(some #{(:column-name %)} select-cols) all-cols)
                       all-cols)]
      [{:table-name  tname
        :schema      "public"
        :columns     final-cols
        :primary-key []
        :indexes     []
        :fkeys       []}]))

  (partition-source [_ _ _ _] nil)

  (close! [_])

  (read-query [_ _sql]
    (throw (UnsupportedOperationException. "read-query not supported for fixed source")))

  (read-rows [_ table-spec]
    ;; Collect all files to read.
    (let [files (cond
                  (:filename-pattern source)
                  (archive/matching-files (:archive-dir source) (:filename-pattern source))

                  (:inline-data source) nil   ; handled below

                  :else
                  [(File. ^String (:path source))])]
      (if (:inline-data source)
        ;; Inline data: read from the embedded string
        (let [rdr (open-reader source encoding)]
          (read-rows-from-reader rdr fields fixed-header?))
        ;; File(s): concatenate rows from all matching files lazily.
        ;; read-rows-from-reader closes each reader when its seq is exhausted.
        (let [rows (mapcat (fn [^File f]
                             (let [cs  (if encoding
                                         (Charset/forName ^String encoding)
                                         StandardCharsets/ISO_8859_1)
                                   rdr (BufferedReader. (InputStreamReader. (io/input-stream f) cs))]
                               (read-rows-from-reader rdr fields fixed-header?)))
                           files)]
          ;; Project to select-cols if specified (post-INTO column list)
          (if (seq select-cols)
            (let [field-names (mapv :name fields)
                  col-set     (set select-cols)
                  proj-idx    (vec (keep-indexed (fn [i _] (when (col-set (nth field-names i)) i))
                                                 (range (count field-names))))]
              (map (fn [row] (mapv #(nth row % nil) proj-idx)) rows))
            rows))))))

(defn create-source
  "Create a FixedFileSource from a parsed source map.

   Expected keys in source-map:
     :path            Absolute path (fixed:// scheme stripped).
     :inline-data     Inline data string.
     :filename-pattern Regex for FILENAME MATCHING.
     :archive-dir     java.io.File temp dir from archive expansion.
     :fields          Vec of {:name :start :length :trim-right :null-if-blanks}.
     :encoding        Charset name.
     :fixed-header    Boolean."
  [source-map]
  (let [fields       (:fields source-map)
        encoding     (:encoding source-map)
        fixed-header (:fixed-header source-map false)
        select-cols  (:select-columns source-map)]
    (->FixedFileSource source-map fields encoding fixed-header select-cols)))
