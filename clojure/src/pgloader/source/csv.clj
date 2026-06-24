(ns pgloader.source.csv
  (:require [pgloader.source.protocol :refer [Source read-rows source-name]]
            [pgloader.cast :as cast]
            [pgloader.transforms :as transforms]
            [pgloader.utils.archive :as archive]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [com.opencsv CSVReader CSVReaderBuilder CSVParserBuilder]
           [java.io StringReader File BufferedReader InputStreamReader]
           [java.nio.charset Charset StandardCharsets]))

(set! *warn-on-reflection* true)

(defn- matching-files
  "Return sorted list of files in directory whose name matches pattern."
  [^String directory ^String pattern]
  (let [dir (File. directory)
        re  (re-pattern pattern)]
    (->> (.listFiles dir)
         (filter #(re-matches re (.getName ^File %)))
         (sort-by #(.getName ^File %)))))

;; ── CSV format guessing ──────────────────────────────────────────────────────

(def ^:private ^:const guess-separators [\tab \, \; \| \% \^ \! \$])

(defn- try-csv-params
  "Return true if all LINES parse with exactly N-COLS columns using the given params."
  [lines n-cols sep quote-char escape-char]
  (try
    (let [parser (.build (doto (CSVParserBuilder.)
                           (.withSeparator sep)
                           (.withQuoteChar quote-char)
                           (.withEscapeChar escape-char)))]
      (every? (fn [line]
                (let [reader (.build (doto (CSVReaderBuilder. (StringReader. line))
                                       (.withCSVParser parser)))
                      row (.readNext reader)]
                  (and row (= n-cols (alength row)))))
              lines))
    (catch Exception _ false)))

(defn guess-csv-params
  "Sample the first N lines of FILE-PATH and try common separators.
   Returns a map of :separator :quote-char :escape-char, or nil if detection fails.
   Mirrors CL pgloader's guess-csv-params."
  [^String file-path n-cols]
  (let [sample (try (with-open [r (io/reader file-path)]
                      (vec (take 10 (line-seq r))))
                    (catch Exception _ nil))]
    (when (seq sample)
      (loop [seps guess-separators]
        (when-let [sep (first seps)]
          (or (when (try-csv-params sample n-cols sep \" \\)
                (log/info (str "Guessed CSV params: fields terminated by '"
                               sep "', fields optionally enclosed by '\"', fields escaped by '\\'"))
                {:separator sep :quote-char \" :escape-char \\})
              (when (try-csv-params sample n-cols sep \" \")
                (log/info (str "Guessed CSV params: fields terminated by '"
                               sep "', fields optionally enclosed by '\"', fields escaped by '\"'"))
                {:separator sep :quote-char \" :escape-char \"})
              (recur (rest seps))))))))

;; ── Quoted-field–aware CSV line parser ──────────────────────────────────────
;; Used when trim-unquoted-blanks is set so we can tell which fields are quoted.

(defn- csv-parse-line-with-flags
  "Parse one CSV line into [values quoted-flags].
   values: vector of String (nil for empty between seps); quoted-flags: boolean vector.
   Returns nil if line is nil."
  [^String line sep-ch qc esc-ch]
  (when line
    (let [n (.length ^String line)
          result-vals (java.util.ArrayList.)
          result-quot (java.util.ArrayList.)
          sb (StringBuilder.)]
      (loop [pos 0 in-q false is-q false]
        (if (>= pos n)
          (do (.add result-vals (str sb))
              (.add result-quot (boolean is-q))
              [(vec result-vals) (vec result-quot)])
          (let [c  (.charAt ^String line pos)
                c* (Character/valueOf c)]
            (cond
              in-q
              (cond
                ;; backslash escape inside quoted field (only when esc ≠ qc)
                (and esc-ch (not= esc-ch qc) (= c* esc-ch) (< (inc pos) n))
                (do (.append sb (.charAt ^String line (inc pos)))
                    (recur (+ pos 2) true is-q))
                ;; closing quote (double-quote escape handled below)
                (and qc (= c* qc))
                (if (and (< (inc pos) n) (= (Character/valueOf (.charAt ^String line (inc pos))) qc))
                  (do (.append sb qc) (recur (+ pos 2) true is-q))
                  (recur (inc pos) false is-q))
                :else
                (do (.append sb c) (recur (inc pos) true is-q)))
              ;; separator
              (= c* sep-ch)
              (do (.add result-vals (if (zero? (.length sb)) nil (str sb)))
                  (.add result-quot (boolean is-q))
                  (.setLength sb 0)
                  (recur (inc pos) false false))
              ;; opening quote at field start (empty sb)
              (and qc (= c* qc) (zero? (.length sb)))
              (recur (inc pos) true true)
              :else
              (do (.append sb c) (recur (inc pos) false is-q)))))))))

(defn- normalize-line-endings
  "Replace custom line terminators with newlines for opencsv."
  [^String s term]
  (if (or (nil? term) (= term \newline))
    s
    (str/replace s (str term) "\n")))

(defn translate-cl-date-format
  "Translate a pgloader CL date format string to Java DateTimeFormatter pattern.
   US (microseconds) is returned as a special marker :us that callers must handle
   via DateTimeFormatterBuilder with appendFraction for variable-width fractional digits."
  [^String cl-fmt]
  (let [has-us (str/includes? cl-fmt "US")]
    [(-> cl-fmt
         (str/replace "YYYY" "yyyy")
         (str/replace "MM"   "MM")
         (str/replace "DD"   "dd")
         (str/replace "HH24" "HH")
         (str/replace "MI"   "mm")
         (str/replace "SS"   "ss")
         (str/replace ".US" "")
         (str/replace "US" ""))
     has-us]))

(defn- build-formatter
  "Build a DateTimeFormatter for the given Java pattern, optionally
   with variable-width fractional seconds (up to 6 digits / microseconds)."
  [^String java-pattern has-us?]
  (if has-us?
    (let [builder (java.time.format.DateTimeFormatterBuilder.)]
      (.appendPattern builder java-pattern)
      (when has-us?
        (.appendFraction builder
                         java.time.temporal.ChronoField/MICRO_OF_SECOND
                         0 6 true))
      (.toFormatter builder))
    (java.time.format.DateTimeFormatter/ofPattern java-pattern)))

(defn- format-datetime
  "Format a LocalDateTime as a PG timestamp string with variable microseconds."
  [^java.time.LocalDateTime dt]
  (let [nano (.getNano dt)
        micros (/ nano 1000)
        base (str (.format dt
                           (java.time.format.DateTimeFormatter/ofPattern
                            "yyyy-MM-dd HH:mm:ss")))]
    (if (zero? micros)
      (str base ".000000")
      (format "%s.%06d" base micros))))

(defn apply-date-format
  "Parse a date/time string using pgloader CL format, return PG-compatible string."
  [^String value ^String cl-fmt]
  (let [[java-pattern has-us?] (translate-cl-date-format cl-fmt)
        formatter (build-formatter java-pattern has-us?)]
    (try
      (-> (java.time.LocalDateTime/parse value formatter)
          format-datetime)
      (catch Exception _
        (-> (java.time.LocalTime/parse value formatter)
            (.format java.time.format.DateTimeFormatter/ISO_LOCAL_TIME))))))

(defrecord CSVSource
           [^String filepath
            ^Charset encoding
            ^long skip-lines
            ^Character separator
            ^Character quote-char
            ^Character escape-char
            column-names
            nullif
            keep-unquoted-blanks
            trim-unquoted-blanks
            inline-data
            projections
            csv-header
            lines-terminator
            column-formats
            column-nullifs
            stdin?]

  Source
  (source-name [_] (cond stdin? "csv:stdin"
                         filepath (str "csv:" filepath)
                         :else "csv:inline"))

  (catalog [_]
    (log/info (str "Reading CSV source: " (cond stdin? "stdin" filepath filepath :else "inline")))
    (let [table-name (cond
                       filepath (-> filepath (str/split #"/") last (str/replace #"\.\w+$" ""))
                       stdin?   "stdin"
                       :else    "inline")
          ;; For csv-header mode with no explicit columns, peek at first line to get column names
          header-cols (when (and csv-header filepath (not (seq column-names)))
                        (try
                          (with-open [rdr (io/reader filepath :encoding (.name encoding))]
                            (let [sep-ch (or separator \,)
                                  qc-ch  (or quote-char \")
                                  ;; eff-esc: nil means no escape (double-quote mode)
                                  eff-esc (when (and escape-char (not= escape-char qc-ch))
                                            escape-char)
                                  line   (.readLine ^BufferedReader (BufferedReader. rdr))]
                              (when line
                                (first (csv-parse-line-with-flags line sep-ch qc-ch eff-esc)))))
                          (catch Exception _ nil)))
          use-columns (if (seq projections)
                        (mapv :column-name projections)
                        (or header-cols column-names))]
      [{:table-name table-name
        :schema "public"
        :columns (if (seq use-columns)
                   (mapv (fn [n] {:column-name n
                                  :column-type "text"
                                  :is-nullable true
                                  :extra nil})
                         use-columns)
                   [{:column-name "data"
                     :column-type "text"
                     :is-nullable true
                     :extra nil}])}]))

  (partition-source [_ _ _ _] nil)

  (close! [_])

  (create-view! [_ _ _ _]
    (throw (UnsupportedOperationException. "create-view! not supported for CSV source")))

  (drop-view! [_ _ _]
    (throw (UnsupportedOperationException. "drop-view! not supported for CSV source")))

  (read-query [_ _sql]
    (throw (UnsupportedOperationException. "read-query not supported for CSV source")))

  (read-rows [_ table-spec]
    (let [guessed   (when (and (not separator) filepath)
                      (guess-csv-params filepath (count column-names)))
          sep       (or separator (:separator guessed) \,)
          qc        (if guessed (:quote-char guessed) quote-char)
          ;; Default escape = quote-char (v3 compat): when qc==esc opencsv disables escape,
          ;; preserving \N in unquoted fields for null-if matching.
          raw-esc   (if (and guessed (:escape-char guessed))
                      (:escape-char guessed)
                      (or escape-char qc))
          ;; eff-esc for opencsv: (char 0) when qc==raw-esc (disables escape processing)
          eff-esc   (if (and qc raw-esc (= (char qc) (char raw-esc)))
                      (char 0)
                      (or raw-esc \\))
          parser (.build
                  (doto (CSVParserBuilder.)
                    (.withSeparator (char sep))
                    (.withQuoteChar (if qc (char qc) (char 0)))
                    (.withEscapeChar (char eff-esc))
                    (.withFieldAsNull com.opencsv.enums.CSVReaderNullFieldIndicator/EMPTY_SEPARATORS)))
          raw-input (cond
                      inline-data
                      (StringReader. (if (and lines-terminator
                                              (not= \newline lines-terminator))
                                       (normalize-line-endings inline-data lines-terminator)
                                       inline-data))
                      stdin?
                      (InputStreamReader. System/in ^Charset encoding)
                      :else
                      (io/reader filepath :encoding (.name encoding)))
          reader (.build
                  (doto (CSVReaderBuilder. raw-input)
                    (.withCSVParser parser)
                    (.withSkipLines (int (if csv-header 0 skip-lines)))))
          header-row (when csv-header (.readNext reader))
          cols (or (when csv-header (vec header-row)) column-names)
          nil-str nullif
          ;; When opencsv uses \ as escape, \N is eaten to N; also match the stripped form
          nil-str-alt (when (and nil-str (= eff-esc \\) (str/starts-with? nil-str "\\"))
                        (subs nil-str 1))
          ;; col-nil-map: column-name → vector of null-if specs.
          ;; Each spec is either :blanks (keyword) or a string to match.
          ;; :nullifs replaces the old single :nullif field.
          col-nil-map (when (and cols (seq column-nullifs))
                        (into {}
                              (map (fn [cn] [(:name cn) (:nullifs cn)])
                                   column-nullifs)))
          ;; When opencsv strips backslash escapes, "\N" becomes "N".
          ;; Build an alt map: original-string → stripped-string for each
          ;; backslash-prefixed null-if value so [null if '\N'] still matches.
          col-nil-val-alt (when (and col-nil-map (= eff-esc \\))
                            (into {}
                                  (keep (fn [[_ vs]]
                                          (some (fn [v]
                                                  (when (and (string? v) (str/starts-with? v "\\"))
                                                    [v (subs v 1)]))
                                                vs))
                                        col-nil-map)))
          trim-blanks trim-unquoted-blanks
          ;; Open a second reader to detect per-field quoted status.
          ;; Used for trim-unquoted-blanks AND for null-if comparison (unquoted fields
          ;; get whitespace stripped before matching, so "   " matches null-if "").
          needs-quot (or trim-blanks nil-str (seq col-nil-map))
          trim-reader (when (and needs-quot filepath (not inline-data) (not stdin?))
                        (let [rdr (BufferedReader.
                                   (InputStreamReader.
                                    (io/input-stream filepath)
                                    (.name encoding)))
                              skip-n (+ (if csv-header 1 0)
                                        (if csv-header 0 (int skip-lines)))]
                          (dotimes [_ skip-n] (.readLine ^BufferedReader rdr))
                          rdr))
          source-idx (and cols (seq projections)
                          (into {} (map-indexed (fn [i n] [n i]) cols)))
          date-transforms (when (and cols column-formats)
                            (let [fmts (into {} (map (fn [cf] [(:name cf) (:date-format cf)])
                                                     column-formats))]
                              (keep-indexed (fn [i n]
                                              (when-let [fmt (get fmts n)]
                                                [i fmt]))
                                            cols)))
          date-transform-fn (when (seq date-transforms)
                              (fn [row]
                                (reduce (fn [acc [i fmt]]
                                          (update acc i #(apply-date-format % fmt)))
                                        row date-transforms)))
          apply-null-trim (fn [v col-nils is-quoted]
                            ;; col-nils is a seq of null-if specs for this column
                            ;; (each is :blanks keyword or a string).
                            ;; For unquoted fields (is-quoted=false, only reliable when
                            ;; trim-reader active) match null-if against raw OR trimmed value.
                            (let [unquoted? (and v trim-reader (false? is-quoted))
                                  matches-spec? (fn [spec]
                                                  (cond
                                                    (= :blanks spec)
                                                    ;; blanks: empty or whitespace-only string
                                                    (and v (str/blank? v))
                                                    (string? spec)
                                                    (when v
                                                      (let [alt (when (and (= eff-esc \\)
                                                                           (str/starts-with? spec "\\"))
                                                                  (subs spec 1))]
                                                        (or (= v spec)
                                                            (when alt (= v alt))
                                                            (when unquoted? (= (str/trim v) spec))
                                                            (when (and alt unquoted?) (= (str/trim v) alt)))))
                                                    :else false))
                                  ;; Check global null-if (WITH ... null if 'x')
                                  global-null? (or (when nil-str (matches-spec? nil-str))
                                                   (when nil-str-alt (matches-spec? nil-str-alt)))
                                  ;; Check per-column null-if specs
                                  col-null?  (when (seq col-nils)
                                               (some matches-spec? col-nils))
                                  v (if (or global-null? col-null?) nil v)]
                              ;; trim-unquoted-blanks: unquoted only; trim and blank → nil
                              (if (and trim-blanks (not is-quoted))
                                (if (nil? v)
                                  nil
                                  (let [trimmed (str/trim v)]
                                    (when-not (str/blank? trimmed) trimmed)))
                                v)))
          ;; Build projection: each entry is [source-idx transform-fn-or-nil]
          proj-fn (when source-idx
                    (let [mappings (mapv (fn [p i]
                                           (let [using (:using p)]
                                             (if using
                                              ;; Check if it's a compilable Clojure fn form
                                               (let [xfn (transforms/compile-using-expr using)]
                                                 (if xfn
                                                  ;; fn transform — read this col by position
                                                   [(when (< i (count cols)) i) xfn]
                                                  ;; Column reference — extract name, strip parens
                                                   (let [src-name (if (= \" (first using))
                                                                    (str/replace using #"^\"|\"$" "")
                                                                    (-> (last (str/split using #"\s+"))
                                                                        (str/replace #"[()]" "")))]
                                                     [(get source-idx src-name) nil])))
                                               [(when (< i (count cols)) i) nil])))
                                         projections (range (count projections)))]
                      (fn [^"[Ljava.lang.String;" line quoted-flags]
                        (mapv (fn [[idx xfn]]
                                (when (and idx (< idx (alength line)))
                                  (let [v     (aget line idx)
                                        is-q  (when (and needs-quot quoted-flags)
                                                (nth quoted-flags idx false))
                                        cnil  (when col-nil-map
                                                (when (and cols idx)
                                                  (get col-nil-map (nth cols idx))))
                                        val   (apply-null-trim v cnil is-q)]
                                    (if xfn
                                      (try (xfn val) (catch Exception _ val))
                                      val))))
                              mappings))))]
      ((fn thisfn []
         (when-let [^"[Ljava.lang.String;" line (.readNext reader)]
           (let [raw-line     (when trim-reader (.readLine ^BufferedReader trim-reader))
                 quoted-flags (when (and needs-quot raw-line)
                                ;; raw-esc for quoted detection (not eff-esc)
                                (let [det-esc (when (and raw-esc (not= (char 0) (char raw-esc)))
                                                (char raw-esc))]
                                  (second (csv-parse-line-with-flags
                                           raw-line (char sep)
                                           (when qc (char qc))
                                           det-esc))))]
             (if (or (and (= 1 (alength line)) (str/blank? (aget line 0)))
                     (and cols (every? true? (map = cols (seq line)))))
               (thisfn)
               (lazy-seq
                (cons (let [raw (if proj-fn
                                  (proj-fn line quoted-flags)
                                  (loop [i 0 result (transient [])]
                                    (if (< i (alength line))
                                      (let [v     (aget line i)
                                            is-q  (when (and needs-quot quoted-flags)
                                                    (nth quoted-flags i false))
                                            cnil  (when col-nil-map
                                                    (when cols
                                                      (get col-nil-map (nth cols i nil))))]
                                        (recur (inc i)
                                               (conj! result (apply-null-trim v cnil is-q))))
                                      (persistent! result))))]
                        (if date-transform-fn
                          (date-transform-fn raw)
                          raw))
                      (thisfn)))))))))))

(defrecord GlobCSVSource
           [^String directory
            ^String pattern
            ^Charset encoding
            ^long skip-lines
            ^Character separator
            ^Character quote-char
            ^Character escape-char
            column-names
            nullif
            keep-unquoted-blanks
            trim-unquoted-blanks
            projections]

  Source
  (source-name [_] (str "csv-glob:" directory "/" pattern))

  (catalog [_]
    (log/info (str "Glob CSV source: " directory " matching " pattern))
    (let [files (matching-files directory pattern)
          use-columns (if (seq projections)
                        (mapv :column-name projections)
                        column-names)]
      [{:table-name (str "glob-" (count files))
        :schema "public"
        :columns (if (seq use-columns)
                   (mapv (fn [n] {:column-name n
                                  :column-type "text"
                                  :is-nullable true
                                  :extra nil})
                         use-columns)
                   [{:column-name "data"
                     :column-type "text"
                     :is-nullable true
                     :extra nil}])}]))

  (partition-source [_ _ _ _] nil)

  (close! [_])

  (create-view! [_ _ _ _]
    (throw (UnsupportedOperationException. "create-view! not supported for CSV source")))

  (drop-view! [_ _ _]
    (throw (UnsupportedOperationException. "drop-view! not supported for CSV source")))

  (read-query [_ _sql]
    (throw (UnsupportedOperationException. "read-query not supported for CSV source")))

  (read-rows [_ table-spec]
    (let [files (matching-files directory pattern)]
      (mapcat (fn [f]
                (read-rows (map->CSVSource {:filepath (.getPath ^File f)
                                            :encoding encoding
                                            :skip-lines skip-lines
                                            :separator separator
                                            :quote-char quote-char
                                            :escape-char escape-char
                                            :column-names column-names
                                            :nullif nullif
                                            :keep-unquoted-blanks keep-unquoted-blanks
                                            :trim-unquoted-blanks trim-unquoted-blanks
                                            :inline-data nil
                                            :projections projections
                                            :csv-header nil
                                            :lines-terminator nil
                                            :column-formats nil
                                            :column-nullifs nil
                                            :stdin? false})
                           table-spec))
              files))))

(defn create-source
  "Create a CSVSource or GlobCSVSource from load file AST or CLI options."
  [{:keys [path columns skip-header delimiter quote-char escape-char encoding
           nullif keep-unquoted-blanks trim-unquoted-blanks inline inline-data stdin
           projections csv-header lines-terminated column-formats column-nullifs
           glob-pattern directory target-columns
           filename-pattern archive-dir]}]
  ;; FILENAME MATCHING inside a LOAD ARCHIVE: resolve pattern against extracted dir.
  (when (and filename-pattern (not archive-dir))
    (throw (ex-info "FILENAME MATCHING requires an archive context" {:pattern filename-pattern})))
  (if (and glob-pattern directory)
    ;; Glob source
    (let [enc (or (when encoding (Charset/forName encoding))
                  StandardCharsets/UTF_8)
          col-names (when (seq columns) (mapv name columns))
          qc (when (some? quote-char) (char quote-char))]
      (->GlobCSVSource directory glob-pattern enc
                       (long (or skip-header 0))
                       delimiter qc
                       (if (and qc escape-char (= (char qc) (char escape-char))) (char 0) (or escape-char \\))
                       col-names nullif keep-unquoted-blanks trim-unquoted-blanks
                       projections))
    ;; Regular CSV source (or FILENAME MATCHING resolved from archive dir)
    (let [filepath (cond
                     (and filename-pattern archive-dir)
                     ;; Resolve the first file matching the regex in the archive dir.
                     (let [matched (archive/matching-files archive-dir filename-pattern)]
                       (when (seq matched) (.getAbsolutePath ^java.io.File (first matched))))
                     (and path (not= path :inline) (not stdin)) path
                     :else nil)
          enc (or (when encoding (Charset/forName encoding))
                  (when inline-data StandardCharsets/UTF_8)
                  StandardCharsets/UTF_8)
          col-names (when (seq columns) (mapv name columns))
          qc (when (some? quote-char) (char quote-char))
          lt (when lines-terminated (char lines-terminated))
          ;; When target-columns is specified but no explicit projections,
          ;; derive projections from target→source column name mapping.
          eff-projections (or (when (seq projections) projections)
                              (when (and (seq target-columns) (seq col-names))
                                (mapv (fn [tc] {:column-name tc :using tc}) target-columns)))]
      (->CSVSource filepath enc
                   (long (or skip-header 0))
                   delimiter qc
                   (if (and qc escape-char (= (char qc) (char escape-char))) (char 0) (or escape-char qc))
                   col-names nullif keep-unquoted-blanks trim-unquoted-blanks
                   inline-data eff-projections
                   (boolean csv-header) lt column-formats column-nullifs
                   (boolean stdin)))))
