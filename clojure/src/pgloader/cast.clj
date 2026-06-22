(ns pgloader.cast
  (:import [java.nio.charset StandardCharsets])
  (:require [clojure.string :as str]))

(set! *warn-on-reflection* true)

;; ── Cast functions ────────────────────────────────────────────────

(defn zero-dates-to-null
  "Convert MySQL zero-dates to SQL NULL"
  [^String v]
  (when v
    (if (or (.startsWith v "0000-00-00")
            (= v "0000-00-00 00:00:00"))
      nil
      v)))

(defn tinyint-to-boolean
  "Convert MySQL tinyint(1) 0/1 to PostgreSQL boolean f/t"
  [^String v]
  (when v (if (= v "0") "f" "t")))

(defn tinyint-to-integer
  "Pass-through for tinyint to integer"
  [^String v]
  v)

(defn year-to-integer
  "Pass-through for year to integer"
  [^String v]
  v)

(defn empty-string-to-null
  "Convert empty string to SQL NULL"
  [^String v]
  (when v
    (when-not (empty? v) v)))

(defn right-trim
  "Right-trim whitespace from string"
  [^String v]
  (when v (clojure.string/trimr v)))

(defn remove-null-characters
  "Remove embedded null characters"
  [^String v]
  (when v (clojure.string/replace v "\0" "")))

(defn int-to-ip
  "Convert a 32-bit integer string to dotted-decimal IP"
  [^String v]
  (when v
    (try
      (let [n (Long/parseLong v)]
        (str (bit-and (bit-shift-right n 24) 0xFF) "."
             (bit-and (bit-shift-right n 16) 0xFF) "."
             (bit-and (bit-shift-right n 8) 0xFF) "."
             (bit-and n 0xFF)))
      (catch NumberFormatException _ v))))

(defn bytes-to-pg-bytea
  "Encode raw bytes as PostgreSQL hex bytea format"
  [^String v]
  (when v
    (let [bytes (.getBytes v java.nio.charset.StandardCharsets/ISO_8859_1)
          sb    (StringBuilder. (+ 2 (* 2 (alength bytes))))]
      (.append sb "\\x")
      (doseq [b bytes]
        (.append sb (format "%02x" (bit-and ^int b 0xff))))
      (.toString sb))))

(defn sqlite-timestamp-to-timestamp
  "Convert SQLite timestamp formats to PostgreSQL timestamp"
  [^String v]
  (when v
    (try
      (let [n (Long/parseLong v)]
        (str n "-01-01"))
      (catch NumberFormatException _
        v))))

(defn base64-decode
  "Decode a Base64 string to its UTF-8 text representation."
  [^String v]
  (when v
    (String. (.decode (java.util.Base64/getDecoder) v)
             java.nio.charset.StandardCharsets/UTF_8)))

(defn byte-vector-to-hexstring
  "Convert a hex-bytea string (\\\\xDEADBEEF) to UUID-like hex string.
   Strips the \\\\x prefix and lowercases."
  [^String v]
  (when v
    (let [hex (if (str/starts-with? v "\\x") (subs v 2) v)]
      (str/lower-case hex))))

(defn numeric-to-integer
  "Truncate a decimal numeric string to a PostgreSQL integer string.
   Used when a DBF NUMERIC field with decimal places is cast to integer."
  [^String v]
  (when v
    (try
      (str (.longValue (java.math.BigDecimal. v)))
      (catch Exception _ v))))

(defn bits-to-boolean
  "Convert MySQL bit(1) to PostgreSQL boolean.
   MySQL returns byte arrays for BIT columns; a single 0x00 byte → 'f', else 't'.
   Matches CL bits-to-boolean which checks the first element of the bit-vector."
  [^String v]
  (when v
    ;; In the Clojure path, BIT(1) arrives as a hex string like \"X00\" or \"X01\"
    ;; (from the byte-array encoding in mysql.clj) or as \"0\"/\"1\" strings.
    (let [lower (str/lower-case (str/trim v))]
      (cond
        (or (= lower "x00") (= lower "0") (= lower "false") (= lower "f")) "f"
        :else "t"))))

(defn int-to-uuid
  "Convert an integer string to UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
   Mirrors CL pgloader's int-to-uuid which slices the integer into UUID-shaped fields."
  [^String v]
  (when v
    (try
      (let [n     (java.math.BigInteger. v)
            m32   (java.math.BigInteger. "FFFFFFFF" 16)
            m16   (java.math.BigInteger. "FFFF" 16)
            m48   (java.math.BigInteger. "FFFFFFFFFFFF" 16)
            p1    (.and (.shiftRight n 96) m32)
            p2    (.and (.shiftRight n 80) m16)
            p3    (.and (.shiftRight n 64) m16)
            p4    (.and (.shiftRight n 48) m16)
            p5    (.and n m48)]
        (format "%08x-%04x-%04x-%04x-%012x"
                (.longValue p1) (.longValue p2) (.longValue p3)
                (.longValue p4) (.longValue p5)))
      (catch Exception _ v))))

(defn hex-to-bytea
  "Convert a hex string to PostgreSQL bytea \\x... format (#1066).
   Input may be plain hex (DEADBEEF) or prefixed (0xDEADBEEF / \\xDEADBEEF).
   Used when loading CSV/fixed fields that carry hex-encoded binary data."
  [^String v]
  (when v
    (let [trimmed (str/trim v)
          hex (cond
                (str/starts-with? trimmed "0x") (subs trimmed 2)
                (str/starts-with? trimmed "\\x") (subs trimmed 2)
                :else trimmed)]
      (str "\\x" (str/lower-case hex)))))

(defn set-to-enum-array
  "Convert a MySQL SET value (comma-separated string) to PostgreSQL array format {val1,val2}.
   Mirrors CL pgloader's set-to-enum-array."
  [^String v]
  (when v (str "{" v "}")))

(defn convert-mysql-point
  "Convert a MySQL POINT WKT string to PostgreSQL point format.
   Input:  'POINT(48.5513589 7.6926827)'
   Output: '(48.5513589,7.6926827)'"
  [^String v]
  (when v
    (let [point (subs v 6 (dec (count v)))
          comma-pos (.indexOf point " ")]
      (if (neg? comma-pos)
        v
        (str "(" (subs point 0 comma-pos) "," (subs point (inc comma-pos)) ")")))))

(defn convert-mysql-linestring
  "Convert a MySQL LINESTRING WKT string to PostgreSQL path format.
   Input:  'LINESTRING(-87.87 45.79,-87.87 45.80)'
   Output: '[(-87.87,45.79),(-87.87,45.80)]'"
  [^String v]
  (when v
    (let [data (subs v 11 (dec (count v)))
          points (str/split data #",(?=[-\d])")]
      (str "["
           (str/join ","
                     (map (fn [pt]
                            (let [sp (.indexOf ^String pt " ")]
                              (str "(" (subs pt 0 sp) "," (subs pt (inc sp)) ")")))
                          points))
           "]"))))

(defn bits-to-hex-bitstring
  "Convert MySQL bit(X) to PostgreSQL bit(X) hex format.
   MySQL returns byte arrays encoded as Xhex strings in our pipeline.
   Default string values look like \"b'0'\"; strip the b' prefix and trailing '.
   Matches CL bits-to-hex-bitstring."
  [^String v]
  (when v
    (let [trimmed (str/trim v)]
      (cond
        ;; Default value from DDL: b'0' → 0
        (and (str/starts-with? trimmed "b'") (str/ends-with? trimmed "'"))
        (subs trimmed 2 (dec (count trimmed)))
        ;; Already an X-hex string from our byte encoding: pass through
        :else trimmed))))

(def registry
  {:zero-dates-to-null           zero-dates-to-null
   :tinyint-to-boolean           tinyint-to-boolean
   :tinyint-to-integer           tinyint-to-integer
   :year-to-integer              year-to-integer
   :empty-string-to-null         empty-string-to-null
   :right-trim                   right-trim
   :remove-null-characters       remove-null-characters
   :int-to-ip                    int-to-ip
   :bytes-to-pg-bytea            bytes-to-pg-bytea
   :sqlite-timestamp-to-timestamp sqlite-timestamp-to-timestamp
   :base64-decode                base64-decode
   :byte-vector-to-hexstring     byte-vector-to-hexstring
   :bits-to-boolean              bits-to-boolean
   :bits-to-hex-bitstring        bits-to-hex-bitstring
   :numeric-to-integer           numeric-to-integer
   :int-to-uuid                  int-to-uuid
   :set-to-enum-array            set-to-enum-array
   :convert-mysql-point          convert-mysql-point
   :convert-mysql-linestring     convert-mysql-linestring
   :hex-to-bytea                 hex-to-bytea
   :none                         identity})

;; ── Rule matching ─────────────────────────────────────────────────

(defn- matches-rule?
  "Return truthy if cast-rule applies to column col."
  [rule col]
  (let [{:keys [source]} rule
        col-type  (str/lower-case (or (:column-type col) ""))
        col-name  (:column-name col)
        col-extra (str/lower-case (or (:extra col) ""))
        col-def   (or (:column-default col) "")]
    (cond
      (= :column (:type source))
      (and (= (str/lower-case col-name)
              (str/lower-case (or (:column source) "")))
           ;; If the rule specifies a table qualifier, it must match
           (or (nil? (:table source))
               (= (str/lower-case (or (:table-name col) ""))
                  (str/lower-case (:table source)))))

      (= :type (:type source))
      (let [src-type   (str/lower-case (or (:name source) ""))
            col-is-ai  (str/includes? col-extra "auto_increment")]
        (and
         (str/starts-with? col-type src-type)
         ;; Mirror CL: a rule without when-extra never matches auto-increment
         ;; columns (CL requires auto-increment to agree between rule and col).
         (if (:when-extra rule)
           (str/includes? col-extra (str/lower-case (:when-extra rule)))
           (not col-is-ai))
         (or (nil? (:when-default rule))
             (= col-def (:when-default rule)))
         (or (not (:when-unsigned rule))
             (str/includes? col-type "unsigned"))))

      :else false)))

;; ── Default per-type casts (no user rule needed) ─────────────────

(defn- type-default-cast
  "Return a default cast function keyword for a MySQL type,
   or nil if no default applies.
   Mirrors the built-in cast rules from CL mysql-cast-rules.lisp."
  [^String col-type]
  (let [lower (str/lower-case col-type)]
    (cond
      ;; tinyint(1) → boolean (must check before plain tinyint)
      (re-find #"^tinyint\(1\)" lower)     :tinyint-to-boolean
      (re-find #"^tinyint" lower)          :tinyint-to-integer
      (re-find #"^year" lower)             :year-to-integer
      ;; bit(1) → boolean; bit(X) → hex bitstring
      (re-find #"^bit\(1\)" lower)         :bits-to-boolean
      (re-find #"^bit" lower)              :bits-to-hex-bitstring
      ;; MySQL SET columns → PostgreSQL array format {val1,val2}
      (re-find #"^set\(" lower)              :set-to-enum-array
      ;; text types: strip embedded NUL characters (CL default rule)
      (re-find #"^(tinytext|mediumtext|longtext|text|varchar|char)" lower)
      :remove-null-characters
      :else nil)))

;; ── Public API ────────────────────────────────────────────────────

(defn- implicit-using
  "When a cast rule has no explicit :using, pick a sensible default based
   on the rule's target type and the column's source type.
   Without this, 'type tinyint to boolean' (no using) would send raw
   integers to a boolean column and PostgreSQL would reject them."
  [rule col]
  (let [target (some-> (:target-type rule) str/lower-case str/trim)]
    (cond
      ;; boolean target: any integer source needs 0→f / else→t conversion
      (= target "boolean")
      (let [src (str/lower-case (or (:column-type col) ""))]
        (if (str/starts-with? src "bit")
          :bits-to-boolean
          :tinyint-to-boolean))

      ;; integer/bigint target from numeric source: truncate decimal digits
      (and (some #{target} ["integer" "bigint" "smallint" "int"])
           (str/starts-with? (str/lower-case (or (:column-type col) "")) "numeric"))
      :numeric-to-integer

      :else :none)))

(defn resolve-specs
  "Given cast rules and a column list, return a vector of cast-function
   keywords (or nil) indexed by column position.
   First matching user rule wins; then type-default cast; else nil.
   Uses :source-column-type (original MySQL type) when present so that
   matching still works after apply-type-overrides has changed :column-type
   to the PostgreSQL target type."
  [cast-rules columns]
  (let [rules (vec cast-rules)]
    (mapv (fn [col]
            (let [col-for-match (if (:source-column-type col)
                                  (assoc col :column-type (:source-column-type col))
                                  col)
                  src-type (or (:source-column-type col) (:column-type col) "text")]
              (or (some (fn [rule]
                          (when (matches-rule? rule col-for-match)
                            (or (:using rule) (implicit-using rule col-for-match))))
                        rules)
                  (type-default-cast src-type))))
          columns)))

(defn apply-type-overrides
  "Return an updated columns vector with target types and options
   from matching cast rules applied.
   Stores :source-column-type (original MySQL type) before overriding, so
   resolve-specs can still match on the original type after DDL generation
   changes :column-type to the PostgreSQL target.
   Also stores :cast-fn so the DDL layer can apply the same transform to
   default values (mirrors CL format-default-value).
   Rules with no :target-type (column cast without a 'to TYPE' clause) match
   the column and apply the cast function without changing its type."
  [columns cast-rules]
  (mapv (fn [col]
          (if-let [rule (some #(when (matches-rule? % col) %) cast-rules)]
            (let [cast-kw (or (:using rule) (implicit-using rule col))]
              (cond-> col
                ;; preserve original so resolve-specs can match on it
                :always
                (assoc :source-column-type (:column-type col))

                cast-kw
                (assoc :cast-fn cast-kw)

                ;; only override the column type when the rule specifies one
                (:target-type rule)
                (assoc :column-type (:target-type rule))

                (and (get-in rule [:options :drop-typemod])
                     (:target-type rule))
                (assoc :column-type
                       (-> (:target-type rule)
                           (or (:column-type col))
                           (str/replace #"\(.*\)" "")))

                (get-in rule [:options :drop-not-null])
                (assoc :is-nullable true)

                (get-in rule [:options :drop-default])
                (assoc :column-default nil)

                (get-in rule [:options :drop-extra])
                (assoc :extra nil)))
            col))
        columns))

(defn apply-cast
  "Apply a cast to a value. cast-spec may be:
   - a keyword  — looked up in the registry
   - an IFn     — called directly (compiled 'using' expression)
   - nil/:none  — identity"
  [cast-spec ^String v]
  (cond
    (keyword? cast-spec)
    (if-let [f (get registry cast-spec)]
      (f v)
      v)
    (fn? cast-spec)  (cast-spec v)
    (ifn? cast-spec) (cast-spec v)
    :else v))
