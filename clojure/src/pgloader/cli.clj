(ns pgloader.cli
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pgloader.load-file.parser :as parser]
            [pgloader.load-file.ast :as ast]
            [pgloader.core :as core]
            [pgloader.copy :as copy]
            [pgloader.regress :as regress]
            [pgloader.stats :as stats])
  (:import [java.net URI]
           [java.nio.charset Charset]
           [org.slf4j LoggerFactory]
           [ch.qos.logback.classic Level Logger]
           [ch.qos.logback.classic.encoder PatternLayoutEncoder]
           [ch.qos.logback.core FileAppender]
           [ch.qos.logback.core.spi AppenderAttachable])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn- set-log-level!
  [^String level-str]
  (let [^Logger root (.getLogger (LoggerFactory/getILoggerFactory) "ROOT")
        level (Level/valueOf level-str)]
    (.setLevel root level)))

(defn- set-appender-level!
  "Set the threshold level on a named appender attached to the root logger."
  [^String appender-name ^String level-str]
  (let [^Logger root (.getLogger (LoggerFactory/getILoggerFactory) "ROOT")
        app          (.getAppender ^AppenderAttachable root appender-name)]
    (when app
      (let [filter (doto (ch.qos.logback.classic.filter.ThresholdFilter.)
                     (.setLevel level-str)
                     (.start))]
        (.clearAllFilters app)
        (.addFilter app filter)))))

(defn- add-file-appender!
  "Add a plain FileAppender writing to path, attached to the root logger."
  [^String path]
  (let [ctx     (LoggerFactory/getILoggerFactory)
        ^Logger root (.getLogger ctx "ROOT")
        enc     (doto (PatternLayoutEncoder.)
                  (.setContext ctx)
                  (.setPattern "%d{HH:mm:ss.SSS} %-5level [%thread] %logger{20} - %msg%n")
                  (.start))
        app     (doto (FileAppender.)
                  (.setContext ctx)
                  (.setFile path)
                  (.setAppend false)
                  (.setEncoder enc)
                  (.start))]
    (.addAppender root app)))

(defn- configure-logging
  "Configure logging based on CLI options."
  [opts]
  (cond
    (:debug opts)   (set-log-level! "TRACE")
    (:verbose opts) (set-log-level! "DEBUG")
    (:quiet opts)   (set-log-level! "ERROR")
    :else           (set-log-level! "INFO"))
  (when-let [f (:logfile opts)]
    (add-file-appender! f))
  ;; Per-destination log thresholds (override root level for a specific appender).
  ;; --client-min-messages controls the CONSOLE appender.
  ;; --log-min-messages    controls the FILE appender (logback.xml "FILE" appender).
  (when-let [lvl (:client-min-messages opts)]
    (set-appender-level! "CONSOLE" (str/upper-case lvl)))
  (when-let [lvl (:log-min-messages opts)]
    (set-appender-level! "FILE" (str/upper-case lvl))))

(defn- print-usage
  "Print help text and exit."
  []
  (println "pgloader v4")
  (println)
  (println "Usage:")
  (println "  pgloader [options] <load-file>")
  (println "  pgloader [options] <source-uri> <target-uri>")
  (println)
  (println "Options:")
  (println "  --help                      Show this help")
  (println "  --version                   Show version")
  (println "  --list-encodings            List all supported character encodings and exit")
  (println "  --verbose                   Enable verbose (DEBUG) logging")
  (println "  --debug                     Enable debug (TRACE) logging and extended summary")
  (println "  --quiet                     Minimal output (ERROR level only)")
  (println "  --client-min-messages LVL   Minimum log level for console (trace/debug/info/warn/error)")
  (println "  --log-min-messages LVL      Minimum log level for logfile (trace/debug/info/warn/error)")
  (println "  --root-dir DIR              Root directory for reject files (default: /tmp/pgloader)")
  (println "  --logfile FILE              Write log output to FILE in addition to stdout")
  (println "  -S, --summary FILE          Write summary to JSON or CSV file")
  (println "  --on-error-stop             Stop on first copy error instead of continuing")
  (println "  --dry-run                   Connect and plan but do not copy any data")
  (println "  --type TYPE                 Force source type (csv, mysql, pgsql, mssql, sqlite, dbf)")
  (println "  --encoding ENC              Source file encoding (e.g. latin-1)")
  (println "  --with OPTION               Add a WITH option (may be repeated)")
  (println "  --set VAR TO VAL            Add a SET parameter (may be repeated)")
  (println "  --cast CAST                 Add a CAST rule (may be repeated)")
  (println "  --field FIELD               Add a FIELD definition (may be repeated)")
  (println "  --before FILE               SQL file to execute before load")
  (println "  --after FILE                SQL file to execute after load")
  (println)
  (println "Deprecated / removed options (not available in v4):")
  (println "  --upgrade-config            (v2 INI → v3 .load converter; INI files removed in v4)")
  (println "  --self-upgrade              (Lisp source hot-reload; not applicable to JAR distribution)")
  (println "  --load-lisp-file / -l       (Lisp extension files; contact maintainers if needed)")
  (println "  --context / -C              (INI variable file; use environment variables instead)")
  (println "  --no-ssl-cert-verification  (use ?sslmode=disable in the connection URI instead)")
  (println)
  (println "Sources:")
  (println "  FILE.load          load file (pgloader DSL)")
  (println "  FILE.csv           CSV file (requires --type csv or .csv suffix)")
  (println "  mysql://...        MySQL / MariaDB database")
  (println "  postgresql://...   PostgreSQL database (source)")
  (println "  mssql://...        MS SQL Server database")
  (println "  sqlite://...       SQLite database")
  (println "  jdbc:mysql://...   JDBC URL (all driver-specific params passed through)")
  (println "  jdbc:sqlserver://... JDBC URL for MS SQL Server")
  (println "  jdbc:postgresql://... JDBC URL for PostgreSQL")
  (println)
  (println "Examples:")
  (println "  pgloader myfile.load")
  (println "  pgloader sqlite:///app.db postgresql:///target")
  (println "  pgloader --on-error-stop mysql://user@localhost/mydb postgresql:///target")
  (println "  pgloader --dry-run --with 'create tables' mysql://user@localhost/mydb postgresql:///target")
  (println "  pgloader --client-min-messages warn --logfile /tmp/load.log myfile.load")
  (System/exit 0))

(defn- print-version
  []
  (println "pgloader v4.0.0")
  (System/exit 0))

(defn- print-encodings
  []
  (doseq [name (sort (keys (Charset/availableCharsets)))]
    (println name))
  (System/exit 0))

(defrecord CLIOptions
           [load-files           ; vector of .load file paths
            load-file            ; single load file (kept for compat)
            source-uri
            target-uri
            verbose
            debug
            quiet
            root-dir
            summary
            on-error-stop        ; --on-error-stop
            dry-run              ; --dry-run
            client-min-messages  ; --client-min-messages
            log-min-messages     ; --log-min-messages
   ;; URI-pair mode overrides
            source-type          ; --type
            encoding             ; --encoding
            with-opts            ; --with (vector of strings)
            set-params           ; --set (vector of [var val] pairs)
            cast-rules           ; --cast (vector of strings)
            field-defs           ; --field (vector of strings)
            before-file          ; --before
            after-file           ; --after
            logfile              ; --logfile
            ])

(defn- deprecated-flag!
  "Warn and exit when the user passes a flag that was removed in v4."
  [flag suggestion]
  (println (str "Error: " flag " is not supported in pgloader v4."))
  (when suggestion (println suggestion))
  (System/exit 1))

(defn parse-args
  "Parse command-line arguments. Returns CLIOptions."
  [args]
  (loop [opts (->CLIOptions [] nil nil nil false false false nil nil false false nil nil nil nil [] [] [] [] nil nil nil)
         remaining args]
    (if-let [arg (first remaining)]
      (case arg
        "--help"                  (print-usage)
        "--version"               (print-version)
        "--list-encodings"        (print-encodings)
        "--verbose"               (recur (assoc opts :verbose true) (rest remaining))
        "--debug"                 (recur (assoc opts :debug true) (rest remaining))
        "--quiet"                 (recur (assoc opts :quiet true) (rest remaining))
        "--on-error-stop"         (recur (assoc opts :on-error-stop true) (rest remaining))
        "--dry-run"               (recur (assoc opts :dry-run true) (rest remaining))
        "--root-dir"              (recur (assoc opts :root-dir (second remaining))
                                         (drop 2 remaining))
        "--summary"               (recur (assoc opts :summary (second remaining))
                                         (drop 2 remaining))
        "-S"                      (recur (assoc opts :summary (second remaining))
                                         (drop 2 remaining))
        "--logfile"               (recur (assoc opts :logfile (second remaining))
                                         (drop 2 remaining))
        "--client-min-messages"   (recur (assoc opts :client-min-messages (second remaining))
                                         (drop 2 remaining))
        "--log-min-messages"      (recur (assoc opts :log-min-messages (second remaining))
                                         (drop 2 remaining))
        "--type"                  (recur (assoc opts :source-type (second remaining))
                                         (drop 2 remaining))
        "--encoding"              (recur (assoc opts :encoding (second remaining))
                                         (drop 2 remaining))
        "--with"                  (recur (update opts :with-opts conj (second remaining))
                                         (drop 2 remaining))
        "--cast"                  (recur (update opts :cast-rules conj (second remaining))
                                         (drop 2 remaining))
        "--field"                 (recur (update opts :field-defs conj (second remaining))
                                         (drop 2 remaining))
        "--before"                (recur (assoc opts :before-file (second remaining))
                                         (drop 2 remaining))
        "--after"                 (recur (assoc opts :after-file (second remaining))
                                         (drop 2 remaining))
        "--set"                   (let [var-name (second remaining)
                                        val      (nth remaining 3 nil)
                                        rest-r   (drop 4 remaining)]
                                    (recur (update opts :set-params conj [var-name val]) rest-r))
        ;; Deprecated flags — exit with a clear message rather than silently ignoring.
        "--upgrade-config"        (deprecated-flag! "--upgrade-config"
                                                    "v4 does not support INI config files. Use .load files instead.")
        "--self-upgrade"          (deprecated-flag! "--self-upgrade"
                                                    "v4 is distributed as a self-contained JAR; use package/container updates.")
        ("--load-lisp-file" "-l") (deprecated-flag! "--load-lisp-file"
                                                    "Lisp extension files are not supported in v4. Contact the maintainers if you need runtime transform extensions.")
        ("--context" "-C")        (deprecated-flag! "--context"
                                                    "Use environment variables for configuration in v4: {{VAR}} in load files expands $VAR.")
        "--no-ssl-cert-verification" (deprecated-flag! "--no-ssl-cert-verification"
                                                       "Use ?sslmode=disable in the connection URI instead, e.g. postgresql://host/db?sslmode=disable")
        ;; positional
        (if (str/ends-with? arg ".load")
          (recur (update opts :load-files conj arg) (rest remaining))
          (if (nil? (:source-uri opts))
            (recur (assoc opts :source-uri arg) (rest remaining))
            (if (nil? (:target-uri opts))
              (recur (assoc opts :target-uri arg) (rest remaining))
              (do (println "Unknown arg:" arg)
                  (System/exit 1))))))
      opts)))

(defn- build-inline-command
  "Build a LoadCommand from source/target URIs and CLI override options.
   Parses a synthetic .load file snippet so that --with/--cast/--field
   strings go through the same grammar as regular .load files."
  [source-uri-str target-uri-str opts]
  (let [;; Build a minimal LOAD DATABASE command string with any CLI overrides
        source-type  (:source-type opts)
        effective-source (if (and source-type
                                  (not (str/includes? source-uri-str "://")))
                           (str source-type "://" source-uri-str)
                           source-uri-str)
        with-clauses (:with-opts opts)
        cast-clauses (:cast-rules opts)
        set-clauses  (:set-params opts)
        before-file  (:before-file opts)
        after-file   (:after-file opts)
        ;; Detect source kind to pick the right LOAD keyword
        source-kind  (cond
                       (= source-type "csv")                                            "CSV"
                       (= source-type "copy")                                           "COPY"
                       (= source-type "fixed")                                          "FIXED"
                       (str/starts-with? (str/lower-case effective-source) "mysql")    "DATABASE"
                       (str/starts-with? (str/lower-case effective-source) "mariadb")  "DATABASE"
                       (str/starts-with? (str/lower-case effective-source) "mssql")    "DATABASE"
                       (str/starts-with? (str/lower-case effective-source) "pgsql")    "DATABASE"
                       (str/starts-with? (str/lower-case effective-source) "postgres") "DATABASE"
                       (str/starts-with? (str/lower-case effective-source) "sqlite")   "DATABASE"
                       :else "DATABASE")
        file-source? (contains? #{"CSV" "COPY" "FIXED"} source-kind)
        ;; Build the WITH ENCODING clause (file sources only)
        encoding-str (when (and file-source? (:encoding opts))
                       (str "  WITH ENCODING '" (:encoding opts) "'"))
        ;; Build HAVING FIELDS clause (file sources only)
        fields-str  (when (and file-source? (seq (:field-defs opts)))
                      (str "HAVING FIELDS (" (str/join ", " (:field-defs opts)) ")"))
        ;; Build the WITH clause
        with-str (when (seq with-clauses)
                   (str "WITH " (str/join ", " with-clauses)))
        ;; Build the CAST clause
        cast-str (when (seq cast-clauses)
                   (str "CAST " (str/join ", " cast-clauses)))
        ;; Build the SET clause
        set-str (when (seq set-clauses)
                  (str "SET " (str/join ", "
                                        (map (fn [[k v]] (str k " to '" v "'")) set-clauses))))
        ;; Build BEFORE / AFTER LOAD DO $$ ... $$
        before-str (when before-file
                     (try
                       (str "BEFORE LOAD DO $$ "
                            (slurp before-file) " $$")
                       (catch Exception e
                         (log/error (str "Cannot read --before file: " (.getMessage e)))
                         nil)))
        after-str (when after-file
                    (try
                      (str "AFTER LOAD DO $$ "
                           (slurp after-file) " $$")
                      (catch Exception e
                        (log/error (str "Cannot read --after file: " (.getMessage e)))
                        nil)))
        ;; Assemble the synthetic load file
        load-str (str/join "\n"
                           (filter some?
                                   [(str "LOAD " source-kind)
                                    (str "  FROM " effective-source)
                                    encoding-str
                                    fields-str
                                    (str "  INTO " target-uri-str)
                                    with-str
                                    cast-str
                                    set-str
                                    before-str
                                    after-str
                                    ";"]))
        result (parser/parse-string load-str)]
    (if (:error result)
      (do (println "Error building inline command:" (:error result))
          (System/exit 1))
      (:ok result))))

(defn run
  "Entry point from the command line."
  [args]
  (if (= (first args) "regress")
    (regress/run (rest args))
    (let [opts (parse-args args)]
      (configure-logging opts)
      (binding [copy/*root-dir*        (or (:root-dir opts) copy/*root-dir*)
                copy/*on-error-stop*   (:on-error-stop opts)
                copy/*dry-run*         (:dry-run opts)]
        (when (:dry-run opts)
          (log/info "DRY RUN — no data will be copied"))
        (if (seq (:load-files opts))
          (do
          ;; Warn if URI-mode-only flags are provided with .load files
            (when (or (seq (:with-opts opts)) (seq (:cast-rules opts))
                      (:before-file opts) (:after-file opts))
              (log/warn "--with, --cast, --before, --after are ignored when using a load file"))
            (doseq [f (:load-files opts)]
              (let [_ (log/info (str "Parsing commands from file " f))
                    result (parser/parse-file f)]
                (if (:error result)
                  (do (println "Error:" (:error result))
                      (System/exit 1))
                  (core/run-command (:ok result) opts)))))
          (if (and (:source-uri opts) (:target-uri opts))
            (let [cmd (build-inline-command (:source-uri opts) (:target-uri opts) opts)]
              (core/run-command cmd opts))
            (do (println "No load file or source/target specified")
                (print-usage))))
        ;; Exit with code 1 when a fatal load error occurred (#634).
        ;; Row-level rejections (type errors) do not count as fatal.
        (when (stats/fatal-error?)
          (System/exit 1))))))

(defn -main
  "Main entry point for the JAR."
  [& args]
  (run args))
