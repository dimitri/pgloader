(ns pgloader.mysql-options
  "Parse MySQL option files (~/.my.cnf, /etc/my.cnf, /etc/mysql/my.cnf).

   Implements the section-lookup convention described in the MySQL manual:
   https://dev.mysql.com/doc/refman/8.0/en/option-files.html

   [client]   — read by all MySQL client programs; supplies the base values.
   [pgloader] — pgloader-specific overrides; takes precedence over [client].

   Only connection-relevant keys are extracted: host, port, user, password,
   database.  Boolean options, socket, and !include/!includedir directives
   are intentionally ignored."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]))

(def ^:private search-paths
  "Option files read in order; later files override earlier ones."
  ["/etc/my.cnf"
   "/etc/mysql/my.cnf"])

(defn- home-cnf []
  (str (System/getProperty "user.home") "/.my.cnf"))

(defn- strip-quotes [^String s]
  (if (and (> (count s) 1)
           (or (and (str/starts-with? s "\"") (str/ends-with? s "\""))
               (and (str/starts-with? s "'")  (str/ends-with? s "'"))))
    (subs s 1 (dec (count s)))
    s))

(defn- parse-file
  "Parse a single option file. Returns a map of section-name → {key → value}.
   Lines beginning with # or ; are comments.  Unknown sections are silently
   collected (the caller decides which sections are relevant)."
  [^java.io.File f]
  (with-open [rdr (io/reader f)]
    (:sections
     (reduce
      (fn [{:keys [sections current]} raw-line]
        (let [line (str/trim raw-line)]
          (cond
            (or (str/blank? line)
                (str/starts-with? line "#")
                (str/starts-with? line ";")
                (str/starts-with? line "!"))
            {:sections sections :current current}

            (and (str/starts-with? line "[") (str/ends-with? line "]"))
            (let [sec (subs line 1 (dec (count line)))]
              {:sections (update sections sec #(or % {})) :current sec})

            (and current (str/includes? line "="))
            (let [[k v] (str/split line #"=" 2)]
              {:sections (assoc-in sections [current (str/trim k)]
                                   (strip-quotes (str/trim v)))
               :current current})

            :else {:sections sections :current current})))
      {:sections {} :current nil}
      (line-seq rdr)))))

(defn- merge-sections
  "Merge all option files: later files override earlier keys within the same
   section."
  [paths]
  (reduce
   (fn [acc path]
     (let [f (io/file path)]
       (if (.exists f)
         (merge-with merge acc (parse-file f))
         acc)))
   {}
   paths))

(defn- extract-conn-keys
  "Pull connection-relevant keys out of a section map."
  [section-map]
  (cond-> {}
    (get section-map "host")     (assoc :host (get section-map "host"))
    (get section-map "port")     (assoc :port (Integer/parseInt (get section-map "port")))
    (get section-map "user")     (assoc :user (get section-map "user"))
    (get section-map "password") (assoc :password (get section-map "password"))
    (get section-map "database") (assoc :db (get section-map "database"))))

(defn read-my-cnf
  "Read MySQL option files and return a merged connection-param map.
   Applies [client] first, then [pgloader] on top (higher priority).
   Returns an empty map when no option files exist."
  []
  (let [all-paths (conj (vec search-paths) (home-cnf))
        sections  (merge-sections all-paths)
        client    (extract-conn-keys (get sections "client" {}))
        pgloader  (extract-conn-keys (get sections "pgloader" {}))]
    (merge client pgloader)))

(defn apply-my-cnf
  "Fill in nil credential slots in URI-MAP from MySQL option files.
   Only replaces keys that are nil in the URI map; never overrides an
   explicit value supplied in the connection URI."
  [uri-map]
  (let [opts (read-my-cnf)]
    (reduce (fn [m [k v]]
              (if (nil? (get m k))
                (assoc m k v)
                m))
            uri-map
            opts)))
