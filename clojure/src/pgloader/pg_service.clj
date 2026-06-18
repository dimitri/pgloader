(ns pgloader.pg-service
  "Parse PostgreSQL service files (pg_service.conf / PGSERVICEFILE) and
   ~/.pgpass password files. Used to resolve connection URIs."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]))

(defn- service-file-path
  "Return the path to the pg_service.conf file, respecting PGSERVICEFILE env var."
  []
  (or (System/getenv "PGSERVICEFILE")
      (str (System/getProperty "user.home") "/.pg_service.conf")))

(defn- parse-service-file
  "Parse an INI-style pg_service.conf into a map of service-name → param-map.
   Returns empty map if file does not exist."
  [path]
  (let [f (io/file path)]
    (if (.exists f)
      (with-open [rdr (io/reader f)]
        (:services
         (reduce (fn [{:keys [services current]} line]
                   (let [trimmed (str/trim line)]
                     (cond
                       (or (str/blank? trimmed) (str/starts-with? trimmed "#"))
                       {:services services :current current}

                       (and (str/starts-with? trimmed "[") (str/ends-with? trimmed "]"))
                       (let [svc-name (subs trimmed 1 (dec (count trimmed)))]
                         {:services (assoc services svc-name {}) :current svc-name})

                       (and current (str/includes? trimmed "="))
                       (let [[k v] (str/split trimmed #"=" 2)]
                         {:services (assoc-in services [current (str/trim k)] (str/trim v))
                          :current current})

                       :else {:services services :current current})))
                 {:services {} :current nil}
                 (line-seq rdr))))
      {})))

(defn lookup
  "Look up a named service in the pg_service.conf file.
   Returns a map with :host :port :db :user :password, or nil if not found."
  [service-name]
  (let [services (parse-service-file (service-file-path))
        params   (get services service-name)]
    (when params
      {:host     (get params "host"     "localhost")
       :port     (Integer/parseInt (get params "port" "5432"))
       :db       (get params "dbname"   (get params "database" ""))
       :user     (get params "user"     nil)
       :password (get params "password" nil)})))

;; ── .pgpass support ───────────────────────────────────────────────────────────

(defn pgpass-file-path
  "Return the path to the .pgpass file, respecting PGPASSFILE env var."
  []
  (or (System/getenv "PGPASSFILE")
      (str (System/getProperty "user.home") "/.pgpass")))

(defn- pgpass-field-matches?
  [pattern value]
  (or (= pattern "*") (= pattern value)))

(defn pgpass-lookup
  "Look up a password in ~/.pgpass (or $PGPASSFILE) for the given connection params.
   Returns the password string or nil if not found."
  [host port db user]
  (let [f (io/file (pgpass-file-path))]
    (when (.exists f)
      (with-open [rdr (io/reader f)]
        (some (fn [line]
                (let [line (str/trim line)]
                  (when-not (or (str/blank? line) (str/starts-with? line "#"))
                    (let [parts (str/split line #"(?<!\\):" 5)]
                      (when (= 5 (count parts))
                        (let [[h p d u pw] parts]
                          (when (and (pgpass-field-matches? h host)
                                     (pgpass-field-matches? p (str port))
                                     (pgpass-field-matches? d db)
                                     (pgpass-field-matches? u (or user "")))
                            pw)))))))
              (line-seq rdr))))))

(defn apply-pgpass
  "If uri-map has no :password, look it up in ~/.pgpass.
   Returns uri-map with :password filled in if found."
  [uri-map]
  (if (:password uri-map)
    uri-map
    (let [pw (pgpass-lookup
               (or (:host uri-map) "localhost")
               (or (:port uri-map) 5432)
               (or (:db uri-map) "")
               (:user uri-map))]
      (if pw (assoc uri-map :password pw) uri-map))))
