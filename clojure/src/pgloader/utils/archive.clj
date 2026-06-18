(ns pgloader.utils.archive
  "HTTP fetch, ZIP/GZ expansion, and filename matching utilities.
   All archive operations use pure Java stdlib — no shell subprocesses."
  (:require [clojure.java.io :as io]
            [clojure.string  :as str]
            [clojure.tools.logging :as log])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]
           [java.io File FileOutputStream InputStream]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.util.zip ZipInputStream GZIPInputStream]))

(set! *warn-on-reflection* true)

;; ── HTTP fetch ────────────────────────────────────────────────────────────────

(defn http-fetch!
  "Download URL to a temporary file. Follows redirects. Returns java.io.File.
   The temp file is registered for deletion on JVM exit."
  ^File [^String url]
  (log/info (str "Fetching " url))
  (let [client (-> (HttpClient/newBuilder)
                   (.followRedirects java.net.http.HttpClient$Redirect/NORMAL)
                   .build)
        req    (-> (HttpRequest/newBuilder)
                   (.uri (URI/create url))
                   .GET
                   .build)
        resp   (.send client req (HttpResponse$BodyHandlers/ofInputStream))
        status (.statusCode resp)]
    (when-not (= 200 status)
      (.close ^InputStream (.body resp))
      (throw (ex-info (str "HTTP " status " fetching " url) {:url url :status status})))
    ;; Preserve original filename so archive-type detection works.
    (let [url-path  (.getPath (URI/create url))
          orig-name (last (str/split url-path #"/"))
          [base ext] (if (str/includes? orig-name ".")
                       (let [dot (.lastIndexOf ^String orig-name ".")]
                         [(subs orig-name 0 dot) (subs orig-name dot)])
                       [orig-name ""])
          tmp       (File/createTempFile (str "pgloader-" base "-") ext)]
      (.deleteOnExit tmp)
      (with-open [in  ^InputStream (.body resp)
                  out (FileOutputStream. tmp)]
        (.transferTo in out))
      (log/info (str "Fetched " (.length tmp) " bytes -> " tmp))
      tmp)))

;; ── Temp dir helpers ─────────────────────────────────────────────────────────

(defn- make-temp-dir! ^File [prefix]
  (-> (Files/createTempDirectory prefix (make-array FileAttribute 0))
      .toFile))

(defn delete-tree!
  "Recursively delete dir and all its contents. Silent no-op if dir does not exist."
  [^File dir]
  (when (and dir (.exists dir))
    (doseq [^File f (reverse (file-seq dir))]
      (.delete f))))

;; ── Archive expansion ─────────────────────────────────────────────────────────

(defn archive-type
  "Return :zip, :gz or nil based on file extension (case-insensitive)."
  [^File f]
  (let [n (str/lower-case (.getName f))]
    (cond
      (str/ends-with? n ".zip") :zip
      (str/ends-with? n ".gz")  :gz
      :else nil)))

(defn expand-zip!
  "Extract all entries from zip-file to a fresh temp directory. Returns the temp dir."
  ^File [^File zip-file]
  (let [tmp-dir (make-temp-dir! "pgloader-zip-")]
    (log/info (str "Extracting " (.getName zip-file) " -> " tmp-dir))
    (with-open [zis (ZipInputStream. (io/input-stream zip-file))]
      (loop []
        (when-let [entry (.getNextEntry zis)]
          (when-not (.isDirectory entry)
            (let [name     (.getName entry)
                  out-file (io/file tmp-dir name)]
              (io/make-parents out-file)
              (with-open [fos (FileOutputStream. out-file)]
                (.transferTo zis fos))
              (log/debug (str "  extracted: " name " (" (.length out-file) " B)"))))
          (.closeEntry zis)
          (recur))))
    tmp-dir))

(defn expand-gz!
  "Decompress a .gz file to a fresh temp directory. Returns the temp dir."
  ^File [^File gz-file]
  (let [tmp-dir  (make-temp-dir! "pgloader-gz-")
        out-name (str/replace (.getName gz-file) #"(?i)\.gz$" "")
        out-file (io/file tmp-dir out-name)]
    (log/info (str "Decompressing " (.getName gz-file) " -> " tmp-dir))
    (with-open [in  (GZIPInputStream. (io/input-stream gz-file))
                out (FileOutputStream. out-file)]
      (.transferTo in out))
    (log/debug (str "  decompressed: " out-name " (" (.length out-file) " B)"))
    tmp-dir))

(defn expand!
  "Expand an archive file to a fresh temp directory. Returns the temp dir."
  ^File [^File archive-file]
  (case (archive-type archive-file)
    :zip (expand-zip! archive-file)
    :gz  (expand-gz!  archive-file)
    (throw (ex-info (str "Unsupported archive type: " (.getName archive-file))
                    {:file (.getAbsolutePath archive-file)}))))

;; ── Filename matching ─────────────────────────────────────────────────────────

(defn matching-files
  "Return all regular files under dir whose name matches regex-pattern (string).
   Results are sorted by absolute path for deterministic ordering."
  [^File dir ^String regex-pattern]
  (let [re (re-pattern regex-pattern)]
    (->> (file-seq dir)
         (filter #(and (.isFile ^File %) (re-find re (.getName ^File %))))
         (sort-by #(.getAbsolutePath ^File %)))))
