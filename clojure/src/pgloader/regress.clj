(ns pgloader.regress
  "pg_regress-style test driver.
   For each sql/<N>-<name>.sql in the suite directory, runs psql and writes
   output to out/<N>-<name>.out, then diffs against expected/<N>-<name>.out.
   psql reads standard PG env vars (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD).

   Usage:
     java -jar pgloader.jar regress [--update] <suite-dir>

   --update  write out/ → expected/ instead of diffing (baseline registration)"
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str])
  (:import [java.io File]))

(set! *warn-on-reflection* true)

(defn- sql-files
  "Sorted list of *.sql files inside <suite-dir>/sql/."
  [^File suite-dir]
  (let [d (io/file suite-dir "sql")]
    (when (.isDirectory d)
      (->> (.listFiles d)
           (filter #(str/ends-with? (.getName ^File %) ".sql"))
           sort))))

(defn- stem [^File f]
  (str/replace (.getName ^File f) #"\.sql$" ""))

(defn- run-psql
  "Execute psql -f sql-file, capturing combined stdout+stderr into out-file.
   Returns the psql process exit code."
  [^File sql-file ^File out-file]
  (let [args ^"[Ljava.lang.String;" (into-array String
              ["psql" "-X" "-P" "pager=off"
               "-v" "ON_ERROR_STOP=1"
               "-f" (.getAbsolutePath sql-file)])]
    (-> (doto (ProcessBuilder. args)
          (.redirectErrorStream true)
          (.redirectOutput out-file))
        .start
        .waitFor)))

(defn- diff-files
  "Unified diff of expected vs actual. Returns true when identical."
  [^File exp-file ^File out-file test-name]
  (cond
    (not (.exists exp-file))
    (do (println (str "  MISSING  " test-name
                      "  (run `make update-expected` to register baseline)"))
        false)

    :else
    (let [{:keys [exit out]} (shell/sh "diff" "-u"
                                       (.getAbsolutePath exp-file)
                                       (.getAbsolutePath out-file))]
      (if (zero? exit)
        (do (println (str "  ok       " test-name)) true)
        (do (println (str "  FAIL     " test-name))
            (print out)
            false)))))

(defn run
  "Entry point called from cli/run when first arg is 'regress'."
  [args]
  (let [update?   (boolean (some #{"--update"} args))
        excludes  (->> args
                       (partition-all 2 1)
                       (keep (fn [[a b]] (when (= "--update" a) nil)
                               (when (= "--exclude" a) b)))
                       (remove nil?)
                       set)
        plain     (remove #(or (str/starts-with? % "--")
                               (contains? excludes %)) args)
        dir-arg   (first plain)
        suite-dir (io/file (or dir-arg "."))
        exp-dir   (io/file suite-dir "expected")
        out-dir   (io/file suite-dir "out")]
    (when update? (.mkdirs exp-dir))
    (.mkdirs out-dir)
    (let [files (cond->> (sql-files suite-dir)
                  (seq excludes) (remove #(excludes (stem %))))]
      (when (empty? files)
        (println (str "No SQL files found under " (str suite-dir) "/sql/"))
        (System/exit 1))
      (let [results
            (mapv
              (fn [^File sql-file]
                (let [s        (stem sql-file)
                      out-file (io/file out-dir (str s ".out"))
                      exp-file (io/file exp-dir (str s ".out"))]
                  (let [exit (run-psql sql-file out-file)]
                    (if (pos? exit)
                      (do (println (str "  ERROR    " s
                                        "  (psql exited " exit ")"))
                          (when (.exists out-file) (print (slurp out-file)))
                          false)
                      (if update?
                        (do (io/copy out-file exp-file)
                            (println (str "  updated  " s))
                            true)
                        (diff-files exp-file out-file s))))))
              files)]
        (println)
        (if (every? true? results)
          (do (println "All tests passed.") (System/exit 0))
          (do (println "Some tests FAILED.") (System/exit 1)))))))
