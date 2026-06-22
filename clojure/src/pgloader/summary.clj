(ns pgloader.summary
  (:require [pgloader.log :as log]
            [pgloader.stats :as stats]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io Writer]))

(set! *warn-on-reflection* true)

;; ── Column widths (match CL pgloader output) ──────────────────────────────
(def ^:private w-errors  9)
(def ^:private w-rows    9)
(def ^:private w-bytes   9)
(def ^:private w-time   14)
(def ^:private w-rw      9)   ; verbose read/write columns
(def ^:private min-label 24)

(defn- label-width [entries]
  (reduce (fn [acc e] (max acc (count (:label e))))
          min-label
          entries))

(defn- sep-line
  ([lw] (sep-line lw false))
  ([lw verbose]
   (str (str/join "" (repeat lw "-"))
        "  " (str/join "" (repeat w-errors "-"))
        "  " (str/join "" (repeat w-rows "-"))
        "  " (str/join "" (repeat w-bytes "-"))
        "  " (str/join "" (repeat w-time "-"))
        (when verbose
          (str "  " (str/join "" (repeat w-rw "-"))
               "  " (str/join "" (repeat w-rw "-")))))))

(defn- fmt-row
  ([lw label errs rows bytes time]
   (fmt-row lw label errs rows bytes time nil nil))
  ([lw label errs rows bytes time rs ws]
   (str (format (str "%" lw "s") label)
        "  " (format (str "%" w-errors "s") errs)
        "  " (format (str "%" w-rows "s") rows)
        "  " (format (str "%" w-bytes "s") bytes)
        "  " (format (str "%" w-time "s") time)
        (when rs
          (str "  " (format (str "%" w-rw "s") rs)
               "  " (format (str "%" w-rw "s") ws))))))

(defn- header-row
  ([lw] (header-row lw false))
  ([lw verbose]
   (fmt-row lw "table name" "errors" "rows" "bytes" "total time"
            (when verbose "read") (when verbose "write"))))

(defn- fmt-entry
  ([lw entry] (fmt-entry lw entry false))
  ([lw entry verbose]
   (let [b (if (zero? (:bytes entry)) "" (log/fmt-bytes (:bytes entry)))
         t (log/fmt-duration (:total-nanos entry))
         r (when (and verbose (pos? (or (:rs-nanos entry) 0)))
             (log/fmt-duration (:rs-nanos entry)))
         w (when (and verbose (pos? (or (:ws-nanos entry) 0)))
             (log/fmt-duration (:ws-nanos entry)))]
     (fmt-row lw (:label entry) (:errs entry) (:rows entry) b t r w))))

(defn print-summary
  ([verbose] (print-summary verbose nil))
  ([verbose wall-nanos]
   (let [all-entries (concat (stats/entries :pre) (stats/entries :data) (stats/entries :post))
         lw          (label-width all-entries)]
     (println)
     (println "Results:")
     (println)
     (when (seq all-entries)
       (println (header-row lw verbose))
       (let [sections (keep (fn [p]
                              (let [es (stats/entries p)]
                                (when (seq es) es)))
                            [:pre :data :post])]
         (println (sep-line lw verbose))
         (doseq [[i section] (map-indexed vector sections)]
           (when (pos? i) (println (sep-line lw verbose)))
           (doseq [entry section]
             (println (fmt-entry lw entry verbose)))))
       (println (sep-line lw verbose))
       (let [g       (stats/grand-totals)
             b       (if (zero? (:bytes g)) "" (log/fmt-bytes (:bytes g)))
             t       (log/fmt-duration (or wall-nanos (:total-nanos g)))
             err-str (if (zero? (:errs g)) "✓" (str (:errs g)))]
         (println (fmt-row lw "Total import time" err-str (:rows g) b t))
         (println))))))

(defn- csv-quote [s]
  (if (re-find #"[;\"]" s)
    (str "\"" (str/replace s "\"" "\"\""))
    s))

(defn write-summary-csv
  ([^String path verbose] (write-summary-csv path verbose nil))
  ([^String path verbose wall-nanos]
   (with-open [^Writer w (io/writer path)]
     (let [header (if verbose
                    ["table name" "errors" "rows" "bytes" "total time" "read time" "write time"]
                    ["table name" "errors" "rows" "bytes" "total time"])]
       (.write w (str/join ";" header))
       (.write w "\n")
       (doseq [phase [:pre :data :post]]
         (doseq [e (stats/entries phase)]
           (let [vals [(csv-quote (:label e))
                       (str (:errs e))
                       (str (:rows e))
                       (str (:bytes e))
                       (log/fmt-duration (:total-nanos e))]]
             (.write w (str/join ";" (if verbose
                                       (conj vals
                                             (log/fmt-duration (:rs-nanos e))
                                             (log/fmt-duration (:ws-nanos e)))
                                       vals)))
             (.write w "\n"))))
       (let [g (stats/grand-totals)]
         (.write w (str/join ";" ["GRAND TOTAL"
                                  (str (:errs g))
                                  (str (:rows g))
                                  (str (:bytes g))
                                  (log/fmt-duration (or wall-nanos (:total-nanos g)))]))
         (.write w "\n"))))))

(defn write-summary-json
  ([^String path verbose] (write-summary-json path verbose nil))
  ([^String path verbose wall-nanos]
   (let [data (reduce
               (fn [acc phase-key]
                 (let [entries (stats/entries phase-key)]
                   (assoc acc (name phase-key)
                          {:tables (mapv
                                    (fn [e]
                                      (merge
                                       {:label (:label e)
                                        :errors (:errs e)
                                        :rows (:rows e)
                                        :bytes (:bytes e)
                                        :total-time (:total-nanos e)}
                                       (when verbose
                                         {:read-time (:rs-nanos e)
                                          :write-time (:ws-nanos e)})))
                                    entries)
                           :total (stats/get-totals phase-key)})))
               {} [:pre :data :post])
         g    (stats/grand-totals)
         full {:phases      data
               :grand-total (cond-> g wall-nanos (assoc :total-nanos wall-nanos))}]
     (spit path (pr-str full)))))

(defn write-summary
  ([path verbose] (write-summary path verbose nil))
  ([path verbose wall-nanos]
   (when path
     (cond
       (str/ends-with? path ".csv")  (write-summary-csv path verbose wall-nanos)
       (str/ends-with? path ".json") (write-summary-json path verbose wall-nanos)
       :else                         (write-summary-csv path verbose wall-nanos))
     (println (str "Summary written to " path)))))
