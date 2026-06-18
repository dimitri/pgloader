(ns pgloader.log
  (:import [java.util Locale]))

(set! *warn-on-reflection* true)

(defn- locale-format
  "Like clojure.core/format but with an explicit Locale to force '.' as decimal separator."
  [locale fmt & args]
  (String/format locale fmt (to-array args)))

(defn fmt-duration
  "Format nanoseconds as the CL pgloader does: always seconds with 3 dp,
   prefixing larger units (minutes, hours, days) when applicable."
  [nanos]
  (let [ms   (long (/ nanos 1000000))
        days  (quot ms 86400000)
        hours (quot (rem ms 86400000) 3600000)
        mins  (quot (rem ms 3600000) 60000)
        secs  (/ (rem ms 60000) 1000.0)]
    (cond
      (>= days 1)  (locale-format Locale/US "%dd %02d:%02d:%06.3f" (int days) (int hours) (int mins) secs)
      (>= hours 1) (locale-format Locale/US "%02d:%02d:%06.3f" (int hours) (int mins) secs)
      (>= mins 1)  (locale-format Locale/US "%dm%06.3fs" (int mins) secs)
      :else        (locale-format Locale/US "%.3fs" (/ ms 1000.0)))))

(defn fmt-bytes
  "Format byte count into a human-readable string.
   Returns:
     1.23 GB  (≥ 1 GB)
     45.6 MB  (≥ 1 MB)
     123 KB   (≥ 1 KB)
     456 B    (< 1 KB)
  "
  [n]
  (let [abs-n (Math/abs ^long n)]
    (cond
      (>= abs-n 1073741824) (locale-format Locale/US "%.2f GB" (/ n 1073741824.0))
      (>= abs-n 1048576)    (locale-format Locale/US "%.2f MB" (/ n 1048576.0))
      (>= abs-n 1024)       (locale-format Locale/US "%.2f kB" (/ n 1024.0))
      :else                 (str n " B"))))
