(ns pgloader.load-file.parser
  (:require [clojure.string :as str]
            [instaparse.core :as insta]
            [pgloader.load-file.grammar :refer [grammar]]
            [pgloader.load-file.ast :as ast]))

(set! *warn-on-reflection* true)

(def parse
  "Parser function. Returns an instaparse result (hiccup tree) or failure."
  (insta/parser grammar :string-ci true :auto-whitespace :standard))

(defn- find-command-end
  "Find the position of the command-terminating semicolon
   that is outside $$ ... $$ quoting."
  [^String s]
  (let [len (.length s)]
    (loop [i 0
           in-dollars false]
      (if (>= i len)
        nil
        (let [c (.charAt s i)]
          (cond
            (and (not in-dollars) (= c \;))
            i

            ;; Check for $$
            (and (<= (+ i 1) len)
                 (= c \$)
                 (= \$ (.charAt s (min (inc i) (dec len)))))
            (recur (+ i 2) (not in-dollars))

            :else
            (recur (inc i) in-dollars)))))))

(defn- strip-comments
  "Remove -- line comments from the command portion (before the command-terminating ;),
   respecting $$ quoting so comments inside $$ blocks are left alone."
  [^String s]
  (let [end-pos (find-command-end s)
        cmd-end  (if end-pos (inc end-pos) (count s))
        cmd-part (subs s 0 cmd-end)
        data-part (if end-pos (subs s cmd-end) "")
        ;; Split on $$, strip comments from even-indexed parts (outside $$)
        outside-parts (->> (clojure.string/split cmd-part #"\$\$")
                           (map-indexed (fn [i part]
                                          (if (even? i)
                                            (clojure.string/replace part #"--[^\n]*" "")
                                            part)))
                           (clojure.string/join "$$"))]
    (str outside-parts data-part)))

(defn- extract-inline-data
  "If the command uses FROM INLINE, extract the text after the semicolon as inline data.
   Returns [command-str inline-data-str]."
  [^String s]
  (if (re-find #"(?i)\bFROM\b\s+\bINLINE\b" s)
    (if-let [semi-pos (find-command-end s)]
      [(subs s 0 (inc semi-pos))
       (clojure.string/trim (subs s (inc semi-pos)))]
      [s nil])
    [s nil]))

(defn parse-string
  "Parse a load command string. Returns {:ok command-tree} or {:error failure}."
  [s]
  (let [cleaned (strip-comments s)
        [cmd-str inline-data] (extract-inline-data cleaned)
        result  (parse cmd-str)]
    (if (insta/failure? result)
      {:error (insta/get-failure result)}
      (try
        {:ok (ast/transform result inline-data)}
        (catch Exception e
          {:error (str "AST transformation failed: " (.getMessage e))})))))

(defn- expand-env
  "Replace {{VAR}} placeholders with the value of the OS environment variable
   VAR. Throws if a referenced variable is not set."
  [s]
  (str/replace s #"\{\{(\w+)\}\}"
               (fn [[_ k]]
                 (or (System/getenv k)
                     (throw (ex-info (str "Undefined template variable: " k)
                                     {:var k}))))))

(defn parse-file
  "Parse a .load file. Expands {{VAR}} placeholders from the OS environment
   before parsing. Returns {:ok command-tree} or {:error message}."
  [path]
  (try
    (let [content (-> (slurp path) expand-env)]
      (parse-string content))
    (catch java.io.FileNotFoundException e
      {:error (str "File not found: " path)})
    (catch clojure.lang.ExceptionInfo e
      {:error (str "Template error: " (ex-message e))})
    (catch Exception e
      {:error (str "Error reading file: " (.getMessage e))})))
