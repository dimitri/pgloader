(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'io.pgloader/pgloader-v4)
(def version "0.1.0-SNAPSHOT")
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def uber-file (format "target/pgloader-v4-%s.jar" version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis     basis
                  :src-dirs  ["src"]
                  :class-dir class-dir
   :ns-compile '[pgloader.core
                                 pgloader.cli
                                 pgloader.cast
                                 pgloader.copy
                                 pgloader.batch
                                 pgloader.prefetch
                                 pgloader.reject
                                 pgloader.load-file.parser
                                 pgloader.load-file.grammar
                                 pgloader.load-file.ast
                                 pgloader.source.protocol
                                 pgloader.source.csv
                                 pgloader.source.mysql
                                 pgloader.ddl.mysql
                                 pgloader.ddl.common]})
   (b/uber {:class-dir class-dir
            :uber-file uber-file
            :basis     basis
            :main      'pgloader.cli})
   ;; Create a stable pgloader.jar symlink so docker-compose volume mounts work.
   (let [target-name (-> uber-file (clojure.string/replace #"^target/" ""))
         symlink     (java.nio.file.Paths/get "target/pgloader.jar" (make-array String 0))
         uber-path   (java.nio.file.Paths/get target-name (make-array String 0))]
     (when (java.nio.file.Files/exists symlink (make-array java.nio.file.LinkOption 0))
       (java.nio.file.Files/delete symlink))
     (java.nio.file.Files/createSymbolicLink symlink uber-path (make-array java.nio.file.attribute.FileAttribute 0))
     (println (str "Symlinked target/pgloader.jar -> " target-name))))
