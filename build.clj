(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib       'com.github.andreborba/borba-kafka-consumer-component)
(def version   (or (System/getenv "APP_VERSION") "dev"))
(def class-dir "target/classes")
(def jar-file  (format "target/%s-%s.jar" (name lib) version))

(defn jar [_]
  (b/write-pom {:class-dir class-dir
                :lib       lib
                :version   version
                :basis     (b/create-basis {:project "deps.edn"})
                :src-dirs  ["src"]})
  (b/copy-dir {:src-dirs   ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file  jar-file})
  (println (str "▶ Built: " jar-file)))
