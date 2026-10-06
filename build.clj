(ns build
  "Builds the library jar. The library name and description come from the
   :build alias of deps.edn; the version from APP_VERSION, otherwise from the
   latest git tag."
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.tools.build.api :as b]))

(def ^:private class-dir "target/classes")
(def ^:private source-dirs ["src" "resources"])
(def ^:private license-name "MIT License")
(def ^:private license-url "https://opensource.org/licenses/MIT")
(def ^:private github-url "https://github.com/")

(defn- build-options
  "Returns the :exec-args of the :build alias of deps.edn."
  []
  (get-in (edn/read-string (slurp "deps.edn"))
          [:aliases :build :exec-args]))

(defn- describe-version
  "Returns the version to build: APP_VERSION when set, otherwise the latest git
   tag without its leading v, otherwise dev."
  []
  (or (System/getenv "APP_VERSION")
      (some-> (b/git-process {:git-args ["describe" "--tags" "--always"]})
              (str/replace #"^v" ""))
      "dev"))

(defn- repository-path
  "Returns the owner/name path of the GitHub repository behind a library.
   - lib: the library symbol, such as io.github.af2b/borba-core-component"
  [lib]
  (str (last (str/split (namespace lib) #"\.")) "/" (name lib)))

(defn clean
  "Removes the build output."
  [_]
  (b/delete {:path "target"}))

(defn jar
  "Writes the library jar and its pom.xml under target/ and returns the jar's
   path.
   - lib: the library symbol (default: :lib of the :build alias)
   - version: the version to build (default: APP_VERSION, else the git tag)"
  [options]
  (let [{:keys [lib description]} (merge (build-options) options)
        version  (or (:version options) (describe-version))
        basis    (b/create-basis {:project "deps.edn"})
        jar-file (format "target/%s-%s.jar" (name lib) version)
        repo-url (str github-url (repository-path lib))]
    (clean nil)
    (b/write-pom {:class-dir class-dir
                  :lib       lib
                  :version   version
                  :basis     basis
                  :src-dirs  ["src"]
                  :scm       {:url repo-url
                              :tag (str "v" version)}
                  :pom-data  [[:description description]
                              [:url repo-url]
                              [:licenses
                               [:license
                                [:name license-name]
                                [:url license-url]]]]})
    (b/copy-dir {:src-dirs   source-dirs
                 :target-dir class-dir})
    (b/jar {:class-dir class-dir
            :jar-file  jar-file})
    (println "built" jar-file)
    jar-file))
