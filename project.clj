(defproject metaffix "0.1.0-SNAPSHOT"
  :description "affix metadata to files in a cbfs"
  :url "http://github.com/apage43/metaffix"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :jvm-opts ["-Xmx1g"]
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/tools.cli "0.2.2"]
                 [clj-http "0.5.3"]
                 [cheshire "4.0.3"]
                 [org.apache.tika/tika-core "1.2"]
                 [org.apache.tika/tika-parsers "1.2"]
                 [clojurewerkz/spyglass "1.1.0-SNAPSHOT"]]
  :main metaffix.core)
