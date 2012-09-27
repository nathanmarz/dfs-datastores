(defproject backtype/dfs-datastores "1.2.0-SNAPSHOT"
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :test-paths ["test/clj"]
  :java-source-paths ["src/jvm" "test/jvm"]
  :hooks [leiningen.hooks.junit]
  :junit ["test/jvm"]
  :junit-options {:fork "off" :haltonfailure "on"}
  :dependencies [[jvyaml "1.0.0"]
                 [com.google.guava/guava "13.0"]]
  :plugins [[lein-midje "1.0.10"]
            [lein-junit "1.0.3"]]
  :profiles {:dev {:dependencies [[junit "4.10"]
                                  [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                                  [midje "1.4.0" :exclusions [org.clojure/clojure]]
                                  [org.clojure/clojure "1.4.0"]]}})
