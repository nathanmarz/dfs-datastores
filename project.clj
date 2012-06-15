(defproject backtype/dfs-datastores "1.1.4-SNAPSHOT"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :java-test-path "test/jvm"
  :javac-options {:debug "true" :fork "true"}
  :javac-source-path [["src"] ["test"]]
  :junit [["classes"]]
  :junit-options {:fork "off" :haltonfailure "on"}
  :dependencies [[jvyaml "1.0.0"]
                 [com.google.guava/guava "r09"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.0" :exclusions [org.clore/clojure]]
                     [org.clojure/clojure "1.2.1"]
                     [lein-midje "1.0.8"]
                     [lein-javac "1.3.0"]
                     [lein-junit "1.0.0"]
                     [junit "4.7"]])
