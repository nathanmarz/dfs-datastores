(defproject org.clojars.sorenmacbeth/dfs-datastores "1.1.1-SNAPSHOT"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :java-test-path "test/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[jvyaml "1.0.0"]
                 [com.google.guava/guava "r09"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [midje "1.3.0"]
                     [junit/junit "3.8.2"]])
