(defproject backtype/dfs-datastores "1.0.5"
  :java-source-path "src/jvm"
  :java-test-path "test/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[jvyaml "1.0.0"]
                 [com.google.guava/guava "r09"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [junit/junit "3.8.2"]])
