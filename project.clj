(defproject backtype/dfs-datastores "1.0.2"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  
  :dependencies [
                 [jvyaml "1.0.0"]
                 [com.google.collections/google-collections "1.0"]
                 ]
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [junit/junit "3.8.2"]
                    ])
