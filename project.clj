(def VERSION (slurp "VERSION"))
(def MODULES (-> "MODULES" slurp (.split "\n")))
(def DEPENDENCIES (for [m MODULES] [(symbol (str "com.backtype/" m)) VERSION]))

(eval `(defproject com.backtype/bundle ~VERSION
   :description "Dead-simple vertical partitioning, compression, appends, and consolidation of data on a distributed filesystem."
   :url "https://github.com/nathanmarz/dfs-datastores"
   :license {:name "Eclipse Public License"
             :url "http://www.eclipse.org/legal/epl-v10.html"}
   :plugins [[~'lein-sub "0.3.0"]]
   :sub [~@MODULES]))
