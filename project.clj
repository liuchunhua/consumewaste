(defproject clj_consumewaste "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [incanter/incanter-core "1.5.7"]
                 [incanter/incanter-io "1.5.7"]
                 [org.clojure/java.jdbc "0.7.0-alpha1"]
                 [com.oracle/ojdbc6 "12.1.0.2.0"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 ]
  :profiles {:dev {:plugins [[cider/cider-nrepl "0.14.0-SNAPSHOT"]]}}
  :local-repo "D:\\m2")
