(defproject clj_consumewaste "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.395"]
                 [incanter/incanter-core "1.5.7"]
                 [incanter/incanter-io "1.5.7"]
                 [org.clojure/java.jdbc "0.7.0-alpha1"]
                 [com.oracle/ojdbc6 "11.2.0.3.0"]
                 [com.mchange/c3p0 "0.9.5.2"]
                 [com.taoensso/carmine "2.15.0"]
                 [org.apache.lucene/lucene-core "6.3.0"]
                 [org.apache.lucene/lucene-analyzers-common "6.3.0"]
                 [org.apache.lucene/lucene-analyzers-smartcn"6.3.0"]
                 [org.apache.lucene/lucene-queryparser "6.3.0"]
                 [org.apache.lucene/lucene-queries"6.3.0"]
                 ]
  :profiles {:dev {:plugins [[cider/cider-nrepl "0.15.0-SNAPSHOT"]]}}
  :local-repo "D:\\m2")
