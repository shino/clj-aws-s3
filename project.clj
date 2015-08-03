(defproject com.shino/clj-aws-s3 "0.3.10-p1-SNAPSHOT"
  :description "Clojure Amazon S3 library"
  :url "https://github.com/shino/clj-aws-s3"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.amazonaws/aws-java-sdk "1.10.5.1" :exclusions [joda-time]]
                 [org.apache.httpcomponents/httpclient "4.3.3"]
                 [joda-time "2.2"]
                 [clj-time "0.6.0"]]
  :plugins [[codox "0.8.10"]])
