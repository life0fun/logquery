(defproject logquery "0.1.0-SNAPSHOT"
  :description "query cte-db3 elastic store for log processing"
  :url "[http://cte-db3:5601/  http://cte-db3:5555]"
  :license {:name "Location Labs"
            :url "http://locationlabs.com"}
  :dependencies [
  	[org.clojure/clojure "1.4.0"]
  	[org.clojure/clojure-contrib "1.2.0"] ; do I stil need the contrib ?
  	[clojurewerkz/elastisch "1.1.0"]  ; elastic search API
  	[korma "0.3.0-RC5"]    ; awesome korma for sql db ORM.            
  	[org.clojure/java.jdbc "0.2.3"]         ; jdbc
  	[mysql/mysql-connector-java "5.1.6"]    ; mysql jdbc driver
 		[org.postgresql/postgresql "9.2-1002-jdbc4"]  ; postgresql
 		[org.xerial/sqlite-jdbc "3.7.2"]  ; sqlite          
 		[clj-redis "0.0.12"]   ;                           
    [clojure-rabbitmq "0.2.1"]                         
    [org.clojure/data.json "0.2.2"]    ;; json package
    [clj-time "0.5.1"]        ; clj-time wraps Joda time
    [incanter "1.4.1"]        ; R-like stats and plotting
  ]
  :main logquery.core)
