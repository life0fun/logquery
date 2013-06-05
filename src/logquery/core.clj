(ns logquery.core
	(:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis]) ; bring in redis namespace
  (:require [logquery.elastic.es :as es])
  (:require [clj-time.core :refer :all :exclude [extend]])  ; :refer :all the same as :use, blow all pub vars into ns
  (:require [clj-time local format])
  (:gen-class :main true))    ; bring in redis namespace


; search
(defn logsearch [args]
  (case )
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (plus now (weeks 1))
        idxname (or args nowidx)]
	  (prn "searching..." idxname fmt-now nowidx nxt-week)
    (es/test-trigger-query idxname)
    (es/test-stats-query idxname)))

; the main 
(defn -main [& args]
 	(prn " >>>> elastic log query <<<<< ")
  (prn " - lein run stats idx-name ")
  (prn " - lein run plot hs-data-file")
	(logsearch args))

