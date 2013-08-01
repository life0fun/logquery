(ns logquery.core
	(:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis]) ; bring in redis namespace
  (:require [logquery.elastic.es :as es]
            [logquery.elastic.facet :as facet])
  (:require [logquery.incanter.plot :as plot])
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format]
            [clj-time.local])
  (:gen-class :main true))


; status query
(defn log-query [test args]
  (prn "log-query args :" test args)
  (let [now (clj-time.local/local-now)
        fns (map (fn [nm] (ns-resolve 'clj-time.core (symbol nm))) ["year" "month" "day"])
        datm (map (fn [f] (format "%02d" (f now))) fns)   ; clojure.core/format string
        nowidx (str "logstash-" (clojure.string/join "." datm))
        fmt-now (clj-time.format/unparse (clj-time.format/formatter "yyyy.MM.dd") now)
        nxt-week (clj-time/plus now (clj-time/weeks 1))
        idxname (or (nil? args) nowidx)]
	  (prn "searching..." idxname fmt-now nowidx nxt-week)
    ;(es/test-trigger-query idxname)
    (case test
      :stats (es/query-stats idxname)
      :email (es/query-email idxname)
      :facet (facet/test-date-hist idxname)
      (es/query-stats idxname))))     ; default query stats


(defn plot-data 
  ([]
    (plot/plot-hs-data "/opt/haijin/svn/wm/project/vci_scale/test/data/hs"))
  ([args]
    ; args ary is ["plot" "/tmp/x"]
    (plot/plot-hs-data args)))  ; second arg is log file


(defn -main [& args]
 	(prn " >>>> elastic log query <<<<< ")
  (prn " - lein run stats index-name ")
  (prn " - lein run plot hs-data-file")
  (case (first args)
    "stats" (log-query :stats args)
    "email" (log-query :email args)
    "plot"  (plot-data)
    "facet" (log-query :facet (rest args))
    (log-query :stats args)))    ; default
