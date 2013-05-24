
(ns logquery.elastic.es
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis])    ; bring in redis namespace
  (:require [clojure.data.json :as json])
  (:require [clojurewerkz.elastisch.rest          :as esr]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.query         :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]
            [clojure.pprint :as pp])
  (:require [clj-time.core :refer :all :exclude [extend]]))

;
; http://www.slideshare.net/clintongormley/terms-of-endearment-the-elasticsearch-query-dsl-explained
; curl -XGET 'http://cte-db3:9200/logstash-2013.05.22/_search?q=@type=finder_core_api
; kibana curls to elastic on browser tools, jsconsole, network, then issue a search, check kibana section.
; "curl -XGET http://localhost:9200/logstash-2013.05.23/_search?pretty -d ' 
; {"size": 100, 
;  "query": {
;     "filtered":{
;       "query":{ 
;         "query_string":{
;           "default_operator": "OR", default_field": "@message",
;           "query": "@message:Stats AND @type:finder_core_application"}}, <- close of query
;       "filter": {
;         "range": {
;           "@timestamp": {
;             "from": "2013-05-22T16:10:48Z", "to": "2013-05-23T02:10:48Z"}}}}},
;   "from": 0,
;   "sort": {
;     "@timestamp":{
;       "order": "desc"}},

(def ^:dynamic *es-conn*)

; wrap connecting fn
(defn connect [host port]
  (esr/connect! (str "http://" host ":" port)))

; eval exprs within a new connection.    
(defmacro with-esconn [[host port] & exprs]
  `(with-open [conn# (esr/connect! (str "http://" ~host ":" ~port))]
     (binding [*es-conn* conn#]                
       (do ~@exprs))))

; document query takes index name, mapping name and query (as a Clojure map)
; curl -s http://127.0.0.1:9200/_status?pretty=true | grep logstash
; curl -XGET 'http://cte-db3:9200/logstash-2013.05.22/_search?q=@type=finder_core_api
(defn test-query-with [tag]
  (with-esconn ["cte-db3" 9200]
    (let [res (esd/search "logstash-2013.05.22" :query (q/term :message "timeInvoked"))
          n    (esrsp/total-hits res)
          hits (esrsp/hits-from res)]
      (println (format "Total hits: %d" n))
      (pp/pprint hits))))

;
; forward declaration
(declare text-query)
(declare stats-query)
(declare format-stats)
(declare trigger-task-query)
(declare process-stats-hits)
(declare process-stats-record)

(defn test-query [idxname query processor]
  (connect "cte-db3" 9200)
  (let [res (esd/search-all-types idxname ;"logstash-2013.05.22"
              :query query
              :sort {"@timestamp" {"order" "desc"}})
         n (esrsp/total-hits res)
         hits (esrsp/hits-from res)
         f (esrsp/facets-from res)]
    (println (format "Total hits: %d" n))
    (processor hits)))
    

(defn test-stats-query [idxname]
  (test-query idxname (stats-query) process-stats-hits))

(defn test-trigger-query [idxname]
  ;(test-query idxname (trigger-task-query) prn))
  (test-query idxname (text-query) prn))

(defn text-query []
  (q/text "text" "elapsed"))


(defn stats-query []
  ; logstash column begin with @
  (q/filtered :query 
    (q/query-string 
      :default_field "@message"
      :query "@message:Stats AND @type:finder_core_application")))


(defn trigger-task-query []
  (q/filtered :query
    (q/query-string
      :default_field "@message"
      :query "@message:elapsed AND @type:finder_core_accounting_triggeredTask")))


; hits contains log message
(defn process-stats-hits [hits]
  (let [msgs (map (fn [row] (-> row :_source (get (keyword "@message")))) hits)
        stats (map process-stats-record msgs)] ; for each msg record, extract timestamp and stats field
    (map format-stats stats)))    ; map format stat fn to each stats in each record


(defn process-stats-record [record]
  ; for each log record, ret the first and 5th fields (timestamp and stats)
  (let [fields (clojure.string/split record #"\|")]  ; log delimiter is |
    ;(prn fields (count fields))
    (map #(nth fields %) [1 5])))

(defn format-stats [stats] ; stats is ("Fri May 24.." "Stats = CNI{hs=10, time=20}")
  ; convert key=val to kv map using regexp. first, capture inside {}, match key=val.
  (let [st (first (re-find #"\{(.*?)\}" (last stats)))  ; capture non-greedy everything inside {}
        kv (re-seq #"([^=}{]+)=([^=}{]+)(?:,|$|}|\s+)" st) ; capture (any except ={) = () as kv pair seq
        ; after reg capture, each entry has 3 ele, [k=v, k, v], extract k v
        statmap (zipmap (map #(% 1) kv) (map #(% 2) kv))   ; now zip key seq and val seq into a map
        elapsed (quot (read-string (re-find #"\d+" (get statmap "timeTaken"))) 64000)] ; 64 threads
    (prn "elapse time: " elapsed "successEvents:" (get statmap "successfulEventCount") 
         (first stats))))
