
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
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format])
  (:require [incanter.core :refer :all]
            [incanter.stats :refer :all]
            [incanter.charts :refer :all]))
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

; For elasticsearch time range query. You can use DateTimeFormatterBuilder, or
; always use (formatters :data-time) formatter.
;

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
(declare view-data-example)

(defn test-query [idxname query process-fn]
  ; if idxname is unknown, we can use search-all-indexes-and-types.
  ; query range to be 
  ;(connect "cte-db3" 9200)
  (connect "localhost" 9200)           
  (let [res (esd/search-all-indexes-and-types ;esd/search-all-types idxname ;"logstash-2013.05.22"
              :size 100
              :query query
              :sort {"@timestamp" {"order" "desc"}})
         n (esrsp/total-hits res)
         hits (esrsp/hits-from res)
         f (esrsp/facets-from res)]
    (println (format "Total hits: %d" n))
    (process-fn hits)))
    

(defn test-stats-query [idxname]
  (test-query idxname (stats-query) process-stats-hits))


(defn test-trigger-query [idxname]
  ;(test-query idxname (trigger-task-query) prn))
  (test-query idxname (text-query) prn))


(defn text-query []
  (q/text "text" "elapsed"))


(defn stats-query []
  ; form query params for Stats query. time range from yesterday to today.
  ; logstash column/field begin with @
  (let [now (clj-time/now) pre (clj-time/minus now (clj-time/days 1))
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)
        prefmt (clj-time.format/unparse (clj-time.format/formatters :date-time) pre)]
    (q/filtered
      :query 
        (q/query-string 
          :default_field "@message"
          :query "@message:Stats AND @type:finder_core_application")
      :filter 
        {:range {"@timestamp" {:from prefmt     ;"2013-05-29T00:44:42"
                               :to nowfmt }}})))


(defn trigger-task-query []
  ; form query params to search elapsed keyword in log message.
  (q/filtered :query
    (q/query-string
      :default_field "@message"
      :query "@message:elapsed AND @type:finder_core_accounting_triggeredTask")))


; hits contains log message
(defn process-stats-hits [hits]
  ; query result contains hits map, extract log message from hits map
  (let [msgs (map (fn [row] (-> row :_source (get (keyword "@message")))) hits)
        stats (map process-stats-record msgs)] ; for each msg record, extract timestamp and stats field
    (map format-stats stats))
    (view-data-example))    ; map format stat fn to each stats in each record


(defn process-stats-record [record]
  ; for each log record, ret the first and 5th fields (timestamp and stats)
  (let [fields (clojure.string/split record #"\|")]  ; log delimiter is |
    ;(prn fields (count fields))
    (map #(nth fields %) [1 5])))

(defn format-stats [stats] 
  ; stats is ("Fri May 24.." "Stats = CNI{hs=10, time=20}")
  ; convert key=val to kv map using regexp. first, capture inside {}, match key=val.
  (let [st (first (re-find #"\{(.*?)\}" (last stats)))  ; capture non-greedy everything inside {}
        kv (re-seq #"([^=}{]+)=([^=}{]+)(?:,|$|}|\s+)" st) ; capture (any except ={) = () as kv pair seq
        ; after reg capture, each entry has 3 ele, [k=v, k, v], extract k v
        statmap (zipmap (map #(% 1) kv) (map #(% 2) kv))   ; now zip key seq and val seq into a map
        elapsed (quot (read-string (re-find #"\d+" (get statmap "timeTaken"))) 64000)] ; 64 threads
    (prn "elapse time: " elapsed "successEvents:" (get statmap "successfulEventCount") 
         (first stats))))

(defn view-data-example []
  ; to draw multiple bins bar-chart, like x with year(08, 09) and y with 4 seasons.
  ; for categories seq, manually repeate years [08 09] for 4 times, [08(spr), 09(spr), 08(sum), 09(sum), ...]
  ; for values seq, manully give value for [08-spr, 09-spr, 08-sum, 09-sum, ...]
  ; for grp by, we have 4 seasons in each year, so repeat 2 times [spr, sum, fall, wint]
  (let [ds (to-dataset [{"test" 1, "elapse" 20, "records" 300}
                        {"test" 2, "elapse" 30, "records" 200}
                        {"test" 3, "elapse" 10, "records" 100}])]

    (with-data ds  ; set $data to ds
      ;(view $data) 
      (let [t ($ :test $data)     ; get test column
            e ($ :elapse $data)
            r ($ :records $data)
            tests (mapcat identity (repeat 2 t))   ; test[1 2 3], each test, show 2 results, elapse, records
            ;vals (mapcat vector e r)    ; map take 1st of e, r, apply vector, concat with 2nd of e, r
            vals (concat e r)  ;elapse of test [1 2 3] followed by records
            grp (apply mapcat vector (repeat 3 [:elapse :records]))  ; 3 tests, on grp, repeat type
           ]
        (prn "test: " t "elapse: " e "records: " r "tests: " tests " vals: " vals)
        (view (bar-chart tests vals ;:test-time :elapse-time
                       :group-by grp
                       :title "Trigger task runs"
                       :x-label "test"
                       :y-label "elapse and records"
                       :legend true))))))
 