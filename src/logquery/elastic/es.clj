;;
;; query elastic search engine
;;
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
  (:require [logquery.incanter.plot :refer :all]))

; overall query object format
; {
;     size: # number of results to return (defaults to 10)
;     from: # offset into results (defaults to 0)
;     fields: # list of document fields that should be returned - http://elasticsearch.org/guide/reference/api/search/fields.html
;     sort: # define sort order - see http://elasticsearch.org/guide/reference/api/search/sort.html
;     query: {
;         query_string: { fields: [] query: "query term"}
;     },
;     facets: {
;         # Facets provide summary information about a particular field or fields in the data
;     }
;     # special case for situations where you want to apply filter/query to results but *not* to facets
;     filter: {
;         # filter objects
;         # a filter is a simple "filter" (query) on a specific field.
;         # Simple means e.g. checking against a specific value or range of values
;     },
; }
;
; http://www.slideshare.net/clintongormley/terms-of-endearment-the-elasticsearch-query-dsl-explained
; curl -XGET 'http://cte-db3:9200/logstash-2013.05.22/_search?q=@type=finder_core_api
; kibana curls to elastic on browser tools, jsconsole, network, then issue a search, check kibana section.
; "curl -XGET http://localhost:9200/logstash-2013.05.23/_search?pretty -d ' 
; {"size": 100, 
;  "query": {
;     "filtered":{  <= a json obj contains a query and a filter, apply filter to the query result
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

; query string to search one one column
; fields to project @tags and @message columns into result json "fields" object.
; {   
;     "fields": ["@tags", "@message"]  <= a list of doc fields should ret
;     ,"query": {
;         "query_string": {
;            "default_field": "@tags", 
;            "query": "kiln"
;         }
;     }
; }
; 

; query string to search for multiple columns, set use_dis_max to true.
; http://www.elasticsearch.org/guide/reference/query-dsl/query-string-query/
; {   
;     "fields": ["@tags", "@type", "@message"]
;     ,"query": {
;         "query_string": {
;            "fields": ["@tags", "@type", "column-name.*"], <= for * match, same name fields must have same type.
;            "query": "kiln OR finder_core_api",
;            "use_dis_max": true  <= convert query into a DisMax query
;         }
;     }
; }

; elasticsearch engine document format
; every doc in ES has [] _id, _index, _type, _score, _source.]
; the _source column is where all custom mapping columns are defined. for logstash,
; @source, @tags, @type, @message, @timestamp, @timestamp, ...

; each index(db) has a list of mappings types(tables) that maps doc fields and their core types.
; each mapping has a name and :properties column family, where all columns and col :type and attrs.
; mapping-type {"person" {:properties {:username {:type "string" :store "yes"} :age {}}}}
;               "vip"    {:properties {:vipname {:type "multi_field" :store "yes"} :age {}}}
; column :type "multi_field" allows a list of core_types to apply to the same column during mapping.
;"properties" : {
;   "name" : {
;     "type" : "multi_field",
;     "path": "just_name",  // how multi-field is accessed, apart from the default field
;     "fields" : {
;         "name" : {"type" : "string", "index" : "analyzed"},
;         "untouched" : {"type" : "string", "index" : "not_analyzed"}}


; logstash grok filter apply to logs with tag [] and append new fields, so in mapping type
; finder-core-application, {"properties" {"@fields" {"properties" {class, host, level, thread}}}}
; grok { 
;   tags => ["format_finder_core_application"]
;   pattern => "^%{FORMAT_FINDER_CORE_APPLICATION}"
;   patterns_dir => "/etc/logstash/patterns"
; }
; FORMAT_FINDER_CORE_APPLICATION (?m)%{HOSTNAME:host} \| %{_FINDER_TS:ts} \| %{DATA:thread} \| %{JAVACLASS:class} \| %{LOGLEVEL:level} \|


; globals
(def ^:dynamic *es-conn*)

(def time-range 1)    ; from now, just 1 day back

; forward declaration
(declare query-string-keyword)
(declare process-record)
(declare text-query-filter)
(declare elastic-query)

(declare query-stats)
(declare process-stats-hits)
(declare format-stats)

(declare query-email)
(declare process-email-hits)
(declare format-email)


; our key logstash fields, not that the result column is keyword. ":@message"
(def logstash-fields ["@type" "@tags" "@message" "@source_host" "@source"])

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


(defn test-trigger-query [idxname]
  ;(test-query idxname (trigger-task-filter) prn))
  (elastic-query idxname (text-query-filter) prn))


(defn trigger-task-filter []
  ; form query params to search elapsed keyword in log message.
  (q/filtered :query
    (q/query-string
      :default_field "@message"
      :query "@message:elapsed AND @type:finder_core_accounting_triggeredTask")))


(defn text-query-filter []
  ; filter to query text field with elapsed keyword
  (q/text "text" "elapsed"))


(defn elastic-query [idxname query process-fn]
  ; if idxname is unknown, we can use search-all-indexes-and-types.
  ; query range to be 
  (connect "cte-db3" 9200)
  ;(connect "localhost" 9200)           
  (let [res ;(esd/search-all-indexes-and-types  ; cant do this, we now have 6billion logs
            (esd/search-all-types idxname ;"logstash-2013.05.22"
              :size 10        ; limits to 10 results
              :query query
              :sort {"@timestamp" {"order" "desc"}})  ; desc order
         n (esrsp/total-hits res)
         hits (esrsp/hits-from res)  ; searched out docs in hits array
         facets (esrsp/facets-from res)]  ; facets
    (println (format "Total hits: %d" n))
    (process-fn hits)))


(defn query-string-keyword [keyname]
  ; form query params for Stats query. time range from yesterday to today.
  ; logstash column/field begin with @
  (let [now (clj-time/now) 
        ;pre (clj-time/minus now (clj-time/days time-range))  ; from now back 2 days
        pre (clj-time/minus now (clj-time/days 7))  ; from now back 1 days
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)
        prefmt (clj-time.format/unparse (clj-time.format/formatters :date-time) pre)]
    (q/filtered
      :query 
        (q/query-string 
          :default_field "@message"
          ;:query "@message:EmailAlertDigestEventHandlerStats AND @type:finder_core_application")
          :query (str "@message:" keyname " AND @type:finder_core_application"))
      :filter 
        {:range {"@timestamp" {:from prefmt     ;"2013-05-29T00:44:42"
                               :to nowfmt }}})))

(defn multifields-query [keyname]
  "query term in multi fields, @type, @tags, @message, @source_host, @source
  logstash field begins with @ symbol"
  (let [now (clj-time/now) 
        ;pre (clj-time/minus now (clj-time/days time-range))  ; from now back 2 days
        pre (clj-time/minus now (clj-time/hours 20))  ; from now back 1 days
        nowfmt (clj-time.format/unparse (clj-time.format/formatters :date-time) now)
        prefmt (clj-time.format/unparse (clj-time.format/formatters :date-time) pre)]
    ; (q/filtered
    ;   :query 
    ;     (q/query-string
    ;       :fields logstash-fields
    ;       ;:query "@message:EmailAlertDigestEventHandlerStats AND @type:finder_core_application")
    ;       :query (str keyname))
    ;   :filter 
    ;     {:range {"@timestamp" {:from prefmt     ;"2013-05-29T00:44:42"
    ;                            :to nowfmt }}})))
    (q/query-string
      :fields logstash-fields
      :query (str keyname))))
      

(defn process-record [record]
  ; logstash log has the following format, [type | timestamp | file-name | log-content ]
  ; for each log record, ret the first and 5th fields (timestamp and stats)
  (let [fields (clojure.string/split record #"\|")]  ; log delimiter is |
    ;(prn fields (count fields))
    (map #(nth fields %) [1 5])))


(defn query-stats [idxname]
  (elastic-query idxname 
                 (query-string-keyword "CNIContactUsageEventHandlerStats") 
                 process-stats-hits))


; hits contains log message
(defn process-stats-hits [hits]
  ; query result contains hits map, extract log message from hits map
  (let [msgs (map (fn [row] (-> row :_source (get (keyword "@message")))) hits)
        stats (map process-record msgs)
        test-result (map format-stats stats)] ; for each msg record, extract timestamp and stats field
    ;(prn test-result)))    ; testing
    (view-stats-data test-result)))  ;map format stat fn to each stats in each record


(defn format-stats [stats] 
  ; stats is ("Fri May 24.." "Stats = CNI{hs=10, time=20}")
  ;(prn stats))
  ; convert key=val to kv map using regexp. first, capture inside {}, match key=val.
  (let [ts (clojure.string/join " " (reverse (map #(first (clojure.string/split % #"\.")) (take 3 (rest (clojure.string/split (clojure.string/trim (first stats)) #"\s+"))))))
        ;tsf (clj-time.format/unparse (clj-time.format/formatters :date-hour-minute-second) (clj-time.format/parse-local ts))
        st (first (re-find #"\{(.*?)\}" (last stats)))  ; capture non-greedy everything inside {}
        kv (re-seq #"([^=}{]+)=([^=}{]+)(?:,|$|}|\s+)" st) ; capture (any except ={) = () as kv pair seq
        ; after reg capture, each entry has 3 ele, [k=v, k, v], extract k v
        statmap (zipmap (map #(% 1) kv) (map #(-> % (nth 2) (->> (re-find #"\d+"))) kv))
        elapsed (quot (read-string (re-find #"\d+" (get statmap "timeTaken"))) 64000)]    ; 64 threads
    (prn "elapse time: " elapsed "successEvents:" (get statmap "successfulEventCount") (rest stats))
    (println)
    (assoc statmap :elapse elapsed :timestamp ts)))  ; ret the map
  

;; section for handling email query
(defn query-email [idxname]
  (elastic-query idxname 
                 (query-string-keyword "EmailAlertDigestEventHandlerStats")
                 process-email-hits))


; hits contains log message
(defn process-email-hits [hits]
  ; query result contains hits map, extract log message from hits map
  (let [msgs (map (fn [row] (-> row :_source (get (keyword "@message")))) hits)
        emailstats (map process-record msgs)
        test-result (map format-email emailstats)] ; for each msg record, extract timestamp and stats field
    ;(prn (first test-result))))
    (view-email-data test-result)))  ;map format stat fn to each stats in each record
                

(defn format-email [stats] 
  ; stats is ("Fri May 24.." "Stats = CNI{hs=10, time=20}")
  ;(prn stats))
  ; convert key=val to kv map using regexp. first, capture inside {}, match key=val.
  (let [ts (clojure.string/join " " (reverse (map #(first (clojure.string/split % #"\.")) (take 3 (rest (clojure.string/split (clojure.string/trim (first stats)) #"\s+"))))))
        ;tsf (clj-time.format/unparse (clj-time.format/formatters :date-hour-minute-second) (clj-time.format/parse-local ts))
        st (first (re-find #"\{(.*?)\}" (last stats)))  ; capture non-greedy everything inside {}
        kv (re-seq #"([^=}{]+)=([^=}{]+)(?:,|$|}|\s+)" st) ; capture (any except ={) = () as kv pair seq
        ; after reg capture, each entry has 3 ele, [k=v, k, v], extract k v
        statmap (zipmap (map #(% 1) kv) (map #(-> % (nth 2) (->> (re-find #"\d+"))) kv))
        elapsed (quot (read-string (re-find #"\d+" (get statmap "totalTimeTaken"))) 64000)]    ; 64 threads
    (prn "elapse time: " elapsed "numberOfEmailsSent:" (get statmap "numberOfEmailsSent") statmap)
    (println)
    (assoc statmap :elapse elapsed :timestamp ts)))  ; ret the map


; hits = :_source {:@source "file://finderapi", :@tags ["finder" "verizon" "core" "FROM_TCP"]
(defn process-finderlog-hits 
  "searched out hit docs ary, get each doc out"
  [hitsary]
  (let [docs (map #(get-in % [:_source]) hitsary)
        projcols (map keyword ["@timestamp" "@type" "@tags" "@message"])
        tags (map #(select-keys % [(keyword "@tags")]) docs)
        rslt (map #(select-keys % projcols) docs)]
    ;(prn docs)
    ; (prn "tags " tags)
    ; (prn rslt)
    (doall (map println rslt))))


(defn query-finderlog [idxname term]
  "query logstash with key name in all columns, eg, 
   @type, @tags, @message, @source_host, @source columns"
  (let [query (multifields-query term)]
    (elastic-query idxname query process-finderlog-hits)))