
(ns logquery.incanter.plot
  (:require [clojure.string :as str])
  (:require [clojure.java.jdbc :as sql])
  (:import [java.io FileReader]
           [java.util Map Map$Entry List ArrayList Collection Iterator HashMap])
  (:require [clj-redis.client :as redis])    ; bring in redis namespace
  (:require [clojure.data.json :as json])
  (:require [clj-time.core :as clj-time :exclude [extend]]
            [clj-time.format])
  (:require [incanter.core :refer :all]
            [incanter.stats :refer :all]
            [incanter.charts :refer :all]
            [incanter.io :refer :all]))


; global configs

; forward declarations
(declare view-stats-data)
(declare plot-hs-data)

(defn view-stats-data 
  ([]   ; no data arg, cock data by myself.
    (let [data [{:timestamp "13:24:58 3 Jun", :elapse 51, "timeTaken" "3327521", "contactsSortedCount" "2597551", "failedEventCount" "0", "successfulEventCount" "10940", "historyStoreRecordsFetched" "1992912", "historyStoreQueryCount" "10940"} 
                {:timestamp "11:40:43 3 Jun", :elapse 51, "timeTaken" "3312814", "contactsSortedCount" "2598514", "failedEventCount" "0", "successfulEventCount" "10943", "historyStoreRecordsFetched" "2060237", "historyStoreQueryCount" "10943"} 
                {:timestamp "11:39:21 3 Jun", :elapse 50, "timeTaken" "3252625", "contactsSortedCount" "2598514", "failedEventCount" "0", "successfulEventCount" "10943", "historyStoreRecordsFetched" "2061081", "historyStoreQueryCount" "10943"} 
                {:timestamp "09:46:53 3 Jun", :elapse 132, "timeTaken" "8469251", "contactsSortedCount" "2598514", "failedEventCount" "0", "successfulEventCount" "10943", "historyStoreRecordsFetched" "2133437", "historyStoreQueryCount" "10943"}]
                ]
      (view-stats-data data)))

  ([data]
    ; to draw multiple bins bar-chart, like x with year(08, 09) and y with 4 seasons.
    ; for categories seq, manually repeate years [08 09] for 4 times, [08(spr), 09(spr), 08(sum), 09(sum), ...]
    ; for values seq, manully give value for [08-spr, 09-spr, 08-sum, 09-sum, ...]
    ; for grp by, we have 4 seasons in each year, so repeat 2 times [spr, sum, fall, wint]
    (prn data) ; data in the following 
    (let [ds (to-dataset data)]
      (with-data ds     ; set $data to ds
        ;(view $data) 
        (let [runs (count data)
              ts ($ :timestamp $data)
              tm ($ :elapse $data)
              ks (keys (first data))   ; keys
              fetched ($ :historyStoreRecordsFetched $data)
              succ ($ :successfulEventCount $data)
              fail ($ :failedEventCount $data)
              grp (apply mapcat vector (repeat runs ["time" "hsrecords(m)" "successEvent(k)" "failEvent"]))  ; 3 tests, on grp, repeat type
              tests (mapcat identity (repeat 4 ts))  ; each test, show 4 result, elapse, fetched, succ, fail
              ;vals (mapcat vector e r)    ; map take 1st of e, r, apply vector, concat with 2nd of e, r
              vals (concat tm 
                           (map #(-> % (read-string) (/ 1000000)) fetched) ; do not use quot
                           (map #(-> % (read-string) (quot 1000)) succ) 
                           (map read-string fail))  ;elapse of test [1 2 3] followed by records
              
              chart (bar-chart tests vals ;:test-time :elapse-time
                         :group-by grp
                         :title "Trigger task runs"
                         :x-label "test"
                         :y-label "times, historystore records(k), events"
                         :legend true)]
          (view chart)
          (save chart "/opt/haijin/svn/wm/project/vci_scale/test/x.png" :width 1200 :height 900))))))


(defn plot-hs-data
  ; scatter plot history data
  ([]   ; no data arg, cock data by myself.
    (let [data (for [d (range 10)] (rand-int 20))
          datfile "/tmp/x"]   ; take 10 rand points within 20
      (spit datfile (clojure.string/join "\n" data))
      (plot-hs-data datfile)))

  ([datfile]
    ; each line in dat file is a single value data point
    ; (prn datfile) ; data in the following 
    (let [ds (read-dataset datfile)]
      (with-data ds     ; set $data to ds, col name becomes :colN
        ;(view $data) 
        (let [rows (count ($ :col0 ds))
              fcol (filter #(< % 5000) ($ :col0 ds))  ; only plot anything within 5 seconds
              x (range rows)
              chart (scatter-plot x fcol
                              :title "history-store-query" :x-label "call" :y-label "time(ms)" 
                              :legend false)]
          (view chart)
          (save chart (str datfile ".png") :width 1200 :height 900))))))