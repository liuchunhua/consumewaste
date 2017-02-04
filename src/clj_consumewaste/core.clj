(ns clj-consumewaste.core
  (:use
   [clojure.string :as str  :only (join split)])
  (:require [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as j]
            [clojure.edn :as edn]
            [taoensso.carmine :as car :refer (wcar)]
            [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]
            [clojure.data.csv :as csv]
            [clojure.tools.logging :as log])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource
           java.sql.SQLException
           (org.apache.lucene.analysis.cn.smart SmartChineseAnalyzer)
           (org.apache.lucene.analysis CharArraySet TokenStream)
           (org.apache.lucene.analysis.tokenattributes OffsetAttribute)
           (org.apache.lucene.store FSDirectory RAMDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.index IndexWriterConfig IndexWriter IndexReader DirectoryReader)
           (org.apache.lucene.search IndexSearcher Query ScoreDoc TopDocs Sort SortField SortField$Type)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.document Document Field TextField StringField Field$Store)
           (java.nio.file Paths)
           (org.postgresql.geometric PGpoint PGpath))
  (:gen-class))

(def db {:classname "oracle.jdbc.OracleDriver"
         :subprotocol "oracle"
         :subname "thin:@10.180.227.25:1521/DEVDB"
         :user "sdhsums"
         :password "sdhsums"})

(def pg-db {:classname "org.postgresql.Driver"
            :subprotocol "postgresql"
            :subname "//10.180.29.35:5432/etc"
            :dbtype "postgresql"
            :dbname "etc"
            :host "localhost"
            :user "analysis"
            :password "data"
            })

(defn db_pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60)))]
    {:datasource cpds}))

(def pooled-db (delay (db_pool pg-db)))

(defn db-conn [] @pooled-db)

(def server2-conn {:pool {:max-total 8} :spec {:host "10.180.29.35" :port 6379}}) ; See `wcar` docstring for opts
(defmacro wcar* [& body] `(car/wcar server2-conn ~@body))

(defn round
  [n m]
  (let [factor (Math/pow 10.0 m)]
    (/ (Math/round (* n factor)) factor)))

(defn bd09-gcj02
  "百度坐标转高德坐标"
  [^floats [lng lat]]
  (let [PI 3.14159265358979324
        radians (/ (* PI 3000.0) 180)
        x (- lng 0.0065)
        y (- lat 0.006)
        z (- (Math/sqrt (+ (* x x) (* y y))) (* 0.00002 (Math/sin (* y radians))))
        theta  (- (Math/atan2 y x) (* 0.000003 (Math/cos (* x radians))))]
    [(round (* z (Math/cos theta)) 6)
     (round (* z (Math/sin theta)) 6)]))

(defn gcj02-bd09
  "高德转百度"
  [^floats [lng, lat]]
  (let [radians (/ (* Math/PI 3000.0) 180)
        z (+ (Math/sqrt (+ (* lng lng) (* lat lat))) (* 0.00002 (Math/sin (* lat radians))))
        theta (+ (Math/atan2 lat lng) (* 0.000003 (Math/cos (* lng radians))))]
    [(round (+ (* z (Math/cos theta)) 0.0065) 6)
     (round (+ (* z (Math/sin theta)) 0.006) 6)]))

(defn distance_pois
  "计算连个坐标之间的直线距离"
  [^floats [lng1,lat1] ^floats [lng2, lat2]]
  (let [radians (/ Math/PI 180)
        lat (* (/ (- lat1 lat2) 2) radians)
        lng (* (/ (- lng1 lng2) 2) radians)
        lat_1 (* lat1 radians)
        lat_2 (* lat2 radians)
        a (+ (Math/pow (Math/sin lat) 2) (* (Math/cos lat_1) (Math/cos lat_2) (Math/pow (Math/sin lng) 2)))
        c (* 2 (Math/atan2 (Math/sqrt a) (Math/sqrt (- 1 a))))]
    (* 6371.008 c)))
(defn carno_extrace
  "获取消费记录中所有的卡号"
  [start end]
  (j/query (db-conn) ["SELECT DISTINCT CARDNO FROM DA_ETC_CONSUME_PARSE where savetime > to_date(?,'YYYY-MM-DD') and savetime < to_date(?,'YYYY-MM-DD')" start end] {:row-fn :cardno :result-set-fn vec}))

(defn car_driver_record
  "获得车辆的行驶记录"
  [start end carno_coll]
  (let [params (join " ," (repeat (count carno_coll) "?"))
        sql (join ["select  CARDNO,ENPROVID||'0000' AS ENPROVID, ENPROVID||'|'||ENNETID||'|'||INSTATIONID||'|'||INSTATION AS INSTATION, INTIME,
                      EXPROVID||'|'||EXNETID||'|'||OUTSTATIONID||'|'||OUTSTATION AS UTSTATION, OUTTIME from DA_ETC_CONSUME_PARSE where cardno in ("
                   params
                   ") and savetime > to_date(?,'YYYY-MM-DD') and savetime < to_date(?,'YYYY-MM-DD')"
                   "and intime is not null and outtime is not null"
                   " order by INTIME"])]
    (do
      (try
        (->> (j/query (db-conn) (concat [sql] carno_coll, [start end]))
             (group-by :cardno)
             (vals))
        (catch Exception e (log/error e sql))
        (finally [])))))

(defn drive_intime
  [record]
  (->> record
       (map :intime)
       (map #(.getTime %))
       (map #(quot % 1000))))

(defn drive_outtime
  [record]
  (->> record
       (map :outtime)
       (map #(.getTime %))
       (map #(quot % 1000))))

(defn rest_time
  [record]
  (map #(- %1 %2) (rest (drive_intime record)) (drive_outtime record)))

(defn province_station
  "计算省界收费站,
  record 车辆行驶记录
  seconds 过省界的时间上线"
  [seconds record]
  (let [rest-time (rest_time record)
        is_province (map-indexed (fn [idx itm] [idx (and (> itm 0) (< itm seconds))]) rest-time);;停留时间小于预定时间则是省界收费站
]
    (->> is_province
         (r/filter (fn [[idx itm]] itm))
         (r/filter (fn [[idx _]] (not= (-> record (get idx) :enprovid) (-> record (get (inc idx)) :enprovid)))) ;'([instation, outstation],...)
         (r/map (fn [[idx _]] [(-> record (get idx) :outstation) (-> record (get (inc idx)) :instation)])) ;'([instation, outstation],...)
         (into []))))

(defn print_province_station
  [record seconds]
  (let [rest (rest_time record)
        is_province (map #(< % seconds) rest);;停留时间小于预定时间则是省界收费站
        num (count rest)]
    (doseq [[e i] (map list is_province (range num))]
      (when (true? e)
        (prn (-> record (get i) (select-keys [:outstation :outtime])))
        (prn (-> record (get (inc i)) (select-keys [:instation :intime])))))))

(defn merge-counts
  ([] {})
  ([& m] (apply merge-with + m)))

(defn parse_all_shengjie_sfz
  [seconds start end]
  (->> (carno_extrace start end)
       (partition-all 10)
       vec
       (r/mapcat (partial car_driver_record start end))
       (r/map (partial province_station seconds))
       (r/map frequencies)
       (r/fold merge-counts)))

(defn save-freq
  "保存省界收费站及频率"
  [f_name start end]
  (let [freq (parse_all_shengjie_sfz 100 start end)]
    (with-open [wr (io/writer f_name)]
      (doseq [[k v] (vec freq)]
        (.write wr (str k ":" v "\n"))))))

(defn gaode
  [key value]
  (let [col (split key #":")
        name-map (zipmap [:station :attr] (split (col 1) #"[()]"))
        lng (Double/valueOf (col 2))
        lat (Double/valueOf (col 3))]
    (merge value name-map {:lng lng,:lat lat})))

(defn redis-keyscan
  "Redis 扫描key"
  [pattern limit f]
  (loop [cursor 0]
    (let [[c coll] (wcar* (car/scan cursor :match pattern :count limit))]
      (when (not= "0" c)
        (f coll)
        (recur (Long/valueOf c))))))

(defn create-gaode-table
  "创建高德收费站表"
  []
  (j/db-do-commands (db-conn)
                    ["drop table gaode_station"
                     (j/create-table-ddl :gaode_station
                                         [[:id "VARCHAR2(16)"]
                                          [:station "VARCHAR2(64)"]
                                          [:attr "VARCHAR2(64)"]
                                          [:lng "NUMBER(16,6)"]
                                          [:lat "NUMBER(16,6)"]
                                          [:province "VARCHAR2(32)"]
                                          [:city "VARCHAR2(64)"]
                                          [:ad "VARCHAR2(64)"]
                                          [:address "VARCHAR2(80)"]
                                          [:pcode "VARCHAR2(8)"]
                                          [:citycode "VARCHAR2(8)"]
                                          [:adcode "VARCHAR2(8)"]
                                          [:tag "VARCHAR2(80)"]])
                     "create index IDX_GAODE_LNGLAT on gaode_station(lng,lat)"
                     "create index IDX_GAODE_PROVINCE on gaode_station(pcode,citycode)"]))

(defn create-baidu-table
  "创建百度收费站表"
  []
  (j/db-do-commands (db-conn)
                    ["drop table baidu_station"
                     (j/create-table-ddl :baidu_station
                                         [[:station "VARCHAR2(80)"]
                                          [:attr "VARCHAR2(64)"]
                                          [:lng "NUMBER(16,6)"]
                                          [:lat "NUMBER(16,6)"]
                                          [:g_lng "NUMBER(16,6)"]
                                          [:g_lat "NUMBER(16,6)"]
                                          [:province "VARCHAR2(32)"]
                                          [:pcode "VARCHAR2(8)"]
                                          [:city "VARCHAR2(64)"]
                                          [:address "VARCHAR2(128)"]
                                          [:tags "VARCHAR2(80)"]])
                     "create index IDX_BAIDU_LANLAT on baidu_station(lng,lat)"]))

(defn insert-gaode-data
  ([] 0)
  ([coll] (when-not (empty? coll) (j/insert-multi! (db-conn) :gaode_station coll)))
  ([i coll] (+ i (count (j/insert-multi! (db-conn) :gaode_station coll)))))

(defn insert-baidu-data
  ([] 0)
  ([i coll] (+ i (count (j/insert-multi! (db-conn) :baidu_station coll)))))

(defn count_num
  ([] 0)
  ([& m] (apply + m)))

(defn save-gaode
  []
  (->> (wcar* (car/keys "GAODE:*"))
       (partition-all 256)
       vec
       (r/mapcat (fn [keys] (map (fn [x y] [x y]) keys (wcar* (mapv car/hgetall* keys))))) ;[([k1,v1] [k2,v2],...),...]
       (r/foldcat)
       vec
       (r/map #(apply gaode %))
       (r/filter #(zero? (j/query (db-conn) ["select count(1) as num from GAODE_STATION where lng = ? and lat = ?" (:lng %) (:lat %)] {:row-fn :num :result-set-fn first})))
       (r/foldcat)
       (partition-all 20)
       vec
       (r/fold + insert-gaode-data)))

(defn save-gaode-to-db
  []
  (redis-keyscan "GAODE:*" 2000
                 (fn [keys]
                   (->> keys
                        ((fn [keys] (map (fn [x y] [x y]) keys (wcar* :as-pipeline (mapv car/hgetall* keys)))))
                        (r/map #(apply gaode %))
                        (r/filter #(zero? (j/query (db-conn) ["select count(1) as num from GAODE_STATION where lng = ? and lat = ?" (:lng %) (:lat %)] {:row-fn :num :result-set-fn first})))
                        (r/foldcat)
                        (insert-gaode-data)))))
(defn- convert-baidu-gaode
  [m]
  (let [{lng :lng lat :lat} m]
    (merge m (zipmap [:g_lng :g_lat] (bd09-gcj02 [lng lat])))))

(defn save-baidu
  []
  (->> (wcar* (car/keys "poi:*"))
       (partition-all 256)
       (mapcat (fn [keys] (map (fn [x y] [x y]) keys (wcar* (mapv car/hgetall* keys))))) ;[([k1,v1] [k2,v2],...),...]
       (partition-all 20)
       vec
       (r/map (fn [coll] (map #(apply gaode %) coll)))
       ;(r/map (fn [coll] (map convert-baidu-gaode coll)))
       (r/fold + insert-baidu-data)))

(defn write-gaode-poi
  "添加百度坐标对应的高德坐标"
  []
  (reduce +
          (->> (wcar* (car/keys "poi:*"))
               vec
               (r/map (fn [k] [k (split k #":")]))
               (r/map (fn [[k coll]] [k (zipmap [:g_lng :g_lat] (bd09-gcj02 (mapv #(Float/parseFloat %) (drop 2 coll))))]))
               (r/foldcat)
               (partition-all 256)
               (map (fn [coll] (wcar* (mapv #(car/hmset* (first %) (last %)) coll))))
               (map count))))

(def province {"江西省"	"360000",
               "北京市"	"110000",
               "海南省"	"460000",
               "广东省"	"440000",
               "黑龙江省" "230000",
               "广西壮族自治区"	"450000",
               "上海市"	"310000",
               "河北省"	"130000",
               "吉林省"	"220000",
               "河南省"	"410000",
               "宁夏回族自治区"	"640000",
               "四川省"	"510000",
               "天津市"	"120000",
               "内蒙古自治区"	"150000",
               "云南省"	"530000",
               "安徽省"	"340000",
               "福建省"	"350000",
               "山东省"	"370000",
               "甘肃省"	"620000",
               "贵州省"	"520000",
               "江苏省"	"320000",
               "重庆市"	"500000",
               "新疆维吾尔自治区"	"650000",
               "青海省"	"630000",
               "山西省"	"140000",
               "湖北省"	"420000",
               "湖南省"	"430000",
               "陕西省"	"610000",
               "辽宁省"	"210000",
               "浙江省"	"330000",
               "香港特别行政区"	"810000"})

(defn baidu-write-pcode
  "添加省份编码"
  []
  (->> (wcar* (car/keys "poi:*"))
       (partition-all 128)
       (r/map (fn [keys] (map (fn [x y] [x y]) keys (wcar* (mapv car/hgetall* keys))))) ;[([k1,v1] [k2,v2],...),...]
       (r/map (fn [coll] (map (fn [[k v]] [k (assoc v :pcode (province (v "province")))]) coll)))
       (r/map (fn [coll] (wcar* (mapv #(car/hmset* (first %) (last %)) coll))))
       (r/map count)
       (r/reduce + 0)))

(defn parse_poi
  [key]
  (mapv #(Float/parseFloat %) (drop 2 (split key #":"))))

(def stop-words
  (doto (CharArraySet. 8 true)
    (.add "收费站")
    (.add "方向")
    (.add "高速")))

(defn parse-station
  [word]
  (with-open [a (SmartChineseAnalyzer. stop-words) ts (.tokenStream a "station" word)]
    (let [offset (.addAttribute ts OffsetAttribute)]
      (.reset ts)
      (loop [c (.incrementToken ts)]
        (when c
          (prn (.toString offset))
          (recur (.incrementToken ts)))))))

(defn- db-gaode-station
  []
  (j/query (db-conn) ["select station,attr,lng,lat,pcode from gaode_station"]))
(defn- gaode-key
  [m]
  (join ":" ["GAODE"  (if (:attr m nil) (str (:station m) "(" (:attr m) ")") (:station m)) (:lng m) (:lat m)]))
(defn- db-baidu-station
  []
  (j/query (db-conn) ["select station,attr,lng,lat,pcode from baidu_station"]))
(defn- baidu-key
  [m]
  (join ":" ["poi"  (if (:attr m nil) (str (:station m) "(" (:attr m) ")") (:station m)) (:lng m) (:lat m)]))
(defn generate-station-index
  "建立站点中文索引"
  [file]
  (with-open [analyzer (SmartChineseAnalyzer. stop-words)
              directory (FSDirectory/open (Paths/get file))
              indexWriter (IndexWriter. directory (IndexWriterConfig. analyzer))]
    (let [gaode (db-gaode-station)]
      (doseq [c gaode]
        (.addDocument indexWriter
                      (doto (Document.)
                        (.add (StringField. "key" (gaode-key c) Field$Store/YES))
                        (.add (TextField. "station" (:station c) Field$Store/YES))
                        (.add (TextField. "attr" (if (:attr c) (:attr c) "") Field$Store/NO))
                        (.add (StringField. "pcode" (:pcode c) Field$Store/YES)))))
      ;; (doseq [c baidu]
      ;;   (.addDocument indexWriter
      ;;                 (doto (Document.)
      ;;                   (.add (StringField. "key" (baidu-key c) Field$Store/YES))
      ;;                   (.add (TextField. "station" (:station c) Field$Store/NO))
      ;;                   (.add (TextField. "attr" (if (:attr c) (:attr c) "") Field$Store/NO))
      ;;                   (.add (StringField. "pcode" (:pcode c) Field$Store/YES)))))
)))

(defn search-station-index
  "搜索站点名"
  [file word]
  (with-open [analyzer (SmartChineseAnalyzer. stop-words)
              indexReader (DirectoryReader/open (FSDirectory/open (Paths/get file)))]
    (let [searcher (IndexSearcher. indexReader)
          parse (QueryParser. "station" analyzer)
          query (.parse parse word)
          results (.search searcher query 10)
          hits (.-scoreDocs results)
          station (transient [])]
      (log/info (.toString query))
      (doseq [hit hits]
        (let [doc (.doc searcher (.-doc hit))
              score (.-score hit)]
          (conj! station {:score score, :key (.get doc "key") :pcode (.get doc "pcode")})))
      (persistent! station))))

(defn station-lucene-searcher
  [file] (IndexSearcher. (DirectoryReader/open (FSDirectory/open (Paths/get file)))))

(defn search-index
  [x]
  (search-station-index (java.net.URI. "file:///D:/lucene-index") x))

(defn genetater-index
  []
  (generate-station-index (java.net.URI. "file:///D:/lucene-index")))

(defn in-out-station
  []
  "SELECT ENPROVID||'0000' AS PCODE,INSTATION, OUTSTATION, avg((OUTTIME - INTIME)*24) AS SPENDTIME FROM DA_ETC_CONSUME_PARSE
where intime > to_date(?,'YYYY-MM-DD') and intime < to_date(?,'YYYY-MM-DD') GROUP BY ENPROVID,INSTATION, OUTSTATION")

(defn fix-station
  [name]
  (str (str/replace name #"收费站|站" "") "收费站"))

(defn gaode-station
  [station province entry]
  (j/with-db-connection [conn (db-conn)]
    (j/query conn ["select station, attr, lng, lat, pcode from gaode_station where station = ? and pcode = ? and (attr is null or attr not like '%'||?||'%')" (fix-station station) province entry])))

(defn baidu-station
  [station province]
  (j/with-db-connection [conn (db-conn)]
    (j/query conn ["select station, attr, g_lng as lng, g_lat as lat, pcode from baidu_station where station = ? and pcode = ?" (fix-station station) province])))

(defn save-station-poi
  "保存区间站点可能的坐标"
  [channel]
  (go (while true
        (let [condition (<! channel)
              spendtime (condition :spendtime)
              in_poi (condition :in_poi [])
              out_poi (condition :out_poi [])]
          (log/debug condition)
          (->> (for [in in_poi out out_poi] {:in in :out out})
               (map (fn [m] (assoc m :speed (if (zero? spendtime) 0 (/ (distance_pois (mapv (m :in) [:lng :lat]) (mapv (m :out) [:lng :lat])) spendtime)))))
               ;;(filter (fn [m] (< (m :speed) 120.0)))
               (assoc condition :road)
               ((fn [m] (dissoc m :out_poi :in_poi)))
               ((fn [m] (wcar* (car/set (join ":" (mapv condition [:pcode :instation :outstation])) m)))))))))

(defn- process-lucene-searchresult
  [searcher station province entry]
  (let [parse (QueryParser. "station" (SmartChineseAnalyzer. stop-words))
        query (.parse parse (str station "^2 attr:" station " -attr:" entry (when-not (nil? province) (str " +pcode:" province))))
        results (.search searcher query 10)
        hits (.-scoreDocs results)
        station (transient [])]
    (log/debug (.toString query))
    (doseq [hit hits]
      (let [doc (.doc searcher (.-doc hit))
            score (.-score hit)]
        (conj! station {:score score, :key (.get doc "key") :pcode (.get doc "pcode")})))
    (let [coll (persistent! station)
          max_score (:score (apply max-key :score (if (empty? coll) [{:score 0}] coll)))]
      (->> coll
           (r/filter #(= max_score (% :score)))
           (r/map #(merge % (gaode (:key %) {})))
           (r/map #(dissoc % :key))
           (into [])))))

(defn search-in-lucene
  "在lucene索引处理站点信息"
  [in-channel out-channel]
  (let [index_dir (if (= "Windows_NT" (System/getenv "os")) "file:///D:/lucene-index" "file:////home/etc/lucene-index")
        searcher (station-lucene-searcher (java.net.URI. index_dir))]
    (go (while true
          (try
            (let [condition (<! in-channel)
                  instation (condition :instation)
                  outstation (condition :outstation)
                  province (condition :pcode)
                  in_poi (condition :in_poi [])
                  out_poi (condition :out_poi [])
                  search (memoize process-lucene-searchresult)
                  inpoi (->> in_poi
                             ((fn [c] (if (seq c) c (search searcher instation province "出口"))))
                             ((fn [c] (if (> (:score (first c) 21) 20.0) c  (search searcher instation nil "出口")))))
                  outpoi (->> out_poi
                              ((fn [c] (if (seq c) c (search searcher outstation province "入口"))))
                              ((fn [c] (if (> (:score (first c) 21) 20.0) c  (search searcher outstation nil "入口")))))]
              (>! out-channel (assoc condition :in_poi inpoi
                                     :out_poi outpoi)))
            (catch Exception e (log/error e) []))))))

(defn search-gaode-station
  [in-channel out-channel final-channel]
  (let []
    (go (while true
          (try
            (let [condition (<! in-channel)
                  instation (condition :instation)
                  outstation (condition :outstation)
                  province (condition :pcode)
                  in_poi (condition :in_poi [])
                  out_poi (condition :out_poi [])
                  search (memoize gaode-station)
                  in_result (search instation province "出口")
                  out_result (search outstation province "入口")]
              (>! (if (or (empty? in_result) (empty? out_result)) out-channel final-channel)
                  (assoc condition :in_poi in_result :out_poi out_result)))
            (catch SQLException se (log/error se) []))))))

(defn search-baidu-station
  [in-channel out-channel]
  (let []
    (go (while true
          (let [condition (<! in-channel)
                instation (condition :instation)
                outstation (condition :outstation)
                province (condition :pcode)
                in_poi (condition :in_poi [])
                out_poi (condition :out_poi [])
                search (memoize baidu-station)]
            (try
              (>! out-channel (->> condition
                                  (#(if (seq in_poi) (assoc % :in_poi (search instation province)) condition))
                                  (#(if (seq out_poi) (assoc % :out_poi (search outstation province)) condition))))
              (catch SQLException se (j/print-sql-exception se))))))))

(defn process-station
  []
  (let [gaode-chan (chan 100)
        baidu-chan (chan 100)
        lucene-chan (chan 100)
        final-chan (chan 100)]
    (save-station-poi final-chan)
    (search-in-lucene lucene-chan final-chan)
    (search-baidu-station baidu-chan lucene-chan)
    (search-gaode-station gaode-chan baidu-chan final-chan)
    gaode-chan))

(defn hyv-speed
  [keys]
  (let [values (wcar* :as-pipeline (mapv #(car/get %) keys))]
    (->> values
         ;;(filter (fn [v] (> (.length (str (v :spendtime))) 6)))
         ((fn [vs] (wcar* (mapv #(car/del (join ":" (mapv % [:pcode :instation :outstation])) %) vs)))))))

;; 分析省界站
;; (defn -main
;;   [& args]
;;   (let [[file start end] args]
;;     (save-freq file start end)))

(defn oracle-count
  [sql & args]
  (j/with-db-connection [conn (db-conn)]
    (j/query conn (concat [(str "select count(1) as num from (" sql ")")] args) {:row-fn :num :result-set-fn first})))
(defn oracle-page-query
  [sql page pagesize & args]
  (let [page-sql (str "SELECT * FROM (SELECT A.*, ROWNUM RN FROM (" sql " ) A WHERE ROWNUM < " (* page pagesize) ") WHERE RN >=" (* (dec page) pagesize))]
    (j/with-db-connection [conn (db-conn)]
      (j/query conn (concat [page-sql] args)))))

(defn oracle-page-query-array
  [sql page pagesize & args]
  (let [page-sql (str "SELECT * FROM (SELECT A.*, ROWNUM RN FROM (" sql " ) A WHERE ROWNUM < " (* page pagesize) ") WHERE RN >=" (* (dec page) pagesize))]
    (j/with-db-connection [conn (db-conn)]
      (j/query conn (concat [page-sql] args) {:as-arrays? true :row-fn drop-last}))))

(defn- freq
  []
  (with-open [rd (io/reader "D:\\freq.txt")]
    (let [coll (transient [])
          s (fn [s] (zipmap [:enprovid :ennetid :instationid :instation] (split s #"\|")))
          o (fn [s] (zipmap [:exprovid :exnetid :outstationid :outstation] (split s #"\|")))
          in (fn [m] (j/insert! (db-conn) :province_station m))]
      (doall (map #(conj! coll (edn/read-string %)) (line-seq rd)))
      (->> (persistent! coll)
           (apply merge-with +)
           vec
           (map (fn [[k v]] (merge (o (first k)) (s (last k)) {:freq v})))
           (map in)
           count
           +))))

(defn search-station-poi
  "查询入站出站的可能坐标,并更新到redis"
  []
  (let [c (process-station)
        sql "select pcode,instation,outstation,spendtime from PCODE_INSTATION_OUTSTATION_7 where  ((inscore >0 and inscore < 20) or (outscore >0 and outscore < 20))"]
    (loop [page 1]
      (when-let [coll (seq (oracle-page-query sql page 2000))]
        (doseq [m coll]
          (when true;;(zero? (wcar* (car/exists (join ":" (mapv m [:pcode :instation :outstation])))))
            (log/debug (assoc m :spendtime (round (:spendtime m) 3))) (>!! c (assoc m :spendtime (round (:spendtime m) 3)))))
        (recur (inc page))))
    (close! c)
    (Thread/sleep 5000))
  ;; (let [pcode (vals province)]
  ;;   (doseq [p pcode]
  ;;     (redis-keyscan (join ":" [p "*"]) 300 (fn [keys] (wcar* (mapv #(car/del %) keys))))))
)

(defn save-station-path
  "抽取所有路径坐标"
  [& args]
  (with-open [writer (io/writer "poitopoi.txt")]
    (doseq [p (vals province)]
      (redis-keyscan (join ":" [p "*"]) 5000
                     (fn [keys]
                       (let [values (wcar* :as-pipeline (mapv #(car/get %) keys))
                             dc-keys (->> values
                                          (r/mapcat #(:road %))
                                          (r/map #(join ":" ["DC"
                                                             (get-in % [:in :lng])
                                                             (get-in % [:in :lat])
                                                             (get-in % [:out :lng])
                                                             (get-in % [:out :lat])]))
                                          (into []))
                             ext (wcar* :as-pipeline (mapv #(car/exists %) dc-keys))]
                         (doseq [m (map list dc-keys ext)]
                           (when (zero? (last m))
                             (log/debug (first m))
                             (.write writer (str (first m) "\n"))))))))))

(defn save-poi-of-stations
  "抽取出入站已确定坐标，用于计算路径"
  [sql file]
  (with-open [writer (io/writer file)]
    (loop [page 1]
      (when-let [coll (seq (oracle-page-query sql page 2000))]
        (doseq [m coll]
          (.write writer (join ":" (conj (map m [:inlng :inlat :outlng :outlat]) "DR")))
          (.write writer "\n"))
        (recur (inc page))))))

;; (defn save-another-redis
;;   [& args]
;;   (redis-keyscan "poi:*" 300
;;                  (fn [keys]
;;                    (let [values (wcar* :as-pipeline (mapv #(car/hgetall* %) keys))]
;;                      (wcarr* (mapv #(car/hmset* %1 %2) keys values))))))

(defn min-path
  [pcode instation outstation]
  (let [key (join ":" [pcode instation outstation])
        value (wcar* (car/get key))
        road (:road value)]
    (->> road
         (map (fn [m] (join ":" ["DC"
                                 (get-in m [:in :lng])
                                 (get-in m [:in :lat])
                                 (get-in m [:out :lng])
                                 (get-in m [:out :lat])])))
         ((fn [keys] (map (fn [x y] (assoc x :distance (if (nil? y) 0 (Double/valueOf y)))) road (wcar* :as-pipeline (mapv #(car/get %) keys)))))
         (sort-by :distance)
         (first))))

(defn road-path
  [cardno]
  (let [coll (j/query (db-conn) ["select ENPROVID||'0000' as pcode,INSTATION,OUTSTATION,intime from DA_ETC_CONSUME_PARSE where cardno = ? and intime > date'2016-07-01' and intime < date'2016-08-01' order by intime" cardno])
        path-search (memoize min-path)
        road (map (fn [m] (let [{pcode :pcode instation :instation outstation :outstation intime :intime} m] (path-search pcode instation outstation))) coll)]
    (with-open [writer (io/writer "road.csv")]
      (doseq [m road]
        (.write writer (join "," [(get-in m [:in :station]) (get-in m [:in :lng]) (get-in m [:in :lat]) (get-in m [:out :station]) (get-in m [:out :lng]) (get-in m [:out :lat]) "\n"]))))))

(defn update-min-route
  "获取出入站所有可能坐标间的最短距离"
  [& args]
  (let [sql "select pcode, instation, outstation from PCODE_INSTATION_OUTSTATION_7 where  (inscore >0 and inscore < 20) or (outscore >0 and outscore < 20)"
        count (oracle-count sql)
        page-count (inc (quot count 500))]
    (->> (range page-count)
         vec
         (r/mapcat #(oracle-page-query sql % 500))
         (r/foldcat)
         (r/map (fn [m] (merge m (min-path (:pcode m) (:instation m) (:outstation m)))))
         ;;(r/filter #(not (zero? (:distance % -1))))
         (r/map #(try (j/with-db-connection [db (db-conn)] (first (j/update! db :PCODE_INSTATION_OUTSTATION_7
                                                                             {:inname (get-in % [:in :station])
                                                                              :inattr (get-in % [:in :attr])
                                                                              :inlng (get-in % [:in :lng])
                                                                              :inlat (get-in % [:in :lat])
                                                                              :outname (get-in % [:out :station])
                                                                              :outattr (get-in % [:out :attr])
                                                                              :outlng (get-in % [:out :lng])
                                                                              :outlat (get-in % [:out :lat])
                                                                              :inscore (get-in % [:in :score] 0)
                                                                              :outscore (get-in % [:out :score] 0)
                                                                              :distance (:distance %)}
                                                                             ["pcode=? and instation=? and outstation=?" (:pcode %) (:instation %) (:outstation %)]))) (catch Exception e (log/error e) 0)))
         (r/fold +))))

(defn get-path-points-from-redis
 [key]
 (->> (wcar* (car/parse-raw (car/get key)))
      (io/input-stream)
      (java.util.zip.GZIPInputStream.)
      (slurp)))
(defn -main
  [& args]
  (update-min-route))

;;(with-open [out-file (io/writer "gaode.csv")] (let [data (j/query (db-conn) ["select g.ID,g.STATION,g.attr,'('||g.lng||','||g.lat||')' AS poi,g.province,g.city,g.ad,g.address,g.pcode,g.citycode,g.adcode,g.tag from gaode_station g"] {:as-arrays? true})] (csv/write-csv out-file data)))

;; (with-open [out-file (io/writer "in_out_station.csv")]
;;   (let [sql "select p.pcode,p.instation,p.outstation,p.spendtime,p.inname,p.inattr,'('||p.inlng||','||p.inlat||')' as in_poi,
;; p.outname,p.outattr,'('||p.outlng||','||p.outlat||')' as out_poi,p.distance,p.inscore,p.outscore,p.speed from pcode_instation_outstation_7 p
;; where p.inlng is not null and p.inlat is not null and p.outlng is not null and p.outlat is not null"
;;         total (oracle-count sql)
;;         pagesize 3000]
;;     (doseq [page (range 1 (+ 2 (quot total pagesize)))]
;;       (csv/write-csv out-file (rest (oracle-page-query-array sql page pagesize))))))


;; (with-open [out-file (io/writer "etc_consumewaste")]
;;   (let [sql "select a.*,b.userid,c.username from da_etc_consume_parse a
;; left join card_userid b on a.cardno = b.cardid 
;; left join etc_t_user c on b.userid = c.userid
;; where a.intime > date'2016-07-01' and a.intime < date'2016-08-01'"
;;         total (oracle-count sql)
;;         pagesize 10000]
;;     (doseq [page (range 1 (+ 2 (quot total pagesize)))]
;;       (csv/write-csv out-file (rest (oracle-page-query-array sql page pagesize))))))


(defn export-large-date
  [sql file]
  (with-open [out (io/writer file)]
    (let [fetchsize 10000]
      (j/with-db-transaction [conn (db-conn)]
        (j/query conn (j/prepare-statement (:connection conn)sql {:fetch-size fetchsize}) {:as-arrays? true :result-set-fn #(csv/write-csv out %)})))))

(defn export-etc-consumewaste-parse
  [file]
  (export-large-date "select a.*,b.userid,c.username from da_etc_consume_parse a
left join card_userid b on a.wasteid = b.wasteid 
left join etc_t_user c on b.userid = c.userid
where a.intime > date'2016-07-01' and a.intime < date'2016-08-01'" file))

(defn- parse-redis-dr-key
  [key]
  (let [a (split key #":")
        start (str "(" (a 1) "," (a 2) ")")
        end (str "(" (a 3) "," (a 4) ")")]
    {:start_poi (PGpoint. start) :end_poi (PGpoint. end)}))
(defn- parse-redis-path
  [v]
  {:route (try (PGpath. (str "[(" (str/replace v #";" "),(") ")]")) (catch Exception e (log/error v)))})

(defn save-road-path-to-pgsql
  []
  (let [not-exists? (fn [key] (zero? (j/query (db-conn) (concat ["select count(1) as num from road_path where start_poi = ? and end_poi = ?" ] (mapv (parse-redis-dr-key key) [:start_poi :end_poi]) {:row-fn :num :result-set-fn first}))))]
    (redis-keyscan "DR:*" 20000
                   (fn handle_keys [keys]
                     (->> keys
                          vec
                          (r/filter #(not-exists? %))
                          (r/map #(hash-map :key % :route (get-path-points-from-redis %)))
                          (r/map #(merge % (parse-redis-dr-key (:key %)) (parse-redis-path (:route %))))
                          (r/map #(dissoc % :key))
                          (r/map #(j/insert! (db-conn) :road_path %))
                          (r/fold + (fn ([] 0) ([x _] (inc x)))))))))
