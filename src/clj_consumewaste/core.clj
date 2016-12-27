(ns clj-consumewaste.core
  (:use
   [clojure.java.jdbc :exclude (resultset-seq)]
   [clojure.string :as string  :only (join split)])
  (:require [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [taoensso.carmine :as car :refer (wcar)]
            [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource
           (org.apache.lucene.analysis.cn.smart SmartChineseAnalyzer)
           (org.apache.lucene.analysis CharArraySet TokenStream)
           (org.apache.lucene.analysis.tokenattributes OffsetAttribute)
           (org.apache.lucene.store FSDirectory RAMDirectory)
           (org.apache.lucene.util Version)
           (org.apache.lucene.index IndexWriterConfig IndexWriter IndexReader DirectoryReader)
           (org.apache.lucene.search IndexSearcher Query ScoreDoc TopDocs Sort SortField SortField$Type)
           (org.apache.lucene.queryparser.classic QueryParser)
           (org.apache.lucene.document Document Field TextField StringField Field$Store)
           (java.nio.file Paths)))

(def db {:classname "oracle.jdbc.OracleDriver"
         :subprotocol "oracle"
         :subname "thin:@10.180.227.25:1521/DEVDB"
         :user "sdhsums"
         :password "sdhsums"})

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

(def pooled-db (delay (db_pool db)))

(defn db-conn [] @pooled-db)

(def server1-conn {:pool {} :spec {:host "10.180.29.70" :port 6379}}) ; See `wcar` docstring for opts
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

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

(defn carno_extrace
  "获取消费记录中所有的卡号"
  [start end]
  (query (db-conn) ["SELECT DISTINCT CARDNO FROM DA_ETC_CONSUME_PARSE where savetime > to_date(?,'YYYY-MM-DD') and savetime < to_date(?,'YYYY-MM-DD')" start end] {:row-fn :cardno :result-set-fn vec}))

(defn car_driver_record
  "获得车辆的行驶记录"
  [start end carno_coll]
  (let [params (join " ," (repeat (count carno_coll) "?"))
        sql (join ["select distinct CARDNO, INSTATION, INTIME, OUTSTATION, OUTTIME from DA_ETC_CONSUME_PARSE where cardno in ("
                   params
                   ") and savetime > to_date(?,'YYYY-MM-DD') and savetime < to_date(?,'YYYY-MM-DD')"
                   "and intime is not null and outtime is not null"
                   " order by INTIME"])]
    (do
      (try
        (->> (query (db-conn) (concat [sql] carno_coll, [start end]))
             (group-by :cardno)
             (vals))
        (catch Exception e (prn (.getNextException e)))
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
  (let [rest (rest_time record)
        is_province (map-indexed (fn [idx itm] [idx (and (> itm 0) (< itm seconds))]) rest);;停留时间小于预定时间则是省界收费站
        ]
    (->> is_province
         (filter (fn [[idx itm]] itm))
         (map (fn [[idx _]] [(-> record (get idx) :outstation) (-> record (get (inc idx)) :instation)])) ;'([instation, outstation],...)
)))

(defn print_province_station
  [record seconds]
  (let [rest (rest_time record)
        is_province (map #(< % seconds) rest);;停留时间小于预定时间则是省界收费站
        num (count rest)]
    (doseq [[e i] (map list is_province (range num))]
      (when (true? e)
        (prn (-> record (get i) (select-keys [:outstation :outtime])))
        (prn (-> record (get (inc i)) (select-keys [:instation :intime])))))))

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
  [f_name]
  (let [freq (parse_all_shengjie_sfz 100 "2016-07-01" "2016-08-01")]
    (with-open [wr (io/writer f_name)]
      (doseq [[k v] (vec freq)]
        (.write wr (str k ":" v "\n"))))))

(defn gaode
  [key value]
  (let [col (split key #":")
        name-map (zipmap [:station :attr] (split (col 1) #"[()]"))
        lng (Float/parseFloat (col 2))
        lat (Float/parseFloat (col 3))]
    (merge value name-map {:lng lng,:lat lat})))

(defn redis-keyscan
  "Redis 扫描key"
  [pattern limit]
  (loop [cursor 0
         result (transient [])]
    (let [[c coll] (wcar* (car/scan cursor :match pattern :count limit))]
      (if (= "0" c)
        (apply concat (persistent! (conj! result coll)))
        (recur c (conj! result coll))))))

(defn create-gaode-table
  "创建高德收费站表"
  []
  (db-do-commands (db-conn)
                  ["drop table gaode_station"
                   (create-table-ddl :gaode_station
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
  (db-do-commands (db-conn)
                  ["drop table baidu_station"
                   (create-table-ddl :baidu_station
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
  ([i coll] (+ i (count (insert-multi! (db-conn) :gaode_station coll)))))

(defn insert-baidu-data
  ([] 0)
  ([i coll] (+ i (count (insert-multi! (db-conn) :baidu_station coll)))))

(defn count_num
  ([] 0)
  ([& m] (apply + m)))

(defn save-gaode
  []
  (->> (wcar* (car/keys "GAODE:*"))
       (partition-all 256)
       (mapcat (fn [keys] (map (fn [x y] [x y]) keys (wcar* (mapv car/hgetall* keys))))) ;[([k1,v1] [k2,v2],...),...]
       (partition-all 20)
       vec
       (r/map (fn [coll] (map #(apply gaode %) coll)))
       (r/fold + insert-gaode-data)))

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

(defn -main
  [& args]
  (save-freq "freq.txt"))

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

(defn generate-station-index
  "建立站点中文索引"
  [file]
  (with-open [analyzer (SmartChineseAnalyzer. stop-words)
              directory (FSDirectory/open (Paths/get file))
              indexWriter (IndexWriter. directory (IndexWriterConfig. analyzer))]
    (let [gaode (query (db-conn) ["select station,attr,lng,lat,pcode from gaode_station"])
          baidu (query (db-conn) ["select station,attr,lng,lat,pcode from baidu_station"])]
      (doseq [c gaode]
        (.addDocument indexWriter
                      (doto (Document.)
                        (.add (StringField. "key" (join ":" ["GAODE"  (if (:attr c nil) (str (:station c) "(" (:attr c) ")") (:station c)) (:lng c) (:lat c)]) Field$Store/YES))
                        (.add (TextField. "station" (:station c) Field$Store/YES))
                        (.add (TextField. "attr" (if (:attr c) (:attr c) "") Field$Store/NO))
                        (.add (StringField. "pcode" (:pcode c) Field$Store/YES)))))
      (doseq [c baidu]
        (.addDocument indexWriter
                      (doto (Document.)
                        (.add (StringField. "key" (join ":" ["poi"  (if (:attr c nil) (str (:station c) "(" (:attr c) ")") (:station c)) (:lng c) (:lat c)]) Field$Store/YES))
                        (.add (TextField. "station" (:station c) Field$Store/NO))
                        (.add (TextField. "attr" (if (:attr c) (:attr c) "") Field$Store/NO))
                        (.add (StringField. "pcode" (:pcode c) Field$Store/YES))))))))

(defn search-station-index
  "搜索站点名"
  [file station]
  (with-open [analyzer (SmartChineseAnalyzer. stop-words)
              indexReader (DirectoryReader/open (FSDirectory/open (Paths/get file)))]
    (let [searcher (IndexSearcher. indexReader)
          parse (QueryParser. "station" analyzer)
          query (.parse parse (str station))
          results (.search searcher query 5)
          hits (.-scoreDocs results)]
      (prn (.toString query))
      (doseq [hit hits]
        (let [doc (.doc searcher (.-doc hit))
              score (.-score hit)
              explanation (.explain searcher query (.-doc hit))]
          (prn score)
          (prn (.get doc "key"))
          (prn (.get doc "pcode")))))))

(defn search-index
  [x]
  (search-station-index (java.net.URI. "file:///D:/lucene-index") x))

(defn genetater-index
  []
  (generate-station-index (java.net.URI. "file:///D:/lucene-index")))

(defn in-out-station
  [start end]
  (merge (query (db-conn) ["SELECT ENPROVID,INSTATION, OUTSTATION, avg((OUTTIME - INTIME)*24) AS SPENDTIME FROM DA_ETC_CONSUME_PARSE
where intime > to_date(?,'YYYY-MM-DD') and intime < to_date(?,'YYYY-MM-DD') GROUP BY ENPROVID,INSTATION, OUTSTATION" start end]) {:instation_poi [], :outstation_poi []}))

(defn fix-station
  [name]
  (str (string/replace name #"消费站|站" "") "收费站"))

(defn start-station-process
  [channel]
  (go (doseq [m (take 5 (in-out-station "2016-07-01" "2016-07-05"))]
        (>! channel m)))
  channel)

(defn redis-station
  [m]
  (let [coll (wcar* (car/keys (str "GAODE:" (fix-station (m "instation")) "*")) (car/keys (str "GAODE:" (fix-station (m "outstation")) "*")))]
    (merge m {:instation_poi (first coll) :outstation_poi (last coll)})))

(defn station-process
  [in]
  (let [channel (chan 10)]
    (go (while true (>! channel (redis-station (<! in)))))
    channel))

