(ns clj-consumewaste.core
  (:use
   [clojure.java.jdbc :exclude (resultset-seq)]
   [clojure.string :only (join split)])
  (:require [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [taoensso.carmine :as car :refer (wcar)])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

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

(defn carno_extrace
  "获取消费记录中所有的卡号"
  []
  (query (db-conn) ["SELECT DISTINCT CARDNO FROM DA_ETC_CONSUME_PARSE where savetime > date'2016-07-01' and savetime < date'2016-08-01' and rownum < 200"] {:row-fn :cardno :result-set-fn vec}))

(defn car_driver_record
  "获得车辆的行驶记录"
  [carno_coll]
  (let [params (join " ," (repeat (count carno_coll) "?"))
        sql (join ["select distinct CARDNO, INSTATION, INTIME, OUTSTATION, OUTTIME from DA_ETC_CONSUME_PARSE where cardno in ("
                   params
                   ") and savetime > date'2016-07-01' and savetime < date'2016-08-01' "
                   "and intime is not null and outtime is not null"
                   " order by INTIME"])]
    (do
      (try
        (->> (query (db-conn) (concat [sql] carno_coll))
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
  [record seconds]
  (let [rest (rest_time record)
        is_province (map-indexed (fn [idx itm] [idx (and (> itm 0) (< itm seconds))]) rest);;停留时间小于预定时间则是省界收费站
        num (count rest)]
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
  []
  (->> (carno_extrace)
       (partition-all 10)
       vec
       (r/mapcat car_driver_record)
       (r/map #(province_station % 300))
       (r/map frequencies)
       (r/fold merge-counts)))

(defn save_freq
  "保存省界收费站及频率"
  [f_name]
  (let [freq (parse_all_shengjie_sfz)]
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
(defn insert-gaode-data
  ([] 0)
  ([i coll] (+ i (count (insert-multi! (db-conn) :gaode_station coll)))))

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
       (r/map (fn [coll] (prn (count coll))(map #(apply gaode %) coll)))
       (r/fold + insert-gaode-data)
       ))

(defn -main
  [& args]
  (create-gaode-table)
  (save-gaode))
