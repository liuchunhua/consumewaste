(ns clj-consumewaste.core
  (:use (incanter core io)
        [clojure.java.jdbc :exclude (resultset-seq)]
        [clojure.string :only (join)])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(def db {:classname "oracle.jdbc.OracleDriver"
         :subprotocol "oracle"
         :subname "thin:@10.180.227.25:1521/DEVDB"
         :user "sdhsums"
         :password "sdhsums"})

(defn pool
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

(def pooled-db (delay (pool db)))

(defn db-conn [] @pooled-db)

(def carnoes ["辽AJ3765", "鲁K6362J","鲁N35877","鲁RE6893","豫GB2909","浙F50739"])

(defn car_driver_record
  "获得车辆的行驶记录"
  [carno_coll]
  (let [params (join " ," (repeat (count carno_coll) "?"))
        sql (join ["select distinct CARNO, INSTATION, INTIME, OUTSTATION, OUTTIME from DA_ETC_CONSUMEWASTE_PARSE where carno in (" params ") and intime > date'2016-01-01' order by INTIME"])]
    (do
      (prn sql)
      (group-by :carno (query (db-conn) (concat [sql] carno_coll))))))

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
  (map #(Math/abs (- %1 %2)) (rest (drive_intime record)) (drive_outtime record)))

(defn province_station
  "计算省界收费站,
  record 车辆行驶记录
  seconds 过省界的时间上线"
  [record seconds]
  (let [rest (rest_time record)
        is_province (map #(< % seconds) rest);;停留时间小于预定时间则是省界收费站
        num (count rest)]
    (filter string? (map #(when %1 (%2 :outstation)) is_province record))))
(defn print_province_station
  [record seconds]
  (let [rest (rest_time record)
        is_province (map #(< % seconds) rest);;停留时间小于预定时间则是省界收费站
        num (count rest)]
    (doseq [[e i] (map list is_province (range num))]
      (when (true? e)
        (prn (-> record (get i) (select-keys [:outstation :outtime])))
        (prn (-> record (get (inc i)) (select-keys [:instation :intime])))))))
