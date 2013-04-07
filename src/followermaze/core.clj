(ns followermaze.core
  (:import (java.net Socket ServerSocket))
  (:import (java.io InputStreamReader BufferedReader OutputStreamWriter BufferedWriter))
  (:require [clojure.string :as str])
  (:gen-class))

(def settings {:client-port 9099 :server-port 9090})

(defn get-reader [socket]
  (BufferedReader. (InputStreamReader. (.getInputStream socket))))

(defn get-writer [socket]
  (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket))))

(defn vals-in [coll ks]
  (vals (select-keys coll ks)))

(defn sequential-sort [unordered indexer first-id]
  (let [inner-sort (fn inner-sort [unordered backlog next-id]
          (if (= next-id (indexer (first backlog)))
            (cons
              (first backlog)
              (lazy-seq (inner-sort unordered (rest backlog) (inc next-id))))
            (recur (rest unordered) (sort-by indexer (cons (first unordered) backlog)) next-id)))]
    (inner-sort unordered nil first-id)))

(defrecord Event [id type from to payload])

(def clients (atom {}))
(def followings (atom {}))

(defn parse-event [payload]
  (let [[id type from to] (str/split payload #"\|")]
    (Event. (Integer. id) type from to payload)))

(defn unordered-events[event-stream]
  (let [payload (.readLine event-stream)]
    (if-not (empty? payload)
      (cons (parse-event payload) (lazy-seq (unordered-events event-stream)))
      ())))

(defn ordered-events [event-stream]
  (sequential-sort (unordered-events event-stream) #(:id %) 1))

(defmulti event-recipients #(:type %))

(defmethod event-recipients "F" [{followed :to}]
  (vals-in @clients [followed]))

(defmethod event-recipients "U" [_]
  ())

(defmethod event-recipients "B" [_]
  (vals @clients))

(defmethod event-recipients "P" [{recipient :to}]
  (vals-in @clients [recipient]))

(defmethod event-recipients "S" [{broadcaster :from}]
  (vals-in @clients (@followings broadcaster)))

(defn notify-client [conn {payload :payload}]
  (doto (get-writer conn)
    (.write payload) (.newLine) (.flush)))

(defn edit-followers [followings follower-id client-id f]
  (assoc followings client-id (f (followings client-id #{}) follower-id)))

(defn add-follower [followings follower-id client-id]
  (edit-followers followings follower-id client-id conj))

(defn remove-follower [followings follower-id client-id]
  (edit-followers followings follower-id client-id disj))

(defmulti update-state #(:type %))

(defmethod update-state "F" [{follower-id :from client-id :to}]
  (swap! followings add-follower follower-id client-id))

(defmethod update-state "U" [{follower-id :from client-id :to}]
  (swap! followings remove-follower follower-id client-id))

(defmethod update-state :default [_])

(defn process-event [event]
  (update-state event)
  (dorun (map #(notify-client % event) (event-recipients event))))

(defn listen-event-source [port]
  (with-open [socket       (new ServerSocket port)
              event-socket (.accept socket)
              reader       (get-reader event-socket)]
    (dorun (map process-event (ordered-events reader)))))

(defn listen-clients [port]
  (with-open [socket (new ServerSocket port)]
    (dorun
      (repeatedly (fn []
        (let [client-socket (.accept socket)
              reader        (get-reader client-socket)
              id            (.readLine reader)]
          (swap! clients assoc id client-socket)))))))

(defn -main [& args]
  (def client-worker (future (listen-clients      (:client-port settings))))
  (def event-worker  (future (listen-event-source (:server-port settings))))
  (println "Started...")
  @client-worker @event-worker)
