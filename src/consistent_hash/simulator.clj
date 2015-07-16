(ns consistent-hash.simulator
  (:require [clojure.core.async :refer [chan pub sub >!! go go-loop <! >! timeout close! dropping-buffer alt!]]
            [incanter.core :refer :all]
            [incanter.stats :refer :all]
            [incanter.charts :refer :all]
            [incanter.io :refer :all]))

(def request-channel (chan (dropping-buffer 10000)))
(def response-channel (chan 100))
(def pub-response-channel (pub response-channel :transaction-id))
(def transaction-id (atom 0))

(defrecord Request [url transaction-id])
(defrecord Response [data transaction-id])

(defn make-transation-id []
  (swap! transaction-id inc))

(defn subscribe [pub transaction-id]
  (let [resp-chan (chan)]
    (sub pub transaction-id resp-chan)
    resp-chan))

(defn browser-request [url stats-updater!]
  (let [transaction-id (make-transation-id)
        response-channel (subscribe pub-response-channel transaction-id)
        sent-date (System/currentTimeMillis)]
    (>!! request-channel (->Request url transaction-id))
    (let [start-time (System/nanoTime)]
      (go
        (let [response (alt!
                         (timeout 10000) :timed-out
                         response-channel ([response] response))
              stat [sent-date (* (- (System/nanoTime) start-time) 1e-9) url]]
          (stats-updater! stat)
          (println response))
        (close! response-channel)))))

(defn server []
  (go-loop [{:keys [transaction-id url]} (<! request-channel)]
    (<! (timeout 250))
    (>! response-channel (->Response url transaction-id))
    (recur (<! request-channel))))

(defonce stats (atom []))
(defonce conj-stats (partial swap! stats conj))


(defn view-stats []
  (view
    (let [ds (dataset ["time" "response-time" "key"] @stats)
          [last-x last-y] (some (fn [[_ y :as p]] (when (>= y 10) p)) @stats)]
      (doto
        (scatter-plot :time :response-time
                      :data ds
                      :group-by :key
                      :x-label "Time (GMT)" :y-label "Response time (seconds)" )
        (add-lines :time :response-time :data ds)
        (add-pointer last-x last-y :text "Request timeout")))))

(defn ddos [urls stats-updater! nb-requests time-span-ms]
  (let [waiting (/ time-span-ms nb-requests)]
    (go-loop [nb-requests nb-requests urls urls]
      (when (pos? nb-requests)
        (<! (timeout waiting))
        (browser-request (first urls) stats-updater!)
        (recur (dec nb-requests) (rest urls))))))

(comment
  (reset! stats [])
  (server)
  (ddos (cycle (map #(str "/user/" %) (range 10))) conj-stats 100 10000))


