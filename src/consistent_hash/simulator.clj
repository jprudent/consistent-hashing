(ns consistent-hash.simulator
  (:require [clojure.core.async :refer [chan pub sub >!! go go-loop <! >! timeout close! dropping-buffer alt!]]))

(def request-channel (chan (dropping-buffer 100)))
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

(defn browser-request [url]
  (let [transaction-id (make-transation-id)
        response-channel (subscribe pub-response-channel transaction-id)]
    (>!! request-channel (->Request url transaction-id))
    (go
      (let [response (alt!
                       (timeout 2000) :timed-out
                       response-channel ([response] response))]
        (println response))
      (close! response-channel))))

(defn server []
  (go-loop [{:keys [transaction-id url]} (<! request-channel)]
    (go (<! (timeout 1000))
        (>! response-channel (->Response (str "data" transaction-id) transaction-id)))
    (recur (<! request-channel))))
