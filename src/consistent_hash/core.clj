(ns consistent-hash.core)

(defprotocol ConsistentHashRing
  "https://en.wikipedia.org/wiki/Consistent_hashing"
  (add-node [this node] "add node to the ring")
  (remove-node [this node] "remove node and its replicas of the ring")
  (find-node [this data] "find the node responsible of data"))

(defn- hash-replica [hash-fn node ^long i]
  (-> node
      (str (* 42 i))         ; add some entropy in case hash function is Object.hashCode
      hash-fn))

(defn- hash-replicas [hash-fn node ^long nb-replicas]
  (map #(hash-replica hash-fn node %) (range nb-replicas)))

(defrecord CHR [nb-replicas hash-fn hmap]
  ConsistentHashRing
  (add-node [{:keys [nb-replicas hash-fn] :as chr} node]
    (update-in chr [:hmap]
                #(apply assoc % (interleave
                                   (hash-replicas hash-fn node nb-replicas)
                                   (cycle [node])))))

  (remove-node [{:keys [nb-replicas hash-fn] :as chr} node]
    (update-in chr [:hmap]
               #(apply dissoc % (hash-replicas hash-fn node nb-replicas))))

  (find-node [{:keys [hash-fn hmap]} data]
    (loop [[x & rest :as xs] (keys hmap)
           i (hash-fn data)]
      (if (empty? xs)
        nil
        (if (>= i x)
          x
          (recur rest i))))))

(def chr
  (->CHR 5 #(.hashCode %) (sorted-map)))

