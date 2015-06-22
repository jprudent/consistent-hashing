(ns consistent-hash.core)

(defprotocol ConsistentHashRing
  "https://en.wikipedia.org/wiki/Consistent_hashing"
  (add-node [this node] "add node to the ring")
  (remove-node [this node] "remove node and its replicas of the ring")
  (find-node [this data] "find the node responsible of data"))

(defn- hash-replica [hash-fn node ^long i]
  (-> node
      (str (if (zero? i) "" (* 42 i)))         ; 42 add some entropy in case hash function is Object.hashCode
      hash-fn))

(defn- hash-replicas [hash-fn node ^long nb-replicas]
  (map #(hash-replica hash-fn node %) (range nb-replicas)))

(defn find-closest-key [xs h]
  (or (first (drop-while #(> h %) xs))
      (first xs)))

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
    (get hmap (find-closest-key (keys hmap) (hash-fn data)))))

(def chr
  (->CHR 5 #(.hashCode %) (sorted-map)))

