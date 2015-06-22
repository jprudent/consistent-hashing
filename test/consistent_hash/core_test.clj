(ns consistent-hash.core-test
  (:require [clojure.test :refer :all]
            [consistent-hash.core :refer :all]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]))

(defn hashcode [o] (.hashCode o))

(defn empty-chr [nb-replicas]
  (->CHR nb-replicas hashcode (sorted-map)))

(defspec adding-nodes-should-add-replicas
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int]
                       (let [chr (empty-chr nb-replicas)
                             chr-added (add-node chr node)]
                         (= (count (:hmap chr-added)) nb-replicas))))

(defspec adding-a-node-twice-has-no-effect-on-the-ring
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int
                        times gen/s-pos-int]
                       (let [chr (empty-chr nb-replicas)
                             chr-added-once (add-node chr node)
                             chr-added-many-times (reduce #(add-node %1 %2) chr (repeat times node))]
                         (= chr-added-once chr-added-many-times))))

(defspec adding-then-removing-a-node-results-in-empty-ring
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int]
                       (let [chr (empty-chr nb-replicas)
                             chr-added (add-node chr node)
                             chr-removed (remove-node chr-added node)]
                         (= chr chr-removed))))

(defspec can-find-back-any-node-of-the-ring
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int]
                       (let [chr (-> (empty-chr nb-replicas)
                                     (add-node node))]
                         (= node (find-node chr node)))))

(defspec there-is-always-a-node-for-a-given-data
         100
         (prop/for-all [nodes (gen/not-empty (gen/vector (gen/not-empty gen/string)))
                        nb-replicas gen/s-pos-int
                        data (gen/not-empty gen/string)]
                       (let [chr (reduce #(add-node %1 %2) (empty-chr nb-replicas) nodes)]
                         (not (nil? (find-node chr data))))))

(defspec finding-node-in-ring-is-a-pure-function
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int
                        data (gen/not-empty gen/string)]
                       (let [chr (-> (->CHR nb-replicas #(.hashCode %) (sorted-map))
                                     (add-node node))]
                         (= (find-node chr data) (find-node (-> (remove-node chr node)
                                                                (add-node node)) data)))))
