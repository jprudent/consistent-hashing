(ns consistent-hash.core-test
  (:require [clojure.test :refer :all]
            [consistent-hash.core :refer :all]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]))

(defspec adding-nodes-should-add-replicas
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int]
                       (let [chr (->CHR nb-replicas #(.hashCode %) (sorted-map))
                             chr-added (add-node chr node)]
                         (= (count (:hmap chr-added)) nb-replicas))))

(defspec adding-a-node-twice-has-no-effect-on-the-ring
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int]
                       (let [chr (->CHR nb-replicas #(.hashCode %) (sorted-map))
                             chr-added-once (add-node chr node)
                             chr-added-twice (add-node chr-added-once node)]
                         (= chr-added-once chr-added-twice))))

(defspec adding-then-removing-a-node-results-in-empty-ring
         100
         (prop/for-all [node (gen/not-empty gen/string)
                        nb-replicas gen/s-pos-int]
                       (let [chr (->CHR nb-replicas #(.hashCode %) (sorted-map))
                             chr-added (add-node chr node)
                             chr-removed (remove-node chr-added node)]
                         (= chr chr-removed))))
