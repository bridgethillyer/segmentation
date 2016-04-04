(ns segmentation.basic-test
  (:require [segmentation.core :refer :all] 
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer :all]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]))

(deftest test-basic-segmentation
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)]
    (with-test-env [test-env [4 env-config peer-config]]
      (let [_ (onyx.api/submit-job peer-config
                                   {:catalog catalog
                                    :workflow workflow
                                    :lifecycles lifecycles
                                    :windows windows
                                    :triggers triggers
                                    :task-scheduler :onyx.task-scheduler/balanced})
            results (take-segments! output-chan)]
        (is (= 6 (count results)))
        (is (= :done (last results)))))))
