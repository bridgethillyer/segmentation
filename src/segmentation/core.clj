(ns segmentation.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))


(defn filter-by-gender [gender segment]
  (filter #(= gender (:gender %)) (vector segment)))

(def workflow
  [[:in :filter-by-gender]
   [:filter-by-gender :group-dmas]
   [:group-dmas :out]])

;;; Use core.async for I/O
(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :filter-by-gender
    :onyx/fn :segmentation.core/filter-by-gender
    :onyx/type :function
    :segmentation.core/gender 0
    :onyx/params [:segmentation.core/gender]
    :onyx/batch-size batch-size}

   {:onyx/name :group-dmas
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/group-by-key :dma-id
    :onyx/flux-policy :kill
    :onyx/min-peers 1
    :onyx/batch-size 1000}
   
   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def windows
  [{:window/id :dma-grouper
    :window/task :group-dmas
    :window/type :fixed
    :window/aggregation :onyx.windowing.aggregation/conj
    :window/window-key :event-time
    :window/range [5 :minutes]}])

(def triggers
  [{:trigger/window-id :dma-grouper
    :trigger/refinement :onyx.refinements/accumulating
    :trigger/on :onyx.triggers/segment 
    :trigger/threshold [5 :elements]
    :trigger/sync ::dump-window!}])

(defn dump-window! [event window-id lower-bound upper-bound state]
  (println (format "Window extent %s, [%s - %s] contents: %s"
                   window-id lower-bound upper-bound state)))

(def input-segments
  [{:person-id 1000 :gender 0 :dma-id 1 :event-time 0}
   {:person-id 1001 :gender 0 :dma-id 2 :event-time 0}
   {:person-id 1002 :gender 1 :dma-id 1 :event-time 0}
   {:person-id 1003 :gender 0 :dma-id 1 :event-time 0}
   {:person-id 1004 :gender 1 :dma-id 3 :event-time 0}
   {:person-id 1005 :gender 1 :dma-id 3 :event-time 0}
   {:person-id 1006 :gender 0 :dma-id 3 :event-time 0}
   {:person-id 1007 :gender 0 :dma-id 3 :event-time 0}
   {:person-id 1008 :gender 1 :dma-id 3`:event-time 0}
   :done])

(doseq [segment input-segments]
  (>!! input-chan segment))

;; The core.async channel to be closed when using batch mode,
;; otherwise an Onyx peer will block indefinitely trying to read.
(close! input-chan)

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx.bookkeeper/server? true
   :onyx.bookkeeper/local-quorum? true
   :onyx.bookkeeper/local-quorum-ports [3196 3197 3198]
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx/tenancy-id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers (count (set (mapcat identity workflow))))

(def v-peers (onyx.api/start-peers n-peers peer-group))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan input-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :segmentation.core/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :segmentation.core/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(onyx.api/submit-job
 peer-config
 {:workflow workflow
  :catalog catalog
  :lifecycles lifecycles
  :windows windows
  :triggers triggers
  :task-scheduler :onyx.task-scheduler/balanced})

(onyx.plugin.core-async/take-segments! output-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)     
