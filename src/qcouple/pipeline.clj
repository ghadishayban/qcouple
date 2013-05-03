(ns qcouple.pipeline
  (:refer-clojure :exclude [->])
  (:import [com.lmax.disruptor EventHandler
            EventFactory EventTranslator
            RingBuffer]
           java.util.concurrent.Executors
           com.lmax.disruptor.util.Util
           [com.lmax.disruptor.dsl
            Disruptor
            EventHandlerGroup]))

(set! *warn-on-reflection* true)

(def default-size 64000)

;; TODO
;; there is a race condition, should that be unsynchronized-mutable?
;; binding-conveyance?
;; ^:side-effect handler
;; Box in java?
;; wait for sentinel handler necessary?

(defprotocol Assignable
  (assign [_ o])
  (retrieve [_]))

(deftype Box [^:unsynchronized-mutable obj]
  Assignable
  (assign [_ o]
    (set! obj o))
  (retrieve [_]
    obj))

(defn- event-handler [f]
  (reify EventHandler
    (onEvent [this e seq-num end-of-batch?]
      (assign e (f (retrieve e))))))

(def event-factory
  (reify EventFactory
   (newInstance [this]
     (Box. nil))))

#_(defn- translator [f]
  (reify EventTranslator
    (translateTo [this event seq-num])))

(defn- default-executor [num-threads]
  (Executors/newFixedThreadPool num-threads))

(defn ->
  [opts & consumers]
  (when-not (> (count consumers) 1)
    (throw (ex-info "Need at least one consumer fn in the chain" {})))
  (let [{:keys [ring-buffer-size executor] :or {ring-buffer-size default-size
                                                executor (default-executor 4)}}
        opts
        d (Disruptor. event-factory
                      (Util/ceilingNextPowerOfTwo ring-buffer-size)
                      executor)]
    (reduce (fn [^Disruptor disruptor [h1 h2]]
              (.handleEventsWith
                disruptor
                (into-array EventHandler h1))
              (.. disruptor
                  (after (into-array EventHandler h1))
                  (handleEventsWith (into-array EventHandler h2)))
              disruptor)
            d (->> consumers
                   (map (comp list event-handler))
                   (partition 2 1)))
    (.start d)))

(defn offer
  [^RingBuffer ring-buffer item]
  (let [next (.next ring-buffer)]
    (try
      (assign (.get ring-buffer next) item)
      (finally
        (.publish ring-buffer next)))))
