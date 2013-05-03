(ns qcouple.pipeline
  (:refer-clojure :exclude [->])
  (:import [com.lmax.disruptor EventHandler EventFactory RingBuffer]
           [com.lmax.disruptor.dsl Disruptor EventHandlerGroup]  
           com.lmax.disruptor.util.Util
           java.util.concurrent.Executors))

(def ^:private default-size 64000)

;; TODO
;; ^:side-effect handler
;; Box in java?
;;
;; make options {} optional
;;
;; ^unsynchronized-mutable ok?
;; wait for sentinel handler necessary?
;; binding-conveyor-fn necessary?
;;
;; vectors should not set! the box
;; possibly macro-ize the consumers

(defprotocol Assignable
  (assign [_ o])
  (retrieve [_]))

(deftype Box [^:volatile-mutable obj]
  Assignable
  (assign [_ o]
    (set! obj o))
  (retrieve [_]
    obj))

(defn- event-handler
  [transform-fn]
  (let [f (#'clojure.core/binding-conveyor-fn transform-fn)]
    (reify EventHandler
      (onEvent [this e seq-num end-of-batch?]
        (println (retrieve e))
        (assign e (f (retrieve e)))))))

(def ^:private event-factory
  (reify EventFactory
   (newInstance [this]
     (Box. nil))))

(defn- default-executor [num-threads]
  (Executors/newFixedThreadPool num-threads))

(defn- parse-consumer-spec 
  [spec]
  (cond
    (fn? spec)
    (into-array EventHandler [(event-handler spec)])
    (vector? spec)
    (into-array EventHandler (map event-handler spec))))

(defn- initialize-disruptor
  [d specs]
  (loop [handlers (map parse-consumer-spec specs)
         group d]
    (when-let [current-handler (first handlers)]
      (.handleEventsWith group current-handler)
      (if (next handlers)
        (recur
          (next handlers)
          (.after d current-handler))))))

(defn ->
  "A function that takes a set of chained consumers that will run
  in parallel, with each other using the Disruptor pattern.
  
  Returns a queue that you can offer items to.

  Like clojure.core/->, the result of one function is passed as
  the input to the next. Consumers can be functions, or vectors of 
  functions.  Vectors of functions will run in parallel with each other,
  presumably for side-effects.

  Each function can take at most one argument.

  Required options map at the beginning specifies
  a size for the underlying ring-buffer, which will be rounded up to
  the next power of two.
  
  Respects bound vars from the initial thread
  
  (let [results (atom []))
        queue (-> {:ring-buffer-size 100000} 
                   read-string
                   inc
                   pr-str
                   #(assoc {} :result %)
                   #(swap results conj %))]
    (doseq [x [\"1\" \"3\" \"2\" \"5\"]]
      (offer queue x))
    @results)
  "
  [opts & consumers]
  (when-not (> (count consumers) 1)
    (throw (ex-info "Need at least one consumer fn in the chain" {})))
  (let [{:keys [ring-buffer-size executor] :or {ring-buffer-size default-size
                                                executor (default-executor 4)}}
        opts
        d (Disruptor. event-factory
                      (Util/ceilingNextPowerOfTwo ring-buffer-size)
                      executor)]
    (initialize-disruptor d consumers) 
    (.start d)))

(defn offer
  "Offers a value to the chained consumers consumption"
  [^RingBuffer ring-buffer item]
  (let [next (.next ring-buffer)]
    (try
      (assign (.get ring-buffer next) item)
      (finally
        (.publish ring-buffer next)))))
