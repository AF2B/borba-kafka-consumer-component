(ns borba.kafka-consumer.logging
  "A logger that keeps what it is given, so a test can say what a service
   logged without a logging backend on the classpath."
  (:require
   [clojure.tools.logging :as log]
   [clojure.tools.logging.impl :as impl]))

(defn- logger
  [entries]
  (reify impl/Logger
    (enabled? [_ _level] true)
    (write! [_ level throwable message]
      (swap! entries conj {:level level :message message :cause throwable}))))

(defn call-capturing
  "Calls a function with logging captured, and returns the entries, each a
   map of :level, :message and :cause, in the order they were logged.
   - f: a function of no arguments"
  [f]
  (let [entries (atom [])
        factory (reify impl/LoggerFactory
                  (name [_] "borba.kafka-consumer.logging")
                  (get-logger [_ _logger-ns] (logger entries)))]
    (binding [log/*logger-factory* factory]
      (f))
    @entries))

(defn messages
  "Returns only the messages of the entries.
   - entries: the entries a capture returned"
  [entries]
  (mapv :message entries))
