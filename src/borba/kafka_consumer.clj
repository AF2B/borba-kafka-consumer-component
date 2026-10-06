(ns borba.kafka-consumer
  "Kafka consumer for a service: an Integrant component that reads topics and
   gives each message to a handler.

     :kafka/consumer-handlers
     {\"order-events\" com.example.orders.events/handle-order-event}

     :components/kafka-consumer
     {:bootstrap-servers #or [#env KAFKA_BROKERS \"localhost:9092\"]
      :group-id          \"orders-service\"
      :topics            [\"order-events\"]
      :handlers          #ig/ref :kafka/consumer-handlers}

   A handler is a function of a message, a map of :key, :value (the JSON, as
   data), :topic, :partition, :offset, :timestamp and :headers. It is given the
   messages of a partition in order, one at a time.

   Delivery is at-least-once. The consumer does not commit offsets by itself: it
   commits the offset of a message when the handler is done with it, so a
   message that was in hand when the process died is given again. A handler
   must therefore be safe to run twice for a message.

   A handler that throws is tried again, `:max-attempts` times, a moment apart.
   A message that is still not handled, or whose value is not valid JSON, is
   reported to `:on-error`, a function of its failure, which can write it to a
   dead-letter topic, and then it is past. When there is no `:on-error` it is
   logged, with where it was and never what it said, and skipped. When
   `:on-error` itself throws the message is not past: the consumer goes back to
   it, so a dead-letter topic that is away does not make the messages vanish.

   A stop finishes the message in hand and commits what is done."
  (:require
   [borba.kafka-consumer.config :as config]
   [borba.kafka-consumer.message :as message]
   [clojure.tools.logging :as log]
   [integrant.core :as ig])
  (:import
   (java.time Duration)
   (java.util.concurrent TimeUnit)
   (org.apache.kafka.clients.admin Admin AdminClient AdminClientConfig)
   (org.apache.kafka.clients.consumer
    Consumer ConsumerRecord KafkaConsumer OffsetAndMetadata)
   (org.apache.kafka.common KafkaFuture TopicPartition)
   (org.apache.kafka.common.errors InterruptException WakeupException)))

(set! *warn-on-reflection* true)

(def default-verify-timeout-ms
  "How long the start waits for the brokers to answer, unless told otherwise."
  5000)

(def ^:private poll-failure-pause-ms 1000)

;; Handling a message

(defn- sleep
  "Waits, and returns false when the thread was interrupted."
  [milliseconds]
  (try (Thread/sleep (long milliseconds))
       true
       (catch InterruptedException _
         false)))

(defn- try-handler
  "Calls the handler of a message. Returns the failure of the attempt, or nil
   when the handler was done."
  [handler
   parsed
   attempt]
  (try (handler parsed)
       nil
       (catch Exception cause
         (message/failure parsed :handler-failed attempt cause))))

(defn- deliver!
  "Gives a message to its handler, up to max-attempts times, and returns :done,
   or the failure of the last attempt, or :stopped when the consumer was
   stopped before the message was handled."
  [handler
   parsed
   {:keys [max-attempts retry-backoff-ms running?]}]
  (loop [attempt 1]
    (let [failed (try-handler handler parsed attempt)]
      (cond
        (nil? failed)
        :done

        (>= attempt max-attempts)
        failed

        (not (and (running?) (sleep retry-backoff-ms)))
        :stopped

        :else
        (recur (inc attempt))))))

(defn- report!
  "Tells what became of a message that could not be handled. Returns true when
   the message is past, and false when it must be given again."
  [failed
   {:keys [on-error]}]
  (let [{:keys [error topic attempts offset]
         partition-number :partition} failed]
    (log/errorf (:cause failed)
                "kafka message skipped (%s) after %d attempt(s): %s-%d at %d"
                (name error) attempts topic partition-number offset)
    (if-not on-error
      true
      (try (on-error failed)
           true
           (catch Exception cause
             (log/errorf cause
                         "on-error failed for %s-%d at %d: it is read again"
                         topic partition-number offset)
             false)))))

(defn- process-record!
  "Handles a record of the consumer. Returns :done when the message is past,
   :again when it must be read again, and :stopped when the consumer stopped
   before it was handled."
  [^ConsumerRecord record
   handlers
   settings]
  (let [raw     (message/raw-message record)
        handler (get handlers (:topic raw))
        parsed  (message/parse raw)]
    (cond
      (nil? handler)
      (do (log/errorf "kafka message of a topic without a handler: %s-%d at %d"
                      (:topic raw) (:partition raw) (:offset raw))
          :done)

      (message/failure? parsed)
      (if (report! parsed settings) :done :again)

      :else
      (let [outcome (deliver! handler parsed settings)]
        (cond
          (= :done outcome)    :done
          (= :stopped outcome) :stopped
          :else                (if (report! outcome settings) :done :again))))))

;; The loop

(defn- topic-partition-of
  ^TopicPartition [^ConsumerRecord record]
  (TopicPartition. (.topic record) (.partition record)))

(defn- offset-of
  "Returns the partition of a record and the offset that is past it."
  [^ConsumerRecord record]
  [(topic-partition-of record)
   (OffsetAndMetadata. (inc (.offset record)))])

(defn- commit!
  "Commits the offsets of the messages that are past. A commit that fails, as
   when a rebalance took the partitions, is logged: the messages are read
   again, which is what at-least-once means."
  [^Consumer consumer
   done]
  (when (seq done)
    (try
      (.commitSync consumer ^java.util.Map done)
      (catch WakeupException _
        (.commitSync consumer ^java.util.Map done))
      (catch Exception cause
        (log/warn cause "kafka commit failed; the messages are read again")))))

(defn- process-batch!
  "Handles the records of a poll in order, and commits the offsets of the ones
   that are past. Stops at a message that must be read again, which it seeks
   back to, or when the consumer is stopped."
  [^Consumer consumer
   records
   handlers
   {:keys [running?] :as settings}]
  (loop [remaining (seq records)
         done      {}]
    (let [^ConsumerRecord record (first remaining)
          outcome (when (and record (running?))
                    (process-record! record handlers settings))]
      (case outcome
        :done
        (recur (next remaining) (conj done (offset-of record)))

        :again
        (do (.seek consumer (topic-partition-of record) (.offset record))
            (commit! consumer done)
            (sleep (:retry-backoff-ms settings)))

        (commit! consumer done)))))

(defn- poll-loop!
  "Polls until the consumer is stopped, handing the records to their handlers.
   Closes the consumer when it is done, from the thread that polled, which is
   the one that may."
  [^Consumer consumer
   handlers
   {:keys [running? poll-timeout-ms] :as settings}]
  (try
    (while (running?)
      (try
        (let [records (.poll consumer (Duration/ofMillis poll-timeout-ms))]
          (process-batch! consumer records handlers settings))
        (catch WakeupException _
          nil)
        (catch InterruptException _
          (.interrupt (Thread/currentThread)))
        (catch Exception cause
          (log/error cause "kafka poll failed; trying again")
          (sleep poll-failure-pause-ms))))
    (finally
      (try (.close consumer (Duration/ofSeconds 5))
           (catch Exception cause
             (log/warn cause "kafka consumer did not close cleanly"))))))

;; The component

(defn- verify-brokers!
  "Asks the brokers who is in the cluster, and fails when none answers in the
   time given."
  [brokers
   timeout-ms]
  (let [properties {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG brokers
                    AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG
                    (int timeout-ms)
                    AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG
                    (int timeout-ms)}]
    (with-open [^Admin admin (AdminClient/create ^java.util.Map properties)]
      (.get ^KafkaFuture (.nodes (.describeCluster admin))
            (long timeout-ms)
            TimeUnit/MILLISECONDS))))

(defn- check-handlers
  "Fails naming the topics that have no handler, which would be read and
   thrown away."
  [topics
   handlers]
  (let [missing (remove #(ifn? (get handlers %)) topics)]
    (when (seq missing)
      (throw (ex-info (str "the topics have no handler: "
                           (pr-str (vec missing)))
                      {:error   ::no-handler
                       :topics  (vec missing)})))))

(defmethod ig/init-key :components/kafka-consumer
  [_ {:keys [handlers verify-connection? verify-timeout-ms]
      :or   {verify-connection? true
             verify-timeout-ms  default-verify-timeout-ms}
      :as   options}]
  (let [properties (config/client-properties options)
        settings   (config/loop-settings options)
        topics     (:topics settings)]
    (if (empty? topics)
      (do (log/info "kafka consumer has no topics, and does nothing")
          nil)
      (let [brokers   (get properties "bootstrap.servers")
            client-id (get properties "client.id")]
        (check-handlers topics handlers)
        (when verify-connection?
          (try (verify-brokers! brokers verify-timeout-ms)
               (catch Exception cause
                 (throw (ex-info (str "the Kafka brokers do not answer on "
                                      brokers)
                                 {:error   ::cannot-connect
                                  :brokers brokers}
                                 cause)))))
        (let [consumer  (KafkaConsumer. ^java.util.Map properties)
              running   (atom true)
              loop-opts (assoc settings :running? #(deref running))
              thread    (doto (Thread.
                               ^Runnable
                               #(poll-loop! consumer handlers loop-opts)
                               ^String (str "borba-kafka-consumer-" client-id))
                          (.setDaemon true))]
          (.subscribe consumer ^java.util.Collection topics)
          (.start thread)
          (log/infof "kafka consumer %s started on %s, topics %s, group %s"
                     client-id brokers topics (get properties "group.id"))
          {:consumer            consumer
           :thread              thread
           :running             running
           :client-id           client-id
           :shutdown-timeout-ms (:shutdown-timeout-ms settings)})))))

(defmethod ig/halt-key! :components/kafka-consumer
  [_ state]
  (when state
    (let [{:keys [^Consumer consumer ^Thread thread running client-id
                  shutdown-timeout-ms]} state]
      (reset! running false)
      (.wakeup consumer)
      (.join thread (long shutdown-timeout-ms))
      (if (.isAlive thread)
        (log/errorf "kafka consumer %s did not stop within %d ms"
                    client-id shutdown-timeout-ms)
        (log/infof "kafka consumer %s stopped" client-id)))))

(defn running?
  "Returns true while the consumer is polling, for a readiness check.
   - consumer: the value of the component, or nil when it has no topics"
  [consumer]
  (boolean (and consumer
                @(:running consumer)
                (.isAlive ^Thread (:thread consumer)))))

;; The handlers

(defn- resolve-handler
  "Returns the function of a handler, which can be a function, a var or the
   qualified symbol of one, whose namespace is loaded for it."
  [topic
   handler]
  (let [resolved (if (symbol? handler) (requiring-resolve handler) handler)]
    (when-not (ifn? resolved)
      (throw (ex-info (str "the handler of " (pr-str topic)
                           " is not a function: " (pr-str handler))
                      {:error   ::invalid-handler
                       :topic   topic
                       :handler handler})))
    resolved))

(defmethod ig/init-key :kafka/consumer-handlers
  [_ handlers]
  (reduce-kv (fn [resolved topic handler]
               (assoc resolved topic (resolve-handler topic handler)))
             {}
             (or handlers {})))
