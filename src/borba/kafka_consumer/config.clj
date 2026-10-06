(ns borba.kafka-consumer.config
  "The options of the consumer: checked, and turned into the properties of the
   Kafka client and the settings of the loop that polls.

   The client is written to give at-least-once delivery: it does not commit
   offsets by itself, so a message is only past when the handler is done with
   it, and it reads only what a transaction has committed."
  (:require
   [clojure.string :as str])
  (:import
   (org.apache.kafka.clients.consumer ConsumerConfig)
   (org.apache.kafka.common.serialization StringDeserializer)))

(set! *warn-on-reflection* true)

(def default-client-id
  "The name of the consumer for the brokers, unless told otherwise."
  "borba-consumer")

(def default-auto-offset-reset
  "Where a group with no committed offset starts, unless told otherwise."
  "earliest")

(def auto-offset-resets
  "Where a group with no committed offset can start."
  #{"earliest" "latest" "none"})

(def default-max-poll-records
  "The most messages a poll returns, unless told otherwise."
  100)

(def default-session-timeout-ms
  "How long the group waits for a sign of life of the consumer before it gives
   its partitions to another, unless told otherwise."
  45000)

(def default-max-poll-interval-ms
  "How long the handlers of a poll have to be done before the group gives the
   partitions to another consumer, unless told otherwise: five minutes."
  300000)

(def default-poll-timeout-ms
  "How long a poll waits for messages, unless told otherwise."
  1000)

(def default-max-attempts
  "How many times a handler is tried for a message, unless told otherwise."
  3)

(def default-retry-backoff-ms
  "How long to wait before a handler is tried again, unless told otherwise."
  500)

(def default-shutdown-timeout-ms
  "How long a stop waits for the message in hand, unless told otherwise."
  10000)

(defn- invalid-option
  [option
   value
   expected]
  (ex-info (str ":" (name option) " is " (pr-str value) ", and must be "
                expected)
           {:error  ::invalid-option
            :option option
            :value  value}))

(defn- positive-int?
  [value]
  (and (int? value) (pos? value)))

(defn- text?
  [value]
  (and (string? value) (not (str/blank? value))))

(defn- servers
  "Returns the brokers as the text the client takes, or nil when they are not
   given as one."
  [bootstrap-servers]
  (cond
    (text? bootstrap-servers)
    bootstrap-servers

    (and (sequential? bootstrap-servers)
         (seq bootstrap-servers)
         (every? text? bootstrap-servers))
    (str/join "," bootstrap-servers)))

(defn client-properties
  "Checks the options of the component and returns the properties of the Kafka
   client that they make, as a map of strings to values. Fails naming the first
   option that is not valid.
   - bootstrap-servers: the brokers, \"host:port,host:port\" or a vector of
     \"host:port\"
   - group-id: the group the consumers that share the messages belong to
   - client-id: the name of the consumer for the brokers (default
     \"borba-consumer\")
   - auto-offset-reset: where a group with no committed offset starts,
     earliest, latest or none (default earliest)
   - max-poll-records: the most messages a poll returns (default 100)
   - session-timeout-ms: how long the group waits for a sign of life (default
     45000)
   - max-poll-interval-ms: how long the handlers of a poll have to be done
     (default 300000)
   - properties: more properties of the client, a map of strings to strings,
     such as the security protocol and the SASL settings, which take the
     place of the ones above (default none)"
  [{:keys [bootstrap-servers group-id client-id auto-offset-reset
           max-poll-records session-timeout-ms max-poll-interval-ms properties]
    :or   {client-id            default-client-id
           auto-offset-reset    default-auto-offset-reset
           max-poll-records     default-max-poll-records
           session-timeout-ms   default-session-timeout-ms
           max-poll-interval-ms default-max-poll-interval-ms}}]
  (let [brokers (servers bootstrap-servers)]
    (when-not brokers
      (throw (invalid-option :bootstrap-servers bootstrap-servers
                             "\"host:port\" or a vector of them")))
    (when-not (text? group-id)
      (throw (invalid-option :group-id group-id "a non-empty string")))
    (when-not (text? client-id)
      (throw (invalid-option :client-id client-id "a non-empty string")))
    (when-not (contains? auto-offset-resets auto-offset-reset)
      (throw (invalid-option :auto-offset-reset auto-offset-reset
                             (str "one of " (sort auto-offset-resets)))))
    (doseq [[option value] [[:max-poll-records max-poll-records]
                            [:session-timeout-ms session-timeout-ms]
                            [:max-poll-interval-ms max-poll-interval-ms]]]
      (when-not (positive-int? value)
        (throw (invalid-option option value "a positive integer"))))
    (when-not (or (nil? properties)
                  (and (map? properties)
                       (every? string? (keys properties))
                       (every? string? (vals properties))))
      (throw (invalid-option :properties properties
                             "a map of strings to strings")))
    (merge {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG        brokers
            ConsumerConfig/GROUP_ID_CONFIG                 group-id
            ConsumerConfig/CLIENT_ID_CONFIG                client-id
            ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG
            (.getName StringDeserializer)
            ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG
            (.getName StringDeserializer)
            ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG       false
            ConsumerConfig/ISOLATION_LEVEL_CONFIG          "read_committed"
            ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        auto-offset-reset
            ConsumerConfig/MAX_POLL_RECORDS_CONFIG
            (int max-poll-records)
            ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG
            (int session-timeout-ms)
            ConsumerConfig/MAX_POLL_INTERVAL_MS_CONFIG
            (int max-poll-interval-ms)}
           properties)))

(defn loop-settings
  "Checks the options that are about handling the messages and returns them,
   with their defaults. Fails naming the first option that is not valid.
   - topics: the topics to read, a vector of names
   - poll-timeout-ms: how long a poll waits for messages (default 1000)
   - max-attempts: how many times a handler is tried for a message (default 3)
   - retry-backoff-ms: how long to wait before it is tried again (default 500)
   - shutdown-timeout-ms: how long a stop waits for the message in hand
     (default 10000)
   - on-error: a function of the failure of a message that could not be
     handled, called after the last attempt (default none: it is logged and
     the message is skipped)"
  [{:keys [topics poll-timeout-ms max-attempts retry-backoff-ms
           shutdown-timeout-ms on-error]
    :or   {poll-timeout-ms     default-poll-timeout-ms
           max-attempts        default-max-attempts
           retry-backoff-ms    default-retry-backoff-ms
           shutdown-timeout-ms default-shutdown-timeout-ms}}]
  (when-not (and (sequential? topics) (every? text? topics))
    (throw (invalid-option :topics topics "a vector of topic names")))
  (doseq [[option value] [[:poll-timeout-ms poll-timeout-ms]
                          [:max-attempts max-attempts]
                          [:retry-backoff-ms retry-backoff-ms]
                          [:shutdown-timeout-ms shutdown-timeout-ms]]]
    (when-not (positive-int? value)
      (throw (invalid-option option value "a positive integer"))))
  (when-not (or (nil? on-error) (ifn? on-error))
    (throw (invalid-option :on-error on-error "a function, or not given")))
  {:topics              (vec topics)
   :poll-timeout-ms     poll-timeout-ms
   :max-attempts        max-attempts
   :retry-backoff-ms    retry-backoff-ms
   :shutdown-timeout-ms shutdown-timeout-ms
   :on-error            on-error})
