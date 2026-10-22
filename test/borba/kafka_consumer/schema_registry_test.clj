(ns borba.kafka-consumer.schema-registry-test
  (:require
   [borba.kafka-consumer :as kafka]
   [borba.kafka-consumer.config :as config]
   [borba.kafka-consumer.message :as message]
   [borba.kafka-consumer.schema-registry :as schema-registry]
   [clojure.test :refer [deftest is testing]]
   [jsonista.core :as jsonista])
  (:import
   (com.fasterxml.jackson.databind JsonNode ObjectMapper)
   (io.confluent.kafka.schemaregistry.client SchemaRegistryClientFactory)
   (io.confluent.kafka.schemaregistry.json
    JsonSchema JsonSchemaProvider JsonSchemaUtils)
   (io.confluent.kafka.serializers.json KafkaJsonSchemaSerializer)
   (java.nio.charset StandardCharsets)
   (org.apache.kafka.clients.consumer ConsumerRecord MockConsumer)
   (org.apache.kafka.common TopicPartition)
   (org.apache.kafka.common.serialization ByteArrayDeserializer)))

(set! *warn-on-reflection* true)

(def ^:private topic "orders")

(def ^:private wait-ms 5000)

(def ^:private ^JsonSchema order-schema
  (JsonSchema.
   ^String (jsonista/write-value-as-string
            {:type                 "object"
             :properties           {:total {:type "number"}}
             :required             ["total"]
             :additionalProperties false})))

(defn- mock-url
  "A registry of its own for each test: the mock registries of Confluent are
   shared by scope across the JVM."
  []
  (str "mock://" (random-uuid)))

(defn- register!
  [url]
  (let [^java.util.List urls      [url]
        ^java.util.List providers [(JsonSchemaProvider.)]
        ^java.util.Map  configs   {}
        ^java.util.Map  headers   {}
        client (SchemaRegistryClientFactory/newClient urls
                                                      (int 10)
                                                      providers
                                                      configs
                                                      headers)]
    (.register client (str topic "-value") order-schema)))

(defn- wire-bytes
  "Writes a value as a producer with the registry does. With validate? false
   the producer writes what the schema refuses, as a producer that does not
   check would."
  ^bytes [url
          value
          validate?]
  (let [serializer (doto (KafkaJsonSchemaSerializer.)
                     (.configure {"schema.registry.url"      url
                                  "auto.register.schemas"    false
                                  "json.fail.invalid.schema" validate?}
                                 false))
        node       (.valueToTree ^ObjectMapper jsonista/default-object-mapper
                                 value)]
    (.serialize serializer
                topic
                (JsonSchemaUtils/envelope ^JsonSchema order-schema
                                          ^JsonNode node))))

(defn- text-bytes
  ^bytes [^String text]
  (.getBytes text StandardCharsets/UTF_8))

(defn- message-of
  [value]
  {:topic topic :partition 0 :offset 7 :key "k" :value value})

(deftest options-test
  (testing "takes a URL"
    (is (= {:url "mock://x"}
           (schema-registry/check-options {:url "mock://x"}))))

  (testing "names the option that is not valid"
    (doseq [[option value] [[:url nil] [:url ""] [:timeout-ms -1]]]
      (is (= {:error  ::schema-registry/invalid-option
              :option option}
             (try (schema-registry/check-options
                   (assoc {:url "mock://x"} option value))
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e))))
          (str option))))

  (testing "the client hands over the bytes, to be read message by message"
    (is (= (.getName ByteArrayDeserializer)
           (get (config/client-properties
                 {:bootstrap-servers "localhost:9092"
                  :group-id          "g"
                  :schema-registry   {:url "mock://x"}})
                "value.deserializer")))))

(deftest parse-test
  (let [url    (mock-url)
        _      (register! url)
        reader (schema-registry/deserializer {:url url})]
    (try
      (testing "reads a value with the schema it names"
        (is (= {:total 10}
               (:value (message/parse
                        (message-of (wire-bytes url {:total 10} true))
                        reader)))))

      (testing "a value its schema refuses is a failure"
        (is (= :schema-violation
               (:error (message/parse
                        (message-of (wire-bytes url {:total "ten"} false))
                        reader)))))

      (testing "a value that is not in the wire format is a failure"
        (is (= :invalid-value
               (:error (message/parse
                        (message-of (text-bytes "{\"total\":10}"))
                        reader)))))

      (testing "a value that names a schema the registry does not have"
        (let [unknown (byte-array [0 0 0 0 99 123 125])]
          (is (= :schema-not-registered
                 (:error (message/parse (message-of unknown) reader))))))

      (testing "a tombstone stays nil"
        (is (nil? (:value (message/parse (message-of nil) reader)))))
      (finally
        (schema-registry/close! reader))))

  (testing "a registry that does not answer"
    (let [reader (schema-registry/deserializer {:url        "http://127.0.0.1:1"
                                                :timeout-ms 500})
          written (wire-bytes (doto (mock-url) register!) {:total 1} true)]
      (try
        (is (= :schema-registry-unavailable
               (:error (message/parse (message-of written) reader))))
        (finally
          (schema-registry/close! reader))))))

(defn- byte-consumer
  ^MockConsumer [values]
  (let [consumer        (MockConsumer. "earliest")
        topic-partition (TopicPartition. topic 0)]
    (.assign consumer [topic-partition])
    (.updateBeginningOffsets consumer {topic-partition 0})
    (doseq [[offset value] (map-indexed vector values)]
      (.addRecord consumer
                  (ConsumerRecord. topic (int 0) (long offset) "k" value)))
    consumer))

(defn- run-until
  "Runs the loop of the consumer until a condition holds, then stops it, and
   returns whether the condition held."
  [^MockConsumer consumer
   handlers
   reader
   condition]
  (let [running  (atom true)
        polling  (future (#'kafka/poll-loop!
                          consumer
                          handlers
                          {:poll-timeout-ms    20
                           :max-attempts       1
                           :retry-backoff-ms   5
                           :running?           #(deref running)
                           :value-deserializer reader}))
        deadline (+ (System/currentTimeMillis) wait-ms)
        held?    (loop []
                   (cond
                     (condition)                             true
                     (> (System/currentTimeMillis) deadline) false
                     :else (do (Thread/sleep 10) (recur))))]
    (reset! running false)
    (.wakeup consumer)
    @polling
    held?))

(deftest loop-test
  (testing "gives the handler the value read with its schema"
    (let [url      (doto (mock-url) register!)
          reader   (schema-registry/deserializer {:url url})
          seen     (atom [])
          consumer (byte-consumer [(wire-bytes url {:total 3} true)])]
      (is (run-until consumer
                     {topic #(swap! seen conj (:value %))}
                     reader
                     #(= [{:total 3}] @seen)))
      (schema-registry/close! reader)))

  (testing "reads a message again while the registry does not answer, and
            never gives it to the handler"
    (let [reader   (schema-registry/deserializer
                    {:url        "http://127.0.0.1:1"
                     :timeout-ms 200})
          seen     (atom [])
          written  (wire-bytes (doto (mock-url) register!) {:total 1} true)
          consumer (byte-consumer [written])]
      (is (not (run-until consumer
                          {topic #(swap! seen conj %)}
                          reader
                          #(seq @seen))))
      (is (empty? @seen))
      (schema-registry/close! reader))))
