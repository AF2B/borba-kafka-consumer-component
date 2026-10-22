(ns borba.kafka-consumer.message
  "What a handler is given for a record, and what becomes of a record that
   cannot be handled.

   A message is a map of the :key, the :value, the :topic, the :partition, the
   :offset, the :timestamp and the :headers, a map of names to text. The value
   is the JSON of the record, read strictly, with its keys as keywords: a value
   with text after it, or an object with a key twice, is not valid, because
   what two readers would read differently is what an attack is made of. A null
   value, a tombstone, is nil.

   With a Schema Registry the value is read with the schema it names instead,
   see borba.kafka-consumer.schema-registry.

   A message that cannot be handled has a failure, a map with an :error:
   :invalid-json when the value cannot be read, which is not tried again
   because it will not read any better (with a registry :invalid-value,
   :schema-violation or :schema-not-registered, and
   :schema-registry-unavailable, which is read again), and :handler-failed
   when the handler threw in every attempt."
  (:require
   [borba.kafka-consumer.schema-registry :as schema-registry]
   [jsonista.core :as jsonista])
  (:import
   (com.fasterxml.jackson.core JsonParser$Feature JsonProcessingException)
   (com.fasterxml.jackson.databind DeserializationFeature ObjectMapper)
   (java.nio.charset StandardCharsets)
   (org.apache.kafka.clients.consumer ConsumerRecord)
   (org.apache.kafka.common.header Header)))

(set! *warn-on-reflection* true)

(def ^:private ^ObjectMapper strict-mapper
  (doto ^ObjectMapper (jsonista/object-mapper {:decode-key-fn true})
    (.enable DeserializationFeature/FAIL_ON_TRAILING_TOKENS)
    (.configure JsonParser$Feature/STRICT_DUPLICATE_DETECTION true)))

(defn- header-text
  "Returns the headers of a record as a map of names to text."
  [^ConsumerRecord record]
  (into {}
        (map (fn [^Header header]
               [(.key header)
                (String. ^bytes (.value header) StandardCharsets/UTF_8)]))
        (.headers record)))

(defn raw-message
  "Returns a record as a message whose :value is still what the client gave:
   the text of the record, or its bytes when it is read with a registry.
   - record: a ConsumerRecord"
  [^ConsumerRecord record]
  {:key       (.key record)
   :value     (.value record)
   :topic     (.topic record)
   :partition (.partition record)
   :offset    (.offset record)
   :timestamp (.timestamp record)
   :headers   (header-text record)})

(defn failure
  "Returns the failure of a message: where it was, and what went wrong. The
   value of the message is not in it, because it can be what must not be
   logged.
   - message: the message
   - error: the keyword that says what went wrong
   - attempts: how many times it was tried
   - cause: the exception, or nil"
  [message
   error
   attempts
   cause]
  (cond-> {:error     error
           :topic     (:topic message)
           :partition (:partition message)
           :offset    (:offset message)
           :key       (:key message)
           :attempts  attempts}
    cause (assoc :cause cause)))

(defn- parse-text
  [message]
  (try
    (update message :value #(jsonista/read-value ^String % strict-mapper))
    (catch JsonProcessingException cause
      (failure message :invalid-json 1 cause))))

(defn- parse-with-schema
  [message
   value-deserializer]
  (try
    (let [node (schema-registry/decode value-deserializer
                                       (:topic message)
                                       (:value message))]
      (assoc message :value (jsonista/read-value (str node) strict-mapper)))
    (catch clojure.lang.ExceptionInfo cause
      (failure message (:error (ex-data cause)) 1 (ex-cause cause)))))

(defn parse
  "Returns the message with its value read, or a failure when the value cannot
   be read: as JSON text, or with the schema it names when there is a
   deserializer of the registry.
   - message: a message from `raw-message`
   - value-deserializer: the deserializer of
     borba.kafka-consumer.schema-registry (default none: the value is JSON
     text)"
  ([message]
   (parse message nil))
  ([message
    value-deserializer]
   (cond
     (nil? (:value message)) message
     value-deserializer      (parse-with-schema message value-deserializer)
     :else                   (parse-text message))))

(defn failure?
  "Returns true when a map is the failure of a message.
   - x: a message or a failure"
  [x]
  (contains? x :error))
