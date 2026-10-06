(ns borba.kafka-consumer.message
  "What a handler is given for a record, and what becomes of a record that
   cannot be handled.

   A message is a map of the :key, the :value, the :topic, the :partition, the
   :offset, the :timestamp and the :headers, a map of names to text. The value
   is the JSON of the record, read strictly, with its keys as keywords: a value
   with text after it, or an object with a key twice, is not valid, because
   what two readers would read differently is what an attack is made of. A null
   value, a tombstone, is nil.

   A message that cannot be handled has a failure, a map with an :error:
   :invalid-json when the value cannot be read, which is not tried again
   because it will not read any better, and :handler-failed when the handler
   threw in every attempt."
  (:require
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
  "Returns a record as a message whose :value is still the text of the record.
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

(defn parse
  "Returns the message with its value read as JSON, or a failure when the value
   is not valid JSON.
   - message: a message from `raw-message`"
  [message]
  (let [text (:value message)]
    (if (nil? text)
      message
      (try
        (assoc message :value (jsonista/read-value ^String text strict-mapper))
        (catch JsonProcessingException cause
          (failure message :invalid-json 1 cause))))))

(defn failure?
  "Returns true when a map is the failure of a message.
   - x: a message or a failure"
  [x]
  (contains? x :error))
