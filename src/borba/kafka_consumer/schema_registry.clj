(ns borba.kafka-consumer.schema-registry
  "The Schema Registry of a consumer: values written in the wire format of
   Confluent, a magic byte and the id of a schema before the JSON, read with
   the schema that the registry holds for that id.

     :schema-registry {:url #env SCHEMA_REGISTRY_URL}

   The consumer needs no schema of its own: a value names the one it was
   written with, which the registry gives, and the value is validated against
   it before a handler sees it.

   A value that cannot be read is a failure of its message, as a value that is
   not JSON is without a registry: :schema-violation when its schema refuses
   it, :schema-not-registered when the registry does not have the schema it
   names, and :invalid-value when it is not in the wire format at all. A
   registry that does not answer, :schema-registry-unavailable, is not the
   message's fault, so the message is read again instead of being past."
  (:require
   [clojure.string :as str])
  (:import
   (com.fasterxml.jackson.databind JsonNode)
   (io.confluent.kafka.schemaregistry.client
    SchemaRegistryClient SchemaRegistryClientFactory)
   (io.confluent.kafka.schemaregistry.client.rest.exceptions
    RestClientException)
   (io.confluent.kafka.schemaregistry.json JsonSchemaProvider)
   (io.confluent.kafka.serializers.json KafkaJsonSchemaDeserializer)
   (java.io IOException)
   (java.net URI)
   (org.apache.kafka.common.serialization ByteArrayDeserializer)
   (org.everit.json.schema ValidationException)))

(set! *warn-on-reflection* true)

(def default-timeout-ms
  "How long a request to the registry waits to connect and to be answered,
   unless told otherwise."
  5000)

(def ^:private client-cache-capacity 100)

(def ^:private user-info-source "USER_INFO")

;; The error codes of the registry for a subject, a version or a schema that it
;; does not have.
(def ^:private not-found-codes #{40401 40402 40403})

;; Options

(defn- invalid-option
  [option
   value
   expected]
  (ex-info (str ":schema-registry " (name option) " is "
                (if (= :basic-auth option) "not valid" (pr-str value))
                ", and must be " expected)
           {:error  ::invalid-option
            :option option}))

(defn- non-blank-string?
  [value]
  (and (string? value) (not (str/blank? value))))

(defn check-options
  "Checks the :schema-registry options of the component, and fails naming the
   first one that is not valid. The credentials are never in the message.
   - url: the URL of the registry
   - basic-auth: the credentials, \"user:password\" (default none)
   - timeout-ms: how long a request to the registry waits (default 5000)
   - options: the whole map, which is returned when it is valid"
  [{:keys [url basic-auth timeout-ms] :as options}]
  (when-not (non-blank-string? url)
    (throw (invalid-option :url url "the URL of the registry")))
  (when-not (or (nil? basic-auth) (non-blank-string? basic-auth))
    (throw (invalid-option :basic-auth basic-auth "\"user:password\"")))
  (when-not (or (nil? timeout-ms) (and (int? timeout-ms) (pos? timeout-ms)))
    (throw (invalid-option :timeout-ms timeout-ms "a positive integer")))
  options)

(defn- connection-properties
  "Returns the properties that reach the registry: where, with which
   credentials and how long to wait."
  [{:keys [url basic-auth timeout-ms]
    :or   {timeout-ms default-timeout-ms}}]
  (cond-> {"schema.registry.url"     url
           "http.connect.timeout.ms" (int timeout-ms)
           "http.read.timeout.ms"    (int timeout-ms)}
    basic-auth (assoc "basic.auth.credentials.source" user-info-source
                      "basic.auth.user.info"          basic-auth)))

(def client-properties
  "The properties of the client for values that are read with the registry:
   the client hands over the bytes, and each message is read on its own, so a
   value that cannot be read is the failure of that message and not of the
   poll."
  {"value.deserializer" (.getName ByteArrayDeserializer)})

;; The start

(defn- address
  "Returns where the registry is, without the credentials a URL can carry."
  [url]
  (try
    (let [uri  (URI. url)
          port (.getPort uri)]
      (str (.getHost uri) (when (pos? port) (str ":" port))))
    (catch Exception _
      "the registry")))

(defn- new-client
  ^SchemaRegistryClient [options]
  (let [^java.util.List urls      [(:url options)]
        ^java.util.List providers [(JsonSchemaProvider.)]
        ^java.util.Map  configs   (connection-properties options)
        ^java.util.Map  headers   {}]
    (SchemaRegistryClientFactory/newClient urls
                                           (int client-cache-capacity)
                                           providers
                                           configs
                                           headers)))

(defn verify!
  "Asks the registry for its subjects, and fails when it does not answer.
   - options: the :schema-registry options of the component"
  [options]
  (let [client (new-client options)
        url    (:url options)]
    (try
      (.getAllSubjects client)
      (catch Exception cause
        (throw (ex-info (str "the schema registry does not answer on "
                             (address url))
                        {:error   ::cannot-connect
                         :address (address url)}
                        cause)))
      (finally
        (.close client)))))

(defn deserializer
  "Returns the deserializer that reads a value with the schema it names, and
   validates it, as a JsonNode. `close!` closes it.
   - options: the :schema-registry options of the component"
  ^KafkaJsonSchemaDeserializer [options]
  (doto (KafkaJsonSchemaDeserializer.)
    (.configure (merge (connection-properties options)
                       {"json.fail.invalid.schema" true
                        "json.value.type"          (.getName JsonNode)})
                false)))

(defn close!
  "Closes a deserializer, and its client of the registry.
   - value-deserializer: what `deserializer` returned (nil does nothing)"
  [^KafkaJsonSchemaDeserializer value-deserializer]
  (when value-deserializer
    (.close value-deserializer)))

;; Reading

(defn- causes
  [^Throwable e]
  (take-while some? (iterate #(.getCause ^Throwable %) e)))

(defn- not-found?
  [cause]
  (and (instance? RestClientException cause)
       (contains? not-found-codes
                  (.getErrorCode ^RestClientException cause))))

(defn- read-error
  "Returns what a value that could not be read was."
  [cause]
  (let [chain (causes cause)]
    (cond
      (some #(instance? ValidationException %) chain)
      :schema-violation

      (some not-found? chain)
      :schema-not-registered

      (some #(instance? IOException %) chain)
      :schema-registry-unavailable

      :else
      :invalid-value)))

(defn decode
  "Reads a value with the schema it names, and returns it as a JsonNode, or
   throws an ex-info whose :error says why it could not be read.
   - value-deserializer: what `deserializer` returned
   - topic: the topic of the value
   - value: the bytes of the value"
  ^JsonNode [^KafkaJsonSchemaDeserializer value-deserializer
             ^String topic
             ^bytes value]
  (try
    (.deserialize value-deserializer topic value)
    (catch Exception cause
      (throw (ex-info "the value cannot be read with its schema"
                      {:error (read-error cause)}
                      cause)))))
