(ns borba.kafka-consumer
  "Integrant component for Kafka Consumer.

   Registers two Integrant keys:
     :components/kafka-consumer — starts a polling loop in a daemon thread
     :kafka/consumer-handlers   — base empty handler map (override in service)

   Handler functions receive:
     {:keys [key value topic partition offset]}
   where :value is already parsed from JSON.

   Usage — override the handler map in your service:
     (defmethod ig/init-key :kafka/consumer-handlers [_ _]
       {\"order-events\" handle-order-event
        \"payment-events\" handle-payment-event})"
  (:require [integrant.core :as ig]
            [cheshire.core :as json])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig)
           (org.apache.kafka.common.serialization StringDeserializer)
           (java.time Duration)))

;; ── Internal polling loop ────────────────────────────────────────────────────

(defn- poll-loop
  [^KafkaConsumer consumer handlers running?]
  (while @running?
    (try
      (let [records (.poll consumer (Duration/ofMillis 1000))]
        (doseq [record records]
          (let [topic   (.topic record)
                handler (get handlers topic)]
            (when handler
              (try
                (handler {:key       (.key record)
                          :value     (json/parse-string (.value record) true)
                          :topic     topic
                          :partition (.partition record)
                          :offset    (.offset record)})
                (catch Exception e
                  (println "❌ [kafka-consumer] Handler error on topic" topic ":" (.getMessage e))))))))
      (catch org.apache.kafka.common.errors.WakeupException _
        nil)
      (catch Exception e
        (println "❌ [kafka-consumer] Poll error:" (.getMessage e))
        (Thread/sleep 1000)))))

;; ── Integrant lifecycle ──────────────────────────────────────────────────────

(defmethod ig/init-key :components/kafka-consumer
  [_ {:keys [bootstrap-servers group-id topics handlers]}]
  (if (empty? topics)
    (do (println "📥 [kafka-consumer] No topics configured — skipping")
        nil)
    (let [config   {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG       bootstrap-servers
                    ConsumerConfig/GROUP_ID_CONFIG                 group-id
                    ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG   (.getName StringDeserializer)
                    ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG (.getName StringDeserializer)
                    ConsumerConfig/AUTO_OFFSET_RESET_CONFIG        "earliest"
                    ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG       true}
          consumer (KafkaConsumer. config)
          running? (atom true)]
      (.subscribe consumer topics)
      (let [thread (Thread.
                    (fn [] (poll-loop consumer handlers running?))
                    "borba-kafka-consumer")]
        (.setDaemon thread true)
        (.start thread))
      (println "📥 [kafka-consumer] Started → topics:" topics)
      {:consumer consumer
       :running? running?})))

(defmethod ig/halt-key! :components/kafka-consumer
  [_ state]
  (when state
    (let [{:keys [consumer running?]} state]
      (reset! running? false)
      (.wakeup ^KafkaConsumer consumer)
      (Thread/sleep 1500)
      (.close ^KafkaConsumer consumer)
      (println "📥 [kafka-consumer] Stopped"))))

;; ── Handler registry ────────────────────────────────────────────────────────
;;
;; The handlers map is declared entirely in EDN (system/stag.edn, etc.)
;; using fully-qualified symbols that point directly to handler functions.
;; No Integrant boilerplate needed in the service's events namespace.
;;
;; Example in stag.edn:
;;   :kafka/consumer-handlers
;;   {"order-events" com.my-service.events.consumer/handle-order-event}
;;
;; The handler namespace is loaded automatically via requiring-resolve,
;; so it does NOT need to be in :service/namespaces.
;;
;; Handler functions still receive the standard message map:
;;   {:keys [key value topic partition offset]}

(defn- resolve-handler
  "Resolves a handler value: returns the function if already a fn,
   or loads the namespace and resolves the var if a qualified symbol."
  [handler]
  (if (symbol? handler)
    (requiring-resolve handler)
    handler))

(defmethod ig/init-key :kafka/consumer-handlers
  [_ handlers]
  (reduce-kv
   (fn [acc topic handler]
     (assoc acc topic (resolve-handler handler)))
   {}
   (or handlers {})))

(defmethod ig/halt-key! :kafka/consumer-handlers [_ _] nil)
