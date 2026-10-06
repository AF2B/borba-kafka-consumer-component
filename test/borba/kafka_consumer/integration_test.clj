(ns ^:integration borba.kafka-consumer.integration-test
  "Runs against a real Kafka, which the pipeline provides and which a developer
   starts with

     docker run --rm -d --name borba-kafka-it -p 127.0.0.1:9092:9092 \\
       apache/kafka:4.0.0

   and points the tests at with KAFKA_BROKERS (127.0.0.1:9092)."
  (:require
   [borba.kafka-consumer :as kafka]
   [clojure.test :refer [deftest is testing]]
   [integrant.core :as ig])
  (:import
   (java.nio.charset StandardCharsets)
   (org.apache.kafka.clients.producer
    KafkaProducer ProducerConfig ProducerRecord)
   (org.apache.kafka.common.serialization StringSerializer)))

(set! *warn-on-reflection* true)

(def ^:private wait-ms 40000)

(defn- brokers
  []
  (or (System/getenv "KAFKA_BROKERS")
      (throw (ex-info "set KAFKA_BROKERS to run the integration tests" {}))))

(defn- unique
  [label]
  (str "borba-it-" label "-" (subs (str (random-uuid)) 0 8)))

(defn- produce!
  "Writes records to a topic, as plain text. A record is a map of :key, :value
   and :headers."
  [topic records]
  (let [properties {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (brokers)
                    ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
                    (.getName StringSerializer)
                    ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
                    (.getName StringSerializer)}]
    (with-open [producer (KafkaProducer. ^java.util.Map properties)]
      (doseq [{:keys [value headers] message-key :key} records]
        (let [record (ProducerRecord. ^String topic
                                      ^String message-key
                                      ^String value)]
          (doseq [[header-name header-value] headers]
            (.add (.headers record)
                  ^String header-name
                  (.getBytes ^String header-value StandardCharsets/UTF_8)))
          @(.send producer record))))))

(defn- eventually
  "Polls until a condition holds, and returns whether it did in time."
  [condition]
  (let [deadline (+ (System/currentTimeMillis) wait-ms)]
    (loop []
      (cond
        (condition) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (Thread/sleep 50) (recur))))))

(defn- start
  "Starts a consumer, with more options, and returns the system."
  [topics group handlers more]
  (ig/init {:components/kafka-consumer
            (merge {:bootstrap-servers (brokers)
                    :group-id          group
                    :client-id         (unique "consumer")
                    :topics            topics
                    :handlers          handlers
                    :retry-backoff-ms  50
                    :poll-timeout-ms   200}
                   more)}))

(defn- stop
  [system]
  (ig/halt! system))

(deftest delivery-test
  (testing "gives the handler each message, in order, with what it carries"
    (let [topic (unique "delivery")
          seen  (atom [])]
      (produce! topic [{:key     "a"
                        :value   "{\"n\":1}"
                        :headers {"request-id" "r1"}}
                       {:key "b" :value "{\"n\":2}"}
                       {:key "c" :value "{\"n\":3}"}])
      (let [system (start [topic] (unique "group")
                          {topic #(swap! seen conj %)} {})]
        (try
          (is (eventually #(= 3 (count @seen))))
          (is (= [{:n 1} {:n 2} {:n 3}] (mapv :value @seen)))
          (is (= ["a" "b" "c"] (mapv :key @seen)))
          (is (= [0 1 2] (mapv :offset @seen)))
          (is (= {"request-id" "r1"} (:headers (first @seen))))
          (is (= topic (:topic (first @seen))))
          (is (pos? (:timestamp (first @seen))))
          (finally
            (stop system)))))))

(deftest tombstone-test
  (testing "gives a tombstone as a nil value"
    (let [topic (unique "tombstone")
          seen  (atom [])]
      (produce! topic [{:key "gone" :value nil}])
      (let [system (start [topic] (unique "group")
                          {topic #(swap! seen conj %)} {})]
        (try
          (is (eventually #(= 1 (count @seen))))
          (is (= "gone" (:key (first @seen))))
          (is (nil? (:value (first @seen))))
          (finally
            (stop system)))))))

(deftest at-least-once-test
  (testing "tries a handler that throws again, and the message is handled once
            it is done"
    (let [topic (unique "retry")
          calls (atom 0)
          done  (atom [])]
      (produce! topic [{:key "k" :value "{\"n\":1}"}])
      (let [system (start [topic] (unique "group")
                          {topic (fn [message]
                                   (when (< (swap! calls inc) 3)
                                     (throw (ex-info "not yet" {})))
                                   (swap! done conj message))}
                          {})]
        (try
          (is (eventually #(= 1 (count @done))))
          (is (= 3 @calls))
          (finally
            (stop system))))))

  (testing "does not give a message again once it is past, to a consumer of the
            same group that starts later"
    (let [topic (unique "committed")
          group (unique "group")
          first-seen (atom [])]
      (produce! topic [{:key "a" :value "{\"n\":1}"}
                       {:key "b" :value "{\"n\":2}"}])
      (let [system (start [topic]
                          group
                          {topic #(swap! first-seen conj %)}
                          {})]
        (try (is (eventually #(= 2 (count @first-seen))))
             (finally (stop system))))
      (produce! topic [{:key "c" :value "{\"n\":3}"}])
      (let [second-seen (atom [])
            system      (start [topic] group
                               {topic #(swap! second-seen conj %)} {})]
        (try
          (is (eventually #(= 1 (count @second-seen))))
          (is (= ["c"] (mapv :key @second-seen)))
          (finally (stop system))))))

  (testing "finishes the message in hand when it is stopped, and does not
            give it again"
    (let [topic    (unique "in-hand")
          group    (unique "group")
          started  (promise)
          finished (atom 0)]
      (produce! topic [{:key "slow" :value "{\"n\":1}"}])
      (let [system (start [topic] group
                          {topic (fn [_message]
                                   (deliver started true)
                                   (Thread/sleep 1500)
                                   (swap! finished inc))}
                          {})]
        (is (true? (deref started wait-ms false)))
        (stop system)
        (is (= 1 @finished)))
      (let [again  (atom [])
            system (start [topic] group {topic #(swap! again conj %)} {})]
        (try
          (Thread/sleep 4000)
          (is (empty? @again))
          (finally (stop system)))))))

(deftest failure-test
  (testing "reports a value that is not JSON, and still gives the next one"
    (let [topic    (unique "poison")
          seen     (atom [])
          failures (atom [])]
      (produce! topic [{:key "bad" :value "{\"secret\":\"hunter2\" oops"}
                       {:key "good" :value "{\"ok\":true}"}])
      (let [system (start [topic] (unique "group")
                          {topic #(swap! seen conj %)}
                          {:on-error #(swap! failures conj %)})]
        (try
          (is (eventually #(= 1 (count @seen))))
          (is (= [{:ok true}] (mapv :value @seen)))
          (is (= [:invalid-json] (mapv :error @failures)))
          (is (= "bad" (:key (first @failures))))
          (is (not (re-find #"hunter2" (pr-str (dissoc (first @failures)
                                                       :cause)))))
          (finally
            (stop system))))))

  (testing "reports a message that the handler never handles, to be written
            somewhere else"
    (let [topic        (unique "dead-letter")
          dead-letters (atom [])]
      (produce! topic [{:key "doomed" :value "{\"n\":1}"}])
      (let [system (start [topic] (unique "group")
                          {topic (fn [_] (throw (ex-info "never" {})))}
                          {:max-attempts 2
                           :on-error     #(swap! dead-letters conj %)})]
        (try
          (is (eventually #(= 1 (count @dead-letters))))
          (let [failed (first @dead-letters)]
            (is (= :handler-failed (:error failed)))
            (is (= 2 (:attempts failed)))
            (is (= "doomed" (:key failed))))
          (finally
            (stop system))))))

  (testing "gives the message again when on-error throws, until it does not"
    (let [topic    (unique "dlq-down")
          reported (atom 0)]
      (produce! topic [{:key "k" :value "{bad"}])
      (let [system (start [topic] (unique "group")
                          {topic identity}
                          {:on-error (fn [_]
                                       (when (< (swap! reported inc) 3)
                                         (throw (ex-info "dlq down" {}))))})]
        (try
          (is (eventually #(<= 3 @reported)))
          (Thread/sleep 1000)
          (is (= 3 @reported))
          (finally
            (stop system)))))))

(deftest topics-test
  (testing "gives each topic to its handler"
    (let [orders   (unique "orders")
          payments (unique "payments")
          seen     (atom {})]
      (produce! orders [{:key "o" :value "{\"kind\":\"order\"}"}])
      (produce! payments [{:key "p" :value "{\"kind\":\"payment\"}"}])
      (let [system (start [orders payments] (unique "group")
                          {orders   #(swap! seen assoc :orders (:value %))
                           payments #(swap! seen assoc :payments (:value %))}
                          {})]
        (try
          (is (eventually #(= 2 (count @seen))))
          (is (= {:orders {:kind "order"} :payments {:kind "payment"}} @seen))
          (finally
            (stop system)))))))

(deftest lifecycle-test
  (testing "is running while it polls, and not after it is stopped"
    (let [topic  (unique "alive")
          system (start [topic] (unique "group") {topic identity} {})
          state  (:components/kafka-consumer system)]
      (is (true? (kafka/running? state)))
      (stop system)
      (is (false? (kafka/running? state)))))

  (testing "fails the start for a topic without a handler, naming it"
    (is (= {:error  :borba.kafka-consumer/no-handler
            :topics ["no-handler-here"]}
           (try (start ["no-handler-here"] (unique "group") {} {})
                nil
                (catch clojure.lang.ExceptionInfo e
                  (ex-data (ex-cause e)))))))

  (testing "brokers that do not answer fail the start, naming where they were"
    (let [started (System/nanoTime)
          thrown  (try (ig/init {:components/kafka-consumer
                                 {:bootstrap-servers "127.0.0.1:1"
                                  :group-id          "g"
                                  :topics            ["t"]
                                  :handlers          {"t" identity}
                                  :verify-timeout-ms 800}})
                       nil
                       (catch clojure.lang.ExceptionInfo e e))]
      (is (= {:error   :borba.kafka-consumer/cannot-connect
              :brokers "127.0.0.1:1"}
             (ex-data (ex-cause thrown))))
      (is (< (/ (- (System/nanoTime) started) 1e6) 5000)))))
