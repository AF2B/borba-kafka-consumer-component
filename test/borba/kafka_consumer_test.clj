(ns borba.kafka-consumer-test
  (:require
   [borba.kafka-consumer :as kafka]
   [borba.kafka-consumer.config :as config]
   [borba.kafka-consumer.logging :as logging]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [integrant.core :as ig])
  (:import
   (org.apache.kafka.clients.consumer
    ConsumerRecord MockConsumer OffsetAndMetadata)
   (org.apache.kafka.common TopicPartition)))

(set! *warn-on-reflection* true)

(def ^:private topic "orders")
(def ^:private wait-ms 5000)

(defn- tp
  ^TopicPartition [partition-number]
  (TopicPartition. topic (int partition-number)))

(defn- mock
  "A consumer with the records already in it, for the partitions 0 and 1."
  ^MockConsumer [records]
  (let [consumer (MockConsumer. "earliest")]
    (.assign consumer [(tp 0) (tp 1)])
    (.updateBeginningOffsets consumer {(tp 0) 0 (tp 1) 0})
    (doseq [{:keys [offset value] partition-number :partition} records]
      (.addRecord consumer
                  (ConsumerRecord. topic
                                   (int (or partition-number 0))
                                   (long offset)
                                   (str "key-" offset)
                                   ^String value)))
    consumer))

(defn- settings
  [more]
  (merge {:poll-timeout-ms  20
          :max-attempts     3
          :retry-backoff-ms 5
          :on-error         nil}
         more))

(defn- committed-offset
  [^MockConsumer consumer partition-number]
  (let [committed (get (.committed consumer #{(tp partition-number)})
                       (tp partition-number))]
    (when committed
      (.offset ^OffsetAndMetadata committed))))

(defn- snapshot
  "What the consumer has committed, and where it is, for each partition. It can
   only be asked while the consumer is open."
  [^MockConsumer consumer]
  {:committed (into {}
                    (keep (fn [n]
                            (when-let [offset (committed-offset consumer n)]
                              [n offset])))
                    [0 1])
   :position  (into {} (map (fn [n] [n (.position consumer (tp n))])) [0 1])})

(defn- wait-for
  "Polls until a condition holds, and returns whether it did in time."
  [condition]
  (let [deadline (+ (System/currentTimeMillis) wait-ms)]
    (loop []
      (cond
        (condition) true
        (> (System/currentTimeMillis) deadline) false
        :else (do (Thread/sleep 10) (recur))))))

(defn- run-loop
  "Runs the loop of the consumer until a condition on what it has done holds,
   then stops it. Returns whether the condition held, and the last snapshot of
   the consumer, which is closed when the loop is done."
  [^MockConsumer consumer handlers more done?]
  (let [running  (atom true)
        last-one (atom nil)
        polling  (future (#'kafka/poll-loop!
                          consumer
                          handlers
                          (assoc (settings more) :running? #(deref running))))
        held?    (wait-for (fn []
                             (let [now (snapshot consumer)]
                               (reset! last-one now)
                               (done? now))))]
    (reset! running false)
    (.wakeup consumer)
    @polling
    {:held? held? :snapshot @last-one}))

(defn- recorder
  "A handler that keeps the messages it is given."
  []
  (let [seen (atom [])]
    {:seen    seen
     :handler (fn [message] (swap! seen conj message))}))

(deftest handling-test
  (testing "gives the handler each message, parsed, and commits what is done"
    (let [{:keys [seen handler]} (recorder)
          consumer (mock [{:offset 0 :value "{\"n\":0}"}
                          {:offset 1 :value "{\"n\":1}"}])
          result   (run-loop consumer {topic handler} {}
                             #(= 2 (get-in % [:committed 0])))]
      (is (:held? result))
      (is (= [{:n 0} {:n 1}] (mapv :value @seen)))
      (is (= [0 1] (mapv :offset @seen)))
      (is (= ["orders" "orders"] (mapv :topic @seen)))
      (is (= {0 2} (get-in result [:snapshot :committed])))))

  (testing "commits each partition for itself"
    (let [{:keys [seen handler]} (recorder)
          consumer (mock [{:offset 0 :partition 0 :value "{\"a\":1}"}
                          {:offset 0 :partition 1 :value "{\"b\":2}"}
                          {:offset 1 :partition 1 :value "{\"c\":3}"}])
          result   (run-loop consumer {topic handler} {}
                             #(= {0 1 1 2} (:committed %)))]
      (is (:held? result))
      (is (= 3 (count @seen)))))

  (testing "gives a tombstone as a nil value"
    (let [{:keys [seen handler]} (recorder)
          consumer (mock [{:offset 0 :value nil}])
          result   (run-loop consumer {topic handler} {}
                             #(= 1 (get-in % [:committed 0])))]
      (is (:held? result))
      (is (nil? (:value (first @seen)))))))

(deftest retry-test
  (testing "tries a handler that throws again, and goes on when it is done"
    (let [calls    (atom 0)
          handled  (atom [])
          handler  (fn [message]
                     (when (< (swap! calls inc) 3)
                       (throw (ex-info "not yet" {})))
                     (swap! handled conj message))
          consumer (mock [{:offset 0 :value "{\"n\":0}"}])
          result   (run-loop consumer {topic handler} {}
                             #(= 1 (get-in % [:committed 0])))]
      (is (:held? result))
      (is (= 3 @calls))
      (is (= 1 (count @handled))))))

(deftest failure-test
  (testing "reports a message that is not handled in the last attempt, and
            moves past it"
    (let [failures (atom [])
          calls    (atom 0)
          handler  (fn [_] (swap! calls inc) (throw (ex-info "always" {})))
          consumer (mock [{:offset 0 :value "{\"secret\":\"hunter2\"}"}])
          result   (run-loop consumer
                             {topic handler}
                             {:on-error #(swap! failures conj %)}
                             #(= 1 (get-in % [:committed 0])))]
      (is (:held? result))
      (is (= 3 @calls))
      (let [failed (first @failures)]
        (is (= {:error :handler-failed :topic topic :partition 0 :offset 0
                :key "key-0" :attempts 3}
               (dissoc failed :cause)))
        (is (= "always" (ex-message (:cause failed))))
        (is (not (str/includes? (pr-str (dissoc failed :cause)) "hunter2"))))))

  (testing "reports a value that is not JSON without calling the handler"
    (let [failures (atom [])
          called   (atom 0)
          handled  (atom [])
          handler  (fn [message]
                     (swap! called inc)
                     (swap! handled conj message))
          consumer (mock [{:offset 0 :value "{not json"}
                          {:offset 1 :value "{\"ok\":true}"}])
          result   (run-loop consumer
                             {topic handler}
                             {:on-error #(swap! failures conj %)}
                             #(= 2 (get-in % [:committed 0])))]
      (is (:held? result))
      (is (= [:invalid-json] (mapv :error @failures)))
      (is (= 1 @called))))

  (testing "logs and skips a message when there is no on-error, without what
            it said"
    (let [consumer (mock [{:offset 0 :value "{\"secret\":\"hunter2\"} oops"}])
          entries  (logging/call-capturing
                    #(run-loop consumer {topic identity} {}
                               (fn [now]
                                 (= 1 (get-in now [:committed 0])))))]
      (is (some #(and (= :error (:level %))
                      (str/includes? (:message %) "orders-0 at 0"))
                entries))
      (is (not-any? #(str/includes? (:message %) "hunter2") entries))))

  (testing "goes back to a message when on-error throws"
    (let [attempts (atom 0)
          on-error (fn [_]
                     (swap! attempts inc)
                     (throw (ex-info "dlq down" {})))
          consumer (mock [{:offset 0 :value "{bad"}])
          result   (run-loop consumer {topic identity} {:on-error on-error}
                             (fn [now]
                               (and (<= 1 @attempts)
                                    (= 0 (get-in now [:position 0])))))]
      (is (:held? result))
      (is (not (contains? (get-in result [:snapshot :committed]) 0)))
      (is (= 0 (get-in result [:snapshot :position 0]))))))

(defn- partition-offsets
  "Returns the offsets that are to be committed as a map of the number of the
   partition to the offset."
  [done]
  (into {}
        (map (fn [[topic-partition offset-and-metadata]]
               [(.partition ^TopicPartition topic-partition)
                (.offset ^OffsetAndMetadata offset-and-metadata)]))
        done))

(deftest stop-test
  (testing "stops handling at once, and commits only what is done"
    (let [running  (atom true)
          handled  (atom [])
          commits  (atom [])
          handler  (fn [message]
                     (swap! handled conj (:offset message))
                     (when (= 1 (:offset message))
                       (reset! running false)))
          consumer (mock [{:offset 0 :value "{\"n\":0}"}
                          {:offset 1 :value "{\"n\":1}"}
                          {:offset 2 :value "{\"n\":2}"}])]
      (with-redefs [kafka/commit! (fn [_consumer done]
                                    (swap! commits conj
                                           (partition-offsets done)))]
        (#'kafka/poll-loop! consumer
                            {topic handler}
                            (assoc (settings {}) :running? #(deref running))))
      (is (= [0 1] @handled))
      (is (= [{0 2}] @commits)))))

(deftest handlers-test
  (testing "fails the start for a topic that has no handler"
    (let [thrown (try (#'kafka/check-handlers ["a" "b"] {"a" identity})
                      (catch clojure.lang.ExceptionInfo e e))]
      (is (= {:error :borba.kafka-consumer/no-handler :topics ["b"]}
             (ex-data thrown)))))

  (testing "resolves the symbol of a handler, and loads its namespace"
    (let [handlers (ig/init-key :kafka/consumer-handlers
                                {"a" 'clojure.string/upper-case
                                 "b" identity})]
      (is (= "X" ((get handlers "a") "x")))
      (is (= identity (get handlers "b")))))

  (testing "has no handlers when there are none"
    (is (= {} (ig/init-key :kafka/consumer-handlers nil))))

  (testing "refuses a handler that is not a function, naming its topic"
    (is (= {:error   :borba.kafka-consumer/invalid-handler
            :topic   "a"
            :handler 5}
           (try (ig/init-key :kafka/consumer-handlers {"a" 5})
                (catch clojure.lang.ExceptionInfo e (ex-data e)))))))

(deftest component-test
  (testing "does nothing when there are no topics"
    (is (nil? (ig/init-key :components/kafka-consumer
                           {:bootstrap-servers "kafka:9092"
                            :group-id          "g"
                            :topics            []}))))

  (testing "refuses options that are not valid, before it connects"
    (is (= :borba.kafka-consumer.config/invalid-option
           (try (ig/init-key :components/kafka-consumer
                             {:bootstrap-servers "kafka:9092" :topics ["a"]})
                (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))))

  (testing "is not running when it was not started"
    (is (false? (kafka/running? nil))))

  (testing "has settings that are the ones of the config"
    (is (= 3 config/default-max-attempts))))
