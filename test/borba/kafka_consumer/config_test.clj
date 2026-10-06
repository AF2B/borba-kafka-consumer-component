(ns borba.kafka-consumer.config-test
  (:require
   [borba.kafka-consumer.config :as config]
   [clojure.test :refer [deftest is testing]]))

(def ^:private minimal
  {:bootstrap-servers "kafka:9092"
   :group-id          "orders"
   :topics            ["order-events"]})

(defn- invalid-option
  "Returns the option that the options are refused for, or nil."
  [options]
  (try (config/client-properties options)
       (config/loop-settings options)
       nil
       (catch clojure.lang.ExceptionInfo e
         (when (= :borba.kafka-consumer.config/invalid-option
                  (:error (ex-data e)))
           (:option (ex-data e))))))

(deftest client-properties-test
  (testing "commits nothing by itself, and reads what is committed"
    (let [properties (config/client-properties minimal)]
      (is (= false (get properties "enable.auto.commit")))
      (is (= "read_committed" (get properties "isolation.level")))
      (is (= "earliest" (get properties "auto.offset.reset")))))

  (testing "has the brokers, the group and the name"
    (is (= {"bootstrap.servers" "kafka:9092"
            "group.id"          "orders"
            "client.id"         "borba-consumer"}
           (select-keys (config/client-properties minimal)
                        ["bootstrap.servers" "group.id" "client.id"]))))

  (testing "takes the brokers as a vector"
    (is (= "a:9092,b:9092"
           (get (config/client-properties
                 (assoc minimal :bootstrap-servers ["a:9092" "b:9092"]))
                "bootstrap.servers"))))

  (testing "takes what it is told"
    (let [properties (config/client-properties
                      (assoc minimal
                             :client-id            "orders-1"
                             :auto-offset-reset    "latest"
                             :max-poll-records     10
                             :session-timeout-ms   20000
                             :max-poll-interval-ms 60000))]
      (is (= "orders-1" (get properties "client.id")))
      (is (= "latest" (get properties "auto.offset.reset")))
      (is (= 10 (get properties "max.poll.records")))
      (is (= 20000 (get properties "session.timeout.ms")))
      (is (= 60000 (get properties "max.poll.interval.ms")))))

  (testing "lets the properties of the client take the place of the others"
    (let [properties (config/client-properties
                      (assoc minimal
                             :properties
                             {"security.protocol" "SASL_SSL"
                              "isolation.level"   "read_uncommitted"}))]
      (is (= "SASL_SSL" (get properties "security.protocol")))
      (is (= "read_uncommitted" (get properties "isolation.level"))))))

(deftest loop-settings-test
  (testing "has attempts, a pause, and a limit to the stop, by default"
    (is (= {:topics              ["order-events"]
            :poll-timeout-ms     1000
            :max-attempts        3
            :retry-backoff-ms    500
            :shutdown-timeout-ms 10000
            :on-error            nil}
           (config/loop-settings minimal))))

  (testing "takes what it is told"
    (let [on-error (fn [_failure] nil)]
      (is (= {:topics              ["a" "b"]
              :poll-timeout-ms     100
              :max-attempts        5
              :retry-backoff-ms    10
              :shutdown-timeout-ms 2000
              :on-error            on-error}
             (config/loop-settings {:topics              ["a" "b"]
                                    :poll-timeout-ms     100
                                    :max-attempts        5
                                    :retry-backoff-ms    10
                                    :shutdown-timeout-ms 2000
                                    :on-error            on-error}))))))

(deftest invalid-options-test
  (testing "the brokers are host:port, or a vector of them"
    (doseq [bad [nil "" [] [""] [1] :kafka]]
      (is (= :bootstrap-servers
             (invalid-option (assoc minimal :bootstrap-servers bad)))
          (pr-str bad))))

  (testing "the group and the name are non-empty strings"
    (is (= :group-id (invalid-option (dissoc minimal :group-id))))
    (is (= :group-id (invalid-option (assoc minimal :group-id " "))))
    (is (= :client-id (invalid-option (assoc minimal :client-id "")))))

  (testing "the offset reset is one that Kafka has"
    (is (= :auto-offset-reset
           (invalid-option (assoc minimal :auto-offset-reset "first")))))

  (testing "the sizes and the timeouts are positive integers"
    (doseq [option [:max-poll-records :session-timeout-ms
                    :max-poll-interval-ms :poll-timeout-ms :max-attempts
                    :retry-backoff-ms :shutdown-timeout-ms]
            bad    [0 -1 1.5 "10"]]
      (is (= option (invalid-option (assoc minimal option bad)))
          (str option " " (pr-str bad)))))

  (testing "the topics are a vector of names"
    (is (= :topics (invalid-option (dissoc minimal :topics))))
    (is (= :topics (invalid-option (assoc minimal :topics "order-events"))))
    (is (= :topics (invalid-option (assoc minimal :topics [""])))))

  (testing "on-error is a function, or not given"
    (is (= :on-error (invalid-option (assoc minimal :on-error "log")))))

  (testing "the properties are strings to strings"
    (is (= :properties (invalid-option (assoc minimal :properties {:a "b"}))))))
