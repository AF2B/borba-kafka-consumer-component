(ns borba.kafka-consumer.message-test
  (:require
   [borba.kafka-consumer.message :as message]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]])
  (:import
   (java.nio.charset StandardCharsets)
   (org.apache.kafka.clients.consumer ConsumerRecord)))

(set! *warn-on-reflection* true)

(defn- record
  "A record as the client gives it."
  ^ConsumerRecord [^String topic
                   partition-number
                   offset
                   ^String k
                   ^String value]
  (ConsumerRecord. topic (int partition-number) (long offset) k value))

(deftest raw-message-test
  (testing "is where it was, what it said and its headers"
    (let [consumer-record (record "orders" 2 41 "k-1" "{\"a\":1}")]
      (.add (.headers consumer-record)
            "request-id"
            (.getBytes "abc" StandardCharsets/UTF_8))
      (is (= {:key       "k-1"
              :value     "{\"a\":1}"
              :topic     "orders"
              :partition 2
              :offset    41
              :headers   {"request-id" "abc"}}
             (dissoc (message/raw-message consumer-record) :timestamp)))))

  (testing "has no key, or no value, when the record has none"
    (let [raw (message/raw-message (record "orders" 0 0 nil nil))]
      (is (nil? (:key raw)))
      (is (nil? (:value raw))))))

(deftest parse-test
  (testing "reads the value as JSON, with keys as keywords"
    (is (= {:total 10 :items [{:sku "a"}]}
           (:value (message/parse
                    (message/raw-message
                     (record "orders" 0 0 "k"
                             "{\"total\":10,\"items\":[{\"sku\":\"a\"}]}")))))))

  (testing "leaves a tombstone as nil"
    (let [parsed (message/parse
                  (message/raw-message (record "orders" 0 0 "k" nil)))]
      (is (nil? (:value parsed)))
      (is (not (message/failure? parsed)))))

  (testing "keeps the rest of the message"
    (let [parsed (message/parse
                  (message/raw-message (record "orders" 3 9 "k" "{\"a\":1}")))]
      (is (= ["orders" 3 9 "k"]
             ((juxt :topic :partition :offset :key) parsed))))))

(deftest invalid-json-test
  (testing "is a failure, for what is not valid"
    (doseq [text ["{" "not json" "{\"a\":1} trailing" "{\"a\":1,\"a\":2}" ""]]
      (let [failed (message/parse
                    (message/raw-message (record "orders" 1 7 "k" text)))]
        (is (message/failure? failed) text)
        (is (= :invalid-json (:error failed)) text))))

  (testing "says where it was, and never what it said"
    (let [text   "{\"password\":\"hunter2\" oops"
          failed (message/parse
                  (message/raw-message (record "orders" 1 7 "k-1" text)))]
      (is (= {:error :invalid-json :topic "orders" :partition 1 :offset 7
              :key "k-1" :attempts 1}
             (dissoc failed :cause)))
      (is (instance? Exception (:cause failed)))
      (is (not (str/includes? (pr-str (dissoc failed :cause)) "hunter2"))))))

(deftest failure-test
  (testing "has the place, the error, the attempts, and the cause if any"
    (let [cause (RuntimeException. "boom")]
      (is (= {:error :handler-failed :topic "t" :partition 0 :offset 3
              :key "k" :attempts 3 :cause cause}
             (message/failure {:topic "t" :partition 0 :offset 3 :key "k"
                               :value {:secret true}}
                              :handler-failed
                              3
                              cause)))
      (is (not (contains? (message/failure {:topic "t" :value 1}
                                           :handler-failed 1 nil)
                          :cause))))))
