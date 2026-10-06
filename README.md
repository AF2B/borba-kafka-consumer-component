# borba-kafka-consumer-component

[![CI](https://github.com/AF2B/borba-kafka-consumer-component/actions/workflows/ci.yml/badge.svg)](https://github.com/AF2B/borba-kafka-consumer-component/actions/workflows/ci.yml)

A Kafka consumer for a Borba service, as an [Integrant](https://github.com/weavejester/integrant) component that reads topics and
gives each message to a handler. Delivery is at-least-once, a handler that throws is tried again, a message that cannot be handled
is reported and not silently lost, and a stop finishes the message in hand.

## Install

```clojure
io.github.af2b/borba-kafka-consumer-component
{:git/url "https://github.com/AF2B/borba-kafka-consumer-component"
 :git/tag "v2.0.0"
 :git/sha "<the commit of the tag, printed in the release notes>"}
```

It depends on Clojure, Integrant, `tools.logging`, [jsonista](https://github.com/metosin/jsonista) and the Apache Kafka client 4.3.1.

## Use

```clojure
{:service/namespaces [borba.kafka-consumer com.example.orders.events]

 :ig/system
 {:kafka/consumer-handlers
  {"order-events" com.example.orders.events/handle-order-event}

  :components/kafka-consumer
  {:bootstrap-servers #or [#env KAFKA_BROKERS "localhost:9092"]
   :group-id          "orders-service"
   :topics            ["order-events"]
   :handlers          #ig/ref :kafka/consumer-handlers}}}
```

A handler is a function of a message, which is a map of:

| Key | What it is |
|---|---|
| `:key` | The key of the record, as text, or nil |
| `:value` | The value, read as JSON with its keys as keywords, or nil for a tombstone |
| `:topic`, `:partition`, `:offset` | Where the record is |
| `:timestamp` | The time of the record, in milliseconds |
| `:headers` | The headers, a map of names to text |

```clojure
(defn handle-order-event
  [{:keys [key value headers]}]
  (orders/apply-event! key value))
```

The messages of a partition are given to the handler in order, one at a time. `:kafka/consumer-handlers` takes the handlers as
functions or as qualified symbols, whose namespace it loads, so a handler needs no registration in the configuration of
`:service/namespaces`. A topic without a handler fails the start naming it, because it would be read and thrown away.

| Option | What it is | Default |
|---|---|---|
| `:bootstrap-servers` | The brokers, `"host:port,host:port"` or a vector of `"host:port"` | required |
| `:group-id` | The group that shares the messages | required |
| `:topics` | The topics to read (none: the component does nothing) | required |
| `:handlers` | A map from each topic to its handler | required |
| `:client-id` | The name of the consumer for the brokers | `"borba-consumer"` |
| `:auto-offset-reset` | Where a group with no committed offset starts: `earliest`, `latest` or `none` | `"earliest"` |
| `:max-poll-records` | The most messages a poll returns | `100` |
| `:session-timeout-ms` | How long the group waits for a sign of life | `45000` |
| `:max-poll-interval-ms` | How long the handlers of a poll have to be done | `300000` |
| `:poll-timeout-ms` | How long a poll waits for messages | `1000` |
| `:max-attempts` | How many times a handler is tried for a message | `3` |
| `:retry-backoff-ms` | How long to wait before it is tried again | `500` |
| `:shutdown-timeout-ms` | How long a stop waits for the message in hand | `10000` |
| `:on-error` | A function of the failure of a message that could not be handled | none |
| `:verify-connection?`, `:verify-timeout-ms` | Whether the start asks the brokers who is in the cluster, and for how long | `true`, `5000` |
| `:properties` | More properties of the client, strings to strings: SASL, TLS | none |

## What at-least-once means

The consumer does not commit offsets by itself. It commits the offset of a message **when the handler is done with it**, so a message
that was in hand when the process died is given again to the next consumer of the group, and none is lost. The price is that a
message can be given twice, so a handler must be safe to run twice for it: an upsert, an idempotent request, a check for what was
already done. The consumer reads only what a transaction has committed (`read_committed`).

```
produce a, b, c        a handler is done with a and b, the process dies in c
                       the group has committed up to b
the process restarts   the first message is c
```

## What becomes of a message that cannot be handled

- **A handler that throws is tried again**, `:max-attempts` times, `:retry-backoff-ms` apart. A retry that is still going when the
  consumer is stopped is not the last one: the message is left for the next consumer.
- **A value that is not valid JSON is not tried again**, since it will not read any better. The value is read strictly: text after
  it, or an object with a key twice, is not valid, because what two readers would read differently is what an attack is made of.
- **What is still not handled is reported to `:on-error`**, a function of its failure, which can write it to a dead-letter topic or a
  table, and then the message is past:

```clojure
{:error     :handler-failed        ; or :invalid-json
 :topic     "order-events"
 :partition 0
 :offset    41
 :key       "order-1"
 :attempts  3
 :cause     #error {...}}          ; the exception
```

The failure has where the message was and never what it said, so it can be logged without leaking what is in the message. Without
`:on-error` the failure is logged, with the same, and the message is skipped.

- **When `:on-error` itself throws, the message is not past.** The consumer goes back to it, so a dead-letter topic that is away does
  not make the messages vanish, and it tries again after the backoff. A consumer that stops reading a partition because it cannot
  report is better than one that reads on and loses what it could not report.

## Stopping

A stop finishes the message in hand, commits the offsets of what is done, and closes the consumer, which hands its partitions to the
rest of the group at once. The messages that were polled and not yet handled are not committed, and are given again.

```
SIGTERM -> the handler of the message in hand is done (up to :shutdown-timeout-ms) -> the offsets are committed -> the consumer leaves the group
```

A stop that waits for longer than `:shutdown-timeout-ms` logs that it did and goes on; the handler is then still running in a thread
that does not keep the JVM alive.

`running?` is for a readiness check: true while the consumer is polling.

## Limits

**Brokers that do not answer fail the start.** The component asks the cluster who is in it, and when no one answers it fails, naming
the brokers and nothing else:

```clojure
(ig/init {:components/kafka-consumer {:bootstrap-servers "127.0.0.1:1" ...}})
;; throws the ExceptionInfo of Integrant, whose cause (ex-cause) is
;;   "the Kafka brokers do not answer on 127.0.0.1:1"
;;   {:error :borba.kafka-consumer/cannot-connect, :brokers "127.0.0.1:1"}
```

The handlers of a poll have `:max-poll-interval-ms` to be done, which is five minutes: when they are not, the group takes the
partitions away and gives them to another consumer. Retries count toward it, so `:max-attempts` times `:retry-backoff-ms` and the time of
the handler must be well inside it.

## API

| Name | What it does |
|---|---|
| `:components/kafka-consumer` | The Integrant key that starts the consumer and stops it |
| `:kafka/consumer-handlers` | The Integrant key that resolves the handlers |
| `running?` | Whether the consumer is polling |
| `borba.kafka-consumer.config` | The checked options, turned into the properties of the client |
| `borba.kafka-consumer.message` | What a handler is given, and the failure of a message |

## Tests

The unit suite runs anywhere; it runs the loop on the `MockConsumer` of Kafka and covers the options and the messages. The integration
suite runs against a real Kafka, which the pipeline provides, and which you can start with Docker:

```bash
docker run --rm -d --name borba-kafka-it -p 127.0.0.1:9092:9092 apache/kafka:4.0.0

KAFKA_BROKERS=127.0.0.1:9092 make test-integration
```

It covers delivery in order with the key, the value and the headers, a tombstone, a handler that throws until the third attempt, a consumer of
the same group that starts after another and is not given what was committed, the message in hand when the consumer is stopped, a value
that is not JSON, a message that is never handled, an `:on-error` that throws until it does not, two topics, and the start of a
consumer that cannot reach the brokers.

## Design notes

- **A message is lost only on purpose.** It is past when the handler is done, or when `:on-error` has taken it, and never because
  something else went wrong in between: the commit is the decision, and it is made by the code that knows.
- **The report has the place and not the content.** What a message said is the thing most likely to be sensitive, and the failure is the
  thing most likely to be logged.
- **A strict reader.** A value that would be two things to two readers is refused, as in `borba-handlers-component`.

## Development

```bash
make check      # lint, format, conventions, reflection, tests, coverage
make ci         # everything the pipelines enforce
```

See [CONTRIBUTING.md](CONTRIBUTING.md). The repository follows the [Borba standard](https://github.com/AF2B/borba-tooling/blob/main/docs/standard.md).

## License

[MIT](LICENSE)
