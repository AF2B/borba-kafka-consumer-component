# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- At-least-once delivery. The consumer commits the offset of a message when its handler is done with it, where it committed on a
  timer whatever the handler had done, so a message that was in hand when the process died is given again and none is lost.
- Retries: a handler that throws is tried again, `:max-attempts` times (3), `:retry-backoff-ms` (500 milliseconds) apart.
- `:on-error`, a function of the failure of a message that could not be handled, which can write it to a dead-letter topic. The
  failure has where the message was and never what it said. A message whose value is not valid JSON is reported without calling the
  handler. When `:on-error` throws, the consumer goes back to the message instead of moving past it.
- Strict JSON for the value: text after it and an object with a key twice are not valid.
- `:headers` and `:timestamp` in the message.
- A stop that finishes the message in hand, commits what is done, and closes the consumer from the thread that polled, within
  `:shutdown-timeout-ms`. It slept a second and a half and closed the consumer from another thread.
- `running?`, for a readiness check.
- The start fails naming the topics that have no handler, and brokers that do not answer fail it with `::cannot-connect` and where they
  were. `:verify-connection? false` turns the second off.
- `:auto-offset-reset`, `:max-poll-records`, `:session-timeout-ms`, `:max-poll-interval-ms`, `:poll-timeout-ms` and `:properties`, for the
  security protocol, SASL and TLS. The options are checked when the system starts.
- A test suite with an integration suite against a real Kafka, run by the pipeline.

### Changed

- **Breaking:** the consumer reads only what a transaction has committed (`read_committed`).
- **Breaking:** a handler that throws is tried again and then reported, where it was logged and the message was lost. The message is
  `{:key :value :topic :partition :offset :timestamp :headers}`, with the value as strict JSON.
- Moves to the Apache Kafka client 4.3.1 and Integrant 1.0. JSON is read with jsonista, and Cheshire is no longer a dependency. The
  component logs through `tools.logging`, and no longer depends on a logging backend.
- The published library is named `io.github.af2b/borba-kafka-consumer-component`.

### Removed

- The repository of Confluent, which nothing used.

### Security

- Pins Jackson to 2.22.3 and `lz4-java` to 1.12.0. The 2.22.2 that jsonista 1.0.1 brings has four high advisories (GHSA-7hhh-6rmp-j9qf,
  GHSA-p6pp-m3f8-5c89, GHSA-cxp5-3px4-pw24 and GHSA-wv8q-qhhj-9h54), and the 1.10.2 that the Kafka client brings has a medium one
  (GHSA-xx22-p4ch-683r).

## [1.0.0] - 2026-03-29

First release: the `:components/kafka-consumer` and `:kafka/consumer-handlers` Integrant components, which poll topics in a thread and
give each message to a handler.

[Unreleased]: https://github.com/AF2B/borba-kafka-consumer-component/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/AF2B/borba-kafka-consumer-component/releases/tag/v1.0.0
