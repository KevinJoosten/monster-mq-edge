# Kafka client bridge

The Kafka client bridge connects the edge broker to an external Kafka cluster.
It is the Go port of MonsterMQ Core's `KAFKA-Client` device (see Core's
`doc/kafka.md`): device configs, the GraphQL API and the record formats are
the same, so a device can be exported from Core and imported here, and both
brokers can share a Kafka topic.

Enable it with:

```yaml
Features:
  Kafka: true
```

`enabledFeatures` then reports `"Kafka"`, and the dashboard shows the Kafka
client pages. Devices are managed through `kafkaClients` / `kafkaClient { ... }`
in GraphQL, or imported with `importDevices`.

## Directions

- **Inbound (Kafka -> MQTT)**, on by default (`inboundEnabled`): consumes the
  Kafka topic named after the device **namespace** with consumer group
  `groupId`, client id `kafkaclient-<name>`, auto-commit and
  `auto.offset.reset=latest`.
- **Outbound (MQTT -> Kafka)**, off by default (`outboundEnabled`): subscribes
  to `outboundTopicFilters` on the local broker and produces to
  `outboundKafkaTopic` (or the namespace when blank) with client id
  `kafkaclient-producer-<name>`. The record key is the MQTT topic after
  `outboundTopicKeyRegex` / `outboundTopicKeyReplacement`.

## Config

| Key | Default | Notes |
|---|---|---|
| `bootstrapServers` | `localhost:9092` | Comma separated `host:port` list. |
| `groupId` | `monstermq-subscriber` | |
| `payloadFormat` | `DEFAULT` | `DEFAULT`, `JSON`, `BINARY`, `TEXT`. |
| `extraConsumerConfig` / `extraProducerConfig` | `{}` | Kafka client properties, see below. |
| `reconnectDelayMs` | 5000 | >= 500. |
| `destinationTopicPrefix` | none | Prepended to every inbound MQTT topic. Trailing `/` added, no `+`/`#`. Legacy `keyTopicPrefix` is accepted. |
| `topicKeyRegex` / `topicKeyReplacement` | none | Inbound key -> topic rewrite (replacement syntax as in Core, `$1`). Applied only when both are set. |
| `inboundEnabled` | true | |
| `outboundEnabled` | false | |
| `outboundTopicFilters` | `[]` | Required when outbound is enabled. |
| `outboundKafkaTopic` | namespace | |
| `outboundTopicKeyRegex` / `outboundTopicKeyReplacement` | none | |

A config nested under an outer `"kafka"` key is accepted.

## Payload formats

| Format | Inbound | Outbound |
|---|---|---|
| `DEFAULT` | Value is Core's `BrokerMessageCodec` binary envelope; the topic and flags come from it. A value that does not decode is tried as a JSON envelope (with a warning). | `BrokerMessageCodec` envelope, bit-compatible with Core. |
| `JSON` | JSON envelope: `topicName` or `topic`, `payloadBase64` or `payload`, `qosLevel`, `isRetain`, `isDup`, `isQueued`, `messageUuid`, `messageId`, `clientId`, `time`. | Same fields, written with `topic`. |
| `BINARY` | Record key = MQTT topic, value = payload. Records with a null key are dropped. QoS 0, not retained. | Raw payload. |
| `TEXT` | As `BINARY`, value read as UTF-8. | Payload as UTF-8. |

`destinationTopicPrefix` applies to all four inbound formats.

## Kafka client properties

`extraConsumerConfig` / `extraProducerConfig` hold standard Kafka client
properties, the configuration names Apache Kafka documents. This is the same
interface as in Core, which hands the maps to its Kafka client unchanged.
Edge accepts every key Core accepts and applies these to its Kafka client
(franz-go):

- connection: `bootstrap.servers`, `client.id`, `request.timeout.ms`,
  `connections.max.idle.ms`, `metadata.max.age.ms`,
  `socket.connection.setup.timeout.ms`, `retry.backoff.ms`,
  `retry.backoff.max.ms`
- `security.protocol`: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`
- `sasl.mechanism` + `sasl.jaas.config`: `PLAIN`, `SCRAM-SHA-256`,
  `SCRAM-SHA-512` (username/password from the JAAS line); `OAUTHBEARER` with a
  static `token="..."` JAAS option
- `ssl.truststore.{location,password,type}`, `ssl.truststore.certificates`,
  `ssl.keystore.{location,password,type}`, `ssl.keystore.certificate.chain`,
  `ssl.keystore.key`, `ssl.key.password`: `PEM` (also encrypted PKCS#8 keys)
  and `PKCS12`
- `ssl.endpoint.identification.algorithm` (empty disables the host name check;
  the chain is still verified)
- consumer: `group.id`, `group.instance.id`, `auto.offset.reset`,
  `enable.auto.commit`, `auto.commit.interval.ms`, `session.timeout.ms`,
  `heartbeat.interval.ms`, `max.poll.interval.ms`, `max.poll.records`,
  `fetch.min.bytes`, `fetch.max.bytes`, `fetch.max.wait.ms`,
  `max.partition.fetch.bytes`, `isolation.level`,
  `partition.assignment.strategy`, `allow.auto.create.topics`, `client.rack`
- producer: `acks`, `enable.idempotence`, `compression.type`, `linger.ms`,
  `batch.size`, `retries`, `delivery.timeout.ms`, `request.timeout.ms`,
  `max.request.size`, `buffer.memory`, `max.in.flight.requests.per.connection`
- serializer / deserializer keys are accepted; the payload format decides.

Like the standard clients, both clients let the broker create a missing topic
(`allow.auto.create.topics` turns it off for the consumer).

No key is ever refused. A key without an equivalent on edge (for example
`transactional.id`, `check.crcs`, or a producer key in the consumer map) is
logged when the device starts. A value a property does not allow (for example
`sasl.mechanism=GSSAPI`, a JKS store, `acks=2`) fails the device start with an
error naming the property, as it fails the Kafka client in Core.

## Differences from Core

Deliberate, edge-only behaviour:

1. Inbound messages are always published under client id `kafkaclient-<name>`
   (Core keeps the envelope's `clientId` for `DEFAULT`/`JSON`). Loop
   prevention therefore works for every format.
2. MQTT 5 properties carried in the `DEFAULT` envelope (content type,
   response topic, correlation data, user properties, expiry, payload format
   indicator) are kept on inbound publish; Core drops them.
3. `messagesIn` counts only records actually published; dropped records count
   in `messagesDropped`, decode failures also in `errors`.
4. Outbound messages wait in a bounded buffer (10000) while Kafka is slow or
   down; on overflow the oldest is dropped and counted. A failed Kafka client
   is recreated after `reconnectDelayMs`.
5. GraphQL create/update also validates the regexes, topic filters and
   `payloadFormat`.
6. Kafka client properties are applied to franz-go (above); every key is
   accepted, as in Core.
7. While `Features.Kafka` is on, network MQTT clients cannot use client ids
   starting with `kafkaclient-`.
8. Per-device metrics are not published under `$SYS/sessions/kafka/...`.
9. `kafkaClient.update` refuses an input name that differs from the device
   name. Core saves under the new name and leaves the old device running.

Smaller differences that follow from edge's engine and libraries:

- Records written for a PeerLink replica carry no peer block or origin node in
  the `DEFAULT` trailer, and an empty content type, response topic or
  correlation data is encoded as absent.
- The local message bus, shared with the MQTT bridge, drops messages for a
  subscriber more than 1024 behind without counting them; the bridge moves
  them into its own 10000-message buffer right away, so this needs a stalled
  process.
- `messagesOut` counts acknowledged records and failed sends count in
  `errors`; Core counts on send.
- A QoS above 2 in an incoming envelope is published as QoS 0.
- Regexes use Go's RE2 syntax (no backreferences or lookaround; `$` and `.`
  treat line terminators slightly differently), and an empty match right after
  a match is not replaced again. Replacement strings follow Core exactly.
- An outbound filter on a WinCC OA native namespace also sends the current
  values when the device starts.
- A device with `nodeId` `local` starts at boot; Core starts it only after a
  config change.
- Timestamps in GraphQL use RFC 3339 with nanoseconds (Core trims the fraction
  to 3, 6 or 9 digits).

## Metrics

Per device: `messagesIn` / `messagesOut` rates through `KafkaClient.metrics`
and `metricsHistory` (metric kind `kafka`, as Core). The stored samples also
hold `messagesDropped`, `errors` and `lastRecordTs`. Broker metrics report the
totals as `kafkaClientIn` / `kafkaClientOut`.

As in Core, `metrics` returns the newest stored sample of the last minute (else
the running connector's latest sample), rates are rounded to two decimals, and
`metricsHistory` needs `lastMinutes` or `from` (optionally `to`); it returns the
samples newest first.

## PeerLink

With a shared config store, a device with `nodeId` `*` or `local` and inbound
enabled runs on every node, so its records arrive twice; startup logs a
warning. Startup also warns when `bootstrapServers` names a PeerLink peer.
Outbound skips PeerLink replicas unless `PeerLink.Receive.BridgeOutbound` is set.

## Testing against a cluster

The live tests in `test/integration/kafka_client_test.go` skip unless these
are set:

- `KAFKA_BOOTSTRAP`: bootstrap servers, e.g. `scada:9092`
- `KAFKA_READ_TOPIC`: a topic with traffic that is only ever consumed (TEXT
  format, key regex, prefix); the test refuses any config that could write to it
- `KAFKA_WRITE_TOPIC`: a topic the round-trip test produces to and consumes
  back, in all four formats

```bash
go test ./test/integration -run TestKafkaClient
```
