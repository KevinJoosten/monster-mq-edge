# MQTT Client Bridge: custom CA, mutual TLS, SNI and ALPN (issue #20)

Port of MonsterMQ Core's MQTT client mTLS (monster-mq #111, commit efa61eb5,
`dev/done/plan-mqtt-client-mtls-issue-111.md` section 3) to the edge bridge.
The GraphQL contract and the validation rules are the ones Andreas listed in
issue #20; the behaviour mirrors Core's `MqttConfig.validate()` and
`MqttClientTls.kt` unless a section below says otherwise.

This work is independent of #25 (MQTT 5 bridge client). It is built on the
current MQTT 3.1.1 bridge in `upstream/main`: the result is one `*tls.Config`,
which the MQTT 5 transport of #25 takes unchanged. Whichever of the two PRs
merges second gets a one-line rebase in `Connector.Start`.

## Fields

Stored in the bridge's device config JSON, next to the existing fields.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `tlsCaCertPath` | string | none | PEM file with the trusted CA certificates (one or more). Replaces the system roots when set. |
| `tlsClientCertPath` | string | none | Client certificate: PEM certificate (chain) or PKCS12 bundle, see `tlsClientKeyFormat`. |
| `tlsClientKeyPath` | string | none | PEM private key. Not used with PKCS12. |
| `tlsClientKeyPassword` | string | none | Password of the key or the PKCS12 bundle. Write-only. |
| `tlsClientKeyFormat` | string | `PEM` | `PEM` or `PKCS12` (case-insensitive on load). |
| `tlsAlpnProtocols` | list of strings | none | ALPN protocols, e.g. `x-amzn-mqtt-ca` for AWS IoT Core on port 443. |
| `tlsServerName` | string | URL host | SNI and the host name the server certificate is verified against. |

`sslVerifyCertificate` already exists in the schema and the stored config but
the connector ignores it today. It becomes effective: `false` skips all server
certificate checks (the CA file is then ignored, as in Core).

Blank paths and an empty password count as not set.

## GraphQL (schema change, specified by the maintainer in #20)

Copied verbatim from Core's SDL, comments included:

- `MqttClientConnectionConfigInput`: `tlsCaCertPath`, `tlsClientCertPath`,
  `tlsClientKeyPath`, `tlsClientKeyPassword`, `tlsClientKeyFormat: String = "PEM"`,
  `tlsAlpnProtocols: [String!]`, `tlsServerName`. All nullable.
- `MqttClientConnectionConfig`: the same without the password, plus
  `tlsClientKeyPasswordSet: Boolean!`; `tlsClientKeyFormat: String!`.

The password is never returned.

Update merge (`mqttClientConfigInputToMergedMap`), as Core's
`mergeMqttClientTlsForUpdate`:

- A `tls*` path or `tlsServerName` that is null keeps the stored value; `""` clears it.
- `tlsAlpnProtocols` null keeps the stored list; `[]` clears it.
- `tlsClientKeyPassword` null keeps the stored value; `""` clears it. It is
  dropped when the merged config has no `tlsClientCertPath`.
- A stored `PKCS12` format is kept when the input carries the default `PEM` and
  no certificate or key path (an older dashboard that does not send the fields).

Create (`mqttClientConfigInputToMap`) stores what is set.

## Validation

At create and update the mutation returns `success: false` with the
messages. Messages are Core's, word for word. Connector start repeats only the
`tls*` rules: the URL and client ID checks are new, and refusing a stored
config at start that ran before would break existing bridges.

- `brokerUrl cannot be blank`
- `brokerUrl must include a protocol (tcp://, ssl://, ws://, or wss://)` when
  there is no scheme; an unknown scheme gives `protocol must be one of: ...`
  listing the schemes the edge accepts (`tcp, mqtt, ssl, tls, mqtts, ws, wss, unix`).
  The edge keeps accepting `mqtt://`, `tls://`, `mqtts://` and `unix://`,
  which existing configs use.
- `brokerUrl must include a hostname` (not for `unix://`)
- `clientId cannot be blank` (GraphQL only: a stored config without a client
  ID keeps getting the generated `edge-<node>-<name>` at start)
- `TLS options (tls*) require an ssl:// or wss:// brokerUrl` when any of the
  CA, certificate, key, ALPN or server name is set and the URL is not a TLS URL
  (`ssl`, `tls`, `mqtts`, `wss`). The password alone does not count.
- PEM: `tlsClientKeyPath is required when tlsClientCertPath is set` and
  `tlsClientCertPath is required when tlsClientKeyPath is set`.
- PKCS12: `tlsClientCertPath (PKCS12 bundle) is required when tlsClientKeyFormat is PKCS12`
  and `tlsClientKeyPath is not used with tlsClientKeyFormat PKCS12, the key is part of the bundle`.
- Other formats: `tlsClientKeyFormat must be 'PEM' or 'PKCS12'`.
- ALPN: `invalid tlsAlpnProtocols entry '<x>': must be 1-255 printable ASCII characters`.

Core also checks keepAlive, reconnectDelay, connectionTimeout, bufferSize and
bufferImplementation. They are out of scope here: the edge accepts values Core
refuses (e.g. reconnectDelay in seconds), and refusing them would break
existing configs.

## Runtime

`buildTLSConfig(cfg Config) (*tls.Config, error)` in the bridge package:

- `MinVersion` TLS 1.2, as today.
- `InsecureSkipVerify = !sslVerifyCertificate`.
- `RootCAs` from `tlsCaCertPath` (all PEM certificates in the file) when verification is on.
- `Certificates`:
  - PEM: certificate chain from `tlsClientCertPath`; key from `tlsClientKeyPath`
    as PKCS#1, SEC1 EC, PKCS#8, encrypted PKCS#8 (`github.com/youmark/pkcs8`,
    already in go.sum) or legacy encrypted PEM (`x509.DecryptPEMBlock`). Key and
    certificate must match (`tls.X509KeyPair`).
  - PKCS12: `software.sslmate.com/src/go-pkcs12` `DecodeChain`, which reads
    bundles from current OpenSSL (AES) as well as legacy ones. New dependency,
    pure Go, named in the contract.
- `ServerName` = `tlsServerName`, `NextProtos` = `tlsAlpnProtocols`.

Error messages, as Core (`<field>` is the field name, `<path>` its value):

- `<field> '<path>': file not found` / `<field> '<path>': file is not readable`
- `tlsCaCertPath '<path>': no certificates found`
- `tlsClientCertPath '<path>': cannot open PKCS12 bundle (wrong tlsClientKeyPassword?): <err>`
- `tlsClientCertPath '<path>': PKCS12 bundle contains no private key`
- `tlsClientKeyPath '<path>': private key is encrypted but no tlsClientKeyPassword is set`
- `tlsClientKeyPath '<path>': cannot decrypt or read private key (wrong tlsClientKeyPassword?): <err>`
- `tlsClientKeyPath '<path>': no PEM private key found`
- `tlsClientKeyPath '<key>' does not match the certificate in tlsClientCertPath '<cert>'`

The TLS config is built once in `Connector.Start` for a TLS URL, before the
client, queue and goroutines are created. A failing `tls*` rule makes `Start`
return `Invalid MQTT client configuration: ...`, a TLS build error
`Invalid MQTT client TLS configuration: ...`.
The manager already logs a failed start (`bridge start failed`) and does not
keep or retry the connector, which is Core's behaviour: a config error is not
retried. The edge's `MqttClient` type has no error field, so the log is where
the error shows; adding one would be a further schema change.

## Open points (asked in #20, no answer yet)

The implementation follows the contract. Both proposals stay out of the PR
unless Andreas agrees:

1. Retrying with backoff when a certificate file is missing or unreadable, and
   re-reading the client certificate on every reconnect (renewal without a
   restart). Core builds the TLS config once and does not retry.
2. Reading the old edge field names `caFile`, `clientCertFile` and
   `clientKeyFile` as a fallback. These exist only in our fork; the fork keeps
   that fallback on its own side.

## Tests

- Unit (`internal/bridge/mqttclient`): `buildTLSConfig` with PEM RSA, PEM EC,
  encrypted PKCS#8, legacy encrypted PEM, PKCS12, CA file, ALPN and server
  name, verify off; each error message (missing file, no certificates, wrong
  password, no password for an encrypted key, key/cert mismatch, PKCS12
  without key). Validation: every rule.
- Unit (`internal/graphql/resolvers`): the create map and the update merge
  rules above, `tlsClientKeyPasswordSet`, the password never in the output.
- Integration (`test/integration/bridge_mtls_test.go`), reusing
  `createTestCertificates` / `startMTLSBroker` from `mtls_test.go`:
  - a bridge with a client certificate and the test CA connects to an mTLS
    edge broker and forwards a message;
  - without a client certificate the bridge does not get in;
  - ALPN and SNI reach the server (checked on a TLS listener that records the
    ClientHello);
  - a bridge with an invalid TLS config does not start (and is not retried).
- Existing bridge tests pass unchanged.

## Docs

README section "MQTT bridge TLS": the fields, a private-CA example, AWS IoT
Core on 8883 and on 443 with ALPN `x-amzn-mqtt-ca`, and a parity note
(same fields and messages as Core; certificates are read at bridge start, a
renewed certificate needs a bridge restart or a toggle of the device).
