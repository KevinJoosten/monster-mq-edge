package kafkaclient

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/youmark/pkcs8"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

// extraConsumerConfig / extraProducerConfig hold standard Kafka client
// properties (the configuration names Apache Kafka documents), the same
// interface as in Core, which hands them to its Kafka client unchanged. Edge
// accepts every key Core accepts and applies the ones below to franz-go.
// Keys without an equivalent are logged at start and otherwise ignored, as a
// Kafka client ignores configuration it does not know.

// propRole says on which client a Kafka property has an effect.
type propRole int

const (
	roleBoth propRole = iota
	roleConsumer
	roleProducer
)

// kafkaProps lists every property edge applies, with the client it affects.
var kafkaProps = map[string]propRole{
	// connection, security
	"bootstrap.servers":                     roleBoth,
	"client.id":                             roleBoth,
	"security.protocol":                     roleBoth,
	"sasl.mechanism":                        roleBoth,
	"sasl.jaas.config":                      roleBoth,
	"ssl.truststore.location":               roleBoth,
	"ssl.truststore.password":               roleBoth,
	"ssl.truststore.type":                   roleBoth,
	"ssl.truststore.certificates":           roleBoth,
	"ssl.keystore.location":                 roleBoth,
	"ssl.keystore.password":                 roleBoth,
	"ssl.keystore.type":                     roleBoth,
	"ssl.keystore.certificate.chain":        roleBoth,
	"ssl.keystore.key":                      roleBoth,
	"ssl.key.password":                      roleBoth,
	"ssl.endpoint.identification.algorithm": roleBoth,
	"request.timeout.ms":                    roleBoth,
	"connections.max.idle.ms":               roleBoth,
	"metadata.max.age.ms":                   roleBoth,
	"socket.connection.setup.timeout.ms":    roleBoth,
	"retry.backoff.ms":                      roleBoth,
	"retry.backoff.max.ms":                  roleBoth,
	// consumer
	"group.id":                      roleConsumer,
	"group.instance.id":             roleConsumer,
	"auto.offset.reset":             roleConsumer,
	"enable.auto.commit":            roleConsumer,
	"auto.commit.interval.ms":       roleConsumer,
	"session.timeout.ms":            roleConsumer,
	"heartbeat.interval.ms":         roleConsumer,
	"max.poll.interval.ms":          roleConsumer,
	"max.poll.records":              roleConsumer,
	"fetch.min.bytes":               roleConsumer,
	"fetch.max.bytes":               roleConsumer,
	"fetch.max.wait.ms":             roleConsumer,
	"max.partition.fetch.bytes":     roleConsumer,
	"isolation.level":               roleConsumer,
	"partition.assignment.strategy": roleConsumer,
	"allow.auto.create.topics":      roleConsumer,
	"client.rack":                   roleConsumer,
	// producer
	"acks":                                  roleProducer,
	"enable.idempotence":                    roleProducer,
	"compression.type":                      roleProducer,
	"linger.ms":                             roleProducer,
	"batch.size":                            roleProducer,
	"retries":                               roleProducer,
	"delivery.timeout.ms":                   roleProducer,
	"max.request.size":                      roleProducer,
	"buffer.memory":                         roleProducer,
	"max.in.flight.requests.per.connection": roleProducer,
	// serialisation follows payloadFormat, as when Core sets these itself
	"key.deserializer":   roleConsumer,
	"value.deserializer": roleConsumer,
	"key.serializer":     roleProducer,
	"value.serializer":   roleProducer,
}

// IgnoredProperties lists the extra config keys (prefixed with the map name)
// edge has no equivalent for, sorted. They are accepted, as in Core, and only
// logged when the connector starts.
func (c Config) IgnoredProperties() []string {
	var out []string
	for _, k := range ignoredKeys(c.ExtraConsumerConfig, roleConsumer) {
		out = append(out, "extraConsumerConfig."+k)
	}
	for _, k := range ignoredKeys(c.ExtraProducerConfig, roleProducer) {
		out = append(out, "extraProducerConfig."+k)
	}
	sort.Strings(out)
	return out
}

func ignoredKeys(props map[string]string, role propRole) []string {
	var out []string
	for k := range props {
		r, ok := kafkaProps[k]
		if !ok || (r != roleBoth && r != role) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// clientSettings is the translation of one extra*Config map.
type clientSettings struct {
	opts           []kgo.Opt
	bootstrap      []string
	clientID       string
	groupID        string
	maxPollRecords int
	ignored        []string // keys without an equivalent, for the start log
}

// translateProps builds the franz-go options for one client from its Kafka
// properties. The caller applies Core's own defaults first, so a property
// here wins, as with Core's putAll. A value the property does not allow is an
// error, as it is for a Kafka client at construction.
func translateProps(props map[string]string, role propRole) (clientSettings, error) {
	s := clientSettings{ignored: ignoredKeys(props, role)}
	p := propReader{props: props}

	if v, ok := p.str("bootstrap.servers"); ok && v != "" {
		s.bootstrap = splitServers(v)
	}
	if v, ok := p.str("client.id"); ok && v != "" {
		s.clientID = v
	}

	protocol := "PLAINTEXT"
	if v, ok := p.str("security.protocol"); ok && v != "" {
		protocol = strings.ToUpper(v)
	}
	useTLS, useSASL := false, false
	switch protocol {
	case "PLAINTEXT":
	case "SSL":
		useTLS = true
	case "SASL_PLAINTEXT":
		useSASL = true
	case "SASL_SSL":
		useTLS, useSASL = true, true
	default:
		return s, fmt.Errorf("security.protocol %q is not supported (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)", protocol)
	}
	if useTLS {
		tc, err := buildTLS(props)
		if err != nil {
			return s, err
		}
		s.opts = append(s.opts, kgo.DialTLSConfig(tc))
	}
	if useSASL {
		mech, err := buildSASL(props)
		if err != nil {
			return s, err
		}
		s.opts = append(s.opts, mech)
	}

	p.millis("request.timeout.ms", func(d time.Duration) kgo.Opt { return kgo.RequestTimeoutOverhead(d) }, &s)
	p.millis("connections.max.idle.ms", func(d time.Duration) kgo.Opt { return kgo.ConnIdleTimeout(d) }, &s)
	p.millis("metadata.max.age.ms", func(d time.Duration) kgo.Opt { return kgo.MetadataMaxAge(d) }, &s)
	p.millis("socket.connection.setup.timeout.ms", func(d time.Duration) kgo.Opt { return kgo.DialTimeout(d) }, &s)
	if base, ok := p.duration("retry.backoff.ms"); ok {
		limit, ok := p.duration("retry.backoff.max.ms")
		if !ok {
			limit = time.Second // the standard client default
		}
		s.opts = append(s.opts, kgo.RetryBackoffFn(exponentialBackoff(base, limit)))
	}

	switch role {
	case roleConsumer:
		consumerProps(&s, &p)
	case roleProducer:
		producerProps(&s, &p)
	}
	return s, p.err
}

func consumerProps(s *clientSettings, p *propReader) {
	if v, ok := p.str("group.id"); ok && v != "" {
		s.groupID = v
	}
	if v, ok := p.str("group.instance.id"); ok && v != "" {
		s.opts = append(s.opts, kgo.InstanceID(v))
	}
	if v, ok := p.str("auto.offset.reset"); ok {
		switch strings.ToLower(v) {
		case "latest":
			s.opts = append(s.opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
		case "earliest":
			s.opts = append(s.opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		case "none":
			s.opts = append(s.opts, kgo.ConsumeResetOffset(kgo.NoResetOffset()))
		default:
			p.fail("auto.offset.reset", v, "latest, earliest, none")
		}
	}
	if b, ok := p.bool("enable.auto.commit"); ok && !b {
		s.opts = append(s.opts, kgo.DisableAutoCommit())
	}
	p.millis("auto.commit.interval.ms", func(d time.Duration) kgo.Opt { return kgo.AutoCommitInterval(d) }, s)
	p.millis("session.timeout.ms", func(d time.Duration) kgo.Opt { return kgo.SessionTimeout(d) }, s)
	p.millis("heartbeat.interval.ms", func(d time.Duration) kgo.Opt { return kgo.HeartbeatInterval(d) }, s)
	// franz-go's rebalance timeout is the group's max.poll.interval.ms.
	p.millis("max.poll.interval.ms", func(d time.Duration) kgo.Opt { return kgo.RebalanceTimeout(d) }, s)
	p.millis("fetch.max.wait.ms", func(d time.Duration) kgo.Opt { return kgo.FetchMaxWait(d) }, s)
	if n, ok := p.positive("max.poll.records"); ok {
		s.maxPollRecords = n
	}
	if n, ok := p.positive32("fetch.min.bytes"); ok {
		s.opts = append(s.opts, kgo.FetchMinBytes(n))
	}
	if n, ok := p.positive32("fetch.max.bytes"); ok {
		s.opts = append(s.opts, kgo.FetchMaxBytes(n))
	}
	if n, ok := p.positive32("max.partition.fetch.bytes"); ok {
		s.opts = append(s.opts, kgo.FetchMaxPartitionBytes(n))
	}
	if v, ok := p.str("isolation.level"); ok {
		switch strings.ToLower(v) {
		case "read_uncommitted":
			s.opts = append(s.opts, kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
		case "read_committed":
			s.opts = append(s.opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
		default:
			p.fail("isolation.level", v, "read_uncommitted, read_committed")
		}
	}
	if v, ok := p.str("partition.assignment.strategy"); ok && v != "" {
		var balancers []kgo.GroupBalancer
		for _, name := range strings.Split(v, ",") {
			b, ok := balancer(strings.TrimSpace(name))
			if !ok {
				p.fail("partition.assignment.strategy", name, "range, roundrobin, sticky, cooperative-sticky or the matching assignor class")
				continue
			}
			balancers = append(balancers, b)
		}
		if len(balancers) > 0 {
			s.opts = append(s.opts, kgo.Balancers(balancers...))
		}
	}
	// The standard consumer lets the broker create a missing topic.
	if b, ok := p.bool("allow.auto.create.topics"); !ok || b {
		s.opts = append(s.opts, kgo.AllowAutoTopicCreation())
	}
	if v, ok := p.str("client.rack"); ok && v != "" {
		s.opts = append(s.opts, kgo.Rack(v))
	}
}

// balancer maps a partition.assignment.strategy entry, short name or assignor
// class name, to its franz-go balancer.
func balancer(name string) (kgo.GroupBalancer, bool) {
	short := strings.ToLower(name[strings.LastIndex(name, ".")+1:])
	switch short {
	case "range", "rangeassignor":
		return kgo.RangeBalancer(), true
	case "roundrobin", "roundrobinassignor":
		return kgo.RoundRobinBalancer(), true
	case "sticky", "stickyassignor":
		return kgo.StickyBalancer(), true
	case "cooperative-sticky", "cooperativestickyassignor":
		return kgo.CooperativeStickyBalancer(), true
	}
	return nil, false
}

func producerProps(s *clientSettings, p *propReader) {
	// The standard producer lets the broker create a missing topic.
	s.opts = append(s.opts, kgo.AllowAutoTopicCreation())
	idempotent := true
	if b, ok := p.bool("enable.idempotence"); ok {
		idempotent = b
	}
	if v, ok := p.str("acks"); ok {
		switch strings.ToLower(v) {
		case "all", "-1":
			s.opts = append(s.opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		case "1":
			s.opts = append(s.opts, kgo.RequiredAcks(kgo.LeaderAck()))
			idempotent = false
		case "0":
			s.opts = append(s.opts, kgo.RequiredAcks(kgo.NoAck()))
			idempotent = false
		default:
			p.fail("acks", v, "0, 1, all, -1")
		}
	}
	if !idempotent {
		s.opts = append(s.opts, kgo.DisableIdempotentWrite())
	}
	if v, ok := p.str("compression.type"); ok {
		codecs := map[string]func() kgo.CompressionCodec{
			"none": kgo.NoCompression, "gzip": kgo.GzipCompression, "snappy": kgo.SnappyCompression,
			"lz4": kgo.Lz4Compression, "zstd": kgo.ZstdCompression,
		}
		if codec, ok := codecs[strings.ToLower(v)]; ok {
			s.opts = append(s.opts, kgo.ProducerBatchCompression(codec()))
		} else {
			p.fail("compression.type", v, "none, gzip, snappy, lz4, zstd")
		}
	}
	p.millis("linger.ms", func(d time.Duration) kgo.Opt { return kgo.ProducerLinger(d) }, s)
	p.millis("delivery.timeout.ms", func(d time.Duration) kgo.Opt { return kgo.RecordDeliveryTimeout(d) }, s)
	// On the producer the request timeout is also the broker's ack timeout.
	p.millis("request.timeout.ms", func(d time.Duration) kgo.Opt { return kgo.ProduceRequestTimeout(d) }, s)
	if n, ok := p.positive32("batch.size"); ok {
		s.opts = append(s.opts, kgo.ProducerBatchMaxBytes(n))
	}
	if n, ok := p.positive32("max.request.size"); ok {
		s.opts = append(s.opts, kgo.BrokerMaxWriteBytes(n))
	}
	if n, ok := p.positive("buffer.memory"); ok {
		s.opts = append(s.opts, kgo.MaxBufferedBytes(n))
	}
	if n, ok := p.positive("max.in.flight.requests.per.connection"); ok {
		s.opts = append(s.opts, kgo.MaxProduceRequestsInflightPerBroker(n))
	}
	if v, ok := p.str("retries"); ok {
		n, err := strconv.ParseInt(v, 10, 64)
		switch {
		case err != nil || n < 0:
			p.fail("retries", v, "a non-negative integer")
		case n < int64(^uint(0)>>1):
			s.opts = append(s.opts, kgo.RecordRetries(int(n)))
		}
	}
}

func exponentialBackoff(base, limit time.Duration) func(int) time.Duration {
	return func(tries int) time.Duration {
		d := base
		for i := 1; i < tries && d < limit; i++ {
			d *= 2
		}
		return min(d, limit)
	}
}

// propReader reads typed property values and keeps the first invalid one.
type propReader struct {
	props map[string]string
	err   error
}

func (p *propReader) str(key string) (string, bool) {
	v, ok := p.props[key]
	return strings.TrimSpace(v), ok
}

func (p *propReader) fail(key, value, allowed string) {
	if p.err == nil {
		p.err = fmt.Errorf("%s %q is not supported (%s)", key, value, allowed)
	}
}

func (p *propReader) bool(key string) (bool, bool) {
	v, ok := p.str(key)
	if !ok {
		return false, false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail(key, v, "true, false")
		return false, false
	}
	return b, true
}

func (p *propReader) duration(key string) (time.Duration, bool) {
	v, ok := p.str(key)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		p.fail(key, v, "milliseconds, a non-negative integer")
		return 0, false
	}
	return time.Duration(n) * time.Millisecond, true
}

func (p *propReader) millis(key string, opt func(time.Duration) kgo.Opt, s *clientSettings) {
	if d, ok := p.duration(key); ok {
		s.opts = append(s.opts, opt(d))
	}
}

func (p *propReader) positive(key string) (int, bool) {
	v, ok := p.str(key)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		p.fail(key, v, "a positive integer")
		return 0, false
	}
	return n, true
}

func (p *propReader) positive32(key string) (int32, bool) {
	v, ok := p.str(key)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil || n <= 0 {
		p.fail(key, v, "a positive integer")
		return 0, false
	}
	return int32(n), true
}

func splitServers(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

var jaasOption = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*"((?:[^"\\]|\\.)*)"`)

// parseJAAS extracts the quoted options of a sasl.jaas.config login module
// line, e.g. `...PlainLoginModule required username="u" password="p";`.
func parseJAAS(s string) map[string]string {
	out := map[string]string{}
	for _, m := range jaasOption.FindAllStringSubmatch(s, -1) {
		out[m[1]] = strings.NewReplacer(`\"`, `"`, `\\`, `\`).Replace(m[2])
	}
	return out
}

func buildSASL(props map[string]string) (kgo.Opt, error) {
	mech := strings.ToUpper(strings.TrimSpace(props["sasl.mechanism"]))
	if mech == "" {
		mech = "GSSAPI" // the standard Kafka client default
	}
	jaas := parseJAAS(props["sasl.jaas.config"])
	user, pass := jaas["username"], jaas["password"]
	switch mech {
	case "PLAIN":
		return kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()), nil
	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()), nil
	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()), nil
	case "OAUTHBEARER":
		// Real token fetching needs a login callback handler; edge accepts a
		// static token in the JAAS options instead.
		token := jaas["token"]
		if token == "" {
			return nil, fmt.Errorf("sasl.mechanism OAUTHBEARER needs token=\"...\" in sasl.jaas.config")
		}
		return kgo.SASL(oauth.Auth{Token: token}.AsMechanism()), nil
	default:
		return nil, fmt.Errorf("sasl.mechanism %q is not supported (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER)", mech)
	}
}

func buildTLS(props map[string]string) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if v, ok := props["ssl.endpoint.identification.algorithm"]; ok && strings.TrimSpace(v) == "" {
		// An empty algorithm disables hostname verification only. Go
		// cannot split that from chain verification, so verify the chain
		// against the roots ourselves.
		tc.InsecureSkipVerify = true
		tc.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("kafka: server sent no certificate")
			}
			opts := x509.VerifyOptions{Roots: tc.RootCAs, Intermediates: x509.NewCertPool()}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	}

	roots, err := loadTrustStore(props)
	if err != nil {
		return nil, err
	}
	tc.RootCAs = roots

	cert, err := loadKeyStore(props)
	if err != nil {
		return nil, err
	}
	if cert != nil {
		tc.Certificates = []tls.Certificate{*cert}
	}
	return tc, nil
}

func storeType(props map[string]string, key, location string) string {
	if t := strings.ToUpper(strings.TrimSpace(props[key])); t != "" {
		return t
	}
	lower := strings.ToLower(location)
	if strings.HasSuffix(lower, ".pem") || strings.HasSuffix(lower, ".crt") {
		return "PEM"
	}
	// The standard Kafka client defaults to JKS, which edge cannot read;
	// PKCS12 is the usual store format today.
	return "PKCS12"
}

func loadTrustStore(props map[string]string) (*x509.CertPool, error) {
	if inline := strings.TrimSpace(props["ssl.truststore.certificates"]); inline != "" {
		return poolFromPEM([]byte(inline), "ssl.truststore.certificates")
	}
	loc := strings.TrimSpace(props["ssl.truststore.location"])
	if loc == "" {
		return nil, nil // system roots
	}
	data, err := os.ReadFile(loc)
	if err != nil {
		return nil, fmt.Errorf("ssl.truststore.location: %v", err)
	}
	switch t := storeType(props, "ssl.truststore.type", loc); t {
	case "PEM":
		return poolFromPEM(data, "ssl.truststore.location")
	case "PKCS12":
		certs, err := pkcs12.DecodeTrustStore(data, props["ssl.truststore.password"])
		if err != nil {
			return nil, fmt.Errorf("ssl.truststore.location '%s': cannot open PKCS12 truststore: %v", loc, err)
		}
		pool := x509.NewCertPool()
		for _, c := range certs {
			pool.AddCert(c)
		}
		return pool, nil
	default:
		return nil, fmt.Errorf("ssl.truststore.type %q is not supported (PEM, PKCS12)", t)
	}
}

func poolFromPEM(data []byte, field string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("%s: no PEM certificates found", field)
	}
	return pool, nil
}

func loadKeyStore(props map[string]string) (*tls.Certificate, error) {
	if chain := strings.TrimSpace(props["ssl.keystore.certificate.chain"]); chain != "" {
		cert, err := pemKeyPair([]byte(chain), []byte(props["ssl.keystore.key"]), props["ssl.key.password"])
		if err != nil {
			return nil, fmt.Errorf("ssl.keystore.certificate.chain/ssl.keystore.key: %v", err)
		}
		return cert, nil
	}
	loc := strings.TrimSpace(props["ssl.keystore.location"])
	if loc == "" {
		return nil, nil
	}
	data, err := os.ReadFile(loc)
	if err != nil {
		return nil, fmt.Errorf("ssl.keystore.location: %v", err)
	}
	switch t := storeType(props, "ssl.keystore.type", loc); t {
	case "PEM":
		cert, err := pemKeyPair(data, data, props["ssl.key.password"])
		if err != nil {
			return nil, fmt.Errorf("ssl.keystore.location '%s': %v", loc, err)
		}
		return cert, nil
	case "PKCS12":
		pass := props["ssl.keystore.password"]
		key, leaf, chain, err := pkcs12.DecodeChain(data, pass)
		if err != nil {
			return nil, fmt.Errorf("ssl.keystore.location '%s': cannot open PKCS12 keystore (wrong ssl.keystore.password?): %v", loc, err)
		}
		cert := &tls.Certificate{Certificate: [][]byte{leaf.Raw}, PrivateKey: key, Leaf: leaf}
		for _, ca := range chain {
			cert.Certificate = append(cert.Certificate, ca.Raw)
		}
		return cert, nil
	default:
		return nil, fmt.Errorf("ssl.keystore.type %q is not supported (PEM, PKCS12)", t)
	}
}

// pemKeyPair builds a certificate from a PEM certificate chain and a PEM
// private key. As in the Kafka PEM keystore, an encrypted key is PKCS#8 and
// is opened with ssl.key.password.
func pemKeyPair(certPEM, keyPEM []byte, keyPassword string) (*tls.Certificate, error) {
	var keyBlock *pem.Block
	for rest := keyPEM; ; {
		var b *pem.Block
		if b, rest = pem.Decode(rest); b == nil {
			break
		}
		if strings.HasSuffix(b.Type, "PRIVATE KEY") {
			keyBlock = b
			break
		}
	}
	if keyBlock == nil {
		return nil, fmt.Errorf("no PEM private key found")
	}
	if keyBlock.Type != "ENCRYPTED PRIVATE KEY" {
		cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(keyBlock))
		if err != nil {
			return nil, err
		}
		return &cert, nil
	}
	if keyPassword == "" {
		return nil, fmt.Errorf("private key is encrypted but ssl.key.password is not set")
	}
	key, err := pkcs8.ParsePKCS8PrivateKey(keyBlock.Bytes, []byte(keyPassword))
	if err != nil {
		return nil, fmt.Errorf("cannot decrypt private key (wrong ssl.key.password?): %v", err)
	}
	cert := &tls.Certificate{PrivateKey: key}
	for rest := certPEM; ; {
		var b *pem.Block
		if b, rest = pem.Decode(rest); b == nil {
			break
		}
		if b.Type == "CERTIFICATE" {
			cert.Certificate = append(cert.Certificate, b.Bytes)
		}
	}
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("no PEM certificate found")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	cert.Leaf = leaf
	return cert, nil
}
