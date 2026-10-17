package kafkaclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/youmark/pkcs8"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

func TestParseJAAS(t *testing.T) {
	got := parseJAAS(`org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="p\"w";`)
	want := map[string]string{"username": "alice", "password": `p"w`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseJAAS = %v, want %v", got, want)
	}
}

// Every key is accepted, as in Core; keys without an edge equivalent, or meant
// for the other client, are only reported.
func TestIgnoredProperties(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ExtraConsumerConfig = map[string]string{
		"auto.offset.reset": "earliest", "fetch.min.bytes": "1", "key.deserializer": "x",
		"check.crcs": "true", "acks": "all",
	}
	cfg.ExtraProducerConfig = map[string]string{"acks": "all", "transactional.id": "t1", "group.id": "g"}
	got := cfg.IgnoredProperties()
	want := []string{
		"extraConsumerConfig.acks", "extraConsumerConfig.check.crcs",
		"extraProducerConfig.group.id", "extraProducerConfig.transactional.id",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ignored = %v, want %v", got, want)
	}
}

func TestTranslatePropsConsumerAndProducer(t *testing.T) {
	cs, err := translateProps(map[string]string{
		"auto.offset.reset": "earliest", "enable.auto.commit": "false", "session.timeout.ms": "10000",
		"max.poll.records": "50", "group.id": "g2", "client.id": "cid", "bootstrap.servers": "a:1, b:2",
		"isolation.level": "read_committed", "fetch.max.bytes": "1048576", "heartbeat.interval.ms": "3000",
		"partition.assignment.strategy": "org.apache.kafka.clients.consumer.CooperativeStickyAssignor,range",
		"allow.auto.create.topics":      "false", "retry.backoff.ms": "100", "unknown.key": "x",
	}, roleConsumer)
	if err != nil {
		t.Fatal(err)
	}
	if cs.maxPollRecords != 50 || cs.groupID != "g2" || cs.clientID != "cid" || !reflect.DeepEqual(cs.bootstrap, []string{"a:1", "b:2"}) {
		t.Fatalf("consumer settings = %+v", cs)
	}
	// reset, autocommit, session, heartbeat, fetch max bytes, isolation,
	// balancers, retry backoff; no auto topic creation.
	if len(cs.opts) != 8 {
		t.Errorf("consumer opts = %d, want 8", len(cs.opts))
	}
	if !reflect.DeepEqual(cs.ignored, []string{"unknown.key"}) {
		t.Errorf("ignored = %v", cs.ignored)
	}

	ps, err := translateProps(map[string]string{
		"acks": "1", "compression.type": "zstd", "linger.ms": "5", "batch.size": "16384",
		"retries": "3", "delivery.timeout.ms": "30000", "max.in.flight.requests.per.connection": "1",
	}, roleProducer)
	if err != nil {
		t.Fatal(err)
	}
	// auto topic creation, acks, idempotence off, compression, linger,
	// delivery timeout, batch size, in-flight, retries
	if len(ps.opts) != 9 {
		t.Errorf("producer opts = %d, want 9", len(ps.opts))
	}

	for _, bad := range []map[string]string{
		{"acks": "2"}, {"compression.type": "brotli"}, {"linger.ms": "-1"}, {"retries": "x"},
		{"security.protocol": "SASL_SSL", "sasl.mechanism": "GSSAPI"},
		{"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "OAUTHBEARER"}, {"security.protocol": "BOGUS"},
	} {
		if _, err := translateProps(bad, roleProducer); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
	for _, bad := range []map[string]string{
		{"isolation.level": "dirty"}, {"partition.assignment.strategy": "magic"}, {"auto.offset.reset": "middle"},
	} {
		if _, err := translateProps(bad, roleConsumer); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
	if _, err := translateProps(map[string]string{"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "SCRAM-SHA-512",
		"sasl.jaas.config": `x required username="u" password="p";`}, roleConsumer); err != nil {
		t.Errorf("SCRAM rejected: %v", err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	f := exponentialBackoff(100*time.Millisecond, time.Second)
	for tries, want := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 4: 800 * time.Millisecond, 10: time.Second} {
		if got := f(tries); got != want {
			t.Errorf("tries %d: %v, want %v", tries, got, want)
		}
	}
}

func writeTestPKI(t *testing.T) (dir string, certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "kafka-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	dir = t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.WriteFile(filepath.Join(dir, "ca.pem"), certPEM, 0o600))
	must(os.WriteFile(filepath.Join(dir, "client.pem"), append(append([]byte{}, certPEM...), keyPEM...), 0o600))
	ks, err := pkcs12.Modern.Encode(key, cert, nil, "secret")
	must(err)
	must(os.WriteFile(filepath.Join(dir, "client.p12"), ks, 0o600))
	ts, err := pkcs12.Modern.EncodeTrustStore([]*x509.Certificate{cert}, "trust")
	must(err)
	must(os.WriteFile(filepath.Join(dir, "trust.p12"), ts, 0o600))
	return dir, certPEM, keyPEM
}

func TestBuildTLSStores(t *testing.T) {
	dir, certPEM, keyPEM := writeTestPKI(t)

	tc, err := buildTLS(map[string]string{
		"ssl.truststore.location": filepath.Join(dir, "ca.pem"), "ssl.truststore.type": "PEM",
		"ssl.keystore.location": filepath.Join(dir, "client.pem"), "ssl.keystore.type": "PEM",
	})
	if err != nil {
		t.Fatalf("PEM stores: %v", err)
	}
	if tc.RootCAs == nil || len(tc.Certificates) != 1 || tc.InsecureSkipVerify {
		t.Fatalf("PEM tls config = %+v", tc)
	}

	tc, err = buildTLS(map[string]string{
		"ssl.truststore.location": filepath.Join(dir, "trust.p12"), "ssl.truststore.password": "trust",
		"ssl.keystore.location": filepath.Join(dir, "client.p12"), "ssl.keystore.password": "secret",
		"ssl.endpoint.identification.algorithm": "",
	})
	if err != nil {
		t.Fatalf("PKCS12 stores: %v", err)
	}
	if tc.RootCAs == nil || len(tc.Certificates) != 1 || !tc.InsecureSkipVerify || tc.VerifyConnection == nil {
		t.Fatalf("PKCS12 tls config = %+v", tc)
	}

	if _, err := buildTLS(map[string]string{"ssl.keystore.certificate.chain": string(certPEM), "ssl.keystore.key": string(keyPEM)}); err != nil {
		t.Fatalf("inline PEM: %v", err)
	}

	_, err = buildTLS(map[string]string{"ssl.keystore.location": filepath.Join(dir, "client.p12"), "ssl.keystore.password": "wrong"})
	if err == nil || !strings.Contains(err.Error(), "ssl.keystore.password") {
		t.Fatalf("wrong keystore password err = %v", err)
	}
	if _, err := buildTLS(map[string]string{"ssl.truststore.location": filepath.Join(dir, "ca.pem"), "ssl.truststore.type": "JKS"}); err == nil {
		t.Fatal("JKS truststore accepted")
	}
}

// The Kafka PEM keystore keeps an encrypted PKCS#8 key opened with
// ssl.key.password.
func TestEncryptedPEMKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	encDER, err := pkcs8.MarshalPrivateKey(key, []byte("kpass"), nil)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: encDER})

	props := map[string]string{"ssl.keystore.certificate.chain": string(certPEM), "ssl.keystore.key": string(keyPEM)}
	if _, err := buildTLS(props); err == nil || !strings.Contains(err.Error(), "ssl.key.password") {
		t.Fatalf("missing password err = %v", err)
	}
	props["ssl.key.password"] = "wrong"
	if _, err := buildTLS(props); err == nil {
		t.Fatal("wrong password accepted")
	}
	props["ssl.key.password"] = "kpass"
	tc, err := buildTLS(props)
	if err != nil {
		t.Fatal(err)
	}
	if len(tc.Certificates) != 1 || tc.Certificates[0].Leaf == nil || tc.Certificates[0].Leaf.Subject.CommonName != "client" {
		t.Fatalf("certificate = %+v", tc.Certificates)
	}
}
