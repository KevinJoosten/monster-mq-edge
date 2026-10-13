package mqttclient

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"

	"github.com/youmark/pkcs8"
	"software.sslmate.com/src/go-pkcs12"
)

// buildTLSConfig returns the bridge's TLS client config. The CA file and the
// client certificate are read on every handshake, so a file that appears or is
// renewed later is used without a restart. The error reports files that fail
// to load right now; the returned config is usable either way and fails its
// handshakes with the same message until the files are right.
func buildTLSConfig(c Config) (*tls.Config, error) {
	c = c.trimTLS()
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: c.TlsAlpnProtocols,
		ServerName: c.serverName(),
	}
	var errs []error
	if c.SslVerifyCertificate != nil && !*c.SslVerifyCertificate {
		cfg.InsecureSkipVerify = true
	} else if c.TlsCaCertPath != "" {
		// Go's own check would use a pool fixed now; VerifyConnection checks
		// against the pool read for each handshake instead.
		cfg.InsecureSkipVerify = true
		path, name := c.TlsCaCertPath, cfg.ServerName
		cfg.VerifyConnection = func(cs tls.ConnectionState) error { return verifyServer(cs, path, name) }
		if _, err := loadCAPool(path); err != nil {
			errs = append(errs, err)
		}
	}
	if c.TlsClientCertPath != "" {
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return loadClientCertificate(c)
		}
		if _, err := loadClientCertificate(c); err != nil {
			errs = append(errs, err)
		}
	}
	return cfg, errors.Join(errs...)
}

// serverName is the SNI and the name the server certificate must carry:
// tlsServerName, else the broker URL host.
func (c Config) serverName() string {
	if c.TlsServerName != "" {
		return c.TlsServerName
	}
	if u, err := url.Parse(c.BrokerURL); err == nil {
		return u.Hostname()
	}
	return ""
}

func verifyServer(cs tls.ConnectionState, caPath, name string) error {
	pool, err := loadCAPool(caPath)
	if err != nil {
		return err
	}
	if len(cs.PeerCertificates) == 0 {
		return errors.New("tls: server sent no certificate")
	}
	if name == "" {
		// x509 skips the host name check for an empty name; Go's default refuses.
		return errors.New("tls: no host name to verify the server certificate against: set tlsServerName")
	}
	inter := x509.NewCertPool()
	for _, ic := range cs.PeerCertificates[1:] {
		inter.AddCert(ic)
	}
	_, err = cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter, DNSName: name})
	return err
}

func readTLSFile(field, path string) ([]byte, error) {
	if fi, err := os.Stat(path); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("%s '%s': file not found", field, path)
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%s '%s': file not found", field, path)
	}
	if err != nil {
		return nil, fmt.Errorf("%s '%s': file is not readable", field, path)
	}
	return data, nil
}

func loadCertificates(field, path string) ([]*x509.Certificate, error) {
	data, err := readTLSFile(field, path)
	if err != nil {
		return nil, err
	}
	var certs []*x509.Certificate
	for {
		var b *pem.Block
		if b, data = pem.Decode(data); b == nil {
			break
		}
		if b.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(b.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%s '%s': cannot parse certificates: %v", field, path, err)
		}
		certs = append(certs, c)
	}
	if len(certs) == 0 {
		return nil, fmt.Errorf("%s '%s': no certificates found", field, path)
	}
	return certs, nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	certs, err := loadCertificates("tlsCaCertPath", path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	for _, c := range certs {
		pool.AddCert(c)
	}
	return pool, nil
}

func loadClientCertificate(c Config) (*tls.Certificate, error) {
	if strings.EqualFold(c.TlsClientKeyFormat, "PKCS12") {
		data, err := readTLSFile("tlsClientCertPath", c.TlsClientCertPath)
		if err != nil {
			return nil, err
		}
		key, leaf, chain, err := pkcs12.DecodeChain(data, c.TlsClientKeyPassword)
		if key == nil && err != nil && strings.Contains(err.Error(), "private key missing") {
			return nil, fmt.Errorf("tlsClientCertPath '%s': PKCS12 bundle contains no private key", c.TlsClientCertPath)
		}
		if err != nil {
			return nil, fmt.Errorf("tlsClientCertPath '%s': cannot open PKCS12 bundle (wrong tlsClientKeyPassword?): %v", c.TlsClientCertPath, err)
		}
		if !keyMatches(leaf, key) {
			return nil, fmt.Errorf("tlsClientCertPath '%s': the private key in the PKCS12 bundle does not match its certificate", c.TlsClientCertPath)
		}
		cert := &tls.Certificate{Certificate: [][]byte{leaf.Raw}, PrivateKey: key, Leaf: leaf}
		for _, ca := range chain {
			cert.Certificate = append(cert.Certificate, ca.Raw)
		}
		return cert, nil
	}
	certs, err := loadCertificates("tlsClientCertPath", c.TlsClientCertPath)
	if err != nil {
		return nil, err
	}
	key, err := loadPEMKey(c.TlsClientKeyPath, c.TlsClientKeyPassword)
	if err != nil {
		return nil, err
	}
	if !keyMatches(certs[0], key) {
		return nil, fmt.Errorf("tlsClientKeyPath '%s' does not match the certificate in tlsClientCertPath '%s'", c.TlsClientKeyPath, c.TlsClientCertPath)
	}
	cert := &tls.Certificate{PrivateKey: key, Leaf: certs[0]}
	for _, x := range certs {
		cert.Certificate = append(cert.Certificate, x.Raw)
	}
	return cert, nil
}

func keyMatches(cert *x509.Certificate, key any) bool {
	signer, ok := key.(crypto.Signer)
	pub, okPub := cert.PublicKey.(interface{ Equal(crypto.PublicKey) bool })
	return ok && okPub && pub.Equal(signer.Public())
}

// loadPEMKey reads PKCS#1, SEC1 EC and PKCS#8 keys, encrypted PKCS#8, and
// legacy OpenSSL-encrypted PEM.
func loadPEMKey(path, password string) (any, error) {
	data, err := readTLSFile("tlsClientKeyPath", path)
	if err != nil {
		return nil, err
	}
	var b *pem.Block
	for {
		if b, data = pem.Decode(data); b == nil || strings.HasSuffix(b.Type, "PRIVATE KEY") {
			break
		}
	}
	if b == nil {
		return nil, fmt.Errorf("tlsClientKeyPath '%s': no PEM private key found", path)
	}
	//lint:ignore SA1019 legacy encrypted PEM keys are still in use
	encrypted := b.Type == "ENCRYPTED PRIVATE KEY" || x509.IsEncryptedPEMBlock(b)
	if encrypted && password == "" {
		return nil, fmt.Errorf("tlsClientKeyPath '%s': private key is encrypted but no tlsClientKeyPassword is set", path)
	}
	var key any
	switch {
	case b.Type == "ENCRYPTED PRIVATE KEY":
		key, err = pkcs8.ParsePKCS8PrivateKey(b.Bytes, []byte(password))
	case encrypted:
		var der []byte
		//lint:ignore SA1019 legacy encrypted PEM keys are still in use
		if der, err = x509.DecryptPEMBlock(b, []byte(password)); err == nil {
			key, err = parseKeyDER(der)
		}
	default:
		key, err = parseKeyDER(b.Bytes)
	}
	if err != nil {
		return nil, fmt.Errorf("tlsClientKeyPath '%s': cannot decrypt or read private key (wrong tlsClientKeyPassword?): %v", path, err)
	}
	return key, nil
}

func parseKeyDER(der []byte) (any, error) {
	if k, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return k, nil
	}
	return x509.ParseECPrivateKey(der)
}
