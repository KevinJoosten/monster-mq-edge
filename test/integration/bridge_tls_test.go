package integration

import (
	"crypto/tls"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A TLS broker URL keeps the bridge's TLS behaviour on both transports: TLS
// 1.2 or later, SNI from the URL, and the server certificate verified against
// the system roots. The test certificate is not trusted there, so the bridge
// must refuse the handshake.
func TestBridgeTLSVerifiesServerCertificate(t *testing.T) {
	certs := createTestCertificates(t)
	cert, err := tls.LoadX509KeyPair(certs.serverCert, certs.serverKey)
	if err != nil {
		t.Fatal(err)
	}
	for i, pv := range []int{5, 4} {
		t.Run(fmt.Sprintf("protocolVersion %d", pv), func(t *testing.T) {
			remotePort, localPort := 27861+2*i, 27862+2*i
			hellos := make(chan *tls.ClientHelloInfo, 8)
			results := make(chan error, 8)
			l, err := tls.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", remotePort), &tls.Config{
				Certificates: []tls.Certificate{cert},
				GetConfigForClient: func(h *tls.ClientHelloInfo) (*tls.Config, error) {
					select {
					case hellos <- h:
					default:
					}
					return nil, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					go func() {
						err := c.(*tls.Conn).Handshake()
						select {
						case results <- err:
						default:
						}
						_ = c.Close()
					}()
				}
			}()

			b := chainBridge(remotePort, "tls-bridge")
			b["brokerUrl"] = fmt.Sprintf("ssl://localhost:%d", remotePort)
			b["protocolVersion"] = pv
			db := filepath.Join(t.TempDir(), "local.db")
			seedBridges(t, db, "local", map[string]map[string]any{"to-remote": b})
			local := startNode(t, "local", localPort, db)
			t.Cleanup(func() { _ = local.Close() })

			select {
			case h := <-hellos:
				if h.ServerName != "localhost" {
					t.Errorf("SNI = %q, want localhost", h.ServerName)
				}
				for _, v := range h.SupportedVersions {
					if v < tls.VersionTLS12 {
						t.Errorf("the bridge offers TLS version 0x%04x, want 1.2 or later only", v)
					}
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the bridge never started a TLS handshake")
			}
			select {
			case err := <-results:
				if err == nil || !strings.Contains(err.Error(), "remote error") {
					t.Errorf("handshake = %v, want the bridge to refuse the untrusted certificate", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the TLS handshake never finished")
			}
		})
	}
}
