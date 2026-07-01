package broker

import (
	"bytes"
	"crypto/tls"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"monstermq.io/edge/internal/auth"
	"monstermq.io/edge/internal/config"
)

// AuthHook bridges mochi's Auth/ACL callbacks to our Cache.
// When CertAuth is enabled, it extracts the OU from the client's TLS peer
// certificate and uses it as the ACL username, bypassing password checks.
type AuthHook struct {
	mqtt.HookBase
	cache    *auth.Cache
	certAuth config.CertAuthConfig
}

func NewAuthHook(cache *auth.Cache, certAuth config.CertAuthConfig) *AuthHook {
	return &AuthHook{cache: cache, certAuth: certAuth}
}

func (h *AuthHook) ID() string { return "monstermq-auth" }

func (h *AuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{mqtt.OnConnectAuthenticate, mqtt.OnACLCheck}, []byte{b})
}

func (h *AuthHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	if h.certAuth.Enabled {
		if role := h.peerRole(cl); role != "" {
			cl.Properties.Username = []byte(role)
			return true
		}
	}
	username := string(cl.Properties.Username)
	password := string(pk.Connect.Password)
	return h.cache.Validate(username, password)
}

func (h *AuthHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	username := string(cl.Properties.Username)
	return h.cache.Allow(username, topic, write)
}

// peerRole extracts the role (OU by default) from the client's TLS peer certificate.
func (h *AuthHook) peerRole(cl *mqtt.Client) string {
	conn := cl.Net.Conn
	if conn == nil {
		return ""
	}
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return ""
	}
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return ""
	}
	cert := state.PeerCertificates[0]
	switch h.certAuth.EffectiveRoleField() {
	case "OU":
		if len(cert.Subject.OrganizationalUnit) > 0 {
			return cert.Subject.OrganizationalUnit[0]
		}
	case "CN":
		return cert.Subject.CommonName
	case "O":
		if len(cert.Subject.Organization) > 0 {
			return cert.Subject.Organization[0]
		}
	}
	return ""
}
