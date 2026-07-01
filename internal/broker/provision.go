package broker

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
)

// provisionAgent runs a temporary HTTP server that serves a QR code
// containing a one-time bootstrap challenge. An external provisioning CA
// presents the challenge back via a POST to complete enrollment.
type provisionAgent struct {
	cfg           config.ProvisionConfig
	renewalCfg    config.CertRenewalConfig
	nodeID        string
	logger        *slog.Logger
	publish       publishFunc
	onProvisioned func() // called once after first successful enrollment
	deviceStore   stores.DeviceConfigStore
	srv           *http.Server
	listener      net.Listener

	mu        sync.Mutex
	challenge string
	completed bool
	certPEM   []byte
}

// ProvisionQR is the JSON payload encoded in the QR code.
type ProvisionQR struct {
	NodeID    string `json:"nodeId"`
	Challenge string `json:"challenge"`
	Endpoint  string `json:"endpoint"`
}

// ProvisionRequest is the JSON body POSTed by the provisioning CA.
type ProvisionRequest struct {
	Challenge string            `json:"challenge"`
	CertPEM   string            `json:"certPem"`
	KeyPEM    string            `json:"keyPem"`
	CaPEM     string            `json:"caPem"`
	Bridges   []ProvisionBridge `json:"bridges,omitempty"`
}

// ProvisionBridge is one MQTT bridge config delivered during enrollment.
type ProvisionBridge struct {
	Name          string `json:"name"`
	Config        string `json:"config"` // raw JSON matching mqttclient.Config
	ClientCertPEM string `json:"clientCertPem,omitempty"`
	ClientKeyPEM  string `json:"clientKeyPem,omitempty"`
}

func startProvisionAgent(ctx context.Context, cfg *config.Config, publish publishFunc, deviceStore stores.DeviceConfigStore, logger *slog.Logger) (*provisionAgent, error) {
	pa := &provisionAgent{
		cfg:         cfg.Provision,
		renewalCfg:  cfg.CertRenewal,
		nodeID:      cfg.NodeID,
		logger:      logger.With("component", "provision"),
		publish:     publish,
		deviceStore: deviceStore,
	}

	// Generate challenge token
	challengeBytes := pa.cfg.ChallengeBytes
	if challengeBytes <= 0 {
		challengeBytes = 32
	}
	token := make([]byte, challengeBytes)
	if _, err := rand.Read(token); err != nil {
		return nil, fmt.Errorf("generate challenge: %w", err)
	}
	pa.challenge = hex.EncodeToString(token)

	// If a cert already exists on disk, mark as provisioned (survives restarts).
	if certData, err := os.ReadFile(pa.cfg.CertPath); err == nil && len(certData) > 0 {
		pa.completed = true
		pa.certPEM = certData
		pa.logger.Info("existing certificate found, marking as provisioned", "path", pa.cfg.CertPath)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", pa.handleQRPage)
	mux.HandleFunc("/provision/qr", pa.handleQR)
	mux.HandleFunc("/provision/enroll", pa.handleEnroll)
	mux.HandleFunc("/provision/status", pa.handleStatus)
	mux.HandleFunc("/provision/reset", pa.handleReset)

	addr := fmt.Sprintf(":%d", pa.cfg.BootstrapPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("provision listen %s: %w", addr, err)
	}
	pa.listener = ln

	pa.srv = &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		pa.logger.Info("bootstrap listener started",
			"port", pa.cfg.BootstrapPort,
			"challenge", pa.challenge[:8]+"...",
		)
		if err := pa.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			pa.logger.Error("provision server error", "err", err)
		}
	}()

	go func() {
		<-ctx.Done()
		pa.stop()
	}()

	return pa, nil
}

func (pa *provisionAgent) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pa.srv.Shutdown(ctx)
}

// handleQRPage serves an HTML page that displays the provisioning QR code.
// This is meant to be shown on a screen connected to the edge device.
func (pa *provisionAgent) handleQRPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	pa.mu.Lock()
	completed := pa.completed
	pa.mu.Unlock()

	if completed {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(pa.buildStatusPage()))
		return
	}

	host := r.Host
	if host == "" {
		host = pa.listener.Addr().String()
	}
	endpoint := fmt.Sprintf("http://%s/provision/enroll", host)

	qr := ProvisionQR{
		NodeID:    pa.nodeID,
		Challenge: pa.challenge,
		Endpoint:  endpoint,
	}
	qrJSON, _ := json.Marshal(qr)

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, provisionPageHTML, pa.nodeID, string(qrJSON))
}

// buildStatusPage generates an HTML page showing provisioned device details.
func (pa *provisionAgent) buildStatusPage() string {
	certPath := pa.cfg.CertPath
	expiry := "unknown"
	subject := ""
	issuer := ""
	serial := ""
	remaining := ""

	if raw, err := os.ReadFile(certPath); err == nil {
		block, _ := pem.Decode(raw)
		if block != nil {
			if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
				expiry = cert.NotAfter.Format(time.RFC3339)
				subject = cert.Subject.CommonName
				issuer = cert.Issuer.CommonName
				serial = fmt.Sprintf("%X", cert.SerialNumber)
				rem := time.Until(cert.NotAfter)
				if rem > 0 {
					remaining = rem.Truncate(time.Second).String()
				} else {
					remaining = "EXPIRED"
				}
			}
		}
	}

	renewalURL := pa.renewalCfg.ESTURL
	renewalProtocol := pa.renewalCfg.Protocol
	renewalInterval := pa.renewalCfg.CheckInterval
	renewalEnabled := pa.renewalCfg.Enabled

	// Discover bridge configs from device store
	bridges := ""
	if pa.deviceStore != nil {
		if devs, err := pa.deviceStore.GetByNode(context.Background(), pa.nodeID); err == nil {
			for _, d := range devs {
				bridges += fmt.Sprintf(`<tr><td>%s</td><td>%s</td><td>%v</td></tr>`, d.Name, d.Type, d.Enabled)
			}
		}
	}

	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>%s — Provisioned</title>
<meta http-equiv="refresh" content="30">
<style>
*{box-sizing:border-box}
body{margin:0;padding:24px;font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0f1419;color:#e6edf3;min-height:100vh}
h1{color:#3fb950;font-size:1.4rem;margin:0 0 4px}
h2{color:#58a6ff;font-size:1rem;margin:24px 0 8px;border-bottom:1px solid #21262d;padding-bottom:4px}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:600;margin-left:8px}
.badge-ok{background:#238636;color:#fff}
.badge-warn{background:#9e6a03;color:#fff}
.badge-off{background:#6e7681;color:#fff}
table{width:100%%;border-collapse:collapse;margin-top:8px}
td,th{padding:6px 10px;text-align:left;border-bottom:1px solid #21262d;font-size:0.85rem}
th{color:#8b949e;font-weight:500}
td{font-family:monospace;word-break:break-all}
.sub{color:#8b949e;font-size:0.8rem}
</style>
</head>
<body>
<h1>&#10003; Provisioned<span class="badge badge-ok">ACTIVE</span></h1>
<p class="sub">Auto-refreshes every 30s</p>

<h2>Node</h2>
<table>
<tr><th>Node ID</th><td>%s</td></tr>
<tr><th>Subject (CN)</th><td>%s</td></tr>
</table>

<h2>Certificate</h2>
<table>
<tr><th>Expires</th><td>%s</td></tr>
<tr><th>Remaining</th><td>%s</td></tr>
<tr><th>Issuer</th><td>%s</td></tr>
<tr><th>Serial</th><td>%s</td></tr>
<tr><th>Path</th><td>%s</td></tr>
</table>

<h2>Renewal</h2>
<table>
<tr><th>Enabled</th><td>%v</td></tr>
<tr><th>Protocol</th><td>%s</td></tr>
<tr><th>CA URL</th><td>%s</td></tr>
<tr><th>Check Interval</th><td>%s</td></tr>
</table>

<h2>Bridges</h2>
<table>
<tr><th>Name</th><th>Type</th><th>Enabled</th></tr>
%s
</table>

</body>
</html>`,
		pa.nodeID,
		pa.nodeID,
		subject,
		expiry,
		remaining,
		issuer,
		serial,
		certPath,
		renewalEnabled,
		renewalProtocol,
		renewalURL,
		renewalInterval,
		bridges,
	)
}

const provisionPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Provision: %s</title>
<style>
body{display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;margin:0;font-family:-apple-system,sans-serif;background:#0f1419;color:#e6edf3}
h1{font-size:1.3rem;color:#58a6ff;margin-bottom:8px}
p{color:#8b949e;font-size:0.9rem;margin:4px 0}
#qr{background:#fff;padding:16px;border-radius:12px;margin:20px 0}
.node{font-family:monospace;color:#58a6ff}
.copy-btn{margin-top:12px;padding:10px 20px;border:none;border-radius:8px;background:#58a6ff;color:#000;font-size:0.9rem;font-weight:600;cursor:pointer}
.copy-btn:active{opacity:0.7}
.copy-btn.copied{background:#3fb950}
</style>
</head>
<body>
<h1>Edge Provisioning</h1>
<p>Scan this QR code with the provisioning app</p>
<div id="qr"></div>
<p>Node: <span class="node">%[1]s</span></p>
<button class="copy-btn" onclick="copyPayload(this)">Copy QR Payload</button>
<script src="https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.min.js"></script>
<script>
var payload=%s;
var payloadStr=typeof payload==='string'?payload:JSON.stringify(payload);
var qr=qrcode(0,'M');
qr.addData(payloadStr);
qr.make();
document.getElementById('qr').innerHTML=qr.createSvgTag({cellSize:6,margin:4});
function copyPayload(btn){navigator.clipboard.writeText(payloadStr).then(function(){btn.textContent='Copied!';btn.classList.add('copied');setTimeout(function(){btn.textContent='Copy QR Payload';btn.classList.remove('copied')},2000)});}
(function poll(){fetch('/provision/status').then(r=>r.json()).then(d=>{if(d.provisioned){location.reload()}else{setTimeout(poll,2000)}}).catch(()=>setTimeout(poll,2000))})();
</script>
</body>
</html>`

// handleQR returns a JSON payload suitable for QR encoding.
// The caller (a terminal, LCD, or web page) renders it as a QR code.
func (pa *provisionAgent) handleQR(w http.ResponseWriter, r *http.Request) {
	pa.mu.Lock()
	completed := pa.completed
	pa.mu.Unlock()

	if completed {
		http.Error(w, "already provisioned", http.StatusGone)
		return
	}

	// Determine the endpoint URL from the request (for the QR payload).
	host := r.Host
	if host == "" {
		host = pa.listener.Addr().String()
	}
	endpoint := fmt.Sprintf("http://%s/provision/enroll", host)

	qr := ProvisionQR{
		NodeID:    pa.nodeID,
		Challenge: pa.challenge,
		Endpoint:  endpoint,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(qr)
}

// handleEnroll accepts the provisioning CA's response with the signed cert.
func (pa *provisionAgent) handleEnroll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pa.mu.Lock()
	if pa.completed {
		pa.mu.Unlock()
		http.Error(w, "already provisioned", http.StatusGone)
		return
	}
	pa.mu.Unlock()

	var req ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	// Constant-time challenge comparison
	if subtle.ConstantTimeCompare([]byte(req.Challenge), []byte(pa.challenge)) != 1 {
		pa.logger.Warn("invalid challenge presented")
		http.Error(w, "invalid challenge", http.StatusForbidden)
		return
	}

	if req.CertPEM == "" || req.KeyPEM == "" {
		http.Error(w, "certPem and keyPem required", http.StatusBadRequest)
		return
	}

	// Write cert and key to disk so the renewal agent can find them.
	// Use direct write (not atomicWrite) because the /certs directory
	// is a bind-mount owned by root; the nonroot user can overwrite
	// existing files but cannot create new .tmp files in the directory.
	if pa.cfg.CertPath != "" && pa.cfg.KeyPath != "" {
		if err := os.WriteFile(pa.cfg.CertPath, []byte(req.CertPEM), 0644); err != nil {
			pa.logger.Error("failed to write cert", "err", err)
			http.Error(w, "failed to write cert", http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile(pa.cfg.KeyPath, []byte(req.KeyPEM), 0600); err != nil {
			pa.logger.Error("failed to write key", "err", err)
			http.Error(w, "failed to write key", http.StatusInternalServerError)
			return
		}
		if req.CaPEM != "" {
			caPath := filepath.Join(filepath.Dir(pa.cfg.CertPath), "ca.crt")
			_ = os.WriteFile(caPath, []byte(req.CaPEM), 0644)
		}
	}

	// Write bridge client certs and seed bridge configs into the device store.
	if pa.deviceStore != nil && len(req.Bridges) > 0 {
		certDir := filepath.Dir(pa.cfg.CertPath)
		for _, b := range req.Bridges {
			if b.ClientCertPEM != "" {
				p := filepath.Join(certDir, b.Name+".crt")
				_ = os.WriteFile(p, []byte(b.ClientCertPEM), 0644)
			}
			if b.ClientKeyPEM != "" {
				p := filepath.Join(certDir, b.Name+".key")
				_ = os.WriteFile(p, []byte(b.ClientKeyPEM), 0600)
			}
			dc := stores.DeviceConfig{
				Name:      b.Name,
				Namespace: "default",
				NodeID:    pa.nodeID,
				Type:      "MQTT_CLIENT",
				Enabled:   true,
				Config:    b.Config,
			}
			if err := pa.deviceStore.Save(context.Background(), dc); err != nil {
				pa.logger.Error("failed to save bridge config", "name", b.Name, "err", err)
			} else {
				pa.logger.Info("bridge config seeded", "name", b.Name)
			}
		}
	}

	pa.mu.Lock()
	alreadyDone := pa.completed
	pa.completed = true
	pa.certPEM = []byte(req.CertPEM)
	pa.mu.Unlock()

	pa.logger.Info("provisioning complete, certificate received")

	if !alreadyDone && pa.onProvisioned != nil {
		go pa.onProvisioned()
	}

	// Publish status
	if pa.publish != nil {
		status, _ := json.Marshal(map[string]string{
			"state":  "provisioned",
			"nodeId": pa.nodeID,
		})
		_ = pa.publish("$SYS/provision/status/"+pa.nodeID, status, true, 1)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleStatus reports the provisioning state of this edge.
func (pa *provisionAgent) handleStatus(w http.ResponseWriter, r *http.Request) {
	pa.mu.Lock()
	completed := pa.completed
	pa.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"nodeId":      pa.nodeID,
		"provisioned": completed,
	})
}

// handleReset clears the provisioned state and generates a new challenge
// so the device can be re-provisioned.
func (pa *provisionAgent) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	challengeBytes := pa.cfg.ChallengeBytes
	if challengeBytes <= 0 {
		challengeBytes = 32
	}
	token := make([]byte, challengeBytes)
	if _, err := rand.Read(token); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Remove cert/key from disk so the device is truly unprovisioned.
	if pa.cfg.CertPath != "" {
		os.Remove(pa.cfg.CertPath)
	}
	if pa.cfg.KeyPath != "" {
		os.Remove(pa.cfg.KeyPath)
	}

	pa.mu.Lock()
	pa.completed = false
	pa.certPEM = nil
	pa.challenge = hex.EncodeToString(token)
	pa.mu.Unlock()

	pa.logger.Info("provision state reset, cert removed from disk")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
}

// CertPEM returns the provisioned certificate PEM, or nil if not yet provisioned.
func (pa *provisionAgent) CertPEM() []byte {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	return pa.certPEM
}
