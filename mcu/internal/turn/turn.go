// Package turn supplies WebRTC ICE servers (STUN + TURN) to the MCU's
// PeerConnections, minting short-lived Cloudflare TURN credentials via the
// Cloudflare Realtime API and refreshing them before they expire.
//
// It mirrors the Node signaling server (server.js, GET /api/turn-credentials):
// POST https://rtc.live.cloudflare.com/v1/turn/keys/{KEY_ID}/credentials/generate-ice-servers
// with `Authorization: Bearer {API_TOKEN}` and `{"ttl": <seconds>}`, so the MCU
// reuses the SAME Cloudflare TURN key (CF_TURN_KEY_ID / CF_API_TOKEN) as the
// browser stack — one set of credentials for the whole system.
//
// Without TURN the MCU still works on the same machine/LAN (host candidates); TURN
// is what lets a browser and the MCU connect across NATs/networks.
package turn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

const credentialsEndpoint = "https://rtc.live.cloudflare.com/v1/turn/keys/%s/credentials/generate-ice-servers"

// Provider mints and caches Cloudflare ICE servers, refreshing before expiry. It
// is safe for concurrent use: ICEServers() is a non-blocking cached read while a
// background goroutine (Run) keeps the credentials fresh.
type Provider struct {
	keyID    string
	token    string
	ttl      time.Duration
	client   *http.Client
	endpoint string // printf template with one %s for the key id (override in tests)

	mu      sync.RWMutex
	servers []webrtc.ICEServer
}

// NewCloudflare builds a provider for one Cloudflare TURN key. ttl is the
// requested credential lifetime (Cloudflare caps it; server.js uses 3h).
func NewCloudflare(keyID, token string, ttl time.Duration) *Provider {
	if ttl <= 0 {
		ttl = 3 * time.Hour
	}
	return &Provider{
		keyID:    keyID,
		token:    token,
		ttl:      ttl,
		client:   &http.Client{Timeout: 15 * time.Second},
		endpoint: credentialsEndpoint,
	}
}

// ICEServers returns the cached ICE servers (empty until the first successful
// Refresh). Non-blocking — safe to call on the per-connection hot path.
func (p *Provider) ICEServers() []webrtc.ICEServer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.servers
}

// Refresh fetches a fresh set of ICE servers from Cloudflare and caches them. On
// error the previously cached servers are kept (so a transient failure does not
// drop TURN for in-flight reconnects).
func (p *Provider) Refresh(ctx context.Context) error {
	url := fmt.Sprintf(p.endpoint, p.keyID)
	body := fmt.Sprintf(`{"ttl":%d}`, int(p.ttl.Seconds()))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("cloudflare turn: status %s: %s", resp.Status, strings.TrimSpace(string(snippet)))
	}

	// Cloudflare's "iceServers" has shipped as both a single object
	// {urls,username,credential} AND an array of such objects. Decode the raw
	// value and accept either: try array first, fall back to a single object.
	var data struct {
		ICEServers json.RawMessage `json:"iceServers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return fmt.Errorf("cloudflare turn: decode: %w", err)
	}
	type entry struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username"`
		Credential string   `json:"credential"`
	}
	var entries []entry
	if err := json.Unmarshal(data.ICEServers, &entries); err != nil {
		var one entry
		if err2 := json.Unmarshal(data.ICEServers, &one); err2 != nil {
			return fmt.Errorf("cloudflare turn: decode iceServers (array: %v; object: %v)", err, err2)
		}
		entries = []entry{one}
	}

	var servers []webrtc.ICEServer
	for _, e := range entries {
		if len(e.URLs) == 0 {
			continue
		}
		servers = append(servers, webrtc.ICEServer{
			URLs:           e.URLs,
			Username:       e.Username,
			Credential:     e.Credential,
			CredentialType: webrtc.ICECredentialTypePassword,
		})
	}
	if len(servers) == 0 {
		return fmt.Errorf("cloudflare turn: response had no urls")
	}

	p.mu.Lock()
	p.servers = servers
	p.mu.Unlock()
	return nil
}

// Run periodically refreshes the credentials until ctx is done, a little before
// each ttl elapses. It does NOT do the initial fetch — call Refresh once at
// startup first so the first connections already have credentials.
func (p *Provider) Run(ctx context.Context) {
	interval := p.ttl - p.ttl/10 // refresh at ~90% of the TTL
	if interval < time.Minute {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c, cancel := context.WithTimeout(ctx, 15*time.Second)
			err := p.Refresh(c)
			cancel()
			if err != nil {
				log.Printf("[turn] refresh failed (keeping previous credentials): %v", err)
			} else {
				log.Printf("[turn] refreshed Cloudflare ICE servers (%d urls)", p.urlCount())
			}
		}
	}
}

// CredentialsHandler serves the current ICE servers (STUN + TURN) as JSON for
// browser clients, mirroring server.js's /api/turn-credentials. Browsers use
// these so they can supply a relay candidate when behind a restrictive NAT — the
// MCU itself does not relay. The response is CORS-open so a client page served
// from another origin can fetch it.
func (p *Provider) CredentialsHandler() http.HandlerFunc {
	type iceServerJSON struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username,omitempty"`
		Credential string   `json:"credential,omitempty"`
	}
	return func(w http.ResponseWriter, _ *http.Request) {
		servers := p.ICEServers()
		out := make([]iceServerJSON, 0, len(servers))
		for _, s := range servers {
			cred, _ := s.Credential.(string)
			out = append(out, iceServerJSON{URLs: s.URLs, Username: s.Username, Credential: cred})
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(map[string]any{"iceServers": out})
	}
}

func (p *Provider) urlCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.servers) == 0 {
		return 0
	}
	return len(p.servers[0].URLs)
}
