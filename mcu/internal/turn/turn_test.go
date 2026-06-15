package turn

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestRefreshParsesCloudflareResponse points the provider at a stub server that
// mimics Cloudflare's generate-ice-servers response and verifies the request
// (auth header + ttl body) and that the response maps to a Pion ICE server.
func TestRefreshParsesCloudflareResponse(t *testing.T) {
	var gotAuth, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"iceServers":{"urls":[` +
			`"stun:stun.cloudflare.com:3478",` +
			`"turn:turn.cloudflare.com:3478?transport=udp",` +
			`"turns:turn.cloudflare.com:5349?transport=tcp"` +
			`],"username":"user123","credential":"cred456"}}`))
	}))
	defer ts.Close()

	p := NewCloudflare("KEYID", "TOKEN", time.Hour)
	p.endpoint = ts.URL + "/keys/%s/credentials/generate-ice-servers"

	if err := p.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	if gotAuth != "Bearer TOKEN" {
		t.Errorf("auth header = %q, want %q", gotAuth, "Bearer TOKEN")
	}
	if !strings.Contains(gotBody, `"ttl":3600`) {
		t.Errorf("request body = %q, want it to contain \"ttl\":3600", gotBody)
	}

	servers := p.ICEServers()
	if len(servers) != 1 {
		t.Fatalf("got %d ICE servers, want 1", len(servers))
	}
	s := servers[0]
	if len(s.URLs) != 3 || s.URLs[0] != "stun:stun.cloudflare.com:3478" {
		t.Fatalf("URLs = %v, want 3 starting with the STUN url", s.URLs)
	}
	if s.Username != "user123" || s.Credential != "cred456" {
		t.Errorf("creds = %q/%v, want user123/cred456", s.Username, s.Credential)
	}
	if s.CredentialType != webrtc.ICECredentialTypePassword {
		t.Errorf("credential type = %v, want password", s.CredentialType)
	}
}

// TestRefreshErrorKeepsPreviousServers verifies a failed refresh surfaces an
// error and does not wipe the previously cached credentials.
func TestRefreshParsesCloudflareArrayResponse(t *testing.T) {
	// Cloudflare also ships "iceServers" as an ARRAY of entries; accept it.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"iceServers":[` +
			`{"urls":["stun:stun.cloudflare.com:3478"]},` +
			`{"urls":["turn:turn.cloudflare.com:3478?transport=udp"],"username":"u","credential":"c"}` +
			`]}`))
	}))
	defer ts.Close()

	p := NewCloudflare("KEYID", "TOKEN", time.Hour)
	p.endpoint = ts.URL + "/keys/%s/credentials/generate-ice-servers"
	if err := p.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	servers := p.ICEServers()
	if len(servers) != 2 {
		t.Fatalf("got %d ICE servers, want 2", len(servers))
	}
	if servers[0].URLs[0] != "stun:stun.cloudflare.com:3478" {
		t.Errorf("server0 URL = %v", servers[0].URLs)
	}
	if servers[1].Username != "u" || servers[1].Credential != "c" {
		t.Errorf("server1 creds = %q/%v, want u/c", servers[1].Username, servers[1].Credential)
	}
}

func TestRefreshErrorKeepsPreviousServers(t *testing.T) {
	status := http.StatusOK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
			return
		}
		_, _ = w.Write([]byte(`{"iceServers":{"urls":["turn:turn.cloudflare.com:3478"],"username":"u","credential":"c"}}`))
	}))
	defer ts.Close()

	p := NewCloudflare("k", "t", time.Hour)
	p.endpoint = ts.URL + "/%s"

	if err := p.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	if len(p.ICEServers()) != 1 {
		t.Fatal("expected cached server after first refresh")
	}

	status = http.StatusNotFound
	if err := p.Refresh(context.Background()); err == nil {
		t.Fatal("expected error on 404 refresh")
	}
	if len(p.ICEServers()) != 1 {
		t.Fatal("a failed refresh must keep the previously cached servers")
	}
}

// TestEmptyURLsIsError verifies a 200 with no urls is treated as a failure.
func TestEmptyURLsIsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"iceServers":{"urls":[],"username":"u","credential":"c"}}`))
	}))
	defer ts.Close()

	p := NewCloudflare("k", "t", time.Hour)
	p.endpoint = ts.URL + "/%s"
	if err := p.Refresh(context.Background()); err == nil {
		t.Fatal("expected error when response has no urls")
	}
}
