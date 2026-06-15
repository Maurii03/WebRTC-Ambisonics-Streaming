// Package signaling exposes the WebSocket endpoint that drives SDP/ICE between a
// connecting browser and its server-side WebRTC Session.
//
// PHASE 0 SCOPE: one Session per WebSocket connection. Because the MCU is itself
// the WebRTC endpoint (the offerer), this does NOT forward SDP between two
// browsers the way the JS relay in server.js does — it terminates the
// connection and answers locally. The browser is the SDP answerer.
//
// Message envelope (JSON), browser ⇄ server:
//
//	server → browser : {"type":"offer","sdp":{...}}   {"type":"ice","candidate":{...}}
//	browser → server : {"type":"answer","sdp":{...}}  {"type":"ice","candidate":{...}}
package signaling

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/Maurii03/ambirtc-mcu/internal/proto"
	"github.com/Maurii03/ambirtc-mcu/internal/room"
	"github.com/Maurii03/ambirtc-mcu/internal/session"
	"github.com/gorilla/websocket"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

// Server is the WebSocket signaling endpoint and Session factory. It owns the
// shared room manager: each connecting client becomes one Session that joins the
// room named by the "room" query parameter, and the per-room mix clock fans the
// summed soundfield back to every member.
type Server struct {
	api        *webrtc.API
	iceServers func() []webrtc.ICEServer // ICE servers put on the MCU's own PeerConnections
	opts       session.Options
	rooms      *room.Manager
	upgrader   websocket.Upgrader
	nextID     atomic.Uint64
}

// ICEConfig configures connectivity for the MCU's PeerConnections.
type ICEConfig struct {
	// PublicIP, if set, is advertised as the MCU's host candidate (NAT1To1). On a
	// public-IP server behind 1:1 NAT this is what makes the MCU DIRECTLY reachable
	// so clients connect without relaying — TURN then only kicks in for clients
	// behind restrictive NATs. Empty = gather real interface IPs.
	PublicIP string
	// StunURL is the STUN server the MCU uses to discover its server-reflexive
	// candidate (helps when PublicIP is not set). The MCU is configured with STUN
	// ONLY, no TURN: it needs no relay of its own — a restrictive-NAT client
	// supplies its own relay candidate, which the publicly-reachable MCU connects
	// to. Empty = no STUN.
	StunURL string
	// UDPMux, if set, makes ALL ICE media share one fixed UDP port (instead of a
	// random ephemeral one) so it can be opened in a cloud firewall. Without an open
	// media port the PublicIP host candidate is advertised but unreachable and every
	// session relays via TURN. nil = ephemeral ports (default).
	UDPMux ice.UDPMux
}

// defaultRoom is used when a client connects without a "room" query parameter.
const defaultRoom = "main"

// New builds the signaling server. opts carries the server-authoritative per-session
// audio tuning applied to every Session; ice configures the MCU's connectivity
// (public-IP host candidate + STUN). The ICE transport policy is left at the
// default ("all"), so direct candidate pairs are always preferred and relay is a
// last resort — never forced.
func New(opts session.Options, iceCfg ICEConfig) *Server {
	var mcuServers []webrtc.ICEServer
	if iceCfg.StunURL != "" {
		mcuServers = []webrtc.ICEServer{{URLs: []string{iceCfg.StunURL}}}
	}
	iceServers := func() []webrtc.ICEServer { return mcuServers }
	se := webrtc.SettingEngine{}
	// Browsers (Firefox/Chrome) obfuscate local host candidates as mDNS ".local"
	// names by default. Pion discards remote mDNS candidates unless told to query
	// them. QueryOnly resolves the browser's ".local" candidates while keeping our
	// own host candidates as real IPs.
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeQueryOnly)
	// Deliberately do NOT advertise a loopback (127.0.0.1) candidate. With a real
	// browser on the same machine, the kernel rewrites the source address on the
	// loopback path, so the browser's DTLS packets no longer match the ICE pair's
	// 5-tuple and Pion drops them — ICE "connects" but DTLS stalls. Real browsers
	// connect via the host/LAN candidate. (The in-process loopback test in
	// session_test.go enables loopback on its own API, unaffected by this.)
	//
	// Advertise the server's public IP as a host candidate (1:1 NAT). Without this,
	// a cloud MCU gathers only its PRIVATE IP, unreachable by remote clients, which
	// forces every session onto the TURN relay — the latency we want to avoid.
	if iceCfg.PublicIP != "" {
		se.SetNAT1To1IPs([]string{iceCfg.PublicIP}, webrtc.ICECandidateTypeHost)
	}
	// One fixed UDP port for all media (NAT1To1 rewrites the candidate IP to
	// PublicIP, so the advertised host candidate becomes PublicIP:<mux port>).
	if iceCfg.UDPMux != nil {
		se.SetICEUDPMux(iceCfg.UDPMux)
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	return &Server{
		api:        api,
		iceServers: iceServers,
		opts:       opts,
		rooms:      room.NewManager(proto.Channels, opts.FrameSize),
		upgrader: websocket.Upgrader{
			// Dev convenience: the client is served from a different origin/port
			// than the signaling server.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

// Snapshot returns a telemetry snapshot of every live room (for the /metrics endpoint).
func (s *Server) Snapshot() []room.RoomMetrics { return s.rooms.Snapshot() }

// clientMessage is the inbound signaling envelope from the browser.
type clientMessage struct {
	Type      string                     `json:"type"`
	SDP       *webrtc.SessionDescription `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit   `json:"candidate,omitempty"`
	T         float64                    `json:"t,omitempty"` // client timestamp echoed by ping/pong
}

// ServeHTTP upgrades the request to a WebSocket and drives one Session through
// SDP/ICE negotiation for the lifetime of the connection.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[signaling] upgrade: %v", err)
		return
	}
	defer conn.Close()

	id := fmt.Sprintf("c%d", s.nextID.Add(1))
	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		roomID = defaultRoom
	}
	log.Printf("[signaling] %s connected from %s (room=%q)", id, conn.RemoteAddr(), roomID)

	// gorilla connections are not safe for concurrent writes, and the session
	// also writes from Pion's ICE goroutine. Serialize every write behind a mutex.
	var writeMu sync.Mutex
	send := func(msg any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(msg)
	}

	// Fresh ICE config per connection so refreshed TURN credentials are used.
	webrtcCfg := webrtc.Configuration{ICEServers: s.iceServers()}
	sess, err := session.New(id, s.api, webrtcCfg, s.opts, s.rooms, roomID, send)
	if err != nil {
		log.Printf("[signaling] %s: %v", id, err)
		return
	}
	defer sess.Close()

	// MCU is the offerer: emit the offer as soon as the socket is up.
	if err := sess.Start(); err != nil {
		log.Printf("[signaling] %s: %v", id, err)
		return
	}

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			log.Printf("[signaling] %s disconnected: %v", id, err)
			return
		}

		var msg clientMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("[signaling] %s bad message: %v", id, err)
			continue
		}

		switch msg.Type {
		case "answer":
			if msg.SDP == nil {
				log.Printf("[signaling] %s answer missing sdp", id)
				continue
			}
			if err := sess.HandleAnswer(*msg.SDP); err != nil {
				log.Printf("[signaling] %s: %v", id, err)
			}
		case "ice":
			if msg.Candidate == nil {
				continue
			}
			if err := sess.AddRemoteICE(*msg.Candidate); err != nil {
				log.Printf("[signaling] %s: %v", id, err)
			}
		case "ping":
			// RTT probe: echo the client's timestamp straight back. Browsers do
			// not reliably expose candidate-pair RTT via getStats (Firefox), so
			// the client measures RTT at the signaling layer instead.
			if err := send(map[string]any{"type": "pong", "t": msg.T}); err != nil {
				log.Printf("[signaling] %s pong: %v", id, err)
			}
		default:
			log.Printf("[signaling] %s unknown message type %q", id, msg.Type)
		}
	}
}
