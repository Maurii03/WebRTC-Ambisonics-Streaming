// Command mcu is the AmbiRTC MCU server.
//
// It serves a WebSocket signaling endpoint and acts as a WebRTC endpoint
// (offerer): for each connecting browser it opens an unordered/no-retransmit
// DataChannel carrying the 16-channel ambisonics packet stream. Each client joins
// a room (the "room" WS query parameter); a per-room mix clock decodes every
// member's soundfield into a per-client jitter buffer, sums them (B-format is
// linear → channel-wise addition), and sends each client the minus-one mix
// (everyone except itself, so no self-echo), re-encoded by a parallel per-session
// encode pool — so per-client cost stays O(1) as the room grows.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/Maurii03/ambirtc-mcu/internal/proto"
	"github.com/Maurii03/ambirtc-mcu/internal/room"
	"github.com/Maurii03/ambirtc-mcu/internal/session"
	"github.com/Maurii03/ambirtc-mcu/internal/signaling"
	"github.com/Maurii03/ambirtc-mcu/internal/turn"
)

// config holds the server-authoritative runtime parameters. frameSize is sent to
// each browser in the offer and drives the per-session jitter buffer, mix-clock
// period, and codecs. encodeWorkers sets the per-session encode parallelism (the
// 16 output channels are split across this many goroutines); total encode
// parallelism is ~N×encodeWorkers, multiplexed onto GOMAXPROCS cores. vadThreshold
// (0 disables) gates silent clients out of the mix for a cleaner noise floor.
type config struct {
	addr           string
	frameSize      int
	encodeWorkers  int
	vadThreshold   int
	vadHangover    int
	turnTTL        time.Duration
	publicIP       string
	stunURL        string
	udpPort        int
	bundled        bool
	bitrateProfile int
}

func loadConfig() (config, error) {
	c := config{
		addr:           envOr("MCU_ADDR", ":8080"),
		frameSize:      envIntOr("MCU_FRAME_SIZE", proto.DefaultFrameSize),
		encodeWorkers:  envIntOr("MCU_ENCODE_WORKERS", 4),
		vadThreshold:   envIntOr("MCU_VAD_THRESHOLD", 0),
		vadHangover:    envIntOr("MCU_VAD_HANGOVER", 15),
		publicIP:       envOr("MCU_PUBLIC_IP", ""),
		stunURL:        envOr("MCU_STUN_URL", "stun:stun.cloudflare.com:3478"),
		udpPort:        envIntOr("MCU_UDP_PORT", 0),
		bundled:        envBoolOr("MCU_PACKING_BUNDLED", false),
		bitrateProfile: envIntOr("MCU_BITRATE_PROFILE", int(proto.DefaultBitrateProfile)),
	}
	flag.StringVar(&c.addr, "addr", c.addr, "listen address for the signaling/WebRTC server")
	flag.IntVar(&c.frameSize, "frame-size", c.frameSize,
		"Opus frame size in samples (server-authoritative); one of 120,240,480,960,1920,2880")
	flag.IntVar(&c.encodeWorkers, "encode-workers", c.encodeWorkers,
		"per-session encode parallelism (16 channels split across N goroutines, clamped to [1,16])")
	flag.IntVar(&c.vadThreshold, "vad-threshold", c.vadThreshold,
		"VAD gate: channel-0 peak (int16) below which a client is treated as silent (0 disables)")
	flag.IntVar(&c.vadHangover, "vad-hangover", c.vadHangover,
		"VAD hangover in frames (stay active after the last loud frame)")
	flag.DurationVar(&c.turnTTL, "turn-ttl", 3*time.Hour, "Cloudflare TURN credential lifetime")
	flag.StringVar(&c.publicIP, "public-ip", c.publicIP,
		"public IP to advertise as the MCU's host candidate (NAT1To1) — set this on a public-IP server so clients connect directly without relaying")
	flag.StringVar(&c.stunURL, "stun", c.stunURL, "STUN URL for the MCU's own candidates (empty to disable)")
	flag.IntVar(&c.udpPort, "udp-port", c.udpPort,
		"fixed UDP port for ALL ICE media (0 = random ephemeral). Set this on a public-IP server and open the port (udp) in the firewall so clients connect DIRECTLY instead of relaying via TURN")
	flag.BoolVar(&c.bundled, "packing-bundled", c.bundled,
		"downlink wire format: bundle multiple channels per packet (v2, R=2 base redundancy) instead of one packet per channel; the uplink is auto-detected either way")
	flag.IntVar(&c.bitrateProfile, "bitrate-profile", c.bitrateProfile,
		"per-channel Opus bitrate (order-tapered, ch0-3/ch4-8/ch9-15): 1=64/64/64 (1024k), 2=64/48/32 (720k), 3=48/32/24 (520k), 4=48/24/24 (480k)")
	flag.Parse()

	if !proto.IsValidFrameSize(c.frameSize) {
		return c, fmt.Errorf("invalid -frame-size %d: must be one of %v", c.frameSize, proto.ValidFrameSizes)
	}
	if !proto.IsValidBitrateProfile(c.bitrateProfile) {
		return c, fmt.Errorf("invalid -bitrate-profile %d: must be 1, 2, 3 or 4", c.bitrateProfile)
	}
	return c, nil
}

func main() {
	// Load .env BEFORE reading config/credentials so MCU_* and CF_* env vars take
	// effect. Default locations: ./.env then ../.env (the repo-root one that holds
	// the Cloudflare TURN credentials); override with MCU_ENV_FILE=/path/.env.
	envFiles := []string{".env", "../.env"}
	if p := os.Getenv("MCU_ENV_FILE"); p != "" {
		envFiles = []string{p}
	}
	loadDotEnv(envFiles...)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("[mcu] config: %v", err)
	}

	opts := session.Options{
		FrameSize:      cfg.frameSize,
		EncodeWorkers:  cfg.encodeWorkers,
		VADThreshold:   cfg.vadThreshold,
		VADHangover:    cfg.vadHangover,
		Bundled:        cfg.bundled,
		BitrateProfile: proto.BitrateProfile(cfg.bitrateProfile),
	}

	// The MCU runs on a public IP, so it must be DIRECTLY reachable: advertise that
	// IP as a host candidate (NAT1To1) and use STUN only — never TURN — for its own
	// connections. ICE policy stays "all", so direct candidate pairs are always
	// preferred and relay is a last resort. TURN is engaged only when a client
	// behind a restrictive NAT supplies its OWN relay candidate (the MCU, being
	// publicly reachable, connects to it) — so media never relays unless a client
	// truly cannot connect directly. Cloudflare credentials, if configured, are
	// served to those clients via /api/turn-credentials (the MCU itself never relays).
	iceCfg := signaling.ICEConfig{
		PublicIP: cfg.publicIP,
		StunURL:  cfg.stunURL,
	}
	// Pin all ICE media to one UDP port so it can be opened in the firewall. Without
	// this Pion uses a random ephemeral port that a cloud firewall blocks, so the
	// public-IP host candidate is advertised but unreachable → every session relays
	// via TURN (added jitter/reordering). One fixed, open port lets clients connect
	// directly. Combined with NAT1To1, the advertised candidate is publicIP:udpPort.
	if cfg.udpPort > 0 {
		udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: cfg.udpPort})
		if err != nil {
			log.Fatalf("[mcu] ICE UDP mux: listen :%d/udp: %v", cfg.udpPort, err)
		}
		iceCfg.UDPMux = webrtc.NewICEUDPMux(nil, udpConn)
		log.Printf("[mcu] ICE UDP mux on :%d/udp — open this port (udp) in the firewall", cfg.udpPort)
	}
	sig := signaling.New(opts, iceCfg)
	turnProvider := startTURN(cfg)

	mux := http.NewServeMux()
	mux.Handle("/ws", sig)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/metrics", metricsHandler(sig, cfg))
	if turnProvider != nil {
		// Browsers fetch ICE/TURN credentials here for the restrictive-NAT fallback.
		mux.HandleFunc("/api/turn-credentials", turnProvider.CredentialsHandler())
	}

	log.Printf("[mcu] AmbiRTC MCU (phase 4: hardening + telemetry) starting")
	brSum := 0
	for _, b := range proto.ChannelBitRates(proto.BitrateProfile(cfg.bitrateProfile)) {
		brSum += b
	}
	log.Printf("[mcu] channels=%d sampleRate=%d frameSize=%d encodeWorkers=%d vadThreshold=%d bitrateProfile=%d (%dkbps/stream) GOMAXPROCS=%d",
		proto.Channels, proto.SampleRate, cfg.frameSize, cfg.encodeWorkers, cfg.vadThreshold, cfg.bitrateProfile, brSum/1000, runtime.GOMAXPROCS(0))
	log.Printf("[mcu] ICE: publicIP=%q stun=%q policy=all (relay is fallback-only — never forced)", cfg.publicIP, cfg.stunURL)
	log.Printf("[mcu] WS: ws://localhost%s/ws  health: http://localhost%s/healthz  metrics: http://localhost%s/metrics",
		cfg.addr, cfg.addr, cfg.addr)

	if err := http.ListenAndServe(cfg.addr, mux); err != nil {
		log.Fatalf("[mcu] server: %v", err)
	}
}

// metricsSnapshot is the top-level JSON returned by GET /metrics — a single
// chartable sample of process + per-room + per-session telemetry. Scrape it
// periodically (e.g. the sweep harness) to build the thesis time series.
type metricsSnapshot struct {
	TS            int64              `json:"tsUnixMs"`
	FrameSize     int                `json:"frameSize"`
	EncodeWorkers int                `json:"encodeWorkers"`
	VADThreshold  int                `json:"vadThreshold"`
	Goroutines    int                `json:"goroutines"`
	GOMAXPROCS    int                `json:"gomaxprocs"`
	NumCPU        int                `json:"numCPU"`
	CPUUserSec    float64            `json:"cpuUserSec"` // cumulative; ΔuserSec/Δt = CPU cores used
	CPUSysSec     float64            `json:"cpuSysSec"`
	Rooms         []room.RoomMetrics `json:"rooms"`
}

func metricsHandler(sig *signaling.Server, cfg config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		user, sys := cpuTimes()
		snap := metricsSnapshot{
			TS:            time.Now().UnixMilli(),
			FrameSize:     cfg.frameSize,
			EncodeWorkers: cfg.encodeWorkers,
			VADThreshold:  cfg.vadThreshold,
			Goroutines:    runtime.NumGoroutine(),
			GOMAXPROCS:    runtime.GOMAXPROCS(0),
			NumCPU:        runtime.NumCPU(),
			CPUUserSec:    user,
			CPUSysSec:     sys,
			Rooms:         sig.Snapshot(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	}
}

// cpuTimes returns cumulative process user and system CPU seconds (Unix
// getrusage). Charting (ΔCPU seconds / Δwall seconds) yields cores-used, which is
// the server-CPU-vs-N curve the thesis needs — no external profiler required.
func cpuTimes() (user, sys float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	user = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sys = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return user, sys
}

// startTURN loads the .env, and if Cloudflare TURN credentials are present
// (CF_TURN_KEY_ID + CF_API_TOKEN) returns a provider that mints + refreshes ICE
// servers. It does the initial fetch synchronously so the first connections have
// credentials, then refreshes in the background. Returns nil (localhost/LAN only)
// when no credentials are configured. The API token is never logged.
func startTURN(cfg config) *turn.Provider {
	keyID := os.Getenv("CF_TURN_KEY_ID")
	token := os.Getenv("CF_API_TOKEN")
	if keyID == "" || token == "" {
		log.Printf("[mcu] no TURN credentials (CF_TURN_KEY_ID/CF_API_TOKEN) — direct/STUN only; a client behind a restrictive NAT may fail to connect")
		return nil
	}
	prov := turn.NewCloudflare(keyID, token, cfg.turnTTL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := prov.Refresh(ctx)
	cancel()
	if err != nil {
		log.Printf("[mcu] Cloudflare TURN: initial fetch failed: %v (retrying in background)", err)
	} else {
		log.Printf("[mcu] Cloudflare TURN ready — credentials served to clients at /api/turn-credentials (ttl %s)", cfg.turnTTL)
	}
	go prov.Run(context.Background())
	return prov
}

// loadDotEnv loads KEY=VALUE lines from the first existing path(s) into the
// process environment WITHOUT overriding variables already set (real exported env
// and earlier files win). Lines may be blank, "# comments", or "export KEY=VALUE";
// surrounding quotes on the value are stripped.
func loadDotEnv(paths ...string) {
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			line = strings.TrimPrefix(line, "export ")
			key, val, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			key = strings.TrimSpace(key)
			val = strings.Trim(strings.TrimSpace(val), `"'`)
			if key == "" {
				continue
			}
			if _, exists := os.LookupEnv(key); !exists {
				_ = os.Setenv(key, val)
			}
		}
		_ = f.Close()
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("[mcu] warning: %s=%q is not an integer; using %d", key, v, def)
	}
	return def
}

func envBoolOr(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		log.Printf("[mcu] warning: %s=%q is not a bool; using %v", key, v, def)
	}
	return def
}
