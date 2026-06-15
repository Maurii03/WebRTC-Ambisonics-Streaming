/*
 * WebRTC Signaling & TURN Credential Server
 * Install: npm install ws express cors
 * Run: node server.js [port] (default: 8080)
 *
 * Required Environment Variables:
 * CF_TURN_KEY_ID -- Cloudflare Calls TURN Key ID
 * CF_API_TOKEN   -- Cloudflare API Token
 *
 * REST API:
 * GET /api/turn-credentials -- Generates and returns Cloudflare ICE/TURN servers credentials
 *
 * WebSocket Messages:
 * join: role, room -- create/join a unidirectional room with role sender/receiver
 * join_node: room -- create/join a bidirectional room; role is auto-assigned
 * role_assigned: role, room -- sent to bidirectional nodes after join_node
 * offer, answer, ice -- relayed to the opposite peer inside the same room family
 * error, message, suggestedRoom? -- sent by server; suggestedRoom present when room is occupied
 * peer_ready -- sent to the peer that must create the offer when both are connected
 */

const { WebSocketServer } = require("ws");
const http = require("http");
const express = require("express");
const cors = require("cors");

const PORT = parseInt(process.argv[2] ?? "8080", 10);
const app = express();

app.use(cors());

const CF_TURN_KEY_ID = process.env.CF_TURN_KEY_ID;
const CF_API_TOKEN = process.env.CF_API_TOKEN;

const TURN_TTL_SECONDS = 3 * 60 * 60; // 3 hours
const REFRESH_THRESHOLD_MS = 60 * 60 * 1000; // 1 hour
let cachedIceServers = null;
let cacheExpiryTime = 0;

if (!CF_TURN_KEY_ID || !CF_API_TOKEN) {
  console.error("[FATAL] Missing required Cloudflare environment variables.");
  process.exit(1);
}

function getOppositeRole(role) {
  if (role === "sender") return "receiver";
  if (role === "receiver") return "sender";
  if (role === "offerer") return "answerer";
  if (role === "answerer") return "offerer";
  return null;
}

// Unidirectional rooms: <roomId, { sender?: ws, receiver?: ws }>
const monoRooms = new Map();

// Bidirectional rooms: <roomId, { offerer?: ws, answerer?: ws }>
const bidirectionalRooms = new Map();

// Create an HTTP server to serve Express routes and attach the WebSocket server.
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

server.listen(PORT, () =>
  console.log(`[signaling] listening on http://localhost:${PORT}`)
);

wss.on("connection", (ws) => {
  ws.room = null;
  ws.role = null;
  ws.roomType = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return send(ws, { type: "error", message: "Invalid JSON" });
    }

    switch (msg.type) {
      case "join": {
        const { role, room: roomId } = msg;

        if (!["sender", "receiver"].includes(role) || !roomId)
          return send(ws, { type: "error", message: "Invalid join params" });

        if (!monoRooms.has(roomId)) monoRooms.set(roomId, {});
        const room = monoRooms.get(roomId);

        // 1 corresponds to the WebSocket.OPEN state
        if (room[role] && room[role].readyState === 1) {
          const suggested = findFreeMonoRoom(role, roomId);
          send(ws, {
            type: "error",
            message: `Room '${roomId}' already has a ${role}`,
            suggestedRoom: suggested,
          });
          console.log(`[${roomId}] rejected ${role} → suggested '${suggested}'`);
          return;
        }

        room[role] = ws;
        ws.room = roomId;
        ws.role = role;
        ws.roomType = "mono";

        send(ws, { type: "joined", role, room: roomId, mode: "mono" });
        console.log(`[mono:${roomId}] ${role} joined`);

        if (role === "receiver" && room.sender?.readyState === 1) {
          send(room.sender, { type: "peer_ready" });
          console.log(`[mono:${roomId}] notified sender -> peer_ready`);
        }
        if (role === "sender" && room.receiver?.readyState === 1) {
          send(ws, { type: "peer_ready" });
          console.log(`[mono:${roomId}] notified sender (receiver already present)`);
        }
        break;
      }

      case "join_node": {
        const { room: roomId } = msg;

        if (!roomId) {
          return send(ws, { type: "error", message: "Invalid join_node params" });
        }

        if (!bidirectionalRooms.has(roomId)) bidirectionalRooms.set(roomId, {});
        const room = bidirectionalRooms.get(roomId);

        let role = null;
        if (!room.offerer || room.offerer.readyState !== 1) role = "offerer";
        else if (!room.answerer || room.answerer.readyState !== 1) role = "answerer";

        if (!role) {
          const suggested = findFreeBidirectionalRoom(roomId);
          send(ws, {
            type: "error",
            message: `Bidirectional room '${roomId}' already has two peers`,
            suggestedRoom: suggested,
          });
          console.log(`[bidirectional:${roomId}] rejected node -> suggested '${suggested}'`);
          return;
        }

        room[role] = ws;
        ws.room = roomId;
        ws.role = role;
        ws.roomType = "bidirectional";

        send(ws, { type: "role_assigned", role, room: roomId, mode: "bidirectional" });
        console.log(`[bidirectional:${roomId}] ${role} joined`);

        if (role === "answerer" && room.offerer?.readyState === 1) {
          send(room.offerer, { type: "peer_ready" });
          console.log(`[bidirectional:${roomId}] notified offerer -> peer_ready`);
        }
        if (role === "offerer" && room.answerer?.readyState === 1) {
          send(ws, { type: "peer_ready" });
          console.log(`[bidirectional:${roomId}] notified offerer (answerer already present)`);
        }
        break;
      }

      case "offer":
        relay(ws, getOppositeRole(ws.role), msg);
        break;

      case "answer":
        relay(ws, getOppositeRole(ws.role), msg);
        break;

      case "ice": {
        const target = getOppositeRole(ws.role);
        relay(ws, target, msg);
        break;
      }

      default:
        send(ws, { type: "error", message: `Unknown type: ${msg.type}` });
    }
  });

  ws.on("close", () => {
    if (!ws.room) return;
    const rooms = getRoomsForSocket(ws);
    const room = rooms?.get(ws.room);
    if (!room) return;

    delete room[ws.role];
    console.log(`[${ws.roomType}:${ws.room}] ${ws.role} disconnected`);

    const other = getOppositeRole(ws.role);
    if (other && room[other]?.readyState === 1) {
      send(room[other], { type: "peer_disconnected" });
    }

    if (isRoomEmpty(room)) {
      rooms.delete(ws.room);
      console.log(`[${ws.roomType}:${ws.room}] room deleted`);
    }
  });
});

function getRoomsForSocket(ws) {
  if (ws.roomType === "mono") return monoRooms;
  if (ws.roomType === "bidirectional") return bidirectionalRooms;
  return null;
}

function isRoomEmpty(room) {
  return !room.sender && !room.receiver && !room.offerer && !room.answerer;
}

/**
 * Sends a JSON object to the given WebSocket client if its connection is open.
 * @param {WebSocket} ws - The target WebSocket client.
 * @param {object} obj - The message object to stringify and send.
 */
function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

/**
 * Returns the first room ID (starting from `baseId`) where `role` is free.
 * Tries baseId‑2, baseId‑3, … up to 100; if baseId already has a numeric
 * suffix (e.g. "room‑3") it increments from that number.
 * @param {'sender'|'receiver'} role - The role that must be vacant.
 * @param {string} baseId - The base room ID (may already contain a suffix).
 * @returns {string} An available room ID.
 */
function findFreeMonoRoom(role, baseId) {
  const match = baseId.match(/^(.+?)-?(\d+)$/);
  const prefix = match ? match[1] : baseId;
  let n = match ? parseInt(match[2], 10) + 1 : 2;

  for (; n <= 100; n++) {
    const candidate = `${prefix}-${n}`;
    const room = monoRooms.get(candidate);
    if (!room || !room[role] || room[role].readyState !== 1) {
      return candidate;
    }
  }
  return `${prefix}-${Date.now()}`; // fallback: unique timestamp suffix
}

/**
 * Returns the first bidirectional room ID with at least one free peer slot.
 * @param {string} baseId - The base room ID (may already contain a suffix).
 * @returns {string} An available room ID.
 */
function findFreeBidirectionalRoom(baseId) {
  const match = baseId.match(/^(.+?)-?(\d+)$/);
  const prefix = match ? match[1] : baseId;
  let n = match ? parseInt(match[2], 10) + 1 : 2;

  for (; n <= 100; n++) {
    const candidate = `${prefix}-${n}`;
    const room = bidirectionalRooms.get(candidate);
    const offererBusy = room?.offerer?.readyState === 1;
    const answererBusy = room?.answerer?.readyState === 1;
    if (!offererBusy || !answererBusy) return candidate;
  }
  return `${prefix}-${Date.now()}`;
}

/**
 * Relays a message from the sender WebSocket to the peer with the specified role
 * in the same room. If the target peer is not connected, an error is sent back.
 * @param {WebSocket} fromWs - The WebSocket that originated the message.
 * @param {'sender'|'receiver'} targetRole - The role of the intended recipient.
 * @param {object} msg - The message object to forward.
 */
function relay(fromWs, targetRole, msg) {
  const rooms = getRoomsForSocket(fromWs);
  const room = rooms?.get(fromWs.room);
  if (!room || !targetRole) return;
  const target = room[targetRole];

  if (target?.readyState === 1) {
    send(target, msg);
  } else {
    send(fromWs, { type: "error", message: `${targetRole} not connected` });
  }
}

/**
 * GET /api/turn-credentials
 * Returns ICE server credentials for TURN, using caching to minimise API calls.
 * On Cloudflare API failure, falls back to previously cached credentials if available.
 */
app.get('/api/turn-credentials', async (req, res) => {
  const now = Date.now();

  if (cachedIceServers && (cacheExpiryTime - now) > REFRESH_THRESHOLD_MS) {
    console.log("[TURN] Cache hit: returning fresh credentials.");
    return res.json({ iceServers: cachedIceServers, expiresAt: cacheExpiryTime });
  }

  console.log("[TURN] Fetching new ICE credentials from Cloudflare...");
  try {
    const response = await fetch(
      `https://rtc.live.cloudflare.com/v1/turn/keys/${CF_TURN_KEY_ID}/credentials/generate-ice-servers`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${CF_API_TOKEN}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ ttl: TURN_TTL_SECONDS })
      }
    );

    if (!response.ok) {
      throw new Error(`Cloudflare error: ${response.statusText}`);
    }

    const data = await response.json();

    cachedIceServers = data.iceServers;
    cacheExpiryTime = now + (TURN_TTL_SECONDS * 1000);

    res.json({ iceServers: cachedIceServers, expiresAt: cacheExpiryTime });
    console.log("[TURN] New credentials fetched, cached and returned successfully.");
  } catch (error) {
    console.error("[TURN] Cloudflare API error:", error);

    if (cachedIceServers && cacheExpiryTime > Date.now()) {
      console.warn("[TURN] Fallback: returning still-valid cached credentials.");
      return res.json({ iceServers: cachedIceServers, expiresAt: cacheExpiryTime });
    }

    console.error("[TURN] Fallback failed: no cache available or credentials expired.");
    res.status(500).json({ error: "Failed to retrieve TURN credentials" });
  }
});