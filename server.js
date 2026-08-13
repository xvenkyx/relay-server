const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;
const RELAY_SECRET = process.env.RELAY_SECRET || 'changeme';

// ── HTTP server (serves mobile.html for phone viewers) ───────────────────────

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/view') {
    const file = path.join(__dirname, 'mobile.html');
    fs.readFile(file, (err, data) => {
      if (err) { res.writeHead(404); res.end('Not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  } else {
    res.writeHead(404);
    res.end();
  }
});

// ── WebSocket server ──────────────────────────────────────────────────────────

const wss = new WebSocketServer({ server });

const viewers = new Set();
let publisher = null;

// Last known state — new viewers get it immediately on connect
let lastSnapshot = null;

function broadcast(msg) {
  const raw = JSON.stringify(msg);
  for (const ws of viewers) {
    if (ws.readyState === ws.OPEN) ws.send(raw);
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost`);
  const role = url.searchParams.get('role');       // 'publisher' or 'viewer'
  const secret = url.searchParams.get('secret');   // publisher auth
  const token = url.searchParams.get('token');     // viewer license token

  // ── Publisher connection ──────────────────────────────────────────────────
  if (role === 'publisher') {
    if (secret !== RELAY_SECRET) {
      ws.close(4001, 'Unauthorized');
      return;
    }
    // Only one publisher at a time
    if (publisher && publisher.readyState === publisher.OPEN) {
      publisher.close(4000, 'Replaced by new publisher');
    }
    publisher = ws;
    console.log('[relay] Publisher connected');

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      // Track snapshot for late-joining viewers
      if (msg.type === 'stream-start') {
        lastSnapshot = null;
      } else if (msg.type === 'stream-chunk' || msg.type === 'manual-content') {
        lastSnapshot = msg;
      }

      broadcast(msg);
    });

    ws.on('close', () => {
      if (publisher === ws) publisher = null;
      console.log('[relay] Publisher disconnected');
    });

    return;
  }

  // ── Viewer connection ─────────────────────────────────────────────────────
  if (!token || token.length < 4) {
    ws.close(4001, 'No token');
    return;
  }

  viewers.add(ws);
  console.log(`[relay] Viewer connected (${viewers.size} total)`);

  // Send current content immediately so late joiners see something
  if (lastSnapshot) {
    ws.send(JSON.stringify(lastSnapshot));
  }

  ws.on('message', (raw) => {
    // Viewers can send user-note messages (typed notes from Electron editor)
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'user-note') {
      broadcast(msg);
      // Also echo back to publisher if connected
      if (publisher && publisher.readyState === publisher.OPEN) {
        publisher.send(JSON.stringify(msg));
      }
    }
  });

  ws.on('close', () => {
    viewers.delete(ws);
    console.log(`[relay] Viewer disconnected (${viewers.size} remaining)`);
  });
});

server.listen(PORT, () => {
  console.log(`[relay] Server running on port ${PORT}`);
});
