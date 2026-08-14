const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;
const RELAY_SECRET = process.env.RELAY_SECRET || 'changeme';

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/view') {
    const file = path.join(__dirname, 'mobile.html');
    fs.readFile(file, (err, data) => {
      if (err) { res.writeHead(404); res.end('Not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(data);
    });
  } else if (url === '/admin') {
    const file = path.join(__dirname, 'admin.html');
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

const wss = new WebSocketServer({ server });

const viewers = new Set();
let publisher = null;  // Chrome extension
let admin = null;      // Admin editor

// Last known admin-edited snapshot — new viewers get it immediately
let lastSnapshot = null;

function broadcast(msg) {
  const raw = JSON.stringify(msg);
  for (const ws of viewers) {
    if (ws.readyState === ws.OPEN) ws.send(raw);
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost`);
  const role = url.searchParams.get('role');
  const secret = url.searchParams.get('secret');
  const token = url.searchParams.get('token');

  // ── Publisher (Chrome extension) ──────────────────────────────
  if (role === 'publisher') {
    if (secret !== RELAY_SECRET) { ws.close(4001, 'Unauthorized'); return; }
    if (publisher && publisher.readyState === publisher.OPEN) {
      publisher.close(4000, 'Replaced');
    }
    publisher = ws;
    console.log('[relay] Publisher connected');

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      // Forward stream events to admin editor (not directly to viewers)
      if (admin && admin.readyState === admin.OPEN) {
        admin.send(JSON.stringify(msg));
      }

      // Also broadcast stream-start/end so viewer knows state
      if (msg.type === 'stream-start' || msg.type === 'stream-end') {
        broadcast(msg);
      }
    });

    ws.on('close', () => {
      if (publisher === ws) publisher = null;
      console.log('[relay] Publisher disconnected');
    });
    return;
  }

  // ── Admin editor ──────────────────────────────────────────────
  if (role === 'admin') {
    if (secret !== RELAY_SECRET) { ws.close(4001, 'Unauthorized'); return; }
    if (admin && admin.readyState === admin.OPEN) {
      admin.close(4000, 'Replaced');
    }
    admin = ws;
    console.log('[relay] Admin connected');

    // Send last snapshot so admin picks up where things left off
    if (lastSnapshot) {
      ws.send(JSON.stringify(lastSnapshot));
    }

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      // Admin edits → broadcast to all viewers
      if (msg.type === 'admin-edit') {
        lastSnapshot = msg;
        broadcast(msg);
      }
    });

    ws.on('close', () => {
      if (admin === ws) admin = null;
      console.log('[relay] Admin disconnected');
    });
    return;
  }

  // ── Viewer ────────────────────────────────────────────────────
  if (!token || token.length < 4) { ws.close(4001, 'No token'); return; }

  viewers.add(ws);
  console.log(`[relay] Viewer connected (${viewers.size} total)`);

  // Send current content immediately
  if (lastSnapshot) {
    ws.send(JSON.stringify(lastSnapshot));
  }

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'user-note') {
      broadcast(msg);
      if (admin && admin.readyState === admin.OPEN) admin.send(JSON.stringify(msg));
      if (publisher && publisher.readyState === publisher.OPEN) publisher.send(JSON.stringify(msg));
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