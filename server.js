const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;
const RELAY_SECRET = process.env.RELAY_SECRET || 'changeme';
const VALIDATE_URL = process.env.VALIDATE_URL || 'https://ln9uzv43q5.execute-api.us-east-1.amazonaws.com/prod/validate';

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

// ── License validation against Lambda ────────────────────────

function validateCode(code) {
  return new Promise((resolve) => {
    const body = JSON.stringify({ code });
    const urlObj = new URL(VALIDATE_URL);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', (c) => (raw += c));
      res.on('end', () => {
        resolve(res.statusCode === 200);
      });
    });
    req.on('error', () => resolve(true)); // network blip — keep viewer connected
    req.write(body);
    req.end();
  });
}

const wss = new WebSocketServer({ server });

const viewers = new Set();       // Set of { ws, code, heartbeatTimer }
let publisher = null;
let admin = null;

// Last known admin-edited snapshot — new viewers get it immediately
let lastSnapshot = null;

function broadcast(msg) {
  const raw = JSON.stringify(msg);
  for (const viewer of viewers) {
    if (viewer.ws.readyState === viewer.ws.OPEN) viewer.ws.send(raw);
  }
}

function removeViewer(viewer) {
  clearInterval(viewer.heartbeatTimer);
  viewers.delete(viewer);
  console.log(`[relay] Viewer disconnected (${viewers.size} remaining)`);
}

wss.on('connection', async (ws, req) => {
  const url = new URL(req.url, `http://localhost`);
  const role = url.searchParams.get('role');
  const secret = url.searchParams.get('secret');
  const code = (url.searchParams.get('token') || '').trim().toUpperCase();

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

    if (lastSnapshot) {
      ws.send(JSON.stringify(lastSnapshot));
    }

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      if (msg.type === 'admin-edit') {
        lastSnapshot = msg;
        broadcast(msg);
      }

      if (msg.type === 'popup') {
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
  if (!code || code.length < 4) { ws.close(4001, 'No license code'); return; }

  // Validate on connect
  const valid = await validateCode(code);
  if (!valid) {
    ws.close(4003, 'Invalid or revoked license');
    console.log(`[relay] Viewer rejected — invalid code: ${code}`);
    return;
  }

  const viewer = { ws, code, heartbeatTimer: null };
  viewers.add(viewer);
  console.log(`[relay] Viewer connected code=${code} (${viewers.size} total)`);

  // Send current content immediately
  if (lastSnapshot) {
    ws.send(JSON.stringify(lastSnapshot));
  }

  // 30s heartbeat — kick viewer if license is revoked
  viewer.heartbeatTimer = setInterval(async () => {
    const still = await validateCode(viewer.code);
    if (!still) {
      console.log(`[relay] Revoking viewer code=${viewer.code}`);
      viewer.ws.close(4003, 'License revoked');
      removeViewer(viewer);
    }
  }, 30000);

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
    removeViewer(viewer);
  });
});

server.listen(PORT, () => {
  console.log(`[relay] Server running on port ${PORT}`);
});
