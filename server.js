#!/usr/bin/env node
"use strict";

const http = require("http");
const https = require("https");
const crypto = require("crypto");
const childProcess = require("child_process");
const os = require("os");
const { EventEmitter } = require("events");
const { URL } = require("url");

let OptionalQRCode = null;
try {
  OptionalQRCode = require("qrcode");
} catch (_error) {
  OptionalQRCode = null;
}

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function parseArgs(argv) {
  const args = {
    host: "0.0.0.0",
    port: 3000,
    publicBaseUrl: process.env.PUBLIC_BASE_URL || "",
    noNgrok: false,
    ngrokBin: "ngrok",
    ngrokTimeoutMs: 20000,
    ngrokApiStartPort: 4040,
    sessionMinutes: 10,
    piKey: process.env.PI_SHARED_KEY || crypto.randomBytes(18).toString("base64url"),
    adminToken: process.env.DASHBOARD_TOKEN || crypto.randomBytes(18).toString("base64url"),
  };

  for (let index = 0; index < argv.length; index += 1) {
    const item = argv[index];
    const next = () => {
      index += 1;
      if (index >= argv.length) throw new Error(`Missing value for ${item}`);
      return argv[index];
    };

    if (item === "--host") args.host = next();
    else if (item === "--port") args.port = Number(next());
    else if (item === "--public-base-url") args.publicBaseUrl = next().replace(/\/$/, "");
    else if (item === "--no-ngrok") args.noNgrok = true;
    else if (item === "--ngrok-bin") args.ngrokBin = next();
    else if (item === "--ngrok-timeout") args.ngrokTimeoutMs = Number(next()) * 1000;
    else if (item === "--ngrok-api-start-port") args.ngrokApiStartPort = Number(next());
    else if (item === "--session-minutes") args.sessionMinutes = Number(next());
    else if (item === "--pi-key") args.piKey = next();
    else if (item === "--dashboard-token") args.adminToken = next();
    else if (item === "--help" || item === "-h") {
      printHelp();
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${item}`);
    }
  }

  if (!Number.isFinite(args.port) || args.port <= 0 || args.port > 65535) {
    throw new Error("--port must be a valid TCP port");
  }
  if (!Number.isFinite(args.ngrokApiStartPort) || args.ngrokApiStartPort <= 0 || args.ngrokApiStartPort > 65535) {
    throw new Error("--ngrok-api-start-port must be a valid TCP port");
  }
  if (!Number.isFinite(args.sessionMinutes) || args.sessionMinutes <= 0) {
    throw new Error("--session-minutes must be greater than zero");
  }
  if (!Number.isFinite(args.ngrokTimeoutMs) || args.ngrokTimeoutMs <= 0) {
    throw new Error("--ngrok-timeout must be greater than zero");
  }

  return args;
}

function printHelp() {
  console.log(`Cab Cam multi-Pi Node relay

Usage:
  node server.js [options]

Options:
  --host <host>                    Address to bind. Default: 0.0.0.0
  --port <port>                    HTTP port. Default: 3000
  --public-base-url <url>          Stable URL used inside per-Pi QR codes
  --no-ngrok                       LAN/local testing; session links do not start ngrok
  --ngrok-bin <path>               ngrok executable. Default: ngrok
  --ngrok-timeout <seconds>        Time to wait for a new tunnel. Default: 20
  --ngrok-api-start-port <port>    First local ngrok API port. Default: 4040
  --session-minutes <minutes>      Viewer session lifetime. Default: 10
  --pi-key <key>                   Shared key required by Pi scripts
  --dashboard-token <token>        Admin dashboard token
`);
}

class WebSocketPeer extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.open = true;
    this.sendQueue = Promise.resolve();

    socket.on("data", (chunk) => this.read(chunk));
    socket.on("close", () => this.markClosed());
    socket.on("error", (error) => {
      this.emit("socketError", error);
      this.markClosed();
    });
  }

  read(chunk) {
    if (!this.open) return;
    this.buffer = Buffer.concat([this.buffer, chunk]);

    while (this.buffer.length >= 2) {
      const first = this.buffer[0];
      const second = this.buffer[1];
      const opcode = first & 0x0f;
      const masked = (second & 0x80) !== 0;
      let length = second & 0x7f;
      let offset = 2;

      if (length === 126) {
        if (this.buffer.length < offset + 2) return;
        length = this.buffer.readUInt16BE(offset);
        offset += 2;
      } else if (length === 127) {
        if (this.buffer.length < offset + 8) return;
        const high = this.buffer.readUInt32BE(offset);
        const low = this.buffer.readUInt32BE(offset + 4);
        if (high !== 0) {
          this.close(1009, "Frame too large");
          return;
        }
        length = low;
        offset += 8;
      }

      if (length > 8 * 1024 * 1024) {
        this.close(1009, "Frame too large");
        return;
      }

      let mask = null;
      if (masked) {
        if (this.buffer.length < offset + 4) return;
        mask = this.buffer.subarray(offset, offset + 4);
        offset += 4;
      }

      const end = offset + length;
      if (this.buffer.length < end) return;

      const payload = Buffer.from(this.buffer.subarray(offset, end));
      this.buffer = this.buffer.subarray(end);

      if (mask) {
        for (let index = 0; index < payload.length; index += 1) {
          payload[index] ^= mask[index % 4];
        }
      }

      if (opcode === 0x8) {
        this.markClosed();
        this.socket.end();
        return;
      }
      if (opcode === 0x9) {
        this.sendFrame(0x0a, payload);
        continue;
      }
      if (opcode === 0x0a) continue;
      if (opcode === 0x1) this.emit("message", payload.toString("utf8"), false);
      else if (opcode === 0x2) this.emit("message", payload, true);
    }
  }

  sendText(value) {
    return this.sendFrame(0x1, Buffer.from(String(value), "utf8"));
  }

  sendBinary(value) {
    return this.sendFrame(0x2, Buffer.from(value));
  }

  sendFrame(opcode, payload) {
    if (!this.open) return Promise.resolve(false);

    let header;
    if (payload.length <= 125) {
      header = Buffer.from([0x80 | opcode, payload.length]);
    } else if (payload.length <= 65535) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode;
      header[1] = 126;
      header.writeUInt16BE(payload.length, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode;
      header[1] = 127;
      header.writeUInt32BE(0, 2);
      header.writeUInt32BE(payload.length, 6);
    }

    const frame = Buffer.concat([header, payload]);
    this.sendQueue = this.sendQueue
      .then(() => new Promise((resolve) => this.socket.write(frame, () => resolve(true))))
      .catch(() => false);
    return this.sendQueue;
  }

  close(code = 1000, reason = "") {
    if (!this.open) return;
    const reasonBuffer = Buffer.from(reason, "utf8");
    const payload = Buffer.alloc(2 + reasonBuffer.length);
    payload.writeUInt16BE(code, 0);
    reasonBuffer.copy(payload, 2);
    this.sendFrame(0x8, payload).finally(() => this.socket.end());
    this.markClosed();
  }

  markClosed() {
    if (!this.open) return;
    this.open = false;
    this.emit("close");
  }
}

function acceptWebSocket(req, socket) {
  const key = req.headers["sec-websocket-key"];
  if (!key) {
    rejectUpgrade(socket, 400, "Bad Request");
    return null;
  }

  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write([
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
    "\r\n",
  ].join("\r\n"));
  return new WebSocketPeer(socket);
}

function rejectUpgrade(socket, status, message) {
  socket.write(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

function sanitizeDeviceId(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_.-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function parseCookies(header) {
  const cookies = {};
  if (!header) return cookies;
  for (const part of header.split(";")) {
    const index = part.indexOf("=");
    if (index === -1) continue;
    cookies[part.slice(0, index).trim()] = decodeURIComponent(part.slice(index + 1).trim());
  }
  return cookies;
}

function sendHtml(res, status, body, headers = {}) {
  const payload = Buffer.from(body, "utf8");
  res.writeHead(status, {
    "Content-Type": "text/html; charset=utf-8",
    "Content-Length": payload.length,
    "Cache-Control": "no-store",
    ...headers,
  });
  res.end(payload);
}

function sendJson(res, status, value) {
  const payload = Buffer.from(JSON.stringify(value, null, 2), "utf8");
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": payload.length,
    "Cache-Control": "no-store",
  });
  res.end(payload);
}

function redirect(res, location, headers = {}) {
  res.writeHead(303, {
    Location: location,
    "Cache-Control": "no-store",
    ...headers,
  });
  res.end();
}

function localLanIp() {
  const fallback = [];
  for (const entries of Object.values(os.networkInterfaces())) {
    for (const entry of entries || []) {
      if (entry.family !== "IPv4" || entry.internal || entry.address.startsWith("169.254.")) continue;
      fallback.push(entry.address);
    }
  }
  for (const [name, entries] of Object.entries(os.networkInterfaces())) {
    if (/virtual|vmware|vbox|docker|wsl|vethernet|loopback|hyper-v/i.test(name)) continue;
    for (const entry of entries || []) {
      if (entry.family === "IPv4" && !entry.internal && !entry.address.startsWith("169.254.")) return entry.address;
    }
  }
  return fallback[0] || "127.0.0.1";
}

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    const transport = url.startsWith("https:") ? https : http;
    const req = transport.get(url, { timeout: 2000 }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
        } catch (error) {
          reject(error);
        }
      });
    });
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", reject);
  });
}

function createApp(config) {
  const state = {
    startedAt: Date.now(),
    localBaseUrl: `http://127.0.0.1:${config.port}`,
    lanBaseUrl: `http://${localLanIp()}:${config.port}`,
    devices: new Map(),
    events: new Set(),
    nextTunnelOffset: 0,
  };

  function adminCookie() {
    return `cabcam_admin=${encodeURIComponent(config.adminToken)}; Path=/; HttpOnly; SameSite=Lax`;
  }

  function adminAuthorized(req, parsed) {
    if (parsed.searchParams.get("admin") === config.adminToken) return true;
    return parseCookies(req.headers.cookie || "").cabcam_admin === config.adminToken;
  }

  function externalBaseUrl(req = null) {
    if (config.publicBaseUrl) return config.publicBaseUrl;
    if (req && req.headers.host) {
      const proto = String(req.headers["x-forwarded-proto"] || "http").split(",", 1)[0].trim() || "http";
      return `${proto}://${req.headers.host}`;
    }
    return state.lanBaseUrl;
  }

  function landingPath(deviceId) {
    return `/d/${encodeURIComponent(deviceId)}`;
  }

  function landingUrl(deviceId, req = null) {
    return `${externalBaseUrl(req)}${landingPath(deviceId)}`;
  }

  function qrPath(deviceId) {
    return `/qr/${encodeURIComponent(deviceId)}.svg`;
  }

  function viewPath(deviceId, token) {
    return `/view/${encodeURIComponent(deviceId)}/${encodeURIComponent(token)}`;
  }

  function getDevice(rawDeviceId, create = false) {
    const deviceId = sanitizeDeviceId(rawDeviceId);
    if (!deviceId) return null;
    if (!state.devices.has(deviceId) && create) {
      state.devices.set(deviceId, {
        id: deviceId,
        name: deviceId,
        conn: null,
        connected: false,
        remoteAddress: "",
        connectedAt: null,
        lastSeen: null,
        camera: false,
        streaming: false,
        width: null,
        height: null,
        fps: 0,
        frames: 0,
        frameSeq: 0,
        fpsWindowStartedAt: Date.now(),
        fpsWindowFrames: 0,
        latestFrame: null,
        message: "",
        lastError: "",
        activeSession: null,
        viewers: new Map(),
        previews: new Map(),
      });
    }
    return state.devices.get(deviceId) || null;
  }

  function deviceSummary(device, req = null) {
    cleanupDeviceSession(device);
    const session = device.activeSession;
    return {
      id: device.id,
      name: device.name,
      connected: device.connected,
      remoteAddress: device.remoteAddress,
      connectedAt: device.connectedAt,
      lastSeen: device.lastSeen,
      camera: device.camera,
      streaming: device.streaming,
      width: device.width,
      height: device.height,
      fps: device.fps,
      frames: device.frames,
      frameSeq: device.frameSeq,
      message: device.message,
      lastError: device.lastError,
      landingUrl: landingUrl(device.id, req),
      qrUrl: `${externalBaseUrl(req)}${qrPath(device.id)}`,
      viewers: device.viewers.size,
      session: session ? {
        active: true,
        token: session.token,
        createdAt: session.createdAt,
        expiresAt: session.expiresAt,
        remainingSeconds: Math.max(0, Math.ceil((session.expiresAt - Date.now()) / 1000)),
        tunnelUrl: session.tunnel ? session.tunnel.publicUrl : null,
        viewerUrl: session.viewerUrl || null,
        tunnelStarting: Boolean(session.tunnelStarting),
        tunnelError: session.tunnelError || "",
      } : {
        active: false,
        token: null,
        createdAt: null,
        expiresAt: null,
        remainingSeconds: 0,
        tunnelUrl: null,
        viewerUrl: null,
        tunnelStarting: false,
        tunnelError: "",
      },
    };
  }

  function allState(req = null) {
    return {
      uptimeSeconds: Math.floor((Date.now() - state.startedAt) / 1000),
      localBaseUrl: state.localBaseUrl,
      lanBaseUrl: state.lanBaseUrl,
      publicBaseUrl: config.publicBaseUrl || null,
      ngrokEnabled: !config.noNgrok,
      devices: Array.from(state.devices.values()).map((device) => deviceSummary(device, req)),
    };
  }

  function pushState() {
    const payload = `data: ${JSON.stringify(allState())}\n\n`;
    for (const res of Array.from(state.events)) {
      try {
        res.write(payload);
      } catch (_error) {
        state.events.delete(res);
      }
    }
  }

  function sendPiCommand(device, action) {
    if (!device.conn || !device.connected) return false;
    device.conn.sendText(JSON.stringify({
      type: "command",
      id: crypto.randomBytes(9).toString("base64url"),
      action,
      at: new Date().toISOString(),
    }));
    return true;
  }

  function receiveFrame(device, frame) {
    device.latestFrame = Buffer.from(frame);
    device.frameSeq += 1;
    device.frames += 1;
    device.lastSeen = new Date().toISOString();
    device.connected = true;
    device.fpsWindowFrames += 1;

    const now = Date.now();
    const elapsed = now - device.fpsWindowStartedAt;
    if (elapsed >= 1000) {
      device.fps = Math.round((device.fpsWindowFrames * 1000 / elapsed) * 10) / 10;
      device.fpsWindowStartedAt = now;
      device.fpsWindowFrames = 0;
      pushState();
    }

    cleanupDeviceSession(device);
    for (const [id, viewer] of Array.from(device.viewers)) {
      if (!device.activeSession || viewer.token !== device.activeSession.token) {
        viewer.conn.close(1000, "Session ended");
        device.viewers.delete(id);
        continue;
      }
      viewer.conn.sendBinary(frame).catch(() => device.viewers.delete(id));
    }

    for (const [id, preview] of Array.from(device.previews)) {
      preview.sendBinary(frame).catch(() => device.previews.delete(id));
    }
  }

  function handlePiText(device, text) {
    let message;
    try {
      message = JSON.parse(text);
    } catch (_error) {
      device.message = text.slice(0, 200);
      pushState();
      return;
    }

    device.lastSeen = new Date().toISOString();
    if (message.type === "hello") {
      device.name = message.name || device.name;
      device.message = "Pi connected";
    } else if (message.type === "status") {
      device.name = message.name || device.name;
      device.camera = Boolean(message.camera);
      device.streaming = Boolean(message.streaming);
      device.width = message.width || device.width;
      device.height = message.height || device.height;
      device.frames = Number(message.frames || device.frames || 0);
      device.message = message.message || "";
      if (message.error) device.lastError = message.error;
    } else if (message.type === "error") {
      device.lastError = message.error || message.message || "Pi error";
      device.message = "Pi error";
    }
    pushState();
  }

  function registerPi(req, peer, parsed) {
    const rawDeviceId = parsed.searchParams.get("device")
      || parsed.searchParams.get("device_id")
      || parsed.searchParams.get("name")
      || "pi";
    const device = getDevice(rawDeviceId, true);
    if (!device) {
      peer.close(1008, "Invalid device id");
      return;
    }

    if (device.conn && device.conn !== peer) {
      device.conn.close(1000, "Replaced by a new connection for this device");
    }

    device.conn = peer;
    device.connected = true;
    device.name = parsed.searchParams.get("name") || device.name;
    device.remoteAddress = req.socket.remoteAddress || "";
    device.connectedAt = new Date().toISOString();
    device.lastSeen = device.connectedAt;
    device.message = "Pi connected";
    device.lastError = "";

    peer.sendText(JSON.stringify({
      type: "hello",
      device_id: device.id,
      device_url: landingUrl(device.id),
      qr_url: `${externalBaseUrl()}${qrPath(device.id)}`,
      at: new Date().toISOString(),
    }));
    pushState();

    peer.on("message", (payload, binary) => {
      if (binary) receiveFrame(device, payload);
      else handlePiText(device, payload);
    });
    peer.on("close", () => {
      if (device.conn === peer) {
        device.conn = null;
        device.connected = false;
        device.camera = false;
        device.streaming = false;
        device.message = "Pi disconnected";
        pushState();
      }
    });
  }

  function registerViewer(device, token, peer) {
    cleanupDeviceSession(device);
    if (!device.activeSession || device.activeSession.token !== token) {
      peer.close(1008, "Invalid session");
      return;
    }

    const id = crypto.randomUUID();
    device.viewers.set(id, { token, conn: peer, connectedAt: Date.now() });
    if (device.latestFrame) peer.sendBinary(device.latestFrame);
    peer.on("close", () => {
      device.viewers.delete(id);
      pushState();
    });
    pushState();
  }

  function registerPreview(device, peer) {
    const id = crypto.randomUUID();
    device.previews.set(id, peer);
    if (device.latestFrame) peer.sendBinary(device.latestFrame);
    peer.on("close", () => {
      device.previews.delete(id);
      pushState();
    });
    pushState();
  }

  function createSession(device) {
    cleanupDeviceSession(device);
    stopSession(device);

    const token = crypto.randomBytes(18).toString("base64url");
    const now = Date.now();
    device.activeSession = {
      token,
      createdAt: now,
      expiresAt: now + config.sessionMinutes * 60 * 1000,
      viewerUrl: null,
      tunnel: null,
      tunnelStarting: false,
      tunnelError: "",
    };
    pushState();
    return device.activeSession;
  }

  function stopSession(device) {
    if (!device.activeSession) return;
    const session = device.activeSession;
    for (const viewer of device.viewers.values()) {
      viewer.conn.close(1000, "Session ended");
    }
    device.viewers.clear();
    if (session.tunnel && session.tunnel.process && !session.tunnel.process.killed) {
      session.tunnel.process.kill();
    }
    device.activeSession = null;
    pushState();
  }

  function cleanupDeviceSession(device) {
    if (!device.activeSession) return;
    if (device.activeSession.expiresAt > Date.now()) return;
    stopSession(device);
  }

  function cleanupAllSessions() {
    for (const device of state.devices.values()) cleanupDeviceSession(device);
  }

  function ngrokTarget() {
    const targetHost = config.host === "0.0.0.0" || config.host === "::" || config.host === ""
      ? "127.0.0.1"
      : config.host;
    return `http://${targetHost}:${config.port}`;
  }

  async function waitForNgrokUrl(apiPort) {
    const deadline = Date.now() + config.ngrokTimeoutMs;
    const apiUrl = `http://127.0.0.1:${apiPort}/api/tunnels`;
    let lastError = "ngrok API was not ready";

    while (Date.now() < deadline) {
      try {
        const payload = await httpGetJson(apiUrl);
        const publicUrls = [];
        for (const tunnel of payload.tunnels || []) {
          if (tunnel.public_url) publicUrls.push(String(tunnel.public_url).replace(/\/$/, ""));
        }
        const httpsUrl = publicUrls.find((url) => url.startsWith("https://"));
        if (httpsUrl) return httpsUrl;
        if (publicUrls.length > 0) return publicUrls[0];
        lastError = "ngrok API returned no public URL";
      } catch (error) {
        lastError = error.message;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(`Timed out waiting for ngrok URL. Last error: ${lastError}`);
  }

  async function startTunnelForSession(device, session) {
    if (config.noNgrok) {
      session.viewerUrl = `${externalBaseUrl()}${viewPath(device.id, session.token)}`;
      pushState();
      return session.viewerUrl;
    }

    const apiPort = config.ngrokApiStartPort + state.nextTunnelOffset;
    state.nextTunnelOffset += 1;
    session.tunnelStarting = true;
    session.tunnelError = "";
    pushState();

    const args = [
      "http",
      ngrokTarget(),
      "--log=stdout",
      `--web-addr=127.0.0.1:${apiPort}`,
    ];

    const proc = childProcess.spawn(config.ngrokBin, args, {
      stdio: ["ignore", "ignore", "pipe"],
    });

    const tunnel = {
      process: proc,
      apiPort,
      publicUrl: null,
    };
    session.tunnel = tunnel;

    proc.stderr.on("data", (chunk) => {
      const line = chunk.toString("utf8").trim();
      if (line) {
        session.tunnelError = line.slice(0, 500);
        pushState();
      }
    });
    proc.on("error", (error) => {
      session.tunnelStarting = false;
      session.tunnelError = error.message;
      pushState();
    });
    proc.on("exit", (code, signal) => {
      if (device.activeSession === session) {
        if (code !== 0 && signal !== "SIGTERM") {
          session.tunnelError = `ngrok exited with code ${code}`;
        }
        if (!session.viewerUrl) session.tunnelStarting = false;
        pushState();
      }
    });

    try {
      const publicUrl = await waitForNgrokUrl(apiPort);
      tunnel.publicUrl = publicUrl;
      session.viewerUrl = `${publicUrl}${viewPath(device.id, session.token)}`;
      session.tunnelStarting = false;
      pushState();
      console.log(`ngrok session for ${device.id}: ${session.viewerUrl}`);
      return session.viewerUrl;
    } catch (error) {
      proc.kill();
      session.tunnelStarting = false;
      session.tunnelError = error.message;
      pushState();
      throw error;
    }
  }

  async function startUserSession(device) {
    if (!device.connected) {
      throw new Error("This Pi is not connected to the Node server.");
    }
    const session = createSession(device);
    return startTunnelForSession(device, session);
  }

  function deviceFromPath(parts, index) {
    return getDevice(decodeURIComponent(parts[index] || ""), false);
  }

  async function handleHttp(req, res) {
    const parsed = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    const parts = parsed.pathname.split("/").filter(Boolean);
    const authed = adminAuthorized(req, parsed);
    const authHeaders = parsed.searchParams.get("admin") === config.adminToken
      ? { "Set-Cookie": adminCookie() }
      : {};

    if (req.method === "GET" && (parsed.pathname === "/" || parsed.pathname === "/admin")) {
      if (!authed) {
        sendHtml(res, 401, lockedPage());
        return;
      }
      sendHtml(res, 200, adminPage(), authHeaders);
      return;
    }

    if (req.method === "GET" && parsed.pathname === "/events") {
      if (!authed) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
      }
      res.writeHead(200, {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });
      res.write(`data: ${JSON.stringify(allState(req))}\n\n`);
      state.events.add(res);
      req.on("close", () => state.events.delete(res));
      return;
    }

    if (req.method === "GET" && parsed.pathname === "/api/devices") {
      if (!authed) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
      }
      sendJson(res, 200, allState(req));
      return;
    }

    if (req.method === "GET" && parsed.pathname === "/api/pi/config") {
      const key = parsed.searchParams.get("key") || "";
      if (key !== config.piKey) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
      }
      const device = getDevice(parsed.searchParams.get("device") || parsed.searchParams.get("device_id"), true);
      if (!device) {
        sendJson(res, 400, { error: "Invalid device id" });
        return;
      }
      sendJson(res, 200, {
        device_id: device.id,
        ws_url: `${externalBaseUrl(req).replace(/^http/, "ws")}/api/pi/ws`,
        device_url: landingUrl(device.id, req),
        qr_url: `${externalBaseUrl(req)}${qrPath(device.id)}`,
      });
      return;
    }

    if (req.method === "GET" && parsed.pathname === "/healthz") {
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("ok\n");
      return;
    }

    if (req.method === "GET" && parts[0] === "qr" && parts[1]) {
      const deviceId = sanitizeDeviceId(parts[1].replace(/\.svg$/i, ""));
      if (!deviceId) {
        sendJson(res, 400, { error: "Invalid device id" });
        return;
      }
      await sendQrSvg(res, landingUrl(deviceId, req));
      return;
    }

    if (req.method === "GET" && parts[0] === "d" && parts[1]) {
      const deviceId = sanitizeDeviceId(decodeURIComponent(parts[1]));
      const device = getDevice(deviceId, false);
      sendHtml(res, 200, userLandingPage(deviceId, device, req));
      return;
    }

    if (req.method === "POST" && parts[0] === "d" && parts[1] && parts[2] === "session") {
      const device = deviceFromPath(parts, 1);
      if (!device) {
        sendHtml(res, 404, userMessagePage("Device not found", "This QR code does not match a registered Pi."));
        return;
      }
      try {
        const viewerUrl = await startUserSession(device);
        redirect(res, viewerUrl);
      } catch (error) {
        sendHtml(res, 503, userMessagePage("Session could not start", error.message));
      }
      return;
    }

    if (req.method === "GET" && parts[0] === "view" && parts[1] && parts[2]) {
      const device = deviceFromPath(parts, 1);
      const token = decodeURIComponent(parts[2]);
      cleanupDeviceSession(device || {});
      if (!device || !device.activeSession || device.activeSession.token !== token) {
        sendHtml(res, 404, userMessagePage("Session expired", "Start a new session from the Pi QR page."));
        return;
      }
      sendHtml(res, 200, viewerPage(device, token));
      return;
    }

    if (req.method === "GET" && parts[0] === "snapshot" && parts[1] && parts[2]) {
      const device = deviceFromPath(parts, 1);
      const token = decodeURIComponent(parts[2]);
      cleanupDeviceSession(device || {});
      if (!device || !device.activeSession || device.activeSession.token !== token) {
        sendJson(res, 404, { error: "Session expired" });
        return;
      }
      if (!device.latestFrame) {
        sendJson(res, 503, { error: "No frame from Pi yet" });
        return;
      }
      res.writeHead(200, {
        "Content-Type": "image/jpeg",
        "Content-Length": device.latestFrame.length,
        "Cache-Control": "no-store",
      });
      res.end(device.latestFrame);
      return;
    }

    if (req.method === "POST" && parts[0] === "api" && parts[1] === "devices" && parts[2] && parts[3] === "commands" && parts[4]) {
      if (!authed) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
      }
      const device = deviceFromPath(parts, 2);
      const actions = new Set(["start_camera", "stop_camera", "restart_camera", "status"]);
      const action = parts[4];
      if (!device || !actions.has(action)) {
        sendJson(res, 404, { error: "Device or command not found" });
        return;
      }
      if (!sendPiCommand(device, action)) {
        sendJson(res, 409, { error: "Pi is not connected" });
        return;
      }
      sendJson(res, 200, { ok: true });
      return;
    }

    if (req.method === "POST" && parts[0] === "api" && parts[1] === "devices" && parts[2] && parts[3] === "session" && parts[4] === "end") {
      if (!authed) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
      }
      const device = deviceFromPath(parts, 2);
      if (!device) {
        sendJson(res, 404, { error: "Device not found" });
        return;
      }
      stopSession(device);
      sendJson(res, 200, { ok: true });
      return;
    }

    sendHtml(res, 404, userMessagePage("Not found", "The requested page does not exist."));
  }

  function handleUpgrade(req, socket) {
    const parsed = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    const parts = parsed.pathname.split("/").filter(Boolean);

    if (parsed.pathname === "/api/pi/ws" || parsed.pathname === "/ws/pi") {
      const key = parsed.searchParams.get("key") || req.headers["x-pi-key"] || "";
      if (key !== config.piKey) {
        rejectUpgrade(socket, 401, "Unauthorized");
        return;
      }
      const peer = acceptWebSocket(req, socket);
      if (peer) registerPi(req, peer, parsed);
      return;
    }

    if (parts[0] === "ws" && parts[1] === "view" && parts[2] && parts[3]) {
      const device = deviceFromPath(parts, 2);
      const token = decodeURIComponent(parts[3]);
      if (!device || !device.activeSession || device.activeSession.token !== token) {
        rejectUpgrade(socket, 404, "Session expired");
        return;
      }
      const peer = acceptWebSocket(req, socket);
      if (peer) registerViewer(device, token, peer);
      return;
    }

    if (parts[0] === "ws" && parts[1] === "preview" && parts[2]) {
      if (!adminAuthorized(req, parsed)) {
        rejectUpgrade(socket, 401, "Unauthorized");
        return;
      }
      const device = deviceFromPath(parts, 2);
      if (!device) {
        rejectUpgrade(socket, 404, "Device not found");
        return;
      }
      const peer = acceptWebSocket(req, socket);
      if (peer) registerPreview(device, peer);
      return;
    }

    rejectUpgrade(socket, 404, "Not Found");
  }

  function lockedPage() {
    return pageShell("Dashboard locked", `<section class="panel"><h1>Dashboard locked</h1><p>Open the dashboard URL printed by the Node server.</p></section>`);
  }

  function adminPage() {
    return pageShell("Cab Cam Control", `
<header><h1>Cab Cam Control</h1><span id="summary">No Pi connected</span></header>
<main>
  <section class="panel">
    <dl>
      <dt>LAN base</dt><dd id="lanBase">-</dd>
      <dt>QR base</dt><dd id="qrBase">-</dd>
      <dt>ngrok</dt><dd id="ngrokMode">-</dd>
    </dl>
  </section>
  <section id="devices" class="devices"></section>
</main>
<script>
const devicesEl = document.getElementById("devices");
const summaryEl = document.getElementById("summary");
const lanBaseEl = document.getElementById("lanBase");
const qrBaseEl = document.getElementById("qrBase");
const ngrokModeEl = document.getElementById("ngrokMode");

function fmtSeconds(value) {
  if (!value) return "0s";
  const mins = Math.floor(value / 60);
  const secs = value % 60;
  return mins > 0 ? mins + "m " + secs + "s" : secs + "s";
}

function render(state) {
  summaryEl.textContent = state.devices.length + " Pi device" + (state.devices.length === 1 ? "" : "s");
  lanBaseEl.textContent = state.lanBaseUrl || "-";
  qrBaseEl.textContent = state.publicBaseUrl || state.lanBaseUrl || "-";
  ngrokModeEl.textContent = state.ngrokEnabled ? "session tunnels on demand" : "disabled";
  if (state.devices.length === 0) {
    devicesEl.innerHTML = '<section class="panel"><h2>No Pi devices</h2><p>Start a Pi script with the command printed by Node.</p></section>';
    return;
  }
  devicesEl.innerHTML = state.devices.map(deviceCard).join("");
}

function deviceCard(device) {
  const sessionText = device.session.active
    ? "active, " + fmtSeconds(device.session.remainingSeconds) + " left"
    : "none";
  const viewerUrl = device.session.viewerUrl || "";
  return '<article class="panel device">' +
    '<div class="deviceHead"><div><h2>' + esc(device.name || device.id) + '</h2><p>' + esc(device.id) + '</p></div>' +
    '<span class="pill ' + (device.connected ? 'ok' : 'bad') + '">' + (device.connected ? 'online' : 'offline') + '</span></div>' +
    '<div class="deviceBody">' +
      '<img class="qr" src="' + esc(new URL(device.qrUrl).pathname) + '" alt="QR for ' + esc(device.id) + '">' +
      '<dl>' +
        '<dt>QR target</dt><dd><a href="' + esc(device.landingUrl) + '" target="_blank" rel="noopener">' + esc(device.landingUrl) + '</a></dd>' +
        '<dt>Camera</dt><dd>' + (device.camera ? 'running' : 'stopped') + ', ' + (device.fps || 0) + ' fps</dd>' +
        '<dt>Frames</dt><dd>' + device.frames + '</dd>' +
        '<dt>Session</dt><dd>' + esc(sessionText) + '</dd>' +
        '<dt>Viewer URL</dt><dd>' + (viewerUrl ? '<a href="' + esc(viewerUrl) + '" target="_blank" rel="noopener">' + esc(viewerUrl) + '</a>' : '-') + '</dd>' +
        '<dt>Message</dt><dd>' + esc(device.message || '-') + '</dd>' +
        '<dt>Error</dt><dd>' + esc(device.lastError || device.session.tunnelError || '-') + '</dd>' +
      '</dl>' +
    '</div>' +
    '<div class="toolbar">' +
      '<button onclick="cmd(\\'' + escAttr(device.id) + '\\', \\'start_camera\\')">Start Camera</button>' +
      '<button class="secondary" onclick="cmd(\\'' + escAttr(device.id) + '\\', \\'stop_camera\\')">Stop Camera</button>' +
      '<button class="secondary" onclick="cmd(\\'' + escAttr(device.id) + '\\', \\'restart_camera\\')">Restart Camera</button>' +
      '<button class="danger" onclick="endSession(\\'' + escAttr(device.id) + '\\')" ' + (device.session.active ? '' : 'disabled') + '>End Session</button>' +
    '</div>' +
  '</article>';
}

async function cmd(id, action) {
  await post("/api/devices/" + encodeURIComponent(id) + "/commands/" + action);
}
async function endSession(id) {
  await post("/api/devices/" + encodeURIComponent(id) + "/session/end");
}
async function post(path) {
  const response = await fetch(path, { method: "POST" });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    alert(body.error || "Request failed");
  }
}
function esc(value) {
  return String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function escAttr(value) {
  return esc(value).replace(/\\\\/g, "\\\\\\\\");
}
const events = new EventSource("/events");
events.onmessage = event => render(JSON.parse(event.data));
fetch("/api/devices").then(r => r.json()).then(render).catch(() => {});
setInterval(() => fetch("/api/devices").then(r => r.json()).then(render).catch(() => {}), 5000);
</script>`);
  }

  function userLandingPage(deviceId, device, req) {
    const online = Boolean(device && device.connected);
    const name = device ? device.name : deviceId;
    return pageShell("Start Session", `
<main class="center">
  <section class="panel startPanel">
    <h1>${escapeHtml(name)}</h1>
    <p class="${online ? "okText" : "badText"}">${online ? "Pi is online" : "Pi is offline"}</p>
    <form method="post" action="${escapeHtml(landingPath(deviceId))}/session">
      <button type="submit" ${online ? "" : "disabled"}>Start a session</button>
    </form>
    <p class="muted">QR target: ${escapeHtml(landingUrl(deviceId, req))}</p>
  </section>
</main>`);
  }

  function viewerPage(device, token) {
    const session = device.activeSession;
    return pageShell("Live Stream", `
<header>
  <h1>${escapeHtml(device.name || device.id)}</h1>
  <span id="status">connecting</span>
  <span id="countdown">--:--</span>
  <a href="/snapshot/${encodeURIComponent(device.id)}/${encodeURIComponent(token)}">Snapshot</a>
</header>
<canvas id="video" width="960" height="540"></canvas>
<script>
const token = ${JSON.stringify(token)};
const deviceId = ${JSON.stringify(device.id)};
const expiresAt = ${session.expiresAt};
const canvas = document.getElementById("video");
const ctx = canvas.getContext("2d", { alpha: false });
const statusEl = document.getElementById("status");
const countdownEl = document.getElementById("countdown");
let latest = null;
let drawing = false;
let frames = 0;
let lastFps = performance.now();
function wsUrl() {
  const scheme = location.protocol === "https:" ? "wss:" : "ws:";
  return scheme + "//" + location.host + "/ws/view/" + encodeURIComponent(deviceId) + "/" + encodeURIComponent(token);
}
function tick() {
  const seconds = Math.max(0, Math.floor((expiresAt - Date.now()) / 1000));
  countdownEl.textContent = "expires " + Math.floor(seconds / 60).toString().padStart(2, "0") + ":" + (seconds % 60).toString().padStart(2, "0");
  if (seconds <= 0) location.reload();
}
function connect() {
  statusEl.textContent = "connecting";
  const ws = new WebSocket(wsUrl());
  ws.binaryType = "arraybuffer";
  ws.onopen = () => statusEl.textContent = "live";
  ws.onmessage = event => { latest = event.data; if (!drawing) requestAnimationFrame(draw); };
  ws.onclose = () => { statusEl.textContent = "reconnecting"; setTimeout(connect, 1000); };
  ws.onerror = () => ws.close();
}
function drawFallback(data) {
  return new Promise(resolve => {
    const url = URL.createObjectURL(new Blob([data], { type: "image/jpeg" }));
    const image = new Image();
    image.onload = () => {
      canvas.width = image.naturalWidth || canvas.width;
      canvas.height = image.naturalHeight || canvas.height;
      ctx.drawImage(image, 0, 0, canvas.width, canvas.height);
      URL.revokeObjectURL(url);
      resolve();
    };
    image.onerror = () => { URL.revokeObjectURL(url); resolve(); };
    image.src = url;
  });
}
async function draw() {
  const data = latest;
  latest = null;
  if (!data) { drawing = false; return; }
  drawing = true;
  try {
    if ("createImageBitmap" in window) {
      const bitmap = await createImageBitmap(new Blob([data], { type: "image/jpeg" }));
      canvas.width = bitmap.width;
      canvas.height = bitmap.height;
      ctx.drawImage(bitmap, 0, 0);
      bitmap.close();
    } else {
      await drawFallback(data);
    }
  } catch (_error) {
    await drawFallback(data);
  }
  frames += 1;
  const now = performance.now();
  if (now - lastFps >= 1000) {
    statusEl.textContent = "live " + frames + " fps";
    frames = 0;
    lastFps = now;
  }
  if (latest) requestAnimationFrame(draw);
  else drawing = false;
}
setInterval(tick, 1000);
tick();
connect();
</script>`);
  }

  function userMessagePage(title, message) {
    return pageShell(title, `<main class="center"><section class="panel startPanel"><h1>${escapeHtml(title)}</h1><p>${escapeHtml(message)}</p></section></main>`);
  }

  function pageShell(title, body) {
    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <style>
    :root {
      color-scheme: light;
      font-family: Arial, Helvetica, sans-serif;
      background: #f4f6f2;
      color: #202124;
      --panel: #fff;
      --border: #c7cdc2;
      --muted: #59615a;
      --accent: #0f766e;
      --accent-dark: #0b5f59;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; }
    header {
      min-height: 58px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 12px;
      padding: 12px 18px;
      background: #252821;
      color: #fff;
    }
    h1, h2, p { margin-top: 0; }
    h1 { font-size: 22px; }
    h2 { font-size: 18px; }
    a { color: #0f5f8f; overflow-wrap: anywhere; }
    main { width: min(100%, 1160px); margin: 0 auto; padding: 18px; }
    .center { min-height: 100vh; display: grid; place-items: center; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px;
    }
    .startPanel { width: min(100%, 440px); }
    .devices { display: grid; gap: 12px; }
    .deviceHead { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
    .deviceHead p { color: var(--muted); margin-bottom: 0; }
    .deviceBody { display: grid; grid-template-columns: 132px 1fr; gap: 14px; align-items: start; }
    .qr { width: 132px; height: 132px; border: 1px solid var(--border); background: white; }
    dl { display: grid; grid-template-columns: 110px 1fr; gap: 8px 10px; margin: 0; }
    dt { color: var(--muted); }
    dd { margin: 0; overflow-wrap: anywhere; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
    button {
      min-height: 40px;
      border: 1px solid transparent;
      border-radius: 6px;
      padding: 0 14px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
    }
    button:hover { background: var(--accent-dark); }
    button.secondary { background: #fff; border-color: var(--border); color: #202124; }
    button.danger { background: var(--danger); }
    button:disabled { opacity: .45; cursor: not-allowed; }
    .pill { display: inline-flex; align-items: center; min-height: 28px; border-radius: 999px; padding: 0 10px; font-weight: 700; font-size: 13px; }
    .pill.ok { background: #d7f2e5; color: #0d5f3a; }
    .pill.bad { background: #fde0dc; color: #8a1f16; }
    .okText { color: #0d5f3a; font-weight: 700; }
    .badText { color: #8a1f16; font-weight: 700; }
    .muted { color: var(--muted); overflow-wrap: anywhere; }
    canvas {
      display: block;
      width: 100%;
      height: calc(100vh - 58px);
      background: #000;
      object-fit: contain;
    }
    @media (max-width: 720px) {
      .deviceBody { grid-template-columns: 1fr; }
      dl { grid-template-columns: 1fr; }
      .center { padding: 18px; }
    }
  </style>
</head>
<body>${body}</body>
</html>`;
  }

  setInterval(cleanupAllSessions, 10000).unref();

  return {
    state,
    handleHttp,
    handleUpgrade,
    stopAll() {
      for (const device of state.devices.values()) stopSession(device);
    },
  };
}

async function sendQrSvg(res, text) {
  if (OptionalQRCode) {
    const svg = await OptionalQRCode.toString(text, {
      type: "svg",
      errorCorrectionLevel: "M",
      margin: 2,
    });
    const payload = Buffer.from(svg, "utf8");
    res.writeHead(200, {
      "Content-Type": "image/svg+xml; charset=utf-8",
      "Content-Length": payload.length,
      "Cache-Control": "no-store",
    });
    res.end(payload);
    return;
  }

  let svg;
  try {
    svg = makeQrSvg(text);
  } catch (_error) {
    svg = `<svg xmlns="http://www.w3.org/2000/svg" width="420" height="120" viewBox="0 0 420 120"><rect width="100%" height="100%" fill="white"/><text x="12" y="32" font-family="Arial" font-size="16">Install npm package "qrcode" for QR rendering.</text><text x="12" y="68" font-family="Arial" font-size="12">${escapeHtml(text)}</text></svg>`;
  }
  const payload = Buffer.from(svg, "utf8");
  res.writeHead(200, {
    "Content-Type": "image/svg+xml; charset=utf-8",
    "Content-Length": payload.length,
    "Cache-Control": "no-store",
  });
  res.end(payload);
}

function makeQrSvg(text) {
  const qr = makeVersion5Qr(text);
  const quiet = 4;
  const size = qr.length + quiet * 2;
  const rects = [];
  for (let row = 0; row < qr.length; row += 1) {
    for (let col = 0; col < qr.length; col += 1) {
      if (qr[row][col]) rects.push(`<rect x="${col + quiet}" y="${row + quiet}" width="1" height="1"/>`);
    }
  }
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${size} ${size}" shape-rendering="crispEdges"><rect width="${size}" height="${size}" fill="#fff"/><g fill="#000">${rects.join("")}</g></svg>`;
}

function makeVersion5Qr(text) {
  const version = 5;
  const size = 17 + version * 4;
  const dataCodewords = 108;
  const eccCodewords = 26;
  const bytes = Array.from(Buffer.from(text, "utf8"));
  if (bytes.length > 106) {
    throw new Error("QR fallback supports URLs up to 106 bytes");
  }

  const data = encodeQrBytes(bytes, dataCodewords);
  const ecc = reedSolomonRemainder(data, reedSolomonDivisor(eccCodewords));
  const codewordBits = [];
  for (const byte of data.concat(ecc)) {
    for (let bit = 7; bit >= 0; bit -= 1) codewordBits.push(((byte >>> bit) & 1) !== 0);
  }

  const matrix = Array.from({ length: size }, () => Array(size).fill(false));
  const reserved = Array.from({ length: size }, () => Array(size).fill(false));
  const set = (row, col, value, isFunction = true) => {
    if (row < 0 || row >= size || col < 0 || col >= size) return;
    matrix[row][col] = Boolean(value);
    if (isFunction) reserved[row][col] = true;
  };

  function finder(top, left) {
    for (let row = -1; row <= 7; row += 1) {
      for (let col = -1; col <= 7; col += 1) {
        const r = top + row;
        const c = left + col;
        if (r < 0 || r >= size || c < 0 || c >= size) continue;
        const black = row >= 0 && row <= 6 && col >= 0 && col <= 6
          && (row === 0 || row === 6 || col === 0 || col === 6 || (row >= 2 && row <= 4 && col >= 2 && col <= 4));
        set(r, c, black);
      }
    }
  }

  finder(0, 0);
  finder(0, size - 7);
  finder(size - 7, 0);

  for (let index = 8; index < size - 8; index += 1) {
    set(6, index, index % 2 === 0);
    set(index, 6, index % 2 === 0);
  }

  for (let row = -2; row <= 2; row += 1) {
    for (let col = -2; col <= 2; col += 1) {
      const max = Math.max(Math.abs(row), Math.abs(col));
      set(30 + row, 30 + col, max === 2 || max === 0);
    }
  }

  set(4 * version + 9, 8, true);

  for (let index = 0; index <= 8; index += 1) {
    if (index !== 6) {
      set(8, index, false);
      set(index, 8, false);
    }
  }
  for (let index = 0; index < 8; index += 1) {
    set(8, size - 1 - index, false);
    set(size - 1 - index, 8, false);
  }

  let bitIndex = 0;
  for (let right = size - 1; right >= 1; right -= 2) {
    if (right === 6) right -= 1;
    for (let vert = 0; vert < size; vert += 1) {
      const upward = ((right + 1) & 2) === 0;
      const row = upward ? size - 1 - vert : vert;
      for (let j = 0; j < 2; j += 1) {
        const col = right - j;
        if (reserved[row][col]) continue;
        let bit = bitIndex < codewordBits.length ? codewordBits[bitIndex] : false;
        bitIndex += 1;
        if ((row + col) % 2 === 0) bit = !bit;
        set(row, col, bit, false);
      }
    }
  }

  drawFormatBits(matrix, reserved, 1, 0);
  return matrix;
}

function encodeQrBytes(bytes, dataCodewords) {
  const bits = [];
  const append = (value, length) => {
    for (let bit = length - 1; bit >= 0; bit -= 1) bits.push((value >>> bit) & 1);
  };
  append(0x4, 4);
  append(bytes.length, 8);
  for (const byte of bytes) append(byte, 8);
  const capacity = dataCodewords * 8;
  const terminator = Math.min(4, capacity - bits.length);
  for (let index = 0; index < terminator; index += 1) bits.push(0);
  while (bits.length % 8 !== 0) bits.push(0);
  const data = [];
  for (let index = 0; index < bits.length; index += 8) {
    let value = 0;
    for (let bit = 0; bit < 8; bit += 1) value = (value << 1) | bits[index + bit];
    data.push(value);
  }
  for (let pad = 0; data.length < dataCodewords; pad += 1) {
    data.push(pad % 2 === 0 ? 0xec : 0x11);
  }
  return data;
}

function gfMultiply(x, y) {
  let z = 0;
  while (y !== 0) {
    if ((y & 1) !== 0) z ^= x;
    x <<= 1;
    if ((x & 0x100) !== 0) x ^= 0x11d;
    y >>>= 1;
  }
  return z & 0xff;
}

function reedSolomonDivisor(degree) {
  const result = Array(degree).fill(0);
  result[degree - 1] = 1;
  let root = 1;
  for (let index = 0; index < degree; index += 1) {
    for (let j = 0; j < result.length; j += 1) {
      result[j] = gfMultiply(result[j], root);
      if (j + 1 < result.length) result[j] ^= result[j + 1];
    }
    root = gfMultiply(root, 0x02);
  }
  return result;
}

function reedSolomonRemainder(data, divisor) {
  const result = Array(divisor.length).fill(0);
  for (const byte of data) {
    const factor = byte ^ result.shift();
    result.push(0);
    for (let index = 0; index < result.length; index += 1) {
      result[index] ^= gfMultiply(divisor[index], factor);
    }
  }
  return result;
}

function drawFormatBits(matrix, reserved, errorCorrectionBits, mask) {
  const size = matrix.length;
  const data = (errorCorrectionBits << 3) | mask;
  let rem = data << 10;
  for (let bit = 14; bit >= 10; bit -= 1) {
    if (((rem >>> bit) & 1) !== 0) rem ^= 0x537 << (bit - 10);
  }
  const bits = ((data << 10) | rem) ^ 0x5412;
  const get = (index) => ((bits >>> index) & 1) !== 0;
  const set = (row, col, value) => {
    matrix[row][col] = Boolean(value);
    reserved[row][col] = true;
  };

  for (let index = 0; index <= 5; index += 1) set(8, index, get(index));
  set(8, 7, get(6));
  set(8, 8, get(7));
  set(7, 8, get(8));
  for (let index = 9; index < 15; index += 1) set(14 - index, 8, get(index));

  for (let index = 0; index < 8; index += 1) set(size - 1 - index, 8, get(index));
  for (let index = 8; index < 15; index += 1) set(8, size - 15 + index, get(index));
  set(8, size - 8, true);
}

async function main() {
  let config;
  try {
    config = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    printHelp();
    process.exit(2);
  }

  const app = createApp(config);
  const server = http.createServer((req, res) => {
    app.handleHttp(req, res).catch((error) => {
      console.error(error);
      sendJson(res, 500, { error: "Internal server error" });
    });
  });
  server.on("upgrade", app.handleUpgrade);

  function stop() {
    console.log("\nStopping Cab Cam server...");
    app.stopAll();
    server.close(() => process.exit(0));
  }
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);

  server.listen(config.port, config.host, () => {
    const lanIp = localLanIp();
    console.log(`Cab Cam Node relay listening on ${config.host}:${config.port}`);
    console.log(`Admin dashboard: http://127.0.0.1:${config.port}/?admin=${encodeURIComponent(config.adminToken)}`);
    console.log(`Admin LAN:       http://${lanIp}:${config.port}/?admin=${encodeURIComponent(config.adminToken)}`);
    console.log(`Pi command:      python3 qr_session_cam_server.py --server ws://${lanIp}:${config.port}/api/pi/ws --key ${config.piKey} --device-id <device-id>`);
    console.log(`Pi shared key:   ${config.piKey}`);
    if (config.publicBaseUrl) {
      console.log(`QR base URL:     ${config.publicBaseUrl}`);
    } else {
      console.log(`QR base URL:     http://${lanIp}:${config.port} (override with --public-base-url when Node has a stable public URL)`);
    }
    if (config.noNgrok) console.log("ngrok disabled: user sessions redirect to local/LAN viewer URLs.");
    else console.log("ngrok enabled: each user Start Session creates a fresh tunnel for that Pi stream.");
  });
}

if (require.main === module) {
  main();
}

module.exports = { createApp, parseArgs, WebSocketPeer, makeQrSvg };
