#!/usr/bin/env node
"use strict";

const http = require("http");
const https = require("https");
const crypto = require("crypto");
const childProcess = require("child_process");
const os = require("os");
const { EventEmitter } = require("events");
const { URL } = require("url");

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

function parseArgs(argv) {
  const args = {
    host: "0.0.0.0",
    port: 3000,
    noNgrok: false,
    ngrokBin: "ngrok",
    ngrokApiUrl: "http://127.0.0.1:4040",
    ngrokUrl: "",
    ngrokTimeout: 20000,
    sessionMinutes: 10,
    piKey: process.env.PI_SHARED_KEY || crypto.randomBytes(18).toString("base64url"),
    dashboardToken: process.env.DASHBOARD_TOKEN || crypto.randomBytes(18).toString("base64url"),
  };

  for (let index = 0; index < argv.length; index += 1) {
    const item = argv[index];
    const next = () => {
      index += 1;
      if (index >= argv.length) {
        throw new Error(`Missing value for ${item}`);
      }
      return argv[index];
    };

    if (item === "--host") args.host = next();
    else if (item === "--port") args.port = Number(next());
    else if (item === "--no-ngrok") args.noNgrok = true;
    else if (item === "--ngrok-bin") args.ngrokBin = next();
    else if (item === "--ngrok-api-url") args.ngrokApiUrl = next();
    else if (item === "--ngrok-url") args.ngrokUrl = next();
    else if (item === "--ngrok-timeout") args.ngrokTimeout = Number(next()) * 1000;
    else if (item === "--session-minutes") args.sessionMinutes = Number(next());
    else if (item === "--pi-key") args.piKey = next();
    else if (item === "--dashboard-token") args.dashboardToken = next();
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
  if (!Number.isFinite(args.sessionMinutes) || args.sessionMinutes <= 0) {
    throw new Error("--session-minutes must be greater than zero");
  }
  if (!Number.isFinite(args.ngrokTimeout) || args.ngrokTimeout <= 0) {
    throw new Error("--ngrok-timeout must be greater than zero");
  }

  return args;
}

function printHelp() {
  console.log(`Cab Cam Node relay

Usage:
  node server.js [options]

Options:
  --host <host>                 Address to bind. Default: 0.0.0.0
  --port <port>                 HTTP port. Default: 3000
  --no-ngrok                    Do not start ngrok; use LAN/local links
  --ngrok-bin <path>            ngrok executable. Default: ngrok
  --ngrok-api-url <url>         Local ngrok API. Default: http://127.0.0.1:4040
  --ngrok-url <url>             Optional reserved ngrok URL/domain
  --ngrok-timeout <seconds>     Time to wait for tunnel URL. Default: 20
  --session-minutes <minutes>   Public viewer link lifetime. Default: 10
  --pi-key <key>                Shared key required by the Raspberry Pi client
  --dashboard-token <token>     Token required for the control dashboard
`);
}

class WebSocketPeer extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.open = true;
    this.sendLock = Promise.resolve();

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

      let mask;
      if (masked) {
        if (this.buffer.length < offset + 4) return;
        mask = this.buffer.subarray(offset, offset + 4);
        offset += 4;
      }

      const end = offset + length;
      if (this.buffer.length < end) return;

      let payload = Buffer.from(this.buffer.subarray(offset, end));
      this.buffer = this.buffer.subarray(end);

      if (masked && mask) {
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
      if (opcode === 0x0a) {
        continue;
      }
      if (opcode === 0x1) {
        this.emit("message", payload.toString("utf8"), false);
      } else if (opcode === 0x2) {
        this.emit("message", payload, true);
      }
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
    const length = payload.length;
    let header;

    if (length <= 125) {
      header = Buffer.from([0x80 | opcode, length]);
    } else if (length <= 65535) {
      header = Buffer.alloc(4);
      header[0] = 0x80 | opcode;
      header[1] = 126;
      header.writeUInt16BE(length, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x80 | opcode;
      header[1] = 127;
      header.writeUInt32BE(0, 2);
      header.writeUInt32BE(length, 6);
    }

    const frame = Buffer.concat([header, payload]);
    this.sendLock = this.sendLock
      .then(() => new Promise((resolve) => {
        this.socket.write(frame, () => resolve(true));
      }))
      .catch(() => false);
    return this.sendLock;
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
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
    socket.destroy();
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

function sendHtml(res, body, status = 200, headers = {}) {
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

function notFound(res) {
  sendHtml(res, "<!doctype html><title>Not found</title><h1>Not found</h1>", 404);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function localLanIp() {
  for (const entries of Object.values(os.networkInterfaces())) {
    for (const entry of entries || []) {
      if (entry.family === "IPv4" && !entry.internal) return entry.address;
    }
  }
  return "127.0.0.1";
}

function createApp(config) {
  const state = {
    startedAt: Date.now(),
    localBaseUrl: `http://localhost:${config.port}`,
    lanBaseUrl: `http://${localLanIp()}:${config.port}`,
    pi: {
      conn: null,
      connected: false,
      name: null,
      remoteAddress: null,
      connectedAt: null,
      lastSeen: null,
      camera: false,
      streaming: false,
      width: null,
      height: null,
      fps: 0,
      frames: 0,
      message: "",
      lastError: "",
    },
    latestFrame: null,
    frameSeq: 0,
    fpsWindow: {
      startedAt: Date.now(),
      frames: 0,
    },
    sessions: new Map(),
    activeToken: null,
    viewers: new Map(),
    dashboardPreviews: new Map(),
    events: new Set(),
    ngrok: {
      enabled: !config.noNgrok,
      process: null,
      publicUrl: null,
      running: false,
      starting: false,
      lastError: "",
    },
    lastStatePush: 0,
  };

  function dashboardAuthorized(req, parsed) {
    const token = parsed.searchParams.get("admin");
    if (token && token === config.dashboardToken) return true;
    const cookies = parseCookies(req.headers.cookie || "");
    return cookies.cabcam_admin === config.dashboardToken;
  }

  function dashboardCookie() {
    return `cabcam_admin=${encodeURIComponent(config.dashboardToken)}; Path=/; HttpOnly; SameSite=Lax`;
  }

  function publicBaseUrl() {
    return state.ngrok.publicUrl || null;
  }

  function activeSession() {
    cleanupSessions();
    if (!state.activeToken) return null;
    return state.sessions.get(state.activeToken) || null;
  }

  function sessionPath(session) {
    return `/view/${session.token}`;
  }

  function publicState() {
    const session = activeSession();
    return {
      uptimeSeconds: Math.floor((Date.now() - state.startedAt) / 1000),
      localBaseUrl: state.localBaseUrl,
      lanBaseUrl: state.lanBaseUrl,
      publicBaseUrl: publicBaseUrl(),
      pi: {
        connected: state.pi.connected,
        name: state.pi.name,
        remoteAddress: state.pi.remoteAddress,
        connectedAt: state.pi.connectedAt,
        lastSeen: state.pi.lastSeen,
        camera: state.pi.camera,
        streaming: state.pi.streaming,
        width: state.pi.width,
        height: state.pi.height,
        fps: state.pi.fps,
        frames: state.pi.frames,
        message: state.pi.message,
        lastError: state.pi.lastError,
      },
      ngrok: {
        enabled: state.ngrok.enabled,
        running: state.ngrok.running,
        starting: state.ngrok.starting,
        publicUrl: state.ngrok.publicUrl,
        lastError: state.ngrok.lastError,
      },
      session: session ? {
        active: true,
        token: session.token,
        path: sessionPath(session),
        createdAt: session.createdAt,
        expiresAt: session.expiresAt,
        remainingSeconds: Math.max(0, Math.ceil((session.expiresAt - Date.now()) / 1000)),
      } : {
        active: false,
        token: null,
        path: null,
        createdAt: null,
        expiresAt: null,
        remainingSeconds: 0,
      },
      viewers: state.viewers.size,
      dashboardPreviews: state.dashboardPreviews.size,
      frameSeq: state.frameSeq,
    };
  }

  function pushState(force = false) {
    const now = Date.now();
    if (!force && now - state.lastStatePush < 300) return;
    state.lastStatePush = now;
    const line = `data: ${JSON.stringify(publicState())}\n\n`;
    for (const res of Array.from(state.events)) {
      try {
        res.write(line);
      } catch (_error) {
        state.events.delete(res);
      }
    }
  }

  function cleanupSessions() {
    const now = Date.now();
    for (const [token, session] of state.sessions) {
      if (session.expiresAt <= now) {
        state.sessions.delete(token);
        if (state.activeToken === token) state.activeToken = null;
      }
    }
    for (const [id, viewer] of Array.from(state.viewers)) {
      if (!state.sessions.has(viewer.token)) {
        viewer.conn.close(1000, "Session expired");
        state.viewers.delete(id);
      }
    }
  }

  function createSession() {
    const token = crypto.randomBytes(18).toString("base64url");
    const now = Date.now();
    const session = {
      token,
      createdAt: now,
      expiresAt: now + config.sessionMinutes * 60 * 1000,
    };

    for (const viewer of state.viewers.values()) {
      viewer.conn.close(1000, "Replaced by a new link");
    }
    state.viewers.clear();
    state.sessions.clear();
    state.sessions.set(token, session);
    state.activeToken = token;
    pushState(true);
    return session;
  }

  function endSession() {
    for (const viewer of state.viewers.values()) {
      viewer.conn.close(1000, "Session ended");
    }
    state.viewers.clear();
    state.sessions.clear();
    state.activeToken = null;
    pushState(true);
  }

  function sendPiCommand(action) {
    if (!state.pi.conn || !state.pi.connected) {
      return false;
    }
    state.pi.conn.sendText(JSON.stringify({
      type: "command",
      id: crypto.randomBytes(9).toString("base64url"),
      action,
      at: new Date().toISOString(),
    }));
    return true;
  }

  function handlePiText(text) {
    let message;
    try {
      message = JSON.parse(text);
    } catch (_error) {
      state.pi.message = text.slice(0, 200);
      pushState();
      return;
    }

    state.pi.lastSeen = new Date().toISOString();
    if (message.type === "hello") {
      state.pi.name = message.name || state.pi.name;
      state.pi.message = "Pi connected";
    } else if (message.type === "status") {
      state.pi.name = message.name || state.pi.name;
      state.pi.camera = Boolean(message.camera);
      state.pi.streaming = Boolean(message.streaming);
      state.pi.width = message.width || state.pi.width;
      state.pi.height = message.height || state.pi.height;
      state.pi.frames = Number(message.frames || state.pi.frames || 0);
      state.pi.message = message.message || "";
      if (message.error) state.pi.lastError = message.error;
    } else if (message.type === "error") {
      state.pi.lastError = message.error || message.message || "Unknown Pi error";
      state.pi.message = "Pi error";
    } else {
      state.pi.message = message.message || message.type || "";
    }
    pushState(true);
  }

  function receiveFrame(frame) {
    state.latestFrame = Buffer.from(frame);
    state.frameSeq += 1;
    state.pi.frames += 1;
    state.pi.connected = true;
    state.pi.lastSeen = new Date().toISOString();

    state.fpsWindow.frames += 1;
    const now = Date.now();
    const elapsed = now - state.fpsWindow.startedAt;
    if (elapsed >= 1000) {
      state.pi.fps = Math.round((state.fpsWindow.frames * 1000 / elapsed) * 10) / 10;
      state.fpsWindow.frames = 0;
      state.fpsWindow.startedAt = now;
      pushState();
    }

    cleanupSessions();
    for (const [id, viewer] of Array.from(state.viewers)) {
      if (!state.sessions.has(viewer.token)) {
        viewer.conn.close(1000, "Session expired");
        state.viewers.delete(id);
        continue;
      }
      viewer.conn.sendBinary(frame).catch(() => {
        state.viewers.delete(id);
      });
    }

    for (const [id, preview] of Array.from(state.dashboardPreviews)) {
      preview.sendBinary(frame).catch(() => {
        state.dashboardPreviews.delete(id);
      });
    }
  }

  function registerPi(req, peer) {
    if (state.pi.conn && state.pi.conn !== peer) {
      state.pi.conn.close(1000, "Replaced by a new Pi connection");
    }
    const parsed = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    state.pi.conn = peer;
    state.pi.connected = true;
    state.pi.name = parsed.searchParams.get("name") || state.pi.name || "raspberry-pi";
    state.pi.remoteAddress = req.socket.remoteAddress;
    state.pi.connectedAt = new Date().toISOString();
    state.pi.lastSeen = state.pi.connectedAt;
    state.pi.message = "Pi connected";
    state.pi.lastError = "";
    peer.sendText(JSON.stringify({ type: "hello", at: new Date().toISOString() }));
    pushState(true);

    peer.on("message", (payload, binary) => {
      if (binary) receiveFrame(payload);
      else handlePiText(payload);
    });
    peer.on("close", () => {
      if (state.pi.conn === peer) {
        state.pi.conn = null;
        state.pi.connected = false;
        state.pi.camera = false;
        state.pi.streaming = false;
        state.pi.message = "Pi disconnected";
        pushState(true);
      }
    });
  }

  function registerViewer(token, peer) {
    cleanupSessions();
    const session = state.sessions.get(token);
    if (!session) {
      peer.close(1008, "Invalid session");
      return;
    }

    const id = crypto.randomUUID();
    state.viewers.set(id, { token, conn: peer, connectedAt: Date.now() });
    if (state.latestFrame) {
      peer.sendBinary(state.latestFrame);
    }
    peer.on("close", () => {
      state.viewers.delete(id);
      pushState();
    });
    pushState(true);
  }

  function registerDashboardPreview(peer) {
    const id = crypto.randomUUID();
    state.dashboardPreviews.set(id, peer);
    if (state.latestFrame) {
      peer.sendBinary(state.latestFrame);
    }
    peer.on("close", () => {
      state.dashboardPreviews.delete(id);
      pushState();
    });
    pushState(true);
  }

  function handleEvents(req, res) {
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.write(`data: ${JSON.stringify(publicState())}\n\n`);
    state.events.add(res);
    req.on("close", () => {
      state.events.delete(res);
    });
  }

  async function handleHttp(req, res) {
    const parsed = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    const authorized = dashboardAuthorized(req, parsed);
    const authHeaders = parsed.searchParams.get("admin") === config.dashboardToken
      ? { "Set-Cookie": dashboardCookie() }
      : {};

    if (req.method === "GET" && (parsed.pathname === "/" || parsed.pathname === "/dashboard")) {
      if (!authorized) {
        sendHtml(res, lockedPage(), 401);
        return;
      }
      sendHtml(res, dashboardPage(), 200, authHeaders);
      return;
    }

    if (req.method === "GET" && parsed.pathname === "/events") {
      if (!authorized) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
      }
      handleEvents(req, res);
      return;
    }

    if (req.method === "GET" && parsed.pathname === "/status.json") {
      if (!authorized) {
        sendJson(res, 401, { error: "Unauthorized" });
        return;
      }
      sendJson(res, 200, publicState());
      return;
    }

    if (req.method === "GET" && parsed.pathname === "/healthz") {
      res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("ok\n");
      return;
    }

    if (req.method === "POST" && parsed.pathname === "/api/pi/start") {
      if (!authorized) return sendJson(res, 401, { error: "Unauthorized" });
      if (!sendPiCommand("start_camera")) return sendJson(res, 409, { error: "Pi is not connected" });
      return sendJson(res, 200, { ok: true });
    }

    if (req.method === "POST" && parsed.pathname === "/api/pi/stop") {
      if (!authorized) return sendJson(res, 401, { error: "Unauthorized" });
      if (!sendPiCommand("stop_camera")) return sendJson(res, 409, { error: "Pi is not connected" });
      return sendJson(res, 200, { ok: true });
    }

    if (req.method === "POST" && parsed.pathname === "/api/pi/restart") {
      if (!authorized) return sendJson(res, 401, { error: "Unauthorized" });
      if (!sendPiCommand("restart_camera")) return sendJson(res, 409, { error: "Pi is not connected" });
      return sendJson(res, 200, { ok: true });
    }

    if (req.method === "POST" && parsed.pathname === "/api/sessions") {
      if (!authorized) return sendJson(res, 401, { error: "Unauthorized" });
      const session = createSession();
      return sendJson(res, 200, {
        ok: true,
        path: sessionPath(session),
        publicBaseUrl: publicBaseUrl(),
        expiresAt: session.expiresAt,
      });
    }

    if (req.method === "POST" && parsed.pathname === "/api/sessions/end") {
      if (!authorized) return sendJson(res, 401, { error: "Unauthorized" });
      endSession();
      return sendJson(res, 200, { ok: true });
    }

    if (req.method === "POST" && parsed.pathname === "/api/ngrok/restart") {
      if (!authorized) return sendJson(res, 401, { error: "Unauthorized" });
      if (config.noNgrok) return sendJson(res, 409, { error: "ngrok is disabled" });
      try {
        await restartNgrok();
        return sendJson(res, 200, { ok: true, publicUrl: state.ngrok.publicUrl });
      } catch (error) {
        return sendJson(res, 500, { error: error.message });
      }
    }

    if (req.method === "GET" && parsed.pathname.startsWith("/view/")) {
      const token = parsed.pathname.split("/").filter(Boolean)[1] || "";
      cleanupSessions();
      const session = state.sessions.get(token);
      if (!session) {
        sendHtml(res, expiredPage(), 404);
        return;
      }
      sendHtml(res, viewerPage(token, session), 200);
      return;
    }

    if (req.method === "GET" && parsed.pathname.startsWith("/snapshot/")) {
      const token = parsed.pathname.split("/").filter(Boolean)[1] || "";
      cleanupSessions();
      if (!state.sessions.has(token)) {
        sendJson(res, 404, { error: "Session expired" });
        return;
      }
      if (!state.latestFrame) {
        sendJson(res, 503, { error: "No frame from Pi yet" });
        return;
      }
      res.writeHead(200, {
        "Content-Type": "image/jpeg",
        "Content-Length": state.latestFrame.length,
        "Cache-Control": "no-store",
      });
      res.end(state.latestFrame);
      return;
    }

    notFound(res);
  }

  function handleUpgrade(req, socket) {
    const parsed = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (parsed.pathname === "/ws/pi") {
      const key = parsed.searchParams.get("key") || req.headers["x-pi-key"] || "";
      if (key !== config.piKey) {
        rejectUpgrade(socket, 401, "Unauthorized");
        return;
      }
      const peer = acceptWebSocket(req, socket);
      if (peer) registerPi(req, peer);
      return;
    }

    if (parsed.pathname.startsWith("/ws/view/")) {
      const token = parsed.pathname.split("/").filter(Boolean)[2] || "";
      cleanupSessions();
      if (!state.sessions.has(token)) {
        rejectUpgrade(socket, 404, "Session expired");
        return;
      }
      const peer = acceptWebSocket(req, socket);
      if (peer) registerViewer(token, peer);
      return;
    }

    if (parsed.pathname === "/ws/dashboard-preview") {
      if (!dashboardAuthorized(req, parsed)) {
        rejectUpgrade(socket, 401, "Unauthorized");
        return;
      }
      const peer = acceptWebSocket(req, socket);
      if (peer) registerDashboardPreview(peer);
      return;
    }

    rejectUpgrade(socket, 404, "Not Found");
  }

  function localNgrokTarget() {
    const targetHost = config.host === "0.0.0.0" || config.host === "::" || config.host === ""
      ? "127.0.0.1"
      : config.host;
    return `http://${targetHost}:${config.port}`;
  }

  function tunnelMatchesPort(tunnel) {
    const addr = String((tunnel.config && tunnel.config.addr) || "");
    return addr === String(config.port)
      || addr.endsWith(`:${config.port}`)
      || addr.includes(`:${config.port}/`);
  }

  function getJson(url) {
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

  async function waitForNgrokUrl() {
    const deadline = Date.now() + config.ngrokTimeout;
    const tunnelsUrl = `${config.ngrokApiUrl.replace(/\/$/, "")}/api/tunnels`;
    let lastError = "ngrok API was not ready";

    while (Date.now() < deadline) {
      try {
        const payload = await getJson(tunnelsUrl);
        const publicUrls = [];
        for (const tunnel of payload.tunnels || []) {
          if (tunnel.public_url && tunnelMatchesPort(tunnel)) {
            publicUrls.push(String(tunnel.public_url).replace(/\/$/, ""));
          }
        }
        const httpsUrl = publicUrls.find((url) => url.startsWith("https://"));
        if (httpsUrl) return httpsUrl;
        if (publicUrls.length > 0) return publicUrls[0];
        lastError = `ngrok API returned no tunnel for local port ${config.port}`;
      } catch (error) {
        lastError = error.message;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(`Timed out waiting for ngrok public URL. Last error: ${lastError}`);
  }

  async function startNgrok() {
    if (config.noNgrok) return null;
    if (state.ngrok.process) return state.ngrok.publicUrl;

    state.ngrok.starting = true;
    state.ngrok.running = false;
    state.ngrok.lastError = "";
    pushState(true);

    const args = ["http", localNgrokTarget(), "--log=stdout"];
    if (config.ngrokUrl) {
      args.push("--url", config.ngrokUrl);
    }

    const proc = childProcess.spawn(config.ngrokBin, args, {
      stdio: ["ignore", "ignore", "pipe"],
    });

    state.ngrok.process = proc;
    proc.stderr.on("data", (chunk) => {
      const line = chunk.toString("utf8").trim();
      if (line) state.ngrok.lastError = line.slice(0, 500);
    });
    proc.on("error", (error) => {
      state.ngrok.lastError = error.message;
      state.ngrok.process = null;
      state.ngrok.running = false;
      state.ngrok.starting = false;
      pushState(true);
    });
    proc.on("exit", (code, signal) => {
      if (state.ngrok.process === proc) {
        state.ngrok.process = null;
        state.ngrok.running = false;
        state.ngrok.starting = false;
        if (code !== 0 && signal !== "SIGTERM") {
          state.ngrok.lastError = `ngrok exited with code ${code}`;
        }
        pushState(true);
      }
    });

    try {
      const publicUrl = await waitForNgrokUrl();
      state.ngrok.publicUrl = publicUrl;
      state.ngrok.running = true;
      state.ngrok.starting = false;
      pushState(true);
      console.log(`ngrok: ${publicUrl} -> ${localNgrokTarget()}`);
      return publicUrl;
    } catch (error) {
      stopNgrok();
      state.ngrok.lastError = error.message;
      state.ngrok.starting = false;
      pushState(true);
      throw error;
    }
  }

  function stopNgrok() {
    if (!state.ngrok.process) return;
    const proc = state.ngrok.process;
    state.ngrok.process = null;
    state.ngrok.running = false;
    state.ngrok.starting = false;
    state.ngrok.publicUrl = null;
    proc.kill();
    pushState(true);
  }

  async function restartNgrok() {
    stopNgrok();
    await new Promise((resolve) => setTimeout(resolve, 500));
    return startNgrok();
  }

  function lockedPage() {
    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cab Cam</title>
  <style>
    :root { font-family: Arial, Helvetica, sans-serif; background: #f6f7f4; color: #1d1f20; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 24px; }
    main { width: min(100%, 420px); border: 1px solid #c9cdc6; border-radius: 8px; padding: 20px; background: #fff; }
    code { overflow-wrap: anywhere; }
  </style>
</head>
<body>
  <main>
    <h1>Dashboard locked</h1>
    <p>Open the dashboard URL printed by the Node server.</p>
  </main>
</body>
</html>`;
  }

  function dashboardPage() {
    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cab Cam Control</title>
  <style>
    :root {
      color-scheme: light;
      font-family: Arial, Helvetica, sans-serif;
      background: #f4f6f2;
      color: #202124;
      --panel: #ffffff;
      --border: #c7cdc2;
      --ink-muted: #59615a;
      --accent: #0f766e;
      --accent-dark: #0b5f59;
      --danger: #b42318;
      --warn: #a16207;
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; }
    header {
      min-height: 58px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 12px 18px;
      background: #252821;
      color: #fff;
    }
    h1 { margin: 0; font-size: 20px; line-height: 1.2; }
    main { width: min(100%, 1180px); margin: 0 auto; padding: 18px; display: grid; gap: 14px; }
    .badge {
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      border-radius: 999px;
      padding: 0 12px;
      background: #3a3e34;
      color: #eef4e8;
      font-size: 13px;
      font-weight: 700;
    }
    .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .panel, .metric {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
    }
    .metric { padding: 12px; min-height: 92px; }
    .metric span { display: block; color: var(--ink-muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0; }
    .metric strong { display: block; margin-top: 8px; font-size: 24px; line-height: 1.1; overflow-wrap: anywhere; }
    .metric small { display: block; margin-top: 6px; color: var(--ink-muted); line-height: 1.35; overflow-wrap: anywhere; }
    .panel { padding: 12px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    button {
      min-height: 38px;
      border: 1px solid transparent;
      border-radius: 6px;
      padding: 0 12px;
      background: var(--accent);
      color: white;
      font-weight: 700;
      cursor: pointer;
    }
    button:hover { background: var(--accent-dark); }
    button.secondary { background: #fff; border-color: var(--border); color: #202124; }
    button.secondary:hover { background: #edf1ea; }
    button.danger { background: var(--danger); }
    button.warn { background: var(--warn); }
    button:disabled { opacity: .45; cursor: not-allowed; }
    .share { display: grid; grid-template-columns: 1fr auto auto; gap: 8px; }
    input {
      min-width: 0;
      min-height: 38px;
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 0 10px;
      font: inherit;
      background: #fff;
      color: #202124;
    }
    canvas {
      display: block;
      width: 100%;
      max-height: 58vh;
      aspect-ratio: 16 / 9;
      background: #050505;
      border: 1px solid var(--border);
      border-radius: 8px;
      object-fit: contain;
    }
    .split { display: grid; grid-template-columns: 2fr 1fr; gap: 14px; align-items: start; }
    dl { display: grid; grid-template-columns: 110px 1fr; gap: 8px 10px; margin: 0; }
    dt { color: var(--ink-muted); }
    dd { margin: 0; overflow-wrap: anywhere; }
    .status-line { min-height: 22px; color: var(--ink-muted); }
    @media (max-width: 860px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .split { grid-template-columns: 1fr; }
      .share { grid-template-columns: 1fr; }
      header { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Cab Cam Control</h1>
    <span class="badge" id="topStatus">Waiting for Pi</span>
  </header>
  <main>
    <section class="grid" aria-label="Status">
      <div class="metric"><span>Pi</span><strong id="piStatus">offline</strong><small id="piName">-</small></div>
      <div class="metric"><span>Camera</span><strong id="cameraStatus">stopped</strong><small id="frameRate">0 fps</small></div>
      <div class="metric"><span>Ngrok</span><strong id="ngrokStatus">starting</strong><small id="publicUrl">-</small></div>
      <div class="metric"><span>Viewers</span><strong id="viewerCount">0</strong><small id="expiry">no active link</small></div>
    </section>

    <section class="panel">
      <div class="toolbar">
        <button id="startBtn" type="button">Start Camera</button>
        <button class="secondary" id="stopBtn" type="button">Stop Camera</button>
        <button class="secondary" id="restartBtn" type="button">Restart Camera</button>
        <button id="newLinkBtn" type="button">New Public Link</button>
        <button class="danger" id="endLinkBtn" type="button">End Link</button>
        <button class="warn" id="restartNgrokBtn" type="button">New Ngrok Link</button>
      </div>
      <p class="status-line" id="actionStatus"></p>
    </section>

    <section class="panel share">
      <input id="shareLink" readonly value="">
      <button class="secondary" id="copyBtn" type="button">Copy</button>
      <button class="secondary" id="openBtn" type="button">Open</button>
    </section>

    <section class="split">
      <canvas id="preview" width="960" height="540"></canvas>
      <div class="panel">
        <dl>
          <dt>LAN</dt><dd id="lanUrl">-</dd>
          <dt>Public</dt><dd id="publicBase">-</dd>
          <dt>Last frame</dt><dd id="lastSeen">-</dd>
          <dt>Pi address</dt><dd id="piAddress">-</dd>
          <dt>Message</dt><dd id="message">-</dd>
          <dt>Error</dt><dd id="error">-</dd>
        </dl>
      </div>
    </section>
  </main>
  <script>
    const topStatus = document.getElementById("topStatus");
    const piStatus = document.getElementById("piStatus");
    const piName = document.getElementById("piName");
    const cameraStatus = document.getElementById("cameraStatus");
    const frameRate = document.getElementById("frameRate");
    const ngrokStatus = document.getElementById("ngrokStatus");
    const publicUrl = document.getElementById("publicUrl");
    const viewerCount = document.getElementById("viewerCount");
    const expiry = document.getElementById("expiry");
    const shareLink = document.getElementById("shareLink");
    const actionStatus = document.getElementById("actionStatus");
    const lanUrl = document.getElementById("lanUrl");
    const publicBase = document.getElementById("publicBase");
    const lastSeen = document.getElementById("lastSeen");
    const piAddress = document.getElementById("piAddress");
    const message = document.getElementById("message");
    const error = document.getElementById("error");
    const restartNgrokBtn = document.getElementById("restartNgrokBtn");
    const canvas = document.getElementById("preview");
    const ctx = canvas.getContext("2d", { alpha: false });
    let latestFrame = null;
    let drawing = false;
    let lastState = null;

    function fullLink(state) {
      if (!state.session.active || !state.session.path) return "";
      const base = state.publicBaseUrl || location.origin;
      return base.replace(/\\/$/, "") + state.session.path;
    }

    function render(state) {
      lastState = state;
      topStatus.textContent = state.pi.connected ? "Pi connected" : "Waiting for Pi";
      piStatus.textContent = state.pi.connected ? "online" : "offline";
      piName.textContent = state.pi.name || "-";
      cameraStatus.textContent = state.pi.camera ? "running" : "stopped";
      frameRate.textContent = (state.pi.fps || 0) + " fps";
      ngrokStatus.textContent = state.ngrok.enabled ? (state.ngrok.running ? "online" : (state.ngrok.starting ? "starting" : "offline")) : "disabled";
      publicUrl.textContent = state.ngrok.publicUrl || "-";
      viewerCount.textContent = String(state.viewers || 0);
      const remaining = state.session.remainingSeconds || 0;
      expiry.textContent = state.session.active ? Math.ceil(remaining / 60) + " min remaining" : "no active link";
      shareLink.value = fullLink(state);
      lanUrl.textContent = state.lanBaseUrl || "-";
      publicBase.textContent = state.publicBaseUrl || "-";
      lastSeen.textContent = state.pi.lastSeen || "-";
      piAddress.textContent = state.pi.remoteAddress || "-";
      message.textContent = state.pi.message || "-";
      error.textContent = state.pi.lastError || state.ngrok.lastError || "-";
      restartNgrokBtn.disabled = !state.ngrok.enabled || state.ngrok.starting;
      document.getElementById("endLinkBtn").disabled = !state.session.active;
      document.getElementById("copyBtn").disabled = !shareLink.value;
      document.getElementById("openBtn").disabled = !shareLink.value;
    }

    async function post(path) {
      actionStatus.textContent = "working";
      const response = await fetch(path, { method: "POST" });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.error || "Request failed");
      actionStatus.textContent = "done";
      setTimeout(() => { if (actionStatus.textContent === "done") actionStatus.textContent = ""; }, 1200);
      return body;
    }

    document.getElementById("startBtn").onclick = () => post("/api/pi/start").catch(err => actionStatus.textContent = err.message);
    document.getElementById("stopBtn").onclick = () => post("/api/pi/stop").catch(err => actionStatus.textContent = err.message);
    document.getElementById("restartBtn").onclick = () => post("/api/pi/restart").catch(err => actionStatus.textContent = err.message);
    document.getElementById("newLinkBtn").onclick = () => post("/api/sessions").catch(err => actionStatus.textContent = err.message);
    document.getElementById("endLinkBtn").onclick = () => post("/api/sessions/end").catch(err => actionStatus.textContent = err.message);
    restartNgrokBtn.onclick = () => post("/api/ngrok/restart").catch(err => actionStatus.textContent = err.message);
    document.getElementById("copyBtn").onclick = async () => {
      if (!shareLink.value) return;
      await navigator.clipboard.writeText(shareLink.value);
      actionStatus.textContent = "copied";
    };
    document.getElementById("openBtn").onclick = () => {
      if (shareLink.value) window.open(shareLink.value, "_blank", "noopener");
    };

    const events = new EventSource("/events");
    events.onmessage = event => render(JSON.parse(event.data));
    events.onerror = async () => {
      try {
        const response = await fetch("/status.json");
        if (response.ok) render(await response.json());
      } catch (_error) {}
    };

    function previewWsUrl() {
      const scheme = location.protocol === "https:" ? "wss:" : "ws:";
      return scheme + "//" + location.host + "/ws/dashboard-preview";
    }

    function connectPreview() {
      const ws = new WebSocket(previewWsUrl());
      ws.binaryType = "arraybuffer";
      ws.onmessage = event => {
        latestFrame = event.data;
        if (!drawing) requestAnimationFrame(draw);
      };
      ws.onclose = () => setTimeout(connectPreview, 1000);
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
      const data = latestFrame;
      latestFrame = null;
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
      if (latestFrame) requestAnimationFrame(draw);
      else drawing = false;
    }

    connectPreview();
    fetch("/status.json").then(response => response.json()).then(render).catch(() => {});
    setInterval(() => {
      if (lastState) render(lastState);
    }, 1000);
  </script>
</body>
</html>`;
  }

  function viewerPage(token, session) {
    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cab Cam Live</title>
  <style>
    :root { font-family: Arial, Helvetica, sans-serif; background: #0b0d0c; color: #f7faf6; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; display: grid; grid-template-rows: auto 1fr auto; }
    header, footer { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; padding: 10px 14px; background: #1d211b; }
    strong { font-size: 16px; }
    span { color: #c6d3c0; }
    a, button { color: #fff; }
    button { min-height: 34px; border: 0; border-radius: 6px; padding: 0 10px; background: #0f766e; font-weight: 700; cursor: pointer; }
    canvas { width: 100%; height: calc(100vh - 102px); background: #000; object-fit: contain; }
  </style>
</head>
<body>
  <header>
    <strong>Cab Cam Live</strong>
    <span id="status">connecting</span>
    <span id="countdown">--:--</span>
    <a href="/snapshot/${escapeHtml(token)}">Snapshot</a>
    <button type="button" id="copyBtn">Copy Link</button>
  </header>
  <canvas id="video" width="960" height="540"></canvas>
  <footer><span>${escapeHtml(new Date(session.expiresAt).toLocaleString())}</span></footer>
  <script>
    const token = ${JSON.stringify(token)};
    const expiresAt = ${session.expiresAt};
    const canvas = document.getElementById("video");
    const ctx = canvas.getContext("2d", { alpha: false });
    const statusEl = document.getElementById("status");
    const countdownEl = document.getElementById("countdown");
    let latest = null;
    let drawing = false;
    let frames = 0;
    let lastFps = performance.now();

    document.getElementById("copyBtn").onclick = () => navigator.clipboard.writeText(location.href);

    function wsUrl() {
      const scheme = location.protocol === "https:" ? "wss:" : "ws:";
      return scheme + "//" + location.host + "/ws/view/" + token;
    }
    function tick() {
      const seconds = Math.max(0, Math.floor((expiresAt - Date.now()) / 1000));
      const minutes = Math.floor(seconds / 60).toString().padStart(2, "0");
      const rest = (seconds % 60).toString().padStart(2, "0");
      countdownEl.textContent = "expires " + minutes + ":" + rest;
      if (seconds <= 0) location.reload();
    }
    function connect() {
      statusEl.textContent = "connecting";
      const ws = new WebSocket(wsUrl());
      ws.binaryType = "arraybuffer";
      ws.onopen = () => statusEl.textContent = "live";
      ws.onmessage = event => {
        latest = event.data;
        if (!drawing) requestAnimationFrame(draw);
      };
      ws.onclose = () => {
        statusEl.textContent = "reconnecting";
        setTimeout(connect, 1000);
      };
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
  </script>
</body>
</html>`;
  }

  function expiredPage() {
    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Cab Cam Link Expired</title>
  <style>
    :root { font-family: Arial, Helvetica, sans-serif; background: #f4f6f2; color: #202124; }
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 24px; }
    main { width: min(100%, 440px); border: 1px solid #c7cdc2; border-radius: 8px; background: white; padding: 20px; }
  </style>
</head>
<body><main><h1>Link expired</h1><p>Ask the cab operator for a fresh stream link.</p></main></body>
</html>`;
  }

  return {
    state,
    handleHttp,
    handleUpgrade,
    startNgrok,
    stopNgrok,
    createSession,
  };
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

  process.on("SIGINT", () => {
    console.log("\nStopping Cab Cam server...");
    app.stopNgrok();
    server.close(() => process.exit(0));
  });
  process.on("SIGTERM", () => {
    app.stopNgrok();
    server.close(() => process.exit(0));
  });

  server.listen(config.port, config.host, async () => {
    const localDashboard = `${app.state.localBaseUrl}/?admin=${encodeURIComponent(config.dashboardToken)}`;
    const lanDashboard = `${app.state.lanBaseUrl}/?admin=${encodeURIComponent(config.dashboardToken)}`;
    const piServerUrl = `ws://${localLanIp()}:${config.port}/ws/pi`;

    console.log(`Cab Cam Node relay listening on ${config.host}:${config.port}`);
    console.log(`Dashboard local: ${localDashboard}`);
    console.log(`Dashboard LAN:   ${lanDashboard}`);
    console.log(`Pi command:      python3 qr_session_cam_server.py --server ${piServerUrl} --key ${config.piKey}`);
    console.log(`Pi shared key:   ${config.piKey}`);

    if (!config.noNgrok) {
      try {
        await app.startNgrok();
      } catch (error) {
        console.error(`ngrok failed: ${error.message}`);
        console.error("Run with --no-ngrok for LAN-only testing, or check ngrok auth/config.");
      }
    } else {
      console.log("ngrok disabled; public links use the dashboard browser origin.");
    }
  });
}

if (require.main === module) {
  main();
}

module.exports = { createApp, parseArgs, WebSocketPeer };
