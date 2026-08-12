"use strict";

const http = require("http");
const { createApp } = require("../server");

const config = {
  host: "0.0.0.0",
  port: Number(process.env.PORT || 3000),
  publicBaseUrl: (process.env.PUBLIC_BASE_URL || "").replace(/\/$/, ""),
  noNgrok: true,
  ngrokBin: process.env.NGROK_BIN || "ngrok",
  ngrokTimeoutMs: Number(process.env.NGROK_TIMEOUT_MS || 20000),
  ngrokApiStartPort: Number(process.env.NGROK_API_START_PORT || 4040),
  sessionMinutes: Number(process.env.SESSION_MINUTES || 10),
  piKey: process.env.PI_SHARED_KEY || "change-me-pi-key",
  adminToken: process.env.DASHBOARD_TOKEN || "change-me-dashboard-token",
};

const app = createApp(config);
const server = http.createServer((req, res) => {
  app.handleHttp(req, res).catch((error) => {
    console.error(error);
    const payload = Buffer.from(JSON.stringify({ error: "Internal server error" }));
    res.writeHead(500, {
      "Content-Type": "application/json; charset=utf-8",
      "Content-Length": payload.length,
      "Cache-Control": "no-store",
    });
    res.end(payload);
  });
});

server.on("upgrade", app.handleUpgrade);

module.exports = server;
