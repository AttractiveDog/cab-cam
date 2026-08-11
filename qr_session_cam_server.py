#!/usr/bin/env python3
"""
QR-gated Raspberry Pi camera session server.

Install on Raspberry Pi OS:
    sudo apt update
    sudo apt install -y python3-opencv python3-qrcode

Configure ngrok once:
    ngrok config add-authtoken <your-token>

Run:
    python3 qr_session_cam_server.py

For the same QR across restarts, use a reserved ngrok domain:
    python3 qr_session_cam_server.py --ngrok-url https://your-domain.ngrok.app

The QR is constant for the running process: it points to the ngrok device landing page. A shareable stream
session URL is created when the QR landing page is opened. Each new browser scan creates a fresh 10-minute token and revokes the previous one. The same browser scanning again renews its active token. The stream page includes an End session button.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import io
import json
import logging
import secrets
import shutil
import socket
import subprocess
import struct
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
import urllib.error
import urllib.request
from urllib.parse import parse_qs, urlparse

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class FrameBuffer(io.BufferedIOBase):
    def __init__(self) -> None:
        self.frame: Optional[bytes] = None
        self.frame_id = 0
        self.condition = threading.Condition()

    def writable(self) -> bool:
        return True

    def write(self, data: bytes) -> int:
        with self.condition:
            self.frame = bytes(data)
            self.frame_id += 1
            self.condition.notify_all()
        return len(data)

    def wait(self, last_id: int, timeout: Optional[float] = None) -> tuple[int, Optional[bytes]]:
        with self.condition:
            if self.frame is None or self.frame_id == last_id:
                ok = self.condition.wait_for(
                    lambda: self.frame is not None and self.frame_id != last_id,
                    timeout=timeout,
                )
                if not ok:
                    return last_id, None
            return self.frame_id, self.frame

    def latest(self, timeout: Optional[float] = None) -> Optional[bytes]:
        with self.condition:
            if self.frame is None:
                self.condition.wait(timeout=timeout)
            return self.frame


class SessionState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.session_id: Optional[str] = None
        self.created_at: Optional[float] = None
        self.expires_at: Optional[float] = None

    def _clear_if_expired_locked(self) -> None:
        if self.session_id and self.expires_at is not None and self.expires_at <= time.time():
            self.session_id = None
            self.created_at = None
            self.expires_at = None

    def create_or_extend(self, presented_token: Optional[str], duration_seconds: float) -> tuple[str, float, str]:
        now = time.time()
        duration_seconds = max(30.0, duration_seconds)
        with self.lock:
            self._clear_if_expired_locked()
            if self.session_id and presented_token == self.session_id:
                self.expires_at = now + duration_seconds
                return self.session_id, self.expires_at, "extended"

            self.session_id = secrets.token_urlsafe(18)
            self.created_at = now
            self.expires_at = now + duration_seconds
            return self.session_id, self.expires_at, "created"

    def current(self) -> Optional[str]:
        with self.lock:
            self._clear_if_expired_locked()
            return self.session_id

    def valid(self, session_id: str) -> bool:
        with self.lock:
            self._clear_if_expired_locked()
            return self.session_id == session_id

    def expiry(self, session_id: str) -> Optional[float]:
        with self.lock:
            self._clear_if_expired_locked()
            if self.session_id == session_id:
                return self.expires_at
            return None

    def remaining(self, session_id: str) -> int:
        expiry = self.expiry(session_id)
        if expiry is None:
            return 0
        return max(0, int(expiry - time.time()))

    def end(self, session_id: str) -> bool:
        with self.lock:
            self._clear_if_expired_locked()
            if self.session_id != session_id:
                return False
            self.session_id = None
            self.created_at = None
            self.expires_at = None
            return True
class OpenCVCamera:
    def __init__(self, args: argparse.Namespace, output: FrameBuffer) -> None:
        try:
            import cv2
        except ImportError as exc:
            raise RuntimeError("Install OpenCV with: sudo apt install -y python3-opencv") from exc

        self.args = args
        self.output = output
        self.cv2 = cv2
        self.running = threading.Event()
        self.running.set()
        self.jpeg_params = [int(cv2.IMWRITE_JPEG_QUALITY), max(1, min(100, args.quality))]
        self.capture, first = self._open_capture()
        self._write_frame(first)
        self.thread = threading.Thread(target=self._loop, name="qr-session-camera", daemon=True)
        self.thread.start()

    def _apis(self) -> list[tuple[str, int]]:
        items = []
        if self.args.opencv_api in ("auto", "v4l2") and hasattr(self.cv2, "CAP_V4L2"):
            items.append(("v4l2", self.cv2.CAP_V4L2))
        if self.args.opencv_api in ("auto", "any"):
            items.append(("any", self.cv2.CAP_ANY))
        return items

    def _fourccs(self) -> list[Optional[str]]:
        if self.args.fourcc == "auto":
            return ["MJPG", "YUYV", "YUY2", None]
        if self.args.fourcc == "none":
            return [None]
        return [self.args.fourcc]

    def _open_capture(self):
        errors = []
        for api_name, api_id in self._apis():
            for fourcc in self._fourccs():
                label = fourcc or "default"
                cap = self.cv2.VideoCapture(self.args.camera_index, api_id)
                if not cap.isOpened():
                    errors.append(f"{api_name}/{label}: open failed")
                    cap.release()
                    continue
                if hasattr(self.cv2, "CAP_PROP_BUFFERSIZE"):
                    cap.set(self.cv2.CAP_PROP_BUFFERSIZE, 1)
                if fourcc:
                    cap.set(self.cv2.CAP_PROP_FOURCC, self.cv2.VideoWriter_fourcc(*fourcc))
                cap.set(self.cv2.CAP_PROP_FRAME_WIDTH, self.args.width)
                cap.set(self.cv2.CAP_PROP_FRAME_HEIGHT, self.args.height)
                if self.args.fps > 0:
                    cap.set(self.cv2.CAP_PROP_FPS, self.args.fps)

                frame = self._first_frame(cap)
                if frame is not None:
                    logging.info(
                        "Camera opened: index=%s api=%s fourcc=%s size=%sx%s fps=%.1f",
                        self.args.camera_index,
                        api_name,
                        label,
                        int(cap.get(self.cv2.CAP_PROP_FRAME_WIDTH)),
                        int(cap.get(self.cv2.CAP_PROP_FRAME_HEIGHT)),
                        cap.get(self.cv2.CAP_PROP_FPS),
                    )
                    return cap, frame
                errors.append(f"{api_name}/{label}: no readable frame")
                cap.release()
        raise RuntimeError("Could not start camera. Tried: " + "; ".join(errors))

    def _first_frame(self, cap):
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            ok, frame = cap.read()
            if ok and frame is not None and getattr(frame, "size", 0) > 0:
                return frame
            time.sleep(0.05)
        return None

    def _write_frame(self, frame) -> bool:
        if self.args.hflip and self.args.vflip:
            frame = self.cv2.flip(frame, -1)
        elif self.args.hflip:
            frame = self.cv2.flip(frame, 1)
        elif self.args.vflip:
            frame = self.cv2.flip(frame, 0)
        ok, encoded = self.cv2.imencode(".jpg", frame, self.jpeg_params)
        if ok:
            self.output.write(encoded.tobytes())
        return bool(ok)

    def _loop(self) -> None:
        interval = 1.0 / self.args.stream_fps if self.args.stream_fps > 0 else 0.0
        next_encode = 0.0
        last_warning = 0.0
        while self.running.is_set():
            ok, frame = self.capture.read()
            if not ok or frame is None or getattr(frame, "size", 0) == 0:
                now = time.monotonic()
                if now - last_warning >= 2.0:
                    logging.warning("Camera frame read failed")
                    last_warning = now
                time.sleep(0.05)
                continue
            now = time.monotonic()
            if interval > 0 and now < next_encode:
                continue
            self._write_frame(frame)
            if interval > 0:
                next_encode = now + interval

    def stop(self) -> None:
        self.running.clear()
        self.thread.join(timeout=2.0)
        self.capture.release()


class AppServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, handler, args: argparse.Namespace, base_url: str) -> None:
        super().__init__(address, handler)
        self.args = args
        self.base_url = base_url.rstrip("/")
        self.sessions = SessionState()
        self.frames = FrameBuffer()
        self.camera_lock = threading.Lock()
        self.camera: Optional[OpenCVCamera] = None

    def start_camera_once(self) -> None:
        with self.camera_lock:
            if self.camera is None:
                self.camera = OpenCVCamera(self.args, self.frames)

    def stop_camera(self) -> None:
        with self.camera_lock:
            if self.camera is not None:
                self.camera.stop()
                self.camera = None


class Handler(BaseHTTPRequestHandler):
    server: AppServer
    session_cookie_name = "cabcam_session"

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/":
            self._scan_entry()
        elif path == "/qr.png":
            self._qr_png()
        elif path == "/status.json":
            self._status()
        elif path == "/expired":
            self._expired_page("Session expired or ended")
        elif path.startswith("/s/"):
            self._viewer(path.removeprefix("/s/").strip("/"))
        elif path.startswith("/ws/"):
            self._websocket(path.removeprefix("/ws/").strip("/"))
        elif path.startswith("/snapshot/"):
            self._snapshot(path.removeprefix("/snapshot/").strip("/"))
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/session":
            self._scan_entry()
        elif path == "/end":
            self._end_session()
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def _landing_url(self) -> str:
        return self.server.base_url + "/"

    def _session_url(self, session_id: str) -> str:
        return f"{self.server.base_url}/s/{session_id}"

    def _duration_seconds(self) -> float:
        return max(0.5, self.server.args.session_minutes) * 60.0

    def _cookies(self) -> dict[str, str]:
        cookies: dict[str, str] = {}
        for item in self.headers.get("Cookie", "").split(";"):
            if "=" not in item:
                continue
            name, value = item.strip().split("=", 1)
            cookies[name] = value
        return cookies

    def _session_cookie(self) -> Optional[str]:
        return self._cookies().get(self.session_cookie_name)

    def _cookie_header(self, token: str, max_age: int) -> str:
        parts = [
            f"{self.session_cookie_name}={token}",
            "Path=/",
            f"Max-Age={max_age}",
            "HttpOnly",
            "SameSite=Lax",
        ]
        if self.server.base_url.startswith("https://"):
            parts.append("Secure")
        return "; ".join(parts)

    def _clear_cookie_header(self) -> str:
        return self._cookie_header("", 0)

    def _scan_entry(self) -> None:
        presented_token = self._session_cookie()
        session_id, expires_at, action = self.server.sessions.create_or_extend(
            presented_token,
            self._duration_seconds(),
        )
        try:
            self.server.start_camera_once()
        except Exception as exc:
            self.server.sessions.end(session_id)
            logging.exception("Camera startup failed")
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return

        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", f"/s/{session_id}?scan={action}")
        self.send_header("Set-Cookie", self._cookie_header(session_id, int(expires_at - time.time())))
        self.end_headers()

    def _viewer(self, session_id: str) -> None:
        expires_at = self.server.sessions.expiry(session_id)
        if expires_at is None:
            self._expired_page("This stream link has expired or was replaced by a newer QR scan.")
            return

        session_url = self._session_url(session_id)
        ws_url = self.server.base_url.replace("https://", "wss://").replace("http://", "ws://") + f"/ws/{session_id}"
        snapshot = self.server.base_url + f"/snapshot/{session_id}"
        qr_url = self.server.base_url + "/qr.png"
        body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Live Stream</title>
  <style>
    :root {{ font-family: Arial, Helvetica, sans-serif; color-scheme: light dark; background: #07090b; color: #f4f6f8; }}
    body {{ margin: 0; min-height: 100vh; display: grid; grid-template-rows: auto 1fr auto; }}
    header, footer {{ display: flex; flex-wrap: wrap; align-items: center; gap: 10px 16px; padding: 10px 14px; background: #14191f; }}
    canvas {{ width: 100%; height: calc(100vh - 112px); object-fit: contain; background: #000; }}
    button, .button {{ min-height: 36px; padding: 0 12px; border: 0; background: #2e8cff; color: white; font-weight: 700; cursor: pointer; }}
    button.danger {{ background: #d94141; }}
    a {{ color: #80c7ff; }}
    #status, #countdown {{ color: #b8c7d4; }}
    details {{ width: 100%; }}
    img {{ width: 120px; height: 120px; background: white; padding: 6px; margin-top: 8px; }}
  </style>
</head>
<body>
  <header>
    <strong>Live Stream</strong>
    <span id="status">connecting</span>
    <span id="countdown">--:--</span>
    <a href="{html.escape(snapshot)}">Snapshot</a>
    <button type="button" onclick="navigator.clipboard.writeText({json.dumps(session_url)})">Copy share link</button>
    <form method="post" action="/end">
      <input type="hidden" name="token" value="{html.escape(session_id)}">
      <button class="danger" type="submit">End session</button>
    </form>
  </header>
  <canvas id="video" width="{self.server.args.width}" height="{self.server.args.height}"></canvas>
  <footer>
    <details>
      <summary>Extend access</summary>
      <p>Scan the cab QR again with this same browser before the timer ends. That renews this stream for another {self.server.args.session_minutes:g} minutes.</p>
      <img src="{html.escape(qr_url)}" alt="Permanent cab QR">
    </details>
  </footer>
  <script>
    const wsUrl = {json.dumps(ws_url)};
    const expiresAt = {int(expires_at * 1000)};
    const canvas = document.getElementById('video');
    const ctx = canvas.getContext('2d', {{alpha:false}});
    const statusEl = document.getElementById('status');
    const countdownEl = document.getElementById('countdown');
    let latest = null, drawing = false, frames = 0, lastFps = performance.now(), timer = null;
    function status(text) {{ statusEl.textContent = text; }}
    function tick() {{
      const remaining = Math.max(0, Math.floor((expiresAt - Date.now()) / 1000));
      const minutes = Math.floor(remaining / 60).toString().padStart(2, '0');
      const seconds = (remaining % 60).toString().padStart(2, '0');
      countdownEl.textContent = 'expires ' + minutes + ':' + seconds;
      if (remaining <= 0) location.replace('/expired');
    }}
    setInterval(tick, 1000);
    tick();
    function connect() {{
      status('connecting');
      const ws = new WebSocket(wsUrl);
      ws.binaryType = 'arraybuffer';
      ws.onopen = () => status('live');
      ws.onmessage = e => {{ latest = e.data; if (!drawing) requestAnimationFrame(draw); }};
      ws.onclose = () => {{ status('reconnecting'); clearTimeout(timer); timer = setTimeout(connect, 1000); }};
      ws.onerror = () => ws.close();
    }}
    function drawImageFallback(data) {{
      return new Promise(resolve => {{
        const url = URL.createObjectURL(new Blob([data], {{type:'image/jpeg'}}));
        const img = new Image();
        img.onload = () => {{ canvas.width = img.naturalWidth || canvas.width; canvas.height = img.naturalHeight || canvas.height; ctx.drawImage(img, 0, 0, canvas.width, canvas.height); URL.revokeObjectURL(url); resolve(); }};
        img.onerror = () => {{ URL.revokeObjectURL(url); resolve(); }};
        img.src = url;
      }});
    }}
    async function draw() {{
      const data = latest; latest = null;
      if (!data) {{ drawing = false; return; }}
      drawing = true;
      try {{
        if ('createImageBitmap' in window) {{
          const bitmap = await createImageBitmap(new Blob([data], {{type:'image/jpeg'}}));
          canvas.width = bitmap.width; canvas.height = bitmap.height; ctx.drawImage(bitmap, 0, 0); bitmap.close();
        }} else await drawImageFallback(data);
      }} catch (e) {{ await drawImageFallback(data); }}
      frames += 1;
      const now = performance.now();
      if (now - lastFps >= 1000) {{ status('live ' + frames + ' fps'); frames = 0; lastFps = now; }}
      if (latest) requestAnimationFrame(draw); else drawing = false;
    }}
    connect();
  </script>
</body>
</html>"""
        self._send(body.encode(), "text/html; charset=utf-8")

    def _expired_page(self, message: str) -> None:
        body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Session Expired</title>
  <style>
    :root {{ font-family: Arial, Helvetica, sans-serif; color-scheme: light dark; background: #101214; color: #f3f6f8; }}
    body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 20px; }}
    main {{ width: min(100%, 520px); }}
    a {{ color: #80c7ff; }}
    img {{ width: 180px; height: 180px; background: white; padding: 8px; }}
  </style>
</head>
<body>
  <main>
    <h1>Session unavailable</h1>
    <p>{html.escape(message)}</p>
    <p>Scan the cab QR again to create a new 10-minute stream session.</p>
    <img src="{html.escape(self.server.base_url + '/qr.png')}" alt="Permanent cab QR">
  </main>
</body>
</html>"""
        self.send_response(HTTPStatus.OK)
        self.send_header("Set-Cookie", self._clear_cookie_header())
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body.encode())))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body.encode())

    def _create_session(self) -> None:
        self._scan_entry()

    def _read_form(self) -> dict[str, str]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length > 0 else ""
        parsed = parse_qs(raw)
        return {key: values[0] for key, values in parsed.items() if values}

    def _end_session(self) -> None:
        form = self._read_form()
        token = form.get("token") or self._session_cookie() or ""
        ended = self.server.sessions.end(token)
        if ended:
            self.server.stop_camera()
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", "/expired")
        self.send_header("Set-Cookie", self._clear_cookie_header())
        self.end_headers()

    def _status(self) -> None:
        session_id = self.server.sessions.current()
        remaining = self.server.sessions.remaining(session_id) if session_id else 0
        body = json.dumps({
            "active": session_id is not None,
            "session_url": self._session_url(session_id) if session_id else None,
            "remaining_seconds": remaining,
            "qr_target": self._landing_url(),
            "qr_url": self.server.base_url + "/qr.png",
            "endable": True,
        }).encode()
        self._send(body, "application/json")

    def _snapshot(self, session_id: str) -> None:
        if not self.server.sessions.valid(session_id):
            self.send_error(HTTPStatus.NOT_FOUND, "Session expired")
            return
        frame = self.server.frames.latest(timeout=5.0)
        if frame is None:
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE, "Camera frame not ready")
            return
        self._send(frame, "image/jpeg")

    def _websocket(self, session_id: str) -> None:
        if not self.server.sessions.valid(session_id):
            self.send_error(HTTPStatus.NOT_FOUND, "Session expired")
            return
        key = self.headers.get("Sec-WebSocket-Key")
        if self.headers.get("Upgrade", "").lower() != "websocket" or not key:
            self.send_error(HTTPStatus.BAD_REQUEST, "Expected WebSocket upgrade")
            return
        accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
        self.send_response(HTTPStatus.SWITCHING_PROTOCOLS)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        self.close_connection = True
        self.connection.settimeout(8.0)
        last_id = 0
        interval = 1.0 / self.server.args.stream_fps if self.server.args.stream_fps > 0 else 0.0
        next_send = 0.0
        try:
            while self.server.sessions.valid(session_id):
                frame_id, frame = self.server.frames.wait(last_id, timeout=5.0)
                if frame is None:
                    continue
                if interval > 0:
                    delay = next_send - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                        latest_id, latest_frame = self.server.frames.wait(last_id, timeout=0)
                        if latest_frame is not None:
                            frame_id, frame = latest_id, latest_frame
                if not self.server.sessions.valid(session_id):
                    break
                self._ws_binary(frame)
                last_id = frame_id
                if interval > 0:
                    next_send = time.monotonic() + interval
        except (BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
            logging.info("WebSocket client disconnected: %s", self.client_address[0])

    def _ws_binary(self, payload: bytes) -> None:
        size = len(payload)
        if size <= 125:
            header = struct.pack("!BB", 0x82, size)
        elif size <= 65535:
            header = struct.pack("!BBH", 0x82, 126, size)
        else:
            header = struct.pack("!BBQ", 0x82, 127, size)
        self.connection.sendall(header)
        self.connection.sendall(payload)

    def _qr_png(self) -> None:
        try:
            import qrcode
        except ImportError:
            body = "Install QR support with: sudo apt install -y python3-qrcode\n\nQR target:\n" + self._landing_url() + "\n"
            self._send(body.encode(), "text/plain; charset=utf-8")
            return
        image = qrcode.make(self._landing_url())
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        self._send(buffer.getvalue(), "image/png")

    def _send(self, body: bytes, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        logging.info("%s - %s", self.client_address[0], fmt % args)


def lan_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"




class NgrokTunnel:
    def __init__(self, process: subprocess.Popen, public_url: str) -> None:
        self.process = process
        self.public_url = public_url.rstrip("/")

    def stop(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=2.0)


def local_ngrok_target(host: str, port: int) -> str:
    target_host = "127.0.0.1" if host in ("", "0.0.0.0", "::") else host
    return f"http://{target_host}:{port}"


def tunnel_matches_port(tunnel: dict, port: int) -> bool:
    addr = str(tunnel.get("config", {}).get("addr", ""))
    return addr == str(port) or addr.endswith(f":{port}") or f":{port}/" in addr


def wait_for_ngrok_url(api_url: str, port: int, timeout: float) -> str:
    tunnels_url = api_url.rstrip("/") + "/api/tunnels"
    deadline = time.monotonic() + timeout
    last_error = "ngrok API was not ready"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(tunnels_url, timeout=2.0) as response:
                payload = json.loads(response.read().decode("utf-8"))
            public_urls = []
            for tunnel in payload.get("tunnels", []):
                public_url = tunnel.get("public_url")
                if public_url and tunnel_matches_port(tunnel, port):
                    public_urls.append(public_url.rstrip("/"))
            https_urls = [url for url in public_urls if url.startswith("https://")]
            if https_urls:
                return https_urls[0]
            if public_urls:
                return public_urls[0]
            last_error = f"ngrok API returned no tunnel for local port {port}"
        except (OSError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for ngrok public URL. Last error: {last_error}")


def start_ngrok_tunnel(args: argparse.Namespace) -> NgrokTunnel:
    if shutil.which(args.ngrok_bin) is None:
        raise SystemExit(
            "ngrok CLI was not found. Install ngrok, then run:\n"
            "  ngrok config add-authtoken <your-token>"
        )

    target = local_ngrok_target(args.host, args.port)
    command = [args.ngrok_bin, "http", target, "--log=stdout"]
    if args.ngrok_url:
        command.extend(["--url", args.ngrok_url])

    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    tunnel = NgrokTunnel(process, "")
    try:
        public_url = wait_for_ngrok_url(args.ngrok_api_url, args.port, args.ngrok_timeout)
    except Exception as exc:
        tunnel.stop()
        raise SystemExit(
            f"ngrok started but no public URL was published: {exc}\n"
            "Check your ngrok authtoken and make sure no other ngrok process is using port 4040."
        ) from exc

    if process.poll() is not None:
        raise SystemExit(f"ngrok exited before the tunnel was ready. Run `ngrok http {args.port}` manually to see its error.")

    tunnel.public_url = public_url
    logging.info("ngrok tunnel started: %s -> %s", public_url, target)
    return tunnel
def default_base_url(port: int) -> str:
    hostname = socket.gethostname().split(".", 1)[0]
    if hostname:
        return f"http://{hostname}.local:{port}"
    return f"http://{lan_ip()}:{port}"
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QR-gated Raspberry Pi camera session server.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--public-url", default=None, help="Advanced: override the QR URL manually. Default: discovered ngrok URL.")
    parser.add_argument("--no-ngrok", action="store_true", help="Use LAN URL instead of starting ngrok. Mainly for local testing.")
    parser.add_argument("--ngrok-bin", default="ngrok", help="Path/name of the ngrok CLI. Default: ngrok")
    parser.add_argument("--ngrok-api-url", default="http://127.0.0.1:4040", help="Local ngrok agent API URL.")
    parser.add_argument("--ngrok-url", default=None, help="Reserved static ngrok URL/domain, for example https://example.ngrok.app.")
    parser.add_argument("--ngrok-timeout", type=float, default=20.0, help="Seconds to wait for ngrok public URL.")
    parser.add_argument("--camera-index", type=int, default=0)
    parser.add_argument("--opencv-api", default="v4l2", choices=("auto", "v4l2", "any"))
    parser.add_argument("--fourcc", default="auto", choices=("auto", "MJPG", "YUYV", "YUY2", "none"))
    parser.add_argument("--width", type=int, default=480)
    parser.add_argument("--height", type=int, default=360)
    parser.add_argument("--fps", type=float, default=20.0)
    parser.add_argument("--stream-fps", type=float, default=15.0)
    parser.add_argument("--quality", type=int, default=45)
    parser.add_argument("--session-minutes", type=float, default=10.0, help="Temporary stream session duration. Default: 10 minutes")
    parser.add_argument("--hflip", action="store_true")
    parser.add_argument("--vflip", action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def print_qr(url: str) -> None:
    try:
        import qrcode
    except ImportError:
        print("Install QR support with: sudo apt install -y python3-qrcode")
        return
    qr = qrcode.QRCode(border=1)
    qr.add_data(url)
    qr.make(fit=True)
    qr.print_ascii(invert=True)


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")

    server = AppServer((args.host, args.port), Handler, args, "http://127.0.0.1")
    ngrok_tunnel: Optional[NgrokTunnel] = None

    try:
        if args.public_url:
            base_url = args.public_url.rstrip("/")
        elif args.no_ngrok:
            base_url = default_base_url(args.port).rstrip("/")
        else:
            ngrok_tunnel = start_ngrok_tunnel(args)
            base_url = ngrok_tunnel.public_url.rstrip("/")

        server.base_url = base_url
        print(f"Constant QR target: {base_url}/")
        print(f"QR PNG:             {base_url}/qr.png")
        print(f"Status JSON:        {base_url}/status.json")
        if ngrok_tunnel:
            print("QR is fixed for this run. Use --ngrok-url with a reserved domain to keep the same QR across restarts.")
        elif args.no_ngrok:
            print("LAN mode enabled. QR uses the local network URL instead of ngrok.")
        else:
            print("QR URL was overridden with --public-url.")
        print("Scanning the QR creates a fresh 10-minute stream session; rescanning from the same browser renews it.")
        print("Press Ctrl+C to stop the server process.")
        print_qr(base_url + "/")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping QR session camera server...")
    finally:
        server.server_close()
        server.stop_camera()
        if ngrok_tunnel is not None:
            ngrok_tunnel.stop()
    return 0

if __name__ == "__main__":
    sys.exit(main())
