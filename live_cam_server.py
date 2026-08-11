#!/usr/bin/env python3
"""
Live camera web server for Raspberry Pi Camera Module.

Install on Raspberry Pi OS:
    sudo apt update
    sudo apt install -y python3-picamera2

Run:
    python3 live_cam_server.py

Then open the printed URL from another device on the same network.
Direct stream URL:
    http://<raspberry-pi-ip>:8000/stream.mjpg
"""

from __future__ import annotations

import argparse
import html
import io
import logging
import socket
import sys
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional


BOUNDARY = b"FRAME"


class StreamingOutput(io.BufferedIOBase):
    """Thread-safe sink used by Picamera2's JPEG encoder."""

    def __init__(self) -> None:
        self.frame: Optional[bytes] = None
        self.condition = threading.Condition()

    def writable(self) -> bool:
        return True

    def write(self, buf: bytes) -> int:
        with self.condition:
            self.frame = bytes(buf)
            self.condition.notify_all()
        return len(buf)

    def get_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        with self.condition:
            if self.frame is None:
                self.condition.wait(timeout=timeout)
            return self.frame


class CameraServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        output: StreamingOutput,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.output = output


class StreamingHandler(BaseHTTPRequestHandler):
    server: CameraServer

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        path = self.path.split("?", 1)[0]

        if path == "/":
            self._serve_index()
            return

        if path == "/stream.mjpg":
            self._serve_stream()
            return

        if path == "/snapshot.jpg":
            self._serve_snapshot()
            return

        if path == "/healthz":
            self._send_bytes(b"ok\n", "text/plain; charset=utf-8")
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def _serve_index(self) -> None:
        host = self.headers.get("Host", f"{self.server.server_address[0]}:{self.server.server_address[1]}")
        stream_url = f"http://{host}/stream.mjpg"
        snapshot_url = f"http://{host}/snapshot.jpg"
        body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Raspberry Pi Live Camera</title>
  <style>
    :root {{
      color-scheme: light dark;
      font-family: Arial, Helvetica, sans-serif;
      background: #101214;
      color: #f4f6f8;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      grid-template-rows: auto 1fr auto;
    }}
    header, footer {{
      padding: 14px 18px;
      background: #181c20;
    }}
    h1 {{
      margin: 0;
      font-size: 20px;
      font-weight: 700;
    }}
    main {{
      display: grid;
      place-items: center;
      padding: 16px;
    }}
    img {{
      width: min(100%, 1200px);
      max-height: 78vh;
      object-fit: contain;
      background: #050607;
      border: 1px solid #2d3339;
    }}
    a {{
      color: #80c7ff;
      overflow-wrap: anywhere;
    }}
    .links {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 18px;
      margin-top: 6px;
      font-size: 14px;
    }}
  </style>
</head>
<body>
  <header>
    <h1>Raspberry Pi Live Camera</h1>
    <div class="links">
      <a href="{html.escape(stream_url)}">Direct stream</a>
      <a href="{html.escape(snapshot_url)}">Snapshot</a>
    </div>
  </header>
  <main>
    <img src="/stream.mjpg" alt="Live camera stream">
  </main>
  <footer>
    Stream URL: <a href="{html.escape(stream_url)}">{html.escape(stream_url)}</a>
  </footer>
</body>
</html>
"""
        self._send_bytes(body.encode("utf-8"), "text/html; charset=utf-8")

    def _serve_stream(self) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Age", "0")
        self.send_header("Cache-Control", "no-cache, private")
        self.send_header("Pragma", "no-cache")
        self.send_header("Content-Type", f"multipart/x-mixed-replace; boundary={BOUNDARY.decode()}")
        self.end_headers()

        try:
            while True:
                frame = self.server.output.get_frame(timeout=5.0)
                if frame is None:
                    continue

                self.wfile.write(b"--" + BOUNDARY + b"\r\n")
                self.wfile.write(b"Content-Type: image/jpeg\r\n")
                self.wfile.write(f"Content-Length: {len(frame)}\r\n\r\n".encode("ascii"))
                self.wfile.write(frame)
                self.wfile.write(b"\r\n")
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            logging.info("Stream client disconnected: %s", self.client_address[0])

    def _serve_snapshot(self) -> None:
        frame = self.server.output.get_frame(timeout=5.0)
        if frame is None:
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE, "Camera frame not ready")
            return

        self._send_bytes(frame, "image/jpeg")

    def _send_bytes(self, body: bytes, content_type: str) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        logging.info("%s - %s", self.client_address[0], format % args)


def get_lan_ip() -> str:
    """Return the Pi's likely LAN IP without sending external traffic."""

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve a Raspberry Pi Camera live stream over HTTP.")
    parser.add_argument("--host", default="0.0.0.0", help="Address to bind to. Default: 0.0.0.0")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on. Default: 8000")
    parser.add_argument("--width", type=int, default=1280, help="Stream width in pixels. Default: 1280")
    parser.add_argument("--height", type=int, default=720, help="Stream height in pixels. Default: 720")
    parser.add_argument("--fps", type=float, default=24.0, help="Camera frame rate. Default: 24")
    parser.add_argument("--quality", type=int, default=85, help="JPEG quality from 1 to 100. Default: 85")
    parser.add_argument("--hflip", action="store_true", help="Flip image horizontally.")
    parser.add_argument("--vflip", action="store_true", help="Flip image vertically.")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def start_camera(args: argparse.Namespace, output: StreamingOutput):
    try:
        from libcamera import Transform
        from picamera2 import Picamera2
        from picamera2.encoders import JpegEncoder
        from picamera2.outputs import FileOutput
    except ImportError as exc:
        raise SystemExit(
            "Missing camera package. Install it on Raspberry Pi OS with:\n"
            "  sudo apt update\n"
            "  sudo apt install -y python3-picamera2"
        ) from exc

    picam2 = Picamera2()
    transform = Transform(hflip=args.hflip, vflip=args.vflip)
    config = picam2.create_video_configuration(
        main={"size": (args.width, args.height)},
        controls={"FrameRate": args.fps},
        transform=transform,
    )
    picam2.configure(config)
    picam2.start_recording(JpegEncoder(q=args.quality), FileOutput(output))
    return picam2


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    output = StreamingOutput()
    camera = start_camera(args, output)
    server = CameraServer((args.host, args.port), StreamingHandler, output)
    lan_ip = get_lan_ip()

    print(f"Viewer page:   http://{lan_ip}:{args.port}/")
    print(f"Direct stream: http://{lan_ip}:{args.port}/stream.mjpg")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping camera server...")
    finally:
        server.server_close()
        camera.stop_recording()

    return 0


if __name__ == "__main__":
    sys.exit(main())
