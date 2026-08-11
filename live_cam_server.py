#!/usr/bin/env python3
"""
Live camera web server for a Raspberry Pi Camera Module or USB webcam.

Install on Raspberry Pi OS:
    sudo apt update
    sudo apt install -y python3-picamera2

For USB webcam fallback support:
    sudo apt install -y python3-opencv

Run:
    python3 live_cam_server.py

Force OpenCV for a USB webcam:
    python3 live_cam_server.py --backend opencv --width 640 --height 480 --fps 15

Smooth public stream with ngrok:
    ngrok config add-authtoken <your-token>
    python3 live_cam_server.py --usb-ngrok

Then open the printed URL from another device on the same network.
Direct stream URL:
    http://<raspberry-pi-ip>:8000/stream.mjpg
"""

from __future__ import annotations

import argparse
import json
import html
import io
import logging
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional


BOUNDARY = b"FRAME"


class StreamingOutput(io.BufferedIOBase):
    """Thread-safe latest-frame buffer used by camera backends."""

    def __init__(self) -> None:
        self.frame: Optional[bytes] = None
        self.frame_id = 0
        self.condition = threading.Condition()

    def writable(self) -> bool:
        return True

    def write(self, buf: bytes) -> int:
        with self.condition:
            self.frame = bytes(buf)
            self.frame_id += 1
            self.condition.notify_all()
        return len(buf)

    def get_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        with self.condition:
            if self.frame is None:
                self.condition.wait(timeout=timeout)
            return self.frame

    def wait_for_frame(self, last_seen_id: int, timeout: Optional[float] = None) -> tuple[int, Optional[bytes]]:
        with self.condition:
            if self.frame is None or self.frame_id == last_seen_id:
                ready = self.condition.wait_for(
                    lambda: self.frame is not None and self.frame_id != last_seen_id,
                    timeout=timeout,
                )
                if not ready:
                    return last_seen_id, None
            return self.frame_id, self.frame


class CameraServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        output: StreamingOutput,
        max_stream_fps: float,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.output = output
        self.max_stream_fps = max_stream_fps if max_stream_fps > 0 else 0.0

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
        scheme = self.headers.get("X-Forwarded-Proto", "http").split(",", 1)[0].strip() or "http"
        stream_url = f"{scheme}://{host}/stream.mjpg"
        snapshot_url = f"{scheme}://{host}/snapshot.jpg"
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

        last_frame_id = 0
        min_interval = 1.0 / self.server.max_stream_fps if self.server.max_stream_fps > 0 else 0.0
        next_send_at = 0.0

        try:
            while True:
                frame_id, frame = self.server.output.wait_for_frame(last_frame_id, timeout=5.0)
                if frame is None:
                    continue

                if min_interval > 0:
                    delay = next_send_at - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                        frame_id, frame = self.server.output.wait_for_frame(last_frame_id, timeout=0)
                        if frame is None:
                            continue

                last_frame_id = frame_id
                self.wfile.write(b"--" + BOUNDARY + b"\r\n")
                self.wfile.write(b"Content-Type: image/jpeg\r\n")
                self.wfile.write(f"Content-Length: {len(frame)}\r\n\r\n".encode("ascii"))
                self.wfile.write(frame)
                self.wfile.write(b"\r\n")
                self.wfile.flush()

                if min_interval > 0:
                    next_send_at = time.monotonic() + min_interval
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
    parser.add_argument(
        "--backend",
        default="auto",
        choices=("auto", "picamera2", "opencv"),
        help="Camera backend to use. Default: auto",
    )
    parser.add_argument(
        "--camera-index",
        type=int,
        default=0,
        help="OpenCV camera index for USB webcams. Default: 0",
    )
    parser.add_argument(
        "--opencv-api",
        default="auto",
        choices=("auto", "v4l2", "any"),
        help="OpenCV capture API for USB webcams. Default: auto, tries V4L2 first.",
    )
    parser.add_argument(
        "--fourcc",
        default="auto",
        choices=("auto", "MJPG", "YUYV", "YUY2", "none"),
        help="USB camera pixel format for OpenCV. Default: auto.",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Address to bind to. Default: 0.0.0.0")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on. Default: 8000")
    parser.add_argument("--width", type=int, default=1280, help="Stream width in pixels. Default: 1280")
    parser.add_argument("--height", type=int, default=720, help="Stream height in pixels. Default: 720")
    parser.add_argument("--fps", type=float, default=24.0, help="Camera frame rate. Default: 24")
    parser.add_argument("--quality", type=int, default=0, help="JPEG quality from 1 to 100. Default: auto, lower when --ngrok is used.")
    parser.add_argument(
        "--stream-fps",
        type=float,
        default=0.0,
        help="Maximum HTTP stream FPS. Default: auto, lower when --ngrok is used.",
    )
    parser.add_argument(
        "--smooth",
        action="store_true",
        help="Prefer lower latency over image quality; useful for ngrok/mobile links.",
    )
    parser.add_argument("--hflip", action="store_true", help="Flip image horizontally.")
    parser.add_argument("--vflip", action="store_true", help="Flip image vertically.")
    parser.add_argument("--ngrok", action="store_true", help="Start an ngrok tunnel and print a public URL.")
    parser.add_argument("--usb-ngrok", action="store_true", help="Shortcut for smooth USB webcam streaming through ngrok.")
    parser.add_argument("--ngrok-bin", default="ngrok", help="Path/name of the ngrok CLI. Default: ngrok")
    parser.add_argument(
        "--ngrok-api-url",
        default="http://127.0.0.1:4040",
        help="Local ngrok agent API URL. Default: http://127.0.0.1:4040",
    )
    parser.add_argument(
        "--ngrok-url",
        default=None,
        help="Optional reserved ngrok URL/domain, for example https://example.ngrok.app.",
    )
    parser.add_argument(
        "--ngrok-timeout",
        type=float,
        default=20.0,
        help="Seconds to wait for ngrok to publish a tunnel URL. Default: 20",
    )
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def _start_picamera2(args: argparse.Namespace, output: StreamingOutput, use_fps_control: bool):
    try:
        from libcamera import Transform
        from picamera2 import Picamera2
        from picamera2.encoders import JpegEncoder
        from picamera2.outputs import FileOutput
    except ImportError as exc:
        raise RuntimeError(
            "Missing camera package. Install it on Raspberry Pi OS with:\n"
            "  sudo apt update\n"
            "  sudo apt install -y python3-picamera2"
        ) from exc

    picam2 = Picamera2()
    transform = Transform(hflip=args.hflip, vflip=args.vflip)
    config_args = {
        "main": {"size": (args.width, args.height)},
        "transform": transform,
    }
    if use_fps_control:
        config_args["controls"] = {"FrameRate": args.fps}

    try:
        config = picam2.create_video_configuration(**config_args)
        picam2.configure(config)
        picam2.start_recording(JpegEncoder(q=args.quality), FileOutput(output))
    except Exception:
        picam2.close()
        raise

    return picam2


def start_picamera2(args: argparse.Namespace, output: StreamingOutput):
    try:
        return _start_picamera2(args, output, use_fps_control=True)
    except RuntimeError as exc:
        if "FrameDurationLimits" not in str(exc):
            raise

        logging.warning(
            "Camera does not advertise libcamera FPS controls; retrying Picamera2 without --fps control. "
            "This is common with USB/UVC cameras."
        )
        return _start_picamera2(args, output, use_fps_control=False)


class OpenCVCamera:
    """Small adapter with the same stop_recording method Picamera2 uses."""

    def __init__(self, args: argparse.Namespace, output: StreamingOutput) -> None:
        try:
            import cv2
        except ImportError as exc:
            raise RuntimeError(
                "Missing OpenCV. Install it on Raspberry Pi OS with:\n"
                "  sudo apt update\n"
                "  sudo apt install -y python3-opencv"
            ) from exc

        self.args = args
        self.output = output
        self.cv2 = cv2
        self.running = threading.Event()
        self.running.set()
        self.jpeg_params = [int(cv2.IMWRITE_JPEG_QUALITY), max(1, min(100, args.quality))]
        self.capture, first_frame = self._open_capture()
        self._write_frame(first_frame)

        self.thread = threading.Thread(target=self._capture_loop, name="opencv-camera", daemon=True)
        self.thread.start()

    def _api_candidates(self) -> list[tuple[str, int]]:
        candidates = []
        if self.args.opencv_api in ("auto", "v4l2") and hasattr(self.cv2, "CAP_V4L2"):
            candidates.append(("v4l2", self.cv2.CAP_V4L2))
        if self.args.opencv_api in ("auto", "any"):
            candidates.append(("any", self.cv2.CAP_ANY))
        return candidates

    def _fourcc_candidates(self) -> list[Optional[str]]:
        if self.args.fourcc == "auto":
            return ["MJPG", "YUYV", "YUY2", None]
        if self.args.fourcc == "none":
            return [None]
        return [self.args.fourcc]

    def _open_capture(self):
        errors = []
        for api_name, api_id in self._api_candidates():
            for fourcc in self._fourcc_candidates():
                label = fourcc or "default"
                capture = self.cv2.VideoCapture(self.args.camera_index, api_id)
                if not capture.isOpened():
                    errors.append(f"{api_name}/{label}: could not open camera index {self.args.camera_index}")
                    capture.release()
                    continue

                if hasattr(self.cv2, "CAP_PROP_BUFFERSIZE"):
                    capture.set(self.cv2.CAP_PROP_BUFFERSIZE, 1)

                if fourcc:
                    capture.set(self.cv2.CAP_PROP_FOURCC, self.cv2.VideoWriter_fourcc(*fourcc))
                capture.set(self.cv2.CAP_PROP_FRAME_WIDTH, self.args.width)
                capture.set(self.cv2.CAP_PROP_FRAME_HEIGHT, self.args.height)
                if self.args.fps > 0:
                    capture.set(self.cv2.CAP_PROP_FPS, self.args.fps)

                first_frame = self._read_first_frame(capture)
                if first_frame is not None:
                    width = int(capture.get(self.cv2.CAP_PROP_FRAME_WIDTH))
                    height = int(capture.get(self.cv2.CAP_PROP_FRAME_HEIGHT))
                    fps = capture.get(self.cv2.CAP_PROP_FPS)
                    logging.info(
                        "Opened camera index %s with OpenCV %s backend, FOURCC %s, %sx%s at %.1f FPS.",
                        self.args.camera_index,
                        api_name,
                        label,
                        width,
                        height,
                        fps,
                    )
                    return capture, first_frame

                errors.append(f"{api_name}/{label}: opened but no frames were readable")
                capture.release()

        raise RuntimeError("Could not read frames from USB camera. Tried: " + "; ".join(errors))

    def _read_first_frame(self, capture):
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            ok, frame = capture.read()
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

    def _capture_loop(self) -> None:
        encode_interval = 1.0 / self.args.stream_fps if self.args.stream_fps > 0 else 0.0
        next_encode_at = 0.0
        last_warning = 0.0

        while self.running.is_set():
            ok, frame = self.capture.read()
            if not ok or frame is None or getattr(frame, "size", 0) == 0:
                now = time.monotonic()
                if now - last_warning >= 2.0:
                    logging.warning("OpenCV could not read a frame from camera index %s", self.args.camera_index)
                    last_warning = now
                time.sleep(0.05)
                continue

            now = time.monotonic()
            if encode_interval > 0 and now < next_encode_at:
                continue

            self._write_frame(frame)
            if encode_interval > 0:
                next_encode_at = now + encode_interval
    def stop_recording(self) -> None:
        self.running.clear()
        self.thread.join(timeout=2.0)
        self.capture.release()

def start_camera(args: argparse.Namespace, output: StreamingOutput):
    backends = ("picamera2", "opencv") if args.backend == "auto" else (args.backend,)
    errors = []

    for backend in backends:
        try:
            if backend == "picamera2":
                camera = start_picamera2(args, output)
            elif backend == "opencv":
                camera = OpenCVCamera(args, output)
            else:
                raise RuntimeError(f"Unknown camera backend: {backend}")

            logging.info("Using %s camera backend.", backend)
            return camera
        except Exception as exc:
            errors.append(f"{backend}: {exc}")
            if args.backend != "auto":
                raise SystemExit(str(exc)) from exc
            logging.warning("%s backend failed: %s", backend, exc)

    raise SystemExit("No camera backend could start:\n  " + "\n  ".join(errors))


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


def start_ngrok_tunnel(args: argparse.Namespace) -> Optional[NgrokTunnel]:
    if not args.ngrok:
        return None

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
            "Check that your ngrok authtoken is configured and no other ngrok process is using port 4040."
        ) from exc

    if process.poll() is not None:
        raise SystemExit(f"ngrok exited before the tunnel was ready. Run `ngrok http {args.port}` manually to see its error.")

    tunnel.public_url = public_url
    logging.info("ngrok tunnel started: %s -> %s", public_url, target)
    return tunnel


def apply_runtime_defaults(args: argparse.Namespace) -> None:
    if args.usb_ngrok:
        args.backend = "opencv"
        args.opencv_api = "v4l2"
        args.fourcc = "YUYV"
        args.width = 640
        args.height = 480
        args.fps = 15.0
        args.stream_fps = 12.0
        args.quality = 60
        args.ngrok = True
    if args.smooth:
        if args.fps <= 0 or args.fps > 12:
            args.fps = 12.0
        if args.stream_fps <= 0 or args.stream_fps > 10:
            args.stream_fps = 10.0
        if args.quality <= 0 or args.quality > 50:
            args.quality = 50

    if args.quality <= 0:
        args.quality = 55 if args.ngrok else 75
    args.quality = max(1, min(100, args.quality))

    if args.stream_fps <= 0:
        if args.ngrok:
            args.stream_fps = min(args.fps, 10.0) if args.fps > 0 else 10.0
        else:
            args.stream_fps = args.fps if args.fps > 0 else 0.0

    if args.ngrok and args.stream_fps > 12:
        logging.info("Capping ngrok HTTP stream FPS from %.1f to 12.0 for lower latency.", args.stream_fps)
        args.stream_fps = 12.0


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    apply_runtime_defaults(args)

    output = StreamingOutput()
    camera = None
    server = None
    ngrok_tunnel = None

    try:
        camera = start_camera(args, output)
        server = CameraServer((args.host, args.port), StreamingHandler, output, args.stream_fps)
        ngrok_tunnel = start_ngrok_tunnel(args)
        lan_ip = get_lan_ip()

        print(f"Local viewer:  http://{lan_ip}:{args.port}/")
        print(f"Local stream:  http://{lan_ip}:{args.port}/stream.mjpg")
        print(f"Streaming:     {args.width}x{args.height}, camera {args.fps:g} FPS, HTTP {args.stream_fps:g} FPS, quality {args.quality}")
        if ngrok_tunnel:
            print(f"Universal link: {ngrok_tunnel.public_url}/")
            print(f"Public stream:   {ngrok_tunnel.public_url}/stream.mjpg")
        print("Press Ctrl+C to stop.")

        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping camera server...")
    finally:
        if server is not None:
            server.server_close()
        if ngrok_tunnel is not None:
            ngrok_tunnel.stop()
        if camera is not None:
            camera.stop_recording()

    return 0

if __name__ == "__main__":
    sys.exit(main())
