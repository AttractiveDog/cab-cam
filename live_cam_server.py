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
import time
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
    parser.add_argument("--quality", type=int, default=85, help="JPEG quality from 1 to 100. Default: 85")
    parser.add_argument("--hflip", action="store_true", help="Flip image horizontally.")
    parser.add_argument("--vflip", action="store_true", help="Flip image vertically.")
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
        frame_interval = 1.0 / self.args.fps if self.args.fps > 0 else 0.0
        last_warning = 0.0

        while self.running.is_set():
            loop_started = time.monotonic()
            ok, frame = self.capture.read()
            if not ok or frame is None or getattr(frame, "size", 0) == 0:
                now = time.monotonic()
                if now - last_warning >= 2.0:
                    logging.warning("OpenCV could not read a frame from camera index %s", self.args.camera_index)
                    last_warning = now
                time.sleep(0.05)
                continue

            self._write_frame(frame)

            if frame_interval > 0:
                elapsed = time.monotonic() - loop_started
                time.sleep(max(0.0, frame_interval - elapsed))

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
