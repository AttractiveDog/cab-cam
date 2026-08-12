#!/usr/bin/env python3
"""
Raspberry Pi camera publisher for the Cab Cam Node relay.

The Node server owns the dashboard, public viewer sessions, and ngrok tunnel.
This Pi script connects outbound to Node, uploads JPEG frames, and listens for
dashboard control commands.

Run the Node server on the relay machine:
    node server.js

Run this on the Raspberry Pi using the command printed by the Node server:
    python3 qr_session_cam_server.py --server ws://<server-lan-ip>:3000/api/pi/ws --key <shared-key> --device-id cab-01

Install camera dependencies on Raspberry Pi OS:
    sudo apt update
    sudo apt install -y python3-opencv
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import logging
import os
import socket
import ssl
import struct
import sys
import threading
import time
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse


WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class FrameBuffer(io.BufferedIOBase):
    """Thread-safe latest-frame buffer used by the camera thread."""

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
                ready = self.condition.wait_for(
                    lambda: self.frame is not None and self.frame_id != last_id,
                    timeout=timeout,
                )
                if not ready:
                    return last_id, None
            return self.frame_id, self.frame


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
        self.thread = threading.Thread(target=self._loop, name="cabcam-opencv-camera", daemon=True)
        self.thread.start()

    def _apis(self) -> list[tuple[str, int]]:
        items: list[tuple[str, int]] = []
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
                time.sleep(min(0.01, next_encode - now))
                continue

            self._write_frame(frame)
            if interval > 0:
                next_encode = now + interval

    def stop(self) -> None:
        self.running.clear()
        self.thread.join(timeout=2.0)
        self.capture.release()


class WebSocketError(RuntimeError):
    pass


class WebSocketClient:
    """Minimal RFC 6455 client for binary JPEG upload and JSON commands."""

    def __init__(self, server_url: str, key: str, device_id: str, name: str, timeout: float) -> None:
        self.server_url = server_url
        self.key = key
        self.device_id = device_id
        self.name = name
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.send_lock = threading.Lock()
        self.closed = True

    def connect(self) -> None:
        parsed = urlparse(self.server_url)
        if parsed.scheme not in ("ws", "wss"):
            raise WebSocketError("--server must start with ws:// or wss://")
        if not parsed.hostname:
            raise WebSocketError("--server must include a hostname")

        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        raw_sock = socket.create_connection((parsed.hostname, port), timeout=self.timeout)
        raw_sock.settimeout(self.timeout)
        if parsed.scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(raw_sock, server_hostname=parsed.hostname)
        else:
            sock = raw_sock

        resource = self._resource(parsed)
        ws_key = base64.b64encode(os.urandom(16)).decode("ascii")
        host_header = parsed.hostname
        if (parsed.scheme == "ws" and port != 80) or (parsed.scheme == "wss" and port != 443):
            host_header = f"{host_header}:{port}"

        request = "\r\n".join([
            f"GET {resource} HTTP/1.1",
            f"Host: {host_header}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {ws_key}",
            "Sec-WebSocket-Version: 13",
            "User-Agent: cabcam-pi-client/1.0",
            "",
            "",
        ]).encode("ascii")
        sock.sendall(request)
        response = self._read_http_response(sock)
        status_line = response.split("\r\n", 1)[0]
        if " 101 " not in status_line:
            sock.close()
            raise WebSocketError(f"WebSocket upgrade failed: {status_line}")

        accept = None
        for line in response.split("\r\n")[1:]:
            if line.lower().startswith("sec-websocket-accept:"):
                accept = line.split(":", 1)[1].strip()
                break
        expected = base64.b64encode(hashlib.sha1((ws_key + WS_GUID).encode("ascii")).digest()).decode("ascii")
        if accept != expected:
            sock.close()
            raise WebSocketError("WebSocket upgrade failed: invalid Sec-WebSocket-Accept")

        self.sock = sock
        self.closed = False

    def _resource(self, parsed) -> str:
        path = parsed.path or "/"
        query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if self.key:
            query_items["key"] = self.key
        if self.device_id:
            query_items["device"] = self.device_id
        if self.name:
            query_items["name"] = self.name
        query = urlencode(query_items)
        return path + (f"?{query}" if query else "")

    def _read_http_response(self, sock: socket.socket) -> str:
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                raise WebSocketError("Connection closed during WebSocket upgrade")
            data += chunk
            if len(data) > 32768:
                raise WebSocketError("WebSocket upgrade response was too large")
        return data.split(b"\r\n\r\n", 1)[0].decode("iso-8859-1")

    def send_json(self, payload: dict) -> None:
        self.send_frame(0x1, json.dumps(payload, separators=(",", ":")).encode("utf-8"))

    def send_binary(self, payload: bytes) -> None:
        self.send_frame(0x2, payload)

    def send_frame(self, opcode: int, payload: bytes) -> None:
        if self.closed or self.sock is None:
            raise WebSocketError("WebSocket is closed")

        length = len(payload)
        if length <= 125:
            header = struct.pack("!BB", 0x80 | opcode, 0x80 | length)
        elif length <= 65535:
            header = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, length)
        else:
            header = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, length)

        mask = os.urandom(4)
        masked = bytearray(payload)
        for index in range(len(masked)):
            masked[index] ^= mask[index % 4]

        with self.send_lock:
            self.sock.sendall(header + mask + bytes(masked))

    def recv_message(self) -> tuple[Optional[int], Optional[bytes]]:
        while True:
            try:
                opcode, payload = self._read_frame()
            except socket.timeout:
                return None, None

            if opcode == 0x8:
                raise WebSocketError("Server closed WebSocket")
            if opcode == 0x9:
                self.send_frame(0xA, payload)
                continue
            if opcode == 0xA:
                continue
            if opcode in (0x1, 0x2):
                return opcode, payload

    def _read_frame(self) -> tuple[int, bytes]:
        header = self._read_exact(2)
        first, second = header[0], header[1]
        opcode = first & 0x0F
        masked = (second & 0x80) != 0
        length = second & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(8))[0]
        mask = self._read_exact(4) if masked else b""
        payload = bytearray(self._read_exact(length))
        if masked:
            for index in range(len(payload)):
                payload[index] ^= mask[index % 4]
        return opcode, bytes(payload)

    def _read_exact(self, size: int) -> bytes:
        if self.sock is None:
            raise WebSocketError("WebSocket is closed")
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise WebSocketError("WebSocket connection closed")
            data += chunk
        return data

    def close(self) -> None:
        if self.closed:
            return
        try:
            self.send_frame(0x8, struct.pack("!H", 1000))
        except Exception:
            pass
        self.closed = True
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None


class PiBridge:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.frames = FrameBuffer()
        self.camera: Optional[OpenCVCamera] = None
        self.camera_lock = threading.Lock()
        self.client: Optional[WebSocketClient] = None
        self.client_lock = threading.Lock()
        self.connected = threading.Event()
        self.stop_event = threading.Event()
        self.started_at = time.monotonic()
        self.last_error = ""

    def run(self) -> None:
        sender = threading.Thread(target=self._sender_loop, name="cabcam-frame-uplink", daemon=True)
        heartbeat = threading.Thread(target=self._heartbeat_loop, name="cabcam-heartbeat", daemon=True)
        sender.start()
        heartbeat.start()

        if not self.args.manual_start:
            try:
                self.start_camera()
            except Exception as exc:
                logging.error("Initial camera startup failed: %s", exc)

        while not self.stop_event.is_set():
            client = WebSocketClient(
                self.args.server,
                self.args.key,
                self.args.device_id,
                self.args.name,
                self.args.connect_timeout,
            )
            try:
                logging.info("Connecting to Node relay: %s", self.args.server)
                client.connect()
                with self.client_lock:
                    self.client = client
                self.connected.set()
                logging.info("Connected to Node relay")
                self._send_hello()
                self.send_status("Pi connected")
                self._receive_loop(client)
            except Exception as exc:
                if not self.stop_event.is_set():
                    self.last_error = str(exc)
                    logging.warning("Node relay connection lost: %s", exc)
            finally:
                self.connected.clear()
                with self.client_lock:
                    if self.client is client:
                        self.client = None
                client.close()
            if not self.stop_event.is_set():
                time.sleep(max(0.5, self.args.reconnect_delay))

    def stop(self) -> None:
        self.stop_event.set()
        with self.client_lock:
            client = self.client
        if client is not None:
            client.close()
        self.stop_camera()

    def start_camera(self) -> None:
        with self.camera_lock:
            if self.camera is not None:
                self.send_status("Camera already running")
                return
            self.camera = OpenCVCamera(self.args, self.frames)
        self.last_error = ""
        logging.info("Camera streaming started")
        self.send_status("Camera started")

    def stop_camera(self) -> None:
        with self.camera_lock:
            camera = self.camera
            self.camera = None
        if camera is not None:
            camera.stop()
            logging.info("Camera streaming stopped")
            self.send_status("Camera stopped")

    def restart_camera(self) -> None:
        self.stop_camera()
        self.start_camera()

    def camera_running(self) -> bool:
        with self.camera_lock:
            return self.camera is not None

    def _receive_loop(self, client: WebSocketClient) -> None:
        while not self.stop_event.is_set() and not client.closed:
            opcode, payload = client.recv_message()
            if opcode is None or payload is None:
                continue
            if opcode == 0x1:
                self._handle_text(payload.decode("utf-8", errors="replace"))

    def _handle_text(self, text: str) -> None:
        try:
            message = json.loads(text)
        except json.JSONDecodeError:
            logging.debug("Ignoring non-JSON server message: %s", text)
            return

        if message.get("type") == "hello":
            device_url = message.get("device_url")
            qr_url = message.get("qr_url")
            if device_url:
                logging.info("Device QR target: %s", device_url)
            if qr_url:
                logging.info("Device QR image:   %s", qr_url)
            self.send_status("Server acknowledged Pi")
            return

        if message.get("type") != "command":
            return

        action = message.get("action")
        logging.info("Dashboard command: %s", action)
        try:
            if action == "start_camera":
                self.start_camera()
            elif action == "stop_camera":
                self.stop_camera()
            elif action == "restart_camera":
                self.restart_camera()
            elif action == "status":
                self.send_status("Status requested")
            else:
                self.send_error(f"Unknown command: {action}")
        except Exception as exc:
            self.last_error = str(exc)
            logging.exception("Command failed")
            self.send_error(str(exc))

    def _sender_loop(self) -> None:
        last_id = 0
        while not self.stop_event.is_set():
            if not self.connected.is_set() or not self.camera_running():
                time.sleep(0.1)
                continue

            frame_id, frame = self.frames.wait(last_id, timeout=1.0)
            if frame is None:
                continue

            with self.client_lock:
                client = self.client
            if client is None:
                continue

            try:
                client.send_binary(frame)
                last_id = frame_id
            except Exception as exc:
                self.last_error = str(exc)
                logging.warning("Frame upload failed: %s", exc)
                client.close()
                self.connected.clear()

    def _heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            if self.connected.is_set():
                self.send_status()
            time.sleep(max(1.0, self.args.status_interval))

    def _send_hello(self) -> None:
        self._send_json({
            "type": "hello",
            "device_id": self.args.device_id,
            "name": self.args.name,
            "camera": self.camera_running(),
            "width": self.args.width,
            "height": self.args.height,
            "stream_fps": self.args.stream_fps,
        })

    def send_status(self, message: str = "") -> None:
        self._send_json({
            "type": "status",
            "device_id": self.args.device_id,
            "name": self.args.name,
            "camera": self.camera_running(),
            "streaming": self.camera_running(),
            "width": self.args.width,
            "height": self.args.height,
            "frames": self.frames.frame_id,
            "uptime_seconds": int(time.monotonic() - self.started_at),
            "message": message,
            "error": self.last_error,
        })

    def send_error(self, error: str) -> None:
        self._send_json({
            "type": "error",
            "device_id": self.args.device_id,
            "name": self.args.name,
            "camera": self.camera_running(),
            "streaming": self.camera_running(),
            "error": error,
        })

    def _send_json(self, payload: dict) -> None:
        with self.client_lock:
            client = self.client
        if client is None or client.closed:
            return
        try:
            client.send_json(payload)
        except Exception as exc:
            self.last_error = str(exc)
            logging.debug("Status upload failed: %s", exc)


def url_has_key(server_url: str) -> bool:
    parsed = urlparse(server_url)
    return any(name == "key" and value for name, value in parse_qsl(parsed.query, keep_blank_values=True))


def default_device_id() -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in socket.gethostname()).strip("-") or "pi"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish Raspberry Pi camera frames to the Cab Cam Node relay.")
    parser.add_argument(
        "--server",
        default=os.environ.get("CAB_CAM_SERVER", "ws://127.0.0.1:3000/api/pi/ws"),
        help="Node relay Pi WebSocket URL. Default: ws://127.0.0.1:3000/api/pi/ws",
    )
    parser.add_argument(
        "--key",
        default=os.environ.get("PI_SHARED_KEY", ""),
        help="Shared Pi key printed by the Node server. Can also be PI_SHARED_KEY.",
    )
    parser.add_argument(
        "--device-id",
        default=os.environ.get("CAB_CAM_DEVICE_ID", default_device_id()),
        help="Stable Pi device id used for its QR route. Default: hostname.",
    )
    parser.add_argument("--name", default=socket.gethostname(), help="Pi name shown in the dashboard.")
    parser.add_argument("--connect-timeout", type=float, default=10.0, help="WebSocket connect/read timeout in seconds.")
    parser.add_argument("--reconnect-delay", type=float, default=3.0, help="Delay before reconnect attempts.")
    parser.add_argument("--status-interval", type=float, default=5.0, help="Seconds between status heartbeats.")
    parser.add_argument("--manual-start", action="store_true", help="Wait for dashboard Start Camera before opening the camera.")
    parser.add_argument("--camera-index", type=int, default=0)
    parser.add_argument("--opencv-api", default="v4l2", choices=("auto", "v4l2", "any"))
    parser.add_argument("--fourcc", default="auto", choices=("auto", "MJPG", "YUYV", "YUY2", "none"))
    parser.add_argument("--width", type=int, default=480)
    parser.add_argument("--height", type=int, default=360)
    parser.add_argument("--fps", type=float, default=20.0)
    parser.add_argument("--stream-fps", type=float, default=15.0)
    parser.add_argument("--quality", type=int, default=45)
    parser.add_argument("--hflip", action="store_true")
    parser.add_argument("--vflip", action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")

    if not args.key and not url_has_key(args.server):
        raise SystemExit("Provide the Pi shared key with --key or PI_SHARED_KEY. The Node server prints it at startup.")

    bridge = PiBridge(args)
    try:
        bridge.run()
    except KeyboardInterrupt:
        print("\nStopping Pi camera publisher...")
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
