# Cab Cam

Cab Cam uses a Node relay as the central server. Multiple Raspberry Pi devices connect outbound to Node, and each Pi gets its own QR route.

## Flow

1. Start the Node server.
2. Start each Pi script with a unique `--device-id`.
3. Node exposes one QR target per Pi: `/d/<device-id>`.
4. A user scans that Pi QR and sees a user page with `Start a session`.
5. When the user starts the session, Node creates a fresh viewer session for that Pi. With ngrok enabled, Node starts a new ngrok tunnel and redirects the user to that tunnel URL.
6. The viewer sees only that Pi stream.

## Node Server

```bash
npm install
node server.js
```

LAN-only testing without ngrok:

```bash
npm run start:lan
```

Useful options:

```bash
node server.js --public-base-url https://your-stable-node-url.example
node server.js --session-minutes 10
node server.js --pi-key shared-secret --dashboard-token admin-secret
```

Use `--public-base-url` when the QR must point to a stable reachable Node URL. If omitted, QR targets use the server LAN IP.

## Raspberry Pi

Use the command printed by Node and give every Pi a unique device id:

```bash
python3 qr_session_cam_server.py --server ws://<node-lan-ip>:3000/api/pi/ws --key <shared-key> --device-id cab-01
```

Useful Pi options:

```bash
python3 qr_session_cam_server.py --device-id cab-01 --manual-start --width 640 --height 480 --stream-fps 12 --quality 50
```

The Pi logs its QR target and QR image URL after Node accepts the connection.
