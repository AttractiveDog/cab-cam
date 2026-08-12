# Cab Cam

Cab Cam uses a Node relay as the central server. Multiple Raspberry Pi devices connect outbound to Node, and each Pi gets its own QR route.

## Flow

1. Start the Node server.
2. Start each Pi script with a unique `--device-id`.
3. Node exposes one QR target per Pi: `/d/<device-id>`.
4. The QR image includes a six-digit code above it.
5. A user scans that Pi QR, enters their name and the six-digit QR code, then clicks `Start a session`.
6. Node creates a fresh viewer session for that Pi. With ngrok enabled, Node starts a new ngrok tunnel and redirects the user to that tunnel URL.
7. The viewer sees only that Pi stream, can end the session from the stream page, and can open the viewer list.

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

The Pi logs its QR target and QR image URL after Node accepts the connection. The admin dashboard also shows the QR code, current session starter, and active viewer names.

## Vercel

Vercel mode uses the Vercel deployment URL as the public stream/session URL. It does not start ngrok inside Vercel.

Set production environment variables before deploying:

```bash
vercel env add PI_SHARED_KEY production
vercel env add DASHBOARD_TOKEN production
vercel env add SESSION_MINUTES production
```

Deploy:

```bash
vercel --prod
```

After deployment, run each Pi against the Vercel WebSocket URL:

```bash
python3 qr_session_cam_server.py --server wss://<your-vercel-domain>/api/pi/ws --key <PI_SHARED_KEY> --device-id cab-01
```
