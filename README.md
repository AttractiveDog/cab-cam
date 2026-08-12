# Cab Cam

Cab Cam now uses a Node relay as the public entry point. The Raspberry Pi connects out to Node and uploads camera frames; Node owns the control dashboard, public viewer links, and ngrok tunnel.

## Start The Node Relay

```bash
node server.js
```

For LAN-only testing without ngrok:

```bash
npm run start:lan
```

The server prints:

- a dashboard URL with an admin token
- the Pi command with the shared key
- the active ngrok URL when the tunnel is ready

## Start The Raspberry Pi Client

Use the command printed by Node:

```bash
python3 qr_session_cam_server.py --server ws://<node-lan-ip>:3000/ws/pi --key <shared-key>
```

Useful Pi options:

```bash
python3 qr_session_cam_server.py --manual-start --width 640 --height 480 --stream-fps 12 --quality 50
```

## Dashboard Flow

Open the printed dashboard URL, wait for the Pi to connect, then use:

- `Start Camera`, `Stop Camera`, `Restart Camera` for Pi control
- `New Public Link` to create a fresh viewer link and revoke the old one
- `New Ngrok Link` to restart the ngrok tunnel from Node

Public viewers open `/view/<token>` links. Each new public link replaces the previous link.
