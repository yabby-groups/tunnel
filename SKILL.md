---
name: tunnel
description: Expose an already-running local HTTP or WebSocket service through Myna Tunnel for temporary public access. Use when the user asks for a public URL, public network access, to expose localhost, to receive external webhooks, or uses terms such as "公网访问" or "内网穿透". Do not use to deploy tunnel-server, configure DNS or TLS, expose TCP services, or manage tunnel infrastructure.
---

# Myna Tunnel

Expose the user's existing local HTTP or WebSocket service. Prefer the
system-installed `tunnel` CLI. Do not start, modify, or deploy the local
service unless the user asks.

## Workflow

1. Determine the local target port or URL and verify that it responds locally.
2. Resolve the CLI. Use the installed binary when it is on `PATH`; otherwise,
   build the current repository's client binary in a temporary directory:

   ```sh
   TUNNEL_BIN="$(command -v tunnel || true)"
   if [ -z "$TUNNEL_BIN" ]; then
     TUNNEL_BIN="$(mktemp -d)/tunnel"
     go build -o "$TUNNEL_BIN" ./cmd/tunnel
   fi
   ```

3. Start the tunnel with the resolved binary:

   ```sh
   "$TUNNEL_BIN" http 3000
   "$TUNNEL_BIN" http http://127.0.0.1:8000
   ```

4. If the CLI cannot load a credential or reports that authorization failed,
   run the device-login flow and have the user complete browser approval:

   ```sh
   "$TUNNEL_BIN" login
   ```

5. Keep the tunnel process running. Report the `Forwarding https://...` URL
   printed by the CLI, then use that URL for the requested external access.
6. Stop the tunnel with `Ctrl-C` when the user no longer needs public access.

## Constraints

- The default tunnel endpoint is `wss://tunnel.huabot.com/connect`; only pass
  `-server` when the user provides a different endpoint.
- Forward only local HTTP or WebSocket services. Do not send credentials, local
  files, or arbitrary TCP traffic through the tunnel.
- Treat the generated URL as temporary and share it only with the intended
  recipient.
