---
name: tunnel
description: Expose an already-running local HTTP or WebSocket service through Myna Tunnel using a temporary URL or a user-reserved fixed subdomain. Use when the user asks for a public URL, public network access, to expose localhost, to receive external webhooks, or uses terms such as "公网访问" or "内网穿透". Do not use to deploy tunnel-server, configure DNS or TLS, expose TCP services, or manage tunnel infrastructure.
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

4. When the user explicitly requests a fixed public address, claim a stable
   subdomain before starting the tunnel. The name must be a lowercase DNS label
   from 5 through 63 characters; use a descriptive name such as `my-service`.

   ```sh
   "$TUNNEL_BIN" domains claim my-service
   "$TUNNEL_BIN" http --subdomain my-service 3000
   ```

   Use `"$TUNNEL_BIN" domains list` to inspect existing names. Only release a
   name with `"$TUNNEL_BIN" domains release my-service` when the user has
   explicitly requested that destructive action.

5. If the CLI cannot load a credential or reports that authorization failed,
   run the device-login flow and have the user complete browser approval:

   ```sh
   "$TUNNEL_BIN" login
   ```

6. Keep the tunnel process running. Report the `Forwarding https://...` URL
   printed by the CLI, then use that URL for the requested external access.
7. Stop the tunnel with `Ctrl-C` when the user no longer needs public access.

## Constraints

- The default tunnel endpoint is `wss://tunnel.huabot.com/connect`; only pass
  `-server` when the user provides a different endpoint.
- Forward only local HTTP or WebSocket services. Do not send credentials, local
  files, or arbitrary TCP traffic through the tunnel.
- A URL without `--subdomain` is temporary but remains stable while the same
  CLI process reconnects. A claimed subdomain remains owned by the user after
  the process stops, and should not be released implicitly.
- Share public tunnel URLs only with the intended recipient.
