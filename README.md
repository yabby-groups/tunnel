# Myna Tunnel

Myna Tunnel is a self-hosted, temporary HTTP and WebSocket reverse tunnel. A
client makes one authenticated outbound WSS connection; public traffic for its
random subdomain is multiplexed over that connection to a local service.

## Quick start

For local development, map *.tunnel.test to 127.0.0.1 (or use a resolver that
does) and run:

    go run ./cmd/tunnel-server -listen :8080 -base-domain tunnel.test -dev-token local-dev-token
    go run ./cmd/tunnel http -server ws://localhost:8080/connect -token local-dev-token 3000

The CLI prints a URL such as https://abcd.tunnel.test. In local plain-HTTP
development, use http://abcd.tunnel.test:8080.

## Production deployment

Place Caddy, Nginx, or Traefik in front of tunnel-server. It must terminate a
wildcard TLS certificate for the configured base domain, forward the original
Host, and permit WebSocket upgrades on /connect.

Run the server with -base-domain and
-control-url https://myna.example.com/api/tunnel/validate. The control endpoint
receives POST {"token":"..."} and returns 200 {"user_id":"..."} for a valid,
non-revoked tunnel credential. Do not use -dev-token outside local development.

The CLI device-login flow uses these myna control-plane endpoints:

- POST /api/tunnel/device/authorize returns device_code, user_code,
  verification_uri, and an optional polling interval.
- POST /api/tunnel/device/token receives device_code and returns
  200 {"token":"..."} after browser approval, or 202 while pending.

The control plane must bind credentials to the logged-in user, allow revocation,
and reject expired credentials. The CLI stores the returned credential in the
user config directory with mode 0600.

## Operations

- /healthz and /readyz return process health.
- /metrics exposes request, proxy-error, and active-session metrics.
- Defaults: one active tunnel per user, 100 concurrent HTTP requests per
  tunnel, a 32 MiB request body, and a 60 second request timeout.

This MVP intentionally excludes persistent names, custom domains, TCP
forwarding, billing, and request-body logging.
