# Myna Tunnel

Myna Tunnel is a self-hosted HTTP and WebSocket reverse tunnel. A client makes
one authenticated outbound WSS connection and public traffic is multiplexed to
its local service. Users can reserve multiple stable subdomains, then run one
client process for each local service. Calling `tunnel http` without a
subdomain still creates a temporary random address that the same client
process retains across WebSocket reconnects.

Stable subdomains must be lowercase DNS labels from 5 through 63 characters.

## Quick start

For local development, map *.tunnel.test to 127.0.0.1 (or use a resolver that
does) and run:

    go run ./cmd/tunnel-server -listen :8080 -base-domain tunnel.test -dev-token local-dev-token
    go run ./cmd/tunnel http -server ws://localhost:8080/connect -token local-dev-token -subdomain demo 3000

The CLI prints a URL such as https://demo.tunnel.test. In local plain-HTTP
development, use http://demo.tunnel.test:8080. The development token accepts
any syntactically valid subdomain; production names must first be reserved.

## Production deployment

Place Caddy, Nginx, or Traefik in front of tunnel-server. It must terminate a
wildcard TLS certificate for the configured base domain, forward the original
Host, and permit WebSocket upgrades on /connect.

Run the server with -base-domain and
-control-url https://myna.example.com/api/tunnel/validate. The control endpoint
receives POST {"token":"...","subdomain":"..."} and returns 200
{"user_id":"..."} for a valid, non-revoked credential that owns the requested
subdomain. An empty subdomain requests a temporary address. Do not use
`-dev-token` outside local development.

The CLI device-login flow uses these myna control-plane endpoints:

- POST /api/tunnel/device/authorize returns device_code, user_code,
  verification_uri, and an optional polling interval.
- POST /api/tunnel/device/token receives device_code and returns
  200 {"token":"..."} after browser approval, or 202 while pending.

The control plane must bind credentials to the logged-in user, allow revocation,
and reject expired credentials. The CLI stores the returned credential in the
user config directory with mode 0600.

Set `TUNNEL_VERIFICATION_URI=https://myna.example.com/console/sandbox/tunnel`
on the Myna backend. Users open that authenticated page, enter the CLI's user
code, then the CLI receives its credential on the next poll. The same page can
reserve, list, and release stable subdomains. Credentials expire after 30 days
and can be revoked there without releasing a subdomain.

For a production Caddy starting point, see `deploy/Caddyfile`. Point
`myna.example.com` at the Myna backend and both `tunnel.example.com` and its
wildcard subdomains at the tunnel server. DNS must provide wildcard coverage
and Caddy must obtain a wildcard TLS certificate. Run one `tunnel-server`
replica: active hostname-to-connection mappings are intentionally in-memory.
Stable subdomain ownership lives in the Myna control-plane database, so a
client reconnect or server restart can restore its address. The active
hostname-to-connection map remains in memory, so run one tunnel-server replica.

    tunnel-server -listen :8080 -base-domain tunnel.example.com \
      -control-url https://myna.example.com/api/tunnel/validate
    tunnel login -control-url https://myna.example.com
    tunnel domains -control-url https://myna.example.com claim my-service
    tunnel http -server wss://tunnel.example.com/connect -subdomain my-service 3000

## Operations

- /healthz and /readyz return process health.
- /metrics exposes request, proxy-error, and active-session metrics.
- Defaults: up to 10 stable subdomains per user, 100 concurrent HTTP requests
  per tunnel, a 32 MiB request body, and a 60 second request timeout.

This implementation intentionally excludes custom domains, TCP forwarding,
billing, and request-body logging.
