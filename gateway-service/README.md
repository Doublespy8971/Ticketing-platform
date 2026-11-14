# Gateway Service

Reactive API Gateway (`:8080`) built with Spring Cloud Gateway and Spring WebFlux. Serves as the single edge ingress entrypoint for all customer and administrative traffic in the ticketing platform.

---

## 1. Quick Start & Execution

To run the gateway locally with dev token issuance enabled and local developer fallbacks:
```bash
make run-gateway
```
*Note: In production environments (`SPRING_PROFILES_ACTIVE=prod`), the dev token endpoint is disabled and fails startup if configured active.*

---

## 2. Responsibilities & Boundaries

- **Edge Routing**: Routes public and protected HTTP traffic strictly matching `docs/arch/06-api.md`:
  - `inventory-seats` (`GET /api/v1/events/*/seats`): Public (IP rate-limited).
  - `inventory-reservations` (`POST /api/v1/reservations`): Protected (`USER` or `ADMIN`).
  - `inventory-admin` (`POST /api/v1/admin/**`, `PATCH /api/v1/admin/**`): Protected (`ADMIN` only).
  - `order-checkout` (`POST /api/v1/orders/*/checkout`): Protected (`USER` or `ADMIN`).
  - `order-get` (`GET /api/v1/orders/*`): Protected (`USER` or `ADMIN`).
  - Undocumented routes (such as payment routes) are unrouted and return `404 Not Found` (`RESOURCE_NOT_FOUND`).
- **Path Hygiene**: A highest-precedence WebFilter intercepts requests before routing and immediately rejects any raw path containing a percent sign (`%`), semicolon (`;`), backslash (`\`), double slashes (`//`), dot-dot segment (`..`), NUL byte (`\0`), or non-ASCII characters with `400 Bad Request` (`VALIDATION_ERROR`).
- **Route-Based Authorization**: Security filter determines access rules strictly from the matched Spring Cloud Gateway Route ID (`ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR`) using constants, never from raw path string manipulation.
- **Roles Allowlist**: Exact upper-case, case-sensitive allowlist `{USER, ADMIN}`. Any request to the dev token endpoint containing unknown roles, comma/whitespace smuggling, blank entries, or an empty list is rejected with `400 VALIDATION_ERROR`. Tokens with invalid roles claims are rejected with `401 UNAUTHORIZED`.
- **Client Address & Trusted Proxies**: IP rate limiting uses the remote TCP socket address. `X-Forwarded-For` is honored only if the TCP connection arrives from a CIDR range in `ticketing.gateway.trusted-proxies`. The right-most untrusted address is chosen and validated as a canonical IPv4/IPv6 address.
- **Header Hygiene**:
  - Strips any client-supplied `X-User-Id`, `X-User-Roles`, and `Authorization` headers.
  - Injects verified `X-User-Id: <sub UUID>` and `X-User-Roles: <comma-joined roles>`.
  - Forwards `Idempotency-Key` and `X-Correlation-Id` unchanged. Generates a new canonical UUIDv4 for `X-Correlation-Id` when absent, invalid, or duplicate, and echoes it on every response (including errors) via a `beforeCommit` hook.
- **Rate Limiting**: Backed by Redis 7 and atomic Lua script (`token_bucket_ratelimit.lua`):
  - Authenticated requests: per-user token bucket (`ratelimit:user:{userId}`, 10 burst capacity, 5 refill rate/s, 60s TTL).
  - Unauthenticated requests: per-IP token bucket (`ratelimit:ip:{ipAddress}`, 20 burst capacity, 10 refill rate/s, 60s TTL).
  - Dev token endpoint: per-IP token bucket (`ratelimit:auth:ip:{ipAddress}`, 10 capacity, 1 refill/s).
  - Breaches return `429 Too Many Requests` (`RATE_LIMIT_EXCEEDED`) with header `Retry-After: 1`.
  - Script execution enforces a 200ms Reactor timeout. Redis downtime/timeout fails open with a rate-limited `WARN` log (at most once every 10s), increments `gateway_redis_failures_total`, and records `ticketing_redis_lua_duration_seconds`.
- **HTTP Client Retries & Pool**:
  - Bounded Reactor Netty pool: fixed type, 200 max connections, 2000ms acquire timeout, 30s max idle time, 5 minutes max life time.
  - `HttpClientCustomizer` disables Reactor Netty's automatic retry (`disableRetry(true)`). The gateway never automatically retries requests (preventing dangerous replays of non-idempotent `POST` requests). Clients retry explicitly using the `Idempotency-Key` header.
- **Dev-Only Token Endpoint**: `POST /api/v1/auth/token` is gated by `@Profile({"dev", "local"})` and property `ticketing.gateway.dev-token.enabled` (default `false`). Startup fails if `prod` profile is active with `dev-token.enabled=true`.

---

## 3. Configuration Properties

| Property | Env Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.port` | `SERVER_PORT` | `8080` | HTTP listening port |
| `server.max-http-request-header-size` | — | `16KB` | Maximum HTTP request header size |
| `server.netty.connection-timeout` | — | `5s` | Netty TCP connection handshake timeout |
| `server.netty.idle-timeout` | — | `15s` | Netty connection idle timeout |
| `management.server.port` | — | `8085` *(prod)* | Dedicated management port in prod profile (not exposed publicly) |
| `management.endpoint.health.show-details` | — | `never` | Health component details visibility (always suppressed) |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` | Redis host |
| `spring.data.redis.port` | `REDIS_PORT` | `6379` | Redis port |
| `spring.data.redis.timeout` | — | `200ms` | Redis command timeout |
| `spring.data.redis.connect-timeout` | — | `500ms` | Redis connect timeout |
| `inventory.service.url` | `INVENTORY_SERVICE_URL` | `http://localhost:8081` | Downstream inventory-service URL |
| `order.service.url` | `ORDER_SERVICE_URL` | `http://localhost:8082` | Downstream order-service URL |
| `JWT_SECRET` | `JWT_SECRET` | *(none)* | Minimum 32-byte HS256 signing secret (UTF-8 bytes) |
| `ticketing.gateway.allow-insecure-dev-secret` | `TICKETING_GATEWAY_ALLOW_INSECURE_DEV_SECRET` | `false` | Fallback to dev secret with WARN if missing/short (forbidden in prod) |
| `ticketing.gateway.dev-token.enabled` | `TICKETING_GATEWAY_DEV_TOKEN_ENABLED` | `false` | Enable `/api/v1/auth/token` (dev/local only!) |
| `ticketing.gateway.dev-token.allow-admin` | `TICKETING_GATEWAY_DEV_TOKEN_ALLOW_ADMIN` | `false` | Permit minting ADMIN role via dev token endpoint |
| `ticketing.gateway.trusted-proxies` | `TICKETING_GATEWAY_TRUSTED_PROXIES` | `""` | Comma-separated trusted CIDR ranges for X-Forwarded-For |
| `spring.cloud.gateway.httpclient.connect-timeout` | — | `2000` | Netty connect timeout in ms (2s) |
| `spring.cloud.gateway.httpclient.response-timeout` | — | `10s` | Netty response timeout (10s) |

---

## 4. Metrics Contract

Metrics carry the common tag `service=gateway-service`:
- `gateway_requests_total`: Counter tagged with `route` (`inventory-seats`, `inventory-reservations`, `inventory-admin`, `order-checkout`, `order-get`, `payment-service`, `auth-token`, `actuator`, `unmatched`) and `status_class` (`2xx`, `3xx`, `4xx`, `5xx`, `cancelled`, `other`). Recorded via `beforeCommit` WebFilter hook and a `doFinally` fallback guarded by an `AtomicBoolean` ensuring aborted/cancelled requests are counted once with `status_class: "cancelled"`. Route tag is strictly derived: `auth-token` applies only to `/api/v1/auth/token` when registered; `actuator` applies only to `/actuator/health`, `/actuator/info`, `/actuator/prometheus`; all other unregistered paths (including `/api/v1/auth/foo` and `/actuator/env`) map to `unmatched`.
- `gateway_rate_limited_total`: Counter tagged with `route`.
- `gateway_auth_failures_total`: Counter tagged with `reason` (`missing`, `invalid`, `expired`, `forbidden`).
- `gateway_redis_failures_total`: Counter tracking Redis rate limiter failures/downtime.
- `ticketing_redis_lua_duration_seconds`: Timer tagged with `service`, `script`, and `result` (`allowed`, `rate_limited`, `error`).

---

## 5. Request Limits & Hygiene Rules

- **Request Body Limits**:
  - `GET` requests must have no body (`Content-Length > 0` or `Transfer-Encoding` present returns `400 Bad Request` with `VALIDATION_ERROR`).
  - `POST` and `PATCH` requests require a `Content-Length` header (`Transfer-Encoding: chunked` returns `411 Length Required` with `LENGTH_REQUIRED`).
  - Request body size limit: 64KB default, 2MB for bulk seat creation (`POST /api/v1/admin/events/{eventId}/seats/bulk`). Bodies exceeding this limit return `413 Payload Too Large` (`PAYLOAD_TOO_LARGE`) without reaching downstream services.
- **Header Hygiene**:
  - Strips any request header whose normalized name (lowercase, underscores replaced with hyphens) matches: `x-user-id`, `x-user-roles`, `authorization`, `x-http-method-override`, `x-http-method`, `x-method-override`.
  - Forwards `Idempotency-Key` and validated canonical UUID `X-Correlation-Id`.
- **Error Response Hygiene**:
  - Error responses strictly return RFC 7807 Problem Details (`application/problem+json`).
  - No request-derived text appears in error responses: role rejection detail is fixed string `"role is not allowlisted"`.
  - The `instance` field for path hygiene rejections and unmatched `404 Not Found` responses is the fixed string `"/"`, never the client-supplied path.

---

## 6. Residual Risks & Production Architecture

1. **Fail-Open Rate Limiting on Redis Slowdown/Outage**:
   - *Design Choice*: The gateway enforces a strict 200ms timeout on Redis Lua execution. If Redis is unavailable or times out, the rate limiter fails open to prioritize ingress availability over strict throughput bounding during an outage.
   - *Residual Risk*: During a sustained Redis outage or an intentional DDoS attack directed at Redis, malicious callers could exceed the documented token-bucket limits.
2. **Shared Redis Risk**:
   - *Design Choice*: In local and staging environments, the gateway may share a Redis 7 instance with domain services (`inventory-service`).
   - *Residual Risk*: High gateway ingress burst traffic or aggressive memory usage could cause key evictions or latency spikes affecting business domain hold keys.
   - *Production Recommendation*: Deploy a dedicated Redis instance or cluster with memory reservations exclusively for `gateway-service` rate limiting, completely segregated from business domain data and cache keys, configured with `maxmemory-policy noeviction` or `volatile-lru`.
3. **Management Endpoint Segregation**:
   - In production (`prod` profile), actuator management endpoints are bound to internal port `8085` (`management.server.port: 8085`) exposing only `health`, `info`, and `prometheus`. Port `8085` must be restricted to internal monitoring systems and never routed through the edge ingress. Health details (`show-details`) are permanently disabled across all profiles.
