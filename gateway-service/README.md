# Gateway Service

Reactive API Gateway (`:8080`) built with Spring Cloud Gateway and Spring WebFlux. Serves as the single edge ingress entrypoint for all customer and administrative traffic in the ticketing platform.

---

## 1. Responsibilities & Boundaries

- **Edge Routing**: Routes public and protected HTTP traffic to `inventory-service` (`:8081`) and `order-service` (`:8082`). Configurable base URLs via `INVENTORY_SERVICE_URL`, `ORDER_SERVICE_URL`, and `PAYMENT_SERVICE_URL`.
- **JWT Verification**: Validates `HS256` Bearer JWTs signed with `JWT_SECRET` (minimum 32 bytes / 256 bits). Validates token signature, expiration (`exp`), and required claims (`sub` as UUID and non-empty `roles` array).
- **Role Enforcement (RBAC)**: Enforces that `/api/v1/admin/**` paths require the `ADMIN` role in the verified JWT `roles` claim. Returns `403 Forbidden` (`FORBIDDEN`) otherwise.
- **Header Hygiene**:
  - Strips any client-supplied `X-User-Id`, `X-User-Roles`, or other identity headers before forwarding downstream.
  - Injects verified `X-User-Id: <sub UUID>` and `X-User-Roles: <comma-joined roles>`.
  - Forwards `Idempotency-Key` and `X-Correlation-Id` unchanged. Generates a new UUIDv4 for `X-Correlation-Id` when absent and echoes it on the outgoing response.
  - Strips the `Authorization` header before forwarding downstream.
- **Rate Limiting**: Backed by Redis 7 and atomic Lua script (`token_bucket_ratelimit.lua`):
  - Authenticated requests: per-user token bucket (`ratelimit:user:{userId}`, 10 burst capacity, 5 refill rate/s, 60s TTL).
  - Unauthenticated requests: per-IP token bucket (`ratelimit:ip:{ipAddress}`, 20 burst capacity, 10 refill rate/s, 60s TTL).
  - Returns `429 Too Many Requests` (`RATE_LIMIT_EXCEEDED`) with header `Retry-After: 1`.
  - Fail-Open: If Redis is unavailable or fails, logs `WARN`, increments counter, and allows requests to proceed.
- **Dev-Only Token Endpoint**: `POST /api/v1/auth/token` allows minting HS256 JWT tokens for development and testing. **Must NEVER be enabled outside development environments.**

---

## 2. Spring Cloud & Dependency Versions

- **Spring Boot**: `3.3.5` (parent BOM).
- **Spring Cloud Release Train**: `2023.0.3` (Spring Cloud Gateway `4.1.5`).
- **Reactive Architecture**: Completely non-blocking (Netty + Spring WebFlux). No servlet or database dependencies (`ticketing-outbox` and `ticketing-consumer` are excluded; `ticketing-common` servlet auto-configurations are automatically skipped on reactive web applications).

---

## 3. Configuration Properties

| Property | Env Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.port` | `SERVER_PORT` | `8080` | HTTP listening port |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` | Redis host |
| `spring.data.redis.port` | `REDIS_PORT` | `6379` | Redis port |
| `inventory.service.url` | `INVENTORY_SERVICE_URL` | `http://localhost:8081` | Downstream inventory-service URL |
| `order.service.url` | `ORDER_SERVICE_URL` | `http://localhost:8082` | Downstream order-service URL |
| `payment.service.url` | `PAYMENT_SERVICE_URL` | `http://localhost:8083` | Downstream payment-service URL |
| `JWT_SECRET` | `JWT_SECRET` | *(none)* | Minimum 32-byte HS256 signing secret |
| `ticketing.gateway.allow-insecure-dev-secret` | `TICKETING_GATEWAY_ALLOW_INSECURE_DEV_SECRET` | `false` | Fallback to dev secret with WARN if missing/short |
| `ticketing.gateway.dev-token.enabled` | `TICKETING_GATEWAY_DEV_TOKEN_ENABLED` | `true` | Enable `/api/v1/auth/token` (dev only!) |
| `ticketing.gateway.dev-token.allow-admin` | `TICKETING_GATEWAY_DEV_TOKEN_ALLOW_ADMIN` | `false` | Permit minting ADMIN role via dev token endpoint |
| `spring.cloud.gateway.httpclient.connect-timeout` | — | `2000` | Netty connect timeout in ms (2s) |
| `spring.cloud.gateway.httpclient.response-timeout` | — | `10s` | Netty response timeout (10s) |

---

## 4. Routes Specification

| Route ID | Method | Path | Target Service | Auth Required |
| :--- | :--- | :--- | :--- | :--- |
| `inventory-seats` | `GET` | `/api/v1/events/{eventId}/seats` | `inventory-service` (`:8081`) | Public (IP rate-limited) |
| `inventory-reservations` | `POST` | `/api/v1/reservations` | `inventory-service` (`:8081`) | `USER` or `ADMIN` |
| `inventory-admin` | `ANY` | `/api/v1/admin/**` | `inventory-service` (`:8081`) | `ADMIN` only |
| `order-checkout` | `POST` | `/api/v1/orders/{orderId}/checkout` | `order-service` (`:8082`) | `USER` or `ADMIN` |
| `order-get` | `GET` | `/api/v1/orders/{orderId}` | `order-service` (`:8082`) | `USER` or `ADMIN` |
| `dev-token` | `POST` | `/api/v1/auth/token` | Gateway Local | Public (`dev`/`local` only) |
| `actuator` | `GET` | `/actuator/**` | Gateway Local | Public |

---

## 5. Metrics Contract

Registered in Micrometer registry and scraped via `GET /actuator/prometheus`:

- `gateway_requests_total`: Counter tagged with `route` (`inventory-seats`, `inventory-reservations`, `inventory-admin`, `order-checkout`, `order-get`, `auth-token`, `actuator`, `unknown`) and `status_class` (`2xx`, `4xx`, `5xx`, `other`).
- `gateway_rate_limited_total`: Counter tagged with `route`.
- `gateway_auth_failures_total`: Counter tagged with `reason` (`missing`, `invalid`, `expired`, `forbidden`).
- `gateway_redis_failures_total`: Counter tracking Redis rate limiter failures/downtime (fail-open mode).

---

## 6. Assumptions & Design Decisions

1. **Spring Cloud Release Train**: Compatible with Spring Boot `3.3.5` is `2023.0.3` (defined in root POM).
2. **Standard Cryptography**: HS256 JWT minting and verification uses JDK standard `javax.crypto.Mac` ("HmacSHA256") and constant-time `MessageDigest.isEqual` comparison to eliminate timing attacks and external library vulnerabilities.
3. **Public Route Matching**: `GET /api/v1/events/{eventId}/seats` is public. Any other method on `/api/v1/events/**` requires authentication.
4. **Dev Token Endpoint Security**: Unregistered or disabled when `ticketing.gateway.dev-token.enabled=false`. Attempting to mint an `ADMIN` role returns `403 Forbidden` (`FORBIDDEN`) unless `ticketing.gateway.dev-token.allow-admin=true`.
5. **Redis Down Policy**: As documented in the platform error-handling guidelines, Redis unavailability in the gateway fails open with a `WARN` log and increments `gateway_redis_failures_total`, ensuring customer requests are not blocked by a cache outage.
6. **Payment Service Downstream**: The platform documentation defines no public HTTP endpoints for `payment-service` because payment processing is fully asynchronous via Kafka events (`order.payment.requested`). `PAYMENT_SERVICE_URL` is retained for potential future routes.

