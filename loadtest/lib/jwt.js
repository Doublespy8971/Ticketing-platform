import crypto from 'k6/crypto';
import encoding from 'k6/encoding';

const CANONICAL_UUID_REGEX = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const ALLOWED_ROLES = new Set(['USER', 'ADMIN']);

// Documented per-user gateway token-bucket limits (docs/arch/08-redis.md & SecurityAndHygieneGlobalFilter):
// 10 req/s burst, 5 req/s sustained.
export const GATEWAY_USER_BURST_CAPACITY = 10;
export const GATEWAY_USER_SUSTAINED_RPS = 5.0;

/**
 * Generates a canonical RFC 4122 UUIDv4 string using k6/crypto.randomBytes.
 */
export function generateUuidV4() {
  const bytes = new Uint8Array(crypto.randomBytes(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  let hex = '';
  for (let i = 0; i < 16; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return (
    hex.slice(0, 8) + '-' +
    hex.slice(8, 12) + '-' +
    hex.slice(12, 16) + '-' +
    hex.slice(16, 20) + '-' +
    hex.slice(20, 32)
  );
}

/**
 * Mints an HS256 JWT matching gateway-service JwtTokenService requirements:
 * - Header: { alg: "HS256", typ: "JWT" }
 * - Claims: sub (canonical UUID), roles (non-empty allowlisted array),
 *   iat and exp (integers, epoch seconds).
 * - Accounts for the gateway's 30-second clock skew by using integer epoch seconds
 *   and a generous default TTL (7200s).
 */
export function mintJwt(secret, sub, roles, ttlSeconds = 7200) {
  if (!secret || secret.length < 32) {
    throw new Error('JWT_SECRET must be provided via environment and be at least 32 bytes long');
  }
  if (!CANONICAL_UUID_REGEX.test(sub)) {
    throw new Error(`Invalid canonical UUID for sub claim: ${sub}`);
  }
  if (!Array.isArray(roles) || roles.length === 0) {
    throw new Error('roles must be a non-empty array');
  }
  for (const role of roles) {
    if (!ALLOWED_ROLES.has(role)) {
      throw new Error(`Role not in allowlist: ${role}`);
    }
  }

  const nowSec = Math.floor(Date.now() / 1000);
  const expSec = nowSec + Math.floor(ttlSeconds);

  const header = { alg: 'HS256', typ: 'JWT' };
  const payload = {
    sub: sub,
    roles: roles,
    iat: nowSec,
    exp: expSec,
  };

  const headerB64 = encoding.b64encode(JSON.stringify(header), 'rawurl');
  const payloadB64 = encoding.b64encode(JSON.stringify(payload), 'rawurl');
  const signingInput = `${headerB64}.${payloadB64}`;
  const signature = crypto.hmac('sha256', secret, signingInput, 'base64rawurl');

  return `${signingInput}.${signature}`;
}

/**
 * Pre-generates a pool of distinct USER JWTs. Call inside k6 SharedArray in init stage.
 */
export function buildUserTokenPool(secret, poolSize = 5000, ttlSeconds = 7200) {
  const pool = new Array(poolSize);
  for (let i = 0; i < poolSize; i++) {
    const userId = generateUuidV4();
    pool[i] = {
      userId: userId,
      token: mintJwt(secret, userId, ['USER'], ttlSeconds),
    };
  }
  return pool;
}

/**
 * Mints a single ADMIN token (roles: ["ADMIN", "USER"]) for setup operations.
 */
export function mintAdminToken(secret, ttlSeconds = 7200) {
  const adminId = generateUuidV4();
  return {
    userId: adminId,
    token: mintJwt(secret, adminId, ['ADMIN', 'USER'], ttlSeconds),
  };
}

/**
 * Computes expected per-user request rate and asserts it stays strictly below the
 * gateway's documented per-user sustained rate limit (5 req/s, 10 burst).
 */
export function assertPerUserRateWithinBucket(peakTotalRps, poolSize, label = 'scenario', singleUserActiveRps = 0) {
  if (poolSize <= 0) {
    throw new Error(`[${label}] User pool size must be positive, got ${poolSize}`);
  }
  const expectedAvgPerUserRps = peakTotalRps / poolSize;
  const effectivePerUserRps = Math.max(expectedAvgPerUserRps, singleUserActiveRps);

  console.log(
    `[${label}] Per-user rate check: peakTotalRps=${peakTotalRps.toFixed(2)}, poolSize=${poolSize}, ` +
    `expectedAvgPerUserRps=${expectedAvgPerUserRps.toFixed(4)}, singleUserActiveRps=${singleUserActiveRps.toFixed(2)}, ` +
    `bucketLimit=${GATEWAY_USER_SUSTAINED_RPS} req/s sustained (${GATEWAY_USER_BURST_CAPACITY} burst)`
  );

  if (effectivePerUserRps >= GATEWAY_USER_SUSTAINED_RPS) {
    throw new Error(
      `[${label}] Expected per-user rate (${effectivePerUserRps.toFixed(4)} req/s) ` +
      `is not below documented gateway sustained bucket (${GATEWAY_USER_SUSTAINED_RPS} req/s)`
    );
  }
  return expectedAvgPerUserRps;
}
