package com.ticketing.gateway.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.gateway.config.JwtProperties;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.PrematureJwtException;
import io.jsonwebtoken.security.Keys;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Service for issuing and verifying HS256 JWTs using JJWT 0.12.6.
 * Enforces strict RFC requirements, max token length (4096), canonical UUID sub,
 * exact upper-case roles, integral iat/exp claims, and 30-second clock skew.
 * Signature is verified first using a reused JwtParser; claims are read only
 * from the verified payload, never from an unverified Base64-decoded copy.
 * Tokens and secret values are never logged.
 */
@Service
public class JwtTokenService {

    private static final int MAX_TOKEN_LENGTH = 4096;
    private static final int CLOCK_SKEW_SECONDS = 30;
    private static final Pattern CANONICAL_UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final JwtProperties jwtProperties;
    private final ObjectMapper objectMapper;
    private final SecretKey signingKey;
    private final JwtParser jwtParser;

    public JwtTokenService(JwtProperties jwtProperties, ObjectMapper objectMapper) {
        this.jwtProperties = jwtProperties;
        this.objectMapper = objectMapper;
        this.signingKey = Keys.hmacShaKeyFor(jwtProperties.getEffectiveSecret().getBytes(StandardCharsets.UTF_8));
        this.jwtParser = Jwts.parser()
                .json(new io.jsonwebtoken.io.Deserializer<java.util.Map<String, ?>>() {
                    @Override
                    public java.util.Map<String, ?> deserialize(byte[] bytes) throws io.jsonwebtoken.io.DeserializationException {
                        try {
                            JsonNode node = objectMapper.readTree(bytes);
                            return validateAndConvert(node);
                        } catch (JwtValidationException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new io.jsonwebtoken.io.DeserializationException("Failed to deserialize JSON", e);
                        }
                    }

                    @Override
                    public java.util.Map<String, ?> deserialize(java.io.Reader reader) throws io.jsonwebtoken.io.DeserializationException {
                        try {
                            JsonNode node = objectMapper.readTree(reader);
                            return validateAndConvert(node);
                        } catch (JwtValidationException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new io.jsonwebtoken.io.DeserializationException("Failed to deserialize JSON", e);
                        }
                    }

                    private java.util.Map<String, ?> validateAndConvert(JsonNode node) {
                        if (node != null && node.isObject()) {
                            JsonNode expNode = node.get("exp");
                            if (expNode != null && !expNode.isIntegralNumber()) {
                                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "exp claim must be an integral number");
                            }
                            JsonNode iatNode = node.get("iat");
                            if (iatNode != null && !iatNode.isIntegralNumber()) {
                                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "iat claim must be an integral number");
                            }
                        }
                        return objectMapper.convertValue(node, new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() {});
                    }
                })
                .verifyWith(this.signingKey)
                .clockSkewSeconds(CLOCK_SKEW_SECONDS)
                .build();
    }

    /**
     * Issues an HS256 JWT with the specified claims.
     */
    public String generateToken(UUID userId, List<String> roles, long ttlSeconds) {
        if (userId == null) {
            throw new IllegalArgumentException("userId must not be null");
        }
        if (roles == null || roles.isEmpty()) {
            throw new IllegalArgumentException("roles must not be null or empty");
        }
        for (String role : roles) {
            if (role == null || !SecurityConstants.ALLOWED_ROLES.contains(role)) {
                throw new IllegalArgumentException("Invalid role in generateToken: " + role);
            }
        }
        if (ttlSeconds <= 0) {
            ttlSeconds = 3600;
        }

        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        Instant exp = now.plusSeconds(ttlSeconds);

        return Jwts.builder()
                .header()
                    .type("JWT")
                    .and()
                .subject(userId.toString())
                .claim("roles", roles)
                .issuedAt(Date.from(now))
                .expiration(Date.from(exp))
                .signWith(signingKey, Jwts.SIG.HS256)
                .compact();
    }

    /**
     * Validates and parses an HS256 JWT.
     * Signature is verified first using the pre-built JwtParser; claims are read only
     * from the verified payload, never from an unverified Base64-decoded copy.
     *
     * @param rawToken raw token or 'Bearer <token>'
     * @return validated ParsedJwt claims
     * @throws JwtValidationException if token is missing, malformed, expired, has invalid signature or claims
     */
    public ParsedJwt validateAndParseToken(String rawToken) {
        if (rawToken == null || rawToken.trim().isEmpty()) {
            throw new JwtValidationException(JwtValidationException.Reason.MISSING, "Missing Authorization token");
        }

        String token = SecurityConstants.stripBearerPrefix(rawToken);

        // 1. Length bound check
        if (token.length() > MAX_TOKEN_LENGTH) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Token exceeds maximum length of 4096");
        }

        // 2. Strict 3-part structure check (no trailing dots or missing sections)
        String[] parts = token.split("\\.", -1);
        if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty() || parts[2].isEmpty()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Malformed token: expected 3 non-empty parts");
        }

        // 3. Verify signature and structure with JJWT parser (built once and reused)
        Jws<Claims> jws;
        try {
            jws = jwtParser.parseSignedClaims(token);
        } catch (JwtValidationException e) {
            throw e;
        } catch (ExpiredJwtException e) {
            throw new JwtValidationException(JwtValidationException.Reason.EXPIRED, "Token has expired", e);
        } catch (PrematureJwtException e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Token is not yet valid (nbf)", e);
        } catch (JwtException | IllegalArgumentException e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Invalid token signature or structure", e);
        }

        // 4. Enforce HS256 algorithm check from header
        String alg = jws.getHeader().getAlgorithm();
        if (!"HS256".equals(alg)) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Unsupported algorithm: " + alg);
        }

        // 5. Read claims strictly from verified payload (never from unverified copy)
        Claims claims = jws.getPayload();

        // exp claim required and must be an integral number
        Object expVal = claims.get("exp");
        if (expVal == null || !(expVal instanceof Integer || expVal instanceof Long)) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing or non-integral exp claim");
        }

        // iat claim required and must be an integral number
        Object iatVal = claims.get("iat");
        if (iatVal == null || !(iatVal instanceof Integer || iatVal instanceof Long)) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing or non-integral iat claim");
        }

        // roles claim must be a non-empty array of allowlisted uppercase strings
        Object rolesVal = claims.get("roles");
        if (rolesVal == null || !(rolesVal instanceof List<?> rawRolesList) || rawRolesList.isEmpty()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing or non-array roles claim");
        }
        List<String> roles = new ArrayList<>();
        for (Object r : rawRolesList) {
            if (!(r instanceof String roleText)) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Role element must be a string");
            }
            if (!SecurityConstants.ALLOWED_ROLES.contains(roleText)) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "role is not allowlisted");
            }
            roles.add(roleText);
        }

        // sub claim must be present and match canonical UUID format
        Object subVal = claims.get("sub");
        if (subVal == null || !(subVal instanceof String subStr) || !CANONICAL_UUID_PATTERN.matcher(subStr).matches()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "sub claim must be a valid canonical UUID");
        }
        UUID userId = UUID.fromString((String) subVal);

        Date issuedAt = claims.getIssuedAt();
        Date expiration = claims.getExpiration();

        return new ParsedJwt(userId, roles, issuedAt.toInstant(), expiration.toInstant());
    }
}
