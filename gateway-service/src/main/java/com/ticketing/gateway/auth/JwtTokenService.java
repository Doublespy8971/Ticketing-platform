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
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Service for issuing and verifying HS256 JWTs using JJWT 0.12.6.
 * Enforces strict RFC requirements, max token length (4096), canonical UUID sub,
 * exact upper-case roles, integral iat/exp claims, and 30-second clock skew.
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

    public JwtTokenService(JwtProperties jwtProperties, ObjectMapper objectMapper) {
        this.jwtProperties = jwtProperties;
        this.objectMapper = objectMapper;
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

        SecretKey key = getSigningKey();

        return Jwts.builder()
                .header()
                    .type("JWT")
                    .and()
                .subject(userId.toString())
                .claim("roles", roles)
                .issuedAt(Date.from(now))
                .expiration(Date.from(exp))
                .signWith(key, Jwts.SIG.HS256)
                .compact();
    }

    /**
     * Validates and parses an HS256 JWT.
     *
     * @param rawToken raw token or 'Bearer <token>'
     * @return validated ParsedJwt claims
     * @throws JwtValidationException if token is missing, malformed, expired, has invalid signature or claims
     */
    public ParsedJwt validateAndParseToken(String rawToken) {
        if (rawToken == null || rawToken.trim().isEmpty()) {
            throw new JwtValidationException(JwtValidationException.Reason.MISSING, "Missing Authorization token");
        }

        String token = rawToken.trim();
        if (token.startsWith("Bearer ") || token.startsWith("bearer ")) {
            token = token.substring(7).trim();
        }

        // 1. Length bound check
        if (token.length() > MAX_TOKEN_LENGTH) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Token exceeds maximum length of 4096");
        }

        // 2. Strict 3-part structure check (no trailing dots or missing sections)
        String[] parts = token.split("\\.", -1);
        if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty() || parts[2].isEmpty()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Malformed token: expected 3 non-empty parts");
        }

        // 3. Pre-verify payload JSON for required claim types (integral numbers for exp & iat, array for roles)
        JsonNode payloadNode;
        try {
            byte[] payloadBytes = Base64.getUrlDecoder().decode(parts[1]);
            payloadNode = objectMapper.readTree(payloadBytes);
        } catch (Exception e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Malformed token payload JSON", e);
        }

        // exp claim required and must be an integral number
        JsonNode expNode = payloadNode.get("exp");
        if (expNode == null || !expNode.isIntegralNumber()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing or non-integral exp claim");
        }

        // iat claim required and must be an integral number
        JsonNode iatNode = payloadNode.get("iat");
        if (iatNode == null || !iatNode.isIntegralNumber()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing or non-integral iat claim");
        }

        // roles claim must be a non-empty array of allowlisted uppercase strings
        JsonNode rolesNode = payloadNode.get("roles");
        if (rolesNode == null || !rolesNode.isArray() || rolesNode.isEmpty()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing or non-array roles claim");
        }
        List<String> roles = new ArrayList<>();
        for (JsonNode r : rolesNode) {
            if (!r.isTextual()) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Role element must be a string");
            }
            String roleText = r.asText();
            if (!SecurityConstants.ALLOWED_ROLES.contains(roleText)) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Role is not allowlisted: " + roleText);
            }
            roles.add(roleText);
        }

        // sub claim must be present and match canonical UUID format
        JsonNode subNode = payloadNode.get("sub");
        if (subNode == null || !subNode.isTextual() || !CANONICAL_UUID_PATTERN.matcher(subNode.asText()).matches()) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "sub claim must be a valid canonical UUID");
        }
        UUID userId = UUID.fromString(subNode.asText());

        // 4. JJWT signature and claim verification with 30-second clock skew
        SecretKey key = getSigningKey();
        JwtParser parser = Jwts.parser()
                .verifyWith(key)
                .clockSkewSeconds(CLOCK_SKEW_SECONDS)
                .build();

        Jws<Claims> jws;
        try {
            jws = parser.parseSignedClaims(token);
        } catch (ExpiredJwtException e) {
            throw new JwtValidationException(JwtValidationException.Reason.EXPIRED, "Token has expired", e);
        } catch (PrematureJwtException e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Token is not yet valid (nbf)", e);
        } catch (JwtException | IllegalArgumentException e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Invalid token signature or structure", e);
        }

        // 5. Enforce HS256 algorithm check from header
        String alg = jws.getHeader().getAlgorithm();
        if (!"HS256".equals(alg)) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Unsupported algorithm: " + alg);
        }

        Date issuedAt = jws.getPayload().getIssuedAt();
        Date expiration = jws.getPayload().getExpiration();

        return new ParsedJwt(userId, roles, issuedAt.toInstant(), expiration.toInstant());
    }

    private SecretKey getSigningKey() {
        byte[] secretBytes = jwtProperties.getEffectiveSecret().getBytes(StandardCharsets.UTF_8);
        return Keys.hmacShaKeyFor(secretBytes);
    }
}
