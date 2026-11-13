package com.ticketing.gateway.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.gateway.config.JwtProperties;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service for issuing and verifying HS256 JWTs.
 * Uses standard JDK cryptographic primitives and constant-time signature verification.
 * Tokens and secret values are never logged.
 */
@Service
public class JwtTokenService {

    private static final String HMAC_SHA256 = "HmacSHA256";
    private static final Base64.Encoder B64_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64_DECODER = Base64.getUrlDecoder();

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
        if (ttlSeconds <= 0) {
            ttlSeconds = 3600;
        }

        Instant now = Instant.now();
        Instant exp = now.plusSeconds(ttlSeconds);

        try {
            // Header
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("alg", "HS256");
            header.put("typ", "JWT");
            String headerB64 = B64_ENCODER.encodeToString(objectMapper.writeValueAsBytes(header));

            // Payload
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("sub", userId.toString());
            payload.put("roles", roles);
            payload.put("iat", now.getEpochSecond());
            payload.put("exp", exp.getEpochSecond());
            String payloadB64 = B64_ENCODER.encodeToString(objectMapper.writeValueAsBytes(payload));

            // Signature
            String signingInput = headerB64 + "." + payloadB64;
            byte[] signatureBytes = computeHmacSha256(signingInput.getBytes(StandardCharsets.US_ASCII), getSecretKeySpec());
            String signatureB64 = B64_ENCODER.encodeToString(signatureBytes);

            return signingInput + "." + signatureB64;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate JWT", e);
        }
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

        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Malformed token: expected 3 parts");
        }

        String headerB64 = parts[0];
        String payloadB64 = parts[1];
        String signatureB64 = parts[2];

        // 1. Verify signature with constant-time equality check
        byte[] expectedSignature = computeHmacSha256((headerB64 + "." + payloadB64).getBytes(StandardCharsets.US_ASCII), getSecretKeySpec());
        byte[] actualSignature;
        try {
            actualSignature = B64_DECODER.decode(signatureB64);
        } catch (IllegalArgumentException e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Malformed token signature");
        }

        if (!MessageDigest.isEqual(expectedSignature, actualSignature)) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Invalid token signature");
        }

        // 2. Parse and validate header
        try {
            JsonNode headerNode = objectMapper.readTree(B64_DECODER.decode(headerB64));
            String alg = headerNode.path("alg").asText(null);
            if (!"HS256".equals(alg)) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Unsupported algorithm: " + alg);
            }
        } catch (JwtValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Malformed token header");
        }

        // 3. Parse and validate payload
        try {
            JsonNode payloadNode = objectMapper.readTree(B64_DECODER.decode(payloadB64));

            // sub claim (UUID)
            String subStr = payloadNode.path("sub").asText(null);
            if (subStr == null || subStr.trim().isEmpty()) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing required sub claim");
            }
            UUID userId;
            try {
                userId = UUID.fromString(subStr);
            } catch (IllegalArgumentException e) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Invalid sub claim: must be a valid UUID");
            }

            // roles claim (non-empty array)
            JsonNode rolesNode = payloadNode.path("roles");
            if (rolesNode.isMissingNode() || rolesNode.isNull() || !rolesNode.isArray() || rolesNode.isEmpty()) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing or empty required roles claim");
            }
            List<String> roles = new ArrayList<>();
            for (JsonNode r : rolesNode) {
                if (r.isTextual() && !r.asText().isBlank()) {
                    roles.add(r.asText());
                }
            }
            if (roles.isEmpty()) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Roles claim must contain at least one role");
            }

            // exp claim
            JsonNode expNode = payloadNode.path("exp");
            if (expNode.isMissingNode() || !expNode.isNumber()) {
                throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Missing required exp claim");
            }
            long expSeconds = expNode.asLong();
            Instant expiresAt = Instant.ofEpochSecond(expSeconds);
            if (Instant.now().isAfter(expiresAt)) {
                throw new JwtValidationException(JwtValidationException.Reason.EXPIRED, "Token has expired");
            }

            // iat claim
            JsonNode iatNode = payloadNode.path("iat");
            Instant issuedAt = (iatNode.isNumber()) ? Instant.ofEpochSecond(iatNode.asLong()) : Instant.now();

            return new ParsedJwt(userId, roles, issuedAt, expiresAt);
        } catch (JwtValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new JwtValidationException(JwtValidationException.Reason.INVALID, "Malformed token payload");
        }
    }

    private SecretKeySpec getSecretKeySpec() {
        byte[] secretBytes = jwtProperties.getEffectiveSecret().getBytes(StandardCharsets.UTF_8);
        return new SecretKeySpec(secretBytes, HMAC_SHA256);
    }

    private byte[] computeHmacSha256(byte[] data, SecretKeySpec secretKeySpec) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(secretKeySpec);
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("HMAC-SHA256 computation failed", e);
        }
    }
}

