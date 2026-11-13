package com.ticketing.gateway.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.gateway.config.JwtProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtTokenServiceTest {

    private JwtTokenService jwtTokenService;
    private JwtProperties jwtProperties;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        jwtProperties = new JwtProperties();
        jwtProperties.setRawSecret("01234567890123456789012345678901"); // 32 bytes
        jwtProperties.setAllowInsecureDevSecret(false);
        jwtProperties.validate();

        objectMapper = new ObjectMapper();
        jwtTokenService = new JwtTokenService(jwtProperties, objectMapper);
    }

    @Test
    @DisplayName("Generate and validate valid user token")
    void generateAndValidateUserToken() {
        UUID userId = UUID.randomUUID();
        List<String> roles = List.of("USER");

        String token = jwtTokenService.generateToken(userId, roles, 3600);
        assertThat(token).isNotBlank();

        ParsedJwt parsed = jwtTokenService.validateAndParseToken("Bearer " + token);
        assertThat(parsed.userId()).isEqualTo(userId);
        assertThat(parsed.roles()).containsExactly("USER");
        assertThat(parsed.expiresAt()).isNotNull();
    }

    @Test
    @DisplayName("Reject expired token with EXPIRED reason")
    void rejectExpiredToken() {
        UUID userId = UUID.randomUUID();
        // Expired token (ttl = -10, fallback in generateToken sets 3600, so let's manually craft or use -1)
        // Let's pass a small custom token with past exp
        String expiredPayload = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER\"],\"iat\":1000,\"exp\":1001}";
        String header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        String b64Payload = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(expiredPayload.getBytes());
        String signingInput = header + "." + b64Payload;

        // Sign with same secret
        String token = sign(signingInput, jwtProperties.getEffectiveSecret());

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.EXPIRED));
    }

    @Test
    @DisplayName("Reject wrongly signed token with INVALID reason")
    void rejectWronglySignedToken() {
        UUID userId = UUID.randomUUID();
        String token = jwtTokenService.generateToken(userId, List.of("USER"), 3600);

        // Tamper with signature
        String[] parts = token.split("\\.");
        String tampered = parts[0] + "." + parts[1] + ".invalidSignature1234567890";

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(tampered))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject token signed with different secret")
    void rejectTokenSignedWithDifferentSecret() {
        JwtProperties otherProps = new JwtProperties();
        otherProps.setRawSecret("different-secret-key-at-least-32-bytes!");
        otherProps.validate();
        JwtTokenService otherService = new JwtTokenService(otherProps, objectMapper);

        UUID userId = UUID.randomUUID();
        String token = otherService.generateToken(userId, List.of("USER"), 3600);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject malformed token")
    void rejectMalformedToken() {
        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken("not.a.valid.jwt.token"))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken("justonestring"))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject missing sub or empty roles claim")
    void rejectMissingClaims() {
        String noSub = "{\"roles\":[\"USER\"],\"iat\":1000,\"exp\":2000000000}";
        String header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        String b64Payload = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(noSub.getBytes());
        String token = sign(header + "." + b64Payload, jwtProperties.getEffectiveSecret());

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));

        String emptyRoles = "{\"sub\":\"" + UUID.randomUUID() + "\",\"roles\":[],\"iat\":1000,\"exp\":2000000000}";
        b64Payload = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(emptyRoles.getBytes());
        String token2 = sign(header + "." + b64Payload, jwtProperties.getEffectiveSecret());

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token2))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    private String sign(String signingInput, String secret) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] sig = mac.doFinal(signingInput.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            return signingInput + "." + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(sig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

