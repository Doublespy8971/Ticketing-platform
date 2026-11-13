package com.ticketing.gateway.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.gateway.config.JwtProperties;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtTokenServiceTest {

    private JwtTokenService jwtTokenService;
    private JwtProperties jwtProperties;
    private ObjectMapper objectMapper;
    private SecretKey signingKey;

    @BeforeEach
    void setUp() {
        jwtProperties = new JwtProperties();
        jwtProperties.setRawSecret("01234567890123456789012345678901"); // 32 bytes
        jwtProperties.setAllowInsecureDevSecret(false);
        jwtProperties.validate();

        objectMapper = new ObjectMapper();
        jwtTokenService = new JwtTokenService(jwtProperties, objectMapper);
        signingKey = Keys.hmacShaKeyFor(jwtProperties.getEffectiveSecret().getBytes(StandardCharsets.UTF_8));
    }

    private String manualToken(String headerJson, String payloadJson, SecretKey key) {
        String hB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
        String pB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
        String signingInput = hB64 + "." + pB64;
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(key);
            byte[] sig = mac.doFinal(signingInput.getBytes(StandardCharsets.US_ASCII));
            return signingInput + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(sig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    @DisplayName("Reject alg none")
    void rejectAlgNone() {
        String header = "{\"alg\":\"none\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"iat\":1000,\"exp\":2000000000}";
        String hB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String pB64 = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String unsignedToken = hB64 + "." + pB64 + ".";

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(unsignedToken))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject alg RS256")
    void rejectAlgRs256() {
        String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"iat\":1000,\"exp\":2000000000}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject alg HS384")
    void rejectAlgHs384() {
        String header = "{\"alg\":\"HS384\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"iat\":1000,\"exp\":2000000000}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject missing exp")
    void rejectMissingExp() {
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"iat\":1000}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject fractional exp")
    void rejectFractionalExp() {
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"iat\":1000,\"exp\":1700000000.5}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject missing iat")
    void rejectMissingIat() {
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + UUID.randomUUID() + "\",\"roles\":[\"USER\"],\"exp\":2000000000}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject non-UUID sub")
    void rejectNonUuidSub() {
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"not-a-valid-uuid-12345\",\"roles\":[\"USER\"],\"iat\":1000,\"exp\":2000000000}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject trailing dot")
    void rejectTrailingDot() {
        UUID userId = UUID.randomUUID();
        String token = jwtTokenService.generateToken(userId, List.of("USER"), 3600);
        String trailingDotToken = token + ".";

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(trailingDotToken))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject oversized token (>4096 characters)")
    void rejectOversizedToken() {
        String big = "a".repeat(4097);
        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(big))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Accept expired token within 30-second skew")
    void acceptExpiredTokenWithinClockSkew() {
        UUID userId = UUID.randomUUID();
        long nowSec = Instant.now().getEpochSecond();
        // Expired 15 seconds ago (within 30-second skew)
        long expSec = nowSec - 15;
        long iatSec = nowSec - 1000;

        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER\"],\"iat\":" + iatSec + ",\"exp\":" + expSec + "}";
        String token = manualToken(header, payload, signingKey);

        ParsedJwt parsed = jwtTokenService.validateAndParseToken(token);
        assertThat(parsed.userId()).isEqualTo(userId);
    }

    @Test
    @DisplayName("Reject expired token beyond 30-second skew")
    void rejectExpiredTokenBeyondClockSkew() {
        UUID userId = UUID.randomUUID();
        long nowSec = Instant.now().getEpochSecond();
        // Expired 45 seconds ago (beyond 30-second skew)
        long expSec = nowSec - 45;
        long iatSec = nowSec - 1000;

        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER\"],\"iat\":" + iatSec + ",\"exp\":" + expSec + "}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.EXPIRED));
    }

    @Test
    @DisplayName("Reject future nbf claim beyond clock skew")
    void rejectFutureNbf() {
        UUID userId = UUID.randomUUID();
        long nowSec = Instant.now().getEpochSecond();
        long nbfSec = nowSec + 120; // 2 minutes in future
        long expSec = nowSec + 3600;

        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER\"],\"iat\":" + nowSec + ",\"nbf\":" + nbfSec + ",\"exp\":" + expSec + "}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject roles as string")
    void rejectRolesAsString() {
        UUID userId = UUID.randomUUID();
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
        String payload = "{\"sub\":\"" + userId + "\",\"roles\":\"USER\",\"iat\":1000,\"exp\":2000000000}";
        String token = manualToken(header, payload, signingKey);

        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(token))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getReason()).isEqualTo(JwtValidationException.Reason.INVALID));
    }

    @Test
    @DisplayName("Reject invalid role values: comma, whitespace, lowercase, empty array")
    void rejectInvalidRoleValues() {
        UUID userId = UUID.randomUUID();
        String header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

        // Comma
        String p1 = "{\"sub\":\"" + userId + "\",\"roles\":[\"USER,ADMIN\"],\"iat\":1000,\"exp\":2000000000}";
        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(manualToken(header, p1, signingKey)))
                .isInstanceOf(JwtValidationException.class);

        // Whitespace
        String p2 = "{\"sub\":\"" + userId + "\",\"roles\":[\" ADMIN \"],\"iat\":1000,\"exp\":2000000000}";
        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(manualToken(header, p2, signingKey)))
                .isInstanceOf(JwtValidationException.class);

        // Lowercase
        String p3 = "{\"sub\":\"" + userId + "\",\"roles\":[\"admin\"],\"iat\":1000,\"exp\":2000000000}";
        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(manualToken(header, p3, signingKey)))
                .isInstanceOf(JwtValidationException.class);

        // Empty array
        String p4 = "{\"sub\":\"" + userId + "\",\"roles\":[],\"iat\":1000,\"exp\":2000000000}";
        assertThatThrownBy(() -> jwtTokenService.validateAndParseToken(manualToken(header, p4, signingKey)))
                .isInstanceOf(JwtValidationException.class);
    }
}
