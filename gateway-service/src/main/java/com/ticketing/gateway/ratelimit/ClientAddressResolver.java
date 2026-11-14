package com.ticketing.gateway.ratelimit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Resolves the client IP address for IP-based rate limiting.
 * Section 4:
 * - Uses TCP remote connection address by default.
 * - Honors X-Forwarded-For only when remote address is within configured trusted proxies.
 * - Uses the right-most address that is not inside the trusted list.
 * - Guarantees the resolved value is a valid parsed IPv4 or IPv6 string.
 */
@Component
public class ClientAddressResolver {

    private static final Pattern IPV4_PATTERN = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");

    private final List<CidrMatcher> trustedMatchers;

    public ClientAddressResolver(@Value("${ticketing.gateway.trusted-proxies:}") String trustedProxiesConfig) {
        this.trustedMatchers = parseTrustedProxies(trustedProxiesConfig);
    }

    public String resolveClientIp(ServerWebExchange exchange) {
        InetSocketAddress remoteSocketAddress = exchange.getRequest().getRemoteAddress();
        InetAddress remoteInetAddress = remoteSocketAddress != null ? remoteSocketAddress.getAddress() : null;

        String fallbackIp = "127.0.0.1";
        if (remoteInetAddress != null) {
            fallbackIp = remoteInetAddress.getHostAddress();
        }

        if (remoteInetAddress == null || trustedMatchers.isEmpty() || !isTrusted(remoteInetAddress)) {
            return fallbackIp;
        }

        // Remote TCP connection is inside a trusted proxy range. Check X-Forwarded-For.
        List<String> xffHeaders = exchange.getRequest().getHeaders().get("X-Forwarded-For");
        if (xffHeaders == null || xffHeaders.isEmpty()) {
            return fallbackIp;
        }

        List<String> entries = new ArrayList<>();
        for (String headerVal : xffHeaders) {
            if (headerVal != null) {
                for (String part : headerVal.split(",")) {
                    String trimmed = part.trim();
                    if (!trimmed.isEmpty()) {
                        entries.add(trimmed);
                    }
                }
            }
        }

        if (entries.isEmpty()) {
            return fallbackIp;
        }

        // Walk right-to-left to find the right-most address that is not inside the trusted list
        for (int i = entries.size() - 1; i >= 0; i--) {
            String candidate = entries.get(i);
            InetAddress candidateInet = parseIpAddress(candidate);
            if (candidateInet == null) {
                // Entry cannot be parsed as an IP address: stop immediately and use connection address
                return fallbackIp;
            }
            if (!isTrusted(candidateInet)) {
                return candidateInet.getHostAddress();
            }
        }

        return fallbackIp;
    }

    public boolean isTrusted(InetAddress address) {
        for (CidrMatcher matcher : trustedMatchers) {
            if (matcher.matches(address)) {
                return true;
            }
        }
        return false;
    }

    public static InetAddress parseIpAddress(String ipStr) {
        if (ipStr == null || ipStr.isBlank()) {
            return null;
        }
        String trimmed = ipStr.trim();
        // Disallow hostnames by requiring literal IPv4 or IPv6 characters only
        if (!isIpLiteral(trimmed)) {
            return null;
        }
        try {
            InetAddress addr = InetAddress.getByName(trimmed);
            // Extra guard against IPv4 octet overflow (e.g. 999.999.999.999)
            if (trimmed.indexOf(':') == -1 && !IPV4_PATTERN.matcher(trimmed).matches()) {
                return null;
            }
            if (trimmed.indexOf(':') == -1) {
                String[] octets = trimmed.split("\\.");
                for (String o : octets) {
                    int val = Integer.parseInt(o);
                    if (val < 0 || val > 255) return null;
                }
            }
            return addr;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isIpLiteral(String str) {
        if (str.isEmpty() || str.length() > 45) {
            return false;
        }
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            boolean isHexOrDotOrColon = (c >= '0' && c <= '9')
                    || (c >= 'a' && c <= 'f')
                    || (c >= 'A' && c <= 'F')
                    || c == '.' || c == ':';
            if (!isHexOrDotOrColon) {
                return false;
            }
        }
        return str.indexOf('.') != -1 || str.indexOf(':') != -1;
    }

    private static List<CidrMatcher> parseTrustedProxies(String config) {
        List<CidrMatcher> matchers = new ArrayList<>();
        if (config == null || config.isBlank()) {
            return matchers;
        }
        for (String part : config.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                matchers.add(new CidrMatcher(trimmed));
            }
        }
        return matchers;
    }

    public static class CidrMatcher {
        private final byte[] networkBytes;
        private final int prefixLength;
        private final int addressFamily;

        public CidrMatcher(String cidrStr) {
            if (cidrStr == null) {
                throw new IllegalArgumentException("CIDR string must not be null");
            }
            String trimmed = cidrStr.trim();
            int firstSlash = trimmed.indexOf('/');
            int lastSlash = trimmed.lastIndexOf('/');
            if (firstSlash == -1 || firstSlash != lastSlash || firstSlash == 0 || firstSlash == trimmed.length() - 1) {
                throw new IllegalArgumentException("Invalid CIDR format (must contain exactly one slash): " + cidrStr);
            }
            String ipPart = trimmed.substring(0, firstSlash).trim();
            String prefixPart = trimmed.substring(firstSlash + 1).trim();

            InetAddress addr = parseIpAddress(ipPart);
            if (addr == null) {
                throw new IllegalArgumentException("Invalid CIDR IP: " + cidrStr);
            }
            this.networkBytes = addr.getAddress();
            this.addressFamily = networkBytes.length;

            int maxPrefix = (addressFamily == 4) ? 32 : 128;
            int prefix;
            try {
                prefix = Integer.parseInt(prefixPart);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid CIDR prefix length: " + cidrStr, e);
            }
            if (prefix < 0 || prefix > maxPrefix) {
                throw new IllegalArgumentException("CIDR prefix length out of range (0-" + maxPrefix + "): " + cidrStr);
            }
            this.prefixLength = prefix;
        }

        public boolean matches(InetAddress address) {
            byte[] targetBytes = address.getAddress();
            if (targetBytes.length != addressFamily) {
                return false;
            }
            int fullBytes = prefixLength / 8;
            int remainingBits = prefixLength % 8;
            for (int i = 0; i < fullBytes; i++) {
                if (targetBytes[i] != networkBytes[i]) {
                    return false;
                }
            }
            if (remainingBits > 0) {
                int mask = (0xFF << (8 - remainingBits)) & 0xFF;
                if ((targetBytes[fullBytes] & mask) != (networkBytes[fullBytes] & mask)) {
                    return false;
                }
            }
            return true;
        }
    }
}

