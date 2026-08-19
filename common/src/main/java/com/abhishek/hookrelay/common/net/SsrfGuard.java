package com.abhishek.hookrelay.common.net;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Locale;

/**
 * Rejects webhook destinations that point inside the network.
 *
 * <p>The endpoint URL is the one input to this system that is <em>supposed</em> to be arbitrary, and
 * the worker that fetches it runs inside the cluster. Without this check a customer can register
 * {@code http://169.254.169.254/latest/meta-data/iam/security-credentials/} and have the platform
 * fetch cloud credentials, sign the request, retry it, and store the response body — which is
 * Server-Side Request Forgery with the product's own features as the exploit.
 *
 * <p>The check is on the <strong>resolved address</strong>, never the hostname. Blocking the string
 * "localhost" accomplishes nothing: {@code evil.example.com} is a perfectly ordinary name that can
 * resolve to {@code 127.0.0.1}.
 *
 * <p><strong>Known limitation — DNS rebinding is narrowed, not closed.</strong> Validation happens
 * at delivery time on a fresh lookup, which defeats an attacker who changes DNS after registration.
 * But the HTTP client then resolves the name again, and only the first lookup was validated, so a
 * time-of-check to time-of-use gap remains. Closing it means connecting to the exact validated IP,
 * which {@code java.net.http.HttpClient} does not expose — rewriting the URL to the literal address
 * would work for {@code http} and break TLS certificate verification for {@code https}, a worse
 * trade. In practice the JVM's DNS cache means the second lookup returns the first's answer and the
 * window is microseconds wide. That is a mitigation, not a guarantee.
 */
public final class SsrfGuard {

    public record Result(boolean allowed, String reason) {
        public static Result allow() {
            return new Result(true, null);
        }

        public static Result deny(String reason) {
            return new Result(false, reason);
        }
    }

    private SsrfGuard() {
    }

    /**
     * Syntax-only validation, suitable for registration.
     *
     * <p>Does not resolve, so it cannot judge where a hostname points.
     */
    public static Result checkUrlSyntax(String url) {
        if (url == null || url.isBlank()) {
            return Result.deny("url is required");
        }

        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            return Result.deny("url is not a valid URI");
        }

        if (!uri.isAbsolute() || uri.getScheme() == null) {
            return Result.deny("url must be absolute");
        }
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            return Result.deny("url scheme must be http or https");
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            return Result.deny("url must contain a host");
        }
        // http://internal-host@evil.example.com/ and its inverse are a standard way to confuse
        // host parsing — the browser, the library and the validator can disagree about which part
        // is the host. Refusing userinfo removes the ambiguity entirely.
        if (uri.getUserInfo() != null) {
            return Result.deny("url must not contain userinfo");
        }
        return Result.allow();
    }

    /**
     * Full validation: syntax, then every address the host resolves to.
     *
     * <p>Every resolved address must be allowed, not merely the first. A hostname with both a public
     * and a private A record would otherwise pass while the connection could go either way.
     *
     * @param allowUnresolvable when true, a host that cannot be resolved is permitted. Used at
     *                          registration, where DNS is allowed to be temporarily broken and the
     *                          delivery-time check is the real gate.
     */
    public static Result checkDestination(String url, boolean allowUnresolvable) {
        Result syntax = checkUrlSyntax(url);
        if (!syntax.allowed()) {
            return syntax;
        }

        String host = URI.create(url).getHost();
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            return allowUnresolvable
                    ? Result.allow()
                    : Result.deny("host " + host + " could not be resolved");
        }

        for (InetAddress address : addresses) {
            String blocked = blockedReason(address);
            if (blocked != null) {
                return Result.deny("host " + host + " resolves to " + address.getHostAddress()
                        + " (" + blocked + ")");
            }
        }
        return Result.allow();
    }

    /** @return why this address is blocked, or null if it is acceptable. */
    public static String blockedReason(InetAddress address) {
        if (address.isLoopbackAddress()) {
            return "loopback";
        }
        if (address.isAnyLocalAddress()) {
            return "wildcard";
        }
        if (address.isLinkLocalAddress()) {
            // Covers 169.254.169.254, the cloud instance metadata service.
            return "link-local";
        }
        if (address.isSiteLocalAddress()) {
            return "private";
        }
        if (address.isMulticastAddress()) {
            return "multicast";
        }

        byte[] bytes = address.getAddress();

        if (address instanceof Inet4Address) {
            int first = bytes[0] & 0xFF;
            int second = bytes[1] & 0xFF;

            if (first == 0) {
                return "this-network";
            }
            // 100.64.0.0/10 — carrier-grade NAT. Not covered by isSiteLocalAddress().
            if (first == 100 && second >= 64 && second <= 127) {
                return "carrier-grade NAT";
            }
            // 198.18.0.0/15 — benchmarking range, routed internally in some networks.
            if (first == 198 && (second == 18 || second == 19)) {
                return "benchmarking";
            }
            if (first == 255) {
                return "broadcast";
            }
            return null;
        }

        if (address instanceof Inet6Address) {
            int first = bytes[0] & 0xFF;
            // fc00::/7 — unique local addresses, the IPv6 equivalent of a private range.
            if ((first & 0xFE) == 0xFC) {
                return "unique-local";
            }
            // An IPv4-mapped address such as ::ffff:127.0.0.1 must be judged by the IPv4 address it
            // encodes; the IPv6 predicates above do not see through the mapping.
            if (isIpv4Mapped(bytes)) {
                try {
                    InetAddress mapped = InetAddress.getByAddress(
                            new byte[]{bytes[12], bytes[13], bytes[14], bytes[15]});
                    String reason = blockedReason(mapped);
                    return reason == null ? null : "IPv4-mapped " + reason;
                } catch (UnknownHostException e) {
                    return "unparseable IPv4-mapped address";
                }
            }
            return null;
        }

        return "unknown address type";
    }

    private static boolean isIpv4Mapped(byte[] bytes) {
        if (bytes.length != 16) {
            return false;
        }
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return (bytes[10] & 0xFF) == 0xFF && (bytes[11] & 0xFF) == 0xFF;
    }
}
