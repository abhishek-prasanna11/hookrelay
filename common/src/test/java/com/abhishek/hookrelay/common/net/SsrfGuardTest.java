package com.abhishek.hookrelay.common.net;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

class SsrfGuardTest {

    private static InetAddress address(String literal) throws UnknownHostException {
        return InetAddress.getByName(literal);
    }

    // ---- address policy -----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
            "127.0.0.1",            // loopback
            "127.1.2.3",            // the whole 127/8 range, not just .0.1
            "0.0.0.0",              // wildcard
            "10.0.0.1",             // private
            "172.16.5.4",           // private
            "172.31.255.255",       // private, top of the range
            "192.168.1.1",          // private
            "169.254.1.1",          // link-local
            "169.254.169.254",      // cloud instance metadata — the one that leaks IAM credentials
            "100.64.0.1",           // carrier-grade NAT
            "100.127.255.255",      // carrier-grade NAT, top of the range
            "198.18.0.1",           // benchmarking range
            "224.0.0.1",            // multicast
            "255.255.255.255"       // broadcast
    })
    @DisplayName("internal IPv4 addresses are blocked")
    void blocksInternalIpv4(String literal) throws Exception {
        assertThat(SsrfGuard.blockedReason(address(literal)))
                .as("%s must be blocked", literal)
                .isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",
            "172.32.0.1",           // just outside 172.16/12
            "100.128.0.1",          // just outside 100.64/10
            "198.20.0.1"            // just outside 198.18/15
    })
    @DisplayName("public IPv4 addresses are allowed")
    void allowsPublicIpv4(String literal) throws Exception {
        assertThat(SsrfGuard.blockedReason(address(literal)))
                .as("%s must be allowed", literal)
                .isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "::1",                  // loopback
            "fe80::1",              // link-local
            "fc00::1",              // unique-local
            "fd12:3456::1",         // unique-local
            "ff02::1"               // multicast
    })
    @DisplayName("internal IPv6 addresses are blocked")
    void blocksInternalIpv6(String literal) throws Exception {
        assertThat(SsrfGuard.blockedReason(address(literal)))
                .as("%s must be blocked", literal)
                .isNotNull();
    }

    @Test
    @DisplayName("an IPv4-mapped IPv6 address is judged by the IPv4 address it encodes")
    void blocksIpv4MappedIpv6() throws Exception {
        // ::ffff:127.0.0.1 reaches loopback while looking like an IPv6 address. The IPv6 predicates
        // do not see through the mapping, so it has to be decoded explicitly.
        byte[] mappedLoopback = new byte[16];
        mappedLoopback[10] = (byte) 0xFF;
        mappedLoopback[11] = (byte) 0xFF;
        mappedLoopback[12] = 127;
        mappedLoopback[15] = 1;

        assertThat(SsrfGuard.blockedReason(InetAddress.getByAddress(mappedLoopback)))
                .isNotNull();
    }

    @Test
    @DisplayName("public IPv6 is allowed")
    void allowsPublicIpv6() throws Exception {
        assertThat(SsrfGuard.blockedReason(address("2606:4700:4700::1111"))).isNull();
    }

    // ---- url syntax ---------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
            "file:///etc/passwd",
            "gopher://internal/",
            "ftp://internal/",
            "/relative/path",
            "not a url at all"
    })
    @DisplayName("non-http schemes and malformed urls are rejected")
    void rejectsBadSchemes(String url) {
        assertThat(SsrfGuard.checkUrlSyntax(url).allowed()).as("%s", url).isFalse();
    }

    @Test
    @DisplayName("userinfo is rejected — it is a standard way to confuse host parsing")
    void rejectsUserinfo() {
        // Parsers disagree about which side of the @ is the host, so a URL that a validator reads as
        // "evil.example.com" can be fetched as "internal-host".
        assertThat(SsrfGuard.checkUrlSyntax("http://internal-host@evil.example.com/").allowed()).isFalse();
        assertThat(SsrfGuard.checkUrlSyntax("https://user:pass@example.com/hook").allowed()).isFalse();
    }

    @Test
    @DisplayName("ordinary https urls pass syntax validation")
    void allowsNormalUrls() {
        assertThat(SsrfGuard.checkUrlSyntax("https://example.com/hook").allowed()).isTrue();
        assertThat(SsrfGuard.checkUrlSyntax("http://example.com:8080/a/b?c=d").allowed()).isTrue();
    }

    @Test
    @DisplayName("a null or blank url is rejected rather than throwing")
    void rejectsMissingUrl() {
        assertThat(SsrfGuard.checkUrlSyntax(null).allowed()).isFalse();
        assertThat(SsrfGuard.checkUrlSyntax("  ").allowed()).isFalse();
    }

    // ---- resolution ---------------------------------------------------------------------------

    @Test
    @DisplayName("a literal loopback destination is denied, with the reason named")
    void deniesLoopbackDestination() {
        SsrfGuard.Result result = SsrfGuard.checkDestination("http://127.0.0.1:9000/hook", false);

        assertThat(result.allowed()).isFalse();
        assertThat(result.reason()).contains("127.0.0.1").contains("loopback");
    }

    @Test
    @DisplayName("the cloud metadata address is denied")
    void deniesMetadataService() {
        SsrfGuard.Result result = SsrfGuard.checkDestination(
                "http://169.254.169.254/latest/meta-data/", false);

        assertThat(result.allowed()).isFalse();
        assertThat(result.reason()).contains("link-local");
    }

    @Test
    @DisplayName("localhost is denied by its resolved address, not by its name")
    void deniesLocalhostByAddress() {
        // The check never looks at the hostname: "evil.example.com" resolving to 127.0.0.1 has to be
        // caught the same way, and only address-based checking does that.
        assertThat(SsrfGuard.checkDestination("http://localhost:5432/", false).allowed()).isFalse();
    }

    @Test
    @DisplayName("an unresolvable host is denied at delivery time and allowed at registration")
    void unresolvableHostDependsOnContext() {
        String url = "http://does-not-exist.invalid/hook";

        assertThat(SsrfGuard.checkDestination(url, false).allowed())
                .as("delivery time: cannot verify, so refuse")
                .isFalse();
        assertThat(SsrfGuard.checkDestination(url, true).allowed())
                .as("registration: DNS is allowed to be temporarily broken")
                .isTrue();
    }
}
