"""
DNS server integration tests.

Concurrency tests fire 50 simultaneous queries via asyncio to exercise the
cache stampede guard and the async event loop under load.
"""

import asyncio
import sys
import os

import dns.asyncquery
import dns.message
import dns.rdatatype
import dns.resolver
import dns.rcode
import pytest

# Import validators directly for unit tests (no server needed)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from server import _validate_hostname, _validate_ipv4, TTLCache

TEST_PORT = int(os.getenv("TEST_PORT", "5353"))


# ---------------------------------------------------------------------------
# Unit tests — no server required
# ---------------------------------------------------------------------------

class TestValidateHostname:
    def test_simple(self):
        assert _validate_hostname("example")
        assert _validate_hostname("my-host")
        assert _validate_hostname("sub.domain.com")
        assert _validate_hostname("host-123")

    def test_rejects_newline(self):
        assert not _validate_hostname("host\nevil.com 60 IN A 1.1.1.1")

    def test_rejects_space(self):
        assert not _validate_hostname("host name")

    def test_rejects_semicolon(self):
        assert not _validate_hostname("host;DROP TABLE lookup;")

    def test_rejects_too_long(self):
        assert not _validate_hostname("a" * 254)

    def test_rejects_empty(self):
        assert not _validate_hostname("")


class TestValidateIPv4:
    def test_valid(self):
        assert _validate_ipv4("1.2.3.4") == "1.2.3.4"
        assert _validate_ipv4("  10.0.0.1  ") == "10.0.0.1"

    def test_rejects_ipv6(self):
        assert _validate_ipv4("::1") is None

    def test_rejects_garbage(self):
        assert _validate_ipv4("not-an-ip") is None

    def test_rejects_injection(self):
        assert _validate_ipv4("1.2.3.4\nevil.com 60 IN A 9.9.9.9") is None


class TestTTLCache:
    def test_set_and_get(self):
        c = TTLCache(ttl=60, maxsize=10)
        c.set(("q", "foo"), "1.2.3.4")
        val, hit = c.get(("q", "foo"))
        assert hit and val == "1.2.3.4"

    def test_miss(self):
        c = TTLCache(ttl=60, maxsize=10)
        _, hit = c.get(("q", "missing"))
        assert not hit

    def test_maxsize_eviction(self):
        c = TTLCache(ttl=60, maxsize=3)
        for i in range(4):
            c.set(("q", f"h{i}"), str(i))
        assert len(c._store) == 3

    def test_invalidate_single(self):
        c = TTLCache(ttl=60, maxsize=10)
        c.set(("q", "foo"), "1.1.1.1")
        c.invalidate(("q", "foo"))
        _, hit = c.get(("q", "foo"))
        assert not hit

    def test_invalidate_all(self):
        c = TTLCache(ttl=60, maxsize=10)
        c.set(("q", "a"), "1.1.1.1")
        c.set(("q", "b"), "2.2.2.2")
        c.invalidate()
        assert len(c._store) == 0


# ---------------------------------------------------------------------------
# Helpers for integration tests
# ---------------------------------------------------------------------------

def sync_resolve(host: str, port: int) -> str:
    """Synchronous A-record lookup via dnspython resolver."""
    r = dns.resolver.Resolver(configure=False)
    r.nameservers = ["127.0.0.1"]
    r.port = port
    r.lifetime = 5.0
    answers = r.resolve(host, "A")
    return str(answers[0])


async def async_resolve(host: str, port: int, timeout: float = 5.0) -> dns.message.Message:
    """Async A-record lookup, returns the raw DNS response."""
    q = dns.message.make_query(host, dns.rdatatype.A)
    return await dns.asyncquery.udp(q, "127.0.0.1", port=port, timeout=timeout)


def response_ip(msg: dns.message.Message) -> str | None:
    for rrset in msg.answer:
        for rdata in rrset:
            if hasattr(rdata, "address"):
                return str(rdata.address)
    return None


def response_rcode(msg: dns.message.Message) -> int:
    return msg.rcode()


# ---------------------------------------------------------------------------
# Integration tests — require the dns_server fixture
# ---------------------------------------------------------------------------

class TestBasicResolution:
    def test_known_host(self, dns_server):
        assert sync_resolve("test-host", dns_server["port"]) == "1.2.3.4"

    def test_another_known_host(self, dns_server):
        assert sync_resolve("another-host", dns_server["port"]) == "5.6.7.8"

    def test_mail_host(self, dns_server):
        assert sync_resolve("mail-host", dns_server["port"]) == "10.10.10.1"

    def test_nxdomain_for_unknown(self, dns_server):
        with pytest.raises(dns.resolver.NXDOMAIN):
            sync_resolve("totally-unknown-xyz-123", dns_server["port"])

    def test_cache_returns_same_ip(self, dns_server):
        ip1 = sync_resolve("test-host", dns_server["port"])
        ip2 = sync_resolve("test-host", dns_server["port"])
        assert ip1 == ip2 == "1.2.3.4"


class TestInputValidation:
    @pytest.mark.asyncio
    async def test_refused_for_unsafe_hostname(self, dns_server):
        # Craft a raw query bypassing dnspython's own validation
        from dnslib import DNSRecord, QTYPE
        import socket

        raw = DNSRecord.question("evil\x00host").pack()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)
        try:
            sock.sendto(raw, ("127.0.0.1", dns_server["port"]))
            data, _ = sock.recvfrom(512)
            resp = DNSRecord.parse(data)
            # Server should respond with REFUSED or NXDOMAIN, never an A record
            from dnslib import RCODE
            assert resp.header.rcode in (RCODE.REFUSED, RCODE.NXDOMAIN)
        finally:
            sock.close()


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_50_concurrent_same_host(self, dns_server):
        """50 simultaneous queries for the same host exercise the cache stampede guard."""
        port = dns_server["port"]
        results = await asyncio.gather(
            *[async_resolve("test-host", port) for _ in range(50)]
        )
        failed = [i for i, r in enumerate(results) if response_ip(r) != "1.2.3.4"]
        assert not failed, f"Queries {failed} returned wrong/no IP"

    @pytest.mark.asyncio
    async def test_50_concurrent_different_hosts(self, dns_server):
        """50 simultaneous queries each for a distinct host — all should resolve correctly."""
        port = dns_server["port"]

        async def check(i):
            msg = await async_resolve(f"host-{i}", port)
            expected = f"10.0.{i // 256}.{i % 256}"
            got = response_ip(msg)
            return i, expected, got

        results = await asyncio.gather(*[check(i) for i in range(1, 51)])
        wrong = [(i, exp, got) for i, exp, got in results if got != exp]
        assert not wrong, f"Incorrect resolutions: {wrong}"

    @pytest.mark.asyncio
    async def test_50_concurrent_nxdomain(self, dns_server):
        """50 simultaneous queries for unknown names should all return NXDOMAIN."""
        port = dns_server["port"]
        results = await asyncio.gather(
            *[async_resolve(f"no-such-host-{i}", port) for i in range(50)]
        )
        non_nxdomain = [
            i for i, r in enumerate(results)
            if response_rcode(r) != dns.rcode.NXDOMAIN
        ]
        assert not non_nxdomain, f"Queries {non_nxdomain} did not return NXDOMAIN"
