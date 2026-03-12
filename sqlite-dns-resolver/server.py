#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
    DBResolver - Async DNS server backed by any SQLAlchemy-compatible database.
    Supports SQLite, MySQL, PostgreSQL via DATABASE_URL env var.

    Stack:
      - asyncio.DatagramProtocol  — UDP DNS server (no threads)
      - asyncio.start_server      — TCP DNS server (for large responses)
      - dnslib                    — DNS packet parsing/serialization only
      - sqlalchemy.ext.asyncio    — async DB queries
      - dns.asyncquery            — async upstream DNS forwarding (dnspython)
"""

import asyncio
import copy
import ipaddress
import logging
import os
import re
import time

import dns.asyncquery
import dns.message
import dns.rdatatype

from dnslib import DNSRecord, RR, RCODE, QTYPE

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# DATABASE_URL async driver formats:
#   SQLite:     sqlite+aiosqlite:////data.db
#   MySQL:      mysql+asyncmy://user:pass@host/db
#   PostgreSQL: postgresql+asyncpg://user:pass@host/db
_SQLITE_DEFAULT = f"sqlite+aiosqlite:///{os.getenv('SQLITE_DB', '/data.db')}"
DATABASE_URL    = os.getenv("DATABASE_URL", _SQLITE_DEFAULT)
SQL_QUERY       = os.getenv("SQL_QUERY", "SELECT dest_addr FROM lookup WHERE name IS NOT NULL AND name LIKE :name")
DNS_TTL         = int(os.getenv("DNS_TTL", "60"))
CACHE_SIZE      = int(os.getenv("CACHE_SIZE", "512"))
UPSTREAM_DNS    = os.getenv("UPSTREAM_DNS", "127.0.0.11")
UPSTREAM_PORT   = int(os.getenv("UPSTREAM_PORT", "53"))
LOOKUP_TIMEOUT  = float(os.getenv("LOOKUP_TIMEOUT", "3.0"))
LISTEN_ADDRESS  = os.getenv("LISTEN_ADDRESS", "")
LISTEN_PORT     = int(os.getenv("PORT", "53"))
ENABLE_TCP      = os.getenv("ENABLE_TCP", "1") not in ("0", "false", "no")

_SAFE_HOSTNAME_RE = re.compile(r'^[a-zA-Z0-9._-]{1,253}$')


def _validate_hostname(name: str) -> bool:
    return bool(_SAFE_HOSTNAME_RE.match(name))


def _validate_ipv4(addr: str) -> str | None:
    try:
        return str(ipaddress.IPv4Address(addr.strip()))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# TTL cache — no locks needed: asyncio is single-threaded cooperative,
# dict operations between await points cannot interleave.
# asyncio.Event used for stampede prevention instead of threading.Event.
# ---------------------------------------------------------------------------

class TTLCache:

    def __init__(self, ttl: int, maxsize: int):
        self.ttl      = ttl
        self.maxsize  = maxsize
        self._store: dict[tuple, tuple] = {}        # key -> (value, expires_at)
        self._inflight: dict[tuple, asyncio.Event] = {}

    def get(self, key: tuple):
        entry = self._store.get(key)
        if entry and time.monotonic() < entry[1]:
            return entry[0], True
        return None, False

    def set(self, key: tuple, value):
        if len(self._store) >= self.maxsize:
            now    = time.monotonic()
            victim = next((k for k, (_, exp) in self._store.items() if exp < now), None)
            if victim is None:
                victim = next(iter(self._store))
            del self._store[victim]
        self._store[key] = (value, time.monotonic() + self.ttl)

    def get_or_lock(self, key: tuple):
        """
        Cache hit  → (value, True,  None)   use value directly
        First miss → (None,  False, None)   caller must query, then finish_inflight()
        Later miss → (None,  False, event)  caller must await event, then re-get()
        """
        entry = self._store.get(key)
        if entry and time.monotonic() < entry[1]:
            return entry[0], True, None

        if key in self._inflight:
            return None, False, self._inflight[key]

        self._inflight[key] = asyncio.Event()
        return None, False, None

    def finish_inflight(self, key: tuple, value):
        self.set(key, value)
        ev = self._inflight.pop(key, None)
        if ev:
            ev.set()

    def invalidate(self, key: tuple = None):
        if key:
            self._store.pop(key, None)
        else:
            self._store.clear()


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

def _build_engine():
    is_sqlite = "sqlite" in DATABASE_URL
    if is_sqlite:
        return create_async_engine(
            DATABASE_URL,
            connect_args={"check_same_thread": False},
        )
    return create_async_engine(
        DATABASE_URL,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
    )


class DBResolver:

    def __init__(self):
        self.engine = _build_engine()
        self._cache = TTLCache(ttl=DNS_TTL, maxsize=CACHE_SIZE)

    async def setup(self):
        """Run once after the event loop starts — configure DB and create table if needed."""
        async with self.engine.begin() as conn:
            if "sqlite" in DATABASE_URL:
                await conn.execute(text("PRAGMA journal_mode=WAL"))
                await conn.execute(text("PRAGMA synchronous=NORMAL"))
                await conn.execute(text("PRAGMA cache_size=10000"))

            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS lookup (
                    host_id   INTEGER NOT NULL PRIMARY KEY,
                    name      VARCHAR(200),
                    dest_addr VARCHAR(200)
                )
            """))

        safe = DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL
        log.info("Database ready: %s", safe)

    async def _db_lookup(self, name: str) -> str | None:
        cache_key = (SQL_QUERY, name)

        ip, hit = self._cache.get(cache_key)
        if hit:
            return ip

        value, hit, ev = self._cache.get_or_lock(cache_key)
        if hit:
            return value

        if ev is not None:
            # Another coroutine is already fetching — wait for it
            try:
                await asyncio.wait_for(ev.wait(), timeout=LOOKUP_TIMEOUT + 1)
            except asyncio.TimeoutError:
                pass
            ip, hit = self._cache.get(cache_key)
            return ip if hit else None

        # We own the inflight slot
        ip = None
        try:
            async with self.engine.connect() as conn:
                row = (await conn.execute(text(SQL_QUERY), {"name": name})).fetchone()
            if row:
                ip = _validate_ipv4(row[0])
                if ip is None:
                    log.warning("DB non-IPv4 for %r: %r — ignored", name, row[0])
        except Exception as e:
            log.error("DB lookup error for %r: %s", name, e)

        self._cache.finish_inflight(cache_key, ip)
        return ip

    async def _upstream_lookup(self, name: str) -> str | None:
        cache_key = ("upstream", name)
        ip, hit = self._cache.get(cache_key)
        if hit:
            return ip

        ip = None
        try:
            q    = dns.message.make_query(name, dns.rdatatype.A)
            resp = await asyncio.wait_for(
                dns.asyncquery.udp(q, UPSTREAM_DNS, port=UPSTREAM_PORT),
                timeout=LOOKUP_TIMEOUT,
            )
            for rrset in resp.answer:
                for rdata in rrset:
                    if hasattr(rdata, "address"):
                        validated = _validate_ipv4(str(rdata.address))
                        if validated:
                            ip = validated
                            break
                if ip:
                    break
        except Exception as e:
            log.warning("Upstream DNS failed for %r via %s:%d: %s", name, UPSTREAM_DNS, UPSTREAM_PORT, e)

        # Cache hits and misses — prevents hammering upstream for unknown names
        self._cache.set(cache_key, ip)
        return ip

    async def resolve(self, request: DNSRecord) -> DNSRecord:
        reply = request.reply()
        qname = request.q.qname
        iname = b".".join(qname.label).decode()

        if not _validate_hostname(iname):
            log.warning("Rejected unsafe hostname %r", iname)
            reply.header.rcode = RCODE.REFUSED
            return reply

        ip = await self._db_lookup(iname)

        if ip is None:
            log.info("DB miss for %r, trying upstream %s", iname, UPSTREAM_DNS)
            ip = await self._upstream_lookup(iname)

        if ip:
            log.info("Resolved %r -> %s", iname, ip)
            for rr in RR.fromZone(f"{iname} {DNS_TTL} IN A {ip}"):
                a = copy.copy(rr)
                a.rname = qname
                reply.add_answer(a)
        else:
            log.warning("NXDOMAIN for %r", iname)
            reply.header.rcode = RCODE.NXDOMAIN

        return reply

    def invalidate_cache(self, name: str = None):
        self._cache.invalidate((SQL_QUERY, name) if name else None)


# ---------------------------------------------------------------------------
# UDP server
# ---------------------------------------------------------------------------

class UDPProtocol(asyncio.DatagramProtocol):

    def __init__(self, resolver: DBResolver):
        self.resolver  = resolver
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        asyncio.create_task(self._handle(data, addr))

    async def _handle(self, data: bytes, addr):
        try:
            request = DNSRecord.parse(data)
            reply   = await self.resolver.resolve(request)
            self.transport.sendto(reply.pack(), addr)
        except Exception as e:
            log.error("UDP handler error from %s: %s", addr, e)

    def error_received(self, exc):
        log.error("UDP socket error: %s", exc)


# ---------------------------------------------------------------------------
# TCP server — DNS-over-TCP uses a 2-byte big-endian length prefix per message
# ---------------------------------------------------------------------------

async def _handle_tcp_client(resolver: DBResolver, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    try:
        while True:
            header = await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
            length = int.from_bytes(header, "big")
            data   = await asyncio.wait_for(reader.readexactly(length), timeout=10.0)

            request  = DNSRecord.parse(data)
            reply    = await resolver.resolve(request)
            response = reply.pack()

            writer.write(len(response).to_bytes(2, "big") + response)
            await writer.drain()
    except asyncio.IncompleteReadError:
        pass  # client closed connection cleanly
    except asyncio.TimeoutError:
        log.debug("TCP timeout from %s", addr)
    except Exception as e:
        log.error("TCP handler error from %s: %s", addr, e)
    finally:
        writer.close()
        await writer.wait_closed()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    resolver = DBResolver()
    await resolver.setup()

    loop = asyncio.get_running_loop()

    udp_transport, _ = await loop.create_datagram_endpoint(
        lambda: UDPProtocol(resolver),
        local_addr=(LISTEN_ADDRESS, LISTEN_PORT),
    )
    log.info("UDP DNS listening on %s:%d", LISTEN_ADDRESS or "*", LISTEN_PORT)

    tcp_server = None
    if ENABLE_TCP:
        tcp_server = await asyncio.start_server(
            lambda r, w: _handle_tcp_client(resolver, r, w),
            host=LISTEN_ADDRESS or None,
            port=LISTEN_PORT,
        )
        log.info("TCP DNS listening on %s:%d", LISTEN_ADDRESS or "*", LISTEN_PORT)

    try:
        await asyncio.Event().wait()  # run forever
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        udp_transport.close()
        if tcp_server:
            tcp_server.close()
            await tcp_server.wait_closed()
        await resolver.engine.dispose()
        log.info("Server stopped.")


if __name__ == "__main__":
    asyncio.run(main())
