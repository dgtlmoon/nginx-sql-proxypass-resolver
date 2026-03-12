import os
import sqlite3
import subprocess
import sys
import time

import dns.resolver
import pytest

TEST_PORT = int(os.getenv("TEST_PORT", "5353"))
SERVER_PY = os.path.join(os.path.dirname(__file__), "..", "server.py")

# Test entries inserted into the DB
TEST_RECORDS = {
    "test-host":    "1.2.3.4",
    "another-host": "5.6.7.8",
    "mail-host":    "10.10.10.1",
}
# 50 extra hosts for concurrent load tests
CONCURRENT_RECORDS = {f"host-{i}": f"10.0.{i // 256}.{i % 256}" for i in range(1, 51)}


@pytest.fixture(scope="session")
def test_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("dns") / "test.db"
    # Table is created by the server on startup (CREATE TABLE IF NOT EXISTS).
    # We only need to populate the rows.
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE lookup (
            host_id   INTEGER NOT NULL PRIMARY KEY,
            name      VARCHAR(200),
            dest_addr VARCHAR(200)
        )
    """)
    all_records = {**TEST_RECORDS, **CONCURRENT_RECORDS}
    for i, (name, addr) in enumerate(all_records.items(), start=1):
        conn.execute("INSERT INTO lookup VALUES (?, ?, ?)", (i, name, addr))
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture(scope="session")
def dns_server(test_db):
    env = os.environ.copy()
    env.update({
        "DATABASE_URL": f"sqlite+aiosqlite:///{test_db}",
        "PORT":         str(TEST_PORT),
        "ENABLE_TCP":   "0",
        "DNS_TTL":      "10",
        "UPSTREAM_DNS": "8.8.8.8",
        "UPSTREAM_PORT": "53",
        "LOOKUP_TIMEOUT": "3.0",
    })

    proc = subprocess.Popen(
        [sys.executable, os.path.abspath(SERVER_PY)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    # Poll until server answers or timeout
    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = ["127.0.0.1"]
    resolver.port = TEST_PORT
    resolver.lifetime = 2.0

    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out, _ = proc.communicate()
            raise RuntimeError(f"DNS server exited early:\n{out.decode()}")
        try:
            resolver.resolve("test-host", "A")
            break
        except Exception:
            time.sleep(0.3)
    else:
        proc.kill()
        out, _ = proc.communicate()
        raise RuntimeError(f"DNS server never became ready:\n{out.decode()}")

    yield {"host": "127.0.0.1", "port": TEST_PORT}

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
