#!/usr/bin/env bash
# Quick smoke test — starts the server, fires 50 concurrent dig queries, checks results.
# Requires: python3, dig, sqlite3
# Usage: ./test.sh [port]

set -euo pipefail

PORT="${1:-5353}"
DB=$(mktemp /tmp/dns-test-XXXXXX.db)
PASS=0
FAIL=0
SERVER_PID=""

cleanup() {
    [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
    rm -f "$DB"
}
trap cleanup EXIT

# ---- create test database ----
sqlite3 "$DB" <<'SQL'
CREATE TABLE lookup (
    host_id   INTEGER NOT NULL PRIMARY KEY,
    name      VARCHAR(200),
    dest_addr VARCHAR(200)
);
INSERT INTO lookup VALUES (1, 'test-host',    '1.2.3.4');
INSERT INTO lookup VALUES (2, 'another-host', '5.6.7.8');
INSERT INTO lookup VALUES (3, 'mail-host',    '10.10.10.1');
SQL
echo "[setup] Test DB created at $DB"

# ---- start server ----
DATABASE_URL="sqlite+aiosqlite:///$DB" \
PORT="$PORT" \
ENABLE_TCP=0 \
UPSTREAM_DNS=8.8.8.8 \
    python3 "$(dirname "$0")/server.py" &
SERVER_PID=$!
echo "[setup] Server PID=$SERVER_PID on port $PORT"

# ---- wait for server to be ready ----
echo -n "[setup] Waiting for server"
for i in $(seq 1 30); do
    if dig @127.0.0.1 -p "$PORT" test-host A +short +time=1 +tries=1 2>/dev/null | grep -q '1.2.3.4'; then
        echo " ready."
        break
    fi
    echo -n "."
    sleep 0.5
done

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo " [FAIL] Server process died."
    exit 1
fi

# ---- helper ----
check() {
    local label="$1" host="$2" expected="$3"
    local got
    got=$(dig @127.0.0.1 -p "$PORT" "$host" A +short +time=2 +tries=1 2>/dev/null || true)
    if [[ "$got" == "$expected" ]]; then
        echo "  [PASS] $label: $host -> $got"
        ((++PASS))
    else
        echo "  [FAIL] $label: $host -> expected '$expected', got '$got'"
        ((++FAIL))
    fi
}

check_nxdomain() {
    local label="$1" host="$2"
    local rc
    dig @127.0.0.1 -p "$PORT" "$host" A +time=2 +tries=1 2>/dev/null | grep -q "NXDOMAIN" && rc=0 || rc=1
    if [[ $rc -eq 0 ]]; then
        echo "  [PASS] $label: $host -> NXDOMAIN"
        ((++PASS))
    else
        echo "  [FAIL] $label: $host expected NXDOMAIN"
        ((++FAIL))
    fi
}

# ---- basic resolution ----
echo ""
echo "=== Basic resolution ==="
check "known host"    "test-host"    "1.2.3.4"
check "known host 2"  "another-host" "5.6.7.8"
check "known host 3"  "mail-host"    "10.10.10.1"
check_nxdomain "unknown host" "no-such-host-xyz"

# ---- 50 concurrent queries (same host) ----
echo ""
echo "=== 50 concurrent queries (same host) ==="
tmpdir=$(mktemp -d)
for i in $(seq 1 50); do
    dig @127.0.0.1 -p "$PORT" test-host A +short +time=3 +tries=1 \
        > "$tmpdir/result-$i" 2>/dev/null &
done
wait
concurrent_pass=0
concurrent_fail=0
for i in $(seq 1 50); do
    got=$(cat "$tmpdir/result-$i")
    if [[ "$got" == "1.2.3.4" ]]; then
        ((++concurrent_pass))
    else
        echo "  [FAIL] concurrent query $i: got '$got'"
        ((++concurrent_fail))
    fi
done
rm -rf "$tmpdir"
echo "  $concurrent_pass/50 passed"
((PASS += concurrent_pass))
((FAIL += concurrent_fail))

# ---- summary ----
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]]
