# Experimental DNS server to act as a `proxy_pass` `resolver` for nginx to bounce your requests to depending on SQLite lookup data

Experimental Python based DNS server that lets you use SQLite as a nginx proxy_pass resolver that performs an SQLite query to decide where to resolve/pass proxy_pass the request to.

( Asks a SQLite DB what the real IP for a lookup should be and returns that so `proxy_pass` can use it )

Based on https://github.com/paulc/dnslib/blob/master/dnslib/fixedresolver.py


`docker-compose up`

Then

- http://127.0.0.1/google Lookup "google" in the SQLite then resolve to an IP that hopefully still works and proxy_pass you there
- http://127.0.0.1/test-container Fallback example, key doesnt exist so we fallback to using the existing defined docker hosts/containers

### Configuration

All settings are environment variables:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:////data.db` | SQLAlchemy connection URL. Supports SQLite, MySQL, PostgreSQL. |
| `SQL_QUERY` | `SELECT dest_addr FROM lookup WHERE name IS NOT NULL AND name LIKE :name` | Query to resolve a name. Must use `:name` as the named parameter. |
| `DNS_TTL` | `60` | TTL (seconds) for DNS responses and cache entries. |
| `CACHE_SIZE` | `512` | Max number of cached entries (LRU eviction). |
| `UPSTREAM_DNS` | `127.0.0.11` | Fallback DNS server for names not found in the DB (Docker embedded DNS). |
| `UPSTREAM_PORT` | `53` | Port for the upstream DNS server. |
| `LOOKUP_TIMEOUT` | `3.0` | Timeout in seconds for upstream DNS queries. |
#### Backend examples

```
# SQLite (default)
DATABASE_URL=sqlite+aiosqlite:////data.db

# MySQL
DATABASE_URL=mysql+asyncmy://user:pass@mysql-host/dbname

# PostgreSQL
DATABASE_URL=postgresql+asyncpg://user:pass@pg-host/dbname
```

Install the matching async driver via `requirements.txt` (see comments in that file).

### See
- http://nginx.org/en/docs/http/ngx_http_core_module.html#resolver
- https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls

### Note

Use at own risk, may or may not scale, may or may not rename your dog and set your house on fire.
