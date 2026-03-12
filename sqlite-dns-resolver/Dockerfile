FROM python:3.10-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Grant the Python binary the capability to bind to privileged ports (<1024)
# so the server can listen on port 53 without running as root.
# setcap must be applied to the real binary, not the symlink.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libcap2-bin \
    && setcap 'cap_net_bind_service=+ep' /usr/local/bin/python3.10 \
    && apt-get purge -y libcap2-bin \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — no shell, no home directory needed
RUN useradd -r -u 1001 -s /sbin/nologin dnsuser

COPY . .

ENV PYTHONUNBUFFERED=1

USER dnsuser

CMD ["/usr/local/bin/python3", "./server.py"]
