# cs2dash — Internet Deployment Runbook

Target: a **home Linux server behind a consumer router**, reachable on the public
internet over HTTPS with a login screen. Architecture:

```
Internet  ──▶  Router (forward 80/443)  ──▶  Caddy (TLS + security headers)  ──▶  127.0.0.1:8080  server.py
```

The app now ships single-user login, loopback-only binding, request-size limits,
and no wildcard CORS. Caddy adds automatic HTTPS. Everything below runs on the
Linux server unless noted.

---

## 0. Prerequisites

- Python 3.8+ (`python3 --version`) — no pip packages needed, stdlib only.
- The app code at `/opt/cs2-dash` and your existing data dir (5 GB SQLite) at
  `/var/lib/cs2dash`. Adjust paths throughout if yours differ.
- sudo access; ability to log into your router admin page.

---

## 1. Create a service user and lay out files

```bash
sudo useradd --system --home /opt/cs2-dash --shell /usr/sbin/nologin cs2dash
sudo mkdir -p /opt/cs2-dash /var/lib/cs2dash /etc/cs2dash

# Put the app code in /opt/cs2-dash (server.py, index.html, tools/, etc.)
# Move/point your existing database dir to /var/lib/cs2dash:
#   cs2dash.sqlite, cs2dash.sqlite-wal, portfolios.json, settings.json ...

sudo chown -R cs2dash:cs2dash /opt/cs2-dash /var/lib/cs2dash
```

> **Checkpoint the WAL before copying the DB** so you copy a consistent file:
> `sqlite3 /path/cs2dash.sqlite "PRAGMA wal_checkpoint(TRUNCATE);"`

---

## 2. Set the login password

```bash
cd /opt/cs2-dash
python3 server.py --hash-password        # prompts twice, prints the hash line
```

Copy the printed `CS2DASH_AUTH_PASSWORD_HASH='...'` value into the env file next.

```bash
sudo cp deploy/cs2dash.env.example /etc/cs2dash/cs2dash.env
sudo nano /etc/cs2dash/cs2dash.env       # paste the hash, set STEAM_COOKIE, paths
sudo chown root:cs2dash /etc/cs2dash/cs2dash.env
sudo chmod 640 /etc/cs2dash/cs2dash.env  # secret: not world-readable
```

---

## 3. Install the systemd service

```bash
sudo cp deploy/cs2dash.service /etc/systemd/system/cs2dash.service
sudo systemctl daemon-reload
sudo systemctl enable --now cs2dash
systemctl status cs2dash                 # should be active (running)
curl -s http://127.0.0.1:8080/api/health # {"ok":true,...}
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/api/status  # 401 = auth working
```

If it refuses to start with "no password is configured", the hash in the env
file is missing/wrong. Logs: `journalctl -u cs2dash -e`.

---

## 4. Get a free hostname with DuckDNS (dynamic DNS)

Home connections usually have a **changing** public IP, so you need dynamic DNS.

1. Sign in at <https://www.duckdns.org> and create a subdomain, e.g.
   `cs2dash.duckdns.org`. Note your **token**.
2. Install an updater so the hostname always points at your current IP:

```bash
sudo tee /usr/local/bin/duckdns-update >/dev/null <<'EOF'
#!/bin/sh
curl -s "https://www.duckdns.org/update?domains=YOURSUB&token=YOURTOKEN&ip=" >/dev/null
EOF
sudo chmod +x /usr/local/bin/duckdns-update
# Run every 5 minutes:
( sudo crontab -l 2>/dev/null; echo "*/5 * * * * /usr/local/bin/duckdns-update" ) | sudo crontab -
/usr/local/bin/duckdns-update            # run once now
```

---

## 5. Forward ports on your router

In your router admin page, forward to the server's **LAN IP** (`ip a` to find it,
e.g. 192.168.0.180). Give the server a static DHCP lease so its LAN IP is stable.

| External port | Internal IP:port | Purpose |
| --- | --- | --- |
| 80  | 192.168.0.180:80  | Let's Encrypt HTTP challenge + redirect to HTTPS |
| 443 | 192.168.0.180:443 | HTTPS |

Do **not** forward 8080 — the app must stay loopback-only.

> Some ISPs use CGNAT (no real public IP) and block inbound 80/443. Test from
> off-network (phone on cellular). If unreachable — or if you'd rather not open
> router ports or expose your home IP — use the **Cloudflare Tunnel** path in
> [`DEPLOY-cloudflare-tunnel.md`](DEPLOY-cloudflare-tunnel.md) instead. It is
> outbound-only (no port forwarding, no DuckDNS, no Caddy) and replaces steps 4–7.

---

## 6. Install Caddy (TLS + reverse proxy)

```bash
# Debian/Ubuntu:
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install -y caddy

# Configure:
sudo cp /opt/cs2-dash/deploy/Caddyfile /etc/caddy/Caddyfile
sudo nano /etc/caddy/Caddyfile            # set your real duckdns hostname
sudo systemctl reload caddy
journalctl -u caddy -f                     # watch it obtain the certificate
```

---

## 7. Open the host firewall

```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
# 8080 stays closed to the outside — it's loopback-only anyway.
```

---

## 8. Verify end to end

From a device **off** your network (phone on cellular):

1. Visit `https://cs2dash.duckdns.org` → padlock (valid cert), login screen.
2. Wrong password is rejected; correct password loads the dashboard.
3. "Sign out" (top-right) returns you to the login screen.

Done — it's live.

---

## Operations

| Action | Command |
| --- | --- |
| Restart app | `sudo systemctl restart cs2dash` |
| App logs | `journalctl -u cs2dash -f` |
| Change password | `python3 server.py --hash-password`, update env file, restart |
| Update code | copy new `server.py`/`index.html`, `sudo systemctl restart cs2dash` |
| Backup | stop app, `PRAGMA wal_checkpoint(TRUNCATE)`, copy `/var/lib/cs2dash` |

## Security notes

- **Login gates everything.** All pages and API calls require a session cookie
  (`HttpOnly; SameSite=Strict; Secure`); unauthenticated API calls get `401`,
  page loads redirect to `/login`.
- **DELETE is owner-only.** Behind the proxy all requests appear to come from
  `127.0.0.1`, so the old "localhost-only DELETE" rule always passes — but DELETE
  now also requires login, so only you (the single account) can delete. Intended.
- **Keep the port private.** The app binds `127.0.0.1` (`CS2DASH_BIND`); never
  forward 8080 or set the bind to `0.0.0.0` on an internet-facing box.
- **Secrets** live only in `/etc/cs2dash/cs2dash.env` (chmod 640). Rotate the
  Steam cookie and password there; never commit them.
