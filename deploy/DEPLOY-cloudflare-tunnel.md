# cs2dash — Cloudflare Tunnel Deployment (no port forwarding, CGNAT-proof)

Use this instead of the router-port-forward + Caddy path in [`DEPLOY.md`](DEPLOY.md) when:

- your ISP uses **CGNAT** (no real public IP) and blocks inbound 80/443, or
- you don't want to open any router ports, or
- you'd rather not expose your home IP address.

`cloudflared` makes an **outbound-only** connection from your server to Cloudflare's
edge, which then serves your hostname over HTTPS and proxies requests down the
tunnel to the app on loopback. No inbound ports, no dynamic DNS, no Caddy.

```
Internet ──▶ Cloudflare edge (TLS)  ══tunnel══  cloudflared (server)  ──▶  127.0.0.1:8080  server.py
                                   (outbound only, no open ports)
```

The app still runs its own login, so it stays protected exactly as in the main
runbook. Steps 1–3 of `DEPLOY.md` (service user, password/env, systemd unit) are
identical — do those first, then replace `DEPLOY.md` steps 4–7 with the below.

---

## Prerequisites

- The cs2dash systemd service is installed and healthy on the server:
  `curl -s http://127.0.0.1:8080/api/health` returns `{"ok":true,...}` and
  `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/api/status` returns `401`.
- A **free Cloudflare account** with a domain added to it (Cloudflare must be the
  domain's authoritative DNS). Any cheap domain works; you'll use a subdomain such
  as `cs2dash.yourdomain.com`.
- Keep `CS2DASH_COOKIE_SECURE=1` and `CS2DASH_BIND=127.0.0.1` in the env file — the
  browser↔Cloudflare leg is HTTPS, so the secure cookie works.

> No domain on Cloudflare yet? For a quick throwaway test only you can run
> `cloudflared tunnel --url http://127.0.0.1:8080`, which prints a random
> `*.trycloudflare.com` URL. Do not use quick tunnels for anything permanent.

---

## 1. Install cloudflared on the server

```bash
# Debian/Ubuntu (amd64):
curl -fsSL https://pkg.cloudflare.com/cloudflare-main.gpg | sudo tee /usr/share/keyrings/cloudflare-main.gpg >/dev/null
echo "deb [signed-by=/usr/share/keyrings/cloudflare-main.gpg] https://pkg.cloudflare.com/cloudflared any main" | sudo tee /etc/apt/sources.list.d/cloudflared.list
sudo apt update && sudo apt install -y cloudflared
cloudflared --version
```

## 2. Authenticate and create the tunnel

```bash
cloudflared tunnel login          # opens a browser link; pick your domain to authorize
cloudflared tunnel create cs2dash # creates the tunnel + a credentials JSON under ~/.cloudflared/
cloudflared tunnel list           # note the Tunnel ID
```

## 3. Route your hostname to the tunnel

```bash
cloudflared tunnel route dns cs2dash cs2dash.yourdomain.com
```

This creates a proxied CNAME in Cloudflare DNS automatically.

## 4. Write the tunnel config

Create `/etc/cloudflared/config.yml` (run cloudflared as root/service so it can
read it). Replace the tunnel ID and credentials filename with yours:

```yaml
tunnel: <TUNNEL-ID>
credentials-file: /etc/cloudflared/<TUNNEL-ID>.json

ingress:
  - hostname: cs2dash.yourdomain.com
    service: http://127.0.0.1:8080
  - service: http_status:404
```

Move the credentials file that `tunnel create` produced into `/etc/cloudflared/`
and lock it down:

```bash
sudo mkdir -p /etc/cloudflared
sudo cp ~/.cloudflared/<TUNNEL-ID>.json /etc/cloudflared/
sudo chmod 600 /etc/cloudflared/<TUNNEL-ID>.json
```

## 5. Run the tunnel as a service (start on boot)

```bash
sudo cloudflared service install
sudo systemctl enable --now cloudflared
systemctl status cloudflared
journalctl -u cloudflared -f       # watch it connect to the edge
```

## 6. Verify end to end

From any device (even on your home network): open `https://cs2dash.yourdomain.com`
→ valid Cloudflare cert, cs2dash login screen. Sign in and confirm the dashboard
loads and "Sign out" works.

---

## Notes

- **No firewall changes and no router port forwarding are needed.** The tunnel is
  outbound-only. You can leave inbound 80/443 closed.
- **Skip DuckDNS and Caddy entirely** on this path — Cloudflare provides the
  hostname and TLS.
- **Optional second factor:** add a **Cloudflare Access** policy (Zero Trust →
  Access → Applications) in front of `cs2dash.yourdomain.com` to require Google/
  email-OTP login at the edge before requests ever reach the app. This layers on
  top of the app's own login.
- **DELETE is owner-only**, same as the proxy path: cloudflared connects from
  `127.0.0.1`, so the app's localhost-only DELETE rule always passes, but DELETE
  also requires app login, so only the single account can delete.
- **Cloudflare proxy body limit:** the free plan caps request bodies around 100 MB,
  which is well above the app's own 64 MB cap; large portfolio imports are fine.
