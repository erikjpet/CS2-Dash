#!/usr/bin/env bash
# cs2dash v1.0 start script
# Usage: ./start.sh [port]  (default: 8080)

PORT=${1:-8080}
DIR="$(cd "$(dirname "$0")" && pwd)"

# Open firewall if needed
if command -v ufw &>/dev/null; then
  sudo ufw allow "$PORT"/tcp &>/dev/null 2>&1
fi
sudo iptables -C INPUT -p tcp --dport "$PORT" -j ACCEPT &>/dev/null 2>&1 || \
  sudo iptables -I INPUT -p tcp --dport "$PORT" -j ACCEPT &>/dev/null 2>&1

cd "$DIR"
python3 server.py "$PORT"
