#!/usr/bin/env bash
# cs2dash local bootstrap/start script.
# Usage:
#   ./start.sh [port]
#   ./start.sh --setup [port]      # recreate local cs2dash.env

set -euo pipefail

APP_NAME="cs2dash"
DEFAULT_PORT="8080"
PORT="$DEFAULT_PORT"
FORCE_SETUP="0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${CS2DASH_ENV_FILE:-$SCRIPT_DIR/cs2dash.env}"

usage() {
  cat <<EOF
Usage: ./start.sh [options] [port]

Options:
  --setup           Recreate the local ignored cs2dash.env file.
  --env-file FILE   Load/write a different env file.
  -h, --help        Show this help.

The script creates an ignored local env file, asks for the required login
password when it is missing, records local pricing settings, then starts
server.py. Provider prices use CSGO Trader snapshots; Steam chart history is
more reliable when STEAM_COOKIE is configured.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

is_interactive() {
  [ -t 0 ] && [ -t 1 ]
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --setup)
      FORCE_SETUP="1"
      shift
      ;;
    --env-file)
      shift
      [ "$#" -gt 0 ] || die "--env-file needs a path"
      ENV_FILE="$1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      PORT="$1"
      shift
      ;;
  esac
done

case "$PORT" in
  ''|*[!0-9]*) die "port must be a number, got '$PORT'" ;;
esac

if command -v python3 >/dev/null 2>&1; then
  PYTHON=(python3)
elif command -v python >/dev/null 2>&1; then
  PYTHON=(python)
elif command -v py >/dev/null 2>&1; then
  PYTHON=(py -3)
else
  die "Python 3.8+ is required, but python3/python/py was not found"
fi

trim_outer_quotes() {
  local value="$1"
  if [[ "$value" == \"*\" && "$value" == *\" ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s' "$value"
}

load_env_file() {
  [ -f "$ENV_FILE" ] || return 0

  local line key value
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    case "$line" in
      ''|\#*) continue ;;
      *=*) ;;
      *) continue ;;
    esac

    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="$(trim_outer_quotes "$value")"

    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    export "$key=$value"
  done < "$ENV_FILE"
}

write_env_value() {
  local key="$1"
  local value="$2"
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "$key contains a newline"

  mkdir -p "$(dirname "$ENV_FILE")"
  touch "$ENV_FILE"
  chmod 600 "$ENV_FILE" 2>/dev/null || true

  local tmp="${ENV_FILE}.tmp.$$"
  awk -v key="$key" -v value="$value" '
    BEGIN { done = 0 }
    $0 ~ "^[[:space:]]*" key "=" {
      print key "=" value
      done = 1
      next
    }
    { print }
    END {
      if (!done) {
        print key "=" value
      }
    }
  ' "$ENV_FILE" > "$tmp"
  mv "$tmp" "$ENV_FILE"
  export "$key=$value"
}

prompt_default() {
  local prompt="$1"
  local default="$2"
  local answer
  if [ -n "$default" ]; then
    read -r -p "$prompt [$default]: " answer
    printf '%s' "${answer:-$default}"
  else
    read -r -p "$prompt: " answer
    printf '%s' "$answer"
  fi
}

prompt_password_hash() {
  "${PYTHON[@]}" - "$SCRIPT_DIR" <<'PY'
import getpass
import importlib.util
import os
import sys

root = sys.argv[1]
spec = importlib.util.spec_from_file_location("cs2dash_server", os.path.join(root, "server.py"))
server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(server)

pw1 = getpass.getpass("New cs2dash login password: ")
pw2 = getpass.getpass("Confirm cs2dash login password: ")
if not pw1:
    raise SystemExit("Password cannot be empty.")
if pw1 != pw2:
    raise SystemExit("Passwords did not match.")
print(server.hash_password(pw1))
PY
}

auth_disabled() {
  case "${CS2DASH_AUTH_DISABLE:-}" in
    1|true|TRUE|yes|YES) return 0 ;;
    *) return 1 ;;
  esac
}

auth_hash_missing() {
  [ -n "${CS2DASH_AUTH_PASSWORD:-}" ] && return 1
  case "${CS2DASH_AUTH_PASSWORD_HASH:-}" in
    ''|*REPLACE_ME*) return 0 ;;
    *) return 1 ;;
  esac
}

create_env_file() {
  if ! is_interactive; then
    die "missing $ENV_FILE; run ./start.sh from an interactive terminal or set CS2DASH_AUTH_PASSWORD_HASH"
  fi

  if [ -f "$ENV_FILE" ]; then
    local backup="${ENV_FILE}.bak.$(date +%Y%m%d%H%M%S)"
    cp "$ENV_FILE" "$backup"
    echo "Backed up existing env file to $backup"
  fi

  : > "$ENV_FILE"
  chmod 600 "$ENV_FILE" 2>/dev/null || true

  echo
  echo "First-time $APP_NAME setup"
  echo "This writes local settings to: $ENV_FILE"
  echo "The file is ignored by Git."
  echo

  local user bind data_dir cookie_secure auto_refresh resume_tasks steam_cookie auth_hash price_base source_priority bulk_source
  user="$(prompt_default "Login username" "${CS2DASH_AUTH_USER:-admin}")"
  bind="$(prompt_default "Bind address" "${CS2DASH_BIND:-127.0.0.1}")"
  data_dir="$(prompt_default "Data directory" "${CS2DASH_DATA_DIR:-data}")"
  cookie_secure="$(prompt_default "Use HTTPS-only cookies? Use 0 for local http:// testing" "${CS2DASH_COOKIE_SECURE:-0}")"

  write_env_value "CS2DASH_AUTH_USER" "$user"
  write_env_value "CS2DASH_BIND" "$bind"
  write_env_value "CS2DASH_DATA_DIR" "$data_dir"
  write_env_value "CS2DASH_COOKIE_SECURE" "$cookie_secure"

  auth_hash="$(prompt_password_hash)"
  write_env_value "CS2DASH_AUTH_PASSWORD_HASH" "$auth_hash"

  echo
  echo "Pricing setup"
  echo "Provider prices come from CSGO Trader snapshots. Steam chart history is separate"
  echo "and works best with a Steam login cookie."
  echo

  price_base="$(prompt_default "CSGO Trader price snapshot base URL" "${CSGOTRADER_PRICE_BASE:-https://prices.csgotrader.app/latest}")"
  source_priority="$(prompt_default "Provider source priority" "${MARKET_SOURCE_PRIORITY:-steam,csfloat,buff163,youpin,skinport}")"
  bulk_source="$(prompt_default "Bulk price source mode" "${MARKET_BULK_SOURCE:-any}")"
  auto_refresh="$(prompt_default "Enable automatic background market refresh? Use 0 while importing locally" "${AUTO_MARKET_REFRESH:-0}")"
  resume_tasks="$(prompt_default "Resume interrupted background data fills on startup? Use 0 while debugging locally" "${DATA_TASK_RESUME_ENABLED:-0}")"

  write_env_value "CSGOTRADER_PRICE_BASE" "$price_base"
  write_env_value "MARKET_SOURCE_PRIORITY" "$source_priority"
  write_env_value "MARKET_BULK_SOURCE" "$bulk_source"
  write_env_value "AUTO_MARKET_REFRESH" "$auto_refresh"
  write_env_value "DATA_TASK_RESUME_ENABLED" "$resume_tasks"

  steam_cookie="$(prompt_default "Steam cookie for chart history (optional, blank to skip)" "")"
  if [ -n "$steam_cookie" ]; then
    write_env_value "STEAM_COOKIE" "$steam_cookie"
    write_env_value "STEAM_MIN_INTERVAL" "${STEAM_MIN_INTERVAL:-2.0}"
    write_env_value "STEAM_HISTORY_FETCH_TIMEOUT" "${STEAM_HISTORY_FETCH_TIMEOUT:-10}"
  fi

  echo
}

ensure_default() {
  local key="$1"
  local value="$2"
  if [ -z "${!key:-}" ]; then
    write_env_value "$key" "$value"
  fi
}

cd "$SCRIPT_DIR"

if [ "$FORCE_SETUP" = "1" ]; then
  create_env_file
fi

load_env_file

if [ ! -f "$ENV_FILE" ] && ! auth_disabled && auth_hash_missing; then
  create_env_file
  load_env_file
fi

ensure_default "CS2DASH_AUTH_USER" "admin"
ensure_default "CS2DASH_BIND" "127.0.0.1"
ensure_default "CS2DASH_DATA_DIR" "data"
ensure_default "CS2DASH_COOKIE_SECURE" "0"
ensure_default "CSGOTRADER_PRICE_BASE" "https://prices.csgotrader.app/latest"
ensure_default "MARKET_SOURCE_PRIORITY" "steam,csfloat,buff163,youpin,skinport"
ensure_default "MARKET_BULK_SOURCE" "any"
ensure_default "AUTO_MARKET_REFRESH" "0"
ensure_default "DATA_TASK_RESUME_ENABLED" "0"

if ! auth_disabled && auth_hash_missing; then
  if ! is_interactive; then
    die "auth is enabled but no password is configured. Run ./start.sh --setup or set CS2DASH_AUTH_PASSWORD_HASH."
  fi
  echo
  echo "Authentication is enabled, but no login password is configured."
  auth_hash="$(prompt_password_hash)"
  write_env_value "CS2DASH_AUTH_PASSWORD_HASH" "$auth_hash"
fi

mkdir -p "${CS2DASH_DATA_DIR:-data}"

if auth_disabled; then
  AUTH_SUMMARY="disabled"
else
  AUTH_SUMMARY="enabled (user: ${CS2DASH_AUTH_USER:-admin})"
fi

echo
echo "$APP_NAME startup"
echo "  Env:     $ENV_FILE"
echo "  URL:     http://${CS2DASH_BIND:-127.0.0.1}:$PORT"
echo "  Data:    ${CS2DASH_DATA_DIR:-data}"
echo "  Auth:    $AUTH_SUMMARY"
echo "  Cookies: CS2DASH_COOKIE_SECURE=${CS2DASH_COOKIE_SECURE:-0}"
echo "  Pricing: CSGOTRADER_PRICE_BASE=${CSGOTRADER_PRICE_BASE:-https://prices.csgotrader.app/latest}"
echo "  Sources: MARKET_SOURCE_PRIORITY=${MARKET_SOURCE_PRIORITY:-steam,csfloat,buff163,youpin,skinport}"
echo "  Refresh: AUTO_MARKET_REFRESH=${AUTO_MARKET_REFRESH:-0}"
echo "  Resume:  DATA_TASK_RESUME_ENABLED=${DATA_TASK_RESUME_ENABLED:-0}"
if [ -n "${STEAM_COOKIE:-}" ]; then
  echo "  Steam:   STEAM_COOKIE configured for chart history"
else
  echo "  Steam:   no STEAM_COOKIE; provider prices still work, Steam chart history may be limited"
fi
if [ "${CS2DASH_COOKIE_SECURE:-0}" != "0" ]; then
  echo "  Note:    secure cookies require HTTPS; set CS2DASH_COOKIE_SECURE=0 for local http:// testing."
fi
if [ "${AUTO_MARKET_REFRESH:-0}" = "0" ]; then
  echo "  Next:    after login/import, use Pull Market Data to populate or refresh cached prices."
else
  echo "  Next:    background market refresh is on; avoid importing while a market pull is running."
fi
echo

exec "${PYTHON[@]}" server.py "$PORT"
