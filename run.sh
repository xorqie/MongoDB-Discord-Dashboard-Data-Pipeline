#!/usr/bin/env bash
set -e

# ── Colours ────────────────────────────────────────────────────────────────────
R='\033[0m'; BOLD='\033[1m'; DIM='\033[2m'
GREEN='\033[32m'; YELLOW='\033[33m'; CYAN='\033[36m'; RED='\033[31m'

ok()   { echo -e "  ${GREEN}✓${R} $1"; }
err()  { echo -e "  ${RED}✗${R} $1"; }
info() { echo -e "  ${DIM}$1${R}"; }

echo ""
echo -e "${CYAN}${BOLD}  ============================================================${R}"
echo -e "${CYAN}${BOLD}   AnimeBot Dashboard  |  github.com/xorqie${R}"
echo -e "${CYAN}${BOLD}  ============================================================${R}"
echo ""

# ── Python check ───────────────────────────────────────────────────────────────
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        VER=$("$cmd" --version 2>&1 | awk '{print $2}')
        MAJOR=$(echo "$VER" | cut -d. -f1)
        MINOR=$(echo "$VER" | cut -d. -f2)
        if [ "$MAJOR" -ge 3 ] && [ "$MINOR" -ge 10 ]; then
            PYTHON="$cmd"
            ok "Python $VER found ($cmd)"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    err "Python 3.10+ not found."
    info "Install it with:"
    info "  Ubuntu/Debian:  sudo apt install python3 python3-pip"
    info "  macOS:          brew install python"
    info "  Or:             https://python.org/downloads"
    exit 1
fi

# ── Dependencies ───────────────────────────────────────────────────────────────
echo ""
info "Checking dependencies..."
$PYTHON -m pip install -q --upgrade pip
$PYTHON -m pip install -q discord.py motor fastapi "uvicorn[standard]" pymongo
ok "Dependencies ready"
echo ""

# ── First-run setup or direct start ───────────────────────────────────────────
if [ ! -f "config.json" ]; then
    echo -e "  ${YELLOW}No config.json found — starting setup wizard...${R}"
    echo ""
    $PYTHON setup.py
else
    ok "config.json found"
    echo ""
    echo -e "  ${CYAN}Dashboard: http://127.0.0.1:5050${R}"
    info "Press Ctrl+C to stop."
    echo ""
    $PYTHON discordbot.py
fi
