#!/usr/bin/env python3
"""
AnimeBot Setup Wizard
Run this once before starting the bot for the first time.
It will guide you through all required configuration and write config.json.
"""

import json
import os
import sys
import re
from datetime import datetime

CONFIG_FILE = "config.json"

# ── Terminal colours (works on Windows 10+, Linux, macOS) ─────────────────────
def _c(code): return f"\033[{code}m"
RESET=_c(0); BOLD=_c(1); DIM=_c(2)
GREEN=_c(32); YELLOW=_c(33); CYAN=_c(36); RED=_c(31); MAGENTA=_c(35)
BG_DARK=_c("40")

def clear():
    os.system("cls" if os.name == "nt" else "clear")

def banner():
    clear()
    print(f"""
{CYAN}{BOLD}
  ░█████╗░███╗░░██╗██╗███╗░░░███╗███████╗██████╗░░█████╗░████████╗
  ██╔══██╗████╗░██║██║████╗░████║██╔════╝██╔══██╗██╔══██╗╚══██╔══╝
  ███████║██╔██╗██║██║██╔████╔██║█████╗░░██████╦╝██║░░██║░░░██║░░░
  ██╔══██║██║╚████║██║██║╚██╔╝██║██╔══╝░░██╔══██╗██║░░██║░░░██║░░░
  ██║░░██║██║░╚███║██║██║░╚═╝░██║███████╗██████╦╝╚█████╔╝░░░██║░░░
  ╚═╝░░╚═╝╚═╝░░╚══╝╚═╝╚═╝░░░░╚═╝╚══════╝╚═════╝░░╚════╝░░░░╚═╝░░░
{RESET}
  {BOLD}Dashboard Setup Wizard{RESET}  {DIM}— by github.com/xorqie{RESET}
  {DIM}{'─' * 60}{RESET}
""")

def section(title):
    print(f"\n{CYAN}{BOLD}{'─' * 60}{RESET}")
    print(f"{CYAN}{BOLD}  {title}{RESET}")
    print(f"{CYAN}{'─' * 60}{RESET}\n")

def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def warn(msg):  print(f"  {YELLOW}⚠{RESET}  {msg}")
def err(msg):   print(f"  {RED}✗{RESET}  {msg}")
def info(msg):  print(f"  {DIM}{msg}{RESET}")
def ask(prompt, default=None, secret=False):
    default_hint = f" {DIM}[{default}]{RESET}" if default else ""
    full_prompt = f"  {BOLD}→{RESET} {prompt}{default_hint}: "
    if secret:
        import getpass
        val = getpass.getpass(full_prompt)
    else:
        val = input(full_prompt).strip()
    if not val and default is not None:
        return str(default)
    return val

def ask_int(prompt, default=None, min_val=None, max_val=None):
    while True:
        raw = ask(prompt, default=str(default) if default is not None else None)
        try:
            v = int(raw)
            if min_val is not None and v < min_val:
                err(f"Must be at least {min_val}")
                continue
            if max_val is not None and v > max_val:
                err(f"Must be at most {max_val}")
                continue
            return v
        except ValueError:
            err("Please enter a number")

def ask_bool(prompt, default=True):
    hint = "Y/n" if default else "y/N"
    raw = ask(f"{prompt} [{hint}]")
    if not raw:
        return default
    return raw.strip().lower() in ("y", "yes", "1", "true")

def validate_snowflake(val):
    """Discord snowflake IDs are 17-19 digit integers."""
    return bool(re.match(r"^\d{17,20}$", val.strip()))

def validate_mongo_uri(uri):
    return uri.startswith("mongodb://") or uri.startswith("mongodb+srv://")

def validate_bot_token(token):
    # Discord bot tokens are roughly 70 chars and contain two dots
    return len(token) > 50 and token.count(".") >= 2

def load_existing():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)

def check_dependencies():
    section("Checking Dependencies")
    missing = []
    packages = [
        ("discord", "discord.py"),
        ("motor", "motor"),
        ("fastapi", "fastapi"),
        ("uvicorn", "uvicorn"),
        ("pymongo", "pymongo"),
    ]
    for module, pip_name in packages:
        try:
            __import__(module)
            ok(pip_name)
        except ImportError:
            err(f"{pip_name}  {DIM}(not installed){RESET}")
            missing.append(pip_name)

    if missing:
        print()
        warn("Some dependencies are missing. Install them with:")
        print(f"\n  {CYAN}pip install {' '.join(missing)}{RESET}\n")
        if not ask_bool("Continue anyway? (useful if you're testing)", default=False):
            sys.exit(1)
    else:
        print()
        ok("All dependencies installed")

def step_discord(cfg):
    section("Step 1 of 5 — Discord Bot Token")
    print(f"  {DIM}Get your token from: https://discord.com/developers/applications{RESET}")
    print(f"  {DIM}Applications → Your Bot → Bot → Token → Reset Token{RESET}\n")

    while True:
        token = ask("Bot Token", secret=True)
        if not token:
            err("Token is required")
            continue
        if not validate_bot_token(token):
            warn("That doesn't look like a valid bot token — double-check it")
            if not ask_bool("Use it anyway?", default=False):
                continue
        cfg["TOKEN"] = token
        ok("Token saved")
        break

def step_server(cfg):
    section("Step 2 of 5 — Discord Server (Guild) IDs")
    print(f"  {DIM}Enable Developer Mode in Discord: Settings → Advanced → Developer Mode{RESET}")
    print(f"  {DIM}Then right-click your server/channels/roles to Copy ID{RESET}\n")

    fields = [
        ("GUILD_ID",           "Server (Guild) ID",           "Your Discord server's ID"),
        ("EPISODES_CHANNEL_ID","Episodes Channel ID",         "Channel for individual episode announcements"),
        ("SEASONS_CHANNEL_ID", "Seasons Channel ID",          "Channel for new season/series announcements"),
        ("MODERATOR_ROLE_ID",  "Moderator Role ID",           "Role that can use bot commands"),
        ("EPISODES_ROLE_ID",   "Episodes Ping Role ID",       "Role pinged for episode announcements (0 = no ping)"),
        ("SEASONS_ROLE_ID",    "Seasons Ping Role ID",        "Role pinged for season announcements (0 = no ping)"),
    ]

    for key, label, hint in fields:
        print(f"  {DIM}{hint}{RESET}")
        existing = str(cfg.get(key, ""))
        while True:
            val = ask(label, default=existing if existing and existing != "0" else None)
            if not val:
                err("Required — enter the ID")
                continue
            if not validate_snowflake(val) and val != "0":
                warn("Doesn't look like a Discord ID (17-19 digits)")
                if not ask_bool("Use it anyway?", default=False):
                    continue
            cfg[key] = int(val)
            ok(f"{label} set")
            break
        print()

def step_mongo(cfg):
    section("Step 3 of 5 — MongoDB Connection")
    print(f"  {DIM}Free cluster: https://cloud.mongodb.com  (M0 tier, no credit card){RESET}")
    print(f"  {DIM}Format: mongodb+srv://user:password@cluster.mongodb.net/{RESET}\n")

    while True:
        uri = ask("MongoDB URI", secret=True, default=cfg.get("MONGO_URI", ""))
        if not uri:
            err("URI is required")
            continue
        if not validate_mongo_uri(uri):
            warn("URI should start with mongodb:// or mongodb+srv://")
            if not ask_bool("Use it anyway?", default=False):
                continue
        cfg["MONGO_URI"] = uri
        ok("MongoDB URI saved")
        break

    db = ask("Database name", default=cfg.get("MONGO_DB", "animedb"))
    cfg["MONGO_DB"] = db or "animedb"
    ok(f"Database: {cfg['MONGO_DB']}")

    ep_col = ask("Episodes collection name", default=cfg.get("COLLECTION_NAME", "episodes"))
    cfg["COLLECTION_NAME"] = ep_col or "episodes"
    cfg["EPISODES_COLLECTION"] = cfg["COLLECTION_NAME"]

    an_col = ask("Anime/series collection name", default=cfg.get("ANIMES_COLLECTION", "animes"))
    cfg["ANIMES_COLLECTION"] = an_col or "animes"
    ok("Collections configured")

def step_behaviour(cfg):
    section("Step 4 of 5 — Bot Behaviour")
    print(f"  {DIM}These control how aggressively the bot batches and posts.{RESET}")
    print(f"  {DIM}Default values work well for most servers — press Enter to keep them.{RESET}\n")

    cfg["CHECK_INTERVAL"]      = ask_int("Scan interval (seconds)", default=cfg.get("CHECK_INTERVAL", 300), min_val=30)
    cfg["BATCH_THRESHOLD"]     = ask_int("Full-season batch threshold (episodes)", default=cfg.get("BATCH_THRESHOLD", 8), min_val=1)
    cfg["MINI_BATCH_THRESHOLD"]= ask_int("Mini-batch threshold (episodes)", default=cfg.get("MINI_BATCH_THRESHOLD", 3), min_val=1)
    cfg["BATCH_HOURS"]         = ask_int("Batch window (hours)", default=cfg.get("BATCH_HOURS", 24), min_val=1)

    print()
    start = ask("Start date (only post episodes after this date, ISO format)",
                default=cfg.get("START_DATE", datetime.utcnow().strftime("%Y-%m-%dT00:00:00Z")))
    cfg["START_DATE"] = start

    emoji = ask("Reaction emoji", default=cfg.get("REACTION_EMOJI", "🎉"))
    cfg["REACTION_EMOJI"] = emoji or "🎉"
    ok("Behaviour configured")

def step_optional(cfg):
    section("Step 5 of 5 — Optional Features")

    print(f"  {BOLD}Weekly Digest{RESET}")
    print(f"  {DIM}Posts a summary embed every Sunday at 18:00 with the week's stats.{RESET}\n")
    cfg["WEEKLY_DIGEST_ENABLED"] = ask_bool("Enable weekly digest?", default=cfg.get("WEEKLY_DIGEST_ENABLED", False))
    if cfg["WEEKLY_DIGEST_ENABLED"]:
        digest_ch = ask("Digest channel ID (leave blank to use Episodes channel)", default="")
        cfg["DIGEST_CHANNEL_ID"] = int(digest_ch) if digest_ch and validate_snowflake(digest_ch) else None
        ok("Weekly digest enabled")
    else:
        cfg["DIGEST_CHANNEL_ID"] = None
        ok("Weekly digest disabled (you can enable it later in config.json)")

    print()
    print(f"  {BOLD}Dashboard{RESET}")
    port = ask_int("Dashboard port", default=cfg.get("DASHBOARD_PORT", 5050), min_val=1024, max_val=65535)
    cfg["DASHBOARD_PORT"] = port
    ok(f"Dashboard will be available at http://127.0.0.1:{port}")

def print_summary(cfg):
    section("Configuration Summary")
    rows = [
        ("Bot Token",        "✓ set (hidden)"),
        ("Server ID",        str(cfg.get("GUILD_ID",""))),
        ("Episodes Channel", str(cfg.get("EPISODES_CHANNEL_ID",""))),
        ("Seasons Channel",  str(cfg.get("SEASONS_CHANNEL_ID",""))),
        ("MongoDB DB",       cfg.get("MONGO_DB","")),
        ("Episodes Col",     cfg.get("COLLECTION_NAME","")),
        ("Check Interval",   f"{cfg.get('CHECK_INTERVAL',300)}s"),
        ("Dashboard Port",   str(cfg.get("DASHBOARD_PORT",5050))),
        ("Weekly Digest",    "enabled" if cfg.get("WEEKLY_DIGEST_ENABLED") else "disabled"),
    ]
    for label, value in rows:
        print(f"  {DIM}{label:<22}{RESET}{BOLD}{value}{RESET}")

def main():
    banner()

    print(f"  {BOLD}Welcome!{RESET} This wizard will help you configure AnimeBot.")
    print(f"  {DIM}It will create a {CYAN}config.json{RESET}{DIM} file in the current directory.{RESET}")
    print(f"  {DIM}Your credentials are stored locally — never sent anywhere.{RESET}\n")

    existing = load_existing()
    if existing:
        warn(f"Found existing {CONFIG_FILE} — pre-filling answers with current values.")
        info("Press Enter on any question to keep the existing value.\n")

    check_dependencies()

    cfg = dict(existing)

    try:
        step_discord(cfg)
        step_server(cfg)
        step_mongo(cfg)
        step_behaviour(cfg)
        step_optional(cfg)
    except KeyboardInterrupt:
        print(f"\n\n  {YELLOW}Setup cancelled.{RESET} No changes saved.\n")
        sys.exit(0)

    print_summary(cfg)
    print()

    if ask_bool("Save this configuration and start AnimeBot?", default=True):
        save_config(cfg)
        ok(f"Saved to {CONFIG_FILE}")
        print(f"\n  {GREEN}{BOLD}Setup complete!{RESET}\n")
        print(f"  Starting AnimeBot now…\n")
        print(f"  {DIM}Dashboard will open at: {CYAN}http://127.0.0.1:{cfg.get('DASHBOARD_PORT', 5050)}{RESET}\n")
        print(f"  {DIM}Press Ctrl+C to stop the bot.{RESET}\n")
        print(f"  {'─' * 60}\n")
        # Hand off to the bot
        import subprocess
        subprocess.run([sys.executable, "discordbot.py"], check=False)
    else:
        save_config(cfg)
        ok(f"Configuration saved to {CONFIG_FILE}")
        print(f"\n  Run {CYAN}python discordbot.py{RESET} when ready.\n")

if __name__ == "__main__":
    main()
