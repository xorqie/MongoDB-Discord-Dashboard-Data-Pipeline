"""
dashboard_server.py  –  Herald Dashboard v3
"""

import asyncio
import csv
import io
import json
import logging
import os
import time
from collections import deque, Counter
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import uvicorn

logger = logging.getLogger("Dashboard")

# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────
class DashboardState:
    def __init__(self):
        self.start_time: float = time.time()
        self.log_buffer: deque = deque(maxlen=2000)
        self.recent_posts: deque = deque(maxlen=200)
        self.posted_episodes_ref: Optional[Set] = None
        self.posted_metadata_ref: Optional[Dict] = None
        self.posted_seasons_ref: Optional[list] = None
        self.posted_animeseasons_ref: Optional[Set] = None  # Discord message IDs from the seasons channel
        self.next_scan_at: Optional[float] = None
        self.mongo_ok: bool = False
        self.discord_ok: bool = False
        self.mongo_client = None
        self.mongo_db_name: str = ""
        self.episodes_col: str = ""
        self.animes_col: str = ""
        self.check_interval: int = 300
        self.anime_cache_ref: Optional[Dict] = None
        self.bot_ref = None
        self.force_scan_fn = None
        self.config_reload_fn = None   # callback(dict) -> None; lets the bot hot-apply config changes
        self.manual_post_season_fn = None  # async callback(series_id) -> dict; explicit one-off season post
        self.sync_animeseasons_fn = None   # async callback(limit=None) -> dict; re-sync posted_animeseasons.json from Discord
        self.guild_info_fn = None          # async callback() -> dict; live guild/channels/roles for Discord Config page
        self.test_announcement_fn = None   # async callback(kind, channel_id, role_id) -> dict; sends a labeled test message
        self._ws_clients: List[WebSocket] = []
        self.error_count: int = 0
        # track session active windows for "total active time in 7 days"
        self._session_start: float = time.time()
        self.active_sessions: List[Dict] = []   # [{start, end}]
        # maintenance mode
        self.maintenance_until: Optional[float] = None
        self.total_scans: int = 0

    async def broadcast(self, event: str, data: Any):
        dead = []
        msg = json.dumps({"event": event, "data": data}, default=str)
        for ws in self._ws_clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in self._ws_clients:
                self._ws_clients.remove(ws)

    def add_log(self, level: str, message: str):
        if level in ("error", "critical"):
            self.error_count += 1
        entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, "msg": message}
        self.log_buffer.append(entry)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.broadcast("log", entry))
                if level in ("error", "critical"):
                    asyncio.ensure_future(self.broadcast("error_count", self.error_count))
        except RuntimeError:
            pass

    def notify_episode_posted(self, meta: Dict):
        self.recent_posts.appendleft(meta)
        _invalidate_analytics_cache()  # new data — next analytics poll will re-compute fresh
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.broadcast("episode_posted", meta))
                # Also create a persistent alert entry for the Notification Center,
                # so episode/season announcements show up there (not just as a toast).
                is_season = meta.get("channel") == "Seasons"
                title = "🌟 New season announced" if is_season else "📺 New episode posted"
                channel_label = "Seasons" if is_season else "Episodes"
                ep_part = f" — Ep {meta.get('episode_number')}" if meta.get("episode_number") and not is_season else ""
                body = f"{meta.get('anime_title','Unknown')}{ep_part} was just announced to #{channel_label}"
                asyncio.ensure_future(_push_alert_async(
                    "season" if is_season else "episode", title, body,
                    extra={"anime_title": meta.get("anime_title"), "episode_number": meta.get("episode_number"),
                           "channel": meta.get("channel"), "series_id": meta.get("series_id")}
                ))
        except RuntimeError:
            pass

    def active_time_7d_secs(self) -> int:
        """Return total seconds the bot was active in the past 7 days."""
        cutoff = time.time() - 7 * 86400
        total = 0
        for s in self.active_sessions:
            end = s.get("end", time.time())
            start = max(s["start"], cutoff)
            if end > cutoff:
                total += max(0, end - start)
        # add current session
        total += max(0, time.time() - max(self._session_start, cutoff))
        return int(total)


state = DashboardState()

# ─────────────────────────────────────────────────────────────────────────────
# Logging handler
# ─────────────────────────────────────────────────────────────────────────────
class DashboardLogHandler(logging.Handler):
    LEVEL_MAP = {logging.DEBUG:"debug", logging.INFO:"info",
                 logging.WARNING:"warning", logging.ERROR:"error", logging.CRITICAL:"critical"}
    def emit(self, record):
        level = self.LEVEL_MAP.get(record.levelno, "info")
        msg = self.format(record)
        if level == "info" and any(x in msg for x in ["✅","posted","announced","sent"]):
            level = "success"
        state.add_log(level, msg)

def attach_log_handler():
    h = DashboardLogHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(name)s %(message)s"))
    logging.getLogger().addHandler(h)

# ─────────────────────────────────────────────────────────────────────────────
# Config helpers
# ─────────────────────────────────────────────────────────────────────────────
_here = os.path.dirname(os.path.abspath(__file__))

def _find_config() -> str:
    """Find config.json in same dir or parent."""
    for d in [_here, os.path.dirname(_here)]:
        p = os.path.join(d, "config.json")
        if os.path.exists(p):
            return p
    return os.path.join(_here, "config.json")

def load_config() -> Dict:
    p = _find_config()
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_config(data: Dict):
    p = _find_config()
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Herald Dashboard", docs_url=None, redoc_url=None)

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = os.path.join(_here, "templates", "index.html")
    with open(html_path, encoding="utf-8") as f:
        return f.read()

def _safe_json(obj):
    if isinstance(obj, dict):   return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [_safe_json(i) for i in obj]
    if isinstance(obj, datetime): return obj.isoformat()
    try: json.dumps(obj); return obj
    except TypeError: return str(obj)

def _uptime_secs(): return int(time.time() - state.start_time)
def _next_scan_secs():
    if state.next_scan_at is None: return None
    return max(0, int(state.next_scan_at - time.time()))

def _fmt_active_time(secs: int) -> str:
    h, r = divmod(secs, 3600); m = r // 60
    return f"{h}h {m}m"

# ── Analytics helper ──────────────────────────────────────────────────────────
def _parse_dt(raw: str) -> Optional[datetime]:
    """Parse a datetime string robustly, always returning UTC-aware or None."""
    if not raw:
        return None
    try:
        s = str(raw).strip()
        # Replace Z suffix
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        # If naive (no tzinfo), assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def _build_analytics(period: str = "alltime"):
    """
    period: "day" | "week" | "month" | "alltime"
    Returns analytics scoped to that period for charts/feeds,
    but always returns lifetime totals for the stat cards.
    """
    meta = state.posted_metadata_ref or {}
    now = datetime.now(timezone.utc)

    # Determine cutoff for charts/feeds (None = all time)
    if period == "day":
        chart_cutoff = now - timedelta(days=1)
    elif period == "week":
        chart_cutoff = now - timedelta(days=7)
    elif period == "month":
        chart_cutoff = now - timedelta(days=30)
    else:
        chart_cutoff = None   # all time

    # ── Always compute LIFETIME totals ──────────────────────────────────────
    all_seasons_posted = set()
    all_series_counter: Counter = Counter()
    all_channels: Counter = Counter()
    all_daily: Counter = Counter()    # for heatmap (always full history)

    # ── Period-scoped counters for charts ────────────────────────────────────
    period_daily: Counter = Counter()
    period_weekly: Counter = Counter()
    period_series: Counter = Counter()
    period_channels: Counter = Counter()
    period_seasons = set()
    recent: list = []

    for v in meta.values():
        dt = _parse_dt(v.get("posted_at", ""))
        title = v.get("anime_title", "Unknown")
        ch = v.get("channel", "Unknown")
        sid = v.get("series_id") or title

        # Lifetime counters (always)
        all_series_counter[title] += 1
        all_channels[ch] += 1
        if ch == "Seasons":
            all_seasons_posted.add(v.get("episode_id") or sid)  # count every Sezone post, not unique series
        if dt:
            all_daily[dt.strftime("%Y-%m-%d")] += 1

        # Period-scoped
        in_period = (chart_cutoff is None) or (dt is not None and dt >= chart_cutoff)
        if in_period:
            period_series[title] += 1
            period_channels[ch] += 1
            if ch == "Seasons":
                period_seasons.add(v.get("episode_id") or sid)
            if dt:
                period_daily[dt.strftime("%Y-%m-%d")] += 1
                period_weekly[dt.strftime("%Y-W%W")] += 1
            recent.append(v)

    recent.sort(key=lambda x: x.get("posted_at", ""), reverse=True)

    # Lifetime totals (stat cards always show these regardless of filter)
    # Primary source: posted_episodes_ref (set of episode IDs from posted.json).
    # Fallback: if that's empty/unavailable but metadata has entries, count from metadata
    # so a corrupted/missing posted.json doesn't zero out the stat unnecessarily.
    total_posted_lifetime = len(state.posted_episodes_ref or set())
    if total_posted_lifetime == 0 and meta:
        total_posted_lifetime = len(meta)

    # Total Seasons Announced: the count of unique, real Discord messages actually
    # present in the seasons announcement channel (SEASONS_CHANNEL_ID), tracked via
    # posted_animeseasons.json. This is the authoritative source — it reflects exactly
    # what's live on Discord, independent of MongoDB and of any local JSON corruption.
    # Falls back to posted_seasons.json, then metadata-derived counting, only if the
    # Discord-message-based store hasn't been populated yet (e.g. before first sync).
    animeseasons_ref = state.posted_animeseasons_ref
    seasons_ref = state.posted_seasons_ref
    if animeseasons_ref:
        total_seasons_lifetime = len(animeseasons_ref)
    elif seasons_ref:
        total_seasons_lifetime = len(seasons_ref)
    else:
        total_seasons_lifetime = len(all_seasons_posted)

    # Period totals (for sub-header context)
    period_posted = len(recent)
    if seasons_ref:
        period_seasons_count = 0
        for s in seasons_ref:
            dt = _parse_dt(s.get("posted_at", ""))
            if (chart_cutoff is None) or (dt is not None and dt >= chart_cutoff):
                period_seasons_count += 1
    else:
        period_seasons_count = len(period_seasons)

    # Heatmap: full all-time history (no cutoff — show every day that has data)
    heatmap_daily = dict(all_daily)  # all days ever recorded

    # Daily breakdown for seasons (used by the Overview sparkline trend indicator)
    seasons_daily: Counter = Counter()
    if seasons_ref:
        for s in seasons_ref:
            dt = _parse_dt(s.get("posted_at", ""))
            if dt:
                seasons_daily[dt.strftime("%Y-%m-%d")] += 1
    sorted_seasons_daily = sorted(seasons_daily.items())[-14:]

    # Charts: decide how many bars to show
    if period == "day":
        sorted_daily = sorted(period_daily.items())[-24:]   # last 24h by hour ideally, use days
        sorted_weekly = []
    elif period == "week":
        sorted_daily = sorted(period_daily.items())[-7:]
        sorted_weekly = sorted(period_weekly.items())[-4:]
    elif period == "month":
        sorted_daily = sorted(period_daily.items())[-30:]
        sorted_weekly = sorted(period_weekly.items())[-8:]
    else:
        sorted_daily = sorted(period_daily.items())[-60:]
        sorted_weekly = sorted(period_weekly.items())[-20:]

    return {
        # Lifetime stat card values
        "total_posted": total_posted_lifetime,
        "total_seasons": total_seasons_lifetime,
        # Period context
        "period_posted": period_posted,
        "period_seasons": period_seasons_count,
        "period": period,
        # Charts (period-scoped)
        "daily": sorted_daily,
        "weekly": sorted_weekly,
        "channels": dict(period_channels) if period != "alltime" else dict(all_channels),
        "top_series": period_series.most_common(10) if period != "alltime" else all_series_counter.most_common(10),
        "recent_posts": recent[:30],
        # Heatmap always full history
        "heatmap_daily": sorted(heatmap_daily.items()),
        # Active time
        "active_time_7d": _fmt_active_time(state.active_time_7d_secs()),
        "active_time_7d_secs": state.active_time_7d_secs(),
        # Total scans estimate
        "total_scans": getattr(state, "total_scans", 0),
        # Sparkline trends (always last 14 days, independent of the period filter above —
        # used for the small inline trend charts next to each Overview stat card)
        "sparkline_episodes": sorted(all_daily.items())[-14:],
        "sparkline_seasons": sorted_seasons_daily,
    }

def _build_insights():
    """Analytics insights derived entirely from existing posted_metadata.json /
    posted_seasons.json data — no new tracking files, no MongoDB queries.
    Returns: monthly trend (12mo), peak day-of-week, peak hour-of-day, and a
    channel comparison summary (episodes vs seasons growth).
    """
    meta = state.posted_metadata_ref or {}

    monthly: Counter = Counter()
    dow_counter: Counter = Counter()   # 0=Mon .. 6=Sun
    hour_counter: Counter = Counter()  # 0-23, in UTC (timestamps are stored as UTC)
    channel_monthly: Dict[str, Counter] = {"Episodes": Counter(), "Seasons": Counter()}

    all_dates = []
    for v in meta.values():
        dt = _parse_dt(v.get("posted_at", ""))
        if not dt:
            continue
        all_dates.append(dt)
        month_key = dt.strftime("%Y-%m")
        monthly[month_key] += 1
        dow_counter[dt.weekday()] += 1
        hour_counter[dt.hour] += 1
        ch = v.get("channel", "Episodes")
        if ch in channel_monthly:
            channel_monthly[ch][month_key] += 1

    # last 12 months, oldest -> newest, zero-filled for months with no activity
    now = datetime.now(timezone.utc)
    last_12 = []
    for i in range(11, -1, -1):
        # subtract i months from now (calendar-safe)
        year = now.year
        month = now.month - i
        while month <= 0:
            month += 12
            year -= 1
        key = f"{year:04d}-{month:02d}"
        last_12.append(key)
    monthly_trend = [(k, monthly.get(k, 0)) for k in last_12]
    channel_trend = {
        ch: [(k, counter.get(k, 0)) for k in last_12]
        for ch, counter in channel_monthly.items()
    }

    dow_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    peak_day = None
    if dow_counter:
        peak_idx = dow_counter.most_common(1)[0][0]
        peak_day = {"day": dow_names[peak_idx], "count": dow_counter[peak_idx]}
    dow_distribution = [{"day": dow_names[i], "count": dow_counter.get(i, 0)} for i in range(7)]

    peak_hour = None
    if hour_counter:
        peak_h = hour_counter.most_common(1)[0][0]
        peak_hour = {"hour": peak_h, "count": hour_counter[peak_h]}
    hour_distribution = [{"hour": h, "count": hour_counter.get(h, 0)} for h in range(24)]

    # growth: compare last 30 days to the 30 days before that
    if all_dates:
        cutoff_recent = now - timedelta(days=30)
        cutoff_prior = now - timedelta(days=60)
        recent_count = sum(1 for d in all_dates if d >= cutoff_recent)
        prior_count = sum(1 for d in all_dates if cutoff_prior <= d < cutoff_recent)
        if prior_count > 0:
            growth_pct = round(((recent_count - prior_count) / prior_count) * 100, 1)
        elif recent_count > 0:
            growth_pct = 100.0
        else:
            growth_pct = 0.0
    else:
        recent_count = prior_count = 0
        growth_pct = 0.0

    return {
        "monthly_trend": monthly_trend,
        "channel_trend": channel_trend,
        "peak_day": peak_day,
        "dow_distribution": dow_distribution,
        "peak_hour": peak_hour,
        "hour_distribution": hour_distribution,
        "growth": {
            "last_30d": recent_count,
            "prior_30d": prior_count,
            "pct_change": growth_pct,
        },
        "total_data_points": len(all_dates),
    }

# ── Endpoints ──────────────────────────────────────────────────────────────────
@app.get("/api/status")
async def api_status():
    cfg = load_config()
    return {
        "uptime_secs": _uptime_secs(),
        "next_scan_secs": _next_scan_secs(),
        "mongo_ok": state.mongo_ok,
        "discord_ok": state.discord_ok,
        "check_interval": state.check_interval,
        "error_count": state.error_count,
        "active_time_7d": _fmt_active_time(state.active_time_7d_secs()),
        "maintenance_until": state.maintenance_until,
        "ts": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/api/health")
async def api_health():
    """Single-endpoint health check for process managers (pm2, systemd, docker healthcheck,
    etc.) to poll and auto-restart the bot if Discord or MongoDB connections drop.

    Returns HTTP 200 with status:"ok" when both Discord and MongoDB are connected.
    Returns HTTP 503 with status:"degraded" or status:"down" otherwise, with `checks`
    detailing exactly which dependency is unhealthy — so the process manager log/alert
    can show the specific cause, not just a generic failure.
    """
    mongo_ok = state.mongo_ok
    discord_ok = state.discord_ok
    # consider the scan loop stalled if next_scan_at is set but long overdue
    # (more than 3x the check interval past due) — a sign the event loop is stuck
    scan_stalled = False
    if state.next_scan_at is not None and state.check_interval:
        overdue = time.time() - state.next_scan_at
        scan_stalled = overdue > (state.check_interval * 3)

    checks = {
        "mongo": "ok" if mongo_ok else "down",
        "discord": "ok" if discord_ok else "down",
        "scan_loop": "stalled" if scan_stalled else "ok",
    }
    all_ok = mongo_ok and discord_ok and not scan_stalled
    overall = "ok" if all_ok else ("down" if not (mongo_ok or discord_ok) else "degraded")

    body = {
        "status": overall,
        "checks": checks,
        "uptime_secs": _uptime_secs(),
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    if all_ok:
        return JSONResponse(body, status_code=200)
    return JSONResponse(body, status_code=503)

_analytics_cache: Dict[str, Any] = {}
_analytics_cache_ts: Dict[str, float] = {}
_insights_cache: Optional[Dict] = None
_insights_cache_ts: float = 0.0
_CACHE_TTL = 20  # seconds — short enough to feel live, long enough to avoid re-iterating thousands of entries on every poll

def _invalidate_analytics_cache():
    """Called when new episode/season data arrives so the next request gets fresh data."""
    _analytics_cache.clear()
    _analytics_cache_ts.clear()
    global _insights_cache, _insights_cache_ts
    _insights_cache = None
    _insights_cache_ts = 0.0

@app.get("/api/analytics")
async def api_analytics(period: str = "alltime"):
    if period not in ("day", "week", "month", "alltime"): period = "alltime"
    now = time.time()
    if period in _analytics_cache and (now - _analytics_cache_ts.get(period, 0)) < _CACHE_TTL:
        return _analytics_cache[period]
    result = _build_analytics(period=period)
    _analytics_cache[period] = result
    _analytics_cache_ts[period] = now
    return result

@app.get("/api/insights")
async def api_insights():
    global _insights_cache, _insights_cache_ts
    now = time.time()
    if _insights_cache is not None and (now - _insights_cache_ts) < _CACHE_TTL:
        return _insights_cache
    result = _build_insights()
    _insights_cache = result
    _insights_cache_ts = now
    return result

@app.get("/api/logs")
async def api_logs(level: str = "all", limit: int = 500):
    logs = list(state.log_buffer)
    if level != "all":
        logs = [l for l in logs if l["level"] == level]
    return {"logs": logs[-limit:], "error_count": state.error_count}

@app.get("/api/logs/file")
async def api_logs_file(filename: str = "bot_events.log", lines: int = 400):
    for d in [_here, os.path.dirname(_here)]:
        path = os.path.join(d, filename)
        if os.path.exists(path):
            break
    else:
        return {"lines": [], "error": f"{filename} not found"}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        parsed = []
        for line in all_lines[-lines:]:
            line = line.strip()
            if not line: continue
            level = "info"
            if " - ERROR - " in line or "ERROR" in line: level = "error"
            elif " - WARNING - " in line: level = "warning"
            elif " - DEBUG - " in line: level = "debug"
            elif "✅" in line or "success" in line.lower(): level = "success"
            # Try to extract real timestamp from log line e.g. "2026-06-18 07:12:34,123"
            ts = ""
            import re
            m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if m: ts = m.group(1)
            parsed.append({"ts": ts, "level": level, "msg": line})
        return {"lines": parsed}
    except Exception as e:
        return {"lines": [], "error": str(e)}

@app.post("/api/scan")
async def api_force_scan():
    if state.maintenance_until and time.time() < state.maintenance_until:
        return JSONResponse({"ok": False, "error": "Bot is in maintenance mode"}, status_code=503)
    if state.force_scan_fn is None:
        return JSONResponse({"ok": False, "error": "scan function not registered"}, status_code=503)
    try:
        asyncio.ensure_future(state.force_scan_fn())
        state.next_scan_at = time.time() + state.check_interval
        state.total_scans += 1
        await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(),
                                       "level": "info", "msg": "🔄 Manual scan triggered from dashboard"})
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/maintenance")
async def api_maintenance(payload: dict):
    """Set or clear maintenance mode. payload: {hours: 0-24} 0 = clear"""
    hours = float(payload.get("hours", 0))
    if hours <= 0:
        state.maintenance_until = None
        msg = "✅ Maintenance mode cleared"
    else:
        state.maintenance_until = time.time() + hours * 3600
        msg = f"🔧 Maintenance mode set for {hours}h"
    await state.broadcast("maintenance", {"until": state.maintenance_until})
    await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(), "level": "warning", "msg": msg})
    return {"ok": True, "maintenance_until": state.maintenance_until}

@app.get("/api/db/collections")
async def api_collections():
    if not state.mongo_client:
        return {"error": "MongoDB not connected", "collections": []}
    try:
        db = state.mongo_client[state.mongo_db_name]
        names = await db.list_collection_names()
        result = []
        for name in names:
            count = await db[name].count_documents({})
            result.append({"name": name, "count": count})
        return {"collections": result}
    except Exception as e:
        return {"error": str(e), "collections": []}

@app.get("/api/db/{collection}")
async def api_collection_data(collection: str, page: int = 1, page_size: int = 25, search: str = ""):
    if not state.mongo_client:
        return {"error": "MongoDB not connected", "rows": [], "total": 0}
    try:
        db = state.mongo_client[state.mongo_db_name]
        col = db[collection]
        query: Dict = {}
        if search:
            query = {"$or": [
                {"tmdbTitle": {"$regex": search, "$options": "i"}},
                {"malTitle": {"$regex": search, "$options": "i"}},
                {"episodeTitle": {"$regex": search, "$options": "i"}},
                {"anime_title": {"$regex": search, "$options": "i"}},
            ]}
        total = await col.count_documents(query)
        skip = (page - 1) * page_size
        docs = await col.find(query).skip(skip).limit(page_size).to_list(length=page_size)
        rows = [_safe_json({k: (str(v) if k == "_id" else v) for k, v in doc.items()}) for doc in docs]
        return {"rows": rows, "total": total, "page": page, "page_size": page_size}
    except Exception as e:
        return {"error": str(e), "rows": [], "total": 0}

@app.get("/api/db/anime/{identifier}")
async def api_anime_detail(identifier: str):
    """Deep dive: one anime's full episode history, posting cadence, and MongoDB metadata.
    `identifier` may be a series_id (MongoDB ObjectId) or an anime_title (used by the
    Top Anime list, which groups by title)."""
    meta = state.posted_metadata_ref or {}
    eps = [v for v in meta.values()
           if v.get("series_id") == identifier or v.get("anime_title") == identifier]
    eps.sort(key=lambda x: (str(x.get("posted_at", ""))))

    # Posting cadence: gaps in days between consecutive posts
    cadence = []
    parsed_dates = []
    for e in eps:
        dt = _parse_dt(e.get("posted_at", ""))
        if dt:
            parsed_dates.append(dt)
    parsed_dates.sort()
    for i in range(1, len(parsed_dates)):
        gap_days = (parsed_dates[i] - parsed_dates[i - 1]).total_seconds() / 86400
        cadence.append(round(gap_days, 1))
    avg_cadence = round(sum(cadence) / len(cadence), 1) if cadence else None

    season_posts = [e for e in eps if e.get("channel") == "Seasons"]
    episode_posts = [e for e in eps if e.get("channel") == "Episodes"]

    # Try to resolve the real series_id from metadata if a title was passed
    resolved_series_id = None
    for e in eps:
        if e.get("series_id"):
            resolved_series_id = e["series_id"]
            break
    if not resolved_series_id and identifier:
        resolved_series_id = identifier

    anime_data = {}
    if state.mongo_client and resolved_series_id:
        try:
            db = state.mongo_client[state.mongo_db_name]
            from bson import ObjectId
            doc = await db[state.animes_col].find_one({"_id": ObjectId(resolved_series_id)})
            if doc:
                anime_data = _safe_json({k: (str(v) if k == "_id" else v) for k, v in doc.items()})
        except Exception:
            pass

    return {
        "episodes": eps,
        "anime": anime_data,
        "total_episodes_posted": len(episode_posts),
        "total_season_posts": len(season_posts),
        "first_posted_at": eps[0].get("posted_at") if eps else None,
        "last_posted_at": eps[-1].get("posted_at") if eps else None,
        "avg_cadence_days": avg_cadence,
        "cadence_days": cadence,
    }

@app.get("/api/preview")
async def api_preview(kind: str = "episode"):
    """Build a preview of the exact embed the bot would post, using real data from
    MongoDB (latest episode or latest season), without actually sending anything to Discord.
    kind: 'episode' | 'season'
    """
    if not state.mongo_client:
        return {"error": "MongoDB not connected"}
    try:
        db = state.mongo_client[state.mongo_db_name]
        cfg = load_config()
        episodes_channel_id = cfg.get("EPISODES_CHANNEL_ID", "")
        seasons_channel_id = cfg.get("SEASONS_CHANNEL_ID", "")
        episodes_role_id = cfg.get("EPISODES_ROLE_ID", "")
        seasons_role_id = cfg.get("SEASONS_ROLE_ID", "")
        reaction_emoji = cfg.get("REACTION_EMOJI", "🎉")

        if kind == "season":
            anime_doc = await db[state.animes_col].find_one(sort=[("_id", -1)])
            if not anime_doc:
                return {"error": "No anime series found in database"}
            anime_title = anime_doc.get("tmdbTitle") or anime_doc.get("malTitle") or "Unknown Anime"
            banner = anime_doc.get("backdrop") or anime_doc.get("poster")
            thumbnail = anime_doc.get("poster") or anime_doc.get("thumbnailDub")
            description_text = anime_doc.get("description") or anime_doc.get("overview") or ""
            fields = []
            if anime_doc.get("genres"):
                fields.append({"name": "🎭 Genres", "value": ", ".join(anime_doc["genres"][:3]), "inline": False})
            if anime_doc.get("tmdbRating"):
                fields.append({"name": "⭐ TMDB", "value": str(anime_doc["tmdbRating"]), "inline": True})
            if anime_doc.get("malRating"):
                fields.append({"name": "⭐ MAL", "value": str(anime_doc["malRating"]), "inline": True})
            return {
                "kind": "season",
                "title": "🌟 New Season Announced!",
                "description": f"**{anime_title}**\n\n{_truncate(description_text, 300)}" if description_text else f"**{anime_title}**",
                "banner": banner, "thumbnail": thumbnail, "fields": fields,
                "color": "#FFD700",
                "channel_id": seasons_channel_id, "role_id": seasons_role_id,
                "reaction_emoji": reaction_emoji,
            }
        else:
            episode_doc = await db[state.episodes_col].find_one(sort=[("_id", -1)])
            if not episode_doc:
                return {"error": "No episodes found in database"}
            series_id = str(episode_doc.get("seriesId", ""))
            anime_doc = None
            try:
                from bson import ObjectId
                anime_doc = await db[state.animes_col].find_one({"_id": ObjectId(series_id)})
            except Exception:
                pass
            anime_title = "Unknown Anime"
            banner = thumbnail = None
            fields = []
            if anime_doc:
                anime_title = anime_doc.get("tmdbTitle") or anime_doc.get("malTitle") or "Unknown Anime"
                banner = anime_doc.get("backdrop") or anime_doc.get("poster")
                thumbnail = anime_doc.get("poster") or anime_doc.get("thumbnailDub")
                if anime_doc.get("genres"):
                    fields.append({"name": "🎭 Genres", "value": ", ".join(anime_doc["genres"][:3]), "inline": False})
                if anime_doc.get("tmdbRating"):
                    fields.append({"name": "⭐ TMDB", "value": str(anime_doc["tmdbRating"]), "inline": True})
                if anime_doc.get("malRating"):
                    fields.append({"name": "⭐ MAL", "value": str(anime_doc["malRating"]), "inline": True})
            ep_number = episode_doc.get("episodeNumber", "N/A")
            ep_title = episode_doc.get("episodeTitle", "N/A")
            fields.append({"name": "🎬 Dostupno", "value": "Player 1, Player 2", "inline": False})
            return {
                "kind": "episode",
                "title": "🎉 New Content Released!",
                "description": f"**{anime_title}**\n\nEpizoda {ep_number}: *{ep_title}*",
                "banner": banner, "thumbnail": thumbnail, "fields": fields,
                "color": "#22C55E",
                "channel_id": episodes_channel_id, "role_id": episodes_role_id,
                "reaction_emoji": reaction_emoji,
            }
    except Exception as e:
        return {"error": str(e)}

def _truncate(text: str, max_length: int = 200) -> str:
    if not text:
        return ""
    return text if len(text) <= max_length else text[:max_length].rstrip() + "…"

@app.get("/api/seasons/announced-count")
async def api_seasons_announced_count():
    """Returns the authoritative 'Total Seasons Announced' count, sourced from
    posted_animeseasons.json (unique real Discord message IDs in the seasons channel)."""
    ref = state.posted_animeseasons_ref
    return {
        "count": len(ref) if ref is not None else 0,
        "source": "posted_animeseasons.json" if ref else "unavailable",
    }

@app.post("/api/seasons/sync")
async def api_seasons_sync():
    """Trigger a fresh re-sync of posted_animeseasons.json by re-reading the seasons
    channel's full message history from Discord. Does not touch MongoDB or posting logic."""
    if state.sync_animeseasons_fn is None:
        return JSONResponse({"ok": False, "count": 0, "message": "Bot has not registered the sync handler (is it fully started?)"}, status_code=503)
    try:
        result = await state.sync_animeseasons_fn()
        if result.get("ok"):
            await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(),
                "level": "success", "msg": f"✅ {result.get('message')}"})
        return result
    except Exception as e:
        return JSONResponse({"ok": False, "count": 0, "message": str(e)}, status_code=500)

@app.get("/api/seasons/pending")
async def api_seasons_pending(limit: int = 100):
    """Read-only dashboard helper — does NOT touch discordbot.py's posting/batching logic.

    Cross-references the SAME season-start rule the bot already uses
    (episodeNumber == 1, from discordbot.py's is_new_season check) against the `animes`
    collection (for real titles/metadata) and against what's already been announced to
    SEASONS_CHANNEL_ID (posted_seasons.json / posted_metadata.json), to build an
    actionable list of anime whose season-1 exists in MongoDB but hasn't been posted
    to the Discord seasons channel yet.
    """
    if not state.mongo_client:
        return {"error": "MongoDB not connected", "pending": [], "count": 0}
    try:
        from bson import ObjectId
        db = state.mongo_client[state.mongo_db_name]
        episodes_col = db[state.episodes_col]
        animes_col = db[state.animes_col]

        # Same rule as discordbot.py: is_new_season = (episodeNumber == 1)
        cursor = episodes_col.find({"episodeNumber": 1}, {"seriesId": 1, "createdAt": 1})
        season_start_series = {}  # series_id -> earliest createdAt seen
        async for doc in cursor:
            sid = doc.get("seriesId")
            if not sid:
                continue
            sid = str(sid)
            created = doc.get("createdAt")
            if sid not in season_start_series or (created and created < season_start_series[sid]):
                season_start_series[sid] = created

        # Already-announced series_ids (from the bot's own tracking — same source as
        # the "Total Seasons Announced" stat card, just read here for comparison)
        already_announced = set()
        seasons_ref = state.posted_seasons_ref
        if seasons_ref:
            for s in seasons_ref:
                if s.get("series_id"):
                    already_announced.add(str(s["series_id"]))
        else:
            meta = state.posted_metadata_ref or {}
            for v in meta.values():
                if v.get("channel") == "Seasons" and v.get("series_id"):
                    already_announced.add(str(v["series_id"]))

        pending_ids = [sid for sid in season_start_series if sid not in already_announced]
        pending_ids.sort(key=lambda sid: season_start_series[sid] or "", reverse=True)
        pending_ids = pending_ids[:limit]

        # Resolve titles from the animes collection
        pending = []
        if pending_ids:
            obj_ids = []
            id_map = {}
            for sid in pending_ids:
                try:
                    oid = ObjectId(sid)
                    obj_ids.append(oid)
                    id_map[str(oid)] = sid
                except Exception:
                    continue
            anime_cursor = animes_col.find({"_id": {"$in": obj_ids}})
            anime_lookup = {}
            async for doc in anime_cursor:
                anime_lookup[str(doc["_id"])] = doc
            for sid in pending_ids:
                doc = anime_lookup.get(sid)
                title = "Unknown"
                genres = []
                rating = None
                if doc:
                    title = doc.get("tmdbTitle") or doc.get("malTitle") or "Unknown"
                    genres = (doc.get("genres") or [])[:3]
                    rating = doc.get("tmdbRating") or doc.get("malRating")
                pending.append({
                    "series_id": sid,
                    "anime_title": title,
                    "genres": genres,
                    "rating": rating,
                    "episode1_created_at": _safe_json(season_start_series.get(sid)),
                })

        return {
            "pending": pending,
            "count": len(season_start_series) - len(already_announced & set(season_start_series.keys())),
            "total_season_starts_in_db": len(season_start_series),
            "already_announced": len(already_announced & set(season_start_series.keys())),
        }
    except Exception as e:
        return {"error": str(e), "pending": [], "count": 0}

@app.post("/api/seasons/post/{series_id}")
async def api_seasons_post(series_id: str):
    """Manually trigger a one-off season announcement for a specific anime, via the
    bot's manual_post_season_fn hook. Separate from the automatic scan/batch loop —
    only ever called by an explicit dashboard button click.
    """
    if state.manual_post_season_fn is None:
        return JSONResponse({"ok": False, "message": "Bot has not registered the manual post handler (is it fully started?)"}, status_code=503)
    try:
        result = await state.manual_post_season_fn(series_id)
        if result.get("ok"):
            await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(),
                "level": "success", "msg": f"✅ Manual season post: {result.get('message')}"})
        else:
            await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(),
                "level": "error", "msg": f"❌ Manual season post failed: {result.get('message')}"})
        return result
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)

@app.get("/api/export/episodes.csv")
async def export_episodes_csv():
    """Stream posted_metadata.json as a UTF-8 CSV download.
    Columns: episode_id, anime_title, episode_number, episode_title, channel, posted_at, series_id.
    Reads entirely from in-memory state — no MongoDB query, instant response.
    """
    meta = state.posted_metadata_ref or {}
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "episode_id", "anime_title", "episode_number", "episode_title",
        "channel", "posted_at", "series_id",
    ], extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for ep_id, row in meta.items():
        writer.writerow({
            "episode_id":     ep_id,
            "anime_title":    row.get("anime_title", ""),
            "episode_number": row.get("episode_number", ""),
            "episode_title":  row.get("episode_title", ""),
            "channel":        row.get("channel", ""),
            "posted_at":      row.get("posted_at", ""),
            "series_id":      row.get("series_id", ""),
        })
    output.seek(0)
    filename = f"herald_episodes_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/seasons.csv")
async def export_seasons_csv():
    """Stream posted_seasons.json as a UTF-8 CSV download.
    Columns: series_id, anime_title, posted_at, channel.
    """
    seasons = state.posted_seasons_ref or []
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "series_id", "anime_title", "posted_at", "channel",
    ], extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in seasons:
        writer.writerow({
            "series_id":   row.get("series_id", ""),
            "anime_title": row.get("anime_title", ""),
            "posted_at":   row.get("posted_at", ""),
            "channel":     row.get("channel", ""),
        })
    output.seek(0)
    filename = f"herald_seasons_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/metadata.json")
async def export_metadata_json():
    """Stream the full posted_metadata.json as a pretty-printed JSON download.
    Useful for migration, backup, or external processing.
    """
    meta = state.posted_metadata_ref or {}
    output = json.dumps(meta, indent=2, ensure_ascii=False, default=str)
    filename = f"herald_metadata_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    return StreamingResponse(
        iter([output]),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/all.csv")
async def export_all_csv():
    """Stream a combined CSV of all posted_metadata entries plus season entries,
    with a 'type' column (episode/season) distinguishing the two. Useful for a
    single-file audit of everything the bot has ever announced.
    """
    meta = state.posted_metadata_ref or {}
    seasons = state.posted_seasons_ref or []
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "type", "episode_id", "series_id", "anime_title",
        "episode_number", "episode_title", "channel", "posted_at",
    ], extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for ep_id, row in meta.items():
        writer.writerow({
            "type":           "episode",
            "episode_id":     ep_id,
            "series_id":      row.get("series_id", ""),
            "anime_title":    row.get("anime_title", ""),
            "episode_number": row.get("episode_number", ""),
            "episode_title":  row.get("episode_title", ""),
            "channel":        row.get("channel", ""),
            "posted_at":      row.get("posted_at", ""),
        })
    for row in seasons:
        writer.writerow({
            "type":        "season",
            "series_id":   row.get("series_id", ""),
            "anime_title": row.get("anime_title", ""),
            "channel":     row.get("channel", ""),
            "posted_at":   row.get("posted_at", ""),
        })
    output.seek(0)
    filename = f"herald_all_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/posted")
async def api_posted(page: int = 1, page_size: int = 30, search: str = "", channel: str = ""):
    meta = state.posted_metadata_ref or {}
    items = list(meta.values())
    if search:
        s = search.lower()
        items = [i for i in items if s in (i.get("anime_title","") + i.get("episode_title","")).lower()]
    if channel and channel != "all":
        items = [i for i in items if i.get("channel","") == channel]
    items.sort(key=lambda x: x.get("posted_at",""), reverse=True)
    total = len(items)
    start = (page - 1) * page_size
    return {"rows": items[start:start+page_size], "total": total, "page": page, "page_size": page_size}

# ── Config / Discord config endpoints ─────────────────────────────────────────
def _mask(val: str, show_last: int = 4) -> str:
    """Mask a value, showing only the last N characters."""
    if not val or len(val) <= show_last:
        return "••••••••"
    return "•" * max(8, len(val) - show_last) + val[-show_last:]

@app.get("/api/guild/info")
async def api_guild_info():
    """Live Discord server metadata, channels, and roles — powers the dropdown
    selectors on the revamped Discord Config page, replacing manual ID entry."""
    if state.guild_info_fn is None:
        return JSONResponse({"ok": False, "message": "Bot has not registered the guild info handler (is it fully started?)"}, status_code=503)
    try:
        result = await state.guild_info_fn()
        return result
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)

@app.post("/api/guild/test-announcement")
async def api_guild_test_announcement(payload: dict):
    """Send a clearly-labeled test announcement / test ping to a specific channel+role,
    for verifying Discord Config selections before relying on them. Never affects
    tracking files or counts as a real announcement."""
    if state.test_announcement_fn is None:
        return JSONResponse({"ok": False, "message": "Bot has not registered the test handler (is it fully started?)"}, status_code=503)
    kind = payload.get("kind", "episode")
    channel_id = payload.get("channel_id")
    role_id = payload.get("role_id")
    if not channel_id:
        return JSONResponse({"ok": False, "message": "channel_id is required"}, status_code=400)
    try:
        result = await state.test_announcement_fn(kind, int(channel_id), int(role_id) if role_id else None)
        if result.get("ok"):
            await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(),
                "level": "success", "msg": f"🧪 {result.get('message')}"})
        return result
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)

@app.get("/api/config")
async def get_config(reveal: bool = False):
    """Return config. Sensitive fields are masked unless reveal=true."""
    cfg = load_config()
    safe = dict(cfg)
    SENSITIVE = {"TOKEN", "MONGO_URI"}
    MASK_PARTIAL = {"EPISODES_CHANNEL_ID", "SEASONS_CHANNEL_ID", "GUILD_ID",
                    "MODERATOR_ROLE_ID", "EPISODES_ROLE_ID", "SEASONS_ROLE_ID", "MONGO_DB"}
    # Discord snowflake IDs (channel/role/guild IDs) are 64-bit integers that exceed
    # JavaScript's safe integer range (2^53-1). If returned as a JSON number, the
    # browser's JSON parser silently rounds them to the nearest representable float64,
    # corrupting the value (e.g. ...167263 becomes ...167232). This breaks any exact-match
    # logic client-side (like pre-selecting the right <option> in a channel/role dropdown).
    # Fix: always serialize these specific fields as strings, never numbers.
    SNOWFLAKE_FIELDS = {"EPISODES_CHANNEL_ID", "SEASONS_CHANNEL_ID", "GUILD_ID",
                         "MODERATOR_ROLE_ID", "EPISODES_ROLE_ID", "SEASONS_ROLE_ID"}
    for k in SNOWFLAKE_FIELDS:
        if k in safe and safe[k] is not None and not isinstance(safe[k], str):
            safe[k] = str(safe[k])
    for k in SENSITIVE:
        if k in safe and safe[k]:
            safe[k] = "••••••••••••(hidden)" if not reveal else safe[k]
    if not reveal:
        for k in MASK_PARTIAL:
            if k in safe and safe[k]:
                safe[k] = _mask(str(safe[k]))
    safe["_masked"] = not reveal
    return safe

@app.post("/api/config")
async def post_config(payload: dict, request: Request):
    """Save config.json and hot-reload safe fields into state."""
    cfg = load_config()
    before = dict(cfg)
    # Fields we allow editing from dashboard (never allow overwriting secrets via UI)
    EDITABLE = {"CHECK_INTERVAL","BATCH_THRESHOLD","MINI_BATCH_THRESHOLD","BATCH_HOURS",
                "START_DATE","REACTION_EMOJI","EPISODES_CHANNEL_ID","SEASONS_CHANNEL_ID",
                "GUILD_ID","MODERATOR_ROLE_ID","EPISODES_ROLE_ID","SEASONS_ROLE_ID",
                "ALLOWED_ROLE_IDS","MONGO_DB","EPISODES_COLLECTION","ANIMES_COLLECTION"}
    changed = []
    diffs = []
    for k, v in payload.items():
        if k in EDITABLE:
            old_v = before.get(k)
            if str(old_v) != str(v):
                diffs.append({"field": k, "old": old_v, "new": v})
            cfg[k] = v
            changed.append(k)
    save_config(cfg)

    # record audit entry (only if something actually changed)
    if diffs:
        client_ip = request.client.host if request and request.client else "unknown"
        _append_audit_entry({
            "ts": datetime.now(timezone.utc).isoformat(),
            "source": client_ip,
            "changes": diffs,
        })

    # hot-reload interval into state
    if "CHECK_INTERVAL" in changed:
        state.check_interval = int(cfg.get("CHECK_INTERVAL", 300))
    # hot-reload into the running bot (channel/guild/role IDs take effect immediately,
    # no restart needed) if the bot registered a reload callback
    live_applied = False
    if state.config_reload_fn is not None:
        try:
            state.config_reload_fn(cfg)
            live_applied = True
        except Exception as e:
            logger.warning(f"config_reload_fn failed: {e}")
    await state.broadcast("config_updated", {"changed": changed, "live": live_applied})
    msg = (f"⚙️ Config updated via dashboard: {', '.join(changed)} — applied live, no restart needed"
           if live_applied else
           f"⚙️ Config updated via dashboard: {', '.join(changed)} — restart bot to apply all changes")
    await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(), "level": "warning", "msg": msg})
    return {"ok": True, "changed": changed, "live": live_applied}

# ── Config audit log ───────────────────────────────────────────────────────────
AUDIT_LOG_FILE = os.path.join(_here, "config_audit_log.json")

def _load_audit_log() -> list:
    if os.path.exists(AUDIT_LOG_FILE):
        try:
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def _append_audit_entry(entry: dict):
    log = _load_audit_log()
    log.insert(0, entry)
    log = log[:200]  # keep last 200 entries
    try:
        with open(AUDIT_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(log, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

@app.get("/api/config/audit")
async def api_config_audit():
    return {"entries": _load_audit_log()}

# ── Dashboard settings ─────────────────────────────────────────────────────────
SETTINGS_FILE = os.path.join(_here, "dashboard_settings.json")

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f: return json.load(f)
    return {}

def save_settings(data):
    with open(SETTINGS_FILE, "w") as f: json.dump(data, f, indent=2)

@app.get("/api/settings")
async def get_settings(): return load_settings()

@app.post("/api/settings")
async def post_settings(payload: dict):
    current = load_settings()
    current.update(payload)
    save_settings(current)
    await state.broadcast("settings_updated", current)
    return {"ok": True, "settings": current}

# ── Alerts inbox ───────────────────────────────────────────────────────────────
ALERTS_FILE = os.path.join(_here, "dashboard_alerts.json")

def load_alerts():
    if os.path.exists(ALERTS_FILE):
        with open(ALERTS_FILE, "r") as f: return json.load(f)
    return []

def save_alerts(alerts):
    with open(ALERTS_FILE, "w") as f: json.dump(alerts[-100:], f, indent=2)

def push_alert(level: str, title: str, body: str, extra: Optional[Dict] = None):
    alerts = load_alerts()
    alerts.insert(0, {
        "ts": datetime.now(timezone.utc).isoformat(), "level": level,
        "title": title, "body": body, "read": False, "extra": extra or {},
    })
    save_alerts(alerts)
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(state.broadcast("alert", alerts[0]))
    except RuntimeError:
        pass

async def _push_alert_async(level: str, title: str, body: str, extra: Optional[Dict] = None):
    """Async-callable wrapper around push_alert, for use inside DashboardState methods
    which may run before this module-level function block is referenced at call time
    (Python resolves names at call time, so this is safe — it only needs to exist by
    the time notify_episode_posted actually fires, well after module load)."""
    push_alert(level, title, body, extra)

@app.get("/api/alerts")
async def get_alerts(): return {"alerts": load_alerts()}

@app.post("/api/alerts/clear")
async def clear_alerts():
    alerts = load_alerts()
    for a in alerts: a["read"] = True
    save_alerts(alerts)
    return {"ok": True}

# ── Mismatch detector ──────────────────────────────────────────────────────────
@app.get("/api/integrity")
async def api_integrity():
    posted = state.posted_episodes_ref or set()
    meta = state.posted_metadata_ref or {}
    in_posted_not_meta = [eid for eid in posted if eid not in meta]
    in_meta_not_posted = [eid for eid in meta if eid not in posted]
    null_fields = []
    for eid, v in meta.items():
        missing = [f for f in ["anime_title","episode_number","posted_at","channel"] if not v.get(f)]
        if missing:
            null_fields.append({"episode_id": eid, "missing": missing, "anime_title": v.get("anime_title","?")})

    # Detect a prior corruption recovery (file was backed up and reset on startup)
    corrupted_backups = []
    for d in [_here, os.path.dirname(_here)]:
        for fname in ["posted.json.corrupted_backup", "posted_metadata.json.corrupted_backup", "posted_seasons.json.corrupted_backup"]:
            p = os.path.join(d, fname)
            if os.path.exists(p):
                corrupted_backups.append(fname)

    # Cross-check: seasons that exist in MongoDB (episodeNumber == 1) but have not
    # been announced to SEASONS_CHANNEL_ID yet — reuses the same pending-seasons logic.
    pending_seasons_count = 0
    pending_seasons_preview = []
    try:
        pending_result = await api_seasons_pending(limit=10)
        if not pending_result.get("error"):
            pending_seasons_count = pending_result.get("count", 0)
            pending_seasons_preview = [p["anime_title"] for p in pending_result.get("pending", [])[:10]]
    except Exception:
        pass

    return {
        "posted_count": len(posted),
        "meta_count": len(meta),
        "in_posted_not_meta": in_posted_not_meta[:50],
        "in_meta_not_posted": in_meta_not_posted[:50],
        "null_fields": null_fields[:50],
        "corrupted_backups": corrupted_backups,
        "pending_seasons_count": pending_seasons_count,
        "pending_seasons_preview": pending_seasons_preview,
        "healthy": len(in_posted_not_meta) == 0 and len(in_meta_not_posted) == 0
                   and len(null_fields) == 0 and len(corrupted_backups) == 0
                   and pending_seasons_count == 0,
    }


@app.post("/api/integrity/fix")
async def api_integrity_fix():
    """Auto-fix the two most common, safe integrity mismatches:

    1. IDs in posted.json with no metadata entry — these are orphan IDs that
       prevent re-announcing but carry no useful data. Safe to remove from
       posted.json (they'll never be re-announced since we don't know their
       metadata, but the mismatch warning goes away).

    2. IDs in metadata with no entry in posted.json — these were tracked with
       metadata but the ID wasn't added to the dedup set. Safe to add them to
       posted.json so they're protected from re-announcing.

    A timestamped backup of both files is written before any change is made.
    Does NOT touch null_field entries (those require manual review) and does
    NOT modify MongoDB.
    """
    posted = state.posted_episodes_ref
    meta = state.posted_metadata_ref
    if posted is None or meta is None:
        return JSONResponse({"ok": False, "message": "State not ready — is the bot running?"}, status_code=503)

    in_posted_not_meta = [eid for eid in posted if eid not in meta]
    in_meta_not_posted = [eid for eid in meta if eid not in posted]

    if not in_posted_not_meta and not in_meta_not_posted:
        return {"ok": True, "message": "Nothing to fix — no mismatches found.", "fixed": 0}

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fixed_count = 0
    actions = []

    # Backup and fix posted.json
    posted_path = os.path.join(_here, "posted.json")
    if os.path.exists(posted_path):
        import shutil
        shutil.copy2(posted_path, posted_path + f".backup_{ts}")

    if in_posted_not_meta:
        # Remove orphan IDs from posted set
        for eid in in_posted_not_meta:
            posted.discard(eid)
        try:
            with open(posted_path, "w", encoding="utf-8") as f:
                json.dump(sorted(posted), f, indent=2)
            actions.append(f"Removed {len(in_posted_not_meta)} orphan ID(s) from posted.json")
            fixed_count += len(in_posted_not_meta)
        except Exception as e:
            return JSONResponse({"ok": False, "message": f"Failed to write posted.json: {e}"}, status_code=500)

    if in_meta_not_posted:
        # Add metadata IDs to the posted set
        for eid in in_meta_not_posted:
            posted.add(eid)
        try:
            with open(posted_path, "w", encoding="utf-8") as f:
                json.dump(sorted(posted), f, indent=2)
            actions.append(f"Added {len(in_meta_not_posted)} missing ID(s) from metadata into posted.json")
            fixed_count += len(in_meta_not_posted)
        except Exception as e:
            return JSONResponse({"ok": False, "message": f"Failed to write posted.json: {e}"}, status_code=500)

    msg = " | ".join(actions)
    await state.broadcast("log", {"ts": datetime.now(timezone.utc).isoformat(),
        "level": "success", "msg": f"✅ Integrity fix: {msg}"})
    return {"ok": True, "message": msg, "fixed": fixed_count, "backup_suffix": f".backup_{ts}"}

# ── WebSocket ──────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    state._ws_clients.append(ws)
    await ws.send_text(json.dumps({"event": "init", "data": {
        "logs": list(state.log_buffer)[-150:],
        "status": {"uptime_secs": _uptime_secs(), "next_scan_secs": _next_scan_secs(),
                   "mongo_ok": state.mongo_ok, "discord_ok": state.discord_ok,
                   "check_interval": state.check_interval, "error_count": state.error_count,
                   "maintenance_until": state.maintenance_until},
    }}, default=str))
    try:
        while True:
            await asyncio.sleep(3)  # 3s tick is plenty — uptime/scan countdown are
            # interpolated client-side between ticks, so this doesn't look "stuck"
            # while cutting WS message volume (and the JSON parse + DOM-update work
            # that follows each one) by 3x over the lifetime of every connection.
            await ws.send_text(json.dumps({"event": "tick", "data": {
                "uptime_secs": _uptime_secs(), "next_scan_secs": _next_scan_secs(),
                "mongo_ok": state.mongo_ok, "discord_ok": state.discord_ok,
                "error_count": state.error_count, "maintenance_until": state.maintenance_until,
            }}, default=str))
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if ws in state._ws_clients:
            state._ws_clients.remove(ws)

# ─────────────────────────────────────────────────────────────────────────────
async def start_dashboard(host: str = "127.0.0.1", port: int = 5050):
    config = uvicorn.Config(app, host=host, port=port, loop="none", log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    await server.serve()
