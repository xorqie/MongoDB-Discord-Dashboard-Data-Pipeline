import discord
from discord.ext import tasks, commands
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import json
from datetime import datetime, timedelta
import os
import traceback
from typing import List, Dict, Optional
import logging

# ---------------- LOGGING SETUP ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_events.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('AnimeBot')

error_logger = logging.getLogger('AnimeBot.Error')
error_handler = logging.FileHandler('error.log', encoding='utf-8')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)

# ---------------- CONFIG ----------------
# ── Dashboard ────────────────────────────────────────────────────────────────
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard"))
from dashboard_server import state as _dash, attach_log_handler, start_dashboard
attach_log_handler()  # wire all bot logs → browser live stream
# ─────────────────────────────────────────────────────────────────────────────

CONFIG_FILE = "config.json"

if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
else:
    raise FileNotFoundError("config.json not found!")

TOKEN = config["TOKEN"]
MONGO_URI = config["MONGO_URI"]
MONGO_DB = config["MONGO_DB"]
EPISODES_COLLECTION = config.get("EPISODES_COLLECTION", "episodes")
ANIMES_COLLECTION = config.get("ANIMES_COLLECTION", "animes")
CHECK_INTERVAL = config.get("CHECK_INTERVAL", 300)
BATCH_THRESHOLD = config.get("BATCH_THRESHOLD", 8)
MINI_BATCH_THRESHOLD = config.get("MINI_BATCH_THRESHOLD", 3)
BATCH_HOURS = config.get("BATCH_HOURS", 24)
START_DATE = config.get("START_DATE", "2025-09-01T00:00:00Z")
REACTION_EMOJI = config.get("REACTION_EMOJI", "🎉")

# SERVER/CHANNEL/ROLE RESTRICTIONS
# Read from config.json so they're editable live from the dashboard; fall back to the
# original hardcoded defaults if not present in config.json (keeps existing installs working).
ALLOWED_GUILD_ID = int(config.get("GUILD_ID", 0))
EPISODES_CHANNEL_ID = int(config.get("EPISODES_CHANNEL_ID", 0))  # Regular episodes
SEASONS_CHANNEL_ID = int(config.get("SEASONS_CHANNEL_ID", 0))    # New seasons
MODERATOR_ROLE_ID = int(config.get("MODERATOR_ROLE_ID", 0))      # Moderator role
EPISODES_ROLE_ID = int(config.get("EPISODES_ROLE_ID", 0))        # Role to ping for episode announcements
SEASONS_ROLE_ID = int(config.get("SEASONS_ROLE_ID", 0))          # Role to ping for season announcements

# One-time migration: if config.json doesn't yet have these keys, write the resolved
# values back to disk so the dashboard's Discord Config page can see and edit them.
_cfg_needs_save = False
for _k, _v in [("GUILD_ID", ALLOWED_GUILD_ID), ("EPISODES_CHANNEL_ID", EPISODES_CHANNEL_ID),
               ("SEASONS_CHANNEL_ID", SEASONS_CHANNEL_ID), ("MODERATOR_ROLE_ID", MODERATOR_ROLE_ID),
               ("EPISODES_ROLE_ID", EPISODES_ROLE_ID), ("SEASONS_ROLE_ID", SEASONS_ROLE_ID),
               ("WEEKLY_DIGEST_ENABLED", False),   # Set to true to enable Sunday 18:00 weekly summary
               ("DIGEST_CHANNEL_ID", None),         # Optional: dedicated digest channel. Defaults to EPISODES_CHANNEL_ID
               ]:
    if _k not in config:
        config[_k] = _v
        _cfg_needs_save = True
if _cfg_needs_save:
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        logger.info("Migrated Discord IDs into config.json (now editable from dashboard)")
    except Exception as e:
        logger.warning(f"Could not migrate Discord IDs into config.json: {e}")

POSTED_FILE = "posted.json"
POSTED_METADATA_FILE = "posted_metadata.json"
# ----------------------------------------

intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------------- MongoDB Connection ----------------
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]
episodes_collection = db[EPISODES_COLLECTION]
animes_collection = db[ANIMES_COLLECTION]

logger.info(f"MongoDB client initialized for database: {MONGO_DB}")
_dash.mongo_client   = mongo_client
_dash.mongo_db_name  = MONGO_DB
_dash.episodes_col   = EPISODES_COLLECTION
_dash.animes_col     = ANIMES_COLLECTION
_dash.check_interval = CHECK_INTERVAL
_dash.mongo_ok       = True

# ---------------- Load posted episodes ----------------
if os.path.exists(POSTED_FILE):
    try:
        with open(POSTED_FILE, "r", encoding="utf-8") as f:
            posted_episodes = set(json.load(f))
        logger.info(f"Loaded {len(posted_episodes)} posted episodes from {POSTED_FILE}")
    except json.JSONDecodeError as e:
        backup_name = POSTED_FILE + ".corrupted_backup"
        try:
            os.replace(POSTED_FILE, backup_name)
        except Exception:
            pass
        error_logger.error(f"{POSTED_FILE} was corrupted ({e}); backed up to {backup_name} and starting fresh")
        posted_episodes = set()
else:
    posted_episodes = set()
    logger.info(f"{POSTED_FILE} not found, starting with empty posted episodes set")

# Load posted metadata
if os.path.exists(POSTED_METADATA_FILE):
    try:
        with open(POSTED_METADATA_FILE, "r", encoding="utf-8") as f:
            posted_metadata = json.load(f)
        logger.info(f"Loaded posted metadata with {len(posted_metadata)} entries")
    except json.JSONDecodeError as e:
        backup_name = POSTED_METADATA_FILE + ".corrupted_backup"
        try:
            os.replace(POSTED_METADATA_FILE, backup_name)
        except Exception:
            pass
        error_logger.error(f"{POSTED_METADATA_FILE} was corrupted ({e}); backed up to {backup_name} and starting fresh. "
                            f"NOTE: this resets dashboard stats (Total Posted / Total Seasons) until episodes post again. "
                            f"Episode IDs in {POSTED_FILE} are preserved, so no re-announcements will occur.")
        posted_metadata = {}
else:
    posted_metadata = {}
    logger.info(f"{POSTED_METADATA_FILE} not found, starting with empty metadata")

# Load posted seasons (dedicated, resilient counter for season announcements,
# independent of posted_metadata.json so a corruption there can't zero out season stats)
POSTED_SEASONS_FILE = "posted_seasons.json"
if os.path.exists(POSTED_SEASONS_FILE):
    try:
        with open(POSTED_SEASONS_FILE, "r", encoding="utf-8") as f:
            posted_seasons = json.load(f)
        if not isinstance(posted_seasons, list):
            posted_seasons = []
        logger.info(f"Loaded {len(posted_seasons)} posted seasons from {POSTED_SEASONS_FILE}")
    except json.JSONDecodeError as e:
        backup_name = POSTED_SEASONS_FILE + ".corrupted_backup"
        try:
            os.replace(POSTED_SEASONS_FILE, backup_name)
        except Exception:
            pass
        error_logger.error(f"{POSTED_SEASONS_FILE} was corrupted ({e}); backed up to {backup_name} and starting fresh")
        posted_seasons = []
else:
    # Backfill from existing metadata so the counter isn't 0 on first run after upgrade
    posted_seasons = [
        {"series_id": v.get("series_id"), "anime_title": v.get("anime_title"),
         "posted_at": v.get("posted_at"), "channel": v.get("channel")}
        for v in posted_metadata.values() if v.get("channel") == "Seasons"
    ]
    logger.info(f"{POSTED_SEASONS_FILE} not found, backfilled {len(posted_seasons)} entries from existing metadata")

def save_posted_seasons():
    try:
        with open(POSTED_SEASONS_FILE, "w", encoding="utf-8") as f:
            json.dump(posted_seasons, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ Saved {len(posted_seasons)} posted seasons to {POSTED_SEASONS_FILE}")
    except Exception as e:
        error_logger.error(f"Failed to save posted seasons: {e}\n{traceback.format_exc()}")

# Load posted anime seasons (Discord message-ID based counter for "Total Seasons
# Announced" — counts unique bot-authored messages actually present in the seasons
# announcement channel, SEASONS_CHANNEL_ID. This is independent of MongoDB and of
# posted_seasons.json; it reflects exactly what's live on Discord right now.
# Populated by sync_posted_animeseasons(), called once on startup and on-demand via
# the dashboard. Does not affect or duplicate the bot's posting logic.
POSTED_ANIMESEASONS_FILE = "posted_animeseasons.json"
if os.path.exists(POSTED_ANIMESEASONS_FILE):
    try:
        with open(POSTED_ANIMESEASONS_FILE, "r", encoding="utf-8") as f:
            posted_animeseasons = set(json.load(f))
        logger.info(f"Loaded {len(posted_animeseasons)} posted anime seasons from {POSTED_ANIMESEASONS_FILE}")
    except json.JSONDecodeError as e:
        backup_name = POSTED_ANIMESEASONS_FILE + ".corrupted_backup"
        try:
            os.replace(POSTED_ANIMESEASONS_FILE, backup_name)
        except Exception:
            pass
        error_logger.error(f"{POSTED_ANIMESEASONS_FILE} was corrupted ({e}); backed up to {backup_name} and starting fresh")
        posted_animeseasons = set()
else:
    posted_animeseasons = set()
    logger.info(f"{POSTED_ANIMESEASONS_FILE} not found, will sync from Discord on startup")

# Share live references with the dashboard
_dash.posted_episodes_ref = posted_episodes
_dash.posted_metadata_ref = posted_metadata
_dash.posted_seasons_ref = posted_seasons
_dash.posted_animeseasons_ref = posted_animeseasons

def _apply_live_config(cfg: dict):
    """Called by the dashboard when config.json is saved. Hot-applies channel/guild/role
    ID changes to the running bot without requiring a restart."""
    global ALLOWED_GUILD_ID, EPISODES_CHANNEL_ID, SEASONS_CHANNEL_ID
    global MODERATOR_ROLE_ID, EPISODES_ROLE_ID, SEASONS_ROLE_ID
    global CHECK_INTERVAL, BATCH_THRESHOLD, MINI_BATCH_THRESHOLD, BATCH_HOURS, START_DATE, REACTION_EMOJI

    if "GUILD_ID" in cfg:
        ALLOWED_GUILD_ID = int(cfg["GUILD_ID"])
    if "EPISODES_CHANNEL_ID" in cfg:
        EPISODES_CHANNEL_ID = int(cfg["EPISODES_CHANNEL_ID"])
    if "SEASONS_CHANNEL_ID" in cfg:
        SEASONS_CHANNEL_ID = int(cfg["SEASONS_CHANNEL_ID"])
    if "MODERATOR_ROLE_ID" in cfg:
        MODERATOR_ROLE_ID = int(cfg["MODERATOR_ROLE_ID"])
    if "EPISODES_ROLE_ID" in cfg:
        EPISODES_ROLE_ID = int(cfg["EPISODES_ROLE_ID"])
    if "SEASONS_ROLE_ID" in cfg:
        SEASONS_ROLE_ID = int(cfg["SEASONS_ROLE_ID"])
    if "CHECK_INTERVAL" in cfg:
        CHECK_INTERVAL = int(cfg["CHECK_INTERVAL"])
    if "BATCH_THRESHOLD" in cfg:
        BATCH_THRESHOLD = int(cfg["BATCH_THRESHOLD"])
    if "MINI_BATCH_THRESHOLD" in cfg:
        MINI_BATCH_THRESHOLD = int(cfg["MINI_BATCH_THRESHOLD"])
    if "BATCH_HOURS" in cfg:
        BATCH_HOURS = int(cfg["BATCH_HOURS"])
    if "START_DATE" in cfg:
        START_DATE = cfg["START_DATE"]
    if "REACTION_EMOJI" in cfg:
        REACTION_EMOJI = cfg["REACTION_EMOJI"]

    logger.info(
        f"🔄 Live config reload applied — Episodes Channel: {EPISODES_CHANNEL_ID}, "
        f"Seasons Channel: {SEASONS_CHANNEL_ID}, Guild: {ALLOWED_GUILD_ID}"
    )

_dash.config_reload_fn = _apply_live_config

# ---------------- Cache for anime data ----------------
anime_cache = {}

# ---------------- Permission Check ----------------
def is_moderator_or_admin():
    """Check if user is admin or has moderator role"""
    async def predicate(ctx):
        if ctx.author.guild_permissions.administrator:
            return True
        moderator_role = ctx.guild.get_role(MODERATOR_ROLE_ID)
        if moderator_role and moderator_role in ctx.author.roles:
            return True
        return False
    return commands.check(predicate)

# ---------------- Helpers ----------------
def save_posted_episodes():
    try:
        with open(POSTED_FILE, "w", encoding="utf-8") as f:
            json.dump(list(posted_episodes), f, indent=4, ensure_ascii=False)
        logger.info(f"✅ Saved {len(posted_episodes)} posted episodes to {POSTED_FILE}")
    except Exception as e:
        error_logger.error(f"Failed to save posted episodes: {e}\n{traceback.format_exc()}")

def save_posted_metadata():
    try:
        with open(POSTED_METADATA_FILE, "w", encoding="utf-8") as f:
            json.dump(posted_metadata, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ Saved posted metadata with {len(posted_metadata)} entries")
    except Exception as e:
        error_logger.error(f"Failed to save posted metadata: {e}\n{traceback.format_exc()}")

def add_to_posted(episode_id: str, episode_data: Dict, anime_title: str, channel_name: str, posted_at: Optional[datetime] = None):
    """Add episode to posted list with metadata
    
    Args:
        episode_id: Episode ID from MongoDB
        episode_data: Episode document from MongoDB
        anime_title: Title of the anime
        channel_name: Name of the channel where it was posted
        posted_at: Optional datetime when the announcement was posted (defaults to now)
    """
    posted_episodes.add(episode_id)
    
    # Use provided timestamp or current time
    if posted_at is None:
        posted_at = datetime.utcnow()
    
    # Convert to ISO format string if it's a datetime object
    if isinstance(posted_at, datetime):
        posted_at_str = posted_at.isoformat()
    else:
        posted_at_str = str(posted_at)
    
    def _to_str(v):
        if isinstance(v, datetime):
            return v.isoformat()
        return str(v) if v is not None else "N/A"

    posted_metadata[episode_id] = {
        "episode_id": episode_id,
        "series_id": str(episode_data.get("seriesId", "")),
        "anime_title": anime_title,
        "episode_number": episode_data.get("episodeNumber", "N/A"),
        "episode_title": episode_data.get("episodeTitle", "N/A"),
        "posted_at": posted_at_str,  # Discord announcement timestamp
        "channel": channel_name,
        # Episode timestamps from database
        "episode_created_at": _to_str(episode_data.get("createdAt")),
        "episode_updated_at": _to_str(episode_data.get("updatedAt")),
    }
    save_posted_episodes()
    save_posted_metadata()
    if channel_name == "Seasons":
        posted_seasons.append({
            "series_id": str(episode_data.get("seriesId", "")),
            "anime_title": anime_title,
            "posted_at": posted_at_str,
            "channel": channel_name,
        })
        save_posted_seasons()
    _dash.notify_episode_posted(posted_metadata[episode_id])

def is_recent(created_at_str: str, hours: int = BATCH_HOURS) -> bool:
    try:
        created_at_str = str(created_at_str).strip()
        
        # Try parsing with multiple formats
        created_at = None
        
        # Format 1: ISO format with T and timezone
        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except:
            pass
        
        # Format 2: Space-separated format
        if not created_at:
            try:
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S.%f")
            except:
                pass
        
        # Format 3: Without microseconds
        if not created_at:
            try:
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        if not created_at:
            logger.error(f"Could not parse date for recent check: {created_at_str}")
            return False
        
        now = datetime.utcnow()
        
        # Make both timezone-naive for comparison
        if created_at.tzinfo is not None:
            created_at = created_at.replace(tzinfo=None)
        
        is_recent_result = now - created_at <= timedelta(hours=hours)
        logger.debug(f"Date check: {created_at_str} is {'recent' if is_recent_result else 'old'} (within {hours}h)")
        return is_recent_result
        
    except Exception as e:
        error_logger.error(f"Error parsing date '{created_at_str}': {e}")
        return False

def after_start_date(created_at_str: str, start_date: str = START_DATE) -> bool:
    try:
        # Handle multiple datetime formats
        created_at_str = str(created_at_str).strip()
        
        # Remove timezone info from START_DATE for comparison
        start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        
        # Try parsing with multiple formats
        created_at = None
        
        # Format 1: ISO format with T and timezone
        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        except:
            pass
        
        # Format 2: Space-separated format
        if not created_at:
            try:
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S.%f")
            except:
                pass
        
        # Format 3: Space-separated without microseconds
        if not created_at:
            try:
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
            except:
                pass
        
        # Format 4: Just date
        if not created_at:
            try:
                created_at = datetime.strptime(created_at_str, "%Y-%m-%d")
            except:
                pass
        
        if not created_at:
            logger.error(f"Could not parse date: {created_at_str}")
            return False
        
        # Make both timezone-aware for comparison (assume UTC if no timezone)
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=start_dt.tzinfo)
        
        result = created_at >= start_dt
        logger.debug(f"Start date check: {created_at_str} is {'after' if result else 'before'} {start_date}")
        return result
        
    except Exception as e:
        error_logger.error(f"Error comparing start date '{created_at_str}': {e}")
        return False

def get_image_url(url: Optional[str]) -> Optional[str]:
    """Validate and return image URL, or None if invalid"""
    if not url:
        return None
    if isinstance(url, str) and url.startswith(('http://', 'https://')):
        return url
    return None

def truncate_text(text: str, max_length: int = 200) -> str:
    """Truncate text to max_length and add ellipsis if needed"""
    if not text:
        return ""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

def get_available_players(episode: Dict) -> Dict[str, List[str]]:
    """Extract available players from episode data"""
    players = episode.get("players", {})
    available = {
        "sub": [],
        "dub": [],
        "sinhro": []
    }
    
    # Check sub players
    sub_players = players.get("sub", {})
    if sub_players.get("hd1"):
        available["sub"].append("Japanski sa srpskim")
    if sub_players.get("hd2"):
        available["sub"].append("Japanski sa engleskim")
    
    # Check dub players
    dub_players = players.get("dub", {})
    if dub_players.get("hd1"):
        available["dub"].append("Engleski sa srpskim")
    if dub_players.get("hd2"):
        available["dub"].append("Engleski sa engleskim")
    
    # Check sinhro players (if exists)
    sinhro_players = players.get("sinhro", {})
    if sinhro_players.get("hd1"):
        available["sinhro"].append("Sinhronizovano HD1")
    if sinhro_players.get("hd2"):
        available["sinhro"].append("Sinhronizovano HD2")
    
    return available

def format_players_field(players_dict: Dict[str, List[str]]) -> str:
    """Format available players into a readable string"""
    all_players = []
    
    for category, players_list in players_dict.items():
        all_players.extend(players_list)
    
    if not all_players:
        return "Nema dostupnih"
    
    return ", ".join(all_players)

async def get_season_available_players(series_id: str) -> Dict[str, List[str]]:
    """Get all available players for a season by checking all episodes"""
    try:
        from bson import ObjectId
        cursor = episodes_collection.find({"seriesId": ObjectId(series_id)})
        episodes = await cursor.to_list(length=None)
        
        combined_players = {
            "sub": set(),
            "dub": set(),
            "sinhro": set()
        }
        
        for episode in episodes:
            ep_players = get_available_players(episode)
            for category, players_list in ep_players.items():
                combined_players[category].update(players_list)
        
        # Convert sets back to lists
        return {k: list(v) for k, v in combined_players.items()}
    except Exception as e:
        error_logger.error(f"Error getting season players: {e}")
        return {"sub": [], "dub": [], "sinhro": []}

# ---------------- Fetch anime data ----------------
async def fetch_anime_data(series_id: str) -> Optional[Dict]:
    if series_id in anime_cache:
        logger.debug(f"Cache hit for anime series ID: {series_id}")
        return anime_cache[series_id]
    
    try:
        from bson import ObjectId
        logger.info(f"Fetching anime data for series ID: {series_id}")
        anime_data = await animes_collection.find_one({"_id": ObjectId(series_id)})
        if anime_data:
            anime_cache[series_id] = anime_data
            anime_title = anime_data.get("tmdbTitle") or anime_data.get("malTitle") or "Unknown"
            logger.info(f"✅ Fetched anime data: {anime_title}")
            logger.debug(f"   Poster: {anime_data.get('poster', 'N/A')}")
            logger.debug(f"   Backdrop: {anime_data.get('backdrop', 'N/A')}")
            logger.debug(f"   ThumbnailDub: {anime_data.get('thumbnailDub', 'N/A')}")
            return anime_data
        else:
            logger.warning(f"⚠️ No anime data found for series ID: {series_id}")
    except Exception as e:
        error_logger.error(f"Error fetching anime data for {series_id}: {e}\n{traceback.format_exc()}")
    return None

# ---------------- Fetch episodes from MongoDB ----------------
async def fetch_new_episodes() -> List[Dict]:
    try:
        logger.info("🔍 Fetching new episodes from MongoDB...")
        
        # Show total count first
        total_count = await episodes_collection.count_documents({})
        logger.info(f"📚 Total episodes in entire database: {total_count}")
        
        # Parse the start date
        start_dt = datetime.fromisoformat(START_DATE.replace("Z", "+00:00"))
        
        # Fetch ALL episodes after START_DATE
        cursor = episodes_collection.find({
            "createdAt": {"$gte": START_DATE}
        }).sort("createdAt", 1)
        
        episodes = await cursor.to_list(length=None)
        
        # If no results with ISO comparison, try with datetime object
        if not episodes:
            logger.info("No episodes with ISO date filter, trying datetime comparison...")
            cursor = episodes_collection.find({}).sort("createdAt", -1)
            all_episodes = await cursor.to_list(length=None)
            
            # Filter episodes after start date
            episodes = [ep for ep in all_episodes if after_start_date(ep.get("createdAt", ""))]
        
        logger.info(f"📦 Fetched {len(episodes)} total episodes from MongoDB (after {START_DATE})")
        
        new_episodes = [ep for ep in episodes if str(ep["_id"]) not in posted_episodes]
        logger.info(f"🆕 Found {len(new_episodes)} new episodes (not yet posted)")
        
        if new_episodes:
            logger.info(f"📋 New episodes preview:")
            for ep in new_episodes[:5]:  # Show first 5
                logger.info(f"   - Episode {ep.get('episodeNumber')}: {ep.get('episodeTitle', 'N/A')}")
            if len(new_episodes) > 5:
                logger.info(f"   ... and {len(new_episodes) - 5} more")
        
        return new_episodes
    except Exception as e:
        error_logger.error(f"MongoDB fetch error: {e}\n{traceback.format_exc()}")
        return []

# ---------------- Get allowed channel ----------------
async def get_channel_by_id(channel_id: int):
    guild = bot.get_guild(ALLOWED_GUILD_ID)
    if not guild:
        error_logger.error(f"❌ Bot is not in the allowed server (ID: {ALLOWED_GUILD_ID})")
        return None
    
    channel = guild.get_channel(channel_id)
    if not channel:
        error_logger.error(f"❌ Channel ID {channel_id} not found in server {guild.name}")
        return None
    
    logger.debug(f"✅ Found channel: #{channel.name} in {guild.name}")
    return channel

# ---------------- Send embed ----------------
async def send_embed(
    channel,
    title: str,
    description: str,
    link: Optional[str] = None,
    banner: Optional[str] = None,
    thumbnail: Optional[str] = None,
    fields: Optional[List[Dict]] = None,
    color: discord.Color = discord.Color.green(),
    anime_description: Optional[str] = None,
    include_description: bool = True,
    is_season: bool = False
):
    embed = discord.Embed(title=title, description=description, color=color)
    
    # Add anime description if provided and allowed
    if anime_description and include_description:
        truncated_desc = truncate_text(anime_description, 300)
        embed.add_field(name="📝 Opis", value=truncated_desc, inline=False)
    
    if fields:
        for field in fields:
            embed.add_field(
                name=field.get("name", ""),
                value=field.get("value", ""),
                inline=field.get("inline", False)
            )
    
    if link:
        embed.add_field(name="🎬 Gledaj sada:", value=f"[Klikni ovdje]({link})", inline=False)
    
    # Validate and set banner image
    banner_url = get_image_url(banner)
    if banner_url:
        embed.set_image(url=banner_url)
        logger.debug(f"Setting banner: {banner_url}")
    else:
        logger.warning("⚠️ No valid banner URL available")
    
    # Validate and set thumbnail image
    thumbnail_url = get_image_url(thumbnail)
    if thumbnail_url:
        embed.set_thumbnail(url=thumbnail_url)
        logger.debug(f"Setting thumbnail: {thumbnail_url}")
    else:
        logger.warning("⚠️ No valid thumbnail URL available")

    view = None
    if link:
        view = discord.ui.View()
        view.add_item(discord.ui.Button(label="Gledaj sada", url=link, style=discord.ButtonStyle.link))

    try:
        # Ping appropriate role based on channel
        role_mention = f"<@&{SEASONS_ROLE_ID}>" if is_season else f"<@&{EPISODES_ROLE_ID}>"
        message = await channel.send(role_mention, embed=embed, view=view)
        if REACTION_EMOJI:
            await message.add_reaction(REACTION_EMOJI)
        logger.info(f"✅ Discord announcement sent: {title}")
        return message
    except discord.Forbidden:
        error_logger.error(f"❌ Missing permissions to send message in #{channel.name}")
        return None
    except discord.HTTPException as e:
        error_logger.error(f"❌ HTTP error sending embed: {e}")
        return None
    except Exception as e:
        error_logger.error(f"Error sending embed: {e}\n{traceback.format_exc()}")
        return None

# ---------------- Process episodes ----------------
async def process_episodes(episodes: List[Dict], episodes_channel, seasons_channel):
    if not episodes:
        logger.info("No episodes to process")
        return

    series_groups = {}
    for ep in episodes:
        series_id = str(ep.get("seriesId"))
        series_groups.setdefault(series_id, []).append(ep)

    logger.info(f"📊 Grouped episodes into {len(series_groups)} anime series")

    for series_id, eps in series_groups.items():
        # Sort by episode number
        eps.sort(key=lambda x: x.get("episodeNumber", 0))
        
        logger.info(f"🎬 Processing series ID: {series_id} with {len(eps)} episodes")
        
        anime_data = await fetch_anime_data(series_id)
        
        if not anime_data:
            logger.error(f"❌ Cannot process episodes without anime data for series ID: {series_id}")
            continue
        
        anime_title = anime_data.get("tmdbTitle") or anime_data.get("malTitle") or "Unknown Anime"
        anime_description = anime_data.get("description", "")
        
        # Get images from anime data
        banner_url = get_image_url(anime_data.get("backdrop")) or get_image_url(anime_data.get("poster"))
        thumbnail_url = get_image_url(anime_data.get("poster")) or get_image_url(anime_data.get("thumbnailDub"))
        
        logger.info(f"🖼️  Images for {anime_title}:")
        logger.info(f"   Banner: {banner_url if banner_url else '❌ NOT AVAILABLE'}")
        logger.info(f"   Thumbnail: {thumbnail_url if thumbnail_url else '❌ NOT AVAILABLE'}")
        
        if not banner_url and not thumbnail_url:
            logger.warning(f"⚠️ No images available for {anime_title} - proceeding with text-only embed")

        # Filter recent episodes based on createdAt timestamp
        recent_eps = [ep for ep in eps if is_recent(ep.get("createdAt", ""))]
        
        logger.info(f"📺 '{anime_title}': {len(eps)} new episodes, {len(recent_eps)} recent (within {BATCH_HOURS}h)")

        # BATCH: 8+ recent episodes (FULL SEASON)
        if len(recent_eps) >= BATCH_THRESHOLD:
            first_ep = recent_eps[0]["episodeNumber"]
            last_ep = recent_eps[-1]["episodeNumber"]
            
            logger.info(f"🌟 Posting FULL SEASON batch for {anime_title}: Episodes {first_ep}-{last_ep}")
            
            # Get season available players
            season_players = await get_season_available_players(series_id)
            players_text = format_players_field(season_players)
            
            fields = []
            if anime_data.get("tmdbRating"):
                fields.append({"name": "⭐ TMDB ocena", "value": str(anime_data["tmdbRating"]), "inline": True})
            if anime_data.get("malRating"):
                fields.append({"name": "⭐ MAL ocena", "value": str(anime_data["malRating"]), "inline": True})
            if anime_data.get("studio"):
                fields.append({"name": "🎨 Studio", "value": anime_data["studio"], "inline": True})
            if anime_data.get("ttype"):
                fields.append({"name": "📺 Tip", "value": anime_data["ttype"], "inline": True})
            
            # Add players field
            fields.append({"name": "🎬 Dostupno", "value": players_text, "inline": False})
            
            season_message = await send_embed(
                seasons_channel,
                title="🌟 New Season Announced!",
                description=f"**{anime_title}**\n\nEpisodes {first_ep}–{last_ep} are now available!",
                banner=banner_url,
                thumbnail=thumbnail_url,
                fields=fields,
                color=discord.Color.gold(),
                anime_description=anime_description,
                include_description=True,
                is_season=True
            )

            # Track the real Discord message ID for the "Total Seasons Announced" stat
            # (posted_animeseasons.json). Purely additive — does not affect posting/batching.
            if season_message is not None:
                posted_animeseasons.add(str(season_message.id))
                try:
                    with open(POSTED_ANIMESEASONS_FILE, "w", encoding="utf-8") as f:
                        json.dump(sorted(posted_animeseasons), f, indent=2)
                except Exception as e:
                    error_logger.error(f"Failed to update {POSTED_ANIMESEASONS_FILE}: {e}")

            # Mark all episodes in the batch as posted
            for ep in recent_eps:
                add_to_posted(str(ep["_id"]), ep, anime_title, "Seasons")
                logger.debug(f"Marked episode {ep.get('episodeNumber')} as posted")

        # MINI BATCH: 3-7 recent episodes
        elif len(recent_eps) >= MINI_BATCH_THRESHOLD:
            first_ep = recent_eps[0]["episodeNumber"]
            last_ep = recent_eps[-1]["episodeNumber"]
            
            logger.info(f"📢 Posting MINI BATCH for {anime_title}: Episodes {first_ep}-{last_ep}")
            
            # Get available players for these episodes
            combined_players = {
                "sub": set(),
                "dub": set(),
                "sinhro": set()
            }
            
            for ep in recent_eps:
                ep_players = get_available_players(ep)
                for category, players_list in ep_players.items():
                    combined_players[category].update(players_list)
            
            players_dict = {k: list(v) for k, v in combined_players.items()}
            players_text = format_players_field(players_dict)
            
            fields = [{"name": "🎬 Dostupno", "value": players_text, "inline": False}]
            
            await send_embed(
                episodes_channel,
                title="📢 Multiple Episodes Released!",
                description=f"**{anime_title}**\n\nEpisodes {first_ep}–{last_ep} are now available!",
                banner=banner_url,
                thumbnail=thumbnail_url,
                fields=fields,
                color=discord.Color.blue(),
                include_description=False,
                is_season=False
            )
            
            # Mark all episodes in the mini batch as posted
            for ep in recent_eps:
                add_to_posted(str(ep["_id"]), ep, anime_title, "Episodes (Batch)")
                logger.debug(f"Marked episode {ep.get('episodeNumber')} as posted")
        
        # INDIVIDUAL EPISODES: Less than 3 recent episodes OR old episodes
        else:
            logger.info(f"📝 Posting {len(eps)} individual episodes for {anime_title}")
            
            for ep in eps:
                ep_number = ep.get("episodeNumber", "N/A")
                ep_title = ep.get("episodeTitle", "Bez naslova")
                is_new_season = ep_number == 1
                
                logger.info(f"{'🌟' if is_new_season else '🎉'} Posting episode {ep_number}: {ep_title}")
                
                # Get available players for this episode
                ep_players = get_available_players(ep)
                players_text = format_players_field(ep_players)
                
                fields = []
                if anime_data.get("genres") and len(anime_data["genres"]) > 0:
                    genres_str = ", ".join(anime_data["genres"][:3])
                    fields.append({"name": "🎭 Zanrovi", "value": genres_str, "inline": False})
                if anime_data.get("tmdbRating"):
                    fields.append({"name": "⭐ TMDB", "value": str(anime_data["tmdbRating"]), "inline": True})
                if anime_data.get("malRating"):
                    fields.append({"name": "⭐ MAL", "value": str(anime_data["malRating"]), "inline": True})
                
                # Add players field
                fields.append({"name": "🎬 Dostupno", "value": players_text, "inline": False})
                
                embed_title = "🌟 New Season Started!" if is_new_season else "🎉 New Episode Released!"
                embed_color = discord.Color.gold() if is_new_season else discord.Color.green()
                
                # Choose channel based on whether it's a new season
                target_channel = seasons_channel if is_new_season else episodes_channel
                channel_name = "Seasons" if is_new_season else "Episodes"
                
                sent_message = await send_embed(
                    target_channel,
                    title=embed_title,
                    description=f"**{anime_title}**\n\nEpisode {ep_number}: *{ep_title}*",
                    banner=banner_url,
                    thumbnail=thumbnail_url,
                    fields=fields,
                    color=embed_color,
                    anime_description=anime_description,
                    include_description=is_new_season,
                    is_season=is_new_season
                )

                # Track the real Discord message ID for "Total Seasons Announced"
                # (posted_animeseasons.json) when this was a season start. Purely
                # additive tracking — does not affect posting/batching logic.
                if is_new_season and sent_message is not None:
                    posted_animeseasons.add(str(sent_message.id))
                    try:
                        with open(POSTED_ANIMESEASONS_FILE, "w", encoding="utf-8") as f:
                            json.dump(sorted(posted_animeseasons), f, indent=2)
                    except Exception as e:
                        error_logger.error(f"Failed to update {POSTED_ANIMESEASONS_FILE}: {e}")

                add_to_posted(str(ep["_id"]), ep, anime_title, channel_name)
                logger.debug(f"Marked episode {ep_number} as posted")
                await asyncio.sleep(2)

# ---------------- Check new episodes ----------------
async def manual_post_season(series_id: str):
    """Manually post a single season announcement for a specific anime, triggered
    explicitly from the dashboard's 'Post This Season' button.

    This is intentionally a SEPARATE code path from the automatic check_new_episodes /
    process_episodes scan loop — it does not run on a timer, does not batch multiple
    series together, and is only ever invoked by an explicit human action. It reuses
    the same send_embed() helper and the same embed shape as the existing !testseason
    command, just targeted at a specific series_id instead of "whatever is latest".

    Returns a dict: {"ok": bool, "message": str}
    """
    try:
        from bson import ObjectId
        anime_data = await animes_collection.find_one({"_id": ObjectId(series_id)})
        if not anime_data:
            return {"ok": False, "message": f"Anime with series_id {series_id} not found in database"}

        channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        if not channel:
            return {"ok": False, "message": "Could not find the configured seasons channel"}

        anime_title = anime_data.get("tmdbTitle") or anime_data.get("malTitle") or "Unknown Anime"
        anime_description = anime_data.get("description", "")
        banner_url = get_image_url(anime_data.get("backdrop")) or get_image_url(anime_data.get("poster"))
        thumbnail_url = get_image_url(anime_data.get("poster")) or get_image_url(anime_data.get("thumbnailDub"))

        season_players = await get_season_available_players(series_id)
        players_text = format_players_field(season_players)
        episode_count = anime_data.get("episodes", "N/A")

        fields = []
        if anime_data.get("tmdbRating"):
            fields.append({"name": "⭐ TMDB ocena", "value": str(anime_data["tmdbRating"]), "inline": True})
        if anime_data.get("malRating"):
            fields.append({"name": "⭐ MAL ocena", "value": str(anime_data["malRating"]), "inline": True})
        if anime_data.get("studio"):
            fields.append({"name": "🎨 Studio", "value": anime_data["studio"], "inline": True})
        if anime_data.get("ttype"):
            fields.append({"name": "📺 Tip", "value": anime_data["ttype"], "inline": True})
        fields.append({"name": "🎬 Dostupno", "value": players_text, "inline": False})

        logger.info(f"📺 Manual season post triggered from dashboard: {anime_title} (series_id={series_id})")

        message = await send_embed(
            channel,
            title="🌟 New Season Announced!",
            description=f"**{anime_title}**\n\n{episode_count} episode{'s' if episode_count != 1 else ''} are now available!",
            banner=banner_url,
            thumbnail=thumbnail_url,
            fields=fields,
            color=discord.Color.gold(),
            anime_description=anime_description,
            include_description=True,
            is_season=True,
        )

        if message is None:
            return {"ok": False, "message": "Failed to send announcement (check error.log)"}

        # Record this in the same tracking files the automatic flow uses, so the
        # dashboard's "Total Seasons Announced" stat reflects this post too.
        posted_at = datetime.utcnow()
        posted_seasons.append({
            "series_id": series_id,
            "anime_title": anime_title,
            "posted_at": posted_at.isoformat(),
            "channel": "Seasons",
        })
        save_posted_seasons()

        # Track the real Discord message ID too, so posted_animeseasons.json (the
        # source of truth for "Total Seasons Announced") reflects this post instantly
        # without waiting for the next full channel sync.
        posted_animeseasons.add(str(message.id))
        try:
            with open(POSTED_ANIMESEASONS_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(posted_animeseasons), f, indent=2)
        except Exception as e:
            error_logger.error(f"Failed to update {POSTED_ANIMESEASONS_FILE}: {e}")

        logger.info(f"✅ Manual season announcement sent and tracked: {anime_title}")
        return {"ok": True, "message": f"Posted season announcement for {anime_title} to #{channel.name}"}

    except Exception as e:
        error_logger.error(f"Manual season post failed for series_id {series_id}: {e}\n{traceback.format_exc()}")
        return {"ok": False, "message": str(e)}

# ══════════════════════════════════════════════════════════════════════════════
# WEEKLY DIGEST
# A summary embed posted once per week (Sunday 18:00–19:00 local time) to the
# configured EPISODES_CHANNEL_ID (or a dedicated digest channel if added to
# config.json as "DIGEST_CHANNEL_ID"). Covers episode + season counts for the
# last 7 days, most active anime, and a comparison to the previous 7 days.
# A small JSON file (weekly_digest.json) tracks the last send date so the task
# never posts more than once per week, even if the bot restarts mid-window.
# ══════════════════════════════════════════════════════════════════════════════

DIGEST_FILE = "weekly_digest.json"

def _load_digest_state() -> dict:
    try:
        with open(DIGEST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_digest_state(data: dict):
    try:
        with open(DIGEST_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        error_logger.error(f"Failed to save digest state: {e}")

def _weekly_stats() -> dict:
    """Compute this week's and last week's episode/season counts from metadata."""
    now = datetime.utcnow()
    week_ago = now - timedelta(days=7)
    two_weeks_ago = now - timedelta(days=14)

    this_week_eps = 0
    last_week_eps = 0
    this_week_seasons = 0
    last_week_seasons = 0
    anime_counts: dict = {}

    for v in posted_metadata.values():
        raw = v.get("posted_at", "")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue
        is_season = v.get("channel") == "Seasons"
        title = v.get("anime_title", "Unknown")

        if week_ago <= dt <= now:
            if is_season:
                this_week_seasons += 1
            else:
                this_week_eps += 1
                anime_counts[title] = anime_counts.get(title, 0) + 1
        elif two_weeks_ago <= dt < week_ago:
            if is_season:
                last_week_seasons += 1
            else:
                last_week_eps += 1

    top_anime = sorted(anime_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "this_week_eps": this_week_eps,
        "last_week_eps": last_week_eps,
        "this_week_seasons": this_week_seasons,
        "last_week_seasons": last_week_seasons,
        "top_anime": top_anime,
        "period_start": week_ago.strftime("%b %d"),
        "period_end": now.strftime("%b %d, %Y"),
    }

@tasks.loop(hours=1)
async def weekly_digest():
    """Fires every hour, but only sends the digest on Sunday between 18:00–19:00
    local server time, and only once per calendar week."""
    now_local = datetime.now()  # local time for the day/hour check
    # Only fire on Sunday (weekday 6) between 18:00 and 19:00
    if now_local.weekday() != 6 or now_local.hour != 18:
        return

    # Guard: don't send more than once per calendar week
    digest_state = _load_digest_state()
    iso_week = now_local.strftime("%Y-W%W")
    if digest_state.get("last_sent_week") == iso_week:
        logger.debug(f"Weekly digest already sent for week {iso_week}, skipping")
        return

    # Check that digest is enabled (opt-in via config)
    if not config.get("WEEKLY_DIGEST_ENABLED", False):
        logger.debug("Weekly digest is disabled (set WEEKLY_DIGEST_ENABLED: true in config.json to enable)")
        return

    # Pick the channel — dedicated digest channel if configured, else episodes channel
    digest_channel_id = int(config.get("DIGEST_CHANNEL_ID", EPISODES_CHANNEL_ID))
    channel = await get_channel_by_id(digest_channel_id)
    if not channel:
        error_logger.error(f"Weekly digest: could not find channel {digest_channel_id}")
        return

    try:
        stats = _weekly_stats()

        # Direction arrows and change text for episode count
        ep_delta = stats["this_week_eps"] - stats["last_week_eps"]
        ep_arrow = "📈" if ep_delta > 0 else ("📉" if ep_delta < 0 else "➡️")
        ep_change = f"{ep_arrow} {'+' if ep_delta >= 0 else ''}{ep_delta} vs last week"

        season_delta = stats["this_week_seasons"] - stats["last_week_seasons"]
        s_arrow = "📈" if season_delta > 0 else ("📉" if season_delta < 0 else "➡️")
        s_change = f"{s_arrow} {'+' if season_delta >= 0 else ''}{season_delta} vs last week"

        embed = discord.Embed(
            title="📅 Weekly Digest",
            description=f"**{stats['period_start']} – {stats['period_end']}**\nHere's what happened on the server this week.",
            color=discord.Color.from_str("#6366f1"),
            timestamp=datetime.utcnow(),
        )
        embed.add_field(
            name="📺 Episodes Announced",
            value=f"**{stats['this_week_eps']}** {ep_change}",
            inline=True,
        )
        embed.add_field(
            name="🌟 New Seasons",
            value=f"**{stats['this_week_seasons']}** {s_change}",
            inline=True,
        )

        if stats["top_anime"]:
            top_lines = "\n".join(
                f"`{i+1}.` {title} — **{count}** ep{'s' if count != 1 else ''}"
                for i, (title, count) in enumerate(stats["top_anime"])
            )
            embed.add_field(name="🏆 Most Active Anime", value=top_lines, inline=False)
        else:
            embed.add_field(name="🏆 Most Active Anime", value="No episodes announced this week.", inline=False)

        embed.add_field(
            name="📊 All-time Totals",
            value=f"**{len(posted_episodes):,}** episodes · **{len(posted_animeseasons):,}** seasons",
            inline=False,
        )
        embed.set_footer(text="Herald Weekly Digest • Disable via WEEKLY_DIGEST_ENABLED: false in config.json")

        await channel.send(embed=embed)
        _save_digest_state({"last_sent_week": iso_week, "last_sent_at": datetime.utcnow().isoformat()})
        logger.info(f"✅ Weekly digest sent to #{channel.name} for week {iso_week}")
        _dash.add_log("success", f"Weekly digest sent — {stats['this_week_eps']} posts, {stats['this_week_seasons']} featured items this week")

    except Exception as e:
        error_logger.error(f"Weekly digest failed: {e}\n{traceback.format_exc()}")

@weekly_digest.before_loop
async def before_weekly_digest():
    await bot.wait_until_ready()

@tasks.loop(seconds=CHECK_INTERVAL)
async def check_new_episodes():
    import time as _time
    _dash.next_scan_at = _time.time() + CHECK_INTERVAL
    _dash.total_scans += 1
    try:
        logger.info("=" * 60)
        logger.info("🔄 Starting periodic episode check...")
        
        episodes_channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
        seasons_channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        
        if not episodes_channel or not seasons_channel:
            logger.error("❌ Could not find required channels")
            return

        episodes = await fetch_new_episodes()
        if not episodes:
            logger.info("✅ No new episodes found. Check complete.")
            return

        await process_episodes(episodes, episodes_channel, seasons_channel)
        logger.info("✅ Episode check complete.")
        logger.info("=" * 60)

    except Exception as e:
        error_logger.error(f"❌ Critical error in check_new_episodes: {e}\n{traceback.format_exc()}")

@check_new_episodes.before_loop
async def before_check():
    await bot.wait_until_ready()
    logger.info("⏳ Bot is ready, waiting to start episode checker...")
    await asyncio.sleep(5)
    logger.info("✅ Episode checker started!")

# ---------------- Restrict messages to allowed server ----------------
@bot.event
async def on_message(message):
    if message.guild and message.guild.id != ALLOWED_GUILD_ID:
        logger.debug(f"Ignored message from unauthorized guild: {message.guild.name} (ID: {message.guild.id})")
        return
    await bot.process_commands(message)

# ---------------- Bot events ----------------
async def sync_posted_animeseasons(limit: Optional[int] = None) -> dict:
    """Fetch all message IDs from the Discord seasons announcement channel
    (SEASONS_CHANNEL_ID), dedupe them, and persist to posted_animeseasons.json.

    This is purely a counting/tracking helper for the dashboard's "Total Seasons
    Announced" stat — it does NOT read or touch MongoDB, does NOT post anything,
    and does NOT alter the existing automatic batching/announcement logic at all.
    It only reads message history from the channel that logic already posts to.

    Returns {"ok": bool, "count": int, "message": str}
    """
    try:
        channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        if not channel:
            return {"ok": False, "count": 0, "message": "Could not find the seasons channel"}

        message_ids = set()
        kwargs = {"limit": limit} if limit else {"limit": None}
        async for message in channel.history(**kwargs):
            # Only count messages actually sent by this bot (real announcements),
            # so manual chatter in the channel doesn't inflate the count.
            if message.author.id == bot.user.id:
                message_ids.add(str(message.id))

        try:
            with open(POSTED_ANIMESEASONS_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(message_ids), f, indent=2)
        except Exception as e:
            error_logger.error(f"Failed to save {POSTED_ANIMESEASONS_FILE}: {e}\n{traceback.format_exc()}")
            return {"ok": False, "count": 0, "message": f"Fetched {len(message_ids)} but failed to save: {e}"}

        global posted_animeseasons
        posted_animeseasons = message_ids
        _dash.posted_animeseasons_ref = posted_animeseasons

        logger.info(f"✅ Synced {len(message_ids)} unique season announcement messages from #{channel.name}")
        return {"ok": True, "count": len(message_ids), "message": f"Synced {len(message_ids)} season announcements from #{channel.name}"}

    except Exception as e:
        error_logger.error(f"sync_posted_animeseasons failed: {e}\n{traceback.format_exc()}")
        return {"ok": False, "count": 0, "message": str(e)}

async def get_guild_info() -> dict:
    """Read-only: returns the connected guild's live metadata, text channels, and roles,
    for the dashboard's Discord Config page (channel/role dropdowns instead of manual
    ID entry). Does NOT modify anything — pure Discord API read via the bot's own
    cached guild object. Safe to call frequently.
    """
    guild = bot.get_guild(ALLOWED_GUILD_ID)
    if not guild:
        return {"ok": False, "message": f"Bot is not in the configured server (ID: {ALLOWED_GUILD_ID})"}

    channels = [
        {"id": str(c.id), "name": c.name, "position": c.position, "category": c.category.name if c.category else None}
        for c in guild.text_channels
    ]
    channels.sort(key=lambda c: c["position"])

    roles = [
        {"id": str(r.id), "name": r.name, "color": str(r.color), "position": r.position, "mentionable": r.mentionable}
        for r in guild.roles
        if r.name != "@everyone"
    ]
    roles.sort(key=lambda r: -r["position"])

    return {
        "ok": True,
        "guild_id": str(guild.id),
        "guild_name": guild.name,
        "guild_icon_url": str(guild.icon.url) if guild.icon else None,
        "member_count": guild.member_count,
        "channels": channels,
        "roles": roles,
        "bot_latency_ms": round(bot.latency * 1000) if bot.latency else None,
    }

async def test_announcement(kind: str, channel_id: int, role_id: int) -> dict:
    """Send a clearly-labeled TEST announcement to a specific channel, optionally
    pinging a specific role — used by the Discord Config page's 'Test announcement' /
    'Test ping' buttons. Always prefixes with '🧪 [TEST]' so it can never be confused
    with a real automatic announcement, and does NOT touch posted_seasons.json,
    posted_animeseasons.json, or any tracking files (it is explicitly NOT a real post).
    """
    try:
        channel = bot.get_guild(ALLOWED_GUILD_ID).get_channel(channel_id) if bot.get_guild(ALLOWED_GUILD_ID) else None
        if not channel:
            return {"ok": False, "message": f"Channel {channel_id} not found in the server"}

        role_mention = ""
        if role_id:
            role = bot.get_guild(ALLOWED_GUILD_ID).get_role(role_id)
            if role:
                role_mention = role.mention + "\n"

        embed = discord.Embed(
            title="🧪 [TEST] " + ("New Season — Test" if kind == "season" else "New Episode — Test"),
            description="This is a test message sent from the Herald dashboard. It does not represent a real announcement.",
            color=discord.Color.purple(),
        )
        embed.set_footer(text="Test announcement triggered from the Herald dashboard")

        message = await channel.send(content=role_mention or None, embed=embed)
        logger.info(f"🧪 Test announcement sent to #{channel.name} (kind={kind}, role_pinged={bool(role_mention)})")
        return {"ok": True, "message": f"Test message sent to #{channel.name}" + (" with role ping" if role_mention else "")}
    except discord.Forbidden:
        return {"ok": False, "message": "Bot lacks permission to send messages in that channel"}
    except Exception as e:
        error_logger.error(f"test_announcement failed: {e}\n{traceback.format_exc()}")
        return {"ok": False, "message": str(e)}

# ══════════════════════════════════════════════════════════════════════════════
# SLASH COMMANDS
# Registered guild-scoped (instant sync, no 1-hour global propagation delay).
# Permission: requires the configured MODERATOR_ROLE_ID or server Administrator.
# Uses discord.app_commands (built into discord.py 2.x) — no extra dependencies.
# ══════════════════════════════════════════════════════════════════════════════

def _is_moderator(interaction: discord.Interaction) -> bool:
    """True if the member has the configured moderator role or is a server admin."""
    member = interaction.user
    if not isinstance(member, discord.Member):
        return False
    if member.guild_permissions.administrator:
        return True
    return any(r.id == MODERATOR_ROLE_ID for r in member.roles)

@bot.tree.command(
    name="stats",
    description="Show Herald statistics — post counts, scan interval, channels, and uptime.",
)
async def slash_stats(interaction: discord.Interaction):
    """Slash-command mirror of !stats — read-only, safe for moderators on mobile."""
    if not _is_moderator(interaction):
        await interaction.response.send_message(
            "❌ You need the Moderator role or Administrator permission to use this command.",
            ephemeral=True,
        )
        return

    ep_count  = len(posted_episodes)
    meta_count = len(posted_metadata)
    season_count = len(posted_seasons)
    animeseasons_count = len(posted_animeseasons)
    uptime_secs = int((datetime.utcnow() - _dash.start_time).total_seconds()) if hasattr(_dash, "start_time") else 0
    h, rem = divmod(uptime_secs, 3600); m, s = divmod(rem, 60)
    uptime_str = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

    embed = discord.Embed(
        title="📊 Herald Statistics",
        color=discord.Color.from_str("#6366f1"),
        timestamp=datetime.utcnow(),
    )
    embed.add_field(name="📺 Episodes Posted", value=f"**{ep_count:,}**", inline=True)
    embed.add_field(name="🗃️ Metadata Entries", value=f"**{meta_count:,}**", inline=True)
    embed.add_field(name="🌟 Seasons Announced", value=f"**{animeseasons_count:,}**", inline=True)
    embed.add_field(name="⏱️ Uptime", value=uptime_str, inline=True)
    embed.add_field(name="🔄 Check Interval", value=f"{CHECK_INTERVAL}s", inline=True)
    embed.add_field(name="📅 Start Date Filter", value=str(START_DATE), inline=True)
    embed.add_field(
        name="📡 Channels",
        value=f"Episodes: <#{EPISODES_CHANNEL_ID}>\nSeasons: <#{SEASONS_CHANNEL_ID}>",
        inline=False,
    )
    embed.add_field(
        name="💾 Files",
        value=f"`posted.json` — {ep_count:,} IDs\n`posted_metadata.json` — {meta_count:,} entries\n`posted_seasons.json` — {season_count:,} entries",
        inline=False,
    )
    embed.set_footer(text="Herald Dashboard • /stats")
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(
    name="pending_seasons",
    description="List anime whose first episode is in MongoDB but hasn't been announced yet.",
)
async def slash_pending_seasons(interaction: discord.Interaction):
    """Slash-command version of the dashboard Pending Seasons panel — safe read-only query."""
    if not _is_moderator(interaction):
        await interaction.response.send_message(
            "❌ You need the Moderator role or Administrator permission to use this command.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)  # DB query may take a moment

    try:
        db = AsyncIOMotorClient(config["MONGO_URI"])[config.get("MONGO_DB", "animedb")]
        animes_col = db[config.get("ANIMES_COLLECTION", "animes")]
        episodes_col = db[config.get("COLLECTION_NAME", "episodes")]

        # Find series whose episode 1 exists in DB
        pipeline = [
            {"$match": {"episodeNumber": 1}},
            {"$group": {"_id": "$seriesId", "anime_title": {"$first": "$animeTitle"}}},
        ]
        season_starts = {str(doc["_id"]): doc.get("anime_title", "Unknown")
                         async for doc in episodes_col.aggregate(pipeline)}

        # Cross-reference against what's already been announced
        pending = [
            (sid, title)
            for sid, title in season_starts.items()
            if sid not in posted_animeseasons and sid not in {str(s.get("series_id","")) for s in posted_seasons}
        ]

        if not pending:
            await interaction.followup.send(
                "✅ All seasons in the database have already been announced!", ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"🌟 Pending Seasons ({len(pending)})",
            description="These anime have Episode 1 in MongoDB but haven't been announced to the seasons channel yet.",
            color=discord.Color.gold(),
            timestamp=datetime.utcnow(),
        )
        # Discord embeds have a 6000 character limit — show up to 20 items safely
        shown = pending[:20]
        lines = "\n".join(f"• **{title}** (ID: `{sid}`)" for sid, title in shown)
        if len(pending) > 20:
            lines += f"\n*…and {len(pending)-20} more. See the dashboard for the full list.*"
        embed.add_field(name="Unannounced seasons", value=lines, inline=False)
        embed.set_footer(text="Use the Herald Dashboard → Pending Items to post any of these.")
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        error_logger.error(f"slash_pending_seasons failed: {e}\n{traceback.format_exc()}")
        await interaction.followup.send(
            f"❌ Could not query the database: `{e}`", ephemeral=True
        )


@bot.event
async def on_ready():
    logger.info("=" * 60)
    logger.info(f"✅ Bot connected as {bot.user} (ID: {bot.user.id})")
    logger.info(f"📊 Connected to {len(bot.guilds)} guild(s)")
    for guild in bot.guilds:
        logger.info(f"   - {guild.name} (ID: {guild.id})")
    logger.info(f"🎯 Allowed Guild ID: {ALLOWED_GUILD_ID}")
    logger.info(f"🎯 Episodes Channel ID: {EPISODES_CHANNEL_ID}")
    logger.info(f"🎯 Seasons Channel ID: {SEASONS_CHANNEL_ID}")
    logger.info(f"👮 Moderator Role ID: {MODERATOR_ROLE_ID}")
    logger.info(f"📢 Episodes Role ID: {EPISODES_ROLE_ID}")
    logger.info(f"📢 Seasons Role ID: {SEASONS_ROLE_ID}")
    logger.info(f"⏱️  Check interval: {CHECK_INTERVAL} seconds")
    logger.info(f"📅 START_DATE filter: {START_DATE}")
    logger.info("=" * 60)
    _dash.discord_ok = True
    _dash.force_scan_fn = check_new_episodes
    _dash.manual_post_season_fn = manual_post_season
    _dash.sync_animeseasons_fn = sync_posted_animeseasons
    _dash.guild_info_fn = get_guild_info
    _dash.test_announcement_fn = test_announcement
    check_new_episodes.start()
    weekly_digest.start()  # Sunday 18:00 digest — opt-in via WEEKLY_DIGEST_ENABLED in config.json
    asyncio.create_task(sync_posted_animeseasons())

    # Sync slash commands to the configured guild (guild-scoped = instant,
    # no 1-hour global Discord propagation delay for app command updates).
    try:
        guild_obj = discord.Object(id=ALLOWED_GUILD_ID)
        bot.tree.copy_global_to(guild=guild_obj)
        synced = await bot.tree.sync(guild=guild_obj)
        logger.info(f"✅ Synced {len(synced)} slash command(s) to guild {ALLOWED_GUILD_ID}: {[c.name for c in synced]}")
    except Exception as e:
        error_logger.error(f"Failed to sync slash commands: {e}\n{traceback.format_exc()}")

@bot.event
async def on_error(event, *args, **kwargs):
    error_logger.error(f"❌ Bot error in event '{event}': {args} {kwargs}\n{traceback.format_exc()}")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ Nemate dozvolu za ovu komandu!")
        logger.warning(f"User {ctx.author} tried to use command without permissions: {ctx.command}")
    elif isinstance(error, commands.CheckFailure):
        await ctx.send("❌ Nemate dozvolu za ovu komandu! Potrebne su administratorske ili moderatorske privilegije.")
        logger.warning(f"User {ctx.author} failed permission check for command: {ctx.command}")
    elif isinstance(error, commands.CommandNotFound):
        logger.debug(f"Command not found: {ctx.message.content}")
    else:
        error_logger.error(f"Command error: {error}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Error: {str(error)}")

# ---------------- Commands ----------------
@bot.command(name="testepisodes")
@is_moderator_or_admin()
async def test_episodes(ctx):
    """Test announcement with the latest episode"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        logger.warning(f"Test episodes command attempted from unauthorized guild: {ctx.guild.name}")
        return
    
    logger.info(f"🧪 Test episodes command invoked by {ctx.author} in #{ctx.channel.name}")
    await ctx.send("🧪 Testing episode announcement...")
    
    try:
        # Get latest episode
        latest_episode = await episodes_collection.find_one(
            {},
            sort=[("_id", -1)]
        )
        
        if not latest_episode:
            await ctx.send("❌ Could not find any episodes in the database.")
            return
        
        # Get episodes channel
        channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
        if not channel:
            await ctx.send("❌ Could not find the episodes channel.")
            return
        
        # Fetch anime data
        series_id = str(latest_episode.get("seriesId"))
        anime_data = await fetch_anime_data(series_id)
        
        if not anime_data:
            await ctx.send("❌ Could not find anime data in the database.")
            return
        
        anime_title = anime_data.get("tmdbTitle") or anime_data.get("malTitle") or "Unknown Anime"
        
        ep_number = latest_episode.get("episodeNumber", "N/A")
        ep_title = latest_episode.get("episodeTitle", "Bez naslova")
        
        # Get images from anime data
        banner_url = get_image_url(anime_data.get("backdrop")) or get_image_url(anime_data.get("poster"))
        thumbnail_url = get_image_url(anime_data.get("poster")) or get_image_url(anime_data.get("thumbnailDub"))
        
        # Get available players
        ep_players = get_available_players(latest_episode)
        players_text = format_players_field(ep_players)
        
        fields = []
        if anime_data.get("genres") and len(anime_data["genres"]) > 0:
            genres_str = ", ".join(anime_data["genres"][:3])
            fields.append({"name": "🎭 Zanrovi", "value": genres_str, "inline": False})
        if anime_data.get("tmdbRating"):
            fields.append({"name": "⭐ TMDB", "value": str(anime_data["tmdbRating"]), "inline": True})
        if anime_data.get("malRating"):
            fields.append({"name": "⭐ MAL", "value": str(anime_data["malRating"]), "inline": True})
        
        # Add players field
        fields.append({"name": "🎬 Dostupno", "value": players_text, "inline": False})
        
        logger.info(f"📺 Test posting episode: {anime_title} - Episode {ep_number}")
        
        await send_embed(
            channel,
            title=f"[TEST] 🎉 New Episode Released!",
            description=f"**{anime_title}**\n\nEpisode {ep_number}: *{ep_title}*",
            banner=banner_url,
            thumbnail=thumbnail_url,
            fields=fields,
            color=discord.Color.green(),
            include_description=False,
            is_season=False
        )
        
        await ctx.send(f"✅ Test episode sent to #{channel.name}!")
        logger.info(f"✅ Test episode announcement sent successfully")
        
    except Exception as e:
        error_logger.error(f"Error in test episodes command: {e}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Test failed: {str(e)}")

@bot.command(name="testseason")
@is_moderator_or_admin()
async def test_season(ctx):
    """Test announcement with the latest season"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        logger.warning(f"Test season command attempted from unauthorized guild: {ctx.guild.name}")
        return
    
    logger.info(f"🧪 Test season command invoked by {ctx.author} in #{ctx.channel.name}")
    await ctx.send("🧪 Testiranje sezone...")
    
    try:
        # Get latest anime (season)
        latest_anime = await animes_collection.find_one(
            {},
            sort=[("_id", -1)]
        )
        
        if not latest_anime:
            await ctx.send("❌ Could not find any anime in the database.")
            return
        
        # Get seasons channel
        channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        if not channel:
            await ctx.send("❌ Could not find the featured/seasons channel.")
            return
        
        anime_title = latest_anime.get("tmdbTitle") or latest_anime.get("malTitle") or "Unknown Anime"
        anime_description = latest_anime.get("description", "")
        
        # Get images
        banner_url = get_image_url(latest_anime.get("backdrop")) or get_image_url(latest_anime.get("poster"))
        thumbnail_url = get_image_url(latest_anime.get("poster")) or get_image_url(latest_anime.get("thumbnailDub"))
        
        # Get season available players
        series_id = str(latest_anime.get("_id"))
        season_players = await get_season_available_players(series_id)
        players_text = format_players_field(season_players)
        
        # Get episode count
        episode_count = latest_anime.get("episodes", "N/A")
        
        fields = []
        if latest_anime.get("tmdbRating"):
            fields.append({"name": "⭐ TMDB ocena", "value": str(latest_anime["tmdbRating"]), "inline": True})
        if latest_anime.get("malRating"):
            fields.append({"name": "⭐ MAL ocena", "value": str(latest_anime["malRating"]), "inline": True})
        if latest_anime.get("studio"):
            fields.append({"name": "🎨 Studio", "value": latest_anime["studio"], "inline": True})
        if latest_anime.get("ttype"):
            fields.append({"name": "📺 Tip", "value": latest_anime["ttype"], "inline": True})
        
        # Add players field
        fields.append({"name": "🎬 Dostupno", "value": players_text, "inline": False})
        
        logger.info(f"📺 Test posting season: {anime_title}")
        
        await send_embed(
            channel,
            title=f"[TEST] 🌟 New Season Announced!",
            description=f"**{anime_title}**\n\n{episode_count} episode{'s' if episode_count != 1 else ''} are now available!",
            banner=banner_url,
            thumbnail=thumbnail_url,
            fields=fields,
            color=discord.Color.gold(),
            anime_description=anime_description,
            include_description=True,
            is_season=True
        )
        
        await ctx.send(f"✅ Test sezone poslan u #{channel.name}!")
        logger.info(f"✅ Test season announcement sent successfully")
        
    except Exception as e:
        error_logger.error(f"Error in test season command: {e}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Test failed: {str(e)}")

@bot.command(name="force_check")
@is_moderator_or_admin()
async def force_check(ctx):
    """Force check for new episodes"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        logger.warning(f"Force check attempted from unauthorized guild: {ctx.guild.name}")
        return
    
    logger.info(f"🔄 Force check command invoked by {ctx.author}")
    await ctx.send("🔄 Force-checking for new content...")
    await check_new_episodes()
    await ctx.send("✅ Check complete.")

@bot.command(name="reset_posted")
@is_moderator_or_admin()
async def reset_posted(ctx):
    """Reset posted episodes list"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🗑️ Reset posted command invoked by {ctx.author}")
    
    old_count = len(posted_episodes)
    posted_episodes.clear()
    posted_metadata.clear()
    save_posted_episodes()
    save_posted_metadata()
    
    await ctx.send(f"✅ Cleared {old_count} posted episodes. The bot will now re-announce all episodes after {START_DATE}.")
    logger.info(f"Posted episodes cleared: {old_count} episodes removed")

@bot.command(name="stats")
@is_moderator_or_admin()
async def stats(ctx):
    """Show bot statistics"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"📊 Stats command invoked by {ctx.author}")
    
    embed = discord.Embed(title="📊 Bot Statistics", color=discord.Color.blue())
    embed.add_field(name="Posts", value=str(len(posted_episodes)), inline=True)
    embed.add_field(name="Cached series", value=str(len(anime_cache)), inline=True)
    embed.add_field(name="Interval provjere", value=f"{CHECK_INTERVAL}s", inline=True)
    embed.add_field(name="Batch threshold", value=str(BATCH_THRESHOLD), inline=True)
    embed.add_field(name="Mini batch threshold", value=str(MINI_BATCH_THRESHOLD), inline=True)
    embed.add_field(name="Batch hours", value=f"{BATCH_HOURS}h", inline=True)
    embed.add_field(name="START_DATE", value=START_DATE, inline=False)
    embed.add_field(name="Channels", value=f"Posts: <#{EPISODES_CHANNEL_ID}>\nFeatured: <#{SEASONS_CHANNEL_ID}>", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="view_posted")
@is_moderator_or_admin()
async def view_posted(ctx, limit: int = 10):
    """View last posted episodes with metadata"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"📋 View posted command invoked by {ctx.author}")
    
    # Debug info
    await ctx.send(f"📊 Debug info:\n- `posted_episodes` set: {len(posted_episodes)} items\n- `posted_metadata` dict: {len(posted_metadata)} items")
    
    if not posted_metadata:
        await ctx.send("❌ Nema metadata! Koristite `!sync_metadata` da popunite metadata iz Discord poruka.")
        return
    
    # Get last N entries
    sorted_metadata = sorted(
        posted_metadata.items(),
        key=lambda x: x[1].get("posted_at", ""),
        reverse=True
    )[:limit]
    
    embed = discord.Embed(title=f"📋 Last {limit} Posted Items", color=discord.Color.green())
    
    for episode_id, data in sorted_metadata:
        anime_title = data.get("anime_title", "N/A")
        ep_num = data.get("episode_number", "N/A")
        ep_title = data.get("episode_title", "N/A")
        channel = data.get("channel", "N/A")
        posted_at = data.get("posted_at", "N/A")
        
        # Format datetime
        try:
            dt = datetime.fromisoformat(posted_at)
            posted_at_formatted = dt.strftime("%Y-%m-%d %H:%M")
        except:
            posted_at_formatted = posted_at
        
        field_value = f"Epizoda {ep_num}: {ep_title}\nKanal: {channel}\nPostavljeno: {posted_at_formatted}"
        embed.add_field(name=anime_title, value=field_value, inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="check_files")
@is_moderator_or_admin()
async def check_files(ctx):
    """Check the status of posted.json and posted_metadata.json files"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"📁 Check files command invoked by {ctx.author}")
    
    embed = discord.Embed(title="📁 Status fajlova", color=discord.Color.blue())
    
    # Check posted.json
    if os.path.exists(POSTED_FILE):
        size = os.path.getsize(POSTED_FILE)
        embed.add_field(
            name="posted.json",
            value=f"✅ Found\n📊 {len(posted_episodes)} entries\n💾 {size} bytes",
            inline=True
        )
    else:
        embed.add_field(name="posted.json", value="❌ Ne postoji", inline=True)
    
    # Check posted_metadata.json
    if os.path.exists(POSTED_METADATA_FILE):
        size = os.path.getsize(POSTED_METADATA_FILE)
        embed.add_field(
            name="posted_metadata.json",
            value=f"✅ Found\n📊 {len(posted_metadata)} entries\n💾 {size} bytes",
            inline=True
        )
    else:
        embed.add_field(name="posted_metadata.json", value="❌ Ne postoji", inline=True)
    
    # Show mismatch warning
    if len(posted_episodes) != len(posted_metadata):
        embed.add_field(
            name="⚠️ Upozorenje",
            value=f"Mismatch: {len(posted_entrsodes)} in posted vs {len(posted_metadata)} in metadata!\nPreporucujem da pokrenete `!rebuild_metadata`",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="fix_json")
@is_moderator_or_admin()
async def fix_json(ctx):
    """Fix corrupted JSON files"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🔧 Fix JSON command invoked by {ctx.author}")
    
    # Check posted.json
    posted_status = "✅ OK"
    try:
        if os.path.exists(POSTED_FILE):
            with open(POSTED_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                posted_status = f"✅ OK ({len(data)} entries)"
        else:
            posted_status = "❌ Ne postoji"
    except json.JSONDecodeError as e:
        posted_status = f"❌ Corrupted: {str(e)}"
    except Exception as e:
        posted_status = f"❌ Error: {str(e)}"
    
    # Check posted_metadata.json
    metadata_status = "✅ OK"
    try:
        if os.path.exists(POSTED_METADATA_FILE):
            with open(POSTED_METADATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                metadata_status = f"✅ OK ({len(data)} entries)"
        else:
            metadata_status = "❌ Ne postoji"
    except json.JSONDecodeError as e:
        metadata_status = f"❌ Corrupted: {str(e)}"
        
        # Try to fix it
        await ctx.send(f"⚠️ posted_metadata.json appears corrupted — attempting repair...")
        try:
            # Reset to empty dict
            with open(POSTED_METADATA_FILE, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=4, ensure_ascii=False)
            metadata_status = "✅ Popravljeno - prazan fajl kreiran"
            # Reload
            global posted_metadata
            posted_metadata = {}
            await ctx.send("✅ posted_metadata.json resetovan na prazan fajl. Pokrenite `!build_metadata` da ga popunite.")
        except Exception as fix_error:
            await ctx.send(f"❌ Ne mogu popraviti: {str(fix_error)}")
    except Exception as e:
        metadata_status = f"❌ Error: {str(e)}"
    
    embed = discord.Embed(title="🔧 Status JSON fajlova", color=discord.Color.blue())
    embed.add_field(name="posted.json", value=posted_status, inline=False)
    embed.add_field(name="posted_metadata.json", value=metadata_status, inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="build_metadata")
@is_moderator_or_admin()
async def build_metadata(ctx, message_limit: int = 500):
    """Build metadata for existing posted episodes by matching with Discord messages and MongoDB
    
    This keeps posted.json intact and just fills posted_metadata.json
    """
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🔨 Build metadata command invoked by {ctx.author}")
    
    if not posted_episodes:
        await ctx.send("❌ posted.json is empty — nothing to synchronise.")
        return
    
    status_msg = await ctx.send(f"🔨 Building metadata for {len(posted_episodes)} entries from posted.json...")
    
    try:
        from bson import ObjectId
        import re
        
        # Step 1: Get all episodes from MongoDB that are in posted_episodes
        logger.info(f"📊 Step 1: Fetching {len(posted_episodes)} episodes from MongoDB...")
        
        episode_ids_objects = [ObjectId(ep_id) for ep_id in posted_episodes if ObjectId.is_valid(ep_id)]
        
        cursor = episodes_collection.find({"_id": {"$in": episode_ids_objects}})
        db_episodes = await cursor.to_list(length=None)
        
        # Create a lookup dict: episode_id -> episode_data
        db_episodes_dict = {str(ep["_id"]): ep for ep in db_episodes}
        
        logger.info(f"✅ Found {len(db_episodes_dict)} episodes in MongoDB")
        await status_msg.edit(content=f"✅ Step 1/3: Found {len(db_entrsodes_dict)}/{len(posted_entrsodes)} entrzoda u MongoDB\n🔄 Step 2/3: Skeniram Discord messages...")
        
        # Step 2: Get anime data for all series
        series_ids = list(set([ep.get("seriesId") for ep in db_episodes]))
        cursor = animes_collection.find({"_id": {"$in": series_ids}})
        animes = await cursor.to_list(length=None)
        animes_dict = {str(anime["_id"]): anime for anime in animes}
        
        logger.info(f"✅ Found {len(animes_dict)} anime series")
        
        # Step 3: Scan Discord messages to find posted_at timestamps
        episodes_channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
        seasons_channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        
        if not episodes_channel or not seasons_channel:
            await ctx.send("❌ Could not find the required channels.")
            return
        
        # Create a mapping: (series_id, episode_number) -> posted_at timestamp
        discord_timestamps = {}
        
        channels_to_scan = [
            (episodes_channel, "Episodes"),
            (seasons_channel, "Seasons")
        ]
        
        message_count = 0
        for channel, channel_name in channels_to_scan:
            async for message in channel.history(limit=message_limit):
                if message.author.id != bot.user.id:
                    continue
                
                if not message.embeds:
                    continue
                
                message_count += 1
                embed = message.embeds[0]
                description = embed.description or ""
                
                # Extract anime title
                anime_title = None
                if "**" in description:
                    parts = description.split("**")
                    if len(parts) >= 2:
                        anime_title = parts[1].strip()
                
                if not anime_title:
                    continue
                
                # Find anime in our animes_dict
                series_id = None
                for sid, anime_data in animes_dict.items():
                    if anime_data.get("tmdbTitle") == anime_title or anime_data.get("malTitle") == anime_title:
                        series_id = sid
                        break
                
                if not series_id:
                    continue
                
                # Extract episode numbers
                batch_match = re.search(r'Epizode (\d+)-(\d+)', description)
                single_match = re.search(r'Epizoda (\d+)', description)
                
                if batch_match:
                    start_ep = int(batch_match.group(1))
                    end_ep = int(batch_match.group(2))
                    for ep_num in range(start_ep, end_ep + 1):
                        key = (series_id, ep_num)
                        if key not in discord_timestamps:
                            discord_timestamps[key] = (message.created_at, channel_name)
                elif single_match:
                    ep_num = int(single_match.group(1))
                    key = (series_id, ep_num)
                    if key not in discord_timestamps:
                        discord_timestamps[key] = (message.created_at, channel_name)
        
        logger.info(f"✅ Scanned {message_count} Discord messages, found timestamps for {len(discord_timestamps)} episodes")
        await status_msg.edit(content=f"✅ Step 2/3: Scanned {message_count} messages, found {len(discord_timestamps)} timestampova\n🔄 Step 3/3: Generisem metadata...")
        
        # Step 4: Build metadata for all episodes in posted_episodes
        success_count = 0
        missing_db_count = 0
        missing_discord_count = 0
        
        for episode_id in posted_episodes:
            # Get episode from MongoDB
            episode_data = db_episodes_dict.get(episode_id)
            
            if not episode_data:
                logger.warning(f"Episode {episode_id} not found in MongoDB")
                missing_db_count += 1
                continue
            
            series_id = str(episode_data.get("seriesId"))
            ep_number = episode_data.get("episodeNumber")
            
            # Get anime data
            anime_data = animes_dict.get(series_id)
            if not anime_data:
                logger.warning(f"Anime {series_id} not found")
                missing_db_count += 1
                continue
            
            anime_title = anime_data.get("tmdbTitle") or anime_data.get("malTitle") or "Unknown"
            
            # Get Discord timestamp
            key = (series_id, ep_number)
            discord_info = discord_timestamps.get(key)
            
            if discord_info:
                posted_at_dt, channel_name = discord_info
                # Convert datetime to ISO string immediately
                posted_at_str = posted_at_dt.isoformat() if isinstance(posted_at_dt, datetime) else str(posted_at_dt)
            else:
                # No Discord message found - use current time as fallback
                posted_at_str = datetime.utcnow().isoformat()
                channel_name = "Unknown"
                missing_discord_count += 1
                logger.warning(f"No Discord message found for {anime_title} Ep {ep_number}")
            
            # Add to metadata (without calling add_to_posted since it's already in posted_episodes)
            posted_metadata[episode_id] = {
                "episode_id": episode_id,
                "series_id": series_id,
                "anime_title": anime_title,
                "episode_number": ep_number,
                "episode_title": episode_data.get("episodeTitle", "N/A"),
                "posted_at": posted_at_str,  # Already converted to string above
                "channel": channel_name,
                "episode_created_at": episode_data.get("createdAt", "N/A"),
                "episode_updated_at": episode_data.get("updatedAt", "N/A")
            }
            success_count += 1
        
        # Save metadata
        save_posted_metadata()
        
        # Create result embed
        result_embed = discord.Embed(title="✅ Metadata generisana", color=discord.Color.green())
        result_embed.add_field(name="✅ Succeeded", value=str(success_count), inline=True)
        result_embed.add_field(name="❌ Nedostaje u MongoDB", value=str(missing_db_count), inline=True)
        result_embed.add_field(name="⚠️ Nedostaje Discord poruka", value=str(missing_discord_count), inline=True)
        result_embed.add_field(name="📊 Ukupno metadata", value=str(len(posted_metadata)), inline=True)
        result_embed.set_footer(text=f"posted.json ostao nepromenjen ({len(posted_episodes)} epizoda)")
        
        await status_msg.edit(content=None, embed=result_embed)
        logger.info(f"✅ Metadata build complete: {success_count} success, {missing_db_count} missing from DB, {missing_discord_count} missing Discord msg")
        
    except Exception as e:
        error_logger.error(f"Error in build_metadata: {e}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Error: {str(e)}")
@is_moderator_or_admin()
async def rebuild_metadata(ctx, message_limit: int = 500):
    """Rebuild metadata from Discord messages (ignores current posted_episodes)
    
    This will scan Discord messages and rebuild the entire posted_episodes and metadata from scratch.
    """
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🔨 Rebuild metadata command invoked by {ctx.author}")
    
    # Ask for confirmation
    confirm_msg = await ctx.send(
        f"⚠️ **UPOZORENJE**: Ovo ce obrisati trenutne podatke i rebuild-ovati ih iz Discord messages.\n\n"
        f"Trenutno stanje:\n"
        f"- `posted_episodes`: {len(posted_episodes)} epizoda\n"
        f"- `posted_metadata`: {len(posted_metadata)} epizoda\n\n"
        f"Reagujte sa ✅ da potvrdite ili ❌ da otkazete."
    )
    
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")
    
    def check(reaction, user):
        return user == ctx.author and str(reaction.emoji) in ["✅", "❌"] and reaction.message.id == confirm_msg.id
    
    try:
        reaction, user = await bot.wait_for('reaction_add', timeout=30.0, check=check)
        
        if str(reaction.emoji) == "❌":
            await ctx.send("❌ Otkazano.")
            return
        
    except asyncio.TimeoutError:
        await ctx.send("⏱️ Timeout - otkazano.")
        return
    
    # Clear everything
    posted_episodes.clear()
    posted_metadata.clear()
    
    status_msg = await ctx.send(f"🔨 Rebuild-ujem metadata iz {message_limit} poruka po kanalu...")
    
    try:
        from bson import ObjectId
        import re
        
        episodes_channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
        seasons_channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        
        if not episodes_channel or not seasons_channel:
            await ctx.send("❌ Could not find the required channels.")
            return
        
        synced_count = 0
        failed_count = 0
        skipped_count = 0
        
        # Process both channels
        channels_to_scan = [
            (episodes_channel, "Episodes"),
            (seasons_channel, "Seasons")
        ]
        
        for channel, channel_name in channels_to_scan:
            logger.info(f"📡 Scanning #{channel.name} for announcements...")
            
            # Fetch message history
            async for message in channel.history(limit=message_limit):
                # Skip if not from bot
                if message.author.id != bot.user.id:
                    continue
                
                # Skip if no embeds
                if not message.embeds:
                    continue
                
                embed = message.embeds[0]
                description = embed.description or ""
                
                # Extract anime title (between ** **)
                anime_title = None
                if "**" in description:
                    parts = description.split("**")
                    if len(parts) >= 2:
                        anime_title = parts[1].strip()
                
                if not anime_title:
                    logger.debug(f"Skipping message - no anime title found")
                    skipped_count += 1
                    continue
                
                # Try to find the anime in database
                anime_data = await animes_collection.find_one({
                    "$or": [
                        {"tmdbTitle": anime_title},
                        {"malTitle": anime_title}
                    ]
                })
                
                if not anime_data:
                    logger.warning(f"⚠️ Anime not found in DB: {anime_title}")
                    failed_count += 1
                    continue
                
                series_id = str(anime_data["_id"])
                
                # Check for batch or single
                batch_match = re.search(r'Epizode (\d+)-(\d+)', description)
                
                if batch_match:
                    # Batch announcement
                    start_ep = int(batch_match.group(1))
                    end_ep = int(batch_match.group(2))
                    
                    # Fetch all episodes in this range
                    cursor = episodes_collection.find({
                        "seriesId": ObjectId(series_id),
                        "episodeNumber": {"$gte": start_ep, "$lte": end_ep}
                    })
                    episodes_in_range = await cursor.to_list(length=None)
                    
                    for ep in episodes_in_range:
                        episode_id = str(ep["_id"])
                        # Now we always add (no duplicate check since we cleared everything)
                        add_to_posted(episode_id, ep, anime_title, channel_name, message.created_at)
                        synced_count += 1
                        logger.info(f"✅ Rebuilt batch episode: {anime_title} - Ep {ep.get('episodeNumber')}")
                else:
                    # Single episode announcement
                    single_match = re.search(r'Epizoda (\d+)', description)
                    if single_match:
                        ep_number = int(single_match.group(1))
                        
                        # Find this specific episode in database
                        episode = await episodes_collection.find_one({
                            "seriesId": ObjectId(series_id),
                            "episodeNumber": ep_number
                        })
                        
                        if episode:
                            episode_id = str(episode["_id"])
                            add_to_posted(episode_id, episode, anime_title, channel_name, message.created_at)
                            synced_count += 1
                            logger.info(f"✅ Rebuilt single episode: {anime_title} - Ep {ep_number}")
                        else:
                            logger.warning(f"⚠️ Episode not found in DB: {anime_title} Ep {ep_number}")
                            failed_count += 1
                    else:
                        logger.debug(f"Could not parse episode info")
                        skipped_count += 1
        
        # Create result embed
        result_embed = discord.Embed(title="✅ Rebuild zavrsen", color=discord.Color.green())
        result_embed.add_field(name="✅ Succeeded rebuild-ovano", value=str(synced_count), inline=True)
        result_embed.add_field(name="❌ Neuspesno", value=str(failed_count), inline=True)
        result_embed.add_field(name="⏭️ Preskoceno", value=str(skipped_count), inline=True)
        result_embed.add_field(name="📊 Ukupno u listi", value=str(len(posted_episodes)), inline=True)
        result_embed.set_footer(text=f"Skenirano {message_limit} poruka po kanalu")
        
        await status_msg.edit(content=None, embed=result_embed)
        logger.info(f"✅ Metadata rebuild complete: {synced_count} synced, {failed_count} failed, {skipped_count} skipped")
        
    except Exception as e:
        error_logger.error(f"Error in rebuild_metadata: {e}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Error: {str(e)}")

@bot.command(name="clear_cache")
@is_moderator_or_admin()
async def clear_cache(ctx):
    """Clear anime cache"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🗑️ Clear cache command invoked by {ctx.author}")
    anime_cache.clear()
    await ctx.send("✅ Kes obrisan!")
    logger.info("Anime cache cleared")

@bot.command(name="logs")
@is_moderator_or_admin()
async def show_logs(ctx, lines: int = 20):
    """Show last N lines of logs"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    try:
        with open('bot_events.log', 'r', encoding='utf-8') as f:
            log_lines = f.readlines()
            last_lines = log_lines[-lines:]
            log_text = ''.join(last_lines)
            
            if len(log_text) > 1900:
                log_text = log_text[-1900:]
            
            await ctx.send(f"```\n{log_text}\n```")
    except Exception as e:
        await ctx.send(f"❌ Ne mogu ucitati logove: {str(e)}")
        
@bot.command(name="test_sync")
@is_moderator_or_admin()
async def test_sync(ctx):
    """Test sync on a single message to debug"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🧪 Test sync command invoked by {ctx.author}")
    await ctx.send("🧪 Testiram sync na jednoj poruci...")
    
    try:
        from bson import ObjectId
        import re
        
        episodes_channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
        if not episodes_channel:
            await ctx.send("❌ Could not find the episodes channel.")
            return
        
        # Get the most recent bot message
        async for message in episodes_channel.history(limit=10):
            if message.author.id != bot.user.id:
                continue
            
            if not message.embeds:
                continue
            
            embed = message.embeds[0]
            description = embed.description or ""
            
            await ctx.send(f"📋 Testiram poruku: {message.jump_url}")
            await ctx.send(f"```\nTitle: {embed.title}\nDescription:\n{description}\n```")
            
            # Extract anime title
            anime_title = None
            if "**" in description:
                parts = description.split("**")
                if len(parts) >= 2:
                    anime_title = parts[1].strip()
            
            await ctx.send(f"🎬 Ekstraktovani anime: `{anime_title}`")
            
            if not anime_title:
                await ctx.send("❌ Nisam mogao ekstraktovati anime naslov!")
                return
            
            # Search in database
            anime_data = await animes_collection.find_one({
                "$or": [
                    {"tmdbTitle": anime_title},
                    {"malTitle": anime_title}
                ]
            })
            
            if anime_data:
                await ctx.send(f"✅ Pronasao anime u bazi: `{anime_data.get('tmdbTitle')}` (ID: {anime_data['_id']})")
            else:
                await ctx.send(f"❌ Anime nije found u bazi!")
                
                # Try fuzzy search
                await ctx.send("🔍 Pokusavam pronaci slicne naslove...")
                cursor = animes_collection.find({}).limit(5)
                similar = await cursor.to_list(length=5)
                for anime in similar:
                    title = anime.get('tmdbTitle') or anime.get('malTitle')
                    await ctx.send(f"   - `{title}`")
                return
            
            # Check for batch or single
            batch_match = re.search(r'Epizode (\d+)-(\d+)', description)
            single_match = re.search(r'Epizoda (\d+)', description)
            
            if batch_match:
                await ctx.send(f"📦 Batch: Epizode {batch_match.group(1)}-{batch_match.group(2)}")
            elif single_match:
                await ctx.send(f"📄 Single: Epizoda {single_match.group(1)}")
            else:
                await ctx.send("❌ Nisam mogao ekstraktovati broj epizode!")
            
            return
        
        await ctx.send("❌ Nisam pronasao nijednu messages!")
        
    except Exception as e:
        error_logger.error(f"Error in test_sync: {e}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Error: {str(e)}")
@is_moderator_or_admin()
async def debug_messages(ctx, limit: int = 5):
    """Debug: Show what the bot sees in recent messages"""
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🐛 Debug messages command invoked by {ctx.author}")
    
    episodes_channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
    if not episodes_channel:
        await ctx.send("❌ Could not find the episodes channel.")
        return
    
    await ctx.send(f"🐛 Analiziram poslednjih {limit} poruka iz #{episodes_channel.name}...")
    
    count = 0
    async for message in episodes_channel.history(limit=limit):
        if message.author.id != bot.user.id:
            continue
        
        if not message.embeds:
            await ctx.send(f"⚠️ Poruka bez embeda: {message.jump_url}")
            continue
        
        embed = message.embeds[0]
        
        debug_info = f"""
📋 **Poruka {count + 1}**
🔗 Link: {message.jump_url}
📅 Poslato: {message.created_at}
📝 Title: `{embed.title or 'N/A'}`
📝 Description:
```
{embed.description or 'N/A'}
```
"""
        await ctx.send(debug_info)
        count += 1
        
        if count >= limit:
            break
    
    if count == 0:
        await ctx.send("❌ Nisam pronasao nijednu svoju messages sa embedom!")

@bot.command(name="sync_metadata")
@is_moderator_or_admin()
async def sync_metadata(ctx, message_limit: int = 100):
    """Sync episodes from Discord channel messages with MongoDB database
    
    Args:
        message_limit: Number of messages to scan from each channel (default: 100)
    """
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🔄 Sync metadata command invoked by {ctx.author}")
    status_msg = await ctx.send(f"🔄 Sinhronizujem metadata iz Discord kanala (poslednji {message_limit} poruka po kanalu)...")
    
    try:
        from bson import ObjectId
        import re
        
        episodes_channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
        seasons_channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        
        if not episodes_channel or not seasons_channel:
            await ctx.send("❌ Could not find the required channels.")
            return
        
        synced_count = 0
        failed_count = 0
        skipped_count = 0
        
        # Process both channels
        channels_to_scan = [
            (episodes_channel, "Episodes"),
            (seasons_channel, "Seasons")
        ]
        
        await status_msg.edit(content=f"🔄 Skeniram kanale za poruke...")
        
        for channel, channel_name in channels_to_scan:
            logger.info(f"📡 Scanning #{channel.name} for announcements...")
            
            # Fetch message history
            async for message in channel.history(limit=message_limit):
                # Skip if not from bot
                if message.author.id != bot.user.id:
                    continue
                
                # Skip if no embeds
                if not message.embeds:
                    continue
                
                embed = message.embeds[0]
                
                # Log what we're seeing
                logger.debug(f"Processing message: Title='{embed.title}', Description preview='{embed.description[:100] if embed.description else 'None'}'")
                
                # Try to extract anime title and episode info from embed
                description = embed.description or ""
                
                # Extract anime title (between ** **)
                anime_title = None
                if "**" in description:
                    parts = description.split("**")
                    if len(parts) >= 2:
                        anime_title = parts[1].strip()
                
                if not anime_title:
                    logger.debug(f"Skipping message - no anime title found in description: {description[:100]}")
                    skipped_count += 1
                    continue
                
                logger.info(f"Found anime title: {anime_title}")
                
                # Try to find the anime in database
                anime_data = await animes_collection.find_one({
                    "$or": [
                        {"tmdbTitle": anime_title},
                        {"malTitle": anime_title}
                    ]
                })
                
                if not anime_data:
                    logger.warning(f"⚠️ Anime not found in DB: {anime_title}")
                    failed_count += 1
                    continue
                
                series_id = str(anime_data["_id"])
                logger.info(f"Found anime in DB with series_id: {series_id}")
                
                # Check if this is a batch announcement (multiple episodes)
                # Match patterns like "Epizode 9-12" (with possible newlines and extra text)
                batch_match = re.search(r'Epizode (\d+)-(\d+)', description)
                
                if batch_match:
                    # Batch announcement
                    start_ep = int(batch_match.group(1))
                    end_ep = int(batch_match.group(2))
                    
                    logger.info(f"Batch detected: Episodes {start_ep}-{end_ep}")
                    
                    # Fetch all episodes in this range for this series
                    cursor = episodes_collection.find({
                        "seriesId": ObjectId(series_id),
                        "episodeNumber": {"$gte": start_ep, "$lte": end_ep}
                    })
                    episodes_in_range = await cursor.to_list(length=None)
                    
                    logger.info(f"Found {len(episodes_in_range)} episodes in DB for range {start_ep}-{end_ep}")
                    
                    for ep in episodes_in_range:
                        episode_id = str(ep["_id"])
                        
                        # Add to posted set and metadata with Discord message timestamp
                        if episode_id not in posted_episodes:
                            add_to_posted(episode_id, ep, anime_title, channel_name, message.created_at)
                            synced_count += 1
                            logger.info(f"✅ Synced batch episode: {anime_title} - Ep {ep.get('episodeNumber')} (posted: {message.created_at})")
                        else:
                            logger.debug(f"Episode {episode_id} already in posted list")
                else:
                    # Single episode announcement
                    # Match patterns like "Epizoda 12:" (with possible newlines and extra text)
                    single_match = re.search(r'Epizoda (\d+)', description)
                    if single_match:
                        ep_number = int(single_match.group(1))
                        
                        logger.info(f"Single episode detected: Episode {ep_number}")
                        
                        # Find this specific episode in database
                        episode = await episodes_collection.find_one({
                            "seriesId": ObjectId(series_id),
                            "episodeNumber": ep_number
                        })
                        
                        if episode:
                            episode_id = str(episode["_id"])
                            
                            # Add to posted set and metadata with Discord message timestamp
                            if episode_id not in posted_episodes:
                                add_to_posted(episode_id, episode, anime_title, channel_name, message.created_at)
                                synced_count += 1
                                logger.info(f"✅ Synced single episode: {anime_title} - Ep {ep_number} (posted: {message.created_at})")
                            else:
                                logger.debug(f"Episode {episode_id} already in posted list")
                        else:
                            logger.warning(f"⚠️ Episode not found in DB: {anime_title} Ep {ep_number}")
                            failed_count += 1
                    else:
                        logger.warning(f"Could not parse episode info from: {description}")
                        skipped_count += 1
        
        # Create result embed
        result_embed = discord.Embed(title="✅ Sinhronizacija zavrsena", color=discord.Color.green())
        result_embed.add_field(name="✅ Succeeded sinhronizovano", value=str(synced_count), inline=True)
        result_embed.add_field(name="❌ Neuspesno", value=str(failed_count), inline=True)
        result_embed.add_field(name="⏭️ Preskoceno", value=str(skipped_count), inline=True)
        result_embed.add_field(name="📊 Ukupno u listi", value=str(len(posted_episodes)), inline=True)
        result_embed.set_footer(text=f"Skenirano {message_limit} poruka po kanalu")
        
        await status_msg.edit(content=None, embed=result_embed)
        logger.info(f"✅ Metadata sync complete: {synced_count} synced, {failed_count} failed, {skipped_count} skipped")
        
    except Exception as e:
        error_logger.error(f"Error in sync_metadata command: {e}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Error")
@is_moderator_or_admin()
async def sync_metadata(ctx, message_limit: int = 100):
    """Sync episodes from Discord channel messages with MongoDB database
    
    Args:
        message_limit: Number of messages to scan from each channel (default: 100)
    """
    if ctx.guild.id != ALLOWED_GUILD_ID:
        return
    
    logger.info(f"🔄 Sync metadata command invoked by {ctx.author}")
    status_msg = await ctx.send(f"🔄 Sinhronizujem metadata iz Discord kanala (poslednji {message_limit} poruka po kanalu)...")
    
    try:
        from bson import ObjectId
        
        episodes_channel = await get_channel_by_id(EPISODES_CHANNEL_ID)
        seasons_channel = await get_channel_by_id(SEASONS_CHANNEL_ID)
        
        if not episodes_channel or not seasons_channel:
            await ctx.send("❌ Could not find the required channels.")
            return
        
        synced_count = 0
        failed_count = 0
        skipped_count = 0
        
        # Process both channels
        channels_to_scan = [
            (episodes_channel, "Episodes"),
            (seasons_channel, "Seasons")
        ]
        
        await status_msg.edit(content=f"🔄 Skeniram kanale za poruke...")
        
        for channel, channel_name in channels_to_scan:
            logger.info(f"📡 Scanning #{channel.name} for announcements...")
            
            # Fetch message history
            async for message in channel.history(limit=message_limit):
                # Skip if not from bot
                if message.author.id != bot.user.id:
                    continue
                
                # Skip if no embeds
                if not message.embeds:
                    continue
                
                embed = message.embeds[0]
                
                # Try to extract anime title and episode info from embed
                # Look for patterns like "Episode X" or "Epizode X-Y"
                description = embed.description or ""
                
                # Extract anime title (between ** **)
                anime_title = None
                if "**" in description:
                    parts = description.split("**")
                    if len(parts) >= 2:
                        anime_title = parts[1].strip()
                
                if not anime_title:
                    logger.debug(f"Skipping message - no anime title found")
                    skipped_count += 1
                    continue
                
                # Try to find the anime in database
                anime_data = await animes_collection.find_one({
                    "$or": [
                        {"tmdbTitle": anime_title},
                        {"malTitle": anime_title}
                    ]
                })
                
                if not anime_data:
                    logger.warning(f"⚠️ Anime not found in DB: {anime_title}")
                    failed_count += 1
                    continue
                
                series_id = str(anime_data["_id"])
                
                # Check if this is a batch announcement (multiple episodes)
                is_batch = "Episodes" in description and "-" in description
                
                if is_batch:
                    # Extract episode range (e.g., "Epizode 1-5")
                    import re
                    match = re.search(r'Epizode (\d+)-(\d+)', description)
                    if match:
                        start_ep = int(match.group(1))
                        end_ep = int(match.group(2))
                        
                        # Fetch all episodes in this range for this series
                        cursor = episodes_collection.find({
                            "seriesId": ObjectId(series_id),
                            "episodeNumber": {"$gte": start_ep, "$lte": end_ep}
                        })
                        episodes_in_range = await cursor.to_list(length=None)
                        
                        for ep in episodes_in_range:
                            episode_id = str(ep["_id"])
                            
                            # Add to posted set and metadata with Discord message timestamp
                            if episode_id not in posted_episodes:
                                add_to_posted(episode_id, ep, anime_title, channel_name, message.created_at)
                                synced_count += 1
                                logger.info(f"✅ Synced batch episode: {anime_title} - Ep {ep.get('episodeNumber')} (posted: {message.created_at})")
                else:
                    # Single episode announcement
                    import re
                    match = re.search(r'Epizoda (\d+)', description)
                    if match:
                        ep_number = int(match.group(1))
                        
                        # Find this specific episode in database
                        episode = await episodes_collection.find_one({
                            "seriesId": ObjectId(series_id),
                            "episodeNumber": ep_number
                        })
                        
                        if episode:
                            episode_id = str(episode["_id"])
                            
                            # Add to posted set and metadata with Discord message timestamp
                            if episode_id not in posted_episodes:
                                add_to_posted(episode_id, episode, anime_title, channel_name, message.created_at)
                                synced_count += 1
                                logger.info(f"✅ Synced single episode: {anime_title} - Ep {ep_number} (posted: {message.created_at})")
                        else:
                            logger.warning(f"⚠️ Episode not found: {anime_title} Ep {ep_number}")
                            failed_count += 1
        
        # Create result embed
        result_embed = discord.Embed(title="✅ Sinhronizacija zavrsena", color=discord.Color.green())
        result_embed.add_field(name="✅ Succeeded sinhronizovano", value=str(synced_count), inline=True)
        result_embed.add_field(name="❌ Neuspesno", value=str(failed_count), inline=True)
        result_embed.add_field(name="⏭️ Preskoceno", value=str(skipped_count), inline=True)
        result_embed.add_field(name="📊 Ukupno u listi", value=str(len(posted_episodes)), inline=True)
        result_embed.set_footer(text=f"Skenirano {message_limit} poruka po kanalu")
        
        await status_msg.edit(content=None, embed=result_embed)
        logger.info(f"✅ Metadata sync complete: {synced_count} synced, {failed_count} failed, {skipped_count} skipped")
        
    except Exception as e:
        error_logger.error(f"Error in sync_metadata command: {e}\n{traceback.format_exc()}")
        await ctx.send(f"❌ Error")

async def _main():
    import time as _time, webbrowser, asyncio
    logger.info("🌐 Dashboard starting on http://127.0.0.1:5050")
    # Open browser after a short delay so server is ready
    asyncio.get_event_loop().call_later(2.5, lambda: webbrowser.open("http://127.0.0.1:5050"))
    await asyncio.gather(
        start_dashboard(host="127.0.0.1", port=5050),
        bot.start(TOKEN),
    )

if __name__ == "__main__":
    logger.info("🚀 Starting bot + dashboard...")
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logger.info("👋 Shutting down.")
    except Exception as e:
        error_logger.critical(f"Failed to start: {e}\n{traceback.format_exc()}")