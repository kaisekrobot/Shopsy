import requests, json, base64, hmac, hashlib, time, urllib.parse, random
import threading, queue, os
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

# ========== CONFIG ==========
BASE_URL = "https://slayyourplaypromo.in/api/users"
MASTER_KEY = os.environ.get("SLAYPROMO_MASTER_KEY", "1709065004")
TELEGRAM_BOT_TOKEN="8776323932:AAEtjWItlFCOr0CD_medoBeNexkaublhCiI"
ADMIN_IDS = [8739344756,8183677305]
ADMIN_ID = ADMIN_IDS[0]  # back-compat

# 150 workers per user, designed for up to 50 simultaneous users
THREADS = int(os.environ.get("SLAYPROMO_THREADS", "150"))
REQUEST_DELAY = float(os.environ.get("SLAYPROMO_REQUEST_DELAY", "0"))
# Global pool hard cap: 50 users x 150 workers = 7500 slots.
# Tasks queue inside the executor when all threads are busy — no crash, no block.
GLOBAL_SEARCH_WORKERS = int(os.environ.get("SLAYPROMO_GLOBAL_WORKERS", "2000"))
# Queue only needs to stay ~10 steps ahead per worker batch
CODE_QUEUE_SIZE = 1500
MAX_TELEGRAM_MSG = 3500  # stay under Telegram 4096 with markup headroom

FINAL_API_ENDPOINT = "getUpiNo"

# Data lives next to the script by default, or under SLAYPROMO_DATA_DIR.
# Keep the data/ folder (or members_v2.json) with the bot so redeploys reload members.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("SLAYPROMO_DATA_DIR") or os.path.join(_SCRIPT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

AUTHORIZED_USERS_FILE = os.path.join(DATA_DIR, "members_v2.json")
# Legacy locations auto-migrated on startup so GitHub/redeploy does not wipe members
LEGACY_MEMBER_FILES = [
    os.path.join(_SCRIPT_DIR, "members_v2.json"),
    os.path.join(_SCRIPT_DIR, "members.json"),
    os.path.join(_SCRIPT_DIR, "authorized_users.json"),
    os.path.join(DATA_DIR, "members.json"),
    os.path.join(DATA_DIR, "authorized_users.json"),
]
ACCESS_REQUESTS_FILE = os.path.join(DATA_DIR, "access_requests_v2.json")
REFERRALS_FILE = os.path.join(DATA_DIR, "referrals_v2.json")
USAGE_LOG_FILE = os.path.join(DATA_DIR, "usage_log.json")
INVALID_CODES_DIR = os.path.join(DATA_DIR, "invalid_codes")
os.makedirs(INVALID_CODES_DIR, exist_ok=True)
INVALID_CODES_PREFIX = os.path.join(INVALID_CODES_DIR, "invalid_codes_")
BOT_USERNAME = ""  # filled at startup
REQUIRED_CHANNELS = [
    {
        "username": "axxuloots",
        "title":"AXXU AXXULOOTS",
        "url": "https://t.me/axxuloots",
        "button": "📢 Join AXXU LOOTS ",
    },
    {
        "username":"axxudiscuss",
        "title":"AXXU X DISCUSSION",
        "url": "https://t.me/axxudiscuss",
        "button": "📢 Join AXXU X DISCUSSION"
    },
]

OTP_PROXY_HOST = os.environ.get("OTP_PROXY_HOST", "")
OTP_PROXY_PORT = os.environ.get("OTP_PROXY_PORT", "")
OTP_PROXY_USER = os.environ.get("OTP_PROXY_USER", "")
OTP_PROXY_PASS = os.environ.get("OTP_PROXY_PASS", "")
_OTP_PROXY_URL = (
    f"http://{OTP_PROXY_USER}:{OTP_PROXY_PASS}@{OTP_PROXY_HOST}:{OTP_PROXY_PORT}"
    if OTP_PROXY_HOST and OTP_PROXY_USER
    else None
)
# ============================

# Set smaller stack size (128 KB) before spawning any threads.
# Default 8 MB x 7500 threads = 60 GB virtual; 128 KB x 7500 = ~900 MB — safe.
threading.stack_size(131072)

# Global search pool shared across all users. _POOL_MAX = 50 users x THREADS + headroom.
# Extra submissions queue inside the executor — no unbounded thread creation.
_POOL_MAX = max(GLOBAL_SEARCH_WORKERS, THREADS * 50, 7500)
_search_pool = ThreadPoolExecutor(max_workers=_POOL_MAX, thread_name_prefix="search")


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _atomic_json_write(path, obj):
    """Write JSON safely so a crash mid-write never corrupts members data."""
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _normalize_users_map(raw):
    """Accept several historical shapes and return {str_id: profile}."""
    if not isinstance(raw, dict):
        return {}
    if isinstance(raw.get("users"), dict):
        src = raw["users"]
    else:
        # flat map of id -> profile
        src = raw
    out = {}
    for k, v in src.items():
        if str(k).startswith("_"):
            continue
        if not isinstance(v, dict):
            continue
        # skip non-user keys
        if not str(k).lstrip("-").isdigit() and k not in ("users",):
            # still allow if looks like telegram id
            if not str(k).isdigit():
                continue
        key = str(k)
        if not key.lstrip("-").isdigit():
            continue
        prof = dict(v)
        prof.setdefault("expires_at", None)
        prof.setdefault("referred_by", "")
        prof.setdefault("name", "")
        prof.setdefault("username", "")
        prof.setdefault("points", 0)
        try:
            prof["points"] = int(prof.get("points", 0) or 0)
        except Exception:
            prof["points"] = 0
        # Members present in store are treated as approved unless explicitly false
        if "approved" not in prof:
            prof["approved"] = True
        else:
            prof["approved"] = bool(prof.get("approved"))
        prof.setdefault("successful_refers", 0)
        prof.setdefault("created_at", _iso(_now()))
        out[key] = prof
    return out


def _load_users_from_path(path):
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return _normalize_users_map(data)
    except Exception as e:
        print(f"[DATA] failed to load {path}: {e}")
        return {}


def load_authorized_users():
    """Load members from data/members_v2.json and merge any legacy files.

    This keeps member data across GitHub uploads / redeploys as long as the
    data folder (or a legacy members JSON next to the script) is present.
    """
    merged = {}
    # Prefer primary file first, then fill gaps from legacy stores
    primary = _load_users_from_path(AUTHORIZED_USERS_FILE)
    merged.update(primary)
    for path in LEGACY_MEMBER_FILES:
        if os.path.abspath(path) == os.path.abspath(AUTHORIZED_USERS_FILE):
            continue
        legacy = _load_users_from_path(path)
        for k, v in legacy.items():
            if k not in merged:
                merged[k] = v
            else:
                # keep richer profile fields
                cur = merged[k]
                for field in ("name", "username", "referred_by", "expires_at", "created_at"):
                    if not cur.get(field) and v.get(field):
                        cur[field] = v[field]
                cur["successful_refers"] = max(
                    int(cur.get("successful_refers", 0) or 0),
                    int(v.get("successful_refers", 0) or 0),
                )
                cur["points"] = max(
                    int(cur.get("points", 0) or 0),
                    int(v.get("points", 0) or 0),
                )
                if not cur.get("approved"):
                    cur["approved"] = bool(v.get("approved", True))
    return merged


authorized_users = load_authorized_users()
authorized_users_lock = threading.RLock()


def save_authorized_users():
    with authorized_users_lock:
        data = {"users": {}}
        for cid, prof in authorized_users.items():
            data["users"][cid] = prof
        _atomic_json_write(AUTHORIZED_USERS_FILE, data)


def load_referrals():
    if not os.path.exists(REFERRALS_FILE):
        return {"pending": {}, "completed": []}
    try:
        with open(REFERRALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[REFERRALS] load failed: {e}")
        return {"pending": {}, "completed": []}


referrals = load_referrals()
referrals_lock = threading.RLock()


def save_referrals(ref_data):
    with referrals_lock:
        _atomic_json_write(REFERRALS_FILE, ref_data)


def load_access_requests():
    if not os.path.exists(ACCESS_REQUESTS_FILE):
        return {}
    try:
        with open(ACCESS_REQUESTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ACCESS_REQUESTS] load failed: {e}")
        return {}


access_requests = load_access_requests()
access_requests_lock = threading.RLock()


def save_access_requests(req_data):
    with access_requests_lock:
        _atomic_json_write(ACCESS_REQUESTS_FILE, req_data)


def load_usage_log():
    global usage_log
    if not os.path.exists(USAGE_LOG_FILE):
        usage_log = {}
        return
    try:
        with open(USAGE_LOG_FILE, "r", encoding="utf-8") as f:
            usage_log = json.load(f)
    except Exception as e:
        print(f"[USAGE] load failed: {e}")
        usage_log = {}


usage_log = {}
usage_log_lock = threading.RLock()


def save_usage_log():
    with usage_log_lock:
        _atomic_json_write(USAGE_LOG_FILE, usage_log)


def log_usage(cid, mode, count):
    date = _now().strftime("%Y-%m-%d")
    with usage_log_lock:
        if date not in usage_log:
            usage_log[date] = {}
        if cid not in usage_log[date]:
            usage_log[date][cid] = {}
        if mode not in usage_log[date][cid]:
            usage_log[date][cid][mode] = 0
        usage_log[date][cid][mode] += count
        save_usage_log()


def is_admin(cid):
    return int(cid) in ADMIN_IDS


def _fmt_reward():
    return "1 Point"


def is_approved(cid):
    with authorized_users_lock:
        prof = authorized_users.get(str(cid), {})
        return bool(prof.get("approved", False))


def is_authorized(cid):
    """Admin has unlimited access forever. Regular users need points or valid expiry."""
    if is_admin(cid):
        return True
    with authorized_users_lock:
        prof = authorized_users.get(str(cid), {})
        if not prof.get("approved", False):
            return False
        points = int(prof.get("points", 0) or 0)
        if points > 0:
            return True
        expires_at = _parse_iso(prof.get("expires_at"))
        if expires_at and expires_at > _now():
            return True
    return False


def get_user_points(cid):
    """Get user's current points."""
    if is_admin(cid):
        return "∞ (Admin)"
    with authorized_users_lock:
        prof = authorized_users.get(str(cid), {})
        return int(prof.get("points", 0) or 0)


def add_user_points(cid, amount):
    """Add points to user (for referrals)."""
    if is_admin(cid):
        return
    with authorized_users_lock:
        if str(cid) not in authorized_users:
            return
        prof = authorized_users[str(cid)]
        current = int(prof.get("points", 0) or 0)
        prof["points"] = current + amount
        save_authorized_users()


def use_user_points(cid, amount):
    """Deduct points when user uses the bot."""
    if is_admin(cid):
        return True
    with authorized_users_lock:
        if str(cid) not in authorized_users:
            return False
        prof = authorized_users[str(cid)]
        current = int(prof.get("points", 0) or 0)
        if current >= amount:
            prof["points"] = current - amount
            save_authorized_users()
            return True
    return False


def no_time_message(cid):
    """Message when user has no access (no points or expired)."""
    points = get_user_points(cid)
    if is_admin(cid):
        return (
            "👑 *Admin Access*\n"
            "────────────────\n"
            "You have unlimited access forever.\n\n"
            "💡 Need to earn points?\n"
            "👉 Invite friends using /refer\n"
            "1 successful referral = 1 Point"
        )
    return (
        f"❌ *No Access*\n"
        "────────────────\n"
        f"📊 Your Points: *{points}*\n\n"
        "💡 How to get points?\n"
        f"1️⃣ Invite friends using /refer\n"
        f"2️⃣ Each successful referral = 1 Point\n"
        f"3️⃣ Use the bot with your points\n\n"
        "👉 Tap below to invite your friends"
    )


def home_message(cid):
    """Home/menu message."""
    points = get_user_points(cid)
    if is_admin(cid):
        return (
            "👑 *Admin Dashboard*\n"
            "────────────────\n"
            f"💬 Bot: @{BOT_USERNAME}\n"
            "📊 Access: ∞ (Unlimited)\n"
            "🎯 Status: Online\n\n"
            "👉 Choose an action below"
        )
    return (
        f"🏠 *Home*\n"
        "────────────────\n"
        f"📊 Your Points: *{points}*\n"
        f"✅ Status: Active\n\n"
        "💡 Every search costs 1 Point\n"
        "🎁 Invite friends to earn more!"
    )


def send_telegram(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.status_code == 200
    except Exception as e:
        print(f"[TG SEND] {chat_id}: {e}")
        return False


def send_edit_telegram(chat_id, msg_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": msg_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.status_code == 200
    except Exception as e:
        print(f"[TG EDIT] {chat_id}:{msg_id} {e}")
        return False


def admin_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🏠 Home", "callback_data": "home"}],
            [{"text": "📈 Full Session", "callback_data": "session_full"}],
            [{"text": "🔑 Login Only", "callback_data": "session_login"}],
            [{"text": "📊 Stats", "callback_data": "stats"}],
            [{"text": "👥 Users", "callback_data": "admin_list"}],
            [{"text": "🎁 Referrals", "callback_data": "admin_referrals"}],
            [{"text": "⚙️ Settings", "callback_data": "admin_settings"}],
        ]
    }


def main_menu_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🏠 Home", "callback_data": "home"}],
            [{"text": "📈 Full Session", "callback_data": "session_full"}],
            [{"text": "🔑 Login Only", "callback_data": "session_login"}],
            [{"text": "📊 Stats", "callback_data": "stats"}],
            [{"text": "🎁 Refer & Earn", "callback_data": "refer"}],
        ]
    }


def get_referral_link(uid):
    """Generate referral link for user."""
    if BOT_USERNAME:
        return f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
    return f"https://t.me/?start=ref_{uid}"


def enforce_channel_membership(chat_id, force=False, intro=""):
    """Check if user is in all required channels."""
    if is_admin(chat_id):
        return True, {}
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChatMember"
    status_map = {}
    for ch in REQUIRED_CHANNELS:
        try:
            r = requests.post(
                url,
                json={"chat_id": f"@{ch['username']}", "user_id": chat_id},
                timeout=10,
            )
            data = r.json()
            if data.get("ok"):
                member = data.get("result", {})
                status = member.get("status", "")
                status_map[ch["username"]] = status in ("member", "creator", "administrator")
            else:
                status_map[ch["username"]] = False
        except Exception as e:
            print(f"[MEMBER CHECK] {chat_id} {ch['username']}: {e}")
            status_map[ch["username"]] = False
    
    all_joined = all(status_map.values())
    if not all_joined and (force or not intro):
        send_join_gate(chat_id, status_map=status_map, intro=intro)
    return all_joined, None, status_map


def send_join_gate(chat_id, status_map=None, intro=""):
    """Send channel join requirement."""
    if status_map is None:
        status_map = {}
    
    if not intro:
        intro = "⛔ *Access locked*\nJoin both channels to continue."
    
    text = intro + "\n\n"
    kb = {"inline_keyboard": []}
    for ch in REQUIRED_CHANNELS:
        joined = status_map.get(ch["username"], False)
        mark = "✅" if joined else "❌"
        text += f"{mark} {ch['title']}\n"
        if not joined:
            kb["inline_keyboard"].append([
                {"text": ch["button"], "url": ch["url"]}
            ])
    
    kb["inline_keyboard"].append([
        {"text": "✅ I've Joined", "callback_data": "check_join"}
    ])
    
    send_telegram(chat_id, text, kb)


def check_channel_membership_cached(chat_id, force=False):
    """Quick cached check for watchdog."""
    if is_admin(chat_id):
        return True, None, {}
    return enforce_channel_membership(chat_id, force=force)


def unlock_after_join(chat_id, user_info=None):
    """Auto-approve user after joining channels."""
    with authorized_users_lock:
        if str(chat_id) not in authorized_users:
            prof = {
                "approved": True,
                "expires_at": None,
                "referred_by": "",
                "name": user_info.get("first_name", "") if user_info else "",
                "username": user_info.get("username", "") if user_info else "",
                "points": 0,
                "successful_refers": 0,
                "created_at": _iso(_now()),
            }
            authorized_users[str(chat_id)] = prof
            save_authorized_users()
            print(f"[APPROVE] auto-approved {chat_id}")


def get_usage_report():
    """Generate daily usage report."""
    with usage_log_lock:
        today = _now().strftime("%Y-%m-%d")
        if today not in usage_log:
            return "📊 *No searches today*"
        
        stats = usage_log[today]
        total_users = len(stats)
        total_full = sum(s.get("full", 0) for s in stats.values())
        total_login = sum(s.get("login", 0) for s in stats.values())
        
        return (
            f"📊 *Daily Usage Report*\n"
            f"────────────────\n"
            f"📅 Date: *{today}*\n"
            f"👥 Active Users: *{total_users}*\n"
            f"🔍 Full Searches: *{total_full}*\n"
            f"🔑 Login Searches: *{total_login}*\n"
            f"📈 Total: *{total_full + total_login}*"
        )


def create_session(chat_id, mode="full"):
    """Create a new user session."""
    return UserSession(chat_id, mode)


user_sessions = {}
sessions_lock = threading.RLock()


def get_or_create_session(chat_id):
    with sessions_lock:
        if chat_id not in user_sessions:
            user_sessions[chat_id] = create_session(chat_id)
        return user_sessions[chat_id]


def stop_user_session_if_active(chat_id, reason=""):
    with sessions_lock:
        if chat_id in user_sessions:
            user_sessions[chat_id].request_stop()
            if reason:
                send_telegram(chat_id, reason)


class UserSession:
    def __init__(self, chat_id, mode="full"):
        self.chat_id = chat_id
        self.mode = mode
        self.session_active = False
        self.request_stop_flag = False
        self.new_session_event = threading.Event()
        self.next_session_mode = mode
        self.worker_thread = None
        self.current_task = None

    def request_stop(self):
        if self.session_active:
            self.request_stop_flag = True
            return True
        return False

    def get_stats_reply(self):
        """Get user's stats."""
        points = get_user_points(self.chat_id)
        if is_admin(self.chat_id):
            return (
                "📊 *Your Stats*\n"
                "────────────────\n"
                f"👤 ID: `{self.chat_id}`\n"
                f"📊 Access: ∞ (Admin)\n"
                f"🎁 Status: Active\n\n"
                "👑 Admin commands ready"
            )
        
        with authorized_users_lock:
            prof = authorized_users.get(str(self.chat_id), {})
            successful_refers = int(prof.get("successful_refers", 0) or 0)
        
        return (
            f"📊 *Your Stats*\n"
            f"────────────────\n"
            f"👤 ID: `{self.chat_id}`\n"
            f"💰 Points: *{points}*\n"
            f"🎁 Successful Referrals: *{successful_refers}*\n\n"
            f"💡 Each referral = 1 Point"
        )

    def handle_input(self, text):
        """Handle user text input during session."""
        pass

    def run_session(self):
        """Main session loop."""
        while True:
            if self.request_stop_flag:
                self.session_active = False
                self.request_stop_flag = False
                with sessions_lock:
                    if self.chat_id in user_sessions:
                        del user_sessions[self.chat_id]
                break
            time.sleep(0.5)


def callback_handler(query):
    """Handle button clicks."""
    chat_id = query["from"]["id"]
    callback_data = query.get("data", "")
    msg_id = query.get("message", {}).get("message_id")
    
    if callback_data == "home":
        kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
        send_telegram(chat_id, home_message(chat_id), kb)
        return
    
    if callback_data == "stats":
        user_session = get_or_create_session(chat_id)
        kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
        send_telegram(chat_id, user_session.get_stats_reply(), kb)
        return
    
    if callback_data == "refer":
        ref_link = get_referral_link(chat_id)
        kb = {
            "inline_keyboard": [
                [{"text": "📋 Copy Link", "callback_data": "ref_copy"}],
                [{"text": "🏠 Back", "callback_data": "home"}],
            ]
        }
        send_telegram(
            chat_id,
            f"🎁 *Invite Friends & Earn Points*\n"
            f"────────────────\n"
            f"📌 Your Referral Link:\n"
            f"`{ref_link}`\n\n"
            f"📊 System: 1 Referral = 1 Point\n"
            f"✅ Share with friends to earn!\n\n"
            f"📈 Your Referrals:\n"
            f"👤 Successful: {get_user_points(chat_id)}",
            kb
        )
        return
    
    if callback_data == "check_join":
        ok, _, status_map = enforce_channel_membership(chat_id, force=True)
        if ok:
            unlock_after_join(chat_id, query.get("from", {}))
            kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
            send_telegram(chat_id, "✅ *Verified!*\n\nYou can now access the bot.", kb)
        return
    
    if callback_data.startswith("session_"):
        if not is_authorized(chat_id):
            send_telegram(chat_id, no_time_message(chat_id), main_menu_keyboard())
            return
        
        user_session = get_or_create_session(chat_id)
        if callback_data == "session_full":
            if not user_session.session_active:
                user_session.next_session_mode = "full"
                user_session.new_session_event.set()
                send_telegram(chat_id, "🚀 Starting full session…")
        elif callback_data == "session_login":
            if not user_session.session_active:
                user_session.next_session_mode = "login"
                user_session.new_session_event.set()
                send_telegram(chat_id, "🔑 Starting login-only session…")
        return
    
    if is_admin(chat_id):
        if callback_data == "admin_list":
            with authorized_users_lock:
                users = authorized_users
            total = len(users)
            approved = sum(1 for p in users.values() if p.get("approved"))
            send_telegram(
                chat_id,
                f"👥 *Users Overview*\n"
                f"────────────────\n"
                f"📊 Total Users: *{total}*\n"
                f"✅ Approved: *{approved}*\n"
                f"❌ Pending: *{total - approved}*\n\n"
                f"📁 File: `{AUTHORIZED_USERS_FILE}`",
                admin_keyboard()
            )
            return
        
        if callback_data == "admin_referrals":
            with referrals_lock:
                ref_data = referrals
            completed = len(ref_data.get("completed", []))
            pending = len(ref_data.get("pending", {}))
            send_telegram(
                chat_id,
                f"🎁 *Referral Stats*\n"
                f"────────────────\n"
                f"✅ Completed: *{completed}*\n"
                f"⏳ Pending: *{pending}*\n\n"
                f"📊 System: 1 Referral = 1 Point",
                admin_keyboard()
            )
            return
        
        if callback_data == "admin_settings":
            send_telegram(
                chat_id,
                f"⚙️ *Admin Settings*\n"
                f"────────────────\n"
                f"🤖 Bot: @{BOT_USERNAME}\n"
                f"📊 Members: {len(authorized_users)}\n"
                f"💾 Data Dir: `{DATA_DIR}`\n\n"
                f"👑 Status: Online\n"
                f"🔓 Access: Unlimited",
                admin_keyboard()
            )
            return


def telegram_listener():
    """Listen to Telegram updates."""
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            r = requests.get(url, params={"offset": offset, "timeout": 30}, timeout=35)
            data = r.json()
            
            if not data.get("ok"):
                print(f"[TG LISTENER] not ok: {data}")
                time.sleep(5)
                continue
            
            for msg in data.get("result", []):
                offset = max(offset, msg["update_id"] + 1)
                
                if "callback_query" in msg:
                    callback_handler(msg["callback_query"])
                    continue
                
                if "message" not in msg:
                    continue
                
                msg_data = msg["message"]
                chat_id = msg_data.get("chat", {}).get("id")
                text = msg_data.get("text", "").strip()
                
                if not chat_id or not text:
                    continue
                
                lower = text.lower()
                
                # Admin commands
                if is_admin(chat_id):
                    if lower.startswith("/grant "):
                        parts = text.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            target_id = int(parts[1])
                            points = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
                            with authorized_users_lock:
                                if str(target_id) in authorized_users:
                                    prof = authorized_users[str(target_id)]
                                    prof["points"] = int(prof.get("points", 0) or 0) + points
                                    save_authorized_users()
                            send_telegram(
                                chat_id,
                                f"✅ Granted *{points}* points to `{target_id}`",
                                admin_keyboard()
                            )
                        else:
                            send_telegram(chat_id, "❌ Usage: /grant <user_id> [points]", admin_keyboard())
                        continue
                    
                    if lower.startswith("/grantall "):
                        parts = text.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            points = int(parts[1])
                            with authorized_users_lock:
                                for prof in authorized_users.values():
                                    prof["points"] = int(prof.get("points", 0) or 0) + points
                                save_authorized_users()
                            send_telegram(
                                chat_id,
                                f"✅ Granted *{points}* points to ALL users",
                                admin_keyboard()
                            )
                        else:
                            send_telegram(chat_id, "❌ Usage: /grantall <points>", admin_keyboard())
                        continue
                    
                    if lower.startswith("/add "):
                        parts = text.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            target_id = int(parts[1])
                            with authorized_users_lock:
                                if str(target_id) not in authorized_users:
                                    authorized_users[str(target_id)] = {
                                        "approved": True,
                                        "expires_at": None,
                                        "referred_by": "",
                                        "name": "",
                                        "username": "",
                                        "points": 0,
                                        "successful_refers": 0,
                                        "created_at": _iso(_now()),
                                    }
                                else:
                                    authorized_users[str(target_id)]["approved"] = True
                                save_authorized_users()
                            send_telegram(chat_id, f"✅ Approved `{target_id}`", admin_keyboard())
                        else:
                            send_telegram(chat_id, "❌ Usage: /add <user_id>", admin_keyboard())
                        continue
                    
                    if lower.startswith("/remove "):
                        parts = text.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            target_id = int(parts[1])
                            with authorized_users_lock:
                                if str(target_id) in authorized_users:
                                    del authorized_users[str(target_id)]
                                    save_authorized_users()
                            send_telegram(chat_id, f"✅ Removed `{target_id}`", admin_keyboard())
                        else:
                            send_telegram(chat_id, "❌ Usage: /remove <user_id>", admin_keyboard())
                        continue
                    
                    if lower == "/list":
                        with authorized_users_lock:
                            users = list(authorized_users.keys())
                        msg_text = "👥 *User List*\n────────────────\n"
                        for i, uid in enumerate(users[:50], 1):
                            msg_text += f"{i}. `{uid}`\n"
                        if len(users) > 50:
                            msg_text += f"\n... and {len(users) - 50} more"
                        send_telegram(chat_id, msg_text, admin_keyboard())
                        continue
                    
                    if lower.startswith("/broadcast "):
                        bmsg = text[len("/broadcast "):].strip()
                        with authorized_users_lock:
                            recipients = list(authorized_users.keys())
                        
                        def _do_broadcast(aid, bmsg, recipients):
                            sent = fail = 0
                            for recipient in recipients:
                                try:
                                    send_telegram(int(recipient), bmsg)
                                    sent += 1
                                except Exception:
                                    fail += 1
                                time.sleep(0.05)
                            send_telegram(
                                aid,
                                f"✅ Broadcast done\n📤 Sent: *{sent}*\n❌ Failed: *{fail}*",
                                admin_keyboard(),
                            )
                        threading.Thread(
                            target=_do_broadcast,
                            args=(chat_id, bmsg, recipients),
                            daemon=True,
                        ).start()
                        send_telegram(
                            chat_id,
                            f"📢 Broadcasting to *{len(recipients)}* users…",
                            admin_keyboard(),
                        )
                        continue
                    
                    if lower == "/usage":
                        send_telegram(chat_id, get_usage_report(), admin_keyboard())
                        continue

                if text.lower().startswith("/start"):
                    parts = text.split(maxsplit=1)
                    payload = parts[1].strip() if len(parts) >= 2 else ""
                    ref_str = ""
                    if payload.startswith("ref_"):
                        ref_str = payload[4:].split()[0]
                    elif payload.isdigit():
                        ref_str = payload
                    if ref_str.isdigit():
                        rid = int(ref_str)
                        if rid != chat_id and not is_admin(chat_id):
                            with referrals_lock:
                                already_pending = str(chat_id) in referrals.get("pending", {})
                                already_done = any(
                                    str(c.get("new_user")) == str(chat_id)
                                    for c in referrals.get("completed", [])
                                )
                                if not already_pending and not already_done:
                                    referrals.setdefault("pending", {})[str(chat_id)] = str(rid)
                                    save_referrals(referrals)
                                    print(f"[REFERRAL] pending set new_user={chat_id} referrer={rid}")
                            send_telegram(
                                chat_id,
                                "👋 *Welcome!*\n"
                                "────────────────\n"
                                f"🎁 Invited by `{rid}`\n\n"
                                "1️⃣ Join both channels\n"
                                "2️⃣ Tap *✅ I've Joined*\n"
                                f"3️⃣ Your friend gets *{_fmt_reward()}*",
                            )

                ok, status_map = enforce_channel_membership(
                    chat_id,
                    force=False,
                    intro="⛔ *Access locked*\nStay joined in both channels to use the bot.",
                )
                if not ok:
                    continue

                # Auto-approve after join — no request-access gate
                if not is_admin(chat_id) and not is_approved(chat_id):
                    unlock_after_join(chat_id, msg_data.get("from", {}))

                if not is_authorized(chat_id):
                    send_telegram(chat_id, no_time_message(chat_id), main_menu_keyboard())
                    continue

                user_session = get_or_create_session(chat_id)

                if text.lower() in ("/start", "/help", "/menu", "/home"):
                    kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
                    send_telegram(chat_id, home_message(chat_id), kb)
                    continue

                if text.lower() == "/stop":
                    if user_session.request_stop():
                        send_telegram(chat_id, "🛑 Stopping session…")
                    else:
                        send_telegram(chat_id, "ℹ️ No active session right now.")
                    continue

                if text.lower() == "/stats":
                    kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
                    send_telegram(chat_id, user_session.get_stats_reply(), kb)
                    continue

                if text.lower() == "/new":
                    if not user_session.session_active:
                        user_session.next_session_mode = "full"
                        user_session.new_session_event.set()
                        send_telegram(chat_id, "🚀 Starting full session…")
                    else:
                        send_telegram(chat_id, "⚠️ Session already running.\n👉 Send /stop first.")
                    continue

                if text.lower() == "/login":
                    if not user_session.session_active:
                        user_session.next_session_mode = "login"
                        user_session.new_session_event.set()
                        send_telegram(chat_id, "🔑 Starting login-only session…")
                    else:
                        send_telegram(chat_id, "⚠️ Session already running.\n👉 Send /stop first.")
                    continue

                user_session.handle_input(text)

        except Exception as e:
            print("Listener error:", e)
            time.sleep(1)


def membership_watchdog():
    while True:
        try:
            with sessions_lock:
                active_ids = [cid for cid, s in user_sessions.items() if s.session_active]
            for cid in active_ids:
                if is_admin(cid):
                    continue
                all_joined, _, status_map = check_channel_membership_cached(cid, force=True)
                if all_joined:
                    continue
                stop_user_session_if_active(
                    cid,
                    reason=(
                        "⛔ *Session stopped*\n"
                        "────────────────\n"
                        "You left a required channel."
                    ),
                )
                send_join_gate(
                    cid,
                    status_map=status_map,
                    intro="⛔ *Access locked*\nRejoin both channels to continue.",
                )
        except Exception as e:
            print("Watchdog error:", e)
        time.sleep(30)


def main():
    global BOT_USERNAME
    load_usage_log()
    print(f"DATA_DIR={DATA_DIR}")
    print(f"Members file: {AUTHORIZED_USERS_FILE}")
    print(f"Loaded members: {len(authorized_users)}")
    print(f"Search: THREADS/user={THREADS} GLOBAL_WORKERS={GLOBAL_SEARCH_WORKERS} POOL_MAX={_POOL_MAX}")
    print(f"Referral reward: 1 Point per referral | Admin access: Unlimited")
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        try:
            j = r.json()
        except Exception:
            j = {}
        if isinstance(j, dict) and j.get("ok"):
            BOT_USERNAME = j["result"].get("username") or ""
            print(f"Bot username: @{BOT_USERNAME}")
        else:
            print(f"getMe not ok: {j}")
    except Exception as e:
        print(f"getMe failed: {e}")

    listener = threading.Thread(target=telegram_listener, daemon=True)
    listener.start()
    watchdog = threading.Thread(target=membership_watchdog, daemon=True)
    watchdog.start()
    start_msg = (
        "🤖 *Bot online*\n"
        "────────────────\n"
        "👑 Admins: " + ", ".join(f"`{a}`" for a in ADMIN_IDS) + "\n\n"
        "🛠 *Commands*\n"
        "🎁 `/grant <id> [points]` — grant points\n"
        "🎯 `/grantall <points>` — grant points to ALL members\n"
        "✅ `/add <id>` — approve (no points)\n"
        "🗑 `/remove <id>` — revoke\n"
        "👥 `/list` — users\n"
        "📈 `/usage` — daily report\n"
        "📢 `/broadcast <msg>` — message all\n\n"
        "📌 *Access rules*\n"
        "• Admin: ∞ Unlimited access forever\n"
        "• Join channels → auto-approved\n"
        "• 1 successful referral = 1 Point\n"
        "• Use bot → costs 1 Point per search\n"
        "• Leave a channel → session stops\n\n"
        f"📁 Members: `{AUTHORIZED_USERS_FILE}`"
    )
    for aid in ADMIN_IDS:
        send_telegram(aid, start_msg, admin_keyboard())
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
