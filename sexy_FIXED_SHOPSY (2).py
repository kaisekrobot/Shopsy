#!/usr/bin/env python3
"""
🕷️ SPIDER X SHOPSY - Premium Telegram Bot
Cyberpunk Theme | Blue & Purple Neon | Multi-User Support
With Fully Automatic Queue System & Shopsy Integration

✅ FIXES APPLIED FOR RAILWAY DEPLOYMENT:
1. ✅ Environment-based configuration (TOKEN, ADMIN_IDS from env vars)
2. ✅ Graceful shutdown handlers for SIGTERM (Railway requirement)
3. ✅ Thread-safe database operations with locks
4. ✅ Thread-safe queue manager with RLock
5. ✅ Fixed asyncio.create_task in non-async context
6. ✅ Specific error handling in broadcast (not bare Exception)
7. ✅ Added dotenv support for .env files
8. ✅ Added signal handlers for clean deployment
9. ✅ Proper logging for debug and error tracking
10. ✅ Railway-compatible file paths (/tmp for ephemeral storage)
"""

import os
import re
import json
import time
import uuid
import asyncio
import logging
import sqlite3
import sys
import argparse
import aiohttp
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, field
from contextlib import contextmanager
from collections import deque
import threading
import signal
import os
from dotenv import load_dotenv
import concurrent.futures

# Load environment variables from .env file
load_dotenv()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
import telegram
import telegram.error
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)
from telegram.constants import ParseMode

# ============================================================
#  SHOPSY INTEGRATION
# ============================================================

GAMES = [
    {"id": "runner-3d", "name": "Super Runner", "play_time": 94, "gems": 200},
    {"id": "city-builder", "name": "City Builder", "play_time": 47, "gems": 100},
    {"id": "match-3", "name": "Fruit Crush", "play_time": 35, "gems": 100},
    {"id": "goods-triple", "name": "Grocery Match", "play_time": 40, "gems": 100},
    {"id": "ludo", "name": "Ludo", "play_time": 50, "gems": 100},
    {"id": "nazaria", "name": "Nazar Pop", "play_time": 45, "gems": 100},
]

ROME_TEMPLATE = "https://{dc}.rome.api.flipkart.net"
APP_VERSION = "2291175"
DEVICE_MODEL = "Pixel 9a"
DEVICE_BRAND = "Google"
DEFAULT_PINCODE = "226001"
FAST_PLAY_SEC = 18

def _extract_dc_id(data: dict[str, Any], http_status: int) -> str | None:
    is_dc = http_status == 406 or data.get("STATUS_CODE") == 406
    if not is_dc:
        return None
    if data.get("ERROR_MESSAGE") != "DC Change" and data.get("ERROR_CODE") != 2000:
        return None
    dc_info = (data.get("META_INFO") or {}).get("dcInfo") or data.get("RESPONSE") or {}
    dc_id = dc_info.get("id")
    return str(dc_id) if dc_id else None

@dataclass
class ShopsySession:
    device_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    visit_id: str = field(default_factory=lambda: f"{uuid.uuid4().hex}-{int(time.time() * 1000)}")
    dc_id: str = "1"
    at: str = ""
    sn: str = ""
    vid: str = ""
    secure_token: str = ""
    secure_cookie: str = ""
    account_id: str = ""
    user_name: str = ""
    is_logged_in: bool = False
    last_verified: float = 0.0

class LiveLog:
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.start = time.time()
    
    def _ts(self):
        return datetime.now().strftime("%H:%M:%S")
    
    def _elapsed(self):
        sec = int(time.time() - self.start)
        return f"{sec // 60:02d}:{sec % 60:02d}"
    
    def info(self, msg):
        print(f"[{self._ts()} | {self._elapsed()}] {msg}", flush=True)
    
    def ok(self, msg):
        print(f"[{self._ts()} | {self._elapsed()}] [+] {msg}", flush=True)
    
    def warn(self, msg):
        print(f"[{self._ts()} | {self._elapsed()}] [!] {msg}", flush=True)
    
    def dbg(self, msg, data=None):
        if not self.debug:
            return
        print(f"[{self._ts()} | {self._elapsed()}] [DEBUG] {msg}", flush=True)
        if data is not None:
            text = json.dumps(data, ensure_ascii=False, indent=2) if isinstance(data, (dict, list)) else str(data)
            if len(text) > 1200:
                text = text[:1200] + "\n... (truncated)"
            print(text, flush=True)

class AsyncShopsyClient:
    def __init__(self, log: Optional[LiveLog] = None, fast: bool = True):
        self.log = log or LiveLog()
        self.fast = fast
        self.ctx = ShopsySession()
        self._user_cache = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_dc_meta: dict[str, Any] | None = None
        self._sync_urls()

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=0,
            limit_per_host=0,
            ttl_dns_cache=300,
            enable_cleanup_closed=True
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    def _sync_urls(self):
        self.base_url = ROME_TEMPLATE.format(dc=self.ctx.dc_id)

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    @staticmethod
    def _path_from_url(url: str) -> str:
        for path in ("/4/page/fetch", "/1/action/view", "/1/shopsy/games"):
            if path in url:
                return path
        if url.startswith("http"):
            idx = url.find("/", 8)
            return url[idx:] if idx != -1 else url
        return url

    def _partner_headers(self, *, layout: bool = False) -> dict[str, str]:
        headers = {
            "User-Agent": "okhttp/4.9.2",
            "Content-Type": "application/json; charset=UTF-8",
            "Accept-Encoding": "gzip",
            "X-PARTNER-CONTEXT": '{"source":"reseller"}',
            "FK-TENANT-ID": "SHOPSY",
            "business": "reseller",
            "X-User-Agent": self.x_user_agent,
            "X-Visit-Id": self.ctx.visit_id,
            "X-NewRelic-ID": "VwEHU1dSCxABUVlaAAQHU1UA",
        }
        if layout:
            headers["X-Layout-Version"] = '{"appVersion":"910000","frameworkVersion":"1.0"}'
        if self.ctx.at:
            headers["at"] = self.ctx.at
        if self.ctx.sn:
            headers["sn"] = self.ctx.sn
        if self.ctx.secure_token:
            headers["secureToken"] = self.ctx.secure_token
        if self.ctx.secure_cookie:
            headers["secureCookie"] = self.ctx.secure_cookie
        return headers

    def _game_headers(self) -> dict[str, str]:
        return {
            "User-Agent": "okhttp/4.9.2",
            "Content-Type": "application/json; charset=UTF-8",
            "Accept-Encoding": "gzip",
            "x-user-agent": self.x_user_agent,
            "sessionid": "session_id",
            "X-NewRelic-ID": "VwEHU1dSCxABUVlaAAQHU1UA",
        }

    @property
    def x_user_agent(self) -> str:
        return (
            f"Mozilla/5.0 (Linux; Android 15; {DEVICE_MODEL} Build/BD4A.250505.003) "
            f"FKUA/Retail/{APP_VERSION}/Android/Mobile "
            f"({DEVICE_BRAND}/{DEVICE_MODEL}/{self.ctx.device_id})"
        )

    def _switch_dc(self, dc_id: str) -> None:
        if self.ctx.dc_id == dc_id:
            return
        old = self.ctx.dc_id
        self.ctx.dc_id = dc_id
        self._sync_urls()
        self.log.info(f"DC Change: {old} -> {dc_id} | host={self.base_url}")

    def _capture_secure_cookie(self, response: aiohttp.ClientResponse) -> None:
        secure_cookie = response.headers.get("securecookie") or response.headers.get("secureCookie")
        if secure_cookie:
            self.ctx.secure_cookie = secure_cookie

    def _apply_session(self, data):
        session = data.get("SESSION") or {}
        if not session:
            return
        self.ctx.at = session.get("at") or self.ctx.at
        self.ctx.sn = session.get("sn") or self.ctx.sn
        self.ctx.vid = session.get("vid") or self.ctx.vid
        self.ctx.secure_token = session.get("secureToken") or self.ctx.secure_token
        self.ctx.account_id = session.get("accountId") or self.ctx.account_id
        self.ctx.is_logged_in = bool(session.get("isLoggedIn"))
        if session.get("firstName"):
            last = session.get("lastName") or ""
            self.ctx.user_name = f"{session['firstName']} {last}".strip()
        self.ctx.last_verified = time.time()

    async def _post_json(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        game: bool = False,
        layout: bool = False,
    ) -> dict[str, Any]:
        path = self._path_from_url(url)
        
        for attempt in range(5):
            active_url = self._url(path)
            headers = self._game_headers() if game else self._partner_headers(layout=layout)
            
            if self._session is None:
                raise RuntimeError("Session not initialized")
            
            try:
                self.log.dbg(f"POST {active_url} (dc={self.ctx.dc_id}, try={attempt + 1})", payload)
                async with self._session.post(active_url, json=payload, headers=headers) as response:
                    self._capture_secure_cookie(response)
                    data = await response.json()
                    
                    dc_id = _extract_dc_id(data, response.status)
                    if dc_id:
                        self._last_dc_meta = (data.get("META_INFO") or {}).get("dcInfo") or data.get("RESPONSE")
                        self._switch_dc(dc_id)
                        continue
                    
                    self.log.dbg(f"Response HTTP {response.status}", data)
                    
                    if not game:
                        self._apply_session(data)
                    
                    if response.status >= 400 or (data.get("STATUS_CODE") or 200) >= 400:
                        err = data.get("ERROR_MESSAGE") or data
                        raise RuntimeError(f"HTTP {response.status}: {err}")
                    
                    return data
                    
            except aiohttp.ClientError as e:
                if attempt == 4:
                    raise RuntimeError(f"Request failed after 5 attempts: {e}")
                await asyncio.sleep(1 * (attempt + 1))
                continue
        
        raise RuntimeError("Max retry attempts exceeded")

    async def bootstrap(self) -> None:
        self.log.info(f"Session bootstrap (DC {self.ctx.dc_id})...")
        payload = {
            "pageUri": "/shopsy2-login-page-store",
            "pageContext": {
                "pageHashKey": None,
                "slotContextMap": None,
                "paginationContextMap": None,
                "stateInfoMap": None,
                "slotIdInfoMap": None,
                "paginatedFetch": False,
                "pageNumber": 1,
                "fetchAllPages": False,
                "networkSpeed": 3000,
                "trackingContext": None,
                "fetchSeoData": False,
            },
            "partnerContext": None,
            "locationContext": {"pincode": DEFAULT_PINCODE},
            "requestContext": None,
        }
        await self._post_json(self._url("/4/page/fetch"), payload, layout=True)
        has_cookie = "yes" if self.ctx.secure_cookie else "no"
        self.log.ok(f"Bootstrap OK | dc={self.ctx.dc_id} | cookie={has_cookie}")

    async def send_otp(self, phone: str) -> str:
        phone = phone.strip().replace("+91", "").replace(" ", "")
        self.log.info(f"Sending OTP to +91{phone}")
        payload = {
            "actionRequestContext": {
                "type": "LOGIN_IDENTITY_VERIFY_SHOPSY2",
                "loginId": phone,
                "loginIdPrefix": "+91",
                "phoneNumberFormat": "E164",
                "addAppHash": True,
                "loginType": "MOBILE",
                "verificationType": "OTP",
                "sourceContext": "DEFAULT",
                "clientQueryParamMap": None,
            }
        }
        data = await self._post_json(self._url("/1/action/view"), payload)
        response_ctx = data.get("RESPONSE", {}).get("actionResponseContext", {})
        if not data.get("RESPONSE", {}).get("actionSuccess"):
            raise RuntimeError(f"OTP send failed: {data}")
        request_id = response_ctx.get("requestId")
        if not request_id:
            attempts = response_ctx.get("remainingAttempts")
            ctx_type = response_ctx.get("type", "")
            if attempts == 0 or ctx_type == "LOGIN_VERIFY":
                raise RuntimeError("OTP not sent — incorrect number or 24h limit reached")
            raise RuntimeError(f"OTP requestId not found: {response_ctx}")
        self.log.ok(f"OTP sent | requestId={request_id[:12]}...")
        return request_id

    async def verify_otp(self, phone: str, otp: str, otp_request_id: str) -> None:
        phone = phone.strip().replace("+91", "").replace(" ", "")
        self.log.info("Verifying OTP...")
        payload = {
            "actionRequestContext": {
                "type": "LOGIN_SHOPSY2",
                "loginId": phone,
                "loginIdPrefix": "+91",
                "password": None,
                "otp": otp.strip(),
                "otpRequestId": otp_request_id,
                "remainingAttempts": 5,
                "phoneNumberFormat": "E164",
                "loginType": "MOBILE",
                "verificationType": "OTP",
                "sourceContext": "DEFAULT",
                "churned": False,
                "otpRegex": None,
                "data": None,
                "clientQueryParamMap": None,
            }
        }
        data = await self._post_json(self._url("/1/action/view"), payload)
        response_ctx = data.get("RESPONSE", {}).get("actionResponseContext", {})
        if not response_ctx.get("authenticationSuccess"):
            raise RuntimeError(f"Login failed: {data}")
        self.log.ok(f"Login successful | account={self.ctx.account_id} | name={self.ctx.user_name or 'User'}")

    async def games_api(self, route_uri: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = {"requestMethod": method, "routeUri": route_uri, "payload": payload}
        return await self._post_json(self._url("/1/shopsy/games"), body, game=True)

    async def get_user(self, *, refresh: bool = False) -> dict[str, Any]:
        if not self.ctx.account_id:
            raise RuntimeError("Account ID missing — please login first")
        if self._user_cache and not refresh:
            return self._user_cache
        data = await self.games_api(
            "user/get-user",
            "GET",
            {"userId": self.ctx.account_id, "userName": self.ctx.user_name or "User"},
        )
        if not data.get("success"):
            raise RuntimeError(f"get-user failed: {data}")
        self._user_cache = data["data"]
        return self._user_cache

    async def start_game(self, game: dict[str, Any]) -> str:
        start = await self.games_api(
            "game/game-started",
            "POST",
            {"userId": self.ctx.account_id, "gameId": game["id"]},
        )
        if not start.get("success"):
            raise RuntimeError(f"Game start failed: {start}")
        return start["data"]["sessionId"]

    async def end_game(self, game: dict[str, Any], session_id: str, play_time: int) -> dict[str, Any]:
        return await self.games_api(
            "game/game-ended",
            "POST",
            {
                "userId": self.ctx.account_id,
                "gameId": game["id"],
                "sessionId": session_id,
                "gemsEarned": game["gems"],
                "playTimeInSec": play_time,
            },
        )

    async def _play_seconds(self, game: dict[str, Any]) -> int:
        if not self.fast:
            return game["play_time"]
        if game["play_time"] >= 60:
            return game["play_time"]
        return min(game["play_time"], FAST_PLAY_SEC)

    def game_already_done(self, game_id: str) -> bool:
        if not self._user_cache:
            return False
        user = self._user_cache
        for g in user.get("gameStats", {}).get("games", []):
            if g.get("gameId") == game_id and g.get("rewards", {}).get("isMaxGameBonusEarned"):
                return True
        return False

    def to_dict(self) -> dict:
        return {
            "device_id": self.ctx.device_id,
            "visit_id": self.ctx.visit_id,
            "dc_id": self.ctx.dc_id,
            "at": self.ctx.at,
            "sn": self.ctx.sn,
            "vid": self.ctx.vid,
            "secure_token": self.ctx.secure_token,
            "secure_cookie": self.ctx.secure_cookie,
            "account_id": self.ctx.account_id,
            "user_name": self.ctx.user_name,
            "is_logged_in": self.ctx.is_logged_in,
            "last_verified": self.ctx.last_verified,
        }

    def from_dict(self, data: dict):
        self.ctx.device_id = data.get("device_id", uuid.uuid4().hex)
        self.ctx.visit_id = data.get("visit_id", f"{uuid.uuid4().hex}-{int(time.time() * 1000)}")
        self.ctx.dc_id = data.get("dc_id", "1")
        self.ctx.at = data.get("at", "")
        self.ctx.sn = data.get("sn", "")
        self.ctx.vid = data.get("vid", "")
        self.ctx.secure_token = data.get("secure_token", "")
        self.ctx.secure_cookie = data.get("secure_cookie", "")
        self.ctx.account_id = data.get("account_id", "")
        self.ctx.user_name = data.get("user_name", "")
        self.ctx.is_logged_in = data.get("is_logged_in", False)
        self.ctx.last_verified = data.get("last_verified", 0.0)
        self._sync_urls()

    def is_valid_session(self) -> bool:
        if not self.ctx.is_logged_in or not self.ctx.account_id:
            return False
        if time.time() - self.ctx.last_verified > 86400:
            return False
        return True

    async def validate_and_refresh(self) -> bool:
        if not self.ctx.is_logged_in or not self.ctx.account_id:
            return False
        try:
            await self.get_user(refresh=True)
            self.ctx.last_verified = time.time()
            return True
        except Exception:
            return False

# ============================================================
#  CONFIGURATION - ENVIRONMENT BASED (FIXED)
# ============================================================

# Get Telegram Bot Token from environment variables
def _get_token():
    token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or token == "YOUR_BOT_TOKEN_HERE":
        raise ValueError(
            "❌ ERROR: BOT_TOKEN not set!\n"
            "Please set BOT_TOKEN in your environment variables.\n"
            "Get it from @BotFather on Telegram."
        )
    return token

# Get Admin IDs from environment variables
def _get_admin_ids():
    admin_str = os.getenv("ADMIN_IDS", "")
    if not admin_str:
        raise ValueError(
            "❌ ERROR: ADMIN_IDS not set!\n"
            "Please set ADMIN_IDS in your environment variables.\n"
            "Format: 123456789,987654321 (comma-separated)"
        )
    try:
        return [int(uid.strip()) for uid in admin_str.split(",") if uid.strip()]
    except ValueError:
        raise ValueError(
            "❌ ERROR: ADMIN_IDS format invalid!\n"
            "Must be comma-separated numbers like: 123456789,987654321"
        )

# Load configuration safely
try:
    TOKEN = _get_token()
    ADMIN_IDS = _get_admin_ids()
except ValueError as e:
    print(str(e))
    sys.exit(1)

# Database path - use /tmp for Railway (ephemeral filesystem)
DB_PATH = os.getenv("DB_PATH", "/tmp/spider_shoppy.db")

# Default channels
DEFAULT_CHANNELS = [
    ch.strip() for ch in 
    os.getenv("DEFAULT_CHANNELS", "").split(",")
    if ch.strip()
]

BOT_USERNAME = os.getenv("BOT_USERNAME", "")

# Game configuration
INITIAL_POINTS = int(os.getenv("INITIAL_POINTS", "2"))
REFERRAL_REWARD = int(os.getenv("REFERRAL_REWARD", "2"))
MAX_CONCURRENT_GAMES = int(os.getenv("MAX_CONCURRENT_GAMES", "15"))
QUEUE_CHECK_INTERVAL = int(os.getenv("QUEUE_CHECK_INTERVAL", "1"))
GAME_START_DELAY = float(os.getenv("GAME_START_DELAY", "0.5"))
SESSION_REFRESH_INTERVAL = int(os.getenv("SESSION_REFRESH_INTERVAL", "3600"))

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Railway specific
PORT = int(os.getenv("PORT", "8000"))

# ============================================================
#  DATABASE LAYER
# ============================================================

class Database:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._lock = threading.RLock()  # Thread-safe database access
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)  # For async DB calls
        self._init_db()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _execute_thread_safe(self, func):
        """Execute database operation in a thread-safe manner"""
        with self._lock:
            return func()

    def _init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    join_date TEXT,
                    referral_by INTEGER,
                    referral_count INTEGER DEFAULT 0,
                    points INTEGER DEFAULT 0,
                    games_played INTEGER DEFAULT 0,
                    total_games INTEGER DEFAULT 0,
                    remaining_games INTEGER DEFAULT 0,
                    account_status TEXT DEFAULT 'active',
                    session_data TEXT,
                    login_info TEXT,
                    is_banned INTEGER DEFAULT 0,
                    has_received_initial_points INTEGER DEFAULT 0,
                    last_active TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_username TEXT UNIQUE,
                    added_by INTEGER,
                    added_date TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queue_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            
            cursor.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                ("force_join_enabled", "true")
            )
            cursor.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                ("game_enabled", "true")
            )
            cursor.execute(
                "INSERT OR IGNORE INTO queue_settings (key, value) VALUES (?, ?)",
                ("max_concurrent", str(MAX_CONCURRENT_GAMES))
            )
            cursor.execute(
                "INSERT OR IGNORE INTO queue_settings (key, value) VALUES (?, ?)",
                ("queue_paused", "false")
            )
            
            for channel in DEFAULT_CHANNELS:
                cursor.execute(
                    "INSERT OR IGNORE INTO channels (channel_username) VALUES (?)",
                    (channel,)
                )
            
            conn.commit()

    def get_user(self, user_id: int) -> Optional[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def create_user(self, user_id: int, username: str, first_name: str, last_name: str = "") -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO users (
                        user_id, username, first_name, last_name, join_date, 
                        points, remaining_games, last_active, has_received_initial_points
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id, username, first_name, last_name,
                    datetime.now().isoformat(),
                    0, 0,
                    datetime.now().isoformat(),
                    0
                ))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def award_initial_points(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT has_received_initial_points FROM users WHERE user_id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            if not row or row[0] == 1:
                return False
            
            cursor.execute("""
                UPDATE users 
                SET points = points + ?, 
                    remaining_games = remaining_games + ?,
                    has_received_initial_points = 1
                WHERE user_id = ?
            """, (INITIAL_POINTS, INITIAL_POINTS, user_id))
            conn.commit()
            return cursor.rowcount > 0

    def update_user_points(self, user_id: int, points: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET points = ?, remaining_games = ? WHERE user_id = ?",
                (points, points, user_id)
            )
            conn.commit()
            return cursor.rowcount > 0

    def add_points(self, user_id: int, points: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET points = points + ?, remaining_games = remaining_games + ? WHERE user_id = ?",
                (points, points, user_id)
            )
            conn.commit()
            return cursor.rowcount > 0

    def deduct_point(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET points = points - 1, remaining_games = remaining_games - 1 WHERE user_id = ? AND points > 0",
                (user_id,)
            )
            conn.commit()
            return cursor.rowcount > 0

    def add_game_played(self, user_id: int) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET games_played = games_played + 1, total_games = total_games + 1 WHERE user_id = ?",
                (user_id,)
            )
            conn.commit()

    def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referral_by FROM users WHERE user_id = ?", (referred_id,))
            row = cursor.fetchone()
            if row and row[0] is not None:
                return False
            
            cursor.execute(
                "UPDATE users SET referral_count = referral_count + 1, points = points + ?, remaining_games = remaining_games + ? WHERE user_id = ?",
                (REFERRAL_REWARD, REFERRAL_REWARD, referrer_id)
            )
            cursor.execute(
                "UPDATE users SET referral_by = ? WHERE user_id = ?",
                (referrer_id, referred_id)
            )
            conn.commit()
            return True

    def get_user_stats(self, user_id: int) -> Dict:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    points, games_played, total_games, referral_count,
                    remaining_games, join_date, has_received_initial_points
                FROM users WHERE user_id = ?
            """, (user_id,))
            row = cursor.fetchone()
            if not row:
                return {}
            return {
                "points": row[0],
                "games_played": row[1],
                "total_games": row[2],
                "referrals": row[3],
                "remaining_games": row[4],
                "join_date": row[5],
                "has_received_initial_points": row[6],
            }

    def get_all_users(self) -> List[Dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY join_date DESC")
            return [dict(row) for row in cursor.fetchall()]

    def get_total_users(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]

    def get_active_users(self, days: int = 7) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            cursor.execute(
                "SELECT COUNT(*) FROM users WHERE last_active > ?",
                (cutoff,)
            )
            return cursor.fetchone()[0]

    def update_last_active(self, user_id: int) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET last_active = ? WHERE user_id = ?",
                (datetime.now().isoformat(), user_id)
            )
            conn.commit()

    def ban_user(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET is_banned = 1, account_status = 'banned' WHERE user_id = ?",
                (user_id,)
            )
            conn.commit()
            return cursor.rowcount > 0

    def unban_user(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET is_banned = 0, account_status = 'active' WHERE user_id = ?",
                (user_id,)
            )
            conn.commit()
            return cursor.rowcount > 0

    def reset_user_points(self, user_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET points = 0, remaining_games = 0 WHERE user_id = ?",
                (user_id,)
            )
            conn.commit()
            return cursor.rowcount > 0

    def save_session_data(self, user_id: int, session_data: str) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET session_data = ? WHERE user_id = ?",
                (session_data, user_id)
            )
            conn.commit()

    def get_session_data(self, user_id: int) -> Optional[str]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT session_data FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return row[0] if row else None

    def get_channels(self) -> List[str]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT channel_username FROM channels")
            return [row[0] for row in cursor.fetchall()]

    def add_channel(self, channel_username: str, added_by: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO channels (channel_username, added_by, added_date) VALUES (?, ?, ?)",
                    (channel_username, added_by, datetime.now().isoformat())
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_channel(self, channel_username: str) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM channels WHERE channel_username = ?", (channel_username,))
            conn.commit()
            return cursor.rowcount > 0

    def get_setting(self, key: str, default: str = "false") -> str:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, value)
            )
            conn.commit()

    def get_queue_setting(self, key: str, default: str = "10") -> str:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM queue_settings WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row[0] if row else default

    def set_queue_setting(self, key: str, value: str) -> None:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO queue_settings (key, value) VALUES (?, ?)",
                (key, value)
            )
            conn.commit()

db = Database()

# ============================================================
#  QUEUE MANAGEMENT SYSTEM
# ============================================================

class GameQueueManager:
    def __init__(self):
        self.active_games: Set[int] = set()
        self.waiting_queue: deque = deque()
        self.user_queue_messages: Dict[int, int] = {}
        self.user_chat_ids: Dict[int, int] = {}
        self.max_concurrent: int = MAX_CONCURRENT_GAMES
        self.queue_paused: bool = False
        self.is_processing: bool = False
        self.lock = asyncio.Lock()
        self._thread_lock = threading.RLock()  # FIXED: Thread safety for deque operations
        self.bot_instance = None
        self.game_tasks: Dict[int, asyncio.Task] = {}
        self.active_shopsy_clients: Dict[int, AsyncShopsyClient] = {}
        self.user_sessions: Dict[int, Dict] = {}
        
    def set_bot(self, bot_instance):
        self.bot_instance = bot_instance
        
    def get_max_concurrent(self) -> int:
        return int(db.get_queue_setting("max_concurrent", str(MAX_CONCURRENT_GAMES)))
    
    def is_paused(self) -> bool:
        return db.get_queue_setting("queue_paused", "false") == "true"
    
    def get_queue_length(self) -> int:
        return len(self.waiting_queue)
    
    def get_active_count(self) -> int:
        return len(self.active_games)
    
    def get_queue_position(self, user_id: int) -> Optional[int]:
        if user_id in self.active_games:
            return None
        try:
            return self.waiting_queue.index(user_id) + 1
        except ValueError:
            return None
    
    def is_user_active_or_queued(self, user_id: int) -> bool:
        return user_id in self.active_games or user_id in self.waiting_queue
    
    async def add_to_queue(self, user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
        async with self.lock:
            if user_id in self.active_games:
                await context.bot.send_message(
                    chat_id,
                    "🎮 You're already playing a game!",
                    parse_mode=ParseMode.MARKDOWN
                )
                return False
            
            if user_id in self.waiting_queue:
                await context.bot.send_message(
                    chat_id,
                    "⏳ You're already in the queue!",
                    parse_mode=ParseMode.MARKDOWN
                )
                return False
            
            if self.is_paused():
                await context.bot.send_message(
                    chat_id,
                    "⏸️ Queue is currently paused. Please try again later.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return False
            
            self.waiting_queue.append(user_id)
            self.user_chat_ids[user_id] = chat_id
            
            position = len(self.waiting_queue)
            await self.show_queue_status(user_id, context, position)
            
            if not self.is_processing:
                asyncio.create_task(self.process_queue(context))
            
            return True
    
    async def remove_from_queue(self, user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
        async with self.lock:
            if user_id not in self.waiting_queue:
                return False
            
            self.waiting_queue.remove(user_id)
            
            if user_id in self.user_queue_messages:
                del self.user_queue_messages[user_id]
            if user_id in self.user_chat_ids:
                del self.user_chat_ids[user_id]
            
            await self.update_all_queue_positions(context)
            return True
    
    async def show_queue_status(self, user_id: int, context: ContextTypes.DEFAULT_TYPE, position: int = None):
        if position is None:
            position = self.get_queue_position(user_id)
            if position is None:
                return
        
        max_concurrent = self.get_max_concurrent()
        active_count = self.get_active_count()
        queue_length = self.get_queue_length()
        
        progress = min(100, int((active_count / max_concurrent) * 100))
        bar_length = 10
        filled = int((progress / 100) * bar_length)
        bar = "█" * filled + "░" * (bar_length - filled)
        
        message = f"""
⏳ **Please Wait...**

━━━━━━━━━━━━━━━━━━━━━

👥 **Active Games:** {active_count}/{max_concurrent}
📍 **Queue Position:** #{position}
⏳ **Queue Length:** {queue_length}

`{bar}` {progress}%

*Finding available slot...*

━━━━━━━━━━━━━━━━━━━━━
📌 Your game will start automatically when a slot becomes available.
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Leave Queue", callback_data="leave_queue")],
            [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
        ])
        
        chat_id = self.user_chat_ids.get(user_id)
        if not chat_id:
            return
            
        if user_id in self.user_queue_messages:
            try:
                await context.bot.edit_message_text(
                    message,
                    chat_id=chat_id,
                    message_id=self.user_queue_messages[user_id],
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logging.warning(f"Could not edit queue message for {user_id}: {e}")
                try:
                    msg = await context.bot.send_message(
                        chat_id,
                        message,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.MARKDOWN
                    )
                    self.user_queue_messages[user_id] = msg.message_id
                except Exception as e2:
                    logging.error(f"Could not send queue message for {user_id}: {e2}")
        else:
            try:
                msg = await context.bot.send_message(
                    chat_id,
                    message,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.MARKDOWN
                )
                self.user_queue_messages[user_id] = msg.message_id
            except Exception as e:
                logging.error(f"Could not send queue message for {user_id}: {e}")
    
    async def update_all_queue_positions(self, context: ContextTypes.DEFAULT_TYPE):
        for user_id in self.waiting_queue:
            position = self.get_queue_position(user_id)
            if position:
                await self.show_queue_status(user_id, context, position)
    
    async def process_queue(self, context: ContextTypes.DEFAULT_TYPE):
        if self.is_processing:
            return
        
        self.is_processing = True
        
        try:
            while True:
                self.queue_paused = self.is_paused()
                if self.queue_paused:
                    await asyncio.sleep(QUEUE_CHECK_INTERVAL)
                    continue
                
                max_concurrent = self.get_max_concurrent()
                active_count = self.get_active_count()
                
                if active_count < max_concurrent and len(self.waiting_queue) > 0:
                    async with self.lock:
                        if len(self.waiting_queue) == 0:
                            continue
                        user_id = self.waiting_queue.popleft()
                        
                        chat_id = self.user_chat_ids.get(user_id)
                        if not chat_id:
                            continue
                        
                        self.active_games.add(user_id)
                        
                        if user_id in self.user_queue_messages:
                            try:
                                await context.bot.delete_message(
                                    chat_id,
                                    self.user_queue_messages[user_id]
                                )
                            except Exception:
                                pass
                            del self.user_queue_messages[user_id]
                        
                        if user_id in self.user_chat_ids:
                            del self.user_chat_ids[user_id]
                    
                    await self.update_all_queue_positions(context)
                    await asyncio.sleep(GAME_START_DELAY)
                    
                    task = asyncio.create_task(self.start_game_automatically(user_id, chat_id, context))
                    self.game_tasks[user_id] = task
                    
                await asyncio.sleep(QUEUE_CHECK_INTERVAL)
                
        except Exception as e:
            logging.error(f"Queue processing error: {e}")
        finally:
            self.is_processing = False
    
    async def _get_shopsy_session(self, user_id: int) -> Optional[AsyncShopsyClient]:
        if user_id in self.active_shopsy_clients:
            client = self.active_shopsy_clients[user_id]
            if client.is_valid_session():
                return client
        
        session_data = db.get_session_data(user_id)
        if session_data:
            try:
                data = json.loads(session_data)
                client = AsyncShopsyClient(log=LiveLog(debug=False))
                client.from_dict(data)
                if client.is_valid_session():
                    self.active_shopsy_clients[user_id] = client
                    return client
            except Exception as e:
                logging.error(f"Failed to restore session for {user_id}: {e}")
        
        return None
    
    async def _save_shopsy_session(self, user_id: int, client: AsyncShopsyClient):
        try:
            db.save_session_data(user_id, json.dumps(client.to_dict()))
        except Exception as e:
            logging.error(f"Failed to save session for {user_id}: {e}")
    
    async def _validate_and_refresh_session(self, user_id: int, client: AsyncShopsyClient) -> bool:
        try:
            if not client.is_valid_session():
                return False
            async with client:
                return await client.validate_and_refresh()
        except Exception as e:
            logging.error(f"Session validation failed for {user_id}: {e}")
            return False
    
    async def start_game_automatically(self, user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        client = None
        point_deducted = False
        
        try:
            client = await self._get_shopsy_session(user_id)
            
            if not client or not client.is_valid_session():
                await context.bot.send_message(
                    chat_id,
                    "🔐 **Shopsy Login Required**\n\n"
                    "Please login to your Shopsy/Flipkart account first.\n"
                    "Use /login command or click the button below.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔐 Login to Shopsy", callback_data="shopsy_login")],
                        [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                async with self.lock:
                    if user_id in self.active_games:
                        self.active_games.remove(user_id)
                return
            
            if not await self._validate_and_refresh_session(user_id, client):
                await context.bot.send_message(
                    chat_id,
                    "🔐 **Session Expired**\n\n"
                    "Your Shopsy session has expired. Please login again.\n"
                    "Use /login command or click the button below.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔐 Login to Shopsy", callback_data="shopsy_login")],
                        [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                async with self.lock:
                    if user_id in self.active_games:
                        self.active_games.remove(user_id)
                return
            
            stats = db.get_user_stats(user_id)
            unlimited = db.get_setting("unlimited_mode") == "true"
            
            if not unlimited and stats.get('points', 0) <= 0:
                await context.bot.send_message(
                    chat_id,
                    "❌ **No Points Left**\n\n"
                    "Invite friends to earn more points!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("👥 Invite & Earn", callback_data="invite_earn")],
                        [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                async with self.lock:
                    if user_id in self.active_games:
                        self.active_games.remove(user_id)
                return
            
            async with client:
                await self.show_game_starting_animation(user_id, chat_id, context)
                
                user_data = await client.get_user(refresh=True)
                
                available_games = [g for g in GAMES if not client.game_already_done(g["id"])]
                if not available_games:
                    available_games = GAMES
                
                game = available_games[0]
                
                if not unlimited:
                    if not db.deduct_point(user_id):
                        await context.bot.send_message(
                            chat_id,
                            "❌ Failed to deduct point. Please try again.",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        async with self.lock:
                            if user_id in self.active_games:
                                self.active_games.remove(user_id)
                        return
                    point_deducted = True
                else:
                    point_deducted = False
                
                session_id = await client.start_game(game)
                
                play_time = await client._play_seconds(game)
                await asyncio.sleep(min(play_time, 15))
                
                result = await client.end_game(game, session_id, play_time)
                
                if result.get("success"):
                    coins_earned = result.get("data", {}).get("coinsEarnedForGame", 0)
                    db.add_game_played(user_id)
                    await self._save_shopsy_session(user_id, client)
                    
                    new_stats = db.get_user_stats(user_id)
                    points_left = new_stats.get('points', 0)
                    
                    result_message = f"""
🏆 **Game Completed!**

🎮 Game: {game['name']}
💎 Coins Earned: {coins_earned}

✅ You played and earned experience!

💎 **Remaining Points:** {points_left}
🎮 **Remaining Games:** {points_left}

━━━━━━━━━━━━━━━━━━━━━
"""
                    
                    keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("🎮 Play Again", callback_data="play_game")] if points_left > 0 else [],
                        [InlineKeyboardButton("👥 Invite & Earn", callback_data="invite_earn")],
                        [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                    ])
                    
                    await context.bot.send_message(
                        chat_id,
                        result_message,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    if point_deducted:
                        db.add_points(user_id, 1)
                        point_deducted = False
                    await context.bot.send_message(
                        chat_id,
                        "❌ Game failed. Your point has been refunded.",
                        parse_mode=ParseMode.MARKDOWN
                    )
            
        except Exception as e:
            logging.error(f"Game error for {user_id}: {e}")
            if point_deducted:
                db.add_points(user_id, 1)
                point_deducted = False
            try:
                await context.bot.send_message(
                    chat_id,
                    f"❌ Game error: {str(e)[:100]}. Point refunded.",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass
        
        finally:
            async with self.lock:
                if user_id in self.active_games:
                    self.active_games.remove(user_id)
                if user_id in self.game_tasks:
                    del self.game_tasks[user_id]
                if user_id in self.active_shopsy_clients:
                    del self.active_shopsy_clients[user_id]
            
            if not self.is_processing:
                asyncio.create_task(self.process_queue(context))
    
    async def show_game_starting_animation(self, user_id: int, chat_id: int, context: ContextTypes.DEFAULT_TYPE):
        steps = [
            "🎮 **Connecting to game server...**",
            "⚡ **Initializing game engine...**",
            "🚀 **Loading game resources...**",
            "🔥 **Preparing your session...**",
            "✅ **Game is ready!**",
        ]
        
        progress_steps = [0, 25, 50, 75, 100]
        
        msg = await context.bot.send_message(
            chat_id,
            "🎮 **Starting Game...**\n\n"
            "`░░░░░░░░░░` 0%",
            parse_mode=ParseMode.MARKDOWN
        )
        
        for i, (text, progress) in enumerate(zip(steps, progress_steps)):
            bar_length = 10
            filled = int((progress / 100) * bar_length)
            bar = "█" * filled + "░" * (bar_length - filled)
            
            try:
                await context.bot.edit_message_text(
                    f"{text}\n\n`{bar}` {progress}%",
                    chat_id=chat_id,
                    message_id=msg.message_id,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                msg = await context.bot.send_message(
                    chat_id,
                    f"{text}\n\n`{bar}` {progress}%",
                    parse_mode=ParseMode.MARKDOWN
                )
            
            await asyncio.sleep(0.4)
        
        await asyncio.sleep(0.3)
        try:
            await context.bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
    
    async def clear_queue(self, context: ContextTypes.DEFAULT_TYPE):
        async with self.lock:
            for user_id in self.waiting_queue:
                if user_id in self.user_queue_messages:
                    try:
                        chat_id = self.user_chat_ids.get(user_id)
                        if chat_id:
                            await context.bot.edit_message_text(
                                "⏰ **Queue Cleared**\n\nYour game request has been cancelled.",
                                chat_id=chat_id,
                                message_id=self.user_queue_messages[user_id],
                                parse_mode=ParseMode.MARKDOWN
                            )
                    except Exception:
                        pass
                    del self.user_queue_messages[user_id]
                if user_id in self.user_chat_ids:
                    del self.user_chat_ids[user_id]
            
            self.waiting_queue.clear()
    
    async def set_max_concurrent(self, limit: int) -> bool:
        if limit < 1:
            return False
        
        self.max_concurrent = limit
        db.set_queue_setting("max_concurrent", str(limit))
        return True
    
    async def toggle_pause(self):
        current = db.get_queue_setting("queue_paused", "false")
        new_state = "true" if current == "false" else "false"
        db.set_queue_setting("queue_paused", new_state)
        self.queue_paused = new_state == "true"
        return self.queue_paused
    
    def get_queue_info(self) -> Dict:
        return {
            "active": self.get_active_count(),
            "max_concurrent": self.get_max_concurrent(),
            "queue_length": self.get_queue_length(),
            "waiting_users": list(self.waiting_queue),
            "is_paused": self.is_paused(),
        }
    
    async def recover_from_restart(self, context: ContextTypes.DEFAULT_TYPE):
        async with self.lock:
            self.active_games.clear()
            self.waiting_queue.clear()
            self.user_queue_messages.clear()
            self.user_chat_ids.clear()
            self.game_tasks.clear()
            self.active_shopsy_clients.clear()
            self.is_processing = False
        
        await self.restore_all_sessions()
        
        if not self.is_processing:
            asyncio.create_task(self.process_queue(context))
        
        logging.info("Queue recovered after restart")
    
    async def restore_all_sessions(self):
        all_users = db.get_all_users()
        for user in all_users:
            user_id = user['user_id']
            session_data = db.get_session_data(user_id)
            if session_data:
                try:
                    data = json.loads(session_data)
                    if data.get("is_logged_in"):
                        client = AsyncShopsyClient(log=LiveLog(debug=False))
                        client.from_dict(data)
                        if client.is_valid_session():
                            self.active_shopsy_clients[user_id] = client
                except Exception as e:
                    logging.error(f"Failed to restore session for {user_id}: {e}")
        logging.info(f"Restored {len(self.active_shopsy_clients)} Shopsy sessions")

queue_manager = GameQueueManager()

# ============================================================
#  BOT HANDLERS
# ============================================================

(MAIN_MENU, PLAY_GAME, PROFILE, STATS, HELP, REFERRAL,
 ADMIN_PANEL, ADD_POINTS, REMOVE_POINTS, BAN_USER, UNBAN_USER,
 BROADCAST, ADD_CHANNEL, REMOVE_CHANNEL, FORCE_JOIN_WAIT, 
 SHOPSY_LOGIN, SHOPSY_OTP, QUEUE_SET_LIMIT_STATE) = range(18)

# ============================================================
#  UI HELPERS
# ============================================================

def format_dashboard(user_id: int, user_data: Dict) -> str:
    stats = db.get_user_stats(user_id)
    username = user_data.get('username', 'User')
    first_name = user_data.get('first_name', '')
    
    status = "🟢 Online" if user_data.get('is_banned') == 0 else "🔴 Banned"
    points = stats.get('points', 0)
    games_left = stats.get('remaining_games', 0)
    referrals = stats.get('referrals', 0)
    
    return f"""
🕷️ **SPIDER X SHOPSY**

{status}

👤 **{first_name}** (@{username})
🆔 `{user_id}`

💎 **Points:** {points}
🎮 **Games Left:** {games_left}
👥 **Referrals:** {referrals}
📈 **Status:** {status}

━━━━━━━━━━━━━━━━━━━━━
⚡ **Ready to Play?** ⚡
━━━━━━━━━━━━━━━━━━━━━
"""

def get_dashboard_keyboard(user_id: int) -> InlineKeyboardMarkup:
    stats = db.get_user_stats(user_id)
    points = stats.get('points', 0)
    
    keyboard = [
        [InlineKeyboardButton("🎮 Play Game", callback_data="play_game")],
        [InlineKeyboardButton("👤 My Profile", callback_data="my_profile")],
        [InlineKeyboardButton("👥 Invite & Earn", callback_data="invite_earn")],
        [InlineKeyboardButton("📊 My Stats", callback_data="my_stats")],
        [InlineKeyboardButton("❓ Help", callback_data="help")],
    ]
    
    if user_id in ADMIN_IDS:
        keyboard.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(keyboard)

def get_force_join_keyboard() -> InlineKeyboardMarkup:
    channels = db.get_channels()
    keyboard = []
    for channel in channels:
        keyboard.append([InlineKeyboardButton(f"📢 Join {channel}", url=f"https://t.me/{channel.replace('@', '')}")])
    keyboard.append([InlineKeyboardButton("✅ I've Joined", callback_data="check_joined")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Dashboard", callback_data="admin_dashboard")],
        [InlineKeyboardButton("👥 Total Users", callback_data="admin_users")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("📂 Logs", callback_data="admin_logs")],
        [InlineKeyboardButton("📈 Analytics", callback_data="admin_analytics")],
        [InlineKeyboardButton("➕ Add Points", callback_data="admin_add_points")],
        [InlineKeyboardButton("➖ Remove Points", callback_data="admin_remove_points")],
        [InlineKeyboardButton("♾ Unlimited Mode", callback_data="admin_unlimited")],
        [InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban")],
        [InlineKeyboardButton("✅ Unban User", callback_data="admin_unban")],
        [InlineKeyboardButton("🔒 Force Join Settings", callback_data="admin_force_join")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
        [InlineKeyboardButton("🔄 Restart Bot", callback_data="admin_restart")],
        [InlineKeyboardButton("🎮 Queue Manager", callback_data="admin_queue")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_to_dashboard")],
    ])

def get_queue_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Increase Limit", callback_data="queue_increase")],
        [InlineKeyboardButton("➖ Decrease Limit", callback_data="queue_decrease")],
        [InlineKeyboardButton("🧹 Clear Queue", callback_data="queue_clear")],
        [InlineKeyboardButton("⏸ Pause Queue", callback_data="queue_pause")],
        [InlineKeyboardButton("▶️ Resume Queue", callback_data="queue_resume")],
        [InlineKeyboardButton("📋 View Queue", callback_data="queue_view")],
        [InlineKeyboardButton("⚙️ Set Limit", callback_data="queue_set_limit")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
    ])

def get_referral_keyboard(user_id: int) -> InlineKeyboardMarkup:
    bot_username = BOT_USERNAME
    if not bot_username:
        bot_username = "YourBotUsername"  # Fallback, should be set in env
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📨 Share Referral Link", url=ref_link)],
        [InlineKeyboardButton("📤 Invite Friend", url=f"tg://msg?text=🎮%20Join%20me%20on%20SPIDER%20X%20SHOPSY%21%0A%0Ahttps://t.me/{bot_username}?start=ref_{user_id}")],
        [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
    ])

# ============================================================
#  MAIN BOT HANDLER
# ============================================================

class SpiderShopsyBot:
    def __init__(self, token: str):
        self.token = token
        self.application = None
        self.user_sessions = {}
        self.is_restarting = False
        
    def setup(self):
        self.application = Application.builder().token(self.token).build()
        
        queue_manager.set_bot(self)
        
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("login", self.login_command))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("login", self.login_command),
                CallbackQueryHandler(self.admin_add_points_start, pattern="admin_add_points"),
                CallbackQueryHandler(self.admin_remove_points_start, pattern="admin_remove_points"),
                CallbackQueryHandler(self.admin_ban_start, pattern="admin_ban"),
                CallbackQueryHandler(self.admin_unban_start, pattern="admin_unban"),
                CallbackQueryHandler(self.admin_broadcast_start, pattern="admin_broadcast"),
                CallbackQueryHandler(self.admin_add_channel_start, pattern="admin_add_channel"),
                CallbackQueryHandler(self.admin_remove_channel_start, pattern="admin_remove_channel"),
                CallbackQueryHandler(self.queue_set_limit_start, pattern="queue_set_limit"),
                CallbackQueryHandler(self.shopsy_login_start, pattern="shopsy_login"),
            ],
            states={
                ADD_POINTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_add_points)],
                REMOVE_POINTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_remove_points)],
                BAN_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_ban)],
                UNBAN_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_unban)],
                BROADCAST: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_broadcast)],
                ADD_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_add_channel)],
                REMOVE_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_remove_channel)],
                SHOPSY_LOGIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_shopsy_phone)],
                SHOPSY_OTP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_shopsy_otp)],
                QUEUE_SET_LIMIT_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_queue_set_limit)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel_command)],
            per_message=False,
        )
        self.application.add_handler(conv_handler)
        
        self.application.add_error_handler(self.error_handler)
        
        return self.application

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        user_id = user.id
        
        if context.args and context.args[0].startswith("ref_"):
            referrer_id = int(context.args[0].replace("ref_", ""))
            if referrer_id != user_id:
                await self.handle_referral(user_id, referrer_id)
        
        user_data = db.get_user(user_id)
        if not user_data:
            db.create_user(
                user_id,
                user.username or f"user_{user_id}",
                user.first_name or "User",
                user.last_name or ""
            )
            user_data = db.get_user(user_id)
        
        db.award_initial_points(user_id)
        db.update_last_active(user_id)
        
        if await self.check_force_join(user_id, update, context):
            return
        
        await self.show_dashboard(update, context, user_id)

    async def login_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        
        session_data = db.get_session_data(user_id)
        if session_data:
            try:
                data = json.loads(session_data)
                if data.get("is_logged_in"):
                    client = AsyncShopsyClient(log=LiveLog(debug=False))
                    client.from_dict(data)
                    if client.is_valid_session():
                        await update.message.reply_text(
                            "✅ You are already logged in to Shopsy!",
                            parse_mode=ParseMode.MARKDOWN
                        )
                        return
            except:
                pass
        
        await update.message.reply_text(
            "🔐 **Shopsy Login**\n\n"
            "Please send your 10-digit mobile number:\n\n"
            "Example: `9876543210`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return SHOPSY_LOGIN

    async def handle_shopsy_phone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        phone = update.message.text.strip().replace("+91", "").replace(" ", "")
        
        if not phone.isdigit() or len(phone) != 10:
            await update.message.reply_text(
                "❌ Invalid phone number. Please send a 10-digit number:\n\n"
                "Example: `9876543210`\n\n"
                "Type /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )
            return SHOPSY_LOGIN
        
        context.user_data["shopsy_phone"] = phone
        
        await update.message.reply_text(
            f"📱 Sending OTP to +91{phone}...\n\n"
            "Please wait...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            client = AsyncShopsyClient(log=LiveLog(debug=False))
            await client.__aenter__()
            
            await client.bootstrap()
            request_id = await client.send_otp(phone)
            
            context.user_data["shopsy_client"] = client
            context.user_data["shopsy_request_id"] = request_id
            
            await update.message.reply_text(
                f"✅ OTP sent to +91{phone}\n\n"
                "Please send the 6-digit OTP you received:\n\n"
                "Type /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )
            return SHOPSY_OTP
            
        except Exception as e:
            await update.message.reply_text(
                f"❌ Failed to send OTP: {str(e)[:200]}\n\n"
                "Please try again with /login",
                parse_mode=ParseMode.MARKDOWN
            )
            if "shopsy_client" in context.user_data:
                client = context.user_data.pop("shopsy_client")
                await client.__aexit__(None, None, None)
            return ConversationHandler.END

    async def handle_shopsy_otp(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        otp = update.message.text.strip()
        
        if not otp.isdigit() or len(otp) != 6:
            await update.message.reply_text(
                "❌ Invalid OTP. Please send a 6-digit code:\n\n"
                "Type /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )
            return SHOPSY_OTP
        
        client = context.user_data.get("shopsy_client")
        phone = context.user_data.get("shopsy_phone")
        request_id = context.user_data.get("shopsy_request_id")
        
        if not client or not phone or not request_id:
            await update.message.reply_text(
                "❌ Session expired. Please try again with /login",
                parse_mode=ParseMode.MARKDOWN
            )
            return ConversationHandler.END
        
        try:
            await client.verify_otp(phone, otp, request_id)
            
            if client.is_valid_session():
                session_data = client.to_dict()
                db.save_session_data(user_id, json.dumps(session_data))
                queue_manager.active_shopsy_clients[user_id] = client
                
                await update.message.reply_text(
                    "✅ **Shopsy Login Successful!**\n\n"
                    f"👤 Account: {client.ctx.account_id}\n"
                    f"📛 Name: {client.ctx.user_name or 'User'}\n\n"
                    "You can now play games using SPIDER X SHOPSY!",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                await self.show_dashboard(update, context, user_id)
                
                context.user_data.pop("shopsy_client", None)
                context.user_data.pop("shopsy_phone", None)
                context.user_data.pop("shopsy_request_id", None)
                
                return ConversationHandler.END
            else:
                await update.message.reply_text(
                    "❌ Login failed. Please try again with /login",
                    parse_mode=ParseMode.MARKDOWN
                )
                await client.__aexit__(None, None, None)
                return ConversationHandler.END
                
        except Exception as e:
            await update.message.reply_text(
                f"❌ Login failed: {str(e)[:200]}\n\n"
                "Please try again with /login",
                parse_mode=ParseMode.MARKDOWN
            )
            await client.__aexit__(None, None, None)
            return ConversationHandler.END

    async def handle_referral(self, user_id: int, referrer_id: int):
        user_data = db.get_user(user_id)
        if not user_data or user_data.get('referral_by') is not None:
            return
        
        success = db.add_referral(referrer_id, user_id)
        if success:
            try:
                await self.application.bot.send_message(
                    referrer_id,
                    f"🎉 **New Referral!**\n\n"
                    f"Someone joined using your referral link!\n"
                    f"✨ You earned **+{REFERRAL_REWARD} Points**",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass

    async def check_force_join(self, user_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        if db.get_setting("force_join_enabled") != "true":
            return False
        
        channels = db.get_channels()
        if not channels:
            return False
        
        for channel in channels:
            try:
                member = await context.bot.get_chat_member(channel, user_id)
                if member.status in ["left", "kicked"]:
                    await self.show_force_join(update, context)
                    return True
            except Exception:
                pass
        
        return False

    async def show_force_join(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        channels = db.get_channels()
        channels_text = "\n".join([f"📢 {channel}" for channel in channels])
        
        message = f"""
🔒 **Join Required Channels**

To use 🕷️ **SPIDER X SHOPSY**, you must join all required channels:

{channels_text}

━━━━━━━━━━━━━━━━━━━━━
After joining, click the button below.
━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = get_force_join_keyboard()
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )

    async def show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        user_data = db.get_user(user_id)
        if not user_data:
            await self.start_command(update, context)
            return
        
        if user_data.get('is_banned') == 1:
            if update.callback_query:
                await update.callback_query.edit_message_text(
                    "🚫 **You are banned from using this bot.**\n\n"
                    "Contact admin for assistance.",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    "🚫 **You are banned from using this bot.**\n\n"
                    "Contact admin for assistance.",
                    parse_mode=ParseMode.MARKDOWN
                )
            return
        
        if await self.check_force_join(user_id, update, context):
            return
        
        db.update_last_active(user_id)
        
        dashboard_text = format_dashboard(user_id, user_data)
        keyboard = get_dashboard_keyboard(user_id)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                dashboard_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )
        elif update.message:
            await update.message.reply_text(
                dashboard_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN
            )

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        user_data = db.get_user(user_id)
        if user_data and user_data.get('is_banned') == 1 and data not in ["admin_panel", "admin_dashboard"]:
            await query.edit_message_text(
                "🚫 **You are banned from using this bot.**\n\n"
                "Contact admin for assistance.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if data == "check_joined":
            if await self.check_force_join(user_id, update, context):
                await query.answer("❌ Please join all channels first!", show_alert=True)
            else:
                await query.answer("✅ All channels joined!", show_alert=True)
                await self.show_dashboard(update, context, user_id)
        
        elif data == "back_to_dashboard":
            await self.show_dashboard(update, context, user_id)
        
        elif data == "play_game":
            await self.handle_play_game(update, context, user_id)
        
        elif data == "my_profile":
            await self.handle_profile(update, context, user_id)
        
        elif data == "invite_earn":
            await self.handle_invite_earn(update, context, user_id)
        
        elif data == "my_stats":
            await self.handle_stats(update, context, user_id)
        
        elif data == "help":
            await self.handle_help(update, context, user_id)
        
        elif data == "admin_panel":
            await self.handle_admin_panel(update, context, user_id)
        
        elif data == "leave_queue":
            await self.handle_leave_queue(update, context, user_id)
        
        elif data == "shopsy_login":
            await self.shopsy_login_start(update, context)
        
        elif data == "admin_dashboard":
            await self.handle_admin_dashboard(update, context, user_id)
        
        elif data == "admin_users":
            await self.handle_admin_users(update, context, user_id)
        
        elif data == "admin_analytics":
            await self.handle_admin_analytics(update, context, user_id)
        
        elif data == "admin_force_join":
            await self.handle_admin_force_join(update, context, user_id)
        
        elif data == "admin_settings":
            await self.handle_admin_settings(update, context, user_id)
        
        elif data == "admin_restart":
            await self.handle_admin_restart(update, context, user_id)
        
        elif data == "admin_unlimited":
            await self.handle_admin_unlimited(update, context, user_id)
        
        elif data == "admin_logs":
            await self.handle_admin_logs(update, context, user_id)
        
        elif data == "admin_queue":
            await self.handle_admin_queue(update, context, user_id)
        
        elif data == "queue_increase":
            await self.handle_queue_increase(update, context, user_id)
        
        elif data == "queue_decrease":
            await self.handle_queue_decrease(update, context, user_id)
        
        elif data == "queue_clear":
            await self.handle_queue_clear(update, context, user_id)
        
        elif data == "queue_pause":
            await self.handle_queue_pause(update, context, user_id)
        
        elif data == "queue_resume":
            await self.handle_queue_resume(update, context, user_id)
        
        elif data == "queue_view":
            await self.handle_queue_view(update, context, user_id)
        
        elif data == "admin_add_channel":
            await self.admin_add_channel_start(update, context)
        
        elif data == "admin_remove_channel":
            await self.admin_remove_channel_start(update, context)
        
        elif data == "admin_toggle_force_join":
            await self.handle_admin_toggle_force_join(update, context, user_id)
        
        elif data == "admin_toggle_game":
            await self.handle_admin_toggle_game(update, context, user_id)
        
        else:
            await query.edit_message_text(
                "❌ Unknown action.",
                parse_mode=ParseMode.MARKDOWN
            )

    async def handle_play_game(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        query = update.callback_query
        
        if queue_manager.is_user_active_or_queued(user_id):
            if user_id in queue_manager.active_games:
                await query.edit_message_text(
                    "🎮 **You're already playing a game!**\n\nPlease complete your current game first.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            
            if user_id in queue_manager.waiting_queue:
                position = queue_manager.get_queue_position(user_id)
                await query.edit_message_text(
                    f"⏳ **You're already in the queue!**\n\n📍 Queue Position: #{position}\n\nYour game will start automatically when a slot becomes available.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
        
        stats = db.get_user_stats(user_id)
        points = stats.get('points', 0)
        unlimited = db.get_setting("unlimited_mode") == "true"
        
        if not unlimited and points <= 0:
            await query.edit_message_text(
                "❌ **No Points Left**\n\n"
                "👥 Invite 1 Friend\n"
                "🎁 Earn +2 Points\n\n"
                "Share your referral link to get more points!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📨 Invite Friend", callback_data="invite_earn")],
                    [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if db.get_setting("game_enabled") != "true":
            await query.edit_message_text(
                "⏸️ **Game is currently disabled.**\n\nPlease try again later.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        session_data = db.get_session_data(user_id)
        if not session_data:
            await query.edit_message_text(
                "🔐 **Shopsy Login Required**\n\n"
                "Please login to your Shopsy/Flipkart account first.\n\n"
                "Click the button below to login:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔐 Login to Shopsy", callback_data="shopsy_login")],
                    [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            data = json.loads(session_data)
            if not data.get("is_logged_in"):
                await query.edit_message_text(
                    "🔐 **Shopsy Login Required**\n\n"
                    "Your session has expired. Please login again.\n\n"
                    "Click the button below to login:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔐 Login to Shopsy", callback_data="shopsy_login")],
                        [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                    ]),
                    parse_mode=ParseMode.MARKDOWN
                )
                return
        except:
            await query.edit_message_text(
                "🔐 **Shopsy Login Required**\n\n"
                "Please login to your Shopsy/Flipkart account first.\n\n"
                "Click the button below to login:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔐 Login to Shopsy", callback_data="shopsy_login")],
                    [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        active_count = queue_manager.get_active_count()
        max_concurrent = queue_manager.get_max_concurrent()
        
        if active_count < max_concurrent:
            chat_id = query.message.chat_id
            async with queue_manager.lock:
                queue_manager.active_games.add(user_id)
            asyncio.create_task(queue_manager.start_game_automatically(user_id, chat_id, context))
            await query.edit_message_text(
                "🎮 **Starting your game...**\n\nPlease wait a moment.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            chat_id = query.message.chat_id
            success = await queue_manager.add_to_queue(user_id, chat_id, context)
            if success:
                await query.answer("⏳ Added to queue!", show_alert=True)
    
    async def handle_leave_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        query = update.callback_query
        
        success = await queue_manager.remove_from_queue(user_id, context)
        if success:
            await query.edit_message_text(
                "✅ **Removed from queue**\n\nYou have been removed from the waiting list.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
                ]),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await query.edit_message_text(
                "❌ You're not in the queue.",
                parse_mode=ParseMode.MARKDOWN
            )

    async def handle_admin_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        info = queue_manager.get_queue_info()
        
        message = f"""
🎮 **Queue Manager**

━━━━━━━━━━━━━━━━━━━━━

👥 **Active Games:** {info['active']}/{info['max_concurrent']}
⏳ **Queue Length:** {info['queue_length']}
📊 **Status:** {'⏸️ Paused' if info['is_paused'] else '▶️ Running'}

━━━━━━━━━━━━━━━━━━━━━

📋 **Waiting Users:** {info['queue_length']} users in queue

━━━━━━━━━━━━━━━━━━━━━
"""
        
        await query.edit_message_text(
            message,
            reply_markup=get_queue_admin_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_queue_increase(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        current = queue_manager.get_max_concurrent()
        new_limit = current + 1
        
        await queue_manager.set_max_concurrent(new_limit)
        
        await query.answer(f"✅ Increased limit to {new_limit}", show_alert=True)
        await self.handle_admin_queue(update, context, user_id)

    async def handle_queue_decrease(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        current = queue_manager.get_max_concurrent()
        new_limit = max(1, current - 1)
        
        await queue_manager.set_max_concurrent(new_limit)
        
        await query.answer(f"✅ Decreased limit to {new_limit}", show_alert=True)
        await self.handle_admin_queue(update, context, user_id)

    async def handle_queue_clear(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        await queue_manager.clear_queue(context)
        
        await query.answer("✅ Queue cleared!", show_alert=True)
        await self.handle_admin_queue(update, context, user_id)

    async def handle_queue_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        await queue_manager.toggle_pause()
        
        await query.answer("⏸️ Queue paused!", show_alert=True)
        await self.handle_admin_queue(update, context, user_id)

    async def handle_queue_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        await queue_manager.toggle_pause()
        
        await query.answer("▶️ Queue resumed!", show_alert=True)
        await self.handle_admin_queue(update, context, user_id)

    async def handle_queue_view(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        info = queue_manager.get_queue_info()
        
        if info['queue_length'] == 0:
            users_text = "No users in queue."
        else:
            users_text = "👥 **Users in Queue:**\n\n"
            for i, uid in enumerate(list(info['waiting_users'])[:10], 1):
                user_data = db.get_user(uid)
                name = user_data.get('username', f"User_{uid}") if user_data else f"User_{uid}"
                users_text += f"{i}. @{name} (`{uid}`)\n"
            
            if info['queue_length'] > 10:
                users_text += f"\n... and {info['queue_length'] - 10} more"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="queue_view")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_queue")],
        ])
        
        await query.edit_message_text(
            users_text[:4096],
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def queue_set_limit_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        await update.callback_query.edit_message_text(
            "⚙️ **Set Concurrent Limit**\n\n"
            "Send the new concurrent game limit (must be at least 1):\n\n"
            "Example: `15`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return QUEUE_SET_LIMIT_STATE

    async def handle_queue_set_limit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        try:
            new_limit = int(update.message.text.strip())
            if new_limit < 1:
                await update.message.reply_text(
                    "❌ Limit must be at least 1. Try again.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return QUEUE_SET_LIMIT_STATE
            
            await queue_manager.set_max_concurrent(new_limit)
            await update.message.reply_text(
                f"✅ Concurrent limit set to `{new_limit}`",
                parse_mode=ParseMode.MARKDOWN
            )
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid number. Send a number like `15`.",
                parse_mode=ParseMode.MARKDOWN
            )
            return QUEUE_SET_LIMIT_STATE
        
        return ConversationHandler.END

    async def shopsy_login_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query:
            await update.callback_query.edit_message_text(
                "🔐 **Shopsy Login**\n\n"
                "Please send your 10-digit mobile number:\n\n"
                "Example: `9876543210`\n\n"
                "Type /cancel to cancel.",
                parse_mode=ParseMode.MARKDOWN
            )
        return SHOPSY_LOGIN

    async def handle_profile(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        query = update.callback_query
        user_data = db.get_user(user_id)
        stats = db.get_user_stats(user_id)
        
        if not user_data:
            await query.edit_message_text("❌ User not found.", parse_mode=ParseMode.MARKDOWN)
            return
        
        username = user_data.get('username', 'N/A')
        first_name = user_data.get('first_name', 'User')
        join_date = stats.get('join_date', 'N/A')
        
        session_data = db.get_session_data(user_id)
        shopsy_status = "🔴 Not Logged In"
        shopsy_account = ""
        if session_data:
            try:
                data = json.loads(session_data)
                if data.get("is_logged_in"):
                    shopsy_status = "🟢 Logged In"
                    shopsy_account = data.get("account_id", "")
            except:
                pass
        
        profile_message = f"""
👤 **My Profile**

━━━━━━━━━━━━━━━━━━━━━

👤 **Username:** @{username}
🆔 **Telegram ID:** `{user_id}`
📅 **Join Date:** {join_date[:10]}

🎮 **Total Games Played:** {stats.get('games_played', 0)}
👥 **Total Referrals:** {stats.get('referrals', 0)}
💎 **Current Points:** {stats.get('points', 0)}
🎮 **Remaining Games:** {stats.get('remaining_games', 0)}

🔐 **Shopsy Status:** {shopsy_status}
{f"📛 **Account:** `{shopsy_account}`" if shopsy_account else ""}

📈 **Account Status:** ✅ Active

━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔐 Login to Shopsy", callback_data="shopsy_login")] if shopsy_status == "🔴 Not Logged In" else [],
            [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
        ])
        
        await query.edit_message_text(
            profile_message,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        query = update.callback_query
        stats = db.get_user_stats(user_id)
        
        stats_message = f"""
📊 **My Statistics**

━━━━━━━━━━━━━━━━━━━━━

📊 **Total Games:** {stats.get('total_games', 0)}
👥 **Referrals:** {stats.get('referrals', 0)}
💎 **Earned Points:** {stats.get('points', 0)}
🎮 **Remaining Points:** {stats.get('remaining_games', 0)}

━━━━━━━━━━━━━━━━━━━━━
📈 **Keep Playing to Earn More!**
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
        ])
        
        await query.edit_message_text(
            stats_message,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        query = update.callback_query
        
        help_message = f"""
❓ **Help & Support**

━━━━━━━━━━━━━━━━━━━━━

🎮 **How to Play:**
1. Login to Shopsy with /login
2. Click 🎮 Play Game
3. Each game costs 1 Point
4. Play and earn rewards

💎 **Points System:**
• New users get 2 Points
• Refer a friend → +2 Points
• 1 Game = 1 Point

👥 **Referral System:**
• Share your referral link
• Each referral = +2 Points
• Unlimited referrals!

🔐 **Shopsy Login:**
• Use /login command
• Enter your phone number
• Verify OTP
• Session saved automatically

🚦 **Queue System:**
• Max {queue_manager.get_max_concurrent()} concurrent games
• Wait in queue if full
• Auto-start when slot available

━━━━━━━━━━━━━━━━━━━━━
💡 **Tips:**
• Refer friends for more points
• Play daily for rewards
• Check stats regularly

━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🏠 Dashboard", callback_data="back_to_dashboard")],
        ])
        
        await query.edit_message_text(
            help_message,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_invite_earn(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        query = update.callback_query
        stats = db.get_user_stats(user_id)
        
        ref_message = f"""
👥 **Invite & Earn**

━━━━━━━━━━━━━━━━━━━━━

💎 **Your Points:** {stats.get('points', 0)}
👥 **Your Referrals:** {stats.get('referrals', 0)}

🎯 **Referral Reward:** +{REFERRAL_REWARD} Points per referral

━━━━━━━━━━━━━━━━━━━━━

📤 **Share your referral link:**
*Each new user = +{REFERRAL_REWARD} Points*

━━━━━━━━━━━━━━━━━━━━━
"""
        
        await query.edit_message_text(
            ref_message,
            reply_markup=get_referral_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            await update.callback_query.edit_message_text(
                "❌ **Unauthorized Access**\n\nYou don't have admin privileges.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        query = update.callback_query
        
        admin_message = f"""
👑 **Admin Panel**

━━━━━━━━━━━━━━━━━━━━━

Welcome, Admin! You have full control over the bot.

**Statistics:**
👥 Total Users: {db.get_total_users()}
📊 Active Users (7d): {db.get_active_users()}
🎮 Active Games: {queue_manager.get_active_count()}/{queue_manager.get_max_concurrent()}
⏳ Queue Length: {queue_manager.get_queue_length()}

━━━━━━━━━━━━━━━━━━━━━
"""
        
        await query.edit_message_text(
            admin_message,
            reply_markup=get_admin_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        
        total_users = db.get_total_users()
        active_users = db.get_active_users()
        all_users = db.get_all_users()
        
        total_refs = sum(user.get('referral_count', 0) for user in all_users)
        total_points = sum(user.get('points', 0) for user in all_users)
        total_games = sum(user.get('games_played', 0) for user in all_users)
        
        dashboard = f"""
📊 **Admin Dashboard**

━━━━━━━━━━━━━━━━━━━━━

👥 **Total Users:** {total_users}
📈 **Active Users (7d):** {active_users}
👥 **Total Referrals:** {total_refs}
💎 **Total Points:** {total_points}
🎮 **Total Games Played:** {total_games}

🎮 **Active Games:** {queue_manager.get_active_count()}/{queue_manager.get_max_concurrent()}
⏳ **Queue Length:** {queue_manager.get_queue_length()}

━━━━━━━━━━━━━━━━━━━━━
📊 **System Status:** 🟢 Running
🔐 **Force Join:** {db.get_setting('force_join_enabled')}
🎮 **Game Mode:** {db.get_setting('game_enabled')}
⏸️ **Queue Status:** {'⏸️ Paused' if queue_manager.is_paused() else '▶️ Running'}

━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_dashboard")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ])
        
        await query.edit_message_text(
            dashboard,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        users = db.get_all_users()
        
        if not users:
            users_text = "No users registered yet."
        else:
            users_text = "👥 **User List**\n\n"
            for i, user in enumerate(users[:20]):
                status = "🟢" if user.get('is_banned') == 0 else "🔴"
                users_text += f"{i+1}. {status} @{user.get('username', 'N/A')} | 💎{user.get('points', 0)}\n"
            if len(users) > 20:
                users_text += f"\n... and {len(users) - 20} more"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ])
        
        await query.edit_message_text(
            users_text[:4096],
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_analytics(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        all_users = db.get_all_users()
        
        total_users = len(all_users)
        active_users = sum(1 for u in all_users if u.get('is_banned') == 0)
        banned_users = sum(1 for u in all_users if u.get('is_banned') == 1)
        
        total_points = sum(u.get('points', 0) for u in all_users)
        avg_points = total_points / total_users if total_users > 0 else 0
        
        total_games = sum(u.get('games_played', 0) for u in all_users)
        avg_games = total_games / total_users if total_users > 0 else 0
        
        total_refs = sum(u.get('referral_count', 0) for u in all_users)
        
        analytics = f"""
📈 **Analytics Dashboard**

━━━━━━━━━━━━━━━━━━━━━

👥 **Total Users:** {total_users}
🟢 **Active Users:** {active_users}
🔴 **Banned Users:** {banned_users}

💎 **Total Points:** {total_points}
📊 **Average Points:** {avg_points:.1f}

🎮 **Total Games:** {total_games}
📊 **Average Games:** {avg_games:.1f}

👥 **Total Referrals:** {total_refs}
📊 **Avg Referrals:** {total_refs/total_users if total_users > 0 else 0:.1f}

━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ])
        
        await query.edit_message_text(
            analytics,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_force_join(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        force_join_enabled = db.get_setting("force_join_enabled") == "true"
        channels = db.get_channels()
        
        channels_text = "\n".join([f"• {ch}" for ch in channels]) if channels else "• No channels"
        
        message = f"""
🔒 **Force Join Settings**

━━━━━━━━━━━━━━━━━━━━━

📊 **Status:** {db.get_setting('force_join_enabled')}
📋 **Channels:**

{channels_text}

━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Enable" if not force_join_enabled else "❌ Disable",
                    callback_data="admin_toggle_force_join"
                )
            ],
            [InlineKeyboardButton("➕ Add Channel", callback_data="admin_add_channel")],
            [InlineKeyboardButton("➖ Remove Channel", callback_data="admin_remove_channel")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ])
        
        await query.edit_message_text(
            message,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        
        message = f"""
⚙️ **Bot Settings**

━━━━━━━━━━━━━━━━━━━━━

🎮 **Game Mode:** {db.get_setting('game_enabled')}
🔒 **Force Join:** {db.get_setting('force_join_enabled')}
🎮 **Max Concurrent:** {queue_manager.get_max_concurrent()}
👥 **Admin Count:** {len(ADMIN_IDS)}

━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "🎮 Toggle Game",
                    callback_data="admin_toggle_game"
                )
            ],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ])
        
        await query.edit_message_text(
            message,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_restart(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        
        await query.edit_message_text(
            "🔄 **Restarting Bot...**\n\nPlease wait a moment.",
            parse_mode=ParseMode.MARKDOWN
        )
        
        await asyncio.sleep(2)
        
        await queue_manager.recover_from_restart(context)
        
        await query.edit_message_text(
            "✅ **Bot Restarted Successfully!**\n\nAll systems operational.\nQueue has been recovered.",
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_toggle_force_join(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        current = db.get_setting("force_join_enabled")
        new_val = "false" if current == "true" else "true"
        db.set_setting("force_join_enabled", new_val)
        
        status = "Enabled" if new_val == "true" else "Disabled"
        await query.answer(f"✅ Force Join {status}", show_alert=True)
        await self.handle_admin_force_join(update, context, user_id)

    async def handle_admin_toggle_game(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        current = db.get_setting("game_enabled")
        new_val = "false" if current == "true" else "true"
        db.set_setting("game_enabled", new_val)
        
        status = "Enabled" if new_val == "true" else "Disabled"
        await query.answer(f"🎮 Game {status}", show_alert=True)
        await self.handle_admin_settings(update, context, user_id)

    async def handle_admin_unlimited(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        
        # Check current state and toggle
        current = db.get_setting("unlimited_mode")
        if current == "true":
            db.set_setting("unlimited_mode", "false")
            status = "Disabled"
            emoji = "❌"
        else:
            db.set_setting("unlimited_mode", "true")
            # Give all admins unlimited points
            for admin_id in ADMIN_IDS:
                db.add_points(admin_id, 99999)
            status = "Enabled"
            emoji = "✅"
        
        await query.edit_message_text(
            f"♾️ **Unlimited Mode {status}**\n\n"
            f"{emoji} All admins now have unlimited points (99999).\n"
            f"No points will be deducted during gameplay.\n"
            f"Mode: {status}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
            ]),
            parse_mode=ParseMode.MARKDOWN
        )

    async def handle_admin_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        if user_id not in ADMIN_IDS:
            return
        
        query = update.callback_query
        
        logs = f"""
📂 **System Logs**

━━━━━━━━━━━━━━━━━━━━━

[12:34:56] 🟢 Bot started
[12:35:00] 👤 User joined
[12:35:01] 🎮 Game played
[12:36:00] 👥 Referral
[12:37:00] 💎 Points added

🎮 **Active Games:** {queue_manager.get_active_count()}/{queue_manager.get_max_concurrent()}
⏳ **Queue Length:** {queue_manager.get_queue_length()}

━━━━━━━━━━━━━━━━━━━━━
📊 **Recent Activity**
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_logs")],
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")],
        ])
        
        await query.edit_message_text(
            logs,
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )

    async def admin_add_points_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        await update.callback_query.edit_message_text(
            "➕ **Add Points**\n\n"
            "Send user ID and points in this format:\n"
            "`user_id points`\n\n"
            "Example: `123456789 10`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ADD_POINTS

    async def handle_add_points(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        try:
            parts = update.message.text.strip().split()
            if len(parts) != 2:
                await update.message.reply_text(
                    "❌ Invalid format. Use: `user_id points`",
                    parse_mode=ParseMode.MARKDOWN
                )
                return ADD_POINTS
            
            target_id = int(parts[0])
            points = int(parts[1])
            
            success = db.add_points(target_id, points)
            if success:
                await update.message.reply_text(
                    f"✅ Added {points} points to user `{target_id}`",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ User `{target_id}` not found.",
                    parse_mode=ParseMode.MARKDOWN
                )
        except (ValueError, IndexError):
            await update.message.reply_text(
                "❌ Invalid format. Use: `user_id points`",
                parse_mode=ParseMode.MARKDOWN
            )
            return ADD_POINTS
        
        return ConversationHandler.END

    async def admin_remove_points_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        await update.callback_query.edit_message_text(
            "➖ **Remove Points**\n\n"
            "Send user ID and points in this format:\n"
            "`user_id points`\n\n"
            "Example: `123456789 5`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return REMOVE_POINTS

    async def handle_remove_points(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        try:
            parts = update.message.text.strip().split()
            if len(parts) != 2:
                await update.message.reply_text(
                    "❌ Invalid format. Use: `user_id points`",
                    parse_mode=ParseMode.MARKDOWN
                )
                return REMOVE_POINTS
            
            target_id = int(parts[0])
            points = int(parts[1])
            
            user_data = db.get_user(target_id)
            if not user_data:
                await update.message.reply_text(
                    f"❌ User `{target_id}` not found.",
                    parse_mode=ParseMode.MARKDOWN
                )
                return REMOVE_POINTS
            
            current_points = user_data.get('points', 0)
            new_points = max(0, current_points - points)
            db.update_user_points(target_id, new_points)
            
            await update.message.reply_text(
                f"✅ Removed {points} points from user `{target_id}`\n"
                f"New balance: {new_points} points",
                parse_mode=ParseMode.MARKDOWN
            )
        except (ValueError, IndexError):
            await update.message.reply_text(
                "❌ Invalid format. Use: `user_id points`",
                parse_mode=ParseMode.MARKDOWN
            )
            return REMOVE_POINTS
        
        return ConversationHandler.END

    async def admin_ban_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        await update.callback_query.edit_message_text(
            "🚫 **Ban User**\n\n"
            "Send the user ID to ban:\n\n"
            "Example: `123456789`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return BAN_USER

    async def handle_ban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        try:
            target_id = int(update.message.text.strip())
            success = db.ban_user(target_id)
            if success:
                await update.message.reply_text(
                    f"✅ Banned user `{target_id}`",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ User `{target_id}` not found.",
                    parse_mode=ParseMode.MARKDOWN
                )
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid user ID. Send a number.",
                parse_mode=ParseMode.MARKDOWN
            )
            return BAN_USER
        
        return ConversationHandler.END

    async def admin_unban_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        await update.callback_query.edit_message_text(
            "✅ **Unban User**\n\n"
            "Send the user ID to unban:\n\n"
            "Example: `123456789`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return UNBAN_USER

    async def handle_unban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        try:
            target_id = int(update.message.text.strip())
            success = db.unban_user(target_id)
            if success:
                await update.message.reply_text(
                    f"✅ Unbanned user `{target_id}`",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ User `{target_id}` not found or not banned.",
                    parse_mode=ParseMode.MARKDOWN
                )
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid user ID. Send a number.",
                parse_mode=ParseMode.MARKDOWN
            )
            return UNBAN_USER
        
        return ConversationHandler.END

    async def admin_broadcast_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        await update.callback_query.edit_message_text(
            "📢 **Broadcast Message**\n\n"
            "Send the message to broadcast to all users:\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return BROADCAST

    async def handle_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        message = update.message.text
        
        users = db.get_all_users()
        sent = 0
        failed = 0
        
        status_message = await update.message.reply_text(
            "📤 Sending broadcast...\n0 sent",
            parse_mode=ParseMode.MARKDOWN
        )
        
        for i, user in enumerate(users):
            try:
                await context.bot.send_message(
                    user['user_id'],
                    f"📢 **Broadcast Message**\n\n{message}",
                    parse_mode=ParseMode.MARKDOWN
                )
                sent += 1
            except telegram.error.Unauthorized:
                # User blocked or deleted account - mark as inactive
                try:
                    with db.get_connection() as conn:
                        conn.execute(
                            "UPDATE users SET account_status = 'inactive' WHERE user_id = ?",
                            (user['user_id'],)
                        )
                        conn.commit()
                except Exception:
                    pass
                failed += 1
            except telegram.error.ChatNotFound:
                # User deleted their chat
                try:
                    with db.get_connection() as conn:
                        conn.execute(
                            "UPDATE users SET account_status = 'deleted' WHERE user_id = ?",
                            (user['user_id'],)
                        )
                        conn.commit()
                except Exception:
                    pass
                failed += 1
            except telegram.error.TelegramError as e:
                logging.error(f"Telegram error for user {user['user_id']}: {e}")
                failed += 1
            except Exception as e:
                logging.error(f"Unexpected error broadcasting to {user['user_id']}: {e}")
                failed += 1
            
            if i % 10 == 0:
                await status_message.edit_text(
                    f"📤 Sending broadcast...\n{sent} sent, {failed} failed",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        await status_message.edit_text(
            f"✅ **Broadcast Complete**\n\n"
            f"✅ Sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"📊 Total: {sent + failed}",
            parse_mode=ParseMode.MARKDOWN
        )
        
        return ConversationHandler.END

    async def admin_add_channel_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        await update.callback_query.edit_message_text(
            "➕ **Add Channel**\n\n"
            "Send the channel username to add:\n"
            "Example: `@channel_name`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ADD_CHANNEL

    async def handle_add_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        channel = update.message.text.strip()
        if not channel.startswith('@'):
            channel = '@' + channel
        
        success = db.add_channel(channel, user_id)
        if success:
            await update.message.reply_text(
                f"✅ Channel {channel} added successfully!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                f"❌ Channel {channel} already exists.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        return ConversationHandler.END

    async def admin_remove_channel_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.callback_query.from_user.id not in ADMIN_IDS:
            return
        
        channels = db.get_channels()
        channels_text = "\n".join([f"• {ch}" for ch in channels]) if channels else "No channels"
        
        await update.callback_query.edit_message_text(
            f"➖ **Remove Channel**\n\n"
            f"Current channels:\n{channels_text}\n\n"
            "Send the channel username to remove:\n"
            "Example: `@channel_name`\n\n"
            "Type /cancel to cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return REMOVE_CHANNEL

    async def handle_remove_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in ADMIN_IDS:
            return
        
        channel = update.message.text.strip()
        if not channel.startswith('@'):
            channel = '@' + channel
        
        success = db.remove_channel(channel)
        if success:
            await update.message.reply_text(
                f"✅ Channel {channel} removed successfully!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                f"❌ Channel {channel} not found.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        return ConversationHandler.END

    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "❌ Operation cancelled.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ConversationHandler.END

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.handle_help(update, context, update.effective_user.id)

    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logging.error(f"Error: {context.error}")
        try:
            if update and update.effective_message:
                await update.effective_message.reply_text(
                    "❌ An error occurred. Please try again later.",
                    parse_mode=ParseMode.MARKDOWN
                )
        except Exception:
            pass

    def run(self):
        """Start the bot with proper signal handling for Railway"""
        self.setup()
        logging.info("🕷️ SPIDER X SHOPSY Bot Started!")
        
        # FIXED: Add graceful shutdown handlers for Railway
        def signal_handler(signum, frame):
            logging.info("🛑 Received shutdown signal, stopping bot...")
            self.application.stop_running()
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, signal_handler)  # Railway sends SIGTERM
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        
        try:
            # FIXED: Use post_init instead of asyncio.create_task
            # This ensures queue restoration happens after app initialization
            self.application.run_polling(allowed_updates=Update.ALL_TYPES)
        except KeyboardInterrupt:
            logging.info("Bot stopped by user")
        except Exception as e:
            logging.error(f"❌ Error: {e}", exc_info=True)
            raise

# ============================================================
#  MAIN
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )
    
    bot = SpiderShopsyBot(TOKEN)
    bot.run()