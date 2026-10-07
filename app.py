"""
لوحة متابعة إعلانات تيليجرام ساعة بساعة.

التشغيل:
    pip install -r requirements.txt
    set TG_ADS_TOKEN=رمز_الوصول        (ويندوز)
    export TG_ADS_TOKEN=رمز_الوصول     (ماك)
    python app.py
ثم افتحي http://localhost:8000

من غير رمز وصول البرنامج يشتغل ببيانات تجريبية علشان تشوفي الشكل.

متغيرات اختيارية:
    DASH_PASSWORD   كلمة سر للصفحة (ضروري لو البرنامج على الإنترنت)، واسم المستخدم DASH_USER (الافتراضي admin)
    TZ_NAME         المنطقة الزمنية، الافتراضي Africa/Cairo
    SYNC_MINUTES    كل كام دقيقة يسحب البيانات، الافتراضي 10
    BACKFILL_DAYS   كام يوم قديم يسحبهم أول مرة، الافتراضي 14
    PORT            الافتراضي 8000
"""
import hmac
import json
import re
import statistics
import math
import os
import random
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

API_BASE = "https://promoteapi.telegram.org"
TOKEN = os.environ.get("TG_ADS_TOKEN", "").strip()
DEMO = not TOKEN
TZ = ZoneInfo(os.environ.get("TZ_NAME", "Africa/Cairo"))
SYNC_MINUTES = int(os.environ.get("SYNC_MINUTES", "10"))
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "14"))
DASH_USER = os.environ.get("DASH_USER", "admin")
DASH_PASSWORD = os.environ.get("DASH_PASSWORD", "")
PORT = int(os.environ.get("PORT", "8000"))
# أسماء المتغيرات تُقرأ بتسامح: مسافات زائدة حول الاسم أو علامات تنصيص حول القيمة لا تمنع قراءتها
ENV = {k.strip().upper(): v.strip().strip('"').strip("'") for k, v in os.environ.items()}
TRACK_BOT_TOKEN = ENV.get("TRACK_BOT_TOKEN", "")   # بوت مشرف في قنواتك يسجل الاشتراك والخروج
NOTIFY_CHAT_ID = ENV.get("NOTIFY_CHAT_ID", "")     # المحادثة التي تصلها تنبيهات الحارس
STAY_MINUTES = int(os.environ.get("STAY_MINUTES", "60"))          # من يخرج قبلها لا يُحسب ليدًا باقيًا
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(HERE, "demo.db" if DEMO else "ads.db"))

STEP = 300            # أصغر فاصل تسمح به الواجهة: خمس دقائق
CHUNK = 1000 * STEP   # أقصى فترة في الطلب الواحد
OVERLAP = 2 * 3600    # نعيد سحب آخر ساعتين كل مرة لأن الأرقام الحديثة بتتعدل
DELETE_WAIT = int(os.environ.get("DELETE_WAIT_SECONDS", "660"))  # تيليجرام يشترط توقف الإعلان 10 دقائق قبل حذفه
DELETE_TRIES = 6      # عدد محاولات الحذف المؤجل قبل إظهاره كفشل
REQUEST_GAP = 0.15    # مهلة بين الطلبات، التوثيق لا يذكر حدًا لعددها


# ---------------------------------------------------------------- الاتصال بتيليجرام
class ApiError(Exception):
    pass


class FloodWait(ApiError):
    """تيليجرام طلب الانتظار مدة طويلة قبل تكرار هذا النوع من الطلبات."""


FLOOD = {}  # اسم الطلب -> الوقت الذي يُسمح بتكراره بعده


def flood_until(method):
    if method not in FLOOD:
        try:
            FLOOD[method] = int(float(get_meta("flood_" + method, "0") or 0))
        except Exception:
            return 0
    return FLOOD[method] if FLOOD[method] > time.time() else 0


def flood_set(method, until):
    FLOOD[method] = int(until)
    try:
        set_meta("flood_" + method, int(until))
    except Exception:
        pass


def clock(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%H:%M يوم %d/%m")


class TelegramAds:
    def __init__(self, token):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {token}"

    def call(self, method, **params):
        last = "NETWORK_ERROR"
        until = flood_until(method)
        if until:  # لا نرسل الطلب أصلًا طوال مهلة الانتظار حتى لا تطول
            raise FloodWait(f"FLOOD_WAIT_{int(until - time.time())}")
        for attempt in range(5):
            try:
                r = self.s.post(f"{API_BASE}/{method}", json=params, timeout=40)
                data = r.json()
            except (requests.RequestException, ValueError) as e:
                last = f"NETWORK_ERROR: {e}"
                time.sleep(2 ** attempt)
                continue
            if data.get("ok"):
                time.sleep(REQUEST_GAP)
                return data.get("result")
            last = str(data.get("error", f"HTTP_{r.status_code}"))
            m = re.search(r"FLOOD_WAIT_(\d+)", last)
            if m and int(m.group(1)) > 30:
                flood_set(method, time.time() + int(m.group(1)))
                raise FloodWait(last)
            if r.status_code == 429 or r.status_code >= 500 or "FLOOD" in last or "TOO_MANY" in last:
                time.sleep(3 * (attempt + 1))
                continue
            raise ApiError(last)
        raise ApiError(last)

    def upload(self, method, name, blob, mime, **params):
        try:
            r = self.s.post(f"{API_BASE}/{method}", data=params,
                            files={"file": (name, blob, mime)}, timeout=120)
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            raise ApiError(f"NETWORK_ERROR: {e}")
        if data.get("ok"):
            return data["result"]
        raise ApiError(str(data.get("error", f"HTTP_{r.status_code}")))


class DemoAds:
    """بيانات وهمية بنفس شكل ردود الواجهة، للتجربة من غير رمز وصول."""
    ADS = [  # الرقم، العنوان، سعر الألف، مشاهدات كل خمس دقائق، نسبة النقر، نسبة التحويل، الحالة
        (101, "عروض أمازون مصر - قنوات التسوق", 1.20, 420, 0.011, 0.42, "active"),
        (102, "خصومات السعودية - جمهور عام", 2.10, 380, 0.009, 0.35, "active"),
        (103, "كوبونات نون - استهداف قنوات", 0.90, 260, 0.014, 0.50, "active"),
        (104, "عروض الجمعة البيضاء - فيديو", 3.40, 300, 0.016, 0.22, "active"),
        (105, "قناة الإلكترونيات - بحث", 1.60, 90, 0.030, 0.55, "active"),
        (106, "بوت متابعة الأسعار", 1.10, 210, 0.012, 0.30, "active"),
        (107, "عروض المطبخ - ربات البيوت", 0.80, 330, 0.008, 0.28, "on_hold"),
        (108, "عروض الموبايلات - القاهرة", 1.90, 150, 0.010, 0.12, "active"),
        (109, "تجربة نص جديد - أ", 1.30, 120, 0.013, 0.40, "in_review"),
        (110, "حملة رمضان القديمة", 1.00, 280, 0.010, 0.33, "stopped"),
        (111, "عروض الأطفال - مرفوض", 1.00, 0, 0.0, 0.0, "declined"),
    ]

    CREATED = {}  # الإعلانات المنشأة في الوضع التجريبي ووقت إنشائها

    def _bucket(self, ad, t):
        ad_id, _, cpm, base, ctr, cvr, status = ad
        if not base or status in ("in_review", "declined"):
            return None
        if status in ("stopped", "on_hold") and t > time.time() - (6 if status == "stopped" else 1) * 86400:
            return None
        rnd = random.Random(ad_id * 1_000_003 + t)
        hour = datetime.fromtimestamp(t, TZ).hour
        curve = 0.2 + 0.8 * max(0.0, math.sin((hour - 6) / 18 * math.pi))
        hour_quality = 0.6 + 0.8 * random.Random(ad_id * 31 + hour).random()
        views = int(base * curve * rnd.uniform(0.6, 1.4))
        clicks = int(views * ctr * rnd.uniform(0.5, 1.5) + rnd.random())
        actions = min(clicks, int(clicks * cvr * hour_quality * rnd.uniform(0.4, 1.6) + rnd.random()))
        return {"from_time": t, "to_time": t + STEP, "views": views, "opens": 0, "clicks": clicks,
                "actions": actions, "currency": "EUR", "spent_budget": round(views / 1000 * cpm, 5)}

    def call(self, method, **p):
        now = int(time.time())
        if method == "getCurrentAccount":
            return {"account_id": "demo", "title": "حساب تجريبي", "currency": "EUR",
                    "spent_budget": 1840.5, "remaining_budget": 312.75, "ads_budget": 96.2}
        if method == "getRelatedAccountsList":
            raise ApiError("DEMO")
        if method == "getAdsList":
            ads = []
            for ad in self.ADS:
                ad_id, title, cpm, base, _, _, status = ad
                ads.append({
                    "ad_id": ad_id, "title": title, "currency": "EUR", "cpm": cpm, "status": status,
                    "text": "أقوى العروض والخصومات اليومية، اشترك في القناة وما تفوتش أي عرض.",
                    "promote_url": "https://t.me/badchan" if ad_id == 108 else "https://t.me/example",
                    "placement": "channel_post",
                    "views": now // STEP * (base or 1) // 50, "clicks": 0, "actions": 0,
                    "spent_budget": round(now // STEP * (base or 0) / 50000 * cpm, 2),
                    "remaining_budget": 25.0, "daily_budget_limit": 15.0, "action_type": "join",
                    "created_date": self.CREATED.get(ad_id, now - 40 * 86400), "is_paused": status == "on_hold",
                    **({"decline_reason": {"text": "النص مخالف لسياسة الإعلانات"}} if status == "declined" else {}),
                })
            return {"total_count": len(ads), "ads": ads}
        if method == "getAdStats":
            ad = next(a for a in self.ADS if a[0] == p["ad_id"])
            out = []
            first = self.CREATED.get(p["ad_id"], 0) // STEP * STEP
            for t in range(max(p["from_time"], first), min(p["to_time"], now // STEP * STEP + STEP), STEP):
                b = self._bucket(ad, t)
                if b:
                    out.append(b)
            return out
        if method == "getTargetChannel":
            name = str(p["channel_id"]).lstrip("@")
            if "group" in name.lower():
                raise ApiError("CHANNEL_INVALID")
            return {"channel_id": abs(hash(name)) % 10**9, "title": "قناة " + name, "username": name}
        if method == "getAdsById":
            ad = next((a for a in self.call("getAdsList")["ads"] if a["ad_id"] in p["ad_ids"]), None)
            if ad:
                ad["target"] = {"type": "channels", "channels": [
                    {"channel_id": 1, "username": "deals_eg", "title": "عروض مصر"},
                    {"channel_id": 2, "username": "offers_sa", "title": "عروض السعودية"}]}
            return [ad] if ad else []
        if method in ("editAd", "deleteAd"):
            i = next((i for i, a in enumerate(self.ADS) if a[0] == p["ad_id"]), None)
            if i is None:
                raise ApiError("AD_NOT_FOUND")
            if method == "deleteAd":
                if self.ADS[i][6] == "active":
                    raise ApiError("AD_IS_ACTIVE")
                del self.ADS[i]
                return True
            status = self.ADS[i][6]
            if "is_paused" in p:
                status = "on_hold" if p["is_paused"] else "active"
            self.ADS[i] = self.ADS[i][:6] + (status,)
            return {"ad_id": p["ad_id"], "status": status}
        if method in ("increaseAdBudget", "decreaseAdBudget"):
            return {"ad_id": p["ad_id"]}
        if method == "createAd":
            if len(p.get("text", "")) > 160:
                raise ApiError("AD_TEXT_TOO_LONG")
            new_id = max(a[0] for a in self.ADS) + 1
            if p.get("initial_budget"):  # إعلان حملة: يعمل فورًا بأداء عشوائي ثابت لكل إعلان
                rnd = random.Random(new_id * 77)
                self.ADS.append((new_id, p["title"], p["cpm"], rnd.randint(5, 15), 0.012,
                                 rnd.choice([0.03, 0.08, 0.2, 0.4, 0.6]), "active"))
                self.CREATED[new_id] = int(time.time()) - int(os.environ.get("DEMO_BACKDATE_HOURS", "0")) * 3600
                return {"ad_id": new_id, "status": "active", "created_date": self.CREATED[new_id]}
            self.ADS.append((new_id, p["title"], p["cpm"], 0, 0.0, 0.0, "in_review"))
            return {"ad_id": new_id, "status": "in_review"}
        raise ApiError("METHOD_NOT_IN_DEMO")

    def upload(self, method, name, blob, mime, **params):
        return {"photo_id": "demo-" + name, "photo_url": ""}


client = DemoAds() if DEMO else TelegramAds(TOKEN)


# ---------------------------------------------------------------- التخزين
def prepare_db_path():
    """يتأكد أن مجلد قاعدة البيانات موجود وقابل للكتابة، وإلا يرجع لمجلد البرنامج."""
    global DB_PATH
    folder = os.path.dirname(DB_PATH) or "."
    try:
        os.makedirs(folder, exist_ok=True)
        sqlite3.connect(DB_PATH, timeout=30).close()
    except (OSError, sqlite3.Error) as e:
        fallback = os.path.join(HERE, os.path.basename(DB_PATH))
        print(f"تحذير: تعذر استخدام {DB_PATH} ({e}). سيتم التخزين مؤقتًا في {fallback} "
              "والبيانات ستُمسح مع كل نشر. راجعي ربط مساحة التخزين.", flush=True)
        DB_PATH = fallback


def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    with db() as con:
        con.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS accounts(
            account_id TEXT PRIMARY KEY, title TEXT, currency TEXT, is_main INTEGER,
            spent REAL, remaining REAL);
        CREATE TABLE IF NOT EXISTS ads(
            account_id TEXT, ad_id INTEGER, title TEXT, text TEXT, promote_url TEXT, status TEXT,
            placement TEXT, cpm REAL, currency TEXT, views INTEGER, spent REAL, remaining REAL,
            daily_limit REAL, action_type TEXT, decline_reason TEXT, created_date INTEGER,
            synced_until INTEGER, PRIMARY KEY(account_id, ad_id));
        CREATE TABLE IF NOT EXISTS stats(
            account_id TEXT, ad_id INTEGER, t INTEGER, views INTEGER, opens INTEGER,
            clicks INTEGER, actions INTEGER, spent REAL,
            PRIMARY KEY(account_id, ad_id, t)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS campaigns(
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, account_id TEXT, template INTEGER,
            target_leads REAL, daily_budget REAL, cpm REAL, mode TEXT, country TEXT, currency TEXT,
            state TEXT, capped TEXT, created INTEGER);
        CREATE TABLE IF NOT EXISTS camp_creatives(
            id INTEGER PRIMARY KEY AUTOINCREMENT, camp_id INTEGER, text TEXT,
            photo BLOB, photo_name TEXT, photo_mime TEXT, photo_id TEXT);
        CREATE TABLE IF NOT EXISTS camp_groups(
            id INTEGER PRIMARY KEY AUTOINCREMENT, camp_id INTEGER, idx INTEGER, channels TEXT);
        CREATE TABLE IF NOT EXISTS camp_ads(
            camp_id INTEGER, account_id TEXT, ad_id INTEGER, creative_id INTEGER, group_id INTEGER,
            state TEXT, daily_limit REAL, created INTEGER, raised INTEGER, stopped INTEGER,
            reclaimed INTEGER, reason TEXT, PRIMARY KEY(account_id, ad_id));
        CREATE TABLE IF NOT EXISTS camp_log(id INTEGER PRIMARY KEY AUTOINCREMENT, camp_id INTEGER, ts INTEGER, text TEXT);
        CREATE TABLE IF NOT EXISTS track_chats(chat_id INTEGER PRIMARY KEY, username TEXT, title TEXT);
        CREATE TABLE IF NOT EXISTS members_log(
            chat_id INTEGER, user_id INTEGER, joined INTEGER, left_ts INTEGER);
        CREATE INDEX IF NOT EXISTS members_log_i ON members_log(chat_id, user_id);
        CREATE INDEX IF NOT EXISTS members_log_j ON members_log(joined);
        CREATE TABLE IF NOT EXISTS guard_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, account_id TEXT, ad_id INTEGER,
            title TEXT, rule TEXT, reason TEXT, state TEXT);
        CREATE TABLE IF NOT EXISTS channel_results(name TEXT PRIMARY KEY, data TEXT, ts INTEGER);
        CREATE TABLE IF NOT EXISTS channel_snap(
            username TEXT, day TEXT, subs INTEGER, PRIMARY KEY(username, day));
        CREATE TABLE IF NOT EXISTS pending_deletes(
            account_id TEXT, ad_id INTEGER, due INTEGER, tries INTEGER, error TEXT,
            PRIMARY KEY(account_id, ad_id));
        """)


def set_meta(key, value):
    with db() as con:
        con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, str(value)))


def get_meta(key, default=""):
    with db() as con:
        row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


# ---------------------------------------------------------------- السحب الدوري
def sync_ad(acc_id, acc_param, ad):
    ad_id = ad["ad_id"]
    now_b = int(time.time()) // STEP * STEP
    with db() as con:
        old = con.execute("SELECT views, spent, synced_until FROM ads WHERE account_id=? AND ad_id=?",
                          (acc_id, ad_id)).fetchone()
    views, spent = ad.get("views", 0), ad.get("spent_budget", 0)
    changed = old is None or not old["synced_until"] or old["views"] != views or old["spent"] != spent
    synced_until = old["synced_until"] if old else None

    # الإعلان النشط يُعاد سحبه كل مرة، لأن أرقام الفواصل الأخيرة قد تتأخر عن الإجمالي
    if (changed or ad.get("status") == "active") and views:
        if synced_until:
            start = synced_until - OVERLAP
        else:
            start = max(ad.get("created_date", 0), now_b - BACKFILL_DAYS * 86400)
        start = start // STEP * STEP
        end = now_b + STEP
        rows = []
        while start < end:
            stop = min(start + CHUNK, end)
            items = client.call("getAdStats", ad_id=ad_id, from_time=start, to_time=stop,
                                interval=STEP, **acc_param) or []
            for it in items:
                v, c = it.get("views", 0), it.get("clicks", 0)
                a = it.get("actions", it.get("joins", 0))
                s = it.get("spent_budget", 0)
                if v or c or a or s:
                    rows.append((acc_id, ad_id, it["from_time"], v, it.get("opens", 0), c, a, s))
            start = stop
        with db() as con:
            con.executemany("INSERT OR REPLACE INTO stats VALUES(?,?,?,?,?,?,?,?)", rows)
    synced_until = now_b

    reason = (ad.get("decline_reason") or {}).get("text", "")
    with db() as con:
        con.execute("INSERT OR REPLACE INTO ads VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            acc_id, ad_id, ad.get("title", ""), ad.get("text", ""), ad.get("promote_url", ""),
            ad.get("status", ""), ad.get("placement", ""), ad.get("cpm", 0), ad.get("currency", ""),
            views, spent, ad.get("remaining_budget", 0), ad.get("daily_budget_limit", 0),
            ad.get("action_type", ""), reason, ad.get("created_date", 0), synced_until))


def sync_once():
    current = client.call("getCurrentAccount")
    accounts = [(current, True)]
    try:
        related = client.call("getRelatedAccountsList") or {}
        accounts += [(a, False) for a in related.get("accounts", [])]
    except ApiError:
        pass  # الحساب ليس حسابًا رئيسيًا، نكتفي به
    for acc, is_main in accounts:
        acc_id = acc["account_id"]
        acc_param = {} if is_main else {"account_id": acc_id}
        with db() as con:
            con.execute("INSERT OR REPLACE INTO accounts VALUES(?,?,?,?,?,?)", (
                acc_id, acc.get("title", ""), acc.get("currency", ""), int(is_main),
                acc.get("spent_budget", 0), acc.get("remaining_budget", 0)))
        offset, seen = None, set()
        while True:
            params = dict(acc_param, limit=100)
            if offset:
                params["offset"] = offset
            res = client.call("getAdsList", **params) or {}
            ads = res.get("ads", [])
            for ad in ads:
                seen.add(ad["ad_id"])
                sync_ad(acc_id, acc_param, ad)
            offset = res.get("next_offset")
            if not offset or not ads:
                break
        with db() as con:  # الإعلانات المحذوفة من تيليجرام تتعلّم كمحذوفة وتفضل بياناتها
            for row in con.execute("SELECT ad_id FROM ads WHERE account_id=?", (acc_id,)).fetchall():
                if row["ad_id"] not in seen:
                    con.execute("UPDATE ads SET status='deleted' WHERE account_id=? AND ad_id=?",
                                (acc_id, row["ad_id"]))


wake = threading.Event()
syncing = threading.Event()


def sync_loop():
    while True:
        syncing.set()
        try:
            sync_once()
            set_meta("last_sync", int(time.time()))
            try:
                guard_run()
            except Exception as e:
                set_meta("guard_error", f"{type(e).__name__}: {e}")
            try:
                campaigns_run()
                set_meta("camp_error", "")
            except Exception as e:
                set_meta("camp_error", f"{type(e).__name__}: {e}")
            set_meta("last_error", "")
        except Exception as e:  # نسجل الخطأ ونكمل في الدورة الجاية
            set_meta("last_error", f"{type(e).__name__}: {e}")
        syncing.clear()
        wake.wait(SYNC_MINUTES * 60)
        wake.clear()


def delete_loop():
    """ينفذ الحذف المؤجل: الإعلانات التي أوقفناها وننتظر مرور المهلة لحذفها."""
    while True:
        time.sleep(min(30, max(2, DELETE_WAIT // 4)))
        try:
            with db() as con:
                rows = con.execute("SELECT * FROM pending_deletes WHERE due<=? AND tries<?",
                                   (int(time.time()), DELETE_TRIES)).fetchall()
            for r in rows:
                acc_id, ad_id = r["account_id"], r["ad_id"]
                try:
                    client.call("deleteAd", ad_id=ad_id, **acc_param_for(acc_id))
                    gone = True
                except ApiError as e:
                    gone = "NOT_FOUND" in str(e)
                    err = str(e)
                with db() as con:
                    if gone:
                        con.execute("DELETE FROM pending_deletes WHERE account_id=? AND ad_id=?", (acc_id, ad_id))
                        con.execute("UPDATE ads SET status='deleted' WHERE account_id=? AND ad_id=?", (acc_id, ad_id))
                    else:  # نعيد المحاولة بعد دقيقتين
                        con.execute("UPDATE pending_deletes SET tries=tries+1, due=?, error=? "
                                    "WHERE account_id=? AND ad_id=?", (int(time.time()) + 120, err, acc_id, ad_id))
        except Exception:
            pass


# ---------------------------------------------------------------- الحسابات
def metrics(v, c, a, s):
    v, c, a, s = v or 0, c or 0, a or 0, s or 0
    return {"views": v, "clicks": c, "actions": a, "spent": round(s, 5),
            "ctr": round(c / v * 100, 2) if v else None,
            "cpc": round(s / c, 6) if c else None,
            "cpl": round(s / a, 6) if a else None,
            "cpm": round(s / v * 1000, 3) if v else None}


def day_start(d):
    return int(datetime(d.year, d.month, d.day, tzinfo=TZ).timestamp())


def today():
    return datetime.now(TZ).date()


def period_range(period, now):
    t = today()
    h = now // 3600 * 3600  # بداية الساعة الحالية
    if period == "now":      # الساعة الحالية من أولها حتى الآن (غير مكتملة)
        return h, now + STEP
    if period == "hour":     # آخر ساعة مكتملة، مثل 14:00 إلى 15:00
        return h - 3600, h
    if period == "3h":       # آخر ثلاث ساعات مكتملة
        return h - 3 * 3600, h
    if period == "yesterday":
        return day_start(t - timedelta(days=1)), day_start(t)
    if period == "7d":
        return day_start(t - timedelta(days=6)), now + STEP
    return day_start(t), now + STEP


def range_text(start, end):
    a, b = datetime.fromtimestamp(start, TZ), datetime.fromtimestamp(end, TZ)
    if a.date() == b.date():
        return f"{a:%d/%m} من {a:%H:%M} إلى {b:%H:%M}"
    return f"من {a:%d/%m %H:%M} إلى {b:%d/%m %H:%M}"


# ---------------------------------------------------------------- الويب
app = Flask(__name__)


@app.before_request
def guard():
    # طلبات التعديل لا تُقبل إلا من الصفحة نفسها، حتى لا يرسلها موقع آخر باسمك
    if request.method == "POST" and request.headers.get("X-Requested-With") != "dashboard":
        return Response("طلب غير مسموح", 403)
    if not DASH_PASSWORD:
        return None
    auth = request.authorization
    same = lambda a, b: hmac.compare_digest((a or "").encode("utf-8"), b.encode("utf-8"))
    if auth and same(auth.username, DASH_USER) and same(auth.password, DASH_PASSWORD):
        return None
    return Response("مطلوب تسجيل الدخول", 401, {"WWW-Authenticate": 'Basic realm="ads"'})


@app.get("/")
def index():
    resp = send_from_directory(HERE, "index.html", max_age=0)
    resp.headers["Cache-Control"] = "no-store"  # حتى لا يعرض المتصفح نسخة قديمة من الصفحة
    return resp


@app.get("/api/overview")
def overview():
    now = int(time.time())
    start, end = period_range(request.args.get("period", "today"), now)
    # شريط الساعات يعرض يومًا تقويميًا كاملًا: أمس لو الفترة أمس، وغير ذلك اليوم الحالي
    is_yday = request.args.get("period") == "yesterday"
    strip_day = today() - timedelta(days=1) if is_yday else today()
    d0, d1 = day_start(strip_day), day_start(strip_day + timedelta(days=1))
    strip_ts = list(range(d0, d1, 3600))
    with db() as con:
        accounts = [dict(r) for r in con.execute("SELECT * FROM accounts ORDER BY is_main DESC, title")]
        ads = [dict(r) for r in con.execute("SELECT * FROM ads")]
        totals = {(r["account_id"], r["ad_id"]): r for r in con.execute(
            "SELECT account_id, ad_id, SUM(views) v, SUM(clicks) c, SUM(actions) a, SUM(spent) s "
            "FROM stats WHERE t>=? AND t<? GROUP BY account_id, ad_id", (start, end))}
        hours = {}
        for r in con.execute(
                "SELECT account_id, ad_id, t/3600 h, SUM(actions) a, SUM(spent) s, SUM(views) v "
                "FROM stats WHERE t>=? AND t<? GROUP BY account_id, ad_id, h", (d0, d1)):
            hours.setdefault((r["account_id"], r["ad_id"]), {})[r["h"]] = (r["a"], round(r["s"], 5), r["v"])
        pend = {(r["account_id"], r["ad_id"]): r for r in con.execute("SELECT * FROM pending_deletes")}
    acc_title = {a["account_id"]: a["title"] for a in accounts}
    delete_errors = []
    out = []
    for ad in ads:
        key = (ad["account_id"], ad["ad_id"])
        t = totals.get(key)
        ad["m"] = metrics(t["v"], t["c"], t["a"], t["s"]) if t else metrics(0, 0, 0, 0)
        h = hours.get(key, {})
        ad["strip"] = [h.get(ts // 3600, (0, 0, 0)) for ts in strip_ts]
        ad["account_title"] = acc_title.get(ad["account_id"], "")
        pd = pend.get(key)
        if pd and ad["status"] != "deleted":
            if pd["tries"] >= DELETE_TRIES:
                delete_errors.append({"title": ad["title"], "error": pd["error"]})
            else:
                ad["delete_due"] = pd["due"]
        out.append(ad)
    real, tracked = real_leads(start, end)
    for ad in out:
        if chan_of(ad["promote_url"]) in tracked:
            got = real.get((ad["account_id"], ad["ad_id"]), [0.0, 0])
            m = ad["m"]
            m["tracked"] = True
            m["real"] = round(got[0], 1)
            m["real_cpl"] = round(m["spent"] / got[0], 6) if got[0] > 0 else None
            m["leak"] = round((1 - got[0] / got[1]) * 100) if got[1] else None
    return jsonify({
        "any_tracked": bool(tracked), "stay_minutes": STAY_MINUTES,
        "demo": DEMO, "syncing": syncing.is_set(), "last_sync": int(get_meta("last_sync", "0") or 0),
        "last_error": get_meta("last_error"), "delete_errors": delete_errors, "sync_minutes": SYNC_MINUTES,
        "strip_ts": strip_ts, "strip_hours": [datetime.fromtimestamp(ts, TZ).hour for ts in strip_ts],
        "strip_date": strip_day.isoformat(), "strip_label": "أمس" if is_yday else "اليوم",
        "range_text": range_text(start, min(end, now)), "tz": str(TZ), "accounts": accounts, "ads": out})


@app.get("/api/ad")
def ad_detail():
    acc_id = request.args.get("account", "")
    ad_id = int(request.args.get("ad_id", "0"))
    days = max(1, min(int(request.args.get("days", "14")), 90))
    try:
        d = date.fromisoformat(request.args.get("date", ""))
    except ValueError:
        d = today()
    with db() as con:
        ad = con.execute("SELECT * FROM ads WHERE account_id=? AND ad_id=?", (acc_id, ad_id)).fetchone()
        if not ad:
            return jsonify({"error": "الإعلان غير موجود"}), 404

        def by_hour(start, end):
            return {r["h"] * 3600: r for r in con.execute(
                "SELECT t/3600 h, SUM(views) v, SUM(clicks) c, SUM(actions) a, SUM(spent) s FROM stats "
                "WHERE account_id=? AND ad_id=? AND t>=? AND t<? GROUP BY h", (acc_id, ad_id, start, end))}

        d0, d1 = day_start(d), day_start(d + timedelta(days=1))
        rows = by_hour(d0, d1)
        hourly = []
        for ts in range(d0, d1, 3600):
            r = rows.get(ts)
            m = metrics(r["v"], r["c"], r["a"], r["s"]) if r else metrics(0, 0, 0, 0)
            m["hour"] = datetime.fromtimestamp(ts, TZ).hour
            m["future"] = ts > time.time()
            hourly.append(m)

        first = today() - timedelta(days=days - 1)
        agg = {}
        for ts, r in by_hour(day_start(first), day_start(today() + timedelta(days=1))).items():
            k = datetime.fromtimestamp(ts, TZ).date().isoformat()
            x = agg.setdefault(k, [0, 0, 0, 0.0])
            x[0] += r["v"]; x[1] += r["c"]; x[2] += r["a"]; x[3] += r["s"]
        daily = []
        for i in range(days):
            k = (today() - timedelta(days=i)).isoformat()
            m = metrics(*agg.get(k, (0, 0, 0, 0)))
            m["date"] = k
            daily.append(m)
    return jsonify({"ad": dict(ad), "date": d.isoformat(), "today": today().isoformat(),
                    "hourly": hourly, "daily": daily})


# ---------------------------------------------------------------- إنشاء إعلانات بنفس إعدادات إعلان موجود
PLACEMENTS = {"channel_post": "منشور في قناة", "bot_banner": "شريط في بوت",
              "search_result": "نتيجة بحث", "video_banner": "شريط فيديو"}
COPY_FIELDS = ["promote_url", "cpm", "placement", "impression_frequency", "website_name", "button",
               "conversion_event_id", "additional_info", "show_userpic", "daily_budget_limit", "schedule"]
PHOTO_CACHE = {}  # مفتاح عدم التكرار -> رقم الصورة المرفوعة، حتى لا تتغير الصورة عند إعادة المحاولة


def acc_param_for(acc_id):
    with db() as con:
        r = con.execute("SELECT is_main FROM accounts WHERE account_id=?", (acc_id,)).fetchone()
    if not r:
        raise ApiError("ACCOUNT_UNKNOWN")
    return {} if r["is_main"] else {"account_id": acc_id}


def fetch_template(acc_id, ad_id):
    res = client.call("getAdsById", ad_ids=[ad_id], return_target=True, **acc_param_for(acc_id))
    if not res:
        raise ApiError("AD_NOT_FOUND")
    return res[0]


def input_target(t):
    """يحوّل الاستهداف كما يرجعه تيليجرام إلى الشكل المطلوب عند الإنشاء."""
    def refs(key, id_key):
        # اسم المستخدم هو المفضل، لأن الأرقام لا تُقبل إلا لو سبق حلّها بالاسم
        return [("@" + x["username"]) if x.get("username") else x[id_key] for x in t.get(key) or []]
    ids = lambda key, id_key: [x[id_key] for x in t.get(key) or []]
    typ = t.get("type")
    out = {"type": typ}
    if typ == "search":
        out["search_queries"] = t.get("search_queries") or []
    elif typ == "bots":
        out["bot_ids"] = refs("bots", "bot_id")
    else:
        if typ == "users":
            out["country_codes"] = ids("countries", "country_code")
            for k in ("intersect_topics", "device", "exclude_political_channels", "political_channels_only"):
                if t.get(k):
                    out[k] = t[k]
        extra = {"language_codes": ids("languages", "language_code"), "topic_ids": ids("topics", "topic_id"),
                 "exclude_topic_ids": ids("exclude_topics", "topic_id"),
                 "channel_ids": refs("channels", "channel_id"),
                 "exclude_channel_ids": refs("exclude_channels", "channel_id"),
                 "location_ids": ids("locations", "location_id"),
                 "audience_ids": ids("audiences", "audience_id"),
                 "exclude_audience_ids": ids("exclude_audiences", "audience_id")}
        out.update({k: v for k, v in extra.items() if v})
    return out


def target_text(t):
    names = lambda key, n="name": "، ".join(str(x.get(n) or x.get("title") or x.get("username") or "") for x in t.get(key) or [])
    typ = t.get("type")
    if typ == "search":
        return "كلمات بحث: " + "، ".join(t.get("search_queries") or [])
    if typ == "bots":
        return f"بوتات ({len(t.get('bots') or [])}): " + names("bots", "title")
    parts = []
    for key, label, n in (("countries", "الدول", "name"), ("locations", "المناطق", "name"), ("languages", "اللغات", "name"),
                          ("topics", "المواضيع", "name"), ("exclude_topics", "مواضيع مستبعدة", "name"),
                          ("channels", "القنوات", "title"), ("exclude_channels", "قنوات مستبعدة", "title"),
                          ("audiences", "جماهير", "title"), ("exclude_audiences", "جماهير مستبعدة", "title")):
        if t.get(key):
            parts.append(f"{label} ({len(t[key])}): {names(key, n)}")
    if t.get("device"):
        parts.append("الجهاز: " + t["device"])
    head = "استهداف قنوات" if typ == "channels" else "استهداف مستخدمين"
    return head + (" — " + " | ".join(parts) if parts else "")


@app.get("/api/template")
def template():
    try:
        ad = fetch_template(request.args.get("account", ""), int(request.args.get("ad_id", "0")))
    except ApiError as e:
        return jsonify({"error": str(e)})
    t = ad.get("target") or {}
    return jsonify({
        "title": ad.get("title", ""), "text": ad.get("text", ""), "promote_url": ad.get("promote_url", ""),
        "cpm": ad.get("cpm", 0), "currency": ad.get("currency", ""),
        "daily_budget_limit": ad.get("daily_budget_limit", 0),
        "placement": PLACEMENTS.get(ad.get("placement"), ad.get("placement", "")),
        "target_type": t.get("type", ""), "target_text": target_text(t),
        "media": "photo" if ad.get("photo") else "video" if ad.get("video") else ""})


@app.post("/api/create_ad")
def create_ad():
    f = request.form
    try:
        acc_id, key = f["account"], f["key"]
        ap = acc_param_for(acc_id)
        tpl = fetch_template(acc_id, int(f["template"]))
        params = {k: tpl[k] for k in COPY_FIELDS if tpl.get(k) not in (None, "", 0, False)}
        params["target"] = input_target(tpl.get("target") or {})
        mode = f.get("target_mode")
        if mode in ("channels", "users"):  # استهداف بقنوات مختارة من صفحة الفحص بدل استهداف الإعلان الأصلي
            chans = ["@" + str(c).lstrip("@") for c in json.loads(f.get("target_channels") or "[]")][:100]
            if not chans:
                raise ValueError("لا توجد قنوات مختارة")
            if mode == "channels":
                params["target"] = {"type": "channels", "channel_ids": chans}
            else:
                country = (f.get("target_country") or "").upper()
                params["target"] = {"type": "users", "country_codes": [country] if country else [],
                                    "channel_ids": chans}
            params.pop("placement", None)  # يحدده تيليجرام من نوع الاستهداف
        if (tpl.get("website_photo") or {}).get("photo_id"):
            params["website_photo_id"] = tpl["website_photo"]["photo_id"]
        params.update(title=f["title"], text=f["text"], cpm=float(f["cpm"]),
                      initial_budget=float(f.get("budget") or 0),
                      daily_budget_limit=float(f.get("daily") or 0),
                      is_paused=f.get("paused") == "1")
        photo = request.files.get("photo")
        if photo:
            if key not in PHOTO_CACHE:
                PHOTO_CACHE[key] = client.upload("uploadAdPhoto", photo.filename, photo.read(),
                                                 photo.mimetype, **ap)["photo_id"]
            params["photo_id"] = PHOTO_CACHE[key]
        elif tpl.get("photo"):
            params["photo_id"] = tpl["photo"]["photo_id"]
        elif tpl.get("video"):
            params["video_id"] = tpl["video"]["video_id"]
        ad = client.call("createAd", idempotency_key=key, **params, **ap)
        wake.set()
        return jsonify({"ok": True, "ad_id": ad.get("ad_id"), "status": ad.get("status", "")})
    except ApiError as e:
        return jsonify({"ok": False, "error": str(e)})
    except (KeyError, ValueError) as e:
        return jsonify({"ok": False, "error": f"بيانات ناقصة أو غير صحيحة: {e}"})


# ---------------------------------------------------------------- فحص القنوات قبل الاستهداف
def parse_num(text):
    m = re.search(r"(\d[\d.,]*)\s*([KkMm]?)", text or "")
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    return int(v * {"k": 1e3, "m": 1e6}.get(m.group(2).lower(), 1))


def parse_preview(html):
    """يقرأ صفحة المعاينة العامة للقناة: عدد المشتركين والمنشورات بمشاهداتها وتفاعلاتها."""
    subs = None
    m = re.search(r'counter_value">([^<]+)</span>\s*<span class="counter_type">\s*(?:subscriber|member)', html)
    if m:
        subs = parse_num(m.group(1))
    t = re.search(r'tgme_channel_info_header_title[^>]*>(.*?)</div>', html, re.S)
    title = re.sub(r"<[^>]+>", "", t.group(1)).strip() if t else ""
    posts = []
    for block in html.split("tgme_widget_message_wrap")[1:]:
        mid = re.search(r'data-post="[^"/]+/(\d+)"', block)
        mv = re.search(r'tgme_widget_message_views">([^<]+)<', block)
        mt = re.search(r'tgme_widget_message_date"[^>]*>\s*<time[^>]*datetime="([^"]+)"', block)
        if not (mid and mv and mt):
            continue
        views = parse_num(mv.group(1))
        try:
            ts = datetime.fromisoformat(mt.group(1)).timestamp()
        except ValueError:
            continue
        reacts = 0
        for part in block.split('class="tgme_reaction')[1:]:
            reacts += parse_num(re.sub(r"<[^>]+>", " ", part[:500])) or 0
        if views is not None:
            posts.append({"id": int(mid.group(1)), "t": ts, "views": views, "reacts": reacts})
    return subs, title, posts


def fetch_preview(name):
    posts, subs, title, before = {}, None, "", None
    for _ in range(3):  # حتى ثلاث صفحات للوصول لمنشورات مرّ عليها وقت كافٍ
        r = requests.get(f"https://t.me/s/{name}", params={"before": before} if before else None, timeout=15,
                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                                "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"})
        if r.status_code == 429:  # تيليجرام يطلب التهدئة
            time.sleep(8)
            continue
        if r.status_code != 200 or "tgme_widget_message" not in r.text:
            break
        s, t, found = parse_preview(r.text)
        subs, title = subs or s, title or t
        new = [x for x in found if x["id"] not in posts]
        posts.update({x["id"]: x for x in new})
        if not new or len(posts) >= 60 or time.time() - min(x["t"] for x in posts.values()) > 36 * 3600:
            break
        before = min(posts)
        time.sleep(0.4)
    return subs, title, list(posts.values())


def demo_preview(name):
    rnd = random.Random(name)
    subs = rnd.choice([3200, 8700, 15400, 42000, 120000])
    kind = rnd.random()
    base = subs * (0.004 if kind < 0.15 else 0.02 if kind < 0.3 else rnd.uniform(0.06, 0.2))
    now = time.time()
    posts = [{"id": i, "t": now - i * 2400, "reacts": int(rnd.uniform(0, 6)),
              "views": int(base * (1 if kind > 0.9 else rnd.uniform(0.6, 1.4)))} for i in range(1, 50)]
    return subs, "قناة " + name, posts


def channel_metrics(subs, posts):
    now = time.time()
    basis, sample = "12h", [x for x in posts if now - x["t"] >= 12 * 3600]
    if len(sample) < 5:
        basis, sample = "2h", [x for x in posts if now - x["t"] >= 2 * 3600]
    if len(sample) < 3:
        basis, sample = "all", posts
    views = [x["views"] for x in sample]
    if not views:
        return {}
    mean = statistics.mean(views)
    med = statistics.median(views)
    span = max(x["t"] for x in posts) - min(x["t"] for x in posts)
    return {"median_views": int(med), "sample": len(sample), "basis": basis,
            "ratio": round(med / subs * 100, 2) if subs else None,
            "cv": round(statistics.pstdev(views) / mean, 3) if len(views) >= 5 and mean else None,
            "reacts_per_1000": round(sum(x["reacts"] for x in sample) / sum(views) * 1000, 2) if sum(views) else None,
            "posts_per_day": round((len(posts) - 1) / (span / 86400), 1) if span > 3600 else None}


CHECK_LIMIT = 2000
check_job = {"names": [], "invalid": [], "done": 0, "running": False, "stop": False}
ads_lock = threading.Lock()  # طلبات واجهة الإعلانات تمر واحدًا واحدًا حتى لا نتجاوز حدودها


def clean_channel(raw):
    name = re.sub(r"^(https?://)?(t\.me/|telegram\.me/)(s/)?", "", raw.strip()).lstrip("@").split("/")[0].split("?")[0]
    return name if re.fullmatch(r"[A-Za-z0-9_]{4,32}", name) else None


def check_one(name):
    out = {"name": name, "title": "", "ads_ok": False, "ads_error": "", "preview_ok": False}
    try:
        with ads_lock:
            ch = client.call("getTargetChannel", channel_id="@" + name) or {}
        out.update(ads_ok=True, title=ch.get("title", ""))
    except ApiError as e:
        out["ads_error"] = str(e)
    try:
        subs, title, posts = demo_preview(name) if DEMO else fetch_preview(name)
        out["title"] = out["title"] or title
        out["subs"] = subs
        if posts:
            out.update(channel_metrics(subs, posts), preview_ok=True)
        if subs:
            with db() as con:  # لقطة يومية لعدد المشتركين، ليظهر النمو مع تكرار الفحص
                con.execute("INSERT OR REPLACE INTO channel_snap VALUES(?,?,?)",
                            (name.lower(), today().isoformat(), subs))
                first = con.execute("SELECT day, subs FROM channel_snap WHERE username=? ORDER BY day LIMIT 1",
                                    (name.lower(),)).fetchone()
            out.update(first_day=first["day"], first_subs=first["subs"])
    except requests.RequestException as e:
        out["preview_error"] = str(e)
    return out


def run_check(names, invalid, force):
    """يفحص القائمة في الخلفية بأربعة خيوط، ويحفظ كل نتيجة فور وصولها."""
    check_job.update(names=names, invalid=invalid, done=0, running=True, stop=False)
    set_meta("check_names", json.dumps(names))
    set_meta("check_invalid", json.dumps(invalid))
    set_meta("check_active", "1")
    fresh = time.time() - 24 * 3600

    def work(name):
        try:
            if check_job["stop"]:
                return
            with db() as con:
                row = con.execute("SELECT data, ts FROM channel_results WHERE name=?", (name.lower(),)).fetchone()
            if not force and row and row["ts"] > fresh and json.loads(row["data"]).get("preview_ok"):
                return  # فُحصت بنجاح خلال آخر 24 ساعة
            res = check_one(name)
            with db() as con:
                con.execute("INSERT OR REPLACE INTO channel_results VALUES(?,?,?)",
                            (name.lower(), json.dumps(res, ensure_ascii=False), int(time.time())))
        except Exception:
            pass
        finally:
            check_job["done"] += 1

    with ThreadPoolExecutor(4) as pool:
        list(pool.map(work, names))
    check_job["running"] = False
    set_meta("check_active", "0")


@app.post("/api/check_start")
def check_start():
    if check_job["running"]:
        return jsonify({"ok": False, "error": "يوجد فحص يعمل الآن"})
    body = request.get_json(silent=True) or {}
    names, invalid, seen = [], [], set()
    for raw in body.get("names") or []:
        name = clean_channel(str(raw))
        if not name:
            invalid.append(str(raw)[:60])
        elif name.lower() not in seen:
            seen.add(name.lower())
            names.append(name)
    names = names[:CHECK_LIMIT]
    threading.Thread(target=run_check, args=(names, invalid[:200], bool(body.get("force"))), daemon=True).start()
    return jsonify({"ok": True, "total": len(names)})


@app.post("/api/check_stop")
def check_stop():
    check_job["stop"] = True
    return jsonify({"ok": True})


@app.get("/api/check_status")
def check_status():
    names = check_job["names"] or json.loads(get_meta("check_names", "[]") or "[]")
    invalid = check_job["invalid"] or json.loads(get_meta("check_invalid", "[]") or "[]")
    with db() as con:
        rows = {r["name"]: r["data"] for r in con.execute("SELECT name, data FROM channel_results")}
    results = [json.loads(rows[n.lower()]) for n in names if n.lower() in rows]
    return jsonify({"running": check_job["running"], "done": check_job["done"], "total": len(names),
                    "results": results, "invalid": invalid, "names": names})


# ---------------------------------------------------------------- تتبع الاشتراك والخروج عبر بوت مشرف
def chan_of(url):
    m = re.match(r"https?://(?:t|telegram)\.me/(?:s/)?([A-Za-z0-9_]{4,32})(?:[/?]|$)", url or "")
    return m.group(1).lower() if m else ""


def handle_update(u):
    cm = u.get("chat_member") or u.get("my_chat_member")
    if not cm:
        return
    chat = cm["chat"]
    with db() as con:
        con.execute("INSERT OR REPLACE INTO track_chats VALUES(?,?,?)",
                    (chat["id"], (chat.get("username") or "").lower(), chat.get("title", "")))
        if "chat_member" not in u:
            return
        inside = lambda m: m.get("status") in ("member", "administrator", "creator") or \
            (m.get("status") == "restricted" and m.get("is_member"))
        was, now_in = inside(cm["old_chat_member"]), inside(cm["new_chat_member"])
        uid, ts = cm["new_chat_member"]["user"]["id"], cm.get("date", int(time.time()))
        if now_in and not was:
            con.execute("INSERT INTO members_log VALUES(?,?,?,NULL)", (chat["id"], uid, ts))
        elif was and not now_in:
            row = con.execute("SELECT rowid FROM members_log WHERE chat_id=? AND user_id=? AND left_ts IS NULL "
                              "ORDER BY joined DESC LIMIT 1", (chat["id"], uid)).fetchone()
            if row:
                con.execute("UPDATE members_log SET left_ts=? WHERE rowid=?", (ts, row["rowid"]))


def track_loop():
    api = f"https://api.telegram.org/bot{TRACK_BOT_TOKEN}/"
    offset = int(get_meta("tg_offset", "0") or 0)
    while True:
        try:
            r = requests.get(api + "getUpdates", timeout=70, params={
                "offset": offset, "timeout": 50,
                "allowed_updates": json.dumps(["chat_member", "my_chat_member"])}).json()
            if not r.get("ok"):
                set_meta("track_error", r.get("description", "خطأ غير معروف"))
                time.sleep(20)
                continue
            set_meta("track_error", "")
            set_meta("track_seen", int(time.time()))
            for u in r["result"]:
                offset = u["update_id"] + 1
                try:
                    handle_update(u)
                except Exception:
                    pass
            if r["result"]:
                set_meta("tg_offset", offset)
        except Exception as e:
            set_meta("track_error", f"{type(e).__name__}")
            time.sleep(15)


def notify(text):
    if TRACK_BOT_TOKEN and NOTIFY_CHAT_ID:
        try:
            requests.post(f"https://api.telegram.org/bot{TRACK_BOT_TOKEN}/sendMessage", timeout=15,
                          json={"chat_id": NOTIFY_CHAT_ID, "text": text[:3900]})
        except requests.RequestException:
            pass


def real_leads(start, end):
    """الليدز الباقية لكل إعلان: ليدز كل ساعة مضروبة في نسبة من بقوا في القناة من مشتركي تلك الساعة.
    يرجع ({مفتاح الإعلان: [الباقي، إجمالي الليدز]}، مجموعة القنوات المتتبَّعة)."""
    with db() as con:
        tracked = {r["username"] for r in con.execute("SELECT username FROM track_chats WHERE username!=''")}
        if not tracked:
            return {}, tracked
        url = {(r["account_id"], r["ad_id"]): chan_of(r["promote_url"])
               for r in con.execute("SELECT account_id, ad_id, promote_url FROM ads")}
        ret = {(r["u"], r["h"]): (r["j"], r["q"]) for r in con.execute(
            "SELECT c.username u, l.joined/3600 h, COUNT(*) j, "
            "SUM(CASE WHEN l.left_ts IS NOT NULL AND l.left_ts - l.joined < ? THEN 1 ELSE 0 END) q "
            "FROM members_log l JOIN track_chats c ON c.chat_id = l.chat_id "
            "WHERE l.joined >= ? GROUP BY u, h", (STAY_MINUTES * 60, start - 3600))}
        out = {}
        for r in con.execute("SELECT account_id, ad_id, t/3600 h, SUM(actions) a FROM stats "
                             "WHERE t>=? AND t<? GROUP BY account_id, ad_id, h HAVING a > 0", (start, end)):
            key = (r["account_id"], r["ad_id"])
            u = url.get(key)
            if u not in tracked:
                continue
            j, q = ret.get((u, r["h"]), (0, 0))
            o = out.setdefault(key, [0.0, 0])
            o[0] += r["a"] * (1 - q / j) if j else r["a"]
            o[1] += r["a"]
    return out, tracked


# ---------------------------------------------------------------- الحارس: قواعد إيقاف تلقائي
GUARD_DEFAULT = {"mode": "suggest", "hours": 24, "min_views": 100, "spend_no_lead": 0, "target_cpl": 0,
                 "tolerance": 50, "min_leads": 5, "ctr_views": 30, "ctr_max": 30,
                 "leak_min_leads": 10, "leak_max": 60}
RULES = {"ctr": "نسبة نقر غير منطقية", "leak": "اشتراكات تخرج فورًا",
         "no_lead": "صرف بدون ليدز", "cpl": "تكلفة الليد أعلى من الهدف"}


def guard_settings():
    try:
        saved = json.loads(get_meta("guard", "{}") or "{}")
    except ValueError:
        saved = {}
    return {**GUARD_DEFAULT, **{k: v for k, v in saved.items() if k in GUARD_DEFAULT}}


def pause_ad(acc_id, ad_id, paused):
    ad = client.call("editAd", ad_id=ad_id, is_paused=paused, **acc_param_for(acc_id)) or {}
    with db() as con:
        con.execute("UPDATE ads SET status=? WHERE account_id=? AND ad_id=?",
                    (ad.get("status") or ("on_hold" if paused else "active"), acc_id, ad_id))


def guard_run():
    g = guard_settings()
    if g["mode"] == "off":
        return
    now = int(time.time())
    start = now - int(g["hours"]) * 3600
    real, tracked = real_leads(start, now + STEP)
    with db() as con:
        ads = con.execute("SELECT * FROM ads WHERE status='active'").fetchall()
        tot = {(r["account_id"], r["ad_id"]): r for r in con.execute(
            "SELECT account_id, ad_id, SUM(views) v, SUM(clicks) c, SUM(actions) a, SUM(spent) s "
            "FROM stats WHERE t>=? GROUP BY account_id, ad_id", (start,))}
        cool = {(r["account_id"], r["ad_id"]) for r in con.execute(
            "SELECT account_id, ad_id FROM guard_log WHERE ts>?", (now - 86400,))}
        cool |= {(r["account_id"], r["ad_id"]) for r in con.execute("SELECT account_id, ad_id FROM camp_ads")}
    lines = []
    for ad in ads:
        key = (ad["account_id"], ad["ad_id"])
        t = tot.get(key)
        if key in cool or not t:  # لا يتكرر القرار على نفس الإعلان خلال 24 ساعة
            continue
        v, c, a, s = t["v"] or 0, t["c"] or 0, t["a"] or 0, t["s"] or 0
        is_tracked = chan_of(ad["promote_url"]) in tracked
        leads = real.get(key, [0.0, 0])[0] if is_tracked else a
        leak = (1 - leads / a) * 100 if is_tracked and a else 0
        kind = "الباقية" if is_tracked else ""
        rule = reason = None
        if v >= g["ctr_views"] and c / v * 100 >= g["ctr_max"]:
            rule, reason = "ctr", f"نسبة النقر {c / v * 100:.0f}% على {v} مشاهدة"
        elif is_tracked and a >= g["leak_min_leads"] and leak >= g["leak_max"]:
            rule, reason = "leak", f"{leak:.0f}% من {a} مشترك خرجوا خلال {STAY_MINUTES} دقيقة"
        elif g["spend_no_lead"] > 0 and leads < 0.5 and s >= g["spend_no_lead"]:
            rule, reason = "no_lead", f"صرف {s:.4f} على {v} مشاهدة بدون ليدز {kind}".strip()
        elif g["target_cpl"] > 0 and leads >= g["min_leads"] and v >= g["min_views"] \
                and s / leads > g["target_cpl"] * (1 + g["tolerance"] / 100):
            rule, reason = "cpl", (f"تكلفة الليد {s / leads:.4f} من {leads:.0f} ليد {kind}".strip()
                                   + f"، والهدف {g['target_cpl']}")
        if not rule:
            continue
        state = "suggested"
        if g["mode"] == "auto":
            try:
                pause_ad(ad["account_id"], ad["ad_id"], True)
                state = "paused"
            except ApiError as e:
                state, reason = "failed", f"{reason} (تعذر الإيقاف: {e})"
        with db() as con:
            con.execute("INSERT INTO guard_log(ts, account_id, ad_id, title, rule, reason, state) VALUES(?,?,?,?,?,?,?)",
                        (now, ad["account_id"], ad["ad_id"], ad["title"], rule, reason, state))
        verb = {"paused": "تم إيقاف", "suggested": "اقتراح بإيقاف", "failed": "تعذر إيقاف"}[state]
        lines.append(f"{verb}: {ad['title']}\n{RULES[rule]}: {reason}")
    if lines:
        notify("حارس الإعلانات\n\n" + "\n\n".join(lines))


@app.get("/api/guard")
def guard_get():
    now = int(time.time())
    with db() as con:
        log = [dict(r) for r in con.execute("SELECT * FROM guard_log ORDER BY id DESC LIMIT 200")]
        chats = [dict(r) for r in con.execute(
            "SELECT c.chat_id, c.username, c.title, COUNT(l.joined) joins, "
            "COALESCE(SUM(CASE WHEN l.left_ts IS NOT NULL AND l.left_ts - l.joined < ? THEN 1 ELSE 0 END), 0) quick "
            "FROM track_chats c LEFT JOIN members_log l ON l.chat_id = c.chat_id AND l.joined > ? "
            "GROUP BY c.chat_id ORDER BY joins DESC", (STAY_MINUTES * 60, now - 86400))]
    return jsonify({"settings": guard_settings(), "log": log, "rules": RULES, "chats": chats,
                    "bot": bool(TRACK_BOT_TOKEN) or DEMO, "notify": bool(TRACK_BOT_TOKEN and NOTIFY_CHAT_ID),
                    "env_names": sorted(k for k in os.environ if re.search(r"TRACK|BOT|NOTIFY|TOKEN", k, re.I)),
                    "track_error": get_meta("track_error"), "guard_error": get_meta("guard_error"),
                    "stay_minutes": STAY_MINUTES})


@app.post("/api/guard_settings")
def guard_set():
    body = request.get_json(silent=True) or {}
    g = guard_settings()
    for k, default in GUARD_DEFAULT.items():
        if k not in body:
            continue
        if k == "mode":
            if body[k] in ("off", "suggest", "auto"):
                g[k] = body[k]
        else:
            try:
                g[k] = max(0.0, float(body[k] or 0))
            except (TypeError, ValueError):
                pass
    set_meta("guard", json.dumps(g))
    return jsonify({"ok": True, "settings": g})


@app.post("/api/guard_act")
def guard_act():
    body = request.get_json(silent=True) or {}
    with db() as con:
        row = con.execute("SELECT * FROM guard_log WHERE id=?", (int(body.get("id", 0)),)).fetchone()
    if not row:
        return jsonify({"ok": False, "error": "القرار غير موجود"})
    act = body.get("action")
    try:
        if act == "apply" and row["state"] in ("suggested", "failed", "undone"):
            pause_ad(row["account_id"], row["ad_id"], True)
            state = "applied"
        elif act == "undo" and row["state"] in ("paused", "applied"):
            pause_ad(row["account_id"], row["ad_id"], False)
            state = "undone"
        elif act == "dismiss" and row["state"] in ("suggested", "failed"):
            state = "dismissed"
        else:
            return jsonify({"ok": False, "error": "الإجراء لا يناسب حالة القرار"})
    except ApiError as e:
        return jsonify({"ok": False, "error": str(e)})
    with db() as con:
        con.execute("UPDATE guard_log SET state=? WHERE id=?", (state, row["id"]))
    return jsonify({"ok": True, "state": state})


# ---------------------------------------------------------------- الحملات: تجارب تلقائية حتى تحقيق هدف الليدز
MIN_LEADS, WIN, LOSE, MAX_TESTS, NEW_PER_CYCLE = 5, 1.2, 1.5, 10, 5


def camp_log(cid, text):
    with db() as con:
        con.execute("INSERT INTO camp_log(camp_id, ts, text) VALUES(?,?,?)", (cid, int(time.time()), text))


def cur_round(currency, x):
    return float(max(1, round(x))) if currency == "XTR" else max(0.01, round(x, 2))


def sums_since(con, acc, start):
    return {r["ad_id"]: r for r in con.execute(
        "SELECT ad_id, SUM(views) v, SUM(clicks) c, SUM(actions) a, SUM(spent) s FROM stats "
        "WHERE account_id=? AND t>=? GROUP BY ad_id", (acc, start))}


def camp_view(c):
    """يحسب أرقام كل إعلانات الحملة: منذ إنشائها، آخر 24 ساعة، واليوم."""
    now, acc = int(time.time()), c["account_id"]
    with db() as con:
        cads = [dict(r) for r in con.execute("SELECT * FROM camp_ads WHERE camp_id=? ORDER BY created", (c["id"],))]
        info = {r["ad_id"]: r for r in con.execute(
            "SELECT ad_id, title, status, remaining, promote_url FROM ads WHERE account_id=?", (acc,))}
        start = min([a["created"] for a in cads] + [c["created"]])
        windows = {"life": sums_since(con, acc, start), "day": sums_since(con, acc, day_start(today())),
                   "h24": sums_since(con, acc, now - 86400)}
    reals = {"life": real_leads(start, now + STEP), "h24": real_leads(now - 86400, now + STEP),
             "day": real_leads(day_start(today()), now + STEP)}
    tracked = reals["life"][1]
    for a in cads:
        i = info.get(a["ad_id"])
        a["status"] = i["status"] if i else ""
        a["title"] = i["title"] if i else ""
        a["remaining"] = i["remaining"] if i else 0
        a["tracked"] = bool(i) and chan_of(i["promote_url"]) in tracked
        for w, table in windows.items():
            t = table.get(a["ad_id"])
            v, cl, act, sp = ((t["v"] or 0, t["c"] or 0, t["a"] or 0, t["s"] or 0) if t else (0, 0, 0, 0.0))
            leads = reals[w][0].get((acc, a["ad_id"]), [0.0, 0])[0] if a["tracked"] else act
            a[w] = {"views": v, "clicks": cl, "actions": act, "spent": round(sp, 5), "leads": round(leads, 1),
                    "cpl": round(sp / leads, 6) if leads > 0 else None}
    return cads


def camp_stop_ad(c, a, reason, call=True):
    if call and a["ad_id"] > 0:
        try:
            pause_ad(c["account_id"], a["ad_id"], True)
        except ApiError as e:
            reason += f" (تعذر الإيقاف: {e})"
    with db() as con:
        con.execute("UPDATE camp_ads SET state='stopped', stopped=?, reason=? WHERE account_id=? AND ad_id=?",
                    (int(time.time()), reason, c["account_id"], a["ad_id"]))
    a["state"] = "stopped"
    camp_log(c["id"], f"إيقاف «{a['title'] or a['ad_id']}»: {reason}")


def camp_create_ad(c, tpl, ap, creative, group, limit):
    cid, acc = c["id"], c["account_id"]
    title = f"{c['name']} ن{creative['n']}-م{group['idx']}"[:120]
    try:
        params = {k: tpl[k] for k in COPY_FIELDS if tpl.get(k) not in (None, "", 0, False)}
        if (tpl.get("website_photo") or {}).get("photo_id"):
            params["website_photo_id"] = tpl["website_photo"]["photo_id"]
        params.pop("placement", None)
        chans = json.loads(group["channels"])
        params["target"] = {"type": "channels", "channel_ids": chans} if c["mode"] == "channels" else \
            {"type": "users", "country_codes": [c["country"]] if c["country"] else [], "channel_ids": chans}
        if creative["photo"] is not None:
            photo_id = creative["photo_id"]
            if not photo_id:
                photo_id = client.upload("uploadAdPhoto", creative["photo_name"], bytes(creative["photo"]),
                                         creative["photo_mime"], **ap)["photo_id"]
                with db() as con:
                    con.execute("UPDATE camp_creatives SET photo_id=? WHERE id=?", (photo_id, creative["id"]))
            params["photo_id"] = photo_id
        params.update(title=title, text=creative["text"], cpm=c["cpm"], daily_budget_limit=limit,
                      initial_budget=limit, is_paused=False)
        ad = client.call("createAd", idempotency_key=f"camp{cid}-{creative['id']}-{group['id']}", **params, **ap)
        ad_id, created, state, reason = ad["ad_id"], ad.get("created_date") or int(time.time()), "testing", ""
        camp_log(cid, f"إعلان تجريبي جديد «{title}» بحد يومي {limit}")
    except FloodWait:  # مهلة مؤقتة من تيليجرام: التركيبة تبقى في الانتظار ولا تُحسب فاشلة
        camp_log(cid, f"تيليجرام طلب الانتظار قبل إنشاء إعلانات جديدة حتى {clock(flood_until('createAd'))}. "
                      "الحملة تكمل تلقائيًا بعدها.")
        return None
    except ApiError as e:  # نسجل التركيبة كفاشلة حتى لا تتكرر المحاولة كل دورة
        ad_id, created, state, reason = -(creative["id"] * 100000 + group["id"]), int(time.time()), "stopped", f"تعذر الإنشاء: {e}"
        camp_log(cid, f"تعذر إنشاء «{title}»: {e}")
    with db() as con:
        con.execute("INSERT OR REPLACE INTO camp_ads VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (cid, acc, ad_id, creative["id"], group["id"], state, limit, created, 0,
                     created if state == "stopped" else 0, 1 if state == "stopped" else 0, reason))
    return state == "testing"


def run_campaign(c):
    cid, acc, now = c["id"], c["account_id"], int(time.time())
    ap = acc_param_for(acc)
    budget, target = c["daily_budget"], c["daily_budget"] / c["target_leads"]
    test_limit = cur_round(c["currency"], 6 * target)
    cads = camp_view(c)
    live = [a for a in cads if a["state"] in ("testing", "winner")]

    # 1) حد الأمان: لو صرف اليوم وصل الحد، كل شيء يتوقف حتى الغد
    spent_today = sum(a["day"]["spent"] for a in cads)
    today_iso = today().isoformat()
    if c["capped"] and c["capped"] != today_iso:
        for a in live:
            pause_ad(acc, a["ad_id"], False)
        with db() as con:
            con.execute("UPDATE campaigns SET capped='' WHERE id=?", (cid,))
        camp_log(cid, "يوم جديد: إعادة تشغيل إعلانات الحملة")
    elif not c["capped"] and spent_today >= budget:
        for a in live:
            pause_ad(acc, a["ad_id"], True)
        with db() as con:
            con.execute("UPDATE campaigns SET capped=? WHERE id=?", (today_iso, cid))
        camp_log(cid, f"صرف اليوم {spent_today:.4f} وصل الحد اليومي، توقفت كل الإعلانات حتى الغد")
        notify(f"حملة {c['name']}: وصل صرف اليوم للحد {budget}، توقفت الإعلانات حتى الغد.")
        return
    elif c["capped"]:
        return

    # 2) الحكم على كل إعلان
    for a in live:
        if a["status"] in ("declined", "deleted"):
            camp_stop_ad(c, a, "رفضته مراجعة تيليجرام" if a["status"] == "declined" else "حُذف", call=False)
            continue
        if a["status"] not in ("active", "on_hold", "stopped"):
            continue  # ما زال في المراجعة أو لم تصل بياناته
        w = a["life"] if a["state"] == "testing" else a["h24"]
        v, cl, act, s, leads, cpl = w["views"], w["clicks"], w["actions"], w["spent"], w["leads"], w["cpl"]
        if v >= 30 and cl / v >= 0.3:
            camp_stop_ad(c, a, f"نسبة نقر غير منطقية {cl / v * 100:.0f}%")
        elif a["tracked"] and act >= 10 and 1 - leads / act >= 0.6:
            camp_stop_ad(c, a, f"{(1 - leads / act) * 100:.0f}% من المشتركين خرجوا فورًا")
        elif a["state"] == "testing":
            if leads >= MIN_LEADS and cpl <= target * WIN:
                with db() as con:
                    con.execute("UPDATE camp_ads SET state='winner' WHERE account_id=? AND ad_id=?", (acc, a["ad_id"]))
                a["state"] = "winner"
                camp_log(cid, f"ناجح «{a['title']}»: {leads:.0f} ليد بتكلفة {cpl:.4f}")
            elif leads >= MIN_LEADS and (cpl > target * LOSE or s >= 8 * target):
                camp_stop_ad(c, a, f"تكلفة الليد {cpl:.4f} أعلى من الهدف {target:.4f}")
            elif leads < 1 and s >= 3 * target:
                camp_stop_ad(c, a, f"صرف {s:.4f} بدون ليدز")
            elif leads < MIN_LEADS and s >= 6 * target:
                camp_stop_ad(c, a, f"{leads:.0f} ليد فقط بعد صرف {s:.4f}")
        elif leads >= MIN_LEADS and cpl > target * LOSE:
            camp_stop_ad(c, a, f"تراجع أداؤه: تكلفة الليد آخر 24 ساعة {cpl:.4f}")

    live = [a for a in cads if a["state"] in ("testing", "winner")]
    committed = sum(a["day"]["spent"] for a in cads if a["state"] == "stopped") + \
        sum(max(a["daily_limit"], a["day"]["spent"]) for a in live)

    # 3) تكبير الناجحين: من استهلك أغلب حده اليومي يُرفع حده في حدود المتاح
    winners = sorted([a for a in live if a["state"] == "winner"], key=lambda a: a["h24"]["cpl"] or 9e9)
    for a in winners:
        if a["day"]["spent"] >= 0.7 * a["daily_limit"] and now - (a["raised"] or 0) > 3 * 3600:
            new = cur_round(c["currency"], a["daily_limit"] * 1.5)
            if committed + new - a["daily_limit"] <= budget:
                client.call("editAd", ad_id=a["ad_id"], daily_budget_limit=new, **ap)
                with db() as con:
                    con.execute("UPDATE camp_ads SET daily_limit=?, raised=? WHERE account_id=? AND ad_id=?",
                                (new, now, acc, a["ad_id"]))
                committed += new - a["daily_limit"]
                camp_log(cid, f"رفع الحد اليومي لـ«{a['title']}» من {a['daily_limit']} إلى {new}")
                a["daily_limit"] = new

    # 4) تغذية ميزانية الإعلانات العاملة
    for a in live:
        if a["status"] == "active" and a["remaining"] < 0.5 * a["daily_limit"]:
            try:
                client.call("increaseAdBudget", ad_id=a["ad_id"], amount=a["daily_limit"],
                            idempotency_key=f"top{a['ad_id']}-{now // 3600}", **ap)
            except ApiError as e:
                camp_log(cid, f"تعذر تزويد ميزانية «{a['title']}»: {e}")

    # 5) هل تحقق الهدف؟
    w_leads = sum(a["h24"]["leads"] for a in winners)
    w_spent = sum(a["h24"]["spent"] for a in winners)
    state = c["state"]
    if w_leads >= c["target_leads"] and w_spent / w_leads <= target * 1.05:
        state = "stable"
    elif state == "stable" and w_leads < 0.8 * c["target_leads"]:
        state = "running"

    # 6) تجارب جديدة مكان ما توقف
    with db() as con:
        creatives = [dict(r) for r in con.execute("SELECT * FROM camp_creatives WHERE camp_id=? ORDER BY id", (cid,))]
        groups = [dict(r) for r in con.execute("SELECT * FROM camp_groups WHERE camp_id=? ORDER BY idx", (cid,))]
    for n, cr in enumerate(creatives, 1):
        cr["n"] = n
    done = {(a["creative_id"], a["group_id"]) for a in cads}
    pool = [(cr, g) for g in groups for cr in creatives if (cr["id"], g["id"]) not in done]
    testing = sum(1 for a in live if a["state"] == "testing")
    if state == "running" and pool and flood_until("createAd"):
        pass  # ننتظر انتهاء مهلة تيليجرام
    elif state == "running" and pool:
        tpl, made = None, 0
        while pool and made < NEW_PER_CYCLE and testing < MAX_TESTS and committed + test_limit <= budget:
            tpl = tpl or fetch_template(acc, c["template"])
            cr, g = pool.pop(0)
            ok = camp_create_ad(c, tpl, ap, cr, g, test_limit)
            if ok is None:
                break
            if ok:
                testing += 1
                committed += test_limit
                made += 1
        if made:
            wake.set()
    elif state == "running" and not pool and not testing:
        state = "exhausted"
    if state != c["state"]:
        with db() as con:
            con.execute("UPDATE campaigns SET state=? WHERE id=?", (state, cid))
        msg = {"stable": f"تحقق الهدف: {w_leads:.0f} ليد في آخر 24 ساعة من {len(winners)} إعلان ناجح.",
               "running": "تراجع الأداء عن الهدف، عادت التجارب.",
               "exhausted": f"جُرّبت كل التركيبات ولم يتحقق الهدف. الناجحون يجلبون {w_leads:.0f} ليد يوميًا. "
                            "الحملة تحتاج نصوصًا أو صورًا أو قنوات جديدة."}[state]
        camp_log(cid, msg)
        notify(f"حملة {c['name']}: {msg}")


def repair_flood():
    """تركيبات سُجّلت فاشلة بسبب مهلة انتظار مؤقتة: تعود للانتظار وتُكمل الحملة بعد المهلة."""
    with db() as con:
        rows = con.execute("SELECT camp_id, created, reason FROM camp_ads WHERE ad_id<0 AND reason LIKE '%FLOOD_WAIT%'").fetchall()
        if not rows:
            return
        until = 0
        for r in rows:
            m = re.search(r"FLOOD_WAIT_(\d+)", r["reason"])
            until = max(until, r["created"] + int(m.group(1)) if m else 0)
        ids = sorted({r["camp_id"] for r in rows})
        con.execute("DELETE FROM camp_ads WHERE ad_id<0 AND reason LIKE '%FLOOD_WAIT%'")
        for i in ids:
            con.execute("UPDATE campaigns SET state='running' WHERE id=? AND state='exhausted'", (i,))
    if until > flood_until("createAd"):
        flood_set("createAd", until)
    for i in ids:
        camp_log(i, "التركيبات لم تفشل، تيليجرام طلب مهلة انتظار مؤقتة"
                    + (f" حتى {clock(until)}" if until > time.time() else "") + ". الحملة تكمل تلقائيًا بعدها.")


def campaigns_run():
    with db() as con:
        camps = con.execute("SELECT * FROM campaigns WHERE state IN ('running','stable','exhausted')").fetchall()
        # استرجاع ما تبقى في ميزانية الإعلانات الموقوفة بعد مرور مهلة تيليجرام
        back = con.execute(
            "SELECT k.camp_id, k.account_id, k.ad_id, a.remaining FROM camp_ads k JOIN ads a "
            "ON a.account_id=k.account_id AND a.ad_id=k.ad_id WHERE k.state='stopped' AND k.reclaimed=0 "
            "AND k.stopped<? AND k.ad_id>0", (int(time.time()) - 900,)).fetchall()
    for r in back:
        try:
            if r["remaining"] and r["remaining"] > 0:
                client.call("decreaseAdBudget", ad_id=r["ad_id"], amount=r["remaining"],
                            idempotency_key=f"back{r['ad_id']}", **acc_param_for(r["account_id"]))
                camp_log(r["camp_id"], f"استرجاع {r['remaining']} من إعلان موقوف إلى رصيد الحساب")
            with db() as con:
                con.execute("UPDATE camp_ads SET reclaimed=1 WHERE account_id=? AND ad_id=?", (r["account_id"], r["ad_id"]))
        except ApiError:
            pass
    for c in camps:
        try:
            run_campaign(dict(c))
        except ApiError as e:
            camp_log(c["id"], f"خطأ من تيليجرام أوقف دورة الحملة: {e}")


@app.post("/api/camp_create")
def camp_create():
    f = request.form
    try:
        acc_id, tpl_id = f["template"].split("|")
        texts = [t.strip() for t in json.loads(f["texts"]) if t.strip()]
        chans = []
        for raw in json.loads(f["channels"]):
            name = clean_channel(str(raw))
            if name and "@" + name not in chans:
                chans.append("@" + name)
        leads, budget, cpm = float(f["target_leads"]), float(f["daily_budget"]), float(f["cpm"])
        photos = request.files.getlist("photos")
        if not texts or not chans or min(leads, budget, cpm) <= 0:
            raise ValueError("النصوص والقنوات والأرقام مطلوبة")
        if any(len(t) > 160 for t in texts):
            raise ValueError("يوجد نص أطول من 160 حرفًا")
        if len(photos) > len(texts):
            raise ValueError("عدد الصور أكبر من عدد النصوص")
        acc_param_for(acc_id)
        with db() as con:
            cur = con.execute("SELECT currency FROM accounts WHERE account_id=?", (acc_id,)).fetchone()["currency"]
            cid = con.execute(
                "INSERT INTO campaigns(name, account_id, template, target_leads, daily_budget, cpm, mode, country, "
                "currency, state, capped, created) VALUES(?,?,?,?,?,?,?,?,?,'running','',?)",
                (f.get("name") or "حملة", acc_id, int(tpl_id), leads, budget, cpm,
                 "users" if f.get("mode") == "users" else "channels", (f.get("country") or "").upper(), cur,
                 int(time.time()))).lastrowid
            for i, text in enumerate(texts):
                ph = photos[i] if i < len(photos) else None
                con.execute("INSERT INTO camp_creatives(camp_id, text, photo, photo_name, photo_mime, photo_id) "
                            "VALUES(?,?,?,?,?,'')", (cid, text, ph.read() if ph else None,
                                                     ph.filename if ph else "", ph.mimetype if ph else ""))
            n = -(-len(chans) // 100)
            size = -(-len(chans) // n)
            for i in range(n):
                con.execute("INSERT INTO camp_groups(camp_id, idx, channels) VALUES(?,?,?)",
                            (cid, i + 1, json.dumps(chans[i * size:(i + 1) * size])))
        camp_log(cid, f"بدأت الحملة: الهدف {leads:.0f} ليد يوميًا بحد {budget}، تكلفة الليد المستهدفة {budget / leads:.4f}")
        wake.set()
        return jsonify({"ok": True, "id": cid})
    except ApiError as e:
        return jsonify({"ok": False, "error": str(e)})
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"ok": False, "error": f"بيانات ناقصة أو غير صحيحة: {e}"})


@app.get("/api/camps")
def camps_get():
    with db() as con:
        camps = [dict(r) for r in con.execute("SELECT * FROM campaigns ORDER BY id DESC")]
    for c in camps:
        with db() as con:
            cre = {r["id"]: r for r in con.execute(
                "SELECT id, text, photo IS NOT NULL has_photo FROM camp_creatives WHERE camp_id=? ORDER BY id", (c["id"],))}
            grp = {r["id"]: r for r in con.execute("SELECT id, idx, channels FROM camp_groups WHERE camp_id=?", (c["id"],))}
            c["log"] = [dict(r) for r in con.execute(
                "SELECT ts, text FROM camp_log WHERE camp_id=? ORDER BY id DESC LIMIT 40", (c["id"],))]
            t = con.execute("SELECT promote_url FROM ads WHERE account_id=? AND ad_id=?", (c["account_id"], c["template"])).fetchone()
        c["promote_url"] = t["promote_url"] if t else ""
        ads = camp_view(c)
        for a in ads:
            cr, g = cre.get(a["creative_id"]), grp.get(a["group_id"])
            a["text"] = cr["text"] if cr else ""
            a["has_photo"] = bool(cr and cr["has_photo"])
            a["group"] = g["idx"] if g else 0
        live = [a for a in ads if a["state"] in ("testing", "winner")]
        c.update(ads=ads, creatives=len(cre), groups=len(grp),
                 channels=sum(len(json.loads(g["channels"])) for g in grp.values()),
                 pool=len(cre) * len(grp) - len(ads), target_cpl=round(c["daily_budget"] / c["target_leads"], 6),
                 today_spent=round(sum(a["day"]["spent"] for a in ads), 5),
                 today_leads=round(sum(a["day"]["leads"] for a in ads), 1),
                 winners_leads24=round(sum(a["h24"]["leads"] for a in live if a["state"] == "winner"), 1))
    return jsonify({"camps": camps, "error": get_meta("camp_error"), "create_wait": flood_until("createAd")})


@app.post("/api/camp_act")
def camp_act():
    body = request.get_json(silent=True) or {}
    with db() as con:
        c = con.execute("SELECT * FROM campaigns WHERE id=?", (int(body.get("id", 0)),)).fetchone()
        if not c:
            return jsonify({"ok": False, "error": "الحملة غير موجودة"})
        live = con.execute("SELECT ad_id FROM camp_ads WHERE camp_id=? AND state IN ('testing','winner') AND ad_id>0",
                           (c["id"],)).fetchall()
    act = body.get("action")
    if act not in ("pause", "resume", "stop"):
        return jsonify({"ok": False, "error": "إجراء غير معروف"})
    errors = []
    for a in live:
        try:
            pause_ad(c["account_id"], a["ad_id"], act != "resume")
        except ApiError as e:
            errors.append(str(e))
    with db() as con:
        con.execute("UPDATE campaigns SET state=?, capped='' WHERE id=?",
                    ({"pause": "paused", "resume": "running", "stop": "ended"}[act], c["id"]))
        if act == "stop":
            con.execute("UPDATE camp_ads SET state='stopped', stopped=?, reason='أُنهيت الحملة' "
                        "WHERE camp_id=? AND state IN ('testing','winner')", (int(time.time()), c["id"]))
    camp_log(c["id"], {"pause": "أوقفتِ الحملة مؤقتًا", "resume": "استأنفتِ الحملة", "stop": "أنهيتِ الحملة"}[act])
    return jsonify({"ok": not errors, "error": "، ".join(errors[:3])})


def demo_seed_tracking():
    """بيانات اشتراك وخروج وهمية للوضع التجريبي: قناة طبيعية وقناة يخرج أغلب مشتركيها فورًا."""
    with db() as con:
        if con.execute("SELECT 1 FROM members_log LIMIT 1").fetchone():
            return
        con.execute("INSERT OR REPLACE INTO track_chats VALUES(1,'example','قناة العروض')")
        con.execute("INSERT OR REPLACE INTO track_chats VALUES(2,'badchan','قناة عليها حركة وهمية')")
        rnd, now, rows, uid = random.Random(7), int(time.time()), [], 0
        for chat, leave in ((1, 0.12), (2, 0.8)):
            for h in range(72):
                for _ in range(rnd.randint(8, 30)):
                    uid += 1
                    j = now - h * 3600 - rnd.randint(0, 3500)
                    rows.append((chat, uid, j, j + rnd.randint(3, 240) if rnd.random() < leave else None))
        con.executemany("INSERT INTO members_log VALUES(?,?,?,?)", rows)


@app.post("/api/bulk")
def bulk():
    """إيقاف أو تشغيل أو حذف مجموعة إعلانات، مع نتيجة مستقلة لكل إعلان."""
    body = request.get_json(silent=True) or {}
    action = body.get("action")
    if action not in ("pause", "resume", "delete"):
        return jsonify({"error": "إجراء غير معروف"}), 400
    out = []
    for it in (body.get("items") or [])[:300]:
        acc_id, ad_id = str(it.get("account", "")), int(it.get("ad_id", 0))
        try:
            ap = acc_param_for(acc_id)
            result = ""
            if action == "delete":
                try:  # لو الإعلان متوقف من مدة كافية يُحذف فورًا
                    client.call("deleteAd", ad_id=ad_id, **ap)
                    status = "deleted"
                except ApiError as first:
                    if "NOT_FOUND" in str(first):
                        raise
                    try:  # وإلا نوقفه الآن ونؤجل حذفه حتى تمر المهلة
                        client.call("editAd", ad_id=ad_id, is_paused=True, **ap)
                    except ApiError as second:
                        raise ApiError(f"{first} / {second}")
                    status, result = "on_hold", "pending"
            else:
                ad = client.call("editAd", ad_id=ad_id, is_paused=(action == "pause"), **ap) or {}
                status = ad.get("status", "")
            with db() as con:
                if status:
                    con.execute("UPDATE ads SET status=? WHERE account_id=? AND ad_id=?", (status, acc_id, ad_id))
                if result == "pending":
                    con.execute("INSERT OR REPLACE INTO pending_deletes VALUES(?,?,?,0,'')",
                                (acc_id, ad_id, int(time.time()) + DELETE_WAIT))
                else:  # التشغيل أو الإيقاف اليدوي أو الحذف الفوري يلغي أي حذف مؤجل سابق
                    con.execute("DELETE FROM pending_deletes WHERE account_id=? AND ad_id=?", (acc_id, ad_id))
            status = result or status
            out.append({"account": acc_id, "ad_id": ad_id, "ok": True, "status": status})
        except ApiError as e:
            out.append({"account": acc_id, "ad_id": ad_id, "ok": False, "error": str(e)})
    wake.set()
    return jsonify({"results": out})


@app.post("/api/sync")
def sync_now():
    wake.set()
    return jsonify({"ok": True})


if __name__ == "__main__":
    prepare_db_path()
    init_db()
    repair_flood()
    threading.Thread(target=sync_loop, daemon=True).start()
    threading.Thread(target=delete_loop, daemon=True).start()
    if DEMO:
        demo_seed_tracking()
    elif TRACK_BOT_TOKEN:
        threading.Thread(target=track_loop, daemon=True).start()
    if get_meta("check_active") == "1":  # فحص قنوات انقطع بإعادة تشغيل الخدمة، نكمله
        threading.Thread(target=run_check, daemon=True, args=(
            json.loads(get_meta("check_names", "[]") or "[]"),
            json.loads(get_meta("check_invalid", "[]") or "[]"), False)).start()
    print("تتبع الاشتراك والخروج:", "مفعّل" if TRACK_BOT_TOKEN else "غير مفعّل (المتغير TRACK_BOT_TOKEN غير موجود)", flush=True)
    if DEMO:
        print("لا يوجد رمز وصول: التشغيل ببيانات تجريبية.")
    if not DASH_PASSWORD:
        print("تنبيه: الصفحة من غير كلمة سر. حددي DASH_PASSWORD قبل وضعها على الإنترنت.")
    print(f"افتحي http://localhost:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
