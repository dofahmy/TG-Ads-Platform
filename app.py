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
import math
import os
import random
import sqlite3
import threading
import time
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
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(HERE, "demo.db" if DEMO else "ads.db"))

STEP = 300            # أصغر فاصل تسمح به الواجهة: خمس دقائق
CHUNK = 1000 * STEP   # أقصى فترة في الطلب الواحد
OVERLAP = 2 * 3600    # نعيد سحب آخر ساعتين كل مرة لأن الأرقام الحديثة بتتعدل
REQUEST_GAP = 0.15    # مهلة بين الطلبات، التوثيق لا يذكر حدًا لعددها


# ---------------------------------------------------------------- الاتصال بتيليجرام
class ApiError(Exception):
    pass


class TelegramAds:
    def __init__(self, token):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {token}"

    def call(self, method, **params):
        last = "NETWORK_ERROR"
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
                    "promote_url": "https://t.me/example", "placement": "channel_post",
                    "views": now // STEP * (base or 1) // 50, "clicks": 0, "actions": 0,
                    "spent_budget": round(now // STEP * (base or 0) / 50000 * cpm, 2),
                    "remaining_budget": 25.0, "daily_budget_limit": 15.0, "action_type": "join",
                    "created_date": now - 40 * 86400, "is_paused": status == "on_hold",
                    **({"decline_reason": {"text": "النص مخالف لسياسة الإعلانات"}} if status == "declined" else {}),
                })
            return {"total_count": len(ads), "ads": ads}
        if method == "getAdStats":
            ad = next(a for a in self.ADS if a[0] == p["ad_id"])
            out = []
            for t in range(p["from_time"], min(p["to_time"], now // STEP * STEP + STEP), STEP):
                b = self._bucket(ad, t)
                if b:
                    out.append(b)
            return out
        if method == "getAdsById":
            ad = next((a for a in self.call("getAdsList")["ads"] if a["ad_id"] in p["ad_ids"]), None)
            if ad:
                ad["target"] = {"type": "channels", "channels": [
                    {"channel_id": 1, "username": "deals_eg", "title": "عروض مصر"},
                    {"channel_id": 2, "username": "offers_sa", "title": "عروض السعودية"}]}
            return [ad] if ad else []
        if method == "createAd":
            if len(p.get("text", "")) > 160:
                raise ApiError("AD_TEXT_TOO_LONG")
            new_id = max(a[0] for a in self.ADS) + 1
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
            set_meta("last_error", "")
        except Exception as e:  # نسجل الخطأ ونكمل في الدورة الجاية
            set_meta("last_error", f"{type(e).__name__}: {e}")
        syncing.clear()
        wake.wait(SYNC_MINUTES * 60)
        wake.clear()


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
    return send_from_directory(HERE, "index.html")


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
    acc_title = {a["account_id"]: a["title"] for a in accounts}
    out = []
    for ad in ads:
        key = (ad["account_id"], ad["ad_id"])
        t = totals.get(key)
        ad["m"] = metrics(t["v"], t["c"], t["a"], t["s"]) if t else metrics(0, 0, 0, 0)
        h = hours.get(key, {})
        ad["strip"] = [h.get(ts // 3600, (0, 0, 0)) for ts in strip_ts]
        ad["account_title"] = acc_title.get(ad["account_id"], "")
        out.append(ad)
    return jsonify({
        "demo": DEMO, "syncing": syncing.is_set(), "last_sync": int(get_meta("last_sync", "0") or 0),
        "last_error": get_meta("last_error"), "sync_minutes": SYNC_MINUTES,
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


@app.post("/api/sync")
def sync_now():
    wake.set()
    return jsonify({"ok": True})


if __name__ == "__main__":
    prepare_db_path()
    init_db()
    threading.Thread(target=sync_loop, daemon=True).start()
    if DEMO:
        print("لا يوجد رمز وصول: التشغيل ببيانات تجريبية.")
    if not DASH_PASSWORD:
        print("تنبيه: الصفحة من غير كلمة سر. حددي DASH_PASSWORD قبل وضعها على الإنترنت.")
    print(f"افتحي http://localhost:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
