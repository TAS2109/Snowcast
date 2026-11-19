# -*- coding: utf-8 -*-
"""
SNOW CAST  -  複数予報モデル統合による降雪確率サイト (Python / Flask)

対象: 甲府市（初期）/ 予報期間: 11日先まで

データ源
  - 予報モデル : Open-Meteo API 経由で 気象庁MSM・GSM / ECMWF / NOAA GFS / DWD ICON
  - アンサンブル: GEPS(gem_global) / ECMWF-ENS / GEFS（全メンバーを個別に雪判別）
  - 実測       : 気象庁 アメダス 甲府(49142)

独自雪判別エンジン
  湿球温度を主軸に、850hPa気温・地上気温・降水量でロジスティック判別
  -> モデル毎の降雪確率 -> モデル一致度・過去成績で重み付け統合
  -> 積雪（融雪付き）・積雪確率を算出

検証・学習
  - 予報を SQLite に記録し、アメダス実測と照合（Brier / BSS / POD / FAR）
  - 湿球温度しきい値・モデル重み・確率補正(Platt)を実測から再学習（/api/retrain）

起動:  python app.py          (開発)
       gunicorn app:app --workers 1 --threads 4 --timeout 120   (本番)
       ※ workers は 1 に固定（バックグラウンド処理が各ワーカーで重複するため）。
         threads を増やすと、予報取得中でも /health や画面が固まらない。
環境変数: SNOWCAST_DB (DBパス), SNOWCAST_BG=0 (バックグラウンド処理を無効化), PORT
    SNOWCAST_ADMIN_TOKEN   設定すると /api/retrain と /api/observe にトークンが必要になる
                           (ヘッダ X-Admin-Token または ?token=)。公開URLで運用するなら設定推奨
  Render無料プラン対策（永続ディスクなしでも検証データを残す）:
    SNOWCAST_BACKUP_REPO   例 "yourname/snowcast"   DBバックアップ先のGitHubリポジトリ
    SNOWCAST_BACKUP_TOKEN  Contents の Read/Write 権限を持つ fine-grained トークン
    SNOWCAST_BACKUP_BRANCH (既定 snowcast-data) / SNOWCAST_BACKUP_H (既定 6 時間ごと)
    SNOWCAST_KEEP_DAYS     (既定 120) 記録保持日数。バックアップを軽くしたいときは 60 など
    RENDER_EXTERNAL_URL    Renderが自動設定。あれば10分おきに自分の /health を叩きスリープを防ぐ
依存: flask (標準ライブラリのみ他に不要)
"""
import atexit
import base64
import gzip
import hmac
import json
import logging
import math
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

from flask import Flask, Response, jsonify, request

# ----------------------------------------------------------------------------
# 設定
# ----------------------------------------------------------------------------
JST = timezone(timedelta(hours=9))
LOCATION = {
    "name": "甲府市",
    "lat": 35.6642,
    "lon": 138.5683,
    "elev": 272,
    "amedas": "49142",  # アメダス 甲府
}
HORIZON_DAYS = 11
HORIZON_H = HORIZON_DAYS * 24

# (Open-Meteoモデル名, 表示名)
DET_MODELS = [
    ("jma_msm", "気象庁 MSM"),
    ("jma_gsm", "気象庁 GSM"),
    ("ecmwf_ifs025", "ECMWF IFS"),
    ("gfs_seamless", "NOAA GFS"),
    ("icon_seamless", "DWD ICON"),
]
ENS_MODELS = [
    ("gem_global", "GEPS (カナダ)"),
    ("ecmwf_ifs025", "ECMWF-ENS"),
    ("gfs025", "GEFS (NOAA)"),
]
DET_VARS_FULL = ["temperature_2m", "relative_humidity_2m", "precipitation",
                 "pressure_msl", "temperature_850hPa"]
DET_VARS_CORE = ["temperature_2m", "relative_humidity_2m", "precipitation",
                 "pressure_msl"]
ENS_VARS = ["temperature_2m", "relative_humidity_2m", "precipitation", "pressure_msl"]
ENS_VARS_850 = ENS_VARS + ["temperature_850hPa"]   # 非対応モデルは自動でENS_VARSにフォールバック
CAL_EPS = 0.005             # 確率補正で使う logit の下限
CAL_MIN_N = 300             # 確率補正を学習する最小サンプル数（リード帯ごと）
CAL_MIN_EVENTS = 20         # 同・雪イベント最小数

DEFAULT_PARAMS = {"thr": 0.8, "scale": 0.6}   # 湿球温度しきい値(℃) と 遷移幅
STALE_MAX = 21600           # この秒数以内なら古い予報を即返し、裏で更新（取得失敗時の代替も兼ねる）
CACHE_TTL = 1800            # 予報キャッシュ 秒
SNAPSHOT_INTERVAL_H = 6     # 検証用スナップショット間隔
SNAPSHOT_MAX_LEAD = 168     # 記録するリード時間
KEEP_DAYS = int(os.environ.get("SNOWCAST_KEEP_DAYS", "120"))   # 記録保持日数
BACKUP_REPO = os.environ.get("SNOWCAST_BACKUP_REPO", "")
BACKUP_TOKEN = os.environ.get("SNOWCAST_BACKUP_TOKEN", "")
BACKUP_BRANCH = os.environ.get("SNOWCAST_BACKUP_BRANCH", "snowcast-data")
BACKUP_FILE = "snowcast.sqlite.gz"
ADMIN_TOKEN = os.environ.get("SNOWCAST_ADMIN_TOKEN", "")
BACKUP_INTERVAL_H = float(os.environ.get("SNOWCAST_BACKUP_H", "6"))
DB_PATH = os.environ.get("SNOWCAST_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "snowcast.sqlite"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snowcast")

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
try:
    app.json.ensure_ascii = False
except Exception:
    pass

_db_lock = threading.Lock()
_fc_lock = threading.Lock()
_cache = {"time": 0, "data": None}
_train_lock = threading.Lock()
_refreshing = threading.Event()


# ----------------------------------------------------------------------------
# 汎用
# ----------------------------------------------------------------------------
def now_jst():
    return datetime.now(JST)


def hour_floor():
    return now_jst().replace(minute=0, second=0, microsecond=0, tzinfo=None)


def tstr(dt):
    return dt.strftime("%Y-%m-%dT%H:00")


def http_get(url, params=None, timeout=25, retries=2):
    if params:
        url = url + "?" + urlparse.urlencode(params)
    last = None
    for k in range(retries + 1):
        try:
            req = urlrequest.Request(url, headers={"User-Agent": "SNOWCAST/1.0"})
            with urlrequest.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except urlerror.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "replace")
                reason = json.loads(body).get("reason") or body
            except Exception:  # noqa
                reason = e.reason
            last = RuntimeError("HTTP %s: %s" % (e.code, str(reason)[:160]))
            if 400 <= e.code < 500 and e.code != 429:
                break                       # 入力不正は再試行しても同じ
            if k < retries:
                time.sleep((3.0 if e.code == 429 else 0.8) * (k + 1))
        except Exception as e:  # noqa
            last = e
            if k < retries:
                time.sleep(0.8 * (k + 1))
    raise last


def http_json(url, params=None, **kw):
    return json.loads(http_get(url, params, **kw))


def sigmoid(x):
    if x < -30:
        return 0.0
    if x > 30:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def interp(x, pts):
    if x <= pts[0][0]:
        return pts[0][1]
    if x >= pts[-1][0]:
        return pts[-1][1]
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= x <= x1:
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return pts[-1][1]


def mean(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def r(v, n=2):
    return None if v is None else round(v, n)


# ----------------------------------------------------------------------------
# 独自雪判別エンジン
# ----------------------------------------------------------------------------
def wet_bulb(t, rh):
    """Stull (2011) の近似式による湿球温度[℃]。"""
    rh = min(max(rh, 1.0), 100.0)
    return (t * math.atan(0.151977 * math.sqrt(rh + 8.313659))
            + math.atan(t + rh) - math.atan(rh - 1.676331)
            + 0.00391838 * rh ** 1.5 * math.atan(0.023101 * rh) - 4.686035)


def p_precip_soft(mm):
    """確定論予報の降水量から降水有無の確率へ。"""
    return 1.0 - math.exp(-mm / 0.3) if mm and mm > 0 else 0.0


def p_snow_given_precip(tw, t2, t850, thr, scale):
    """降水があるとき、それが雪である確率。"""
    z = (thr - tw) / scale
    if t850 is not None:
        # 上空が暖かいと雨・みぞれ寄り、冷たいと雪寄り
        z += max(-1.5, min(1.5, -0.25 * (t850 + 3.0)))
    if t2 > 4.0:
        z -= 0.8 * (t2 - 4.0)
    return sigmoid(z)


SLR_PTS = [(-10, 18), (-5, 14), (-2, 11), (0, 8), (1, 6), (2, 4)]


def snow_ratio(t2):
    """降水1mmあたりの降雪の厚さ比（雪水比）。"""
    return interp(t2, SLR_PTS)


def run_member(n, mem, params, hard, depth0=0.0):
    """1つの予報系列（確定論 or アンサンブル1メンバー）を雪判別する。"""
    thr, scale = params["thr"], params["scale"]
    t_arr, h_arr, p_arr = mem["temp"], mem["rh"], mem["precip"]
    a850 = mem.get("t850")
    ps = [None] * n
    cm = [None] * n
    depth = [None] * n
    tw = [None] * n
    d = depth0
    for i in range(n):
        t, h, p = t_arr[i], h_arr[i], p_arr[i]
        if t is None or h is None or p is None:
            continue
        w = wet_bulb(t, h)
        tw[i] = w
        pc = p_snow_given_precip(w, t, a850[i] if a850 else None, thr, scale)
        pp = (1.0 if p >= 0.1 else 0.0) if hard else p_precip_soft(p)
        ps[i] = pp * pc
        snow_cm = p * pc * snow_ratio(t) / 10.0
        melt = max(0.0, t) * 0.017 + p * (1.0 - pc) * 0.3
        d = max(0.0, d + snow_cm - melt)
        cm[i] = snow_cm
        depth[i] = d
    return {"ps": ps, "cm": cm, "depth": depth, "tw": tw}


def lead_factor(kind, lead):
    if kind == "det":
        return 1.0 if lead <= 48 else (0.8 if lead <= 120 else 0.5)
    return 0.8 if lead <= 48 else 1.0


# ----------------------------------------------------------------------------
# DB
# ----------------------------------------------------------------------------
@contextmanager
def db():
    """コミット/ロールバックして必ず close する（標準の with sqlite3.connect は close しない）。"""
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def init_db():
    with _db_lock, db() as con:
        con.execute("PRAGMA journal_mode=WAL")     # 読み書きの同時実行でロックしにくくする
        con.executescript("""
        CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS fc_snap(
            issue TEXT, target TEXT, model TEXT, lead INTEGER,
            temp REAL, rh REAL, precip REAL, t850 REAL, tw REAL, p_snow REAL,
            PRIMARY KEY(issue, target, model));
        CREATE TABLE IF NOT EXISTS final_snap(
            issue TEXT, target TEXT, lead INTEGER, p REAL, lo REAL, hi REAL, cm REAL,
            PRIMARY KEY(issue, target));
        CREATE TABLE IF NOT EXISTS obs(
            time TEXT PRIMARY KEY, temp REAL, rh REAL, precip REAL,
            snow_depth REAL, snow1h REAL, pressure REAL,
            snow_flag INTEGER, flag_src TEXT);
        CREATE TABLE IF NOT EXISTS params(key TEXT PRIMARY KEY, value REAL);
        CREATE TABLE IF NOT EXISTS weights(
            model TEXT PRIMARY KEY, w REAL, n INTEGER, brier REAL, updated TEXT);
        CREATE TABLE IF NOT EXISTS calib(
            bin TEXT PRIMARY KEY, a REAL, b REAL, n INTEGER, events INTEGER, updated TEXT);
        CREATE INDEX IF NOT EXISTS ix_fc_target ON fc_snap(target);
        CREATE INDEX IF NOT EXISTS ix_final_target ON final_snap(target);
        """)


def get_params():
    p = dict(DEFAULT_PARAMS)
    try:
        with db() as con:
            for row in con.execute("SELECT key, value FROM params"):
                p[row["key"]] = row["value"]
    except Exception:
        pass
    return p


def kv_get(key):
    try:
        with db() as con:
            row = con.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
            return row["value"] if row else None
    except Exception:
        return None


def kv_set(key, value):
    with _db_lock, db() as con:
        con.execute("INSERT OR REPLACE INTO kv VALUES (?,?)", (key, value))


def load_persisted_forecast():
    """再起動/スリープ明けでも直近の予報をすぐ返せるよう、DBに保存した最後の予報を読み込む。"""
    raw = kv_get("last_forecast")
    if not raw:
        return
    try:
        js = json.loads(raw)
        _cache.update({"time": float(js["saved"]), "data": js["data"]})
        log.info("last forecast loaded from DB (age %d min)", (time.time() - _cache["time"]) / 60)
    except Exception as e:  # noqa
        log.warning("persisted forecast unreadable: %s", e)


def get_weights():
    try:
        with db() as con:
            return {row["model"]: row["w"] for row in con.execute("SELECT model, w FROM weights")}
    except Exception:
        return {}


def logit(p):
    p = min(1.0 - CAL_EPS, max(CAL_EPS, p))
    return math.log(p / (1.0 - p))


def get_calib():
    """リード時間帯ごとの確率補正係数 {bin: {a, b, n, events, updated}}"""
    try:
        with db() as con:
            return {row["bin"]: dict(row) for row in
                    con.execute("SELECT bin, a, b, n, events, updated FROM calib")}
    except Exception:
        return {}


def calibrate(p, lead, calib):
    """統合確率 p を実測ベースの Platt 補正で較正する（単調変換なので lo<=p<=hi は保たれる）。"""
    if p is None or not calib:
        return p
    c = calib.get(lead_bin(lead))
    if not c:
        return p

    def f(q):
        return sigmoid(c["a"] * logit(q) + c["b"])
    if p < CAL_EPS:     # 降水なし等の極小確率は連続的に比例縮小/拡大
        return min(1.0, p * f(CAL_EPS) / CAL_EPS)
    return f(p)


# ----------------------------------------------------------------------------
# DBバックアップ/復元（Render無料プランはディスクが再起動で消えるため、GitHubに退避）
#   専用ブランチに「親なし1コミット」を強制更新するので、リポジトリ履歴が肥大化しない。
# ----------------------------------------------------------------------------
_backup_lock = threading.Lock()
_last_backup = 0.0


def backup_enabled():
    return bool(BACKUP_REPO and BACKUP_TOKEN)


def _gh(method, path, body=None, accept="application/vnd.github+json", timeout=60):
    url = "https://api.github.com/repos/%s%s" % (BACKUP_REPO, path)
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Authorization": "Bearer " + BACKUP_TOKEN, "Accept": accept,
               "User-Agent": "SNOWCAST/1.0", "X-GitHub-Api-Version": "2022-11-28"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urlrequest.Request(url, data=data, method=method, headers=headers)
    with urlrequest.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def db_restore():
    """DBファイルが無いとき（=再起動直後）だけ、GitHubのバックアップから復元する。"""
    global _last_backup
    if not backup_enabled() or os.path.exists(DB_PATH):
        return False
    try:
        raw = _gh("GET", "/contents/%s?ref=%s" % (BACKUP_FILE, urlparse.quote(BACKUP_BRANCH)),
                  accept="application/vnd.github.raw+json", timeout=90)
        tmp = DB_PATH + ".restore"
        with open(tmp, "wb") as f:
            f.write(gzip.decompress(raw))
        chk = sqlite3.connect(tmp)
        try:
            chk.execute("SELECT COUNT(*) FROM obs").fetchone()
        finally:
            chk.close()
        for ext in ("-wal", "-shm"):
            try:
                os.remove(DB_PATH + ext)
            except OSError:
                pass
        os.replace(tmp, DB_PATH)
        _last_backup = time.time()
        log.info("DB restored from GitHub backup (%d bytes)", len(raw))
        return True
    except urlerror.HTTPError as e:
        if e.code == 404:
            log.info("no DB backup found yet (first run)")
        else:
            log.warning("DB restore failed: HTTP %s", e.code)
    except Exception as e:  # noqa
        log.warning("DB restore failed: %s", e)
    return False


def db_backup(force=False):
    """DBを一貫したコピーにして gzip し、GitHubの専用ブランチへ上書き保存する。"""
    global _last_backup
    if not backup_enabled() or not os.path.exists(DB_PATH):
        return False
    if not _backup_lock.acquire(blocking=False):
        return False
    tmp = DB_PATH + ".bak"
    try:
        if not force and time.time() - _last_backup < BACKUP_INTERVAL_H * 3600:
            return False
        src, dst = sqlite3.connect(DB_PATH, timeout=30), sqlite3.connect(tmp)
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        with open(tmp, "rb") as f:
            payload = gzip.compress(f.read(), 6)
        blob = json.loads(_gh("POST", "/git/blobs",
                              {"content": base64.b64encode(payload).decode("ascii"), "encoding": "base64"}))
        del payload
        tree = json.loads(_gh("POST", "/git/trees", {"tree": [
            {"path": BACKUP_FILE, "mode": "100644", "type": "blob", "sha": blob["sha"]}]}))
        commit = json.loads(_gh("POST", "/git/commits", {
            "message": "snowcast db backup " + now_jst().strftime("%Y-%m-%d %H:%M"),
            "tree": tree["sha"], "parents": []}))
        ref = "refs/heads/" + BACKUP_BRANCH
        try:
            _gh("PATCH", "/git/" + ref, {"sha": commit["sha"], "force": True})
        except urlerror.HTTPError as e:
            if e.code in (404, 422):    # ブランチ未作成
                _gh("POST", "/git/refs", {"ref": ref, "sha": commit["sha"]})
            else:
                raise
        _last_backup = time.time()
        log.info("DB backup uploaded")
        return True
    except Exception as e:  # noqa
        log.warning("DB backup failed: %s", e)
        return False
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        _backup_lock.release()


def keepalive_loop(url):
    """Render無料プランのスリープ（15分無アクセス）対策。自分の /health を定期的に叩く。"""
    while True:
        time.sleep(600)
        try:
            http_get(url.rstrip("/") + "/health", timeout=15, retries=0)
        except Exception as e:  # noqa
            log.warning("keepalive failed: %s", e)


# ----------------------------------------------------------------------------
# 予報データ取得 (Open-Meteo)
# ----------------------------------------------------------------------------
def _base_params():
    return {
        "latitude": LOCATION["lat"], "longitude": LOCATION["lon"],
        "elevation": LOCATION["elev"], "timezone": "Asia/Tokyo",
        "forecast_days": HORIZON_DAYS + 1,
    }


def fetch_det(model):
    url = "https://api.open-meteo.com/v1/forecast"
    last = None
    for vars_, minimal in ((DET_VARS_FULL, False), (DET_VARS_CORE, False), (DET_VARS_CORE, True)):
        try:
            p = _base_params()
            if minimal:
                p.pop("elevation", None)
                p["forecast_days"] = 7
            p.update({"models": model, "hourly": ",".join(vars_)})
            js = http_json(url, p)
            h = js["hourly"]
            out = {"time": h["time"]}
            for v in vars_:
                out[v] = h.get(v) or h.get(f"{v}_{model}")
            if not out.get("temperature_2m") or all(x is None for x in out["temperature_2m"]):
                raise ValueError("no data")
            return out
        except Exception as e:  # noqa
            last = e
            if not (isinstance(e, ValueError) or str(e).startswith("HTTP 400")):
                break
    raise last


def _parse_ens(js):
    h = js["hourly"]
    members = []
    for k in h:
        if k.startswith("temperature_2m"):
            suf = k[len("temperature_2m"):]
            t850 = h.get("temperature_850hPa" + suf)
            if t850 is not None and all(x is None for x in t850):
                t850 = None     # 未対応モデルは全欠測で返る場合がある
            m = {
                "temperature_2m": h[k],
                "relative_humidity_2m": h.get("relative_humidity_2m" + suf),
                "precipitation": h.get("precipitation" + suf),
                "pressure_msl": h.get("pressure_msl" + suf),
                "temperature_850hPa": t850,
            }
            if m["relative_humidity_2m"] and m["precipitation"]:
                members.append(m)
    if not members:
        raise ValueError("no members")
    return {"time": h["time"], "members": members}


def fetch_ens(model):
    """アンサンブル全メンバー取得。850hPa気温つき→失敗時は地上要素のみで再取得。"""
    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    last = None
    for vars_, retries in ((ENS_VARS_850, 1), (ENS_VARS, 2)):
        try:
            p = _base_params()
            p.update({"models": model, "hourly": ",".join(vars_)})
            return _parse_ens(http_json(url, p, timeout=40, retries=retries))
        except Exception as e:  # noqa
            last = e
    raise last


def fetch_all():
    det, ens, errors = {}, {}, {}
    jobs = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for key, _ in DET_MODELS:
            jobs.append(("det", key, ex.submit(fetch_det, key)))
        for key, _ in ENS_MODELS:
            jobs.append(("ens", key, ex.submit(fetch_ens, key)))
        for kind, key, fut in jobs:
            try:
                (det if kind == "det" else ens)[key] = fut.result()
            except Exception as e:  # noqa
                errors[("ens_" if kind == "ens" else "") + key] = str(e)[:200]
                log.warning("fetch %s %s failed: %s", kind, key, e)
    return det, ens, errors


# ----------------------------------------------------------------------------
# 統合
# ----------------------------------------------------------------------------
def build_forecast(det=None, ens=None, errors=None, start=None):
    """予報を取得し、モデル毎に雪判別→統合。(公開JSON, 検証用行, 最終行) を返す。"""
    if det is None and ens is None:
        det, ens, errors = fetch_all()
    errors = errors or {}
    if not det and not ens:
        raise RuntimeError("予報データを取得できませんでした: " + json.dumps(errors, ensure_ascii=False))

    start = start or hour_floor()
    n = HORIZON_H
    T = [tstr(start + timedelta(hours=i)) for i in range(n)]
    tidx = {t: i for i, t in enumerate(T)}
    params = get_params()
    wts = get_weights()
    calib = get_calib()

    def al(times, arr):
        out = [None] * n
        if not arr:
            return out
        for j, t in enumerate(times):
            i = tidx.get(t)
            if i is not None and j < len(arr):
                out[i] = arr[j]
        return out

    # 実測の最新積雪深を初期値に
    depth0 = 0.0
    try:
        with db() as con:
            row = con.execute("SELECT snow_depth FROM obs WHERE snow_depth IS NOT NULL "
                              "ORDER BY time DESC LIMIT 1").fetchone()
            if row and row["snow_depth"]:
                depth0 = float(row["snow_depth"])
    except Exception:
        pass

    srcs = []
    for key, label in DET_MODELS:
        r_ = det.get(key)
        if not r_:
            continue
        tm = r_["time"]
        mem = {
            "temp": al(tm, r_.get("temperature_2m")), "rh": al(tm, r_.get("relative_humidity_2m")),
            "precip": al(tm, r_.get("precipitation")), "pres": al(tm, r_.get("pressure_msl")),
            "t850": al(tm, r_.get("temperature_850hPa")) if r_.get("temperature_850hPa") else None,
        }
        srcs.append({"key": key, "label": label, "kind": "det", "members": [mem]})
    for key, label in ENS_MODELS:
        r_ = ens.get(key)
        if not r_:
            continue
        tm = r_["time"]
        mems = []
        for m in r_["members"]:
            mems.append({
                "temp": al(tm, m["temperature_2m"]), "rh": al(tm, m["relative_humidity_2m"]),
                "precip": al(tm, m["precipitation"]), "pres": al(tm, m.get("pressure_msl")),
                "t850": al(tm, m["temperature_850hPa"]) if m.get("temperature_850hPa") else None,
            })
        srcs.append({"key": "ens_" + key, "label": label, "kind": "ens", "members": mems})

    # 日付グループ
    day_idx = {}
    for i, t in enumerate(T):
        day_idx.setdefault(t[:10], []).append(i)

    for s in srcs:
        hard = s["kind"] == "ens"
        runs = [run_member(n, m, params, hard, depth0) for m in s["members"]]
        s["runs"] = runs
        s["w"] = wts.get(s["key"], 1.0)

        def avg(field, runs=runs):
            return [mean([rn[field][i] for rn in runs]) for i in range(n)]

        s["m"] = {"ps": avg("ps"), "cm": avg("cm"), "depth": avg("depth"), "tw": avg("tw"),
                  "temp": [mean([m["temp"][i] for m in s["members"]]) for i in range(n)],
                  "rh": [mean([m["rh"][i] for m in s["members"]]) for i in range(n)],
                  "precip": [mean([m["precip"][i] for m in s["members"]]) for i in range(n)],
                  "pres": [mean([m["pres"][i] for m in s["members"]]) for i in range(n)],
                  "t850": [mean([m["t850"][i] for m in s["members"] if m["t850"]]) for i in range(n)]}
        # 日別
        s["days"] = {}
        for d, idxs in day_idx.items():
            tot, dmax = [], []
            for rn in runs:
                cms = [rn["cm"][i] for i in idxs if rn["cm"][i] is not None]
                dps = [rn["depth"][i] for i in idxs if rn["depth"][i] is not None]
                if len(cms) < max(6, len(idxs) // 2):
                    continue
                tot.append(sum(cms))
                dmax.append(max(dps))
            if not tot:
                continue
            if s["kind"] == "ens":
                ex = mean([1.0 if x >= 0.5 else 0.0 for x in tot])
            else:
                ex = min(1.0, tot[0] / 0.5)
            s["days"][d] = {
                "ex": ex, "cm": mean(tot), "dmax": mean(dmax),
                "d1": mean([1.0 if x >= 1 else 0.0 for x in dmax]),
                "d5": mean([1.0 if x >= 5 else 0.0 for x in dmax]),
                "d10": mean([1.0 if x >= 10 else 0.0 for x in dmax]),
            }

    # 時間別の統合
    hourly = []
    pres_series = []
    raw_hourly = [None] * n     # 検証用に補正前の (p, lo, hi) を保持
    for i in range(n):
        items = [(s, s["m"]["ps"][i]) for s in srcs if s["m"]["ps"][i] is not None]
        if not items:
            hourly.append({"time": T[i], "p": None})
            pres_series.append(None)
            continue
        ws = [s["w"] * lead_factor(s["kind"], i) for s, _ in items]
        W = sum(ws) or 1.0
        pv = [v for _, v in items]
        p = sum(w * v for w, v in zip(ws, pv)) / W

        def wavg(field):
            num = den = 0.0
            for (s, _), w in zip(items, ws):
                v = s["m"][field][i]
                if v is not None:
                    num += w * v
                    den += w
            return num / den if den else None

        std = 0.0
        if len(pv) > 1:
            mu = sum(pv) / len(pv)
            std = math.sqrt(sum((x - mu) ** 2 for x in pv) / len(pv))
        pres = wavg("pres")
        pres_series.append(pres)
        dp6 = (pres - pres_series[i - 6]) if (i >= 6 and pres is not None and pres_series[i - 6] is not None) else None
        raw_hourly[i] = (p, min(pv), max(pv))
        hourly.append({
            "time": T[i], "p": r(calibrate(p, i, calib), 3),
            "lo": r(calibrate(min(pv), i, calib), 3), "hi": r(calibrate(max(pv), i, calib), 3),
            "agree": r(1.0 - min(1.0, std / 0.5), 2),
            "temp": r(wavg("temp"), 1), "tw": r(wavg("tw"), 1), "precip": r(wavg("precip"), 2),
            "cm": r(wavg("cm"), 2), "depth": r(wavg("depth"), 1),
            "pres": r(pres, 1), "dp6": r(dp6, 1),
            "by": {s["key"]: r(s["m"]["ps"][i], 3) for s, _ in items},
        })

    # 日別の統合
    daily = []
    for d, idxs in day_idx.items():
        items = [(s, s["days"][d]) for s in srcs if d in s["days"]]
        if not items:
            continue
        ws = [s["w"] for s, _ in items]
        W = sum(ws) or 1.0

        def wd(field):
            return sum(w * x[field] for w, (_, x) in zip(ws, items)) / W

        exs = [x["ex"] for _, x in items]
        std = 0.0
        if len(exs) > 1:
            mu = sum(exs) / len(exs)
            std = math.sqrt(sum((e - mu) ** 2 for e in exs) / len(exs))
        temps = [hourly[i].get("temp") for i in idxs if hourly[i].get("p") is not None and hourly[i].get("temp") is not None]
        ps_ = [hourly[i]["p"] for i in idxs if hourly[i].get("p") is not None]
        daily.append({
            "date": d, "p": r(wd("ex"), 3), "pmax_hour": r(max(ps_), 3) if ps_ else None,
            "cm": r(wd("cm"), 1), "dmax": r(wd("dmax"), 1),
            "d1": r(wd("d1"), 3), "d5": r(wd("d5"), 3), "d10": r(wd("d10"), 3),
            "tmin": r(min(temps), 1) if temps else None, "tmax": r(max(temps), 1) if temps else None,
            "agree": r(1.0 - min(1.0, std / 0.5), 2),
            "by": {s["key"]: r(x["ex"], 3) for s, x in items},
        })

    models = []
    for s in srcs:
        models.append({
            "key": s["key"], "label": s["label"], "kind": s["kind"], "members": len(s["members"]),
            "weight": r(s["w"], 2), "hours": sum(1 for v in s["m"]["ps"] if v is not None),
        })
    for k, msg in errors.items():
        models.append({"key": k, "label": k, "kind": "error", "error": msg})

    now_h = [h for h in hourly[:24] if h.get("p") is not None]
    out = {
        "location": LOCATION, "generated_at": now_jst().strftime("%Y-%m-%d %H:%M"),
        "start": T[0], "params": params, "models": models,
        "calib": {k: {"a": r(v["a"], 3), "b": r(v["b"], 3), "n": v["n"]} for k, v in calib.items()},
        "hourly": hourly, "daily": daily,
        "summary": {
            "max_p_24h": max((h["p"] for h in now_h), default=None),
            "snow_days": [d["date"] for d in daily if d["p"] is not None and d["p"] >= 0.3],
        },
    }

    issue = tstr(start)
    snap_rows, final_rows = [], []
    for s in srcs:
        m = s["m"]
        for i in range(min(SNAPSHOT_MAX_LEAD, n)):
            if m["ps"][i] is None:
                continue
            snap_rows.append((issue, T[i], s["key"], i, m["temp"][i], m["rh"][i], m["precip"][i],
                              m["t850"][i], m["tw"][i], m["ps"][i]))
    for i in range(min(SNAPSHOT_MAX_LEAD, n)):
        h = hourly[i]
        if raw_hourly[i] is not None:   # 補正前の値を保存（補正の再学習が自己参照にならないように）
            pr, lo_r, hi_r = raw_hourly[i]
            final_rows.append((issue, T[i], i, r(pr, 3), r(lo_r, 3), r(hi_r, 3), h["cm"]))
    return out, snap_rows, final_rows


def save_snapshot(snap_rows, final_rows):
    with _db_lock, db() as con:
        con.executemany("INSERT OR REPLACE INTO fc_snap VALUES (?,?,?,?,?,?,?,?,?,?)", snap_rows)
        con.executemany("INSERT OR REPLACE INTO final_snap VALUES (?,?,?,?,?,?,?)", final_rows)
        cutoff = tstr(hour_floor() - timedelta(days=KEEP_DAYS))
        con.execute("DELETE FROM fc_snap WHERE target < ?", (cutoff,))
        con.execute("DELETE FROM final_snap WHERE target < ?", (cutoff,))


def last_issue():
    try:
        with db() as con:
            row = con.execute("SELECT MAX(issue) AS m FROM final_snap").fetchone()
            return row["m"] if row else None
    except Exception:
        return None


def _stale_copy():
    """古い予報を返すとき、過ぎた時刻の時間別データは落として『いま』が先頭になるようにする。"""
    age = time.time() - _cache["time"]
    now_t = tstr(hour_floor())
    d = dict(_cache["data"], stale=True, age_min=int(age // 60))
    d["hourly"] = [h for h in d["hourly"] if h["time"] >= now_t]
    return d


def _refresh_async():
    """古いキャッシュを返しつつ、裏で1本だけ更新を走らせる。"""
    if _refreshing.is_set():
        return
    _refreshing.set()

    def run():
        try:
            get_forecast(force=True)
        except Exception as e:  # noqa
            log.warning("async refresh failed: %s", e)
        finally:
            _refreshing.clear()
    threading.Thread(target=run, daemon=True, name="snowcast-refresh").start()


def get_forecast(force=False):
    t0 = time.time()
    if not force and _cache["data"]:
        age = time.time() - _cache["time"]
        if age < CACHE_TTL:
            return _cache["data"]
        if age < STALE_MAX:             # 待たせず古いデータを返し、裏で更新
            _refresh_async()
            return _stale_copy()
    with _fc_lock:
        # 待っている間に別スレッドが更新済みなら、二重に取得しない
        if _cache["data"] and (_cache["time"] >= t0 or (not force and time.time() - _cache["time"] < CACHE_TTL)):
            return _cache["data"]
        try:
            out, snap_rows, final_rows = build_forecast()
        except Exception:
            if _cache["data"] and time.time() - _cache["time"] < STALE_MAX:
                log.warning("forecast refresh failed; serving stale cache")
                return _stale_copy()
            raise
        _cache.update({"time": time.time(), "data": out})
        try:
            kv_set("last_forecast", json.dumps({"saved": _cache["time"], "data": out}, ensure_ascii=False))
        except Exception as e:  # noqa
            log.warning("persist forecast failed: %s", e)
        li = last_issue()
        due = True
        if li:
            try:
                due = (hour_floor() - datetime.strptime(li, "%Y-%m-%dT%H:%M")) >= timedelta(hours=SNAPSHOT_INTERVAL_H)
            except Exception:
                due = True
        if due:
            try:
                save_snapshot(snap_rows, final_rows)
                log.info("snapshot saved: %d model rows", len(snap_rows))
            except Exception as e:  # noqa
                log.warning("snapshot failed: %s", e)
        return out


# ----------------------------------------------------------------------------
# アメダス実測
# ----------------------------------------------------------------------------
def _v(rec, key):
    x = rec.get(key)
    if isinstance(x, list) and x:
        return x[0]
    return None


def ingest_obs(hours=72):
    base = "https://www.jma.go.jp/bosai/amedas/data"
    latest = http_get(base + "/latest_time.txt").strip()
    end = datetime.fromisoformat(latest)
    begin = end - timedelta(hours=hours)
    b = begin.replace(minute=0, second=0, microsecond=0)
    b = b.replace(hour=b.hour - b.hour % 3)
    urls = []
    while b <= end:
        urls.append(f"{base}/point/{LOCATION['amedas']}/{b.strftime('%Y%m%d')}_{b.strftime('%H')}.json")
        b += timedelta(hours=3)

    def get(u):
        try:
            return json.loads(http_get(u, timeout=15, retries=1))
        except Exception:
            return {}

    rows = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for js in ex.map(get, urls):
            for k, rec in js.items():
                if not k.endswith("0000") or len(k) != 14:
                    continue
                t = f"{k[0:4]}-{k[4:6]}-{k[6:8]}T{k[8:10]}:00"
                temp, rh = _v(rec, "temp"), _v(rec, "humidity")
                pr = _v(rec, "precipitation1h")
                depth, s1 = _v(rec, "snow"), _v(rec, "snow1h")
                pres = _v(rec, "pressure")
                if s1 is not None:
                    flag, src = (1 if s1 > 0 else 0), "amedas_snow"
                elif pr is not None and temp is not None:
                    # 積雪観測が無い場合の代理指標（雨雪の独立判定: 降水あり かつ 気温<=0.5℃）
                    flag, src = (1 if (pr > 0 and temp <= 0.5) else 0), "proxy"
                else:
                    flag, src = None, None
                rows[t] = (t, temp, rh, pr, depth, s1, pres, flag, src)
    n = 0
    with _db_lock, db() as con:
        for t, temp, rh, pr, depth, s1, pres, flag, src in rows.values():
            old = con.execute("SELECT flag_src FROM obs WHERE time=?", (t,)).fetchone()
            if old and old["flag_src"] == "manual":
                con.execute("UPDATE obs SET temp=?, rh=?, precip=?, snow_depth=?, snow1h=?, pressure=? WHERE time=?",
                            (temp, rh, pr, depth, s1, pres, t))
            else:
                con.execute("INSERT OR REPLACE INTO obs VALUES (?,?,?,?,?,?,?,?,?)",
                            (t, temp, rh, pr, depth, s1, pres, flag, src))
            n += 1
    return n


# ----------------------------------------------------------------------------
# 検証・学習
# ----------------------------------------------------------------------------
def lead_bin(lead):
    if lead < 24:
        return "0-24h"
    if lead < 72:
        return "24-72h"
    return "72-168h"


def metrics(pairs):
    """pairs: [(p, obs_flag)]"""
    n = len(pairs)
    if n == 0:
        return None
    brier = sum((p - o) ** 2 for p, o in pairs) / n
    base = sum(o for _, o in pairs) / n
    clim = base * (1 - base)
    hit = sum(1 for p, o in pairs if p >= 0.5 and o == 1)
    fa = sum(1 for p, o in pairs if p >= 0.5 and o == 0)
    miss = sum(1 for p, o in pairs if p < 0.5 and o == 1)
    return {
        "n": n, "events": int(sum(o for _, o in pairs)), "brier": r(brier, 4),
        "bss": r(1 - brier / clim, 3) if clim > 0 else None,
        "pod": r(hit / (hit + miss), 3) if (hit + miss) else None,
        "far": r(fa / (hit + fa), 3) if (hit + fa) else None,
    }


def verification(days=30):
    since = tstr(hour_floor() - timedelta(days=days))
    out = {"days": days, "since": since, "by_model": {}, "final": {}, "reliability": []}
    with db() as con:
        rows = con.execute(
            "SELECT f.model, f.lead, f.p_snow AS p, o.snow_flag AS o FROM fc_snap f "
            "JOIN obs o ON o.time=f.target WHERE o.snow_flag IS NOT NULL AND f.target>=?", (since,)).fetchall()
        frows = con.execute(
            "SELECT f.lead, f.p AS p, o.snow_flag AS o FROM final_snap f "
            "JOIN obs o ON o.time=f.target WHERE o.snow_flag IS NOT NULL AND f.target>=?", (since,)).fetchall()
        src = con.execute("SELECT flag_src, COUNT(*) c FROM obs WHERE time>=? GROUP BY flag_src", (since,)).fetchall()
    out["obs_source"] = {(x["flag_src"] or "none"): x["c"] for x in src}
    groups = {}
    for x in rows:
        groups.setdefault(x["model"], {}).setdefault(lead_bin(x["lead"]), []).append((x["p"], x["o"]))
    for model, bins in groups.items():
        out["by_model"][model] = {b: metrics(v) for b, v in sorted(bins.items())}
    # final_snap.p は補正前の統合確率。現在の補正を当てた値（補正後）と並べて比較する
    # （補正は同じ期間で学習しているため補正後は学習期間内の成績＝やや楽観的）
    calib = get_calib()
    fb, fc = {}, {}
    for x in frows:
        b = lead_bin(x["lead"])
        fb.setdefault(b, []).append((x["p"], x["o"]))
        fc.setdefault(b, []).append((calibrate(x["p"], x["lead"], calib), x["o"]))
    out["final"] = {b: metrics(v) for b, v in sorted(fb.items())}
    out["final_cal"] = {b: metrics(v) for b, v in sorted(fc.items())} if calib else {}
    out["calib"] = {k: {"a": r(v["a"], 3), "b": r(v["b"], 3), "n": v["n"], "events": v["events"],
                        "updated": v["updated"]} for k, v in calib.items()}
    # 信頼度曲線（統合予報: 補正後 / 補正前）
    out["reliability_raw"] = []
    for key, getp in (("reliability", lambda x: calibrate(x["p"], x["lead"], calib)),
                      ("reliability_raw", lambda x: x["p"])):
        bins = [[] for _ in range(10)]
        for x in frows:
            bins[min(9, int(getp(x) * 10))].append(x["o"])
        for i, b in enumerate(bins):
            if b:
                out[key].append({"bin": f"{i*10}-{i*10+10}%", "n": len(b), "obs_rate": r(sum(b) / len(b), 3)})
    return out


def fit_platt(pairs, lam=5.0, iters=40):
    """p_cal = sigmoid(a*logit(p)+b) をログ損失＋(1,0)への弱い正則化でニュートン法により推定。"""
    xs = [logit(p) for p, _ in pairs]
    ys = [o for _, o in pairs]
    a, b = 1.0, 0.0
    for _ in range(iters):
        ga, gb = 2 * lam * (a - 1.0), 2 * lam * b
        haa, hbb, hab = 2 * lam, 2 * lam, 0.0
        for x, y in zip(xs, ys):
            q = sigmoid(a * x + b)
            e = q - y
            w = q * (1.0 - q)
            ga += e * x
            gb += e
            haa += w * x * x
            hbb += w
            hab += w * x
        det = haa * hbb - hab * hab
        if det <= 1e-12:
            break
        da = (hbb * ga - hab * gb) / det
        db_ = (haa * gb - hab * ga) / det
        a = min(3.0, max(0.2, a - da))      # a>0 で単調性を保証
        b = min(4.0, max(-4.0, b - db_))
        if abs(da) + abs(db_) < 1e-7:
            break
    return a, b


def fit_calibration():
    """統合確率（補正前）と実測の対応から、リード時間帯ごとの補正係数を学習して保存する。"""
    with db() as con:
        rows = con.execute(
            "SELECT f.lead, f.p AS p, o.snow_flag AS o FROM final_snap f "
            "JOIN obs o ON o.time=f.target WHERE o.snow_flag IS NOT NULL AND f.p IS NOT NULL").fetchall()
    groups = {}
    for x in rows:
        groups.setdefault(lead_bin(x["lead"]), []).append((x["p"], x["o"]))
    out = {}
    stamp = now_jst().strftime("%Y-%m-%d %H:%M")
    for b, pairs in sorted(groups.items()):
        ev = int(sum(o for _, o in pairs))
        if len(pairs) >= CAL_MIN_N and ev >= CAL_MIN_EVENTS:
            a, bb = fit_platt(pairs)
            with _db_lock, db() as con:
                con.execute("INSERT OR REPLACE INTO calib VALUES (?,?,?,?,?,?)", (b, a, bb, len(pairs), ev, stamp))
            out[b] = {"a": r(a, 3), "b": r(bb, 3), "n": len(pairs), "events": ev}
        else:
            out[b] = {"status": f"データ不足（{CAL_MIN_N}サンプル・雪{CAL_MIN_EVENTS}件以上で学習）",
                      "n": len(pairs), "events": ev}
    return out


def _retrain():
    """実測に基づき 湿球温度しきい値・遷移幅 を再推定し、モデル重み・確率補正を更新する。"""
    res = {"params": None, "weights": {}}
    with db() as con:
        rows = con.execute(
            "SELECT f.temp, f.precip, f.t850, f.tw, o.snow_flag AS o FROM fc_snap f "
            "JOIN obs o ON o.time=f.target WHERE o.snow_flag IS NOT NULL AND f.lead<=72 "
            "AND f.model NOT LIKE 'ens_%' AND f.tw IS NOT NULL AND f.precip IS NOT NULL").fetchall()
        events = sum(x["o"] for x in rows)
        if len(rows) >= 200 and events >= 15:
            # 降水0なら p=0 でパラメータに依存しない → 降水ありの行だけ探索し、残りは定数として加算
            se0 = sum(x["o"] for x in rows if not x["precip"] > 0)
            wet = [(p_precip_soft(x["precip"]), x["tw"], x["temp"], x["t850"], x["o"])
                   for x in rows if x["precip"] > 0]
            best = None
            for thr10 in range(-10, 26):
                thr = thr10 / 10.0
                for scale in (0.4, 0.6, 0.9):
                    se = se0
                    for pp, tw_, t2_, t850_, o_ in wet:
                        se += (pp * p_snow_given_precip(tw_, t2_, t850_, thr, scale) - o_) ** 2
                    if best is None or se < best[0]:
                        best = (se, thr, scale)
            res["params"] = {"thr": best[1], "scale": best[2], "brier": r(best[0] / len(rows), 4), "n": len(rows), "events": events}
            with _db_lock, db() as c2:
                c2.execute("INSERT OR REPLACE INTO params VALUES ('thr', ?)", (best[1],))
                c2.execute("INSERT OR REPLACE INTO params VALUES ('scale', ?)", (best[2],))
        else:
            res["params"] = {"status": "データ不足（雪イベント15件・200サンプル以上で再学習）", "n": len(rows), "events": int(events)}
        mrows = con.execute(
            "SELECT f.model, f.p_snow AS p, o.snow_flag AS o FROM fc_snap f JOIN obs o ON o.time=f.target "
            "WHERE o.snow_flag IS NOT NULL AND f.lead<=72").fetchall()
    per = {}
    for x in mrows:
        per.setdefault(x["model"], []).append((x["p"] - x["o"]) ** 2)
    scores = {m: sum(v) / len(v) for m, v in per.items() if len(v) >= 50}
    if len(scores) >= 2:
        ref = sum(scores.values()) / len(scores)
        with _db_lock, db() as c2:
            for m, b in scores.items():
                w = max(0.3, min(2.0, (ref + 1e-4) / (b + 1e-4)))
                c2.execute("INSERT OR REPLACE INTO weights VALUES (?,?,?,?,?)",
                           (m, w, len(per[m]), b, now_jst().strftime("%Y-%m-%d %H:%M")))
                res["weights"][m] = r(w, 2)
    res["calib"] = fit_calibration()
    _refresh_async()    # 新しいパラメータで予報を作り直す（古い予報は作り直し完了まで返し続ける）
    return res


def retrain():
    if not _train_lock.acquire(blocking=False):
        return {"status": "busy"}
    try:
        return _retrain()
    finally:
        _train_lock.release()


# ----------------------------------------------------------------------------
# バックグラウンド処理
# ----------------------------------------------------------------------------
def bg_loop():
    last_train_day = None
    while True:
        try:
            get_forecast(force=True)
        except Exception as e:  # noqa
            log.warning("forecast refresh failed: %s", e)
        try:
            ingest_obs()
        except Exception as e:  # noqa
            log.warning("ingest_obs failed: %s", e)
        try:
            today = now_jst().strftime("%Y-%m-%d")
            if last_train_day != today and now_jst().hour >= 4:
                retrain()
                last_train_day = today
        except Exception as e:  # noqa
            log.warning("retrain failed: %s", e)
        db_backup()     # 6時間ごと（SNOWCAST_BACKUP_H）。未設定なら何もしない
        time.sleep(1800)


# ----------------------------------------------------------------------------
# API
# ----------------------------------------------------------------------------
def qint(name, default, lo, hi):
    try:
        v = int(request.args.get(name, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def authorized():
    if not ADMIN_TOKEN:
        return True
    tok = request.headers.get("X-Admin-Token") or request.args.get("token", "")
    return hmac.compare_digest(tok.encode("utf-8"), ADMIN_TOKEN.encode("utf-8"))


@app.route("/health")
def health():
    age = int((time.time() - _cache["time"]) // 60) if _cache["data"] else None
    return jsonify({"ok": True, "time": now_jst().isoformat(), "forecast_age_min": age})


@app.route("/api/forecast")
def api_forecast():
    try:
        return jsonify(get_forecast(force=request.args.get("refresh") == "1"))
    except Exception as e:  # noqa
        log.exception("forecast failed")
        return jsonify({"error": str(e)}), 502


@app.route("/api/observations")
def api_observations():
    hours = qint("hours", 48, 1, 720)
    since = tstr(hour_floor() - timedelta(hours=hours))
    with db() as con:
        n = con.execute("SELECT COUNT(*) c FROM obs").fetchone()["c"]
    if n == 0:
        try:
            ingest_obs()
        except Exception as e:  # noqa
            return jsonify({"error": str(e), "rows": []}), 502
    with db() as con:
        rows = [dict(x) for x in con.execute("SELECT * FROM obs WHERE time>=? ORDER BY time", (since,))]
    return jsonify({"rows": rows})


@app.route("/api/observe", methods=["POST"])
def api_observe():
    """目視などの雪の有無を手動登録。{"time":"2026-12-01T09:00","snow":true}"""
    if not authorized():
        return jsonify({"error": "unauthorized"}), 401
    js = request.get_json(force=True, silent=True) or {}
    t = str(js.get("time", ""))[:13] + ":00"
    try:
        datetime.strptime(t, "%Y-%m-%dT%H:%M")
    except Exception:
        return jsonify({"error": "time は YYYY-MM-DDTHH:00 形式で指定してください"}), 400
    flag = 1 if js.get("snow") else 0
    with _db_lock, db() as con:
        if con.execute("SELECT 1 FROM obs WHERE time=?", (t,)).fetchone():
            con.execute("UPDATE obs SET snow_flag=?, flag_src='manual' WHERE time=?", (flag, t))
        else:
            con.execute("INSERT INTO obs(time, snow_flag, flag_src) VALUES (?,?,'manual')", (t, flag))
    return jsonify({"ok": True, "time": t, "snow": flag})


@app.route("/api/verification")
def api_verification():
    return jsonify(verification(qint("days", 30, 1, 365)))


@app.route("/api/retrain", methods=["GET", "POST"])
def api_retrain():
    if not authorized():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(retrain())


@app.route("/api/params")
def api_params():
    return jsonify({"params": get_params(), "weights": get_weights(), "calib": get_calib()})


# ----------------------------------------------------------------------------
# 画面
# ----------------------------------------------------------------------------
INDEX_HTML = r"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0b1424">
<title>SNOW CAST - 甲府の降雪確率</title>
<link rel="preconnect" href="https://cdn.jsdelivr.net" crossorigin>
<style>
:root{--bg:#0b1424;--bg2:#101d33;--ln:#1f3350;--tx:#f2f7ff;--sub:#8ea3c0;--ice:#9fd3ff;--warm:#ffc27a;--ok:#7be0a1;
--r:16px}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{scroll-padding-top:120px}
body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.6 -apple-system,BlinkMacSystemFont,"Hiragino Sans","Noto Sans JP",sans-serif;
padding:env(safe-area-inset-top) 0 env(safe-area-inset-bottom)}
.wrap{max-width:960px;margin:auto;padding:0 14px}
.top{display:flex;justify-content:space-between;align-items:center;padding:14px 0 6px}
.brand{font-weight:800;font-size:17px;letter-spacing:.14em}
.brand small{display:block;font-weight:400;font-size:12px;letter-spacing:0;color:var(--sub)}
.iconbtn{border:1px solid var(--ln);background:var(--bg2);color:var(--tx);border-radius:999px;padding:8px 14px;font-size:13px;cursor:pointer}
.iconbtn:disabled{opacity:.5;cursor:wait}
.iconbtn:focus-visible,.tab:focus-visible,.chip:focus-visible{outline:2px solid var(--ice);outline-offset:2px}

/* ヒーロー: 結論の一文 + 24時間の帯 */
.hero{padding:18px 0 6px}
.hero h1{margin:0;font-size:clamp(24px,6.4vw,38px);line-height:1.25;font-weight:800}
.hero p{margin:8px 0 0;color:var(--sub);font-size:14px}
.strip{display:grid;grid-template-columns:repeat(24,1fr);gap:2px;margin-top:18px;height:64px;align-items:end}
.cell{border-radius:4px 4px 2px 2px;min-height:6px;background:var(--ice);cursor:pointer;border:0;padding:0;transition:opacity .15s}
.cell.sel{outline:2px solid var(--tx);outline-offset:1px}
.ticks{display:grid;grid-template-columns:repeat(24,1fr);gap:2px;font-size:11px;color:var(--sub);margin-top:4px}
.ticks span{white-space:nowrap;overflow:visible}
#pick{min-height:24px;margin-top:8px;font-size:13px;color:var(--tx)}
#pick b{color:var(--ice)}

.notice{margin:10px 0 0;padding:8px 12px;border-radius:10px;background:#3a2a12;color:var(--warm);font-size:13px}
.notice:empty{display:none}

/* タブ */
.tabs{position:sticky;top:0;z-index:5;margin:14px -14px 0;padding:8px 14px;background:rgba(11,20,36,.92);backdrop-filter:blur(8px);
display:flex;gap:6px;overflow-x:auto;border-bottom:1px solid var(--ln)}
.tab{flex:1;min-width:76px;border:0;background:transparent;color:var(--sub);font-size:14px;font-weight:600;padding:9px 10px;border-radius:10px;cursor:pointer}
.tab[aria-selected=true]{background:var(--ice);color:#06223a}
section{display:none;padding:14px 0 40px}
section.on{display:block}
h2{font-size:16px;margin:22px 0 10px}
h2:first-child{margin-top:0}
.note{font-size:12.5px;color:var(--sub);margin:8px 0}
.seg{display:inline-flex;background:var(--bg2);border:1px solid var(--ln);border-radius:999px;padding:3px;margin-bottom:10px}
.chip{border:0;background:transparent;color:var(--sub);padding:5px 14px;border-radius:999px;font-size:13px;cursor:pointer}
.chip.on{background:var(--tx);color:#0b1424;font-weight:700}
.canvas{position:relative;height:280px}

/* 日別 */
.days{display:flex;gap:8px;overflow-x:auto;scroll-snap-type:x proximity;padding-bottom:8px;margin:0 -14px;padding:0 14px 8px}
.day{flex:0 0 108px;scroll-snap-align:start;background:var(--bg2);border-radius:14px;padding:10px;border:1px solid var(--ln)}
.day.today{border-color:var(--ice)}
.day .d{font-size:12px;color:var(--sub)}
.day .i{font-size:22px;height:30px;line-height:30px}
.day .p{font-size:24px;font-weight:800;line-height:1.1}
.bar{height:4px;border-radius:2px;background:var(--ln);margin:6px 0}
.bar i{display:block;height:100%;border-radius:2px;background:var(--ice)}
.day .s{font-size:12px;color:var(--sub)}
.day .z{font-size:12px;color:var(--tx);margin-top:2px}

table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:7px 6px;text-align:center;border-bottom:1px solid var(--ln)}
th{color:var(--sub);font-weight:600}
th:first-child,td:first-child{text-align:left;position:sticky;left:0;background:var(--bg);white-space:nowrap}
.scroll{overflow-x:auto;margin:0 -14px;padding:0 14px}
.tag{display:inline-block;font-size:11px;padding:0 6px;border-radius:6px;background:var(--bg2);color:var(--sub);margin-left:4px}
.err{color:var(--warm)}
.skel{height:30px;border-radius:8px;background:linear-gradient(90deg,var(--bg2),#17284a,var(--bg2));background-size:200% 100%;animation:sk 1.4s linear infinite}
@keyframes sk{to{background-position:-200% 0}}
@media (prefers-reduced-motion:reduce){.skel{animation:none}.cell{transition:none}}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.foot{font-size:12px;color:var(--sub);padding:0 0 30px}
</style>
</head>
<body>
<div class="wrap">
<div class="top"><div class="brand">SNOW CAST<small>甲府市の降雪確率</small></div>
<button class="iconbtn" id="btnRefresh">更新</button></div>

<div class="hero">
<h1 id="headline"><span class="skel" style="display:block;width:70%">&nbsp;</span></h1>
<p id="subline"></p>
<div class="strip" id="strip" role="list" aria-label="今後24時間の降雪確率"></div>
<div class="ticks" id="ticks"></div>
<div id="pick"></div>
<div class="notice" id="notice"></div>
<div class="note" id="gen">読み込み中…</div>
</div>

<nav class="tabs" role="tablist">
<button class="tab" role="tab" data-tab="fc" aria-selected="true">予報</button>
<button class="tab" role="tab" data-tab="snow" aria-selected="false">積雪</button>
<button class="tab" role="tab" data-tab="model" aria-selected="false">モデル比較</button>
<button class="tab" role="tab" data-tab="ver" aria-selected="false">精度</button>
</nav>

<section id="s-fc" class="on">
<h2>11日間の雪予報</h2><div class="days" id="days"></div>
<h2>1時間ごとの降雪確率</h2>
<div class="seg" role="group"><button class="chip on" data-r="48">48時間</button><button class="chip" data-r="120">5日</button><button class="chip" data-r="264">11日</button></div>
<div class="canvas"><canvas id="cHour"></canvas></div>
<div class="note">棒: 降雪確率 / 薄い帯: モデル間の最小〜最大 / 線: 気温・湿球温度。凡例の「気圧」をタップで表示。</div>
</section>

<section id="s-snow">
<h2>予想降雪量と積雪</h2>
<div class="canvas"><canvas id="cSnow"></canvas></div>
<h2>積雪が出る確率</h2>
<div class="scroll"><table id="tSnow"></table></div>
<div class="note">降雪量は雪水比×降水量、積雪は融雪を考慮した簡易推定です。確率は各モデル・アンサンブルメンバーの比率を重み付けして統合しています。</div>
</section>

<section id="s-model">
<h2>日ごとの降雪確率（モデル別）</h2>
<div class="scroll"><table id="tModel"></table></div>
<h2>直近72時間のモデル別</h2>
<div class="canvas"><canvas id="cModel"></canvas></div>
<div class="note">モデルの色が揃っているほど予報の信頼度が高く、ばらけているほど不確実です。</div>
</section>

<section id="s-ver">
<h2>予報は当たっているか</h2>
<div id="ver" class="note">集計中…</div>
<div class="scroll"><table id="tVer"></table></div>
<h2>確率ごとの実際の雪の割合</h2>
<div class="note">予報確率に近いほど良好です。</div>
<div class="scroll"><table id="tRel"></table></div>
<div class="row" style="margin-top:12px"><button class="iconbtn" id="btnTrain">実測から再学習</button><span class="note" id="trainRes"></span></div>
<h2>直近のアメダス実測（甲府）</h2>
<div class="scroll"><table id="tObs"></table></div>
</section>

<div class="foot">予報データ: Open-Meteo（気象庁MSM/GSM・ECMWF・NOAA・DWD・GEPS等）、実測: 気象庁アメダス。実験的な独自モデルです。防災判断は気象庁の情報を優先してください。</div>
</div>

<script>
const $=s=>document.querySelector(s);
const WD='日月火水木金土', charts={}, GRID='rgba(255,255,255,.07)', TICK='#8ea3c0';
const COLORS=['#9fd3ff','#ffc27a','#7be0a1','#ff8fa8','#c9a6ff','#f5e663','#7fdbda','#ff9f6b'];
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const pct=p=>p==null?'-':Math.round(p*100)+'%';
const hh=t=>t.slice(5,10).replace('-','/')+' '+t.slice(11,13)+'時';
const dl=d=>{const x=new Date(d+'T00:00:00');return (x.getMonth()+1)+'/'+x.getDate()+'('+WD[x.getDay()]+')'};
const heat=p=>`rgba(159,211,255,${(0.10+0.85*p).toFixed(2)})`;
const ico=p=>p>=.6?'❄️':p>=.3?'🌨️':p>=.1?'🌥️':'';
const word=p=>p>=.6?'雪の可能性が高い':p>=.3?'雪になりそう':p>=.1?'わずかに可能性':'雪なし';
let FC=null, RANGE=48, TAB='fc', VERLOADED=false;
(()=>{const s=document.createElement('script');s.src='https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js';
  s.onload=()=>{Chart.defaults.color=TICK;Chart.defaults.font.family='inherit';draw();};document.head.appendChild(s);})();

function mk(id,cfg){ if(!window.Chart) return; if(charts[id]) charts[id].destroy(); charts[id]=new Chart($(id),cfg); }
function showErr(m){const g=$('#gen');g.textContent='';const e=document.createElement('span');e.className='err';e.textContent=m;g.appendChild(e);$('#headline').textContent='予報を読み込み中です…';}

/* ---------- ヒーロー ---------- */
function drawHero(){
  const f=FC, H=f.hourly.filter(h=>h.p!=null);
  const next=H.find(h=>h.p>=0.3), m24=Math.max(...H.slice(0,24).map(h=>h.p));
  const dayLabel=t=>{const d=new Date(t.slice(0,10)+'T00:00:00'),n=new Date(H[0].time.slice(0,10)+'T00:00:00');
    const k=Math.round((d-n)/864e5);return (k==0?'今日':k==1?'明日':k==2?'明後日':dl(t.slice(0,10)))+' '+(+t.slice(11,13))+'時ごろ'};
  $('#headline').textContent = next ? (dayLabel(next.time)+'から雪の可能性（'+pct(next.p)+'）') : '11日以内に雪の予報はありません';
  $('#subline').textContent = '今後24時間の最大降雪確率は '+pct(m24)+'。'+(next&&next.time!==H[0].time?'':'');
  const S=H.slice(0,24);
  $('#strip').innerHTML=S.map((h,i)=>`<button class="cell" role="listitem" data-i="${i}" aria-label="${hh(h.time)} ${pct(h.p)}" style="height:${Math.max(10,h.p*100)}%;background:${heat(h.p)}"></button>`).join('');
  $('#ticks').innerHTML=S.map((h,i)=>`<span>${i%6==0?(+h.time.slice(11,13))+'時':''}</span>`).join('');
  const pick=i=>{const h=S[i];document.querySelectorAll('.cell').forEach((c,k)=>c.classList.toggle('sel',k==i));
    $('#pick').innerHTML=`<b>${hh(h.time)}</b>　降雪確率 <b>${pct(h.p)}</b>　気温 ${h.temp??'-'}℃　降水 ${h.precip??'-'}mm`;};
  document.querySelectorAll('.cell').forEach(c=>c.onclick=()=>pick(+c.dataset.i));
  const k=S.findIndex(h=>h.p>=0.3); pick(k>=0?k:0);
  $('#gen').textContent='更新 '+f.generated_at+'（JST） / 湿球しきい値 '+f.params.thr+'℃ / 確率補正: '+(Object.keys(f.calib||{}).length?'実測で学習済み':'未学習');
  const bad=f.models.filter(m=>m.kind=='error').length;
  $('#notice').textContent=(f.stale?'最新の取得に失敗、または更新中のため、約'+f.age_min+'分前のデータを表示しています。':'')+
    (bad?(f.stale?' ':'')+'取得できなかった予報モデルが'+bad+'件あります（「モデル比較」タブで理由を確認できます）。':'');
}

/* ---------- 各タブ ---------- */
function drawHour(){
  const H=FC.hourly.filter(h=>h.p!=null).slice(0,RANGE);
  mk('#cHour',{data:{labels:H.map(h=>hh(h.time)),datasets:[
    {type:'bar',label:'降雪確率(%)',data:H.map(h=>h.p*100),backgroundColor:H.map(h=>heat(h.p)),yAxisID:'y',order:3},
    {type:'line',label:'最大モデル',data:H.map(h=>h.hi*100),borderColor:'rgba(255,255,255,.2)',pointRadius:0,borderWidth:1,yAxisID:'y',order:2},
    {type:'line',label:'最小モデル',data:H.map(h=>h.lo*100),borderColor:'rgba(255,255,255,.2)',pointRadius:0,borderWidth:1,yAxisID:'y',fill:'-1',backgroundColor:'rgba(255,255,255,.06)',order:2},
    {type:'line',label:'気温(℃)',data:H.map(h=>h.temp),borderColor:COLORS[1],pointRadius:0,borderWidth:2,yAxisID:'y2',order:1},
    {type:'line',label:'湿球温度(℃)',data:H.map(h=>h.tw),borderColor:COLORS[2],pointRadius:0,borderWidth:2,borderDash:[4,3],yAxisID:'y2',order:1},
    {type:'line',label:'気圧(hPa)',data:H.map(h=>h.pres),borderColor:COLORS[4],pointRadius:0,borderWidth:1.5,yAxisID:'y3',hidden:true,order:1}]},
    options:{maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
      scales:{y:{min:0,max:100,grid:{color:GRID},ticks:{callback:v=>v+'%'}},y2:{position:'right',grid:{display:false},ticks:{callback:v=>v+'℃'}},y3:{position:'right',display:'auto',grid:{display:false}},x:{grid:{display:false},ticks:{maxTicksLimit:6}}},
      plugins:{legend:{labels:{boxWidth:10,usePointStyle:true}}}}});
}
function drawFc(){
  const D=FC.daily;
  $('#days').innerHTML=D.map((d,i)=>`<div class="day${i==0?' today':''}"><div class="d">${i==0?'今日':dl(d.date)}</div><div class="i">${ico(d.p||0)}</div>
    <div class="p">${pct(d.p)}</div><div class="bar"><i style="width:${(d.p||0)*100}%"></i></div>
    <div class="z">${word(d.p||0)}</div>
    <div class="s">${d.tmin!=null?Math.round(d.tmin)+'〜'+Math.round(d.tmax)+'℃':''}</div>
    <div class="s">降雪 ${d.cm??'-'}cm</div></div>`).join('');
  drawHour();
}
function drawSnow(){
  const D=FC.daily;
  mk('#cSnow',{type:'bar',data:{labels:D.map(d=>dl(d.date)),datasets:[
    {label:'降雪量(cm/日)',data:D.map(d=>d.cm),backgroundColor:'rgba(159,211,255,.75)',borderRadius:4},
    {label:'最大積雪(cm)',data:D.map(d=>d.dmax),backgroundColor:'rgba(242,247,255,.6)',borderRadius:4}]},
    options:{maintainAspectRatio:false,scales:{y:{beginAtZero:true,grid:{color:GRID}},x:{grid:{display:false},ticks:{maxTicksLimit:6}}},plugins:{legend:{labels:{boxWidth:10,usePointStyle:true}}}}});
  $('#tSnow').innerHTML='<tr><th>日付</th><th>降雪0.5cm以上</th><th>積雪1cm以上</th><th>5cm以上</th><th>10cm以上</th></tr>'+
    D.map(d=>`<tr><td>${dl(d.date)}</td><td>${pct(d.p)}</td><td>${pct(d.d1)}</td><td>${pct(d.d5)}</td><td>${pct(d.d10)}</td></tr>`).join('');
}
function drawModel(){
  const D=FC.daily, M=FC.models.filter(m=>m.kind!='error'), H72=FC.hourly.filter(h=>h.p!=null).slice(0,72);
  $('#tModel').innerHTML='<tr><th>モデル</th><th>重み</th>'+D.map(d=>`<th>${dl(d.date).replace(/\(.*/,'')}</th>`).join('')+'</tr>'+
    '<tr><td><b>SNOW CAST 統合</b></td><td>-</td>'+D.map(d=>`<td style="background:${heat(d.p||0)};color:#fff">${pct(d.p)}</td>`).join('')+'</tr>'+
    M.map(m=>`<tr><td>${m.label}<span class="tag">${m.kind=='ens'?m.members+'メンバー':'確定論'}</span></td><td>${m.weight}</td>`+
      D.map(d=>{const v=d.by[m.key];return `<td style="background:${v==null?'transparent':heat(v)}">${v==null?'':pct(v)}</td>`}).join('')+'</tr>').join('')+
    FC.models.filter(m=>m.kind=='error').map(m=>`<tr><td class="err" colspan="${D.length+2}">取得失敗: ${esc(m.label)}　${esc(m.error||'')}</td></tr>`).join('');
  mk('#cModel',{type:'line',data:{labels:H72.map(h=>hh(h.time)),datasets:[
    ...M.map((m,i)=>({label:m.label,data:H72.map(h=>h.by[m.key]==null?null:h.by[m.key]*100),borderColor:COLORS[i%8],pointRadius:0,borderWidth:1.5,tension:.2})),
    {label:'統合',data:H72.map(h=>h.p*100),borderColor:'#fff',borderWidth:3,pointRadius:0}]},
    options:{maintainAspectRatio:false,scales:{y:{min:0,max:100,grid:{color:GRID},ticks:{callback:v=>v+'%'}},x:{grid:{display:false},ticks:{maxTicksLimit:6}}},plugins:{legend:{labels:{boxWidth:10,usePointStyle:true}}}}});
}
async function loadVer(){
  const v=await fetch('/api/verification?days=30').then(r=>r.json());
  const src=Object.entries(v.obs_source||{}).map(([k,c])=>k+':'+c).join(' / ')||'実測なし';
  $('#ver').textContent='過去30日。実測の雪判定: '+src+'（proxy=気温・降水からの代理判定、目視の登録は /api/observe）。BSSが0より大きいほど、平年並みの予想より良好です。';
  const rows=[], add=(name,o)=>{for(const [b,m] of Object.entries(o||{})) if(m) rows.push(`<tr><td>${name}</td><td>${b}</td><td>${m.n}</td><td>${m.events}</td><td>${m.brier}</td><td>${m.bss??'-'}</td><td>${pct(m.pod)}</td><td>${pct(m.far)}</td></tr>`)};
  const hasCal=Object.keys(v.final_cal||{}).length>0;
  if(hasCal) add('<b>統合（補正後）</b>',v.final_cal);
  add(hasCal?'統合（補正前）':'<b>統合</b>',v.final);
  for(const [k,o] of Object.entries(v.by_model||{})) add(k,o);
  const rel=v.reliability||[], raw=Object.fromEntries((v.reliability_raw||[]).map(x=>[x.bin,x]));
  $('#tRel').innerHTML=rel.length?'<tr><th>予報確率</th><th>件数</th><th>実際の雪</th>'+(hasCal?'<th>補正前:件数</th><th>補正前:雪</th>':'')+'</tr>'+
    rel.map(x=>`<tr><td>${x.bin}</td><td>${x.n}</td><td>${pct(x.obs_rate)}</td>`+(hasCal?`<td>${raw[x.bin]?raw[x.bin].n:'-'}</td><td>${raw[x.bin]?pct(raw[x.bin].obs_rate):'-'}</td>`:'')+'</tr>').join(''):'';
  $('#tVer').innerHTML=rows.length?'<tr><th>系統</th><th>先の時間</th><th>件数</th><th>雪の回数</th><th>Brier</th><th>BSS</th><th>検出率</th><th>空振り率</th></tr>'+rows.join(''):
    '<tr><td>照合できる記録がまだありません。予報は6時間ごとに自動記録され、実測と突き合わせて蓄積されます。</td></tr>';
}
async function loadObs(){
  const o=await fetch('/api/observations?hours=36').then(r=>r.json());
  const R=(o.rows||[]).slice(-24).reverse();
  $('#tObs').innerHTML='<tr><th>時刻</th><th>気温</th><th>湿度</th><th>降水1h</th><th>積雪深</th><th>雪判定</th></tr>'+
   R.map(x=>`<tr><td>${hh(x.time)}</td><td>${x.temp??'-'}</td><td>${x.rh??'-'}</td><td>${x.precip??'-'}</td><td>${x.snow_depth??'-'}</td><td>${x.snow_flag==null?'-':(x.snow_flag?'雪':'なし')}<span class="tag">${x.flag_src||''}</span></td></tr>`).join('');
}

/* ---------- タブ・操作 ---------- */
function draw(){
  if(!FC) return;
  if(TAB=='fc') drawFc(); else if(TAB=='snow') drawSnow(); else if(TAB=='model') drawModel();
  else if(TAB=='ver' && !VERLOADED){VERLOADED=true; loadVer().catch(()=>{$('#ver').textContent='検証データを取得できませんでした。しばらくしてから再度開いてください。';VERLOADED=false;}); loadObs().catch(()=>{});}
}
function setTab(t){
  TAB=t;
  document.querySelectorAll('.tab').forEach(b=>b.setAttribute('aria-selected',b.dataset.tab==t));
  document.querySelectorAll('section').forEach(s=>s.classList.toggle('on',s.id=='s-'+t));
  history.replaceState(null,'','#'+t); draw();
}
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>setTab(b.dataset.tab));
document.querySelectorAll('[data-r]').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('[data-r]').forEach(x=>x.classList.remove('on'));b.classList.add('on');RANGE=+b.dataset.r;if(FC)drawHour();});

async function loadForecast(force){
  const b=$('#btnRefresh'); b.disabled=true; b.textContent=force?'更新中…':'更新';
  try{
    const j=await fetch('/api/forecast'+(force?'?refresh=1':'')).then(r=>r.json());
    if(j.error){ if(!FC){ showErr(j.error); setTimeout(()=>loadForecast(false),8000); } return; }
    FC=j; drawHero(); draw();
  }catch(e){ if(!FC) showErr(String(e)); }
  finally{ b.disabled=false; b.textContent='更新'; }
}
$('#btnRefresh').onclick=()=>loadForecast(true);

const tok=()=>sessionStorage.getItem('adm')||'';
async function trainReq(){
  const h={}; if(tok()) h['X-Admin-Token']=tok();
  let res=await fetch('/api/retrain',{method:'POST',headers:h});
  if(res.status===401){
    const t=prompt('管理トークンを入力してください'); if(!t) throw new Error('キャンセルしました');
    sessionStorage.setItem('adm',t);
    res=await fetch('/api/retrain',{method:'POST',headers:{'X-Admin-Token':t}});
    if(res.status===401){sessionStorage.removeItem('adm'); throw new Error('トークンが違います');}
  }
  if(!res.ok) throw new Error('HTTP '+res.status);
  return res.json();
}
function fmtTrain(r){
  if(r.status==='busy') return '別の学習が実行中です';
  const p=r.params||{}, out=[];
  out.push(p.status ? p.status+'（'+p.n+'件）' : '湿球しきい値 '+p.thr+'℃ / 遷移幅 '+p.scale+'（'+p.n+'件）');
  const c=Object.entries(r.calib||{}).map(([b,v])=>b+': '+(v.status?'データ不足('+v.n+'件)':'学習済(a='+v.a+', b='+v.b+')'));
  if(c.length) out.push('補正 '+c.join(' / '));
  return out.join(' ｜ ');
}
$('#btnTrain').onclick=async()=>{
  const b=$('#btnTrain'); b.disabled=true; $('#trainRes').textContent='実行中…';
  try{ $('#trainRes').textContent=fmtTrain(await trainReq()); VERLOADED=false; loadVer().catch(()=>{}); setTimeout(()=>loadForecast(false),30000); }
  catch(e){ $('#trainRes').textContent='失敗: '+e.message; }
  finally{ b.disabled=false; }
};

(async()=>{
  const h=location.hash.slice(1); if(['fc','snow','model','ver'].includes(h)) TAB=h;
  await loadForecast(false);
  setTab(TAB);
  setInterval(()=>loadForecast(false), 30*60*1000);
})();
</script>
</body></html>
"""


@app.route("/")
def index():
    return Response(INDEX_HTML, mimetype="text/html")


# ----------------------------------------------------------------------------
# 起動
# ----------------------------------------------------------------------------
_BG = os.environ.get("SNOWCAST_BG", "1") != "0"
_ready = threading.Event()


def _startup():
    """DB復元(GitHubから最大90秒)などの重い準備はポートを開いたあとに行う。
    トップページ(HTML)は準備を待たずに返し、/api/* だけ準備完了まで待たせる。"""
    try:
        db_restore()
        init_db()
        load_persisted_forecast()
    except Exception:  # noqa
        log.exception("startup failed")
    finally:
        _ready.set()
    if _BG:
        bg_loop()


@app.before_request
def _wait_ready():
    if request.path.startswith("/api/") and not _ready.wait(90):
        return jsonify({"error": "起動中です。数秒後に自動で再試行します"}), 503


if _BG:
    threading.Thread(target=_startup, daemon=True, name="snowcast-bg").start()
else:
    _startup()
if _BG:
    _ka = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("SNOWCAST_KEEPALIVE_URL")
    if _ka:
        threading.Thread(target=keepalive_loop, args=(_ka,), daemon=True, name="snowcast-keepalive").start()
    if backup_enabled():
        atexit.register(lambda: db_backup(force=True))   # デプロイ/停止時に最後の退避

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
