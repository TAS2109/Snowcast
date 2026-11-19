"""SNOW CAST 静的サイト生成（GitHub Actions 用）。

  1) 前回のDB(snowcast.sqlite.gz)を復元
  2) アメダス実測の取り込み → 1日1回の再学習 → 予報の再計算（スナップショット保存も app.py と同じ）
  3) site/ に index.html と data/*.json を出力 → GitHub Pages で公開
  4) DBを snowcast.sqlite.gz に固めて返す（ワークフローが専用ブランチへ保存）

app.py は変更不要。Render 版と同じ計算ロジックをそのまま使う。
"""
import gzip
import json
import os
import re
import shutil
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(ROOT, "snowcast.sqlite")
DB_GZ = DB + ".gz"
SITE = os.path.join(ROOT, "site")


def log(*a):
    print("[build]", *a, flush=True)


def static_html(src):
    """app.py の画面を、サーバ無し（JSONファイル読み込み）用に書き換える。"""
    pairs = [
        ("fetch('/api/verification?days=30')", "fetch('data/verification.json?'+Date.now())"),
        ("fetch('/api/observations?hours=36')", "fetch('data/observations.json?'+Date.now())"),
        ("fetch('/api/forecast'+(force?'?refresh=1':''))", "fetch('data/forecast.json?'+Date.now())"),
    ]
    for old, new in pairs:
        assert src.count(old) == 1, "画面側のfetch箇所が見つかりません: " + old
        src = src.replace(old, new)
    # 再学習ボタンはサーバが無いので隠す（学習は毎日自動で実行される）
    return src.replace("</style>", "#btnTrain,#trainRes{display:none}\n</style>", 1)


def main(patch=None):
    os.chdir(ROOT)
    if os.path.exists(DB_GZ):
        with gzip.open(DB_GZ, "rb") as f, open(DB, "wb") as o:
            shutil.copyfileobj(f, o)
        log("DB restored")
    os.environ["SNOWCAST_DB"] = DB
    os.environ["SNOWCAST_BG"] = "0"
    sys.path.insert(0, ROOT)
    import app as A
    if patch:
        patch(A)
    A._refresh_async = lambda: None          # 再学習後の非同期更新は使わない（下で順に実行する）

    try:
        A.ingest_obs()
    except Exception as e:  # noqa
        log("ingest_obs failed:", e)

    today = A.now_jst().strftime("%Y-%m-%d")
    if A.now_jst().hour >= 4 and A.kv_get("last_train_day") != today:
        try:
            A._retrain()
            A.kv_set("last_train_day", today)
            log("retrained")
        except Exception as e:  # noqa
            log("retrain failed:", e)

    try:
        fc = A.get_forecast(force=True, fresh=True)
    except Exception as e:  # noqa
        log("forecast failed:", e)
        if not A._cache["data"]:
            sys.exit("予報を作れず、前回の予報も残っていません")
        fc = dict(A._cache["data"], stale=True,
                  age_min=int((A.time.time() - A._cache["time"]) // 60))   # 前回分を『古い』印つきで出す
    errs = [m for m in fc.get("models", []) if m.get("kind") == "error"]
    log("forecast ok; models:", len(fc.get("models", [])), "errors:", len(errs), "stale:", bool(fc.get("stale")))

    since = A.tstr(A.hour_floor() - A.timedelta(hours=36))
    with A.db() as con:
        obs = [dict(x) for x in con.execute("SELECT * FROM obs WHERE time>=? ORDER BY time", (since,))]
    ver = A.verification(30)

    os.makedirs(os.path.join(SITE, "data"), exist_ok=True)
    for name, obj in (("forecast", fc), ("verification", ver), ("observations", {"rows": obs})):
        with open(os.path.join(SITE, "data", name + ".json"), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    with open(os.path.join(SITE, "index.html"), "w", encoding="utf-8") as f:
        f.write(static_html(A.INDEX_HTML))
    open(os.path.join(SITE, ".nojekyll"), "w").close()

    # DBを固めて保存用に出力（WALをDBに取り込んでから）
    with A.db() as con:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    with open(DB, "rb") as f, gzip.open(DB_GZ, "wb", compresslevel=9) as o:
        shutil.copyfileobj(f, o)
    log("done; db.gz = %d KB" % (os.path.getsize(DB_GZ) // 1024))


if __name__ == "__main__":
    main()
