"""MACRO PULSE の修復スクリプト用: ALFRED（FREDの版の履歴）から観測ごとの版を取る（2026-10-04 指示書M-3）

fetch_versions(series_id) は {観測日: [(realtime_start, realtime_end, 値), ...]}（realtime_start昇順）を返す。
- FRED API の series/observations（output_type=1、全版）を、版の日付が2,000以下になるように実時間の期間を分けて取る
  （output_type=4 などは「版の日付が2,000を超える期間」を受け付けない。T10Y2Y・VIXCLS・T5YIEが該当）
- 1回の応答は最大100,000件。超える場合はoffsetで続きを取る
- 期間の境目で同じ版が2つに分かれた分は、値が同じで連続していれば1つにまとめる
- 結果は cache_dir にJSONで保存し、2回目以降はそれを使う（FRED APIの呼び出しを減らすため）

ALFREDの版の記録には始まりの日がある（例: PERMIT 1999-08-17、IC4WSA 2009-05-28、T10Y2Y 2014-01-27、
BAMLH0A0HYM2 2023-10-03）。その日より前の観測の最初の版のrealtime_startは記録の始まりの日で、実際の初回公表日
ではない。first_release() はそういう観測について None を返す（呼び出し元で推定する）。
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

API = "https://api.stlouisfed.org/fred/"
OPEN_END = "9999-12-31"
TZ_NY = ZoneInfo("America/New_York")

# events.csv の indicator → FRED系列（NFPはPAYEMSの水準から前月比を作る）
SERIES_FOR_INDICATOR = {
    "Yield Curve 10Y-2Y": "T10Y2Y",
    "HY Spread": "BAMLH0A0HYM2",
    "VIX": "VIXCLS",
    "Michigan Inflation 5Y": "T5YIE",
    "Initial Claims 4W MA": "IC4WSA",
    "Sahm Rule Recession Indicator": "SAHMCURRENT",
    "Building Permits": "PERMIT",
    "NFP": "PAYEMS",
    "Michigan Consumer Sentiment": "UMCSENT",
    "Philadelphia Fed Manufacturing": "GACDFSA066MSFRBPHI",
    "Chicago Fed National Activity": "CFNAIMA3",
    "Michigan Inflation 1Y": "MICH",
}

# 観測日から公表までの日数の推定（ALFREDに無い行に使う）。INDICATOR_CONFIG の obs_to_release_lag がある指標は同じ値。
# 無い指標は、公表の慣行からの目安（CFNAI=翌月下旬、Philly=当月第3木曜、Sahm=翌月第1金曜、日次=翌営業日）
LAG_DAYS = {
    "Yield Curve 10Y-2Y": 1, "HY Spread": 1, "VIX": 1, "Michigan Inflation 5Y": 1,
    "Initial Claims 4W MA": 7, "NFP": 35, "Building Permits": 47, "Michigan Consumer Sentiment": 10,
    "Michigan Inflation 1Y": 10, "Philadelphia Fed Manufacturing": 18, "Chicago Fed National Activity": 52,
    "Sahm Rule Recession Indicator": 35, "CB Consumer Confidence": 27, "Conference Board LEI": 50,
}


def et_0830_utc(d: str) -> str:
    """'YYYY-MM-DD' の米国東部時間 08:30 を UTC の 'YYYY-MM-DDTHH:MM:SSZ' にする。"""
    dt = datetime.combine(date.fromisoformat(d), datetime.min.time().replace(hour=8, minute=30), tzinfo=TZ_NY)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def plus_days(d: str, n: int) -> str:
    return (date.fromisoformat(d) + timedelta(days=n)).isoformat()


def _get(params: dict, api_key: str) -> dict:
    for attempt in range(4):
        r = requests.get(API + "series/observations" if "output_type" in params else API + "series/vintagedates",
                         params={**params, "api_key": api_key, "file_type": "json"}, timeout=180)
        if r.status_code == 429:
            time.sleep(20 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return {}


def fetch_versions(series_id: str, api_key: str, cache_dir: str) -> dict:
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"alfred_{series_id}.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    vd = _get({"series_id": series_id, "limit": 10000}, api_key).get("vintage_dates", [])
    windows = []
    for i in range(0, len(vd), 1999):
        start = vd[i]
        end = (date.fromisoformat(vd[i + 1999]) - timedelta(days=1)).isoformat() if i + 1999 < len(vd) else OPEN_END
        windows.append((start, end))
    raw: dict = {}
    for ws, we in windows:
        offset = 0
        while True:
            j = _get({"series_id": series_id, "realtime_start": ws, "realtime_end": we, "output_type": 1,
                      "limit": 100000, "offset": offset}, api_key)
            obs = j.get("observations", [])
            for x in obs:
                if x["value"] == ".":
                    continue
                raw.setdefault(x["date"], []).append([x["realtime_start"], x["realtime_end"], float(x["value"])])
            offset += len(obs)
            if not obs or offset >= int(j.get("count", 0)):
                break
    out = {}
    for d, vs in raw.items():
        vs.sort()
        merged = []
        for s, e, v in vs:
            if merged and merged[-1][2] == v and plus_days(merged[-1][1], 1) >= s:
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e, v])
        out[d] = merged
    result = {"first_vintage": vd[0] if vd else None, "versions": out}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f)
    return result


def first_release(data: dict, obs_date: str):
    """(初回公表日, 初回の値)。ALFREDの記録の始まりより前の観測はNone。"""
    vs = data["versions"].get(obs_date)
    if not vs:
        return None
    s, _, v = vs[0]
    if data.get("first_vintage") and s <= data["first_vintage"] and obs_date < data["first_vintage"]:
        return None
    return s, v


def latest_version(data: dict, obs_date: str):
    """(最新の版の公表日, 最新の値)。"""
    vs = data["versions"].get(obs_date)
    if not vs:
        return None
    s, _, v = vs[-1]
    return s, v


def value_at(data: dict, obs_date: str, when: str):
    """when（'YYYY-MM-DD'）の時点で有効だった版の値。"""
    for s, e, v in data["versions"].get(obs_date, []):
        if s <= when <= e:
            return v
    return None
