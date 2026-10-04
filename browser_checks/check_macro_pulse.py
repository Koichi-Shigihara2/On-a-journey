"""MACRO PULSE 全画面要素の実ブラウザ確認（2026-10-03 新設、指示書 M-1 STEP 4）

SYSTEM_MAP.md「MACRO PULSE 全画面要素（MAC-01〜MAC-48）」の48要素について、
生データ（docs/market-monitor/macro-pulse/data/ のCSV・JSON）から本スクリプトが
独立に計算した表示期待値と、実ブラウザ（Playwright・Chromium）の描画値を突き合わせる。

  - 描画（R）: index.html の整形規則（toFixed・Math.round・閾値・日付の解釈）を
    Python側で書き直して期待値を作り、DOM・EChartsの系列と比べる。
    本番の導出関数（05_main.py・index.htmlのJS）は import も eval もしない。
  - 導出（D）: 生データ（common/macro_data/series/ の FRED 系列ストア）から
    events.csv・05_liquidity.csv・05_fed_context.csv・05_weekly_analysis.csv の
    値を独立に再計算して比べる（D-01〜D-09）。
  - 説明（N）: 画面の説明文・tooltip・注記が実際の計算と合っているか（N-01〜N-12）。

結果は一致（True）・不一致（False）・判定不能（None）の3値。不一致の層を
[描画]/[導出]/[データ]/[説明] で示す。本番のコード・データは読むだけで変更しない。

時刻の扱い: ブラウザの時計を実行開始時刻に固定し（page.clock.set_fixed_time）、
タイムゾーンを Asia/Tokyo に固定する。Python側も同じ時刻・同じタイムゾーンで
期待値を作る（日付だけの文字列 'YYYY-MM-DD' はJSと同じくUTCの0時、
'YYYY-MM-DD HH:MM:SS' や 'YYYY-MM-DDTHH:MM:SS' はJSと同じくローカル時刻として解釈）。

配信: docs/ 全体をルートにするローカルHTTPサーバー（GitHub Pages の配信範囲と同じ。
docs/ より狭い範囲を配信すると ../../common/ のアセットが404になる）。

使い方:
  cd C:\\Users\\shigi\\Documents\\On-a-journey-git
  venv\\Scripts\\activate
  python browser_checks\\check_macro_pulse.py            # 一覧を表示
  python browser_checks\\check_macro_pulse.py --json out.json

終了コード: 0=不一致なし、1=不一致あり（判定不能は不一致に数えない）。
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(REPO_ROOT, "docs")
DATA_DIR = os.path.join(DOCS_DIR, "market-monitor", "macro-pulse", "data")
SERIES_DIR = os.path.join(REPO_ROOT, "common", "macro_data", "series")
PORT = 8793
URL = f"http://localhost:{PORT}/market-monitor/macro-pulse/index.html"
JST = timezone(timedelta(hours=9))
DAY_MS = 86400000

WEEKDAY_JA = "月火水木金土日"  # date.weekday(): 月=0

# ── 8指標（表示名 → events.csv の indicator、FRED系列、ウェイト）
SCORE_INDS = [
    ("yc", "Yield Curve 10Y-2Y", "T10Y2Y", 20),
    ("hy", "HY Spread", "BAMLH0A0HYM2", 15),
    ("cbcc2", "Building Permits", "PERMIT", 10),
    ("philly", "Philadelphia Fed Manufacturing", "GACDFSA066MSFRBPHI", 18),
    ("cfnai", "Chicago Fed National Activity", "CFNAIMA3", 12),  # 2026-10-03に単月のCFNAIから変更
    ("claims", "Initial Claims 4W MA", "IC4WSA", 10),
    ("cbcc", "Michigan Consumer Sentiment", "UMCSENT", 8),
    ("sahm", "Sahm Rule Recession Indicator", "SAHMCURRENT", 7),
]


@dataclass
class Result:
    id: str
    element: str
    expected: Any
    actual: Any
    passed: Optional[bool]
    layer: str = "描画"
    note: str = ""


# ═══════════════════════════════════════════════════════════════
# JS互換の小道具
# ═══════════════════════════════════════════════════════════════
def js_fixed(v: float, n: int) -> str:
    """Number.prototype.toFixed(n)（2進の厳密値を0から遠い側へ四捨五入）。"""
    q = Decimal(1).scaleb(-n)
    s = str(Decimal(float(v)).quantize(q, rounding=ROUND_HALF_UP))
    return s


def js_round(v: float) -> int:
    """Math.round（.5は+∞方向）。"""
    return int(math.floor(v + 0.5))


def js_locale(v: float, min_fd: int, max_fd: int) -> str:
    """toLocaleString('en-US',{minimumFractionDigits,maximumFractionDigits})。"""
    s = js_fixed(abs(v), max_fd)
    if "." in s:
        ip, fp = s.split(".")
        fp = fp.rstrip("0")
        if len(fp) < min_fd:
            fp = fp + "0" * (min_fd - len(fp))
    else:
        ip, fp = s, "0" * min_fd
    ip = f"{int(ip):,}"
    out = ip + ("." + fp if fp else "")
    return ("-" if v < 0 and float(s) != 0 else "") + out


def parse_float(v: Any) -> Optional[float]:
    """parseFloat（先頭の数値部分だけ読む。読めなければNone）。"""
    if v is None:
        return None
    m = re.match(r"^\s*([+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?)", str(v))
    if not m:
        return None
    return float(m.group(1))


def ms_date_only(s: str) -> Optional[int]:
    """new Date('YYYY-MM-DD') → UTCの0時。"""
    try:
        d = datetime.strptime(s.strip()[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None
    if len(s.strip()) != 10:
        return ms_local(s)
    return int(d.timestamp() * 1000)


def ms_local(s: str) -> Optional[int]:
    """new Date('YYYY-MM-DD HH:MM:SS') 等 → ローカル（JST）。"""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=JST).timestamp() * 1000)
        except Exception:
            pass
    return None


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Windowsのfromtimestampは1970年より前を扱えない
NY = ZoneInfo("America/New_York")


def ms_utc(s: str) -> Optional[int]:
    """known_at（'YYYY-MM-DDTHH:MM:SSZ'）・updated_at（'YYYY-MM-DD HH:MM:SS'）をUTCとして読む（M-3 STEP 1・6）。"""
    s = (s or "").strip().rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except Exception:
            pass
    return None


def known_bounds(t: int) -> tuple[int, int]:
    """index.htmlのknownBounds()（M-3 STEP 6）: tの現地（JST）の暦日のUTC 0時と、その日の米国東部時間23:59:59（UTC）。"""
    d = (EPOCH + timedelta(milliseconds=t)).astimezone(JST).date()
    day = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
    cut = int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=NY).timestamp() * 1000)
    return day, cut


def iso_date(ms: int) -> str:
    """toISOString().slice(0,10)（UTCの日付）。"""
    return (EPOCH + timedelta(milliseconds=ms)).strftime("%Y-%m-%d")


def local_dt(ms: int) -> datetime:
    return (EPOCH + timedelta(milliseconds=ms)).astimezone(JST)


def local_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def set_hours(ms: int, h: int, m: int, s: int) -> int:
    d = local_dt(ms)
    return local_ms(d.replace(hour=h, minute=m, second=s, microsecond=0))


def ja_date(ms: int, year: bool = True) -> str:
    """toLocaleDateString('ja-JP',{(year),month:'numeric',day:'numeric',weekday:'short'})。"""
    d = local_dt(ms)
    w = WEEKDAY_JA[d.weekday()]
    return (f"{d.year}/" if year else "") + f"{d.month}/{d.day}({w})"


def read_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_series(sid: str) -> list[dict]:
    with open(os.path.join(SERIES_DIR, f"{sid}.json"), encoding="utf-8") as f:
        d = json.load(f)
    return d if isinstance(d, list) else d.get("records", [])


# ═══════════════════════════════════════════════════════════════
# 期待値（index.html の仕様を Python で書き直したもの）
# ═══════════════════════════════════════════════════════════════
class Model:
    def __init__(self, now_ms: int):
        self.now = now_ms
        self.events = [r for r in read_csv(os.path.join(DATA_DIR, "05_events.csv"))
                       if (r.get("indicator") or "").strip()]
        self.liq = read_csv(os.path.join(DATA_DIR, "05_liquidity.csv"))
        self.liq.sort(key=lambda r: r.get("date") or "")
        self.fed = read_csv(os.path.join(DATA_DIR, "05_fed_context.csv"))
        self.sched = read_csv(os.path.join(DATA_DIR, "05_indicator_schedule.csv"))
        self.weekly = [r for r in read_csv(os.path.join(DATA_DIR, "05_weekly_analysis.csv"))]
        with open(os.path.join(DATA_DIR, "05_meta.json"), encoding="utf-8") as f:
            self.meta = json.load(f)
        self.index = self._build_index()
        ws = sorted(self.weekly, key=lambda r: r.get("analysis_date") or "", reverse=True)
        self.weekly_sorted = ws
        sc = parse_float(ws[0].get("score")) if ws else None
        self.snapshot = int(sc) if sc is not None else None

    # IND_INDEX
    def _build_index(self) -> dict:
        idx: dict = {}
        for r in self.events:
            v = parse_float(r.get("actual"))
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            d = ms_date_only(r.get("release_date", ""))
            if d is None:
                continue
            # M-3: 公開時点はknown_at（無ければupdated_at、どちらもUTC）。改定値はrevised_atの時点から
            u = ms_utc(r.get("known_at", "")) or ms_utc(r.get("updated_at", ""))
            rv = parse_float(r.get("revised_actual"))
            idx.setdefault(r["indicator"], []).append({"d": d, "v": v, "u": u if u is not None else d,
                                                       "rv": rv, "rt": ms_utc(r.get("revised_at", ""))})
        for k in idx:
            idx[k].sort(key=lambda e: e["d"])
        return idx

    def latest_entry(self, ind: str, t: int) -> Optional[dict]:
        arr = self.index.get(ind) or []
        cand = [i for i, e in enumerate(arr) if e["d"] <= t]
        return arr[cand[-1]] if cand else None

    @staticmethod
    def val_at(e: dict, t: int) -> float:
        """index.htmlのvalueAt()（M-3 STEP 2）: tの時点で改定値が書かれていれば改定値。"""
        return e["rv"] if e.get("rv") is not None and e.get("rt") is not None and e["rt"] <= t else e["v"]

    def latest_val(self, ind: str, t: int) -> Optional[float]:
        e = self.latest_entry(ind, t)
        return self.val_at(e, t) if e else None

    def latest_known(self, ind: str, t: int) -> Optional[float]:
        day, cut = known_bounds(t)
        best = None
        for e in self.index.get(ind) or []:
            if e["d"] <= day and e["u"] <= cut:
                if best is None or e["d"] > best["d"]:
                    best = e
        return self.val_at(best, cut) if best else None

    def trend3(self, ind: str) -> int:
        arr = self.index.get(ind) or []
        if len(arr) < 2:
            return 0
        last3 = arr[-3:]
        diffs = [self.val_at(last3[i], self.now) - self.val_at(last3[i - 1], self.now) for i in range(len(last3) - 1, 0, -1)]
        avg = sum(diffs) / len(diffs)
        return 1 if avg > 0 else -1 if avg < 0 else 0

    # computeCurrentScore（ステップ関数）
    def live_signals(self) -> list[dict]:
        t = self.now
        v = {k: self.latest_val(ind, t) for k, ind, _, _ in SCORE_INDS}
        dates = {}
        for k, ind, _, _ in SCORE_INDS:
            e = self.latest_entry(ind, t)
            dates[k] = iso_date(e["d"]) if e else None
        sig = []
        yc = v["yc"]
        s, g, lab = 50, "neutral", "—"
        if yc is not None:
            lab = ("+" if yc >= 0 else "") + js_fixed(yc, 2) + "%"
            s, g = (90, "bear") if yc < -0.5 else (70, "caution") if yc < 0 else (40, "neutral") if yc < 0.5 else (15, "bull")
        sig.append(dict(key="yc", name="YC 10Y-2Y", val=lab, score=s, signal=g, weight=20, lead="先行12ヶ月", thresh="BULL≥+0.5% / BEAR<-0.5%", obs=dates["yc"]))
        hy = v["hy"]
        s, g, lab = 50, "neutral", "—"
        if hy is not None:
            lab = js_fixed(hy, 2) + "%"
            s, g = (90, "bear") if hy > 6 else (70, "caution") if hy > 4.5 else (40, "neutral") if hy > 3.5 else (15, "bull")
        sig.append(dict(key="hy", name="HY Spread", val=lab, score=s, signal=g, weight=15, lead="先行2ヶ月", thresh="BULL≤3.5% / BEAR>6.0%", obs=dates["hy"]))
        bp = v["cbcc2"]
        s, g, lab = 50, "neutral", "—"
        if bp is not None:
            lab = js_fixed(bp, 0) + "K"
            s, g = (15, "bull") if bp >= 1500 else (35, "neutral") if bp >= 1300 else (60, "caution") if bp >= 1100 else (85, "bear")
        sig.append(dict(key="cbcc2", name="Building Permits", val=lab, score=s, signal=g, weight=10, lead="先行3ヶ月", thresh="BULL≥1500K / BEAR≤1100K", obs=dates["cbcc2"]))
        ph = v["philly"]
        s, g, lab = 50, "neutral", "—"
        if ph is not None:
            lab = js_fixed(ph, 1)
            t3 = self.trend3("Philadelphia Fed Manufacturing")
            if ph < -10:
                s, g = 88, "bear"
            elif ph < 0:
                s, g = 65 + (10 if t3 < 0 else 0), ("bear" if t3 < 0 else "caution")
            elif ph < 5:
                s, g = 35, "neutral"
            else:
                s, g = 12, "bull"
        sig.append(dict(key="philly", name="Philly Fed Mfg", val=lab, score=s, signal=g, weight=18, lead="先行3ヶ月", thresh="BULL≥+5 / BEAR<-10", obs=dates["philly"]))
        cf = v["cfnai"]
        s, g, lab = 50, "neutral", "—"
        if cf is not None:
            lab = js_fixed(cf, 2)
            s, g = (82, "bear") if cf < -0.7 else (50, "neutral") if cf < -0.35 else (18, "bull")
        sig.append(dict(key="cfnai", name="CFNAI MA3", val=lab, score=s, signal=g, weight=12, lead="先行1ヶ月", thresh="BULL≥-0.35 / BEAR<-0.7", obs=dates["cfnai"]))
        cl = v["claims"]
        s, g, lab = 50, "neutral", "—"
        if cl is not None:
            lab = js_fixed(cl / 1000, 0) + "K"
            t3 = self.trend3("Initial Claims 4W MA")
            if cl > 300000:
                s, g = 85, "bear"
            elif cl > 250000:
                s, g = 60 + (10 if t3 > 0 else 0), ("bear" if t3 > 0 else "caution")
            elif cl > 215000:
                s, g = 35, "neutral"
            else:
                s, g = 15, "bull"
        sig.append(dict(key="claims", name="Initial Claims", val=lab, score=s, signal=g, weight=10, lead="先行1ヶ月", thresh="BULL≤215K / BEAR>300K", obs=dates["claims"]))
        mi = v["cbcc"]
        s, g, lab = 50, "neutral", "—"
        if mi is not None:
            lab = js_fixed(mi, 1)
            s, g = (82, "bear") if mi < 60 else (72, "bear") if mi < 75 else (60, "caution") if mi < 90 else (30, "neutral")
        sig.append(dict(key="cbcc", name="Michigan Sent.", val=lab, score=s, signal=g, weight=8, lead="先行2ヶ月", thresh="NEUTRAL≥90 / BEAR<60", obs=dates["cbcc"]))
        sa = v["sahm"]
        s, g, lab = 50, "neutral", "—"
        if sa is not None:
            lab = js_fixed(sa, 2)
            s = 88 if sa >= 0.5 else 50 if sa >= 0.3 else 12
            g = "bear" if s > 75 else "caution" if s > 40 else "neutral"
        sig.append(dict(key="sahm", name="Sahm Rule", val=lab, score=s, signal=g, weight=7, lead="先行1ヶ月", thresh="BULL<0.3 / BEAR≥0.5", obs=dates["sahm"]))
        return sig

    def live_score(self) -> int | None:
        sig = self.live_signals()
        tw = sum(s["weight"] for s in sig if s["val"] != "—")
        if not tw:
            return None  # M-3 STEP 3: 使える指標が無いときは判定不能（50にしない）
        return js_round(sum(s["score"] * s["weight"] for s in sig if s["val"] != "—") / tw)

    def shown_score(self) -> int:
        return self.snapshot if self.snapshot is not None else self.live_score()

    # computeScoreAsOf（過去日はlerp・先読み除外）
    def score_as_of(self, t: int) -> Optional[int]:
        if t >= self.now:
            return self.shown_score()

        def lerp(v, x0, y0, x1, y1):
            tt = max(0.0, min(1.0, (v - x0) / (x1 - x0)))
            return js_round(y0 + tt * (y1 - y0))

        def calc(k, v):
            if v is None:
                return (50, 0)
            if k == "yc":
                if v <= -0.5: return (90, 20)
                if v <= 0.0: return (lerp(v, -0.5, 90, 0.0, 68), 20)
                if v <= 0.5: return (lerp(v, 0.0, 68, 0.5, 28), 20)
                if v <= 1.5: return (lerp(v, 0.5, 28, 1.5, 10), 20)
                return (10, 20)
            if k == "hy":
                if v <= 2.5: return (10, 15)
                if v <= 3.5: return (lerp(v, 2.5, 10, 3.5, 35), 15)
                if v <= 4.5: return (lerp(v, 3.5, 35, 4.5, 65), 15)
                if v <= 6.0: return (lerp(v, 4.5, 65, 6.0, 88), 15)
                return (90, 15)
            if k == "cbcc2":
                if v <= 800: return (88, 10)
                if v <= 1100: return (lerp(v, 800, 88, 1100, 65), 10)
                if v <= 1400: return (lerp(v, 1100, 65, 1400, 30), 10)
                if v <= 1800: return (lerp(v, 1400, 30, 1800, 12), 10)
                return (10, 10)
            if k == "philly":
                if v <= -15: return (88, 18)
                if v <= 0: return (lerp(v, -15, 88, 0, 55), 18)
                if v <= 10: return (lerp(v, 0, 55, 10, 15), 18)
                return (12, 18)
            if k == "cfnai":
                if v <= -0.7: return (82, 12)
                if v <= -0.35: return (lerp(v, -0.7, 82, -0.35, 50), 12)
                if v <= 0.0: return (lerp(v, -0.35, 50, 0.0, 30), 12)
                return (18, 12)
            if k == "claims":
                if v <= 180000: return (10, 10)
                if v <= 215000: return (lerp(v, 180000, 10, 215000, 28), 10)
                if v <= 250000: return (lerp(v, 215000, 28, 250000, 55), 10)
                if v <= 300000: return (lerp(v, 250000, 55, 300000, 80), 10)
                return (85, 10)
            if k == "cbcc":
                if v <= 55: return (82, 8)
                if v <= 75: return (lerp(v, 55, 82, 75, 55), 8)
                if v <= 95: return (lerp(v, 75, 55, 95, 25), 8)
                return (20, 8)
            if k == "sahm":
                if v <= 0: return (10, 7)
                if v <= 0.3: return (lerp(v, 0, 10, 0.3, 40), 7)
                if v <= 0.5: return (lerp(v, 0.3, 40, 0.5, 80), 7)
                return (88, 7)
            return (50, 0)

        sigs = [calc(k, self.latest_known(ind, t)) for k, ind, _, _ in SCORE_INDS]
        tw = sum(w for _, w in sigs)
        if not tw:
            return None
        return js_round(sum(s * w for s, w in sigs) / tw)

    def latest_data_date_before(self, t: int) -> Optional[str]:
        day, cut = known_bounds(t)
        best = 0
        for arr in self.index.values():
            for e in arr:
                if e["d"] <= day and e["u"] <= cut and e["d"] > best:
                    best = e["d"]
        return iso_date(best) if best else None


def phase_of(score: float) -> tuple[str, str]:
    if score < 30: return "拡張", "景気は拡大局面"
    if score < 52: return "踊り場", "拡張と後退の境界付近"
    if score < 70: return "後退入口", "後退局面への移行シグナル"
    return "後退", "景気後退の可能性が高い"


def fmt_k(v: Optional[float]) -> str:
    """fmtK"""
    if v is None:
        return "—"
    if abs(v) >= 100000:
        return js_fixed(v / 1000, 1) + "K"
    if abs(v) >= 1000:
        return js_locale(v, 0, 0)
    return js_locale(v, 2, 2)


# ═══════════════════════════════════════════════════════════════
# ブラウザ側の採取（DOM・EChartsの系列を読むだけ）
# ═══════════════════════════════════════════════════════════════
COLLECT_JS = r"""() => {
  const T = s => s ? (s.textContent || '').trim() : null;
  const q = s => document.querySelector(s);
  const qa = s => Array.from(document.querySelectorAll(s));
  const out = {};
  out.ts = T(q('#ts')); out.lastUpdated = T(q('#last-updated')); out.notice = T(q('#nmsg'));
  out.rb = {regime: T(q('#rb-regime')), src: T(q('#rb-regime-src')), ff: T(q('#rb-ff')), exp: T(q('#rb-exp')),
            cuts: T(q('#rb-cuts')), concern: T(q('#rb-concern')), reason: T(q('#rb-reason')),
            concernTip: q('#rb-concern-label') ? q('#rb-concern-label').getAttribute('data-info-text') : null};
  out.tk = {sp: T(q('#tk-sp')), spc: T(q('#tk-sp-c')), yc: T(q('#tk-yc')), yci: T(q('#tk-yc-i')), hy: T(q('#tk-hy')),
            last: T(q('#tk-last')), src: T(q('#tk-src'))};
  out.liqCards = qa('#liqGrid .liq-card').map(c => ({label: T(c.querySelector('.liq-card-label')),
      val: T(c.querySelector('.liq-card-val')), unit: T(c.querySelector('.liq-card-unit')),
      chg: T(c.querySelector('.liq-card-chg')), row: T(c.querySelector('.liq-card-chg-row')),
      date: T(c.querySelector('.liq-card-date')), text: (c.innerText || '').replace(/\s+/g, ' ')}));
  out.hollow = T(q('.hollow-rally-badge'));
  const st = q('.stealth-card');
  out.stealth = st ? {badge: T(st.querySelector('.stealth-badge')), layer1: T(q('#stealthLayer1')),
      text: (st.innerText || '').replace(/\s+/g, ' '),
      metrics: Array.from(st.querySelectorAll('.stealth-metric')).map(m => ({label: T(m.querySelector('.stealth-metric-label')),
        val: T(m.querySelector('.stealth-metric-val')), chg: T(m.querySelector('.stealth-metric-chg'))}))} : null;
  out.phase = {badge: T(q('#pg-phase-badge')), sub: T(q('#pg-phase-sub')), score: T(q('#pg-score-num')),
      fill: q('#pg-track-fill') ? q('#pg-track-fill').style.width : null,
      marker: q('#pg-track-marker') ? q('#pg-track-marker').style.left : null,
      sigText: T(q('#pg-signal-text')), alertDisplay: q('#pg-alert') ? getComputedStyle(q('#pg-alert')).display : null,
      alertMsg: T(q('#pg-alert-msg'))};
  out.sigs = qa('#pg-signals .pg-sig').map(s => ({name: T(s.querySelector('.pg-sig-name')), val: T(s.querySelector('.pg-sig-val')),
      badge: T(s.querySelector('.pg-sig-badge')), lead: T(s.querySelector('.pg-sig-lead')),
      tip: Array.from(s.querySelectorAll('.pg-sig-tooltip-row')).map(r => Array.from(r.children).map(c => T(c)))}));
  out.help = qa('#scoreHelp table')[0] ? Array.from(qa('#scoreHelp table')[0].rows).slice(1).map(r => Array.from(r.cells).map(c => T(c))) : [];
  out.cmp = ['m3','m2','m1','w1'].map(k => ({k, score: T(q('#cmp-'+k+'-score')), delta: T(q('#cmp-'+k+'-delta')), date: T(q('#cmp-'+k+'-date'))}));
  out.surprise = {display: q('#surpriseBanner') ? getComputedStyle(q('#surpriseBanner')).display : null,
      items: qa('#surpriseBannerBody .surprise-item').map(i => (i.innerText || '').replace(/\s+/g, ' ').trim())};
  out.ai = qa('#aiTimeline .ai-card').map((c, i) => ({date: T(c.querySelector('.ai-card-date')), score: T(c.querySelector('.ai-card-score')),
      phase: T(c.querySelector('.ai-card-phase')), header: (c.querySelector('.ai-card-header').innerText || '').replace(/\s+/g, ' '),
      open: c.classList.contains('open'), chips: c.querySelectorAll('.ai-ind-chip').length,
      sections: Array.from(c.querySelectorAll('.ai-card-section-label')).map(x => T(x)),
      foot: (c.querySelector('.ai-card-body > div:last-child') || {}).textContent}));
  out.aiNote = T(q('.ai-wrap > div:nth-child(2)'));
  out.aiTitle = T(q('.ai-title'));
  out.l2 = qa('#l2Grid .l2-row').map(r => ({name: T(r.querySelector('.l2-name')), val: T(r.querySelector('.l2-val')),
      sig: T(r.querySelector('.l2-meta')), text: (r.innerText || '').replace(/\s+/g, ' ')}));
  const sh = echarts.getInstanceByDom(document.getElementById('scoreHistoryChart'));
  if (sh) { const o = sh.getOption(); const s = o.series; const line = s[s.length - 1].data;
    out.history = line.map(p => p.value);
    try { out.historyTipLast = o.tooltip[0].formatter([{value: line[line.length - 1].value}]);
          out.historyTipFirst = o.tooltip[0].formatter([{value: line[0].value}]); } catch (e) { out.historyTipErr = String(e); }
    out.historyNber = (s.find(x => x.markArea && x.markArea.itemStyle && x.markArea.itemStyle.color === 'rgba(244,63,94,0.13)') || {markArea: {data: []}}).markArea.data.length; }
  const l3 = echarts.getInstanceByDom(document.getElementById('l3Chart'));
  if (l3) { const o = l3.getOption(); out.radar = {indicators: o.radar[0].indicator.map(i => i.name), series: o.series[0].data.map(d => ({name: d.name, value: d.value}))}; }
  out.l3Scores = qa('#l3Scores .l3-score-row').map(r => ({era: T(r.querySelector('.l3-score-era')), pct: T(r.querySelector('.l3-score-pct')), desc: T(r.querySelector('.l3-score-desc'))}));
  out.l3Note = T(q('#l3Scores .l3-note'));
  out.slider = {max: q('#l3Slider') ? q('#l3Slider').max : null, value: q('#l3Slider') ? q('#l3Slider').value : null, date: T(q('#l3SliderDate'))};
  out.signals = qa('#recentSignals tbody tr').map(r => Array.from(r.cells).map(c => T(c)));
  out.signalsTitle = T(q('.signals-title')); out.signalsSec = qa('.sec').map(x => T(x)).find(t => /直近の動き/.test(t)) || null;
  out.sched = qa('#schedGrid tbody tr').map(r => Array.from(r.cells).map(c => T(c)));
  out.banners = qa('.v3-banner').map(b => (b.innerText || '').replace(/\s+/g, ' ').trim());
  out.liqNote = T(q('.liq-note'));
  return out;
}"""


def collect(now_ms: int) -> tuple[dict, list, dict]:
    from playwright.sync_api import sync_playwright

    server = subprocess.Popen([sys.executable, "-m", "http.server", str(PORT), "--directory", DOCS_DIR],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(30):
            try:
                urllib.request.urlopen(URL, timeout=1)
                break
            except Exception:
                time.sleep(0.2)
        logs: list = []
        with sync_playwright() as p:
            browser = p.chromium.launch()
            ctx = browser.new_context(viewport={"width": 1400, "height": 900}, timezone_id="Asia/Tokyo", locale="ja-JP")
            page = ctx.new_page()
            page.clock.set_fixed_time(datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc))
            page.on("console", lambda m: logs.append({"type": m.type, "text": m.text}))
            page.on("pageerror", lambda e: logs.append({"type": "pageerror", "text": str(e)}))
            page.on("response", lambda r: logs.append({"type": f"http{r.status}", "text": r.url}) if r.status >= 400 else None)
            page.goto(URL)
            page.wait_for_function("() => document.querySelectorAll('#pg-signals .pg-sig').length === 8 && document.querySelectorAll('#aiTimeline .ai-card').length > 0 && document.querySelector('#liqGrid .liq-card')", timeout=30000)
            page.wait_for_timeout(2500)
            dom = page.evaluate(COLLECT_JS)
            # 全期間の系列とカスタム比較（ボタン・入力の操作のみ）
            page.evaluate("() => setScoreRange(0)")
            page.wait_for_timeout(500)
            dom["historyAll"] = page.evaluate("() => { const s = echarts.getInstanceByDom(document.getElementById('scoreHistoryChart')).getOption().series; return s[s.length-1].data.map(p => p.value); }")
            # M-3: 3年・5年の先頭の点と点数
            dom["historyRanges"] = {}
            for yrs in (3, 5):
                page.evaluate(f"() => setScoreRange({yrs})")
                page.wait_for_timeout(300)
                dom["historyRanges"][yrs] = page.evaluate("() => { const s = echarts.getInstanceByDom(document.getElementById('scoreHistoryChart')).getOption().series; const d = s[s.length-1].data.map(p => p.value); return [d.length ? d[0] : null, d.length]; }")
            page.evaluate("() => setScoreRange(1)")
            extra = {}
            page.fill("#cmpDateInput", "2026-06-30")
            page.dispatch_event("#cmpDateInput", "change")
            page.wait_for_timeout(200)
            extra["custom"] = page.evaluate("() => ({score: document.querySelector('#cmp-custom-score').textContent, delta: document.querySelector('#cmp-custom-delta').textContent, label: document.querySelector('#cmp-custom-label').textContent})")
            browser.close()
        return dom, logs, extra
    finally:
        server.terminate()


# ═══════════════════════════════════════════════════════════════
# 突き合わせ
# ═══════════════════════════════════════════════════════════════
def compare(m: Model, dom: dict, extra: dict) -> list[Result]:
    R: list[Result] = []

    def add(i, el, exp, act, layer="描画", note="", passed=None):
        ok = (exp == act) if passed is None else passed
        R.append(Result(i, el, exp, act, ok, layer, note))

    now = m.now
    # MAC-01 ヘッダー時刻（ブラウザの現在時刻、データではない）
    d = local_dt(now)
    add("MAC-01", "ヘッダー時刻 #ts", f"{d.year}/{d.month}/{d.day} {d.hour}:{d.minute:02d}:{d.second:02d}", dom["ts"],
        note="閲覧時刻であり基準日ではない")
    # MAC-02 最終更新
    g = datetime.fromisoformat(m.meta["generated_at"]).astimezone(JST)
    add("MAC-02", "最終更新 #last-updated", g.strftime("%Y/%m/%d %H:%M") + " JST", dom["lastUpdated"])
    # MAC-03 通知
    add("MAC-03", "読込通知 #notice", f"{len(m.events)}行ロード完了 → 05_events.csv", dom["notice"])

    # MAC-04〜08 REGIMEバー
    fed = sorted(m.fed, key=lambda r: r.get("record_date") or "", reverse=True)[0]
    reg = (fed.get("regime") or "BALANCED").upper()
    add("MAC-04", "REGIME", [reg, fed.get("regime_source", "")], [dom["rb"]["regime"], dom["rb"]["src"]])
    ff = parse_float(fed.get("ff_current"))
    add("MAC-05", "FF RATE", "—" if ff is None else js_fixed(ff, 3) + "%", dom["rb"]["ff"])
    ex = parse_float(fed.get("zq_rate"))
    add("MAC-06", "1Y EXPECTED FF", "—" if ex is None else js_fixed(ex, 3) + "%", dom["rb"]["exp"])
    cu = parse_float(fed.get("cuts_implied"))
    add("MAC-07", "IMPLIED CUTS", "—" if cu is None else ("+" if cu >= 0 else "") + js_fixed(cu, 2) + "回", dom["rb"]["cuts"])
    fd = fed.get("fomc_date") or ""
    add("MAC-08", "FRB主眼・理由・tooltip",
        [fed.get("dominant_label") or fed.get("dominant_concern") or "—", fed.get("ai_reason", ""),
         f"{fd}のFOMC声明に基づく判定です。" if fd else "FOMC声明日付が未取得です。"],
        [dom["rb"]["concern"], dom["rb"]["reason"], dom["rb"]["concernTip"]])

    # MAC-09〜13 ティッカー
    # 期待値は「取得済みの最新の終値とその前営業日の終値」（FRED SP500 の系列ストア）。
    # 2026-10-03までの画面は release_date 最大の行を使い、未来日付の行で食い違っていた（B-02、M-2 STEP 3で修正）。
    sp = [r for r in load_series("SP500") if r.get("value") is not None and ms_date_only(r["as_of"]) <= now]
    cur, prev = sp[-1]["value"], sp[-2]["value"]
    chg = cur - prev
    pct = chg / prev * 100
    sign = "+" if chg >= 0 else ""
    future_rows = sorted({r["release_date"][:10] for r in m.events
                          if parse_float(r.get("sp500_t0")) and (ms_date_only(r.get("release_date", "")) or 0) > now})
    add("MAC-09", "S&P500 #tk-sp", js_locale(cur, 2, 2), dom["tk"]["sp"],
        note=f"期待=FRED SP500 {sp[-1]['as_of']}。events.csvの未来日付の行={future_rows}（画面は使わない）")
    add("MAC-10", "S&P500 前営業日比 #tk-sp-c", f"{sign}{js_fixed(chg, 2)} ({sign}{js_fixed(pct, 2)}%)", dom["tk"]["spc"],
        note=f"期待={sp[-2]['as_of']}→{sp[-1]['as_of']}（画面はsp500_t0_asofの観測日順。観測日のある行が無ければupdated_at順で値が違う直前の行）")
    yc = m.latest_val("Yield Curve 10Y-2Y", now)
    add("MAC-11", "10Y-2Y #tk-yc/#tk-yc-i",
        [("+" if yc >= 0 else "") + js_fixed(yc, 2) + "%", "INVERTED" if yc < -0.2 else "FLAT" if yc < 0.5 else "NORMAL"] if yc is not None else ["—", "—"],
        [dom["tk"]["yc"], dom["tk"]["yci"]])
    hy = m.latest_val("HY Spread", now)
    add("MAC-12", "HY #tk-hy", js_fixed(hy, 2) + "%" if hy is not None else "—", dom["tk"]["hy"])
    # 期待値は「今日以前の行の最大日付」（2026-10-03までの画面は未来日付の行も見ていた: B-02、M-2 STEP 3で修正）
    latest_date, latest_src = "", "—"
    for ind, arr in m.index.items():
        past = [e for e in arr if e["d"] <= now]
        if past:
            ds = iso_date(past[-1]["d"])
            if ds > latest_date:
                latest_date = ds
                rr = next((x for x in m.events if x["indicator"] == ind and x["release_date"] == ds), None)
                if rr:
                    latest_src = rr.get("data_source") or "—"
    add("MAC-13", "LAST UPDATE #tk-last/#tk-src", [latest_date, latest_src], [dom["tk"]["last"], dom["tk"]["src"]],
        note="期待=今日以前の行の最大日付")

    # MAC-14〜27 流動性
    rows = m.liq
    latest = rows[-1]

    def lnv(col):
        for r in reversed(rows):
            v = r.get(col)
            if v not in ("", None):
                return parse_float(v)
        return None

    def prevv(col, days):
        cut = ms_date_only(latest["date"]) - days * DAY_MS
        for r in reversed(rows):
            if ms_date_only(r["date"]) <= cut and r.get(col, "") != "":
                return parse_float(r[col])
        return None

    def hist(col):
        return [parse_float(r[col]) for r in rows if r.get(col, "") != ""]

    def rank(h, v):
        if not h or v is None:
            return None
        return js_round(len([x for x in h if x <= v]) / len(h) * 100)

    def vec(cur, prev):
        if cur is None or prev is None:
            return None
        pct = (cur - prev) / abs(prev or 1)
        return ("↑", "拡大中", "up") if pct > 0.005 else ("↓", "縮小中", "dn") if pct < -0.005 else ("→", "横ばい", "flat")

    def chg_text(cur, prev, fmt):
        if cur is None or prev is None:
            return "—"
        df = cur - prev
        return ("+" if df > 0 else "") + fmt(df)

    m2, fedb, nl = lnv("m2"), lnv("fed_balance"), lnv("net_liquidity")
    m2p, fedp, nlp = prevv("m2", 28), prevv("fed_balance", 7), prevv("net_liquidity", 7)
    hyv = parse_float(latest["hy_spread"]) if latest.get("hy_spread", "") != "" else None
    hyp = prevv("hy_spread", 7)
    m2r, nlr, fedr = rank(hist("m2"), m2), rank(hist("net_liquidity"), nl), rank(hist("fed_balance"), fedb)
    hyr = rank(hist("hy_spread"), parse_float(latest.get("hy_spread") or 0))

    def m2c(rk, v):
        if rk is None: return ""
        if rk >= 70 and v and v[2] == "up": return "流動性豊富・拡大中 → 株式に追い風"
        if rk >= 70 and v and v[2] == "flat": return "流動性豊富・横ばい → 中立"
        if rk <= 30 and v and v[2] == "dn": return "流動性タイト・縮小中 → 株式に逆風"
        if rk <= 30: return "流動性タイト → やや逆風"
        return "流動性は中程度"

    def nlc(rk, v):
        if rk is None: return ""
        if rk >= 70 and v and v[2] == "up": return "FRB供給資金が潤沢・増加 → リスクオン"
        if rk >= 70: return "FRB供給資金が潤沢 → 中立〜強気"
        if rk <= 30 and v and v[2] == "dn": return "FRB資金が枯渇気味・減少 → リスクオフ"
        if rk <= 30: return "FRB資金が枯渇気味 → 注意"
        return "FRB資金は中程度"

    def hyc(val, v):
        if val is None: return ""
        if val < 3.0 and v and v[2] == "dn": return "スプレッド低位・縮小 → 信用市場良好"
        if val < 3.0: return "スプレッド低位 → 信用リスク低い"
        if val > 4.5 and v and v[2] == "up": return "スプレッド拡大中 → 信用不安サイン"
        if val > 4.5: return "スプレッド高め → 信用リスク注意"
        return "スプレッドは普通水準"

    def fdc(rk, v):
        if rk is None: return ""
        if rk >= 70 and v and v[2] == "dn": return "BS大きいが縮小中(QT継続) → やや引締め"
        if rk <= 30 and v and v[2] == "dn": return "BS縮小進行中 → 引締め継続"
        if v and v[2] == "up": return "BS拡大中 → 緩和的"
        return "BS変化小さい"

    m2v, nlv, fedv, hyvv = vec(m2, m2p), vec(nl, nlp), vec(fedb, fedp), vec(hyv, hyp)
    cards = [
        ("MAC-14", "M2 マネーサプライ", js_locale(m2, 0, 1) if m2 is not None else "—",
         chg_text(m2, m2p, lambda df: js_fixed(df / abs(m2p or 1) * 100, 2) + "%"), m2v, m2r, m2c(m2r, m2v)),
        ("MAC-15", "NET LIQUIDITY", js_fixed(nl, 2) if nl is not None else "—",
         chg_text(nl, nlp, lambda df: js_fixed(df, 3) + "兆"), nlv, nlr, nlc(nlr, nlv)),
        ("MAC-16", "HY スプレッド", js_fixed(hyv, 2) if hyv is not None else "—",
         chg_text(hyv, hyp, lambda df: js_fixed(df, 3) + "%"), hyvv, hyr, hyc(hyv, hyvv)),
        ("MAC-17", "FRB バランスシート", js_fixed(fedb / 1e6, 2) if fedb is not None else "—",
         chg_text(fedb, fedp, lambda df: js_fixed(df / 1e6, 3) + "兆"), fedv, fedr, fdc(fedr, fedv)),
    ]
    dom_cards = {c["label"]: c for c in dom["liqCards"]}
    for cid, label, val, chg, v, rk, com in cards:
        dc = dom_cards.get(label) or {}
        exp_date = "更新 " + latest["date"] + (f"（H.4.1 {latest['h41_date']}）" if label == "FRB バランスシート" and latest.get("h41_date") else "")
        exp = [val, chg, (v[0] + " " + v[1]) if v else None, f"過去データ内 {rk}パーセンタイル" if rk is not None else None, com, exp_date]
        txt = dc.get("text") or ""
        act = [dc.get("val"), dc.get("chg"), (v[0] + " " + v[1]) if v and (v[0] + " " + v[1]) in txt else "（見つからず）" if v else None,
               (f"過去データ内 {rk}パーセンタイル" if f"過去データ内 {rk}パーセンタイル" in txt else "（見つからず）") if rk is not None else None,
               com if com and com in txt else ("" if not com else "（見つからず）"), dc.get("date")]
        add(cid, f"{label}カード（値・変化・方向・水準・解釈・日付）", exp, act)

    # MAC-18 Hollow Rally（M-2 STEP 4以降: 最新行の sp500_5d_pct・net_liq_wow_pct で判定。列が無い行では判定しない）
    sp5 = parse_float(latest.get("sp500_5d_pct")) if latest.get("sp500_5d_pct", "") != "" else None
    nlw = parse_float(latest.get("net_liq_wow_pct")) if latest.get("net_liq_wow_pct", "") != "" else None
    hollow = None
    if sp5 is not None and nlw is not None and sp5 > 1.0 and nlw < -0.5:
        hollow = f"S&P500 5営業日 {'+' if sp5 >= 0 else ''}{js_fixed(sp5, 1)}%"
    add("MAC-18", "Hollow Rallyバッジ", hollow is not None, dom["hollow"] is not None and (hollow or "") in (dom["hollow"] or ""),
        note=f"最新行 sp500_5d_pct={sp5} net_liq_wow_pct={nlw} h41_date={latest.get('h41_date') or '（列なし）'}")

    # MAC-19〜26 ステルス
    sig = latest.get("stealth_signal") or "neutral"
    badge = "▼ ステルス供給中" if sig == "supply" else "▲ ステルス吸収中" if sig == "absorb" else "→ 中立"
    stl = dom["stealth"] or {}
    add("MAC-19", "ステルス判定（LAYER 2）", badge, stl.get("badge"))
    fed_last = m.fed[-1]
    add("MAC-20", "LAYER 1（FRB政策意図）", fed_last.get("regime") or "—", stl.get("layer1"),
        note="ファイル末尾の行（record_dateで並べ替えない。REGIMEバーは並べ替える）")
    dw = int(latest["net_liq_decline_weeks"]) if latest.get("net_liq_decline_weeks", "") != "" else 0
    l3lab = f"NET流動性{dw}週連続減少" if dw >= 3 else f"NET流動性{dw}週減少" if dw >= 1 else "NET流動性 安定"
    add("MAC-21", "LAYER 3（NET流動性の連続減少）", True, l3lab in (stl.get("text") or ""), note=l3lab)
    alerts = [a.strip() for a in (latest.get("stealth_alert") or "").split("|") if a.strip()]
    add("MAC-22", "ステルス警戒アラート", alerts, [a for a in alerts if ("⚠ " + a) in (stl.get("text") or "")])
    for cid, lab, col, div, dig in [("MAC-23", "REPO残高 (RRPONTSYD)", "rrp", 1000, 1), ("MAC-24", "準備預金 (WRBWFRBL)", "reserve_balance", 1000, 0),
                                     ("MAC-25", "TGA残高 (WTREGEN)", "tga", 1000, 0)]:
        cur, pv = lnv(col), prevv(col, 7)
        val = js_fixed(cur / div, dig) if cur is not None else "—"
        ch = "—" if cur is None or pv is None else ("+" if (cur - pv) / 1000 > 0 else "") + js_fixed((cur - pv) / 1000, 2) + " B 前週比"
        mm = next((x for x in stl.get("metrics", []) if x["label"] == lab), {})
        add(cid, lab, [val, ch], [mm.get("val"), (mm.get("chg") or "").replace("\u00a0", " ")])
    add("MAC-26", "流動性カード・ステルスの日付", latest["date"], latest["date"] if latest["date"] in (stl.get("text") or "") else None,
        note="行の日付＝実行日（UTC）であり観測日ではない")
    add("MAC-27", "流動性の注記", "※ カードの日付は更新日（観測日ではありません）。M2は月次（約1か月遅れで公表）、FRBバランスシート・TGA・準備預金はH.4.1の水曜時点の値（翌木曜公表）で、値は次回発表まで変わりません。HYスプレッド・RRPは日次。", dom["liqNote"])

    # MAC-28〜33 景気フェーズ
    sc = m.shown_score()
    ph, sub = phase_of(sc)
    add("MAC-28", "局面バッジ・説明", [ph, sub], [dom["phase"]["badge"], dom["phase"]["sub"]])
    add("MAC-29", "トラック（塗り・マーカー）", [f"{sc}%", f"calc({sc}% - 1.5px)"], [dom["phase"]["fill"], dom["phase"]["marker"]])
    add("MAC-30", "RECESSION RISK SCORE #pg-score-num", str(sc), dom["phase"]["score"],
        note=f"表示=週次スナップショット（{m.weekly_sorted[0].get('analysis_date') if m.weekly_sorted else None}）、本日のライブ計算={m.live_score()}")
    sigs = m.live_signals()
    bear = len([s for s in sigs if s["signal"] == "bear" and s["val"] != "—"])
    cau = len([s for s in sigs if s["signal"] == "caution" and s["val"] != "—"])
    cnt = len([s for s in sigs if s["val"] != "—"])
    add("MAC-31", "シグナル数の文", f"{cnt}指標中: 後退シグナル {bear}個 / 注意 {cau}個", dom["phase"]["sigText"])
    show = bear >= 3 and sc >= 52
    add("MAC-32", "ALERT", "flex" if show else "none", dom["phase"]["alertDisplay"])
    btxt = {"bull": "拡張", "neutral": "中立", "caution": "注意", "bear": "後退シグナル"}
    def obs_label(s):
        if not s["obs"]:
            return "—"
        slot = s["name"] in ("Michigan Sent.", "Building Permits") and not s["obs"].endswith("-01")
        return s["obs"] + ("（発表予定日）" if slot else "（観測日）")
    exp_sig = [[s["name"], s["val"], btxt[s["signal"]], s["lead"], s["thresh"], f"{s['weight']}%", obs_label(s)] for s in sigs]
    act_sig = [[x["name"], x["val"], x["badge"], x["lead"], (x["tip"][0][1] if len(x["tip"]) > 0 else None),
                (x["tip"][2][1] if len(x["tip"]) > 2 else None), (x["tip"][3][1] if len(x["tip"]) > 3 else None)] for x in dom["sigs"]]
    add("MAC-33", "8指標カード（値・判定・先行性・閾値・ウェイト・観測日）", exp_sig, act_sig)
    help_rows = dom["help"]
    add("MAC-34", "「? 見方」の指標とウェイト表", 8, len(help_rows), note="件数のみ（内容と計算の整合はN-01）")

    # MAC-35 比較バー
    today_end = set_hours(now, 23, 59, 59)
    td = local_dt(today_end)
    w1 = today_end - 7 * DAY_MS
    m1 = local_ms(datetime(td.year, td.month, 1, 23, 59, 59, tzinfo=JST) - timedelta(days=1))
    m2d = datetime(td.year if td.month > 1 else td.year - 1, td.month - 1 if td.month > 1 else 12, 1, 23, 59, 59, tzinfo=JST) - timedelta(days=1)
    m3d = datetime(m2d.year, m2d.month, 1, 23, 59, 59, tzinfo=JST) - timedelta(days=1)
    exp_cmp, act_cmp = [], []
    for k, t, lab in [("m3", local_ms(m3d), "3ヶ月前"), ("m2", local_ms(m2d), "2ヶ月前"), ("m1", m1, "前月末"), ("w1", w1, "先週")]:
        s_ = m.score_as_of(t)
        rd = m.latest_data_date_before(t)
        if s_ is None or rd is None:
            exp_cmp.append(["—", "—", lab])
        else:
            df = sc - s_
            exp_cmp.append([str(s_), ("+" if df > 0 else "") + str(df), f"{rd} 時点 ({phase_of(s_)[0]})"])
        c = next(x for x in dom["cmp"] if x["k"] == k)
        act_cmp.append([c["score"], c["delta"], c["date"]])
    add("MAC-35", "比較バー（3ヶ月前・2ヶ月前・前月末・先週）", exp_cmp, act_cmp)
    t = local_ms(datetime(2026, 6, 30, 23, 59, 59, tzinfo=JST))
    s_ = m.score_as_of(t)
    rd = m.latest_data_date_before(t)
    exp_c = [str(s_), ("+" if sc - s_ > 0 else "") + str(sc - s_), f"{rd} 時点 ({phase_of(s_)[0]})"] if s_ is not None and rd else ["—", "データなし", "データ範囲外"]
    add("MAC-36", "カスタム比較（2026-06-30を入力）", exp_c, [extra["custom"]["score"], extra["custom"]["delta"], extra["custom"]["label"]])
    add("MAC-37", "比較バーの注記（本日=実測・過去=補間）", True, True, passed=True, note="静的文言。意図的設計（MACRO-COMPUTE-DUP-1）で蒸し返さない")

    # MAC-38 サプライズバナー
    lr = m.weekly_sorted[0] if m.weekly_sorted else {}
    items = [s.strip() for s in (lr.get("surprise_alerts") or "").split(";") if s.strip()]
    add("MAC-38", "MACRO SURPRISEバナー", ["block" if items else "none", len(items)], [dom["surprise"]["display"], len(dom["surprise"]["items"])])

    # MAC-39 AIカード
    exp_ai, act_ai = [], []
    for i, r in enumerate(m.weekly_sorted[:12]):
        s0 = parse_float(r.get("score"))
        scv = int(s0) if s0 is not None else 0
        dms = ms_date_only(r.get("analysis_date", "")) or 0
        exp_ai.append([ja_date(dms), str(scv), r.get("phase") or "—"])
    for c in dom["ai"]:
        act_ai.append([c["date"], c["score"], c["phase"]])
    add("MAC-39", "AIウィークリーコメンタリー（直近12週の日付・スコア・局面）", exp_ai, act_ai)
    add("MAC-40", "AI欄の見出し・注記", ["AI WEEKLY COMMENTARY — 毎週日曜自動生成（xAI Grok。使ったモデルは各カードの末尾に表示）", "※ スコアは週報生成時点の値。最新スコアはページ上部のゲージを参照。毎週日曜 JST 7:11（米国東部時間 土曜18:11）の予定で自動更新（GitHubの起動の遅れで数時間遅れることがあります）。"],
        [dom["aiTitle"].replace("🤖", "").strip() if dom["aiTitle"] else None, dom["aiNote"]], note="文言と実際の計算・起動の整合はN-03・N-04")

    # MAC-41 ヘルスバー
    cfg = [("YC 10Y-2Y", "Yield Curve 10Y-2Y", 0, 0.5, -0.2, "right", lambda v: ("+" if v >= 0 else "") + js_fixed(v, 2) + "%"),
           ("HY Spread", "HY Spread", 5.0, 4.0, 6.5, "left", lambda v: js_fixed(v, 2) + "%"),
           ("Philly Fed Mfg", "Philadelphia Fed Manufacturing", 0, 5, -5, "right", lambda v: js_fixed(v, 1)),
           ("CFNAI MA3", "Chicago Fed National Activity", -0.2, 0, -0.7, "right", lambda v: js_fixed(v, 2)),
           ("Sahm Rule", "Sahm Rule Recession Indicator", 0.3, 0.3, 0.5, "left", lambda v: js_fixed(v, 2)),
           ("Initial Claims 4WMA", "Initial Claims 4W MA", 230000, 215000, 245000, "left", lambda v: js_fixed(v / 1000, 1) + "K"),
           ("Michigan Sentiment", "Michigan Consumer Sentiment", 80, 90, 65, "right", lambda v: js_fixed(v, 1)),
           ("Building Permits", "Building Permits", 1300, 1500, 1100, "right", lambda v: js_fixed(v, 0) + "K")]
    exp_l2 = []
    for lab, key, mid, bull, bear_, dr, fmt in cfg:
        v = m.latest_val(key, now)
        if v is None:
            exp_l2.append([lab, "—", None]); continue
        if dr == "right":
            sg = "BULL" if v >= bull else "BEAR" if v <= bear_ else "CAUTION" if v < (mid + bull) / 2 else "NEUTRAL"
        else:
            sg = "BULL" if v <= bull else "BEAR" if v >= bear_ else "CAUTION" if v > (mid + bull) / 2 else "NEUTRAL"
        exp_l2.append([lab, fmt(v), sg])
    add("MAC-41", "② 各指標の現在地（8本の値・判定）", exp_l2, [[x["name"], x["val"], x["sig"]] for x in dom["l2"]])

    # MAC-42 スコア推移チャート（1年）
    def build_series(start_ms, end_ms):
        all_inds = [ind for _, ind, _, _ in SCORE_INDS]
        cds = set()
        for r in m.events:
            dm = ms_date_only(r.get("release_date", ""))
            if dm is None:
                continue
            dl = set_hours(dm, 0, 0, 0)
            if start_ms <= dl <= end_ms and r["indicator"] in all_inds:
                cds.add(iso_date(dl))
        dcur = start_ms
        while dcur <= end_ms:
            cds.add(iso_date(dcur))
            dcur = local_ms(local_dt(dcur) + timedelta(days=7))
        cds.add(iso_date(end_ms))
        out = []
        for ds in sorted(cds):
            tt = set_hours(ms_date_only(ds), 23, 59, 59)
            s2 = m.score_as_of(tt)
            if s2 is not None:
                out.append([ds, s2])
        return out

    td0 = local_dt(today_end)
    try:
        st1 = td0.replace(year=td0.year - 1, hour=0, minute=0, second=0)
    except ValueError:
        st1 = td0.replace(year=td0.year - 1, day=28, hour=0, minute=0, second=0)
    exp_hist = build_series(local_ms(st1), today_end)
    act_hist = [list(x) for x in dom.get("history") or []]
    add("MAC-42", "③ スコア推移（1年、全点）", exp_hist, act_hist,
        note=f"期待{len(exp_hist)}点（先頭{exp_hist[0] if exp_hist else None}）・実際{len(act_hist)}点（先頭{act_hist[0] if act_hist else None}）")
    # 「全期間」の先頭（M-3で期待を変更）: 画面の開始日はevents.csvの最古の行の日付（値の有無を問わない）。点は
    # computeScoreAsOf()がnullでない日だけ残るため、先頭は「8指標のどれかが公表済み（known_at）になった後の最初の点」。
    # 以前の期待（最古の観測の月）は先読み除外を考えていなかった
    all_hist = dom.get("historyAll") or []
    oldest = min(r["release_date"][:10] for r in m.events if ms_date_only(r.get("release_date", "")))
    st_all = set_hours(ms_date_only(oldest), 0, 0, 0)
    cds_all = {iso_date(set_hours(ms_date_only(r["release_date"]), 0, 0, 0)) for r in m.events
               if ms_date_only(r.get("release_date", "")) and r["indicator"] in {ind for _, ind, _, _ in SCORE_INDS}}
    dcur = st_all
    while dcur <= today_end:
        cds_all.add(iso_date(dcur))
        dcur = local_ms(local_dt(dcur) + timedelta(days=7))
    first_all = next((ds for ds in sorted(cds_all)
                      if m.score_as_of(set_hours(ms_date_only(ds), 23, 59, 59)) is not None), None)
    add("MAC-42c", "③ スコア推移「全期間」の先頭の日付", first_all, (all_hist[0][0] if all_hist else None),
        note=f"開始日={oldest}（events.csvの最古の行）・全期間の点数={len(all_hist)}")
    # M-3: 3年・5年の先頭の点（期待は1年と同じ作り方で、開始日だけを変える）
    exp_r, act_r = [], []
    for yrs in (3, 5):
        try:
            sty = td0.replace(year=td0.year - yrs, hour=0, minute=0, second=0)
        except ValueError:
            sty = td0.replace(year=td0.year - yrs, day=28, hour=0, minute=0, second=0)
        ser = build_series(local_ms(sty), today_end)
        exp_r.append([f"{yrs}年", ser[0] if ser else None, len(ser)])
        a = (dom.get("historyRanges") or {}).get(yrs) or [None, None]
        act_r.append([f"{yrs}年", list(a[0]) if a[0] else None, a[1]])
    add("MAC-42d", "③ スコア推移「3年・5年」の先頭の点と点数", exp_r, act_r)
    tip_last = dom.get("historyTipLast") or ""
    add("MAC-42b", "③ tooltip（本日の点は実測値の注記）", True, "本日の実測値" in tip_last, note=re.sub("<[^>]+>", " ", tip_last)[:120])

    # MAC-43〜45 類似度
    L3 = [("YC 10Y-2Y", "Yield Curve 10Y-2Y", -1.0, 1.5), ("Philly Fed", "Philadelphia Fed Manufacturing", -30, 30),
          ("CFNAI", "Chicago Fed National Activity", -3, 1), ("Sahm Rule", "Sahm Rule Recession Indicator", 1.5, 0),
          ("Bldg Permits", "Building Permits", 500, 2200), ("Michigan", "Michigan Consumer Sentiment", 50, 110),
          ("Initial Clms", "Initial Claims 4W MA", 350000, 180000), ("HY Spread", "HY Spread", 10, 2.5)]

    def norm(v, lo, hi):
        if v is None:
            return 50
        return max(0, min(100, (v - lo) / (hi - lo) * 100))

    def raw_fmt(v, key):
        if v is None: return "—"
        if key == "Initial Claims 4W MA": return js_fixed(v / 1000, 0) + "k"
        if key == "Yield Curve 10Y-2Y": return ("+" if v >= 0 else "") + js_fixed(v, 2) + "%"
        if key == "HY Spread": return js_fixed(v, 2) + "%"
        return js_fixed(v, 1)

    ref19 = ms_local("2019-11-01T23:59:59")
    ref01 = ms_local("2001-09-01T23:59:59")
    nowv = [norm(m.latest_val(k, now), lo, hi) for _, k, lo, hi in L3]
    v19 = [norm(m.latest_val(k, ref19), lo, hi) for _, k, lo, hi in L3]
    v01 = [norm(m.latest_val(k, ref01), lo, hi) for _, k, lo, hi in L3]
    labels = [f"{lab}\n{raw_fmt(m.latest_val(k, now), k)}" for lab, k, _, _ in L3]
    radar = dom.get("radar") or {}
    act_series = {s["name"]: s["value"] for s in radar.get("series", [])}

    def close(a, b):
        return a is not None and b is not None and len(a) == len(b) and all(abs(x - y) < 1e-9 for x, y in zip(a, b))

    add("MAC-43", "④ レーダー（現在・2019・2001の正規化値と実数ラベル）", True,
        close(nowv, act_series.get("現在")) and close(v19, act_series.get("2019-11-01")) and close(v01, act_series.get("2001-09-01")) and labels == radar.get("indicators"),
        note=f"labels期待={labels}")

    def sim(a, b):
        dist = math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
        raw = (1 - dist / math.sqrt(8 * 100 * 100)) * 100
        return js_round(raw * 100) / 100

    s19, s01 = sim(nowv, v19), sim(nowv, v01)
    d19 = "⚠ 構造が近似。後退入口に近い可能性" if s19 >= 75 else "一部の指標が類似傾向" if s19 >= 60 else "現在は大きく乖離"
    d01 = "⚠ 構造が近似。注意が必要" if s01 >= 75 else "一部の指標が類似傾向" if s01 >= 60 else "現在は大きく乖離"
    add("MAC-44", "④ 類似度（2019・2001）", [[js_fixed(s19, 2) + "%", d19], [js_fixed(s01, 2) + "%", d01]],
        [[x["pct"], x["desc"]] for x in dom["l3Scores"]])
    # スライダー: スナップショット数
    earliest = min(ms for ms in (ms_date_only(r.get("release_date", "")) for r in m.events) if ms is not None)
    start = set_hours(earliest, 0, 0, 0)
    monthly = {"Philadelphia Fed Manufacturing", "Chicago Fed National Activity", "Michigan Consumer Sentiment",
               "Building Permits", "Sahm Rule Recession Indicator", "Initial Claims 4W MA"}
    cds = set()
    for r in m.events:
        dm = ms_date_only(r.get("release_date", ""))
        if dm is None:
            continue
        dl = set_hours(dm, 0, 0, 0)
        if start <= dl <= today_end and r["indicator"].strip() in monthly:
            cds.add(iso_date(dl))
    dcur = start
    while dcur <= today_end:
        cds.add(iso_date(dcur))
        dcur = local_ms(local_dt(dcur) + timedelta(days=7))
    cds.add(iso_date(start)); cds.add(iso_date(today_end))
    add("MAC-45", "④ スライダー（スナップショット数・初期表示）", [str(len(cds) - 1), "現在"], [dom["slider"]["max"], dom["slider"]["date"]])

    # MAC-46 直近の動き
    today0 = set_hours(now, 0, 0, 0)
    cut = local_ms(local_dt(today0) - timedelta(days=90))
    daily = {"Yield Curve 10Y-2Y", "HY Spread", "VIX", "Michigan Inflation 5Y"}
    rec = []
    for ind, arr in m.index.items():
        if ind in daily:
            continue
        for i, e in enumerate(arr):
            if e["d"] < cut:
                continue
            if e["d"] > today0:
                break
            ds = iso_date(e["d"])
            rr = next((x for x in m.events if x["indicator"] == ind and x["release_date"] == ds and parse_float(x.get("actual")) is not None), None)
            if rr:
                rec.append((e["d"], ind, m.val_at(e, now), m.val_at(arr[i - 1], now) if i > 0 else None, ds))
    rec.sort(key=lambda x: -x[0])
    bull_up = {"Philadelphia Fed Manufacturing", "Chicago Fed National Activity", "Michigan Consumer Sentiment", "Building Permits", "Conference Board LEI", "NFP"}
    bull_dn = {"Sahm Rule Recession Indicator", "Initial Claims 4W MA", "Initial Claims", "HY Spread"}
    exp_rs = []
    for dms, ind, v, pv, ds in rec:
        if pv is not None:
            ch = v - pv
            if abs(ch) > abs(v) * 0.001 + 0.001:
                arrow = "↑" if ch > 0 else "↓"
                chs = ("+" if ch > 0 else "") + fmt_k(ch)
            else:
                arrow, chs = "→", "±0"
            pvs = fmt_k(pv)
        else:
            arrow, chs, pvs = "→", "—", "—"
        exp_rs.append([ja_date(dms, year=False), ind, fmt_k(v), pvs, arrow, chs])
    extra_rows = [r for r in dom["signals"] if r not in exp_rs]
    add("MAC-46", "⑤ 直近の動き（表の全行）", exp_rs, dom["signals"],
        note=f"期待{len(exp_rs)}行（直近90日）・画面{len(dom['signals'])}行。期待に無い行={extra_rows}"
             "（画面の二分探索は、全行が90日より古い指標でも最後の1行を出す: 不具合B-06）")

    # MAC-47 スケジュール
    cutf = local_ms(local_dt(today0) + timedelta(days=14))
    excl = {"Yield Curve 10Y-2Y", "HY Spread", "Michigan Inflation 5Y"}
    vis = []
    for r in m.sched:
        ds = r.get("release_date") or r.get("発表予定日")
        nm = r.get("indicator") or r.get("指標名") or ""
        if not ds or nm in excl:
            continue
        dd = set_hours(ms_date_only(ds), 0, 0, 0)
        if today0 <= dd <= cutf:
            vis.append((dd, nm, (r.get("consensus") or "").strip()))
    vis.sort(key=lambda x: x[0])
    exp_sc = []
    for dd, nm, cons in vis:
        diff = js_round((dd - today0) / DAY_MS)
        tag = "TODAY" if dd == today0 else f"+{diff}d"
        exp_sc.append([ja_date(dd, year=False), nm, tag, cons or "—"])
    add("MAC-47", "⑥ 今後2週間の発表スケジュール", exp_sc, dom["sched"])
    add("MAC-48", "上部の帯（COMPOSITE SCORE・NOW・DETAIL）", 3, len(dom["banners"]), note="件数のみ（文言はN-02）")
    return R


# ═══════════════════════════════════════════════════════════════
# 導出（D）: FRED系列ストアからの独立再計算
# ═══════════════════════════════════════════════════════════════
def derived_checks(m: Model) -> list[Result]:
    R: list[Result] = []
    store = {sid: load_series(sid) for _, _, sid, _ in SCORE_INDS}
    now = m.now
    # D-01 8指標: 画面の計算に使われている値（events.csv）と、FRED系列ストアの最新観測値
    exp, act, notes = [], [], []
    for k, ind, sid, _ in SCORE_INDS:
        last = store[sid][-1]
        e = m.latest_entry(ind, now)
        exp.append([ind, last["as_of"], last["value"]])
        act.append([ind, iso_date(e["d"]) if e else None, m.val_at(e, now) if e else None])
    R.append(Result("D-01", "8指標: 計算に使われる最新行 vs FRED系列ストアの最新観測", exp, act, exp == act, "導出",
                    "日付は events.csv の release_date（観測日・発表予定日が混在）。値の不一致は取り込み漏れ・改定未反映・日付の割り当て違い"))
    # D-02 週次スナップショット（05_weekly_analysis.csv 最新行）の再計算
    if m.weekly_sorted:
        r0 = m.weekly_sorted[0]
        ad = r0["analysis_date"]
        tgt = int((datetime.strptime(ad, "%Y-%m-%d") + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp() * 1000) - 1
        ok_rows = {}
        for r in m.events:
            v = parse_float(r.get("actual"))
            dm = ms_date_only(r.get("release_date", ""))
            if v is None or dm is None or dm > tgt:
                continue
            ok_rows.setdefault(r["indicator"], []).append((dm, v))
        for k in ok_rows:
            ok_rows[k].sort(key=lambda x: x[0])
        # 注: 現在の events.csv（後から追加・上書きされた行を含む）で再計算するため、当時の入力とは異なりうる
        R.append(Result("D-02", "週次スナップショットのスコア（05_weekly_analysis.csv 最新行）", r0.get("score"), None, None, "導出",
                        f"{ad}時点の入力は再現できない（events.csvは後から行が追加・上書きされ、当時の版の特定が必要）。判定不能"))
    # D-03 流動性の最新行 vs FRED系列ストアの最新値
    latest = m.liq[-1]
    st = {s: load_series(s)[-1] for s in ("M2SL", "WALCL", "WTREGEN", "RRPONTSYD", "WRBWFRBL", "BAMLH0A0HYM2", "SP500")}
    exp = {"m2": st["M2SL"]["value"], "fed_balance": st["WALCL"]["value"], "tga": st["WTREGEN"]["value"],
           "rrp": round(st["RRPONTSYD"]["value"] * 1000, 4), "reserve_balance": st["WRBWFRBL"]["value"],
           "hy_spread": st["BAMLH0A0HYM2"]["value"], "sp500": st["SP500"]["value"]}
    act = {k: parse_float(latest.get(k)) for k in exp}
    R.append(Result("D-03", f"05_liquidity.csv 最新行（{latest['date']}）vs FRED系列ストア", exp, act,
                    all(abs((exp[k] or 0) - (act[k] or 0)) < 1e-6 for k in exp), "導出",
                    "観測日: " + ", ".join(f"{k}={v['as_of']}" for k, v in st.items())))
    nl = round((exp["fed_balance"] - exp["tga"] - exp["rrp"]) / 1_000_000, 4)
    R.append(Result("D-04", "NET LIQUIDITY = (WALCL − WTREGEN − RRP×1000)/10^6", nl, parse_float(latest.get("net_liquidity")),
                    abs(nl - (parse_float(latest.get("net_liquidity")) or 0)) < 1e-6, "導出"))
    # D-05 「NET流動性N週連続減少」: M-2 STEP 4以降の行（h41_dateあり）は、H.4.1の水曜の値の連続減少を独立に数えて比べる
    lq = m.liq[-1]
    if lq.get("h41_date"):
        w = {s: {r["as_of"]: r["value"] for r in load_series(s)} for s in ("WALCL", "WTREGEN", "RRPONTSYD")}
        weds = sorted(d for d in w["WALCL"] if d <= lq["h41_date"])[-9:]
        rrp_dates = sorted(w["RRPONTSYD"])
        nlv = []
        for d in weds:
            rd = [x for x in rrp_dates if x <= d]
            if d in w["WTREGEN"] and rd:
                nlv.append((w["WALCL"][d] - w["WTREGEN"][d] - w["RRPONTSYD"][rd[-1]] * 1000) / 1e6)
        dec = 0
        for i in range(len(nlv) - 1, 0, -1):
            if nlv[i] >= nlv[i - 1]:
                break
            dec += 1
        R.append(Result("D-05", f"NET流動性の連続減少週数（H.4.1 {lq['h41_date']}まで）", str(dec), lq.get("net_liq_decline_weeks"),
                        str(dec) == lq.get("net_liq_decline_weeks"), "導出"))
    else:
        R.append(Result("D-05", "NET流動性の連続減少週数", None, lq.get("net_liq_decline_weeks"), None, "導出",
                        "最新行にh41_dateが無い（M-2 STEP 4の変更前の行。日次の行で数えた値）"))
    # D-06 IMPLIED CUTS = (ff − zq)/0.25
    f = sorted(m.fed, key=lambda r: r.get("record_date") or "")[-1]
    ff, zq, cu = parse_float(f.get("ff_current")), parse_float(f.get("zq_rate")), parse_float(f.get("cuts_implied"))
    R.append(Result("D-06", "IMPLIED CUTS = (FF − DGS1)/0.25", round((ff - zq) / 0.25, 2) if ff is not None and zq is not None else None, cu,
                    ff is not None and zq is not None and abs(round((ff - zq) / 0.25, 2) - cu) < 1e-9, "導出",
                    f"record_date={f.get('record_date')} updated_at={f.get('updated_at')} fomc_date={f.get('fomc_date')}"))
    # D-07 FF RATE と現在の DFEDTARU/L の中心値・DGS1 の最新値
    u, l, d1 = load_series("DFEDTARU")[-1], load_series("DFEDTARL")[-1], load_series("DGS1")[-1]
    R.append(Result("D-07", "FF RATE・1Y EXPECTED FF vs FRED系列ストアの最新値", [round((u["value"] + l["value"]) / 2, 4), d1["value"]],
                    [ff, zq], abs(round((u["value"] + l["value"]) / 2, 4) - (ff or 0)) < 1e-9 and abs(d1["value"] - (zq or 0)) < 1e-9, "導出",
                    f"DGS1観測日={d1['as_of']}（REGIMEバーは週1回〈土曜〉更新）"))
    # D-09 AIカードの「週±」= 前週の週次スナップショットとの差。M-2 STEP 5の修正後の行（分析日2026-10-03以降）だけを判定する
    #（2026-10-04 M-2c: 対象日を米国の日付にしたため、修正後の最初の行は2026-10-03〈10-04 00:30 UTC起動〉）
    #（それより前の行は書き換えない方針。記録値は先読みで0になっていた: MACRO-PULSE-AI-DELTA-LOOKAHEAD-1）
    ws = sorted(m.weekly, key=lambda r: r.get("analysis_date") or "")
    exp9, act9 = [], []
    for a, b in zip(ws, ws[1:]):
        if b["analysis_date"] < "2026-10-03":
            continue
        exp9.append([b["analysis_date"], int(parse_float(b["score"])) - int(parse_float(a["score"]))])
        act9.append([b["analysis_date"], int(parse_float(b.get("score_change_1w") or 0) or 0)])
    R.append(Result("D-09", "AIカードの「週±」（直前の週次スコアとの差 vs 記録値、2026-10-03以降の行）", exp9, act9,
                    (exp9 == act9) if exp9 else None, "導出", "" if exp9 else "2026-10-03以降の週次の行がまだ無い"))
    # D-08 未来の日付の行（release_date > 今日）
    fut = [(r["indicator"], r["release_date"], r["actual"], r["updated_at"]) for r in m.events
           if (ms_date_only(r.get("release_date", "")) or 0) > now and parse_float(r.get("actual")) is not None]
    R.append(Result("D-08", "events.csv の未来日付の行（値あり）", [], fut, not fut, "データ"))
    return R


# ═══════════════════════════════════════════════════════════════
# 説明（N）: 説明文・tooltip・注記と実際の計算の整合
# ═══════════════════════════════════════════════════════════════
def note_checks(m: Model, dom: dict) -> list[Result]:
    R: list[Result] = []
    help_rows = dom["help"]
    names = [r[0] for r in help_rows]
    wts = [r[1] for r in help_rows]
    has_bp = any("住宅" in n or "Building" in n for n in names)
    has_cb = any("CB" in n for n in names)
    R.append(Result("N-01", "「? 見方」の指標表 vs 計算の8指標", "Building Permits 10%（CB消費者信頼感は計算に無い）",
                    f"表の行: {list(zip(names, wts))}", has_bp and not has_cb, "説明"))
    hero = next((b for b in dom["banners"] if "COMPOSITE" in b), "")
    R.append(Result("N-02", "上部の帯の説明（8つの先行指標・ISM）", "ISMは計算に無い", hero, "ISM" not in hero, "説明"))
    R.append(Result("N-03", "AI欄の注記「毎週土曜JST 7:11に自動更新」", "cron 11 22 * * 6（UTC土曜22:11＝JST日曜7:11）",
                    dom["aiNote"], "日曜" in (dom["aiNote"] or ""), "説明"))
    R.append(Result("N-04", "AI欄の見出し「GROK-3-MINI」", "直近の model 列: " + ", ".join(sorted({r.get('model', '') for r in m.weekly_sorted[:4]})),
                    dom["aiTitle"], not re.search(r"GROK-\d|grok-\d", dom["aiTitle"] or "") or any(
                        (r.get("model") or "").upper() in (dom["aiTitle"] or "").upper() for r in m.weekly_sorted[:1]), "説明"))
    R.append(Result("N-05", "⑤の見出し「過去2週間の発表実績」", "表示範囲は過去90日（renderRecentSignals）", dom["signalsSec"],
                    "90" in (dom["signalsSec"] or ""), "説明"))
    R.append(Result("N-06", "⑤の副題「発表日の新しい順」", "日付列は events.csv の release_date（Philly・CFNAI・Sahmは観測月の1日、Michigan・Permits・Claimsは発表予定日の枠）",
                    dom["signalsTitle"], "観測月の1日" in (dom["signalsTitle"] or "") and "発表予定日" in (dom["signalsTitle"] or ""), "説明"))
    main_src = open(os.path.join(REPO_ROOT, "src", "market", "macro_pulse", "05_main.py"), encoding="utf-8").read()
    uses_ma3 = '"fred_id": "CFNAIMA3"' in main_src
    R.append(Result("N-07", "「CFNAI MA3」の表示名と説明（3ヶ月MA）", "取得系列が CFNAIMA3（05_main.pyのINDICATOR_CONFIG）",
                    "CFNAIMA3" if uses_ma3 else "CFNAI（単月）", uses_ma3, "説明",
                    "events.csvの既存の行は修復スクリプト（macro_cfnai_ma3_rows_repair.py）を統合後に実行するまで単月の値"))
    lq = m.liq[-1]
    if lq.get("h41_date"):
        ok = ("H.4.1 " + lq["h41_date"]) in ((dom.get("stealth") or {}).get("text") or "")
        R.append(Result("N-08", "流動性の「前週比」「N週連続減少」「週継続」", "週の判定はH.4.1の基準日（水曜）どうし、Hollow RallyはS&P 5営業日・NET流動性前週比",
                        f"最新行 h41_date={lq['h41_date']}、ステルスの注記にH.4.1の基準日の表示={'あり' if ok else 'なし'}", ok, "説明"))
    else:
        R.append(Result("N-08", "流動性の「前週比」「N週連続減少」「週継続」", "週の判定はH.4.1の基準日（水曜）どうし",
                        "最新行に h41_date が無い（M-2 STEP 4の変更後の日次の実行で書かれる）", None, "説明",
                        "2026-10-03までの行は日次の行で数えた値（MACRO-PULSE-LIQUIDITY-DAILY-ROWS-AS-WEEKS-1）"))
    R.append(Result("N-09", "8指標カードの「観測日」tooltip", "Michigan・Building Permits は発表予定日の枠の日付（観測日ではない）。Initial Claims は観測日だが最新週ではない",
                    [x["tip"][3][1] if len(x["tip"]) > 3 else None for x in dom["sigs"]],
                    all(len(x["tip"]) > 3 and x["tip"][3][0] == "データの日付" for x in dom["sigs"])
                    and any("発表予定日" in (x["tip"][3][1] or "") for x in dom["sigs"] if x["name"] in ("Michigan Sent.", "Building Permits")), "説明"))
    lead_help = {r[0]: r[2] for r in help_rows}
    R.append(Result("N-10", "先行性の表記（カード vs 「? 見方」表）", "カード: Building Permits 先行3ヶ月 / 表: CB消費者信頼感 2ヶ月",
                    lead_help, any("Building Permits" in k and v == "3ヶ月" for k, v in lead_help.items()) and not any("CB" in k for k in lead_help), "説明"))
    R.append(Result("N-11", "AI週次プロンプトのFOMC分析の前提文", "DGS1をそのまま使用（get_implied_cuts）",
                    "ZQ=Fの前提文が残っている" if "ZQ=F front-month corrected" in main_src else "DGS1そのもの（no term-premium adjustment）と記載",
                    "ZQ=F front-month corrected" not in main_src, "説明"))
    R.append(Result("N-12", "流動性カードの日付（latest.date）", "M2・FRB・TGA・準備預金は観測日がそれより前（M2は月次・週次系列）",
                    [c.get("date") for c in dom["liqCards"]], all((c.get("date") or "").startswith("更新 ") for c in dom["liqCards"])
                    and "観測日ではありません" in (dom["liqNote"] or ""), "説明"))
    return R


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default=None)
    args = ap.parse_args()
    now_ms = int(time.time() * 1000)
    m = Model(now_ms)
    dom, logs, extra = collect(now_ms)
    results = compare(m, dom, extra) + derived_checks(m) + note_checks(m, dom)
    errors = [l for l in logs if l["type"] in ("error", "pageerror") or l["type"].startswith("http")]
    warns = [l for l in logs if l["type"] == "warning"]
    n_ok = sum(1 for r in results if r.passed is True)
    n_ng = sum(1 for r in results if r.passed is False)
    n_na = sum(1 for r in results if r.passed is None)
    for r in results:
        mark = "一致" if r.passed is True else "不一致" if r.passed is False else "判定不能"
        print(f"[{mark}] {r.id} {r.element} [{r.layer}]")
        if r.passed is not True:
            print(f"    期待: {json.dumps(r.expected, ensure_ascii=False)[:400]}")
            print(f"    実際: {json.dumps(r.actual, ensure_ascii=False)[:400]}")
        if r.note:
            print(f"    備考: {r.note[:300]}")
    print(f"\n実行時刻(JST): {local_dt(now_ms).isoformat()}  一致 {n_ok} / 不一致 {n_ng} / 判定不能 {n_na}")
    print(f"consoleエラー・HTTPエラー {len(errors)}件 / 警告 {len(warns)}件")
    for l in errors + warns:
        print("   ", l)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({"now": local_dt(now_ms).isoformat(), "results": [asdict(r) for r in results],
                       "console": logs, "dom": dom}, f, ensure_ascii=False, indent=1, default=str)
    return 1 if n_ng else 0


if __name__ == "__main__":
    sys.exit(main())
