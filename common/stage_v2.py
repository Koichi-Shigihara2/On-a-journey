"""HypeCore の stage の定義v2（確定・4回目の改訂）の並行計算（docs/common/stage_v2/{T}.json）。[[STAGE-V2-ROLLOUT-1]] ①（2026-11-06）

定義は docs/architecture/HYPECORE_STAGE_DEFINITION_V2.md（確定、4回目の改訂）。共通の部品を読むだけで、部品の中身を作り直さない:
  業種 config/sector_master.json・収益の段階 common/profitability.py（時点を指定）・Layer3 の資本の使い方（buyback・dividends_paid）・
  倍率 docs/common/multiples/{T}.json・業種の中の位置 common/multiples.py の peer_percentile()
  - 倍率の種類は収益の段階で決める（黒字→PER・黒字化目前→EV/粗利・赤字→EV/売上）。切り替えは2か月続けて同じ種類が選ばれたときだけ。
    景気循環株は5年平均の利益の倍率、金融は P/TBV（どちらも収益の段階を使わない）
  - §4 の判定の順序と修正a〜d、残る穴の受け皿 h1・h2・h4・h5（h3は採らない）。値幅は条件にしない（h4・h5）
  - 持続の条件: 判定の結果が2か月の月末で続けて同じになったときだけ stage を変える（移行中・判定不能は直前の stage を保つ）
公開の値・今の HypeCore の出力は変えない（読み手は今は無い。2027-11ごろの12か月の並びの評価と、切り替えで読む）。
今の定義の stage（docs/value-monitor/hypecore/data/{T}_poc.json の同じ月）を並べて持つ。

月: 倍率の部品の月（2021-01〜前の月末）で判定し、出力は 2022-01〜（g_M に12か月前の倍率が要る）。今月の途中は作らない。

使い方:
  python common/stage_v2.py --all            # 全銘柄
  python common/stage_v2.py --ticker NVDA    # 1銘柄（登録の手順の Step 1.9。業種の中の位置のために全銘柄の材料は読む）
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from datetime import date
from statistics import median
from typing import Any, Callable, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from common import multiples as MP  # noqa: E402
from common import profitability as PF  # noqa: E402
from common import sector_master as SM  # noqa: E402

OUT_DIR = os.path.join(_REPO, "docs", "common", "stage_v2")
MULT_DIR = MP.OUT_DIR
POC_DIR = os.path.join(_REPO, "docs", "value-monitor", "hypecore", "data")
OUTPUT_FROM = "2022-01"
SCHEMA_VERSION = "1.0"

# ── 閾値（定義v2 §4、確定の値。1か所） ─────────────────────────────────────
GM_FLAT = 0.10            # g_M ほぼ0（対数）
DELTA_FLAT = 0.15         # Δ ほぼ0
DELTA_BIG = 0.30          # Δ 大きくプラス（S3。効率と株主還元の時期の会社は1.5倍）
MATURE_DELTA_FACTOR = 1.5
L_LOW, L_HIGH = 33, 67    # L（業種の中の位置・自社3年のパーセンタイル）の低い・高い
GF_FLAT = 0.05            # g_F 横ばい
GF_STABLE_MAX = 0.15      # 安定の g_F の上限
DECEL = 0.05              # 鈍化（g_F が6か月前より小さい幅）
MIN_PEERS = 5             # 業種の中で比べる最小の銘柄数（未満は全銘柄）
OWN_MONTHS, OWN_MIN = 36, 12        # 自社の過去3年の比較（12点以上）
L_WEIGHT_INDUSTRY = 0.6             # L＝業種0.6＋自社0.4（3年以上の履歴があるとき）
MATURE_RETURN_RATIO = 0.5           # 成熟: 資本還元÷FCF ≥ 0.5 かつ売上の伸び ≤ 15%
MATURE_REV_GROWTH = 0.15
PERSIST_MONTHS = 2

STAGE_LABELS = {"S0": "蓄積", "S1": "期待覚醒", "S2": "期待拡大", "S3": "陶酔", "S4": "期待剥落", "S5": "失望",
                "消化": "消化", "安定": "安定", "該当なし": "該当なし（衰退）", "移行中": "移行中", "判定不能": "判定不能"}


def _d(s: str) -> date:
    return date.fromisoformat(s[:10])


def add_months(m: str, k: int) -> str:
    y, mo = int(m[:4]), int(m[5:7]) + k
    while mo > 12:
        y, mo = y + 1, mo - 12
    while mo < 1:
        y, mo = y - 1, mo + 12
    return f"{y:04d}-{mo:02d}"


def _load(path: str) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


# ── 材料（部品を読むだけ） ───────────────────────────────────────────────
def financials(store: Dict[str, Any], master_entry: Dict[str, Any], ticker: str, d: str) -> Dict[str, Any]:
    """収益の段階の部品（時点 d）と、g_F・成熟の判定に使う TTM（部品と同じ関数）"""
    f = (store or {}).get("fields") or {}
    ent = lambda x: (f.get(x) or {}).get("entries") or []
    out: Dict[str, Any] = {"prof": PF.compute(ticker, d, store or {}, master_entry)}
    ttm = {k: PF.ttm_series(PF.quarterly_points(ent(k), d)) for k in
           ("revenue", "operating_income", "operating_cash_flow", "capital_expenditure", "buyback", "dividends_paid")}
    rev = ttm["revenue"]
    if rev:
        last = max(rev)
        prev = [e for e in rev if abs((_d(last) - _d(e)).days - 364) <= 20]
        p = prev[-1] if prev else None
        out.update(last=last, rev=rev[last], rev_p=rev.get(p) if p else None,
                   oi=ttm["operating_income"].get(last), oi_p=ttm["operating_income"].get(p) if p else None,
                   ocf=ttm["operating_cash_flow"].get(last), ocf_p=ttm["operating_cash_flow"].get(p) if p else None,
                   capex=ttm["capital_expenditure"].get(last), bb=ttm["buyback"].get(last) or 0.0,
                   div=ttm["dividends_paid"].get(last) or 0.0)
    return out


def g_f(f: Dict[str, Any]) -> Optional[float]:
    """実体の向き（§3①）: 黒字は売上と営業利益の前年比（対数）の平均、赤字は売上の前年比と営業利益率・OCFマージンの改善の幅"""
    if not f.get("rev") or not f.get("rev_p") or f["rev"] <= 0 or f["rev_p"] <= 0:
        return None
    lr = math.log(f["rev"] / f["rev_p"])
    oi, oi_p = f.get("oi"), f.get("oi_p")
    if oi is not None and oi > 0:
        return 0.5 * lr + 0.5 * max(-1.0, min(1.0, math.log(oi / oi_p))) if (oi_p and oi_p > 0) else lr
    m = oi / f["rev"] if oi is not None else None
    mp = oi_p / f["rev_p"] if oi_p is not None else None
    dm = (m - mp) if (m is not None and mp is not None) else 0.0
    om = f["ocf"] / f["rev"] if f.get("ocf") is not None else None
    omp = f["ocf_p"] / f["rev_p"] if f.get("ocf_p") is not None else None
    docf = (om - omp) if (om is not None and omp is not None) else dm
    return max(-1.0, min(1.0, lr + max(-0.5, min(0.5, 0.5 * (dm + docf)))))


def is_mature(f: Dict[str, Any]) -> bool:
    """効率と株主還元の時期（S3に入る条件を厳しく、§4補足）: 資本還元÷FCF ≥ 0.5 かつ売上の伸び ≤ 15%"""
    fcf = (f.get("ocf") or 0) - abs(f.get("capex") or 0)
    ret = abs(f.get("bb") or 0) + abs(f.get("div") or 0)
    g = math.log(f["rev"] / f["rev_p"]) if f.get("rev_p") else 0
    return fcf > 0 and ret / fcf >= MATURE_RETURN_RATIO and g <= math.log(1 + MATURE_REV_GROWTH)


def multiple_type(master_entry: Dict[str, Any], revenue_stage: str) -> str:
    """倍率の種類（§2、4回目の改訂）"""
    if master_entry.get("cyclical"):
        return "pe_5y_avg_earnings"
    if (master_entry.get("financial") or {}).get("value"):
        return "p_tbv"
    return {"profit": "per", "near_profit": "ev_gross_profit", "loss": "ev_sales"}.get(revenue_stage, "ev_sales")


# ── 判定 ─────────────────────────────────────────────────────────────
def judge(b: Dict[str, Any], hist: List[Dict[str, Any]]) -> str:
    """§4（確定）。hist は直前までの月末の {raw, gF, gM, L, close}（古い順）"""
    if "undet" in b or b.get("L") is None:
        return "判定不能"
    gF, gM, L, D = b["gF"], b["gM"], b["L"], b["Delta"]
    h6 = hist[-6:]
    h3 = hist[-3] if len(hist) >= 3 else None
    h6v = hist[-6] if len(hist) >= 6 else None
    g = lambda h, k: h.get(k) if h else None
    gF6, gM6, gM3, L6 = g(h6v, "gF"), g(h6v, "gM"), g(h3, "gM"), g(h6v, "L")
    s3_recent = any(h.get("raw") == "S3" for h in h6)
    s4_recent = any(h.get("raw") == "S4" for h in h6)
    lhigh_recent = any((h.get("L") or 0) > L_HIGH for h in h6)
    decel = gF6 is not None and gF < gF6 - DECEL
    flat = abs(gF) <= GF_FLAT
    slow = decel or flat                                   # 修正d: 横ばいは S4・S5 では鈍化の側
    thr = DELTA_BIG * (MATURE_DELTA_FACTOR if b.get("mature") else 1.0)
    px3 = g(h3, "close")
    catchup = D < -DELTA_FLAT                              # 修正a
    if gM < -GM_FLAT:                                      # S4
        turned = gM6 is None or gM6 >= -GM_FLAT
        if slow and turned and L >= L_LOW and (s3_recent or lhigh_recent):
            return "S4"
        if gF > GF_FLAT and L > L_HIGH and s3_recent:
            return "S4"
    if (slow or gF < -GF_FLAT) and gM < -GM_FLAT and gM3 is not None and gM3 < -GM_FLAT and L <= L_HIGH \
            and L6 is not None and L < L6 and s4_recent and px3 and b.get("close") and b["close"] < px3:
        return "S5"
    if D > thr and L > L_HIGH:
        return "S3"
    if gF > GF_FLAT and gM > GM_FLAT and L > L_HIGH and catchup and s3_recent:
        return "消化"                                      # h2 陶酔の後の追いつき
    if -GF_FLAT <= gF <= GF_STABLE_MAX and abs(gM) <= GM_FLAT and L_LOW <= L <= L_HIGH:
        return "安定"                                      # h5 値幅は条件にしない
    if gF > GF_FLAT and catchup and (L_LOW <= L <= L_HIGH or (L > L_HIGH and not s3_recent)):
        return "消化"
    if gF > GF_FLAT and abs(D) <= DELTA_FLAT and L >= L_LOW:
        return "S2"
    if gF > GF_FLAT and DELTA_FLAT < D <= thr and L >= L_LOW:
        return "S2"                                        # 修正b
    if D > thr and L_LOW <= L <= L_HIGH:
        return "S2"                                        # 修正b
    if L <= L_HIGH:
        gm_turn = gM > GM_FLAT and (not h6 or any((h.get("gM") is not None and h["gM"] <= GM_FLAT) for h in h6))  # 修正c
        if gF > GF_FLAT and (gF6 is None or gF6 <= GF_FLAT or gF - gF6 >= DECEL) and gm_turn:
            return "S1"
        if gF < -GF_FLAT and gm_turn:
            return "S1"
        if gF > GF_FLAT and catchup and L < L_LOW and gF6 is not None and gF > gF6 and gM3 is not None and gM > gM3:
            return "S1"
        if D > thr and L < L_LOW:
            return "S1"                                    # 修正b
        if gF > GF_FLAT and gM > GM_FLAT and L < L_LOW:
            return "S1"                                    # h1 覚醒の継続
    if L < L_LOW and ((gF >= -GF_FLAT and abs(gM) <= GM_FLAT) or ((gF > GF_FLAT or flat) and catchup)):
        return "S0"                                        # h4 値幅は条件にしない・修正d
    if gF < -GF_FLAT and gM <= GM_FLAT:
        return "該当なし"
    return "移行中"


def persist(seq: List[str]) -> List[Optional[str]]:
    """持続の条件（2か月）"""
    out: List[Optional[str]] = []
    cur, cand, cnt = None, None, 0
    for s in seq:
        if s in ("移行中", "判定不能"):
            out.append(cur)
            continue
        if cur is None:
            cur = s
        elif s != cur:
            cnt = cnt + 1 if s == cand else 1
            cand = s
            if cnt >= PERSIST_MONTHS:
                cur, cand, cnt = s, None, 0
        else:
            cand, cnt = None, 0
        out.append(cur)
    return out


# ── 全銘柄の月ごとの計算 ───────────────────────────────────────────────
def compute_all(tickers: List[str], master: Dict[str, Any], mult: Dict[str, Dict[str, dict]],
                stores: Dict[str, Dict[str, Any]], daily: Dict[str, List[dict]]) -> Dict[str, List[Dict[str, Any]]]:
    months = sorted({m for t in tickers for m in mult.get(t, {})})
    sector = {t: master[t].get("sector") for t in tickers}
    hist: Dict[str, List[Dict[str, Any]]] = {t: [] for t in tickers}
    tstate: Dict[str, Dict[str, Any]] = {t: {} for t in tickers}
    rows: Dict[str, List[Dict[str, Any]]] = {t: [] for t in tickers}
    for m in months:
        bases: Dict[str, Dict[str, Any]] = {}
        for t in tickers:
            row = mult.get(t, {}).get(m) or {}
            d = row.get("date") or next((mult[u][m]["date"] for u in tickers if m in mult.get(u, {})), m + "-28")
            b: Dict[str, Any] = {"date": d}
            if master[t].get("unclassified"):
                b["undet"] = "unclassified"
            elif not row or row.get("undetermined"):
                b["undet"] = (row or {}).get("undetermined") or "no_multiples"
            else:
                f = financials(stores.get(t) or {}, master[t], t, d)
                gF = g_f(f)
                if gF is None:
                    b["undet"] = "no_ttm" if not f.get("rev") else "no_g_f"
                else:
                    stage = f["prof"]["revenue_stage"]
                    chosen = multiple_type(master[t], stage)
                    st = tstate[t]
                    mtype = st["used"] if (st.get("used") and chosen != st["used"] and st.get("cand") != chosen) else chosen
                    r12 = mult[t].get(add_months(m, -12)) or {}
                    now, old = row.get(mtype), r12.get(mtype)
                    if (not now or not old) and mtype != "ev_sales":
                        mtype = "ev_sales"           # §2: 両端で同じ倍率が取れないときは両端とも EV/売上
                        now, old = row.get(mtype), r12.get(mtype)
                    b.update(chosen=chosen, revenue_stage=stage, gF=gF, mature=is_mature(f))
                    if not now or not old or now <= 0 or old <= 0:
                        b["undet"] = "no_multiple_change"
                    else:
                        b.update(mtype=mtype, mult=now, gM_abs=math.log(now / old))
            bases[t] = b
        ok = {t: b for t, b in bases.items() if "undet" not in b}
        by = defaultdict(list)
        for t, b in ok.items():
            by[sector[t]].append(b["gM_abs"])
        allg = [b["gM_abs"] for b in ok.values()]
        for t, b in ok.items():
            peers = by[sector[t]] if len(by[sector[t]]) >= MIN_PEERS else allg
            b["gM"] = b["gM_abs"] - median(peers)
            vals = {u: x["mult"] for u, x in ok.items() if x["mtype"] == b["mtype"]}
            li = MP.peer_percentile(vals, t, sector, MIN_PEERS)
            first = daily[t][0]["date"] if daily.get(t) else b["date"]
            lo = None
            if (_d(b["date"]) - _d(first)).days >= 3 * 365:
                h = [(mult[t].get(add_months(m, -k)) or {}).get(b["mtype"]) for k in range(1, OWN_MONTHS + 1)]
                h = [x for x in h if x]
                if len(h) >= OWN_MIN:
                    lo = 100.0 * sum(1 for x in h if x < b["mult"]) / len(h)
            b["L_industry"], b["L_own"] = li, lo
            b["L"] = None if li is None and lo is None else (lo if li is None else (li if lo is None else
                                                              L_WEIGHT_INDUSTRY * li + (1 - L_WEIGHT_INDUSTRY) * lo))
            b["Delta"] = b["gM"] - b["gF"]
        for t in tickers:
            b = bases[t]
            px = MP.close_on_or_before(daily.get(t) or [], b["date"])
            b["close"] = px["close"] if px else None
            raw = judge(b, hist[t])
            # 判定不能の月の g_F は、後の月の「6か月前」の比べに使わない（段Cの検証と同じ。倍率が無い月は材料がそろっていない）
            hist[t].append({"raw": raw, "gF": None if "undet" in b else b.get("gF"), "gM": b.get("gM"), "L": b.get("L"),
                            "close": b["close"]})
            if "chosen" in b:
                prev = tstate[t].get("used")
                used = b.get("mtype") or prev
                tstate[t] = {"used": used, "cand": b["chosen"] if b["chosen"] != b.get("mtype") else None}
            rnd = lambda v, n=4: None if v is None else round(v, n)
            rows[t].append({"month": m, "date": b["date"], "raw": raw,
                            "undetermined": b.get("undet"), "multiple_type": b.get("mtype"), "revenue_stage": b.get("revenue_stage"),
                            "g_f": rnd(b.get("gF")), "g_m": rnd(b.get("gM")), "delta": rnd(b.get("Delta")),
                            "l": rnd(b.get("L"), 1), "l_industry": rnd(b.get("L_industry"), 1), "l_own": rnd(b.get("L_own"), 1),
                            "mature": b.get("mature")})
    return rows


def finalize(ticker: str, rows: List[Dict[str, Any]], poc_dir: str = POC_DIR) -> Dict[str, Any]:
    out_rows = [r for r in rows if r["month"] >= OUTPUT_FROM]
    stages = persist([r["raw"] for r in out_rows])
    poc = {m["month"]: m for m in (_load(os.path.join(poc_dir, f"{ticker}_poc.json")).get("monthly") or [])}
    for r, s in zip(out_rows, stages):
        r["stage"] = s
        r["stage_label"] = STAGE_LABELS.get(s) if s else None
        p = poc.get(r["month"])
        r["current_definition_stage"] = p.get("stage") if p else None
        r["current_definition_label"] = p.get("stage_label") if p else None
    return {"_schema_version": SCHEMA_VERSION, "ticker": ticker,
            "_description": "HypeCoreのstageの定義v2（確定・4回目の改訂）の並行計算（common/stage_v2.py が機械で作る）。raw は月末の生の判定、"
                            "stage は持続の条件（2か月）の後。current_definition_* は同じ月の今の定義（_poc.json）。公開の値は変えない。"
                            "読み手は2027-11ごろの12か月の並びの評価と切り替え（[[STAGE-V2-ROLLOUT-1]]）",
            "months": out_rows}


def _default_store(ticker: str) -> Optional[Dict[str, Any]]:
    import logging
    from common.sec_data.layer3_builder import build_ticker_store
    logging.disable(logging.CRITICAL)
    try:
        return build_ticker_store(ticker)
    finally:
        logging.disable(logging.NOTSET)


def build(tickers: Optional[List[str]] = None, out_dir: str = OUT_DIR, store_fn: Callable = _default_store,
          lookup_path: Optional[str] = None) -> Dict[str, Any]:
    """作り直してファイルに書く。業種の中の位置のために全銘柄の材料を読む（書くのは tickers だけ、None は全銘柄）"""
    master = SM.load().get("tickers") or {}
    universe = sorted(t for t in SM.registered_tickers(lookup_path) if t in master)
    mult = {t: {r["month"]: r for r in (_load(os.path.join(MULT_DIR, f"{t}.json")).get("months") or [])} for t in universe}
    stores = {t: store_fn(t) or {} for t in universe}
    daily = {t: MP._daily_rows(t) for t in universe}
    rows = compute_all(universe, master, mult, stores, daily)
    targets = universe if tickers is None else [t.upper() for t in tickers if t.upper() in rows]
    written = 0
    for t in targets:
        if MP._write_if_changed(os.path.join(out_dir, f"{t}.json"), finalize(t, rows[t])):
            written += 1
    return {"tickers": len(targets), "written": written, "months": len(next(iter(rows.values()))) if rows else 0}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="HypeCoreのstageの定義v2の並行計算（docs/common/stage_v2/{T}.json）")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true")
    g.add_argument("--ticker", action="append")
    a = ap.parse_args(argv)
    s = build(None if a.all else a.ticker)
    print(f"定義v2の並行計算: {s['tickers']}銘柄、書いた {s['written']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
