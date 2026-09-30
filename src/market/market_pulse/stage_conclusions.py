"""Market Pulse 8段階の「結論1行」・天気・data_quality・過去の実績（指示書㉓ 実装A）。

判定ルールは docs/architecture/MARKET_PULSE_REDESIGN.md の1・2・5・6章のとおり。閾値はすべてここに置き、
scripts/analysis/market_pulse_redesign.py（設計書の分布の再計算）と同じ値を使う。

本モジュールの関数は、market_data.jsonのエントリの値（indicators・asset_flow・breadth）と
common/market_data/daily/の終値だけを入力にする純粋な計算で、外部取得はしない。
実装Aで出せる段階は0・1・2・3・4・5・8。段階6・7と段階5のセクター4象限は実装Bで追加する。
"""
from __future__ import annotations

import math
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

# ── 段階1・2 ────────────────────────────────────────────
S1_BANDS = ((1.0, "大幅高"), (0.3, "上昇"), (-0.3, "小動き"), (-1.0, "下落"))
S2_THRESH = {"金利": 8.0, "原油": 3.0, "VIX": 10.0, "為替": 0.7}   # 金利はbp、他は%
TAG_NASDAQ_PT = 0.5
TAG_GOLD_PCT = 1.5
# ── 段階4・5 ────────────────────────────────────────────
S5_PT = 0.5
# ── 段階8 ─────────────────────────────────────────────
S8_MIN_N = 20
# 指示書㉔ STEP B: |z|≥2（偶然のばらつき〈標準誤差〉の約2倍）のときだけ「平常時より上昇が多い／少ない」。
# 差（pt）は表示するが判定には使わない
S8_Z = 2.0
# 見出しの結論に使うシグナル（有意になる割合が高い方。5年分で段階1のみ15.9%・段階1×VIX水準2.4%、5営業日後）
S8_HEADLINE_SIGNAL = "stage1"
S8_HORIZONS = (1, 5, 20)
S8_HISTORY_DAYS = 1600   # 2021-01〜の約5.5年分（daily/の全期間）

# data_qualityの判定に使う要素 → その値を使う段階
DQ_INDICATORS = {
    "S&P500": (1, 3, 8), "NASDAQ": (1,), "VIX指数": (1, 2, 8), "米10年債": (1, 2), "ドル円": (1, 2),
    "WTI原油": (1, 2), "金（GOLD）": (1,), "S&P500グロース(IVW)": (5,), "S&P500バリュー(IVE)": (5,),
}
DQ_ASSET_FLOW = {"ultra_short": (4,), "gold": (4,), "long_bond": (4,), "ig_bond": (4,), "hy_bond": (4,), "equity": (4,)}


def _num(x) -> Optional[float]:
    if isinstance(x, bool) or not isinstance(x, (int, float)):
        return None
    return None if math.isnan(x) else float(x)


def _ind(ind: dict, key: str, field: str = "change_percent") -> Optional[float]:
    v = (ind or {}).get(key)
    if not isinstance(v, dict):
        return None
    # [[MARKETDATA-FUTURES-ROLL-1]]: 限月乗り換え日（同じ限月で比べられなかった日）と、過去のエントリで乗り換えの
    # 可能性がある日（roll_suspect）は、前日比による判定から外す（値はそのまま残す）
    if field in ("change_percent", "change") and (v.get("roll_suspect") or (v.get("contract_roll") or {}).get("method") == "excluded"):
        return None
    return _num(v.get(field))


def tnx_change_bp(ind: dict) -> Optional[float]:
    """10年債利回りの前営業日差（bp）。change_bpがあればそれ、無ければchange（%pt）×100。"""
    v = (ind or {}).get("米10年債")
    if not isinstance(v, dict):
        return None
    bp = _num(v.get("change_bp"))
    if bp is not None:
        return bp
    ch = _num(v.get("change"))
    return None if ch is None else round(ch * 100, 1)


# ─────────────────────────────────────────────────────────
#  段階1〜5・天気
# ─────────────────────────────────────────────────────────

def s1_label(spx_pct: Optional[float]) -> Optional[str]:
    if spx_pct is None:
        return None
    for th, lab in S1_BANDS:
        if (spx_pct >= th) if th > 0 else (spx_pct > th):
            return lab
    return "大幅安"


def stage1(ind: dict) -> dict:
    spx = _ind(ind, "S&P500")
    nasdaq = _ind(ind, "NASDAQ")
    tags = []
    if spx is not None and nasdaq is not None:
        d = nasdaq - spx
        if d >= TAG_NASDAQ_PT:
            tags.append(f"NASDAQ優位（{d:+.2f}pt）")
        elif d <= -TAG_NASDAQ_PT:
            tags.append(f"NASDAQ劣後（{d:+.2f}pt）")
    vix = _ind(ind, "VIX指数")
    if vix is not None and abs(vix) >= S2_THRESH["VIX"]:
        tags.append(f"VIX急変（{vix:+.1f}%）")
    bp = tnx_change_bp(ind)
    if bp is not None and abs(bp) >= S2_THRESH["金利"]:
        tags.append(f"10年債（{bp:+.0f}bp）")
    fx = _ind(ind, "ドル円")
    if fx is not None and abs(fx) >= S2_THRESH["為替"]:
        tags.append(f"ドル円（{fx:+.2f}%）")
    oil = _ind(ind, "WTI原油")
    if oil is not None and abs(oil) >= S2_THRESH["原油"]:
        tags.append(f"原油（{oil:+.2f}%）")
    gold = _ind(ind, "金（GOLD）")
    if gold is not None and abs(gold) >= TAG_GOLD_PCT:
        tags.append(f"金（{gold:+.2f}%）")
    label = s1_label(spx)
    return {"label": label, "line": None if label is None else f"S&P500 {spx:+.2f}%（{label}）",
            "tags": tags, "inputs": {"sp500_pct": spx}}


def s2_items(tnx_bp, oil, vix, fx) -> Optional[List[str]]:
    vals = {"金利": tnx_bp, "原油": oil, "VIX": vix, "為替": fx}
    if all(v is None for v in vals.values()):
        return None
    return [k for k, v in vals.items() if v is not None and abs(v) >= S2_THRESH[k]]


def stage2(ind: dict) -> dict:
    items = s2_items(tnx_change_bp(ind), _ind(ind, "WTI原油"), _ind(ind, "VIX指数"), _ind(ind, "ドル円"))
    line = None if items is None else "同時に大きく動いたもの：" + ("・".join(items) if items else "なし")
    return {"label": line, "line": line, "items": items}


def s3_label(spx, adv, dec) -> Optional[str]:
    if spx is None or adv is None or dec is None or (adv + dec) == 0:
        return None
    adr = adv / max(dec, 1)
    if spx >= 0.3:
        return "広がりのある上昇" if adr >= 1.5 else ("一部主導の上昇" if adr < 1.0 else "やや広がりのある上昇")
    if spx <= -0.3:
        return "広く売られた下落" if adr <= 0.67 else ("指数主導の下落" if adr > 1.0 else "やや広い下落")
    return "方向感なし"


def stage3(ind: dict, breadth: Optional[dict]) -> dict:
    b = breadth or {}
    spx = _ind(ind, "S&P500")
    adv, dec = b.get("advances"), b.get("declines")
    label = s3_label(spx, adv, dec)
    line = None if label is None else f"{label}（上昇{adv}・下落{dec}銘柄）"
    return {"label": label, "line": line,
            "inputs": {"sp500_pct": spx, "advances": adv, "declines": dec, "breadth_date": b.get("date")}}


def s4_label(risk, safe) -> Optional[str]:
    if risk is None or safe is None:
        return None
    if risk >= 0.3 and risk - safe >= 0.3:
        return "リスク資産へ"
    if safe >= 0.1 and risk <= -0.3:
        return "安全資産へ"
    if risk <= -0.3 and safe <= -0.1:
        return "全面安（現金化）"
    return "偏りなし"


def stage4(af: Optional[dict]) -> dict:
    af = af or {}

    def pct(k):
        v = af.get(k)
        return _num(v.get("change_pct")) if isinstance(v, dict) else None
    r = [pct("equity"), pct("hy_bond")]
    s = [pct("long_bond"), pct("gold")]
    risk = None if None in r else sum(r) / 2
    safe = None if None in s else sum(s) / 2
    label = s4_label(risk, safe)
    line = None if label is None else f"{label}（リスク資産{risk:+.2f}%・安全資産{safe:+.2f}%）"
    return {"label": label, "line": line,
            "inputs": {"risk_avg_pct": None if risk is None else round(risk, 3),
                       "safe_avg_pct": None if safe is None else round(safe, 3)}}


def s5_label(gv) -> Optional[str]:
    if gv is None:
        return None
    if gv >= S5_PT:
        return "グロース優勢"
    if gv <= -S5_PT:
        return "バリュー優勢"
    return "拮抗"


def stage5(ind: dict) -> dict:
    gv = _ind(ind, "グロース対バリュー比", "diff_percent")
    label = s5_label(gv)
    line = None if label is None else f"{label}（IVW−IVE {gv:+.2f}pt）"
    return {"label": label, "line": line, "inputs": {"ivw_minus_ive_pt": gv}}


def weather(s3: Optional[str], s4: Optional[str]) -> Optional[str]:
    """設計書2章のv3。段階3・4のどちらかが判定できない日はNone。"""
    if s3 is None or s4 is None:
        return None
    if s3 == "広く売られた下落" and s4 in ("安全資産へ", "全面安（現金化）"):
        return "嵐"
    if s3 in ("広がりのある上昇", "やや広がりのある上昇", "一部主導の上昇") and s4 != "安全資産へ":
        return "晴れ"
    return "曇り"


WEATHER_REASON = {
    "嵐": "下落が広く、資金が安全資産へ逃げたか全面安",
    "晴れ": "上昇していて、資金が安全資産へ逃げていない",
    "曇り": "晴れ・嵐のどちらの条件にも当たらない",
}


# ─────────────────────────────────────────────────────────
#  段階0 data_quality
# ─────────────────────────────────────────────────────────

def data_quality(ind: dict, af: Optional[dict], breadth: Optional[dict], expected_close: Optional[str]) -> dict:
    """設計書6章。期待する終値日と要素ごとのas_ofを比べる（FRED系列は判定から外す）。"""
    as_of: Dict[str, Optional[str]] = {}
    stages: Dict[str, Tuple[int, ...]] = {}
    fallback = set()
    provisional = set()   # 指示書㉕ STEP B: 暫定の値（日足の確定前に保存された行）を含む要素はpartialにする
    for k, st in DQ_INDICATORS.items():
        v = (ind or {}).get(k)
        as_of[k] = v.get("date") if isinstance(v, dict) else None
        stages[k] = st
        if isinstance(v, dict) and v.get("is_fallback"):
            fallback.add(k)
        if isinstance(v, dict) and v.get("provisional"):
            provisional.add(k)
    for k, st in DQ_ASSET_FLOW.items():
        v = (af or {}).get(k)
        name = f"asset_flow.{k}"
        as_of[name] = v.get("date") if isinstance(v, dict) else None
        stages[name] = st
        if isinstance(v, dict) and v.get("is_fallback"):
            fallback.add(name)
        if isinstance(v, dict) and v.get("provisional"):
            provisional.add(name)
    as_of["breadth"] = (breadth or {}).get("date")
    stages["breadth"] = (3,)
    out = {"expected_close_date": expected_close, "as_of": as_of, "status": "unknown",
           "old_elements": [], "old_stages": [], "provisional_elements": sorted(provisional)}
    if not expected_close or as_of.get("S&P500") is None:
        return out   # S&P500の基準日が無い（取得失敗、または基準日を記録する前の2026-06-07以前のエントリ）は判定不能
    spx = as_of.get("S&P500")
    old = sorted(k for k, d in as_of.items() if d is None or d < expected_close or k in fallback or k in provisional)
    out["old_elements"] = old
    out["old_stages"] = sorted({s for k in old for s in stages[k]})
    if spx < expected_close or "S&P500" in fallback:
        out["status"] = "stale"
    else:
        out["status"] = "complete" if not old else "partial"
    return out


def stage0(dq: dict) -> dict:
    st, exp = dq.get("status"), dq.get("expected_close_date")
    if st == "complete":
        line = f"{exp}の終値"
    elif st == "partial":
        line = (f"{exp}の終値（一部の値が前営業日または暫定）" if dq.get("provisional_elements")
                else f"{exp}の終値（一部の値が前営業日）")
    elif st == "stale":
        line = "前営業日のデータ（最新の終値が未反映）"
    else:
        line = "データ基準日を判定できない"
    return {"label": st, "line": line}


# ─────────────────────────────────────────────────────────
#  段階8 過去の実績（同じシグナルの日のその後、基準率との差）
# ─────────────────────────────────────────────────────────

def vix_zone(v: Optional[float]) -> Optional[str]:
    if v is None:
        return None
    return "VIX<15" if v < 15 else "VIX15-20" if v < 20 else "VIX20-30" if v < 30 else "VIX>=30"


def _closes(get_series, symbol: str, as_of: str) -> List[Tuple[str, float]]:
    rows = get_series(symbol, as_of, S8_HISTORY_DAYS)
    return [(r["date"], float(r["close"])) for r in rows
            if not r.get("_gap") and _num(r.get("close")) is not None and r["close"] > 0]


def s8_label(n: int, z: Optional[float]) -> str:
    if n < S8_MIN_N or z is None:
        return "件数不足"
    if z >= S8_Z:
        return "平常時より上昇が多い"
    if z <= -S8_Z:
        return "平常時より上昇が少ない"
    return "平常時と差なし"


def _outcome(closes: List[float], signals: List[Optional[str]], today: str, h: int) -> dict:
    """結果が確定している日（j+h ≤ 最終日）だけで、同じシグナルの日と全日の上昇割合を比べる。"""
    last = len(closes) - 1
    same, base = [], []
    for j in range(0, last - h + 1):
        r = (closes[j + h] / closes[j] - 1) * 100
        base.append(r)
        if signals[j] == today:
            same.append(r)
    n = len(same)
    out = {"horizon": h, "n": n, "base_n": len(base)}
    if not base:
        out.update({"label": "件数不足"})
        return out
    b = sum(1 for r in base if r > 0) / len(base)
    out["base_up_pct"] = round(b * 100, 1)
    if n < S8_MIN_N:
        out["label"] = "件数不足"
        return out
    p = sum(1 for r in same if r > 0) / n
    diff = (p - b) * 100
    se = math.sqrt(b * (1 - b) / n) * 100
    z = diff / se if se else 0.0
    out.update({"up_pct": round(p * 100, 1), "diff_pt": round(diff, 1), "z": round(z, 2),
                "mean_pct": round(sum(same) / n, 3), "median_pct": round(median(same), 3),
                "label": s8_label(n, z),
                "guide": "偶然では出にくい差" if abs(z) >= S8_Z else "偶然でも出る範囲の差"})
    return out


def stage8(get_series, as_of: Optional[str]) -> dict:
    """get_series(symbol, as_of, days)はreader.get_price_series_as_of互換。as_ofはS&P500の終値日。
    as_of以前の終値だけを使うため、過去のエントリに当てはめても先読みにならない。"""
    if not as_of:
        return {"label": None, "line": None}
    spx = _closes(get_series, "^GSPC", as_of)
    vix = dict(_closes(get_series, "^VIX", as_of))
    if len(spx) < 30 or spx[-1][0] != as_of:
        return {"label": None, "line": None, "note": "S&P500の終値が不足"}
    dates = [d for d, _ in spx]
    closes = [c for _, c in spx]
    s1 = [None] + [s1_label((closes[i] / closes[i - 1] - 1) * 100) for i in range(1, len(closes))]
    combo = [None if a is None or vix_zone(vix.get(d)) is None else f"{a}|{vix_zone(vix.get(d))}"
             for a, d in zip(s1, dates)]
    res = {"as_of": as_of, "history_from": dates[0], "signals": {}}
    for name, sig in (("stage1_vix", combo), ("stage1", s1)):
        today = sig[-1]
        res["signals"][name] = {"signal": today,
                                "outcomes": [_outcome(closes, sig, today, h) for h in S8_HORIZONS] if today else []}
    main = res["signals"][S8_HEADLINE_SIGNAL]
    res["headline_signal"] = S8_HEADLINE_SIGNAL
    five = next((o for o in main["outcomes"] if o["horizon"] == 5), None)
    if five is None:
        res.update({"label": None, "line": None})
    elif five["label"] == "件数不足":
        res.update({"label": "件数不足", "line": f"5営業日後: 件数不足（{five['n']}件）"})
    else:
        res.update({"label": five["label"],
                    "line": f"5営業日後: {five['label']}（{five['n']}件、{five['diff_pt']:+.1f}pt、z={five['z']:+.2f}）"})
    return res


# ─────────────────────────────────────────────────────────
#  まとめ
# ─────────────────────────────────────────────────────────

def build_stage_conclusions(ind: dict, af: Optional[dict], breadth: Optional[dict],
                            expected_close: Optional[str], get_series=None) -> Dict[str, Any]:
    dq = data_quality(ind, af, breadth, expected_close)
    st = {"0": stage0(dq), "1": stage1(ind), "2": stage2(ind), "3": stage3(ind, breadth),
          "4": stage4(af), "5": stage5(ind)}
    if get_series is not None:
        spx_date = ((ind or {}).get("S&P500") or {}).get("date") if isinstance((ind or {}).get("S&P500"), dict) else None
        try:
            st["8"] = stage8(get_series, spx_date)
        except Exception as e:  # 過去の実績が出せなくても他の段階は出す
            st["8"] = {"label": None, "line": None, "note": f"{type(e).__name__}: {e}"}
    w = weather(st["3"]["label"], st["4"]["label"])
    return {"stages": st, "weather": {"label": w, "reason": WEATHER_REASON.get(w), "rule": "v3",
                                      "inputs": {"stage3": st["3"]["label"], "stage4": st["4"]["label"]}},
            "data_quality": dq}
