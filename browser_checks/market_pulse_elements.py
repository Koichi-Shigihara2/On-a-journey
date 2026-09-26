"""Market Pulse 全画面要素の実ブラウザ確認（check_dependency_map.pyから呼ばれる）

2026-09-26新設（指示書⑲ STEP 3）。SYSTEM_MAP.md「Market Pulse・MACRO PULSE
画面要素→導出関数→生データソース 依存関係マップ」のMarket Pulse全29要素
（MP-01〜MP-29）について、次の3層を突き合わせる。

  - 描画: market_data.json最新エントリから本スクリプトが独立に算出した
    表示期待値（index.htmlの整形規則〈toFixed・Math.round・閾値〉を
    Python側で再実装）と、実ブラウザのDOM/チャートの値
  - 導出: 生データ（common/market_data/daily/・breadth_data.json等）から
    導出値（センチメントスコア・前日比・資産フロー騰落率・ブレッス）を
    独立に再計算し、market_data.jsonの記録値と比較（D-01〜D-05）
  - データ: 各要素の値の「データ基準日」を、米国市場の直近終値日と比較
    （classify_base_dates()、STEP 2の分類表の再現用）

結果は一致（True）・不一致（False）・判定不能（None）の3値。不一致は
どの層で食い違っているかをlayerに記録する。本番のコード・データは
読むだけで変更しない。
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

MARKET_DATA_JSON = os.path.join(
    REPO_ROOT, "docs", "market-monitor", "market-pulse", "data", "market_data.json"
)
BREADTH_DATA_JSON = os.path.join(
    REPO_ROOT, "docs", "market-monitor", "market-pulse", "data", "breadth_data.json"
)
SP500_TICKERS_JSON = os.path.join(
    REPO_ROOT, "docs", "market-monitor", "market-pulse", "data", "sp500_tickers.json"
)
JST = timezone(timedelta(hours=9))

SUB_SCORE_NAMES = {
    "vix_level": "VIX水準", "sp500_ma_dev": "S&P/50日MA", "ad_ratio": "騰落比率",
    "hyg_lqd_dir": "クレジット", "nh_nl": "NH-NL差", "growth_value": "グロース優勢",
    "distribution": "出来高圧力", "rsp_spy_divergence": "Equal Weight乖離",
}
# index.htmlのINFO MODAL「計算式」表の行名 → sub_scoresのキー（2026-09-26に8指標・新重みへ更新）
MODAL_ROW_KEYS = {"VIX Level": "vix_level", "S&P vs 50MA": "sp500_ma_dev", "AD Ratio 5d": "ad_ratio",
                  "HYG/LQD": "hyg_lqd_dir", "NH-NL Diff": "nh_nl", "Growth/Value": "growth_value",
                  "Volume Flow": "distribution", "Equal Weight乖離": "rsp_spy_divergence"}


def fg_zone_label(s: float, rating: Optional[str]) -> str:
    """index.htmlのfgZoneLabel()（CNNのratingがあれば大文字、無ければCNNの区分）。"""
    if rating:
        return str(rating).upper()
    if s <= 25: return "EXTREME FEAR"
    if s <= 45: return "FEAR"
    if s <= 55: return "NEUTRAL"
    if s <= 75: return "GREED"
    return "EXTREME GREED"


def bought(key: str, p: Optional[float]) -> Optional[float]:
    """index.htmlのbought(): 短期国債（利回りの変化率）は「買われた」方向の符号に反転する。"""
    if p is None:
        return None
    return -p if key == "short_bond" else p
CARD_DEFS = [("S&P500", "S&P500"), ("NASDAQ", "NASDAQ"), ("米10年債", "10Y利回"),
             ("ドル円", "USD/JPY"), ("WTI原油", "WTI原油"), ("金（GOLD）", "GOLD")]
AF_KEYS = ["ultra_short", "short_bond", "gold", "long_bond", "ig_bond", "hy_bond", "equity"]
# 資産フローのキー → common/market_data/daily/のシンボル（short_bondはFRED DGS3MO）
AF_SYMBOLS = {"ultra_short": "SHV", "gold": "GLD", "long_bond": "TLT", "ig_bond": "LQD",
              "hy_bond": "HYG", "equity": "SPY"}
INDICATOR_SYMBOLS = {
    "米10年債": "^TNX", "VIX指数": "^VIX", "VIX9D（短期VIX）": "^VIX9D", "ドル円": "JPY=X",
    "S&P500": "^GSPC", "NASDAQ": "^IXIC", "WTI原油": "CL=F", "金（GOLD）": "GC=F",
    "HYG（ハイイールド債ETF）": "HYG", "LQD（投資適格債ETF）": "LQD",
    "S&P500グロース(IVW)": "IVW", "S&P500バリュー(IVE)": "IVE", "Russell2000小型(RUT)": "^RUT",
    "NYSE Composite": "^NYA",
}


# ─────────────────────────────────────────────────────────────────
#  JSの数値整形の再実装
# ─────────────────────────────────────────────────────────────────

def js_fixed(v: float, n: int) -> str:
    """Number.prototype.toFixed(n)と同じ結果（2進の厳密値を四捨五入）。"""
    q = Decimal(1).scaleb(-n)
    s = str(Decimal(float(v)).quantize(q, rounding=ROUND_HALF_UP))
    return "0" if s in ("-0", "-0.0") and n == 0 else s


def js_round(v: float) -> int:
    """Math.round（0.5は+∞方向）。"""
    return int((Decimal(float(v)) + Decimal("0.5")).to_integral_value(rounding="ROUND_FLOOR"))


def js_num(v) -> str:
    """JSのテンプレート文字列での数値表示（26.0→"26"、26.5→"26.5"）。"""
    return str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)


def signed(v: float, n: int) -> str:
    return ("+" if v > 0 else "") + js_fixed(v, n)


def sentiment_label(s: float) -> str:
    if s <= 20: return "EXTREME FEAR"
    if s <= 35: return "FEAR"
    if s <= 50: return "CAUTION"
    if s <= 65: return "NEUTRAL"
    if s <= 80: return "GREED"
    return "EXTREME GREED"


def tp_label(s: float) -> str:
    return sentiment_label(s)


# ─────────────────────────────────────────────────────────────────
#  データ読込（index.htmlのinit()と同じ並べ替え・期間フィルタ）
# ─────────────────────────────────────────────────────────────────

def load_entries() -> list:
    with open(MARKET_DATA_JSON, encoding="utf-8") as f:
        data = json.load(f)
    return sorted(data, key=lambda d: datetime.fromisoformat(d["date"]))


def filter_days(entries: list, days: int, now: datetime) -> list:
    """applyPeriod()/renderOscChart()の「now−days日以降」フィルタ。"""
    if days >= 9999:
        return list(entries)
    cutoff = now - timedelta(days=days)
    out = [d for d in entries if datetime.fromisoformat(d["date"]) >= cutoff]
    return out or list(entries)


def fmt_md(iso: str) -> str:
    d = datetime.fromisoformat(iso).astimezone(JST)
    return f"{d.month}/{d.day}"


# ─────────────────────────────────────────────────────────────────
#  描画層: 表示期待値の独立算出
# ─────────────────────────────────────────────────────────────────

def expected_signal(filtered: list) -> str:
    """renderGauge()のシグナルバッジ（JSONにsignalが無い場合のフロント簡易判定）。"""
    latest = filtered[-1]["sentiment"]
    if latest.get("signal"):
        return latest["signal"]["signal"]
    score = latest["score"]
    if score <= 20:
        return "BUY"
    if len(filtered) >= 3:
        recent = [d.get("sentiment", {}).get("score") for d in filtered[-20:]]
        recent = [x for x in recent if x is not None]
        if len(recent) >= 3:
            peak = max(recent)
            if peak >= 70 and (peak - score) >= 5:
                return "TAKE PROFIT"
    return "HOLD"


def expected_mini_gauges(entries: list, filtered: list) -> list:
    """renderMiniGauges()の3枚（明日・5日後・20日後）の予測スコア表示。"""
    cur = filtered[-1]["sentiment"]["score"]
    valid = [d for d in entries if ((d.get("indicators") or {}).get("S&P500") or {}).get("value") is not None
             and (d.get("sentiment") or {}).get("score") is not None]
    zones = [(0, 20, "Extreme Fear"), (21, 40, "Fear"), (41, 60, "Neutral"), (61, 80, "Greed"),
             (81, 100, "Extreme Greed")]
    zone = next((z for z in zones if z[0] <= cur <= z[1]), zones[2])
    out = []
    for idx in (1, 5, 20):
        rets = []
        for i in range(len(valid) - idx):
            s = valid[i]["sentiment"]["score"]
            if s < zone[0] or s > zone[1]:
                continue
            s0 = valid[i]["indicators"]["S&P500"]["value"]
            sn = ((valid[i + idx].get("indicators") or {}).get("S&P500") or {}).get("value")
            if s0 and sn and s0 > 1000 and sn > 1000:
                rets.append((sn - s0) / s0 * 100)
        avg = sum(rets) / len(rets) if rets else None
        pred = min(100, max(0, cur + avg * 2)) if avg is not None else cur
        out.append({"score": js_round(pred), "n": len(rets),
                    "ret": (("+" if avg >= 0 else "") + js_fixed(avg, 2) + "%") if avg is not None else None})
    return out


def expected_asset_flow_judge(entries: list) -> tuple:
    with_af = [d for d in entries if d.get("asset_flow") is not None]
    rec = with_af[-5:]

    def avg(keys):
        vs = [bought(k, ((d.get("asset_flow") or {}).get(k) or {}).get("change_pct")) for d in rec for k in keys]
        vs = [v for v in vs if v is not None]
        return sum(vs) / len(vs) if vs else None
    safe, risk = avg(["ultra_short", "short_bond"]), avg(["hy_bond", "equity"])
    if risk is not None and risk > 0.3:
        j = "リスク資産に資金流入"
    elif safe is not None and safe > 0.05 and (risk is None or risk < -0.1):
        j = "安全資産に資金流入"
    else:
        j = "混在・方向感なし"
    return j, risk, safe, len(rec)


def pct_text(p: Optional[float]) -> str:
    return "—" if p is None else ("+" if p >= 0 else "") + js_fixed(p, 2) + "%"


# ─────────────────────────────────────────────────────────────────
#  ブラウザ側の値取得（1回のevaluateで全要素のDOM/チャートを取得）
# ─────────────────────────────────────────────────────────────────

DOM_SNAPSHOT_JS = """
() => {
  const t = id => { const e = document.getElementById(id); return e ? e.innerText.trim() : null; };
  const ds = ch => ch ? ch.data.datasets.map(d => ({label: d.label, raw: d._rawData || d.data})) : null;
  return {
    lastUpdated: t('lastUpdated'), gaugeScore: t('gaugeScore'), gaugeLabel: t('gaugeLabel'),
    gaugeDelta: t('gaugeDelta'), signalBadge: t('signalBadge'), vix9dRow: t('vix9dRow'),
    breadthSummary: t('breadthSummary'), miniGauges: t('miniGaugesSection'),
    subRows: [...document.querySelectorAll('#subScores .sub-row')].map(r => ({
      name: r.querySelector('.sub-name').innerText.trim(), val: r.querySelector('.sub-val').innerText.trim()})),
    miniCards: [...document.querySelectorAll('.mini-gauge-card')].map(c => ({
      score: c.querySelector('.mini-gauge-score').innerText.trim(), ret: c.querySelector('.mini-gauge-ret').innerText.trim()})),
    fgGaugeScore: t('fgGaugeScore'), fgGaugeLbl: t('fgGaugeLbl'), tpGaugeScore: t('tpGaugeScore'),
    tpGaugeLbl: t('tpGaugeLbl'), tpDivVal: t('tpDivVal'), tpDivBadge: t('tpDivBadge'),
    tpDivZscore: t('tpDivZscore'), tpCVXN: t('tpCVXN'), tpCQQQ: t('tpCQQQ'), tpCZscore: t('tpCZscore'),
    tpChecklist: t('tpChecklistInner'), buyChecklist: t('buyChecklistInner'),
    afTiles: [...document.querySelectorAll('.af-u-tile .af-cell-pct')].map(e => e.innerText.trim()),
    afArrows: [...document.querySelectorAll('.af-u-tile')].map(e => { const a = e.querySelector('.af-arrow'); return a ? a.innerText.trim() : null; }),
    afPctColors: [...document.querySelectorAll('.af-u-tile .af-cell-pct')].map(e => e.style.color),
    afCells: [...document.querySelectorAll('.af-u-cell')].map(e => e.innerText.trim()).filter(x => x !== ''),
    afJudge: document.querySelector('.af-flow-judge') ? document.querySelector('.af-flow-judge').innerText.trim() : null,
    phasePills: [...document.querySelectorAll('.phase-pill')].map(e => e.innerText.trim()),
    tlCount: t('tlCount'),
    tlFirst: document.querySelector('#timeline .tl-item') ? document.querySelector('#timeline .tl-item').innerText.trim() : null,
    metricCards: [...document.querySelectorAll('#metricsRow .metric-card')].map(c => c.innerText.trim()),
    detail: t('detailInner'),
    modalRows: [...document.querySelectorAll('#infoModal table')[0].querySelectorAll('tr')].slice(1)
      .map(r => [...r.querySelectorAll('td')].map(td => td.innerText.trim())),
    osc: ds(typeof oscChartInst !== 'undefined' ? oscChartInst : null),
    tpChart: ds(typeof tpChartInst !== 'undefined' ? tpChartInst : null),
    mainChart: ds(typeof chartInst !== 'undefined' ? chartInst : null),
  };
}
"""


def _res(results: list, cls, element: str, expected: Any, actual: Any, passed: Optional[bool],
         layer: str = "描画", note: str = "") -> None:
    results.append(cls(element=element, expected=expected, actual=actual, passed=passed,
                       note=(f"[{layer}] " + note) if note or layer else note))


def run_market_pulse_element_checks(page, results: list, cls, now: Optional[datetime] = None) -> None:
    """Market Pulseページ（読み込み済みのpage）の全29要素を確認してresultsに追加する。"""
    now = now or datetime.now(JST)
    entries = load_entries()
    filtered = filter_days(entries, 30, now)          # PERIODS既定30日
    osc = filter_days(entries, 30, now)               # oscDays既定30日（allData基準）
    L = entries[-1]
    s = L["sentiment"]
    b = s.get("breadth") or {}
    tp = L.get("tech_pulse") or {}
    fg = (L.get("fear_greed") or {}).get("score")
    dom = page.evaluate(DOM_SNAPSHOT_JS)

    # MP-01 更新日時
    exp = "更新 " + datetime.fromisoformat(L["date"]).astimezone(JST).strftime("%Y/%m/%d %H:%M") + " JST"
    _res(results, cls, "MP-01 更新日時（#lastUpdated）", exp, dom["lastUpdated"], dom["lastUpdated"] == exp)

    # MP-02 センチメントスコア
    exp = js_fixed(s["score"], 0)
    _res(results, cls, "MP-02 センチメントスコア（#gaugeScore）", exp, dom["gaugeScore"], dom["gaugeScore"] == exp)

    # MP-03 センチメントラベル
    _res(results, cls, "MP-03 センチメントラベル（#gaugeLabel）", s["label"], dom["gaugeLabel"],
         dom["gaugeLabel"] == s["label"])

    # MP-04 前回比
    prev = filtered[-2]["sentiment"]["score"] if len(filtered) >= 2 else None
    if prev is None:
        _res(results, cls, "MP-04 前回比（#gaugeDelta）", None, dom["gaugeDelta"], None, note="比較対象なし")
    else:
        d = s["score"] - prev
        exp = ("▲ " if d >= 0 else "▼ ") + ("+" if d > 0 else "") + js_fixed(d, 1) + "pt"
        _res(results, cls, "MP-04 前回比（#gaugeDelta）", exp, dom["gaugeDelta"], dom["gaugeDelta"] == exp)

    # MP-05 シグナルバッジ
    exp = expected_signal(filtered)
    _res(results, cls, "MP-05 シグナルバッジ（#signalBadge）", exp, dom["signalBadge"], dom["signalBadge"] == exp)

    # MP-06 スコア構成指標バー
    exp = [{"name": SUB_SCORE_NAMES.get(k, k), "val": f"{js_fixed(v['score'], 0)}({js_round(v['weight'] * 100)}%)"}
           for k, v in s["sub_scores"].items()]
    act = [{"name": r["name"], "val": r["val"].replace(" ", "").replace("\n", "")} for r in dom["subRows"]]
    # innerTextはCSSのtext-transform（uppercase）適用後の文字列を返すため大文字小文字は区別しない
    norm = lambda xs: [{k: v.upper() for k, v in x.items()} for x in xs]
    _res(results, cls, "MP-06 スコア構成指標バー（#subScores、8本）", exp, act, norm(act) == norm(exp))

    # MP-07 VIX短期vs中期
    ind = L.get("indicators") or {}
    r9, v9, vv = ind.get("VIX9D対VIX比"), ind.get("VIX9D（短期VIX）"), ind.get("VIX指数")
    if r9 and v9 and vv:
        state = "短期安定" if r9["contango"] else "⚠ 短期警戒"
        exp = f"{state} / 9D {js_fixed(v9['value'], 1)} {'<' if r9['contango'] else '>'} 30D {js_fixed(vv['value'], 1)}"
        act = dom["vix9dRow"] or ""
        ok = state in act and f"9D {js_fixed(v9['value'], 1)} {'<' if r9['contango'] else '>'} 30D {js_fixed(vv['value'], 1)}" in act
        _res(results, cls, "MP-07 VIX短期vs中期（#vix9dRow）", exp, act.replace("\n", " / "), ok)
    else:
        _res(results, cls, "MP-07 VIX短期vs中期（#vix9dRow）", "(非表示)", dom["vix9dRow"], dom["vix9dRow"] == "")

    # MP-08 市場の広がり
    prev_b = (filtered[-2].get("sentiment") or {}).get("breadth") if len(filtered) >= 2 else None
    mc, prev_mc = b.get("mcclellan_oscillator"), (prev_b or {}).get("mcclellan_oscillator")
    sig = [b.get("ad_ratio_5d") is not None and b["ad_ratio_5d"] < 0.8,
           b.get("nh_nl_diff") is not None and b["nh_nl_diff"] < -50,
           b.get("rsp_spy_divergence_20d_avg") is not None and b["rsp_spy_divergence_20d_avg"] < 0,
           mc is not None and prev_mc is not None and mc < 0 and mc < prev_mc,
           b.get("pct_above_50ma") is not None and b["pct_above_50ma"] < 60]
    nhnl = b.get("nh_nl_diff") or 0
    tokens = [f"▲{b.get('advances')}", f"AD(5d) {b.get('ad_ratio_5d') or '-'}", f"▼{b.get('declines')}",
              f"NH {b.get('new_highs_52w') or 0}", ("+" if nhnl >= 0 else "") + str(nhnl), f"NL {b.get('new_lows_52w') or 0}"]
    if b.get("pct_above_50ma") is not None:
        tokens.append(f">50MA {js_num(b['pct_above_50ma'])}%")
    if b.get("pct_above_200ma") is not None:
        tokens.append(f">200MA {js_num(b['pct_above_200ma'])}%")
    if b.get("rsp_spy_divergence_20d_avg") is not None:
        v = b["rsp_spy_divergence_20d_avg"]
        tokens.append(("+" if v >= 0 else "") + js_fixed(v, 2) + "pt")
    if mc is not None:
        tokens.append(("+" if mc >= 0 else "") + js_fixed(mc, 1))
    badges = {"市場が薄い": sig[0], "新安値優勢": sig[1], "二極化": sig[2]}
    combo = sum(sig) >= 3
    act = (dom["breadthSummary"] or "").upper()  # text-transform: uppercase対策
    missing = [tkn for tkn in tokens if tkn.upper() not in act]
    badge_ng = [k for k, on in badges.items() if (f"⚠ {k}".upper() in act) != on]
    combo_ok = (f"{sum(sig)}/5シグナル点灯" in act) == combo
    _res(results, cls, "MP-08 市場の広がり（#breadthSummary）",
         {"tokens": tokens, "badges": {k: v for k, v in badges.items()}, "combo": f"{sum(sig)}/5"},
         {"欠落トークン": missing, "バッジ不一致": badge_ng, "combo一致": combo_ok},
         not missing and not badge_ng and combo_ok)

    # MP-09 センチメント推移チャート（既定: Score・CNN F&G、30日）
    exp = {"Score": [(d.get("sentiment") or {}).get("score") for d in osc],
           "CNN F&G": [(d.get("fear_greed") or {}).get("score") for d in osc]}
    act = {x["label"]: x["raw"] for x in (dom["osc"] or [])}
    _res(results, cls, "MP-09 センチメント推移チャート（#oscChart）", f"{len(osc)}点×2系列",
         {k: (len(v) if v else None) for k, v in act.items()}, act == exp,
         note="系列値を全点比較")

    # MP-10 センチメント予測ミニゲージ
    exp = expected_mini_gauges(entries, filtered)
    act = dom["miniCards"]
    ok = len(act) == 3 and all(
        a["score"] == str(e["score"]) and (e["ret"] is None and a["ret"] == "データ不足" or
                                            e["ret"] is not None and a["ret"] == f"S&P500 {e['ret']} (n={e['n']})")
        for a, e in zip(act, exp))
    _res(results, cls, "MP-10 センチメント予測ミニゲージ（3枚）", exp, act, ok)

    # MP-11 CNN F&Gゲージ
    if fg is None:
        _res(results, cls, "MP-11 CNN F&Gゲージ（#fgGaugeScore/#fgGaugeLbl）", "—/NO DATA",
             (dom["fgGaugeScore"], dom["fgGaugeLbl"]), dom["fgGaugeScore"] == "—")
    else:
        exp = (js_fixed(fg, 0), fg_zone_label(fg, L["fear_greed"].get("rating")))
        act = (dom["fgGaugeScore"], dom["fgGaugeLbl"])
        _res(results, cls, "MP-11 CNN F&Gゲージ（#fgGaugeScore/#fgGaugeLbl）", exp, act, act == exp,
             note="ラベルはCNNのrating（無ければCNNの区分25/45/55/75）")

    # MP-12 Tech Pulseゲージ
    tps = tp.get("score")
    exp = (str(tps), tp.get("label") or tp_label(tps)) if tps is not None else ("—", "NO DATA")
    act = (dom["tpGaugeScore"], dom["tpGaugeLbl"])
    _res(results, cls, "MP-12 Tech Pulseゲージ（#tpGaugeScore/#tpGaugeLbl）", exp, act, act == exp)

    # MP-13 乖離
    dv = (tp.get("divergence") or {}).get("value")
    if dv is None and tps is not None and fg is not None:
        dv = tps - fg
    if dv is None:
        _res(results, cls, "MP-13 乖離（#tpDivVal/#tpDivBadge）", "—", dom["tpDivVal"], dom["tpDivVal"] == "—")
    else:
        sigtxt = (tp.get("divergence") or {}).get("signal") or ""
        badge = sigtxt if sigtxt else (f"乖離{'+' if dv > 0 else ''}{js_fixed(dv, 0)} 要注目水準" if abs(dv) >= 20 else "")
        exp = (("+" if dv > 0 else "") + js_fixed(dv, 0), badge)
        act = (dom["tpDivVal"], dom["tpDivBadge"])
        _res(results, cls, "MP-13 乖離（#tpDivVal/#tpDivBadge）", exp, act,
             act[0] == exp[0] and act[1].upper() == exp[1].upper(), note="バッジはCSSでuppercase表示")

    # MP-14 乖離Zスコア
    z = (tp.get("divergence") or {}).get("zscore")
    exp = (f"Z: {'+' if z > 0 else ''}{js_fixed(z, 2)}σ", f"{'+' if z > 0 else ''}{js_fixed(z, 2)}σ") if z is not None else ("", "—")
    act = (dom["tpDivZscore"], dom["tpCZscore"])
    _res(results, cls, "MP-14 乖離Zスコア（#tpDivZscore/#tpCZscore）", exp, act, act == exp)

    # MP-15 VXN
    vxn = (tp.get("components") or {}).get("vxn_latest")
    exp = js_fixed(vxn, 1) if vxn is not None else "—"
    _res(results, cls, "MP-15 VXN（#tpCVXN）", exp, dom["tpCVXN"], dom["tpCVXN"] == exp)

    # MP-16 QQQ vs SPY 20日
    q = (tp.get("components") or {}).get("qqq_vs_spy_20d")
    exp = (("+" if q >= 0 else "") + js_fixed(q, 1) + "%") if q is not None else "—"
    _res(results, cls, "MP-16 QQQ vs SPY 20日（#tpCQQQ）", exp, dom["tpCQQQ"], dom["tpCQQQ"] == exp,
         note="描画はデータどおり。データ側の欠落はD-05で判定")

    # MP-17 CNN F&G vs Tech Pulse推移（30日、allData基準）
    exp = {"CNN F&G": [(d.get("fear_greed") or {}).get("score") for d in osc],
           "Tech Pulse": [(d.get("tech_pulse") or {}).get("score") for d in osc]}
    act = {x["label"]: x["raw"] for x in (dom["tpChart"] or [])}
    _res(results, cls, "MP-17 CNN F&G vs Tech Pulse推移（#tpChart）", f"{len(osc)}点×2系列",
         {k: (len(v) if v else None) for k, v in act.items()}, act == exp, note="系列値を全点比較")

    # MP-18 TAKE PROFITチェックリスト
    tpc = L.get("take_profit_checklist")
    if tpc and not tpc.get("triggered"):
        fgd = js_round(tpc["fg_score"]) if tpc.get("fg_score") is not None else (js_round(fg) if fg is not None else None)
        exp = f"F&G < 75のため非発動（現在: {fgd}）"
        _res(results, cls, "MP-18 TAKE PROFITチェックリスト", exp, dom["tpChecklist"], (dom["tpChecklist"] or "").startswith(exp))
    elif tpc:
        exp = [tpc["action"], f"{tpc['points']} / 3"] + [c["label"] for c in tpc.get("checks") or []]
        act = dom["tpChecklist"] or ""
        _res(results, cls, "MP-18 TAKE PROFITチェックリスト", exp, act, all(x in act for x in exp))
    else:
        _res(results, cls, "MP-18 TAKE PROFITチェックリスト", "(データなし)", dom["tpChecklist"], None)

    # MP-19 BUYチェックリスト
    bc = L.get("buy_checklist")
    if bc and not bc.get("triggered"):
        fgd = js_round(bc["fg_score"]) if bc.get("fg_score") is not None else (js_round(fg) if fg is not None else None)
        exp = f"F&G > 25のため非発動（現在: {fgd}）"
        _res(results, cls, "MP-19 BUYチェックリスト", exp, dom["buyChecklist"], (dom["buyChecklist"] or "").startswith(exp))
    elif bc:
        exp = [bc["action"], f"{bc['points']} / 3"]
        act = dom["buyChecklist"] or ""
        _res(results, cls, "MP-19 BUYチェックリスト", exp, act, all(x in act for x in exp))
    else:
        _res(results, cls, "MP-19 BUYチェックリスト", "(データなし)", dom["buyChecklist"], None)

    # MP-20 資金フロー 今日のタイル
    af = L.get("asset_flow") or {}
    exp = []
    for k in AF_KEYS:
        it = af.get(k)
        if not it or it.get("change_pct") is None:
            exp.append("—")
        else:
            exp.append(("+" if it["change_pct"] >= 0 else "") + js_fixed(it["change_pct"], 2) + "%" + ("※" if it.get("is_fallback") else ""))
    _res(results, cls, "MP-20 資金フロー 今日のタイル（7資産）", exp, dom["afTiles"], dom["afTiles"] == exp)
    # MP-20b 短期国債タイル: 利回りの上昇＝「売られた」（赤）、低下＝「買われた」（緑）
    sb = af.get("short_bond") or {}
    if sb.get("change_pct") is not None:
        p = sb["change_pct"]
        exp = f"利回り{'▲' if p >= 0 else '▼'}（{'買われた' if bought('short_bond', p) >= 0 else '売られた'}）"
        act = (dom.get("afArrows") or [None, None])[1]
        color_ok = (dom.get("afPctColors") or [None, None])[1] == ("var(--grn)" if bought("short_bond", p) >= 0 else "var(--red)")
        _res(results, cls, "MP-20b 資金フロー 短期国債タイルの向きと色", exp, act, act == exp and color_ok,
             note="色も「買われた/売られた」に一致するか確認")

    # MP-21 資金フロー 直近7日グリッド
    with_af = [d for d in entries if d.get("asset_flow") is not None]
    exp = []
    for d in reversed(with_af[-7:]):
        for k in AF_KEYS:
            it = (d.get("asset_flow") or {}).get(k) or {}
            p = it.get("change_pct")
            exp.append("—" if p is None else pct_text(p) + ("※" if it.get("is_fallback") else ""))
    _res(results, cls, "MP-21 資金フロー 直近7日グリッド", f"{len(exp)}セル", f"{len(dom['afCells'])}セル",
         dom["afCells"] == exp, note="セル値を全件比較（休場行の空セルは除外）")

    # MP-22 資金フロー 5日平均判定
    j, risk, safe, n = expected_asset_flow_judge(entries)
    exp = f"直近{n}日平均: {j} リスク資産 {pct_text(risk)} / 安全資産 {pct_text(safe)}"
    act = re.sub(r"\s+", " ", dom["afJudge"] or "")
    _res(results, cls, "MP-22 資金フロー 5日平均判定", exp, act, act == exp)

    # MP-23 市場フェーズピル
    cnt = {"晴れ": 0, "曇り": 0, "嵐": 0}
    for d in filtered:
        jd = d.get("judgment") or "不明"
        jd = "曇り" if jd == "曇" else "晴れ" if jd == "晴" else jd
        if jd in cnt:
            cnt[jd] += 1
    lab = {"晴れ": "☀ 晴れ", "曇り": "☁ 曇り", "嵐": "⛈ 嵐"}
    exp = [f"{lab[k]} {v}日" for k, v in cnt.items() if v > 0] + [f"計 {len(filtered)} 日"]
    act = [re.sub(r"\s+", " ", x) for x in dom["phasePills"]]
    _res(results, cls, "MP-23 市場フェーズピル（#phasePills）", exp, act, act == exp)

    # MP-24 分析履歴タイムライン（最新行）
    vix = (ind.get("VIX指数") or {}).get("value")
    dv_tl = (tp.get("divergence") or {}).get("value")
    if dv_tl is None and tps is not None and fg is not None:
        dv_tl = tps - fg
    lbl_short = {"EXTREME FEAR": "X-FEAR", "EXTREME GREED": "X-GREED"}.get(s["label"], s["label"])
    exp = [datetime.fromisoformat(L["date"]).astimezone(JST).strftime("%Y/%m/%d"), js_fixed(s["score"], 0), lbl_short,
           f"F&G {js_fixed(fg, 0)}" if fg is not None else None, f"VIX {js_fixed(vix, 1)}" if vix is not None else "VIX -",
           f"TECH {tps}" if tps is not None else "TECH —", f"乖離 {('+' if dv_tl > 0 else '') + js_fixed(dv_tl, 0)}" if dv_tl is not None else "乖離 —"]
    exp = [x for x in exp if x]
    act = dom["tlFirst"] or ""
    miss = [x for x in exp if x not in act]
    _res(results, cls, "MP-24 分析履歴タイムライン（最新行）", exp, {"欠落": miss, "件数表示": dom["tlCount"]},
         not miss and dom["tlCount"] == f"{len(filtered)} 件")

    # MP-25 指標6カード
    exp = []
    for key, short in CARD_DEFS:
        it = ind.get(key)
        if not it or it.get("value") is None:
            exp.append(f"{short}\n—")
            continue
        v = it["value"]
        val = f"{js_round(v):,}" if v >= 1000 else js_fixed(v, 2)
        chg = it.get("change_percent")
        ctext = "-" if chg is None else ("+" if chg >= 0 else "") + js_fixed(chg, 2) + "%"
        exp.append(f"{short}\n{val}{'※' if it.get('is_fallback') else ''}\n{ctext}")
    _res(results, cls, "MP-25 指標6カード（#metricsRow）", exp, dom["metricCards"], dom["metricCards"] == exp,
         note="1000以上はtoLocaleString(ja-JP, 小数0桁)")

    # MP-26 推移チャート（既定: VIX・S&P500、filteredData）
    exp = {"VIX": [((d.get("indicators") or {}).get("VIX指数") or {}).get("value") for d in filtered],
           "S&P500": [((d.get("indicators") or {}).get("S&P500") or {}).get("value") for d in filtered]}
    act = {x["label"]: x["raw"] for x in (dom["mainChart"] or [])}
    _res(results, cls, "MP-26 推移チャート（#mainChart）", f"{len(filtered)}点×2系列",
         {k: (len(v) if v else None) for k, v in act.items()}, act == exp, note="正規化前の生値系列を全点比較")

    # MP-27 詳細カード: クレジット・Risk-Offスコア
    cr = L.get("credit") or {}
    ro = cr.get("risk_off_score")
    if ro is not None:
        zone = "on" if ro <= 33 else "caution" if ro <= 66 else "off"
        conf = js_round((34 - ro) / 34 * 100) if zone == "on" else js_round((1 - abs(ro - 50) / 17) * 100) if zone == "caution" else js_round((ro - 66) / 34 * 100)
        conf = max(0, min(100, conf))
        cl = "高" if conf >= 70 else "中" if conf >= 40 else "低"
        exp = [f"株 {cr.get('stock')}", f"債券 {cr.get('bond')}", f"クレジット {cr.get('credit')}",
               {"on": "RISK ON", "caution": "RISK CAUTION", "off": "RISK OFF"}[zone], f"{ro}", f"確信度 {cl} {conf}%"]
        act = dom["detail"] or ""
        miss = [x for x in exp if x not in act]
        _res(results, cls, "MP-27 詳細カード クレジット判定・Risk-Offスコア", exp, {"欠落": miss}, not miss)
    else:
        _res(results, cls, "MP-27 詳細カード クレジット判定・Risk-Offスコア", "(スコアなし)", None, None)

    # MP-28 詳細カード: AI分析本文
    body = re.sub(r"<[^>]+>", "", L.get("summary") or "")
    head = re.sub(r"\s+", " ", body)[:60].strip()
    act = re.sub(r"\s+", " ", dom["detail"] or "")
    _res(results, cls, "MP-28 詳細カード AI分析本文（summary）", head, "(先頭一致)" if head in act else act[:80], head in act)

    # MP-29 計算式モーダルの重み表と実際の重み
    modal = {}
    for row in dom["modalRows"]:
        if len(row) >= 2:
            modal[row[0]] = row[1]
    actual_w = {k: round(v["weight"] * 100, 1) for k, v in s["sub_scores"].items()}
    modal_w = {}
    for name, key in MODAL_ROW_KEYS.items():
        m = re.match(r"([\d.]+)%", modal.get(name, ""))
        modal_w[key] = float(m.group(1)) if m else None
    ok = set(modal_w) == set(actual_w) and all(
        modal_w[k] is not None and abs(modal_w[k] - actual_w[k]) < 0.05 for k in actual_w)
    _res(results, cls, "MP-29 計算式モーダル（#infoModal 重み表）",
         f"実際の重み（%）: {actual_w}", f"モーダル記載: {modal_w}", ok,
         layer="描画（静的説明）", note="行名で対応づけ、8指標の重みを比較")


# ─────────────────────────────────────────────────────────────────
#  導出層・データ層: 生データからの独立再計算
# ─────────────────────────────────────────────────────────────────

def _real_rows(symbol: str, days: int = 10) -> list:
    from common.market_data.reader import get_price_series
    return [r for r in get_price_series(symbol, days=days) if not r.get("_gap") and r.get("close") is not None]


def run_derivation_checks(results: list, cls) -> None:
    entries = load_entries()
    L = entries[-1]
    s = L["sentiment"]

    # D-01 センチメントスコア = Σ(sub.score×weight)/Σweight
    subs = s["sub_scores"]
    rec = sum(v["score"] * v["weight"] for v in subs.values()) / sum(v["weight"] for v in subs.values())
    _res(results, cls, "D-01 センチメントスコアの再計算（sub_scoresの加重平均）", round(rec, 1), s["score"],
         abs(rec - s["score"]) <= 0.15, layer="導出", note="sub_scoreは0.1刻みに丸めて保存されるため±0.15を許容")

    # D-02 指標の前日比（daily/の同じ日付の終値から再計算）
    from common.market_data.reader import get_price_series
    ng = []
    for name, sym in INDICATOR_SYMBOLS.items():
        it = (L.get("indicators") or {}).get(name)
        if not isinstance(it, dict) or it.get("date") is None or it.get("change_percent") is None:
            continue
        rows = [r for r in get_price_series(sym, days=15) if not r.get("_gap") and r.get("close") is not None
                and r["date"] <= it["date"]]
        if len(rows) < 2 or rows[-1]["date"] != it["date"]:
            ng.append(f"{name}: {it['date']}の終値がdaily/に無い")
            continue
        chg = (rows[-1]["close"] - rows[-2]["close"]) / rows[-2]["close"] * 100
        if abs(round(chg, 2) - it["change_percent"]) > 0.011:
            ng.append(f"{name}: 記録{it['change_percent']} vs 再計算{round(chg, 2)}")
    _res(results, cls, "D-02 指標の前日比（indicators、daily/から再計算）", "全指標一致", ng or "全指標一致",
         not ng, layer="導出")

    # D-03 資産フロー騰落率（daily/から再計算）
    ng = []
    for k, sym in AF_SYMBOLS.items():
        it = (L.get("asset_flow") or {}).get(k) or {}
        if it.get("date") is None:
            continue
        rows = [r for r in get_price_series(sym, days=15) if not r.get("_gap") and r.get("close") is not None
                and r["date"] <= it["date"]]
        if len(rows) < 2 or rows[-1]["date"] != it["date"]:
            ng.append(f"{k}: {it['date']}の終値がdaily/に無い")
            continue
        chg = (rows[-1]["close"] - rows[-2]["close"]) / rows[-2]["close"] * 100
        if abs(round(chg, 3) - it["change_pct"]) > 0.0015:
            ng.append(f"{k}: 記録{it['change_pct']} vs 再計算{round(chg, 3)}")
    _res(results, cls, "D-03 資産フロー騰落率（asset_flow、daily/から再計算）", "全資産一致", ng or "全資産一致",
         not ng, layer="導出")

    # D-04 ブレッス: 基準日の終値と前営業日の終値がそろう銘柄だけで集計されているか
    # （2026-09-26、MARKETPULSE-BREADTH-MIXED-DATES-1修正後の仕様。ブレッスの算出後に
    # daily/が更新されていると件数がずれるため、daily/を読んだ時点の再計算と比べる）
    b = s.get("breadth") or {}
    bd = b.get("date")
    import pandas_market_calendars as _mcal
    _days = _mcal.get_calendar("NYSE").valid_days(start_date=(date.fromisoformat(bd) - timedelta(days=15)).isoformat(),
                                                  end_date=bd)
    prev_day = _days[-2].strftime("%Y-%m-%d")
    with open(SP500_TICKERS_JSON, encoding="utf-8") as f:
        tk = json.load(f)
    tk = tk if isinstance(tk, list) else tk.get("tickers", [])
    same = other = 0
    for t in tk:
        rows = _real_rows(t, days=6)
        if not rows:
            continue
        if len(rows) >= 2 and rows[-1]["date"] == bd and rows[-2]["date"] == prev_day:
            same += 1
        else:
            other += 1
    exp = {"total_stocks": same, "stocks_excluded_date_mismatch": other}
    act = {"total_stocks": b.get("total_stocks"), "stocks_excluded_date_mismatch": b.get("stocks_excluded_date_mismatch")}
    _res(results, cls, f"D-04 ブレッスの基準日（{bd}と前営業日{prev_day}の終値がそろう銘柄だけで集計）",
         exp, act, act == exp, layer="データ",
         note="compute_breadth()は基準日と前営業日の終値がそろう銘柄だけを集計し、除外数を記録する")

    # D-05 Tech Pulseの構成要素（QQQ系2要素・VXN）と乖離
    c = (L.get("tech_pulse") or {}).get("components") or {}
    missing = [k for k in ("qqq_vs_ma125", "qqq_vs_spy_20d", "vxn_vs_ma50") if c.get(k) is None]
    tp = L.get("tech_pulse") or {}
    fg = (L.get("fear_greed") or {}).get("score")
    div_ok = tp.get("score") is None or fg is None or abs((tp["score"] - fg) - (tp.get("divergence") or {}).get("value", 0)) < 0.05
    _res(results, cls, "D-05 Tech Pulseの構成要素（QQQ vs MA125・QQQ vs SPY・VXN vs MA50）と乖離",
         "3要素すべて算出・乖離=TP−CNN F&G", {"欠落要素": missing, "乖離一致": div_ok},
         not missing and div_ok, layer="データ",
         note="QQQは2026-08-25のdaily/欠損行によりMA125が算出不能（get_ma_deviation()は窓内に欠損があるとNone）")


# ─────────────────────────────────────────────────────────────────
#  データ基準日の分類（STEP 2）
# ─────────────────────────────────────────────────────────────────

def expected_close_date(entry_iso: str) -> date:
    """エントリ生成時刻（JST）から、その時点で確定している直近の米国終値日を返す
    （夏時間20:00 UTC・冬時間21:00 UTCの終値確定後に生成されている前提。
    週末は直前の金曜。米国祝日は考慮しない〈呼び出し側で注記する〉）。"""
    t = datetime.fromisoformat(entry_iso).astimezone(timezone.utc)
    close_h = 20  # 夏時間（3月第2日曜〜11月第1日曜）。本スクリプトの対象期間は夏時間
    d = t.date() if t.hour >= close_h else t.date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def classify_base_dates(entry: Optional[dict] = None) -> list:
    """最新エントリ（または指定エントリ）の要素ごとのデータ基準日と分類を返す。"""
    entry = entry or load_entries()[-1]
    exp = expected_close_date(entry["date"])
    ind = entry.get("indicators") or {}
    af = entry.get("asset_flow") or {}
    rows = []

    def add(elem, d, note=""):
        if d is None:
            rows.append((elem, None, "判定不能", note)); return
        dd = date.fromisoformat(d[:10])
        cls = "前営業日の終値基準（正常）" if dd == exp else ("1営業日以上古い" if dd < exp else "取引時間中の値の疑い")
        rows.append((elem, d[:10], cls, note))
    def dt(k):
        return (ind.get(k) or {}).get("date") if isinstance(ind.get(k), dict) else None
    for k in ["S&P500", "NASDAQ", "VIX指数", "VIX9D（短期VIX）", "米10年債", "ドル円", "WTI原油", "金（GOLD）",
              "HYG（ハイイールド債ETF）", "LQD（投資適格債ETF）", "S&P500グロース(IVW)",
              "S&P500バリュー(IVE)", "Russell2000小型(RUT)", "NYSE Composite"]:
        add(f"indicators.{k}", dt(k))
    # HYG対LQD比はHYGの日付をラベルにするが、LQDの終値も使う（古い方が実際の基準日）
    pair = [d for d in (dt("HYG（ハイイールド債ETF）"), dt("LQD（投資適格債ETF）")) if d]
    add("indicators.HYG対LQD比（→sub_scores.hyg_lqd_dir）", min(pair) if pair else None,
        f"ラベルは{dt('HYG対LQD比')}（HYG側）" if pair and min(pair) != dt("HYG対LQD比") else "")
    gv = [d for d in (dt("S&P500グロース(IVW)"), dt("S&P500バリュー(IVE)")) if d]
    add("sub_scores.growth_value（IVW−IVE）", min(gv) if gv else None)
    for k in AF_KEYS:
        add(f"asset_flow.{k}", (af.get(k) or {}).get("date"))
    # ブレッス: ラベルは全銘柄の最大日付。現在のdaily/で、その日付の終値を持つ銘柄数を数える
    bd = ((entry.get("sentiment") or {}).get("breadth") or {}).get("date")
    on = off = 0
    try:
        with open(SP500_TICKERS_JSON, encoding="utf-8") as f:
            tk = json.load(f)
        tk = tk if isinstance(tk, list) else tk.get("tickers", [])
        for t in tk:
            r = _real_rows(t, days=6)
            if r:
                on, off = (on + 1, off) if r[-1]["date"] == bd else (on, off + 1)
    except Exception:
        pass
    if off:
        rows.append(("sentiment.breadth（→AD・NH/NL・Hindenburg）", bd, "混在",
                     f"{bd}の終値{on}銘柄 / 前営業日以前{off}銘柄（現在のdaily/で判定）"))
    else:
        add("sentiment.breadth（→AD・NH/NL・Hindenburg）", bd)
    # FRED（common/macro_data）経由の入力。as_ofはFREDの観測日
    try:
        from common.macro_data.reader import get_series
        start = (datetime.fromisoformat(entry["date"]) - timedelta(days=20)).strftime("%Y-%m-%d")
        for sid, elem in (("VXNCLS", "tech_pulse.vxn（FRED VXNCLS）"),
                          ("BAMLH0A0HYM2", "checklist.hy_spread（FRED BAMLH0A0HYM2）"),
                          ("DGS3MO", "asset_flow.short_bond（FRED DGS3MO）")):
            recs = get_series(sid, start=start)
            add(elem, recs[-1]["as_of"] if recs else None, "FREDの観測日（公表ラグ）")
    except Exception as e:
        rows.append(("FRED系列", None, "判定不能", str(e)[:60]))
    gen_utc = datetime.fromisoformat(entry["date"]).astimezone(timezone.utc)
    rows.append(("fear_greed（CNN、実行時に取得）", exp.isoformat(), "前営業日の終値基準（正常）",
                 f"取得{gen_utc.strftime('%m-%d %H:%M')} UTC＝終値確定後"))
    return [("(期待する終値日)", exp.isoformat(), "", "")] + rows
