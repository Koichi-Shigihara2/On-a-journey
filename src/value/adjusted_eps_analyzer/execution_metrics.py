"""
execution_metrics.py

[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]④（経営者の実行力評価、
2026-09-23実装）: 売上成長加速度・ROICトレンド・SBC比率の3指標を計算する。

設計方針（GROWTH-1の反省を踏まえる）:
- 3指標は単一スコアへ合成しない。個別フィールドとして提示する
  （恣意的な重み付けを避けるため）
- 測定できない銘柄は推測で埋めず、reason付きのNoneとして明示する
  （2026-08-15セッションで確立した既存原則）
- 売上成長加速度は「直近4四半期平均YoY − その前4四半期平均YoY」
  （2階差分ではなく移動平均の差分。単純な2階差分はbase effect
  〈前年がたまたま急成長/急減速だった場合のノイズ〉に弱く、
  経営者の実行力とは無関係な変動を拾ってしまうため不採用）
"""
import os
from typing import Any, Dict, List, Optional

from common.sec_data.roic import calc_roic_wacc_ratio

_ROIC_TREND_YEARS = 4
_ROIC_TREND_MIN_OK_YEARS = 3
_GROWTH_ACCEL_QUARTERS = 4


def _yoy_series(quarters_newest_first: List[Dict[str, Any]], count: int) -> List[Optional[float]]:
    """直近count四半期分のYoY成長率を新しい順で返す（算出不能はNone）"""
    revs = [q.get("revenue") for q in quarters_newest_first]
    out = []
    for i in range(count):
        if i + _GROWTH_ACCEL_QUARTERS >= len(revs):
            out.append(None)
            continue
        cur, base = revs[i], revs[i + _GROWTH_ACCEL_QUARTERS]
        if cur is None or base in (None, 0):
            out.append(None)
            continue
        out.append((cur - base) / base)
    return out


def calc_revenue_growth_acceleration(quarters_newest_first: List[Dict[str, Any]]) -> Dict[str, Any]:
    """直近4四半期平均YoY − その前4四半期平均YoY を計算する

    12四半期分の売上（直近4Q分のYoYを出すための8Q + その前4Q分の
    YoYを出すための追加4Q = 直近16四半期分のrevenueが必要）が揃わない
    銘柄はNoneとして明示する（推測で埋めない）。
    """
    recent4 = _yoy_series(quarters_newest_first, _GROWTH_ACCEL_QUARTERS)
    prior4 = _yoy_series(quarters_newest_first[_GROWTH_ACCEL_QUARTERS:], _GROWTH_ACCEL_QUARTERS)

    if any(v is None for v in recent4) or any(v is None for v in prior4):
        return {"value": None, "reason": "insufficient_data", "method": "avg4q_yoy_diff"}

    recent_avg = sum(recent4) / len(recent4)
    prior_avg = sum(prior4) / len(prior4)
    return {
        "value": recent_avg - prior_avg,
        "reason": "ok",
        "method": "avg4q_yoy_diff",
        "recent_4q_avg_yoy": recent_avg,
        "prior_4q_avg_yoy": prior_avg,
    }


def calc_roic_trend(ticker: str, repo_root: str) -> Dict[str, Any]:
    """直近4年分のROIC/WACC比率を、TANUKI VALUATIONと共有のroic.pyで計算する

    3年未満しか算出できない場合はトレンド全体をreason="insufficient_data"
    として明示する（個別年のNoneは各yearエントリのreasonに残す）。
    """
    sec_dir = os.path.join(repo_root, "common", "sec_data", "data", ticker)
    if not os.path.isdir(sec_dir):
        return {"years": [], "reason": "no_sec_dir"}
    years = sorted([
        int(fn[7:11]) for fn in os.listdir(sec_dir)
        if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
    ], reverse=True)
    target_years = years[:_ROIC_TREND_YEARS]

    entries = []
    ok_count = 0
    for y in target_years:
        val, reason = calc_roic_wacc_ratio(ticker, repo_root, year=y)
        entries.append({"year": y, "roic_wacc_ratio": val, "reason": reason})
        if reason == "ok":
            ok_count += 1

    overall_reason = "ok" if ok_count >= _ROIC_TREND_MIN_OK_YEARS else "insufficient_data"
    return {"years": entries, "reason": overall_reason}


def calc_sbc_ratio(quarters_newest_first: List[Dict[str, Any]]) -> Dict[str, Any]:
    """直近四半期のSBC(adjustmentsのitem_id=="sbc") / revenue を計算する"""
    if not quarters_newest_first:
        return {"value": None, "reason": "no_data", "period_end": None}
    latest = quarters_newest_first[0]
    revenue = latest.get("revenue")
    if not revenue:
        return {"value": None, "reason": "no_revenue", "period_end": latest.get("period_end")}
    sbc_amount = None
    for adj in latest.get("adjustments", []):
        if adj.get("item_id") == "sbc":
            sbc_amount = adj.get("amount")
            break
    if sbc_amount is None:
        return {"value": None, "reason": "no_sbc_data", "period_end": latest.get("period_end")}
    return {
        "value": sbc_amount / revenue,
        "reason": "ok",
        "period_end": latest.get("period_end"),
    }


def calc_execution_metrics(ticker: str, quarterly_results: List[Dict[str, Any]], repo_root: str) -> Dict[str, Any]:
    """3指標をまとめて計算する。quarterly_resultsは新しい順（filing_date降順）を想定。"""
    quarters_newest_first = sorted(
        quarterly_results, key=lambda q: q.get("filing_date", ""), reverse=True,
    )
    return {
        "revenue_growth_acceleration": calc_revenue_growth_acceleration(quarters_newest_first),
        "roic_trend": calc_roic_trend(ticker, repo_root),
        "sbc_ratio": calc_sbc_ratio(quarters_newest_first),
    }
