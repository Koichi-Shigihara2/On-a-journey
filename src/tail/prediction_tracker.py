#!/usr/bin/env python3
"""
TANUKI TAIL Prediction Tracker
stage2 kpi_forecasts（1年後予測）と layer2 KPI実績を照合して乖離率を記録する。

出力: docs/portfolio/tail/data/prediction_history.json

使用方法:
  python src/tail/prediction_tracker.py --ticker SOFI PLTR TSLA
  python src/tail/prediction_tracker.py           # 全ティッカー自動検出
"""

import os
import sys
import json
import glob
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

_THIS_DIR  = os.path.dirname(os.path.abspath(__file__))
_SRC_DIR   = os.path.dirname(_THIS_DIR)
_REPO_ROOT = os.path.dirname(_SRC_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.tail import review_status  # noqa: E402

REVIEWS_DIR  = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "reviews")
KPI_DIR      = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "kpi")
OUTPUT_PATH  = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "prediction_history.json")

JST = timezone(timedelta(hours=9))


def _add_quarters(q: str, n: int) -> str:
    """'2025Q1' + 4 → '2026Q1'"""
    year, qnum = int(q[:4]), int(q[5])
    qnum += n
    year += (qnum - 1) // 4
    qnum = (qnum - 1) % 4 + 1
    return f"{year}Q{qnum}"


def _load_layer2_actuals(ticker: str) -> Dict[str, Dict[str, Any]]:
    """layer2.json から {kpi_name: {quarter: value}} マッピングを返す"""
    path = os.path.join(KPI_DIR, f"{ticker}_layer2.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        l2 = json.load(f)
    result: Dict[str, Dict[str, Any]] = {}
    for kpi_name, entry in l2.get("kpis", {}).items():
        result[kpi_name] = {d["quarter"]: d["value"] for d in entry.get("data", [])}
    return result


def _classify_deviation(dev_pct: float) -> str:
    if abs(dev_pct) <= 10.0:
        return "accurate"
    elif dev_pct > 10.0:
        return "under_estimated"
    else:
        return "over_estimated"


def _load_reviews_for_ticker(ticker: str, queue: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """ticker の全レビューを四半期昇順で返す。

    キューでsuperseded・skipped_oldの期のレビュー（[[TAIL-REVIEW-LATEST-MISMATCH-1]]）と、後から作った
    レビュー（backfilled、[[TAIL-REVIEW-PERIOD-FILTER-1]]）は外す。判定はreview_status.is_comparable()。
    """
    return review_status.comparable_reviews(REVIEWS_DIR, ticker, queue)


def track_ticker(ticker: str) -> List[Dict[str, Any]]:
    """
    ticker の全レビューを走査して予測vs実績照合リストを返す。
    kpi_forecastsがない、または実績データがない四半期はスキップ。
    """
    reviews = _load_reviews_for_ticker(ticker)
    actuals = _load_layer2_actuals(ticker)
    entries: List[Dict[str, Any]] = []

    for rv in reviews:
        review_quarter = rv.get("quarter", "")
        if not review_quarter:
            continue

        s2 = rv.get("stage2", {})
        sc = s2.get("scenarios", {})

        for scenario in ("base", "bear", "bull"):
            sc_data = sc.get(scenario, {})
            kpi_forecasts = sc_data.get("kpi_forecasts", {})
            forecast_1y = kpi_forecasts.get("1年後")
            forecast_3y = kpi_forecasts.get("3年後")

            has_forecast = bool(forecast_1y)

            if not has_forecast:
                if scenario == "base":
                    # base のみ記録（kpi_forecast_available=false）
                    entries.append({
                        "review_quarter": review_quarter,
                        "forecast_target": _add_quarters(review_quarter, 4),
                        "scenario": scenario,
                        "predictions": {},
                        "kpi_forecast_available": False,
                    })
                continue

            # 1年後照合
            target_1y = _add_quarters(review_quarter, 4)
            predictions_1y: Dict[str, Any] = {}

            for kpi_name, predicted_val in (forecast_1y or {}).items():
                if kpi_name not in actuals:
                    continue  # layer2にないKPI（SBC, EPSなど）はスキップ
                actual_val = actuals[kpi_name].get(target_1y)
                if actual_val is None:
                    continue  # 実績データなし（将来四半期）

                if predicted_val and abs(predicted_val) > 0:
                    dev_pct = round((actual_val - predicted_val) / abs(predicted_val) * 100, 1)
                    accuracy = _classify_deviation(dev_pct)
                else:
                    dev_pct = None
                    accuracy = "unknown"

                predictions_1y[kpi_name] = {
                    "predicted": predicted_val,
                    "actual": actual_val,
                    "deviation_pct": dev_pct,
                    "accuracy": accuracy,
                }

            entries.append({
                "review_quarter": review_quarter,
                "forecast_target": target_1y,
                "scenario": scenario,
                "predictions": predictions_1y,
                "kpi_forecast_available": True,
                "matchable": len(predictions_1y) > 0,
            })

    return entries


def build_prediction_history(tickers: List[str]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for ticker in tickers:
        entries = track_ticker(ticker)
        result[ticker] = entries
        matchable = sum(1 for e in entries if e.get("matchable"))
        print(f"  {ticker}: {len(entries)}件処理, {matchable}件照合可能")
    return result


def load_existing() -> Dict[str, Any]:
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def detect_tickers() -> List[str]:
    files = glob.glob(os.path.join(REVIEWS_DIR, "*_review.json"))
    tickers = sorted({os.path.basename(f).split("_")[0] for f in files})
    return tickers


def format_for_prompt(ticker: str, history: Dict[str, Any], max_entries: int = 2) -> str:
    """Stage1プロンプトに埋め込む予測振り返りテキストを生成する"""
    entries = history.get(ticker, [])
    matchable = [
        e for e in entries
        if e.get("kpi_forecast_available") and e.get("matchable") and e.get("scenario") == "base"
    ]
    if not matchable:
        return ""

    recent = matchable[-max_entries:]
    lines = []
    for entry in recent:
        rq = entry["review_quarter"]
        tq = entry["forecast_target"]
        preds = entry.get("predictions", {})
        if not preds:
            continue
        lines.append(f"【{rq}レビュー → {tq}実績】")
        lines.append("| KPI | 予測値 | 実績値 | 乖離率 |")
        lines.append("|-----|--------|--------|--------|")
        for kpi_name, p in preds.items():
            pred = p["predicted"]
            actual = p["actual"]
            dev = p["deviation_pct"]

            def fmt(v: Any) -> str:
                if v is None:
                    return "N/A"
                if isinstance(v, float) and abs(v) <= 10:
                    return f"{v*100:.1f}%"
                if abs(v) >= 1_000_000_000:
                    return f"${v/1e9:.2f}B"
                if abs(v) >= 1_000_000:
                    return f"${v/1e6:.0f}M"
                return str(v)

            dev_str = f"{dev:+.1f}%" if dev is not None else "N/A"
            lines.append(f"| {kpi_name} | {fmt(pred)} | {fmt(actual)} | {dev_str} |")
        lines.append("")

    if not lines:
        return ""

    body = "\n".join(lines)
    return (
        f"## 過去予測の振り返り\n"
        f"{body}\n"
        f"過去の予測精度を踏まえて今回の評価の楽観バイアスを調整してください。"
        f"特に過去に過大評価したKPIには追加の保守的調整を加えてください。\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TANUKI TAIL Prediction Tracker — kpi_forecasts vs 実績照合"
    )
    parser.add_argument("--ticker", nargs="+", help="対象ティッカー（複数可）")
    args = parser.parse_args()

    now_jst = datetime.now(JST)
    print(f"TANUKI TAIL Prediction Tracker — {now_jst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 60)

    tickers = args.ticker if args.ticker else detect_tickers()
    print(f"対象: {tickers}")
    print()

    existing = load_existing()
    history = build_prediction_history(tickers)

    # 既存データとマージ（対象外ティッカーは保持）
    merged = dict(existing)
    merged.update(history)
    merged["_updated_at"] = now_jst.isoformat()

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"\n✓ 保存: {OUTPUT_PATH}")

    # 照合可能ケースの詳細出力
    print("\n── 照合可能ケース詳細 ──")
    for ticker in tickers:
        for entry in merged.get(ticker, []):
            if not entry.get("matchable"):
                continue
            if entry.get("scenario") != "base":
                continue
            rq = entry["review_quarter"]
            tq = entry["forecast_target"]
            preds = entry.get("predictions", {})
            print(f"\n  {ticker} {rq}→{tq}:")
            for kpi_name, p in preds.items():
                dev = p["deviation_pct"]
                acc = p["accuracy"]
                dev_str = f"{dev:+.1f}%" if dev is not None else "N/A"
                print(f"    {kpi_name}: 予測={p['predicted']:,} / 実績={p['actual']:,} / 乖離={dev_str} ({acc})")


if __name__ == "__main__":
    main()
