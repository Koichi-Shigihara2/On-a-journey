#!/usr/bin/env python3
"""
TANUKI TAIL DCF Bridge
Grok stage2シナリオ（売上成長率・営業利益率）→ 将来理論価格計算

入力:
  - docs/value-monitor/tanuki_valuation/data/{ticker}/latest.json
  - docs/portfolio/tail/data/reviews/{ticker}_*_review.json (is_latest=true)

出力:
  - docs/portfolio/scenario/{ticker}/bear.json
  - docs/portfolio/scenario/{ticker}/base.json
  - docs/portfolio/scenario/{ticker}/bull.json

使用方法:
  python src/tail/tail_dcf_bridge.py --ticker PLTR SOFI TSLA
  python src/tail/tail_dcf_bridge.py           # VALUATION_DIR配下の全ティッカー
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

# calculate_future_values を tanuki_valuation から直接 import
_THIS_DIR  = os.path.dirname(os.path.abspath(__file__))   # src/tail
_SRC_DIR   = os.path.dirname(_THIS_DIR)                    # src
_REPO_ROOT = os.path.dirname(_SRC_DIR)                     # repo root
_CALC_DIR  = os.path.join(_SRC_DIR, "value", "tanuki_valuation", "calculator")
if _CALC_DIR not in sys.path:
    sys.path.insert(0, _CALC_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from future_values import calculate_future_values          # noqa: E402
from common.sec_data import tickers as _tickers_mod         # noqa: E402
# [フェーズD Step2-3対応、2026-08-07] SEC EDGAR Layer3
# （common/sec_data/layer3_builder.py、company_facts.json由来の統合
# スキーマ）経由でnormalized/参照を廃止した。本ファイル内の
# layer1/layer2（_load_layer1_financials/_load_layer2_kpi、KPI取得元
# 区分）とは完全に別概念のため、SEC側を指す場合は必ず「SEC EDGAR
# Layer3」と明示して区別する。
from common.sec_data.layer3_builder import (        # noqa: E402
    build_ticker_store, get_latest_quarterly,
    WEIGHTED_AVG_DILUTED_SHARES_TAG,
)

# ── パス定数 ──────────────────────────────────────────────────────
REVIEWS_DIR   = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "reviews")
VALUATION_DIR = os.path.join(_REPO_ROOT, "docs", "value-monitor", "tanuki_valuation", "data")
SCENARIO_DIR  = os.path.join(_REPO_ROOT, "docs", "portfolio", "scenario")
POSITIONS_DIR         = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "positions")
KPI_DIR               = os.path.join(_REPO_ROOT, "docs", "portfolio", "tail", "data", "kpi")

JST = timezone(timedelta(hours=9))

# PascalCase（本ファイル内の既存呼び出し表記）→ SEC EDGAR Layer3の
# snake_caseフィールド名の対応表（フェーズD Step2-3対応）。
_SEC_LAYER3_FIELD_MAP = {
    "Revenue": "revenue",
    "OperatingIncome": "operating_income",
    "SBC": "stock_based_compensation",
    "NetIncome": "net_income",
    "SharesDiluted": "shares_diluted",
}


def _load_latest_valuation(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(VALUATION_DIR, ticker, "latest.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_latest_review(ticker: str) -> Optional[Dict[str, Any]]:
    """is_latest=True のレビュー、なければファイル名で最新を返す"""
    if not os.path.exists(REVIEWS_DIR):
        return None
    files = sorted(
        [f for f in os.listdir(REVIEWS_DIR)
         if f.startswith(f"{ticker}_") and f.endswith("_review.json")],
        reverse=True,
    )
    if not files:
        return None
    for fname in files:
        with open(os.path.join(REVIEWS_DIR, fname), encoding="utf-8") as f:
            rv = json.load(f)
        if rv.get("is_latest"):
            return rv
    with open(os.path.join(REVIEWS_DIR, files[0]), encoding="utf-8") as f:
        return json.load(f)


def _load_thesis(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(POSITIONS_DIR, f"{ticker}_thesis.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_layer2_kpi(ticker: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(KPI_DIR, f"{ticker}_layer2.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_current_kpi_values(
    thesis: Optional[Dict[str, Any]],
    layer2: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """layer2.kpis の全KPI → 最新値のマッピングを返す（layer2_name をキーに）"""
    if not layer2:
        return {}
    result: Dict[str, Any] = {}
    for l2_name, entry in layer2.get("kpis", {}).items():
        data = entry.get("data", [])
        if data:
            result[l2_name] = data[0].get("value")
    return result


def _load_layer1_financials(ticker: str) -> Dict[str, Any]:
    """SEC EDGAR Layer3（company_facts.json由来の統合スキーマ）四半期
    データ + latest.json から財務指標（TANUKI TAIL側の「layer1」区分、
    ＝財務諸表由来のKPI）を取得する。

    ※ 戻り値の意味上の区分名「layer1」（財務諸表由来）は、本関数が参照
    するデータソース「SEC EDGAR Layer3」（layer3_builder.py、統合
    スキーマ）とは無関係の別軸の用語。混同しないこと。
    """
    result: Dict[str, Any] = {}
    store = build_ticker_store(ticker)
    if store is not None:
        rev = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["Revenue"])
        oi  = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["OperatingIncome"])
        sbc = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["SBC"])
        ni  = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["NetIncome"])
        # [[TAIL-SHARESDILUTED-Q4-TIMING-RISK-1]]対応: shares_dilutedは
        # CommonStockSharesOutstanding（期末発行済株式数、BS概念）を
        # フォールバックタグとして含むため、source_tagで加重平均
        # （PL概念）由来のみに絞り込む（pipeline.py::[[LAYER3-
        # SHARESDILUTED-TAG-GAP-1]]と同一条件、common/sec_data/
        # layer3_builder.pyの共通定数を参照）。
        sd  = get_latest_quarterly(store, _SEC_LAYER3_FIELD_MAP["SharesDiluted"],
                                    source_tag=WEIGHTED_AVG_DILUTED_SHARES_TAG)

        if rev and oi and rev.get("val"):
            result["operating_margin"] = round(oi["val"] / rev["val"], 4)
        if sbc:
            result["sbc_quarterly"] = sbc["val"]
        if ni and sd and sd.get("val"):
            result["eps_diluted"] = round(ni["val"] / sd["val"], 4)

    latest_path = os.path.join(VALUATION_DIR, ticker, "latest.json")
    if os.path.exists(latest_path):
        with open(latest_path, encoding="utf-8") as f:
            lat = json.load(f)
        fwd_eps = (lat.get("components") or {}).get("forward_eps")
        if fwd_eps:
            result["eps_forward"] = fwd_eps

    return result


def _fcf_margin(valuation: Dict[str, Any]) -> float:
    """直近実績FCFマージン率（比率）。マイナス or 未取得の場合は 0.05 で代替"""
    history = valuation.get("fcf_history", [])
    if not history:
        return 0.05
    margin = history[-1].get("fcf_margin", 0.0)
    if margin <= 0:
        return 0.05
    return margin / 100.0


def _weighted_growth(y1: float, y2: float, y3: float) -> float:
    """Y1×3 + Y2×2 + Y3×1 の加重平均（近い将来を重く見る）"""
    return (y1 * 3 + y2 * 2 + y3 * 1) / 6


def generate_scenario_files(ticker: str) -> bool:
    """
    ticker の将来理論価格シナリオファイルを生成する。

    Returns:
        True  = 全シナリオ生成成功
        False = スキップ（データ欠損など）
    """
    valuation = _load_latest_valuation(ticker)
    if not valuation:
        print(f"  [WARN] {ticker}: latest.json 未発見 → スキップ")
        print(f"  [SKIP] {ticker}: latest.json 不在のためシナリオ生成対象外")
        return False

    review = _load_latest_review(ticker)
    if not review:
        print(f"  [WARN] {ticker}: レビュー未発見 → スキップ")
        return False

    thesis  = _load_thesis(ticker)
    layer2  = _load_layer2_kpi(ticker)
    layer1  = _load_layer1_financials(ticker)

    # kpi_current: layer2_name キーの現在値 + layer1 財務指標
    l2_current = _build_current_kpi_values(thesis, layer2)
    kpi_current: Dict[str, Any] = dict(l2_current)
    kpi_layer1_keys: List[str] = []
    for l1_src, json_key in [("operating_margin", "営業利益率"),
                               ("sbc_quarterly",    "SBC"),
                               ("eps_diluted",      "希薄化後EPS")]:
        if l1_src in layer1:
            kpi_current[json_key] = layer1[l1_src]
            kpi_layer1_keys.append(json_key)

    # kpi_format: JS側フォーマット（"usd" | "pct" | "usd_small"）
    kpi_format: Dict[str, str] = {}
    for key, val in kpi_current.items():
        if key == "希薄化後EPS":
            kpi_format[key] = "usd_small"
        elif key == "SBC" or (val is not None and abs(val) >= 1e6):
            kpi_format[key] = "usd"
        elif val is not None and abs(val) <= 10:
            kpi_format[key] = "pct"
        else:
            kpi_format[key] = "usd_small"

    scenarios = review.get("stage2", {}).get("scenarios", {})
    if not scenarios:
        print(f"  [WARN] {ticker}: stage2.scenarios 未発見 → スキップ")
        return False

    quarter         = review.get("quarter", "unknown")
    current_iv      = float(valuation.get("intrinsic_value_per_share") or 0)
    current_price   = float((valuation.get("components") or {}).get("current_price") or 0)
    latest_revenue  = float((valuation.get("components") or {}).get("latest_revenue") or 0)
    fcf_adj         = _fcf_margin(valuation)
    now_jst         = datetime.now(JST)

    out_dir = os.path.join(SCENARIO_DIR, ticker)
    os.makedirs(out_dir, exist_ok=True)

    success = True
    for sc_name in ("bear", "base", "bull"):
        sc = scenarios.get(sc_name)
        if not sc:
            print(f"  [WARN] {ticker}: {sc_name} シナリオなし → スキップ")
            success = False
            continue

        y1       = float(sc.get("revenue_growth_y1", 0))
        y2       = float(sc.get("revenue_growth_y2", 0))
        y3       = float(sc.get("revenue_growth_y3", 0))
        terminal = float(sc.get("terminal_growth", 0.03))
        op_margin = float(sc.get("operating_margin_terminal", 0))
        wg       = _weighted_growth(y1, y2, y3)
        kpi_forecasts = sc.get("kpi_forecasts") or {}

        future_vals = calculate_future_values(
            current_value     = current_iv,
            high_growth_rate  = wg,
            high_growth_years = 3,      # Y1-Y3をPhase1として扱う
            terminal_growth   = terminal,
            projection_years  = 5,
        )

        output = {
            "ticker":  ticker,
            "quarter": quarter,
            "scenario": sc_name,
            "assumptions": {
                "Y1_growth":        round(y1, 4),
                "Y2_growth":        round(y2, 4),
                "Y3_growth":        round(y3, 4),
                "terminal_growth":  terminal,
                "operating_margin": op_margin,
                "weighted_growth":  round(wg, 4),
                "fcf_adjustment":   round(fcf_adj, 4),
                "base_revenue":     latest_revenue,
            },
            "base_intrinsic_value": round(current_iv, 2),
            "current_price":        round(current_price, 2),
            "future_values":        future_vals,
            "kpi_forecasts":        kpi_forecasts,
            "kpi_current":          kpi_current,
            "kpi_layer1_keys":      kpi_layer1_keys,
            "kpi_format":           kpi_format,
            "generated_at":         now_jst.isoformat(),
        }

        out_path = os.path.join(out_dir, f"{sc_name}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

    if success:
        print(f"  ✓ {ticker}: シナリオファイル生成 → {out_dir}")
    return success


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TANUKI TAIL DCF Bridge — Grokシナリオ → 将来理論価格"
    )
    parser.add_argument("--ticker", nargs="+", help="対象ティッカー（複数可）")
    args = parser.parse_args()

    now_jst = datetime.now(JST)
    print(f"TANUKI TAIL DCF Bridge — {now_jst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 60)

    if args.ticker:
        tickers = args.ticker
    else:
        # VALUATION_DIR 配下のディレクトリのうち、tanuki=true の銘柄のみを対象とする
        # （TICKER-DIRECT-ACCESS-GUARD-1で発見: 以前はos.listdir(VALUATION_DIR)で
        #  tanukiフラグを見ずディレクトリ実在だけでスキャン対象を決めており、
        #  tanuki=false化済み銘柄もVALUATION_DIRにディレクトリが残存していれば
        #  無条件に処理してしまう構造だった。tickers.get_tanuki_tickers()との
        #  積集合に限定する。GitHub Actionsワークフローは常に--ticker明示指定の
        #  ため実害はなかったが、手動での引数なし実行時の潜在リスクを解消する）
        if os.path.exists(VALUATION_DIR):
            tanuki_set = set(_tickers_mod.get_tanuki_tickers())
            tickers = sorted(
                d for d in os.listdir(VALUATION_DIR)
                if os.path.isdir(os.path.join(VALUATION_DIR, d)) and d in tanuki_set
            )
        else:
            print(f"[WARN] VALUATION_DIR 未発見: {VALUATION_DIR}")
            tickers = []

    ok, ng = [], []
    for ticker in tickers:
        result = generate_scenario_files(ticker)
        (ok if result else ng).append(ticker)

    print(f"\n{'━' * 60}")
    print(f"完了: 生成 {len(ok)} 件 / スキップ {len(ng)} 件")
    for t in ok:
        print(f"  OK: {t}")
    for t in ng:
        print(f"  --: {t}")


if __name__ == "__main__":
    main()
