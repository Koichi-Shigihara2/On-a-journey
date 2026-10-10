# discover/stonks-silo/src/signal_detector.py
"""
財務ベクトル計算モジュール

Layer3ストア（common/sec_data/layer3_builder.py::build_ticker_store()の
戻り値、company_facts.json由来）の四半期データから各指標のYoY・QoQ
ベクトルを計算し、全銘柄のパーセンタイルで正規化してベクトル（角度・長さ）
に変換する。

[フェーズD Step2-2対応、2026-08-07] 従来は`common/sec_data/normalized/`
（quarterly.py出力）を参照していたが、Layer3
（`layer3_builder.py::get_field_entries()`）経由に切替済み
（`SEC_EDGAR_LAYER_DESIGN.md`フェーズD Step2-2）。SMフィールドは
`[[FINTREND-SM-JOBY-NONE-1]]`の通りLayer3の挙動（selling_and_marketing
単体タグのみを候補とし、SGA総額へのフォールバックを行わない。JOBY等で
該当フィールドがNoneになる）をそのまま受け入れている。

ベクトル定義:
  角度: 半円180度。真上(90°)=最大改善, 水平(0°)=変化なし, 真下(-90°)=最大悪化
  長さ: 全銘柄×全期間の変化率分布のパーセンタイル(0-1)
  原価(原価率): 下がる方が改善のため符号を反転

[[STONKS-FINANCIAL-VECTORS-RELATIVE-1]]、2026-09-06: angle/length/
percentileは絶対的な変化率ではなく、この実行時点で有効なchange_pctを
持つ銘柄集合に対する相対順位である（同時点の全STONKS SILO銘柄集合とは
限らず、フィールド・期間ごとにデータが揃っている銘柄数が異なる）。
新規銘柄の追加・除外のたびに、対象銘柄自身のデータが変わっていなくても
値が変動しうる。この母集団サイズを`population_size`として各yoy/qoq
辞書に記録する。change_pct/val_latest/val_prev/series_qは絶対値のため
母集団変動の影響を受けない。画面表示（docs/value-monitor/stonks-silo/
index.html）ではpercentile/angle/length/compositeはいずれも未使用
（change_pct等の絶対値のみ表示）であることを確認済みのため、本注記は
results.json側のメタ情報としてのみ追加し、画面表示への変更は行わない。

出力フィールド（results.jsonのticker配下に追加）:
  "financial_vectors": {
    "updated_at": "...",
    "fields": {
      "OCF":        { "yoy": {"angle":45, "length":0.7, "percentile":75,
                               "population_size":42, "val_latest":-26M, "val_prev":-35M},
                      "qoq": {"angle":20, "length":0.4, "percentile":60,
                               "population_size":40, ...} },
      "RD":         { ... },
      "NetIncome":  { ... },
      "CapEx":      { ... },
      "Revenue":    { ... },   # 取得できない銘柄はNone
      "GrossProfit":{ ... },   # 取得できない銘柄はNone（逆算含む）
    },
    "composite": {
      "yoy": {"angle":35, "length":0.6},
      "qoq": {"angle":50, "length":0.8},
    },
    "data_quality": {
      "fields_available": ["OCF","RD","NetIncome","CapEx"],
      "fields_missing":   ["Revenue","GrossProfit"],
    }
  }
"""

from __future__ import annotations

import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_THIS_DIR = Path(__file__).resolve().parent
# discover/stonks-silo/src/ → repo root は3階層上
# テスト環境ではフォールバックとして現在ディレクトリから探す
try:
    _REPO_ROOT = _THIS_DIR.parents[2]
except IndexError:
    _REPO_ROOT = Path.cwd()

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from common.sec_data.layer3_builder import (  # noqa: E402
    build_ticker_store, get_field_entries, get_quarterly_series,
)
from common.sec_data.q4_implied import build_q4_implied_entries  # noqa: E402

# 計算対象フィールド（PascalCase、既存の表示・フロントエンド契約を維持）
# invert=True: 値が下がる方が改善（原価率）
VECTOR_FIELDS = [
    {"name": "Revenue",         "invert": False, "weight": 1.5},
    {"name": "GrossProfit",     "invert": False, "weight": 1.5},  # 原価率の合成
    {"name": "OperatingIncome", "invert": False, "weight": 1.5},  # 営業利益（黒字化への道のり用）
    {"name": "RD",              "invert": False, "weight": 1.0},  # 投資継続性（増加=良い）
    {"name": "NetIncome",       "invert": False, "weight": 2.0},  # 損益改善
    {"name": "OCF",             "invert": False, "weight": 2.0},  # 現金創出力
    {"name": "CapEx",           "invert": False, "weight": 0.5},  # 投資強度（参考）
]

# ヒートマップ対象フィールド（全フィールド・データある四半期のみ表示）
HEATMAP_FIELDS = {"Revenue", "GrossProfit", "OperatingIncome", "RD", "NetIncome", "OCF"}

# サブパネル用（別折りたたみ）
SUB_FIELDS = ["SM", "SBC"]

# 合成ベクトルに使うフィールド（RD/CapExは方向性が複雑なため除外）
COMPOSITE_FIELDS = {"Revenue", "GrossProfit", "NetIncome", "OCF"}

# PascalCase（既存フィールド名・フロントエンド契約）→ Layer3 snake_case
# フィールド名の対応表（フェーズD Step2-2対応）。
_FIELD_MAP = {
    "Revenue": "revenue",
    "GrossProfit": "gross_profit",
    "OperatingIncome": "operating_income",
    "RD": "research_and_development",
    "NetIncome": "net_income",
    "OCF": "operating_cash_flow",
    "CapEx": "capital_expenditure",
    "SM": "selling_and_marketing",
    "SBC": "stock_based_compensation",
}


# Q4 implied生成本体はcommon/sec_data/q4_implied.py::build_q4_implied_entries()
# に集約済み（[[Q4-IMPLIED-CALC-TRIPLICATION-1]]対応、移行実装計画フェーズB）。


def _get_quarterly_entries(store: dict, field_name: str) -> list:
    """standalone Q エントリ + Q4 implied を返す（年次・YTD除外）。

    store はLayer3ストア（layer3_builder.build_ticker_store()の戻り値）。
    field_name は本モジュールのPascalCase表記（VECTOR_FIELDS/SUB_FIELDS）
    をそのまま受け取り、内部で_FIELD_MAP経由でLayer3のsnake_case
    フィールド名に変換する。
    """
    snake_field = _FIELD_MAP[field_name]
    quarterly = get_quarterly_series(store, snake_field)
    entries = get_field_entries(store, snake_field)
    annual = [e for e in entries if e.get("is_annual")]

    # Q4 impliedを追加
    q4_implied = build_q4_implied_entries(annual, quarterly, snake_field)

    # 既存Q4エントリと重複しないよう end_date で管理
    existing_ends = {e["end"] for e in quarterly}
    for q4 in q4_implied:
        if q4["end"] not in existing_ends:
            quarterly.append(q4)
            existing_ends.add(q4["end"])

    return sorted(quarterly, key=lambda x: x["end"])



# [[STONKS-HEATMAP-FQ-LABEL-1]]（2026-10-10、旧[[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]の方式を置き換え）:
# 四半期の識別はend（期末日）だけで行い、fpは判断に使わない。Layer3の四半期エントリの
# fp/fyは「その数値を最後に申告した書類のfp/fy」で、期間そのものの四半期ではない
# （例: JOBYの2025-03-31・2026-03-31の純利益は、2026年Q2の10-Qで再掲されたためfp="Q2"）。
# 以前はfpでグルーピングしてから日数を検証していたため、こうした系列では前年同期比が
# 算出不能（None）になっていた（2026-10-10時点で24銘柄×7指標の168系列中10系列）。
# 前年同期 = 最新エントリのendから330〜400日前にendがあるエントリ（複数なら365日に最も近いもの）。
# QoQ = 直前のエントリとの日数差が60〜120日のときだけ（欠けた四半期・決算期の変更をまたがない）。
# QoQの閾値はindex.htmlのヒートマップ（QOQ_GAP_DAYS_MIN/MAX）と同じ値。
_YOY_GAP_DAYS_MIN = 330
_YOY_GAP_DAYS_MAX = 400
_YOY_TARGET_DAYS = 365
_QOQ_GAP_DAYS_MIN = 60
_QOQ_GAP_DAYS_MAX = 120


def _days_between(end_later: str, end_earlier: str) -> Optional[int]:
    try:
        return (datetime.strptime(end_later, "%Y-%m-%d") - datetime.strptime(end_earlier, "%Y-%m-%d")).days
    except (ValueError, TypeError):
        return None


def _calc_yoy_change(entries: list) -> Optional[dict]:
    """
    直近四半期と前年同期のYoY変化率を計算する（期末日だけで前年同期を決める、上記コメント参照）。

    前年同期は、最新エントリ（entries[-1]）のendから330〜400日前にendがあるエントリ。
    該当が複数あれば365日に最も近いもの、無ければNone。戻り値の"fp"は互換のため残すが
    空文字（期間はend_latest・end_prevで分かる。消費者がいないことは2026-10-10に確認）。
    """
    if len(entries) < 5:
        return None

    latest = entries[-1]
    end_latest_str = latest["end"]
    candidates = []
    for e in entries[:-1]:
        gap = _days_between(end_latest_str, e["end"])
        if gap is not None and _YOY_GAP_DAYS_MIN <= gap <= _YOY_GAP_DAYS_MAX:
            candidates.append((abs(gap - _YOY_TARGET_DAYS), e))
    if not candidates:
        return None
    prev = min(candidates, key=lambda x: x[0])[1]

    val_latest = latest["val"]
    val_prev   = prev["val"]

    if val_prev == 0:
        return None

    change_pct = (val_latest - val_prev) / abs(val_prev) * 100
    return {
        "change_pct": change_pct,
        "val_latest": val_latest,
        "val_prev":   val_prev,
        "end_latest": end_latest_str,
        "end_prev":   prev["end"],
        "fp":         "",
    }


def _calc_qoq_change(entries: list) -> Optional[dict]:
    """直近四半期と前四半期のQoQ変化率を計算。直前のエントリとの日数差が60〜120日のときだけ
    （範囲外は欠けた四半期・決算期の変更をまたいだ比較になるためNone）。"""
    if len(entries) < 2:
        return None

    gap = _days_between(entries[-1]["end"], entries[-2]["end"])
    if gap is None or not (_QOQ_GAP_DAYS_MIN <= gap <= _QOQ_GAP_DAYS_MAX):
        return None

    val_latest = entries[-1]["val"]
    val_prev   = entries[-2]["val"]

    if val_prev == 0:
        return None

    change_pct = (val_latest - val_prev) / abs(val_prev) * 100
    return {
        "change_pct": change_pct,
        "val_latest": val_latest,
        "val_prev":   val_prev,
        "end_latest": entries[-1]["end"],
        "end_prev":   entries[-2]["end"],
    }


def _pct_to_angle(pct: float) -> float:
    """
    パーセンタイル(0-100) → 角度(-90°〜+90°)
    50パーセンタイル = 0°（水平、変化なし）
    100パーセンタイル = +90°（真上、最大改善）
    0パーセンタイル = -90°（真下、最大悪化）
    """
    normalized = (pct - 50) / 50  # -1 〜 +1
    return round(normalized * 90, 1)


def _pct_to_length(pct: float) -> float:
    """パーセンタイル(0-100) → 長さ(0-1)。50パーセンタイルを0.5とする。"""
    return round(abs(pct - 50) / 50, 3)


def compute_vectors(all_stores: dict[str, dict]) -> dict[str, dict]:
    """
    全銘柄のLayer3ストアからベクトルを計算する。

    Parameters
    ----------
    all_stores: {ticker: layer3_store_dict}
        layer3_store_dict は layer3_builder.build_ticker_store() の戻り値
        （引数名は`load_all_normalized()`と同様、既存呼び出し元
        〈pipeline.py〉との互換性のため維持しているが、中身はLayer3
        ストアである点に注意）。

    Returns
    -------
    {ticker: financial_vectors_dict}
    """
    # Step1: 全銘柄×全フィールドのchange_pctを収集してパーセンタイル基準を作る
    all_changes: dict[str, dict[str, list[float]]] = {
        "yoy": {f["name"]: [] for f in VECTOR_FIELDS},
        "qoq": {f["name"]: [] for f in VECTOR_FIELDS},
    }

    raw_changes: dict[str, dict] = {}  # {ticker: {field: {yoy/qoq: change_info}}}

    for ticker, store in all_stores.items():
        raw_changes[ticker] = {}
        for field_cfg in VECTOR_FIELDS:
            fname = field_cfg["name"]
            entries = _get_quarterly_entries(store, fname)
            yoy = _calc_yoy_change(entries)
            qoq = _calc_qoq_change(entries)
            raw_changes[ticker][fname] = {"yoy": yoy, "qoq": qoq}

            if yoy is not None:
                pct = yoy["change_pct"]
                if field_cfg["invert"]:
                    pct = -pct
                all_changes["yoy"][fname].append(pct)
            if qoq is not None:
                pct = qoq["change_pct"]
                if field_cfg["invert"]:
                    pct = -pct
                all_changes["qoq"][fname].append(pct)

    # Step2: 各フィールドのパーセンタイル計算用ソート済みリスト
    sorted_changes: dict[str, dict[str, list[float]]] = {
        "yoy": {fname: sorted(vals) for fname, vals in all_changes["yoy"].items()},
        "qoq": {fname: sorted(vals) for fname, vals in all_changes["qoq"].items()},
    }

    def _calc_percentile(val: float, sorted_vals: list[float]) -> float:
        """変化率をパーセンタイル(0-100)に変換"""
        if not sorted_vals:
            return 50.0
        n = len(sorted_vals)
        # 二分探索で位置を求める
        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            if sorted_vals[mid] < val:
                lo = mid + 1
            else:
                hi = mid
        return round(lo / n * 100, 1)

    # Step3: 各銘柄のベクトルを計算
    results: dict[str, dict] = {}

    for ticker, field_data in raw_changes.items():
        store = all_stores[ticker]
        fields_out: dict = {}
        available: list[str] = []
        missing: list[str] = []
        composite_angles_yoy: list[tuple[float, float]] = []  # (angle, weight)
        composite_angles_qoq: list[tuple[float, float]] = []

        # 全フィールドの最新endを取得して基準日を決定
        all_entries_by_field = {
            field_cfg["name"]: _get_quarterly_entries(store, field_cfg["name"])
            for field_cfg in VECTOR_FIELDS
        }
        latest_ends = [
            entries[-1]["end"]
            for entries in all_entries_by_field.values()
            if entries
        ]
        # 基準日 = 全フィールドの最新endの最大値
        base_end = max(latest_ends) if latest_ends else None

        for field_cfg in VECTOR_FIELDS:
            fname = field_cfg["name"]
            invert = field_cfg["invert"]
            weight = field_cfg["weight"]
            info = field_data.get(fname, {})

            field_out: dict = {}

            # 時系列データ: base_endより古い（または等しい）エントリの末尾8件
            entries = all_entries_by_field[fname]
            if base_end:
                filtered = [e for e in entries if e["end"] <= base_end]
            else:
                filtered = entries
            recent = filtered[-8:]
            # fpはLayer3の値（申告した書類のfp）をそのまま残す。期間そのものの四半期ではない
            # ため、計算・画面のラベルには使わない（[[STONKS-HEATMAP-FQ-LABEL-1]]、画面はendのyy/mm）
            field_out["series_q"] = [
                {"end": e["end"], "fp": e.get("fp", ""), "val": e["val"]}
                for e in recent
            ]

            for period in ("yoy", "qoq"):
                change = info.get(period)
                if change is None:
                    field_out[period] = None
                    continue

                pct_val = change["change_pct"]
                if invert:
                    pct_val = -pct_val

                sv = sorted_changes[period].get(fname, [])
                pct_rank = _calc_percentile(pct_val, sv)
                angle = _pct_to_angle(pct_rank)
                length = _pct_to_length(pct_rank)

                field_out[period] = {
                    "angle":      angle,
                    "length":     length,
                    "percentile": pct_rank,
                    # [[STONKS-FINANCIAL-VECTORS-RELATIVE-1]]: angle/length/
                    # percentileは絶対的な変化率ではなく、この実行時点で
                    # 有効なchange_pctを持つ銘柄集合（下記population_size件）
                    # に対する相対順位である。フィールド・期間（yoy/qoq）ごとに
                    # データが揃っている銘柄数が異なるため、population_sizeは
                    # フィールド・期間ごとに個別に記録する（全銘柄一律の値では
                    # ない）。change_pct/val_latest/val_prev/series_qは絶対値
                    # のため母集団変動の影響を受けない。
                    "population_size": len(sv),
                    "change_pct": round(change["change_pct"], 1),
                    "val_latest": change["val_latest"],
                    "val_prev":   change["val_prev"],
                    "end_latest": change["end_latest"],
                    "end_prev":   change["end_prev"],
                }
                if "fp" in change:
                    field_out[period]["fp"] = change["fp"]

                # 合成ベクトル用に収集（対象フィールドのみ）
                if fname in COMPOSITE_FIELDS:
                    if period == "yoy":
                        composite_angles_yoy.append((angle, weight))
                    else:
                        composite_angles_qoq.append((angle, weight))

            fields_out[fname] = field_out
            if any(field_out.get(p) is not None for p in ("yoy", "qoq")):
                available.append(fname)
            else:
                missing.append(fname)

        # 合成ベクトル（加重平均角度）
        def _weighted_angle(angle_weights: list[tuple[float, float]]) -> Optional[dict]:
            if not angle_weights:
                return None
            total_w = sum(w for _, w in angle_weights)
            if total_w == 0:
                return None
            avg_angle = sum(a * w for a, w in angle_weights) / total_w
            # 角度の絶対値を長さとして使用（-90〜+90 → 0〜1）
            length = round(abs(avg_angle) / 90, 3)
            return {
                "angle":  round(avg_angle, 1),
                "length": length,
            }

        results[ticker] = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "fields": fields_out,
            "composite": {
                "yoy": _weighted_angle(composite_angles_yoy),
                "qoq": _weighted_angle(composite_angles_qoq),
            },
            "data_quality": {
                "fields_available": available,
                "fields_missing":   missing,
                "heatmap_fields":   sorted(HEATMAP_FIELDS),
            },
        }

    return results


def load_all_normalized(tickers: list[str]) -> dict[str, dict]:
    """対象ティッカーのLayer3ストアを一括構築する。

    [フェーズD Step2-2対応] 関数名はpipeline.py等の既存呼び出し元との
    互換性のため維持しているが、戻り値は従来のnormalized/ JSON辞書では
    なく layer3_builder.build_ticker_store() の戻り値（Layer3ストア、
    company_facts.json由来）である点に注意。
    """
    result = {}
    for ticker in tickers:
        store = build_ticker_store(ticker)
        if store is None:
            logger.warning("[%s] Layer3 store not found (company_facts.json missing)", ticker)
            continue
        result[ticker] = store
    return result
