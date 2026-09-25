"""
TANUKI VALUATION - Segment Growth (XBRL決定論的算出)

[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]案①（2026-09-18、
Koichiさん承認済み）: セグメント別成長率の手書き定数（config/segment_
config.json、大半がFY2025時点で更新停止）を、TANUKI TAILの
xbrl_segment_fetcher.pyが生成する`docs/portfolio/tail/data/kpi/
{ticker}_layer2.json`（XBRL標準セグメント軸から取得したセグメント別
実績売上）とSEC EDGAR Layer3（全社売上、`common.sec_data.
layer3_builder`）から決定論的に算出する。AIは一切使わない
（CapEx/SBC/OCFのボトムアップFCF化と同じ設計思想。参照:
[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]）。

対象は7銘柄（APP/CRWV/NVDA/PLTR/SOFI/SOUN/TSLA）。ADBE/CELHは調査
フェーズで判明した通りconfig側とXBRL側のセグメント分類軸が異なる
（例: ADBEはconfig=会計セグメント〈Digital Media/Digital Experience〉
に対しMD&A記述は顧客区分〈Creative & Marketing Professionals等〉、
CELHはconfig=地域〈North America/International〉に対しXBRLは
ブランド〈Celsius/Alani Nu/Rockstar〉）ため対象外とし、既存の
segment_config.json静的値のまま維持する。

セグメント名の対応関係（[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-
IDEA-1]]案①投資フェーズの実データ検証で確定）:
  - PLTR/SOFI/NVDA: config側セグメント名とlayer2.json側KPI名が
    完全一致または表記ゆれのみ（例: "Compute and Networking"↔
    "Compute and Networking売上"）。NVDAの"Graphics"は当初layer2.json
    に未収録だったため、tail_kpi_map.jsonへ`nvda:GraphicsSegmentMember`
    タグを追加（実データで存在確認済み）した上で対応
  - TSLA: 実際のXBRL報告セグメント軸（`us-gaap:StatementBusiness
    SegmentsAxis`）は"Automotive"と"Energy Generation and Storage"の
    2つのみで、config側の3分割（Automotive/Energy/Services and Other）
    のうち"Services and Other"は独立したXBRL報告セグメントとして
    存在しない（実データ確認済み）。よって"Automotive & Services and
    Other"を「全社売上 − Energy Generation and Storage売上」の控除で
    導出し、実際の開示構造（2セグメント）に合わせる
  - APP/CRWV/SOUN: config側は単一セグメント（weight=1.0）のため、
    加重平均は全社売上YoYそのものに一致する。全社売上は
    layer2.json経由ではなくSEC EDGAR Layer3から直接取得する
    （layer2.jsonの"総売上高"系KPIはtail_kpi_map.json側の登録有無に
    依存し全銘柄で保証されないため、既に全99銘柄で確立済みの
    Layer3を単一の信頼できる情報源とする）

平滑化とクリップ（2026-09-18、Koichiさん指示で追加）:
  第1版実装（単一四半期の実績YoYをそのまま採用）を全99銘柄regen前の
  実地検証で流したところ、NVDA($727→$38,623、乖離+17,957%)・
  APP・CRWVでvalidate_calculation()のanomaly_detection（乖離率>1000%）
  がFAILした。原因は、単一四半期のブレをそのまま複数年のDCF Phase1へ
  複利適用してしまうこと（一過性の加速）に加え、NVDA型の「数四半期
  持続する本物の高成長」であってもクリップ機構が皆無だったこと。
  対策として2段構えを導入する:
    1. 入力の平滑化: 直近四半期のみではなく、直近最大4四半期分の
       個別YoY比率を平均する（`_recent_yoy_average()`）。10-Qのみを
       対象とするxbrl_segment_fetcher.pyの構造上、暦年決算銘柄は
       第4四半期（10-K側にのみ含まれる）のデータが常に欠落するため、
       「暦年で連続する4四半期」ではなく「前年同期比較が可能な、
       新しい方から数えて最大4件の四半期」を対象にする（実データで
       PLTR/SOFI/TSLAはQ1-Q3のみ・NVDAは2四半期しか前年同期ペアが
       ない等、暦年連続を要求すると成立しないケースが常態のため）。
    2. 最終防御線としてのクリップ: 平滑化後も個別銘柄の実力次第では
       50%を超えうる（例: NVDAの実測は平滑化後も100%超）ため、
       calculate_fcf_cagr()と全く同じgrowth_floor=15%/growth_cap=50%
       を、加重平均後の最終値（DCFへ実際に渡す値）に適用する。
  透明性確保のため、クリップ前の値（`raw_weighted_growth`）とクリップ
  後の値（`weighted_growth`、実際にDCFへ渡す値）の両方を返す。
  クリップが発動したかは`clipped`で判定できる。個別セグメントの
  `growth`は平滑化後・クリップ前の値（表示上「実際の実績に基づく値」
  であることを維持するため、セグメント単位ではクリップしない）。

フェイルセーフ: 有効なYoYペアが得られない（`_MIN_YOY_PERIODS`未満）
セグメントが1つでもあれば、部分採用（一部セグメントだけ手書き値の
まま混在させる）はせず、呼び出し元全体でNoneを返す（fcf_outlier_ai.py
と同型のフェイルセーフ哲学: 中途半端な値より「使わない」を選ぶ）。
"""

import json
import os
import sys
from datetime import date
from typing import Any, Dict, List, Optional

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_THIS_DIR))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data.layer3_builder import build_ticker_store, get_quarterly_series  # noqa: E402

# config側セグメント名 → layer2.json側KPI名。TSLAの
# "Automotive & Services and Other"は直接のKPIを持たず、
# _compute_multi_segment()内で全社売上からの控除により導出する。
_SEGMENT_KPI_ALIASES: Dict[str, Dict[str, str]] = {
    "PLTR": {
        "Government": "Government売上",
        "Commercial": "Commercial売上",
    },
    "SOFI": {
        "Lending": "Lending売上",
        "Technology Platform": "Technology Platform売上",
        "Financial Services": "Financial Services売上",
    },
    "NVDA": {
        "Compute and Networking": "Compute and Networking売上",
        "Graphics": "Graphics売上",
    },
    "TSLA": {
        "Energy Generation and Storage": "Energy Generation & Storage売上",
    },
}

# config側が単一セグメント（weight=1.0）のため、加重平均が全社売上YoY
# と一致する銘柄。全社売上はSEC EDGAR Layer3から取得する。
_SINGLE_SEGMENT_TICKERS: Dict[str, str] = {
    "APP": "Software Platform",
    "CRWV": "Cloud Infrastructure",
    "SOUN": "Voice AI",
}

# calculate_fcf_cagr()（calculator/growth.py）と完全に同一の値。
# 「一時的な急成長をそのまま複数年複利適用しない」という設計思想を
# segment_xbrlにも同様に適用する（2026-09-18、Koichiさん指示）。
_GROWTH_FLOOR = 0.15
_GROWTH_CAP = 0.50

# 平滑化に使うYoY比較の最大件数・最低件数。10-Qのみを対象とする
# xbrl_segment_fetcher.pyの構造上「暦年で連続する4四半期」は保証
# されないため、「前年同期比較が可能な直近N件」という定義にする。
_MAX_YOY_PERIODS = 4
_MIN_YOY_PERIODS = 2

# weight算出（直近実績の売上構成比）に使う四半期件数の上限。
_MAX_WEIGHT_QUARTERS = 4

# 基準四半期の遅延許容（2026-09-25、Koichiさん指示）: segment_xbrlの基準
# 四半期（layer2.jsonのセグメント系列の最新共通四半期）がSEC EDGAR Layer3の
# 全社売上の最新四半期からこの四半期数以上遅れている場合、古い成長率を
# DCFへ使わず呼び出し元の次順位ソース（segment_config.json）へフォール
# バックする。比較は両者とも暦年ラベル（_quarter_label()、end日の暦月から
# 算出）で行う。layer2.jsonのquarterも暦年ラベル（例: NVDAの"2026Q3"は
# 2026-07-26期末＝会計年度2027Q2、filed 2026-08-26）であり、common/sec_data/
# data/のquarterly_{fy}{fp}.json（会計年度ラベル）とは直接比較できない。
_STALE_LAG_QUARTERS = 2


def _quarter_label(end: str) -> str:
    d = date.fromisoformat(end[:10])
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def _quarter_index(quarter: str) -> int:
    """"2026Q3" → 2026*4+3（四半期差の算出用）"""
    return int(quarter[:4]) * 4 + int(quarter[5:])


def _prev_year_quarter(quarter: str) -> str:
    return f"{int(quarter[:4]) - 1}{quarter[4:]}"


def _sorted_quarters_desc(series: Dict[str, float]) -> List[str]:
    return sorted(series.keys(), reverse=True)


def _recent_yoy_values(series: Dict[str, float], max_n: int = _MAX_YOY_PERIODS) -> List[float]:
    """新しい四半期から順に、前年同期比較が可能なYoY比率を最大max_n件
    集める（新しい順）。前年同期データが無い四半期はスキップする
    （10-Qのみのデータでは暦年第4四半期が常に欠落するため）。"""
    yoys = []
    for q in _sorted_quarters_desc(series):
        if len(yoys) >= max_n:
            break
        curr = series.get(q)
        prev = series.get(_prev_year_quarter(q))
        if curr is None or prev is None or prev == 0:
            continue
        yoys.append((curr - prev) / abs(prev))
    return yoys


def _smoothed_growth(series: Dict[str, float]) -> Optional[float]:
    """直近最大_MAX_YOY_PERIODS件のYoY比率の単純平均（クリップ前）。
    有効なYoYペアが_MIN_YOY_PERIODS件未満ならNoneを返す。"""
    yoys = _recent_yoy_values(series)
    if len(yoys) < _MIN_YOY_PERIODS:
        return None
    return sum(yoys) / len(yoys)


def _clip_growth(rate: float) -> float:
    return max(_GROWTH_FLOOR, min(_GROWTH_CAP, rate))


def _recent_weight_bases(series_map: Dict[str, Dict[str, float]], max_n: int = _MAX_WEIGHT_QUARTERS) -> Dict[str, float]:
    """比較対象の全系列に共通して存在する四半期のうち、直近max_n件分の
    生値合計を各系列について返す（季節性の影響を減らすため単一四半期
    ではなく複数四半期を使う）。全系列が同じ四半期集合を共有する前提
    （同一ティッカー内の複数セグメントは同じ10-Qファイリング由来の
    ため常に成立する）。"""
    common = set.intersection(*(set(s) for s in series_map.values()))
    recent = sorted(common, reverse=True)[:max_n]
    return {name: sum(series[q] for q in recent) for name, series in series_map.items()}


def _load_layer2_kpi_series(repo_root: str, ticker: str, kpi_name: str) -> Dict[str, float]:
    path = os.path.join(repo_root, "docs", "portfolio", "tail", "data", "kpi", f"{ticker}_layer2.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            layer2 = json.load(f)
    except Exception:
        return {}
    data = (layer2.get("kpis", {}).get(kpi_name) or {}).get("data", [])
    return {d["quarter"]: d["value"] for d in data if d.get("value") is not None}


def _total_revenue_series(ticker: str) -> Dict[str, float]:
    """SEC EDGAR Layer3（全99銘柄で確立済みの共通データ源）から
    四半期別の全社売上系列を取得する。"""
    store = build_ticker_store(ticker)
    if store is None:
        return {}
    result: Dict[str, float] = {}
    for e in get_quarterly_series(store, "revenue"):
        val = e.get("val")
        end = e.get("end")
        if val is None or not end:
            continue
        result[_quarter_label(end)] = val
    return result


def _finalize(segments_out: Dict[str, Dict[str, float]], quarter: str, method: str) -> Dict[str, Any]:
    """weightで加重平均した後、最終値にのみgrowth_floor/growth_capを
    適用する（個別セグメントのgrowthは実績に基づく非クリップ値のまま
    表示用に保持し、クリップ前後の両方を透明性のため返す）。"""
    raw_weighted = sum(v["weight"] * v["growth"] for v in segments_out.values())
    clipped_weighted = _clip_growth(raw_weighted)
    return {
        "segments": segments_out,
        "weighted_growth": clipped_weighted,
        "raw_weighted_growth": raw_weighted,
        "clipped": abs(clipped_weighted - raw_weighted) > 1e-12,
        "growth_floor": _GROWTH_FLOOR,
        "growth_cap": _GROWTH_CAP,
        "quarter": quarter,
        "method": method,
    }


def _compute_multi_segment(repo_root: str, ticker: str) -> Optional[Dict[str, Any]]:
    aliases = _SEGMENT_KPI_ALIASES.get(ticker)
    if not aliases:
        return None

    seg_series: Dict[str, Dict[str, float]] = {
        name: _load_layer2_kpi_series(repo_root, ticker, kpi_name)
        for name, kpi_name in aliases.items()
    }
    if any(not s for s in seg_series.values()):
        return None

    if ticker == "TSLA":
        total_series = _total_revenue_series(ticker)
        energy_series = seg_series["Energy Generation and Storage"]
        if not total_series:
            return None
        auto_series = {q: total_series[q] - energy_series[q] for q in total_series if q in energy_series}
        if not auto_series:
            return None

        auto_g = _smoothed_growth(auto_series)
        energy_g = _smoothed_growth(energy_series)
        if auto_g is None or energy_g is None:
            return None

        bases = _recent_weight_bases({"auto": auto_series, "energy": energy_series})
        auto_w_basis, energy_w_basis = bases["auto"], bases["energy"]
        total_w_basis = auto_w_basis + energy_w_basis
        if total_w_basis <= 0:
            return None

        latest_q = max(set(auto_series) & set(energy_series))
        segments_out = {
            "Automotive & Services and Other": {
                "weight": auto_w_basis / total_w_basis, "growth": auto_g,
            },
            "Energy Generation and Storage": {
                "weight": energy_w_basis / total_w_basis, "growth": energy_g,
            },
        }
        return _finalize(segments_out, latest_q, "segment_xbrl_yoy_smoothed")

    growths = {name: _smoothed_growth(s) for name, s in seg_series.items()}
    if any(g is None for g in growths.values()):
        return None

    bases = _recent_weight_bases(seg_series)
    total_rev = sum(bases.values())
    if total_rev <= 0:
        return None

    latest_q = max(set.intersection(*(set(s) for s in seg_series.values())))
    segments_out = {
        name: {"weight": bases[name] / total_rev, "growth": growths[name]}
        for name in seg_series
    }
    return _finalize(segments_out, latest_q, "segment_xbrl_yoy_smoothed")


def _compute_single_segment(ticker: str) -> Optional[Dict[str, Any]]:
    seg_name = _SINGLE_SEGMENT_TICKERS.get(ticker)
    if not seg_name:
        return None
    total_series = _total_revenue_series(ticker)
    if not total_series:
        return None
    g = _smoothed_growth(total_series)
    if g is None:
        return None
    latest_q = max(total_series)
    segments_out = {seg_name: {"weight": 1.0, "growth": g}}
    return _finalize(segments_out, latest_q, "total_revenue_yoy_smoothed")


def compute_xbrl_segment_growth(ticker: str, repo_root: str = _REPO_ROOT) -> Optional[Dict[str, Any]]:
    """セグメント別実績YoY（直近最大4四半期の平均・平滑化）と売上構成比
    から加重平均成長率を決定論的に算出し、calculate_fcf_cagr()と同じ
    growth_floor/growth_capでクリップする。対応不可（対象外銘柄・
    データ欠損）の場合はNoneを返す（呼び出し元は既存の優先順位チェーン
    へフォールバックする）。

    Returns:
        {"segments": {name: {"weight", "growth"}, ...}  # growthは平滑化後・クリップ前
         "weighted_growth": float,       # クリップ後、DCFへ実際に渡す値
         "raw_weighted_growth": float,   # クリップ前（表示用）
         "clipped": bool, "growth_floor": float, "growth_cap": float,
         "quarter": "2026Q2", "method": str}
        または対応不可の場合はNone
    """
    status = get_xbrl_segment_status(ticker, repo_root=repo_root)
    if status is None or status["stale"]:
        return None
    return status["result"]


def get_xbrl_segment_status(ticker: str, repo_root: str = _REPO_ROOT) -> Optional[Dict[str, Any]]:
    """segment_xbrlの算出結果と基準四半期の遅延状況を返す。

    Returns:
        {"result": compute_xbrl_segment_growth()相当のdict,
         "quarter": 基準四半期（暦年ラベル）,
         "reference_quarter": Layer3全社売上の最新四半期（暦年ラベル、
                              取得できなければNone）,
         "lag_quarters": int（reference_quarterがNoneなら0）,
         "stale": lag_quarters >= _STALE_LAG_QUARTERS}
        対象外銘柄・データ欠損の場合はNone。
    compute_xbrl_segment_growth()はstale=Trueの場合Noneを返し、呼び出し元は
    次順位ソースへフォールバックする。本関数はその理由（遅延四半期数）を
    latest.jsonへ記録するための窓口（pipeline.py::_load_extra_data()）。
    """
    try:
        if ticker in _SINGLE_SEGMENT_TICKERS:
            result = _compute_single_segment(ticker)
        elif ticker in _SEGMENT_KPI_ALIASES:
            result = _compute_multi_segment(repo_root, ticker)
        else:
            return None
    except Exception:
        return None
    if result is None:
        return None
    try:
        total = _total_revenue_series(ticker)
    except Exception:
        total = {}
    reference_quarter = max(total) if total else None
    lag = 0
    if reference_quarter and result.get("quarter"):
        lag = max(0, _quarter_index(reference_quarter) - _quarter_index(result["quarter"]))
    return {
        "result": result,
        "quarter": result.get("quarter"),
        "reference_quarter": reference_quarter,
        "lag_quarters": lag,
        "stale": lag >= _STALE_LAG_QUARTERS,
    }
