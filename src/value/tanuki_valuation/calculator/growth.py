"""
TANUKI VALUATION - Growth Rate Calculator
成長率決定ロジック

責務: 高成長期間の成長率を決定
優先順位: セグメント加重成長率 > FCF CAGR > デフォルト
"""

import logging
import os
import sys
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

# 外部のsegment_config.pyからインポート
try:
    from segment_config import get_segment_growth as _get_segment_growth_from_config
    HAS_SEGMENT_CONFIG = True
except ImportError:
    HAS_SEGMENT_CONFIG = False
    _get_segment_growth_from_config = None

# [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]: common/sec_data/contracts.pyの
# FCFSeriesをFCF CAGR算出直前の順序再検証に使う。data_fetcher.py::
# TTMReaderと同じsys.path解決パターン（2段構えtry/except）を、growth.py
# 自身の場所（src/value/tanuki_valuation/calculator/）から独立して踏襲する。
HAS_CONTRACTS = False
FCFSeries = None
ContractViolation = None

try:
    _current_dir = os.path.dirname(os.path.abspath(__file__))
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_current_dir))))
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from common.sec_data.contracts import FCFSeries, ContractViolation
    HAS_CONTRACTS = True
except Exception:
    pass

if not HAS_CONTRACTS:
    try:
        _github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if _github_workspace and _github_workspace not in sys.path:
            sys.path.insert(0, _github_workspace)
        from common.sec_data.contracts import FCFSeries, ContractViolation
        HAS_CONTRACTS = True
    except Exception:
        pass


@dataclass
class GrowthResult:
    """成長率計算結果"""
    rate: float
    source: str  # "segment_weighted" | "fcf_cagr" | "default"
    segment_detail: Optional[Dict[str, Any]] = None
    cagr_detail: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "rate": self.rate,
            "source": self.source
        }
        if self.segment_detail:
            result["segment_detail"] = self.segment_detail
        if self.cagr_detail:
            result["cagr_detail"] = self.cagr_detail
        return result


def get_segment_growth(ticker: str) -> Optional[GrowthResult]:
    """
    セグメント加重平均成長率を取得
    
    Args:
        ticker: 銘柄コード
    
    Returns:
        GrowthResult or None (設定なし/無効の場合)
    """
    if not HAS_SEGMENT_CONFIG:
        return None
    
    # segment_config.pyのget_segment_growth()を呼び出す
    config = _get_segment_growth_from_config(ticker)
    
    if config is None:
        return None
    
    # segment_config.pyがdictを返す場合の処理
    if isinstance(config, dict):
        if not config.get("enabled"):
            return None
        
        weighted_growth = config.get("weighted_growth")
        if weighted_growth is None:
            return None
        
        return GrowthResult(
            rate=weighted_growth,
            source="segment_weighted",
            segment_detail={
                "enabled": True,
                "weighted_growth": weighted_growth,
                "fiscal_year": config.get("fiscal_year"),
                "source": config.get("source", "segment_config")
            }
        )

    # segment_config.pyがfloatを返す場合の処理
    if isinstance(config, (int, float)):
        return GrowthResult(
            rate=float(config),
            source="segment_weighted",
            segment_detail={
                "enabled": True,
                "weighted_growth": float(config),
                "source": "segment_config"
            }
        )
    
    return None


def _filter_positive_with_dates(
    values: List[float], dates: Optional[List[Any]]
) -> "tuple[List[float], Optional[List[Any]]]":
    """
    正の値のみを残すフィルタを、値と日付のペア対応を保ったまま適用する。

    [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]: 値だけをフィルタして日付を
    素通りさせると、フィルタ後の値と日付の対応がズレてしまい、順序検証
    自体が「何を検証しているか分からない」無意味な状態になる。datesが
    Noneの場合（日付なし経路・後方互換）はフィルタ後もNoneのまま返す。
    """
    if dates is None:
        return [f for f in values if f > 0], None
    filtered_pairs = [(f, d) for f, d in zip(values, dates) if f > 0]
    filtered_values = [f for f, _d in filtered_pairs]
    filtered_dates = [d for _f, d in filtered_pairs]
    return filtered_values, filtered_dates


def calculate_fcf_cagr(
    fcf_list: List[float],
    min_periods: int = 2,
    growth_floor: float = 0.15,
    growth_cap: float = 0.50,
    fcf_dates: Optional[List[Any]] = None
) -> Optional[GrowthResult]:
    """
    FCF CAGRを計算

    Args:
        fcf_list: FCFリスト（新しい順、fcf_list[0]が直近。adjustments.py等
            本コードベース全体の規約に合わせる）
        min_periods: 最低期間
        growth_floor: 成長率下限
        growth_cap: 成長率上限
        fcf_dates: fcf_listと対応する日付（TTM経路はttm_end文字列、年次
            経路は会計年度int）。[[GATE2-READER-FCFLIST-1]]/
            [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]対応。渡された場合のみ
            common.sec_data.contracts.FCFSeriesで新しい順規約を
            construction時に検証し、`.oldest`/`.newest`アクセサ経由で
            start_value/end_valueを取得する。順序違反はログWARNのみで
            処理を継続する（例外を送出してパイプラインを止めない。
            CHECK-32〜40と同型の「検知してWARN・処理は継続」方針）。
            Noneの場合は従来通り検証をスキップし、直接インデックス
            アクセスのまま計算する（後方互換）。

    Returns:
        GrowthResult or None (計算不可の場合)
    """
    if len(fcf_list) < 3:
        return None

    # 正のFCFのみを対象（直近5年分。fcf_listは新しい順のため先頭からスライス）
    # GROWTH-CAGR-SIGN-1（2026-07-11修正前）: 旧実装は fcf_list[-5:] と
    #末尾からスライスしており、5年超のデータがある場合に最古側を
    # 誤って対象にしていた。
    head_dates = fcf_dates[:5] if fcf_dates is not None else None
    recent_fcfs, recent_dates = _filter_positive_with_dates(fcf_list[:5], head_dates)
    if len(recent_fcfs) < min_periods:
        return None

    # CAGR計算（fcf_list[0]=直近・fcf_list[-1]=最古の規約に合わせ、
    # start_value=最古・end_value=直近とする）
    # GROWTH-CAGR-SIGN-1（2026-07-11修正前）: 旧実装は
    # start_value=recent_fcfs[0]（直近）・end_value=recent_fcfs[-1]（最古）
    # と割り当てており、raw_cagr = (最古/直近)**(1/n)-1 という逆向きの
    # 計算になっていた（adjustments.py:268-272の正しい実装
    # `(fcf_list[0] / fcf_list[-1])` と方向が食い違っていた）。
    #
    # [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]: recent_datesがある場合は
    # FCFSeriesで降順規約を検証した上で.oldest/.newestアクセサから取得
    # する（直接インデックスの取り違え再発防止）。違反時・日付なし時は
    # 従来通りrecent_fcfs[-1]/[0]を使う（値は常に同一、検証は付加的な
    # 安全網に過ぎない）。
    start_value = recent_fcfs[-1]
    end_value = recent_fcfs[0]
    if recent_dates is not None and HAS_CONTRACTS:
        try:
            series = FCFSeries(recent_fcfs, recent_dates)
            start_value = series.oldest
            end_value = series.newest
        except ContractViolation as e:
            logging.warning(
                "calculate_fcf_cagr: FCFSeries順序規約違反を検知（WARNのみ、"
                "処理は継続）: %s", e
            )
        except TypeError as e:
            # 年次経路のperiodはティッカーによりint/str型が混在しうる
            # （実データ確認済み: 例えばLOARは"period"を文字列で保持）。
            # 同一ティッカー内で型が揃わない場合`dates[i] < dates[i+1]`が
            # TypeErrorを送出しうるため、順序規約違反と同様にWARNのみで
            # 処理を継続する（検証不能というだけで本番を止めない）。
            logging.warning(
                "calculate_fcf_cagr: FCFSeries検証中に型エラーを検知（WARNのみ、"
                "処理は継続）: %s", e
            )

    periods = len(recent_fcfs) - 1

    if start_value <= 0:
        return None

    raw_cagr = (end_value / start_value) ** (1 / periods) - 1

    # 範囲クリッピング
    clipped_cagr = max(growth_floor, min(growth_cap, raw_cagr))
    
    return GrowthResult(
        rate=clipped_cagr,
        source="fcf_cagr",
        cagr_detail={
            "start_value": start_value,
            "end_value": end_value,
            "periods": periods,
            "raw_cagr": raw_cagr,
            "clipped_cagr": clipped_cagr,
            "floor": growth_floor,
            "cap": growth_cap
        }
    )


def determine_growth_rate(
    ticker: str,
    fcf_list: Optional[List[float]] = None,
    default_rate: float = 0.25,
    fcf_dates: Optional[List[Any]] = None
) -> GrowthResult:
    """
    成長率を決定（優先順位付き）

    優先順位:
    1. セグメント加重成長率（設定がある場合）
    2. FCF CAGR（計算可能な場合）
    3. デフォルト値

    Args:
        ticker: 銘柄コード
        fcf_list: FCFリスト（オプション）
        default_rate: デフォルト成長率
        fcf_dates: fcf_listと対応する日付（オプション）。calculate_fcf_cagr()
            にそのまま渡す（[[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]）。
            segment_weighted経由で決定される場合はfcf_list/fcf_dates自体を
            参照しないため、このパラメータの有無は影響しない。

    Returns:
        GrowthResult: 決定された成長率
    """
    # 1. セグメント成長率を試行
    segment_result = get_segment_growth(ticker)
    if segment_result:
        return segment_result

    # 2. FCF CAGRを試行
    if fcf_list:
        cagr_result = calculate_fcf_cagr(fcf_list, fcf_dates=fcf_dates)
        if cagr_result:
            return cagr_result
    
    # 3. デフォルト
    return GrowthResult(
        rate=default_rate,
        source="default"
    )


if __name__ == "__main__":
    print("=== Growth Rate Calculator テスト ===\n")
    
    print(f"segment_config.py available: {HAS_SEGMENT_CONFIG}")
    
    # ケース1: セグメント設定あり（segment_config.pyが存在する場合）
    result1 = determine_growth_rate("NVDA")
    print(f"NVDA: {result1.rate:.1%} (source: {result1.source})")
    
    # ケース2: FCF CAGRフォールバック
    fcf_list = [100, 150, 200, 280, 350]
    result2 = determine_growth_rate("UNKNOWN", fcf_list)
    print(f"UNKNOWN (with FCF): {result2.rate:.1%} (source: {result2.source})")
    
    # ケース3: デフォルト
    result3 = determine_growth_rate("UNKNOWN")
    print(f"UNKNOWN (no FCF): {result3.rate:.1%} (source: {result3.source})")
