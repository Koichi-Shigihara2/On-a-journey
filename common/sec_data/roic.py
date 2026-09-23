"""
common/sec_data/roic.py

ROIC（投下資本利益率）計算の共有モジュール。
[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]④実装（2026-09-23）で、
`src/value/tanuki_valuation/pipeline.py::_calc_roic_wacc_ratio()`から
切り出した。TANUKI VALUATION（Moat Score・RICE-1 vc_factor向け、最新
1年のみ）・EPS Analyzer（経営者実行力評価の参考指標向け、複数年の
トレンド）の両方から呼び出される。

ROIC = NOPAT / Invested_Capital
NOPAT = Operating_Income × (1 - 21%)（実効税率固定）
Invested_Capital = Equity + Net_Debt（総資本 - 現金）
"""
import json
import os
from typing import Callable, Optional


def calc_roic_wacc_ratio(
    ticker: str,
    repo_root: str,
    wacc_rm: float = 0.10,
    estimate_ttm_operating_income_fn: Optional[Callable[[str], Optional[float]]] = None,
    get_lt_debt_fallback_fn: Optional[Callable[[str], float]] = None,
    year: Optional[int] = None,
) -> tuple:
    """年次データから ROIC/WACC_Rm を計算（RICE-1 価値創造係数）

    戻り値: (ROIC/wacc_rm または None, 理由コード)
    （例: ROIC=15%・WACC=10% → (1.5, "ok")）

    Args:
        ticker: 銘柄コード
        repo_root: リポジトリルート（`common/sec_data/data/{ticker}/`の
                    親を特定するため）
        wacc_rm: WACC（Rmベース、デフォルト10%）
        estimate_ttm_operating_income_fn: operating_incomeが取得不能な
                    場合のTTM代替算出コールバック（例: TANUKI VALUATION
                    のLayer3ベースフォールバック）。Noneの場合はフォール
                    バックせず"no_operating_income"を返す
        get_lt_debt_fallback_fn: long_term_debtが欠落した場合の代替値
                    コールバック。Noneの場合はフォールバックせず0扱い
                    （呼び出し元が意図的にLayer3等を持たない場合向け）
        year: 対象年度を明示指定する場合（複数年トレンド計算向け）。
                    Noneの場合は最新年度を自動選択する（従来動作）

    [[MOAT-SCORE-PARTIAL-NULL-1]]: 理由コードはcalculate_moat_score()が
    Noneの原因別に扱い（真の赤字は算入、それ以外は除外）を判定するために
    使う。RICE-1側（vc_factor）は理由コードを見ず、値がNoneかどうかのみで
    判定するため、本変更による既存挙動への影響はない。

    理由コード一覧:
      "ok"                      - 正常算出
      "reported_negative_oi"    - operating_incomeは取得できたがNOPAT<=0（真の赤字）
      "no_operating_income"     - operating_income自体が取得不能（標準タグ・
                                   parser.py再構成〈[[OPERATING-INCOME-
                                   EXTRACTION-GAP-1]]〉・TTMフォールバック
                                   いずれも失敗。2026-08-16時点で該当0件だが
                                   将来発生しうる真の欠損）
      "missing_equity_data"/"missing_cash_data" - BS項目欠損
      "negative_invested_capital" - 投下資本が非正（自社株買い等で自己資本が
                                   大幅マイナスの銘柄。実際に赤字なのではなく
                                   測定不能なだけ）
      "roic_non_positive"       - ROICが0以下（invested_capital算出後の異常値）
      "roic_diverged_over10"    - ROICが1000%以上（測定アーティファクトの
                                   疑い。2026-08-16時点で該当0件、未検証）
      "no_sec_dir"/"no_annual_data"/"exception" - データ自体が存在しない
      "year_not_found"          - `year`を明示指定したが該当年度のannual
                                   JSONが存在しない
    """
    sec_dir = os.path.join(repo_root, "common", "sec_data", "data", ticker)
    if not os.path.exists(sec_dir):
        return None, "no_sec_dir"
    years = sorted([
        int(fn[7:11]) for fn in os.listdir(sec_dir)
        if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
    ])
    if not years:
        return None, "no_annual_data"
    target_year = year if year is not None else years[-1]
    if year is not None and year not in years:
        return None, "year_not_found"
    try:
        with open(os.path.join(sec_dir, f"annual_{target_year}.json"), encoding="utf-8") as f:
            ann = json.load(f)
        pl = ann.get("pl", {})
        bs = ann.get("bs", {})
        # [[OPERATING-INCOME-EXTRACTION-GAP-1]]: `or 0`によるNone→0
        # のすり替えは、真の欠損（未報告）と真の赤字（NOPAT<=0）を
        # 区別できなくしていた。`parser.py::_backfill_operating_income()`
        # がGP-R&D-SGA法・pretax調整法で既にannual_YYYY.json側を
        # 補完しているため、ここでNoneになるのは両方式とも失敗した
        # 真の欠損のみ。
        oi = pl.get("operating_income")
        if oi is None and estimate_ttm_operating_income_fn is not None:
            oi = estimate_ttm_operating_income_fn(ticker)
        if oi is None:
            return None, "no_operating_income"
        nopat = oi * (1 - 0.21)
        if nopat <= 0:
            # ここに到達する時点でoiはNoneではない（真に報告/再構成された
            # 値）ため、赤字と判定してよい（[[MOAT-SCORE-PARTIAL-NULL-1]]
            # のroic_reason="reported_negative_oi"に対応）。
            return None, "reported_negative_oi"
        # FY52WEEK-BS-NULL-SILENT-1 Phase A: stockholders_equity/
        # cash_and_equivalentsはNone率がほぼ0-4%（全105銘柄実測）で、
        # Noneはほぼ確実にデータ異常のシグナル。従来は`or 0`で暗黙に
        # ゼロ化してinvested_capitalを算出し続けていたが、DuPont分解
        # と同じ「除外」方針に倣いNoneを返す（既存のinvested_capital<=0
        # →None→VC_Factor=1.0フォールバック安全弁はそのまま維持）。
        # long_term_debt/short_term_debtは真のゼロとの判別困難のため
        # 対象外（Phase B/C、従来通り`or 0`を維持）。
        _equity_se = bs.get("stockholders_equity")
        _equity_te = bs.get("total_equity")
        _cash_raw = bs.get("cash_and_equivalents")
        if _equity_se is None and _equity_te is None:
            return None, "missing_equity_data"
        if _cash_raw is None:
            return None, "missing_cash_data"
        equity = _equity_se or _equity_te or 0
        lt_debt_fallback = get_lt_debt_fallback_fn(ticker) if get_lt_debt_fallback_fn is not None else 0.0
        lt_debt = bs.get("long_term_debt") or lt_debt_fallback
        st_debt = bs.get("short_term_debt") or 0
        cash = _cash_raw
        invested_capital = equity + lt_debt + st_debt - cash
        if invested_capital <= 0:
            return None, "negative_invested_capital"
        roic = nopat / invested_capital
        if roic <= 0:
            return None, "roic_non_positive"
        if roic >= 10.0:
            return None, "roic_diverged_over10"
        return roic / wacc_rm, "ok"
    except Exception:
        return None, "exception"
