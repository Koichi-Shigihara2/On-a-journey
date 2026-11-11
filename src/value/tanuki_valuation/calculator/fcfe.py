"""
TANUKI VALUATION - FCFE Equity DCF Calculator
金融機関（銀行持株会社等）向けFCFEエクイティDCF（参考表示専用）

責務: FCFF（企業DCF）が適合しにくい金融機関銘柄向けに、業界標準の
FCFEベースのエクイティDCFを計算する。[[TANUKI-FIN-2]]確定方針により、
report.txt上の参考表示としてのみ追加し、TANUKI SCORE・upside_percent・
verdict等の判定ロジックには一切使わない。

設計方針（[[TANUKI-FIN-2]]）:
- 銘柄非依存の共通コードとして1つだけ実装する。ticker別に異なる計算式・
  分岐は書かない。対象ティッカー（現状SOFIのみ、JPM/GSは登録されると自動）は
  業種のマスター（config/sector_master.json）の financial で決める
  （[[SECTOR-MASTER-ADOPT-1]] 段3。以前の config/financial_institution_config.json は廃止）
- DDM（配当割引モデル）は不採用（SOFIが無配のため原理的に適用不可）
- Cost of Equityは既存のCAPM（calculate_wacc()）を再利用する。
  calculate_wacc()は実装上、負債/自己資本の加重平均を一切行わない
  純粋なCAPM（Rf + β×ERP）であり、既にCost of Equityと数学的に同一
  （投資調査で確認済み）。よって本モジュールはCost of Equityを再計算
  せず、呼び出し元（pipeline.py）が渡すvaluation["wacc"]["value"]を
  そのまま使う
- 高成長期年数・成長率・DCF構造（2段階/3段階）は既存のgrowth決定
  ロジック（calculator.growth）・maturity_config.jsonが既に算出した
  値をそのまま再利用する。DCF計算本体も既存のcalculate_two_stage_dcf()
  をそのまま呼ぶ（新規のDCF数式は実装しない）

計算式（Damodaran方式、金融機関のFCFE代理指標）:
    Equity Reinvestment Rate = g / ROE
    FCFE = Net Income × (1 - Equity Reinvestment Rate)

    ROE <= 0 の場合、Equity Reinvestment Rateは定義不能（分母ゼロ/負）
    のためavailable=Falseを返す。
    g > ROE の場合（成長率がROEを上回る）、Equity Reinvestment Rate > 1
    となりFCFEはマイナスになる。これは「現在の高成長率を内部留保だけ
    では賄えず継続的な増資が前提」であることを意味する経済的に正しい
    結果だが、理論株価としては意味をなさないため、available=Falseを
    返しreasonで理由を明示する（マイナス値を無理にDCFへ通さない）。
"""

from typing import Any, Dict, Optional

from .dcf import calculate_two_stage_dcf, DCFResult

DEFAULT_TERMINAL_GROWTH = 0.03


def is_financial_institution_ticker(ticker: str) -> bool:
    """tickerがFCFEエクイティDCF計算パスの対象かを判定する。
    業種のマスター（config/sector_master.json）の financial が true で、保険でない銘柄
    （[[SECTOR-MASTER-ADOPT-1]] 段3。以前は config/financial_institution_config.json の手書きの一覧〈SOFIだけ〉で、廃止した）。
    保険（industry が Insurance）は FCFE の想定外なので除く。JPM・GS は登録されるとマスターで自動的に対象になる
    （登録後に FCFE の結果とマスターの区分を目視する）。"""
    from common.sector_master import master_entry, is_financial_ticker
    if not is_financial_ticker(ticker):
        return False
    industry = ((master_entry(ticker) or {}).get("industry") or "").lower()
    return "insurance" not in industry


def calculate_equity_reinvestment_rate(growth_rate: float, roe: float) -> Optional[float]:
    """Equity Reinvestment Rate = g / ROE

    ROEが0以下の場合はROICが定義不能（自己資本コストを回収できない/
    自己資本がマイナス）なため、Noneを返す。
    """
    if roe is None or roe <= 0:
        return None
    return growth_rate / roe


def calculate_fcfe(net_income: float, equity_reinvestment_rate: float) -> float:
    """FCFE = Net Income × (1 - Equity Reinvestment Rate)"""
    return net_income * (1 - equity_reinvestment_rate)


def calculate_fcfe_valuation(
    net_income: float,
    growth_rate: float,
    roe: float,
    cost_of_equity: float,
    diluted_shares: float,
    high_growth_years: int = 5,
    terminal_growth: float = DEFAULT_TERMINAL_GROWTH,
) -> Dict[str, Any]:
    """FCFEベースのエクイティDCFを計算する（銘柄非依存の共通関数）。

    既存のcalculate_two_stage_dcf()（企業DCFと同一の関数）を、
    base_fcf=FCFE・wacc=cost_of_equityとして呼ぶことで、DCF計算本体を
    再利用する（新規のDCF数式は実装しない）。

    Args:
        net_income: 直近TTM純利益（dupont分解のni_ttmを想定）
        growth_rate: 高成長期の成長率（TANUKI本体のgrowth.rateを想定）
        roe: 自己資本利益率（dupont分解のroe_decomposedを想定）
        cost_of_equity: 株主資本コスト（TANUKI本体のwacc.valueを想定。
            calculate_wacc()は負債加重を行わない純粋なCAPMのため、
            既存WACC値をそのままCost of Equityとして再利用できる）
        diluted_shares: 希薄化後発行済株式数
        high_growth_years: 高成長期年数（TANUKI本体のgrowth.phase1_years
            を想定）
        terminal_growth: 永続成長率（デフォルト3%、maturity_config.json
            の_defaultと同値）

    Returns:
        dict: available=Trueの場合はintrinsic_value_per_share等を含む。
            available=Falseの場合はreasonのみで理論株価は含めない
            （report.txt側は理由付きでN/A表示する）。
    """
    equity_reinvestment_rate = calculate_equity_reinvestment_rate(growth_rate, roe)

    base_detail = {
        "net_income": net_income,
        "growth_rate": growth_rate,
        "roe": roe,
        "cost_of_equity": cost_of_equity,
        "equity_reinvestment_rate": equity_reinvestment_rate,
    }

    if equity_reinvestment_rate is None:
        return {
            "available": False,
            "reason": "roe_not_positive",
            **base_detail,
        }

    fcfe = calculate_fcfe(net_income, equity_reinvestment_rate)
    base_detail["fcfe"] = fcfe

    if fcfe <= 0:
        return {
            "available": False,
            "reason": "negative_or_zero_fcfe",
            **base_detail,
        }

    if cost_of_equity <= terminal_growth:
        return {
            "available": False,
            "reason": "cost_of_equity_below_terminal_growth",
            **base_detail,
        }

    if not diluted_shares or diluted_shares <= 0:
        return {
            "available": False,
            "reason": "diluted_shares_unavailable",
            **base_detail,
        }

    dcf_result: DCFResult = calculate_two_stage_dcf(
        base_fcf=fcfe,
        high_growth_rate=growth_rate,
        wacc=cost_of_equity,
        high_growth_years=high_growth_years,
        terminal_growth=terminal_growth,
    )
    intrinsic_value_per_share = dcf_result.v0 / diluted_shares

    return {
        "available": True,
        "method": "FCFE_equity_dcf",
        "intrinsic_value_total": dcf_result.v0,
        "intrinsic_value_per_share": intrinsic_value_per_share,
        "high_growth_years": high_growth_years,
        "terminal_growth": terminal_growth,
        "dcf_components": dcf_result.to_dict(),
        **base_detail,
    }
