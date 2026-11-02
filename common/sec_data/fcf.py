"""
common/sec_data/fcf.py
FCFの定義の1か所（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段1、2026-11-02）

FCF＝営業CF − |CapEx|。ファイナンス・リースの支払いは足さない（引かない）。
（2026-11-01まではparser.py・ttm_calculator.py・layer3_builder.pyに「営業CF − max(0, |CapEx| − |リースの支払い|)」が
別々にあり、リースの支払いを足していた。CapExのタグにリースを含む銘柄は無い〈32銘柄を10-Kで確認、FINDINGS 2026-11-01〉）

読み手: parser.py（年次・四半期の cf.free_cash_flow）・ttm_calculator.py（TTMの flow.FCF）・
STONKS SILO（src/value/stonks_silo/fetcher.py の補完・analyzer.py の月次バーン）

CapExの欠け（None）の扱いは呼び出し側が決める（今は parser と STONKS SILO の月次バーンが0とみなし、TTMはNone。
1つの規則にするのは段5〈申告のXBRLインスタンスで「CF計算書にCapExの行なし」を判定〉）。
"""

from __future__ import annotations

from typing import Optional


def free_cash_flow(ocf: Optional[float], capex: Optional[float]) -> Optional[float]:
    """FCF＝営業CF − |CapEx|。どちらかがNoneならNone（欠けを0とみなすかは呼び出し側）"""
    if ocf is None or capex is None:
        return None
    return ocf - abs(capex)
