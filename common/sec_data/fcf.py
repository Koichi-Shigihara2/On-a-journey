"""
common/sec_data/fcf.py
FCFの定義の1か所（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段1、2026-11-02）

FCF＝営業CF − |CapEx|。ファイナンス・リースの支払いは足さない（引かない）。
（2026-11-01まではparser.py・ttm_calculator.py・layer3_builder.pyに「営業CF − max(0, |CapEx| − |リースの支払い|)」が
別々にあり、リースの支払いを足していた。CapExのタグにリースを含む銘柄は無い〈32銘柄を10-Kで確認、FINDINGS 2026-11-01〉）

読み手: parser.py（年次・四半期の cf.free_cash_flow）・ttm_calculator.py（TTMの flow.FCF）・
STONKS SILO（src/value/stonks_silo/fetcher.py の補完・analyzer.py の月次バーン）

CapExの欠け（None）は欠け（FCFもNone）。0とみなすのは「申告のCF計算書にCapExの行なし」の期だけで、
その判定と、Layer3・parserの年次に無い期を申告のCF計算書の行で埋める規則は common/sec_data/cf_lines.py の1か所
（段5b、2026-11-04。それまでは parser が欠けを0とみなし、TTMはNone、APPは TICKER_RESTRICTIONS でTTMのCapExを除外していた）。
STONKS SILOの月次バーン（analyzer.py）は年次ファイルの値を読み、欠けの年だけ0とみなす（年次ファイルは上の規則を当てた後の値）。
"""

from __future__ import annotations

from typing import Optional


# TTMの設備投資（4四半期の合計）が負の点の印（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段2、2026-11-03）。四半期は符号つき
# （YTDが前の四半期より減った期は負）で保存するため、合計が負になる点がある（FLYW TTM 2022-06-30・ONDS TTM 2025-06-30）。
# 負の設備投資は |CapEx| で引くと符号が逆になるため、その点の設備投資は欠け（None）にし、FCFも作らない（読み手は既存の代用）
CAPEX_TTM_NEGATIVE_MARK = "capex_ttm_negative"

# 申告のCF計算書の投資CFの区分に取得系の行（有形固定資産等・ソフトウェア）が無い期の印（段5b、2026-11-04）。
# その期のCapExは0（欠けではない）。判定は cf_lines.py（APPの2023年以降の申告等）
CAPEX_NO_CF_LINE_MARK = "CF計算書にCapExの行なし"


def ttm_capex(total: Optional[float]) -> Optional[float]:
    """TTMの設備投資（4四半期の合計）。負なら欠け（None）。判定はここの1か所"""
    if total is None or total < 0:
        return None
    return total


def free_cash_flow(ocf: Optional[float], capex: Optional[float]) -> Optional[float]:
    """FCF＝営業CF − |CapEx|。どちらかがNoneならNone（欠けを0とみなすかは呼び出し側）"""
    if ocf is None or capex is None:
        return None
    return ocf - abs(capex)
