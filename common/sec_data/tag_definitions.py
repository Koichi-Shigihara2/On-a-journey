"""
common/sec_data/tag_definitions.py
XBRLタグ候補の共通定義（LLY-CAPEX-STALE-1 Phase 2a・QUALITY-GATES-EPIC-1）

quarterly.py（四半期/TTM側）とparser.py（年次側）が独立にタグ候補リストを
管理しており、片方だけタグを追加すると四半期/年次で扱いが食い違う問題が
LLYのCapExで発生した（旧タグ PaymentsToAcquireProductiveAssets が2022年
Q3以降申告停止、新タグ PaymentsToAcquireOtherPropertyPlantAndEquipment への
切替が quarterly.py 側にしか（それも件数不足時のみ）反映されず、parser.py
側は新タグを一度も試行していなかった）。

このモジュールは、quarterly.py と parser.py 双方のタグ候補リストのうち
「優先順位・候補集合が完全に一致している」または「一方が他方の厳密な
上位集合になっている」フィールドのみを集約する。統合しても既存の
意図的な設計判断を壊さないフィールドに限定している。

【NET_INCOME拡張（EPS-ANALYZER-NORMALIZE-SCOPE-1、2026-07-20）】
src/value/adjusted_eps_analyzer/extract_key_facts.pyが独自に保持していた
NET_INCOME_ANNUAL_TAGS/NET_INCOME_QUARTERLY_TAGS（6タグ）をこのTAG_CANDIDATES
["NET_INCOME"]に統合した。既存3タグ（parser.py/quarterly.py実績検証済み、
順序変更なし）の末尾にEPS Analyzer固有の3タグを追加する方式を採用（全101銘柄
シミュレーションで既存3タグの選定結果より新しいデータを持つ銘柄は0件と確認済み
のため、parser.py/quarterly.py側の挙動への影響はない）。extract_key_facts.py
が内部に持っていた第3の候補リスト（net_income_priority、四半期後段で無条件
上書きする別優先順位）は本タグ集合と矛盾していたため削除し、単一の選定ロジック
（このTAG_CANDIDATES参照＋既存の「最新データを持つタグを採用」アルゴリズム）
に統一した。

【意図的に統合していないフィールド】（優先順位や候補集合が構造的に異なり、
無条件でのマージは既存の修正済みバグ・設計判断を壊すリスクがあるため。
詳細は BACKLOG.md [TAG-DEFS-UNIFY-1] 参照）:
- LTDebt/long_term_debt: 優先タグの順序が quarterly.py と parser.py で逆
  （parser.py は BUG-NETDEBT-2 対策で LongTermDebtNoncurrent を意図的に最優先）
- SM/selling_and_marketing: quarterly.py は SGA 全体への最終フォールバックを
  持つが、parser.py は SGA を sga_gap_warning 専用に意図的に分離している
- DA/depreciation_and_amortization: primary タグの優先順序が逆かつ
  parser.py は merge_all_tags、quarterly.py は単一タグのみで挙動が根本的に異なる
- RPO/rpo: current/noncurrent 区分が異なるタグが混在しており単純な合算は
  概念的に不正確になりうる
- Revenue/revenue: ティッカー別 revenue_concept オーバーライドと
  merge_all_tags の相互作用が複雑なため対象外
"""

TAG_CANDIDATES: dict[str, tuple[str, ...]] = {
    "CAPITAL_EXPENDITURE": (
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",       # NVDA等: 広義CapEx
        "PaymentsForCapitalImprovements",
        # LLY: 2023年以降の新タグ（LLY-CAPEX-STALE-1・旧タグは2022-09-30で申告停止）
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",
    ),
    "FINANCE_LEASE_PAYMENTS": (
        "FinanceLeasePrincipalPayments",
        "PaymentsForFinanceLeases",
        "RepaymentsOfLongTermCapitalLeaseObligations",
    ),
    "STOCK_BASED_COMPENSATION": (
        "ShareBasedCompensation",
        "AllocatedShareBasedCompensationExpense",  # CEG等
    ),
    "GROSS_PROFIT": (
        "GrossProfit",
        "GrossProfitLoss",
    ),
    "NET_INCOME": (
        "NetIncomeLoss",
        "ProfitLoss",                                        # AVGO等
        "NetIncomeLossAvailableToCommonStockholdersBasic",    # BKNG/AVAV等
        # EPS-ANALYZER-NORMALIZE-SCOPE-1（2026-07-20）: 以下3タグは
        # extract_key_facts.py固有だった候補。既存3タグの順序は変えず末尾に追加
        "NetIncomeLossAvailableToCommonStockholders",
        "NetIncomeLossAttributableToParent",
        "IncomeLossFromContinuingOperations",
    ),
    "CASH_AND_EQUIVALENTS": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",  # RCAT等
        "Cash",
        # [[CASH-TAG-MISSING-1]]対応（2026-08-30）: ASU 2016-18（制限付き
        # 現金を含むキャッシュフロー期首・期末残高調整表示義務化）対応後に
        # 移行した企業向け。GEV・SITM等、上記3タグのいずれも報告していない
        # 銘柄のcash_and_equivalentsが完全欠落していたことを解消する。
        # 「制限付き現金」を含む定義のため、純粋な現金同等物より広い概念
        # である点に注意。CPRT・HEIは上記CashAndCashEquivalentsAtCarrying
        # Valueが引き続き機能しているにもかかわらず、このタグの方が
        # 直近年度まで新しく報告されているため
        # _extract_values_best_candidate()の「最新annual年が新しい候補が
        # 全期間の採用タグになる」設計により、機能していた期間まで
        # 過大計上（制限付き現金の混入）に置き換わってしまうリスクを
        # 全105銘柄シミュレーションで確認した。両銘柄はquarterly.py::
        # TICKER_RESTRICTIONSのcash_concept上書きで明示的に既存タグへ
        # 固定し、このリスクを回避している。
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ),
    "RESEARCH_AND_DEVELOPMENT": (
        # [[JNJ-RD-TAG-PRIORITY-1]]対応: ExcludingAcquiredInProcessCostを
        # ResearchAndDevelopmentExpenseより優先する。JNJは両タグを2023年以降
        # 並存報告しており、後者はキャッシュフロー計算書の非資金調整項目
        # 「Charge for purchase of in-process research and development assets」
        # （M&A時のIPR&D即時費用化、通常のR&D活動とは無関係の一時的項目）を指す
        # ため、損益計算書本体の主要R&D科目である前者を優先する必要がある
        # （10-K原本R5.htm/R10.htm、FY2023-2025の3期で確認済み）。
        "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
        "ResearchAndDevelopmentExpense",
        "CapitalizedComputerSoftwareDevelopmentCosts",
        "CapitalizedComputerSoftwareAmortization1",  # UNH等
    ),
    "BUYBACK": (
        "PaymentsForRepurchaseOfCommonStock",
    ),
    "OPERATING_CASH_FLOW": (
        "NetCashProvidedByUsedInOperatingActivities",
    ),
}


# ---------------------------------------------------------------------------
# net_income: 親会社帰属を正とする共通ルール
# （[[NET-INCOME-NCI-PARENT-ATTRIBUTION-1]]、2026-09-24）
# ---------------------------------------------------------------------------
# 従来はTAG_CANDIDATES["NET_INCOME"]の2番目にProfitLoss（非支配持分〈NCI〉
# 込みの連結純利益）があり、親会社帰属タグが無い期間にNCI込みの値を採用して
# いた（FCXでは親会社帰属2,204Mに対し連結4,152M〈FY2025、10-K R3で確認〉）。
#
# 対応: 候補の優先順位と各系統の選定アルゴリズム（年度単位の選定／系列
# 丸ごとの置換／タグ単位正規化後の期間マージ）は変えず、NCIを含みうる
# 連結系タグ（NET_INCOME_CONSOLIDATED_TAGS）だけを「同じ期間・同じaccnの
# NCIを差し引いた派生概念」に置き換える。これによりどの候補が選ばれても
# 値は親会社帰属になり、NCIのない銘柄の挙動は変わらない。
# （検討した別案: ①候補順の入れ替え→quarterly.pyの系列丸ごと置換で
# AVAVの直近6四半期が欠落、②全候補を期間単位で1概念に統合→Layer3の
# タグ単位フォールバックが効かなくなりDDOGの2024Q4〈NetIncomeLossに1か月
# スタブあり〉が消失。いずれも回帰のため不採用）
#
# 派生概念の値（ProfitLoss等の各ファクトについて、期間×accnごと）:
#   - 同じ期間・同じaccnに親会社帰属タグ（NET_INCOME_PARENT_TAGS）の値が
#     ある → その値（提出者自身が報告した親会社帰属額。FCXのFY2021 10-Kは
#     NCIを符号逆〈-1,059M〉でタグ付けしており、機械的な控除では誤るため）
#   - 同じ期間・同じaccnにNCI（NET_INCOME_NCI_TAG）がある → 差し引いた値
#   - そのaccnにNCIの申告が1件も無い（NCIのない企業・提出書類）→ そのまま
#   - そのaccnにNCIの申告はあるが当該期間の値が無い → 推測で埋めず除外
#
# parser.py（annual/quarterly JSON）・quarterly.py（normalized/）・
# layer3_builder.py（Layer3/ttm/）はNET_INCOME_CANDIDATESを唯一の候補定義
# として参照し、company_factsを読んだ直後にwith_derived_net_income()で
# 派生概念を追加する。EPS Analyzer（extract_key_facts.py）は本ルールの
# 対象外で、引き続きTAG_CANDIDATES["NET_INCOME"]を独自ロジックで使う。
NET_INCOME_NCI_TAG = "NetIncomeLossAttributableToNoncontrollingInterest"
# 親会社帰属を直接表すタグ（TAG_CANDIDATES["NET_INCOME"]の優先順）
NET_INCOME_PARENT_TAGS: tuple[str, ...] = (
    "NetIncomeLoss",
    "NetIncomeLossAvailableToCommonStockholdersBasic",
    "NetIncomeLossAvailableToCommonStockholders",
    "NetIncomeLossAttributableToParent",
)
# 連結系タグ → NCI控除後の派生概念名（us-gaapに実在しない名前。呼び出し元が
# company_factsのコピーへ追加するだけで、ファイル・他の読み手には現れない）
NET_INCOME_CONSOLIDATED_TAGS: dict[str, str] = {
    "ProfitLoss": "ProfitLossAttributableToParentDerived",
    "IncomeLossFromContinuingOperations": "IncomeLossFromContinuingOperationsAttributableToParentDerived",
}
# 3系統共通のnet_income候補（優先順位順）。TAG_CANDIDATES["NET_INCOME"]と
# 同じ順序で、連結系タグだけを派生概念に差し替えたもの
NET_INCOME_CANDIDATES: tuple[str, ...] = tuple(
    NET_INCOME_CONSOLIDATED_TAGS.get(tag, tag) for tag in TAG_CANDIDATES["NET_INCOME"]
)
# 派生概念の元になる生のXBRLタグ（kpi_proposer等のタグ→フィールド対応用）
NET_INCOME_SOURCE_TAGS: tuple[str, ...] = TAG_CANDIDATES["NET_INCOME"]


def derive_nci_adjusted_facts(us_gaap: dict, tag: str, unit: str = "USD") -> list:
    """連結系タグtagのファクトを、上記ルールでNCI控除後の値に変換して返す。

    各ファクトは元ファクトの写し（start/end/accn/fy/fp/form/filed/frame等を
    保持）で、valのみ置き換わる。期間を持たない（instant）ファクトは対象外。
    """
    def units(t):
        try:
            return us_gaap[t]["units"][unit] or []
        except (KeyError, TypeError):
            return []

    parent_by_key: dict = {}
    for ptag in NET_INCOME_PARENT_TAGS:
        for f in units(ptag):
            if f.get("start") and f.get("val") is not None:
                parent_by_key.setdefault((f["start"], f["end"], f.get("accn")), f["val"])

    nci_by_key: dict = {}
    nci_accns: set = set()
    for f in units(NET_INCOME_NCI_TAG):
        if not f.get("start") or f.get("val") is None:
            continue
        nci_accns.add(f.get("accn"))
        nci_by_key.setdefault((f["start"], f["end"], f.get("accn")), f["val"])

    out = []
    for f in units(tag):
        if not f.get("start") or f.get("val") is None:
            continue
        key = (f["start"], f["end"], f.get("accn"))
        if key in parent_by_key:
            out.append({**f, "val": parent_by_key[key]})
        elif key in nci_by_key:
            out.append({**f, "val": f["val"] - nci_by_key[key]})
        elif f.get("accn") in nci_accns:
            continue  # NCIの申告がある提出書類で当該期間のNCIが無い → 推測しない
        else:
            out.append(dict(f))
    return out


def with_derived_net_income(company_facts: dict) -> dict:
    """company_factsのコピーを返し、us-gaapにNCI控除後の派生概念を追加する。

    元のcompany_facts（ファイルから読んだdict）は変更しない。
    """
    if not isinstance(company_facts, dict):
        return company_facts
    facts = company_facts.get("facts") or {}
    us_gaap = facts.get("us-gaap") or {}
    new_us_gaap = dict(us_gaap)
    for tag, derived in NET_INCOME_CONSOLIDATED_TAGS.items():
        if tag in us_gaap:
            new_us_gaap[derived] = {
                "label": f"{tag} attributable to parent (derived, NCI deducted)",
                "units": {"USD": derive_nci_adjusted_facts(us_gaap, tag)},
            }
    return {**company_facts, "facts": {**facts, "us-gaap": new_us_gaap}}
