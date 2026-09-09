"""
SEC XBRL データパーサー
Company Facts 生データを正規化された年次/四半期データに変換
"""

import json
import os
from datetime import datetime
from typing import Optional, Dict, Any, List

from .config import get_ticker_info
from .quarterly import TICKER_RESTRICTIONS, _classify_period
from .normalizer import _ytd_to_quarterly
from .tag_definitions import TAG_CANDIDATES
from .utils import (
    determine_fiscal_year, detect_fiscal_end_month, detect_fiscal_anchor_date,
    detect_fiscal_anchor_clusters, _day_of_year,
)
from .fetcher import load_submissions, load_former_names

_FACT_OVERRIDES_PATH = os.path.join(os.path.dirname(__file__), "fact_overrides.json")
_FIXED_REGISTRY_PATH = os.path.join(os.path.dirname(__file__), "fixed_registry.json")

# SECDATA-STORAGE-FRAGMENTATION-1 normalized/→data/統合: quarterly_{FYQ}.jsonの
# pl/cf/shares区分がYTD累積値のまま保存されていた問題への対応（統一アルゴリズム）。
# 加重平均フィールド（差分計算が数学的に無効）は差分計算フォールバックの対象外とし、
# SA(単一四半期)候補が存在しない場合はその四半期を欠損のまま許容する。
_QUARTERLY_NO_DIFF_FIELDS = frozenset({"shares_diluted", "shares_basic"})


def _pick_quarterly_period_representative(candidates: List[tuple]) -> Optional[Dict[str, Any]]:
    """同一(fy, fp)内の複数候補（当期・比較年度再掲・重複タグ等）から代表エントリを
    1件選ぶ。

    優先順位: ①SA（単一四半期相当。quarterly.py::_classify_period()の
    is_ytd=Falseかつis_annual=False）を優先 ②end_dateが最新の方を優先
    （当期データが比較年度再掲より優先されるようにする）③end_date同点時は
    期待日数91日への近さを優先（SEC-TAG-FICO-CPRT-1と同種のタイブレーク。
    RCAT等で確認された、同一end_dateを持つ極端に短い縮退エントリの誤採用を防ぐ）。

    candidates: [(start, end, val, fp), ...]
    戻り値: 代表エントリのdict（start/end/val/fp/is_ytd/period_days）。
            候補が全てis_annual判定される等で空になった場合はNone。
    """
    classified = []
    for start, end, val, fp in candidates:
        cls = _classify_period(start, end, fp, "10-Q")
        if cls["is_annual"]:
            continue
        classified.append({
            "start": start, "end": end, "val": val, "fp": fp,
            "is_ytd": cls["is_ytd"], "period_days": cls["period_days"],
        })
    if not classified:
        return None

    sa_candidates = [c for c in classified if not c["is_ytd"]]
    pool = sa_candidates if sa_candidates else classified

    best = None
    for c in pool:
        if best is None or c["end"] > best["end"]:
            best = c
        elif c["end"] == best["end"] and abs(c["period_days"] - 91) < abs(best["period_days"] - 91):
            best = c
    return best


def _resolve_quarterly_values(quarterly_candidates: List[tuple], field_name: str) -> Dict[str, Any]:
    """収集した四半期生候補群から単一四半期(SA)値を確定する統一アルゴリズム。

    [[SECDATA-STORAGE-FRAGMENTATION-1]] normalized/→data/統合の一環（2026-08-05）。
    quarterly_{FYQ}.jsonのpl/cf/shares区分がYTD累積値のまま保存されていた問題
    （約65〜66%のエントリが該当）への対応。normalized/側で実績のある
    quarterly.py::_classify_period()・normalizer.py::_ytd_to_quarterly()を
    そのまま再利用し、ロジックの二重実装を避ける（annual側day変数判定
    〈340-380日必須〉とは対象とする期間種別が異なるため共通化していないが、
    「期間日数で期間種別を判定する」という考え方自体は同一）。

    手順:
      ①同一(fy, fp)ごとに代表エントリを1件選定（_pick_quarterly_period_
        representative()、SA優先）
      ②fy単位でend_date昇順のチェーンを構築
      ③_ytd_to_quarterly()でYTDエントリを差分変換する
        （SA優先のため、実際に差分計算が発火するのはSA候補が存在しない
        フィールド〈operating_cash_flow・stock_based_compensation等〉
        の四半期のみ。SA候補が存在する四半期はそのまま通る）
      ④shares_diluted等の加重平均フィールド（_QUARTERLY_NO_DIFF_FIELDS）は
        差分計算が数学的に無効なため③をスキップし、SA代表のみ採用する
        （SA候補がない四半期はキー自体を発行せず欠損のまま許容する）

    quarterly_candidates: [(fy, fp, start, end, val), ...]
    戻り値: {"{fy}{fp}": val, ...}（差分不能・SA候補なしの四半期はキー自体が
            存在しない＝呼び出し元のresult["quarterly"]はその四半期を更新しない）
    """
    by_period: Dict[tuple, list] = {}
    for fy, fp, start, end, val in quarterly_candidates:
        by_period.setdefault((fy, fp), []).append((start, end, val, fp))

    by_fy: Dict[Any, list] = {}
    for (fy, fp), cands in by_period.items():
        rep = _pick_quarterly_period_representative(cands)
        if rep is None:
            continue
        by_fy.setdefault(fy, []).append(rep)

    resolved: Dict[str, Any] = {}
    no_diff = field_name in _QUARTERLY_NO_DIFF_FIELDS
    for fy, plist in by_fy.items():
        plist_sorted = sorted(plist, key=lambda p: p["end"])
        if no_diff:
            for p in plist_sorted:
                if not p["is_ytd"]:
                    resolved[f"{fy}{p['fp']}"] = p["val"]
            continue
        converted, _unresolved = _ytd_to_quarterly(plist_sorted)
        for c in converted:
            resolved[f"{fy}{c['fp']}"] = c["val"]
    return resolved


def _load_fixed_registry() -> dict:
    """fixed_registry.json（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]の
    フィックス機構。検証済み・確定済みのticker×年度について、以後の
    抽出ロジック変更の影響を受けないよう既存annual_{year}.jsonの値へ
    強制復元する）を読み込む。fact_overrides.jsonと同型のロード方式。
    ファイル不在時・例外時は空dict。
    """
    if os.path.exists(_FIXED_REGISTRY_PATH):
        try:
            with open(_FIXED_REGISTRY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _load_fact_overrides() -> dict:
    """fact_overrides.json（法人再編に伴う遡及修正で会計基準自体が変わった
    特定ティッカー・年度・フィールドの個別上書き設定）を読み込む。

    CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1: GOOGL FY2012/2013のように、本人データ
    優先ロジックが「当初申告値」を採用してしまうが、後年の遡及修正
    （非継続事業区分変更等）により会計基準自体が変わっているケース向け。
    ticker_overrides（fcf_conversion_config.json）と同型の、ticker+年度+
    フィールド単位の明示的な手動リスト方式。閾値による自動判定は行わない。
    ファイル不在時は空dict。
    """
    if os.path.exists(_FACT_OVERRIDES_PATH):
        try:
            with open(_FACT_OVERRIDES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


class SECParser:
    """SEC Company Facts データパーサー"""
    
    # タグをまたいでデータをマージするフィールド（早期終了しない）
    # 企業によって年代ごとに異なるXBRLタグを使うフィールドを列挙する
    # 例: GOOGLはFY2022-2024に RevenueFromContractWithCustomerExcludingAssessedTax,
    #         FY2025に Revenues を使用しており、早期終了するとFY2022-2024が欠落する
    MERGE_ALL_TAGS_FIELDS = {"revenue", "selling_and_marketing", "depreciation_and_amortization"}

    # instant fact（単一時点end_dateのみを持ち、start_dateを持たないXBRL概念）
    # のフィールド。BS9項目 + RPO残高。本人データ判定は_collect_own_data_instant()
    # （start_date不要版）を使う（ARCH-DATA-1残課題④: _collect_own_data_annual()は
    # start_date必須フィルタを持つためinstant factを常に除外していた）
    INSTANT_FACT_FIELDS = {
        "total_assets", "stockholders_equity", "total_liabilities",
        "cash_and_equivalents", "short_term_investments",
        "long_term_debt", "short_term_debt",
        "current_assets", "current_liabilities",
        "rpo",
    }

    # [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1で対象とするBSフィールド。
    # SPAC合併等で同一年度のBS(instant fact)が異なる法的実体（accn）から
    # 混在採用され、current_assets>total_assets等の数学的矛盾を起こす
    # ケースの是正対象。short_term_investments/rpoは調査スコープ外のため含めない。
    _BS_ENTITY_MIXING_FIELDS = (
        "total_assets", "stockholders_equity", "total_liabilities", "cash_and_equivalents",
        "long_term_debt", "short_term_debt", "current_assets", "current_liabilities",
    )

    # [[PERIOD-LENGTH-VALIDATION-GAP-1]]対応: _extract_single_key()（MERGE_ALL_
    # TAGS_FIELDS以外の全FLOW型フィールドが経由する候補選定）で、年次候補として
    # 受理する際に期間長(340-380日)を必須条件とするフィールド。GrossProfit等の
    # タグが主要財務諸表になく「四半期実績（未監査）」注記にのみ存在する企業で、
    # 91日程度の四半期値が年次値として誤採用される構造的バグへの対応
    # （TDY/AVGO/CPRT/ABBV/CAT/FICO/HEI/HON/KLAC/MRVL/COHR/INTU等で実害確認済み、
    # 105銘柄×全対象フィールドのオフラインシミュレーションで安全性確認済み）。
    # 対象は全母集団シミュレーション済みの9フィールドに限定し、同じ
    # _extract_single_key()を経由する他フィールド（eps_diluted/eps_basic・
    # buyback・finance_lease_payments・shares_diluted/shares_basic）は
    # 未シミュレーションのため対象外のまま据え置く。
    PERIOD_LENGTH_VALIDATED_FIELDS = {
        "gross_profit", "cost_of_revenue", "net_income", "operating_income",
        "research_and_development", "selling_general_and_administrative",
        "operating_cash_flow", "capital_expenditure", "stock_based_compensation",
    }

    # XBRL項目マッピング（優先順位順）
    XBRL_MAPPING = {
        # BS（貸借対照表）
        "total_assets": [
            "Assets",
        ],
        "stockholders_equity": [
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        ],
        "total_liabilities": [
            "Liabilities",
            "LiabilitiesAndStockholdersEquity",
        ],

        # BS詳細（ネットキャッシュ計算用）
        # tag_definitions.pyのTAG_CANDIDATESから取得する（LLY-CAPEX-STALE-1 Phase 2a）
        "cash_and_equivalents": list(TAG_CANDIDATES["CASH_AND_EQUIVALENTS"]),
        "short_term_investments": [
            "ShortTermInvestments",
            "MarketableSecuritiesCurrent",
            "AvailableForSaleSecuritiesCurrent",
            "AvailableForSaleSecurities",
            # FY52WEEK-BS-NULL-SILENT-1 Phase B Stage1（2026-07-19追加）:
            # CASH-TAG-MISSING-1と同型のタグ網羅漏れ。CECL(ASU 2016-13)taxonomy
            # 対応後に多くの企業が移行した"Current"明示タグを追加。10-K原本で
            # BS本体の「Marketable securities/Short-term investments」流動資産行
            # と一致することを個別確認済み（ALAB/BBAI/CRM/DDOG/GTLB/INTU/IOT/KO/
            # NET/NOW/RBRK/RMBS/SITM/VRT/ZS）。KLAC/NVDA/SOFI/TER/Vは合算タグでは
            # 過大/過小評価となるため対象外（[[FY52WEEK-BS-STI-OVERRIDE-DESIGN-1]]
            # で銘柄別override設計を別途検討）。CAT/LLYはBS本体に科目行自体が
            # 存在しない（footnote専用）ため対象外。
            "AvailableForSaleSecuritiesDebtSecuritiesCurrent",
            "DebtSecuritiesAvailableForSaleExcludingAccruedInterestCurrent",
            "DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent",
            "OtherShortTermInvestments",
        ],
        "long_term_debt": [
            # LongTermDebtNoncurrent を優先する。
            # LongTermDebt は current+non-current の合計値のため、LongTermDebtCurrent と
            # 組み合わせると Total_Debt が二重計上される（BUG-NETDEBT-2）。
            "LongTermDebtNoncurrent",
            "LongTermDebt",
            "LongTermNotesPayable",
            "SeniorNotes",
            # FY52WEEK-BS-NULL-SILENT-1 Phase B Stage1（2026-07-19追加）:
            # AVGO型（VMware買収後にLongTermDebtNoncurrentの申告を停止し
            # LongTermDebtAndCapitalLeaseObligationsへ移行）と同種のタグ切替
            # 欠損。10-K原本で連結BS本体の「Long-term debt」行と一致することを
            # 個別確認済み（AVGO/CDNS/CON/DDOG/HEI/KO/NET/NOW/ONDS/PM/RBRK/
            # RXRX/VZ/XOM/ZS）。SOFIは既存のticker_restrictions
            # （ltdebt_concept=DebtLongtermAndShorttermCombinedAmount、
            # SOFI-DATA-1）で個別対応済みのため、本リストには追加しない
            # （追加するとSOFIのcurrent/noncurrent二重計上リスクがあるため注意）。
            "LongTermDebtAndCapitalLeaseObligations",
            "UnsecuredLongTermDebt",
            "ConvertibleLongTermNotesPayable",
            "ConvertibleDebtNoncurrent",
            "OtherLongTermDebt",
        ],
        "short_term_debt": [
            "ShortTermBorrowings",
            "NotesPayableCurrent",
            "LongTermDebtCurrent",
            "DebtCurrent",
            "CommercialPaper",
            # FY52WEEK-BS-NULL-SILENT-1 Phase B Stage1（2026-07-19追加）:
            # long_term_debtのAndCapitalLeaseObligations系タグのCurrent版・
            # 転換社債Current版。10-K原本で連結BS本体の流動負債区分と一致する
            # ことを個別確認済み（CON/DDOG/ELF/NET/QBTS/RXRX/ZS）。
            # AVAV/ESTC/ZETA/SOFIは該当額が既にlong_term_debt側に正しく
            # 計上済みのため対象外（二重計上防止）。SCCOは最新年度の
            # Current portion of long-term debtが明示的に$0（生涯フェード
            # アウト相当）のため対象外。
            "LongTermDebtAndCapitalLeaseObligationsCurrent",
            "ConvertibleNotesPayableCurrent",
            "ConvertibleDebtCurrent",
            "OtherLongTermDebtCurrent",
        ],
        # 流動項目（シガーバット検出用）
        "current_assets": [
            "AssetsCurrent",
        ],
        "current_liabilities": [
            "LiabilitiesCurrent",
        ],

        # RPO（残存履行義務）- SaaS企業向け
        "rpo": [
            "RevenueRemainingPerformanceObligation",
            "RemainingPerformanceObligation",
            "ContractWithCustomerLiability",
            "DeferredRevenue",
            # FY52WEEK-BS-NULL-SILENT-1 Phase B Stage1（2026-07-19追加）:
            # 親タグ（流動+非流動合算のContractWithCustomerLiability/
            # DeferredRevenue）を申告せず、Current変種のみ申告する企業向け。
            # 10-K原本でBS本体の該当科目と一致することを個別確認済み
            # （ADSK/ALAB/APP/BBAI/CAKE/CART/CELH/CIX/CPRT/DOCN/ENTG/FLYW/
            # INTU/JOBY/KULR/MRVL/RXRX/TASK/VRT/ZETA）。非SaaS業態の
            # 真のゼロ銘柄（ABBV/JNJ/KO/PEP/PM/XOM/SCCO等）は該当タグが
            # 一切存在しないため引き続きNoneのまま。
            "ContractWithCustomerLiabilityCurrent",
            "DeferredRevenueCurrent",
        ],
        
        # PL（損益計算書）
        "revenue": [
            "Revenues",  # 最優先（最も汎用的）
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",  # SOUN等
            "RevenueFromContractWithCustomer",
            "SalesRevenueNet",
            "TotalRevenue",
            "RevenuesNetOfInterestExpense",  # 銀行向け（SOFI等）
        ],
        # net_income・gross_profitはtag_definitions.pyのTAG_CANDIDATESから取得する
        # （LLY-CAPEX-STALE-1 Phase 2a・quarterly.py/parser.pyのタグリスト統合）
        "net_income": list(TAG_CANDIDATES["NET_INCOME"]),
        "gross_profit": list(TAG_CANDIDATES["GROSS_PROFIT"]),
        "cost_of_revenue": [
            "CostOfRevenue",
            "CostOfGoodsAndServicesSold",
            "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization",  # IONQ等
        ],
        # R&D費（RICE計算の投資強度に使用）
        # CapitalizedComputerSoftwareDevelopmentCosts:
        #   費用化されずBSに資産計上されるソフトウェア開発費。
        #   PLに現れないR&D投資を捕捉するためのフォールバック。
        "research_and_development": list(TAG_CANDIDATES["RESEARCH_AND_DEVELOPMENT"]),
        "eps_diluted": [
            "EarningsPerShareDiluted",
        ],
        "eps_basic": [
            "EarningsPerShareBasic",
        ],
        # 販売・マーケティング費（RICE投資強度計算用）
        # 軽資産型企業（CELH等）はR&D/CapExが極小だが
        # マーケティング投資が成長の主な源泉のため投資強度に加算する
        "selling_and_marketing": [
            "MarketingAndAdvertisingExpense",
            "SellingAndMarketingExpense",    # CELH等: FY2022以前の旧タグ
            "MarketingExpense",              # AMZN等: Sales&Marketing費用
            "AdvertisingExpense",
        ],
        # SGA整合性チェック用参照フィールド（取得済みR&D+S&Mとの差分でタグ漏れを検出）
        "selling_general_and_administrative": [
            "SellingGeneralAndAdministrativeExpense",
        ],
        "operating_income": [
            "OperatingIncomeLoss",
        ],

        # CF（キャッシュフロー計算書）
        # operating_cash_flow・capital_expenditure・finance_lease_paymentsは
        # tag_definitions.pyのTAG_CANDIDATESから取得する（LLY-CAPEX-STALE-1 Phase 2a）。
        # capital_expenditureにはLLYが2023年以降申告する新タグ
        # PaymentsToAcquireOtherPropertyPlantAndEquipmentが含まれる
        # （旧タグPaymentsToAcquireProductiveAssetsは2022-09-30で申告停止）。
        "operating_cash_flow": list(TAG_CANDIDATES["OPERATING_CASH_FLOW"]),
        "capital_expenditure": list(TAG_CANDIDATES["CAPITAL_EXPENDITURE"]),
        # ファイナンスリース返済（AMZN等）: FCF計算から除外するために別取得
        "finance_lease_payments": list(TAG_CANDIDATES["FINANCE_LEASE_PAYMENTS"]),
        # 減価償却費（R&D資本化・維持CapEx分離に使用）
        # DepreciationAndAmortization: 最も汎用的なタグ（多くの企業）
        # DepreciationDepletionAndAmortization: 資源系企業等で使用
        # Depreciation: D&Aを分割開示する企業のDepreciation単独タグ
        # AmortizationOfIntangibleAssets: 無形資産償却を別開示する企業のフォールバック
        "depreciation_and_amortization": [
            "DepreciationAndAmortization",
            "DepreciationDepletionAndAmortization",
            "Depreciation",
            "AmortizationOfIntangibleAssets",
        ],
        # stock_based_compensation・buybackはtag_definitions.pyのTAG_CANDIDATESから取得する
        "stock_based_compensation": list(TAG_CANDIDATES["STOCK_BASED_COMPENSATION"]),
        # 自社株買い（キャッシュトラップ検出用）
        "buyback": list(TAG_CANDIDATES["BUYBACK"]),

        # 株式数
        "shares_diluted": [
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "CommonStockSharesOutstanding",  # フォールバック
        ],
        "shares_basic": [
            "WeightedAverageNumberOfSharesOutstandingBasic",
            "CommonStockSharesOutstanding",
        ],
    }
    
    def __init__(self, data_dir: str = None):
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")

    def _fiscal_detection_keys(self) -> List[str]:
        """会計年度末月・決算アンカー日検出に使うXBRLタグ候補（net_income+revenue）"""
        return self.XBRL_MAPPING.get("net_income", []) + self.XBRL_MAPPING.get("revenue", [])

    def _detect_fiscal_end_month(self, us_gaap: dict) -> int:
        """10-K FYエントリから会計年度末月を検出（最頻月を返す、デフォルト12）

        ARCH-DATA-1ステージ2: 実体はcommon/sec_data/utils.py::detect_fiscal_end_month()
        に統一済み（extract_key_facts.py側の独自実装も同関数を参照する）。
        ここでは本クラスのXBRL_MAPPING候補タグを渡す薄いラッパーとして残す。
        """
        return detect_fiscal_end_month(us_gaap, self._fiscal_detection_keys())

    def _detect_fiscal_anchor_date(self, us_gaap: dict) -> Optional[tuple]:
        """本人10-K annualエントリから決算アンカー日（月+日）を検出する

        ARCH-DATA-1ステージ2: common/sec_data/utils.py::detect_fiscal_anchor_date()
        の薄いラッパー。_detect_fiscal_end_monthと同じ候補タグを使う。
        """
        return detect_fiscal_anchor_date(us_gaap, self._fiscal_detection_keys())

    def parse_company_facts(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Company Facts 生データを読み込んでパース
        
        Returns:
            dict: {
                "ticker": str,
                "cik": str,
                "company_name": str,
                "annual": {2024: {...}, 2023: {...}, ...},
                "quarterly": {"2024Q1": {...}, ...}
            }
        """
        ticker = ticker.upper()
        raw_path = os.path.join(self.data_dir, ticker, "company_facts.json")
        
        if not os.path.exists(raw_path):
            print(f"   [{ticker}] company_facts.json が見つかりません")
            return None
        
        try:
            with open(raw_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except Exception as e:
            print(f"   [{ticker}] ファイル読み込みエラー: {e}")
            return None

        # submissions.json（accn -> reportDate）を読み込む。
        # 存在しない場合は空dictとなり、本人データ判定はスキップされ
        # determine_fiscal_year()フォールバックのみで動作する（後方互換）
        accn_reportdate = load_submissions(ticker, data_dir=self.data_dir)

        # [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2: 法人名変更履歴（現CIKの
        # formerNames）を読み込む。存在しない場合は空リストとなり、段階2の
        # 追加検知は発火せず段階1（矛盾トリガー型）のみで動作する（後方互換）
        former_names = load_former_names(ticker, data_dir=self.data_dir)

        return self._parse_raw_data(ticker, raw_data, accn_reportdate=accn_reportdate,
                                     former_names=former_names)

    def _parse_raw_data(self, ticker: str, raw_data: dict, accn_reportdate: Optional[Dict[str, str]] = None,
                         former_names: Optional[list] = None) -> Dict[str, Any]:
        """生データをパース"""
        result = {
            "ticker": ticker,
            "cik": raw_data.get("cik", ""),
            "company_name": raw_data.get("entityName", ""),
            "annual": {},
            "quarterly": {},
            "parsed_at": datetime.now().isoformat(),
        }
        
        facts = raw_data.get("facts", {})
        us_gaap = facts.get("us-gaap", {})

        # 銘柄別 revenue_concept オーバーライド（quarterly.py の TICKER_RESTRICTIONS から参照）
        # 金融系銘柄（SOFI等）は MERGE_ALL_TAGS による先着タグ優先で狭義revenuタグが勝つ問題を回避
        _rev_concept_override = TICKER_RESTRICTIONS.get(ticker, {}).get("revenue_concept")
        # 銘柄別 ltdebt_concept オーバーライド（SOFI-DATA-1: 銀行免許取得後にLongTermDebt系
        # タグの申告を停止し代替タグへ移行したケース向け。quarterly.pyと同一の
        # ticker_restrictionsを参照する）
        _ltdebt_concept_override = TICKER_RESTRICTIONS.get(ticker, {}).get("ltdebt_concept")
        # 銘柄別 sti_concept オーバーライド（FY52WEEK-BS-STI-OVERRIDE-DESIGN-1:
        # KLAC/TER/V/SOFIはshort_term_investmentsの標準候補群（XBRL_MAPPING）が
        # 申告停止済み・非分類BS・正しいタグが汎用的すぎるため、ltdebt_conceptと
        # 同型のticker限定オーバーライドとする）
        _sti_concept_override = TICKER_RESTRICTIONS.get(ticker, {}).get("sti_concept")
        # 銘柄別 cash_concept オーバーライド（CASH-TAG-MISSING-1: CPRT/HEIは
        # ASU 2016-18対応タグ追加後、そちらの方が直近年度まで新しいため
        # 全期間の採用タグが切り替わり過大計上になるのを防ぐ。sti_concept等
        # と同型のticker限定オーバーライド）
        _cash_concept_override = TICKER_RESTRICTIONS.get(ticker, {}).get("cash_concept")
        # 銘柄別 cogs_concept オーバーライド（[[LAYER3-COGS-CANDIDATE-TAG-
        # EXPANSION-1]]、2026-09-06: JOBY/CEG/CPRTはcost_of_revenue相当の
        # 値をOtherCostAndExpenseOperating/CostDirectMaterialという汎用的な
        # 名称のタグで申告しているが、両タグとも他銘柄（AMZN/CAKE/CAT/FCX/
        # KO/LMT/META/RCAT/LYFT等）でCOGSとは無関係の少額項目に使われて
        # いることを全105銘柄相当のcompany_facts.json横断チェックで確認済み
        # のため、グローバル候補リスト（XBRL_MAPPING["cost_of_revenue"]）
        # へは追加せずticker限定オーバーライドとする。VSTは同タグを申告して
        # いるが原価の76%相当が別のカスタムタグのため意図的に適用しない
        # （quarterly.py::TICKER_RESTRICTIONS内のJOBYコメント参照）。
        _cogs_concept_override = TICKER_RESTRICTIONS.get(ticker, {}).get("cogs_concept")
        # 銘柄別 cross_filing_tags オーバーライド（NVDA-STI-TAG-UNIDENTIFIED-1:
        # ANOMALY-PATTERN-CATALOG-1型C。単一タグでは捕捉できず、複数XBRL概念を
        # 指定end_date・指定form制限で直接検索し合算する必要があるケース向け。
        # sti_concept等と異なり、ticker×period×fieldを明示指定した組み合わせ
        # にのみ適用され、他の全銘柄・全フィールドの既存抽出ロジックには
        # 一切影響しない。_apply_cross_filing_tags()参照）
        _cross_filing_tags = TICKER_RESTRICTIONS.get(ticker, {}).get("cross_filing_tags")

        # 会計年度末月を検出（非12月決算企業対応・determine_fiscal_year に渡す。
        # 本人データが存在しない年度のフォールバック判定にのみ使用する）
        fiscal_end_month = self._detect_fiscal_end_month(us_gaap)
        # 決算アンカー日（月+日）を検出（ARCH-DATA-1ステージ2: アンカー日
        # ウィンドウ方式）。検出不可の場合はNone,Noneとなり、determine_fiscal_year()
        # 側で従来の月のみ比較にフォールバックする
        _anchor = self._detect_fiscal_anchor_date(us_gaap)
        anchor_month, anchor_day = _anchor if _anchor else (None, None)
        # [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②: 複数クラスタ検出時、主anchor
        # 以外の有意なクラスタ（support>=2）をera別決算期変更の追加候補として
        # determine_fiscal_year()に渡す。単一クラスタ銘柄（105銘柄中100銘柄）では
        # 空リストとなり、既存の計算結果と数学的に完全同一になる
        extra_anchors = detect_fiscal_anchor_clusters(us_gaap, self._fiscal_detection_keys())

        # FY52WEEK-BUCKET-MISPLACE-1 根本修正: reportDate==end_dateが成立する
        # 「本人の当期データ」のfyタグを年度キーとしてそのまま採用する。
        # accn_reportdateが空（submissions.json未取得）の場合は全件フォールバックのみで動作する。
        _accn_reportdate = accn_reportdate or {}
        _fy_collisions: List[Dict[str, Any]] = []
        _fy_tag_mismatches: List[Dict[str, Any]] = []
        # FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: 本人データ側と既存（フォールバック）
        # 側の生fyタグが異なるまま同一年度バケツで競合したケースを記録する
        _fye_boundary_collisions: List[Dict[str, Any]] = []

        # 全項目を抽出
        extracted = {}
        for field_name, xbrl_keys in self.XBRL_MAPPING.items():
            # 株式数は同一期間で最大値を採用（異常値対策）
            use_max = False  # 株式数は優先順位順で取得（最大値ではない）
            # 以前はuse_max=Trueだったが、CommonStockSharesOutstanding（期末発行済）が
            # WeightedAverageNumberOfDilutedSharesOutstanding（加重平均希薄化後）より
            # 大きい場合に誤った値が取得される問題があった
            merge_all = field_name in self.MERGE_ALL_TAGS_FIELDS
            # revenue_concept が指定されている場合はそのタグのみ使用（mergeなし）
            if field_name == "revenue" and _rev_concept_override:
                xbrl_keys = [_rev_concept_override]
                merge_all = False
            # ltdebt_concept が指定されている場合はそのタグのみ使用
            if field_name == "long_term_debt" and _ltdebt_concept_override:
                xbrl_keys = [_ltdebt_concept_override]
            # sti_concept が指定されている場合はそのタグのみ使用
            if field_name == "short_term_investments" and _sti_concept_override:
                xbrl_keys = [_sti_concept_override]
            # cash_concept が指定されている場合はそのタグのみ使用
            if field_name == "cash_and_equivalents" and _cash_concept_override:
                xbrl_keys = [_cash_concept_override]
            # cogs_concept が指定されている場合は既存候補リストへ追加する
            # （置換ではない）。CPRTはFY2016-2019をCostOfGoodsAndServices
            # Sold等の既存候補タグが既に値を提供しており、cogs_concept
            # （CostDirectMaterial）はFY2018以降のみcompany_facts.jsonに
            # 存在するため、[override]へ全置換するとFY2016-2017が完全に
            # 欠損する回帰が生じることをQA中に発見した。_extract_values_
            # best_candidate()は全candidateを横断してfreshest（最新期末）
            # のタグを「勝者」として採用しつつ、本人データ（is_own_data）
            # backfillは勝者タグに限らずxbrl_keys全体から収集するため、
            # 追加（末尾append）のみでCostDirectMaterialが新規に候補入り
            # しても既存候補の年度別データは失われない
            # （[[LAYER3-COGS-CANDIDATE-TAG-EXPANSION-1]]、2026-09-06）
            if field_name == "cost_of_revenue" and _cogs_concept_override:
                xbrl_keys = list(xbrl_keys) + [_cogs_concept_override]
            extracted[field_name] = self._extract_values(
                us_gaap, xbrl_keys, use_max=use_max, merge_all_tags=merge_all,
                fiscal_end_month=fiscal_end_month, accn_reportdate=_accn_reportdate,
                field_name=field_name, collisions_out=_fy_collisions,
                anchor_month=anchor_month, anchor_day=anchor_day,
                extra_anchors=extra_anchors,
                fy_mismatches_out=_fy_tag_mismatches,
                boundary_collisions_out=_fye_boundary_collisions,
            )

        # [[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]: fact_overrides.jsonの
        # 個別上書きを、抽出直後・全ての逆算バックフィル処理より前に適用する
        # （旧実装はsave_parsed_data()内の最終段〈逆算バックフィルより後〉で
        # 適用しており、gross_profit逆算に補正前revenueが使われる不整合が
        # あった）
        self._apply_fact_overrides(ticker, extracted)

        # [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1・段階2: 複数accn混在かつ
        # （①数学的整合性が破綻している、または②アンカー候補accnのreportDateが
        # 法人名変更履歴の期間内にあり社名変更前後のデータ混在が疑われる）
        # 年度についてのみ、本人データ(is_own_data=True)を提供するaccnへ統一する
        _spac_shell_detections: List[Dict[str, Any]] = []
        self._resolve_bs_entity_mixing(extracted, accn_reportdate=_accn_reportdate,
                                        former_names=former_names or [],
                                        spac_detections_out=_spac_shell_detections)

        # [[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]] VRT型対応:
        # stockholders_equityの本人データが皆無の年度のみ、total_assets/
        # total_liabilitiesが共通採用したaccnの値で補う（欠損穴埋め型）
        self._align_stockholders_equity_to_sibling_accn(extracted, us_gaap, fiscal_end_month,
                                                          anchor_month, anchor_day, extra_anchors)

        # [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]: XBRL_MAPPING
        # ["total_liabilities"]の2番目の候補LiabilitiesAndStockholdersEquityが
        # 誤採用された年度（total_liabilities==total_assetsという数学的
        # シグネチャで検知）のみ、貸借対照表恒等式で逆算した値に置き換える
        self._backfill_total_liabilities_via_identity(extracted)

        # NVDA-STI-TAG-UNIDENTIFIED-1: cross_filing_tagsに明示登録された
        # ticker×period×fieldの組み合わせについてのみ、複数タグ合算値で
        # extracted[field]の該当バケツを上書きする（型C対応・①案）。
        # 標準抽出ループの後に適用することで、通常経路の結果を出発点として
        # 上書きし、既存の抽出ロジック自体には一切手を加えない。
        if _cross_filing_tags:
            self._apply_cross_filing_tags(us_gaap, extracted, _cross_filing_tags)

        # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案a・b: revenueと
        # cost_of_revenueが異なるaccnから独立採用されている年度についてのみ、
        # revenueまたはgross_profitと同一accn・同一期間を持つcost_of_revenue
        # 候補タグ（CostOfGoodsSold等の拡張候補含む）が存在すればそちらを
        # 優先採用する（欠損穴埋め型のゲート条件、既に一致済みのケースには
        # 一切触れない）
        self._align_cost_of_revenue_to_revenue_period(extracted, us_gaap)

        # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案c: 上記でもなお矛盾が
        # 残る年度についてのみ、cost_of_revenue採用元accn内の2つの異なる
        # 候補タグの合算で矛盾が厳密に解消する場合に限り合算値を採用する
        # （RMBS(2018)型。ENTG/TER等の重複タグ付けは矛盾が既にないため
        # 対象外＝二重計上の巻き添えなし）
        self._backfill_cost_of_revenue_via_tag_sum(extracted, us_gaap)

        # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案d: revenue・
        # cost_of_revenue・gross_profitが同一accn・同一期間から採用されて
        # いるにも関わらず矛盾が残る年度についてのみ、同一accn内の別の
        # revenue候補タグへ切り替えると矛盾が厳密に解消する場合に限り
        # 採用する（BSY(2019)型。他銘柄のrevenue優先順位には一切影響
        # しない極めて狭いゲート条件）
        self._align_revenue_to_cost_gross_profit_identity(extracted, us_gaap)

        # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案e: gross_profitの
        # 採用元accn・同一期間に revenue候補タグとcost_of_revenue候補
        # タグの両方が存在し、その差が現在のgross_profitと厳密に一致する
        # 年度についてのみ、revenue・cost_of_revenueの両方をそのaccnの
        # 値に置き換える（ONDS(2017)・RMBS(2019)型。gross_profitは
        # 既に信頼できる値である前提でアンカーとして固定し、revenue・
        # cost_of_revenueの2フィールドを同時に是正する）
        self._align_revenue_and_cost_to_gross_profit_own_accn(extracted, us_gaap)

        # [[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]①: 標準タグから
        # gross_profitが取得できない年度のみ、revenue-cost_of_revenueで
        # 逆算した値を本番annual_YYYY.jsonへ書き戻す（欠損の穴埋めのみ、
        # 既存の正しい値は上書きしない）
        self._backfill_gross_profit_from_revenue_cogs(extracted)

        # [[OPERATING-INCOME-EXTRACTION-GAP-1]]: OperatingIncomeLossタグが
        # 取得できない年度のみ、GP-R&D-SGA法・pretax調整法で再構成した値を
        # 書き戻す（欠損の穴埋めのみ、既存の正しい値は上書きしない）。
        # 上のgross_profit逆算より後に実行することで、gross_profit自体が
        # 逆算値の場合もそのまま利用できる。
        self._backfill_operating_income(extracted, us_gaap)

        # ARCH-DATA-1ステージ3: fyタグ裏取り不一致を記録（0件でも毎回書き込む。
        # fy_collision_logと同じ化石ファイル対策）
        self._save_fy_tag_mismatch_log(ticker, _fy_tag_mismatches)

        # 衝突0件でも毎回書き込む（IOT/AVGO/MRVLの化石ファイル問題の再発防止。
        # 一度検知された衝突が後日解消された場合に古いログが残り続けることを防ぐ）
        self._save_fy_collision_log(ticker, _fy_collisions)

        # [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2: 0件でも毎回書き込む（同上の
        # 化石ファイル対策と同じ理由）
        self._save_spac_shell_detection_log(ticker, _spac_shell_detections)

        # FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: 0件でも毎回書き込む（同上の化石
        # ファイル対策と同じ理由）
        self._save_fye_boundary_collision_log(ticker, _fye_boundary_collisions)

        # 年次データを集約
        years = self._get_available_years(extracted)
        for year in years:
            annual_data = self._build_period_data(extracted, year, is_annual=True)
            if annual_data:
                result["annual"][year] = annual_data

        # [[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]: fixed_registry.json
        # 登録済みのticker×年度について、フィックス対象フィールドを
        # 既存annual_{year}.jsonの値へ強制復元する（差分適用方式）。
        # 上記の年次データ集約直後・quarterly集約より前に実行する
        # （annual側のみを対象とし、quarterly/TTM側〈layer3_builder.py〉は
        # 別実装のパイプラインのため本メソッドの対象外）
        self._apply_fixed_registry_freeze(ticker, result)

        # 四半期データを集約
        quarters = self._get_available_quarters(extracted)
        for quarter in quarters:
            quarterly_data = self._build_period_data(extracted, quarter, is_annual=False)
            if quarterly_data:
                result["quarterly"][quarter] = quarterly_data

        # [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]: 会計恒等式
        # TA=TL+SE(+NCI+一時的持分)の検証。0件でも毎回書き込む（同上の
        # 化石ファイル対策と同じ理由）
        _bs_identity_violations = self._check_bs_identity_violations(ticker, result, us_gaap)
        self._save_bs_identity_violations_log(ticker, _bs_identity_violations)

        return result

    @staticmethod
    def _bs_math_violations(bs: Dict[str, Any]) -> bool:
        """BS項目間の基本的な包含関係が破綻しているか判定する
        （[[SPAC-SHELL-BS-ENTITY-MIXING-1]]）。current_assets<=total_assets・
        current_liabilities<=total_liabilities・long_term_debt<=total_liabilities・
        short_term_debt<=total_liabilities・(long_term_debt+short_term_debt)<=
        total_liabilities・cash_and_equivalents<=total_assetsのいずれかに
        違反する場合にTrueを返す。値が片方でもNoneの組は判定対象外（比較不能）。
        """
        ta = bs.get("total_assets")
        tl = bs.get("total_liabilities")
        ca = bs.get("current_assets")
        cl = bs.get("current_liabilities")
        ltd = bs.get("long_term_debt")
        std = bs.get("short_term_debt")
        cash = bs.get("cash_and_equivalents")
        if ta is not None and ca is not None and ca > ta:
            return True
        if tl is not None and cl is not None and cl > tl:
            return True
        if tl is not None and ltd is not None and ltd > tl:
            return True
        if tl is not None and std is not None and std > tl:
            return True
        if tl is not None and ltd is not None and std is not None and (ltd + std) > tl:
            return True
        if ta is not None and cash is not None and cash > ta:
            return True
        return False

    @staticmethod
    def _report_date_in_former_name_window(report_date: str, former_name: Dict[str, Any]) -> bool:
        """
        [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2: reportDate（"YYYY-MM-DD"）が
        formerNamesエントリの[from, to]区間（SEC APIはISO8601、例:
        "2020-12-31T05:00:00.000Z"）に含まれるか判定する（日付部分のみで
        比較、時刻・タイムゾーンは無視）。

        境界は両端含む（inclusive）: BBAI実データでreportDate=2020-12-31が
        formerNamesの"from"=2020-12-31と完全一致するケースを確認済みのため、
        "from"側も含める必要がある。
        """
        if not report_date:
            return False
        try:
            rd = datetime.strptime(report_date[:10], "%Y-%m-%d").date()
            frm = datetime.strptime(str(former_name.get("from", ""))[:10], "%Y-%m-%d").date()
            to = datetime.strptime(str(former_name.get("to", ""))[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return False
        return frm <= rd <= to

    def _resolve_bs_entity_mixing(self, extracted: Dict[str, Any],
                                   accn_reportdate: Optional[Dict[str, str]] = None,
                                   former_names: Optional[list] = None,
                                   spac_detections_out: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1・段階2: SPAC合併等により同一
        年度のBS(instant fact)フィールドが異なる法的実体（accn）から混在
        採用されているケースを是正する（実例: BBAI/RDW/RKLB/SOFI/VRT/ONDS/
        KULR(2016)、段階2追加によりSPIR(2020)も対象）。

        「①複数accnが混在」かつ「②本人データ(is_own_data=True)を提供する
        accnが単一に定まる」かつ「③現に数学的矛盾が確認できる、**または
        ③'アンカー候補accnのreportDateが法人名変更履歴（former_names）の
        いずれかの[from, to]区間内にある（段階2追加）**」かつ「④アンカーへの
        統一により実際に矛盾が解消する（③'単独発火時は元々矛盾がないため
        自明に満たす）」の4条件をすべて満たす年度についてのみ、本人データの
        accnをアンカーとして採用し、アンカー以外のaccnから採用された
        _BS_ENTITY_MIXING_FIELDSの値をNone化する（安全側のNone化、
        [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]と同じ設計方針）。

        ③'（段階2）はSPIR(2020)型（合併前SPACシェルのBSと合併後本体の
        BSが混在するが、たまたま数学的矛盾が顕在化していない"事故的な
        正しさ"）を、矛盾の有無に頼らず事前検知するために追加した。
        former_names自体は「単なる社名変更・法人形態変更」でも記録される
        ため単独では合併の疑いと区別できないが、本条件は常に①②（複数accn
        混在・本人データaccnの一意性）とのAND条件でのみ発火するため、
        単純な改名（accn混在を伴わない、同一法人が継続してBSを報告する
        ケース）では①の時点で対象外となり誤検知しない。

        条件④はKULR(2019)型（矛盾の原因となる2フィールドが元々同一accnから
        採用されており、accn混在自体は矛盾と無関係）を除外するために必須。
        条件④なしでは、KULR(2019)のshort_term_debt（矛盾とは無関係な別
        フィールド、たまたま他accn由来）が巻き添えでNone化され、根本の矛盾
        （current_liabilities>total_liabilities）は解消されないまま無関係な
        変更だけが生じることを実データで確認した。

        条件を1つも満たさない年度（矛盾もSPAC疑いもない正常系、本人データ
        accnが0個または2個以上で一意に定まらない年度、統一しても矛盾が
        解消しない年度）は一切変更しない。105銘柄・87件の複数accn混在
        ケースへのオフラインシミュレーションで、本条件により矛盾のない
        56件（41銘柄）・KULR(2019)に一切影響しないことを確認済み（減算的
        〈subtractive〉設計であり、既存の正しい値を上書きする経路は持たない）。

        spac_detections_out: ③'（former_names一致）で発火した年度の詳細
        （アンカーaccn・reportDate・一致したformerNameエントリ・None化した
        フィールド）を記録する。呼び出し元がspac_shell_detection_log.jsonへ
        保存する。
        """
        former_names = former_names or []
        years = set()
        for field in self._BS_ENTITY_MIXING_FIELDS:
            years.update(extracted.get(field, {}).get("_annual_provenance", {}).keys())

        for year in years:
            field_accn: Dict[str, str] = {}
            own_accns = set()
            bs_values: Dict[str, Any] = {}
            for field in self._BS_ENTITY_MIXING_FIELDS:
                field_data = extracted.get(field, {})
                val = field_data.get("annual", {}).get(year)
                if val is None:
                    continue
                bs_values[field] = val
                prov = field_data.get("_annual_provenance", {}).get(year)
                if prov and prov.get("accn"):
                    field_accn[field] = prov["accn"]
                    if prov.get("is_own_data"):
                        own_accns.add(prov["accn"])

            if len(set(field_accn.values())) < 2:
                continue  # ①複数accn混在なし
            if len(own_accns) != 1:
                continue  # ②アンカーが一意に定まらない

            anchor = next(iter(own_accns))
            violation_now = self._bs_math_violations(bs_values)

            # ③'は矛盾の有無に関わらず常に評価する（violation_now=Trueの場合も
            # 冪等性確認・監査ログの網羅性のため判定自体はスキップしない。
            # BBAI/RDW/RKLB/SOFI/VRT〈矛盾も同時に存在〉でも実データで確認済み）
            matched_former_name = None
            if accn_reportdate and former_names:
                anchor_report_date = accn_reportdate.get(anchor)
                if anchor_report_date:
                    for fn in former_names:
                        if self._report_date_in_former_name_window(anchor_report_date, fn):
                            matched_former_name = fn
                            break

            if not violation_now and matched_former_name is None:
                continue  # ③④矛盾もSPAC疑い（③'）もない

            candidate_values = {f: v for f, v in bs_values.items() if field_accn.get(f) == anchor}
            if self._bs_math_violations(candidate_values):
                # ④アンカーへの統一後も矛盾が解消しない（KULR 2019型: 矛盾の
                # 原因フィールドが元々同一accn内にあり、accn混在は無関係）。
                # この場合は是正効果がないため一切変更しない（無関係フィールドの
                # 巻き添えNone化を防ぐ）
                continue

            nulled_fields = [f for f, a in field_accn.items() if a != anchor]
            for field in nulled_fields:
                extracted[field]["annual"].pop(year, None)
                extracted[field].get("_annual_provenance", {}).pop(year, None)

            if matched_former_name is not None and spac_detections_out is not None:
                spac_detections_out.append({
                    "year": year,
                    "anchor_accn": anchor,
                    "anchor_report_date": accn_reportdate.get(anchor) if accn_reportdate else None,
                    "former_name": matched_former_name.get("name"),
                    "former_name_from": matched_former_name.get("from"),
                    "former_name_to": matched_former_name.get("to"),
                    "nulled_fields": nulled_fields,
                    "triggered_by": "math_violation" if violation_now else "former_names_window",
                })

    def _align_stockholders_equity_to_sibling_accn(self, extracted: Dict[str, Any], us_gaap: dict,
                                                     fiscal_end_month: int,
                                                     anchor_month: Optional[int] = None,
                                                     anchor_day: Optional[int] = None,
                                                     extra_anchors: Optional[List[tuple]] = None) -> None:
        """
        [[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]] VRT型（0アンカー）対応。

        stockholders_equityにその年度の本人データ（is_own_data=True）が
        一件も存在しない年度についてのみ、total_assets/total_liabilities
        （BSの他フィールド）が共通して採用したaccnに、同一年度バケツに
        属するstockholders_equity候補タグのエントリが存在すれば、そちらを
        フォールバックの採用値より優先する（欠損穴埋め型の追加ステップ）。

        _resolve_bs_entity_mixing()との関係: 同関数は「本人データを提供する
        accnが単一に定まる年度（own_accns==1）」のみを対象とするのに対し、
        本メソッドは「stockholders_equity自身の本人データが皆無の年度
        （is_own_data==False）」のみを対象とし、適用条件が排他的（同一年度で
        両方が同時に発火することはない）。呼び出し順は
        _resolve_bs_entity_mixing()の直後（本メソッドは全く別のaccnから
        値を補うだけで、_resolve_bs_entity_mixing()がNone化した値を復元
        する意図はない）。

        VRT(2017)実例: stockholders_equityの本人データが存在せず、フォール
        バックは10-K/A（2021年、無関係な事業実体の比較列再表示、-$129.6M）
        を誤って採用していたが、total_assets/total_liabilitiesが共通採用
        したaccn（VRTのFY2018 10-K、2019-03-13提出）には正しい比較列値
        （$23,724）が存在し、採用するとTA=TL+SEの恒等式が完全一致することを
        確認済み。CWAN(2023)のような「正しいaccnにタグ自体が存在しない」
        構造的必然パターンは本メソッドの対象外のまま変化しない（一致する
        エントリが見つからなければ何もしない、安全側）。

        適用範囲の限定（105銘柄シミュレーションで発見、2026-08-30）:
        現在の採用値のaccnが既にanchor_accnと同じ場合は対象外とする
        （AVGO(2017)実例参照）。この場合はVRT型の「別filingへの誤誘導」
        ではなく、同一accn内での候補タグ選択（StockholdersEquity単体 vs
        NCI込みタグ）という別問題であり、本メソッドで機械的に片方を
        優先すると、TA=TL+SEの恒等式がNCI込み値でのみ完全一致する銘柄で
        逆に恒等式を壊してしまうことが判明したため、意図的にスコープ外
        とした。
        """
        se_field = extracted.get("stockholders_equity")
        ta_field = extracted.get("total_assets")
        tl_field = extracted.get("total_liabilities")
        if se_field is None or ta_field is None or tl_field is None:
            return

        se_annual = se_field.get("annual", {})
        se_prov = se_field.setdefault("_annual_provenance", {})
        ta_prov = ta_field.get("_annual_provenance", {})
        tl_prov = tl_field.get("_annual_provenance", {})

        xbrl_keys = self.XBRL_MAPPING.get("stockholders_equity", [])

        for year in list(se_annual.keys()):
            prov = se_prov.get(year)
            if prov is None or prov.get("is_own_data"):
                continue  # 本人データが既に存在する年度は対象外（CRM型は別ロジックで対応済み）

            ta_p = ta_prov.get(year)
            tl_p = tl_prov.get(year)
            if not ta_p or not tl_p:
                continue
            anchor_accn = ta_p.get("accn")
            if not anchor_accn or anchor_accn != tl_p.get("accn"):
                continue  # total_assets/total_liabilitiesが同一accnで一致する年度のみ対象

            # 105銘柄シミュレーションで発見（2026-08-30）: stockholders_equity
            # の現在の採用accnが既にanchor_accnと同じ場合は対象外とする。
            # AVGO(2017)実例で判明: このケースは「0アンカー」（VRT型）ではなく
            # 同一accn内でのタグ選択（StockholdersEquity単体 vs
            # ...IncludingPortionAttributableToNoncontrollingInterest、NCI込み）
            # の優劣という別問題であり、XBRL_MAPPINGの優先順位を機械的に
            # 適用すると、TA=TL+SEの会計恒等式がNCI込み値でのみ完全一致する
            # 銘柄（AVGO(2017)）で逆に恒等式を破壊してしまう
            # （$54,418M=TL+SE〈NCI込み〉が完全一致、$51,517M=TL+SE〈NCI抜き〉
            # は$2,901M＝AVGOのNCI相当分ずれる）。VRT型の本来の対象は
            # 「現在の採用accnがanchor_accnと全く異なる」ケースに限定し、
            # 同一accn内のタグ選択には踏み込まない（タグ選択自体は既存の
            # フォールバック機構〈全体を通じた鮮度比較〉の判断に委ねる）。
            existing_accn = prov.get("accn")
            if existing_accn == anchor_accn:
                continue

            # anchor_accn内で、stockholders_equity候補タグのうちこの年度バケツに
            # 属する年次エントリ（10-K/10-K/A・fp==FY）を探す。複数該当する場合は
            # xbrl_keys（優先タグ順）→end_date新しい方の順で選ぶ
            # （_extract_single_key()の年次候補採用条件と同一の絞り込み）。
            best_entry = None
            for key in xbrl_keys:
                if key not in us_gaap:
                    continue
                for unit_type in ["USD", "shares", "USD/shares"]:
                    for entry in us_gaap[key].get("units", {}).get(unit_type, []):
                        if entry.get("accn") != anchor_accn:
                            continue
                        if entry.get("form") not in ("10-K", "10-K/A") or entry.get("fp") != "FY":
                            continue
                        end_date = entry.get("end", "")
                        val = entry.get("val")
                        if val is None or not end_date or len(end_date) < 10:
                            continue
                        try:
                            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                        except ValueError:
                            continue
                        if determine_fiscal_year(end_dt, fiscal_end_month, anchor_month, anchor_day,
                                                  extra_anchors) != year:
                            continue
                        if best_entry is None or end_date > best_entry["end"]:
                            best_entry = {"end": end_date, "val": val, "fy": entry.get("fy"),
                                          "filed": entry.get("filed", "")}
                if best_entry is not None:
                    break  # xbrl_keys優先順位順: 先に見つかったタグを優先

            if best_entry is None or best_entry["val"] == se_annual.get(year):
                continue  # 一致するエントリがない、または値に変化がなければ何もしない

            se_annual[year] = best_entry["val"]
            se_prov[year] = {
                "accn": anchor_accn, "filed": best_entry["filed"], "is_own_data": False,
                "fy_tag": best_entry["fy"], "aligned_to_sibling_accn": True,
            }

    def _backfill_gross_profit_from_revenue_cogs(self, extracted: Dict[str, Any]) -> None:
        """
        [[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]①: GrossProfit/
        GrossProfitLossタグからgross_profitが取得できない年度についてのみ、
        revenue - cost_of_revenueで逆算した値をフォールバックとして採用し、
        本番annual_YYYY.jsonに書き戻す。

        適用条件（Case Aの定義を厳密に踏襲）:
          - gross_profitが標準タグから取得できていない（欠損）年度のみ
            （標準タグでの取得を常に優先する。既存の正しい値を上書きする
            経路は持たない、減算的〈subtractive〉ではなく加算的
            〈additive〉だが「欠損の穴埋めのみ」という点で安全側設計）
          - revenue・cost_of_revenueが同一年度で両方present（Noneでない）

        採用した値のprovenanceには"derived": Trueを付与し、実タグに基づかない
        逆算値であることを明示する（既存のbs_provenance側の
        is_approximated/_apply_cross_filing_tags規約と同種のシグナル。
        report_consistency_check.py等が実タグ由来の値と区別できるようにする
        ため）。

        Predecessor/Successor型（SPAC合併等でrevenue/cost_of_revenue自体も
        Noneの年度、例: ELF 2014/2019・BBAI/RDW/RKLB/SOFI/VRT 2020/2019）は
        フォールバック元データ自体が存在しないため、判定を経由せず自動的に
        対象外となる。
        """
        gp_field = extracted.get("gross_profit")
        rev_field = extracted.get("revenue")
        cogs_field = extracted.get("cost_of_revenue")
        if gp_field is None or rev_field is None or cogs_field is None:
            return

        gp_annual = gp_field.setdefault("annual", {})
        gp_prov = gp_field.setdefault("_annual_provenance", {})
        rev_annual = rev_field.get("annual", {})
        cogs_annual = cogs_field.get("annual", {})

        for year, rev_val in rev_annual.items():
            if gp_annual.get(year) is not None:
                continue  # 標準タグで既に取得済み（優先、上書きしない）
            cogs_val = cogs_annual.get(year)
            if rev_val is None or cogs_val is None:
                continue
            gp_annual[year] = rev_val - cogs_val
            gp_prov[year] = {
                "accn": None, "filed": "", "is_own_data": False, "fy_tag": year,
                "derived": True,
            }

    # [[OPERATING-INCOME-EXTRACTION-GAP-1]]: pretax income・非事業性項目の
    # タグ候補。XBRL_MAPPINGには含めない（operating_income再構成専用の
    # 補助タグであり、他フィールドのような独立extracted項目として一般公開
    # しない）。優先順位は「連結ベースのpretax」を優先する程度の単純な
    # 順序であり、他フィールドのような銘柄横断の実績検証は今回未実施
    # （必要になった時点で追加検証する）。
    _OI_PRETAX_TAGS = (
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments",
    )
    _OI_NONOP_AGGREGATE_TAGS = ("NonoperatingIncomeExpense",)
    # [[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]: GP法（gross_profit -
    # R&D - SGA/S&M）が控除しない別建て営業費用のうち、実データバック
    # テスト（全105銘柄・772件、2026-08-21〜22再検証）でGP法の誤差改善に
    # 一貫して寄与すると確認できたタグのみ。`RestructuringCosts`（COHR等
    # 19銘柄が使用する別名タグ）は同一方法論で単独バックテストした結果
    # 改善率48%（既に不採用の他候補と同水準）だったため意図的に含めない
    # （COHRのようにChargesタグが欠落しCostsタグのみ報告される年度が
    # 将来出た場合、本フォールバックは効かず未控除のまま残る既知の制約。
    # 2026-08-22時点でCOHRはFY2023・FY2025ともRestructuringChargesタグ
    # 自体を報告しているため実害なしを確認済み）。
    _OI_RESTRUCTURING_TAGS = ("RestructuringCharges",)
    # 費用側（正の値=コスト）: pretaxから営業利益へ戻す際は加算（足し戻す）。
    # 同一概念の別タグ（片方のみ報告）のため候補内は先勝ち方式でよい。
    _OI_NONOP_EXPENSE_TAGS = ("InterestExpense", "InterestExpenseDebt")
    # 収益側（正の値=利得）: pretaxから営業利益へ戻す際は減算。
    # 受取利息と「その他非事業性損益」は別概念で両方同時に開示されうる
    # （JNJ実測で両方存在・合算しないと非事業性項目の説明力を過小評価する
    # ことを2026-08-16に確認）ため、カテゴリごとに別スロットで独立に
    # 検索し合算する（カテゴリ内は先勝ち方式、カテゴリ間は合算）。
    _OI_NONOP_INTEREST_INCOME_TAGS = ("InvestmentIncomeInterest", "InterestIncomeOther")
    _OI_NONOP_OTHER_INCOME_TAGS = ("OtherNonoperatingIncomeExpense",)

    def _find_fy_tag_value(self, us_gaap: Dict[str, Any], tag_names: tuple, year: int) -> Optional[float]:
        """指定タグ候補（優先順）から、10-K/10-K/Aのfp='FY'・fy==yearに一致する
        最新end日のエントリの値を返す（無ければNone）。
        [[OPERATING-INCOME-EXTRACTION-GAP-1]]専用の簡易ルックアップ。
        _extract_values()のような本人データ優先・アンカー日・衝突検知は
        行わない（再構成フォールバック用の補助情報という位置づけのため）。
        """
        for tag in tag_names:
            node = us_gaap.get(tag)
            if not node:
                continue
            entries = node.get("units", {}).get("USD", [])
            matches = [
                e for e in entries
                if e.get("fp") == "FY" and e.get("form") in ("10-K", "10-K/A")
                and e.get("fy") == year and e.get("val") is not None
            ]
            if matches:
                matches.sort(key=lambda e: e.get("end", ""))
                return matches[-1]["val"]
        return None

    def _lookup_pretax_and_nonop_adjustment(
        self, us_gaap: Dict[str, Any], year: int
    ) -> tuple:
        """pretax income と非事業性項目の調整額を取得する。

        戻り値: (pretax, nonop_adjustment, nonop_items)
          - pretax: pretaxタグの値。取得不可ならNone
          - nonop_adjustment: 営業利益 = pretax - nonop_adjustment となる
            調整額。集計タグ（NonoperatingIncomeExpense）があればそのまま
            採用、無ければ個別タグ（支払利息は加算・受取利息等の利得は
            減算）を合算する。該当タグが1件も無ければ0.0（pretaxをその
            まま使う）
          - nonop_items: 発見した非事業性タグの{名前: 値}。突き合わせ
            検証のcoverage計算・provenance記録に使う
        """
        pretax = self._find_fy_tag_value(us_gaap, self._OI_PRETAX_TAGS, year)
        if pretax is None:
            return None, None, {}

        agg = self._find_fy_tag_value(us_gaap, self._OI_NONOP_AGGREGATE_TAGS, year)
        if agg is not None:
            return pretax, agg, {"nonop_net": agg}

        nonop_adjustment = 0.0
        nonop_items: Dict[str, float] = {}
        expense_val = self._find_fy_tag_value(us_gaap, self._OI_NONOP_EXPENSE_TAGS, year)
        if expense_val is not None:
            nonop_adjustment -= expense_val
            nonop_items["interest_expense"] = expense_val
        interest_income_val = self._find_fy_tag_value(us_gaap, self._OI_NONOP_INTEREST_INCOME_TAGS, year)
        if interest_income_val is not None:
            nonop_adjustment += interest_income_val
            nonop_items["interest_income"] = interest_income_val
        other_income_val = self._find_fy_tag_value(us_gaap, self._OI_NONOP_OTHER_INCOME_TAGS, year)
        if other_income_val is not None:
            nonop_adjustment += other_income_val
            nonop_items["other_nonop_income"] = other_income_val

        return pretax, nonop_adjustment, nonop_items

    def _backfill_operating_income(self, extracted: Dict[str, Any], us_gaap: Dict[str, Any]) -> None:
        """
        [[OPERATING-INCOME-EXTRACTION-GAP-1]]: `OperatingIncomeLoss`タグが
        取得できない年度について、以下の優先順位で再構成する。

        1. GP法: gross_profit - research_and_development -
           selling_general_and_administrative（gross_profitは
           _backfill_gross_profit_from_revenue_cogs()で既にRevenue-COGS
           逆算済みの場合を含む。既存の抽出済みフィールドをそのまま使う
           ため、Revenue-COGS代替を本関数で重複実装しない）
        2. pretax調整法: pretax income - 非事業性項目（GP法が使えない年度
           のみのフォールバック。詳細は下記「フォールバック向き」参照）

        **フォールバック向き（2026-08-19、本線3・ゲート1第一歩の実測で
        反転）**: GP法が算出可能な場合は**常にGP法を採用する**
        （source="reconstructed_gp"）。pretax調整法はGP法が構造的に
        算出不可能な年度（gross_profit/R&D/SGAのいずれかが欠落。XOM・
        ASTS等、業態上COGS区分自体が存在しない企業を含む）専用の
        フォールバックとする。

        **反転の根拠（実データ、2026-08-19）**: 当初は「pretax_rawとGP法
        推定値の乖離のうち、発見できた非事業性項目が50%以上を説明できれば
        GP法を採用し、50%未満ならGP法を棄却してpretax調整法へフォール
        バックする」設計だった。しかしyfinance実測（CHECK-35第一歩、
        期末日を正しく一致させた比較）で以下が判明した:
        - GP法が算出可能だった4銘柄（LLY/JNJ/KLAC/COHR）**全てで
          yfinanceと誤差0.0%**（完全一致）
        - 旧設計でフォールバックが実際に発動した2銘柄（LLY・COHR）は、
          pretax採用値がyfinanceに対しそれぞれ-11.4%・-82.4%も乖離して
          いた
        - COHR自身の過去3年（`OperatingIncomeLoss`標準タグがまだ開示
          されていたFY2022〜2024）でバックテストしても、GP法が3年連続で
          旧pretax方式を上回った（GP法誤差: 0.0%/+320.9%/+28.1%、
          旧pretax法誤差: -32.0%/-857.9%/-253.6%）
        - 設計上の理由: pretaxは営業利益から最も遠く（非事業性の収益・
          費用を全て含む）、非事業性費用が大きい企業ほどpretaxは営業
          利益から乖離する。**そしてそういう企業でこそ2手法は不一致に
          なりやすい**ため、「不一致時にpretaxへ落ちる」旧設計は、
          最も間違えやすいケースで最も乖離の大きい値を選ぶ構造になって
          いた
        （詳細な調査記録はBACKLOG_DONE.md `[[OPERATING-INCOME-
        EXTRACTION-GAP-1]]`参照）

        **coverage_ratioの役割変更**: 旧設計では「GP法を採用してよいかの
        門番」（0.5以上でGP法採用）だったが、反転後は**「pretax調整法が
        どの程度信頼できたかの事後診断指標」**になる（高ければpretaxも
        近い値だった、低ければpretaxは大きく外れていた）。GP法の採否
        判定には使わなくなったが、pretaxの信頼性を後から追跡できるよう
        provenanceには引き続き記録する。

        **GP法自体も「正解」ではなく「現時点で得られる最良の選択」に
        過ぎない点に注意**: COHRのFY2023はGP法でも誤差+320.9%だった。
        またCOHRは`RestructuringCosts`（$160M）・
        `OtherOperatingIncomeExpenseNet`（$47.6M）というGP法
        （Revenue-COGS-R&D-SGA）にも含まれない別建ての営業費用項目を
        持つことが判明しており、GP法が構造的に見落としている非事業性
        ではない営業費用が存在しうる（追加の営業費用タグの取り込みは
        本反転のスコープ外、別途登録・検討）。

        採用値はpl_provenanceに"derived": True・"source"
        （NAMING_CONVENTIONS.md規則4準拠のprovenance値）を付与する。
        標準タグで既に取得済みの年度は一切変更しない（欠損の穴埋めのみ）。
        """
        oi_field = extracted.setdefault("operating_income", {})
        oi_annual = oi_field.setdefault("annual", {})
        oi_prov = oi_field.setdefault("_annual_provenance", {})

        gp_annual = extracted.get("gross_profit", {}).get("annual", {})
        rd_annual = extracted.get("research_and_development", {}).get("annual", {})
        sga_annual = extracted.get("selling_general_and_administrative", {}).get("annual", {})
        sm_annual = extracted.get("selling_and_marketing", {}).get("annual", {})
        rev_annual = extracted.get("revenue", {}).get("annual", {})
        ni_annual = extracted.get("net_income", {}).get("annual", {})

        for year in rev_annual:
            if oi_annual.get(year) is not None:
                continue  # 標準タグで既に取得済み（優先、上書きしない）

            # GP法: 統合SGAを報告する企業はGP-R&D-SGA、マーケティング費を
            # 別建て報告しSGAを報告しない企業（SOFI等の一部金融/フィンテック）
            # はGP-R&D-S&Mで代替する（_estimate_ttm_operating_income()の
            # 四半期版と同じ発想。2026-08-16、SOFIの検証で発見・追加）。
            gp_val, rd_val = gp_annual.get(year), rd_annual.get(year)

            # [[OPERATING-INCOME-EXTRACTION-GAP-1]] GP法入力の整合性ガード
            # （2026-08-19、案D）: gross_profit = revenue - COGS という
            # 定義上の関係が成立しない年度は、gross_profitそのものが
            # 測定値として信頼できない（取得失敗等）とみなしGP法を使わせない
            # （gp_valをNoneに落とし、後続のgp_estimate計算を自然にスキップ
            # させる）。
            #
            # **net_incomeとの比較（A-2案）は不採用**：「営業利益は通常
            # net_incomeを下回らない」は普遍的に真ではなく、非事業性損益が
            # 大きい年は正当に逆転しうる（HON FY2011で実データ確認済み：
            # revenue/gross_profitの関係は正常〈比率21.8%、他年度と同水準〉
            # だがnet_income$2,067M > GP法値$775M。これは非事業性の利得が
            # 大きい年の正当な結果であり、GP法の入力自体は健全）。
            # net_income比較はGP法の値そのものの正しさではなく別の量との
            # 相対関係を見る代理判定であり、正当なケースを誤って除外する
            # リスクがある。代わりに、GP法の**入力**（revenueと
            # gross_profitの関係）が定義上成立しているかそのものを確認する。
            #
            # 条件は全105銘柄・全年度の実測（2026-08-19）に基づく:
            # revenueが有効な全年度（n=1096）でratio=|gross_profit|/
            # |revenue|は中央値0.543・p99=0.939に収まり、1.0を超える例は
            # 実質存在しない（早期段階の極小額銘柄RCAT 2件を除き、両者とも
            # 標準タグ採用済みでGP法自体は不使用のため実害なし）。
            # revenue=0または未取得なのにgross_profitが非ゼロという組み合わせは
            # VRT FY2018（revenue=0、gross_profit=-$28.65億）で実在確認済み
            # （SPAC合併前の前身法人データの取得不全と推測、詳細は別途
            # BACKLOG登録）。
            gp_rejected_reason = None
            if gp_val is not None:
                rev_val = rev_annual.get(year)
                if (rev_val is None or rev_val == 0) and gp_val != 0:
                    gp_rejected_reason = (
                        f"revenue={rev_val}かつgross_profit={gp_val:,.0f}"
                        f"（revenue=0/未取得なのにgross_profitが非ゼロ、"
                        f"定義上gross_profit=revenue-COGSと矛盾）"
                    )
                    gp_val = None
                elif rev_val and abs(gp_val) > abs(rev_val):
                    gp_rejected_reason = (
                        f"|gross_profit|({abs(gp_val):,.0f}) > |revenue|"
                        f"({abs(rev_val):,.0f})、COGSが負値であることを意味し"
                        f"gross_profitが測定値として信頼できない"
                    )
                    gp_val = None

            gp_estimate = None
            if gp_val is not None and rd_val is not None:
                sga_val = sga_annual.get(year)
                if sga_val is not None:
                    gp_estimate = gp_val - rd_val - sga_val
                else:
                    sm_val = sm_annual.get(year)
                    if sm_val is not None:
                        gp_estimate = gp_val - rd_val - sm_val

            # [[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]対応（2026-08-22）:
            # GP法はRestructuringCharges等の別建て営業費用を控除しないため
            # 系統的に誤差を生む（HON実測: -11.9%〜-38.3%、COHR FY2023実測:
            # +320.9%）。全105銘柄・772件のバックテストでRestructuringCharges
            # 追加が該当行の61%で改善に寄与する（中央値誤差5.82%→4.83%）ことを
            # 確認した上で追加する。gp_estimateが算出できた年度でのみ、同年度の
            # RestructuringChargesタグが存在すれば追加控除する（既存の
            # R&D・SGA/S&M控除ロジック・他の呼び出し元・銘柄には影響しない）。
            # 発見元のCOHR FY2023は誤差+320.9%→-0.96%まで解消することを
            # 実測確認済み（詳細はBACKLOG.md参照）。
            gp_restructuring_val: Optional[float] = None
            if gp_estimate is not None:
                gp_restructuring_val = self._find_fy_tag_value(
                    us_gaap, self._OI_RESTRUCTURING_TAGS, year
                )
                if gp_restructuring_val is not None:
                    gp_estimate = gp_estimate - gp_restructuring_val

            pretax, nonop_adjustment, nonop_items = self._lookup_pretax_and_nonop_adjustment(us_gaap, year)

            chosen_val = None
            chosen_source = None
            match_detail: Dict[str, Any] = {}
            if gp_rejected_reason is not None:
                match_detail["gp_rejected_reason"] = gp_rejected_reason

            # フォールバック向き反転（2026-08-19、実データでGP法が
            # yfinanceと4/4銘柄で完全一致・pretax法は不一致時に-11.4%〜
            # -82.4%外れることが判明したため）: GP法が算出可能なら常に
            # それを採用する。pretax調整法はGP法が構造的に算出不可能な
            # 場合専用のフォールバック。
            if gp_estimate is not None:
                chosen_val = gp_estimate
                chosen_source = "reconstructed_gp"
                if pretax is not None:
                    gap = abs(gp_estimate - pretax)
                    coverage = sum(abs(v) for v in nonop_items.values())
                    ratio = (coverage / gap) if gap > 0 else 1.0
                    match_detail = {
                        "gp_estimate": gp_estimate, "pretax_raw": pretax,
                        # nonop_coverage_ratio: GP法採否の門番ではなく、
                        # 「pretax調整法がどの程度信頼できたか」の事後
                        # 診断指標（高い=pretaxもGP法に近かった、
                        # 低い=pretaxは大きく外れていた）。
                        "nonop_coverage_ratio": round(ratio, 4),
                        # [[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]:
                        # gp_estimateは既にこの値を控除済み。未控除
                        # （RestructuringChargesタグなし）の場合はNone。
                        "restructuring_charges_deducted": gp_restructuring_val,
                    }
                else:
                    match_detail = {
                        "gp_estimate": gp_estimate, "cross_check": "unavailable_no_pretax",
                        "restructuring_charges_deducted": gp_restructuring_val,
                    }
            elif pretax is not None:
                if nonop_adjustment is not None:
                    chosen_val = pretax - nonop_adjustment
                    chosen_source = "reconstructed_pretax"
                    cross_check = (
                        "rejected_by_integrity_guard" if gp_rejected_reason is not None
                        else "unavailable_no_gp_estimate"
                    )
                    match_detail = {"pretax_raw": pretax, "cross_check": cross_check}
                    if gp_rejected_reason is not None:
                        match_detail["gp_rejected_reason"] = gp_rejected_reason

            # [[OPERATING-INCOME-EXTRACTION-GAP-1]] 妥当性ガード
            # （2026-08-16、SOFI検証で発見・追加）: pretax調整法は
            # 「受取利息等は非事業性」という仮定に基づくが、銀行/フィンテック
            # 企業（SOFI等）では受取利息が本業収益そのものであり、この仮定が
            # 成立しない。営業利益は通常net_incomeを下回らない
            # （taxes等でさらに減るため）ことを利用し、pretax調整法の結果が
            # net_incomeを下回る場合は非事業性項目の分類を誤っている強い
            # シグナルとみなし採用しない。GP法（reconstructed_gp）はこの
            # 仮定に依存しないため対象外（JNJ等、税率要因でoi<net_incomeに
            # なる正当なケースを誤って除外しないため）。
            if chosen_source == "reconstructed_pretax" and chosen_val is not None:
                ni_val = ni_annual.get(year)
                if ni_val is not None and chosen_val < ni_val:
                    match_detail["rejected_reason"] = (
                        f"reconstructed_pretax({chosen_val:,.0f}) < net_income({ni_val:,.0f})、"
                        f"非事業性項目の分類ミス（金融/フィンテック企業等で受取利息が"
                        f"本業収益の可能性）を示唆するため不採用"
                    )
                    chosen_val = None
                    chosen_source = None

            # 注: 妥当性ガードで不採用になった年度（chosen_valがNoneに戻った
            # 場合）は、既存の_record()ヘルパー（本関数の外、save呼び出し前の
            # 共通処理）がval=Noneのフィールドをprovenanceごとスキップする
            # 仕様のため、不採用の理由（match_detail["rejected_reason"]）は
            # 最終的なannual_YYYY.jsonには残らない（operating_income=None
            # という結果のみが残る、他の全欠損フィールドと同じ扱い）。
            # 理由を追跡したい場合は本関数を直接呼び出すか、この関数のロジックを
            # 参照すること。
            if chosen_val is not None:
                oi_annual[year] = chosen_val
                oi_prov[year] = {
                    "accn": None, "filed": "", "is_own_data": False, "fy_tag": year,
                    "derived": True, "source": chosen_source,
                    "nonop_items": nonop_items, **match_detail,
                }

    def _backfill_total_liabilities_via_identity(self, extracted: Dict[str, Any]) -> None:
        """
        [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]: XBRL_MAPPING
        ["total_liabilities"]の2番目のフォールバック候補
        LiabilitiesAndStockholdersEquityは、定義上必ずtotal_assetsと
        数学的に一致する（負債合計ではなく貸借対照表の借方・貸方合計
        そのもの）ため、Liabilitiesタグが存在しない年度でこの候補が
        採用されると、total_liabilitiesに実質total_assetsの値が格納
        される。

        適用条件（数学的シグネチャで機械的に検知、銘柄名のハードコードなし）:
          - total_liabilities == total_assets（誤った候補採用の結果）
          - stockholders_equity != 0（等しい場合は正常値の可能性があり対象外）
          - 3項目すべてが同一年度でpresent（Noneでない）

        該当年度についてのみ、貸借対照表恒等式
        （total_liabilities = total_assets - stockholders_equity）で
        逆算した値に置き換える。全母集団シミュレーション（チャット記録）で
        278件全件が計算可能・代替候補タグが存在する7件中5件は逆算値と厳密
        一致、残り2件（NVDA(2015)・RCAT(2023)）も同一accn内では厳密一致
        する軽微差と確認済み。

        採用した値のprovenanceには"derived": Trueを付与する
        （_backfill_gross_profit_from_revenue_cogs()と同型の設計）。
        加えて、逆算元のtotal_assets/stockholders_equity自体が本人データ
        (is_own_data=True)かどうかを"source_is_own_data"に記録する
        （is_own_data はderived値自体を指し常にFalseとなるため、逆算元
        データの確度を別途識別できるようにするための追加フィールド）。
        """
        tl_field = extracted.get("total_liabilities")
        ta_field = extracted.get("total_assets")
        se_field = extracted.get("stockholders_equity")
        if tl_field is None or ta_field is None or se_field is None:
            return

        tl_annual = tl_field.setdefault("annual", {})
        tl_prov = tl_field.setdefault("_annual_provenance", {})
        ta_annual = ta_field.get("annual", {})
        ta_prov = ta_field.get("_annual_provenance", {})
        se_annual = se_field.get("annual", {})
        se_prov = se_field.get("_annual_provenance", {})

        for year, tl_val in list(tl_annual.items()):
            ta_val = ta_annual.get(year)
            se_val = se_annual.get(year)
            if tl_val is None or ta_val is None or se_val is None:
                continue
            if se_val == 0:
                continue
            if tl_val != ta_val:
                continue  # バグの数学的シグネチャに該当しない（既存の正しい値は上書きしない）
            derived = ta_val - se_val
            tl_annual[year] = derived
            ta_own = bool((ta_prov.get(year) or {}).get("is_own_data"))
            se_own = bool((se_prov.get(year) or {}).get("is_own_data"))
            tl_prov[year] = {
                "accn": None, "filed": "", "is_own_data": False, "fy_tag": year,
                "derived": True,
                "source_is_own_data": ta_own and se_own,
            }

    @staticmethod
    def _period_days(start: Optional[str], end: Optional[str]) -> Optional[int]:
        """(start, end)の日数を返す。パース不能・欠損の場合はNone"""
        if not start or not end:
            return None
        try:
            return (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days
        except ValueError:
            return None

    def _find_revenue_end_date_by_value(self, us_gaap: dict, accn: str, val: Any) -> Optional[str]:
        """指定accn内で、revenue候補タグ（XBRL_MAPPING["revenue"]）のうち
        340-380日・値がvalと一致するエントリのend_dateを返す
        （[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案b: 既存のprovenance
        にはend_dateが保持されていないため、値の一致から逆引きする）。
        複数一致した場合は最初に見つかったものを返す（同一accn内の同一
        値・同一期間のタグ重複は実質的に等価なため区別不要）。
        """
        for tag in self.XBRL_MAPPING["revenue"]:
            for entry in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
                if entry.get("accn") != accn or entry.get("val") != val:
                    continue
                days = self._period_days(entry.get("start"), entry.get("end"))
                if days is not None and 340 <= days <= 380:
                    return entry.get("end")
        return None

    # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案a: _find_cost_of_revenue_in_accn()
    # 専用の拡張候補タグリスト（AMD/KO/JNJの本人データに存在するが、
    # XBRL_MAPPING["cost_of_revenue"]〈_extract_values_best_candidate()の
    # 主候補選定に使われる〉には含まれていないCostOfGoodsSoldを追加）。
    # 意図的にXBRL_MAPPING本体とは分離している——本体に追加すると
    # 全105銘柄の主候補選定に影響し、LLY/FCX/CAT/ABBVで新規劣化10件
    # （実データシミュレーションで確認済み）を引き起こすため、影響範囲を
    # 「既に矛盾が確認された年度のみに適用される本メソッド経由の検索」
    # に限定する。
    _COST_OF_REVENUE_ALIGNMENT_CANDIDATES = [
        "CostOfRevenue",
        "CostOfGoodsAndServicesSold",
        "CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization",
        "CostOfGoodsSold",
    ]

    def _find_cost_of_revenue_in_accn(self, us_gaap: dict, accn: str,
                                       end_date: str) -> Optional[tuple]:
        """指定accn・指定end_date（340-380日の年次期間）に一致する
        cost_of_revenue候補タグ（_COST_OF_REVENUE_ALIGNMENT_CANDIDATES
        優先順位順）を検索する（[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]
        案a・案b）。

        Returns: (tag, val, filed) のタプル、なければNone
        """
        for tag in self._COST_OF_REVENUE_ALIGNMENT_CANDIDATES:
            for entry in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
                if entry.get("accn") != accn or entry.get("end") != end_date:
                    continue
                days = self._period_days(entry.get("start"), entry.get("end"))
                if days is None or not (340 <= days <= 380):
                    continue
                val = entry.get("val")
                if val is None:
                    continue
                return (tag, val, entry.get("filed", ""))
        return None

    def _find_gross_profit_end_date_by_value(self, us_gaap: dict, accn: str, val: Any) -> Optional[str]:
        """指定accn内で、gross_profit候補タグ（XBRL_MAPPING["gross_profit"]）
        のうち340-380日・値がvalと一致するエントリのend_dateを返す
        （_find_revenue_end_date_by_value()のgross_profit版、
        [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案a拡張: JNJ型
        ——revenueの本人データが別accnの比較列にしか存在しないが、
        gross_profitは本人データを保持しているケース——のための
        セカンダリアンカー）。
        """
        for tag in self.XBRL_MAPPING["gross_profit"]:
            for entry in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
                if entry.get("accn") != accn or entry.get("val") != val:
                    continue
                days = self._period_days(entry.get("start"), entry.get("end"))
                if days is not None and 340 <= days <= 380:
                    return entry.get("end")
        return None

    def _align_cost_of_revenue_to_revenue_period(self, extracted: Dict[str, Any], us_gaap: dict) -> None:
        """
        [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案b: revenueと
        cost_of_revenueが異なるaccnから独立に採用されている年度についてのみ、
        revenueと同一accn・同一(start,end)期間を持つcost_of_revenue候補
        タグが存在すればそちらを優先採用する。

        適用条件（数学的シグネチャで機械的に検知、銘柄名のハードコードなし。
        [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]と同じ「欠損穴埋め
        のみ・既存の正しい値は上書きしない」ゲート条件）:
          - revenue・cost_of_revenue・gross_profitが同一年度でpresent
            （Noneでない）。gross_profitは実タグ由来の値のみを対象とする
            （本関数は_backfill_gross_profit_from_revenue_cogs()より前に
            実行されるため、この時点でgross_profitがNoneであれば実タグが
            存在しない年度＝比較不能として対象外とする。derived値との
            比較を避けることで、GOOGL(2008)/HON(2008)/SCCO(2009/2010)等
            gross_profit自体が導出値の年度を誤って書き換える巻き添えを
            防ぐ〈実データ検証で発見・是正済み〉）
          - 現に`revenue − cost_of_revenue ≠ gross_profit`という数学的
            矛盾が確認できる年度のみ（矛盾のない年度＝両者のprovenance.accn
            が異なっていても結果的に整合している年度は対象外）
          - revenueと同一accn・同一期間のcost_of_revenue候補が
            company_facts.json上に実在し、かつその値を採用すると
            `revenue − 新cost_of_revenue = gross_profit`が厳密に成立する
            （矛盾が実際に解消する）場合のみ置換する。解消しない場合は
            現状を維持する（KULR(2019)型の巻き添え防止と同じ設計、
            [[SPAC-SHELL-BS-ENTITY-MIXING-1]]の条件④と同種）

        CRM(2013)型（同一accn・別期間の本人データ年度違い）は、accnが
        「一致」しているため本ロジックの対象外となる既知の限界がある
        （案b単独では解決しない、次回セッションでの期間一致精密化の対象）。

        [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案a拡張（2026-09-09、
        JNJ/MRVL/ONDS型対応）: revenueのaccnで候補が見つからない場合、
        gross_profitのaccnもセカンダリアンカーとして試す。JNJ(2017)では
        revenue自体はSEC側の再掲値が本人データと同一の値のまま
        （復元不要）だが、cost_of_revenueだけが後年filingの微修正値
        （$25,439M）に誤って揃っており、本人年度のgross_profit（同一
        accn内に本人データのCostOfGoodsSold $25,354Mが存在）を基準に
        揃えると矛盾が厳密に解消することを確認済み。ゲート条件は
        revenueアンカーと完全に同一（矛盾が現に存在する年度のみ・
        置換後に矛盾が厳密に解消する場合のみ採用）で、探索対象accnを
        増やしているだけであり安全性の性質は変えない。
        """
        rev_field = extracted.get("revenue")
        cogs_field = extracted.get("cost_of_revenue")
        gp_field = extracted.get("gross_profit")
        if rev_field is None or cogs_field is None or gp_field is None:
            return

        rev_annual = rev_field.get("annual", {})
        rev_prov = rev_field.get("_annual_provenance", {})
        gp_annual = gp_field.get("annual", {})
        cogs_annual = cogs_field.setdefault("annual", {})
        cogs_prov = cogs_field.setdefault("_annual_provenance", {})

        for year, cogs_val in list(cogs_annual.items()):
            rev_val = rev_annual.get(year)
            gp_val = gp_annual.get(year)
            if rev_val is None or cogs_val is None or gp_val is None:
                continue
            if (rev_val - cogs_val) == gp_val:
                continue  # 現に矛盾がない（対象外、ゲート条件）
            rev_p = rev_prov.get(year)
            cogs_p = cogs_prov.get(year)
            if not rev_p or not cogs_p:
                continue
            rev_accn = rev_p.get("accn")
            cogs_accn = cogs_p.get("accn")
            gp_p = (gp_field.get("_annual_provenance", {}) or {}).get(year)
            gp_accn = gp_p.get("accn") if gp_p else None

            # アンカー候補: revenue accn（従来通り、第一候補） →
            # gross_profit accn（案a拡張、JNJ/MRVL/ONDS型のセカンダリ
            # アンカー）。
            #
            # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案a再拡張
            # （2026-09-09、CRM(2013)型対応）: revenueアンカーの
            # 「rev_accn != cogs_accn」制約を撤廃した。CRM(2013)は
            # revenue・cost_of_revenueが同一accn（同一10-K内の複数年度
            # 比較列）から採用されているにも関わらず、cost_of_revenueだけ
            # 異なる期間（隣接年度の比較列）を誤って参照していたケース
            # だった。「accnが一致している＝同じ期間を見ている」という
            # 当初の前提が誤りだったと判明（1つのaccnに複数の(start,end)
            # 期間が混在するため）。既存のゲート（矛盾が現に存在する年度
            # のみ・置換後に矛盾が厳密に解消する場合のみ採用）は不変の
            # ため、accnが一致し期間も一致済みの真に正しいケースは
            # 「値が変わらないなら何もしない」で安全にスキップされる。
            anchor_candidates = []
            if rev_accn:
                anchor_candidates.append(("revenue", rev_accn, rev_val))
            if gp_accn and gp_accn != rev_accn:
                anchor_candidates.append(("gross_profit", gp_accn, gp_val))

            for anchor_kind, anchor_accn, anchor_val in anchor_candidates:
                if anchor_kind == "revenue":
                    anchor_end = self._find_revenue_end_date_by_value(us_gaap, anchor_accn, anchor_val)
                else:
                    anchor_end = self._find_gross_profit_end_date_by_value(us_gaap, anchor_accn, anchor_val)
                if anchor_end is None:
                    continue

                aligned = self._find_cost_of_revenue_in_accn(us_gaap, anchor_accn, anchor_end)
                if aligned is None:
                    continue  # このアンカーaccn・同一期間に候補が存在しない、次のアンカーへ

                _, aligned_val, aligned_filed = aligned
                if aligned_val == cogs_val:
                    continue  # 値が変わらないなら何もしない（無用な書き換え回避）
                if (rev_val - aligned_val) != gp_val:
                    continue  # 置換しても矛盾が解消しないなら採用しない（巻き添え防止）

                cogs_annual[year] = aligned_val
                cogs_prov[year] = {
                    "accn": anchor_accn,
                    "filed": aligned_filed,
                    "is_own_data": (rev_p if anchor_kind == "revenue" else gp_p).get("is_own_data", False),
                    "fy_tag": cogs_p.get("fy_tag"),
                    "accn_aligned": True,
                    "accn_aligned_anchor": anchor_kind,
                }
                break  # このyearについては解決済み、次のyearへ

    def _backfill_cost_of_revenue_via_tag_sum(self, extracted: Dict[str, Any], us_gaap: dict) -> None:
        """
        [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案c: cost_of_revenueが
        単一タグでは矛盾を解消できない年度についてのみ、cost_of_revenueの
        採用元accn・同一期間に存在する2つの異なる候補タグ（例:
        `CostOfRevenue`+`CostOfGoodsAndServicesSold`）の合算値を試し、
        合算すると`revenue − 合算値 = gross_profit`が厳密に成立する場合
        のみ採用する（RMBS(2018)実例: 真のコストが2つの独立タグの合算値
        だったケース）。

        _align_cost_of_revenue_to_revenue_period()（案a・b）の後に実行し、
        その時点でなお矛盾が残る年度のみを対象とする。

        安全ゲート（ENTG/TER型の巻き添え防止）:
          - 現に`revenue − cost_of_revenue ≠ gross_profit`という矛盾が
            確認できる年度のみ（ENTG/TERのように2タグが同一filingへの
            重複タグ付けで、既存の単一タグ採用値が既に正しい年度は
            この時点で矛盾なし＝対象外となり、合算による二重計上を
            起こさない）
          - 2つの候補タグは異なるタグ名で、かつ両方が現在のcost_of_revenue
            採用元accn・同一(start,end)期間に実在する場合のみ
          - 合算値を採用すると矛盾が厳密に解消する場合のみ（RMBS(2019)の
            ように合算しても残差が残る場合＝restatement等の別要因の
            可能性が高いため、採用せず現状維持のまま残す）
        """
        rev_field = extracted.get("revenue")
        cogs_field = extracted.get("cost_of_revenue")
        gp_field = extracted.get("gross_profit")
        if rev_field is None or cogs_field is None or gp_field is None:
            return

        rev_annual = rev_field.get("annual", {})
        gp_annual = gp_field.get("annual", {})
        cogs_annual = cogs_field.setdefault("annual", {})
        cogs_prov = cogs_field.setdefault("_annual_provenance", {})

        candidates = self._COST_OF_REVENUE_ALIGNMENT_CANDIDATES

        for year, cogs_val in list(cogs_annual.items()):
            rev_val = rev_annual.get(year)
            gp_val = gp_annual.get(year)
            if rev_val is None or cogs_val is None or gp_val is None:
                continue
            if (rev_val - cogs_val) == gp_val:
                continue  # 現に矛盾がない（対象外、ゲート条件）
            cogs_p = cogs_prov.get(year)
            if not cogs_p:
                continue
            cogs_accn = cogs_p.get("accn")
            if not cogs_accn:
                continue

            # cogs採用元accn・同一期間に存在する候補タグを全て集める
            # （end_dateはcogs_valそのものの逆引きで特定する）
            cogs_end = None
            for tag in candidates:
                for entry in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
                    if entry.get("accn") != cogs_accn or entry.get("val") != cogs_val:
                        continue
                    days = self._period_days(entry.get("start"), entry.get("end"))
                    if days is not None and 340 <= days <= 380:
                        cogs_end = entry.get("end")
                        break
                if cogs_end:
                    break
            if cogs_end is None:
                continue

            present_tags: Dict[str, float] = {}
            for tag in candidates:
                for entry in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
                    if entry.get("accn") != cogs_accn or entry.get("end") != cogs_end:
                        continue
                    days = self._period_days(entry.get("start"), entry.get("end"))
                    if days is None or not (340 <= days <= 380):
                        continue
                    val = entry.get("val")
                    if val is not None:
                        present_tags[tag] = val
                    break

            if len(present_tags) < 2:
                continue  # 合算候補が1つ以下なら合算のしようがない

            # 2タグの組み合わせを総当たりし、厳密に矛盾解消する組み合わせのみ採用
            tag_items = list(present_tags.items())
            for i in range(len(tag_items)):
                for j in range(i + 1, len(tag_items)):
                    tag_a, val_a = tag_items[i]
                    tag_b, val_b = tag_items[j]
                    summed = val_a + val_b
                    if summed == cogs_val:
                        continue  # 合算しても既存値と同じなら意味がない
                    if (rev_val - summed) != gp_val:
                        continue  # 矛盾が解消しないなら不採用（巻き添え防止）

                    cogs_annual[year] = summed
                    cogs_prov[year] = {
                        "accn": cogs_accn,
                        "filed": cogs_p.get("filed", ""),
                        "is_own_data": cogs_p.get("is_own_data", False),
                        "fy_tag": cogs_p.get("fy_tag"),
                        "tag_sum_backfilled": True,
                        "tag_sum_components": [tag_a, tag_b],
                    }
                    break
                else:
                    continue
                break

    def _align_revenue_to_cost_gross_profit_identity(self, extracted: Dict[str, Any], us_gaap: dict) -> None:
        """
        [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案d: revenue・
        cost_of_revenue・gross_profitが同一accn・同一期間からの採用で
        あるにも関わらず矛盾が残る年度についてのみ、同一accn・同一期間に
        存在する別のrevenue候補タグ（例:
        `RevenueFromContractWithCustomerExcludingAssessedTax`）へ切り替える
        と矛盾が厳密に解消する場合に限り採用する（BSY(2019)実例:
        `Revenues`＄734.8Mではなく`RevenueFromContractWithCustomerExcluding
        AssessedTax`＄736.7Mが正しく、cost_of_revenue・gross_profitは
        元々同一accn内の正しい値だった）。

        案dの一般適用（revenue優先順位の恒久的な変更）は全105銘柄
        シミュレーションで「極めて危険」と確定済み（WMT/XOM等主力銘柄を
        破壊するリスク）のため、本メソッドは以下の極めて狭いゲート条件
        でのみ発動する:
          - revenue・cost_of_revenue・gross_profitの採用元accnが
            3つとも完全に一致している（cross-accn問題ではなく、
            同一filing内でのrevenueタグ誤選択のみを対象とする）
          - その一致accn・同一期間で現に矛盾が確認できる年度のみ
          - 同一accn・同一期間に存在する別のrevenue候補タグへ切り替えると
            矛盾が厳密に解消する場合のみ（他銘柄13件中12件はgenuine定義差
            のため解消せず、この場合は現状を一切変更しない）
        """
        rev_field = extracted.get("revenue")
        cogs_field = extracted.get("cost_of_revenue")
        gp_field = extracted.get("gross_profit")
        if rev_field is None or cogs_field is None or gp_field is None:
            return

        rev_annual = rev_field.get("annual", {})
        rev_prov = rev_field.setdefault("_annual_provenance", {})
        cogs_annual = cogs_field.get("annual", {})
        cogs_prov = cogs_field.get("_annual_provenance", {})
        gp_annual = gp_field.get("annual", {})
        gp_prov = gp_field.get("_annual_provenance", {})

        for year, rev_val in list(rev_annual.items()):
            cogs_val = cogs_annual.get(year)
            gp_val = gp_annual.get(year)
            if rev_val is None or cogs_val is None or gp_val is None:
                continue
            if (rev_val - cogs_val) == gp_val:
                continue  # 現に矛盾がない（対象外、ゲート条件）

            rev_p = rev_prov.get(year)
            cogs_p = cogs_prov.get(year)
            gp_p = gp_prov.get(year)
            if not rev_p or not cogs_p or not gp_p:
                continue
            rev_accn = rev_p.get("accn")
            cogs_accn = cogs_p.get("accn")
            gp_accn = gp_p.get("accn")
            if not rev_accn or not (rev_accn == cogs_accn == gp_accn):
                continue  # 3フィールドが同一accnでない（cross-accn型は案a/bの対象、本メソッド対象外）

            rev_end = self._find_revenue_end_date_by_value(us_gaap, rev_accn, rev_val)
            if rev_end is None:
                continue

            for tag in self.XBRL_MAPPING["revenue"]:
                found_alt = None
                for entry in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
                    if entry.get("accn") != rev_accn or entry.get("end") != rev_end:
                        continue
                    days = self._period_days(entry.get("start"), entry.get("end"))
                    if days is None or not (340 <= days <= 380):
                        continue
                    val = entry.get("val")
                    if val is None or val == rev_val:
                        continue  # 現在値と同じなら候補にならない
                    found_alt = (tag, val, entry.get("filed", ""))
                    break
                if found_alt is None:
                    continue
                alt_tag, alt_val, alt_filed = found_alt
                if (alt_val - cogs_val) != gp_val:
                    continue  # 切り替えても矛盾が解消しないなら不採用
                rev_annual[year] = alt_val
                rev_prov[year] = {
                    "accn": rev_accn,
                    "filed": alt_filed,
                    "is_own_data": rev_p.get("is_own_data", False),
                    "fy_tag": rev_p.get("fy_tag"),
                    "revenue_tag_realigned": True,
                    "revenue_tag_realigned_to": alt_tag,
                }
                break

    # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案e: _align_revenue_and_
    # cost_to_gross_profit_own_accn()専用の拡張revenue候補タグリスト。
    # XBRL_MAPPING["revenue"]に加え、ONDS(2017)の本人データが使う
    # `SalesRevenueGoodsNet`（商品売上高、サービス売上高と分離して
    # 報告する小型企業に見られるタグ）を追加。案aのcost_of_revenue版
    # 拡張リストと同じ理由で、主候補選定に使われるXBRL_MAPPING本体とは
    # 意図的に分離する。
    _REVENUE_ALIGNMENT_CANDIDATES = [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "RevenueFromContractWithCustomer",
        "SalesRevenueNet",
        "TotalRevenue",
        "RevenuesNetOfInterestExpense",
        "SalesRevenueGoodsNet",
    ]

    def _align_revenue_and_cost_to_gross_profit_own_accn(self, extracted: Dict[str, Any], us_gaap: dict) -> None:
        """
        [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案e: revenue・
        cost_of_revenueの両方が、gross_profitの採用元accn・同一期間とは
        異なる誤ったaccnから採用されている年度についてのみ、
        gross_profitの採用元accn・同一期間に存在するrevenue候補タグと
        cost_of_revenue候補タグの両方を調べ、その差が現在のgross_profit
        と厳密に一致する場合に限り、revenue・cost_of_revenueの両方を
        そのaccnの値へ同時に置き換える。

        既存の案a〜dはいずれも「revenue・gross_profitは正しい前提で
        cost_of_revenueのみ是正する」（案a・b・c）か「revenue側のみを
        是正する」（案d）設計だったが、以下2つの実例ではgross_profitを
        アンカーとして固定し、revenue・cost_of_revenueの**両方**を同時に
        是正する必要があることが判明した:

        - ONDS(2017)実例: gross_profitは本人データ（accn内に
          `SalesRevenueGoodsNet`=$2,846・`CostOfGoodsSold`=$9,073が
          存在し、2,846−9,073=−6,227=gross_profitと厳密に一致）。一方
          revenue・cost_of_revenueは共に別の後年filing（SPAC合併疑いの
          ある異なる報告主体、[[SPAC-SHELL-BS-ENTITY-MIXING-1]]と同種）
          から誤って採用されており、$274,403・$79,768という本人データと
          二桁近く乖離した値になっていた。
        - RMBS(2019)実例: gross_profitの採用元accn（2022年提出の
          10-K）に、revenue候補`RevenueFromContractWithCustomer
          IncludingAssessedTax`=$227,603千・cost_of_revenue候補
          `CostOfRevenue`=$51,375千が同一期間で存在し、227,603−51,375
          =176,228=gross_profitと厳密に一致する。一方revenue・
          cost_of_revenueは共に本人年度の10-K（2020年提出）の値
          （restatement前の値、$224,027千・$24,219千）を使っており、
          2021年の10-K/Aによる事後的な原価区分の見直し
          （restatement）を反映できていなかった。

        安全ゲート（他銘柄への巻き添え防止）:
          - 現に`revenue − cost_of_revenue ≠ gross_profit`という矛盾が
            確認できる年度のみ（矛盾のない年度は対象外）
          - gross_profitの採用元accn・同一期間に、revenue候補タグと
            cost_of_revenue候補タグの両方が実在する場合のみ
          - その2つの値の差が現在のgross_profitと厳密に一致する場合の
            み採用（部分的にしか解消しない組み合わせは不採用のまま
            現状維持）
          - gross_profit自体は変更しない（アンカーとして固定するのみ）
        """
        rev_field = extracted.get("revenue")
        cogs_field = extracted.get("cost_of_revenue")
        gp_field = extracted.get("gross_profit")
        if rev_field is None or cogs_field is None or gp_field is None:
            return

        rev_annual = rev_field.get("annual", {})
        rev_prov = rev_field.setdefault("_annual_provenance", {})
        cogs_annual = cogs_field.get("annual", {})
        cogs_prov = cogs_field.setdefault("_annual_provenance", {})
        gp_annual = gp_field.get("annual", {})
        gp_prov = gp_field.get("_annual_provenance", {})

        for year, gp_val in list(gp_annual.items()):
            rev_val = rev_annual.get(year)
            cogs_val = cogs_annual.get(year)
            if rev_val is None or cogs_val is None or gp_val is None:
                continue
            if (rev_val - cogs_val) == gp_val:
                continue  # 現に矛盾がない（対象外、ゲート条件）

            gp_p = gp_prov.get(year)
            if not gp_p:
                continue
            gp_accn = gp_p.get("accn")
            if not gp_accn:
                continue

            gp_end = self._find_gross_profit_end_date_by_value(us_gaap, gp_accn, gp_val)
            if gp_end is None:
                continue

            # gp採用元accn・同一期間に存在するrevenue候補タグを探す
            found_rev = None
            for tag in self._REVENUE_ALIGNMENT_CANDIDATES:
                for entry in us_gaap.get(tag, {}).get("units", {}).get("USD", []):
                    if entry.get("accn") != gp_accn or entry.get("end") != gp_end:
                        continue
                    days = self._period_days(entry.get("start"), entry.get("end"))
                    if days is None or not (340 <= days <= 380):
                        continue
                    val = entry.get("val")
                    if val is None:
                        continue
                    found_rev = (tag, val, entry.get("filed", ""))
                    break
                if found_rev:
                    break
            if found_rev is None:
                continue

            # 同一accn・同一期間に存在するcost_of_revenue候補タグを探す
            found_cogs = self._find_cost_of_revenue_in_accn(us_gaap, gp_accn, gp_end)
            if found_cogs is None:
                continue

            rev_tag, cand_rev_val, rev_filed = found_rev
            cogs_tag, cand_cogs_val, cogs_filed = found_cogs

            if cand_rev_val == rev_val and cand_cogs_val == cogs_val:
                continue  # 両方とも値が変わらないなら何もしない（無用な書き換え回避）
            if (cand_rev_val - cand_cogs_val) != gp_val:
                continue  # 置換しても矛盾が解消しないなら採用しない（巻き添え防止）

            rev_annual[year] = cand_rev_val
            rev_prov[year] = {
                "accn": gp_accn,
                "filed": rev_filed,
                "is_own_data": gp_p.get("is_own_data", False),
                "fy_tag": rev_prov.get(year, {}).get("fy_tag"),
                "gp_anchor_realigned": True,
                "gp_anchor_realigned_tag": rev_tag,
            }
            cogs_annual[year] = cand_cogs_val
            cogs_prov[year] = {
                "accn": gp_accn,
                "filed": cogs_filed,
                "is_own_data": gp_p.get("is_own_data", False),
                "fy_tag": cogs_prov.get(year, {}).get("fy_tag"),
                "gp_anchor_realigned": True,
                "gp_anchor_realigned_tag": cogs_tag,
            }

    def _extract_values(self, us_gaap: dict, xbrl_keys: List[str], use_max: bool = False, merge_all_tags: bool = False,
                         fiscal_end_month: int = 12, accn_reportdate: Optional[Dict[str, str]] = None,
                         field_name: str = "", collisions_out: Optional[List[Dict[str, Any]]] = None,
                         anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                         extra_anchors: Optional[List[tuple]] = None,
                         fy_mismatches_out: Optional[List[Dict[str, Any]]] = None,
                         boundary_collisions_out: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        指定されたXBRLキーから値を抽出

        Args:
            us_gaap: SEC XBRL データ
            xbrl_keys: 優先順位順のXBRLキーリスト
            use_max: 同一期間に複数値がある場合、最大値を使用（株式数向け）
                     Trueの場合、全XBRLキーを検索して最大値を採用
            merge_all_tags: 年代ごとにXBRLタグが切り替わる銘柄向け。全キーの値を統合する
            accn_reportdate: {accn: reportDate} のマッピング（本人データ判定用。
                              FY52WEEK-BUCKET-MISPLACE-1根本修正）
            field_name: ログ記録用のフィールド名（"revenue"等）
            collisions_out: 本人データ同士のfyキー衝突を記録するリスト（呼び出し元で集約）
            anchor_month/anchor_day: 決算アンカー日（ARCH-DATA-1ステージ2）。
                              determine_fiscal_year()にそのまま渡す
            extra_anchors: 主anchor以外の有意なクラスタのアンカー日候補リスト
                              （[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②）。
                              determine_fiscal_year()にそのまま渡す
            fy_mismatches_out: 採用エントリのfyタグと年度バケツキーの不一致を記録する
                              リスト（ARCH-DATA-1ステージ3: fyタグ裏取り、呼び出し元で集約）
            boundary_collisions_out: 本人データ側と既存（フォールバック）側の生fyタグが
                              異なるまま同一年度バケツで競合したケースを記録するリスト
                              （FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1、呼び出し元で集約）

        Returns:
            dict: {
                "annual": {2024: value, 2023: value, ...},
                "quarterly": {"2024Q1": value, ...}
            }
        """
        if use_max or merge_all_tags:
            return self._extract_values_merged(us_gaap, xbrl_keys, use_max, fiscal_end_month,
                                                accn_reportdate=accn_reportdate, field_name=field_name,
                                                collisions_out=collisions_out,
                                                anchor_month=anchor_month, anchor_day=anchor_day,
                                                extra_anchors=extra_anchors,
                                                fy_mismatches_out=fy_mismatches_out,
                                                boundary_collisions_out=boundary_collisions_out)
        return self._extract_values_best_candidate(us_gaap, xbrl_keys, fiscal_end_month,
                                                    accn_reportdate=accn_reportdate, field_name=field_name,
                                                    collisions_out=collisions_out,
                                                    anchor_month=anchor_month, anchor_day=anchor_day,
                                                    extra_anchors=extra_anchors,
                                                    fy_mismatches_out=fy_mismatches_out,
                                                    boundary_collisions_out=boundary_collisions_out)

    def _find_entry_by_end_date(self, us_gaap: dict, tag: str, end_date: str,
                                 forms: tuple) -> Optional[Dict[str, Any]]:
        """指定タグ・指定end_date・指定form群に一致するエントリを1件返す
        （NVDA-STI-TAG-UNIDENTIFIED-1: _apply_cross_filing_tags()専用の
        ピンポイント検索。既存の_collect_own_data_annual/_instantが持つ
        accn_reportdate自己一致チェックを意図的に迂回する唯一の経路のため、
        TICKER_RESTRICTIONSのcross_filing_tagsに明示登録された呼び出し元
        からのみ使用すること）。

        複数ヒットした場合（同一end_dateへの10-K/A訂正申告等）はfiled日が
        最新のものを採用する。

        Returns:
            該当エントリのdict（"val"/"form"/"accn"/"filed"等）、なければNone
        """
        candidates = []
        units = us_gaap.get(tag, {}).get("units", {})
        for entry in units.get("USD", []):
            if entry.get("form") not in forms:
                continue
            if entry.get("end") != end_date:
                continue
            if entry.get("val") is None:
                continue
            candidates.append(entry)
        if not candidates:
            return None
        candidates.sort(key=lambda e: e.get("filed", ""))
        return candidates[-1]

    def _apply_cross_filing_tags(self, us_gaap: dict, extracted: Dict[str, Any],
                                  cross_filing_config: Dict[str, tuple]) -> None:
        """NVDA-STI-TAG-UNIDENTIFIED-1（ANOMALY-PATTERN-CATALOG-1型C: 資産クラス
        変化・当年度未タグ化型）向け。TICKER_RESTRICTIONSのcross_filing_tagsに
        明示登録されたticker×period×fieldの組み合わせについてのみ、複数XBRL
        タグを指定end_date・指定form制限で直接検索し合算した値で
        extracted[field]の該当バケツ（annual/quarterly）を上書きする。

        既存の_collect_own_data_annual/_instantが持つ`form in (10-K, 10-K/A)`
        フィルタ・accn_reportdate自己一致チェックはここでは一切参照しない
        （_find_entry_by_end_date経由の意図的な迂回）。本関数はcross_filing_tags
        に明示登録された組み合わせにのみ発火するため、他の全銘柄・全フィールド・
        当該ticker自身の他フィールドの既存抽出結果には一切影響しない。

        periodがint（年度）の場合はannualバケツ、str（例:"2027Q1"、parser.pyの
        quarter_key形式 f"{fy}{fp}"）の場合はquarterlyバケツを上書きする。
        合算対象タグの一部が指定end_date・form条件で見つからない場合、その
        periodの上書きはスキップする（中途半端な部分合算値を書き込まない）。
        """
        for field_name, period_specs in cross_filing_config.items():
            field_result = extracted.setdefault(field_name, {"annual": {}, "quarterly": {}})
            for spec in period_specs:
                period = spec["period"]
                end_date = spec["end_date"]
                components = spec["components"]

                total = 0.0
                source_tags = []
                found_all = True
                for comp in components:
                    entry = self._find_entry_by_end_date(us_gaap, comp["tag"], end_date, comp["forms"])
                    if entry is None:
                        found_all = False
                        break
                    total += entry["val"]
                    source_tags.append(comp["tag"])
                if not found_all:
                    continue

                bucket = "annual" if isinstance(period, int) else "quarterly"
                field_result[bucket][period] = total

                residual_pct = spec.get("approx_residual_pct")
                if bucket == "annual":
                    prov = field_result.setdefault("_annual_provenance", {})
                    prov[period] = {
                        "is_own_data": True,
                        "is_approximated": residual_pct is not None,
                        "residual_pct": residual_pct,
                        "combined_tags": source_tags,
                    }

    def _tag_quarterly_corroboration(self, us_gaap: dict, tag: str, annual_end_date: str,
                                      annual_val: float) -> bool:
        """
        REVENUE-TAG-PRIORITY-FRAGILE-1: 指定タグの年次end_date・年次値が、
        同一タグの10-Q単独四半期実績（Q1〜Q3、80〜100日）による裏付けを
        持つかを判定する。

        annual_end_dateまでの過去366日以内にある単独四半期エントリ（重複
        end_dateは1件に統合）が3件以上存在し、それらの合計を年次値から
        差し引いた残差（Q4相当）が正の値で、かつQ1〜Q3平均から極端に
        乖離していない（1.5倍以内）場合に「裏付けあり」とする。判定基準は
        意図的に緩め（早期成長企業の四半期変動が大きいケースも許容する
        ため）に設定しており、目的は「タグが四半期分解できる実在の
        集計値か、それとも年次エントリしか存在しない断片的・誤タグ付けの
        可能性がある値か」の大まかな判別であり、厳密な整合性検証ではない。
        """
        if tag not in us_gaap or annual_val is None:
            return False
        units = us_gaap[tag].get("units", {}).get("USD", [])
        try:
            annual_dt = datetime.strptime(annual_end_date, '%Y-%m-%d')
        except (ValueError, TypeError):
            return False

        standalone_qs: Dict[str, float] = {}
        for entry in units:
            if entry.get("form") != "10-Q":
                continue
            end = entry.get("end", "")
            start = entry.get("start", "")
            if not end or not start or len(end) < 10 or len(start) < 10:
                continue
            try:
                end_dt = datetime.strptime(end, '%Y-%m-%d')
                start_dt = datetime.strptime(start, '%Y-%m-%d')
            except ValueError:
                continue
            if not (80 <= (end_dt - start_dt).days <= 100):
                continue  # 単独四半期（YTD等は除外）のみ対象
            if not (0 <= (annual_dt - end_dt).days <= 366):
                continue  # 当該年度の直近4四半期の範囲外
            val = entry.get("val")
            if val is None:
                continue
            standalone_qs[end] = val  # 同一end_dateの重複filingは後勝ちで統合

        if len(standalone_qs) < 3:
            return False
        total_q = sum(standalone_qs.values())
        implied_q4 = annual_val - total_q
        if implied_q4 <= 0:
            return False
        avg_q = total_q / len(standalone_qs)
        return abs(implied_q4 - avg_q) / avg_q < 1.5

    def _collect_own_data_annual(self, us_gaap: dict, xbrl_keys: List[str],
                                  accn_reportdate: Dict[str, str], fiscal_end_month: int, field_name: str,
                                  collisions_out: Optional[List[Dict[str, Any]]],
                                  anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                                  extra_anchors: Optional[List[tuple]] = None) -> Dict[int, Any]:
        """
        reportDate==end_dateが成立する「本人の当期データ」のみを対象に、
        fyタグを年度キーとして採用した値マップを構築する
        （FY52WEEK-BUCKET-MISPLACE-1根本修正: determine_fiscal_year()の月のみ判定を
        経由せず、企業自身が申告したfyタグを信頼する）。

        fp=="Q4"も候補に含める: DELLの真のFY2019 10-K原本はfp="Q4"でタグ付けされており
        fp=="FY"限定では原本自体が候補から除外されてしまう（本調査で確認済み）。

        form=="10-K/A"（訂正申告）も候補に含める（ARCH-DATA-1ステージ1）:
        10-K/Aで再提出された期間もreportDate==end_dateが成立すれば本人データ
        として扱う。ただし同一タグ内で元の10-Kとその10-K/Aが同一(fy, end_date)を
        報告する場合は下記のfiled日タイブレークで新しい方（通常は10-K/A）を採用する。

        同一fyキーに複数の本人end_dateが競合した場合（CRM/FCX/CAKE/HON/COHR/AVAV/
        FICO/NVDA等で実在確認済みの、filing代行者側のタグ付け起因と推測される矛盾）は
        まずdetermine_fiscal_year()フォールバックで両end_dateが自然に別の年度へ
        分離できないか確認する。分離できる場合（CRM/FCX等の固定月決算企業。
        fyタグだけが誤っており日付ベースの判定自体は曖昧でない）は、共有された
        誤ったfyタグを使わず各自のフォールバック年度をキーとして採用する
        （これを怠ると、fyタグの衝突が全く新しい真値喪失を引き起こす回帰と
        なることを確認済み——CRMの真のFY2025値がFY2026値に上書きされて消失した
        実例で発覚）。フォールバックでも分離できない場合（CAKE。52/53週の
        月またぎとfyタグ誤りが同一年度で重なるケース）のみ、end_dateが
        新しい方を優先するtie-breakを適用する。

        Returns:
            dict: {fy: (val, end_date, accn, filed, raw_fy_tag)}
        """
        # fy -> {end_date: {"val":..., "accn":..., "filed":..., "key":...}}
        # "key"は採用元のXBRLタグ名（xbrl_keysの優先順位を尊重するために保持する）
        by_fy: Dict[int, Dict[str, Dict[str, Any]]] = {}
        for key in xbrl_keys:
            if key not in us_gaap:
                continue
            units = us_gaap[key].get("units", {})
            for unit_type in ["USD", "shares", "USD/shares"]:
                if unit_type not in units:
                    continue
                for entry in units[unit_type]:
                    if entry.get("form") not in ("10-K", "10-K/A") or entry.get("fp") not in ("FY", "Q4"):
                        continue
                    fy = entry.get("fy")
                    val = entry.get("val")
                    end_date = entry.get("end", "")
                    start_date = entry.get("start", "")
                    accn = entry.get("accn")
                    filed = entry.get("filed", "")
                    if val is None or fy is None or not end_date or len(end_date) < 10:
                        continue
                    if not start_date or len(start_date) < 10:
                        continue
                    try:
                        days = (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days
                    except ValueError:
                        continue
                    if not (340 <= days <= 380):
                        # 真の年次期間（約365日）以外は除外する。10-K内に選択四半期データ
                        # （例: JNJの90日間Q4内訳）が form=10-K・fp=FY で同一end_dateとして
                        # 混入し、reportDate一致だけでは年次実績と区別できないため
                        # （JNJ FY2011のQ4内訳$218Mが本人データと誤判定される問題への対応）
                        continue
                    if accn_reportdate.get(accn) != end_date:
                        continue  # 本人データではない（比較年度再掲・IPO前データ等）

                    # 同一(fy, end_date)に複数タグが該当する場合、xbrl_keysの優先順位
                    # （先勝ち）を尊重する。WMTはRevenues（総収益）とSalesRevenueNet
                    # （純売上高）を同一filingで併記しており、後から処理したタグで
                    # 無条件上書きすると優先順位の低いタグが勝ってしまう
                    # （WMT: Revenues $485,651M が優先されるべきところSalesRevenueNet
                    # $482,229M に上書きされる回帰を検出・修正）
                    fy_bucket = by_fy.setdefault(fy, {})
                    existing = fy_bucket.get(end_date)
                    if existing is None:
                        fy_bucket[end_date] = {"val": val, "accn": accn, "filed": filed, "key": key}
                    elif existing["key"] == key:
                        # 同一タグ内で同一(fy, end_date)が複数filingに登場する場合
                        # （元の10-Kとその10-K/A訂正申告等）はfiled日が新しい方を採用する
                        # （ARCH-DATA-1ステージ1: 値の確定）。異なるタグ間の優先順位
                        # （上記WMT対応）はここでは変更しない（先勝ちのまま）。
                        if filed and filed > existing.get("filed", ""):
                            fy_bucket[end_date] = {"val": val, "accn": accn, "filed": filed, "key": key}
                    elif field_name == "revenue":
                        # REVENUE-TAG-PRIORITY-FRAGILE-1: 異なるタグが同一
                        # (fy, end_date)で競合した場合、従来はxbrl_keysの
                        # 優先順位（先勝ち）を無条件に尊重していたが、これは
                        # 「先に列挙されたタグがたまたま正しい」という前提に
                        # 依存する脆弱な設計だった（ASTS・TDYで実例確認、
                        # 2026-09-09対応）。全105銘柄相当の実データ調査の
                        # 結果、現在の勝者が10-Q単独四半期実績による裏付け
                        # （Q1〜Q3合計から逆算した残差=Q4相当が正の妥当な値）
                        # を一切持たず、かつ今回の候補が持つ場合に限り、
                        # 頑健な四半期整合性を優先して逆転させる（TDYで
                        # 実証済み: Revenuesタグは同年度の10-Q実績が一切
                        # 存在しないがSalesRevenueNetは存在し四半期合計と
                        # 完全一致）。両者とも裏付けを持つ場合は既存の
                        # 優先順位を変更しない（CAT/WMT/XOM/LYFT/PM/CEG/
                        # VST/FCXで実例確認済みの通り、両タグが真に異なる
                        # 会計上の集計範囲—例: CATの「Financial Products
                        # 含む総売上」vs「Machinery/Energy/Transportation
                        # セグメント売上」—を意味し、数値の優劣だけでは
                        # どちらが望ましいか決められないため、この場合は
                        # 既存のタグ優先順位に委ねる）。
                        if (not self._tag_quarterly_corroboration(us_gaap, existing["key"], end_date, existing["val"])
                                and self._tag_quarterly_corroboration(us_gaap, key, end_date, val)):
                            fy_bucket[end_date] = {"val": val, "accn": accn, "filed": filed, "key": key}

        winners: Dict[int, tuple] = {}  # fy -> (val, end_date, accn, filed, raw_fy_tag)
        # raw_fy_tag はこのエントリ群が実際にXBRLで申告していたfyタグ（=外側ループのfy）を
        # そのまま保持する。placementキー（winnersの辞書キー）はフォールバック分離・
        # tie-breakの結果fyと異なる場合があるため、両者を区別してARCH-DATA-1ステージ3
        # （fyタグ裏取り）用に保持する。
        for fy, end_date_vals in by_fy.items():
            if len(end_date_vals) == 1:
                (end_date, info), = end_date_vals.items()
                winners[fy] = (info["val"], end_date, info["accn"], info["filed"], fy)
                continue

            # 同一fyキーに複数の異なる本人end_dateが競合
            fallback_years = {
                end_date: determine_fiscal_year(datetime.strptime(end_date, '%Y-%m-%d'), fiscal_end_month,
                                                 anchor_month, anchor_day, extra_anchors)
                for end_date in end_date_vals
            }
            if len(set(fallback_years.values())) == len(end_date_vals):
                # フォールバックが自然に分離できる → 誤ったfyタグではなく
                # 各end_date自身のフォールバック年度をキーにする（CRM/FCX型）
                for end_date, info in end_date_vals.items():
                    winners[fallback_years[end_date]] = (info["val"], end_date, info["accn"], info["filed"], fy)
                if collisions_out is not None:
                    collisions_out.append({
                        "field": field_name, "fy": fy,
                        "end_dates": sorted(end_date_vals.keys()),
                        "resolution": "fyタグ衝突だがフォールバック年度で自然分離",
                    })
            else:
                # フォールバックも同一年度に衝突する（CAKE型・52/53週またぎと
                # fyタグ誤りが同一年度で重複）→ end_dateが新しい方を優先
                newest_end = max(end_date_vals.keys())
                newest_info = end_date_vals[newest_end]
                winners[fy] = (newest_info["val"], newest_end, newest_info["accn"], newest_info["filed"], fy)
                if collisions_out is not None:
                    collisions_out.append({
                        "field": field_name, "fy": fy,
                        "end_dates": sorted(end_date_vals.keys()),
                        "resolution": "end_date新しい方を採用(フォールバックも衝突)",
                    })

        return winners

    def _collect_own_data_instant(self, us_gaap: dict, xbrl_keys: List[str],
                                   accn_reportdate: Dict[str, str], fiscal_end_month: int, field_name: str,
                                   collisions_out: Optional[List[Dict[str, Any]]],
                                   anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                                   extra_anchors: Optional[List[tuple]] = None) -> Dict[int, Any]:
        """
        instant fact（BS項目・RPO残高等、単一時点のend_dateのみを持ちstart_dateを
        持たないXBRL概念）向けに、reportDate==end_dateが成立する「本人の当期データ」
        を判定する（ARCH-DATA-1残課題④）。

        _collect_own_data_annual()との違いはstart_date・期間長（340-380日）フィルタを
        持たない点のみ。instant factのXBRL instant contextには元々start属性が存在せず、
        _collect_own_data_annual()のstart_date必須フィルタ（duration fact向けに、
        10-K内に混入する90日間の四半期内訳等を除外する目的）はinstant factには
        意味を持たない（そもそも比較対象となる「期間長」自体が存在しない）ため、
        同フィルタを持たない別実装として複製する。

        fyタグ衝突時のフォールバック分離・end_date新しい方優先のtie-breakロジックは
        _collect_own_data_annual()と同一（詳細は同メソッドのdocstring参照）。

        Returns:
            dict: {fy: (val, end_date, accn, filed, raw_fy_tag)}
        """
        by_fy: Dict[int, Dict[str, Dict[str, Any]]] = {}
        for key in xbrl_keys:
            if key not in us_gaap:
                continue
            units = us_gaap[key].get("units", {})
            for unit_type in ["USD", "shares", "USD/shares"]:
                if unit_type not in units:
                    continue
                for entry in units[unit_type]:
                    if entry.get("form") not in ("10-K", "10-K/A") or entry.get("fp") not in ("FY", "Q4"):
                        continue
                    fy = entry.get("fy")
                    val = entry.get("val")
                    end_date = entry.get("end", "")
                    accn = entry.get("accn")
                    filed = entry.get("filed", "")
                    if val is None or fy is None or not end_date or len(end_date) < 10:
                        continue
                    if accn_reportdate.get(accn) != end_date:
                        continue  # 本人データではない（比較年度再掲・IPO前データ等）

                    # 同一(fy, end_date)に複数タグが該当する場合、xbrl_keysの優先順位
                    # （先勝ち）を尊重する（_collect_own_data_annual()と同じ方針）
                    fy_bucket = by_fy.setdefault(fy, {})
                    existing = fy_bucket.get(end_date)
                    if existing is None:
                        fy_bucket[end_date] = {"val": val, "accn": accn, "filed": filed, "key": key}
                    elif existing["key"] == key:
                        if filed and filed > existing.get("filed", ""):
                            fy_bucket[end_date] = {"val": val, "accn": accn, "filed": filed, "key": key}

        winners: Dict[int, tuple] = {}
        for fy, end_date_vals in by_fy.items():
            if len(end_date_vals) == 1:
                (end_date, info), = end_date_vals.items()
                winners[fy] = (info["val"], end_date, info["accn"], info["filed"], fy)
                continue

            # 同一fyキーに複数の異なる本人end_dateが競合
            fallback_years = {
                end_date: determine_fiscal_year(datetime.strptime(end_date, '%Y-%m-%d'), fiscal_end_month,
                                                 anchor_month, anchor_day, extra_anchors)
                for end_date in end_date_vals
            }
            if len(set(fallback_years.values())) == len(end_date_vals):
                for end_date, info in end_date_vals.items():
                    winners[fallback_years[end_date]] = (info["val"], end_date, info["accn"], info["filed"], fy)
                if collisions_out is not None:
                    collisions_out.append({
                        "field": field_name, "fy": fy,
                        "end_dates": sorted(end_date_vals.keys()),
                        "resolution": "fyタグ衝突だがフォールバック年度で自然分離",
                    })
            else:
                newest_end = max(end_date_vals.keys())
                newest_info = end_date_vals[newest_end]
                winners[fy] = (newest_info["val"], newest_end, newest_info["accn"], newest_info["filed"], fy)
                if collisions_out is not None:
                    collisions_out.append({
                        "field": field_name, "fy": fy,
                        "end_dates": sorted(end_date_vals.keys()),
                        "resolution": "end_date新しい方を採用(フォールバックも衝突)",
                    })

        return winners

    def _collect_own_data(self, us_gaap: dict, xbrl_keys: List[str],
                           accn_reportdate: Dict[str, str], fiscal_end_month: int, field_name: str,
                           collisions_out: Optional[List[Dict[str, Any]]],
                           anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                           extra_anchors: Optional[List[tuple]] = None) -> Dict[int, Any]:
        """duration fact / instant factに応じて_collect_own_data_annual/_instantへ振り分ける
        （ARCH-DATA-1残課題④）。呼び出し元（_extract_values_merged/_extract_values_best_candidate）
        は本メソッド経由で呼び出すことで、フィールド種別を意識せず本人データ判定を利用できる。
        """
        if field_name in self.INSTANT_FACT_FIELDS:
            return self._collect_own_data_instant(us_gaap, xbrl_keys, accn_reportdate, fiscal_end_month, field_name,
                                                    collisions_out, anchor_month, anchor_day, extra_anchors)
        return self._collect_own_data_annual(us_gaap, xbrl_keys, accn_reportdate, fiscal_end_month, field_name,
                                              collisions_out, anchor_month, anchor_day, extra_anchors)

    # FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: 真の境界衝突（決算期変更）とみなす
    # 最小の(月,日)循環距離。common/sec_data/fye_change_candidate_scan.py の
    # MIN_CLUSTER_DISTANCE_DAYSと同じ値を使い、判定基準を一致させる。
    _BOUNDARY_COLLISION_MIN_ANCHOR_DISTANCE_DAYS = 30

    @staticmethod
    def _fiscal_anchors_far_apart(end_date_a: str, end_date_b: str) -> bool:
        """
        2つのend_dateの(月,日)のみを比較し、暦年をまたぐ循環距離が
        _BOUNDARY_COLLISION_MIN_ANCHOR_DISTANCE_DAYSを超えるか判定する。

        「生fyタグが異なる・end_dateも異なる」だけでは不十分で、ADSK/AVAV/CRM/
        CAKE等の固定決算日企業では「同一の(月,日)・隣接する暦年」の組み合わせが
        頻出する（filer側のfyタグが実際の期間より1年ずれるWARN-23既知パターン。
        例: end=2011-01-31/fy=2010とend=2012-01-31/fy=2011が同一年度バケツで
        競合するが、これは決算期変更ではなく単なるfyタグの年ズレ）。
        本関数は(月,日)のみを見て「そもそも決算日そのものが動いたか」を判定し、
        同一の(月,日)（52/53週の前後変動込み）は除外する。
        """
        try:
            dt_a = datetime.strptime(end_date_a, "%Y-%m-%d")
            dt_b = datetime.strptime(end_date_b, "%Y-%m-%d")
        except ValueError:
            return False
        doy_a = _day_of_year(dt_a.month, dt_a.day)
        doy_b = _day_of_year(dt_b.month, dt_b.day)
        diff = abs(doy_a - doy_b)
        circular_dist = min(diff, 366 - diff)
        return circular_dist > SECParser._BOUNDARY_COLLISION_MIN_ANCHOR_DISTANCE_DAYS

    @classmethod
    def _is_boundary_collision(cls, existing_end: Optional[str], existing_fy_tag: Optional[int],
                                own_end: str, own_fy_tag: Optional[int]) -> bool:
        """
        FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: 既存（フォールバック採用済み）側と
        本人データ候補側が「真の境界衝突」（決算期変更、RCAT型）とみなせるかを
        判定する純関数。呼び出し前後で退避した既存側の情報と組み合わせて使う
        （_extract_values_merged/_extract_values_best_candidate内の2箇所から
        呼び出される）。同じexisting_fy_tag/own_fy_tagは
        [[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]]対応で
        `_own_override_is_safe()`（_extract_values_best_candidate側の
        呼び出しのみ、既存のexisting_fy_tag/own_fy_tagがNoneのデフォルトでは
        従来通り無効化）にも渡すようになった。

        判定条件（すべて満たす場合のみTrue）:
        1. 既存側end_dateが存在する（フォールバックが実際に何かを採用済み）
        2. 既存側fyタグが存在し、本人データ側fyタグと異なる
        3. 既存側end_dateと本人データ側end_dateが異なる
        4. 2つのend_dateの(月,日)が_fiscal_anchors_far_apart()で「十分離れている」
           （ADSK/AVAV/CRM/CAKE等の「同一(月,日)・隣接暦年」パターン＝fyタグの
           年ズレ〈WARN-23既知〉を除外するため）
        """
        if existing_end is None or existing_fy_tag is None:
            return False
        if existing_fy_tag == own_fy_tag:
            return False
        if existing_end == own_end:
            return False
        return cls._fiscal_anchors_far_apart(existing_end, own_end)

    def _own_override_is_safe(self, year: int, own_end_date: str, fiscal_end_month: int,
                               annual_end_dates: Dict[int, str], annual_durations: Dict[int, Any],
                               annual_accn: Dict[int, str], accn_reportdate: Dict[str, str],
                               anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                               is_instant: bool = False,
                               extra_anchors: Optional[List[tuple]] = None,
                               existing_fy_tag: Optional[int] = None,
                               own_fy_tag: Optional[int] = None) -> bool:
        """
        本人データによる上書きが安全か判定する。

        フォールバック（determine_fiscal_year()ベースのtie-break）が既にこの年度キーに
        「別の真の年次期間として自己無矛盾な、正規の年次データ（340-380日）」を
        採用済みの場合は上書きしない。

        WMTの実例で発覚: WMTの真のFY2010データ（$408,214M、end=2010-01-31）が、
        WMT自身のfiling原本内でfy=2009と誤りタグ付けされている（比較年度再掲ではなく
        原本自体の誤り）。この1件だけを見るとDELLのFY2019パターン（本人データの
        fyタグがdetermine_fiscal_year()の計算結果と食い違う正当なケース）と区別が
        つかないが、上書き先のfy=2009キーには既に自己無矛盾な真のFY2009年次データ
        （$404,374M、end=2009-01-31、reportDateとの一致はないがdetermine_fiscal_year()
        computed_year==2009と自己整合）が存在しており、無条件上書きするとこの正しい
        データを破壊してしまう。DELLの場合、上書き前のfallback値は90日間スタブ
        （determine_fiscal_year()の計算結果がキーと一致しない）であり本チェックには
        引っかからず、正しく上書きされる。

        ARCH-DATA-1ステージ2（設計変更）: 旧実装は「月またぎ補正
        (end_date.month > fiscal_end_month による+1)なしで自己整合する場合のみ
        安全とみなす」事前フィルタ（no_crossing_needed = existing_end_dt.month
        <= fiscal_end_month）を持っていたが、これはdetermine_fiscal_year()本体
        と同じ「月のみ比較」欠陥を共有しており、12月決算企業では
        month<=12が恒常的にTrueとなり事前フィルタが機能しない欠陥があった
        （CDNS FY2015のtotal_assets/revenueがFY2014値のまま誤って保持され続ける
        実害を確認済み）。determine_fiscal_year()自体がアンカー日ウィンドウ方式
        に置き換わり、月境界をまたぐか否かに関わらず「end_dateに最も近いアンカー日
        候補年度」を正しく計算できるようになったため、事前フィルタ自体を廃止し、
        統一版determine_fiscal_year()の計算結果とyearの一致判定のみで安全性を
        判定する（IOT型の52/53週誤配置とWMT型の自己整合データの区別は
        アンカー日ウィンドウ方式が両方とも正しく解決するため、月またぎの有無で
        場合分けする必要がなくなった）。

        ARCH-DATA-1残課題④（instant fact対応、VZ型の発覚）: 「existing_end ==
        own_end_date → 同一期間なので上書き安全」という最初のショートカットは
        duration factでは「同一期間を指す2つの候補タグは同じ概念の別名表記
        （WMT Revenues/SalesRevenueNet等）である可能性が高い」という前提の下で
        成立するが、instant fact（BS項目）ではこの前提が成立しない。BS項目は
        同一会計年度内であれば異なるタグ（例: ShortTermBorrowings＝短期借入金と
        LongTermDebtCurrent＝長期債務の流動化部分）でもend_dateが機械的に一致
        するため、このショートカットが「別概念の値」を安全と誤判定してしまう
        （VZの実例: xbrl_keys優先順位1位のShortTermBorrowings本人データ$441M
        が、freshness選定で勝っていたLongTermDebtCurrent本人データ$18,618M
        〈同一end_date〉を誤って上書きし、Net Debtが約$18.6B過小評価される
        回帰を検出）。instant fact向け呼び出し（is_instant=True）ではこの
        ショートカットをスキップし、後続のaccnベースの判定（既に別の本人データ
        が採用済みか）のみで安全性を判定する。
        """
        existing_end = annual_end_dates.get(year)
        if existing_end is None:
            return True  # フォールバックに何もない → 上書きして問題ない
        if not is_instant and existing_end == own_end_date:
            return True  # 同一期間 → 実質的に同じ値のはず（duration factのみ。上記docstring参照）

        existing_accn = annual_accn.get(year)
        if existing_accn is not None and accn_reportdate.get(existing_accn) == existing_end:
            # [[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]] CRM型対応:
            # 既存エントリが自accnの本人データ（reportDateとend_dateが一致）で
            # あっても、その生fyタグ〈existing_fy_tag〉がこのyearバケツ自体と
            # 食い違っている場合（WARN-23と同種の「fyタグ裏取り不一致」）は、
            # 本バケツにとっての正当な本人データとは限らない。CRM(2011)実例:
            # end=2011-01-31（fy=2010タグ）がdetermine_fiscal_year()で偶然
            # year=2011バケツに計算され、本来year=2011の本人データである別accn
            # ・別タグの候補（own_fy_tag==2011で一致）を誤ってブロックしていた。
            # own_fy_tag側がyearと一致する〈真にこのバケツ向けの本人データ〉場合
            # に限り、この短絡判定をスキップして通常の安全確認へフォールスルー
            # する（existing_fy_tag/own_fy_tag省略時は従来通りFalseを返す。
            # WMT型〈existing_fy_tag==yearで自己整合〉はこの分岐に入らず、
            # 従来通りFalseで保護され続ける）。
            if not (existing_fy_tag is not None and existing_fy_tag != year
                    and own_fy_tag is not None and own_fy_tag == year):
                return False  # 既に別の本人データが採用済み → 上書きしない（タグ優先順位を尊重）

        existing_days = annual_durations.get(year)
        if existing_days is not None and 340 <= existing_days <= 380:
            try:
                existing_end_dt = datetime.strptime(existing_end, '%Y-%m-%d')
            except ValueError:
                return True
            if determine_fiscal_year(existing_end_dt, fiscal_end_month, anchor_month, anchor_day,
                                      extra_anchors) == year:
                return False  # 既存エントリは別の真の年次データとして自己無矛盾に存在する

        return True

    def _extract_values_merged(self, us_gaap: dict, xbrl_keys: List[str], use_max: bool, fiscal_end_month: int,
                                accn_reportdate: Optional[Dict[str, str]] = None, field_name: str = "",
                                collisions_out: Optional[List[Dict[str, Any]]] = None,
                                anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                                extra_anchors: Optional[List[tuple]] = None,
                                fy_mismatches_out: Optional[List[Dict[str, Any]]] = None,
                                boundary_collisions_out: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """use_max=True または merge_all_tags=True の場合の抽出（全キーを検索して統合）"""
        result = {"annual": {}, "quarterly": {}}
        # 期末日を記録（同一end_yearで最新のend日付を優先するため）
        annual_end_dates = {}
        quarterly_end_dates = {}
        # SECDATA-STORAGE-FRAGMENTATION-1 normalized/→data/統合: 四半期の生候補を
        # 即座に確定せず収集し、全キー処理後に_resolve_quarterly_values()で
        # まとめて解決する（SA優先＋YTD差分計算フォールバックの統一アルゴリズム）
        quarterly_candidates: List[tuple] = []
        # fy==end_yearの完全一致フラグ: 非December FY企業でQ1等中間期エントリが
        # 同一end_yearを持ち全年データを上書きするのを防ぐ（INTU等の対策）
        annual_exact_match = {}
        # 期間日数（end-start）を記録。同一end_date・同一exact_matchレベルで
        # 複数タグが競合した場合に365日（正規の年次期間）に近い方を優先するために使う
        # （SEC-TAG-FICO-CPRT-1: 91日間の四半期比較開示がform='10-K'・fp='FY'で
        #  年次候補に混入し、XBRL_MAPPINGの列挙順（先に処理されたタグ）が
        #  実質的に勝ってしまう早い者勝ちバグへの対応。FICO/CPRT/LITEで確認）
        annual_durations = {}
        # 採用accnを記録（本人データ上書き判定用。FY52WEEK-BUCKET-MISPLACE-1根本修正）
        annual_accn: Dict[int, str] = {}
        # 採用filed日・formを記録（ARCH-DATA-1ステージ1: 真に同一期間〈end_date一致〉の
        # 複数バージョンが競合し、かつ一方が10-K/A〈訂正申告〉である場合のみ、
        # filed日が新しい方を優先するタイブレークに使う）
        annual_filed: Dict[int, str] = {}
        annual_form: Dict[int, str] = {}
        # 採用エントリの生XBRL fyタグを記録（ARCH-DATA-1ステージ3: fyタグ裏取り用。
        # end_yearキー〈determine_fiscal_year()の計算結果〉と一致しない場合がある）
        annual_fy_tag: Dict[int, int] = {}

        for key in xbrl_keys:
            if key not in us_gaap:
                continue

            units = us_gaap[key].get("units", {})

            # USD or shares
            for unit_type in ["USD", "shares", "USD/shares"]:
                if unit_type not in units:
                    continue

                for entry in units[unit_type]:
                    form = entry.get("form", "")
                    fy = entry.get("fy")
                    fp = entry.get("fp", "")
                    val = entry.get("val")
                    end_date = entry.get("end", "")
                    accn = entry.get("accn")
                    filed = entry.get("filed", "")

                    if val is None or fy is None:
                        continue

                    # 年次（10-K・10-K/A）- determine_fiscal_year で会計年度キーを統一定義に従って決定
                    # 10-K/A（訂正申告）も候補に含める（ARCH-DATA-1ステージ1）
                    if form in ("10-K", "10-K/A") and fp == "FY":
                        # determine_fiscal_year(期末日, fiscal_end_month) が単一定義
                        # fy==end_yearはFY通年データとして信頼度が高い（exact match）
                        # fy!=end_yearは比較年度エントリ（FCX等）または中間期エントリ（INTU Q1等）
                        if end_date and len(end_date) >= 10:
                            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                            end_year = determine_fiscal_year(end_dt, fiscal_end_month, anchor_month, anchor_day,
                                                              extra_anchors)
                        else:
                            end_year = fy  # end_date 不明時は SEC の fy フィールドを使用
                        exact = (fy == end_year)
                        start_date = entry.get("start", "")
                        days = None
                        if start_date and end_date and len(start_date) >= 10 and len(end_date) >= 10:
                            try:
                                days = (end_dt - datetime.strptime(start_date, '%Y-%m-%d')).days
                            except ValueError:
                                days = None
                        if use_max:
                            # 最大値を採用（株式数の異常値対策）
                            if end_year not in result["annual"] or val > result["annual"][end_year]:
                                result["annual"][end_year] = val
                                annual_end_dates[end_year] = end_date
                                annual_accn[end_year] = accn
                                annual_filed[end_year] = filed
                                annual_form[end_year] = form
                                annual_fy_tag[end_year] = fy
                        else:
                            # [[PERIOD-LENGTH-VALIDATION-GAP-1]]対応: 従来は
                            # 同一end_date・同一exact_matchレベルで複数候補が
                            # 競合した場合のみ期間長でtie-breakしていた
                            # （SEC-TAG-FICO-CPRT-1）。候補が単一の年度では
                            # このtie-break自体が発動せず、91日程度の四半期
                            # エントリがform='10-K'・fp='FY'で年次候補に混入した
                            # まま無条件で採用されてしまう構造だったため、
                            # 候補プールへの受理時点で340-380日を必須条件とし、
                            # 単一候補の場合も含めて無条件に適用する。
                            if days is not None and not (340 <= days <= 380):
                                continue
                            if end_year not in result["annual"]:
                                result["annual"][end_year] = val
                                annual_end_dates[end_year] = end_date
                                annual_exact_match[end_year] = exact
                                annual_durations[end_year] = days
                                annual_accn[end_year] = accn
                                annual_filed[end_year] = filed
                                annual_form[end_year] = form
                                annual_fy_tag[end_year] = fy
                            elif exact and not annual_exact_match.get(end_year, True):
                                # exact matchで上書き: 非December FY企業のQ1等中間期エントリ
                                # (fy=N+1, end_year=N) が全年データ(fy=N, end_year=N)を上書きするのを防ぐ
                                result["annual"][end_year] = val
                                annual_end_dates[end_year] = end_date
                                annual_exact_match[end_year] = True
                                annual_durations[end_year] = days
                                annual_accn[end_year] = accn
                                annual_filed[end_year] = filed
                                annual_form[end_year] = form
                                annual_fy_tag[end_year] = fy
                            elif exact == annual_exact_match.get(end_year, False):
                                stored_end = annual_end_dates.get(end_year, "")
                                if end_date > stored_end:
                                    # 同じexact_matchレベル: 最新のend_dateを優先
                                    result["annual"][end_year] = val
                                    annual_end_dates[end_year] = end_date
                                    annual_durations[end_year] = days
                                    annual_accn[end_year] = accn
                                    annual_filed[end_year] = filed
                                    annual_form[end_year] = form
                                    annual_fy_tag[end_year] = fy
                                elif end_date == stored_end and (form == "10-K/A" or annual_form.get(end_year) == "10-K/A"):
                                    # ARCH-DATA-1ステージ1: end_dateも同一＝真に同一期間の
                                    # 複数バージョンが競合。一方が10-K/A（訂正申告）の場合に
                                    # 限り、filed日が新しい方を優先する（「値の確定」の主軸）。
                                    # 10-K/Aが関与しない場合（通常の比較年度再掲同士の食い違い。
                                    # discontinued operations区分変更等で数字の意味自体が
                                    # 変わりうるため）は下記のSEC-TAG-FICO-CPRT-1タイブレーク
                                    # （期間日数365日近似）のみで判定する従来動作を変更しない。
                                    stored_filed = annual_filed.get(end_year, "")
                                    if filed and filed > stored_filed:
                                        result["annual"][end_year] = val
                                        annual_durations[end_year] = days
                                        annual_accn[end_year] = accn
                                        annual_filed[end_year] = filed
                                        annual_form[end_year] = form
                                        annual_fy_tag[end_year] = fy
                                    elif filed == stored_filed and days is not None:
                                        stored_days = annual_durations.get(end_year)
                                        if stored_days is None or abs(days - 365) < abs(stored_days - 365):
                                            result["annual"][end_year] = val
                                            annual_durations[end_year] = days
                                            annual_accn[end_year] = accn
                                            annual_fy_tag[end_year] = fy
                                elif end_date == stored_end and days is not None:
                                    # SEC-TAG-FICO-CPRT-1: end_dateも同一（10-K/A非関与）の
                                    # 場合、期間日数が365日（正規の年次期間）に近い方を優先する
                                    stored_days = annual_durations.get(end_year)
                                    if stored_days is None or abs(days - 365) < abs(stored_days - 365):
                                        result["annual"][end_year] = val
                                        annual_durations[end_year] = days
                                        annual_accn[end_year] = accn
                                        annual_fy_tag[end_year] = fy

                    # 四半期（10-Q）
                    elif form == "10-Q" and fp in ["Q1", "Q2", "Q3"]:
                        if use_max:
                            # use_maxは現在全フィールドでFalse固定のため実質到達しない
                            # （株式数の異常値対策を優先順位方式へ変更した経緯により
                            # 事実上デッドパスだが、既存シグネチャ互換のため維持する）
                            quarter_key = f"{fy}{fp}"
                            if quarter_key not in result["quarterly"] or val > result["quarterly"][quarter_key]:
                                result["quarterly"][quarter_key] = val
                                quarterly_end_dates[quarter_key] = end_date
                        else:
                            # SECDATA-STORAGE-FRAGMENTATION-1 normalized/→data/統合:
                            # 即座に確定せず候補として収集し、全キー処理後に
                            # _resolve_quarterly_values()でまとめて解決する
                            quarterly_candidates.append((fy, fp, entry.get("start", ""), end_date, val))

                # 最初に見つかったunit_typeのデータを使用
                if result["annual"] or result["quarterly"] or quarterly_candidates:
                    break

            # 全キーを検索（早期終了しない。merge_all_tagsは年代ごとのタグ切替を横断統合するため）

        # 出所メタデータのサイドカー（ARCH-DATA-1ステージ1: 「値の確定」）。
        # フォールバック採用分をまず記録し、本人データ上書きが適用された年度は
        # 後段で上書きする。annual_provenance は _build_period_data が
        # {field}_provenance として消費し、既存の bs/pl/cf 等のスキーマは変更しない。
        annual_provenance: Dict[int, Dict[str, Any]] = {}
        for year, accn in annual_accn.items():
            if accn is None:
                continue
            annual_provenance[year] = {
                "accn": accn,
                "filed": annual_filed.get(year, ""),
                "is_own_data": accn_reportdate.get(accn) == annual_end_dates.get(year) if accn_reportdate else False,
                "fy_tag": annual_fy_tag.get(year),
            }

        # FY52WEEK-BUCKET-MISPLACE-1根本修正: フォールバック(上記のdetermine_fiscal_year()
        # ベースのtie-break)が「本人データではない」候補を採用してしまった年度キーのみを
        # 本人データで上書きする。フォールバックが既に本人データを採用済みの年度キーは
        # 一切変更しない（WMTのRevenues/SalesRevenueNet併記のような、複数タグが同時に
        # 本人データとして存在するケースで、xbrl_keysの優先順位に基づく既存のtie-break
        # 判断を上書きの副作用で覆してしまう回帰を防ぐため）。
        if accn_reportdate:
            own_data = self._collect_own_data(us_gaap, xbrl_keys, accn_reportdate, fiscal_end_month, field_name,
                                               collisions_out, anchor_month, anchor_day, extra_anchors)
            for year, (val, own_end, own_accn, own_filed, own_fy_tag) in own_data.items():
                # FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: 上書き判定の前に既存
                # （フォールバック採用済み）側のend_date/fyタグを退避しておく。
                # _own_override_is_safe自体はannual_end_dates[year]を書き換えない
                # ため、判定結果に関わらずここで取得した値が「上書き前の既存側」を表す。
                _existing_end = annual_end_dates.get(year)
                _existing_fy_tag = annual_fy_tag.get(year)
                _existing_accn = annual_accn.get(year)
                _override_ok = self._own_override_is_safe(year, own_end, fiscal_end_month, annual_end_dates,
                                                            annual_durations, annual_accn, accn_reportdate,
                                                            anchor_month, anchor_day,
                                                            is_instant=field_name in self.INSTANT_FACT_FIELDS,
                                                            extra_anchors=extra_anchors)
                if (boundary_collisions_out is not None
                        and self._is_boundary_collision(_existing_end, _existing_fy_tag, own_end, own_fy_tag)):
                    boundary_collisions_out.append({
                        "field": field_name, "year": year,
                        "own_data_side": {"fy_tag": own_fy_tag, "accn": own_accn, "end_date": own_end},
                        "other_side": {
                            "fy_tag": _existing_fy_tag, "accn": _existing_accn, "end_date": _existing_end,
                            "is_own_data": bool(_existing_accn and accn_reportdate.get(_existing_accn) == _existing_end),
                        },
                        "override_applied": _override_ok,
                    })
                if _override_ok:
                    result["annual"][year] = val
                    annual_end_dates[year] = own_end
                    annual_provenance[year] = {
                        "accn": own_accn, "filed": own_filed, "is_own_data": True,
                        "fy_tag": own_fy_tag,
                    }

        # ARCH-DATA-1ステージ3: fyタグ裏取り。年度バケツキー（determine_fiscal_year()の
        # 計算結果）と採用エントリの生fyタグが食い違う場合を記録する（呼び出し元
        # _parse_raw_data が全フィールド横断で集約し fy_tag_mismatch_log.json に保存）。
        # is_own_data=False（比較年度再掲エントリ等）は対象外とする: fyタグは
        #「その数値がどの10-Kに載っていたか」というfiling側の属性であり、比較年度
        # 再掲エントリでは「載っていた10-Kの年」と「数値が表す期間」が一致しないのが
        # 正常仕様（企業のfyタグ誤りとは無関係）。全105銘柄検証で4,434件・105銘柄
        # というノイズになることが判明したため、is_own_data=True（本人データ自身の
        # fyタグが実際に採用されてしまっているケース）のみを対象にする
        # （2026-07-17設計変更）。
        if fy_mismatches_out is not None:
            for year, prov in annual_provenance.items():
                if not prov.get("is_own_data"):
                    continue
                fy_tag = prov.get("fy_tag")
                if fy_tag is not None and fy_tag != year:
                    fy_mismatches_out.append({
                        "field": field_name,
                        "end_date": annual_end_dates.get(year, ""),
                        "fy_tag": fy_tag,
                        "computed_year": year,
                    })

        result["_annual_provenance"] = annual_provenance
        # SECDATA-STORAGE-FRAGMENTATION-1 normalized/→data/統合: 全キー分の四半期
        # 生候補が出揃った時点でSA優先＋YTD差分計算フォールバックの統一
        # アルゴリズムを適用する（use_max分岐はresult["quarterly"]へ既に確定済みの
        # ため、quarterly_candidates収集分のみをここで解決してmergeする）
        if quarterly_candidates:
            result["quarterly"].update(_resolve_quarterly_values(quarterly_candidates, field_name))
        return result

    def _extract_values_best_candidate(self, us_gaap: dict, xbrl_keys: List[str], fiscal_end_month: int,
                                        accn_reportdate: Optional[Dict[str, str]] = None, field_name: str = "",
                                        collisions_out: Optional[List[Dict[str, Any]]] = None,
                                        anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                                        extra_anchors: Optional[List[tuple]] = None,
                                        fy_mismatches_out: Optional[List[Dict[str, Any]]] = None,
                                        boundary_collisions_out: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """use_max/merge_all_tagsいずれもFalseの場合の抽出（1つの候補タグを選んで採用する）

        LLY-CAPEX-STALE-1 Phase 2a: 候補キーを優先順位順に「最新期末年が
        取れた最初のキーで打ち切る」方式は、真に最新のデータを持つタグが
        候補リストに含まれていない場合に検知できない（LLYのcapital_expenditure:
        旧候補3タグはいずれも年次(10-K FY)エントリを一度も持たず常に空扱い
        だった）。候補キーをすべて独立に抽出し、annualデータを持つ候補の
        中から最新end_yearが最も新しいものを採用する（quarterly.pyの
        _select_best_candidate と同じ考え方）。annualデータを持つ候補が
        皆無の場合のみ、quarterlyの最新度で採用する。
        """
        candidates: list[tuple[str, Dict[str, Any]]] = []
        for key in xbrl_keys:
            if key not in us_gaap:
                continue
            key_result = self._extract_single_key(us_gaap, key, fiscal_end_month, anchor_month, anchor_day,
                                                   extra_anchors=extra_anchors, field_name=field_name)
            if not key_result["annual"] and not key_result["quarterly"]:
                continue
            candidates.append((key, key_result))

        if not candidates:
            return {"annual": {}, "quarterly": {}}

        def _freshness(idx: int) -> tuple[int, str, int]:
            _, key_result = candidates[idx]
            latest_annual = max(key_result["annual"].keys(), default=0)
            latest_quarter = max(key_result["quarterly"].keys(), default="")
            return latest_annual, latest_quarter, -idx

        qualified = [i for i in range(len(candidates)) if candidates[i][1]["annual"]]
        pool = qualified if qualified else list(range(len(candidates)))
        best_idx = max(pool, key=_freshness)
        result = candidates[best_idx][1]
        winning_end_dates = result.pop("_annual_end_dates", {})
        winning_accns = result.pop("_annual_accn", {})
        winning_durations = result.pop("_annual_durations", {})
        winning_filed = result.pop("_annual_filed", {})
        winning_fy_tags = result.pop("_annual_fy_tag", {})
        for _, key_result in candidates:
            key_result.pop("_annual_end_dates", None)
            key_result.pop("_annual_accn", None)
            key_result.pop("_annual_durations", None)
            key_result.pop("_annual_filed", None)
            key_result.pop("_annual_fy_tag", None)

        # 出所メタデータのサイドカー（ARCH-DATA-1ステージ1: 「値の確定」）。
        # 勝者タグのフォールバック採用分をまず記録し、本人データ上書きが
        # 適用された年度は後段で上書きする。
        annual_provenance: Dict[int, Dict[str, Any]] = {}
        for year, accn in winning_accns.items():
            if accn is None:
                continue
            annual_provenance[year] = {
                "accn": accn,
                "filed": winning_filed.get(year, ""),
                "is_own_data": accn_reportdate.get(accn) == winning_end_dates.get(year) if accn_reportdate else False,
                "fy_tag": winning_fy_tags.get(year),
            }

        # FY52WEEK-BUCKET-MISPLACE-1根本修正: フォールバックが「本人データではない」
        # 候補を採用してしまった年度キーのみを、本人データで上書きする
        # （選ばれたタグ単体に本人データが無い年度でも他の候補タグには存在する場合が
        # あるため、xbrl_keysの優先順位順に全タグを横断して本人データを収集する。
        # JNJのnet_income: NetIncomeLossタグに2011年の本人365日データが無く
        # ProfitLossタグには存在する、という実例で確認済み）。
        # フォールバックが既にそのタグ自身の本人データを採用済みの年度キー、または
        # 別の真の年次データとして自己無矛盾に存在する年度キーは変更しない
        # （WMT型の回帰防止。_own_override_is_safe参照）
        if accn_reportdate:
            combined_own_data: Dict[int, Any] = {}
            for key in xbrl_keys:
                if key not in us_gaap:
                    continue
                key_own_data = self._collect_own_data(us_gaap, [key], accn_reportdate, fiscal_end_month, field_name,
                                                       collisions_out, anchor_month, anchor_day, extra_anchors)
                for fy, (val, end_date, own_accn, own_filed, own_fy_tag) in key_own_data.items():
                    if fy not in combined_own_data:
                        combined_own_data[fy] = (val, end_date, own_accn, own_filed, own_fy_tag)
            for year, (val, own_end, own_accn, own_filed, own_fy_tag) in combined_own_data.items():
                # FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: _extract_values_mergedと同一パターン
                _existing_end = winning_end_dates.get(year)
                _existing_fy_tag = winning_fy_tags.get(year)
                _existing_accn = winning_accns.get(year)
                _override_ok = self._own_override_is_safe(year, own_end, fiscal_end_month, winning_end_dates,
                                                            winning_durations, winning_accns, accn_reportdate,
                                                            anchor_month, anchor_day,
                                                            is_instant=field_name in self.INSTANT_FACT_FIELDS,
                                                            extra_anchors=extra_anchors,
                                                            existing_fy_tag=_existing_fy_tag,
                                                            own_fy_tag=own_fy_tag)
                if (boundary_collisions_out is not None
                        and self._is_boundary_collision(_existing_end, _existing_fy_tag, own_end, own_fy_tag)):
                    boundary_collisions_out.append({
                        "field": field_name, "year": year,
                        "own_data_side": {"fy_tag": own_fy_tag, "accn": own_accn, "end_date": own_end},
                        "other_side": {
                            "fy_tag": _existing_fy_tag, "accn": _existing_accn, "end_date": _existing_end,
                            "is_own_data": bool(_existing_accn and accn_reportdate.get(_existing_accn) == _existing_end),
                        },
                        "override_applied": _override_ok,
                    })
                if _override_ok:
                    result["annual"][year] = val
                    winning_end_dates[year] = own_end
                    annual_provenance[year] = {
                        "accn": own_accn, "filed": own_filed, "is_own_data": True,
                        "fy_tag": own_fy_tag,
                    }

        # ARCH-DATA-1ステージ3: fyタグ裏取り（_extract_values_mergedと同じロジック。
        # is_own_data=Falseの比較年度再掲エントリは対象外。詳細は_extract_values_merged
        # 側のコメント参照）
        if fy_mismatches_out is not None:
            for year, prov in annual_provenance.items():
                if not prov.get("is_own_data"):
                    continue
                fy_tag = prov.get("fy_tag")
                if fy_tag is not None and fy_tag != year:
                    fy_mismatches_out.append({
                        "field": field_name,
                        "end_date": winning_end_dates.get(year, ""),
                        "fy_tag": fy_tag,
                        "computed_year": year,
                    })

        result["_annual_provenance"] = annual_provenance
        return result

    def _extract_single_key(self, us_gaap: dict, key: str, fiscal_end_month: int,
                             anchor_month: Optional[int] = None, anchor_day: Optional[int] = None,
                             extra_anchors: Optional[List[tuple]] = None,
                             field_name: str = "") -> Dict[str, Any]:
        """1つのXBRLキーからannual/quarterly値を抽出する（候補選定の評価単位）

        field_nameがPERIOD_LENGTH_VALIDATED_FIELDSに含まれる場合、年次候補として
        受理する際に期間長(340-380日)を必須条件とする（[[PERIOD-LENGTH-VALIDATION-
        GAP-1]]対応）。_collect_own_data_annual()の同種フィルタ（617行目）と
        同一パターン。instant fact（start_dateを持たずdays=Noneになる）は
        このフィールド集合の対象外（INSTANT_FACT_FIELDS）のため影響しない。
        """
        result: Dict[str, Any] = {"annual": {}, "quarterly": {}}
        annual_end_dates: Dict[int, str] = {}
        quarterly_end_dates: Dict[str, str] = {}
        # SECDATA-STORAGE-FRAGMENTATION-1 normalized/→data/統合: 四半期の生候補を
        # 即座に確定せず収集し、_resolve_quarterly_values()でまとめて解決する
        # （SA優先＋YTD差分計算フォールバックの統一アルゴリズム）
        quarterly_candidates: List[tuple] = []
        annual_exact_match: Dict[int, bool] = {}
        annual_accn: Dict[int, str] = {}
        annual_durations: Dict[int, Any] = {}
        # 採用filed日・formを記録（ARCH-DATA-1ステージ1: 真に同一期間の複数バージョンが
        # 競合し、かつ一方が10-K/A〈訂正申告〉である場合のみ、filed日が新しい方を
        # 優先するタイブレークに使う）
        annual_filed: Dict[int, str] = {}
        annual_form: Dict[int, str] = {}
        # 採用エントリの生XBRL fyタグを記録（ARCH-DATA-1ステージ3: fyタグ裏取り用）
        annual_fy_tag: Dict[int, int] = {}

        units = us_gaap.get(key, {}).get("units", {})
        for unit_type in ["USD", "shares", "USD/shares"]:
            if unit_type not in units:
                continue

            for entry in units[unit_type]:
                form = entry.get("form", "")
                fy = entry.get("fy")
                fp = entry.get("fp", "")
                val = entry.get("val")
                end_date = entry.get("end", "")
                accn = entry.get("accn")
                filed = entry.get("filed", "")

                if val is None or fy is None:
                    continue

                # 10-K/A（訂正申告）も候補に含める（ARCH-DATA-1ステージ1）
                if form in ("10-K", "10-K/A") and fp == "FY":
                    if end_date and len(end_date) >= 10:
                        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                        end_year = determine_fiscal_year(end_dt, fiscal_end_month, anchor_month, anchor_day,
                                                          extra_anchors)
                    else:
                        end_year = fy
                    exact = (fy == end_year)
                    start_date = entry.get("start", "")
                    days = None
                    if start_date and end_date and len(start_date) >= 10 and len(end_date) >= 10:
                        try:
                            days = (end_dt - datetime.strptime(start_date, '%Y-%m-%d')).days
                        except ValueError:
                            days = None

                    # [[PERIOD-LENGTH-VALIDATION-GAP-1]]対応: PERIOD_LENGTH_
                    # VALIDATED_FIELDS対象フィールドでは、年次候補として受理する
                    # 前に期間長(340-380日)を必須条件とする。instant fact
                    # （days=None、INSTANT_FACT_FIELDSはそもそも対象外）は
                    # 対象外のため影響しない。
                    if (field_name in self.PERIOD_LENGTH_VALIDATED_FIELDS
                            and days is not None and not (340 <= days <= 380)):
                        continue

                    if end_year not in result["annual"]:
                        result["annual"][end_year] = val
                        annual_end_dates[end_year] = end_date
                        annual_exact_match[end_year] = exact
                        annual_accn[end_year] = accn
                        annual_durations[end_year] = days
                        annual_filed[end_year] = filed
                        annual_form[end_year] = form
                        annual_fy_tag[end_year] = fy
                    elif exact and not annual_exact_match.get(end_year, True):
                        result["annual"][end_year] = val
                        annual_end_dates[end_year] = end_date
                        annual_exact_match[end_year] = True
                        annual_accn[end_year] = accn
                        annual_durations[end_year] = days
                        annual_filed[end_year] = filed
                        annual_form[end_year] = form
                        annual_fy_tag[end_year] = fy
                    elif exact == annual_exact_match.get(end_year, False):
                        stored_end = annual_end_dates.get(end_year, "")
                        if end_date > stored_end:
                            result["annual"][end_year] = val
                            annual_end_dates[end_year] = end_date
                            annual_accn[end_year] = accn
                            annual_durations[end_year] = days
                            annual_filed[end_year] = filed
                            annual_form[end_year] = form
                            annual_fy_tag[end_year] = fy
                        elif end_date == stored_end and (form == "10-K/A" or annual_form.get(end_year) == "10-K/A"):
                            # ARCH-DATA-1ステージ1: end_dateも同一＝真に同一期間の
                            # 複数バージョンが競合。一方が10-K/A（訂正申告）の場合に限り、
                            # filed日が新しい方を優先する（「値の確定」の主軸）。
                            # 10-K/Aが関与しない場合（通常の比較年度再掲同士の食い違い。
                            # discontinued operations区分変更等で数字の意味自体が変わり
                            # うるため）は従来通り先勝ちのまま変更しない。
                            stored_filed = annual_filed.get(end_year, "")
                            if filed and filed > stored_filed:
                                result["annual"][end_year] = val
                                annual_accn[end_year] = accn
                                annual_durations[end_year] = days
                                annual_filed[end_year] = filed
                                annual_form[end_year] = form
                                annual_fy_tag[end_year] = fy
                            elif filed == stored_filed and days is not None:
                                stored_days = annual_durations.get(end_year)
                                if stored_days is None or abs(days - 365) < abs(stored_days - 365):
                                    result["annual"][end_year] = val
                                    annual_accn[end_year] = accn
                                    annual_fy_tag[end_year] = fy
                                    annual_durations[end_year] = days

                elif form == "10-Q" and fp in ["Q1", "Q2", "Q3"]:
                    # SECDATA-STORAGE-FRAGMENTATION-1 normalized/→data/統合:
                    # 即座に確定せず候補として収集し、ループ後に
                    # _resolve_quarterly_values()でまとめて解決する
                    quarterly_candidates.append((fy, fp, entry.get("start", ""), end_date, val))

            if result["annual"] or quarterly_candidates:
                break

        # SECDATA-STORAGE-FRAGMENTATION-1 normalized/→data/統合: SA優先＋YTD
        # 差分計算フォールバックの統一アルゴリズムで四半期候補を解決する
        if quarterly_candidates:
            result["quarterly"].update(_resolve_quarterly_values(quarterly_candidates, field_name))

        # 本人データの上書きは呼び出し元(_extract_values_best_candidate)で
        # タグ横断・優先順位順に一括適用する（このタグ単体では本人データが
        # 存在しない年度でも、他候補タグには存在する場合があるため。
        # JNJのnet_income: NetIncomeLossタグには2011年の本人365日データが
        # 存在しないが、ProfitLossタグには存在する、という実例で確認済み）。
        # フォールバックが既に本人データを採用済みかどうかの判定用に、
        # 採用end_date/accn/duration/filedを"_"接頭辞のメタ情報として同梱する
        result["_annual_end_dates"] = annual_end_dates
        result["_annual_accn"] = annual_accn
        result["_annual_durations"] = annual_durations
        result["_annual_filed"] = annual_filed
        result["_annual_fy_tag"] = annual_fy_tag
        return result

    def _get_available_years(self, extracted: dict) -> List[int]:
        """利用可能な年度を取得"""
        years = set()
        for field_data in extracted.values():
            years.update(field_data.get("annual", {}).keys())
        return sorted(years, reverse=True)
    
    def _get_available_quarters(self, extracted: dict) -> List[str]:
        """利用可能な四半期を取得"""
        quarters = set()
        for field_data in extracted.values():
            quarters.update(field_data.get("quarterly", {}).keys())
        return sorted(quarters, reverse=True)
    
    def _build_period_data(self, extracted: dict, period: Any, is_annual: bool) -> Optional[Dict[str, Any]]:
        """特定期間のデータを構築"""
        period_type = "annual" if is_annual else "quarterly"

        data = {
            "period": str(period),
            "bs": {},
            "pl": {},
            "cf": {},
            "shares": {},
            "other": {},
        }

        # 出所メタデータのサイドカー（ARCH-DATA-1ステージ1: 「値の確定」）。
        # 既存の bs/pl/cf/shares/other スキーマ（フラットな{フィールド名: 値}）は
        # 変更せず、{セクション}_provenance という追加キーとしてのみ付与する
        # （既存消費者は未知キーを無視するため無改修で動作する）。
        # 年次データのみ対象（quarterly側は_annual_provenanceに該当エントリが
        # 存在しないため自然に空のまま）。
        provenance_sections: Dict[str, Dict[str, Any]] = {
            "bs": {}, "pl": {}, "cf": {}, "shares": {}, "other": {},
        }

        def _record(section: str, field: str, val: Any) -> None:
            if val is None:
                return
            data[section][field] = val
            if not is_annual:
                return
            prov = extracted.get(field, {}).get("_annual_provenance", {}).get(period)
            if prov:
                provenance_sections[section][field] = prov

        # BS
        for field in ["total_assets", "stockholders_equity", "total_liabilities",
                      "cash_and_equivalents", "short_term_investments",
                      "long_term_debt", "short_term_debt",
                      "current_assets", "current_liabilities"]:
            _record("bs", field, extracted.get(field, {}).get(period_type, {}).get(period))

        # PL
        for field in ["revenue", "gross_profit", "cost_of_revenue", "net_income", "eps_diluted", "eps_basic",
                      "research_and_development", "selling_and_marketing",
                      "selling_general_and_administrative", "operating_income"]:
            _record("pl", field, extracted.get(field, {}).get(period_type, {}).get(period))

        # CF
        for field in ["operating_cash_flow", "capital_expenditure", "finance_lease_payments",
                      "depreciation_and_amortization", "stock_based_compensation", "buyback"]:
            _record("cf", field, extracted.get(field, {}).get(period_type, {}).get(period))
        
        # FCF計算（ファイナンスリース除外）
        #
        # FCF = OCF - (|CapEx| - |FinanceLeasePmts|)
        # ファイナンスリースはCapExから除外（AMZN等対応）
        ocf   = data["cf"].get("operating_cash_flow", 0)
        capex = data["cf"].get("capital_expenditure", 0)
        fl    = data["cf"].get("finance_lease_payments", 0)

        # CapEx・ファイナンスリースはSECデータで負値の場合があるためabs()
        abs_capex = abs(capex)
        abs_fl    = abs(fl)
        pure_capex = abs_capex - abs_fl  # リース除外後の純CapEx

        if ocf != 0:
            data["cf"]["free_cash_flow"] = ocf - max(0, pure_capex)
            data["cf"]["fcf_method"] = "traditional"
            data["cf"]["finance_lease_payments_applied"] = abs_fl > 0
        
        # Shares
        for field in ["shares_diluted", "shares_basic"]:
            _record("shares", field, extracted.get(field, {}).get(period_type, {}).get(period))

        # Other (RPO等)
        for field in ["rpo"]:
            _record("other", field, extracted.get(field, {}).get(period_type, {}).get(period))

        # 出所メタデータのサイドカーを空でないセクションのみ付与する
        # （既存スキーマへの無用なサイズ増加・省略時の互換性維持のため）
        for section, prov in provenance_sections.items():
            if prov:
                data[f"{section}_provenance"] = prov

        # SGA整合性チェック:
        # selling_and_marketing が完全に未取得（None）かつ
        # SGA総額が売上比5%超の場合に data_quality 警告を記録する。
        # ※ SGA = Selling + G&A であり G&A は投資強度に含めないため、
        #   (R&D + S&M) vs SGA 差額による比率チェックは常に誤発火する。
        #   S&M が何らかの値で取得できている場合は警告不要。
        sga_total  = data["pl"].get("selling_general_and_administrative")
        sm_missing = data["pl"].get("selling_and_marketing") is None
        revenue    = data["pl"].get("revenue") or 0
        if sm_missing and sga_total and sga_total > 0 and revenue > 0:
            if (sga_total / revenue) > 0.05:
                data["data_quality"] = {
                    "sga_gap_warning": True,
                    "sga_total": sga_total,
                    "sga_captured": 0,
                    "gap_amount": sga_total,
                    "gap_ratio": 1.0,
                    "note": (
                        f"S&M未取得かつSGA総額${sga_total/1e6:.0f}M"
                        f"（売上比{sga_total/revenue*100:.0f}%）。"
                        "企業がSGA内訳を非開示の可能性。投資強度が過小になる場合があります"
                    ),
                }

        # 最低限のデータがあるか確認
        if not any([data["bs"], data["pl"], data["cf"]]):
            return None
        
        return data

    def _save_fy_collision_log(self, ticker: str, collisions: List[Dict[str, Any]]) -> None:
        """
        本人データ同士のfyキー衝突（CRM/FCX/CAKE/HON/COHR/AVAV/FICO/NVDA等で
        実在確認済み。filing代行者側のタグ付け起因と推測されるが原因追及は対象外）を
        report_consistency_check.pyから参照できる形で記録する。

        FY-COLLISION-LOG-NONDETERMINISTIC-1: `_extract_values_best_candidate()`が
        フィールドの候補XBRLタグごとに`_collect_own_data`を独立呼び出しするため、
        複数の候補タグが同一(fy, end_dates)衝突を独立に検出すると、内容が完全
        同一のエントリが候補タグ数だけ重複して`collisions`に含まれる場合がある
        （AVAV/CAKE/COHR/CRM/FCX/FICO/HONで実在確認済み。決定的なバグであり、
        実行のたびに増殖するのではなく毎回同じ件数だけ重複する）。値の採用
        ロジック自体には影響しないため、根本原因（`_extract_values_best_candidate`
        の候補タグループ）には手を入れず、ここで(field, fy, end_dates, resolution)
        をキーに重複排除してから書き込む対症療法とした。
        """
        seen = set()
        deduped: List[Dict[str, Any]] = []
        for c in collisions:
            key = (c.get("field"), c.get("fy"), tuple(c.get("end_dates", [])), c.get("resolution"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(c)

        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        path = os.path.join(ticker_dir, "fy_collision_log.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"ticker": ticker, "collisions": deduped}, f, ensure_ascii=False, indent=2)
        if deduped:
            print(f"   [{ticker}] fyキー競合を検知・記録: {len(deduped)}件 ({path})")

    def _save_fy_tag_mismatch_log(self, ticker: str, mismatches: List[Dict[str, Any]]) -> None:
        """
        ARCH-DATA-1ステージ3（fyタグ裏取り）: 年度バケツキー（determine_fiscal_year()の
        計算結果）と採用エントリの生XBRL fyタグが食い違うケースを
        report_consistency_check.pyから参照できる形で記録する。
        CHECK-22（fy_collision_log.json、同一fyタグへの複数本人end_date競合を検知）
        とは独立した別軸のチェックであり、こちらは「fyタグは単一だが値の年度
        バケツ配置自体がfyタグと異なる」ケース（CDNS型）を対象とする。
        自動修正は行わない（WARN出力のみ）。
        """
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        path = os.path.join(ticker_dir, "fy_tag_mismatch_log.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"ticker": ticker, "mismatches": mismatches}, f, ensure_ascii=False, indent=2)
        if mismatches:
            print(f"   [{ticker}] fyタグ裏取り不一致を検知・記録: {len(mismatches)}件 ({path})")

    def _save_fye_boundary_collision_log(self, ticker: str, collisions: List[Dict[str, Any]]) -> None:
        """
        FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: 決算期変更の境界年で「本人データ」
        （is_own_data判定を通過したエントリ）と、翌年10-Kの比較年度再掲データ等
        「生fyタグが異なる別のエントリ」が同一年度バケツ（computed_year）で競合した
        ケースを記録する。CHECK-22（fy_collision_log.json、同一fyタグへの複数
        本人end_date競合）・CHECK-23（fy_tag_mismatch_log.json、勝者自身の
        fyタグと年度バケツの不一致）のいずれとも異なる軸: 本件は競合する2エントリの
        生fyタグが元々異なるため（例: RCAT、本人データ側fy=2024・非本人データ側
        fy=2025）、CHECK-22（同一fyタグ前提）・CHECK-23（勝者自身のfyタグと
        バケツの不一致が対象、敗者側は対象外）のいずれの検知条件にも該当しない。

        `_own_override_is_safe()`自体は変更せず、その呼び出し前後で既存
        （フォールバック採用済み）側と本人データ候補側の生fyタグを比較する形で
        検知する（詳細は_extract_values_merged/_extract_values_best_candidate
        内のboundary_collisions_out追記箇所参照）。自動修正は行わない
        （WARN出力のみ）。0件でも毎回書き込む（fy_collision_log/
        fy_tag_mismatch_logと同じ化石ファイル対策）。
        """
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        path = os.path.join(ticker_dir, "fye_boundary_collision_log.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"ticker": ticker, "collisions": collisions}, f, ensure_ascii=False, indent=2)
        if collisions:
            print(f"   [{ticker}] 決算期変更境界の年度バケツ競合を検知・記録: {len(collisions)}件 ({path})")

    def _save_spac_shell_detection_log(self, ticker: str, detections: List[Dict[str, Any]]) -> None:
        """
        [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2: 法人名変更履歴（former_names）
        の[from, to]区間一致によりBS実体混在の是正が発火した年度を記録する
        （`_resolve_bs_entity_mixing()`のspac_detections_out参照）。

        数学的矛盾の有無に関わらず、former_namesが一致して発火したケースは
        すべて記録する（BBAI/RDW/RKLB/SOFI/VRTのように矛盾も同時に存在する
        ケースも含む。段階2の検知範囲を監査目的で可視化するため）。
        0件でも毎回書き込む（fy_collision_log等と同じ化石ファイル対策）。
        """
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        path = os.path.join(ticker_dir, "spac_shell_detection_log.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"ticker": ticker, "detections": detections}, f, ensure_ascii=False, indent=2)
        if detections:
            print(f"   [{ticker}] SPAC合併疑い(法人名変更履歴一致)を検知・記録: {len(detections)}件 ({path})")

    # [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]: 会計恒等式
    # Total_Assets = Total_Liabilities + Stockholders_Equity（+NCI+一時的
    # 持分）の検証。実装前シミュレーション（チャット記録）で、無条件に
    # NCI・一時的持分を加算する設計は既存の正しいケース（KO/WMT/VZ等、
    # stockholders_equityが候補タグ選定の結果次第でNCI込みの場合がある）で
    # 二重計上による新規誤検知を引き起こすと判明したため、①まず
    # TA==TL+SE（本体のみ）を試し、②不一致の場合のみ許可リストの
    # NCI・一時的持分タグを加算した拡張形を試す、というOR条件の
    # フォールバック方式を採用する。

    # 許可リスト: 簿価（carrying amount）を表すタグのみに限定する。
    # 実装前シミュレーションで、単純な部分一致（タグ名に"Noncontrolling"・
    # "TemporaryEquity"を含むか）は、TemporaryEquityLiquidationPreference
    # （清算優先分配額）・RedeemableNoncontrollingInterestEquityCommon
    # FairValue（公正価値）等、簿価とは異なる測定基準の開示専用タグまで
    # 合算してしまい、LYFT(2018)で$10.3Bの過大計上を引き起こす等、重大な
    # 誤りがあることが判明したため、簿価タグのみへ限定する。
    # [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]ASTS(2020)個別調査（Stage 3
    # 実装、チャット記録）: `TemporaryEquityValueExcludingAdditionalPaid
    # InCapital`を追加。RDW型（RedemptionValue、フォールバック機構へ追加）
    # とは異なり、本タグは無条件でこの主許可リストへ追加した。理由:
    # ①本タグはASTS own accnの一次パス（own-accn限定）だけで残差
    # $150,596,928と完全一致（diff=$0）し、二次パス（cross-accn探索）を
    # 経由する`MinorityInterest`のcross-accn一致（$2,490,000、後年
    # filingの比較列由来）は一次パスで既に恒等式が解決するため参照され
    # ない（本タグをフォールバック機構側へ追加した場合、既にcross-accnで
    # matched済みのMinorityInterestの上に本タグが後乗せされ、
    # diff=-$2,490,000という不正確な〈許容誤差内のため見かけ上resolved
    # になるだけの〉合算になってしまうことをシミュレーションで確認済み）。
    # ②「Value Excluding APIC」という名称自体が貸借対照表上の簿価
    # （CarryingAmount系と同種の測定基準）を表しており、RedemptionValue
    # のような測定基準の異なる開示専用タグではない。
    # 全105銘柄・全既知違反年度の机上シミュレーションで、本タグの追加は
    # ASTS(2020)を解消する以外に、FRSH(2020)（既に解消済み・
    # `TemporaryEquityCarryingAmountAttributableToParent`使用）で
    # 同タグの$0.0001（XBRL上の名目値、複数期間で同一の定型値と確認済み）
    # が追加で一致するのみで、金額・解決判定とも実質的な影響なしと確認
    # 済み。ASTS(2019)（別問題、[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]
    # ③要さらなる確認）は本タグ追加後も未解消のまま（cross-accn値が
    # 必要額と一致しないため）で、想定通り無関係。
    # [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]: frozensetは
    # ハッシュランダム化でイテレーション順序が実行のたびに変わり、下記
    # `for tag in self._BS_IDENTITY_ALLOWLIST:`（3010・3026行目）で構築する
    # `matched`/`extra_components`辞書のキー順序がbs_identity_violations_
    # log.json上で無関係なdiffを発生させていたため、順序が決定的なtupleに
    # 変更した（メンバーシップ判定以外の集合演算では使われていないため
    # 型変更の副作用なし。値・ロジックは変更しない純粋な型変更）。
    _BS_IDENTITY_ALLOWLIST = (
        "MinorityInterest",
        "TemporaryEquityCarryingAmount",
        "TemporaryEquityCarryingAmountAttributableToParent",
        "TemporaryEquityCarryingAmountIncludingPortionAttributableToNoncontrollingInterests",
        "RedeemableNoncontrollingInterestEquityCarryingAmount",
        "RedeemableNoncontrollingInterestEquityCommonCarryingAmount",
        "RedeemableNoncontrollingInterestEquityPreferredCarryingAmount",
        "TemporaryEquityValueExcludingAdditionalPaidInCapital",
    )

    # "Including...NoncontrollingInterests"系タグ（一時的持分のうちNCI分も
    # 含む合算値）が存在する場合、"AttributableToParent"系・無印の
    # "TemporaryEquityCarryingAmount"はその内訳の一部を指すため、両方を
    # 合算すると二重計上になる。前者が存在する場合は後者を除外する。
    #
    # [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]ONDS(2023)個別調査（実装前
    # チャット記録）: RedeemableNoncontrollingInterestEquityCarrying
    # Amount（優先株式・普通株式を合算した総額）が存在するのに、内訳の
    # 一部である...PreferredCarryingAmount/...CommonCarryingAmountを
    # 別途加算すると、優先株式分が二重計上される（ONDS(2023)で実測:
    # base_diff($11,920,694)がCarryingAmount単体と完全一致するのに対し、
    # ...PreferredCarryingAmount($14,692,000)まで加算すると
    # 過大計上〈-13.7%〉に転じることを確認済み）。同型のルールを追加する。
    _BS_IDENTITY_SUPERSEDES = {
        "TemporaryEquityCarryingAmountIncludingPortionAttributableToNoncontrollingInterests": frozenset([
            "TemporaryEquityCarryingAmountAttributableToParent",
            "TemporaryEquityCarryingAmount",
        ]),
        "RedeemableNoncontrollingInterestEquityCarryingAmount": frozenset([
            "RedeemableNoncontrollingInterestEquityCommonCarryingAmount",
            "RedeemableNoncontrollingInterestEquityPreferredCarryingAmount",
        ]),
    }

    # [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]HEI(2009-2013)個別調査
    # （実装前チャット記録）: 2009-2013年当時のHEIは一時的持分の簿価を
    # 表す"CarryingAmount"系タグを一切報告しておらず、
    # `TemporaryEquityRedemptionValue`（償還価額）がその代わりに貸借
    # 対照表上の簿価として機能していたことを実測で確認済み（例: 2013年度
    # TL+SE+MinorityInterest+RedemptionValueがTAと完全一致）。ただし
    # RedemptionValueは通常、簿価とは異なる測定基準の開示専用タグである
    # ため（LYFT(2018)等での過大計上の教訓）、無条件加算は行わず、同一
    # accn・同一end_dateに簿価系タグ（下記_BS_IDENTITY_CARRYING_AMOUNT_
    # TEMP_EQUITY_TAGS）が1つも存在しない場合のみのフォールバックとして
    # 限定する。
    #
    # [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]RDW(2020)個別調査（Stage 3
    # 実装、チャット記録）: `RedeemableNoncontrollingInterestEquityCommon
    # RedemptionValue`もHEI型と同型の別タグ名パターンと確認（RDW own accn
    # ・own end_dateに簿価系タグが1つも存在せず、このRedemptionValueタグ
    # （$120,314,578）を加算するとTA=TL+SE+RedemptionValueが完全一致）。
    # 全105銘柄・全既知違反年度での机上シミュレーションで、このタグを
    # `_BS_IDENTITY_ALLOWLIST`へ無条件追加する案・本フォールバック機構へ
    # 追加する案のいずれも結果は同一（RDW(2020)のみ解消、他104銘柄・
    # RDW自身の他年度に影響なし）と確認したが、RedemptionValueは簿価とは
    # 異なる測定基準であるという上記TemporaryEquityRedemptionValueと同じ
    # 設計上の懸念が当てはまるため、安全側であるフォールバック機構への
    # 追加を採用した（無条件許可リストへの追加は、将来别銘柄で簿価系タグと
    # このRedemptionValueタグが同一accn・同一end_dateに共存した場合の
    # 二重計上リスクを理論上残すため）。
    _BS_IDENTITY_CARRYING_AMOUNT_TEMP_EQUITY_TAGS = frozenset([
        "TemporaryEquityCarryingAmount",
        "TemporaryEquityCarryingAmountAttributableToParent",
        "TemporaryEquityCarryingAmountIncludingPortionAttributableToNoncontrollingInterests",
    ])
    _BS_IDENTITY_FALLBACK_ONLY_TAGS = (
        "TemporaryEquityRedemptionValue",
        "RedeemableNoncontrollingInterestEquityCommonRedemptionValue",
    )

    _BS_IDENTITY_TOL_REL = 0.02
    _BS_IDENTITY_TOL_ABS = 2_000_000

    # [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]: 重複値ガード用の
    # 許容誤差（本体の恒等式許容誤差とは別の、より厳格な基準）。
    _BS_IDENTITY_DUP_TOL_REL = 0.001
    _BS_IDENTITY_DUP_TOL_ABS = 1_000

    @staticmethod
    def _bs_identity_values_equal(a: float, b: float) -> bool:
        if a is None or b is None:
            return False
        if abs(a - b) <= SECParser._BS_IDENTITY_DUP_TOL_ABS:
            return True
        denom = max(abs(a), abs(b), 1)
        return abs(a - b) / denom <= SECParser._BS_IDENTITY_DUP_TOL_REL

    @staticmethod
    def _find_assets_end_date(us_gaap: dict, accn: Optional[str], target_val: Any) -> Optional[str]:
        """total_assetsの採用accn・値から対応するend_dateを逆引きする
        （total_assetsの候補タグはXBRL_MAPPINGで"Assets"単独のため、
        このタグのみを見れば良い）。"""
        if not accn:
            return None
        entries = us_gaap.get("Assets", {}).get("units", {}).get("USD", [])
        for e in entries:
            if e.get("accn") == accn and e.get("val") == target_val and e.get("form") in ("10-K", "10-K/A"):
                return e.get("end")
        for e in entries:
            if e.get("accn") == accn and e.get("val") == target_val:
                return e.get("end")
        return None

    def _bs_identity_extra_components(self, us_gaap: dict, accn: str, end_date: str,
                                       ta: float, tl: float, se: float) -> Dict[str, float]:
        """同一accn・同一end_dateに紐づくNCI・一時的持分の簿価タグ（許可
        リストのみ）を収集する。instant fact（start_date不要）のみ対象。

        [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]: own-accnに該当
        タグが1件も見つからない場合、同一end_dateの他filing（後続四半期の
        比較列等）へ探索範囲を広げるフォールバックを行う。M&A・組織再編
        直後、一時的持分が当該年度自身の10-Kには一切開示されず後続filing
        の比較列としてのみ開示されるケース（COHR/CRWV/VRT）に対応。

        フォールバックには2段階のガードを設ける（実データ検証で判明した
        回帰〈SOUN2021・PM2010/2011・TSLA2020/2021・HEI2014・FCX2015〉の
        再発防止）:
        ① ベースゲート: own-accnのみの値で恒等式が厳密に一致（diff=0）
           する場合、そのentity（年度）に対してはcross-accnフォール
           バック自体を一切実行しない。own-accnのみで既に完成している
           解を、無関係な後続filingの値で上書きしないため。
        ② 重複値ガード: ①を通過した場合でも、フォールバックで見つかった
           候補値が既にmatched済みの他タグの値と一致（許容誤差0.1%または
           $1,000以内）する場合は採用しない（別タグ族での同額の二重計上
           を防ぐ。実例: FCX(2015)の`RedeemableNoncontrollingInterest
           EquityCarryingAmount`が、既にmatched済みの`TemporaryEquity
           CarryingAmountIncludingPortion...`と同額$764,000,000で重複
           していた）。
        """
        matched: Dict[str, float] = {}
        for tag in self._BS_IDENTITY_ALLOWLIST:
            tagdata = us_gaap.get(tag)
            if not tagdata:
                continue
            entries = tagdata.get("units", {}).get("USD", [])
            for e in entries:
                if (e.get("accn") == accn and e.get("end") == end_date
                        and not e.get("start")):
                    matched[tag] = e.get("val")
                    break
        for winner, losers in self._BS_IDENTITY_SUPERSEDES.items():
            if winner in matched:
                for loser in losers:
                    matched.pop(loser, None)

        if ta - (tl + se + sum(matched.values())) != 0:
            for tag in self._BS_IDENTITY_ALLOWLIST:
                if tag in matched:
                    continue
                tagdata = us_gaap.get(tag)
                if not tagdata:
                    continue
                entries = tagdata.get("units", {}).get("USD", [])
                candidate = None
                for e in entries:
                    if e.get("end") == end_date and not e.get("start"):
                        candidate = e.get("val")
                        break
                if candidate is None:
                    continue
                if any(self._bs_identity_values_equal(candidate, v) for v in matched.values()):
                    continue
                matched[tag] = candidate
            for winner, losers in self._BS_IDENTITY_SUPERSEDES.items():
                if winner in matched:
                    for loser in losers:
                        matched.pop(loser, None)

        # [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]HEI型フォールバック
        # （RDW(2020)対応でRedeemableNoncontrollingInterestEquityCommon
        # RedemptionValueを追加、複数タグ対応化）:
        # 簿価系タグ（_BS_IDENTITY_CARRYING_AMOUNT_TEMP_EQUITY_TAGS）が
        # 1つも見つからなかった場合のみ、測定基準の異なるRedemptionValue系
        # タグ（_BS_IDENTITY_FALLBACK_ONLY_TAGS）を先頭から順に探索し、
        # 最初に見つかったものを最終手段として採用する。
        if not (self._BS_IDENTITY_CARRYING_AMOUNT_TEMP_EQUITY_TAGS & matched.keys()):
            for tag in self._BS_IDENTITY_FALLBACK_ONLY_TAGS:
                tagdata = us_gaap.get(tag)
                if not tagdata:
                    continue
                entries = tagdata.get("units", {}).get("USD", [])
                found = False
                for e in entries:
                    if (e.get("accn") == accn and e.get("end") == end_date
                            and not e.get("start")):
                        matched[tag] = e.get("val")
                        found = True
                        break
                if found:
                    break

        return matched

    def _check_bs_identity_violations(self, ticker: str, result: Dict[str, Any],
                                       us_gaap: dict) -> List[Dict[str, Any]]:
        """年次データのTotal_Assets = Total_Liabilities + Stockholders_
        Equity（+NCI+一時的持分）を検証する。検知専用（自動修正なし）。
        ①本体一致で解消したケースはログに含めない（ノイズ削減、既存の
        fy_collision_log等と同じ「異常のみ記録」方針）。②拡張形で解消した
        ケース・③いずれでも解消しないケースのみ記録する。
        """
        violations: List[Dict[str, Any]] = []
        for year, data in result.get("annual", {}).items():
            bs = data.get("bs", {})
            ta = bs.get("total_assets")
            tl = bs.get("total_liabilities")
            se = bs.get("stockholders_equity")
            if ta is None or tl is None or se is None:
                continue

            base_diff = ta - (tl + se)
            base_denom = max(abs(ta), abs(tl + se), 1)
            base_pct = base_diff / base_denom
            if abs(base_diff) <= self._BS_IDENTITY_TOL_ABS or abs(base_pct) <= self._BS_IDENTITY_TOL_REL:
                continue  # ①本体一致で解消。ログ対象外。

            prov = data.get("bs_provenance", {}).get("total_assets", {})
            accn = prov.get("accn")
            end_date = self._find_assets_end_date(us_gaap, accn, ta)

            entry: Dict[str, Any] = {
                "period": year,
                "accn": accn,
                "end_date": end_date,
                "total_assets": ta,
                "total_liabilities": tl,
                "stockholders_equity": se,
                "diff_base": base_diff,
                "diff_base_pct": round(base_pct, 4),
            }

            if not accn or not end_date:
                entry["extra_components"] = {}
                entry["diff_extended"] = base_diff
                entry["resolved_by_extension"] = False
                entry["method"] = None
                violations.append(entry)
                continue

            extra = self._bs_identity_extra_components(us_gaap, accn, end_date, ta, tl, se)
            extra_sum = sum(extra.values())
            ext_diff = ta - (tl + se + extra_sum)
            ext_denom = max(abs(ta), abs(tl + se + extra_sum), 1)
            ext_pct = ext_diff / ext_denom
            resolved = abs(ext_diff) <= self._BS_IDENTITY_TOL_ABS or abs(ext_pct) <= self._BS_IDENTITY_TOL_REL

            entry["extra_components"] = extra
            entry["diff_extended"] = ext_diff
            entry["resolved_by_extension"] = resolved
            entry["method"] = "extended" if resolved else None
            violations.append(entry)

        return violations

    def _save_bs_identity_violations_log(self, ticker: str, violations: List[Dict[str, Any]]) -> None:
        """[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]: BS恒等式検証の
        結果をreport_consistency_check.pyから参照できる形で記録する。
        本体一致（method="base"相当）のケースはノイズ削減のため
        _check_bs_identity_violations()側で除外済みで、ここに記録される
        のは①拡張形で解消したケース（resolved_by_extension=True）・
        ②いずれでも解消しないケース（resolved_by_extension=False）のみ。
        0件でも毎回書き込む（fy_collision_log等と同じ化石ファイル対策）。
        """
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        path = os.path.join(ticker_dir, "bs_identity_violations_log.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"ticker": ticker, "violations": violations}, f, ensure_ascii=False, indent=2)
        if violations:
            unresolved = sum(1 for v in violations if not v.get("resolved_by_extension"))
            print(f"   [{ticker}] 会計恒等式(TA=TL+SE)検知: {len(violations)}件"
                  f"（拡張形で解消{len(violations)-unresolved}件・未解消{unresolved}件） ({path})")

    def _apply_fact_overrides(self, ticker: str, extracted: Dict[str, Any]) -> None:
        """fact_overrides.json記載の個別上書きを、extracted[field]["annual"]
        [year]へ直接適用する（該当tickerが未登録なら何もしない）。

        [[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]: 全ての逆算バックフィル処理
        （_backfill_total_liabilities_via_identity()・_backfill_gross_
        profit_from_revenue_cogs()等）より前、_parse_raw_data()の抽出直後に
        実行する。旧実装はsave_parsed_data()内の最終段（逆算バックフィルより
        後）でdata["pl"]に直接書き込んでいたため、GOOGL(2012/2013)で
        gross_profitが補正前revenueを使った古い逆算値のまま保存される不整合
        があった。extracted側で書き換えることで、後続の逆算バックフィルが
        補正後の値を入力として使用できる。

        CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1: 本人データ優先ロジックの一般
        動作はここでは一切変更しない。ロジックが出した結果を、特定
        ticker+year+fieldのみ明示的に差し替える後処理。
        """
        overrides = _load_fact_overrides().get(ticker, {})
        if not overrides:
            return
        for year_str, override in overrides.items():
            try:
                year = int(year_str)
            except ValueError:
                continue
            reason = override.get("reason", "")
            for field, ov in override.get("fields", {}).items():
                field_data = extracted.get(field)
                if field_data is None:
                    continue
                annual = field_data.get("annual", {})
                if annual.get(year) is None:
                    continue  # 抽出値が存在しない年度は対象外（元実装と同じゲート条件）
                annual[year] = ov["value"]
                field_data.setdefault("_annual_provenance", {})[year] = {
                    "accn": ov.get("source_accn"),
                    "filed": ov.get("source_filed"),
                    "is_own_data": False,
                    "override_applied": True,
                    "override_reason": reason,
                }

    _FIXED_REGISTRY_CATEGORIES = ("bs", "pl", "cf", "shares", "other")

    def _apply_fixed_registry_freeze(self, ticker: str, result: Dict[str, Any]) -> None:
        """fixed_registry.json登録済みのticker×年度について、
        `fields_snapshot`に記録されたフィールドを既存annual_{year}.jsonの
        値へ強制復元する（差分適用方式）。

        [[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]で確定した「フィックス」
        機構の実装。`_apply_fact_overrides()`・各種逆算バックフィルより
        後、`result["annual"]`組み立て直後の最終段に配置することで、通常の
        抽出・上書き・バックフィルを一通り計算させた後に、フィックス対象
        フィールドだけ強制的に旧値へ上書きする「最後の関所」として機能
        させる（今回計算された値が何であれ、最終的にはフィックス時点の
        値で上書きされる）。

        `fields_snapshot`に無いフィールド（＝フィックス後にXBRL_MAPPING等
        へ新規追加されたフィールド）は通常の抽出結果をそのまま通す
        （差分適用方式）。

        annual（本メソッド）のみを対象とし、quarterly/TTM側
        （`layer3_builder.py`・`ttm_calculator.py`）は`parser.py`とは
        完全に独立した別パイプラインのため対象外（[[TTM-DATA-DRIFT-
        BEHIND-PIPELINE-1]]、スコープ限定は設計時に確定済み）。
        """
        registry = _load_fixed_registry().get(ticker, {})
        if not registry:
            return

        for year_str, entry in registry.items():
            year = int(year_str)
            if year not in result["annual"]:
                raise RuntimeError(
                    f"{ticker} {year}: fixed_registry.json登録済みだが今回の"
                    f"抽出結果に該当年度が存在しない（データ欠落の疑い）"
                )

            old_path = os.path.join(self.data_dir, ticker, f"annual_{year}.json")
            if not os.path.exists(old_path):
                raise RuntimeError(
                    f"{ticker} {year}: fixed_registry.json登録済みだが"
                    f"旧annual_{year}.jsonが見つからない"
                )
            with open(old_path, "r", encoding="utf-8") as f:
                old_data = json.load(f)

            fixed_fields = entry.get("fields_snapshot", [])
            for field in fixed_fields:
                found = False
                for category in self._FIXED_REGISTRY_CATEGORIES:
                    old_cat = old_data.get(category, {})
                    if field not in old_cat:
                        continue
                    result["annual"][year].setdefault(category, {})[field] = old_cat[field]
                    prov_key = f"{category}_provenance"
                    old_prov = old_data.get(prov_key, {})
                    if field in old_prov:
                        result["annual"][year].setdefault(prov_key, {})[field] = old_prov[field]
                    found = True
                    break
                if not found:
                    raise RuntimeError(
                        f"{ticker} {year}: fixed_registry.jsonのfields_snapshotに"
                        f"記録されたフィールド'{field}'が旧annual_{year}.jsonに"
                        f"見つからない（registryとデータの不整合）"
                    )

    def save_parsed_data(self, ticker: str, parsed: dict) -> None:
        """パース済みデータを個別ファイルに保存"""
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)

        # 年次データ
        for year, data in parsed.get("annual", {}).items():
            path = os.path.join(ticker_dir, f"annual_{year}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "period": year,
                    "form": "10-K",
                    **data
                }, f, ensure_ascii=False, indent=2)
        
        # 四半期データ
        for quarter, data in parsed.get("quarterly", {}).items():
            path = os.path.join(ticker_dir, f"quarterly_{quarter}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "period": quarter,
                    "form": "10-Q",
                    **data
                }, f, ensure_ascii=False, indent=2)
        
        print(f"   [{ticker}] 保存完了: {len(parsed.get('annual', {}))}年次, {len(parsed.get('quarterly', {}))}四半期")
    
    def parse_and_save(self, ticker: str) -> Optional[Dict[str, Any]]:
        """パースして保存"""
        parsed = self.parse_company_facts(ticker)
        if parsed:
            self.save_parsed_data(ticker, parsed)
        return parsed


if __name__ == "__main__":
    parser = SECParser()
    
    # テスト
    parsed = parser.parse_and_save("TSLA")
    if parsed:
        print(f"\n年次データ: {list(parsed['annual'].keys())}")
        print(f"四半期データ: {list(parsed['quarterly'].keys())[:8]}...")
