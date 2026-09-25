"""
common/sec_data/quarterly.py
責務: company_facts.json から四半期Raw Tableを生成する（インメモリ、
normalizer.py::normalize()への入力）。ディスクへの永続化は行わない
（[[SECDATA-STORAGE-FRAGMENTATION-1]]、2026-08-05: raw/への書き込みは
実消費者ゼロのデッドコードと判明したため削除。以前は
raw/{ticker}_quarterly_raw.jsonへ保存していた）。
"""

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta

from .tag_definitions import (
    TAG_CANDIDATES, NET_INCOME_CANDIDATES, with_derived_net_income,
)
from .utils import quarters_in_trailing_window
from .fact_selection import select_latest_filed

logger = logging.getLogger(__name__)

# 銘柄別制限（ハードコード）
TICKER_RESTRICTIONS: dict[str, dict] = {
    "MSFT":  {"exclude": ["DA"]},
    "APP":   {"exclude": ["CapEx"]},
    "GOOGL": {
        "approximate": ["DA"],
        "note_discontinuous": ["LTDebt"],
    },
    # 金融系: Revenueタグを銀行固有のタグに上書き
    # 【調査済み 2026-05-20】
    # SOFIは "Revenues" タグを申告していない。
    # フォールバックで RevenueFromContractWithCustomerExcludingAssessedTax(~130M, 手数料のみ)と
    # RevenuesNetOfInterestExpense(~1100M, 全社収益)が同一end/start/filedでマージされ、
    # max(filed)が不定になるため採用タグがランダムになる。
    # → revenue_concept で RevenuesNetOfInterestExpense に固定することが必須。
    # SOFI-DATA-1（2026-06-24発見・2026-07-13パイプライン恒久化）:
    # 銀行免許取得後（2022年以降）LongTermDebt/LongTermDebtNoncurrentタグの
    # 申告を停止し、短期+長期合算タグDebtLongtermAndShorttermCombinedAmountに
    # 移行。standard fallbackへの追加はAVGO/VZ等の無関係な既存LTDebtデータにも
    # 波及することを検証で確認したため、SOFI限定のticker_restrictionsとした。
    # FY52WEEK-BS-STI-OVERRIDE-DESIGN-1（2026-07-19）: short_term_investments
    # もBS本体「Investment securities」全体を表すOtherInvestmentsタグに固定。
    # 当初想定していたAvailableForSaleSecuritiesDebtSecuritiesはAFS分類分の
    # サブセット（FY2025時点で95.3%のみ）しか捕捉できず、残差はNote 15
    # （公正価値ヒエラルキー）に含まれる非AFS分類の資産担保証券・残余持分
    # （証券化VIE由来）であることを一次情報で確認済み。OtherInvestmentsは
    # BS「Investment securities」合計と全期間で完全一致する。
    "SOFI": {
        "revenue_concept": "RevenuesNetOfInterestExpense",
        "ltdebt_concept": "DebtLongtermAndShorttermCombinedAmount",
        "sti_concept": "OtherInvestments",
        "note": "フィンテック銀行。Revenuesタグなし。フォールバックが"
                "RevenueFromContract(130M=手数料のみ)とRevenuesNetOfInterest(1100M=全社収益)を"
                "混在させ採用タグが不定になる。revenue_conceptで単一タグに固定が必須。"
                "LTDebtも2022年以降LongTermDebt系タグの申告を停止しており"
                "ltdebt_conceptで単一タグに固定が必須。short_term_investmentsは"
                "非分類BS（流動/非流動を区分しない銀行持株会社）のため"
                "sti_conceptで単一タグ（OtherInvestments、BS合計と完全一致）に固定。",
    },
    # BUG-REV-SPAC-1 (2026-06-12 修正)
    # IONQの2022年10-KにおいてRevenuesタグが$1,235M (SPAC関連資金調達額) を誤タグして報告している。
    # merge_all_tags=True + 同一end_date (2022-12-31) でRevenuesが先頭タグのため勝ち、
    # 正しい営業収益RevenueFromContractWithCustomer($11.1M)が採用されない。
    # → revenue_conceptで正しいタグに固定。
    "IONQ": {
        "revenue_concept": "RevenueFromContractWithCustomerExcludingAssessedTax",
        "note": "量子コンピューティング企業。2022年10-KのRevenuesタグが"
                "SPAC調達金($1,235M)を誤タグ。正しい営業収益は"
                "RevenueFromContractWithCustomerExcludingAssessedTax($11.1M)。",
    },
    # FY52WEEK-BS-STI-OVERRIDE-DESIGN-1（2026-07-19）:
    # short_term_investmentsのXBRL_MAPPING標準候補群（Current接尾辞系タグ）は
    # KLAC/TER/Vいずれも数年前に申告停止済みで機能しない。かつ正しいタグが
    # 汎用的な名称（他銘柄でも別の意味で広く使われる）のためグローバル候補
    # リストへは追加せず、ticker限定のsti_conceptで固定する。
    "KLAC": {
        "sti_concept": "AvailableForSaleSecuritiesDebtSecurities",
        "note": "BACKLOG当初想定のAvailableForSaleSecuritiesDebtSecuritiesCurrent"
                "は2021-03-31を最後に申告停止済みの死んだタグ。'Current'接尾辞なしの"
                "AvailableForSaleSecuritiesDebtSecuritiesが現在も継続申告されており、"
                "BS「Marketable securities」の99.0%（FY2025）に一致（残差は"
                "EquitySecuritiesFvNiCost約$22.9Mの株式性有価証券、本タグの対象外）。",
    },
    "TER": {
        "sti_concept": "AvailableForSaleSecuritiesDebtMaturitiesWithinOneYearFairValue",
        "note": "標準候補群（AvailableForSaleSecuritiesCurrent等）は2021年以降"
                "申告停止済み。満期別内訳の「1年以内」タグがBS「Marketable "
                "securities」（FY2025: $28,247K）と完全一致（誤差ゼロ）。",
    },
    "V": {
        "sti_concept": "Investments",
        "note": "標準候補群は2020年以降申告停止済み。'Investments'タグ（'Current'"
                "接尾辞なしの汎用名）がBS流動資産側「Investment securities」"
                "（FY2025: $1,833,000,000）と完全一致。他9銘柄（ADSK/BSY/CEG/"
                "CRWV/DELL/LRCX/LYFT/MO/ONDS）も同タグを別の意味で申告している"
                "ため、V限定オーバーライドとして厳格運用し、グローバル候補"
                "リストには絶対に追加しないこと。",
    },
    # CASH-TAG-MISSING-1（2026-08-30）: cash_and_equivalentsの候補リストへ
    # CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents（ASU
    # 2016-18対応タグ、制限付き現金を含む）を追加した際、CPRT・HEIは
    # 従来のCashAndCashEquivalentsAtCarryingValueが引き続き機能して
    # いるにもかかわらず、新タグの方が直近年度まで新しく報告されている
    # ため_extract_values_best_candidate()の「最新annual年が新しい候補が
    # 全期間の勝者になる」設計により、機能していた期間まで制限付き現金
    # 混入の過大計上に置き換わってしまうことを全105銘柄シミュレーション
    # で確認した。両銘柄はcash_concept明示指定で既存タグへ固定する
    # （sti_concept等と同型のticker限定オーバーライド）。
    "CPRT": {
        "cash_concept": "CashAndCashEquivalentsAtCarryingValue",
        # LAYER3-COGS-CANDIDATE-TAG-EXPANSION-1（2026-09-06）:
        # CPRTの「Cost of vehicle sales」はCostDirectMaterialタグで
        # 申告されている（company_facts.jsonにFY2018から全期間存在、
        # 標準候補6タグのいずれにも含まれない）。CostDirectMaterialは
        # 名称が汎用的で、LYFT（部品費約$9M、COGSとは無関係の少額項目）
        # 等でも別の意味で申告されているため、グローバル候補リストへは
        # 追加せずticker限定オーバーライドとする。
        "cogs_concept": "CostDirectMaterial",
        "note": "CashAndCashEquivalentsAtCarryingValueが2019年まで機能して"
                "いるが、新設のCashCashEquivalentsRestrictedCashAndRestricted"
                "CashEquivalents（2025年まで申告）の方が新しいため無対応だと"
                "全期間の採用タグが後者に切り替わり、2019年以前を含め制限付き"
                "現金混入の過大計上になる。既存タグへ固定して回避する。"
                "cogs_conceptはCostDirectMaterial（Cost of vehicle sales、"
                "全期間存在）に固定。LYFT等がCostDirectMaterialを別の意味"
                "（COGSとは無関係の少額の部品費）で申告しているため、"
                "グローバル候補リストへは追加しない。",
    },
    # LAYER3-COGS-CANDIDATE-TAG-EXPANSION-1（2026-09-06）: CEG「Purchased
    # power and fuel」はCostDirectMaterialタグ（CPRTと同一タグ、
    # company_facts.jsonにFY2020から全期間存在）で申告されている。
    # CPRTと同じ理由でグローバル候補リストへは追加せず、ticker限定
    # オーバーライドとする。
    "CEG": {
        "cogs_concept": "CostDirectMaterial",
        "note": "「Purchased power and fuel」がCostDirectMaterialタグで"
                "全期間申告されている。CostDirectMaterialは他銘柄（LYFT等）"
                "で全く異なる少額項目に使われているため、グローバル候補"
                "リストへは追加せずCPRTと同じくticker限定オーバーライドと"
                "する。",
    },
    # LAYER3-COGS-CANDIDATE-TAG-EXPANSION-1（2026-09-06）: JOBY「Cost of
    # Revenue」はOtherCostAndExpenseOperatingタグ（company_facts.jsonに
    # 2021年から全期間存在）で申告されている。
    #
    # 【VSTには適用しない、意図的な除外】: VSTも同タグ（「Operating
    # costs」$2,803M、FY2025）を申告しているが、原価の最大コンポーネント
    # （Fuel/Purchased Power costs、約76%相当）がVista固有のカスタム
    # 拡張タグvistra:CostOfFuelPurchasedPowerAndDeliveryのため、
    # OtherCostAndExpenseOperating単独では原価の24%程度しか捕捉できない。
    # 部分回収した値をcost_of_revenueとして採用すると、gross_margin計算上
    # 実態より大幅に良好なマージンを表示してしまうミスリードになるため、
    # VSTはcogs_conceptを設定せずNoneのまま維持する方が誠実と判断した
    # （2026-09-04付BACKLOG登録時の判断を実装時も踏襲）。
    "JOBY": {
        "cogs_concept": "OtherCostAndExpenseOperating",
        "note": "「Cost of Revenue」がOtherCostAndExpenseOperatingタグで"
                "全期間申告されている（2021年〜、2025年Blade買収以降のみ"
                "有意な値を持つ点に注意——過去年度との時系列比較〈gross"
                "margin trend等〉に不連続が生じる）。OtherCostAndExpense"
                "Operatingは他銘柄（AMZN/CAKE/CAT/FCX/KO/LMT/META/RCAT等）"
                "で全く異なる少額の会計項目に使われていることを全105銘柄"
                "相当のcompany_facts.json横断チェックで確認済みのため、"
                "グローバル候補リストへは追加せずJOBY限定オーバーライドと"
                "する。VSTは同タグを申告しているが、原価の76%相当が別の"
                "カスタムタグのため意図的に適用しない（本ファイル冒頭の"
                "コメント参照）。",
    },
    "HEI": {
        "cash_concept": "CashAndCashEquivalentsAtCarryingValue",
        "note": "CPRTと同型。CashAndCashEquivalentsAtCarryingValueが2022年"
                "まで機能しているが、新設タグの方が2025年まで新しいため"
                "無対応だと2022年以前を含め過大計上になる。既存タグへ固定して"
                "回避する。",
    },
    # NVDA-STI-TAG-UNIDENTIFIED-1（2026-07-19実装・対応方針①採用）:
    # ANOMALY-PATTERN-CATALOG-1 型C（資産クラス変化・当年度未タグ化型）。
    # FY2026第1四半期に非上場投資先1社が上場したことで、BS「Marketable
    # securities」の内訳に上場株式の公正価値が混入し、単一タグでの捕捉が
    # 不可能になった（KLAC/TER/V/SOFIのsti_conceptのような単一タグ差替えでは
    # 解決しない）。
    #
    # cross_filing_tags: ticker × period × field を明示指定した場合のみ、
    # 指定されたXBRL概念タグ群を「指定end_date・指定form」で直接検索し合算する
    # （parser.py::_apply_cross_filing_tags/_find_entry_by_end_date参照）。
    # 通常の_collect_own_data_annual/_instantが持つ`form in (10-K, 10-K/A)`
    # フィルタ・accn_reportdate自己一致チェックはグローバルには一切変更せず、
    # 本テーブルに明示登録された組み合わせにのみ迂回を適用する。
    #
    # periodがint（例: 2026）の場合はannualバケツ、str（例: "2027Q1"、
    # parser.pyのquarter_key形式 f"{fy}{fp}"）の場合はquarterlyバケツを上書きする。
    "NVDA": {
        "cross_filing_tags": {
            "short_term_investments": (
                {
                    # FY2026（FYE 2026-01-25）annual: 10-K本体BSの流動資産
                    # 「Marketable securities $51,951M」は単一のus-gaapタグで
                    # companyfactsに現れない。後続10-Q（Q1 FY2027 accn
                    # 0001045810-26-000052／Q2 accn -000075）が比較年度末値として
                    # 開示した`DebtSecuritiesCurrent`（$39,065M）＋
                    # `EquitySecuritiesFvNi`（$12,886M）の合算が10-K BSの
                    # $51,951Mと完全一致するため、両タグをform=10-Qで参照する
                    # （=真のクロスfiling参照）。
                    # 2026-07-19の初回登録（[[NVDA-STI-TAG-UNIDENTIFIED-1]]）は
                    # 債券部分に`AvailableForSaleSecuritiesDebtSecurities`
                    # （$39,520M、満期1年超の非流動分を含むAFS総額）を使っており
                    # $52,406M（+0.88%）の近似値だった。2026-09-25（[[BBAI-RDW-
                    # RUNWAY-VERIFICATION-1]]後続）に流動分のみのタグへ是正し
                    # 残差0となったため、approx_residual_pctは登録しない。
                    "period": 2026,
                    "end_date": "2026-01-25",
                    "components": (
                        {"tag": "DebtSecuritiesCurrent", "forms": ("10-Q",)},
                        {"tag": "EquitySecuritiesFvNi", "forms": ("10-Q",)},
                    ),
                },
                {
                    # 2027Q1（Q1 FY2027、end 2026-04-26）: 両タグとも当該10-Q
                    # 自身（accn 0001045810-26-000052、form=10-Q）のown data。
                    # 既存の_extract_values_best_candidate()は複数タグを同時に
                    # 合算する機構を持たない（freshness最良の1タグのみを採用する
                    # 設計）ため、本テーブルの同一の合算ロジックを転用する。
                    # 10-Q BSの流動「Marketable debt securities $37,098M」
                    # 「Marketable equity securities $30,237M」（計$67,335M）と
                    # 一致する`DebtSecuritiesCurrent`＋`EquitySecuritiesFvNi`を
                    # 使う（2026-09-25是正。初回登録のAvailableForSaleSecurities
                    # DebtSecurities $39,233Mは非流動分を含み$69,470Mと+$2,135M
                    # 過大だった）。
                    "period": "2027Q1",
                    "end_date": "2026-04-26",
                    "components": (
                        {"tag": "DebtSecuritiesCurrent", "forms": ("10-Q",)},
                        {"tag": "EquitySecuritiesFvNi", "forms": ("10-Q",)},
                    ),
                },
                {
                    # 2027Q2（Q2 FY2027、end 2026-07-26）: [[BBAI-RDW-RUNWAY-
                    # VERIFICATION-1]]後続（2026-09-25）で未登録による欠損
                    # （short_term_investments=0扱い）を発見し追加。10-Q原本
                    # （accn 0001045810-26-000075）のBS流動資産「Marketable debt
                    # securities $34,143M」「Marketable equity securities
                    # $42,783M」と一致する`DebtSecuritiesCurrent`＋
                    # `EquitySecuritiesFvNi`（計$76,926M）を採用する。
                    # `AvailableForSaleSecuritiesDebtSecurities`（本期$46,900M）
                    # は満期1年超の非流動分を含むAFS総額で、BSの流動行とは一致
                    # しないため使わない（本期で判明し、上2期も同構成へ是正）。
                    # タグ構成は将来の期で再び変わりうるため、全期一律適用には
                    # せず期ごとの明示登録（BS原本との照合付き）を維持する。新四半期の登録漏れは
                    # report_consistency_check.pyのCHECK-49で検知する。
                    "period": "2027Q2",
                    "end_date": "2026-07-26",
                    "components": (
                        {"tag": "DebtSecuritiesCurrent", "forms": ("10-Q",)},
                        {"tag": "EquitySecuritiesFvNi", "forms": ("10-Q",)},
                    ),
                },
            ),
        },
        "note": "FY2026(FYE 2026-01-25)以降、非上場投資先の上場に伴う資産再分類で"
                "short_term_investmentsが単一タグで捕捉不可（型C）。cross_filing_tags"
                "でDebtSecuritiesCurrent+EquitySecuritiesFvNiを合算する（各期BS原本の"
                "流動Marketable securitiesと一致確認済み）。annual FY2026は後続10-Qの"
                "比較年度値を参照するクロスfiling参照。詳細はBACKLOG_DONE.md "
                "[[NVDA-STI-TAG-UNIDENTIFIED-1]]・[[BBAI-RDW-RUNWAY-VERIFICATION-1]]参照。",
    },
    # [[FCF-CONVRATE-LOWER-DIVERGENCE-1]]ボトムアップFCF移行（2026-09-17）:
    # LYFTはPaymentsToAcquirePropertyPlantAndEquipment等CAPITAL_EXPENDITURE
    # 標準候補4タグを一度も申告していない（company_facts.json確認済み）。
    # 実際に計上しているのはCapitalizedComputerSoftwareAdditions（ソフト
    # ウェア資産化費用、$4〜12M/年、2022-2024年）のみ。他21銘柄
    # （AAPL/AMZN/APP等）も同タグを申告しているため、グローバル候補
    # リストへ追加するとAPP（既存のticker_restrictions "exclude": ["CapEx"]
    # で意図的にCapEx除外中）等の既存挙動を破壊するリスクがある。
    # LYFT限定のcapex_conceptオーバーライドとする（sti_concept等と同型の
    # 設計判断。parser.py側のみ対応、quarterly.py側のTTM抽出は本タスクの
    # スコープ外＝未対応のまま）。注: 金額自体は小さく（LYFTのOCFは
    # $1.0B超）、この修正単体でLYFTのraw_fcfへの影響は軽微。
    "LYFT": {
        "capex_concept": "CapitalizedComputerSoftwareAdditions",
        "note": "標準CapExタグ4種を一度も申告せず、ソフトウェア資産化費用"
                "（CapitalizedComputerSoftwareAdditions）のみ計上。LYFT限定"
                "オーバーライドとし、グローバル候補リストへは追加しない"
                "（21銘柄が同タグを申告しており、うちAPPは既存のexclude"
                "設定と衝突するため）。",
    },
}

# 会計年度タイプ（将来対応用）
FISCAL_YEAR_TYPE: dict[str, str] = {
    # "AMZN": "53week",  # 53週会計年度（AMZNオンボード時に有効化）
}

# XBRL概念マッピング（field_name → (concept, unit)）
# CapEx・FinanceLeasePmts・SBC・GrossProfit・NetIncome・Cash・RD・Buyback・OCFの
# primaryタグはtag_definitions.pyのTAG_CANDIDATES（各タプルの先頭）から取得する
# （LLY-CAPEX-STALE-1 Phase 2a・quarterly.py/parser.pyのタグリスト統合）。
FIELD_CONCEPTS: dict[str, tuple[str, str]] = {
    "OCF":              (TAG_CANDIDATES["OPERATING_CASH_FLOW"][0], "USD"),
    "ICF":              ("NetCashProvidedByUsedInInvestingActivities", "USD"),
    "CFF":              ("NetCashProvidedByUsedInFinancingActivities", "USD"),
    "CapEx":            (TAG_CANDIDATES["CAPITAL_EXPENDITURE"][0], "USD"),
    "FinanceLeasePmts": (TAG_CANDIDATES["FINANCE_LEASE_PAYMENTS"][0], "USD"),
    "SBC":              (TAG_CANDIDATES["STOCK_BASED_COMPENSATION"][0], "USD"),
    "DA":               ("DepreciationDepletionAndAmortization", "USD"),
    "Revenue":          ("Revenues", "USD"),
    "GrossProfit":      (TAG_CANDIDATES["GROSS_PROFIT"][0], "USD"),
    "OperatingIncome":  ("OperatingIncomeLoss", "USD"),
    # [[NET-INCOME-NCI-PARENT-ATTRIBUTION-1]]: 3系統共通のNET_INCOME_CANDIDATES
    # （連結系タグをNCI控除後の派生概念に差し替えたもの）を使う
    "NetIncome":        (NET_INCOME_CANDIDATES[0], "USD"),
    "Cash":             (TAG_CANDIDATES["CASH_AND_EQUIVALENTS"][0], "USD"),
    # [[SCHEMA-NORMALIZED-ISSUES-1]]①（2026-09-23、罠防止コメント）:
    # このSTDebtは単一タグ・フォールバックなしのためparser.py側の
    # short_term_debt（9タグ候補＋フォールバック）に比べ網羅性が著しく
    # 劣化している（実測: AAPL 33/51件・XOM 51/51件・V 30/51件が
    # normalized側で0件）。実消費者ゼロを確認済みだが、新規にこの
    # フィールドを参照するコードを書く場合はdata/annual_*.json層
    # （parser.py::XBRL_MAPPING）のshort_term_debtを使うこと。
    "STDebt":           ("ShortTermBorrowings", "USD"),
    # [[SCHEMA-NORMALIZED-ISSUES-1]]③（2026-09-23）: parser.py::XBRL_MAPPING
    # と優先順序を統一。LongTermDebtNoncurrentを優先することで
    # LongTermDebtCurrentとの二重計上リスクを避ける（BUG-NETDEBT-2の
    # 設計意図と同一、251-252行目参照）。
    "LTDebt":           ("LongTermDebtNoncurrent", "USD"),
    "DeferredRevenue":  ("DeferredRevenue", "USD"),
    "Equity":           ("StockholdersEquity", "USD"),
    "Assets":           ("Assets", "USD"),
    # [[SCHEMA-NORMALIZED-ISSUES-1]]④（2026-09-23、罠防止コメント）:
    # このSharesBasicはBS項目（期末時点の発行済株式数）だが、parser.py
    # 側のshares_basicはPL項目（WeightedAverageNumberOfShares
    # OutstandingBasic、期中加重平均株式数）であり、単なる優先順序差
    # ではなく意味的に異なる財務概念。実消費者ゼロを確認済みだが、
    # 新規にこのフィールドを参照するコードを書く場合はdata/annual_*.json
    # 層（parser.py::XBRL_MAPPING）のshares_basicを使うこと。
    "SharesBasic":      ("CommonStockSharesOutstanding", "shares"),
    "SharesDiluted":    ("WeightedAverageNumberOfDilutedSharesOutstanding", "shares"),
    # R&D / 販売・マーケティング費（RICE計算用）
    "RD":               (TAG_CANDIDATES["RESEARCH_AND_DEVELOPMENT"][0], "USD"),
    # [[SCHEMA-NORMALIZED-ISSUES-1]]②（2026-09-23、罠防止コメント）:
    # このSMは単一フィールドだが、S&M単体タグ未申告銘柄
    # （JOBY/NVDA/CIX/ELF/KO等）では_FIELD_FALLBACKS["SM"]経由で
    # SGA総額へ静かにフォールバックする（334-341行目参照）。同じSM値が
    # 銘柄によって「純S&M費用」と「SGA総額」という異なる意味を持ちうるが
    # フィールド名からは判別できない。parser.py側はselling_and_marketing
    # とselling_general_and_administrativeを別フィールドとして両方保持
    # している。実消費者ゼロを確認済みだが、新規にこのフィールドを
    # 参照するコードを書く場合はdata/annual_*.json層のこの2フィールドを
    # 使うこと。
    "SM":               ("SellingAndMarketingExpense", "USD"),
    # RPO: 残存履行義務（SaaS/クラウド企業向けストック値）
    "RPO":              ("RevenueRemainingPerformanceObligation", "USD"),
    # GrossProfit逆算用（内部フィールド）
    "_COGS":            ("CostOfRevenue", "USD"),
    # BS流動項目（シガーバット検出用）
    "CurrentAssets":      ("AssetsCurrent", "USD"),
    "CurrentLiabilities": ("LiabilitiesCurrent", "USD"),
    # 自社株買い（キャッシュトラップ検出用）
    "Buyback":            (TAG_CANDIDATES["BUYBACK"][0], "USD"),
}

# _COGS フォールバック概念（CostOfRevenue未申告の場合）
_COGS_FALLBACKS = (
    "CostOfGoodsAndServicesSold",
    "CostOfGoodsSold",
    "CostOfServices",
    "CostOfGoodsSoldExcludingDepreciationDepletionAndAmortization",
)

# Revenue フォールバック概念（優先順位順・メインタグとマージして最多エントリを採用）
_REVENUE_FALLBACKS = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
    "TotalRevenue",
    "RevenuesNetOfInterestExpense",  # 銀行・金融系（SOFI等）
)

# RD・SM・CapEx・SBC フォールバック概念（primaryと重複しない候補のみ）
# CapEx・SBC・GrossProfit・NetIncome・Cash・RDはtag_definitions.pyのTAG_CANDIDATES
# （primary除く残り）から取得する（LLY-CAPEX-STALE-1 Phase 2a）。
# LTDebt・SM・RPOはparser.py側と優先順位・候補集合が構造的に異なるため
# 統合対象外（tag_definitions.pyのdocstring・BACKLOG [TAG-DEFS-UNIFY-1] 参照）。
_FIELD_FALLBACKS: dict[str, tuple[str, ...]] = {
    # AVGO: NetIncomeLossの四半期データが2019以前で途絶えているため ProfitLoss を使用
    # BKNG/AVAV: NetIncomeLoss自体が未申告のため以下をフォールバック
    # （ProfitLoss等はNCI控除後の派生概念、[[NET-INCOME-NCI-PARENT-ATTRIBUTION-1]]）
    "NetIncome": NET_INCOME_CANDIDATES[1:],
    "CapEx": TAG_CANDIDATES["CAPITAL_EXPENDITURE"][1:],
    "RD": TAG_CANDIDATES["RESEARCH_AND_DEVELOPMENT"][1:],
    "SM": (
        "MarketingAndAdvertisingExpense",
        "MarketingExpense",
        "AdvertisingExpense",
        # SGA全体をフォールバック（SM単独タグ未申告の銘柄向け: JOBY/NVDA/CIX/ELF/KO等）
        "SellingGeneralAndAdministrativeExpense",
        "GeneralAndAdministrativeExpense",
    ),
    # RCAT等: CashAndCashEquivalentsAtCarryingValue が少ない場合
    "Cash": TAG_CANDIDATES["CASH_AND_EQUIVALENTS"][1:],
    # CEG等: ShareBasedCompensation未申告の場合
    "SBC": TAG_CANDIDATES["STOCK_BASED_COMPENSATION"][1:],
    # AMZN等: GrossProfitタグがある場合（normalizer._calc_gross_profitで逆算済みの場合は不要）
    "GrossProfit": TAG_CANDIDATES["GROSS_PROFIT"][1:],
    "LTDebt": (
        # [[SCHEMA-NORMALIZED-ISSUES-1]]③（2026-09-23）: primaryを
        # LongTermDebtNoncurrentに変更したため、fallbackはLongTermDebt
        # （current+non-current合計値のタグ）に変更。CEG等、
        # LongTermDebtNoncurrentを申告せずLongTermDebt(total)のみ
        # 申告する銘柄向け。
        "LongTermDebt",
    ),
    "RPO": (
        "RemainingPerformanceObligation",
        "ContractWithCustomerLiabilityNoncurrent",
        "DeferredRevenueNoncurrent",
    ),
    # [[SCHEMA-NORMALIZED-ISSUES-1]]⑥（2026-09-16）: DAはフォールバック
    # 候補が一切なく、primaryタグ（DepreciationDepletionAndAmortization）
    # を報告しない銘柄（LMT等）でnormalized/側のDAフィールドが完全に
    # 空になっていた。parser.py側の4候補（tag_definitions.py同様の
    # 優先順位）のうち、現在のprimary以外の残り3つをフォールバックとして
    # 追加する。
    "DA": (
        "DepreciationAndAmortization",
        "Depreciation",
        "AmortizationOfIntangibleAssets",
    ),
}

# FinanceLeasePmts・Buyback・OCFはprimaryのみ（fallbackなし）だったが、
# parser.py側にFinanceLeasePmtsの追加候補があったため統合する（Phase 2a）。
_FIELD_FALLBACKS["FinanceLeasePmts"] = TAG_CANDIDATES["FINANCE_LEASE_PAYMENTS"][1:]

# 取得期間
_QUARTERLY_YEARS = 5
_ANNUAL_YEARS = 6


def build_raw_table(ticker: str, company_facts: dict) -> dict:
    """
    company_facts から全フィールドの四半期Raw Tableを抽出。

    [[SCHEMA-NORMALIZED-ISSUES-1]]⑤（2026-09-23、罠防止コメント）:
    出力先ファイル名は`normalized/{TICKER}_quarterly_normalized.json`
    と"quarterly"を冠しているが、各フィールドのエントリには
    `is_annual: true`の年次データも同一リストに混在保持している
    （company_factsの10-K由来エントリをそのまま格納するため）。
    ファイル名から「四半期データのみ」と誤推測しないこと。

    戻り値構造:
    {
      "ticker": "NVDA",
      "generated_at": "2026-05-07T...",
      "fields": {
        "OCF": [
          {
            "end": "2024-10-27",
            "start": "2024-07-29",
            "val": 7000000000,
            "accn": "0001045810-24-...",
            "fp": "Q3",
            "fy": 2025,
            "form": "10-Q",
            "filed": "2024-11-20",
            "period_days": 90,
            "is_ytd": False,
            "is_annual": False,
          },
          ...
        ],
        "Revenue": [...],
        ...
      }
    }
    """
    ticker = ticker.upper()
    restrictions = TICKER_RESTRICTIONS.get(ticker, {})
    excluded = set(restrictions.get("exclude", []))
    # [[NET-INCOME-NCI-PARENT-ATTRIBUTION-1]]: NetIncome用の派生概念を追加
    company_facts = with_derived_net_income(company_facts)

    fields: dict[str, list] = {}

    for field_name, (concept, unit) in FIELD_CONCEPTS.items():
        if field_name in excluded:
            fields[field_name] = []
            continue

        entries = _get_field_units(company_facts, concept, unit)

        if field_name == "Revenue":
            # 銘柄固有のRevenue概念が指定されている場合はそれを優先使用
            ticker_revenue_concept = restrictions.get("revenue_concept")
            if ticker_revenue_concept:
                override_entries = _get_field_units(company_facts, ticker_revenue_concept, unit)
                if override_entries:
                    entries = _process_entries(override_entries)
                    # 規約③（出所メタデータ）: ticker_restrictionsによる明示オーバーライドは
                    # 「なぜこの値か」が自明でないため_provenanceに記録する
                    entries = [
                        {**e, "_provenance": {"source_tag": ticker_revenue_concept}}
                        for e in entries
                    ]
                    logger.debug("[%s] Revenue override: %s (%d entries)",
                                 ticker, ticker_revenue_concept, len(entries))
                    fields[field_name] = entries
                    continue
            # Revenueは企業によってタグが途中で変わる・複数タグ併用のため
            # メインタグ＋全フォールバックをマージして最多エントリを確保する
            # 注意: 同一accn内にYTDとstandalone両方が含まれる場合があるため
            # accnではなく(end, start, val)の組み合わせで重複排除する
            all_entries = list(entries)
            seen_key = {(e.get("end"), e.get("start"), e.get("val")) for e in entries}
            for fallback_concept in _REVENUE_FALLBACKS:
                fb = _get_field_units(company_facts, fallback_concept, unit)
                for e in fb:
                    key = (e.get("end"), e.get("start"), e.get("val"))
                    if key not in seen_key:
                        all_entries.append(e)
                        seen_key.add(key)
            entries = all_entries
            if entries:
                logger.debug("[%s] Revenue merged: %d entries", ticker, len(entries))

        if field_name == "LTDebt":
            # 銘柄固有のLTDebt概念が指定されている場合はそれを優先使用
            # SOFI-DATA-1: 銀行免許取得後（2022年以降）LongTermDebt/
            # LongTermDebtNoncurrentの申告自体を停止し、短期+長期合算タグに
            # 移行したケース向け。standard候補群への一般フォールバック追加は
            # AVGO/VZ等の無関係な既存データにも波及するため見送り、
            # ticker_restrictionsによる明示的な銘柄限定オーバーライドとする。
            ticker_ltdebt_concept = restrictions.get("ltdebt_concept")
            if ticker_ltdebt_concept:
                override_entries = _get_field_units(company_facts, ticker_ltdebt_concept, unit)
                if override_entries:
                    entries = _process_entries(override_entries)
                    # 規約③（出所メタデータ）: SOFI-DATA-1のような明示オーバーライドは
                    # 「なぜこの値か」が自明でないため_provenanceに記録する
                    entries = [
                        {**e, "_provenance": {"source_tag": ticker_ltdebt_concept}}
                        for e in entries
                    ]
                    logger.debug("[%s] LTDebt override: %s (%d entries)",
                                 ticker, ticker_ltdebt_concept, len(entries))
                    fields[field_name] = entries
                    continue

        if field_name == "_COGS":
            if not entries:
                # CostOfRevenue未申告の場合、代替COGSタグを試みる
                for fallback_concept in _COGS_FALLBACKS:
                    fb = _get_field_units(company_facts, fallback_concept, unit)
                    if fb:
                        entries = fb
                        logger.debug("[%s] _COGS fallback: %s", ticker, fallback_concept)
                        break

            # 銘柄固有のcogs_concept（cost_of_revenue相当タグ）が指定されて
            # いる場合は追加候補としてマージする（[[LAYER3-COGS-CANDIDATE-
            # TAG-EXPANSION-1]]、2026-09-06）。OtherCostAndExpenseOperating・
            # CostDirectMaterialは名称が汎用的で他銘柄（AMZN/CAKE/CAT/FCX/
            # KO/LMT/META/RCAT/LYFT等）でCOGSとは全く異なる少額の会計項目
            # に使われていることを全105銘柄相当のcompany_facts.json横断
            # チェックで確認済みのため、グローバル候補リスト（_COGS_
            # FALLBACKS）へは追加せず、ticker限定オーバーライドとする
            # （V/KLAC/TER等のsti_conceptと同型の設計判断）。
            # 「置換」ではなく既存entriesへの「マージ」とする理由: CPRTは
            # FY2016-2019をCostOfGoodsSold等の既存候補タグが本人データ
            # （is_own_data=True）として既に正しく提供しており、cogs_concept
            # （CostDirectMaterial）はFY2018以降のみ・かつ一部年度は後年
            # 提出分の比較年度データ（is_own_data=False）しか無い。全置換
            # するとFY2016-2019の高品質な既存データを低品質な比較年度データ
            # で潰す回帰が生じることをQA中に発見し、マージ方式に修正した。
            # 年度ごとの最良エントリ選択（is_own_data優先）は下流のロジック
            # （SECParser側）に委ねる。
            ticker_cogs_concept = restrictions.get("cogs_concept")
            if ticker_cogs_concept:
                override_entries = _get_field_units(company_facts, ticker_cogs_concept, unit)
                if override_entries:
                    seen_key = {(e.get("end"), e.get("start"), e.get("val")) for e in entries}
                    merged = list(entries)
                    added = 0
                    for e in override_entries:
                        key = (e.get("end"), e.get("start"), e.get("val"))
                        if key not in seen_key:
                            merged.append(e)
                            seen_key.add(key)
                            added += 1
                    entries = merged
                    logger.debug("[%s] _COGS merged with override: %s (+%d entries)",
                                 ticker, ticker_cogs_concept, added)

        processed = _process_entries(entries)

        # 汎用フォールバック:
        # ① タグ欠如またはエントリなし → 全フィールド対象
        # ② Q件数が少ない（4件未満）場合もフォールバックを試みてより多い方を採用
        #    ※ SMはメインタグで少数取れていてもSGA等に乗り換えると意味が変わるため除外
        _FALLBACK_MIN = 4
        # NetIncome: quarterly数が少ない場合もフォールバックを試みる
        #   AVGO等: NetIncomeLossの四半期データが5年窓外で q_count=0 になる場合に
        #   ProfitLossへのフォールバックが必要（annualはあるため not processed=False）
        _FALLBACK_MIN_FIELDS = {"Cash", "SBC", "CapEx", "GrossProfit", "NetIncome", "LTDebt"}
        if field_name in _FIELD_FALLBACKS:
            q_count = sum(1 for e in processed if not e.get("is_annual"))
            use_min = field_name in _FALLBACK_MIN_FIELDS and q_count < _FALLBACK_MIN
            # [[SCHEMA-NORMALIZED-ISSUES-1]]③派生（2026-09-23、FLYW型）:
            # primaryの件数がuse_minを満たしても、企業がそのタグの申告自体を
            # 停止し（陳腐化）フォールバック候補の方に新しいデータがある場合、
            # 件数条件だけでは検知できない（LLY-CAPEX-STALE-1と同根だが、
            # そちらは「件数不足」経由でしか_select_best_candidate()を
            # 起動しない設計だったため、件数が足りるケースを見逃していた）。
            # primaryの最新end日よりフォールバック候補の最新end日が新しい場合も
            # 起動対象に追加する。
            stale = False
            if field_name in _FALLBACK_MIN_FIELDS and processed:
                primary_latest_end = max((e["end"] for e in processed), default="")
                for _fb_concept in _FIELD_FALLBACKS[field_name]:
                    _fb_raw = _get_field_units(company_facts, _fb_concept, unit)
                    if _fb_raw and max(e["end"] for e in _fb_raw) > primary_latest_end:
                        stale = True
                        break
            if not processed or use_min or stale:
                processed, _source_tag = _select_best_candidate(
                    company_facts, unit, concept, processed,
                    _FIELD_FALLBACKS[field_name], _FALLBACK_MIN,
                )
                if _source_tag:
                    # 規約③（出所メタデータ）: フォールバック候補が採用された場合のみ
                    # 記録する（primary採用時は想定通りのためnoise回避で付与しない）
                    processed = [
                        {**e, "_provenance": {"source_tag": _source_tag}}
                        for e in processed
                    ]
                logger.debug("[%s] %s fallback evaluated (best candidate selected)", ticker, field_name)

        fields[field_name] = processed

    logger.info("[%s] raw table built: %d fields", ticker, len(fields))
    return {
        "ticker": ticker,
        "generated_at": datetime.now().isoformat(),
        "fields": fields,
    }


def _get_field_units(company_facts: dict, concept: str, unit: str = "USD") -> list:
    """company_facts.facts.us-gaap.{concept}.units.{unit} を安全に取得"""
    try:
        return company_facts["facts"]["us-gaap"][concept]["units"][unit]
    except (KeyError, TypeError):
        return []


def _select_best_candidate(
    company_facts: dict,
    unit: str,
    primary_concept: str,
    primary_processed: list,
    fallback_concepts: tuple[str, ...],
    min_count: int,
) -> tuple[list, str | None]:
    """primary概念＋フォールバック候補群の中から採用する候補を選ぶ。

    LLY-CAPEX-STALE-1 Phase 2a: 「候補タグ群の中で最初に条件（最小件数）を
    満たしたものを採用して打ち切る」方式は、タグがサイレントに切り替わった
    銘柄（旧タグは件数を満たすが更新が止まっている）を検知できない
    （LLYのCapEx: 旧タグPaymentsToAcquireProductiveAssetsは4四半期分あるが
    2022-09-30で申告停止しており、新タグへの切替後データを一切拾えなかった）。

    最小件数(min_count)を満たす候補が複数ある場合、「最初に見つかったもの」
    ではなく「四半期の最新end日が最も新しいもの」を採用する。最小件数を
    満たす候補が皆無の場合のみ、従来どおり最多件数の候補を採用する。
    同着（end日・件数が同一）の場合は優先順位（primary→fallback_concepts順）を維持する。

    戻り値: (選定されたエントリリスト, 採用概念名)。採用概念名はprimary_conceptが
    選ばれた場合はNone（想定通りの経路のためprovenance記録不要）、フォールバック
    概念が選ばれた場合はその概念名（QUALITY-GATES-EPIC-1 Phase 3a: 呼び出し元で
    EntryProvenance.source_tagとして記録するため）。
    """
    def _stats(processed: list) -> tuple[int, str]:
        q_count = sum(1 for e in processed if not e.get("is_annual"))
        latest_end = max((e["end"] for e in processed), default="")
        return q_count, latest_end

    candidates: list[tuple[list, int, str, str]] = []
    q0, end0 = _stats(primary_processed)
    candidates.append((primary_processed, q0, end0, primary_concept))
    for concept in fallback_concepts:
        fb_entries = _get_field_units(company_facts, concept, unit)
        if not fb_entries:
            continue
        fb_processed = _process_entries(fb_entries)
        q, end = _stats(fb_processed)
        candidates.append((fb_processed, q, end, concept))

    qualified = [c for c in candidates if c[1] >= min_count]
    if qualified:
        # 最小件数を満たす候補の中から最新end日優先（同着は優先順位＝リスト順で先勝ち）
        best = max(qualified, key=lambda c: (c[2], -candidates.index(c)))
    else:
        # 最小件数を満たす候補が皆無 → 従来どおり最多件数優先（同点はend日→優先順位）
        best = max(candidates, key=lambda c: (c[1], c[2], -candidates.index(c)))
    winning_concept = best[3]
    source_tag = winning_concept if winning_concept != primary_concept else None
    return best[0], source_tag


def _select_best_filing(filings: list, end_date: str) -> dict | None:
    """同一期間に複数filingがある場合、最新filed優先で選択"""
    candidates = [f for f in filings if f.get("end") == end_date]
    if not candidates:
        return None
    return select_latest_filed(candidates)


def _classify_period(start: str, end: str, fp: str, form: str = "") -> dict:
    """
    期間を分類する。

    戻り値: {"period_days": int, "is_ytd": bool, "is_annual": bool}
    """
    try:
        s = date.fromisoformat(start)
        e = date.fromisoformat(end)
        days = (e - s).days
    except (ValueError, TypeError):
        days = 0

    # is_annualはform=10-K/10-K-A限定（PARSER-ENTG-COMPYEAR-1の教訓）:
    # 10-Qには比較用に365日近いdurationのcontextRefが混入することがあり、
    # form制限なしだとそれが「最新filed優先」ロジックで正規の10-K年次値を
    # 上書きしてしまう（ENTG FY2022でQ4合成値が$0になる不具合が発生した）
    #
    # fp=='FY'のみでの判定は不十分（XBRL-TAG-KLAC-1の教訓）:
    # SEC XBRL APIのfpは「filing自体の期間種別」を指すため、10-K内に
    # 埋め込まれた四半期duration（例: period_days=91の比較用開示）にも
    # 一律fp='FY'が付与される。fp=='FY'単独で年次判定すると、KLACの
    # FY2021 10-K内の四半期GrossProfit開示（period_days=89〜91）が
    # 誤って年次データとして扱われ、その後の年度に本来の年次値が
    # 一度も上書きされず4四半期分の古いデータが残存し続けた。
    # fp=='FY'採用時もdays>130（4半期の最短妥当日数）を必須とする。
    #
    # 上限も必須（[[QUARTERLY-CLASSIFY-PERIOD-NO-UPPER-BOUND-1]]対応、
    # 2026-08-30）: 従来は下限のみでdays>300なら無条件に年次扱いして
    # いたため、10-K内に埋め込まれた中間的な期間長の比較開示
    # （6〜9ヶ月累計等、DELLの2023-08-04期181日等で実例確認）や、
    # 一部タグの多年度累計開示（数年〜十年規模のdurationも存在）が
    # 誤って年次データとして混入していた。全105銘柄・company_facts.json
    # 全概念を対象にis_annual=True判定済みエントリのperiod_days分布を
    # 実測した結果、真の年次エントリは349〜372日に集中し（大半は
    # 363〜370日、52/53週決算の揺れを含む）、336〜362日の範囲には
    # 1件も存在しない明確な空白があることを確認した。この空白の中間値
    # 340日を下限、真の年次エントリの最大値372日に安全マージンを
    # 加えた400日を上限として採用する。この変更は既存の年次判定を
    # 狭める方向にのみ働く（130<days<=400のいずれの分岐も340〜400の
    # 部分集合に置き換わるため、新たに年次と判定されるようになる
    # エントリは発生しない）。
    is_annual = form in ("10-K", "10-K/A") and 340 <= days <= 400
    # is_annual=False かつ days>130 → YTD（is_annual側の範囲拡張に伴い、
    # 従来is_annual=Trueだった130<days<340・400<daysの区間もここに
    # 含まれるようになった。誤って年次扱いされていたエントリがYTD
    # 候補として_ytd_to_quarterly()等の既存の解決ロジックに正しく
    # 委ねられるようになる）
    is_ytd = (not is_annual) and (days > 130)

    return {
        "period_days": days,
        "is_ytd": is_ytd,
        "is_annual": is_annual,
    }


def _process_entries(raw_entries: list) -> list:
    """
    生エントリをフィルタ・分類・重複排除・ソートして返す。

    四半期: (start, end) の期間単位でグルーピングし、同一期間内は最新filed優先。
    ※ end日付だけでグルーピングすると、同一end・異なるstart（例: Q3単独 vs
      Q1-Q3累計）の別期間エントリを誤って「重複」扱いし、一方（主にYTD）を
      完全に破棄してしまう（BUG-CON-YTD-1）。YTDは欠落四半期の逆算に使える
      有用な情報のため、start違いは別期間として保持する。
    年次: 直近6年、四半期: 直近5年。
    """
    today = date.today()
    cutoff_q = (today - timedelta(days=_QUARTERLY_YEARS * 365)).isoformat()
    cutoff_a = (today - timedelta(days=_ANNUAL_YEARS * 365)).isoformat()

    # (start, end) → 候補リスト（quarterly）、end → 候補リスト（annual）
    quarterly_by_period: dict[tuple, list] = defaultdict(list)
    annual_by_end: dict[str, list] = defaultdict(list)

    for entry in raw_entries:
        form = entry.get("form", "")
        if form not in ("10-Q", "10-Q/A", "10-K", "10-K/A"):
            continue

        end = entry.get("end", "")
        start = entry.get("start", "")
        fp = entry.get("fp", "")
        val = entry.get("val")

        if val is None or not end:
            continue

        period_info = _classify_period(start, end, fp, form)

        # 10-Q/10-Q-A由来でduration>300日のエントリは比較用contextRefの混入
        # （PARSER-ENTG-COMPYEAR-1）とみなし、YTD四半期扱いにもせず除外する
        # （実際の10-Q YTDは最大9ヶ月≒270日程度のため、300日超は正規の
        #   四半期・YTD概念に該当しない）
        if form in ("10-Q", "10-Q/A") and period_info["period_days"] > 300:
            continue

        enriched = {
            "end": end,
            "start": start,
            "val": val,
            "accn": entry.get("accn", ""),
            "fp": fp,
            "fy": entry.get("fy"),
            "form": form,
            "filed": entry.get("filed", ""),
            "period_days": period_info["period_days"],
            "is_ytd": period_info["is_ytd"],
            "is_annual": period_info["is_annual"],
        }

        if period_info["is_annual"]:
            if end >= cutoff_a:
                annual_by_end[end].append(enriched)
        else:
            if end >= cutoff_q:
                quarterly_by_period[(start, end)].append(enriched)

    result: list = []

    # 四半期: 同一(start, end)内は最新filed優先。異なるstartは別期間として全保持。
    for period, candidates in quarterly_by_period.items():
        result.append(select_latest_filed(candidates))

    # 年次: 最新filed
    for end_date, candidates in annual_by_end.items():
        result.append(select_latest_filed(candidates))

    result.sort(key=lambda x: x["end"])
    return result



# ---------------------------------------------------------------------------
# Revenue品質チェック
# ---------------------------------------------------------------------------

def check_revenue_quality(ticker: str, normalized: dict) -> dict:
    """
    normalized JSONのRevenueフィールドに対して品質チェックを行う。

    チェック項目:
      1. 四半期件数が少ない（8件未満）
      2. 最新Q 前Q比の異常値
         - seasonal_q1_jump=True の銘柄はQ1（3月末）の前Q比チェックをスキップ
         - それ以外: |QoQ| > 60% → ISSUE, > 35% → WARN
      3. 最新yoyトレンドの急変（±20pt超）
      4. 四半期合計とFY年次の整合性（乖離 > 5% → ISSUE）
      5. 金融・保険系フラグ（seasonal_q1_jump or revenue_concept上書きあり）
      6. Revenue負値

    戻り値:
    {
      "ticker": str,
      "status": "OK" | "WARN" | "ISSUE",
      "issues": [str, ...],   # 重大問題
      "warnings": [str, ...], # 注意事項
      "latest_rev_yoy": float | None,
      "latest_q_end": str | None,
    }
    """
    ticker = ticker.upper()
    restrictions = TICKER_RESTRICTIONS.get(ticker, {})
    seasonal_q1 = restrictions.get("seasonal_q1_jump", False)

    entries = normalized.get("fields", {}).get("Revenue", [])
    q_only = sorted(
        [(e["end"], e["val"]) for e in entries if not e.get("is_annual")],
        key=lambda x: x[0],
    )
    a_only = sorted(
        [(e["end"], e["val"]) for e in entries if e.get("is_annual")],
        key=lambda x: x[0],
    )

    issues: list[str] = []
    warnings: list[str] = []
    latest_yoy: float | None = None
    latest_q_end: str | None = q_only[-1][0] if q_only else None

    if not q_only:
        issues.append("四半期Revenueエントリなし")
        return _build_result(ticker, issues, warnings, latest_yoy, latest_q_end)

    # --- チェック1: 件数 ---
    if len(q_only) < 8:
        warnings.append(f"四半期データが少ない ({len(q_only)}件 < 8件)")

    # --- チェック2: 最新Q 前Q比 ---
    if len(q_only) >= 2:
        latest_end, latest_val = q_only[-1]
        prev_end, prev_val = q_only[-2]
        qoq = (latest_val - prev_val) / abs(prev_val) * 100
        is_q1 = latest_end[5:7] == "03"  # 3月末 = Q1

        if seasonal_q1 and is_q1:
            # Q1季節性銘柄はQ1ジャンプをスキップし、過去Q1比で異常検出
            q1_entries = [(e, v) for e, v in q_only if e[5:7] == "03"]
            if len(q1_entries) >= 3:
                past_q1_qoqs = []
                for i in range(1, len(q1_entries) - 1):
                    # 直前Q4を探して比較
                    q1_end = q1_entries[i][0]
                    q4_candidates = [(e, v) for e, v in q_only
                                     if e < q1_end and e[5:7] == "12"]
                    if q4_candidates:
                        q4_end, q4_val = q4_candidates[-1]
                        past_q1_qoqs.append(
                            (q1_entries[i][1] - q4_val) / abs(q4_val) * 100
                        )
                if past_q1_qoqs:
                    avg_q1_jump = sum(past_q1_qoqs) / len(past_q1_qoqs)
                    # Q4を探して現在の実績と比較
                    q4_for_latest = [(e, v) for e, v in q_only
                                     if e < latest_end and e[5:7] == "12"]
                    if q4_for_latest:
                        _, q4_val = q4_for_latest[-1]
                        latest_q1_jump = (latest_val - q4_val) / abs(q4_val) * 100
                        diff = latest_q1_jump - avg_q1_jump
                        if diff > 30:
                            warnings.append(
                                f"Q1季節性ジャンプが過去平均を大幅超過: "
                                f"今回{latest_q1_jump:.1f}% vs 過去平均{avg_q1_jump:.1f}% "
                                f"(+{diff:.1f}pt)"
                            )
            # Q1スキップのメモをWARNに残す
            warnings.append(
                f"seasonal_q1_jump: 前Q比{qoq:+.1f}%はQ1季節性として評価スキップ"
            )
        else:
            if abs(qoq) > 60:
                issues.append(
                    f"最新Q 前Q比異常: {qoq:+.1f}% ({prev_end}→{latest_end})"
                )
            elif abs(qoq) > 35:
                warnings.append(
                    f"最新Q 前Q比大きめ: {qoq:+.1f}% ({prev_end}→{latest_end}) 季節性か要確認"
                )

    # --- チェック3: yoyトレンド急変 ---
    if len(q_only) >= 5:
        yoys = []
        for i in range(4, len(q_only)):
            e, v = q_only[i]
            pe, pv = q_only[i - 4]
            yoys.append((e, (v - pv) / abs(pv) * 100))
        latest_yoy = yoys[-1][1]
        if len(yoys) >= 3:
            recent = [y for _, y in yoys[-3:]]
            avg_prev = sum(recent[:-1]) / len(recent[:-1])
            jump = recent[-1] - avg_prev
            if jump > 20:
                warnings.append(
                    f"最新yoy急加速: {avg_prev:.1f}%→{recent[-1]:.1f}% (+{jump:.1f}pt)"
                )
            elif jump < -20:
                warnings.append(
                    f"最新yoy急減速: {avg_prev:.1f}%→{recent[-1]:.1f}% ({jump:.1f}pt)"
                )

    # --- チェック4: 四半期合計 vs FY年次 整合性 ---
    # 暦年ラベル(a_end[:4])での四半期グルーピングは非12月決算企業（KLAC/LRCX等）で
    # 誤検知する（CHECK-QREV-FYE-1）。年次end日を起点にtrailing 12ヶ月窓で
    # 該当4四半期を抽出する会計年度ベースのグルーピングに変更した
    # （ARCH-DATA-1残課題①でquarters_in_trailing_window()に一本化）。
    for a_end, a_val in a_only[-3:]:
        try:
            q_in_fy = quarters_in_trailing_window(q_only, a_end)
        except ValueError:
            continue
        if len(q_in_fy) == 4:
            q_total = sum(q_in_fy)
            gap_pct = abs(q_total - a_val) / abs(a_val) * 100 if a_val else 0
            if gap_pct > 5:
                issues.append(
                    f"FY(期末{a_end}) 年次vs四半期合計 乖離{gap_pct:.1f}%: "
                    f"annual={a_val/1e6:.0f}M, Q合計={q_total/1e6:.0f}M"
                )

    # --- チェック5: 金融・保険系フラグ ---
    if restrictions.get("revenue_concept") or restrictions.get("seasonal_q1_jump"):
        kind = "銀行/フィンテック" if restrictions.get("revenue_concept") else "保険/季節性"
        warnings.append(
            f"特殊銘柄({kind}): TICKER_RESTRICTIONSで管理済み"
        )

    # --- チェック6: Revenue負値 ---
    neg = [(e, v) for e, v in q_only if v < 0]
    if neg:
        issues.append(
            f"Revenue負値: {[(e, round(v/1e6, 1)) for e, v in neg]}"
        )

    return _build_result(ticker, issues, warnings, latest_yoy, latest_q_end)


def _build_result(
    ticker: str,
    issues: list[str],
    warnings: list[str],
    latest_yoy: float | None,
    latest_q_end: str | None,
) -> dict:
    status = "ISSUE" if issues else ("WARN" if warnings else "OK")
    result = {
        "ticker": ticker,
        "status": status,
        "issues": issues,
        "warnings": warnings,
        "latest_rev_yoy": round(latest_yoy, 2) if latest_yoy is not None else None,
        "latest_q_end": latest_q_end,
    }
    logger.info(
        "[%s] revenue quality: %s  issues=%d warnings=%d  yoy=%s",
        ticker,
        status,
        len(issues),
        len(warnings),
        f"{latest_yoy:.1f}%" if latest_yoy is not None else "N/A",
    )
    return result
