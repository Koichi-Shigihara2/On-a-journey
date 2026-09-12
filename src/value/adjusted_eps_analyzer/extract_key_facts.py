"""
SEC EDGARから企業の財務データを抽出するモジュール（会計年度対応・四半期分類改善版・複数期間対応）
- CIKマップファイルから銘柄のCIKを取得
- SECのCompany Facts APIから直接XBRLデータを取得
- 10-Qから四半期データを取得し、期間から正しい四半期番号（Q1, Q2, Q3）を割り当て
- 10-Kから通期データを取得し、Q4を計算（通期 - Q1~Q3合計）
- 会計年度が暦年と異なる場合にも対応（例：NVDAの1月決算）
- 複数クラス株式の希薄化後株式数を合算（ただし、同じend, startの期間ごとに合算）
- 調整項目は元のXBRLタグ名で保存
- adjustment_items.json から必要なXBRLタグを動的に取得し、全て抽出する
- 詳細なデバッグ出力とエラーハンドリング
- ★ 税引前利益が直接取得できない場合、net_income + tax_expense から計算する処理を追加（強化版）
- ★ Q4（10-K）にも年次の税費用を追加する処理を追加
- ★ Q4（10-K）にも年次の調整項目タグを追加する処理を追加（ただし、二重計上を避けるため年次からQ1-3を差し引く）
- ★ 計算した tax_expense を 'tax_expense' キーでも保存（pipeline で取得可能にする）
- ★ 当期純利益を優先順位付きタグから取得（親会社株主帰属利益を優先）
- ★ 各四半期データに fiscal_year と quarter の数値を保存
- ★ 実際のQ4データが存在する場合、計算で上書きしない
"""
import os
import sys
import csv
import json
import requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

# ============================================
# 定数設定
# ============================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# CURRENT_DIR = src/value/adjusted_eps_analyzer
# PROJECT_ROOT = リポジトリルート（3階層上）
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")

# common.sec_data.utils を importできるようにプロジェクトルートをパスに追加
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from common.sec_data.utils import determine_fiscal_year, detect_fiscal_end_month, detect_fiscal_anchor_date
from common.sec_data.tag_definitions import TAG_CANDIDATES
from common.sec_data.fact_selection import select_latest_filed
# MARKETDATA-LAYER-CONSTRUCTION-1着手順序6: 株式数フォールバック④（旧yfinance直接
# 呼び出し）をcommon.market_data.reader経由に切替（beta_fetcher.pyと同じ簡潔な
# トップレベルimportパターン、try/exceptガードなし）
from common.market_data.reader import get_attributes as _get_market_data_attributes
CIK_FILE = os.path.join(CONFIG_DIR, "cik_lookup.csv")
ADJUSTMENT_ITEMS_FILE = os.path.join(CONFIG_DIR, "adjustment_items.json")

HEADERS = {
    'User-Agent': 'jamablue01@gmail.com',  # 必須：連絡先メールアドレス
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}

# 四半期とみなす期間の範囲（日数）- 少し余裕を持たせる
QUARTER_DAYS_MIN = 70
QUARTER_DAYS_MAX = 120
# 年次とみなす期間の最小日数（10-Kの場合）
ANNUAL_DAYS_MIN = 300

# ============================================
# 調整項目から必要なXBRLタグを動的に収集
# ============================================
def load_required_xbrl_tags() -> List[str]:
    """adjustment_items.json から全ての xbrl_tags を収集し、重複を排除して返す"""
    try:
        with open(ADJUSTMENT_ITEMS_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Warning: {ADJUSTMENT_ITEMS_FILE} not found. Using empty list.")
        return []
    
    tags = set()
    categories = config.get("categories", [])
    for cat in categories:
        for sub in cat.get("sub_items", []):
            xbrl_tags = sub.get("xbrl_tags", [])
            for tag in xbrl_tags:
                tags.add(tag)
    
    # 基本的な必須タグ（プレフィックス付きで格納）- バリエーションを拡充
    # 当期純利益（優先順位付きで後で選択するため、全て含める）
    tags.add("us-gaap:NetIncomeLoss")
    tags.add("us-gaap:NetIncomeLossAttributableToParent")                  # 親会社帰属
    tags.add("us-gaap:NetIncomeLossAvailableToCommonStockholders")        # 普通株主帰属（基本）
    tags.add("us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic")   # 普通株主帰属（基本）※代替
    tags.add("us-gaap:ProfitLoss")                                         # SCCOなど: 2012以降NetIncomeLoss未申告、ProfitLossで代替

    # 税引前利益（継続事業）系 → 四半期でよく使われるバリエーションを網羅
    tags.add("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit")   # 標準・最優先
    tags.add("us-gaap:IncomeFromContinuingOperationsBeforeIncomeTaxes")                  # 古いor代替
    tags.add("us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefit")                          # 広め
    tags.add("us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefitAndExtraordinaryItems")     # 稀だが存在
    tags.add("us-gaap:IncomeBeforeTax")                                                  # 簡略版

    # 法人税費用（継続事業）系 → これが四半期で一番取れやすい
    tags.add("us-gaap:IncomeTaxExpenseBenefitContinuingOperations")                      # 四半期最優先
    tags.add("us-gaap:IncomeTaxExpenseBenefit")                                          # 通期・全体用（フォールバック）
    tags.add("us-gaap:ProvisionForIncomeTaxes")                                          # 代替表現（Provision）
    tags.add("us-gaap:IncomeTaxExpenseBenefitFromContinuingOperations")                  # 稀なバリエーション

    # 税引後継続事業利益（最終チェック用）
    tags.add("us-gaap:IncomeLossFromContinuingOperations")
    tags.add("us-gaap:IncomeFromContinuingOperations")

    # EPS計算に必須
    tags.add("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding")
    tags.add("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic")   # 希薄化後が取れない場合の代用
    tags.add("us-gaap:EarningsPerShareDiluted")   # 直接EPSがあれば便利（ただし計算優先）

    # 売上高（get_revenue()が参照するタグ群 ── ここに追加しないと revenue=0.0 になる）
    tags.add("us-gaap:Revenues")
    tags.add("us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax")
    tags.add("us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax")
    tags.add("us-gaap:RevenueFromContractWithCustomer")
    tags.add("us-gaap:NetSales")
    tags.add("us-gaap:SalesRevenueNet")
    tags.add("us-gaap:RevenuesNetOfInterestExpense")
    tags.add("us-gaap:NetInterestIncome")
    tags.add("us-gaap:InterestIncomeExpenseNet")
    tags.add("us-gaap:NoninterestIncome")

    # その他よく使う調整関連（adjustment_items.json にない場合の保険）
    tags.add("us-gaap:ShareBasedCompensation")

    # ★★★ 特殊事情検知用タグ（公正価値変動など） ★★★
    # 買収関連・条件付対価の公正価値変動
    tags.add("us-gaap:BusinessCombinationContingentConsiderationArrangementsChangeInAmountOfContingentConsiderationLiability1")
    tags.add("us-gaap:BusinessCombinationContingentConsiderationArrangementsChangeInTheRangeOfOutcomesContingentConsiderationLiabilityValueHigh")
    tags.add("us-gaap:FairValueAdjustmentOfWarrants")
    tags.add("us-gaap:GainLossOnDerivativeInstrumentsNetPretax")
    tags.add("us-gaap:GainLossOnSaleOfDerivatives")
    tags.add("us-gaap:DerivativeGainLossOnDerivativeNet")
    tags.add("us-gaap:UnrealizedGainLossOnDerivatives")
    # 営業外・非現金の公正価値変動
    tags.add("us-gaap:GainLossOnInvestments")
    tags.add("us-gaap:UnrealizedGainLossOnInvestments")
    tags.add("us-gaap:OtherNonoperatingIncomeExpense")
    tags.add("us-gaap:OtherNonoperatingGainsLosses")

    return list(tags)

# ============================================
# CIKマップ管理
# ============================================
def load_cik_map() -> Dict[str, str]:
    """CIKマップをCSVから読み込む"""
    cik_map = {}
    try:
        if not os.path.exists(CIK_FILE):
            print(f"Warning: {CIK_FILE} not found. Creating empty mapping.")
            os.makedirs(CONFIG_DIR, exist_ok=True)
            with open(CIK_FILE, 'w', encoding='utf-8') as f:
                f.write("ticker,cik,name\n")
            return cik_map
        
        with open(CIK_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['ticker'] and row['cik']:
                    cik = row['cik'].strip().zfill(10)
                    cik_map[row['ticker'].strip().upper()] = cik
        print(f"Loaded {len(cik_map)} CIK mappings from {CIK_FILE}")
        return cik_map
    except Exception as e:
        print(f"Error loading CIK map: {e}")
        return {}

def save_cik_map(cik_map: Dict[str, str]) -> bool:
    """CIKマップをCSVに保存"""
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(CIK_FILE, 'w', encoding='utf-8') as f:
            f.write("ticker,cik,name\n")
            for ticker, cik in sorted(cik_map.items()):
                f.write(f"{ticker},{cik},\n")
        print(f"Saved {len(cik_map)} CIK mappings to {CIK_FILE}")
        return True
    except Exception as e:
        print(f"Error saving CIK map: {e}")
        return False

def get_cik(ticker: str) -> str:
    """ティッカーからCIKを取得"""
    ticker = ticker.strip().upper()
    cik_map = load_cik_map()
    
    if ticker in cik_map:
        return cik_map[ticker]
    
    # SEC APIから直接取得
    print(f"CIK not found for {ticker} in local file. Trying SEC API...")
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for item in data.values():
                if item['ticker'] and item['ticker'].upper() == ticker:
                    cik = str(item['cik_str']).zfill(10)
                    cik_map[ticker] = cik
                    save_cik_map(cik_map)
                    return cik
    except Exception as e:
        print(f"SEC API lookup failed: {e}")
    
    raise Exception(f"CIK not found for {ticker}. Please add to {CIK_FILE}")

# ============================================
# SEC Company Facts APIからデータ取得
# ============================================
def fetch_company_facts(cik: str) -> Dict:
    """
    SEC Company Facts APIから企業の全XBRLファクトを取得
    Args:
        cik: 10桁のCIK番号
    Returns:
        Dict: 企業ファクトデータ
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    print(f"Fetching company facts from {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching company facts: {e}")
        return {}

def extract_value_from_facts(facts_data: Dict, us_gaap_tag: str, form_type: Optional[str] = None, limit: int = 40) -> List[Dict]:
    """
    Company Factsから特定タグの時系列データを抽出
    Args:
        facts_data: Company Facts APIのレスポンス
        us_gaap_tag: タグ名（例: 'NetIncomeLoss' または 'us-gaap:NetIncomeLoss'）
        form_type: フォーム種類（'10-Q', '10-K'）でフィルタする場合は指定
        limit: 取得する最大件数
    Returns:
        List[Dict]: 各期のデータ
    """
    # タグ名から 'us-gaap:' プレフィックスを除去（API内のキーはプレフィックスなし）
    if us_gaap_tag.startswith('us-gaap:'):
        tag = us_gaap_tag[8:]
    else:
        tag = us_gaap_tag

    results = []
    try:
        if 'facts' not in facts_data or 'us-gaap' not in facts_data['facts']:
            return results
        
        if tag not in facts_data['facts']['us-gaap']:
            return results
        
        units_data = facts_data['facts']['us-gaap'][tag]['units']
        for unit_key in units_data:
            if 'USD' in unit_key or 'shares' in unit_key:
                for item in units_data[unit_key]:
                    if form_type and not item.get('form', '').startswith(form_type):
                        continue
                    # 期間情報があるものだけ採用（instantではなくduration）
                    if 'start' in item and 'end' in item:
                        results.append({
                            'end': item.get('end'),
                            'val': item.get('val'),
                            'filed': item.get('filed'),
                            'form': item.get('form'),
                            'unit': unit_key,
                            'start': item.get('start')
                        })
                break
    except Exception as e:
        print(f"Error extracting {us_gaap_tag}: {e}")
    
    # 日付でソート（新しい順）
    results.sort(key=lambda x: x['end'], reverse=True)
    return results[:limit]

def get_diluted_shares_from_facts(facts_data: Dict, form_type: Optional[str] = None, limit: int = 40) -> List[Dict]:
    """
    希薄化後株式数を取得（複数クラスがある場合は合算）
    戻り値の各要素は {'end': str, 'val': float, 'filed': str, 'form': str, 'unit': str, 'start': str} の形式
    修正点: グループ化キーを (end, start) に変更し、同じ期間内のクラス株を合算する。
    """
    tag = "WeightedAverageNumberOfDilutedSharesOutstanding"
    try:
        if 'facts' not in facts_data or 'us-gaap' not in facts_data['facts']:
            return []
        if tag not in facts_data['facts']['us-gaap']:
            return []
        
        units_data = facts_data['facts']['us-gaap'][tag]['units']
        # 通常は 'shares' 単位
        for unit_key in units_data:
            if 'shares' in unit_key:
                # 同じ (end, start) 日付のものをグループ化して合算
                period_map = {}
                for item in units_data[unit_key]:
                    if form_type and not item.get('form', '').startswith(form_type):
                        continue
                    if 'start' in item and 'end' in item:
                        key = (item['end'], item['start'])  # ★修正ポイント
                        if key not in period_map:
                            period_map[key] = {
                                'end': item['end'],
                                'start': item['start'],
                                'val': 0,
                                'filed': item.get('filed'),
                                'form': item.get('form'),
                                'unit': unit_key,
                            }
                        period_map[key]['val'] += item.get('val', 0)
                
                # マップをリストに変換
                results = list(period_map.values())
                results.sort(key=lambda x: x['end'], reverse=True)
                return results[:limit]
    except Exception as e:
        print(f"Error getting diluted shares: {e}")
    return []

# ============================================
# 会計年度判定と四半期分類
# ============================================
# ARCH-DATA-1ステージ2: 会計年度末月の検出ロジックはcommon/sec_data/utils.py::
# detect_fiscal_end_month()に統一済み（旧determine_fiscal_year_end()はここに
# あったローカル実装。form.startswith('10-K')で10-K/Aを含めてしまう基準だったが、
# parser.py側の「form=='10-K'完全一致」というより保守的な基準に統一した）。

def get_quarter_number(end_date: datetime, fiscal_end_month: int) -> int:
    """
    終了日と会計年度終了月から四半期番号（1-4）を決定する
    """
    end_month = end_date.month
    
    # 会計年度終了月からのオフセットを計算
    # 例： fiscal_end_month=1（1月決算）の場合
    #   end_month=1 → Q4 (offset 0)
    #   end_month=10 → Q1 (offset 3)
    #   end_month=7  → Q2 (offset 6)
    #   end_month=4  → Q3 (offset 9)
    if end_month <= fiscal_end_month:
        offset = fiscal_end_month - end_month
    else:
        offset = fiscal_end_month + 12 - end_month
    
    # offset から四半期をマッピング
    if offset <= 1:
        return 4
    elif offset <= 4:
        return 3
    elif offset <= 7:
        return 2
    else:
        return 1

def _neighbor_quarter_diluted_shares(
    quarters_map: Dict[Tuple[int, int], Dict[str, Any]],
    target_key: Tuple[int, int],
) -> float:
    """target_key（(fiscal_year, quarter)）に時間的に最も近い他の四半期の
    実株数（diluted_shares、0より大きい値）を返す。直前優先・見つからなければ直後。
    どちらにも実データがない場合は0.0を返す（呼び出し元でyfinance等の
    最終フォールバックへ委ねる）。

    ASTS-SHARES-OSCILLATION-1: 株式数フォールバック④（yfinance現在株数の
    無条件代入）が「全期間タグ欠落」銘柄（Visa等）向けの設計だったにも
    関わらず、部分的な欠落四半期（ASTS/AVAV/RCAT）にも適用され、現在時点の
    株数が過去の四半期に逆行伝播していた問題への対応。Q4ブロック（10-Kから
    Q4を計算する際の「Q3の実株数を引き継ぐ」既存パターン）を一般化し、
    Q1〜Q3の欠落にも同じ考え方を適用する（本関数はフォールバック③として
    Q1〜Q3欠落にも使われる）。
    """
    other_keys = sorted(k for k in quarters_map.keys() if k != target_key)
    if not other_keys:
        return 0.0

    prior_keys = [k for k in other_keys if k < target_key]
    next_keys = [k for k in other_keys if k > target_key]

    for k in reversed(prior_keys):  # target_keyに最も近い直前から確認
        val = normalize_value(quarters_map[k].get('diluted_shares', {'value': 0}))
        if val > 0:
            return val
    for k in next_keys:  # target_keyに最も近い直後から確認
        val = normalize_value(quarters_map[k].get('diluted_shares', {'value': 0}))
        if val > 0:
            return val
    return 0.0


# ============================================
# メイン抽出関数（改善版）
# ============================================
def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """
    四半期データを取得（SEC API直アクセス＋会計年度対応＋10-KからのQ4補完）
    Args:
        ticker: 銘柄ティッカー
        years: 取得する年数
    Returns:
        List[Dict]: 四半期データのリスト（Q1～Q4が含まれる）
    """
    try:
        # CIK取得
        cik = get_cik(ticker)
        print(f"CIK: {cik}")
        
        # Company Facts取得
        facts = fetch_company_facts(cik)
        if not facts:
            print(f"No facts data for {ticker}")
            return []
        
        # 必要なXBRLタグを動的に収集
        required_tags = load_required_xbrl_tags()
        print(f"Required XBRL tags: {required_tags}")
        
        # タグごとにデータを抽出し、マップに保存
        tag_data_map = {}  # tag -> list of items
        for tag in required_tags:
            # 10-Qと10-Kの両方を取得（後でフィルタする）
            items = extract_value_from_facts(facts, tag, form_type=None, limit=years*6)  # 多めに取得
            tag_data_map[tag] = items
            print(f"Extracted {len(items)} items for {tag}")
        
        # ---------- 10-Kから年次データを抽出 ----------
        # 年次データ（10-K）のみを期間フィルタ（300日以上）で抽出
        annual_data_by_tag = {}
        for tag, items in tag_data_map.items():
            annual_items = []
            for item in items:
                if item.get('form', '').startswith('10-K') and 'start' in item and 'end' in item:
                    start = datetime.strptime(item['start'], '%Y-%m-%d')
                    end = datetime.strptime(item['end'], '%Y-%m-%d')
                    days_diff = (end - start).days
                    if days_diff >= ANNUAL_DAYS_MIN:
                        annual_items.append(item)
            annual_data_by_tag[tag] = annual_items
        
        # 会計年度終了月を特定（複数タグをフォールバックしながら取得）
        # EPS-ANALYZER-NORMALIZE-SCOPE-1: common/sec_data/tag_definitions.py::
        # TAG_CANDIDATES["NET_INCOME"]（parser.py/quarterly.pyと共有）を参照する。
        # 順序: NetIncomeLoss, ProfitLoss, NetIncomeLossAvailableToCommonStockholdersBasic
        # （既存3タグ、順序変更なし）+ NetIncomeLossAvailableToCommonStockholders,
        # NetIncomeLossAttributableToParent, IncomeLossFromContinuingOperations
        # （EPS Analyzer固有だった3タグ、末尾に追加）
        NET_INCOME_ANNUAL_TAGS = [f'us-gaap:{tag}' for tag in TAG_CANDIDATES["NET_INCOME"]]
        net_income_annual = []
        _annual_tag_used = None
        _annual_tag_map = {t: annual_data_by_tag.get(t, []) for t in NET_INCOME_ANNUAL_TAGS if annual_data_by_tag.get(t)}
        if _annual_tag_map:
            # 最新10-K/10-Qの終了日が最も新しいタグを採用
            def _annual_latest(items):
                return max((i.get('end', '') for i in items), default='')
            _annual_tag_used = max(
                _annual_tag_map.keys(),
                key=lambda t: (_annual_latest(_annual_tag_map[t]), -NET_INCOME_ANNUAL_TAGS.index(t))
            )
            net_income_annual = _annual_tag_map[_annual_tag_used]

        # 会計年度末月・決算アンカー日の検出はcommon/sec_data/utils.pyに統一
        # （ARCH-DATA-1ステージ2）。parser.py::_detect_fiscal_end_monthと同じ
        # 「form=='10-K'完全一致・fp=='FY'」基準で生のus-gaapデータを直接走査する
        # ため、net_income_annual（form.startswith('10-K')で10-K/Aを含む・
        # ANNUAL_DAYS_MIN=300の緩い基準でQ4計算用に別途フィルタ済みのリスト）
        # とは独立して判定する。
        _us_gaap = facts.get('facts', {}).get('us-gaap', {})
        fiscal_end_month = detect_fiscal_end_month(_us_gaap, NET_INCOME_ANNUAL_TAGS)
        print(f"Detected fiscal year end month: {fiscal_end_month}")

        # 決算アンカー日（月+日）を検出（ARCH-DATA-1ステージ2: アンカー日ウィンドウ方式）。
        # 検出不可の場合はNone,Noneとなり、determine_fiscal_year()側で従来の
        # 月のみ比較にフォールバックする
        _anchor = detect_fiscal_anchor_date(_us_gaap, NET_INCOME_ANNUAL_TAGS)
        anchor_month, anchor_day = _anchor if _anchor else (None, None)
        print(f"Detected fiscal anchor date: {anchor_month}/{anchor_day}")

        # 希薄化後株式数のマップ（end, start -> value）を作成（Q4計算用に年次も必要）
        diluted_shares_all = tag_data_map.get('us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding', [])
        diluted_map = {}  # key: (end, start) -> value
        for item in diluted_shares_all:
            if 'start' in item and 'end' in item:
                key = (item['end'], item['start'])
                diluted_map[key] = item['val']
        
        # ---------- 10-Qデータを四半期ごとに分類 ----------
        # まず、すべての10-Qデータを期間でフィルタ（70～120日）し、さらに同じend日付で複数ある場合は最も短い期間（四半期）を優先
        # NetIncomeLossがない銘柄（例: AVAV）のためフォールバックタグを試みる
        # EPS-ANALYZER-NORMALIZE-SCOPE-1: NET_INCOME_ANNUAL_TAGSと同じ共有定数を使う
        # （旧実装は年次・四半期で別々のリストを保持していたが値は同一だった）
        NET_INCOME_QUARTERLY_TAGS = NET_INCOME_ANNUAL_TAGS
        # タグごとに四半期候補を収集し、最新データを持つタグを選択する
        # (SCCO等: NetIncomeLossは2012で終了、ProfitLossが2026まで継続)
        _tag_candidates_map = {}
        for _qtag in NET_INCOME_QUARTERLY_TAGS:
            net_income_10q = tag_data_map.get(_qtag, [])
            _q_candidates = []
            for q_item in net_income_10q:
                if not q_item.get('form', '').startswith('10-Q'):
                    continue
                if 'start' not in q_item or 'end' not in q_item:
                    continue
                start = datetime.strptime(q_item['start'], '%Y-%m-%d')
                end = datetime.strptime(q_item['end'], '%Y-%m-%d')
                days_diff = (end - start).days
                if QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX:
                    _q_candidates.append({
                        'start': start,
                        'end': end,
                        'end_str': q_item['end'],
                        'start_str': q_item['start'],
                        'val': q_item['val'],
                        'unit': q_item['unit'],
                        'filed': q_item.get('filed', q_item['end']),
                        'days': days_diff
                    })
            if _q_candidates:
                _tag_candidates_map[_qtag] = _q_candidates

        quarterly_candidates = []
        _quarterly_tag_used = None
        if _tag_candidates_map:
            # 最新エントリが最も新しいタグを採用（同率の場合は優先順位リスト順）
            def _latest_end(cands):
                return max(c['end_str'] for c in cands)
            best_tag = max(
                _tag_candidates_map.keys(),
                key=lambda t: (_latest_end(_tag_candidates_map[t]),
                               -NET_INCOME_QUARTERLY_TAGS.index(t))
            )
            quarterly_candidates = _tag_candidates_map[best_tag]
            _quarterly_tag_used = best_tag
        print(f"Quarterly candidates: {len(quarterly_candidates)} items (from {_quarterly_tag_used or 'none'})")
        
        # 同じ終了日(end)のデータをグループ化し、最も期間の短いものを採用（四半期データとして適切）
        best_quarterly = {}
        for cand in quarterly_candidates:
            end_str = cand['end_str']
            if end_str not in best_quarterly or cand['days'] < best_quarterly[end_str]['days']:
                best_quarterly[end_str] = cand
        
        # quarters_map の構築
        quarters_map = {}  # key: (fiscal_year, quarter) -> data
        
        for end_str, cand in best_quarterly.items():
            # 会計年度を決定（共通関数で統一）
            fiscal_year = determine_fiscal_year(cand['end'], fiscal_end_month, anchor_month, anchor_day)

            quarter_num = get_quarter_number(cand['end'], fiscal_end_month)
            key = (fiscal_year, quarter_num)
            
            # 基本データを作成
            if key not in quarters_map:
                quarters_map[key] = {
                    'filing_date': end_str,
                    'form': '10-Q',
                    'start': cand['start_str'],
                    'end': end_str,
                    'filed': cand['filed'],
                    'quarter': quarter_num,
                    'fiscal_year': fiscal_year
                }
            else:
                # 既存のデータより提出日が新しければ更新
                existing_filed = quarters_map[key].get('filed', '')
                if cand['filed'] > existing_filed:
                    quarters_map[key]['filed'] = cand['filed']
            
            # NetIncomeLossをセット
            quarters_map[key]['net_income'] = {'value': cand['val'], 'unit': cand['unit']}
        
        # 他のタグのデータを追加（10-Qのみ）
        for tag in required_tags:
            if tag == 'us-gaap:NetIncomeLoss' or tag == 'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding':
                continue  # 特別扱い済み
            tag_items = tag_data_map.get(tag, [])
            for item in tag_items:
                if not item.get('form', '').startswith('10-Q'):
                    continue
                if 'start' not in item or 'end' not in item:
                    continue
                start = datetime.strptime(item['start'], '%Y-%m-%d')
                end = datetime.strptime(item['end'], '%Y-%m-%d')
                days_diff = (end - start).days
                if not (QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX):
                    continue
                
                fiscal_year = determine_fiscal_year(end, fiscal_end_month, anchor_month, anchor_day)

                quarter_num = get_quarter_number(end, fiscal_end_month)
                key = (fiscal_year, quarter_num)
                if key in quarters_map:
                    quarters_map[key][tag] = {'value': item['val'], 'unit': item['unit']}
        
        # 希薄化後株式数を追加（四半期）
        # SPLIT-AUTO-CHECK-1: 同一期間に複数のfactが競合する場合（株式分割による
        # 比較年度再掲等、SEC-TAG-FICO-CPRT-1と同型のfact競合パターン）、
        # filed日が最新のものを優先する。以前は無条件上書き（リスト末尾勝ち）だった。
        # EPS-ANALYZER-NORMALIZE-SCOPE-1: filed日最新優先の判定自体は
        # common/sec_data/fact_selection.py::select_latest_filed()
        # （quarterly.pyと共有）に委譲する。まず(fiscal_year, quarter)キーで
        # 候補をグルーピングしてから、キーごとに最新filedの1件を選ぶ。
        _diluted_shares_candidates: Dict[Tuple[int, int], list] = {}
        for item in diluted_shares_all:
            if not item.get('form', '').startswith('10-Q'):
                continue
            if 'start' not in item or 'end' not in item:
                continue
            start = datetime.strptime(item['start'], '%Y-%m-%d')
            end = datetime.strptime(item['end'], '%Y-%m-%d')
            days_diff = (end - start).days
            if not (QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX):
                continue

            fiscal_year = determine_fiscal_year(end, fiscal_end_month, anchor_month, anchor_day)

            quarter_num = get_quarter_number(end, fiscal_end_month)
            key = (fiscal_year, quarter_num)
            if key not in quarters_map:
                continue
            _diluted_shares_candidates.setdefault(key, []).append(item)

        for key, candidates in _diluted_shares_candidates.items():
            best_item = select_latest_filed(candidates)
            quarters_map[key]['diluted_shares'] = {'value': best_item['val'], 'unit': best_item['unit']}
        
        # EPS-ANALYZER-NORMALIZE-SCOPE-1: 以前はここに`net_income_priority`という
        # 第3の候補タグリスト（NET_INCOME_QUARTERLY_TAGSとは異なる順序）が存在し、
        # 上のbest_tag選定結果を無条件で上書きしていた。この上書きロジックは
        # 'us-gaap:NetIncomeLoss'を探すが、そのキーは直前の「他のタグのデータを
        # 追加」ループで意図的に除外されており`data`辞書には決して現れないため、
        # NetIncomeLossAvailableToCommonStockholders/AttributableToParentが
        # 存在しない銘柄（ABBV/XOM/WMT/VZ等、NetIncomeLossとProfitLossを両方
        # 申告する成熟企業に多い）では常にProfitLoss（非支配持分込みの連結利益）
        # へ意図せずフォールバックしていた。全101銘柄の実値検証で、このバグにより
        # 40銘柄・434四半期でProfitLossが誤採用されていたことを確認済み
        # （削除後はNetIncomeLoss＝親会社帰属利益が正しく採用される。詳細は
        # tests/test_extract_key_facts_net_income_selection.py参照）。
        # net_income はNET_INCOME_QUARTERLY_TAGS（TAG_CANDIDATES["NET_INCOME"]）
        # ベースのbest_tag選定結果（quarters_map[key]['net_income']、568行目で設定済み）
        # をそのまま採用する。

        # ---------- 10-KからQ4を計算（ただし、実際のQ4が存在する場合はスキップ）----------
        # まず、実際のQ4データが既に quarters_map に含まれているか確認するために、各fiscal_yearのQ4キーをチェック
        actual_q4_keys = []
        for key in quarters_map.keys():
            if key[1] == 4:  # quarter 4
                actual_q4_keys.append(key)
        
        # 各 fiscal_year の Q1, Q2, Q3 が揃っているかを確認
        fiscal_years = set(k[0] for k in quarters_map.keys() if k[1] in (1,2,3))
        for fiscal_year in fiscal_years:
            q1_key = (fiscal_year, 1)
            q2_key = (fiscal_year, 2)
            q3_key = (fiscal_year, 3)
            q4_key = (fiscal_year, 4)
            
            # 実際のQ4が既に存在する場合はスキップ
            if q4_key in actual_q4_keys:
                print(f"  Skipping Q4 calculation for fiscal year {fiscal_year}: actual Q4 exists.")
                continue
            
            if q1_key in quarters_map and q2_key in quarters_map and q3_key in quarters_map:
                # Q1-Q3 のデータがある場合、年次データからQ4を計算
                # 四半期選択と同じタグを使用（quarterly/_annual_tag_usedと一貫性を保つ）
                net_income_annual_items = net_income_annual
                # この fiscal_year に対応する10-Kを探す（endが fiscal_year の終了日と一致するもの）
                target_k_item = None
                for item in net_income_annual_items:
                    item_end = datetime.strptime(item['end'], '%Y-%m-%d')
                    # 会計年度の決定（共通関数で統一）
                    item_fiscal_year = determine_fiscal_year(item_end, fiscal_end_month, anchor_month, anchor_day)
                    if item_fiscal_year == fiscal_year:
                        target_k_item = item
                        break
                
                if target_k_item:
                    q1_income = normalize_value(quarters_map[q1_key].get('net_income', {'value':0}))
                    q2_income = normalize_value(quarters_map[q2_key].get('net_income', {'value':0}))
                    q3_income = normalize_value(quarters_map[q3_key].get('net_income', {'value':0}))
                    q1q3_sum = q1_income + q2_income + q3_income
                    
                    annual_net = target_k_item['val']
                    q4_net = annual_net - q1q3_sum
                    
                    # 希薄化後株式数（年次）を取得
                    # SPLIT-AUTO-CHECK-1: 先頭一致でbreakする方式（=最古filed優先）を廃し、
                    # Q1〜Q3と同じ「filed日が最新のfactを優先」ルールに統一する
                    diluted_val = 0
                    _diluted_val_filed = None
                    for d_item in diluted_shares_all:
                        if d_item.get('form', '').startswith('10-K') and d_item.get('end') == target_k_item['end']:
                            d_filed = d_item.get('filed', '')
                            if _diluted_val_filed is None or d_filed > _diluted_val_filed:
                                diluted_val = d_item['val']
                                _diluted_val_filed = d_filed
                    if diluted_val == 0:
                        # ASTS-SHARES-OSCILLATION-1: Q3限定の引き継ぎから、Q3も欠落している
                        # 場合に備えて最も近い他の実四半期を探す一般化版に変更
                        # （通常ケースではQ3がそのまま最近傍のため挙動は変わらない）
                        diluted_val = _neighbor_quarter_diluted_shares(quarters_map, q4_key)

                    # 単位サニティチェック: 10-KのshareがQ3の1%未満なら千株単位と判断して×1000
                    if diluted_val > 0 and q3_key in quarters_map:
                        q3_shares = normalize_value(quarters_map[q3_key].get('diluted_shares', {'value': 0}))
                        if q3_shares > 0 and diluted_val < q3_shares * 0.01:
                            print(f"  [WARN] 10-K diluted_shares={diluted_val:,.0f} << Q3 shares={q3_shares:,.0f} → 千株単位と判断し×1000補正")
                            diluted_val *= 1000
                    
                    # Q4データを作成
                    q4_data = {
                        'filing_date': target_k_item['end'],
                        'form': '10-K',
                        'net_income': {'value': q4_net, 'unit': 'USD'},
                        'diluted_shares': {'value': diluted_val, 'unit': 'shares'},
                        'start': quarters_map[q3_key]['end'] if q3_key in quarters_map else target_k_item['start'],
                        'end': target_k_item['end'],
                        'filed': target_k_item.get('filed', target_k_item['end']),
                        'quarter': 4,
                        'fiscal_year': fiscal_year
                    }
                    quarters_map[q4_key] = q4_data
                    print(f"  Calculated Q4 for fiscal year {fiscal_year} (end {target_k_item['end']}): net_income={q4_net:,.0f}, diluted_shares={diluted_val:,.0f}")
                else:
                    print(f"  Warning: No 10-K found for fiscal year {fiscal_year}")
            else:
                print(f"  Warning: Missing Q1-Q3 for fiscal year {fiscal_year} (have: {q1_key in quarters_map}, {q2_key in quarters_map}, {q3_key in quarters_map})")
        
        # ★★★ すべての四半期に税費用タグを追加 ★★★
        print("\n=== Adding tax expense to all quarters ===")
        tax_tag_candidates = [
            'us-gaap:IncomeTaxExpenseBenefit',
            'us-gaap:IncomeTaxExpenseBenefitContinuingOperations',
            'us-gaap:ProvisionForIncomeTaxes',
            'us-gaap:IncomeTaxExpenseBenefitFromContinuingOperations'
        ]
        
        for key, data in quarters_map.items():
            filing_date = data['filing_date']
            form = data.get('form', '')
            
            # 税費用を探す（年次データも含めて）
            found_tax = False
            for tax_tag in tax_tag_candidates:
                # まず、同じ filing_date の年次データから探す（Q4の場合）
                if form == '10-K':
                    annual_items = annual_data_by_tag.get(tax_tag, [])
                    for item in annual_items:
                        if item['end'] == filing_date:
                            data[tax_tag] = {'value': item['val'], 'unit': item['unit']}
                            print(f"  Added {tax_tag} to Q4 {filing_date}: {item['val']:,.0f}")
                            found_tax = True
                            break
                else:
                    # 10-Qの場合は、既に quarters_map に含まれているか確認
                    if tax_tag in data:
                        found_tax = True
                        # 既存の値を使う（何もしない）
                        pass
                
                if found_tax:
                    break
            
            if not found_tax:
                print(f"  No tax expense found for {filing_date}")
        
        # ★★★ Q4に年次の調整項目タグを追加（ただし、年次からQ1-3の合計を差し引く）★★★
        print("\n=== Adding adjustment items to Q4 (10-K) ===")
        # adjustment_items.json から調整項目タグのリストを取得（すでに required_tags に含まれている）
        # 調整項目タグとして考えられるもの：株式報酬、リストラ費用など
        # 簡易的に、特定のタグを調整項目とみなす（実際には設定ファイルから取得すべき）
        # ここでは、'us-gaap:ShareBasedCompensation', 'us-gaap:RestructuringCharges' などを例示
        adjustment_tags = [tag for tag in required_tags if tag not in tax_tag_candidates and tag not in [
            'us-gaap:NetIncomeLoss', 'us-gaap:NetIncomeLossAttributableToParent',
            'us-gaap:NetIncomeLossAvailableToCommonStockholders', 'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic',
            'us-gaap:ProfitLoss',
            'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding',
            'us-gaap:EarningsPerShareDiluted', 'us-gaap:IncomeLossFromContinuingOperations',
            'us-gaap:IncomeFromContinuingOperations', 'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
            'us-gaap:IncomeFromContinuingOperationsBeforeIncomeTaxes', 'us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefit',
            'us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefitAndExtraordinaryItems', 'us-gaap:IncomeBeforeTax'
        ]]
        
        for key, data in quarters_map.items():
            if data.get('form') != '10-K':
                continue  # Q4のみ処理
            filing_date = data['filing_date']
            fiscal_year = data['fiscal_year']
            
            # Q1-3の合計を計算するために、同じ fiscal_year の Q1-3 データを取得
            q1_key = (fiscal_year, 1)
            q2_key = (fiscal_year, 2)
            q3_key = (fiscal_year, 3)
            
            # Q3期末日とQ1期首日を取得（YTD値検索に使用）
            q3_end_str   = quarters_map[q3_key].get('end')   if q3_key in quarters_map else None
            q1_start_str = quarters_map[q1_key].get('start') if q1_key in quarters_map else None

            for adj_tag in adjustment_tags:
                # 年次データから adj_tag の値を取得
                annual_items = annual_data_by_tag.get(adj_tag, [])
                annual_val = None
                for item in annual_items:
                    if item['end'] == filing_date:
                        annual_val = item['val']
                        break
                if annual_val is None:
                    continue

                # ★ YTD 9-month値を tag_data_map から直接探す（タグ混在問題への対策）
                # Q2/Q3が別タグ（例: AllocatedShareBasedCompensationExpense）を使う場合、
                # 同タグのQ1-3合算では正しく減算できない。YTD累計値を使う方が確実。
                ytd_9m_val = None
                if q3_end_str and q1_start_str:
                    for item in tag_data_map.get(adj_tag, []):
                        if (item.get('form', '').startswith('10-Q') and
                                item.get('start', '') == q1_start_str and
                                item.get('end', '') == q3_end_str):
                            d = (datetime.strptime(item['end'], '%Y-%m-%d') -
                                 datetime.strptime(item['start'], '%Y-%m-%d')).days
                            if 200 <= d <= 310:
                                ytd_9m_val = item['val']
                                break

                if ytd_9m_val is not None:
                    # YTD法: Q4 = Annual - YTD_Q3（タグ混在に依存しない）
                    q4_val = annual_val - ytd_9m_val
                    print(f"  [YTD] {adj_tag} Q4 {filing_date}: {q4_val:,.0f} (annual={annual_val:,.0f}, ytd_9m={ytd_9m_val:,.0f})")
                else:
                    # フォールバック: 同タグのQ1-3合計を減算
                    # [[EPS-LITE-ANNUAL-AS-QUARTERLY-1]]: Q1〜Q3のいずれにも
                    # adj_tagの四半期(10-Q)申告が1件も存在しない場合、
                    # 「未申告（キー自体が無い）」と「申告はあるが実際に0円」を
                    # 区別できず、q123_sumが常に0になりQ4=年次そのままという
                    # 誤った値を書き込んでしまう（年次はA、四半期はBという別タグに
                    # 切り替えている銘柄で発生。revenue系タグ〈us-gaap:Revenues・
                    # RevenueFromContractWithCustomerExcludingAssessedTax等〉が
                    # このループに巻き込まれることで顕在化し、LITE・APP等9銘柄で
                    # 実測確認済み）。少なくとも1四半期でadj_tagの申告キー自体が
                    # 存在する場合に限り差し引き法を適用し、皆無の場合はこの
                    # タグへのQ4書き込み自体をスキップする（get_revenue()等の
                    # 呼び出し元が優先順位に従って他の候補タグへフォールバック
                    # できるようにするため）。
                    q1_present = q1_key in quarters_map and adj_tag in quarters_map[q1_key]
                    q2_present = q2_key in quarters_map and adj_tag in quarters_map[q2_key]
                    q3_present = q3_key in quarters_map and adj_tag in quarters_map[q3_key]
                    if not (q1_present or q2_present or q3_present):
                        print(f"  [SKIP] {adj_tag} Q4 {filing_date}: Q1-Q3にこのタグの四半期申告が"
                              f"1件も存在しないためスキップ (annual={annual_val:,.0f})")
                        continue
                    q1_val = normalize_value(quarters_map[q1_key].get(adj_tag)) if q1_key in quarters_map else 0
                    q2_val = normalize_value(quarters_map[q2_key].get(adj_tag)) if q2_key in quarters_map else 0
                    q3_val = normalize_value(quarters_map[q3_key].get(adj_tag)) if q3_key in quarters_map else 0
                    q123_sum = q1_val + q2_val + q3_val
                    q4_val = annual_val - q123_sum
                    print(f"  [Q1-3sum] {adj_tag} Q4 {filing_date}: {q4_val:,.0f} (annual={annual_val:,.0f}, q123={q123_sum:,.0f})")

                if abs(q4_val) > 0.01:  # 0でなければ保存
                    data[adj_tag] = {'value': q4_val, 'unit': 'USD'}

        # ★★★ Q2/Q3 YTD差分補完（YTDのみ存在する調整項目への対応）★★★
        # KO/NOW/MRVL等はXBRLで3ヶ月値を報告せずYTD累計値のみ → Q2=YTD6m-Q1, Q3=YTD9m-YTD6m
        print("\n=== Filling Q2/Q3 adjustment items via YTD diff ===")
        for fiscal_year in fiscal_years:
            q1_key = (fiscal_year, 1)
            q2_key = (fiscal_year, 2)
            q3_key = (fiscal_year, 3)
            if q1_key not in quarters_map or q2_key not in quarters_map or q3_key not in quarters_map:
                continue
            q1_data = quarters_map[q1_key]
            q2_data = quarters_map[q2_key]
            q3_data = quarters_map[q3_key]
            q1_start_str = q1_data.get('start')
            q2_end_str   = q2_data.get('end')
            q3_end_str   = q3_data.get('end')
            if not q1_start_str or not q2_end_str or not q3_end_str:
                continue
            for adj_tag in adjustment_tags:
                # YTD_6m検索（start=Q1_start, end=Q2_end, days 140-210）
                ytd_6m_val = None
                for item in tag_data_map.get(adj_tag, []):
                    if (item.get('form', '').startswith('10-Q') and
                            item.get('start', '') == q1_start_str and
                            item.get('end', '') == q2_end_str):
                        d = (datetime.strptime(item['end'], '%Y-%m-%d') -
                             datetime.strptime(item['start'], '%Y-%m-%d')).days
                        if 140 <= d <= 210:
                            ytd_6m_val = item['val']
                            break
                # YTD_9m検索（start=Q1_start, end=Q3_end, days 200-310）
                ytd_9m_val = None
                for item in tag_data_map.get(adj_tag, []):
                    if (item.get('form', '').startswith('10-Q') and
                            item.get('start', '') == q1_start_str and
                            item.get('end', '') == q3_end_str):
                        d = (datetime.strptime(item['end'], '%Y-%m-%d') -
                             datetime.strptime(item['start'], '%Y-%m-%d')).days
                        if 200 <= d <= 310:
                            ytd_9m_val = item['val']
                            break
                if ytd_6m_val is None and ytd_9m_val is None:
                    continue
                q1_val = normalize_value(q1_data.get(adj_tag))
                # Q2: 欠落かつ YTD_6m 存在 → Q2 = YTD_6m - Q1
                if adj_tag not in q2_data and ytd_6m_val is not None:
                    q2_val = ytd_6m_val - q1_val
                    if abs(q2_val) > 0.01:
                        q2_data[adj_tag] = {'value': q2_val, 'unit': 'USD'}
                        print(f"  [YTD6m] {adj_tag} Q2 FY{fiscal_year}: {q2_val:,.0f} (ytd6m={ytd_6m_val:,.0f}, q1={q1_val:,.0f})")
                # Q3: 欠落かつ YTD_9m 存在 → Q3 = YTD_9m - YTD_6m (YTD_6m不在時は - Q1 - Q2)
                if adj_tag not in q3_data and ytd_9m_val is not None:
                    q2_val_now = normalize_value(q2_data.get(adj_tag))
                    if ytd_6m_val is not None:
                        q3_val = ytd_9m_val - ytd_6m_val
                    else:
                        q3_val = ytd_9m_val - q1_val - q2_val_now
                    if abs(q3_val) > 0.01:
                        q3_data[adj_tag] = {'value': q3_val, 'unit': 'USD'}
                        print(f"  [YTD9m] {adj_tag} Q3 FY{fiscal_year}: {q3_val:,.0f} (ytd9m={ytd_9m_val:,.0f})")

        # ★★★ 税引前利益の計算（net_income + tax_expense）- 強化版 ★★★
        print("\n=== Computing pretax_income from net_income + tax_expense ===")
        
        for key, data in quarters_map.items():
            # 既に pretax_income が直接存在する場合はスキップ
            if 'pretax_income' in data:
                continue
                
            net_income = data.get('net_income')
            if not net_income:
                continue
                
            # 税費用を複数の候補タグから探す
            tax_expense = None
            used_tax_tag = None
            for tax_tag in tax_tag_candidates:
                tax_val = data.get(tax_tag)
                if tax_val:
                    tax_expense = tax_val
                    used_tax_tag = tax_tag
                    break
            
            if net_income and tax_expense:
                net_val = normalize_value(net_income)
                tax_val = normalize_value(tax_expense)
                pretax_val = net_val + tax_val  # 税引前利益 = 当期純利益 + 法人税等
                data['pretax_income'] = {'value': pretax_val, 'unit': 'USD'}
                # ★ tax_expense を 'tax_expense' キーでも保存（pipeline で取得できるようにする）
                data['tax_expense'] = {'value': tax_val, 'unit': 'USD'}
                print(f"  Computed pretax_income for {data['filing_date']} (FY{data.get('fiscal_year')} Q{data.get('quarter')}): {pretax_val:,.0f} USD (net={net_val:,.0f}, tax={tax_val:,.0f} from {used_tax_tag})")
            else:
                print(f"  Cannot compute pretax_income for {data['filing_date']}: missing net_income or tax_expense (tax tags checked: {tax_tag_candidates})")
        
        # ---------- 株式数フォールバック① EarningsPerShareDiluted から逆算（XOM 等 WA 株数未提供銘柄） ----------
        print("\n=== Share count fallback: EPS-derived ===")
        _eps_items = tag_data_map.get('us-gaap:EarningsPerShareDiluted', [])
        _eps_map: Dict[str, float] = {}
        for _item in _eps_items:
            if not _item.get('form', '').startswith('10-Q'):
                continue
            if 'start' not in _item or 'end' not in _item:
                continue
            _s = datetime.strptime(_item['start'], '%Y-%m-%d')
            _e = datetime.strptime(_item['end'], '%Y-%m-%d')
            if QUARTER_DAYS_MIN <= (_e - _s).days <= QUARTER_DAYS_MAX:
                _eps_map[_item['end']] = _item['val']
        for _key, _data in quarters_map.items():
            if normalize_value(_data.get('diluted_shares', {'value': 0})) != 0:
                continue
            _ni = normalize_value(_data.get('net_income', {'value': 0}))
            _eps = _eps_map.get(_data.get('filing_date', ''))
            if _eps and abs(_eps) > 1e-9 and _ni != 0:
                _implied = abs(_ni / _eps)
                if _implied > 1e5:
                    _data['diluted_shares'] = {'value': _implied, 'unit': 'shares'}
                    print(f"  [EPS逆算] {_data['filing_date']}: {_implied/1e6:.1f}M株 (NI={_ni/1e9:.3f}B / EPS={_eps:.4f})")

        # ---------- 株式数フォールバック② WeightedAverageNumberOfSharesOutstandingBasic を代用 ----------
        print("\n=== Share count fallback: Basic shares ===")
        _basic_items = tag_data_map.get('us-gaap:WeightedAverageNumberOfSharesOutstandingBasic', [])
        _basic_map: Dict[str, float] = {}
        for _item in _basic_items:
            if not _item.get('form', '').startswith('10-Q'):
                continue
            if 'start' not in _item or 'end' not in _item:
                continue
            _s = datetime.strptime(_item['start'], '%Y-%m-%d')
            _e = datetime.strptime(_item['end'], '%Y-%m-%d')
            if QUARTER_DAYS_MIN <= (_e - _s).days <= QUARTER_DAYS_MAX:
                _basic_map[_item['end']] = _item['val']
        for _key, _data in quarters_map.items():
            if normalize_value(_data.get('diluted_shares', {'value': 0})) != 0:
                continue
            _basic = _basic_map.get(_data.get('filing_date', ''))
            if _basic and _basic > 1e5:
                _data['diluted_shares'] = {'value': _basic, 'unit': 'shares'}
                print(f"  [Basic株数代用] {_data['filing_date']}: {_basic/1e6:.1f}M株")

        # ---------- 株式数フォールバック③ 隣接する実四半期からの引き継ぎ ----------
        # ASTS-SHARES-OSCILLATION-1: フォールバック④（yfinance現在株数の無条件代入）が
        # 「全期間タグ欠落」銘柄（Visa等）向けの設計だったにも関わらず、一部四半期のみ
        # 欠落している銘柄（ASTS/AVAV/RCAT）にも適用され、現在時点の株数が過去の
        # 四半期に逆行伝播していた。Q4ブロック（10-KからQ4を計算する際の
        # 「直近の実四半期を引き継ぐ」既存パターン）を一般化し、Q1〜Q3の欠落にも適用する。
        print("\n=== Share count fallback: nearest quarter carry-over ===")
        for _key, _data in quarters_map.items():
            if normalize_value(_data.get('diluted_shares', {'value': 0})) != 0:
                continue
            _neighbor_val = _neighbor_quarter_diluted_shares(quarters_map, _key)
            if _neighbor_val > 0:
                _data['diluted_shares'] = {'value': _neighbor_val, 'unit': 'shares'}
                print(f"  [隣接四半期引き継ぎ] {_data['filing_date']}: {_neighbor_val/1e6:.1f}M株")

        # ---------- 株式数フォールバック④ market_data属性の現在株式数を代用（V 等 SEC に株数未提供） ----------
        # 隣接する実四半期も存在しない（＝全期間で欠落している）かつ net_income がある
        # 場合のみ発動する（本来の設計意図「全期間欠落銘柄向け」を維持）
        # MARKETDATA-LAYER-CONSTRUCTION-1着手順序6: yfinance直接呼び出し（.info単発）を
        # common.market_data.reader.get_attributes()経由に切替。優先順位パターン
        # （sharesOutstanding優先→impliedSharesOutstandingフォールバック）はそのまま維持。
        _missing_shares_quarters = [
            _d for _d in quarters_map.values()
            if normalize_value(_d.get('diluted_shares', {'value': 0})) == 0
               and normalize_value(_d.get('net_income', {'value': 0})) != 0
        ]
        if _missing_shares_quarters:
            print(f"\n=== Share count fallback: market_data attributes ({len(_missing_shares_quarters)} quarters missing shares) ===")
            try:
                _attrs = _get_market_data_attributes(ticker)
                _shares_val = None
                if _attrs:
                    _shares_val = (
                        _attrs.get('shares_outstanding')
                        or _attrs.get('implied_shares_outstanding')
                    )
                if _shares_val and _shares_val > 1e5:
                    print(f"  [market_data] {ticker}: {_shares_val/1e6:.1f}M株")
                    for _data in _missing_shares_quarters:
                        _data['diluted_shares'] = {'value': float(_shares_val), 'unit': 'shares'}
                        print(f"    → {_data['filing_date']}: market_data株数を適用")
                else:
                    print(f"  [market_data] 株数取得失敗: {_shares_val}")
            except Exception as _yf_err:
                print(f"  [market_data] エラー: {_yf_err}")

        # ---------- 最終的なリストに変換 ----------
        quarterly_list = []
        # キーを (fiscal_year, quarter) でソート（古い順）
        for (fiscal_year, quarter), data in sorted(quarters_map.items()):
            # 必須データの確認
            if 'net_income' in data and 'diluted_shares' in data:
                # fiscal_year と quarter を明示的に保存
                data['fiscal_year'] = fiscal_year
                data['quarter'] = quarter
                quarterly_list.append(data)
                net_val = normalize_value(data['net_income'])
                shr_val = normalize_value(data['diluted_shares'])
                print(f"  ✓ {data['filing_date']} (FY{fiscal_year} Q{quarter}): net_income={net_val:,.0f}, diluted_shares={shr_val:,.0f}")
            else:
                missing = []
                if 'net_income' not in data:
                    missing.append('net_income')
                if 'diluted_shares' not in data:
                    missing.append('diluted_shares')
                print(f"  ✗ {data.get('filing_date', 'unknown')} (FY{fiscal_year} Q{quarter}): missing {', '.join(missing)}")
        
        # 日付順にソート（新しい順）して返す（UIの期待に合わせて）
        quarterly_list.sort(key=lambda x: x['filing_date'], reverse=True)

        # 株式数サニティチェック（2段階）
        # ① 全期間の平均株式数が 1M 未満 → 全社的に千株単位で申告（例: LOAR）→ 全期間 ×1000
        # ② 個別四半期が中央値の 1% 未満かつ ×1000 が中央値の 2 倍以内 → 単独期の申告ミス（例: ONDS）→ 当該期 ×1000
        if quarterly_list:
            _valid_shares = [
                normalize_value(q.get('diluted_shares', {'value': 0}))
                for q in quarterly_list
                if normalize_value(q.get('diluted_shares', {'value': 0})) > 0
            ]
            if _valid_shares:
                _avg_shares = sum(_valid_shares) / len(_valid_shares)
                if 0 < _avg_shares < 1_000_000:
                    # ① 全期間の千株単位補正
                    print(f"[WARN] {ticker}: 平均株式数={_avg_shares:,.0f} < 1M → 千株単位と判断し ×1000 補正")
                    for q in quarterly_list:
                        ds = q.get('diluted_shares')
                        if ds and isinstance(ds, dict) and normalize_value(ds) > 0:
                            ds['value'] = ds['value'] * 1000
                else:
                    # ② 単独期の外れ値補正: 直近8四半期の中央値の 1% 未満は千株単位と判断し ×1000
                    # （直近中央値を使う: 古い期間の株式数が少なく全期間中央値が低くなるケースを防ぐ）
                    _dated_shares = sorted(
                        [(q.get('filing_date', ''), normalize_value(q.get('diluted_shares', {'value': 0})))
                         for q in quarterly_list
                         if normalize_value(q.get('diluted_shares', {'value': 0})) > 0],
                        reverse=True
                    )
                    _recent_shares = [s for _, s in _dated_shares[:8]]
                    if _recent_shares:
                        _sorted_r = sorted(_recent_shares)
                        _med_r = _sorted_r[len(_sorted_r) // 2]
                        if _med_r > 0:
                            for q in quarterly_list:
                                ds = q.get('diluted_shares')
                                if not (ds and isinstance(ds, dict)):
                                    continue
                                _sv = normalize_value(ds)
                                if 0 < _sv < _med_r * 0.01:
                                    print(
                                        f"[WARN] {ticker}: {q.get('filing_date','?')} 株式数={_sv:,.0f}"
                                        f" << 直近中央値={_med_r:,.0f} → 千株単位と判断し ×1000 補正"
                                    )
                                    ds['value'] = ds['value'] * 1000

        print(f"\n{ticker}: {len(quarterly_list)}件の四半期データを取得")
        return quarterly_list
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        import traceback
        traceback.print_exc()
        return []

def normalize_value(value_dict: Optional[Dict]) -> float:
    """
    単位正規化（すべてUSD absolute valueに統一）
    Args:
        value_dict: {"value": 数値, "unit": "USD"|"shares"|"thousands"|...}
    Returns:
        float: 正規化された値
    """
    if not value_dict:
        return 0.0
    
    value = float(value_dict.get("value", 0))
    unit = value_dict.get("unit", "USD").lower()
    
    if unit in ["thousands", "thousand"]:
        return value * 1_000
    elif unit in ["millions", "million"]:
        return value * 1_000_000
    elif unit in ["billions", "billion"]:
        return value * 1_000_000_000
    else:
        return value

# ============================================
# テスト用メイン関数
# ============================================
def main():
    """テスト実行用"""
    ticker = "TSLA"  # テストしたい銘柄
    print(f"Testing data extraction for {ticker}...")
    
    data = extract_quarterly_facts(ticker, years=5)
    
    if data:
        print(f"\nSuccessfully extracted {len(data)} quarters:")
        for i, quarter in enumerate(data[:15]):
            print(f"\nQuarter {i+1}: {quarter['filing_date']} ({quarter.get('form', 'unknown')})")
            net = normalize_value(quarter.get('net_income'))
            shares = normalize_value(quarter.get('diluted_shares'))
            print(f"  Net Income: {net:,.0f} USD")
            print(f"  Diluted Shares: {shares:,.0f}")
            if shares > 0:
                eps = net / shares
                print(f"  Implied EPS: {eps:.4f} USD")
            
            # 調整項目の例
            sbc = quarter.get('us-gaap:ShareBasedCompensation')
            if sbc:
                sbc_val = normalize_value(sbc)
                print(f"  SBC: {sbc_val:,.0f} USD")
    else:
        print("No data extracted")

if __name__ == "__main__":
    main()
