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
import csv
import json
import requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta

# ============================================
# 定数設定
# ============================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
CIK_FILE = os.path.join(CONFIG_DIR, "cik_lookup.csv")
ADJUSTMENT_ITEMS_FILE = os.path.join(CONFIG_DIR, "adjustment_items.json")

HEADERS = {
    'User-Agent': 'jamablue01@gmail.com',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}

QUARTER_DAYS_MIN = 70
QUARTER_DAYS_MAX = 120
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
    
    # 基本的な必須タグ
    tags.add("us-gaap:NetIncomeLoss")
    tags.add("us-gaap:NetIncomeLossAttributableToParent")
    tags.add("us-gaap:NetIncomeLossAvailableToCommonStockholders")
    tags.add("us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic")
    tags.add("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit")
    tags.add("us-gaap:IncomeFromContinuingOperationsBeforeIncomeTaxes")
    tags.add("us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefit")
    tags.add("us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefitAndExtraordinaryItems")
    tags.add("us-gaap:IncomeBeforeTax")
    tags.add("us-gaap:IncomeTaxExpenseBenefitContinuingOperations")
    tags.add("us-gaap:IncomeTaxExpenseBenefit")
    tags.add("us-gaap:ProvisionForIncomeTaxes")
    tags.add("us-gaap:IncomeTaxExpenseBenefitFromContinuingOperations")
    tags.add("us-gaap:IncomeLossFromContinuingOperations")
    tags.add("us-gaap:IncomeFromContinuingOperations")
    tags.add("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding")
    tags.add("us-gaap:EarningsPerShareDiluted")
    tags.add("us-gaap:ShareBasedCompensation")
    tags.add("us-gaap:AllocatedShareBasedCompensationExpense")       # 銀行・金融機関で主に使用
    tags.add("us-gaap:EmployeeBenefitsAndShareBasedCompensation")    # 給付合算型
    tags.add("us-gaap:StockBasedCompensation")                       # 旧タグ名
    tags.add("us-gaap:ShareBasedCompensationExpense")                # PLTR等で使用
    tags.add("us-gaap:RestrictedStockExpense")                       # RSU費用（PLTR等）
    tags.add("us-gaap:EmployeeServiceShareBasedCompensationNonvestedAwardsTotalCompensationCostNotYetRecognized")  # まれに使用

    # ★★★ 売上高タグ（SBC/売上高比率用）★★★
    tags.add("us-gaap:Revenues")
    tags.add("us-gaap:RevenueFromContractWithCustomer")
    tags.add("us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax")
    tags.add("us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax")
    tags.add("us-gaap:NetSales")
    tags.add("us-gaap:TotalRevenue")
    tags.add("us-gaap:SalesRevenueNet")
    tags.add("us-gaap:InterestAndDividendIncomeOperating")   # 金融・FinTech系
    tags.add("us-gaap:RevenuesNetOfInterestExpense")         # 金融系
    tags.add("us-gaap:NetInterestIncome")                    # 銀行：純金利収益
    tags.add("us-gaap:NoninterestIncome")                    # 銀行：非金利収益
    tags.add("us-gaap:InterestIncomeExpenseNet")             # 銀行：純金利収益（別タグ）

    return list(tags)

# ============================================
# CIKマップ管理
# ============================================
def load_cik_map() -> Dict[str, str]:
    cik_map = {}
    try:
        if not os.path.exists(CIK_FILE):
            print(f"Warning: {CIK_FILE} not found. Creating empty mapping.")
            os.makedirs(CONFIG_DIR, exist_ok=True)
            with open(CIK_FILE, 'w', encoding='utf-8') as f:
                f.write("ticker,cik,name,sector\n")
            return cik_map
        
        with open(CIK_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('ticker') and row.get('cik'):
                    cik = row['cik'].strip().zfill(10)
                    cik_map[row['ticker'].strip().upper()] = cik
        print(f"Loaded {len(cik_map)} CIK mappings from {CIK_FILE}")
        return cik_map
    except Exception as e:
        print(f"Error loading CIK map: {e}")
        return {}

def save_cik_map(cik_map: Dict[str, str]) -> bool:
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(CIK_FILE, 'w', encoding='utf-8') as f:
            f.write("ticker,cik,name,sector\n")
            for ticker, cik in sorted(cik_map.items()):
                f.write(f"{ticker},{cik},\n")
        print(f"Saved {len(cik_map)} CIK mappings to {CIK_FILE}")
        return True
    except Exception as e:
        print(f"Error saving CIK map: {e}")
        return False

def get_cik(ticker: str) -> str:
    ticker = ticker.strip().upper()
    cik_map = load_cik_map()
    if ticker in cik_map:
        return cik_map[ticker]
    
    print(f"CIK not found for {ticker} in local file. Trying SEC API...")
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data = response.json()
            for item in data.values():
                if item.get('ticker') and item['ticker'].upper() == ticker:
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
    results.sort(key=lambda x: x['end'], reverse=True)
    return results[:limit]

def get_diluted_shares_from_facts(facts_data: Dict, form_type: Optional[str] = None, limit: int = 40) -> List[Dict]:
    tag = "WeightedAverageNumberOfDilutedSharesOutstanding"
    try:
        if 'facts' not in facts_data or 'us-gaap' not in facts_data['facts']:
            return []
        if tag not in facts_data['facts']['us-gaap']:
            return []
        units_data = facts_data['facts']['us-gaap'][tag]['units']
        for unit_key in units_data:
            if 'shares' in unit_key:
                period_map = {}
                for item in units_data[unit_key]:
                    if form_type and not item.get('form', '').startswith(form_type):
                        continue
                    if 'start' in item and 'end' in item:
                        key = (item['end'], item['start'])
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
                results = list(period_map.values())
                results.sort(key=lambda x: x['end'], reverse=True)
                return results[:limit]
    except Exception as e:
        print(f"Error getting diluted shares: {e}")
    return []

# ============================================
# 会計年度判定と四半期分類
# ============================================

# ★ 非支配持分考慮：純利益タグの優先順位（要件定義書 4.2①）
NET_INCOME_PRIORITY_TAGS = [
    'us-gaap:NetIncomeLossAvailableToCommonStockholders',
    'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic',
    'us-gaap:NetIncomeLossAttributableToParent',
    'us-gaap:NetIncomeLoss',  # フォールバック：連結純利益
]

def select_net_income_items(tag_data_map: Dict) -> List[Dict]:
    """
    非支配持分を考慮した純利益タグを優先順位に従って選択する（四半期10-Q用）。
    10-Qデータが最も多いタグを優先し、上位タグのデータ件数が
    フォールバック(NetIncomeLoss)の50%未満の場合はスキップする。
    """
    fallback_count = len(tag_data_map.get('us-gaap:NetIncomeLoss', []))
    threshold = max(4, fallback_count // 2)

    for tag in NET_INCOME_PRIORITY_TAGS:
        items = tag_data_map.get(tag, [])
        if not items:
            continue
        if tag == 'us-gaap:NetIncomeLoss' or len(items) >= threshold:
            print(f"  [NetIncome] Using tag: {tag} ({len(items)} items)")
            return items
        else:
            print(f"  [NetIncome] Skipping tag: {tag} ({len(items)} items < threshold {threshold})")
    return []

def select_net_income_annual(annual_data_by_tag: Dict) -> List[Dict]:
    """
    年次データから非支配持分考慮の純利益を優先順位に従って選択する（10-K用）。
    四半期用とは独立して最も多くの年次データを持つタグを選択する。
    """
    # 年次データが最も多いタグを優先（件数が同じなら優先順位順）
    best_tag = None
    best_items = []
    best_count = 0

    for tag in NET_INCOME_PRIORITY_TAGS:
        items = annual_data_by_tag.get(tag, [])
        if len(items) > best_count:
            best_count = len(items)
            best_tag = tag
            best_items = items

    if best_tag:
        print(f"  [NetIncome Annual] Using tag: {best_tag} ({best_count} items)")
        return best_items
    return []

def determine_fiscal_year_end(annual_data: List[Dict]) -> int:
    month_counts = {}
    for item in annual_data:
        if 'end' in item:
            end_date = datetime.strptime(item['end'], '%Y-%m-%d')
            month = end_date.month
            month_counts[month] = month_counts.get(month, 0) + 1
    if not month_counts:
        return 12
    return max(month_counts.items(), key=lambda x: x[1])[0]

def get_quarter_number(end_date: datetime, fiscal_end_month: int) -> int:
    end_month = end_date.month
    if end_month <= fiscal_end_month:
        offset = fiscal_end_month - end_month
    else:
        offset = fiscal_end_month + 12 - end_month
    if offset <= 1:
        return 4
    elif offset <= 4:
        return 3
    elif offset <= 7:
        return 2
    else:
        return 1

# ============================================
# メイン抽出関数
# ============================================
def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    try:
        cik = get_cik(ticker)
        print(f"CIK: {cik}")
        facts = fetch_company_facts(cik)
        if not facts:
            print(f"No facts data for {ticker}")
            return []
        
        required_tags = load_required_xbrl_tags()
        print(f"Required XBRL tags: {required_tags}")
        
        tag_data_map = {}
        for tag in required_tags:
            items = extract_value_from_facts(facts, tag, limit=years*8)  # ★ 大企業でも古い四半期が欠落しないよう拡張
            tag_data_map[tag] = items
            print(f"Extracted {len(items)} items for {tag}")
        
        # 年次データ抽出
        annual_data_by_tag = {}
        for tag, items in tag_data_map.items():
            annual_items = [item for item in items if item.get('form', '').startswith('10-K') and 'start' in item and 'end' in item and (datetime.strptime(item['end'], '%Y-%m-%d') - datetime.strptime(item['start'], '%Y-%m-%d')).days >= ANNUAL_DAYS_MIN]
            annual_data_by_tag[tag] = annual_items
        
        net_income_annual = select_net_income_annual(annual_data_by_tag)  # ★ 非支配持分考慮
        fiscal_end_month = determine_fiscal_year_end(net_income_annual)
        print(f"Detected fiscal year end month: {fiscal_end_month}")
        
        # 希薄化後株式数マップ
        diluted_shares_all = tag_data_map.get('us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding', [])
        diluted_map = {}
        for item in diluted_shares_all:
            if 'start' in item and 'end' in item:
                key = (item['end'], item['start'])
                diluted_map[key] = item['val']
        
        # 10-Q 四半期候補
        net_income_10q = select_net_income_items(tag_data_map)  # ★ 非支配持分考慮
        quarterly_candidates = []
        for q_item in net_income_10q:
            if not q_item.get('form', '').startswith('10-Q'):
                continue
            if 'start' not in q_item or 'end' not in q_item:
                continue
            start = datetime.strptime(q_item['start'], '%Y-%m-%d')
            end = datetime.strptime(q_item['end'], '%Y-%m-%d')
            days_diff = (end - start).days
            if QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX:
                quarterly_candidates.append({
                    'start': start, 'end': end, 'end_str': q_item['end'], 'start_str': q_item['start'],
                    'val': q_item['val'], 'unit': q_item['unit'], 'filed': q_item.get('filed', q_item['end']),
                    'days': days_diff
                })
        
        best_quarterly = {}
        for cand in quarterly_candidates:
            end_str = cand['end_str']
            if end_str not in best_quarterly or cand['days'] < best_quarterly[end_str]['days']:
                best_quarterly[end_str] = cand
        
        quarters_map = {}
        for end_str, cand in best_quarterly.items():
            if cand['end'].month > fiscal_end_month:
                fiscal_year = cand['end'].year + 1
            else:
                fiscal_year = cand['end'].year
            quarter_num = get_quarter_number(cand['end'], fiscal_end_month)
            key = (fiscal_year, quarter_num)
            
            if key not in quarters_map:
                quarters_map[key] = {
                    'filing_date': end_str, 'form': '10-Q', 'start': cand['start_str'],
                    'end': end_str, 'filed': cand['filed'], 'quarter': quarter_num, 'fiscal_year': fiscal_year
                }
            quarters_map[key]['net_income'] = {'value': cand['val'], 'unit': cand['unit']}
        
        # 他のタグ追加（10-Q）
        for tag in required_tags:
            if tag in ['us-gaap:NetIncomeLoss', 'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding']:
                continue
            for item in tag_data_map.get(tag, []):
                if not item.get('form', '').startswith('10-Q'):
                    continue
                if 'start' not in item or 'end' not in item:
                    continue
                start = datetime.strptime(item['start'], '%Y-%m-%d')
                end = datetime.strptime(item['end'], '%Y-%m-%d')
                days_diff = (end - start).days
                if not (QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX):
                    continue
                if end.month > fiscal_end_month:
                    fiscal_year = end.year + 1
                else:
                    fiscal_year = end.year
                quarter_num = get_quarter_number(end, fiscal_end_month)
                key = (fiscal_year, quarter_num)
                if key in quarters_map:
                    quarters_map[key][tag] = {'value': item['val'], 'unit': item['unit']}
        
        # 希薄化後株式数追加
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
            if end.month > fiscal_end_month:
                fiscal_year = end.year + 1
            else:
                fiscal_year = end.year
            quarter_num = get_quarter_number(end, fiscal_end_month)
            key = (fiscal_year, quarter_num)
            if key in quarters_map:
                quarters_map[key]['diluted_shares'] = {'value': item['val'], 'unit': item['unit']}
        
        # ★★★ SBC YTD累計値を _ytd_ プレフィックスで quarters_map に追加 ★★★
        # pipeline.py でYTD差分計算するために、生のYTD累計値を別キーで保存
        SBC_YTD_TAGS = [
            'us-gaap:ShareBasedCompensation',
            'us-gaap:AllocatedShareBasedCompensationExpense',
            'us-gaap:EmployeeBenefitsAndShareBasedCompensation',
            'us-gaap:StockBasedCompensation',
            'us-gaap:ShareBasedCompensationExpense',
            'us-gaap:RestrictedStockExpense',
        ]
        for tag in SBC_YTD_TAGS:
            for item in tag_data_map.get(tag, []):
                if 'start' not in item or 'end' not in item:
                    continue
                start = datetime.strptime(item['start'], '%Y-%m-%d')
                end   = datetime.strptime(item['end'],   '%Y-%m-%d')
                days  = (end - start).days
                # YTD累計値（120日超）のみ対象
                if days <= QUARTER_DAYS_MAX:
                    continue
                fy   = end.year if end.month <= fiscal_end_month else end.year + 1
                qnum = get_quarter_number(end, fiscal_end_month)
                key  = (fy, qnum)
                if key not in quarters_map:
                    continue
                ytd_key = f'_ytd_{tag}'
                # 既存値より大きければ上書き（最新・最大のYTD値を保持）
                existing = quarters_map[key].get(ytd_key, {}).get('value', 0)
                if item['val'] > existing:
                    quarters_map[key][ytd_key] = {'value': item['val'], 'unit': item['unit']}

                # 税費用・pretax_income計算（省略せず完全実装）
        tax_tag_candidates = [
            'us-gaap:IncomeTaxExpenseBenefit', 'us-gaap:IncomeTaxExpenseBenefitContinuingOperations',
            'us-gaap:ProvisionForIncomeTaxes', 'us-gaap:IncomeTaxExpenseBenefitFromContinuingOperations'
        ]
        for key, data in quarters_map.items():
            for tax_tag in tax_tag_candidates:
                if tax_tag in data:
                    data['tax_expense'] = data[tax_tag]
                    break
            net = normalize_value(data.get('net_income'))
            tax = normalize_value(data.get('tax_expense'))
            if net and tax is not None:
                data['pretax_income'] = {'value': net + tax, 'unit': 'USD'}
        
        # ★★★ 10-KからQ4を計算（旧版ロジックベース） ★★★
        # Q1〜Q3が10-Qで取得済みの fiscal_year について、年次(10-K)からQ4を計算
        fiscal_years_with_q1q3 = set(k[0] for k in quarters_map.keys() if k[1] in (1,2,3))
        for fiscal_year in fiscal_years_with_q1q3:
            q1_key = (fiscal_year, 1)
            q2_key = (fiscal_year, 2)
            q3_key = (fiscal_year, 3)
            q4_key = (fiscal_year, 4)

            if q4_key in quarters_map:
                continue  # 既にQ4データあり

            if not (q1_key in quarters_map and q2_key in quarters_map and q3_key in quarters_map):
                print(f"  Warning: Missing Q1-Q3 for FY{fiscal_year}")
                continue

            # この fiscal_year に対応する10-Kを探す
            net_income_annual_items = select_net_income_annual(annual_data_by_tag)  # ★ 非支配持分考慮
            target_k_item = None
            for item in net_income_annual_items:
                item_end = datetime.strptime(item['end'], '%Y-%m-%d')
                item_fy = item_end.year + 1 if item_end.month > fiscal_end_month else item_end.year
                if item_fy == fiscal_year:
                    target_k_item = item
                    break

            if not target_k_item:
                print(f"  Warning: No 10-K found for FY{fiscal_year}")
                continue

            # Q4純利益 = 年次 - Q1 - Q2 - Q3
            annual_net = target_k_item['val']
            ni_q1 = normalize_value(quarters_map[q1_key].get('net_income', {'value': 0}))
            ni_q2 = normalize_value(quarters_map[q2_key].get('net_income', {'value': 0}))
            ni_q3 = normalize_value(quarters_map[q3_key].get('net_income', {'value': 0}))
            q4_net = annual_net - ni_q1 - ni_q2 - ni_q3

            # 希薄化後株式数：10-Kの年次値を優先、なければQ3を使用
            diluted_val = 0
            for d_item in diluted_shares_all:
                if d_item.get('form', '').startswith('10-K') and d_item.get('end') == target_k_item['end']:
                    diluted_val = d_item['val']
                    break
            if diluted_val == 0:
                diluted_val = normalize_value(quarters_map[q3_key].get('diluted_shares', {'value': 0}))

            # Q4データを作成
            q4_data = {
                'filing_date': target_k_item['end'],
                'form': '10-K',
                'net_income': {'value': q4_net, 'unit': 'USD'},
                'diluted_shares': {'value': diluted_val, 'unit': 'shares'},
                'start': quarters_map[q3_key]['end'],
                'end': target_k_item['end'],
                'filed': target_k_item.get('filed', target_k_item['end']),
                'quarter': 4,
                'fiscal_year': fiscal_year
            }
            quarters_map[q4_key] = q4_data
            print(f"  [Q4計算] FY{fiscal_year} Q4 end={target_k_item['end']}: net={q4_net/1e6:.1f}M (annual={annual_net/1e6:.1f}M - Q1-Q3={(ni_q1+ni_q2+ni_q3)/1e6:.1f}M)")

            # Q4の税費用 = 年次 - Q1 - Q2 - Q3（差し引き計算）
            tax_tag_candidates_q4 = [
                'us-gaap:IncomeTaxExpenseBenefit',
                'us-gaap:IncomeTaxExpenseBenefitContinuingOperations',
                'us-gaap:ProvisionForIncomeTaxes',
                'us-gaap:IncomeTaxExpenseBenefitFromContinuingOperations'
            ]
            for tax_tag in tax_tag_candidates_q4:
                annual_tax_items = annual_data_by_tag.get(tax_tag, [])
                annual_tax_val = None
                for item in annual_tax_items:
                    if item['end'] == target_k_item['end']:
                        annual_tax_val = item
                        break
                if not annual_tax_val:
                    continue
                v_annual = annual_tax_val['val']
                v_q1 = normalize_value(quarters_map[q1_key].get(tax_tag))
                v_q2 = normalize_value(quarters_map[q2_key].get(tax_tag))
                v_q3 = normalize_value(quarters_map[q3_key].get(tax_tag))
                v_q4 = v_annual - v_q1 - v_q2 - v_q3
                q4_data[tax_tag] = {'value': v_q4, 'unit': annual_tax_val['unit']}
                break  # 最初に見つかった税費用タグを使用

            # Q4の調整項目タグ = 年次値 - Q1値 - Q2値 - Q3値（差し引き計算）
            # SBC・R&D等はYTD累計で報告されるため、差し引きでQ4単体値を得る
            skip_tags = {
                'us-gaap:NetIncomeLoss', 'us-gaap:NetIncomeLossAttributableToParent',
                'us-gaap:NetIncomeLossAvailableToCommonStockholders',
                'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic',
                'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding',
                'us-gaap:EarningsPerShareDiluted',
                'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit',
                'us-gaap:IncomeFromContinuingOperationsBeforeIncomeTaxes',
                'us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefit',
                'us-gaap:IncomeLossBeforeIncomeTaxExpenseBenefitAndExtraordinaryItems',
                'us-gaap:IncomeBeforeTax',
            } | set(tax_tag_candidates_q4)
            for tag in required_tags:
                if tag in skip_tags:
                    continue
                annual_tag_items = annual_data_by_tag.get(tag, [])
                annual_tag_val = None
                for item in annual_tag_items:
                    if item['end'] == target_k_item['end']:
                        annual_tag_val = item
                        break
                if not annual_tag_val:
                    continue
                # Q1〜Q3の値を取得（normalize_valueで単位統一）
                v_annual = annual_tag_val['val']
                v_q1 = normalize_value(quarters_map[q1_key].get(tag))
                v_q2 = normalize_value(quarters_map[q2_key].get(tag))
                v_q3 = normalize_value(quarters_map[q3_key].get(tag))
                v_q4 = v_annual - v_q1 - v_q2 - v_q3
                if v_q4 != 0:
                    q4_data[tag] = {'value': v_q4, 'unit': annual_tag_val['unit']}

        # quarterly_list 作成（ここがエラー原因だった部分）
        quarterly_list = []
        for (fiscal_year, quarter), data in sorted(quarters_map.items()):
            if 'net_income' in data and 'diluted_shares' in data:
                data['fiscal_year'] = fiscal_year
                data['quarter'] = quarter
                quarterly_list.append(data)
        
        quarterly_list.sort(key=lambda x: x['filing_date'], reverse=True)
        print(f"\n{ticker}: {len(quarterly_list)}件の四半期データを取得")
        return quarterly_list
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        import traceback
        traceback.print_exc()
        return []

def normalize_value(value_dict: Optional[Dict]) -> float:
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
    return value

# テスト用
if __name__ == "__main__":
    ticker = "TSLA"
    print(f"Testing data extraction for {ticker}...")
    data = extract_quarterly_facts(ticker, years=5)
    if data:
        print(f"\nSuccessfully extracted {len(data)} quarters:")
        for i, quarter in enumerate(data[:5]):
            print(f"\nQuarter {i+1}: {quarter['filing_date']} ({quarter.get('form', 'unknown')})")
            print(f"  Net Income: {normalize_value(quarter.get('net_income')):,.0f}")
            print(f"  Revenue: {normalize_value(quarter.get('us-gaap:Revenues') or quarter.get('us-gaap:RevenueFromContractWithCustomer') or {}):,.0f}")
    else:
        print("No data extracted")
