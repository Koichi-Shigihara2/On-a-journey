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
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
CIK_FILE = os.path.join(CONFIG_DIR, "cik_lookup.csv")
ADJUSTMENT_ITEMS_FILE = os.path.join(CONFIG_DIR, "adjustment_items.json")

HEADERS = {
    'User-Agent': 'jamablue01@gmail.com',
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

    # ★★★ ここを追加 ★★★ 売上高を必ず取得（SBC/売上高比率のため）
    tags.add("us-gaap:Revenues")
    tags.add("us-gaap:RevenueFromContractWithCustomer")
    tags.add("us-gaap:NetSales")
    tags.add("us-gaap:TotalRevenue")
    tags.add("us-gaap:SalesRevenueNet")

    return list(tags)

# ============================================
# CIKマップ管理（以下は元のまま）
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
                if row['ticker'] and row['cik']:
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
                if item['ticker'] and item['ticker'].upper() == ticker:
                    cik = str(item['cik_str']).zfill(10)
                    cik_map[ticker] = cik
                    save_cik_map(cik_map)
                    return cik
    except Exception as e:
        print(f"SEC API lookup failed: {e}")
    raise Exception(f"CIK not found for {ticker}. Please add to {CIK_FILE}")

# ============================================
# SEC Company Facts APIからデータ取得（以下は元のまま）
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
# 会計年度判定と四半期分類（以下は元のまま）
# ============================================
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
# メイン抽出関数（以下は元のまま）
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
            items = extract_value_from_facts(facts, tag, form_type=None, limit=years*6)
            tag_data_map[tag] = items
            print(f"Extracted {len(items)} items for {tag}")
        
        # （以下は元のコードと同じなので省略せず全文をコピーしていますが、ここではスペース節約のため「元のまま」とします。実際はあなたの元のextract_key_facts.pyをそのまま残して、上記のload_required_xbrl_tags関数だけ置き換えてください）
        # ...（残りの関数はすべて元のまま）...

        # 最終的なリストを返す部分まで元のコード通り
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
    else:
        return value

# テスト用メイン関数（元のまま）
if __name__ == "__main__":
    ticker = "TSLA"
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
            revenue = normalize_value(quarter.get('us-gaap:Revenues') or quarter.get('us-gaap:RevenueFromContractWithCustomer') or {})
            if revenue > 0:
                print(f"  Revenue: {revenue:,.0f} USD")
    else:
        print("No data extracted")
