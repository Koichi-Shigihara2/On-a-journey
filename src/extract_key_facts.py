"""
SEC EDGARから企業の財務データを抽出するモジュール（SEC API直アクセス・高精度版）
- YTDとQuarterの混同防止（fpフィールド活用）
- 複数クラス株式の合算対応
- レート制限対策（リトライ＋スリープ）
"""
import os
import csv
import json
import time
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

# ============================================
# 定数設定
# ============================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
CIK_FILE = os.path.join(CONFIG_DIR, "cik_lookup.csv")

HEADERS = {
    'User-Agent': 'jamablue01@gmail.com',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}

# レート制限対策：リクエスト間隔（秒）
REQUEST_INTERVAL = 0.2  # 5 req/sec 以下に抑える

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
# SEC Company Facts APIからデータ取得（リトライ付き）
# ============================================
def fetch_company_facts_with_retry(cik: str, max_retries: int = 3) -> Dict:
    """
    リトライとレート制限対策付きでCompany Factsを取得
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    print(f"Fetching company facts from {url}")
    
    for attempt in range(max_retries):
        try:
            # レート制限対策：リクエスト間隔を空ける
            if attempt > 0:
                wait_time = 2 ** attempt  # 指数バックオフ
                print(f"Retry {attempt}/{max_retries} after {wait_time}s...")
                time.sleep(wait_time)
            else:
                time.sleep(REQUEST_INTERVAL)
            
            response = requests.get(url, headers=HEADERS, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"Rate limited (429). Retrying...")
                time.sleep(5)  # レート制限時は長めに待つ
                continue
            else:
                print(f"HTTP {response.status_code}: {response.text[:100]}")
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt == max_retries - 1:
                raise
    
    return {}

def is_quarterly_item(item: Dict) -> bool:
    """
    四半期データかどうかを判定
    - fp フィールドが 'Q1', 'Q2', 'Q3' のものを四半期とみなす
    - FY（年度）は除外
    - start と end の差分が約90日（±10日）のものも四半期とみなす（fpがない場合のフォールバック）
    """
    # fp フィールドによる判定（最も信頼性が高い）
    fp = item.get('fp')
    if fp in ['Q1', 'Q2', 'Q3']:
        return True
    if fp == 'FY':
        return False
    
    # fpがない場合、期間の長さで判定（フォールバック）
    start = item.get('start')
    end = item.get('end')
    if start and end:
        try:
            start_date = datetime.strptime(start, '%Y-%m-%d')
            end_date = datetime.strptime(end, '%Y-%m-%d')
            days = (end_date - start_date).days
            # 四半期は通常80-100日程度
            if 80 <= days <= 100:
                return True
        except:
            pass
    
    return False

def extract_quarterly_values(facts_data: Dict, us_gaap_tag: str) -> List[Dict]:
    """
    Company Factsから四半期データのみを抽出
    複数クラスがある場合は合算する
    """
    results = []
    try:
        if 'facts' not in facts_data or 'us-gaap' not in facts_data['facts']:
            return results
        
        if us_gaap_tag not in facts_data['facts']['us-gaap']:
            return results
        
        units_data = facts_data['facts']['us-gaap'][us_gaap_tag]['units']
        
        for unit_key, items in units_data.items():
            if 'USD' not in unit_key and 'shares' not in unit_key:
                continue
            
            for item in items:
                # 10-Q かつ 四半期データのみ
                if item.get('form', '').startswith('10-Q') and is_quarterly_item(item):
                    results.append({
                        'end': item.get('end'),
                        'start': item.get('start'),
                        'val': item.get('val'),
                        'filed': item.get('filed'),
                        'form': item.get('form'),
                        'fp': item.get('fp'),
                        'unit': unit_key
                    })
    except Exception as e:
        print(f"Error extracting {us_gaap_tag}: {e}")
    
    return results

def get_combined_diluted_shares(facts_data: Dict) -> List[Dict]:
    """
    希薄化後株式数を取得（複数クラスがある場合は合算）
    Returns:
        List[Dict]: 期末日ごとの合算値
    """
    # 通常の希薄化後株式数タグからデータ取得
    shares_items = extract_quarterly_values(facts_data, 'WeightedAverageNumberOfDilutedSharesOutstanding')
    
    # 期末日でグループ化して合算
    combined = {}
    for item in shares_items:
        end_date = item['end']
        if end_date not in combined:
            combined[end_date] = {
                'end': end_date,
                'start': item.get('start'),
                'val': 0,
                'filed': item.get('filed'),
                'form': item.get('form'),
                'fp': item.get('fp'),
                'unit': item['unit']
            }
        combined[end_date]['val'] += item['val']
    
    # 合算結果をリストに変換
    result = list(combined.values())
    result.sort(key=lambda x: x['end'], reverse=True)
    
    # デバッグ出力
    if len(shares_items) != len(result):
        print(f"    Combined {len(shares_items)} diluted share entries into {len(result)} quarters")
    
    return result

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """
    四半期データを取得（YTD混同防止・複数クラス合算・レート制限対策版）
    """
    try:
        # CIK取得
        cik = get_cik(ticker)
        print(f"CIK: {cik}")
        
        # Company Facts取得（リトライ付き）
        facts = fetch_company_facts_with_retry(cik)
        if not facts:
            print(f"No facts data for {ticker}")
            return []
        
        # 純利益（四半期のみ）
        net_income_items = extract_quarterly_values(facts, 'NetIncomeLoss')
        if not net_income_items:
            net_income_items = extract_quarterly_values(facts, 'NetIncomeLossAttributableToParent')
        
        # 希薄化後株式数（複数クラス合算）
        diluted_shares_items = get_combined_diluted_shares(facts)
        if not diluted_shares_items:
            # フォールバック：Basic Shares
            diluted_shares_items = extract_quarterly_values(facts, 'WeightedAverageNumberOfSharesOutstandingBasic')
        
        # 税引前利益
        pretax_items = extract_quarterly_values(facts, 'IncomeLossFromContinuingOperationsBeforeIncomeTaxes')
        
        # 税金費用
        tax_items = extract_quarterly_values(facts, 'IncomeTaxExpenseBenefit')
        
        # 株式報酬
        sbc_items = extract_quarterly_values(facts, 'ShareBasedCompensation')
        
        # 期間をキーにマップ作成
        quarterly_map = {}
        
        # Net Income
        for item in net_income_items:
            end_date = item['end']
            if end_date not in quarterly_map:
                quarterly_map[end_date] = {
                    'filing_date': end_date,
                    'start_date': item.get('start'),
                    'form': '10-Q',
                    'fp': item.get('fp')
                }
            quarterly_map[end_date]['net_income'] = {
                'value': item['val'],
                'unit': item['unit'],
                'filed': item.get('filed')
            }
        
        # Diluted Shares
        for item in diluted_shares_items:
            end_date = item['end']
            if end_date not in quarterly_map:
                quarterly_map[end_date] = {
                    'filing_date': end_date,
                    'start_date': item.get('start'),
                    'form': '10-Q',
                    'fp': item.get('fp')
                }
            quarterly_map[end_date]['diluted_shares'] = {
                'value': item['val'],
                'unit': item['unit'],
                'filed': item.get('filed')
            }
        
        # Pretax Income
        for item in pretax_items:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['pretax_income'] = {
                    'value': item['val'],
                    'unit': item['unit']
                }
        
        # Tax Expense
        for item in tax_items:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['tax_expense'] = {
                    'value': item['val'],
                    'unit': item['unit']
                }
        
        # SBC
        for item in sbc_items:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['sbc'] = {
                    'value': item['val'],
                    'unit': item['unit']
                }
        
        # リストに変換し、必須データが揃っているものだけ抽出
        quarterly_list = []
        for end_date, data in sorted(quarterly_map.items(), reverse=True):
            if 'net_income' in data and 'diluted_shares' in data:
                quarterly_list.append(data)
                print(f"  ✓ {end_date} (Q{data.get('fp', '?')}): net_income={data['net_income']['value']:,.0f}, diluted_shares={data['diluted_shares']['value']:,.0f}")
            else:
                missing = []
                if 'net_income' not in data:
                    missing.append('net_income')
                if 'diluted_shares' not in data:
                    missing.append('diluted_shares')
                print(f"  ✗ {end_date}: missing {', '.join(missing)}")
        
        # 指定年数分に制限
        quarterly_list = quarterly_list[:years*4]
        
        print(f"\n{ticker}: {len(quarterly_list)}件の四半期データを取得")
        return quarterly_list
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        import traceback
        traceback.print_exc()
        return []

def normalize_value(value_dict: Optional[Dict]) -> float:
    """単位正規化"""
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

# ============================================
# テスト用メイン関数
# ============================================
def main():
    """テスト実行用"""
    ticker = "PLTR"
    print(f"Testing data extraction for {ticker}...")
    
    data = extract_quarterly_facts(ticker, years=5)
    
    if data:
        print(f"\nSuccessfully extracted {len(data)} quarters:")
        for i, quarter in enumerate(data[:5]):
            print(f"\nQuarter {i+1}: {quarter['filing_date']} (Q{quarter.get('fp', '?')})")
            net = normalize_value(quarter.get('net_income'))
            shares = normalize_value(quarter.get('diluted_shares'))
            print(f"  Net Income: {net:,.0f} USD")
            print(f"  Diluted Shares: {shares:,.0f}")
            if shares > 0:
                eps = net / shares
                print(f"  Implied EPS: {eps:.4f} USD")
    else:
        print("No data extracted")

if __name__ == "__main__":
    main()
