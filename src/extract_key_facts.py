"""
SEC EDGARから企業の財務データを抽出するモジュール（最終版）
- CIKマップファイルから銘柄のCIKを取得
- SECのCompany Facts APIから直接XBRLデータを取得
- 期間の長さ（60〜100日または360〜370日）で四半期/年次データをフィルタリング
- 複数クラス株式（PLTRなど）の希薄化後株式数を合算
"""
import os
import csv
import json
import requests
import pandas as pd
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

QUARTER_DAYS_MIN = 60
QUARTER_DAYS_MAX = 100
YEAR_DAYS_MIN = 360
YEAR_DAYS_MAX = 370

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
    """CIKマップをCSVに保存"""
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(CIK_FILE, 'w', encoding='utf-8') as f:
            f.write("ticker,cik,name,sector\n")
            for ticker, cik in sorted(cik_map.items()):
                f.write(f"{ticker},{cik},,\n")
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

def fetch_company_facts(cik: str) -> Dict:
    """SEC Company Facts APIから企業の全XBRLファクトを取得"""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    print(f"Fetching company facts from {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching company facts: {e}")
        return {}

def extract_value_from_facts(facts_data: Dict, us_gaap_tag: str, form_type: str = None, limit: int = 40) -> List[Dict]:
    """Company Factsから特定タグの時系列データを抽出（四半期または年次）"""
    results = []
    try:
        if 'facts' not in facts_data or 'us-gaap' not in facts_data['facts']:
            return results
        if us_gaap_tag not in facts_data['facts']['us-gaap']:
            return results
        
        units_data = facts_data['facts']['us-gaap'][us_gaap_tag]['units']
        for unit_key in units_data:
            if 'USD' in unit_key or 'shares' in unit_key:
                for item in units_data[unit_key]:
                    # form_typeが指定されていればフィルタ、なければ全て取得
                    if form_type and not item.get('form', '').startswith(form_type):
                        continue
                    
                    if 'start' in item and 'end' in item:
                        start = datetime.strptime(item['start'], '%Y-%m-%d')
                        end = datetime.strptime(item['end'], '%Y-%m-%d')
                        days_diff = (end - start).days
                        
                        # 四半期（60-100日）または年次（360-370日）を許可
                        if (QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX) or (YEAR_DAYS_MIN <= days_diff <= YEAR_DAYS_MAX):
                            results.append({
                                'end': item['end'],
                                'val': item['val'],
                                'filed': item.get('filed'),
                                'form': item.get('form'),
                                'unit': unit_key,
                                'start': item['start']
                            })
                        else:
                            print(f"      Skipping {us_gaap_tag} for {item['end']} (period {days_diff} days)")
                break
    except Exception as e:
        print(f"Error extracting {us_gaap_tag}: {e}")
    results.sort(key=lambda x: x['end'], reverse=True)
    return results[:limit]

def get_diluted_shares_from_facts(facts_data: Dict, form_type: str = None, limit: int = 40) -> List[Dict]:
    """希薄化後株式数を取得（複数クラスがある場合は合算）"""
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
                        start = datetime.strptime(item['start'], '%Y-%m-%d')
                        end = datetime.strptime(item['end'], '%Y-%m-%d')
                        days_diff = (end - start).days
                        if (QUARTER_DAYS_MIN <= days_diff <= QUARTER_DAYS_MAX) or (YEAR_DAYS_MIN <= days_diff <= YEAR_DAYS_MAX):
                            key = item['end']
                            if key not in period_map:
                                period_map[key] = {
                                    'end': key,
                                    'val': 0,
                                    'filed': item.get('filed'),
                                    'form': item.get('form'),
                                    'unit': unit_key,
                                    'start': item['start']
                                }
                            period_map[key]['val'] += item.get('val', 0)
                results = list(period_map.values())
                results.sort(key=lambda x: x['end'], reverse=True)
                return results[:limit]
    except Exception as e:
        print(f"Error getting diluted shares: {e}")
    return []

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """四半期データを取得（SEC API直アクセス＋期間フィルタリング）"""
    try:
        cik = get_cik(ticker)
        print(f"CIK: {cik}")
        
        facts = fetch_company_facts(cik)
        if not facts:
            print(f"No facts data for {ticker}")
            return []
        
        # form_type=None で10-Qと10-Kの両方を取得
        net_income_data = extract_value_from_facts(facts, 'NetIncomeLoss', form_type=None, limit=years*6)  # 少し多めに
        diluted_shares_data = get_diluted_shares_from_facts(facts, form_type=None, limit=years*6)
        if not diluted_shares_data:
            diluted_shares_data = extract_value_from_facts(facts, 'WeightedAverageNumberOfDilutedSharesOutstanding', form_type=None, limit=years*6)
        basic_shares_data = extract_value_from_facts(facts, 'WeightedAverageNumberOfSharesOutstandingBasic', form_type=None, limit=years*6)
        pretax_data = extract_value_from_facts(facts, 'IncomeLossFromContinuingOperationsBeforeIncomeTaxes', form_type=None, limit=years*6)
        tax_data = extract_value_from_facts(facts, 'IncomeTaxExpenseBenefit', form_type=None, limit=years*6)
        sbc_data = extract_value_from_facts(facts, 'ShareBasedCompensation', form_type=None, limit=years*6)
        restructuring_data = extract_value_from_facts(facts, 'RestructuringCharges', form_type=None, limit=years*6)
        
        quarterly_map = {}
        
        for item in net_income_data:
            end_date = item['end']
            if end_date not in quarterly_map:
                quarterly_map[end_date] = {
                    'filing_date': end_date,
                    'form': item.get('form', '10-Q'),
                    'net_income': {'value': item['val'], 'unit': item['unit']}
                }
            else:
                quarterly_map[end_date]['net_income'] = {'value': item['val'], 'unit': item['unit']}
        
        for item in diluted_shares_data:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['diluted_shares'] = {'value': item['val'], 'unit': item['unit']}
            else:
                quarterly_map[end_date] = {
                    'filing_date': end_date,
                    'form': item.get('form', '10-Q'),
                    'diluted_shares': {'value': item['val'], 'unit': item['unit']}
                }
        
        for item in basic_shares_data:
            end_date = item['end']
            if end_date in quarterly_map and 'diluted_shares' not in quarterly_map[end_date]:
                quarterly_map[end_date]['diluted_shares'] = {'value': item['val'], 'unit': item['unit']}
        
        for item in pretax_data:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['pretax_income'] = {'value': item['val'], 'unit': item['unit']}
        
        for item in tax_data:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['tax_expense'] = {'value': item['val'], 'unit': item['unit']}
        
        for item in sbc_data:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['sbc'] = {'value': item['val'], 'unit': item['unit']}
        
        for item in restructuring_data:
            end_date = item['end']
            if end_date in quarterly_map:
                quarterly_map[end_date]['restructuring'] = {'value': item['val'], 'unit': item['unit']}
        
        quarterly_list = []
        for end_date, data in sorted(quarterly_map.items(), reverse=True):
            if 'net_income' in data and 'diluted_shares' in data:
                quarterly_list.append(data)
                net_val = data['net_income']['value']
                shr_val = data['diluted_shares']['value']
                print(f"  ✓ {end_date} ({data['form']}): net_income={net_val:,.0f}, diluted_shares={shr_val:,.0f}")
            else:
                missing = []
                if 'net_income' not in data:
                    missing.append('net_income')
                if 'diluted_shares' not in data:
                    missing.append('diluted_shares')
                print(f"  ✗ {end_date}: missing {', '.join(missing)}")
        
        print(f"\n{ticker}: {len(quarterly_list)}件のデータを取得")
        return quarterly_list
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        import traceback
        traceback.print_exc()
        return []

def normalize_value(value_dict: Optional[Dict]) -> float:
    """単位正規化（すべてUSD absolute valueに統一）"""
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

def main():
    ticker = "PLTR"
    print(f"Testing data extraction for {ticker}...")
    data = extract_quarterly_facts(ticker, years=5)
    if data:
        print(f"\nSuccessfully extracted {len(data)} quarters:")
        for i, quarter in enumerate(data[:5]):
            print(f"\nQuarter {i+1}: {quarter['filing_date']} ({quarter['form']})")
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
