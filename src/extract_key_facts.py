"""
SEC EDGARから企業の財務データを抽出するモジュール
- CIKマップファイルから銘柄のCIKを取得
- HTTP/2問題を回避するための設定
- XBRLデータから四半期財務諸表を抽出
"""
import os
import ssl
import csv
import json
import urllib3
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

# ============================================
# ネットワーク設定（HTTP/2問題の回避）
# ============================================
# SSL警告を無効化
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 環境変数でHTTP/2を完全に無効化
os.environ["HTTP2"] = "0"
os.environ["HTTPX_HTTP2"] = "0"
os.environ["NO_PROXY"] = "sec.gov,www.sec.gov"
os.environ["no_proxy"] = "sec.gov,www.sec.gov"

# SSLコンテキストの設定
try:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED
except Exception as e:
    print(f"SSL context creation warning: {e}")

# edgarライブラリのインポート（設定後）
from edgar import Company, set_identity

# ============================================
# 定数設定
# ============================================
set_identity("jamablue01@gmail.com")

# パス設定
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
CIK_FILE = os.path.join(CONFIG_DIR, "cik_lookup.csv")

# ============================================
# CIKマップ管理
# ============================================
def load_cik_map() -> Dict[str, str]:
    """
    CIKマップをCSVから読み込む
    Returns:
        Dict[str, str]: ティッカー→CIKの辞書
    """
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
    """
    CIKマップをCSVに保存
    Args:
        cik_map: 保存するCIKマップ
    Returns:
        bool: 成功したかどうか
    """
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
    """
    ティッカーからCIKを取得
    Args:
        ticker: 銘柄ティッカー（例: 'PLTR'）
    Returns:
        str: 10桁のCIK番号
    Raises:
        Exception: CIKが見つからない場合
    """
    ticker = ticker.strip().upper()
    cik_map = load_cik_map()
    
    if ticker in cik_map:
        return cik_map[ticker]
    
    # SEC APIから直接取得を試みる
    print(f"CIK not found for {ticker} in local file. Trying SEC API...")
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {'User-Agent': 'jamablue01@gmail.com'}
        response = requests.get(url, headers=headers, timeout=10)
        
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
    
    raise Exception(
        f"CIK not found for {ticker}. Please add to {CIK_FILE}\n"
        f"Format: {ticker},[10-digit CIK],Company Name\n"
        f"Example: PLTR,0001321655,Palantir Technologies Inc."
    )

# ============================================
# データ抽出機能
# ============================================
def fetch_filings(ticker: str, count: int = 40) -> List:
    """
    10-Q/10-K取得
    Args:
        ticker: 銘柄ティッカー
        count: 取得するファイリング数
    Returns:
        List: ファイリングオブジェクトのリスト
    """
    print(f"Fetching filings for {ticker}...")
    
    try:
        cik = get_cik(ticker)
        print(f"CIK: {cik}")
        
        company = Company(cik)
        filings = company.get_filings(form=["10-Q", "10-K"])
        print(f"Found {len(filings)} total filings")
        
        filing_list = []
        for filing in filings:
            filing_list.append(filing)
        
        for i, filing in enumerate(filing_list[:5]):
            try:
                filing_date = getattr(filing, 'filing_date', 'unknown')
                form = getattr(filing, 'form', 'unknown')
                print(f"  {i+1}: {filing_date} - {form}")
            except Exception as e:
                print(f"  Error accessing filing {i}: {e}")
        
        return filing_list[:count]
        
    except Exception as e:
        print(f"Error fetching filings for {ticker}: {e}")
        return []

def safe_get_xbrl_value(xbrl, tag: str) -> Optional[Dict[str, Any]]:
    """
    XBRLから安全に値を取得
    Args:
        xbrl: XBRLオブジェクト
        tag: 取得するタグ名
    Returns:
        Optional[Dict]: 値と単位を含む辞書
    """
    try:
        df = xbrl.to_pandas(tag)
        if df is not None and not df.empty:
            latest = df.iloc[-1]
            return {
                "value": float(latest["value"]),
                "unit": latest.get("unit", "USD"),
                "period": latest.get("period", {}),
                "filed": latest.get("filed", None)
            }
    except Exception:
        pass
    return None

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """
    四半期データを取得（過去10年分）
    Args:
        ticker: 銘柄ティッカー
        years: 取得する年数
    Returns:
        List[Dict]: 四半期データのリスト
    """
    try:
        filings = fetch_filings(ticker, count=years * 4)
        if not filings:
            print(f"No filings found for {ticker}")
            return []
        
        quarterly_data = []
        for i, filing in enumerate(filings):
            try:
                print(f"\nProcessing filing {i+1}/{len(filings)}: {getattr(filing, 'filing_date', 'unknown')} ({getattr(filing, 'form', 'unknown')})")
                
                xbrl = filing.xbrl()
                if not xbrl:
                    print("  No XBRL data available")
                    continue
                
                period_data = {
                    "filing_date": str(getattr(filing, 'filing_date', 'unknown')),
                    "form": str(getattr(filing, 'form', 'unknown')),
                    "accession_no": str(getattr(filing, 'accession_no', 'unknown'))
                }
                
                # Net Income
                net_income = safe_get_xbrl_value(xbrl, "us-gaap:NetIncomeLoss")
                if net_income:
                    period_data["net_income"] = net_income
                    print(f"  Net Income: {net_income['value']:,.0f} {net_income['unit']}")
                
                if "net_income" not in period_data:
                    net_income_parent = safe_get_xbrl_value(xbrl, "us-gaap:NetIncomeLossAttributableToParent")
                    if net_income_parent:
                        period_data["net_income"] = net_income_parent
                        print(f"  Net Income (Parent): {net_income_parent['value']:,.0f} {net_income_parent['unit']}")
                
                # Diluted Shares
                diluted_shares = safe_get_xbrl_value(xbrl, "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding")
                if diluted_shares:
                    period_data["diluted_shares"] = diluted_shares
                    print(f"  Diluted Shares: {diluted_shares['value']:,.0f} {diluted_shares['unit']}")
                
                if "diluted_shares" not in period_data:
                    basic_shares = safe_get_xbrl_value(xbrl, "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic")
                    if basic_shares:
                        period_data["diluted_shares"] = basic_shares
                        print(f"  Basic Shares (used as fallback): {basic_shares['value']:,.0f} {basic_shares['unit']}")
                
                # Tax Expense
                tax_expense = safe_get_xbrl_value(xbrl, "us-gaap:IncomeTaxExpenseBenefit")
                if tax_expense:
                    period_data["tax_expense"] = tax_expense
                    print(f"  Tax Expense: {tax_expense['value']:,.0f} {tax_expense['unit']}")
                
                # Pretax Income
                pretax_income = safe_get_xbrl_value(xbrl, "us-gaap:IncomeLossBeforeEquityMethodInvestmentsIncomeTax")
                if not pretax_income:
                    pretax_income = safe_get_xbrl_value(xbrl, "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes")
                if pretax_income:
                    period_data["pretax_income"] = pretax_income
                    print(f"  Pretax Income: {pretax_income['value']:,.0f} {pretax_income['unit']}")
                
                # SBC
                sbc = safe_get_xbrl_value(xbrl, "us-gaap:ShareBasedCompensation")
                if sbc:
                    period_data["sbc"] = sbc
                    print(f"  SBC: {sbc['value']:,.0f} {sbc['unit']}")
                
                # Restructuring
                restructuring = safe_get_xbrl_value(xbrl, "us-gaap:RestructuringCharges")
                if restructuring:
                    period_data["restructuring"] = restructuring
                
                if "net_income" in period_data and "diluted_shares" in period_data:
                    quarterly_data.append(period_data)
                    print(f"  ✓ Added to results")
                else:
                    missing = []
                    if "net_income" not in period_data:
                        missing.append("net_income")
                    if "diluted_shares" not in period_data:
                        missing.append("diluted_shares")
                    print(f"  ✗ Missing required data: {', '.join(missing)}")
                
            except Exception as e:
                print(f"  Error processing filing: {e}")
                continue
        
        print(f"\n{ticker}: {len(quarterly_data)}件の四半期データを取得")
        return quarterly_data
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        return []

def normalize_value(value_dict: Optional[Dict]) -> float:
    """
    単位正規化（すべてUSD absolute valueに統一）
    Args:
        value_dict: {"value": 数値, "unit": "USD"|"thousands"|"millions"|"billions"}
    Returns:
        float: USD換算された値
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
    ticker = "PLTR"
    print(f"Testing data extraction for {ticker}...")
    
    data = extract_quarterly_facts(ticker, years=5)
    
    if data:
        print(f"\nSuccessfully extracted {len(data)} quarters:")
        for i, quarter in enumerate(data[:5]):
            print(f"\nQuarter {i+1}: {quarter['filing_date']}")
            print(f"  Net Income: {normalize_value(quarter.get('net_income')):,.0f} USD")
            print(f"  Diluted Shares: {normalize_value(quarter.get('diluted_shares')):,.0f}")
    else:
        print("No data extracted")

if __name__ == "__main__":
    main()
