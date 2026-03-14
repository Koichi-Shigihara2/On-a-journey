"""
SEC EDGARから企業の財務データを抽出するモジュール（完全版）
- CIKマップファイルから銘柄のCIKを取得
- HTTP/2問題を回避するための設定
- 複数クラス株式（PLTRなど）に対応した希薄化後株式数の合算
- 期間（duration/instant）を指定したXBRLデータ抽出
- edgartools 5.6.0 APIに完全対応
- 詳細なデバッグ出力とエラーハンドリング
"""
import os
import ssl
import csv
import json
import urllib3
import requests
import pandas as pd
from typing import Dict, Any, Optional, List
from datetime import datetime

# ============================================
# ネットワーク設定（HTTP/2問題の回避）
# ============================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

os.environ["HTTP2"] = "0"
os.environ["HTTPX_HTTP2"] = "0"
os.environ["NO_PROXY"] = "sec.gov,www.sec.gov"
os.environ["no_proxy"] = "sec.gov,www.sec.gov"

try:
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED
except Exception as e:
    print(f"SSL context creation warning: {e}")

from edgar import Company, set_identity

# ============================================
# 定数設定
# ============================================
set_identity("jamablue01@gmail.com")

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
# XBRLデータ抽出（edgartools 5.6.0 API対応）
# ============================================
def safe_get_xbrl_value(xbrl, tag: str, period_type: str = "duration") -> Optional[Dict[str, Any]]:
    """
    XBRLから安全に値を取得（期間指定可能）
    Args:
        xbrl: XBRLオブジェクト
        tag: 取得するタグ名（例: "us-gaap:NetIncomeLoss"）
        period_type: "duration"（期間）または "instant"（時点）
    Returns:
        Optional[Dict]: 値と単位を含む辞書、失敗時はNone
    """
    try:
        print(f"    Trying to get tag: {tag} (period_type={period_type})")
        result = xbrl.to_pandas(tag)
        
        # 戻り値の型チェック（DataFrameであることを期待）
        if result is None:
            print(f"    ✗ No data for {tag} (returned None)")
            return None
        
        if not isinstance(result, pd.DataFrame):
            print(f"    ✗ Unexpected type for {tag}: {type(result)}")
            return None
        
        if result.empty:
            print(f"    ✗ Empty DataFrame for {tag}")
            return None
        
        print(f"    ✓ Found {tag}: {len(result)} rows")
        
        # 期間タイプでフィルタリング
        if period_type == "duration":
            # startDate を含む行 = 期間データ
            mask = result['period'].apply(lambda x: 'startDate' in x if isinstance(x, dict) else False)
        else:
            # instant を含む行 = 時点データ
            mask = result['period'].apply(lambda x: 'instant' in x if isinstance(x, dict) else False)
        
        filtered = result[mask]
        if filtered.empty:
            print(f"    ✗ No {period_type} data for {tag}")
            return None
        
        latest = filtered.iloc[-1]
        print(f"      Latest value: {latest['value']} {latest.get('unit', 'USD')}")
        
        return {
            "value": float(latest["value"]),
            "unit": latest.get("unit", "USD"),
            "period": latest.get("period", {}),
            "filed": latest.get("filed", None)
        }
        
    except Exception as e:
        print(f"    ✗ Error getting {tag}: {type(e).__name__}: {e}")
        return None

def get_diluted_shares_total(xbrl) -> Optional[Dict[str, Any]]:
    """
    複数クラス株式の加重平均希薄化後株式数を合算する（dimension_filters不使用版）
    PLTRのように複数クラスがある場合、同じタグで複数行が返されることを利用し、それらを合計する。
    """
    try:
        print("    Trying to get diluted shares (multi-class sum)...")
        result = xbrl.to_pandas("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding")
        
        if result is None or not isinstance(result, pd.DataFrame) or result.empty:
            print("    ✗ No diluted shares data found")
            return None
        
        # 期間データのみにフィルタリング
        mask = result['period'].apply(lambda x: 'startDate' in x if isinstance(x, dict) else False)
        filtered = result[mask]
        if filtered.empty:
            print("    ✗ No duration data for diluted shares")
            return None
        
        # 全ての行の値を合計（複数クラスある場合、各クラスが個別の行になっている）
        total_shares = filtered['value'].sum()
        print(f"      Total diluted shares (sum of all classes): {total_shares:,.0f}")
        
        # 単位を確認（通常は 'shares'）
        unit = filtered.iloc[-1].get('unit', 'shares')
        
        return {
            "value": float(total_shares),
            "unit": unit,
            "period": {"duration": "combined"},
            "filed": None
        }
        
    except Exception as e:
        print(f"    Error in get_diluted_shares_total: {e}")
        return None

# ============================================
# ファイリング取得
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
        
        # 最初の5件を表示
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

def debug_available_tags(xbrl):
    """XBRLで利用可能なタグを表示（デバッグ用）"""
    try:
        if hasattr(xbrl, 'facts'):
            facts = xbrl.facts
            print(f"    Total facts available: {len(facts)}")
            
            income_tags = [f for f in facts if 'Income' in f.name or 'Earnings' in f.name]
            print(f"    Income-related tags found: {len(income_tags)}")
            for tag in income_tags[:10]:
                print(f"      - {tag.name}")
            
            share_tags = [f for f in facts if 'Share' in f.name or 'Stock' in f.name]
            print(f"    Share-related tags found: {len(share_tags)}")
            for tag in share_tags[:10]:
                print(f"      - {tag.name}")
    except Exception as e:
        print(f"    Error listing facts: {e}")

# ============================================
# メイン抽出関数
# ============================================
def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """
    四半期データを取得（過去10年分）- 複数クラス株式対応版
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
                filing_date = getattr(filing, 'filing_date', 'unknown')
                form = getattr(filing, 'form', 'unknown')
                print(f"\nProcessing filing {i+1}/{len(filings)}: {filing_date} ({form})")
                
                xbrl = filing.xbrl()
                if not xbrl:
                    print("  No XBRL data available")
                    continue
                
                print("  XBRL data loaded successfully")
                # debug_available_tags(xbrl)  # 必要に応じてコメント解除
                
                period_data = {
                    "filing_date": str(filing_date),
                    "form": str(form),
                    "accession_no": str(getattr(filing, 'accession_no', 'unknown'))
                }
                
                # 純利益 (Net Income)
                net_income = safe_get_xbrl_value(xbrl, "us-gaap:NetIncomeLoss", "duration")
                if net_income:
                    period_data["net_income"] = net_income
                else:
                    # 代替タグ
                    net_income_parent = safe_get_xbrl_value(xbrl, "us-gaap:NetIncomeLossAttributableToParent", "duration")
                    if net_income_parent:
                        period_data["net_income"] = net_income_parent
                
                # 希薄化後株式数（複数クラス合算）
                diluted_shares = get_diluted_shares_total(xbrl)
                if diluted_shares:
                    period_data["diluted_shares"] = diluted_shares
                else:
                    # 通常の単一クラス用フォールバック
                    ds = safe_get_xbrl_value(xbrl, "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "duration")
                    if ds:
                        period_data["diluted_shares"] = ds
                    else:
                        bs = safe_get_xbrl_value(xbrl, "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "duration")
                        if bs:
                            period_data["diluted_shares"] = bs
                
                # 税引前利益
                pretax = safe_get_xbrl_value(xbrl, "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes", "duration")
                if not pretax:
                    pretax = safe_get_xbrl_value(xbrl, "us-gaap:IncomeLossBeforeEquityMethodInvestmentsIncomeTax", "duration")
                if pretax:
                    period_data["pretax_income"] = pretax
                
                # 税金費用
                tax = safe_get_xbrl_value(xbrl, "us-gaap:IncomeTaxExpenseBenefit", "duration")
                if tax:
                    period_data["tax_expense"] = tax
                
                # 株式報酬 (SBC)
                sbc = safe_get_xbrl_value(xbrl, "us-gaap:ShareBasedCompensation", "duration")
                if sbc:
                    period_data["sbc"] = sbc
                
                # リストラ費用
                restructuring = safe_get_xbrl_value(xbrl, "us-gaap:RestructuringCharges", "duration")
                if restructuring:
                    period_data["restructuring"] = restructuring
                
                # 必須データが揃っているかチェック
                if "net_income" in period_data and "diluted_shares" in period_data:
                    quarterly_data.append(period_data)
                    net_val = period_data['net_income']['value']
                    shr_val = period_data['diluted_shares']['value']
                    print(f"  ✓ Added to results (net_income={net_val:,.0f}, diluted_shares={shr_val:,.0f})")
                else:
                    missing = []
                    if "net_income" not in period_data:
                        missing.append("net_income")
                    if "diluted_shares" not in period_data:
                        missing.append("diluted_shares")
                    print(f"  ✗ Missing required data: {', '.join(missing)}")
                
            except Exception as e:
                print(f"  Error processing filing: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n{ticker}: {len(quarterly_data)}件の四半期データを取得")
        return quarterly_data
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        import traceback
        traceback.print_exc()
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
    else:  # USD or others
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
