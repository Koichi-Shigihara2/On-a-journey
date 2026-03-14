from edgar import Company, set_identity
from typing import Dict, Any, Optional, List
import pandas as pd
import csv
import os

set_identity("jamablue01@gmail.com")

# 設定ファイルのパス
CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
CIK_FILE = os.path.join(CONFIG_DIR, "cik_lookup.csv")

def load_cik_map() -> Dict[str, str]:
    """CIKマップをCSVから読み込む"""
    cik_map = {}
    try:
        with open(CIK_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cik_map[row['ticker']] = row['cik']
        print(f"Loaded {len(cik_map)} CIK mappings from {CIK_FILE}")
    except FileNotFoundError:
        print(f"Warning: {CIK_FILE} not found. Creating empty mapping.")
        # 空のファイルを作成
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(CIK_FILE, 'w', encoding='utf-8') as f:
            f.write("ticker,cik,name\n")
    except Exception as e:
        print(f"Error loading CIK map: {e}")
    
    return cik_map

def save_cik_map(cik_map: Dict[str, str]):
    """CIKマップをCSVに保存（新規追加用）"""
    try:
        with open(CIK_FILE, 'w', encoding='utf-8') as f:
            f.write("ticker,cik,name\n")
            for ticker, cik in cik_map.items():
                f.write(f"{ticker},{cik},\n")
        print(f"Saved {len(cik_map)} CIK mappings to {CIK_FILE}")
    except Exception as e:
        print(f"Error saving CIK map: {e}")

def get_cik(ticker: str) -> str:
    """ティッカーからCIKを取得（設定ファイル優先→APIフォールバック）"""
    # 設定ファイルから読み込み
    cik_map = load_cik_map()
    
    # マップにあればそれを使う
    if ticker in cik_map:
        return cik_map[ticker]
    
    # なければAPIから取得を試みる
    print(f"CIK not found for {ticker} in local file. Trying SEC API...")
    try:
        from edgar.reference.tickers import get_company_cik_lookup
        lookup = get_company_cik_lookup()
        if ticker in lookup:
            cik = lookup[ticker]
            # 見つかったら保存（次回用）
            cik_map[ticker] = cik
            save_cik_map(cik_map)
            return cik
    except Exception as e:
        print(f"SEC API lookup failed: {e}")
    
    raise Exception(f"CIK not found for {ticker}. Please add to {CIK_FILE}")

def fetch_filings(ticker: str, count: int = 40) -> List:
    """10-Q/10-K取得"""
    print(f"Fetching filings for {ticker}...")
    
    # CIKを取得
    cik = get_cik(ticker)
    print(f"CIK: {cik}")
    
    # CIKでCompany作成
    company = Company(cik)
    filings = company.get_filings(form=["10-Q", "10-K"])
    print(f"Found {len(filings)} total filings")
    
    # リストに変換
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

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """四半期データを取得"""
    try:
        filings = fetch_filings(ticker, count=years*4)
        
        quarterly_data = []
        for i, filing in enumerate(filings):
            try:
                print(f"\nProcessing filing {i+1}/{len(filings)}: {filing.filing_date} ({filing.form})")
                
                xbrl = filing.xbrl()
                if not xbrl:
                    print("  No XBRL data available")
                    continue
                
                period_data = {
                    "filing_date": str(filing.filing_date),
                    "form": filing.form,
                }
                
                # Net Income
                try:
                    df = xbrl.to_pandas("us-gaap:NetIncomeLoss")
                    if not df.empty:
                        period_data["net_income"] = {
                            "value": float(df.iloc[-1]["value"]),
                            "unit": df.iloc[-1].get("unit", "USD")
                        }
                except Exception as e:
                    print(f"  Error getting NetIncomeLoss: {e}")
                
                # Diluted Shares
                try:
                    df = xbrl.to_pandas("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding")
                    if not df.empty:
                        period_data["diluted_shares"] = {
                            "value": float(df.iloc[-1]["value"]),
                            "unit": df.iloc[-1].get("unit", "USD")
                        }
                except Exception as e:
                    print(f"  Error getting DilutedShares: {e}")
                
                # 必須データが揃っている場合のみ追加
                if "net_income" in period_data and "diluted_shares" in period_data:
                    quarterly_data.append(period_data)
                    print(f"  ✓ Added to results")
                
            except Exception as e:
                print(f"  Error processing filing: {e}")
                continue
        
        print(f"\n{ticker}: {len(quarterly_data)}件の四半期データを取得")
        return quarterly_data
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        return []

def normalize_value(value_dict: Optional[Dict]) -> float:
    """単位正規化"""
    if not value_dict:
        return 0.0
    value = value_dict.get("value", 0)
    unit = value_dict.get("unit", "USD")
    
    if unit == "USD":
        return float(value)
    elif unit == "thousands":
        return float(value) * 1000
    elif unit == "millions":
        return float(value) * 1_000_000
    elif unit == "billions":
        return float(value) * 1_000_000_000
    return float(value)
