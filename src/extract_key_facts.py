from edgar import Company, set_identity
from typing import Dict, Any, Optional, List
import pandas as pd

set_identity("jamablue01@gmail.com")

def fetch_filings(ticker: str, count: int = 40) -> List:
    """10-Q/10-K取得（デバッグ出力追加）"""
    print(f"Fetching filings for {ticker}...")
    company = Company(ticker)
    filings = company.get_filings(form=["10-Q", "10-K"])
    print(f"Found {len(filings)} total filings")
    
    # 最初の5件を表示（デバッグ）
    for i, f in enumerate(filings[:5]):
        print(f"  {i+1}: {f.filing_date} - {f.form} - {f.accession_no}")
    
    return filings[:count]

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """
    四半期データを取得（デバッグ版）
    """
    try:
        # filings取得
        filings = fetch_filings(ticker, count=years*4)
        
        quarterly_data = []
        for i, filing in enumerate(filings):
            try:
                print(f"\nProcessing filing {i+1}/{len(filings)}: {filing.filing_date} ({filing.form})")
                
                # XBRLデータ取得
                xbrl = filing.xbrl()
                if not xbrl:
                    print("  No XBRL data available")
                    continue
                
                print("  XBRL data loaded successfully")
                
                # 利用可能なタグを確認（デバッグ）
                try:
                    facts = xbrl.facts
                    print(f"  Available facts: {len(facts)}")
                    
                    # NetIncomeLossタグを探す
                    income_tags = [f for f in facts if 'NetIncomeLoss' in f.name]
                    print(f"  NetIncomeLoss tags found: {len(income_tags)}")
                    for tag in income_tags[:3]:
                        print(f"    - {tag.name}")
                        
                except Exception as e:
                    print(f"  Error checking facts: {e}")
                
                # 基本データ抽出
                period_data = {
                    "filing_date": str(filing.filing_date),
                    "form": filing.form,
                    "accession_no": filing.accession_no
                }
                
                # 各タグの値を取得
                # Net Income
                try:
                    df = xbrl.to_pandas("us-gaap:NetIncomeLoss")
                    if not df.empty:
                        period_data["net_income"] = {
                            "value": float(df.iloc[-1]["value"]),
                            "unit": df.iloc[-1].get("unit", "USD")
                        }
                        print(f"  Net Income: {period_data['net_income']['value']}")
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
                        print(f"  Diluted Shares: {period_data['diluted_shares']['value']}")
                except Exception as e:
                    print(f"  Error getting DilutedShares: {e}")
                
                # Tax Expense
                try:
                    df = xbrl.to_pandas("us-gaap:IncomeTaxExpenseBenefit")
                    if not df.empty:
                        period_data["tax_expense"] = {
                            "value": float(df.iloc[-1]["value"]),
                            "unit": df.iloc[-1].get("unit", "USD")
                        }
                        print(f"  Tax Expense: {period_data['tax_expense']['value']}")
                except Exception as e:
                    print(f"  Error getting TaxExpense: {e}")
                
                # Pretax Income
                try:
                    df = xbrl.to_pandas("us-gaap:IncomeLossBeforeEquityMethodInvestmentsIncomeTax")
                    if not df.empty:
                        period_data["pretax_income"] = {
                            "value": float(df.iloc[-1]["value"]),
                            "unit": df.iloc[-1].get("unit", "USD")
                        }
                        print(f"  Pretax Income: {period_data['pretax_income']['value']}")
                except Exception as e:
                    print(f"  Error getting PretaxIncome: {e}")
                
                # SBC
                try:
                    df = xbrl.to_pandas("us-gaap:ShareBasedCompensation")
                    if not df.empty:
                        period_data["sbc"] = {
                            "value": float(df.iloc[-1]["value"]),
                            "unit": df.iloc[-1].get("unit", "USD")
                        }
                        print(f"  SBC: {period_data['sbc']['value']}")
                except Exception as e:
                    print(f"  Error getting SBC: {e}")
                
                # 必須データが揃っているかチェック
                if "net_income" in period_data and "diluted_shares" in period_data:
                    quarterly_data.append(period_data)
                    print(f"  ✓ Added to results")
                else:
                    print(f"  ✗ Missing required data (net_income or diluted_shares)")
                
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
