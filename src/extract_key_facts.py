from edgar import Company, set_identity
from typing import Dict, Any, Optional, List
import pandas as pd

set_identity("jamablue01@gmail.com")

# XBRLタグマッピング（adjustment_items.jsonと連携）
TAG_MAPPING = {
    "net_income": ["us-gaap:NetIncomeLoss", "us-gaap:NetIncomeLossAttributableToParent"],
    "diluted_shares": ["us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding"],
    "basic_shares": ["us-gaap:WeightedAverageNumberOfSharesOutstandingBasic"],
    "tax_expense": ["us-gaap:IncomeTaxExpenseBenefit"],
    "pretax_income": ["us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes", 
                      "us-gaap:IncomeLossBeforeEquityMethodInvestmentsIncomeTax"],
    "revenues": ["us-gaap:Revenues", "us-gaap:SalesRevenueNet"],
    "operating_income": ["us-gaap:OperatingIncomeLoss"],
    # 調整項目用タグ（adjustment_items.jsonから動的生成も可能）
    "restructuring": ["us-gaap:RestructuringCharges", "us-gaap:RestructuringAndRelatedCosts"],
    "sbc": ["us-gaap:ShareBasedCompensation"],
    "goodwill_impairment": ["us-gaap:GoodwillImpairmentLoss"],
    "intangible_amortization": ["us-gaap:AmortizationOfIntangibleAssets"],
    "acquisition_costs": ["us-gaap:BusinessCombinationAcquisitionRelatedCosts"],
    "discontinued_ops": ["us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax"],
}

def extract_quarterly_facts(ticker: str, years: int = 10) -> List[Dict[str, Any]]:
    """
    四半期データを取得（過去10年分）
    """
    try:
        company = Company(ticker)
        filings = company.get_filings(form=["10-Q", "10-K"]).head(years * 4)  # 最大40件
        
        quarterly_data = []
        for filing in filings:
            try:
                # ファイリングからXBRLデータを取得
                xbrl = filing.xbrl()
                if not xbrl:
                    continue
                
                # 当期データを抽出
                period_data = {"filing_date": filing.filing_date, "form": filing.form}
                
                # 各タグの値を取得（最新の期間データ）
                for key, tags in TAG_MAPPING.items():
                    for tag in tags:
                        try:
                            df = xbrl.to_pandas(tag)
                            if not df.empty:
                                # 最最新の値を取得（期間末日ベース）
                                latest = df.iloc[-1]
                                period_data[key] = {
                                    "value": float(latest["value"]),
                                    "period": latest.get("period", {}),
                                    "unit": latest.get("unit", "USD")
                                }
                                break
                        except:
                            continue
                
                # 必須データが揃っている場合のみ追加
                if "net_income" in period_data and "diluted_shares" in period_data:
                    quarterly_data.append(period_data)
                    
            except Exception as e:
                print(f"  Filing {filing.accession_no} 処理エラー: {e}")
                continue
        
        print(f"{ticker}: {len(quarterly_data)}件の四半期データを取得")
        return quarterly_data
        
    except Exception as e:
        print(f"{ticker} データ取得エラー: {e}")
        return []

def normalize_value(value_dict: Dict) -> float:
    """単位正規化（すべてUSD absolute valueに統一）"""
    if not value_dict:
        return 0.0
    value = value_dict.get("value", 0)
    unit = value_dict.get("unit", "USD")
    
    # 単位変換（必要に応じて拡張）
    if unit == "USD":
        return float(value)
    elif unit == "thousands":
        return float(value) * 1000
    elif unit == "millions":
        return float(value) * 1_000_000
    elif unit == "billions":
        return float(value) * 1_000_000_000
    return float(value)
