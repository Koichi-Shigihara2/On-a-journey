"""
SEC XBRL データパーサー
Company Facts 生データを正規化された年次/四半期データに変換
"""

import json
import os
from datetime import datetime
from typing import Optional, Dict, Any, List

from .config import get_ticker_info


class SECParser:
    """SEC Company Facts データパーサー"""
    
    # XBRL項目マッピング（優先順位順）
    XBRL_MAPPING = {
        # BS（貸借対照表）
        "total_assets": [
            "Assets",
        ],
        "stockholders_equity": [
            "StockholdersEquity",
            "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        ],
        "total_liabilities": [
            "Liabilities",
            "LiabilitiesAndStockholdersEquity",
        ],
        
        # RPO（残存履行義務）- SaaS企業向け
        "rpo": [
            "RevenueRemainingPerformanceObligation",
            "RemainingPerformanceObligation",
            "ContractWithCustomerLiability",
            "DeferredRevenue",
        ],
        
        # PL（損益計算書）
        "revenue": [
            "Revenues",  # 最優先（最も汎用的）
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",  # SOUN等
            "RevenueFromContractWithCustomer",
            "SalesRevenueNet",
            "TotalRevenue",
            "RevenuesNetOfInterestExpense",  # 銀行向け（SOFI等）
        ],
        "net_income": [
            "NetIncomeLoss",
            "ProfitLoss",
            "NetIncomeLossAvailableToCommonStockholdersBasic",
        ],
        "eps_diluted": [
            "EarningsPerShareDiluted",
        ],
        "eps_basic": [
            "EarningsPerShareBasic",
        ],
        
        # CF（キャッシュフロー計算書）
        "operating_cash_flow": [
            "NetCashProvidedByUsedInOperatingActivities",
        ],
        "capital_expenditure": [
            "PaymentsToAcquirePropertyPlantAndEquipment",
            "PaymentsToAcquireProductiveAssets",
            "PaymentsForCapitalImprovements",
        ],
        
        # 株式数
        "shares_diluted": [
            "WeightedAverageNumberOfDilutedSharesOutstanding",
            "CommonStockSharesOutstanding",  # フォールバック
        ],
        "shares_basic": [
            "WeightedAverageNumberOfSharesOutstandingBasic",
            "CommonStockSharesOutstanding",
        ],
    }
    
    def __init__(self, data_dir: str = None):
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    def parse_company_facts(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Company Facts 生データを読み込んでパース
        
        Returns:
            dict: {
                "ticker": str,
                "cik": str,
                "company_name": str,
                "annual": {2024: {...}, 2023: {...}, ...},
                "quarterly": {"2024Q1": {...}, ...}
            }
        """
        ticker = ticker.upper()
        raw_path = os.path.join(self.data_dir, ticker, "company_facts.json")
        
        if not os.path.exists(raw_path):
            print(f"   [{ticker}] company_facts.json が見つかりません")
            return None
        
        try:
            with open(raw_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except Exception as e:
            print(f"   [{ticker}] ファイル読み込みエラー: {e}")
            return None
        
        return self._parse_raw_data(ticker, raw_data)
    
    def _parse_raw_data(self, ticker: str, raw_data: dict) -> Dict[str, Any]:
        """生データをパース"""
        result = {
            "ticker": ticker,
            "cik": raw_data.get("cik", ""),
            "company_name": raw_data.get("entityName", ""),
            "annual": {},
            "quarterly": {},
            "parsed_at": datetime.now().isoformat(),
        }
        
        facts = raw_data.get("facts", {})
        us_gaap = facts.get("us-gaap", {})
        
        # 全項目を抽出
        extracted = {}
        for field_name, xbrl_keys in self.XBRL_MAPPING.items():
            # 株式数は同一期間で最大値を採用（異常値対策）
            use_max = field_name in ["shares_diluted", "shares_basic"]
            extracted[field_name] = self._extract_values(us_gaap, xbrl_keys, use_max=use_max)
        
        # 年次データを集約
        years = self._get_available_years(extracted)
        for year in years:
            annual_data = self._build_period_data(extracted, year, is_annual=True)
            if annual_data:
                result["annual"][year] = annual_data
        
        # 四半期データを集約
        quarters = self._get_available_quarters(extracted)
        for quarter in quarters:
            quarterly_data = self._build_period_data(extracted, quarter, is_annual=False)
            if quarterly_data:
                result["quarterly"][quarter] = quarterly_data
        
        return result
    
    def _extract_values(self, us_gaap: dict, xbrl_keys: List[str], use_max: bool = False) -> Dict[str, Any]:
        """
        指定されたXBRLキーから値を抽出
        
        Args:
            us_gaap: SEC XBRL データ
            xbrl_keys: 優先順位順のXBRLキーリスト
            use_max: 同一期間に複数値がある場合、最大値を使用（株式数向け）
                     Trueの場合、全XBRLキーを検索して最大値を採用
        
        Returns:
            dict: {
                "annual": {2024: value, 2023: value, ...},
                "quarterly": {"2024Q1": value, ...}
            }
        """
        result = {"annual": {}, "quarterly": {}}
        # 期末日を記録（同一FYで最新のend日付を優先するため）
        annual_end_dates = {}
        quarterly_end_dates = {}
        
        # 最新FYを特定するため、全キーの最大FYを事前に調べる
        max_fy_in_data = 0
        for key in xbrl_keys:
            if key not in us_gaap:
                continue
            units = us_gaap[key].get("units", {})
            for unit_type in ["USD", "shares", "USD/shares"]:
                if unit_type not in units:
                    continue
                for entry in units[unit_type]:
                    if entry.get("form") == "10-K" and entry.get("fp") == "FY":
                        fy = entry.get("fy", 0)
                        if fy > max_fy_in_data:
                            max_fy_in_data = fy
        
        for key in xbrl_keys:
            if key not in us_gaap:
                continue
            
            units = us_gaap[key].get("units", {})
            
            # USD or shares
            for unit_type in ["USD", "shares", "USD/shares"]:
                if unit_type not in units:
                    continue
                
                for entry in units[unit_type]:
                    form = entry.get("form", "")
                    fy = entry.get("fy")
                    fp = entry.get("fp", "")
                    val = entry.get("val")
                    end_date = entry.get("end", "")
                    
                    if val is None or fy is None:
                        continue
                    
                    # 年次（10-K）
                    if form == "10-K" and fp == "FY":
                        if use_max:
                            # 最大値を採用（株式数の異常値対策）
                            if fy not in result["annual"] or val > result["annual"][fy]:
                                result["annual"][fy] = val
                                annual_end_dates[fy] = end_date
                        else:
                            # 同一FYでは最新のend日付を優先
                            if fy not in result["annual"]:
                                result["annual"][fy] = val
                                annual_end_dates[fy] = end_date
                            elif end_date > annual_end_dates.get(fy, ""):
                                result["annual"][fy] = val
                                annual_end_dates[fy] = end_date
                    
                    # 四半期（10-Q）
                    elif form == "10-Q" and fp in ["Q1", "Q2", "Q3"]:
                        quarter_key = f"{fy}{fp}"
                        if use_max:
                            if quarter_key not in result["quarterly"] or val > result["quarterly"][quarter_key]:
                                result["quarterly"][quarter_key] = val
                                quarterly_end_dates[quarter_key] = end_date
                        else:
                            # 同一四半期では最新のend日付を優先
                            if quarter_key not in result["quarterly"]:
                                result["quarterly"][quarter_key] = val
                                quarterly_end_dates[quarter_key] = end_date
                            elif end_date > quarterly_end_dates.get(quarter_key, ""):
                                result["quarterly"][quarter_key] = val
                                quarterly_end_dates[quarter_key] = end_date
                
                # 最初に見つかったunit_typeのデータを使用
                if result["annual"] or result["quarterly"]:
                    break
            
            # use_max=Trueの場合は全キーを検索
            # use_max=Falseの場合は「最新FYのデータが取れた」場合のみ終了
            if use_max:
                continue  # 全キーを検索
            
            if result["annual"]:
                # 最新FYのデータがあるか確認
                if max_fy_in_data in result["annual"]:
                    break  # 最新FYが取れたので終了
                # 最新FYがない場合は次のキーを試す
            elif result["quarterly"]:
                break  # 四半期データは従来通り
        
        return result
    
    def _get_available_years(self, extracted: dict) -> List[int]:
        """利用可能な年度を取得"""
        years = set()
        for field_data in extracted.values():
            years.update(field_data.get("annual", {}).keys())
        return sorted(years, reverse=True)
    
    def _get_available_quarters(self, extracted: dict) -> List[str]:
        """利用可能な四半期を取得"""
        quarters = set()
        for field_data in extracted.values():
            quarters.update(field_data.get("quarterly", {}).keys())
        return sorted(quarters, reverse=True)
    
    def _build_period_data(self, extracted: dict, period: Any, is_annual: bool) -> Optional[Dict[str, Any]]:
        """特定期間のデータを構築"""
        period_type = "annual" if is_annual else "quarterly"
        
        data = {
            "period": str(period),
            "bs": {},
            "pl": {},
            "cf": {},
            "shares": {},
            "other": {},
        }
        
        # BS
        for field in ["total_assets", "stockholders_equity", "total_liabilities"]:
            val = extracted.get(field, {}).get(period_type, {}).get(period)
            if val is not None:
                data["bs"][field] = val
        
        # PL
        for field in ["revenue", "net_income", "eps_diluted", "eps_basic"]:
            val = extracted.get(field, {}).get(period_type, {}).get(period)
            if val is not None:
                data["pl"][field] = val
        
        # CF
        for field in ["operating_cash_flow", "capital_expenditure"]:
            val = extracted.get(field, {}).get(period_type, {}).get(period)
            if val is not None:
                data["cf"][field] = val
        
        # FCF計算
        ocf = data["cf"].get("operating_cash_flow", 0)
        capex = data["cf"].get("capital_expenditure", 0)
        if ocf != 0:
            data["cf"]["free_cash_flow"] = ocf - abs(capex)
        
        # Shares
        for field in ["shares_diluted", "shares_basic"]:
            val = extracted.get(field, {}).get(period_type, {}).get(period)
            if val is not None:
                data["shares"][field] = val
        
        # Other (RPO等)
        for field in ["rpo"]:
            val = extracted.get(field, {}).get(period_type, {}).get(period)
            if val is not None:
                data["other"][field] = val
        
        # 最低限のデータがあるか確認
        if not any([data["bs"], data["pl"], data["cf"]]):
            return None
        
        return data
    
    def save_parsed_data(self, ticker: str, parsed: dict) -> None:
        """パース済みデータを個別ファイルに保存"""
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        
        # 年次データ
        for year, data in parsed.get("annual", {}).items():
            path = os.path.join(ticker_dir, f"annual_{year}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "period": year,
                    "form": "10-K",
                    **data
                }, f, ensure_ascii=False, indent=2)
        
        # 四半期データ
        for quarter, data in parsed.get("quarterly", {}).items():
            path = os.path.join(ticker_dir, f"quarterly_{quarter}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "ticker": ticker,
                    "period": quarter,
                    "form": "10-Q",
                    **data
                }, f, ensure_ascii=False, indent=2)
        
        print(f"   [{ticker}] 保存完了: {len(parsed.get('annual', {}))}年次, {len(parsed.get('quarterly', {}))}四半期")
    
    def parse_and_save(self, ticker: str) -> Optional[Dict[str, Any]]:
        """パースして保存"""
        parsed = self.parse_company_facts(ticker)
        if parsed:
            self.save_parsed_data(ticker, parsed)
        return parsed


if __name__ == "__main__":
    parser = SECParser()
    
    # テスト
    parsed = parser.parse_and_save("TSLA")
    if parsed:
        print(f"\n年次データ: {list(parsed['annual'].keys())}")
        print(f"四半期データ: {list(parsed['quarterly'].keys())[:8]}...")
