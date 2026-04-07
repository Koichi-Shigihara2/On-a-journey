"""
SEC データリーダー
各ツール（Adjusted EPS Analyzer, TANUKI VALUATION等）からのアクセス用インターフェース
"""

import json
import os
from typing import Optional, Dict, Any, List

from .config import TICKERS, get_ticker_info


class SECReader:
    """SECデータ読み取りインターフェース"""
    
    def __init__(self, data_dir: str = None):
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    # =========================================
    # 年次データ取得
    # =========================================
    
    def get_annual(self, ticker: str, year: int) -> Optional[Dict[str, Any]]:
        """
        年次データ取得
        
        Args:
            ticker: ティッカーシンボル
            year: 年度（例: 2024）
        
        Returns:
            dict: {
                "ticker": "TSLA",
                "period": 2024,
                "form": "10-K",
                "bs": {"total_assets": ..., "stockholders_equity": ..., ...},
                "pl": {"revenue": ..., "net_income": ..., "eps_diluted": ...},
                "cf": {"operating_cash_flow": ..., "capital_expenditure": ..., "free_cash_flow": ...},
                "shares": {"shares_diluted": ..., "shares_basic": ...}
            }
        """
        ticker = ticker.upper()
        path = os.path.join(self.data_dir, ticker, f"annual_{year}.json")
        return self._load_json(path)
    
    def get_annual_range(self, ticker: str, years: int = 5) -> List[Dict[str, Any]]:
        """
        直近N年分の年次データ取得
        
        Args:
            ticker: ティッカーシンボル
            years: 取得年数（デフォルト5年）
        
        Returns:
            list: 年次データのリスト（新しい順）
        """
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        
        if not os.path.exists(ticker_dir):
            return []
        
        # 利用可能な年次ファイルを検索
        results = []
        files = sorted(os.listdir(ticker_dir), reverse=True)
        
        for f in files:
            if f.startswith("annual_") and f.endswith(".json"):
                data = self._load_json(os.path.join(ticker_dir, f))
                if data:
                    results.append(data)
                    if len(results) >= years:
                        break
        
        return results
    
    # =========================================
    # 四半期データ取得
    # =========================================
    
    def get_quarterly(self, ticker: str, quarter: str) -> Optional[Dict[str, Any]]:
        """
        四半期データ取得
        
        Args:
            ticker: ティッカーシンボル
            quarter: 四半期（例: "2024Q1"）
        
        Returns:
            dict: 四半期財務データ
        """
        ticker = ticker.upper()
        path = os.path.join(self.data_dir, ticker, f"quarterly_{quarter}.json")
        return self._load_json(path)
    
    def get_quarterly_range(self, ticker: str, quarters: int = 8) -> List[Dict[str, Any]]:
        """
        直近N四半期分のデータ取得
        
        Args:
            ticker: ティッカーシンボル
            quarters: 取得四半期数（デフォルト8）
        
        Returns:
            list: 四半期データのリスト（新しい順）
        """
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        
        if not os.path.exists(ticker_dir):
            return []
        
        results = []
        files = sorted(os.listdir(ticker_dir), reverse=True)
        
        for f in files:
            if f.startswith("quarterly_") and f.endswith(".json"):
                data = self._load_json(os.path.join(ticker_dir, f))
                if data:
                    results.append(data)
                    if len(results) >= quarters:
                        break
        
        return results
    
    # =========================================
    # TANUKI VALUATION用ヘルパー
    # =========================================
    
    def get_fcf_5yr_avg(self, ticker: str) -> float:
        """FCF 5年平均を取得"""
        annual_data = self.get_annual_range(ticker, 5)
        
        fcf_list = []
        for data in annual_data:
            fcf = data.get("cf", {}).get("free_cash_flow")
            if fcf is not None:
                fcf_list.append(fcf)
        
        return sum(fcf_list) / len(fcf_list) if fcf_list else 0.0
    
    def get_roe_avg(self, ticker: str, years: int = 10) -> float:
        """ROE平均を取得（純利益÷株主資本）"""
        annual_data = self.get_annual_range(ticker, years)
        
        roe_list = []
        for data in annual_data:
            net_income = data.get("pl", {}).get("net_income")
            equity = data.get("bs", {}).get("stockholders_equity")
            
            if net_income is not None and equity and equity > 0:
                roe = net_income / equity
                roe_list.append(roe)
        
        return sum(roe_list) / len(roe_list) if roe_list else 0.0
    
    def get_diluted_shares(self, ticker: str) -> int:
        """直近の希薄化後株式数を取得"""
        annual_data = self.get_annual_range(ticker, 1)
        
        if annual_data:
            shares = annual_data[0].get("shares", {}).get("shares_diluted")
            if shares:
                return int(shares)
        
        return 0
    
    def get_latest_revenue(self, ticker: str) -> float:
        """直近の売上高を取得"""
        annual_data = self.get_annual_range(ticker, 1)
        
        if annual_data:
            revenue = annual_data[0].get("pl", {}).get("revenue")
            if revenue:
                return float(revenue)
        
        return 0.0
    
    def get_fcf_list(self, ticker: str, years: int = 5) -> List[float]:
        """FCFリストを取得（古い順）"""
        annual_data = self.get_annual_range(ticker, years)
        
        fcf_list = []
        for data in reversed(annual_data):  # 古い順に並び替え
            fcf = data.get("cf", {}).get("free_cash_flow")
            if fcf is not None:
                fcf_list.append(fcf)
        
        return fcf_list
    
    # =========================================
    # Adjusted EPS Analyzer用ヘルパー
    # =========================================
    
    def get_eps_diluted(self, ticker: str, quarter: str) -> Optional[float]:
        """四半期EPSを取得"""
        data = self.get_quarterly(ticker, quarter)
        if data:
            return data.get("pl", {}).get("eps_diluted")
        return None
    
    # =========================================
    # ユーティリティ
    # =========================================
    
    def _load_json(self, path: str) -> Optional[Dict[str, Any]]:
        """JSONファイル読み込み"""
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return None
    
    def get_available_tickers(self) -> List[str]:
        """データが存在するティッカー一覧"""
        if not os.path.exists(self.data_dir):
            return []
        
        tickers = []
        for name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, name)
            if os.path.isdir(path) and not name.startswith("_"):
                tickers.append(name)
        
        return sorted(tickers)
    
    def get_data_summary(self, ticker: str) -> Dict[str, Any]:
        """ティッカーのデータサマリー"""
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        
        if not os.path.exists(ticker_dir):
            return {"error": "データなし"}
        
        files = os.listdir(ticker_dir)
        annual_files = [f for f in files if f.startswith("annual_")]
        quarterly_files = [f for f in files if f.startswith("quarterly_")]
        
        info = get_ticker_info(ticker)
        
        return {
            "ticker": ticker,
            "name": info["name"],
            "status": info["status"],
            "annual_count": len(annual_files),
            "quarterly_count": len(quarterly_files),
            "has_company_facts": "company_facts.json" in files,
        }


# シングルトンインスタンス
_reader = None

def get_reader() -> SECReader:
    """グローバルリーダーインスタンス取得"""
    global _reader
    if _reader is None:
        _reader = SECReader()
    return _reader


if __name__ == "__main__":
    reader = SECReader()
    
    # テスト
    print("=== 利用可能ティッカー ===")
    print(reader.get_available_tickers())
    
    print("\n=== TSLA サマリー ===")
    print(reader.get_data_summary("TSLA"))
    
    print("\n=== TSLA FCF 5年平均 ===")
    print(f"${reader.get_fcf_5yr_avg('TSLA'):,.0f}")
    
    print("\n=== TSLA ROE平均 ===")
    print(f"{reader.get_roe_avg('TSLA'):.1%}")
