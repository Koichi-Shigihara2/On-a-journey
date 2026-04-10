"""
TANUKI VALUATION - Data Fetcher v3.2
SEC EDGARデータ優先 + Alpha Vantage補完 + yfinance価格取得

v3.2変更点:
- SEC reader (common/sec_data/reader.py) を使用
- RPO取得対応
- 加重平均希薄化後株式数の取得
"""

import os
import sys
from typing import Dict, Any, Optional

# SEC reader のインポート
# src/value/tanuki_valuation/ から common/sec_data/ へのパス
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
sys.path.insert(0, repo_root)

try:
    from common.sec_data.reader import SECReader
    HAS_SEC_READER = True
except ImportError as e:
    print(f"Warning: SEC reader not available: {e}")
    HAS_SEC_READER = False

# yfinance for current price
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False


class TanukiDataFetcher:
    """TANUKI VALUATION用データ取得"""
    
    def __init__(self):
        # SEC reader初期化
        if HAS_SEC_READER:
            # GitHub Actions環境では common/sec_data/data を使用
            sec_data_dir = os.path.join(repo_root, "common", "sec_data", "data")
            self.sec_reader = SECReader(data_dir=sec_data_dir)
        else:
            self.sec_reader = None
    
    def get_financials(self, ticker: str) -> Dict[str, Any]:
        """
        財務データ取得
        
        Returns:
            dict: {
                "fcf_5yr_avg": float,
                "fcf_list_raw": list,
                "diluted_shares": int,
                "roe_10yr_avg": float,
                "current_price": float,
                "latest_revenue": float,
                "rpo": float,
                "eps_data": {"ticker": str}
            }
        """
        print(f"\n   [{ticker}] データ取得開始")
        
        result = {
            "fcf_5yr_avg": 0.0,
            "fcf_list_raw": [],
            "diluted_shares": 0,
            "roe_10yr_avg": 0.0,
            "current_price": 0.0,
            "latest_revenue": 0.0,
            "rpo": 0.0,
            "eps_data": {"ticker": ticker}
        }
        
        # ===========================================
        # SEC EDGAR からデータ取得
        # ===========================================
        if self.sec_reader:
            try:
                # FCF 5年平均
                fcf_avg = self.sec_reader.get_fcf_5yr_avg(ticker)
                if fcf_avg != 0:
                    result["fcf_5yr_avg"] = fcf_avg
                    print(f"   [{ticker}] SEC FCF 5yr avg: ${fcf_avg:,.0f}")
                
                # FCFリスト
                fcf_list = self.sec_reader.get_fcf_list(ticker, years=5)
                if fcf_list:
                    result["fcf_list_raw"] = fcf_list
                    print(f"   [{ticker}] SEC FCF list: {len(fcf_list)}年分")
                
                # 希薄化後株式数
                shares = self.sec_reader.get_diluted_shares(ticker)
                if shares > 0:
                    result["diluted_shares"] = shares
                    print(f"   [{ticker}] SEC shares: {shares:,}")
                
                # ROE平均（連続黒字期間のみ）
                roe = self.sec_reader.get_roe_avg(ticker, years=10)
                if roe > 0:
                    result["roe_10yr_avg"] = roe
                    print(f"   [{ticker}] SEC ROE avg: {roe:.1%}")
                
                # 直近売上高
                revenue = self.sec_reader.get_latest_revenue(ticker)
                if revenue > 0:
                    result["latest_revenue"] = revenue
                    print(f"   [{ticker}] SEC revenue: ${revenue:,.0f}")
                
                # RPO（残存履行義務）
                rpo = self.sec_reader.get_rpo(ticker)
                if rpo > 0:
                    result["rpo"] = rpo
                    print(f"   [{ticker}] SEC RPO: ${rpo:,.0f}")
                    
            except Exception as e:
                print(f"   [{ticker}] SEC reader error: {e}")
        
        # ===========================================
        # yfinance から現在価格取得
        # ===========================================
        if HAS_YFINANCE:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # 現在価格
                price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
                if price > 0:
                    result["current_price"] = float(price)
                    print(f"   [{ticker}] yfinance price: ${price:.2f}")
                
            except Exception as e:
                print(f"   [{ticker}] yfinance error: {e}")
        
        # ===========================================
        # 結果サマリー
        # ===========================================
        print(f"   [{ticker}] 最終結果:")
        print(f"       FCF 5yr Avg: ${result['fcf_5yr_avg']:,.0f}")
        print(f"       Diluted Shares: {result['diluted_shares']:,}")
        print(f"       ROE avg: {result['roe_10yr_avg']:.1%}")
        print(f"       Current Price: ${result['current_price']:.2f}")
        print(f"       Revenue: ${result['latest_revenue']:,.0f}")
        if result['rpo'] > 0:
            print(f"       RPO: ${result['rpo']:,.0f}")
        
        return result


if __name__ == "__main__":
    # テスト実行
    fetcher = TanukiDataFetcher()
    
    for ticker in ["TSLA", "PLTR", "MSFT"]:
        data = fetcher.get_financials(ticker)
        print(f"\n{ticker}: {data}")
