"""
TANUKI VALUATION - Data Fetcher v2.0
FMP API を主データソースとして使用、フォールバック対応

データソース優先順位:
1. FMP API (Financial Modeling Prep) - メイン
2. Alpha Vantage - フォールバック
3. 手動入力/キャッシュ - 最終手段
"""

import requests
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List


class TanukiDataFetcher:
    """財務データ取得クラス - FMP API優先"""

    def __init__(self):
        self.fmp_key = os.getenv("FMP_API_KEY")
        self.av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.cache_dir = "cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # キャッシュ有効期限（秒）
        self.cache_ttl = 86400  # 24時間

    def get_financials(self, ticker: str) -> Dict[str, Any]:
        """
        メイン財務データ取得関数
        
        Returns:
            dict: {
                "fcf_5yr_avg": float,        # FCF 5年平均
                "diluted_shares": float,      # 希薄化後株式数
                "roe_10yr_avg": float,        # ROE 10年平均
                "current_price": float,       # 現在株価
                "fcf_list_raw": list,         # FCF生データ
                "latest_revenue": float,      # 直近売上高
                "eps_data": dict              # EPS関連データ
            }
        """
        print(f"\n   [{ticker}] データ取得開始")
        
        result = {
            "fcf_5yr_avg": 0.0,
            "diluted_shares": 0,
            "roe_10yr_avg": 0.0,
            "current_price": 0.0,
            "fcf_list_raw": [],
            "latest_revenue": 0.0,
            "eps_data": {"ticker": ticker}
        }

        # 1. FMP APIからデータ取得
        if self.fmp_key:
            fmp_data = self._fetch_fmp_data(ticker)
            if fmp_data:
                result.update(fmp_data)
                print(f"   [{ticker}] FMP APIからデータ取得完了")

        # 2. Alpha Vantageで補完（FMP不足時）
        if self.av_key and (result["diluted_shares"] == 0 or result["fcf_5yr_avg"] == 0):
            av_data = self._fetch_av_data(ticker)
            if av_data:
                # 不足データのみ補完
                if result["diluted_shares"] == 0:
                    result["diluted_shares"] = av_data.get("diluted_shares", 0)
                if result["fcf_5yr_avg"] == 0:
                    result["fcf_5yr_avg"] = av_data.get("fcf_5yr_avg", 0)
                    result["fcf_list_raw"] = av_data.get("fcf_list_raw", [])
                if result["roe_10yr_avg"] == 0:
                    result["roe_10yr_avg"] = av_data.get("roe_10yr_avg", 0)
                print(f"   [{ticker}] Alpha Vantageで補完完了")

        # 3. 現在株価取得
        if result["current_price"] == 0:
            result["current_price"] = self._fetch_current_price(ticker)

        # ログ出力
        print(f"   [{ticker}] 最終結果:")
        print(f"       FCF 5yr Avg: ${result['fcf_5yr_avg']:,.0f}")
        print(f"       Diluted Shares: {result['diluted_shares']:,.0f}")
        print(f"       ROE 10yr: {result['roe_10yr_avg']:.1%}")
        print(f"       Current Price: ${result['current_price']:.2f}")
        print(f"       Revenue: ${result['latest_revenue']:,.0f}")

        return result

    def _fetch_fmp_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """FMP APIからデータ取得"""
        if not self.fmp_key:
            return None
            
        result = {}
        
        try:
            # プロフィール取得（株式数、現在価格）
            profile = self._cached_request(
                f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={self.fmp_key}",
                f"{ticker}_profile"
            )
            if profile and len(profile) > 0:
                p = profile[0]
                result["diluted_shares"] = int(p.get("mktCap", 0) / p.get("price", 1)) if p.get("price", 0) > 0 else 0
                result["current_price"] = float(p.get("price", 0))
                
            # キーメトリクス取得（ROE）
            metrics = self._cached_request(
                f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?limit=10&apikey={self.fmp_key}",
                f"{ticker}_metrics"
            )
            if metrics and len(metrics) > 0:
                roe_values = [float(m.get("roe", 0)) for m in metrics if m.get("roe") is not None]
                if roe_values:
                    result["roe_10yr_avg"] = sum(roe_values) / len(roe_values)
                    
            # 株式数（より正確な値）
            shares_data = self._cached_request(
                f"https://financialmodelingprep.com/api/v3/enterprise-values/{ticker}?limit=1&apikey={self.fmp_key}",
                f"{ticker}_shares"
            )
            if shares_data and len(shares_data) > 0:
                diluted = shares_data[0].get("numberOfShares", 0)
                if diluted > 0:
                    result["diluted_shares"] = int(diluted)

            # キャッシュフロー計算書（FCF）
            cf = self._cached_request(
                f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?limit=5&apikey={self.fmp_key}",
                f"{ticker}_cashflow"
            )
            if cf and len(cf) > 0:
                fcf_list = []
                for report in cf:
                    ocf = float(report.get("operatingCashFlow", 0))
                    capex = abs(float(report.get("capitalExpenditure", 0)))
                    fcf = ocf - capex
                    fcf_list.append(fcf)
                
                if fcf_list:
                    # 古い順に並び替え
                    fcf_list.reverse()
                    result["fcf_list_raw"] = fcf_list
                    result["fcf_5yr_avg"] = sum(fcf_list) / len(fcf_list)

            # 損益計算書（売上高）
            income = self._cached_request(
                f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=1&apikey={self.fmp_key}",
                f"{ticker}_income"
            )
            if income and len(income) > 0:
                result["latest_revenue"] = float(income[0].get("revenue", 0))

        except Exception as e:
            print(f"   [{ticker}] FMP APIエラー: {e}")
            return None

        return result if result else None

    def _fetch_av_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Alpha Vantageからデータ取得（フォールバック）"""
        if not self.av_key:
            return None
            
        result = {}
        
        try:
            # OVERVIEW
            overview = self._cached_request(
                f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={self.av_key}",
                f"{ticker}_av_overview"
            )
            if overview and "SharesOutstanding" in overview:
                result["diluted_shares"] = int(float(overview.get("SharesOutstanding", 0)))
                roe_str = overview.get("ReturnOnEquityTTM", "0")
                if roe_str:
                    try:
                        result["roe_10yr_avg"] = float(roe_str.replace("%", "")) / 100 if "%" in str(roe_str) else float(roe_str)
                    except:
                        pass
                result["latest_revenue"] = float(overview.get("RevenueTTM", 0) or 0)

            # CASH_FLOW
            cf = self._cached_request(
                f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker}&apikey={self.av_key}",
                f"{ticker}_av_cashflow"
            )
            if cf and "annualReports" in cf:
                fcf_list = []
                for report in cf["annualReports"][:5]:
                    ocf = float(report.get("operatingCashflow", 0) or 0)
                    capex = abs(float(report.get("capitalExpenditures", 0) or 0))
                    fcf = ocf - capex
                    fcf_list.append(fcf)
                
                if fcf_list:
                    result["fcf_list_raw"] = fcf_list
                    result["fcf_5yr_avg"] = sum(fcf_list) / len(fcf_list)

        except Exception as e:
            print(f"   [{ticker}] Alpha Vantageエラー: {e}")
            return None

        return result if result else None

    def _fetch_current_price(self, ticker: str) -> float:
        """現在株価を取得"""
        # FMP Quote
        if self.fmp_key:
            try:
                quote = self._cached_request(
                    f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={self.fmp_key}",
                    f"{ticker}_quote",
                    cache_ttl=3600  # 1時間キャッシュ
                )
                if quote and len(quote) > 0:
                    return float(quote[0].get("price", 0))
            except:
                pass
        
        # Alpha Vantage GLOBAL_QUOTE
        if self.av_key:
            try:
                quote = self._cached_request(
                    f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={self.av_key}",
                    f"{ticker}_av_quote",
                    cache_ttl=3600
                )
                if quote and "Global Quote" in quote:
                    return float(quote["Global Quote"].get("05. price", 0))
            except:
                pass
        
        return 0.0

    def _cached_request(self, url: str, cache_key: str, cache_ttl: int = None) -> Optional[Any]:
        """キャッシュ付きHTTPリクエスト"""
        if cache_ttl is None:
            cache_ttl = self.cache_ttl
            
        cache_path = os.path.join(self.cache_dir, f"{cache_key}.json")
        
        # キャッシュ確認
        if os.path.exists(cache_path):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).total_seconds()
            if age < cache_ttl:
                try:
                    with open(cache_path, "r", encoding="utf-8") as f:
                        return json.load(f)
                except:
                    pass

        # API呼び出し
        try:
            resp = requests.get(url, timeout=15, headers={
                "User-Agent": "TanukiValuation/2.0"
            })
            if resp.status_code == 200:
                data = resp.json()
                # エラーレスポンスのチェック
                if isinstance(data, dict) and ("Error" in data or "Note" in data or "Information" in data):
                    print(f"   [API WARNING] {data.get('Error') or data.get('Note') or data.get('Information')}")
                    return None
                # キャッシュ保存
                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                return data
        except Exception as e:
            print(f"   [HTTP ERROR] {cache_key}: {e}")
        
        return None


if __name__ == "__main__":
    # テスト実行
    fetcher = TanukiDataFetcher()
    data = fetcher.get_financials("TSLA")
    print("\n=== Test Result ===")
    print(json.dumps(data, indent=2, default=str))
