import requests
import json
from datetime import datetime
import os
import time

# SEC EDGARフォールバック
try:
    from ..adjusted_eps_analyzer.extract_key_facts import extract_quarterly_facts, get_cik
    HAS_SEC = True
except:
    HAS_SEC = False

class TanukiDataFetcher:
    def __init__(self):
        self.av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.cache_dir = "cache"
        os.makedirs(self.cache_dir, exist_ok=True)

    def get_financials(self, ticker: str) -> dict:
        print(f"   [{ticker}] データ取得開始（SEC優先）")

        diluted_shares = 0.0
        latest_revenue = 0.0
        roe = 0.0
        fcf_list = []

        # 1. SEC EDGARを優先
        if HAS_SEC:
            print(f"   [{ticker}] SEC EDGARから財務データ取得")
            try:
                quarterly_data = extract_quarterly_facts(ticker)
                if quarterly_data and len(quarterly_data) > 0:
                    print(f"   [{ticker}] SECから{len(quarterly_data)}件の四半期データを取得")

                    for q in quarterly_data[:8]:  # 最新8四半期まで確認
                        # shares取得（複数のタグ対応）
                        for key in ["us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
                                   "us-gaap:CommonStockSharesOutstanding",
                                   "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic"]:
                            if key in q and isinstance(q[key], dict):
                                val = float(q[key].get("value", 0) or 0)
                                if val > 100_000:
                                    diluted_shares = max(diluted_shares, val)
                                    print(f"   [{ticker}] SECから{key}取得成功: {val:,.0f}")

                        # 売上高取得
                        for key in ["us-gaap:Revenues", "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
                                   "us-gaap:TotalRevenue", "us-gaap:NetSales"]:
                            if key in q and isinstance(q[key], dict):
                                rev = float(q[key].get("value", 0) or 0)
                                if rev > latest_revenue:
                                    latest_revenue = rev

                else:
                    print(f"   [{ticker}] SECから四半期データが取得できませんでした")

            except Exception as e:
                print(f"   [{ticker}] SEC取得エラー: {e}")

        # 2. Alpha Vantageを補完（ROEとRevenueTTM）
        overview = self._fetch_av(ticker, "OVERVIEW")
        if overview:
            if diluted_shares == 0:
                diluted_shares = float(overview.get("SharesOutstanding", 0) or 0)
            if roe == 0:
                roe_str = overview.get("ReturnOnEquityTTM", "0")
                roe = float(roe_str.replace("%", "")) / 100 if "%" in roe_str else float(roe_str) / 100
            if latest_revenue == 0:
                latest_revenue = float(overview.get("RevenueTTM", 0) or 0)

        # 3. FCF
        cf_data = self._fetch_av(ticker, "CASH_FLOW")
        if cf_data and "annualReports" in cf_data:
            for report in cf_data["annualReports"][:5]:
                ocf = float(report.get("operatingCashflow", 0))
                capex = abs(float(report.get("capitalExpenditures", 0)))
                fcf = ocf - capex
                fcf_list.append(fcf)

        fcf_avg = sum(fcf_list) / len(fcf_list) if fcf_list else 0.0

        # 4. 株価
        quote = self._fetch_av(ticker, "GLOBAL_QUOTE")
        current_price = float(quote.get("Global Quote", {}).get("05. price", 0)) if quote else 0.0

        print(f"   [{ticker}] FCF 5yr Avg = {fcf_avg:,.0f} | diluted_shares = {diluted_shares:,.0f} | ROE = {roe:.1%} | Revenue = ${latest_revenue:,.0f}")

        return {
            "fcf_5yr_avg": fcf_avg,
            "diluted_shares": diluted_shares,
            "roe_10yr_avg": roe,
            "current_price": current_price,
            "fcf_list_raw": fcf_list,
            "latest_revenue": latest_revenue,
            "eps_data": {"ticker": ticker}
        }

    def _fetch_av(self, ticker: str, function: str):
        cache_path = os.path.join(self.cache_dir, f"{ticker}_{function}.json")
        if os.path.exists(cache_path):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_path))).total_seconds()
            if age < 86400:
                with open(cache_path, "r") as f:
                    return json.load(f)

        url = f"https://www.alphavantage.co/query?function={function}&symbol={ticker}&apikey={self.av_key}"
        try:
            resp = requests.get(url, timeout=15)
            print(f"   [AV STATUS {ticker} {function}] {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                with open(cache_path, "w") as f:
                    json.dump(data, f)
                return data
            return None
        except Exception as e:
            print(f"   [AV EXCEPTION {ticker} {function}] {e}")
            return None
