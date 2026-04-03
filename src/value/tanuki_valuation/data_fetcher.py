import requests
import json
from datetime import datetime
import os
import time

# SECフォールバック用（rate limit対策付き）
try:
    from ..adjusted_eps_analyzer.extract_key_facts import extract_quarterly_facts
    HAS_SEC_FALLBACK = True
except:
    HAS_SEC_FALLBACK = False

class TanukiDataFetcher:
    def __init__(self):
        self.av_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        self.cache_dir = "cache"
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, ticker: str, endpoint: str):
        return os.path.join(self.cache_dir, f"{ticker}_{endpoint}.json")

    def _is_cache_valid(self, path: str) -> bool:
        if not os.path.exists(path):
            return False
        age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(path))).total_seconds()
        return age < 86400

    def get_financials(self, ticker: str) -> dict:
        print(f"   [{ticker}] Alpha Vantage API 取得開始")

        # 1. Alpha Vantageメイン
        overview = self._fetch_av(ticker, "OVERVIEW")
        diluted_shares = float(overview.get("SharesOutstanding", 0) or 0) if overview else 0.0
        roe = 0.0
        if overview:
            roe_str = overview.get("ReturnOnEquityTTM", "0")
            roe = float(roe_str.replace("%", "")) / 100 if "%" in roe_str else float(roe_str) / 100

        # 2. INCOME / BALANCE で多角的取得
        for endpoint in ["INCOME_STATEMENT", "BALANCE_SHEET"]:
            data = self._fetch_av(ticker, endpoint)
            if data and "annualReports" in data:
                for report in data["annualReports"][:3]:
                    for key in ["commonStockSharesOutstanding", "weightedAverageShsOutDil", "weightedAverageShsOut",
                               "weightedAverageNumberOfDilutedSharesOutstanding", "sharesOutstanding"]:
                        val = float(report.get(key, 0) or 0)
                        if val > 100_000:
                            diluted_shares = max(diluted_shares, val)
                            print(f"   [{ticker}] {endpoint}から{key}取得成功: {val:,.0f}")
                            break

        # 3. SEC EDGARフォールバック（Alpha Vantageで取れなかった場合のみ・安全対策付き）
        if diluted_shares <= 100_000 and HAS_SEC_FALLBACK:
            print(f"   [{ticker}] Alpha Vantageでshares不足 → SEC EDGARフォールバック実行")
            try:
                quarterly_data = extract_quarterly_facts(ticker)
                if quarterly_data:
                    for q in quarterly_data[:5]:  # 最新5四半期のみ
                        for key in ["us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
                                   "us-gaap:CommonStockSharesOutstanding"]:
                            if key in q and isinstance(q[key], dict) and q[key].get("value", 0) > 100_000:
                                diluted_shares = max(diluted_shares, float(q[key]["value"]))
                                print(f"   [{ticker}] SEC EDGARからdiluted_shares取得成功: {diluted_shares:,.0f}")
                                break
                time.sleep(3)  # rate limit対策
            except Exception as e:
                print(f"   [{ticker}] SECフォールバック失敗: {e}")

        # 4. FCF
        cf_data = self._fetch_av(ticker, "CASH_FLOW")
        fcf_list = []
        if cf_data and "annualReports" in cf_data:
            for report in cf_data["annualReports"][:5]:
                ocf = float(report.get("operatingCashflow", 0))
                capex = abs(float(report.get("capitalExpenditures", 0)))
                fcf = ocf - capex
                fcf_list.append(fcf)

        fcf_avg = sum(fcf_list) / len(fcf_list) if fcf_list else 0.0

        # 5. 株価
        quote = self._fetch_av(ticker, "GLOBAL_QUOTE")
        current_price = float(quote.get("Global Quote", {}).get("05. price", 0)) if quote else 0.0

        print(f"   [{ticker}] FCF 5yr Avg = {fcf_avg:,.0f} | diluted_shares = {diluted_shares:,.0f} | ROE = {roe:.1%}")

        return {
            "fcf_5yr_avg": fcf_avg,
            "diluted_shares": diluted_shares,
            "roe_10yr_avg": roe,
            "current_price": current_price,
            "fcf_list_raw": fcf_list,
            "eps_data": {"ticker": ticker}
        }

    def _fetch_av(self, ticker: str, function: str):
        cache_path = self._get_cache_path(ticker, function)
        if self._is_cache_valid(cache_path):
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
            else:
                print(f"   [AV ERROR {ticker} {function}] {resp.status_code}")
                return None
        except Exception as e:
            print(f"   [AV EXCEPTION {ticker} {function}] {e}")
            return None
