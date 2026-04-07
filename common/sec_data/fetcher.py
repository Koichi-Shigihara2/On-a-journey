"""
SEC EDGAR データ取得モジュール
Company Facts API を使用してBS/PL/CFデータを取得・保存
"""

import requests
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any

from .config import get_all, get_ticker_info


class SECFetcher:
    """SEC EDGAR Company Facts API クライアント"""
    
    BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
    CIK_LOOKUP_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
    
    # SEC APIは10リクエスト/秒制限
    RATE_LIMIT_DELAY = 0.15
    
    def __init__(self, data_dir: str = None):
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(self.data_dir, exist_ok=True)
        
        # CIKキャッシュ
        self.cik_cache_path = os.path.join(self.data_dir, "_cik_cache.json")
        self.cik_cache = self._load_cik_cache()
        
        # User-Agent必須（SEC要件）
        self.headers = {
            "User-Agent": "Koichi Personal Investment Tools koichi@example.com",
            "Accept": "application/json"
        }
    
    def _load_cik_cache(self) -> dict:
        """CIKキャッシュ読み込み"""
        if os.path.exists(self.cik_cache_path):
            try:
                with open(self.cik_cache_path, "r") as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def _save_cik_cache(self):
        """CIKキャッシュ保存"""
        with open(self.cik_cache_path, "w") as f:
            json.dump(self.cik_cache, f, indent=2)
    
    def get_cik(self, ticker: str) -> Optional[str]:
        """ティッカーからCIKを取得（10桁ゼロパディング）"""
        ticker = ticker.upper()
        
        # キャッシュ確認
        if ticker in self.cik_cache:
            return self.cik_cache[ticker]
        
        # SEC ticker.json から取得
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            resp = requests.get(url, headers=self.headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for entry in data.values():
                    if entry.get("ticker", "").upper() == ticker:
                        cik = str(entry["cik_str"]).zfill(10)
                        self.cik_cache[ticker] = cik
                        self._save_cik_cache()
                        return cik
        except Exception as e:
            print(f"   [CIK ERROR] {ticker}: {e}")
        
        return None
    
    def fetch_company_facts(self, ticker: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Company Facts API から全データ取得
        
        Returns:
            dict: SEC Company Facts 生データ
        """
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        
        raw_path = os.path.join(ticker_dir, "company_facts.json")
        
        # キャッシュ確認（24時間有効）
        if not force_refresh and os.path.exists(raw_path):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(raw_path))).total_seconds()
            if age < 86400:
                try:
                    with open(raw_path, "r", encoding="utf-8") as f:
                        print(f"   [{ticker}] キャッシュから読み込み")
                        return json.load(f)
                except:
                    pass
        
        # CIK取得
        cik = self.get_cik(ticker)
        if not cik:
            print(f"   [{ticker}] CIK取得失敗")
            return None
        
        # Company Facts API呼び出し
        url = f"{self.BASE_URL}/CIK{cik}.json"
        print(f"   [{ticker}] SEC API取得中... (CIK: {cik})")
        
        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            resp = requests.get(url, headers=self.headers, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                
                # 保存
                with open(raw_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                print(f"   [{ticker}] SEC API取得完了")
                return data
            else:
                print(f"   [{ticker}] SEC API エラー: {resp.status_code}")
                return None
                
        except Exception as e:
            print(f"   [{ticker}] SEC API 例外: {e}")
            return None
    
    def fetch_all(self, tickers: list = None, force_refresh: bool = False) -> Dict[str, bool]:
        """
        複数ティッカーの一括取得
        
        Returns:
            dict: {ticker: success_flag}
        """
        if tickers is None:
            tickers = get_all()
        
        results = {}
        total = len(tickers)
        
        print(f"\n{'='*60}")
        print(f"SEC EDGAR データ一括取得開始（{total}銘柄）")
        print(f"{'='*60}")
        
        for i, ticker in enumerate(tickers, 1):
            print(f"\n[{i}/{total}] {ticker}")
            info = get_ticker_info(ticker)
            print(f"   {info['name']} ({info['status']})")
            
            data = self.fetch_company_facts(ticker, force_refresh)
            results[ticker] = data is not None
        
        # サマリー
        success = sum(1 for v in results.values() if v)
        print(f"\n{'='*60}")
        print(f"取得完了: {success}/{total}")
        print(f"{'='*60}")
        
        return results


if __name__ == "__main__":
    fetcher = SECFetcher()
    
    # テスト: 単一ティッカー
    data = fetcher.fetch_company_facts("TSLA")
    if data:
        print(f"\nTSLA facts keys: {list(data.get('facts', {}).keys())}")
