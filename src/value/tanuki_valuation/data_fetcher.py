"""
TANUKI VALUATION - Data Fetcher v2.2
SEC EDGAR + yfinance ハイブリッド取得（マイクロキャップ対応）

v2.2 変更点:
- 完全希薄化後株式数の取得ロジック強化
- yfinance impliedSharesOutstanding 最優先
- SEC diluted vs yfinance outstanding の max 採用
- 大規模増資検出（乖離5倍以上）→ yfinance優先
"""

import os
import sys
from typing import Dict, Any, Tuple

# yfinance
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# SEC EDGAR - common/sec_data/reader.py
# クラス名は SECReader（SECDataReaderではない）
HAS_SEC = False
SECReader = None

# パス解決
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    
    from common.sec_data.reader import SECReader
    HAS_SEC = True
except Exception as e:
    pass

# フォールバック: GITHUB_WORKSPACE
if not HAS_SEC:
    try:
        github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if github_workspace and github_workspace not in sys.path:
            sys.path.insert(0, github_workspace)
        from common.sec_data.reader import SECReader
        HAS_SEC = True
    except Exception as e:
        pass


class TanukiDataFetcher:
    """
    TANUKI VALUATION 用データフェッチャー v2.2
    
    データソース優先順位:
    1. 株式数: yfinance implied > max(SEC diluted, yfinance outstanding)
    2. FCF/Revenue/ROE/RPO: SEC XBRL (SECReaderのヘルパーメソッド使用)
    3. 株価: yfinance
    """
    
    def __init__(self):
        self.sec_reader = SECReader() if HAS_SEC else None
    
    def get_financials(self, ticker: str) -> Dict[str, Any]:
        """財務データ取得メイン関数"""
        print(f"\n   [{ticker}] データ取得開始")
        
        fcf_list = []
        fcf_avg = 0.0
        sec_diluted = 0
        roe_avg = 0.0
        revenue = 0.0
        rpo = 0.0
        
        # ========================================
        # 1. SEC EDGAR - SECReaderのヘルパーメソッド使用
        # ========================================
        if self.sec_reader:
            try:
                # FCF 5年平均
                fcf_avg = self.sec_reader.get_fcf_5yr_avg(ticker)
                print(f"   [{ticker}] SEC FCF 5yr avg: ${fcf_avg:,.0f}")
                
                # FCFリスト
                fcf_list = self.sec_reader.get_fcf_list(ticker, years=5)
                print(f"   [{ticker}] SEC FCF list: {len(fcf_list)}年分")
                
                # 希薄化後株式数
                sec_diluted = self.sec_reader.get_diluted_shares(ticker)
                if sec_diluted > 0:
                    print(f"   [{ticker}] SEC shares: {sec_diluted:,.0f}")
                
                # ROE平均（連続黒字期間）
                roe_avg = self.sec_reader.get_roe_avg(ticker, years=10)
                print(f"   [{ticker}] SEC ROE avg: {roe_avg:.1%}")
                
                # 売上高
                revenue = self.sec_reader.get_latest_revenue(ticker)
                print(f"   [{ticker}] SEC revenue: ${revenue:,.0f}")
                
                # RPO
                rpo = self.sec_reader.get_rpo(ticker)
                if rpo > 0:
                    print(f"   [{ticker}] SEC RPO: ${rpo:,.0f}")
                    
            except Exception as e:
                print(f"   [{ticker}] SEC取得エラー: {e}")
        
        # ========================================
        # 2. yfinance から株式数と株価を取得
        # ========================================
        yf_implied = 0
        yf_outstanding = 0
        current_price = 0.0
        
        if HAS_YFINANCE:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # 完全希薄化後株式数（最優先）
                yf_implied = info.get("impliedSharesOutstanding", 0) or 0
                if yf_implied > 0:
                    print(f"   [{ticker}] yfinance implied shares: {yf_implied:,.0f}")
                
                # 発行済株式数
                yf_outstanding = info.get("sharesOutstanding", 0) or 0
                if yf_outstanding > 0:
                    print(f"   [{ticker}] yfinance outstanding shares: {yf_outstanding:,.0f}")
                
                # 株価
                current_price = (
                    info.get("currentPrice") or 
                    info.get("regularMarketPrice") or 
                    info.get("previousClose") or 0
                )
                if current_price > 0:
                    print(f"   [{ticker}] yfinance price: ${current_price:.2f}")
                    
            except Exception as e:
                print(f"   [{ticker}] yfinance取得エラー: {e}")
        
        # ========================================
        # 3. 完全希薄化後株式数の決定
        # ========================================
        final_shares, shares_source = self._determine_diluted_shares(
            ticker, yf_implied, yf_outstanding, sec_diluted
        )
        
        # ========================================
        # 最終サマリー
        # ========================================
        print(f"   [{ticker}] 最終結果:")
        print(f"       FCF 5yr Avg: ${fcf_avg:,.0f}")
        print(f"       Diluted Shares: {final_shares:,.0f} ({shares_source})")
        print(f"       ROE avg: {roe_avg:.1%}")
        print(f"       Current Price: ${current_price:.2f}")
        print(f"       Revenue: ${revenue:,.0f}")
        if rpo > 0:
            print(f"       RPO: ${rpo:,.0f}")
        
        return {
            "fcf_5yr_avg": fcf_avg,
            "fcf_list_raw": fcf_list,
            "diluted_shares": final_shares,
            "roe_10yr_avg": roe_avg,
            "current_price": current_price,
            "latest_revenue": revenue,
            "rpo": rpo,
            "eps_data": {"ticker": ticker},
            "_shares_source": shares_source
        }
    
    def _determine_diluted_shares(
        self, 
        ticker: str,
        yf_implied: int, 
        yf_outstanding: int, 
        sec_diluted: int
    ) -> Tuple[int, str]:
        """
        完全希薄化後株式数を決定
        
        優先順位:
        1. yfinance impliedSharesOutstanding（取得できれば最優先）
        2. 大規模増資検出（乖離5倍以上）→ yfinance outstanding
        3. max(SEC diluted, yfinance outstanding)
        """
        MIN_SHARES = 100_000
        
        # 1. yfinance implied（完全希薄化後）
        if yf_implied > MIN_SHARES:
            print(f"   [{ticker}] → yfinance implied採用（完全希薄化後）")
            return int(yf_implied), "yf_implied"
        
        # 2. SEC vs yfinance 比較
        has_sec = sec_diluted > MIN_SHARES
        has_yf = yf_outstanding > MIN_SHARES
        
        if has_sec and has_yf:
            ratio = yf_outstanding / sec_diluted
            
            if ratio > 5:
                # 大規模増資
                print(f"   [{ticker}] ⚠️ 大規模増資検出: yf={yf_outstanding:,.0f} vs SEC={sec_diluted:,.0f} (×{ratio:.1f})")
                print(f"   [{ticker}] → yfinance outstanding採用（増資後の現在値）")
                return int(yf_outstanding), "yf_outstanding_post_dilution"
            
            elif ratio < 0.2:
                # 株式併合など
                print(f"   [{ticker}] → SEC diluted採用")
                return int(sec_diluted), "sec_diluted"
            
            else:
                # 通常ケース → max
                max_shares = max(sec_diluted, yf_outstanding)
                source = "max_sec" if sec_diluted >= yf_outstanding else "max_yf"
                print(f"   [{ticker}] → max採用: {max_shares:,.0f} ({source})")
                return int(max_shares), source
        
        # 3. どちらか一方のみ
        if has_yf:
            print(f"   [{ticker}] → yfinance outstanding採用")
            return int(yf_outstanding), "yf_outstanding"
        
        if has_sec:
            print(f"   [{ticker}] → SEC diluted採用")
            return int(sec_diluted), "sec_diluted"
        
        print(f"   [{ticker}] ⚠️ 株式数取得不可")
        return 0, "none"


# スタンドアロンテスト
if __name__ == "__main__":
    print(f"HAS_SEC: {HAS_SEC}")
    print(f"HAS_YFINANCE: {HAS_YFINANCE}")
    
    fetcher = TanukiDataFetcher()
    result = fetcher.get_financials("ONDS")
    print(f"\nONDS: {result['diluted_shares']:,} ({result['_shares_source']})")
