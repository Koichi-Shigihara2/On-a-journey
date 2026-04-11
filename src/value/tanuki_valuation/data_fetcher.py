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
from typing import Dict, Any, Optional, Tuple

# yfinance
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# SEC EDGAR - common/sec_data/reader.py
HAS_SEC = False
SECDataReader = None

# デバッグ出力
print("[DEBUG] data_fetcher.py loading...")
print(f"[DEBUG] __file__: {os.path.abspath(__file__)}")
print(f"[DEBUG] cwd: {os.getcwd()}")
print(f"[DEBUG] GITHUB_WORKSPACE: {os.environ.get('GITHUB_WORKSPACE', 'not set')}")

# 方法1: __file__ ベースのパス解決
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # src/value/tanuki_valuation → src/value → src → repo_root
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    common_path = os.path.join(repo_root, "common", "sec_data")
    
    print(f"[DEBUG] Method 1: repo_root={repo_root}")
    print(f"[DEBUG] Method 1: common_path={common_path}")
    print(f"[DEBUG] Method 1: exists={os.path.exists(common_path)}")
    
    if os.path.exists(common_path):
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
        from common.sec_data.reader import SECDataReader
        HAS_SEC = True
        print("[DEBUG] Method 1: SUCCESS - SECDataReader loaded")
except Exception as e:
    print(f"[DEBUG] Method 1: FAILED - {e}")

# 方法2: GITHUB_WORKSPACE 環境変数
if not HAS_SEC:
    try:
        github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        print(f"[DEBUG] Method 2: GITHUB_WORKSPACE={github_workspace}")
        
        if github_workspace:
            common_path = os.path.join(github_workspace, "common", "sec_data")
            print(f"[DEBUG] Method 2: common_path={common_path}")
            print(f"[DEBUG] Method 2: exists={os.path.exists(common_path)}")
            
            if os.path.exists(common_path):
                if github_workspace not in sys.path:
                    sys.path.insert(0, github_workspace)
                from common.sec_data.reader import SECDataReader
                HAS_SEC = True
                print("[DEBUG] Method 2: SUCCESS - SECDataReader loaded")
    except Exception as e:
        print(f"[DEBUG] Method 2: FAILED - {e}")

# 方法3: cwd ベース（working-directory: src/value/tanuki_valuation の場合）
if not HAS_SEC:
    try:
        cwd = os.getcwd()
        # src/value/tanuki_valuation → src/value → src → repo_root
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(cwd)))
        common_path = os.path.join(repo_root, "common", "sec_data")
        
        print(f"[DEBUG] Method 3: cwd-based repo_root={repo_root}")
        print(f"[DEBUG] Method 3: common_path={common_path}")
        print(f"[DEBUG] Method 3: exists={os.path.exists(common_path)}")
        
        if os.path.exists(common_path):
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            from common.sec_data.reader import SECDataReader
            HAS_SEC = True
            print("[DEBUG] Method 3: SUCCESS - SECDataReader loaded")
    except Exception as e:
        print(f"[DEBUG] Method 3: FAILED - {e}")

# 方法4: ディレクトリ一覧で確認
if not HAS_SEC:
    try:
        # cwdの親ディレクトリ構造を表示
        cwd = os.getcwd()
        print(f"[DEBUG] Method 4: Listing directory structure from cwd...")
        
        # 3階層上まで表示
        for i, path in enumerate([cwd, os.path.dirname(cwd), os.path.dirname(os.path.dirname(cwd)), os.path.dirname(os.path.dirname(os.path.dirname(cwd)))]):
            if os.path.exists(path):
                contents = os.listdir(path)
                print(f"[DEBUG]   Level -{i}: {path}")
                print(f"[DEBUG]     Contents: {contents[:10]}...")  # 最初の10個
                
                # commonがあれば詳細表示
                if "common" in contents:
                    common_full = os.path.join(path, "common")
                    print(f"[DEBUG]     FOUND 'common' at {common_full}")
                    print(f"[DEBUG]     common contents: {os.listdir(common_full)}")
                    
                    # sec_dataがあればインポート試行
                    if "sec_data" in os.listdir(common_full):
                        sec_data_full = os.path.join(common_full, "sec_data")
                        print(f"[DEBUG]     sec_data contents: {os.listdir(sec_data_full)}")
                        
                        # インポート試行
                        if path not in sys.path:
                            sys.path.insert(0, path)
                        from common.sec_data.reader import SECDataReader
                        HAS_SEC = True
                        print(f"[DEBUG] Method 4: SUCCESS - SECDataReader loaded from {path}")
                        break
    except Exception as e:
        print(f"[DEBUG] Method 4: FAILED - {e}")

print(f"[DEBUG] Final: HAS_SEC={HAS_SEC}, HAS_YFINANCE={HAS_YFINANCE}")


class TanukiDataFetcher:
    """TANUKI VALUATION 用データフェッチャー v2.2"""
    
    def __init__(self):
        self.sec_reader = SECDataReader() if HAS_SEC else None
        if self.sec_reader:
            print("[INFO] SECDataReader initialized")
        else:
            print("[WARN] SECDataReader not available - FCF/ROE will be 0")
    
    def get_financials(self, ticker: str) -> Dict[str, Any]:
        """財務データ取得メイン関数"""
        print(f"\n   [{ticker}] データ取得開始")
        
        fcf_list = []
        fcf_avg = 0.0
        sec_diluted = 0
        roe_avg = 0.0
        revenue = 0.0
        rpo = 0.0
        
        # 1. SEC EDGAR
        if self.sec_reader:
            try:
                annual = self.sec_reader.get_annual_data(ticker, years=10)
                
                if annual and len(annual) > 0:
                    for yr in annual[:5]:
                        ocf = yr.get("operating_cash_flow", 0) or 0
                        capex = abs(yr.get("capital_expenditures", 0) or 0)
                        fcf_list.append(ocf - capex)
                    
                    if fcf_list:
                        fcf_avg = sum(fcf_list) / len(fcf_list)
                        print(f"   [{ticker}] SEC FCF 5yr avg: ${fcf_avg:,.0f}")
                        print(f"   [{ticker}] SEC FCF list: {len(fcf_list)}年分")
                    
                    sec_diluted = annual[0].get("diluted_shares", 0) or 0
                    if sec_diluted > 0:
                        print(f"   [{ticker}] SEC shares: {sec_diluted:,.0f}")
                    
                    roe_list = []
                    for yr in annual:
                        r = yr.get("return_on_equity", 0) or 0
                        if r > 0:
                            roe_list.append(r)
                        else:
                            break
                    roe_avg = sum(roe_list) / len(roe_list) if roe_list else 0.0
                    print(f"   [{ticker}] SEC ROE avg: {roe_avg:.1%}")
                    
                    revenue = annual[0].get("total_revenue", 0) or 0
                    print(f"   [{ticker}] SEC revenue: ${revenue:,.0f}")
                    
                    rpo = annual[0].get("remaining_performance_obligation", 0) or 0
                    if rpo > 0:
                        print(f"   [{ticker}] SEC RPO: ${rpo:,.0f}")
                        
            except Exception as e:
                print(f"   [{ticker}] SEC取得エラー: {e}")
        
        # 2. yfinance
        yf_implied = 0
        yf_outstanding = 0
        current_price = 0.0
        
        if HAS_YFINANCE:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                yf_implied = info.get("impliedSharesOutstanding", 0) or 0
                if yf_implied > 0:
                    print(f"   [{ticker}] yfinance implied shares: {yf_implied:,.0f}")
                
                yf_outstanding = info.get("sharesOutstanding", 0) or 0
                if yf_outstanding > 0:
                    print(f"   [{ticker}] yfinance outstanding shares: {yf_outstanding:,.0f}")
                
                current_price = (
                    info.get("currentPrice") or 
                    info.get("regularMarketPrice") or 
                    info.get("previousClose") or 0
                )
                if current_price > 0:
                    print(f"   [{ticker}] yfinance price: ${current_price:.2f}")
                    
            except Exception as e:
                print(f"   [{ticker}] yfinance取得エラー: {e}")
        
        # 3. 株式数決定
        final_shares, shares_source = self._determine_diluted_shares(
            ticker, yf_implied, yf_outstanding, sec_diluted
        )
        
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
        self, ticker: str, yf_implied: int, yf_outstanding: int, sec_diluted: int
    ) -> Tuple[int, str]:
        """完全希薄化後株式数を決定"""
        MIN_SHARES = 100_000
        
        if yf_implied > MIN_SHARES:
            print(f"   [{ticker}] → yfinance implied採用（完全希薄化後）")
            return int(yf_implied), "yf_implied"
        
        has_sec = sec_diluted > MIN_SHARES
        has_yf = yf_outstanding > MIN_SHARES
        
        if has_sec and has_yf:
            ratio = yf_outstanding / sec_diluted
            
            if ratio > 5:
                print(f"   [{ticker}] ⚠️ 大規模増資検出: yf={yf_outstanding:,.0f} vs SEC={sec_diluted:,.0f} (×{ratio:.1f})")
                print(f"   [{ticker}] → yfinance outstanding採用（増資後の現在値）")
                return int(yf_outstanding), "yf_outstanding_post_dilution"
            elif ratio < 0.2:
                print(f"   [{ticker}] → SEC diluted採用")
                return int(sec_diluted), "sec_diluted"
            else:
                max_shares = max(sec_diluted, yf_outstanding)
                source = "max_sec" if sec_diluted >= yf_outstanding else "max_yf"
                print(f"   [{ticker}] → max採用: {max_shares:,.0f} ({source})")
                return int(max_shares), source
        
        if has_yf:
            print(f"   [{ticker}] → yfinance outstanding採用")
            return int(yf_outstanding), "yf_outstanding"
        
        if has_sec:
            print(f"   [{ticker}] → SEC diluted採用")
            return int(sec_diluted), "sec_diluted"
        
        print(f"   [{ticker}] ⚠️ 株式数取得不可")
        return 0, "none"
