"""
TANUKI VALUATION - Data Fetcher v2.4
SEC EDGAR + yfinance ハイブリッド取得（マイクロキャップ対応 + β取得）

v2.4 変更点:
- beta_config.json によるKoichi意図βを最優先採用
  優先順位: beta_config.json override > yfinance β > セクターデフォルト
- β採用理由をログ出力・latest.jsonに記録
"""

import os
import sys
import json
from typing import Dict, Any, Optional, Tuple

# yfinance
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

# SEC EDGAR - common/sec_data/reader.py
HAS_SEC = False
SECReader = None

try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    
    from common.sec_data.reader import SECReader
    HAS_SEC = True
except Exception:
    pass

if not HAS_SEC:
    try:
        github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if github_workspace and github_workspace not in sys.path:
            sys.path.insert(0, github_workspace)
        from common.sec_data.reader import SECReader
        HAS_SEC = True
    except Exception:
        pass


# セクター別デフォルトβ
SECTOR_DEFAULT_BETA = {
    "Technology": 1.20,
    "Consumer Cyclical": 1.10,
    "Consumer Defensive": 0.80,
    "Communication Services": 1.00,
    "Healthcare": 0.90,
    "Financial Services": 1.30,
    "Industrials": 1.10,
    "Energy": 1.15,
    "Basic Materials": 1.10,
    "Real Estate": 0.90,
    "Utilities": 0.50,
    "default": 1.00
}


def _load_beta_config() -> Dict[str, Any]:
    """
    config/beta_config.json を読み込む

    GitHub Actions 実行時は GITHUB_WORKSPACE から、
    ローカル実行時はファイルの相対パスで解決する。
    """
    search_paths = []

    # GitHub Actions 環境
    workspace = os.environ.get("GITHUB_WORKSPACE", "")
    if workspace:
        search_paths.append(os.path.join(workspace, "config", "beta_config.json"))

    # ローカル: このファイルから4階層上がるとリポジトリルート
    try:
        current = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current)))
        search_paths.append(os.path.join(repo_root, "config", "beta_config.json"))
    except Exception:
        pass

    for path in search_paths:
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"   [WARN] beta_config.json 読み込みエラー: {e}")
                return {}

    return {}


class TanukiDataFetcher:
    """
    TANUKI VALUATION 用データフェッチャー v2.3
    
    データソース優先順位:
    1. 株式数: yfinance implied > max(SEC diluted, yfinance outstanding)
    2. FCF/Revenue/ROE/RPO: SEC XBRL (SECReaderのヘルパーメソッド使用)
    3. 株価/β: yfinance
    """
    
    def __init__(self):
        self.sec_reader = SECReader() if HAS_SEC else None
        # beta_config.json をロード（起動時1回のみ）
        _cfg = _load_beta_config()
        self._beta_overrides: Dict[str, Any] = _cfg.get("overrides", {})
        if self._beta_overrides:
            print(f"   [INFO] beta_config.json: {len(self._beta_overrides)}銘柄のβオーバーライドを読み込みました")
    
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
        # 1. SEC EDGAR
        # ========================================
        if self.sec_reader:
            try:
                fcf_avg = self.sec_reader.get_fcf_5yr_avg(ticker)
                print(f"   [{ticker}] SEC FCF 5yr avg: ${fcf_avg:,.0f}")
                
                fcf_list = self.sec_reader.get_fcf_list(ticker, years=5)
                print(f"   [{ticker}] SEC FCF list: {len(fcf_list)}年分")
                
                sec_diluted = self.sec_reader.get_diluted_shares(ticker)
                if sec_diluted > 0:
                    print(f"   [{ticker}] SEC shares: {sec_diluted:,.0f}")
                
                roe_avg = self.sec_reader.get_roe_avg(ticker, years=10)
                print(f"   [{ticker}] SEC ROE avg: {roe_avg:.1%}")
                
                revenue = self.sec_reader.get_latest_revenue(ticker)
                print(f"   [{ticker}] SEC revenue: ${revenue:,.0f}")
                
                rpo = self.sec_reader.get_rpo(ticker)
                if rpo > 0:
                    print(f"   [{ticker}] SEC RPO: ${rpo:,.0f}")
                    
            except Exception as e:
                print(f"   [{ticker}] SEC取得エラー: {e}")
        
        # ========================================
        # 2. yfinance（株式数、株価、β、セクター）
        # ========================================
        yf_implied = 0
        yf_outstanding = 0
        current_price = 0.0
        beta = None
        sector = "default"
        
        if HAS_YFINANCE:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # 株式数
                yf_implied = info.get("impliedSharesOutstanding", 0) or 0
                if yf_implied > 0:
                    print(f"   [{ticker}] yfinance implied shares: {yf_implied:,.0f}")
                
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
                
                # β（ベータ）
                beta = info.get("beta")
                if beta is not None and beta > 0:
                    print(f"   [{ticker}] yfinance beta: {beta:.2f}")
                
                # セクター
                sector = info.get("sector", "default")
                if sector and sector != "default":
                    print(f"   [{ticker}] yfinance sector: {sector}")
                    
            except Exception as e:
                print(f"   [{ticker}] yfinance取得エラー: {e}")
        
        # ========================================
        # 3. β決定（beta_config.json > yfinance > セクターデフォルト）
        # ========================================
        final_beta, beta_source = self._determine_beta(ticker, beta, sector)
        
        # ========================================
        # 4. 株式数決定
        # ========================================
        final_shares, shares_source = self._determine_diluted_shares(
            ticker, yf_implied, yf_outstanding, sec_diluted
        )
        
        # ========================================
        # FCF 直近2年平均の計算（CV方式のベース判定用）
        # ========================================
        fcf_2yr_avg = self._calc_fcf_2yr_avg(fcf_list)
        if fcf_2yr_avg > 0:
            print(f"   [{ticker}] SEC FCF 2yr avg: ${fcf_2yr_avg:,.0f}")

        # ========================================
        # 最終サマリー
        # ========================================
        print(f"   [{ticker}] 最終結果:")
        print(f"       FCF 5yr Avg: ${fcf_avg:,.0f}")
        print(f"       FCF 2yr Avg: ${fcf_2yr_avg:,.0f}")
        print(f"       Diluted Shares: {final_shares:,.0f} ({shares_source})")
        print(f"       ROE avg: {roe_avg:.1%}")
        print(f"       Current Price: ${current_price:.2f}")
        print(f"       Revenue: ${revenue:,.0f}")
        print(f"       Beta: {final_beta:.2f} ({beta_source})")
        if rpo > 0:
            print(f"       RPO: ${rpo:,.0f}")
        
        return {
            "fcf_5yr_avg": fcf_avg,
            "fcf_2yr_avg": fcf_2yr_avg,
            "fcf_list_raw": fcf_list,
            "diluted_shares": final_shares,
            "roe_10yr_avg": roe_avg,
            "current_price": current_price,
            "latest_revenue": revenue,
            "rpo": rpo,
            "beta": final_beta,
            "beta_yf_raw": float(beta) if (beta is not None and beta > 0) else None,
            "sector": sector,
            "eps_data": {"ticker": ticker},
            "_shares_source": shares_source,
            "_beta_source": beta_source
        }
    
    def _calc_fcf_2yr_avg(self, fcf_list: list) -> float:
        """
        FCFリストから直近2年平均を計算

        fcf_listはget_fcf_list()の返却値（新しい順、インデックス0が最新）。
        最新2件はリストの先頭[:2]で取得する。
        2件未満の場合は0.0を返す。
        """
        if not fcf_list or len(fcf_list) < 2:
            return 0.0
        recent_2 = fcf_list[:2]  # 新しい順なので先頭が最新
        if all(v is not None for v in recent_2):
            return sum(recent_2) / 2
        return 0.0

    def _determine_beta(
        self,
        ticker: str,
        yf_beta: float,
        sector: str
    ) -> Tuple[float, str]:
        """
        β（ベータ）を決定

        優先順位:
        1. beta_config.json の overrides（Koichi意図β）
        2. yfinanceのβ（0.1〜3.0の範囲内）
        3. セクター別デフォルトβ
        4. 全体デフォルト（1.0）
        """
        # ── 1. beta_config.json オーバーライド（最優先） ──
        override = self._beta_overrides.get(ticker)
        if override and isinstance(override.get("beta"), (int, float)):
            beta_val = float(override["beta"])
            reason   = override.get("reason", "")
            sector_label = override.get("sector", "")
            print(f"   [{ticker}] → beta_config.json採用: β={beta_val:.2f} ({sector_label})")
            if reason:
                print(f"   [{ticker}]   理由: {reason}")
            return beta_val, f"beta_config({sector_label})"

        # ── 2. yfinanceのβ（有効範囲: 0.1〜3.0） ──
        if yf_beta is not None and 0.1 <= yf_beta <= 3.0:
            return float(yf_beta), "yfinance"

        # ── 3. セクター別デフォルト ──
        sector_beta = SECTOR_DEFAULT_BETA.get(sector)
        if sector_beta:
            print(f"   [{ticker}] → セクターデフォルトβ採用: {sector} = {sector_beta}")
            return float(sector_beta), f"sector_{sector}"

        # ── 4. 全体デフォルト ──
        default_beta = SECTOR_DEFAULT_BETA["default"]
        print(f"   [{ticker}] → デフォルトβ採用: {default_beta}")
        return float(default_beta), "default"
    
    def _determine_diluted_shares(
        self, 
        ticker: str,
        yf_implied: int, 
        yf_outstanding: int, 
        sec_diluted: int
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
