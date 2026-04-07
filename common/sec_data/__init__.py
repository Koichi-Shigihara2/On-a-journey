"""
共通SECデータモジュール

使用例:
    from common.sec_data import reader
    
    # TANUKI VALUATION用
    fcf_avg = reader.get_fcf_5yr_avg("TSLA")
    roe_avg = reader.get_roe_avg("TSLA")
    shares = reader.get_diluted_shares("TSLA")
    
    # Adjusted EPS Analyzer用
    eps = reader.get_eps_diluted("TSLA", "2024Q3")
    
    # 生データアクセス
    annual = reader.get_annual("TSLA", 2024)
    quarterly = reader.get_quarterly("TSLA", "2024Q3")
"""

from .config import TICKERS, get_all, get_holdings, get_watchlist, get_ticker_info
from .fetcher import SECFetcher
from .parser import SECParser
from .reader import SECReader, get_reader

__all__ = [
    # Config
    "TICKERS",
    "get_all",
    "get_holdings",
    "get_watchlist",
    "get_ticker_info",
    
    # Classes
    "SECFetcher",
    "SECParser",
    "SECReader",
    
    # Singleton
    "get_reader",
]
