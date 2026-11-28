"""
common/yfinance_utils.py
yfinance を安全に呼び出すユーティリティ関数。

全ての yfinance 呼び出しはこのモジュール経由で行い、
ネットワーク障害時のリトライ・ログ出力を一元化する。
"""

import logging
import time

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def safe_yf_ticker(ticker: str, retries: int = 2, wait: float = 3.0):
    """
    yfinance Ticker を安全に取得する。
    失敗時は retries 回リトライ後 None を返す。
    """
    for attempt in range(retries + 1):
        try:
            t = yf.Ticker(ticker)
            _ = t.fast_info  # 軽量な疎通確認
            return t
        except Exception as e:
            logger.warning(f"[yfinance] {ticker} attempt {attempt + 1} failed: {e}")
            if attempt < retries:
                time.sleep(wait)
    logger.error(f"[yfinance] {ticker} 全リトライ失敗 → None を返す")
    return None

