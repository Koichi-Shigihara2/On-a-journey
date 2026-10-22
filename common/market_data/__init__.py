"""
common/market_data — yfinance統合層（新DB構築プロジェクト フェーズ1）

BACKLOG [[MARKETDATA-LAYER-CONSTRUCTION-1]] の設計に基づく一次データ層。
common/sec_data/ と同型構成（fetcher.py がネットワーク取得・保存前検証・
保存を担当、reader.py が読み取りAPIを提供）。

保存構造:
    common/market_data/
        daily/{SYMBOL}.json            日次価格層（.history()/yf.download()由来）
        attributes/{SYMBOL}.json       週次準静的属性層（.info由来のスナップショット）
        analyst_history/{SYMBOL}.json  イベント履歴層（upgrades_downgrades由来）
        （{SYMBOL}/market_data_violations_log.json は読み手0件のため2026-10-22に削除）

現時点の消費者はまだ存在しないグリーンフィールドモジュールであり、
既存システムへの依存・影響はない。

使用例:
    from common.market_data import reader

    latest = reader.get_latest_price("AAPL")
    series = reader.get_price_series("AAPL", days=30)
    ma_dev = reader.get_ma_deviation("AAPL", window=50)
    attrs = reader.get_attributes("AAPL")
"""

from . import fetcher
from . import reader

__all__ = ["fetcher", "reader"]
