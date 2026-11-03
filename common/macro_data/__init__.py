"""
common/macro_data — FRED統合層（新DB構築プロジェクト フェーズ1）

BACKLOG [[MACRODATA-LAYER-CONSTRUCTION-1]] の設計に基づく一次データ層。
common/market_data/ と同型構成（fetcher.py がネットワーク取得・保存前
検証・保存を担当、reader.py が読み取りAPIを提供）。

保存構造:
    common/macro_data/
        series/{SERIES_ID}.json          系列単位の時系列ストア
        series_meta.json                  系列単位のメタ情報（fred_release_id等）
        macro_data_violations_log.json    保存前検証の結果ログ

【読み手】
series/{SERIES_ID}.json は `05_main.py`（MACRO PULSE、2026-08-12に切り替え済み）・`collect_and_send.py`
（Market Pulse）が reader.py 経由で読み、macro_data_violations_log.json は System Health の [L] が読む。
読み手の正は出力の一覧 docs/architecture/output_inventory/macro_data_layer.md（[[CHECK-REFERENCE-DEFINITION-1]]、2026-11-03にグリーンフィールド実装のころの古い記述を直した）。

使用例:
    from common.macro_data import reader

    latest = reader.get_latest("BAMLH0A0HYM2")
    series = reader.get_series("T10Y2Y", start="2026-01-01")
    val_on = reader.get_value_as_of("PAYEMS", "2026-07-15")
"""

from . import fetcher
from . import reader

__all__ = ["fetcher", "reader"]
