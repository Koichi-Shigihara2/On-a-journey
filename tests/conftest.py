"""テスト共通の設定。

[[MARKETDATA-DAILY-CLOSE-NONE-RECUR-1]]（2026-09-30）: fetch_daily_prices()は終値の無い足を数分おきに取り直し、
最後にyf.Ticker().history()を試す。テストでは待ち時間とネットワークアクセスが発生しないよう、既定で
取り直しを0回にし、別経路の取得を空にする。取り直しそのものを確かめるテストは引数で回数・待ち時間を渡す。
"""
import pytest


@pytest.fixture(autouse=True)
def _no_close_retry_network(monkeypatch):
    try:
        from common.market_data import fetcher
    except Exception:
        return
    monkeypatch.setattr(fetcher, "CLOSE_RETRY_ATTEMPTS", 0)
    monkeypatch.setattr(fetcher, "CLOSE_RETRY_WAIT_SEC", 0)
    monkeypatch.setattr(fetcher, "_history_bars_single", lambda symbol, start: [])
