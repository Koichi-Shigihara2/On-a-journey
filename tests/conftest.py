"""テスト共通の設定。

[[MARKETDATA-DAILY-CLOSE-NONE-RECUR-1]]（2026-09-30）: fetch_daily_prices()は終値の無い足を数分おきに取り直し、
最後にyf.Ticker().history()を試す。テストでは待ち時間とネットワークアクセスが発生しないよう、既定で
取り直しを0回にし、別経路の取得を空にする。取り直しそのものを確かめるテストは引数で回数・待ち時間を渡す。

[[TEST-SYSMODULES-MOCK-LEAK-1]]（2026-10-07）: 収集の完了時点で、TANUKI VALUATIONの依存モジュールが
sys.modules上でMagicMockのまま残っていないかを確かめる。残っていると、以後のテストの実行時のimport
（core_calculator.py内の`from data_fetcher import ...`等）が本物ではなくMagicMockを受け取り、
比較を伴わないテストは本物のコードを実行せずに成功しうる。スタブが要るテストは
`_tanuki_pipeline_stub.load_stubbed_pipeline()`を使う（スタブはpipelineのimportの間だけ入れて戻す）。
"""
import sys
from unittest import mock

import pytest

_NO_MOCK_MODULES = ("data_fetcher", "core_calculator", "validator", "growth_sanity", "xlrd")


def pytest_collection_finish(session):
    leaked = [n for n in _NO_MOCK_MODULES if isinstance(sys.modules.get(n), mock.NonCallableMock)]
    if leaked:
        pytest.exit(
            "[TEST-SYSMODULES-MOCK-LEAK-1] 収集の完了時点でsys.modulesにMagicMockが残っている: "
            + ", ".join(leaked)
            + "（スタブはtests/_tanuki_pipeline_stub.pyのload_stubbed_pipeline()で入れ、終わったら戻す）",
            returncode=1,
        )


@pytest.fixture(autouse=True)
def _no_close_retry_network(monkeypatch):
    try:
        from common.market_data import fetcher
    except Exception:
        return
    monkeypatch.setattr(fetcher, "CLOSE_RETRY_ATTEMPTS", 0)
    monkeypatch.setattr(fetcher, "CLOSE_RETRY_WAIT_SEC", 0)
    monkeypatch.setattr(fetcher, "_history_bars_single", lambda symbol, start: [])
    monkeypatch.setattr(fetcher, "_futures_contract", lambda symbol: None)
