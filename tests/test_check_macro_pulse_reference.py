"""browser_checks/check_macro_pulse.py の照合の相手（[[CHECK-REFERENCE-DEFINITION-1]]、2026-11-03）。

画面の値は画面が読むファイル（同じcommitの05_events.csv）と比べ、導出の確認は、そのCSVを書いた実行が読めた系列ストアの版
（CSVを書いた時刻の時点でリポジトリにあった版）と比べる。ストアの今の最新とは比べない。
2026-10-23 07:56 JST の5件の不一致（MAC-09・10、D-01・03・07）は、実行の時刻（ストアが先に新しい観測日を持ち、CSVが追いつく前）による。
そのcommitの状態の再現（git worktree・--now 2026-10-23T07:56:00+09:00、Playwright）は手元で確認: 古い比べ方 一致67・不一致5、新しい比べ方 一致72・不一致0。
ここでは比べ方の部品をネットワーク・ブラウザなしで確かめる。
"""
import importlib.util
import os
import sys
from datetime import datetime, timezone

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def cmp():
    spec = importlib.util.spec_from_file_location("check_macro_pulse", os.path.join(_REPO, "browser_checks", "check_macro_pulse.py"))
    m = importlib.util.module_from_spec(spec)
    sys.modules["check_macro_pulse"] = m          # dataclass が自分のモジュールを引くため
    spec.loader.exec_module(m)
    return m


def _ms(s):
    return int(datetime.fromisoformat(s).timestamp() * 1000)


def test_ticker_expected_comes_from_events_csv_asof(cmp):
    """MAC-09・10 の期待値は 05_events.csv の sp500_t0_asof ごとの終値（同じ観測日は後の行が勝つ・未来の観測日は使わない）"""
    ev = [{"sp500_t0": "7712.57", "sp500_t0_asof": "2026-10-20"},
          {"sp500_t0": "7690.73", "sp500_t0_asof": "2026-10-17"},
          {"sp500_t0": "7700.00", "sp500_t0_asof": "2026-10-20"},      # 同じ観測日の後の行
          {"sp500_t0": "9999.99", "sp500_t0_asof": "2026-10-24"},      # 未来の観測日
          {"sp500_t0": "", "sp500_t0_asof": "2026-10-21"}]
    assert cmp.sp500_closes_from_events(ev, _ms("2026-10-23T07:56:00+09:00")) == [("2026-10-17", 7690.73), ("2026-10-20", 7700.0)]


def test_csv_written_at_is_max_updated_at_utc(cmp):
    rows = [{"updated_at": "2026-10-22 03:04:11"}, {"updated_at": "2026-10-21 13:03:40"}, {"updated_at": ""}]
    assert cmp.csv_written_at(rows) == datetime(2026, 10, 22, 3, 4, 11, tzinfo=timezone.utc)
    assert cmp.csv_written_at([{"updated_at": ""}]) is None


def test_store_is_read_at_csv_write_time_not_latest(cmp, monkeypatch):
    """系列ストアは CSV を書いた時刻より前の最後の commit の版を読む（作業ツリーの今の最新は読まない）"""
    calls = []

    def fake_git(*args):
        calls.append(args)
        if args[0] == "log":
            return "abc1234567890 2026-10-22T02:10:00+00:00\n"
        if args[0] == "show":
            return '[{"as_of": "2026-10-20", "value": 7712.57}]'
        return ""
    monkeypatch.setattr(cmp, "_git_out", fake_git)
    monkeypatch.setattr(cmp, "load_series", lambda sid: pytest.fail("作業ツリーの今の最新を読んではいけない"))
    cmp._SERIES_AT.clear()
    recs, ver = cmp.load_series_at("SP500", datetime(2026, 10, 22, 3, 4, 11, tzinfo=timezone.utc))
    assert recs[-1]["as_of"] == "2026-10-20" and ver.startswith("abc1234567")
    assert "--before=2026-10-22 03:04:11 +0000" in calls[0] and calls[1] == ("show", "abc1234567890:common/macro_data/series/SP500.json")


def test_store_without_write_time_falls_back_to_worktree_and_says_so(cmp, monkeypatch):
    monkeypatch.setattr(cmp, "load_series", lambda sid: [{"as_of": "2026-10-21", "value": 1.0}])
    recs, ver = cmp.load_series_at("DGS1", None)
    assert recs[-1]["as_of"] == "2026-10-21" and "作業ツリー" in ver


def test_no_check_compares_screen_values_with_latest_store(cmp):
    """MAC-09・10・D-01・03・07 は load_series（作業ツリーの今の最新）を直接呼ばない"""
    import inspect
    src = inspect.getsource(cmp.compare) + inspect.getsource(cmp.derived_checks)
    assert "load_series(" not in src.replace("load_series_at(", "")
