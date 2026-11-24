"""
tests/test_shares_as_of.py

[[POINT-IN-TIME-HISTORY-INTEGRITY-1]] (4)（2026-11-24）: 倍率の履歴の株数は、その月末までに申告された値（as-of）。
後の10-Qが前年同期の値を丸めて置き換えても（CRWV・ONDS型）、過去の月の株数は変わらない。今日の行（current）・as_of 無しは最新の値のまま。
作りのデータ（銘柄名は使わない）。

実行方法:
    python -m pytest tests/test_shares_as_of.py -v
"""
from common import multiples as mp
from common.sec_data import split_adjust as sa

TAG = sa.WEIGHTED_DILUTED_TAG


def _store():
    # 2025-09-30 の四半期: 最初の申告 2025-11-05 は 1,000,000,000 株、2026-11-09 の10-Qが前年比較として丸めた 1,000,200,000 に置き換えた
    e1 = {"end": "2025-09-30", "start": "2025-07-01", "val": 1_000_200_000.0, "filed": "2026-11-09",
          "first_filed": "2025-11-05", "first_val": 1_000_000_000.0, "is_annual": False, "is_ytd": False, "source_tag": TAG}
    e2 = {"end": "2026-09-30", "start": "2026-07-01", "val": 1_100_000_000.0, "filed": "2026-11-09",
          "first_filed": "2026-11-09", "first_val": 1_100_000_000.0, "is_annual": False, "is_ytd": False, "source_tag": TAG}
    return {"fields": {"shares_diluted": {"entries": [e1, e2]}}}


def test_none_keeps_latest_value():
    out = sa.shares_by_end(_store(), ["2025-09-30", "2026-09-30"], [])
    assert out["2025-09-30"]["shares"] == 1_000_200_000.0


def test_as_of_uses_the_value_filed_by_then():
    s = _store()
    assert sa.shares_by_end(s, ["2025-09-30"], [], as_of="2025-12-31")["2025-09-30"]["shares"] == 1_000_000_000.0
    assert sa.shares_by_end(s, ["2025-09-30"], [], as_of="2026-11-30")["2025-09-30"]["shares"] == 1_000_200_000.0


def test_as_of_before_first_filing_has_no_value():
    assert sa.shares_by_end(_store(), ["2025-09-30"], [], as_of="2025-10-31") == {}


def test_as_of_without_filing_dates_keeps_value():
    e = {"end": "2025-09-30", "val": 5.0, "is_annual": False, "source_tag": TAG}
    assert sa.shares_by_end({"fields": {"shares_diluted": {"entries": [e]}}}, ["2025-09-30"], [], as_of="2025-12-31")["2025-09-30"]["shares"] == 5.0


def test_multiples_history_rows_use_as_of_and_current_stays_latest():
    prices = [{"date": d, "close": 10.0} for d in ("2025-12-31", "2026-12-31")]
    months = [("2025-12", "2025-12-31"), ("2026-12", "2026-12-31")]
    out = mp.compute("TEST", _store(), prices, months, [], lambda d: {}, [], current="2026-12-31")
    rows = {r["month"]: r for r in out["months"]}
    assert rows["2025-12"]["shares"] == 1_000_000_000.0          # 当時申告済みの値（後の丸めの置き換えは使わない）
    assert rows["2026-12"]["shares"] == 1_100_000_000.0
    assert out["current"]["shares_end"] == "2026-09-30"
    assert out["current"]["shares"] == 1_100_000_000.0           # 今日の行は変わらない


def test_as_of_converts_only_values_filed_before_the_split():
    # 分割（2026-03-01、1株→10株）。期末 2025-12-31 は分割の前の申告（2026-02-10）と後の申告（2026-05-10、換算済み）で値が違う
    splits = [{"date": "2026-03-01", "ratio": 10}]
    e = {"end": "2025-12-31", "start": "2025-10-01", "val": 1_000_000_000.0, "filed": "2026-05-10",
         "first_filed": "2026-02-10", "first_val": 100_000_000.0, "is_annual": False, "is_ytd": False, "source_tag": TAG}
    s = {"fields": {"shares_diluted": {"entries": [e]}}}
    assert sa.shares_by_end(s, ["2025-12-31"], splits, as_of="2026-02-28")["2025-12-31"]["shares"] == 1_000_000_000.0   # 100M×10（今の基準）
    assert sa.shares_by_end(s, ["2025-12-31"], splits, as_of="2026-06-30")["2025-12-31"]["shares"] == 1_000_000_000.0   # 換算済みの値
