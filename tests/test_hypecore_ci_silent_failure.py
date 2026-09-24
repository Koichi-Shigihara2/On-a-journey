"""
tests/test_hypecore_ci_silent_failure.py

[[HYPECORE-CI-SILENT-FAILURE-1]]の回帰テスト。
- market_data層がimportできない場合・失敗率が閾値超の場合はexit 1扱い
  （数銘柄の通常の取得失敗では落とさない）
- _safe_round()が文字列"Infinity"/"NaN"もnullへ変換する（ZETA_poc.jsonの
  trailing_pe=Infinity混入の再発防止）

実行方法:
    python -m pytest tests/test_hypecore_ci_silent_failure.py -v
"""

import json
import os
import sys

import pytest

_HYPECORE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "hypecore")
)
if _HYPECORE_DIR not in sys.path:
    sys.path.insert(0, _HYPECORE_DIR)

import hypecore  # noqa: E402


def test_fatal_when_market_data_unavailable():
    assert hypecore._fatal_reason(False) is not None
    assert hypecore._fatal_reason(False, 102, 0) is not None


@pytest.mark.parametrize("n_failed", [0, 1, 5, 20])
def test_not_fatal_for_few_failures(n_failed):
    # 102銘柄中20銘柄（19.6%）までは通常の取得失敗としてworkflowを落とさない
    assert hypecore._fatal_reason(True, 102, n_failed) is None


@pytest.mark.parametrize("n_failed", [21, 50, 102])
def test_fatal_for_mass_failure(n_failed):
    assert hypecore._fatal_reason(True, 102, n_failed) is not None


@pytest.mark.parametrize("v", ["Infinity", "-Infinity", "NaN", float("inf"), float("nan"), None, "abc"])
def test_safe_round_non_finite_to_none(v):
    assert hypecore._safe_round(v) is None


@pytest.mark.parametrize("v,expected", [(1.23456, 1.235), ("25.6789", 25.679), (3, 3.0)])
def test_safe_round_finite(v, expected):
    assert hypecore._safe_round(v) == expected


def test_month_record_is_strict_json_serializable():
    rec = hypecore._build_month_record(
        __import__("pandas").Timestamp("2026-09-30"),
        {"stage": 3, "trailing_pe": "Infinity", "forward_pe": 25.679},
    )
    assert rec["trailing_pe"] is None
    json.dumps(rec, allow_nan=False)
