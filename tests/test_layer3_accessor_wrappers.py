"""
tests/test_layer3_accessor_wrappers.py

移行実装計画（SEC_EDGAR_LAYER_DESIGN.md 8章）フェーズD Step1対応。

layer3_builder.py::get_quarterly_series() / get_latest_quarterly() の
単体テスト。reader.py側（normalized/経由）の対応するテストクラス
（tests/test_gate2_phase3b1_reader_integration.py::TestGetQuarterlySeries /
TestGetLatestQuarterly）とフィルタ条件・ソート順の挙動が同一であることを
確認する（第一引数の形だけがnormalized dictからLayer3 store dictに
変わる）。

この段階では新規追加のみで、既存normalized/経由の消費者（5系統）は
一切変更しない。
"""

import os
import sys

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data.layer3_builder import (  # noqa: E402
    get_field_entries,
    get_quarterly_series,
    get_latest_quarterly,
    WEIGHTED_AVG_DILUTED_SHARES_TAG,
)


def _make_store(field_name: str, entries: list) -> dict:
    """build_ticker_store()の戻り値と同じshapeの合成storeを作る。"""
    return {
        "ticker": "TEST",
        "fields": {
            field_name: {
                "source_tag": "TestConcept",
                "category": "flow",
                "entries": entries,
            }
        },
    }


class TestGetFieldEntries:
    def test_returns_entries_list(self):
        store = _make_store("revenue", [{"end": "2024-03-31", "val": 100}])
        assert get_field_entries(store, "revenue") == [{"end": "2024-03-31", "val": 100}]

    def test_missing_field_returns_empty_list(self):
        assert get_field_entries({"fields": {}}, "revenue") == []
        assert get_field_entries({}, "revenue") == []


class TestGetQuarterlySeries:
    def test_excludes_annual_and_ytd(self):
        store = _make_store("revenue", [
            {"end": "2024-03-31", "val": 100, "is_annual": False, "is_ytd": False},
            {"end": "2024-12-31", "val": 400, "is_annual": True,  "is_ytd": False},
            {"end": "2024-06-30", "val": 999, "is_annual": False, "is_ytd": True},
            {"end": "2024-06-30", "val": 110, "is_annual": False, "is_ytd": False},
        ])
        result = get_quarterly_series(store, "revenue")
        assert [e["val"] for e in result] == [100, 110]

    def test_sorts_by_end_ascending(self):
        store = _make_store("revenue", [
            {"end": "2024-09-30", "val": 3, "is_annual": False, "is_ytd": False},
            {"end": "2024-03-31", "val": 1, "is_annual": False, "is_ytd": False},
            {"end": "2024-06-30", "val": 2, "is_annual": False, "is_ytd": False},
        ])
        result = get_quarterly_series(store, "revenue")
        assert [e["end"] for e in result] == ["2024-03-31", "2024-06-30", "2024-09-30"]

    def test_missing_field_returns_empty_list(self):
        assert get_quarterly_series({"fields": {}}, "revenue") == []
        assert get_quarterly_series({}, "revenue") == []

    def test_source_tag_field_ignored_by_filter(self):
        """Layer3 entriesにのみ存在するsource_tag等の追加キーがあっても
        フィルタ・ソートに影響しないことを確認（normalized/にはない
        キーだが、entries自体の互換性を壊さないことの回帰確認）。"""
        store = _make_store("revenue", [
            {"end": "2024-03-31", "val": 100, "is_annual": False, "is_ytd": False,
             "source_tag": "RevenueFromContractWithCustomerExcludingAssessedTax"},
        ])
        result = get_quarterly_series(store, "revenue")
        assert [e["val"] for e in result] == [100]


class TestGetLatestQuarterly:
    def test_returns_last_entry_by_end(self):
        store = _make_store("net_income", [
            {"end": "2024-03-31", "val": 1, "is_annual": False, "is_ytd": False},
            {"end": "2024-09-30", "val": 3, "is_annual": False, "is_ytd": False},
            {"end": "2024-06-30", "val": 2, "is_annual": False, "is_ytd": False},
        ])
        latest = get_latest_quarterly(store, "net_income")
        assert latest["val"] == 3
        assert latest["end"] == "2024-09-30"

    def test_empty_series_returns_none(self):
        assert get_latest_quarterly({"fields": {}}, "net_income") is None
        assert get_latest_quarterly({}, "net_income") is None


class TestSourceTagFilter:
    """[[TAIL-SHARESDILUTED-Q4-TIMING-RISK-1]]回帰テスト:
    get_quarterly_series()/get_latest_quarterly()のsource_tag引数が
    正しく絞り込みを行うこと。shares_dilutedはCommonStockShares
    Outstanding（期末発行済株式数、BS概念）をフォールバックタグとして
    含むため、WeightedAverageNumberOfDilutedSharesOutstanding（PL概念）
    由来のみに絞り込む必要がある（Q4タイミングではWeightedAverage側の
    四半期タグが報告されないことがあり、絞り込みなしだと直近end日の
    エントリとしてCommonStockSharesOutstanding由来の値が誤って
    採用されるリスクがある）。"""

    def test_get_quarterly_series_filters_by_source_tag(self):
        store = _make_store("shares_diluted", [
            {"end": "2024-03-31", "val": 100, "is_annual": False, "is_ytd": False,
             "source_tag": WEIGHTED_AVG_DILUTED_SHARES_TAG},
            {"end": "2024-06-30", "val": 200, "is_annual": False, "is_ytd": False,
             "source_tag": "CommonStockSharesOutstanding"},
            {"end": "2024-09-30", "val": 300, "is_annual": False, "is_ytd": False,
             "source_tag": WEIGHTED_AVG_DILUTED_SHARES_TAG},
        ])
        result = get_quarterly_series(store, "shares_diluted", source_tag=WEIGHTED_AVG_DILUTED_SHARES_TAG)
        assert [e["val"] for e in result] == [100, 300]

    def test_source_tag_none_keeps_default_behavior(self):
        """source_tag未指定（デフォルトNone）は従来通り絞り込みなし
        （他フィールドの既存呼び出し元に影響を与えないことの確認）。"""
        store = _make_store("revenue", [
            {"end": "2024-03-31", "val": 100, "is_annual": False, "is_ytd": False,
             "source_tag": "SomeTag"},
            {"end": "2024-06-30", "val": 200, "is_annual": False, "is_ytd": False,
             "source_tag": "OtherTag"},
        ])
        result = get_quarterly_series(store, "revenue")
        assert [e["val"] for e in result] == [100, 200]

    def test_get_latest_quarterly_q4_timing_excludes_bs_concept_fallback(self):
        """[[TAIL-SHARESDILUTED-Q4-TIMING-RISK-1]]の核心シナリオ:
        直近end日のエントリがCommonStockSharesOutstanding（BS概念）
        由来で、その1つ前のエントリがWeightedAverage（PL概念）由来の
        場合、source_tag未指定だと誤って直近end日（BS概念）を返すが、
        source_tag指定時は正しく1つ前（PL概念）を返す。"""
        store = _make_store("shares_diluted", [
            {"end": "2024-03-31", "val": 1_000_000, "is_annual": False, "is_ytd": False,
             "source_tag": WEIGHTED_AVG_DILUTED_SHARES_TAG},
            {"end": "2024-06-30", "val": 1_010_000, "is_annual": False, "is_ytd": False,
             "source_tag": WEIGHTED_AVG_DILUTED_SHARES_TAG},
            # Q4タイミング: WeightedAverage側の四半期タグが報告されず、
            # 期末発行済株式数（BS概念）がフォールバックとして混入
            {"end": "2024-09-30", "val": 5_000_000, "is_annual": False, "is_ytd": False,
             "source_tag": "CommonStockSharesOutstanding"},
        ])

        # 修正前の挙動（source_tag未指定）: BS概念の値を誤って採用してしまう
        unfiltered = get_latest_quarterly(store, "shares_diluted")
        assert unfiltered["val"] == 5_000_000
        assert unfiltered["source_tag"] == "CommonStockSharesOutstanding"

        # 修正後の挙動（source_tag指定）: PL概念のみに絞り込み正しい値を返す
        filtered = get_latest_quarterly(store, "shares_diluted", source_tag=WEIGHTED_AVG_DILUTED_SHARES_TAG)
        assert filtered["val"] == 1_010_000
        assert filtered["end"] == "2024-06-30"
        assert filtered["source_tag"] == WEIGHTED_AVG_DILUTED_SHARES_TAG

    def test_get_latest_quarterly_source_tag_none_keeps_default_behavior(self):
        """source_tag未指定（デフォルトNone）は従来通りfield_nameのみで
        絞り込む（他フィールドの既存呼び出し元に影響を与えないことの
        確認）。"""
        store = _make_store("net_income", [
            {"end": "2024-03-31", "val": 1, "is_annual": False, "is_ytd": False},
            {"end": "2024-06-30", "val": 2, "is_annual": False, "is_ytd": False},
        ])
        latest = get_latest_quarterly(store, "net_income")
        assert latest["val"] == 2
