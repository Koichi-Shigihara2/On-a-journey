"""
tests/test_layer3_formula_yoy.py

[[TAIL-LAYER3-FORMULA-YOY-UNSUPPORTED-1]]回帰テスト。
`xbrl_segment_fetcher.py::fetch_layer3_kpis()`の`layer3_formula`に
追加した`"yoy(field_name)"`構文（同一フィールドの前年同期比）を検証する。
既存の`"field_a/field_b"`除算構文が影響を受けないことも確認する。

実行方法:
    python -m pytest tests/test_layer3_formula_yoy.py -v
"""

import os
import sys

_TAIL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "tail"))
if _TAIL_DIR not in sys.path:
    sys.path.insert(0, _TAIL_DIR)

import xbrl_segment_fetcher as xsf  # noqa: E402


def _entry(end, val, filed=None):
    return {"end": end, "val": val, "filed": filed or (end + "T00:00:00")}


class TestYoyFormula:
    def test_yoy_normal_case(self, monkeypatch):
        """8四半期分のうち、直近4件（前年同期が存在する分）でYoYが
        (今期値-前年同期値)/|前年同期値| として正しく計算される"""
        series = [
            _entry("2024-03-31", 1.00),
            _entry("2024-06-30", 1.10),
            _entry("2024-09-30", 1.20),
            _entry("2024-12-31", 1.30),
            _entry("2025-03-31", 1.50),  # vs 1.00 -> +0.5
            _entry("2025-06-30", 1.32),  # vs 1.10 -> +0.2
            _entry("2025-09-30", 0.60),  # vs 1.20 -> -0.5
            _entry("2025-12-31", 1.17),  # vs 1.30 -> -0.1
        ]
        monkeypatch.setattr(xsf, "build_ticker_store", lambda ticker: {"dummy": True})
        monkeypatch.setattr(xsf, "get_quarterly_series", lambda store, field: series)

        result = xsf.fetch_layer3_kpis("TESTCO", [
            {"kpi_name": "希薄化後EPS成長率", "layer3_formula": "yoy(eps_diluted)"},
        ])
        entries = result["希薄化後EPS成長率"]
        assert len(entries) == 4  # 最初の4四半期は前年同期がないためスキップ
        assert entries[0]["quarter"] == "2025Q1"
        assert entries[0]["value"] == 0.5
        assert entries[1]["value"] == 0.2
        assert entries[2]["value"] == -0.5
        assert entries[3]["value"] == round((1.17 - 1.30) / abs(1.30), 6)

    def test_yoy_skips_when_prior_year_value_is_none(self, monkeypatch):
        series = [
            _entry("2024-03-31", None),
            _entry("2024-06-30", 1.10),
            _entry("2024-09-30", 1.20),
            _entry("2024-12-31", 1.30),
            _entry("2025-03-31", 1.50),  # 前年同期がNone -> スキップ
            _entry("2025-06-30", 1.32),  # vs 1.10 -> 有効
        ]
        monkeypatch.setattr(xsf, "build_ticker_store", lambda ticker: {"dummy": True})
        monkeypatch.setattr(xsf, "get_quarterly_series", lambda store, field: series)

        result = xsf.fetch_layer3_kpis("TESTCO", [
            {"kpi_name": "test_kpi", "layer3_formula": "yoy(x)"},
        ])
        entries = result["test_kpi"]
        assert len(entries) == 1
        assert entries[0]["quarter"] == "2025Q2"

    def test_yoy_skips_when_prior_year_value_is_zero(self, monkeypatch):
        series = [
            _entry("2024-03-31", 0),
            _entry("2024-06-30", 1.10),
            _entry("2024-09-30", 1.20),
            _entry("2024-12-31", 1.30),
            _entry("2025-03-31", 1.50),  # 前年同期が0 -> falsy-zero回避でスキップ
        ]
        monkeypatch.setattr(xsf, "build_ticker_store", lambda ticker: {"dummy": True})
        monkeypatch.setattr(xsf, "get_quarterly_series", lambda store, field: series)

        result = xsf.fetch_layer3_kpis("TESTCO", [
            {"kpi_name": "test_kpi", "layer3_formula": "yoy(x)"},
        ])
        assert result["test_kpi"] == []

    def test_yoy_uses_calendar_matching_not_naive_index_when_series_has_gap(self, monkeypatch):
        """PLTRの実データ検証で発見した実例の回帰テスト: eps_dilutedの
        時系列に1四半期分の欠測（2021-12-31相当）があると、単純な
        「配列で4件前」方式では欠測以降の全四半期で前年同期が1四半期分
        ズレる。end日ベースのカレンダー一致（350〜380日前）ならズレない
        ことを検証する。"""
        series = [
            _entry("2021-09-30", -0.05),
            # 2021-12-31 が欠測（実際のPLTR eps_dilutedで確認された欠測パターン）
            _entry("2022-03-31", -0.05),
            _entry("2022-06-30", -0.09),
            _entry("2022-09-30", -0.06),
            _entry("2023-03-31", 0.01),
            _entry("2023-06-30", 0.01),
            _entry("2023-09-30", 0.03),
            _entry("2024-03-31", 0.04),
            _entry("2024-06-30", 0.06),
            _entry("2024-09-30", 0.06),
            _entry("2025-03-31", 0.08),
            _entry("2025-06-30", 0.13),
        ]
        monkeypatch.setattr(xsf, "build_ticker_store", lambda ticker: {"dummy": True})
        monkeypatch.setattr(xsf, "get_quarterly_series", lambda store, field: series)

        result = xsf.fetch_layer3_kpis("PLTR", [
            {"kpi_name": "eps_growth", "layer3_formula": "yoy(eps_diluted)"},
        ])
        entries_by_q = {e["quarter"]: e["value"] for e in result["eps_growth"]}
        # 2025Q2(2025-06-30, val=0.13)の前年同期は2024Q2(2024-06-30, val=0.06)
        # であるべき（配列で4件前は2024-03-31=0.04になり誤り）
        expected = round((0.13 - 0.06) / abs(0.06), 6)
        assert entries_by_q["2025Q2"] == expected

    def test_yoy_insufficient_history_returns_empty(self, monkeypatch):
        """4四半期未満のデータしかない場合は前年同期を計算できずentries空"""
        series = [_entry("2025-03-31", 1.0), _entry("2025-06-30", 1.1)]
        monkeypatch.setattr(xsf, "build_ticker_store", lambda ticker: {"dummy": True})
        monkeypatch.setattr(xsf, "get_quarterly_series", lambda store, field: series)

        result = xsf.fetch_layer3_kpis("TESTCO", [
            {"kpi_name": "test_kpi", "layer3_formula": "yoy(x)"},
        ])
        assert result["test_kpi"] == []


class TestDivisionFormulaUnaffected:
    def test_existing_division_formula_still_works(self, monkeypatch):
        """既存の"field_a/field_b"除算構文がyoy()追加後も影響を受けない"""
        num_series = [_entry("2025-03-31", 10.0), _entry("2025-06-30", 20.0)]
        den_series = [_entry("2025-03-31", 100.0), _entry("2025-06-30", 0.0)]

        def fake_get_quarterly_series(store, field):
            return num_series if field == "num_field" else den_series

        monkeypatch.setattr(xsf, "build_ticker_store", lambda ticker: {"dummy": True})
        monkeypatch.setattr(xsf, "get_quarterly_series", fake_get_quarterly_series)

        result = xsf.fetch_layer3_kpis("TESTCO", [
            {"kpi_name": "margin", "layer3_formula": "num_field/den_field"},
        ])
        entries = result["margin"]
        # 分母0の四半期(2025-06-30)はスキップされ、1件のみ残る
        assert len(entries) == 1
        assert entries[0]["quarter"] == "2025Q1"
        assert entries[0]["value"] == 0.1
