"""
tests/test_fcf_component_lists.py

[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]ボトムアップFCF移行の一環。

data_fetcher.py::build_fcf_component_lists()の回帰テスト。SECReader.
get_annual_range()が返す年次データからCapEx/SBC/OCF/D&Aを個別系列として
取り出す際、get_fcf_list_with_dates()と同一のフィルタ条件
（free_cash_flowが存在する年のみ採用）を適用することを検証する。

実データ（AAPL）でfcf_list_raw（TTM系列に置き換わり得る）とcapex_list等
（常に年次ベース）の長さが食い違うことを発見した経緯があり、
build_fcf_component_lists()単体を切り出してテスト容易性を確保した。

data_fetcher.pyはsrc.value.tanuki_valuationパッケージ経由（__init__.pyの
wacc未解決import）ではimportできないため、tests/test_data_fetcher_
market_data_switch.pyと同じパターンでモジュール単体を読む。

実行方法:
    python -m pytest tests/test_fcf_component_lists.py -v
"""

import os
import sys

_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

import data_fetcher as df  # noqa: E402


def _year(period, capex=None, sbc=None, ocf=None, da=None, fcf=0):
    cf = {"free_cash_flow": fcf}
    if capex is not None:
        cf["capital_expenditure"] = capex
    if sbc is not None:
        cf["stock_based_compensation"] = sbc
    if ocf is not None:
        cf["operating_cash_flow"] = ocf
    if da is not None:
        cf["depreciation_and_amortization"] = da
    return {"period": period, "cf": cf}


class TestBuildFcfComponentLists:
    def test_extracts_all_four_series_aligned_with_input_order(self):
        annual_data = [
            _year(2025, capex=100, sbc=50, ocf=500, da=30, fcf=400),
            _year(2024, capex=90, sbc=45, ocf=450, da=28, fcf=360),
        ]
        capex, sbc, ocf, da = df.build_fcf_component_lists(annual_data)
        assert capex == [100, 90]
        assert sbc == [50, 45]
        assert ocf == [500, 450]
        assert da == [30, 28]

    def test_years_without_free_cash_flow_are_excluded(self):
        """get_fcf_list_with_dates()と同じフィルタ（free_cash_flowが
        Noneの年は除外）を適用することを回帰テストする"""
        annual_data = [
            _year(2025, capex=100, sbc=50, ocf=500, da=30, fcf=400),
            {"period": 2024, "cf": {}},  # free_cash_flow自体が存在しない年
            _year(2023, capex=80, sbc=40, ocf=400, da=25, fcf=320),
        ]
        capex, sbc, ocf, da = df.build_fcf_component_lists(annual_data)
        assert capex == [100, 80]  # 2024年はスキップされる
        assert len(capex) == len(sbc) == len(ocf) == len(da) == 2

    def test_missing_component_tag_yields_none_not_zero(self):
        """LYFTのcapital_expenditure欠損年（standard candidate 4タグ未申告）
        のようなケース: タグ自体が存在しない年はNone（0とは区別する）"""
        annual_data = [
            {"period": 2025, "cf": {"free_cash_flow": 1_000, "operating_cash_flow": 1_000}},
        ]
        capex, sbc, ocf, da = df.build_fcf_component_lists(annual_data)
        assert capex == [None]
        assert sbc == [None]
        assert ocf == [1_000]

    def test_empty_annual_data_returns_empty_lists(self):
        capex, sbc, ocf, da = df.build_fcf_component_lists([])
        assert (capex, sbc, ocf, da) == ([], [], [], [])

    def test_ttm_substituted_fcf_list_does_not_affect_component_list_length(self):
        """実データ（AAPL）で発見した回帰: fcf_list_rawがTTM系列（4点）に
        置き換わっても、build_fcf_component_lists()は渡された年次データ
        （5点）をそのまま処理し、TTM置換の影響を受けない設計であることを
        固定データで確認する"""
        annual_data = [_year(2025 - i, capex=i * 10, ocf=i * 100, fcf=i * 100 - i * 10) for i in range(5)]
        capex, sbc, ocf, da = df.build_fcf_component_lists(annual_data)
        assert len(capex) == 5  # fcf_list_rawが後段でTTM置換されても本関数の結果は不変
