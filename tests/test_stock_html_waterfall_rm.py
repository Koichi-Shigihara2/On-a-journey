"""
tests/test_stock_html_waterfall_rm.py

stock.htmlのウォーターフォール図（VALUATION COMPONENTS、renderChart()）が、
メイン理論株価（Rm基準）のPV内訳で棒を積むことの回帰テスト（2026-10-06、
[[TANUKI-BETA-BASIS-FIELDS-UNLABELED-1]]）。

修正前は棒にβ込みWACCのPV（components.pv_high・pv_terminal、
dcf_components.pv_phase1・pv_phase2）を使い、V₀のラベルだけv0_rmだった。
Plotlyのtotalの棒の高さは前の棒の合計になるため、V₀の棒の高さ（β版の合計）と
ラベル（Rm版）が全99銘柄で食い違っていた（差の率の中央値は約21%）。

JSの実行環境が無いため、tests/test_sens_matrix_dual_impl.pyと同じく
ソースのパターンで確認する。実表示はbrowser_checks/check_valuation_chart_basis.pyで確認する。

実行方法:
    python -m pytest tests/test_stock_html_waterfall_rm.py -v
"""

import json
import glob
import os

_REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_STOCK_HTML = os.path.join(_REPO, "docs", "value-monitor", "tanuki_valuation", "stock.html")
_DATA_DIR = os.path.join(_REPO, "docs", "value-monitor", "tanuki_valuation", "data")


def _render_chart_src() -> str:
    with open(_STOCK_HTML, encoding="utf-8") as f:
        content = f.read()
    i = content.index("function renderChart(")
    j = content.index("function renderHistoryChart(", i)
    return content[i:j]


class TestRenderChartUsesRmParts:
    def test_two_stage_and_tapering_use_rm_fields(self):
        src = _render_chart_src()
        assert "dcfC.pv_fcf_rm" in src
        assert "dcfC.pv_tv_rm" in src

    def test_three_stage_uses_rm_fields(self):
        src = _render_chart_src()
        assert "dcfC.pv_phase1_rm" in src
        assert "dcfC.pv_phase2_rm" in src

    def test_beta_fallback_is_annotated(self):
        """*_rmが無いときだけβ版に戻し、その旨を図に注記すること"""
        src = _render_chart_src()
        assert "β込みWACC基準" in src
        assert "valuation-chart-note" in src

    def test_labels_not_clipped(self):
        src = _render_chart_src()
        assert "cliponaxis: false" in src
        assert "automargin: true" in src


def test_rm_parts_sum_to_v0_rm_in_latest():
    """図が使うRm版の内訳の合計が、全銘柄でv0_rm（V₀のラベル）と一致すること。"""
    paths = sorted(glob.glob(os.path.join(_DATA_DIR, "*", "latest.json")))
    assert paths
    for p in paths:
        d = json.load(open(p, encoding="utf-8"))
        dc = d.get("dcf_components") or {}
        if dc.get("v0_rm") is None:
            continue
        if d.get("dcf_type") == "three_stage":
            parts = [dc["pv_phase1_rm"], dc["pv_phase2_rm"], dc["pv_tv_rm"]]
        else:
            parts = [dc["pv_fcf_rm"], dc["pv_tv_rm"]]
        assert abs(sum(parts) - dc["v0_rm"]) <= max(1.0, abs(dc["v0_rm"]) * 1e-9), p
