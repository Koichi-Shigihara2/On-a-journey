"""
tests/test_dcf_phase1_taper.py

[[DCF-1b]]（2026-09-25）: 3段階DCF（calculator/dcf.py::calculate_three_stage_dcf）
のPhase1を「Phase1初年度g → Phase2のg」へ線形逓減させる変更の回帰テスト。
[[DCF-1]]のcalculate_tapering_dcf()と同一の補間式（_linear_taper_rate）を使う。

背景: NVDA（g=50%×Phase1 9年、Phase1終了時FCFが基準の38.4倍、IV/株÷株価
9.5倍）・APP（11.7倍）で、Phase1固定成長の複利によりIVが桁違いに膨らんで
いた。逓減後はNVDAのPhase1終了時FCF倍率が12.2倍になる（2026-09-25試算）。

実行方法:
    python -m pytest tests/test_dcf_phase1_taper.py -v
"""

import os
import sys

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_TANUKI_DIR = os.path.join(_REPO_ROOT, "src", "value", "tanuki_valuation")
if _TANUKI_DIR not in sys.path:
    sys.path.insert(0, _TANUKI_DIR)

from calculator.dcf import (  # noqa: E402
    calculate_tapering_dcf,
    calculate_three_stage_dcf,
)

# NVDA 2026-09-25時点の実値（base FCF・g1・g2・Phase1/2年数・永久成長率・Rm）
_NVDA = dict(base_fcf=108_368_833_333.33, phase1_growth_rate=0.50, phase2_growth_rate=0.15,
             wacc=0.10, phase1_years=9, phase2_years=5, terminal_growth=0.035)


class TestThreeStagePhase1Taper:
    def test_nvda_type_growth_path_and_phase1_end_multiple(self):
        r = calculate_three_stage_dcf(**_NVDA)
        path = r.phase1_growth_path
        assert path[0] == pytest.approx(0.50)
        assert path[-1] == pytest.approx(0.15)
        assert path[1] == pytest.approx(0.50 - 0.35 / 8)
        assert [d["growth_rate"] for d in r.phase1_detail] == path
        multiple = r.phase1_detail[-1]["fcf"] / _NVDA["base_fcf"]
        assert multiple == pytest.approx(12.18, abs=0.01)  # 固定50%×9年なら38.4倍
        assert r.phase1_detail[-1]["fcf"] < _NVDA["base_fcf"] * 1.5 ** 9

    def test_phase1_matches_calculate_tapering_dcf(self):
        """Phase1部分はDCF-1のcalculate_tapering_dcf()と同一の年次FCF・PV"""
        r = calculate_three_stage_dcf(**_NVDA)
        tap = calculate_tapering_dcf(base_fcf=_NVDA["base_fcf"], g_start=0.50, g_end=0.15,
                                     wacc=0.10, high_growth_years=9, terminal_growth=0.035)
        for a, b in zip(r.phase1_detail, tap.high_growth_detail):
            assert a["fcf"] == pytest.approx(b["fcf"])
            assert a["pv"] == pytest.approx(b["pv"])

    def test_phase2_continues_from_tapered_phase1_end(self):
        r = calculate_three_stage_dcf(**_NVDA)
        assert r.phase2_detail[0]["fcf"] == pytest.approx(r.phase1_detail[-1]["fcf"] * 1.15)
        assert r.v0 == pytest.approx(r.pv_phase1 + r.pv_phase2 + r.pv_terminal)

    def test_provenance_in_to_dict(self):
        d = calculate_three_stage_dcf(**_NVDA).to_dict()
        assert d["phase1_tapered"] is True
        assert len(d["phase1_growth_path"]) == 9
        assert d["dcf_type"] == "three_stage"

    def test_single_year_phase1_is_not_tapered(self):
        r = calculate_three_stage_dcf(**dict(_NVDA, phase1_years=1))
        assert r.phase1_growth_path == [0.50]
        assert r.phase1_tapered is False

    def test_equal_growth_is_flat_and_not_tapered(self):
        r = calculate_three_stage_dcf(**dict(_NVDA, phase1_growth_rate=0.15))
        assert all(g == pytest.approx(0.15) for g in r.phase1_growth_path)
        assert r.phase1_tapered is False


class TestReportPhase1TaperLine:
    def test_report_line_lists_growth_path(self, tmp_path):
        """report.txtにDCF_Phase1_Taper行（初年度→最終年と各年のg）が出る"""
        import test_pipeline_logic as tpl  # _make_pipe等の既存フィクスチャを再利用
        pipe = tpl._make_pipe(tmp_path)
        pipe._eps_summary_cache = {}
        val = tpl._minimal_valuation(upside=30.0)
        dc = calculate_three_stage_dcf(**_NVDA).to_dict()
        val["dcf_type"] = "three_stage"
        val["dcf_components"] = dc
        val["maturity_profile"] = {"type": "three_stage", "phase1": {"years": 5, "growth": None},
                                   "phase2": {"years": 5, "growth": 0.15}, "terminal_growth": 0.035}
        report = pipe._generate_report("TAPERTEST", val, tpl._minimal_score_data(), tpl._minimal_extra())
        line = next((l for l in report.splitlines() if l.startswith("DCF_Phase1_Taper:")), None)
        assert line is not None
        assert "50.0% → 15.0%" in line and "9yr" in line
