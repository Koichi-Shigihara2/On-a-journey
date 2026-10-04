"""
tests/test_core_calculator_rf_reference.py

2026-10-04: 参考②（Rf理論上限）だけをpipelineから渡したRf現在値（rf_reference）で
計算する。メイン（Rm=10%）・参考①（β込みWACC、固定Rf 0.043）は変えない。

実行方法:
    python -m pytest tests/test_core_calculator_rf_reference.py -v
"""

import os
import sys

_TV_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation"))
if _TV_DIR not in sys.path:
    sys.path.insert(0, _TV_DIR)

from core_calculator import KoichiValuationCalculator  # type: ignore[import]
from test_core_calculator_v0_note import _minimal_financials  # type: ignore[import]


def _calc(**kw):
    return KoichiValuationCalculator().calculate_pt(_minimal_financials(), **kw)


def test_rf_reference_changes_only_reference2():
    base = _calc()
    live = _calc(rf_reference=0.0528)
    # 参考②は現在値で計算し、使ったRfを残す
    assert live["intrinsic_value_rf_rate"] == 0.0528
    assert live["intrinsic_value_rf"] < base["intrinsic_value_rf"]
    # メイン・参考①・WACC（Ke）は変わらない
    for key in ("intrinsic_value_per_share", "upside_percent", "intrinsic_value_beta",
                "upside_percent_beta", "v0", "wacc", "scenario_valuations", "sensitivity", "rice"):
        assert live[key] == base[key], key
    assert live["dcf_components"]["v0_rm"] == base["dcf_components"]["v0_rm"]
    assert live["wacc"]["risk_free_rate"] == 0.043


def test_without_rf_reference_uses_fixed_wacc_rf():
    base = _calc()
    assert base["intrinsic_value_rf_rate"] == 0.043
