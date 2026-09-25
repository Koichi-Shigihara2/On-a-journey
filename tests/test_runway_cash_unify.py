"""
tests/test_runway_cash_unify.py

[[BBAI-RDW-RUNWAY-VERIFICATION-1]]（2026-09-25）の回帰テスト。
Runway算出用cashをcommon/sec_data/reader.py::get_runway_cash()へ集約し、
STONKS SILO（discover/stonks-silo/src/analyzer.py::_analyze_runway()）が
直近年次annual_*.jsonのみでなくSECReader.get_net_cash()の四半期優先値を
使うことを検証する。TANUKI VALUATION側（computed_runway_months）の
回帰テストはtests/test_pipeline_logic.py::
TestComputedRunwayIncludesShortTermInvestments。

一次情報（10-Q、2026-06-30時点）:
- RDW: cash $557.0M（H1増資$566.2M）、H1 FCF -$48.0M → 実態SAFE
  （年次FY2025 cash $94.5Mのみで算出するとDANGER）
- BBAI: cash $36.278M + 流動AFS $282.913M → 実態SAFE

実行方法:
    python -m pytest tests/test_runway_cash_unify.py -v
"""

import importlib.util
import os
import sys

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_STONKS_SRC = os.path.join(_REPO_ROOT, "discover", "stonks-silo", "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data.reader import get_runway_cash  # noqa: E402

# test_pipeline_logic.py等と同名モジュール衝突を避けるため一意な名前でロード
_spec = importlib.util.spec_from_file_location(
    "stonks_silo_analyzer_runway", os.path.join(_STONKS_SRC, "analyzer.py")
)
_analyzer = importlib.util.module_from_spec(_spec)
sys.modules["stonks_silo_analyzer_runway"] = _analyzer
_spec.loader.exec_module(_analyzer)


def _net_cash(cash, sti, period="2026Q2", cash_missing=False):
    """SECReader.get_net_cash()の返却値のうちRunwayに関係するキーのみ"""
    return {
        "cash": cash, "short_term_investments": sti,
        "long_term_debt": 0.0, "short_term_debt": 0.0,
        "net_cash": cash + sti, "available": not cash_missing,
        "net_debt_period": period, "cash_missing": cash_missing,
    }


def _records(cash, sti, ocf, capex):
    return {2025: {
        "pl": {}, "cf": {"operating_cash_flow": ocf, "capital_expenditure": capex},
        "bs": {"cash_and_equivalents": cash, "short_term_investments": sti},
    }}


class TestGetRunwayCash:
    def test_bbai_type_adds_short_term_investments(self):
        assert get_runway_cash(_net_cash(36_278_000.0, 282_913_000.0)) == pytest.approx(319_191_000.0)

    def test_cash_missing_returns_none(self):
        assert get_runway_cash(_net_cash(0.0, 0.0, cash_missing=True)) is None

    def test_none_or_empty_returns_none(self):
        assert get_runway_cash(None) is None
        assert get_runway_cash({}) is None


class TestStonksRunwayQuarterlyPriority:
    # RDW FY2025: OCF-|CapEx| = 年約-$190.8M（月次バーン約$15.9M）
    _OCF = -150_000_000
    _CAPEX = -40_810_000

    def test_rdw_type_post_annual_raise_reflected(self):
        """年次決算後の増資（四半期cash $557.0M）でDANGER→SAFEになる"""
        a = _analyzer.StonksAnalyzer()
        ra = a._analyze_runway(
            [2025], _records(94_467_000, None, self._OCF, self._CAPEX),
            _net_cash(556_967_000.0, 0.0),
        )
        assert ra.cash == pytest.approx(556_967_000.0)
        assert ra.verdict == "SAFE", ra.verdict_reason

    def test_bbai_type_uses_quarterly_cash_plus_sti(self):
        a = _analyzer.StonksAnalyzer()
        ra = a._analyze_runway(
            [2025], _records(87_126_000, 200_461_000, -40_000_000, -2_476_000),
            _net_cash(36_278_000.0, 282_913_000.0),
        )
        assert ra.cash == pytest.approx(319_191_000.0)
        assert ra.verdict == "SAFE"

    def test_falls_back_to_annual_without_net_cash_data(self):
        """net_cash_data未指定（単体呼び出し）時は従来通り直近年次BSを使う"""
        a = _analyzer.StonksAnalyzer()
        ra = a._analyze_runway([2025], _records(94_467_000, None, self._OCF, self._CAPEX))
        assert ra.cash == pytest.approx(94_467_000)
        assert ra.verdict == "DANGER"

    def test_analyze_passes_net_cash_data_through(self):
        """analyze()のnet_cash_data引数が_analyze_runway()まで届く"""
        a = _analyzer.StonksAnalyzer()
        data = {
            "ticker": "RDWTEST", "years": [2025],
            "records": _records(94_467_000, None, self._OCF, self._CAPEX),
        }
        data["records"][2025]["pl"] = {"revenue": 300_000_000, "revenue_sanitized": 300_000_000,
                                       "net_income": -200_000_000}
        result = a.analyze(data, net_cash_data=_net_cash(556_967_000.0, 0.0))
        assert result.runway.verdict == "SAFE"
