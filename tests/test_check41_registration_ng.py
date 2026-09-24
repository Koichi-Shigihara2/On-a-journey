"""
tests/test_check41_registration_ng.py

[[REGISTRATION-VALIDATOR-P2A-PERIOD-MISMATCH-1]]追補（指示書2026-09-24⑩）の回帰テスト。
新規銘柄登録モード（report_consistency_check.py --include-provisioning）でのみ、
CHECK-41 revenueのyfinance乖離がREGISTRATION_REVENUE_NG_THRESHOLD（30%）を超えたら
NGにする。日次・週次のCI（登録モードでない）では従来どおりWARNのみ。
- 単位誤り型（SECがyfinanceの1000倍）: 登録モードでNG
- ONDS型（同一期間でSEC=yfinance）: NGにならない
- 登録モードでない: 1000倍でもWARNのまま
- yfinance取得不可: NGにもWARNにもしない（登録フロー側が未実行を明示）

実行方法:
    python -m pytest tests/test_check41_registration_ng.py -v
"""

import json
import types

import pytest

from common.sec_data import report_consistency_check as rcc

_TICKER = "ZZ41"
_ONDS_REV = 50_731_000  # ONDS FY2025（SEC=yfinance、乖離+0.0%の実測値）


@pytest.fixture
def run41(tmp_path, monkeypatch):
    def _run(sec_revenue, yf_revenue, registration_mode):
        d = tmp_path / _TICKER
        d.mkdir(exist_ok=True)
        (d / "annual_2025.json").write_text(json.dumps(
            {"ticker": _TICKER, "period": 2025, "pl": {"revenue": sec_revenue}}), encoding="utf-8")
        monkeypatch.setattr(rcc, "SEC_DATA_DIR", str(tmp_path))
        monkeypatch.setattr(rcc, "_get_annual_period_end", lambda t, ann: "2025-12-31")
        monkeypatch.setattr(rcc, "_get_yf_financial_value",
                            lambda t, end, label: yf_revenue if label == "Total Revenue" else None)
        ng = []
        warn = rcc._check_revenue_net_income_reconciliation(
            _TICKER, include_yfinance=True, registration_mode=registration_mode, ng_out=ng)
        return ng, warn
    return _run


def test_unit_error_is_ng_in_registration_mode(run41):
    ng, warn = run41(_ONDS_REV * 1000, _ONDS_REV, registration_mode=True)
    assert len(ng) == 1 and "NG-41" in ng[0]
    assert any("WARN-41 revenue" in w for w in warn)  # WARN行は従来どおり残る


def test_onds_type_same_period_match_is_not_ng(run41):
    ng, warn = run41(_ONDS_REV, _ONDS_REV, registration_mode=True)
    assert ng == []


@pytest.mark.parametrize("sec,yf,expect_ng", [
    (130, 100, False),   # +30.0%ちょうど: 閾値以下はNGにしない
    (131, 100, True),    # +31%
    (69, 100, True),     # -31%（1/10等の縮小側）
    (115.6, 100, False),  # 既存最大（MO 15.6%）はNGにしない
])
def test_threshold_boundary(run41, sec, yf, expect_ng):
    ng, _ = run41(sec * 10**6, yf * 10**6, registration_mode=True)
    assert bool(ng) is expect_ng


def test_non_registration_mode_stays_warn(run41):
    ng, warn = run41(_ONDS_REV * 1000, _ONDS_REV, registration_mode=False)
    assert ng == []
    assert any("WARN-41 revenue" in w for w in warn)


def test_yfinance_unavailable_neither_ng_nor_warn(run41):
    ng, warn = run41(_ONDS_REV * 1000, None, registration_mode=True)
    assert ng == [] and warn == []


@pytest.mark.parametrize("include_provisioning", [True, False])
def test_run_checks_passes_registration_mode(tmp_path, monkeypatch, include_provisioning):
    (tmp_path / _TICKER).mkdir()
    (tmp_path / _TICKER / "report.txt").write_text("x", encoding="utf-8")
    monkeypatch.setattr(rcc, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(rcc._tickers_mod, "get_tanuki_tickers", lambda *a, **k: [_TICKER])
    monkeypatch.setattr(rcc._tickers_mod, "get_registrable_tickers", lambda *a, **k: [_TICKER])
    seen = {}
    monkeypatch.setattr(rcc, "check_ticker",
                        lambda t, *a, registration_mode=False, **k: (seen.setdefault("rm", registration_mode), ([], []))[1])
    rcc.run_checks(types.SimpleNamespace(ticker=_TICKER, include_provisioning=include_provisioning,
                                         include_yfinance_checks=True, quiet=True, fail_on_ng=True))
    assert seen["rm"] is include_provisioning
