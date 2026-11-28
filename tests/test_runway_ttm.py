"""資金の持ちの流出を直近4四半期（TTM）にする（[[STONKS-DEFICIT-REALITY-EPIC-1]] ①、method_change ID STONKS-DEFICIT-REALITY-EPIC-1-1）"""
import os
import sys

from common.sec_data import reader

_STONKS = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "value", "stonks_silo"))
if _STONKS not in sys.path:
    sys.path.insert(0, _STONKS)


def _patch(monkeypatch, series, cash_ends):
    from common.sec_data import layer3_builder, ttm_calculator
    monkeypatch.setattr(ttm_calculator, "calc_ttm_series", lambda t, st, n_periods=1: series)
    monkeypatch.setattr(layer3_builder, "get_quarterly_series",
                        lambda st, f, source_tag=None: [{"end": e, "val": 1.0} for e in cash_ends])


def _ttm(ocf, capex, q=4, missing=0, end="2026-09-30"):
    return [{"ttm_end": end, "flow": {"operating_cash_flow": {"val": ocf, "quarters_used": q, "missing": missing},
                                       "capital_expenditure": {"val": capex, "quarters_used": 4, "missing": 0}}}]


def test_ttm_fcf_when_four_quarters_and_cash_end_matches(monkeypatch):
    _patch(monkeypatch, _ttm(-117_872_000, 11_382_000), ["2026-06-30", "2026-09-30"])
    r = reader.get_runway_burn("SOUN", store={})
    assert r["basis"] == reader.RUNWAY_BURN_BASIS_TTM
    assert r["fcf_ttm"] == -129_254_000                     # 営業CF − 設備投資（SOUN 10-Q の TTM）
    assert 19.2 < 207_579_000 / (abs(r["fcf_ttm"]) / 12) < 19.4   # 10-Q の約18〜19か月


def test_fallback_when_ttm_incomplete(monkeypatch):
    _patch(monkeypatch, _ttm(-1.0, 0.0, q=3), ["2026-09-30"])
    r = reader.get_runway_burn("RCAT", store={})
    assert r["basis"] == reader.RUNWAY_BURN_BASIS_ANNUAL and r["fcf_ttm"] is None and r["reason"] == "ocf_ttm_incomplete"
    _patch(monkeypatch, _ttm(-1.0, 0.0, missing=1), ["2026-09-30"])
    assert reader.get_runway_burn("X", store={})["basis"] == reader.RUNWAY_BURN_BASIS_ANNUAL


def test_fallback_when_cash_end_differs_from_ttm_end(monkeypatch):
    _patch(monkeypatch, _ttm(-100.0, 10.0, end="2026-07-31"), ["2023-04-30"])
    r = reader.get_runway_burn("CPRT", store={})
    assert r["basis"] == reader.RUNWAY_BURN_BASIS_ANNUAL and r["reason"].startswith("cash_end_mismatch")


def test_cash_end_unknown_uses_ttm_with_reason(monkeypatch):
    _patch(monkeypatch, _ttm(-100.0, 10.0), [])
    r = reader.get_runway_burn("SITM", store={})
    assert r["basis"] == reader.RUNWAY_BURN_BASIS_TTM and r["reason"] == "cash_end_unverified"


def test_missing_capex_is_zero(monkeypatch):
    s = _ttm(-100.0, 0.0)
    s[0]["flow"]["capital_expenditure"] = {"val": None, "quarters_used": 0, "missing": 4}
    _patch(monkeypatch, s, ["2026-09-30"])
    assert reader.get_runway_burn("X", store={})["fcf_ttm"] == -100.0


def _records():
    return {2025: {"bs": {"cash_and_equivalents": 1.0, "short_term_investments": 0.0},
                   "cf": {"operating_cash_flow": -98_222_000, "capital_expenditure": 4_902_000}}}


def test_stonks_runway_uses_ttm_and_marks_basis(monkeypatch):
    import analyzer as A
    monkeypatch.setattr(reader, "get_runway_burn", lambda t, store=None: {
        "basis": "ttm", "fcf_ttm": -129_254_000, "ttm_end": "2026-09-30", "cash_end": "2026-09-30", "reason": None})
    nc = {"cash": 207_579_000.0, "short_term_investments": 0.0, "cash_missing": False}
    ra = A.StonksAnalyzer()._analyze_runway([2025], _records(), nc, ticker="SOUN", sector="Technology", industry="Software")
    assert ra.burn_basis == "ttm" and ra.burn_ttm_end == "2026-09-30"
    assert 19.2 < ra.runway_months < 19.4 and ra.verdict == "WATCH"           # 年次なら24.2か月（SAFE）


def test_stonks_runway_falls_back_to_annual_and_marks(monkeypatch):
    import analyzer as A
    monkeypatch.setattr(reader, "get_runway_burn", lambda t, store=None: {
        "basis": "annual_fallback", "fcf_ttm": None, "ttm_end": None, "cash_end": None, "reason": "ocf_ttm_incomplete"})
    nc = {"cash": 207_579_000.0, "short_term_investments": 0.0, "cash_missing": False}
    ra = A.StonksAnalyzer()._analyze_runway([2025], _records(), nc, ticker="SOUN", sector="Technology", industry="Software")
    assert ra.burn_basis == "annual_fallback" and ra.burn_reason == "ocf_ttm_incomplete"
    assert abs(ra.runway_months - 24.15) < 0.1                                # 今の年次の計算のまま


def test_financial_is_excluded_from_runway(monkeypatch):
    import analyzer as A
    monkeypatch.setattr(reader, "get_runway_burn", lambda *a, **k: (_ for _ in ()).throw(AssertionError("金融は呼ばない")))
    nc = {"cash": 7_802_000_000.0, "short_term_investments": 0.0, "cash_missing": False}
    ra = A.StonksAnalyzer()._analyze_runway([2025], _records(), nc, ticker="SOFI", sector="Financial Services", industry="Credit Services")
    assert ra.burn_basis == "financial_excluded" and ra.runway_months is None and ra.verdict == "UNKNOWN"
    assert "金融" in ra.verdict_reason


def test_every_runway_burn_reason_code_has_japanese_text():
    """get_runway_burn（と STONKS の analyzer）が出す reason のコードの全種類が、日本語の対応表（RUNWAY_BURN_REASON_JA）にある"""
    import re
    from common.sec_data import reader as rd
    src = open(rd.__file__, encoding="utf-8").read()
    codes = set(re.findall(r'out\["reason"\] = f?"([a-z_]+)', src))
    codes |= {"no_ticker"}      # analyzer.py: 銘柄が特定できないときの reason
    assert codes >= {"ocf_ttm_incomplete", "ttm_unavailable", "cash_end_mismatch", "cash_end_unverified"}
    for c in codes:
        ja = rd.runway_burn_reason_ja(c + (":2026-01-31!=2026-03-31" if c == "cash_end_mismatch" else ""))
        assert ja and ja != c and not re.fullmatch(r"[a-z_]+", ja), c
    assert rd.runway_burn_reason_ja("cash_end_mismatch:2023-04-30!=2026-07-31") == "現金の期末（2023-04-30）がTTMの期末（2026-07-31）とずれる"
    assert rd.runway_burn_reason_ja("ttm_unavailable:KeyError('x')") == "TTMを作れない"
    assert rd.runway_burn_reason_ja(None) == ""
