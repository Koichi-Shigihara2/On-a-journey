"""
tests/test_stonks_silo_financial_as_of.py

[[STONKS-DATA-ASOF-MISSING-1]]（2026-10-10）の回帰テスト。STONKS SILOのresults.jsonに
財務データの基準日（financial_as_of: 最新四半期末・スコアの基準年度とその期末日・stale）を
表示用に出す。verdict・score・overallには使わない。

実行方法:
    python -m pytest tests/test_stonks_silo_financial_as_of.py -v
"""
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_STONKS_SRC = os.path.join(_REPO_ROOT, "discover", "stonks-silo", "src")
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
if _STONKS_SRC not in sys.path:
    sys.path.insert(0, _STONKS_SRC)

_spec = importlib.util.spec_from_file_location(
    "stonks_silo_financial_as_of", os.path.join(_STONKS_SRC, "financial_as_of.py"))
fa = importlib.util.module_from_spec(_spec)
sys.modules["stonks_silo_financial_as_of"] = fa
_spec.loader.exec_module(fa)

# pipeline.pyは同名の"pipeline"モジュール（tanuki_valuation）と衝突しないよう一意な名前で読む
_pspec = importlib.util.spec_from_file_location(
    "stonks_silo_pipeline_asof", os.path.join(_STONKS_SRC, "pipeline.py"))
sp = importlib.util.module_from_spec(_pspec)
sys.modules["stonks_silo_pipeline_asof"] = sp
_pspec.loader.exec_module(sp)

GEN = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


# ── stale の判定 ─────────────────────────────────────────────────────
def test_stale_boundary_is_135_days():
    assert fa.STALE_DAYS_AFTER_QUARTER_END == 135
    assert fa.is_stale("2026-05-28", GEN) is False   # 135日
    assert fa.is_stale("2026-05-27", GEN) is True    # 136日
    assert fa.is_stale("2026-07-31", GEN) is False


def test_stale_unknown_when_no_quarter_end():
    assert fa.is_stale(None, GEN) is None
    assert fa.is_stale("not-a-date", GEN) is None


# ── スコアの基準年度の期末日（10-KのreportDate） ─────────────────────
def _write(tmp_path, ticker, name, doc):
    d = tmp_path / ticker
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(json.dumps(doc), encoding="utf-8")


def test_scoring_fy_end_from_own_10k_accn(tmp_path):
    _write(tmp_path, "GTLB", "annual_2026.json", {
        "pl_provenance": {"revenue": {"accn": "A-10K", "is_own_data": True}},
        "cf_provenance": {"operating_cash_flow": {"accn": "A-10K", "is_own_data": True},
                          "capex": {"accn": "A-OLD", "is_own_data": False}},  # 他社・比較列は使わない
    })
    _write(tmp_path, "GTLB", "submissions.json",
           {"accn_to_reportdate": {"A-10K": "2026-01-31", "A-OLD": "2025-01-31"}})
    assert fa.scoring_fy_end("GTLB", 2026, data_dir=tmp_path) == "2026-01-31"


def test_scoring_fy_end_none_when_missing(tmp_path):
    assert fa.scoring_fy_end("NONE", 2025, data_dir=tmp_path) is None
    assert fa.scoring_fy_end("NONE", None, data_dir=tmp_path) is None
    _write(tmp_path, "XYZ", "annual_2025.json", {"pl_provenance": {"revenue": {"accn": "A-1", "is_own_data": True}}})
    assert fa.scoring_fy_end("XYZ", 2025, data_dir=tmp_path) is None  # submissionsにaccnが無い


# ── 最新四半期末 ─────────────────────────────────────────────────────
def test_latest_quarter_end_from_layer3_ttm(monkeypatch):
    import common.sec_data.ttm_calculator as tc
    monkeypatch.setattr(tc, "calc_ttm_series", lambda t, st, n_periods=1: [{"ttm_end": "2026-07-31", "flow": {}}])
    assert fa.latest_quarter_end("GTLB", {"dummy": 1}) == "2026-07-31"
    assert fa.latest_quarter_end("GTLB", None) is None
    monkeypatch.setattr(tc, "calc_ttm_series", lambda t, st, n_periods=1: [])
    assert fa.latest_quarter_end("GTLB", {"dummy": 1}) is None


def test_build_and_restamp(monkeypatch, tmp_path):
    monkeypatch.setattr(fa, "latest_quarter_end", lambda t, st: "2026-03-31")
    monkeypatch.setattr(fa, "scoring_fy_end", lambda t, fy, data_dir=None: "2025-12-31")
    out = fa.build("SOUN", [2023, 2024, 2025], {"x": 1}, GEN)
    assert out == {"latest_quarter_end": "2026-03-31", "scoring_fy": 2025, "scoring_fy_end": "2025-12-31",
                   "stale": True, "stale_threshold_days": 135}  # 193日
    results = {"SOUN": {"financial_as_of": dict(out, stale=False)},
               "GTLB": {"financial_as_of": {"latest_quarter_end": "2026-07-31"}},
               "OLD": {"overall_score": 50}}  # financial_as_ofの無い古い行は触らない
    assert fa.restamp(results, GEN) == 1
    assert results["SOUN"]["financial_as_of"]["stale"] is True
    assert results["GTLB"]["financial_as_of"]["stale"] is False
    assert "financial_as_of" not in results["OLD"]


# ── pipeline.run() を通した出力 ─────────────────────────────────────
class _FakeAnalysis:
    overall_verdict = "PROMISING"
    overall_score = 70.0


def test_pipeline_run_writes_financial_as_of_without_touching_scores(monkeypatch, tmp_path):
    out_file = tmp_path / "results.json"
    monkeypatch.setattr(sp, "_OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(sp, "_OUTPUT_FILE", out_file)
    monkeypatch.setattr(sp, "stonks_tickers", lambda: ["AAA"])
    monkeypatch.setattr(sp, "load_annual_data", lambda t, years=5: {
        "years": [2024, 2025], "records": {2024: {"pl": {}}, 2025: {"pl": {"revenue": 10}}}})
    monkeypatch.setattr(sp, "fetch_valuation", lambda t: {
        "sector": "", "industry": "", "market_cap": None, "current_price": 1.0, "enterprise_value": None,
        "total_debt": None, "fetched_at": "x", "error": None})

    class _Reader:
        def get_net_cash(self, t, sector=None, industry=""):
            return {"available": False}
    monkeypatch.setattr(sp, "SECReader", _Reader)

    class _Analyzer:
        def analyze(self, data, net_cash_data=None):
            return _FakeAnalysis()
    monkeypatch.setattr(sp, "StonksAnalyzer", _Analyzer)
    monkeypatch.setattr(sp, "_to_dict", lambda obj: {"overall_score": 70.0, "overall_verdict": "PROMISING"}
                        if isinstance(obj, _FakeAnalysis) else obj)
    monkeypatch.setattr(sp, "build_ticker_store", lambda t: {"store": t})
    monkeypatch.setattr(sp, "get_ttm_revenue", lambda t, store=None: None)
    monkeypatch.setattr(sp, "load_all_normalized", lambda ts: {})
    monkeypatch.setattr(sp, "compute_vectors", lambda stores: {})
    monkeypatch.setattr(sp.financial_as_of, "latest_quarter_end", lambda t, st: "2026-06-30" if st else None)
    monkeypatch.setattr(sp.financial_as_of, "scoring_fy_end", lambda t, fy, data_dir=None: "2025-12-31")

    sp.run()
    doc = json.loads(out_file.read_text(encoding="utf-8"))
    r = doc["tickers"]["AAA"]
    assert r["overall_score"] == 70.0 and r["overall_verdict"] == "PROMISING"
    assert r["financial_as_of"]["latest_quarter_end"] == "2026-06-30"
    assert r["financial_as_of"]["scoring_fy"] == 2025
    assert r["financial_as_of"]["scoring_fy_end"] == "2025-12-31"
    gen = datetime.fromisoformat(doc["generated_at"])
    assert r["financial_as_of"]["stale"] is fa.is_stale("2026-06-30", gen)


def test_pipeline_financial_as_of_failure_does_not_break_ticker(monkeypatch, tmp_path):
    out_file = tmp_path / "results.json"
    monkeypatch.setattr(sp, "_OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(sp, "_OUTPUT_FILE", out_file)
    monkeypatch.setattr(sp, "stonks_tickers", lambda: ["AAA"])
    monkeypatch.setattr(sp, "load_annual_data", lambda t, years=5: {"years": [2025], "records": {2025: {"pl": {}}}})
    monkeypatch.setattr(sp, "fetch_valuation", lambda t: {
        "sector": "", "industry": "", "market_cap": None, "current_price": None, "enterprise_value": None,
        "total_debt": None, "fetched_at": "x", "error": None})

    class _Reader:
        def get_net_cash(self, t, sector=None, industry=""):
            return {"available": False}
    monkeypatch.setattr(sp, "SECReader", _Reader)

    class _Analyzer:
        def analyze(self, data, net_cash_data=None):
            return _FakeAnalysis()
    monkeypatch.setattr(sp, "StonksAnalyzer", _Analyzer)
    monkeypatch.setattr(sp, "_to_dict", lambda obj: {"overall_score": 70.0} if isinstance(obj, _FakeAnalysis) else obj)
    monkeypatch.setattr(sp, "build_ticker_store", lambda t: (_ for _ in ()).throw(RuntimeError("no store")))
    monkeypatch.setattr(sp, "get_ttm_revenue", lambda t, store=None: None)
    monkeypatch.setattr(sp, "load_all_normalized", lambda ts: {})
    monkeypatch.setattr(sp, "compute_vectors", lambda stores: {})
    monkeypatch.setattr(sp.financial_as_of, "scoring_fy_end",
                        lambda t, fy, data_dir=None: (_ for _ in ()).throw(ValueError("broken")))

    sp.run()
    r = json.loads(out_file.read_text(encoding="utf-8"))["tickers"]["AAA"]
    assert r["overall_score"] == 70.0
    assert r["financial_as_of"]["latest_quarter_end"] is None and r["financial_as_of"]["stale"] is None
