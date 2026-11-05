"""収益の段階・黒字化の見込み・構造的赤字・赤字の質の点数（common/profitability.py）。[[HYPECORE-STAGE-TIMESCALE-1]] 段B-2（2026-11-05）

テストは作った値（Layer3 の形の store）と一時フォルダだけを使う。本物のファイル・外部の取得に触れない。
"""
import csv
import json
import os
import sys
from datetime import date, timedelta

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "src", "value", "stonks_silo"))
from common import profitability as pf


def _qends(n, last="2026-06-30"):
    """last から古い順に n 個の四半期末（91日おき）"""
    d = date.fromisoformat(last)
    return sorted((d - timedelta(days=91 * k)).isoformat() for k in range(n))


def _entries(values, ends, filed_lag=40, first=None):
    out = []
    for i, (e, v) in enumerate(zip(ends, values)):
        ff = (date.fromisoformat(e) + timedelta(days=filed_lag)).isoformat()
        out.append({"end": e, "val": v, "first_val": (first[i] if first else v), "first_filed": ff, "filed": ff,
                    "period_days": 91, "is_annual": False, "is_ytd": False})
    return out


def _store(rev, oi, ocf=None, capex=None, n=None, annual=None):
    n = n or len(rev)
    ends = _qends(n)
    f = {"revenue": {"entries": _entries(rev, ends)}, "operating_income": {"entries": _entries(oi, ends)}}
    if ocf:
        f["operating_cash_flow"] = {"entries": _entries(ocf, ends)}
    if capex:
        f["capital_expenditure"] = {"entries": _entries(capex, ends)}
    for k, pts in (annual or {}).items():
        f.setdefault(k, {"entries": []})["entries"] += [
            {"end": e, "val": v, "first_val": v, "first_filed": (date.fromisoformat(e) + timedelta(days=60)).isoformat(),
             "period_days": 365, "is_annual": True, "is_ytd": False} for e, v in pts]
    return {"fields": f}


def test_profit_stage_by_ttm_operating_income():
    r = pf.compute("A", "2026-12-31", _store([100] * 12, [5] * 12))
    assert r["revenue_stage"] == "profit" and r["ttm_operating_income"] == 20 and r["breakeven"] is None


def test_near_profit_when_margin_reaches_zero_within_two_years():
    # 四半期の営業利益率が −30% から毎期 +2pt 改善 → TTM の率も上向き、2年以内に0
    oi = [-30 + 2 * k for k in range(14)]
    r = pf.compute("A", "2026-12-31", _store([100] * 14, oi))
    assert r["revenue_stage"] == "near_profit"
    be = r["breakeven"]
    assert be["reason"] == "PREDICTED" and be["method"] == pf.BREAKEVEN_METHOD and len(be["basis_ends"]) == 8
    assert 0 <= be["quarters_to_zero"] <= 8 and be["slope_per_quarter"] > 0


def test_loss_when_flat_far_or_short():
    flat = pf.compute("A", "2026-12-31", _store([100] * 14, [-20] * 14))
    assert flat["revenue_stage"] == "loss" and flat["breakeven"]["reason"] == "NO_TREND"
    far = pf.compute("A", "2026-12-31", _store([100] * 14, [-60 + 0.5 * k for k in range(14)]))
    assert far["revenue_stage"] == "loss" and far["breakeven"]["reason"] == "TOO_FAR"
    short = pf.compute("A", "2026-12-31", _store([100] * 8, [-30 + 3 * k for k in range(8)]))
    assert short["revenue_stage"] == "loss" and short["breakeven"]["reason"] == "NO_DATA"   # TTMは5点だけ


def test_point_in_time_uses_first_filed_and_first_val():
    ends = _qends(8)
    store = {"fields": {"revenue": {"entries": _entries([100] * 8, ends)},
                        # 後の申告で黒字に組み替えられた値（val）でも、時点の値（first_val）は赤字
                        "operating_income": {"entries": _entries([10] * 8, ends, first=[-10] * 8)}}}
    assert pf.compute("A", "2026-12-31", store)["ttm_operating_income"] == -40
    # 最後の四半期の申告日より前の時点では、その四半期を使わない
    before = (date.fromisoformat(ends[-1]) + timedelta(days=10)).isoformat()
    assert pf.compute("A", before, store)["ttm_end"] == ends[-2]


def test_sector_master_unclassified_and_financial():
    s = _store([100] * 12, [5] * 12)
    assert pf.compute("A", "2026-12-31", s, {"unclassified": "区分なし（Biotechnology）"})["revenue_stage"] == "unclassified"
    r = pf.compute("A", "2026-12-31", s, {"unclassified": None, "financial": {"value": True}})
    assert r["revenue_stage"] == "unclassified" and "P/TBV" in r["stage_reason"]


def test_structural_deficit_calls_tanuki_definition():
    n = 24
    neg = pf.compute("A", "2026-12-31", _store([100] * n, [5] * n, ocf=[5] * n, capex=[10] * n))
    assert neg["structural_deficit"]["value"] is True and len(neg["structural_deficit"]["fcf_points"]) == 5
    pos = pf.compute("A", "2026-12-31", _store([100] * n, [5] * n, ocf=[15] * n, capex=[10] * n))
    assert pos["structural_deficit"]["value"] is False
    # 判定は TANUKI の関数そのもの
    import importlib.util
    spec = importlib.util.spec_from_file_location("adj", os.path.join(REPO, "src", "value", "tanuki_valuation", "calculator", "adjustments.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    for a, b in ((-1, -1), (-1, 1), (1, -1), (0, 0), (None, None)):
        assert pf.is_structural_deficit(a, b) == m.is_structural_deficit(a, b)


def test_deficit_quality_score_matches_stonks_and_fixed_cases():
    from analyzer import StonksAnalyzer
    cases = [
        ((-5.0, 55.0, {}, 30.0, 35.0, 75.0), ("GOOD_DEFICIT", 95.0)),
        ((-5.0, 25.0, {}, 15.0, 10.0, 40.0), ("WATCH", 43.0)),
        ((-5.0, 5.0, {}, None, None, 20.0), ("BAD_DEFICIT", 5.0)),
        ((10.0, 12.0, {}, 5.0, 5.0, 55.0), ("PROFITABLE", 40.0)),
        ((None, None, {}, None, None, None), ("BAD_DEFICIT", 0.0)),
    ]
    a = StonksAnalyzer()
    for args, (verdict, score) in cases:
        v, why, sc = pf.deficit_quality_verdict(*args)
        assert (v, sc) == (verdict, score), (args, v, sc, why)
        assert a._deficit_verdict(*args) == (v, why, sc)        # STONKS は同じ関数を呼ぶ


def test_build_single_ticker_keeps_others_and_write_is_idempotent(tmp_path):
    lookup = tmp_path / "cik.csv"
    with open(lookup, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "cik", "status"])
        for i, t in enumerate(("AAA", "BBB", "OLD")):
            w.writerow([t, str(i + 1).zfill(10), "retired" if t == "OLD" else "active"])
    master = tmp_path / "sm.json"
    master.write_text(json.dumps({"tickers": {"BBB": {"unclassified": None, "financial": {"value": True}}}}), encoding="utf-8")
    out = tmp_path / "profitability.json"
    stores = {"AAA": _store([100] * 12, [5] * 12), "BBB": _store([100] * 12, [5] * 12)}
    kw = dict(path=str(out), store_fn=stores.get, sector_master_path=str(master), lookup_path=str(lookup),
              as_of="2026-12-31", now="2026-12-31T00:00:00+00:00")
    full = pf.build(**kw)
    assert set(full["tickers"]) == {"AAA", "BBB"} and full["tickers"]["BBB"]["revenue_stage"] == "unclassified"
    assert pf.write(full, str(out)) is True and pf.write(pf.build(**kw), str(out)) is False
    stores["AAA"] = _store([100] * 12, [-5] * 12)
    one = pf.build(["AAA"], **kw)
    assert one["tickers"]["AAA"]["revenue_stage"] == "loss" and one["tickers"]["BBB"] == full["tickers"]["BBB"]
