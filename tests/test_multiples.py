"""倍率の履歴の部品（common/multiples.py）とスナップショットの履歴（common/market_data/valuation_history.py）。
[[HYPECORE-STAGE-TIMESCALE-1]] 段B-4（2026-11-05）。テストは作った値と一時フォルダだけを使う
"""
import json
import os
import sys
from datetime import date, timedelta

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import multiples as mp
from common.market_data import valuation_history as vh


# ── HON型の分割の検知 ─────────────────────────────────────────────────
def test_hon_reverse_split_with_spin_is_detected():
    """HON（2026-06-29 1株を0.5株に、分社と同時）: その時点の株価227.38（TANUKIの記録、06-25 01:40 JST）と、今の日足の
    06-24の終値238.51の比は1.049倍で、比0.5から期待される2倍と違う→ mismatch"""
    daily = [{"date": "2026-06-24", "close": 238.51}, {"date": "2026-06-26", "close": 243.53}, {"date": "2026-06-29", "close": 227.8}]
    recorded = [("2026-06-25T01:40:03+09:00", 227.38), ("2026-06-30T02:21:44+09:00", 235.17)]
    r = mp.detect_split_price("HON", [{"date": "2026-06-29", "ratio": 0.5}], daily, recorded)
    assert r[0]["status"] == "mismatch" and abs(r[0]["observed_factor"] - 238.51 / 227.38) < 1e-6 and r[0]["expected_factor"] == 2.0


def test_normal_split_is_ok_and_no_record_is_unverifiable():
    # 10:1 の分割を正しく調整した系列（今の日足は記録の1/10）
    ok = mp.detect_split_price("X", [{"date": "2024-06-10", "ratio": 10}],
                               [{"date": "2024-06-06", "close": 120.0}], [("2024-06-07T01:00:00+09:00", 1200.0)])
    assert ok[0]["status"] == "ok"
    none = mp.detect_split_price("X", [{"date": "2024-06-10", "ratio": 10}], [{"date": "2024-06-06", "close": 120.0}], [])
    assert none[0]["status"] == "unverifiable"


# ── 1銘柄の計算 ───────────────────────────────────────────────────────
def _e(end, val, filed, days=91, annual=False):
    return {"end": end, "val": val, "first_val": val, "first_filed": filed, "filed": filed,
            "period_days": 365 if annual else days, "is_annual": annual, "is_ytd": False}


def _store():
    q_ends = [(date(2025, 3, 31) + timedelta(days=91 * k)).isoformat() for k in range(6)]
    f = lambda end: (date.fromisoformat(end) + timedelta(days=40)).isoformat()
    return {"fields": {
        "net_income": {"entries": [_e(e, 25.0, f(e)) for e in q_ends]},
        "revenue": {"entries": [_e(e, 100.0, f(e)) for e in q_ends]},
        "gross_profit": {"entries": [_e(e, 60.0, f(e)) for e in q_ends]},
        "shares_diluted": {"entries": [{**_e(e, 10.0, f(e)), "source_tag": "WeightedAverageNumberOfDilutedSharesOutstanding"} for e in q_ends]},
        "stockholders_equity": {"entries": [{**_e(e, 500.0, f(e)), "period_days": 0} for e in q_ends]},
        "cash_and_equivalents": {"entries": [{**_e(e, 50.0, f(e)), "period_days": 0} for e in q_ends]},
        "long_term_debt": {"entries": [{**_e(e, 150.0, f(e)), "period_days": 0} for e in q_ends]},
    }}, q_ends


def test_compute_point_in_time_multiples():
    store, q_ends = _store()
    prices = [{"date": "2026-06-30", "close": 200.0}, {"date": "2026-07-31", "close": 210.0}]
    months = [("2026-06", "2026-06-30"), ("2026-07", "2026-07-31")]
    tce = {e: 400.0 for e in q_ends}
    out = mp.compute("X", store, prices, months, [], tce, [])
    jun, jul = out["months"]
    # 6月末: 最新で申告済みの四半期は 2026-03-30（申告 05-09）。TTM純利益100・売上400・粗利240、株数10
    assert jun["ttm_end"] == q_ends[4] and jun["shares"] == 10.0
    assert jun["market_cap"] == 2000.0 and jun["enterprise_value"] == 2100.0
    assert jun["per"] == 20.0 and abs(jun["ev_sales"] - 2100 / 400) < 1e-9 and abs(jun["ev_gross_profit"] - 2100 / 240) < 1e-9
    assert jun["p_tbv"] == 5.0 and jun["pe_5y_avg_earnings"] is None      # 年次が3年に足りない
    assert jul["per"] == 21.0                                               # 7月末の株価


def test_split_mismatch_blocks_months_before_split_and_past_months_stable():
    store, q_ends = _store()
    prices = [{"date": "2026-06-30", "close": 200.0}, {"date": "2026-07-31", "close": 210.0}, {"date": "2026-08-31", "close": 220.0}]
    ledger = [{"ticker": "X", "split_date": "2026-07-15", "status": "mismatch"}]
    m2 = [("2026-06", "2026-06-30"), ("2026-07", "2026-07-31")]
    out = mp.compute("X", store, prices, m2, [], {}, ledger)
    assert out["months"][0]["undetermined"] == "split_price_mismatch" and out["months"][0]["per"] is None
    assert out["months"][1]["undetermined"] is None
    # 月が増えても過去の月の値は変わらない
    out3 = mp.compute("X", store, prices, m2 + [("2026-08", "2026-08-31")], [], {}, ledger)
    assert out3["months"][:2] == out["months"]


def test_forward_pe_snapshot_at_month_end():
    store, q_ends = _store()
    prices = [{"date": "2026-11-30", "close": 200.0}, {"date": "2026-12-31", "close": 210.0}, {"date": "2027-02-26", "close": 220.0}]
    snaps = [{"date": "2026-11-01", "forward_pe": 12.0}, {"date": "2026-12-27", "forward_pe": 13.0}]
    rows = mp.compute("X", store, prices, [("2026-11", "2026-11-30"), ("2026-12", "2026-12-31"), ("2027-02", "2027-02-26")],
                      [], {}, [], snaps)["months"]
    assert [(r["forward_pe_snapshot"], r["forward_pe_snapshot_date"]) for r in rows] ==         [(12.0, "2026-11-01"), (13.0, "2026-12-27"), (None, None)]          # 31日より古い値は使わない


def test_month_ends_exclude_current_month():
    rows = [{"date": d} for d in ("2026-09-29", "2026-09-30", "2026-10-30", "2026-11-03")]
    assert mp.month_ends(rows, today="2026-11-05", start="2026-09") == [("2026-09", "2026-09-30"), ("2026-10", "2026-10-30")]


def test_peer_percentile_uses_sector_or_all():
    vals = {"A": 10.0, "B": 20.0, "C": 30.0, "D": 40.0, "E": 50.0, "F": 5.0}
    sector = {"A": "Tech", "B": "Tech", "C": "Tech", "D": "Tech", "E": "Tech", "F": "Energy"}
    assert mp.peer_percentile(vals, "C", sector) == 50.0                   # Tech 5銘柄の中
    assert mp.peer_percentile(vals, "F", sector) == 0.0                    # Energy は1銘柄→全銘柄
    assert mp.peer_percentile({**vals, "G": None}, "G", sector) is None


# ── スナップショットの履歴 ─────────────────────────────────────────────
def test_valuation_history_is_append_only(tmp_path):
    attr, out = tmp_path / "attr", tmp_path / "out"
    attr.mkdir()
    (attr / "X.json").write_text(json.dumps({"fetched_at": "2026-11-01T18:00:00+00:00", "forward_pe": 12.0, "trailing_pe": 25.0}),
                                 encoding="utf-8")
    assert vh.append("X", str(attr), str(out)) is True
    assert vh.append("X", str(attr), str(out)) is False                    # 同じ日付は足さない
    (attr / "X.json").write_text(json.dumps({"fetched_at": "2026-11-08T18:00:00+00:00", "forward_pe": 13.0}), encoding="utf-8")
    assert vh.append("X", str(attr), str(out)) is True
    recs = json.loads((out / "X.json").read_text(encoding="utf-8"))["records"]
    assert [(r["date"], r["forward_pe"]) for r in recs] == [("2026-11-01", 12.0), ("2026-11-08", 13.0)]
