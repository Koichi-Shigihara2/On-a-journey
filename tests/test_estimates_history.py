"""アナリストの EPS 予想の改定の週1の履歴（common/market_data/estimates_history.py、[[EPS-REVISION-OUTLOOK-MARK-1]]、2026-11-28）"""
import json
import os
import sys
from datetime import date

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common.market_data import estimates_history as eh  # noqa: E402
from common.market_data import fetcher  # noqa: E402


def _attrs(fetched="2026-11-28T10:00:00+00:00"):
    rev = {p: {"up_7d": 0, "down_7d": 2, "up_30d": 2, "down_30d": 19} for p in eh.PERIODS}
    trend = {p: {"current": 3.19923, "ago_7d": 3.76, "ago_30d": 3.85, "ago_60d": 3.85, "ago_90d": 4.17} for p in eh.PERIODS}
    return {"symbol": "INTU", "fetched_at": fetched,
            "estimates": {"eps_revisions": rev, "eps_trend": trend, "fetched_at": fetched,
                          "period_ends": {"0q": "2027-01-31", "+1q": "2027-04-30", "0y": "2027-07-31", "+1y": "2028-07-31"},
                          "fiscal": {"most_recent_quarter": "2026-10-31", "next_fiscal_year_end": "2027-07-31"}}}


def test_row_has_all_four_periods_with_end_dates_and_fetch_date():
    row = eh.build_row(_attrs())
    assert row["date"] == "2026-11-28" and row["fetched_at"].startswith("2026-11-28")
    assert set(row["p"]) == {"0q", "+1q", "0y", "+1y"}
    c = row["p"]["0q"]
    assert c == {"end": "2027-01-31", "up7": 0, "dn7": 2, "up30": 2, "dn30": 19, "e0": 3.1992, "e7": 3.76, "e30": 3.85, "e60": 3.85, "e90": 4.17}
    assert row["fiscal"]["most_recent_quarter"] == "2026-10-31"


def test_no_estimates_means_no_row():
    assert eh.build_row({"symbol": "X", "fetched_at": "2026-11-28T00:00:00+00:00"}) is None
    assert eh.build_row({"estimates": {"fetched_at": "2026-11-28T00:00:00+00:00"}}) is None


def test_append_is_add_only_and_one_row_per_date(tmp_path):
    attr, out = tmp_path / "attributes", tmp_path / "hist"
    attr.mkdir()
    (attr / "INTU.json").write_text(json.dumps(_attrs()), encoding="utf-8")
    assert eh.append("INTU", str(attr), str(out)) is True
    assert eh.append("INTU", str(attr), str(out)) is False          # 同じ日付は足さない
    (attr / "INTU.json").write_text(json.dumps(_attrs("2026-12-05T10:00:00+00:00")), encoding="utf-8")
    assert eh.append("INTU", str(attr), str(out)) is True
    doc = json.load(open(out / "INTU.json", encoding="utf-8"))
    assert [r["date"] for r in doc["records"]] == ["2026-11-28", "2026-12-05"]
    lines = open(out / "INTU.json", encoding="utf-8").read().splitlines()
    assert sum(1 for ln in lines if ln.strip().startswith('{"date"')) == 2         # 1行が1行（毎週の差分が増えた行だけ）


def test_period_ends_are_derived_from_fiscal_dates():
    """0y＝次の決算年度末、+1y＝+12か月、0q＝直近の発表済み四半期末+3か月、+1q＝+6か月（月末に合わせる）"""
    from datetime import datetime, timezone
    ts = lambda y, m, d: datetime(y, m, d, tzinfo=timezone.utc).timestamp()
    r = fetcher._period_ends({"mostRecentQuarter": ts(2026, 10, 31), "nextFiscalYearEnd": ts(2027, 7, 31), "lastFiscalYearEnd": ts(2026, 7, 31)})
    assert r["period_ends"] == {"0q": "2027-01-31", "+1q": "2027-04-30", "0y": "2027-07-31", "+1y": "2028-07-31"}
    assert r["fiscal"] == {"most_recent_quarter": "2026-10-31", "next_fiscal_year_end": "2027-07-31", "last_fiscal_year_end": "2026-07-31"}
    r2 = fetcher._period_ends({"mostRecentQuarter": ts(2026, 9, 30), "lastFiscalYearEnd": ts(2025, 12, 31)})      # nextFiscalYearEnd が無ければ last + 12か月
    assert r2["period_ends"] == {"0q": "2026-12-31", "+1q": "2027-03-31", "0y": "2026-12-31", "+1y": "2027-12-31"}
    assert fetcher._period_ends({})["period_ends"] == {}


def test_fetch_estimates_returns_four_periods_and_trend(monkeypatch):
    import pandas as pd

    class T:
        revenue_estimate = pd.DataFrame()
        eps_revisions = pd.DataFrame({"upLast7days": [0, 1, 2, 2], "upLast30days": [2, 2, 3, 2], "downLast30days": [19, 2, 2, 3],
                                      "downLast7Days": [2, 0, 1, 1]}, index=pd.Index(list(eh.PERIODS), name="period"))
        eps_trend = pd.DataFrame({"current": [3.2, 13.4, 22.9, 25.9], "7daysAgo": [3.7, 13.3, 23.0, 26.1], "30daysAgo": [3.8, 13.3, 23.0, 26.1],
                                  "60daysAgo": [3.8, 13.3, 23.5, 26.6], "90daysAgo": [4.2, 13.6, 24.0, 27.1]}, index=pd.Index(list(eh.PERIODS), name="period"))
    out = fetcher._fetch_estimates(T(), {})
    assert set(out["eps_revisions"]) == set(eh.PERIODS) and out["eps_revisions"]["0q"]["down_30d"] == 19
    assert set(out["eps_trend"]) == set(eh.PERIODS) and out["eps_trend"]["0q"]["ago_90d"] == 4.2
    assert "period_ends" in out
    # 既存の読み手（guidance_gap）は +1y の30日の件数を同じキーで読む
    assert out["eps_revisions"]["+1y"]["up_30d"] == 2 and out["eps_revisions"]["+1y"]["down_30d"] == 3
