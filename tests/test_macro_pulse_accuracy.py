"""
tests/test_macro_pulse_accuracy.py

MACRO PULSE の正確性修正（指示書M-2、2026-10-03）の回帰テスト。
- [[MACRO-PULSE-CLAIMS-RELEASE-ID-WRONG-1]]: Initial Claimsの公表元・観測日への配置・離れた行同士の比較
- 先読み除外（_compute_current_score は、その日までに書き込まれた行だけを使う）

実行方法:
    python -m pytest tests/test_macro_pulse_accuracy.py -v
"""

import importlib.util
import pathlib
from datetime import date, timedelta

import pandas as pd

_MACRO_DIR = pathlib.Path(__file__).resolve().parent.parent / "src" / "market" / "macro_pulse"


def _load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, _MACRO_DIR / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


main05 = _load_module("main05_accuracy_test", "05_main.py")


def _event(indicator, release_date, actual, updated_at=None):
    r = {c: "" for c in main05.EVENTS_COLUMNS}
    r.update({
        "event_id": f"{indicator}_{release_date}",
        "indicator": indicator,
        "release_date": release_date,
        "actual": str(actual),
        "updated_at": updated_at or f"{release_date} 12:00:00",
    })
    return r


def _events(rows):
    return pd.DataFrame(rows, columns=main05.EVENTS_COLUMNS).fillna("")


# ─────────────────────────────────────────────────────────────────
#  Initial Claims: 公表元と、観測日への配置
# ─────────────────────────────────────────────────────────────────
class TestClaimsReleaseAndPlacement:
    def test_release_id_is_weekly_claims_report(self):
        # 321 は Empire State Manufacturing Survey、180 が Unemployment Insurance Weekly Claims Report
        assert main05.INDICATOR_CONFIG["Initial Claims 4W MA"]["fred_release_id"] == 180

    def test_latest_week_is_placed_on_observation_date_not_future_slot(self, monkeypatch):
        """2026-10-02の実例: 09-26週の値が予定の枠 2026-10-15 の行に入り、今日の計算に使われなかった。"""
        def fake_latest(series_id):
            if series_id == "IC4WSA":
                return 200000.0, date(2026, 9, 26)
            return None, None
        monkeypatch.setattr(main05, "fred_latest", fake_latest)
        schedule = pd.DataFrame([
            {**{c: "" for c in main05.SCHEDULE_COLUMNS},
             "indicator": "Initial Claims 4W MA", "release_date": "2026-10-15", "fred_id": "IC4WSA"},
        ])
        events = _events([_event("Initial Claims 4W MA", "2026-09-19", 202250.0)])
        rows = main05.refresh_monthly_indicators(date(2026, 10, 2), {}, schedule, events, None)
        claims = [r for r in rows if r["indicator"] == "Initial Claims 4W MA"]
        assert len(claims) == 1
        assert claims[0]["release_date"] == "2026-09-26"
        assert claims[0]["event_id"] == "ic4wsa_2026-09-26"

    def test_monthly_indicator_still_uses_schedule_slot(self, monkeypatch):
        """週次以外（Building Permits）は従来どおり予定の枠に置く（今回の修正の対象外）。"""
        def fake_latest(series_id):
            if series_id == "PERMIT":
                return 1403.0, date(2026, 8, 1)
            return None, None
        monkeypatch.setattr(main05, "fred_latest", fake_latest)
        schedule = pd.DataFrame([
            {**{c: "" for c in main05.SCHEDULE_COLUMNS},
             "indicator": "Building Permits", "release_date": "2026-09-15", "fred_id": "PERMIT"},
        ])
        rows = main05.refresh_monthly_indicators(date(2026, 9, 18), {}, schedule, _events([]), None)
        permits = [r for r in rows if r["indicator"] == "Building Permits"]
        assert permits and permits[0]["release_date"] == "2026-09-15"


# ─────────────────────────────────────────────────────────────────
#  サプライズ検知: 離れた行同士を比べない
# ─────────────────────────────────────────────────────────────────
class TestSurpriseConsecutiveRowsOnly:
    def test_claims_rows_five_weeks_apart_are_not_compared(self):
        """2026-07-11の実例: 05-16週（202,500）と06-20週（224,250）を比べて「+21,750件 急悪化」と出た。"""
        t = date.today()
        rows = [
            _event("Initial Claims 4W MA", (t - timedelta(days=40)).isoformat(), 202500.0),
            _event("Initial Claims 4W MA", (t - timedelta(days=5)).isoformat(), 224250.0),
        ]
        alerts = main05.detect_macro_surprises(_events(rows))
        assert not [a for a in alerts if "Initial Claims" in a]

    def test_consecutive_weeks_are_still_compared(self):
        t = date.today()
        rows = [
            _event("Initial Claims 4W MA", (t - timedelta(days=12)).isoformat(), 202500.0),
            _event("Initial Claims 4W MA", (t - timedelta(days=5)).isoformat(), 224250.0),
        ]
        alerts = main05.detect_macro_surprises(_events(rows))
        assert [a for a in alerts if "Initial Claims" in a]


# ─────────────────────────────────────────────────────────────────
#  _compute_current_score: その日までに書き込まれた行だけを使う
# ─────────────────────────────────────────────────────────────────
class TestComputeScoreKnownAsOf:
    def test_row_written_after_target_date_is_not_used(self):
        """観測日（09-01）に置いたPhilly Fed 9月分は09-18に書かれた。09-13時点の計算には入らない。"""
        rows = [
            _event("Philadelphia Fed Manufacturing", "2026-08-01", 47.4, "2026-08-21 22:44:17"),
            _event("Philadelphia Fed Manufacturing", "2026-09-01", 37.8, "2026-09-18 00:22:28"),
        ]
        res = main05._compute_current_score(_events(rows), date(2026, 9, 13))
        assert res["indicators"]["philly"]["value"] == 47.4
        res2 = main05._compute_current_score(_events(rows), date(2026, 9, 20))
        assert res2["indicators"]["philly"]["value"] == 37.8

    def test_row_written_after_utc_midnight_by_same_us_day_run_is_used(self):
        """10-02分の日次の実行がUTCの0時をまたいで10-03T02:06Zに書いた行は、target_date=10-02（米国の日付）で使う。
        比較は「10-02の米国東部時間23:59:59」（=10-03T03:59:59Z）とupdated_at（UTC）の時刻で行う。"""
        rows = [
            _event("Initial Claims 4W MA", "2026-09-19", 202250.0, "2026-09-25 00:35:10"),
            _event("Initial Claims 4W MA", "2026-09-26", 200000.0, "2026-10-03 02:06:00"),
        ]
        res = main05._compute_current_score(_events(rows), date(2026, 10, 2))
        assert res["indicators"]["claims"]["value"] == 200000.0

    def test_row_written_after_us_eastern_day_end_is_not_used(self):
        """10-03T04:30Z（米国東部時間10-03 00:30）に書いた行は、target_date=10-02では使わない。"""
        rows = [
            _event("Initial Claims 4W MA", "2026-09-19", 202250.0, "2026-09-25 00:35:10"),
            _event("Initial Claims 4W MA", "2026-09-26", 200000.0, "2026-10-03 04:30:00"),
        ]
        res = main05._compute_current_score(_events(rows), date(2026, 10, 2))
        assert res["indicators"]["claims"]["value"] == 202250.0
