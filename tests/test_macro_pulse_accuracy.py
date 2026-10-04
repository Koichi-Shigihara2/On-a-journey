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


# ─────────────────────────────────────────────────────────────────
#  [[MACRO-PULSE-LIQUIDITY-DAILY-ROWS-AS-WEEKS-1]]: 週単位（H.4.1の水曜）の判定
# ─────────────────────────────────────────────────────────────────
def _weekly_series(nl_points, rrp_daily=None, sp=None):
    """nl_points: [(水曜, WALCL, TGA, RRP_billions)]"""
    s = {"WALCL": [], "WTREGEN": [], "WRBWFRBL": [], "RRPONTSYD": [], "SP500": sp or []}
    for w, fed, tga, rrp in nl_points:
        s["WALCL"].append((w, fed))
        s["WTREGEN"].append((w, tga))
        s["WRBWFRBL"].append((w, 2900000.0))
        s["RRPONTSYD"].append((w, rrp))
    for d, v in (rrp_daily or []):
        s["RRPONTSYD"].append((d, v))
    return s


class TestWeeklyLiquidityState:
    def test_daily_declines_within_one_week_are_not_counted_as_weeks(self):
        """2026-10-02の実例: 09-30〜10-02に日次のNET流動性が3日続けて減り「3週連続減少」と出た。
        水曜の値で比べると、NET流動性は09-23 5.770→09-30 5.783兆ドルで増えており、連続減少は0週
        （10-01のRRPの日次の値は、週の判定には使わない）。"""
        s = _weekly_series(
            [("2026-09-16", 6746548.0, 877028.0, 0.576), ("2026-09-23", 6747704.0, 977084.0, 0.63),
             ("2026-09-30", 6743031.0, 948674.0, 11.539)],
            rrp_daily=[("2026-10-01", 0.35)])
        st = main05.weekly_liquidity_state(s, date(2026, 10, 2))
        assert st["h41_date"] == "2026-09-30"
        assert st["decline_weeks"] == 0

    def test_consecutive_weekly_declines_are_counted(self):
        s = _weekly_series([("2026-09-02", 6.8e6, 8.0e5, 1.0), ("2026-09-09", 6.79e6, 8.1e5, 1.0),
                            ("2026-09-16", 6.78e6, 8.2e5, 1.0), ("2026-09-23", 6.77e6, 8.3e5, 1.0)])
        assert main05.weekly_liquidity_state(s, date(2026, 9, 25))["decline_weeks"] == 3

    def test_absorb_vs_supply_uses_weekly_walcl_change(self):
        """吸収額（RRP増＋TGA増）と供給額（WALCLの増加）は同じ週の間隔で比べる。"""
        s = _weekly_series([("2026-09-16", 6.70e6, 8.0e5, 1.0), ("2026-09-23", 6.80e6, 8.5e5, 1.0)])
        st = main05.weekly_liquidity_state(s, date(2026, 9, 25))
        assert st["absorb_exceeds_supply"] is False  # 吸収5万 < 供給10万

    def test_sp500_5d_return_uses_five_observations_back(self):
        sp = [("2026-09-24", 100.0), ("2026-09-25", 101.0), ("2026-09-28", 102.0), ("2026-09-29", 103.0),
              ("2026-09-30", 104.0), ("2026-10-01", 102.0)]
        s = _weekly_series([("2026-09-23", 6.7e6, 8e5, 1.0), ("2026-09-30", 6.7e6, 8e5, 1.0)], sp=sp)
        st = main05.weekly_liquidity_state(s, date(2026, 10, 2))
        assert abs(st["sp500_5d_pct"] - 2.0) < 1e-9

    def test_update_liquidity_csv_counts_weeks_not_rows(self, tmp_path, monkeypatch):
        """CSVに日次で減り続ける行（3日）があっても、数えるのは水曜の値の連続減少（09-16→09-23の1週）。"""
        liq = tmp_path / "05_liquidity.csv"
        rows = []
        for d, nl in [("2026-09-29", "5.77"), ("2026-09-30", "5.7698"), ("2026-10-01", "5.7592")]:
            r = {c: "" for c in main05.LIQUIDITY_COLUMNS}
            r.update({"date": d, "fed_balance": "6747704.0", "tga": "977084.0", "rrp": "576.0",
                      "net_liquidity": nl, "reserve_balance": "2969922.0", "stealth_signal": "neutral",
                      "stealth_absorb_weeks": "0", "net_liq_decline_weeks": "2", "sp500": "7650.0"})
            rows.append(r)
        pd.DataFrame(rows, columns=main05.LIQUIDITY_COLUMNS).to_csv(liq, index=False)
        monkeypatch.setattr(main05, "LIQUIDITY_PATH", str(liq))
        monkeypatch.setattr(main05, "BASE_DATA_DIR", str(tmp_path))
        latest = {"M2SL": 23342.8, "BAMLH0A0HYM2": 3.12, "WALCL": 6747704.0, "WTREGEN": 977084.0,
                  "RRPONTSYD": 11.539, "WRBWFRBL": 2969922.0}
        monkeypatch.setattr(main05._md_reader, "get_latest",
                            lambda sid: {"value": latest[sid], "as_of": "2026-10-01"} if sid in latest else None)
        weekly = _weekly_series([("2026-09-16", 6746548.0, 877028.0, 0.576),
                                 ("2026-09-23", 6747704.0, 977084.0, 0.63)])
        monkeypatch.setattr(main05._md_reader, "get_series", lambda sid, **kw: [
            {"as_of": d, "value": v} for d, v in weekly.get(sid, [])])
        main05.update_liquidity_csv(date(2026, 10, 2), sp500_val=7651.54)
        df = pd.read_csv(liq, dtype=str).fillna("")
        last = df[df["date"] == "2026-10-02"].iloc[0]
        assert last["net_liq_decline_weeks"] == "1"
        assert "週連続減少" not in last["stealth_alert"]
        assert last["h41_date"] == "2026-09-23"


# ─────────────────────────────────────────────────────────────────
#  [[MACRO-PULSE-AI-DELTA-LOOKAHEAD-1]]: AIカードの週±は前週の週次スナップショットとの差
# ─────────────────────────────────────────────────────────────────
class TestWeeklyDeltaFromSnapshot:
    def _wa(self, rows):
        return pd.DataFrame([{**{c: "" for c in main05.WEEKLY_ANALYSIS_COLUMNS}, "analysis_date": d, "score": str(s)}
                             for d, s in rows], columns=main05.WEEKLY_ANALYSIS_COLUMNS)

    def test_delta_is_difference_from_previous_snapshot(self):
        """2026-08-22の実例: 前週（08-15）のスナップショット27→22。記録された週±は0だった。"""
        wa = self._wa([("2026-08-08", 27), ("2026-08-15", 27)])
        assert main05._score_change_vs_prev_snapshot(22, date(2026, 8, 22), wa) == -5

    def test_no_recent_snapshot_returns_none(self):
        wa = self._wa([("2026-07-01", 27)])
        assert main05._score_change_vs_prev_snapshot(22, date(2026, 8, 22), wa) is None

    def test_run_weekly_analysis_passes_snapshot_delta_to_grok(self, tmp_path, monkeypatch):
        """run_weekly_analysis()がGrokへ渡す先週比・CSVのscore_change_1wが、前週のスナップショットとの差になる。"""
        wa_path = tmp_path / "05_weekly_analysis.csv"
        self._wa([("2026-09-20", 30)]).to_csv(wa_path, index=False)
        monkeypatch.setattr(main05, "WEEKLY_ANALYSIS_PATH", str(wa_path))
        monkeypatch.setattr(main05, "FED_CONTEXT_PATH", str(tmp_path / "none.csv"))
        monkeypatch.setattr(main05, "BASE_DATA_DIR", str(tmp_path))
        ev = _events([_event("Yield Curve 10Y-2Y", "2026-09-25", 0.36, "2026-09-26 00:38:00"),
                      _event("HY Spread", "2026-09-25", 2.93, "2026-09-26 00:38:00")])
        monkeypatch.setattr(main05, "load_events", lambda: ev)
        captured = {}

        def fake_grok(target_date, score_data, recent_events, score_1w, score_1m, fed_context, indicator_deltas=None):
            captured["score"] = score_data["score"]
            captured["score_1w"] = score_1w
            return {"summary": "", "_used_model": "test"}
        monkeypatch.setattr(main05, "generate_weekly_analysis_with_grok", fake_grok)
        monkeypatch.setattr(main05, "send_discord", lambda msg: None)
        main05.run_weekly_analysis(date(2026, 9, 27))
        assert captured["score_1w"] == captured["score"] - 30
        saved = pd.read_csv(wa_path, dtype=str)
        assert saved[saved["analysis_date"] == "2026-09-27"].iloc[0]["score_change_1w"] == str(captured["score"] - 30)


# ─────────────────────────────────────────────────────────────────
#  STEP 6: CFNAI-MA3・実行の対象日
# ─────────────────────────────────────────────────────────────────
class TestCfnaiMa3Series:
    def test_fetches_three_month_moving_average(self):
        """[[MACRO-PULSE-CFNAI-MA3-SERIES-1]]: 表示・説明・閾値（−0.7）はCFNAI-MA3の前提。"""
        assert main05.INDICATOR_CONFIG["Chicago Fed National Activity"]["fred_id"] == "CFNAIMA3"

    def test_series_meta_lists_ma3(self):
        import json
        meta_path = pathlib.Path(__file__).resolve().parent.parent / "common" / "macro_data" / "series_meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert "CFNAIMA3" in meta and "CFNAI" not in meta


class TestDefaultTargetDate:
    """[[MACRO-PULSE-RUN-DATE-UTC-SHIFT-1]]: 起動が遅れてUTCの0時をまたいでも、予定日（米国の日付）と一致する。"""

    def _utc(self, s):
        from datetime import datetime, timezone
        return datetime.strptime(s, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)

    def test_on_time_run_is_scheduled_day(self):
        # 10-02 22:15 UTC = 10-02 18:15 EDT
        assert main05.default_target_date(self._utc("2026-10-02T22:15")) == date(2026, 10, 2)

    def test_delayed_run_after_utc_midnight_is_still_scheduled_day(self):
        # 実例: 10-02 22:15 UTCのcronが10-03 01:12 UTC（10-02 21:12 EDT）に起動
        assert main05.default_target_date(self._utc("2026-10-03T01:12")) == date(2026, 10, 2)
        # 直近20回で最大の遅れ（+3.7h）: 09-28 22:15 → 09-29 01:58 UTC
        assert main05.default_target_date(self._utc("2026-09-29T01:58")) == date(2026, 9, 28)

    def test_before_1700_eastern_uses_previous_business_day(self):
        # 10-02 15:00 UTC = 10-02 11:00 EDT → 10-01
        assert main05.default_target_date(self._utc("2026-10-02T15:00")) == date(2026, 10, 1)
        # 月曜の朝（10-05 14:00 UTC = 10:00 EDT）→ 前の金曜 10-02
        assert main05.default_target_date(self._utc("2026-10-05T14:00")) == date(2026, 10, 2)

    def test_previous_business_day_skips_holiday(self):
        # 2026-09-08（火）10:00 EDT → 9/7はLabor Day → 9/4（金）
        assert main05.default_target_date(self._utc("2026-09-08T14:00")) == date(2026, 9, 4)

    def test_winter_time(self):
        # 12-01 22:15 UTC = 12-01 17:15 EST → 12-01
        assert main05.default_target_date(self._utc("2026-12-01T22:15")) == date(2026, 12, 1)
        # 12-02 01:30 UTC = 12-01 20:30 EST → 12-01
        assert main05.default_target_date(self._utc("2026-12-02T01:30")) == date(2026, 12, 1)


# ─────────────────────────────────────────────────────────────────
#  M-3 STEP 1: 公開時点の列（known_at）[[MACRO-PULSE-HISTORY-IMPORT-UPDATED-AT-1]]
# ─────────────────────────────────────────────────────────────────
class TestKnownAt:
    def test_events_columns_have_known_at(self):
        assert "known_at" in main05.EVENTS_COLUMNS and "known_at_source" in main05.EVENTS_COLUMNS

    def test_fetch_event_row_writes_known_at(self, monkeypatch):
        monkeypatch.setattr(main05, "fred_latest", lambda sid: (0.45, date(2026, 10, 2)))
        row = main05.fetch_event_row("Yield Curve 10Y-2Y", date(2026, 10, 2), {}, pd.DataFrame(columns=main05.SCHEDULE_COLUMNS), _events([]))
        assert row["known_at_source"] == "written"
        assert row["known_at"].endswith("Z") and len(row["known_at"]) == 20

    def test_rewriting_same_event_keeps_first_known_at(self):
        """日次の指標で最新の観測が変わらない日に同じevent_idを書き直しても、最初のknown_atを残す。"""
        old = _event("Yield Curve 10Y-2Y", "2026-10-01", 0.46, "2026-10-03 01:14:00")
        old["known_at"], old["known_at_source"] = "2026-10-02T21:04:00Z", "alfred"
        new = dict(old, updated_at="2026-10-04 00:41:00", known_at="2026-10-04T00:41:00Z", known_at_source="written")
        kept = main05._keep_first_known_at([new], _events([old]))
        assert kept[0]["known_at"] == "2026-10-02T21:04:00Z" and kept[0]["known_at_source"] == "alfred"

    def test_compute_score_uses_known_at_not_updated_at(self):
        """取り込み分の行（updated_at=2026-03-28）でも、known_at（当時の公表時刻）が計算日以前なら使う。"""
        r = _event("Philadelphia Fed Manufacturing", "2019-10-01", 5.6, "2026-03-28 19:30:24")
        r["known_at"], r["known_at_source"] = "2019-10-17T12:30:00Z", "estimated"
        res = main05._compute_current_score(_events([r]), date(2019, 10, 31))
        assert res["indicators"]["philly"]["value"] == 5.6


# ─────────────────────────────────────────────────────────────────
#  M-3 STEP 2: 改定値（revised_actual・revised_at）[[MACRO-PULSE-REVISION-NOT-APPLIED-1]]
# ─────────────────────────────────────────────────────────────────
class TestRevisions:
    def test_events_columns_have_revised(self):
        assert "revised_actual" in main05.EVENTS_COLUMNS and "revised_at" in main05.EVENTS_COLUMNS

    def test_apply_revisions_keeps_actual_and_writes_revised(self, monkeypatch):
        store = {"PERMIT": {"2026-07-01": 1400.0, "2026-08-01": 1394.0}}
        monkeypatch.setattr(main05, "_store_values", lambda fid: store.get(fid, {}))
        ev = _events([_event("Building Permits", "2026-07-01", 1362.0), _event("Building Permits", "2026-08-01", 1394.0)])
        out = main05.apply_revisions(ev)
        r = out.set_index("release_date")
        assert r.at["2026-07-01", "actual"] == "1362.0"
        assert float(r.at["2026-07-01", "revised_actual"]) == 1400.0 and r.at["2026-07-01", "revised_at"].endswith("Z")
        assert r.at["2026-08-01", "revised_actual"] == ""

    def test_apply_revisions_nfp_uses_month_over_month(self, monkeypatch):
        store = {"PAYEMS": {"2026-07-01": 159000.0, "2026-08-01": 159015.0}}
        monkeypatch.setattr(main05, "_store_values", lambda fid: store.get(fid, {}))
        out = main05.apply_revisions(_events([_event("NFP", "2026-08-01", 22000)]))
        assert float(out.iloc[0]["revised_actual"]) == 15000

    def test_compute_score_uses_revised_only_after_revised_at(self):
        r = _event("Philadelphia Fed Manufacturing", "2026-08-01", 5.0, "2026-08-21 22:00:00")
        r["revised_actual"], r["revised_at"] = "-20.0", "2026-09-20T12:30:00Z"
        ev = _events([r])
        before = main05._compute_current_score(ev, date(2026, 9, 1))
        after = main05._compute_current_score(ev, date(2026, 9, 25))
        assert before["indicators"]["philly"]["value"] == 5.0
        assert after["indicators"]["philly"]["value"] == -20.0


# ─────────────────────────────────────────────────────────────────
#  M-3 STEP 3: 使える指標が無いときは判定不能（50にしない）
# ─────────────────────────────────────────────────────────────────
class TestScoreNoData:
    def test_no_usable_rows_gives_none(self):
        res = main05._compute_current_score(_events([]), date(2026, 10, 3))
        assert res["score"] is None and res["phase"] == "判定不能"

    def test_rows_only_known_after_target_give_none(self):
        r = _event("Philadelphia Fed Manufacturing", "2019-10-01", 5.6, "2026-03-28 19:30:24")
        res = main05._compute_current_score(_events([r]), date(2019, 10, 31))
        assert res["score"] is None

    def test_weekly_analysis_skips_when_none(self, monkeypatch):
        r = _event("Philadelphia Fed Manufacturing", "2019-10-01", 5.6, "2026-03-28 19:30:24")
        monkeypatch.setattr(main05, "load_events", lambda: _events([r]))
        called = []
        monkeypatch.setattr(main05, "load_weekly_analysis", lambda: called.append(1) or pd.DataFrame())
        main05.run_weekly_analysis(date(2019, 10, 31))
        assert called == []


# ─────────────────────────────────────────────────────────────────
#  M-3 STEP 5: sp500_t0の観測日（sp500_t0_asof）[[MACRO-PULSE-TICKER-SP500-NO-ASOF-1]]
# ─────────────────────────────────────────────────────────────────
class TestSp500Asof:
    def test_events_columns_have_sp500_t0_asof(self):
        assert "sp500_t0_asof" in main05.EVENTS_COLUMNS

    def test_get_sp500_with_asof_returns_fred_obs_date(self, monkeypatch):
        monkeypatch.setattr(main05, "fred_latest", lambda sid: (7722.72, date(2026, 10, 2)))
        assert main05.get_sp500_with_asof(date(2026, 10, 3)) == (7722.72, "2026-10-02")
        assert main05.get_sp500(date(2026, 10, 3)) == 7722.72

    def test_get_sp500_with_asof_fallback_has_no_date(self, monkeypatch):
        monkeypatch.setattr(main05, "fred_latest", lambda sid: (None, None))
        monkeypatch.setattr(main05, "_stooq", lambda sym, d: 7700.0)
        assert main05.get_sp500_with_asof(date(2026, 10, 3)) == (7700.0, "")

    def test_lookup_sp500_with_asof_returns_close_date(self):
        cache = pd.Series([7316.15, 7325.0], index=pd.to_datetime(["2026-07-29", "2026-07-30"]))
        assert main05._lookup_sp500_with_asof(cache, date(2026, 8, 1)) == (7325.0, "2026-07-30")
        assert main05._lookup_sp500(cache, date(2026, 7, 29)) == 7316.15


class TestSp500AsofRepairWindow:
    """M-3 STEP 5追加: 修復スクリプトは、行の日付の0〜7日前の終値とだけ一致させる（何年も前の同じ値の終値とは一致させない）。"""

    def _load(self):
        p = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "analysis" / "macro_events_sp500_asof_repair.py"
        spec = importlib.util.spec_from_file_location("sp500_asof_repair", p)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_does_not_match_close_from_years_ago(self, tmp_path, monkeypatch):
        import json
        import sys
        mod = self._load()
        sp = tmp_path / "SP500.json"
        sp.write_text(json.dumps([{"as_of": "2019-06-19", "value": 2926.46},
                                  {"as_of": "2026-09-29", "value": 6600.12}]), encoding="utf-8")
        ev = tmp_path / "05_events.csv"
        pd.DataFrame([
            {"event_id": "a", "indicator": "VIX", "release_date": "2026-03-10", "sp500_t0": "2926.46",
             "updated_at": "2026-03-28 19:30:24"},
            {"event_id": "b", "indicator": "VIX", "release_date": "2026-09-30", "sp500_t0": "6600.12",
             "updated_at": "2026-09-30 23:00:00"},
        ]).to_csv(ev, index=False)
        monkeypatch.setattr(mod, "SP500", str(sp))
        monkeypatch.setattr(mod, "EVENTS", str(ev))
        monkeypatch.setattr(sys, "argv", ["x", "--apply"])
        assert mod.main() == 0
        out = pd.read_csv(ev, dtype=str).fillna("").set_index("event_id")
        assert out.at["a", "sp500_t0_asof"] == ""
        assert out.at["b", "sp500_t0_asof"] == "2026-09-29"
