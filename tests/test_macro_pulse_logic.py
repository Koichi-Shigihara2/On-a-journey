"""
tests/test_macro_pulse_logic.py

MACRO PULSE (src/market/macro_pulse/05_main.py, 05_audit.py) のユニットテスト。
MACRO-NFP-1:
  ① NFP「水準」→「前月比」変換ロジック
  ② 同一FRED観測値の重複書き込み防止ロジック
  ③ 軽量整合性チェックスクリプト
を検証する。

実行方法:
    python -m pytest tests/test_macro_pulse_logic.py -v
"""

import importlib.util
import os
import pathlib
from datetime import date

import pandas as pd
import pytest

_MACRO_DIR = pathlib.Path(__file__).resolve().parent.parent / "src" / "market" / "macro_pulse"


def _load_module(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, _MACRO_DIR / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


main05 = _load_module("main05_test", "05_main.py")
audit05 = _load_module("audit05_test", "05_audit.py")


# ─────────────────────────────────────────────────────────────────
#  ① fred_latest_with_prev / fred_latest
#  MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替（2026-08-12）:
#  common.macro_data.reader経由に切替たため、fredapiの偽クライアントでは
#  なくmain05._md_reader.get_series()/get_latest()をmonkeypatchする。
# ─────────────────────────────────────────────────────────────────
def _records(pairs):
    """[(date_str, value), ...] からcommon.macro_data.reader.get_series()
    が返すレコード形式（観測日昇順のdictリスト）を作る。"""
    return [
        {"value": v, "as_of": d, "fetched_at": "2026-01-01T00:00:00+09:00",
         "source": "FRED", "source_detail": "series=TEST"}
        for d, v in pairs
    ]


class TestFredLatestWithPrev:
    def test_returns_latest_and_prev(self, monkeypatch):
        records = _records([("2026-04-01", 158736.0), ("2026-05-01", 159001.0)])
        monkeypatch.setattr(main05._md_reader, "get_series", lambda series_id, **kw: records)
        val_now, d_now, val_prev, d_prev = main05.fred_latest_with_prev("PAYEMS")
        assert val_now == 159001.0
        assert val_prev == 158736.0
        assert d_now == date(2026, 5, 1)
        assert d_prev == date(2026, 4, 1)

    def test_single_observation_returns_none_prev(self, monkeypatch):
        records = _records([("2026-05-01", 159001.0)])
        monkeypatch.setattr(main05._md_reader, "get_series", lambda series_id, **kw: records)
        val_now, d_now, val_prev, d_prev = main05.fred_latest_with_prev("PAYEMS")
        assert val_now == 159001.0
        assert val_prev is None
        assert d_prev is None

    def test_empty_series_returns_all_none(self, monkeypatch):
        monkeypatch.setattr(main05._md_reader, "get_series", lambda series_id, **kw: [])
        result = main05.fred_latest_with_prev("PAYEMS")
        assert result == (None, None, None, None)


# ─────────────────────────────────────────────────────────────────
#  ① fetch_event_row: NFPは前月比(人)に変換して格納する
# ─────────────────────────────────────────────────────────────────
class TestFetchEventRowNFPDiff:
    def test_nfp_actual_is_diff_times_1000(self, monkeypatch):
        records = _records([("2026-04-01", 158736.0), ("2026-05-01", 159001.0)])
        monkeypatch.setattr(main05._md_reader, "get_series", lambda series_id, **kw: records)
        row = main05.fetch_event_row(
            "NFP", date(2026, 5, 20),
            fin_ctx={}, schedule=pd.DataFrame(columns=main05.SCHEDULE_COLUMNS),
            events=pd.DataFrame(columns=main05.EVENTS_COLUMNS),
        )
        # (159001.0 - 158736.0) * 1000 = 265000
        assert row["actual"] == "265000"
        assert row["release_date"] == "2026-05-01"

    def test_nfp_actual_can_be_negative(self, monkeypatch):
        records = _records([("2026-04-01", 159001.0), ("2026-05-01", 158984.0)])
        monkeypatch.setattr(main05._md_reader, "get_series", lambda series_id, **kw: records)
        row = main05.fetch_event_row(
            "NFP", date(2026, 5, 20),
            fin_ctx={}, schedule=pd.DataFrame(columns=main05.SCHEDULE_COLUMNS),
            events=pd.DataFrame(columns=main05.EVENTS_COLUMNS),
        )
        assert row["actual"] == "-17000"

    def test_nfp_no_prev_leaves_actual_empty(self, monkeypatch):
        records = _records([("2026-05-01", 159001.0)])
        monkeypatch.setattr(main05._md_reader, "get_series", lambda series_id, **kw: records)
        row = main05.fetch_event_row(
            "NFP", date(2026, 5, 20),
            fin_ctx={}, schedule=pd.DataFrame(columns=main05.SCHEDULE_COLUMNS),
            events=pd.DataFrame(columns=main05.EVENTS_COLUMNS),
        )
        assert row["actual"] == ""

    def test_non_nfp_indicator_still_uses_raw_level(self, monkeypatch):
        # 非NFPはfred_latest()＝reader.get_latest()経由（末尾の1件のみ使用）
        monkeypatch.setattr(
            main05._md_reader, "get_latest",
            lambda series_id, **kw: {"value": 1420.0, "as_of": "2026-05-01"},
        )
        row = main05.fetch_event_row(
            "Building Permits", date(2026, 5, 20),
            fin_ctx={}, schedule=pd.DataFrame(columns=main05.SCHEDULE_COLUMNS),
            events=pd.DataFrame(columns=main05.EVENTS_COLUMNS),
        )
        assert row["actual"] == "1420.0"


# ─────────────────────────────────────────────────────────────────
#  ② dedupe_new_rows: 同一FRED観測値の重複書き込み防止
# ─────────────────────────────────────────────────────────────────
class TestDedupeNewRows:
    def _row(self, indicator, release_date, actual, event_id=None):
        r = {c: "" for c in main05.EVENTS_COLUMNS}
        r.update({
            "indicator": indicator,
            "release_date": release_date,
            "actual": actual,
            "event_id": event_id or f"{indicator}_{release_date}",
        })
        return r

    def _schedule(self, *indicators):
        """指定した指標が05_indicator_schedule.csvに登録済み（＝scheduled
        ループの対象になりうる）状態を模したscheduleDataFrameを返す。
        _duplicate_risk_indicators()はscheduleとの積集合で判定するため、
        重複判定を有効化したい指標のみ渡す。"""
        if not indicators:
            return pd.DataFrame(columns=main05.SCHEDULE_COLUMNS)
        rows = [
            {**{c: "" for c in main05.SCHEDULE_COLUMNS}, "indicator": ind, "release_date": "2026-01-01"}
            for ind in indicators
        ]
        return pd.DataFrame(rows)

    def test_duplicate_within_lag_window_is_dropped(self):
        rows = [
            self._row("NFP", "2026-06-01", "158984.0"),
            self._row("NFP", "2026-07-02", "158984.0"),  # 同一観測値・lag(35日)以内
        ]
        kept = main05.dedupe_new_rows(rows, pd.DataFrame(columns=main05.EVENTS_COLUMNS), self._schedule("NFP"))
        assert len(kept) == 1
        assert kept[0]["event_id"] == "NFP_2026-06-01"

    def test_different_values_are_both_kept(self):
        rows = [
            self._row("NFP", "2026-06-01", "158984.0"),
            self._row("NFP", "2026-07-02", "159200.0"),
        ]
        kept = main05.dedupe_new_rows(rows, pd.DataFrame(columns=main05.EVENTS_COLUMNS), self._schedule("NFP"))
        assert len(kept) == 2

    def test_dedup_against_existing_events(self):
        existing = pd.DataFrame([{
            **{c: "" for c in main05.EVENTS_COLUMNS},
            "indicator": "NFP", "release_date": "2026-06-01",
            "actual": "158984.0", "event_id": "nfp_2026-06-01",
        }])
        rows = [self._row("NFP", "2026-07-02", "158984.0")]
        kept = main05.dedupe_new_rows(rows, existing, self._schedule("NFP"))
        assert kept == []

    def test_outside_lag_window_not_treated_as_duplicate(self):
        rows = [
            self._row("Initial Claims 4W MA", "2026-01-01", "210000.0"),
            self._row("Initial Claims 4W MA", "2026-06-01", "210000.0"),  # lag=7日を大きく超える
        ]
        kept = main05.dedupe_new_rows(
            rows, pd.DataFrame(columns=main05.EVENTS_COLUMNS), self._schedule("Initial Claims 4W MA")
        )
        assert len(kept) == 2

    def test_regression_building_permits_duplicate_across_runs(self):
        """
        実際に発生した重複(permit_2026-03-01 と permit_2026-03-17、
        共に1363.0、lag=47日以内)を再現し、2回目の書き込みが弾かれることを確認する。
        """
        existing = pd.DataFrame([{
            **{c: "" for c in main05.EVENTS_COLUMNS},
            "indicator": "Building Permits", "release_date": "2026-03-01",
            "actual": "1363.0", "event_id": "permit_2026-03-01",
        }])
        new_row = self._row("Building Permits", "2026-03-17", "1363.0", event_id="permit_2026-03-17")
        kept = main05.dedupe_new_rows([new_row], existing, self._schedule("Building Permits"))
        assert kept == []

    def test_regression_michigan_sentiment_duplicate_across_runs(self):
        """
        実際に発生した重複(mich_sent_2026-03-01 と mich_sent_2026-03-13、
        共に53.3、lag=10日以内)を再現し、2回目の書き込みが弾かれることを確認する。
        """
        existing = pd.DataFrame([{
            **{c: "" for c in main05.EVENTS_COLUMNS},
            "indicator": "Michigan Consumer Sentiment", "release_date": "2026-03-01",
            "actual": "53.3", "event_id": "mich_sent_2026-03-01",
        }])
        new_row = self._row("Michigan Consumer Sentiment", "2026-03-13", "53.3", event_id="mich_sent_2026-03-13")
        kept = main05.dedupe_new_rows([new_row], existing, self._schedule("Michigan Consumer Sentiment"))
        assert kept == []

    def test_sahm_rule_legitimate_repeat_value_is_kept(self):
        """
        [[MACRO-THRESHOLD-INCONSISTENCY-1]]回帰テスト。Sahm Ruleは狭い値域の
        低カーディナリティ指標のため、異なる観測月でも値が完全一致しうる
        （実データ確認: 930ヶ月中155件、約17%が直前月と完全一致）。
        修正前はこれをvalue等価性で誤って重複と判定し、正当な新規データを
        捨てていた（実データ再現済み）。Sahm Ruleは05_indicator_schedule.csv
        に登録されない（実測: 該当行0件）ため、本テストでもscheduleに
        含めず _duplicate_risk_indicators() の対象外とする。
        """
        existing = pd.DataFrame([{
            **{c: "" for c in main05.EVENTS_COLUMNS},
            "indicator": "Sahm Rule Recession Indicator", "release_date": "2026-08-01",
            "actual": "-0.07", "event_id": "sahm_rule_2026-08-01",
        }])
        new_row = self._row("Sahm Rule Recession Indicator", "2026-09-01", "-0.07",
                             event_id="sahm_rule_2026-09-01")
        kept = main05.dedupe_new_rows([new_row], existing, self._schedule())
        assert len(kept) == 1
        assert kept[0]["event_id"] == "sahm_rule_2026-09-01"

    def test_cfnai_legitimate_repeat_value_is_kept(self):
        """[[MACRO-THRESHOLD-INCONSISTENCY-1]]回帰テスト。CFNAI MA3も同様に
        低カーディナリティ指標のため、修正前は正当な反復値が誤って
        重複除外されていた（実データ確認: 367ヶ月中1件が該当）。CFNAIも
        schedule未登録（実測: 該当行0件）のため対象外とする。"""
        existing = pd.DataFrame([{
            **{c: "" for c in main05.EVENTS_COLUMNS},
            "indicator": "Chicago Fed National Activity", "release_date": "2026-07-01",
            "actual": "-0.08", "event_id": "cfnai_ma3_2026-07-01",
        }])
        new_row = self._row("Chicago Fed National Activity", "2026-08-01", "-0.08",
                             event_id="cfnai_ma3_2026-08-01")
        kept = main05.dedupe_new_rows([new_row], existing, self._schedule())
        assert len(kept) == 1
        assert kept[0]["event_id"] == "cfnai_ma3_2026-08-01"

    def test_nfp_true_duplicate_still_caught_after_exemption_added(self):
        """[[MACRO-THRESHOLD-INCONSISTENCY-1]]非退行確認: schedule登録済みの
        NFP（MACRO-NFP-1の元々の対象）では、従来通りvalue等価性による
        重複判定が機能し続けることを確認する。"""
        rows = [
            self._row("NFP", "2026-06-01", "158984.0"),
            self._row("NFP", "2026-07-02", "158984.0"),
        ]
        kept = main05.dedupe_new_rows(rows, pd.DataFrame(columns=main05.EVENTS_COLUMNS), self._schedule("NFP"))
        assert len(kept) == 1
        assert kept[0]["event_id"] == "NFP_2026-06-01"

    def test_sahm_rule_would_still_dedupe_if_ever_schedule_registered(self):
        """_duplicate_risk_indicators()は動的判定のため、将来Sahm Ruleが
        05_indicator_schedule.csvに登録される事態になれば、value等価性
        チェックも自動的に有効化されることを確認する（静的な例外リストで
        はなく動的判定を採用した設計意図の裏付け）。"""
        existing = pd.DataFrame([{
            **{c: "" for c in main05.EVENTS_COLUMNS},
            "indicator": "Sahm Rule Recession Indicator", "release_date": "2026-08-01",
            "actual": "-0.07", "event_id": "sahm_rule_2026-08-01",
        }])
        new_row = self._row("Sahm Rule Recession Indicator", "2026-09-01", "-0.07",
                             event_id="sahm_rule_2026-09-01")
        kept = main05.dedupe_new_rows([new_row], existing, self._schedule("Sahm Rule Recession Indicator"))
        assert kept == []


# ─────────────────────────────────────────────────────────────────
#  ③ 05_audit.py: 軽量整合性チェック
# ─────────────────────────────────────────────────────────────────
class TestAuditNFPLevelResidue:
    def _events_df(self, nfp_vals_by_date):
        rows = []
        for d, v in nfp_vals_by_date:
            row = {c: "" for c in main05.EVENTS_COLUMNS}
            row.update({"indicator": "NFP", "release_date": d, "actual": str(v),
                        "event_id": f"nfp_{d}"})
            rows.append(row)
        return pd.DataFrame(rows)

    def test_flags_narrow_band_level_residue(self):
        events = self._events_df([
            ("2026-02-01", 158637.0), ("2026-03-01", 158736.0),
            ("2026-04-01", 159001.0), ("2026-05-01", 159001.0),
            ("2026-06-01", 158984.0), ("2026-07-01", 158984.0),
        ])
        ng, warn = audit05.check_nfp_level_residue(events)
        assert len(ng) == 1

    def test_does_not_flag_genuine_month_over_month_diffs(self):
        # 前月比なら大きく振れる（数万〜十数万・符号も反転しうる）のが正常
        events = self._events_df([
            ("2026-02-01", 180000.0), ("2026-03-01", 210000.0),
            ("2026-04-01", -20000.0), ("2026-05-01", 265000.0),
            ("2026-06-01", 150000.0), ("2026-07-01", 300000.0),
        ])
        ng, warn = audit05.check_nfp_level_residue(events)
        assert ng == []

    def test_insufficient_history_no_flag(self):
        events = self._events_df([("2026-06-01", 158984.0), ("2026-07-01", 158984.0)])
        ng, warn = audit05.check_nfp_level_residue(events)
        assert ng == []


class TestAuditDuplicateEvents:
    def test_flags_duplicate_within_risk_indicators_only(self):
        events = pd.DataFrame([
            {**{c: "" for c in main05.EVENTS_COLUMNS},
             "indicator": "NFP", "release_date": "2026-06-01", "actual": "158984.0",
             "event_id": "nfp_2026-06-01"},
            {**{c: "" for c in main05.EVENTS_COLUMNS},
             "indicator": "NFP", "release_date": "2026-07-02", "actual": "158984.0",
             "event_id": "nfp_2026-07-02"},
        ])
        schedule = pd.DataFrame([{"indicator": "NFP", "release_date": "2026-07-02"}])
        ng, warn = audit05.check_duplicate_events(events, schedule)
        assert ng == []
        assert len(warn) == 1

    def test_non_risk_indicator_not_flagged(self):
        # Sahm Ruleはscheduleに登録されていないため対象外（正常な横ばいをNG化しない）
        events = pd.DataFrame([
            {**{c: "" for c in main05.EVENTS_COLUMNS},
             "indicator": "Sahm Rule Recession Indicator", "release_date": "2026-01-01",
             "actual": "-0.1", "event_id": "sahm_rule_2026-01-01"},
            {**{c: "" for c in main05.EVENTS_COLUMNS},
             "indicator": "Sahm Rule Recession Indicator", "release_date": "2026-02-01",
             "actual": "-0.1", "event_id": "sahm_rule_2026-02-01"},
        ])
        schedule = pd.DataFrame(columns=["indicator", "release_date"])
        ng, warn = audit05.check_duplicate_events(events, schedule)
        assert ng == [] and warn == []


class TestUpdateLiquidityCsvSp500:
    """[[HOLLOW-RALLY-DEAD-1]]対応: update_liquidity_csv()がsp500_val引数を
    LIQUIDITY_COLUMNSの"sp500"列へ正しく格納し、他列には影響しないことを検証する。

    テストは事前に前日分の1行を種としてCSVへ書き込んでから対象日を実行する
    （実運用のCSVは常に履歴を持つため、この状態が実際の挙動を反映する。
    完全に空のCSVへ直接update_liquidity_csv()を呼ぶと、prev_rows.emptyが
    真になりprev_rrp等が未定義のままステルスシグナル計算部で参照される
    既存のUnboundLocalError〈本タスクのスコープ外、[[LIQUIDITY-CSV-FIRST-
    ROW-UNBOUNDLOCALERROR-1]]として別途報告〉に当たるため、この回避は
    意図的）。
    """

    def _mock_fred_series(self, monkeypatch, values):
        """series_id -> valueのdictを渡し、_md_reader.get_latest()をmockする。
        未指定のseries_idはNoneを返す（値なし扱い）。"""
        def _get_latest(series_id):
            if series_id in values:
                return {"value": values[series_id], "as_of": "2026-01-01"}
            return None
        monkeypatch.setattr(main05._md_reader, "get_latest", _get_latest)

    def _seed_prior_row(self, liq_path):
        seed = {c: "" for c in main05.LIQUIDITY_COLUMNS}
        seed.update({
            "date": "2026-01-01", "m2": "22900.0", "hy_spread": "2.6",
            "fed_balance": "6690000.0", "tga": "890000.0", "rrp": "280.0",
            "net_liquidity": "5.82", "reserve_balance": "2880000.0",
            "stealth_signal": "neutral", "stealth_absorb_weeks": "0",
            "net_liq_decline_weeks": "0", "stealth_alert": "", "sp500": "6000.0",
        })
        pd.DataFrame([seed], columns=main05.LIQUIDITY_COLUMNS).to_csv(liq_path, index=False)

    def test_sp500_value_is_stored(self, tmp_path, monkeypatch):
        liq_path = tmp_path / "05_liquidity.csv"
        self._seed_prior_row(liq_path)
        monkeypatch.setattr(main05, "LIQUIDITY_PATH", str(liq_path))
        monkeypatch.setattr(main05, "BASE_DATA_DIR", str(tmp_path))  # 05_meta.json書き込み隔離
        self._mock_fred_series(monkeypatch, {
            "M2SL": 23000.0, "BAMLH0A0HYM2": 2.7, "WALCL": 6700000.0,
            "WTREGEN": 900000.0, "RRPONTSYD": 0.3, "WRBWFRBL": 2900000.0,
        })
        main05.update_liquidity_csv(date(2026, 1, 2), sp500_val=6234.56)

        df = pd.read_csv(liq_path, dtype=str)
        assert "sp500" in df.columns
        row = df[df["date"] == "2026-01-02"].iloc[0]
        assert row["sp500"] == "6234.56"
        # 他列（m2等）が影響を受けていないことも確認
        assert row["m2"] == "23000.0"
        # 種として入れた前日行が変更されていないことも確認
        prior = df[df["date"] == "2026-01-01"].iloc[0]
        assert prior["sp500"] == "6000.0"

    def test_sp500_none_leaves_column_blank(self, tmp_path, monkeypatch):
        liq_path = tmp_path / "05_liquidity.csv"
        self._seed_prior_row(liq_path)
        monkeypatch.setattr(main05, "LIQUIDITY_PATH", str(liq_path))
        monkeypatch.setattr(main05, "BASE_DATA_DIR", str(tmp_path))  # 05_meta.json書き込み隔離
        self._mock_fred_series(monkeypatch, {
            "M2SL": 23000.0, "BAMLH0A0HYM2": 2.7, "WALCL": 6700000.0,
            "WTREGEN": 900000.0, "RRPONTSYD": 0.3, "WRBWFRBL": 2900000.0,
        })
        main05.update_liquidity_csv(date(2026, 1, 2), sp500_val=None)

        df = pd.read_csv(liq_path, dtype=str).fillna("")
        row = df[df["date"] == "2026-01-02"].iloc[0]
        assert row["sp500"] == ""

    def test_rerun_same_date_updates_sp500(self, tmp_path, monkeypatch):
        """既存日付への再実行時もsp500列がupdate_colsに含まれ更新されること
        （[[HOLLOW-RALLY-DEAD-1]]対応で追加、既存日付の上書きロジックの回帰確認）。
        """
        liq_path = tmp_path / "05_liquidity.csv"
        self._seed_prior_row(liq_path)
        monkeypatch.setattr(main05, "LIQUIDITY_PATH", str(liq_path))
        monkeypatch.setattr(main05, "BASE_DATA_DIR", str(tmp_path))  # 05_meta.json書き込み隔離
        self._mock_fred_series(monkeypatch, {
            "M2SL": 23000.0, "BAMLH0A0HYM2": 2.7, "WALCL": 6700000.0,
            "WTREGEN": 900000.0, "RRPONTSYD": 0.3, "WRBWFRBL": 2900000.0,
        })
        main05.update_liquidity_csv(date(2026, 1, 2), sp500_val=6000.0)
        main05.update_liquidity_csv(date(2026, 1, 2), sp500_val=6100.0)

        df = pd.read_csv(liq_path, dtype=str)
        assert len(df) == 2  # 種の前日行＋対象日1行（対象日は追記ではなく上書き）
        assert df[df["date"] == "2026-01-02"].iloc[0]["sp500"] == "6100.0"

    def test_empty_csv_first_run_does_not_raise(self, tmp_path, monkeypatch):
        """[[LIQUIDITY-CSV-FIRST-ROW-UNBOUNDLOCALERROR-1]]の回帰テスト。
        05_liquidity.csvが存在しない状態（完全な初回実行）でprev_rowsが
        空になり、prev_rrp/prev_tga/prev_rsvが未定義のままステルス吸収額
        vs FED供給額比較ブロックで参照されUnboundLocalErrorになっていた。
        """
        liq_path = tmp_path / "05_liquidity.csv"
        assert not liq_path.exists()
        monkeypatch.setattr(main05, "LIQUIDITY_PATH", str(liq_path))
        monkeypatch.setattr(main05, "BASE_DATA_DIR", str(tmp_path))  # 05_meta.json書き込み隔離
        self._mock_fred_series(monkeypatch, {
            "M2SL": 23000.0, "BAMLH0A0HYM2": 2.7, "WALCL": 6700000.0,
            "WTREGEN": 900000.0, "RRPONTSYD": 0.3, "WRBWFRBL": 2900000.0,
        })
        main05.update_liquidity_csv(date(2026, 1, 2), sp500_val=6234.56)

        df = pd.read_csv(liq_path, dtype=str)
        assert len(df) == 1
        assert df.iloc[0]["stealth_signal"] == "neutral"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
