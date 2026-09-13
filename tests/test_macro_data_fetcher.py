"""
tests/test_macro_data_fetcher.py

BACKLOG [[MACRODATA-LAYER-CONSTRUCTION-1]] common/macro_data/fetcher.py の
単体テスト。fetch_series()のリトライ・update_series()のマージ（upsert）
ロジック・保存前検証・macro_data_violations_log.json書き込みを検証する。
ネットワークアクセス（fredapi呼び出し）は行わない
（common/market_data/fetcher.pyのテストパターンを踏襲）。

実行方法:
    python -m pytest tests/test_macro_data_fetcher.py -v
"""

import json
import os

import pandas as pd
import pytest

from common.macro_data import fetcher


class _FakeFred:
    """呼び出し回数・引数を記録する偽fredapiクライアント。"""

    def __init__(self, series=None, fail_times=0, exc=RuntimeError("boom")):
        self.series = series if series is not None else pd.Series(dtype=float)
        self.fail_times = fail_times
        self.exc = exc
        self.calls = 0
        self.call_kwargs = []

    def get_series(self, series_id, **kwargs):
        self.calls += 1
        self.call_kwargs.append(kwargs)
        if self.calls <= self.fail_times:
            raise self.exc
        return self.series


def _series(pairs):
    """[(date_str, value), ...] からpandas.Seriesを作る。"""
    dates = pd.to_datetime([d for d, _ in pairs])
    values = [v for _, v in pairs]
    return pd.Series(values, index=dates)


class TestFetchSeries:
    def test_missing_api_key_returns_none_without_retry(self, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        monkeypatch.setattr(fetcher, "_FRED_CLIENT", None)
        result = fetcher.fetch_series("BAMLH0A0HYM2")
        assert result is None

    def test_succeeds_on_first_attempt(self, monkeypatch):
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        result = fetcher.fetch_series("T10Y2Y")
        assert result is not None
        assert fake.calls == 1

    def test_retries_on_failure_then_succeeds(self, monkeypatch):
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]), fail_times=2)
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        monkeypatch.setattr(fetcher.time, "sleep", lambda s: None)
        result = fetcher.fetch_series("T10Y2Y")
        assert result is not None
        assert fake.calls == 3

    def test_all_three_attempts_fail_returns_none(self, monkeypatch):
        fake = _FakeFred(fail_times=99)
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        monkeypatch.setattr(fetcher.time, "sleep", lambda s: None)
        result = fetcher.fetch_series("T10Y2Y")
        assert result is None
        assert fake.calls == 3

    def test_start_param_passed_as_observation_start(self, monkeypatch):
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.fetch_series("T10Y2Y", start="2020-01-01")
        assert fake.call_kwargs[0] == {"observation_start": "2020-01-01"}

    def test_no_start_param_omits_observation_start(self, monkeypatch):
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.fetch_series("T10Y2Y")
        assert fake.call_kwargs[0] == {}


class TestUpdateSeriesUpsert:
    def test_first_call_creates_series_file(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 4.1), ("2026-02-01", 4.2)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)

        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert result["updated"] == 2
        assert result["warnings"] == []

        path = os.path.join(base, "series", "TESTSERIES.json")
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        assert payload["series_id"] == "TESTSERIES"
        assert [r["as_of"] for r in payload["records"]] == ["2026-01-01", "2026-02-01"]

    def test_second_call_upserts_overlapping_date_and_appends_new_date(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 4.1), ("2026-02-01", 4.2)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("TESTSERIES", base_dir=base)

        fake.series = _series([("2026-02-01", 4.25), ("2026-03-01", 4.3)])
        fetcher.update_series("TESTSERIES", base_dir=base)

        path = os.path.join(base, "series", "TESTSERIES.json")
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        records = {r["as_of"]: r["value"] for r in payload["records"]}
        assert records == {"2026-01-01": 4.1, "2026-02-01": 4.25, "2026-03-01": 4.3}
        assert [r["as_of"] for r in payload["records"]] == sorted(records.keys())

    def test_each_record_has_provenance_fields(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("TESTSERIES", base_dir=base)

        path = os.path.join(base, "series", "TESTSERIES.json")
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        record = payload["records"][0]
        assert record["source"] == "FRED"
        assert record["source_detail"] == "series=TESTSERIES"
        assert record["as_of"] == "2026-01-01"
        assert "fetched_at" in record
        assert "+09:00" in record["fetched_at"]  # JST

    def test_nan_values_are_dropped(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        s = _series([("2026-01-01", 1.0), ("2026-02-01", 2.0)])
        s.iloc[1] = float("nan")
        fake = _FakeFred(series=s)
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert result["updated"] == 1

    def test_fetch_failure_returns_zero_updated_and_writes_empty_violations(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(fail_times=99)
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        monkeypatch.setattr(fetcher.time, "sleep", lambda s: None)

        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert result == {"series_id": "TESTSERIES", "updated": 0, "warnings": []}

        log_path = os.path.join(base, "macro_data_violations_log.json")
        with open(log_path, encoding="utf-8") as f:
            log = json.load(f)
        assert log["TESTSERIES"]["warnings"] == []


class TestUpdateSeriesValidation:
    def test_duplicate_as_of_in_batch_is_flagged(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0), ("2026-01-01", 2.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert any("duplicate as_of" in w for w in result["warnings"])

    def test_tenfold_jump_up_is_flagged(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("TESTSERIES", base_dir=base)

        fake.series = _series([("2026-02-01", 10.0)])
        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert any("order-of-magnitude jump" in w for w in result["warnings"])

    def test_tenfold_jump_down_is_flagged(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 100.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("TESTSERIES", base_dir=base)

        fake.series = _series([("2026-02-01", 10.0)])
        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert any("order-of-magnitude jump" in w for w in result["warnings"])

    def test_normal_change_is_not_flagged(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 4.10)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("TESTSERIES", base_dir=base)

        fake.series = _series([("2026-02-01", 4.25)])
        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert result["warnings"] == []

    def test_jump_check_uses_prior_stored_value_when_batch_has_single_new_point(self, tmp_path, monkeypatch):
        """バッチが1件のみの場合でも、既存ストアの直近値と比較して検証すること"""
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 5.0), ("2026-02-01", 5.1)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("TESTSERIES", base_dir=base)

        fake.series = _series([("2026-03-01", 51.0)])
        result = fetcher.update_series("TESTSERIES", base_dir=base)
        assert any("order-of-magnitude jump" in w for w in result["warnings"])


class TestViolationsLog:
    def test_written_every_run_even_with_zero_warnings(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("TESTSERIES", base_dir=base)

        log_path = os.path.join(base, "macro_data_violations_log.json")
        assert os.path.exists(log_path)
        with open(log_path, encoding="utf-8") as f:
            log = json.load(f)
        assert log["TESTSERIES"]["warnings"] == []
        assert "checked_at" in log["TESTSERIES"]

    def test_multiple_series_each_get_own_section_without_clobbering(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.update_series("SERIES_A", base_dir=base)
        fetcher.update_series("SERIES_B", base_dir=base)

        log_path = os.path.join(base, "macro_data_violations_log.json")
        with open(log_path, encoding="utf-8") as f:
            log = json.load(f)
        assert "SERIES_A" in log
        assert "SERIES_B" in log


class TestStartParamPropagation:
    """[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]: update_series()・
    fetch_all_series()のstart引数がfetch_series()経由でFRED APIの
    observation_startへ正しく伝播することを確認する（日次cronが
    毎回全期間取得してしまう問題への対応）。"""

    def test_update_series_start_reaches_fred_call(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)

        fetcher.update_series("TESTSERIES", start="2025-08-10", base_dir=base)
        assert fake.call_kwargs[0] == {"observation_start": "2025-08-10"}

    def test_update_series_no_start_omits_observation_start(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)

        fetcher.update_series("TESTSERIES", base_dir=base)
        assert fake.call_kwargs[0] == {}

    def test_fetch_all_series_start_reaches_every_series(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        os.makedirs(base, exist_ok=True)
        with open(os.path.join(base, "series_meta.json"), "w", encoding="utf-8") as f:
            json.dump({"SERIES_A": {}, "SERIES_B": {}}, f)

        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)

        fetcher.fetch_all_series(base_dir=base, start="2025-08-10")
        assert fake.calls == 2
        assert all(kw == {"observation_start": "2025-08-10"} for kw in fake.call_kwargs)

    def test_fetch_all_series_no_start_omits_observation_start_for_every_series(
        self, tmp_path, monkeypatch
    ):
        base = str(tmp_path)
        os.makedirs(base, exist_ok=True)
        with open(os.path.join(base, "series_meta.json"), "w", encoding="utf-8") as f:
            json.dump({"SERIES_A": {}, "SERIES_B": {}}, f)

        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)

        fetcher.fetch_all_series(base_dir=base)
        assert fake.calls == 2
        assert all(kw == {} for kw in fake.call_kwargs)

    def test_fetch_all_series_start_does_not_affect_existing_records_overlap(
        self, tmp_path, monkeypatch
    ):
        """既存動作の非破壊確認: startありの呼び出しでも既存series/*.jsonの
        重複日付は従来通りupsert（新しい取得値で上書き）されること。"""
        base = str(tmp_path)
        os.makedirs(base, exist_ok=True)
        with open(os.path.join(base, "series_meta.json"), "w", encoding="utf-8") as f:
            json.dump({"TESTSERIES": {}}, f)

        fake = _FakeFred(series=_series([("2026-01-01", 4.1), ("2026-02-01", 4.2)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        fetcher.fetch_all_series(base_dir=base)

        fake.series = _series([("2026-02-01", 4.25), ("2026-03-01", 4.3)])
        fetcher.fetch_all_series(base_dir=base, start="2026-02-01")

        path = os.path.join(base, "series", "TESTSERIES.json")
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        records = {r["as_of"]: r["value"] for r in payload["records"]}
        assert records == {"2026-01-01": 4.1, "2026-02-01": 4.25, "2026-03-01": 4.3}


class TestFetchAllSeries:
    def test_defaults_to_series_meta_keys(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        os.makedirs(base, exist_ok=True)
        with open(os.path.join(base, "series_meta.json"), "w", encoding="utf-8") as f:
            json.dump({"SERIES_A": {}, "SERIES_B": {}}, f)

        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)

        results = fetcher.fetch_all_series(base_dir=base)
        ids = {r["series_id"] for r in results}
        assert ids == {"SERIES_A", "SERIES_B"}

    def test_explicit_series_ids_overrides_meta(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        os.makedirs(base, exist_ok=True)
        with open(os.path.join(base, "series_meta.json"), "w", encoding="utf-8") as f:
            json.dump({"SERIES_A": {}, "SERIES_B": {}}, f)

        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)

        results = fetcher.fetch_all_series(series_ids=["SERIES_C"], base_dir=base)
        assert [r["series_id"] for r in results] == ["SERIES_C"]

    def test_missing_series_meta_yields_no_targets(self, tmp_path, monkeypatch):
        base = str(tmp_path)
        fake = _FakeFred(series=_series([("2026-01-01", 1.0)]))
        monkeypatch.setattr(fetcher, "_get_fred_client", lambda: fake)
        results = fetcher.fetch_all_series(base_dir=base)
        assert results == []


class TestAtomicWriteJson:
    def test_write_then_read_roundtrip(self, tmp_path):
        path = os.path.join(str(tmp_path), "nested", "out.json")
        fetcher._atomic_write_json(path, {"a": 1})
        with open(path, encoding="utf-8") as f:
            assert json.load(f) == {"a": 1}

    def test_overwrite_replaces_content(self, tmp_path):
        path = os.path.join(str(tmp_path), "out.json")
        fetcher._atomic_write_json(path, {"version": 1})
        fetcher._atomic_write_json(path, {"version": 2})
        with open(path, encoding="utf-8") as f:
            assert json.load(f) == {"version": 2}
