"""
tests/test_method_change.py

計算方法の変更の印（common/method_change.py、[[LAYER3-RETENTION-PERIOD-1]]、2026-10-14新設）と、
score_watcher.pyが印のある実行の変化を「計算方法の変更による変化」として相場の変化と分けて通知することを検証する。

実行方法:
    venv/Scripts/python.exe -m pytest tests/test_method_change.py -v
"""

import json
import os
import sys
from datetime import date

from common import method_change as mc

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _cfg(tmp_path, changes):
    p = tmp_path / "method_change.json"
    p.write_text(json.dumps({"changes": changes}), encoding="utf-8")
    return str(p)


class TestActiveMethodChanges:
    def test_config_entry_for_today_is_active(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [{"id": "LAYER3-RETENTION-PERIOD-1", "date": "2026-10-19"}])
        assert mc.active_method_changes(date(2026, 10, 19), cfg) == ["LAYER3-RETENTION-PERIOD-1"]
        assert mc.active_method_changes(date(2026, 10, 20), cfg) == []

    def test_env_var_and_config_are_merged_without_duplicates(self, tmp_path, monkeypatch):
        monkeypatch.setenv(mc.ENV_NAME, "TTM-STALE-QUARTER-IN-WINDOW-1, LAYER3-RETENTION-PERIOD-1")
        cfg = _cfg(tmp_path, [{"id": "LAYER3-RETENTION-PERIOD-1", "date": "2026-10-19"}])
        assert mc.active_method_changes(date(2026, 10, 19), cfg) == ["TTM-STALE-QUARTER-IN-WINDOW-1", "LAYER3-RETENTION-PERIOD-1"]

    def test_invalid_ids_are_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv(mc.ENV_NAME, "not an id,abc-1,")
        assert mc.active_method_changes(date(2026, 10, 19), _cfg(tmp_path, [])) == []

    def test_missing_config_file_is_not_an_error(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        assert mc.active_method_changes(date(2026, 10, 19), str(tmp_path / "none.json")) == []

    def test_stamp_adds_key_only_when_active(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [{"id": "LAYER3-RETENTION-PERIOD-1", "date": "2026-10-19"}])
        assert mc.stamp({"a": 1}, date(2026, 10, 19), cfg) == {"a": 1, "method_change": "LAYER3-RETENTION-PERIOD-1"}
        assert mc.stamp({"a": 1}, date(2026, 10, 18), cfg) == {"a": 1}

    def test_repo_config_has_no_active_entry_by_default(self, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        with open(mc.CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
        assert isinstance(cfg.get("changes"), list)


def _sw():
    path = os.path.join(_REPO_ROOT, "src", "value", "tanuki_valuation")
    if path not in sys.path:
        sys.path.insert(0, path)
    import score_watcher
    return score_watcher


_PREV = {"tanuki_score": "WATCH", "funda_score": 70, "upside_percent": 10.0, "hype_stage": 2, "hype_label": "x"}


class TestScoreWatcherMethodChange:
    def test_changes_are_marked_when_latest_has_method_change(self, monkeypatch):
        sw = _sw()
        monkeypatch.setattr(sw, "load_latest", lambda t: {"tanuki_score": "BUY", "funda_score": 70, "upside_percent": 30.0,
                                                          "method_change": "LAYER3-RETENTION-PERIOD-1"})
        monkeypatch.setattr(sw, "load_hype_stage", lambda t: (2, "x"))
        monkeypatch.setattr(sw, "load_hype_method_change", lambda t: None)
        changes, _ = sw.detect_changes("DDOG", dict(_PREV))
        assert {c["type"] for c in changes} == {"score", "upside"}
        assert all(c["method_change"] == "LAYER3-RETENTION-PERIOD-1" for c in changes)

    def test_hype_change_uses_poc_mark(self, monkeypatch):
        sw = _sw()
        monkeypatch.setattr(sw, "load_latest", lambda t: {"tanuki_score": "WATCH", "funda_score": 70, "upside_percent": 10.0,
                                                          "method_change": "LAYER3-RETENTION-PERIOD-1"})
        monkeypatch.setattr(sw, "load_hype_stage", lambda t: (3, "y"))
        monkeypatch.setattr(sw, "load_hype_method_change", lambda t: None)
        changes, _ = sw.detect_changes("NVDA", dict(_PREV))
        assert [c["type"] for c in changes] == ["hype"]
        assert "method_change" not in changes[0]

    def test_message_separates_method_change_from_market(self):
        sw = _sw()
        msg = sw.build_discord_message({
            "DDOG": [{"type": "score", "label": "判定変化", "detail": "WATCH → **BUY**", "method_change": "LAYER3-RETENTION-PERIOD-1"}],
            "NVDA": [{"type": "hype", "label": "HypeCoreフェーズ転換", "detail": "Phase2 → **Phase3**"}],
        })
        assert "📈 **相場の変化**" in msg
        assert "🛠 **計算方法の変更による変化**（LAYER3-RETENTION-PERIOD-1）" in msg
        assert msg.index("NVDA") < msg.index("計算方法の変更による変化") < msg.index("DDOG")

    def test_message_without_mark_keeps_existing_format(self):
        sw = _sw()
        msg = sw.build_discord_message({"NVDA": [{"type": "hype", "label": "HypeCoreフェーズ転換", "detail": "Phase2 → **Phase3**"}]})
        assert "計算方法の変更" not in msg and "相場の変化" not in msg
        assert "**NVDA**" in msg


# ── 新しい形（[[METHOD-CHANGE-PUBLISH-TIMING-1]]、2026-11-02）: 前回の出力の method_change_applied と比べる ──

_NEW = {"id": "FCF-DEFINITION-SINGLE-SOURCE-1", "committed_at": "2026-11-05T10:00:00+09:00",
        "scope": [{"ticker": "HON", "fields": ["intrinsic_value_per_share", "tanuki_score", "upside_percent"],
                   "before": {"intrinsic_value_per_share": 122.45, "tanuki_score": "TRIM", "upside_percent": -40.0},
                   "after": {"intrinsic_value_per_share": 18.43, "tanuki_score": "WATCH", "upside_percent": -55.0}}]}
_OLD = {"id": "LAYER3-RETENTION-PERIOD-1", "date": "2026-10-19"}


class TestFirstReadMarks:
    def test_marks_only_the_run_that_first_reads_the_id(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_OLD, _NEW])
        first = mc.stamp({}, date(2026, 11, 6), cfg, prev={"tanuki_score": "TRIM"}, ticker="HON")
        assert first == {"method_change": "FCF-DEFINITION-SINGLE-SOURCE-1",
                         "method_change_applied": ["FCF-DEFINITION-SINGLE-SOURCE-1"]}
        second = mc.stamp({}, date(2026, 11, 6), cfg, prev=first, ticker="HON")   # 同じ日の次の実行
        assert "method_change" not in second and second["method_change_applied"] == ["FCF-DEFINITION-SINGLE-SOURCE-1"]

    def test_ticker_outside_scope_is_not_marked(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_NEW])
        out = mc.stamp({}, date(2026, 11, 6), cfg, prev={}, ticker="CAKE")
        assert "method_change" not in out and out["method_change_applied"] == ["FCF-DEFINITION-SINGLE-SOURCE-1"]

    def test_mark_is_not_decided_by_date(self, tmp_path, monkeypatch):
        """コミットの日に実行が無くても（日曜の連鎖が翌日に動いても）、初めて読んだ実行に付く"""
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_NEW])
        prev = {"method_change_applied": []}
        for d in (date(2026, 11, 5), date(2026, 11, 9), date(2026, 12, 1)):
            assert mc.stamp({}, d, cfg, prev=prev, ticker="HON").get("method_change") == "FCF-DEFINITION-SINGLE-SOURCE-1"

    def test_new_form_without_scope_marks_all_tickers(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [{"id": "STAGE-V2-ROLLOUT-1", "committed_at": "2026-11-05T10:00:00+09:00"}])
        for t in ("HON", "CAKE"):
            assert mc.stamp({}, date(2026, 11, 6), cfg, prev={}, ticker=t)["method_change"] == "STAGE-V2-ROLLOUT-1"
        assert mc.scope_for("STAGE-V2-ROLLOUT-1", "CAKE", cfg) == mc.ALL

    def test_old_form_still_marks_all_tickers_on_its_date(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_OLD])
        assert mc.stamp({}, date(2026, 10, 19), cfg, prev={}, ticker="CAKE") == {"method_change": "LAYER3-RETENTION-PERIOD-1"}
        assert mc.stamp({}, date(2026, 10, 20), cfg, prev={}, ticker="CAKE") == {}
        assert mc.scope_for("LAYER3-RETENTION-PERIOD-1", "CAKE", cfg) == mc.ALL

    def test_no_previous_output_records_without_marking(self, tmp_path, monkeypatch):
        """前回の出力が無い（新しい銘柄）は比べる相手が無いので印を付けず、読んだことだけ書く"""
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_NEW])
        assert mc.stamp({}, date(2026, 11, 6), cfg, prev=None, ticker="HON") == {
            "method_change_applied": ["FCF-DEFINITION-SINGLE-SOURCE-1"]}

    def test_whole_file_output_is_marked_when_scope_has_tickers(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_NEW, {"id": "ROTCE-NOOP-1", "committed_at": "2026-11-05T10:00:00+09:00", "scope": []}])
        out = mc.stamp({}, date(2026, 11, 6), cfg, prev={})
        assert out["method_change"] == "FCF-DEFINITION-SINGLE-SOURCE-1"

    def test_previous_mark_is_removed(self, tmp_path, monkeypatch):
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_NEW])
        prev = {"method_change": "FCF-DEFINITION-SINGLE-SOURCE-1", "method_change_applied": ["FCF-DEFINITION-SINGLE-SOURCE-1"]}
        assert "method_change" not in mc.stamp(dict(prev), date(2026, 11, 7), cfg, prev=prev, ticker="HON")

    def test_without_new_form_entries_outputs_do_not_change(self, tmp_path, monkeypatch):
        """新しい形の件が無い間は method_change_applied を書かない（今の出力と同じ）"""
        monkeypatch.delenv(mc.ENV_NAME, raising=False)
        cfg = _cfg(tmp_path, [_OLD])
        assert mc.stamp({"a": 1}, date(2026, 11, 6), cfg, prev={"a": 1}, ticker="HON") == {"a": 1}


class TestScoreWatcherScope:
    def _run(self, monkeypatch, tmp_path, latest, stage=(2, "x"), poc_mark=None):
        sw = _sw()
        cfg = _cfg(tmp_path, [_NEW])
        real = mc.scope_for
        monkeypatch.setattr(sw._method_change, "scope_for", lambda cid, t, config_path=None: real(cid, t, cfg))
        monkeypatch.setattr(sw, "load_latest", lambda t: latest)
        monkeypatch.setattr(sw, "load_hype_stage", lambda t: stage)
        monkeypatch.setattr(sw, "load_hype_method_change", lambda t: poc_mark)
        return sw

    def test_only_scope_fields_go_to_method_section(self, monkeypatch, tmp_path):
        sw = self._run(monkeypatch, tmp_path, {"tanuki_score": "WATCH", "funda_score": 40, "upside_percent": -60.0,
                                                "method_change": "FCF-DEFINITION-SINGLE-SOURCE-1"})
        prev = {"tanuki_score": "TRIM", "funda_score": 90, "upside_percent": -40.0, "hype_stage": 2, "hype_label": "x"}
        changes, _ = sw.detect_changes("HON", prev)
        by = {c["type"]: c for c in changes}
        assert by["score"]["method_change"] == "FCF-DEFINITION-SINGLE-SOURCE-1"
        assert "TRIM → WATCH" in by["score"]["method_note"] and "残り（相場等）: なし" in by["score"]["method_note"]
        assert by["upside"]["method_change"] == "FCF-DEFINITION-SINGLE-SOURCE-1"
        assert "残り（相場等）: -5.0pt" in by["upside"]["method_note"]   # 実際 −20pt のうち試算 −15pt
        assert "method_change" not in by["funda"]          # fundaはscopeの項目に無い → 相場の節
        msg = sw.build_discord_message({"HON": changes})
        assert "📈 **相場の変化**" in msg and "🛠 **計算方法の変更による変化**（FCF-DEFINITION-SINGLE-SOURCE-1）" in msg
        assert msg.index("ファンダスコア変化") < msg.index("計算方法の変更による変化") < msg.index("判定変化")
        assert "計算方法の分（株価を固定した試算）" in msg

    def test_ticker_without_mark_is_market(self, monkeypatch, tmp_path):
        sw = self._run(monkeypatch, tmp_path, {"tanuki_score": "TRIM", "funda_score": 70, "upside_percent": 10.0})
        changes, _ = sw.detect_changes("CAKE", dict(_PREV))
        assert changes and all("method_change" not in c for c in changes)
        assert "計算方法" not in sw.build_discord_message({"CAKE": changes})

    def test_hype_stage_outside_scope_is_market(self, monkeypatch, tmp_path):
        sw = self._run(monkeypatch, tmp_path, {"tanuki_score": "WATCH", "funda_score": 70, "upside_percent": 10.0},
                       stage=(3, "y"), poc_mark="FCF-DEFINITION-SINGLE-SOURCE-1")
        changes, _ = sw.detect_changes("HON", dict(_PREV))
        assert [c["type"] for c in changes] == ["hype"] and "method_change" not in changes[0]   # hype.stageはscopeに無い


class TestScopeScript:
    def _snap(self, root, tanuki, poc_stage):
        (root / "tanuki").mkdir(parents=True)
        (root / "hypecore").mkdir()
        for t, d in tanuki.items():
            (root / "tanuki" / f"{t}.json").write_text(json.dumps(d), encoding="utf-8")
            (root / "hypecore" / f"{t}_poc.json").write_text(
                json.dumps({"generated_at": "x", "monthly": [{"month": "2026-10", "stage": poc_stage[t]}]}), encoding="utf-8")

    def test_scope_from_before_after(self, tmp_path):
        import pytest
        sys.path.insert(0, os.path.join(_REPO_ROOT, "scripts"))
        import method_change_scope as ms
        self._snap(tmp_path / "b", {"HON": {"intrinsic_value_per_share": 122.45, "tanuki_score": "TRIM", "calculation_date": "a",
                                            "wacc": {"wacc": 0.08}, "fcf_history": [1]},
                                    "CAKE": {"intrinsic_value_per_share": 50.0, "tanuki_score": "HOLD"}}, {"HON": 4, "CAKE": 2})
        self._snap(tmp_path / "a", {"HON": {"intrinsic_value_per_share": 18.43, "tanuki_score": "WATCH", "calculation_date": "b",
                                            "wacc": {"wacc": 0.08}, "fcf_history": [2]},
                                    "CAKE": {"intrinsic_value_per_share": 50.0, "tanuki_score": "HOLD"}}, {"HON": 4, "CAKE": 3})
        e = ms.make_entry("FCF-DEFINITION-SINGLE-SOURCE-1", str(tmp_path / "b"), str(tmp_path / "a"), "2026-11-05T10:00:00+09:00")
        assert e["scope"] == [
            {"ticker": "CAKE", "fields": ["hype.stage"], "before": {"hype.stage": 2}, "after": {"hype.stage": 3}},
            {"ticker": "HON", "fields": ["intrinsic_value_per_share", "tanuki_score"],
             "before": {"intrinsic_value_per_share": 122.45, "tanuki_score": "TRIM"},
             "after": {"intrinsic_value_per_share": 18.43, "tanuki_score": "WATCH"}},
        ]
        cfg = tmp_path / "mc.json"
        cfg.write_text(json.dumps({"changes": [_OLD]}), encoding="utf-8")
        ms.write_entry(e, str(cfg))
        assert json.loads(cfg.read_text(encoding="utf-8"))["changes"][-1]["id"] == "FCF-DEFINITION-SINGLE-SOURCE-1"
        with pytest.raises(SystemExit):
            ms.write_entry(e, str(cfg))
