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
