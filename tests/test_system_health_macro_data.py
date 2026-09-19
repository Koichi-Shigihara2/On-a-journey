"""
tests/test_system_health_macro_data.py

[[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]対応（2026-09-19）の回帰テスト。
common/system_health.py に追加したcheck L（common/macro_data/の系列単位
fetch_status監視）が、以下を正しく検知できることを検証する:

- macro_data_violations_log.json内にfetch_status=="failed"の系列がある
  場合にWARNとして検知する
- 全系列がfetch_status=="ok"の場合は正常と判定する
- fetch_statusフィールド自体が存在しない旧形式エントリ（移行期の
  過渡データ）はfailedと誤検知しない
- ログファイル自体が存在しない場合は異常扱いしない（スキップ）

実行方法:
    python -m pytest tests/test_system_health_macro_data.py -v
"""

import json
import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common import system_health as sh  # noqa: E402


def _write_log(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def test_all_ok_series_is_healthy(tmp_path, monkeypatch):
    log_path = os.path.join(str(tmp_path), "macro_data_violations_log.json")
    _write_log(log_path, {
        "PAYEMS": {"checked_at": "2026-09-18T22:56:52+09:00", "warnings": [], "fetch_status": "ok"},
        "CFNAI": {"checked_at": "2026-09-18T22:56:52+09:00", "warnings": [], "fetch_status": "ok"},
    })
    monkeypatch.setattr(sh, "_MACRO_DATA_VIOLATIONS_LOG", log_path)

    label, ok, detail = sh.check_l_macro_data()
    assert ok is True
    assert "✅" in label
    assert "全系列取得成功" in detail


def test_failed_series_is_detected(tmp_path, monkeypatch):
    log_path = os.path.join(str(tmp_path), "macro_data_violations_log.json")
    _write_log(log_path, {
        "PAYEMS": {"checked_at": "2026-09-18T22:56:52+09:00", "warnings": [], "fetch_status": "ok"},
        "FTSD": {
            "checked_at": "2026-09-18T22:57:06+09:00", "warnings": [],
            "fetch_status": "failed",
            "failure_reason": "FRED取得失敗（3回リトライ後も失敗）: Bad Request.",
        },
    })
    monkeypatch.setattr(sh, "_MACRO_DATA_VIOLATIONS_LOG", log_path)

    label, ok, detail = sh.check_l_macro_data()
    assert ok is False
    assert "⚠️" in label
    assert "FTSD" in detail
    assert "取得失敗" in detail


def test_multiple_failed_series_are_all_counted(tmp_path, monkeypatch):
    log_path = os.path.join(str(tmp_path), "macro_data_violations_log.json")
    _write_log(log_path, {
        "A": {"checked_at": "x", "warnings": [], "fetch_status": "ok"},
        "B": {"checked_at": "x", "warnings": [], "fetch_status": "failed", "failure_reason": "boom"},
        "C": {"checked_at": "x", "warnings": [], "fetch_status": "failed", "failure_reason": "boom2"},
    })
    monkeypatch.setattr(sh, "_MACRO_DATA_VIOLATIONS_LOG", log_path)

    label, ok, detail = sh.check_l_macro_data()
    assert ok is False
    assert "2件" in detail
    assert "B" in detail
    assert "C" in detail


def test_legacy_entries_without_fetch_status_are_not_flagged(tmp_path, monkeypatch):
    """移行期: まだ新フォーマットで上書きされていない旧エントリ
    （fetch_statusキー自体が存在しない）は"failed"として誤検知しない
    （手動編集で移行させない設計、次回実行で自然に新形式へ置き換わる）。"""
    log_path = os.path.join(str(tmp_path), "macro_data_violations_log.json")
    _write_log(log_path, {
        "FTSD": {"checked_at": "2026-09-18T22:57:06+09:00", "warnings": []},
    })
    monkeypatch.setattr(sh, "_MACRO_DATA_VIOLATIONS_LOG", log_path)

    label, ok, detail = sh.check_l_macro_data()
    assert ok is True


def test_missing_log_file_is_skipped_not_flagged(tmp_path, monkeypatch):
    log_path = os.path.join(str(tmp_path), "does_not_exist.json")
    monkeypatch.setattr(sh, "_MACRO_DATA_VIOLATIONS_LOG", log_path)

    label, ok, detail = sh.check_l_macro_data()
    assert ok is True
    assert "skip" in detail
