"""
tests/test_system_health_macro_data.py

[[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]対応（2026-09-19）の回帰テストと、
[[CHECK-REFERENCE-DEFINITION-1]]（2026-11-03）の取得の失敗の重大度の表（common/fetch_failure.py）のテスト。

- 取得する系列（series_meta.json）のうちfetch_status=="failed"の系列を数える。ログに残る昔の系列は数えない
- 重大度: 全系列が失敗、または同じ系列の失敗が2日以上続けば🔴、一部の失敗で1日目なら⚠️、失敗なしは✅
- 続いた日数は書き手（fetcher._write_violations_section）が残すfailed_sinceから数える
- 2026-10-25〜10-30（FREDの全25系列の失敗、ログはfailed_sinceを足す前の形）の状態で🔴になる
- 通知の1行で系列名を切らない（以前は「BAMLH0A0H」で切れていた）

実行方法:
    python -m pytest tests/test_system_health_macro_data.py -v
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common import fetch_failure as ff  # noqa: E402
from common import system_health as sh  # noqa: E402
from common.macro_data import fetcher as mfetch  # noqa: E402

JST = timezone(timedelta(hours=9))
# 2026-10-24〜10-30 の series_meta.json の25系列（今と同じ）
SERIES_25 = ['BAMLH0A0HYM2', 'CFNAIMA3', 'DFEDTARL', 'DFEDTARU', 'DGS1', 'DGS3MO', 'FEDFUNDS', 'GACDFSA066MSFRBPHI',
             'IC4WSA', 'M2SL', 'MICH', 'PAYEMS', 'PERMIT', 'RRPONTSYD', 'SAHMCURRENT', 'SP500', 'T10Y2Y', 'T5YIE',
             'UMCSENT', 'VIXCLS', 'VXNCLS', 'WALCL', 'WDTGAL', 'WRBWFRBL', 'WTREGEN']


def _write(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


@pytest.fixture
def setup_l(tmp_path, monkeypatch):
    """(ログ, 取得する系列) を書いて [L] を回す関数を返す"""
    log_path = os.path.join(str(tmp_path), "macro_data_violations_log.json")
    meta_path = os.path.join(str(tmp_path), "series_meta.json")
    monkeypatch.setattr(sh, "_MACRO_DATA_VIOLATIONS_LOG", log_path)
    monkeypatch.setattr(sh, "_MACRO_DATA_SERIES_META", meta_path)

    def run(log, series, now=None):
        if log is not None:
            _write(log_path, log)
        _write(meta_path, {s: {} for s in series})
        if now is not None:
            monkeypatch.setattr(sh, "_utcnow", lambda: now)
        return sh.check_l_macro_data()
    return run


def _ok(t="2026-09-18T22:56:52+09:00"):
    return {"checked_at": t, "warnings": [], "fetch_status": "ok"}


def _failed(t="2026-09-18T22:57:06+09:00", since=None, reason="boom"):
    e = {"checked_at": t, "warnings": [], "fetch_status": "failed", "failure_reason": reason}
    if since:
        e["failed_since"] = since
    return e


def test_all_ok_series_is_healthy(setup_l):
    label, ok, detail = setup_l({"PAYEMS": _ok(), "WALCL": _ok()}, ["PAYEMS", "WALCL"])
    assert ok is True and "✅" in label and "全2系列取得成功" in detail


def test_one_failed_series_first_day_is_warning(setup_l):
    label, ok, detail = setup_l({"PAYEMS": _ok(), "WALCL": _failed()}, ["PAYEMS", "WALCL"])
    assert ok is False and label.startswith("⚠️") and "WALCL" in detail and "取得失敗 1/2系列" in detail


def test_multiple_failed_series_are_all_counted(setup_l):
    label, ok, detail = setup_l({"A": _ok(), "B": _failed(), "C": _failed()}, ["A", "B", "C"])
    assert ok is False and label.startswith("⚠️") and "取得失敗 2/3系列（Bほか1件）" in detail


def test_all_series_failed_is_critical(setup_l):
    label, ok, detail = setup_l({"A": _failed(), "B": _failed()}, ["A", "B"])
    assert label.startswith("🔴") and "全件が失敗" in detail


def test_same_series_failing_two_days_is_critical(setup_l):
    now = datetime(2026, 10, 26, 23, 30, tzinfo=timezone.utc)   # JST 10-27 08:30 の System Health
    log = {"A": _ok(), "B": _failed("2026-10-27T01:21:00+09:00", since="2026-10-26T01:00:00+09:00")}
    label, ok, detail = setup_l(log, ["A", "B"], now=now)
    assert label.startswith("🔴") and "B" in detail and "2日以上" in detail
    # 同じ日の失敗だけなら1日目（⚠️）
    log["B"] = _failed("2026-10-27T01:21:00+09:00", since="2026-10-27T01:00:00+09:00")
    label, ok, detail = setup_l(log, ["A", "B"], now=now)
    assert label.startswith("⚠️")


def test_old_series_left_in_log_are_not_counted(setup_l):
    """ログに残る昔の系列（取得しなくなった CFNAI・FTSD）は数えない（書き手が回す series_meta.json の系列だけ）"""
    log = {"PAYEMS": _ok(), "FTSD": _failed(), "CFNAI": _ok("2026-10-03T01:00:55+09:00")}
    label, ok, detail = setup_l(log, ["PAYEMS"])
    assert ok is True and "全1系列取得成功" in detail


def test_legacy_entries_without_fetch_status_are_not_flagged(setup_l):
    """fetch_statusキー自体が存在しない旧エントリは"failed"として誤検知しない"""
    label, ok, detail = setup_l({"FTSD": {"checked_at": "2026-09-18T22:57:06+09:00", "warnings": []}}, ["FTSD"])
    assert ok is True


def test_missing_log_file_is_skipped_not_flagged(setup_l):
    label, ok, detail = setup_l(None, ["PAYEMS"])
    assert ok is True and "skip" in detail


@pytest.mark.parametrize("day", ["2026-10-25", "2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30"])
def test_replay_2026_10_25_to_30_fred_outage_is_critical(setup_l, day):
    """10-24夜〜10-30のMacro Dataの実行は全25系列が「FREDクライアント初期化失敗: No module named 'common'」
    （[[MACRODATA-FRED-IMPORT-PATH-1]]）。ログはfailed_sinceを足す前の形。ログには昔の系列 CFNAI・FTSD も残っていた。
    各日の朝（JST 08:30）の System Health は🔴（以前は⚠️）"""
    run_at = (datetime.fromisoformat(day + "T00:30:00+09:00") - timedelta(hours=1)).isoformat()
    log = {s: _failed(run_at, reason="FREDクライアント初期化失敗: No module named 'common'") for s in SERIES_25}
    log["CFNAI"] = _ok("2026-10-03T01:00:55+09:00")
    log["FTSD"] = {"checked_at": "2026-09-18T22:57:06+09:00", "warnings": []}
    now = datetime.fromisoformat(day + "T08:30:00+09:00").astimezone(timezone.utc)
    label, ok, detail = setup_l(log, SERIES_25, now=now)
    assert label.startswith("🔴")
    assert detail.startswith("取得失敗 25/25系列（BAMLH0A0HYM2ほか24件）")


def test_discord_line_keeps_full_series_name(monkeypatch, setup_l):
    log = {s: _failed() for s in SERIES_25}
    label, ok, det = setup_l(log, SERIES_25)
    results = {"L": {"ok": ok, "short": det, "severity": sh._item_severity(label, ok)}}
    line = sh.build_one_line("2026-10-27", results)
    assert "BAMLH0A0HYM2ほか24件" in line and "MacroData🔴" in line


def test_writer_carries_failed_since_until_success(tmp_path, monkeypatch):
    """書き手: 失敗の行だけ failed_since を書き、続く失敗では最初の時刻を引き継ぎ、成功で消える"""
    base = str(tmp_path)
    times = iter(["2026-10-24T22:41:20+09:00", "2026-10-25T23:00:11+09:00", "2026-10-30T20:44:04+09:00"])
    monkeypatch.setattr(mfetch, "_now_jst_iso", lambda: next(times))
    mfetch._write_violations_section("PAYEMS", [], base, fetch_status="failed", failure_reason="x")
    mfetch._write_violations_section("PAYEMS", [], base, fetch_status="failed", failure_reason="x")
    with open(os.path.join(base, "macro_data_violations_log.json"), encoding="utf-8") as f:
        e = json.load(f)["PAYEMS"]
    assert e["failed_since"] == "2026-10-24T22:41:20+09:00" and e["checked_at"].startswith("2026-10-25")
    assert ff.failed_days(e["failed_since"], datetime(2026, 10, 25, 23, 30, tzinfo=JST)) == 2
    mfetch._write_violations_section("PAYEMS", [], base, fetch_status="ok")
    with open(os.path.join(base, "macro_data_violations_log.json"), encoding="utf-8") as f:
        assert "failed_since" not in json.load(f)["PAYEMS"]


def test_severity_table_rows():
    assert ff.judge(3, {}) == (0, "失敗なし")
    assert ff.judge(3, {"A": 1})[0] == 1
    assert ff.judge(3, {"A": 2})[0] == 2
    assert ff.judge(2, {"A": 1, "B": 1})[0] == 2
    assert [s for s, _ in ff.SEVERITY_TABLE] == [2, 2, 1, 0]
