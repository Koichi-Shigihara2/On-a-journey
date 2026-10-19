"""
TANUKI TAIL edgar_rss_monitor.py の提出の遅れのアラート（2026-10-19、FINDINGS.md 2026-10-16・2026-10-19）。

期待日を「前回の提出日＋75日」ではなく、次の報告期（最新の提出の対象期末＋3か月）の法定の期限
（10-Q 45日・10-K 90日、土日なら次の月曜）にし、期限を過ぎてからの平日の数を遅れの日数として毎回アラートを出す。
RSS・submissionsの取得は差し替え、ネットワークには出ない。
"""
import os
import sys
from datetime import date, datetime

import pytest

_TAIL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "tail")
sys.path.insert(0, _TAIL_DIR)

import edgar_rss_monitor as erm  # noqa: E402

# 対象期末2026-03-31の提出（記録済み）。次の期末は06-30、10-Qの期限は08-14（金）
FILED = {"accn": "0000000001-26-000010", "filed": "2026-05-08", "form": "10-Q"}
NEW = {"accn": "0000000001-26-000020", "filed": "2026-08-17", "form": "10-Q"}
TIMEOUT = (False, None)


def _state(last_period="2026-03-31"):
    return {"XYZ": {"last_accn": FILED["accn"], "last_filed": FILED["filed"], "last_period": last_period,
                    "last_checked": "x", "no_filing_days": 0}}


@pytest.fixture
def env(monkeypatch):
    """env.q / env.k でその回のRSSの結果、env.fye で会計年度末を決める。"""
    class _E:
        q = (True, FILED)
        k = (True, None)
        fye = "1231"
        fye_calls = 0

    e = _E()
    monkeypatch.setattr(erm, "_fetch_rss_form", lambda cik, form: e.q if form == "10-Q" else e.k)
    monkeypatch.setattr(erm, "get_filing_period", lambda cik, accn: "2026-06-30")

    def _fye(cik):
        e.fye_calls += 1
        return e.fye
    monkeypatch.setattr(erm, "get_fiscal_year_end", _fye)
    return e


def _run(state, day, queue=None):
    now = datetime(day.year, day.month, day.day, 17, 0, tzinfo=erm.JST)
    return erm.process_ticker("XYZ", "0000000001", state, {"queue": []} if queue is None else queue, now)


def _overdue(alerts):
    return [a for a in alerts if a["type"] == "overdue"]


# ── 期限の前後と毎平日 ─────────────────────────────────────────

def test_no_alert_on_day_before_and_on_due_date(env):
    for day in (date(2026, 8, 13), date(2026, 8, 14)):
        state = _state()
        assert _overdue(_run(state, day)) == []
        assert state["XYZ"]["no_filing_days"] == 0


def test_alert_on_next_weekday_and_every_weekday_after(env):
    state = _state()
    a = _overdue(_run(state, date(2026, 8, 17)))           # 月曜（期限の金曜の翌平日）
    assert [(x["form"], x["period_end"], x["due"], x["no_filing_days"]) for x in a] == \
        [("10-Q", "2026-06-30", "2026-08-14", 1)]
    assert state["XYZ"]["no_filing_days"] == 1
    assert _overdue(_run(state, date(2026, 8, 18)))[0]["no_filing_days"] == 2
    assert _overdue(_run(state, date(2026, 8, 24)))[0]["no_filing_days"] == 6


def test_weekend_days_are_not_counted(env):
    state = _state()
    assert _overdue(_run(state, date(2026, 8, 15))) == []   # 土曜: 期限の翌日だが平日0日
    assert state["XYZ"]["no_filing_days"] == 0
    assert _overdue(_run(state, date(2026, 8, 21)))[0]["no_filing_days"] == 5  # 月〜金
    assert _overdue(_run(state, date(2026, 8, 23)))[0]["no_filing_days"] == 5  # 日曜は増えない


def test_due_date_on_weekend_rolls_to_monday(env):
    """対象期末06-30の次の期末09-30の10-Qの期限は11-14（土）→ 11-16（月）。11-16は出ず、11-17に出る。"""
    state = _state(last_period="2026-06-30")
    assert _overdue(_run(state, date(2026, 11, 16))) == []
    assert _overdue(_run(state, date(2026, 11, 17)))[0]["due"] == "2026-11-16"


def test_no_alert_on_day_filing_is_found(env):
    state = _state()
    env.q = (True, NEW)
    alerts = _run(state, date(2026, 8, 17))
    assert [a["type"] for a in alerts] == ["new_filing"]
    assert state["XYZ"]["no_filing_days"] == 0
    assert state["XYZ"]["last_accn"] == NEW["accn"]
    # 次の回は新しい対象期末（06-30）の次の期末（09-30）の期限まで出ない
    env.q = (True, NEW)
    assert _overdue(_run(state, date(2026, 8, 18))) == []


def test_fiscal_year_end_is_not_looked_up_before_10q_deadline(env):
    _run(_state(), date(2026, 8, 14))
    assert env.fye_calls == 0


# ── 10-Qと10-Kの期限の切り替え ─────────────────────────────────

def test_fiscal_year_end_period_uses_10k_deadline(env):
    """対象期末09-30の次の期末12-31は会計年度末 → 10-K、期限は2027-03-31（90日）。"""
    state = _state(last_period="2026-09-30")
    assert _overdue(_run(state, date(2027, 2, 16))) == []   # 10-Qの期限（02-15）は過ぎたが10-Kの期限の前
    assert state["XYZ"]["no_filing_days"] == 0
    a = _overdue(_run(state, date(2027, 4, 1)))
    assert [(x["form"], x["period_end"], x["due"], x["no_filing_days"]) for x in a] == \
        [("10-K", "2026-12-31", "2027-03-31", 1)]


def test_52_53_week_fiscal_year_end():
    """NVDA（fiscalYearEnd 0131、期末は1月の最終日曜）・ADBE（1127）の形。"""
    assert erm.filing_due(date(2027, 1, 25), "0131")[0] == "10-K"
    assert erm.filing_due(date(2026, 10, 26), "0131")[0] == "10-Q"
    assert erm.filing_due(date(2026, 11, 28), "1127")[0] == "10-K"
    assert erm.filing_due(date(2026, 8, 28), "1127")[0] == "10-Q"


def test_unknown_fiscal_year_end_uses_later_10k_deadline(env):
    env.fye = None
    state = _state()
    assert _overdue(_run(state, date(2026, 9, 1))) == []    # 10-Qなら遅れだが、10-Kの期限（09-28）の前
    assert erm.filing_due(date(2026, 6, 30), None) == ("10-K", date(2026, 9, 28))


# ── RSSの取得エラーの回（2d8dcf33abの動きを壊さない） ──────────────

def test_rss_error_does_not_judge_new_filing_but_still_checks_delay(env):
    state = _state()
    env.q, env.k = TIMEOUT, (True, {"accn": "0000000001-26-000001", "filed": "2026-02-20", "form": "10-K"})
    alerts = _run(state, date(2026, 8, 17))
    assert [a["type"] for a in alerts] == ["overdue"]
    assert state["XYZ"]["last_accn"] == FILED["accn"]


# ── Discordの本文 ─────────────────────────────────────────────

def test_discord_message_has_due_date_and_days():
    msg = erm.format_alert({"type": "overdue", "ticker": "XYZ", "form": "10-Q", "period_end": "2026-09-30",
                            "due": "2026-11-16", "no_filing_days": 3})
    assert "XYZ" in msg and "10-Q" in msg and "2026-09-30" in msg and "2026-11-16" in msg and "3日" in msg
    assert msg != erm.format_alert({"type": "overdue", "ticker": "XYZ", "form": "10-Q", "period_end": "2026-09-30",
                                    "due": "2026-11-16", "no_filing_days": 4})


def test_month_end_period_stays_month_end():
    assert erm.next_period_end(date(2026, 9, 30)) == date(2026, 12, 31)
    assert erm.next_period_end(date(2026, 6, 30)) == date(2026, 9, 30)
    assert erm.next_period_end(date(2026, 11, 30)) == date(2027, 2, 28)
    assert erm.next_period_end(date(2026, 8, 28)) == date(2026, 11, 28)
