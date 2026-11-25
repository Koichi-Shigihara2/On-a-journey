"""決算の反映前の印（[[EARNINGS-REFLECTION-LAG-1]] 案B、common/earnings_pending.py）。表示だけで、判定・理論株価は変えない"""
from datetime import date

from common import earnings_pending as ep

TODAY = date(2026, 11, 25)


def test_before_announcement_no_mark():
    # 発表日が今日より後 → 付かない
    r = ep.evaluate(["2026-11-30"], "2026-07-26", TODAY)
    assert r["status"] == ep.NONE


def test_announced_but_not_taken_in_marks():
    # 発表日 ≤ 今日、Layer3の最新の期末（07-26）の次の四半期（10-26）は発表日（11-18）より前に終わっている → 印
    r = ep.evaluate(["2026-11-18"], "2026-07-26", TODAY)
    assert r["status"] == ep.PENDING
    assert r["label"] == "最新の決算（11-18 発表）は未反映。次の取り込みは日曜"


def test_removed_after_taken_in():
    # Layer3に10-25の期が入ったら外れる
    r = ep.evaluate(["2026-11-18"], "2026-10-25", TODAY)
    assert r["status"] == ep.NONE


def test_no_date_no_mark_and_report_says_so():
    r = ep.evaluate(None, "2026-07-26", TODAY)
    assert r["status"] == ep.NO_DATE
    assert "付けられません" in ep.report_line(r)
    assert ep.evaluate([], "2026-07-26", TODAY)["status"] == ep.NO_DATE


def test_stale_date_not_marked():
    r = ep.evaluate(["2026-06-01"], "2026-03-31", TODAY)
    assert r["status"] == ep.NO_DATE


def test_latest_past_date_is_used_among_list():
    r = ep.evaluate(["2026-08-27", "2026-11-18", "2027-02-18"], "2026-07-26", TODAY)
    assert r["status"] == ep.PENDING and r["earnings_date"] == "2026-11-18"


def test_item_202_preferred_over_yahoo():
    r = ep.evaluate(["2026-11-24"], "2026-07-26", TODAY, item_202_dates=["2026-11-18"])
    assert r["earnings_date"] == "2026-11-18" and r["earnings_source"] == "8-K_2.02"


def test_add_quarter_month_end():
    assert ep.add_quarter(date(2026, 11, 30)) == date(2027, 2, 28)
    assert ep.add_quarter(date(2026, 6, 30)) == date(2026, 9, 30)
    assert ep.add_quarter(date(2026, 7, 26)) == date(2026, 10, 26)


def test_prev_pending_date_is_carried_when_yahoo_rolled():
    # Yahooが予定日を次の四半期へ進めても、前回印が付いた発表日は取り込みまで引き継ぐ
    prev = {"status": ep.PENDING, "earnings_date": "2026-11-13"}
    r = ep.build("ZZZZ", TODAY, calendar_dict={"earnings_date": ["2027-02-12"]}, prev=prev)
    # ZZZZのLayer3は無い → no_layer3（引き継いだ日付は使われている）
    assert r["earnings_date"] == "2026-11-13" and r["status"] == ep.NO_LAYER3


def test_no_layer3_not_marked():
    r = ep.evaluate(["2026-11-18"], None, TODAY)
    assert r["status"] == ep.NO_LAYER3 and ep.report_line(r) is None


def test_us_today_is_a_date_not_ahead_of_japan():
    from datetime import timedelta
    assert ep.us_today() <= date.today() + timedelta(days=1)
