"""
TANUKI TAIL edgar_rss_monitor.py の新規提出の誤検知の対策（[[TAIL-RSS-FALSE-NEW-FILING-1]]、2026-10-16）。

10-QのRSSがタイムアウトすると古い10-Kが最新に見え、新規提出として扱われていた
（run 36451110918・36583985823、CELHは09-22〜09-29に2025Q4と2026Q2を交互に検知）。
RSS・submissionsの取得は差し替え、ネットワークには出ない。
"""
import os
import sys
from datetime import date, datetime

import pytest

_TAIL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "tail")
sys.path.insert(0, _TAIL_DIR)

import edgar_rss_monitor as erm  # noqa: E402

NOW = datetime(2026, 10, 16, 17, 0, tzinfo=erm.JST)

# CELHの実例（rss_state.json・review_queue.jsonの値）
CELH_Q = {"accn": "0001341766-26-000050", "filed": "2026-08-06", "form": "10-Q"}  # 2026Q2
CELH_K = {"accn": "0001341766-26-000024", "filed": "2026-03-02", "form": "10-K"}  # 2025Q4
TIMEOUT = (False, None)


def _celh_state(**kw):
    s = {"last_accn": CELH_Q["accn"], "last_filed": CELH_Q["filed"], "last_period": "2026-06-30",
         "last_checked": "2026-09-21T22:00:00+09:00", "no_filing_days": 0}
    s.update(kw)
    return {"CELH": s}


@pytest.fixture
def rss(monkeypatch):
    """rss.set(q=(ok, entry), k=(ok, entry)) でその回のRSSの結果を決める。"""
    class _R:
        q = (True, None)
        k = (True, None)

        def set(self, q, k):
            self.q, self.k = q, k

    r = _R()
    monkeypatch.setattr(erm, "_fetch_rss_form", lambda cik, form: r.q if form == "10-Q" else r.k)
    periods = {CELH_Q["accn"]: "2026-06-30", CELH_K["accn"]: "2025-12-31"}
    monkeypatch.setattr(erm, "get_filing_period", lambda cik, accn: periods.get(accn, "2026-09-30"))
    monkeypatch.setattr(erm, "get_fiscal_year_end", lambda cik: "1231")
    return r


def _run(state, queue):
    return erm.process_ticker("CELH", "0001341766", state, queue, NOW)


def test_10q_timeout_does_not_treat_old_10k_as_new(rss):
    state, queue = _celh_state(), {"queue": []}
    rss.set(q=TIMEOUT, k=(True, CELH_K))
    alerts = _run(state, queue)
    assert [a for a in alerts if a["type"] == "new_filing"] == []
    assert state["CELH"]["last_accn"] == CELH_Q["accn"]
    assert state["CELH"]["last_filed"] == CELH_Q["filed"]
    assert queue["queue"] == []


def test_10q_timeout_does_not_accept_newer_10k_either(rss):
    """片方が取れない回は、もう片方が新しくても判定しない（次の回に両方そろってから）。"""
    state, queue = _celh_state(), {"queue": []}
    new_k = {"accn": "0001341766-26-000099", "filed": "2026-10-15", "form": "10-K"}
    rss.set(q=TIMEOUT, k=(True, new_k))
    assert _run(state, queue) == []
    assert state["CELH"]["last_accn"] == CELH_Q["accn"]
    # 次の回に両方取れれば新規として扱う
    rss.set(q=(True, CELH_Q), k=(True, new_k))
    alerts = _run(state, queue)
    assert [a["accn"] for a in alerts if a["type"] == "new_filing"] == [new_k["accn"]]


def test_alternating_detection_celh_0922_0929(rss):
    """09-22〜09-29の形: タイムアウトの回と両方取れる回が交互に来ても、新規提出は一度も出ない。"""
    state, queue = _celh_state(), {"queue": []}
    runs = [(TIMEOUT, (True, CELH_K)), ((True, CELH_Q), (True, CELH_K))] * 3
    for q, k in runs:
        rss.set(q=q, k=k)
        alerts = _run(state, queue)
        assert [a for a in alerts if a["type"] == "new_filing"] == []
        assert state["CELH"]["last_accn"] == CELH_Q["accn"]
    assert queue["queue"] == []


def test_revert_to_older_filing_is_ignored(rss):
    """両方取れても、RSSの最新が記録済みより古い書類なら更新も通知もしない。"""
    state, queue = _celh_state(), {"queue": []}
    old_q = {"accn": "0001341766-26-000010", "filed": "2026-05-07", "form": "10-Q"}
    rss.set(q=(True, old_q), k=(True, CELH_K))
    assert erm.classify_filing(old_q, state["CELH"], []) == "older"
    assert [a for a in _run(state, queue) if a["type"] == "new_filing"] == []
    assert state["CELH"]["last_accn"] == CELH_Q["accn"]
    assert queue["queue"] == []


def test_genuine_new_filing(rss):
    state, queue = _celh_state(), {"queue": []}
    new_q = {"accn": "0001341766-26-000070", "filed": "2026-11-05", "form": "10-Q"}
    rss.set(q=(True, new_q), k=(True, CELH_K))
    alerts = _run(state, queue)
    assert alerts == [{"type": "new_filing", "ticker": "CELH", "quarter": "2026Q3",
                       "accn": new_q["accn"], "filed": new_q["filed"]}]
    assert state["CELH"]["last_accn"] == new_q["accn"]
    assert state["CELH"]["last_filed"] == new_q["filed"]
    assert [e["accn"] for e in queue["queue"]] == [new_q["accn"]]


def test_same_filed_date_new_accn_is_new():
    prev = _celh_state()["CELH"]
    same_day = {"accn": "0001341766-26-000051", "filed": CELH_Q["filed"], "form": "10-Q"}
    assert erm.classify_filing(same_day, prev, []) == "new"
    assert erm.classify_filing(CELH_Q, prev, []) == "same"


def test_overdue_alert_still_fires_when_10q_times_out(rss):
    """片方が取れない回も、遅れの通知は今までどおり出す（新規提出の判定だけをしない）。
    対象期末2026-03-31の次の期末06-30の10-Qの期限は08-14。NOW（10-16）では期限を過ぎている。"""
    state = _celh_state(last_period="2026-03-31")
    rss.set(q=TIMEOUT, k=(True, CELH_K))
    alerts = _run(state, {"queue": []})
    assert [a["type"] for a in alerts] == ["overdue"]
    assert state["CELH"]["no_filing_days"] == erm.weekdays_overdue(date(2026, 8, 14), NOW.date())
    assert state["CELH"]["last_accn"] == CELH_Q["accn"]


def test_first_record_postponed_when_one_form_fails(rss):
    state, queue = {}, {"queue": []}
    rss.set(q=TIMEOUT, k=(True, CELH_K))
    assert _run(state, queue) == []
    assert "CELH" not in state
    rss.set(q=(True, CELH_Q), k=(True, CELH_K))
    assert _run(state, queue) == []
    assert state["CELH"]["last_accn"] == CELH_Q["accn"]
    assert state["CELH"]["last_filed"] == CELH_Q["filed"]


def test_legacy_state_without_filed_date_same_accn(rss):
    """提出日の無い旧形式: 同じ書類を見た回に提出日を足して保存する。"""
    state = {"CELH": {"last_accn": CELH_Q["accn"], "last_checked": "x", "no_filing_days": 0}}
    rss.set(q=(True, CELH_Q), k=(True, CELH_K))
    assert _run(state, {"queue": []}) == []
    assert state["CELH"]["last_filed"] == CELH_Q["filed"]
    assert state["CELH"]["last_accn"] == CELH_Q["accn"]


def test_legacy_state_without_filed_date_new_accn(rss):
    """旧形式でaccnが違えば従来どおり新規。提出日をlast_accnと一緒に保存する。"""
    state, queue = {"CELH": {"last_accn": CELH_K["accn"], "no_filing_days": 0}}, {"queue": []}
    rss.set(q=(True, CELH_Q), k=(True, CELH_K))
    alerts = _run(state, queue)
    assert [a["accn"] for a in alerts if a["type"] == "new_filing"] == [CELH_Q["accn"]]
    assert state["CELH"]["last_filed"] == CELH_Q["filed"]


def test_legacy_state_without_filed_date_timeout_does_not_crash(rss):
    state = {"CELH": {"last_accn": CELH_Q["accn"], "no_filing_days": 0}}
    rss.set(q=TIMEOUT, k=(True, CELH_K))
    assert _run(state, {"queue": []}) == []
    assert state["CELH"]["last_accn"] == CELH_Q["accn"]


def test_fetch_rss_latest_reports_failed_forms(monkeypatch):
    monkeypatch.setattr(erm, "_fetch_rss_form",
                        lambda cik, form: TIMEOUT if form == "10-Q" else (True, CELH_K))
    assert erm.fetch_rss_latest("x") == (CELH_K, ["10-Q"])
    monkeypatch.setattr(erm, "_fetch_rss_form",
                        lambda cik, form: (True, CELH_Q) if form == "10-Q" else (True, None))
    assert erm.fetch_rss_latest("x") == (CELH_Q, [])
