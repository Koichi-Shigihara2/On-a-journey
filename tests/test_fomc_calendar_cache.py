"""[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] A2（2026-11-04）: FOMCの予定のページの予備を、決め打ちの一覧ではなく取れた回のキャッシュに。
キャッシュが古いと System Health の [P] ExternalChange が⚠️（重大度は common/fetch_failure.py の表）。予備を使った回は出力に印"""
import importlib.util
import json
import os
import sys
from datetime import date, datetime, timezone

import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import fetch_failure  # noqa: E402
from common import system_health as sh  # noqa: E402
from common.macro_data import fomc_calendar as fc  # noqa: E402

_spec = importlib.util.spec_from_file_location("main05_fomc", os.path.join(REPO, "src", "market", "macro_pulse", "05_main.py"))
main05 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(main05)


def _page(year_rows, links):
    body = ""
    for year, rows in year_rows:
        body += f"<h4>{year} FOMC Meetings</h4>"
        for mon, d in rows:
            body += (f'<div class="fomc-meeting__month col-xs-5"><strong>{mon}</strong></div>'
                     f'<div class="fomc-meeting__date col-xs-4">{d}</div>')
    body += "".join(f'<a href="/newsevents/pressreleases/monetary{d}a.htm">s</a>' for d in links)
    return body


PAGE = _page([(2026, [("January", "27-28"), ("Oct/Nov", "31-1"), ("August", "22 (notation vote)"), ("December", "8-9*")])],
             ["20260128", "20261028"])


class _Resp:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def test_parse_meetings_and_statements():
    assert fc.parse_meetings(PAGE) == ["2026-01-28", "2026-11-01", "2026-12-09"]   # 臨時（日が1つ）は除く、「Oct/Nov」は後の月
    assert [d for d, _ in fc.parse_statements(PAGE)] == ["20261028", "20260128"]


def test_save_and_fallback_and_staleness(tmp_path):
    p = str(tmp_path / "c.json")
    fc.save_cache(PAGE, now=datetime(2026, 11, 4, tzinfo=timezone.utc), path=p)
    assert fc.latest_statement_from_cache(date(2026, 11, 3), p)[0] == "2026-10-28"
    assert fc.latest_statement_from_cache(date(2026, 10, 1), p)[0] == "2026-01-28"
    assert fc.staleness(date(2026, 11, 10), p) == []
    r = fc.staleness(date(2026, 11, 12), p)
    assert len(r) == 1 and "8日" in r[0]                                              # 最後に取れてから7日を超える
    assert any("すべて過去" in x for x in fc.staleness(date(2026, 12, 10), p))         # 次の会合を知らない
    assert fc.staleness(date(2026, 11, 4), str(tmp_path / "none.json")) == ["FOMCの予定のキャッシュが無い"]
    assert fc.save_cache("<html>作りが変わった</html>", path=p) == {}                 # 声明のリンクが無いページは保存しない
    assert json.load(open(p, encoding="utf-8"))["statements"]


def test_system_health_p_uses_the_table(tmp_path):
    p = str(tmp_path / "c.json")
    fc.save_cache(PAGE, now=datetime(2026, 11, 4, tzinfo=timezone.utc), path=p)
    ok = sh._check_p(date(2026, 11, 5), p)
    assert ok["ok"] and ok["severity"] == 0 and "次の会合 2026-12-09" in ok["label"]
    ng = sh._check_p(date(2026, 11, 20), p)
    assert not ng["ok"] and ng["severity"] == 1 and fetch_failure.STALE_CACHE_ROW[1] in ng["label"]
    assert "ExtChange⚠️" in sh.build_one_line("2026-11-20", {"P": {"ok": False, "short": "x", "severity": 1}})


def test_05_main_uses_cache_when_page_unavailable_and_marks_row(tmp_path, monkeypatch):
    cache = str(tmp_path / "c.json")
    fc.save_cache(PAGE, now=datetime(2026, 11, 4, tzinfo=timezone.utc), path=cache)
    monkeypatch.setattr(main05, "FOMC_CACHE_PATH", cache)

    def frb(path, label=""):
        if path == main05.FOMC_CALENDAR_PATH:
            return _Resp("", 503)
        return _Resp("<p>The Federal Open Market Committee decided ...</p>")
    monkeypatch.setattr(main05, "frb_get", frb)
    d, text, src = main05.fetch_latest_fomc_statement(date(2026, 11, 3))
    assert (d, src) == ("2026-10-28", main05.FOMC_SOURCE_CACHE) and text

    monkeypatch.setattr(main05, "frb_get", lambda path, label="": _Resp(PAGE) if path == main05.FOMC_CALENDAR_PATH
                        else _Resp("<p>The Federal Open Market Committee decided ...</p>"))
    assert main05.fetch_latest_fomc_statement(date(2026, 11, 3))[2] == ""            # 取れた回は印なし

    # 出力の印: 予備で判定した行の fomc_date_source が "cache"
    monkeypatch.setattr(main05, "frb_get", frb)
    out = tmp_path / "fed.csv"
    monkeypatch.setattr(main05, "FED_CONTEXT_PATH", str(out))
    monkeypatch.setattr(main05, "BASE_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(main05, "get_ff_current", lambda: 3.875)
    monkeypatch.setattr(main05, "get_zq_futures", lambda target_date=None: ("FRED:DGS1", None, 4.52))
    monkeypatch.delenv("XAI_API_KEY", raising=False)
    monkeypatch.setattr(main05, "grok_text", lambda *a, **k: (_ for _ in ()).throw(main05.ExternalApiError("x")))
    main05.update_fed_context(date(2026, 11, 3))
    row = pd.read_csv(out, dtype=str).fillna("").iloc[-1]
    assert row["fomc_date"] == "2026-10-28" and row["fomc_date_source"] == "cache"


def test_no_hardcoded_fallback_dates():
    src = open(os.path.join(REPO, "src", "market", "macro_pulse", "05_main.py"), encoding="utf-8").read()
    assert "20260507" not in src and "known = [" not in src


def test_saved_cache_is_valid():
    c = fc.load_cache()
    assert c.get("statements") and c.get("meetings") and c.get("last_success_utc")
