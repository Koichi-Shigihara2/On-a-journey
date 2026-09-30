"""Market Pulse 実装C（指示書㉗）: ニュースの見出し・予定・先物の最新値の単体テスト。外部取得はすべてモック。

取得に失敗した要素が「取得できず」（failed・status）になり他の段階に影響しないこと、data_qualityに反映されること、
予定の日付変換（ET→JST、夏時間の切り替え）、祝日・短縮取引、満期（祝日なら前営業日）、決算の対象の限定、
ニュースがAIの入力に入らないことを確認する。
"""
import json
import os
import sys
from datetime import date, datetime, time, timezone

import pytest

_MARKET_PULSE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "market", "market_pulse"))
if _MARKET_PULSE_DIR not in sys.path:
    sys.path.insert(0, _MARKET_PULSE_DIR)

import news_headlines as nh  # noqa: E402
import market_calendar as mc  # noqa: E402
import futures_snapshot as fs  # noqa: E402
import stage_conclusions as sc  # noqa: E402
import collect_and_send as cs  # noqa: E402

RSS = """<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>
<item><title>{t1}</title><link>https://example.com/1</link><pubDate>Tue, 29 Sep 2026 22:00:00 GMT</pubDate></item>
<item><title>{t2}</title><link>https://example.com/2</link><pubDate>Tue, 29 Sep 2026 21:00:00 GMT</pubDate></item>
</channel></rss>"""


class TestHeadlines:
    def test_ok_dedupe_and_fields(self):
        def fetch(url):
            return RSS.format(t1="Stocks rise", t2="Oil falls").encode()
        h = nh.fetch_headlines(fetch)
        assert h["status"] == "ok" and h["failed"] == []
        titles = [x["title"] for x in h["items"]]
        assert titles.count("Stocks rise") == 1          # 同じ見出しは1件
        x = h["items"][0]
        assert set(x) == {"title", "published_utc", "link", "source", "region"}   # 見出し・時刻・リンク・出典のみ
        assert x["published_utc"] == "2026-09-29T22:00:00Z"

    def test_partial_and_total_failure(self):
        def fetch_one_bad(url):
            if "nhk" in url:
                raise ConnectionError("down")
            return RSS.format(t1="A " + url[-5:], t2="B " + url[-5:]).encode()
        h = nh.fetch_headlines(fetch_one_bad)
        assert h["status"] == "partial" and h["failed"] == ["NHK 経済"] and h["items"]
        h = nh.fetch_headlines(lambda url: (_ for _ in ()).throw(TimeoutError("x")))
        assert h["status"] == "failed" and h["items"] == [] and len(h["failed"]) == 3

    def test_region_cap_keeps_japan(self):
        def fetch(url):
            region_jp = "nhk" in url
            items = "".join(f"<item><title>{'JP' if region_jp else url[-8:]} {i}</title><link>https://e/{i}</link>"
                            f"<pubDate>Tue, 29 Sep 2026 {'01' if region_jp else '23'}:{i:02d}:00 GMT</pubDate></item>" for i in range(10))
            return f'<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>{items}</channel></rss>'.encode()
        h = nh.fetch_headlines(fetch)
        regions = [x["region"] for x in h["items"]]
        assert regions.count("日本") == 6 and regions.count("米国") == 14


class TestCalendar:
    @pytest.mark.parametrize("s,t", [("3:30 p.m.", time(15, 30)), ("10:00 a.m.", time(10, 0)), ("12:00 p.m.", time(12, 0)),
                                     ("12:15 a.m.", time(0, 15)), ("TBD", None)])
    def test_parse_fed_time(self, s, t):
        assert mc.parse_fed_time(s) == t

    def test_et_to_jst_dst(self):
        assert mc._et_to_jst(date(2026, 10, 28), time(14, 0)) == "2026-10-29 03:00"   # 夏時間（EDT、+13時間）
        assert mc._et_to_jst(date(2026, 12, 9), time(14, 0)) == "2026-12-10 04:00"    # 冬時間（EST、+14時間）

    def test_fed_events_window_and_types(self):
        j = {"events": [
            {"type": "FOMC", "title": "FOMC Meeting", "month": "2026-10", "days": "27-28", "time": "2:00 p.m."},
            {"type": "Speeches", "title": "Speech - Chair", "month": "2026-10", "days": "1", "time": "10:00 a.m."},
            {"type": "Stat", "title": "H.4.1", "month": "2026-10", "days": "1", "time": ""},
            {"type": "FOMC", "title": "FOMC Minutes", "month": "2026-11", "days": "18", "time": "2:00 p.m."}]}
        ev = mc.fed_events(date(2026, 9, 30), date(2026, 10, 28), get_json=lambda url, p=None: j)
        assert [(e["date_et"], e["kind"], e["time_jst"]) for e in ev] == [
            ("2026-10-28", "FOMC", "2026-10-29 03:00"), ("2026-10-01", "FRB要人の講演", "2026-10-01 23:00")]

    def test_econ_only_whitelisted_and_no_time(self):
        j = {"release_dates": [{"date": "2026-10-02", "release_id": 50, "release_name": "Employment Situation"},
                               {"date": "2026-10-02", "release_id": 999, "release_name": "Minor"}]}
        ev = mc.econ_events(date(2026, 9, 30), date(2026, 10, 6), get_json=lambda url, p=None: j, api_key="k")
        assert [(e["title"], e["time_et"]) for e in ev] == [("雇用統計", None)]   # 時刻は推測で付けない

    def test_holidays_and_early_close(self):
        ev = mc.holiday_events(date(2026, 11, 23), date(2026, 11, 27))
        titles = {(e.get("date_et") or e.get("date_local"), e["title"]) for e in ev}
        assert ("2026-11-26", "米国市場 休場") in titles
        assert ("2026-11-27", "米国市場 短縮取引（13:00 ET 引け）") in titles
        assert ("2026-11-23", "日本市場 休場") in titles   # 勤労感謝の日

    def test_expiry_moves_before_holiday(self):
        ev = mc.expiry_events(date(2026, 6, 15), date(2026, 6, 19))
        us = [e for e in ev if e["region"] == "米国"]
        # 2026-06-19（第3金曜）はJuneteenthで休場 → 前営業日の06-18。6月は同時満期
        assert us and us[0]["date_et"] == "2026-06-18" and "同時満期" in us[0]["title"]
        jp = mc.expiry_events(date(2026, 6, 8), date(2026, 6, 12))
        assert any(e["region"] == "日本" and e["date_local"] == "2026-06-12" and "メジャーSQ" in e["title"] for e in jp)

    def test_earnings_limited_to_given_tickers(self, tmp_path):
        for t, d in (("AAA", "2026-10-02"), ("ZZZ", "2026-10-02")):
            p = tmp_path / "docs" / "value-monitor" / "tanuki_valuation" / "data" / t
            p.mkdir(parents=True)
            (p / "latest.json").write_text(json.dumps({"next_earnings_date": d}), encoding="utf-8")
        ev = mc.earnings_events(date(2026, 9, 30), date(2026, 10, 6), ["AAA", "BBB"], str(tmp_path),
                                get_calendar=lambda t: {"earnings_date": ["2026-10-05"]} if t == "BBB" else {})
        assert sorted(e["title"] for e in ev) == ["AAA 決算発表", "BBB 決算発表"]   # ZZZは対象外

    def test_build_calendar_records_failures(self, tmp_path):
        def bad(url, params=None):
            raise ConnectionError("down")
        c = mc.build_calendar(str(tmp_path), [], now=datetime(2026, 9, 30, 12, tzinfo=timezone.utc), get_json=bad)
        assert "経済指標（FRED）" in c["failed"] and "FOMC・FRB要人（Federal Reserve）" in c["failed"]
        assert c["status"] == "partial"   # 祝日・満期・決算は取得できる


class TestFutures:
    def test_snapshot_ok_and_failure(self):
        def quote(sym):
            if sym == "NIY=F":
                raise RuntimeError("no data")
            return {"value": 100.0, "previous_close": 99.0, "bar_time": datetime(2026, 9, 30, 4, 45, tzinfo=timezone.utc),
                    "contract": "ESZ26.CME" if sym == "ES=F" else None}
        f = fs.snapshot(quote, now=datetime(2026, 9, 30, 5, tzinfo=timezone.utc))
        by = {x["symbol"]: x for x in f["items"]}
        assert by["ES=F"]["change_pct"] == 1.01 and by["ES=F"]["bar_time_utc"] == "2026-09-30T04:45:00Z"
        assert by["NIY=F"]["status"] == "取得できず" and by["NIY=F"]["value"] is None
        assert f["failed"] == ["NIY=F"] and f["status"] == "partial" and f["fetched_at"] == "2026-09-30T05:00:00Z"


class TestDataQualityAndAI:
    def _base(self):
        ind = {k: {"change_percent": 0.1, "date": "2026-09-29"} for k in sc.DQ_INDICATORS}
        af = {k: {"change_pct": 0.1, "date": "2026-09-29"} for k in sc.DQ_ASSET_FLOW}
        return ind, af

    def test_unavailable_makes_partial_other_stages_unaffected(self):
        ind, af = self._base()
        ok = sc.build_stage_conclusions(ind, af, {"date": "2026-09-29"}, "2026-09-29")
        r = sc.build_stage_conclusions(ind, af, {"date": "2026-09-29"}, "2026-09-29",
                                       implc={"headlines": {"failed": ["CNBC Markets"]}, "futures": {"failed": []},
                                              "calendar": {"failed": ["経済指標（FRED）"]}})
        dq = r["data_quality"]
        assert dq["status"] == "partial" and dq["unavailable"] == ["ニュースの見出し: CNBC Markets", "予定: 経済指標（FRED）"]
        assert dq["old_stages"] == [2, 8] and r["stages"]["0"]["line"] == "2026-09-29の終値（一部の要素が取得できず）"
        for k in ("1", "2", "3", "4", "5"):
            assert r["stages"][k] == ok["stages"][k]

    def test_ai_facts_do_not_include_news(self):
        stage = {"stages": {"2": {"line": "同時に大きく動いたもの：なし"}}, "weather": {"label": "曇り"},
                 "data_quality": {"status": "complete", "as_of": {}}}
        f = cs.build_ai_facts(stage, {}, {"score": 50.0}, None, None, None)
        s = json.dumps(f, ensure_ascii=False)
        assert "headlines" not in s and "見出し" not in s and "calendar" not in s
