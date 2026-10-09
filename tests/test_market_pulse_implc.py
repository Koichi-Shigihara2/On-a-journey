"""Market Pulse 実装C（指示書㉗）: ニュースの見出し・予定・先物の最新値の単体テスト。外部取得はすべてモック。

取得に失敗した要素が「取得できず」（failed・status）になり他の段階に影響しないこと、data_qualityに反映されること、
予定の日付変換（ET→JST、夏時間の切り替え）、祝日・短縮取引、満期（祝日なら前営業日）、決算の対象の限定、
ニュースがAIの入力に入らないことを確認する。
"""
import json
import os
import sys
from datetime import date, datetime, time, timedelta, timezone

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


NOW = datetime(2026, 9, 30, 0, 0, tzinfo=timezone.utc)   # RSSの最新記事（09-29 22:00 UTC）から2時間後


def _rss(items):
    """items: [(title, pubDate文字列, <source>の出典名 or None)]"""
    body = "".join(f"<item><title>{t}</title><link>https://e/{i}</link><pubDate>{d}</pubDate>"
                   + (f'<source url="https://p">{src}</source>' if src else "") + "</item>"
                   for i, (t, d, src) in enumerate(items))
    return f'<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>{body}</channel></rss>'.encode()


class TestHeadlines:
    def test_sources_us_only(self):
        # NHK 経済は外した（[[MARKETPULSE-HEADLINES-NHK-STALE-1]]、日本の配信元は置かない）
        assert [s["region"] for s in nh.SOURCES] == ["米国", "米国"]
        assert not any("nhk" in s["url"] for s in nh.SOURCES)
        assert nh.REGION_MAX == {"米国": nh.MAX_ITEMS}

    def test_ok_dedupe_and_fields(self):
        def fetch(url):
            return RSS.format(t1="Stocks rise", t2="Oil falls").encode()
        h = nh.fetch_headlines(fetch, now=NOW)
        assert h["status"] == "ok" and h["failed"] == [] and h["failed_reasons"] == {}
        titles = [x["title"] for x in h["items"]]
        assert titles.count("Stocks rise") == 1          # 同じ見出しは1件
        cnbc = [x for x in h["items"] if x["source"] == "CNBC Markets"][0]
        assert set(cnbc) == {"title", "published_utc", "link", "source", "region"}   # 見出し・時刻・リンク・出典のみ
        assert cnbc["published_utc"] == "2026-09-29T22:00:00Z"

    def test_partial_and_total_failure(self):
        def fetch_one_bad(url):
            if "cnbc" in url:
                raise ConnectionError("down")
            return RSS.format(t1="A " + url[-5:], t2="B " + url[-5:]).encode()
        h = nh.fetch_headlines(fetch_one_bad, now=NOW)
        assert h["status"] == "partial" and h["failed"] == ["CNBC Markets"] and h["items"]
        assert h["failed_reasons"] == {"CNBC Markets": "fetch_error"}
        h = nh.fetch_headlines(lambda url: (_ for _ in ()).throw(TimeoutError("x")), now=NOW)
        assert h["status"] == "failed" and h["items"] == [] and len(h["failed"]) == 2

    @pytest.mark.parametrize("hours,stale", [(72, False), (72.0003, True), (1, False), (24 * 60, True)])
    def test_stale_boundary_72h(self, hours, stale):
        # 配信元の最新記事が取得時刻から72時間より古ければstale（ちょうど72時間は古くない）
        newest = datetime(2026, 9, 29, 22, 0, tzinfo=timezone.utc)
        h = nh.fetch_headlines(lambda url: RSS.format(t1="S " + url[-5:], t2="T " + url[-5:]).encode(),
                               now=newest + timedelta(hours=hours))
        if stale:
            assert h["status"] == "failed" and h["items"] == []
            assert h["failed_reasons"] == {s["name"]: "stale" for s in nh.SOURCES}
        else:
            assert h["status"] == "ok" and h["failed"] == [] and len(h["items"]) == 4

    def test_one_stale_source_is_partial_and_its_items_dropped(self):
        # 2026-10-07〜09のNHKの再現: 取得は成功（200）するが記事が2か月前 → その配信元の見出しは使わずfailed（stale）
        def fetch(url):
            if "cnbc" in url:
                return _rss([("Old news", "Sat, 08 Aug 2026 06:10:11 GMT", None)])
            return _rss([("Fresh news", "Tue, 29 Sep 2026 22:00:00 GMT", None)])
        h = nh.fetch_headlines(fetch, now=NOW)
        assert h["status"] == "partial" and h["failed"] == ["CNBC Markets"] and h["failed_reasons"] == {"CNBC Markets": "stale"}
        assert [x["title"] for x in h["items"]] == ["Fresh news"]

    def test_no_dates_is_stale(self):
        # 時刻が1件も無ければ鮮度を確かめられないので、新しいとはみなさない
        body = "<item><title>No date</title><link>https://e/1</link></item>"
        h = nh.fetch_headlines(lambda url: f'<?xml version="1.0"?><rss version="2.0"><channel>{body}</channel></rss>'.encode(), now=NOW)
        assert h["status"] == "failed" and set(h["failed_reasons"].values()) == {"stale"}

    def test_region_cap_us_only(self):
        # 地域の上限は「米国 = MAX_ITEMS」（日本の枠は無い）。各配信元PER_SOURCE件ずつで、全体はMAX_ITEMS件以内
        def fetch(url):
            return _rss([(f"{url[-8:]} {i}", f"Tue, 29 Sep 2026 23:{i:02d}:00 GMT", None) for i in range(15)])
        h = nh.fetch_headlines(fetch, now=NOW + timedelta(hours=1))
        regions = [x["region"] for x in h["items"]]
        assert regions == ["米国"] * min(nh.PER_SOURCE * len(nh.SOURCES), nh.MAX_ITEMS)

    def test_region_cap_truncates_at_max_items(self, monkeypatch):
        monkeypatch.setattr(nh, "PER_SOURCE", 15)
        def fetch(url):
            return _rss([(f"{url[-8:]} {i}", f"Tue, 29 Sep 2026 23:{i:02d}:00 GMT", None) for i in range(15)])
        h = nh.fetch_headlines(fetch, now=NOW + timedelta(hours=1))
        assert len(h["items"]) == nh.MAX_ITEMS == 20

    def test_publisher_split_google_only(self):
        def fetch(url):
            if "google" in url:
                return _rss([("Fed holds rates - steady outlook - Reuters", "Tue, 29 Sep 2026 22:00:00 GMT", "Reuters"),
                             ("Oil - a 3% drop - CNBC", "Tue, 29 Sep 2026 21:00:00 GMT", None)])
            return _rss([("Dow - Nasdaq slip", "Tue, 29 Sep 2026 20:00:00 GMT", None)])
        h = nh.fetch_headlines(fetch, now=NOW)
        by = {x["title"]: x for x in h["items"]}
        assert by["Fed holds rates - steady outlook"]["publisher"] == "Reuters"   # <source>と一致する末尾だけを切る
        assert by["Oil - a 3% drop"]["publisher"] == "CNBC"                       # <source>が無いときは最後の「 - 」
        assert "publisher" not in by["Dow - Nasdaq slip"]                         # CNBCの見出しは切らない

    @pytest.mark.parametrize("title,src,exp", [
        ("A - B - WSJ", "WSJ", ("A - B", "WSJ")),
        ("Title without suffix", "Reuters", ("Title without suffix", "Reuters")),   # 末尾が一致しなければ切らない
        ("No dash at all", None, ("No dash at all", None)),
        (" - Bloomberg", "Bloomberg", (" - Bloomberg", "Bloomberg")),               # 見出しが空になる切り方はしない
    ])
    def test_split_publisher(self, title, src, exp):
        assert nh.split_publisher(title, src) == exp

    def test_title_whitespace_collapsed_like_browser(self):
        # 見出しの連続した空白（スペース・タブ・改行）は、ブラウザの表示と同じく1つにまとめて保存する（C-02の不一致の再発防止）。
        # 全角スペースはブラウザもまとめないので残す
        def fetch(url):
            return RSS.format(t1="Gen Z.  Here’s why \t experts\n worry", t2="日本　　経済").encode()
        h = nh.fetch_headlines(fetch, now=NOW)
        titles = {x["title"] for x in h["items"]}
        assert "Gen Z. Here’s why experts worry" in titles
        assert "日本　　経済" in titles


def _resp(content):
    return {"choices": [{"message": {"content": content}}]}


class TestHeadlineTranslation:
    """[[MARKETPULSE-HEADLINES-JA-1]]: 見出しの日本語訳。外部APIはすべてモック。"""

    def test_ok_sends_titles_only(self):
        sent = {}

        def post(payload, key):
            sent.update(payload)
            return _resp('```json\n["株が上昇", "原油が下落"]\n```')
        assert nh.translate_titles(["Stocks rise", "Oil falls"], post=post, api_key="k") == ["株が上昇", "原油が下落"]
        assert sent["temperature"] == 0 and sent["model"] == nh.GROK_MODEL
        prompt = sent["messages"][0]["content"]
        assert '["Stocks rise", "Oil falls"]' in prompt and "http" not in prompt   # 見出しの文字だけ（リンク等は渡さない）

    def test_no_key_returns_none_without_call(self):
        def post(payload, key):
            raise AssertionError("呼ばれてはいけない")
        assert nh.translate_titles(["a"], post=post, api_key="") is None

    @pytest.mark.parametrize("content", ['["一件だけ"]', '["1", "2", "3"]', "訳せません", '{"a": 1}', '["訳", ""]', '["訳", null]'])
    def test_count_or_format_mismatch_returns_none(self, content):
        # 件数が合わない・形式が違うときは全件Noneにする（訳と見出しの取り違えを防ぐ）
        assert nh.translate_titles(["a", "b"], post=lambda p, k: _resp(content), api_key="k") is None

    def test_api_error_returns_none(self):
        def post(payload, key):
            raise ConnectionError("down")
        assert nh.translate_titles(["a"], post=post, api_key="k") is None

    def test_add_title_ja_ok_keeps_original(self):
        h = {"items": [{"title": "Stocks rise"}, {"title": "Oil falls"}]}
        nh.add_title_ja(h, translate=lambda ts: ["株が上昇", "原油が下落"])
        assert [(x["title"], x["title_ja"]) for x in h["items"]] == [("Stocks rise", "株が上昇"), ("Oil falls", "原油が下落")]
        assert h["translation"]["status"] == "ok"

    @pytest.mark.parametrize("translate", [lambda ts: None, lambda ts: ["一件だけ"],
                                           lambda ts: (_ for _ in ()).throw(RuntimeError("x"))])
    def test_add_title_ja_failure_falls_back_to_null(self, translate):
        # 訳せなかったときはtitle_jaをnullにし（画面は原文だけ）、例外は外に出さない（毎晩の実行を止めない）
        h = {"items": [{"title": "Stocks rise"}, {"title": "Oil falls"}]}
        nh.add_title_ja(h, translate=translate)
        assert [x["title_ja"] for x in h["items"]] == [None, None]
        assert [x["title"] for x in h["items"]] == ["Stocks rise", "Oil falls"]
        assert h["translation"]["status"] == "failed"

    def test_add_title_ja_no_items_skips(self):
        def translate(ts):
            raise AssertionError("呼ばれてはいけない")
        h = {"items": [], "status": "failed"}
        nh.add_title_ja(h, translate=translate)
        assert h["translation"] == {"status": "skipped"}

    def test_stale_reason_shown_in_data_quality(self):
        r = sc.build_stage_conclusions({}, None, None, None,
                                       implc={"headlines": {"failed": ["CNBC Markets", "Google News ビジネス（US）"],
                                                            "failed_reasons": {"CNBC Markets": "stale",
                                                                               "Google News ビジネス（US）": "fetch_error"}}})
        assert r["data_quality"]["unavailable"] == ["ニュースの見出し: CNBC Markets（更新停止）",
                                                    "ニュースの見出し: Google News ビジネス（US）"]


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

    def test_snapshot_marks_intraday_values_by_day_boundary(self):
        # 取引中の15分足の値: 先物は「清算前」、ドル円は「日中」（fetcher.expected_provisional_kind、日足の区切りの定義から判定）
        def quote(sym):
            return {"value": 100.0, "previous_close": 99.0, "bar_time": datetime(2026, 9, 30, 4, 45, tzinfo=timezone.utc)}
        by = {x["symbol"]: x for x in fs.snapshot(quote, now=datetime(2026, 9, 30, 5, tzinfo=timezone.utc))["items"]}
        for sym in ("ES=F", "NQ=F", "NIY=F"):
            assert by[sym]["provisional"] is True and by[sym]["provisional_kind"] == "清算前", sym
        assert by["JPY=X"]["provisional_kind"] == "日中"


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
