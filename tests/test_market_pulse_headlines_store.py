"""Market Pulse の見出しの日ごとの積み足し（[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]] ①、2026-11-01）の試験。外部取得はすべてモック。

- リンクの正規化（3本の実際のリンクの形）・題名での重複・also_in・title_updates
- 米国東部の日付の振り分け（夏時間・冬時間の境目、時刻の無い記事）
- 止まった配信元（stale）を積まない・新しい記事が無い回は書き込まない
- 夜の回の訳・判定を写す・market_data.json に all_items を保存しない（compute_implc）
- System Health の [N] の閾値（12時間・24時間）
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_MARKET_PULSE_DIR = os.path.join(_REPO_ROOT, "src", "market", "market_pulse")
for _p in (_REPO_ROOT, _MARKET_PULSE_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import headlines_store as hs  # noqa: E402
import headlines_fetch as hf  # noqa: E402
import news_headlines as nh  # noqa: E402
from common import system_health as sh  # noqa: E402

# 2026-11-01 に取得した3本の実際のリンクの形
CNBC = "https://www.cnbc.com/investingclub/2026/10/31/stocks-started-the-week-with-promise-but-ended-in-despair.html"
YAHOO = ("https://247wallst.com/personal-finance/2026/11/01/trade-futures-or-sp-500-index-options-the-irs-treats-60-of-your-gains"
         "-as-long-term-even-on-a-day-trade/?.tsrc=rss")
GOOGLE = "https://news.google.com/rss/articles/CBMirgFBVV95cUxPZjBKNHBsSy1EQlN4NnM4QXQ0OXNDeFlzUnY1bVVtYVB3RU9ZSk5vOGU0?oc=5"

SRC = {"CNBC Earnings": "ok", "Yahoo Finance S&P 500": "ok", "Google News ビジネス（US）": "ok"}


def _it(title, link, pub, source="CNBC Earnings", **kw):
    return dict({"title": title, "link": link, "published_utc": pub, "source": source, "region": "米国"}, **kw)


def _read(d, date):
    with open(os.path.join(d, f"{date}.json"), encoding="utf-8") as f:
        return json.load(f)


# ── 正規化 ──────────────────────────────────────────────────────────
class TestNormalize:
    def test_cnbc_unchanged(self):
        assert hs.normalize_link(CNBC) == CNBC

    def test_yahoo_drops_tsrc(self):
        assert hs.normalize_link(YAHOO) == YAHOO[: -len("?.tsrc=rss")]

    def test_google_drops_oc_keeps_article_id(self):
        assert hs.normalize_link(GOOGLE) == GOOGLE[: -len("?oc=5")]

    @pytest.mark.parametrize("url,exp", [
        ("HTTPS://WWW.CNBC.COM/a.html#x", "https://www.cnbc.com/a.html"),                         # ホスト・スキームを小文字、フラグメントを外す
        ("https://e.com/a?utm_source=rss&id=7&UTM_Medium=x", "https://e.com/a?id=7"),              # utm_* を外し、ほかは残す
        ("https://e.com/A/Path?b=2&a=1", "https://e.com/A/Path?b=2&a=1"),                          # パスの大文字・クエリの順は変えない
        ("https://finance.yahoo.com/news/x.html?.tsrc=rss&p=1", "https://finance.yahoo.com/news/x.html?p=1"),
    ])
    def test_rules(self, url, exp):
        assert hs.normalize_link(url) == exp


# ── 積み足し ────────────────────────────────────────────────────────
class TestAccumulate:
    def test_new_items_and_fetch_record(self, tmp_path):
        r = hs.accumulate([_it("A", CNBC, "2026-10-30T13:29:50Z"), _it("B", YAHOO, "2026-10-30T14:00:00Z", "Yahoo Finance S&P 500")],
                          fetched_at="2026-10-30T18:40:00Z", run="scheduled", sources=SRC, data_dir=str(tmp_path))
        assert r == {"written": ["2026-10-30"], "new": 2, "updated": 0, "by_date": {"2026-10-30": 2}}
        doc = _read(tmp_path, "2026-10-30")
        assert doc["date_et"] == "2026-10-30"
        assert [x["title"] for x in doc["items"]] == ["B", "A"]     # 時刻の新しい順
        a = doc["items"][1]
        assert a == {"id": hs.item_id(CNBC), "title": "A", "published_utc": "2026-10-30T13:29:50Z", "link": CNBC,
                     "link_norm": CNBC, "source": "CNBC Earnings", "first_seen": "2026-10-30T18:40:00Z"}
        assert doc["fetches"] == [{"fetched_at": "2026-10-30T18:40:00Z", "run": "scheduled", "sources": SRC,
                                   "new": 2, "updated": 0, "new_by_date": {"2026-10-30": 2}}]

    def test_second_run_same_items_writes_nothing(self, tmp_path):
        items = [_it("A", CNBC, "2026-10-30T13:29:50Z"), _it("B", YAHOO, "2026-10-30T14:00:00Z", "Yahoo Finance S&P 500")]
        hs.accumulate(items, fetched_at="2026-10-30T18:40:00Z", run="scheduled", sources=SRC, data_dir=str(tmp_path))
        before = (tmp_path / "2026-10-30.json").read_bytes()
        # Yahooの.tsrcが違っても同じ記事（正規化したリンクで比べる）
        again = [dict(items[0]), dict(items[1], link=YAHOO.replace("?.tsrc=rss", "?.tsrc=rss2"))]
        r = hs.accumulate(again, fetched_at="2026-10-31T00:40:00Z", run="scheduled", sources=SRC, data_dir=str(tmp_path))
        assert r == {"written": [], "new": 0, "updated": 0, "by_date": {}}
        assert (tmp_path / "2026-10-30.json").read_bytes() == before
        assert sorted(os.listdir(tmp_path)) == ["2026-10-30.json"]   # 取得の日（10-30 ET）のファイルも作らない

    def test_same_title_other_link_goes_to_also_in(self, tmp_path):
        # Googleの中継リンク（CNBCの記事と同じ題名、出典名はnews_headlinesが切った後）
        items = [_it("Intel stock plunges 21%", CNBC, "2026-10-30T00:39:52Z"),
                 _it("Intel  Stock plunges 21%", GOOGLE, "2026-10-30T00:41:00Z", "Google News ビジネス（US）", publisher="CNBC")]
        r = hs.accumulate(items, fetched_at="2026-10-30T06:40:00Z", run="scheduled", sources=SRC, data_dir=str(tmp_path))
        assert r["new"] == 1 and r["updated"] == 1
        (x,) = _read(tmp_path, "2026-10-29")["items"]      # 00:39 UTC は 10-29 20:39 EDT
        assert x["source"] == "CNBC Earnings"
        assert x["also_in"] == [{"source": "Google News ビジネス（US）", "link": GOOGLE}]
        # 次の回で同じものが来ても also_in は増えない
        r = hs.accumulate(items, fetched_at="2026-10-30T12:40:00Z", run="scheduled", sources=SRC, data_dir=str(tmp_path))
        assert r["written"] == [] and len(_read(tmp_path, "2026-10-29")["items"][0]["also_in"]) == 1

    def test_same_link_other_source_goes_to_also_in(self, tmp_path):
        hs.accumulate([_it("A", CNBC, "2026-10-30T15:00:00Z"), _it("A", CNBC + "#top", "2026-10-30T15:00:00Z", "Yahoo Finance S&P 500")],
                      fetched_at="2026-10-30T18:40:00Z", run="scheduled", sources=SRC, data_dir=str(tmp_path))
        (x,) = _read(tmp_path, "2026-10-30")["items"]
        assert x["also_in"] == [{"source": "Yahoo Finance S&P 500", "link": CNBC + "#top"}]

    def test_title_change_goes_to_title_updates(self, tmp_path):
        hs.accumulate([_it("Old title", CNBC, "2026-10-30T15:00:00Z")], fetched_at="2026-10-30T18:40:00Z", run="scheduled",
                      sources=SRC, data_dir=str(tmp_path))
        r = hs.accumulate([_it("New title", CNBC, "2026-10-30T15:00:00Z")], fetched_at="2026-10-31T00:40:00Z", run="scheduled",
                          sources=SRC, data_dir=str(tmp_path))
        assert r["updated"] == 1 and r["new"] == 0
        (x,) = _read(tmp_path, "2026-10-30")["items"]
        assert x["title"] == "Old title"                   # 最初の題名を残す
        assert x["title_updates"] == [{"title": "New title", "seen_at": "2026-10-31T00:40:00Z"}]
        # 変更があった回は、取得の日（10-30 20:40 EDT → 10-30）のファイルに記録が残る
        assert [f["fetched_at"] for f in _read(tmp_path, "2026-10-30")["fetches"]] == ["2026-10-30T18:40:00Z", "2026-10-31T00:40:00Z"]
        r = hs.accumulate([_it("New title", CNBC, "2026-10-30T15:00:00Z")], fetched_at="2026-10-31T06:40:00Z", run="scheduled",
                          sources=SRC, data_dir=str(tmp_path))
        assert r["written"] == []                          # 同じ題名の変更は2回足さない

    @pytest.mark.parametrize("pub,exp", [
        # 冬時間に入る境目（2026-11-01 06:00 UTC = 02:00 EDT → 01:00 EST）
        ("2026-11-01T03:59:59Z", "2026-10-31"), ("2026-11-01T04:00:00Z", "2026-11-01"),   # EDT（UTC−4）
        ("2026-11-02T04:59:59Z", "2026-11-01"), ("2026-11-02T05:00:00Z", "2026-11-02"),   # EST（UTC−5）
        # 夏時間に入る境目（2026-03-08 07:00 UTC）
        ("2026-03-08T04:59:59Z", "2026-03-07"), ("2026-03-08T05:00:00Z", "2026-03-08"),   # EST
        ("2026-03-09T03:59:59Z", "2026-03-08"), ("2026-03-09T04:00:00Z", "2026-03-09"),   # EDT
    ])
    def test_et_date_dst_boundaries(self, tmp_path, pub, exp):
        r = hs.accumulate([_it("A", CNBC, pub)], fetched_at="2026-11-02T12:40:00Z", run="scheduled", sources=SRC,
                          data_dir=str(tmp_path))
        assert r["by_date"] == {exp: 1}
        assert _read(tmp_path, exp)["items"][0]["published_utc"] == pub

    def test_no_time_goes_to_fetch_date_with_flag(self, tmp_path):
        # 取得 2026-11-02 03:00 UTC = 11-01 22:00 EST → 11-01 のファイル
        r = hs.accumulate([_it("No date", CNBC, None)], fetched_at="2026-11-02T03:00:00Z", run="scheduled", sources=SRC,
                          data_dir=str(tmp_path))
        assert r["by_date"] == {"2026-11-01": 1}
        (x,) = _read(tmp_path, "2026-11-01")["items"]
        assert x["published_missing"] is True and x["published_utc"] is None

    def test_items_split_into_two_files_and_fetch_in_fetch_date(self, tmp_path):
        items = [_it("Night", CNBC, "2026-10-29T22:00:00Z"), _it("Morning", YAHOO, "2026-10-30T11:00:00Z", "Yahoo Finance S&P 500")]
        r = hs.accumulate(items, fetched_at="2026-10-30T12:40:00Z", run="scheduled", sources=SRC, data_dir=str(tmp_path))
        assert r["by_date"] == {"2026-10-29": 1, "2026-10-30": 1}
        assert _read(tmp_path, "2026-10-29")["fetches"] == []          # 取得の記録は取得の日（10-30 ET）だけ
        assert len(_read(tmp_path, "2026-10-30")["fetches"]) == 1

    def test_annotations_copied_and_filled_later(self, tmp_path):
        # 6時間ごとの回（訳なし）の後に、夜の回が訳・判定を写す
        hs.accumulate([_it("A", CNBC, "2026-11-02T15:00:00Z")], fetched_at="2026-11-02T18:40:00Z", run="scheduled", sources=SRC,
                      data_dir=str(tmp_path))
        ann = {hs.normalize_link(CNBC): {"title_ja": "A（訳）", "market": True},
               hs.normalize_link(YAHOO): {"title_ja": None, "market": None}}
        r = hs.accumulate([_it("A", CNBC, "2026-11-02T15:00:00Z"), _it("B", YAHOO, "2026-11-02T20:00:00Z", "Yahoo Finance S&P 500")],
                          fetched_at="2026-11-02T21:30:00Z", run="evening", sources=SRC, annotations=ann, data_dir=str(tmp_path))
        assert r["new"] == 1 and r["updated"] == 1
        by = {x["title"]: x for x in _read(tmp_path, "2026-11-02")["items"]}
        assert by["A"]["title_ja"] == "A（訳）" and by["A"]["market"] is True
        assert "title_ja" not in by["B"] and "market" not in by["B"]   # 訳せなかったもの（None）は写さない

    def test_bad_fetched_at_raises(self, tmp_path):
        with pytest.raises(ValueError):
            hs.accumulate([], fetched_at="2026-11-02 12:40", run="x", sources={}, data_dir=str(tmp_path))


# ── 取得から積み足しまで（fetch_headlines の all_items） ─────────────────
def _rss(items):
    body = "".join(f"<item><title>{t}</title><link>{l}</link><pubDate>{d}</pubDate></item>" for t, l, d in items)
    return f'<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>{body}</channel></rss>'.encode()


NOW = datetime(2026, 11, 2, 12, 40, tzinfo=timezone.utc)


_REAL_FETCH = nh.fetch_headlines


def _fetch_with(stale_source=None, n=20):
    def fetch(url):
        k = [s["url"] for s in nh.SOURCES].index(url)
        if stale_source and nh.SOURCES[k]["name"] == stale_source:
            return _rss([("Old", "https://e.com/old", "Sat, 08 Aug 2026 06:10:11 GMT")])
        return _rss([(f"src{k} title {i}", f"https://e.com/{k}/{i}", f"Mon, 02 Nov 2026 {i % 12:02d}:{i:02d}:00 GMT")
                     for i in range(n)])
    return fetch


class TestFetchToStore:
    def test_all_items_not_cut_and_not_deduped(self):
        h = nh.fetch_headlines(_fetch_with(n=20), now=NOW)
        assert len(h["items"]) == 48 and len(h["all_items"]) == 60       # 切る前の全件（各20件×3本）
        assert {x["source"] for x in h["all_items"]} == {s["name"] for s in nh.SOURCES}

    def test_stale_source_not_accumulated(self, tmp_path):
        h = nh.fetch_headlines(_fetch_with(stale_source="Yahoo Finance S&P 500", n=5), now=NOW)
        assert h["failed_reasons"] == {"Yahoo Finance S&P 500": "stale"}
        assert all(x["source"] != "Yahoo Finance S&P 500" for x in h["all_items"])
        r = hs.store_from_fetch(h, "scheduled", data_dir=str(tmp_path))
        assert r["new"] == 10 and not os.path.exists(tmp_path / "2026-08-08.json")   # 止まった配信元の古い日付のファイルを作らない
        f = _read(tmp_path, "2026-11-02")["fetches"][0]
        assert f["sources"] == {"CNBC Earnings": "ok", "Yahoo Finance S&P 500": "stale", "Google News ビジネス（US）": "ok"}

    def test_headlines_fetch_main_twice(self, tmp_path, monkeypatch):
        monkeypatch.setattr(nh, "fetch_headlines", lambda: _REAL_FETCH(_fetch_with(n=5), now=NOW))
        assert hf.main(["--data-dir", str(tmp_path)]) == 0
        first = {n: (tmp_path / n).read_bytes() for n in os.listdir(tmp_path)}
        assert sum(len(_read(tmp_path, n[:-5])["items"]) for n in first) == 15
        assert hf.main(["--data-dir", str(tmp_path)]) == 0
        assert {n: (tmp_path / n).read_bytes() for n in os.listdir(tmp_path)} == first    # 2回目は何も書かない

    def test_headlines_fetch_all_failed_exits_1(self, tmp_path, monkeypatch):
        monkeypatch.setattr(nh, "fetch_headlines",
                            lambda: _REAL_FETCH(lambda url: (_ for _ in ()).throw(TimeoutError("x")), now=NOW))
        assert hf.main(["--data-dir", str(tmp_path)]) == 1
        assert os.listdir(tmp_path) == []



# ── 夜の回（compute_implc）: all_items を market_data.json に残さず、訳・判定を写す ─────
def test_compute_implc_pops_all_items_and_stores(monkeypatch):
    import collect_and_send as cs
    import market_calendar as mc
    import futures_snapshot as fs
    import sector_rotation as sr
    h = {"fetched_at": "2026-11-02T21:30:00Z", "items": [_it("A", CNBC, "2026-11-02T15:00:00Z")], "failed": [],
         "failed_reasons": {}, "status": "ok", "sources": list(SRC), "all_items": [_it("A", CNBC, "2026-11-02T15:00:00Z")]}
    monkeypatch.setattr(nh, "fetch_headlines", lambda: h)

    def annotate(x):
        for it in x["items"]:
            it["title_ja"], it["market"] = "A（訳）", True
        return x
    monkeypatch.setattr(nh, "annotate_headlines", annotate)
    monkeypatch.setattr(mc, "build_calendar", lambda *a, **k: {"events": [], "failed": [], "status": "ok"})
    monkeypatch.setattr(fs, "snapshot", lambda: {"items": [], "failed": [], "status": "ok"})
    monkeypatch.setattr(sr, "watch_tickers", lambda root: {"all": []})
    got = []
    monkeypatch.setattr(hs, "store_from_fetch", lambda hh, run, annotate=False: got.append((hh, run, annotate)) or
                        {"new": 1, "updated": 0, "written": ["2026-11-02"]})
    out = cs.compute_implc()
    assert "all_items" not in out["headlines"]
    (hh, run, ann), = got
    assert run == "evening" and ann is True and len(hh["all_items"]) == 1 and hh["items"][0]["title_ja"] == "A（訳）"


def test_compute_implc_store_failure_does_not_stop(monkeypatch):
    import collect_and_send as cs
    import market_calendar as mc
    import futures_snapshot as fs
    import sector_rotation as sr
    monkeypatch.setattr(nh, "fetch_headlines", lambda: {"fetched_at": "2026-11-02T21:30:00Z", "items": [], "failed": [],
                                                        "failed_reasons": {}, "status": "ok", "sources": [],
                                                        "all_items": [_it("A", CNBC, None)]})
    monkeypatch.setattr(nh, "annotate_headlines", lambda x: x)
    monkeypatch.setattr(mc, "build_calendar", lambda *a, **k: {"events": [], "failed": [], "status": "ok"})
    monkeypatch.setattr(fs, "snapshot", lambda: {"items": [], "failed": [], "status": "ok"})
    monkeypatch.setattr(sr, "watch_tickers", lambda root: {"all": []})
    monkeypatch.setattr(hs, "store_from_fetch", lambda *a, **k: (_ for _ in ()).throw(OSError("disk")))
    out = cs.compute_implc()
    assert "all_items" not in out["headlines"] and "calendar" in out


# ── System Health [N] ───────────────────────────────────────────────
def _write_fetch(d, date, fetched_at):
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{date}.json"), "w", encoding="utf-8") as f:
        json.dump({"date_et": date, "fetches": [{"fetched_at": fetched_at, "run": "scheduled"}], "items": []}, f)


LAST = datetime(2026, 11, 2, 12, 40, tzinfo=timezone.utc)


class TestSystemHealthN:
    @pytest.mark.parametrize("hours,ok,critical,icon", [
        (1, True, False, "✅"), (12, True, False, "✅"),                 # ちょうど12時間は古くない
        (12.01, False, False, "⚠️"), (24, False, False, "⚠️"),          # 12時間より古い → ⚠️（ちょうど24時間は⚠️のまま）
        (24.01, False, True, "🔴"), (72, False, True, "🔴"),            # 24時間より古い → 🔴
    ])
    def test_thresholds(self, tmp_path, hours, ok, critical, icon):
        assert sh.HEADLINES_WARN_HOURS == 12 and sh.HEADLINES_NG_HOURS == 24
        _write_fetch(str(tmp_path), "2026-11-02", "2026-11-02T12:40:00Z")
        r = sh._check_n(data_dir=str(tmp_path), now=LAST + timedelta(hours=hours))
        assert (r["ok"], r["critical"]) == (ok, critical) and r["label"].startswith(icon)
        assert sh._item_severity(r["label"], r["ok"]) == (0 if ok else (2 if critical else 1))

    def test_latest_across_files(self, tmp_path):
        _write_fetch(str(tmp_path), "2026-11-01", "2026-11-02T03:00:00Z")
        _write_fetch(str(tmp_path), "2026-11-02", "2026-11-02T12:40:00Z")
        # 未来の日付の記事だけのファイル（取得の記録なし）があっても、取得の記録を拾う
        with open(tmp_path / "2026-11-03.json", "w", encoding="utf-8") as f:
            json.dump({"date_et": "2026-11-03", "fetches": [], "items": []}, f)
        r = sh._check_n(data_dir=str(tmp_path), now=LAST + timedelta(hours=1))
        assert r["ok"] and "11-02 12:40 UTC" in r["detail"]

    def test_no_dir_is_skip(self, tmp_path):
        r = sh._check_n(data_dir=str(tmp_path / "none"), now=LAST)
        assert r["ok"] is True and "未作成" in r["label"]

    def test_no_fetches_is_red(self, tmp_path):
        with open(tmp_path / "2026-11-02.json", "w", encoding="utf-8") as f:
            json.dump({"date_et": "2026-11-02", "fetches": [], "items": []}, f)
        r = sh._check_n(data_dir=str(tmp_path), now=LAST)
        assert r["critical"] is True and not r["ok"]

    def test_unreadable_is_red(self, tmp_path):
        (tmp_path / "2026-11-02.json").write_text("{", encoding="utf-8")
        r = sh._check_n(data_dir=str(tmp_path), now=LAST)
        assert r["critical"] is True

    def test_store_output_is_read_by_check(self, tmp_path):
        # 積み足しが書いた fetches を [N] がそのまま読める（書き手と読み手の形が一致）
        hs.accumulate([_it("A", CNBC, "2026-11-02T11:00:00Z")], fetched_at="2026-11-02T12:40:00Z", run="scheduled",
                      sources=SRC, data_dir=str(tmp_path))
        r = sh._check_n(data_dir=str(tmp_path), now=LAST + timedelta(hours=2))
        assert r["ok"] and "scheduled" in r["detail"]

    def test_one_line_label(self):
        assert "Headlines⚠️x" in sh.build_one_line("2026-11-02", {"N": {"ok": False, "short": "x"}})

    def test_default_dir_matches_store(self):
        assert os.path.normpath(sh._HEADLINES_DIR) == os.path.normpath(hs.HEADLINES_DIR)
