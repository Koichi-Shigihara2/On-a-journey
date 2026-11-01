"""Market Pulse の5分足の日ごとの保存（intraday_store.py、[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]] ②、2026-11-01）の試験。
外部取得はすべてモック（fetch を差し替える）。

- 時刻帯の統一（シカゴ→ET）、夏時間・冬時間の境目
- 取引の終わっていない日（ET の今日の引け前）を書かない
- 欠けた日だけ取り直す・足を減らさない
- 1本が取れないときの欠けの記録（ほかは書く）
- 何度動かしても同じ
- System Health の [O]
"""
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_MARKET_PULSE_DIR = os.path.join(_REPO_ROOT, "src", "market", "market_pulse")
for _p in (_REPO_ROOT, _MARKET_PULSE_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import intraday_store as ist  # noqa: E402
from common import system_health as sh  # noqa: E402

ET = ZoneInfo("America/New_York")
CHI = ZoneInfo("America/Chicago")
UTC = timezone.utc


def _bar(ts, c=100.0, v=10):
    return {"ts": ts, "open": c, "high": c + 1, "low": c - 1, "close": c, "volume": v}


def _bars_for(day, tz=ET, start="09:30", end="16:00", step=5, c0=100.0):
    """その日（tz の時刻で）start〜end の5分足。"""
    d = date.fromisoformat(day)
    h, m = map(int, start.split(":"))
    t = datetime(d.year, d.month, d.day, h, m, tzinfo=tz)
    eh, em = map(int, end.split(":"))
    stop = datetime(d.year, d.month, d.day, eh, em, tzinfo=tz)
    out, i = [], 0
    while t <= stop:
        out.append(_bar(t, c0 + i))
        t += timedelta(minutes=step)
        i += 1
    return out


def _fetch_all(days, fail=(), calls=None):
    """全10本に、指定の日の 09:30〜16:00 ET の足を返す fetch。fail の銘柄は例外。"""
    def fetch(sym, start, end):
        if calls is not None:
            calls.append((sym, start, end))
        if sym in fail:
            raise RuntimeError("Yahoo error")
        tz = CHI if sym in ("^VIX", "^TNX") else ET
        out = []
        for d in days:
            if start <= date.fromisoformat(d) < end:
                # シカゴ時間で返す銘柄は、ETの09:30〜16:00 をシカゴの時刻で返す（中身は同じ時刻）
                out += [dict(b, ts=b["ts"].astimezone(tz)) for b in _bars_for(d)]
        return out
    return fetch


def _read(d, day):
    with open(os.path.join(d, f"{day}.json"), encoding="utf-8") as f:
        return json.load(f)


# 2026-11-03（火）21:30 UTC = 16:30 EST（引けの後）
NOW = datetime(2026, 11, 3, 21, 30, tzinfo=UTC)


class TestSplit:
    def test_chicago_converted_to_et(self):
        # ^VIX・^TNX はシカゴ時間で返る。08:30 CST（冬時間）= 09:30 EST
        bars = [_bar(datetime(2026, 11, 3, 8, 30, tzinfo=CHI)), _bar(datetime(2026, 11, 3, 15, 0, tzinfo=CHI))]
        out = ist.split_by_et_date(bars)
        assert out == {"2026-11-03": {"t": ["09:30", "16:00"], "o": [100.0, 100.0], "h": [101.0, 101.0], "l": [99.0, 99.0],
                                      "c": [100.0, 100.0], "v": [10, 10]}}

    def test_chicago_summer(self):
        # 夏時間: 08:30 CDT = 09:30 EDT（2026-10-30）
        out = ist.split_by_et_date([_bar(datetime(2026, 10, 30, 8, 30, tzinfo=CHI))])
        assert out["2026-10-30"]["t"] == ["09:30"]

    @pytest.mark.parametrize("utc,exp_date,exp_t", [
        # 冬時間に入る境目（2026-11-01 06:00 UTC）。先物の夜の足は ET の日付で振り分ける
        (datetime(2026, 11, 1, 3, 55, tzinfo=UTC), "2026-10-31", "23:55"),   # EDT（UTC−4）
        (datetime(2026, 11, 1, 4, 0, tzinfo=UTC), "2026-11-01", "00:00"),
        (datetime(2026, 11, 2, 4, 55, tzinfo=UTC), "2026-11-01", "23:55"),   # EST（UTC−5）
        (datetime(2026, 11, 2, 5, 0, tzinfo=UTC), "2026-11-02", "00:00"),
        # 夏時間に入る境目（2026-03-08 07:00 UTC）
        (datetime(2026, 3, 8, 4, 55, tzinfo=UTC), "2026-03-07", "23:55"),
        (datetime(2026, 3, 9, 3, 55, tzinfo=UTC), "2026-03-08", "23:55"),
        (datetime(2026, 3, 9, 4, 0, tzinfo=UTC), "2026-03-09", "00:00"),
    ])
    def test_et_date_dst_boundaries(self, utc, exp_date, exp_t):
        assert ist.split_by_et_date([_bar(utc)]) == {exp_date: {"t": [exp_t], "o": [100.0], "h": [101.0], "l": [99.0],
                                                                 "c": [100.0], "v": [10]}}

    def test_naive_dropped_nan_none_and_6_digits(self):
        out = ist.split_by_et_date([_bar(datetime(2026, 11, 3, 9, 30)),                       # tz なしは使わない
                                    dict(_bar(datetime(2026, 11, 3, 9, 35, tzinfo=ET)), close=float("nan"), volume=float("nan")),
                                    dict(_bar(datetime(2026, 11, 3, 9, 40, tzinfo=ET)), close=6812.123456789)])
        assert out["2026-11-03"]["t"] == ["09:35", "09:40"]
        assert out["2026-11-03"]["c"] == [None, 6812.12] and out["2026-11-03"]["v"] == [None, 10]


class TestUpdate:
    def test_writes_trading_days_and_not_before_close(self, tmp_path):
        days = ["2026-10-30", "2026-11-02", "2026-11-03"]
        # 11-03 14:00 UTC = 09:00 EST（引けの前）→ 11-03 は書かない
        r = ist.update(datetime(2026, 11, 3, 14, 0, tzinfo=UTC), start=date(2026, 10, 30), fetch=_fetch_all(days),
                       data_dir=str(tmp_path))
        assert r["written"] == ["2026-10-30", "2026-11-02"]
        assert sorted(os.listdir(tmp_path)) == ["2026-10-30.json", "2026-11-02.json"]   # 土日（足なし）はファイルを作らない
        doc = _read(tmp_path, "2026-11-02")
        assert list(doc["tickers"]) == ist.TICKERS and doc["missing"] == {} and doc["tz"] == "America/New_York"
        assert doc["tickers"]["^VIX"]["t"][0] == "09:30" and doc["tickers"]["^VIX"]["t"][-1] == "16:00"   # シカゴ→ET
        assert set(doc["tickers"]["^GSPC"]) == {"t", "o", "h", "l", "c", "v"}
        # 引けの後なら今日も書く
        r = ist.update(NOW, start=date(2026, 10, 30), fetch=_fetch_all(days), data_dir=str(tmp_path))
        assert r["written"] == ["2026-11-03"]

    def test_file_is_compact_single_line(self, tmp_path):
        ist.update(NOW, start=date(2026, 11, 3), fetch=_fetch_all(["2026-11-03"]), data_dir=str(tmp_path))
        raw = (tmp_path / "2026-11-03.json").read_text(encoding="utf-8")
        assert raw.count("\n") == 1 and ", " not in raw and ": " not in raw   # 整形なし

    def test_idempotent(self, tmp_path):
        days = ["2026-10-29", "2026-10-30", "2026-11-02", "2026-11-03"]
        ist.update(NOW, start=date(2026, 10, 29), fetch=_fetch_all(days), data_dir=str(tmp_path))
        first = {n: (tmp_path / n).read_bytes() for n in os.listdir(tmp_path)}
        r = ist.update(NOW, start=date(2026, 10, 29), fetch=_fetch_all(days), data_dir=str(tmp_path))
        assert r["written"] == []
        assert {n: (tmp_path / n).read_bytes() for n in os.listdir(tmp_path)} == first

    def test_only_missing_days_refetched(self, tmp_path):
        days = ["2026-10-29", "2026-10-30", "2026-11-02"]
        now = datetime(2026, 11, 3, 1, 0, tzinfo=UTC)          # 11-02 20:00 EST（11-02 はまだ終わっていない）
        ist.update(now, start=date(2026, 10, 29), fetch=_fetch_all(days), data_dir=str(tmp_path))
        os.remove(tmp_path / "2026-10-30.json")                 # 夜の回が抜けた日
        calls = []
        r = ist.update(NOW, start=date(2026, 10, 29), fetch=_fetch_all(days, calls=calls), data_dir=str(tmp_path))
        # 10-29・11-02（NOW では ET で終わっている）はそろい済みで取り直さない。10-30（無い）・11-03（今日、引けの後）と、
        # 足が無くファイルを作らない土日（10-31・11-01）が対象
        assert r["written"] == ["2026-10-30", "2026-11-03"]
        assert r["skipped_complete"] == 2
        assert {c[1] for c in calls} == {date(2026, 10, 29)}   # 取得は対象の最初の日（10-30）の1日前から
        assert len(calls) == len(ist.TICKERS)                   # 1銘柄1回

    def test_complete_days_not_refetched_at_all(self, tmp_path):
        days = ["2026-10-30"]
        ist.update(NOW, start=date(2026, 10, 30), fetch=_fetch_all(days), data_dir=str(tmp_path))
        calls = []
        r = ist.update(NOW + timedelta(days=1), start=date(2026, 10, 30), fetch=_fetch_all(days, calls=calls),
                       data_dir=str(tmp_path))
        # 10-30 はそろい済み。対象は11-03・11-04 と土日だけ（土日は足が無いのでファイルを作らない）
        assert "2026-10-30" not in r["written"]
        assert calls and all(c[1] >= date(2026, 10, 30) for c in calls)

    def test_one_ticker_fails_recorded_others_written(self, tmp_path):
        r = ist.update(NOW, start=date(2026, 11, 3), fetch=_fetch_all(["2026-11-03"], fail=("ES=F",)), data_dir=str(tmp_path))
        doc = _read(tmp_path, "2026-11-03")
        assert doc["missing"] == {"ES=F": "fetch_error"} and "ES=F" not in doc["tickers"] and len(doc["tickers"]) == 9
        assert r["missing"] == {"2026-11-03": {"ES=F": "fetch_error"}}
        # 次の回で取れたら埋まり、欠けの記録は消える
        r = ist.update(NOW + timedelta(days=1), start=date(2026, 11, 3), fetch=_fetch_all(["2026-11-03"]), data_dir=str(tmp_path))
        doc = _read(tmp_path, "2026-11-03")
        assert doc["missing"] == {} and len(doc["tickers"]) == 10

    def test_no_bars_on_trading_day_is_retried(self, tmp_path):
        def fetch(sym, start, end):
            return [] if sym == "HYG" else _fetch_all(["2026-11-03"])(sym, start, end)
        ist.update(NOW + timedelta(days=1), start=date(2026, 11, 3), fetch=fetch, data_dir=str(tmp_path))
        doc = _read(tmp_path, "2026-11-03")
        assert doc["missing"] == {"HYG": "no_bars"}
        assert ist.is_complete(doc, "2026-11-03", NOW + timedelta(days=1)) is False   # 取引日に足の無い銘柄 → 取り直す

    def test_never_drops_bars(self, tmp_path):
        ist.update(NOW + timedelta(days=1), start=date(2026, 11, 3), fetch=_fetch_all(["2026-11-03"]), data_dir=str(tmp_path))
        before = _read(tmp_path, "2026-11-03")
        # わざと足りない状態にして（1銘柄を欠けに）、取り直しで足が少ない応答が来ても、ある銘柄の足は減らさない
        before["missing"] = {"NQ=F": "fetch_error"}
        nq = before["tickers"].pop("NQ=F")
        (tmp_path / "2026-11-03.json").write_text(json.dumps(before, separators=(",", ":")) + "\n", encoding="utf-8")

        def short(sym, start, end):
            bars = _fetch_all(["2026-11-03"])(sym, start, end)
            return bars[:5]
        ist.update(NOW + timedelta(days=1), start=date(2026, 11, 3), fetch=short, data_dir=str(tmp_path))
        after = _read(tmp_path, "2026-11-03")
        assert after["tickers"]["^GSPC"] == before["tickers"]["^GSPC"]          # 多い方を残す
        assert len(after["tickers"]["NQ=F"]["t"]) == 5 and len(nq["t"]) == 79   # 欠けていた銘柄は取れた分を入れる

    def test_weekend_futures_bars_get_a_file(self, tmp_path):
        # 日曜の夜（18:00 ET〜）の先物は、日曜の ET のファイルに入る（月曜のギャップを見るため）
        def fetch(sym, start, end):
            if sym in ("ES=F", "NQ=F", "CL=F", "GC=F", "DX-Y.NYB"):
                return _bars_for("2026-11-01", start="18:00", end="23:55")
            return []
        ist.update(NOW, start=date(2026, 11, 1), fetch=fetch, data_dir=str(tmp_path))
        doc = _read(tmp_path, "2026-11-01")
        assert sorted(doc["tickers"]) == sorted(["CL=F", "DX-Y.NYB", "ES=F", "GC=F", "NQ=F"])
        assert set(doc["missing"].values()) == {"no_bars"}
        assert ist.is_complete(doc, "2026-11-01", NOW) is True    # 休場日の足なしは欠けではない（取り直さない）

    def test_all_fail_does_not_raise(self, tmp_path):
        r = ist.update(NOW, start=date(2026, 11, 3), fetch=_fetch_all([], fail=tuple(ist.TICKERS)), data_dir=str(tmp_path))
        doc = _read(tmp_path, "2026-11-03")
        assert doc["tickers"] == {} and set(doc["missing"].values()) == {"fetch_error"}
        assert r["written"] == ["2026-11-03"]

    def test_default_target_is_last_10_trading_days(self):
        ds = ist.target_dates(NOW)
        assert ds[0] == "2026-10-21" and ds[-1] == "2026-11-03"   # 10-21〜11-03 の10取引日（暦日で全部）
        assert ist.target_dates(datetime(2026, 11, 3, 20, 0, tzinfo=UTC))[-1] == "2026-11-02"   # 15:00 EST は引けの前

    def test_start_clamped_to_60_days(self, tmp_path):
        calls = []
        ist.update(NOW, start=date(2026, 8, 1), fetch=_fetch_all([], calls=calls), data_dir=str(tmp_path))
        assert calls[0][1] == NOW.date() - timedelta(days=ist.MAX_LOOKBACK_DAYS)


def test_collect_and_send_calls_update_without_stopping():
    with open(os.path.join(_MARKET_PULSE_DIR, "collect_and_send.py"), encoding="utf-8") as f:
        t = f.read()
    i = t.index("_is.update(_now_utc)")
    assert "import intraday_store as _is" in t[i - 300:i] and "except Exception" in t[i:i + 300]


# ── System Health [O] ───────────────────────────────────────────────
class TestSystemHealthO:
    def test_missing_latest_trading_day_file_warns(self, tmp_path):
        r = sh._check_o(data_dir=str(tmp_path), now=NOW)        # 直近の取引日（引けの後）は 11-03
        assert r["ok"] is False and "2026-11-03" in r["label"] and r["label"].startswith("⚠️")
        assert sh._item_severity(r["label"], r["ok"]) == 1

    def test_before_close_uses_previous_trading_day(self, tmp_path):
        ist.update(NOW, start=date(2026, 11, 2), fetch=_fetch_all(["2026-11-02"]), data_dir=str(tmp_path))
        r = sh._check_o(data_dir=str(tmp_path), now=datetime(2026, 11, 3, 20, 0, tzinfo=UTC))
        assert r["ok"] is True and "2026-11-02: 10本" in r["detail"]

    def test_present_with_missing_is_ok_with_detail(self, tmp_path):
        ist.update(NOW, start=date(2026, 11, 3), fetch=_fetch_all(["2026-11-03"], fail=("HYG",)), data_dir=str(tmp_path))
        r = sh._check_o(data_dir=str(tmp_path), now=NOW)
        assert r["ok"] is True and "9本・欠け HYG(fetch_error)" in r["detail"]

    def test_no_dir_is_skip(self, tmp_path):
        r = sh._check_o(data_dir=str(tmp_path / "none"), now=NOW)
        assert r["ok"] is True and "未作成" in r["label"]

    def test_one_line_label(self):
        assert "Intraday⚠️x" in sh.build_one_line("2026-11-03", {"O": {"ok": False, "short": "x"}})

    def test_default_dir_matches_store(self):
        assert os.path.normpath(sh._INTRADAY_DIR) == os.path.normpath(ist.INTRADAY_DIR)
