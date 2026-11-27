"""[[MARKETPULSE-REVIEW-EPIC-1]] 順1（型L: 値の日付・鮮度）の回帰テスト。

- common/freshness.py: 入力の種類ごとの「期待する日」と、古い／日付なしの判定の1か所
- stage_conclusions.data_quality: 日経・HYスプレッドを含め、同じ部品で判定する（日経は構造上の遅れを見込む）
- daily_guard: 米国の終値がそろっていても・NYSEの休場日でも、^N225 の期待する日の行が未保存なら取り直す
- System Health [Q]: 画面と同じ部品で判定し直す
"""
import json
import os
import sys
from datetime import datetime, timezone

from common import freshness as fr

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "market", "market_pulse"))
import stage_conclusions as sc  # noqa: E402

UTC = timezone.utc
# 2026-11-26 06:28 JST（Market Data Daily の定時の取得）。11-24・11-25の日本の足の終値は、この時刻にはまだYahooに無い
RUN = datetime(2026, 11, 25, 21, 28, tzinfo=UTC)


def test_jpx_expected_date_allows_for_yahoo_confirmation_lag():
    # 引け（15:30 JST）の約15時間後の定時の取得では、前の日の足も、その前の日の足も確定を待つ
    assert fr.jpx_expected_date(RUN) == "2026-11-20"           # 11-23は祝日、11-24は 11-25 21:30 UTC から
    assert fr.jpx_expected_date(datetime(2026, 11, 25, 21, 31, tzinfo=UTC)) == "2026-11-24"
    assert fr.jpx_expected_date(datetime(2026, 11, 27, 0, 44, tzinfo=UTC)) == "2026-11-25"


def test_fred_daily_expects_one_business_day_before_us_close():
    assert fr.expected_as_of("fred_daily", RUN, "2026-11-25") == "2026-11-24"
    assert fr.expected_as_of("fred_daily", RUN, "2026-11-23") == "2026-11-20"      # 月曜の前は金曜
    assert fr.expected_as_of("us_close", RUN, "2026-11-25") == "2026-11-25"


def test_judge_fresh_stale_unknown():
    assert fr.judge("2026-11-24", "2026-11-24")["status"] == "fresh"
    assert fr.judge("2026-11-25", "2026-11-24")["status"] == "fresh"                # 期待する日より新しい
    old = fr.judge("2026-11-20", "2026-11-24")
    assert old["status"] == "stale" and old["lag_days"] == 4
    assert fr.judge(None, "2026-11-24")["status"] == "unknown"                       # 日付なしは新しい扱いにしない
    assert fr.judge("2026-11-24", None)["status"] == "unknown"


def test_assess_uses_kind_per_input():
    res = fr.assess({"S&P500": "2026-11-25", "日経平均": "2026-11-20", "HYスプレッド": "2026-11-24", "X": None},
                    RUN, "2026-11-25")
    assert res["S&P500"]["status"] == "fresh" and res["S&P500"]["kind"] == "us_close"
    assert res["日経平均"]["status"] == "fresh" and res["日経平均"]["kind"] == "jpx"         # 11-20 は期待する日そのもの
    assert res["HYスプレッド"]["status"] == "fresh" and res["HYスプレッド"]["kind"] == "fred_daily"
    assert res["X"]["status"] == "unknown"
    assert list(fr.stale_names(fr.assess({"日経平均": "2026-11-19"}, RUN, "2026-11-25"))) == ["日経平均"]


def _entry(nikkei="2026-11-20", spx="2026-11-25"):
    ind = {k: {"change_percent": 0.1, "date": spx} for k in sc.DQ_INDICATORS}
    ind["日経平均"] = {"change_percent": 0.1, "date": nikkei}
    af = {k: {"change_pct": 0.1, "date": spx} for k in sc.DQ_ASSET_FLOW}
    return ind, af, {"date": spx}


def test_data_quality_nikkei_one_trading_day_behind_is_not_flagged_but_older_is():
    ind, af, b = _entry(nikkei="2026-11-20")
    dq = sc.data_quality(ind, af, b, "2026-11-25", now_utc=RUN, extras={"HYスプレッド": "2026-11-24"})
    assert dq["status"] == "complete" and dq["old_elements"] == [] and dq["freshness"] == {}
    assert dq["expected_by_kind"] == {"us_close": "2026-11-25", "jpx": "2026-11-20", "fred_daily": "2026-11-24"}
    ind, af, b = _entry(nikkei="2026-11-19")
    dq = sc.data_quality(ind, af, b, "2026-11-25", now_utc=RUN, extras={"HYスプレッド": "2026-11-24"})
    assert dq["status"] == "partial" and dq["old_elements"] == ["日経平均"]
    assert dq["freshness"]["日経平均"]["status"] == "stale" and dq["freshness"]["日経平均"]["kind"] == "jpx"
    assert dq["old_stages"] == []                          # 日経は段階の判定に使わない


def test_data_quality_hy_spread_stale_or_missing_date():
    ind, af, b = _entry()
    dq = sc.data_quality(ind, af, b, "2026-11-25", now_utc=RUN, extras={"HYスプレッド": "2026-11-20"})
    assert dq["old_elements"] == ["HYスプレッド"] and dq["freshness"]["HYスプレッド"]["expected"] == "2026-11-24"
    dq = sc.data_quality(ind, af, b, "2026-11-25", now_utc=RUN, extras={"HYスプレッド": None})
    assert dq["old_elements"] == ["HYスプレッド"] and dq["freshness"]["HYスプレッド"]["status"] == "unknown"


def _n225(tmp_path, dates):
    d = tmp_path / "daily"
    d.mkdir()
    recs = [{"date": x, "close": 50000.0, "volume": 1000} for x in dates]
    (d / "^N225.json").write_text(json.dumps({"symbol": "^N225", "records": recs}), encoding="utf-8")
    return str(d)


def test_daily_guard_retries_when_n225_expected_bar_is_missing(tmp_path):
    from common.market_data import daily_guard as g
    base = _n225(tmp_path, ["2026-11-19", "2026-11-20"])
    # NYSE休場日（感謝祭）でも、11-24の足が未保存なら取り直す
    now = datetime(2026, 11, 26, 21, 47, tzinfo=UTC)
    assert g.jpx_pending(now, base) == "2026-11-25"
    out = g.decide(now, base)
    assert out["run"] == "true" and "^N225" in out["reason"]
    # 保存済みなら何もしない
    sub = tmp_path / "b"
    sub.mkdir()
    base2 = _n225(sub, ["2026-11-24", "2026-11-25"])
    assert g.jpx_pending(now, base2) is None and g.decide(now, base2)["run"] == "false"


def test_daily_guard_gives_up_after_retry_window_and_without_file(tmp_path, monkeypatch):
    from common.market_data import daily_guard as g
    base = _n225(tmp_path, ["2026-11-10"])
    now = datetime(2026, 11, 26, 21, 47, tzinfo=UTC)                                      # 期待する日は 11-25
    assert g.jpx_pending(now, base) == "2026-11-25"
    monkeypatch.setattr(g, "JPX_RETRY_DAYS", 0)                                           # 期待する日から日数が過ぎたら起動し続けない
    assert g.jpx_pending(now, base) is None
    empty = tmp_path / "empty"
    empty.mkdir()
    assert g.jpx_pending(datetime(2026, 11, 26, 21, 47, tzinfo=UTC), str(empty)) is None


def test_system_health_q_flags_stale_and_reads_same_component(tmp_path):
    from common import system_health as sh
    entry = {"data_freshness": {"checked_at": "2026-11-25T21:28:00Z"},
             "data_quality": {"expected_close_date": "2026-11-25",
                              "as_of": {"S&P500": "2026-11-25", "日経平均": "2026-11-19", "HYスプレッド": None}}}
    p = tmp_path / "market_data.json"
    p.write_text(json.dumps([entry]), encoding="utf-8")
    r = sh._check_q(str(p), str(tmp_path / "none.csv"))
    assert not r["ok"] and "日経平均" in r["label"] and "日付なし" in r["label"] and "HYスプレッド" in r["label"]
    entry["data_quality"]["as_of"] = {"S&P500": "2026-11-25", "日経平均": "2026-11-20", "HYスプレッド": "2026-11-24"}
    p.write_text(json.dumps([entry]), encoding="utf-8")
    assert sh._check_q(str(p), str(tmp_path / "none.csv"))["ok"]


def test_system_health_q_macro_liquidity_asof(tmp_path):
    from common import system_health as sh
    mp = tmp_path / "market_data.json"
    mp.write_text(json.dumps([{"data_freshness": {"checked_at": "2026-11-25T21:28:00Z"},
                               "data_quality": {"expected_close_date": "2026-11-25", "as_of": {"S&P500": "2026-11-25"}}}]),
                  encoding="utf-8")
    liq = tmp_path / "05_liquidity.csv"
    liq.write_text("date,sp500,sp500_asof,hy_spread_asof\n2026-11-26,7000,2026-11-20,2026-11-25\n", encoding="utf-8")
    r = sh._check_q(str(mp), str(liq))
    assert not r["ok"] and "S&P500（FRED）" in r["label"]          # 11-26の行なのに観測日が11-20
    liq.write_text("date,sp500,sp500_asof,hy_spread_asof\n2026-11-26,7000,2026-11-25,2026-11-25\n", encoding="utf-8")
    assert sh._check_q(str(mp), str(liq))["ok"]
    liq.write_text("date,sp500\n2026-11-26,7000\n", encoding="utf-8")            # 日付の列が無い間は問題にしない
    assert sh._check_q(str(mp), str(liq))["ok"]
