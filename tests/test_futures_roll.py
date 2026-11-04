"""[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] B3（2026-11-04）: 先物の限月の欄の異常と切り替えの印（common/market_data/futures_roll.py、印は write_daily_rows の MARK_ONLY）"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common.market_data import futures_roll as fr
from common.market_data.fetcher import MARK_ONLY, write_daily_rows


# ── B3 ───────────────────────────────────────────────────────────────
def _read(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _write(path, doc):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f)


def _row(d, close, contract=None, **kw):
    r = {"date": d, "open": close, "high": close, "low": close, "close": close, "volume": 10, "_validation_warnings": []}
    if contract:
        r["contract"] = contract
    r.update(kw)
    return r


def test_month_code_and_expiry():
    assert fr.has_month_code("CLZ26.NYM") and fr.has_month_code("NIYZ26.CME") and not fr.has_month_code("ES.CME")
    assert fr.is_quarterly_expiry("2026-03-20") and not fr.is_quarterly_expiry("2026-03-19") and not fr.is_quarterly_expiry("2026-04-17")


def test_desired_marks_confirmed_and_possible():
    rs = [_row("2026-10-14", 95.0, "CLX26.NYM"), _row("2026-10-15", 98.0, "CLZ26.NYM"), _row("2026-10-16", 97.0, "CLZ26.NYM")]
    assert fr.desired_marks("CL=F", rs) == {"2026-10-15": "confirmed"}
    es = [_row("2026-03-19", 100.0), _row("2026-03-20", 102.0)]
    px = [_row("2026-03-19", 100.0), _row("2026-03-20", 100.0)]
    assert fr.desired_marks("ES=F", es, px) == {"2026-03-20": "possible"}
    assert fr.desired_marks("GC=F", es, px) == {}                                         # 相手と引けの時刻が違う先物は探さない


def test_mark_only_keeps_values_and_anomalies(tmp_path):
    (tmp_path / "daily").mkdir()
    rows = [_row("2026-10-01", 100.0, "ESZ26.CME"), _row("2026-10-30", 101.0, "ES.CME"), _row("2026-11-02", 99.0, "ESZ26.CME")]
    _write(tmp_path / "daily" / "ES=F.json", {"symbol": "ES=F", "records": rows})
    out = write_daily_rows("ES=F", [{**rows[1], "_contract_roll": "possible"}], str(tmp_path), reason=MARK_ONLY)
    assert out["marked"] == ["2026-10-30"]
    saved = {r["date"]: r for r in _read(tmp_path / "daily" / "ES=F.json")["records"]}
    assert saved["2026-10-30"]["_contract_roll"] == "possible" and saved["2026-10-30"]["close"] == 101.0
    bad = write_daily_rows("ES=F", [{**rows[1], "close": 5.0, "_contract_roll": "possible"}], str(tmp_path), reason=MARK_ONLY)
    assert bad["rejected"] == ["2026-10-30"]                                              # 印の経路で値は変えられない
    assert [(a["ticker"], a["match"]) for a in fr.contract_anomalies(str(tmp_path / "daily"))] == [("ES=F", "2026-10-30")]


def test_saved_daily_marks():
    def by(s):
        recs = _read(os.path.join(REPO, "common", "market_data", "daily", f"{s}.json"))["records"]
        return {r["date"]: r.get("_contract_roll") for r in recs if r.get("_contract_roll")}
    assert by("CL=F").get("2026-10-15") == "confirmed"
    assert len(by("ES=F")) >= 10 and set(by("ES=F").values()) == {"possible"} and len(by("NQ=F")) >= 10
