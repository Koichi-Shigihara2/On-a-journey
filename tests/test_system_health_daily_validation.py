"""System Health [R]: 日足の検証の印（`_validation_warnings`）を数え、想定内（出来高0が普通の指数・為替・先物）とそれ以外に分け、
直近7日にそれ以外が新しく出ていたら ⚠️（2026-11-28）。過去の日足は書き換えない"""
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import system_health as sh  # noqa: E402

TODAY = date(2026, 11, 28)


def _write(d, sym, rows):
    with open(os.path.join(d, f"{sym}.json"), "w", encoding="utf-8") as f:
        json.dump({"symbol": sym, "data": rows}, f)


def _row(day, warns=None):
    r = {"date": day, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100}
    if warns is not None:
        r["_validation_warnings"] = warns
    return r


def test_expected_volume_zero_marks_are_not_alerts(tmp_path):
    _write(tmp_path, "^VIX", [_row("2026-11-25", ["volume must be > 0 (got 0)"])])
    _write(tmp_path, "JPY=X", [_row("2026-11-26", ["volume must be > 0 (got 0)"])])
    _write(tmp_path, "AAPL", [_row("2026-11-26", [])])
    r = sh._check_r(TODAY, str(tmp_path))
    assert r["ok"] and r["label"].startswith("✅") and "想定内 2" in r["label"] and "それ以外 0行" in r["label"]


def test_new_price_anomaly_in_last_7_days_warns_and_old_one_does_not(tmp_path):
    _write(tmp_path, "HPE", [_row("2026-11-25", ["expected high >= close >= low (got high=1, close=2, low=0)"]),
                             _row("2026-09-30", ["open must be > 0 (got 0)"])])
    r = sh._check_r(TODAY, str(tmp_path))
    assert not r["ok"] and r["label"].startswith("⚠️") and r["detail"] == "新しい印1件"
    assert "HPE 2026-11-25 expected high >= close >= low" in r["label"] and "それ以外 2行/1銘柄" in r["label"]
    r2 = sh._check_r(date(2026, 12, 10), str(tmp_path))          # 7日より前になれば警告しない（件数には残る）
    assert r2["ok"] and "新しい印0" in r2["label"]


def test_volume_zero_on_a_stock_is_not_expected(tmp_path):
    _write(tmp_path, "PTC", [_row("2026-11-27", ["volume must be > 0 (got 0)"])])
    r = sh._check_r(TODAY, str(tmp_path))
    assert not r["ok"] and "PTC 2026-11-27 volume must be > 0" in r["label"]


def test_main_wires_check_r_and_one_line():
    src = open(sh.__file__, encoding="utf-8").read()
    assert "_check_r()" in src and '"R": {"ok": ok_r' in src and '"R": "DailyVal"' in src


def test_fx_high_close_low_violation_is_expected_but_not_for_stocks(tmp_path):
    """為替（=X）の high ≥ close ≥ low の違反は想定内（Yahooの今の値でも出る）。株の同じ印は想定内にしない"""
    hcl = "expected high >= close >= low (got high=1, close=2, low=0)"
    _write(tmp_path, "JPY=X", [_row("2026-11-25", [hcl])])
    r = sh._check_r(TODAY, str(tmp_path))
    assert r["ok"] and "想定内 1" in r["label"] and "それ以外 0行" in r["label"]
    _write(tmp_path, "HPE", [_row("2026-11-25", [hcl])])
    r2 = sh._check_r(TODAY, str(tmp_path))
    assert not r2["ok"] and "HPE 2026-11-25 expected high >= close >= low" in r2["label"]
