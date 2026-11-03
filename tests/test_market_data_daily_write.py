"""日足の行の書き込みの決まり（[[MARKETDATA-DAILY-ROW-WRITE-1]]、2026-11-03）。

- daily/{SYMBOL}.json の行を書くのは common/market_data/fetcher.py::write_daily_rows だけ（直接書く場所は DAILY_WRITE_ALLOWED の1か所）
- 確定の行は上書きしない。違う値が来たら書かずに記録（どの行・どの項目・前と新しい値・経路）
- 直すのは理由を指定した経路だけ（株式分割の調整・暫定の行の確定・明示の修正）。前の値と理由を行に残す
- 暫定の印は、どの経路で書いても同じ判定（calendar_universe.is_provisional_bar・is_flat_futures_bar）で付く
"""
import ast
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common.market_data import fetcher  # noqa: E402

# daily/ を書いてよい（ファイル, 関数）
DAILY_WRITE_ALLOWED = {("common/market_data/fetcher.py", "write_daily_rows")}

LATE = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)


def _read(base, sym):
    with open(os.path.join(base, "daily", f"{sym}.json"), encoding="utf-8") as f:
        return {r["date"]: r for r in json.load(f)["records"]}


def _log(base):
    p = os.path.join(base, fetcher.DAILY_REJECT_LOG)
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8") as f:
        return json.load(f)["entries"]


def _bar(d, close, vol=1000, o=None):
    return {"date": d, "open": o if o is not None else close, "high": close + 1, "low": close - 1, "close": close, "volume": vol}


# ── 確定の行は上書きしない ──
def test_confirmed_row_overwrite_is_rejected_and_logged(tmp_path):
    base = str(tmp_path)
    fetcher.write_daily_rows("AAPL", [_bar("2026-10-01", 100.0)], base, now=LATE)
    res = fetcher.write_daily_rows("AAPL", [_bar("2026-10-01", 100.0, vol=1300)], base, now=LATE, source="--backfill")
    assert res["rejected"] == ["2026-10-01"] and _read(base, "AAPL")["2026-10-01"]["volume"] == 1000
    log = _log(base)
    assert log == [{"symbol": "AAPL", "date": "2026-10-01", "source": "--backfill",
                    "fields": {"volume": {"old": 1000, "new": 1300}},
                    "first_at": "2026-11-03T12:00:00Z", "last_at": "2026-11-03T12:00:00Z", "count": 1}]
    # 同じ新しい値をもう一度拒むと、1件のまま回数が増える
    fetcher.write_daily_rows("AAPL", [_bar("2026-10-01", 100.0, vol=1300)], base, now=LATE, source="--backfill")
    assert len(_log(base)) == 1 and _log(base)[0]["count"] == 2


def test_same_values_do_not_change_confirmed_row_or_its_marks(tmp_path):
    base = str(tmp_path)
    os.makedirs(os.path.join(base, "daily"))
    row = {**_bar("2021-01-04", 27255.0, vol=0, o=27255.0), "high": 27255.0, "low": 27255.0, "_validation_warnings": ["x"]}
    with open(os.path.join(base, "daily", "NIY=F.json"), "w", encoding="utf-8") as f:
        json.dump({"symbol": "NIY=F", "records": [row]}, f)
    before = open(os.path.join(base, "daily", "NIY=F.json"), encoding="utf-8").read()
    res = fetcher.write_daily_rows("NIY=F", [dict(row)], base, now=LATE)
    assert res["unchanged"] == ["2021-01-04"]
    assert open(os.path.join(base, "daily", "NIY=F.json"), encoding="utf-8").read() == before


# ── 理由を指定した経路だけが直せる ──
def test_split_adjustment_with_reason_corrects_and_keeps_previous_values(tmp_path):
    base = str(tmp_path)
    fetcher.write_daily_rows("NVDA", [_bar("2024-06-07", 1200.0, vol=400)], base, now=LATE)
    res = fetcher.correct_daily_rows("NVDA", [_bar("2024-06-07", 120.0, vol=4000)], reason="split_adjust", note="10:1",
                                     base_dir=base, now=LATE)
    row = _read(base, "NVDA")["2024-06-07"]
    assert res["corrected"] == ["2024-06-07"] and row["close"] == 120.0 and row["volume"] == 4000
    c = row["_corrections"][-1]
    assert c["reason"] == "split_adjust" and c["prev"]["close"] == 1200.0 and c["prev"]["volume"] == 400 and c["note"] == "10:1"
    assert _log(base) == []


def test_unknown_reason_is_refused(tmp_path):
    with pytest.raises(ValueError):
        fetcher.write_daily_rows("AAPL", [_bar("2026-10-01", 1.0)], str(tmp_path), reason="because")


def test_provisional_row_is_replaced_without_reason(tmp_path):
    base = str(tmp_path)
    early = datetime(2026, 9, 29, 1, 0, tzinfo=timezone.utc)
    fetcher.write_daily_rows("CL=F", [_bar("2026-09-28", 93.33)], base, now=early)
    assert _read(base, "CL=F")["2026-09-28"]["_provisional"] is True
    res = fetcher.write_daily_rows("CL=F", [_bar("2026-09-28", 92.60)], base, now=LATE)
    assert res["replaced"] == ["2026-09-28"] and "_provisional" not in _read(base, "CL=F")["2026-09-28"]


def test_mark_only_cannot_change_values(tmp_path):
    base = str(tmp_path)
    fetcher.write_daily_rows("AAPL", [_bar("2026-10-01", 100.0)], base, now=LATE)
    res = fetcher.write_daily_rows("AAPL", [{**_bar("2026-10-01", 99.0), "_provisional": True}], base, now=LATE,
                                   reason=fetcher.MARK_ONLY)
    assert res["rejected"] == ["2026-10-01"] and "_provisional" not in _read(base, "AAPL")["2026-10-01"]
    res = fetcher.write_daily_rows("AAPL", [{**_read(base, "AAPL")["2026-10-01"], "_provisional": True}], base, now=LATE,
                                   reason=fetcher.MARK_ONLY)
    assert res["marked"] == ["2026-10-01"] and _read(base, "AAPL")["2026-10-01"]["_provisional"] is True


# ── 暫定の印はどの経路でも同じ判定 ──
def _flat(d, v=69560.0):
    return {"date": d, "open": v, "high": v, "low": v, "close": v, "volume": 0}


def test_flat_futures_bar_is_marked_provisional_on_every_path(tmp_path, monkeypatch):
    base = str(tmp_path)
    # 毎日の取得の経路（最新の足の保存）
    fetcher._append_daily_record("NIY=F", _flat("2026-10-07"), base_dir=base, now=LATE)
    # 過去分の取り込み（--backfill・登録のStep 1.5）
    monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {"NIY=F": [_flat("2026-10-06", 70585.0)]})
    fetcher.backfill_daily_prices(["NIY=F"], base_dir=base)
    # 抜けた取引日の取り直し
    monkeypatch.setattr(fetcher, "find_missing_trading_days", lambda s, base_dir=None: ["2026-10-05"])
    monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {"NIY=F": [_flat("2026-10-05", 70000.0)]})
    fetcher.repair_missing_trading_days(["NIY=F"], base_dir=base)
    rows = _read(base, "NIY=F")
    assert [rows[d].get("_provisional") for d in ("2026-10-05", "2026-10-06", "2026-10-07")] == [True, True, True]


def test_n225_zero_volume_bar_is_marked_on_backfill(tmp_path, monkeypatch):
    base = str(tmp_path)
    monkeypatch.setattr(fetcher, "_download_historical_bars", lambda syms, period="5d", start=None: {"^N225": [_bar("2026-10-30", 50000.0, vol=0)]})
    fetcher.backfill_daily_prices(["^N225"], base_dir=base)
    assert _read(base, "^N225")["2026-10-30"]["_provisional"] is True


# ── daily/ を直接書く場所の禁止（COMMON-PATH-GUARD と同じ形） ──
def _py_files():
    out = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO, capture_output=True, text=True, encoding="utf-8", check=True)
    return [p for p in out.stdout.splitlines() if os.path.exists(os.path.join(REPO, p)) and not p.startswith("tests/")]


def _is_daily_join(n):
    return (isinstance(n, ast.Call) and getattr(n.func, "attr", getattr(n.func, "id", "")) == "join"
            and any(isinstance(a, ast.Constant) and a.value == "daily" for a in n.args))


def _daily_write_functions(tree):
    """関数のうち、書き込み（_atomic_write_json・open(…, "w"/"a")・to_csv・write_text・os.replace）の先が
    "daily" を含むパス（os.path.join(…, "daily", …) か、それを代入した名前）のもの"""
    bad = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        daily_names = {t.id for n in ast.walk(fn) if isinstance(n, ast.Assign) and any(_is_daily_join(x) for x in ast.walk(n.value))
                       for t in n.targets if isinstance(t, ast.Name)}

        def is_daily(a):
            return any(_is_daily_join(x) for x in ast.walk(a)) or (isinstance(a, ast.Name) and a.id in daily_names)
        for n in ast.walk(fn):
            if not isinstance(n, ast.Call) or not n.args:
                continue
            f = n.func
            name = f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""
            target = n.args[-1] if name == "replace" else n.args[0]
            if name == "open":
                mode = n.args[1] if len(n.args) > 1 else next((k.value for k in n.keywords if k.arg == "mode"), None)
                if not (isinstance(mode, ast.Constant) and isinstance(mode.value, str) and set(mode.value) & set("wa")):
                    continue
            elif name not in ("_atomic_write_json", "to_csv", "write_text", "replace"):
                continue
            if is_daily(target):
                bad.append(fn.name)
                break
    return bad


def test_only_write_daily_rows_writes_daily_rows():
    bad = []
    for rel in _py_files():
        with open(os.path.join(REPO, rel), encoding="utf-8") as f:
            src = f.read()
        if '"daily"' not in src and "'daily'" not in src:
            continue
        bad += [f"{rel}::{fn}" for fn in _daily_write_functions(ast.parse(src)) if (rel, fn) not in DAILY_WRITE_ALLOWED]
    assert not bad, ("daily/ を直接書いている（common/market_data/fetcher.py::write_daily_rows を呼ぶ。"
                     f"許すなら DAILY_WRITE_ALLOWED に）: {bad}")


def test_detector_finds_direct_write():
    tree = ast.parse("def f(b):\n    p = os.path.join(b, 'daily', 'X.json')\n    _atomic_write_json(p, {})\n"
                     "def g(b):\n    return json.load(open(os.path.join(b, 'daily', 'X.json')))\n")
    assert _daily_write_functions(tree) == ["f"]
