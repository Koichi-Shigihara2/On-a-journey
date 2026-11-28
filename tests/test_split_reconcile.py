"""日足の分割・分社の割り戻し（common/market_data/split_reconcile.py、[[POINT-IN-TIME-HISTORY-INTEGRITY-1]] (1)）。

- 比が一定なら割り戻す／比がばらつけば何もしない／確定の行のほか（暫定・終値なし・境の日以降）は触らない
- 書き手は correct_daily_rows だけ（daily/ を直接書かない）
- 2回目は当たらない
"""
import json
import os
import sys
from datetime import date, timedelta

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common.market_data import fetcher, split_reconcile as sr  # noqa: E402

N = 100          # 全部で100行
BOUNDARY_IDX = 90  # 90行目から一致


def _dates(n=N):
    d0 = date(2026, 1, 5)
    out, d = [], d0
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _write(base, sym, rows):
    os.makedirs(os.path.join(base, "daily"), exist_ok=True)
    with open(os.path.join(base, "daily", f"{sym}.json"), "w", encoding="utf-8") as f:
        json.dump({"symbol": sym, "records": rows}, f)


def _read(base, sym):
    with open(os.path.join(base, "daily", f"{sym}.json"), encoding="utf-8") as f:
        return {r["date"]: r for r in json.load(f)["records"]}


def _mk(dates, ratio, noise=None, idx=BOUNDARY_IDX):
    """保存した行（境より前は ratio 倍）と、Yahooの行（調整済み）"""
    stored, yahoo = [], {}
    for i, d in enumerate(dates):
        y = 100.0 + i * 0.1
        k = 1.0 if i >= idx else (ratio if noise is None else ratio * (1 + noise[i % len(noise)]))
        stored.append({"date": d, "open": y * k, "high": y * k * 1.01, "low": y * k * 0.99, "close": y * k,
                       "volume": 1000 if i >= idx else int(1000 / ratio), "_validation_warnings": []})
        yahoo[d] = {"close": y, "volume": 1000.0}
    return stored, yahoo


def _provider(table):
    return lambda symbols, start: {s: table.get(s, {}) for s in symbols}


def test_constant_ratio_is_adjusted(tmp_path):
    base = str(tmp_path)
    ds = _dates()
    stored, yahoo = _mk(ds, 2.0)
    _write(base, "AAA", stored)
    hits = sr.scan(["AAA"], base, provider_fn=_provider({"AAA": yahoo}))
    assert list(hits) == ["AAA"]
    assert hits["AAA"]["ratio"] == pytest.approx(2.0)
    assert hits["AAA"]["boundary"] == ds[BOUNDARY_IDX]
    assert hits["AAA"]["volume_ratio"] == pytest.approx(2.0)   # 出来高も分割の比
    done = sr.apply(hits, total=100, base_dir=base)
    assert done == {"AAA": BOUNDARY_IDX}
    rows = _read(base, "AAA")
    for i, d in enumerate(ds):
        assert rows[d]["close"] == pytest.approx(yahoo[d]["close"])
    assert rows[ds[0]]["volume"] == 1000
    c = rows[ds[0]]["_corrections"][-1]
    assert c["reason"] == "split_adjust" and c["note"].startswith("ratio=2")
    assert "_corrections" not in rows[ds[BOUNDARY_IDX]]          # 境の日以降は触らない
    # 2回目は当たらない
    assert sr.scan(["AAA"], base, provider_fn=_provider({"AAA": yahoo})) == {}


def test_spin_off_keeps_volume(tmp_path):
    base = str(tmp_path)
    ds = _dates()
    stored, yahoo = _mk(ds, 6.665)
    for r in stored:
        r["volume"] = 1000                                        # 分社: 出来高は調整されない
    _write(base, "SPN", stored)
    hits = sr.scan(["SPN"], base, provider_fn=_provider({"SPN": yahoo}))
    assert hits["SPN"]["volume_ratio"] == 1.0
    sr.apply(hits, total=100, base_dir=base)
    assert _read(base, "SPN")[ds[0]]["volume"] == 1000


def test_scattered_ratio_does_nothing(tmp_path):
    base = str(tmp_path)
    ds = _dates()
    stored, yahoo = _mk(ds, 2.0, noise=[0.0, 0.3, -0.3, 0.5, -0.4])
    _write(base, "BBB", stored)
    before = _read(base, "BBB")
    assert sr.scan(["BBB"], base, provider_fn=_provider({"BBB": yahoo})) == {}
    assert _read(base, "BBB") == before


def test_matching_series_does_nothing(tmp_path):
    base = str(tmp_path)
    ds = _dates()
    stored, yahoo = _mk(ds, 1.0)
    _write(base, "CCC", stored)
    assert sr.scan(["CCC"], base, provider_fn=_provider({"CCC": yahoo})) == {}


def test_short_mismatch_run_does_nothing(tmp_path):
    """食い違いが全体の8割に満たない（一時の乱れ）なら当てない"""
    base = str(tmp_path)
    ds = _dates()
    stored, yahoo = _mk(ds, 2.0, idx=50)                           # 食い違い50/100行 < 8割
    _write(base, "DDD", stored)
    assert sr.scan(["DDD"], base, provider_fn=_provider({"DDD": yahoo})) == {}


def test_provisional_and_closeless_rows_untouched(tmp_path):
    base = str(tmp_path)
    ds = _dates()
    stored, yahoo = _mk(ds, 2.0)
    stored[5]["_provisional"] = True
    stored[6]["close"] = None
    _write(base, "EEE", stored)
    hits = sr.scan(["EEE"], base, provider_fn=_provider({"EEE": yahoo}))
    assert list(hits) == ["EEE"]
    sr.apply(hits, total=100, base_dir=base)
    rows = _read(base, "EEE")
    assert rows[ds[5]]["close"] == stored[5]["close"] and "_corrections" not in rows[ds[5]]
    assert rows[ds[6]]["close"] is None
    assert "_corrections" in rows[ds[0]]


def test_too_many_hits_writes_nothing(tmp_path):
    base = str(tmp_path)
    ds = _dates()
    stored, yahoo = _mk(ds, 2.0)
    _write(base, "FFF", stored)
    before = _read(base, "FFF")
    hits = sr.scan(["FFF"], base, provider_fn=_provider({"FFF": yahoo}))
    assert sr.apply(hits, total=10, base_dir=base) == {}            # 1/10 > 5%
    assert _read(base, "FFF") == before


def test_eligible_symbols_skip_indexes_and_futures(tmp_path):
    base = str(tmp_path)
    for s in ("AAPL", "^VIX", "GC=F", "JPY=X"):
        _write(base, s, [])
    assert sr.eligible_symbols(base) == ["AAPL"]


def test_only_correct_daily_rows_writes():
    """split_reconcile は daily/ を直接書かず、fetcher.correct_daily_rows を通す"""
    src = open(os.path.join(REPO, "common", "market_data", "split_reconcile.py"), encoding="utf-8").read()
    assert "correct_daily_rows" in src
    assert "write_daily_rows(" not in src and "_atomic_write_json" not in src
    # 書くのは [SKIP] の記録（common/market_data/_split_reconcile_skips.json。daily/ ではない）だけ
    assert "open(" in src and src.count('"w"') == 1 and "'w'" not in src and '"w"' in src.split("def record_skips")[1].split("def main")[0]


def _mk_sndk_shape(dates):
    """SNDK（2026-11-20・11-23 が二重に調整された形）: 境より前は ratio 8 倍の行、そのうち境の直前の2行は取得時に既に分割後の尺度、
    そのときのYahooの値は2行とも8分の1（一時的に二重に調整されていた）。それ以外の行は Yahoo と比が8、境の後は一致"""
    stored, yahoo = [], {}
    for i, d in enumerate(dates):
        y = 100.0 + i * 0.1
        if i < 90:
            k_store, k_yahoo = 8.0, 1.0              # 保存した行は分割前の尺度、Yahooは調整済み
        elif i < 92:
            k_store, k_yahoo = 1.0, 1.0 / 8.0        # 既に分割後の尺度で保存した2行、Yahooは一時的に二重に調整
        else:
            k_store, k_yahoo = 1.0, 1.0
        stored.append({"date": d, "open": y * k_store, "high": y * k_store * 1.01, "low": y * k_store * 0.99, "close": y * k_store,
                       "volume": 1000, "_validation_warnings": []})
        yahoo[d] = {"close": y * k_yahoo, "volume": 1000.0}
    return stored, yahoo


def test_double_adjusted_neighbour_rows_are_not_divided_again(tmp_path):
    """境より前の行のうち、既に分割後の尺度の行まで割ると終値が1日で8分の1に飛ぶ（SNDK 2026-11-20・11-23）。連続性の確認で当てない"""
    base = str(tmp_path)
    dates = _dates()
    stored, yahoo = _mk_sndk_shape(dates)
    _write(base, "SNDK", stored)
    skipped = {}
    hits = sr.scan(["SNDK"], base_dir=base, provider_fn=_provider({"SNDK": yahoo}), skipped=skipped)
    assert hits == {}                                              # 当てない
    assert skipped["SNDK"]["reason"] == "continuity" and skipped["SNDK"]["boundary"] == dates[92]
    assert abs(skipped["SNDK"]["move"] + 0.875) < 0.01             # 割ると1日で −87.5%（8分の1）
    rows = _read(base, "SNDK")
    assert rows[dates[90]]["close"] == pytest.approx(stored[90]["close"])      # 保存した行は変わらない


def test_without_continuity_check_the_old_behaviour_divides_the_adjusted_rows(monkeypatch, tmp_path):
    """確認の代わりの対照: 上限を外すと、昔の動き（既に分割後の行まで割る）になる＝確認が効いていることの証拠"""
    monkeypatch.setattr(sr, "CONTINUITY_MAX_MOVE", 1e9)
    base = str(tmp_path)
    dates = _dates()
    stored, yahoo = _mk_sndk_shape(dates)
    _write(base, "SNDK", stored)
    hits = sr.scan(["SNDK"], base_dir=base, provider_fn=_provider({"SNDK": yahoo}))
    assert hits["SNDK"]["boundary"] == dates[92] and hits["SNDK"]["ratio"] == pytest.approx(8.0, rel=0.02)


def test_genuine_split_passes_the_continuity_check(tmp_path):
    """本物の分割（境の前後が割り戻しで連続になる）は今までどおり当たる"""
    base = str(tmp_path)
    dates = _dates()
    stored, yahoo = _mk(dates, 8.0)
    _write(base, "AAA", stored)
    skipped = {}
    hits = sr.scan(["AAA"], base_dir=base, provider_fn=_provider({"AAA": yahoo}), skipped=skipped)
    assert "AAA" in hits and not skipped


def test_record_skips_keeps_first_seen_and_drops_cleared_symbols(tmp_path):
    """[SKIP] の記録: 同じ銘柄・同じ境なら first_seen を引き継ぐ。当てなくなった銘柄は消す。1度も出ていなければファイルを作らない"""
    base = str(tmp_path)
    os.makedirs(os.path.join(base, "daily"), exist_ok=True)
    assert sr.record_skips({}, base_dir=base, today="2026-11-28") is False and not os.path.exists(os.path.join(base, sr.SKIPS_FILE))
    k = {"reason": "continuity", "date": "2026-11-24", "move": 6.11, "ratio": 8.0, "boundary": "2026-11-24"}
    assert sr.record_skips({"SNDK": k}, base_dir=base, today="2026-11-28") is True
    assert sr.record_skips({"SNDK": k}, base_dir=base, today="2026-12-05") is False          # 同じなら書かない（first_seen は 11-28 のまま）
    with open(os.path.join(base, sr.SKIPS_FILE), encoding="utf-8") as f:
        assert json.load(f)["skips"]["SNDK"]["first_seen"] == "2026-11-28"
    assert sr.record_skips({}, base_dir=base, today="2026-12-12") is True                      # 当てなくなったら消す
    with open(os.path.join(base, sr.SKIPS_FILE), encoding="utf-8") as f:
        assert json.load(f)["skips"] == {}
