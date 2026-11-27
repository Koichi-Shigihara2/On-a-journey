"""[[MARKETPULSE-REVIEW-EPIC-1]] 順3の残り（型S）: latest.json（最新のエントリだけ）と、古いエントリの trail の整理（2026-11-27）。

- 書き手は1か所（collect_and_send.save_data_to_json_and_csv）: market_data.json（履歴。最新以外から sector_rotation.trail を消す）と
  latest.json（最新のエントリだけ）を書く
- 最新だけを読む所（TANUKI SCORE の画面・pipeline の _load_live_fg・snapshot・System Health・検査）は latest.json を読む。
  無ければ market_data.json の最後のエントリ
- 移し替えのスクリプトは、trail が日足から作り直した trail と1つずつ一致するエントリだけ消す
"""
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "market" / "market_pulse"))

from common import market_pulse_data as mpd  # noqa: E402
import collect_and_send as cs  # noqa: E402


def _rot(trail=True, ratio=101.0):
    sec = {"name": "情報技術", "ratio": ratio, "momentum": 99.0, "quadrant": "Weakening", "change_pct": -0.5, "change_5d_pct": -2.0}
    if trail:
        sec["trail"] = [{"week": "2026-11-20", "ratio": 100.5, "momentum": 98.0}, {"week": "2026-11-27", "ratio": ratio, "momentum": 99.0}]
    return {"as_of": "2026-11-25", "sectors": {"XLK": sec}, "by_quadrant": {"Weakening": ["XLK"]}}


def test_strip_old_trails_keeps_last_and_non_trail_fields():
    es = [{"date": "d1", "sector_rotation": _rot()}, {"date": "d2", "sector_rotation": _rot()}, {"date": "d3", "sector_rotation": _rot()}]
    assert mpd.strip_old_trails(es) == 2
    assert "trail" not in es[0]["sector_rotation"]["sectors"]["XLK"] and "trail" not in es[1]["sector_rotation"]["sectors"]["XLK"]
    assert es[0]["sector_rotation"]["sectors"]["XLK"]["quadrant"] == "Weakening" and es[0]["sector_rotation"]["sectors"]["XLK"]["change_5d_pct"] == -2.0
    assert len(es[2]["sector_rotation"]["sectors"]["XLK"]["trail"]) == 2          # 最新は変えない
    assert mpd.strip_old_trails([{"date": "x"}]) == 0 and mpd.strip_old_trails([]) == 0


def test_load_latest_prefers_latest_json_then_history_last(tmp_path):
    assert mpd.load_latest(str(tmp_path)) is None
    (tmp_path / "market_data.json").write_text(json.dumps([{"date": "a"}, {"date": "b", "fear_greed": {"score": 9.6}}]), encoding="utf-8")
    assert mpd.load_latest(str(tmp_path))["date"] == "b"                           # latest.json が無ければ履歴の最後
    (tmp_path / "latest.json").write_text(json.dumps({"date": "c", "fear_greed": {"score": 20}}), encoding="utf-8")
    assert mpd.load_latest(str(tmp_path))["date"] == "c"
    (tmp_path / "latest.json").write_text("{壊れた", encoding="utf-8")
    assert mpd.load_latest(str(tmp_path))["date"] == "b"                           # 壊れていれば履歴の最後
    (tmp_path / "market_data.json").write_text("[]", encoding="utf-8")
    assert mpd.load_latest(str(tmp_path)) is None


def test_writer_writes_history_and_latest_and_strips_previous_trails(tmp_path, monkeypatch):
    monkeypatch.setattr(cs, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cs, "JSON_PATH", str(tmp_path / "market_data.json"))
    monkeypatch.setattr(cs, "CSV_PATH", str(tmp_path / "market_data.csv"))
    (tmp_path / "market_data.json").write_text(
        json.dumps([{"date": "2026-11-24T21:30:00+09:00", "summary": "A", "sector_rotation": _rot()}], ensure_ascii=False), encoding="utf-8")
    sd = {"S&P500": {"value": 7000.0, "change": 1.0, "change_percent": 0.1, "volume_ratio": 1.0, "date": "2026-11-25"}}
    cs.save_data_to_json_and_csv("B", sd, {"score": 50.0, "label": "NEUTRAL"}, implb={"sector_rotation": _rot()})
    hist = json.loads((tmp_path / "market_data.json").read_text(encoding="utf-8"))
    latest = json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    assert len(hist) == 2 and latest == hist[-1] and latest["summary"] == "B"
    assert "trail" in hist[-1]["sector_rotation"]["sectors"]["XLK"]                # 最新は trail を持つ
    assert "trail" not in hist[0]["sector_rotation"]["sectors"]["XLK"]             # 前のエントリの trail は消えた
    assert hist[0]["sector_rotation"]["sectors"]["XLK"]["quadrant"] == "Weakening"


def _mig():
    spec = importlib.util.spec_from_file_location("mig_latest", ROOT / "scripts" / "migrate_market_data_latest.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_migration_keeps_trail_when_not_reproducible_and_writes_latest(tmp_path, monkeypatch):
    mig = _mig()
    import sector_rotation as sr
    same_trail = _rot()["sectors"]["XLK"]["trail"]
    es = [{"date": "2026-11-23", "sector_rotation": _rot()}, {"date": "2026-11-24", "sector_rotation": _rot()}, {"date": "2026-11-25", "sector_rotation": _rot()}]
    es[1]["sector_rotation"]["sectors"]["XLK"]["trail"] = [{"week": "x", "ratio": 1, "momentum": 1}]     # 日足から作り直せない trail
    # 日足から作り直した trail は same_trail
    monkeypatch.setattr(sr, "sector_rotation", lambda gs, as_of: {"sectors": {"XLK": {"trail": same_trail}}})
    assert mig.not_reproducible(es, lambda *a, **k: []) == [(1, "2026-11-24")]
    p = tmp_path / "market_data.json"
    p.write_text(json.dumps(es, ensure_ascii=False, indent=2), encoding="utf-8")
    monkeypatch.setattr("common.market_data.reader.get_price_series_as_of", lambda *a, **k: [])
    assert mig.main([str(tmp_path)]) == 0
    out = json.loads(p.read_text(encoding="utf-8"))
    assert "trail" not in out[0]["sector_rotation"]["sectors"]["XLK"]             # 作り直せる → 消した
    assert out[1]["sector_rotation"]["sectors"]["XLK"]["trail"][0]["week"] == "x"   # 作り直せない → 残した
    assert "trail" in out[2]["sector_rotation"]["sectors"]["XLK"]                   # 最新は変えない
    assert json.loads((tmp_path / "latest.json").read_text(encoding="utf-8")) == out[-1]
    assert mig.main([str(tmp_path), "--check"]) == 1                                # 作り直せないエントリが残っている間は 1


def test_pipeline_live_fg_reads_latest_json(tmp_path):
    tdir = str(ROOT / "src" / "value" / "tanuki_valuation")
    sys.path.insert(0, tdir)
    try:
        spec = importlib.util.spec_from_file_location("tanuki_pipeline_latest_test", ROOT / "src" / "value" / "tanuki_valuation" / "pipeline.py")
        pl = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(pl)
    finally:
        sys.path.remove(tdir)
    d = tmp_path / "docs" / "market-monitor" / "market-pulse" / "data"
    d.mkdir(parents=True)
    (d / "latest.json").write_text(json.dumps({"date": "x", "fear_greed": {"score": 9.6}}), encoding="utf-8")
    obj = pl.TanukiValuationPipeline.__new__(pl.TanukiValuationPipeline)
    obj.repo_root = str(tmp_path)
    assert obj._load_live_fg() == 9.6
    obj2 = pl.TanukiValuationPipeline.__new__(pl.TanukiValuationPipeline)
    obj2.repo_root = str(tmp_path / "none")
    assert obj2._load_live_fg() == 50                                              # どちらも無ければ既定の50


def test_readers_do_not_read_the_history_for_the_latest_entry():
    """最新だけを読む所が market_data.json（履歴）を読み直していない（latest.json か共通の load_latest を使う）"""
    for rel, must in (("src/portfolio/snapshot.py", "load_latest"), ("src/value/tanuki_valuation/pipeline.py", "load_latest"),
                      ("browser_checks/check_dependency_map.py", "load_latest")):
        src = (ROOT / rel).read_text(encoding="utf-8")
        assert must in src, rel
    sh = (ROOT / "common" / "system_health.py").read_text(encoding="utf-8")
    assert '"latest.json"' in sh
    html = (ROOT / "docs" / "value-monitor" / "tanuki_score" / "index.html").read_text(encoding="utf-8")
    assert "market-pulse/data/latest.json" in html and "const M_PATH_HISTORY" in html
