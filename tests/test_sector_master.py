"""業種のマスター（common/sector_master.py → config/sector_master.json）。[[HYPECORE-STAGE-TIMESCALE-1]] 段B-1（2026-11-05）

決まり: 主は yfinance の sector・industry、SIC・社名は控え。区分と写像は業種の名前の規則（銘柄の一覧を書かない）。
yfinance の業種が変わった銘柄は [P] ExternalChange の件（kind "sector_change"）。今は読み手を変えない。
"""
import csv
import json
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import external_watch as ew
from common import sector_master as sm


def _setup(tmp_path, attrs, subs=None, status=None):
    attr_dir, sec_dir = tmp_path / "attributes", tmp_path / "sec"
    attr_dir.mkdir()
    sec_dir.mkdir()
    for t, (sector, industry) in attrs.items():
        (attr_dir / f"{t}.json").write_text(json.dumps({"symbol": t, "sector": sector, "industry": industry,
                                                        "fetched_at": "2026-11-01T00:00:00+00:00"}), encoding="utf-8")
    for t, s in (subs or {}).items():
        (sec_dir / t).mkdir()
        (sec_dir / t / "submissions.json").write_text(json.dumps(s), encoding="utf-8")
    lookup = tmp_path / "cik.csv"
    with open(lookup, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ticker", "cik", "status"])
        for i, t in enumerate(attrs):
            w.writerow([t, str(i + 1).zfill(10), (status or {}).get(t, "active")])
    return dict(path=str(tmp_path / "sector_master.json"), attr_dir=str(attr_dir), sec_dir=str(sec_dir), lookup_path=str(lookup))


def test_classification_by_industry_rules(tmp_path):
    kw = _setup(tmp_path, {
        "AAA": ("Industrials", "Farm & Heavy Construction Machinery"),   # 段Aの4業種（画面の語には無い）
        "BBB": ("Consumer Cyclical", "Auto Manufacturers"),              # 画面の語（Auto）
        "CCC": ("Healthcare", "Biotechnology"),                          # 区分なし
        "DDD": ("Financial Services", "Credit Services"),                # SIC 6xxx → 金融
        "EEE": ("Financial Services", "Credit Services"),                # SIC 7389 → 金融ではない（決済網）
        "FFF": ("Financial Services", "Financial Data & Stock Exchanges"),
        "GGG": ("Technology", "Software - Infrastructure"),
        "HHH": (None, None),                                             # 属性なし
    }, subs={"DDD": {"sic": "6199", "sic_description": "Finance Services", "entity_name": "D"},
             "EEE": {"sic": "7389", "sic_description": "Services-Business Services, NEC", "entity_name": "E"}})
    m = sm.build(**kw, now="2026-11-05T00:00:00+00:00")
    T = m["tickers"]
    assert T["AAA"]["cyclical"] and T["BBB"]["cyclical"] and not T["GGG"]["cyclical"]
    assert T["CCC"]["unclassified"] and T["HHH"]["unclassified"] and not T["GGG"]["unclassified"]
    assert T["DDD"]["financial"] == {"value": True, "basis": "sector+sic"}
    assert T["EEE"]["financial"] == {"value": False, "basis": "sector+sic"}
    assert T["FFF"]["financial"]["value"] is False
    assert T["GGG"]["financial"] == {"value": False, "basis": "sector_only"}
    assert T["DDD"]["sic"] == 6199 and T["EEE"]["sic_description"].startswith("Services")
    assert T["AAA"]["eps_category"] == "一般消費財 / 製造 / EV" and T["GGG"]["eps_category"] == "Technology"
    assert T["GGG"]["damodaran_industry"] == "Software (System & Application)" and T["GGG"]["etf"] == "XLK"
    assert T["HHH"]["eps_category"] is None and T["HHH"]["etf"] is None
    assert m["rules"]["financial_sic_range"] == [6000, 6799]


def test_retired_not_in_master_and_single_ticker_keeps_others(tmp_path):
    kw = _setup(tmp_path, {"AAA": ("Technology", "Semiconductors"), "ZZZ": ("Technology", "Semiconductors")},
                status={"ZZZ": "retired"})
    assert set(sm.build(**kw)["tickers"]) == {"AAA"}
    assert sm.write(sm.build(**kw), kw["path"]) is True
    assert sm.write(sm.build(**kw), kw["path"]) is False            # 中身が同じなら書かない
    # 新しい銘柄（provisioning）を1銘柄だけ足す
    with open(kw["lookup_path"], "a", encoding="utf-8", newline="") as f:
        csv.writer(f).writerow(["NEW", "0000000009", "provisioning"])
    (tmp_path / "attributes" / "NEW.json").write_text(json.dumps({"sector": "Energy", "industry": "Oil & Gas Integrated"}),
                                                      encoding="utf-8")
    m = sm.build(["NEW"], **kw)
    assert set(m["tickers"]) == {"AAA", "NEW"} and m["tickers"]["NEW"]["cyclical"]


def test_industry_change_goes_to_external_change(tmp_path):
    kw = _setup(tmp_path, {"AAA": ("Technology", "Software - Application")})
    sm.write(sm.build(**kw), kw["path"])
    (tmp_path / "attributes" / "AAA.json").write_text(json.dumps({"sector": "Technology", "industry": "Software - Infrastructure"}),
                                                      encoding="utf-8")
    sm.write(sm.build(**kw), kw["path"])
    items = sm.change_items(kw["path"])
    assert items == [{"kind": "sector_change", "ticker": "AAA", "match": "industry:Software - Application→Software - Infrastructure",
                      "detail": items[0]["detail"]}]
    sm.write(sm.build(**kw), kw["path"])
    assert len(sm.change_items(kw["path"])) == 1                     # 同じ変化は1件のまま（台帳に入れるまで残る）
    new, ack = ew.split(items, [{"kind": "sector_change", "ticker": "AAA", "match": items[0]["match"]}])
    assert new == [] and len(ack) == 1
    assert "sector_change" in ew.KIND_LABELS
    # 属性が一時的に無い（空）回は変化に数えない
    (tmp_path / "attributes" / "AAA.json").write_text(json.dumps({}), encoding="utf-8")
    assert len(sm.build(**kw)["changes"]) == 1


def test_external_watch_collects_sector_changes(tmp_path):
    p = tmp_path / "sm.json"
    p.write_text(json.dumps({"changes": [{"kind": "sector_change", "ticker": "A", "match": "sector:X→Y", "detail": "d",
                                          "detected_at": "t"}]}), encoding="utf-8")
    empty = tmp_path / "w.json"
    empty.write_text("{}", encoding="utf-8")
    (tmp_path / "daily").mkdir()
    items = ew.collect(str(empty), str(tmp_path / "daily"), str(p))
    assert [i["kind"] for i in items] == ["sector_change"]


def test_no_ticker_list_in_rules():
    """規則は業種の名前だけ（銘柄の一覧を書かない、CHAT_RULESのハードコード禁止）"""
    from common.sec_data.tickers import get_all_tickers
    src = open(os.path.join(REPO, "common", "sector_master.py"), encoding="utf-8").read()
    quoted = set(re.findall(r'"([A-Z][A-Z0-9.\-]{0,5})"', src))
    assert not (quoted & set(get_all_tickers())), quoted & set(get_all_tickers())


def test_mappings_match_existing_names():
    """写像の表の名前が今の読み手の名前と同じ（ETFは Market Pulse の表と一致、Damodaranは growth_sanity の業種名、EPSは sectors.yaml の区分）"""
    sys.path.insert(0, os.path.join(REPO, "src", "value", "tanuki_valuation"))
    import yaml
    import growth_sanity as gs
    import maturity_config as mc
    from src.market.market_pulse.sector_rotation import SECTOR_MAP
    assert sm.ETF_BY_SECTOR == SECTOR_MAP
    known = set(gs.SECTOR_TO_DAMODARAN.values()) | set(gs.TICKER_INDUSTRY_OVERRIDES.values()) | set(mc._DAMODARAN_TV_G)
    assert set(sm.DAMODARAN_BY_INDUSTRY.values()) <= known
    eps_names = {s["name"] for s in yaml.safe_load(open(os.path.join(REPO, "config", "sectors.yaml"), encoding="utf-8"))["sectors"]}
    assert set(sm.EPS_CATEGORY_BY_SECTOR.values()) | set(sm.EPS_CATEGORY_BY_INDUSTRY.values()) <= eps_names


def test_saved_master_covers_registered_tickers():
    """config/sector_master.json に登録簿（retired 以外）の全銘柄がある（作り直し忘れの検知）"""
    m = sm.load()
    assert set(m.get("tickers") or {}) == set(sm.registered_tickers())
