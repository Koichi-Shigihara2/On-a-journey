"""EPS ANALYZERの区分は業種のマスターの eps_category だけで決める（[[SECTOR-MASTER-ADOPT-1]] 段4、2026-11-11）。

以前の決め方（eps_sector の手設定 > SICコード > 会社名のキーワード > ティッカーのキーワード）は廃止した。
sectors.yaml は区分の name と exclusions だけ。写像は financial に従う（金融でない Financial Services は Technology）。
"""
import csv
import os
import sys

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import sector_master as sm  # noqa: E402
from src.value.adjusted_eps_analyzer.sector_classifier_v2 import SectorClassifierV2  # noqa: E402

SECTORS_YAML = os.path.join(REPO, "config", "sectors.yaml")
CIK_CSV = os.path.join(REPO, "config", "cik_lookup.csv")


def test_eps_category_follows_financial():
    assert sm.eps_category("Financial Services", "Credit Services", True) == "フィンテック / 銀行"      # SOFI
    assert sm.eps_category("Financial Services", "Credit Services", False) == "Technology"            # V（SIC 7389）
    assert sm.eps_category("Financial Services", "Financial Data & Stock Exchanges", False) == "Technology"   # MSCI
    # financial を渡さない呼び出しは今までどおり（industry・sector だけ）
    assert sm.eps_category("Financial Services", "Credit Services") == "フィンテック / 銀行"
    assert sm.eps_category("Industrials", "Aerospace & Defense", False) == "宇宙・航空"


def test_classify_reads_master_for_the_former_manual_and_sic_cases():
    c = SectorClassifierV2(SECTORS_YAML)
    assert c.classify("RKLB") == "宇宙・航空"                 # 手設定（宇宙・航空）は写像と同じ
    assert c.classify("SPIR") == "一般消費財 / 製造 / EV"     # 手設定（Technology）は写像に寄せた
    assert c.classify("VRT") == "一般消費財 / 製造 / EV"
    assert c.classify("RCAT") == "宇宙・航空"                 # SIC 7372（ソフトウェア）でなく industry（Aerospace & Defense）
    assert c.classify("V") == "Technology" and c.classify("MSCI") == "Technology"
    assert c.classify("SOFI") == "フィンテック / 銀行"
    assert c.classify("") is None


def test_classify_ticker_not_in_master_is_built_from_attributes(monkeypatch):
    monkeypatch.setattr(sm, "master_entry", lambda t, path=sm.MASTER_PATH: None)
    monkeypatch.setattr(sm, "build_entry", lambda t, *a, **k: {"eps_category": "ヘルスケア / バイオ"})
    assert SectorClassifierV2(SECTORS_YAML).classify("NEWCO") == "ヘルスケア / バイオ"
    monkeypatch.setattr(sm, "build_entry", lambda t, *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    assert SectorClassifierV2(SECTORS_YAML).classify("NEWCO") is None


def test_every_eps_ticker_gets_a_category():
    """EPS ANALYZERの対象（cik_lookup.csvのeps=true、retired以外）は全銘柄に区分が付く（以前は41銘柄が出なかった）"""
    c = SectorClassifierV2(SECTORS_YAML)
    names = set(c.get_all_sectors())
    with open(CIK_CSV, encoding="utf-8-sig", newline="") as f:
        rows = [r for r in csv.DictReader(f) if r["eps"].lower() == "true" and r["status"] != "retired"]
    missing = [r["ticker"] for r in rows if c.classify(r["ticker"]) not in names]
    assert missing == []


def test_sectors_yaml_has_only_name_and_exclusions():
    d = yaml.safe_load(open(SECTORS_YAML, encoding="utf-8"))["sectors"]
    assert d and all(set(s) <= {"name", "exclusions"} for s in d)
    assert set(sm.EPS_CATEGORY_BY_SECTOR.values()) | set(sm.EPS_CATEGORY_BY_INDUSTRY.values()) <= {s["name"] for s in d}


def test_cik_lookup_has_no_eps_sector_column():
    with open(CIK_CSV, encoding="utf-8-sig", newline="") as f:
        assert "eps_sector" not in csv.DictReader(f).fieldnames
