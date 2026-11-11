"""金融の判定は業種のマスターの financial（[[SECTOR-MASTER-ADOPT-1]] 段3、2026-11-11）。

yfinance の sector=="Financial Services" の直接比較をやめ、common/sector_master.is_financial_ticker に1か所に寄せた。
マスターにある銘柄はマスターの financial（sector＋SIC）、無い銘柄（登録の直後）は sector・industry だけの financial() と同じ。
保険の判定（_is_insurance）は別の概念として残す。
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "src", "value", "tanuki_valuation"))
from common import sector_master as sm  # noqa: E402
from calculator.adjustments import _get_rpo_application_rate  # noqa: E402
from calculator.rice import calculate_rice  # noqa: E402


def _master(tmp_path, entries):
    p = tmp_path / "sector_master.json"
    p.write_text(json.dumps({"tickers": {t: {"financial": {"value": v}} for t, v in entries.items()}}), encoding="utf-8")
    return str(p)


def test_master_value_wins_over_sector_string(tmp_path):
    p = _master(tmp_path, {"V": False, "SOFI": True})
    assert sm.is_financial_ticker("V", "Financial Services", "Credit Services", path=p) is False
    assert sm.is_financial_ticker("SOFI", "Financial Services", "Credit Services", path=p) is True
    assert sm.is_financial_ticker("sofi", None, None, path=p) is True          # sector を渡さなくてもマスターで決まる


def test_ticker_not_in_master_follows_sector_and_industry(tmp_path):
    p = _master(tmp_path, {})
    assert sm.is_financial_ticker("NEW", "Financial Services", "Banks - Regional", path=p) is True
    assert sm.is_financial_ticker("NEW", "Financial Services", "Financial Data & Stock Exchanges", path=p) is False
    assert sm.is_financial_ticker("NEW", "Technology", "Software", path=p) is False
    assert sm.is_financial_ticker("", "", "", path=p) is False


def test_missing_master_file_falls_back(tmp_path):
    assert sm.is_financial_ticker("V", "Financial Services", None, path=str(tmp_path / "none.json")) is True


def test_real_master_v_msci_not_financial_sofi_financial():
    """実際のマスター（SIC 7389のV・MSCIは金融でない、SOFIは SIC 6199 で金融）"""
    assert sm.is_financial_ticker("V", "Financial Services") is False
    assert sm.is_financial_ticker("MSCI", "Financial Services") is False
    assert sm.is_financial_ticker("SOFI", "Financial Services") is True


def test_rpo_fintech_rate_follows_master():
    """金融（SOFI）は Fintech 50%、V・MSCI は sector 既定（Financial Services は表に無く 0%）。保険は 0% のまま"""
    assert _get_rpo_application_rate("Financial Services", "SOFI", "Credit Services")[:2] == (0.5, "Fintech")
    assert _get_rpo_application_rate("Financial Services", "V", "Credit Services")[1] == "Non-SaaS"
    assert _get_rpo_application_rate("Financial Services", "ALL", "Insurance - Property & Casualty")[1] == "Insurance"


def test_rice_excluded_for_financial_only():
    """RICEの除外は金融（マスター）。V は計算対象に入る（除外の note が付かない）、SOFI は除外"""
    def run(t, sector, industry):
        return calculate_rice(annual_data=[], wacc=0.1, scenario_valuations=None, sector=sector, industry=industry, ticker=t)
    assert "セクター除外" in run("SOFI", "Financial Services", "Credit Services").note
    assert "セクター除外" not in (run("V", "Financial Services", "Credit Services").note or "")
    assert "セクター除外" in run("KO", "Consumer Defensive", "Beverages").note    # 金融以外の除外sectorは今のまま


def test_net_cash_guard_follows_master(monkeypatch):
    """get_net_cash の金融ガード（短期負債0）は金融（マスター）だけ。V はガードなし"""
    from common.sec_data.reader import SECReader
    r = SECReader()
    nc = r.get_net_cash("V", sector="Financial Services", industry="Credit Services")
    assert nc.get("sector_guard") == "none"
    nc = r.get_net_cash("SOFI", sector="Financial Services", industry="Credit Services")
    assert nc.get("sector_guard") == "fintech_st_debt_excluded"
