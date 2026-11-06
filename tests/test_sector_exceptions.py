"""Damodaranの業種の例外の台帳（config/sector_exceptions.json）。[[SECTOR-MASTER-ADOPT-1]] 段1（2026-11-06）

銘柄ごとの手書きの辞書（TICKER_INDUSTRY_OVERRIDES・beta_config.overrides.sector）をやめ、業種は
1. 例外の台帳（理由つき、写像と違う銘柄だけ）→ 2. 業種のマスターの写像（全銘柄）の順で引く。
テストは作った値と読み取りだけ（外部の取得に触れない）
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _tanuki_pipeline_stub import load_growth_sanity_with_stub_xlrd  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_gs = load_growth_sanity_with_stub_xlrd()


def _ledger():
    return json.load(open(os.path.join(REPO, "config", "sector_exceptions.json"), encoding="utf-8"))["damodaran_industry"]


def test_every_exception_has_a_reason_and_differs_from_the_mapping():
    """台帳の各銘柄は理由があり、写像と違う（同じなら例外ではない＝台帳に置かない）"""
    master = json.load(open(os.path.join(REPO, "config", "sector_master.json"), encoding="utf-8"))["tickers"]
    for t, e in _ledger().items():
        assert e.get("reason", "").strip(), f"{t}: 理由の無い例外は残さない"
        assert t in master, f"{t}: 登録簿に無い銘柄の例外"
        assert e["industry"] != master[t]["damodaran_industry"], f"{t}: 写像と同じなので例外ではない"


def test_decided_exceptions_only():
    """決定（チャット側 2026-11-06）: 台帳に残すのは APP・META・NET・CIX・KULR・PAYS。ABBV・ASTS・CRWV・HQYは写像に寄せた"""
    assert set(_ledger()) == {"APP", "META", "NET", "CIX", "KULR", "PAYS"}


def test_industry_lookup_order_exception_then_master():
    t = _gs._load_sector_tables()
    assert _gs.industry_for_ticker("APP") == "Software (System & Application)"           # 台帳
    assert _gs.industry_for_ticker("ABBV") == t["master"]["ABBV"]                         # 写像
    assert _gs.industry_for_ticker("NO_SUCH_TICKER") is None
    assert not hasattr(_gs, "TICKER_INDUSTRY_OVERRIDES")                                  # 手書きの辞書はやめた


def test_get_industry_benchmark_uses_ledger_then_master_then_sector():
    bench = {"Software (System & Application)": {"g_ebit": 0.2, "roc": 0.1, "rr": 0.5},
             "Software (Internet)": {"g_ebit": 0.1, "roc": 0.1, "rr": 0.5}}
    _gs._damodaran_data.update(bench)
    _gs._damodaran_loaded = True
    try:
        assert _gs.get_industry_benchmark("APP", None)["industry"] == "Software (System & Application)"
    finally:
        for k in bench:
            _gs._damodaran_data.pop(k, None)
