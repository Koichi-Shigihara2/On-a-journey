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


# ───────── 段2（永久成長率・メガテック）[[SECTOR-MASTER-ADOPT-1]] 2026-11-06 ─────────
sys.path.insert(0, os.path.join(REPO, "src", "value", "tanuki_valuation"))
sys.path.insert(0, REPO)


def test_terminal_growth_exceptions_need_a_reason():
    """永久成長率の例外は理由つきだけ（今は0件。直引きの63銘柄は WACC-1 の『業種の規則を当てた結果のキャッシュ』で理由の記録が無く規則に寄せた）"""
    led = json.load(open(os.path.join(REPO, "config", "sector_exceptions.json"), encoding="utf-8"))
    for t, e in (led.get("terminal_growth") or {}).items():
        assert isinstance(e.get("value"), (int, float)) and e.get("reason", "").strip(), f"{t}: 値と理由が要る"


def test_terminal_growth_follows_the_industry_rule_not_a_per_ticker_table():
    import maturity_config as mc
    assert not hasattr(mc, "_TICKER_TV_G")
    master = json.load(open(os.path.join(REPO, "config", "sector_master.json"), encoding="utf-8"))["tickers"]
    gs = load_growth_sanity_with_stub_xlrd()
    # growth_sanity は別のモジュールとして読んだものなので、maturity_config が import する本物の方で確かめる
    import growth_sanity as real_gs
    for t, e in master.items():
        ind = real_gs.industry_for_ticker(t)
        rule = mc._DAMODARAN_TV_G.get(ind)
        assert mc.get_terminal_growth(t) == (rule if rule is not None else 0.03), t
    assert mc.get_terminal_growth("VST") == 0.02                      # 業種の規則（Power）で出る。直引きは要らない
    assert mc.get_terminal_growth("HON") == 0.03                      # 業種の表に無い業種（Diversified）は既定
    assert gs is not None


def test_terminal_growth_exception_ledger_wins(monkeypatch):
    import growth_sanity as real_gs
    import maturity_config as mc
    monkeypatch.setitem(real_gs._load_sector_tables()["terminal_growth"], "NVDA", 0.041)
    assert mc.get_terminal_growth("NVDA") == 0.041


def test_mega_tech_is_a_rule_on_market_cap_and_industry():
    from common import sector_master as sm
    assert sm.is_mega_tech("Software - Infrastructure", 3.9e12)
    assert sm.is_mega_tech("Internet Retail", 2.6e12)
    assert sm.is_mega_tech("Semiconductors", 1.2e12)
    assert sm.is_mega_tech("Consumer Electronics", 5.0e12)
    assert not sm.is_mega_tech("Auto Manufacturers", 1.6e12)              # TSLA: 業種で外れる
    assert not sm.is_mega_tech("Drug Manufacturers - General", 1.3e12)   # LLY: 業種で外れる
    assert not sm.is_mega_tech("Semiconductor Equipment & Materials", 2e12)
    assert not sm.is_mega_tech("Software - Application", 9.9e11)          # 1兆ドル未満
    assert not sm.is_mega_tech(None, 2e12) and not sm.is_mega_tech("Software - Application", None)


def test_mega_tech_rule_gives_the_same_six_tickers_as_the_old_hand_list():
    """今の登録銘柄では、規則で出る銘柄が以前の手書きの6銘柄（_mega_tech_tickers）と同じ"""
    import maturity_config as mc
    master = json.load(open(os.path.join(REPO, "config", "sector_master.json"), encoding="utf-8"))["tickers"]
    assert {t for t, e in master.items() if e.get("mega_tech")} == {"MSFT", "AMZN", "GOOGL", "AAPL", "META", "NVDA"}
    assert mc.is_mega_tech_ticker("NVDA") and not mc.is_mega_tech_ticker("TSLA") and not mc.is_mega_tech_ticker("LLY")
    cfg = json.load(open(os.path.join(REPO, "config", "maturity_config.json"), encoding="utf-8"))
    assert "_mega_tech_tickers" not in cfg["_sector_caps"] and cfg["_sector_caps"]["_mega_tech_cap"] == 0.15
