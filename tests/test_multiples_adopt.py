"""[[MULTIPLES-ADOPT-1]]（2026-11-13の決定）の回帰テスト。

- 部品の `current` の行 × 生の株価（common/multiples.py::at_price）。時価総額の作り方は1か所
- GAAP PERの列にforwardが混ざらない（部品がNoneならNone。forwardは別の列）
- yfinanceの時価総額は検証（gap）と、株数が欠ける銘柄の代わり（印つき）だけ
- P/TBVの履歴のTCEは時点の値（rotce.py）
"""
import os
import re

from common import multiples as M

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

ROW = {"shares": 100.0, "net_debt": 50.0, "ttm_net_income": 200.0, "ttm_revenue": 1000.0, "ttm_gross_profit": 400.0,
       "net_income_5y_avg": 100.0, "tce": 500.0, "ttm_end": "2026-09-30"}


def test_at_price_uses_live_price_times_part_shares():
    r = M.at_price(ROW, 20.0, yf_market_cap=2400.0)
    assert r["market_cap"] == 2000.0 and r["market_cap_source"] == "sec"
    assert r["per"] == 10.0 and r["ps"] == 2.0 and r["p_tbv"] == 4.0
    assert r["enterprise_value"] == 2050.0 and abs(r["ev_sales"] - 2.05) < 1e-12
    assert abs(r["market_cap_yf_gap"] - 0.2) < 1e-12      # yf÷部品−1（検証用）


def test_yfinance_market_cap_is_used_only_when_shares_missing_and_marked():
    r = M.at_price({**ROW, "shares": None}, 20.0, yf_market_cap=2400.0)
    assert r["market_cap"] == 2400.0 and r["market_cap_source"] == M.MCAP_SOURCE_YF_FALLBACK
    assert r["per"] == 12.0 and r["market_cap_yf_gap"] is None
    r2 = M.at_price({**ROW, "shares": None}, 20.0)
    assert r2["market_cap"] is None and r2["per"] is None            # yfも無ければ出さない


def test_multiple_is_none_not_forward_when_denominator_not_positive():
    r = M.at_price({**ROW, "ttm_net_income": -5.0}, 20.0)
    assert r["per"] is None
    assert M.at_price(None, 20.0)["per"] is None and M.at_price(ROW, None)["per"] is None


def test_compute_makes_current_row_even_without_shares():
    """株数が無い銘柄（V型）でも current の行に分母・純負債が入る。月末の行は今までどおり判定不能"""
    rows = [{"date": "2026-10-30", "close": 10.0}]
    store = {"fields": {"net_income": {"entries": []}}}
    out = M.compute("ZZZ", store, rows, [("2026-10", "2026-10-30")], [], lambda d: {}, [], current="2026-10-30")
    assert out["current"]["undetermined"] == "no_shares" and "net_debt" in out["current"]
    assert out["months"][0]["undetermined"] == "no_shares" and "net_debt" not in out["months"][0]


def _src(rel):
    with open(os.path.join(REPO, rel), encoding="utf-8") as f:
        return f.read()


def test_market_cap_is_built_in_one_place():
    """時価総額（株価×株数）を作るのは common/multiples.py::at_price だけ。読み手はyfinanceの値を引数に渡すだけで、
    自分で時価総額や PSR を組み立てない"""
    for rel in ("src/value/tanuki_valuation/data_fetcher.py", "src/value/stonks_silo/valuation_fetcher.py",
                "src/value/hypecore/hypecore.py"):
        s = _src(rel)
        assert "current_multiples(" in s, rel
        for m in re.finditer(r'attrs\.get\("(market_cap|price_to_sales|trailing_pe)"\)', s):
            line = s[s.rfind("\n", 0, m.start()) + 1: s.find("\n", m.end())]
            assert m.group(1) == "market_cap" and ("yf_market_cap" in line or "current_multiples" in line), (rel, line)
    html = _src("docs/value-monitor/tanuki_valuation/stock.html")
    assert "ps_ttm_sec" in html
    assert all("旧キー読み" in l for l in html.splitlines() if "currentPrice * dilutedSharesVal" in l)   # 旧データ用の読みだけ


def test_per_column_never_mixes_forward():
    s = _src("src/value/tanuki_valuation/data_fetcher.py")
    assert "per_is_forward" not in s and "_trailing_pe or _forward_pe" not in s
    assert "per_forward_yf" in s
    sc = _src("docs/value-monitor/tanuki_score/index.html")
    # 旧キー読み（MULTIPLES-ADOPT-1の残り）の行以外に per_is_forward を使わない
    assert "perIsFwd" not in sc and "per_forward_yf" in sc
    assert all("旧キー読み" in l for l in sc.splitlines() if "per_is_forward" in l)


def test_rotce_ptbv_history_uses_first_val_tce():
    s = _src("common/sec_data/rotce.py")
    assert "_tce_pit(q)" in s and "tce_first_val" in s


def test_old_key_fallback_never_turns_forward_into_gaap_per():
    """公開直後の旧データ（per_is_forward=true のとき per は予想PER）でも、GAAP PERの列には入らない"""
    from common.screening.dcf_validity_checker import check_f_eps_analyzer_delta as F
    assert F({"components": {"per": 28.0, "per_is_forward": True, "per_adjusted": 68.0}})[0] is False
    assert F({"components": {"per": 14.0, "per_adjusted": 3.5}})[0] is True            # 旧データ（GAAP）
    assert F({"components": {"per_gaap_ttm_sec": 14.0, "per_adjusted_ttm": 3.5}})[0] is True


def test_every_renamed_key_reader_has_old_key_fallback_marker():
    for rel in ("docs/value-monitor/tanuki_score/index.html", "docs/value-monitor/tanuki_valuation/stock.html",
                "docs/value-monitor/adjusted_eps_analyzer/stock.html", "docs/value-monitor/hypecore/detail.html"):
        assert "旧キー読み" in _src(rel), rel
