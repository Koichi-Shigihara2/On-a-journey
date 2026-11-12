"""DEFICIT-QUALITY-ADOPT-1（2026-11-12）: 黒字化の予測は収益の部品の1か所・STONKSの対象は持続の条件・粗利率の導出・型④の区別。

- 黒字化の規則（回帰）が common/profitability.py の1か所にだけあること（STONKS・TANUKI に別の回帰が残っていない）
- STONKS の対象の選び方は stage_v2 の持続の条件（2か月）を通すので、境目の銘柄が1か月の符号で出入りしない
- 手で書くフラグ（cik_lookup.csv の stonks_silo 列）が無い
- 粗利率は粗利が無ければ 売上−売上原価 から導出する（STONKS の gross_margin_derived と同じ）
"""
import json
import os
import re
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common import profitability as PF  # noqa: E402
from common import stonks_target as ST  # noqa: E402


def _read(rel):
    with open(os.path.join(REPO, rel), encoding="utf-8") as f:
        return f.read()


# ── 黒字化の規則は1か所 ─────────────────────────────────
def test_breakeven_rule_lives_only_in_the_component():
    """回帰で黒字化の年を出す関数は部品の breakeven() だけ。STONKS の会計の黒字化・TANUKI の調整後EPSの黒字化は無い"""
    pipe = _read("src/value/tanuki_valuation/pipeline.py")
    assert "def compute_eps_breakeven" not in pipe and '"breakeven_estimate"' not in pipe and '"breakeven_reason"' not in pipe
    ana = _read("src/value/stonks_silo/analyzer.py")
    assert "_gaap_margin_breakeven" not in ana and "gaap_breakeven" not in ana
    assert "breakeven_view" in ana and "breakeven_view" in pipe
    assert "def breakeven(" in _read("common/profitability.py")


def test_ocf_breakeven_stays_as_the_cash_axis_with_its_own_name():
    ana = _read("src/value/stonks_silo/analyzer.py")
    assert "ocf_breakeven_year" in ana and "def _margin_breakeven_detail" in ana
    assert "op_breakeven_year" in ana                                  # 会計の黒字化は別の名前


@pytest.mark.parametrize("entry,state,year,reason", [
    (None, "unknown", None, "NO_ENTRY"),
    ({"revenue_stage": "profit"}, "achieved", None, "ACHIEVED"),
    ({"revenue_stage": "near_profit", "breakeven": {"year": 2027, "reason": "PREDICTED", "r2": 0.8}}, "predicted", 2027, "PREDICTED"),
    ({"revenue_stage": "loss", "breakeven": {"year": 2029, "reason": "TOO_FAR", "r2": 0.9}}, "predicted", 2029, "TOO_FAR"),
    ({"revenue_stage": "loss", "breakeven": {"year": None, "reason": "LOW_FIT", "r2": 0.39}}, "unknown", None, "LOW_FIT"),
    ({"revenue_stage": "unclassified"}, "unknown", None, "UNCLASSIFIED"),
])
def test_breakeven_view(entry, state, year, reason):
    v = PF.breakeven_view(entry)
    assert (v["state"], v["year"], v["reason"]) == (state, year, reason)


# ── STONKS の対象: 持続の条件 ──────────────────────────────
def _h(*states, basis=None):
    """月末ごとの行 [月末, 状態, 根拠]。basis を渡すと状態ごとの根拠（既定は TTM、unknown は none）"""
    return [[f"2026-{i + 1:02d}-28", st, (basis[i] if basis else ("none" if st == "unknown" else "ttm"))] for i, st in enumerate(states)]


@pytest.mark.parametrize("states,want", [
    (("loss", "loss", "loss", "loss"), "loss"),
    (("loss", "loss", "loss", "profit"), "loss"),               # 1か月だけ黒字になっても出ない（境目の銘柄が出入りしない）
    (("loss", "loss", "profit", "profit"), "profit"),           # 2か月続けて黒字になって初めて外れる
    (("profit", "profit", "loss", "profit"), "profit"),         # 1か月だけ赤字でも入らない
    (("profit", "profit", "loss", "loss"), "loss"),             # 2か月続けて赤字で入る
    (("loss", "loss", "unknown", "unknown"), "loss"),           # 判定不能は直前の状態を保つ
    (("unknown", "unknown", "unknown", "unknown"), None),
])
def test_persistence_of_the_operating_state(states, want):
    assert ST.persisted_state(_h(*states)) == want


def test_persistence_uses_the_stage_v2_function_not_a_new_threshold():
    from common import stage_v2
    assert stage_v2.PERSIST_MONTHS == 2
    src = _read("common/stonks_target.py")
    assert "from common.stage_v2 import persist" in src
    assert not re.search(r"\b(PERSIST|THRESHOLD)\w*\s*=", src)          # 新しい閾値を作っていない


def _prof(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"tickers": {t: {"operating_state_history": _h(*s)} for t, s in rows.items()}}, f)


def _csv(path, tickers):
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("ticker,cik,name,tanuki,eps,hypecore,status\n")
        for t, st in tickers.items():
            f.write(f"{t},0,{t},true,true,true,{st}\n")


def test_targets_apply_persistence_financials_and_status(tmp_path):
    prof, csvp = str(tmp_path / "p.json"), str(tmp_path / "c.csv")
    _prof(prof, {"AAA": ("loss",) * 4, "BBB": ("loss", "loss", "loss", "profit"), "CCC": ("profit",) * 4,
                 "DDD": ("loss",) * 4, "EEE": ("loss",) * 4, "FFF": ("loss",) * 4})
    _csv(csvp, {"AAA": "active", "BBB": "active", "CCC": "active", "DDD": "retired", "EEE": "provisioning", "FFF": "active"})
    fin = lambda t: t == "FFF"                                       # 金融（業種のマスターの判定）は除く
    assert ST.targets(prof, csvp, is_financial=fin) == ["AAA", "BBB"]
    assert ST.targets(prof, csvp, include_provisioning=True, is_financial=fin) == ["AAA", "BBB", "EEE"]
    assert ST.explain("FFF", prof, is_financial=fin)["target"] is False
    assert ST.explain("BBB", prof, is_financial=lambda t: False)["target"] is True


def test_missing_ttm_months_do_not_make_the_ticker_flip():
    """TTM営業利益が取れない月は直前の状態を保つ（出入りしない）。印 held を付ける"""
    assert ST.persisted_state(_h("loss", "loss", "unknown", "unknown")) == "loss"
    assert ST.persisted_state(_h("profit", "profit", "unknown", "loss")) == "profit"      # 判定できた月が1回だけでは変えない
    assert ST.marks(_h("loss", "loss", "unknown", "loss")) == ["held"]
    assert ST.marks(_h("loss", "loss", "loss", "loss")) == []


def test_never_ttm_ticker_uses_the_latest_annual_sign_with_a_mark():
    h = _h("loss", "loss", "loss", "loss", basis=["annual"] * 4)
    assert ST.persisted_state(h) == "loss" and ST.marks(h) == ["annual"]
    h2 = _h("profit", "profit", "profit", "profit", basis=["annual"] * 4)
    assert ST.persisted_state(h2) == "profit"


def test_no_data_ticker_is_excluded_with_a_mark(tmp_path):
    h = _h("unknown", "unknown", "unknown", "unknown")
    assert ST.persisted_state(h) is None and ST.marks(h) == ["no_data"]
    prof, csvp = str(tmp_path / "p.json"), str(tmp_path / "c.csv")
    _prof(prof, {"AAA": ("loss",) * 4, "NOD": ("unknown",) * 4})
    _csv(csvp, {"AAA": "active", "NOD": "active"})
    assert ST.targets_with_marks(prof, csvp, is_financial=lambda t: False) == {"AAA": []}
    assert ST.explain("NOD", prof, is_financial=lambda t: False)["marks"] == ["no_data"]


def test_no_hand_written_exceptions_remain():
    """pinned・excluded・手書きのフラグ列は無い。例外は規則（金融の判定・TTM欠けの扱い）だけ"""
    assert not os.path.exists(os.path.join(REPO, "config", "stonks_target.json"))
    src = _read("common/stonks_target.py")
    assert "pinned" not in src.split('"""', 2)[2] and "excluded" not in src.split('"""', 2)[2]
    assert "is_financial_ticker" in src


def test_missing_history_stops_instead_of_an_empty_target_list(tmp_path):
    """月末の履歴が1銘柄も無いときは空の対象で走らせず、止める（公開の順を間違えた場合に STONKS の結果を空にしない）"""
    prof, csvp = str(tmp_path / "p.json"), str(tmp_path / "c.csv")
    with open(prof, "w", encoding="utf-8") as f:
        json.dump({"tickers": {"AAA": {"revenue_stage": "loss"}}}, f)
    _csv(csvp, {"AAA": "active"})
    with pytest.raises(RuntimeError):
        ST.targets(prof, csvp, is_financial=lambda t: False)
    with pytest.raises(RuntimeError):
        ST.targets(str(tmp_path / "none.json"), csvp, is_financial=lambda t: False)


def test_flag_column_is_gone_and_readers_use_the_derived_list():
    head = _read("config/cik_lookup.csv").splitlines()[0]
    assert "stonks_silo" not in head.split(",")
    tk = _read("common/sec_data/tickers.py")
    assert "stonks_target.targets" in tk
    from common.sec_data import tickers as T
    assert T.get_stonks_silo_tickers() == T.get_active_tickers("stonks_silo") == ST.targets()
    assert not os.path.exists(os.path.join(REPO, "common", "sec_data", "stonks_flag_rule.py"))
    assert not os.path.exists(os.path.join(REPO, "config", "stonks_flag_rule.json"))
    from common.sector_master import is_financial_ticker
    assert is_financial_ticker("SOFI") and "SOFI" not in ST.targets()      # 金融は規則（業種のマスター）で除く
    assert "ASTS" in ST.targets()                                          # 手書きの固定なしで、TTMの営業利益から赤字と判定される


# ── 部品: 月末の符号・粗利率の導出・型④ ─────────────────────
def test_month_ends_upto():
    assert PF.month_ends_upto("2026-11-12") == ["2026-07-31", "2026-08-31", "2026-09-30", "2026-10-31"]
    assert PF.month_ends_upto("2026-10-31") == ["2026-07-31", "2026-08-31", "2026-09-30", "2026-10-31"]
    assert PF.month_ends_upto("2026-01-15", 3) == ["2025-10-31", "2025-11-30", "2025-12-31"]


def _q(end, val, filed):
    from datetime import date, timedelta
    e = date.fromisoformat(end)
    return {"end": end, "start": (e - timedelta(days=90)).isoformat(), "val": val, "first_val": val, "filed": filed,
            "first_filed": filed, "period_days": 90, "is_annual": False, "is_ytd": False}


def test_operating_state_uses_only_values_filed_by_then():
    ends = ["2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30", "2026-09-30"]
    filed = ["2025-11-05", "2026-02-10", "2026-05-08", "2026-08-07", "2026-11-04"]
    vals = [-10.0, -10.0, -10.0, -10.0, 100.0]                       # 2026-09-30 の四半期で TTM が黒字に転じる（申告は11-04）
    oi = [_q(e, v, f) for e, v, f in zip(ends, vals, filed)]
    fields = {"operating_income": {"entries": oi}}
    assert PF.operating_state(fields, "2026-10-31") == ("loss", "ttm")          # 11-04 の申告の前（TTMは2026-06-30まで、−40）
    assert PF.operating_state(fields, "2026-11-30") == ("profit", "ttm")        # 申告の後（TTMは2026-09-30、+70）
    assert PF.operating_state({"operating_income": {"entries": oi[:2]}}, "2026-11-30") == ("unknown", "none")   # 4四半期がそろわない・年次も無い


def test_operating_state_falls_back_to_the_latest_annual_sign():
    """TTMが作れない（四半期が4つそろわない）銘柄は、取れる最新の年次の営業利益の符号を使う"""
    ann = {"end": "2025-12-31", "start": "2025-01-01", "val": -50.0, "first_val": -50.0, "filed": "2026-02-20",
           "first_filed": "2026-02-20", "period_days": 365, "is_annual": True, "is_ytd": False}
    assert PF.operating_state({"operating_income": {"entries": [ann]}}, "2026-06-30") == ("loss", "annual")
    assert PF.operating_state({"operating_income": {"entries": [ann]}}, "2026-01-31") == ("unknown", "none")   # 申告の前


def test_gross_margin_is_derived_from_cost_of_revenue():
    """粗利が無ければ 売上−売上原価。STONKS の5銘柄（CRWV・IONQ・JOBY・LYFT・SOUN）の粗利率ずれの原因"""
    def ann(end, val, filed):
        return {"end": end, "val": val, "first_val": val, "filed": filed, "first_filed": filed, "period_days": 365, "is_annual": True,
                "is_ytd": False}
    rev = [ann(f"{y}-12-31", v, f"{y + 1}-02-20") for y, v in ((2022, 100.0), (2023, 150.0), (2024, 200.0), (2025, 300.0))]
    cogs = [ann("2025-12-31", 180.0, "2026-02-20")]                    # 粗利率 40%
    store = {"fields": {"revenue": {"entries": rev}, "cost_of_revenue": {"entries": cogs},
                        "net_income": {"entries": [ann("2025-12-31", -20.0, "2026-02-20")]}}}
    r = PF.compute("ZZZ", "2026-06-30", store)
    assert "粗利率 40%" in r["deficit_quality"]["reason"]
    # 粗利がある年は粗利を使う
    store["fields"]["gross_profit"] = {"entries": [ann("2025-12-31", 90.0, "2026-02-20")]}
    r2 = PF.compute("ZZZ", "2026-06-30", store)
    assert "粗利率 30%" in r2["deficit_quality"]["reason"]


def test_structural_deficit_kind():
    assert PF.structural_deficit_kind({"revenue_stage": "profit"}) == "capex_profit"     # AMZN・CEG型
    assert PF.structural_deficit_kind({"revenue_stage": "loss"}) == "cash_loss"
    assert PF.structural_deficit_kind(None) == "cash_loss"
    pipe = _read("src/value/tanuki_valuation/pipeline.py")
    assert "FCFが負（設備投資）" in pipe and "structural_deficit_kind" in pipe
    assert "FCFが負(設備投資)" in _read("docs/value-monitor/tanuki_score/index.html")
