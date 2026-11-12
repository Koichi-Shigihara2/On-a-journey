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
def _h(*states):
    return [[f"2026-{i + 1:02d}-28", s] for i, s in enumerate(states)]


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


def test_targets_apply_persistence_pins_exclusions_and_status(tmp_path):
    prof, csvp = str(tmp_path / "p.json"), str(tmp_path / "c.csv")
    _prof(prof, {"AAA": ("loss",) * 4, "BBB": ("loss", "loss", "loss", "profit"), "CCC": ("profit",) * 4,
                 "DDD": ("loss",) * 4, "EEE": ("loss",) * 4, "FFF": ("loss",) * 4})
    _csv(csvp, {"AAA": "active", "BBB": "active", "CCC": "active", "DDD": "retired", "EEE": "provisioning",
                "FFF": "active", "PIN": "active", "EXC": "active"})
    cfg = {"excluded": {"FFF": "金融"}, "pinned": {"PIN": "TTM取得不可"}}
    assert ST.targets(prof, cfg, csvp) == ["AAA", "BBB", "PIN"]
    assert ST.targets(prof, cfg, csvp, include_provisioning=True) == ["AAA", "BBB", "EEE", "PIN"]
    assert ST.explain("BBB", prof, cfg)["target"] is True
    assert ST.explain("FFF", prof, cfg)["reason"].startswith("excluded")


def test_missing_history_stops_instead_of_an_empty_target_list(tmp_path):
    """月末の履歴が1銘柄も無いときは空の対象で走らせず、止める（公開の順を間違えた場合に STONKS の結果を空にしない）"""
    prof, csvp = str(tmp_path / "p.json"), str(tmp_path / "c.csv")
    with open(prof, "w", encoding="utf-8") as f:
        json.dump({"tickers": {"AAA": {"revenue_stage": "loss"}}}, f)
    _csv(csvp, {"AAA": "active"})
    with pytest.raises(RuntimeError):
        ST.targets(prof, {"excluded": {}, "pinned": {}}, csvp)
    with pytest.raises(RuntimeError):
        ST.targets(str(tmp_path / "none.json"), {"excluded": {}, "pinned": {}}, csvp)


def test_flag_column_is_gone_and_readers_use_the_derived_list():
    head = _read("config/cik_lookup.csv").splitlines()[0]
    assert "stonks_silo" not in head.split(",")
    tk = _read("common/sec_data/tickers.py")
    assert "stonks_target.targets" in tk
    from common.sec_data import tickers as T
    assert T.get_stonks_silo_tickers() == T.get_active_tickers("stonks_silo") == ST.targets()
    assert not os.path.exists(os.path.join(REPO, "common", "sec_data", "stonks_flag_rule.py"))
    assert not os.path.exists(os.path.join(REPO, "config", "stonks_flag_rule.json"))
    cfg = ST.load_config()
    assert "SOFI" in cfg["excluded"] and "ASTS" in cfg["pinned"]


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
    assert PF.operating_state(fields, "2026-10-31") == "loss"          # 11-04 の申告の前（TTMは2026-06-30まで、−40）
    assert PF.operating_state(fields, "2026-11-30") == "profit"        # 申告の後（TTMは2026-09-30、+70）
    assert PF.operating_state({"operating_income": {"entries": oi[:2]}}, "2026-11-30") == "unknown"   # 4四半期がそろわない


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
