"""[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段3（2026-11-03）。

- FCFの外れ値の注記・FCF内訳は、計算に使った期（TTMの点なら "TTM 2026-09-30"）と、その期の値で書く
  （以前は年次の年 "FY2025" と年次の内訳を書き、使った値はTTMだった。TSLA）
- 外れ値を除外しない理由の文言を3つに分ける（一過性費用なし／金額としきい値に届かず／上方乖離のため除外しない）。
  一過性費用はFCFと同じ期（TTMの4四半期）のEPSの調整から探す
- 調整後EPSの公正価値の損益: 年度のどれかの四半期で調整した項目は、その年度の全四半期で調整する（案B）
"""
import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "src", "value", "tanuki_valuation"))

from calculator.adjustments import analyze_fcf_outlier, compose_fcf_bottom_up  # noqa: E402
from data_fetcher import fcf_period_label  # noqa: E402
from src.value.adjusted_eps_analyzer.fair_value_detector import apply_fair_value_detection  # noqa: E402


def test_period_label():
    assert fcf_period_label("2026-09-30") == "TTM 2026-09-30"
    assert fcf_period_label(2025) == "FY2025"
    assert fcf_period_label(None) is None


def _eps_dir(tmp_path, quarters):
    d = tmp_path / "eps" / "TEST"
    d.mkdir(parents=True)
    (d / "quarterly.json").write_text(json.dumps({"quarters": quarters}), encoding="utf-8")
    (d / "annual.json").write_text(json.dumps({"years": [{"year": "2025", "adjustments": [
        {"category": "リストラ・事業再編関連", "item_name": "年次だけ", "amount": 9e9}]}]}), encoding="utf-8")
    return str(tmp_path / "eps")


def _q(end, amount):
    return {"period_end": end, "adjustments": [
        {"category": "リストラ・事業再編関連", "item_name": "リストラ費用", "amount": amount, "reason": "一過性"}]}


def _outlier(tmp_path, latest, quarters, avg=5276e6):
    return analyze_fcf_outlier("TEST", [latest, 5e9, 6e9, 5.5e9, 6.1e9], avg, cv=0.47, fiscal_year_of_latest=2025,
                               eps_data_dir=_eps_dir(tmp_path, quarters),
                               period_label="TTM 2026-09-30", transient_ttm_end="2026-09-30")


def test_outlier_note_uses_ttm_label_and_searches_ttm_quarters(tmp_path):
    # TTM 2026-09-30 の4四半期（2025-12-31〜2026-09-30）の 152M+238M。2025-09-30 は窓の外、年次の 9e9 は使わない
    r = _outlier(tmp_path, 3303e6, [_q("2025-09-30", 5e8), _q("2025-12-31", 152e6), _q("2026-09-30", 238e6)])
    assert r.period_label == "TTM 2026-09-30" and r.transient_total == 390e6 and r.action == "flagged"
    assert r.note.startswith("TTM 2026-09-30 FCF($3303M)") and "FY" not in r.note
    assert "一過性費用$390Mは乖離$1973Mの20%（$395M）に届かず除外しない" in r.note
    assert r.to_dict()["period_label"] == "TTM 2026-09-30"


def test_outlier_note_three_wordings(tmp_path):
    none = _outlier(tmp_path / "a", 3303e6, [])
    assert "一過性費用はEPSアナライザーの同じ期の調整に無い" in none.note
    up = _outlier(tmp_path / "b", 9000e6, [_q("2026-06-30", 4e9)])
    assert "上方乖離のため一過性費用では除外しない" in up.note and up.action == "flagged"
    assert "確認されず" not in none.note + up.note


def test_breakdown_uses_components_of_the_point_used():
    r = compose_fcf_bottom_up("TEST", 3.3e9, capex_list=[8.53e9], sbc_list=[2.8e9], ocf_list=[14.75e9], da_list=[5e9],
                              latest_components={"label": "TTM 2026-09-30", "ocf": 18.71e9, "capex": 15.407e9,
                                                 "sbc": 3e9, "da": 6e9},
                              period_label="TTM 2026-09-30")
    assert r.ocf == 18.71e9 and r.capex == 15.407e9 and r.period_label == "TTM 2026-09-30"
    assert "OCF$18.71B - CapEx$15.41B（TTM 2026-09-30。" in r.note and r.to_dict()["period_label"] == "TTM 2026-09-30"
    old = compose_fcf_bottom_up("TEST", 3.3e9, [8.53e9], [2.8e9], [14.75e9], [5e9], period_label="FY2025")
    assert old.ocf == 14.75e9 and "（FY2025。" in old.note


# ── 調整後EPS: 公正価値の損益を年度内でそろえる（案B） ──
TAG = "us-gaap:UnrealizedGainLossOnInvestments"   # 投資公正価値変動損益（negate_value: 利得は差し引く）


def _raw(fy, qn, v):
    return {"fiscal_year": fy, "quarter": qn, "filing_date": f"{fy}-{qn * 3:02d}-28", TAG: {"value": v, "unit": "USD"}}


def _res(fy, qn, ni, rev=25e9):
    return {"fiscal_year": fy, "quarter": qn, "filing_date": f"{fy}-{qn * 3:02d}-28", "gaap_net_income": ni,
            "gaap_eps": ni / 3.5e9, "revenue": rev, "diluted_shares_used": 3.5e9, "adjustments": [],
            "net_adjustment_total": 0.0, "adjusted_net_income": ni, "adjusted_eps": ni / 3.5e9}


def test_fair_value_item_is_aligned_within_fiscal_year():
    # 2026: Q2 の +1,015M は純利益の49%で調整、Q3 の −142M（18%）と Q1 の −10M は今までは調整されなかった
    raw = [_raw(2026, 1, -10e6), _raw(2026, 2, 1015e6), _raw(2026, 3, -142e6), _raw(2025, 3, 3e6)]
    res = [_res(2025, 3, 1373e6), _res(2026, 1, 409e6), _res(2026, 2, 2070e6), _res(2026, 3, 787e6)]
    out = {(q["fiscal_year"], q["quarter"]): q for q in apply_fair_value_detection(raw, res)}
    for k in ((2026, 1), (2026, 2), (2026, 3)):
        ids = [a["item_id"] for a in out[k]["adjustments"]]
        assert ids == ["investment_fv_change"], (k, ids)
        assert "FAIR_VALUE_ADJUSTED" in out[k]["special_flags"]
    # Q3 の評価損 −142M は戻し入れ（利得の符号を反転）。税21%
    assert abs(out[(2026, 3)]["adjusted_net_income"] - (787e6 + 142e6 * 0.79)) < 1
    assert "年度内でそろえる" in out[(2026, 3)]["special_notes"]["fair_value_auto_detect"]["note"]
    # 調整していない年度（2025）の同じタグは今までどおり（+3M、純利益の0.2%）
    assert out[(2025, 3)]["adjustments"] == []
