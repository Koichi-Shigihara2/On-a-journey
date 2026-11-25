"""黒字化の予測から現金を伴わない評価損益・減損を除く（[[STONKS-DEFICIT-REALITY-EPIC-1]] ②、common/profitability.py）。

作った値（Layer3 の形の store）だけを使う。本物のファイル・外部の取得に触れない（設定は config/noncash_operating_items.json）。
"""
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import profitability as pf
from tests.test_profitability import _entries, _qends, _store  # type: ignore[import]


def _with_field(store, name, values, n=None):
    ends = _qends(n or len(values))
    store["fields"][name] = {"entries": _entries(values, ends)}
    return store


CFG = {"items": [{"field": "contingent_consideration_change", "kind": "fair_value_change", "sign": 1},
                 {"field": "goodwill_impairment", "kind": "impairment", "sign": 1}],
       "min_share_applied": 0.02}


def _soun_like():
    """営業利益が評価益（負の値＝営業利益を押し上げる）で振れ、本業は赤字がほぼ一定の銘柄"""
    rev = [100] * 12
    gain = [0, 0, 0, 0, 0, 0, 0, -20, -40, -60, -80, -95]             # 評価益（負）
    core = [-100] * 12                                               # 本業の営業利益（赤字で一定）
    oi = [c - g for c, g in zip(core, gain)]                          # 報告された営業利益 = 本業 − 評価損益（評価益で押し上がる）
    st = _store(rev, oi)
    _with_field(st, "contingent_consideration_change", gain)
    return st


def test_noncash_gains_are_added_back_and_prediction_changes():
    st = _soun_like()
    base = pf.compute("A", "2026-12-31", st, noncash_cfg={"items": []})        # 足し戻さない（今まで）
    adj = pf.compute("A", "2026-12-31", st, noncash_cfg=CFG)
    assert base["breakeven"]["reason"] == "PREDICTED"                          # 評価益で営業利益率が急に上向き＝黒字化目前に見える
    assert adj["breakeven"]["reason"] != "PREDICTED"                           # 評価益を除くと本業は赤字のまま
    assert adj["noncash_adjustment"]["status"] == pf.NONCASH_APPLIED
    assert adj["noncash_adjustment"]["by_kind"]["fair_value_change"] < 0


def test_stage_uses_reported_operating_income_sign():
    """収益の段階（黒字／赤字）は報告された営業利益のまま。足し戻しで符号が変わっても段階は変えない"""
    rev = [100] * 12
    oi = [5] * 12                                                           # 報告は黒字
    st = _store(rev, oi)
    _with_field(st, "goodwill_impairment", [-50] * 12)                      # 大きな評価益を足し戻すと調整後は赤字
    r = pf.compute("A", "2026-12-31", st, noncash_cfg=CFG)
    assert r["revenue_stage"] == "profit" and r["ttm_operating_income"] == 20
    assert r["ttm_operating_income_adjusted"] < 0                           # 調整後の値は別に残る


def test_status_none_when_no_item_and_candidate_from_facts():
    st = _store([100] * 12, [-10] * 12)
    none = pf.compute("A", "2026-12-31", st, noncash_cfg=CFG)
    assert none["noncash_adjustment"]["status"] == pf.NONCASH_NONE
    facts = {"facts": {"us-gaap": {"FairValueAdjustmentOfWarrants": {"units": {"USD": [
        {"start": "2026-01-01", "end": "2026-03-31", "val": -30_000_000, "form": "10-Q", "filed": "2026-05-01"},
        {"start": "2026-04-01", "end": "2026-06-30", "val": -30_000_000, "form": "10-Q", "filed": "2026-08-01"},
        {"start": "2025-10-01", "end": "2025-12-31", "val": -30_000_000, "form": "10-K", "filed": "2026-03-01"},
        {"start": "2025-07-01", "end": "2025-09-30", "val": -30_000_000, "form": "10-Q", "filed": "2025-11-01"}]}}}}}
    cfg = dict(CFG, candidate={"name_pattern": "FairValueAdjust", "min_share_of_ttm_operating_income": 0.1,
                               "min_share_of_ttm_revenue": 0.02, "min_abs_usd": 5_000_000})
    c = pf.compute("A", "2026-12-31", st, noncash_cfg=cfg, company_facts=facts)
    assert c["noncash_adjustment"]["status"] == pf.NONCASH_CANDIDATE
    assert c["noncash_adjustment"]["candidates"][0]["tag"] == "FairValueAdjustmentOfWarrants"
    assert c["breakeven"] == none["breakeven"]                              # 候補は自動では除かない（予測は変わらない）


def test_small_amount_is_none_not_applied():
    st = _store([100] * 12, [-100] * 12)
    _with_field(st, "contingent_consideration_change", [0.1] * 12)          # TTM営業利益の0.1%（2%未満）
    r = pf.compute("A", "2026-12-31", st, noncash_cfg=CFG)
    assert r["noncash_adjustment"]["status"] == pf.NONCASH_NONE


def test_uses_first_val_not_restated_value():
    """Layer3 の first_val（その時点で最初に申告された値）の四半期を使う"""
    st = _store([100] * 12, [-10] * 12)
    ends = _qends(12)
    ent = _entries([-9999] * 12, ends, first=[-30] * 12)                    # val は後の再申告、first_val が最初の値
    st["fields"]["contingent_consideration_change"] = {"entries": ent}
    r = pf.compute("A", "2026-12-31", st, noncash_cfg=CFG)
    assert r["noncash_adjustment"]["ttm_addback"] == -120.0                 # 4四半期 × −30


def test_config_fields_exist_in_layer3_and_are_excluded_from_ttm():
    from common.sec_data.layer3_builder import load_concept_definitions
    from common.sec_data.ttm_calculator import EXCLUDED_FIELDS
    fields = load_concept_definitions()["fields"]
    cfg = pf.load_noncash_config()
    assert cfg["items"], "許可リストが空"
    for it in cfg["items"]:
        assert it["field"] in fields and fields[it["field"]]["candidates"], it["field"]
        assert it["field"] in EXCLUDED_FIELDS                               # TTM出力には出さない
        assert it.get("kind") and it.get("label")
