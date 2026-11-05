"""HypeCore の stage の定義v2（確定）の並行計算（common/stage_v2.py）。[[STAGE-V2-ROLLOUT-1]] ①（2026-11-06）
テストは作った値だけを使う（本物のファイル・外部の取得に触れない）
"""
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
from common import stage_v2 as sv


def _b(gF, gM, L, mature=False, close=100.0):
    return {"gF": gF, "gM": gM, "L": L, "Delta": gM - gF, "mature": mature, "close": close}


def test_main_stages():
    assert sv.judge(_b(0.10, 0.60, 80), []) == "S3"                       # Δ大・L高
    assert sv.judge(_b(0.30, 0.25, 50), []) == "S2"                       # |Δ|≤0.15・L中位以上
    assert sv.judge(_b(0.30, -0.20, 50), []) == "消化"                    # 修正a: Δ<−0.15・L中位
    assert sv.judge(_b(0.00, 0.00, 20), []) == "S0"                       # h4: 値幅は条件にしない
    assert sv.judge(_b(0.08, 0.00, 50), []) == "安定"                     # h5
    assert sv.judge(_b(-0.20, -0.20, 50), []) == "該当なし"
    assert sv.judge({"undet": "no_multiples"}, []) == "判定不能"


def test_h1_awakening_continues_and_h2_catchup_after_euphoria():
    # g_Mが6か月ずっとプラスでも（「転じる」でなくても）、L低・g_Fプラス・g_Mプラスは S1
    hist = [{"raw": "S1", "gF": 0.2, "gM": 0.3, "L": 20, "close": 90}] * 6
    assert sv.judge(_b(0.20, 0.35, 20), hist) == "S1"
    # 直前6か月にS3、g_F・g_Mプラス、L高、Δ<−0.15 → 消化
    hist3 = [{"raw": "S3", "gF": 0.1, "gM": 0.8, "L": 90, "close": 120}] * 6
    assert sv.judge(_b(0.60, 0.20, 80), hist3) == "消化"


def test_mature_company_needs_bigger_delta_for_s3():
    assert sv.judge(_b(0.05, 0.40, 80), []) == "S3"
    assert sv.judge(_b(0.05, 0.40, 80, mature=True), []) != "S3"          # 閾値は0.30×1.5


def test_multiple_type_by_revenue_stage_cyclical_financial():
    assert sv.multiple_type({}, "profit") == "per"
    assert sv.multiple_type({}, "near_profit") == "ev_gross_profit"
    assert sv.multiple_type({}, "loss") == "ev_sales"
    assert sv.multiple_type({"cyclical": True}, "loss") == "pe_5y_avg_earnings"
    assert sv.multiple_type({"financial": {"value": True}}, "unclassified") == "p_tbv"


def test_persistence_two_months():
    seq = ["S2", "S3", "S2", "S3", "S3", "移行中", "判定不能", "S4"]
    assert sv.persist(seq) == ["S2", "S2", "S2", "S2", "S3", "S3", "S3", "S3"]


def test_compute_all_type_switch_needs_two_months_and_output_from_2022():
    """収益の段階が1か月だけ変わっても倍率の種類は変えない。出力は2022-01から"""
    months = [f"{y}-{m:02d}" for y in (2021, 2022) for m in range(1, 13)]
    tickers = [f"T{i}" for i in range(6)]
    master = {t: {"sector": "Tech", "cyclical": False, "financial": {"value": False}, "unclassified": None} for t in tickers}
    mult = {t: {m: {"month": m, "date": f"{m}-28", "per": 20.0 + i + k * 0.1, "ev_sales": 5.0 + i + k * 0.05,
                    "ev_gross_profit": 8.0 + i} for k, m in enumerate(months)} for i, t in enumerate(tickers)}
    stages = {}

    def fake_fin(store, me, t, d):
        stg = "loss" if (t == "T0" and d.startswith("2022-03")) else "profit"
        stages.setdefault(t, []).append(stg)
        return {"prof": {"revenue_stage": stg}, "rev": 110.0, "rev_p": 100.0, "oi": 10.0, "oi_p": 9.0,
                "ocf": 12.0, "ocf_p": 11.0, "capex": 1.0, "bb": 0.0, "div": 0.0}
    orig = sv.financials
    sv.financials = fake_fin
    try:
        rows = sv.compute_all(tickers, master, mult, {t: {} for t in tickers}, {t: [{"date": "2018-01-02", "close": 1.0}] for t in tickers})
    finally:
        sv.financials = orig
    r = {x["month"]: x for x in rows["T0"]}
    assert r["2022-03"]["multiple_type"] == "per"                          # 1か月だけの赤字では種類を変えない
    out = sv.finalize("T0", rows["T0"], poc_dir=os.path.join(REPO, "tests", "_no_such_dir"))
    assert out["months"][0]["month"] == "2022-01" and out["months"][0]["current_definition_stage"] is None
