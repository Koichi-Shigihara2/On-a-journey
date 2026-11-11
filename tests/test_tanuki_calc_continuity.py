"""[[TANUKI-CALC-CONTINUITY-1]]（2026-11-12）: TANUKIの計算の境目で値が飛ばないこと

物差しは tests/test_growth_sanity_continuity.py と同じ（入力を少しずつ動かし、1刻みあたりの出力の動きが入力の動きの LIPSCHITZ_MAX 倍以下）。
  B1  fcf_base の CV 0.5: 5年平均 ↔ 直近2年平均（傾斜 CV 0.40〜0.60）
  B1' 外れ値の閾値: mature 20% ↔ growth 60%（同じ重み）
  B8  taper: s=0 と s=0+ でBULLが飛ばない（終点＝推奨成長率）
  B9  逆算Q4が負なら欠け: 対象の8項目（粗利を除く）と、粗利は負のまま残ること
テストは作った値だけを使う
"""
import os
import sys

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "src", "value", "tanuki_valuation"))
sys.path.insert(0, os.path.dirname(__file__))

from calculator import adjustments as ADJ  # noqa: E402
from calculator.scenarios import create_scenario_calc_func  # noqa: E402
from common.sec_data import q4_implied  # noqa: E402
from _tanuki_pipeline_stub import load_growth_sanity_with_stub_xlrd  # noqa: E402

_gs = load_growth_sanity_with_stub_xlrd()

# 傾斜の幅 0.20（CV）で base が最大 |5年平均−2年平均| 動くので、相対で 1/0.20×1.5（smoothstep の最大傾き）≈ 7.5。余裕を見て10
LIPSCHITZ_MAX = 10.0


def _frange(a, b, n):
    return [a + (b - a) * i / n for i in range(n + 1)]


def _list(k):
    """5年平均を100に固定し、偏りを k 倍に広げる（CV ∝ k）。新しい順。特殊ケース（赤字・15%未満・CAGR−5%）に入らない形"""
    dev = [0.6, 0.4, -0.2, -0.4, -0.4]
    return [100.0 * (1.0 + k * d) for d in dev]


def _base(k):
    f = _list(k)
    return ADJ.determine_fcf_base(sum(f) / 5, sum(f[:2]) / 2, f)


def test_b1_fcf_base_is_continuous_across_cv_half():
    ks = _frange(0.5, 1.6, 2000)
    rs = [_base(k) for k in ks]
    assert all(r.method in ("avg_5yr", "recent_2yr") and r.cv < 999 for r in rs)
    cvs = [r.cv for r in rs]
    assert cvs[0] < 0.40 and cvs[-1] > 0.60          # 傾斜の全体をまたぐ
    ys = [r.base_fcf / 100.0 for r in rs]
    worst = max(abs(ys[i] - ys[i - 1]) / (cvs[i] - cvs[i - 1]) for i in range(1, len(ks)))
    # 5年平均と2年平均の差（相対）÷ 傾斜の幅 ×1.5 が理論上の上限
    span = abs(rs[-1].fcf_2yr_avg - rs[-1].fcf_5yr_avg) / 100.0
    assert worst <= LIPSCHITZ_MAX * max(span, 1e-9) / 0.20 / 1.5 * 1.5
    # 端: CV≤0.40 は5年平均、CV≥0.60 は2年平均、CV=0.50 はちょうど真ん中
    assert abs(_base(0.5).base_fcf - _base(0.5).fcf_5yr_avg) < 1e-9
    assert abs(ADJ.cv_weight(0.50) - 0.5) < 1e-12 and ADJ.cv_weight(0.40) == 0.0 and ADJ.cv_weight(0.60) == 1.0


def test_b1_old_binary_rule_would_have_jumped():
    """今までの規則（CV≤0.5 で5年平均、超で2年平均）は 0.5 をまたぐと base が一気に動いた。傾斜では最大の1刻みがその数十分の1"""
    ks = _frange(0.5, 1.6, 2000)
    rs = [_base(k) for k in ks]
    jump = abs(rs[0].fcf_2yr_avg - rs[0].fcf_5yr_avg)
    worst_step = max(abs(rs[i].base_fcf - rs[i - 1].base_fcf) for i in range(1, len(rs)))
    assert worst_step < jump * 0.02


def test_b1_prime_outlier_threshold_is_continuous():
    thr = []
    cvs = _frange(0.30, 0.70, 400)
    for cv in cvs:
        r = ADJ.analyze_fcf_outlier("T", [500.0, 100.0, 100.0, 100.0, 100.0], 100.0, cv, 2026, cv_threshold=0.5)
        assert r.detected
        thr.append(r.threshold_pct)
    worst = max(abs(thr[i] - thr[i - 1]) / (cvs[i] - cvs[i - 1]) for i in range(1, len(cvs)))
    assert worst <= LIPSCHITZ_MAX * (0.60 - 0.20) / 0.20 / 1.5 * 1.5 / 1.0
    assert abs(thr[0] - 0.20) < 1e-12 and abs(thr[-1] - 0.60) < 1e-12


def _bull(s, rec=0.12, ind=0.04):
    end = _gs.tapering_g_end(rec, s, ind)
    f = create_scenario_calc_func(base_fcf=100.0, wacc=0.09, high_growth_years=5, diluted_shares=1, rpo_pv=0.0, alpha=1.0,
                                  tapering_g_end=end)
    return f(rec * 1.4)


def test_b8_bull_does_not_jump_between_s0_and_s0plus():
    xs = _frange(0.0, 0.2, 200)
    ys = [_bull(s) for s in xs]
    worst = max(abs(ys[i] - ys[i - 1]) / ys[0] / (xs[i] - xs[i - 1]) for i in range(1, len(xs)))
    assert worst <= LIPSCHITZ_MAX
    # 今までの規則: s=0 は逓減なし（tapering_g_end=None）で BULL が s=0+ より大きく飛んだ
    f0 = create_scenario_calc_func(base_fcf=100.0, wacc=0.09, high_growth_years=5, diluted_shares=1, rpo_pv=0.0, alpha=1.0,
                                   tapering_g_end=None)
    assert abs(f0(0.12 * 1.4) - ys[1]) / ys[0] > 0.02


def test_b8_pipeline_passes_end_at_s0():
    src = open(os.path.join(_ROOT, "src", "value", "tanuki_valuation", "pipeline.py"), encoding="utf-8").read()
    assert 'if _growth_sanity and _recommended_g is not None:\n            _tapering_g_end' in src.replace("\r\n", "\n")
    assert 'growth_blend_s") or 0.0) > 0.0' not in src


def _e(start, end, val, annual=False):
    return {"start": start, "end": end, "val": val, "is_annual": annual, "accn": "A", "filed": "2026-02-20"}


EIGHT = ("buyback", "stock_based_compensation", "research_and_development", "selling_and_marketing",
         "selling_general_and_administrative", "revenue", "cost_of_revenue", "depreciation_and_amortization")


def test_b9_eight_items_negative_q4_missing_and_gross_profit_kept():
    qs = [_e("2025-01-01", "2025-03-31", 100.0), _e("2025-04-01", "2025-06-30", 100.0), _e("2025-07-01", "2025-09-30", 100.0)]
    for f in EIGHT:
        marks = []
        assert q4_implied.build_q4_implied_entries([_e("2025-01-01", "2025-12-31", 299.0, True)], qs, f, marks_out=marks) == [], f
        assert marks and marks[0]["implied_val"] == -1.0, f
        ok = q4_implied.build_q4_implied_entries([_e("2025-01-01", "2025-12-31", 401.0, True)], qs, f)
        assert ok and ok[0]["val"] == 101.0, f
    # 粗利は実際に負になる（KULR 2025年のQ4）ので負のQ4も作る
    g = q4_implied.build_q4_implied_entries([_e("2025-01-01", "2025-12-31", 299.0, True)], qs, "gross_profit")
    assert g and g[0]["val"] == -1.0
    assert "GrossProfit" not in q4_implied.non_negative_fields()
