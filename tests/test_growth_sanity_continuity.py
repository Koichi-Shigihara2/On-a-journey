"""推奨成長率の連続性（[[GROWTH-SANITY-CONTINUITY-1]] 段1、2026-11-06）

第一条件: 入力が少し変わったとき、推奨成長率が少しだけ変わる（境目で値が飛ばない）。
入力を少しずつ動かし、1刻みあたりの推奨成長率の動きが「入力の刻み × LIPSCHITZ_MAX」以下であることを確かめる。
確かめる境目（今までの規則で飛んでいた所）:
  J1 候補が2件に届くか（None↔値）   J2 floor（p1が0.15±0.002の二値）   J3 候補の符号（>0のときだけ入る）
  J4 候補の数の偶数/奇数            J5 逓減モデルへの切り替え（0.50超の二値）
  J6 逓減の終点が業種>0かどうか      J7 taperの有無（growth_modelラベルの二値）
今までの規則では J3・J5 は入力の動きの20〜60倍（MRVL: RR×ROICの0.1%で5pt、p1の0.50→0.51で19pt）、J1・J2 は None↔値 だった。
テストは作った値だけを使う（Damodaranの表・外部の取得に触れない）
"""
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from _tanuki_pipeline_stub import load_growth_sanity_with_stub_xlrd  # noqa: E402

_gs = load_growth_sanity_with_stub_xlrd()

# 1刻みあたりの動きの上限（推奨成長率の動き ÷ 入力の動き）。重みの傾斜が0〜5%（WEIGHT_RAMP）なので、
# 候補の値の差が最大でも約0.5のとき傾斜は 0.5/0.05 = 10 まで。今までの規則の20〜60倍・None↔値は全部これを超える
LIPSCHITZ_MAX = 10.0


def _rec(p1=0.20, ttm=None, c3=None, c5=None, gf=None, ind=None, src=None):
    bench = {"industry": "X", "g_ebit": ind, "roc": None, "rr": None} if ind is not None else None
    with patch.object(_gs, "get_industry_benchmark", return_value=bench), \
            patch.object(_gs, "calc_revenue_cagr", return_value={"cagr_3yr": c3, "cagr_5yr": c5}):
        return _gs.check_growth_sanity(
            "T", phase1_growth=p1, sector=None, annual_revenues=[1.0], g_fundamental=gf,
            ttm_actual=ttm if ttm is not None else (p1 if p1 > 0 else None), growth_source=src,
        )


def _frange(a, b, n):
    return [a + (b - a) * i / n for i in range(n + 1)]


def _assert_continuous(xs, ys, label):
    assert all(y is not None for y in ys), f"{label}: 推奨成長率が None になる点がある（None↔値の飛び）"
    worst = max(abs(ys[i] - ys[i - 1]) / (xs[i] - xs[i - 1]) for i in range(1, len(xs)))
    assert worst <= LIPSCHITZ_MAX, f"{label}: 入力の動きに対する推奨成長率の動きの最大が {worst:.1f} 倍（上限 {LIPSCHITZ_MAX}）"


def test_j1_candidate_count_gate_no_none_to_value():
    """業種だけが候補（LOAR型）で業種の値を0→6%に動かす。以前は候補1件でNone、2件で突然値。今は連続"""
    xs = _frange(0.0, 0.06, 60)
    ys = [_rec(p1=0.50, ind=x)["recommended_g"] for x in xs]
    _assert_continuous(xs, ys, "J1 業種の値 0→6%")
    # 候補が何も無くても None にならず、p1 で埋める
    assert _rec(p1=0.20)["recommended_g"] == 0.20


def test_j2_floor_is_not_a_binary_switch():
    """fcf_cagr の floor（0.15）の付近。以前は abs(p1-0.15)<0.002 の二値で、ここで候補1件の例外が入っていた"""
    xs = _frange(0.13, 0.17, 80)
    ys = [_rec(p1=x, ind=0.066, src="fcf_cagr")["recommended_g"] for x in xs]
    _assert_continuous(xs, ys, "J2 floor付近のp1 13→17%")


def test_j3_j4_sign_gate_and_parity_no_jump():
    """RR×ROICを0→3%、5年CAGRを−2→+2%、業種を−3→+3%に動かす（MRVL・XOM型）。
    以前は0をまたぐ瞬間に候補が1件増減し、中央値が5pt以上飛んでいた（偶数個/奇数個の切り替わり）"""
    base = dict(p1=0.306, ttm=0.306, c3=0.114, c5=0.225)
    xs = _frange(0.0, 0.03, 60)
    _assert_continuous(xs, [_rec(**base, gf=x)["recommended_g"] for x in xs], "J3 RR×ROIC 0→3%")
    xs = _frange(-0.02, 0.02, 80)
    _assert_continuous(xs, [_rec(**{**base, "c5": x})["recommended_g"] for x in xs], "J3 5年CAGR −2→+2%")
    xs = _frange(-0.03, 0.03, 120)
    _assert_continuous(xs, [_rec(**base, gf=0.008, ind=x)["recommended_g"] for x in xs], "J3 業種の値 −3→+3%")


def test_j3_negative_cagr_counts_as_zero_not_excluded():
    """負のCAGRは0として入れる（捨てない）。5年CAGR −2% と 0% は同じ推奨成長率"""
    a = _rec(p1=0.20, c3=0.10, c5=-0.02)["recommended_g"]
    b = _rec(p1=0.20, c3=0.10, c5=0.0)["recommended_g"]
    assert a == b
    assert _rec(p1=0.20, c3=0.10, c5=0.0)["recommended_g"] < _rec(p1=0.20, c3=0.10)["recommended_g"]  # 0%の伸びは候補として効く


def test_j5_decay_switch_is_a_ramp_not_a_cliff():
    """逓減モデルへの切り替え（tmax 0.50〜0.70でなだらかに）。以前は0.50→0.51で19〜25pt飛んでいた"""
    xs = _frange(0.40, 0.80, 160)
    ys = [_rec(p1=x, ttm=x, c3=0.116, c5=0.139, ind=0.216)["recommended_g"] for x in xs]
    _assert_continuous(xs, ys, "J5 p1・今の伸び 40→80%")
    s = [_rec(p1=x, ttm=x, c3=0.116, c5=0.139, ind=0.216)["growth_blend_s"] for x in xs]
    assert s[0] == 0.0 and s[-1] == 1.0
    assert _rec(p1=0.50, ttm=0.50, c3=0.1)["growth_blend_s"] == 0.0     # 0.50ちょうどに頭打ちされた銘柄は今のまま
    assert _rec(p1=0.58, ttm=0.58, c3=0.1)["growth_model"] == "median"  # s<0.5
    assert _rec(p1=0.70, ttm=0.70, c3=0.1)["growth_model"] == "decay"   # s=1


def test_j6_decay_end_is_continuous_in_industry_sign():
    """逓減モデルの終点が業種>0かどうか（0%↔10%）。業種の値が0をまたいでも連続（負の業種は0として使う）"""
    xs = _frange(-0.03, 0.03, 120)
    ys = [_rec(p1=0.80, ttm=0.80, c3=0.30, ind=x)["recommended_g"] for x in xs]
    _assert_continuous(xs, ys, "J6 逓減モデルの業種の値 −3→+3%")
    assert abs(_rec(p1=0.80, ttm=0.80, c3=0.30, ind=-0.02)["recommended_g"]
               - _rec(p1=0.80, ttm=0.80, c3=0.30, ind=0.0)["recommended_g"]) < 1e-12


def test_j7_tapering_end_is_continuous_in_blend_and_industry():
    """taperの終点: rec − s·(rec − 業種)。s=0 で推奨成長率〈逓減なし〉、s=1 で業種、業種が0以下なら0"""
    f = _gs.tapering_g_end
    assert f(0.30, 0.0, 0.05) == 0.30
    assert abs(f(0.30, 1.0, 0.05) - 0.05) < 1e-12
    assert abs(f(0.30, 1.0, -0.02) - 0.0) < 1e-12
    xs = _frange(0.0, 1.0, 100)
    ys = [f(0.30, x, 0.05) for x in xs]
    assert max(abs(ys[i] - ys[i - 1]) / (xs[i] - xs[i - 1]) for i in range(1, len(xs))) <= 0.30 + 1e-9
    xs = _frange(-0.03, 0.03, 120)
    ys = [f(0.30, 0.6, x) for x in xs]
    assert max(abs(ys[i] - ys[i - 1]) / (xs[i] - xs[i - 1]) for i in range(1, len(xs))) <= 1.0 + 1e-9
    assert f(0.30, 0.5, None) == 0.30 - 0.5 * (0.30 - _gs.DECAY_END_DEFAULT)


def test_constants_in_one_place():
    """定数は1か所（理由はコメント）。RR×ROICと業種の重みの傾斜は同じ1つの定数（0〜5%）"""
    assert _gs.WEIGHT_RAMP == 0.05
    assert _gs._weight_ramp(0.0) == 0.0 and _gs._weight_ramp(0.025) == 0.5 and _gs._weight_ramp(0.05) == 1.0
    assert _gs.DECAY_BLEND_LO == 0.50 and _gs.DECAY_BLEND_HI == 0.70
    assert _gs.FILL_WEIGHT == 2.0


def test_floor_hit_still_uses_legacy_no_candidate_rule():
    """verdict の floor_hit は段1では変えない（旧規則で『推奨成長率が出ない』銘柄を検知）"""
    r = _rec(p1=0.15, src="fcf_cagr")          # 候補なし → 旧規則では推奨成長率なし
    assert r["floor_hit"] is True
    r2 = _rec(p1=0.15, src="fcf_cagr", c3=0.10, c5=0.08)  # 候補2件 → 旧規則でも出る
    assert r2["floor_hit"] is False
