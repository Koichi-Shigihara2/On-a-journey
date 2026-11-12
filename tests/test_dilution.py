"""株数の変化（希薄化率）の部品 common/dilution.py（[[DILUTION-DEFINITION-UNIFY-1]]）。

- 定義: 直近の四半期の加重平均の希薄化後株数 ÷ 約3年前（1,096日前±50日）の四半期の年率。分割は split_history.yaml の比で調整
- Layer3の四半期にはQ4が無い（Q1〜Q3だけ）ので、点の個数ではなく日付で3年前を探す
- 連続性: 3年の境目（3年前の点が無ければ欠け・あれば値・窓が進んでも飛ばない）。年次に落ちない。
  減点の傾斜は20%・40%の境目で連続（段の分岐は無い）
- 未登録の分割は割り戻さず印だけ
"""
import os
import sys
from datetime import date

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from common import dilution as dl  # noqa: E402


def _ends(n, start=(2022, 6, 30)):
    """四半期末の日付を n 個（約91日おき）"""
    y, m, d = start
    out = []
    for _ in range(n):
        out.append(date(y, m, d).isoformat())
        m += 3
        if m > 12:
            m -= 12
            y += 1
        d = {3: 31, 6: 30, 9: 30, 12: 31}[m]
    return out


def _series(n, growth_per_q=0.01, base=100e6):
    ends = _ends(n)
    return [(e, base * (1 + growth_per_q) ** i) for i, e in enumerate(ends)]


# ── 定義 ────────────────────────────────────────────
def test_cagr_is_latest_over_12_quarters_back():
    r = dl.share_count_change(_series(13, 0.02))
    assert r["cagr_pct"] == pytest.approx(((1.02 ** 12) ** (1 / 3) - 1) * 100, rel=2e-3)      # 12四半期＝3年
    assert r["shares_start"] == pytest.approx(100e6) and r["n"] == 13
    assert r["split_suspect"] == []


def test_registered_split_is_adjusted_with_split_history_ratio():
    """1-for-8（ratio 0.125）の分割の前の値は split_history の比で割り戻す（株数が減ったことにしない）"""
    ends = _ends(13)
    vals = [800e6 + i * 8e6 for i in range(5)] + [100e6 + i * 1e6 for i in range(5, 13)]   # 分割の前後
    pts = list(zip(ends, vals))
    split_date = ends[5]                                                                  # 6点目から分割後
    plain = dl.share_count_change(pts)
    adj = dl.share_count_change(pts, [{"date": split_date, "ratio": 0.125}])
    assert plain["cagr_pct"] < 0 and plain["split_suspect"] == [ends[5]]                 # 未登録＝割り戻さず印
    assert adj["split_suspect"] == [] and adj["cagr_pct"] == pytest.approx(((112e6 / 100e6) ** (1 / 3) - 1) * 100, rel=2e-3)


def test_unregistered_split_is_marked_not_adjusted():
    ends = _ends(13)
    pts = [(e, (400e6 if i < 6 else 50e6)) for i, e in enumerate(ends)]
    r = dl.share_count_change(pts)
    assert r["split_suspect"] == [ends[6]]
    assert r["shares_start"] == 400e6                                                     # 割り戻していない


# ── 12四半期の境目 ───────────────────────────────────
def test_twelve_points_is_missing_not_annual_fallback():
    assert dl.share_count_change(_series(12)) is None
    assert dl.share_count_change(_series(13)) is not None


def test_continuity_when_the_window_moves_by_one_quarter():
    """13点→14点→15点で窓が1四半期ずつ進んでも、年率は直前の値から急に飛ばない（一定の伸びなら同じ値）"""
    vals = [dl.share_count_change(_series(n, 0.015))["cagr_pct"] for n in (13, 14, 15, 16)]
    assert max(vals) - min(vals) < 0.05     # 一定の伸びなら窓が進んでも同じ（日数の1〜2日の差だけ）
    # 伸びが加速している系列でも、1四半期進むごとの変化は小さい（ゼロ円の飛びが無い）
    acc = [(e, 100e6 * (1 + 0.002 * i * i / 4)) for i, e in enumerate(_ends(16))]
    seq = [dl.share_count_change(acc[:n])["cagr_pct"] for n in (13, 14, 15, 16)]
    assert all(abs(b - a) < 3.0 for a, b in zip(seq, seq[1:]))


def test_quarters_without_q4_use_the_point_three_years_back():
    """Layer3の四半期の株数はQ1〜Q3だけ（Q4が無い）。12個前の点は約4年前なので使わず、日付で3年前の同じ四半期を探す"""
    pts = []
    for y in range(2022, 2027):
        for m, d in ((3, 31), (6, 30), (9, 30)):
            if (y, m) <= (2026, 9):
                pts.append((date(y, m, d).isoformat(), 100e6 * 1.1 ** (y - 2022 + m / 12)))
    r = dl.share_count_change(pts)
    assert r["end"] == "2026-09-30" and r["start_end"] == "2023-09-30"
    assert r["cagr_pct"] == pytest.approx(10.0, abs=0.05)           # 年率10%の伸び＝3年の年率も10%（4年で割らない）


def test_window_out_of_range_is_missing():
    """欠けた四半期があって13点が3年に収まらないときは欠け（年次に落とさない）"""
    ends = _ends(13)
    ends[5:] = [date.fromisoformat(e).replace(year=date.fromisoformat(e).year + 2).isoformat() for e in ends[5:]]
    assert dl.share_count_change([(e, 100e6) for e in ends]) is None


def test_outstanding_gap_is_a_display_mark_only():
    r = dl.share_count_change(_series(13, 0.0), outstanding=("2026-06-30", 112e6))
    assert r["outstanding_gap_pct"] == pytest.approx(12.0) and r["cagr_pct"] == pytest.approx(0.0)


# ── 上場の前後（ALAB・CART・RBRK・LOAR型）────────────────
def _ipo_series():
    """上場（最初の定期報告の期末 2024-03-31）の前は少数の普通株、上場後は転換の後の株数。Q4が無い四半期の並び"""
    pts = [("2023-03-31", 35e6), ("2023-06-30", 36e6), ("2023-09-30", 37e6), ("2024-03-31", 52e6)]     # 上場の前（〜最初の報告の期）
    post = [("2024-06-30", 155e6), ("2024-09-30", 160e6), ("2025-03-31", 165e6), ("2025-06-30", 168e6),
            ("2025-09-30", 170e6), ("2026-03-31", 175e6), ("2026-06-30", 178e6), ("2026-09-30", 180e6)]
    return pts + post


def test_three_years_back_before_listing_uses_first_quarter_after_listing():
    r = dl.share_count_change(_ipo_series(), first_report_period_end="2024-03-31")
    assert r["basis"] == "since_listing" and r["start_end"] == "2024-06-30" and r["split_suspect"] == []   # 上場の前後の跳びは窓に入らない
    years = (date(2026, 9, 30) - date(2024, 6, 30)).days / 365.25
    assert r["cagr_pct"] == pytest.approx(((180 / 155) ** (1 / years) - 1) * 100, rel=1e-6)           # 年率は実際の日数


def test_without_listing_info_the_basis_is_not_changed():
    r = dl.share_count_change(_ipo_series())                      # 申告の情報が無い→基準は変えない
    assert r is None or r["basis"] == "3y"


def test_fewer_than_four_quarters_after_listing_is_missing():
    pts = [("2024-03-31", 52e6), ("2024-09-30", 155e6), ("2024-12-31", 160e6), ("2025-03-31", 165e6)]    # 上場後の最初の四半期から182日（3四半期）
    assert dl.share_count_change(pts, first_report_period_end="2024-03-31") is None
    pts.append(("2025-09-30", 168e6))                                                                      # 365日（4四半期以上）になると値が出る
    assert dl.share_count_change(pts, first_report_period_end="2024-03-31")["basis"] == "since_listing"


def test_old_listing_with_missing_three_year_point_stays_missing():
    """上場が3年より前の銘柄で3年前の点が無いのは単なる欠け。古い基準で年率を出さない"""
    pts = [("2025-03-31", 100e6), ("2025-06-30", 101e6), ("2025-09-30", 102e6), ("2026-03-31", 103e6),
           ("2026-06-30", 104e6), ("2026-09-30", 105e6)]
    assert dl.share_count_change(pts, first_report_period_end="2015-03-31") is None


def test_listing_before_the_window_start_is_normal_three_years():
    """3年前の点が上場の後ならふつうの3年（上場の規則は使わない）"""
    r = dl.share_count_change(_series(13, 0.01), first_report_period_end="2022-03-31")
    assert r["basis"] == "3y"


def test_unit_error_stays_marked_and_inside_window():
    """CIX型（単位の誤り）: 上場の規則では救わず、印（split_suspect）のまま"""
    ends = _ends(13)
    pts = [(e, 12.3e6) for e in ends[:-1]] + [(ends[-1], 12337.0)]
    r = dl.share_count_change(pts, first_report_period_end="2015-03-31")
    assert r["split_suspect"] == [ends[-1]]


def test_first_report_end_reads_the_earliest_filing(tmp_path):
    import json
    d = tmp_path / "ZZZ"
    d.mkdir()
    (d / "submissions.json").write_text(json.dumps({
        "accn_to_filingdate": {"a-2": "2024-08-01", "a-1": "2024-05-08", "a-3": "2024-11-01"},
        "accn_to_reportdate": {"a-2": "2024-06-30", "a-1": "2024-03-31", "a-3": "2024-09-30"}}), encoding="utf-8")
    assert dl.first_report_end("ZZZ", str(tmp_path)) == "2024-03-31"
    assert dl.first_report_end("NOPE", str(tmp_path)) is None


# ── 減点（傾斜だけ）────────────────────────────────
def test_penalty_is_slope_only_and_continuous_at_the_boundaries():
    f = dl.dilution_penalty
    eps = 1e-6
    for b in (20.0, 40.0):
        assert abs(f(b + eps) - f(b - eps)) < 1e-3
    assert f(None) == 0 and f(-5) == 0 and f(20) == 0 and f(40) == 25 and f(30) == pytest.approx(12.5) and f(60) == 25 and f(10) == 0
    xs = [i / 2 for i in range(0, 120)]
    assert all(f(b) >= f(a) for a, b in zip(xs, xs[1:]))                        # 単調増加


def test_no_step_branch_remains():
    assert not hasattr(dl, "PENALTY_MODE") and not hasattr(dl, "STEP_LOW")
    with pytest.raises(TypeError):
        dl.dilution_penalty(30, "step")
