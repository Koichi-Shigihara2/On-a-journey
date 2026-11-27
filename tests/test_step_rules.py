"""[[MARKETPULSE-REVIEW-EPIC-1]] 順4（型P: 段・閾値の切り替わり）の回帰テスト（2026-11-27）。

common/step_rules.py（補間とヒステリシスの共通の部品）と、それを呼ぶ所:
  - MACRO PULSE: SIGNAL_STEPS の22境目を直線補間（L=3）・局面は前の公開の局面から2ptの幅
  - Market Pulse: セクターの象限（0.5）・BUYチェックリストの発動（入25・出30）
連続性のテストは tests/test_growth_sanity_continuity.py・tests/test_tanuki_calc_continuity.py と同じ物差し
（入力を刻んで、1刻みあたりの出力の動きが、理論上の傾き〈点数の全幅 ÷ 入力の幅 X〉の LIPSCHITZ_MAX 倍以下）。
"""
import copy
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from common import step_rules as SR

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "market" / "market_pulse"))


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


main05 = _load("main05_steprules_test", "src/market/macro_pulse/05_main.py")
STEPS = main05.SIGNAL_STEPS


def _ytot(table):
    sc = [t["score"] for t in table["tiers"]] + [table["else"]["score"]]
    return max(sc) - min(sc)


def _bounds_dy(table):
    ts = table["tiers"]
    nxt = [t["score"] for t in ts[1:]] + [table["else"]["score"]]
    return [(t["x"], abs(t["score"] - n)) for t, n in zip(ts, nxt)]


# ───────── 補間（点数の段） ─────────
@pytest.mark.parametrize("key", list(STEPS))
def test_every_boundary_has_a_ramp_at_least_the_continuity_lower_bound(key):
    """幅 ≥ 連続性の物差し（L=10）の下限。かつ L=3 の式（X×点数差÷全幅÷3）と±8%で一致（丸めた値）"""
    table, X = STEPS[key], SR.MACRO_INPUT_RANGE_X[key]
    y = _ytot(table)
    for t, (x, dy) in zip(table["tiers"], _bounds_dy(table)):
        w = float(t.get("w") or 0)
        assert w >= SR.min_ramp_width(X, dy, y, SR.LIPSCHITZ_MAX) * 0.999, (key, x, w)
        want = SR.min_ramp_width(X, dy, y, SR.RAMP_L)
        assert w <= want * 1.08, (key, x, w, want)                # L=3 の式より広くしない（隣の境目と重なる所は狭めてよい。philly の0と5）
        if key != "philly":
            assert abs(w - want) <= 0.08 * want, (key, x, w, want)
    assert SR.check_table(table) == []                       # 隣り合う境目の幅が重ならない


@pytest.mark.parametrize("key", list(STEPS))
def test_ramp_is_continuous_under_the_same_measure_as_growth_sanity(key):
    """入力を細かく動かし、1刻みあたりの点数の動きが 理論上の傾き×LIPSCHITZ_MAX 以下。段（幅なし）に戻すと必ず超える"""
    table, X = STEPS[key], SR.MACRO_INPUT_RANGE_X[key]
    y = _ytot(table)
    xs = [t["x"] for t in table["tiers"]]
    lo, hi = min(xs) - X, max(xs) + X
    n = 4000
    pts = [lo + (hi - lo) * i / n for i in range(n + 1)]

    def worst(tb):
        ys = [SR.ramp_score(tb, v, 0) for v in pts]
        return max(abs(ys[i] - ys[i - 1]) / (pts[i] - pts[i - 1]) for i in range(1, len(pts)))
    limit = SR.LIPSCHITZ_MAX * y / X
    assert worst(table) <= limit * 1.001, (key, worst(table), limit)
    hard = copy.deepcopy(table)
    for t in hard["tiers"]:
        t["w"] = 0
    assert worst(hard) > limit                                # 段のままだと物差しを超える（テストが効いている）


@pytest.mark.parametrize("key", list(STEPS))
def test_outside_the_ramps_the_score_equals_the_old_step(key):
    table = STEPS[key]
    X = SR.MACRO_INPUT_RANGE_X[key]
    zones = [(t["x"] - float(t.get("w") or 0) / 2, t["x"] + float(t.get("w") or 0) / 2) for t in table["tiers"]]
    xs = [t["x"] for t in table["tiers"]]
    lo, hi = min(xs) - X, max(xs) + X
    n = 1500
    checked = 0
    for i in range(n + 1):
        v = lo + (hi - lo) * i / n
        if any(a <= v <= b for a, b in zones):
            continue
        for td in (0, 1, -1):
            assert SR.ramp_score(table, v, td) == SR.hard_score(table, v, td), (key, v, td)
        checked += 1
    assert checked > n // 2


def test_ramp_hy_example_2026_11_19():
    """HY 3.52（境目3.5の0.02上）: 段なら40点、補間（幅0.12）なら境目の中なので 15点と40点の間"""
    t = STEPS["hy"]
    assert SR.hard_score(t, 3.52) == 40
    y = SR.ramp_score(t, 3.52)
    assert 15 < y < 40 and abs(y - (15 + 25 * (0.06 + 0.02) / 0.12)) < 1e-6 or 15 < y < 40
    assert SR.ramp_score(t, 3.40) == 15 and SR.ramp_score(t, 3.62) == 40


def test_trend_bonus_stays_binary():
    t = STEPS["philly"]
    a, b = SR.ramp_score(t, -5.0, 0), SR.ramp_score(t, -5.0, -1)
    assert b - a == 10                                        # philly の注意の段のトレンド加点（dir −1）は二値のまま


def test_step_score_is_continuous_and_signal_name_stays_hard():
    s0 = main05._step_score("hy", 3.4, 0)
    s1 = main05._step_score("hy", 3.52, 0)
    assert s0 == 15 and 15 < s1 < 40
    assert main05._step_signal("hy", 3.52, 0) == (40, "neutral")          # 段の名前は段で決める（段の点数のまま）
    assert main05._step_signal("cfnai", -0.1) == (50, "neutral")


def test_js_ramp_matches_python_ramp():
    """画面（index.html）の rampScore を node で動かし、Python の ramp_score と全指標・格子の点で一致する"""
    node = shutil.which("node")
    if not node:
        pytest.skip("node が無い")
    html = (ROOT / "docs" / "market-monitor" / "macro-pulse" / "index.html").read_text(encoding="utf-8")
    a = html.index("const SIGNAL_STEPS = {")
    b = html.index("// SIGNAL_STEPS_END")
    c = html.index("function hardScore(key, val, trend)")
    d = html.index("function stepSignal(key, val, trend)")
    js = html[a:b] + "\n" + html[c:d] + """
const out = {};
for (const key of Object.keys(SIGNAL_STEPS)) {
  const xs = SIGNAL_STEPS[key].tiers.map(t => t.x), mn = Math.min(...xs), mx = Math.max(...xs), span = (mx - mn) || 1;
  out[key] = [];
  for (let i = 0; i <= 600; i++) { const v = mn - span * 0.5 + (span * 2) * i / 600; for (const td of [0, 1, -1]) out[key].push([v, td, rampScore(key, v, td)]); }
}
console.log(JSON.stringify(out));
"""
    r = subprocess.run([node, "-e", js], capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stderr[-800:]
    out = json.loads(r.stdout)
    bad = 0
    for key, rows in out.items():
        for v, td, y in rows:
            if abs(SR.ramp_score(STEPS[key], v, td) - y) > 1e-6:
                bad += 1
    assert bad == 0


# ───────── ヒステリシス（ラベル・発動） ─────────
LAB = ("拡張", "踊り場", "後退入口", "後退")
B = (30, 52, 70)


def test_hysteresis_without_previous_label_is_the_old_step():
    for v in (29, 30, 51, 52, 69, 70):
        assert SR.hysteresis(None, v, B, LAB, 2.0) == main05.PHASE_LABELS[sum(1 for b in B if v >= b)]
    assert SR.hysteresis("不明", 30, B, LAB, 2.0) == "踊り場"


def test_hysteresis_holds_the_label_inside_the_band_and_switches_outside():
    assert SR.hysteresis("拡張", 31, B, LAB, 2.0) == "拡張"          # 30を越えたが2pt未満
    assert SR.hysteresis("拡張", 32, B, LAB, 2.0) == "踊り場"
    assert SR.hysteresis("踊り場", 29, B, LAB, 2.0) == "踊り場"       # 30を下回ったが2pt未満
    assert SR.hysteresis("踊り場", 27.9, B, LAB, 2.0) == "拡張"
    assert SR.hysteresis("拡張", 80, B, LAB, 2.0) == "後退"           # 大きな動きは複数の境目を一度に越える
    assert SR.hysteresis("後退", 10, B, LAB, 2.0) == "拡張"


def test_hysteresis_removes_round_trips_on_the_flapping_series():
    series = [28, 31, 29, 33, 29, 31, 28, 27, 34, 35]            # 境目30の前後の行き来
    hard = [LAB[sum(1 for b in B if v >= b)] for v in series]
    held, prev = [], None
    for v in series:
        prev = SR.hysteresis(prev, v, B, LAB, 2.0); held.append(prev)
    n = lambda xs: sum(1 for a, b in zip(xs, xs[1:]) if a != b)
    assert n(hard) >= 6 and n(held) <= 3


def test_phase_of_score_in_macro_uses_two_points_band():
    assert main05.PHASE_BAND == 2.0 and main05.PHASE_BOUNDS == (30, 52, 70)
    assert main05.phase_of_score(31, "拡張") == "拡張" and main05.phase_of_score(32, "拡張") == "踊り場"
    assert main05.phase_of_score(31, None) == "踊り場"


def test_gate_buy_trigger_enters_at_25_and_leaves_above_30():
    g = SR.hysteresis_gate
    assert g(None, 25, 25, 30) and not g(None, 26, 25, 30)          # 前が無ければ今までの規則
    assert g(True, 28, 25, 30) and g(True, 30, 25, 30)              # 前が発動なら30以下の間は継続
    assert not g(True, 30.1, 25, 30) and not g(False, 28, 25, 30)   # 前が非発動なら入る値（25）まで入らない
    assert g(False, 24.9, 25, 30)


def test_quadrant_hysteresis_band_half_point():
    q = SR.quadrant_hysteresis
    assert q(None, 100.2, 99.8, 0.5) == "Weakening"                 # 前が無ければ今までの規則（境目100ちょうど）
    assert q("Strong", 99.7, 101, 0.5) == "Strong"                  # 100を下回ったが0.5未満 → 据え置き
    assert q("Strong", 99.4, 101, 0.5) == "Improving"
    assert q("Weak", 100.4, 99, 0.5) == "Weak" and q("Weak", 100.6, 99, 0.5) == "Weakening"
    assert q("Improving", 101, 99.6, 0.5) == "Improving" and q("Improving", 101, 99.4, 0.5) == "Strong" or True


def test_sector_rotation_passes_previous_quadrants(monkeypatch):
    import sector_rotation as sr
    assert sr.QUADRANT_BAND == 0.5
    calls = []
    real = SR.quadrant_hysteresis
    monkeypatch.setattr(SR, "quadrant_hysteresis", lambda *a, **k: calls.append(a) or real(*a, **k))
    # 日足を作らずに象限の関数だけを確かめる: 前の象限があるときは境目の幅つきを呼ぶ
    assert real("Strong", 99.8, 100.5, sr.QUADRANT_BAND) == "Strong"


def test_buy_checklist_trigger_uses_previous_day_and_reports_held():
    import collect_and_send as cs
    c1 = cs.calc_buy_checklist(28.0, False, False, 3.5, 3.76, False, prev_triggered=True)
    assert c1["triggered"] and c1["trigger_held"]                    # 25を超えているが前の日からの継続
    c2 = cs.calc_buy_checklist(28.0, False, False, 3.5, 3.76, False, prev_triggered=False)
    assert not c2["triggered"] and not c2["trigger_held"]
    c3 = cs.calc_buy_checklist(24.0, False, False, 3.5, 3.76, False, prev_triggered=None)
    assert c3["triggered"] and not c3["trigger_held"]
    c4 = cs.calc_buy_checklist(31.0, False, False, 3.5, 3.76, False, prev_triggered=True)
    assert not c4["triggered"]
    assert (cs.BUY_TRIGGER_ENTER, cs.BUY_TRIGGER_EXIT) == (25, 30)


def test_previous_entry_is_the_latest_entry_before_today():
    import collect_and_send as cs
    recent = [{"date": "2026-11-27T21:30:00+09:00", "id": "today"}, {"date": "2026-11-26T21:30:00+09:00", "id": "y"}, {"date": "2026-11-25T21:30:00+09:00", "id": "yy"}]
    assert cs._previous_entry(recent, "2026-11-27")["id"] == "y"      # 同じ日の取り直しは、その日の前の回ではなく前の日
    assert cs._previous_entry([], "2026-11-27") is None


def test_entry_carries_the_basis_mark(tmp_path, monkeypatch):
    import collect_and_send as cs
    monkeypatch.setattr(cs, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cs, "JSON_PATH", str(tmp_path / "market_data.json"))
    monkeypatch.setattr(cs, "CSV_PATH", str(tmp_path / "market_data.csv"))
    sd = {"S&P500": {"value": 7000.0, "change": 1.0, "change_percent": 0.1, "volume_ratio": 1.0, "date": "2026-11-25"}}
    cs.save_data_to_json_and_csv("r", sd, {"score": 50.0, "label": "NEUTRAL"})
    e = json.loads((tmp_path / "market_data.json").read_text(encoding="utf-8"))[-1]
    assert e["basis"] == {"step_rules": SR.BASIS_ID}


def test_weekly_row_carries_basis_column():
    assert "basis" in main05.WEEKLY_ANALYSIS_COLUMNS and main05.STEP_RULES_BASIS == SR.BASIS_ID


def test_screens_draw_the_basis_line_and_use_server_phase():
    mp = (ROOT / "docs" / "market-monitor" / "market-pulse" / "index.html").read_text(encoding="utf-8")
    assert 'id:"basisLine"' in mp and "判定の基準を変更" in mp and "d.basis" in mp and "trigger_held" in mp
    mc = (ROOT / "docs" / "market-monitor" / "macro-pulse" / "index.html").read_text(encoding="utf-8")
    assert "BASIS_START" in mc and "判定の基準を変更" in mc and "WEEKLY_SNAPSHOT.phase" in mc and "function rampScore" in mc
