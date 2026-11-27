"""[[MARKETPULSE-REVIEW-EPIC-1]] 順2（型E: 説明と計算のずれ）の回帰テスト（2026-11-27）。

画面・AIの説明の文言を、計算の定義（各指標の点数と寄与）から作る:
- MACRO PULSE の週次AI解説: 8指標の点数と寄与（スコアの計算そのもの）を渡し、寄与0の指標を理由に挙げさせない
- Market Pulse のAIの見解: センチメント指数の内訳（寄与pt。合計＝指数−50）を渡す。業種は「SPYより相対的に強い」
- BUYチェックリスト②: 文言と判定が同じ閾値の定数から出る
- 画面: 段階7・8の題名、セクター表の1日・5日の騰落、BUYチェックリスト②の文言
"""
import importlib.util
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "market" / "market_pulse"))


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


main05 = _load("main05_typee_test", "src/market/macro_pulse/05_main.py")


def _score(vals, keys=("yc", "hy", "philly", "cfnai", "claims", "cbcc2", "cbcc", "sahm")):
    """_compute_current_score と同じ形のdict（点数は SIGNAL_STEPS から）"""
    weights = {"yc": 20, "hy": 15, "philly": 18, "cfnai": 12, "claims": 10, "cbcc2": 10, "cbcc": 8, "sahm": 7}
    inds, inputs = {}, []
    for k in keys:
        v = vals.get(k)
        inds[k] = {"name": k.upper(), "value": v, "date": "2026-11-20", "trend": 0}
        if v is None:
            continue
        inputs.append({"key": k, "score": main05._step_score(k, v, 0), "weight": weights[k]})
    tw = sum(i["weight"] for i in inputs)
    raw = sum(i["score"] * i["weight"] for i in inputs) / tw
    return {"score": round(raw), "raw": raw, "phase": "踊り場", "indicators": inds, "score_inputs": inputs, "weight_total": tw}


NOW = {"yc": 0.3, "hy": 3.52, "philly": -14.6, "cfnai": -0.1, "claims": 230000, "cbcc2": 1290, "cbcc": 50, "sahm": 0.1}
PREV = {**NOW, "hy": 3.48, "cbcc2": 1306}       # HYが3.5%を超え（40点→…）、許可が1,300を割った（点数が変わるのは2指標だけ）


def test_contributions_sum_to_the_raw_score_change_and_flag_zero():
    cur, prev = _score(NOW), _score(PREV)
    rows = main05.score_contributions(cur, prev)
    assert abs(sum(r["contribution"] for r in rows) - (cur["raw"] - prev["raw"])) < 0.06      # 各寄与を小数2桁に丸めた分だけ違う
    by = {r["key"]: r for r in rows}
    assert by["hy"]["contribution"] > 0 and by["cbcc2"]["contribution"] > 0
    # 値が動いても点数が同じ指標（philly・claims）は寄与0
    assert by["philly"]["zero"] and by["claims"]["zero"] and by["philly"]["score_now"] == by["philly"]["score_prev"]
    assert [r["key"] for r in rows][:2] == ["hy", "cbcc2"] or [r["key"] for r in rows][:2] == ["cbcc2", "hy"]   # 寄与の大きい順
    assert main05.score_contributions(None, prev) == [] and main05.score_contributions({"score": None}, prev) == []


def test_zero_contribution_mentions_detects_only_zero_indicators():
    rows = main05.score_contributions(_score(NOW), _score(PREV))
    ok = "HYスプレッドの拡大と住宅着工許可の減少がスコアを押し上げた。"
    bad = ok + "フィラデルフィア連銀製造業指数の悪化と新規失業保険申請の増加も背景にある。"
    assert main05.zero_contribution_mentions(ok, rows) == []
    assert sorted(main05.zero_contribution_mentions(bad, rows)) == ["claims", "philly"]


def test_weekly_prompt_has_points_and_contributions_and_retries_once_on_zero_mention(monkeypatch):
    rows = main05.score_contributions(_score(NOW), _score(PREV))
    prompts = []
    answers = iter([
        json.dumps({"summary": "s", "factor_analysis": "フィラデルフィア連銀の悪化が主因", "watchpoints": "w", "indicator_comments": ""}),
        json.dumps({"summary": "s", "factor_analysis": "HYスプレッドと住宅着工許可が主因", "watchpoints": "w", "indicator_comments": ""}),
    ])

    def fake_grok(messages, model, label, **kw):
        prompts.append(messages[0]["content"])
        return next(answers)

    monkeypatch.setenv("XAI_API_KEY", "test")
    monkeypatch.setattr(main05, "grok_text", fake_grok)
    from datetime import date
    out = main05.generate_weekly_analysis_with_grok(
        date(2026, 11, 27), _score(NOW), [], 3, 2, {"regime": "BALANCED"}, {}, contributions=rows)
    p = prompts[0]
    assert "8指標の点数と寄与" in p and "寄与0）" in p and "寄与0の指標（PHILLY、CLAIMS）" in p.replace("(", "（").replace(")", "）") or "寄与0の指標" in p
    assert re.search(r"HY: 点数 [\d.]+（1週前 [\d.]+）.*寄与 \+", p)
    assert len(prompts) == 2 and "修正指示" in prompts[1] and "フィラデルフィア" in prompts[1]       # 言い直しは1回
    assert out["factor_analysis"] == "HYスプレッドと住宅着工許可が主因"


def test_weekly_prompt_keeps_first_answer_when_retry_still_mentions_zero(monkeypatch):
    rows = main05.score_contributions(_score(NOW), _score(PREV))
    bad = json.dumps({"summary": "s", "factor_analysis": "新規失業保険申請が主因", "watchpoints": "w", "indicator_comments": ""})
    calls = []
    monkeypatch.setenv("XAI_API_KEY", "test")
    monkeypatch.setattr(main05, "grok_text", lambda *a, **k: calls.append(1) or bad)
    from datetime import date
    out = main05.generate_weekly_analysis_with_grok(date(2026, 11, 27), _score(NOW), [], 3, 2, {}, {}, contributions=rows)
    assert len(calls) == 2 and out["factor_analysis"] == "新規失業保険申請が主因"


# ───────── Market Pulse ─────────
import collect_and_send as cs  # noqa: E402


def test_sentiment_contributions_sum_to_index_minus_50_and_flag_unused():
    sub = {"vix_level": {"score": 23.9, "weight": 0.225, "raw": 28.36}, "sp500_ma_dev": {"score": 15.5, "weight": 0.18, "raw": -5.52},
           "ad_ratio": {"score": 38.0, "weight": 0.135, "raw": 1.07}, "hyg_lqd_dir": {"score": 23.3, "weight": 0.108, "raw": -0.002668},
           "nh_nl": {"score": 74.0, "weight": 0.09, "raw": 24}, "growth_value": {"score": 24.2, "weight": 0.09, "raw": -1.55},
           "distribution": {"score": 50.0, "weight": 0.072, "raw": {"vol_ratio": 1.05, "chg_pct": -0.25}},
           "rsp_spy_divergence": {"score": 66.2, "weight": 0.10, "raw": 0.325}}
    rows = cs.sentiment_contributions(sub)
    total_w = sum(v["weight"] for v in sub.values())
    index = sum(v["score"] * v["weight"] for v in sub.values()) / total_w
    assert abs(sum(r["寄与pt"] for r in rows) - (index - 50)) < 0.1
    assert rows[0]["指標"] == "S&P500の50日MA乖離" and rows[0]["寄与pt"] < 0                      # 寄与の絶対値の大きい順
    dist = next(r for r in rows if r["指標"] == "出来高の売り抜け判定")
    assert dist["寄与pt"] == 0 and dist["使った値"] is True                          # 中立（50）の値は寄与0
    sub["nh_nl"] = {"score": 50.0, "weight": 0.09, "raw": None}                         # 値が取れず中立に固定
    nh = next(r for r in cs.sentiment_contributions(sub) if r["指標"] == "新高値−新安値")
    assert nh["使った値"] is False and nh["寄与pt"] == 0
    assert cs.sentiment_contributions(None) is None and cs.sentiment_contributions({}) is None


def test_ai_facts_include_contributions_and_prompt_forbids_zero_reasons():
    facts = cs.build_ai_facts({"stages": {}}, {}, {"score": 34.9, "label": "FEAR", "sub_scores": {"vix_level": {"score": 24.0, "weight": 0.5, "raw": 28.0}}},
                              {}, {}, {})
    assert facts["センチメント指数の内訳（寄与ptの合計＝指数−50）"][0]["指標"] == "VIX水準"
    assert "寄与ptが0の指標" in cs.AI_PROMPT and "SPYより相対的に強い" in cs.AI_PROMPT


def test_buy_checklist_text_and_judgement_share_one_threshold():
    assert cs.HY_BUY_MARGIN == 0.30 and f"{cs.HY_BUY_MARGIN:.2f}%" in cs.HY_BUY_RULE_TEXT
    c = cs.calc_buy_checklist(9.6, False, False, hy_current=3.52, hy_max_90d=3.52, hindenburg_active=True)
    assert c["checks"]["hy_spread"]["rule"] == cs.HY_BUY_RULE_TEXT and c["checks"]["hy_spread"]["point"] == 0
    c2 = cs.calc_buy_checklist(9.6, False, False, hy_current=3.20, hy_max_90d=3.52, hindenburg_active=True)
    assert c2["checks"]["hy_spread"]["point"] == 1


# ───────── 画面の文言（index.html の静的な部分） ─────────
def test_screen_wording():
    html = (ROOT / "docs" / "market-monitor" / "market-pulse" / "index.html").read_text(encoding="utf-8")
    assert "明日どうなるか" not in html and html.count("過去の似た日、その後どうなったか") >= 2
    assert "資金が向かっているセクター" not in html and "SPYより相対的に強いセクター" in html
    assert "縮小に転じているか" not in html and "0.30%以上低いか" in html
    assert "5日の騰落" in html and "絶対の騰落ではありません" in html
