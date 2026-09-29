"""Market Pulse 8段階の結論・天気・data_quality・過去の実績（指示書㉓ 実装A）の単体テスト。

閾値の境界値、入力欠損時にNone、天気の判定表、段階8の先読みなし（未来の値を書き換えても結果が
変わらない）を確認する。判定ルールは docs/architecture/MARKET_PULSE_REDESIGN.md。
"""
import os
import sys
from datetime import date, datetime, timedelta, timezone

import pytest

_MARKET_PULSE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "market", "market_pulse")
)
if _MARKET_PULSE_DIR not in sys.path:
    sys.path.insert(0, _MARKET_PULSE_DIR)

import stage_conclusions as sc  # noqa: E402
import collect_and_send as cs  # noqa: E402


def _ind(**kw):
    """indicatorsの最小構成。kw: 名前=(change_percent, date) または change_percentのみ"""
    out = {}
    for k, v in kw.items():
        name = {"spx": "S&P500", "ndx": "NASDAQ", "vix": "VIX指数", "fx": "ドル円", "oil": "WTI原油",
                "gold": "金（GOLD）"}[k]
        out[name] = {"value": 1.0, "change_percent": v, "date": "2026-09-25"}
    return out


class TestStage1:
    @pytest.mark.parametrize("pct,label", [
        (1.0, "大幅高"), (0.99, "上昇"), (0.3, "上昇"), (0.29, "小動き"), (-0.29, "小動き"),
        (-0.3, "下落"), (-0.99, "下落"), (-1.0, "大幅安"), (None, None),
    ])
    def test_bands(self, pct, label):
        assert sc.s1_label(pct) == label

    def test_tags(self):
        ind = _ind(spx=0.1, ndx=0.7, vix=-10.0, fx=0.69, oil=3.0, gold=-1.5)
        ind["米10年債"] = {"value": 4.2, "change": 0.08, "change_bp": 8.0, "date": "2026-09-25"}
        tags = sc.stage1(ind)["tags"]
        assert tags == ["NASDAQ優位（+0.60pt）", "VIX急変（-10.0%）", "10年債（+8bp）", "原油（+3.00%）", "金（-1.50%）"]

    def test_bp_falls_back_to_change(self):
        assert sc.tnx_change_bp({"米10年債": {"change": -0.05}}) == -5.0
        assert sc.tnx_change_bp({}) is None


class TestStage2:
    def test_none_when_all_missing(self):
        assert sc.stage2({})["line"] is None

    def test_wording_is_fact_only(self):
        assert sc.stage2(_ind(oil=3.1, vix=2.0, fx=0.1))["line"] == "同時に大きく動いたもの：原油"
        assert sc.stage2(_ind(oil=1.0, vix=2.0, fx=0.1))["line"] == "同時に大きく動いたもの：なし"
        ind = _ind(oil=-3.5, vix=12.0, fx=0.1)
        ind["米10年債"] = {"change_bp": 9.0}
        assert sc.stage2(ind)["line"] == "同時に大きく動いたもの：金利・原油・VIX"


class TestStage3:
    @pytest.mark.parametrize("spx,adv,dec,label", [
        (0.5, 300, 200, "広がりのある上昇"), (0.5, 250, 200, "やや広がりのある上昇"), (0.5, 199, 200, "一部主導の上昇"),
        (-0.5, 134, 200, "広く売られた下落"), (-0.5, 190, 200, "やや広い下落"), (-0.5, 250, 200, "指数主導の下落"),
        (0.1, 100, 400, "方向感なし"), (None, 1, 1, None), (0.5, None, 1, None), (0.5, 0, 0, None),
    ])
    def test_labels(self, spx, adv, dec, label):
        assert sc.s3_label(spx, adv, dec) == label


class TestStage4And5:
    @pytest.mark.parametrize("risk,safe,label", [
        (0.3, 0.0, "リスク資産へ"), (0.3, 0.01, "偏りなし"), (-0.3, 0.1, "安全資産へ"),
        (-0.3, -0.1, "全面安（現金化）"), (0.0, 0.0, "偏りなし"), (None, 0.1, None),
    ])
    def test_stage4(self, risk, safe, label):
        assert sc.s4_label(risk, safe) == label

    def test_stage4_from_asset_flow(self):
        af = {"equity": {"change_pct": 1.0}, "hy_bond": {"change_pct": 0.2}, "long_bond": {"change_pct": -0.2},
              "gold": {"change_pct": 0.0}}
        assert sc.stage4(af)["label"] == "リスク資産へ"
        del af["gold"]
        assert sc.stage4(af)["label"] is None

    @pytest.mark.parametrize("gv,label", [(0.5, "グロース優勢"), (0.49, "拮抗"), (-0.5, "バリュー優勢"), (None, None)])
    def test_stage5(self, gv, label):
        assert sc.s5_label(gv) == label


class TestWeather:
    @pytest.mark.parametrize("s3,s4,w", [
        ("広く売られた下落", "安全資産へ", "嵐"), ("広く売られた下落", "全面安（現金化）", "嵐"),
        ("広く売られた下落", "偏りなし", "曇り"), ("広がりのある上昇", "偏りなし", "晴れ"),
        ("一部主導の上昇", "リスク資産へ", "晴れ"), ("やや広がりのある上昇", "安全資産へ", "曇り"),
        ("方向感なし", "リスク資産へ", "曇り"), (None, "偏りなし", None), ("方向感なし", None, None),
    ])
    def test_v3_table(self, s3, s4, w):
        assert sc.weather(s3, s4) == w


class TestDataQuality:
    def _entry(self, spx_date="2026-09-25", other="2026-09-25", fallback=False):
        ind = {k: {"change_percent": 0.1, "date": other} for k in sc.DQ_INDICATORS}
        ind["S&P500"] = {"change_percent": 0.1, "date": spx_date, **({"is_fallback": True} if fallback else {})}
        af = {k: {"change_pct": 0.1, "date": other} for k in sc.DQ_ASSET_FLOW}
        return ind, af, {"date": other}

    def test_complete(self):
        dq = sc.data_quality(*self._entry(), "2026-09-25")
        assert dq["status"] == "complete" and dq["old_elements"] == []
        assert sc.stage0(dq)["line"] == "2026-09-25の終値"

    def test_partial_lists_stages(self):
        ind, af, b = self._entry()
        af["gold"]["date"] = "2026-09-24"
        dq = sc.data_quality(ind, af, b, "2026-09-25")
        assert dq["status"] == "partial" and dq["old_elements"] == ["asset_flow.gold"] and dq["old_stages"] == [4]

    def test_stale_when_spx_old_or_fallback(self):
        assert sc.data_quality(*self._entry(spx_date="2026-09-24"), "2026-09-25")["status"] == "stale"
        assert sc.data_quality(*self._entry(fallback=True), "2026-09-25")["status"] == "stale"

    def test_unknown_without_expected_or_spx_date(self):
        assert sc.data_quality(*self._entry(), None)["status"] == "unknown"
        ind, af, b = self._entry()
        ind["S&P500"] = None
        assert sc.data_quality(ind, af, b, "2026-09-25")["status"] == "unknown"


def _series(closes, vix, start=date(2021, 1, 4)):
    """合成の終値系列（平日の連番）と、reader.get_price_series_as_of互換の取得関数"""
    days, d = [], start
    while len(days) < len(closes):
        if d.weekday() < 5:
            days.append(d.isoformat())
        d += timedelta(days=1)
    data = {"^GSPC": dict(zip(days, closes)), "^VIX": dict(zip(days, vix))}

    def get(sym, as_of, n):
        return [{"date": k, "close": v} for k, v in sorted(data[sym].items()) if k <= as_of][-n:]
    return days, get


class TestStage8:
    def test_no_lookahead(self):
        """as_of以降の値を書き換えても、as_of時点の結果は変わらない"""
        import random
        rnd = random.Random(1)
        closes = [100.0]
        for _ in range(399):
            closes.append(closes[-1] * (1 + rnd.uniform(-0.015, 0.016)))
        vix = [rnd.uniform(12, 25) for _ in closes]
        days, get = _series(closes, vix)
        as_of = days[300]
        a = sc.stage8(get, as_of)
        closes2 = closes[:301] + [c * 3 for c in closes[301:]]
        _, get2 = _series(closes2, vix)
        assert sc.stage8(get2, as_of) == a
        assert a["signals"]["stage1_vix"]["signal"] is not None

    def test_baseline_diff_and_labels(self):
        # 上昇日（+0.5%）の翌日は必ず上昇、それ以外の翌日は下落する系列 → 「上昇」シグナルは平常時より上昇が多い
        closes, pattern = [100.0], [0.005, 0.005, -0.004, -0.004] * 60
        for r in pattern:
            closes.append(closes[-1] * (1 + r))
        days, get = _series(closes, [14.0] * len(closes))
        r = sc.stage8(get, days[-1])
        o1 = next(o for o in r["signals"]["stage1_vix"]["outcomes"] if o["horizon"] == 1)
        assert r["signals"]["stage1_vix"]["signal"] == "下落|VIX<15"
        assert o1["n"] >= 20 and o1["label"] in ("平常時より上昇が多い", "平常時より上昇が少ない", "平常時と差なし")
        assert o1["base_up_pct"] == pytest.approx(50.0, abs=1.0)
        assert "guide" in o1 and r["line"].startswith("5営業日後: ")

    @pytest.mark.parametrize("n,z,label", [
        (19, 5.0, "件数不足"), (20, 2.0, "平常時より上昇が多い"), (20, -2.0, "平常時より上昇が少ない"),
        (20, 1.99, "平常時と差なし"), (20, -1.99, "平常時と差なし"),
    ])
    def test_label_thresholds_z(self, n, z, label):
        """指示書㉔ STEP B: |z|≥2のときだけ多い／少ない（差のptは判定に使わない）"""
        assert sc.s8_label(n, z) == label

    def test_headline_uses_stage1_only_5d(self):
        closes, pattern = [100.0], [0.005, 0.005, -0.004, -0.004] * 60
        for r in pattern:
            closes.append(closes[-1] * (1 + r))
        days, get = _series(closes, [14.0] * len(closes))
        r = sc.stage8(get, days[-1])
        five = next(o for o in r["signals"]["stage1"]["outcomes"] if o["horizon"] == 5)
        assert r["headline_signal"] == "stage1" and r["label"] == five["label"]
        assert r["line"].startswith("5営業日後: ") and "z=" in r["line"]

    def test_missing_as_of(self):
        assert sc.stage8(lambda *a: [], None)["line"] is None


class TestCollectHelpers:
    @pytest.mark.parametrize("s,label", [(25, "EXTREME FEAR"), (25.1, "FEAR"), (45, "FEAR"), (55, "NEUTRAL"),
                                         (75, "GREED"), (75.1, "EXTREME GREED")])
    def test_zone_label_cnn(self, s, label):
        assert cs.zone_label(s) == label

    @pytest.mark.parametrize("tlt,spy,out", [(0.5, -0.6, "債券買い"), (-0.01, 0.2, "債券売り"), (0.0, 0.2, "中立"),
                                             (0.4, 0.1, "中立"), (None, None, "中立")])
    def test_bond_direction(self, tlt, spy, out):
        assert cs.bond_direction(tlt, spy) == out

    def test_sentiment_signal_uses_date_window(self):
        hist = [("2026-08-01", 90.0), ("2026-09-10", 72.0), ("2026-09-20", 68.0)]
        s = cs.sentiment_signal(65.0, hist, "2026-09-01")
        assert s["signal"] == "TAKE PROFIT" and s["peak"] == 72.0     # 08-01の90は窓の外
        assert cs.sentiment_signal(20.0, hist, "2026-09-01")["signal"] == "BUY"
        assert cs.sentiment_signal(69.0, hist, "2026-09-01")["signal"] == "HOLD"

    def test_signal_window_is_20_trading_days(self):
        start = cs.signal_window_start(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        assert start == "2026-08-28"

    def test_split_haiku(self):
        body, haiku = cs.split_haiku("見解の本文。\n二文目。\n俳句：秋の風 指数静かに 雲流る")
        assert body == "見解の本文。\n二文目。" and haiku == "秋の風 指数静かに 雲流る"
        assert cs.split_haiku("俳句なし")[1] is None

    def test_ai_facts_contain_only_given_values(self):
        """AIの入力JSONは、渡した事実（結論1行・数値・基準日）だけで構成される"""
        stage = {"stages": {"1": {"line": "S&P500 +0.51%（上昇）", "tags": []}}, "weather": {"label": "晴れ"},
                 "data_quality": {"status": "complete", "expected_close_date": "2026-09-25", "as_of": {}}}
        ind = {"S&P500": {"value": 7000.0, "change_percent": 0.51, "date": "2026-09-25", "volume_ratio": 1.2}}
        f = cs.build_ai_facts(stage, ind, {"score": 55.0, "label": "NEUTRAL"}, None, None, None)
        assert f["今日の結論"] == {"段階1": "S&P500 +0.51%（上昇）"}
        assert f["指標"]["S&P500"] == {"value": 7000.0, "change_percent": 0.51, "date": "2026-09-25"}
        assert "ニュース" not in str(f)
