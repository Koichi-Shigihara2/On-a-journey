"""Market Pulse 実装B（指示書㉖）: セクター4象限・半導体とM7・監視銘柄の単体テスト。

RS-Ratio・RS-Momentumは手計算と一致すること、象限の境界、セクターのマッピング、監視銘柄の和集合、段階6の正負対称、
段階7の結論1行、段階1のSOX・M7のタグ、段階5のStrongのセクター名を確認する。ネットワークアクセスはしない。
"""
import json
import os
import sys
from datetime import date, timedelta

import pytest

_MARKET_PULSE_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "market", "market_pulse"))
if _MARKET_PULSE_DIR not in sys.path:
    sys.path.insert(0, _MARKET_PULSE_DIR)

import sector_rotation as sr  # noqa: E402
import stage_conclusions as sc  # noqa: E402


class TestRsRatioMomentum:
    def test_matches_hand_calculation(self):
        # 3週平均・1週ラグの小さな例で手計算と一致
        sec = [10, 11, 12, 13, 12]
        spy = [100, 100, 100, 100, 100]
        pts = sr.rs_ratio_momentum(sec, spy, n=3, lag=1)
        rs = [x / 100 for x in sec]
        r2 = rs[2] / (sum(rs[0:3]) / 3) * 100
        r3 = rs[3] / (sum(rs[1:4]) / 3) * 100
        r4 = rs[4] / (sum(rs[2:5]) / 3) * 100
        assert pts[0] == (None, None) and pts[1] == (None, None)
        assert pts[2][0] == pytest.approx(r2) and pts[2][1] is None
        assert pts[3] == (pytest.approx(r3), pytest.approx(r3 / r2 * 100))
        assert pts[4] == (pytest.approx(r4), pytest.approx(r4 / r3 * 100))

    @pytest.mark.parametrize("ratio,mom,q", [(100, 100, "Strong"), (100, 99.99, "Weakening"), (99.99, 100, "Improving"),
                                             (99.99, 99.99, "Weak")])
    def test_quadrant_boundaries(self, ratio, mom, q):
        assert sr.quadrant(ratio, mom) == q


def _series_factory(closes_by_symbol):
    def get(sym, as_of, days):
        c = closes_by_symbol.get(sym, {})
        return [{"date": d, "close": v} for d, v in sorted(c.items()) if d <= as_of][-days:]
    return get


def _weekdays(start, n):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


class TestSectorRotation:
    def test_leader_is_strong_and_excluded_when_no_close(self):
        days = _weekdays(date(2026, 3, 2), 150)
        data = {"SPY": {d: 100.0 for d in days}}
        for i, s in enumerate(sr.SECTORS):
            # XLKだけが最近加速して上昇、他は横ばい
            data[s] = {d: 50.0 * (1.004 ** max(0, j - 100) if s == "XLK" else 1.0) for j, d in enumerate(days)}
        del data["XLE"][days[-1]]   # 当日の終値が無い → 象限から外す
        r = sr.sector_rotation(_series_factory(data), days[-1])
        assert r["sectors"]["XLK"]["quadrant"] == "Strong"
        assert "XLE" in r["excluded"] and "XLE" not in r["sectors"]
        assert len(r["sectors"]["XLK"]["trail"]) == sr.TRAIL_WEEKS
        assert r["by_quadrant"]["Strong"][0] == "XLK"

    def test_no_lookahead(self):
        days = _weekdays(date(2026, 3, 2), 150)
        data = {"SPY": {d: 100.0 + j * 0.1 for j, d in enumerate(days)}}
        for s in sr.SECTORS:
            data[s] = {d: 50.0 + j * 0.05 for j, d in enumerate(days)}
        a = sr.sector_rotation(_series_factory(data), days[120])
        for s in sr.SECTORS:
            for d in days[121:]:
                data[s][d] *= 3
        assert sr.sector_rotation(_series_factory(data), days[120]) == a


class TestSemisM7AndStage6:
    def test_m7_requires_all_members_same_prev_day(self):
        days = ["2026-09-28", "2026-09-29"]
        data = {"^SOX": {days[0]: 100.0, days[1]: 103.0}}
        for t in sr.M7:
            data[t] = {days[0]: 100.0, days[1]: 101.0}
        out = sr.semis_m7(_series_factory(data), days[1], 0.5)
        assert out["sox_pct"] == 3.0 and out["m7_pct"] == 1.0 and out["sox_vs_sp500_pt"] == 2.5 and out["m7_vs_sp500_pt"] == 0.5
        del data["TSLA"][days[1]]
        out = sr.semis_m7(_series_factory(data), days[1], 0.5)
        assert out["m7_pct"] is None and out["m7_missing"] == ["TSLA"]

    @pytest.mark.parametrize("sox,m7,label", [
        (1.0, 0.5, "半導体・M7がけん引"), (-1.0, -0.5, "半導体・M7が重し"), (1.0, -0.5, "半導体とM7が逆方向"),
        (-1.0, 0.5, "半導体とM7が逆方向"), (1.0, 0.0, "半導体がけん引"), (-1.0, 0.0, "半導体が重し"),
        (0.0, 0.5, "M7がけん引"), (0.0, -0.5, "M7が重し"), (0.99, 0.49, "特定分野の突出なし"), (None, 0.5, None),
    ])
    def test_stage6_symmetric(self, sox, m7, label):
        assert sr.s6_label(sox, m7) == label


class TestWatchList:
    def test_union_order_and_line(self, tmp_path, monkeypatch):
        root = tmp_path
        (root / "docs" / "portfolio" / "data").mkdir(parents=True)
        (root / "docs" / "portfolio" / "data" / "portfolio.json").write_text(json.dumps(
            {"brokers": {"a": {"positions": {"AAA": {}, "BBB": {}}}, "b": {"positions": {"CCC": {}}}}}), encoding="utf-8")
        monkeypatch.setattr(sr, "watch_tickers", lambda repo: {"held": ["AAA", "BBB", "CCC"], "tail": ["BBB", "DDD"],
                                                              "all": ["AAA", "BBB", "CCC", "DDD"]})
        attrs = {"AAA": {"sector": "Technology"}, "BBB": {"sector": "Energy"}, "CCC": {"sector": "Technology"},
                 "DDD": {"sector": "Communication Services"}}
        rot = {"sectors": {"XLK": {"quadrant": "Strong", "momentum": 101.0}, "XLE": {"quadrant": "Weak", "momentum": 95.0},
                           "XLC": {"quadrant": "Improving", "momentum": 103.0}}}
        wl = sr.watch_list(str(root), rot, lambda t: attrs.get(t))
        assert wl["flowing"] == ["DDD", "AAA", "CCC"]      # Momentumの高い順（XLC→XLK）、同じセクター内はティッカー順
        assert [r["ticker"] for r in wl["rows"]] == ["DDD", "AAA", "CCC", "BBB"]
        assert sr.stage7_line(wl) == "資金が向かっているセクターにいる監視銘柄：DDD・AAA ほか1銘柄"
        assert sr.stage7_line({"flowing": []}) == "資金が向かっているセクターにいる監視銘柄：なし"
        assert sr.stage7_line({"flowing": ["X", "Y"]}) == "資金が向かっているセクターにいる監視銘柄：X・Y"

    def test_real_watch_tickers_is_union(self):
        repo = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
        wt = sr.watch_tickers(repo)
        assert set(wt["all"]) == set(wt["held"]) | set(wt["tail"])

    def test_sector_mapping_covers_yfinance_sectors(self):
        assert set(sr.SECTOR_MAP.values()) == set(sr.SECTORS)


class TestStageIntegration:
    def test_stage1_tags_sox_m7(self):
        ind = {"S&P500": {"change_percent": 0.1}}
        tags = sc.stage1(ind, {"sox_pct": -2.0, "m7_vs_sp500_pt": 1.0})["tags"]
        assert tags == ["SOX（-2.00%）", "M7均等加重（S&P500比+1.00pt）"]
        assert sc.stage1(ind, {"sox_pct": 1.99, "m7_vs_sp500_pt": 0.99})["tags"] == []

    def test_stage5_appends_strong(self):
        ind = {"グロース対バリュー比": {"diff_percent": 0.58}}
        assert sc.stage5(ind, {"by_quadrant": {"Strong": ["XLK", "XLC"]}})["line"] == "グロース優勢（IVW−IVE +0.58pt）／Strong: XLK・XLC"
        assert sc.stage5(ind, {"by_quadrant": {"Strong": []}})["line"].endswith("／Strong: なし")
        assert sc.stage5(ind)["line"] == "グロース優勢（IVW−IVE +0.58pt）"

    def test_excluded_sector_makes_partial(self):
        ind = {k: {"change_percent": 0.1, "date": "2026-09-29"} for k in sc.DQ_INDICATORS}
        af = {k: {"change_pct": 0.1, "date": "2026-09-29"} for k in sc.DQ_ASSET_FLOW}
        r = sc.build_stage_conclusions(ind, af, {"date": "2026-09-29"}, "2026-09-29",
                                       implb={"sector_rotation": {"excluded": ["XLE"], "sectors": {}, "by_quadrant": {}}})
        assert r["data_quality"]["status"] == "partial" and 5 in r["data_quality"]["old_stages"]
