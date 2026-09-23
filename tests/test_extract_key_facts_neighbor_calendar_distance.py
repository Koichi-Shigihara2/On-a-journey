"""
tests/test_extract_key_facts_neighbor_calendar_distance.py

[[CART-QUARTERLY-REVENUE-EXTRACTION-GAP-1]]段階2aの回帰テスト。

_neighbor_quarter_diluted_shares()は、target_keyに「最も近い」他の
四半期を探して実株数を引き継ぐフォールバック（[[ASTS-SHARES-
OSCILLATION-1]]由来）だが、従来は(fiscal_year, quarter)のタプル順序
で近さを判定していた。決算期変更銘柄（RCAT: 2023年に12月→4月決算へ
変更）では、この期間中にタプル順序と実際の暦日順序が一致しない区間が
生じ、2018年FY10-Kの株数（196,802,751株）が2022-2023年の四半期
（本来はより暦日的に近い2023年4月期10-KTの53,860,199株を引き継ぐべき）
に誤って伝播していた（EPS-UPC-PREREORG-1横展開調査で発見、実データ
common/sec_data/data/RCAT/company_facts.jsonで確認済み）。

本テストは、period_endの暦日距離で最近傍を判定する新実装が、
タプル順序上は「遠い」が暦日的には「近い」候補を正しく選ぶことを
確認する（RCAT実例を最小化したモック）。

実行方法:
    python -m pytest tests/test_extract_key_facts_neighbor_calendar_distance.py -v
"""

import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import src.value.adjusted_eps_analyzer.extract_key_facts as ekf  # noqa: E402


def _q(end, diluted_shares=None):
    d = {"end": end}
    if diluted_shares is not None:
        d["diluted_shares"] = {"value": diluted_shares, "unit": "shares"}
    return d


class TestCalendarDistanceOverridesTupleOrder:
    """RCAT実例の最小化: (fiscal_year, quarter)のタプル順序では
    2018年FY(古い実データ)の方が「遠い」インデックスに位置するが、
    暦日的には2023年の10-KTの方が近い。新実装はタプル順序ではなく
    暦日距離を採用するため、正しく近い方（2023年10-KT）を選ぶこと"""

    def test_picks_calendar_nearest_not_tuple_nearest(self):
        quarters_map = {
            # 2018年FYの10-K相当（タプル順序上は(2018, 4)で(2022, 3)より
            # 遠いが、実際にはこちらが正しい候補ではない）
            (2018, 4): _q("2018-12-31", diluted_shares=196_802_751),
            # target: 2022年10月期のQ3相当、diluted_sharesが欠落
            (2022, 3): _q("2022-10-31"),
            # 2023年4月期の10-KT相当（決算期変更の移行期報告書、
            # タプル順序上は(2023, 4)であり(2022, 3)から見て
            # 直接の隣接ではないが、暦日的には最も近い）
            (2023, 4): _q("2023-04-30", diluted_shares=53_860_199),
        }
        result = ekf._neighbor_quarter_diluted_shares(quarters_map, (2022, 3))
        assert result == 53_860_199, (
            "暦日的に最も近い2023年10-KTの株数を採用すべき"
            "（2018年FYの株数が誤って採用されてはならない）"
        )

    def test_tie_break_prefers_prior_when_equidistant(self):
        """target からの暦日距離が同一（105日）の場合は直前を優先する"""
        quarters_map = {
            (2023, 1): _q("2023-01-01", diluted_shares=100_000_000),  # 105日前
            (2023, 4): _q("2023-07-30", diluted_shares=200_000_000),  # 105日後
            (2023, 2): _q("2023-04-16"),  # target
        }
        result = ekf._neighbor_quarter_diluted_shares(quarters_map, (2023, 2))
        assert result == 100_000_000, "同距離の場合は直前を優先すべき"

    def test_no_upper_bound_on_distance(self):
        """距離の上限は設けない（ASTS-SHARES-OSCILLATION-1で塞いだ
        yfinance逆行伝播の再発防止のため、遠くても実データがあれば採用する）"""
        quarters_map = {
            (2015, 1): _q("2015-03-31", diluted_shares=50_000_000),  # 10年近く前
            (2025, 1): _q("2025-03-31"),  # target
        }
        result = ekf._neighbor_quarter_diluted_shares(quarters_map, (2025, 1))
        assert result == 50_000_000, "遠くても他に候補がなければ採用すべき（上限なし）"


class TestExistingBehaviorUnaffected:
    """通常（決算期変更なし）のケースでは、暦日距離判定でも従来と
    同じ結果になること（回帰確認）"""

    def test_normal_sequential_quarters_prior_preferred(self):
        quarters_map = {
            (2023, 1): _q("2023-03-31", diluted_shares=100_000_000),
            (2023, 2): _q("2023-06-30"),  # target
            (2023, 3): _q("2023-09-30", diluted_shares=120_000_000),
        }
        result = ekf._neighbor_quarter_diluted_shares(quarters_map, (2023, 2))
        assert result == 100_000_000, "直前(Q1)が同距離以下の場合は直前を優先すべき"

    def test_no_other_quarters_returns_zero(self):
        quarters_map = {(2023, 1): _q("2023-03-31")}
        result = ekf._neighbor_quarter_diluted_shares(quarters_map, (2023, 1))
        assert result == 0.0
