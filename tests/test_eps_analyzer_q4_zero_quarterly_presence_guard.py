"""
tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py

[[EPS-LITE-ANNUAL-AS-QUARTERLY-1]]の回帰テスト。

extract_key_facts.pyの「Q4に年次の調整項目タグを追加」ループ
（772〜844行目）は、YTD法・Q1-3合計法のいずれでも該当タグの四半期
データが見つからない場合、「未申告（キー自体が無い）」と「申告はあるが
実際に0円」を区別できず後者と誤認し、q123_sum=0として
`Q4 = 年次 − 0 = 年次そのまま`という誤った値を書き込んでいた。

LITE（Lumentum）はrevenue系タグを年度によって使い分けており（ASC606
移行等）、ある年度で「年次のみ申告・四半期(10-Q)申告が皆無」のタグが
このループに巻き込まれ、adjusted_eps=-95.004という桁違いの異常値を
生んでいた。APP等tanuki=true全99銘柄中9銘柄で同型のrevenue誤抽出が
確認され、さらに調査の結果depreciation・sbc・amortization_intangibles
等の調整項目タグにも同一の構造的欠陥があることが判明した
（保有銘柄ではPLTR・SOFIのadjusted_epsに実際の影響を確認）。

本テストは、少なくとも1四半期でそのタグの申告キー自体が存在する場合に
限り差し引き法を適用し、皆無の場合はQ4への書き込み自体をスキップする
ガードを検証する。

実行方法:
    python -m pytest tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py -v
"""

import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import src.value.adjusted_eps_analyzer.extract_key_facts as ekf  # noqa: E402


def _units(*items, unit="USD"):
    return {unit: list(items)}


def _fact(start, end, val, filed, form="10-Q", fp=None):
    d = {"start": start, "end": end, "val": val, "filed": filed, "form": form}
    if fp is not None:
        d["fp"] = fp
    return d


def _patch_common(monkeypatch, facts):
    monkeypatch.setattr(ekf, "get_cik", lambda ticker: "0000000001")
    monkeypatch.setattr(ekf, "fetch_company_facts", lambda cik: facts)


def _base_net_income_and_shares():
    """Q1〜Q3・年次のnet_income/diluted_sharesを一律で用意する（Q4キー生成に必要）"""
    return {
        "NetIncomeLoss": {
            "units": _units(
                _fact("2023-01-01", "2023-03-31", 1_000_000, "2023-04-15"),
                _fact("2023-04-01", "2023-06-30", 1_100_000, "2023-07-15"),
                _fact("2023-07-01", "2023-09-30", 1_200_000, "2023-10-15"),
                _fact("2023-01-01", "2023-12-31", 5_000_000, "2024-02-15",
                      form="10-K", fp="FY"),
            )
        },
        "WeightedAverageNumberOfDilutedSharesOutstanding": {
            "units": _units(
                _fact("2023-01-01", "2023-03-31", 100_000_000, "2023-04-15"),
                _fact("2023-04-01", "2023-06-30", 100_000_000, "2023-07-15"),
                _fact("2023-07-01", "2023-09-30", 100_000_000, "2023-10-15"),
                _fact("2023-01-01", "2023-12-31", 100_000_000, "2024-02-15",
                      form="10-K", fp="FY"),
                unit="shares",
            )
        },
    }


class TestSkipWhenZeroQuarterlyPresence:
    """LITE/APP実例相当: 年次のみ申告・四半期(10-Q)申告が皆無のタグは、
    Q4への書き込み自体をスキップする（他の候補タグへのフォールバックに
    委ねる）"""

    def test_annual_only_tag_is_skipped_from_q4(self, monkeypatch):
        us_gaap = _base_net_income_and_shares()
        # LITE/APP相当: 年次のみ申告、四半期(10-Q)申告が一切ない
        us_gaap["RevenueFromContractWithCustomerExcludingAssessedTax"] = {
            "units": _units(
                _fact("2023-01-01", "2023-12-31", 400_000_000, "2024-02-15",
                      form="10-K", fp="FY"),
            )
        }
        # 正しい四半期データを持つ「兄弟タグ」（LITEのIncludingAssessedTax・
        # APPのExcludingAssessedTax本体に相当）
        us_gaap["RevenueFromContractWithCustomerIncludingAssessedTax"] = {
            "units": _units(
                _fact("2023-01-01", "2023-03-31", 90_000_000, "2023-04-15"),
                _fact("2023-04-01", "2023-06-30", 95_000_000, "2023-07-15"),
                _fact("2023-07-01", "2023-09-30", 100_000_000, "2023-10-15"),
                _fact("2023-01-01", "2023-12-31", 400_000_000, "2024-02-15",
                      form="10-K", fp="FY"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q4 = next(q for q in quarters if q["end"] == "2023-12-31" and q.get("quarter") == 4)

        assert "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax" not in q4, (
            "四半期申告が皆無のタグは、年次総額のままQ4へ書き込まれてはならない"
        )
        # 正しい兄弟タグは通常通りYTD法/Q1-3合計法で正しく差し引かれる
        # (400,000,000 - (90,000,000+95,000,000+100,000,000) = 115,000,000)
        assert q4["us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax"]["value"] == 115_000_000

    def test_annual_only_tag_with_no_sibling_is_simply_absent(self, monkeypatch):
        """兄弟タグが存在しない場合でも、皆無タグはQ4へ一切書き込まれない
        （年次総額を書き込むよりは「欠損」の方が安全という設計判断の確認）"""
        us_gaap = _base_net_income_and_shares()
        us_gaap["RestructuringCharges"] = {
            "units": _units(
                _fact("2023-01-01", "2023-12-31", 20_000_000, "2024-02-15",
                      form="10-K", fp="FY"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q4 = next(q for q in quarters if q["end"] == "2023-12-31" and q.get("quarter") == 4)

        assert "us-gaap:RestructuringCharges" not in q4


class TestStillAppliesWhenAtLeastOneQuarterPresent:
    """少なくとも1四半期でタグの申告キーが存在する場合は、従来通り
    Q1-3合計法で差し引く（ガード追加による回帰がないことの確認）"""

    def test_q1_3_sum_still_applied_when_one_quarter_has_the_tag(self, monkeypatch):
        us_gaap = _base_net_income_and_shares()
        # Q2のみこのタグの申告がある（Q1・Q3は0円ではなく未申告）想定でも、
        # 1件でも存在すれば差し引き法を適用する
        us_gaap["RestructuringCharges"] = {
            "units": _units(
                _fact("2023-04-01", "2023-06-30", 3_000_000, "2023-07-15"),
                _fact("2023-01-01", "2023-12-31", 20_000_000, "2024-02-15",
                      form="10-K", fp="FY"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q4 = next(q for q in quarters if q["end"] == "2023-12-31" and q.get("quarter") == 4)

        # 20,000,000 - (0 + 3,000,000 + 0) = 17,000,000
        assert q4["us-gaap:RestructuringCharges"]["value"] == 17_000_000


class TestYtdMethodUnaffectedByGuard:
    """YTD(9ヶ月累計)法で解決できる場合は、新設ガードの影響を受けず
    従来通り動作する（ガードはQ1-3合計法フォールバック側のみに限定）"""

    def test_ytd_method_still_preferred(self, monkeypatch):
        us_gaap = _base_net_income_and_shares()
        us_gaap["ShareBasedCompensation"] = {
            "units": _units(
                # 9ヶ月YTD値（Q1開始日〜Q3終了日、200-310日）
                _fact("2023-01-01", "2023-09-30", 30_000_000, "2023-10-15"),
                _fact("2023-01-01", "2023-12-31", 42_000_000, "2024-02-15",
                      form="10-K", fp="FY"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q4 = next(q for q in quarters if q["end"] == "2023-12-31" and q.get("quarter") == 4)

        # 42,000,000 - 30,000,000 = 12,000,000（YTD法、Q1-3合計法ではない）
        assert q4["us-gaap:ShareBasedCompensation"]["value"] == 12_000_000
