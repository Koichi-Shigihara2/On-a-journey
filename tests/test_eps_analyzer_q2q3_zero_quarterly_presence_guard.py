"""
tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py

[[CART-QUARTERLY-REVENUE-EXTRACTION-GAP-1]]横展開調査で発見した、
extract_key_facts.pyの「Q2/Q3 YTD差分補完」ループ（881〜937行目付近）の
バグに対する回帰テスト。

[[EPS-LITE-ANNUAL-AS-QUARTERLY-1]]で修正済みの「Q4に年次の調整項目タグを
追加」ループ（772〜880行目）と全く同型の欠陥が、Q2/Q3向けの兄弟ループに
残存していた。「該当タグの四半期(10-Q)申告キーが対象四半期に一件も
存在しない」場合と「申告はあるが実際に0円」を区別できず、前者を後者と
誤認してq1_val/q2_valを0とみなし、YTD累計値を丸ごと単体四半期の値として
誤って書き込んでいた。

IONQ実例（company_facts.json確認済み）: `us-gaap:Revenues`タグが
2021-01-01〜2021-09-30の9ヶ月累計として$1,070,000,000という異常値
（IonQの当時の事業規模から見て不自然、既存[[XBRL-UNIT-SCALE-MISMATCH-
DETECTION-1]]にIONQが該当銘柄として記録済み）を申告している一方、
Q1・Q2単体のRevenuesタグ申告は皆無だった。修正前は
`Q3 = ytd_9m − 0 − 0 = $1,070,000,000`という値がQ3 2021単体の
revenueとしてそのまま書き込まれていた。

本テストは、Q1・Q2いずれのタグ申告も一件も存在しない場合はQ3への
YTD_9m直接減算によるフォールバックをスキップすること（Q2側もQ1申告が
皆無ならスキップすること）、および少なくとも1四半期に申告がある場合や
YTD_6m経由の差分（申告値同士の差分のためガード不要）は従来通り動作する
ことを検証する。

実行方法:
    python -m pytest tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py -v
"""

import os
import sys
from datetime import datetime, timedelta

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import src.value.adjusted_eps_analyzer.extract_key_facts as ekf  # noqa: E402

# [[CART-QUARTERLY-REVENUE-EXTRACTION-GAP-1]]段階2で日付ベースの
# min_end_date窓（years×365.25日ちょうど、バッファなし）を導入したため、
# 固定の過去年（"2021"等）をフィクスチャに使うとテスト実行時点によって
# 窓の外に出てしまう。テスト実行時から2年前を基準年とし、以降も
# 十分な余裕（years=5指定に対し2年前）を持たせる。
_FY = (datetime.now() - timedelta(days=2 * 365)).year


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
    """Q1〜Q3のnet_income/diluted_sharesを一律で用意する
    （Q2/Q3キー生成に必要。Q4/年次は本ループの対象外なので省略）"""
    return {
        "NetIncomeLoss": {
            "units": _units(
                _fact(f"{_FY}-01-01", f"{_FY}-03-31", -2_000_000, f"{_FY}-05-15"),
                _fact(f"{_FY}-04-01", f"{_FY}-06-30", -8_000_000, f"{_FY}-08-15"),
                _fact(f"{_FY}-07-01", f"{_FY}-09-30", -15_000_000, f"{_FY}-11-15"),
            )
        },
        "WeightedAverageNumberOfDilutedSharesOutstanding": {
            "units": _units(
                _fact(f"{_FY}-01-01", f"{_FY}-03-31", 119_000_000, f"{_FY}-05-15"),
                _fact(f"{_FY}-04-01", f"{_FY}-06-30", 119_000_000, f"{_FY}-08-15"),
                _fact(f"{_FY}-07-01", f"{_FY}-09-30", 120_000_000, f"{_FY}-11-15"),
                unit="shares",
            )
        },
    }


class TestSkipQ3WhenNeitherQ1NorQ2HasTheTag:
    """IONQ実例相当: Q1・Q2いずれもこのタグの四半期申告が皆無で
    YTD_9mのみ存在する場合、Q3への書き込み自体をスキップする"""

    def test_ytd9m_only_tag_is_skipped_from_q3(self, monkeypatch):
        us_gaap = _base_net_income_and_shares()
        # IONQ相当: Q1・Q2単体の申告は一切なく、9ヶ月累計のみ存在
        us_gaap["Revenues"] = {
            "units": _units(
                _fact(f"{_FY}-01-01", f"{_FY}-09-30", 1_070_000_000, f"{_FY}-11-15"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q3 = next(q for q in quarters if q["end"] == f"{_FY}-09-30" and q.get("quarter") == 3)

        assert "us-gaap:Revenues" not in q3, (
            "Q1・Q2にこのタグの申告が皆無の場合、YTD_9m全額がQ3単体として"
            "書き込まれてはならない"
        )

    def test_ytd9m_only_tag_with_partial_q1_is_applied(self, monkeypatch):
        """Q1にのみ申告がある場合は従来通りQ1-2直接減算法を適用する
        （ガード追加による回帰がないことの確認）"""
        us_gaap = _base_net_income_and_shares()
        us_gaap["Revenues"] = {
            "units": _units(
                _fact(f"{_FY}-01-01", f"{_FY}-03-31", 100_000_000, f"{_FY}-05-15"),
                _fact(f"{_FY}-01-01", f"{_FY}-09-30", 1_070_000_000, f"{_FY}-11-15"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q3 = next(q for q in quarters if q["end"] == f"{_FY}-09-30" and q.get("quarter") == 3)

        # 1,070,000,000 - 100,000,000 - 0(Q2未申告) = 970,000,000
        assert q3["us-gaap:Revenues"]["value"] == 970_000_000


class TestSkipQ2WhenQ1HasNoTheTag:
    """Q1にこのタグの申告が皆無でYTD_6mのみ存在する場合、
    Q2への書き込み自体をスキップする"""

    def test_ytd6m_only_tag_is_skipped_from_q2(self, monkeypatch):
        us_gaap = _base_net_income_and_shares()
        us_gaap["Revenues"] = {
            "units": _units(
                _fact(f"{_FY}-01-01", f"{_FY}-06-30", 500_000_000, f"{_FY}-08-15"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q2 = next(q for q in quarters if q["end"] == f"{_FY}-06-30" and q.get("quarter") == 2)

        assert "us-gaap:Revenues" not in q2


class TestYtd6mSubtractionUnaffectedByGuard:
    """Q3をYTD_9m − YTD_6mで解決できる場合（申告値同士の差分）は、
    Q1・Q2の個別申告有無に関わらず新設ガードの影響を受けない"""

    def test_ytd6m_minus_ytd9m_still_applied_without_q1_q2(self, monkeypatch):
        us_gaap = _base_net_income_and_shares()
        us_gaap["Revenues"] = {
            "units": _units(
                _fact(f"{_FY}-01-01", f"{_FY}-06-30", 500_000_000, f"{_FY}-08-15"),
                _fact(f"{_FY}-01-01", f"{_FY}-09-30", 800_000_000, f"{_FY}-11-15"),
            )
        }
        _patch_common(monkeypatch, {"facts": {"us-gaap": us_gaap}})

        quarters = ekf.extract_quarterly_facts("TESTCO", years=5)
        q3 = next(q for q in quarters if q["end"] == f"{_FY}-09-30" and q.get("quarter") == 3)

        # 800,000,000 - 500,000,000 = 300,000,000（申告値同士の差分、ガード対象外）
        assert q3["us-gaap:Revenues"]["value"] == 300_000_000
