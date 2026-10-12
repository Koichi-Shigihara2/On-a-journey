"""
tests/test_eps_cik_history_merge.py

[[SEC-XOM-CIK-HOLDING-REORG-1]]の回帰テスト。

法人再編でCIKが切り替わった銘柄（common/sec_data/cik_history.jsonに登録）について、
EPS ANALYZER（extract_key_facts.py）も旧CIKのCompany Factsを取得して統合すること。
統合はSECデータの層（fetcher.py）と同じ関数（merge_us_gaap_facts: us-gaapのentryを
連結するだけで、どちらを採るかは下流の既存の選び方に任せる）を使う。

実行方法:
    python -m pytest tests/test_eps_cik_history_merge.py -v
"""

import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import src.value.adjusted_eps_analyzer.extract_key_facts as ekf  # noqa: E402
from common.sec_data import fetcher as fetcher_mod  # noqa: E402

NEW_CIK = "0002115436"
OLD_CIK = "0000034088"


def _q(start, end, val, filed, form="10-Q"):
    return {"start": start, "end": end, "val": val, "filed": filed, "form": form}


def _facts(net_income_entries, share_entries):
    """Company Factsのレスポンスの形（facts.us-gaap.{concept}.units.{unit}）"""
    return {"facts": {"us-gaap": {
        "NetIncomeLoss": {"units": {"USD": net_income_entries}},
        "WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": share_entries}},
    }}}


# 新CIK: 2026Q2の10-Qだけ（比較期間の2025Q2を含む）。旧CIK: 2025Q2〜2026Q1の10-Q
NEW_FACTS = _facts(
    [_q("2026-04-01", "2026-06-30", 7_000, "2026-08-03"), _q("2025-04-01", "2025-06-30", 7_100, "2026-08-03")],
    [_q("2026-04-01", "2026-06-30", 4_300e6, "2026-08-03"), _q("2025-04-01", "2025-06-30", 4_401e6, "2026-08-03")],
)
OLD_FACTS = _facts(
    [_q("2025-04-01", "2025-06-30", 7_100, "2025-08-04"), _q("2025-07-01", "2025-09-30", 7_500, "2025-11-03"),
     _q("2026-01-01", "2026-03-31", 8_000, "2026-05-04")],
    [_q("2025-04-01", "2025-06-30", 4_400e6, "2025-08-04"), _q("2025-07-01", "2025-09-30", 4_350e6, "2025-11-03"),
     _q("2026-01-01", "2026-03-31", 4_320e6, "2026-05-04")],
)


def _patch(monkeypatch, history):
    monkeypatch.setattr(ekf, "get_cik", lambda ticker: NEW_CIK)
    fetched = []

    def fake_fetch(cik):
        fetched.append(cik)
        return {NEW_CIK: NEW_FACTS, OLD_CIK: OLD_FACTS}.get(cik, {})

    monkeypatch.setattr(ekf, "fetch_company_facts", fake_fetch)
    monkeypatch.setattr(fetcher_mod, "_load_cik_history", lambda: history)
    monkeypatch.setattr(ekf, "_get_market_data_attributes", lambda ticker: None)
    return fetched


def test_eps_merges_legacy_cik_facts(monkeypatch):
    fetched = _patch(monkeypatch, {"XOM": {"legacy_ciks": ["34088"]}})
    quarters = ekf.extract_quarterly_facts("XOM", years=10)
    ends = sorted(q["end"] for q in quarters)
    assert OLD_CIK in fetched  # 旧CIK（10桁にゼロ埋め）も取得する
    assert ends == ["2025-06-30", "2025-09-30", "2026-03-31", "2026-06-30"]
    by_end = {q["end"]: q for q in quarters}
    assert ekf.normalize_value(by_end["2026-06-30"]["net_income"]) == 7_000  # 新CIKだけにある期
    assert ekf.normalize_value(by_end["2026-03-31"]["net_income"]) == 8_000  # 旧CIKだけにある期
    # 両方にある期の株式数は、既存の規則（select_latest_filed: filedが最新）で新CIKの再掲値（4,401M）
    assert ekf.normalize_value(by_end["2025-06-30"]["diluted_shares"]) == 4_401e6
    assert ekf.normalize_value(by_end["2026-03-31"]["diluted_shares"]) == 4_320e6


def test_eps_without_history_uses_primary_only(monkeypatch):
    fetched = _patch(monkeypatch, {})
    quarters = ekf.extract_quarterly_facts("XOM", years=10)
    assert fetched == [NEW_CIK]
    assert sorted(q["end"] for q in quarters) == ["2025-06-30", "2026-06-30"]


def test_eps_legacy_fetch_failure_does_not_return_truncated_history(monkeypatch):
    """旧CIKが取れないときは、新CIKだけの短い履歴を返さない（失敗としてrun_statusに出す）"""
    _patch(monkeypatch, {"XOM": {"legacy_ciks": ["99999"]}})
    assert ekf.extract_quarterly_facts("XOM", years=10) == []


def test_merge_function_is_shared_with_sec_data_layer():
    """EPS ANALYZERとSECデータの層が同じ統合関数を使う（重複した実装を作らない）"""
    assert ekf.merge_us_gaap_facts is fetcher_mod.merge_us_gaap_facts
    merged = fetcher_mod.merge_us_gaap_facts(NEW_FACTS, OLD_FACTS)
    ni = merged["facts"]["us-gaap"]["NetIncomeLoss"]["units"]["USD"]
    assert len(ni) == 5  # 連結するだけ（重複の除去・優先順位の判定はしない）
    assert fetcher_mod.SECFetcher._merge_us_gaap_facts(NEW_FACTS, OLD_FACTS) == merged


def test_legacy_ciks_for(monkeypatch):
    monkeypatch.setattr(fetcher_mod, "_load_cik_history", lambda: {"XOM": {"legacy_ciks": ["34088"]}})
    assert fetcher_mod.legacy_ciks_for("xom") == ["34088"]
    assert fetcher_mod.legacy_ciks_for("AAPL") == []
