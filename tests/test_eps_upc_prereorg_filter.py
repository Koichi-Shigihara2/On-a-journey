"""
tests/test_eps_upc_prereorg_filter.py

[[EPS-UPC-PREREORG-1]]の回帰テスト。

Up-C構造・組織再編前四半期（BROS 2021 Q1/Q2等）では、SEC申告の
NetIncomeLoss（PubCo帰属分）が正確に0となる一方、実際の事業収益は
存在する（revenue>0）という特異なパターンを示す。この場合、
gaap_net_income(0) + 税効果調整後SBC等の加算により、見かけ上プラスの
Adjusted EPSが算出されてしまうが、これは実態のない会計上のアーティ
ファクトである（一次情報確認済み: BROSの`ProfitLoss`タグ〈非支配持分
込みの連結損益〉は同四半期で実額の非ゼロ値を持つ。「NetIncomeLoss=0
だがProfitLoss非ゼロ」という乖離自体がUp-C特有のPubCo/OpCo分離会計の
証跡であり、本テストのモック値はBROSの実データ〈company_facts.json〉
から直接転記している）。

- apply_upc_prereorg_filter(): net_income正確に0かつrevenue>0の四半期に
  special_flags=["UPC_PREREORG_ZERO_PROFIT"]を付与する（削除はしない、
  監査可能性維持）
- calculate_ttm(): フラグ付き四半期を1件でも含む窓はNoneを返す
  （[[EPS-LOAR-1]]のSHARE_STRUCTURE_MISMATCHと同型の除外機構を共有）
- aggregate_annual(): フラグ付き四半期を年度集計の対象から除外する
  （除外後4四半期未満の年度は既存ロジックで自然にスキップされる）
- generate_summary(): summary.jsonのlatest/YoY計算からフラグ付き
  四半期を除外する

実行方法:
    python -m pytest tests/test_eps_upc_prereorg_filter.py -v
"""

import os
import sys

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import src.value.adjusted_eps_analyzer.pipeline as aea_pipeline  # noqa: E402


def _make_quarter(filing_date, gaap_net_income, revenue, diluted_shares=90_000_000,
                   adjusted_eps=0.1, net_adjustment_total=0.0, fiscal_year=None, quarter=None):
    return {
        "filing_date": filing_date,
        "period_end": filing_date,
        "gaap_net_income": gaap_net_income,
        "gaap_eps": gaap_net_income / diluted_shares if diluted_shares else 0.0,
        "revenue": revenue,
        "diluted_shares": diluted_shares,
        "diluted_shares_used": diluted_shares,
        "adjusted_eps": adjusted_eps,
        "net_adjustment_total": net_adjustment_total,
        "fiscal_year": fiscal_year or int(filing_date[:4]),
        "quarter": quarter,
        "special_flags": [],
        "special_notes": {},
        "adjustments": [],
    }


class TestApplyUpcPrereorgFilter:
    def test_bros_actual_2021_quarters_are_flagged(self):
        """BROS実データ（common/sec_data/data/BROS/company_facts.json、
        NetIncomeLoss=0確認済み）相当のQ1・Q2 2021が検出されること。
        値はBROSの一次情報（SEC EDGAR company_facts.json）から転記した
        実データであり、検知条件と同型の誤りを持つ自己整合的なモックでは
        ない"""
        quarters = [
            _make_quarter("2021-03-31", 0.0, 98_785_000, fiscal_year=2021, quarter=1),
            _make_quarter("2021-06-30", 0.0, 129_208_000, fiscal_year=2021, quarter=2),
            _make_quarter("2021-09-30", -6_738_000, 129_803_000, fiscal_year=2021, quarter=3),
            _make_quarter("2021-12-31", -6_176_000, 140_080_000, fiscal_year=2021, quarter=4),
        ]
        result = aea_pipeline.apply_upc_prereorg_filter("BROS", quarters)

        flagged = [q for q in result if "UPC_PREREORG_ZERO_PROFIT" in q["special_flags"]]
        unflagged = [q for q in result if "UPC_PREREORG_ZERO_PROFIT" not in q["special_flags"]]
        assert {q["filing_date"] for q in flagged} == {"2021-03-31", "2021-06-30"}
        assert len(unflagged) == 2

    def test_flagged_quarter_carries_explanatory_note(self):
        quarters = [_make_quarter("2021-03-31", 0.0, 98_785_000)]
        result = aea_pipeline.apply_upc_prereorg_filter("BROS", quarters)
        assert "upc_prereorg_zero_profit" in result[0]["special_notes"]
        assert "98,785,000" in result[0]["special_notes"]["upc_prereorg_zero_profit"]

    def test_cart_is_not_flagged(self):
        """CART実データ（company_facts.json確認済み）相当: NetIncomeLossは
        対象四半期で一度も正確な0にならないため、検知条件に一致しない
        （[[EPS-UPC-PREREORG-1]]Phase1調査で確認済みの前提の回帰確認）"""
        quarters = [
            _make_quarter("2022-03-31", -82_000_000, 0.0),   # revenue抽出ギャップ（別課題）
            _make_quarter("2022-06-30", 8_000_000, 0.0),
            _make_quarter("2022-09-30", 36_000_000, 668_000_000),
        ]
        result = aea_pipeline.apply_upc_prereorg_filter("CART", quarters)
        assert all(q["special_flags"] == [] for q in result)

    def test_zero_net_income_with_zero_revenue_is_not_flagged(self):
        """revenue<=0の場合は検知条件を満たさない（net_income=0だけでは
        不十分、事業実在の証跡としてrevenue>0を要求する設計の確認）"""
        quarters = [_make_quarter("2020-01-01", 0.0, 0.0)]
        result = aea_pipeline.apply_upc_prereorg_filter("TEST", quarters)
        assert result[0]["special_flags"] == []

    def test_near_zero_net_income_is_not_flagged(self):
        """端数のある僅少net_income（$1000等）は「正確に0」条件を満たさず
        検知対象外（意図的に厳格なフィルタである設計の確認）"""
        quarters = [_make_quarter("2020-01-01", 1_000.0, 50_000_000)]
        result = aea_pipeline.apply_upc_prereorg_filter("TEST", quarters)
        assert result[0]["special_flags"] == []

    def test_empty_is_noop(self):
        assert aea_pipeline.apply_upc_prereorg_filter("TEST", []) == []


class TestCalculateTtmSkipsUpcFlaggedWindows:
    def test_window_containing_flagged_quarter_returns_none(self):
        quarters = [
            _make_quarter("2021-03-31", 0.0, 98_785_000),
            _make_quarter("2021-06-30", 0.0, 129_208_000),
            _make_quarter("2021-09-30", -6_738_000, 129_803_000),
            _make_quarter("2021-12-31", -6_176_000, 140_080_000),
        ]
        quarters[0]["special_flags"] = ["UPC_PREREORG_ZERO_PROFIT"]
        quarters[1]["special_flags"] = ["UPC_PREREORG_ZERO_PROFIT"]
        assert aea_pipeline.calculate_ttm(quarters, 3) is None

    def test_window_without_flagged_quarter_computes_normally(self):
        quarters = [
            _make_quarter("2025-03-31", 5_000_000, 95_000_000),
            _make_quarter("2025-06-30", 5_100_000, 95_100_000),
            _make_quarter("2025-09-30", 5_200_000, 95_200_000),
            _make_quarter("2025-12-31", 5_300_000, 95_300_000),
        ]
        ttm = aea_pipeline.calculate_ttm(quarters, 3)
        assert ttm is not None
        assert ttm["net_income"] == sum(q["gaap_net_income"] for q in quarters)


class TestAggregateAnnualExcludesUpcFlaggedQuarters:
    def test_bros_fy2021_short_of_four_is_skipped(self):
        """BROS FY2021相当: Q1/Q2がフラグ済みのため実質2四半期しか残らず、
        年度集計自体がスキップされること"""
        quarters = [
            _make_quarter("2021-03-31", 0.0, 98_785_000, fiscal_year=2021),
            _make_quarter("2021-06-30", 0.0, 129_208_000, fiscal_year=2021),
            _make_quarter("2021-09-30", -6_738_000, 129_803_000, fiscal_year=2021),
            _make_quarter("2021-12-31", -6_176_000, 140_080_000, fiscal_year=2021),
        ]
        quarters[0]["special_flags"] = ["UPC_PREREORG_ZERO_PROFIT"]
        quarters[1]["special_flags"] = ["UPC_PREREORG_ZERO_PROFIT"]
        result = aea_pipeline.aggregate_annual(quarters)
        assert result == []

    def test_clean_year_is_unaffected(self):
        quarters = [_make_quarter(f"2025-{m:02d}-30", 5_000_000, 95_000_000, fiscal_year=2025)
                    for m in [3, 6, 9, 12]]
        result = aea_pipeline.aggregate_annual(quarters)
        assert len(result) == 1
        assert result[0]["year"] == "2025"


class TestGenerateSummaryExcludesUpcFlaggedLatest:
    def test_latest_flagged_quarter_is_skipped_for_summary(self):
        """quarters[0]（最新）がフラグ済みの場合、summary.jsonのlatestには
        その次の非フラグ四半期が採用されること（[[EPS-LOAR-1]]の
        SHARE_STRUCTURE_MISMATCH向け既存防御的措置と同型の確認）"""
        quarters = [
            _make_quarter("2021-06-30", 0.0, 129_208_000, adjusted_eps=5.0),
            _make_quarter("2021-03-31", 0.0, 98_785_000, adjusted_eps=3.0),
            _make_quarter("2020-12-31", -1_000_000, 90_000_000, adjusted_eps=-0.05),
        ]
        quarters[0]["special_flags"] = ["UPC_PREREORG_ZERO_PROFIT"]
        quarters[1]["special_flags"] = ["UPC_PREREORG_ZERO_PROFIT"]
        tickers_data = {"BROS": {"quarters": quarters, "company_name": "Dutch Bros"}}
        summary = aea_pipeline.generate_summary(tickers_data)
        entry = next(t for t in summary["tickers"] if t["ticker"] == "BROS")
        assert entry["latest_filing_date"] == "2020-12-31"
        assert entry["adjusted_eps"] == -0.05
