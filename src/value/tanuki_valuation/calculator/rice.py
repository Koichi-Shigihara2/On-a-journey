"""
TANUKI VALUATION - RICE Calculator
RICE = Reinvestment & Compounding Efficiency

定義:
    RICE = (G × Q × CF) / WACC

変数:
    G    : TANUKIシナリオ別forward成長率（bear/base/bull）
    Q    : OCF ÷ 純利益（直近3年平均）キャッシュ転換効率
    CF   : 売上成長率 ÷ 投資強度（1年ラグ・直近3年平均）投資再生産効率
           投資強度 = (R&D + CapEx) / Revenue
    WACC : Rmβなし（市場期待リターン10%固定）※v7.3以降

設計方針:
    - RICEは「投資効率の純粋な測定指標」として割り切る
    - PERに対する相対評価（RICE/PER）で銘柄間比較に使用
    - 総合的な割高・割安判定はRICEを含む複数指標の組み合わせで将来設計（課題）

データソース:
    - annual_{year}.json の cf.operating_cash_flow / pl.net_income
    - annual_{year}.json の pl.revenue / cf.capital_expenditure
    - pl.research_and_development（R&D費）
    - シナリオ成長率: scenario_valuations（core_calculatorが計算済み）
    - WACC: wacc_result.value（core_calculatorが計算済み）

注意:
    - R&Dが annual_{year}.json に存在しない銘柄は CapEx のみで投資強度を計算
    - OCF/純利益が両方ゼロの年は Q 計算から除外
    - CF計算に必要な最低年数: 4年分（1年ラグ + 3点平均）
"""

import os
import sys
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class RICEScenario:
    """シナリオ別RICE結果

    [[RICE-ADJ-ASYMMETRIC-ZERO-1]]対応（2026-09-19）: 測定不能な場合は
    0.0で埋めずNoneとし、理由をrice_na_reason/rice_adj_na_reasonに残す。
    rice: cf<=0（投資再生産効率が構造的に測定不能）の場合はNone。
    rice_adj: cf_adj<=0またはwacc<=0の場合はNone。
    """
    growth_rate: float          # G: forward成長率
    rice: Optional[float]                 # RICE値（測定不能時はNone）
    rice_per_ratio: Optional[float]       # RICE / PER（riceがNoneの場合もNone）
    rice_adj: Optional[float] = None      # RICE_adj: CF_adj（CapExのみ投資強度）使用版（測定不能時はNone）
    rice_na_reason: Optional[str] = None      # riceがNoneの理由（Noneでない場合のみ）
    rice_adj_na_reason: Optional[str] = None  # rice_adjがNoneの理由（Noneでない場合のみ）

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "growth_rate": round(self.growth_rate, 4),
            "rice": round(self.rice, 3) if self.rice is not None else None,
            "rice_per_ratio": round(self.rice_per_ratio, 4) if self.rice_per_ratio is not None else None,
            "rice_adj": round(self.rice_adj, 3) if self.rice_adj is not None else None,
        }
        if self.rice_na_reason:
            d["rice_na_reason"] = self.rice_na_reason
        if self.rice_adj_na_reason:
            d["rice_adj_na_reason"] = self.rice_adj_na_reason
        return d


@dataclass
class RICEResult:
    """RICE計算結果"""
    # 構成要素
    q: float                    # OCF転換効率（直近3年平均）
    cf_conversion: float        # 投資再生産効率（1年ラグ・直近3年平均）
    wacc: float                 # WACC

    # 内訳（デバッグ・表示用）
    q_years: int                # Q計算に使用した年数
    cf_years: int               # CF計算に使用した年数
    avg_intensity: float        # 平均投資強度
    avg_rev_growth: float       # 平均売上成長率（ラグあり）

    # CF_adj（R&D除外・CapExのみ投資強度）
    cf_adj: float = 0.0         # CF_adj = RevGrowth / (CapEx/Revenue) のみ

    # RICE-1: 価値創造係数（Damodaran式 ROIC/WACC）
    vc_factor: Optional[float] = None  # clamp(ROIC/WACC, 0.3, 2.0)。None=未適用
    roic_wacc_ratio: Optional[float] = None  # 元のROIC/WACC比（参照用）

    # シナリオ別RICE
    bear: Optional[RICEScenario] = None
    base: Optional[RICEScenario] = None
    bull: Optional[RICEScenario] = None

    # メタ
    available: bool = True
    note: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "available": self.available,
            "q": round(self.q, 4),
            "cf_conversion": round(self.cf_conversion, 4),
            "cf_adj": round(self.cf_adj, 4),
            "wacc": round(self.wacc, 4),
            "q_years": self.q_years,
            "cf_years": self.cf_years,
            "avg_intensity": round(self.avg_intensity, 4),
            "avg_rev_growth_lagged": round(self.avg_rev_growth, 4),
            "warnings": self.note != "",   # 警告ありフラグ（フロントで表示判定用）
            "note": self.note,
        }
        if self.vc_factor is not None:
            d["vc_factor"] = round(self.vc_factor, 3)
        if self.roic_wacc_ratio is not None:
            d["roic_wacc_ratio"] = round(self.roic_wacc_ratio, 3)
        if self.bear:
            d["bear"] = self.bear.to_dict()
        if self.base:
            d["base"] = self.base.to_dict()
        if self.bull:
            d["bull"] = self.bull.to_dict()
        return d


def _calc_q(annual_data: List[Dict[str, Any]], years: int = 3) -> Tuple[float, int, bool]:
    """
    Q = OCF ÷ (純利益 + SBC)（直近3年平均、SBC補正済み）

    SBC補正: SBCは非現金費用であり純利益を圧縮するため、
    キャッシュ転換効率の分母には純利益+SBCを使用する。
    SBCが取得できない年はSBC=0として扱う（後退互換）。

    スキップ条件（優先順）:
      1. NI < 0: GAAP赤字年はスキップ（SBCで辛うじてプラスになる偽陽性を防ぐ）
         例: NI=-469M, SBC=+608M → earnings=139M だがQ=OCF/139M が巨大化
      2. earnings(NI+SBC) <= 0
      3. earnings < 売上の1%（利益がほぼゼロの年）

    Args:
        annual_data: get_annual_range()の返却値（新しい順）
        years: 計算年数

    Returns:
        (Q値, 使用年数, SBC補正が適用されたか)
    """
    q_list = []
    sbc_applied_any = False
    for data in annual_data[:years]:
        ocf = data.get("cf", {}).get("operating_cash_flow")
        ni  = data.get("pl", {}).get("net_income")
        sbc = data.get("cf", {}).get("stock_based_compensation") or 0.0
        rev = data.get("pl", {}).get("revenue") or 0.0
        if ocf is not None and ni is not None:
            if sbc != 0.0:
                sbc_applied_any = True
            # GAAP赤字年はSBCで偽陽性が生じるため先にスキップ
            if ni < 0:
                continue
            earnings = ni + sbc
            # 利益がほぼゼロ（売上の1%未満）→ Q未定義のためスキップ
            if earnings <= 0:
                continue
            if rev > 0 and abs(earnings) < rev * 0.01:
                continue
            q_list.append(ocf / earnings)

    if not q_list:
        return 0.0, 0, False

    return sum(q_list) / len(q_list), len(q_list), sbc_applied_any


def _calc_cf_lagged(
    annual_data: List[Dict[str, Any]], years: int = 3
) -> Tuple[float, float, int, float, float, List[str]]:
    """
    CF = 売上成長率(t+1) ÷ 投資強度(t)（1年ラグ・直近N点平均）

    annual_data は新しい順（annual_data[0]が最新）なので
    インデックスの扱いに注意:
        t   = annual_data[i+1]  （古い年）
        t+1 = annual_data[i]    （新しい年）

    Args:
        annual_data: get_annual_range()の返却値（新しい順）
        years: 使用するラグペアの数（最低3点）

    Returns:
        (CF値, CF_adj値, 使用年数, 平均投資強度, 平均売上成長率, 警告リスト)
        CF     : R&D+CapEx+SM ベースの投資強度で計算（既存）
        CF_adj : CapEx のみの投資強度で計算（R&D資本化想定）

    異常値検出:
        - R&Dフィールドが存在しない年がある場合 → 警告
        - 投資強度 < 1% の年がある場合 → 警告（R&D未取得の可能性）
        - CF点が50超の場合 → 警告（非現実的な値）
    """
    # 異常値検出の閾値
    INTENSITY_MIN  = 0.01   # 投資強度の最低ライン（1%）
    CF_POINT_MAX   = 50.0   # CF点の警告閾値
    CF_POINT_CLAMP = 10.0   # CF点のクリップ上限（異常値による理論株価への影響を防ぐ）

    cf_list         = []
    cf_adj_list     = []   # CapEx のみの投資強度ベース
    intensity_list  = []
    rev_growth_list = []
    warnings: List[str] = []

    # R&D欠損チェック（計算対象年のいずれかでNoneなら警告）
    rd_missing_years = []
    for i in range(min(years, len(annual_data) - 1)):
        older = annual_data[i + 1]
        rd = older.get("pl", {}).get("research_and_development")
        fy = older.get("period", "?")
        if rd is None:
            rd_missing_years.append(str(fy))

    if rd_missing_years:
        warnings.append(
            f"R&D未取得（FY{', '.join(rd_missing_years)}）: "
            f"parser.pyにResearchAndDevelopmentExpenseタグ追加を検討"
        )

    # SM欠損チェック（販売・マーケティング費）
    sm_missing_years = []
    for i in range(min(years, len(annual_data) - 1)):
        older = annual_data[i + 1]
        sm = older.get("pl", {}).get("selling_and_marketing")
        fy = older.get("period", "?")
        if sm is None:
            sm_missing_years.append(str(fy))

    if sm_missing_years:
        warnings.append(
            f"販売・マーケティング費未取得（FY{', '.join(sm_missing_years)}）: "
            f"投資強度が過小になる可能性"
        )

    # SGA整合性チェック: parser.pyが annual JSON に記録した data_quality 警告を伝播
    sga_gap_years = []
    for i in range(min(years, len(annual_data) - 1)):
        older = annual_data[i + 1]
        dq = older.get("data_quality", {})
        if dq.get("sga_gap_warning"):
            fy       = older.get("period", "?")
            gap_pct  = round(dq.get("gap_ratio", 0) * 100)
            sga_gap_years.append(f"FY{fy}({gap_pct}%)")
    if sga_gap_years:
        warnings.append(
            f"SGA取得漏れ疑い（{', '.join(sga_gap_years)}）: "
            f"投資強度が過小の可能性。company_factsを再取得して確認してください"
        )

    # annual_data[0]=最新, annual_data[1]=1年前, ...
    # ペア: (t=data[i+1], t+1=data[i]) で i=0,1,...
    for i in range(min(years, len(annual_data) - 1)):
        newer = annual_data[i]      # t+1年
        older = annual_data[i + 1]  # t年

        rev_t   = older.get("pl", {}).get("revenue")
        capex_t = older.get("cf", {}).get("capital_expenditure")
        rd_t    = older.get("pl", {}).get("research_and_development")
        sm_t    = older.get("pl", {}).get("selling_and_marketing")
        fy_t    = older.get("period", "?")

        if rev_t is None or rev_t == 0 or capex_t is None:
            continue

        invest_t = (abs(capex_t)
                    + (abs(rd_t) if rd_t is not None else 0.0)
                    + (abs(sm_t) if sm_t is not None else 0.0))
        intensity_t = invest_t / rev_t

        # 投資強度が異常に低い場合（R&D未取得の典型症状）
        if intensity_t < INTENSITY_MIN:
            warnings.append(
                f"投資強度が低すぎる（FY{fy_t}: {intensity_t:.3%}）: "
                f"R&D未取得の可能性。CF値が過大になる場合がある"
            )

        rev_t1 = newer.get("pl", {}).get("revenue")
        if rev_t1 is None or rev_t == 0:
            continue

        rev_growth_t1 = (rev_t1 - rev_t) / abs(rev_t)

        if intensity_t == 0:
            continue

        cf_point = rev_growth_t1 / intensity_t

        # CF点が非現実的に大きい場合 → 警告＋クリップ
        if abs(cf_point) > CF_POINT_MAX:
            warnings.append(
                f"CF点が異常値（FY{fy_t}→: {cf_point:.1f}）: "
                f"R&D未取得による投資強度過小の可能性"
            )
        # CF点をクリップ（異常値が平均を歪めるのを防ぐ）
        cf_point = max(-CF_POINT_CLAMP, min(CF_POINT_CLAMP, cf_point))

        cf_list.append(cf_point)
        intensity_list.append(intensity_t)
        rev_growth_list.append(rev_growth_t1)

        # ── CF_adj: CapEx のみの投資強度で計算（R&D を資本として扱う想定）──
        intensity_adj_t = abs(capex_t) / rev_t
        if intensity_adj_t > 0:
            cf_point_adj = rev_growth_t1 / intensity_adj_t
            cf_point_adj = max(-CF_POINT_CLAMP, min(CF_POINT_CLAMP, cf_point_adj))
            cf_adj_list.append(cf_point_adj)

    if not cf_list:
        return 0.0, 0.0, 0, 0.0, 0.0, warnings

    cf_avg     = sum(cf_list) / len(cf_list)
    cf_adj_avg = sum(cf_adj_list) / len(cf_adj_list) if cf_adj_list else 0.0
    intensity_avg  = sum(intensity_list) / len(intensity_list)
    rev_growth_avg = sum(rev_growth_list) / len(rev_growth_list)

    return cf_avg, cf_adj_avg, len(cf_list), intensity_avg, rev_growth_avg, warnings


# RICEを構造的に計算できないセクター（投資強度の定義が成立しない）
RICE_EXCLUDED_SECTORS = {
    "Insurance",           # 保険会社: R&D/CapExがほぼゼロ・FCFがフロート
    "Consumer Defensive",  # 飲食・日用品: R&D=0が多くCF膨張
    "Consumer Cyclical",   # 外食・小売: 同上（CAKE等）
    "Financial Services",  # 銀行・金融: 投資強度の定義が異なる
    "Real Estate",         # REIT等: CapEx性質が異なる
    "Utilities",           # 公益: 設備投資の性質が異なる
}

# industryベースの除外（sectorがHealthcareでも保険会社はRICE除外）
RICE_EXCLUDED_INDUSTRIES = {
    "Healthcare Plans",    # 保険会社はsector=Healthcareだが構造は保険
}


def calculate_rice(
    annual_data: List[Dict[str, Any]],
    wacc: float,
    scenario_valuations: Optional[Dict[str, Any]],
    current_per: float = 0.0,
    q_years: int = 3,
    cf_years: int = 3,
    sector: str = "",
    industry: str = "",
    roic_wacc_ratio: Optional[float] = None,
) -> RICEResult:
    """
    RICE計算メイン関数

    Args:
        annual_data:          get_annual_range()の返却値（新しい順、最低4年分推奨）
        wacc:                 TANUKI VALUATIONが計算したWACC
        scenario_valuations:  latest.jsonの scenario_valuations（bear/base/bull成長率）
        current_per:          現在のPER（RICE/PER計算用、0の場合は比率計算をスキップ）
        q_years:              Q計算年数（デフォルト3）
        cf_years:             CF計算年数（デフォルト3）
        sector:               yfinanceセクター名（除外判定用）

    Returns:
        RICEResult
    """
    # ── セクター・業種除外チェック ──
    if sector and sector in RICE_EXCLUDED_SECTORS:
        return RICEResult(
            q=0.0, cf_conversion=0.0, wacc=wacc,
            q_years=0, cf_years=0,
            avg_intensity=0.0, avg_rev_growth=0.0,
            available=False,
            note=f"セクター除外（{sector}）: 投資強度の定義が成立しないため計算対象外"
        )
    if industry and industry in RICE_EXCLUDED_INDUSTRIES:
        return RICEResult(
            q=0.0, cf_conversion=0.0, wacc=wacc,
            q_years=0, cf_years=0,
            avg_intensity=0.0, avg_rev_growth=0.0,
            available=False,
            note=f"業種除外（{industry}）: 保険会社はFCF構造が異なるため計算対象外"
        )

    # ── データ不足チェック ──
    if len(annual_data) < 2:
        return RICEResult(
            q=0.0, cf_conversion=0.0, wacc=wacc,
            q_years=0, cf_years=0,
            avg_intensity=0.0, avg_rev_growth=0.0,
            available=False,
            note="年次データ不足（最低2年分必要）"
        )

    # ── Q計算 ──
    q, q_used, sbc_adjusted = _calc_q(annual_data, q_years)
    if q_used == 0:
        return RICEResult(
            q=0.0, cf_conversion=0.0, wacc=wacc,
            q_years=0, cf_years=0,
            avg_intensity=0.0, avg_rev_growth=0.0,
            available=False,
            note="Q計算不可（OCF/純利益データなし）"
        )

    # ── Q異常値ガード ──
    # |Q| > 5.0 は保険会社のフロート・赤字急拡大企業等で発生する構造的異常
    Q_MAX = 5.0
    if abs(q) > Q_MAX:
        return RICEResult(
            q=q, cf_conversion=0.0, wacc=wacc,
            q_years=q_used, cf_years=0,
            avg_intensity=0.0, avg_rev_growth=0.0,
            available=False,
            note=f"Q異常値（Q={q:.2f}）: 保険会社フロートまたは赤字急拡大企業。RICE計算除外"
        )

    # ── CF計算（1年ラグ） ──
    cf, cf_adj, cf_used, avg_intensity, avg_rev_growth, cf_warnings = _calc_cf_lagged(annual_data, cf_years)
    if cf_used == 0:
        return RICEResult(
            q=q, cf_conversion=0.0, wacc=wacc,
            q_years=q_used, cf_years=0,
            avg_intensity=0.0, avg_rev_growth=0.0,
            available=False,
            note="CF計算不可（Revenue/CapExデータなし）"
        )

    # ── RICE-1: 価値創造係数（Damodaran式 ROIC/WACC）──
    # ROIC > WACC: 再投資が価値を創造 → G に正の倍率（上限2.0）
    # ROIC < WACC: 再投資が価値を毀損 → G を割引（下限0.3）
    # roic_wacc_ratio=None: 従来通り（後退互換）
    if roic_wacc_ratio is not None:
        vc_factor = max(0.3, min(roic_wacc_ratio, 2.0))
    else:
        vc_factor = 1.0

    # ── シナリオ別RICE計算 ──
    scenarios: Dict[str, Optional[RICEScenario]] = {
        "bear": None, "base": None, "bull": None
    }

    if scenario_valuations and wacc > 0:
        for sc_name in ("bear", "base", "bull"):
            sc = scenario_valuations.get(sc_name)
            if sc is None:
                continue
            g = sc.get("growth_rate")
            if g is None:
                continue

            # [[RICE-ADJ-ASYMMETRIC-ZERO-1]]対応（2026-09-19）: cf<=0の場合、
            # riceは符号反転した無意味な値を返すのではなくNone（測定不能）とする。
            if cf > 0:
                rice_val = (g * vc_factor * q * cf) / wacc
                rice_na_reason = None
            else:
                rice_val = None
                # 理由文言の分岐（2026-09-19仕上げ対応）: guard導入前の生値
                # （g×vc_factor×q×cf、wacc>0固定のため符号に無関係）の符号で
                # 2パターンに分ける。
                # ①raw<0（qが正常でcfのみ負の通常ケース）: 旧実装でも既に
                #   負値を返し「N/A (OCF赤字)」という固定ラベルで表示されて
                #   いたため、その表示文言（意味論）をそのまま踏襲する
                # ②raw>=0（qも負でcfとの符号相殺により旧実装が見かけ上
                #   プラスの値を返していたケース、SPIR等）: cfが負である
                #   ことそのものが測定不能の原因である旨を明示する
                #   （「OCF赤字」ラベルは実態〈符号相殺で見えなくなっていた
                #   問題〉と食い違うため使わない）
                _raw_would_be = g * vc_factor * q * cf
                if _raw_would_be < 0:
                    rice_na_reason = "OCF赤字"
                else:
                    rice_na_reason = f"cf={cf:.3f}のためCFがマイナスで投資再生産効率が測定不能"

            if rice_val is not None:
                ratio = rice_val / current_per if current_per > 0 else 0.0
            else:
                ratio = None

            # rice_adjはcf_adj<=0またはwacc<=0（wacc<=0はこのif節自体
            # 〈scenario_valuations and wacc > 0〉で既に除外済みのため
            # 通常到達しないが、ガード条件の意図を明示するため維持）の場合None。
            if cf_adj > 0 and wacc > 0:
                rice_adj_val = (g * vc_factor * q * cf_adj) / wacc
                rice_adj_na_reason = None
            else:
                rice_adj_val = None
                if cf_adj <= 0 and wacc <= 0:
                    rice_adj_na_reason = f"cf_adj<=0 かつ wacc<=0（cf_adj={cf_adj:.3f}, wacc={wacc:.4f}）"
                elif cf_adj <= 0:
                    rice_adj_na_reason = f"cf_adj<=0（CapExのみ投資強度が測定不能、cf_adj={cf_adj:.3f}）"
                else:
                    rice_adj_na_reason = f"wacc<=0（wacc={wacc:.4f}）"

            scenarios[sc_name] = RICEScenario(
                growth_rate=g,
                rice=rice_val,
                rice_per_ratio=ratio,
                rice_adj=rice_adj_val,
                rice_na_reason=rice_na_reason,
                rice_adj_na_reason=rice_adj_na_reason,
            )

    # ── ノート生成（データ不足 + 異常値警告を統合） ──
    note_parts = []
    if sbc_adjusted:
        note_parts.append("SBC補正済み（純利益+SBCを分母に使用）")
    if q_used < q_years:
        note_parts.append(f"Q: {q_used}年のみ使用（{q_years}年要求）")
    if cf_used < cf_years:
        note_parts.append(f"CF: {cf_used}点のみ使用（{cf_years}点要求）")
    if roic_wacc_ratio is not None:
        note_parts.append(f"RICE-1価値創造係数={vc_factor:.2f}(ROIC/WACC={roic_wacc_ratio:.2f})")
    note_parts.extend(cf_warnings)
    note = " / ".join(note_parts) if note_parts else ""

    return RICEResult(
        q=q,
        cf_conversion=cf,
        cf_adj=cf_adj,
        wacc=wacc,
        q_years=q_used,
        cf_years=cf_used,
        avg_intensity=avg_intensity,
        avg_rev_growth=avg_rev_growth,
        vc_factor=vc_factor if roic_wacc_ratio is not None else None,
        roic_wacc_ratio=roic_wacc_ratio,
        bear=scenarios["bear"],
        base=scenarios["base"],
        bull=scenarios["bull"],
        available=True,
        note=note,
    )


if __name__ == "__main__":
    # 簡易テスト（annual_dataのモックで動作確認）
    mock_annual = [
        # FY2026（最新）
        {"pl": {"revenue": 215_938_000_000, "net_income": 72_880_000_000, "research_and_development": 12_910_000_000},
         "cf": {"operating_cash_flow": 96_000_000_000, "capital_expenditure": -1_330_000_000}},
        # FY2025
        {"pl": {"revenue": 130_497_000_000, "net_income": 29_760_000_000, "research_and_development": 8_680_000_000},
         "cf": {"operating_cash_flow": 64_090_000_000, "capital_expenditure": -1_070_000_000}},
        # FY2024
        {"pl": {"revenue": 60_922_000_000, "net_income": 4_370_000_000, "research_and_development": 5_870_000_000},
         "cf": {"operating_cash_flow": 28_080_000_000, "capital_expenditure": -700_000_000}},
        # FY2023
        {"pl": {"revenue": 26_974_000_000, "net_income": 4_370_000_000, "research_and_development": 3_970_000_000},
         "cf": {"operating_cash_flow": 5_640_000_000, "capital_expenditure": -600_000_000}},
    ]

    mock_scenarios = {
        "bear": {"growth_rate": 0.227, "intrinsic_value_per_share": 284.21},
        "base": {"growth_rate": 0.325, "intrinsic_value_per_share": 408.67},
        "bull": {"growth_rate": 0.390, "intrinsic_value_per_share": 514.03},
    }

    result = calculate_rice(
        annual_data=mock_annual,
        wacc=0.10285,
        scenario_valuations=mock_scenarios,
        current_per=35.0,
    )

    import json
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
