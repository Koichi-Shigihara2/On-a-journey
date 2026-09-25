# discover/stonks-silo/src/analyzer.py
"""
Stonks Silo Analyzer
3本柱の計算ロジック。

① 良い赤字 vs 悪い赤字
   → 売上成長率 × R&D・S&M比率で「攻めの赤字」か「沈みゆく赤字」かを判定

② 生存能力（Runway）
   → 現金 ÷ 月次バーン（OCF + CapEx）で何ヶ月戦えるか

③ 隠れ黒字化の標準フォーマット
   → R&D・S&M除外後の本業利益
   → OCF改善の角度（速度・加速度）
   → 素直な黒字化 / 隠れ黒字化の時期予測
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# discover/stonks-silo/src/ → repo root は3階層上（financial_trend_calculator.pyと同型）
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from common.sec_data.reader import get_runway_cash  # noqa: E402


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class DeficitQuality:
    """① 良い赤字 vs 悪い赤字"""

    # 入力サマリー（最新年）
    latest_year: int
    revenue: Optional[float]
    net_income: Optional[float]

    # 成長率系列 (year → YoY成長率 %)
    revenue_growth_pct: dict[int, Optional[float]] = field(default_factory=dict)
    cagr_3yr: Optional[float] = None          # 3年CAGR (%)

    # コスト比率（対Revenue）最新年
    rnd_ratio: Optional[float] = None         # R&D / Revenue
    sm_ratio: Optional[float] = None          # S&M / Revenue
    gross_margin: Optional[float] = None      # GrossProfit / Revenue
    gross_margin_derived: bool = False        # True = revenue-cost から逆算

    # 総合判定
    verdict: str = "UNKNOWN"                  # GOOD_DEFICIT / BAD_DEFICIT / PROFITABLE / UNKNOWN
    verdict_reason: str = ""
    score: Optional[float] = None             # 0-100 (高いほど「攻めの赤字」)

    # 追加指標
    # [[RULE40-DEFINITION-MISMATCH-1]]: rule_of_40 → rule40_cagr3y_opmarginへ
    # 改名（2026-08-13）。旧コメント「売上成長率」は実装（3年CAGR）と
    # 食い違っていたため、実装に合わせて修正（NAMING_CONVENTIONS.md規則2）。
    rule40_cagr3y_opmargin: Optional[float] = None  # 3年CAGR + 営業利益率
    mature_profit: Optional[float] = None     # 純利益 + R&D + SM
    sbc_adjusted_fcf: Optional[float] = None  # FCF - SBC
    sbc_ratio: Optional[float] = None         # SBC ÷ 売上
    sbc_yoy_change: Optional[float] = None    # SBC前年比変化率(%)
    dilution_risk: str = "UNKNOWN"            # HIGH/MEDIUM/LOW/UNKNOWN
    deficit_fixed_risk: str = "MEDIUM"        # HIGH/MEDIUM/LOW
    mature_profit_note: str = ""              # 投資除外後も赤字の場合に注記
    revenue_outlier_years: list[int] = field(default_factory=list)  # 外れ値除去された年

    # ユニットエコノミクス（DESIGN-11）
    gross_margin_trend: Optional[str] = None      # "improving"/"stable"/"declining"
    gross_margin_note: str = ""                    # "construction_phase" など
    unit_economics_score: Optional[float] = None   # 0-100
    unit_economics_label: Optional[str] = None     # "優秀"/"良好"/"低調"



@dataclass
class RunwayAnalysis:
    """② 生存能力"""

    latest_year: int
    cash: Optional[float]                     # 現金+短期投資
    monthly_burn: Optional[float]             # 月次バーン (負値 = 流出)
    runway_months: Optional[float]            # 生存可能月数
    ocf_annual: Optional[float]
    capex_annual: Optional[float]

    verdict: str = "UNKNOWN"                  # SAFE / WATCH / DANGER / UNKNOWN
    verdict_reason: str = ""
    score: Optional[float] = None             # 0-100


@dataclass
class ProfitabilityPath:
    """③ 隠れ黒字化パス"""

    # R&D・S&M除外後の本業利益（各年）
    core_profit: dict[int, Optional[float]] = field(default_factory=dict)  # OCF + RnD + SM

    # OCF改善速度・加速度（年次）
    ocf_annual: dict[int, Optional[float]] = field(default_factory=dict)
    ocf_yoy_change: dict[int, Optional[float]] = field(default_factory=dict)    # 速度
    ocf_acceleration: dict[int, Optional[float]] = field(default_factory=dict)  # 加速度

    # OCF改善トレンドの向き
    ocf_trend: str = "UNKNOWN"   # ACCELERATING / IMPROVING / FLAT / DETERIORATING

    # 黒字化予測
    gaap_breakeven_year: Optional[int] = None     # GAAP純利益ベース
    gaap_breakeven_reason: str = ""               # ACHIEVED/PREDICTED/NO_TREND/NO_DATA/TOO_FAR
    ocf_breakeven_year: Optional[int] = None      # OCFベース（隠れ黒字化）
    ocf_breakeven_reason: str = ""                # ACHIEVED/PREDICTED/NO_TREND/NO_DATA/TOO_FAR
    hidden_profit_already: bool = False            # OCFが既に黒字かどうか

    verdict_reason: str = ""
    score: Optional[float] = None             # 0-100

    # 非連続成長フラグ
    discontinuous_growth: bool = False
    discontinuous_growth_note: str = ""

    # 拡大再生産指標
    incremental_margin: Optional[float] = None
    incremental_margin_prev: Optional[float] = None
    incremental_margin_trend: str = "UNKNOWN"
    incremental_rev_delta: Optional[float] = None
    incremental_gp_delta: Optional[float] = None
    reproduction_score: int = 0
    reproduction_label: str = "不明"


@dataclass
class StonksAnalysis:
    """3本柱まとめ"""

    ticker: str
    years: list[int]

    deficit_quality: DeficitQuality
    runway: RunwayAnalysis
    profitability_path: ProfitabilityPath

    # 総合スコア（0-100）
    overall_score: Optional[float] = None
    overall_verdict: str = "UNKNOWN"
    summary: str = ""


# ---------------------------------------------------------------------------
# メイン計算クラス
# ---------------------------------------------------------------------------

class StonksAnalyzer:

    def analyze(self, data: dict, net_cash_data: Optional[dict] = None) -> StonksAnalysis:
        """
        fetcher.load_annual_data() の戻り値を受け取り、StonksAnalysis を返す。

        net_cash_data: SECReader.get_net_cash()の返却値。渡された場合、
        Runwayのcashは四半期優先の値を使う（_analyze_runway()参照）。
        """
        ticker = data["ticker"]
        years = data["years"]
        records = data["records"]

        dq = self._analyze_deficit_quality(years, records)
        ra = self._analyze_runway(years, records, net_cash_data)
        pp = self._analyze_profitability_path(years, records)
        dq.deficit_fixed_risk = self._calc_deficit_fixed_risk(dq, pp)

        overall_score, overall_verdict = self._overall(dq, ra, pp)
        summary = self._build_summary(ticker, dq, ra, pp, overall_verdict, years, records)

        return StonksAnalysis(
            ticker=ticker,
            years=years,
            deficit_quality=dq,
            runway=ra,
            profitability_path=pp,
            overall_score=overall_score,
            overall_verdict=overall_verdict,
            summary=summary,
        )

    # ------------------------------------------------------------------
    # ① 良い赤字 vs 悪い赤字
    # ------------------------------------------------------------------

    def _analyze_deficit_quality(self, years: list[int], records: dict) -> DeficitQuality:
        latest_year = years[-1]
        latest = records[latest_year]
        pl = latest["pl"]

        revenue = pl.get("revenue")                      # 生データ（表示・参照用）
        revenue_san = pl.get("revenue_sanitized")         # サニタイズ済み（計算用）
        net_income = pl.get("net_income")

        # 外れ値除去された年を収集
        revenue_outlier_years = [
            yr for yr in years
            if records[yr]["pl"].get("revenue_is_outlier", False)
        ]

        # 売上成長率（YoY）— サニタイズ済み値を使用
        rev_growth: dict[int, Optional[float]] = {}
        revenues = {}
        for yr in years:
            rev = records[yr]["pl"].get("revenue_sanitized")
            revenues[yr] = rev

        for i, yr in enumerate(years):
            if i == 0:
                rev_growth[yr] = None
                continue
            prev_yr = years[i - 1]
            curr = revenues[yr]
            prev = revenues[prev_yr]
            if curr is not None and prev and prev > 0:
                rev_growth[yr] = (curr / prev - 1) * 100
            else:
                rev_growth[yr] = None

        # 3年CAGR
        cagr_3yr = None
        if len(years) >= 4:
            y_end = years[-1]
            y_start = years[-4]
            r_end = revenues.get(y_end)
            r_start = revenues.get(y_start)
            if r_end and r_start and r_start > 0:
                cagr_3yr = ((r_end / r_start) ** (1 / 3) - 1) * 100

        # 最新年コスト比率（サニタイズ済み売上を使用）
        rnd_ratio = sm_ratio = gross_margin = None
        gross_margin_derived = False
        if revenue_san and revenue_san > 0:
            rnd = pl.get("research_and_development")
            sm = pl.get("selling_and_marketing")
            gp = pl.get("gross_profit")
            if rnd is not None:
                rnd_ratio = rnd / revenue_san * 100
            if sm is not None:
                sm_ratio = sm / revenue_san * 100
            if gp is not None:
                gross_margin = gp / revenue_san * 100
                gross_margin_derived = bool(pl.get("gross_profit_derived", False))

        # 判定ロジック
        verdict, reason, score = self._deficit_verdict(
            net_income, cagr_3yr, rev_growth, rnd_ratio, sm_ratio, gross_margin
        )

        # Rule of 40（3年CAGR + 営業利益率）
        rule40_cagr3y_opmargin = None
        if revenue_san and revenue_san > 0 and cagr_3yr is not None:
            operating_income = pl.get("operating_income")
            if operating_income is not None:
                rule40_cagr3y_opmargin = round(cagr_3yr + operating_income / revenue_san * 100, 1)

        # 成熟想定利益（純利益 + R&D + SM）
        mature_profit = None
        mature_profit_note = ""
        if net_income is not None:
            _rnd = pl.get("research_and_development")
            _sm = pl.get("selling_and_marketing")
            mature_profit = net_income + (_rnd or 0) + (_sm or 0)
            if mature_profit < 0:
                mature_profit_note = "投資除外後も赤字"

        # SBC 関連
        cf = latest["cf"]
        sbc = cf.get("stock_based_compensation")
        fcf = cf.get("free_cash_flow")
        sbc_adjusted_fcf = (fcf - sbc) if fcf is not None and sbc is not None else None
        sbc_ratio_val = (sbc / revenue_san * 100) if sbc is not None and revenue_san and revenue_san > 0 else None

        # SBC YoY 変化
        sbc_yoy_change = None
        if len(years) >= 2 and sbc is not None:
            prev_sbc = records[years[-2]]["cf"].get("stock_based_compensation")
            if prev_sbc is not None and prev_sbc != 0:
                sbc_yoy_change = (sbc - prev_sbc) / abs(prev_sbc) * 100

        # 希薄化リスク
        if sbc_ratio_val is None:
            dilution_risk = "UNKNOWN"
        elif sbc_ratio_val >= 15:
            dilution_risk = "HIGH"
        elif sbc_ratio_val >= 8:
            dilution_risk = "MEDIUM"
        else:
            dilution_risk = "LOW"

        # ── DESIGN-11: Unit Economics ──
        # 粗利率系列（複数年）
        gm_series: dict[int, Optional[float]] = {}
        for yr in years:
            yr_pl = records[yr]["pl"]
            yr_rev = yr_pl.get("revenue_sanitized")
            yr_gp  = yr_pl.get("gross_profit")
            if yr_rev and yr_rev > 0 and yr_gp is not None:
                gm_series[yr] = yr_gp / yr_rev * 100
            else:
                gm_series[yr] = None

        # 粗利率トレンド（直近3年の先頭→末尾の変化量）
        gm_vals = [gm_series[yr] for yr in years[-3:] if gm_series.get(yr) is not None]
        if len(gm_vals) >= 2:
            gm_delta = gm_vals[-1] - gm_vals[0]
            gross_margin_trend: Optional[str] = (
                "improving" if gm_delta > 2.0 else ("declining" if gm_delta < -2.0 else "stable")
            )
        else:
            gross_margin_trend = None

        # 粗利率が取得できない銘柄は建設フェーズ扱い
        gross_margin_note = "construction_phase" if gross_margin is None else ""

        # LPR（損失/売上比率）
        def _lpr(ni: Optional[float], rev: Optional[float]) -> Optional[float]:
            if ni is None or rev is None or rev <= 0:
                return None
            return abs(min(ni, 0.0)) / rev * 100  # 黒字なら 0%

        lpr_latest = _lpr(net_income, revenue_san)
        lpr_prev: Optional[float] = None
        if len(years) >= 2:
            _prev_pl = records[years[-2]]["pl"]
            lpr_prev = _lpr(_prev_pl.get("net_income"), _prev_pl.get("revenue_sanitized"))

        # Unit Economics スコア（gross_margin が null の場合はスキップ）
        ue_score: Optional[float] = None
        ue_label: Optional[str] = None
        if gross_margin is not None:
            s = 0.0
            # GM 水準 (20pt)
            if gross_margin >= 50:
                s += 20.0
            elif gross_margin >= 30:
                s += 10.0
            # GM トレンド (20pt)
            if gross_margin_trend == "improving":
                s += 20.0
            elif gross_margin_trend == "stable":
                s += 10.0
            # LPR 水準 (30pt)
            if lpr_latest is None:
                s += 15.0
            elif lpr_latest <= 10.0:
                s += 30.0
            elif lpr_latest <= 30.0:
                s += 15.0
            # LPR 改善 (30pt)
            if lpr_prev is None or lpr_latest is None:
                s += 15.0
            elif lpr_latest == 0.0 and lpr_prev == 0.0:
                s += 30.0  # 連続黒字
            elif lpr_prev > 0.0:
                improvement = (lpr_prev - lpr_latest) / lpr_prev * 100.0
                if improvement >= 30.0:
                    s += 30.0
                elif improvement >= 10.0:
                    s += 15.0
            ue_score = round(s, 1)
            ue_label = "優秀" if s >= 70.0 else ("良好" if s >= 40.0 else "低調")

        return DeficitQuality(
            latest_year=latest_year,
            revenue=revenue,
            net_income=net_income,
            revenue_growth_pct=rev_growth,
            cagr_3yr=cagr_3yr,
            rnd_ratio=rnd_ratio,
            sm_ratio=sm_ratio,
            gross_margin=gross_margin,
            gross_margin_derived=gross_margin_derived,
            verdict=verdict,
            verdict_reason=reason,
            score=score,
            rule40_cagr3y_opmargin=rule40_cagr3y_opmargin,
            mature_profit=mature_profit,
            mature_profit_note=mature_profit_note,
            sbc_adjusted_fcf=sbc_adjusted_fcf,
            sbc_ratio=sbc_ratio_val,
            sbc_yoy_change=sbc_yoy_change,
            dilution_risk=dilution_risk,
            revenue_outlier_years=revenue_outlier_years,
            gross_margin_trend=gross_margin_trend,
            gross_margin_note=gross_margin_note,
            unit_economics_score=ue_score,
            unit_economics_label=ue_label,
        )

    def _deficit_verdict(
        self,
        net_income: Optional[float],
        cagr_3yr: Optional[float],
        rev_growth: dict,
        rnd_ratio: Optional[float],
        sm_ratio: Optional[float],
        gross_margin: Optional[float],
    ) -> tuple[str, str, float]:
        """
        総合判定。スコアリング方式で0-100点を算出。

        スコア構成:
          売上成長  (40pt): CAGR >50%=40, >30%=30, >20%=20, >10%=10, else=0
          投資姿勢  (30pt): R&D+SM >60%=30, >40%=20, >20%=10, else=5
          粗利率    (20pt): >70%=20, >50%=15, >30%=8,  else=0
          赤字状況  (10pt): 純赤字=5 (赤字でも投資中), 純黒字=10
        """
        is_profitable = net_income is not None and net_income > 0

        score = 0.0
        reasons = []

        # 売上成長 (40pt)
        if cagr_3yr is not None:
            if cagr_3yr >= 50:
                score += 40; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (超高成長)")
            elif cagr_3yr >= 30:
                score += 30; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (高成長)")
            elif cagr_3yr >= 20:
                score += 20; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (成長中)")
            elif cagr_3yr >= 10:
                score += 10; reasons.append(f"売上CAGR +{cagr_3yr:.0f}% (緩成長)")
            else:
                reasons.append(f"売上CAGR {cagr_3yr:.0f}% (成長鈍化)")
        else:
            reasons.append("成長率計算不可")

        # 投資姿勢 (30pt) — R&D + S&M 合計対Revenue
        invest_ratio = 0.0
        if rnd_ratio is not None:
            invest_ratio += rnd_ratio
        if sm_ratio is not None:
            invest_ratio += sm_ratio
        if invest_ratio >= 60:
            score += 30; reasons.append(f"R&D+SM {invest_ratio:.0f}% (積極投資)")
        elif invest_ratio >= 40:
            score += 20; reasons.append(f"R&D+SM {invest_ratio:.0f}% (投資中)")
        elif invest_ratio >= 20:
            score += 10; reasons.append(f"R&D+SM {invest_ratio:.0f}% (標準)")
        elif invest_ratio > 0:
            score += 5; reasons.append(f"R&D+SM {invest_ratio:.0f}% (低投資)")
        else:
            reasons.append("投資比率計算不可")

        # 粗利率 (20pt)
        if gross_margin is not None:
            if gross_margin >= 70:
                score += 20; reasons.append(f"粗利率 {gross_margin:.0f}% (高収益構造)")
            elif gross_margin >= 50:
                score += 15; reasons.append(f"粗利率 {gross_margin:.0f}% (良好)")
            elif gross_margin >= 30:
                score += 8;  reasons.append(f"粗利率 {gross_margin:.0f}% (普通)")
            else:
                reasons.append(f"粗利率 {gross_margin:.0f}% (低い)")
        else:
            reasons.append("粗利率計算不可")

        # 黒字状況 (10pt)
        if is_profitable:
            score += 10; reasons.append("純利益黒字")
        elif net_income is not None:
            score += 5

        # 最終判定
        if is_profitable:
            verdict = "PROFITABLE"
        elif score >= 65:
            verdict = "GOOD_DEFICIT"
        elif score >= 35:
            verdict = "WATCH"
        else:
            verdict = "BAD_DEFICIT"

        return verdict, " / ".join(reasons), round(score, 1)

    # ------------------------------------------------------------------
    def _calc_deficit_fixed_risk(self, dq: DeficitQuality, pp: ProfitabilityPath) -> str:
        if dq.verdict == "GOOD_DEFICIT":
            if pp.ocf_trend in ("ACCELERATING", "IMPROVING"):
                return "LOW"
            return "MEDIUM"
        if dq.verdict == "BAD_DEFICIT" and pp.ocf_trend == "DETERIORATING":
            return "HIGH"
        return "MEDIUM"

    # ② 生存能力（Runway）
    # ------------------------------------------------------------------

    def _analyze_runway(
        self, years: list[int], records: dict, net_cash_data: Optional[dict] = None
    ) -> RunwayAnalysis:
        # [[BBAI-RDW-RUNWAY-VERIFICATION-1]]（2026-09-25）: cashは
        # common/sec_data/reader.py::get_runway_cash()（TANUKI VALUATIONの
        # computed_runway_monthsと共通）で算出する。get_net_cash()の四半期
        # 優先ロジックで確定したcash_and_equivalents + short_term_investments。
        # 2026-09-19時点では直近年次annual_{yr}.jsonのみを参照し「TANUKIとは
        # 統一しない」としていたが、一次情報（10-Q、2026-06-30時点）で
        # RDWは年次決算後のH1増資$566.2Mによりcash $557.0M（H1 FCF -$48.0M）
        # と実態SAFEであり、年次cash $94.5Mで算出したDANGER判定が誤りと
        # 確認したため四半期優先へ切り替えた。
        # 非流動AFS（満期1年超）は含めない（get_runway_cash()参照）。
        # net_cash_dataがない（単体呼び出し等）かcashを取得できなかった
        # 場合のみ、従来通り直近年次のBSにフォールバックする。
        latest_year = years[-1]
        latest = records[latest_year]
        bs = latest["bs"]
        cf = latest["cf"]

        cash = get_runway_cash(net_cash_data)
        if cash is None:
            cash = _sum_not_none(
                bs.get("cash_and_equivalents"),
                bs.get("short_term_investments"),
            )
        ocf = cf.get("operating_cash_flow")
        capex = cf.get("capital_expenditure")

        # 月次バーン = (OCF - |CapEx|) / 12
        # CapEx は正・負どちらで格納されていても abs() で正規化して差し引く
        monthly_burn = None
        if ocf is not None and capex is not None:
            annual_burn = ocf - abs(capex)
            monthly_burn = annual_burn / 12
        elif ocf is not None:
            monthly_burn = ocf / 12

        # Runway 計算
        runway_months = None
        if cash is not None and monthly_burn is not None:
            if monthly_burn >= 0:
                # 現金が増えている or トントン → Runway 無限大扱い
                runway_months = float("inf")
            else:
                runway_months = cash / abs(monthly_burn)

        # 判定
        verdict, reason = self._runway_verdict(runway_months, monthly_burn, cash)

        return RunwayAnalysis(
            latest_year=latest_year,
            cash=cash,
            monthly_burn=monthly_burn,
            runway_months=runway_months,
            ocf_annual=ocf,
            capex_annual=capex,
            verdict=verdict,
            verdict_reason=reason,
        )

    def _runway_verdict(
        self,
        runway_months: Optional[float],
        monthly_burn: Optional[float],
        cash: Optional[float],
    ) -> tuple[str, str]:
        if runway_months is None:
            return "UNKNOWN", "データ不足"
        if runway_months == float("inf") or (monthly_burn is not None and monthly_burn >= 0):
            return "SAFE", "キャッシュフロー黒字またはトントン"
        if runway_months >= 24:
            return "SAFE", f"Runway {runway_months:.0f}ヶ月（2年超）"
        if runway_months >= 12:
            return "WATCH", f"Runway {runway_months:.0f}ヶ月（1-2年）要注意"
        return "DANGER", f"Runway {runway_months:.0f}ヶ月（1年未満）危険"

    # ------------------------------------------------------------------
    # ③ 隠れ黒字化パス
    # ------------------------------------------------------------------

    def _analyze_profitability_path(self, years: list[int], records: dict) -> ProfitabilityPath:
        # OCF 年次
        ocf_annual = {}
        for yr in years:
            ocf_annual[yr] = records[yr]["cf"].get("operating_cash_flow")

        # コア利益 = OCF + R&D + S&M（投資的コストを足し戻した本業キャッシュ）
        # ※ R&D・SM は P/L 上の費用。OCF はその後の現金収支なので厳密には別軸だが、
        #   「投資費用を除いた収益力」の代理指標として利用する。
        core_profit = {}
        for yr in years:
            pl = records[yr]["pl"]
            ocf = ocf_annual[yr]
            rnd = pl.get("research_and_development")
            sm = pl.get("selling_and_marketing")
            if ocf is not None:
                add_back = (rnd or 0) + (sm or 0)
                core_profit[yr] = ocf + add_back
            else:
                core_profit[yr] = None

        # OCF YoY 変化（速度）
        ocf_yoy: dict[int, Optional[float]] = {}
        for i, yr in enumerate(years):
            if i == 0:
                ocf_yoy[yr] = None
                continue
            prev_yr = years[i - 1]
            curr = ocf_annual[yr]
            prev = ocf_annual[prev_yr]
            if curr is not None and prev is not None:
                ocf_yoy[yr] = curr - prev
            else:
                ocf_yoy[yr] = None

        # OCF YoY 加速度（2階微分）
        ocf_accel: dict[int, Optional[float]] = {}
        for i, yr in enumerate(years):
            if i < 2:
                ocf_accel[yr] = None
                continue
            prev_yr = years[i - 1]
            curr_v = ocf_yoy[yr]
            prev_v = ocf_yoy[prev_yr]
            if curr_v is not None and prev_v is not None:
                ocf_accel[yr] = curr_v - prev_v
            else:
                ocf_accel[yr] = None

        # OCF トレンド判定
        ocf_trend = self._ocf_trend(years, ocf_annual, ocf_yoy, ocf_accel)

        # 黒字化予測
        gaap_be, ocf_be, hidden_already, gaap_reason, ocf_reason, reason, any_predicted = self._breakeven_estimate(
            years, records, ocf_annual, ocf_trend
        )

        # 非連続成長の検出（実際に将来年を予測〈PREDICTED/IMMINENT〉した
        # 場合のみ、その予測の精度への影響を確認する）
        discontinuous_growth = False
        discontinuous_growth_note = ""
        if any_predicted and len(years) >= 2:
            latest_yr = years[-1]
            prev_yr = years[-2]
            r_latest = records[latest_yr]["pl"].get("revenue_sanitized")
            r_prev = records[prev_yr]["pl"].get("revenue_sanitized")
            if r_latest and r_prev and r_prev > 0:
                latest_yoy = (r_latest / r_prev - 1) * 100
                if latest_yoy >= 200:
                    past_yoys = []
                    for i in range(max(1, len(years) - 4), len(years) - 1):
                        rc = records[years[i]]["pl"].get("revenue_sanitized")
                        rp = records[years[i - 1]]["pl"].get("revenue_sanitized")
                        if rc and rp and rp > 0:
                            past_yoys.append((rc / rp - 1) * 100)
                    if past_yoys:
                        avg_past = sum(past_yoys) / len(past_yoys)
                        if avg_past > 0 and latest_yoy / avg_past > 3:
                            discontinuous_growth = True
                            discontinuous_growth_note = f"直近売上が急拡大（+{latest_yoy:.0f}%）、予測精度が低下している可能性があります"

        # 増分粗利率計算
        inc_results = _calc_incremental_margin(years, records)
        incremental_margin = None
        incremental_margin_prev = None
        incremental_margin_trend = "UNKNOWN"
        incremental_rev_delta = None
        incremental_gp_delta = None
        reproduction_score = 0
        reproduction_label = "不明"

        if inc_results:
            _, im0, rd0, gd0 = inc_results[0]
            incremental_margin = im0
            incremental_rev_delta = rd0
            incremental_gp_delta = gd0

            if len(inc_results) >= 2:
                _, im1, _, _ = inc_results[1]
                incremental_margin_prev = im1
                # OLS trend: x = 年次インデックス（古い順に 0,1,2...）、y = 増分粗利率
                pts_ols = list(reversed(inc_results))  # 古い順
                n_ols = len(pts_ols)
                xs = list(range(n_ols))
                ys = [im for _, im, _, _ in pts_ols]
                xm = sum(xs) / n_ols
                ym = sum(ys) / n_ols
                ss_xx = sum((x - xm) ** 2 for x in xs)
                if ss_xx > 0:
                    slope_im = sum((xs[j] - xm) * (ys[j] - ym) for j in range(n_ols)) / ss_xx
                    if slope_im > 5:
                        incremental_margin_trend = "IMPROVING"
                    elif slope_im < -5:
                        incremental_margin_trend = "DETERIORATING"
                    else:
                        incremental_margin_trend = "FLAT"
                else:
                    incremental_margin_trend = "FLAT"
            elif len(inc_results) == 1:
                incremental_margin_trend = "UNKNOWN"

            if incremental_margin >= 70:
                base_score = 4
            elif incremental_margin >= 50:
                base_score = 3
            elif incremental_margin >= 30:
                base_score = 2
            elif incremental_margin >= 0:
                base_score = 1
            else:
                base_score = 0

            reproduction_score = min(4, base_score + (0.5 if incremental_margin_trend == "IMPROVING" else 0))

            if reproduction_score >= 3.5:
                reproduction_label = "極めて強い拡大再生産"
            elif reproduction_score >= 2.5:
                reproduction_label = "拡大再生産の兆候あり"
            elif reproduction_score >= 1.5:
                reproduction_label = "限定的な拡大再生産"
            elif reproduction_score >= 0.5:
                reproduction_label = "拡大再生産は弱い"
            else:
                reproduction_label = "拡大再生産なし"
        else:
            reproduction_label = "データ不足"

        return ProfitabilityPath(
            core_profit=core_profit,
            ocf_annual=ocf_annual,
            ocf_yoy_change=ocf_yoy,
            ocf_acceleration=ocf_accel,
            ocf_trend=ocf_trend,
            gaap_breakeven_year=gaap_be,
            gaap_breakeven_reason=gaap_reason,
            ocf_breakeven_year=ocf_be,
            ocf_breakeven_reason=ocf_reason,
            hidden_profit_already=hidden_already,
            verdict_reason=reason,
            discontinuous_growth=discontinuous_growth,
            discontinuous_growth_note=discontinuous_growth_note,
            incremental_margin=incremental_margin,
            incremental_margin_prev=incremental_margin_prev,
            incremental_margin_trend=incremental_margin_trend,
            incremental_rev_delta=incremental_rev_delta,
            incremental_gp_delta=incremental_gp_delta,
            reproduction_score=reproduction_score,
            reproduction_label=reproduction_label,
        )

    def _ocf_trend(
        self,
        years: list[int],
        ocf_annual: dict,
        ocf_yoy: dict,
        ocf_accel: dict,
    ) -> str:
        """直近2年の速度と加速度からトレンドを判定"""
        if len(years) < 2:
            return "UNKNOWN"

        # 直近2年の YoY（新しい順）
        recent_yoys = [
            ocf_yoy[yr] for yr in years[-2:]
            if ocf_yoy.get(yr) is not None
        ]
        if not recent_yoys:
            return "UNKNOWN"

        latest_yoy = recent_yoys[-1]  # 最新年のYoY

        # 加速度（直近）
        latest_accel = None
        for yr in reversed(years):
            if ocf_accel.get(yr) is not None:
                latest_accel = ocf_accel[yr]
                break

        latest_ocf = ocf_annual.get(years[-1])

        # 最新年YoYがマイナス → 悪化
        if latest_yoy <= 0:
            if latest_ocf is not None and latest_ocf > 0:
                return "FLAT"  # 黒字維持だが改善なし
            return "DETERIORATING"

        # 最新年YoYがプラス かつ 加速度もプラス → 加速中
        if latest_accel is not None and latest_accel > 0:
            return "ACCELERATING"

        # 最新年YoYがプラス かつ 直近2年とも改善 → 改善中
        if len(recent_yoys) == 2 and recent_yoys[0] > 0:
            return "IMPROVING"

        # 最新年YoYはプラスだが直前年はマイナス（混在）→ FLAT
        return "FLAT"

    def _breakeven_estimate(
        self,
        years: list[int],
        records: dict,
        ocf_annual: dict,
        ocf_trend: str,
    ) -> tuple[Optional[int], Optional[int], bool, str, str, str, bool]:
        """
        常に直近4年のマージン比率OLS回帰で黒字化年を予測する
        （[[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]、2026-09-05統一）。
        Returns: (gaap_be_year, ocf_be_year, hidden_already, gaap_reason, ocf_reason, combined_reason, any_predicted)
        reason codes: ACHIEVED / PREDICTED / IMMINENT / NO_TREND[:XX%→XX%] / NO_DATA / TOO_FAR
        any_predicted: GAAP・OCFいずれかがPREDICTED/IMMINENT（実際に将来年を
        算出）した場合のみTrue。非連続成長チェックのゲートに使う
        （旧ols_used、絶対値OLSフォールバックの廃止に伴い意味を変更）。
        """
        net_incomes = {yr: records[yr]["pl"].get("net_income") for yr in years}
        latest_ni = net_incomes.get(years[-1])
        predicted_gaap = False
        if latest_ni is not None and latest_ni > 0:
            gaap_be = None
            gaap_reason = "ACHIEVED"
        else:
            gaap_be, gaap_reason, predicted_gaap = _gaap_margin_breakeven(years, net_incomes, records)

        latest_ocf = ocf_annual.get(years[-1])
        hidden_already = latest_ocf is not None and latest_ocf > 0
        predicted_ocf = False
        if hidden_already:
            ocf_be = None
            ocf_reason = "ACHIEVED"
        else:
            ocf_be, ocf_reason, predicted_ocf = _margin_breakeven(years, ocf_annual, records)

        if ocf_trend == "DETERIORATING" and gaap_reason not in ("ACHIEVED", "PREDICTED", "IMMINENT"):
            gaap_be = None
            gaap_reason = "NO_TREND"

        any_predicted = predicted_gaap or predicted_ocf
        combined = f"{gaap_reason} | {ocf_reason}"
        return gaap_be, ocf_be, hidden_already, gaap_reason, ocf_reason, combined, any_predicted

    # ------------------------------------------------------------------
    # 総合スコア・サマリー
    # ------------------------------------------------------------------

    def _overall(
        self, dq: DeficitQuality, ra: RunwayAnalysis, pp: ProfitabilityPath
    ) -> tuple[Optional[float], str]:
        """
        3本柱を統合したスコア。
        - 赤字品質  40%
        - 生存能力  30%
        - 黒字化パス 30%
        """
        weights = {"deficit": 0.4, "runway": 0.3, "path": 0.3}

        # 赤字品質スコア (0-100)
        dq_score = dq.score or 0

        # 生存能力スコア (0-100)
        runway_map = {"SAFE": 100, "WATCH": 60, "DANGER": 20, "UNKNOWN": 50}
        ra_score = runway_map.get(ra.verdict, 0)

        # 黒字化パス (0-100)
        trend_map = {
            "ACCELERATING": 100,
            "IMPROVING": 75,
            "FLAT": 50,
            "DETERIORATING": 20,
            "UNKNOWN": 0,
        }
        path_score = trend_map.get(pp.ocf_trend, 0)
        if pp.hidden_profit_already:
            path_score = max(path_score, 80)

        ra.score = float(ra_score)
        pp.score = float(path_score)

        overall = (
            dq_score * weights["deficit"]
            + ra_score * weights["runway"]
            + path_score * weights["path"]
        )

        if overall >= 75:
            verdict = "10x_CANDIDATE"
        elif overall >= 55:
            verdict = "PROMISING"
        elif overall >= 35:
            verdict = "WATCH"
        else:
            verdict = "AVOID"

        return round(overall, 1), verdict

    def _build_summary(
        self,
        ticker: str,
        dq: DeficitQuality,
        ra: RunwayAnalysis,
        pp: ProfitabilityPath,
        overall_verdict: str,
        years: list[int] | None = None,
        records: dict | None = None,
    ) -> str:
        verdict_label_map = {
            "GOOD_DEFICIT": "高成長×積極投資の好例",
            "WATCH":        "成長・投資のバランス要確認",
            "BAD_DEFICIT":  "採算性に懸念",
            "PROFITABLE":   "黒字転換済み",
        }
        runway_label_map = {
            "SAFE":   "極めて安全水準",
            "WATCH":  "要注意水準",
            "DANGER": "危険水準",
        }
        path_label_map = {
            "ACCELERATING":  "明確な道筋",
            "IMPROVING":     "改善中",
            "FLAT":          "横ばい",
            "DETERIORATING": "悪化中",
        }
        trend_ja_map = {
            "ACCELERATING":  "加速中",
            "IMPROVING":     "改善中",
            "FLAT":          "横ばい",
            "DETERIORATING": "悪化中",
            "UNKNOWN":       "不明",
        }

        def cagr_cmt(v):
            if v is None: return "計算不可"
            if v >= 50: return "極めて強い成長エンジン"
            if v >= 30: return "高い成長力"
            if v >= 20: return "安定した成長"
            if v >= 10: return "緩やかな成長"
            return "成長鈍化"

        def invest_cmt(v):
            if v >= 60: return "将来への戦略的投資を継続"
            if v >= 40: return "積極的な投資姿勢"
            if v >= 20: return "標準的な投資水準"
            return "投資水準は低め"

        def gm_cmt(v):
            if v is None: return "データなし"
            if v >= 70: return "高収益ビジネスモデル"
            if v >= 50: return "良好な収益構造"
            if v >= 30: return "改善余地はあるが安定基盤"
            return "収益構造の改善が課題"

        def fmt_b(v):
            if v is None: return "N/A"
            if abs(v) >= 1e9: return f"${v/1e9:.1f}B"
            if abs(v) >= 1e6: return f"${v/1e6:.0f}M"
            return f"${v:,.0f}"

        # 投資比率
        invest_no_data = dq.rnd_ratio is None and dq.sm_ratio is None
        invest_ratio = (dq.rnd_ratio or 0.0) + (dq.sm_ratio or 0.0)

        # 純利益マージン改善（直近2年）— abs(margin)≤1000% かつ 売上10%以上の点のみ有効
        ni_margin_line = None
        if years and records and len(years) >= 2:
            latest_rev_s = records.get(years[-1], {}).get("pl", {}).get("revenue_sanitized")
            margins = []
            for yr in years[-2:]:
                ni = records.get(yr, {}).get("pl", {}).get("net_income")
                rev = records.get(yr, {}).get("pl", {}).get("revenue_sanitized")
                if ni is not None and rev and rev > 0:
                    if latest_rev_s is None or rev >= latest_rev_s * 0.10:
                        m = ni / rev * 100
                        if abs(m) <= 1000:
                            margins.append(m)
            if len(margins) == 2:
                imp = margins[1] - margins[0]
                ni_margin_line = f"純利益マージン改善: {margins[0]:.1f}%→{margins[1]:.1f}%（{imp:+.1f}pt/年）"

        # ランウェイ
        if ra.monthly_burn is not None and ra.monthly_burn >= 0:
            runway_detail = "∞（CF黒字）"
        elif ra.runway_months == float("inf"):
            runway_detail = "∞（CF黒字）"
        elif ra.runway_months is not None:
            runway_detail = f"{ra.runway_months:.0f}ヶ月確保"
        else:
            runway_detail = "算出不可"

        # OCF黒字化テキスト
        if pp.hidden_profit_already:
            ocf_be_text = "営業CF黒字：達成済み"
        elif pp.ocf_breakeven_year:
            ocf_be_text = f"営業CF黒字化予測：{pp.ocf_breakeven_year}年頃"
        else:
            m = {"NO_TREND": "黒字化予測不可（悪化中）", "NO_DATA": "データ不足", "TOO_FAR": "5年超"}
            ocf_be_text = f"営業CF黒字化：{m.get(pp.ocf_breakeven_reason, pp.ocf_breakeven_reason or '不明')}"

        # GAAP黒字化テキスト
        if pp.gaap_breakeven_reason == "ACHIEVED":
            gaap_be_text = "純利益黒字：達成済み"
        elif pp.gaap_breakeven_year:
            gaap_be_text = f"純利益黒字化予測：{pp.gaap_breakeven_year}年頃"
        else:
            m = {"NO_TREND": "純利益黒字化予測不可", "NO_DATA": "純利益データ不足", "TOO_FAR": "純利益黒字化：5年超"}
            gaap_be_text = m.get(pp.gaap_breakeven_reason, f"純利益：{pp.gaap_breakeven_reason or '不明'}")

        verdict_label = verdict_label_map.get(dq.verdict, dq.verdict)
        runway_label  = runway_label_map.get(ra.verdict, ra.verdict)
        path_label    = path_label_map.get(pp.ocf_trend, pp.ocf_trend)
        trend_ja      = trend_ja_map.get(pp.ocf_trend, pp.ocf_trend)

        cagr_str = f"{dq.cagr_3yr:.1f}" if dq.cagr_3yr is not None else "N/A"
        gm_str   = f"{dq.gross_margin:.1f}" if dq.gross_margin is not None else "N/A"
        dq_s = int(dq.score or 0)
        ra_s = int(ra.score or 0)
        pp_s = int(pp.score or 0)

        lines = [
            "【総合スコア判定根拠】",
            "",
            f"赤字品質 {dq_s}点　（{verdict_label}）",
            f"• 売上成長率 {cagr_str}%　→　{cagr_cmt(dq.cagr_3yr)}",
        ]
        if invest_no_data:
            lines.append("• 研究開発+販管費 データなし")
        else:
            lines.append(f"• 研究開発+販管費 {invest_ratio:.1f}%　→　{invest_cmt(invest_ratio)}")
        lines.append(f"• 粗利率 {gm_str}%　→　{gm_cmt(dq.gross_margin)}")
        if ni_margin_line:
            lines.append(f"• {ni_margin_line}")

        lines += [
            "",
            f"生存能力 {ra_s}点　（{runway_label}）",
            f"• 現金 {fmt_b(ra.cash)}・月次バーン {fmt_b(ra.monthly_burn)}/月　→　ランウェイ{runway_detail}",
            "",
            f"黒字化パス {pp_s}点　（{path_label}）",
            f"• 営業CFトレンド {trend_ja}",
            f"• {ocf_be_text}",
            f"• {gaap_be_text}",
        ]

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _sum_not_none(*values) -> Optional[float]:
    """None を除いた値の合計。全て None なら None を返す。"""
    result = None
    for v in values:
        if v is not None:
            result = (result or 0) + v
    return result


def _calc_incremental_margin(
    years: list[int], records: dict
) -> list[tuple[int, float, float, float]]:
    """全年ペアの増分粗利率を新しい順に返す。戻り値: [(year, inc_margin%, rev_delta, gp_delta), ...]"""
    latest_rev = records[years[-1]]["pl"].get("revenue_sanitized")
    results = []
    for i in range(len(years) - 1, 0, -1):
        yr = years[i]
        prev_yr = years[i - 1]
        curr_rev = records[yr]["pl"].get("revenue_sanitized")
        prev_rev = records[prev_yr]["pl"].get("revenue_sanitized")
        curr_gp = records[yr]["pl"].get("gross_profit")
        prev_gp = records[prev_yr]["pl"].get("gross_profit")
        if all(v is not None for v in [curr_rev, prev_rev, curr_gp, prev_gp]):
            if latest_rev and prev_rev < latest_rev * 0.10:
                continue
            rev_delta = curr_rev - prev_rev
            if rev_delta <= 0:
                continue
            gp_delta = curr_gp - prev_gp
            inc_margin = gp_delta / rev_delta * 100
            results.append((yr, inc_margin, rev_delta, gp_delta))
    return results


def _fmt_margin_trend(margin_data: list, scale: float = 1) -> str:
    """直近2マージン値を 'XX%→XX%' 形式に変換。scale=1 は %-単位、scale=100 は ratio-単位。"""
    pts = [v * scale for _, v in margin_data]
    if len(pts) >= 2:
        return f"{pts[-2]:.0f}%→{pts[-1]:.0f}%"
    if len(pts) == 1:
        return f"{pts[-1]:.0f}%"
    return ""


def _ols_slope_intercept(xs: list[float], ys: list[float]) -> tuple[Optional[float], Optional[float]]:
    """単回帰OLSのslope・interceptを返す。分散ゼロ（全x同値）なら(None, None)。"""
    n = len(xs)
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    ss_xx = sum((x - x_mean) ** 2 for x in xs)
    if ss_xx == 0:
        return None, None
    slope = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n)) / ss_xx
    intercept = y_mean - slope * x_mean
    return slope, intercept


def _margin_breakeven(
    years: list[int],
    ocf_annual: dict[int, Optional[float]],
    records: dict,
    horizon: int = 5,
) -> tuple[Optional[int], str, bool]:
    """
    OCFマージン（OCF/Revenue）の改善外挿で黒字化年を予測する。

    [[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]（2026-09-05）: 常に直近4年
    （データがあれば）のマージン比率OLS回帰に統一した。旧実装は「直近2点の
    傾き優先→条件次第で絶対値ベース3点OLSへフォールバック」という2段階
    方式だったが、対象指標をマージン比率から絶対値へ途中で切り替える
    設計は黒字化予測の一貫性を損なうため廃止し、TANUKI VALUATION側
    （調整後EPS・常に4点OLS）と回帰の方式を揃えた。有効データ点の
    フィルタ（売上規模が直近年の10%未満の年を除外・|マージン|>1000%を
    除外）は既存の校正値のまま変更していない。

    Returns: (year, reason, predicted)
    reason codes: ACHIEVED / PREDICTED / NO_TREND[:XX%→XX%] / NO_DATA / TOO_FAR
    predicted: PREDICTED（実際に将来年を算出した場合）のみTrue。
    呼び出し元の非連続成長チェックのゲートに使う。
    """
    latest_rev = None
    for yr in reversed(years):
        r = records.get(yr, {}).get("pl", {}).get("revenue_sanitized")
        if r and r > 0:
            latest_rev = r
            break

    margin_data: list[tuple[int, float]] = []
    for yr in years[-4:]:
        ocf = ocf_annual.get(yr)
        rev = records.get(yr, {}).get("pl", {}).get("revenue_sanitized")
        if ocf is not None and rev is not None and rev > 0:
            if latest_rev is None or rev >= latest_rev * 0.10:
                m = ocf / rev  # ratio
                if abs(m) <= 10.0:  # abs(margin) ≤ 1000%
                    margin_data.append((yr, m))

    if len(margin_data) < 2:
        return None, "NO_DATA", False

    latest_yr, latest_m = margin_data[-1]
    if latest_m >= 0:
        return latest_yr, "ACHIEVED", False

    xs = [yr for yr, _ in margin_data]
    ys = [m for _, m in margin_data]
    slope, intercept = _ols_slope_intercept(xs, ys)
    if slope is None:
        return None, "NO_DATA", False

    # 傾き<=0（改善傾向なし）、または500pt/年超（信頼できない急変、既存の
    # 校正値のまま）はいずれもNO_TRENDとして予測を出さない
    if slope <= 0 or slope * 100 > 500:
        trend_str = _fmt_margin_trend(margin_data, scale=100)
        reason = f"NO_TREND:{trend_str}" if trend_str else "NO_TREND"
        return None, reason, False

    be_year = max(int(-intercept / slope + 0.5), latest_yr)
    if be_year > latest_yr + horizon:
        return None, "TOO_FAR", False
    return be_year, "PREDICTED", True


def _gaap_margin_breakeven(
    years: list[int],
    net_incomes: dict[int, Optional[float]],
    records: dict,
    horizon: int = 5,
) -> tuple[Optional[int], str, bool]:
    """
    純利益マージン（NI/Revenue × 100）の改善外挿でGAAP黒字化年を予測する。

    [[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]（2026-09-05）: `_margin_
    breakeven()`と同様、常に直近4年のマージン比率OLS回帰に統一した
    （旧: 2点傾き優先→絶対値3点OLSフォールバック）。フィルタの校正値
    （売上規模10%未満除外・|マージン|>1000%除外・500pt/年超除外）は
    変更していない。

    Returns: (year, reason, predicted)
    reason codes: ACHIEVED / PREDICTED / IMMINENT / NO_TREND[:XX%→XX%] / NO_DATA / TOO_FAR
    predicted: PREDICTED・IMMINENT（実際に将来年を算出した場合）のみTrue。
    """
    latest_rev = None
    for yr in reversed(years):
        r = records.get(yr, {}).get("pl", {}).get("revenue_sanitized")
        if r and r > 0:
            latest_rev = r
            break

    margin_data: list[tuple[int, float]] = []
    for yr in years[-4:]:
        ni = net_incomes.get(yr)
        rev = records.get(yr, {}).get("pl", {}).get("revenue_sanitized")
        if ni is not None and rev is not None and rev > 0:
            if latest_rev is None or rev >= latest_rev * 0.10:
                m = ni / rev * 100  # percentage
                if abs(m) <= 1000:
                    margin_data.append((yr, m))

    if len(margin_data) < 2:
        return None, "NO_DATA", False

    latest_yr, latest_m = margin_data[-1]
    if latest_m >= 0:
        return latest_yr, "ACHIEVED", False

    xs = [yr for yr, _ in margin_data]
    ys = [m for _, m in margin_data]
    slope, intercept = _ols_slope_intercept(xs, ys)
    if slope is None:
        return None, "NO_DATA", False

    if slope <= 0 or slope > 500:
        trend_str = _fmt_margin_trend(margin_data, scale=1)
        reason = f"NO_TREND:{trend_str}" if trend_str else "NO_TREND"
        return None, reason, False

    be_year = int(-intercept / slope + 0.5)
    if be_year <= latest_yr:
        return latest_yr + 1, "IMMINENT", True
    if be_year > latest_yr + horizon:
        return None, "TOO_FAR", False
    return be_year, "PREDICTED", True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from fetcher import load_annual_data

    ticker = sys.argv[1] if len(sys.argv) > 1 else "PLTR"
    data = load_annual_data(ticker, years=5)

    analyzer = StonksAnalyzer()
    result = analyzer.analyze(data)

    print("\n" + "=" * 60)
    print(result.summary)
    print("=" * 60)

    dq = result.deficit_quality
    ra = result.runway
    pp = result.profitability_path

    print(f"\n【① 赤字品質詳細】")
    print(f"  粗利率: {_pct(dq.gross_margin)}  R&D比: {_pct(dq.rnd_ratio)}  S&M比: {_pct(dq.sm_ratio)}")
    print(f"  3年CAGR: {_pct(dq.cagr_3yr)}")
    print(f"  売上成長YoY: ", end="")
    for yr, g in dq.revenue_growth_pct.items():
        print(f"{yr}={_pct(g)}", end="  ")
    print()

    print(f"\n【② 生存能力詳細】")
    print(f"  現金: {_fmt(ra.cash)}  月次バーン: {_fmt(ra.monthly_burn)}/月")

    print(f"\n【③ 黒字化パス詳細】")
    print(f"  年度     OCF          OCF速度      OCF加速度    コア利益")
    for yr in result.years:
        o = _fmt(pp.ocf_annual.get(yr))
        v = _fmt(pp.ocf_yoy_change.get(yr))
        a = _fmt(pp.ocf_acceleration.get(yr))
        c = _fmt(pp.core_profit.get(yr))
        print(f"  {yr}  {o:>12}  {v:>12}  {a:>12}  {c:>12}")

    print(f"\n  GAAP黒字化予測: {result.profitability_path.gaap_breakeven_year or '不明'}")
    print(f"  OCF黒字化予測 : {result.profitability_path.ocf_breakeven_year or '既に黒字' if pp.hidden_profit_already else '不明'}")


def _pct(v) -> str:
    if v is None: return "N/A"
    return f"{v:.1f}%"

def _fmt(v) -> str:
    if v is None: return "N/A"
    if abs(v) >= 1e9: return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6: return f"${v/1e6:.0f}M"
    return f"${v:,.0f}"
