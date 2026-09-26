"""
TANUKI VALUATION - Core Calculator v6.2
Koichi式株価評価モデル（FCFベース自動判定対応）

P_t = V_0 × (1 + α) + RPO_PV + GrowthOption_PV
V_0 = 3段階DCF or 2段階DCF（maturity_config参照）

v6.2 追加:
  - FCFベース自動判定（determine_fcf_base）
    直近2年 / 5年平均 の比率が閾値超 → recent_2yr を自動選択
    安定・成熟企業 → avg_5yr（既存動作）

v8.1 追加:
  - RPO補正: セクター別適用率（SaaS100%/Fintech50%/保険0%/消費者0%）
  - BS補正: セクターガード（保険の負債除外/FintechのST債務除外）

計算フロー:
  1. WACC計算（CAPM）
  2. 成長率決定
  3. FCF補正（マイナスFCF対応）
  4. FCFベース自動判定（v6.2追加）← NEW
  4b. FCF外れ値分析（v7.1追加）
  4c. FCF実力推定（v7.2追加）
  4d. R&D資本化補正（v8.2追加）← NEW
  5. DCF計算（2段階 or 3段階）
  6. RPO補正（v8.1: セクター別適用率）
  7. 成長オプションPV計算
  8. α計算
  9. 本質的価値（P_t）算出
 10. 感度分析
 11. シナリオ分析
 12. 将来価値予測
 13. RICE計算（v8.0追加）
"""

import os
import sys

_current_dir = os.path.dirname(os.path.abspath(__file__))
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from calculator import (
    calculate_wacc, WACCResult,
    determine_growth_rate, GrowthResult,
    calculate_two_stage_dcf, DCFResult,
    DEFAULT_HIGH_GROWTH_YEARS, DEFAULT_TERMINAL_GROWTH,
    adjust_fcf, adjust_rpo, calculate_alpha,
    calculate_intrinsic_value, calculate_per_share_value, calculate_upside,
    DEFAULT_RETENTION_RATE, DEFAULT_ALPHA_CAP,
    calculate_sensitivity_matrix, create_sensitivity_calc_func, SensitivityResult,
    calculate_scenario_valuations, create_scenario_calc_func as create_scenario_func, ScenarioResult,
    calculate_future_values,
    calculate_return_metrics,
)

from calculator.dcf import calculate_three_stage_dcf, ThreeStageDCFResult
from calculator.adjustments import (
    calculate_growth_option_pv, GrowthOptionResult,
    determine_fcf_base, FCFBaseResult,          # v6.2追加
    calculate_moat_score, MoatScoreResult,      # ALPHA-REDESIGN-1
    DEFAULT_FCF_CV_THRESHOLD,
    calculate_bs_adjustment, BSAdjustmentResult,  # v7.0追加
    compose_fcf_bottom_up, FCFCompositionResult,  # v10.0: [[FCF-CONVRATE-LOWER-DIVERGENCE-1]]
    analyze_fcf_outlier, FCFOutlierResult,        # v7.1追加
    capitalize_rd, RDCapitalizationResult,        # v8.2追加
)
from calculator.fcf_outlier_ai import assess_transient_qualitative  # [[FCF-OUTLIER-QUAL-1]]

try:
    from maturity_config import get_maturity_profile, is_three_stage, get_terminal_growth
    HAS_MATURITY_CONFIG = True
except ImportError:
    HAS_MATURITY_CONFIG = False

try:
    _repo_root = os.path.dirname(os.path.dirname(os.path.dirname(_current_dir)))
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from common.sec_data.reader import SECReader
    HAS_SEC_READER = True
except ImportError:
    HAS_SEC_READER = False
    SECReader = None

from calculator.rice import calculate_rice, RICEResult


def resolve_alpha_cap(
    ticker: str,
    sector: Optional[str],
    industry: Optional[str],
    default_alpha_cap: float = DEFAULT_ALPHA_CAP,
) -> float:
    """α（成長期待プレミアム）のセクター別・業種別上限を、
    `config/maturity_config.json`から解決する。

    優先順位: mega_tech（tickerが`_sector_caps._mega_tech_tickers`に
    一致）→ 業種別（`_industry_alpha_caps`）→ セクター別（`_alpha_caps`）
    → `default_alpha_cap`。

    **[[VALIDATOR-ALPHA-CAP-STALE-1]]対応（2026-08-20）**: 元々
    `calculate_pt()`内にインライン実装されていたロジックをそのまま
    関数として切り出した（計算内容・優先順位は変更していない）。
    `validator.py`がこの関数をimportして使うことで、alpha_cap解決
    ロジックを2箇所に独立実装せず、本番（`core_calculator.py`）と
    検証（`validator.py`）で常に同一の判定を保証する（今回の不具合の
    再発防止——`validator.py`側で優先順位を書き写すと、将来
    `core_calculator.py`側の判定が変わった際に再び乖離するため）。

    設定読み込みに失敗した場合は`default_alpha_cap`をそのまま返す
    （元の`try/except Exception: pass`と同じフォールバック挙動）。
    """
    alpha_cap = default_alpha_cap
    try:
        import json as _json, pathlib as _pl
        _cfg_path = _pl.Path(__file__).parent.parent.parent.parent / "config" / "maturity_config.json"
        _cfg_all  = _json.loads(_cfg_path.read_text(encoding="utf-8"))
        _alpha_caps = _cfg_all.get("_alpha_caps", {})
        _industry_alpha_caps = _cfg_all.get("_industry_alpha_caps", {})
        _mega = _cfg_all.get("_sector_caps", {}).get("_mega_tech_tickers", [])
        if ticker in _mega:
            alpha_cap = _alpha_caps.get("mega_tech", default_alpha_cap)
        elif industry and industry in _industry_alpha_caps:
            alpha_cap = _industry_alpha_caps[industry]
        elif sector and sector in _alpha_caps:
            alpha_cap = _alpha_caps[sector]
    except Exception:
        pass
    return alpha_cap


class KoichiValuationCalculator:
    """
    Koichi式 v6.2 バリュエーション計算エンジン

    v6.2変更点:
      - FCFベース自動判定（急拡大銘柄は直近2年平均を使用）
    """

    VERSION = "8.2.0"

    def __init__(
        self,
        high_growth_years: int = DEFAULT_HIGH_GROWTH_YEARS,
        terminal_growth: float = DEFAULT_TERMINAL_GROWTH,
        retention_rate: float = DEFAULT_RETENTION_RATE,
        alpha_cap: float = DEFAULT_ALPHA_CAP,
        min_fcf_years: int = 3,
        fcf_base_threshold: float = DEFAULT_FCF_CV_THRESHOLD,
        eps_data_dir: str = "",
        sec_data_dir: str = "",
    ):
        self.high_growth_years = high_growth_years
        self.terminal_growth = terminal_growth
        self.retention_rate = retention_rate
        self.alpha_cap = alpha_cap
        self.min_fcf_years = min_fcf_years
        self.fcf_base_threshold = fcf_base_threshold
        self.eps_data_dir = eps_data_dir
        self.sec_data_dir = sec_data_dir  # v7.1: EPSアナライザーdataディレクトリ

    def calculate_pt(self, financials: Dict[str, Any], tapering_g_end: float | None = None, bear_multiplier: float = 0.7) -> Dict[str, Any]:
        """メイン計算関数。tapering_g_end が設定された場合は線形逓減DCFを適用（DCF-1）"""

        # ── データ抽出 ──
        fcf_avg        = financials.get("fcf_5yr_avg", 0.0)
        fcf_2yr_avg    = financials.get("fcf_2yr_avg", 0.0)   # v6.2追加
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg        = financials.get("roe_10yr_avg")
        roe_years_used = financials.get("roe_years_used", 0)
        roe_outlier_adj = financials.get("roe_outlier_adj", False)
        latest_revenue = financials.get("latest_revenue", 0.0)
        fcf_list_raw   = financials.get("fcf_list_raw", [])
        # [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]: fcf_list_rawの順序検証用日付
        # （未取得時はNone。determine_growth_rate()側で検証可否を判定する）
        fcf_dates_raw  = financials.get("fcf_dates_raw")
        current_price  = financials.get("current_price")  # None=有効な終値なし（upside等は計算不能）
        ticker         = financials.get("eps_data", {}).get("ticker", "Unknown")
        rpo            = financials.get("rpo", 0.0)
        beta           = financials.get("beta")
        sector         = financials.get("sector")
        industry       = financials.get("industry", "")   # v8.1: 保険判定精度向上
        net_cash_data  = financials.get("net_cash_data", {"net_cash": 0.0, "available": False})  # v7.0
        # v7.1: FCF外れ値分析用（net_cash_dataのfiscal_yearを流用）
        fiscal_year_of_latest = net_cash_data.get("fiscal_year", 0)

        # ── バリデーション ──
        if diluted_shares <= 100_000:
            return {"error": "diluted_shares missing or invalid", "ticker": ticker}

        if len(fcf_list_raw) < self.min_fcf_years:
            return {
                "error": f"FCFデータ不足 ({len(fcf_list_raw)}年)",
                "ticker": ticker,
                "fcf_years_available": len(fcf_list_raw),
                "min_required": self.min_fcf_years
            }

        # ── STEP 1: WACC計算 ──
        wacc_result: WACCResult = calculate_wacc(beta=beta, sector=sector)
        wacc = wacc_result.value
        print(f"   [{ticker}] WACC (CAPM): {wacc:.1%} (β={wacc_result.beta:.2f})")

        # ── STEP 2: 成長率決定 ──
        growth_result: GrowthResult = determine_growth_rate(
            ticker=ticker, fcf_list=fcf_list_raw, fcf_dates=fcf_dates_raw
        )
        high_growth_rate = growth_result.rate
        print(f"   [{ticker}] 成長率: {high_growth_rate:.1%} (source: {growth_result.source})")

        # ── STEP 3: FCF補正（マイナスFCF対応） ──
        fcf_adjustment = adjust_fcf(fcf_avg=fcf_avg, latest_revenue=latest_revenue)
        adjusted_fcf_5yr = fcf_adjustment.adjusted_fcf
        if fcf_adjustment.method != "none":
            print(f"   [{ticker}] FCF補正: ${fcf_adjustment.original_fcf:,.0f} → ${adjusted_fcf_5yr:,.0f}")

        # ── STEP 4: FCFベース自動判定（v6.3 CV方式） ──
        # 元のfcf_2yr_avg（マイナス含む）をそのまま渡す。
        # フォールバック処理はdetermine_fcf_base内で行う。
        fcf_base_result: FCFBaseResult = determine_fcf_base(
            fcf_5yr_avg=adjusted_fcf_5yr,
            fcf_2yr_avg=fcf_2yr_avg,
            fcf_list=fcf_list_raw,
            threshold=self.fcf_base_threshold
        )
        adjusted_fcf_2yr = fcf_2yr_avg  # ログ表示用
        base_fcf = fcf_base_result.base_fcf
        print(f"   [{ticker}] FCFベース: {fcf_base_result.method}  "
              f"5yr=${adjusted_fcf_5yr/1e9:.2f}B  "
              f"2yr=${adjusted_fcf_2yr/1e9:.2f}B  "
              f"→ 採用=${base_fcf/1e9:.2f}B"
              + (f"  (CV={fcf_base_result.cv:.2f})" if fcf_base_result.cv < 999 else "  (CV=データ不足)"))

        # ── STEP 4b: FCF外れ値分析（v7.1追加）──
        # EPSアナライザーと突合して一過性費用を確認・記録
        fcf_outlier_result: FCFOutlierResult = analyze_fcf_outlier(
            ticker=ticker,
            fcf_list=fcf_list_raw,
            fcf_5yr_avg=fcf_avg,       # 元の5年平均（補正前）
            cv=fcf_base_result.cv,
            fiscal_year_of_latest=fiscal_year_of_latest,
            eps_data_dir=self.eps_data_dir,
            cv_threshold=self.fcf_base_threshold,
        )
        if fcf_outlier_result.detected:
            action_tag = "除外" if fcf_outlier_result.action == "excluded" else "要確認"
            transient_str = (
                f"一過性費用合計${fcf_outlier_result.transient_total/1e6:.0f}M確認済"
                if fcf_outlier_result.transient_found
                else "一過性費用の証拠なし"
            )
            print(f"   [{ticker}] FCF外れ値: {fcf_outlier_result.rule} → {action_tag}（{transient_str}）")

        # ── STEP 4b後: FCF外れ値除外後の base_fcf 再計算 ──
        # action="excluded" のとき外れ値年（fcf_list[0]）を除いた残りで平均を再計算し上書き
        _fcf_outlier_excl_avg: float | None = None
        if fcf_outlier_result.action == "excluded" and len(fcf_list_raw) > 1:
            _remaining = fcf_list_raw[1:]   # 外れ値は常に fcf_list[0]
            _excl_avg = sum(_remaining) / len(_remaining)
            if _excl_avg > 0:
                _old_base_fcf = base_fcf
                base_fcf = _excl_avg
                _fcf_outlier_excl_avg = _excl_avg
                print(
                    f"   [{ticker}] FCF外れ値除外後再計算: {len(_remaining)}年平均"
                    f" ${base_fcf/1e6:.0f}M ← 除外前${_old_base_fcf/1e6:.0f}M"
                )

        # ── STEP 4c: FCF内訳分解（ボトムアップ、OCF-CapEx）v10.0 ──
        # [[FCF-CONVRATE-LOWER-DIVERGENCE-1]]（2026-09-17）: 業種別固定
        # 転換率（adj_net_income×conversion_rate）方式を廃止し、raw_fcf
        # （=base_fcf、既にOCF-CapExベース）をそのまま採用した上でCapEx/
        # SBC/OCF/D&Aの内訳を開示する方式へ移行した。sector自体はWACCの
        # βフォールバック（STEP 1で使用済み）以外に用途がなくなったため
        # ここでの再取得は不要。
        _sw_provisional = False
        _sw_provisional_note = ""
        try:
            from data_fetcher import _load_beta_config
            _bcfg = _load_beta_config()
            _ticker_bcfg = _bcfg.get('overrides', {}).get(ticker, {})
            # 過去にbeta_fetcher.py::classify_software_system_subgroup()
            # （conversion_rate方式廃止に伴い削除済み）が設定した銘柄が
            # 残っていた場合のみ表示（新規に設定されることはない）
            _sw_provisional = bool(_ticker_bcfg.get('software_system_provisional', False))
            _sw_provisional_note = _ticker_bcfg.get('software_system_provisional_note', '') or ''
        except Exception:
            pass

        fcf_composition: FCFCompositionResult = compose_fcf_bottom_up(
            ticker=ticker,
            raw_fcf=base_fcf,
            capex_list=financials.get("capex_list", []),
            sbc_list=financials.get("sbc_list", []),
            ocf_list=financials.get("ocf_list", []),
            da_list=financials.get("da_list", []),
        )
        if fcf_composition.applied:
            print(f"   [{ticker}] FCF内訳: フォールバック → {fcf_composition.fallback_reason}")
        else:
            print(f"   [{ticker}] FCF内訳: {fcf_composition.note}")

        # ── STEP 4d: R&D資本化補正（v8.2追加）──
        # R&Dを費用ではなく投資として扱い、FCFの過小評価を補正する
        # 適用条件: R&D/Revenue >= 5%（軽資産企業・消費財等は非適用）
        rd_capitalization: RDCapitalizationResult = RDCapitalizationResult(
            applied=False, rd_current=0.0, rd_avg_3yr=0.0,
            capitalized_rd=0.0, amortization_current=0.0,
            rd_adjustment=0.0, rd_revenue_ratio=0.0,
            threshold=0.05, years_used=0, note="未実行"
        )
        if self.sec_data_dir:
            try:
                rd_capitalization = capitalize_rd(
                    ticker=ticker,
                    sec_data_dir=self.sec_data_dir,
                )
                if rd_capitalization.applied:
                    base_fcf += rd_capitalization.rd_adjustment
                    sign = "+" if rd_capitalization.rd_adjustment >= 0 else ""
                    print(f"   [{ticker}] R&D資本化: {sign}${rd_capitalization.rd_adjustment/1e9:.2f}B"
                          f" → 調整後FCF=${base_fcf/1e9:.2f}B"
                          f" (R&D/Rev={rd_capitalization.rd_revenue_ratio:.1%})")
                else:
                    print(f"   [{ticker}] R&D資本化: 非適用 ({rd_capitalization.note})")
            except Exception as _rd_e:
                print(f"   [{ticker}] R&D資本化エラー: {_rd_e}")

        # ── STEP 4e: Moat Score → Phase1期間自動計算（ALPHA-REDESIGN-1）──
        # [[MOAT-SCORE-PARTIAL-NULL-1]]: roic_reasonにより「真の赤字は算入・
        # それ以外（測定不能）は除外」を判定する（2026-08-16実装）。
        moat_result: MoatScoreResult = calculate_moat_score(
            gross_margin_3yr_avg=financials.get("moat_gross_margin_3yr"),
            roic=financials.get("moat_roic"),
            fcf_margin_3yr_avg=financials.get("moat_fcf_margin_3yr"),
            roic_reason=financials.get("moat_roic_reason", "ok"),
        )
        _moat_phase1_years: int = moat_result.phase1_years
        _fmt_norm = lambda v: f"{v:.2f}" if v is not None else "N/A"
        _moat_source_note = "" if moat_result.source == "measured" else "  ⚠️中立フォールバック(有効指標<2)"
        print(f"   [{ticker}] Moat Score: {moat_result.moat_score:.3f}"
              f"  (GM={_fmt_norm(moat_result.gross_margin_norm)}"
              f"  ROIC={_fmt_norm(moat_result.roic_norm)}"
              f"  FCF={_fmt_norm(moat_result.fcf_margin_norm)})"
              f"  → Phase1={_moat_phase1_years}yr{_moat_source_note}")

        # ── STEP 5: DCF計算（2段階 or 3段階） ──
        dcf_type = "two_stage"
        dcf_result = None
        three_stage_result: Optional[ThreeStageDCFResult] = None
        maturity_profile = None

        terminal_growth = self.terminal_growth
        if HAS_MATURITY_CONFIG:
            terminal_growth = get_terminal_growth(ticker)

        if HAS_MATURITY_CONFIG and is_three_stage(ticker):
            maturity_profile = get_maturity_profile(ticker)
            p1 = maturity_profile["phase1"]
            p2 = maturity_profile["phase2"]

            phase1_growth = p1["growth"] if p1["growth"] is not None else high_growth_rate
            phase2_growth = p2["growth"]
            phase1_years  = _moat_phase1_years  # ALPHA-REDESIGN-1: Moat Score連動
            phase2_years  = p2["years"]

            # ③ Phase2成長率にセクター上限を適用
            try:
                import json as _json, pathlib as _pl
                _cfg_path = _pl.Path(__file__).parent.parent.parent.parent / "config" / "maturity_config.json"
                _cfg_all  = _json.loads(_cfg_path.read_text(encoding="utf-8"))
                _caps     = _cfg_all.get("_sector_caps", {})
                _mega     = _caps.get("_mega_tech_tickers", [])
                _mega_cap = _caps.get("_mega_tech_cap")
                if ticker in _mega and _mega_cap is not None:
                    _cap = _mega_cap
                elif sector and sector in _caps:
                    _cap = _caps[sector]
                else:
                    _cap = None
                if _cap is not None and phase2_growth is not None and phase2_growth > _cap:
                    print(f"   [{ticker}] Phase2上限適用: {phase2_growth:.1%} → {_cap:.1%} (sector={sector})")
                    phase2_growth = _cap
            except Exception:
                pass

            # [[DCF-1b]]: Phase1はphase1_growth→phase2_growthへ線形逓減（calculator/dcf.py参照）
            print(f"   [{ticker}] DCF: 3段階  P1={phase1_years}yr@{phase1_growth:.1%}→{phase2_growth:.1%}(逓減)  P2={phase2_years}yr@{phase2_growth:.1%}  TV={terminal_growth:.1%}")

            three_stage_result = calculate_three_stage_dcf(
                base_fcf=base_fcf,
                phase1_growth_rate=phase1_growth,
                phase2_growth_rate=phase2_growth,
                wacc=wacc,
                phase1_years=phase1_years,
                phase2_years=phase2_years,
                terminal_growth=terminal_growth
            )
            v0 = three_stage_result.v0
            dcf_type = "three_stage"
        else:
            # 線形逓減DCF（DCF-1）: growth_model==decayの高成長銘柄に適用
            _use_tapering = (
                tapering_g_end is not None
                and high_growth_rate > tapering_g_end
            )
            if _use_tapering:
                from calculator.dcf import calculate_tapering_dcf, TaperingDCFResult
                print(f"   [{ticker}] DCF: 逓減  g={high_growth_rate:.1%}→{tapering_g_end:.1%}  TV={terminal_growth:.1%}")
                _tapering_result = calculate_tapering_dcf(
                    base_fcf=base_fcf,
                    g_start=high_growth_rate,
                    g_end=tapering_g_end,
                    wacc=wacc,
                    high_growth_years=_moat_phase1_years,
                    terminal_growth=terminal_growth,
                )
                dcf_result = DCFResult(
                    v0=_tapering_result.v0,
                    pv_high_growth=_tapering_result.pv_high_growth,
                    pv_terminal=_tapering_result.pv_terminal,
                    high_growth_detail=_tapering_result.high_growth_detail,
                    terminal_fcf=_tapering_result.terminal_fcf,
                    terminal_value=_tapering_result.terminal_value,
                )
                dcf_type = "tapering"
            else:
                print(f"   [{ticker}] DCF: 2段階  g={high_growth_rate:.1%}  TV={terminal_growth:.1%}")
                dcf_result = calculate_two_stage_dcf(
                    base_fcf=base_fcf,
                    high_growth_rate=high_growth_rate,
                    wacc=wacc,
                    high_growth_years=_moat_phase1_years,
                    terminal_growth=terminal_growth
                )
            v0 = dcf_result.v0

        # ── STEP 6: RPO補正（normalizedからRPO時系列・利益率を取得）──
        _rpo_series: list = []
        _rev_yoy: Optional[float] = None
        _rev_ttm: Optional[float] = None
        _op_margin: Optional[float] = None
        if HAS_SEC_READER and ticker != "Unknown":
            try:
                _sec_reader = SECReader()
                _rpo_ctx = _sec_reader.get_rpo_context(ticker)
                _rpo_series = _rpo_ctx.get("rpo_series", [])
                _rev_yoy   = _rpo_ctx.get("rev_yoy")
                _rev_ttm   = _rpo_ctx.get("rev_ttm")
                _op_margin = _rpo_ctx.get("op_margin")
            except Exception:
                pass

        # rpo_series[-1]=最新, rpo_series[-5]=約4四半期前（前年同期）
        rpo_latest = _rpo_series[-1]["rpo"] if _rpo_series else rpo
        rpo_yago   = _rpo_series[-5]["rpo"] if len(_rpo_series) >= 5 else None

        rpo_adjustment = adjust_rpo(
            rpo=rpo_latest,
            sector=sector,
            ticker=ticker,
            industry=industry,
            op_margin=_op_margin or 0.0,
            rpo_yago=rpo_yago,
            rev_yoy=_rev_yoy,
            rev_ttm=_rev_ttm,
        )
        rpo_pv = rpo_adjustment.rpo_pv
        if rpo_adjustment.applied:
            rate_str = f" ({rpo_adjustment.application_rate:.0%} 適用: {rpo_adjustment.sector_category})"
            incr_str = (f" incremental=${rpo_adjustment.rpo_incremental/1e9:.1f}B"
                        if rpo_adjustment.rpo_incremental > 0 else "")
            margin_str = f" margin={_op_margin:.1%}" if _op_margin is not None else ""
            print(f"   [{ticker}] RPO補正: ${rpo_latest:,.0f} → PV ${rpo_pv:,.0f}{rate_str}{incr_str}{margin_str}")
        elif rpo_latest > 0:
            reason = (f"適用率0%: {rpo_adjustment.sector_category}"
                      if rpo_adjustment.application_rate == 0.0
                      else f"赤字(margin={_op_margin:.1%})" if _op_margin is not None and _op_margin <= 0
                      else "incremental=0")
            print(f"   [{ticker}] RPO補正: スキップ ({reason})")

        # ── STEP 7: 成長オプションPV計算 ──
        go_result: GrowthOptionResult = calculate_growth_option_pv(ticker)
        growth_option_pv = go_result.total_pv
        if go_result.applied:
            print(f"   [{ticker}] 成長オプション: {go_result.count}件  PV=${growth_option_pv/1e9:.2f}B")

        # ── STEP 8: α計算 ──
        # RM基準・Rf取得（STEP8以降で使用）
        _rf = wacc_result.risk_free_rate  # Rf: 通常4.3%
        _rm = wacc_result.market_return   # Rm: 通常10.0%（βなし・メイン割引率）

        # ④ αセクター別・業種別上限を決定（業種 > セクター の優先順）
        # [[VALIDATOR-ALPHA-CAP-STALE-1]]対応: ロジックはモジュール
        # 関数resolve_alpha_cap()へ切り出し済み（validator.pyと共用）。
        _alpha_cap = resolve_alpha_cap(ticker, sector, industry, self.alpha_cap)

        # αはRM基準（βなし・市場期待リターン10%）で計算
        # roe_avg=None は負債超過（負PBR）を意味し alpha=0 として扱う
        alpha_result = calculate_alpha(
            roe=roe_avg if roe_avg is not None else 0.0, wacc=_rm,
            retention_rate=self.retention_rate, alpha_cap=_alpha_cap
        )
        alpha = alpha_result.alpha
        if alpha_result.was_capped:
            print(f"   [{ticker}] α: {alpha_result.alpha_uncapped:.3f} → cap({_alpha_cap:.1f}) → {alpha:.3f}")
        else:
            print(f"   [{ticker}] α: {alpha:.3f}")

        # ── STEP 9: BS評価補正（v7.0追加）──
        # ネットキャッシュ/純負債を1株あたり価値に換算して加算
        bs_adjustment: BSAdjustmentResult = calculate_bs_adjustment(
            net_cash_data=net_cash_data,
            diluted_shares=diluted_shares
        )
        if bs_adjustment.applied:
            nc = bs_adjustment.net_cash
            ncs = bs_adjustment.net_cash_per_share
            sign = "+" if nc >= 0 else ""
            guard_str = f" [{bs_adjustment.sector_guard}]" if bs_adjustment.sector_guard != "none" else ""
            print(f"   [{ticker}] BS補正: ネットキャッシュ {sign}${nc/1e9:.2f}B → {sign}${ncs:.2f}/株{guard_str}")

        # ── 割引率定数（STEP10bで使用）──

        # ── STEP 10: 本質的価値（P_t）算出 ──
        # ALPHA-REDESIGN-1: alpha乗算廃止（alpha=0.0）。alphaは参考値として保持
        intrinsic_value_pt = calculate_intrinsic_value(
            v0=v0, rpo_pv=rpo_pv, alpha=0.0,
            growth_option_pv=growth_option_pv
        )

        # DCFベース理論株価 + ネットキャッシュ/株（β込みWACC版・参考用）
        intrinsic_value_per_share_beta = (
            calculate_per_share_value(
                intrinsic_value_pt=intrinsic_value_pt,
                diluted_shares=diluted_shares
            )
            + bs_adjustment.net_cash_per_share
        )



        # ── STEP 10b: 割引率別理論株価（v7.3）──
        def _calc_ivps_with_wacc(discount_rate):
            """指定した割引率で理論株価を計算（共通処理）"""
            if dcf_type == "three_stage" and maturity_profile:
                _p1 = maturity_profile["phase1"]
                _p2 = maturity_profile["phase2"]
                _res = calculate_three_stage_dcf(
                    base_fcf=base_fcf,
                    phase1_growth_rate=_p1["growth"] if _p1["growth"] is not None else high_growth_rate,
                    phase2_growth_rate=_p2["growth"],
                    wacc=discount_rate,
                    phase1_years=_moat_phase1_years,  # ALPHA-REDESIGN-1
                    phase2_years=_p2["years"],
                    terminal_growth=terminal_growth,
                )
            elif dcf_type == "tapering" and tapering_g_end is not None:
                from calculator.dcf import calculate_tapering_dcf as _calc_tap
                _tap = _calc_tap(
                    base_fcf=base_fcf,
                    g_start=high_growth_rate,
                    g_end=tapering_g_end,
                    wacc=discount_rate,
                    high_growth_years=_moat_phase1_years,  # ALPHA-REDESIGN-1
                    terminal_growth=terminal_growth,
                )
                _res = DCFResult(
                    v0=_tap.v0,
                    pv_high_growth=_tap.pv_high_growth,
                    pv_terminal=_tap.pv_terminal,
                    high_growth_detail=_tap.high_growth_detail,
                    terminal_fcf=_tap.terminal_fcf,
                    terminal_value=_tap.terminal_value,
                )
            else:
                _res = calculate_two_stage_dcf(
                    base_fcf=base_fcf,
                    high_growth_rate=high_growth_rate,
                    wacc=discount_rate,
                    high_growth_years=_moat_phase1_years,  # ALPHA-REDESIGN-1
                    terminal_growth=terminal_growth,
                )
            _ivpt = calculate_intrinsic_value(
                v0=_res.v0, rpo_pv=rpo_pv, alpha=0.0,  # ALPHA-REDESIGN-1: alpha廃止
                growth_option_pv=growth_option_pv
            )
            return (
                calculate_per_share_value(
                    intrinsic_value_pt=_ivpt,
                    diluted_shares=diluted_shares
                )
                + bs_adjustment.net_cash_per_share
            )

        # ② Rmβなし = Rmで計算（メイン理論株価）
        # ── メイン理論株価はRmβなし（市場期待リターン）で計算 v7.3 ──
        # 理由: β込みWACCは市場の評価を割引率に持ち込むため
        #       「市場から独立した本質的価値」という目的と矛盾する
        # RM基準V0を別途計算して保存（UIのSTEP11表示用）
        if dcf_type == "three_stage" and maturity_profile:
            _p1 = maturity_profile["phase1"]; _p2 = maturity_profile["phase2"]
            _res_rm = calculate_three_stage_dcf(
                base_fcf=base_fcf,
                phase1_growth_rate=_p1["growth"] if _p1["growth"] is not None else high_growth_rate,
                phase2_growth_rate=_p2["growth"], wacc=_rm,
                phase1_years=_moat_phase1_years, phase2_years=_p2["years"],  # ALPHA-REDESIGN-1
                terminal_growth=terminal_growth,
            )
        elif dcf_type == "tapering" and tapering_g_end is not None:
            from calculator.dcf import calculate_tapering_dcf as _calc_tap_rm
            _res_rm = _calc_tap_rm(
                base_fcf=base_fcf, g_start=high_growth_rate, g_end=tapering_g_end,
                wacc=_rm, high_growth_years=_moat_phase1_years,  # ALPHA-REDESIGN-1
                terminal_growth=terminal_growth,
            )
        else:
            _res_rm = calculate_two_stage_dcf(
                base_fcf=base_fcf, high_growth_rate=high_growth_rate,
                wacc=_rm, high_growth_years=_moat_phase1_years,  # ALPHA-REDESIGN-1
                terminal_growth=terminal_growth,
            )
        _v0_rm = _res_rm.v0  # RM基準V0（UIのSTEP11に表示すべき値）
        intrinsic_value_per_share = _calc_ivps_with_wacc(_rm)
        upside_percent = calculate_upside(
            intrinsic_value_per_share=intrinsic_value_per_share,
            current_price=current_price
        )
        _ivps_rm_no_beta = intrinsic_value_per_share
        _upside_rm_no_beta = upside_percent

        # ③ Rf（リスクゼロ）で計算
        _ivps_rf = _calc_ivps_with_wacc(_rf)
        _upside_rf = calculate_upside(
            intrinsic_value_per_share=_ivps_rf,
            current_price=current_price
        )

        # ── STEP 10: 感度分析 ──
        # Phase2パラメータを取得（3段階DCFの場合）
        _phase2_growth = None
        _phase2_years = 0
        if dcf_type == "three_stage" and maturity_profile:
            _phase2_growth = maturity_profile.get("phase2", {}).get("growth")
            _phase2_years  = maturity_profile.get("phase2", {}).get("years", 0)

        sensitivity_calc_func = create_sensitivity_calc_func(
            base_fcf=base_fcf,
            high_growth_rate=high_growth_rate,
            diluted_shares=diluted_shares,
            rpo_pv=rpo_pv + growth_option_pv,
            alpha=0.0,  # ALPHA-REDESIGN-1: alpha廃止
            terminal_growth=terminal_growth,
            net_cash_per_share=bs_adjustment.net_cash_per_share,  # v7.1: BS補正
            phase2_growth=_phase2_growth,                          # v7.1: 3段階対応
            phase2_years=_phase2_years,                            # v7.1: 3段階対応
        )
        # 感度分析のbase_yearsはMoat Score連動Phase1年数（ALPHA-REDESIGN-1）
        _sensitivity_base_years = _moat_phase1_years

        sensitivity_result: SensitivityResult = calculate_sensitivity_matrix(
            calc_func=sensitivity_calc_func,
            base_wacc=_rm,  # v7.3: Rmβなし基点（中央セルがメイン理論株価と一致）
            base_years=_sensitivity_base_years
        )

        # ── STEP 11: シナリオ分析 ──
        # growth source を問わず全銘柄でシナリオを計算する
        scenario_result: Optional[ScenarioResult] = None
        _scenario_p1_years = _sensitivity_base_years  # Phase1実際年数
        scenario_calc_func = create_scenario_func(
            base_fcf=base_fcf,
            wacc=_rm,  # v7.3: Rmβなし（メイン理論株価と整合）
            high_growth_years=_scenario_p1_years,
            diluted_shares=diluted_shares,
            rpo_pv=rpo_pv + growth_option_pv,
            alpha=0.0,  # ALPHA-REDESIGN-1: alpha廃止
            terminal_growth=terminal_growth,
            net_cash_per_share=bs_adjustment.net_cash_per_share,  # v7.1: BS補正
            phase2_growth=_phase2_growth,                          # v7.1: 3段階対応
            phase2_years=_phase2_years,                            # v7.1: 3段階対応
            tapering_g_end=tapering_g_end,                         # DCF-1: 線形逓減
        )
        scenario_result = calculate_scenario_valuations(
            calc_func=scenario_calc_func,
            base_growth_rate=high_growth_rate,
            bear_multiplier=bear_multiplier,
        )

        # ── STEP 12: 将来価値予測 ──
        # BASE シナリオの ivps を起点とする（segment_weighted の場合）
        # segment_weighted でない場合は Rmβなし ivps をそのまま使用
        _future_base_val = intrinsic_value_per_share
        if scenario_result is not None:
            _sv = scenario_result.to_dict()
            _future_base_val = _sv.get("base", {}).get("intrinsic_value_per_share", intrinsic_value_per_share)
        future_values = calculate_future_values(
            current_value=_future_base_val,
            high_growth_rate=high_growth_rate,
            high_growth_years=_moat_phase1_years,  # ALPHA-REDESIGN-1
            terminal_growth=terminal_growth,
            projection_years=5,
        )
        print(f"   [{ticker}] 1〜5年後理論株価: {future_values}")

        # DESIGN-3: 期待リターン指標（current_price が有効な場合のみ）
        _return_metrics: dict | None = None
        if current_price and current_price > 0:
            _return_metrics = calculate_return_metrics(
                current_value=_future_base_val,
                current_price=current_price,
                future_values=future_values,
            )

        # ── STEP 13: RICE計算（v8.0追加）──
        rice_result: RICEResult = RICEResult(
            q=0.0, cf_conversion=0.0, wacc=_rm,  # v7.3: フォールバックもRm基準に統一
            q_years=0, cf_years=0,
            avg_intensity=0.0, avg_rev_growth=0.0,
            available=False, note="SEC年次データ未取得"
        )
        try:
            _rice_data = financials.get("rice_annual_data")
            if _rice_data:
                _current_per = financials.get("per") or financials.get("current_per") or 0.0
                _sc_val = scenario_result.to_dict() if scenario_result else None
                _rice_sector = financials.get("rice_sector") or sector or ""
                _roic_wacc_ratio = financials.get("roic_wacc_ratio")
                rice_result = calculate_rice(
                    annual_data=_rice_data,
                    wacc=_rm,  # v7.3: RICEもRmβなし基準に統一
                    scenario_valuations=_sc_val,
                    current_per=_current_per,
                    sector=_rice_sector,
                    industry=industry or "",
                    roic_wacc_ratio=_roic_wacc_ratio,  # RICE-1
                )
                if rice_result.available:
                    _base_rice = rice_result.base.rice if rice_result.base else 0.0
                    _vc_info = f"  vc={rice_result.vc_factor:.2f}" if rice_result.vc_factor is not None else ""
                    print(f"   [{ticker}] RICE: Q={rice_result.q:.2f}  "
                          f"CF={rice_result.cf_conversion:.2f}  base={_base_rice:.1f}{_vc_info}")
                else:
                    print(f"   [{ticker}] RICE: 計算不可 ({rice_result.note})")
        except Exception as _rice_e:
            print(f"   [{ticker}] RICE計算エラー: {_rice_e}")
            rice_result = RICEResult(
                q=0.0, cf_conversion=0.0, wacc=_rm,  # v7.3: フォールバックもRm基準に統一
                q_years=0, cf_years=0,
                avg_intensity=0.0, avg_rev_growth=0.0,
                available=False, note=f"エラー: {_rice_e}"
            )

        # ── DCF詳細（共通フォーマット） ──
        if dcf_type == "three_stage" and three_stage_result:
            dcf_components = three_stage_result.to_dict()
            pv_high = three_stage_result.pv_high_growth
            pv_terminal = three_stage_result.pv_terminal
        else:
            dcf_components = dcf_result.to_dict() if dcf_result else {}
            pv_high = dcf_result.pv_high_growth if dcf_result else 0.0
            pv_terminal = dcf_result.pv_terminal if dcf_result else 0.0

        # ── FCF一過性費用の定性評価（AI、[[FCF-OUTLIER-QUAL-1]]案B） ──
        # fcf_outlier_result.action等の決定（上記STEP4b）は既に完了済み。
        # 本ブロックはreport.txt上の参考表示専用の情報を後付けで取得する
        # だけであり、action・DCF計算（base_fcf・compose_fcf_bottom_up等、
        # いずれも上記で計算済み）には一切影響しない。transient_found=False
        # の場合はassess_transient_qualitative()内部でAPI呼び出し自体を
        # 行わずNoneを返す。AI呼び出し失敗時もNoneを返しパイプラインは継続する。
        _fcf_outlier_ai_assessment = None
        if fcf_outlier_result.transient_found:
            _fcf_outlier_ai_assessment = assess_transient_qualitative(
                ticker=ticker,
                transient_items=fcf_outlier_result.transient_items,
            )

        # ── 結果返却 ──
        result = {
            "intrinsic_value_pt": float(intrinsic_value_pt),
            "intrinsic_value_per_share": float(intrinsic_value_per_share),
            # 割引率別理論株価（v7.3: 差額分析用）
            # メイン: Rmβなし（10%）→ intrinsic_value_per_share
            # 参考①: β込みWACC
            "intrinsic_value_beta": round(float(intrinsic_value_per_share_beta), 2),
            "upside_percent_beta": _round_or_none(calculate_upside(intrinsic_value_per_share_beta, current_price), 1),
            # 参考②: Rf（リスクゼロ理論上限）
            "intrinsic_value_rf": round(float(_ivps_rf), 2),
            "upside_percent_rf": _round_or_none(_upside_rf, 1),
            "v0": float(v0),
            # [[V0-V0RM-CONFUSION-RISK-1]]対応（2026-08-30）: v0はβ込み
            # CAPM WACCベースのDCF結果（intrinsic_value_betaの計算根拠、
            # 参考①）であり、メインの理論株価（intrinsic_value_per_share）
            # の計算根拠ではない。メイン計算根拠はdcf_components.v0_rm
            # （market_return 10%固定・βなし）。latest.jsonを直接読む
            # 外部AI・レビュアーがv0からIVを積み上げ検算しようとして
            # 誤った根拠を使う罠を防ぐため、既存フィールド構成は変更せず
            # （後方互換性を維持）注記フィールドのみ追加する。
            "v0_note": "v0はβ込みCAPMベース（intrinsic_value_betaの参考値）。"
                       "メインの理論株価計算根拠はdcf_components.v0_rm"
                       "（market_return 10%固定・βなし）を参照すること。",
            "alpha": float(alpha),
            "alpha_was_capped": alpha_result.was_capped,
            "future_values": future_values,
            "return_metrics": _return_metrics,
            "upside_percent": _round_or_none(upside_percent, 1),
            "calculation_date": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
            "formula": f"Koichi式 v{self.VERSION}（動的WACC + {dcf_type} DCF + FCFベース自動判定 + 成長オプション）",
            "dcf_type": dcf_type,

            "growth": {
                "rate": round(high_growth_rate, 4),
                "source": growth_result.source,
                "phase1_years": _moat_phase1_years,  # ALPHA-REDESIGN-1: Moat Score連動
            },

            "wacc": wacc_result.to_dict(),
            "sensitivity": sensitivity_result.to_dict(),
            "growth_scenarios": {
                "primary": {"rate": high_growth_rate, "source": growth_result.source},
                "segment": growth_result.segment_detail
            },
            "scenario_valuations": scenario_result.to_dict() if scenario_result else None,
            "growth_options": go_result.to_dict(),
            "maturity_profile": maturity_profile,
            "dcf_components": {
                **dcf_components,
                "v0_rm":     float(_v0_rm),
                "pv_fcf_rm": float(_res_rm.pv_high_growth),
                "pv_tv_rm":  float(_res_rm.pv_terminal),
                # 3段階専用: Phase1/Phase2 の Rm 内訳
                **({
                    "pv_phase1_rm": float(_res_rm.pv_phase1),
                    "pv_phase2_rm": float(_res_rm.pv_phase2),
                } if hasattr(_res_rm, "pv_phase1") else {}),
            },

            # FCFベース判定結果（v6.2追加）
            "fcf_base": fcf_base_result.to_dict(),

            # FCF外れ値分析結果（v7.1追加）
            "fcf_outlier": fcf_outlier_result.to_dict(ai_assessment=_fcf_outlier_ai_assessment),
            "fcf_estimation": fcf_composition.to_dict(),
            "software_system_provisional": {
                "is_provisional": _sw_provisional,
                "note": _sw_provisional_note,
            },

            # R&D資本化補正結果（v8.2追加）
            "rd_capitalization": rd_capitalization.to_dict(),

            # RPO補正結果（v9.0追加: rpo_incremental・op_margin収録）
            "rpo_adjustment": rpo_adjustment.to_dict(),

            # BS評価補正結果（v7.0追加）
            "bs_adjustment": bs_adjustment.to_dict(),

            # RICE（投資効率指標）v8.0追加
            "rice": rice_result.to_dict(),

            # FCF/RICEデータソース記録
            "fcf_source": financials.get("fcf_source", "unknown"),
            "fcf_ttm_end": financials.get("fcf_ttm_end"),
            "fcf_ttm_periods": financials.get("fcf_ttm_periods", 0),
            "rice_data_source": financials.get("rice_data_source", "unknown"),

            "components": {
                "fcf_5yr_avg": financials.get("fcf_5yr_avg"),
                "fcf_2yr_avg": fcf_2yr_avg,
                # [[FCF-CONVRATE-LOWER-DIVERGENCE-1]]STEP6（2026-09-17、
                # Koichiさん決定3・保守的版）: 5年・2年平均とも実績FCFが
                # 負（QBTS/SOUN型、会計処理の問題ではなく実際にキャッシュを
                # 消費している構造的赤字）を検知するフラグ。IV/Classification
                # は変更せず、report.txt上でSTONKS SILOの赤字銘柄評価
                # 枠組みを参照するよう促す注記のみ追加する（保守的スコープ、
                # 5yr平均のみ負・2yr平均のみ負〈S型、改善途上〉は対象外）。
                "structural_deficit": (
                    (financials.get("fcf_5yr_avg") or 0) <= 0
                    and (fcf_2yr_avg or 0) <= 0
                ),
                "fcf_base_used": base_fcf,
                "fcf_base_method": fcf_base_result.method,
                "fcf_outlier_excl_avg": _fcf_outlier_excl_avg,
                "fcf_list_raw": fcf_list_raw,
                "diluted_shares": diluted_shares,
                "roe_10yr_avg": roe_avg,
                "roe_years_used": roe_years_used,
                "roe_outlier_adj": roe_outlier_adj,
                "current_price": current_price,
                "latest_revenue": latest_revenue,
                "rpo": rpo_latest,
                "beta": wacc_result.beta,
                "sector": sector,
                "industry": industry or "",
                "eps_data": financials.get("eps_data"),
                "_shares_source": financials.get("_shares_source"),
                "_beta_source": financials.get("_beta_source"),
                "high_growth_rate_used": high_growth_rate,
                "high_growth_years": _moat_phase1_years,  # ALPHA-REDESIGN-1
                "terminal_growth_used": terminal_growth,
                "moat_score": moat_result.moat_score,
                "moat_score_source": moat_result.source,  # [[MOAT-SCORE-PARTIAL-NULL-1]]: measured/neutral_fallback
                "moat_score_n_present": moat_result.n_present,
                "moat_phase1_years": moat_result.phase1_years,
                "moat_gross_margin_norm": moat_result.gross_margin_norm,
                "moat_roic_norm": moat_result.roic_norm,
                "moat_fcf_margin_norm": moat_result.fcf_margin_norm,
                "pv_high": pv_high,
                "pv_terminal": pv_terminal,
                "roe_used": roe_avg,
                "fcf_floor_applied": fcf_adjustment.floor_applied,
                "fcf_growth_floor": 0.15,
                "fcf_growth_cap": 0.50,
                "bear_multiplier": 0.7,
                "bull_multiplier": 1.2,
                "rpo_pv": rpo_pv,
                "growth_option_pv": growth_option_pv,
                "alpha_uncapped": alpha_result.alpha_uncapped,
                "per": financials.get("per"),
                "per_is_forward": financials.get("per_is_forward", False),
                "per_adjusted": _calc_adjusted_per(
                    ticker=ticker,
                    current_price=financials.get("current_price"),
                    eps_data_dir=self.eps_data_dir,
                ),
                "peg": financials.get("peg"),
                "ps": financials.get("ps"),
                "ev_ebitda": financials.get("ev_ebitda"),
                "ma200": financials.get("ma200"),
                "forward_eps": financials.get("forward_eps"),
                "next_quarter_eps": financials.get("next_quarter_eps"),
                "analyst_target_median": financials.get("analyst_target_median"),
                "analyst_target_mean": financials.get("analyst_target_mean"),
                "analyst_target_low": financials.get("analyst_target_low"),
                "analyst_target_high": financials.get("analyst_target_high"),
                "analyst_count": financials.get("analyst_count"),
                "analyst_rec_key": financials.get("analyst_rec_key", ""),
                "dividend_yield": financials.get("dividend_yield", 0.0),
                "payout_ratio": financials.get("payout_ratio", 0.0),
                "insider_buy_count": financials.get("insider_buy_count"),
                "insider_sell_count": financials.get("insider_sell_count"),
                "insider_net_direction": financials.get("insider_net_direction"),
                "insider_latest_date": financials.get("insider_latest_date"),
            }
        }

        return result


def _round_or_none(v, n):
    """Noneはそのまま返す（株価欠損時のupside等、MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1）。"""
    return None if v is None else round(v, n)


def _calc_adjusted_per(
    ticker: str,
    current_price: float,
    eps_data_dir: str,
) -> Optional[float]:
    """
    EPS Analyzerの調整後TTM EPS（直近4Q合計）から調整後PERを計算する。
    GAAP PER（yfinance trailingPE = TTM）と同一期間ベースで比較可能。

    4Q分の四半期データが揃わない場合はNoneを返す（年次フォールバックなし）。

    Returns:
        調整後PER（float）またはNone（データなし・EPS<=0・4Q未満の場合）
    """
    import os, json as _json
    if not eps_data_dir or current_price is None or current_price <= 0:
        return None
    q_file = os.path.join(eps_data_dir, ticker.upper(), "quarterly.json")
    if not os.path.exists(q_file):
        return None
    try:
        with open(q_file, "r", encoding="utf-8") as f:
            q_data = _json.load(f)
        quarters = q_data.get("quarters", [])
        if len(quarters) < 4:
            return None
        # quartersは新しい順に格納 → 先頭4件がTTM
        ttm_adj_eps = sum(q.get("adjusted_eps", 0) for q in quarters[:4])
        if ttm_adj_eps <= 0:
            return None
        return round(current_price / ttm_adj_eps, 2)
    except Exception:
        return None


def create_calculator(**kwargs) -> KoichiValuationCalculator:
    return KoichiValuationCalculator(**kwargs)


if __name__ == "__main__":
    calculator = KoichiValuationCalculator()

    test_data = {
        "fcf_5yr_avg": 8_234_200_000,
        "fcf_2yr_avg": 50_000_000_000,
        "diluted_shares": 10_754_251_799,
        "roe_10yr_avg": 0.182,
        "current_price": 254.17,
        "fcf_list_raw": [2e9, 3e9, 5e9, 25e9, 75e9],
        "latest_revenue": 716_924_000_000,
        "eps_data": {"ticker": "AMZN"},
        "rpo": 25_000_000_000,
        "beta": 1.38,
        "sector": "Consumer Cyclical"
    }

    result = calculator.calculate_pt(test_data)

    if "error" not in result:
        print(f"\n=== 結果 ===")
        print(f"DCFタイプ      : {result['dcf_type']}")
        print(f"FCFベース      : {result['fcf_base']['method']}  (CV={result['fcf_base']['cv']})")
        print(f"理論株価       : ${result['intrinsic_value_per_share']:.2f}")
        print(f"乖離率         : {result['upside_percent']:.1f}%")
    else:
        print(f"エラー: {result['error']}")
