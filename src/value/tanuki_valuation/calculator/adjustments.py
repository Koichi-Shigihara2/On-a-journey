"""
TANUKI VALUATION - Adjustments
FCF補正、RPO補正、α計算、成長オプション補正、FCFベース判定

責務: 各種調整・補正ロジック

v6.1 追加:
  - GrowthOptionResult / calculate_growth_option_pv()
  - calculate_intrinsic_value() に growth_option_pv 引数追加

v6.2 追加:
  - FCFBaseResult / determine_fcf_base()
    FCFリストのトレンドから5年平均 or 直近2年平均を自動判定

v8.1 追加:
  - adjust_rpo() にセクター別適用率を追加（② RPO補正精度改善）
    SaaS=100% / Fintech=50% / 保険=0% / 消費者=0%
  - BSAdjustmentResult に sector_guard フィールドを追加
  - calculate_bs_adjustment() が sector_guard を net_cash_data から受け取る
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass, field


@dataclass
class FCFAdjustmentResult:
    """FCF補正結果"""
    adjusted_fcf: float
    original_fcf: float
    floor_applied: float
    method: str  # "none" | "revenue_floor"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "adjusted_fcf": self.adjusted_fcf,
            "original_fcf": self.original_fcf,
            "floor_applied": self.floor_applied,
            "method": self.method
        }


@dataclass
class RPOAdjustmentResult:
    """RPO補正結果"""
    rpo_pv: float
    rpo_raw: float
    discount_rate: float
    assumed_years: float
    applied: bool
    application_rate: float = 1.0   # v8.1: セクター別適用率（0.0〜1.0）
    sector_category: str = ""       # v8.1: 判定セクターカテゴリ
    rpo_incremental: float = 0.0   # 非連続RPO額（前年比成長超過分）
    op_margin: float = 0.0         # 使用した営業利益率（TTMベース）
    exclusion_reason: str = ""     # 比率条件除外時の理由 e.g. "RPO/Revenue=0.12 < 0.3, not applied"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rpo_pv": self.rpo_pv,
            "rpo_raw": self.rpo_raw,
            "discount_rate": self.discount_rate,
            "assumed_realization_years": self.assumed_years,
            "applied": self.applied,
            "application_rate": self.application_rate,
            "sector_category": self.sector_category,
            "rpo_incremental": self.rpo_incremental,
            "op_margin": self.op_margin,
            "exclusion_reason": self.exclusion_reason,
        }


@dataclass
class AlphaResult:
    """α計算結果"""
    alpha: float
    alpha_uncapped: float
    was_capped: bool
    roe: float
    retention_rate: float
    wacc: float
    g_individual: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alpha": self.alpha,
            "alpha_uncapped": self.alpha_uncapped,
            "was_capped": self.was_capped,
            "roe": self.roe,
            "retention_rate": self.retention_rate,
            "wacc": self.wacc,
            "g_individual": self.g_individual
        }


@dataclass
class MoatScoreResult:
    """Moat Score計算結果"""
    moat_score: float
    phase1_years: int
    # [[MOAT-SCORE-PARTIAL-NULL-1]]: 指標が欠損（除外）の場合はNone。
    # 実測値0との区別を可能にするため2026-08-16にOptional化した
    # （従来はfloat固定で欠損時も便宜上0.5等の数値を入れていた）。
    gross_margin_norm: Optional[float]
    roic_norm: Optional[float]
    fcf_margin_norm: Optional[float]
    # [[MOAT-SCORE-PARTIAL-NULL-1]]（2026-08-16追記）: moat_score=0.5が
    # 「実測の結果0.5」か「最低2指標ルールによる中立フォールバック」かを
    # 区別するprovenance（NAMING_CONVENTIONS.md規則4準拠）。
    # "measured"（有効指標2件以上で算出）/ "neutral_fallback"
    # （有効指標1件以下のためmoat_score=0.5をプレースホルダとして採用）。
    source: str = "measured"
    n_present: int = 0


@dataclass
class GrowthOptionResult:
    """成長オプション（仮説セグメント）PV計算結果"""
    total_pv: float
    options: List[Dict[str, Any]]
    count: int
    applied: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_pv": self.total_pv,
            "count": self.count,
            "applied": self.applied,
            "options": [
                {
                    "name": o["name"],
                    "tam": o["tam"],
                    "penetration": o["penetration"],
                    "fcf_margin": o["fcf_margin"],
                    "probability": o["probability"],
                    "delay_years": o["delay_years"],
                    "expected_fcf": o["expected_fcf"],
                    "pv": o["pv"],
                    "note": o.get("note", "")
                }
                for o in self.options
            ]
        }


@dataclass
class FCFBaseResult:
    """
    FCFベース判定結果 v6.3

    DCFの出発点となるベースFCFと、その選択根拠を保持する。

    v6.3変更: ratio方式 → CV（変動係数）方式
        recent_2yr がデフォルト。FCFが安定している場合のみ avg_5yr を使用。
    """
    base_fcf: float       # 採用したベースFCF
    method: str           # "avg_5yr" | "recent_2yr"
    fcf_5yr_avg: float    # 5年平均（参考値）
    fcf_2yr_avg: float    # 直近2年平均（参考値）
    cv: float             # 変動係数 std/|mean|（安定性の指標）
    cv_threshold: float   # CV判定閾値

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_fcf": self.base_fcf,
            "method": self.method,
            "fcf_5yr_avg": self.fcf_5yr_avg,
            "fcf_2yr_avg": self.fcf_2yr_avg,
            "cv": round(self.cv, 3),
            "cv_threshold": self.cv_threshold,
            # 後方互換: ratio/thresholdはcv/cv_thresholdのエイリアス
            "ratio": round(self.cv, 3),
            "threshold": self.cv_threshold
        }


# ========================================
# FCFベース自動判定 v6.3（CV方式）
# ========================================

DEFAULT_FCF_CV_THRESHOLD = 0.5  # CV≤この値なら安定→avg_5yr

def determine_fcf_base(
    fcf_5yr_avg: float,
    fcf_2yr_avg: float,
    fcf_list: List[float],
    threshold: float = DEFAULT_FCF_CV_THRESHOLD
) -> FCFBaseResult:
    """
    FCFの変動係数（CV）で安定性を判定し、ベースFCFを自動選択

    判定ロジック（v6.3）:
        CV = std(fcf_list) / |mean(fcf_list)|
        CV ≤ threshold（安定） → avg_5yr（成熟企業: KO, MSFT等）
        CV > threshold（不安定）→ recent_2yr（成長企業: NVDA, AMD等）

    旧ratio方式との違い:
        旧: 5年平均がデフォルト、急成長時だけ2年平均
        新: 2年平均がデフォルト、安定時だけ5年平均
        → 成長企業のFCF過小評価を構造的に解消

    特殊ケース（CVより優先）:
        fcf_2yr_avg ≤ 0（直近赤字）  → avg_5yr（FCF補正に委ねる）
        fcf_5yr_avg ≤ 0（過去赤字含む）→ recent_2yr
        データ不足（< 3年）           → recent_2yr（保守的にデフォルト）

    Args:
        fcf_5yr_avg  : 5年平均FCF
        fcf_2yr_avg  : 直近2年平均FCF
        fcf_list     : FCFリスト（CV計算用）
        threshold    : CV閾値（デフォルト0.5）

    Returns:
        FCFBaseResult
    """
    import statistics

    # ── 特殊ケース（データ不足）──
    if len(fcf_list) < 3:
        return FCFBaseResult(
            base_fcf=fcf_2yr_avg if fcf_2yr_avg > 0 else fcf_5yr_avg,
            method="recent_2yr" if fcf_2yr_avg > 0 else "avg_5yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            cv=999.0,
            cv_threshold=threshold
        )

    # ── 特殊ケース（直近赤字）──
    # ①2年平均がマイナス または ②直近1年がマイナス → avg_5yrにフォールバック
    # CELHのような「最新年マイナス・前年プラス」で平均がほぼゼロになるケースを防ぐ
    latest_year_negative = len(fcf_list) >= 1 and fcf_list[0] < 0
    if fcf_2yr_avg <= 0 or latest_year_negative:
        return FCFBaseResult(
            base_fcf=fcf_5yr_avg,
            method="avg_5yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            cv=999.0,
            cv_threshold=threshold
        )

    # ── 特殊ケース（直近2年平均が5年平均の15%未満）──
    # 例: 1年黒字・1年赤字でほぼゼロに見える場合（LITEのような一時低迷後回復型）
    # 見かけ上の近ゼロ平均は実力を著しく過小評価するため5年平均にフォールバック
    if fcf_5yr_avg > 0 and 0 < fcf_2yr_avg < fcf_5yr_avg * 0.15:
        return FCFBaseResult(
            base_fcf=fcf_5yr_avg,
            method="avg_5yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            cv=999.0,
            cv_threshold=threshold
        )

    # ── 特殊ケース（過去赤字含む）──
    if fcf_5yr_avg <= 0:
        return FCFBaseResult(
            base_fcf=fcf_2yr_avg,
            method="recent_2yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            cv=999.0,
            cv_threshold=threshold
        )

    # ── CV計算 ──
    try:
        mean_fcf = abs(statistics.mean(fcf_list))
        std_fcf  = statistics.stdev(fcf_list)
        cv = std_fcf / mean_fcf if mean_fcf > 0 else 999.0
    except Exception:
        cv = 999.0

    # ── TANUKI-DCF-1① FCF減少トレンド判定 ──
    # fcf_list[0]=直近, fcf_list[-1]=最古。CAGR<-5%なら直近値を基準に使用
    # ただし回復途上（直近>3yr平均 or 前期比+20%以上）はavg_5yr維持
    _n = len(fcf_list) - 1
    # 最古値が負（先行投資期）の場合はCAGR計算不能 → スキップしてCV判定に委ねる
    _cagr = (fcf_list[0] / fcf_list[-1]) ** (1 / _n) - 1 if fcf_list[-1] > 0 else None
    if _cagr is not None and _cagr < -0.05:
        _fcf_3yr_avg = sum(fcf_list[:3]) / 3
        _prev = fcf_list[1] if len(fcf_list) >= 2 else fcf_list[0]
        _recovering_vs_mid = fcf_list[0] > _fcf_3yr_avg
        _recovering_vs_prev = _prev > 0 and (fcf_list[0] / _prev - 1) >= 0.20
        if _recovering_vs_mid or _recovering_vs_prev:
            return FCFBaseResult(
                base_fcf=fcf_5yr_avg,
                method="avg_5yr_recovery",
                fcf_5yr_avg=fcf_5yr_avg,
                fcf_2yr_avg=fcf_2yr_avg,
                cv=cv,
                cv_threshold=threshold,
            )
        else:
            return FCFBaseResult(
                base_fcf=fcf_list[0],
                method="recent_1yr",
                fcf_5yr_avg=fcf_5yr_avg,
                fcf_2yr_avg=fcf_2yr_avg,
                cv=cv,
                cv_threshold=threshold,
            )

    # ── CV判定 ──
    if cv <= threshold:
        # 安定 → avg_5yr（成熟企業）
        return FCFBaseResult(
            base_fcf=fcf_5yr_avg,
            method="avg_5yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            cv=cv,
            cv_threshold=threshold
        )
    else:
        # 不安定・成長 → recent_2yr
        return FCFBaseResult(
            base_fcf=fcf_2yr_avg,
            method="recent_2yr",
            fcf_5yr_avg=fcf_5yr_avg,
            fcf_2yr_avg=fcf_2yr_avg,
            cv=cv,
            cv_threshold=threshold
        )


# ========================================
# 既存関数（変更なし）
# ========================================

def adjust_fcf(
    fcf_avg: float,
    latest_revenue: float,
    revenue_floor_ratio: float = 0.08
) -> FCFAdjustmentResult:
    """FCF補正（マイナスFCF対応）"""
    if fcf_avg > 0:
        return FCFAdjustmentResult(
            adjusted_fcf=fcf_avg,
            original_fcf=fcf_avg,
            floor_applied=0.0,
            method="none"
        )

    if latest_revenue <= 0:
        return FCFAdjustmentResult(
            adjusted_fcf=fcf_avg,
            original_fcf=fcf_avg,
            floor_applied=0.0,
            method="none"
        )

    fcf_floor = latest_revenue * revenue_floor_ratio
    adjusted_fcf = max(fcf_avg, fcf_floor)
    floor_applied = adjusted_fcf - fcf_avg

    return FCFAdjustmentResult(
        adjusted_fcf=adjusted_fcf,
        original_fcf=fcf_avg,
        floor_applied=floor_applied,
        method="revenue_floor"
    )


def _is_insurance(ticker: str, sector: Optional[str], industry: str) -> bool:
    """
    保険会社かどうかを判定する（v8.1 industry優先方式）

    判定優先順位:
      1. industry文字列に "Insurance" / "Health Insurance" 等が含まれる → 保険
      2. ticker ブラックリスト（yfinanceが別sectorを返すことがある銘柄）
      3. sector == "Insurance"

    yfinanceのindustry文字列例:
      保険系: "Insurance—Life", "Insurance—Property & Casualty",
              "Insurance—Specialty", "Insurance Brokers",
              "Health Insurance", "Managed Health Care"
      非保険: "Health Care Plans"（OSCRはこれになる可能性）
    """
    # 1. industry文字列チェック（最優先・最も正確）
    if industry:
        industry_lower = industry.lower()
        if "insurance" in industry_lower or "managed health" in industry_lower:
            return True

    # 2. tickerブラックリスト（yfinanceが誤ったsectorを返す実績銘柄）
    INSURANCE_TICKERS = {
        "UNH", "CVS", "CI", "HUM", "ELV", "CNC", "MOH",   # 医療保険
        "MET", "PRU", "AFL", "ALL", "TRV", "CB", "HIG",    # 生保・損保
        "PGR", "AIZ", "CINF", "AIG", "L", "GL",            # 損保・複合
    }
    if ticker.upper() in INSURANCE_TICKERS:
        return True

    # 3. sectorフォールバック
    return sector == "Insurance"


def resolve_rpo_config_path() -> Optional[str]:
    """rpo_config.jsonのパス解決ロジック（report_consistency_check.pyの
    設定ファイル読み込み横断チェックと共用するため、2026-08-16に
    _load_rpo_config()から切り出した。[[CONFIG-LOAD-SILENT-FALLBACK-1]]、
    resolve_fcf_conversion_config_path()と同型パターン）。

    Returns:
        解決できたパス（存在確認済み）、解決できなければNone
    """
    config_path = str(Path(__file__).parents[4] / "config" / "rpo_config.json")
    return config_path if os.path.exists(config_path) else None


def _load_rpo_config() -> Dict[str, Any]:
    """rpo_config.json を読み込む（存在しない場合はデフォルト値を返す）"""
    config_path = resolve_rpo_config_path()
    try:
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "whitelist": {
                "CRM": {}, "NOW": {}, "PLTR": {}, "MSFT": {},
                "SNOW": {}, "DDOG": {}, "ZS": {}, "CRWD": {},
                "NET": {}, "TEAM": {}, "HUBS": {}, "MDB": {},
                "ESTC": {}, "BILL": {}, "GTLB": {},
            },
            "industry_keywords": ["software", "cloud", "saas"],
            "min_rpo_revenue_ratio": 0.30,
        }


def _get_rpo_application_rate(
    sector: Optional[str], ticker: str, industry: str = "",
    rpo_cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[float, str, bool]:
    """
    セクター別RPO適用率を返す（v9.2: via_whitelist フラグ追加）

    Returns: (application_rate, sector_category, via_whitelist)
      via_whitelist=True → 比率条件チェックをスキップ（明示登録済み）

    RPOは「将来の収益確実性」の代理指標:
      - SaaS/クラウド (100%): サブスクリプション契約バックログ → 収益化確実
      - Fintech (50%):        将来収益見通し → 不確実性中程度
      - 保険 (0%):            保険準備金 → 収益でなく義務
      - その他 (0%):          製品デリバリー完了後が多い

    判定順: config whitelist → 保険判定(_is_insurance) → industry keyword → sector fallback
    """
    if rpo_cfg is None:
        rpo_cfg = _load_rpo_config()
    whitelist: Dict[str, Any] = rpo_cfg.get("whitelist", {})
    industry_keywords: list = rpo_cfg.get("industry_keywords", ["software", "cloud", "saas"])

    # configホワイトリスト優先（比率チェック免除）
    if ticker.upper() in whitelist:
        return 1.0, "SaaS", True

    # 保険判定（industry優先）
    if _is_insurance(ticker, sector, industry):
        return 0.0, "Insurance", False

    # Fintech
    if sector == "Financial Services":
        return 0.5, "Fintech", False

    # industry文字列でSaaS/Tech系を追加判定（configキーワードのみ使用）
    if industry:
        industry_lower = industry.lower()
        if any(kw in industry_lower for kw in industry_keywords):
            if sector in ("Technology", "Communication Services"):
                return 1.0, "SaaS", False

    SECTOR_RATES: Dict[str, Tuple[float, str]] = {
        "Technology": (0.0, "Non-SaaS"),          # whitelist/keywordなしはNon-SaaS
        "Communication Services": (1.0, "SaaS"),
        "Consumer Cyclical": (0.0, "Consumer"),
        "Consumer Defensive": (0.0, "Consumer"),
        "Industrials": (0.0, "Non-SaaS"),
        "Energy": (0.0, "Non-SaaS"),
        "Utilities": (0.0, "Non-SaaS"),
        "Real Estate": (0.0, "Non-SaaS"),
        "Basic Materials": (0.0, "Non-SaaS"),
        "Healthcare": (0.0, "Healthcare"),
    }

    if sector:
        rate, category = SECTOR_RATES.get(sector, (0.0, "Non-SaaS"))
        return rate, category, False

    return 0.0, "Unknown", False


def adjust_rpo(
    rpo: float,
    discount_rate: float = 0.15,
    assumed_realization_years: float = 1.5,
    sector: Optional[str] = None,
    ticker: str = "",
    industry: str = "",          # v8.1: industry優先判定
    op_margin: float = 0.0,      # 追加: 営業利益率（TTM）
    rpo_yago: Optional[float] = None,  # 追加: 前年同期RPO残高
    rev_yoy: Optional[float] = None,   # 追加: Revenue前年比成長率
    rev_ttm: Optional[float] = None,   # 追加: TTM Revenue（前年比なし時の代替）
) -> RPOAdjustmentResult:
    """
    RPO補正（残存履行義務の現在価値化）v9.0

    3重バグ修正:
      ① 非連続RPO: 前年比成長超過分のみを補正対象とする
         前年比あり: rpo_incremental = max(0, rpo - rpo_yago*(1+rev_yoy))
         前年比なし: rpo_incremental = max(0, rpo - rev_ttm*1.0)
      ② 利益率補正: 赤字（op_margin<=0）はrpo_pv=0
         rpo_pv = effective_rpo * op_margin / (1+discount_rate)^years
      ③ α外出し: rpo_pvはcalculate_intrinsic_value()でαの外に加算

    セクター別適用率（v8.1継承）:
        SaaS (100%) / Fintech (50%) / 保険・消費者 (0%)
    """
    if rpo <= 0:
        return RPOAdjustmentResult(
            rpo_pv=0.0, rpo_raw=0.0,
            discount_rate=discount_rate,
            assumed_years=assumed_realization_years,
            applied=False, application_rate=1.0, sector_category="",
            rpo_incremental=0.0, op_margin=op_margin,
        )

    rpo_cfg = _load_rpo_config()
    application_rate, sector_category, via_whitelist = _get_rpo_application_rate(
        sector, ticker, industry, rpo_cfg=rpo_cfg
    )

    if application_rate == 0.0:
        return RPOAdjustmentResult(
            rpo_pv=0.0, rpo_raw=rpo,
            discount_rate=discount_rate,
            assumed_years=assumed_realization_years,
            applied=False, application_rate=0.0,
            sector_category=sector_category,
            rpo_incremental=0.0, op_margin=op_margin,
        )

    # 比率条件ゲート（whitelist登録済みは免除）
    # whitelist以外で RPO/Revenue < min_ratio の場合は適用しない
    if not via_whitelist and rev_ttm is not None and rev_ttm > 0:
        rpo_rev_ratio = rpo / rev_ttm
        min_ratio: float = rpo_cfg.get("min_rpo_revenue_ratio", 0.30)
        if rpo_rev_ratio < min_ratio:
            excl = f"RPO/Revenue={rpo_rev_ratio:.2f} < {min_ratio}, not applied"
            return RPOAdjustmentResult(
                rpo_pv=0.0, rpo_raw=rpo,
                discount_rate=discount_rate,
                assumed_years=assumed_realization_years,
                applied=False, application_rate=application_rate,
                sector_category=sector_category,
                rpo_incremental=0.0, op_margin=op_margin,
                exclusion_reason=excl,
            )

    # ① 非連続RPO計算（前年比成長超過分のみ補正対象）
    SAAS_NORMAL_RPO_REV_RATIO = 1.0
    if rpo_yago is not None and rev_yoy is not None:
        rpo_incremental = max(0.0, rpo - rpo_yago * (1 + rev_yoy))
    elif rev_ttm is not None and rev_ttm > 0:
        rpo_incremental = max(0.0, rpo - rev_ttm * SAAS_NORMAL_RPO_REV_RATIO)
    else:
        rpo_incremental = 0.0

    # ② 利益率補正（赤字はrpo_pv=0）
    if op_margin <= 0:
        rpo_pv = 0.0
    else:
        effective_rpo = rpo_incremental * application_rate
        rpo_pv = effective_rpo * op_margin / (1 + discount_rate) ** assumed_realization_years

    return RPOAdjustmentResult(
        rpo_pv=rpo_pv, rpo_raw=rpo,
        discount_rate=discount_rate,
        assumed_years=assumed_realization_years,
        applied=rpo_pv > 0,
        application_rate=application_rate,
        sector_category=sector_category,
        rpo_incremental=rpo_incremental,
        op_margin=op_margin,
    )


def calculate_alpha(
    roe: float,
    wacc: float,
    retention_rate: float = 0.60,
    alpha_cap: float = 1.0,
    discount_factor: float = 0.7
) -> AlphaResult:
    """α（成長期待プレミアム）計算"""
    g_individual = max(0.0, roe * retention_rate)

    if wacc <= 0:
        alpha_raw = 0.0
    else:
        alpha_raw = (g_individual / wacc) * discount_factor

    alpha_uncapped = max(0.0, alpha_raw)
    alpha = min(alpha_cap, alpha_uncapped)

    return AlphaResult(
        alpha=alpha,
        alpha_uncapped=alpha_uncapped,
        was_capped=alpha_uncapped > alpha_cap,
        roe=roe,
        retention_rate=retention_rate,
        wacc=wacc,
        g_individual=g_individual
    )


_MOAT_WEIGHTS = {"gm": 0.40, "roic": 0.40, "fcf": 0.20}


def calculate_moat_score(
    gross_margin_3yr_avg: Optional[float],
    roic: Optional[float],
    fcf_margin_3yr_avg: Optional[float],
    roic_reason: str = "ok",
    rm: float = 0.10,
) -> MoatScoreResult:
    """Moat Score計算（粗利率・ROIC超過幅・FCFマージンの加重平均）

    各指標を絶対値基準で正規化し、Phase1成長期間（3〜10年）を自動決定する。

    [[MOAT-SCORE-PARTIAL-NULL-1]]（2026-08-16、`(値 or 0.0)`問題の解消）:
    欠損指標を「実測値ゼロ」として平均に混入させず、Noneとして除外した上で
    残りの指標のみで加重平均する（重みを再正規化）。ただしroicがNoneの
    原因によって扱いを変える（`roic_reason`、`_calc_roic_wacc_ratio()`の
    戻り値2番目の要素）:
      - "reported_negative_oi"（真の赤字でnopat<=0確認済み）→
        roic_norm=0.0で**算入**（弱いモートという実態を正しく反映。
        [[OPERATING-INCOME-EXTRACTION-GAP-1]]により、この理由コードは
        operating_income自体が取得できた上での赤字のみを指すため、
        「真に欠損」との混同は起きない）
      - "roic_diverged_over10"（ROIC>=1000%、測定アーティファクトの疑い）
        → roic_norm=1.0（上限クランプ）で**算入**。2026-08-16時点で該当
        銘柄0件のため実データでは未検証（将来該当した場合に備えた設計）
      - それ以外（missing_cash_data/negative_invested_capital/
        no_operating_income等、測定不能）→ **除外**（重み再正規化の対象）

    **最低2指標ルール**: 有効指標（gm/roic/fcf のうち実際に算入される
    もの）が2未満の場合、`moat_score=0.5`（中央値）にフォールバックする
    （旧実装の「3指標全欠損時のデフォルト」を一般化）。根拠:
    「薄い根拠から確信ありげな出力を出さない」という原則を、高スコア側
    （1指標だけで1.0に振れる）・低スコア側（1指標だけで0付近に沈む）の
    両方に対称的に適用するため。

    各norm値（gross_margin_norm等）は、その指標が実際に算入された場合の
    み数値を返し、除外された場合はNoneを返す（呼び出し元は`:.2f`等での
    フォーマット時にNone考慮が必要）。
    """
    def _clamp(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))

    gm_norm: Optional[float] = None
    if gross_margin_3yr_avg is not None:
        gm_norm = _clamp(gross_margin_3yr_avg / 1.0, 0.0, 1.0)

    fcf_norm: Optional[float] = None
    if fcf_margin_3yr_avg is not None:
        fcf_norm = _clamp(fcf_margin_3yr_avg / 0.30, 0.0, 1.0)

    roic_norm: Optional[float] = None
    if roic is not None:
        roic_norm = _clamp((roic - rm) / 0.30, 0.0, 1.0)
    elif roic_reason == "reported_negative_oi":
        roic_norm = 0.0
    elif roic_reason == "roic_diverged_over10":
        roic_norm = 1.0
    # それ以外の理由（missing_cash_data等）はroic_norm=Noneのまま（除外）

    present = {
        k: v for k, v in (("gm", gm_norm), ("roic", roic_norm), ("fcf", fcf_norm))
        if v is not None
    }

    if len(present) < 2:
        moat_score = 0.5
    else:
        w_sum = sum(_MOAT_WEIGHTS[k] for k in present)
        moat_score = sum(v * _MOAT_WEIGHTS[k] for k, v in present.items()) / w_sum

    phase1_years = 3 + round(moat_score * 7)
    source = "measured" if len(present) >= 2 else "neutral_fallback"

    return MoatScoreResult(
        moat_score=round(moat_score, 4),
        phase1_years=phase1_years,
        gross_margin_norm=round(gm_norm, 4) if gm_norm is not None else None,
        roic_norm=round(roic_norm, 4) if roic_norm is not None else None,
        fcf_margin_norm=round(fcf_norm, 4) if fcf_norm is not None else None,
        source=source,
        n_present=len(present),
    )


def calculate_intrinsic_value(
    v0: float,
    rpo_pv: float,
    alpha: float,
    growth_option_pv: float = 0.0
) -> float:
    """本質的価値（P_t）計算
    P_t = V0 × (1 + α) + rpo_pv + growth_option_pv
    rpo_pvはαの外に出すことで成長プレミアムの二重適用を防ぐ（v9.0修正）
    """
    intrinsic_value_pt = v0 * (1 + alpha) + rpo_pv + growth_option_pv
    return intrinsic_value_pt


def calculate_per_share_value(
    intrinsic_value_pt: float,
    diluted_shares: int
) -> float:
    """1株あたり本質的価値計算"""
    if diluted_shares <= 0:
        return 0.0
    return intrinsic_value_pt / diluted_shares


def calculate_upside(
    intrinsic_value_per_share: float,
    current_price: float
) -> float:
    """乖離率計算"""
    if current_price <= 0:
        return 0.0
    return ((intrinsic_value_per_share / current_price) - 1) * 100


def calculate_growth_option_pv(ticker: str) -> GrowthOptionResult:
    """仮説セグメント（成長オプション）の合計PVを計算"""
    try:
        from segment_config import calculate_growth_option_total_pv
        result = calculate_growth_option_total_pv(ticker)
        return GrowthOptionResult(
            total_pv=result["total_pv"],
            options=result["options"],
            count=result["count"],
            applied=result["count"] > 0
        )
    except ImportError:
        return GrowthOptionResult(
            total_pv=0.0,
            options=[],
            count=0,
            applied=False
        )


# ========================================
# BS評価補正 v7.0 新規
# ========================================

@dataclass
class BSAdjustmentResult:
    """
    BSネットキャッシュ補正結果

    理論株価への加算方式:
        PT = DCF理論株価 + net_cash_per_share
    純負債の場合はマイナス（理論株価を引き下げ）
    """
    cash: float                    # 現金・現金同等物
    short_term_investments: float  # 短期投資
    long_term_debt: float          # 長期有利子負債
    short_term_debt: float         # 短期有利子負債
    net_cash: float                # ネットキャッシュ（+/-）
    net_cash_per_share: float      # 1株あたりネットキャッシュ
    fiscal_year: int               # 取得会計年度
    applied: bool                  # 補正適用フラグ
    sector_guard: str = "none"     # v8.1: 適用したセクターガード名
    net_debt_period: str = ""      # ARCH-DATA-1残課題①: 実際にBS項目を取得した時点のラベル
    # NVDA-STI-TAG-UNIDENTIFIED-1: short_term_investmentsがticker_restrictions
    # のcross_filing_tags経由の複数タグ合算近似値である場合True・その残差率
    sti_approximated: bool = False
    sti_residual_pct: Optional[float] = None
    # [[BBAI-RDW-RUNWAY-VERIFICATION-1]]後続: 四半期優先分岐で採用した四半期に
    # short_term_investmentsが存在しなかった（0として計算）場合True。
    # 実測ゼロ（False）と区別する（reader.py::get_net_cash()参照）
    sti_quarterly_missing: bool = False
    # FY52WEEK-BS-FADEOUT-FALLBACK-1: 最新年度が完全欠損（None）かつ過去の
    # 直近既知値が明示的0だった場合に真のゼロと推定した場合True・その最終
    # 確認年度。年数閾値なし。直近既知値が非ゼロの場合は適用されない
    # （[[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]参照）
    sti_estimated_zero: bool = False
    sti_last_confirmed_zero_year: Optional[int] = None
    ltdebt_estimated_zero: bool = False
    ltdebt_last_confirmed_zero_year: Optional[int] = None
    stdebt_estimated_zero: bool = False
    stdebt_last_confirmed_zero_year: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cash":                   self.cash,
            "short_term_investments": self.short_term_investments,
            "long_term_debt":         self.long_term_debt,
            "short_term_debt":        self.short_term_debt,
            "net_cash":               self.net_cash,
            "net_cash_per_share":     self.net_cash_per_share,
            "fiscal_year":            self.fiscal_year,
            "applied":                self.applied,
            "sector_guard":           self.sector_guard,
            "net_debt_period":        self.net_debt_period,
            "sti_approximated":       self.sti_approximated,
            "sti_residual_pct":       self.sti_residual_pct,
            "sti_quarterly_missing":  self.sti_quarterly_missing,
            "sti_estimated_zero":     self.sti_estimated_zero,
            "sti_last_confirmed_zero_year": self.sti_last_confirmed_zero_year,
            "ltdebt_estimated_zero":  self.ltdebt_estimated_zero,
            "ltdebt_last_confirmed_zero_year": self.ltdebt_last_confirmed_zero_year,
            "stdebt_estimated_zero":  self.stdebt_estimated_zero,
            "stdebt_last_confirmed_zero_year": self.stdebt_last_confirmed_zero_year,
        }


def calculate_bs_adjustment(
    net_cash_data: dict,
    diluted_shares: int
) -> BSAdjustmentResult:
    """
    BSネットキャッシュ補正値を計算

    Args:
        net_cash_data: SECReader.get_net_cash()の返却値
                       v8.1以降は sector_guard フィールドを含む
        diluted_shares: 希薄化後株式数

    Returns:
        BSAdjustmentResult
    """
    net_cash = net_cash_data.get("net_cash", 0.0)
    available = net_cash_data.get("available", False)

    net_cash_per_share = (
        net_cash / diluted_shares
        if diluted_shares > 0 and available
        else 0.0
    )

    return BSAdjustmentResult(
        cash=net_cash_data.get("cash", 0.0),
        short_term_investments=net_cash_data.get("short_term_investments", 0.0),
        long_term_debt=net_cash_data.get("long_term_debt", 0.0),
        short_term_debt=net_cash_data.get("short_term_debt", 0.0),
        net_cash=net_cash,
        net_cash_per_share=net_cash_per_share,
        fiscal_year=net_cash_data.get("fiscal_year", 0),
        applied=available and net_cash != 0.0,
        sector_guard=net_cash_data.get("sector_guard", "none"),  # v8.1
        net_debt_period=net_cash_data.get("net_debt_period", ""),  # ARCH-DATA-1残課題①
        sti_approximated=net_cash_data.get("sti_approximated", False),  # NVDA-STI-TAG-UNIDENTIFIED-1
        sti_residual_pct=net_cash_data.get("sti_residual_pct"),
        sti_quarterly_missing=net_cash_data.get("sti_quarterly_missing", False),
        # FY52WEEK-BS-FADEOUT-FALLBACK-1
        sti_estimated_zero=net_cash_data.get("sti_estimated_zero", False),
        sti_last_confirmed_zero_year=net_cash_data.get("sti_last_confirmed_zero_year"),
        ltdebt_estimated_zero=net_cash_data.get("ltdebt_estimated_zero", False),
        ltdebt_last_confirmed_zero_year=net_cash_data.get("ltdebt_last_confirmed_zero_year"),
        stdebt_estimated_zero=net_cash_data.get("stdebt_estimated_zero", False),
        stdebt_last_confirmed_zero_year=net_cash_data.get("stdebt_last_confirmed_zero_year"),
    )



# ========================================
# FCF外れ値分析 v7.1（EPSアナライザー連携）
# ========================================

# 一過性費用として認識するカテゴリ
TRANSIENT_CATEGORIES = {
    "リストラ・事業再編関連",
    "在庫・サプライチェーン関連",
    "金融関連",
}

# FCF外れ値判定の閾値（CV区分別）
FCF_OUTLIER_THRESHOLDS = {
    "mature":  0.20,   # CV≤0.5（成熟企業）: 5年平均から±20%超で外れ値候補
    "growth":  0.60,   # CV>0.5 （成長企業）: 5年平均から±60%超で外れ値候補
}


@dataclass
class FCFOutlierResult:
    """
    FCF外れ値分析結果

    detected     : 外れ値ルールがトリガーされたか
    rule         : トリガーされたルール名
    fiscal_year  : 対象会計年度（fcf_list[0]の年度）
    fcf_value    : 問題のFCF値
    threshold_pct: 適用した閾値（%）
    deviation_pct: 5年平均からの乖離%（DCF-REL-SYNC-1）。
                   rule="latest_negative"（FCFマイナス型）は乖離%の概念が
                   成立しないためNone。note文字列に埋め込む値と同一の計算式。
    transient_found   : EPSアナライザーで一過性費用が確認されたか
    transient_items   : 一過性費用の詳細リスト
    transient_total   : 一過性費用の合計（税前）
    action       : "excluded" | "flagged" | "none"
    note         : 人間が読める説明文
    """
    detected: bool
    rule: str                    # "none" | "latest_negative" | "deviation_large"
    fiscal_year: int
    fcf_value: float
    threshold_pct: float         # 適用した乖離閾値（%）
    transient_found: bool
    transient_items: List[Dict[str, Any]]
    transient_total: float       # 一過性費用の合計（税前）
    action: str                  # "excluded" | "flagged" | "none"
    note: str
    deviation_pct: Optional[float] = None  # DCF-REL-SYNC-1: 5年平均からの乖離%（latest_negative型はNone）

    def to_dict(self, ai_assessment: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Args:
            ai_assessment: [[FCF-OUTLIER-QUAL-1]]（案B）で追加。一過性費用の
                内容（構造的問題の兆候か一時的な問題か）に関するAI定性評価。
                report.txt上の参考表示専用で、action判定・DCF計算には
                一切使用しない（呼び出し元がself.action等を決定した"後"に
                任意で付与する情報のため、本メソッドの引数として独立させ、
                FCFOutlierResult自体やanalyze_fcf_outlier()の判定ロジックは
                変更しない設計）。
        """
        return {
            "detected": self.detected,
            "rule": self.rule,
            "fiscal_year": self.fiscal_year,
            "fcf_value": self.fcf_value,
            "threshold_pct": self.threshold_pct,
            "deviation_pct": self.deviation_pct,
            "transient_evidence": {
                "found": self.transient_found,
                "source": "adjusted_eps_analyzer" if self.transient_found else None,
                "items": self.transient_items,
                "total_transient_amount": self.transient_total,
                "ai_assessment": ai_assessment,
            },
            "action": self.action,
            "note": self.note,
        }


def _load_eps_annual(ticker: str, eps_data_dir: str) -> Optional[Dict[str, Any]]:
    """
    EPSアナライザーのannual.jsonを読み込む

    パス: docs/value-monitor/adjusted_eps_analyzer/data/{TICKER}/annual.json
    """
    import os, json
    path = os.path.join(eps_data_dir, ticker.upper(), "annual.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _find_transient_items(
    eps_annual: Dict[str, Any],
    target_year: int,
) -> List[Dict[str, Any]]:
    """
    指定年度のEPSアナライザー調整項目から一過性費用を抽出

    Returns:
        一過性費用の詳細リスト（カテゴリが TRANSIENT_CATEGORIES に該当するもの）
    """
    items = []
    for yr in eps_annual.get("years", []):
        # 年度文字列 "2025" を int 2025 として比較
        try:
            yr_int = int(yr.get("year", 0))
        except (ValueError, TypeError):
            continue
        if yr_int != target_year:
            continue

        for adj in yr.get("adjustments", []):
            if adj.get("category") in TRANSIENT_CATEGORIES:
                items.append({
                    "category":  adj["category"],
                    "item_name": adj.get("item_name", ""),
                    "amount":    adj.get("amount", 0),
                    "reason":    adj.get("reason", ""),
                })
    return items


def analyze_fcf_outlier(
    ticker: str,
    fcf_list: List[float],
    fcf_5yr_avg: float,
    cv: float,
    fiscal_year_of_latest: int,
    eps_data_dir: str = "",
    cv_threshold: float = DEFAULT_FCF_CV_THRESHOLD,
) -> FCFOutlierResult:
    """
    FCF外れ値を分析し、EPSアナライザーと突合して一過性判定を行う

    Args:
        ticker             : ティッカーシンボル
        fcf_list           : FCFリスト（新しい順、fcf_list[0]が直近）
        fcf_5yr_avg        : 5年平均FCF
        cv                 : 変動係数（determine_fcf_baseで計算済み）
        fiscal_year_of_latest: fcf_list[0]の会計年度（例: 2025）
        eps_data_dir       : EPSアナライザーのdataディレクトリパス
        cv_threshold       : 成熟/成長の分岐CV閾値

    Returns:
        FCFOutlierResult
    """
    NO_OUTLIER = FCFOutlierResult(
        detected=False, rule="none",
        fiscal_year=fiscal_year_of_latest,
        fcf_value=fcf_list[0] if fcf_list else 0,
        threshold_pct=0.0,
        transient_found=False, transient_items=[], transient_total=0.0,
        action="none", note=""
    )

    if not fcf_list or fcf_5yr_avg == 0:
        return NO_OUTLIER

    latest_fcf = fcf_list[0]

    # ── ルール1: 直近1年がマイナス ──
    if latest_fcf < 0:
        rule = "latest_negative"
        threshold_pct = 0.0
        deviation_pct = None  # DCF-REL-SYNC-1: マイナスFCF型は乖離%の概念が成立しないためNone
    else:
        # ── ルール2: 5年平均からの乖離が閾値超 ──
        is_mature = cv <= cv_threshold
        threshold_pct = FCF_OUTLIER_THRESHOLDS["mature"] if is_mature else FCF_OUTLIER_THRESHOLDS["growth"]
        deviation = abs(latest_fcf - fcf_5yr_avg) / abs(fcf_5yr_avg)
        if deviation > threshold_pct:
            rule = "deviation_large"
            deviation_pct = deviation * 100  # DCF-REL-SYNC-1: note文字列と同一の計算式を数値として保持
        else:
            return NO_OUTLIER  # 外れ値なし

    # ── EPSアナライザーとの突合 ──
    transient_items = []
    transient_total = 0.0
    transient_found = False

    if eps_data_dir:
        eps_annual = _load_eps_annual(ticker, eps_data_dir)
        if eps_annual:
            transient_items = _find_transient_items(eps_annual, fiscal_year_of_latest)
            # マイナス値（評価益等）は除外してプラスのみ合計
            transient_total = max(0.0, sum(
                item["amount"] for item in transient_items if item["amount"] > 0
            ))
            transient_found = len(transient_items) > 0 and transient_total > 0

    # ── アクション決定 ──
    # 一過性費用がFCF乖離の20%未満の場合は除外しない（金額が小さすぎる）
    fcf_deviation = abs(latest_fcf - fcf_5yr_avg) if fcf_5yr_avg != 0 else abs(latest_fcf)
    # latest_negativeの場合: 一過性費用の金額が5年平均の10%以上なら除外
    # deviation_largeの場合:  一過性費用の金額がFCF乖離の20%以上なら除外
    #   ただし: latest_fcf > fcf_5yr_avg（上方乖離）のケースは除外しない
    #   一過性コストはFCFを下押しするため、上方乖離を「一過性コスト由来」とするのは矛盾
    #   （もし本当に上方乖離が一時的なら flagged として報告するに留める）
    is_upward_deviation = rule == "deviation_large" and latest_fcf > fcf_5yr_avg
    if rule == "latest_negative":
        transient_explains = transient_found and transient_total >= abs(fcf_5yr_avg) * 0.10
    elif is_upward_deviation:
        transient_explains = False  # 上方乖離は一過性コストで除外しない（FCF-OUTLIER-1）
    else:
        transient_explains = transient_found and transient_total >= fcf_deviation * 0.20

    if transient_explains:
        action = "excluded"
        # 一過性費用の内訳を文字列化
        summary = "、".join(
            "{cat}({name} ${amt:.0f}M)".format(
                cat=it["category"], name=it["item_name"], amt=it["amount"]/1e6
            )
            for it in transient_items
        )
        if rule == "latest_negative":
            note = (
                "FY{yr} FCF(${val:.1f}M)がマイナス。"
                "一過性費用合計${total:.0f}M({summary})による影響と判断し除外。"
                "5年平均(${avg:.0f}M)を採用。"
            ).format(
                yr=fiscal_year_of_latest,
                val=latest_fcf/1e6,
                total=transient_total/1e6,
                summary=summary,
                avg=fcf_5yr_avg/1e6,
            )
        else:
            deviation_pct = abs(latest_fcf - fcf_5yr_avg) / abs(fcf_5yr_avg) * 100
            note = (
                "FY{yr} FCF(${val:.0f}M)が5年平均(${avg:.0f}M)から{dev:.0f}%乖離。"
                "一過性費用合計${total:.0f}M({summary})による影響と判断し除外。"
            ).format(
                yr=fiscal_year_of_latest,
                val=latest_fcf/1e6,
                avg=fcf_5yr_avg/1e6,
                dev=deviation_pct,
                total=transient_total/1e6,
                summary=summary,
            )
    else:
        action = "flagged"
        if rule == "latest_negative":
            note = (
                "FY{yr} FCF(${val:.1f}M)がマイナス。"
                "一過性費用の証拠はEPSアナライザーで確認されず。要確認。"
            ).format(yr=fiscal_year_of_latest, val=latest_fcf/1e6)
        else:
            deviation_pct = abs(latest_fcf - fcf_5yr_avg) / abs(fcf_5yr_avg) * 100
            note = (
                "FY{yr} FCF(${val:.0f}M)が5年平均(${avg:.0f}M)から{dev:.0f}%乖離。"
                "一過性費用の証拠はEPSアナライザーで確認されず。要確認。"
            ).format(
                yr=fiscal_year_of_latest,
                val=latest_fcf/1e6,
                avg=fcf_5yr_avg/1e6,
                dev=deviation_pct,
            )

    return FCFOutlierResult(
        detected=True,
        rule=rule,
        fiscal_year=fiscal_year_of_latest,
        fcf_value=latest_fcf,
        threshold_pct=threshold_pct,
        transient_found=transient_found,
        transient_items=transient_items,
        transient_total=transient_total,
        action=action,
        note=note,
        deviation_pct=deviation_pct,
    )


# デフォルトパラメータ
DEFAULT_RETENTION_RATE = 0.60
DEFAULT_ALPHA_CAP = 1.0
DEFAULT_RPO_DISCOUNT_RATE = 0.15
DEFAULT_FCF_FLOOR_RATIO = 0.08
# 後方互換エイリアス: v6.3でratio方式(1.5)→CV方式(0.5)に変更済み
# DEFAULT_FCF_CV_THRESHOLD が正値。こちらは互換性維持のみ。
DEFAULT_FCF_BASE_THRESHOLD = DEFAULT_FCF_CV_THRESHOLD


if __name__ == "__main__":
    print("=== FCFベース自動判定テスト ===\n")

    # AMZN相当（急拡大 → recent_2yr期待）
    r1 = determine_fcf_base(
        fcf_5yr_avg=8_234_200_000,
        fcf_2yr_avg=50_000_000_000,
        fcf_list=[2e9, 3e9, 5e9, 25e9, 75e9]
    )
    print(f"AMZN相当: method={r1.method}  ratio={r1.ratio:.2f}  base=${r1.base_fcf/1e9:.1f}B")

    # MSFT相当（安定 → avg_5yr期待）
    r2 = determine_fcf_base(
        fcf_5yr_avg=65_284_800_000,
        fcf_2yr_avg=70_000_000_000,
        fcf_list=[55e9, 60e9, 65e9, 68e9, 72e9]
    )
    print(f"MSFT相当: method={r2.method}  ratio={r2.ratio:.2f}  base=${r2.base_fcf/1e9:.1f}B")

    # SOFI相当（5年平均マイナス → recent_2yr）
    r3 = determine_fcf_base(
        fcf_5yr_avg=-4_269_811_800,
        fcf_2yr_avg=500_000_000,
        fcf_list=[-3e9, -2e9, -1e9, 0.2e9, 0.8e9]
    )
    print(f"SOFI相当: method={r3.method}  ratio={r3.ratio:.2f}  base=${r3.base_fcf/1e9:.2f}B")


# ── R&D資本化補正 v8.2 ──────────────────────────────────────────────

@dataclass
class RDCapitalizationResult:
    """
    R&D資本化補正結果

    R&Dを費用ではなく投資（無形資産）として扱い、
    3年均等償却した場合のFCF増分を計算する。

    調整後FCF = 元FCF + capitalized_rd - amortization_current
              = 元FCF + rd_adjustment

    rd_adjustment > 0 : R&D増加局面（FCFが過小評価されていた）
    rd_adjustment < 0 : R&D減少局面（稀）
    applied = False   : R&D/Revenue < threshold のため適用せず
    """
    applied: bool
    rd_current: float          # 直近年度R&D費
    rd_avg_3yr: float          # 過去3年R&D費の平均（償却費の代理）
    capitalized_rd: float      # 資本化額 = rd_current（当年全額を無形資産計上）
    amortization_current: float  # 当年償却費 = rd_avg_3yr（3年均等）
    rd_adjustment: float       # FCF増分 = capitalized_rd - amortization_current
    rd_revenue_ratio: float    # R&D/Revenue（適用判定用）
    threshold: float           # 適用閾値（デフォルト0.05）
    years_used: int            # 実際に使用した年数（3年未満の場合）
    note: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "applied": self.applied,
            "rd_current": self.rd_current,
            "rd_avg_3yr": self.rd_avg_3yr,
            "capitalized_rd": self.capitalized_rd,
            "amortization_current": self.amortization_current,
            "rd_adjustment": self.rd_adjustment,
            "rd_revenue_ratio": round(self.rd_revenue_ratio, 4),
            "threshold": self.threshold,
            "years_used": self.years_used,
            "note": self.note,
        }


def capitalize_rd(
    ticker: str,
    sec_data_dir: str,
    amortization_years: int = 3,
    rd_threshold: float = 0.05,
) -> RDCapitalizationResult:
    """
    R&D資本化補正を計算する（v8.2）

    R&Dを費用ではなく投資（無形資産）として扱い、
    3年均等償却した場合のFCF増分を返す。

    計算ロジック:
        capitalized_rd      = rd_current（当年全額を無形資産計上）
        amortization_current = mean(rd[t-1], rd[t-2], rd[t-3])（3年平均を当年償却）
        rd_adjustment       = capitalized_rd - amortization_current
        調整後FCF           = 元FCF + rd_adjustment

    適用条件:
        R&D/Revenue >= rd_threshold（デフォルト5%）
        かつ rd_current > 0
        かつ 年次データが2年以上存在する

    データソース:
        common/sec_data/data/{TICKER}/annual_*.json
        pl.research_and_development / pl.revenue

    Args:
        ticker          : ティッカーシンボル
        sec_data_dir    : common/sec_data/data/ ディレクトリパス
        amortization_years: 償却年数（デフォルト3）
        rd_threshold    : R&D/Revenue適用閾値（デフォルト0.05）

    Returns:
        RDCapitalizationResult
    """
    import json, os, glob

    NOT_APPLIED = lambda note: RDCapitalizationResult(
        applied=False,
        rd_current=0.0, rd_avg_3yr=0.0,
        capitalized_rd=0.0, amortization_current=0.0,
        rd_adjustment=0.0, rd_revenue_ratio=0.0,
        threshold=rd_threshold, years_used=0,
        note=note,
    )

    ticker_dir = os.path.join(sec_data_dir, ticker.upper())
    if not os.path.isdir(ticker_dir):
        return NOT_APPLIED(f"SEC dataディレクトリなし: {ticker_dir}")

    # annual_*.json を年度降順で読み込む
    pattern = os.path.join(ticker_dir, "annual_*.json")
    files = sorted(glob.glob(pattern), reverse=True)
    if len(files) < 2:
        return NOT_APPLIED("年次データが2年分未満のため適用不可")

    # R&DとRevenueを年度降順で取得
    rd_series: list = []   # [(year, rd_value), ...]
    revenue_latest: float = 0.0

    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        rd_val = d.get("pl", {}).get("research_and_development")
        year   = d.get("period")
        if rd_val is not None and rd_val > 0:
            rd_series.append((year, float(rd_val)))
        if revenue_latest == 0.0:
            rev = d.get("pl", {}).get("revenue")
            if rev and rev > 0:
                revenue_latest = float(rev)

    if not rd_series:
        return NOT_APPLIED("R&Dデータなし（全年度）")

    rd_current = rd_series[0][1]

    # R&D/Revenue チェック
    rd_revenue_ratio = rd_current / revenue_latest if revenue_latest > 0 else 0.0
    if rd_revenue_ratio < rd_threshold:
        return NOT_APPLIED(
            f"R&D/Revenue={rd_revenue_ratio:.1%} < 閾値{rd_threshold:.0%}のため適用なし"
        )

    # 過去N年（amortization_years）分のR&D平均を償却費とする
    # rd_series[0]が直近なので、rd_series[1:]が過去年度
    past_rd_raw = [v for _, v in rd_series[1: amortization_years + 1]]

    # IPO年SBC膨張など異常年を除外: 過去R&D > 現在R&Dの3倍は外れ値とみなす
    past_rd = [v for v in past_rd_raw if v <= rd_current * 3.0]

    years_used = len(past_rd)
    if years_used == 0:
        return NOT_APPLIED("過去年度R&Dデータなし（償却費計算不可）")

    # 信頼性確保のため過去2年以上必要（1年では償却基準が不安定）
    if years_used < 2:
        return NOT_APPLIED(
            f"過去年度R&Dデータが{years_used}年のみ（2年以上必要）- "
            "外れ値除外後データ不足の可能性あり"
        )

    rd_avg_3yr = sum(past_rd) / years_used
    capitalized_rd = rd_current
    amortization_current = rd_avg_3yr
    rd_adjustment = capitalized_rd - amortization_current

    note = (
        f"R&D資本化適用: 当年R&D${rd_current/1e9:.2f}B - "
        f"過去{years_used}年平均償却${rd_avg_3yr/1e9:.2f}B "
        f"= FCF調整${rd_adjustment/1e9:+.2f}B "
        f"(R&D/Rev={rd_revenue_ratio:.1%})"
    )

    return RDCapitalizationResult(
        applied=True,
        rd_current=rd_current,
        rd_avg_3yr=rd_avg_3yr,
        capitalized_rd=capitalized_rd,
        amortization_current=amortization_current,
        rd_adjustment=rd_adjustment,
        rd_revenue_ratio=rd_revenue_ratio,
        threshold=rd_threshold,
        years_used=years_used,
        note=note,
    )




# ── FCF内訳分解（ボトムアップFCF、OCF-CapEx）v10.0 ────────────────────
# [[FCF-CONVRATE-LOWER-DIVERGENCE-1]]（2026-09-17）: 業種別固定転換率
# （adj_net_income × conversion_rate）方式を廃止し、CapEx/SBC/OCFの
# 個別開示によるボトムアップFCFへ移行した。旧方式の実測: 99銘柄中
# 較正済みレートで意図通り機能していたのはわずか13銘柄（13%）、47銘柄
# （47%）は業種未収録でdefault(0.70)への機械的フォールバック、39銘柄
# （39%）はadj_net_income<=0等でraw_fcfへの完全フォールバックだった
# （調査フェーズで実データ確認済み）。
#
# raw_fcf（determine_fcf_base()の出力）は既にLayer2のfree_cash_flow
# （=OCF-CapEx、common/sec_data/parser.py内で算出）を基礎としており、
# 概念的に「ボトムアップFCF」そのもの。したがって本設計は「新しい計算式を
# 導入する」のではなく「adj_net_income×conversion_rateという代理変数の
# 算出・上書きをやめ、raw_fcfをそのまま採用した上でCapEx/SBC/OCF/D&Aの
# 内訳を開示する」ことが本質。
#
# 廃止した旧機構（conversion_rate方式に付随するものとして丸ごと廃止、
# 2026-09-17指示書でKoichiさん承認済み）:
# - estimate_fcf_from_eps()・FCFEstimationResult（本節で置き換え）
# - MA統合費用控除ロジック（ma_addback）: Adj_NI×conversion_rate方式に
#   特有の補正だった。OCFは実際の現金の動きのみを反映するGAAP指標のため、
#   この歪みはボトムアップ方式では構造的に発生しない（現金支出を伴う
#   M&A統合費用はOCFに正しく反映され、Adj_NI側の非現金add-backはそもそも
#   OCFに現れない）。
# - 保険/金融特別処理（Healthcare Plans/Financial Services、conversion_
#   rate=1.0強制、use_ni_direct）: 全99銘柄の実測でこの特別処理を通過する
#   銘柄は現在0件と確認済み（死んでいたコードパス）。
# - Software_System_Mature/SaaS自己補正機構（SOFTWARE_SYSTEM_SUBGROUP_
#   RATES・check_software_system_reclassification()・beta_fetcher.py::
#   classify_software_system_subgroup()）: sector別conversion_rateの
#   精度を上げるための仕組みであり、conversion_rate自体を廃止したため
#   意味を失う。sector分類自体（WACCのβフォールバックに使用）は維持する。
# - resolve_fcf_conversion_config_path()・config/fcf_conversion_
#   config.json: 参照元（estimate_fcf_from_eps()）を削除したため不要に
#   なった。report_consistency_check.py::_CONFIG_LOADER_REGISTRYの
#   対応エントリも削除する。


@dataclass
class FCFCompositionResult:
    """ボトムアップFCF（OCF-CapEx）の内訳分解結果"""
    applied: bool
    # [[FCF-CONVRATE-LOWER-DIVERGENCE-1]]STEP5（2026-09-17、Koichiさん
    # 決定4）: 「実際のCapEx/SBC/OCFデータが取得できずフォールバック値を
    # 使った銘柄」を示すフラグとして再定義した。旧FCFEstimationResult.
    # appliedの「conversion_rate方式が適用されたか」とは意味が反転して
    # いる点に注意（旧: True=特殊計算を使った／新: True=データ欠損で
    # フォールバックした）。
    fallback_reason: str    # フォールバック理由（appliedがFalseなら空文字）
    method: str              # "bottom_up" or "fallback_capex_missing"
    estimated_fcf: float     # 採用FCF（=raw_fcf。ボトムアップ方式では
                              # raw_fcfを上書きする別の推定値を持たない）
    raw_fcf: float           # determine_fcf_base()が算出したFCFベース
    capex: Optional[float]   # 直近年CapEx（開示専用、NoneはCapExタグ未申告）
    sbc: Optional[float]     # 直近年SBC（開示専用。非現金項目としてOCFに
                              # 既に加算済みのため、DCF計算には別途
                              # 加減算しない）
    ocf: Optional[float]     # 直近年OCF（開示専用）
    da: Optional[float]      # 直近年D&A（開示専用）
    note: str

    def to_dict(self):
        return {
            "applied": self.applied,
            "fallback_reason": self.fallback_reason,
            "method": self.method,
            "estimated_fcf": self.estimated_fcf,
            "raw_fcf": self.raw_fcf,
            "capex": self.capex,
            "sbc": self.sbc,
            "ocf": self.ocf,
            "da": self.da,
            "note": self.note,
        }


def compose_fcf_bottom_up(
    ticker: str,
    raw_fcf: float,
    capex_list: List[Optional[float]],
    sbc_list: List[Optional[float]],
    ocf_list: List[Optional[float]],
    da_list: List[Optional[float]],
) -> FCFCompositionResult:
    """
    ボトムアップFCF（OCF-CapEx）の内訳を決定する（[[FCF-CONVRATE-LOWER-
    DIVERGENCE-1]]、業種別固定転換率方式からの移行）。

    raw_fcfは既にdetermine_fcf_base()（5yr/2yr平均・CV安定性判定）と
    analyze_fcf_outlier()による外れ値除外を経た値であり、Layer2の
    free_cash_flow（=OCF-CapEx）を基礎としている。本関数はraw_fcfの値
    自体を変更しない（DCFの計算式は「OCF-CapEx」のまま）。責務は以下
    2点のみ:

    ① 直近年のCapEx/SBC/OCF/D&Aをreport.txt向けの内訳として保持する
       （SBCは非現金項目としてOCFに既に加算済みのため、DCF計算には
       別途加減算しない。参考表示のみ）
    ② CapExデータが全期間にわたり欠損している銘柄（標準候補タグを
       一度も申告していない等）を検知し、raw_fcfが実質的にOCFそのもの
       （common/sec_data/parser.pyがCapExを暗黙的に0として扱った値）
       であることを明示する。1年分のみの欠損（5yr平均への影響が
       限定的）はフォールバック扱いとしない。

    Args:
        ticker: 銘柄コード
        raw_fcf: determine_fcf_base()が算出したFCFベース
        capex_list, sbc_list, ocf_list, da_list: data_fetcher.py::
            build_fcf_component_lists()が返す年次系列（新しい順）

    Returns:
        FCFCompositionResult
    """
    latest_capex = capex_list[0] if capex_list else None
    latest_sbc = sbc_list[0] if sbc_list else None
    latest_ocf = ocf_list[0] if ocf_list else None
    latest_da = da_list[0] if da_list else None

    capex_all_missing = bool(capex_list) and all(c is None for c in capex_list)

    if capex_all_missing:
        fallback_reason = (
            f"CapEx申告タグなし（標準候補4タグを直近{len(capex_list)}年分"
            f"いずれも未申告）のため、生FCF（OCFからCapExを0として扱った"
            f"値）を採用。"
        )
        note = f"CapExデータ欠損のためフォールバック。{fallback_reason}"
        return FCFCompositionResult(
            applied=True,
            fallback_reason=fallback_reason,
            method="fallback_capex_missing",
            estimated_fcf=raw_fcf,
            raw_fcf=raw_fcf,
            capex=None, sbc=latest_sbc, ocf=latest_ocf, da=latest_da,
            note=note,
        )

    if latest_ocf is not None and latest_capex is not None:
        note = (
            f"OCF${latest_ocf/1e9:.2f}B - CapEx${latest_capex/1e9:.2f}B"
            f"（直近年。5yr/2yr平均・外れ値除外後の採用値は"
            f"${raw_fcf/1e9:.2f}B）"
        )
    else:
        note = f"生FCF${raw_fcf/1e9:.2f}Bをそのまま採用"

    return FCFCompositionResult(
        applied=False,
        fallback_reason="",
        method="bottom_up",
        estimated_fcf=raw_fcf,
        raw_fcf=raw_fcf,
        capex=latest_capex, sbc=latest_sbc, ocf=latest_ocf, da=latest_da,
        note=note,
    )
