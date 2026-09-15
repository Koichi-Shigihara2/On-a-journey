"""
growth_sanity.py
成長率サニティチェックモジュール

設定された Phase1 成長率が業界ベンチマーク・過去実績と比べて
明らかに非現実的でないかを検証し、根拠サマリーを生成する。
"""

import os
import json
import logging
import sys
import xlrd

_SCRIPT_DIR_FOR_IMPORT = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT_FOR_IMPORT = os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR_FOR_IMPORT)))
if _REPO_ROOT_FOR_IMPORT not in sys.path:
    sys.path.insert(0, _REPO_ROOT_FOR_IMPORT)
from common.sec_data.contracts import GrowthVerdict  # GATE2-PHASE3B-1③-a

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Damodaran キャッシュのパス
# このスクリプトは src/value/tanuki_valuation/ にあるため
# リポジトリルートを基準にした絶対パスで参照する
# ─────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_SCRIPT_DIR)))
_CACHE_DIR = os.path.join(_REPO_ROOT, "docs", "value-monitor", "tanuki_valuation", "common", "damodaran_cache")
_FUNDGR_PATH = os.path.join(_CACHE_DIR, "fundgrEB.xls")
_INDNAME_PATH = os.path.join(_CACHE_DIR, "indname.xls")
_META_PATH = os.path.join(_CACHE_DIR, "cache_meta.json")


# ─────────────────────────────────────────────
# TANUKI sector → Damodaran Industry Name マッピング
#
# 【重要】Damodaran の分類は SIC コードベースのため
# 実態と乖離する銘柄がある（例: MSFT→Trucking, ZS→Steel）。
# ticker_overrides で個別上書きを優先する。
# ─────────────────────────────────────────────
SECTOR_TO_DAMODARAN = {
    # ── TANUKI 旧形式キー（小文字・略称）──
    "semiconductor":     "Semiconductor",
    "semiconductor_eq":  "Semiconductor Equip",
    "software":          "Software (System & Application)",
    "cloud":             "Software (System & Application)",
    "cybersecurity":     "Software (System & Application)",
    "internet":          "Software (Internet)",
    "ecommerce":         "Retail (General)",
    "fintech":           "Financial Svcs. (Non-bank & Insurance)",
    "biotech":           "Drugs (Biotechnology)",
    "pharma":            "Drugs (Pharmaceutical)",
    "healthcare":        "Healthcare Products",
    "healthcare_it":     "Heathcare Information and Technology",
    "ev":                "Auto & Truck",
    "defense":           "Aerospace/Defense",
    "general_tech":      "Computers/Peripherals",
    "advertising":       "Advertising",
    "entertainment":     "Entertainment",
    "telecom":           "Telecom. Services",
    "restaurant":        "Restaurant/Dining",
    "education":         "Education",
    "retail_auto":       "Retail (Automotive)",
    # ── beta_config.json の sector キー形式（admin.html と共通）──
    "Semiconductor":          "Semiconductor",
    "Semiconductor_Equip":    "Semiconductor Equip",
    "Software_System":        "Software (System & Application)",
    "Software_System_Mature": "Software (System & Application)",  # FCF-CONVRATE-DESIGN-LIMIT-1: Software_Systemのサブ分類。Damodaran業種は同一
    "Software_System_SaaS":   "Software (System & Application)",  # 同上
    "Software_Internet":      "Software (Internet)",
    "Software_Entertainment": "Software (Entertainment)",
    "Cloud_Services":         "Software (System & Application)",
    "AdTech_Internet":        "Advertising",
    "Advertising":            "Advertising",
    "Aerospace_Defense":      "Aerospace/Defense",
    "Space_Defense":          "Aerospace/Defense",
    "Air_Transport":          "Air Transport",
    "Apparel":                "Apparel",
    "Auto_Truck":             "Auto & Truck",
    "Auto_Parts":             "Auto Parts",
    "EV_Automotive":          "Auto & Truck",
    "Bank_Money_Center":      "Bank (Money Center)",
    "Banks_Regional":         "Banks (Regional)",
    "Beverage_Alcoholic":     "Beverage (Alcoholic)",
    "Beverage_Soft":          "Beverage (Soft)",
    "Consumer_Beverage":      "Beverage (Soft)",
    "Broadcasting":           "Broadcasting",
    "Brokerage_IB":           "Brokerage & Investment Banking",
    "Building_Materials":     "Building Materials",
    "Business_Consumer_Svcs": "Business & Consumer Services",
    "Cable_TV":               "Cable TV",
    "Chemical_Basic":         "Chemical (Basic)",
    "Chemical_Specialty":     "Chemical (Specialty)",
    "Coal_Energy":            "Coal & Related Energy",
    "Computer_Services":      "Computer Services",
    "Computers_Peripherals":  "Computers/Peripherals",
    "Construction_Supplies":  "Construction Supplies",
    "Diversified":            "Diversified",
    "Drugs_Biotech":          "Drugs (Biotechnology)",
    "Drugs_Pharma":           "Drugs (Pharmaceutical)",
    "Education":              "Education",
    "Electrical_Equipment":   "Electrical Equipment",
    "Electronics_General":    "Electronics (General)",
    "Engineering_Construction":"Engineering/Construction",
    "Entertainment":          "Entertainment",
    "Environmental_Waste":    "Environmental & Waste Services",
    "Farming_Agriculture":    "Farming/Agriculture",
    "Financial_NonBank":      "Financial Svcs. (Non-bank & Insurance)",
    "Fintech":                "Financial Svcs. (Non-bank & Insurance)",
    "Food_Processing":        "Food Processing",
    "Food_Wholesalers":       "Food Wholesalers",
    "Green_Renewable":        "Green & Renewable Energy",
    "Healthcare_IT":          "Heathcare Information and Technology",
    "Healthcare_Products":    "Healthcare Products",
    "Healthcare_Support":     "Healthcare Support Services",
    "Homebuilding":           "Homebuilding",
    "Hospitals_Healthcare":   "Hospitals/Healthcare Facilities",
    "Hotel_Gaming":           "Hotel/Gaming",
    "Household_Products":     "Household Products",
    "Information_Services":   "Information Services",
    "Insurance_General":      "Insurance (General)",
    "Insurance_Life":         "Insurance (Life)",
    "Insurance_PropCas":      "Insurance (Prop/Cas.)",
    "Investments_AM":         "Investments & Asset Management",
    "Machinery":              "Machinery",
    "Metals_Mining":          "Metals & Mining",
    "Oil_Gas_Distribution":   "Oil/Gas Distribution",
    "Oil_Gas_Exploration":    "Oil/Gas (Production and Exploration)",
    "Oil_Gas_Integrated":     "Oil/Gas (Integrated)",
    "Oilfield_Services":      "Oilfield Svcs/Equip.",
    "Packaging":              "Packaging & Container",
    "Paper_Forest":           "Paper/Forest Products",
    "Power_Utility":          "Power",
    "Precious_Metals":        "Precious Metals",
    "Publishing":             "Publishing & Newspapers",
    "REIT":                   "R.E.I.T.",
    "REIT_Retail":            "Retail (REITs)",
    "Real_Estate_Dev":        "Real Estate (Development)",
    "Real_Estate_General":    "Real Estate (General/Diversified)",
    "Real_Estate_Ops":        "Real Estate (Operations & Services)",
    "Recreation":             "Recreation",
    "Restaurant_Dining":      "Restaurant/Dining",
    "Retail_Automotive":      "Retail (Automotive)",
    "Retail_Building":        "Retail (Building Supply)",
    "Retail_Distributors":    "Retail (Distributors)",
    "Retail_General":         "Retail (General)",
    "Retail_Grocery":         "Retail (Grocery and Food)",
    "Retail_Special":         "Retail (Special Lines)",
    "Shipbuilding":           "Shipbuilding & Marine",
    "Shoe":                   "Shoe",
    "Steel":                  "Steel",
    "Telecom_Equipment":      "Telecom. Equipment",
    "Telecom_Services":       "Telecom. Services",
    "Telecom_Wireless":       "Telecom (Wireless)",
    "Transportation":         "Transportation",
    "Transportation_Railroad":"Transportation (Railroads)",
    "Trucking":               "Trucking",
    "Utility_General":        "Utility (General)",
    "Utility_Water":          "Utility (Water)",
}

# 銘柄個別の Damodaran 分類上書き
# indname.xls で実際に確認したマッピング（SICベース分類が実態と乖離する場合）
_PHASE_LABELS: dict[int, str] = {
    0: "失望/蓄積期",
    1: "期待覚醒期",
    2: "期待拡大期",
    3: "陶酔期",
    4: "期待剥落期",
}

# 銘柄個別のベンチマーク成長率直接指定（Damodaran未分類 or 保守的調整が必要な銘柄）
# Damodaran 業種マッピングより優先される
_TICKER_BENCHMARK_OVERRIDES: dict[str, float] = {
    "CELH": 0.06,  # Beverage (Soft)成熟期: 高成長期終了後の正規化を考慮し保守的に6%設定
                   # ※Damodaran Beverage (Soft) g_ebit=9.85%だが、デストック後の実力値として保守化
}

TICKER_INDUSTRY_OVERRIDES = {
    # SICベース分類が実態と乖離 → 実態に近い業種に上書き
    "APP":   "Software (System & Application)",   # SIC→Retail(General) を上書き
    "CRM":   "Software (System & Application)",   # SIC→Prepackaged Software を上書き
    "NOW":   "Software (System & Application)",   # SIC→Services-Prepackaged を上書き
    "BILL":  "Software (System & Application)",   # SIC→Services-Computers を上書き
    "SPOT":  "Entertainment",                     # SIC→Services-Radio を上書き
    "MDB":   "Software (Internet)",               # SIC→Services-Prepackaged を上書き
    "OKTA":  "Software (Internet)",               # SIC→Services-Prepackaged を上書き
    "SHOP":  "Software (Internet)",               # SIC→Retail(General) を上書き
    "SQ":    "Financial Svcs. (Non-bank & Insurance)",  # SIC→Services-Computers を上書き
    "META":  "Software (Entertainment)",           # indname.xls実態分類（旧"Advertising"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "AMZN":  "Retail (General)",                   # indname.xls実態分類（旧"Software (System & Application)"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "NET":   "Software (Internet)",                # indname.xls実態分類（旧"Software (System & Application)"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "ZS":    "Software (System & Application)",   # SIC→Steel を上書き
    "ANET":  "Telecom. Equipment",                # SIC→Telecom.Services を上書き
    "ARM":   "Semiconductor",                     # SIC→Transportation を上書き
    "S":     "Software (System & Application)",   # SIC→Utility(Water) を上書き
    "DIS":   "Entertainment",                     # SIC→Real Estate を上書き
    "CIX":   "Office Equipment & Services",       # indname.xls実態分類（旧"Electrical Equipment"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "AAPL":  "Computers/Peripherals",             # SIC→Software_Internet デフォルトを上書き。indname.xls実態分類
    "ADBE":  "Software (System & Application)",   # Creative/Document Cloud SaaS
    "BKNG":  "Hotel/Gaming",                       # indname.xls実態分類（旧"Software (Internet)"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "ADSK":  "Software (System & Application)",   # AEC/製造業向けSaaS
    "CDNS":  "Software (System & Application)",   # EDA半導体設計ツール
    "PAYS":  "Financial Svcs. (Non-bank & Insurance)",  # 小型FinTech・プリペイドカード
    "INTU":  "Software (System & Application)",   # 税務・会計SaaS
    "HEI":   "Aerospace/Defense",                 # 航空MRO部品・防衛電子機器
    "HWM":   "Aerospace/Defense",                 # 航空エンジン精密部品
    "HON":   "Diversified",                        # indname.xls実態分類（旧"Machinery"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "TDY":   "Electronics (General)",              # indname.xls実態分類（旧"Aerospace/Defense"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "KULR":  "Electrical Equipment",               # indname.xls実態分類（旧"Aerospace/Defense"はテストデータ。SECTOR-FCF-RATE-BROKEN-1）
    "CEG":   "Power",                             # 原子力主体の規制電力会社
    "VST":   "Power",                             # 原子力+ガス火力のハイブリッド電力会社
    "SCCO":  "Metals & Mining",                   # ペルー・メキシコ中心の銅鉱山
    "FCX":   "Metals & Mining",                   # グラスバーグ鉱山主力の銅生産会社
    "MO":    "Tobacco",                           # beta_config.jsonにsector未設定のため業種ベンチマークがNoneだった
                                                   # （GROWTH-SANITY-CLASS-SYNC-1調査で発見）。fundgrEB.xlsに
                                                   # Tobacco業種が実在（g_ebit≈1.5%）することを確認済み
}


# ─────────────────────────────────────────────
# Damodaran データ読み込み（起動時1回だけ実行）
# ─────────────────────────────────────────────
_damodaran_data: dict = {}   # {industry_name: {roc, rr, g_ebit}}
_damodaran_loaded = False


def _load_damodaran():
    global _damodaran_data, _damodaran_loaded
    if _damodaran_loaded:
        return

    if not os.path.exists(_FUNDGR_PATH):
        logger.warning(f"Damodaran cache not found: {_FUNDGR_PATH}")
        _damodaran_loaded = True
        return

    try:
        wb = xlrd.open_workbook(_FUNDGR_PATH)
        sh = wb.sheet_by_name("Industry Averages")
        # row7 がヘッダー: Industry Name / Number of Firms / ROC / Reinvestment Rate / Expected Growth in EBIT
        for i in range(8, sh.nrows):
            row = sh.row_values(i)
            name = str(row[0]).strip()
            if not name or name.startswith("Total"):
                continue
            roc = row[2] if isinstance(row[2], float) else None
            rr  = row[3] if isinstance(row[3], float) else None
            g   = row[4] if isinstance(row[4], float) else None
            _damodaran_data[name] = {"roc": roc, "rr": rr, "g_ebit": g}

        logger.info(f"Damodaran data loaded: {len(_damodaran_data)} industries")

        # キャッシュ年を確認して古ければ警告
        if os.path.exists(_META_PATH):
            with open(_META_PATH, encoding="utf-8") as f:
                meta = json.load(f)
            cache_year = meta.get("year", 0)
            import datetime
            current_year = datetime.date.today().year
            if current_year - cache_year >= 2:
                logger.warning(
                    f"Damodaran cache is from {cache_year}. "
                    "Consider updating: https://pages.stern.nyu.edu/~adamodar/pc/datasets/fundgrEB.xls"
                )
    except Exception as e:
        logger.warning(f"Failed to load Damodaran data: {e}")
    finally:
        _damodaran_loaded = True


def get_industry_benchmark(ticker: str, sector: str | None) -> dict | None:
    """
    ticker と sector から Damodaran の業種ベンチマークを返す。
    戻り値: {"industry": str, "g_ebit": float, "roc": float, "rr": float} or None
    """
    _load_damodaran()
    if not _damodaran_data:
        return None

    # 最優先: 銘柄個別ベンチマーク直接指定
    if ticker in _TICKER_BENCHMARK_OVERRIDES:
        return {
            "industry": f"{ticker}_custom",
            "g_ebit":   _TICKER_BENCHMARK_OVERRIDES[ticker],
            "roc":      None,
            "rr":       None,
        }

    # 優先順位: ticker個別上書き > sector マッピング
    industry_name = TICKER_INDUSTRY_OVERRIDES.get(ticker)
    if industry_name is None and sector:
        industry_name = SECTOR_TO_DAMODARAN.get(sector)
    if industry_name is None:
        return None

    data = _damodaran_data.get(industry_name)
    if data is None:
        return None

    return {
        "industry": industry_name,
        "g_ebit": data["g_ebit"],
        "roc": data["roc"],
        "rr": data["rr"],
    }


# ─────────────────────────────────────────────
# Revenue CAGR 計算
# ─────────────────────────────────────────────
def calc_revenue_cagr(annual_revenues: list[float]) -> dict:
    """
    annual_revenues: 古い順のリスト [rev_oldest, ..., rev_latest]
    戻り値: {"cagr_3yr": float|None, "cagr_5yr": float|None}
    """
    result = {"cagr_3yr": None, "cagr_5yr": None}
    if not annual_revenues or len(annual_revenues) < 2:
        return result

    latest = annual_revenues[-1]
    if latest <= 0:
        return result

    if len(annual_revenues) >= 4:
        base = annual_revenues[-4]
        if base > 0:
            result["cagr_3yr"] = (latest / base) ** (1 / 3) - 1

    if len(annual_revenues) >= 6:
        base = annual_revenues[-6]
        if base > 0:
            result["cagr_5yr"] = (latest / base) ** (1 / 5) - 1

    return result


# ─────────────────────────────────────────────
# ファンダメンタル成長率（RR × ROIC）
# ─────────────────────────────────────────────
def calc_fundamental_growth(
    operating_income: float,
    tax_rate: float,
    total_equity: float,
    total_debt: float,
    cash: float,
    capex: float,
    depreciation: float,
    delta_working_capital: float,
) -> float | None:
    """
    g = Reinvestment Rate × ROIC
    計算不能（負のROIC等）の場合は None を返す。
    """
    try:
        nopat = operating_income * (1 - tax_rate)
        if nopat <= 0:
            return None

        invested_capital = total_equity + total_debt - cash
        if invested_capital <= 0:
            return None

        roic = nopat / invested_capital

        reinvestment = capex - depreciation + delta_working_capital
        reinvestment_rate = reinvestment / nopat

        g = reinvestment_rate * roic
        # 異常値を除外（-100%〜+200%の範囲外）
        if not (-1.0 <= g <= 2.0):
            return None
        return g
    except Exception:
        return None


# ─────────────────────────────────────────────
# メイン判定関数
# ─────────────────────────────────────────────
def check_growth_sanity(
    ticker: str,
    phase1_growth: float,
    sector: str | None = None,
    annual_revenues: list[float] | None = None,
    g_fundamental: float | None = None,
    ttm_actual: float | None = None,  # TTM Revenue YoY (decimal) from STONKS/SEC
    hype_phase: int | None = None,         # GROWTH-1: HypeCoreフェーズ（1〜4）
    hype_phase_label: str | None = None,   # poc.json の stage_label
    hype_substage_label: str | None = None, # poc.json の substage_label
    fcf_margins: list[float] | None = None,  # TANUKI-DCF-1③: FCFマージン時系列（古い順）
    growth_source: str | None = None,  # GROWTH-FLOOR-VERDICT-1: phase1_growthの採用経路("fcf_cagr"等)
) -> dict:
    """
    成長率サニティチェックを実行し、結果 dict を返す。

    戻り値例:
    {
        "verdict": GrowthVerdict.PLAUSIBLE,  # GrowthVerdict型（GATE2-PHASE3B-1③-a）。
                                          # str継承のためJSON出力・比較は生文字列と同様に動作
        "phase1_growth": 0.20,
        "industry_benchmark": 0.096,
        "damodaran_industry": "Semiconductor",
        "damodaran_year": 2025,
        "rev_cagr_3yr": 0.221,
        "rev_cagr_5yr": 0.198,
        "g_fundamental": 0.312,
        "signals": [...],
        "warnings": [...],
        "floor_hit": False,              # GROWTH-FLOOR-VERDICT-1
    }
    """
    signals = []
    warnings = []

    # calculator/growth.py:calculate_fcf_cagr() の growth_floor と同値。
    # Stage 1（候補閾値緩和の判定）・floor_hit算出の両方で参照する
    # [[TANUKI-VALUATION-MISC-GAPS-1]]⑦（2026-09-16追記）: この0.15は
    # growth.py側のgrowth_floorとは別変数として本ファイルに独立
    # ハードコードされており、一方だけ変更すると値がずれる二重管理状態
    # にある（今回は明示のみで統合はしない）。値自体の根拠は
    # growth.py::calculate_fcf_cagr()の引数定義部コメント参照。
    _FCF_CAGR_FLOOR = 0.15

    # --- Damodaran ベンチマーク取得 ---
    benchmark = get_industry_benchmark(ticker, sector)
    industry_g = benchmark["g_ebit"] if benchmark else None

    # --- Revenue CAGR 計算 ---
    cagr = calc_revenue_cagr(annual_revenues or [])

    # --- チェック1: 業界ベンチマーク比較 ---
    if industry_g is not None and industry_g > 0:
        ratio = phase1_growth / industry_g
        ind_label = f"{benchmark['industry']}({industry_g:.1%})"
        if ratio <= 1.5:
            signals.append(f"業界平均{ind_label}の{ratio:.1f}倍以内 ✅")
        elif ratio <= 2.5:
            signals.append(f"業界平均{ind_label}の{ratio:.1f}倍 ℹ️")
        else:
            warnings.append(f"業界平均{ind_label}の{ratio:.1f}倍超 ⚠️")
    elif industry_g is not None and industry_g <= 0:
        # 業界平均がマイナスの場合（単純比較不可）
        signals.append(f"業界平均({benchmark['industry']})はマイナス成長。個別銘柄の独自評価が必要 ℹ️")

    # --- チェック2: 過去実績 CAGR 比較 ---
    historical_cagrs = [v for v in [cagr.get("cagr_3yr"), cagr.get("cagr_5yr")] if v and v > 0]
    if historical_cagrs:
        best = max(historical_cagrs)
        ratio_hist = phase1_growth / best
        label_3yr = f"{cagr['cagr_3yr']:.1%}" if cagr.get("cagr_3yr") else "N/A"
        label_5yr = f"{cagr['cagr_5yr']:.1%}" if cagr.get("cagr_5yr") else "N/A"
        hist_label = f"過去実績(3yr:{label_3yr} / 5yr:{label_5yr})"
        if ratio_hist <= 1.3:
            signals.append(f"{hist_label}と整合 ✅")
        elif ratio_hist <= 2.0:
            signals.append(f"{hist_label}より高め（減速想定あり） ℹ️")
        else:
            warnings.append(f"{hist_label}の{ratio_hist:.1f}倍超 ⚠️")

    # --- チェック3: ファンダメンタル成長率（RR×ROIC）上限 ---
    if g_fundamental is not None and g_fundamental > 0:
        if phase1_growth <= g_fundamental * 1.2:
            signals.append(f"RR×ROIC上限({g_fundamental:.1%})以内 ✅")
        else:
            signals.append(f"RR×ROIC({g_fundamental:.1%})：再投資より還元重視型、または期待値先行 ℹ️")

    # --- データ不足の場合 ---
    if not signals and not warnings:
        signals.append("ベンチマークデータ不足のため自動検証スキップ ℹ️")

    # --- 総合判定 ---
    if len(warnings) == 0:
        verdict = GrowthVerdict.PLAUSIBLE
    elif len(warnings) == 1:
        verdict = GrowthVerdict.REVIEW
    else:
        verdict = GrowthVerdict.AGGRESSIVE

    # Damodaran キャッシュ年を取得
    damodaran_year = None
    if os.path.exists(_META_PATH):
        try:
            with open(_META_PATH, encoding="utf-8") as f:
                damodaran_year = json.load(f).get("year")
        except Exception:
            pass

    # ─── Stage 1: 複数指標の中央値 ───
    _rec_candidates = []
    if cagr.get("cagr_3yr") is not None and cagr["cagr_3yr"] > 0:
        _rec_candidates.append(cagr["cagr_3yr"])
    if cagr.get("cagr_5yr") is not None and cagr["cagr_5yr"] > 0:
        _rec_candidates.append(cagr["cagr_5yr"])
    if industry_g is not None and industry_g > 0:
        _rec_candidates.append(industry_g)
    if g_fundamental is not None and g_fundamental > 0:
        _rec_candidates.append(g_fundamental)

    # GROWTH-SANITY-CLASS-SYNC-1（MO型iv）: 通常は中央値に候補2件以上が必要だが、
    # 「fcf_cagr経路でfloor(15%)に到達している」かつ「業界ベンチマークのみ1件」の
    # 銘柄に限り、industry_g単独をrecommended_gとして採用する（案B'）。
    # 一般的な閾値緩和（候補1件で常に許容）はLOARのような真の高成長銘柄で
    # 成熟業界平均を誤って押し付ける副作用が事前検証で確認されたため、
    # floor到達中の銘柄のみへ厳密に限定する。
    # 注: floor_hit変数自体はrecommended_g確定後（本ブロックの後段）でしか
    # 算出できないため、同じ判定条件をここで独立に再現する
    # （_FCF_CAGR_FLOORは関数末尾のfloor_hit算出と共用）。
    _is_fcf_cagr_floor_candidate = (
        growth_source == "fcf_cagr"
        and phase1_growth is not None
        and abs(phase1_growth - _FCF_CAGR_FLOOR) < 0.002
    )
    _min_rec_candidates = 2
    if (
        _is_fcf_cagr_floor_candidate
        and len(_rec_candidates) == 1
        and industry_g is not None
        and industry_g > 0
    ):
        _min_rec_candidates = 1

    recommended_g_median = None
    if len(_rec_candidates) >= _min_rec_candidates:
        _rec_candidates.sort()
        _n = len(_rec_candidates)
        if _n % 2 == 1:
            recommended_g_median = _rec_candidates[_n // 2]
        else:
            recommended_g_median = (_rec_candidates[_n // 2 - 1] + _rec_candidates[_n // 2]) / 2

    # ─── recommended_g ───
    _cagr_vals = [v for v in [cagr.get("cagr_3yr"), cagr.get("cagr_5yr")] if v is not None and v > 0]
    _ttm_best_cagr = max(_cagr_vals) if _cagr_vals else None
    _ttm_actual_pos = ttm_actual if (ttm_actual is not None and ttm_actual > 0) else None
    # 逓減モデルのトリガー: ttm_actual / cagr_3yr / cagr_5yr のいずれかが 50% 超
    _trigger_vals = [v for v in [_ttm_actual_pos, *_cagr_vals] if v is not None]
    _trigger_max = max(_trigger_vals) if _trigger_vals else None

    if _trigger_max is not None and _trigger_max > 0.50:
        # ── 逓減モデル: TTM/CAGRピークと業界平均の中間値 ──
        # GROWTH-1（2026-05-31導入、HypeCoreフェーズに応じてTTM/業界平均の
        # 加重比率をPhase1-2=65%/Phase3=50%/Phase4=35%と変えていた）は、実証
        # データ（バックテスト等）なしに理論的仮説のみで導入されており、かつ
        # Funda側（DCF成長率）にTiming側（HypePhase＝価格・出来高ベースの
        # センチメント指標、report.txt[7]HYPECORE参照）の信号を混ぜるという
        # 設計思想上の非対称性があったため、2026-08-26
        # [[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]の結論に基づき固定50:50へ
        # 復元した。HypePhase由来の「勢いの持続性・剥落」という発想自体は
        # 無駄にせず、Timing側（HypeCore）の機能として
        # [[STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1]]で別途引き取りを検討中。
        _ttm_weight = 0.50  # 固定（GROWTH-1のフェーズ別加重は廃止）

        # A-2: decay モデルの start_g を「逓減を発動させた最大値」に統一。
        # ttm_actual (phase1_growth) と cagr_3yr/5yr の最大値をスタート点とする。
        # これにより start_g と _trigger_max が常に一致し、ラベルも正確になる。
        _start_raw_ttm  = _ttm_actual_pos or 0.0
        _start_raw_cagr = _ttm_best_cagr  or 0.0
        if _start_raw_cagr > _start_raw_ttm:
            _start_raw = _start_raw_cagr
            _start_src = "CAGR_max"
        else:
            _start_raw = _start_raw_ttm
            _start_src = "G入力値"   # phase1_growth (セグメント設定値 or TTM注入値)
        start_g = min(_start_raw, 1.0)
        end_g = industry_g if (industry_g is not None and industry_g > 0) else 0.10
        recommended_g = start_g * _ttm_weight + end_g * (1 - _ttm_weight)
        growth_model = "decay"
        growth_model_reason = (
            f"{_start_src}={start_g:.1%}と業界平均{end_g:.1%}の中間値"
            f"{recommended_g:.1%}を採用（固定50:50、将来の成長減速を"
            f"織り込んだ保守的推定）"
        )
    else:
        # 中央値モデル
        recommended_g = recommended_g_median  # None if < 2 candidates
        growth_model = "median"
        # A-2: _trigger_max は max(rev_cagr_3yr, rev_cagr_5yr, phase1_g) であり
        # "TTM YoY Revenue Growth" とは別物。CAGR_max として表示する。
        _reason_cagr = f"CAGR_max={_trigger_max:.1%}" if _trigger_max is not None else "CAGRデータなし"
        growth_model_reason = f"{_reason_cagr}のため中央値モデル適用"

        # 上限キャップ: max(industry_benchmark × 3, 50%)
        if recommended_g is not None:
            _cap = max(
                industry_g * 3.0 if (industry_g is not None and industry_g > 0) else 0.50,
                0.50,
            )
            recommended_g = min(recommended_g, _cap)

        # マイナス成長は自動調整対象外
        if recommended_g is not None and recommended_g < 0:
            recommended_g = None

    # ── TANUKI-DCF-1③ FCFマージン悪化補正係数 ──
    # fcf_margins は古い順。直近3年平均 vs 最新でトレンドを判定
    fcf_margin_bear_multiplier = 1.0
    fcf_margin_note = None
    if fcf_margins and len(fcf_margins) >= 3:
        _latest_m = fcf_margins[-1]
        _avg3_m   = sum(fcf_margins[-3:]) / 3
        if _avg3_m > 0 and (_avg3_m - _latest_m) >= 2.0:
            _ratio = min(1.0, _latest_m / _avg3_m)
            fcf_margin_bear_multiplier = round(max(_ratio, 0.4), 4)
            fcf_margin_note = (
                f"FCFマージン悪化({_avg3_m:.1f}%→{_latest_m:.1f}%) "
                f"BEAR補正係数={fcf_margin_bear_multiplier:.3f}"
            )

    # ── GROWTH-FLOOR-VERDICT-1: fcf_cagr floor値張り付き検知 ──
    # calculator/growth.py:calculate_fcf_cagr() の growth_floor（0.15）と同値
    # （[[TANUKI-VALUATION-MISC-GAPS-1]]⑦: 関数冒頭_FCF_CAGR_FLOORと同じく
    # growth.py側とは独立ハードコードの二重管理、根拠は同関数コメント参照）。
    # 本関数はpipeline.py内でrecommended_gによるDCF再計算（override）の"前"に
    # 呼ばれるため、growth_source/phase1_growthは常にoverride適用前の値になる
    # （JNJ等、override成功後にsegment_weightedへ差し替わる銘柄は事前状態では
    # fcf_cagr floorのことが多く、それだけで判定するとoverride成功銘柄まで
    # 誤検知してしまう）。recommended_gがこの時点で算出できている＝pipeline.py側で
    # override適用が見込める銘柄は対象から除外し、recommended_g=Noneのため
    # override自体が発火しない銘柄（MO/LOAR/XOM型）のみを検知する。
    # （_FCF_CAGR_FLOORはStage 1の候補閾値緩和判定でも使うため関数冒頭で定義済み）
    _fcf_cagr_floor_hit = (
        growth_source == "fcf_cagr"
        and phase1_growth is not None
        and abs(phase1_growth - _FCF_CAGR_FLOOR) < 0.002
        and recommended_g is None
    )

    # JOBY-STATIC-GROWTH-HARDCODE-1: segment_weighted経路（admin.html::
    # fetchSegmentsForTicker()が新規銘柄登録時に機械的に書き込むGeneral
    # 100%セグメントのgrowth値、デフォルト0.15）が、rev_cagr_3yr/5yr/
    # g_fundamentalの実測系3指標すべてNone（量産前・低ベース効果等で
    # TTM注入の安全弁〈_inject_ttm_for_general_segment〉にも棄却され、
    # pipeline.py側のoverrideが一切発火しない）ため、未検証のまま
    # DCF成長率として使われ続けているケースを検知する。
    # recommended_g is None を必須条件に含めるのは、CRWV型（同じ実測系
    # 3指標None・segment_weightedだが、ttm_actual経由でdecayモデルの
    # recommended_gが正しく算出済み＝overrideが機能している銘柄）を
    # 誤検知しないため（全母集団シミュレーションで確認済み、2026-07-20）。
    _segment_weighted_all_none_hit = (
        growth_source == "segment_weighted"
        and cagr.get("cagr_3yr") is None
        and cagr.get("cagr_5yr") is None
        and g_fundamental is None
        and recommended_g is None
    )

    floor_hit = _fcf_cagr_floor_hit or _segment_weighted_all_none_hit
    if floor_hit:
        verdict = GrowthVerdict.FLOOR_HIT_REVIEW
        if _segment_weighted_all_none_hit:
            warnings.append(
                f"実測データ不足のためテンプレートのデフォルト成長率"
                f"（{_FCF_CAGR_FLOOR:.0%}）が未検証のまま使用中 ⚠️"
            )

    return {
        "verdict": verdict,
        "floor_hit": floor_hit,
        "phase1_growth": phase1_growth,
        "industry_benchmark": industry_g,
        "damodaran_industry": benchmark["industry"] if benchmark else None,
        "damodaran_year": damodaran_year,
        "rev_cagr_3yr": cagr.get("cagr_3yr"),
        "rev_cagr_5yr": cagr.get("cagr_5yr"),
        "g_fundamental": g_fundamental,
        "recommended_g_median": recommended_g_median,
        "recommended_g": recommended_g,
        "growth_model": growth_model,
        "growth_model_reason": growth_model_reason,
        "hype_phase_used": hype_phase,
        "hype_phase_label": hype_phase_label or (_PHASE_LABELS.get(hype_phase) if hype_phase is not None else None),
        "hype_substage_label": hype_substage_label,
        "signals": signals,
        "warnings": warnings,
        "fcf_margin_bear_multiplier": fcf_margin_bear_multiplier,
        "fcf_margin_note": fcf_margin_note,
    }
