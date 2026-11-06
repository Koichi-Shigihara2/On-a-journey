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
# 銘柄個別のベンチマーク成長率直接指定（Damodaran未分類 or 保守的調整が必要な銘柄）
# Damodaran 業種マッピングより優先される
_TICKER_BENCHMARK_OVERRIDES: dict[str, float] = {
    "CELH": 0.06,  # Beverage (Soft)成熟期: 高成長期終了後の正規化を考慮し保守的に6%設定
                   # ※Damodaran Beverage (Soft) g_ebit=9.85%だが、デストック後の実力値として保守化
}

# ─────────────────────────────────────────────
# 銘柄 → Damodaran業種（[[SECTOR-MASTER-ADOPT-1]] 段1、2026-11-06）
# 以前は TICKER_INDUSTRY_OVERRIDES（手書きの35銘柄）と beta_config.overrides.sector（手書き35銘柄）の2か所だった。
# 今は 1. 例外の台帳 config/sector_exceptions.json（理由つき、写像と違う銘柄だけ）→ 2. config/sector_master.json の
# damodaran_industry（yfinanceのsector・industryからの写像、全銘柄）。どちらにも無い銘柄（登録前）は呼び出し側の sector → SECTOR_TO_DAMODARAN
# ─────────────────────────────────────────────
_EXCEPTIONS_PATH = os.path.join(_REPO_ROOT, "config", "sector_exceptions.json")
_MASTER_PATH = os.path.join(_REPO_ROOT, "config", "sector_master.json")
_sector_tables: dict = {}


def _load_sector_tables() -> dict:
    """{"exceptions": {T: 業種}, "master": {T: 業種}}（初回に読んで保持。読めなければ空）"""
    if _sector_tables:
        return _sector_tables
    tables = {"exceptions": {}, "master": {}}
    try:
        with open(_EXCEPTIONS_PATH, encoding="utf-8") as f:
            tables["exceptions"] = {t: e["industry"] for t, e in (json.load(f).get("damodaran_industry") or {}).items()}
    except (OSError, ValueError, KeyError, TypeError) as e:
        logger.warning("sector_exceptions.json を読めません: %s", e)
    try:
        with open(_MASTER_PATH, encoding="utf-8") as f:
            tables["master"] = {t: e.get("damodaran_industry") for t, e in (json.load(f).get("tickers") or {}).items()
                                if e.get("damodaran_industry")}
    except (OSError, ValueError, TypeError) as e:
        logger.warning("sector_master.json を読めません: %s", e)
    _sector_tables.update(tables)
    return _sector_tables


def industry_for_ticker(ticker: str) -> str | None:
    """例外の台帳 → 業種のマスターの写像の順でDamodaran業種名を返す（どちらにも無ければ None）"""
    t = _load_sector_tables()
    return t["exceptions"].get(ticker) or t["master"].get(ticker)


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

    # 優先順位: 例外の台帳 > 業種のマスターの写像 > sector マッピング（登録前の銘柄）
    industry_name = industry_for_ticker(ticker)
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
# 推奨成長率の定数（[[GROWTH-SANITY-CONTINUITY-1]] 段1、2026-11-06、チャット側の決定）
# 第一条件: 入力が少し変わったとき、推奨成長率が少しだけ変わる（候補の数・符号・逓減への切り替えの境目で値が飛ばない）
# ─────────────────────────────────────────────
# RR×ROIC・業種の値の「重み」が0から1になる幅（0〜5%、1つの定数）。0以下は重み0、5%以上は重み1。
# 今は「正のときだけ候補」の二値で、RR×ROICが0.06%（LOAR）のような「測れていない値」も1件と数えて中央値を押し下げ、
# 0.1%の違いで推奨成長率が5pt動いていた（MRVL）。値が小さいほど候補への寄与を連続に小さくする
WEIGHT_RAMP = 0.05
# 候補の重みの合計がこの値に届くまで、足りない分を今のDCFの成長率 max(p1, 0) で埋める（場合分けしない）。
# 今の「候補2件以上、なければ推奨成長率なし（上書きなし）」の連続版。業種だけ・1件だけでも成熟業種の平均を押し付けず、p1との間に置く
FILL_WEIGHT = 2.0
# 成長の測り（売上CAGR）の上限。逓減モデルの start_g の上限（100%）と同じ
GROWTH_MEASURE_CAP = 1.0
# 逓減モデルへの切り替えを tmax（今の伸び・3年・5年CAGRの最大）0.50〜0.70 でなだらかに混ぜる（今は0.50超で二値）。
# DCFの成長率p1の上限が0.50なので、窓は0.50から上に置く（0.50ちょうどに頭打ちされた銘柄は今のまま）
DECAY_BLEND_LO = 0.50
DECAY_BLEND_HI = 0.70
# 業種が無いときの終点（今の end_g の既定と同じ）
DECAY_END_DEFAULT = 0.10
# 段2（2026-11-06）: 成長の測り（売上CAGR・今の伸び）の基準の売上がこの額（ドル）未満なら、その測りの重みを 0→1 に連続で下げる。
# 売上がほぼ0の会社のCAGR・前年比は底上げ効果で何百〜何万%にもなり（JOBY 118,568%）、そのまま平均に入ると推奨成長率を押し上げる
GROWTH_BASE_REVENUE_FULL = 50_000_000.0
# 2年CAGR（年次が3点の銘柄だけ）は3年・5年より短い期間の平均なので、重みを半分にする
CAGR_2YR_WEIGHT = 0.5
# 今の伸び（TTMの売上の前年比）は、年次の最大より何pt高い所まで認めるか（連続のmin）。買収・原油の戻り等の一時的な高さを抑える
TTM_GROWTH_EXCESS_CAP = 0.10


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _smoothstep(t: float) -> float:
    t = _clamp(t, 0.0, 1.0)
    return t * t * (3.0 - 2.0 * t)


def _weight_ramp(x: float) -> float:
    """RR×ROIC・業種の値の重み。0以下で0、WEIGHT_RAMP以上で1、その間は直線"""
    return _clamp(x / WEIGHT_RAMP, 0.0, 1.0)


def tapering_g_end(recommended_g: float, blend_s: float, industry_g: float | None) -> float:
    """線形逓減DCFの終点。混ぜ具合sで推奨成長率から業種へ連続に近づける（s=0なら推奨成長率〈逓減なし〉、s=1なら業種）。
    業種が0以下のときは0（逓減の終点を負にしない）。pipelineが tapering_g_end に渡す"""
    end = max(industry_g, 0.0) if industry_g is not None else DECAY_END_DEFAULT
    return recommended_g - blend_s * (recommended_g - end)


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
    fcf_margins: list[float] | None = None,  # TANUKI-DCF-1③: FCFマージン時系列（古い順）
    growth_source: str | None = None,  # GROWTH-FLOOR-VERDICT-1: phase1_growthの採用経路("fcf_cagr"等)
    ttm_rev_yoy: float | None = None,  # 段2: 会社の今の伸び = TTMの売上の前年比（Layer3のTTM系列の最新と1年前の点、小数）
    ttm_base_revenue: float | None = None,  # 段2: その1年前のTTMの売上（ドル）。今の伸びの基準の売上
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

    # ─── 推奨成長率（候補の重み付き平均 → 逓減モデルとのなだらかな混合）───
    # [[GROWTH-SANITY-CONTINUITY-1]] 段1（2026-11-06）。今までの規則の飛び:
    #   候補が「>0」のときだけ入る（符号）・2件未満はNone・偶数/奇数で中央値が切り替わる・floor例外が±0.002の二値・
    #   逓減モデルへの切り替えが0.50超の二値・逓減の終点が業種>0かどうかの二値・taperの有無がラベルの二値
    # 候補: 売上CAGR 3年・5年（負は0として入れる）、RR×ROIC・業種の値（0〜WEIGHT_RAMPで重み0→1）
    # 段2: 年次が3点だけの銘柄は2年CAGRも、会社の今の伸び（TTMの売上の前年比）も候補にする。
    # 成長の測りは、基準の売上が GROWTH_BASE_REVENUE_FULL 未満なら重みを連続で下げる
    _revs = annual_revenues or []
    _cagr_2yr = None
    if len(_revs) == 3 and _revs[-3] > 0 and _revs[-1] > 0:
        _cagr_2yr = (_revs[-1] / _revs[-3]) ** 0.5 - 1

    def _base_w(base: float | None) -> float:
        return 1.0 if base is None else _clamp(base / GROWTH_BASE_REVENUE_FULL, 0.0, 1.0)

    _growth_meas = []  # (名前, 値〈0〜GROWTH_MEASURE_CAP〉, 重み, 基準の売上の重み)
    for _nm, _cv, _w0, _bi in (("cagr_3yr", cagr.get("cagr_3yr"), 1.0, -4),
                               ("cagr_5yr", cagr.get("cagr_5yr"), 1.0, -6),
                               ("cagr_2yr", _cagr_2yr, CAGR_2YR_WEIGHT, -3)):
        if _cv is not None:
            _bw = _base_w(_revs[_bi] if len(_revs) >= -_bi else None)
            _growth_meas.append([_nm, _clamp(_cv, 0.0, GROWTH_MEASURE_CAP), _w0 * _bw, _bw])
    if ttm_rev_yoy is not None:
        _growth_meas.append(["ttm_rev_yoy", _clamp(ttm_rev_yoy, 0.0, GROWTH_MEASURE_CAP), 1.0 * _base_w(ttm_base_revenue),
                             _base_w(ttm_base_revenue)])
    # 今の伸びは、年次の測り（基準の重みをかけた値）の最大より TTM_GROWTH_EXCESS_CAP 高い所まで
    # （年次の測りが無ければ業種の値〈0以上〉、それも無ければ既定の終点を基準にする）
    _ann_vals = [m[1] * m[3] for m in _growth_meas if m[0] != "ttm_rev_yoy"]
    _anchor = max(_ann_vals) if _ann_vals else (max(industry_g, 0.0) if industry_g is not None else DECAY_END_DEFAULT)
    for _m in _growth_meas:
        if _m[0] == "ttm_rev_yoy":
            _m[1] = min(_m[1], _anchor + TTM_GROWTH_EXCESS_CAP)
    _meas: list[tuple[float, float]] = [(m[1], m[2]) for m in _growth_meas]
    if g_fundamental is not None:
        _meas.append((max(g_fundamental, 0.0), _weight_ramp(g_fundamental)))
    if industry_g is not None:
        _meas.append((max(industry_g, 0.0), _weight_ramp(industry_g)))
    _w_sum = sum(w for _, w in _meas)
    _fill_g = max(phase1_growth, 0.0) if phase1_growth is not None else 0.0
    if _w_sum > 0:
        _mean = sum(v * w for v, w in _meas) / _w_sum
        _conf = min(1.0, _w_sum / FILL_WEIGHT)
        recommended_g_median = _conf * _mean + (1.0 - _conf) * _fill_g
    else:
        recommended_g_median = _fill_g
    # 上限キャップ: max(industry_benchmark × 3, 50%)（通常モデルの側だけ）
    _cap = max(
        industry_g * 3.0 if (industry_g is not None and industry_g > 0) else 0.50,
        0.50,
    )
    recommended_g_median = min(recommended_g_median, _cap)

    # 逓減モデルとの混合: tmax（今の伸び・3年・5年CAGRの最大、正のもの）がDECAY_BLEND_LO〜HIで s=0→1
    _cagr_vals = [v for v in [cagr.get("cagr_3yr"), cagr.get("cagr_5yr")] if v is not None and v > 0]
    _ttm_best_cagr = max(_cagr_vals) if _cagr_vals else None
    _ttm_actual_pos = ttm_actual if (ttm_actual is not None and ttm_actual > 0) else None
    _trigger_vals = [v for v in [_ttm_actual_pos, *_cagr_vals] if v is not None]
    _trigger_max = max(_trigger_vals) if _trigger_vals else None
    growth_blend_s = _smoothstep(((_trigger_max or 0.0) - DECAY_BLEND_LO) / (DECAY_BLEND_HI - DECAY_BLEND_LO))

    # 逓減モデル: TTM/CAGRピークと業種の中間（固定50:50。GROWTH-1のフェーズ別加重は2026-08-26に廃止済み）
    # start_g は「逓減を発動させた最大値」（ttm_actual と cagr_3yr/5yr の最大）、上限100%
    _start_raw_ttm = _ttm_actual_pos or 0.0
    _start_raw_cagr = _ttm_best_cagr or 0.0
    if _start_raw_cagr > _start_raw_ttm:
        _start_raw = _start_raw_cagr
        _start_src = "CAGR_max"
    else:
        _start_raw = _start_raw_ttm
        _start_src = "G入力値"   # phase1_growth (セグメント設定値 or TTM注入値)
    start_g = min(_start_raw, 1.0)
    end_g = max(industry_g, 0.0) if industry_g is not None else DECAY_END_DEFAULT
    _decay_g = start_g * 0.50 + end_g * 0.50
    recommended_g = (1.0 - growth_blend_s) * recommended_g_median + growth_blend_s * _decay_g
    growth_model = "decay" if growth_blend_s >= 0.5 else "median"
    if growth_blend_s >= 0.5:
        growth_model_reason = (
            f"{_start_src}={start_g:.1%}と業界平均{end_g:.1%}の中間値"
            f"{_decay_g:.1%}を採用（固定50:50、将来の成長減速を織り込んだ保守的推定）"
        )
    else:
        _reason_cagr = f"CAGR_max={_trigger_max:.1%}" if _trigger_max is not None else "CAGRデータなし"
        growth_model_reason = f"{_reason_cagr}のため候補の重み付き平均モデル適用"
    if 0.0 < growth_blend_s < 1.0:
        growth_model_reason += f"（逓減モデルとの混合 s={growth_blend_s:.2f}）"

    # 旧規則で「推奨成長率が出ない」銘柄（候補が正の値で2件未満。floor例外つき）。floor_hit・verdict の判定にだけ使う
    # （段1では表示の判定を変えない）
    _legacy_cands = [v for v in (cagr.get("cagr_3yr"), cagr.get("cagr_5yr"), industry_g, g_fundamental)
                     if v is not None and v > 0]
    _legacy_min = 2
    if (
        growth_source == "fcf_cagr"
        and phase1_growth is not None
        and abs(phase1_growth - _FCF_CAGR_FLOOR) < 0.002
        and len(_legacy_cands) == 1
        and industry_g is not None
        and industry_g > 0
    ):
        _legacy_min = 1
    _legacy_rec_none = len(_legacy_cands) < _legacy_min

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
        and _legacy_rec_none
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
        and _legacy_rec_none
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
        "growth_blend_s": growth_blend_s,
        "rec_evidence_weight": _w_sum,
        "rev_cagr_2yr": _cagr_2yr,
        "ttm_rev_yoy": ttm_rev_yoy,
        "growth_model": growth_model,
        "growth_model_reason": growth_model_reason,
        "hype_phase_used": hype_phase,
        "signals": signals,
        "warnings": warnings,
        "fcf_margin_bear_multiplier": fcf_margin_bear_multiplier,
        "fcf_margin_note": fcf_margin_note,
    }
