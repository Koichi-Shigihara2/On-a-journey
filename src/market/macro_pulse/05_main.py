"""
MACRO PULSE — Economic Indicators Auto-Update  v6.0
====================================================
変更点 (v5.0 → v6.0):
  [スキーマ刷新]
    - 05_economic_history.csv → 05_events.csv に移行
    - イベント単位での記録（event_id = {indicator_slug}_{release_date}）
    - 金融環境スナップショット（regime, ff_rate, yc_10y2y, hy_spread, vix, cuts_implied）を同時保存
    - S&P500 t0/t1/t5/t10/t20 と変化率を後から自動補完

  [監視指標]
    手入力:  ISM製造業PMI, ISM非製造業PMI
    FRED自動: NFP, 失業保険4週MA, ミシガン1Y/5Yインフレ期待, CB消費者信頼感,
              住宅建築許可, 10Y-2Yカーブ, HYスプレッド, VIX

  [Discord リマインダー]
    --remind フラグ: 当日発表予定の手入力指標を Discord に通知

  [市場反応自動補完]
    --fill-returns フラグ: sp500_t1/t5/t10/t20 と ret_* を後から補完

  [後方互換]
    --update-schedule, --recalc は引き続き動作
"""

import os, sys, time, json, logging, argparse, traceback, re
from datetime import datetime, timedelta, date, timezone
from io import StringIO

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ============================================================
# 出力先を docs/market-monitor/macro-pulse/data/ に変更
# ============================================================
import pathlib
_SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent.parent.parent
BASE_DATA_DIR = str(_REPO_ROOT / "docs" / "market-monitor" / "macro-pulse" / "data")

EVENTS_PATH      = os.path.join(BASE_DATA_DIR, "05_events.csv")
SCHEDULE_PATH    = os.path.join(BASE_DATA_DIR, "05_indicator_schedule.csv")
FED_CONTEXT_PATH = os.path.join(BASE_DATA_DIR, "05_fed_context.csv")
WEEKLY_ANALYSIS_PATH = os.path.join(BASE_DATA_DIR, "05_weekly_analysis.csv")
LIQUIDITY_PATH   = os.path.join(BASE_DATA_DIR, "05_liquidity.csv")

# common/macro_data - MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替
# （2026-08-12）: FRED系列取得をcommon.macro_data.reader経由に切替。
# collect_and_send.py等と同じHAS_MACRO_DATAガード・二段構えsys.path
# 解決パターンを踏襲する（本ファイルは`python src/market/macro_pulse/
# 05_main.py`で直接実行されるためリポジトリルートがsys.pathに含まれ
# ない。_REPO_ROOTは上で既に計算済みのため流用する）。
HAS_MACRO_DATA = False
_md_reader = None

try:
    _repo_root_str = str(_REPO_ROOT)
    if _repo_root_str not in sys.path:
        sys.path.insert(0, _repo_root_str)
    from common.macro_data import reader as _md_reader
    HAS_MACRO_DATA = True
except Exception:
    pass

if not HAS_MACRO_DATA:
    try:
        _github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if _github_workspace and _github_workspace not in sys.path:
            sys.path.insert(0, _github_workspace)
        from common.macro_data import reader as _md_reader
        HAS_MACRO_DATA = True
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────
#  カラム定義
# ─────────────────────────────────────────────────────────────────
EVENTS_COLUMNS = [
    "event_id", "indicator", "release_date",
    "actual", "consensus", "surprise", "surprise_pct",
    "regime", "ff_rate", "yc_10y2y", "hy_spread", "vix", "cuts_implied",
    "sp500_t0", "sp500_t1", "sp500_t5", "sp500_t10", "sp500_t20",
    "ret_t1", "ret_t5", "ret_t10", "ret_t20",
    "forecast_source", "data_source", "analysis", "updated_at",
]

SCHEDULE_COLUMNS = [
    "indicator", "release_date", "fred_id", "input_method", "consensus", "actual", "status",
]

FED_CONTEXT_COLUMNS = [
    "record_date", "fomc_date", "regime",
    "dominant_concern", "dominant_label",
    "ff_current", "zq_ticker", "zq_price", "zq_rate",
    "cuts_implied", "ai_reason", "regime_source", "updated_at",
]

WEEKLY_ANALYSIS_COLUMNS = [
    "analysis_date", "score", "phase",
    "summary", "factor_analysis", "watchpoints",
    "indicator_comments", "indicator_deltas",
    "score_change_1w", "score_change_1m",
    "surprise_alerts",
    "model", "updated_at",
]

# ─────────────────────────────────────────────────────────────────
#  DESIGN-13: マクロサプライズ検知
# ─────────────────────────────────────────────────────────────────
_SURPRISE_CATEGORIES = {
    "NFP":                              "雇用",
    "Initial Claims 4W MA":            "雇用",
    "Sahm Rule Recession Indicator":   "雇用",
    "Michigan Inflation 1Y":           "インフレ",
    "Michigan Inflation 5Y":           "インフレ",
    "Michigan Consumer Sentiment":     "景気",
    "Philadelphia Fed Manufacturing":  "景気",
    "Chicago Fed National Activity":   "景気",
    "Building Permits":                "景気",
}

# 上昇が悪化方向の指標
_INVERSE_INDICATORS = {
    "Initial Claims 4W MA",
    "Michigan Inflation 1Y",
    "Michigan Inflation 5Y",
    "Sahm Rule Recession Indicator",
}

# 「サプライズ」とみなす前回比の閾値（絶対値）
_SURPRISE_THRESHOLDS = {
    "NFP":                             50_000,  # 5万人
    "Initial Claims 4W MA":            20_000,  # 2万件
    "Sahm Rule Recession Indicator":    0.20,   # 0.2pp
    "Michigan Inflation 1Y":            0.30,   # 0.3pp
    "Michigan Inflation 5Y":            0.30,   # 0.3pp
    "Michigan Consumer Sentiment":      8.0,    # 8pt
    "Philadelphia Fed Manufacturing":  10.0,    # 10pt
    "Chicago Fed National Activity":    0.30,   # 0.3pt
    "Building Permits":               100.0,    # 10万件（単位: 千件）
}


def detect_macro_surprises(events: pd.DataFrame, lookback_days: int = 60) -> list[str]:
    """
    DESIGN-13: 経済指標の前回比急変を検知してアラート文字列リストを返す。

    戻り値: セミコロン区切りで weekly_analysis.csv に保存するリスト要素。
    例: ["[雇用] NFP: -75,000人 ↓ 急悪化（前回250000→最新175000）",
         "[複合景気サプライズ] 景気系指標が複数同時悪化: Philly Fed, CFNAI"]
    """
    _DAILY = {'Yield Curve 10Y-2Y', 'HY Spread', 'VIX', 'Michigan Inflation 5Y'}
    today = date.today()
    cutoff = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")

    recent = events[
        (events["release_date"] >= cutoff) &
        (events["release_date"] <= today_str) &
        (~events["indicator"].isin(_DAILY)) &
        (events["actual"].str.strip() != "") &
        (events["actual"].str.strip() != "nan")
    ].copy()

    alerts: list[str] = []
    category_deteriorated: dict[str, list[str]] = {}

    for ind_name, threshold in _SURPRISE_THRESHOLDS.items():
        rows = recent[recent["indicator"] == ind_name].sort_values("release_date")
        if len(rows) < 2:
            continue
        try:
            val_now  = float(rows.iloc[-1]["actual"])
            val_prev = float(rows.iloc[-2]["actual"])
        except (ValueError, TypeError):
            continue

        delta = val_now - val_prev
        if abs(delta) < threshold:
            continue

        is_inverse  = ind_name in _INVERSE_INDICATORS
        deteriorated = (delta > 0) if is_inverse else (delta < 0)
        direction   = "急悪化" if deteriorated else "急改善"
        arrow       = "↑" if delta > 0 else "↓"
        cat         = _SURPRISE_CATEGORIES.get(ind_name, "その他")

        # 単位に応じたフォーマット
        if ind_name == "NFP":
            delta_str = f"{delta:+,.0f}人"
        elif ind_name == "Initial Claims 4W MA":
            delta_str = f"{delta:+,.0f}件"
        elif ind_name == "Building Permits":
            delta_str = f"{delta:+.1f}千件"
        else:
            delta_str = f"{delta:+.2f}"

        alerts.append(
            f"[{cat}] {ind_name}: {delta_str} {arrow} {direction}"
            f"（前回{val_prev:.3g}→最新{val_now:.3g}）"
        )
        if deteriorated:
            category_deteriorated.setdefault(cat, []).append(ind_name)

    # 複合サプライズ: 同カテゴリで2件以上悪化
    for cat, inds in category_deteriorated.items():
        if len(inds) >= 2:
            alerts.append(
                f"[複合{cat}サプライズ] {cat}系指標が同時悪化: {' / '.join(inds)}"
            )

    return alerts

# ─────────────────────────────────────────────────────────────────
#  指標マスタ（v6.0 確定12指標）※変更なし
# ─────────────────────────────────────────────────────────────────
INDICATOR_CONFIG = {
    "Philadelphia Fed Manufacturing": {
        "fred_id": "GACDFSA066MSFRBPHI",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "philly_fed_mfg",
        "threshold_bull": 5.0,
        "threshold_bear": 0.0,
        "unit": "index",
        "discord_remind": False,
    },
    "Chicago Fed National Activity": {
        "fred_id": "CFNAI",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "cfnai_ma3",
        "threshold_bull": 0.0,
        "threshold_bear": -0.35,
        "unit": "index",
        "discord_remind": False,
    },
    # ── FRED自動取得 ─────────────────────────────────────────────
    "NFP": {
        "fred_id": "PAYEMS",
        "input_method": "FRED",
        "fred_release_id": 50,
        "obs_to_release_lag": 35,  # obs_date=月初1日, 翌月第1金曜に発表
        "slug": "nfp",
        "threshold_bull": 200000,
        "threshold_bear": 100000,
        "unit": "千人",
        "discord_remind": False,
    },
    "Initial Claims 4W MA": {
        "fred_id": "IC4WSA",
        "input_method": "FRED",
        "fred_release_id": 321,
        "obs_to_release_lag": 7,   # obs_date=週末, 翌木曜に発表
        "slug": "ic4wsa",
        "threshold_bull": 250000,
        "threshold_bear": 300000,
        "unit": "件",
        "discord_remind": False,
    },
    "Michigan Inflation 1Y": {
        "fred_id": "MICH",
        "input_method": "FRED",
        "fred_release_id": None,
        "obs_to_release_lag": 10,  # obs_date=月初1日, 速報は約10日後
        "michigan_rule": True,
        "slug": "mich_1y",
        "threshold_bull": 2.5,
        "threshold_bear": 4.0,
        "unit": "%",
        "discord_remind": False,
    },
    "Michigan Inflation 5Y": {
        "fred_id": "T5YIE",          # 5-Year Breakeven Inflation Rate（市場ベース代替）
        "input_method": "FRED",
        "fred_release_id": None,
        "michigan_rule": True,
        "slug": "mich_5y",
        "threshold_bull": 2.5,
        "threshold_bear": 3.5,
        "unit": "%",
        "discord_remind": False,
    },
    "Michigan Consumer Sentiment": {
        "fred_id": "UMCSENT",
        "input_method": "FRED",
        "fred_release_id": None,
        "obs_to_release_lag": 10,  # obs_date=月初1日, 速報は約10日後
        "michigan_rule": True,
        "slug": "mich_sent",
        "threshold_bull": 90.0,
        "threshold_bear": 70.0,
        "unit": "index",
        "discord_remind": False,
    },
    "Building Permits": {
        "fred_id": "PERMIT",
        "input_method": "FRED",
        "fred_release_id": None,   # FRED Release Calendar が空のためルールベース算出
        "obs_to_release_lag": 47,  # obs_date=月初1日, 翌々月第3週に発表
        "permit_rule": True,       # 毎月第3週火曜（Housing Starts と同日発表）
        "slug": "permit",
        "threshold_bull": 1400.0,
        "threshold_bear": 1200.0,
        "unit": "千件",
        "discord_remind": False,
    },
    # ── サームルール（月次自動）────────────────────────────
    "Sahm Rule Recession Indicator": {
        "fred_id": "SAHMCURRENT",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "sahm_rule",
        "threshold_bull": 0.3,
        "threshold_bear": 0.5,
        "unit": "pp",
        "discord_remind": False,
    },
    # ── デイリー指標（毎日自動記録）────────────────────────────
    "Yield Curve 10Y-2Y": {
        "fred_id": "T10Y2Y",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "yc_10y2y",
        "unit": "%",
        "daily": True,
        "discord_remind": False,
    },
    "HY Spread": {
        "fred_id": "BAMLH0A0HYM2",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "hy_spread",
        "unit": "%",
        "daily": True,
        "discord_remind": False,
    },
    "VIX": {
        "fred_id": "VIXCLS",
        "input_method": "FRED",
        "fred_release_id": None,
        "slug": "vix",
        "unit": "index",
        "daily": True,
        "discord_remind": False,
    },
}

# ─────────────────────────────────────────────────────────────────
#  event_id 生成
# ─────────────────────────────────────────────────────────────────
def make_event_id(indicator: str, release_date) -> str:
    slug = INDICATOR_CONFIG.get(indicator, {}).get("slug", "")
    if not slug:
        slug = re.sub(r"[^a-z0-9]+", "_", indicator.lower()).strip("_")
    if isinstance(release_date, date):
        date_str = release_date.strftime("%Y-%m-%d")
    else:
        date_str = str(release_date)
    return f"{slug}_{date_str}"

# ─────────────────────────────────────────────────────────────────
#  米国祝日・営業日計算（変更なし）
# ─────────────────────────────────────────────────────────────────
def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + (n - 1) * 7)

def us_holidays(year: int) -> set:
    import calendar
    def last_weekday(y, m, wd):
        last_day = calendar.monthrange(y, m)[1]
        last = date(y, m, last_day)
        delta = (last.weekday() - wd) % 7
        return last - timedelta(days=delta)

    holidays = set()
    ny = date(year, 1, 1)
    if ny.weekday() == 5:   ny = date(year, 12, 31)
    elif ny.weekday() == 6: ny = date(year, 1, 2)
    holidays.add(ny)
    holidays.add(nth_weekday(year, 1, 0, 3))
    holidays.add(nth_weekday(year, 2, 0, 3))
    holidays.add(last_weekday(year, 5, 0))
    jul4 = date(year, 7, 4)
    if jul4.weekday() == 5:   jul4 = date(year, 7, 3)
    elif jul4.weekday() == 6: jul4 = date(year, 7, 5)
    holidays.add(jul4)
    holidays.add(nth_weekday(year, 9, 0, 1))
    holidays.add(nth_weekday(year, 10, 0, 2))
    nov11 = date(year, 11, 11)
    if nov11.weekday() == 5:   nov11 = date(year, 11, 10)
    elif nov11.weekday() == 6: nov11 = date(year, 11, 12)
    holidays.add(nov11)
    holidays.add(nth_weekday(year, 11, 3, 4))
    xmas = date(year, 12, 25)
    if xmas.weekday() == 5:   xmas = date(year, 12, 24)
    elif xmas.weekday() == 6: xmas = date(year, 12, 26)
    holidays.add(xmas)
    return holidays

def nth_us_business_day(year: int, month: int, n: int) -> date:
    import calendar
    holidays = us_holidays(year) | us_holidays(year - 1) | us_holidays(year + 1)
    count = 0
    d = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    while d <= date(year, month, last_day):
        if d.weekday() < 5 and d not in holidays:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)
    raise ValueError(f"{year}-{month:02d} の第{n}営業日が見つかりません")

def us_business_days_add(start: date, n: int) -> date:
    holidays = us_holidays(start.year) | us_holidays(start.year + 1)
    count = 0
    d = start + timedelta(days=1)
    while True:
        if d.weekday() < 5 and d not in holidays:
            count += 1
            if count == n:
                return d
        d += timedelta(days=1)

def michigan_consumer_sentiment_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            first_day = date(year, month, 1)
            first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
            second_friday = first_friday + timedelta(weeks=1)
            if second_friday >= today:
                results.append(("Michigan Consumer Sentiment", second_friday))
        except Exception as e:
            logger.warning(f"Michigan Consumer Sentiment date calc error: {e}")
    return results

def building_permit_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            release_date = nth_weekday(year, month, 1, 3)
            if release_date >= today:
                results.append(("Building Permits", release_date))
        except Exception as e:
            logger.warning(f"Building Permits date calc error: {e}")
    return results

def michigan_release_dates(months_ahead: int = 3) -> list[tuple[str, date]]:
    today = date.today()
    results = []
    for offset in range(months_ahead + 1):
        year  = today.year + (today.month - 1 + offset) // 12
        month = (today.month - 1 + offset) % 12 + 1
        try:
            prelim = nth_weekday(year, month, 4, 2)
            final  = nth_weekday(year, month, 4, 4)
            for nm, rd in [("Michigan Inflation 1Y", prelim), ("Michigan Inflation 5Y", prelim),
                            ("Michigan Inflation 1Y", final),  ("Michigan Inflation 5Y", final)]:
                if rd >= today:
                    results.append((nm, rd))
        except Exception as e:
            logger.warning(f"Michigan date calc error: {e}")
    return results

# ─────────────────────────────────────────────────────────────────
#  FRED Release Calendar（変更なし）
# ─────────────────────────────────────────────────────────────────
def fred_release_dates(fred_api_key: str, days_ahead: int = 90) -> dict[str, list[date]]:
    today    = date.today()
    end_date = today + timedelta(days=days_ahead)
    results  = {}
    for ind_name, cfg in INDICATOR_CONFIG.items():
        release_id = cfg.get("fred_release_id")
        if not release_id:
            continue
        all_dates = []
        url = (
            f"https://api.stlouisfed.org/fred/release/dates"
            f"?release_id={release_id}"
            f"&realtime_start={today.strftime('%Y-%m-%d')}"
            f"&realtime_end={end_date.strftime('%Y-%m-%d')}"
            f"&include_release_dates_with_no_data=true"
            f"&api_key={fred_api_key}"
            f"&file_type=json"
        )
        for attempt in range(3):
            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                data  = r.json()
                dates = [
                    datetime.strptime(d["date"], "%Y-%m-%d").date()
                    for d in data.get("release_dates", [])
                    if datetime.strptime(d["date"], "%Y-%m-%d").date() >= today
                ]
                all_dates.extend(dates)
                time.sleep(0.3)
                break
            except Exception as e:
                wait = 2 ** attempt
                if attempt < 2:
                    logger.warning(f"[FRED Release] {ind_name} attempt {attempt+1}: {e} → retry {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"[FRED Release] {ind_name} failed: {e}")
        results[ind_name] = sorted(set(all_dates))
        logger.info(f"[FRED Release] {ind_name}: {[str(d) for d in results[ind_name]]}")
    return results

# ─────────────────────────────────────────────────────────────────
#  スケジュール CSV（v6.0 スキーマ）※パス修正のみ
# ─────────────────────────────────────────────────────────────────
def ensure_schedule_csv():
    if os.path.exists(SCHEDULE_PATH):
        return
    pd.DataFrame(columns=SCHEDULE_COLUMNS).to_csv(SCHEDULE_PATH, index=False, encoding="utf-8")
    logger.info(f"Created schedule: {SCHEDULE_PATH}")

def load_schedule() -> pd.DataFrame:
    if not os.path.exists(SCHEDULE_PATH):
        return pd.DataFrame(columns=SCHEDULE_COLUMNS)
    df = pd.read_csv(SCHEDULE_PATH, encoding="utf-8", dtype=str).fillna("")
    if "指標名" in df.columns and "indicator" not in df.columns:
        df = df.rename(columns={"指標名": "indicator", "発表予定日": "release_date"})
    for col in SCHEDULE_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df

def update_schedule(fred_api_key: str, days_ahead: int = 90):
    ensure_schedule_csv()
    df = load_schedule()
    registered = set(zip(df["indicator"], df["release_date"]))

    # events.csv に actual が入っている (indicator, release_date) はスケジュールに追加しない
    events_df = load_events()
    already_released: set = set()
    if not events_df.empty:
        has_actual = (
            events_df["actual"].str.strip().ne("") &
            events_df["actual"].str.strip().ne("nan")
        )
        for _, ev in events_df[has_actual].iterrows():
            already_released.add((ev["indicator"], ev["release_date"]))

    def _skip(ind_name: str, date_str: str) -> bool:
        return (ind_name, date_str) in registered or (ind_name, date_str) in already_released

    new_rows = []

    for ind_name, dates in fred_release_dates(fred_api_key, days_ahead).items():
        cfg = INDICATOR_CONFIG.get(ind_name, {})
        for rd in dates:
            date_str = rd.strftime("%Y-%m-%d")
            if _skip(ind_name, date_str):
                continue
            new_rows.append({
                "indicator":    ind_name,
                "release_date": date_str,
                "fred_id":      cfg.get("fred_id", ""),
                "input_method": cfg.get("input_method", "FRED"),
                "consensus":    "",
                "actual":       "",
                "status":       "scheduled",
            })

    for ind_name, rd in michigan_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if _skip(ind_name, date_str):
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", ""),
            "input_method": "FRED",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })

    for ind_name, rd in building_permit_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if _skip(ind_name, date_str):
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", ""),
            "input_method": "FRED",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })
        logger.info(f"[Schedule+] {ind_name}: {date_str} (第3週火曜 ルールベース算出)")

    for ind_name, rd in michigan_consumer_sentiment_release_dates(months_ahead=3):
        date_str = rd.strftime("%Y-%m-%d")
        if _skip(ind_name, date_str):
            continue
        new_rows.append({
            "indicator":    ind_name,
            "release_date": date_str,
            "fred_id":      INDICATOR_CONFIG.get(ind_name, {}).get("fred_id", ""),
            "input_method": "FRED",
            "consensus":    "",
            "actual":       "",
            "status":       "scheduled",
        })
        logger.info(f"[Schedule+] {ind_name}: {date_str} (第2金曜 ルールベース算出)")

    if not new_rows:
        logger.info("Schedule up to date.")
        return

    new_df = pd.DataFrame(new_rows, columns=SCHEDULE_COLUMNS)
    combined = pd.concat([df, new_df], ignore_index=True)
    combined = combined.sort_values(["release_date", "indicator"]).reset_index(drop=True)
    combined.to_csv(SCHEDULE_PATH, index=False, encoding="utf-8")
    logger.info(f"Schedule updated: +{len(new_rows)} rows")

# ─────────────────────────────────────────────────────────────────
#  Discord リマインダー（変更なし）
# ─────────────────────────────────────────────────────────────────
def send_discord(message: str):
    webhook_url = os.environ.get("DISCORD_WEB_HOOK", "")
    if not webhook_url:
        logger.warning("DISCORD_WEB_HOOK not set. Skip notification.")
        return
    try:
        r = requests.post(webhook_url, json={"content": message}, timeout=10)
        r.raise_for_status()
        logger.info("Discord notification sent.")
    except Exception as e:
        logger.warning(f"Discord notification failed: {e}")

def remind_manual_indicators(target_date: date):
    schedule = load_schedule()
    date_str = target_date.strftime("%Y-%m-%d")
    today_rows = schedule[
        (schedule["release_date"] == date_str) &
        (schedule["input_method"] == "manual")
    ]

    if today_rows.empty:
        logger.info(f"No manual indicators today ({date_str}).")
        return

    lines = [f"📊 **MACRO PULSE — 手入力リマインダー** ({date_str})"]
    for _, row in today_rows.iterrows():
        ind  = row["indicator"]
        cons = row["consensus"]
        cons_str = f"  コンセンサス: {cons}" if cons else "  コンセンサス: 未設定"
        lines.append(f"• **{ind}**\n{cons_str}")

    lines.append("\n→ `data/05_indicator_schedule.csv` の `actual` 列に値を入力してください。")
    send_discord("\n".join(lines))
    logger.info(f"Reminded {len(today_rows)} manual indicators.")

def remind_missing_actuals(target_date: date):
    schedule = load_schedule()
    cutoff = (target_date - timedelta(days=30)).strftime("%Y-%m-%d")
    today_str = target_date.strftime("%Y-%m-%d")
    missing = schedule[
        (schedule["release_date"] >= cutoff) &
        (schedule["release_date"] <= today_str) &
        (schedule["input_method"] == "manual") &
        (schedule["actual"].str.strip() == "")
    ]

    if missing.empty:
        logger.info("No missing actuals.")
        return

    lines = [f"⚠️ **MACRO PULSE — 未入力アラート** ({today_str})"]
    for _, row in missing.iterrows():
        lines.append(f"• **{row['indicator']}** ({row['release_date']}) — actual 未入力")
    lines.append("\n→ `data/05_indicator_schedule.csv` を更新してください。")
    send_discord("\n".join(lines))
    logger.info(f"Missing actuals alert: {len(missing)} rows")

# ─────────────────────────────────────────────────────────────────
#  FRED アクセス（common.macro_data.reader経由に切替、
#  MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替、2026-08-12）
# ─────────────────────────────────────────────────────────────────
def fred_latest(series_id: str):
    """common.macro_data.reader.get_latest()経由でFRED系列の最新値を返す。
    戻り値は旧実装（fredapi直接呼び出し）と同じ(value, date)形状を維持
    する（呼び出し側の変更を最小化するため）。取得失敗時・データ未取得
    系列は(None, None)を返す（例外は投げない、旧実装と同じエラー耐性）。

    旧実装は`target_date`/`lookback`引数で「target_date以前・lookback
    日以内の直近値」を都度FREDへ問い合わせていたが、reader.get_latest()
    は`common/macro_data/series/{series_id}.json`に保存済みの最新値を
    返す設計のため、これらの引数は廃止した（本番の日次cron実行では
    target_date=today相当のため実質的な挙動差はない。詳細はBACKLOG
    [[MACRODATA-LAYER-CONSTRUCTION-1]]着手順序参照）。
    """
    if not HAS_MACRO_DATA:
        logger.warning(f"FRED [{series_id}]: common.macro_data.reader が利用できません")
        return None, None
    rec = _md_reader.get_latest(series_id)
    if not rec or rec.get("value") is None:
        return None, None
    try:
        obs_date = date.fromisoformat(rec["as_of"])
    except (KeyError, ValueError, TypeError):
        obs_date = None
    return float(rec["value"]), obs_date

def fred_latest_with_prev(series_id: str):
    """
    MACRO-NFP-1: fred_latest()に加え、直前の観測値も取得する。
    PAYEMS（雇用者数の水準）から前月比を算出する用途で使用する。
    戻り値: (最新値, 最新観測日, 直前値, 直前観測日)。
    直前値が取得できない場合は (val, date, None, None)。

    common.macro_data.reader.get_series()で観測日昇順の全期間データを
    取得し末尾2件を使う（MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替、
    2026-08-12）。
    """
    if not HAS_MACRO_DATA:
        logger.warning(f"FRED [{series_id}]: common.macro_data.reader が利用できません")
        return None, None, None, None
    records = _md_reader.get_series(series_id)
    if not records:
        return None, None, None, None
    try:
        val_now = float(records[-1]["value"])
        date_now = date.fromisoformat(records[-1]["as_of"])
    except (KeyError, ValueError, TypeError, IndexError):
        return None, None, None, None
    val_prev, date_prev = None, None
    if len(records) >= 2:
        try:
            val_prev = float(records[-2]["value"])
            date_prev = date.fromisoformat(records[-2]["as_of"])
        except (KeyError, ValueError, TypeError):
            val_prev, date_prev = None, None
    return val_now, date_now, val_prev, date_prev

def get_ff_current():
    v_hi, _ = fred_latest("DFEDTARU")
    v_lo, _ = fred_latest("DFEDTARL")
    if v_hi is not None and v_lo is not None:
        return round((v_hi + v_lo) / 2, 4)
    v, _ = fred_latest("FEDFUNDS")
    return round(v, 4) if v is not None else None

def get_implied_cuts(target_date: date = None):
    """
    1Y EXPECTED FF = DGS1（FRED: 1年国債利回り）をそのまま使用。
    term premium 補正や ZQ=F 先物取得は廃止し、シンプルに DGS1 を採用。

    IMPLIED CUTS = (ff_current - DGS1) / 0.25
      正値: 市場が利下げを織り込み（DGS1 < ff_current）
      負値: 市場が利上げ/高止まりを織り込み（DGS1 > ff_current）

    target_dateはfred_latest()がcommon.macro_data.reader経由に切替後
    使用しなくなったため事実上未使用（呼び出し側との互換のため引数
    自体は残置、MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替）。
    """
    dgs1, _ = fred_latest("DGS1")
    if dgs1 is None:
        return None, None, None

    return "FRED:DGS1", None, round(dgs1, 4)


# 後方互換エイリアス
def get_zq_futures(target_date: date = None):
    return get_implied_cuts(target_date)

# ─────────────────────────────────────────────────────────────────
#  金融環境スナップショット
# ─────────────────────────────────────────────────────────────────
def get_financial_context(target_date: date = None) -> dict:
    """target_dateはfred_latest()がcommon.macro_data.reader経由に
    切替後使用しなくなったため事実上未使用（呼び出し側との互換のため
    引数自体は残置、MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替）。"""
    ctx = {
        "regime": "BALANCED",
        "ff_rate": None,
        "yc_10y2y": None,
        "hy_spread": None,
        "vix": None,
        "cuts_implied": None,
    }

    if os.path.exists(FED_CONTEXT_PATH):
        try:
            fc = pd.read_csv(FED_CONTEXT_PATH, dtype=str).fillna("")
            if not fc.empty:
                last = fc.iloc[-1]
                ctx["regime"]       = last.get("regime", "BALANCED")
                ctx["ff_rate"]      = _safe_float(last.get("ff_current"))
                ctx["cuts_implied"] = _safe_float(last.get("cuts_implied"))
        except Exception as e:
            logger.warning(f"fed_context read: {e}")

    yc, _ = fred_latest("T10Y2Y")
    hy, _ = fred_latest("BAMLH0A0HYM2")
    vx, _ = fred_latest("VIXCLS")
    if yc is not None: ctx["yc_10y2y"]  = round(yc, 4)
    if hy is not None: ctx["hy_spread"]  = round(hy, 4)
    if vx is not None: ctx["vix"]        = round(vx, 2)
    ff = get_ff_current()
    if ff is not None: ctx["ff_rate"]    = ff

    return ctx

def _safe_float(v):
    try:
        return float(v) if v not in (None, "", "nan") else None
    except (ValueError, TypeError):
        return None

# ─────────────────────────────────────────────────────────────────
#  S&P500 取得（変更なし）
# ─────────────────────────────────────────────────────────────────
def _stooq(symbol: str, target_date: date):
    try:
        d1 = (target_date - timedelta(days=10)).strftime("%Y%m%d")
        d2 = target_date.strftime("%Y%m%d")
        url = f"https://stooq.com/q/d/l/?s={symbol}&d1={d1}&d2={d2}&i=d"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        txt = r.text.strip()
        if not txt or "No data" in txt:
            return None
        df = pd.read_csv(StringIO(txt))
        df.columns = [c.strip() for c in df.columns]
        if "Close" not in df.columns or df.empty:
            return None
        df["Date"] = pd.to_datetime(df["Date"])
        return round(float(df.sort_values("Date")["Close"].iloc[-1]), 2)
    except Exception as e:
        logger.warning(f"stooq [{symbol}]: {e}")
        return None

def get_sp500(target_date: date):
    v, _ = fred_latest("SP500")
    if v:
        return v
    return _stooq("%5Espx", target_date)

# ─────────────────────────────────────────────────────────────────
#  events.csv I/O（パス修正のみ）
# ─────────────────────────────────────────────────────────────────
def load_events() -> pd.DataFrame:
    if not os.path.exists(EVENTS_PATH):
        return pd.DataFrame(columns=EVENTS_COLUMNS)
    try:
        df = pd.read_csv(EVENTS_PATH, encoding="utf-8", dtype=str).fillna("")
        for c in EVENTS_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        return df
    except Exception as e:
        logger.warning(f"events.csv read error: {e}")
        return pd.DataFrame(columns=EVENTS_COLUMNS)

def save_events(df: pd.DataFrame):
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    df = df.drop_duplicates(subset=["event_id"], keep="last")
    df = df.sort_values(["release_date", "indicator"]).reset_index(drop=True)
    df.to_csv(EVENTS_PATH, index=False, encoding="utf-8")
    logger.info(f"events.csv saved: {EVENTS_PATH} ({len(df)} rows)")

# ─────────────────────────────────────────────────────────────────
#  期待値解決（変更なし）
# ─────────────────────────────────────────────────────────────────
def resolve_forecast(indicator: str, release_date_str: str, actual_val,
                     schedule: pd.DataFrame, events: pd.DataFrame):
    mask = (schedule["indicator"] == indicator) & (schedule["release_date"] == release_date_str)
    hits = schedule[mask]
    if not hits.empty:
        cons_str = hits.iloc[-1].get("consensus", "")
        if cons_str and cons_str.strip():
            try:
                fv = float(cons_str)
                src = "user"
                surp = round(actual_val - fv, 4) if actual_val is not None else None
                surp_pct = round(surp / abs(fv) * 100, 4) if (surp is not None and fv != 0) else None
                return fv, src, surp, surp_pct
            except (ValueError, TypeError):
                pass

    if not events.empty:
        ev_mask = (events["indicator"] == indicator) & (events["release_date"] == release_date_str)
        ev_hits = events[ev_mask]
        if not ev_hits.empty:
            cons_str = ev_hits.iloc[-1].get("consensus", "")
            if cons_str and cons_str.strip():
                try:
                    fv = float(cons_str)
                    src = str(ev_hits.iloc[-1].get("forecast_source", "stored") or "stored")
                    surp = round(actual_val - fv, 4) if actual_val is not None else None
                    surp_pct = round(surp / abs(fv) * 100, 4) if (surp is not None and fv != 0) else None
                    return fv, src, surp, surp_pct
                except (ValueError, TypeError):
                    pass

    if actual_val is not None:
        return actual_val, "actual_as_forecast", 0.0, 0.0
    return None, "none", None, None

# ─────────────────────────────────────────────────────────────────
#  指標フェッチ → event row 生成（変更なし）
# ─────────────────────────────────────────────────────────────────
def fetch_event_row(indicator: str, target_date: date,
                    fin_ctx: dict, schedule: pd.DataFrame,
                    events: pd.DataFrame,
                    override_actual=None) -> dict:
    cfg      = INDICATOR_CONFIG.get(indicator, {})
    fred_id  = cfg.get("fred_id", "")
    date_str = target_date.strftime("%Y-%m-%d")
    event_id = make_event_id(indicator, target_date)

    row = {col: "" for col in EVENTS_COLUMNS}
    row.update({
        "event_id":    event_id,
        "indicator":   indicator,
        "release_date": date_str,
        "regime":      fin_ctx.get("regime", ""),
        "ff_rate":     _fmt(fin_ctx.get("ff_rate")),
        "yc_10y2y":    _fmt(fin_ctx.get("yc_10y2y")),
        "hy_spread":   _fmt(fin_ctx.get("hy_spread")),
        "vix":         _fmt(fin_ctx.get("vix")),
        "cuts_implied": _fmt(fin_ctx.get("cuts_implied")),
        "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

    actual_val = override_actual

    # MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替（2026-08-12）:
    # common.macro_data.reader経由（ローカルファイル読み取りのみ）に
    # 切替たため、ネットワークI/Oを前提としたリトライ・指数バックオフ
    # （旧: for attempt in range(3): ... time.sleep(2**attempt)）は不要
    # になり削除した。
    if fred_id and actual_val is None:
        if indicator == "NFP":
            # MACRO-NFP-1: PAYEMSは雇用者数の「水準」のため、
            # 前月からの増減（人）に変換してから格納する
            level_now, d, level_prev, _ = fred_latest_with_prev(fred_id)
            if level_now is not None and level_prev is not None:
                actual_val = round((level_now - level_prev) * 1000)
                if d:
                    row["release_date"] = d.strftime("%Y-%m-%d")
                    row["event_id"]     = make_event_id(indicator, d)
        else:
            a, d = fred_latest(fred_id)
            if a is not None:
                actual_val = a
                if d:
                    row["release_date"] = d.strftime("%Y-%m-%d")
                    row["event_id"]     = make_event_id(indicator, d)

    row["actual"] = _fmt(actual_val)

    fv, src, surp, surp_pct = resolve_forecast(
        indicator, row["release_date"], actual_val, schedule, events)
    row["consensus"]     = _fmt(fv)
    row["surprise"]      = _fmt(surp)
    row["surprise_pct"]  = _fmt(surp_pct)
    row["forecast_source"] = src or ""
    row["data_source"]   = "FRED" if (fred_id and override_actual is None) else \
                           ("manual" if override_actual is not None else "N/A")

    return row

def _fmt(v) -> str:
    if v is None or v == "" :
        return ""
    try:
        f = float(v)
        if f != f:
            return ""
        return str(v)
    except (ValueError, TypeError):
        return str(v)

# ─────────────────────────────────────────────────────────────────
#  S&P500 変化率の後補完 (--fill-returns) パス修正のみ
# ─────────────────────────────────────────────────────────────────
def _load_sp500_cache(from_date: str, to_date: str) -> pd.Series:
    """MACRODATA-LAYER-CONSTRUCTION-1本番消費者切替（2026-08-12）:
    fred.get_series()直接呼び出しをcommon.macro_data.reader.get_series()
    経由に切替（fill_returns()が複数日分の履歴を必要とするため、単一値
    のfred_latest()ではなくget_series()を使う）。"""
    logger.info(f"S&P500 一括取得中 ({from_date} 〜 {to_date})...")
    if HAS_MACRO_DATA:
        try:
            records = _md_reader.get_series("SP500", start=from_date, end=to_date)
            if records:
                s = pd.Series(
                    {pd.Timestamp(r["as_of"]): float(r["value"]) for r in records}
                ).sort_index()
                if not s.empty:
                    logger.info(f"S&P500 (FRED): {len(s)} obs")
                    return s
        except Exception as e:
            logger.warning(f"S&P500 FRED: {e} → stooq fallback")

    try:
        d1 = from_date.replace("-", "")
        d2 = to_date.replace("-", "")
        url = f"https://stooq.com/q/d/l/?s=%5Espx&d1={d1}&d2={d2}&i=d"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        df = pd.read_csv(StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        df["Date"] = pd.to_datetime(df["Date"])
        s = df.set_index("Date")["Close"].dropna()
        logger.info(f"S&P500 (stooq): {len(s)} obs")
        return s
    except Exception as e:
        logger.warning(f"S&P500 stooq: {e}")
        return pd.Series(dtype=float)

def _lookup_sp500(cache: pd.Series, target_date: date):
    if cache.empty:
        return None
    td = pd.Timestamp(target_date)
    idx = cache.index
    if hasattr(idx, 'tz') and idx.tz is not None:
        idx = idx.tz_localize(None)
        cache = pd.Series(cache.values, index=idx)
    s = cache[cache.index <= td]
    if s.empty:
        return None
    return round(float(s.iloc[-1]), 2)

def fill_returns():
    events = load_events()
    if events.empty:
        logger.info("No events to fill.")
        return

    today = date.today()
    DAILY_INDS_SET = {'Yield Curve 10Y-2Y', 'HY Spread', 'VIX', 'Michigan Inflation 5Y'}
    need = events[
        (events["release_date"] != "") &
        (~events["indicator"].isin(DAILY_INDS_SET)) &
        (
            (events["sp500_t0"] == "") |
            (events["sp500_t1"] == "") |
            (events["sp500_t5"] == "") |
            (events["sp500_t10"] == "") |
            (events["sp500_t20"] == "")
        )
    ]
    if need.empty:
        logger.info("fill-returns: nothing to update.")
        return

    raw_min = pd.to_datetime(need["release_date"].min()).date()
    min_date = (raw_min - timedelta(days=7)).strftime("%Y-%m-%d")
    max_rd   = pd.to_datetime(need["release_date"].max()).date()
    max_date = min(today, max_rd + timedelta(days=45)).strftime("%Y-%m-%d")
    logger.info(f"fill-returns: {len(need)} rows need update ({need['release_date'].min()} 〜 {need['release_date'].max()})")

    sp_cache = _load_sp500_cache(min_date, max_date)
    if sp_cache.empty:
        logger.error("S&P500 cache empty. Cannot fill returns.")
        return

    updated = 0
    skip_no_sp0 = 0
    skip_future = 0
    skip_no_spn = 0
    DAILY_INDS = {'Yield Curve 10Y-2Y', 'HY Spread', 'VIX', 'Michigan Inflation 5Y'}

    for idx, row in need.iterrows():
        try:
            rd = datetime.strptime(row["release_date"], "%Y-%m-%d").date()
        except Exception as e:
            logger.warning(f"fill-returns: release_date parse error idx={idx} val={repr(row['release_date'])}: {e}")
            continue

        if not events.at[idx, "sp500_t0"]:
            sp0 = _lookup_sp500(sp_cache, rd)
            if sp0:
                events.at[idx, "sp500_t0"] = str(sp0)
                updated += 1
            else:
                skip_no_sp0 += 1
                if skip_no_sp0 <= 3:
                    logger.warning(f"fill-returns: sp0 not found for {row['indicator']} {rd} (cache range: {sp_cache.index.min()} 〜 {sp_cache.index.max()})")
                continue

        t0_str = events.at[idx, "sp500_t0"]
        if not t0_str:
            continue
        try:
            t0_val = float(t0_str)
        except (ValueError, TypeError):
            continue

        if row.get("indicator", "") in DAILY_INDS:
            continue

        for n, col_sp, col_ret in [
            (1,  "sp500_t1",  "ret_t1"),
            (5,  "sp500_t5",  "ret_t5"),
            (10, "sp500_t10", "ret_t10"),
            (20, "sp500_t20", "ret_t20"),
        ]:
            if events.at[idx, col_sp]:
                continue
            target_n = us_business_days_add(rd, n)
            if target_n > today:
                skip_future += 1
                continue
            sp_n = _lookup_sp500(sp_cache, target_n)
            if sp_n is None:
                skip_no_spn += 1
                continue
            ret_n = round((sp_n - t0_val) / t0_val * 100, 4)
            events.at[idx, col_sp]  = str(sp_n)
            events.at[idx, col_ret] = str(ret_n)
            updated += 1

    logger.info(f"fill-returns stats: skip_no_sp0={skip_no_sp0} skip_future={skip_future} skip_no_spn={skip_no_spn}")

    if updated:
        save_events(events)
        logger.info(f"fill-returns: {updated} cells updated.")
    else:
        logger.info("fill-returns: nothing to update.")

# ─────────────────────────────────────────────────────────────────
#  --recalc（サプライズ再計算）（変更なし）
# ─────────────────────────────────────────────────────────────────
def recalc(events: pd.DataFrame) -> pd.DataFrame:
    updated = 0
    for idx, row in events.iterrows():
        try:
            actual   = float(row["actual"])
            forecast = float(row["consensus"])
        except (ValueError, TypeError):
            continue
        src     = str(row.get("forecast_source", "") or "")
        new_sur = round(actual - forecast, 4)
        new_pct = round(new_sur / abs(forecast) * 100, 4) if forecast != 0 else 0.0
        old_sur = row.get("surprise", "")

        if src == "actual_as_forecast" and forecast != actual:
            events.at[idx, "surprise"]       = str(new_sur)
            events.at[idx, "surprise_pct"]   = str(new_pct)
            events.at[idx, "forecast_source"] = "user_retroactive"
            events.at[idx, "updated_at"]     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated += 1
            logger.info(f"[RECALC] {row['event_id']}: {old_sur} → {new_sur}")
        elif src in ("user", "user_retroactive"):
            old_str = str(old_sur)
            if old_str != str(new_sur):
                events.at[idx, "surprise"]     = str(new_sur)
                events.at[idx, "surprise_pct"] = str(new_pct)
                events.at[idx, "updated_at"]   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updated += 1
    logger.info(f"Recalc done: {updated} rows updated.")
    return events

# ─────────────────────────────────────────────────────────────────
#  fed_context.csv 更新（v5から継承）パス修正のみ
# ─────────────────────────────────────────────────────────────────
def fetch_latest_fomc_statement():
    cal_url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    found_url = None
    fomc_date = None
    try:
        r = requests.get(cal_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r.raise_for_status()
        html = r.text
        pats = [
            r'href="(/newsevents/pressreleases/monetary(\d{8})a\d?\.htm)"',
            r'href="(/monetarypolicy/(\d{8})a\d?\.htm)"',
        ]
        candidates = []
        for pat in pats:
            for path, dt in re.findall(pat, html):
                candidates.append((dt, path))
        if candidates:
            candidates.sort(key=lambda x: x[0], reverse=True)
            best_dt, best_path = candidates[0]
            found_url = "https://www.federalreserve.gov" + best_path
            fomc_date = datetime.strptime(best_dt, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(f"FOMC calendar fetch: {e}")

    if not found_url:
        known = ["20260507","20260318","20260129","20251218","20251107","20250918","20250730"]
        today_str = date.today().strftime("%Y%m%d")
        for best_dt in sorted([d for d in known if d <= today_str], reverse=True):
            u = f"https://www.federalreserve.gov/newsevents/pressreleases/monetary{best_dt}a.htm"
            try:
                r = requests.get(u, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                if r.status_code == 200:
                    found_url = u
                    fomc_date = datetime.strptime(best_dt, "%Y%m%d").strftime("%Y-%m-%d")
                    break
            except Exception:
                pass

    if not found_url:
        return None, None

    try:
        r2 = requests.get(found_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        r2.raise_for_status()
        text = re.sub(r'<[^>]+>', ' ', r2.text)
        text = re.sub(r'\s+', ' ', text).strip()
        start = -1
        for marker in ["Recent indicators","The Federal Open Market Committee","Information received since"]:
            idx = text.find(marker)
            if idx != -1:
                start = idx
                break
        stmt_text = text[start:start+3000] if start != -1 else text[500:3500]
        return fomc_date, stmt_text
    except Exception as e:
        logger.warning(f"FOMC statement body: {e}")
        return None, None

def _fallback_regime(ff_current, zq_rate, cuts_implied):
    src = "DGS1数値ベース"
    if cuts_implied is None:
        return {"regime":"BALANCED","dominant_concern":"BALANCED","dominant_label":"両睨み","ai_reason":"データ取得失敗のためルールベース判定。","regime_source":src}
    if cuts_implied >= 1.0:
        return {"regime":"EASING","dominant_concern":"EMPLOYMENT_FOCUS","dominant_label":"雇用重視","ai_reason":f"DGS1ベースで{cuts_implied:.1f}回の利下げを織り込み。EASING局面と判定（AI分析なし）。","regime_source":src}
    elif cuts_implied <= -1.0:
        return {"regime":"TIGHTENING","dominant_concern":"INFLATION_FOCUS","dominant_label":"インフレ警戒","ai_reason":f"DGS1ベースで{abs(cuts_implied):.1f}回の利上げを織り込み。TIGHTENING局面と判定（AI分析なし）。","regime_source":src}
    else:
        return {"regime":"BALANCED","dominant_concern":"BALANCED","dominant_label":"両睨み","ai_reason":f"DGS1ベースの織り込みが{cuts_implied:+.1f}回でBALANCED局面と判定（AI分析なし）。","regime_source":src}

def analyze_fomc_with_grok(fomc_date, stmt_text, ff_current, zq_rate, cuts_implied):
    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        return _fallback_regime(ff_current, zq_rate, cuts_implied)
    prompt = f"""You are a Federal Reserve policy analyst. Analyze the following FOMC statement and market data.

FOMC Statement ({fomc_date}):
{stmt_text}

Market Context:
- Current FF Rate: {ff_current}%
- 12-month ahead FF futures implied rate: {zq_rate}% (ZQ=F front-month corrected; DGS1 adjusted for term premium)
- Market-implied rate changes in 12M: {cuts_implied:+.1f} cuts (25bp each)

Respond ONLY in this exact JSON format (no markdown, no extra text):
{{"regime":"EASING","dominant_concern":"EMPLOYMENT_FOCUS","dominant_label":"雇用重視","ai_reason":"日本語で100字以内で判断理由を記載。"}}"""
    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    # [[GROK-MODEL-PRICE-1]]対応: grok-3-mini/grok-3は実際にはgrok-4.3へ
    # 自動ルーティングされ、grok-2-1212は廃止済み（API側でModel not found）。
    # 実態に合わせ、実際に応答する現行モデル名を明示する（フォールバック
    # ループ構造自体は一時的なネットワーク障害時の再試行として維持）。
    models = ["grok-4.3", "grok-4.3", "grok-4.3"]
    text = None
    for model in models:
        try:
            logger.info(f"Grokモデル試行中 (FOMC): {model}")
            r = requests.post(url, headers=headers, json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1024,
                "temperature": 0.1,
            }, timeout=60)
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"]
            logger.info(f"Grokモデル成功 (FOMC): {model}")
            break
        except Exception as e:
            logger.warning(f"Grokモデル失敗 (FOMC) ({model}): {e}")
            text = None
    if not text:
        logger.error("すべてのGrokモデルで失敗しました (FOMC)")
        return _fallback_regime(ff_current, zq_rate, cuts_implied)
    raw = text.strip()
    m = re.search(r'\{.*\}', raw, re.DOTALL)
    if m:
        parsed = json.loads(m.group())
        _VALID_REGIMES = {"EASING", "BALANCED", "TIGHTENING"}
        if parsed.get("regime") not in _VALID_REGIMES:
            logger.warning(f"Grok returned invalid regime '{parsed.get('regime')}'. Using fallback.")
            return _fallback_regime(ff_current, zq_rate, cuts_implied)
        parsed["regime_source"] = "FOMC声明分析（Grok）"
        return parsed
    logger.warning(f"No JSON found in Grok response (FOMC). Full response: {raw[:500]}")
    return _fallback_regime(ff_current, zq_rate, cuts_implied)

def update_fed_context(target_date: date):
    logger.info("=== Updating Fed Context ===")
    if os.path.exists(FED_CONTEXT_PATH):
        ctx_df = pd.read_csv(FED_CONTEXT_PATH, dtype=str).fillna("")
        # 旧CSV に存在しない列を補完
        for c in FED_CONTEXT_COLUMNS:
            if c not in ctx_df.columns:
                ctx_df[c] = ""
        # regime_source の後付け推定（既存行）
        if "regime_source" in ctx_df.columns:
            mask = ctx_df["regime_source"] == ""
            ctx_df.loc[mask, "regime_source"] = ctx_df.loc[mask, "ai_reason"].apply(
                lambda r: "DGS1数値ベース" if "AI分析なし" in str(r) else (
                    "FOMC声明分析（Grok）" if r else "DGS1数値ベース"))
    else:
        ctx_df = pd.DataFrame(columns=FED_CONTEXT_COLUMNS)

    zq_ticker, zq_price, zq_rate = get_zq_futures(target_date)
    ff_current = get_ff_current()
    if ff_current is None:
        ff_current = 3.625  # 現行誘導目標 3.50-3.75% の中心値（FRED取得失敗時の fallback）

    cuts_implied = None
    if zq_rate is not None and ff_current is not None:
        cuts_implied = round((ff_current - zq_rate) / 0.25, 2)

    record_month = target_date.strftime("%Y-%m")
    already = (not ctx_df.empty and "record_date" in ctx_df.columns and
               ctx_df["record_date"].str.startswith(record_month).any())

    def _n(v):
        return "" if v is None else str(v)

    if already:
        last_idx = ctx_df[ctx_df["record_date"].str.startswith(record_month)].index[-1]
        ctx_df.loc[last_idx, "ff_current"] = str(ff_current)
        ctx_df.loc[last_idx, "updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if zq_rate is not None:
            ctx_df.loc[last_idx, "zq_ticker"]    = zq_ticker or ""
            ctx_df.loc[last_idx, "zq_price"]     = _n(zq_price)
            ctx_df.loc[last_idx, "zq_rate"]      = _n(zq_rate)
            ctx_df.loc[last_idx, "cuts_implied"] = _n(cuts_implied)
    else:
        fomc_date, stmt_text = fetch_latest_fomc_statement()
        if stmt_text:
            analysis = analyze_fomc_with_grok(
                fomc_date or target_date.strftime("%Y-%m-%d"),
                stmt_text, ff_current, zq_rate if zq_rate is not None else ff_current,
                cuts_implied if cuts_implied is not None else 0)
        else:
            analysis = _fallback_regime(ff_current, zq_rate, cuts_implied)
            fomc_date = target_date.strftime("%Y-%m-%d")

        new_row = {
            "record_date":      target_date.strftime("%Y-%m-%d"),
            "fomc_date":        fomc_date or "",
            "regime":           analysis.get("regime", "BALANCED"),
            "dominant_concern": analysis.get("dominant_concern", "BALANCED"),
            "dominant_label":   analysis.get("dominant_label", "両睨み"),
            "ff_current":       str(ff_current),
            "zq_ticker":        zq_ticker or "",
            "zq_price":         _n(zq_price),
            "zq_rate":          _n(zq_rate),
            "cuts_implied":     _n(cuts_implied),
            "ai_reason":        analysis.get("ai_reason", ""),
            "regime_source":    analysis.get("regime_source", "DGS1数値ベース"),
            "updated_at":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ctx_df = pd.concat([ctx_df, pd.DataFrame([new_row])], ignore_index=True)

    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    ctx_df.to_csv(FED_CONTEXT_PATH, index=False, encoding="utf-8")
    logger.info(f"Fed context saved: {FED_CONTEXT_PATH}")

# ─────────────────────────────────────────────────────────────────
#  週次AI解説（--weekly-analysis）
# ─────────────────────────────────────────────────────────────────
def load_weekly_analysis() -> pd.DataFrame:
    if not os.path.exists(WEEKLY_ANALYSIS_PATH):
        return pd.DataFrame(columns=WEEKLY_ANALYSIS_COLUMNS)
    try:
        df = pd.read_csv(WEEKLY_ANALYSIS_PATH, encoding="utf-8", dtype=str).fillna("")
        for c in WEEKLY_ANALYSIS_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        # 不正行フィルタ（analysis_dateが空またはYYYY-MM-DD形式でない行を除去）
        df = df[df["analysis_date"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)]
        return df.reset_index(drop=True)
    except Exception as e:
        logger.warning(f"weekly_analysis.csv read error: {e}")
        return pd.DataFrame(columns=WEEKLY_ANALYSIS_COLUMNS)

def save_weekly_analysis(df: pd.DataFrame):
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    df = df.drop_duplicates(subset=["analysis_date"], keep="last")
    df = df.sort_values("analysis_date").reset_index(drop=True)
    df.to_csv(WEEKLY_ANALYSIS_PATH, index=False, encoding="utf-8")
    logger.info(f"weekly_analysis.csv saved: {WEEKLY_ANALYSIS_PATH} ({len(df)} rows)")

_EPOCH = datetime(1970, 1, 1)

def _to_ms(dt: datetime) -> float:
    """datetime → ミリ秒（1970-01-01基準）。Windowsの timestamp() バグ回避。"""
    return (dt - _EPOCH).total_seconds() * 1000

def _compute_current_score(events: pd.DataFrame, target_date: date) -> dict:
    """events.csv から target_date 時点のスコアと各指標の値を計算する"""
    target_ms = _to_ms(datetime.combine(target_date, datetime.max.time()))

    # indicator -> [(date, actual)] sorted by date
    ind_data = {}
    for _, r in events.iterrows():
        ind = r.get("indicator", "")
        actual_str = str(r.get("actual", "")).strip()
        date_str = str(r.get("release_date", "")).strip()
        if not ind or not actual_str or not date_str:
            continue
        try:
            val = float(actual_str)
            d = datetime.strptime(date_str, "%Y-%m-%d")
            d_ms = _to_ms(d)
            if d_ms > target_ms:
                continue
            if ind not in ind_data:
                ind_data[ind] = []
            ind_data[ind].append((d_ms, val, date_str))
        except (ValueError, TypeError):
            continue

    for ind in ind_data:
        ind_data[ind].sort(key=lambda x: x[0])

    def latest(ind):
        arr = ind_data.get(ind, [])
        return (arr[-1][1], arr[-1][2]) if arr else (None, None)

    def trend3(ind):
        arr = ind_data.get(ind, [])
        if len(arr) < 2:
            return 0, []
        last3 = arr[-3:]
        changes = []
        for i in range(1, len(last3)):
            changes.append(last3[i][1] - last3[i-1][1])
        avg = sum(changes) / len(changes)
        return (1 if avg > 0 else -1 if avg < 0 else 0), changes

    indicators = {}
    score_inputs = []

    # 各指標の最新値を取得
    indicator_keys = [
        ('Yield Curve 10Y-2Y', 'yc'),
        ('HY Spread', 'hy'),
        ('Philadelphia Fed Manufacturing', 'philly'),
        ('Chicago Fed National Activity', 'cfnai'),
        ('Initial Claims 4W MA', 'claims'),
        ('Building Permits', 'cbcc2'),
        ('Michigan Consumer Sentiment', 'cbcc'),
        ('Sahm Rule Recession Indicator', 'sahm'),
    ]

    weights = {'yc':20, 'hy':15, 'philly':18, 'cfnai':12, 'claims':10, 'cbcc2':10, 'cbcc':8, 'sahm':7}

    for ind_name, key in indicator_keys:
        val, val_date = latest(ind_name)
        trend_dir, _ = trend3(ind_name)
        indicators[key] = {'name': ind_name, 'value': val, 'date': val_date, 'trend': trend_dir}

        if val is None:
            continue

        # スコア計算（renderPhaseGaugeと同一ロジック。MACRO-BUG-1: philly/claimsの
        # トレンド補正±10ptが欠落していたため追加し、JSと完全一致させた）
        if key == 'yc':
            s = 90 if val < -0.5 else 70 if val < 0 else 40 if val < 0.5 else 15
        elif key == 'hy':
            s = 90 if val > 6 else 70 if val > 4.5 else 40 if val > 3.5 else 15
        elif key == 'cbcc2':
            s = 15 if val >= 1500 else 35 if val >= 1300 else 60 if val >= 1100 else 85
        elif key == 'philly':
            s = 88 if val < -10 else (65 + (10 if trend_dir < 0 else 0)) if val < 0 else 35 if val < 5 else 12
        elif key == 'cfnai':
            s = 82 if val < -0.7 else 50 if val < -0.35 else 18
        elif key == 'claims':
            s = 85 if val > 300000 else (60 + (10 if trend_dir > 0 else 0)) if val > 250000 else 35 if val > 215000 else 15
        elif key == 'cbcc':
            s = 82 if val < 60 else 72 if val < 75 else 60 if val < 90 else 30
        elif key == 'sahm':
            s = 88 if val >= 0.5 else 50 if val >= 0.3 else 12
        else:
            s = 50

        score_inputs.append({'key': key, 'score': s, 'weight': weights.get(key, 0)})

    total_w = sum(si['weight'] for si in score_inputs)
    raw_score = sum(si['score'] * si['weight'] for si in score_inputs) / total_w if total_w > 0 else 50
    score = round(raw_score)

    if score < 30:
        phase = '拡張'
    elif score < 52:
        phase = '踊り場'
    elif score < 70:
        phase = '後退入口'
    else:
        phase = '後退'

    return {
        'score': score,
        'phase': phase,
        'indicators': indicators,
        'score_inputs': score_inputs,
    }

def _compute_score_change(events: pd.DataFrame, target_date: date, days_back: int) -> int:
    """N日前のスコアとの差分を計算"""
    past_date = target_date - timedelta(days=days_back)
    current = _compute_current_score(events, target_date)
    past = _compute_current_score(events, past_date)
    if current['score'] is None or past['score'] is None:
        return 0
    return current['score'] - past['score']

def _get_recent_events_summary(events: pd.DataFrame, target_date: date, days: int = 7) -> list:
    """直近N日間の主要指標発表をサマリとして取得"""
    DAILY_INDS = {'Yield Curve 10Y-2Y', 'HY Spread', 'VIX', 'Michigan Inflation 5Y'}
    cutoff = (target_date - timedelta(days=days)).strftime("%Y-%m-%d")
    target_str = target_date.strftime("%Y-%m-%d")

    recent = []
    for _, r in events.iterrows():
        ind = str(r.get("indicator", "")).strip()
        rd = str(r.get("release_date", "")).strip()
        if ind in DAILY_INDS or not rd:
            continue
        if cutoff <= rd <= target_str:
            actual = r.get("actual", "")
            surprise = r.get("surprise", "")
            recent.append({
                'indicator': ind,
                'date': rd,
                'actual': actual,
                'surprise': surprise,
            })
    return sorted(recent, key=lambda x: x['date'], reverse=True)

def generate_weekly_analysis_with_grok(target_date: date, score_data: dict,
                                        recent_events: list,
                                        score_1w: int, score_1m: int,
                                        fed_context: dict,
                                        indicator_deltas: dict = None) -> dict:
    """xAI Grok APIで週次AI解説を生成"""
    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        logger.warning("XAI_API_KEY not set. Generating fallback analysis.")
        return _fallback_weekly_analysis(target_date, score_data, score_1w, score_1m)

    # 指標サマリを構築（差分情報を含む）
    # 逆方向指標（値が上がると景気悪化）
    INVERSE_INDICATORS = {'HY Spread', 'Initial Claims 4W MA', 'Sahm Rule Recession Indicator'}

    ind_lines = []
    for key, info in score_data['indicators'].items():
        val = info['value']
        if val is None:
            continue
        trend_str = '↑上昇' if info['trend'] > 0 else '↓下降' if info['trend'] < 0 else '→横ばい'
        is_inverse = info['name'] in INVERSE_INDICATORS
        direction_note = '（※この指標は上昇=悪化、下降=改善）' if is_inverse else ''

        delta_info = ""
        if indicator_deltas and info['name'] in indicator_deltas:
            d = indicator_deltas[info['name']]
            # Initial Claimsは件数なので整数フォーマット
            if info['name'] == 'Initial Claims 4W MA':
                w = f"{d['delta_1w']:+,.0f}件" if d['delta_1w'] is not None else "N/A"
                m = f"{d['delta_1m']:+,.0f}件" if d['delta_1m'] is not None else "N/A"
            else:
                w = f"{d['delta_1w']:+.2f}" if d['delta_1w'] is not None else "N/A"
                m = f"{d['delta_1m']:+.2f}" if d['delta_1m'] is not None else "N/A"
            delta_info = f", 週差: {w}, 月差: {m}"
        ind_lines.append(f"  - {info['name']}: {val} (トレンド: {trend_str}{delta_info}){direction_note}")

    # 直近発表イベント
    event_lines = []
    for ev in recent_events[:10]:
        surp = ev['surprise']
        surp_str = f", サプライズ: {surp}" if surp else ""
        event_lines.append(f"  - {ev['date']} {ev['indicator']}: {ev['actual']}{surp_str}")

    # FED context
    regime = fed_context.get('regime', 'BALANCED')
    ff_rate = fed_context.get('ff_current', '—')
    cuts = fed_context.get('cuts_implied', '—')

    prompt = f"""あなたは米国マクロ経済の専門アナリストです。以下のデータに基づいて、個人投資家向けの週次景気解説を日本語で作成してください。

【重要】スコアの解釈ルール:
- このスコアは「景気後退リスク」を測るもので、0=景気好調（後退リスクなし）、100=景気後退の可能性が高い、です。
- スコアが低いほど景気は良好です。スコア24は「後退リスクが低い=景気が良い」という意味です。
- 「スコアが低い」ことをネガティブに表現しないでください。「リスクが低い」「良好」と表現してください。
- フェーズ: 0-30=拡張（好調）、30-52=踊り場、52-70=後退入口、70-100=後退

【重要】指標の方向性ルール:
- 多くの指標は「上昇=景気改善」だが、以下の3指標は逆方向（上昇=景気悪化）:
  - HY Spread: 上昇=信用リスク拡大=悪化、下降=改善
  - Initial Claims 4W MA: 上昇=失業増加=悪化、下降=改善
  - Sahm Rule Recession Indicator: 上昇=後退リスク上昇=悪化、下降=改善
- 差分を言及する際は、この方向性を踏まえて「改善」「悪化」を正しく使い分けてください。

■ 現在の景気後退リスクスコア: {score_data['score']}/100 (フェーズ: {score_data['phase']})
■ 先週比: {score_1w:+d}pt, 前月比: {score_1m:+d}pt （マイナス=改善、プラス=悪化）
■ FED政策局面: {regime}, FF金利: {ff_rate}%, 利下げ織り込み: {cuts}回

■ 8指標の最新値（週差=1週間前との差分、月差=1ヶ月前との差分）:
{chr(10).join(ind_lines)}

■ 直近1週間の主要発表:
{chr(10).join(event_lines) if event_lines else '  なし'}

以下のJSON形式で回答してください（マークダウンなし、バッククォートなし）:
{{"summary":"全体の景気判断を3〜4文で簡潔に（150字以内）","factor_analysis":"スコア変動の要因分析を3〜5文で。各指標の差分データを根拠として言及すること（200字以内）","watchpoints":"今後1〜2週間で注視すべきポイントを2〜3個（200字以内）","indicator_comments":"Yield Curve 10Y-2Y:コメント;HY Spread:コメント;Philadelphia Fed Manufacturing:コメント;Chicago Fed National Activity:コメント;Initial Claims 4W MA:コメント;Building Permits:コメント;Michigan Consumer Sentiment:コメント;Sahm Rule Recession Indicator:コメント の形式で必ず半角セミコロン(;)で8個に区切る。各コメントは20字以内。句読点に全角セミコロンや。を使わないこと"}}"""

    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    # [[GROK-MODEL-PRICE-1]]対応: grok-3-mini/grok-3は実際にはgrok-4.3へ
    # 自動ルーティングされ、grok-2-1212は廃止済み（API側でModel not found）。
    # 実態に合わせ、実際に応答する現行モデル名を明示する（フォールバック
    # ループ構造自体は一時的なネットワーク障害時の再試行として維持）。
    models = ["grok-4.3", "grok-4.3", "grok-4.3"]
    text = None
    used_model = None
    prompt_len = len(prompt)
    logger.info(f"Grok weekly analysis: prompt length={prompt_len} chars")
    for model in models:
        try:
            logger.info(f"Grokモデル試行中: {model}")
            r = requests.post(url, headers=headers, json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4096,
                "temperature": 0.3,
            }, timeout=60)
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"]
            used_model = model
            logger.info(f"Grokモデル成功: {model}")
            break
        except Exception as e:
            logger.warning(f"Grokモデル失敗 ({model}): {e}")
            text = None

    if not text:
        logger.error("すべてのGrokモデルで失敗しました")
        return _fallback_weekly_analysis(target_date, score_data, score_1w, score_1m)

    raw = text.strip()
    logger.info(f"Grok raw response length={len(raw)} chars, first 300: {raw[:300]}")
    m = re.search(r'\{.*\}', raw, re.DOTALL)
    if m:
        result = json.loads(m.group())
        result["_used_model"] = used_model
        logger.info(f"Weekly analysis generated via Grok ({used_model}).")
        return result
    else:
        logger.warning(f"No JSON found in Grok response. Full response: {raw[:500]}")

    return _fallback_weekly_analysis(target_date, score_data, score_1w, score_1m)

def _fallback_weekly_analysis(target_date, score_data, score_1w, score_1m):
    """Grok失敗時のフォールバック"""
    score = score_data['score']
    phase = score_data['phase']
    direction = "改善" if score_1w < 0 else "悪化" if score_1w > 0 else "横ばい"

    return {
        "summary": f"景気後退リスクスコアは{score}/100（{phase}局面）。先週比{score_1w:+d}ptで{direction}傾向。AI解説は生成できませんでした（ルールベース判定）。",
        "factor_analysis": f"先週比{score_1w:+d}pt、前月比{score_1m:+d}ptの変動。詳細な要因分析にはGemini API接続が必要です。",
        "watchpoints": "Gemini API未接続のため注視ポイントは自動生成されていません。各指標の個別トレンドを確認してください。",
        "indicator_comments": "",
    }

def run_weekly_analysis(target_date: date):
    """週次AI解説のメイン処理"""
    logger.info(f"=== Weekly Analysis | {target_date} ===")

    events = load_events()
    if events.empty:
        logger.warning("No events data. Skipping weekly analysis.")
        return

    # 現在のスコアと指標状態を計算
    score_data = _compute_current_score(events, target_date)
    score_1w = _compute_score_change(events, target_date, 7)
    score_1m = _compute_score_change(events, target_date, 30)

    # 各指標の1週前・1ヶ月前との差分を計算
    score_data_1w = _compute_current_score(events, target_date - timedelta(days=7))
    score_data_1m = _compute_current_score(events, target_date - timedelta(days=30))
    indicator_deltas = {}
    for key, info in score_data['indicators'].items():
        val = info['value']
        if val is None:
            continue
        val_1w = score_data_1w['indicators'].get(key, {}).get('value')
        val_1m = score_data_1m['indicators'].get(key, {}).get('value')
        delta_1w = round(val - val_1w, 4) if val_1w is not None else None
        delta_1m = round(val - val_1m, 4) if val_1m is not None else None
        indicator_deltas[info['name']] = {
            'value': val,
            'delta_1w': delta_1w,
            'delta_1m': delta_1m,
        }

    # 直近1週間の発表イベント
    recent_events = _get_recent_events_summary(events, target_date, days=7)

    # FED context
    fed_context = {}
    if os.path.exists(FED_CONTEXT_PATH):
        try:
            fc = pd.read_csv(FED_CONTEXT_PATH, dtype=str).fillna("")
            if not fc.empty:
                fed_context = fc.iloc[-1].to_dict()
        except Exception:
            pass

    # Grok で解説生成
    analysis = generate_weekly_analysis_with_grok(
        target_date, score_data, recent_events, score_1w, score_1m, fed_context,
        indicator_deltas
    )

    # CSV に保存
    wa_df = load_weekly_analysis()
    # テキストフィールド内の改行・カンマ・ダブルクォートを安全な文字に置換
    # （フロントの簡易CSVパーサーはRFC 4180非準拠のため必要）
    def _sanitize(s):
        if not s:
            return s
        return (str(s)
                .replace('\n', ' ')
                .replace('\r', ' ')
                .replace('"', "'")
                .replace(',', '、')  # CSVカラム区切りとの誤認を防止
               )
    # indicator_deltas をセミコロン区切り文字列に変換
    # 形式: "指標名:値:週差:月差;..."
    deltas_str_parts = []
    for ind_name, d in indicator_deltas.items():
        w = f"{d['delta_1w']:+.2f}" if d['delta_1w'] is not None else "N/A"
        m = f"{d['delta_1m']:+.2f}" if d['delta_1m'] is not None else "N/A"
        deltas_str_parts.append(f"{ind_name}:{d['value']}:{w}:{m}")
    deltas_str = ";".join(deltas_str_parts)

    # DESIGN-13: マクロサプライズ検知
    _surprises = detect_macro_surprises(events, lookback_days=60)
    _surprises_str = ";".join(_surprises) if _surprises else ""

    new_row = {
        "analysis_date": target_date.strftime("%Y-%m-%d"),
        "score":         str(score_data['score']),
        "phase":         score_data['phase'],
        "summary":       _sanitize(analysis.get("summary", "")),
        "factor_analysis": _sanitize(analysis.get("factor_analysis", "")),
        "watchpoints":   _sanitize(analysis.get("watchpoints", "")),
        "indicator_comments": _sanitize(analysis.get("indicator_comments", "")),
        "indicator_deltas": _sanitize(deltas_str),
        "score_change_1w": str(score_1w),
        "score_change_1m": str(score_1m),
        "surprise_alerts": _sanitize(_surprises_str),
        "model":         analysis.get("_used_model", "grok-unknown"),
        "updated_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if _surprises:
        logger.info(f"[surprise] {len(_surprises)} alerts detected: {_surprises}")

    # 同日の既存行を上書き
    wa_df = wa_df[wa_df["analysis_date"] != target_date.strftime("%Y-%m-%d")]
    wa_df = pd.concat([wa_df, pd.DataFrame([new_row])], ignore_index=True)
    save_weekly_analysis(wa_df)

    # Discord通知
    _surprise_discord = ("\n\n⚡ **サプライズ検知:**\n" + "\n".join(f"• {a}" for a in _surprises)) if _surprises else ""
    discord_msg = (
        f"📊 **MACRO PULSE — 週次AI解説** ({target_date.strftime('%Y-%m-%d')})\n\n"
        f"**スコア: {score_data['score']}/100 ({score_data['phase']})** "
        f"(先週比{score_1w:+d} / 前月比{score_1m:+d}){_surprise_discord}\n\n"
        f"**総括:** {analysis.get('summary', '—')}\n\n"
        f"**要因分析:** {analysis.get('factor_analysis', '—')}\n\n"
        f"**注視ポイント:** {analysis.get('watchpoints', '—')}"
    )
    send_discord(discord_msg)
    logger.info("=== Weekly Analysis complete ===")

# ─────────────────────────────────────────────────────────────────
#  月次FRED指標の自動補完
#  スケジュール未登録 (Philly Fed / CFNAI / Sahm Rule 等) および
#  スケジュール済みだが FRED が空を返したイベントを毎日再試行する。
# ─────────────────────────────────────────────────────────────────
_MONTHLY_REFRESH_SET = {
    "Philadelphia Fed Manufacturing",
    "Chicago Fed National Activity",
    "NFP",
    "Initial Claims 4W MA",
    "Building Permits",
    "Sahm Rule Recession Indicator",
    "Michigan Consumer Sentiment",
}

def refresh_monthly_indicators(target_date: date, fin_ctx: dict,
                                schedule: pd.DataFrame, events: pd.DataFrame,
                                sp500_t0) -> list:
    """
    毎日の normal run で呼び出し、月次 FRED 指標を補完する。
    - FREDの最新観測日が events.csv に未登録または actual が空 → 新規追加
    - スケジュールに依存しないため Philly Fed / CFNAI / Sahm Rule も自動取得できる
    - FREDのobs_dateが月初1日等になる場合、scheduleの実発表日で上書きする
    """
    new_rows = []
    for ind_name in _MONTHLY_REFRESH_SET:
        cfg = INDICATOR_CONFIG.get(ind_name, {})
        fred_id = cfg.get("fred_id", "")
        if not fred_id:
            continue

        val, obs_date = fred_latest(fred_id)
        if val is None or obs_date is None:
            continue

        # FREDのobs_dateが月初1日等になる場合、scheduleの実発表日で上書き。
        # obs_to_release_lag を使って「この obs_date が実際に発表される release_date」に
        # 絞り込んだウィンドウでスケジュールを検索する。ウィンドウが広すぎると
        # 既存スロット（前回分）を飛ばして未来スロットに誤マッピングされるため、
        # lag ± 14 日の狭い範囲だけを見る。
        release_date = obs_date
        cfg_ind = INDICATOR_CONFIG.get(ind_name, {})
        lag = cfg_ind.get("obs_to_release_lag", 35)
        win_start = (obs_date + timedelta(days=max(1, lag - 14))).strftime("%Y-%m-%d")
        win_end   = (obs_date + timedelta(days=lag + 14)).strftime("%Y-%m-%d")
        sched_hits = schedule[
            (schedule["indicator"] == ind_name) &
            (schedule["release_date"] >= win_start) &
            (schedule["release_date"] <= win_end)
        ].sort_values("release_date")

        if not sched_hits.empty:
            found_slot = False
            for _, srow in sched_hits.iterrows():
                try:
                    candidate_date = datetime.strptime(srow["release_date"], "%Y-%m-%d").date()
                except Exception:
                    continue
                candidate_id = make_event_id(ind_name, candidate_date)
                existing_c = events[events["event_id"] == candidate_id]
                if not existing_c.empty:
                    actual_str = str(existing_c.iloc[0].get("actual", "")).strip()
                    if actual_str not in ("", "nan"):
                        continue  # このスロットは既にデータあり → 次を試す
                release_date = candidate_date
                found_slot = True
                logger.info(f"[monthly-refresh] {ind_name}: obs={obs_date} → sched_release={release_date}")
                break
            if not found_slot:
                continue  # ウィンドウ内の全スロットに正常データあり → スキップ
        else:
            # スケジュールなし（またはウィンドウ外） → obs_date ベースの event_id を確認
            event_id = make_event_id(ind_name, release_date)
            existing = events[events["event_id"] == event_id]
            if not existing.empty:
                actual_str = str(existing.iloc[0].get("actual", "")).strip()
                if actual_str not in ("", "nan"):
                    continue  # 既に正常データあり → スキップ

        try:
            row = fetch_event_row(ind_name, target_date, fin_ctx, schedule, events)
            row["release_date"] = release_date.strftime("%Y-%m-%d")
            row["event_id"]     = make_event_id(ind_name, release_date)
            row["sp500_t0"] = str(sp500_t0) if sp500_t0 else ""
            new_rows.append(row)
            logger.info(f"[monthly-refresh] {ind_name}: obs={obs_date} val={val}")
            time.sleep(0.3)
        except Exception as e:
            logger.warning(f"[monthly-refresh] {ind_name}: {e}")

    if new_rows:
        logger.info(f"[monthly-refresh] {len(new_rows)} indicators refreshed.")
    return new_rows

# ─────────────────────────────────────────────────────────────────
#  MACRO-NFP-1: 同一FRED観測値の重複書き込み防止（防御的ガード）
# ─────────────────────────────────────────────────────────────────
def dedupe_new_rows(new_rows: list, events: pd.DataFrame) -> list:
    """
    run()内で今回新規に追加しようとしている行(new_rows)のうち、
    「同一indicator×同一actual値×release_dateが指標のobs_to_release_lag以内」
    の行を重複とみなして後発分を除外する。

    scheduledループとrefresh_monthly_indicators()が同一実行内で
    同一FRED観測値を別々のevent_idスロットに書き込んでしまうケース
    （events_snapshot反映後も理論上残りうる取りこぼし）への
    最終防御ライン。既存events（保存済み）とも比較し、実行をまたいだ
    重複も検出する。
    """
    if not new_rows:
        return new_rows

    seen: list[tuple[str, float, date]] = []
    if events is not None and not events.empty:
        for _, r in events.iterrows():
            try:
                d = datetime.strptime(str(r["release_date"]), "%Y-%m-%d").date()
                v = float(r["actual"])
            except (ValueError, TypeError, KeyError):
                continue
            seen.append((r["indicator"], v, d))

    kept = []
    for row in new_rows:
        ind = row.get("indicator", "")
        try:
            av = float(row.get("actual", ""))
            rd = datetime.strptime(str(row["release_date"]), "%Y-%m-%d").date()
        except (ValueError, TypeError, KeyError):
            kept.append(row)
            continue

        # refresh_monthly_indicators()のスケジュール窓判定は obs_date±(lag±14日) で
        # スロットを探すため、同一観測値の重複候補はraw obs_date基準の行（差0日）から
        # 窓上限（lag+14日）まで離れうる。窓の下限（lag-14日）ではなく上限側で
        # 比較しないと、このケース（MACRO-NFP-1で実際に発生）を取りこぼす。
        lag = INDICATOR_CONFIG.get(ind, {}).get("obs_to_release_lag", 35) + 14
        is_dup = any(
            s_ind == ind and abs(s_av - av) < 1e-6 and abs((s_rd - rd).days) <= lag
            for s_ind, s_av, s_rd in seen
        )
        if is_dup:
            logger.warning(
                f"[dedupe] {ind} {row.get('event_id')}: 同一観測値(actual={av})の"
                f"重複行を除外しました"
            )
            continue

        kept.append(row)
        seen.append((ind, av, rd))

    return kept

# ─────────────────────────────────────────────────────────────────
#  流動性モニター CSV 更新
#  【単位統一】全カラムを Millions USD で保存
#    WALCL (FED)  : FRED→Millions USD   そのまま
#    WTREGEN(TGA) : FRED→Millions USD   そのまま
#    RRPONTSYD    : FRED→Billions USD   × 1000 → Millions USD
#    WRBWFRBL     : FRED→Billions USD   × 1000 → Millions USD
#    M2SL         : FRED→Billions USD   そのまま（別単位・表示専用）
#  NET LIQUIDITY(兆USD) = (WALCL - WTREGEN - RRPONTSYD_M) / 1,000,000
# ─────────────────────────────────────────────────────────────────
LIQUIDITY_COLUMNS = [
    "date", "m2", "hy_spread", "fed_balance", "tga", "rrp", "net_liquidity",
    "reserve_balance", "stealth_signal",
    # DESIGN-12: 3層構造
    "stealth_absorb_weeks",   # 連続ステルス吸収週数
    "net_liq_decline_weeks",  # NET流動性連続減少週数
    "stealth_alert",          # 警戒アラート（|区切り）
    "sp500",                  # [[HOLLOW-RALLY-DEAD-1]]: S&P500終値レベル（日次、FRED "SP500"系列）
]

def update_liquidity_csv(target_date: date, sp500_val: float | None = None) -> None:
    """流動性指標を FRED から取得して 05_liquidity.csv に追記・更新する。
    RRP / reserve_balance は Billions → Millions に変換して保存する。

    sp500_val: [[HOLLOW-RALLY-DEAD-1]]対応。呼び出し元run()が既に
    get_sp500(target_date)で取得済みの値を渡す（本関数内で新規に
    FRED取得を発生させない）。未取得（値なし）の日は空欄のまま保存する
    （carry-forwardしない。5営業日リターン計算に古い価格が紛れ込む
    ことを避けるため、他の週次系列とは異なり意図的に非補完とする）。
    """
    # M2マネーサプライ: 月次, Billions USD
    m2_val,  _ = fred_latest("M2SL")
    # FRBバランスシート (WALCL): 週次, Millions USD
    fed_val, _ = fred_latest("WALCL")
    # HYスプレッド (BAMLH0A0HYM2): 日次, %
    hy_val,  _ = fred_latest("BAMLH0A0HYM2")
    # TGA (WTREGEN): 週次, Millions USD — 代替: FTSD
    # （FTSDはFRED上に系列が実在しないため常に取得失敗する既知の問題、
    # [[MACRODATA-FTSD-SERIES-ID-INVALID-1]]参照。旧実装でも同様に
    # 機能していなかったフォールバックのため、今回の切替による回帰では
    # ない。フォールバック構造自体は変更せず維持する）
    tga_val, _ = fred_latest("WTREGEN")
    if tga_val is None:
        tga_val, _ = fred_latest("FTSD")
    # RRP (RRPONTSYD): 日次, Billions USD → × 1000 で Millions に統一
    rrp_b,   _ = fred_latest("RRPONTSYD")
    rrp_val    = round(rrp_b * 1000, 4) if rrp_b is not None else None   # Millions USD
    # 準備預金残高 (WRBWFRBL): 週次, Millions USD（FREDはMillions USD単位で返す）
    rsv_m,   _ = fred_latest("WRBWFRBL")
    rsv_val    = round(rsv_m, 4) if rsv_m is not None else None   # Millions USD（×1000 不要）

    if all(v is None for v in (m2_val, hy_val, fed_val)):
        logger.warning("[Liquidity] All series returned None. Skipping.")
        return

    # NET LIQUIDITY (兆USD) = (WALCL_M - WTREGEN_M - RRPONTSYD_M) / 1,000,000
    # rrp_val はすでに Millions に変換済み
    net_liq = None
    if fed_val is not None and tga_val is not None and rrp_val is not None:
        net_liq = round((fed_val - tga_val - rrp_val) / 1_000_000, 4)
    if net_liq is None:
        logger.warning(
            f"[Liquidity] net_liquidity skipped: "
            f"fed={fed_val} tga={tga_val} rrp_b={rrp_b}"
        )

    if os.path.exists(LIQUIDITY_PATH):
        try:
            df = pd.read_csv(LIQUIDITY_PATH, dtype=str).fillna("")
            for col in LIQUIDITY_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
        except Exception as e:
            logger.warning(f"[Liquidity] CSV read error: {e}")
            df = pd.DataFrame(columns=LIQUIDITY_COLUMNS)
    else:
        df = pd.DataFrame(columns=LIQUIDITY_COLUMNS)

    date_str = target_date.strftime("%Y-%m-%d")

    # ── Carry-forward: 週次系列が今日 FRED から取得できない場合に直前値を補完 ──
    # 理由: WALCL/WTREGEN は木曜公表・RRPONTSYD は日次のため、非公表日は None になる。
    # fed/tga/rrp のどれか 1 つでも None だと net_liquidity が空欄になるため、
    # CSV に保存済みの直前値を使って計算できるようにする。
    _prev_df = df[df["date"] < date_str].sort_values("date")
    if not _prev_df.empty:
        def _carryforward(current_val, col_name, label):
            if current_val is not None:
                return current_val
            prev_rows = _prev_df[_prev_df[col_name] != ""]
            if not prev_rows.empty:
                carried = float(prev_rows.iloc[-1][col_name])
                logger.info(
                    f"[Liquidity] {label} carry-forward from "
                    f"{prev_rows.iloc[-1]['date']}: {carried}"
                )
                return carried
            return None
        fed_val = _carryforward(fed_val, "fed_balance",     "fed_balance")
        tga_val = _carryforward(tga_val, "tga",             "tga")
        rrp_val = _carryforward(rrp_val, "rrp",             "rrp")
        rsv_val = _carryforward(rsv_val, "reserve_balance", "reserve_balance")
        hy_val  = _carryforward(hy_val,  "hy_spread",       "hy_spread")

    # carry-forward 後に net_liq を再計算（初回計算で None だった場合に対応）
    if net_liq is None and fed_val is not None and tga_val is not None and rrp_val is not None:
        net_liq = round((fed_val - tga_val - rrp_val) / 1_000_000, 4)
        logger.info(f"[Liquidity] net_liquidity recomputed after carry-forward: {net_liq}")

    # ── ステルス流動性シグナル計算 ──
    # RRP減少 OR TGA減少 → supply（ステルス供給）
    # RRP増加 AND TGA増加 → absorb（ステルス吸収）
    # reserve_balance 増加 かつ rrp/tga が neutral → supply（補助判定）
    stealth_sig = "neutral"
    prev_rows = df[df["date"] < date_str].sort_values("date")
    # [[LIQUIDITY-CSV-FIRST-ROW-UNBOUNDLOCALERROR-1]]: prev_rows空時（05_liquidity.csv
    # が存在しない・完全に空の状態からの初回実行）でも後続の「ステルス吸収額 vs FED供給額の
    # 比較」ブロックが分岐外からprev_rrp/prev_tga/prev_rsvを無条件参照するため、
    # if文の外側で先にNone初期化しておく（_fed_prevと同型のガード）。
    prev_rrp = prev_tga = prev_rsv = None
    if not prev_rows.empty:
        prev = prev_rows.iloc[-1]
        prev_rrp = float(prev["rrp"]) if prev.get("rrp", "") != "" else None
        prev_tga = float(prev["tga"]) if prev.get("tga", "") != "" else None
        prev_rsv = float(prev["reserve_balance"]) if prev.get("reserve_balance", "") != "" else None
        rrp_dec = rrp_val is not None and prev_rrp is not None and rrp_val < prev_rrp
        tga_dec = tga_val is not None and prev_tga is not None and tga_val < prev_tga
        rrp_inc = rrp_val is not None and prev_rrp is not None and rrp_val > prev_rrp
        tga_inc = tga_val is not None and prev_tga is not None and tga_val > prev_tga
        rsv_inc = rsv_val is not None and prev_rsv is not None and rsv_val > prev_rsv
        if rrp_dec or tga_dec:
            stealth_sig = "supply"
        elif rrp_inc and tga_inc:
            stealth_sig = "absorb"
        elif rsv_inc:
            # 準備預金増加（rrp/tga は中立）→ 銀行流動性上昇＝補助的供給シグナル
            stealth_sig = "supply"

    # ── DESIGN-12: Layer 3 — 連続週数 & 警戒アラート計算 ──

    # 連続ステルス吸収週数（現在週を含む）
    _sig_history = list(df[df["date"] < date_str].sort_values("date").tail(7)["stealth_signal"])
    _sig_history.append(stealth_sig)
    _absorb_weeks = 0
    for _s in reversed(_sig_history):
        if _s == "absorb":
            _absorb_weeks += 1
        else:
            break

    # NET流動性の連続減少週数
    _nl_rows = df[(df["date"] < date_str) & (df["net_liquidity"] != "")].sort_values("date").tail(5)
    _nl_vals: list[float] = [float(r) for r in _nl_rows["net_liquidity"] if r != ""]
    if net_liq is not None:
        _nl_vals.append(net_liq)
    _decline_weeks = 0
    for _i in range(len(_nl_vals) - 1, 0, -1):
        if _nl_vals[_i] < _nl_vals[_i - 1]:
            _decline_weeks += 1
        else:
            break

    # ステルス吸収額 vs FED供給額の比較
    _fed_prev = float(prev_rows.iloc[-1].get("fed_balance", 0) or 0) if not prev_rows.empty else None
    _stealth_absorb_vol  = None  # rrp増加 + tga増加 (Millions USD)
    _fed_supply_vol      = None  # fed_balance増加 (Millions USD)
    if (prev_rrp is not None and rrp_val is not None and
            prev_tga is not None and tga_val is not None and _fed_prev is not None and fed_val is not None):
        _stealth_absorb_vol = max(0, (rrp_val - prev_rrp) + (tga_val - prev_tga))
        _fed_supply_vol     = max(0, fed_val - _fed_prev)

    # 警戒アラート文字列（|区切り）
    _alerts: list[str] = []
    if _absorb_weeks >= 4:
        _alerts.append(f"政策EASINGの効果が限定的（ステルス吸収{_absorb_weeks}週継続）")
    if _decline_weeks >= 3:
        _alerts.append(f"実質的にTIGHTENINGに近い状態（NET流動性{_decline_weeks}週連続減少）")
    if (_stealth_absorb_vol is not None and _fed_supply_vol is not None
            and _stealth_absorb_vol > 0 and _stealth_absorb_vol > _fed_supply_vol):
        _alerts.append("EASING認識の見直しを推奨（ステルス吸収額が政策供給額を超過）")

    new_row = {
        "date":             date_str,
        "m2":               str(round(m2_val,  4)) if m2_val  is not None else "",
        "hy_spread":        str(round(hy_val,  4)) if hy_val  is not None else "",
        "fed_balance":      str(round(fed_val, 4)) if fed_val is not None else "",
        "tga":              str(round(tga_val, 4)) if tga_val is not None else "",
        "rrp":              str(rrp_val)            if rrp_val is not None else "",
        "net_liquidity":    str(net_liq)            if net_liq is not None else "",
        "reserve_balance":  str(rsv_val)            if rsv_val is not None else "",
        "stealth_signal":   stealth_sig,
        "stealth_absorb_weeks":  str(_absorb_weeks),
        "net_liq_decline_weeks": str(_decline_weeks),
        "stealth_alert":         "|".join(_alerts),
        "sp500":                 str(sp500_val) if sp500_val is not None else "",
    }
    if _alerts:
        logger.info(f"[Stealth L3] alerts={_alerts}")

    update_cols = ["m2", "hy_spread", "fed_balance", "tga", "rrp", "net_liquidity",
                   "reserve_balance", "stealth_signal",
                   "stealth_absorb_weeks", "net_liq_decline_weeks", "stealth_alert",
                   "sp500"]
    if date_str in df["date"].values:
        idx = df.index[df["date"] == date_str][0]
        for col in update_cols:
            if new_row[col]:
                df.at[idx, col] = new_row[col]
    else:
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    df = df.sort_values("date").reset_index(drop=True)
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    df.to_csv(LIQUIDITY_PATH, index=False, encoding="utf-8")

    JST = timezone(timedelta(hours=9))
    meta = {"generated_at": datetime.now(JST).strftime("%Y-%m-%dT%H:%M:%S+09:00")}
    meta_path = os.path.join(BASE_DATA_DIR, "05_meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f)

    logger.info(
        f"[Liquidity] Saved {date_str}: "
        f"m2={new_row['m2']} fed={new_row['fed_balance']} "
        f"tga={new_row['tga']} rrp_M={new_row['rrp']} net_liq={new_row['net_liquidity']} "
        f"rsv_M={new_row['reserve_balance']} stealth={stealth_sig} sp500={new_row['sp500']}"
    )

# ─────────────────────────────────────────────────────────────────
#  メインオーケストレーター
# ─────────────────────────────────────────────────────────────────
def run(target_date: date, test_mode: bool = False, do_recalc: bool = False,
        do_update_schedule: bool = False, do_remind: bool = False,
        do_fill_returns: bool = False, do_weekly_analysis: bool = False):
    logger.info(f"=== MACRO PULSE v6.0 | {target_date} | recalc={do_recalc} | "
                f"update_schedule={do_update_schedule} | remind={do_remind} | "
                f"fill_returns={do_fill_returns} | weekly_analysis={do_weekly_analysis} ===")

    ensure_schedule_csv()
    schedule = load_schedule()
    events   = load_events()

    if do_remind:
        remind_manual_indicators(target_date)
        return

    if do_weekly_analysis:
        run_weekly_analysis(target_date)
        return

    if do_update_schedule:
        logger.info("=== UPDATE SCHEDULE MODE ===")
        fred_api_key = os.environ.get("FRED_API_KEY", "")
        if not fred_api_key:
            logger.error("FRED_API_KEY not set.")
            sys.exit(1)
        update_schedule(fred_api_key)
        update_fed_context(target_date)
        remind_missing_actuals(target_date)
        logger.info("=== Schedule + Fed Context update complete ===")
        return

    if do_recalc:
        logger.info("=== RECALC MODE ===")
        updated = recalc(events)
        save_events(updated)
        return

    if do_fill_returns:
        logger.info("=== FILL RETURNS MODE ===")
        fill_returns()
        return

    fin_ctx  = get_financial_context(target_date)
    sp500_t0 = get_sp500(target_date)
    logger.info(f"Financial context: {fin_ctx}")
    logger.info(f"S&P500 t0: {sp500_t0}")

    date_str  = target_date.strftime("%Y-%m-%d")
    scheduled = schedule[schedule["release_date"] == date_str].to_dict("records")
    logger.info(f"Scheduled today: {[r['indicator'] for r in scheduled]}")

    new_rows = []

    for sched in scheduled:
        ind = sched["indicator"]
        if INDICATOR_CONFIG.get(ind, {}).get("daily"):
            continue

        override = None
        raw = str(sched.get("actual", "")).strip()
        if raw and raw.lower() not in ("", "nan"):
            try:
                override = float(raw)
            except ValueError:
                pass

        try:
            row = fetch_event_row(ind, target_date, fin_ctx, schedule, events, override)
            row["sp500_t0"] = str(sp500_t0) if sp500_t0 else ""
            new_rows.append(row)
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"[{ind}]: {e}\n{traceback.format_exc()}")

    for ind_name in ["Yield Curve 10Y-2Y", "HY Spread", "VIX"]:
        try:
            row = fetch_event_row(ind_name, target_date, fin_ctx, schedule, events)
            row["sp500_t0"] = str(sp500_t0) if sp500_t0 else ""
            new_rows.append(row)
        except Exception as e:
            logger.error(f"[{ind_name}]: {e}")

    # 月次FRED指標の自動補完（スケジュール未登録・空actual の再試行）
    # MACRO-NFP-1: 上のscheduledループで追加済みの行を反映したスナップショットを渡す。
    # 素の events のまま渡すと、同一実行内で追加された行が見えず、
    # 同一FRED観測値が別のevent_idスロットに二重書き込みされる（BUG再発防止）。
    events_snapshot = (
        pd.concat([events, pd.DataFrame(new_rows, columns=EVENTS_COLUMNS)], ignore_index=True)
        if new_rows else events
    )
    new_rows.extend(
        refresh_monthly_indicators(target_date, fin_ctx, schedule, events_snapshot, sp500_t0)
    )

    if not new_rows:
        logger.info("No rows to add.")
        return

    # MACRO-NFP-1: 同一indicator×同一actual値×近接release_dateの重複行を除外する
    new_rows = dedupe_new_rows(new_rows, events)

    if not new_rows:
        logger.info("No rows to add after dedup.")
        return

    new_df = pd.DataFrame(new_rows, columns=EVENTS_COLUMNS)
    key_new = set(new_df["event_id"])
    existing_filtered = events[~events["event_id"].isin(key_new)]
    combined = pd.concat([existing_filtered, new_df], ignore_index=True)
    save_events(combined)

    # 流動性モニター更新（スコア計算には影響しない参考情報）
    # [[HOLLOW-RALLY-DEAD-1]]: sp500_t0は本関数冒頭で既にget_sp500()
    # 取得済みのため、update_liquidity_csv()内で再取得せずそのまま渡す
    update_liquidity_csv(target_date, sp500_t0)

    logger.info("=== Run complete ===")

# ─────────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="MACRO PULSE v6.0")
    p.add_argument("--test",            action="store_true")
    p.add_argument("--recalc",          action="store_true", help="Recalculate surprises")
    p.add_argument("--update-schedule", action="store_true", help="Update schedule + fed context")
    p.add_argument("--remind",          action="store_true", help="Send Discord reminders for today's manual indicators")
    p.add_argument("--fill-returns",    action="store_true", help="Backfill S&P500 t+N returns")
    p.add_argument("--weekly-analysis", action="store_true", help="Generate weekly AI commentary")
    p.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: yesterday)")
    args = p.parse_args()
    target = (datetime.strptime(args.date, "%Y-%m-%d").date()
              if args.date else (datetime.now() - timedelta(days=1)).date())
    run(target,
        test_mode=args.test,
        do_recalc=args.recalc,
        do_update_schedule=args.update_schedule,
        do_remind=args.remind,
        do_fill_returns=args.fill_returns,
        do_weekly_analysis=args.weekly_analysis)

if __name__ == "__main__":
    main()