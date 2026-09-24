"""
HypeCore PoC v2 - src/value/hypecore/poc.py

ステージ定義（Koichi定義準拠）:
  S0 失望/蓄積期: 機関未参入・規模小・赤字頻発・スマートマネー仕込み段階
  S1 期待覚醒期:  EPS上方修正・出来高急増・MA50突破・新カタリスト出現
  S2 期待拡大期:  マルチプル拡大・機関積み増し・カタリスト連発・黒字定着
  S3 陶酔期:      RSI過熱・PER異常・insider売り・良ニュースで株価反応鈍化
  S4 期待剥落期:  ガイダンス下方修正・良決算でも下落・機関Distribution開始
"""

import json
import os
import sys
import warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── パス設定 ──────────────────────────────────────────────
_HERE      = Path(__file__).resolve().parent
_REPO_ROOT = Path(__file__).resolve().parents[3]
_TANUKI_DIR = _REPO_ROOT / "docs" / "value-monitor" / "tanuki_valuation" / "data"
_OUT_DIR    = _HERE / "data"
_OUT_DIR.mkdir(exist_ok=True, parents=True)

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# [フェーズD Step2-4対応、2026-08-07] SEC EDGAR Layer3
# （common/sec_data/layer3_builder.py、company_facts.json由来の統合
# スキーマ）経由でnormalized/参照を廃止した。
from common.sec_data.layer3_builder import (  # noqa: E402
    build_ticker_store, get_quarterly_series,
)

# common/market_data - [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8:
# yfinance直接呼び出し（.history()・.info・upgrades_downgrades・
# earnings_history・recommendations）をcommon.market_data.reader経由に
# 切替（2026-08-11）。他の切替済みファイルと同じHAS_MARKET_DATAガード・
# 二段構えsys.path解決パターンを踏襲する（sys.pathは上のREPO_ROOT設定で
# 既に解決済みのため一段目は不要だが、他ファイルとの一貫性のため
# 同型のtry/exceptで統一する）。
HAS_MARKET_DATA = False
_md_get_price_series = None
_md_get_latest_price = None
_md_get_attributes = None
_md_get_analyst_events = None
_md_get_earnings_history = None
_md_get_recommendations_history = None
# [[HYPECORE-CI-SILENT-FAILURE-1]]: import失敗理由を保持し、__main__で
# 致命扱い（exit 1）にする際に表示する
_MARKET_DATA_IMPORT_ERROR = None

try:
    from common.market_data.reader import get_price_series as _md_get_price_series  # noqa: E402
    from common.market_data.reader import get_latest_price as _md_get_latest_price  # noqa: E402
    from common.market_data.reader import get_attributes as _md_get_attributes  # noqa: E402
    from common.market_data.reader import get_analyst_events as _md_get_analyst_events  # noqa: E402
    from common.market_data.reader import get_earnings_history as _md_get_earnings_history  # noqa: E402
    from common.market_data.reader import get_recommendations_history as _md_get_recommendations_history  # noqa: E402
    HAS_MARKET_DATA = True
except Exception as _e:
    _MARKET_DATA_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"

if not HAS_MARKET_DATA:
    try:
        _github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if _github_workspace and _github_workspace not in sys.path:
            sys.path.insert(0, _github_workspace)
        from common.market_data.reader import get_price_series as _md_get_price_series
        from common.market_data.reader import get_latest_price as _md_get_latest_price
        from common.market_data.reader import get_attributes as _md_get_attributes
        from common.market_data.reader import get_analyst_events as _md_get_analyst_events
        from common.market_data.reader import get_earnings_history as _md_get_earnings_history
        from common.market_data.reader import get_recommendations_history as _md_get_recommendations_history
        HAS_MARKET_DATA = True
    except Exception as _e:
        _MARKET_DATA_IMPORT_ERROR = f"{type(_e).__name__}: {_e}"

# PascalCase（本ファイル内の既存呼び出し表記）→ SEC EDGAR Layer3の
# snake_caseフィールド名の対応表（フェーズD Step2-4対応）。
_SEC_LAYER3_FIELD_MAP = {
    "Revenue": "revenue",
    "NetIncome": "net_income",
    "OCF": "operating_cash_flow",
}

# ── ステージ定義 ──────────────────────────────────────────
STAGE_LABELS = {
    0: "失望/蓄積期",
    1: "期待覚醒期",
    2: "期待拡大期",
    3: "陶酔期",
    4: "期待剥落期",
}

# 正解ラベル（PLTR・月次・Koichi感覚値）
PLTR_GROUND_TRUTH = {
    "2024-01": 2, "2024-02": 3, "2024-03": 3,
    "2024-04": 2, "2024-05": 2, "2024-06": 3,
    "2024-07": 3, "2024-08": 3, "2024-09": 3,
    "2024-10": 3, "2024-11": 3, "2024-12": 3,
    "2025-01": 3, "2025-02": 3, "2025-03": 3,
    "2025-04": 3, "2025-05": 3, "2025-06": 3,
    "2025-07": 3, "2025-08": 3, "2025-09": 3,
    "2025-10": 3, "2025-11": 4, "2025-12": 4,
    "2026-01": 4, "2026-02": 4, "2026-03": 4,
    "2026-04": 4, "2026-05": 4,
}


def z_score_series(s: pd.Series, window: int = 24) -> pd.Series:
    """ローリングZ-score（自分自身の過去window期間を基準）"""
    s = s.replace([np.inf, -np.inf], np.nan)
    roll_mean = s.rolling(window, min_periods=6).mean()
    roll_std  = s.rolling(window, min_periods=6).std()
    return (s - roll_mean) / (roll_std + 1e-9)


def _positive_or_none(v):
    """[[TANUKI-VALUATION-MISC-GAPS-1]]②（2026-09-16）: EV/EBITDAは
    EBITDA<0の場合、比率自体が意味をなさない（分母が負）。従来は負値も
    そのまま格納し表示側（detail.html）でのみフィルタしていたが、格納
    時点でNone化する（消費者側で個別にガードしなくても安全な値になる）。
    """
    try:
        return float(v) if v is not None and float(v) > 0 else None
    except (TypeError, ValueError):
        return None


# ── データ取得 ────────────────────────────────────────────

def fetch_price_data(ticker: str, start: str = "2021-01-01") -> pd.DataFrame:
    """market_data daily/層から日次株価・出来高を取得し月次に集約

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8: yfinance直接呼び出し
    （.history(start=...)）をcommon.market_data.reader経由に切替
    （2026-08-11）。reader.get_price_series(ticker, days=1400)相当で
    daily/層の生系列（前提作業1で2021-01-01以降をカバーするようbackfill
    済み）を取得し、以降のma50/ma200/rsi/volume_ratio自前rolling計算
    ロジックはpandas DataFrameへ変換した上でそのまま維持する（daily/層
    内での自前計算という設計方針、事前調査で確立済み）。startパラメータは
    シグネチャ互換のため残すが、daily/層のバックフィル済み範囲
    （2021-01-01〜）を前提とした固定日数指定に置き換わったため実際の
    絞り込みには使用しない。

    旧来の`hist.empty`時のValueError送出は、reader.get_price_series()が
    「対象銘柄のデータが完全に存在しない」場合のみ空リストを返す設計を
    活かし、実データ0件（_gapのみ・全欠損）の場合に限定して判定する
    よう改善した（単発の営業日欠損では発火しない、事前調査の改善案）。
    """
    series = _md_get_price_series(ticker, days=1400) if HAS_MARKET_DATA else []
    real_records = [r for r in series if not r.get("_gap") and r.get("close") is not None]
    if not real_records:
        raise ValueError(f"{ticker}: 株価データ取得失敗")

    hist = pd.DataFrame(real_records)
    hist["date"] = pd.to_datetime(hist["date"])
    hist = hist.set_index("date").sort_index()
    hist = hist[["close", "volume"]].rename(columns={"close": "price"})

    monthly = hist.resample("ME").agg({"price": "last", "volume": "mean"})
    monthly.index = monthly.index.to_period("M").to_timestamp()

    # テクニカル指標（日次計算→月末値）
    hist["ma50"]  = hist["price"].rolling(50).mean()
    hist["ma200"] = hist["price"].rolling(200).mean()

    delta = hist["price"].diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    hist["rsi"] = 100 - 100 / (1 + gain / (loss + 1e-9))

    hist["ma50_dev"]  = (hist["price"] - hist["ma50"])  / (hist["ma50"]  + 1e-9) * 100
    # [[HYPECORE-MISC-NAMING-GAPS-1]]③（2026-08-29）: TANUKI VALUATION
    # 側（index.html::buildRows()）にも同名「ma200_dev」（現在は
    # common.market_data.reader.get_ma_deviation()基準、旧BACKLOG記載の
    # 「yfinance twoHundredDayAverage」は2026-08-11のMARKETDATA-LAYER-
    # CONSTRUCTION-1移行で陳腐化していたと着手時に判明・訂正済み。
    # ma200_dev_mdへ改名）が別実装として存在し、規則1（データソース
    # 接尾辞）の趣旨に基づき自前pandas rolling(200)計算であることを
    # 示す_localを本フィールドに付与する。
    hist["ma200_dev_local"] = (hist["price"] - hist["ma200"]) / (hist["ma200"] + 1e-9) * 100

    # 出来高比（日次20日平均比。[[HYPECORE-MISC-NAMING-GAPS-1]]⑤:
    # 月次6ヶ月平均比のvol_surge_6mとは時間粒度が異なる別指標のため、
    # 規則2〈期間接尾辞〉に基づき窓幅を明示する）
    hist["vol_20d_avg"] = hist["volume"].rolling(20).mean()
    hist["volume_ratio_20d"] = hist["volume"] / (hist["vol_20d_avg"] + 1e-9)

    tech = hist[["ma50_dev", "ma200_dev_local", "rsi", "volume_ratio_20d"]].resample("ME").last()
    tech.index = tech.index.to_period("M").to_timestamp()

    # 月次出来高（月平均）
    vol_monthly = hist[["volume"]].resample("ME").mean()
    vol_monthly.index = vol_monthly.index.to_period("M").to_timestamp()
    vol_monthly.columns = ["volume_monthly"]

    return monthly.join(tech).join(vol_monthly)


def fetch_info_snapshot(ticker: str) -> dict:
    """market_data attributes/層から現時点のバリュエーション・アナリスト
    情報を取得

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8: yfinance直接呼び出し
    （.info）をcommon.market_data.reader経由に切替（2026-08-11）。
    14フィールド中7件は着手順序4-2で追加済みの既存attributes/スキーマ、
    残り7件は前提作業2（2026-08-11）で追加済み。volume_vs_avgのみ
    daily/層の最新出来高（reader.get_latest_price()のvolume）と
    attributes/のaverage_volumeを組み合わせて算出するが、これは
    reader.py自体が層をまたいで内部計算するわけではなく、消費者側
    （本関数）が2つの独立したAPI呼び出し結果を単純に除算しているだけ
    （旧コードのcur_vol/avg_volと同型の計算、いずれも「出来高」という
    同じ物理量同士の比較のため、BACKLOG確定事項4が禁じる「異なる時点の
    価格を組み合わせた指標再計算」には該当しない）。

    common.market_data未import環境・データ未取得銘柄では、旧来の
    `.info`取得失敗時と同じ中立デフォルト（全フィールドNone）に倒す。
    """
    if not HAS_MARKET_DATA:
        return {
            "forward_pe": None, "trailing_pe": None, "psr": None, "peg_ratio": None,
            "revenue_growth": None, "earnings_growth": None, "gross_margins": None,
            "recommendation_mean": None, "num_analysts": None, "short_pct_float": None,
            "short_ratio": None, "volume_vs_avg": None, "market_cap": None,
            "shares": None, "ev_ebitda": None,
        }
    attrs = _md_get_attributes(ticker) or {}
    avg_vol = attrs.get("average_volume") or 1
    latest_price = _md_get_latest_price(ticker)
    cur_vol = (latest_price.get("volume") if latest_price is not None else None) or avg_vol
    return {
        "forward_pe":         attrs.get("forward_pe"),
        "trailing_pe":        attrs.get("trailing_pe"),
        "psr":                attrs.get("price_to_sales"),
        "peg_ratio":          attrs.get("peg_ratio"),
        "revenue_growth":     attrs.get("revenue_growth"),        # YoY (小数)
        "earnings_growth":    attrs.get("earnings_growth"),       # YoY (小数)
        "gross_margins":      attrs.get("gross_margins"),
        "recommendation_mean": attrs.get("recommendation_mean"),  # 1=Strong Buy, 5=Sell
        "num_analysts":       attrs.get("analyst_count"),
        "short_pct_float":    attrs.get("short_pct_float"),
        "short_ratio":        attrs.get("short_ratio"),
        "volume_vs_avg":      cur_vol / avg_vol if avg_vol else None,
        "market_cap":         attrs.get("market_cap"),
        "shares":             attrs.get("shares_outstanding"),
        "ev_ebitda":          _positive_or_none(attrs.get("ev_to_ebitda")),
    }


def fetch_quarterly_fundamentals(ticker: str) -> pd.DataFrame:
    """SEC EDGAR Layer3（company_facts.json由来の統合スキーマ）から
    四半期財務データを取得し月次補間"""
    store = build_ticker_store(ticker)
    if store is None:
        print(f"  警告: Layer3ストア構築不可（company_facts.json欠落）: {ticker}")
        return pd.DataFrame()

    def extract(fname: str) -> pd.Series:
        entries = [
            e for e in get_quarterly_series(store, _SEC_LAYER3_FIELD_MAP[fname])
            if e.get("val") is not None
        ]
        if not entries:
            return pd.Series(dtype=float)
        df = pd.DataFrame(entries)[["end", "val"]].copy()
        df["end"] = pd.to_datetime(df["end"])
        return df.set_index("end").sort_index()["val"]

    rev = extract("Revenue")
    ni  = extract("NetIncome")
    ocf = extract("OCF")

    if rev.empty:
        return pd.DataFrame()

    rev_ttm       = rev.rolling(4).sum()
    rev_ttm_prior = rev_ttm.shift(4).replace(0, np.nan)
    rev_yoy       = (rev_ttm / rev_ttm_prior - 1) * 100
    ni_yoy      = ni.pct_change(4) * 100 if not ni.empty else pd.Series(dtype=float)
    # [[RULE40-DEFINITION-MISMATCH-1]]: 旧変数名"op_margin"はNetIncome/Revenue
    # （純利益率）を計算しており誤称だったため、net_marginへ改名（2026-08-13）。
    net_margin  = (ni / rev.replace(0, np.nan) * 100) if (not ni.empty) else pd.Series(dtype=float)
    # rule40 → rule40_yoy_netmargin（TTM売上YoY＋単一四半期純利益率であることを
    # 明示。NAMING_CONVENTIONS.md規則2、STONKS SILOのrule40_cagr3y_opmarginとの
    # 定義相違を区別するための改名）。
    rule40_yoy_netmargin = rev_yoy + net_margin if not net_margin.empty else pd.Series(dtype=float)

    result = pd.DataFrame({
        "rev_yoy":              rev_yoy,
        "ni_yoy":               ni_yoy,
        "rule40_yoy_netmargin": rule40_yoy_netmargin,
        "ocf":                  ocf,
        "revenue":              rev,
    }).dropna(how="all")

    result.index = pd.to_datetime(result.index)
    monthly_idx = pd.date_range(
        start=result.index.min(),
        end=date.today().isoformat(),
        freq="MS"
    )
    return result.reindex(monthly_idx, method="ffill")


def fetch_tanuki_iv(ticker: str) -> pd.Series:
    """TANUKI history + latestからIV時系列を取得"""
    ticker_dir = _TANUKI_DIR / ticker
    iv_series = {}

    history_dir = ticker_dir / "history"
    if history_dir.exists():
        for f in sorted(history_dir.glob("*.json")):
            try:
                with open(f, encoding="utf-8") as fh:
                    d = json.load(fh)
                iv = d.get("intrinsic_value_per_share") or d.get("iv_per_share")
                calc_date = d.get("calculation_date") or f.stem
                if iv and calc_date:
                    ts = pd.Timestamp(calc_date[:10]).to_period("M").to_timestamp()
                    iv_series[ts] = float(iv)
            except Exception:
                continue

    latest_path = ticker_dir / "latest.json"
    if latest_path.exists():
        with open(latest_path, encoding="utf-8") as f:
            d = json.load(f)
        iv = d.get("intrinsic_value_per_share") or d.get("iv_per_share")
        if iv:
            today = pd.Timestamp(date.today()).to_period("M").to_timestamp()
            iv_series[today] = float(iv)

    if iv_series:
        s = pd.Series(iv_series).sort_index()
        print(f"  TANUKI IV: {len(s)}件 最新=${s.iloc[-1]:.2f}")
        return s

    print(f"  警告: {ticker} のTANUKI IVが見つかりません")
    return pd.Series(dtype=float)


def fetch_analyst_history(ticker: str) -> pd.DataFrame:
    """
    market_data analyst_history/層からアナリスト履歴を月次DataFrameに変換。

    取得データ:
      upgrades_downgrades → 月次アナリスト修正率（上方/下方）
      earnings_history    → 四半期EPSサプライズ率
      recommendations     → 直近Buy/Hold/Sell比率（現時点値）

    返却列:
      analyst_upgrade_rate : 上方修正/(上方+下方) 3ヶ月移動平均
      analyst_downgrade_rate: 下方修正率
      eps_surprise         : EPSサプライズ率% (四半期→月次前方補完)
      buy_ratio            : (StrongBuy+Buy)/全アナリスト (現時点値のみ)
                              [[HYPECORE-MISC-NAMING-GAPS-1]]①（2026-08-29）:
                              旧名buy_hold_ratioは実際の計算式にholdが
                              含まれない誤称だったためリネーム
      sell_on_good_news    : EPSサプライズ>0 かつ 当月株価変化<-3% (bool→float)

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-8: yfinance直接呼び出し
    （upgrades_downgrades/earnings_history/quarterly_earnings/.info/
    recommendations）をcommon.market_data.reader経由に切替（2026-08-11）。
    - upgrades_downgrades: reader.get_analyst_events()（フィールド名は
      market_data側のsnake_case、date/firm/to_grade/from_grade/action）
    - eps_surprise: reader.get_earnings_history()から最新四半期を取得。
      フォールバック①（quarterly_earnings）は前提作業3の事前調査で
      現行yfinanceにおいて常にNoneを返すと実測確認済みのため実装しない。
      フォールバック②（info['earningsGrowth']）は前提作業2で既に
      attributes/{SYMBOL}.jsonのearnings_growthとして保存済みのため
      reader.get_attributes()["earnings_growth"]を直接参照する（fetcher.py
      側でのフォールバック合成はしない設計方針、前回投資調査通り）。
    - buy_ratio: reader.get_recommendations_history(ticker,
      latest_only=True)のbuy_hold_ratioキーから取得（fetcher.py側で
      既に計算済みの値をそのまま使う、旧コードの現場計算ロジックは
      fetcher.py側へ移譲）。fetcher.py側の生データキー名（analyst_history/
      {SYMBOL}.jsonの蓄積履歴、market_data層の共有インフラ）は
      buy_hold_ratioのまま維持し、本関数の出力列名のみbuy_ratioへ
      リネームする（[[HYPECORE-MISC-NAMING-GAPS-1]]①、2026-08-29）。
    """
    result = pd.DataFrame()

    # ── 1. upgrades_downgrades → 月次修正率 ──────────────────
    try:
        events = _md_get_analyst_events(ticker) if HAS_MARKET_DATA else []
        if events:
            ud = pd.DataFrame(events)
            ud["date"] = pd.to_datetime(ud["date"])
            ud = ud.set_index("date").sort_index()

            # to_gradeでBuy系/Sell系を分類
            BUY_GRADES  = {"Buy","Strong Buy","Outperform","Overweight","Market Outperform","Positive"}
            SELL_GRADES = {"Sell","Strong Sell","Underperform","Underweight","Market Underperform","Negative"}

            ud["is_upgrade"]   = ud["to_grade"].isin(BUY_GRADES).astype(int)
            ud["is_downgrade"] = ud["to_grade"].isin(SELL_GRADES).astype(int)

            monthly_ud = ud.resample("ME").agg(
                upgrades=("is_upgrade", "sum"),
                downgrades=("is_downgrade", "sum"),
            )
            monthly_ud.index = monthly_ud.index.to_period("M").to_timestamp()
            monthly_ud["total_changes"] = monthly_ud["upgrades"] + monthly_ud["downgrades"]
            monthly_ud["analyst_upgrade_rate"] = (
                monthly_ud["upgrades"] / (monthly_ud["total_changes"] + 1e-9)
            )
            monthly_ud["analyst_downgrade_rate"] = (
                monthly_ud["downgrades"] / (monthly_ud["total_changes"] + 1e-9)
            )
            # 3ヶ月移動平均でノイズ除去
            monthly_ud["analyst_upgrade_rate"] = (
                monthly_ud["analyst_upgrade_rate"].rolling(3, min_periods=1).mean()
            )
            result = monthly_ud[["analyst_upgrade_rate", "analyst_downgrade_rate"]]
            print(f"  アナリスト修正: {len(events)}件（{ud.index.min().date()}〜）")
    except Exception as e:
        print(f"  警告: upgrades_downgrades取得失敗: {e}")

    # ── 2. earnings_history → 四半期EPSサプライズ率 ──────────
    eps_fetched = False
    try:
        earnings_hist = _md_get_earnings_history(ticker) if HAS_MARKET_DATA else []
        if earnings_hist:
            eh = pd.DataFrame(earnings_hist)
            eh["quarter"] = pd.to_datetime(eh["quarter"])
            eh = eh.set_index("quarter").sort_index()
            eh_monthly = eh[["surprise_percent"]].copy()
            eh_monthly.columns = ["eps_surprise"]
            eh_monthly["eps_surprise"] *= 100  # 生値（小数）→% 変換は消費側の責務
            if not result.empty:
                monthly_idx = result.index
            else:
                monthly_idx = pd.date_range(
                    start=eh_monthly.index.min(),
                    end=date.today().isoformat(),
                    freq="MS"
                )
            eps_monthly = eh_monthly.reindex(monthly_idx, method="ffill", limit=4)
            # 最新月がNullの場合、最後の有効値で補完
            if "eps_surprise" in eps_monthly.columns:
                eps_monthly["eps_surprise"] = eps_monthly["eps_surprise"].ffill()
            if result.empty:
                result = eps_monthly
            else:
                result = result.join(eps_monthly, how="outer")
            print(f"  EPSサプライズ: {len(earnings_hist)}件")
            eps_fetched = True
    except Exception as e:
        print(f"  警告: earnings_history取得失敗: {e}")

    # フォールバック①（quarterly_earnings）は着手順序4-8前提作業3の事前調査で
    # 現行yfinanceにおいて常にNoneを返す（DeprecationWarning: 'Ticker.earnings'
    # is deprecated as not available via API）と実測確認済みのため実装しない。

    # フォールバック②: attributes/のearnings_growthを欠損月のみに適用
    if not eps_fetched:
        try:
            attrs = _md_get_attributes(ticker) if HAS_MARKET_DATA else None
            eg = attrs.get("earnings_growth") if attrs else None
            if eg is not None:
                eg_val = round(float(eg) * 100, 2)
                if not result.empty:
                    if "eps_surprise" not in result.columns:
                        result["eps_surprise"] = eg_val
                    else:
                        result["eps_surprise"] = result["eps_surprise"].fillna(eg_val)
                print(f"  EPSサプライズ(fallback/earnings_growth): {eg:.1%}")
                eps_fetched = True
        except Exception as e:
            print(f"  警告: earnings_growth取得失敗: {e}")

    # 最終補完: eps_surpriseのNullを前方補完（最大3ヶ月）
    if not result.empty and "eps_surprise" in result.columns:
        result["eps_surprise"] = result["eps_surprise"].ffill(limit=3)

    if not eps_fetched:
        print(f"  警告: EPSサプライズ全手段で取得失敗")

    # ── 3. recommendations → Buy/Hold/Sell比率（現時点） ──────
    try:
        rec = _md_get_recommendations_history(ticker, latest_only=True) if HAS_MARKET_DATA else {}
        buy_ratio = rec.get("buy_hold_ratio") if rec else None
        if buy_ratio is not None:
            # 現時点値を全期間に設定（将来は月次記録で上書き、週次バッチが
            # 蓄積するrecommendations_historyが育つにつれ自然に解消される）
            if "buy_ratio" not in result.columns:
                result["buy_ratio"] = np.nan
            today_ts = pd.Timestamp(date.today()).to_period("M").to_timestamp()
            if today_ts in result.index:
                result.loc[today_ts, "buy_ratio"] = buy_ratio
            print(f"  Buy比率: {buy_ratio:.1%}（現時点）")
    except Exception as e:
        print(f"  警告: recommendations取得失敗: {e}")

    return result


# ── スコア計算 ────────────────────────────────────────────

def compute_scores(ticker: str) -> pd.DataFrame:
    """全指標を月次DataFrameに統合"""
    print(f"\n[{ticker}] データ取得中...")

    price_df = fetch_price_data(ticker, start="2021-01-01")
    print(f"  株価: {len(price_df)}ヶ月分")

    fund_df = fetch_quarterly_fundamentals(ticker)
    print(f"  財務: {len(fund_df)}行")

    iv_series = fetch_tanuki_iv(ticker)

    # アナリスト履歴（月次）
    analyst_df = fetch_analyst_history(ticker)

    # .info（現時点値）
    info = fetch_info_snapshot(ticker)
    _fpe = info.get('forward_pe')
    print(f"  .info: ForwardPE={_fpe:.1f} PEG={info.get('peg_ratio')} "
          f"RevGrowth={info.get('revenue_growth')}"
          if _fpe is not None else
          f"  .info: ForwardPE=N/A PEG={info.get('peg_ratio')} "
          f"RevGrowth={info.get('revenue_growth')}")

    # 結合
    df = price_df.copy()
    if not fund_df.empty:
        df = df.join(fund_df, how="left")

    # IV乖離率（時系列）
    if not iv_series.empty:
        iv_monthly = iv_series.reindex(df.index, method="ffill")
        df["iv"] = iv_monthly
        df["price_iv_ratio"] = df["price"] / df["iv"]
    else:
        df["price_iv_ratio"] = np.nan

    # OCF Yield（単一四半期のOCF÷時価総額。[[HYPECORE-MISC-NAMING-GAPS-1]]②
    # 〈2026-08-29〉: 旧名fcf_yieldは実際には分子がOCF〈CapEx控除前〉で
    # FCFではなく、年率換算もされていない誤称だったためリネーム。
    # _qは「単一四半期の生値・非年率換算」を表す本コード独自の接尾辞
    # 〈NAMING_CONVENTIONS.md規則2の既存4種_yoy/_ttm/_fy/_cagrNyには
    # 該当なし、規則2の趣旨〈期間の明示〉を踏まえた拡張〉）
    shares = info.get("shares")
    if shares and "ocf" in df.columns:
        df["ocf_yield_q"] = df["ocf"] / (df["price"] * shares) * 100
    else:
        df["ocf_yield_q"] = np.nan

    # .info現時点値を最新月にのみセット（将来は月次記録に拡張）
    today_ts = pd.Timestamp(date.today()).to_period("M").to_timestamp()
    for key in ["forward_pe", "trailing_pe", "peg_ratio", "earnings_growth",
                "recommendation_mean", "short_pct_float", "volume_vs_avg",
                "gross_margins", "psr", "ev_ebitda"]:
        df[key] = np.nan
        if today_ts in df.index:
            df.loc[today_ts, key] = info.get(key)

    # revenue_growth（yfinance）: [[HYPECORE-MISC-NAMING-GAPS-1]]⑥
    # 〈2026-08-29〉。同一概念のrev_yoy（SEC EDGAR TTM%）と算出基準が
    # 異なるため規則1〈データソース接尾辞〉に基づき_yfを付与する。
    # detectLifecycle()（index.html/detail.html）がrevenue_growth_yf
    # null時にrev_yoyへ暗黙フォールバックする構造は変わらないため、
    # フロントエンド側でどちらが実際に使われたかをtitle属性で明示する
    # （下記フロントエンド変更参照）。
    df["revenue_growth_yf"] = np.nan
    if today_ts in df.index:
        df.loc[today_ts, "revenue_growth_yf"] = info.get("revenue_growth")

    # アナリスト履歴を結合
    if not analyst_df.empty:
        df = df.join(analyst_df, how="left")

    # eps_surpriseのffill（join後に欠損が生じる場合の補完）
    if "eps_surprise" in df.columns:
        df["eps_surprise"] = df["eps_surprise"].ffill(limit=4)
    
    # EPSが完全に取れない場合、earningsGrowthで補完
    if "eps_surprise" not in df.columns or df["eps_surprise"].isna().all():
        eg = info.get("earnings_growth")
        if eg is not None:
            df["eps_surprise"] = round(float(eg) * 100, 2)

    # ── 生値指標 ──────────────────────────────────────────

    df["peak_24m"]    = df["price"].rolling(24, min_periods=6).max()
    df["from_peak"]   = (df["price"] - df["peak_24m"]) / df["peak_24m"] * 100
    df["price_mom3m"] = df["price"].pct_change(3) * 100
    df["ma200_mom"]   = df["ma200_dev_local"].diff(3)
    df["ma50_cross"]  = (df["ma50_dev"] > 0).astype(int)  # MA50上抜け

    # 出来高急増フラグ（月次平均の1.5倍以上。[[HYPECORE-MISC-NAMING-GAPS-1]]
    # ⑤〈2026-08-29〉: 日次20日平均比のvolume_ratio_20dとは時間粒度が
    # 異なる別指標のため_6mを付与）
    vol_avg = df["volume_monthly"].rolling(6, min_periods=3).mean()
    df["vol_surge_6m"] = df["volume_monthly"] / (vol_avg + 1e-9)

    # sell_on_good_news: EPSサプライズ>0 かつ 当月株価変化<-3%（S4の核心シグナル）
    df["price_mom1m"] = df["price"].pct_change(1) * 100
    if "eps_surprise" in df.columns:
        df["sell_on_good_news"] = (
            (df["eps_surprise"] > 5) & (df["price_mom1m"] < -3)
        ).astype(float)
    else:
        df["sell_on_good_news"] = np.nan

    # アナリストスコア（期待スコアへの寄与）
    # upgrade_rate高い=期待上昇中、低い=期待剥落中
    if "analyst_upgrade_rate" in df.columns:
        df["analyst_score"] = z_score_series(df["analyst_upgrade_rate"])
    else:
        df["analyst_score"] = np.nan

    # ── Z-scoreスコア ──────────────────────────────────────

    # 期待スコア（高いほど期待過熱）
    expect_cols = []
    for col in ["ma200_dev_local", "ma50_dev"]:
        if col in df.columns:
            expect_cols.append(z_score_series(df[col]))
    if not df["price_iv_ratio"].isna().all():
        expect_cols.append(z_score_series(df["price_iv_ratio"]))
    # アナリスト修正率（上方修正が多いほど期待過熱）
    if "analyst_score" in df.columns and not df["analyst_score"].isna().all():
        expect_cols.append(df["analyst_score"])
    df["expectation_score"] = pd.concat(expect_cols, axis=1).mean(axis=1) if expect_cols else np.nan

    # 実体スコア（高いほど実体良好）
    fund_cols = []
    for col in ["rev_yoy", "ni_yoy", "rule40_yoy_netmargin", "ocf_yield_q"]:
        if col in df.columns and not df[col].isna().all():
            fund_cols.append(z_score_series(df[col]))
    df["fundamental_score"] = pd.concat(fund_cols, axis=1).mean(axis=1) if fund_cols else np.nan

    # モメンタムスコア（高いほど上昇トレンド）
    mom_cols = []
    for col in ["ma50_dev", "ma200_dev_local", "rsi"]:
        if col in df.columns:
            mom_cols.append(z_score_series(df[col]))
    df["momentum_score"] = pd.concat(mom_cols, axis=1).mean(axis=1) if mom_cols else np.nan

    return df


# ── ステージ判定 ────────────────────────────────────────────

def determine_stage(row: pd.Series, prev_stage: int = 2, s3_streak: int = 0, s4_streak: int = 0) -> int:
    """
    Koichi定義に基づくステージ判定。
    「期待と実体の関係性」で判断する。

    s3_streak: 直前からS3が何ヶ月連続しているか（S4転落の文脈判定に使用）
    s4_streak: 直前からS4が何ヶ月連続しているか（S4脱出条件に使用）

    判定優先順位:
      1. S4優先チェック（S3が一定期間続いた後の急落）
      2. S3（陶酔期）: 期待が実体を大幅超過
      3. S4（期待剥落期）: 期待の崩壊
      4. S0（失望/蓄積期）: 深い低迷・無関心
      5. S1（期待覚醒期）: 期待の萌芽
      6. S2（期待拡大期）: 期待と実体が噛み合っている
    """
    # テクニカル生値
    ma200_dev   = row.get("ma200_dev_local", 0) or 0
    ma200_mom   = row.get("ma200_mom",   0) or 0
    from_peak   = row.get("from_peak",   0) or 0
    price_mom3m = row.get("price_mom3m", 0) or 0
    rsi         = row.get("rsi",        50) or 50
    vol_surge   = row.get("vol_surge_6m", 1) or 1

    # アナリスト・EPS指標
    sell_on_good = row.get("sell_on_good_news", 0) or 0  # 良決算でも下落
    eps_surprise = row.get("eps_surprise")               # EPSサプライズ率%
    upgrade_rate = row.get("analyst_upgrade_rate")       # アナリスト上方修正率
    buy_ratio    = row.get("buy_ratio")                  # Buy比率（[[HYPECORE-MISC-NAMING-GAPS-1]]①旧buy_hold_ratio）

    # バリュエーション（現時点値・NaNの場合は判定に使わない）
    forward_pe  = row.get("forward_pe")
    peg         = row.get("peg_ratio")
    rev_growth  = row.get("revenue_growth_yf")   # 小数（YoY、未使用の内部変数）
    earn_growth = row.get("earnings_growth")  # 小数（YoY）
    short_pct   = row.get("short_pct_float")
    rec_mean    = row.get("recommendation_mean")

    # Z-scoreスコア
    e = row.get("expectation_score",  0) or 0
    f = row.get("fundamental_score",  0) or 0
    m = row.get("momentum_score",     0) or 0

    # ── S4優先チェック（S3が続いた後の急落 = 期待剥落確定）──────
    # S3が2ヶ月以上続いた後にピーク比-28%超かつRSI<47 → 期待崩壊
    if prev_stage in (3, 4) and s3_streak >= 2 and from_peak < -28 and rsi < 47:
        return 4
    # S4脱出（長期S4かつ実体が強い → バリュエーション訂正完了とみなしてS2へ）
    if prev_stage == 4 and s4_streak >= 6:
        rev_yoy_val = row.get("rev_yoy")
        ni_yoy_val  = row.get("ni_yoy")
        if rev_yoy_val is not None and float(rev_yoy_val) > 20:
            if ni_yoy_val is not None and float(ni_yoy_val) > 0:
                return 2
    # S4慣性（直前S4で下落継続中 → 期待はまだ戻っていない）
    if prev_stage == 4 and from_peak < -8 and ma200_dev < 30:
        return 4

    # ── S3: 陶酔期 ──────────────────────────────────────────
    # 【核心】期待が実体を大幅に超過している過熱状態
    # 条件A: MA200乖離が大きく上昇継続中
    if ma200_dev > 40:
        return 3
    if ma200_dev > 25 and rsi > 50:
        return 3
    # 条件B: MA200 > 12% かつ RSI高騰 かつ上昇モメンタム
    if ma200_dev > 12 and rsi > 58 and price_mom3m > 5:
        return 3
    if ma200_dev > 15 and rsi > 55:
        return 3
    # 条件C: バリュエーション過熱
    if forward_pe is not None and forward_pe > 60 and ma200_dev > 15:
        return 3
    if peg is not None and peg > 2.5 and ma200_dev > 10 and rsi > 55:
        return 3
    # 条件D: Buy比率が異常に高い + テクニカル過熱
    if buy_ratio is not None and buy_ratio > 0.8 and ma200_dev > 15:
        return 3
    # 条件E: S3慣性（MA200高水準で-25%未満かつMA200momが悪化していない）
    if prev_stage == 3 and ma200_dev > 20 and from_peak > -25 and ma200_mom > -10:
        return 3
    # 条件F: Z-scoreベース
    if e > 0.7 and m > 0.5:
        return 3

    # ── S4: 期待剥落期 ──────────────────────────────────────
    # 【核心】S3から転落開始、または下落継続中
    # 条件A: 良決算でも株価下落（S4の最強シグナル）
    if sell_on_good == 1 and from_peak < -5:
        return 4
    # 条件B: S3/S4からの転落（ピーク比-25%超かつMA200mom悪化）
    if prev_stage in (3, 4) and from_peak < -25 and ma200_mom < -10:
        return 4
    # 条件C: MA200乖離が急速に悪化
    if from_peak < -8 and rsi < 50 and ma200_mom < -10:
        return 4
    # 条件D: MA200割れ + 下落継続
    if ma200_dev < 0 and from_peak < -15 and price_mom3m < -3:
        return 4
    # 条件E: 大幅下落でMA200がまだプラスでも
    if from_peak < -40 and ma200_dev < 25:
        return 4

    # ── S0: 失望/蓄積期 ──────────────────────────────────────
    # 【核心】機関未参入・深い低迷・スマートマネー仕込み段階
    _is_s0 = (
        ma200_dev < -20
        or (short_pct is not None and short_pct > 0.08 and ma200_dev < -10)
        or (from_peak < -50 and ma200_dev < -15)
    )
    if _is_s0:
        # S0→S1昇格: 反発モメンタムが強ければS1に昇格
        # 浅い低迷（MA200 > -20%）: 3M+10%超で昇格
        # 深い低迷（MA200 <= -20%）: 3M+20%超の強烈反発のみ昇格
        if (ma200_dev > -20 and price_mom3m > 10) or (ma200_dev <= -20 and price_mom3m > 20):
            return 1
        return 0

    # ── S1: 期待覚醒期 ──────────────────────────────────────
    # 【核心】底打ち確認 + 新しいトリガー + 出来高急増
    # 条件A: MA200 < -10% から強い反発
    if ma200_dev < -10 and price_mom3m > 10:
        return 1
    # 条件B: MA200 < -5% から急反発
    if ma200_dev < -5 and price_mom3m > 20:
        return 1
    # 条件C: 出来高急増 + 低迷圏からの回復（強い上昇のみ）
    if vol_surge > 1.5 and ma200_dev < 0 and price_mom3m > 10:
        return 1
    # 条件D: EPSサプライズ + アナリスト上方修正開始（底値圏）
    if (eps_surprise is not None and eps_surprise > 10 and
            upgrade_rate is not None and upgrade_rate > 0.6 and
            ma200_dev < 5):
        return 1
    # 条件E: アナリストが強気転換し始め + 底値圏
    if rec_mean is not None and rec_mean < 2.0 and ma200_dev < -5 and price_mom3m > 0:
        return 1
    # 条件F: Z-scoreベース
    if e < -0.3 and m > 0.5:
        return 1

    # ── S2: 期待拡大期 ──────────────────────────────────────
    # 【核心】実体成長 + マルチプル拡大中。過熱でも低迷でもない上昇局面
    return 2



def _v(val):
    """pandas NaN を None に変換（NaN は is None で検出できないため）"""
    return None if pd.isna(val) else val


def compute_real_strong(row: pd.Series) -> bool:
    """実体（売上・EPS）の強さ判定。detect_substage()の内部フェーズ判定と
    JSON出力（{ticker}_poc.jsonのreal_strongフィールド）の双方から呼ばれる
    唯一の実装（[[HYPECORE-REALSTRONG-DUAL-IMPL-1]]対応: 修正前はdetail.html
    側`getRec()`が`(rev_yoy>30)&&(eps_surprise>0)`という別条件・別閾値の
    簡略版を独自に再実装しており、サーバー側substageとクライアント側の
    推奨表示が矛盾する組み合わせが起こり得た。JSON出力にreal_strongの
    値自体を含め、クライアント側の再計算を廃止する形で解消する）。
    """
    rev_yoy  = _v(row.get("rev_yoy"))
    eps_surp = _v(row.get("eps_surprise"))

    # 条件A: 標準（売上>15% かつ EPSが大きくミスしていない、またはEPS黒字サプライズ）
    _real_standard = (
        (rev_yoy is not None and rev_yoy > 15 and (eps_surp is None or eps_surp > -5)) or
        (eps_surp is not None and eps_surp > 0 and rev_yoy is not None and rev_yoy > 0)
    )
    # 条件B: 高成長グロース（売上>30%かつEPS大幅ミスでない）
    # 赤字成長企業でも売上が急拡大していれば「実体崩壊」とは言えない
    _real_growth = (
        rev_yoy is not None and rev_yoy > 30 and
        (eps_surp is None or eps_surp > -30)
    )
    return bool(_real_standard or _real_growth)


def detect_substage(row: pd.Series, stage: int, stage_months: int) -> dict:
    """
    各ステージの内部フェーズ（入口・中盤・出口）を判定。
    「今のステージはどこにいるか」「次に何が起きるか」を示す。
    """
    ma200  = row.get("ma200_dev_local", 0) or 0
    fp     = row.get("from_peak",   0) or 0
    rsi    = row.get("rsi",        50) or 50
    mom3m  = row.get("price_mom3m", 0) or 0
    ma200m = row.get("ma200_mom",   0) or 0

    rev_yoy   = _v(row.get("rev_yoy"))
    eps_surp  = _v(row.get("eps_surprise"))

    real_strong = compute_real_strong(row)

    if stage == 3:  # 陶酔期
        if ma200m < -5 and fp < -5:
            return dict(phase="出口", label="ピークアウト兆候",
                watch="高値から離れ始め、MA200乖離も縮小中。売りの準備タイミング。高値更新が止まっているか確認。",
                next="S4（期待剥落期）への移行に備える。ポジション縮小を開始。")
        if rsi < 40 and ma200 > 30:
            return dict(phase="出口", label="過熱感に陰り",
                watch="RSIが低下。モメンタムが弱まっている。出来高・高値更新の有無を確認。",
                next="出来高を伴う下落が出れば売りシグナル。")
        if ma200 > 50 and fp > -5:
            return dict(phase="中盤", label="過熱継続",
                watch="MA200乖離が高水準で推移。良ニュースへの株価反応が鈍くなっていないか。",
                next="良決算でも株価が反応しなくなったらS4転換のサイン。")
        if stage_months <= 3:
            return dict(phase="入口", label="陶酔期入り",
                watch="過熱が始まったばかり。まだ上昇余地がある可能性。",
                next="急いで売らない。MA200乖離がさらに拡大するか監視。")
        return dict(phase="中盤", label="陶酔継続",
            watch="期待過熱が続いている。ピークアウトのシグナルを待つ。",
            next="MA200乖離の縮小・RSI低下・高値更新停止が売りの合図。")

    elif stage == 4:  # 期待剥落期
        ma200_shrinking = ma200m > -5
        price_iv  = row.get("price_iv_ratio")
        forward_pe = row.get("forward_pe")

        # バリュエーション過熱チェック
        # 株価÷IV > 2.0 または FwdPE > 100 の場合は「底打ち兆候」に昇格しない
        # （実体が強くても期待がまだ過熱しており、さらなる訂正余地が大きい）
        valuation_overheat = (
            (price_iv is not None and price_iv > 2.0) or
            (forward_pe is not None and forward_pe > 100)
        )

        # 底打ち兆候：実体強い + MA200収縮鈍化 + RSI>40 + 過大下落していない
        bottoming = real_strong and ma200_shrinking and rsi > 40 and fp > -45

        if bottoming:
            if valuation_overheat:
                piv_str = f"株価÷IV={price_iv:.2f}x" if price_iv is not None else ""
                fpe_str = f"FwdPE={forward_pe:.0f}x" if forward_pe is not None else ""
                overheat_str = "、".join(filter(None, [piv_str, fpe_str]))
                return dict(phase="中盤B", label="実体維持・バリュエーション過熱",
                    watch=f"実体（売上・EPS）は強いが、{overheat_str}と割高感が残る。"
                           "期待の訂正がまだ終わっていない可能性。",
                    next="バリュエーションが正常化（FwdPE<60 または 株価÷IV<2.0）するまで様子見。"
                         "実体の強さだけで底打ちと判断するのは早計。")
            return dict(phase="出口", label="底打ち兆候",
                watch="実体（売上・EPS）は強く、MA200乖離の縮小が鈍化。期待と実体が近づいている。",
                next="打診買いの準備を始める。出来高増加を伴う株価反発で本格エントリー。")
        # 長期調整（S4が6ヶ月超かつ実体強い）：バリュエーション訂正が長引いている状態
        if stage_months >= 6 and real_strong:
            ni_yoy_val = _v(row.get("ni_yoy"))
            ni_str = f"・純利益{ni_yoy_val:+.1f}%" if ni_yoy_val is not None else ""
            rev_str = f"売上{rev_yoy:+.1f}%" if rev_yoy is not None else "売上データなし"
            return dict(phase="中盤B", label="長期調整・実体強",
                watch=f"S4が{stage_months}ヶ月継続。{rev_str}{ni_str}と実体は強い。"
                       "バリュエーション訂正が長引く典型的な長期調整局面。",
                next="実体の強さが続けばS2への回帰が近い。出来高を伴う反発・MA200復帰に注目。")
        if real_strong:
            return dict(phase="中盤B", label="実体維持・期待崩壊中",
                watch="売上・EPSは強い。期待の過熱訂正が続いているが、実体が下支え。",
                next="実体の強さが続けば底は近い。MA200乖離の縮小ペースを監視。")
        if stage_months <= 2:
            return dict(phase="入口", label="期待剥落開始",
                watch="S3から転落直後。Distribution（大口売り抜け）が始まっている可能性。",
                next="ポジション縮小推奨。実体（次の決算）が強ければ早期底打ちの可能性。")
        # EPSデータなし＋売上が強い場合は中盤Bに寄せる（誤判定防止）
        eps_missing = eps_surp is None
        if eps_missing and rev_yoy is not None and rev_yoy > 15:
            return dict(phase="中盤B*", label="実体維持の可能性（EPS要確認）",
                watch=f"売上成長{rev_yoy:+.1f}%は維持されているが、EPSデータが未取得。決算結果を別途確認推奨。",
                next="次の決算でEPS黒字なら底打ちの可能性。マイナスなら中盤A（実体崩壊）に移行。")
        # 中盤A: rev_yoyの水準でラベルを変える
        # rev_yoyがプラスなら「実体崩壊」とは言えず「軟化」が適切
        if rev_yoy is not None and rev_yoy > 5:
            # eps_surprise の実際の値に応じてテキストを分岐（固定「大幅ミス」を回避）
            if eps_surp is not None and eps_surp < -5:
                eps_text = f"EPSが予想比{eps_surp:+.1f}%の大幅ミスで期待の修正が続いている。"
            elif eps_surp is not None and eps_surp < 0:
                eps_text = f"EPSが予想比{eps_surp:+.1f}%とわずかにミスしている。"
            else:
                eps_text = "EPSは予想並みまたは超過。株価調整はバリュエーション面の見直し。"
            return dict(phase="中盤A", label="実体軟化・期待崩壊中",
                watch=f"売上成長{rev_yoy:+.1f}%はあるが、{eps_text}",
                next="次の決算でEPS改善が確認されれば「実体維持」に格上げ。悪化なら本格的な崩壊へ。")
        return dict(phase="中盤A", label="実体も崩壊中",
            watch="売上・EPSの悪化も確認される本格的な下落局面。",
            next="実体の回復（売上加速・EPSサプライズ）が出るまで売り継続。")

    elif stage == 2:  # 期待拡大期
        if ma200 > 20 and rsi > 65 and mom3m > 15:
            return dict(phase="出口", label="過熱の手前",
                watch="MA200乖離が拡大し過熱圏に近づいている。バリュエーションの拡大速度を確認。",
                next="S3（陶酔期）移行に備える。利益確定の準備を始める。")
        if stage_months <= 3:
            return dict(phase="入口", label="期待拡大開始",
                watch="期待と実体が噛み合い始めた段階。",
                next="売上成長の加速・アナリスト上方修正増加が続けばS3へ向かう。")
        return dict(phase="中盤", label="拡大継続",
            watch="健全な上昇局面。過熱サインを監視。",
            next="MA200乖離が20%超かつRSI65超で過熱の兆候。")

    elif stage == 1:  # 期待覚醒期
        if ma200 > -5 and rsi > 55 and mom3m > 5:
            return dict(phase="出口", label="S2移行の兆候",
                watch="底打ちが確認され、モメンタムが回復。",
                next="S2（期待拡大）への移行が近い。ポジション構築を検討。")
        return dict(phase="入口", label="覚醒初期",
            watch="本物の覚醒か、だましの反発かを見極める。出来高が鍵。",
            next="出来高を伴う連続上昇でS2への移行を確認。")

    else:  # S0 失望/蓄積期
        if mom3m > 10 and rsi > 40:
            return dict(phase="出口", label="物語の萌芽",
                watch="モメンタムが回復し始めた。スマートマネーが動いている可能性。",
                next="S1（期待覚醒）への移行を確認。出来高急増が確認されればエントリー検討。")
        return dict(phase="中盤", label="低迷継続",
            watch="底値形成中。まだ急いで動かない。",
            next="出来高を伴う株価反発・新しい物語（製品・契約・提携）の出現を待つ。")

def _safe_round(v):
    """floatに丸めてJSON出力可能な値へ変換（NaN/Inf/Noneはnullにする）

    NaN/Inf判定はfloat変換後に行う（[[HYPECORE-CI-SILENT-FAILURE-1]]:
    変換前にisinstance(v, float)で判定していたため、yfinance由来の文字列
    "Infinity"がfloat("Infinity")=infとして素通りし、ZETA_poc.jsonの
    trailing_peに非標準JSONのInfinityが混入した）。
    """
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    return None if (np.isnan(f) or np.isinf(f)) else round(f, 3)


def _build_month_record(idx, row) -> dict:
    """1ヶ月分の月次JSONレコードを構築する（run_poc()のJSON保存ループから抽出）。

    [[HYPECORE-MISC-NAMING-GAPS-1]]④: ma200_momはdetermine_stage()の複数の
    重要分岐（S3慣性・S4転落判定等）で使われるが、従来JSON出力に含まれて
    おらず判定根拠を事後検証できなかった。ここで出力に追加する。
    """
    safe = _safe_round
    return {
        "month":              idx.strftime("%Y-%m"),
        "price":              safe(row.get("price")),
        "stage":              int(row["stage"]),
        "stage_label":        STAGE_LABELS[int(row["stage"])],
        # テクニカル
        # [[HYPECORE-MISC-NAMING-GAPS-1]]（2026-08-29）: ma200_dev_local・
        # volume_ratio_20d・vol_surge_6m・ocf_yield_q・revenue_growth_yf・
        # buy_ratioは全て本項目のリネーム対応。詳細はBACKLOG_DONE.md参照。
        "ma200_dev_local":    safe(row.get("ma200_dev_local")),
        "ma200_mom":          safe(row.get("ma200_mom")),
        "ma50_dev":           safe(row.get("ma50_dev")),
        "from_peak":          safe(row.get("from_peak")),
        "rsi":                safe(row.get("rsi")),
        "volume_ratio_20d":   safe(row.get("volume_ratio_20d")),
        "vol_surge_6m":       safe(row.get("vol_surge_6m")),
        # 財務
        "rev_yoy":            safe(row.get("rev_yoy")),
        "ni_yoy":             safe(row.get("ni_yoy")),
        "rule40_yoy_netmargin": safe(row.get("rule40_yoy_netmargin")),
        "ocf_yield_q":        safe(row.get("ocf_yield_q")),
        # バリュエーション（現時点値）
        "forward_pe":         safe(row.get("forward_pe")),
        "trailing_pe":        safe(row.get("trailing_pe")),
        "peg_ratio":          safe(row.get("peg_ratio")),
        "psr":                safe(row.get("psr")),
        "revenue_growth_yf":  safe(row.get("revenue_growth_yf")),
        "earnings_growth":    safe(row.get("earnings_growth")),
        "recommendation_mean": safe(row.get("recommendation_mean")),
        "short_pct_float":    safe(row.get("short_pct_float")),
        # アナリスト・EPS
        "eps_surprise":       safe(row.get("eps_surprise")),
        "analyst_upgrade_rate": safe(row.get("analyst_upgrade_rate")),
        "analyst_downgrade_rate": safe(row.get("analyst_downgrade_rate")),
        "sell_on_good_news":  safe(row.get("sell_on_good_news")),
        "buy_ratio":          safe(row.get("buy_ratio")),
        # 内部フェーズ
        "substage_phase":     row["substage"]["phase"] if isinstance(row.get("substage"), dict) else None,
        "substage_label":     row["substage"]["label"] if isinstance(row.get("substage"), dict) else None,
        "substage_watch":     row["substage"]["watch"] if isinstance(row.get("substage"), dict) else None,
        "substage_next":      row["substage"]["next"]  if isinstance(row.get("substage"), dict) else None,
        # [[HYPECORE-REALSTRONG-DUAL-IMPL-1]]: サーバー側detect_substage()と
        # 同一ロジック（compute_real_strong()）の判定結果をそのまま出力する。
        # detail.html側は独自に再計算せずこの値を使う
        "real_strong":        bool(row.get("real_strong", False)),
        # スコア
        "expectation_score":  safe(row.get("expectation_score")),
        "fundamental_score":  safe(row.get("fundamental_score")),
        "momentum_score":     safe(row.get("momentum_score")),
        # IV
        "price_iv_ratio":     safe(row.get("price_iv_ratio")),
        # [[TANUKI-VALUATION-MISC-GAPS-1]]②: 上流のfetch_info_snapshot()で
        # 既にNone化済みだが、_build_month_record()単独でも安全な値になる
        # よう二重に適用する（防御的、rowの供給元が将来変わっても安全）。
        "ev_ebitda":          safe(_positive_or_none(row.get("ev_ebitda"))),
        # 低ベース効果: 前年rev_yoy<-10% かつ 今年rev_yoy>50%
        "low_base_effect":    bool(row.get("low_base_effect", False)),
    }


def run_poc(ticker: str = "PLTR") -> dict:
    """PoC実行"""
    print(f"\n{'='*55}")
    print(f"HypeCore PoC v2 - {ticker}")
    print(f"{'='*55}")

    df = compute_scores(ticker)

    # ステージ判定（慣性ルールのため順番に処理）
    stages = []
    prev = 2
    s3_streak = 0  # S3連続月数
    s4_streak = 0  # S4連続月数
    for _, row in df.iterrows():
        s = determine_stage(row, prev_stage=prev, s3_streak=s3_streak, s4_streak=s4_streak)
        stages.append(s)
        s3_streak = s3_streak + 1 if s == 3 else 0
        s4_streak = s4_streak + 1 if s == 4 else 0
        prev = s
    df["stage"] = stages
    df["stage_label"] = df["stage"].map(STAGE_LABELS)

    # 内部フェーズ判定
    substages = []
    real_strongs = []
    stage_months = 0
    prev_s = None
    for _, row in df.iterrows():
        s = int(row["stage"])
        if s != prev_s:
            stage_months = 1
        else:
            stage_months += 1
        substages.append(detect_substage(row, s, stage_months))
        real_strongs.append(compute_real_strong(row))
        prev_s = s
    df["substage"] = substages
    df["real_strong"] = real_strongs

    # 低ベース効果検出: 前年(12ヶ月前)のrev_yoyがマイナスかつ今年が50%超
    df["prev_rev_yoy"] = df["rev_yoy"].shift(12)
    df["low_base_effect"] = (
        df["prev_rev_yoy"].notna()
        & (df["prev_rev_yoy"] < -10)
        & (df["rev_yoy"].fillna(0) > 50)
    )

    df_out = df[df.index >= "2024-01-01"].copy()

    # PLTRのみ正解ラベルと比較
    if ticker == "PLTR":
        gt = PLTR_GROUND_TRUTH
        df_out["ground_truth"] = df_out.index.strftime("%Y-%m").map(gt)
        df_out["correct"] = df_out["stage"] == df_out["ground_truth"]
        accuracy = df_out["correct"].mean()
        print(f"\n【検証結果】正解率: {accuracy:.1%}")
        print(f"\n{'月':10s} {'予測':5s} {'正解':5s} {'一致':5s} {'MA200':8s} {'ピーク比':8s} {'RSI':5s} {'株価':8s}")
        print("-" * 65)
        for idx, row in df_out.iterrows():
            ym  = idx.strftime("%Y-%m")
            pred = int(row["stage"])
            g    = gt.get(ym, "-")
            ok   = "✅" if row.get("correct") else "❌"
            ma   = f"{row['ma200_dev_local']:+.1f}%" if not pd.isna(row.get("ma200_dev_local", float("nan"))) else "—"
            fp   = f"{row['from_peak']:+.1f}%"  if not pd.isna(row.get("from_peak", float("nan"))) else "—"
            rs   = f"{row['rsi']:.0f}"          if not pd.isna(row.get("rsi",       float("nan"))) else "—"
            p    = f"${row['price']:.1f}"
            print(f"{ym:10s} {pred!s:5s} {g!s:5s} {ok:5s} {ma:8s} {fp:8s} {rs:5s} {p:8s}")

    # JSON保存
    out = [_build_month_record(idx, row) for idx, row in df_out.iterrows()]

    JST = timezone(timedelta(hours=9))
    result = {
        "ticker":       ticker,
        "generated":    date.today().isoformat(),
        "generated_at": datetime.now(JST).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
        "monthly":      out,
    }
    out_path = _OUT_DIR / f"{ticker}_poc.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n保存完了: {out_path}")
    return result


def _filter_hypecore_tickers(target, hypecore_tickers):
    """CLI引数等でticker明示指定時にhypecore=trueのみへ絞り込む。

    FLAG-CONSUMER-AUDIT-3: --allはget_hypecore_tickers()経由で正しく
    フィルタされる一方、--batch/単体指定はCLI引数をそのまま処理しており
    hypecore=trueフラグの検証を一切行っていなかった
    （tanuki_valuation/pipeline.pyのCLI引数パスと同型のギャップ）。
    範囲外のticker指定は警告し無条件実行しない。
    """
    hypecore_set = set(hypecore_tickers)
    excluded = [t for t in target if t.upper() not in hypecore_set]
    if excluded:
        print(
            f"警告: hypecore=false のため除外: {', '.join(excluded)}"
            f"（cik_lookup.csvでhypecore=trueに変更しない限り処理されません）"
        )
    return [t for t in target if t.upper() in hypecore_set]


# [[HYPECORE-CI-SILENT-FAILURE-1]]: 失敗率がこれを超えたらexit 1。数銘柄の
# 通常のyfinance/データ欠損による失敗では落とさず、構造的な全面失敗のみを
# 検知する水準（全102銘柄なら21銘柄以上の失敗で発火）。
_MAX_FAILURE_RATIO = 0.2


def _fatal_reason(has_market_data: bool, n_tickers: int = 0, n_failed: int = 0,
                  max_ratio: float = _MAX_FAILURE_RATIO):
    """バッチ実行を致命的失敗（exit 1）とすべき理由を返す。問題なければNone。

    [[HYPECORE-CI-SILENT-FAILURE-1]]: market_data層が使えないと全銘柄が
    「株価データ取得失敗」になるにもかかわらずexit 0で終了し、CIは
    tickers.jsonのupdated_atだけをコミットし続けていた（2026-08-11〜09-21、
    HypeCore_Update.ymlのpyyaml未インストールが原因）。exit 1はCommit
    ステップより前（本スクリプト内）で発生するため、以降のコミットも止まる。
    """
    if not has_market_data:
        return (f"common.market_data.reader をimportできません"
                f"（{_MARKET_DATA_IMPORT_ERROR}）。全銘柄の株価・属性データが"
                f"取得できないため処理を中止します")
    if n_tickers and n_failed / n_tickers > max_ratio:
        return (f"失敗率 {n_failed}/{n_tickers} が閾値 {max_ratio:.0%} を"
                f"超えました")
    return None


def _save_tickers_index(docs_dir) -> None:
    """docs/value-monitor/hypecore/data/tickers.json を実データ基準で再生成する

    index.html はハードコードされたticker配列を持たず、このファイルを
    fetch して一覧表示するため、poc.json生成後は必ず本関数で最新化する
    （HYPECORE-TICKERS-INDEX-1: 新規銘柄登録時にindex.htmlへの反映漏れが
    発生していたため導入）。実際に *_poc.json が存在する銘柄のみを対象と
    し、cik_lookup.csvから除外された銘柄が一覧に残り続けることも防ぐ。
    """
    import json as _json
    from datetime import datetime as _datetime

    index_path = docs_dir / "tickers.json"
    tickers = sorted(p.stem[:-4] for p in docs_dir.glob("*_poc.json"))

    index_data = {
        "tickers": tickers,
        "updated_at": _datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(tickers),
    }

    with open(index_path, "w", encoding="utf-8") as f:
        _json.dump(index_data, f, ensure_ascii=False, indent=2)

    print(f"   📋 tickers.json 更新: {len(tickers)}銘柄")


if __name__ == "__main__":
    import sys
    import shutil

    _DOCS_DIR = _REPO_ROOT / "docs" / "value-monitor" / "hypecore" / "data"
    _DOCS_DIR.mkdir(parents=True, exist_ok=True)

    # cik_lookup.csv の hypecore=true 銘柄を動的に取得
    try:
        import sys as _sys
        _sys.path.insert(0, str(_REPO_ROOT / "common" / "sec_data"))
        from tickers import get_hypecore_tickers, get_registrable_tickers
        ALL_TICKERS = get_hypecore_tickers()
        # --batch/単体指定（明示ticker）の妥当性検証には
        # get_registrable_tickers()を使う（get_hypecore_tickers()とは
        # 異なりstatus=provisioningを除外しない）。新規銘柄登録
        # オーケストレーション（[[REGISTER-FLOW-REDESIGN-1]]方針3、
        # common/registration/register_ticker.py Step 5）が
        # provisioning中のティッカーを明示指定して実行できるようにする
        # ため（2026-09-03）。--all（ALL_TICKERS）は引き続きstatus
        # フィルタ済みのまま——スケジュール実行等が登録処理中の
        # ティッカーを誤って自動処理することはない。
        REGISTRABLE_TICKERS = get_registrable_tickers("hypecore")
        print(f"[INFO] hypecore対象: {len(ALL_TICKERS)}銘柄 (cik_lookup.csv)")
    except Exception as _e:
        print(f"[WARN] tickers.py読み込み失敗、フォールバックリストを使用: {_e}")
        ALL_TICKERS = [
            "AAPL","ALAB","AMAT","AMD","AMZN","APP","ASTS","AVAV","BBAI",
            "BSY","CAKE","CART","CEG","CELH","COHR","CRM","CRWV",
            "ELF","GOOGL","GTLB","IONQ","IOT","JOBY","KO","LITE","LLY","LMT",
            "META","MRVL","MSFT","NOW","NVDA","ONDS","PLTR","QBTS","RBRK","RCAT",
            "RDW","RKLB","RXRX","S","SITM","SOFI","SOUN","SPIR","TSLA",
            "VRT","ZETA",
        ]
        REGISTRABLE_TICKERS = ALL_TICKERS

    _fatal = _fatal_reason(HAS_MARKET_DATA)
    if _fatal:
        print(f"[FATAL] {_fatal}")
        sys.exit(1)

    args = sys.argv[1:]
    if not args:
        tickers = ["PLTR"]
    elif args[0] == "--all":
        tickers = ALL_TICKERS
    elif args[0] == "--batch":
        tickers = _filter_hypecore_tickers(args[1:], REGISTRABLE_TICKERS) if len(args) > 1 else ["PLTR"]
    else:
        tickers = _filter_hypecore_tickers([args[0]], REGISTRABLE_TICKERS)

    success, failed = [], []
    for t in tickers:
        try:
            run_poc(t)
            src = _OUT_DIR / f"{t}_poc.json"
            dst = _DOCS_DIR / f"{t}_poc.json"
            if src.exists():
                shutil.copy2(src, dst)
                print(f"  → docs にコピー完了: {dst.name}")
            success.append(t)
        except Exception as e:
            print(f"[ERROR] {t}: {e}")
            failed.append(t)

    print(f"\n{'='*50}")
    print(f"完了: {len(success)}銘柄 / 失敗: {len(failed)}銘柄")
    if failed:
        print(f"失敗銘柄: {', '.join(failed)}")
    if success:
        print(f"成功銘柄: {', '.join(success)}")

    _fatal = _fatal_reason(HAS_MARKET_DATA, len(tickers), len(failed))
    if _fatal:
        # tickers.jsonのupdated_atも更新しない（成功したように見せない）
        print(f"[FATAL] {_fatal}")
        sys.exit(1)

    _save_tickers_index(_DOCS_DIR)
