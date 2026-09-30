import os
import sys
import math
import urllib.request
import feedparser
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
import json
import csv
import re
import requests   # 追加: requests で非ASCII URLを扱う

# --- 設定 ---
XAI_API_KEY  = os.getenv("XAI_API_KEY")
GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
# RSSファイルはスクリプトと同じ scr/ ディレクトリに配置
RSS_LIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "02_market_rss.txt")

JST = timezone(timedelta(hours=9))

# データ保存先（GitHub Pages 配信対象の docs/market-monitor/market-pulse/data/）
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = os.path.join(REPO_ROOT, "docs", "market-monitor", "market-pulse", "data")
JSON_PATH = os.path.join(DATA_DIR, "market_data.json")
CSV_PATH = os.path.join(DATA_DIR, "market_data.csv")
BREADTH_JSON = os.path.join(DATA_DIR, "breadth_data.json")

# common/market_data - [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-6:
# 指数・ETF・商品先物のyfinance直接呼び出し（fetch_hist()の.history()・
# _get_sp500_ma_deviation()・fetch_qqq_tech_data()）をcommon.market_data.
# reader経由に切替。他の切替済みファイルと同じHAS_MARKET_DATAガード・
# 二段構えsys.path解決パターンを踏襲する（本ファイルは`python src/market/
# market_pulse/collect_and_send.py`で直接実行されるためリポジトリルートが
# sys.pathに含まれない。REPO_ROOTは上で既に計算済みのため流用する）。
HAS_MARKET_DATA = False
_md_get_price_series = None
_md_get_ma_deviation = None

try:
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    from common.market_data.reader import get_price_series as _md_get_price_series
    from common.market_data.reader import get_ma_deviation as _md_get_ma_deviation
    HAS_MARKET_DATA = True
except Exception:
    pass

if not HAS_MARKET_DATA:
    try:
        _github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if _github_workspace and _github_workspace not in sys.path:
            sys.path.insert(0, _github_workspace)
        from common.market_data.reader import get_price_series as _md_get_price_series
        from common.market_data.reader import get_ma_deviation as _md_get_ma_deviation
        HAS_MARKET_DATA = True
    except Exception:
        pass

# common/macro_data - [[MACRODATA-LAYER-CONSTRUCTION-1]]本番消費者切替
# （2026-08-12）: VXNCLS/BAMLH0A0HYM2/DGS3MOのfredapi直接呼び出し
# （fetch_vxn_from_fred/fetch_hy_spread_from_fred/fetch_fred_short_bond
# それぞれが個別にFred()を生成していた）をcommon.macro_data.reader経由に
# 切替。同じHAS_MACRO_DATAガード・二段構えsys.path解決パターンを踏襲
# （REPO_ROOTは上のmarket_dataブロックで既に計算済みのため流用）。
HAS_MACRO_DATA = False
_mdata_get_series = None

try:
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    from common.macro_data.reader import get_series as _mdata_get_series
    HAS_MACRO_DATA = True
except Exception:
    pass

if not HAS_MACRO_DATA:
    try:
        _github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if _github_workspace and _github_workspace not in sys.path:
            sys.path.insert(0, _github_workspace)
        from common.macro_data.reader import get_series as _mdata_get_series
        HAS_MACRO_DATA = True
    except Exception:
        pass

# CSVのカラム定義（必要に応じて拡張）
CSV_COLUMNS = [
    "date", "judgment",
    "VIX指数_value", "VIX指数_change", "VIX指数_change_percent", "VIX指数_volume_ratio",
    "VIX9D（短期VIX）_value", "VIX9D（短期VIX）_change_percent", "VIX9D対VIX比_value", "VIX9D対VIX比_contango",
    "日経平均_value", "日経平均_change", "日経平均_change_percent",
    "ドル円_value", "ドル円_change", "ドル円_change_percent", "ドル円_volume_ratio",
    "米10年債_value", "米10年債_change", "米10年債_change_percent", "米10年債_volume_ratio",
    "S&P500_value", "S&P500_change", "S&P500_change_percent", "S&P500_volume_ratio",
    # [[MARKETPULSE-MINOR-INCONSISTENCIES-1]]③対応: NASDAQ本体（main_tickersで
    # 取得・structured_dataには存在するがCSV_COLUMNS未登録のため無条件に
    # 欠落していた）
    "NASDAQ_value", "NASDAQ_change", "NASDAQ_change_percent", "NASDAQ_volume_ratio",
    "WTI原油_value", "WTI原油_change", "WTI原油_change_percent", "WTI原油_volume_ratio",
    "金（GOLD）_value", "金（GOLD）_change", "金（GOLD）_change_percent", "金（GOLD）_volume_ratio",
    "HYG（ハイイールド債ETF）_value", "HYG（ハイイールド債ETF）_change", "HYG（ハイイールド債ETF）_change_percent", "HYG（ハイイールド債ETF）_volume_ratio",
    "LQD（投資適格債ETF）_value", "LQD（投資適格債ETF）_change", "LQD（投資適格債ETF）_change_percent", "LQD（投資適格債ETF）_volume_ratio",
    "NYSE Composite_value", "NYSE Composite_change_percent", "NYSE Composite_volume_ratio", "NYSE Composite_divergence_vs_sp",
    "S&P500グロース(IVW)_value", "S&P500グロース(IVW)_change_percent",
    "S&P500バリュー(IVE)_value", "S&P500バリュー(IVE)_change_percent",
    "Russell2000小型(RUT)_value", "Russell2000小型(RUT)_change_percent",
    "グロース対バリュー比_diff_percent",
    "大型対小型比_diff_percent",
    "HYG対LQD比_value", "HYG対LQD比_change",
    "sentiment_score", "sentiment_label",
    "summary"
]


def _is_nan(x):
    """値がNaN（非数）かどうかを判定する。math.isnan()のラッパーで、
    None・文字列等の非数値はNaNではない（False）として扱う。
    yfinance等が返す欠損足はNaNとして混入するが、`x is not None`では
    検出できずすり抜けるため、抽出直後の検証に使う（MP-DATA-NULL-1）。"""
    try:
        return math.isnan(x)
    except TypeError:
        return False


def fetch_recent_records(ticker, count=2, days=5):
    """直近count件（既定2件）の実データ日次レコード（date/close/volume等、
    古い順）をリストで返す。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-6: yfinance直接呼び出し
    （旧fetch_hist()の.history()）をcommon.market_data.reader経由に切替
    （2026-08-11）。_gapプレースホルダー（営業日欠損）を除外した実データの
    末尾count件を使う（単発の営業日欠損に対する耐性を持たせる、
    collect.py::get_price_change()切替時と同型の防御的設計）。
    common.market_data未import環境（HAS_MARKET_DATA=False）・データ不足
    時は旧コードの「hist取得失敗」時と同じくNoneを返す（中立デフォルト）。
    """
    if not HAS_MARKET_DATA:
        return None
    try:
        series = _md_get_price_series(ticker, days=days)
        reals = [r for r in series if not r.get("_gap") and r.get("close") is not None]
        if len(reals) < count:
            return None
        return reals[-count:]
    except Exception as e:
        print(f"  [{ticker}] market_data取得失敗: {e}")
        return None


def format_line(name, records):
    """記録リスト（fetch_recent_records()の戻り値、date/close/volume・
    古い順）から表示用の1行を生成する。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-6: 引数をyfinance
    DataFrameからcommon.market_data.reader経由のレコードリストに変更
    （2026-08-11）。
    """
    if records is None:
        return f"● {name}: 取得制限あり\n"
    try:
        latest_rec = records[-1]
        latest = latest_rec.get("close")
        if latest is None or _is_nan(latest):
            return f"● {name}: 取得制限あり\n"
        last_date = datetime.strptime(latest_rec["date"], "%Y-%m-%d").strftime("%m/%d")
        diff, pct, vol_msg = 0.0, 0.0, ""
        if len(records) >= 2 and records[-2].get("close") is not None and not _is_nan(records[-2]["close"]):
            prev = records[-2]["close"]
            diff = latest - prev
            pct = (diff / prev) * 100 if prev else 0.0
            vol_latest = latest_rec.get("volume")
            vol_prev = records[-2].get("volume")
            if vol_latest is not None and vol_prev is not None and vol_latest > 0 and vol_prev > 0:
                vol_msg = f" | 前日比出来高比:{vol_latest / vol_prev:.2f}"
        return f"● {name}: {latest:.2f} [{diff:+.2f} ({pct:+.2f}%){vol_msg}] ({last_date} 確定)\n"
    except Exception as e:
        return f"● {name}: 解析エラー ({e})\n"


# ──────────────────────────────────────────────────────
# センチメントスコア算出（Phase 1: 5指標版）
# ──────────────────────────────────────────────────────
def zone_label(score):
    """0〜100のスコアのゾーン名（MP-03・MP-12、指示書㉓）。CNN Fear & Greedの区分
    （0-25 Extreme Fear / 25-45 Fear / 45-55 Neutral / 55-75 Greed / 75-100 Extreme Greed）に揃える。
    以前は20/35/50/65/80で、中立点50をまたいで「CAUTION」「NEUTRAL」に分かれていた。"""
    if score <= 25: return "EXTREME FEAR"
    if score <= 45: return "FEAR"
    if score <= 55: return "NEUTRAL"
    if score <= 75: return "GREED"
    return "EXTREME GREED"


def clamp01(v):
    """0.0〜1.0にクランプ"""
    return max(0.0, min(1.0, v))


def compute_sentiment(structured_data):
    """
    structured_data + breadth_data.json から8指標でセンチメントスコア(0-100)を算出。
    0=EXTREME FEAR, 50=NEUTRAL, 100=EXTREME GREED

    サブ指標 (Phase 3 — 8指標版、MP-BREADTH-2でrsp_spy_divergence追加。
    既存7指標のweightは元の値×0.9に圧縮し、新指標に10%を配分):
      1. VIX水準             (Weight 22.5%) — 12→100, 35→0
      2. S&P500 vs 50日MA乖離 (Weight 18.0%) — -8%→0, +8%→100
      3. AD Ratio (5日)       (Weight 13.5%) — 0.5→0, 2.0→100
      4. HYG/LQD比 変化方向   (Weight 10.8%) — 下落→0, 上昇→100
      5. NH-NL差分            (Weight  9.0%) — -50→0, +50→100
      6. グロース対バリュー比  (Weight  9.0%) — バリュー優勢→0, グロース優勢→100
      7. 出来高比(Distribution)(Weight  7.2%) — 出来高比>1.1+下落→0, 通常→100
      8. RSP/SPY乖離(20日平均) (Weight 10.0%) — -1.0pt→0(集中), +1.0pt→100(広範)
    """
    sub_scores = {}

    # breadth_data.json を読み込み
    breadth = _load_latest_breadth()

    # --- 1. VIX水準 (22.5%) --- ※VIX9D逆転（短期リスクオフ）で補正あり
    vix_data   = structured_data.get("VIX指数")
    vix9d_ratio = structured_data.get("VIX9D対VIX比")
    if vix_data and vix_data.get("value") is not None:
        vix = vix_data["value"]
        score = clamp01((35 - vix) / (35 - 12))
        # VIX9D逆転（VIX9D > VIX）= 短期リスクオフ準備 → -0.05補正
        if vix9d_ratio and vix9d_ratio.get("contango") is False:
            score = clamp01(score - 0.05)
        sub_scores["vix_level"] = {"score": score, "weight": 0.225, "raw": vix}
    else:
        sub_scores["vix_level"] = {"score": 0.5, "weight": 0.225, "raw": None}

    # --- 2. S&P500 vs 50日MA乖離率 (18.0%) ---
    sp500_ma_data = _get_sp500_ma_deviation()
    if sp500_ma_data is not None:
        sp500_ma_dev = sp500_ma_data["deviation_50"]
        score = clamp01((sp500_ma_dev + 8) / 16)
        sub_scores["sp500_ma_dev"] = {"score": score, "weight": 0.18, "raw": round(sp500_ma_dev, 2)}
    else:
        sp500_ma_dev = None
        sub_scores["sp500_ma_dev"] = {"score": 0.5, "weight": 0.18, "raw": None}

    # --- 3. AD Ratio 5日 (13.5%) ---
    if breadth and breadth.get("ad_ratio_5d") is not None:
        ad5 = breadth["ad_ratio_5d"]
        # 0.5→0(FEAR), 2.0→100(GREED) の線形補間
        score = clamp01((ad5 - 0.5) / (2.0 - 0.5))
        sub_scores["ad_ratio"] = {"score": score, "weight": 0.135, "raw": ad5}
    else:
        sub_scores["ad_ratio"] = {"score": 0.5, "weight": 0.135, "raw": None}

    # --- 4. HYG/LQD比 変化方向 (10.8%) ---
    hyg_lqd = structured_data.get("HYG対LQD比")
    if hyg_lqd and hyg_lqd.get("change") is not None:
        chg = hyg_lqd["change"]
        score = clamp01((chg + 0.005) / 0.01)
        sub_scores["hyg_lqd_dir"] = {"score": score, "weight": 0.108, "raw": round(chg, 6)}
    else:
        sub_scores["hyg_lqd_dir"] = {"score": 0.5, "weight": 0.108, "raw": None}

    # --- 5. NH-NL差分 (9.0%) ---
    if breadth and breadth.get("nh_nl_diff") is not None:
        nh_nl = breadth["nh_nl_diff"]
        # -50→0(FEAR), +50→100(GREED) の線形補間
        score = clamp01((nh_nl + 50) / 100)
        sub_scores["nh_nl"] = {"score": score, "weight": 0.09, "raw": nh_nl}
    else:
        sub_scores["nh_nl"] = {"score": 0.5, "weight": 0.09, "raw": None}

    # --- 6. グロース対バリュー比 (9.0%) ---
    gv = structured_data.get("グロース対バリュー比")
    if gv and gv.get("diff_percent") is not None:
        diff = gv["diff_percent"]
        score = clamp01((diff + 3) / 6)
        sub_scores["growth_value"] = {"score": score, "weight": 0.09, "raw": round(diff, 2)}
    else:
        sub_scores["growth_value"] = {"score": 0.5, "weight": 0.09, "raw": None}

    # --- 7. Distribution判定 (7.2%) ---
    sp_data = structured_data.get("S&P500")
    if sp_data and sp_data.get("volume_ratio") is not None and sp_data.get("change_percent") is not None:
        vol_ratio = sp_data["volume_ratio"]
        chg_pct = sp_data["change_percent"]
        if vol_ratio > 1.1 and chg_pct < -0.3:
            score = 0.0  # Distribution
        elif vol_ratio > 1.1 and chg_pct > 0.3:
            score = 1.0  # Accumulation
        else:
            score = 0.5  # Neutral
        sub_scores["distribution"] = {"score": score, "weight": 0.072, "raw": {"vol_ratio": vol_ratio, "chg_pct": chg_pct}}
    else:
        sub_scores["distribution"] = {"score": 0.5, "weight": 0.072, "raw": None}

    # --- 8. RSP/SPY乖離 20日平均 (10.0%、MP-BREADTH-2) ---
    # プラス=RSP(均等加重)優勢=広範な上昇　マイナス=SPY(時価総額加重)のみ上昇=二極化
    if breadth and breadth.get("rsp_spy_divergence_20d_avg") is not None:
        div20 = breadth["rsp_spy_divergence_20d_avg"]
        # -1.0pt→0(集中/FEAR), +1.0pt→100(広範/GREED) の線形補間
        score = clamp01((div20 + 1.0) / 2.0)
        sub_scores["rsp_spy_divergence"] = {"score": score, "weight": 0.10, "raw": div20}
    else:
        sub_scores["rsp_spy_divergence"] = {"score": 0.5, "weight": 0.10, "raw": None}

    # --- 加重平均 ---
    total_score = sum(s["score"] * s["weight"] for s in sub_scores.values())
    total_weight = sum(s["weight"] for s in sub_scores.values())
    final_score = round((total_score / total_weight) * 100, 1) if total_weight > 0 else 50.0

    # ラベル判定（MP-03、指示書㉓: CNN F&Gの区分〈25/45/55/75〉に揃える）
    label = zone_label(final_score)

    # サブスコアを100点満点に変換して記録
    sub_detail = {}
    for k, v in sub_scores.items():
        sub_detail[k] = {
            "score": round(v["score"] * 100, 1),
            "weight": v["weight"],
            "raw": v["raw"]
        }

    # breadthデータもsentimentに含める（フロントエンド表示用）
    breadth_summary = None
    if breadth:
        breadth_summary = {
            "advances": breadth.get("advances"),
            "declines": breadth.get("declines"),
            # [[MARKETPULSE-MINOR-INCONSISTENCIES-1]]④対応: breadth_data.json
            # には存在するがパススルー対象から漏れていた5フィールドを追加
            "unchanged": breadth.get("unchanged"),
            "ad_ratio_1d": breadth.get("ad_ratio_1d"),
            "ad_ratio_5d": breadth.get("ad_ratio_5d"),
            "new_highs_52w": breadth.get("new_highs_52w"),
            "new_lows_52w": breadth.get("new_lows_52w"),
            "nh_nl_diff": breadth.get("nh_nl_diff"),
            "total_stocks": breadth.get("total_stocks"),
            "pct_above_50ma": breadth.get("pct_above_50ma"),
            "pct_above_200ma": breadth.get("pct_above_200ma"),
            "rsp_return_1d": breadth.get("rsp_return_1d"),
            "spy_return_1d": breadth.get("spy_return_1d"),
            "rsp_spy_divergence_1d": breadth.get("rsp_spy_divergence_1d"),
            "rsp_spy_divergence_20d_avg": breadth.get("rsp_spy_divergence_20d_avg"),
            "ad_line": breadth.get("ad_line"),
            "mcclellan_oscillator": breadth.get("mcclellan_oscillator"),
            # 同じ基準日の銘柄だけで集計した結果、除外した銘柄数と5日騰落比の対象数
            # （MARKETPULSE-BREADTH-MIXED-DATES-1、2026-09-26）
            "stocks_excluded_date_mismatch": breadth.get("stocks_excluded_date_mismatch"),
            "stocks_counted_5d": breadth.get("stocks_counted_5d"),
            "date": breadth.get("date"),
        }

    return {
        "score": final_score,
        "label": label,
        "sub_scores": sub_detail,
        "breadth": breadth_summary,
    }


def _load_latest_breadth():
    """breadth_data.json から最新のブレッスデータを読み込む"""
    if not os.path.exists(BREADTH_JSON):
        print("[INFO] breadth_data.json が見つかりません（breadth_calculator.py 未実行）")
        return None
    try:
        with open(BREADTH_JSON, 'r', encoding='utf-8') as f:
            all_breadth = json.load(f)
        if not all_breadth:
            return None
        # 最新のエントリを返す（日付順ソート済み前提）
        latest = all_breadth[-1]
        print(f"[INFO] ブレッスデータ読み込み: {latest.get('date')} "
              f"ADV={latest.get('advances')} DEC={latest.get('declines')} "
              f"NH={latest.get('new_highs_52w')} NL={latest.get('new_lows_52w')}")
        return latest
    except Exception as e:
        print(f"[WARN] breadth_data.json読み込み失敗: {e}")
        return None


def _get_sp500_ma_deviation():
    """S&P500の現在値と50日/200日移動平均の情報を返す
    Returns dict with keys: deviation_50, above_ma200, ma200_slope

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-6: yfinance直接呼び出し
    （.history(period="1y")）をcommon.market_data.reader経由に切替
    （2026-08-11）。deviation_50はget_ma_deviation(window=50)の乖離率式
    （(latest-ma)/ma*100）と完全一致するためそのまま流用。above_ma200は
    get_ma_deviation(window=200)の符号（乖離率>0 ⟺ 終値>MA200）から導出。
    ma200_slope（10営業日前時点のMA200との比較）はget_ma_deviation()が
    最新1点のみ返す設計のため代替不可、get_price_series()で取得した生の
    daily/系列からdaily/層内で自前rolling計算する（hypecore.py事前調査で
    確立した「daily/層内での自前計算はreader.pyの層またぎ禁止ルールの
    例外規定に整合する」という設計方針を踏襲）。
    """
    if not HAS_MARKET_DATA:
        return None
    try:
        deviation_50 = _md_get_ma_deviation("^GSPC", window=50)
        deviation_200 = _md_get_ma_deviation("^GSPC", window=200)
        if deviation_50 is None or deviation_200 is None:
            return None
        above_ma200 = deviation_200 > 0
        # MA200傾き: 直近MA200 vs 10営業日前のMA200（daily/層内の自前計算）
        series = _md_get_price_series("^GSPC", days=220)
        closes = [r["close"] for r in series if not r.get("_gap") and r.get("close") is not None]
        if len(closes) >= 210:
            ma200_now = sum(closes[-200:]) / 200
            ma200_10d_ago = sum(closes[-210:-10]) / 200
            ma200_slope = bool(ma200_now > ma200_10d_ago)
        else:
            ma200_slope = None
        print(f"[INFO] S&P500: MA50乖離={deviation_50:+.2f}%, above_MA200={above_ma200}, MA200傾き={'↑' if ma200_slope else '↓' if ma200_slope is not None else '—'}")
        return {
            "deviation_50": round(deviation_50, 2),
            "above_ma200": above_ma200,
            "ma200_slope": ma200_slope,
        }
    except Exception as e:
        print(f"[WARN] S&P500 MA乖離率の取得失敗: {e}")
        return None


# ──────────────────────────────────────────────────────
# Tech Pulse — QQQ/VXN/F&Gベースのナスダック感情指数
# ──────────────────────────────────────────────────────
def fetch_qqq_tech_data():
    """QQQのMA125乖離率とQQQ/SPY 20日相対強度を返す（%表示）

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-6: yfinance直接呼び出し
    （.history(period="200d")）をcommon.market_data.reader経由に切替
    （2026-08-11）。qqq_vs_ma125はget_ma_deviation(window=125)の乖離率式
    と完全一致するためそのまま流用。qqq_vs_spy_20dは21営業日前時点との
    単純比較のためget_price_series()の生系列（series[-21] vs series[-1]）
    で算出する。daily/層は常に確定済み前日終値のみを保持する設計のため、
    旧コードの「市場開場前・開場中の当日データ除外フィルタ」は不要になり
    削除した（寄り付き前の未確定データが混入する余地がそもそもない）。
    """
    if not HAS_MARKET_DATA:
        print("[WARN] common.market_data未import。Tech Pulseスキップ。")
        return None, None
    try:
        qqq_vs_ma125 = _md_get_ma_deviation("QQQ", window=125)
        if qqq_vs_ma125 is None:
            print("[WARN] QQQデータ不足。Tech Pulseスキップ。")
            return None, None
        qqq_vs_ma125 = round(qqq_vs_ma125, 2)

        qqq_vs_spy_20d = None
        qqq_series = _md_get_price_series("QQQ", days=25)
        spy_series = _md_get_price_series("SPY", days=25)
        qqq_reals = [r for r in qqq_series if not r.get("_gap") and r.get("close") is not None]
        spy_reals = [r for r in spy_series if not r.get("_gap") and r.get("close") is not None]
        if len(qqq_reals) >= 21 and len(spy_reals) >= 21:
            qqq_ret = (qqq_reals[-1]["close"] / qqq_reals[-21]["close"] - 1) * 100
            spy_ret = (spy_reals[-1]["close"] / spy_reals[-21]["close"] - 1) * 100
            # 単純差分（%pt）: QQQ超過リターン。旧式の比率計算は spy_ret≈0 で発散するため廃止
            qqq_vs_spy_20d = round(qqq_ret - spy_ret, 2)
        print(f"[INFO] QQQ: vs_MA125={qqq_vs_ma125:+.2f}%, vs_SPY_20d={qqq_vs_spy_20d}")
        return qqq_vs_ma125, qqq_vs_spy_20d
    except Exception as e:
        print(f"[WARN] QQQデータ取得失敗: {e}")
        return None, None


def fetch_vxn_from_fred():
    """common.macro_data.reader経由でVXNCLS（ナスダック恐怖指数）を
    取得しMA50乖離率（%）を返す（MACRODATA-LAYER-CONSTRUCTION-1本番
    消費者切替、2026-08-12。旧実装はfredapi直接呼び出し・都度Fred()
    生成だった）。"""
    if not HAS_MACRO_DATA:
        print("[WARN] common.macro_data.reader が利用できません。VXN取得スキップ。")
        return None, None
    try:
        start = (datetime.now() - timedelta(days=120)).strftime('%Y-%m-%d')
        records = _mdata_get_series("VXNCLS", start=start)
        vxn_values = [float(r["value"]) for r in records if r.get("value") is not None]
        if len(vxn_values) < 50:
            print("[WARN] VXNデータが不足しています。")
            return None, None
        vxn_latest = vxn_values[-1]
        ma50 = sum(vxn_values[-50:]) / 50
        vxn_vs_ma50 = round((vxn_latest / ma50 - 1) * 100, 2)
        print(f"[INFO] VXN: {vxn_latest:.2f}, MA50={ma50:.2f}, vs_MA50={vxn_vs_ma50:+.2f}%")
        return round(vxn_latest, 2), vxn_vs_ma50
    except Exception as e:
        print(f"[WARN] VXN取得失敗: {e}")
        return None, None


def fetch_hy_spread_from_fred():
    """common.macro_data.reader経由でHYスプレッド（BAMLH0A0HYM2: ICE
    BofA US High Yield Index OAS）を取得する（MACRODATA-LAYER-
    CONSTRUCTION-1本番消費者切替、2026-08-12。旧実装はfredapi直接
    呼び出し・都度Fred()生成だった）。
    Returns: {"current", "min_90d", "max_90d", "is_expanding", "is_contracting"} or None
    """
    if not HAS_MACRO_DATA:
        print("[WARN] common.macro_data.reader が利用できません。HYスプレッド取得スキップ。")
        return None
    try:
        start = (datetime.now() - timedelta(days=120)).strftime('%Y-%m-%d')
        records = _mdata_get_series("BAMLH0A0HYM2", start=start)
        hy_values = [float(r["value"]) for r in records if r.get("value") is not None]
        if len(hy_values) < 10:
            print("[WARN] HYスプレッドデータが不足しています。")
            return None
        window = hy_values[-90:] if len(hy_values) >= 90 else hy_values
        current = hy_values[-1]
        min_90d = min(window)
        max_90d = max(window)
        is_expanding = bool(current > min_90d + 0.30)    # 90日最小値から30bps以上拡大
        is_contracting = bool(current < max_90d - 0.30)  # 90日最大値から30bps以上縮小
        print(f"[INFO] HYスプレッド: {current:.2f}%, min_90d={min_90d:.2f}%, max_90d={max_90d:.2f}%, expanding={is_expanding}, contracting={is_contracting}")
        return {
            "current": round(current, 2),
            "min_90d": round(min_90d, 2),
            "max_90d": round(max_90d, 2),
            "is_expanding": is_expanding,
            "is_contracting": is_contracting,
        }
    except Exception as e:
        print(f"[WARN] HYスプレッド取得失敗: {e}")
        return None


def fetch_fg_score_from_feargreedchart():
    """feargreedchart.com APIからF&Gスコア（0〜100）を取得する"""
    try:
        resp = requests.get(
            "https://feargreedchart.com/api/?action=all",
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        score = float(resp.json()["score"]["score"])
        print(f"[INFO] feargreedchart.com F&G score: {score}")
        return score
    except Exception as e:
        print(f"[WARN] feargreedchart.com F&G取得失敗: {e}")
        return None


def _load_tech_pulse_history(json_path, window=90):
    """market_data.jsonから過去window日分のTech Pulse指標リストを返す"""
    hist = {"qqq_vs_ma125": [], "vxn_vs_ma50": [], "qqq_vs_spy_20d": []}
    if not os.path.exists(json_path):
        return hist
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            all_data = json.load(f)
    except Exception:
        return hist
    cutoff = datetime.now(timezone.utc) - timedelta(days=window)
    for entry in all_data:
        try:
            d = datetime.fromisoformat(entry.get("date", ""))
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if d < cutoff:
            continue
        comp = ((entry.get("tech_pulse") or {}).get("components") or {})
        for key in hist:
            v = comp.get(key)
            if v is not None:
                hist[key].append(float(v))
    # qqq_vs_spy_20dは旧比率式の外れ値（±50超）を除外して分布を保護
    hist["qqq_vs_spy_20d"] = [v for v in hist["qqq_vs_spy_20d"] if abs(v) <= 50]
    return hist


def _load_prev_tech_pulse_score(json_path):
    """market_data.jsonから直近のTech Pulseスコアを返す（なければNone）"""
    if not os.path.exists(json_path):
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            all_data = json.load(f)
        for entry in reversed(all_data):
            score = (entry.get("tech_pulse") or {}).get("score")
            if score is not None:
                return float(score)
    except Exception:
        pass
    return None


FALLBACK_LOOKBACK_ENTRIES = 5  # この件数遡っても本物の値が見つからなければnullのまま据え置く（無限に古いデータを引きずらないための上限）


def _load_recent_entries(json_path=JSON_PATH, limit=FALLBACK_LOOKBACK_ENTRIES):
    """market_data.jsonの直近エントリを新しい順に最大limit件返す（取得失敗時のフォールバック値探索用）"""
    if not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            all_data = json.load(f)
        if not isinstance(all_data, list):
            return []
        return list(reversed(all_data[-limit:]))
    except Exception as e:
        print(f"[WARN] フォールバック探索用の既存データ読込失敗: {e}")
        return []


def _is_real_value(item):
    """
    item（indicators/asset_flowの1エントリ）が「本物の値」を持つか判定する。
    is_fallbackタグ付きは除外する。また、value/change_pctフィールドを持つ場合に
    そのフィールド自体がNoneなら本物とみなさない（コンテナのdict自体は存在するが
    中の値だけNoneという、MP-DATA-NULL-1のNaN→null置換で生じた混入データを
    誤って「正常値」と判定しないための防御。詳細はBACKLOG_DONE.md参照）。
    """
    if not item or item.get("is_fallback"):
        return False
    if "value" in item and item["value"] is None:
        return False
    if "change_pct" in item and item["change_pct"] is None:
        return False
    return True


def _fill_fallbacks(current_dict, container_key, recent_entries):
    """
    取得失敗（値がNone、または値はあるがフィールド自体がNoneの混入データ）になっている
    キーを、直近エントリ（新しい順）の中から最初に見つかった「本物の値」
    （_is_real_value参照）で補完する（MP-FALLBACK-DISPLAY-1）。
    フォールバックの連鎖（フォールバック値をさらにフォールバック元にする）を避けるため、
    過去に is_fallback=true で補完されたエントリはスキップしてさらに遡る。
    recent_entries は FALLBACK_LOOKBACK_ENTRIES 件に制限されているため、それでも
    本物の値が見つからない場合はNoneのまま据え置く（「—」表示は変更しない）。
    補完した値には is_fallback=true を付与する（元データはコピーして変更しない）。
    """
    filled = dict(current_dict)
    for key, val in current_dict.items():
        if _is_real_value(val):
            continue
        found = None
        for entry in recent_entries:
            container = entry.get(container_key) or {}
            prev_val = container.get(key)
            if _is_real_value(prev_val):
                found = prev_val
                break
        if found:
            copied = dict(found)
            copied["is_fallback"] = True
            filled[key] = copied
            print(f"[WARN] {container_key}.{key}: 取得失敗のため前回値(date={copied.get('date')})で補完 [is_fallback=true]")
        else:
            print(f"[WARN] {container_key}.{key}: 直近{len(recent_entries)}件以内に本物の値が見つからずnullのまま")
    return filled


def _load_div_history(json_path, window=90):
    """過去window日分の乖離値（TP score − CNN F&G）リストを返す。
    保存済み divergence.value を優先使用し、ない場合は fear_greed.score
    （CNN、当日のdiv_value算出と同一ソース）で代替計算する。
    どちらの経路もCNN F&Gベースで一貫性が保たれる。
    """
    if not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            all_data = json.load(f)
    except Exception:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=window)
    result = []
    for entry in all_data:
        try:
            d = datetime.fromisoformat(entry.get("date", ""))
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if d < cutoff:
            continue
        div_stored = ((entry.get("tech_pulse") or {}).get("divergence") or {}).get("value")
        if div_stored is not None:
            result.append(float(div_stored))
            continue
        # [[MARKETPULSE-MINOR-INCONSISTENCIES-1]]⑤対応: 旧エントリ
        # （divergence.value 未記録）は fear_greed.score（CNN、当日の
        # div_value算出と同一ソース）で再計算する。旧実装は
        # tech_pulse.components.fg_score（feargreedchart.com、別ソース）
        # を参照しておりdocstringの「CNN F&Gベースで一貫性が保たれる」
        # という主張と矛盾していた（2026-08-26②で発見・修正）。
        tp_s = (entry.get("tech_pulse") or {}).get("score")
        fg_s = (entry.get("fear_greed") or {}).get("score")
        if tp_s is not None and fg_s is not None:
            result.append(float(tp_s) - float(fg_s))
    return result


def _calc_divergence_zscore(div_history):
    """乖離値リストの最後の値のZスコアを返す（履歴不足の場合はNone）"""
    import statistics as _stats
    if len(div_history) < 5:
        return None
    mean = _stats.mean(div_history)
    stdev = _stats.stdev(div_history)
    if stdev == 0:
        return 0.0
    return round((div_history[-1] - mean) / stdev, 2)


def _get_tp_signal(div_value, div_zscore, fg_score):
    """3条件シグナル文字列を返す"""
    if div_value is None or div_zscore is None or fg_score is None:
        return ""
    if fg_score < 30 and div_value > 10 and div_zscore > 1.0:
        return "ハイテク先行反発シグナル"
    if div_value < -10 and div_zscore < -1.0:
        return "ハイテク先行下落注意"
    return ""


def _tp_label(score):
    return zone_label(score)


def calc_tech_pulse_score(qqq_vs_ma125, vxn_vs_ma50, qqq_vs_spy_20d, history_90d):
    """Tech Pulseスコアを算出する（0〜100）— 過去90日パーセンタイル正規化"""
    try:
        from scipy.stats import percentileofscore
    except ImportError:
        print("[WARN] scipy未インストール。Tech Pulse固定50を返します。")
        return 50
    percentiles = []
    hist = history_90d or {}
    if qqq_vs_ma125 is not None and hist.get("qqq_vs_ma125"):
        percentiles.append(percentileofscore(hist["qqq_vs_ma125"], qqq_vs_ma125))
    if vxn_vs_ma50 is not None and hist.get("vxn_vs_ma50"):
        percentiles.append(100 - percentileofscore(hist["vxn_vs_ma50"], vxn_vs_ma50))
    # 最低5件の履歴がないとパーセンタイルが不安定なため閾値を設ける
    if qqq_vs_spy_20d is not None and len(hist.get("qqq_vs_spy_20d", [])) >= 5:
        percentiles.append(percentileofscore(hist["qqq_vs_spy_20d"], qqq_vs_spy_20d))
    if not percentiles:
        return 50
    score = max(0, min(100, round(sum(percentiles) / len(percentiles))))
    if vxn_vs_ma50 is None:
        capped = min(score, 75)
        if capped < score:
            print(f"[WARN] VXN欠落のためスコアをキャップ（上限75）: {score} → {capped}")
        score = capped
    return score


def _aligned_pair_closes(sym_a, sym_b, days=10):
    """2銘柄の終値がそろっている最新の共通日と、その直前の営業日（両方の終値あり）の
    ((前日, a, b), (当日, a, b))を返す。そろわない場合はNone
    （[[MARKETPULSE-HYG-LQD-DATE-MIX-1]]、2026-09-26）。"""
    if not HAS_MARKET_DATA:
        return None
    try:
        series_a = _md_get_price_series(sym_a, days=days)
        series_b = _md_get_price_series(sym_b, days=days)
    except Exception:
        return None
    sa = {r["date"]: r["close"] for r in series_a
          if not r.get("_gap") and r.get("close") is not None and not _is_nan(r["close"])}
    sb = {r["date"]: r["close"] for r in series_b
          if not r.get("_gap") and r.get("close") is not None and not _is_nan(r["close"])}
    trading = sorted({r["date"] for r in series_a} | {r["date"] for r in series_b})
    common = [d for d in trading if d in sa and d in sb]
    if len(common) < 2:
        return None
    d_now = common[-1]
    idx = trading.index(d_now)
    if idx == 0 or trading[idx - 1] not in common:
        return None  # 直前の営業日に両方の終値がない（2営業日分の変化になるため使わない）
    d_prev = trading[idx - 1]
    return (d_prev, sa[d_prev], sb[d_prev]), (d_now, sa[d_now], sb[d_now])


def _mark_provisional_item(item, record):
    """daily/の行が暫定（日足の確定前に保存、`_provisional`、[[MARKETDATA-DAILY-PROVISIONAL-ROWS-1]]）なら、
    その値に`provisional: True`を付ける（指示書㉕ STEP B: data_qualityをpartialにする）。"""
    if record.get("_provisional"):
        item["provisional"] = True
    return item


def apply_same_contract_change(item, prev, last):
    """[[MARKETDATA-FUTURES-ROLL-1]]（2026-09-30）: 先物（CL=F・GC=F）の限月が前の行から変わった日（乗り換え日）は、
    新しい限月自身の前営業日の終値と比べた前日比にする（daily/の行のcontract・contract_close・contract_prev_close）。
    新しい限月の前営業日の終値が無い場合は前日比を出さず（None）、「限月乗り換え日」として段階1・2の判定から外す。
    推測では補わない。乗り換え日でない日と、限月の記録が無い行（2026-09-30より前）は通常の前日比のまま。"""
    c, pc = last.get("contract"), prev.get("contract")
    if not c:
        return item
    item["contract"] = c
    if not pc or pc == c:
        return item
    roll = {"from": pc, "to": c, "raw_change_percent": item.get("change_percent")}
    cc, cp = last.get("contract_close"), last.get("contract_prev_close")
    if cc and cp:
        roll.update({"method": "same_contract", "prev_date": last.get("contract_prev_date"), "prev_close": cp, "close": cc})
        item["change"] = round(cc - cp, 2)
        item["change_percent"] = round((cc - cp) / cp * 100, 2)
    else:
        roll["method"] = "excluded"
        item["change"] = None
        item["change_percent"] = None
    item["contract_roll"] = roll
    return item


def get_realtime_data():
    """表示用テキストと構造化データを返す"""
    summary = ""
    data = {}

    # 主要指標
    main_tickers = {
        "米10年債": "^TNX",
        "VIX指数": "^VIX",
        "VIX9D（短期VIX）": "^VIX9D",
        "ドル円": "JPY=X",
        "日経平均": "^N225",
        "S&P500": "^GSPC",
        "NASDAQ": "^IXIC",
        "WTI原油": "CL=F",
        "金（GOLD）": "GC=F",
    }
    for name, ticker in main_tickers.items():
        records = fetch_recent_records(ticker)
        summary += format_line(name, records)
        if (records is not None
                and records[-1].get("close") is not None and records[-2].get("close") is not None
                and not _is_nan(records[-1]["close"]) and not _is_nan(records[-2]["close"])):
            latest = records[-1]["close"]
            prev = records[-2]["close"]
            change = latest - prev
            change_percent = (change / prev) * 100 if prev else 0.0
            vol_latest = records[-1].get("volume")
            vol_prev = records[-2].get("volume")
            volume_ratio = vol_latest / vol_prev if (vol_latest is not None and vol_prev) else None
            data[name] = {
                "value": round(latest, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 2),
                "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
                "date": records[-1]["date"]
            }
            _mark_provisional_item(data[name], records[-1])
            if name == "米10年債":
                # MP-25（指示書㉓）: 利回りの変化はbpで表示する（^TNXは利回り〈%〉そのもの）
                data[name]["change_bp"] = round(change * 100, 1)
            if name in ("WTI原油", "金（GOLD）"):
                apply_same_contract_change(data[name], records[-2], records[-1])
        else:
            data[name] = None

    # NYSE Composite
    summary += "\n--- NYSE騰落統計（代替指標） ---\n"
    nya_records = fetch_recent_records("^NYA")
    sp_records = fetch_recent_records("^GSPC")
    nya_data = None
    if (nya_records is not None
            and nya_records[-1].get("close") is not None and nya_records[-2].get("close") is not None
            and not _is_nan(nya_records[-1]["close"]) and not _is_nan(nya_records[-2]["close"])):
        nya_latest = nya_records[-1]["close"]
        nya_prev = nya_records[-2]["close"]
        nya_pct = (nya_latest - nya_prev) / nya_prev * 100
        vol_latest = nya_records[-1].get("volume")
        vol_prev = nya_records[-2].get("volume")
        vol_ratio = vol_latest / vol_prev if (vol_latest is not None and vol_prev) else None
        vol_ratio_str = f"{vol_ratio:.2f}" if vol_ratio is not None else "N/A"
        last_date = datetime.strptime(nya_records[-1]["date"], "%Y-%m-%d").strftime("%m/%d")
        summary += f"● NYSE Composite(^NYA): {nya_latest:.2f} [{nya_pct:+.2f}%] | 前日比出来高比:{vol_ratio_str} ({last_date} 確定)\n"
        sp_valid = (sp_records is not None
                    and sp_records[-1].get("close") is not None and sp_records[-2].get("close") is not None
                    and not _is_nan(sp_records[-1]["close"]) and not _is_nan(sp_records[-2]["close"]))
        if sp_valid:
            sp_pct = (sp_records[-1]["close"] - sp_records[-2]["close"]) / sp_records[-2]["close"] * 100
            divergence = nya_pct - sp_pct
            summary += f"● NYA対S&P500乖離（騰落代理）: {divergence:+.2f}%pt"
            if divergence < -0.5:
                summary += " → 中小型株が大型株を下回る＝市場内部の弱さ\n"
            elif divergence > 0.5:
                summary += " → 中小型株が大型株を上回る＝市場の広がり確認\n"
            else:
                summary += " → 概ね連動\n"
        nya_data = {
            "value": round(nya_latest, 2),
            "change_percent": round(nya_pct, 2),
            "volume_ratio": round(vol_ratio, 2) if vol_ratio is not None else None,
            "date": nya_records[-1]["date"]
        }
        if sp_valid:
            nya_data["divergence_vs_sp"] = round(divergence, 2)
    else:
        summary += "● NYSE騰落統計（代替）: 取得制限あり\n"
    data["NYSE Composite"] = nya_data

    # スタイル・規模
    summary += "\n--- スタイル・規模間相対パフォーマンス ---\n"
    style_tickers = {
        "S&P500グロース(IVW)": "IVW",
        "S&P500バリュー(IVE)": "IVE",
        "Russell2000小型(RUT)": "^RUT",
    }
    style_data = {}
    for name, ticker in style_tickers.items():
        records = fetch_recent_records(ticker)
        summary += format_line(name, records)
        if (records is not None
                and records[-1].get("close") is not None and records[-2].get("close") is not None
                and not _is_nan(records[-1]["close"]) and not _is_nan(records[-2]["close"])):
            latest = records[-1]["close"]
            prev = records[-2]["close"]
            pct = (latest - prev) / prev * 100
            style_data[name] = pct
            data[name] = {
                "value": round(latest, 2),
                "change_percent": round(pct, 2),
                "date": records[-1]["date"]
            }
            _mark_provisional_item(data[name], records[-1])
        else:
            data[name] = None

    if "S&P500グロース(IVW)" in style_data and "S&P500バリュー(IVE)" in style_data:
        gv_diff = style_data["S&P500グロース(IVW)"] - style_data["S&P500バリュー(IVE)"]
        direction = "グロース優勢（リスクオン）" if gv_diff > 0 else "バリュー優勢（ディフェンシブ）"
        summary += f"  グロース対バリュー比（日次）: {gv_diff:+.2f}%pt → {direction}\n"
        data["グロース対バリュー比"] = {"diff_percent": round(gv_diff, 2)}

    sp500_records = fetch_recent_records("^GSPC")
    if (sp500_records is not None and "Russell2000小型(RUT)" in style_data
            and sp500_records[-1].get("close") is not None and sp500_records[-2].get("close") is not None
            and not _is_nan(sp500_records[-1]["close"]) and not _is_nan(sp500_records[-2]["close"])):
        sp_pct = (sp500_records[-1]["close"] - sp500_records[-2]["close"]) / sp500_records[-2]["close"] * 100
        lsv_diff = sp_pct - style_data["Russell2000小型(RUT)"]
        direction = "大型優勢（質への逃避）" if lsv_diff > 0 else "小型優勢（リスク選好）"
        summary += f"  大型対小型比（日次、S&P500対RUT）: {lsv_diff:+.2f}%pt → {direction}\n"
        data["大型対小型比"] = {"diff_percent": round(lsv_diff, 2)}

    # VIX9D vs VIX比較
    summary += "\n--- VIX9D vs VIX（短期・中期リスク比較） ---\n"
    vix9d_records = fetch_recent_records("^VIX9D")
    vix_records2  = fetch_recent_records("^VIX")
    if (vix9d_records is not None and vix_records2 is not None
            and vix9d_records[-1].get("close") is not None and vix9d_records[-2].get("close") is not None
            and vix_records2[-1].get("close") is not None
            and not _is_nan(vix9d_records[-1]["close"]) and not _is_nan(vix9d_records[-2]["close"])
            and not _is_nan(vix_records2[-1]["close"])):
        vix9d_now  = float(vix9d_records[-1]["close"])
        vix9d_prev = float(vix9d_records[-2]["close"])
        vix_now    = float(vix_records2[-1]["close"])
        vix9d_chg  = vix9d_now - vix9d_prev
        vix9d_pct  = vix9d_chg / vix9d_prev * 100 if vix9d_prev > 0 else 0
        ratio      = vix9d_now / vix_now if vix_now > 0 else None
        contango   = vix9d_now < vix_now  # True=順鞘(通常), False=逆転(短期リスクオフ)
        state      = "順鞘（通常）" if contango else "逆転（短期リスクオフ準備）"
        last_date  = datetime.strptime(vix9d_records[-1]["date"], "%Y-%m-%d").strftime("%m/%d")
        summary += f"● VIX9D: {vix9d_now:.2f} [{vix9d_chg:+.2f} ({vix9d_pct:+.1f}%)] ({last_date} 確定) → {state}\n"
        if ratio is not None:
            summary += f"  VIX9D対VIX比: {ratio:.3f} (VIX9D {'>' if not contango else '<'} VIX={vix_now:.2f})\n"
        data["VIX9D（短期VIX）"] = {
            "value": round(vix9d_now, 2),
            "change": round(vix9d_chg, 2),
            "change_percent": round(vix9d_pct, 2),
            "date": vix9d_records[-1]["date"]
        }
        data["VIX9D対VIX比"] = {
            "value": round(ratio, 3) if ratio is not None else None,
            "contango": contango,
            "date": vix9d_records[-1]["date"]
        }
    else:
        summary += "● VIX9D: 取得失敗\n"
        data["VIX9D（短期VIX）"] = None
        data["VIX9D対VIX比"] = None

    # クレジット
    summary += "\n--- クレジット・金融コンディション ---\n"
    hyg_records = fetch_recent_records("HYG")
    lqd_records = fetch_recent_records("LQD")
    summary += format_line("HYG（ハイイールド債ETF）", hyg_records)
    summary += format_line("LQD（投資適格債ETF）", lqd_records)

    if hyg_records is not None and lqd_records is not None:
        for records, name in [(hyg_records, "HYG（ハイイールド債ETF）"), (lqd_records, "LQD（投資適格債ETF）")]:
            if (records[-1].get("close") is None or records[-2].get("close") is None
                    or _is_nan(records[-1]["close"]) or _is_nan(records[-2]["close"])):
                data[name] = None
                continue
            latest = records[-1]["close"]
            prev = records[-2]["close"]
            change = latest - prev
            change_percent = (change / prev) * 100 if prev else 0.0
            vol_latest = records[-1].get("volume")
            vol_prev = records[-2].get("volume")
            volume_ratio = vol_latest / vol_prev if (vol_latest is not None and vol_prev) else None
            data[name] = {
                "value": round(latest, 2),
                "change": round(change, 2),
                "change_percent": round(change_percent, 2),
                "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
                "date": records[-1]["date"]
            }
            _mark_provisional_item(data[name], records[-1])
        # [[MARKETPULSE-HYG-LQD-DATE-MIX-1]]（2026-09-26）: 以前はHYG・LQDそれぞれの
        # 直近2終値で比を取り、日付はHYG側を記録していたため、片方の終値が欠けた日は
        # 別の日の値を組み合わせていた（2026-09-26: HYGは09-25、LQDは09-24）。
        # 両方の終値がそろっている最新の共通日と、その前の共通日（隣り合う営業日）で計算する。
        pair = _aligned_pair_closes("HYG", "LQD")
        if pair is not None:
            try:
                (d_prev, hyg_prev, lqd_prev), (d_now, hyg_now, lqd_now) = pair
                ratio_now = hyg_now / lqd_now
                ratio_prev = hyg_prev / lqd_prev
                ratio_chg = ratio_now - ratio_prev
                last_date = datetime.strptime(d_now, "%Y-%m-%d").strftime("%m/%d")
                direction = "HY優勢＝リスクオン" if ratio_chg > 0 else "スプレッド拡大示唆＝リスクオフ"
                summary += f"● HYG対LQD比（クレジット代理）: {ratio_now:.4f} [{ratio_chg:+.6f}] ({last_date} 確定) → {direction}\n"
                data["HYG対LQD比"] = {
                    "value": round(ratio_now, 4),
                    "change": round(ratio_chg, 6),
                    "date": d_now,
                    "prev_date": d_prev,
                }
            except Exception as e:
                summary += f"● HYG/LQD比率: 計算エラー ({e})\n"
                data["HYG対LQD比"] = None
        else:
            data["HYG対LQD比"] = None
    else:
        data["HYG（ハイイールド債ETF）"] = None
        data["LQD（投資適格債ETF）"] = None
        data["HYG対LQD比"] = None

    return summary, data


def get_market_news():
    """RSSフィードからニュース取得（requests使用）"""
    if not os.path.exists(RSS_LIST_FILE):
        print(f"[WARN] RSSファイルが見つかりません: {RSS_LIST_FILE}")
        return []
    with open(RSS_LIST_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    all_entries = []
    for url in urls:
        try:
            # requests で取得（非ASCII文字列でも自動処理）
            response = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            for e in feed.entries[:8]:
                all_entries.append(f"T: {e.title}\nS: {e.get('summary', '')}")
        except Exception as e:
            print(f"[WARN] RSS取得失敗: {url} ({e})")
    print(f"[INFO] RSS取得件数: {len(all_entries)} 件")
    return all_entries


def build_ai_facts(stage_result, structured_data, sentiment_data, fear_greed_data, tech_pulse_data, asset_flow_data, credit_data=None):
    """MP-28（指示書㉓）: AIに渡す「確定した事実」のJSON。各段階の結論1行・判定に使った数値・基準日だけを入れる。
    ニュース本文やAIに推測させる材料は入れない。"""
    def ind(k, *fields):
        v = (structured_data or {}).get(k)
        if not isinstance(v, dict):
            return None
        return {f: v.get(f) for f in ("value", "change_percent", "change_bp", "date") + fields if v.get(f) is not None}
    st = (stage_result or {}).get("stages") or {}
    facts = {
        "今日の結論": {f"段階{k}": (v or {}).get("line") for k, v in sorted(st.items())},
        "天気": (stage_result or {}).get("weather"),
        "データ基準日": {k: v for k, v in ((stage_result or {}).get("data_quality") or {}).items() if k != "as_of"},
        "指標": {k: ind(k) for k in ("S&P500", "NASDAQ", "VIX指数", "米10年債", "ドル円", "WTI原油", "金（GOLD）")},
        "段階1の事実タグ": (st.get("1") or {}).get("tags"),
        "資産の前営業日比": {k: (v or {}).get("change_pct") for k, v in (asset_flow_data or {}).items()},
        "センチメント指数": {"score": (sentiment_data or {}).get("score"), "label": (sentiment_data or {}).get("label")},
        "CNN_Fear_Greed": {"score": (fear_greed_data or {}).get("score"), "rating": (fear_greed_data or {}).get("rating")},
        "Tech_Pulse": {"score": (tech_pulse_data or {}).get("score")},
        "市場の広がり": {k: ((sentiment_data or {}).get("breadth") or {}).get(k)
                     for k in ("date", "advances", "declines", "new_highs_52w", "new_lows_52w", "pct_above_50ma")},
        "過去の実績（段階8）": (st.get("8") or {}).get("signals"),
    }
    if credit_data:
        facts["株・債券・クレジット"] = credit_data
    return facts


AI_PROMPT = """あなたは市場の解説者だ。以下のJSONは、本システムが閾値で機械的に判定した「確定した事実」である。
このJSONだけを根拠に、今日の米国市場の見解を日本語で書け。

制約:
- JSONに無い数値を書かない。数値を新たに計算・推測・丸め直ししない（引用するときはJSONの値のまま）
- JSONに無い出来事・ニュース・発言・予定に触れない
- 因果を断定しない（「〜が原因」「〜を受けて」は使わない）。同じ日に起きた事実として並べる
- 将来の値動きを予測しない。売買を勧めない
- Markdown記法は使わない。プレーンテキスト

出力形式:
1行目から5〜8文の見解（段落1つ）。
最後の行に「俳句：」に続けて、今日の市場を詠んだ五・七・五の俳句を1句（数値は入れない）。

【確定した事実（JSON）】
{facts}
"""


def split_haiku(text):
    """AIの出力から最後の「俳句：」行を取り出す。(本文, 俳句 or None)"""
    if not text:
        return text, None
    lines = text.rstrip().splitlines()
    for i in range(len(lines) - 1, -1, -1):
        m = re.match(r"\s*俳句[：:]\s*(.+)$", lines[i])
        if m:
            return "\n".join(lines[:i]).rstrip(), m.group(1).strip()
    return text, None


def analyse_market(facts):
    """xAI Grok API（OpenAI互換エンドポイント）で「AIの見解」を生成する（MP-28、指示書㉓）。
    入力は確定した事実のJSON（build_ai_facts()）だけ。戻り値は(見解の本文, 俳句)。"""
    prompt = AI_PROMPT.format(facts=json.dumps(facts, ensure_ascii=False, indent=1, default=str))
    url = "https://api.x.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {XAI_API_KEY}",
    }
    # [[GROK-MODEL-PRICE-1]]対応: grok-3-mini/grok-3は実際にはgrok-4.3へ
    # 自動ルーティングされ、grok-2-1212は廃止済み（API側でModel not found）。
    # 実態に合わせ、実際に応答する現行モデル名を明示する（フォールバック
    # ループ構造自体は一時的なネットワーク障害時の再試行として維持）。
    models = ["grok-4.3", "grok-4.3", "grok-4.3"]
    last_error = None
    for model in models:
        try:
            print(f"[INFO] Grokモデル試行中: {model}")
            resp = requests.post(url, headers=headers, json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 2048,
                "temperature": 0.3,
            }, timeout=120)
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]
            print(f"[OK] Grokモデル成功: {model}")
            return split_haiku(text)
        except Exception as e:
            print(f"[WARN] Grokモデル失敗 ({model}): {e}")
            last_error = e
    print("[ERROR] すべてのGrokモデルで失敗しました")
    raise last_error


def fetch_fred_short_bond(asset_def):
    """
    common.macro_data.reader経由で短期国債（3ヶ月T-Bill）データ
    （DGS3MO系列）を取得する（MACRODATA-LAYER-CONSTRUCTION-1本番消費者
    切替、2026-08-12。旧実装はfredapi直接呼び出し・都度Fred()生成
    だった）。
    ^IRX（yfinance）はGitHub Actions環境からの取得が直近4日連続で失敗しており
    （Yahoo Finance公式サイトでは同期間のデータ存在を確認済み＝取得経路側の
    問題と判断）、short_bondのみFRED APIに切替（MP-IRX-FRED-1）。
    change_pctは他6資産（ETF価格ベース）との表示整合性のため、利回り値そのものの
    変化率（%）として算出する（bp差分ではない。^IRX時代の定義をそのまま踏襲）。
    FREDのDGS3MOは更新に1営業日程度のラグがあるため、dateフィールドは
    FRED側の実際の最終データ日付をそのまま使う（当日分とは限らない）。
    """
    if not HAS_MACRO_DATA:
        print("[WARN] asset_flow 短期国債(DGS3MO): common.macro_data.reader が利用できません。スキップ。")
        return None
    try:
        start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        records = _mdata_get_series("DGS3MO", start=start)
        records = [r for r in records if r.get("value") is not None]
        if len(records) < 2:
            print(f"[WARN] asset_flow 短期国債(DGS3MO): データ不足（{len(records)}件）。")
            return None
        latest = float(records[-1]["value"])
        prev = float(records[-2]["value"])
        chg_pct = (latest - prev) / prev * 100 if prev > 0 else 0
        date_str = records[-1]["as_of"]
        print(f"[INFO] asset_flow {asset_def['label']}(DGS3MO): {chg_pct:+.2f}% (latest={latest}, date={date_str})")
        return {
            "label":    asset_def["label"],
            "ticker":   asset_def["ticker"],
            "desc":     asset_def["desc"],
            "value":    round(latest, 4),
            "change_pct": round(chg_pct, 3),
            "date":     date_str,
        }
    except Exception as e:
        print(f"[WARN] asset_flow 短期国債(DGS3MO)取得失敗: {e}")
        return None


def collect_asset_flow():
    """
    資産クラス間資金フロービジュアライザー用データ収集
    並び順（安全→リスク）: 超短期国債→短期国債→金→長期国債→投資適格社債→HY社債→株式
    short_bond（短期国債）のみFRED API経由（fetch_fred_short_bond参照、MP-IRX-FRED-1）。
    他6資産はcommon.market_data.reader経由（fetch_recent_records()、VIX/
    S&P500等の主要指標で既に使われている既存ヘルパーを再利用）。

    [[MARKETDATA-COLLECT-ASSET-FLOW-UNTRACKED-1]]の切替（2026-08-13）:
    SHV（超短期国債ETF）がmarket_dataの現行カバレッジ（INDEX_ETF_
    COMMODITY_SYMBOLS）に未収録のため本関数は_fetch_hist_legacy()を
    使い続けていたが、SHVを同シンボルへ追加・バックフィル（daily/には
    2021-01-04〜の全期間、他資産と同じ1,408件保存済み）した上で6資産
    全てをreader経由に切替。
    """
    ASSETS = [
        {"key": "ultra_short", "label": "超短期国債", "ticker": "SHV",     "desc": "1-3ヶ月T-Bill ETF"},
        {"key": "short_bond",  "label": "短期国債",   "ticker": "DGS3MO", "desc": "3ヶ月T-Bill利回り"},
        {"key": "gold",        "label": "金",          "ticker": "GLD",     "desc": "金ETF"},
        {"key": "long_bond",   "label": "長期国債",    "ticker": "TLT",     "desc": "20年超米国債ETF"},
        {"key": "ig_bond",     "label": "投資適格社債","ticker": "LQD",     "desc": "投資適格社債ETF"},
        {"key": "hy_bond",     "label": "HY社債",      "ticker": "HYG",     "desc": "ハイイールド社債ETF"},
        {"key": "equity",      "label": "株式",         "ticker": "SPY",     "desc": "S&P500 ETF"},
    ]
    result = {}
    for a in ASSETS:
        if a["key"] == "short_bond":
            result[a["key"]] = fetch_fred_short_bond(a)
            continue
        try:
            records = fetch_recent_records(a["ticker"])
            if (records is None or records[-1].get("close") is None or records[-2].get("close") is None
                    or _is_nan(records[-1]["close"]) or _is_nan(records[-2]["close"])):
                reason = "取得失敗（データ不足またはmarket_data未import）" if records is None else "Close値にNone/NaN混入"
                print(f"[WARN] asset_flow {a['label']}({a['ticker']}): 取得失敗 - {reason}")
                result[a["key"]] = None
                continue
            latest = records[-1]["close"]
            prev   = records[-2]["close"]
            chg_pct = (latest - prev) / prev * 100 if prev > 0 else 0
            date_str = records[-1]["date"]
            result[a["key"]] = {
                "label":    a["label"],
                "ticker":   a["ticker"],
                "desc":     a["desc"],
                "value":    round(latest, 4),
                "change_pct": round(chg_pct, 3),
                "date":     date_str,
            }
            _mark_provisional_item(result[a["key"]], records[-1])
            print(f"[INFO] asset_flow {a['label']}({a['ticker']}): {chg_pct:+.2f}%")
        except Exception as e:
            print(f"[WARN] asset_flow {a['ticker']}: {e}")
            result[a["key"]] = None
    return result

def calc_hindenburg(breadth, sp500_up_50d):
    """ヒンデンブルグ・オーメンの一般的な定義（MP-18、指示書㉓）。条件ごとの成否を返す。

    1. 新高値・新安値がともに銘柄数の2.2%以上（新高値・新安値は実際に52週高値・安値を更新した銘柄。
       breadth_calculator.py）
    2. 指数が上昇トレンド（S&P500が50営業日前より高い、で近似）
    3. McClellan Oscillatorが負
    4. 新高値が新安値の2倍以下
    元の定義はNYSE全銘柄だが、本システムはS&P500構成銘柄で数える。

    [[MARKETPULSE-MINOR-INCONSISTENCIES-1]]①: 銘柄数は固定値500ではなくbreadthの実測total_stocks
    （取得できない場合のみ500）。

    Returns:
        {"active": bool|None, "conditions": {...}}。breadthがNone/空ならNone。
        入力が欠けた条件はNoneとし、そのときactiveもNone（判定不能）
    """
    if not breadth:
        return None
    nh = breadth.get("new_highs_52w") or 0
    nl = breadth.get("new_lows_52w") or 0
    total_stocks = breadth.get("total_stocks") or 500
    mc = breadth.get("mcclellan_oscillator")
    cond = {
        "nh_nl_both_2_2pct": bool(nh >= total_stocks * 0.022 and nl >= total_stocks * 0.022),
        "sp500_uptrend_50d": None if sp500_up_50d is None else bool(sp500_up_50d),
        "mcclellan_negative": None if mc is None else bool(mc < 0),
        "nh_le_2x_nl": bool(nh <= 2 * nl),
    }
    if any(v is False for v in cond.values()):
        active = False
    elif any(v is None for v in cond.values()):
        active = None
    else:
        active = True
    return {"active": active, "conditions": cond, "new_highs": nh, "new_lows": nl, "total_stocks": total_stocks}


def calc_hindenburg_active(breadth, sp500_up_50d=None):
    """calc_hindenburg()のactiveだけを返す（チェックリスト用）。"""
    h = calc_hindenburg(breadth, sp500_up_50d)
    return None if h is None else h["active"]


def sp500_up_vs_50d():
    """S&P500の最新終値が50営業日前の終値より高いか（daily/）。取れなければNone。"""
    if not HAS_MARKET_DATA:
        return None
    try:
        rows = [r for r in _md_get_price_series("^GSPC", days=80) if not r.get("_gap") and r.get("close") is not None]
        if len(rows) < 51:
            return None
        return rows[-1]["close"] > rows[-51]["close"]
    except Exception as e:
        print(f"[WARN] S&P500の50営業日比較に失敗: {e}")
        return None


def calc_take_profit_checklist(fg_score, above_ma200, ma200_slope, hy_is_expanding, hindenburg_active):
    """TAKE PROFITチェックリスト（F&G>=75で発動）
    3チェック項目、1点ずつ採点: 2点→PARTIAL、3点→TAKE PROFIT
    """
    triggered = fg_score is not None and fg_score >= 75

    checks = []

    # チェック1: S&P500 200日MAシグナル
    # 終値 < MA200 または MA200が下向きで警戒
    if above_ma200 is not None and ma200_slope is not None:
        c1_warn = (not above_ma200) or (not ma200_slope)
        checks.append({
            "key": "ma200",
            "label": "S&P500 200日MA",
            "passed": not c1_warn,
            "point": 1 if c1_warn else 0,
            "detail": f"終値{'＞' if above_ma200 else '＜'}MA200 / MA200傾き{'↑上向き' if ma200_slope else '↓下向き'}",
        })
    else:
        checks.append({"key": "ma200", "label": "S&P500 200日MA", "passed": True, "point": 0, "detail": "データ取得不可"})

    # チェック2: HYスプレッド拡大（リスクオフシグナル）
    if hy_is_expanding is not None:
        checks.append({
            "key": "hy_spread",
            "label": "HYスプレッド",
            "passed": not hy_is_expanding,
            "point": 1 if hy_is_expanding else 0,
            "detail": "スプレッド拡大中（90日最小値+30bps超）" if hy_is_expanding else "スプレッド安定",
        })
    else:
        checks.append({"key": "hy_spread", "label": "HYスプレッド", "passed": True, "point": 0, "detail": "データ取得不可"})

    # チェック3: ヒンデンブルグ・オーメン（天井シグナル）
    if hindenburg_active is not None:
        checks.append({
            "key": "hindenburg",
            "label": "ヒンデンブルグ・オーメン",
            "passed": not hindenburg_active,
            "point": 1 if hindenburg_active else 0,
            "detail": "シグナル発生（新高値・新安値の同時出現、上昇トレンド、McClellan負、新高値≦新安値×2）" if hindenburg_active else "シグナルなし",
        })
    else:
        checks.append({"key": "hindenburg", "label": "ヒンデンブルグ・オーメン", "passed": True, "point": 0, "detail": "データ取得不可"})

    points = sum(c["point"] for c in checks)

    if points >= 3:
        action = "TAKE PROFIT"
    elif points >= 2:
        action = "PARTIAL"
    else:
        action = "HOLD"

    return {
        "triggered": triggered,
        "fg_score": fg_score,
        "points": points,
        "action": action,
        "checks": checks,
    }


def calc_buy_checklist(fg_score, above_ma200, ma200_slope, hy_current, hy_max_90d, hindenburg_active):
    """BUYチェックリスト（F&G<=25で発動）
    3チェック項目、1点ずつ採点: 2点以上→BUY
    """
    triggered = fg_score is not None and fg_score <= 25
    extreme = fg_score is not None and fg_score <= 10

    checks = {}

    # チェック①: S&P500が200日線下方または200日線下向き（売られすぎ環境）
    if above_ma200 is not None and ma200_slope is not None:
        c1_match = (not above_ma200) or (not ma200_slope)
        checks["sp500_ma200"] = {
            "above": above_ma200,
            "slope_up": ma200_slope,
            "point": 1 if c1_match else 0,
        }
    else:
        checks["sp500_ma200"] = {"above": None, "slope_up": None, "point": 0}

    # チェック②: HYスプレッドが90日最高値から30bps縮小（信用収縮が緩和）
    if hy_current is not None and hy_max_90d is not None:
        is_contracting = bool(hy_current < hy_max_90d - 0.30)
        checks["hy_spread"] = {
            "current": hy_current,
            "max_90d": hy_max_90d,
            "is_contracting": is_contracting,
            "point": 1 if is_contracting else 0,
        }
    else:
        checks["hy_spread"] = {"current": None, "max_90d": None, "is_contracting": None, "point": 0}

    # チェック③: ヒンデンブルグ・オーメンが非活性（市場の二極化が解消）
    if hindenburg_active is not None:
        checks["hindenburg"] = {
            "active": hindenburg_active,
            "point": 1 if not hindenburg_active else 0,
        }
    else:
        checks["hindenburg"] = {"active": None, "point": 0}

    points = sum(c["point"] for c in checks.values())
    action = "BUY（積極的に拾う）" if points >= 2 else "WATCH（準備段階・様子見）"

    return {
        "triggered": triggered,
        "extreme": extreme,
        "points": points,
        "action": action,
        "fg_score": fg_score,
        "checks": checks,
    }


def check_data_freshness(now_utc=None, symbols=("^GSPC", "SPY")):
    """daily/の最新日付が、実行時点で確定している直近の米国終値日より古くないかを返す
    （[[MARKETPULSE-MDD-CHECKOUT-RACE-1]]、2026-09-26）。

    期待する終値日はNYSEカレンダーの取引日のうち、取引終了時刻（夏時間20:00 UTC・
    冬時間21:00 UTC、短縮取引日は早い）が実行時刻より前の最新日。daily/の最新日付は
    reader.get_latest_price()（有効な終値を持つ最新の行）の`date`。
    結果はmarket_data.jsonのエントリの`data_freshness`に記録する（画面表示は未実装）。
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    out = {"checked_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), "expected_close_date": None,
           "daily_latest": {}, "stale": None}
    try:
        import pandas_market_calendars as _mcal
        cal = _mcal.get_calendar("NYSE")
        sched = cal.schedule(start_date=(now_utc - timedelta(days=10)).date(), end_date=now_utc.date())
        closed = sched[sched["market_close"] <= now_utc]
        if len(closed):
            out["expected_close_date"] = closed.index[-1].strftime("%Y-%m-%d")
        if HAS_MARKET_DATA:
            from common.market_data.reader import get_latest_price as _md_get_latest_price
            for s in symbols:
                p = _md_get_latest_price(s)
                out["daily_latest"][s] = p.get("date") if p else None
        exp = out["expected_close_date"]
        if exp and out["daily_latest"]:
            out["stale"] = any(d is None or d < exp for d in out["daily_latest"].values())
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    if out.get("stale"):
        print(f"[WARN] daily/の最新日付{out['daily_latest']}が期待する終値日{out['expected_close_date']}より古い")
    return out


def calc_credit(structured_data, asset_flow_data):
    """株・債券・クレジットの3方向の判定とRisk-Offスコア（MP-27）。"""
    sp500_chg   = ((structured_data.get("S&P500") or {}).get("change_percent") or 0)
    hyg_chg_pct = ((structured_data.get("HYG（ハイイールド債ETF）") or {}).get("change_percent") or 0)
    lqd_chg_pct = ((structured_data.get("LQD（投資適格債ETF）") or {}).get("change_percent") or 0)

    # credit_stock: ^GSPC（S&P500指数）ベース。ETFの分配・トラッキング
    # 誤差を含まない、市場全体の日次方向性を測る単純な指標として指数を使う
    # （SPY統一案〈②案a〉は asset_flow_data取得失敗時にcredit_stockまで
    # 連鎖的に判定不能になる結合リスクを生むため見送った）。
    credit_stock  = "リスクオフ" if sp500_chg < -1.0 else "リスクオン"
    # クレジット: HYG変化率 < LQD変化率 → スプレッド拡大 → リスクオフ
    # HYG対LQD比（共通日でそろえた比の変化）を優先する。取れない場合のみ従来の
    # 各自の前日比の差で判定する（MARKETPULSE-HYG-LQD-DATE-MIX-1、2026-09-26）
    _hl = structured_data.get("HYG対LQD比") or {}
    if _hl.get("change") is not None:
        credit_credit = "リスクオフ" if _hl["change"] < 0 else "リスクオン"
    else:
        credit_credit = "リスクオフ" if (hyg_chg_pct - lqd_chg_pct) < 0 else "リスクオン"

    # 債券: TLT(long_bond)上昇 & SPY(equity)下落 → 質への逃避 → 債券買い
    # TLTが下落（利回り上昇＝債券安）の場合は債券売り。"リスクオン/オフ"表記は誤解を招くため廃止。
    # credit_bond: SPY（S&P500 ETF）ベース。「資金が株式から債券へ逃避
    # しているか」という資金フロー概念のため、実際に売買可能なファンド
    # （TLTとの比較対象としてもSPY）が必須（^GSPC統一案〈②案b〉は指数
    # 自体に資金流出入の概念がなく、collect_asset_flow()の7資産クラス
    # ラインナップとの整合性も失われるため見送った）。
    # MP-27（指示書㉓）: 以前は「債券買い」以外のすべての日を「債券売り」にしていた。
    # 「債券売り」はTLTが実際に下落した日だけにし、それ以外は「中立」にする。
    tlt_af = ((asset_flow_data or {}).get("long_bond") or {})
    spy_af = ((asset_flow_data or {}).get("equity")    or {})
    tlt_af_chg = tlt_af.get("change_pct")
    spy_af_chg = spy_af.get("change_pct")
    credit_bond = bond_direction(tlt_af_chg, spy_af_chg)

    risk_off_count = sum([
        credit_stock  == "リスクオフ",
        credit_bond   == "債券買い",   # 質への逃避（旧: リスクオフ）をリスクオフシグナルとして計上
        credit_credit == "リスクオフ",
    ])
    risk_off_score = round(risk_off_count / 3 * 100)

    credit_data = {
        "stock":          credit_stock,
        "bond":           credit_bond,
        "credit":         credit_credit,
        "risk_off_score": risk_off_score,
    }
    print(f"[INFO] credit: stock={credit_stock} bond={credit_bond} credit={credit_credit} → risk_off_score={risk_off_score}")
    return credit_data


def bond_direction(tlt_chg, spy_chg):
    """MP-27の債券判定。TLT>+0.3%かつSPY<−0.5%→債券買い、TLT<0→債券売り、それ以外→中立。"""
    if tlt_chg is not None and spy_chg is not None and tlt_chg > 0.3 and spy_chg < -0.5:
        return "債券買い"
    if tlt_chg is not None and tlt_chg < 0:
        return "債券売り"
    return "中立"


def sentiment_signal(score, history, window_start):
    """MP-05（指示書㉓）: センチメントのシグナル（BUY / TAKE PROFIT / HOLD）。
    以前は画面の表示期間の「直近20エントリ」で判定しており、表示期間で結果が変わった。
    日付で直近20営業日（window_start以降、JSTの実行日で比較）のエントリと今回のスコアで判定する。

    Args:
        score: 今回のスコア
        history: [(エントリの日付'YYYY-MM-DD', スコア), ...]（今回を含まない）
        window_start: 直近20営業日の初日'YYYY-MM-DD'
    """
    if score is None:
        return None
    recent = [s for d, s in history if d >= window_start and s is not None] + [score]
    peak = max(recent)
    if score <= 20:
        sig, reason = "BUY", "スコア20以下"
    elif len(recent) >= 3 and peak >= 70 and peak - score >= 5:
        sig, reason = "TAKE PROFIT", f"直近20営業日のピーク{peak:.1f}から{peak - score:.1f}pt低下"
    else:
        sig, reason = "HOLD", "BUY・TAKE PROFITの条件に当たらない"
    return {"signal": sig, "reason": reason, "window_start": window_start, "peak": peak, "n": len(recent)}


def signal_window_start(now_utc, n=20):
    """now_utc時点の直近n営業日（NYSE）の初日。"""
    import pandas_market_calendars as _mcal
    days = _mcal.get_calendar("NYSE").valid_days(start_date=(now_utc - timedelta(days=n * 2 + 15)).date().isoformat(),
                                                 end_date=now_utc.date().isoformat())
    return days[-n].strftime("%Y-%m-%d")


def write_nyse_holidays(path=None, now_utc=None):
    """MP-21（指示書㉓）: 資金フローの直近7日グリッドで米国の祝日にも「休場」行を入れるため、
    前年〜翌年のNYSEの休場日（土日以外）をdata/nyse_holidays.jsonに書く。"""
    path = path or os.path.join(DATA_DIR, "nyse_holidays.json")
    now_utc = now_utc or datetime.now(timezone.utc)
    try:
        import pandas as _pd
        import pandas_market_calendars as _mcal
        y = now_utc.year
        start, end = f"{y - 1}-01-01", f"{y + 1}-12-31"
        valid = set(d.strftime("%Y-%m-%d") for d in _mcal.get_calendar("NYSE").valid_days(start_date=start, end_date=end))
        hol = [d.strftime("%Y-%m-%d") for d in _pd.bdate_range(start, end) if d.strftime("%Y-%m-%d") not in valid]
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"source": "pandas_market_calendars NYSE", "from": start, "to": end, "holidays": hol},
                      f, ensure_ascii=False, indent=1)
        return hol
    except Exception as e:
        print(f"[WARN] NYSE休場日の書き出しに失敗: {e}")
        return None


def save_data_to_json_and_csv(report_text, structured_data, sentiment_data, fear_greed_data=None, tech_pulse_data=None, asset_flow_data=None, take_profit_checklist=None, buy_checklist=None, data_freshness=None, stage_result=None, haiku=None, ai_facts=None, hindenburg=None):
    os.makedirs(DATA_DIR, exist_ok=True)
    jst_now = datetime.now(JST)
    date_str = jst_now.strftime('%Y-%m-%dT%H:%M:%S+09:00')
    # MP-23（指示書㉓）: 天気はAIの文章から抽出せず、段階3・4の結論からルール（v3）で判定する。
    # 判定できない日（段階3・4の入力が欠けた日）は「不明」
    judgment = ((stage_result or {}).get("weather") or {}).get("label") or "不明"

    # JSON
    if os.path.exists(JSON_PATH):
        try:
            with open(JSON_PATH, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    all_data = []
                else:
                    f.seek(0)
                    all_data = json.load(f)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"[WARN] JSONファイルが破損しています。新しく作成します: {JSON_PATH} (エラー: {e})")
            all_data = []
    else:
        all_data = []

    # ── Credit / Risk-Off Score 計算 ──
    # 3軸（株・債券・クレジット）のリスクオフ判定からスコアを算出
    #
    # [[MARKETPULSE-MINOR-INCONSISTENCIES-1]]②再確認（2026-08-26、案c採用
    # ＝現状維持・意図的差異として容認、コメント明記のみでロジック変更なし）:
    # credit_stock（下記）は structured_data["S&P500"]（^GSPC指数）を、
    # credit_bond（後述）は asset_flow_data["equity"]（SPY ETF）をそれぞれ
    # 参照しており、同じ「株式」でも原資産が異なる。これは無区別な混在では
    # なく意図的な使い分け:
    #   - credit_stock: 「市場全体の今日の方向性」を測る単純な指標のため、
    #     ETFの分配・トラッキング誤差の影響を受けない指数（^GSPC）が適切
    #   - credit_bond: 「資金が株式から債券へ逃避しているか」という資金
    #     フロー概念のため、指数自体には資金流出入が存在せず、実際に売買
    #     可能なファンド（SPY/TLT、collect_asset_flow()の7資産クラス
    #     ラインナップと整合）の比較が必須
    # 統一案（credit_stockをSPY化 or credit_bondを^GSPC化）はいずれも
    # 個別の設計上の問題を生むため見送った（それぞれ後述コメント参照）。
    # 残存リスク: SPYの四半期分配落ち（ex-dividend）時、^GSPCとSPYの
    # change_percentが最大0.673pt（2026-06-27〜29で実測）乖離することを
    # 確認済み。credit_bondの閾値（spy_af_chg<-0.5）はこの乖離幅より
    # 小さいため、TLT側条件が同時に境界へ来る局面ではinstrument選択が
    # credit_bondの最終判定を左右しうる（今回の実測期間ではTLT側条件が
    # 不成立だったため実際の判定は変わらなかった）。
    credit_data = calc_credit(structured_data, asset_flow_data)

    # sentiment スコア範囲チェック（CSV列ズレ等による異常値混入を防ぐ）
    _sent_score = (sentiment_data or {}).get("score")
    if _sent_score is not None and not (0 <= _sent_score <= 100):
        raise ValueError(
            f"[ERROR] sentiment_score が 0〜100 範囲外です: {_sent_score}。"
            "collect_and_send.py の compute_sentiment 出力を確認してください。"
        )

    # 同日の既存エントリを削除して上書き（同日に複数回実行された場合の重複防止）
    today = date_str[:10]
    all_data = [d for d in all_data if d.get("date", "")[:10] != today]

    # AIコメント履歴: 当日分 + 直近最大11件の過去分（新しい順、max12件）
    hist_prev = [
        {"date": prev.get("date", ""), "summary": prev.get("summary", "")}
        for prev in reversed(all_data[-11:])
        if prev.get("summary")
    ]
    comments_history = [{"date": date_str, "summary": report_text}] + hist_prev

    new_entry = {
        "date": date_str,
        "judgment": judgment,
        "indicators": structured_data,
        "sentiment": sentiment_data,
        "fear_greed": fear_greed_data,
        "tech_pulse": tech_pulse_data,
        "asset_flow": asset_flow_data,
        "credit": credit_data,
        "take_profit_checklist": take_profit_checklist,
        "buy_checklist": buy_checklist,
        "summary": report_text,
        "comments_history": comments_history
    }
    if data_freshness is not None:
        new_entry["data_freshness"] = data_freshness
    if stage_result is not None:
        new_entry["stage_conclusions"] = stage_result["stages"]
        new_entry["weather"] = stage_result["weather"]
        new_entry["data_quality"] = stage_result["data_quality"]
    if haiku:
        new_entry["haiku"] = haiku
    if ai_facts is not None:
        new_entry["ai_facts"] = ai_facts
    if hindenburg is not None:
        new_entry["hindenburg"] = hindenburg
    all_data.append(new_entry)

    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] JSON保存完了: {JSON_PATH} (全{len(all_data)}件)")

    # CSV
    row = {"date": date_str, "judgment": judgment}
    for key, value in structured_data.items():
        if value is None:
            continue
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                col_name = f"{key}_{subkey}"
                row[col_name] = subvalue
        else:
            row[key] = value
    row["sentiment_score"] = sentiment_data.get("score", "")
    row["sentiment_label"] = sentiment_data.get("label", "")
    row["summary"] = report_text

    # CSVヘッダー整合チェック: CSV_COLUMNS変更時に既存ヘッダーを自動更新
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'r', newline='', encoding='utf-8') as f:
            existing_header = next(csv.reader(f), None)
        if existing_header is not None and list(existing_header) != CSV_COLUMNS:
            with open(CSV_PATH, 'r', newline='', encoding='utf-8') as f:
                old_rows = list(csv.DictReader(f))
            with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
                writer_fix = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction='ignore')
                writer_fix.writeheader()
                writer_fix.writerows(old_rows)
            print(f"[INFO] CSVヘッダーを自動更新しました: {CSV_PATH}")

    file_exists = os.path.exists(CSV_PATH)
    with open(CSV_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    print(f"[INFO] CSV保存完了: {CSV_PATH}")


def send_email(body, sentiment_data):
    jst_now = datetime.now(JST)
    score = sentiment_data.get("score", "?")
    label = sentiment_data.get("label", "")
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = f"\U0001f99d02_\u3010\u5e02\u6cc1\u5206\u6790\u3011{label} ({score}) {jst_now.strftime('%m/%d %H:%M')}"
    msg['From'], msg['To'] = GMAIL_USER, GMAIL_USER
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(GMAIL_USER, GMAIL_PASSWORD)
            smtp.send_message(msg)
        print("[INFO] メール送信成功")
    except Exception as e:
        print(f"[ERROR] メール送信失敗: {e}")
        raise


def fetch_cnn_fear_greed():
    """CNN Fear & Greed Indexを取得する。

    [[FEARGREED-DUPKEY-BUG-1]]対応（2026-08-26）: 旧実装は`fg.get()`
    （ラッパー、`history`辞書は`1w`/`1m`/`3m`/`6m`/`1y`の5キーのみで
    日次・前日相当のキーが存在しない）を使い、`previous_close`と
    `one_week_ago`の両方を誤って`history.get("1w")`（1週間前の値）で
    埋めていた。真の前日終値は生API応答
    （`fg.fetch()`、`get()`が内部で呼ぶのと同じセッション・エラー
    ハンドリングを使う単一のHTTPリクエスト）の
    `data["fear_and_greed"]["previous_close"]`にのみ存在し、`get()`は
    これを握りつぶして返さない。`get()`を経由せず`fetch()`から直接
    score/rating/previous_close/previous_1_week/previous_1_monthを
    抽出することで、二重フェッチを避けつつ正しい値を取得する。
    """
    try:
        import fear_greed as fg
        data = fg.fetch()
        fear = data["fear_and_greed"]
        score = fear.get("score")
        rating = fear.get("rating", "")
        print(f"[INFO] CNN Fear & Greed: {score:.1f} ({rating})")

        def _r(key):
            v = fear.get(key)
            return round(v, 2) if v is not None else None

        return {
            "score": round(score, 1) if score is not None else None,
            "rating": rating,
            "previous_close": _r("previous_close"),
            "one_week_ago": _r("previous_1_week"),
            "one_month_ago": _r("previous_1_month"),
        }
    except ImportError:
        print("[WARN] fear-greed パッケージ未インストール。CNN F&Gスキップ。")
        return None
    except Exception as e:
        print(f"[WARN] CNN Fear & Greed取得失敗: {e}")
        return None


if __name__ == "__main__":
    # --- 必須環境変数チェック ---
    required_env_vars = ["XAI_API_KEY", "GMAIL_USER", "GMAIL_PASSWORD"]
    missing = [v for v in required_env_vars if not os.getenv(v)]
    if missing:
        print(f"[ERROR] 必須環境変数が設定されていません: {', '.join(missing)}")
        sys.exit(1)

    data_freshness = check_data_freshness()
    realtime_text, structured_data = get_realtime_data()

    # センチメントスコア算出（フォールバック補完前の今回実測データのみで算出。
    # スコアリングロジックは変更しない＝MP-FALLBACK-DISPLAY-1のスコープ外）
    sentiment_data = compute_sentiment(structured_data)
    print(f"[INFO] センチメントスコア: {sentiment_data['score']} ({sentiment_data['label']})")
    for k, v in sentiment_data["sub_scores"].items():
        print(f"  {k}: {v['score']:.1f} (weight={v['weight']}, raw={v['raw']})")

    # 取得失敗(None)になったindicatorsを前回値で補完（表示用、MP-FALLBACK-DISPLAY-1）
    _recent_entries = _load_recent_entries()
    structured_data = _fill_fallbacks(structured_data, "indicators", _recent_entries)

    # CNN Fear & Greed Index取得
    fear_greed_data = fetch_cnn_fear_greed()

    # Tech Pulse（ナスダック感情指数）算出
    qqq_vs_ma125, qqq_vs_spy_20d = fetch_qqq_tech_data()
    vxn_latest, vxn_vs_ma50 = fetch_vxn_from_fred()
    fg_score_tech = fetch_fg_score_from_feargreedchart()
    history_90d = _load_tech_pulse_history(JSON_PATH, window=90)
    tp_score = calc_tech_pulse_score(qqq_vs_ma125, vxn_vs_ma50, qqq_vs_spy_20d, history_90d)
    # 異常値ガード: score=100 かつ直前スコアとの差が20以上なら前回値を保持
    _prev_tp_score = _load_prev_tech_pulse_score(JSON_PATH)
    if tp_score == 100 and _prev_tp_score is not None and abs(tp_score - _prev_tp_score) >= 20:
        print(f"[WARN] Tech Pulse異常値検出: score={tp_score} (前回={_prev_tp_score}) → 前回値を保持")
        tp_score = _prev_tp_score
    tp_label = _tp_label(tp_score)

    # 乖離・Zスコア・シグナル（乖離=Tech Pulse - CNN F&G、画面表示と統一）
    fg_cnn_score = (fear_greed_data or {}).get("score")
    div_value = round(float(tp_score) - float(fg_cnn_score), 1) if fg_cnn_score is not None else None
    div_hist = _load_div_history(JSON_PATH, window=90)
    if div_value is not None:
        div_hist.append(div_value)
    div_zscore = _calc_divergence_zscore(div_hist)
    tp_signal = _get_tp_signal(div_value, div_zscore, fg_cnn_score)

    tech_pulse_data = {
        "score": tp_score,
        "label": tp_label,
        "components": {
            "qqq_vs_ma125": qqq_vs_ma125,
            "vxn_latest": vxn_latest,
            "vxn_vs_ma50": vxn_vs_ma50,
            "qqq_vs_spy_20d": qqq_vs_spy_20d,
            "fg_score": fg_score_tech,
            "vxn_available": vxn_vs_ma50 is not None,
        },
        "divergence": {
            "value": div_value,
            "zscore": div_zscore,
            "signal": tp_signal,
        },
    }
    print(f"[INFO] Tech Pulseスコア: {tp_score} ({tp_label}), 乖離={div_value}, Z={div_zscore}, シグナル={tp_signal or 'なし'}")

    # TAKE PROFIT チェックリスト算出
    sp500_ma_data = _get_sp500_ma_deviation()
    above_ma200 = sp500_ma_data.get("above_ma200") if sp500_ma_data else None
    ma200_slope = sp500_ma_data.get("ma200_slope") if sp500_ma_data else None
    hy_spread_data = fetch_hy_spread_from_fred()
    hy_is_expanding = (hy_spread_data or {}).get("is_expanding")
    breadth_tp = _load_latest_breadth()
    hindenburg = calc_hindenburg(breadth_tp, sp500_up_vs_50d())
    hindenburg_active = None if hindenburg is None else hindenburg["active"]
    tp_checklist = calc_take_profit_checklist(
        fg_cnn_score, above_ma200, ma200_slope, hy_is_expanding, hindenburg_active
    )
    print(f"[INFO] TAKE PROFIT: triggered={tp_checklist['triggered']}, points={tp_checklist['points']}, action={tp_checklist['action']}")

    hy_current = (hy_spread_data or {}).get("current")
    hy_max_90d = (hy_spread_data or {}).get("max_90d")
    buy_checklist = calc_buy_checklist(
        fg_cnn_score, above_ma200, ma200_slope, hy_current, hy_max_90d, hindenburg_active
    )
    print(f"[INFO] BUY: triggered={buy_checklist['triggered']}, extreme={buy_checklist['extreme']}, points={buy_checklist['points']}, action={buy_checklist['action']}")

    asset_flow_data = collect_asset_flow()
    asset_flow_data = _fill_fallbacks(asset_flow_data, "asset_flow", _recent_entries)

    # MP-05（指示書㉓）: シグナルは日付で直近20営業日のスコアから判定し、JSONに記録する
    _now_utc = datetime.now(timezone.utc)
    try:
        _hist = [(e.get("date", "")[:10], (e.get("sentiment") or {}).get("score"))
                 for e in _load_recent_entries(JSON_PATH, limit=60)]
        sentiment_data["signal"] = sentiment_signal(sentiment_data.get("score"), _hist, signal_window_start(_now_utc))
    except Exception as e:
        print(f"[WARN] シグナル判定に失敗: {e}")

    # 8段階の結論・天気・data_quality（指示書㉓ 実装A）
    from stage_conclusions import build_stage_conclusions
    from common.market_data.reader import get_price_series_as_of as _md_series_as_of
    stage_result = build_stage_conclusions(
        structured_data, asset_flow_data, sentiment_data.get("breadth"), data_freshness.get("expected_close_date"),
        get_series=lambda sym, as_of, days: _md_series_as_of(sym, as_of, days=days))
    print(f"[INFO] 天気: {stage_result['weather']['label']} / data_quality: {stage_result['data_quality']['status']}")
    for _k, _v in stage_result["stages"].items():
        print(f"  段階{_k}: {_v.get('line')}")
    write_nyse_holidays(now_utc=_now_utc)

    # MP-28（指示書㉓）: AIの入力は確定した事実のJSONだけ（ニュースは渡さない）
    ai_facts = build_ai_facts(stage_result, structured_data, sentiment_data, fear_greed_data, tech_pulse_data,
                              asset_flow_data, calc_credit(structured_data, asset_flow_data))
    report, haiku = analyse_market(ai_facts)
    save_data_to_json_and_csv(report, structured_data, sentiment_data, fear_greed_data, tech_pulse_data, asset_flow_data,
                              tp_checklist, buy_checklist, data_freshness, stage_result=stage_result, haiku=haiku,
                              ai_facts=ai_facts, hindenburg=hindenburg)
    if GMAIL_USER and GMAIL_PASSWORD:
        send_email(report, sentiment_data)
    else:
        print("[INFO] メール送信スキップ（認証情報なし）")
