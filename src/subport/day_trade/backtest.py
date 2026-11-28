"""
Momentum Burst デイトレード戦略 v3.0 バックテスト
=================================================
yfinance で過去6ヶ月の5分足データを取得し、
Layer 0〜5 + エグジットロジックを全て再現して検証する。

実行:
  python backtest.py                          # デフォルト（QQQ/NVDA/TSLA/AAPL）
  python backtest.py --ticker NVDA --days 180
  python backtest.py --ticker QQQ --days 90 --no-short
  python backtest.py --all                    # config.jsonの全銘柄

出力:
  backtest_results/summary.csv    — 銘柄別・条件別サマリー
  backtest_results/trades.csv     — 全トレード詳細
  backtest_results/filter_stats.csv — フィルター有効性分析
"""

import argparse
import json
import warnings
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

CONFIG_PATH = Path(__file__).parent / "config.json"
OUTPUT_DIR  = Path(__file__).parent / "backtest_results"

ET = timezone(timedelta(hours=-4))   # 夏時間（EDT）


# ============================================================
# 設定読み込み
# ============================================================

def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# データ取得
# ============================================================

def fetch_data(ticker: str, days: int) -> pd.DataFrame:
    """5分足データ取得"""
    end   = datetime.now()
    start = end - timedelta(days=days)
    df = yf.download(ticker, start=start, end=end,
                     interval="5m", progress=False, auto_adjust=True)
    if df.empty:
        return df
    df.index = pd.to_datetime(df.index)
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("America/New_York")
    else:
        df.index = df.index.tz_convert("America/New_York")
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [c.lower() for c in df.columns]
    return df


# ============================================================
# テクニカル計算
# ============================================================

def calc_ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def calc_atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def calc_vwap(df: pd.DataFrame) -> pd.Series:
    """日次リセットのVWAP"""
    df = df.copy()
    df["date"] = df.index.date
    df["typical"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_vol"]  = df["typical"] * df["volume"]
    df["cum_tpv"] = df.groupby("date")["tp_vol"].cumsum()
    df["cum_vol"] = df.groupby("date")["volume"].cumsum()
    return df["cum_tpv"] / df["cum_vol"]


def calc_rvol(df: pd.DataFrame, n: int = 20) -> pd.Series:
    """相対出来高（当日同時刻の過去N日平均比）"""
    df = df.copy()
    df["time_of_day"] = df.index.time
    avg = df.groupby("time_of_day")["volume"].transform(
        lambda x: x.rolling(n, min_periods=1).mean().shift(1)
    )
    return df["volume"] / avg.replace(0, np.nan)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema9"]  = calc_ema(df["close"], 9)
    df["ema21"] = calc_ema(df["close"], 21)
    df["atr"]   = calc_atr(df)
    df["vwap"]  = calc_vwap(df)
    df["rvol"]  = calc_rvol(df)
    df["body"]  = (df["close"] - df["open"]).abs()
    df["wick"]  = df["high"] - df["low"]
    df["upper_wick"] = df["high"] - df[["open", "close"]].max(axis=1)
    return df


# ============================================================
# Layer 0：やらない日判定
# ============================================================

def is_skip_day(date: datetime.date, cfg: dict,
                vix_level: float, spy_gap: float,
                consecutive_losses: int, weekly_loss_pct: float,
                peak_balance: float, current_balance: float) -> tuple[bool, str]:
    l0 = cfg["layer0"]

    if vix_level >= l0["vix_max"]:
        return True, f"VIX過大({vix_level:.0f})"
    if abs(spy_gap) >= l0["spy_gap_pct_max"]:
        return True, f"SPYギャップ({spy_gap:+.1f}%)"
    if consecutive_losses >= l0["consecutive_loss_skip"]:
        return True, f"{consecutive_losses}連敗スキップ"
    if weekly_loss_pct >= l0["weekly_loss_pct_max"]:
        return True, f"週次損失上限({weekly_loss_pct:.1f}%)"
    dd = (peak_balance - current_balance) / peak_balance * 100
    if dd >= l0["max_drawdown_pct"]:
        return True, f"最大DD({dd:.1f}%)到達"
    if l0["skip_monday_open"] and date.weekday() == 0:
        return True, "月曜スキップ"

    return False, ""


# ============================================================
# Layer 1：トレンド日スコア
# ============================================================

def calc_trend_score(spy_bar, qqq_bar, vix_change: float,
                     premarket_gap: float, rvol: float,
                     sector_aligned: bool, yield_change_bp: float,
                     capital_flow_score: int, cfg: dict) -> tuple[int, list]:
    l1    = cfg["layer1"]
    score = 0
    reasons = []

    # ① SPY
    if spy_bar["ema9"] > spy_bar["ema21"] and spy_bar["close"] > spy_bar["vwap"]:
        score += 1; reasons.append("SPY↑")

    # ② QQQ
    if qqq_bar["ema9"] > qqq_bar["ema21"] and qqq_bar["close"] > qqq_bar["vwap"]:
        score += 1; reasons.append("QQQ↑")

    # ③ VIX
    if vix_change <= l1["vix_drop_threshold"]:
        score += 1; reasons.append(f"VIX低下{vix_change:.1f}%")
    elif vix_change >= l1["vix_spike_threshold"]:
        score -= 1; reasons.append(f"VIX急騰{vix_change:.1f}% ⚠️")

    # ④ プレマーケットギャップ
    if abs(premarket_gap) >= l1["premarket_gap_min"]:
        score += 1; reasons.append(f"ギャップ{premarket_gap:+.1f}%")

    # ⑤ RVOL
    if rvol >= l1["rvol_min"]:
        score += 1; reasons.append(f"RVOL{rvol:.1f}x")

    # ⑥ セクターETF
    if sector_aligned:
        score += 1; reasons.append("セクター一致")

    # ⑦ 10年債
    if abs(yield_change_bp) < l1["bond_yield_stable_bp"]:
        score += 1; reasons.append("金利安定")
    elif yield_change_bp > l1["bond_yield_spike_bp"]:
        score -= 1; reasons.append("金利急騰 ⚠️")

    # ⑧⑨ 大口資金（バックテストでは簡略化・ランダムシミュレート）
    score += capital_flow_score

    return score, reasons


def get_trend_multiplier(score: int, cfg: dict) -> float:
    mult_map = cfg["layer1"]["trend_multipliers"]
    key = str(min(score, 9))
    return mult_map.get(key, 0.0)


# ============================================================
# Layer 4：ORBロジック
# ============================================================

def find_orb(day_bars: pd.DataFrame, cfg: dict,
             prev_day_bars: "pd.DataFrame | None" = None) -> "dict | None":
    """9:30〜9:44の15分でORBを計算。atrは前日の値幅を使用。"""
    l4 = cfg["layer4"]
    market_open = time(9, 30)
    orb_end     = time(9, 44)

    orb_bars = day_bars[
        (day_bars.index.time >= market_open) &
        (day_bars.index.time <= orb_end)
    ]
    if len(orb_bars) < 2:
        return None

    orb_high = orb_bars["high"].max()
    orb_low  = orb_bars["low"].min()
    orb_size = orb_high - orb_low

    # ATR: 前日の値幅を使用（前日データがなければ当日で代替）
    if prev_day_bars is not None and not prev_day_bars.empty:
        prev_high = prev_day_bars["high"].max()
        prev_low  = prev_day_bars["low"].min()
        atr_val   = prev_high - prev_low
    else:
        # フォールバック: 当日の値幅
        atr_val = day_bars["high"].max() - day_bars["low"].min()

    if atr_val <= 0:
        return None

    # レンジサイズフィルター
    if orb_size < atr_val * l4["orb_size_atr_min"]:
        return None
    if orb_size > atr_val * l4["orb_size_atr_max"]:
        return None

    return {"high": orb_high, "low": orb_low, "size": orb_size, "atr": atr_val}


def find_orb_entry(day_bars: pd.DataFrame, orb: dict,
                   direction: str, cfg: dict) -> dict | None:
    """
    ORB確認足エントリーを探す。
    3ステップ: 第1ブレイク → プルバック → 再ブレイク
    """
    l4 = cfg["layer4"]
    buf = l4["breakout_buffer"]

    entry_start = time(9, 45)
    entry_end   = time(10, 30)

    bars = day_bars[day_bars.index.time >= entry_start].copy()

    state = "WAIT_FIRST"
    first_break_time  = None
    pullback_time     = None

    for idx, bar in bars.iterrows():
        t = idx.time()
        if t >= entry_end:
            break

        if direction == "long":
            threshold = orb["high"] * (1 + buf)
            pullback_cond  = bar["close"] < orb["high"]
            rebreak_cond   = bar["close"] > threshold

            if state == "WAIT_FIRST":
                if bar["close"] > threshold:
                    state = "WAIT_PULLBACK"
                    first_break_time = idx

            elif state == "WAIT_PULLBACK":
                elapsed = (idx - first_break_time).seconds / 60
                if pullback_cond:
                    state = "WAIT_REBREAK"
                    pullback_time = idx
                elif elapsed > l4["pullback_timeout_min"]:
                    state = "WAIT_FIRST"  # リセット

            elif state == "WAIT_REBREAK":
                elapsed = (idx - pullback_time).seconds / 60
                if rebreak_cond:
                    return {"bar": bar, "idx": idx, "orb": orb}
                elif elapsed > l4["rebreak_timeout_min"]:
                    state = "WAIT_FIRST"  # リセット

        else:  # short
            threshold = orb["low"] * (1 - buf)
            pullback_cond  = bar["close"] > orb["low"]
            rebreak_cond   = bar["close"] < threshold

            if state == "WAIT_FIRST":
                if bar["close"] < threshold:
                    state = "WAIT_PULLBACK"
                    first_break_time = idx

            elif state == "WAIT_PULLBACK":
                elapsed = (idx - first_break_time).seconds / 60
                if pullback_cond:
                    state = "WAIT_REBREAK"
                    pullback_time = idx
                elif elapsed > l4["pullback_timeout_min"]:
                    state = "WAIT_FIRST"

            elif state == "WAIT_REBREAK":
                elapsed = (idx - pullback_time).seconds / 60
                if rebreak_cond:
                    return {"bar": bar, "idx": idx, "orb": orb}
                elif elapsed > l4["rebreak_timeout_min"]:
                    state = "WAIT_FIRST"

    return None


# ============================================================
# Layer 5：エントリー品質フィルター
# ============================================================

def check_entry_quality(entry_bar, prev_bars: pd.DataFrame,
                        direction: str, vwap: float,
                        avg_volume: float, spy_direction: str,
                        orb_high: float, orb_low: float,
                        cfg: dict) -> tuple[bool, str]:
    l5 = cfg["layer5"]
    c  = entry_bar

    body        = c["body"]
    wick        = c["wick"]
    upper_wick  = c["upper_wick"]
    vol         = c["volume"]

    # ① ヒゲフィルター
    if body > 0 and wick > body * l5["wick_body_ratio_max"]:
        return False, f"ヒゲ過大({wick/body:.1f}x)"
    if direction == "long" and body > 0 and upper_wick > body * l5["upper_wick_body_ratio"]:
        return False, f"上ヒゲ過大({upper_wick/body:.1f}x)"

    # ② 出来高継続性
    if avg_volume > 0 and vol < avg_volume * l5["breakout_volume_ratio"]:
        return False, f"出来高不足({vol/avg_volume:.1f}x)"
    if len(prev_bars) >= 2:
        prev_vols = prev_bars["volume"].iloc[-2:].values
        if all(v < avg_volume for v in prev_vols):
            return False, "フォロースルーなし"

    # ③ 指数一致
    if direction == "long" and spy_direction == "down":
        return False, "SPY逆行"
    if direction == "short" and spy_direction == "up":
        return False, "SPY逆行"

    # ④ 実体方向
    if direction == "long" and c["close"] < c["open"]:
        return False, "陰線ブレイク"
    if direction == "short" and c["close"] > c["open"]:
        return False, "陽線ブレイク"

    # ⑤ VWAP位置
    if direction == "long" and c["close"] < vwap:
        return False, "VWAP以下"
    if direction == "short" and c["close"] > vwap:
        return False, "VWAP以上"

    # ⑥ 飛びつき防止
    if direction == "long":
        dist = (c["close"] - orb_high) / orb_high
    else:
        dist = (orb_low - c["close"]) / orb_low

    if dist > l5["orb_distance_max_pct"] / 100:
        return False, f"ORB乖離{dist*100:.1f}%"

    return True, "OK"


# ============================================================
# エグジットシミュレーション
# ============================================================

def simulate_exit(entry_price: float, direction: str,
                  atr: float, post_bars: pd.DataFrame,
                  cfg: dict) -> dict:
    ex = cfg["exit"]
    atr_pct = atr / entry_price

    target1 = entry_price * (1 + ex["target1_atr_mult"] * atr_pct) \
              if direction == "long" else \
              entry_price * (1 - ex["target1_atr_mult"] * atr_pct)
    target2 = entry_price * (1 + ex["target2_atr_mult"] * atr_pct) \
              if direction == "long" else \
              entry_price * (1 - ex["target2_atr_mult"] * atr_pct)
    stop    = entry_price * (1 - ex["stop_atr_mult"] * atr_pct) \
              if direction == "long" else \
              entry_price * (1 + ex["stop_atr_mult"] * atr_pct)

    partial_done = False
    be_stop      = stop      # ブレークイーブン後の損切
    force_time   = time(14, 30)
    avg_vol      = post_bars["volume"].mean()

    exit_price  = None
    exit_reason = None
    exit_time   = None

    for idx, bar in post_bars.iterrows():
        t = idx.time()

        # 強制決済
        if t >= force_time:
            exit_price  = bar["close"]
            exit_reason = "タイムアウト"
            exit_time   = idx
            break

        price = bar["close"]

        # 損切
        if (direction == "long" and price <= be_stop) or \
           (direction == "short" and price >= be_stop):
            exit_price  = be_stop
            exit_reason = "損切" if be_stop == stop else "BE損切"
            exit_time   = idx
            break

        # 第1利確
        if not partial_done:
            hit1 = (direction == "long" and price >= target1) or \
                   (direction == "short" and price <= target1)
            if hit1:
                partial_done = True
                be_stop = entry_price  # 損切をBEに引き上げ

        # 第2利確
        hit2 = (direction == "long" and price >= target2) or \
               (direction == "short" and price <= target2)
        if hit2:
            exit_price  = target2
            exit_reason = "第2利確"
            exit_time   = idx
            break

        # 時間フィルター（30分で勢いなし）
        elapsed = (idx - post_bars.index[0]).seconds / 60
        ret = (price - entry_price) / entry_price
        if direction == "short":
            ret = -ret
        R = ex["target1_atr_mult"] * atr_pct
        if elapsed >= ex["time_filter_minutes"] and ret < ex["time_filter_r_min"] * R:
            exit_price  = price
            exit_reason = "30分ルール"
            exit_time   = idx
            break

        # VWAP割れ
        vwap = bar.get("vwap", np.nan)
        if not pd.isna(vwap):
            if direction == "long" and price < vwap:
                exit_price  = price
                exit_reason = "VWAP割れ"
                exit_time   = idx
                break
            if direction == "short" and price > vwap:
                exit_price  = price
                exit_reason = "VWAP回復"
                exit_time   = idx
                break

        # 出来高フェード
        recent_vols = post_bars.loc[:idx]["volume"].iloc[-3:]
        if len(recent_vols) >= 3:
            if recent_vols.mean() < avg_vol * ex["volume_fade_ratio"]:
                exit_price  = price
                exit_reason = "出来高フェード"
                exit_time   = idx
                break

    # 未決済はタイムアウト
    if exit_price is None:
        last = post_bars.iloc[-1]
        exit_price  = last["close"]
        exit_reason = "タイムアウト"
        exit_time   = post_bars.index[-1]

    return_pct = (exit_price - entry_price) / entry_price * 100
    if direction == "short":
        return_pct = -return_pct

    # スリッページ適用
    slippage = cfg["risk"]["slippage_pct"]
    return_pct -= slippage

    return {
        "exit_price":   round(exit_price, 4),
        "exit_reason":  exit_reason,
        "exit_time":    exit_time,
        "return_pct":   round(return_pct, 3),
        "target1":      round(target1, 4),
        "target2":      round(target2, 4),
        "stop":         round(stop, 4),
    }


# ============================================================
# performance_mult 計算
# ============================================================

def calc_performance_mult(trades_so_far: list, cfg: dict) -> float:
    pc = cfg["performance"]
    if len(trades_so_far) < 3:
        return 1.0

    df = pd.DataFrame(trades_so_far)
    recent = df.tail(pc["lookback_trades"])
    win_rate = (recent["return_pct"] > 0).mean()

    # 月次成長率（簡略：直近全トレードのトータルリターン）
    total_ret = df["return_pct"].sum() / 100
    monthly_growth = total_ret / max(1, len(df) / 20)

    # ドローダウン（簡略計算）
    balance = 1.0
    peak    = 1.0
    max_dd  = 0.0
    for r in df["return_pct"]:
        balance *= (1 + r / 100)
        peak = max(peak, balance)
        dd = (peak - balance) / peak
        max_dd = max(max_dd, dd)

    # 勝率係数
    wr_map = {0.70: 1.30, 0.60: 1.10, 0.50: 1.00, 0.40: 0.80, 0.00: 0.60}
    perf_wr = 0.60
    for threshold, mult in sorted(wr_map.items()):
        if win_rate >= threshold:
            perf_wr = mult

    # 月次成長係数
    growth_map = {0.05: 1.20, 0.02: 1.10, 0.00: 1.00, -0.03: 0.85, -9.99: 0.70}
    perf_growth = 0.70
    for threshold, mult in sorted(growth_map.items()):
        if monthly_growth >= threshold:
            perf_growth = mult

    # ドローダウン係数
    dd_map = {0.03: 1.00, 0.05: 0.85, 0.08: 0.65, 0.10: 0.40, 9.99: 0.00}
    perf_dd = 0.00
    for threshold, mult in sorted(dd_map.items()):
        if max_dd <= threshold:
            perf_dd = mult
            break

    result = perf_wr * perf_growth * perf_dd
    return round(max(pc["perf_mult_min"], min(pc["perf_mult_max"], result)), 3)


# ============================================================
# メインバックテストループ
# ============================================================

def run_backtest(ticker: str, days: int, allow_short: bool,
                 cfg: dict) -> tuple[list, dict]:
    print(f"\n[{ticker}] データ取得中（{days}日）...")

    df     = fetch_data(ticker, days)
    spy_df = fetch_data("SPY", days)
    qqq_df = fetch_data("QQQ", days)

    if df.empty or spy_df.empty:
        print(f"  データ取得失敗")
        return [], {}

    df      = add_indicators(df)
    spy_df  = add_indicators(spy_df)
    qqq_df  = add_indicators(qqq_df)

    # VIX・10年債を日足で取得（実データ）
    end   = datetime.now()
    start = end - timedelta(days=days + 10)
    vix_daily = yf.download("^VIX", start=start, end=end,
                             interval="1d", progress=False, auto_adjust=True)
    tny_daily = yf.download("^TNX", start=start, end=end,
                             interval="1d", progress=False, auto_adjust=True)

    # 日足DataFrameをdate→値のdictに変換
    def to_date_dict(daily_df, col="Close"):
        if daily_df.empty:
            return {}
        daily_df = daily_df.copy()
        daily_df.index = pd.to_datetime(daily_df.index).date
        if isinstance(daily_df.columns, pd.MultiIndex):
            vals = daily_df.iloc[:, 0]
        else:
            vals = daily_df[col] if col in daily_df.columns else daily_df.iloc[:, 0]
        return vals.to_dict()

    vix_dict = to_date_dict(vix_daily)
    tny_dict = to_date_dict(tny_daily)

    avg_volume = df["volume"].rolling(20).mean()

    trades = []
    skips  = []
    filter_stats = {
        "ヒゲ過大": 0, "出来高不足": 0, "フォロースルーなし": 0,
        "SPY逆行": 0, "陰線ブレイク": 0, "陽線ブレイク": 0,
        "VWAP以下": 0, "VWAP以上": 0, "ORB乖離": 0,
    }

    # 状態変数
    balance           = cfg["base"]["subport_usd"]
    peak_balance      = balance
    consecutive_loss  = 0
    weekly_loss       = 0.0
    week_start        = None

    trading_days = sorted(set(df.index.date))
    print(f"  取引日数: {len(trading_days)}")
    skip_summary = {}  # スキップ理由の集計用

    for day in trading_days:
        day_bars = df[df.index.date == day].copy()
        if len(day_bars) < 10:
            continue

        # 週次リセット
        week_num = pd.Timestamp(day).isocalendar()[1]
        if week_start != week_num:
            week_start   = week_num
            weekly_loss  = 0.0

        # SPY/QQQの当日バー
        spy_day = spy_df[spy_df.index.date == day]
        qqq_day = qqq_df[qqq_df.index.date == day]
        if spy_day.empty or qqq_day.empty:
            continue

        spy_945 = spy_day[spy_day.index.time >= time(9, 45)]
        qqq_945 = qqq_day[qqq_day.index.time >= time(9, 45)]
        if spy_945.empty or qqq_945.empty:
            continue

        spy_bar = spy_945.iloc[0]
        qqq_bar = qqq_945.iloc[0]

        # 前日比
        prev_days = [d for d in trading_days if d < day]
        if not prev_days:
            continue
        prev_day_bars = df[df.index.date == prev_days[-1]]
        if prev_day_bars.empty:
            continue
        prev_close = prev_day_bars["close"].iloc[-1]
        today_open = day_bars["open"].iloc[0]
        spy_gap = (today_open - prev_close) / prev_close * 100

        # VIX変化率（実データ）
        prev_vix = vix_dict.get(prev_days[-1]) if prev_days else None
        today_vix = vix_dict.get(day)
        if prev_vix and today_vix and prev_vix > 0:
            vix_change = (today_vix - prev_vix) / prev_vix * 100
            vix_level  = today_vix
        else:
            vix_change = 0.0
            vix_level  = 20.0  # フォールバック

        # 10年債利回り変化（実データ・bp単位）
        prev_tny = tny_dict.get(prev_days[-1]) if prev_days else None
        today_tny = tny_dict.get(day)
        if prev_tny and today_tny:
            yield_change = (today_tny - prev_tny) * 100  # % → bp
        else:
            yield_change = 0.0

        # 大口資金フロー（バックテストでは引き続き簡略化）
        cap_flow = np.random.choice([-1, 0, 0, 1], p=[0.1, 0.4, 0.4, 0.1])

        # Layer 0
        skip, skip_reason = is_skip_day(
            day, cfg, vix_level, spy_gap,
            consecutive_loss,
            weekly_loss / balance * 100 if balance > 0 else 0,
            peak_balance, balance
        )
        if skip:
            skips.append({"date": day, "reason": skip_reason})
            skip_summary[skip_reason] = skip_summary.get(skip_reason, 0) + 1
            continue

        # ORB計算（前日データを渡す）
        prev_day_bars_data = df[df.index.date == prev_days[-1]] if prev_days else None
        orb = find_orb(day_bars, cfg, prev_day_bars_data)
        if orb is None:
            skips.append({"date": day, "reason": "ORBサイズNG"})
            skip_summary["ORBサイズNG"] = skip_summary.get("ORBサイズNG", 0) + 1
            continue

        # direction 決定（バックテストでは市場環境で判断）
        spy_up = spy_bar["ema9"] > spy_bar["ema21"]
        qqq_up = qqq_bar["ema9"] > qqq_bar["ema21"]

        if spy_up and qqq_up:
            direction = "long"
        elif not spy_up and not qqq_up and allow_short:
            direction = "short"
        else:
            skips.append({"date": day, "reason": "市場方向不一致"})
            skip_summary["市場方向不一致"] = skip_summary.get("市場方向不一致", 0) + 1
            continue

        # Layer 1 スコア
        spy_direction = "up" if spy_up else "down"
        premarket_gap = spy_gap
        rvol_val = day_bars["rvol"].iloc[3] if len(day_bars) > 3 else 1.0
        if pd.isna(rvol_val):
            rvol_val = 1.0

        sector_aligned = True  # バックテストでは簡略化
        score, reasons = calc_trend_score(
            spy_bar, qqq_bar, vix_change, premarket_gap,
            rvol_val, sector_aligned, yield_change, cap_flow, cfg
        )

        trend_mult = get_trend_multiplier(score, cfg)
        if trend_mult == 0:
            reason = f"スコア不足(score={score})"
            skips.append({"date": day, "reason": reason})
            skip_summary[reason] = skip_summary.get(reason, 0) + 1
            continue

        # AI確信度（バックテストでは簡略化）
        confidence = np.random.randint(55, 95)
        ai_mult_map = cfg["ai_multipliers"]
        ai_mult = 0.0
        for threshold in sorted([int(k) for k in ai_mult_map.keys()], reverse=True):
            if confidence >= threshold:
                ai_mult = ai_mult_map[str(threshold)]
                break
        if ai_mult == 0:
            reason = f"確信度不足({confidence})"
            skips.append({"date": day, "reason": reason})
            skip_summary[reason] = skip_summary.get(reason, 0) + 1
            continue

        if direction == "short" and confidence < cfg["layer3"]["confidence_min_short"]:
            reason = f"Short確信度不足({confidence})"
            skips.append({"date": day, "reason": reason})
            skip_summary[reason] = skip_summary.get(reason, 0) + 1
            continue

        # ORB確認足エントリー探索
        entry_info = find_orb_entry(day_bars, orb, direction, cfg)
        if entry_info is None:
            skips.append({"date": day, "reason": "ORBシグナルなし"})
            skip_summary["ORBシグナルなし"] = skip_summary.get("ORBシグナルなし", 0) + 1
            continue

        entry_bar = entry_info["bar"]
        entry_idx = entry_info["idx"]
        entry_price = entry_bar["close"]
        atr_val = orb["atr"]

        # Layer 5 エントリー品質
        bars_before = day_bars[day_bars.index < entry_idx]
        avg_vol = avg_volume[entry_idx] if entry_idx in avg_volume.index else day_bars["volume"].mean()
        vwap_val = entry_bar.get("vwap", entry_price)
        if pd.isna(vwap_val):
            vwap_val = entry_price

        ok, fail_reason = check_entry_quality(
            entry_bar, bars_before, direction,
            vwap_val, avg_vol, spy_direction,
            orb["high"], orb["low"], cfg
        )
        if not ok:
            for key in filter_stats:
                if key in fail_reason:
                    filter_stats[key] += 1
                    break
            skips.append({"date": day, "reason": f"品質NG:{fail_reason}"})
            continue

        # performance_mult
        perf_mult = calc_performance_mult(trades, cfg)

        # ポジションサイズ
        base   = cfg["base"]["base_amount_usd"]
        invest = min(base * trend_mult * ai_mult * perf_mult,
                     cfg["base"]["max_amount_usd"])
        invest = min(invest, balance)
        qty    = int(invest / entry_price)
        if qty <= 0:
            skips.append({"date": day, "reason": "株数0"})
            continue

        # エグジットシミュレーション
        post_bars = day_bars[day_bars.index > entry_idx].copy()
        post_bars["vwap"] = day_bars.loc[day_bars.index > entry_idx, "vwap"]
        if post_bars.empty:
            skips.append({"date": day, "reason": "後続データなし"})
            continue

        result = simulate_exit(entry_price, direction, atr_val, post_bars, cfg)

        pnl = result["return_pct"] / 100 * invest
        balance += pnl
        peak_balance = max(peak_balance, balance)
        weekly_loss  = min(0, weekly_loss + min(pnl, 0))

        if result["return_pct"] < 0:
            consecutive_loss += 1
        else:
            consecutive_loss = 0

        trade = {
            "date":           str(day),
            "ticker":         ticker,
            "direction":      direction,
            "trend_score":    score,
            "score_breakdown": "/".join(reasons),
            "ai_confidence":  confidence,
            "trend_mult":     trend_mult,
            "ai_mult":        ai_mult,
            "perf_mult":      perf_mult,
            "invest_usd":     round(invest, 2),
            "qty":            qty,
            "entry_price":    round(entry_price, 4),
            "exit_price":     result["exit_price"],
            "return_pct":     result["return_pct"],
            "pnl_usd":        round(pnl, 2),
            "exit_reason":    result["exit_reason"],
            "orb_size":       round(orb["size"], 4),
            "atr":            round(atr_val, 4),
            "balance":        round(balance, 2),
        }
        trades.append(trade)

    print(f"  スキップ理由: {dict(sorted(skip_summary.items(), key=lambda x: -x[1]))}")
    return trades, filter_stats


# ============================================================
# 結果集計・出力
# ============================================================

def summarize(trades: list, ticker: str) -> dict:
    if not trades:
        return {"ticker": ticker, "n": 0}
    df = pd.DataFrame(trades)
    n  = len(df)
    wins = (df["return_pct"] > 0).sum()

    return {
        "ticker":        ticker,
        "n":             n,
        "win_rate":      round(wins / n * 100, 1),
        "avg_return":    round(df["return_pct"].mean(), 2),
        "median_return": round(df["return_pct"].median(), 2),
        "total_pnl":     round(df["pnl_usd"].sum(), 2),
        "max_return":    round(df["return_pct"].max(), 2),
        "min_return":    round(df["return_pct"].min(), 2),
        "exit_reasons":  df["exit_reason"].value_counts().to_dict(),
        "score_win_rate": df.groupby("trend_score")["return_pct"].apply(
            lambda x: round((x > 0).mean() * 100, 1)
        ).to_dict(),
    }


def print_summary(s: dict):
    if s["n"] == 0:
        print(f"  [{s['ticker']}] トレードなし")
        return
    print(f"\n{'='*60}")
    print(f"[{s['ticker']}] {s['n']}トレード")
    print(f"  勝率        : {s['win_rate']}%")
    print(f"  平均R       : {s['avg_return']}%")
    print(f"  中央値      : {s['median_return']}%")
    print(f"  合計損益    : ${s['total_pnl']:+.2f}")
    print(f"  最大/最小   : {s['max_return']}% / {s['min_return']}%")
    print(f"  スコア別勝率: {s['score_win_rate']}")
    print(f"  決済理由    : {s['exit_reasons']}")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Momentum Burst バックテスト v3.0")
    parser.add_argument("--ticker",    default=None)
    parser.add_argument("--days",      type=int, default=55)
    parser.add_argument("--no-short",  action="store_true")
    parser.add_argument("--all",       action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    OUTPUT_DIR.mkdir(exist_ok=True)

    tickers = cfg["tickers_backtest"] if args.all else \
              [args.ticker] if args.ticker else cfg["tickers_backtest"]
    allow_short = not args.no_short

    all_trades  = []
    all_summary = []

    for ticker in tickers:
        trades, filter_stats = run_backtest(ticker, args.days, allow_short, cfg)
        s = summarize(trades, ticker)
        print_summary(s)
        all_trades.extend(trades)
        all_summary.append(s)

        if filter_stats:
            print(f"  フィルター除外: {filter_stats}")

    # CSV保存
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        trades_df.to_csv(OUTPUT_DIR / "trades.csv", index=False, encoding="utf-8-sig")

    summary_rows = [{k: v for k, v in s.items()
                     if k not in ("exit_reasons", "score_win_rate")}
                    for s in all_summary if s["n"] > 0]
    if summary_rows:
        pd.DataFrame(summary_rows).to_csv(
            OUTPUT_DIR / "summary.csv", index=False, encoding="utf-8-sig"
        )

    print(f"\n[保存] {OUTPUT_DIR}/")
    print("✅ バックテスト完了")


if __name__ == "__main__":
    main()
