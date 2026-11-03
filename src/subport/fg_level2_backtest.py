"""
F&G第2水準戦略 バックテストスクリプト (Stage 1) v2
================================================
データ取得:
  1st: CNN公式API (production.dataviz.cnn.io) - 2022-02〜現在
  2nd: GitHub (whit3rabbit/fear-greed-data) - 2011〜現在の結合CSV
  株価: yfinance (QQQ)

使い方:
  python fg_level2_backtest.py --fetch        # データ取得
  python fg_level2_backtest.py                # バックテスト（利確10%, 30日）
  python fg_level2_backtest.py --all          # 全条件マトリクス
  python fg_level2_backtest.py --target 0.15 --hold 20  # 条件指定
"""

import argparse
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# 取得は common/external_api.py の web_get（[[COMMON-PATH-GUARD-1]]）。直接実行でも common を読めるように根を足す
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from common.external_api import web_get  # noqa: E402

warnings.filterwarnings("ignore")

# ============================================================
# 設定
# ============================================================
OUTPUT_DIR = Path(__file__).parent / "backtest_results"
FG_CSV     = Path(__file__).parent / "fg_historical.csv"
PRICE_CSV  = Path(__file__).parent / "qqq_prices.csv"

PRIMARY_TICKER = "QQQ"
TARGET_GAINS   = [0.10, 0.15, 0.20]
HOLD_DAYS_LIST = [10, 20, 30, 60]
COOLDOWN_DAYS  = 5

FG_ZONES = {
    "Extreme Fear (1-10)":  (1, 10),
    "Level2 Fear (11-20)":  (11, 20),   # ← 本戦略
    "Fear (21-40)":         (21, 40),
    "Neutral (41-60)":      (41, 60),
}

# ============================================================
# Step 1: F&Gデータ取得
# ============================================================

def fetch_fg_cnn_api() -> "pd.DataFrame | None":
    """CNN公式APIから取得 (production.dataviz.cnn.io)"""
    start_date = "2011-01-01"
    url = f"https://production.dataviz.cnn.io/index/fearandgreed/graphdata/{start_date}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://www.cnn.com/",
        "Origin": "https://www.cnn.com",
    }
    print(f"  [CNN API] {url}")
    try:
        resp = web_get(url, "CNN F&G", headers=headers, expect_json=True)
        resp.raise_for_status()
        data = resp.json()

        historical = data.get("fear_and_greed_historical", {}).get("data", [])
        if not historical:
            print("  CNN API: データなし")
            return None

        rows = []
        for pt in historical:
            ts    = int(pt["x"]) / 1000
            date  = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            score = int(pt["y"])
            rows.append({"date": date, "fg_score": score})

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
        print(f"  CNN API 成功: {len(df)}日分 ({df['date'].min().date()} 〜 {df['date'].max().date()})")
        return df

    except Exception as e:
        print(f"  CNN API 失敗: {e}")
        return None


def fetch_fg_github() -> "pd.DataFrame | None":
    """GitHub (whit3rabbit/fear-greed-data) からCSV取得 (2011〜現在)"""
    url = "https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/fear-greed.csv"
    print(f"  [GitHub] {url}")
    try:
        resp = web_get(url, "GitHub F&G CSV", headers={})
        resp.raise_for_status()

        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        df.columns = [c.lower().strip() for c in df.columns]

        date_col  = next((c for c in df.columns if "date" in c), None)
        score_col = next((c for c in df.columns if c in ("score", "value", "fear_greed", "fg")), None)
        if score_col is None:
            # 数値列を探す
            num_cols = df.select_dtypes(include="number").columns.tolist()
            score_col = num_cols[0] if num_cols else None

        if not date_col or not score_col:
            print(f"  列名不明: {list(df.columns)}")
            return None

        df = df[[date_col, score_col]].copy()
        df.columns = ["date", "fg_score"]
        df["date"]     = pd.to_datetime(df["date"], errors="coerce")
        df["fg_score"] = pd.to_numeric(df["fg_score"], errors="coerce")
        df = df.dropna().sort_values("date").drop_duplicates("date").reset_index(drop=True)
        print(f"  GitHub 成功: {len(df)}日分 ({df['date'].min().date()} 〜 {df['date'].max().date()})")
        return df

    except Exception as e:
        print(f"  GitHub 失敗: {e}")
        return None


def fetch_fg_data(save_path: Path) -> pd.DataFrame:
    """F&Gデータ取得（CNN API → GitHub フォールバック）"""
    print("[データ取得] F&Gヒストリカルデータ...")

    df = fetch_fg_cnn_api()

    if df is None or len(df) < 100:
        print("  → GitHub フォールバック...")
        df_git = fetch_fg_github()
        if df_git is not None:
            if df is not None and len(df) > 0:
                df = pd.concat([df_git, df]).drop_duplicates("date").sort_values("date").reset_index(drop=True)
            else:
                df = df_git

    if df is None or len(df) == 0:
        print("\n❌ F&Gデータ取得失敗。手動でCSVを用意してください。")
        print("   形式: date,fg_score (例: 2024-01-15,45)")
        print(f"   保存先: {save_path}")
        sys.exit(1)

    df.to_csv(save_path, index=False, encoding="utf-8")
    print(f"  保存: {save_path} ({len(df)}行)")
    return df


def fetch_price_data(ticker: str, start: str, end: str, save_path: Path) -> pd.DataFrame:
    """yfinanceで株価取得"""
    import yfinance as yf
    print(f"[データ取得] {ticker} 株価 ({start} 〜 {end})...")
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
    if df.empty:
        print(f"  ❌ {ticker} の株価取得失敗")
        sys.exit(1)
    df = df[["Close"]].copy()
    df.columns = [ticker]
    df.index = pd.to_datetime(df.index)
    df.to_csv(save_path, encoding="utf-8")
    print(f"  保存: {save_path} ({len(df)}行)")
    return df


# ============================================================
# Step 2: バックテスト本体
# ============================================================

def load_data(fg_path: Path, price_path: Path):
    fg = pd.read_csv(fg_path, parse_dates=["date"])
    fg.columns = [c.lower().strip() for c in fg.columns]
    score_col = next((c for c in fg.columns if "score" in c or "fg" in c), None)
    if score_col and score_col != "fg_score":
        fg = fg.rename(columns={score_col: "fg_score"})
    fg["fg_score"] = pd.to_numeric(fg["fg_score"], errors="coerce")
    fg = fg.dropna(subset=["date", "fg_score"]).sort_values("date").reset_index(drop=True)

    prices = pd.read_csv(price_path, index_col=0, parse_dates=True)

    print(f"F&G: {len(fg)}日分 ({fg['date'].min().date()} 〜 {fg['date'].max().date()})")
    print(f"株価: {len(prices)}日分 ({prices.index.min().date()} 〜 {prices.index.max().date()})")
    return fg, prices


def run_backtest(fg: pd.DataFrame, prices: pd.DataFrame,
                 ticker: str, zone: tuple, target_gain: float, hold_days: int) -> list:
    """
    バックテスト本体。
    エントリー: zone条件F&G日の終値で買い（翌日近似）
    エグジット: target_gain到達 or hold_days経過（早い方）
    クールダウン: COOLDOWN_DAYS日間は新規エントリーなし
    """
    if ticker not in prices.columns:
        return []

    price_series  = prices[ticker].dropna()
    trading_days  = price_series.index.tolist()
    fg_indexed    = fg.set_index("date")["fg_score"]
    trades        = []
    cooldown_until = pd.Timestamp("2000-01-01")

    for i, entry_date in enumerate(trading_days):
        if entry_date <= cooldown_until:
            continue
        if i + 1 >= len(trading_days):
            break

        # F&Gスコア確認（直近3日以内）
        fg_candidates = fg_indexed.index[fg_indexed.index <= entry_date]
        if len(fg_candidates) == 0:
            continue
        fg_date = fg_candidates[-1]
        if (entry_date - fg_date).days > 3:
            continue

        fg_val = fg_indexed[fg_date]
        if not (zone[0] <= fg_val <= zone[1]):
            continue

        entry_price  = price_series[entry_date]
        target_price = entry_price * (1 + target_gain)
        stop_idx     = min(i + hold_days, len(trading_days) - 1)
        stop_date    = trading_days[stop_idx]
        future       = price_series.iloc[i:stop_idx + 1]
        hit_mask     = future >= target_price

        if hit_mask.any():
            exit_date  = hit_mask[hit_mask].index[0]
            exit_price = price_series[exit_date]
            hit_target = True
        else:
            exit_date  = stop_date
            exit_price = price_series[stop_date]
            hit_target = False

        return_pct  = (exit_price - entry_price) / entry_price * 100
        hold_actual = (exit_date - entry_date).days

        trades.append({
            "entry_date":      entry_date.date(),
            "exit_date":       exit_date.date(),
            "fg_score":        int(fg_val),
            "entry_price":     round(float(entry_price), 2),
            "exit_price":      round(float(exit_price), 2),
            "return_pct":      round(float(return_pct), 2),
            "hold_days":       hold_actual,
            "hit_target":      hit_target,
            "target_gain_pct": target_gain * 100,
        })
        cooldown_until = exit_date + timedelta(days=COOLDOWN_DAYS)

    return trades


def summarize(trades: list, label: str) -> dict:
    if not trades:
        return {"label": label, "n": 0}
    df  = pd.DataFrame(trades)
    n   = len(df)
    pos = (df["return_pct"] > 0).sum()
    hit = df["hit_target"].sum()
    return {
        "label":           label,
        "n":               n,
        "win_rate":        round(pos / n * 100, 1),
        "target_hit_rate": round(hit / n * 100, 1),
        "avg_return":      round(df["return_pct"].mean(), 2),
        "median_return":   round(df["return_pct"].median(), 2),
        "avg_hold_days":   round(df["hold_days"].mean(), 1),
        "max_return":      round(df["return_pct"].max(), 2),
        "min_return":      round(df["return_pct"].min(), 2),
    }


# ============================================================
# Step 3: 出力
# ============================================================

def print_report(results: list, target_gain: float, hold_days: int, ticker: str):
    print(f"\n{'='*72}")
    print(f"F&G第2水準戦略 | {ticker} | 利確={target_gain*100:.0f}% | タイムアウト={hold_days}日")
    print(f"{'='*72}")
    print(f"{'ゾーン':<26} {'N':>4} {'勝率':>7} {'目標達成':>8} {'平均R':>7} {'中央値':>7} {'最大':>7} {'最小':>7}")
    print("-" * 72)
    for r in results:
        if r["n"] == 0:
            print(f"{r['label']:<26} {'0':>4}  (シグナルなし)")
            continue
        print(f"{r['label']:<26} {r['n']:>4} {r['win_rate']:>6.1f}% {r['target_hit_rate']:>7.1f}%"
              f" {r['avg_return']:>6.1f}% {r['median_return']:>6.1f}%"
              f" {r['max_return']:>6.1f}% {r['min_return']:>6.1f}%")
    print()


def save_results(all_results: list, all_trades: dict, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    valid = [r for r in all_results if r.get("n", 0) > 0]
    if valid:
        pd.DataFrame(valid).to_csv(output_dir / "summary.csv", index=False, encoding="utf-8-sig")
    rows = []
    for label, trades in all_trades.items():
        for t in trades:
            t["label"] = label
            rows.append(t)
    if rows:
        pd.DataFrame(rows).to_csv(output_dir / "trades.csv", index=False, encoding="utf-8-sig")
    print(f"[保存] {output_dir}/")


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="F&G第2水準戦略バックテスト v2")
    parser.add_argument("--fetch",     action="store_true")
    parser.add_argument("--fg-csv",    default=str(FG_CSV))
    parser.add_argument("--price-csv", default=str(PRICE_CSV))
    parser.add_argument("--ticker",    default=PRIMARY_TICKER)
    parser.add_argument("--target",    type=float, default=0.10)
    parser.add_argument("--hold",      type=int,   default=30)
    parser.add_argument("--all",       action="store_true")
    args = parser.parse_args()

    fg_path    = Path(args.fg_csv)
    price_path = Path(args.price_csv)

    if args.fetch:
        fg    = fetch_fg_data(fg_path)
        start = str(fg["date"].min().date())
        end   = datetime.today().strftime("%Y-%m-%d")
        fetch_price_data(args.ticker, start, end, price_path)
        print("\n✅ データ取得完了。バックテストは引数なしで実行してください。")
        return

    if not fg_path.exists() or not price_path.exists():
        missing = [str(p) for p in [fg_path, price_path] if not p.exists()]
        print(f"❌ CSVが見つかりません: {missing}")
        print("   まず --fetch を実行してください。")
        sys.exit(1)

    print("[読み込み中...]")
    fg, prices = load_data(fg_path, price_path)

    conditions  = [(t, h) for t in TARGET_GAINS for h in HOLD_DAYS_LIST] if args.all \
                  else [(args.target, args.hold)]
    all_results = []
    all_trades  = {}

    for target_gain, hold_days in conditions:
        results = []
        for zone_name, zone_range in FG_ZONES.items():
            trades = run_backtest(fg, prices, args.ticker, zone_range, target_gain, hold_days)
            s = summarize(trades, zone_name)
            s.update({"target_gain_pct": target_gain * 100, "hold_days_max": hold_days, "ticker": args.ticker})
            results.append(s)
            all_trades[f"{zone_name}|{target_gain*100:.0f}%|{hold_days}d"] = trades
            all_results.append(s)
        print_report(results, target_gain, hold_days, args.ticker)

    suffix = f"_{args.ticker}" + ("_all" if args.all else f"_t{int(args.target*100)}_h{args.hold}")
    save_results(all_results, all_trades, OUTPUT_DIR / f"fg_level2{suffix}")
    print("✅ 完了！")


if __name__ == "__main__":
    main()
