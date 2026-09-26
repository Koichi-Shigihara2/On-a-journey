"""
TANUKI SCORE 判定実績検証 (ACTION-2)
score_history.json の各エントリに 30日/60日/90日後リターンを書き込む。

使い方:
  python src/value/tanuki_valuation/score_verifier.py            # 全銘柄
  python src/value/tanuki_valuation/score_verifier.py --ticker NVDA
"""

import argparse
import json
import os
import sys
from datetime import date as dt_date, datetime, timedelta


def _repo_root() -> str:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))


def _data_dir() -> str:
    return os.path.join(_repo_root(), "docs", "value-monitor", "tanuki_valuation", "data")


# common/ を import できるように repo root を sys.path に追加
_repo = _repo_root()
if _repo not in sys.path:
    sys.path.insert(0, _repo)

from common.market_data import reader as _market_data_reader  # noqa: E402
from common.sec_data import tickers as _tickers_mod  # noqa: E402


def fetch_price_after(ticker: str, base_date: str, days: int) -> float | None:
    """base_date から days 日後の終値を返す。未経過 or 取得失敗なら None。

    [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5-2でyfinance直接呼び出し
    から`reader.get_price_on_or_after()`経由に切替。target日以降5営業日
    ウィンドウで先頭値を採用するクエリ形状自体は変更していない。
    """
    try:
        base   = datetime.strptime(base_date, "%Y-%m-%d").date()
        target = base + timedelta(days=days)
        if target > dt_date.today():
            return None
        record = _market_data_reader.get_price_on_or_after(ticker, target.isoformat())
        if record is None or record.get("close") is None:
            return None
        return float(record["close"])
    except Exception as e:
        print(f"  [{ticker}] price fetch error ({days}d after {base_date}): {e}", file=sys.stderr)
        return None


def verify_ticker(ticker: str, data_dir: str) -> int:
    history_path = os.path.join(data_dir, ticker, "score_history.json")
    if not os.path.exists(history_path):
        return 0

    with open(history_path, encoding="utf-8") as f:
        entries: list[dict] = json.load(f)

    updated = 0
    for entry in entries:
        if entry.get("invalid"):
            continue  # 価格欠損で無効と印を付けた日（2026-09-26、MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1）
        base_date = entry.get("date")
        base_price = entry.get("price_at_judgment")
        if not base_date or not base_price:
            continue

        changed = False
        for days, key in [(30, "return_30d"), (60, "return_60d"), (90, "return_90d")]:
            if entry.get(key) is not None:
                continue
            price_n = fetch_price_after(ticker, base_date, days)
            if price_n is not None:
                entry[key] = round((price_n - base_price) / base_price * 100, 2)
                changed = True
            else:
                entry.setdefault(key, None)

        if changed:
            updated += 1
            print(f"  [{ticker}] {base_date}: "
                  f"30d={entry.get('return_30d')} "
                  f"60d={entry.get('return_60d')} "
                  f"90d={entry.get('return_90d')}")

    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)

    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description="TANUKI SCORE 判定実績検証")
    parser.add_argument("--ticker", help="単体銘柄（省略時: 全銘柄）")
    args = parser.parse_args()

    data_dir = _data_dir()

    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        # FLAG-CONSUMER-AUDIT-2: 以前はos.listdir(data_dir)で
        # tanukiフラグを見ずディレクトリ実在だけでスキャン対象を決めており、
        # tanuki=false化済み銘柄（ZS・RKLB等）のscore_history.jsonも
        # 更新対象に混入していた。tanuki=true銘柄に限定する。
        # 既存のscore_history.json自体は削除しない（過去実績データのため）。
        tanuki_set = set(_tickers_mod.get_tanuki_tickers())
        tickers = sorted(
            d for d in os.listdir(data_dir)
            if os.path.isdir(os.path.join(data_dir, d)) and d in tanuki_set
        )

    print(f"Verifying {len(tickers)} ticker(s)...")
    total_updates = sum(verify_ticker(t, data_dir) for t in tickers)
    print(f"Done. {total_updates} entries updated.")


if __name__ == "__main__":
    main()
