#!/usr/bin/env python3
"""
ポートフォリオ日次スナップショット生成
毎日JST 22:30実行
"""
import json, os
from datetime import datetime, timezone, timedelta
from pathlib import Path

JST = timezone(timedelta(hours=9))

def _nested_get(d, *keys):
    """ネストした辞書を安全に辿る。
    キーが存在しても値がNoneの場合、dict.get(key, default)のdefaultは効かず
    None.get(...)でAttributeErrorになるため、各段でdict判定してから辿る。"""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def main():
    # portfolio.jsonを読み込む
    pf_path = Path("docs/portfolio/data/portfolio.json")
    if not pf_path.exists():
        print("portfolio.json not found")
        return

    with open(pf_path, encoding="utf-8") as f:
        pf = json.load(f)

    # market_data.jsonからUSD/JPYと現在株価を取得
    mp_path = Path("docs/market-monitor/market-pulse/data/market_data.json")
    with open(mp_path, encoding="utf-8") as f:
        mp = json.load(f)

    last_mp = mp[-1]
    usdjpy = _nested_get(last_mp, "indicators", "ドル円", "value") or 150.0
    sp500  = _nested_get(last_mp, "indicators", "S&P500", "value")
    date_str = datetime.now(JST).strftime("%Y-%m-%d")

    # HypeCoreから現在株価を取得
    prices = {}
    hc_dir = Path("docs/value-monitor/hypecore/data")
    for f in hc_dir.glob("*_poc.json"):
        ticker = f.stem.replace("_poc", "")
        with open(f, encoding="utf-8") as fp:
            h = json.load(fp)
        monthly = h.get("monthly", [])
        if monthly:
            prices[ticker] = monthly[-1].get("price", 0)

    # ブローカー別・銘柄別に集計
    total_equity_usd = 0.0
    total_cash_usd   = 0.0
    total_cost_usd   = 0.0
    positions_snap   = {}
    broker_snap      = {}

    brokers = pf.get("brokers", {})
    for broker, bdata in brokers.items():
        cash = float(bdata.get("cash", 0))
        total_cash_usd += cash
        b_equity = 0.0
        b_cost   = 0.0

        for ticker, pos in bdata.get("positions", {}).items():
            shares   = float(pos.get("shares", 0))
            avg_cost = float(pos.get("avg_cost", 0))
            price    = prices.get(ticker, 0)

            cost  = shares * avg_cost
            value = shares * price
            pnl   = value - cost

            total_equity_usd += value
            total_cost_usd   += cost
            b_equity += value
            b_cost   += cost

            # 銘柄別集計（全ブローカー合算）
            if ticker not in positions_snap:
                positions_snap[ticker] = {
                    "shares": 0, "cost": 0, "value": 0, "price": price
                }
            positions_snap[ticker]["shares"] += shares
            positions_snap[ticker]["cost"]   += cost
            positions_snap[ticker]["value"]  += value

        broker_snap[broker] = {
            "cash_usd":   cash,
            "equity_usd": b_equity,
            "cost_usd":   b_cost,
            "pnl_usd":    b_equity - b_cost,
        }

    total_assets_usd = total_equity_usd + total_cash_usd
    total_pnl_usd    = total_equity_usd - total_cost_usd
    total_pnl_pct    = (total_pnl_usd / total_cost_usd * 100) if total_cost_usd else 0

    snapshot = {
        "date":             date_str,
        "usdjpy":           usdjpy,
        "total_assets_usd": total_assets_usd,
        "total_assets_jpy": total_assets_usd * usdjpy,
        "total_equity_usd": total_equity_usd,
        "total_equity_jpy": total_equity_usd * usdjpy,
        "total_cash_usd":   total_cash_usd,
        "total_cash_jpy":   total_cash_usd * usdjpy,
        "total_pnl_usd":    total_pnl_usd,
        "total_pnl_jpy":    total_pnl_usd * usdjpy,
        "total_pnl_pct":    total_pnl_pct,
        "brokers":          broker_snap,
        "positions":        positions_snap,
    }

    # history.jsonに追記
    hist_path = Path("docs/portfolio/data/history.json")
    if hist_path.exists():
        with open(hist_path, encoding="utf-8") as f:
            history = json.load(f)
    else:
        history = {"snapshots": []}

    # 同日のスナップショットがあれば上書き
    snaps = history.get("snapshots", [])
    snaps = [s for s in snaps if s.get("date") != date_str]
    snaps.append(snapshot)
    snaps.sort(key=lambda x: x["date"])
    history["snapshots"] = snaps

    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    print(f"スナップショット保存: {date_str}")
    print(f"  総資産: ${total_assets_usd:,.0f} / ¥{total_assets_usd*usdjpy:,.0f}")
    print(f"  株式: ${total_equity_usd:,.0f} / キャッシュ: ${total_cash_usd:,.0f}")
    print(f"  評価損益: ${total_pnl_usd:,.0f} ({total_pnl_pct:+.1f}%)")

if __name__ == "__main__":
    main()
