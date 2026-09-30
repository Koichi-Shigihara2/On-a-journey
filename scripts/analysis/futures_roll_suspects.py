"""Market Pulseの過去エントリで、原油（CL=F）・金（GC=F）の前日比が限月の乗り換えでゆがんだ可能性のある日を洗い出す
（指示書㉔の追加確認）。

Yahooは期限切れの限月を返さないため、同じ限月どうしの前日比を過去にさかのぼって計算できない。代わりに、
同じ商品に連動するETF（原油はUSO、金はGLD）の同じ2日間の前日比と比べ、差が大きい日を
「乗り換えの可能性がある日」とする。USOはyfinanceから読むだけ（daily/には保存しない）、GLDはdaily/。

使い方: python scripts/analysis/futures_roll_suspects.py --out <JSON> [--threshold 2.0]
"""
import argparse
import json
import os
from datetime import datetime

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MD = os.path.join(REPO, "docs", "market-monitor", "market-pulse", "data", "market_data.json")
PAIRS = {"WTI原油": ("CL=F", "USO"), "金（GOLD）": ("GC=F", "GLD")}


def etf_closes():
    import yfinance as yf
    uso = yf.Ticker("USO").history(start="2026-03-20", auto_adjust=False)["Close"]
    out = {"USO": {i.strftime("%Y-%m-%d"): float(v) for i, v in uso.items()}}
    recs = json.load(open(os.path.join(REPO, "common", "market_data", "daily", "GLD.json"), encoding="utf-8"))["records"]
    out["GLD"] = {r["date"]: r["close"] for r in recs if isinstance(r.get("close"), (int, float))}
    return out


def futures_prev_dates():
    """各エントリの前日比が使った2日間（daily/の先物の行の、指標の日付とその直前の行の日付）。"""
    out = {}
    for sym in ("CL=F", "GC=F"):
        recs = json.load(open(os.path.join(REPO, "common", "market_data", "daily", f"{sym}.json"), encoding="utf-8"))["records"]
        ds = [r["date"] for r in recs if isinstance(r.get("close"), (int, float))]
        out[sym] = {d: ds[i - 1] for i, d in enumerate(ds) if i}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--threshold", type=float, default=2.0)
    args = ap.parse_args()
    etf = etf_closes()
    prev = futures_prev_dates()
    md = json.load(open(MD, encoding="utf-8"))
    rows = []
    for e in md:
        for key, (fut, proxy) in PAIRS.items():
            v = (e.get("indicators") or {}).get(key)
            if not isinstance(v, dict) or v.get("change_percent") is None or not v.get("date"):
                continue
            d = v["date"]
            p = prev[fut].get(d)
            c = etf[proxy]
            if p is None or d not in c or p not in c:
                continue
            ec = (c[d] / c[p] - 1) * 100
            rows.append({"entry": e["date"][:16], "key": key, "date": d, "prev_date": p,
                         "futures_pct": v["change_percent"], "etf": proxy, "etf_pct": round(ec, 2),
                         "diff_pt": round(v["change_percent"] - ec, 2)})
    diffs = sorted(abs(r["diff_pt"]) for r in rows if r["key"] == "WTI原油")
    gd = sorted(abs(r["diff_pt"]) for r in rows if r["key"] == "金（GOLD）")
    q = lambda xs, p: xs[min(len(xs) - 1, int(len(xs) * p))] if xs else None
    sus = [r for r in rows if abs(r["diff_pt"]) > args.threshold]
    out = {"threshold_pt": args.threshold, "n_rows": len(rows),
           "abs_diff_quantiles": {"WTI原油": {p: q(diffs, p) for p in (0.5, 0.9, 0.95, 0.99)},
                                  "金（GOLD）": {p: q(gd, p) for p in (0.5, 0.9, 0.95, 0.99)}},
           "suspects": sus, "all": rows}
    json.dump(out, open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps({k: v for k, v in out.items() if k != "all"}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
