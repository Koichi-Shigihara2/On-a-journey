"""market_data.jsonの過去エントリで、原油・金の前日比が限月の乗り換えでゆがんだ可能性のある日を、前日比による判定から外す
一過性ツール（[[MARKETDATA-FUTURES-ROLL-1]]、指示書㉔）。

- 入力: scripts/analysis/futures_roll_suspects.py の結果（suspects: 同じ2日間のETF〈USO・GLD〉との前日比の差がしきい値超の日）
- 値（value・change_percent）はそのまま残し、indicators[キー]["roll_suspect"]に印（ETFの前日比・差・しきい値）を付ける。
  段階1・2の結論1行とタグは、印の付いた値を除いて計算し直す（stage_conclusions._indが印の付いた前日比を使わない）
- 限月が特定できる日（SAME_CONTRACT）は、同じ限月自身の終値で前日比を計算し直し、contract_rollに記録する
  （元の前日比はraw_change_percentに残す）。2026-09-18: 夕方の取得で10月限→11月限に切り替わった日（CLX26.NYM）

使い方: python src/market/market_pulse/backfill_futures_roll.py --suspects <JSON> [--dry-run]
"""
import argparse
import json
import os
import sys

_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)
import collect_and_send as cs  # noqa: E402
from stage_conclusions import stage1, stage2  # noqa: E402

# 同じ限月で計算し直せる日: {(指標キー, 日付): (乗り換え前の限月, 乗り換え後の限月)}
SAME_CONTRACT = {("WTI原油", "2026-09-18"): ("CLV26.NYM", "CLX26.NYM")}


def contract_closes(contract):
    import yfinance as yf
    h = yf.Ticker(contract).history(start="2026-09-01", auto_adjust=False)
    return {i.strftime("%Y-%m-%d"): float(v) for i, v in h["Close"].items() if v == v}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--suspects", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    sus = json.load(open(args.suspects, encoding="utf-8"))
    th = sus["threshold_pt"]
    marks = {(r["entry"], r["key"]): r for r in sus["suspects"]}
    with open(cs.JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    closes = {}
    changed = []
    for e in data:
        ind = e.get("indicators") or {}
        touched = False
        for key in ("WTI原油", "金（GOLD）"):
            v = ind.get(key)
            if not isinstance(v, dict):
                continue
            sc = SAME_CONTRACT.get((key, v.get("date")))
            if sc:
                old, new = sc
                if new not in closes:
                    closes[new] = contract_closes(new)
                cl = closes[new]
                prev = [d for d in sorted(cl) if d < v["date"]]
                if v["date"] in cl and prev:
                    cc, cp = cl[v["date"]], cl[prev[-1]]
                    v["contract_roll"] = {"from": old, "to": new, "method": "same_contract", "prev_date": prev[-1],
                                          "prev_close": round(cp, 4), "close": round(cc, 4),
                                          "raw_change_percent": v.get("change_percent"),
                                          "note": "過去エントリの再計算（2026-09-30）。closeは限月自身の清算値"}
                    v["contract_roll"]["raw_change"] = v.get("change")
                    v["change"] = round(cc - cp, 2)
                    v["change_percent"] = round((cc - cp) / cp * 100, 2)
                    v.pop("roll_suspect", None)
                    touched = True
                    continue
            m = marks.get((e["date"][:16], key))
            if m:
                v["roll_suspect"] = {"etf": m["etf"], "etf_change_percent": m["etf_pct"], "diff_pt": m["diff_pt"],
                                     "threshold_pt": th, "note": "限月乗り換えの可能性（前日比による判定から外す、2026-09-30）"}
                touched = True
        if touched and "stage_conclusions" in e:
            before = (e["stage_conclusions"]["1"].get("tags"), e["stage_conclusions"]["2"].get("line"))
            e["stage_conclusions"]["1"] = stage1(ind)
            e["stage_conclusions"]["2"] = stage2(ind)
            after = (e["stage_conclusions"]["1"].get("tags"), e["stage_conclusions"]["2"].get("line"))
            changed.append({"entry": e["date"][:16], "before": before, "after": after, "changed": before != after})
    for c in changed:
        print(("変化 " if c["changed"] else "同じ ") + c["entry"], c["before"], "→", c["after"])
    print(f"印を付けた／再計算したエントリ: {len(changed)}件、結論が変わったエントリ: {sum(c['changed'] for c in changed)}件")
    if args.dry_run:
        return
    with open(cs.JSON_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("保存しました")


if __name__ == "__main__":
    main()
