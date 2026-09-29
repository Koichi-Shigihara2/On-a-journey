"""market_data.jsonの既存エントリに、8段階の結論・天気・data_quality・シグナルを付け足す一過性ツール
（指示書㉓ 実装A）。

- 追加するキー: stage_conclusions / weather / data_quality（各エントリ）、sentiment.signal（無いエントリのみ）
- 既存のキー（judgment〈AIの判定〉・indicators・sentiment.label等）は変更しない。daily pick・TANUKI・
  ポートフォリオ等の他システムが読むキーは変わらない
- 段階8（過去の実績）は、そのエントリのS&P500の終値日までのdaily/だけで計算する（先読みなし）
- 期待する終値日は、data_freshnessがあればその値、無ければエントリの生成時刻とNYSEカレンダーから求める
- 既にstage_conclusionsを持つエントリ（本ツール適用後にcollect_and_send.pyが書いたもの）は変更しない

使い方: python src/market/market_pulse/backfill_stage_conclusions.py [--dry-run]
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(_DIR))))

import collect_and_send as cs  # noqa: E402
from stage_conclusions import build_stage_conclusions  # noqa: E402
from common.market_data.reader import get_price_series_as_of  # noqa: E402


def expected_close_for(entry_iso, cal):
    t = datetime.fromisoformat(entry_iso).astimezone(timezone.utc)
    sched = cal.schedule(start_date=(t - timedelta(days=10)).date(), end_date=t.date())
    closed = sched[sched["market_close"] <= t]
    return closed.index[-1].strftime("%Y-%m-%d") if len(closed) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    import pandas_market_calendars as mcal
    cal = mcal.get_calendar("NYSE")
    with open(cs.JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    data.sort(key=lambda e: datetime.fromisoformat(e["date"]))
    get = lambda sym, as_of, days: get_price_series_as_of(sym, as_of, days=days)  # noqa: E731
    n_stage = n_sig = 0
    for i, e in enumerate(data):
        if "stage_conclusions" not in e:
            exp = (e.get("data_freshness") or {}).get("expected_close_date") or expected_close_for(e["date"], cal)
            r = build_stage_conclusions(e.get("indicators") or {}, e.get("asset_flow"),
                                        (e.get("sentiment") or {}).get("breadth"), exp, get_series=get)
            e["stage_conclusions"] = r["stages"]
            e["weather"] = r["weather"]
            e["data_quality"] = r["data_quality"]
            n_stage += 1
        s = e.get("sentiment")
        if isinstance(s, dict) and s.get("score") is not None and not s.get("signal"):
            t = datetime.fromisoformat(e["date"]).astimezone(timezone.utc)
            hist = [(p["date"][:10], (p.get("sentiment") or {}).get("score")) for p in data[max(0, i - 60):i]]
            s["signal"] = cs.sentiment_signal(s["score"], hist, cs.signal_window_start(t))
            n_sig += 1
    print(f"段階の結論を追加: {n_stage}件 / シグナルを追加: {n_sig}件（全{len(data)}件）")
    if args.dry_run:
        return
    with open(cs.JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    cs.write_nyse_holidays()
    print("保存しました")


if __name__ == "__main__":
    main()
