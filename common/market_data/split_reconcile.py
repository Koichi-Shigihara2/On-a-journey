"""日足の分割・分社の割り戻し（[[POINT-IN-TIME-HISTORY-INTEGRITY-1]] (1)、2026-11-12）。

Yahooの日足は分割・分社の後に過去の足も調整した値で返る。毎日の取得は直近の行しか取り直さないので、古い行は
調整されないまま残る（APHの2026-09-03の2:1分割・CTVAの2026-10-01の分社）。`_daily_rejected_overwrites.json`
には出ない。ここでは `split_history.yaml` に依らず、**確定した終値とYahooの終値の比**で決める:

  - 比（保存した終値 / Yahooの終値）が±2%を超える行が、比べた行の8割以上
  - その比が1つに揃う（超えた行の98%以上が中央値の±2%以内）
  - 超えた行が境の日より前に固まり、境の日より後は全部一致している
  → 境の日より前の確定の行を `fetcher.correct_daily_rows(reason="split_adjust", note="ratio=…")` で割り戻す

書き手は `correct_daily_rows` だけ（daily/ を書くのは fetcher.write_daily_rows だけの決まり）。銘柄名は手で書かない。
暫定の行（_provisional）・終値の無い行は触らない。一度当てた銘柄は次の走査で一致するので、2回目は0件。
走査の結果が多すぎるとき（取得元の異常の疑い）は何も書かない。

使い方:  python common/market_data/split_reconcile.py            # 走査だけ（書かない）
         python common/market_data/split_reconcile.py --apply    # 当たった銘柄を割り戻す
         python common/market_data/split_reconcile.py AAPL --apply
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
import sys
from typing import Any, Callable, Dict, List, Optional

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)

from common.market_data import fetcher  # noqa: E402

RATIO_TOLERANCE = 0.02          # 行ごとの食い違い・比の揃い方の幅
MIN_MISMATCH_SHARE = 0.8        # 食い違う行が比べた行のこの割合以上
MIN_CONSISTENT_SHARE = 0.98     # 食い違う行のうち、比が中央値の±幅に入る割合
MIN_COMPARED = 60               # 比べる行の最小数（少ないと判定しない）
MAX_HIT_SHARE = 0.05            # 当たった銘柄がこの割合を超えたら取得元の異常とみなして書かない
PRICE_FIELDS = ("open", "high", "low", "close")
SKIP_SUFFIXES = ("=F", "=X")
PROVIDER_START = "2020-12-28"
BATCH = 50

ProviderFn = Callable[[List[str], str], Dict[str, Dict[str, Dict[str, float]]]]


def eligible_symbols(base_dir: Optional[str] = None) -> List[str]:
    """走査する銘柄。指数（^）・先物・為替は分割が無いので外す"""
    base = fetcher._resolve_base_dir(base_dir)
    out = []
    for p in sorted(glob.glob(os.path.join(base, "daily", "*.json"))):
        s = os.path.basename(p)[:-5]
        if s.startswith("^") or s.endswith(SKIP_SUFFIXES):
            continue
        out.append(s)
    return out


def yahoo_provider(symbols: List[str], start: str) -> Dict[str, Dict[str, Dict[str, float]]]:
    """Yahooの日足（auto_adjust=False。分割・分社は調整済み、配当は調整なし）。{銘柄: {日付: {close, volume}}}"""
    import yfinance as yf
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for i in range(0, len(symbols), BATCH):
        chunk = symbols[i:i + BATCH]
        try:
            raw = yf.download(chunk, start=start, group_by="ticker", auto_adjust=False, progress=False, threads=True)
        except Exception as e:  # noqa: BLE001  取得の失敗はその銘柄を判定しないだけ
            print(f"   [WARN] yf.download失敗 {chunk[0]}〜: {e}")
            continue
        for s in chunk:
            try:
                df = raw[s] if len(chunk) > 1 else raw
            except KeyError:
                continue
            rows: Dict[str, Dict[str, float]] = {}
            for ts, r in df.dropna(subset=["Close"]).iterrows():
                v = r.get("Volume")
                rows[ts.strftime("%Y-%m-%d")] = {"close": float(r["Close"]),
                                                  "volume": float(v) if v == v and v is not None else 0.0}
            out[s] = rows
    return out


def detect(records: List[Dict[str, Any]], provider: Dict[str, Dict[str, float]]) -> Optional[Dict[str, Any]]:
    """1銘柄の判定。当たれば {ratio, boundary, volume_ratio, compared, mismatched}、当たらなければ None"""
    pairs = []
    for r in sorted(records, key=lambda x: x.get("date") or ""):
        d, c = r.get("date"), r.get("close")
        p = provider.get(d) if d else None
        if not c or not p or not p.get("close") or r.get("_provisional"):
            continue
        pairs.append((d, c / p["close"], r.get("volume"), p.get("volume")))
    if len(pairs) < MIN_COMPARED:
        return None
    bad = [x for x in pairs if abs(x[1] - 1) > RATIO_TOLERANCE]
    if not bad or len(bad) / len(pairs) < MIN_MISMATCH_SHARE:
        return None
    ratio = statistics.median(x[1] for x in bad)
    if sum(1 for x in bad if abs(x[1] / ratio - 1) <= RATIO_TOLERANCE) / len(bad) < MIN_CONSISTENT_SHARE:
        return None
    last_bad = bad[-1][0]
    after = [x for x in pairs if x[0] > last_bad]
    if any(abs(x[1] - 1) > RATIO_TOLERANCE for x in after):
        return None
    # 境の日: 最後の食い違いの後の、保存した最初の確定の行の日付（その日から一致）。後の行が無ければ食い違いの翌日扱いにできない
    if not after:
        return None
    boundary = after[0][0]
    # 出来高: Yahooの出来高 / 保存した出来高 が比に近ければ（分割）出来高も比を掛ける。1に近ければ（分社）そのまま
    vr = [x[3] / x[2] for x in bad if x[2] and x[3]]
    volume_ratio = 1.0
    if vr:
        m = statistics.median(vr)
        if abs(m / ratio - 1) <= RATIO_TOLERANCE:
            volume_ratio = ratio
    return {"ratio": ratio, "boundary": boundary, "volume_ratio": volume_ratio,
            "compared": len(pairs), "mismatched": len(bad)}


def corrected_rows(records: List[Dict[str, Any]], hit: Dict[str, Any]) -> List[Dict[str, Any]]:
    """境の日より前の確定の行を割り戻した行（暫定の行・終値の無い行は含めない）"""
    ratio, vr = hit["ratio"], hit["volume_ratio"]
    out = []
    for r in records:
        if not r.get("date") or r["date"] >= hit["boundary"] or r.get("_provisional") or not r.get("close"):
            continue
        n = {k: v for k, v in r.items() if k not in ("_corrections",)}
        for k in PRICE_FIELDS:
            if n.get(k) is not None:
                n[k] = n[k] / ratio
        if n.get("volume") is not None and vr != 1.0:
            n["volume"] = int(round(n["volume"] * vr))
        out.append(n)
    return out


def scan(symbols: List[str], base_dir: Optional[str] = None, provider_fn: ProviderFn = yahoo_provider,
         start: str = PROVIDER_START) -> Dict[str, Dict[str, Any]]:
    """走査（書かない）。{銘柄: hit}"""
    base = fetcher._resolve_base_dir(base_dir)
    prov = provider_fn(symbols, start)
    hits: Dict[str, Dict[str, Any]] = {}
    for s in symbols:
        p = os.path.join(base, "daily", f"{s}.json")
        try:
            with open(p, encoding="utf-8") as f:
                recs = json.load(f).get("records") or []
        except (OSError, ValueError):
            continue
        h = detect(recs, prov.get(s) or {})
        if h:
            hits[s] = h
    return hits


def apply(hits: Dict[str, Dict[str, Any]], total: int, base_dir: Optional[str] = None, now=None) -> Dict[str, int]:
    """当たった銘柄を割り戻す（correct_daily_rows のみ）。当たりが多すぎるときは書かない"""
    base = fetcher._resolve_base_dir(base_dir)
    if total and len(hits) / total > MAX_HIT_SHARE:
        print(f"[ABORT] 当たりが {len(hits)}/{total} 銘柄で多すぎる（取得元の異常の疑い）。書かない")
        return {}
    done: Dict[str, int] = {}
    for s, h in sorted(hits.items()):
        with open(os.path.join(base, "daily", f"{s}.json"), encoding="utf-8") as f:
            recs = json.load(f).get("records") or []
        rows = corrected_rows(recs, h)
        note = f"ratio={h['ratio']:.6g} boundary={h['boundary']} volume_ratio={h['volume_ratio']:.6g}"
        res = fetcher.correct_daily_rows(s, rows, reason="split_adjust", note=note, base_dir=base, now=now)
        done[s] = len(res["corrected"])
        print(f"[APPLY] {s}: {done[s]}行を割り戻し（{note}）")
    return done


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="日足の分割・分社の割り戻し（Yahooとの比で決める）")
    ap.add_argument("tickers", nargs="*")
    ap.add_argument("--apply", action="store_true", help="当たった銘柄を割り戻す（無ければ走査だけ）")
    a = ap.parse_args(argv)
    syms = [t.upper() for t in a.tickers] or eligible_symbols()
    hits = scan(syms)
    print(f"走査 {len(syms)}銘柄、当たり {len(hits)}銘柄")
    for s, h in sorted(hits.items()):
        print(f"  {s}: 比 {h['ratio']:.6g}、境 {h['boundary']}、食い違い {h['mismatched']}/{h['compared']}行、出来高比 {h['volume_ratio']:.6g}")
    if a.apply and hits:
        apply(hits, len(syms))
    elif hits:
        print("（走査だけ。--apply で割り戻す）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
