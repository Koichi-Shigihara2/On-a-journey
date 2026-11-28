"""日足の分割・分社の割り戻し（[[POINT-IN-TIME-HISTORY-INTEGRITY-1]] (1)、2026-11-12）。

Yahooの日足は分割・分社の後に過去の足も調整した値で返る。毎日の取得は直近の行しか取り直さないので、古い行は
調整されないまま残る（APHの2026-09-03の2:1分割・CTVAの2026-10-01の分社）。`_daily_rejected_overwrites.json`
には出ない。ここでは `split_history.yaml` に依らず、**確定した終値とYahooの終値の比**で決める:

  - 比（保存した終値 / Yahooの終値）が±2%を超える行が、比べた行の8割以上
  - その比が1つに揃う（超えた行の98%以上が中央値の±2%以内）
  - 超えた行が境の日より前に固まり、境の日より後は全部一致している
  → 境の日より前の確定の行を `fetcher.correct_daily_rows(reason="split_adjust", note="ratio=…")` で割り戻す

  - **連続性の確認**（2026-11-28、SNDK の 11-20・11-23 の二重の調整〔終値が8分の1になった〕を受けて）: 割り戻した後の終値の系列で、境の前後
    CONTINUITY_ROWS 行のどこかに、1日の動きが CONTINUITY_MAX_MOVE（±50%）を超える所が出るなら、当てない（Yahooの値が一時的に二重に
    調整されている・境の判定が実際の分割の日とずれている疑い）。理由は skips に残し、走査の出力に [SKIP] として出す（人が確かめる）

  - 当てなかった銘柄は、走査のたびに common/market_data/_split_reconcile_skips.json に残す（最初に見た日 first_seen つき。今は空の記録を置いてある）。
    System Health の [R] が「新しい [SKIP]」を ⚠️ で出す（2026-11-28）

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
CONTINUITY_ROWS = 6             # 境の前後それぞれこの行数で、割り戻した後の終値の1日の動きを確かめる
CONTINUITY_MAX_MOVE = 0.5       # その動きの上限（±50%。これを超えるなら当てない）
SKIP_SUFFIXES = ("=F", "=X")
PROVIDER_START = "2020-12-28"
BATCH = 50

SKIPS_FILE = "_split_reconcile_skips.json"

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


def _continuity_break(records: List[Dict[str, Any]], ratio: float, boundary: str) -> Optional[Dict[str, Any]]:
    """割り戻した後の終値の系列（境より前の確定の行は ratio で割る）で、境の前後 CONTINUITY_ROWS 行の1日の動きが
    CONTINUITY_MAX_MOVE を超える所があれば {date, move}、無ければ None"""
    rows = [r for r in sorted(records, key=lambda x: x.get("date") or "")
            if r.get("date") and r.get("close") and not r.get("_provisional")]
    series = [(r["date"], r["close"] / ratio if r["date"] < boundary else r["close"]) for r in rows]
    idx = next((i for i, (d, _) in enumerate(series) if d >= boundary), len(series))
    lo, hi = max(1, idx - CONTINUITY_ROWS), min(len(series), idx + CONTINUITY_ROWS + 1)
    for i in range(lo, hi):
        prev, cur = series[i - 1][1], series[i][1]
        if prev and abs(cur / prev - 1) > CONTINUITY_MAX_MOVE:
            return {"date": series[i][0], "move": cur / prev - 1}
    return None


def detect(records: List[Dict[str, Any]], provider: Dict[str, Dict[str, float]],
           skips: Optional[List[Dict[str, Any]]] = None) -> Optional[Dict[str, Any]]:
    """1銘柄の判定。当たれば {ratio, boundary, volume_ratio, compared, mismatched}、当たらなければ None。
    連続性の確認で当てなかったときは、skips（渡したときだけ）に {reason, date, move, ratio, boundary} を足す"""
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
    brk = _continuity_break(records, ratio, boundary)
    if brk is not None:
        if skips is not None:
            skips.append({"reason": "continuity", "date": brk["date"], "move": brk["move"], "ratio": ratio, "boundary": boundary})
        return None
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
         start: str = PROVIDER_START, skipped: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
    """走査（書かない）。{銘柄: hit}。連続性の確認で当てなかった銘柄は、skipped（渡したときだけ）に {銘柄: 理由} で足す"""
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
        sk: List[Dict[str, Any]] = []
        h = detect(recs, prov.get(s) or {}, sk)
        if h:
            hits[s] = h
        elif sk and skipped is not None:
            skipped[s] = sk[0]
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


def record_skips(skipped: Dict[str, Dict[str, Any]], base_dir: Optional[str] = None, today: Optional[str] = None) -> bool:
    """連続性の確認で当てなかった銘柄を common/market_data/_split_reconcile_skips.json に残す（System Health の [R] が読む）。
    {"skips": {銘柄: {boundary, ratio, date, move, reason, first_seen}}}。同じ銘柄・同じ境が前回もあれば first_seen を引き継ぐ。
    もう当てなくなった銘柄は消す。中身が前回と同じなら書かない。書いたら True"""
    from datetime import date as _date
    base = fetcher._resolve_base_dir(base_dir)
    path = os.path.join(base, SKIPS_FILE)
    today = today or _date.today().isoformat()
    try:
        with open(path, encoding="utf-8") as f:
            prev = (json.load(f) or {}).get("skips") or {}
    except (OSError, ValueError):
        prev = {}
    skips = {}
    for s, k in sorted(skipped.items()):
        first = prev.get(s, {}).get("first_seen") if prev.get(s, {}).get("boundary") == k["boundary"] else None
        skips[s] = {"boundary": k["boundary"], "ratio": round(k["ratio"], 6), "date": k["date"], "move": round(k["move"], 4),
                    "reason": k["reason"], "first_seen": first or today}
    if skips == prev and os.path.exists(path):
        return False
    if not skips and not prev and not os.path.exists(path):
        return False          # 1度も出ていなければファイルを作らない
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"_description": "split_reconcile の走査で、連続性の確認により当てなかった銘柄（人が確かめる）。System Health の [R] が読む",
                   "skips": skips}, f, ensure_ascii=False, indent=1)
        f.write("\n")
    return True


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="日足の分割・分社の割り戻し（Yahooとの比で決める）")
    ap.add_argument("tickers", nargs="*")
    ap.add_argument("--apply", action="store_true", help="当たった銘柄を割り戻す（無ければ走査だけ）")
    a = ap.parse_args(argv)
    syms = [t.upper() for t in a.tickers] or eligible_symbols()
    skipped: Dict[str, Dict[str, Any]] = {}
    hits = scan(syms, skipped=skipped)
    print(f"走査 {len(syms)}銘柄、当たり {len(hits)}銘柄")
    for s, k in sorted(skipped.items()):
        print(f"  [SKIP] {s}: 比 {k['ratio']:.6g}・境 {k['boundary']} で割り戻すと {k['date']} の終値が1日で {k['move']:+.0%} 動く（連続性）。当てない・人が確かめる")
    for s, h in sorted(hits.items()):
        print(f"  {s}: 比 {h['ratio']:.6g}、境 {h['boundary']}、食い違い {h['mismatched']}/{h['compared']}行、出来高比 {h['volume_ratio']:.6g}")
    if a.tickers == [] and record_skips(skipped):          # 全銘柄の走査のときだけ残す（銘柄を指定した走査では消さない）
        print(f"  [SKIP] の記録を更新した（{SKIPS_FILE}）")
    if a.apply and hits:
        apply(hits, len(syms))
    elif hits:
        print("（走査だけ。--apply で割り戻す）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
