"""parser_vs_l3.py の all.json から、parser の値を Layer3 の値に置き換えた annual_*.json・quarterly_*.json を作る（試算用）。
使い方: python parser_overlay.py S1|S2|S2nz|S3 OUT
  S1: 違う値（diff＝parserの値がLayer3のval・first_valのどちらとも違う）だけ Layer3 の値（first_val、無ければval）に置き換える
  S3: Layer3 の最新の値（val）で、parser と違う値（diff 全部。原本の値と同じものを含む）を置き換える
  S2: S1 ＋ parser に無い項目（l3_only）を Layer3 の値で足す（0も足す）
parser にだけある値（parser_only）・期末日が決まらない期・Layer3 に対応の無い項目（free_cash_flow・insurance_reserves・restricted_cash）はそのまま。
"""
import collections, json, os, sys

mode, out = sys.argv[1], sys.argv[2]
flt = set(sys.argv[3].split(",")) if len(sys.argv) > 3 else None   # 例: quarterly:bs,annual:cf
rows = json.load(open(os.path.join(os.environ["TEMP"], "parser_l3", "all.json"), encoding="utf-8"))
by = collections.defaultdict(list)
for x in rows:
    st = x.get("st")
    if mode != "S3" and st == "diff" and x.get("l3_first") is not None and x["parser"] == x["l3_first"]:
        continue            # 原本の値（first_val）と同じ＝定義の違いではなく改訂の違い。置き換えない
    if flt is not None and f"{x['kind']}:{x['sec']}" not in flt:
        continue
    if st == "l3_only" and mode == "S2nz" and x.get("l3") == 0:
        continue            # S2nz: S2から0の値の追加だけを除いた試算（0埋めが原因かの切り分け、[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段0）
    if st == "diff" or (mode in ("S2", "S2nz") and st == "l3_only"):
        by[(x["t"], x["kind"], x["period"])].append(x)
n = 0
for (t, kind, period), xs in by.items():
    src = f"common/sec_data/data/{t}/{'annual' if kind == 'annual' else 'quarterly'}_{period}.json"
    d = json.load(open(src, encoding="utf-8"))
    for x in xs:
        v = x["l3"] if mode == "S3" else (x.get("l3_first") if x.get("l3_first") is not None else x["l3"])
        if v is None:
            continue
        d.setdefault(x["sec"], {})[x["f"]] = v
        n += 1
    dst = os.path.join(out, t, os.path.basename(src))
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    json.dump(d, open(dst, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print(mode, "files", len(by), "cells", n)
