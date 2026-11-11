"""[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段1の前の守り（val の検査）の試作（試算用。common には入れていない）。

Layer3 の val は「その期を最後に載せた申告の値」。最新の申告に取得元の誤りや内訳の欠けがあると、そのまま入る
（COHR 2023年次のD&A 1,000倍、APP 2021・2022年次の capex でソフト開発費の行が落ちる）。機械で見分ける規則:
  (i)   val と first_val の比が 10 の累乗（|k|>=3、持ち株数・1株の値は分割があるので |k|>=3 だけ）= 単位の誤り。どちらが誤りかは隣の期（同じ種類で前後2期ずつの中央値）に近い方を正とする
  (ii)  cf_lines の capex: 最新の申告の内訳の行（有形固定資産・ソフト開発費）が、同じ期を載せた前の申告より少ない → 内訳の揃った最新の申告を採る
  (iii) 隣の期から 10^0.5（約3.2倍）より外れるのに first_val は 10^0.3（約2倍）以内
  共通の除外: 持ち株数・1株の値は(iii)の対象外（分割の組み替えは val が正）、同じ期末で5項目以上が first_val から20%超動いていれば期全体の組み替え（SPAC等）として対象外
当たったときは val を first_val（(ii)は内訳の揃った申告の値）に置き換え、エントリに val_guard（理由）・val_latest（置き換え前の val）を残す。
"""
import math
import statistics

SPLIT_FIELDS = {"shares_diluted", "shares_basic_weighted_avg", "shares_outstanding_period_end_sec", "eps_basic", "eps_diluted",
                "dividends_per_share"}
POW10_TOL = 0.003          # log10 の比が整数から 0.003 以内（約0.7%）
DEV_VAL = 0.5              # val が隣の期の中央値から離れる log10
DEV_FIRST = 0.3            # first_val が離れていてもよい上限
BREADTH_LIMIT = 5          # 同じ期末で first_val から 20%超動いた項目の数がこれ以上なら期全体の組み替え


def _is_pow10(r: float, split: bool):
    lg = math.log10(r)
    k = round(lg)
    if k == 0 or abs(lg - k) > POW10_TOL:
        return None
    if split and abs(k) < 3:
        return None
    return k


def detect(store: dict) -> list:
    """store の中で当たった期の一覧 [{field, end, is_annual, rule, val, first_val, pick}]（store は書き換えない）"""
    fields = store["fields"]
    breadth = {}
    for f, fd in fields.items():
        if f in SPLIT_FIELDS:
            continue
        for e in fd["entries"]:
            v, fv = e.get("val"), e.get("first_val")
            if v is None or fv is None or e.get("is_ytd") or v == fv:
                continue
            if abs(fv) < 1e-9 or abs(v / fv - 1) > 0.2:
                key = (e["end"], bool(e.get("is_annual")))
                breadth[key] = breadth.get(key, 0) + 1
    hits = []
    for f, fd in fields.items():
        es = [e for e in fd["entries"] if e.get("val") is not None and not e.get("is_ytd")]
        split = f in SPLIT_FIELDS
        for ann in (True, False):
            ks = sorted([e for e in es if bool(e.get("is_annual")) == ann], key=lambda e: e["end"])
            for i, e in enumerate(ks):
                v, fv = e["val"], e.get("first_val")
                if fv is None or v == fv or not v or not fv:
                    continue
                if breadth.get((e["end"], ann), 0) >= BREADTH_LIMIT:
                    continue
                nb = [abs(x["val"]) for x in ks[max(0, i - 2):i] + ks[i + 1:i + 3] if x["val"]]
                med = statistics.median(nb) if len(nb) >= 2 else None
                dv = abs(math.log10(abs(v) / med)) if med else None
                df = abs(math.log10(abs(fv) / med)) if med else None
                k = _is_pow10(abs(v / fv), split)
                if k is not None:
                    if med is None:
                        hits.append({"field": f, "end": e["end"], "is_annual": ann, "rule": "i_pow10_undecided", "k": k,
                                     "val": v, "first_val": fv, "pick": None})
                    elif dv > df:
                        hits.append({"field": f, "end": e["end"], "is_annual": ann, "rule": "i_pow10", "k": k,
                                     "val": v, "first_val": fv, "pick": "first_val"})
                    else:
                        hits.append({"field": f, "end": e["end"], "is_annual": ann, "rule": "i_pow10_first_bad", "k": k,
                                     "val": v, "first_val": fv, "pick": "val"})
                    continue
                if split or med is None:
                    continue
                if dv > DEV_VAL and df < DEV_FIRST:
                    hits.append({"field": f, "end": e["end"], "is_annual": ann, "rule": "iii_neighbor", "k": None,
                                 "val": v, "first_val": fv, "pick": "first_val", "dev_val": round(dv, 2), "dev_first": round(df, 2)})
    return hits


ADOPTED_RULES = ("i_pow10",)       # 採る規則。(iii)は15件中1件しか真の誤りでなく、置き換えるとHypeCoreの過去の月が動く（AVAV）ので採らない


def apply_store(store: dict, rules: tuple = ADOPTED_RULES) -> list:
    """当たった期の val を first_val に置き換える（store を書き換える）。(i)の「first_val 側が誤り」は何もしない。戻り値は置き換えた一覧"""
    done = []
    for h in detect(store):
        if h["pick"] != "first_val" or h["rule"] not in rules:
            continue
        for e in store["fields"][h["field"]]["entries"]:
            if e["end"] == h["end"] and bool(e.get("is_annual")) == h["is_annual"] and e.get("val") == h["val"] and not e.get("is_ytd"):
                e["val_latest"] = e["val"]
                e["val"] = e["first_val"]
                e["val_guard"] = h["rule"]
                done.append(h)
                break
    return done


# ───────── (ii) cf_lines の capex: 内訳の行が減った最新の申告を避ける ─────────
def install_cf_lines_guard():
    """cf_lines.period_values を差し替える（試算用）。同じ期を載せた申告の中で、内訳の行（ppe・software）の種類が
    いちばん多い申告のうち最新のものを採る（最新の申告が内訳の一部を落としていても、揃った申告の値）。
    採った申告が最新でないとき、値に guard=（落ちた行の種類）を残す"""
    from common.sec_data import cf_lines as C
    orig = C.period_values

    def period_values(records):
        out = orig(records)
        cand = {}
        for accn, rec in records.items():
            if rec.get("status") != "ok":
                continue
            kinds = C.line_kinds(rec)
            cap = [x for k, x in kinds if k in ("ppe", "software")]
            if cap and all(not x["values"] for x in cap):
                continue
            has_line = bool(cap)
            tot = next((x for k, x in kinds if k == "total"), None)
            periods = set()
            for _k, x in kinds:
                periods |= set(x["values"])
            for p in periods:
                if tot is not None and p not in tot["values"]:
                    continue
                v = {"ppe": 0.0, "software": 0.0, "other_acq": 0.0, "has_capex_line": has_line, "accn": accn,
                     "filed": rec["filed"], "form": rec["form"], "present": set()}
                for k, x in kinds:
                    if k in ("ppe", "software", "other_acq") and p in x["values"]:
                        v[k] += x["values"][p]
                        if k in ("ppe", "software"):
                            v["present"].add(k)
                cand.setdefault(p, []).append(v)
        for p, vs in cand.items():
            best = max(len(v["present"]) for v in vs)
            full = [v for v in vs if len(v["present"]) == best]
            pick = max(full, key=lambda v: v["filed"])
            latest = max(vs, key=lambda v: v["filed"])
            if len(latest["present"]) < best and pick is not latest and (
                    pick["ppe"] + pick["software"] != latest["ppe"] + latest["software"]):
                pick = {k: x for k, x in pick.items() if k != "present"}
                pick["guard"] = "components_dropped:" + ",".join(sorted({"ppe", "software"} - latest["present"]))
                out[p] = pick
        return out

    C.period_values = period_values
    return C


def install_store_guard():
    """build_ticker_store を包んで、(i)の val を first_val に置き換える。すでに import 済みの参照（from ... import）も差し替える"""
    import sys
    from common.sec_data import layer3_builder as L
    orig = L.build_ticker_store

    def guarded(ticker):
        s = orig(ticker)
        if s:
            apply_store(s)
        return s

    for m in list(sys.modules.values()):
        if getattr(m, "build_ticker_store", None) is orig:
            m.build_ticker_store = guarded
    return guarded


def apply(ctx):
    """sim.py の --hook 用"""
    install_cf_lines_guard()
    install_store_guard()
