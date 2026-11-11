"""段階0: parser（annual_*.json・quarterly_*.json）の値と、Layer3（build_ticker_store）の同じ期・同じ項目の値を全銘柄で比べる。
期末日は parser のファイルに無いので、その期の各項目の値と一致する Layer3 のエントリの期末日の多数決で決める。
出力: %TEMP%/parser_l3/{T}.json と all.json（項目×期の差の一覧）
"""
import glob, json, os, sys, collections, contextlib, io

REPO = os.getcwd()
sys.path.insert(0, REPO)
from common.sec_data import layer3_builder as L

OUT = os.path.join(os.environ["TEMP"], "parser_l3")
os.makedirs(OUT, exist_ok=True)

# parser のフィールド → Layer3 のフィールド
MAP = {
    ("bs", "cash_and_equivalents"): "cash_and_equivalents", ("bs", "current_assets"): "current_assets",
    ("bs", "current_liabilities"): "current_liabilities", ("bs", "goodwill"): "goodwill",
    ("bs", "intangible_assets_excl_goodwill"): "intangible_assets_excl_goodwill", ("bs", "long_term_debt"): "long_term_debt",
    ("bs", "minority_interest"): "minority_interest", ("bs", "preferred_stock"): "preferred_stock",
    ("bs", "short_term_debt"): "short_term_debt", ("bs", "short_term_investments"): "short_term_investments",
    ("bs", "stockholders_equity"): "stockholders_equity", ("bs", "total_assets"): "total_assets",
    ("bs", "total_liabilities"): "total_liabilities",
    ("cf", "buyback"): "buyback", ("cf", "capital_expenditure"): "capital_expenditure",
    ("cf", "depreciation_and_amortization"): "depreciation_and_amortization",
    ("cf", "operating_cash_flow"): "operating_cash_flow", ("cf", "stock_based_compensation"): "stock_based_compensation",
    ("other", "rpo"): "rpo",
    ("pl", "cost_of_revenue"): "cost_of_revenue", ("pl", "eps_basic"): "eps_basic", ("pl", "eps_diluted"): "eps_diluted",
    ("pl", "gross_profit"): "gross_profit", ("pl", "net_income"): "net_income", ("pl", "operating_income"): "operating_income",
    ("pl", "research_and_development"): "research_and_development", ("pl", "revenue"): "revenue",
    ("pl", "selling_and_marketing"): "selling_and_marketing",
    ("pl", "selling_general_and_administrative"): "selling_general_and_administrative",
    ("shares", "shares_basic"): "shares_basic_weighted_avg", ("shares", "shares_diluted"): "shares_diluted",
}
# Layer3 に対応が無い parser のフィールド
NO_L3 = [("bs", "insurance_reserves"), ("bs", "restricted_cash"), ("cf", "free_cash_flow")]


def kind_ok(e, kind, category):
    if category == "stock":
        return not e.get("is_ytd")
    if kind == "annual":
        return bool(e.get("is_annual"))
    return (not e.get("is_annual")) and (not e.get("is_ytd"))


def l3_entries(store, f, kind):
    d = store["fields"].get(f)
    if not d:
        return []
    cat = d.get("category")
    return [e for e in d["entries"] if e.get("val") is not None and kind_ok(e, kind, cat)]


def near(a, b):
    if a is None or b is None:
        return False
    return abs(abs(a) - abs(b)) <= max(1e-6, 1e-9 * max(abs(a), abs(b)))   # 符号は別に集計（capex・buyback）


def main(tickers):
    allrows = []
    for t in tickers:
        d = os.path.join("common/sec_data/data", t)
        with contextlib.redirect_stdout(io.StringIO()):
            try:
                store = L.build_ticker_store(t)
            except Exception as ex:
                store = None
        if not store:
            allrows.append({"t": t, "err": "store"}); continue
        rows = []
        for path in sorted(glob.glob(d + "/annual_*.json") + glob.glob(d + "/quarterly_*.json")):
            kind = "annual" if os.path.basename(path).startswith("annual_") else "quarterly"
            p = json.load(open(path, encoding="utf-8"))
            period = p.get("period")
            # 期末の多数決
            votes = collections.Counter()
            vals = {}
            for (sec, f), lf in MAP.items():
                v = (p.get(sec) or {}).get(f)
                if v is None:
                    continue
                vals[(sec, f)] = v
                ends = {e["end"] for e in l3_entries(store, lf, kind) if near(e["val"], v) and v != 0}
                for en in ends:
                    votes[en] += 1
            end = None
            try:
                ly = int(str(period)[:4])
            except Exception:
                ly = None
            votes = collections.Counter({en: n for en, n in votes.items() if ly is None or abs(int(en[:4]) - ly) <= 1})
            if votes and votes.most_common(1)[0][1] < max(4, int(0.35 * len(vals))):
                votes = collections.Counter()
            if votes:
                top = votes.most_common(2)
                end = top[0][0]
                if len(top) > 1 and top[1][1] == top[0][1]:
                    end = max(top[0][0], top[1][0]) if kind == "annual" else top[0][0]
            if not end:
                for (sec, f), lf in MAP.items():
                    if (p.get(sec) or {}).get(f) is not None:
                        rows.append({"t": t, "kind": kind, "period": period, "end": None, "sec": sec, "f": f, "st": "unresolved", "parser": p[sec][f], "l3": None})
                continue
            for (sec, f), lf in MAP.items():
                v = (p.get(sec) or {}).get(f)
                l3v = None
                l3f = None
                if end:
                    cand = [e for e in l3_entries(store, lf, kind) if e["end"] == end]
                    if cand:
                        l3v = cand[-1]["val"]
                        l3f = cand[-1].get("first_val")
                if v is None and l3v is None:
                    continue
                if v is not None and l3v is not None:
                    if v == l3v: st = "same"
                    elif near(v, l3v): st = "sign"
                    else: st = "diff"
                elif v is not None:
                    st = "parser_only"
                else:
                    st = "l3_only"
                if st != "same":
                    rows.append({"t": t, "kind": kind, "period": period, "end": end, "sec": sec, "f": f, "st": st, "parser": v, "l3": l3v, "l3_first": l3f})
                else:
                    rows.append({"t": t, "kind": kind, "period": period, "end": end, "sec": sec, "f": f, "st": "same"})
            for (sec, f) in NO_L3:
                if (p.get(sec) or {}).get(f) is not None:
                    rows.append({"t": t, "kind": kind, "period": period, "end": end, "sec": sec, "f": f, "st": "no_l3_field", "parser": p[sec][f], "l3": None})
        json.dump(rows, open(os.path.join(OUT, t + ".json"), "w", encoding="utf-8"), ensure_ascii=False)
        allrows.extend(rows)
        print(t, len(rows), flush=True)
    json.dump(allrows, open(os.path.join(OUT, "all.json"), "w", encoding="utf-8"), ensure_ascii=False)


if __name__ == "__main__":
    tk = sys.argv[1:] or sorted(os.path.basename(x) for x in glob.glob("common/sec_data/data/*") if glob.glob(x + "/annual_*.json"))
    main(tk)
