"""parser の四半期（quarterly_{FYQ}.json）を Layer3 の val から作る1か所（[[PARSER-LAYER3-SINGLE-SOURCE-1]] 段1・段2a）。
段2a（2026-11-27〜）: bs に加え cf（buyback・OCF・SBC・D&A）・pl・other.rpo も置き換える。段2b: EPS・株数も置き換える（plのすべて＋shares）。
基準の意味（段2bの決定、2026-11-27）: 四半期のファイルの値は「今の時点の見方」＝Layer3 の val（その期を最後に申告した値）。分割の調整は、最新の申告が再掲した分だけ
反映される（再掲の無い期は分割前のまま）ので、現在の基準が要る読み手は split_adjust.shares_by_end（val ＋ split_history.yaml の換算）を通す。
このファイルに分割の換算を入れない（二重になる）。倍率の履歴は別で、その月末までに申告された値（first_val・as-of、common/multiples.py）。

parser（SECParser）が company_facts から別の規則で取っていた四半期のBSの値のうち、同じ期・同じ項目の Layer3
（build_ticker_store）の val と違うものを、Layer3 の val（守り〈val_guard〉の後）で置き換える。parser にだけある値・
Layer3 にだけある値（0の補いなど）は触らない（0で埋めない）。符号だけが違う値も触らない。
年次は置き換えない（REPLACED_FIELDS）。
段2aの守り: (1) 非負の項目（売上・費用・R&D・S&M・SG&A・SBC）は、parser が正で Layer3 の val が負になる置き換えをしない。値は欠け（None）にして
record["replace_marks"][項目]="nonneg_layer3_negative" の印を付ける（HONの継続事業の組み替えで累計の差が負になる型）。(2) Layer3 の val が
val_suspect（隣の期から外れる印、val_guard の (iii)）のセルは置き換えて record["replace_marks"][項目]="val_suspect:…" の印を残す（原本の再表示・訂正のことが多い）。

parser の四半期のファイルには期末日が無いので、その期の値と一致する Layer3 のエントリの期末日の多数決で決める
（tools/sim/parser_l3/parser_vs_l3.py の段0の比較と同じ規則。parser のラベル f"{fy}{fp}" は10-Qの前年同期の比較値と
区別できないため、ラベルでは決めない）。決まらない期（票が少ない）は置き換えない。

書き手はここだけ: SECParser.parse_and_save が保存の前に呼ぶ（日曜の SEC_Data_Update）。手元で既存のファイルに当てるときは
`python common/sec_data/quarterly_bs.py [TICKER ...]`（同じ関数を既存ファイルに当てる。SECは呼ばない）。
"""
from __future__ import annotations

import collections
import contextlib
import glob
import io
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

DATA_DIR = os.path.join(REPO, "common", "sec_data", "data")

# parser のフィールド → Layer3 のフィールド（期末日の多数決に使う全項目。置き換えるのは bs だけ）
PARSER_TO_LAYER3: Dict[Tuple[str, str], str] = {
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
# 置き換える項目: bs の全部と、cf の D&A（parser の四半期の D&A が部分のタグ〈減価償却だけ・償却だけ〉の値になる期がある。
# Layer3 は部分の足し算・再タグの突き合わせで直した値を持つ。SEC-RETAG-RECONCILE-1、2026-11-24）
REPLACED_FIELDS = (
    {k for k in PARSER_TO_LAYER3 if k[0] in ("bs", "cf", "pl", "shares")}
    | {("other", "rpo")}
)
# 分割の調整・組み替えで意味が変わる項目（EPS・株数）。parser と Layer3 の比が SPLIT_LIKE_RATIO 倍超のときだけ印を考える
SPLIT_FIELDS = {("pl", "eps_basic"), ("pl", "eps_diluted"), ("shares", "shares_basic"), ("shares", "shares_diluted")}
SPLIT_LIKE_RATIO = 1.5
# 段2bの印（replace_marks）: 一部の分割だけ調整済み（登録の分割のうち、Layer3 の val が反映したのは一部）／登録の分割が無いのに比が大きい（SPAC の
# 組み替え・再表示・parser の誤り）／Layer3 の val が0で parser が0でない値（ASTS 型。0 で埋めず欠けにする）
MARK_SPLIT_PARTIAL = "split_basis_partial"
MARK_RECAST = "recast_no_registered_split"
MARK_LAYER3_ZERO = "layer3_zero_parser_nonzero"
# 正から負への置き換えをしない項目（負になるのは組み替え・累計の差の誤り）
NONNEG_FIELDS = {("pl", "revenue"), ("pl", "cost_of_revenue"), ("pl", "research_and_development"),
                 ("pl", "selling_and_marketing"), ("pl", "selling_general_and_administrative"),
                 ("cf", "stock_based_compensation")}
MIN_VOTES = 4
MIN_VOTE_SHARE = 0.35


def _near(a: float, b: float) -> bool:
    """符号だけが違う値も同じとみなす（capex・buyback の符号の違いは別の話）"""
    if a is None or b is None:
        return False
    return abs(abs(a) - abs(b)) <= max(1e-6, 1e-9 * max(abs(a), abs(b)))


def _entries(store: Dict[str, Any], field: str) -> List[Dict[str, Any]]:
    d = (store.get("fields") or {}).get(field)
    if not d:
        return []
    stock = d.get("category") == "stock"
    out = []
    for e in d.get("entries") or []:
        if e.get("val") is None or e.get("is_ytd"):
            continue
        if not stock and e.get("is_annual"):
            continue
        out.append(e)
    return out


def resolve_period_end(record: Dict[str, Any], store: Dict[str, Any]) -> Optional[str]:
    """parser の四半期のレコードの期末日（Layer3 の同じ値の期末日の多数決）。決まらなければ None"""
    votes: collections.Counter = collections.Counter()
    n_vals = 0
    for (sec, f), lf in PARSER_TO_LAYER3.items():
        v = (record.get(sec) or {}).get(f)
        if v is None:
            continue
        n_vals += 1
        if v == 0:
            continue
        for en in {e["end"] for e in _entries(store, lf) if _near(e["val"], v)}:
            votes[en] += 1
    try:
        ly = int(str(record.get("period"))[:4])
    except (TypeError, ValueError):
        ly = None
    votes = collections.Counter({en: n for en, n in votes.items() if ly is None or abs(int(en[:4]) - ly) <= 1})
    if not votes or votes.most_common(1)[0][1] < max(MIN_VOTES, int(MIN_VOTE_SHARE * n_vals)):
        return None
    return votes.most_common(1)[0][0]


def _split_mark(sec: str, f: str, before: float, after: float, end: str, splits: Optional[List[Dict[str, Any]]]) -> Optional[str]:
    """EPS・株数の置き換えの印。比が大きい（分割の調整・組み替え）とき: 登録の分割（end より後の分割の積）と比が合えば印なし、
    登録の分割があって合わなければ split_basis_partial、登録の分割が無ければ recast_no_registered_split。比が小さければ印なし"""
    if (sec, f) not in SPLIT_FIELDS or not before or not after:
        return None
    ratio = abs(before / after)
    if ratio <= SPLIT_LIKE_RATIO and ratio >= 1 / SPLIT_LIKE_RATIO:
        return None
    exp = 1.0
    for s in splits or []:
        if str(s.get("date")) > end:
            exp *= float(s["ratio"])
    if exp == 1.0:
        return MARK_RECAST
    signed = (after / before) if sec == "shares" else (before / after)   # 株数は後÷前、EPS は前÷後 が、その期より後の分割の積になる
    return None if abs(signed / exp - 1) < 0.1 else MARK_SPLIT_PARTIAL


def replace_record(record: Dict[str, Any], store: Dict[str, Any],
                   splits: Optional[List[Dict[str, Any]]] = None) -> List[Tuple[str, Any, Any]]:
    """record の REPLACED_FIELDS の値を Layer3 の val で置き換える（その場で）。戻り値: [(項目, 前, 後)]。
    非負の項目が正→負になる場合は後を None（欠け）にして replace_marks に残す。EPS・株数は、Layer3 の val が0で parser が0でない値のとき
    0 に置き換えず欠け（None）にして印を付ける。splits（その銘柄の config/split_history.yaml の分割）を渡すと、EPS・株数の分割の印を付ける"""
    if not record.get("bs") and not record.get("pl") and not record.get("cf"):
        return []
    end = resolve_period_end(record, store)
    if not end:
        return []
    changed = []
    for (sec, f), lf in PARSER_TO_LAYER3.items():
        if (sec, f) not in REPLACED_FIELDS:
            continue
        target = record.get(sec) or {}
        if target.get(f) is None:
            continue
        cand = [e for e in _entries(store, lf) if e["end"] == end]
        if not cand:
            continue
        v3 = cand[-1]["val"]
        if v3 != target[f] and not _near(v3, target[f]):
            marks = record.setdefault("replace_marks", {})
            if (sec, f) in NONNEG_FIELDS and target[f] > 0 and v3 < 0:
                changed.append((f, target[f], None))
                target[f] = None
                marks[f] = "nonneg_layer3_negative"
                continue
            if (sec, f) in SPLIT_FIELDS and v3 == 0 and target[f] != 0:
                changed.append((f, target[f], None))
                target[f] = None
                marks[f] = MARK_LAYER3_ZERO
                continue
            before = target[f]
            changed.append((f, before, v3))
            target[f] = v3
            if cand[-1].get("val_suspect"):
                marks[f] = f"val_suspect:{cand[-1]['val_suspect']}"
            else:
                m = _split_mark(sec, f, before, v3, end, splits)
                if m:
                    marks[f] = m
            if not marks:
                record.pop("replace_marks", None)
    return changed


def _build_store(ticker: str) -> Optional[Dict[str, Any]]:
    import logging
    from common.sec_data.layer3_builder import build_ticker_store
    logging.disable(logging.CRITICAL)
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            return build_ticker_store(ticker)
    except Exception:  # noqa: BLE001  Layer3 が作れない銘柄は置き換えない（parser の値のまま）
        return None
    finally:
        logging.disable(logging.NOTSET)


def _splits(ticker: str) -> List[Dict[str, Any]]:
    from common.sec_data.split_adjust import load_split_history
    return load_split_history().get(ticker, [])


def replace_from_layer3(ticker: str, parsed: Dict[str, Any], store: Optional[Dict[str, Any]] = None) -> int:
    """parse_company_facts の戻り（parsed['quarterly']）の bs を Layer3 の val で置き換える。置き換えたセルの数"""
    quarterly = (parsed or {}).get("quarterly") or {}
    if not quarterly:
        return 0
    store = store if store is not None else _build_store(ticker)
    if not store:
        return 0
    splits = _splits(ticker)
    n = 0
    for period, rec in quarterly.items():
        rec.setdefault("period", period)
        n += len(replace_record(rec, store, splits))
    return n


def regenerate(tickers: Optional[List[str]] = None, data_dir: str = DATA_DIR) -> Dict[str, int]:
    """既存の quarterly_*.json の bs に同じ置き換えを当てる（手元・SECは呼ばない）。戻り値: 銘柄→置き換えたセルの数"""
    res: Dict[str, int] = {}
    tickers = tickers or sorted(os.path.basename(d) for d in glob.glob(os.path.join(data_dir, "*")) if glob.glob(os.path.join(d, "quarterly_*.json")))
    for t in tickers:
        paths = sorted(glob.glob(os.path.join(data_dir, t, "quarterly_*.json")))
        store = _build_store(t)
        if not store or not paths:
            continue
        splits = _splits(t)
        n = 0
        for p in paths:
            with open(p, encoding="utf-8") as f:
                rec = json.load(f)
            ch = replace_record(rec, store, splits)
            if ch:
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                n += len(ch)
        if n:
            res[t] = n
    return res


if __name__ == "__main__":
    out = regenerate([a.upper() for a in sys.argv[1:]] or None)
    print(f"置き換えたセル {sum(out.values())}（{len(out)}銘柄）")
    for t, n in sorted(out.items()):
        print(f"  {t}: {n}")
