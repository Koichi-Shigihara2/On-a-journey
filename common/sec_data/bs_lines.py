"""
common/sec_data/bs_lines.py
申告の貸借対照表の行（借入金・短期投資・優先株・無形資産・のれん・非支配持分）の取得・保存と、欠けの補い
（[[EXTERNAL-SOURCE-CHANGE-WATCH-1]] ②、2026-11-04）。

companyfactsのAPIには会社の独自タグ（CRWVの crwv:RecourseDebtCurrent 等、2026年Q2の10-Qで借入金が移った）が出ず、
タグで最新の期の値が欠ける。cf_lines（申告のインスタンスと表示の構造）と同じ取得で、貸借対照表の表の行と値を
common/sec_data/data/{T}/bs_lines/{accn}.json に保存する（取得は cf_lines.fetch_filing と同じリクエストを使う。1度取った申告は取り直さない）。

表の特定（extract()）: 資産合計（Assets）と負債・純資産合計（LiabilitiesAndStockholdersEquity）を両方含む表のうち、いちばん大きいもの。
  値は次元の無い時点（instant）の文脈だけ。保存するのは、下の分類の表のどれかに当たる行だけ（タグと値）

行の分類（classify()、読むときに決める。ここの1か所）:
  long_term_debt・short_term_debt（1年内返済を含む）・short_term_investments・preferred_stock・goodwill・
  intangible_assets_excl_goodwill・minority_interest（償還可能を含む）、と照合の目印の cash_and_equivalents。独自タグも名前の規則で拾う
  （リース・利息・発行費用・有価証券の負債は除く）。同じ申告・同じ時点の同じ分類の行は足す（CRWVの償還請求権あり・なしの2行）

使い方（補いだけ。段5bと同じ形）:
  - Layer3（fill_layer3_bs、ROTCEのTCEが読む）: 純資産の値がある期末日で、その項目にcompanyfactsの値が無い期を bs_lines の値で埋める。
    印 source_tag="bs_lines"。その申告の貸借対照表に行が無く、近い名前の行（NEAR_NAMES）も無い分類は0（印 NO_BS_LINE_MARK「貸借対照表に行なし」、
    段5bの「CF計算書にCapExの行なし」と同じ形）。ただし0にするのは ZERO_ALWAYS（借入金・優先株・非支配持分）だけで、ZERO_IF_10K
    （無形資産・のれん・短期投資）は直近の10-Kの貸借対照表にも行が無いときだけ
  - TANUKIの純有利子負債（reader.get_net_cash が latest_balance を呼ぶ）: 最新の四半期のファイルに借入金・短期投資が無いとき、
    最新の申告の貸借対照表の値を使う（その申告の現金の行が四半期のファイルの現金と同じときだけ＝同じ時点）
"""
from __future__ import annotations

import json
import os
import re
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from . import cf_lines as _cf

DATA_DIR = _cf.DATA_DIR
XBRLI = _cf.XBRLI
LINK, XL = _cf.LINK, _cf.XL

# ━━━ 行の分類の表（1か所。銘柄の一覧は作らない） ━━━
_NOT_DEBT = re.compile(r"Lease|Interest|Issuance|Discount|Premium|Derivative|Securit|Investment|Receivable|Asset|Accrued|Deferred|Tax", re.I)
# 借入金とファイナンス・リースの合計の標準タグは借入金（parserの候補と同じ。名前の「Lease」で除かない）
_DEBT_WITH_LEASE = re.compile(r"^LongTermDebtAndCapitalLeaseObligations(Current)?$")
RULES: Tuple[Tuple[str, re.Pattern], ...] = (
    ("cash_and_equivalents", re.compile(r"^(CashAndCashEquivalentsAtCarryingValue|Cash|CashAndDueFromBanks|"
                                        r"CashCashEquivalentsAndShortTermInvestments)$")),
    ("goodwill", re.compile(r"^Goodwill$")),
    ("intangible_assets_excl_goodwill", re.compile(
        r"^(IntangibleAssetsNetExcludingGoodwill|FiniteLivedIntangibleAssetsNet|IndefiniteLivedIntangibleAssetsExcludingGoodwill|"
        r"IndefiniteLivedTrademarks|IndefiniteLivedLicenseAgreements|IndefiniteLivedFranchiseRights|OtherIntangibleAssetsNet|"
        r"\w*(?<!Including)(?<!IncludingGoodwill)IntangibleAssetsNet(ExcludingGoodwill)?)$")),
    ("preferred_stock", re.compile(r"^(PreferredStockValue|PreferredStockValueOutstanding|ConvertiblePreferredStock\w*Value|"
                                   r"PreferredStockIncludingAdditionalPaidInCapital\w*)$")),
    ("minority_interest", re.compile(r"^(MinorityInterest|RedeemableNoncontrollingInterestEquity(Common)?CarryingAmount)$")),
    ("short_term_investments", re.compile(r"^(ShortTermInvestments|MarketableSecuritiesCurrent|OtherShortTermInvestments|"
                                          r"AvailableForSaleSecuritiesDebtSecuritiesCurrent|AvailableForSaleSecuritiesCurrent|"
                                          r"DebtSecuritiesAvailableForSale\w*Current|HeldToMaturitySecuritiesCurrent|"
                                          r"DebtSecuritiesHeldToMaturity\w*Current|\w*ShortTermInvestments)$")),
    ("short_term_debt", re.compile(r"^(ShortTermBorrowings|CommercialPaper|DebtCurrent|LongTermDebtCurrent|NotesPayableCurrent|"
                                   r"LongTermDebtAndCapitalLeaseObligationsCurrent|"
                                   r"\w*(Debt|Borrowings?|NotesPayable|Notes|LineOfCredit|LinesOfCredit|CommercialPaper)\w*Current)$")),
    ("long_term_debt", re.compile(r"^(LongTermDebt|LongTermDebtNoncurrent|LongTermNotesPayable|SeniorNotes|ConvertibleNotesPayable|"
                                  r"LongTermDebtAndCapitalLeaseObligations|UnsecuredLongTermDebt|ConvertibleLongTermNotesPayable|"
                                  r"\w*(Debt|Borrowings?|NotesPayable|Notes|LineOfCredit|LinesOfCredit)\w*(Noncurrent|NonCurrent))$")),
)
DEBT_FIELDS = ("long_term_debt", "short_term_debt")
# 「行が無ければ0」の安全の条件: 分類に当たらない行に、その分類に近い名前の行が1つでもあれば0にしない（名前の規則で拾えない独自の行が
# あるかもしれない。借入金を誤って0にすると純有利子負債が小さく出る）
NEAR_NAMES = {
    "long_term_debt": re.compile(r"Debt|Borrow|NotesPayable|SeniorNotes|Loan|LineOfCredit|LinesOfCredit|Convertible|Bond", re.I),
    "short_term_debt": re.compile(r"Debt|Borrow|NotesPayable|SeniorNotes|Loan|LineOfCredit|LinesOfCredit|CommercialPaper|Convertible|Bond", re.I),
    "short_term_investments": re.compile(r"Investment|MarketableSecurit|AvailableForSale|HeldToMaturity|TimeDeposit", re.I),
    "preferred_stock": re.compile(r"Preferred", re.I),
    "goodwill": re.compile(r"Goodwill", re.I),
    "intangible_assets_excl_goodwill": re.compile(r"Intangible|Trademark|TradeName|License|Patent|CustomerRelationship|Franchise", re.I),
    "minority_interest": re.compile(r"Noncontrolling|NonControlling|Minority", re.I),
}
# 近い名前から外す、分類ごとの期間の取り違え（短期の分類に長期の行、長期の分類に短期の行を数えない。LLYの長期投資〈LongTermInvestments〉が
# 短期投資の「近い名前」になって0にならなかった、2026-11-04 夜）
NEAR_EXCLUDE_BY_FIELD = {
    "short_term_investments": re.compile(r"LongTerm|Non[Cc]urrent"),
    "short_term_debt": re.compile(r"Non[Cc]urrent|LongTerm(?!\w*Current)"),
    "long_term_debt": re.compile(r"(?<![Nn]on)Current$|ShortTerm"),
}
_NEAR_EXCLUDE = re.compile(r"Lease|InterestPayable|Accrued|Receivable|IncludingPortionAttributableToNoncontrollingInterest|"
                           r"StockholdersEquityAttributableToParent|Expense|Income", re.I)
# Layer3 で補う項目（ROTCEのTCE・TANUKIが読む項目）
NO_BS_LINE_MARK = "貸借対照表に行なし"
# 「行が無ければ0」の分類（チャット側の決定、2026-11-04 夜）:
#   ZERO_ALWAYS    その申告の貸借対照表に行が無ければ0（あれば載せる項目。借入金は返済で行が消える〈CAKE・FLYW〉）
#   ZERO_IF_10K    10-Qに行が無くても0にしない（10-Qは「その他の資産」にまとめることがある〈WMT・AMZN・FCXの無形資産〉）。
#                  直近の10-K（その申告と同じか前に出た10-K）の貸借対照表にも行が無く、さらにLayer3に10-Kで載った値（注記を含むcompanyfacts）が
#                  直近の400日以内に無いときだけ0（PLTR型）。10-Kでも「その他の資産」にまとめて注記に値がある銘柄（WMT・AMZN・FCX）は0にしない
ZERO_ALWAYS = ("long_term_debt", "short_term_debt", "preferred_stock", "minority_interest")
ZERO_IF_10K = ("goodwill", "intangible_assets_excl_goodwill", "short_term_investments")
LAYER3_FIELDS = ("long_term_debt", "short_term_debt", "short_term_investments", "preferred_stock", "goodwill",
                 "intangible_assets_excl_goodwill", "minority_interest")


def classify(concept: str) -> Optional[str]:
    local = concept.split(":", 1)[-1]
    for kind, rx in RULES:
        if kind == "short_term_debt" and re.search(r"Non[Cc]urrent$", local):
            continue                                  # 「…NonCurrent」は長期（短期の規則の「…Current」に当たらないように）
        if rx.match(local):
            if kind in DEBT_FIELDS and _NOT_DEBT.search(local) and not _DEBT_WITH_LEASE.match(local):
                return None
            return kind
    return None


# ━━━ 抜き出し ━━━

def bs_role_lines(pre: bytes) -> Optional[Tuple[str, List[str]]]:
    """貸借対照表の表（資産合計と負債・純資産合計を両方含む、いちばん大きい表）の抽象でない行"""
    import xml.etree.ElementTree as ET
    root = ET.fromstring(pre)
    best = None
    for pl in root.iter(LINK + "presentationLink"):
        names = set()
        for loc in pl.iter(LINK + "loc"):
            names.add(loc.get(XL + "href", "").split("#", 1)[-1].replace("_", ":", 1))
        if not any(n.endswith(":Assets") for n in names) or not any(n.endswith(":LiabilitiesAndStockholdersEquity") for n in names):
            continue
        lines = sorted(n for n in names if not n.endswith("Abstract"))
        if best is None or len(lines) > len(best[1]):
            best = (pl.get(XL + "role", ""), lines)
    return best


def instant_values(inst: bytes, concepts: List[str]) -> Dict[str, Dict[str, float]]:
    """次元の無い時点の文脈だけ。{concept: {"YYYY-MM-DD": 値}}"""
    import io
    import xml.etree.ElementTree as ET
    root = ET.fromstring(inst)
    ctx = {}
    for c in root.iter(XBRLI + "context"):
        if c.find(XBRLI + "entity/" + XBRLI + "segment") is not None or c.find(XBRLI + "scenario") is not None:
            continue
        i = c.find(XBRLI + "period/" + XBRLI + "instant")
        if i is not None and i.text:
            ctx[c.get("id")] = i.text.strip()[:10]
    nsmap: Dict[str, str] = {}
    for _ev, (pfx, uri) in ET.iterparse(io.BytesIO(inst), events=["start-ns"]):
        nsmap.setdefault(uri, pfx)
    want = set(concepts)
    vals: Dict[str, Dict[str, float]] = {c: {} for c in concepts}
    for el in root.iter():
        if not isinstance(el.tag, str) or not el.tag.startswith("{") or el.get("contextRef") not in ctx:
            continue
        uri, local = el.tag[1:].split("}")
        name = f"{nsmap.get(uri, '?')}:{local}"
        if name not in want:
            continue
        try:
            vals[name][ctx[el.get("contextRef")]] = float(el.text)
        except (TypeError, ValueError):
            continue
    return vals


def extract(pre: Optional[bytes], inst: Optional[bytes]) -> Dict[str, Any]:
    if pre is None or inst is None:
        return {"status": "no_bs_statement", "reason": "インスタンスか表示の構造（_pre.xml・.xsd）が無い"}
    tr = bs_role_lines(pre)
    if not tr:
        return {"status": "no_bs_statement", "reason": "資産合計と負債・純資産合計を両方含む表が無い"}
    keep = [c for c in tr[1] if classify(c)]
    vals = instant_values(inst, keep)
    return {"status": "ok", "role": tr[0], "lines": [{"concept": c, "values": vals[c]} for c in keep if vals[c]],
            "concepts": tr[1]}   # 表の全部の行の名前（「行が無ければ0」の安全の条件に使う）


# ━━━ 保存 ━━━

def bs_lines_dir(ticker: str, data_dir: str = DATA_DIR) -> str:
    return os.path.join(data_dir, ticker.upper(), "bs_lines")


def save_record(ticker: str, accn: str, rec: Dict[str, Any], data_dir: str = DATA_DIR) -> None:
    d = bs_lines_dir(ticker, data_dir)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{accn}.json"), "w", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False, indent=1, sort_keys=True)


def load_records(ticker: str, data_dir: str = DATA_DIR) -> Dict[str, Dict[str, Any]]:
    d = bs_lines_dir(ticker, data_dir)
    out: Dict[str, Dict[str, Any]] = {}
    if not os.path.isdir(d):
        return out
    for accn in sorted(n[:-5] for n in os.listdir(d) if n.endswith(".json")):
        with open(os.path.join(d, f"{accn}.json"), encoding="utf-8") as f:
            out[accn] = json.load(f)
    return out


# ━━━ 補いの規則 ━━━

def record_values(rec: Dict[str, Any]) -> Dict[Tuple[str, str], float]:
    """1申告の {(分類, 時点): 値}（同じ分類の行は足す）"""
    out: Dict[Tuple[str, str], float] = {}
    for x in rec.get("lines") or []:
        k = classify(x["concept"])
        if not k:
            continue
        for d, v in (x.get("values") or {}).items():
            out[(k, d)] = out.get((k, d), 0.0) + v
    return out


def zero_fields(rec: Dict[str, Any]) -> List[str]:
    """その申告の貸借対照表に行が無く、近い名前の行も無い分類（当期の値を0と読む分類）"""
    inst = record_instant(rec)
    present = {k for (k, d) in record_values(rec) if d == inst}          # 当期の時点の行だけ（前の期の列は数えない）
    others = [c.split(":", 1)[-1] for c in rec.get("concepts") or [] if not classify(c)
              and not re.search(r"(Member|Axis|Domain|Table|LineItems)$", c)]   # 軸の要素は行ではない
    out = []
    with_values = {k for (k, _d) in record_values(rec)}           # どれかの時点に値がある分類（当期の欄が空でも前の期の列にある＝CAKE）
    listed = {classify(c) for c in rec.get("concepts") or []}
    for f in LAYER3_FIELDS:
        if f in present or (f in listed and f not in with_values):
            continue        # 行はあるがどの時点にも軸の無い値が無い（軸つきの値だけ、CATの部門別の借入金等）は0にしない＝不明。
            #                 前の期の列にだけ値がある（当期の欄が空、CAKEの1年内返済の借入金）は0
        ex = NEAR_EXCLUDE_BY_FIELD.get(f)
        if any(NEAR_NAMES[f].search(o) and not _NEAR_EXCLUDE.search(o) and not (ex and ex.search(o)) for o in others):
            continue
        out.append(f)
    return out


def record_instant(rec: Dict[str, Any]) -> Optional[str]:
    """申告の当期の時点（現金の行のいちばん新しい時点）"""
    ds = [d for (k, d) in record_values(rec) if k == "cash_and_equivalents"]
    return max(ds) if ds else None


def zero_fields_with_10k(rec: Dict[str, Any], records: Dict[str, Dict[str, Any]]) -> List[str]:
    """当期を0と読む分類（ZERO_ALWAYS は zero_fields のまま、ZERO_IF_10K は直近の10-Kの貸借対照表にも行が無いときだけ）"""
    z = zero_fields(rec)
    if not any(f in ZERO_IF_10K for f in z):
        return [f for f in z if f in ZERO_ALWAYS]
    tenk = [r for r in records.values() if r.get("status") == "ok" and str(r.get("form", "")).startswith("10-K")
            and r.get("concepts") and (r.get("filed") or "") <= (rec.get("filed") or "")]
    last = max(tenk, key=lambda r: r["filed"]) if tenk else None
    zk = set(zero_fields(last)) if last else set()
    return [f for f in z if f in ZERO_ALWAYS or (f in ZERO_IF_10K and f in zk)]


def values(records: Dict[str, Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """{(分類, 時点): {"val", "accn", "filed", "form"[, "no_bs_line"]}}（その時点を載せた最新の申告の値）。
    行が無く近い名前の行も無い分類は、その申告の当期の時点を0（no_bs_line=True）"""
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for accn, rec in sorted(records.items(), key=lambda kv: kv[1].get("filed") or ""):
        if rec.get("status") != "ok":
            continue
        items = {key: (v, False) for key, v in record_values(rec).items()}
        inst = record_instant(rec)
        if inst and rec.get("concepts"):
            for f in zero_fields_with_10k(rec, records):
                items.setdefault((f, inst), (0, True))
        for key, (v, z) in items.items():
            cur = out.get(key)
            if cur is None or rec["filed"] >= cur["filed"]:
                out[key] = {"val": _cf._num(v), "accn": accn, "filed": rec["filed"], "form": rec["form"], **({"no_bs_line": True} if z else {})}
    return out


ANNUAL_NOTE_DAYS = 400


def annual_note_value(field: str, entries: List[Dict[str, Any]], end: str) -> Optional[Dict[str, Any]]:
    """ZERO_IF_10K（無形資産・のれん・短期投資）で、end 以前 ANNUAL_NOTE_DAYS 日以内の年度末の値（注記を含むcompanyfacts）。
    10-Qに行が無いときは、この年次の値で補う（ROTCEの年次の補い〈rotce.component_at〉）。借入金等は年次で補ったとみなさない（古い値が残る＝CRWV型）。
    Layer3の貸借対照表の項目は is_annual を持たず、10-Kの値が後の10-Qに再掲されると form は10-Q になるので、fp="FY" か form=10-K で見る"""
    if field not in ZERO_IF_10K:
        return None
    prior = sorted((e for e in entries if (e.get("fp") == "FY" or str(e.get("form", "")).startswith("10-K"))
                    and e.get("val") and e["end"] <= end), key=lambda e: e["end"])
    if prior and (date.fromisoformat(end) - date.fromisoformat(prior[-1]["end"])).days <= ANNUAL_NOTE_DAYS:
        return prior[-1]
    return None


def covered_by_rules(field: str, entries: List[Dict[str, Any]], end: str) -> bool:
    """決めた補いの規則で end の値が埋まるか（[P]の4a〈タグの途切れ〉は埋まらない件だけ知らせる。source_watch.py が呼ぶ）。
    entries は bs_lines の補い（fill_layer3_bs）を当てた後のLayer3のエントリ。end の値がある（companyfacts か bs_lines〈行なしの0を含む〉）
    か、ZERO_IF_10K で直近の年度末の値（annual_note_value）があれば埋まる"""
    if any(e.get("end") == end and e.get("val") is not None for e in entries):
        return True
    return annual_note_value(field, entries, end) is not None


def fill_layer3_bs(ticker: str, fields_out: Dict[str, Any], data_dir: str = DATA_DIR) -> Dict[str, int]:
    """Layer3の貸借対照表の項目（LAYER3_FIELDS）に、純資産の値がある期末日で値が無い期を bs_lines の値で足す（その場で書き換え）。
    戻り値は項目ごとの足した数"""
    recs = load_records(ticker, data_dir)
    if not recs:
        return {}
    bv = values(recs)
    equity = {e["end"]: e for e in (fields_out.get("stockholders_equity") or {}).get("entries") or [] if e.get("val") is not None}
    added: Dict[str, int] = {}
    for f in LAYER3_FIELDS:
        fd = fields_out.setdefault(f, {"source_tag": None, "entries": []})
        have = {e["end"] for e in fd.get("entries") or [] if e.get("val") is not None}
        new = []
        never = not any(e.get("val") is not None for e in fd.get("entries") or [])
        for end, eq in equity.items():
            v = bv.get((f, end))
            if end in have or v is None:
                continue
            if v.get("no_bs_line") and never:
                continue        # 一度も申告の無い項目は足さない（ROTCEは0として扱っている。最近の期だけに0を足すと過去の期が欠けになる〈XOMの優先株〉）
            if v.get("no_bs_line") and annual_note_value(f, fd.get("entries") or [], end) is not None:
                continue        # 10-Kの貸借対照表に行が無くても、注記の値が直近の年にあれば0にしない（WMT・AMZN・FCXの無形資産）
            new.append({"end": end, "start": "", "val": v["val"], "first_val": v["val"], "accn": v["accn"], "form": v["form"],
                        "filed": v["filed"], "first_filed": v["filed"], "fp": eq.get("fp"), "fy": eq.get("fy"),
                        "period_days": 0, "is_ytd": False, "is_annual": bool(eq.get("is_annual")), "source_tag": "bs_lines",
                        **({"mark": NO_BS_LINE_MARK} if v.get("no_bs_line") else {})})
        if new:
            fd["entries"] = sorted((fd.get("entries") or []) + new, key=lambda e: (e["end"], bool(e.get("is_annual"))))
            added[f] = len(new)
    return added


FACE_DEBT_MARK = "bs_lines"      # 借入金を貸借対照表の行で置き換えた印（エントリの debt_basis）


def complete_debt_record(rec: Dict[str, Any]) -> bool:
    """その申告の貸借対照表の借入金の行が、保存した行（lines）に全部入っているか。表のコンセプトの一覧（concepts）に借入金と分類される
    名前があるのに lines に無い申告（保存した時点の分類の規則が今と違った古い記録。ABBVの「1年内返済の長期債務」の行など、全記録の約15%）は
    借入金の合計が過小になるので、貸借対照表の行を正にする置き換えには使わない。concepts が無い記録も確かめられないので使わない"""
    if rec.get("status") != "ok" or not rec.get("concepts"):
        return False
    have = {ln["concept"] for ln in rec.get("lines") or []}
    return not [c for c in rec["concepts"] if classify(c.split(":")[-1]) in DEBT_FIELDS and c not in have]


def _without_total_ltd_overlap(rec: Dict[str, Any]) -> Dict[str, Any]:
    """同じ時点に長期借入金の合計のタグ（LongTermDebt、1年内返済を含む）と非流動のタグ（…Noncurrent）の両方の行がある申告は、
    合計の行を除く（足すと非流動を二重に数える。全申告で RKLB の14件）。それ以外の複数の行（転換社債＋銀行借入など）は別の行として足す"""
    by_date: Dict[str, List[str]] = {}
    for ln in rec.get("lines") or []:
        if classify(ln["concept"]) == "long_term_debt":
            for d in ln.get("values") or {}:
                by_date.setdefault(d, []).append(ln["concept"].split(":")[-1])
    dup = {d for d, names in by_date.items() if "LongTermDebt" in names and any(n.endswith(("Noncurrent", "NonCurrent")) for n in names)}
    if not dup:
        return rec
    lines = []
    for ln in rec["lines"]:
        if ln["concept"].split(":")[-1] == "LongTermDebt":
            ln = dict(ln, values={d: v for d, v in ln["values"].items() if d not in dup})
        lines.append(ln)
    return dict(rec, lines=lines)


def prefer_face_debt(ticker: str, fields_out: Dict[str, Any], data_dir: str = DATA_DIR) -> Dict[str, int]:
    """Layer3 の long_term_debt・short_term_debt を、申告の貸借対照表の行（完全に取れた申告の最新）で置き換える（その場で書き換え）。
    置き換えるのは Layer3 に値があり、行の値と違う期だけ（short_term_debt は行の値の方が大きい期だけ）（欠けの補いは fill_layer3_bs）。val・first_val を同じ差だけずらし
    （最初の申告との差を保つ。cf_lines の capex と同じ）、印 debt_basis="bs_lines"・置き換え前の値 val_tag_val を残す。
    行が無く0になった期（no_bs_line）では置き換えない。戻り値は項目ごとの置き換えた数"""
    recs = {a: _without_total_ltd_overlap(r) for a, r in load_records(ticker, data_dir).items() if complete_debt_record(r)}
    if not recs:
        return {}
    bv = values(recs)
    changed: Dict[str, int] = {}
    for f in DEBT_FIELDS:
        for e in (fields_out.get(f) or {}).get("entries") or []:
            if e.get("is_ytd") or e.get("val") is None or e.get("source_tag") == "bs_lines":
                continue
            v = bv.get((f, e["end"]))
            if v is None or v.get("no_bs_line") or abs(v["val"] - e["val"]) <= 0.5:
                continue
            if f == "short_term_debt" and v["val"] < e["val"]:
                continue        # 流動の借入金は、行の値がタグより小さいときは使わない（行の名前の規則が拾わない行があり得る〈KOの Loans and notes payable〉）。行がタグの取りこぼしを足すときだけ
            delta = v["val"] - e["val"]
            e["val_tag_val"] = e["val"]
            e["val"] = _cf._num(v["val"])
            if e.get("first_val") is not None:
                e["first_val"] = _cf._num(e["first_val"] + delta)
            if e.get("_asof"):
                e["_asof"] = [[fl, x + delta if x is not None else x] for fl, x in e["_asof"]]
            e["debt_basis"] = FACE_DEBT_MARK
            changed[f] = changed.get(f, 0) + 1
    return changed


def latest_balance(ticker: str, cash: Optional[float], data_dir: str = DATA_DIR) -> Dict[str, Any]:
    """TANUKIの純有利子負債の補い: 最新の申告の貸借対照表の当期（いちばん新しい時点）の値。現金の行が cash と同じときだけ返す
    （同じ時点の確認）。{"end", "accn", 分類: 値, ...} か {}"""
    allrecs = load_records(ticker, data_dir)
    recs = [r for r in allrecs.values() if r.get("status") == "ok"]
    if not recs or cash is None:
        return {}
    rec = max(recs, key=lambda r: r["filed"])
    rv = record_values(rec)
    ends = sorted({d for _k, d in rv})
    if not ends:
        return {}
    end = ends[-1]
    if abs(rv.get(("cash_and_equivalents", end), float("nan")) - float(cash)) > 0.5:
        return {}
    out: Dict[str, Any] = {"end": end, "accn": rec["accn"], "no_bs_line": []}
    for (k, d), v in rv.items():
        if d == end and k != "cash_and_equivalents":
            out[k] = _cf._num(v)
    if rec.get("concepts"):
        for f in zero_fields_with_10k(rec, allrecs):
            if f in ZERO_IF_10K:
                continue        # TANUKIの短期投資は、四半期に無ければ今も0として扱う（sti_quarterly_missing）。ここでは0にしない
            out.setdefault(f, 0)
            out["no_bs_line"].append(f)
    return out
