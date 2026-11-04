"""
common/sec_data/cf_lines.py
申告のCF計算書（投資CFの区分）の行の取得・保存と、CapExの欠けの規則（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段5a・段5b、2026-11-04）

companyfactsのAPIには、会社の独自タグ（例: LYFTの lyft:PaymentsToAcquirePropertyAndEquipmentAndScooterFleet）と
「CF計算書にCapExの行が無い」こと（APPの2023年以降）が出ない。申告ごとのXBRLインスタンスと表示の構造
（_pre.xml、無ければ .xsd に埋め込まれた linkbase）から、CF計算書の投資CFの区分の行と値を取り、
common/sec_data/data/{T}/cf_lines/{accn}.json に保存する（生のファイルは保存しない。一度取った申告は取り直さない）。

段5a（取得・保存）: fetch_new_filings()。SEC_Data_Updateの1銘柄ごとに、company_factsに出てくる2021年以降の
  10-K・10-Q（/Aを含む）のうち、まだ保存していない申告だけを取る（1申告3リクエスト: ディレクトリの一覧・
  インスタンス・_pre.xml か .xsd）。間隔は MIN_GAP_SEC 以上（毎秒10件未満）、1回の実行の上限は MAX_REQUESTS_PER_RUN。
  User-Agent は common/external_api.py の sec_get（環境変数 SEC_USER_AGENT_EMAIL、CIは既存のsecret）。
  保存の status: ok（行あり）・no_cf_statement（CF計算書の無い申告。10-K/Aの第3部だけの訂正、合併前のSPAC等）・
  not_found（SECが404。ディレクトリかファイルが無い）。通信の失敗・5xxは保存せず、次の実行で取り直す。

行の特定（extract()）: 営業CFと投資CFの合計（NetCashProvidedByUsedIn{Operating,Investing}Activities、抽象でない）を
  両方含む表のうち、いちばん大きいものをCF計算書とする（表の名前に頼らない）。木を順に平らにし、直前の合計の行
  （…Activities）の次から投資CFの合計までを投資CFの区分とする（平らな木・入れ子の木の両方）。値は次元の無い文脈だけ。

行の種類（classify()、読むときに決める。保存はタグと値だけ）:
  ppe（有形固定資産等の取得。独自タグ・取得と売却の純額の行〈LOARの PaymentsForProceedsFromProductiveAssets〉を含む）・
  software（ソフトウェアの資産計上・取得）・other_acq（無形資産の取得等）・excluded（企業買収・投資・有価証券・売却収入等）・
  total（投資CFの合計）

段5b（CapExの欠けの規則、ここの1か所）:
  - 申告のCF計算書の投資CFの区分に、取得系の行（ppe・software）が無ければ、その申告が最新の期のCapExは0
    （印 fcf.CAPEX_NO_CF_LINE_MARK「CF計算書にCapExの行なし」）
  - Layer3に無い期（単独の四半期〈Q4はTTMの逆算に任せる〉・年次）は、cf_linesの ppe の行で埋める（案A。software は含めない＝段5c）
  - parserの年次: その年の10-Kの行で、行が無ければ0（印）、parserに値が無ければ ppe で埋める。それでも無い年は欠け（None）
"""
from __future__ import annotations

import gzip
import io
import json
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .fcf import CAPEX_NO_CF_LINE_MARK

_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(_MODULE_DIR, "data")
SEC_BASE = "https://www.sec.gov"

FROM_FILED = "2021-01-01"          # 取る申告の提出日の下限
FORMS = ("10-K", "10-Q", "10-K/A", "10-Q/A")
MIN_GAP_SEC = 0.13                 # リクエストの始まりの間隔（毎秒10件未満。sec_get も0.11秒を守る）
MAX_REQUESTS_PER_RUN = 3000        # 1回の実行で取るリクエストの上限（約1,000申告）

LINK = "{http://www.xbrl.org/2003/linkbase}"
XL = "{http://www.w3.org/1999/xlink}"
XBRLI = "{http://www.xbrl.org/2003/instance}"

_EXCL = re.compile(r"Proceed|Sale|Business|Acquisitions?NetOfCash|Investment|Securit|Marketable|Equity|Loan|Debt|Deposit|"
                   r"Divest|Disposal|Maturit|Restricted|Notes?Receivable|Interest|Collateral|Derivative|Hedg|Lease(?!hold)", re.I)
_PPE = re.compile(r"(PropertyPlantAndEquipment|PropertyAndEquipment|ProductiveAssets|CapitalExpenditure|FixedAssets|"
                  r"Equipment|Leasehold|RealEstate|Facilit|Building|Construction|ScooterFleet|Fleet|ProductiveAsset)", re.I)
_ACQ = re.compile(r"^(Payments?|Purchases?|Acquisitions?|Capitaliz|Capital|Additions?|Investments?In(Property|Software))", re.I)


# ━━━ 行の種類 ━━━

def classify(concept: str) -> str:
    local = concept.split(":", 1)[-1]
    if re.match(r"NetCashProvidedByUsedInInvestingActivities", local):
        return "total"
    if re.match(r"PaymentsForProceedsFrom(ProductiveAssets|PropertyPlantAndEquipment|OtherProductiveAssets)", local):
        return "ppe"  # 取得と売却の純額の行（LOAR等）
    if not _ACQ.match(local) and not re.search(r"Capitaliz|Expenditure", local, re.I):
        return "excluded"
    if re.search(r"Software", local, re.I) and not re.search(r"Proceed|Sale", local, re.I):
        return "software"
    if _EXCL.search(local) and not _PPE.search(local):
        return "excluded"
    if re.search(r"Proceed|Sale|Business", local, re.I):
        return "excluded"
    if _PPE.search(local):
        return "ppe"
    return "other_acq"


# ━━━ 抜き出し ━━━

def cf_role_tree(pre: bytes) -> Optional[Tuple[str, list]]:
    """CF計算書の表: 営業CFと投資CFの合計（抽象でない）を両方含む表のうち、いちばん大きいもの"""
    root = ET.fromstring(pre)
    best = None
    for pl in root.iter(LINK + "presentationLink"):
        loc = {}
        for l in pl.iter(LINK + "loc"):
            frag = l.get(XL + "href", "").split("#", 1)[-1]
            loc[l.get(XL + "label")] = frag.replace("_", ":", 1)
        names = set(loc.values())
        if not any(re.search(r":NetCashProvidedByUsedInInvestingActivities", n) and not n.endswith("Abstract") for n in names):
            continue
        if not any(re.search(r":NetCashProvidedByUsedInOperatingActivities", n) and not n.endswith("Abstract") for n in names):
            continue
        arcs = [(loc.get(a.get(XL + "from")), loc.get(a.get(XL + "to")), float(a.get("order") or 0))
                for a in pl.iter(LINK + "presentationArc")]
        if best is None or len(arcs) > len(best[1]):
            best = (pl.get(XL + "role", ""), arcs)
    return best


def investing_lines(arcs: list) -> List[str]:
    """木を順に平らにし、直前の「…Activities」の合計の次から投資CFの合計までを投資CFの区分とする"""
    kids: Dict[str, list] = {}
    tos = set()
    for f, t, o in arcs:
        kids.setdefault(f, []).append((o, t))
        tos.add(t)
    flat: List[str] = []
    seen = set()

    def walk(n):
        for _o, c in sorted(kids.get(n, []), key=lambda x: x[0]):
            if c is None or c in seen:
                continue
            seen.add(c)
            flat.append(c)
            walk(c)

    for r in [f for f in kids if f not in tos]:
        walk(r)
    tot = [i for i, c in enumerate(flat)
           if re.search(r":NetCashProvidedByUsedInInvestingActivities", c) and not c.endswith("Abstract")]
    if not tot:
        return []
    end = tot[0]
    start = 0
    for i in range(end - 1, -1, -1):
        if re.search(r":(NetCashProvidedByUsedIn\w*Activities|CashProvidedByUsedIn\w*Activities)", flat[i]) \
                and not flat[i].endswith("Abstract"):
            start = i + 1
            break
    return [c for c in flat[start:end + 1] if not c.endswith("Abstract")]


def instance_values(inst: bytes, concepts: List[str]) -> Dict[str, Dict[str, float]]:
    """次元の無い期間の文脈だけ。{concept: {"start~end": 値}}"""
    root = ET.fromstring(inst)
    ctx = {}
    for c in root.iter(XBRLI + "context"):
        if c.find(XBRLI + "entity/" + XBRLI + "segment") is not None or c.find(XBRLI + "scenario") is not None:
            continue
        p = c.find(XBRLI + "period")
        s, e = p.find(XBRLI + "startDate"), p.find(XBRLI + "endDate")
        if s is not None and e is not None:
            ctx[c.get("id")] = f"{s.text.strip()}~{e.text.strip()}"
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
            v = float(el.text)
        except (TypeError, ValueError):
            continue
        vals[name][ctx[el.get("contextRef")]] = v
    return vals


def extract(pre: Optional[bytes], inst: Optional[bytes]) -> Dict[str, Any]:
    """1申告の投資CFの区分の行。{"status": "ok", "role", "lines": [{"concept", "values"}]} か
    {"status": "no_cf_statement", "reason"}"""
    if pre is None or inst is None:
        return {"status": "no_cf_statement", "reason": "インスタンスか表示の構造（_pre.xml・.xsd）が無い"}
    tr = cf_role_tree(pre)
    if not tr:
        return {"status": "no_cf_statement", "reason": "営業CFと投資CFの合計を両方含む表が無い"}
    lines = investing_lines(tr[1])
    if not lines:
        return {"status": "no_cf_statement", "reason": "投資CFの区分を特定できない", "role": tr[0]}
    vals = instance_values(inst, lines)
    return {"status": "ok", "role": tr[0], "lines": [{"concept": c, "values": vals[c]} for c in lines]}


# ━━━ 保存 ━━━

def cf_lines_dir(ticker: str, data_dir: str = DATA_DIR) -> str:
    return os.path.join(data_dir, ticker.upper(), "cf_lines")


def save_record(ticker: str, accn: str, rec: Dict[str, Any], data_dir: str = DATA_DIR) -> None:
    d = cf_lines_dir(ticker, data_dir)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{accn}.json"), "w", encoding="utf-8") as f:
        json.dump(rec, f, ensure_ascii=False, indent=1, sort_keys=True)


def load_records(ticker: str, data_dir: str = DATA_DIR) -> Dict[str, Dict[str, Any]]:
    d = cf_lines_dir(ticker, data_dir)
    out: Dict[str, Dict[str, Any]] = {}
    if not os.path.isdir(d):
        return out
    for accn in sorted(n[:-5] for n in os.listdir(d) if n.endswith(".json")):
        with open(os.path.join(d, f"{accn}.json"), encoding="utf-8") as f:
            out[accn] = json.load(f)
    return out


def target_filings(company_facts: Dict[str, Any]) -> Dict[str, Tuple[str, str]]:
    """company_factsに出てくる2021年以降の10-K・10-Q（/Aを含む）。{accn: (form, filed)}"""
    acc: Dict[str, Tuple[str, str]] = {}
    for ns in (company_facts or {}).get("facts", {}).values():
        for tag in ns.values():
            for u in tag.get("units", {}).values():
                for e in u:
                    if e.get("form") in FORMS and e.get("filed", "") >= FROM_FILED and e.get("accn"):
                        acc.setdefault(e["accn"], (e["form"], e["filed"]))
    return acc


# ━━━ 取得（段5a） ━━━

_last = [0.0]
_lock = threading.Lock()


class Budget:
    def __init__(self, limit: int):
        self.limit, self.used = limit, 0


def _get(url: str, label: str, budget: Budget, js: bool = False):
    from common.external_api import sec_get
    if budget.used >= budget.limit:
        raise RuntimeError("1回の実行のリクエストの上限に達した")
    with _lock:
        w = MIN_GAP_SEC - (time.monotonic() - _last[0])
        if w > 0:
            time.sleep(w)
        _last[0] = time.monotonic()
        budget.used += 1
    return sec_get(url, label, expect_json=js)


def _pick_files(items: List[str]) -> Tuple[Optional[str], Optional[str]]:
    inst = [n for n in items if n.endswith("_htm.xml")]
    if not inst:
        inst = [n for n in items if n.lower().endswith(".xml")
                and not re.search(r"(_cal|_def|_lab|_pre)\.xml$|filingsummary|^R\d+\.xml$", n, re.I)]
    pre = [n for n in items if n.endswith("_pre.xml")] or [n for n in items if n.endswith(".xsd")]
    return (inst[0] if inst else None), (pre[0] if pre else None)


def fetch_filing(cik: int, accn: str, form: str, filed: str, budget: Budget) -> Optional[Dict[str, Any]]:
    """1申告を取って記録を返す。通信の失敗・5xxは None（保存しない＝次の実行で取り直す）"""
    base = f"{SEC_BASE}/Archives/edgar/data/{int(cik)}/{accn.replace('-', '')}"
    head = {"accn": accn, "form": form, "filed": filed}
    r = _get(base + "/index.json", f"cf_lines {accn} index", budget, js=True)
    if r.status_code == 404:
        return {**head, "status": "not_found", "reason": "SECが404（申告のディレクトリが無い）"}
    if r.status_code != 200:
        return None
    items = [i["name"] for i in r.json().get("directory", {}).get("item", [])]
    inst_n, pre_n = _pick_files(items)
    if not inst_n or not pre_n:
        return {**head, "status": "no_cf_statement", "reason": "インスタンスか表示の構造（_pre.xml・.xsd）が無い"}
    got = {}
    for kind, n in (("instance", inst_n), ("pre", pre_n)):
        rr = _get(f"{base}/{n}", f"cf_lines {accn} {kind}", budget)
        if rr.status_code == 404:
            return {**head, "status": "not_found", "reason": f"SECが404（{n}）"}
        if rr.status_code != 200:
            return None
        got[kind] = rr.content
    rec = extract(got["pre"], got["instance"])
    return {**head, **rec, "source_files": {"instance": inst_n, "pre": pre_n}}


def fetch_new_filings(ticker: str, cik: int, company_facts: Dict[str, Any], data_dir: str = DATA_DIR,
                      budget: Optional[Budget] = None) -> Dict[str, int]:
    """まだ保存していない申告だけを取って保存する。戻り値は件数（取得・保存・未保存〈次の実行で取り直す〉）"""
    budget = budget or Budget(MAX_REQUESTS_PER_RUN)
    have = set(load_records(ticker, data_dir))
    todo = sorted(((a, f, d) for a, (f, d) in target_filings(company_facts).items() if a not in have), key=lambda x: x[2])
    n = {"new": len(todo), "saved": 0, "retry_later": 0}
    for accn, form, filed in todo:
        try:
            rec = fetch_filing(cik, accn, form, filed, budget)
        except RuntimeError:
            n["retry_later"] += len(todo) - n["saved"] - n["retry_later"]
            break
        except Exception as e:  # noqa: BLE001 — 通信の失敗は次の実行で取り直す
            print(f"   [WARN] cf_lines {accn}: {e}")
            rec = None
        if rec is None:
            n["retry_later"] += 1
            continue
        save_record(ticker, accn, rec, data_dir)
        n["saved"] += 1
    return n


# ━━━ CapExの欠けの規則（段5b） ━━━

def _num(v: float):
    """整数の値は int で書く（companyfactsの値と同じ書き方）"""
    return int(v) if float(v).is_integer() else v


def _months(s: str, e: str) -> int:
    return round((date.fromisoformat(e) - date.fromisoformat(s)).days / 30.44)


def period_values(records: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """期間（start~end）ごとに、その期間を載せた最新の申告の {ppe, software, other_acq, has_capex_line, accn, filed, form}。
    投資CFの合計の無い期間（別表の期間等）は除く"""
    out: Dict[str, Dict[str, Any]] = {}
    for accn, rec in records.items():
        if rec.get("status") != "ok":
            continue
        kinds = [(classify(x["concept"]), x) for x in rec["lines"]]
        has_line = any(k in ("ppe", "software") for k, _x in kinds)
        tot = next((x for k, x in kinds if k == "total"), None)
        periods = set()
        for _k, x in kinds:
            periods |= set(x["values"])
        for p in periods:
            if tot is not None and p not in tot["values"]:
                continue
            v = {"ppe": 0.0, "software": 0.0, "other_acq": 0.0, "has_capex_line": has_line,
                 "accn": accn, "filed": rec["filed"], "form": rec["form"]}
            for k, x in kinds:
                if k in ("ppe", "software", "other_acq") and p in x["values"]:
                    v[k] += x["values"][p]
            cur = out.get(p)
            if cur is None or v["filed"] >= cur["filed"]:
                out[p] = v
    return out


def capex_periods(records: Dict[str, Dict[str, Any]]) -> Dict[Tuple[str, bool], Dict[str, Any]]:
    """単独の四半期・年次の CapEx（ppe の行。CapExの行が無い申告の期は0）。{(end, is_annual): {...}}。
    四半期は3か月の列を優先し、無ければ同じ申告の中のYTDの差（6か月−3か月・9か月−6か月）、同じ申告に前の期間が無いときだけ
    別の申告の値と引く。負になる四半期は使わない。Q4（12か月−9か月）は作らない（TTMのQ4の逆算〈q4_implied.py〉に任せる）。
    年次は10-K（/Aを含む）の12か月だけ（10-QのTTMの列は使わない）。期末は2021年より前でもよい（2021年以降の申告の前年の列）"""
    pv = period_values(records)

    def val(v):
        return 0.0 if not v["has_capex_line"] else v["ppe"]

    by_filing: Dict[str, Dict[Tuple[str, str], Dict[str, Any]]] = {}
    for p, v in pv.items():
        s, e = p.split("~")
        by_filing.setdefault(v["accn"], {})[(s, e)] = v
    out: Dict[Tuple[str, bool], Dict[str, Any]] = {}
    for accn, per in by_filing.items():
        for (s, e), v in per.items():
            m = _months(s, e)
            base = {"accn": v["accn"], "filed": v["filed"], "form": v["form"],
                    "no_capex_line": not v["has_capex_line"]}
            if m == 12 and v["form"].startswith("10-K"):
                out.setdefault((e, True), {**base, "start": s, "val": val(v)})
            if m == 3:
                out[(e, False)] = {**base, "start": s, "val": val(v)}
            elif m in (6, 9) and (e, False) not in out:  # Q4（12か月−9か月）は作らない（TTMのQ4の逆算に任せる）
                prev = [k for k in per if k[0] == s and _months(*k) == m - 3]
                src = per
                if not prev:
                    prev = [tuple(p.split("~")) for p in pv if p.split("~")[0] == s and _months(*p.split("~")) == m - 3]
                    src = {k: pv[f"{k[0]}~{k[1]}"] for k in prev}
                if prev:
                    q = val(v) - val(src[prev[0]])
                    if q >= 0:
                        qs = date.fromordinal(date.fromisoformat(prev[0][1]).toordinal() + 1).isoformat()
                        out[(e, False)] = {**base, "start": qs, "val": q}
    return out


def fill_layer3_capex(ticker: str, entries: List[Dict[str, Any]], data_dir: str = DATA_DIR) -> List[Dict[str, Any]]:
    """Layer3の capital_expenditure のエントリに段5bの規則を当てた新しいリストを返す（元のリストは変えない）。
    - CapExの行が無い申告が最新の期: 0（印）。Layer3に値があっても置き換える
    - Layer3に無い期（単独の四半期〈Q4を除く〉・年次。cf_linesは2021年以降の申告で、前年の列を含む）: cf_lines の ppe の行で埋める"""
    recs = load_records(ticker, data_dir)
    if not recs:
        return entries
    cp = capex_periods(recs)
    have = {(e["end"], bool(e.get("is_annual"))) for e in entries if not e.get("is_ytd") and e.get("val") is not None}
    fy = {e["end"]: e.get("fy") for e in entries if e.get("fy") is not None}
    keep, add = [], []
    zero_keys = {k for k, v in cp.items() if v["no_capex_line"]}
    for e in entries:
        if not e.get("is_ytd") and (e["end"], bool(e.get("is_annual"))) in zero_keys:
            continue
        keep.append(e)
    for (end, ann), v in sorted(cp.items()):
        if (end, ann) in have and (end, ann) not in zero_keys:
            continue
        n = {"end": end, "start": v["start"], "val": _num(v["val"]), "first_val": _num(v["val"]), "accn": v["accn"],
             "filed": v["filed"], "first_filed": v["filed"], "form": v["form"],
             "fp": "FY" if ann else "Q", "fy": fy.get(end, int(end[:4])), "is_ytd": False, "is_annual": ann,
             "period_days": (date.fromisoformat(end) - date.fromisoformat(v["start"])).days,
             "source_tag": "cf_lines", "capex_basis": "cf_lines"}
        if v["no_capex_line"]:
            n["capex_basis"] = "no_cf_line"
            n["mark"] = CAPEX_NO_CF_LINE_MARK
        add.append(n)
    return sorted(keep + add, key=lambda e: (e["end"], bool(e.get("is_annual")), bool(e.get("is_ytd"))))


def annual_capex_from_10k(ticker: str, accn: Optional[str], data_dir: str = DATA_DIR) -> Optional[Dict[str, Any]]:
    """parserの年次: その年の10-K（accn）の当期の12か月の CapEx。{"val", "no_capex_line", "accn"} か None（記録が無い）"""
    if not accn:
        return None
    rec = load_records(ticker, data_dir).get(accn)
    if not rec or rec.get("status") != "ok":
        return None
    pv = period_values({accn: rec})
    yrs = [(p.split("~")[1], v) for p, v in pv.items() if _months(*p.split("~")) == 12]
    if not yrs:
        return None
    end, v = max(yrs, key=lambda x: x[0])
    return {"end": end, "val": 0 if not v["has_capex_line"] else _num(v["ppe"]),
            "no_capex_line": not v["has_capex_line"], "accn": accn}
