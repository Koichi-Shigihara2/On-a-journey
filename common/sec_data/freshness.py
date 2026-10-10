"""
SECの一次データ（company_facts.json）の鮮度の判定と記録（[[SEC-FETCH-CACHE-MTIME-CI-1]]、2026-10-10）。

背景: fetcher.pyの24時間キャッシュはファイルのmtimeで判定しており、actions/checkout後の
mtimeはチェックアウトの時刻になるため、SEC_Data_Update（CI）は一度もSEC APIを呼んで
いなかった（company_facts.jsonは2026-08-30の手動実行から更新されていなかった）。

方式: submissionsを毎回取り直し、手元のcompany_facts.jsonに無い10-K/10-Q(/A)の提出がある
銘柄だけcompany_factsを取り直す。判定にmtimeは使わない。結果は_freshness.jsonに記録し、
report_consistency_check.py（CHECK-60、WARN）とSystem Health（[M]）が読む。
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

FRESHNESS_FILENAME = "_freshness.json"

# 分類
OK_NO_NEW = "OK_NO_NEW"          # 新しい提出なし（手元のcompany_factsをそのまま使う）
REFRESHED = "REFRESHED"          # 取り直して反映（company_facts.jsonが無かった銘柄の初回取得を含む）
SEC_LAG = "SEC_LAG"              # 提出はあるが、取り直してもcompanyfactsに入っていない（SEC側の未反映）
FETCH_FAILED = "FETCH_FAILED"    # submissionsかcompany_factsの取得に失敗（手元のファイルで続行）
STATUSES = (OK_NO_NEW, REFRESHED, SEC_LAG, FETCH_FAILED)

# 閾値（CHECK-60とSystem Health [M]が共有する。変えるときはここだけ）
SEC_LAG_WARN_DAYS = 14                 # CHECK-60: SEC_LAGが提出日からこの日数を超えたらWARN
SEC_LAG_CRITICAL_DAYS = 30             # [M]: SEC_LAGが提出日からこの日数を超えた銘柄があればCRITICAL
FRESHNESS_MAX_AGE_DAYS = 8             # [M]: generated_at（全銘柄の実行の時刻）がこの日数より古ければCRITICAL
FETCH_FAILED_CRITICAL_RATIO = 0.10     # [M]: FETCH_FAILEDが全銘柄のこの割合以上ならCRITICAL


def default_path(data_dir: Optional[str] = None) -> str:
    base = data_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    return os.path.join(base, FRESHNESS_FILENAME)


def facts_index(company_facts: Optional[dict]) -> tuple[str, set[str]]:
    """company_factsの全taxonomy（us-gaap・dei等）から、filedの最大値Lとaccnの集合を返す。

    deiも含める: Part IIIだけの10-K/A等は財務のfactを持たないが、表紙のdeiのfactで
    accnがcompanyfactsに入る（2026-10-10、2024年以降の1,088件の提出で確認）。
    """
    latest = ""
    accns: set[str] = set()
    for concepts in ((company_facts or {}).get("facts") or {}).values():
        for concept in concepts.values():
            for entries in (concept.get("units") or {}).values():
                for e in entries:
                    accn = e.get("accn")
                    if accn:
                        accns.add(accn)
                    filed = e.get("filed") or ""
                    if filed > latest:
                        latest = filed
    return latest, accns


def pending_filings(accn_to_filingdate: dict[str, str], latest_filed: str,
                    facts_accns: Iterable[str]) -> list[dict[str, str]]:
    """submissionsの10-K/10-Q(/A)のうち、手元のcompany_factsに入っていない新しい提出。

    条件: filingDate >= L（手元の最大filed）かつ、そのaccnがcompany_factsに無い。
    filingDate > L の提出は定義上company_factsに無いので「filingDate > L」と同じで、
    Lと同じ日の別の提出だけを追加で拾う。L より前の提出は対象にしない（過去の
    欠け〈例 ASTSの2025-09-12の10-Q/A〉で毎週取り直さないため）。
    """
    have = set(facts_accns)
    out = [
        {"accn": accn, "filing_date": fd}
        for accn, fd in (accn_to_filingdate or {}).items()
        if fd and fd >= (latest_filed or "") and accn not in have
    ]
    return sorted(out, key=lambda x: (x["filing_date"], x["accn"]))


def lag_days(filing_date: str, today: Optional[date] = None) -> Optional[int]:
    try:
        d = date.fromisoformat(filing_date[:10])
    except (TypeError, ValueError):
        return None
    return ((today or datetime.now(timezone.utc).date()) - d).days


def entry(status: str, case: str, latest_filed: str, latest_submission: Optional[str],
          pending: list[dict[str, str]], note: str = "") -> dict[str, Any]:
    """_freshness.jsonの1銘柄分。case は判定の分岐（a: company_factsが無い / b: 新しい提出あり /
    c: 新しい提出なし / submissions_failed: submissionsが取れず判定できない）。"""
    assert status in STATUSES, status
    return {
        "status": status,
        "case": case,
        "local_latest_filed": latest_filed or None,
        "latest_submission_filing_date": latest_submission,
        "pending": pending,
        "note": note,
        "checked_at": utc_now_iso(),
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load(path: Optional[str] = None) -> Optional[dict]:
    p = path or default_path()
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def write(entries: dict[str, dict], full_run: bool, path: Optional[str] = None) -> dict:
    """_freshness.jsonを書く。

    全銘柄の実行（full_run=True）は全体を上書きし、generated_atを更新する。銘柄を絞った実行
    （新規登録・workflow_dispatchのtickers指定）は、その銘柄の行だけ置き換えてgenerated_atは
    変えない（last_partial_run_atだけ更新）。絞った実行でgenerated_atが新しくなると、週次の
    全銘柄の実行が止まったことをSystem Health [M]が見落とすため。
    """
    p = path or default_path()
    now = utc_now_iso()
    if full_run:
        doc = {"generated_at": now, "tickers": dict(sorted(entries.items()))}
    else:
        doc = load(p) or {"generated_at": None, "tickers": {}}
        doc.setdefault("tickers", {}).update(entries)
        doc["tickers"] = dict(sorted(doc["tickers"].items()))
        doc["last_partial_run_at"] = now
    doc["counts"] = count_statuses(doc["tickers"])
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return doc


def count_statuses(tickers: dict[str, dict]) -> dict[str, int]:
    counts = {s: 0 for s in STATUSES}
    for e in tickers.values():
        s = (e or {}).get("status")
        if s in counts:
            counts[s] += 1
    return counts


def max_pending_lag(e: dict, today: Optional[date] = None,
                    exclude_accns: Iterable[str] = ()) -> Optional[int]:
    """1銘柄の未反映の提出のうち、最も古いものの提出日からの日数（exclude_accnsは除く）。"""
    skip = set(exclude_accns)
    lags = [lag_days(p.get("filing_date", ""), today) for p in (e or {}).get("pending") or []
            if p.get("accn") not in skip]
    lags = [x for x in lags if x is not None]
    return max(lags) if lags else None


# 確認済みのSEC_LAG（既存の台帳config/warn_acknowledged.jsonを使う。新しい台帳は作らない）。
# エントリは {"check": "WARN-60", "ticker": "KO", "match": "<accn>", ...}。CHECK-60はSEC_LAGを
# accnごとに1行で出すので、report_consistency_check.pyのannotate_warn()（matchを含む行だけ
# 確認済み）がそのまま使える。System Health [M]はこの関数で同じエントリを読み、確認済みの
# accnをCRITICALの判定から外す（詳細には「確認済み」と表示し続ける）。
ACK_CHECK_ID = "WARN-60"
ACK_LEDGER_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                               "config", "warn_acknowledged.json")


def acknowledged_accns(path: Optional[str] = None) -> set[tuple[str, str]]:
    """warn_acknowledged.jsonのWARN-60のエントリから {(ticker, accn)} を返す（無い・読めないときは空）。"""
    try:
        with open(path or ACK_LEDGER_PATH, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return set()
    return {
        (e["ticker"], e["match"]) for e in data.get("acknowledged", [])
        if e.get("check") == ACK_CHECK_ID and e.get("ticker") and e.get("match")
    }


def step_summary_markdown(doc: dict) -> str:
    counts = doc.get("counts") or count_statuses(doc.get("tickers") or {})
    lines = ["", "## SECデータの鮮度（submissionsで新しい提出を検知）", "",
             "| 分類 | 件数 |", "|:---|---:|"]
    lines += [f"| {s} | {counts.get(s, 0)} |" for s in STATUSES]
    for s in (REFRESHED, SEC_LAG, FETCH_FAILED):
        rows = [(t, e) for t, e in (doc.get("tickers") or {}).items() if e.get("status") == s]
        if not rows:
            continue
        lines += ["", f"**{s}**（{len(rows)}）", ""]
        for t, e in rows:
            pend = ", ".join(f"{p['accn']}（{p['filing_date']}）" for p in e.get("pending") or [])
            extra = f" 未反映: {pend}" if pend else ""
            note = f" {e['note']}" if e.get("note") else ""
            lines.append(f"- {t}: 手元の最大filed {e.get('local_latest_filed')}・"
                         f"submissionsの最新 {e.get('latest_submission_filing_date')}{extra}{note}")
    return "\n".join(lines) + "\n"
