#!/usr/bin/env python3
"""
common/registration/preflight_check.py
[[QUALITY-GATES-EPIC-1]]ゲート0（[[PREFLIGHT-CHECK-1]]想定機能①〜④）。

register_ticker.pyのStep 0.5直後（Step 1本実行前）に、SEC EDGARの
公開情報のみから機械的に判定できる3種のリスクフラグを提示する:
    ① 上場後3年未満（初回10-K/10-Q提出日から算出）
       → データ不安定リスクあり
    ② 直近の年次/四半期報告書提出が20-F/40-F等（10-K/10-Q以外）
       → 四半期データ欠落の可能性
    ③ 収益系XBRLタグ（Revenues等）が1つも存在しない
       → 売上ゼロ企業の可能性

いずれもフラグ立て（判断材料の提示）のみで、登録処理を自動停止しない
（想定機能④）。登録前の事前チェックのため、ローカルファイルへの
書き込みは一切行わない（Step 1のcompany_facts.jsonキャッシュとは
独立した読み取り専用チェック）。

背景（ENBの実例）: ENB（IFRS/40-F企業、SEC annual data 0件のまま
遡及登録され孤立、2026-09-05に管理対象除外済み）は、本チェックが
実装されていれば①（上場履歴の10-K/10-Q不在）で登録前に検知できた
はずのケース。

Usage:
    python common/registration/preflight_check.py TICKER [TICKER2 ...]
"""
from __future__ import annotations

import argparse
import sys
import os
import time
from dataclasses import dataclass, field
from datetime import date

import requests

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data import tickers as _tickers_mod  # noqa: E402
from common.sec_data.parser import SECParser  # noqa: E402

from common.external_api import sec_get  # noqa: E402  User-Agent・毎秒10件の制限・再試行（[[EXTERNAL-API-CLIENT-UNIFY-1]]）
_SUBMISSIONS_URL   = "https://data.sec.gov/submissions/CIK{cik}.json"
_COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

_LISTING_AGE_THRESHOLD_YEARS = 3

# ②判定対象: 「年次/四半期報告書」に相当するフォーム種別のみに絞り込む。
# 8-K・SCHEDULE 13D・S-8・DEF 14A等の無関係な提出書類まで「直近提出書類」
# として拾うと、ほぼ全銘柄で常時発火する無意味なチェックになるため
# （実際にAPGEの直近提出はSCHEDULE 13D/Aだったが、これは同社が10-K/10-Qを
# 提出していないことを意味しない。2026-09-05確認）、この集合の中で
# 最新のものだけを見る。
_QUARTERLY_FORMS = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
_ANNUAL_OR_QUARTERLY_FORMS = _QUARTERLY_FORMS | {
    "20-F", "20-F/A", "40-F", "40-F/A", "6-K",
}

# ③: parser.py::SECParser.XBRL_MAPPING["revenue"]（cost_of_revenue等と
# 同じ候補タグリスト）をそのまま流用し、独自の重複リストを作らない
# （依頼文「既存のcost_of_revenue調査等で使用した候補タグリストがあれば
# 流用」に対応）。
REVENUE_TAG_CANDIDATES = tuple(SECParser.XBRL_MAPPING["revenue"])


@dataclass
class PreflightResult:
    ticker: str
    flags: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """flags・errorsいずれも0件ならTrue。Falseは「登録を止めるべき」
        という意味ではなく、単に判断材料（フラグ）があることを示す
        （想定機能④、自動停止しない設計）。"""
        return not self.flags and not self.errors


def _fetch_json(url: str) -> dict | None:
    try:
        resp = sec_get(url, "登録の事前確認")
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _check_listing_age_and_form_type(result: PreflightResult, submissions: dict) -> None:
    """①上場後3年未満、②直近の年次/四半期報告書が10-K/10-Q以外、を判定する。

    `filings.recent`（直近分、通常は最大1000件程度）のみを見る。上場後
    3年未満かどうかの判定にはこれで十分（3年以内に上場した企業が、
    直近1000件の提出に収まらないほど大量の申告を行っていることは
    現実的にありえない）。
    """
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    n = min(len(forms), len(filing_dates))

    # ①
    quarterly_dates = [filing_dates[i] for i in range(n) if forms[i] in _QUARTERLY_FORMS]
    if quarterly_dates:
        oldest_str = min(quarterly_dates)
        try:
            oldest_date = date.fromisoformat(oldest_str)
            age_years = (date.today() - oldest_date).days / 365.25
        except ValueError:
            age_years = None
        if age_years is not None and age_years < _LISTING_AGE_THRESHOLD_YEARS:
            result.flags.append(
                f"①上場後{age_years:.1f}年（初回10-K/10-Q提出日: {oldest_str}、"
                f"閾値{_LISTING_AGE_THRESHOLD_YEARS}年未満）→ データ不安定リスクあり"
            )
    else:
        result.flags.append(
            "①直近の提出履歴内に10-K/10-Q系の提出が1件も見つかりません"
            "（上場後間もない、または10-K/10-Q以外を主提出書式とする企業の"
            "可能性）→ データ不安定リスクあり"
        )

    # ②（配列は最新が先頭という前提。SEC submissions APIの標準仕様、
    # xbrl_segment_fetcher.py::get_recent_filings()等でも同じ前提を採用済み）
    latest_idx = next((i for i in range(n) if forms[i] in _ANNUAL_OR_QUARTERLY_FORMS), None)
    if latest_idx is not None:
        latest_form = forms[latest_idx]
        if latest_form not in _QUARTERLY_FORMS:
            result.flags.append(
                f"②直近の年次/四半期報告書が{latest_form}"
                f"（{filing_dates[latest_idx]}提出、10-K/10-Q以外）"
                "→ 四半期データ欠落の可能性"
            )


def _check_revenue_tags(result: PreflightResult, company_facts: dict) -> None:
    """③収益系XBRLタグが1つも存在しないかを判定する。"""
    us_gaap = company_facts.get("facts", {}).get("us-gaap", {})
    if not any(tag in us_gaap for tag in REVENUE_TAG_CANDIDATES):
        result.flags.append(
            "③収益系XBRLタグ（Revenues等）が1件も存在しません"
            "→ 売上ゼロ企業の可能性"
        )


def run_preflight_check(ticker: str) -> PreflightResult:
    """指定ティッカーの①②③を判定する（cik_lookup.csvにCIKが記録済み
    であること＝Step 0.5完了が前提）。"""
    ticker = ticker.upper()
    result = PreflightResult(ticker=ticker)

    cik = _tickers_mod.get_cik(ticker)
    if not cik:
        result.errors.append(
            "cik_lookup.csvにCIKが未登録のためプリフライトチェックを実行"
            "できません（Step 0.5の完了を確認してください）"
        )
        return result

    submissions = _fetch_json(_SUBMISSIONS_URL.format(cik=cik))
    if submissions is None:
        result.errors.append(
            f"SEC submissions API（CIK{cik}）への到達に失敗したため①②を判定できません"
        )
    else:
        _check_listing_age_and_form_type(result, submissions)

    company_facts = _fetch_json(_COMPANY_FACTS_URL.format(cik=cik))
    if company_facts is None:
        result.errors.append(
            f"SEC company facts API（CIK{cik}）への到達に失敗したため③を判定できません"
        )
    else:
        _check_revenue_tags(result, company_facts)

    return result


def print_preflight_report(result: PreflightResult) -> None:
    print(f"\n--- Step 0.5後プリフライトチェック: {result.ticker} ---")
    if not result.flags and not result.errors:
        print("  ✅ 該当するリスクフラグなし")
        return
    for flag in result.flags:
        print(f"  ⚠️  {flag}")
    for err in result.errors:
        print(f"  ❓ {err}（判定不能。手動確認を推奨）")
    print("  ※ 自動停止はしません。判断材料として確認の上、続行してください"
          "（[[PREFLIGHT-CHECK-1]]想定機能④）。")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="新規銘柄登録プリフライトチェック（Step 0.5直後・Step 1実行前）"
    )
    parser.add_argument("tickers", nargs="+", help="対象ティッカー（複数指定可）")
    args = parser.parse_args()

    any_flag = False
    for t in args.tickers:
        result = run_preflight_check(t)
        print_preflight_report(result)
        if not result.ok:
            any_flag = True

    # 判断材料の提示のみが目的のため、フラグが立ってもexit codeは常に0
    # （呼び出し元スクリプトの処理を止めない設計、想定機能④）
    return 0


if __name__ == "__main__":
    sys.exit(main())
