#!/usr/bin/env python3
"""
TANUKI TAIL — edgar_rss_monitor.py

Core 銘柄（type=core）の 10-Q/10-K 提出を EDGAR Atom RSS で監視し、
新規提出を review_queue.json に追加する。

使用方法:
    python src/tail/edgar_rss_monitor.py
    python src/tail/edgar_rss_monitor.py --ticker PLTR SOFI

必要パッケージ: requests
環境変数:
    DISCORD_WEBHOOK  Discord Incoming Webhook URL（省略可）
"""

import os
import sys
import json
import time
import re
import argparse
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, List
from zoneinfo import ZoneInfo

try:
    import requests
except ImportError:
    print("requests が必要です: pip install requests")
    sys.exit(1)

# ── パス設定 ──────────────────────────────────────────────────
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root  = os.path.abspath(os.path.join(script_dir, "..", ".."))

DATA_DIR          = os.path.join(repo_root, "docs", "portfolio", "tail", "data")
POSITIONS_INDEX   = os.path.join(DATA_DIR, "positions_index.json")
POSITIONS_DIR     = os.path.join(DATA_DIR, "positions")
RSS_STATE_PATH    = os.path.join(DATA_DIR, "rss_state.json")
REVIEW_QUEUE_PATH = os.path.join(DATA_DIR, "review_queue.json")

JST = ZoneInfo("Asia/Tokyo")

# ── SEC API 設定 ──────────────────────────────────────────────
SEC_HEADERS = {
    "User-Agent": "Koichi Personal Investment Tools koichi@example.com",
    "Accept":     "application/xml,application/atom+xml,*/*",
}
THROTTLE = 0.3

# 前回提出から EXPECTED_DAYS 日後を「期待提出日」とし、
# そこから ALERT_BIZ_DAYS 回スクリプトが実行されても変化なければアラート
EXPECTED_DAYS  = 75
ALERT_BIZ_DAYS = 3

# ── Atom RSS パース用正規表現 ─────────────────────────────────
# EDGAR Atom フィードの実際のタグ構造:
#   <filing-type>10-Q</filing-type>  (※ form-type ではない)
#   <accession-number>0001321655-26-000028</accession-number>
#   <filing-date>2026-05-05</filing-date>
#   period-of-report は含まれない → submissions API で補完
_ENTRY_RE = re.compile(r'<entry\b[^>]*>(.*?)</entry>', re.DOTALL | re.IGNORECASE)
_ACCN_RE  = re.compile(r'<accession-number>\s*([0-9\-]+)\s*</accession-number>', re.IGNORECASE)
_DATE_RE  = re.compile(r'<filing-date>\s*(\d{4}-\d{2}-\d{2})\s*</filing-date>', re.IGNORECASE)
_FORM_RE  = re.compile(r'<filing-type>\s*(10-[QK])\s*</filing-type>', re.IGNORECASE)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RSS 取得・パース
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fetch_rss_form(cik: str, form_type: str) -> Optional[Dict[str, str]]:
    """EDGAR Atom RSS から指定フォームタイプの最新エントリを1件返す。"""
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={cik}&type={form_type}"
        "&dateb=&owner=include&count=5&search_text=&output=atom"
    )
    try:
        time.sleep(THROTTLE)
        r = requests.get(url, headers=SEC_HEADERS, timeout=20)
        r.raise_for_status()
        xml = r.text
    except Exception as e:
        print(f"  RSS 取得エラー ({form_type}): {e}")
        return None

    for m in _ENTRY_RE.finditer(xml):
        entry = m.group(1)
        fm = _FORM_RE.search(entry)
        if not fm:
            continue
        am = _ACCN_RE.search(entry)
        dm = _DATE_RE.search(entry)
        if am and dm:
            return {
                "accn":  am.group(1).strip(),
                "filed": dm.group(1),
                "form":  fm.group(1).upper(),
            }
    return None


def get_filing_period(cik: str, accn: str) -> Optional[str]:
    """submissions API で特定 accn の reportDate (period end) を取得。

    現状維持（意図的設計、SEC-SUBMISSIONS-DUAL-FETCH-1、2026-09-19判断確定）:
    common/sec_data/fetcher.py::fetch_submissions() が同じ submissions API を
    週次キャッシュ（submissions.json）として取得済みだが、本関数はあえて
    参照せず特定accnのみをlive fetchする。新規提出直後は週次キャッシュに
    未反映のため、TANUKI TAILの遅延検知（新規filing検知後すぐにperiodを
    確定する必要がある）を成立させるための鮮度目的。単純な参照統合は
    この鮮度要件を壊すリスクがあるため見送り、重複APIコールは許容する。
    """
    try:
        time.sleep(THROTTLE)
        r = requests.get(
            f"https://data.sec.gov/submissions/CIK{cik}.json",
            headers=SEC_HEADERS, timeout=20,
        )
        r.raise_for_status()
        recent = r.json().get("filings", {}).get("recent", {})
        accns  = recent.get("accessionNumber", [])
        dates  = recent.get("reportDate", [])
        for i, a in enumerate(accns):
            if a == accn:
                return dates[i] if i < len(dates) else None
    except Exception as e:
        print(f"  period 取得エラー: {e}")
    return None


def fetch_rss_latest(cik: str) -> Optional[Dict[str, str]]:
    """10-Q と 10-K のうち filed が新しい方を返す。"""
    latest: Optional[Dict[str, str]] = None
    for form_type in ("10-Q", "10-K"):
        entry = _fetch_rss_form(cik, form_type)
        if entry is None:
            continue
        if latest is None or entry["filed"] > latest["filed"]:
            latest = entry
    return latest


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CIK / ティッカー
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def get_cik_map(tickers: List[str]) -> Dict[str, Optional[str]]:
    """company_tickers.json を1回だけ取得して複数ティッカーを一括変換。"""
    upper = {t.upper() for t in tickers}
    result: Dict[str, Optional[str]] = {t: None for t in upper}
    try:
        time.sleep(THROTTLE)
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=SEC_HEADERS, timeout=15,
        )
        r.raise_for_status()
        for entry in r.json().values():
            t = entry.get("ticker", "").upper()
            if t in upper:
                result[t] = str(entry["cik_str"]).zfill(10)
    except Exception as e:
        print(f"  CIK 一括取得エラー: {e}")
    return result


def get_core_tickers() -> List[str]:
    """positions_index.json + thesis ファイルから type=core のティッカー一覧を返す。"""
    try:
        with open(POSITIONS_INDEX, encoding="utf-8") as f:
            idx = json.load(f)
    except Exception as e:
        print(f"positions_index.json 読み込みエラー: {e}")
        return []

    tickers: List[str] = []
    for fname in idx.get("positions", []):
        path = os.path.join(POSITIONS_DIR, fname)
        try:
            with open(path, encoding="utf-8") as f:
                thesis = json.load(f)
            if thesis.get("type") == "core":
                tickers.append(fname.replace("_thesis.json", "").upper())
        except Exception:
            continue
    return tickers


def get_monitored_tickers() -> List[str]:
    """positions_index.json + thesis ファイルから、保有している全ポジション
    のティッカー一覧を返す。

    **2026-08-19方針決定（`[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`）**:
    RSS監視・四半期レビュー自動生成の対象は全保有ポジションとする。
    core/satelliteの区別はthesis内のポジション重み付けとして残るが、
    監視対象の決定には使わない（保有している以上、決算は見る）。

    `get_core_tickers()`（type=coreのみ）とは異なり、typeを一切見ずに
    positions_index.jsonの全件をそのまま返す。判定は既存のデータソース
    （positions_index.json + thesis.json、ファイルの存在・パース可否の
    確認のみ）を使い、独自の一覧は作らない。
    """
    try:
        with open(POSITIONS_INDEX, encoding="utf-8") as f:
            idx = json.load(f)
    except Exception as e:
        print(f"positions_index.json 読み込みエラー: {e}")
        return []

    tickers: List[str] = []
    for fname in idx.get("positions", []):
        path = os.path.join(POSITIONS_DIR, fname)
        try:
            with open(path, encoding="utf-8") as f:
                json.load(f)  # thesisファイルの存在・パース可否のみ確認（typeは見ない）
            tickers.append(fname.replace("_thesis.json", "").upper())
        except Exception:
            continue
    return tickers


def get_excluded_positions() -> List[tuple]:
    """positions_index.json の全ポジションのうち、get_monitored_tickers()が
    返さなかったもの（＝何らかの理由で監視対象から除外されたポジション）
    を (ticker, type) のリストで返す。

    2026-08-19の方針変更（監視対象を全保有ポジションに拡大）により、
    現時点ではget_monitored_tickers()がpositions_index.jsonの全件を
    そのまま返すため、本関数は通常空リストを返す（thesisファイルの
    パース失敗等、get_monitored_tickers()側で個別にスキップされた
    ポジションがあれば、そのpositions_index上の記載だけがここに
    現れる）。

    判定基準を固定の`type!="core"`比較ではなく**get_monitored_tickers()
    の実際の戻り値との差分**にしたことで、将来get_monitored_tickers()側
    に何らかの除外条件が追加された場合も、本関数を個別に修正しなくても
    自動的にその除外を検出できる（沈黙除外の再発防止をget_monitored_
    tickers()の変更に追随する形で維持する設計）。
    """
    try:
        with open(POSITIONS_INDEX, encoding="utf-8") as f:
            idx = json.load(f)
    except Exception as e:
        print(f"positions_index.json 読み込みエラー: {e}")
        return []

    monitored = set(get_monitored_tickers())
    excluded: List[tuple] = []
    for fname in idx.get("positions", []):
        ticker = fname.replace("_thesis.json", "").upper()
        if ticker in monitored:
            continue
        path = os.path.join(POSITIONS_DIR, fname)
        ptype = None
        try:
            with open(path, encoding="utf-8") as f:
                ptype = json.load(f).get("type")
        except Exception:
            pass
        excluded.append((ticker, ptype))
    return excluded


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 状態・キュー I/O
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_state() -> Dict[str, Any]:
    if os.path.exists(RSS_STATE_PATH):
        with open(RSS_STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(RSS_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_queue() -> Dict[str, Any]:
    if os.path.exists(REVIEW_QUEUE_PATH):
        with open(REVIEW_QUEUE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"queue": []}


def save_queue(queue: Dict[str, Any]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(REVIEW_QUEUE_PATH, "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=2)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ユーティリティ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def quarter_label(period_end: str) -> str:
    """'2026-03-31' → '2026Q1'"""
    try:
        d = date.fromisoformat(period_end[:10])
        return f"{d.year}Q{(d.month - 1) // 3 + 1}"
    except Exception:
        return period_end


def send_discord(message: str) -> bool:
    webhook = os.environ.get("DISCORD_WEBHOOK", "")
    if not webhook:
        print(f"  Discord スキップ (DISCORD_WEBHOOK 未設定): {message[:80]}")
        return False
    try:
        r = requests.post(webhook, json={"content": message}, timeout=10)
        ok = r.status_code in (200, 204)
        print(f"  Discord {'送信OK' if ok else f'失敗 HTTP {r.status_code}'}")
        return ok
    except Exception as e:
        print(f"  Discord 送信エラー: {e}")
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1銘柄処理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def process_ticker(
    ticker: str,
    cik: str,
    state: Dict[str, Any],
    queue: Dict[str, Any],
    now_jst: datetime,
) -> List[Dict[str, Any]]:
    """
    RSS で最新提出を確認し、必要に応じてキューへ追加・遅延を記録。
    Returns: Discord 通知用 alert dict のリスト。
    """
    alerts: List[Dict[str, Any]] = []

    latest = fetch_rss_latest(cik)
    if latest is None:
        print(f"  [{ticker}] RSS 取得失敗 → スキップ")
        return alerts

    prev      = state.get(ticker, {})
    prev_accn = prev.get("last_accn")

    if prev_accn is None:
        # 初回実行: 現状を記録するだけでキューには追加しない
        period_init = get_filing_period(cik, latest["accn"]) or ""
        print(f"  [{ticker}] 初回記録  accn={latest['accn']}  filed={latest['filed']}  period={period_init}")
        state[ticker] = {
            "last_accn":      latest["accn"],
            "last_filed":     latest["filed"],
            "last_period":    period_init,
            "last_checked":   now_jst.isoformat(),
            "no_filing_days": 0,
        }

    elif latest["accn"] != prev_accn:
        # 新規提出を検知: RSS に period-of-report がないので submissions API で補完
        period = get_filing_period(cik, latest["accn"]) or ""
        quarter = quarter_label(period) if period else "不明"
        print(f"  [{ticker}] ★ 新規提出: {latest['accn']} ({quarter}) filed={latest['filed']}")

        existing_accns = {item["accn"] for item in queue.get("queue", [])}
        if latest["accn"] not in existing_accns:
            queue.setdefault("queue", []).append({
                "ticker":    ticker,
                "quarter":   quarter,
                "accn":      latest["accn"],
                "filed":     latest["filed"],
                "status":    "pending",
                "queued_at": now_jst.isoformat(),
            })

        state[ticker] = {
            "last_accn":      latest["accn"],
            "last_filed":     latest["filed"],
            "last_period":    period,
            "last_checked":   now_jst.isoformat(),
            "no_filing_days": 0,
        }
        alerts.append({
            "type":    "new_filing",
            "ticker":  ticker,
            "quarter": quarter,
            "accn":    latest["accn"],
            "filed":   latest["filed"],
        })

    else:
        # 変更なし — 遅延チェック
        last_filed = date.fromisoformat(prev["last_filed"])
        expected   = last_filed + timedelta(days=EXPECTED_DAYS)
        today      = date.today()

        if today >= expected:
            no_filing_days = prev.get("no_filing_days", 0) + 1
            state[ticker]["no_filing_days"] = no_filing_days
            state[ticker]["last_checked"]   = now_jst.isoformat()

            if no_filing_days == ALERT_BIZ_DAYS:
                print(f"  [{ticker}] ⚠ 提出遅延アラート {no_filing_days}日 (expected={expected})")
                alerts.append({
                    "type":           "overdue",
                    "ticker":         ticker,
                    "no_filing_days": no_filing_days,
                    "expected":       expected.isoformat(),
                })
            else:
                print(f"  [{ticker}] 変更なし、遅延中 {no_filing_days}日 (expected={expected})")
        else:
            state[ticker]["no_filing_days"] = 0
            state[ticker]["last_checked"]   = now_jst.isoformat()
            days_left = (expected - today).days
            print(f"  [{ticker}] 変更なし (次回期待日={expected}, あと{days_left}日)")

    return alerts


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# エントリーポイント
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    parser = argparse.ArgumentParser(
        description="TANUKI TAIL — EDGAR RSS Monitor"
    )
    parser.add_argument(
        "--ticker", nargs="+",
        help="対象ティッカー（省略時は positions_index.json の全保有ポジションを自動取得）",
    )
    args = parser.parse_args()

    now_jst = datetime.now(JST)
    print(f"TANUKI TAIL EDGAR RSS Monitor — {now_jst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 60)

    if args.ticker:
        tickers = [t.upper() for t in args.ticker]
    else:
        tickers = get_monitored_tickers()
        excluded = get_excluded_positions()
        for ex_ticker, ex_type in excluded:
            print(f"[SKIP] {ex_ticker}: type={ex_type} のため RSS監視対象外")
        print(f"監視対象: {len(tickers)}銘柄（全保有ポジション）")
    if not tickers:
        print("対象ティッカーなし → 終了")
        sys.exit(0)
    print(f"対象: {tickers}")

    # CIK 一括取得
    print("\n── CIK 取得 ──")
    cik_map = get_cik_map(tickers)
    for t, cik in cik_map.items():
        print(f"  {t}: {cik or '失敗'}")

    # 状態・キュー読み込み
    state = load_state()
    queue = load_queue()
    all_alerts: List[Dict[str, Any]] = []

    # RSS チェック
    print("\n── RSS チェック ──")
    for ticker in tickers:
        cik = cik_map.get(ticker)
        if not cik:
            print(f"  [{ticker}] CIK 不明 → スキップ")
            continue
        alerts = process_ticker(ticker, cik, state, queue, now_jst)
        all_alerts.extend(alerts)

    # 保存
    save_state(state)
    save_queue(queue)
    print(f"\n✓ rss_state.json, review_queue.json 保存")

    # Discord 通知
    for alert in all_alerts:
        if alert["type"] == "new_filing":
            msg = (
                f"📊 **{alert['ticker']}** の **{alert['quarter']}** 決算 ({alert['accn']}) が提出されました。"
                f"\nレビュー生成を開始します。  提出日: {alert['filed']}"
            )
        else:
            msg = (
                f"⚠️ **{alert['ticker']}** の 10-Q 提出が遅延しています"
                f" ({alert['no_filing_days']}営業日超過、期待日: {alert['expected']})"
            )
        send_discord(msg)

    # サマリ
    new_pending = [a for a in all_alerts if a["type"] == "new_filing"]
    print(f"\n{'━' * 60}")
    print(f"完了: 新規pending {len(new_pending)} 件")
    for a in new_pending:
        print(f"  NEW_PENDING: {a['ticker']} {a['quarter']} {a['accn']}")


if __name__ == "__main__":
    main()
