"""stock.html「シグナル整合性チェック」新セクション 実ブラウザ確認スクリプト
（[[STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1]]①実装、2026-09-23新設）

目的:
  stock.htmlに追加した新セクション（price_iv_ratio時系列・ERP現在値・
  方向性突合の一文要約）が、実ブラウザで表示崩れ・consoleエラーなく
  描画されること、およびデータが薄い/存在しない銘柄でも安全にフォール
  バックすることを確認する。

対象:
  - LITE / TSLA / AAPL（price_iv_ratioデータあり、初回テストケース）
  - データが薄い銘柄1件（latest.json・poc.jsonがそろい、price_iv_ratioの非null月が
    1件以下の銘柄のうち、いちばん少ないものをその場で選ぶ。無ければ判定不能）

前提:
  cd C:\\Users\\shigi\\Documents\\On-a-journey-git
  venv\\Scripts\\activate
  python -m playwright install chromium   # 未インストールなら実行

使い方:
  python browser_checks\\check_signal_consistency_section.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(REPO_ROOT, "docs")
PORT = 8793
BASE_URL = f"http://127.0.0.1:{PORT}"

STOCK_URL_TMPL = f"{BASE_URL}/value-monitor/tanuki_valuation/stock.html?ticker={{ticker}}"

TICKERS = ["LITE", "TSLA", "AAPL"]
# データが薄い銘柄: latest.json・poc.jsonともに存在し、price_iv_ratioの非null月が1件以下の銘柄で、
# トレンド計算（2点以上必要）の「データ不足」へのフォールバックを確認する。
# 2026-10-08: 名指し（旧SN）をやめ、非nullの件数がいちばん少ない銘柄をその場で選ぶ（SNは登録解除、
# また非nullが2件に増えて条件に合わなくなっていた）。条件に合う銘柄が無ければ判定不能とする。
SPARSE_MAX_POINTS = 1
TANUKI_DATA_DIR = os.path.join(DOCS_DIR, "value-monitor", "tanuki_valuation", "data")
HYPECORE_DATA_DIR = os.path.join(DOCS_DIR, "value-monitor", "hypecore", "data")


def _piv_points(ticker: str) -> int:
    """stock.htmlのloadAndRenderSignalConsistency()と同じ数え方（poc.monthlyのうち
    price_iv_ratioとmonthがそろう点の数）。"""
    import json
    with open(os.path.join(HYPECORE_DATA_DIR, f"{ticker}_poc.json"), encoding="utf-8") as f:
        monthly = json.load(f).get("monthly") or []
    return sum(1 for m in monthly if m.get("price_iv_ratio") is not None and m.get("month") is not None)


def pick_sparse_ticker() -> tuple[str | None, int | None]:
    """latest.json・poc.jsonがそろう銘柄のうち、price_iv_ratioの非null月がいちばん少ない銘柄
    （同数ならティッカー順で先）を返す。その件数がSPARSE_MAX_POINTSを超えるなら銘柄はNone。"""
    # 銘柄の一覧はtickers.py経由（データのフォルダを直接走査しない、tests/test_no_direct_ticker_access.py）
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    from common.sec_data import tickers as _tickers
    counts = []
    for t in sorted(_tickers.get_tanuki_tickers()):
        if (os.path.exists(os.path.join(TANUKI_DATA_DIR, t, "latest.json"))
                and os.path.exists(os.path.join(HYPECORE_DATA_DIR, f"{t}_poc.json"))):
            counts.append((_piv_points(t), t))
    if not counts:
        return None, None
    n, t = min(counts)
    return (t if n <= SPARSE_MAX_POINTS else None), n


def start_server() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(PORT), "--directory", DOCS_DIR],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        try:
            urllib.request.urlopen(f"{BASE_URL}/value-monitor/tanuki_valuation/index.html", timeout=1)
            return proc
        except Exception:
            time.sleep(0.2)
    proc.terminate()
    raise RuntimeError("ローカルHTTPサーバーの起動に失敗しました")


# stock.html:349が絶対パス`/On-a-journey/common/sec_data/normalized/...`を
# fetchする（本番のGitHub Pagesベースパス前提）。本スクリプトはdocs/を
# サーバールートとして配信するため、このURLだけは本番でも意味を持つ
# 既知の環境差分であり、本チェックの対象（新設セクションのJSエラー）
# とは無関係。check_dependency_map.pyのREADMEにも同種の注意書きあり。
# Chromeのconsole「error」テキストにはURLが含まれない（"Failed to load
# resource: the server responded with a status of 404 (File not found)"の
# みで発生源不明）ため、response側でURLを突き合わせて除外する。
_KNOWN_PRE_EXISTING_404 = "/On-a-journey/common/sec_data/normalized/"


def check_ticker(browser, ticker: str, expect_data: bool) -> dict:
    page = browser.new_page()
    console_errors: list[str] = []
    known_404_count = 0
    unexpected_failed_urls: list[tuple[str, int]] = []

    page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
    page.on("pageerror", lambda exc: console_errors.append(str(exc)))

    def on_response(resp):
        nonlocal known_404_count
        if resp.status >= 400:
            if _KNOWN_PRE_EXISTING_404 in resp.url:
                known_404_count += 1
            else:
                unexpected_failed_urls.append((resp.url, resp.status))

    page.on("response", on_response)
    page.goto(STOCK_URL_TMPL.format(ticker=ticker), wait_until="networkidle")
    try:
        page.wait_for_selector("#signal-consistency-section", timeout=15000, state="attached")
    except Exception:
        pass
    page.wait_for_timeout(1500)

    section = page.query_selector("#signal-consistency-section")
    display = section.evaluate("el => getComputedStyle(el).display") if section else None
    body_text = page.text_content("#signal-consistency-body") or ""
    chart_present = page.query_selector("#signal-consistency-chart") is not None

    # console_errorsのうち「Failed to load resource」系は既知404の件数分だけ
    # 差し引く（Chromeのconsoleテキストに発生元URLが含まれないため、件数で
    # 相殺する近似。unexpected_failed_urlsがあれば別途errorsに追加する）
    resource_fail_texts = [e for e in console_errors if "Failed to load resource" in e]
    other_console_errors = [e for e in console_errors if "Failed to load resource" not in e]
    unexplained_resource_fails = max(0, len(resource_fail_texts) - known_404_count)
    errors = other_console_errors + [f"unexplained resource failure x{unexplained_resource_fails}"] * (1 if unexplained_resource_fails else 0) \
        + [f"{url} ({status})" for url, status in unexpected_failed_urls]

    result = {
        "ticker": ticker,
        "console_errors": errors,
        "section_display": display,
        "body_nonempty": len(body_text.strip()) > 0,
        "chart_present": chart_present,
        "body_text_snippet": body_text.strip()[:200],
    }
    page.close()
    return result


def main() -> int:
    proc = start_server()
    try:
        from playwright.sync_api import sync_playwright

        overall_ok = True
        with sync_playwright() as p:
            browser = p.chromium.launch()

            print("=== データありティッカー（LITE/TSLA/AAPL） ===")
            for t in TICKERS:
                r = check_ticker(browser, t, expect_data=True)
                ok = (len(r["console_errors"]) == 0 and r["section_display"] not in (None, "none")
                      and r["body_nonempty"])
                overall_ok = overall_ok and ok
                print(f"[{'OK' if ok else 'NG'}] {t}: display={r['section_display']} "
                      f"chart_present={r['chart_present']} console_errors={len(r['console_errors'])}")
                if r["console_errors"]:
                    for e in r["console_errors"]:
                        print("    console error:", e)
                print("    body snippet:", r["body_text_snippet"].replace("\n", " ")[:150])

            sparse, n_min = pick_sparse_ticker()
            print(f"\n=== データが薄いティッカー（price_iv_ratio非null{SPARSE_MAX_POINTS}件以下でいちばん少ない銘柄、フォールバック確認） ===")
            if sparse is None:
                # 条件に合う銘柄が無い: 確認できないので判定不能（NGにはしない）
                print(f"[判定不能] 条件に合う銘柄なし（latest.json・poc.jsonがそろう銘柄の非null月の最少は{n_min}件）")
            else:
                r = check_ticker(browser, sparse, expect_data=False)
                # latest.jsonは存在するのでページ本体は正常描画される想定。
                # トレンド計算（2点以上必要）が「データ不足」を返し、
                # チャート自体（1点のみ）は描画されるが判定は不可表示になる。
                # JSエラーが出ていないことのみを主眼に確認する。
                ok = (len(r["console_errors"]) == 0 and r["section_display"] not in (None, "none")
                      and r["body_nonempty"] and "データ不足" in r["body_text_snippet"])
                overall_ok = overall_ok and ok
                print(f"[{'OK' if ok else 'NG'}] {sparse}（非null{n_min}件）: display={r['section_display']} "
                      f"chart_present={r['chart_present']} console_errors={len(r['console_errors'])}")
                print("    body snippet:", r["body_text_snippet"].replace("\n", " ")[:200])
                if r["console_errors"]:
                    for e in r["console_errors"]:
                        print("    console error:", e)

            browser.close()

        note = "（データが薄い銘柄の確認は判定不能）" if sparse is None else ""
        print(f"\n{'✅ 全チェック通過' + note if overall_ok else '❌ 問題あり'}")
        return 0 if overall_ok else 1
    finally:
        proc.terminate()


if __name__ == "__main__":
    sys.exit(main())
