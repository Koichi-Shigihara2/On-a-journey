"""stock.html「シグナル整合性チェック」新セクション 実ブラウザ確認スクリプト
（[[STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1]]①実装、2026-09-23新設）

目的:
  stock.htmlに追加した新セクション（price_iv_ratio時系列・ERP現在値・
  方向性突合の一文要約）が、実ブラウザで表示崩れ・consoleエラーなく
  描画されること、およびデータが薄い/存在しない銘柄でも安全にフォール
  バックすることを確認する。

対象:
  - LITE / TSLA / AAPL（price_iv_ratioデータあり、初回テストケース）
  - データが薄い/存在しない銘柄1件（poc.json自体が存在しないダミー
    ティッカーでフォールバック確認）

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
# SN: latest.json・poc.jsonともに存在するが、price_iv_ratioの非null月が
# 1件のみ（history記録がまだ1ヶ月分しか蓄積されていない薄いデータの
# 実例）。トレンド計算（2点以上必要）のフォールバックを確認する。
SPARSE_TICKER = "SN"


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

            print("\n=== データが薄いティッカー（SN、price_iv_ratio非null1件のみ、フォールバック確認） ===")
            r = check_ticker(browser, SPARSE_TICKER, expect_data=False)
            # latest.jsonは存在するのでページ本体は正常描画される想定。
            # トレンド計算（2点以上必要）が「データ不足」を返し、
            # チャート自体（1点のみ）は描画されるが判定は不可表示になる。
            # JSエラーが出ていないことのみを主眼に確認する。
            ok = (len(r["console_errors"]) == 0 and r["section_display"] not in (None, "none")
                  and r["body_nonempty"] and "データ不足" in r["body_text_snippet"])
            overall_ok = overall_ok and ok
            print(f"[{'OK' if ok else 'NG'}] {SPARSE_TICKER}: display={r['section_display']} "
                  f"chart_present={r['chart_present']} console_errors={len(r['console_errors'])}")
            print("    body snippet:", r["body_text_snippet"].replace("\n", " ")[:200])
            if r["console_errors"]:
                for e in r["console_errors"]:
                    print("    console error:", e)

            browser.close()

        print(f"\n{'✅ 全チェック通過' if overall_ok else '❌ 問題あり'}")
        return 0 if overall_ok else 1
    finally:
        proc.terminate()


if __name__ == "__main__":
    sys.exit(main())
