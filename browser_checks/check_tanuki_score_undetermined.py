"""TANUKI SCORE画面の「判定不能（UNDETERMINED）」表示の実ブラウザ確認（2026-09-26新設）

株価（有効な終値）が取れない銘柄は、latest.jsonでtanuki_score=UNDETERMINED・
timing_score=Noneになる（[[MARKETDATA-DAILY-CLOSE-NONE-PERMANENT-1]]）。本番データには
現在その状態の銘柄が無いため、ADBEのlatest.jsonだけをブラウザ側で差し替えて表示を確認する
（ファイルは変更しない）。確認項目:
  - 詳細表のタイミング欄が「—」（0で埋めない）
  - 分類カードに「判定不能」が出て、ADBEが含まれる

使い方（check_dependency_map.pyと同じ前提）:
  venv\\Scripts\\python browser_checks\\check_tanuki_score_undetermined.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(REPO_ROOT, "docs")
PORT = 8792
URL = f"http://localhost:{PORT}/value-monitor/tanuki_score/index.html"
TICKER = "ADBE"


def main() -> int:
    proc = subprocess.Popen([sys.executable, "-m", "http.server", str(PORT), "--directory", DOCS_DIR],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(URL, timeout=1)
                break
            except Exception:
                time.sleep(0.2)
        with open(os.path.join(DOCS_DIR, "value-monitor", "tanuki_valuation", "data", TICKER, "latest.json"),
                  encoding="utf-8") as f:
            latest = json.load(f)
        latest["tanuki_score"] = "UNDETERMINED"
        latest["timing_score"] = None
        latest["upside_percent"] = None
        latest.setdefault("components", {})["current_price"] = None

        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            errors: list[str] = []
            # ロゴ等の本番用絶対パス（/On-a-journey/…）はdocs/をルートに配信するローカルでは404になるため除外
            page.on("response", lambda r: errors.append(f"{r.status} {r.url}")
                    if r.status >= 400 and "/On-a-journey/" not in r.url else None)
            page.on("console", lambda m: errors.append(m.text)
                    if m.type == "error" and "Failed to load resource" not in m.text else None)
            page.route(f"**/tanuki_valuation/data/{TICKER}/latest.json*",
                       lambda route: route.fulfill(status=200, content_type="application/json",
                                                   body=json.dumps(latest)))
            page.goto(URL, wait_until="networkidle")
            page.wait_for_timeout(2500)
            timing_cell = page.evaluate("""(t) => {
              const rows = [...document.querySelectorAll('tr')].filter(r => r.innerText.trim().startsWith(t));
              for (const r of rows) {
                const tds = [...r.querySelectorAll('td')];
                const idx = [...(r.closest('table')?.querySelectorAll('th') || [])].findIndex(th => th.innerText.includes('タイミング'));
                if (idx >= 0 && tds[idx]) return tds[idx].innerText.trim();
              }
              return null;
            }""", TICKER)
            body = page.inner_text("body")
            browser.close()
        checks = {
            "タイミング欄が「—」": timing_cell == "—",
            "分類カード「判定不能」": "判定不能" in body,
            "consoleエラー0件": not errors,
        }
        for k, v in checks.items():
            print(f"{'✅' if v else '❌'} {k}" + ("" if v else f"（実測: timing={timing_cell!r}, errors={errors[:2]}）"))
        return 0 if all(checks.values()) else 1
    finally:
        proc.terminate()


if __name__ == "__main__":
    sys.exit(main())
