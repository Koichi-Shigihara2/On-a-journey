"""stock.html メイン理論株価の説明系（ウォーターフォール図・感応度）の基準確認
（2026-10-06新設。TANUKI-BETA-BASIS-FIELDS-UNLABELED-1のウォーターフォール部分と、
逓減型の感応度・WACC調整スライダーの基準混在の修正の確認用）

確認すること（各銘柄）:
  1. ウォーターフォール図（#valuation-chart）
     - V₀の棒の高さ（Plotlyが前の棒を積んだ値）が、V₀のラベルと一致する
     - V₀のラベルがlatest.jsonのdcf_components.v0_rm（メイン、Rm基準）である
     - 本質価値P_tの棒の高さ・ラベルが v0_rm + RPO PV + 成長OPT PV である
     - 注記がRm基準（β版へのフォールバックではない）
  2. SENSITIVITY ANALYSIS
     - 表の中央セル（.base-cell）がメイン理論株価（intrinsic_value_per_share）と一致する
     - 見出し（#sensBaseIvps）がメイン理論株価と一致する
     - 「WACC調整」スライダー（#waccSlider）が無い
  3. ページエラー（pageerror・console error）が0件
     （stock.htmlが絶対パスで取りにいく/On-a-journey/common/sec_data/normalized/の404は
     ローカル配信だけの既知の差分として除外する。check_signal_consistency_section.pyと同じ扱い）

期待値はlatest.jsonから独立に作る（画面のJSは使わない）。

使い方:
  python browser_checks\\check_valuation_chart_basis.py                 # 全銘柄
  python browser_checks\\check_valuation_chart_basis.py RXRX NVDA ALAB  # 指定銘柄
  python browser_checks\\check_valuation_chart_basis.py --docs-dir <別worktreeのdocs>
     （再生成したデータを別のworktreeで確かめるとき）

終了コード0=全項目一致、1=不一致あり（または実行中に例外）。
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 8795
BASE_URL = f"http://127.0.0.1:{PORT}"
STOCK_URL_TMPL = f"{BASE_URL}/value-monitor/tanuki_valuation/stock.html?ticker={{ticker}}"
_KNOWN_PRE_EXISTING_404 = "/On-a-journey/common/sec_data/normalized/"


def fmt_b(n: float) -> str:
    """stock.htmlのfmtB()と同じ書式。"""
    if n is None:
        return "—"
    if n < 0:
        return "-" + fmt_b(-n)
    for unit, div in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if n >= div:
            return f"${n / div:.2f}{unit}"
    return f"${round(n):,}"


def start_server(docs_dir: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(PORT), "--directory", docs_dir],
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


JS_STATE = r"""() => {
  const gd = document.getElementById('valuation-chart');
  const out = {chart: null};
  if (gd && gd.calcdata) {
    out.chart = {
      x: gd.data[0].x,
      text: gd.data[0].text,
      top: gd.calcdata[0].map(c => c.s1),   // 棒の上端（軸の単位）
    };
  }
  out.note = document.getElementById('valuation-chart-note')?.textContent || '';
  out.baseCell = document.querySelector('#matrixBody .base-cell')?.textContent?.trim() || null;
  out.sensBaseIvps = document.getElementById('sensBaseIvps')?.textContent?.trim() || null;
  out.slider = !!document.getElementById('waccSlider');
  return out;
}"""


def expected(d: dict) -> dict:
    dc = d.get("dcf_components") or {}
    comps = d.get("components") or {}
    v0 = dc.get("v0_rm") or d.get("v0") or 0
    rpo = comps.get("rpo_pv") or 0
    go = (d.get("growth_options") or {}).get("total_pv") or 0
    return {
        "v0": v0,
        "pt": v0 + rpo + go,
        "ivps": f"${d['intrinsic_value_per_share']:.2f}",
    }


def check_ticker(browser, ticker: str, docs_dir: str) -> tuple[bool, list[str]]:
    path = os.path.join(docs_dir, "value-monitor", "tanuki_valuation", "data", ticker, "latest.json")
    d = json.load(open(path, encoding="utf-8"))
    exp = expected(d)

    page = browser.new_page(viewport={"width": 1300, "height": 1000})
    console_errors: list[str] = []
    known_404 = 0
    unexpected_failed: list[str] = []
    page.on("console", lambda m: console_errors.append(m.text) if m.type == "error" else None)
    page.on("pageerror", lambda e: console_errors.append(str(e)))

    def on_response(resp):
        nonlocal known_404
        if resp.status >= 400:
            if _KNOWN_PRE_EXISTING_404 in resp.url:
                known_404 += 1
            else:
                unexpected_failed.append(f"{resp.url} ({resp.status})")

    page.on("response", on_response)
    page.goto(STOCK_URL_TMPL.format(ticker=ticker), wait_until="networkidle")
    try:
        page.wait_for_function("() => document.getElementById('valuation-chart')?.calcdata", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(300)
    s = page.evaluate(JS_STATE)
    page.close()

    ng: list[str] = []
    ch = s["chart"]
    if not ch:
        ng.append("ウォーターフォール図が描画されていない")
    else:
        i_v0 = ch["x"].index("V₀")
        top_v0 = ch["top"][i_v0] * 1e9
        if abs(top_v0 - exp["v0"]) > max(1e6, abs(exp["v0"]) * 1e-9):
            ng.append(f"V₀の棒の高さ {fmt_b(top_v0)} ≠ v0_rm {fmt_b(exp['v0'])}")
        if ch["text"][i_v0] != fmt_b(exp["v0"]):
            ng.append(f"V₀のラベル {ch['text'][i_v0]} ≠ {fmt_b(exp['v0'])}")
        top_pt = ch["top"][-1] * 1e9
        if abs(top_pt - exp["pt"]) > max(1e6, abs(exp["pt"]) * 1e-9):
            ng.append(f"P_tの棒の高さ {fmt_b(top_pt)} ≠ {fmt_b(exp['pt'])}")
        if ch["text"][-1] != fmt_b(exp["pt"]):
            ng.append(f"P_tのラベル {ch['text'][-1]} ≠ {fmt_b(exp['pt'])}")
        if "Rm基準" not in s["note"] or "β込み" in s["note"]:
            ng.append(f"注記がRm基準ではない: {s['note']!r}")
    if s["baseCell"] != exp["ivps"]:
        ng.append(f"感応度の中央セル {s['baseCell']} ≠ メインIV {exp['ivps']}")
    if s["sensBaseIvps"] != exp["ivps"]:
        ng.append(f"感応度の見出し {s['sensBaseIvps']} ≠ メインIV {exp['ivps']}")
    if s["slider"]:
        ng.append("WACC調整スライダーが残っている")

    res_fail = [e for e in console_errors if "Failed to load resource" in e]
    other = [e for e in console_errors if "Failed to load resource" not in e]
    unexplained = max(0, len(res_fail) - known_404)
    errs = other + unexpected_failed + ([f"unexplained resource failure x{unexplained}"] if unexplained else [])
    if errs:
        ng.append(f"ページエラー {len(errs)}件: {errs[:3]}")
    return (not ng), ng


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*")
    ap.add_argument("--docs-dir", default=os.path.join(REPO_ROOT, "docs"))
    args = ap.parse_args()
    docs_dir = os.path.abspath(args.docs_dir)
    data_dir = os.path.join(docs_dir, "value-monitor", "tanuki_valuation", "data")
    if args.tickers:
        tickers = args.tickers
    else:
        # 対象はTANUKI VALUATIONの有効銘柄（common/sec_data/tickers.py経由。data/の直接走査はしない）のうちlatest.jsonがあるもの
        sys.path.insert(0, REPO_ROOT)
        from common.sec_data.tickers import get_tanuki_tickers

        tickers = sorted(
            t for t in get_tanuki_tickers() if os.path.exists(os.path.join(data_dir, t, "latest.json"))
        )

    proc = start_server(docs_dir)
    n_ok = 0
    n_ng = 0
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch()
            for t in tickers:
                try:
                    ok, ng = check_ticker(browser, t, docs_dir)
                except Exception as e:  # noqa: BLE001
                    ok, ng = False, [f"例外: {e}"]
                if ok:
                    n_ok += 1
                else:
                    n_ng += 1
                    print(f"[NG] {t}: " + " / ".join(ng))
            browser.close()
    finally:
        proc.terminate()
    print(f"\n一致 {n_ok}・不一致 {n_ng}（{len(tickers)}銘柄、docs={docs_dir}）")
    return 0 if n_ng == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
