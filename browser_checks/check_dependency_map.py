"""Market Pulse / MACRO PULSE 実ブラウザ確認スクリプト
（SYSTEM_MAP.md「Market Pulse・MACRO PULSE 画面要素→導出関数→生データ
ソース 依存関係マップ」7要素を対象、2026-08-26新設）

目的:
  データ層・導出層の正しさは pytest/audit.py/report_consistency_check.py
  で確認できるが、フロントエンド描画（実ブラウザでの表示）までは
  カバーされていない。本スクリプトは、生データファイル（CSV/JSON）から
  独立に計算した「期待値」と、Playwrightで実際にレンダリングした
  ブラウザ上の値を突き合わせ、7要素それぞれについて一致/不一致を報告する。

対象の7要素（SYSTEM_MAP.md参照）:
  1. MACRO PULSEゲージ（#pg-score-num）
  2. MACRO PULSE AIウィークリーコメンタリー（.ai-card-score）
  3. MACRO PULSEスコア推移チャート・tooltip
  4. Hindenburg omen関連表示（Market PulseのTake Profit/Buyチェックリスト）
  5. Hollow Rally関連表示（MACRO PULSE流動性モニター上部バッジ）
  6. Fear & Greed関連表示（Market Pulse F&Gゲージ）
  7. breadth_summary関連表示（Market Pulse 市場の広がり）

前提:
  - venv（Playwright 1.62.0、chromiumインストール済み）を使う。
    `python -m playwright install chromium` が未実施の環境では先に実行する。
  - docs/ 全体をルート配信するローカルHTTPサーバーを本スクリプトが
    自動起動する（本番のGitHub Pagesルート配信と同じ相対パス構造で
    確認するため。docs/配下だけを配信するテストサーバーと本番の配信
    構造が一致しているかは、コンソールエラー0件であることで間接確認する）。

スコープ外（意図的）:
  - CI組み込み・定期自動実行化は行わない（本スクリプトは手動実行のみを
    想定。CI化はKoichiさんの投資判断事項のため別途）。
  - pytestの自動収集対象にしない（tests/配下に置かない・ファイル名も
    test_*にしない。Playwright実行にはブラウザバイナリのダウンロードが
    必要で、通常のpytest実行環境には無いことが多いため）。

使い方:
  cd C:\\Users\\shigi\\Documents\\On-a-journey-git
  venv\\Scripts\\activate
  python browser_checks\\check_dependency_map.py
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(REPO_ROOT, "docs")
PORT = 8791
BASE_URL = f"http://localhost:{PORT}"

MACRO_PULSE_URL = f"{BASE_URL}/market-monitor/macro-pulse/index.html"
MARKET_PULSE_URL = f"{BASE_URL}/market-monitor/market-pulse/index.html"

LIQUIDITY_CSV = os.path.join(
    DOCS_DIR, "market-monitor", "macro-pulse", "data", "05_liquidity.csv"
)
WEEKLY_ANALYSIS_CSV = os.path.join(
    DOCS_DIR, "market-monitor", "macro-pulse", "data", "05_weekly_analysis.csv"
)
MARKET_DATA_JSON = os.path.join(
    DOCS_DIR, "market-monitor", "market-pulse", "data", "market_data.json"
)
BREADTH_DATA_JSON = os.path.join(
    DOCS_DIR, "market-monitor", "market-pulse", "data", "breadth_data.json"
)


@dataclass
class CheckResult:
    element: str
    expected: Any
    actual: Any
    passed: Optional[bool]  # None=判定不能（2026-09-26、MP全要素化で追加）
    note: str = ""


@dataclass
class RunReport:
    results: list = field(default_factory=list)
    console_errors: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────
#  期待値の独立計算（生データファイルから直接算出。ブラウザが取得した
#  値とは無関係に、このスクリプト自身がファイルを読み直す）
# ─────────────────────────────────────────────────────────────────

def expected_weekly_score() -> Optional[int]:
    """05_weekly_analysis.csv の最新行の score を返す
    （renderPhaseGauge() の WEEKLY_SNAPSHOT ロジックが正とする値と同じ
    ソース。macro-pulse/index.html:2242-2244 参照）。
    """
    if not os.path.exists(WEEKLY_ANALYSIS_CSV):
        return None
    with open(WEEKLY_ANALYSIS_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None
    rows.sort(key=lambda r: r.get("analysis_date", ""))
    latest = rows[-1]
    try:
        return int(float(latest["score"]))
    except (KeyError, ValueError, TypeError):
        return None


def expected_hollow_rally_trigger() -> tuple[Optional[bool], dict]:
    """05_liquidity.csv から Hollow Rally 判定条件
    （S&P500 5営業日リターン > +1.0% かつ NET流動性の前回行比 < -0.5%）
    を独立に再計算する。ロジックは macro-pulse/index.html の
    インライン実装（2590-2607行目付近、専用関数化されていない）を
    Pythonで再現したもの。
    """
    if not os.path.exists(LIQUIDITY_CSV):
        return None, {}
    with open(LIQUIDITY_CSV, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    sp500_rows = [r for r in rows if r.get("sp500") not in ("", None)]
    nl_rows = [r for r in rows if r.get("net_liquidity") not in ("", None)]
    if len(sp500_rows) < 6 or len(nl_rows) < 2:
        return False, {"reason": "insufficient rows"}
    sp_latest = float(sp500_rows[-1]["sp500"])
    sp_5d_ago = float(sp500_rows[max(0, len(sp500_rows) - 6)]["sp500"])
    sp5d_chg = (sp_latest - sp_5d_ago) / abs(sp_5d_ago) * 100
    nl_latest = float(nl_rows[-1]["net_liquidity"])
    nl_prev = float(nl_rows[-2]["net_liquidity"])
    nl_wk_chg_pct = (nl_latest - nl_prev) / abs(nl_prev or 1) * 100
    trigger = sp5d_chg > 1.0 and nl_wk_chg_pct < -0.5
    return trigger, {"sp5d_chg_pct": round(sp5d_chg, 3), "nl_wk_chg_pct": round(nl_wk_chg_pct, 3)}


def expected_fear_greed_score() -> Optional[float]:
    """market_data.json 最新エントリの fear_greed.score を返す
    （Market Pulseにおける唯一のCNN F&G取得経路、
    collect_and_send.py::fetch_cnn_fear_greed() が生成）。
    """
    if not os.path.exists(MARKET_DATA_JSON):
        return None
    with open(MARKET_DATA_JSON, encoding="utf-8") as f:
        data = json.load(f)
    if not data:
        return None
    return data[-1].get("fear_greed", {}).get("score")


def expected_breadth_summary() -> dict:
    """market_data.json 最新エントリの sentiment.breadth を返す。"""
    if not os.path.exists(MARKET_DATA_JSON):
        return {}
    with open(MARKET_DATA_JSON, encoding="utf-8") as f:
        data = json.load(f)
    if not data:
        return {}
    return data[-1].get("sentiment", {}).get("breadth") or {}


def expected_hindenburg_active() -> tuple[Optional[bool], dict]:
    """breadth_data.json 最新エントリから Hindenburg Omen 判定
    （新高値・新安値がともに全銘柄数の2.2%を超えて出現）を独立に
    再計算する。ロジックは collect_and_send.py::calc_hindenburg_active()
    のPython再実装（同一ファイルを再度読み込むだけであり、本番コード
    そのものをimportして呼び出す方がより厳密だが、本スクリプトは
    フロントエンド確認が主目的のため計算式を直接複製している。
    計算式自体の正しさはcollect_and_send.py側の既存pytestで別途担保
    される前提）。
    """
    if not os.path.exists(BREADTH_DATA_JSON):
        return None, {}
    with open(BREADTH_DATA_JSON, encoding="utf-8") as f:
        data = json.load(f)
    if not data:
        return None, {}
    latest = data[-1]
    nh = latest.get("new_highs_52w") or 0
    nl = latest.get("new_lows_52w") or 0
    total_stocks = latest.get("total_stocks") or 500
    active = bool(nh >= total_stocks * 0.022 and nl >= total_stocks * 0.022)
    return active, {"new_highs_52w": nh, "new_lows_52w": nl, "total_stocks": total_stocks}


# ─────────────────────────────────────────────────────────────────
#  ローカルHTTPサーバー（docs/ をルート配信、本番GitHub Pagesと同じ
#  相対パス構造で確認するため）
# ─────────────────────────────────────────────────────────────────

def start_server() -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(PORT), "--directory", DOCS_DIR],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        try:
            urllib.request.urlopen(f"{BASE_URL}/market-monitor/macro-pulse/index.html", timeout=1)
            return proc
        except Exception:
            time.sleep(0.2)
    proc.terminate()
    raise RuntimeError("ローカルHTTPサーバーの起動に失敗しました")


# ─────────────────────────────────────────────────────────────────
#  ブラウザ側の値取得
# ─────────────────────────────────────────────────────────────────

def run_browser_checks(report: RunReport) -> None:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()

        # ── MACRO PULSE ページ ──
        page = browser.new_page()
        errors: list[str] = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.goto(MACRO_PULSE_URL, wait_until="networkidle")
        page.wait_for_selector("#pg-score-num", timeout=15000)
        # WEEKLY_SNAPSHOT反映・チャート描画の非同期処理を待つ
        page.wait_for_timeout(2500)
        report.console_errors["macro-pulse"] = errors

        # ① MACRO PULSEゲージ
        gauge_text = page.text_content("#pg-score-num") or ""
        expected_score = expected_weekly_score()
        actual_score = int(gauge_text) if gauge_text.strip().lstrip("-").isdigit() else None
        report.results.append(CheckResult(
            element="① MACRO PULSEゲージ（#pg-score-num）",
            expected=expected_score,
            actual=actual_score,
            passed=(expected_score is not None and expected_score == actual_score),
            note="期待値=05_weekly_analysis.csv最新行のscore",
        ))

        # ② AIウィークリーコメンタリー
        ai_card = page.query_selector(".ai-card-score")
        ai_text = ai_card.text_content().strip() if ai_card else None
        ai_actual = int(ai_text) if ai_text and ai_text.lstrip("-").isdigit() else None
        report.results.append(CheckResult(
            element="② AIウィークリーコメンタリー（.ai-card-score 最新カード）",
            expected=expected_score,
            actual=ai_actual,
            passed=(expected_score is not None and expected_score == ai_actual),
            note="ゲージと同一値を参照する設計（[[MACRO-PULSE-3M-FORECAST-SNAPSHOT-MISMATCH-1]]統合済み）",
        ))

        # ③ スコア推移チャート・tooltip
        chart_info = page.evaluate("""
        () => {
          const el = document.getElementById('scoreHistoryChart');
          const chart = echarts.getInstanceByDom(el);
          if (!chart) return null;
          const opt = chart.getOption();
          const series = opt.series.filter(s => s.data && s.data.length > 0);
          const mainSeries = series[series.length - 1];
          const mainIdx = opt.series.indexOf(mainSeries);
          const dataIdx = mainSeries.data.length - 1;
          const lastPoint = mainSeries.data[dataIdx];
          chart.dispatchAction({type: 'showTip', seriesIndex: mainIdx, dataIndex: dataIdx});
          return {lastPoint, mainIdx, dataIdx};
        }
        """)
        page.wait_for_timeout(400)
        tooltip_texts = page.evaluate("""
        () => {
          const all = Array.from(document.querySelectorAll('*'));
          return all
            .filter(e => e.tagName !== 'SCRIPT' && e.children.length === 0 && e.textContent
                         && (e.textContent.includes('本日の実測値') || e.textContent.includes('補間表示')))
            .map(e => e.textContent);
        }
        """)
        chart_today_value = chart_info["lastPoint"]["value"][1] if chart_info else None
        chart_today_date = chart_info["lastPoint"]["value"][0] if chart_info else None
        tooltip_has_note = any("本日の実測値" in t for t in tooltip_texts)
        report.results.append(CheckResult(
            element="③ スコア推移チャート・tooltip",
            expected=f"score={expected_score}, tooltip注記あり",
            actual=f"score={chart_today_value} (date={chart_today_date}), tooltip注記={'あり' if tooltip_has_note else 'なし'}",
            passed=(expected_score is not None and chart_today_value == expected_score and tooltip_has_note),
            note="本日のチャート点はゲージと同一値・tooltipに「本日の実測値」注記が出ることを確認",
        ))

        # ⑤ Hollow Rally
        badge = page.query_selector(".hollow-rally-badge")
        badge_present = badge is not None
        expected_trigger, hr_detail = expected_hollow_rally_trigger()
        report.results.append(CheckResult(
            element="⑤ Hollow Rally関連表示（.hollow-rally-badge）",
            expected=f"badge表示={expected_trigger} ({hr_detail})",
            actual=f"badge表示={badge_present}",
            passed=(expected_trigger == badge_present),
            note="05_liquidity.csvから条件を独立再計算(S&P500 5d>+1.0% かつ NET流動性前回比<-0.5%)",
        ))

        page.close()

        # ── Market Pulse ページ ──
        page2 = browser.new_page()
        errors2: list[str] = []
        page2.on("console", lambda msg: errors2.append(msg.text) if msg.type == "error" else None)
        page2.goto(MARKET_PULSE_URL, wait_until="networkidle")
        page2.wait_for_selector("#fgGaugeScore", timeout=15000)
        page2.wait_for_timeout(2000)
        report.console_errors["market-pulse"] = errors2

        # ⑥ Fear & Greed
        fg_text = (page2.text_content("#fgGaugeScore") or "").strip()
        fg_actual = int(fg_text) if fg_text.lstrip("-").isdigit() else None
        fg_expected_raw = expected_fear_greed_score()
        fg_expected = round(fg_expected_raw) if fg_expected_raw is not None else None
        report.results.append(CheckResult(
            element="⑥ Fear & Greed関連表示（#fgGaugeScore）",
            expected=f"{fg_expected} (raw={fg_expected_raw})",
            actual=fg_actual,
            passed=(fg_expected is not None and fg_expected == fg_actual),
            note="期待値=market_data.json最新fear_greed.scoreを四捨五入(表示側は.toFixed(0))",
        ))

        # ⑦ breadth_summary
        live_data = page2.evaluate("""
        () => {
          const latest = filteredData[filteredData.length - 1];
          return {
            breadth: latest && latest.sentiment ? latest.sentiment.breadth : null,
            tp: latest ? latest.take_profit_checklist : null,
            buy: latest ? latest.buy_checklist : null,
          };
        }
        """)
        browser_breadth = live_data.get("breadth") or {}
        expected_breadth = expected_breadth_summary()
        breadth_match = all(
            browser_breadth.get(k) == v for k, v in expected_breadth.items()
        ) and bool(expected_breadth)
        report.results.append(CheckResult(
            element="⑦ breadth_summary関連表示（filteredData[].sentiment.breadth）",
            expected=expected_breadth,
            actual=browser_breadth,
            passed=breadth_match,
            note="market_data.jsonを再読込した値とブラウザ上のJSデータを突き合わせ（データ取得・パース経路の確認）",
        ))

        # ④ Hindenburg omen
        expected_active, hind_detail = expected_hindenburg_active()
        tp_hindenburg = None
        buy_hindenburg_active = None
        tp = live_data.get("tp") or {}
        for c in tp.get("checks", []) or []:
            if c.get("key") == "hindenburg":
                tp_hindenburg = c
        buy = live_data.get("buy") or {}
        buy_hind = (buy.get("checks") or {}).get("hindenburg")
        if buy_hind:
            buy_hindenburg_active = buy_hind.get("active")
        checklist_visible = bool(tp.get("triggered") or buy.get("triggered") or buy.get("extreme"))
        passed = (expected_active is not None and buy_hindenburg_active == expected_active
                  and tp_hindenburg is not None and tp_hindenburg.get("passed") == (not expected_active))
        report.results.append(CheckResult(
            element="④ Hindenburg omen関連表示（Take Profit/Buyチェックリスト）",
            expected=f"active={expected_active} ({hind_detail})",
            actual=f"buy_checklist.hindenburg.active={buy_hindenburg_active}, "
                   f"take_profit_checklist.hindenburg.passed={tp_hindenburg.get('passed') if tp_hindenburg else None}",
            passed=passed,
            note=(
                "breadth_data.jsonから2.2%閾値判定を独立再計算し、ブラウザがロードした"
                "market_data.json由来のチェックリストデータと突合。"
                + ("" if checklist_visible else
                   " 【注記】現在F&Gが25〜75の中立域のためチェックリストUI自体は非発動"
                   "（「非発動」バナーのみ表示）で、個別チェック項目はDOM上に描画されない。"
                   "本チェックはブラウザがロードしたデータの整合性を検証するものであり、"
                   "画面上の可視表示そのものの確認ではない点に留意。")
            ),
        ))

        # MP-01〜MP-29（Market Pulse全要素、2026-09-26追加）
        from market_pulse_elements import run_market_pulse_element_checks
        run_market_pulse_element_checks(page2, report.results, CheckResult)

        page2.close()
        browser.close()


def main() -> int:
    report = RunReport()
    server = start_server()
    try:
        run_browser_checks(report)
        # D-01〜D-05（導出層・データ層の独立再計算、ブラウザ不要）
        from market_pulse_elements import run_derivation_checks
        run_derivation_checks(report.results, CheckResult)
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except Exception:
            server.kill()

    print("=" * 100)
    print("Market Pulse / MACRO PULSE 依存関係マップ 実ブラウザ確認結果")
    print("=" * 100)
    all_passed = True
    counts = {"一致": 0, "不一致": 0, "判定不能": 0}
    for r in report.results:
        status = "✅ 一致" if r.passed else ("➖ 判定不能" if r.passed is None else "❌ 不一致")
        counts[status.split()[1]] += 1
        if r.passed is False:
            all_passed = False
        print(f"\n{status}  {r.element}")
        print(f"  期待値: {r.expected}")
        print(f"  実測値: {r.actual}")
        if r.note:
            print(f"  備考: {r.note}")

    print("\n" + "-" * 100)
    for page_name, errs in report.console_errors.items():
        if errs:
            all_passed = False
            print(f"⚠ {page_name}: consoleエラー{len(errs)}件検出: {errs[:3]}")
        else:
            print(f"✅ {page_name}: consoleエラー0件")

    print(f"\n件数: 一致{counts['一致']} / 不一致{counts['不一致']} / 判定不能{counts['判定不能']}")
    print("\n" + "-" * 100)
    print("データ基準日の分類（market_data.json最新エントリ）")
    from market_pulse_elements import classify_base_dates
    for elem, d, cls, note in classify_base_dates():
        print(f"  {elem:40} {d or '-':12} {cls} {note}")

    print("\n" + "=" * 100)
    print(f"総合結果: {'✅ 全項目一致' if all_passed else '❌ 不一致あり（詳細は上記参照）'}")
    print("=" * 100)
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
