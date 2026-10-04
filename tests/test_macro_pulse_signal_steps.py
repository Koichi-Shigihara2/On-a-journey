"""MACRO PULSE: 8指標のカードとヘルスバーの判定が同じ段の定義から作られることの確認（2026-10-04 指示書M-5 STEP 5、
[[MACRO-PULSE-CARD-HEALTHBAR-MISMATCH-1]]）

- 05_main.py の SIGNAL_STEPS と index.html の SIGNAL_STEPS（JSON）が同じであること
- 実ブラウザ（Playwright）で、8指標それぞれの値と直近3点の向きを動かし、実際に描かれたカードの文言（拡張・中立・注意・後退シグナル）と
  ヘルスバーの判定（BULL・NEUTRAL・CAUTION・BEAR）が全ての値で一致すること。Playwright・chromium が無い環境では飛ばす
"""
import importlib.util
import json
import pathlib
import socket
import subprocess
import sys
import time
import urllib.request

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
HTML = ROOT / "docs" / "market-monitor" / "macro-pulse" / "index.html"
_spec = importlib.util.spec_from_file_location("main05_steps", ROOT / "src" / "market" / "macro_pulse" / "05_main.py")
main05 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(main05)

# (段のキー, events.csvの指標名, カードの名前, ヘルスバーの名前, 走査の範囲)
INDS = [
    ("yc", "Yield Curve 10Y-2Y", "YC 10Y-2Y", "YC 10Y-2Y", (-1.5, 2.0)),
    ("hy", "HY Spread", "HY Spread", "HY Spread", (2.0, 12.0)),
    ("philly", "Philadelphia Fed Manufacturing", "Philly Fed Mfg", "Philly Fed Mfg", (-30.0, 30.0)),
    ("cfnai", "Chicago Fed National Activity", "CFNAI MA3", "CFNAI MA3", (-3.0, 1.0)),
    ("sahm", "Sahm Rule Recession Indicator", "Sahm Rule", "Sahm Rule", (0.0, 1.5)),
    ("claims", "Initial Claims 4W MA", "Initial Claims", "Initial Claims 4WMA", (180000.0, 400000.0)),
    ("cbcc", "Michigan Consumer Sentiment", "Michigan Sent.", "Michigan Sentiment", (50.0, 110.0)),
    ("cbcc2", "Building Permits", "Building Permits", "Building Permits", (500.0, 2200.0)),
]
CARD_TO_BAR = {"拡張": "BULL", "中立": "NEUTRAL", "注意": "CAUTION", "後退シグナル": "BEAR"}


def _js_steps() -> dict:
    body = HTML.read_text(encoding="utf-8").split("// SIGNAL_STEPS_BEGIN", 1)[1].split("// SIGNAL_STEPS_END", 1)[0]
    return json.loads(body.split("=", 1)[1].strip().rstrip(";"))


def test_python_and_js_steps_are_the_same():
    assert "SIGNAL_STEPS_BEGIN" in HTML.read_text(encoding="utf-8")
    assert _js_steps() == main05.SIGNAL_STEPS


def _values(key, lo, hi):
    xs = {lo + (hi - lo) * i / 60 for i in range(61)}
    for t in main05.SIGNAL_STEPS[key]["tiers"]:
        span = (hi - lo) / 1000
        xs |= {t["x"] - span, t["x"], t["x"] + span}
    return sorted(xs)


@pytest.fixture(scope="module")
def page():
    pw = pytest.importorskip("playwright.sync_api")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = subprocess.Popen([sys.executable, "-m", "http.server", str(port), "--directory", str(ROOT / "docs")],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"http://127.0.0.1:{port}/market-monitor/macro-pulse/index.html"
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(url, timeout=1)
                break
            except Exception:
                time.sleep(0.2)
        with pw.sync_playwright() as p:
            try:
                browser = p.chromium.launch()
            except Exception as e:  # chromium が無い環境
                pytest.skip(f"chromium を起動できない: {e}")
            pg = browser.new_context(timezone_id="Asia/Tokyo").new_page()
            pg.goto(url)
            pg.wait_for_function("typeof IND_INDEX!=='undefined' && Object.keys(IND_INDEX).length>5 && typeof renderL2==='function'",
                                 timeout=60000)
            yield pg
            browser.close()
    finally:
        srv.terminate()


def test_card_and_healthbar_agree_for_all_values(page):
    if "SIGNAL_STEPS_BEGIN" not in HTML.read_text(encoding="utf-8"):
        pytest.fail("index.html に SIGNAL_STEPS が無い（カードとヘルスバーが別の閾値で判定している）")
    cases = [(key, ind, card, bar, v) for key, ind, card, bar, (lo, hi) in INDS for v in _values(key, lo, hi)]
    res = page.evaluate("""(cases) => {
      const out = [];
      const day = 86400000, now = Date.now();
      for (const [key, ind, card, bar, v] of cases) {
        for (const trend of [-1, 0, 1]) {
          const step = Math.max(Math.abs(v) * 0.01, 0.001);
          const vals = [v - 2 * trend * step, v - trend * step, v];
          const saved = IND_INDEX[ind];
          IND_INDEX[ind] = vals.map((x, i) => ({dateMs: now - (3 - i) * day, actual: x, updatedMs: 0, rev: null, revMs: null}));
          renderPhaseGauge(); renderL2();
          const c = [...document.querySelectorAll('#pg-signals .pg-sig')].find(e => e.querySelector('.pg-sig-name').textContent.trim() === card);
          const b = [...document.querySelectorAll('#l2Grid .l2-row')].find(e => e.querySelector('.l2-name').textContent.trim() === bar);
          out.push([key, v, trend, c ? c.querySelector('.pg-sig-badge').textContent.trim() : null,
                    b && b.querySelector('.l2-meta') ? b.querySelector('.l2-meta').textContent.trim() : null]);
          IND_INDEX[ind] = saved;
        }
      }
      return out;
    }""", cases)
    mismatch = [r for r in res if CARD_TO_BAR.get(r[3]) != r[4]]
    by_key = {}
    for k, v, tr, c, b in mismatch:
        by_key.setdefault(k, []).append((round(v, 4), tr, c, b))
    assert not mismatch, {k: (len(xs), xs[:4]) for k, xs in by_key.items()}
    assert len(res) == len(cases) * 3
