"""
tests/test_external_trigger_worker.py

2026-10-03（外部起動の準備）: tools/external_trigger/（Cloudflare Worker）の設定を確かめる。
- wrangler.tomlのcronが無料プランの上限（アカウントあたり5本）に収まり、平日20:25・20:55・21:25・21:50 UTCを表す
- lib.jsのNYSE_HOLIDAYSが、daily_guard.pyと同じpandas_market_calendarsのNYSEカレンダーの平日休場日と一致する
- nodeがあればlib.test.js（node --test）を実行する（無ければskip）
"""

import json
import os
import re
import shutil
import subprocess
import tomllib

import pandas as pd
import pandas_market_calendars as mcal
import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DIR = os.path.join(_ROOT, "tools", "external_trigger")
_FREE_PLAN_CRON_LIMIT = 5   # https://developers.cloudflare.com/workers/platform/limits/ （2026-10-03確認）
_DOW = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}


def _crons():
    with open(os.path.join(_DIR, "wrangler.toml"), "rb") as f:
        return tomllib.load(f)["triggers"]["crons"]


def _expand(field, names=None):
    out = set()
    for part in field.split(","):
        if "-" in part:
            a, b = part.split("-")
            a, b = (names[a], names[b]) if names else (int(a), int(b))
            out.update(range(a, b + 1))
        else:
            out.add(names[part] if names else int(part))
    return out


def test_crons_fit_free_plan_and_cover_the_four_times():
    crons = _crons()
    assert len(crons) <= _FREE_PLAN_CRON_LIMIT
    times = set()
    for c in crons:
        minute, hour, dom, month, dow = c.split()
        assert (dom, month) == ("*", "*")
        assert _expand(dow, _DOW) == {0, 1, 2, 3, 4}, f"{c}: 平日（MON-FRI）以外を含む"
        times |= {(h, m) for h in _expand(hour) for m in _expand(minute)}
    assert times == {(20, 25), (20, 55), (21, 25), (21, 50)}


def test_wrangler_has_no_public_url():
    with open(os.path.join(_DIR, "wrangler.toml"), "rb") as f:
        assert tomllib.load(f)["workers_dev"] is False


def _js_holidays():
    with open(os.path.join(_DIR, "lib.js"), encoding="utf-8") as f:
        src = f.read()
    block = re.search(r"export const NYSE_HOLIDAYS = \{(.*?)\};", src, re.S).group(1)
    return {int(y): json.loads(days) for y, days in re.findall(r"(\d{4}):\s*(\[[^\]]*\])", block)}


def test_holiday_table_matches_nyse_calendar():
    table = _js_holidays()
    assert 2026 in table and 2027 in table
    cal = mcal.get_calendar("NYSE")
    for year, days in table.items():
        sched = cal.schedule(start_date=f"{year}-01-01", end_date=f"{year}-12-31")
        open_days = set(sched.index.date)
        expected = [d.strftime("%m-%d") for d in pd.bdate_range(f"{year}-01-01", f"{year}-12-31")
                    if d.date() not in open_days]
        assert days == expected, f"{year}年"


def test_worker_module_exports_only_default():
    # メインモジュールの名前付きexportはWorkersでハンドラ・クラスとして扱われるため、worker.jsはdefaultだけ
    with open(os.path.join(_DIR, "worker.js"), encoding="utf-8") as f:
        src = f.read()
    assert re.findall(r"^export\s+(\S+)", src, re.M) == ["default"]


def test_node_unit_tests():
    node = shutil.which("node")
    if not node:
        pytest.skip("nodeが無い（tools/external_trigger/で`node --test`を実行すると同じ確認ができる）")
    r = subprocess.run([node, "--test"], cwd=_DIR, capture_output=True, text=True, encoding="utf-8", timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
