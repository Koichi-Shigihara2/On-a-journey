"""
tests/test_ttm_flow_key_names.py

[[REGISTRATION-VALIDATOR-TTM-REVENUE-KEY-STALE-1]]の回帰テスト。
ttm_calculator.pyのフェーズC移行（2026-07-25）でttm/*_ttm_series.jsonのflowキーは
snake_case（"revenue"・"net_income"等）に変わったが、registration_validator.pyの
P2-A（年次売上とTTM売上の乖離チェック）は旧PascalCaseキー"Revenue"を読み続け、
常にNoneで無言のままスキップされていた（[[TTM-PASCALCASE-KEY-STALE-1]]の取り残し）。

- （P2-A自体は2026-09-24に廃止。[[REGISTRATION-VALIDATOR-P2A-PERIOD-MISMATCH-1]]）
- 再発防止: ttm/を読む全ソースとテストのモックから、flowに対する旧PascalCase
  キー参照を検出する（CHAT_RULES.md事例15: モックがバグと同じ誤りを再現して
  いると検知できないため、テスト側も検査対象に含める）

実行方法:
    python -m pytest tests/test_ttm_flow_key_names.py -v
"""

import glob
import json
import os
import re
import shutil
import sys

import pytest

_REPO = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_SEC_DATA_DIR = os.path.join(_REPO, "common", "sec_data")
if _SEC_DATA_DIR not in sys.path:
    sys.path.insert(0, _SEC_DATA_DIR)

import registration_validator as rv  # noqa: E402

_REAL_TTM_DIR = os.path.join(_SEC_DATA_DIR, "ttm")


def _real_series0(ticker):
    with open(os.path.join(_REAL_TTM_DIR, f"{ticker}_ttm_series.json"), encoding="utf-8") as f:
        return json.load(f)["series"][0]


def test_p2a_removed():
    """旧P2-A（_ttm_revenue()を使う年次売上とTTM売上の比較）は2026-09-24に廃止
    （[[REGISTRATION-VALIDATOR-P2A-PERIOD-MISMATCH-1]]）。代替は新規登録フロー
    Step 7.5のCHECK-35/41/47（tests/test_registration_consistency_gate.py）"""
    assert not hasattr(rv, "_ttm_revenue")


# ── 再発防止: flowに対する旧PascalCaseキー参照の検出 ─────────────────────
# ttm_calculator.py移行前のflowキー（quarterly.py/normalizer.pyのPascalCase名）。
# "FCF"はttm_calculator.pyが現在もこの名前で書き出す正規のキーのため対象外。
from common.sec_data.q4_implied import _SNAKE_TO_PASCAL  # noqa: E402

_OLD_KEYS = sorted(set(_SNAKE_TO_PASCAL.values()) - {"_COGS"} | {"EPSBasic", "EPSDiluted"})
_OLD = "|".join(_OLD_KEYS)
# flow（またはflow系の変数名）に続く .get("Old") / ["Old"] と、モックの "flow": {"Old": ...}
_PATTERNS = [
    re.compile(r'''flow\w*["']?\]?\s*(?:,\s*\{\}\s*\))?\s*(?:\.get\(\s*|\[\s*)["'](%s)["']''' % _OLD),
    re.compile(r'''["']flow["']\s*:\s*\{\s*["'](%s)["']''' % _OLD),
]
_TTM_MARKERS = ("_ttm_series", "TTMReader", "calc_ttm_series", "ttm_series", "TTM_DIR")


def _ttm_reading_files():
    files = []
    for pat in ("common/**/*.py", "src/**/*.py", "tests/*.py"):
        for p in glob.glob(os.path.join(_REPO, pat), recursive=True):
            if os.path.basename(p) == os.path.basename(__file__):
                continue
            with open(p, encoding="utf-8") as f:
                txt = f.read()
            if any(m in txt for m in _TTM_MARKERS):
                files.append((p, txt))
    return files


def test_ttm_reading_files_found():
    names = {os.path.basename(p) for p, _ in _ttm_reading_files()}
    # 検出対象の取りこぼしがないこと（主要な消費者が含まれる）
    assert {"data_fetcher.py", "pipeline.py", "registration_validator.py", "audit.py"} <= names


def test_no_pascalcase_flow_keys():
    hits = []
    for p, txt in _ttm_reading_files():
        for i, line in enumerate(txt.split("\n"), 1):
            if line.lstrip().startswith("#"):
                continue
            for pat in _PATTERNS:
                m = pat.search(line)
                if m:
                    hits.append(f"{os.path.relpath(p, _REPO)}:{i}: {m.group(1)}")
    assert hits == [], "ttm flowに旧PascalCaseキー参照: " + ", ".join(hits)


@pytest.mark.parametrize("line", [
    'rv = d["series"][0].get("flow", {}).get("Revenue")',
    'x = flow.get("NetIncome", {}).get("val")',
    'y = s["flow"]["OCF"]["val"]',
    'mock = {"flow": {"Revenue": {"val": 1}}}',
])
def test_detector_catches_known_patterns(line):
    assert any(p.search(line) for p in _PATTERNS)


@pytest.mark.parametrize("line", [
    'x = flow.get("revenue", {}).get("val")',
    'vals = [s["flow"]["FCF"]["val"] for s in filtered]',
    'get_quarterly_series(normalized, "Revenue")',
])
def test_detector_ignores_valid_patterns(line):
    assert not any(p.search(line) for p in _PATTERNS)
