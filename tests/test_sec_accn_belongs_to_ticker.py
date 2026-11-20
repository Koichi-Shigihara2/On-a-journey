"""銘柄ごとに accn を持つ保存の記録が、その銘柄の申告の一覧（company_facts・旧CIKのcompany_facts_legacy・submissions）に
あるかを全銘柄で確かめる（別の会社の申告を取り違えて保存していないかの守り。EXTRACTED-FILINGS-MISMATCH-1）。

対象（銘柄のフォルダ `common/sec_data/data/{T}/`）:
  - 全JSONの `accn`・`source_accn`（annual_*・quarterly_*・来歴など）
  - `bs_lines/{accn}.json`・`cf_lines/{accn}.json` のファイル名
派生（ファイル名が `{T}_` で始まる）: `common/sec_data/normalized/`・`common/sec_data/ttm/`・`docs/common/sec_data/`
設定: `common/sec_data/fact_overrides.json`（トップのキーが銘柄）
銘柄名・申告番号は手で書かず、すべてデータから読む。
"""
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "common" / "sec_data" / "data"
ACC = re.compile(r"^\d{10}-\d{2}-\d{6}$")
KEYS = ("accn", "source_accn")
SKIP = {"company_facts.json", "submissions.json"}


def _load(p):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _valid_accns(tdir: Path):
    s = set()
    for p in list(tdir.glob("company_facts*.json")):
        d = _load(p)
        for tax in ((d or {}).get("facts") or {}).values():
            for c in tax.values():
                for arr in (c.get("units") or {}).values():
                    for x in arr:
                        if x.get("accn"):
                            s.add(x["accn"])
    sp = tdir / "submissions.json"
    d = _load(sp) if sp.exists() else None
    if d:
        s.update(((d.get("filings") or {}).get("recent") or {}).get("accessionNumber", []))
    return s


def _walk(o, out):
    if isinstance(o, dict):
        for k, v in o.items():
            if k in KEYS and isinstance(v, str) and ACC.match(v):
                out.append(v)
            _walk(v, out)
    elif isinstance(o, list):
        for v in o:
            _walk(v, out)


def _accns_in(path: Path):
    d = _load(path)
    out = []
    _walk(d, out)
    return out


def _violations():
    if not DATA.exists():
        pytest.skip("common/sec_data/data が無い")
    tickers = sorted(p.name for p in DATA.iterdir() if p.is_dir())
    valid = {t: _valid_accns(DATA / t) for t in tickers}
    viol = []
    checked = 0
    for t in tickers:
        if not valid[t]:
            continue
        for p in (DATA / t).rglob("*.json"):
            if p.name in SKIP or p.name.startswith("company_facts"):
                continue
            for a in _accns_in(p):
                checked += 1
                if a not in valid[t]:
                    viol.append(f"{t}: {p.relative_to(ROOT).as_posix()} の accn {a}")
            if p.parent.name in ("bs_lines", "cf_lines"):
                checked += 1
                if p.stem not in valid[t]:
                    viol.append(f"{t}: {p.relative_to(ROOT).as_posix()}（ファイル名の accn）")
    for rel in ("common/sec_data/normalized", "common/sec_data/ttm", "docs/common/sec_data"):
        base = ROOT / rel
        if not base.exists():
            continue
        for p in base.rglob("*.json"):
            t = p.name.split("_")[0].split(".")[0]
            if t in valid and valid[t]:
                for a in _accns_in(p):
                    checked += 1
                    if a not in valid[t]:
                        viol.append(f"{t}: {p.relative_to(ROOT).as_posix()} の accn {a}")
    fo = _load(ROOT / "common" / "sec_data" / "fact_overrides.json") or {}
    for t, d in fo.items():
        if t in valid and valid[t]:
            out = []
            _walk(d, out)
            for a in out:
                checked += 1
                if a not in valid[t]:
                    viol.append(f"{t}: common/sec_data/fact_overrides.json の accn {a}")
    return viol, checked


def test_every_saved_accn_is_in_the_tickers_own_filings():
    viol, checked = _violations()
    assert checked > 1000, f"点検の件数が少なすぎる（{checked}）。読み取りが壊れていないか"
    assert not viol, "銘柄の申告の一覧に無い accn（別の会社の申告の取り違えの疑い）:\n" + "\n".join(viol[:30])


def test_scanner_detects_a_foreign_accn(tmp_path):
    """点検が空振りしないこと: 別の会社の accn を混ぜたデータでは検出する"""
    valid = {"0000000001-26-000001"}
    out = []
    _walk({"a": {"accn": "0000000001-26-000001"}, "b": [{"source_accn": "0000000002-26-000002"}]}, out)
    assert [a for a in out if a not in valid] == ["0000000002-26-000002"]
