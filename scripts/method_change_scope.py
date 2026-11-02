"""
scripts/method_change_scope.py
計算方法の変更の scope を、全銘柄のシミュレーションの前後から機械的に作る（[[METHOD-CHANGE-PUBLISH-TIMING-1]]、2026-11-02）。
config/method_change.json の scope は手で書かない。

手順（CHAT_RULES.md・CLAUDE_CODE_START.md「計算方法の変更の手順」）:
  1. 株価・市場データを固定して、前のコードで TANUKI VALUATION・HypeCore・Stonks・ROTCE を手元で回し、
     python scripts/method_change_scope.py snapshot <前のフォルダ>
  2. 新しいコードで同じように回し、
     python scripts/method_change_scope.py snapshot <後のフォルダ>
  3. python scripts/method_change_scope.py diff --id <BACKLOGの項目ID> --before <前> --after <後> [--write]
     差のあった銘柄・項目を {"ticker", "fields", "before", "after"} にし、--write で
     config/method_change.json に {"id", "committed_at", "scope"} を足す（同じIDの新しい形の件があれば止める）

スナップショットのフォルダ:
  tanuki/{T}.json または tanuki/{T}/latest.json   … TANUKI VALUATIONのlatest.json（項目はキーのまま）
  hypecore/{T}_poc.json                            … HypeCoreの最新の月（項目は "hype.<キー>"）
  stonks/results.json                              … Stonksの各銘柄（項目は "stonks.<キー>"）
  rotce/{T}.json                                   … ROTCEの各銘柄（項目は "rotce.<キー>"）
  無いものは比べない。dictは1段だけ "<キー>.<キー>" に開き、listは比べない。時刻・印のキーは比べない
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(REPO_ROOT, "config", "method_change.json")
SOURCES = {
    "tanuki": os.path.join(REPO_ROOT, "docs", "value-monitor", "tanuki_valuation", "data"),
    "hypecore": os.path.join(REPO_ROOT, "src", "value", "hypecore", "data"),
    "stonks": os.path.join(REPO_ROOT, "docs", "value-monitor", "stonks-silo", "data", "results.json"),
    "rotce": os.path.join(REPO_ROOT, "docs", "common", "sec_data", "rotce"),
}
IGNORE = {"calculation_date", "generated", "generated_at", "checked_at", "method_change", "method_change_applied"}
JST = timezone(timedelta(hours=9))


def _read(path: str) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def flatten(d: dict, prefix: str = "") -> dict[str, Any]:
    """スカラーの項目だけを1段開いて返す（listは比べない、時刻・印のキーは除く）"""
    out: dict[str, Any] = {}
    for k, v in (d or {}).items():
        if k in IGNORE:
            continue
        if isinstance(v, dict):
            for k2, v2 in v.items():
                if k2 not in IGNORE and not isinstance(v2, (dict, list)):
                    out[f"{prefix}{k}.{k2}"] = v2
        elif not isinstance(v, list):
            out[f"{prefix}{k}"] = v
    return out


def load_snapshot(root: str) -> dict[str, dict[str, Any]]:
    """{銘柄: {項目: 値}}"""
    snap: dict[str, dict[str, Any]] = {}

    def put(t: str, fields: dict[str, Any]) -> None:
        snap.setdefault(t.upper(), {}).update(fields)

    tdir = os.path.join(root, "tanuki")
    if os.path.isdir(tdir):
        for name in sorted(os.listdir(tdir)):
            p = os.path.join(tdir, name)
            if name.endswith(".json") and os.path.isfile(p):
                put(name[:-5], flatten(_read(p)))
            elif os.path.isfile(os.path.join(p, "latest.json")):
                put(name, flatten(_read(os.path.join(p, "latest.json"))))
    hdir = os.path.join(root, "hypecore")
    if os.path.isdir(hdir):
        for name in sorted(os.listdir(hdir)):
            if name.endswith("_poc.json"):
                monthly = (_read(os.path.join(hdir, name)) or {}).get("monthly") or []
                if monthly:
                    put(name[: -len("_poc.json")], flatten(monthly[-1], "hype."))
    spath = os.path.join(root, "stonks", "results.json")
    if os.path.isfile(spath):
        for t, row in ((_read(spath) or {}).get("tickers") or {}).items():
            put(t, flatten(row, "stonks."))
    rdir = os.path.join(root, "rotce")
    if os.path.isdir(rdir):
        for name in sorted(os.listdir(rdir)):
            if name.endswith(".json") and not name.startswith("_"):
                put(name[:-5], flatten(_read(os.path.join(rdir, name)), "rotce."))
    return snap


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12) or (math.isnan(a) and math.isnan(b))
    return a == b


def build_scope(before: dict, after: dict) -> list[dict]:
    """前の出力にあった項目のうち、値が変わった（無くなったも含む）銘柄・項目の一覧（銘柄の順）。
    前の出力に無かった項目（その変更で足した新しいキー）と、前の出力に無かった銘柄は数えない
    （新しいキーを数えると全銘柄がscopeに入る。[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段1の公開の前、2026-11-02）"""
    scope = []
    for t in sorted(before):
        b, a = before.get(t) or {}, after.get(t) or {}
        fields = sorted(k for k in b if not _same(b.get(k), a.get(k)))
        if fields:
            scope.append({"ticker": t, "fields": fields,
                          "before": {k: b.get(k) for k in fields}, "after": {k: a.get(k) for k in fields}})
    return scope


def make_entry(change_id: str, before_root: str, after_root: str, committed_at: str | None = None) -> dict:
    return {"id": change_id,
            "committed_at": committed_at or datetime.now(JST).isoformat(timespec="seconds"),
            "scope": build_scope(load_snapshot(before_root), load_snapshot(after_root))}


def write_entry(entry: dict, config_path: str = CONFIG_PATH) -> None:
    cfg = _read(config_path) if os.path.exists(config_path) else {"changes": []}
    for c in cfg.get("changes") or []:
        if c.get("id") == entry["id"] and "committed_at" in c:
            raise SystemExit(f"{entry['id']} の新しい形の件がもうある（2回目の変更はIDの末尾に番号を足す）")
    cfg.setdefault("changes", []).append(entry)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.write("\n")


def snapshot(dest: str) -> None:
    """今の手元の出力を dest に写す（シミュレーションの前・後で1回ずつ）"""
    os.makedirs(dest, exist_ok=True)
    src = SOURCES["tanuki"]
    for t in sorted(os.listdir(src)):
        p = os.path.join(src, t, "latest.json")
        if os.path.isfile(p):
            os.makedirs(os.path.join(dest, "tanuki"), exist_ok=True)
            shutil.copy2(p, os.path.join(dest, "tanuki", f"{t}.json"))
    for key, pred in (("hypecore", lambda n: n.endswith("_poc.json")),
                      ("rotce", lambda n: n.endswith(".json") and not n.startswith("_"))):
        if os.path.isdir(SOURCES[key]):
            os.makedirs(os.path.join(dest, key), exist_ok=True)
            for n in sorted(os.listdir(SOURCES[key])):
                if pred(n):
                    shutil.copy2(os.path.join(SOURCES[key], n), os.path.join(dest, key, n))
    if os.path.isfile(SOURCES["stonks"]):
        os.makedirs(os.path.join(dest, "stonks"), exist_ok=True)
        shutil.copy2(SOURCES["stonks"], os.path.join(dest, "stonks", "results.json"))


def summarize(entry: dict) -> str:
    from collections import Counter
    cnt = Counter(f for s in entry["scope"] for f in s["fields"])
    watched = [s["ticker"] for s in entry["scope"]
               if {"tanuki_score", "funda_score", "upside_percent", "intrinsic_value_per_share", "hype.stage"} & set(s["fields"])]
    lines = [f"{entry['id']}: 銘柄 {len(entry['scope'])}（判定・funda・乖離率・理論株価・stageが変わる銘柄 {len(watched)}: {', '.join(watched) or 'なし'}）"]
    lines += [f"  {f}: {n}銘柄" for f, n in cnt.most_common(20)]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[2])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("dest")
    d = sub.add_parser("diff")
    d.add_argument("--id", required=True)
    d.add_argument("--before", required=True)
    d.add_argument("--after", required=True)
    d.add_argument("--committed-at")
    d.add_argument("--write", action="store_true")
    d.add_argument("--config", default=CONFIG_PATH)
    a = ap.parse_args(argv)
    if a.cmd == "snapshot":
        snapshot(a.dest)
        print(f"保存: {a.dest}")
        return 0
    entry = make_entry(a.id, a.before, a.after, a.committed_at)
    print(summarize(entry))
    if a.write:
        write_entry(entry, a.config)
        print(f"書き込み: {a.config}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
