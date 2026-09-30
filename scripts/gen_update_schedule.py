"""更新スケジュールの一覧を .github/workflows/*.yml から自動で生成する（指示書㉘、2026-09-30）。

生成するもの:
  1. docs/architecture/UPDATE_SCHEDULE.md の「ワークフロー一覧」節（<!-- BEGIN GENERATED --> 〜 <!-- END GENERATED --> の間）
  2. config/workflow_dependencies.json のうちYAMLから決まる項目（各ワークフローの depends_on〈workflow_runの起動元〉・
     outputs〈commitするパス〉）。表示名（label）・yml・accepts_tickers・input_param と一括更新の順序（bulk_update_order・
     bulk_update_phases・new_ticker_order）は docs/value-monitor/admin.html の一括更新が使う手作業の定義のため、そのまま残す
     （accepts_tickers・input_paramは、YAMLの手動実行の入力と合っているかだけを確認する）

使い方:
  python scripts/gen_update_schedule.py          # 生成して書き込む
  python scripts/gen_update_schedule.py --check  # 書き込まずに、文書・JSONとの差分を表示（差分ありで終了コード1）
report_consistency_check.py の CHECK-58 が --check と同じ判定（check_drift()）を使う。
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WF_DIR = os.path.join(REPO, ".github", "workflows")
DOC = os.path.join(REPO, "docs", "architecture", "UPDATE_SCHEDULE.md")
DEPS = os.path.join(REPO, "config", "workflow_dependencies.json")
BEGIN = "<!-- BEGIN GENERATED: workflows（scripts/gen_update_schedule.pyが生成。手で編集しない） -->"
END = "<!-- END GENERATED: workflows -->"
DOW = ["日", "月", "火", "水", "木", "金", "土"]


# ─────────────────────────────────────────────────────────
#  YAMLの読み取り
# ─────────────────────────────────────────────────────────

def _expand(field: str, lo: int, hi: int) -> Optional[List[int]]:
    """cronの1項目を値のリストに展開する（*はNone）。"""
    if field == "*":
        return None
    out = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/")
            step = int(s)
        if part == "*":
            a, b = lo, hi
        elif "-" in part:
            a, b = map(int, part.split("-"))
        else:
            a = b = int(part)
        out.update(range(a, b + 1, step))
    return sorted(out)


def _dow_text(days: Optional[List[int]]) -> str:
    if days is None:
        return "毎日"
    days = sorted({d % 7 for d in days})
    if days == [1, 2, 3, 4, 5]:
        return "月〜金"
    if days == [2, 3, 4, 5, 6]:
        return "火〜土"
    return "・".join(DOW[d] for d in days)


def cron_text(expr: str) -> Tuple[str, str]:
    """cron式 → (UTCの説明, JSTの説明)。日・月の指定はそのまま添える。"""
    mi, hr, dom, mon, dow = expr.split()
    mins = _expand(mi, 0, 59)
    hrs = _expand(hr, 0, 23)
    dows = _expand(dow, 0, 6)
    extra = ("" if dom == "*" else f"（{dom.replace('-', '〜')}日）") + ("" if mon == "*" else f"（{mon}月）")
    if mins is None or hrs is None:
        return f"`{expr}`", f"`{expr}`（UTC+9）"
    utc_times, jst_by_shift = [], {0: [], 1: []}
    for h in hrs:
        for m in mins:
            utc_times.append(f"{h:02d}:{m:02d}")
            jh = h + 9
            jst_by_shift[1 if jh >= 24 else 0].append(f"{jh % 24:02d}:{m:02d}")
    utc = f"{_dow_text(dows)}{extra} {'・'.join(utc_times)}"
    parts = []
    for shift, times in jst_by_shift.items():
        if not times:
            continue
        jd = None if dows is None else [d + shift for d in dows]
        parts.append(f"{_dow_text(jd)}{extra}{'（翌日）' if shift and dows is None else ''} {'・'.join(times)}")
    return utc, " ／ ".join(parts)


def load_workflows(wf_dir: str = WF_DIR) -> List[Dict[str, Any]]:
    out = []
    for p in sorted(glob.glob(os.path.join(wf_dir, "*.yml"))):
        raw = open(p, encoding="utf-8").read()
        d = yaml.safe_load(raw) or {}
        on = d.get(True, d.get("on")) or {}
        if isinstance(on, str):
            on = {on: None}
        wf = {"file": os.path.basename(p), "name": d.get("name") or os.path.basename(p), "crons": [], "after": [],
              "after_types": [], "dispatch_inputs": None, "push": None, "concurrency": d.get("concurrency"),
              "job_if": [], "guard_steps": [], "outputs": []}
        for k, v in on.items():
            if k == "schedule":
                wf["crons"] = [x["cron"] for x in v]
            elif k == "workflow_run":
                wf["after"] = list(v.get("workflows") or [])
                wf["after_types"] = list(v.get("types") or [])
            elif k == "workflow_dispatch":
                wf["dispatch_inputs"] = list(((v or {}).get("inputs") or {}).keys())
            elif k == "push":
                wf["push"] = v
        for j in (d.get("jobs") or {}).values():
            if j.get("if"):
                wf["job_if"].append(" ".join(str(j["if"]).split()))
            for s in j.get("steps") or []:
                if "guard" in (s.get("name") or "").lower() or "cancel this run" in (s.get("name") or "").lower():
                    wf["guard_steps"].append(s.get("name"))
        wf["outputs"] = sorted(set(re.findall(r"git add ([^\s|;&]+)", raw)))
        out.append(wf)
    by_name = {w["name"]: w for w in out}
    for w in out:
        w["downstream"] = sorted(x["file"] for x in out if w["name"] in x["after"])
        w["after_files"] = [by_name[n]["file"] if n in by_name else f"（不明: {n}）" for n in w["after"]]
    return out


# ─────────────────────────────────────────────────────────
#  文書の節
# ─────────────────────────────────────────────────────────

def _if_text(expr: str) -> str:
    if expr == "github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success'":
        return "起動元が成功したときだけ動く（手動・cronは常に動く）"
    if "Stonks Silo Update" in expr and "failure" in expr:
        return "起動元が成功したときだけ動く。Stonks Silo Updateは失敗でも動く（手動・cronは常に動く）"
    return f"`{expr}`"


def render_section(wfs: List[Dict[str, Any]]) -> str:
    L = [BEGIN, "", f"全{len(wfs)}本（.github/workflows/）。時刻はcronの指定（GitHubの混雑で遅れて起動することがある）。", ""]
    L.append("| ワークフロー | 起動（cron） UTC | 起動（cron） JST | 起動元（workflow_run、完了で起動） | 手動の入力 | 同時実行 | ガード・動く条件 | 出力先（commitするパス） | 下流 |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for w in wfs:
        utc = "<br>".join(cron_text(c)[0] for c in w["crons"]) or "—"
        jst = "<br>".join(cron_text(c)[1] for c in w["crons"]) or "—"
        after = "<br>".join(f"{f}" for f in w["after_files"]) or "—"
        if w["push"]:
            after = (after + "<br>" if after != "—" else "") + "push: " + "・".join((w["push"] or {}).get("paths") or [])
        di = w["dispatch_inputs"]
        inputs = "—（手動実行なし）" if di is None else ("・".join(di) if di else "あり（入力なし）")
        conc = (w["concurrency"] or {}).get("group") if isinstance(w["concurrency"], dict) else (w["concurrency"] or "—")
        guards = [_if_text(x) for x in w["job_if"]] + [f"段「{s}」" for s in w["guard_steps"]]
        outs = "<br>".join(f"`{o}`" for o in w["outputs"]) or "（commitしない）"
        down = "<br>".join(w["downstream"]) or "—"
        L.append(f"| `{w['file']}`<br>{w['name']} | {utc} | {jst} | {after} | {inputs} | {conc or '—'} | {'<br>'.join(guards) or '—'} | {outs} | {down} |")
    L += ["", END]
    return "\n".join(L)


def apply_section(doc_text: str, section: str) -> str:
    i, j = doc_text.find(BEGIN), doc_text.find(END)
    if i < 0 or j < 0:
        raise ValueError("UPDATE_SCHEDULE.mdに生成部分の目印（BEGIN/END）が無い")
    return doc_text[:i] + section + doc_text[j + len(END):]


# ─────────────────────────────────────────────────────────
#  config/workflow_dependencies.json
# ─────────────────────────────────────────────────────────

def generated_deps(wfs: List[Dict[str, Any]], deps: Dict[str, Any]) -> Dict[str, Any]:
    """YAMLから決まる項目を差し替えたdepsを返す（キー・label・順序の定義はそのまま）。"""
    by_file = {w["file"]: w for w in wfs}
    key_by_file = {v.get("yml"): k for k, v in (deps.get("workflows") or {}).items()}
    out = json.loads(json.dumps(deps))
    out.pop("known_issues", None)   # 古い時刻の記述（UPDATE_SCHEDULE.mdの変更履歴へ移した、2026-09-30）
    out["_generated"] = ("workflows.*のdepends_on・outputsはscripts/gen_update_schedule.pyが.github/workflows/*.ymlから生成する。"
                         "label・yml・accepts_tickers・input_param・bulk_update_*・new_ticker_orderはadmin.htmlの一括更新用の手作業の定義"
                         "（accepts_tickers・input_paramはYAMLの手動実行の入力と合っているかを確認する）。"
                         "起動時刻・連鎖の説明はdocs/architecture/UPDATE_SCHEDULE.md")
    for k, v in (out.get("workflows") or {}).items():
        w = by_file.get(v.get("yml"))
        if w is None:
            continue
        v["depends_on"] = [key_by_file.get(f, f) for f in w["after_files"]]
        v["outputs"] = w["outputs"]
    return out


def curated_input_problems(wfs: List[Dict[str, Any]], deps: Dict[str, Any]) -> List[str]:
    """admin.htmlが使う手作業の定義（accepts_tickers・input_param）が、YAMLの手動実行の入力と合っているか。
    accepts_tickers=trueなのにinput_paramがworkflow_dispatchの入力に無い場合を返す（値は書き換えない）。"""
    by_file = {w["file"]: w for w in wfs}
    out = []
    for k, v in (deps.get("workflows") or {}).items():
        w = by_file.get(v.get("yml"))
        if w is None:
            out.append(f"{k}: yml {v.get('yml')} が.github/workflows/に無い")
        elif v.get("accepts_tickers") is not False and v.get("input_param", "tickers") not in (w["dispatch_inputs"] or []):
            out.append(f"{k}: input_param {v.get('input_param', 'tickers')} が{w['file']}の手動実行の入力に無い")
    return out


def _dump(d: Dict[str, Any]) -> str:
    return json.dumps(d, ensure_ascii=False, indent=2) + "\n"


def check_drift(repo: str = REPO) -> List[str]:
    """YAMLから生成した内容と、文書・JSONの差分（WARN用のメッセージ）。"""
    wfs = load_workflows(os.path.join(repo, ".github", "workflows"))
    msgs = []
    doc = os.path.join(repo, "docs", "architecture", "UPDATE_SCHEDULE.md")
    try:
        text = open(doc, encoding="utf-8").read().replace("\r\n", "\n")
        if apply_section(text, render_section(wfs)) != text:
            msgs.append("UPDATE_SCHEDULE.mdのワークフロー一覧が.github/workflows/*.ymlと一致しない")
    except (OSError, ValueError) as e:
        msgs.append(f"UPDATE_SCHEDULE.mdを確認できない（{e}）")
    dp = os.path.join(repo, "config", "workflow_dependencies.json")
    try:
        cur = json.load(open(dp, encoding="utf-8"))
        if generated_deps(wfs, cur) != cur:
            msgs.append("config/workflow_dependencies.jsonのYAML由来の項目（depends_on・outputs）が.github/workflows/*.ymlと一致しない")
        msgs += [f"config/workflow_dependencies.json: {m}" for m in curated_input_problems(wfs, cur)]
    except (OSError, ValueError) as e:
        msgs.append(f"workflow_dependencies.jsonを確認できない（{e}）")
    return msgs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    if args.check:
        msgs = check_drift()
        for m in msgs:
            print("[DRIFT] " + m)
        print("一致" if not msgs else f"差分{len(msgs)}件（python scripts/gen_update_schedule.py で更新する）")
        sys.exit(1 if msgs else 0)
    wfs = load_workflows()
    text = open(DOC, encoding="utf-8").read()
    nl = "\r\n" if "\r\n" in text else "\n"
    new = apply_section(text.replace("\r\n", "\n"), render_section(wfs))
    open(DOC, "w", encoding="utf-8", newline="").write(new.replace("\n", nl))
    deps = json.load(open(DEPS, encoding="utf-8"))
    open(DEPS, "w", encoding="utf-8", newline="\n").write(_dump(generated_deps(wfs, deps)))
    print(f"生成しました: {DOC} の一覧（{len(wfs)}本）、{DEPS}")


if __name__ == "__main__":
    main()
