"""試算の共通の道具（計算方法の変更の前後を、全銘柄で、公開のデータに触れずに比べる）。

これまで件ごとに scratchpad で作っていた harness.py（TANUKIを出力先を差し替えて回す）・compare.py（前後の表）・
method_change_scope の呼び出しを1つにまとめた。値の計算は変えない（TANUKI VALUATION・HypeCoreのコードをそのまま呼ぶ）。

  python tools/sim/sim.py run   NAME [--root DIR] [--hook FILE ...]           TANUKI VALUATION を NAME の出力先で全銘柄回す
  python tools/sim/sim.py chain NAME [--root DIR] [--hook FILE ...]           TANUKI → HypeCore → TANUKI（読み合い。公開のとおりの順）
  python tools/sim/sim.py compare BEFORE AFTER [--root DIR] [--top N]        変化の表（変化の大きい順・判定の変化は全件・保有銘柄は別）
  python tools/sim/sim.py scope --id ID BEFORE AFTER [--root DIR] [--write]   method_change.json の scope を作る（手で書かない）

決まり（安全）:
  - Grok・SEC・Discord は呼ばない（XAI_API_KEY・DISCORD_WEB_HOOK を外し、SECの取得は例外にする）。公開のデータ（docs/・src/…/data）は書かない
  - 出力は --root（既定 .sim_out/、gitignore）の下の NAME/ だけ。前後で同じ条件にするため、前のコードは git stash で戻して同じ道具で回す
  - 件ごとの差し替え（一時的な変更の確かめ）は --hook FILE.py の apply(ctx) に書く。ctx は dict(GS=growth_sanity, MC=maturity_config, P=pipeline, HC=hypecore)
"""
from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import math
import os
import shutil
import sys
from typing import Any, Callable, Dict, List, Optional

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_ROOT = os.path.join(REPO, ".sim_out")
TANUKI_SRC = os.path.join(REPO, "src", "value", "tanuki_valuation")
PORTFOLIO_JSON = os.path.join(REPO, "docs", "portfolio", "data", "portfolio.json")
JUDGMENT_KEY = "tanuki_score"
TABLE_KEYS = ("intrinsic_value_per_share", JUDGMENT_KEY, "recommended_g", "upside_percent")


# ───────── 環境（外部を呼ばない） ─────────
def safe_environment() -> None:
    """Grok・Discord を外し、SEC の連絡先は試験用の値にして、SEC の取得は呼ばれたら例外にする"""
    os.environ.pop("XAI_API_KEY", None)
    os.environ.pop("DISCORD_WEB_HOOK", None)
    os.environ["SEC_USER_AGENT_EMAIL"] = "tests@example.invalid"
    os.chdir(REPO)
    for p in (REPO, TANUKI_SRC):
        if p not in sys.path:
            sys.path.insert(0, p)
    from common import external_api as ea

    def _no_sec(*a: Any, **k: Any) -> Any:
        raise ea.ExternalApiError("試算ではSECを呼ばない")

    ea.sec_get = _no_sec


def _load_hooks(paths: List[str], ctx: Dict[str, Any]) -> None:
    for i, path in enumerate(paths or []):
        spec = importlib.util.spec_from_file_location(f"sim_hook_{i}", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.apply(ctx)


def holdings() -> set:
    """保有銘柄（docs/portfolio/data/portfolio.json の全ブローカー・全ポジション）。読めなければ空"""
    try:
        with open(PORTFOLIO_JSON, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return set()
    out: set = set()
    for b in ((data or {}).get("brokers") or {}).values():
        out.update((b.get("positions") or {}).keys())
    return out


# ───────── 回す ─────────
def run_tanuki(out_dir: str, ctx: Dict[str, Any], hype_dir: Optional[str] = None) -> int:
    """TANUKI VALUATION を全銘柄、out_dir に書く。hype_dir を渡すと HypeCore の出力をそこから読む（読み合いの2回目）"""
    P = ctx["P"]
    os.makedirs(out_dir, exist_ok=True)
    pipe = P.TanukiValuationPipeline(use_ai_validation=False)
    pipe.output_dir = out_dir
    real_join = os.path.join
    real_hype = real_join(pipe.repo_root, "docs", "value-monitor", "hypecore", "data")

    def _join(a: str, *parts: str) -> str:
        r = real_join(a, *parts)
        if hype_dir and r.startswith(real_hype):
            return real_join(hype_dir, r[len(real_hype):].lstrip("\\/"))
        return r

    if hype_dir:
        os.path.join = _join   # 公開の HypeCore のデータ置き場の読みだけを差し替える（試算の間だけ）
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            pipe.run(None)
    finally:
        os.path.join = real_join
    return len([d for d in os.listdir(out_dir) if os.path.isdir(os.path.join(out_dir, d))])


def run_hypecore(tanuki_dir: str, out_dir: str, ctx: Dict[str, Any]) -> int:
    """HypeCore を、TANUKI の出力を tanuki_dir から読み、out_dir に書く形で全銘柄回す"""
    from pathlib import Path
    HC = ctx["HC"]
    os.makedirs(out_dir, exist_ok=True)
    HC._TANUKI_DIR = Path(tanuki_dir)
    HC._OUT_DIR = Path(out_dir)
    tickers = sorted(d for d in os.listdir(tanuki_dir) if os.path.isfile(os.path.join(tanuki_dir, d, "latest.json")))
    n = 0
    with contextlib.redirect_stdout(io.StringIO()):
        for t in tickers:
            try:
                HC.run_poc(t)
                n += 1
            except Exception:
                pass
    return n


def _context() -> Dict[str, Any]:
    safe_environment()
    import growth_sanity as GS
    import maturity_config as MC
    import pipeline as P
    from src.value.hypecore import hypecore as HC
    return {"GS": GS, "MC": MC, "P": P, "HC": HC}


def cmd_run(name: str, root: str, hooks: List[str]) -> str:
    ctx = _context()
    _load_hooks(hooks, ctx)
    n = run_tanuki(os.path.join(root, name, "tanuki"), ctx)
    print(f"done {name}: TANUKI {n}銘柄 → {os.path.join(root, name)}")
    return os.path.join(root, name)


def cmd_chain(name: str, root: str, hooks: List[str]) -> str:
    ctx = _context()
    _load_hooks(hooks, ctx)
    base = os.path.join(root, name)
    n1 = run_tanuki(os.path.join(base, "tanuki"), ctx)
    n2 = run_hypecore(os.path.join(base, "tanuki"), os.path.join(base, "hypecore"), ctx)
    n3 = run_tanuki(os.path.join(base, "tanuki2"), ctx, hype_dir=os.path.join(base, "hypecore"))
    print(f"done {name}: TANUKI {n1} → HypeCore {n2} → TANUKI {n3} → {base}（最後の出力は tanuki2/）")
    return base


# ───────── 比べる ─────────
def _latest(root: str, name: str, ticker: str) -> Optional[dict]:
    base = os.path.join(root, name)
    for sub in ("tanuki2", "tanuki", ""):
        p = os.path.join(base, sub, ticker, "latest.json")
        if os.path.isfile(p):
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    return None


def _tickers(root: str, name: str) -> List[str]:
    base = os.path.join(root, name)
    for sub in ("tanuki2", "tanuki", ""):
        d = os.path.join(base, sub)
        if os.path.isdir(d):
            ts = sorted(t for t in os.listdir(d) if os.path.isfile(os.path.join(d, t, "latest.json")))
            if ts:
                return ts
    return []


def compare(root: str, before: str, after: str, hold: Optional[set] = None, top: int = 25) -> Dict[str, Any]:
    """前後の比べ。{rows, changed, judgments, holdings, text}。理論株価の変化の大きい順、判定の変化は全件、保有銘柄は別"""
    hold = holdings() if hold is None else hold
    rows = []
    for t in _tickers(root, before):
        x, y = _latest(root, before, t), _latest(root, after, t)
        if not x or not y:
            continue
        iv0, iv1 = x.get("intrinsic_value_per_share"), y.get("intrinsic_value_per_share")
        pct = (iv1 / iv0 - 1) * 100 if iv0 and iv1 else None
        rows.append({"t": t, "iv0": iv0, "iv1": iv1, "pct": pct, "j0": x.get(JUDGMENT_KEY), "j1": y.get(JUDGMENT_KEY),
                     "g0": x.get("recommended_g"), "g1": y.get("recommended_g"),
                     "tg0": (x.get("components") or {}).get("terminal_growth_used"),
                     "tg1": (y.get("components") or {}).get("terminal_growth_used"),
                     "up0": x.get("upside_percent"), "up1": y.get("upside_percent"), "hold": t in hold})
    changed = [r for r in rows if r["pct"] is not None and abs(r["pct"]) > 0.05]
    judgments = [r for r in rows if r["j0"] != r["j1"]]
    fmt = lambda v: "—" if v is None else (f"{v:.2f}" if isinstance(v, float) else str(v))
    lines = [f"銘柄 {len(rows)}・理論株価が変わる {len(changed)}・判定が変わる {len(judgments)}", "", f"理論株価の変化の大きい順（上位{top}）"]
    for r in sorted(changed, key=lambda r: -abs(r["pct"]))[:top]:
        lines.append(f"| {r['t']}{'（保有）' if r['hold'] else ''} | {fmt(r['iv0'])}→{fmt(r['iv1'])}（{r['pct']:+.1f}%） | "
                     f"推奨g {fmt(r['g0'])}→{fmt(r['g1'])} | 永久g {fmt(r['tg0'])}→{fmt(r['tg1'])} | {r['j0']}→{r['j1']} |")
    lines += ["", "判定の変化（全件）"]
    for r in judgments:
        lines.append(f"| {r['t']}{'（保有）' if r['hold'] else ''} | {r['j0']}→{r['j1']} | 理論株価 {fmt(r['iv0'])}→{fmt(r['iv1'])} | 乖離率 {fmt(r['up0'])}→{fmt(r['up1'])} |")
    lines += ["", "保有銘柄"]
    held = [r for r in rows if r["hold"]]
    for r in held:
        pc = "—" if r["pct"] is None else f"{r['pct']:+.1f}%"
        lines.append(f"| {r['t']} | {fmt(r['iv0'])}→{fmt(r['iv1'])}（{pc}） | {r['j0']}→{r['j1']} |")
    return {"rows": rows, "changed": changed, "judgments": judgments, "holdings": held, "text": "\n".join(lines)}


def cmd_compare(root: str, before: str, after: str, top: int) -> None:
    res = compare(root, before, after, top=top)
    out = os.path.join(root, f"cmp_{before}_{after}.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write(res["text"] + "\n")
    print(res["text"].split("\n")[0], f"→ {out}")


# ───────── scope ─────────
def cmd_scope(change_id: str, root: str, before: str, after: str, write: bool) -> None:
    """method_change_scope.py の snapshot 形（tanuki/{T}.json）を作って diff を呼ぶ"""
    sys.path.insert(0, os.path.join(REPO, "scripts"))
    import method_change_scope as mcs
    snaps = {}
    for label, name in (("before", before), ("after", after)):
        d = os.path.join(root, f"_snap_{label}", "tanuki")
        shutil.rmtree(os.path.dirname(d), ignore_errors=True)
        os.makedirs(d)
        for t in _tickers(root, name):
            shutil.copyfile(os.path.join(root, name, "tanuki2" if os.path.isdir(os.path.join(root, name, "tanuki2")) else "tanuki", t, "latest.json"),
                            os.path.join(d, f"{t}.json"))
        snaps[label] = os.path.dirname(d)
    entry = mcs.make_entry(change_id, snaps["before"], snaps["after"])
    print(mcs.summarize(entry).split("\n")[0])
    if write:
        mcs.write_entry(entry)
        print(f"書き込み: {mcs.CONFIG_PATH}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for c in ("run", "chain"):
        s = sub.add_parser(c)
        s.add_argument("name")
        s.add_argument("--root", default=DEFAULT_ROOT)
        s.add_argument("--hook", action="append", default=[])
    s = sub.add_parser("compare")
    s.add_argument("before"); s.add_argument("after")
    s.add_argument("--root", default=DEFAULT_ROOT); s.add_argument("--top", type=int, default=25)
    s = sub.add_parser("scope")
    s.add_argument("before"); s.add_argument("after"); s.add_argument("--id", required=True)
    s.add_argument("--root", default=DEFAULT_ROOT); s.add_argument("--write", action="store_true")
    a = ap.parse_args(argv)
    if a.cmd == "run":
        cmd_run(a.name, a.root, a.hook)
    elif a.cmd == "chain":
        cmd_chain(a.name, a.root, a.hook)
    elif a.cmd == "compare":
        cmd_compare(a.root, a.before, a.after, a.top)
    else:
        cmd_scope(a.id, a.root, a.before, a.after, a.write)
    return 0


if __name__ == "__main__":
    sys.exit(main())
