"""見出しのファイルを含む commit を push する（[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]] ①、2026-11-01）。

6時間ごとの`Market_Pulse_Headlines.yml`と夜の`Market_Pulse_Update.yml`は、どちらも
`docs/market-monitor/market-pulse/data/headlines/YYYY-MM-DD.json`に積み足す。見出しのファイルは merge=ours を使わない
（片方の回の積み足しを黙って捨てないため）。push は次の手順で最大3回試す:

1. `git pull --rebase`。成功して、見出しのファイルがJSONとして読め、同じファイルに同じリンク（link_norm）が2件無ければ push
2. rebase が衝突した（または文字の合わせ込みで見出しのファイルが壊れた）ら:
   rebase をやめ、HEAD をリモートの先頭に合わせ（作業中のファイルは残す）、見出しのファイルだけリモートの版に戻し、
   この回の入力（headlines_store.save_pending が ${RUNNER_TEMP}/headlines_pending.json に残した items・訳と判定）で accumulate をやり直し、
   この回が書くパス（--add）を commit し直して push
3. 3回とも失敗したら終了コード1（job を失敗にする。System Health の [J] が失敗として数える）

ほかのデータのファイル（market_data.json 等）は今までどおり .gitattributes の merge=ours のまま。
やり直しの手順2では、この回が書くパス（--add）は作業中のファイル（この回の版）を使う（merge=ours と同じ扱い）。

使い方（ワークフローの最後の段）:
  python src/market/market_pulse/headlines_push.py --add docs/market-monitor/market-pulse/data/headlines/ --message "chore: ..."
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import headlines_store as hs  # noqa: E402

HEADLINES_REL = "docs/market-monitor/market-pulse/data/headlines"


def _git(repo: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, encoding="utf-8")


def _staged(repo: str) -> bool:
    return _git(repo, "diff", "--cached", "--quiet").returncode == 1


def headlines_problems(repo: str, rel: str = HEADLINES_REL) -> List[str]:
    """見出しのファイルの壊れ（JSONとして読めない・同じファイルに同じ link_norm が2件）。文字の合わせ込みの後に確かめる。"""
    d = os.path.join(repo, rel)
    out = []
    if not os.path.isdir(d):
        return out
    for n in sorted(os.listdir(d)):
        if not n.endswith(".json"):
            continue
        try:
            with open(os.path.join(d, n), encoding="utf-8") as f:
                doc = json.load(f)
            links = [x["link_norm"] for x in doc.get("items") or []]
        except Exception as e:
            out.append(f"{n}: {type(e).__name__}")
            continue
        if len(links) != len(set(links)):
            out.append(f"{n}: 同じリンクが{len(links) - len(set(links))}件")
    return out


def rebuild(repo: str, branch: str, pending: Optional[dict], add: List[str], message: str,
            rel: str = HEADLINES_REL) -> None:
    """HEAD をリモートの先頭に合わせ、見出しのファイルをリモートの版に戻してから、この回の入力で積み足し直して commit する。"""
    _git(repo, "rebase", "--abort")
    r = _git(repo, "fetch", "origin", branch)
    if r.returncode != 0:
        raise RuntimeError(f"git fetch に失敗: {r.stderr.strip()}")
    remote = f"origin/{branch}"
    r = _git(repo, "reset", "--mixed", remote)      # HEAD・索引はリモート、作業中のファイルはこの回の版のまま
    if r.returncode != 0:
        raise RuntimeError(f"git reset に失敗: {r.stderr.strip()}")
    # 見出しのファイルだけリモートの版に戻す（リモートに無いファイルは消す。この回の分は下で積み足し直す）
    remote_files = set(_git(repo, "ls-tree", "-r", "--name-only", remote, "--", rel).stdout.split())
    d = os.path.join(repo, rel)
    if os.path.isdir(d):
        for n in os.listdir(d):
            p = f"{rel}/{n}"
            if p not in remote_files and os.path.isfile(os.path.join(d, n)):
                os.remove(os.path.join(d, n))
    if remote_files:
        r = _git(repo, "checkout", remote, "--", rel)
        if r.returncode != 0:
            raise RuntimeError(f"見出しのファイルをリモートの版に戻せない: {r.stderr.strip()}")
    if pending:
        res = hs.accumulate(pending["items"], d, fetched_at=pending["fetched_at"], run=pending["run"],
                            sources=pending["sources"], annotations=pending.get("annotations"))
        print(f"[headlines_push] 積み足し直し: 新しい記事 {res['new']}件・変更 {res['updated']}件・書いたファイル {res['written']}")
    _git(repo, "add", "-A", "--", *add)
    if _staged(repo):
        r = _git(repo, "commit", "-m", message)
        if r.returncode != 0:
            raise RuntimeError(f"git commit に失敗: {r.stderr.strip()}")
    else:
        print("[headlines_push] 積み足し直した結果、リモートと同じ（commit しない）")
    _git(repo, "checkout", "--", ".")               # この回が書かないファイルは、作業中もリモートの版にそろえる


def push(repo: str, branch: str, add: List[str], message: str, pending_path: Optional[str] = None,
         attempts: int = 3, wait: float = 15, rel: str = HEADLINES_REL) -> int:
    _git(repo, "add", "-A", "--", *add)
    if _staged(repo):
        r = _git(repo, "commit", "-m", message)
        if r.returncode != 0:
            print(f"::error::[headlines_push] git commit に失敗: {r.stderr.strip()}")
            return 1
    elif _git(repo, "rev-list", "--count", f"origin/{branch}..HEAD").stdout.strip() in ("", "0"):
        print("[headlines_push] 新しい変更が無い（commit・push しない）")
        return 0
    pending = hs.load_pending(pending_path)   # 省くと ${RUNNER_TEMP}/headlines_pending.json。無ければ None（見出しはリモートの版のまま）
    for i in range(1, attempts + 1):
        pr = _git(repo, "pull", "--rebase", "origin", branch)
        probs = headlines_problems(repo, rel) if pr.returncode == 0 else []
        try:
            if pr.returncode != 0 or probs:
                why = f"見出しのファイルが壊れた（{', '.join(probs)}）" if probs else "pull --rebase が衝突"
                print(f"[headlines_push] {i}回目: {why}。見出しのファイルをリモートの版に戻して積み足し直す")
                rebuild(repo, branch, pending, add, message, rel)
        except Exception as e:
            print(f"[headlines_push] {i}回目: やり直しに失敗（{e}）")
        else:
            ps = _git(repo, "push", "origin", branch)
            if ps.returncode == 0:
                if i > 1:
                    print(f"[headlines_push] {i}回目で成功")
                return 0
            print(f"[headlines_push] {i}回目: push が失敗（{ps.stderr.strip()[:200]}）")
        if i < attempts:
            time.sleep(wait * i)
    print(f"::error::[headlines_push] {attempts}回試して push できなかった（{branch}）。見出しの積み足しは捨てていない（job を失敗にする）")
    return 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--add", action="append", required=True, help="この回が書くパス（git add する。複数可）")
    ap.add_argument("--message", required=True)
    ap.add_argument("--pending", default="", help="この回の入力の一時のファイル（省くと ${RUNNER_TEMP}/headlines_pending.json）")
    ap.add_argument("--branch", default="kaihatsu")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--attempts", type=int, default=3)
    ap.add_argument("--wait", type=float, default=15)
    a = ap.parse_args(argv)
    return push(os.path.abspath(a.repo), a.branch, a.add, a.message, a.pending or None, a.attempts, a.wait)


if __name__ == "__main__":
    sys.exit(main())
