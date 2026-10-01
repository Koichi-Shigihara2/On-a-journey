"""
tests/test_push_with_retry.py

2026-10-01: botがpushする全ワークフローは .github/scripts/push_with_retry.sh を通して pushする
（pull --rebase と push を最大3回、間隔を空けて繰り返す）。2026-09-30 23:57、Market Pulseのpushが
Stonks Siloのpull --rebaseとpushの間に入り、Stonks Siloのpushが拒否されて結果が保存されなかった。
"""

import glob
import os
import shutil
import subprocess

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_WF_DIR = os.path.join(_REPO, ".github", "workflows")
_SCRIPT = os.path.join(_REPO, ".github", "scripts", "push_with_retry.sh")


def test_every_workflow_pushes_through_the_retry_script():
    problems = []
    n_commit = 0
    for path in sorted(glob.glob(os.path.join(_WF_DIR, "*.yml"))):
        with open(path, encoding="utf-8") as f:
            text = f.read()
        name = os.path.basename(path)
        if "git push" in text:
            problems.append(f"{name}: git pushを直接実行している")
        if "git commit" in text:
            n_commit += 1
            if "bash .github/scripts/push_with_retry.sh" not in text:
                problems.append(f"{name}: commitしているがpush_with_retry.shを呼んでいない")
    assert problems == []
    assert n_commit >= 20


def _git_bash():
    """Git for Windowsのbash（WindowsのSystem32\\bash.exeはWSLなので使わない）。他のOSはPATHのbash。"""
    if os.name != "nt":
        return shutil.which("bash")
    exec_path = subprocess.run(["git", "--exec-path"], capture_output=True, text=True).stdout.strip()
    cand = os.path.normpath(os.path.join(exec_path, "..", "..", "..", "bin", "bash.exe"))
    return cand if os.path.exists(cand) else None


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout


def _setup(tmp_path):
    remote = tmp_path / "remote.git"
    _git(tmp_path, "init", "--bare", "-b", "kaihatsu", str(remote))
    clones = []
    for name in ("a", "b"):
        d = tmp_path / name
        _git(tmp_path, "clone", "-q", str(remote), str(d))
        _git(d, "config", "user.name", "t")
        _git(d, "config", "user.email", "t@example.com")
        _git(d, "checkout", "-q", "-B", "kaihatsu")
        clones.append(d)
    a, b = clones
    (a / "base.txt").write_text("base\n")
    _git(a, "add", ".")
    _git(a, "commit", "-q", "-m", "base")
    _git(a, "push", "-q", "origin", "kaihatsu")
    _git(b, "pull", "-q", "origin", "kaihatsu")
    return a, b


def _run_script(bash, cwd):
    env = dict(os.environ, PUSH_RETRY_WAIT="0")
    return subprocess.run([bash, _SCRIPT, "kaihatsu"], cwd=cwd, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", env=env)


@pytest.fixture
def bash():
    b = _git_bash()
    if not b:
        pytest.skip("bashが無い")
    return b


def test_push_succeeds_after_remote_moved(tmp_path, bash):
    a, b = _setup(tmp_path)
    (b / "other.txt").write_text("from b\n")
    _git(b, "add", ".")
    _git(b, "commit", "-q", "-m", "b")
    _git(b, "push", "-q", "origin", "kaihatsu")
    (a / "mine.txt").write_text("from a\n")
    _git(a, "add", ".")
    _git(a, "commit", "-q", "-m", "a")
    r = _run_script(bash, a)
    assert r.returncode == 0, r.stdout + r.stderr
    log = _git(tmp_path / "remote.git", "log", "--format=%s", "kaihatsu").split()
    assert log[:3] == ["a", "b", "base"]


def test_gives_up_after_three_attempts_and_aborts_rebase(tmp_path, bash):
    a, b = _setup(tmp_path)
    (b / "base.txt").write_text("from b\n")
    _git(b, "commit", "-q", "-am", "b")
    _git(b, "push", "-q", "origin", "kaihatsu")
    (a / "base.txt").write_text("from a\n")
    _git(a, "commit", "-q", "-am", "a")
    r = _run_script(bash, a)
    assert r.returncode == 1
    assert r.stdout.count("回目のpull --rebase/pushが失敗") == 2
    assert "3回試してpushできませんでした" in r.stdout
    assert not (a / ".git" / "rebase-merge").exists() and not (a / ".git" / "rebase-apply").exists()
    assert _git(a, "log", "-1", "--format=%s").strip() == "a"
