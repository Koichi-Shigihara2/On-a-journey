"""見出しのファイルの push の衝突のやり直し（headlines_push.py、[[MARKETPULSE-MOVE-POSTMORTEM-EPIC-1]] ①、2026-11-01）の試験。

本物の git で、一時のフォルダにリモート（bare）と2つの作業コピー（6時間ごとの回・夜の回）を作る。外部への通信は無い。
- リモートに先に別の回の積み足しがある状態から、やり直して両方の記事が残る
- 夜の回の title_ja・market が残る。夜の回が書くほかのファイル（market_data.json）も残る
- 文字の合わせ込みで見出しのファイルが壊れたら、やり直す
- 3回とも失敗したら終了コード1（黙って捨てない）
- 新しい変更が無ければ commit・push しない
"""
import json
import os
import shutil
import subprocess
import sys

import pytest

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_MARKET_PULSE_DIR = os.path.join(_REPO_ROOT, "src", "market", "market_pulse")
if _MARKET_PULSE_DIR not in sys.path:
    sys.path.insert(0, _MARKET_PULSE_DIR)

import headlines_push as hp  # noqa: E402
import headlines_store as hs  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git が無い")

REL = hp.HEADLINES_REL
DATE = "2026-11-02"
SRC = {"CNBC Earnings": "ok", "Yahoo Finance S&P 500": "ok", "Google News ビジネス（US）": "ok"}


def git(cwd, *args):
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, encoding="utf-8")
    assert r.returncode == 0, (args, r.stdout, r.stderr)
    return r.stdout


def _it(title, link, pub, source="CNBC Earnings"):
    return {"title": title, "link": link, "published_utc": pub, "source": source, "region": "米国"}


def _clone(tmp_path, remote, name):
    d = tmp_path / name
    git(tmp_path, "clone", "-q", "-c", "core.autocrlf=false", "-b", "kaihatsu", str(remote), name)
    for k, v in (("user.name", "t"), ("user.email", "t@example.com"), ("core.autocrlf", "false"), ("pull.rebase", "true")):
        git(d, "config", k, v)
    return d


@pytest.fixture
def repos(tmp_path):
    """リモートと、最初の見出しのファイル（1件）を入れた2つの作業コピー（a: 6時間ごとの回、b: 夜の回）。"""
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "-q", "--bare", "-b", "kaihatsu", str(remote))
    seed = tmp_path / "seed"
    git(tmp_path, "init", "-q", "-b", "kaihatsu", "seed")
    for k, v in (("user.name", "t"), ("user.email", "t@example.com"), ("core.autocrlf", "false")):
        git(seed, "config", k, v)
    git(seed, "remote", "add", "origin", str(remote))
    hs.accumulate([_it("Seed", "https://www.cnbc.com/seed.html", "2026-11-02T10:00:00Z")], str(seed / REL),
                  fetched_at="2026-11-02T12:40:00Z", run="scheduled", sources=SRC)
    (seed / "docs/market-monitor/market-pulse/data/market_data.json").write_text("[1]\n", encoding="utf-8")
    git(seed, "add", "-A")
    git(seed, "commit", "-q", "-m", "seed")
    git(seed, "push", "-q", "origin", "kaihatsu")
    return remote, _clone(tmp_path, remote, "a"), _clone(tmp_path, remote, "b")


def _run_store(repo, items, fetched_at, run, pending, annotations=None):
    """その回の積み足しと、一時のファイル（pending）への入力の保存（store_from_fetch と同じ形）。"""
    h = {"all_items": items, "fetched_at": fetched_at, "failed_reasons": {}, "sources": list(SRC),
         "items": [dict(x, title_ja=(annotations or {}).get(x["link"], (None, None))[0],
                        market=(annotations or {}).get(x["link"], (None, None))[1]) for x in items]}
    return hs.store_from_fetch(h, run, annotate=annotations is not None, data_dir=str(repo / REL), pending_path=str(pending))


def _remote_doc(tmp_path, remote, date=DATE):
    n = 0
    while (tmp_path / f"check{n}").exists():   # Windows では git の objects が読み取り専用で消しにくいので、毎回別の名前にする
        n += 1
    check = tmp_path / f"check{n}"
    git(tmp_path, "clone", "-q", "-c", "core.autocrlf=false", "-b", "kaihatsu", str(remote), check.name)
    with open(check / REL / f"{date}.json", encoding="utf-8") as f:
        return json.load(f), check


def test_conflict_rebuilds_and_keeps_both_runs(tmp_path, repos):
    remote, a, b = repos
    # a（6時間ごとの回）が先に積み足して push
    _run_store(a, [_it("From A", "https://e.com/a?.tsrc=rss", "2026-11-02T17:00:00Z", "Yahoo Finance S&P 500")],
               "2026-11-02T18:40:00Z", "scheduled", tmp_path / "pa.json")
    assert hp.push(str(a), "kaihatsu", [REL + "/"], "chore: a", str(tmp_path / "pa.json"), wait=0) == 0
    # b（夜の回）は古い版の上で、同じファイルに積み足し・訳と判定・market_data.json も書く
    ann = {"https://www.cnbc.com/b.html": ("Bの訳", True)}
    _run_store(b, [_it("From B", "https://www.cnbc.com/b.html", "2026-11-02T20:00:00Z")], "2026-11-02T21:30:00Z", "evening",
               tmp_path / "pb.json", annotations=ann)
    (b / "docs/market-monitor/market-pulse/data/market_data.json").write_text("[1, 2]\n", encoding="utf-8")
    rc = hp.push(str(b), "kaihatsu", ["docs/market-monitor/market-pulse/data/"], "chore: b", str(tmp_path / "pb.json"), wait=0)
    assert rc == 0
    doc, check = _remote_doc(tmp_path, remote)
    by = {x["title"]: x for x in doc["items"]}
    assert set(by) == {"Seed", "From A", "From B"}                       # 両方の回の記事が残る
    assert by["From B"]["title_ja"] == "Bの訳" and by["From B"]["market"] is True   # 夜の回の訳・判定が残る
    assert [f["run"] for f in doc["fetches"]] == ["scheduled", "scheduled", "evening"]
    assert (check / "docs/market-monitor/market-pulse/data/market_data.json").read_text(encoding="utf-8") == "[1, 2]\n"
    assert hp.headlines_problems(str(check)) == []
    assert git(b, "status", "--porcelain").strip() == ""                 # 作業コピーもきれい


def test_evening_annotations_fill_items_already_pushed_by_other_run(tmp_path, repos):
    # 同じ記事を a が訳なしで先に push し、b（夜の回）が訳・判定を持って後から来る → リモートの記事に訳・判定が写る
    remote, a, b = repos
    item = _it("Same", "https://www.cnbc.com/same.html", "2026-11-02T19:00:00Z")
    _run_store(a, [item], "2026-11-02T19:40:00Z", "scheduled", tmp_path / "pa.json")
    assert hp.push(str(a), "kaihatsu", [REL + "/"], "chore: a", str(tmp_path / "pa.json"), wait=0) == 0
    _run_store(b, [item], "2026-11-02T21:30:00Z", "evening", tmp_path / "pb.json",
               annotations={item["link"]: ("同じ記事の訳", False)})
    assert hp.push(str(b), "kaihatsu", ["docs/market-monitor/market-pulse/data/"], "chore: b", str(tmp_path / "pb.json"), wait=0) == 0
    doc, _ = _remote_doc(tmp_path, remote)
    (x,) = [i for i in doc["items"] if i["title"] == "Same"]
    assert x["title_ja"] == "同じ記事の訳" and x["market"] is False and x["first_seen"] == "2026-11-02T19:40:00Z"


def test_new_date_file_only_in_local_is_rebuilt(tmp_path, repos):
    # b だけが新しい日付のファイルを作っていて、a の push で衝突した場合も、b の分は積み足し直される
    remote, a, b = repos
    _run_store(a, [_it("A2", "https://e.com/a2", "2026-11-02T17:00:00Z")], "2026-11-02T18:40:00Z", "scheduled", tmp_path / "pa.json")
    assert hp.push(str(a), "kaihatsu", [REL + "/"], "chore: a", str(tmp_path / "pa.json"), wait=0) == 0
    _run_store(b, [_it("B2", "https://e.com/b2", "2026-11-02T20:00:00Z"), _it("B3", "https://e.com/b3", "2026-11-03T14:00:00Z")],
               "2026-11-03T21:30:00Z", "evening", tmp_path / "pb.json")
    assert hp.push(str(b), "kaihatsu", ["docs/market-monitor/market-pulse/data/"], "chore: b", str(tmp_path / "pb.json"), wait=0) == 0
    doc, _ = _remote_doc(tmp_path, remote)
    assert {x["title"] for x in doc["items"]} == {"Seed", "A2", "B2"}
    doc3, _ = _remote_doc(tmp_path, remote, "2026-11-03")
    assert [x["title"] for x in doc3["items"]] == ["B3"] and doc3["fetches"][0]["run"] == "evening"


def test_broken_json_after_clean_rebase_is_rebuilt(tmp_path, repos, monkeypatch):
    # rebase が文字の上では成功しても、見出しのファイルが壊れていればやり直す
    remote, a, b = repos
    _run_store(b, [_it("B", "https://e.com/b", "2026-11-02T20:00:00Z")], "2026-11-02T21:30:00Z", "evening", tmp_path / "pb.json")
    calls = []
    real = hp.headlines_problems

    def probs(repo, rel=REL):
        calls.append(1)
        return ["2026-11-02.json: JSONDecodeError"] if len(calls) == 1 else real(repo, rel)
    monkeypatch.setattr(hp, "headlines_problems", probs)
    assert hp.push(str(b), "kaihatsu", ["docs/market-monitor/market-pulse/data/"], "chore: b", str(tmp_path / "pb.json"), wait=0) == 0
    doc, _ = _remote_doc(tmp_path, remote)
    assert {x["title"] for x in doc["items"]} == {"Seed", "B"}


def test_headlines_problems_detects_bad_json_and_duplicates(tmp_path):
    d = tmp_path / REL
    d.mkdir(parents=True)
    (d / "2026-11-01.json").write_text("{", encoding="utf-8")
    (d / "2026-11-02.json").write_text(json.dumps({"items": [{"link_norm": "x"}, {"link_norm": "x"}]}), encoding="utf-8")
    (d / "2026-11-03.json").write_text(json.dumps({"items": [{"link_norm": "x"}]}), encoding="utf-8")
    assert hp.headlines_problems(str(tmp_path)) == ["2026-11-01.json: JSONDecodeError", "2026-11-02.json: 同じリンクが1件"]


def test_three_failures_exit_1_and_keep_local_commit(tmp_path, repos):
    remote, a, b = repos
    _run_store(b, [_it("B", "https://e.com/b", "2026-11-02T20:00:00Z")], "2026-11-02T21:30:00Z", "evening", tmp_path / "pb.json")
    git(b, "remote", "set-url", "--push", "origin", str(tmp_path / "no_such_remote.git"))   # push だけが失敗する
    rc = hp.push(str(b), "kaihatsu", ["docs/market-monitor/market-pulse/data/"], "chore: b", str(tmp_path / "pb.json"), wait=0)
    assert rc == 1
    assert "chore: b" in git(b, "log", "-1", "--format=%s")              # 積み足しは捨てていない（commit が残る）
    with open(b / REL / f"{DATE}.json", encoding="utf-8") as f:
        assert {x["title"] for x in json.load(f)["items"]} == {"Seed", "B"}


def test_nothing_new_does_not_commit_or_push(tmp_path, repos):
    remote, a, b = repos
    before = git(a, "rev-parse", "HEAD")
    assert hp.push(str(a), "kaihatsu", [REL + "/"], "chore: a", None, wait=0) == 0
    assert git(a, "rev-parse", "HEAD") == before
    assert git(tmp_path, "--git-dir", str(remote), "rev-parse", "kaihatsu") == before


def test_store_from_fetch_writes_pending_before_accumulate(tmp_path, monkeypatch):
    p = tmp_path / "pending.json"
    h = {"all_items": [_it("A", "https://e.com/a", "2026-11-02T20:00:00Z")], "fetched_at": "2026-11-02T21:30:00Z",
         "failed_reasons": {"Yahoo Finance S&P 500": "stale"}, "sources": list(SRC),
         "items": [dict(_it("A", "https://e.com/a", "2026-11-02T20:00:00Z"), title_ja="訳", market=True)]}
    p = tmp_path / hs.PENDING_NAME
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))          # GitHub Actions の一時のフォルダ
    hs.store_from_fetch(h, "evening", annotate=True, data_dir=str(tmp_path / "d"))
    got = hs.load_pending()                                    # 省くと ${RUNNER_TEMP}/headlines_pending.json
    assert got["run"] == "evening" and got["fetched_at"] == "2026-11-02T21:30:00Z" and len(got["items"]) == 1
    assert got["sources"]["Yahoo Finance S&P 500"] == "stale"
    assert got["annotations"] == {"https://e.com/a": {"title_ja": "訳", "market": True}}
    monkeypatch.delenv("RUNNER_TEMP")
    p.unlink()
    hs.store_from_fetch(h, "evening", data_dir=str(tmp_path / "d2"))   # 手元（RUNNER_TEMP なし）では残さない
    assert not p.exists() and hs.load_pending() is None
    assert not os.path.exists(hs.PENDING_NAME)                         # 今のフォルダにも書かない


def test_gitattributes_headlines_not_merge_ours():
    with open(os.path.join(_REPO_ROOT, ".gitattributes"), encoding="utf-8") as f:
        lines = [l.split() for l in f if l.strip() and not l.startswith("#")]
    rows = {l[0]: l[1:] for l in lines}
    assert rows["docs/market-monitor/market-pulse/data/headlines/*.json"] == ["text", "eol=lf"]
    assert "merge=ours" in rows["docs/market-monitor/market-pulse/data/market_data.json"]   # ほかは変えない


def test_workflows_use_headlines_push_with_pending():
    for wf, add in (("Market_Pulse_Headlines.yml", "docs/market-monitor/market-pulse/data/headlines/"),
                    ("Market_Pulse_Update.yml", "docs/market-monitor/market-pulse/data/")):
        with open(os.path.join(_REPO_ROOT, ".github", "workflows", wf), encoding="utf-8") as f:
            t = f.read()
        assert "headlines_push.py" in t and f"--add {add}" in t and "push_with_retry.sh" not in t, wf
        assert f"git add {add}" in t, wf          # scripts/gen_update_schedule.py が出力のパスを読む
