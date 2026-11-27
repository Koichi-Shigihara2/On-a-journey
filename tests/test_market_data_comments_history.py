"""[[MARKETPULSE-REVIEW-EPIC-1]] 順3（型S: market_data.json の分割）の回帰テスト（2026-11-27）。

comments_history（各エントリが直近12件のAIコメントを丸ごと複製。ファイルの56%）は、各エントリの summary から作れる。
- 書き手は旧キーを書かない（collect_and_send.py の1か所）
- 移行のスクリプトは、保存済みの履歴が前のエントリの summary から作った履歴と1件ずつ一致するときだけ消す
- 画面（index.html）は前のエントリの summary から作る。旧キーは同じ push で消すので読まない（書き手のないキーを画面が読むと INV-5 が NG になる）
"""
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "market" / "market_pulse"))
import collect_and_send as cs  # noqa: E402

_spec = importlib.util.spec_from_file_location("mig_ch", ROOT / "scripts" / "migrate_market_data_comments_history.py")
mig = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mig)


def _entries(n=14, with_key_from=3):
    es = [{"date": f"2026-07-{i + 1:02d}T21:30:00+09:00", "summary": f"s{i}" if i != 5 else "", "x": i} for i in range(n)]
    for i, e in enumerate(es):
        if i >= with_key_from:
            e["comments_history"] = mig.derive(es, i)
    return es


def test_writer_no_longer_writes_comments_history(tmp_path, monkeypatch):
    monkeypatch.setattr(cs, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cs, "JSON_PATH", str(tmp_path / "market_data.json"))
    monkeypatch.setattr(cs, "CSV_PATH", str(tmp_path / "market_data.csv"))
    sd = {"S&P500": {"value": 7000.0, "change": 1.0, "change_percent": 0.1, "volume_ratio": 1.0, "date": "2026-11-26"}}
    cs.save_data_to_json_and_csv("report A", sd, {"score": 50.0, "label": "NEUTRAL"})
    cs.save_data_to_json_and_csv("report B", sd, {"score": 51.0, "label": "NEUTRAL"})
    data = json.loads((tmp_path / "market_data.json").read_text(encoding="utf-8"))
    assert data and all("comments_history" not in e for e in data)
    assert data[-1]["summary"] == "report B"


def test_derive_is_current_entry_plus_previous_eleven_skipping_empty_summaries():
    es = _entries(14, with_key_from=99)
    h = mig.derive(es, 12)
    assert [x["summary"] for x in h] == ["s12", "s11", "s10", "s9", "s8", "s7", "s6", "s4", "s3", "s2", "s1"]   # s5は空なので除く、s0は11件の外
    assert len(h) == 11 and h[0]["date"] == es[12]["date"]
    assert mig.derive(es, 0) == [{"date": es[0]["date"], "summary": "s0"}]


def test_strip_removes_only_when_stored_history_matches_derived():
    es = _entries()
    assert mig.mismatches(es) == []
    n = mig.strip(es)
    assert n == 11 and all("comments_history" not in e for e in es)
    assert es[3]["x"] == 3 and es[3]["summary"] == "s3"                         # ほかのキーは変わらない
    bad = _entries()
    bad[8]["comments_history"][1]["summary"] = "書き換えられた"
    before = json.dumps(bad, ensure_ascii=False)
    with pytest.raises(ValueError):
        mig.strip(bad)
    assert json.dumps(bad, ensure_ascii=False) == before                          # 食い違いがあれば何も消さない


def test_migration_script_roundtrip_keeps_file_format(tmp_path):
    p = tmp_path / "md.json"
    es = _entries()
    p.write_text(json.dumps(es, ensure_ascii=False, indent=2), encoding="utf-8")
    assert mig.main([str(p), "--check"]) == 0
    assert mig.main([str(p)]) == 0
    out = json.loads(p.read_text(encoding="utf-8"))
    assert len(out) == len(es) and all("comments_history" not in e for e in out)
    assert mig.main([str(p)]) == 0                                              # 2回目は何もしない（冪等）


def test_repo_market_data_history_is_derivable_or_already_removed():
    """手元の market_data.json: 旧キーが残っているエントリは、前のエントリの summary から作った履歴と一致する"""
    path = ROOT / "docs" / "market-monitor" / "market-pulse" / "data" / "market_data.json"
    es = json.loads(path.read_text(encoding="utf-8"))
    assert mig.mismatches(es) == []


def test_screen_derives_history_and_does_not_read_the_old_key():
    html = (ROOT / "docs" / "market-monitor" / "market-pulse" / "index.html").read_text(encoding="utf-8")
    assert "function commentsHistoryOf(d)" in html and "d.comments_history" not in html          # 書き手のないキーは読まない（INV-5）
    assert "const hist=commentsHistoryOf(d);" in html and "const hist=d.comments_history;" not in html
    # 履歴を持っていた最初のエントリの日（2026-06-24）より前は、これまでどおり履歴なし（表示を変えない）
    assert 'COMMENTS_HISTORY_FROM="2026-06-24"' in html
