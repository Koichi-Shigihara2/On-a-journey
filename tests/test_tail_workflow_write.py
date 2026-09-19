"""
tests/test_tail_workflow_write.py

TANUKI TAIL — src/tail/workflow_write.py のユニットテスト（TAIL-SEC-1 phase2）。
workflow_dispatch経由のポジション登録・ジャーナル記録・KPI確定ロジックを
実ファイルではなくtmp_pathに対して検証する。

実行方法:
    python -m pytest tests/test_tail_workflow_write.py -v
"""

import sys
import os
import json
import pytest

_TAIL_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "src", "tail"))
sys.path.insert(0, _TAIL_DIR)

import workflow_write as ww  # noqa: E402


@pytest.fixture
def wired(tmp_path, monkeypatch):
    data_dir      = tmp_path / "docs" / "portfolio" / "tail" / "data"
    positions_dir = data_dir / "positions"
    index_path    = data_dir / "positions_index.json"
    journal_path  = data_dir / "journal.json"
    positions_dir.mkdir(parents=True)

    monkeypatch.setattr(ww, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(ww, "POSITIONS_DIR", str(positions_dir))
    monkeypatch.setattr(ww, "INDEX_PATH", str(index_path))
    monkeypatch.setattr(ww, "JOURNAL_PATH", str(journal_path))
    return {"data_dir": data_dir, "positions_dir": positions_dir,
            "index_path": index_path, "journal_path": journal_path}


class TestRegisterPosition:
    def test_core_position_creates_thesis_and_index(self, wired):
        payload = {
            "ticker": "pltr", "type": "core",
            "thesis": "AIPによる収益拡大", "entry_story": "FY24決算後に分割打診",
            "exit_guide": "成長率10%割れで撤退", "entry_price": 25.5, "entry_date": "2026-01-01",
        }
        msg = ww.register_position(payload)
        assert "PLTR" in msg

        thesis = json.loads((wired["positions_dir"] / "PLTR_thesis.json").read_text(encoding="utf-8"))
        assert thesis["ticker"] == "PLTR"
        assert thesis["type"] == "core"
        assert thesis["thesis"] == "AIPによる収益拡大"
        assert thesis["kpis"] == []

        index = json.loads(wired["index_path"].read_text(encoding="utf-8"))
        assert index["positions"] == ["PLTR_thesis.json"]

    def test_satellite_position_requires_strategy_fields(self, wired):
        with pytest.raises(ValueError):
            ww.register_position({"ticker": "SOFI", "type": "satellite"})

    def test_re_registration_dedupes_index(self, wired):
        payload = {
            "ticker": "PLTR", "type": "core",
            "thesis": "t", "entry_story": "e", "exit_guide": "x",
        }
        ww.register_position(payload)
        ww.register_position(payload)
        index = json.loads(wired["index_path"].read_text(encoding="utf-8"))
        assert index["positions"] == ["PLTR_thesis.json"]

    def test_invalid_ticker_rejected(self, wired):
        with pytest.raises(ValueError):
            ww.register_position({"ticker": "../../etc/passwd", "type": "core",
                                   "thesis": "t", "entry_story": "e", "exit_guide": "x"})


class TestRegisterJournal:
    def test_appends_entry_with_generated_id(self, wired):
        wired["journal_path"].write_text(json.dumps({"entries": []}), encoding="utf-8")
        payload = {"ticker": "PLTR", "type": "entry", "date": "2026-06-21", "reason": "決算好調"}
        msg = ww.register_journal(payload)
        assert "PLTR" in msg

        jnl = json.loads(wired["journal_path"].read_text(encoding="utf-8"))
        assert len(jnl["entries"]) == 1
        assert jnl["entries"][0]["id"] == "2026-06-21-PLTR-001"

    def test_duplicate_date_ticker_increments_sequence(self, wired):
        wired["journal_path"].write_text(json.dumps({
            "entries": [{"id": "2026-06-21-PLTR-001"}]
        }), encoding="utf-8")
        payload = {"ticker": "PLTR", "type": "memo", "date": "2026-06-21", "reason": "追記"}
        ww.register_journal(payload)
        jnl = json.loads(wired["journal_path"].read_text(encoding="utf-8"))
        assert jnl["entries"][-1]["id"] == "2026-06-21-PLTR-002"

    def test_missing_reason_raises(self, wired):
        wired["journal_path"].write_text(json.dumps({"entries": []}), encoding="utf-8")
        with pytest.raises(ValueError):
            ww.register_journal({"ticker": "PLTR", "type": "memo", "date": "2026-06-21", "reason": ""})


class TestConfirmKpis:
    def test_updates_existing_thesis_kpis(self, wired):
        thesis_path = wired["positions_dir"] / "PLTR_thesis.json"
        thesis_path.write_text(json.dumps({
            "ticker": "PLTR", "type": "core", "kpis": [], "updated_at": "2026-01-01T00:00:00+09:00"
        }), encoding="utf-8")

        selected = [{
            "name": "Government Revenue Growth", "auto_fetchable": True,
            "warning_threshold": "15%以下", "xbrl_tag": "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        }]
        msg = ww.confirm_kpis({"ticker": "pltr", "kpis": selected})
        assert "1件" in msg

        thesis = json.loads(thesis_path.read_text(encoding="utf-8"))
        assert thesis["kpis"] == selected
        assert thesis["updated_at"] != "2026-01-01T00:00:00+09:00"

    def test_empty_kpis_rejected(self, wired):
        with pytest.raises(ValueError):
            ww.confirm_kpis({"ticker": "PLTR", "kpis": []})

    # TAILKPI-FIELD-VALIDATION-GAP-1（2026-09-19実装）の回帰テスト。
    # 実データ形状（docs/portfolio/tail/data/positions/*_thesis.json全件、
    # config/tail_kpi_map.json）に基づき、以下を検証する:
    # warning_thresholdは自然文字列（数値ではない）・xbrl_tagは
    # "namespace:LocalName"形式かNone/未設定のいずれも許容される。

    def test_real_auto_fetchable_kpi_shape_accepted(self, wired):
        """実データ実例（SOFI 正味貸倒率〈NCO〉）: auto_fetchable=Falseでも
        xbrl_tagを保持するケース。auto_fetchableとの相関では拒否しないこと。"""
        thesis_path = wired["positions_dir"] / "SOFI_thesis.json"
        thesis_path.write_text(json.dumps({
            "ticker": "SOFI", "type": "core", "kpis": [], "updated_at": "2026-01-01T00:00:00+09:00"
        }), encoding="utf-8")
        selected = [{
            "name": "正味貸倒率（NCO）", "auto_fetchable": False,
            "warning_threshold": "2.0%以上", "xbrl_tag": "us-gaap:NetChargeOffs",
        }]
        ww.confirm_kpis({"ticker": "SOFI", "kpis": selected})
        thesis = json.loads(thesis_path.read_text(encoding="utf-8"))
        assert thesis["kpis"] == selected

    def test_manual_kpi_without_xbrl_tag_key_accepted(self, wired):
        """フロントエンド手動追加KPI（tail/index.html confirmKpis()）は
        xbrl_tagキー自体を含まないオブジェクトを送信する。missing keyは
        Noneと同様に許容されること。"""
        thesis_path = wired["positions_dir"] / "PLTR_thesis.json"
        thesis_path.write_text(json.dumps({
            "ticker": "PLTR", "type": "core", "kpis": [], "updated_at": "2026-01-01T00:00:00+09:00"
        }), encoding="utf-8")
        selected = [{
            "name": "手動追加KPI", "description": "", "source": "決算資料",
            "warning_threshold": "前四半期比横ばい", "exit_threshold": "",
            "related_exit_condition": "", "auto_fetchable": False,
        }]
        assert "xbrl_tag" not in selected[0]
        ww.confirm_kpis({"ticker": "PLTR", "kpis": selected})
        thesis = json.loads(thesis_path.read_text(encoding="utf-8"))
        assert thesis["kpis"] == selected

    def test_xbrl_tag_none_accepted(self, wired):
        thesis_path = wired["positions_dir"] / "PLTR_thesis.json"
        thesis_path.write_text(json.dumps({
            "ticker": "PLTR", "type": "core", "kpis": [], "updated_at": "2026-01-01T00:00:00+09:00"
        }), encoding="utf-8")
        selected = [{"name": "顧客解約率", "warning_threshold": "5%以上", "xbrl_tag": None}]
        ww.confirm_kpis({"ticker": "PLTR", "kpis": selected})

    def test_missing_warning_threshold_rejected(self, wired):
        with pytest.raises(ValueError, match="warning_threshold"):
            ww.confirm_kpis({"ticker": "PLTR", "kpis": [{"name": "K1"}]})

    def test_empty_string_warning_threshold_rejected(self, wired):
        with pytest.raises(ValueError, match="warning_threshold"):
            ww.confirm_kpis({"ticker": "PLTR", "kpis": [{"name": "K1", "warning_threshold": "   "}]})

    def test_numeric_warning_threshold_rejected(self, wired):
        """warning_thresholdは実データ上つねに自然文字列（例:「120%以下」）で
        あり数値ではないため、数値型はむしろ想定外の形状として拒否する。"""
        with pytest.raises(ValueError, match="warning_threshold"):
            ww.confirm_kpis({"ticker": "PLTR", "kpis": [{"name": "K1", "warning_threshold": 15}]})

    def test_malformed_xbrl_tag_rejected(self, wired):
        with pytest.raises(ValueError, match="xbrl_tag"):
            ww.confirm_kpis({
                "ticker": "PLTR",
                "kpis": [{"name": "K1", "warning_threshold": "10%以下", "xbrl_tag": "not_a_valid_tag"}],
            })

    def test_xbrl_tag_missing_colon_rejected(self, wired):
        with pytest.raises(ValueError, match="xbrl_tag"):
            ww.confirm_kpis({
                "ticker": "PLTR",
                "kpis": [{"name": "K1", "warning_threshold": "10%以下", "xbrl_tag": "us-gaapRevenues"}],
            })

    def test_xbrl_tag_empty_local_name_rejected(self, wired):
        with pytest.raises(ValueError, match="xbrl_tag"):
            ww.confirm_kpis({
                "ticker": "PLTR",
                "kpis": [{"name": "K1", "warning_threshold": "10%以下", "xbrl_tag": "us-gaap:"}],
            })

    def test_non_dict_kpi_item_rejected(self, wired):
        with pytest.raises(ValueError):
            ww.confirm_kpis({"ticker": "PLTR", "kpis": ["not-a-dict"]})

    def test_all_real_confirmed_kpi_shapes_accepted(self, wired):
        """docs/portfolio/tail/data/positions/*_thesis.json 全10銘柄・
        20件のkpis実データを、実際にconfirm_kpis()へ通して1件も拒否
        されないことを確認する（既存の有効な値を拒否しない、という
        実装要件の直接検証）。"""
        import glob
        repo_root = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
        pos_glob = os.path.join(repo_root, "docs", "portfolio", "tail", "data", "positions", "*_thesis.json")
        files = glob.glob(pos_glob)
        assert files, "実データのthesis.jsonが見つからない（テスト環境の前提が崩れている）"

        for i, fp in enumerate(files):
            with open(fp, encoding="utf-8") as f:
                real_thesis = json.load(f)
            kpis = real_thesis.get("kpis") or []
            if not kpis:
                continue
            ticker = f"T{i}"
            thesis_path = wired["positions_dir"] / f"{ticker}_thesis.json"
            thesis_path.write_text(json.dumps({
                "ticker": ticker, "type": "core", "kpis": [], "updated_at": "2026-01-01T00:00:00+09:00"
            }), encoding="utf-8")
            ww.confirm_kpis({"ticker": ticker, "kpis": kpis})


class TestMain:
    def test_main_writes_commit_message_file(self, wired, monkeypatch, tmp_path):
        commit_msg_path = tmp_path / "commit_message.txt"
        monkeypatch.setenv("ACTION", "register_position")
        monkeypatch.setenv("PAYLOAD", json.dumps({
            "ticker": "PLTR", "type": "core", "thesis": "t", "entry_story": "e", "exit_guide": "x",
        }))

        original_open = open

        def _patched_open(path, *args, **kwargs):
            if path == "/tmp/tail_commit_message.txt":
                return original_open(commit_msg_path, *args, **kwargs)
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(ww, "open", _patched_open, raising=False)
        ww.main()
        assert "PLTR" in commit_msg_path.read_text(encoding="utf-8")

    def test_unknown_action_exits(self, wired, monkeypatch):
        monkeypatch.setenv("ACTION", "delete_everything")
        monkeypatch.setenv("PAYLOAD", "{}")
        with pytest.raises(SystemExit):
            ww.main()
