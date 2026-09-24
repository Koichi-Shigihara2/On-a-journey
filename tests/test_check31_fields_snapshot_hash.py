"""
tests/test_check31_fields_snapshot_hash.py

[[CHECK31-WHOLE-FILE-HASH-VS-DIFF-FREEZE-1]]の回帰テスト。CHECK-31
（fixed_registry不整合）は従来annual_{year}.json全体のハッシュで比較して
いたため、parser.pyへの新フィールド追加（凍結機構は差分適用方式で新規
フィールドを通す設計）だけで凍結年度が全てNG化し、2026-09-20の
SEC_Data_Updateを停止させた（`restricted_cash`追加で136件NG）。
fields_snapshotに記録されたフィールドの値・provenanceのみのハッシュへ
変更したことを検証する:
  (1) 凍結年度に新フィールドを追加 → NGにならない
  (2) 凍結済みフィールドの値を書き換え → 必ずNG（保護が弱まっていない）
  (3) 凍結済みフィールドのprovenanceを書き換え → NG
  (4) 凍結済みフィールドの欠落 → NG
  (5) fields_snapshot_hash未記録のエントリ → 黙ってPASSさせずNG

実行方法:
    python -m pytest tests/test_check31_fields_snapshot_hash.py -v
"""

import copy
import json
import os

import pytest

from common.sec_data import report_consistency_check as rcc
from common.sec_data.utils import compute_fields_snapshot_hash, compute_snapshot_hash

_TICKER = "TEST"
_YEAR = "2020"
_FIELDS = ["revenue", "cash_and_equivalents"]

_BASE = {
    "ticker": _TICKER,
    "period": 2020,
    "form": "10-K",
    "pl": {"revenue": 1000, "net_income": 100},
    "bs": {"cash_and_equivalents": 500, "total_assets": 2000},
    "bs_provenance": {
        "cash_and_equivalents": {"accn": "0000-20-1", "filed": "2021-02-01",
                                 "is_own_data": True, "fy_tag": 2020},
        "total_assets": {"accn": "0000-20-1", "filed": "2021-02-01",
                         "is_own_data": True, "fy_tag": 2020},
    },
}


def _run(tmp_path, monkeypatch, data, entry_overrides=None):
    ticker_dir = tmp_path / _TICKER
    ticker_dir.mkdir()
    with open(ticker_dir / f"annual_{_YEAR}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    entry = {
        # 実registryと同様、旧方式のファイル全体ハッシュも保持する
        "snapshot_hash": compute_snapshot_hash(_BASE),
        "fields_snapshot": _FIELDS,
        "fields_snapshot_hash": compute_fields_snapshot_hash(_BASE, _FIELDS),
    }
    entry.update(entry_overrides or {})
    monkeypatch.setattr(rcc, "SEC_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(rcc, "_load_fixed_registry", lambda: {_TICKER: {_YEAR: entry}})
    return rcc._check_fixed_registry_integrity(_TICKER)


def test_unchanged_passes(tmp_path, monkeypatch):
    assert _run(tmp_path, monkeypatch, copy.deepcopy(_BASE)) == []


def test_new_field_added_to_frozen_year_passes(tmp_path, monkeypatch):
    data = copy.deepcopy(_BASE)
    data["bs"]["restricted_cash"] = 2_000_000
    data["bs_provenance"]["restricted_cash"] = {"accn": "0000-22-9", "filed": "2022-02-01",
                                                "is_own_data": False, "fy_tag": 2022}
    assert _run(tmp_path, monkeypatch, data) == []


def test_non_frozen_existing_field_change_passes(tmp_path, monkeypatch):
    # fields_snapshot外のフィールドは凍結機構も保護しない（差分適用方式）
    data = copy.deepcopy(_BASE)
    data["pl"]["net_income"] = 999
    assert _run(tmp_path, monkeypatch, data) == []


@pytest.mark.parametrize("mutate", [
    lambda d: d["pl"].__setitem__("revenue", 1001),
    lambda d: d["bs"].__setitem__("cash_and_equivalents", None),
    lambda d: d["bs_provenance"]["cash_and_equivalents"].__setitem__("accn", "0000-99-9"),
    lambda d: d["pl"].pop("revenue"),
    # 凍結フィールドが別カテゴリへ移動した場合も検知する
    lambda d: (d["pl"].pop("revenue"), d["bs"].__setitem__("revenue", 1000)),
], ids=["value_changed", "value_to_none", "provenance_changed", "field_missing", "category_moved"])
def test_frozen_field_mutation_is_ng(tmp_path, monkeypatch, mutate):
    data = copy.deepcopy(_BASE)
    mutate(data)
    ng = _run(tmp_path, monkeypatch, data)
    assert len(ng) == 1 and "NG-31" in ng[0]


@pytest.mark.parametrize("overrides", [
    {"fields_snapshot_hash": None},
    {"fields_snapshot": []},
], ids=["hash_missing", "fields_empty"])
def test_entry_without_fields_hash_is_ng(tmp_path, monkeypatch, overrides):
    ng = _run(tmp_path, monkeypatch, copy.deepcopy(_BASE), overrides)
    assert len(ng) == 1 and "NG-31" in ng[0]


def test_real_registry_all_entries_have_fields_hash():
    """実fixed_registry.jsonの全エントリがfields_snapshot/fields_snapshot_hashを持つ"""
    path = os.path.join(os.path.dirname(rcc.SEC_DATA_DIR), "fixed_registry.json")
    with open(path, encoding="utf-8") as f:
        registry = json.load(f)
    missing = [(t, y) for t, ys in registry.items() for y, e in ys.items()
               if not e.get("fields_snapshot") or not e.get("fields_snapshot_hash")]
    assert missing == []
