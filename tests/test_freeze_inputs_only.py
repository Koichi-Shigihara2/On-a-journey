"""
凍結の規則（[[FCF-DEFINITION-SINGLE-SOURCE-1]] 段1の残り、2026-11-03。説明は SYSTEM_MAP.md の fixed_registry の節）:
凍結するのは申告の値（入力）だけ。計算した値は凍結せず、凍結した入力から毎回作る。

  (1) 登録の側: fixed_registry.json の fields_snapshot に計算した値（FREEZE_FORBIDDEN_FIELDS）が無い
  (2) 登録の側: 逆算した値（provenanceのderived）を凍結するなら frozen_derived の印がある
  (3) 凍結の時に計算した値があれば止める
  (4) 凍結の後に、凍結した入力から FCF・逆算の値を作り直す（frozen_derived の項目は凍結のまま）
"""
import json
import os

import pytest

from common.sec_data import parser as parser_mod
from common.sec_data.parser import SECParser
from common.sec_data.utils import FREEZE_DERIVABLE_FIELDS, FREEZE_FORBIDDEN_FIELDS

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DATA = os.path.join(_ROOT, "common", "sec_data", "data")
_REGISTRY = json.load(open(os.path.join(_ROOT, "common", "sec_data", "fixed_registry.json"), encoding="utf-8"))
_CAT = {"gross_profit": "pl", "operating_income": "pl", "total_liabilities": "bs"}


def test_registry_has_no_computed_fields():
    bad = [(t, y, sorted(set(e.get("fields_snapshot") or []) & FREEZE_FORBIDDEN_FIELDS))
           for t, ys in _REGISTRY.items() for y, e in ys.items()
           if set(e.get("fields_snapshot") or []) & FREEZE_FORBIDDEN_FIELDS]
    assert bad == []


def test_registry_frozen_derived_is_marked():
    bad = []
    for t, ys in _REGISTRY.items():
        for y, e in ys.items():
            snap = set(e.get("fields_snapshot") or [])
            marked = set(e.get("frozen_derived") or [])
            assert marked <= snap, (t, y, "frozen_derived は fields_snapshot の項目だけ")
            path = os.path.join(_DATA, t, f"annual_{y}.json")
            if not (snap & FREEZE_DERIVABLE_FIELDS) or not os.path.exists(path):
                continue
            a = json.load(open(path, encoding="utf-8"))
            for f in snap & FREEZE_DERIVABLE_FIELDS:
                if ((a.get(_CAT[f] + "_provenance") or {}).get(f) or {}).get("derived") and f not in marked:
                    bad.append((t, y, f))
    assert bad == [], "逆算した値を凍結するなら frozen_derived に書く（入力があるなら凍結から外す）"


def _setup(tmp_path, monkeypatch, entry, old):
    d = tmp_path / "T"
    d.mkdir()
    (d / "annual_2020.json").write_text(json.dumps(old), encoding="utf-8")
    monkeypatch.setattr(parser_mod, "_load_fixed_registry", lambda: {"T": {"2020": entry}})
    return SECParser(data_dir=str(tmp_path))


def _result():
    # 今回の抽出（凍結の前）。入力は旧い値と違い、逆算の値は今回の入力から作られている
    return {"annual": {2020: {
        "pl": {"revenue": 900, "cost_of_revenue": 500, "gross_profit": 400},
        "pl_provenance": {"gross_profit": {"derived": True}},
        "bs": {"total_assets": 2000, "stockholders_equity": 800, "total_liabilities": 1200},
        "bs_provenance": {"total_liabilities": {"derived": True}},
        "cf": {"operating_cash_flow": 300, "capital_expenditure": -50, "free_cash_flow": 250},
    }}}


_OLD = {"pl": {"revenue": 1000, "cost_of_revenue": 600, "gross_profit": 999},
        "bs": {"total_assets": 2100, "stockholders_equity": 700, "total_liabilities": 999},
        "cf": {"operating_cash_flow": 500, "capital_expenditure": -120, "free_cash_flow": 500}}


def test_freeze_rejects_computed_field(tmp_path, monkeypatch):
    p = _setup(tmp_path, monkeypatch, {"fields_snapshot": ["operating_cash_flow", "free_cash_flow"]}, _OLD)
    with pytest.raises(RuntimeError, match="計算した値"):
        p._apply_fixed_registry_freeze("T", _result())


def test_freeze_recomputes_from_frozen_inputs(tmp_path, monkeypatch):
    snap = ["revenue", "cost_of_revenue", "total_assets", "stockholders_equity",
            "operating_cash_flow", "capital_expenditure"]
    p = _setup(tmp_path, monkeypatch, {"fields_snapshot": snap}, _OLD)
    r = _result()
    p._apply_fixed_registry_freeze("T", r)
    a = r["annual"][2020]
    assert a["cf"]["free_cash_flow"] == 500 - 120      # 旧い凍結のFCF（500＝営業CF）ではなく、凍結した入力から
    assert a["pl"]["gross_profit"] == 1000 - 600
    assert a["bs"]["total_liabilities"] == 2100 - 700


def test_frozen_derived_stays_frozen(tmp_path, monkeypatch):
    snap = ["revenue", "gross_profit"]
    p = _setup(tmp_path, monkeypatch, {"fields_snapshot": snap, "frozen_derived": ["gross_profit"]}, _OLD)
    r = _result()
    p._apply_fixed_registry_freeze("T", r)
    assert r["annual"][2020]["pl"]["gross_profit"] == 999   # 逆算の凍結の印があれば旧い値のまま


def test_reported_value_is_not_recomputed(tmp_path, monkeypatch):
    # 申告の値（derived でない）は引き直さない
    p = _setup(tmp_path, monkeypatch, {"fields_snapshot": ["revenue", "cost_of_revenue"]}, _OLD)
    r = _result()
    r["annual"][2020]["pl_provenance"] = {"gross_profit": {"accn": "x", "is_own_data": True}}
    p._apply_fixed_registry_freeze("T", r)
    assert r["annual"][2020]["pl"]["gross_profit"] == 400
