"""EPS ANALYZER の get_cik() は登録簿（config/cik_lookup.csv）を読むだけで書かない。

R-AのEPSの段階1（2026-10-21）: 旧 save_cik_map() は登録簿に無い銘柄で SEC の API から CIK を取り、
13列の cik_lookup.csv を3列（ticker・cik・name）で上書きしえた（FINDINGS 2026-10-21）。
CIK の引き方は common/sec_data/tickers.py::get_cik() の1つにした。
"""
import hashlib

import pytest

from common.sec_data import tickers as tickers_mod
from src.value.adjusted_eps_analyzer import extract_key_facts as ekf

_HEADER = ("ticker,cik,name,eps_sector,stonks_silo,tanuki,eps,hypecore,status,"
           "registered_date,registration_source,registration_note,exclusion_reason\n")


@pytest.fixture
def registry(tmp_path, monkeypatch):
    path = tmp_path / "cik_lookup.csv"
    path.write_text(_HEADER
                    + "AAA,0000000123,Aaa Inc,tech,false,true,true,false,active,2026-01-01,manual,,\n"
                    + "BBB,1769628,Bbb Inc,tech,false,true,true,false,active,2026-01-01,manual,,\n",
                    encoding="utf-8")
    monkeypatch.setattr(tickers_mod, "_DEFAULT_CSV", str(path))
    monkeypatch.setattr(ekf, "CIK_FILE", str(path))
    return path


def _digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_returns_registry_cik_zero_padded(registry):
    assert ekf.get_cik("AAA") == "0000000123"
    assert ekf.get_cik(" bbb ") == "0001769628"


def test_unregistered_ticker_raises_and_leaves_registry_unchanged(registry, monkeypatch):
    before = _digest(registry)

    def _no_network(*a, **k):
        raise AssertionError("SEC の API を呼ばない")

    monkeypatch.setattr(ekf.requests, "get", _no_network)
    with pytest.raises(Exception, match="CIK not found for ZZZ. Please add to"):
        ekf.get_cik("zzz")
    assert _digest(registry) == before


def test_no_registry_writer_left():
    assert not hasattr(ekf, "save_cik_map")
    assert not hasattr(ekf, "load_cik_map")
