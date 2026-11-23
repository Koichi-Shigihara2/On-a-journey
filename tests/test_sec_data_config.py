"""
tests/test_sec_data_config.py

common/sec_data/config.py::get_all() のユニットテスト。
[[TICKER-LOADING-UNIFICATION-1]]（2026-09-05）でtickers.py独自実装
（TICKERSキー列挙）からtickers.get_registrable_tickers()経由に統一した。
update.py（SEC EDGAR生データ取得のStep 1）が引き続き
provisioning銘柄も取得対象に含めることを、本番に影響しないテスト銘柄
（tmp_pathの一時CSV）で確認する。

実行方法:
    python -m pytest tests/test_sec_data_config.py -v
"""

import csv
import sys
import os

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from common.sec_data import config  # noqa: E402
from common.sec_data import tickers as tk  # noqa: E402


def _write_csv(tmp_path, rows: list[dict]) -> str:
    path = tmp_path / "cik_lookup.csv"
    fieldnames = ["ticker", "cik", "name", "eps_sector", "stonks_silo",
                  "tanuki", "eps", "hypecore", "status", "registered_date",
                  "registration_source", "registration_note"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            full = {k: "" for k in fieldnames}
            full.update(row)
            writer.writerow(full)
    return str(path)


def _row(ticker, tanuki="false", stonks_silo="false", eps="false",
         hypecore="false", status="active"):
    return {
        "ticker": ticker, "cik": "0000000000", "name": ticker,
        "tanuki": tanuki, "stonks_silo": stonks_silo, "eps": eps,
        "hypecore": hypecore, "status": status,
    }


class TestGetAllDelegatesToTickers:
    """config.get_all()がtickers.get_registrable_tickers()（flag指定なし）
    に正しく委譲していること。tickers.pyの_DEFAULT_CSVを一時ファイルに
    差し替えて、config.py側の実際の呼び出し経路を経由して検証する
    （config.get_all()自体はcsv_path引数を取らないため）。"""

    def test_includes_provisioning_ticker(self, tmp_path, monkeypatch):
        """update.py Step 1がprovisioning状態の新規登録中銘柄も
        SEC生データ取得の対象に含め続けることの根拠
        （[[REGISTER-FLOW-REDESIGN-1]]方針3）"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", status="active"),
            _row("BBB", status="provisioning"),
        ])
        monkeypatch.setattr(tk, "_DEFAULT_CSV", csv_path)
        assert config.get_all() == ["AAA", "BBB"]

    def test_excludes_retired_ticker(self, tmp_path, monkeypatch):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", status="active"),
            _row("BBB", status="retired"),
        ])
        monkeypatch.setattr(tk, "_DEFAULT_CSV", csv_path)
        assert config.get_all() == ["AAA"]

    def test_ignores_pipeline_flags(self, tmp_path, monkeypatch):
        """update.pyはtanuki/eps/hypecore/stonks_siloいずれの
        パイプラインフラグにも依存しない共有インフラ層のため、
        フラグの値に関わらず全銘柄が対象になること"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", hypecore="false", status="active"),
            _row("BBB", tanuki="false", hypecore="false", stonks_silo="false",
                 eps="false", status="active"),
        ])
        monkeypatch.setattr(tk, "_DEFAULT_CSV", csv_path)
        assert config.get_all() == ["AAA", "BBB"]


class TestProductionGetAll:
    """本番cik_lookup.csvに対する回帰テスト: 旧TICKERS辞書ベース実装との
    完全一致確認（[[TICKER-LOADING-UNIFICATION-1]]統合前後で挙動が
    変わっていないこと）"""

    def test_matches_legacy_tickers_dict_keys(self):
        # retired（登録抹消済み）は get_all から外れる（TICKERS は cik_lookup.csv の全行）
        retired = {r["ticker"] for r in tk.get_all_rows() if (r.get("status") or "").strip().lower() == "retired"}
        assert config.get_all() == [t for t in config.TICKERS if t not in retired]

    def test_matches_get_registrable_tickers_no_flag(self):
        assert config.get_all() == tk.get_registrable_tickers()
