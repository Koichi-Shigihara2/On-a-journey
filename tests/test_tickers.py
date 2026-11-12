"""
tests/test_tickers.py

common/sec_data/tickers.py のユニットテスト。
get_active_tickers()がフラグ='true'かつstatusが'retired'でない銘柄のみを
返すこと（ZS-TICKERS-LEAK-1・銘柄リスト統一アクセサ導入）を検証する。

実行方法:
    python -m pytest tests/test_tickers.py -v
"""

import csv
import sys
import os

_SEC_DATA_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "common", "sec_data")
)
sys.path.insert(0, _SEC_DATA_DIR)

import tickers as tk  # noqa: E402


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


class TestGetActiveTickers:
    def test_returns_only_flagged_tickers(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true"),
            _row("BBB", tanuki="false"),
            _row("CCC", tanuki="true"),
        ])
        assert tk.get_active_tickers("tanuki", csv_path) == ["AAA", "CCC"]

    def test_excludes_retired_status_even_if_flag_true(self, tmp_path):
        """ZS-TICKERS-LEAK-1型の再発防止: statusがretiredなら
        フラグがtrueでも対象外とする"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", status="active"),
            _row("BBB", tanuki="true", status="retired"),
        ])
        assert tk.get_active_tickers("tanuki", csv_path) == ["AAA"]

    def test_excludes_provisioning_status_even_if_flag_true(self, tmp_path):
        """[[REGISTER-FLOW-REDESIGN-1]]方針2: statusがprovisioning
        （登録処理中・Step 8のNG=0確認前）ならフラグがtrueでも対象外
        とする（retired同様、中途半端な登録の本番混入防止）"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", status="active"),
            _row("BBB", tanuki="true", status="provisioning"),
        ])
        assert tk.get_active_tickers("tanuki", csv_path) == ["AAA"]

    def test_candidate_status_is_still_included(self, tmp_path):
        """status=candidateは既存の運用（WST/CON等）を壊さないため対象に含める"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", status="active"),
            _row("BBB", tanuki="true", status="candidate"),
        ])
        assert tk.get_active_tickers("tanuki", csv_path) == ["AAA", "BBB"]

    def test_empty_flag_value_excluded(self, tmp_path):
        csv_path = _write_csv(tmp_path, [_row("AAA", tanuki="")])
        assert tk.get_active_tickers("tanuki", csv_path) == []

    def test_case_and_whitespace_insensitive(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            {"ticker": "AAA", "cik": "0", "name": "AAA", "tanuki": " TRUE ",
             "status": " Active "},
        ])
        assert tk.get_active_tickers("tanuki", csv_path) == ["AAA"]


class TestGetRegistrableTickers:
    """get_registrable_tickers()がget_active_tickers()と異なり
    provisioningを除外しない（retiredのみ除外する）こと
    （[[REGISTER-FLOW-REDESIGN-1]]方針3、register_ticker.pyが
    Step 3/5/5bで明示指定するprovisioningティッカーを処理できるように
    するための専用関数）"""

    def test_includes_provisioning_status(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", status="active"),
            _row("BBB", tanuki="true", status="provisioning"),
        ])
        assert tk.get_registrable_tickers("tanuki", csv_path) == ["AAA", "BBB"]

    def test_still_excludes_retired_status(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", status="active"),
            _row("BBB", tanuki="true", status="retired"),
        ])
        assert tk.get_registrable_tickers("tanuki", csv_path) == ["AAA"]

    def test_excludes_flag_false_even_if_provisioning(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="false", status="provisioning"),
        ])
        assert tk.get_registrable_tickers("tanuki", csv_path) == []


class TestGetRegistrableTickersNoFlag:
    """get_registrable_tickers(flag=None)がフラグによる絞り込みを行わず、
    status='retired'以外の全銘柄を返すこと（[[TICKER-LOADING-
    UNIFICATION-1]]、2026-09-05追加。common/sec_data/config.py::get_all()
    統合用。update.py Step 1がprovisioning銘柄のSEC生データ取得も
    行える必要があるため、get_active_tickers()〈provisioning除外〉ではなく
    こちらを使う）"""

    def test_returns_all_flags_regardless_of_flag_values(self, tmp_path):
        """個々のフラグ値に関わらず（tanuki/eps/hypecore/stonks_silo
        いずれかがfalseでも）全銘柄が対象になること"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", hypecore="false", status="active"),
            _row("BBB", tanuki="false", hypecore="true", status="active"),
        ])
        assert tk.get_registrable_tickers(csv_path=csv_path) == ["AAA", "BBB"]

    def test_includes_provisioning_status(self, tmp_path):
        """update.py Step 1がprovisioning銘柄のSEC生データ取得を
        継続できることの根拠（register_ticker.py Step 1）"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", status="active"),
            _row("BBB", status="provisioning"),
        ])
        assert tk.get_registrable_tickers(csv_path=csv_path) == ["AAA", "BBB"]

    def test_excludes_retired_status(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", status="active"),
            _row("BBB", status="retired"),
        ])
        assert tk.get_registrable_tickers(csv_path=csv_path) == ["AAA"]

    def test_default_flag_is_none(self, tmp_path):
        """位置引数を省略した場合もflag=None相当（全銘柄対象）になること"""
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="false", status="active"),
        ])
        assert tk.get_registrable_tickers(csv_path=csv_path) == ["AAA"]


class TestGetCik:
    """tickers.get_cik()が[[TICKER-LOADING-UNIFICATION-1]]（2026-09-05）で
    src/tail/kpi_proposer.py・sec_ctrl_fetcher.py・text_kpi_extractor.pyの
    重複load_cik(ticker)実装を統合したもの。SEC submissions APIが要求する
    10桁ゼロ埋め形式への正規化を常に行うこと（旧kpi_proposer.py側の
    2026-08-19⑦ CRWV未パディング修正を引き継ぐ）を確認する"""

    def test_returns_cik_zero_padded(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            {"ticker": "AAA", "cik": "0000320193", "name": "AAA",
             "status": "active"},
        ])
        assert tk.get_cik("AAA", csv_path) == "0000320193"

    def test_zero_pads_unpadded_cik(self, tmp_path):
        """CRWV型: CSV上のCIKが10桁未満（未パディング）の行を正規化する"""
        csv_path = _write_csv(tmp_path, [
            {"ticker": "CRWV", "cik": "1769628", "name": "CRWV",
             "status": "active"},
        ])
        assert tk.get_cik("CRWV", csv_path) == "0001769628"

    def test_returns_none_for_unknown_ticker(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            {"ticker": "AAA", "cik": "0000320193", "name": "AAA",
             "status": "active"},
        ])
        assert tk.get_cik("ZZZ", csv_path) is None

    def test_returns_none_when_cik_empty(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            {"ticker": "AAA", "cik": "", "name": "AAA", "status": "active"},
        ])
        assert tk.get_cik("AAA", csv_path) is None

    def test_case_and_whitespace_insensitive_ticker_match(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            {"ticker": "AAA", "cik": "0000320193", "name": "AAA",
             "status": "active"},
        ])
        assert tk.get_cik(" aaa ", csv_path) == "0000320193"


class TestConvenienceWrappersUseActiveTickers:
    """get_tanuki_tickers等の既存便利関数がget_active_tickers経由になったこと
    （statusフィルタが一貫して適用されること）を確認する"""

    def test_get_tanuki_tickers_excludes_retired(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", status="active"),
            _row("BBB", tanuki="true", status="retired"),
        ])
        assert tk.get_tanuki_tickers(csv_path) == ["AAA"]

    def test_get_stonks_silo_tickers_excludes_retired(self, tmp_path, monkeypatch):
        """STONKSの対象は列ではなく収益の部品の判定（common/stonks_target.py、[[DEFICIT-QUALITY-ADOPT-1]]）。statusは同じく効く"""
        import json
        from common import stonks_target
        csv_path = _write_csv(tmp_path, [
            _row("AAA", status="active"),
            _row("BBB", status="retired"),
        ])
        prof = tmp_path / "profitability.json"
        hist = [["2026-07-31", "loss"], ["2026-08-31", "loss"], ["2026-09-30", "loss"], ["2026-10-31", "loss"]]
        prof.write_text(json.dumps({"tickers": {"AAA": {"operating_state_history": hist},
                                               "BBB": {"operating_state_history": hist}}}), encoding="utf-8")
        monkeypatch.setattr(stonks_target, "PROFITABILITY_PATH", str(prof))
        monkeypatch.setattr(stonks_target, "load_config", lambda path=None: {"excluded": {}, "pinned": {}})
        assert tk.get_stonks_silo_tickers(csv_path) == ["AAA"]

    def test_get_eps_tickers_excludes_retired(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", eps="true", status="active"),
            _row("BBB", eps="true", status="retired"),
        ])
        assert tk.get_eps_tickers(csv_path) == ["AAA"]

    def test_get_hypecore_tickers_excludes_retired(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", hypecore="true", status="active"),
            _row("BBB", hypecore="true", status="retired"),
        ])
        assert tk.get_hypecore_tickers(csv_path) == ["AAA"]


class TestGetAllRows:
    """get_all_rows()がticker以外の列（status/registered_date/
    registration_source等）も含む全行を返すこと（[[QUALITY-GATES-
    EPIC-1]]ゲート4、system_health.py::check_k_ticker_audit()向けに
    2026-09-05新設）"""

    def test_returns_all_columns(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", status="candidate"),
        ])
        rows = tk.get_all_rows(csv_path)
        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAA"
        assert rows[0]["status"] == "candidate"

    def test_returns_all_rows_regardless_of_flags_or_status(self, tmp_path):
        csv_path = _write_csv(tmp_path, [
            _row("AAA", tanuki="true", status="active"),
            _row("BBB", tanuki="false", status="retired"),
        ])
        rows = tk.get_all_rows(csv_path)
        assert [r["ticker"] for r in rows] == ["AAA", "BBB"]

    def test_default_path_matches_get_all_tickers(self):
        """csv_path省略時は本番cik_lookup.csvを読み、get_all_tickers()と
        同じ銘柄集合になること"""
        rows = tk.get_all_rows()
        assert [r["ticker"] for r in rows] == tk.get_all_tickers()


class TestProductionCikLookup:
    """本番cik_lookup.csvに対する回帰テスト（ZS-TICKERS-LEAK-1の実害確認）"""

    def test_zs_excluded_from_tanuki_tickers(self):
        """ZSはtanuki=falseのため対象外であること"""
        assert "ZS" not in tk.get_tanuki_tickers()

    def test_zs_included_in_stonks_silo_tickers(self):
        """ZSはstonks_silo=trueのためSTONKS SILO側では引き続き対象"""
        assert "ZS" in tk.get_stonks_silo_tickers()

    def test_rklb_excluded_from_tanuki_tickers(self):
        assert "RKLB" not in tk.get_tanuki_tickers()
