"""
tests/test_beta_fetcher.py

[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-1（beta_fetcher.py切替）の
回帰テスト。β取得元をyfinance直接呼び出しからcommon/market_data/reader.py
経由に切替たこと・overrides[ticker]書き込みが既存entryとマージされる
（sector等の副次キーを消去しない）ことを検証する。

実行方法:
    python -m pytest tests/test_beta_fetcher.py -v
"""

import json
import os
import sys

import pytest

# beta_fetcher.pyはsrc.value.tanuki_valuationパッケージ経由（__init__.pyの
# wacc未解決import）ではimportできないため、tests/test_growth.pyと同じ
# パターン（対象ディレクトリ自体をsys.pathへ追加）でモジュール単体を読む。
_PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

import beta_fetcher as bf  # noqa: E402
from common.market_data import reader as md_reader  # noqa: E402


@pytest.fixture
def isolated_config(tmp_path, monkeypatch):
    """CONFIG_PATHを一時ファイルに差し替え、本番config/beta_config.jsonへの
    書き込みを防ぐ。"""
    cfg_path = tmp_path / "beta_config.json"
    monkeypatch.setattr(bf, "CONFIG_PATH", cfg_path)
    return cfg_path


def _write_config(cfg_path, overrides: dict) -> None:
    cfg_path.write_text(json.dumps({"overrides": overrides}, ensure_ascii=False), encoding="utf-8")


def _write_market_data_beta(market_data_dir, ticker: str, beta) -> None:
    attr_dir = os.path.join(str(market_data_dir), "attributes")
    os.makedirs(attr_dir, exist_ok=True)
    with open(os.path.join(attr_dir, f"{ticker}.json"), "w", encoding="utf-8") as f:
        json.dump({"symbol": ticker, "beta": beta}, f)


@pytest.fixture
def market_data_source(tmp_path, monkeypatch):
    """fetch_market_data_beta()の参照先をtmp_path配下の一時market_data
    ディレクトリに差し替える。"""
    md_dir = tmp_path / "market_data"
    monkeypatch.setattr(
        bf, "_get_market_data_attributes",
        lambda t: md_reader.get_attributes(t, base_dir=str(md_dir)),
    )
    return md_dir


class TestFetchMarketDataBeta:
    def test_returns_beta_when_attributes_exist(self, tmp_path):
        _write_market_data_beta(tmp_path, "AAPL", 1.086)
        result = md_reader.get_attributes("AAPL", base_dir=str(tmp_path))
        assert result["beta"] == 1.086

    def test_returns_none_when_attributes_missing(self, tmp_path):
        attrs = md_reader.get_attributes("NOPE", base_dir=str(tmp_path))
        assert attrs is None


class TestRefreshTickersMarketDataSource:
    def test_updates_beta_from_market_data(self, isolated_config, market_data_source):
        _write_config(isolated_config, {"XYZ": {"beta": 1.0, "source": "yfinance_5yr"}})
        _write_market_data_beta(market_data_source, "XYZ", 1.5)

        updated, skipped = bf.refresh_tickers(["XYZ"], dry_run=False)

        assert len(updated) == 1
        assert updated[0]["new"] == 1.5
        saved = json.loads(isolated_config.read_text(encoding="utf-8"))
        assert saved["overrides"]["XYZ"]["beta"] == 1.5

    def test_missing_market_data_is_skipped_not_raised(self, isolated_config, market_data_source):
        _write_config(isolated_config, {})
        updated, skipped = bf.refresh_tickers(["NEVERFETCHED"], dry_run=False)
        assert updated == []
        assert len(skipped) == 1
        assert skipped[0]["ticker"] == "NEVERFETCHED"
        assert "market_data" in skipped[0]["reason"]

    def test_unchanged_beta_produces_no_update(self, isolated_config, market_data_source):
        _write_config(isolated_config, {"XYZ": {"beta": 1.5, "source": "yfinance_5yr"}})
        _write_market_data_beta(market_data_source, "XYZ", 1.5)
        updated, skipped = bf.refresh_tickers(["XYZ"], dry_run=False)
        assert updated == []
        assert skipped == []

    def test_beta_is_capped_at_upper_bound(self, isolated_config, market_data_source):
        _write_config(isolated_config, {})
        _write_market_data_beta(market_data_source, "HIGH", 5.0)
        updated, _ = bf.refresh_tickers(["HIGH"], dry_run=False)
        assert updated[0]["new"] == bf.BETA_CAP

    def test_beta_is_floored_at_lower_bound(self, isolated_config, market_data_source):
        _write_config(isolated_config, {})
        _write_market_data_beta(market_data_source, "LOW", 0.05)
        updated, _ = bf.refresh_tickers(["LOW"], dry_run=False)
        assert updated[0]["new"] == bf.BETA_FLOOR

    def test_zero_or_negative_beta_still_floors_via_refresh(self, isolated_config, market_data_source, capsys):
        """[[BETA-FALLBACK-DESIGN-GAPS-1]]: raw_beta<=0でもクリップ処理
        自体（下限0.3への丸め）は従来通り行われることを、refresh_tickers()
        経由でも確認する（calc_capped_beta()単体のテストはTestCalcCapped
        BetaZeroOrNegativeWarning参照）。"""
        _write_config(isolated_config, {})
        _write_market_data_beta(market_data_source, "ZERO", 0.0)
        updated, _ = bf.refresh_tickers(["ZERO"], dry_run=False)
        assert updated[0]["new"] == bf.BETA_FLOOR
        captured = capsys.readouterr()
        assert "[WARN] ZERO" in captured.out

    def test_dry_run_does_not_write_config(self, isolated_config, market_data_source):
        _write_config(isolated_config, {"XYZ": {"beta": 1.0, "source": "yfinance_5yr"}})
        _write_market_data_beta(market_data_source, "XYZ", 2.0)
        bf.refresh_tickers(["XYZ"], dry_run=True)
        saved = json.loads(isolated_config.read_text(encoding="utf-8"))
        assert saved["overrides"]["XYZ"]["beta"] == 1.0  # 変化していない


class TestCalcCappedBetaZeroOrNegativeWarning:
    """[[BETA-FALLBACK-DESIGN-GAPS-1]]: calc_capped_beta()がraw_beta<=0を
    検知した場合にWARNログを出すこと（クリップ処理自体は変更しない）を
    検証する。2026-09-12調査で全101銘柄に実例0件と確認済みだが、将来の
    データ異常が無警告で握りつぶされないようにするための安全網。"""

    def test_warns_on_zero_raw_beta(self, capsys):
        capped, src = bf.calc_capped_beta(0.0, "ZEROBETA")
        assert capped == bf.BETA_FLOOR
        captured = capsys.readouterr()
        assert "[WARN]" in captured.out
        assert "ZEROBETA" in captured.out

    def test_warns_on_negative_raw_beta(self, capsys):
        capped, src = bf.calc_capped_beta(-0.5, "NEGBETA")
        assert capped == bf.BETA_FLOOR
        captured = capsys.readouterr()
        assert "[WARN]" in captured.out
        assert "NEGBETA" in captured.out

    def test_no_warning_for_normal_positive_beta(self, capsys):
        """通常の正のβ（フロア/キャップ範囲内・範囲外いずれも）ではWARNを
        出さないことを確認する（過検知でないことの確認）"""
        bf.calc_capped_beta(1.5, "NORMAL")
        bf.calc_capped_beta(0.05, "BELOWFLOOR")  # 正だがフロア未満（既存の挙動）
        bf.calc_capped_beta(5.0, "ABOVECAP")     # 正だがキャップ超過（既存の挙動）
        captured = capsys.readouterr()
        assert "[WARN]" not in captured.out


class TestOverridesMergePreservesExtraKeys:
    """overrides[ticker]書き込みが既存キー（sector等）を消去しないことを確認する
    （[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-1で発見・修正した回帰）。"""

    def test_sector_key_survives_beta_update(self, isolated_config, market_data_source):
        _write_config(isolated_config, {
            "XYZ": {"beta": 1.0, "source": "yfinance_5yr", "sector": "Some_Sector"},
        })
        _write_market_data_beta(market_data_source, "XYZ", 2.0)
        bf.refresh_tickers(["XYZ"], dry_run=False)
        saved = json.loads(isolated_config.read_text(encoding="utf-8"))
        assert saved["overrides"]["XYZ"]["sector"] == "Some_Sector"
        assert saved["overrides"]["XYZ"]["beta"] == 2.0

    def test_provisional_classification_keys_survive_beta_update(self, isolated_config, market_data_source):
        _write_config(isolated_config, {
            "XYZ": {
                "beta": 1.0, "source": "yfinance_5yr",
                "sector": "Software_System_SaaS",
                "software_system_provisional": True,
                "software_system_provisional_note": "境界近傍のため暫定判定",
            },
        })
        _write_market_data_beta(market_data_source, "XYZ", 1.8)
        bf.refresh_tickers(["XYZ"], dry_run=False)
        saved = json.loads(isolated_config.read_text(encoding="utf-8"))["overrides"]["XYZ"]
        assert saved["sector"] == "Software_System_SaaS"
        assert saved["software_system_provisional"] is True
        assert saved["software_system_provisional_note"] == "境界近傍のため暫定判定"

    def test_sector_key_survives_no_change_case(self, isolated_config, market_data_source):
        """betaが変化しない場合はentry自体に触れないため当然保持されるが、
        回帰として明示的に確認する"""
        _write_config(isolated_config, {
            "XYZ": {"beta": 1.5, "source": "yfinance_5yr", "sector": "Unchanged_Sector"},
        })
        _write_market_data_beta(market_data_source, "XYZ", 1.5)
        bf.refresh_tickers(["XYZ"], dry_run=False)
        saved = json.loads(isolated_config.read_text(encoding="utf-8"))
        assert saved["overrides"]["XYZ"]["sector"] == "Unchanged_Sector"


class TestDamodaranOverridesProtection:
    def test_existing_damodaran_source_is_protected(self, isolated_config, market_data_source):
        _write_config(isolated_config, {
            "LMT": {"beta": 0.74, "source": "damodaran_2025_aerospace_defense", "sector": "Aerospace_Defense"},
        })
        updated, skipped = bf.refresh_tickers(["LMT"], dry_run=False)
        assert updated == []
        assert skipped[0]["reason"] == "Damodaran手動設定を保護"
        saved = json.loads(isolated_config.read_text(encoding="utf-8"))
        assert saved["overrides"]["LMT"]["sector"] == "Aerospace_Defense"

    def test_first_time_damodaran_apply_preserves_existing_extra_keys(self, isolated_config, market_data_source):
        """初回Damodaran適用時も既存のsector等のキーを消去しないことを確認
        （[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-1で修正した回帰）。"""
        _write_config(isolated_config, {
            "LMT": {"beta": 1.2, "source": "yfinance_5yr", "sector": "Aerospace_Defense", "note": "keep me"},
        })
        updated, skipped = bf.refresh_tickers(["LMT"], dry_run=False)
        assert len(updated) == 1
        saved = json.loads(isolated_config.read_text(encoding="utf-8"))["overrides"]["LMT"]
        assert saved["beta"] == 0.74
        assert saved["source"] == "damodaran_2025_aerospace_defense"
        assert saved["sector"] == "Aerospace_Defense"
        assert saved["note"] == "keep me"

    def test_market_data_is_not_consulted_for_damodaran_tickers(self, isolated_config, market_data_source):
        """DAMODARAN_OVERRIDES該当銘柄はmarket_dataを一切参照しない
        （attributes/未生成でも正常に固定値が適用される）ことを確認"""
        _write_config(isolated_config, {})
        # market_data側に何も置かない（LMTのattributes/は存在しない）
        updated, skipped = bf.refresh_tickers(["LMT"], dry_run=False)
        assert len(updated) == 1
        assert updated[0]["new"] == 0.74
        assert skipped == []
