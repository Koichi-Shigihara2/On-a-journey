"""
tests/_tanuki_pipeline_stub.py

TANUKI VALUATIONのpipeline.pyを、依存モジュール（data_fetcher・core_calculator・
validator・growth_sanity）をMagicMockに差し替えた状態でimportするための共通処理
（[[TEST-SYSMODULES-MOCK-LEAK-1]]、2026-10-07）。

以前はtest_pipeline_logic.py・test_tanuki_eps_breakeven_safety.pyが収集時に
sys.modulesへMagicMockを入れたまま戻さなかったため、以後のテストの実行時のimport
（core_calculator.py内の`from data_fetcher import _load_beta_config`等）がMagicMockを
受け取り、収集の順番を変えると最大156件が失敗していた。

ここではスタブをpipelineのimportの間だけ入れ、終わったらsys.modulesを元に戻す。
pipelineモジュールの中の名前（pipeline.TanukiDataFetcher等）はMagicMockに
つながったまま残り、sys.modules["pipeline"]にも登録したままにする
（test_split_adjust.py等が`import pipeline`で同じモジュールを受け取るため）。
tests/conftest.pyの収集完了時の検査が、MagicMockが残っていないことを確かめる。
"""

import importlib
import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock

PIPELINE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "tanuki_valuation")
)
if PIPELINE_DIR not in sys.path:
    sys.path.insert(0, PIPELINE_DIR)

STUBBED_DEPS = ("data_fetcher", "core_calculator", "validator", "growth_sanity")

_MISSING = object()
_pipeline = None


@contextmanager
def temporary_modules(stubs: dict, fresh: tuple = ()):
    """sys.modulesにstubsを入れ、freshの名前を一旦取り除く（その中で新しく読み込ませる）。
    終了時にstubs・freshの名前を元の状態（無かった名前は削除）へ戻す。"""
    saved = {name: sys.modules.get(name, _MISSING) for name in (*stubs, *fresh)}
    for name in fresh:
        sys.modules.pop(name, None)
    sys.modules.update(stubs)
    try:
        yield
    finally:
        for name, mod in saved.items():
            if mod is _MISSING:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


def load_growth_sanity_with_stub_xlrd():
    """xlrd（Damodaran XLSの読み込み）だけをMagicMockにした本物のgrowth_sanityを返す。

    sys.modulesのxlrd・growth_sanityは呼び出し前の状態に戻す（毎回新しく読み込む）。
    """
    with temporary_modules({"xlrd": MagicMock()}, fresh=("growth_sanity",)):
        return importlib.import_module("growth_sanity")


def load_stubbed_pipeline():
    """依存モジュールをMagicMockにした状態でimportしたpipelineを返す（2回目以降は同じもの）。"""
    global _pipeline
    if _pipeline is None:
        with temporary_modules({name: MagicMock() for name in STUBBED_DEPS}):
            sys.modules.pop("pipeline", None)
            _pipeline = importlib.import_module("pipeline")
    sys.modules["pipeline"] = _pipeline
    return _pipeline
