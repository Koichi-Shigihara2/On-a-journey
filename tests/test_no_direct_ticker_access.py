"""
tests/test_no_direct_ticker_access.py

TICKER-DIRECT-ACCESS-GUARD-1: FLAG-CONSUMER-AUDIT-2/3で発見した7箇所の独立実装
（銘柄フラグ判定ロジックの重複実装）の再発防止。「共有アクセサ
（common/sec_data/tickers.py）経由で銘柄リストを取得する」という規約が
CLAUDE_CODE_START.mdにのみ存在し、CIによる機械的強制がなかったことが原因。

検知する2パターン:
  ① cik_lookup.csv を csv.DictReader で直接パースしている箇所
  ② SEC/TANUKI VALUATION/HypeCore/EPS ANALYZERの「ルートデータディレクトリ」
     （全ティッカーのサブディレクトリを含む場所）を os.listdir() で
     直接スキャンしている箇所

いずれも「共有アクセサを使うべきなのに独自実装している」ことを機械的に
検知するためのものであり、cik_lookup.csv・SEC dataディレクトリへの
アクセス自体を全面禁止するものではない（単一ティッカーのCIK参照・
既知ティッカーのサブディレクトリ内ファイル列挙等は正当な用途であり
許可リストで区別する）。

既知の限界（意図的な設計上の簡略化）:
  - 変数解決は同一関数スコープ・モジュールスコープ・同一クラスの
    self.attr 代入に限定する。関数呼び出しを介した間接参照
    （例: score_verifier.py の `data_dir = _data_dir()`）は解決しない
    （解決できない場合は安全側＝検知しない、を選ぶ）。
  - 文字列結合（+ 演算子）・f-string によるパス構築は認識しない。
  - 「ルートディレクトリ」の判定は、os.path.join()の末尾引数が
    リテラル文字列 "data"（またはそれで終わる文字列）で、かつ
    それ以降に位置引数が続かない（＝ティッカー名等の追加セグメントが
    ない）ことで判定する。

実行方法:
    python -m pytest tests/test_no_direct_ticker_access.py -v
"""

from __future__ import annotations

import ast
import os

import pytest

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))

_EXCLUDE_DIR_PARTS = {
    "tests", "venv", "__pycache__", ".git", "node_modules", ".pytest_cache",
}

# ── ① cik_lookup.csv 直接パース: 許可リスト ─────────────────────────
# 各エントリの用途を明記する。「銘柄フラグに基づくバッチ処理対象リストの
# 構築」に該当するものはここに含めない（tickers.py経由に修正が必要なため）。
_CIK_LOOKUP_DIRECT_PARSE_ALLOWED = {
    # 共有アクセサ本体
    "common/sec_data/tickers.py",
    "common/sec_data/config.py",
    # 監査ツール（設計上、cik_lookup.csv全体を無条件スキャンする必要がある。
    # P4-CIKOrphan/P4-CIKIncomplete/eps=false抑制リスト構築はいずれも
    # 「フラグでは絞り込めない・絞り込んではいけない」種類のチェック）
    "common/sec_data/registration_validator.py",
    # common/system_health.py・src/tail/kpi_proposer.py・
    # sec_ctrl_fetcher.py・text_kpi_extractor.pyは[[TICKER-LOADING-
    # UNIFICATION-1]]（2026-09-05）でtickers.py経由（get_all_tickers()・
    # get_cik()）に統一され、直接パースしなくなったため許可リストから
    # 除外した。
    "src/value/tanuki_valuation/data_fetcher.py",
    "src/value/tanuki_valuation/pipeline.py",
    # 新規銘柄登録オーケストレーション（[[REGISTER-FLOW-REDESIGN-1]]
    # 方針3、2026-09-03新設）。単一ティッカー自身の行（status・4フラグ）を
    # 参照するのみで、バッチ対象リスト構築ではない
    "common/registration/register_ticker.py",
    # EPS ANALYZER独自パイプライン（common/sec_data/とは完全に独立と
    # SYSTEM_MAP.mdに明記済み）。CIKマッピング構築・ticker→name表示用・
    # eps_sector列参照のいずれもバッチ対象リスト構築ではない
    "src/value/adjusted_eps_analyzer/extract_key_facts.py",
    "src/value/adjusted_eps_analyzer/pipeline.py",
    "src/value/adjusted_eps_analyzer/sector_classifier_v2.py",
}

# ── ② ルートディレクトリ os.listdir() 直接スキャン: 許可リスト ──────
_ROOT_DIR_LISTDIR_ALLOWED = {
    # 共有アクセサ・監査ツール
    "common/sec_data/reader.py",              # get_available_tickers(): 未使用(__main__専用のデバッグ関数、本番呼び出し元なし)
    "common/sec_data/registration_validator.py",  # P4-SecDataOrphan/P5監査: 設計上無条件スキャンが必要
    # tickers.get_tanuki_tickers()との積集合でフィルタ済み（本ガードは
    # os.listdir()呼び出し自体を検知するため、後続のフィルタ有無に関わらず
    # 検出される。2026-07-13 TICKER-DIRECT-ACCESS-GUARD-1で発見・同日中に
    # tanuki=falseの除外フィルタを追加して解消済み）
    "src/tail/tail_dcf_bridge.py",
    # 2026-07-13時点で発見した既存の直し漏れ（phase1_scan.py・
    # backfill_history.py）は、[[DEAD-CODE-AUDIT-BATCH-1]]（2026-09-09）で
    # いずれも一回限りの診断/バックフィルスクリプトと確認しファイルごと
    # 削除したため、本許可リストからも除外した
}


def _iter_py_files():
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames if d not in _EXCLUDE_DIR_PARTS]
        for fn in filenames:
            if fn.endswith(".py"):
                yield os.path.join(dirpath, fn)


def _rel(path: str) -> str:
    return os.path.relpath(path, REPO_ROOT).replace(os.sep, "/")


def _find_cik_lookup_direct_parsers() -> dict[str, list[int]]:
    """cik_lookup.csv を csv.DictReader で直接パースしているファイルを検出する。

    戻り値: {相対パス: [行番号, ...]}
    """
    hits: dict[str, list[int]] = {}
    for path in _iter_py_files():
        rel = _rel(path)
        try:
            with open(path, encoding="utf-8") as f:
                text = f.read()
        except (UnicodeDecodeError, OSError):
            continue
        if "cik_lookup.csv" not in text or "csv.DictReader" not in text:
            continue
        try:
            tree = ast.parse(text, filename=path)
        except SyntaxError:
            continue
        lines: list[int] = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "DictReader"
            ):
                lines.append(node.lineno)
        if lines:
            hits[rel] = lines
    return hits


def _join_call_is_root_data_dir(call: ast.Call) -> bool:
    """os.path.join(...) 呼び出しが「ルートデータディレクトリ」
    （末尾引数がリテラル"data"、それ以降に追加の位置引数がない）
    パターンに一致するかを判定する。
    """
    if not (isinstance(call.func, ast.Attribute) and call.func.attr == "join"):
        return False
    if not call.args:
        return False
    last = call.args[-1]
    if isinstance(last, ast.Constant) and isinstance(last.value, str):
        return last.value == "data" or last.value.rstrip("/").endswith("/data")
    return False


def _value_is_root_data_dir(value: ast.AST) -> bool:
    """代入右辺（os.path.join(...)・os.path.normpath(os.path.join(...))・
    文字列リテラル）が「ルートデータディレクトリ」パターンかを判定する。
    """
    node = value
    # os.path.normpath(...) のラップを1段はがす
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "normpath"
        and node.args
    ):
        node = node.args[0]
    if isinstance(node, ast.Call):
        return _join_call_is_root_data_dir(node)
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.rstrip("/").endswith("/data")
    return False


def _find_root_dir_listdir_scans() -> dict[str, list[int]]:
    """SEC/TANUKI VALUATION/HypeCore/EPS ANALYZERのルートデータディレクトリを
    os.listdir()で直接スキャンしているファイルを検出する。

    戻り値: {相対パス: [行番号, ...]}
    """
    hits: dict[str, list[int]] = {}
    for path in _iter_py_files():
        rel = _rel(path)
        try:
            with open(path, encoding="utf-8") as f:
                text = f.read()
        except (UnicodeDecodeError, OSError):
            continue
        if "os.listdir" not in text:
            continue
        try:
            tree = ast.parse(text, filename=path)
        except SyntaxError:
            continue

        # 「risky」な変数名・self属性名をファイル全体（全スコープ横断）から収集する。
        # ast.walk(tree) はモジュール直下だけでなく関数・メソッド内部の代入も
        # 含めて辿るため、スコープごとに分けて解決する必要はない（本ファイルの
        # 命名規約では同名変数がスコープをまたいで異なる意味を持つことは
        # 想定していない簡略化）。
        risky_names: set[str] = set()
        self_attr_risky: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and _value_is_root_data_dir(node.value):
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        risky_names.add(tgt.id)
                    elif (
                        isinstance(tgt, ast.Attribute)
                        and isinstance(tgt.value, ast.Name)
                        and tgt.value.id == "self"
                    ):
                        self_attr_risky.add(tgt.attr)

        lines: list[int] = []
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "listdir"
            ):
                continue
            if not node.args:
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Name) and arg.id in risky_names:
                lines.append(node.lineno)
            elif (
                isinstance(arg, ast.Attribute)
                and isinstance(arg.value, ast.Name)
                and arg.value.id == "self"
                and arg.attr in self_attr_risky
            ):
                lines.append(node.lineno)
            elif isinstance(arg, ast.Call) and _join_call_is_root_data_dir(arg):
                lines.append(node.lineno)

        if lines:
            hits[rel] = sorted(set(lines))
    return hits


def test_no_unlisted_cik_lookup_csv_direct_parse():
    hits = _find_cik_lookup_direct_parsers()
    unlisted = {
        rel: ln for rel, ln in hits.items()
        if rel not in _CIK_LOOKUP_DIRECT_PARSE_ALLOWED
    }
    assert not unlisted, (
        "cik_lookup.csv を csv.DictReader で直接パースしている未許可ファイルを検出:\n"
        + "\n".join(f"  {rel}: line {ln}" for rel, lns in unlisted.items() for ln in lns)
        + "\n\n銘柄フラグに基づくバッチ処理対象リストの構築であれば "
          "common/sec_data/tickers.py の get_active_tickers() 系関数を使うこと。"
          "単一ティッカーのCIK参照等、正当な理由がある場合は "
          "_CIK_LOOKUP_DIRECT_PARSE_ALLOWED に追加すること。"
    )


def test_no_unlisted_root_dir_listdir_scan():
    hits = _find_root_dir_listdir_scans()
    unlisted = {
        rel: ln for rel, ln in hits.items()
        if rel not in _ROOT_DIR_LISTDIR_ALLOWED
    }
    assert not unlisted, (
        "SEC/TANUKI VALUATION/HypeCore/EPS ANALYZERのルートデータディレクトリを "
        "os.listdir() で直接スキャンしている未許可ファイルを検出:\n"
        + "\n".join(f"  {rel}: line {ln}" for rel, lns in unlisted.items() for ln in lns)
        + "\n\n銘柄フラグに基づくバッチ処理対象リストの構築であれば "
          "common/sec_data/tickers.py の get_active_tickers() 系関数を使うこと。"
          "監査ツール等、正当な理由がある場合は "
          "_ROOT_DIR_LISTDIR_ALLOWED に追加すること。"
    )


def test_allowed_files_still_exist():
    """許可リストが陳腐化していないか（該当ファイルが削除・リネームされていないか）確認する"""
    all_allowed = _CIK_LOOKUP_DIRECT_PARSE_ALLOWED | _ROOT_DIR_LISTDIR_ALLOWED
    missing = [rel for rel in all_allowed if not os.path.exists(os.path.join(REPO_ROOT, rel))]
    assert not missing, f"許可リストに存在しないファイルが記載されている: {missing}"
