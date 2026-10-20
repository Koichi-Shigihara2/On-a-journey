"""出力の一覧（書き手・読み手・画面・定義・規則）を AST と grep で機械的に作る（[[OUTPUT-INVENTORY-CHECK-1]] 段階1、2026-10-20）。

対象（段階1）: common/sec_data/ の .py が書く出力（ファイル・Step Summary・Discord）と、ファイルの最上位のキー。
一覧は AI が書き起こさない（CHAT_RULES.md「発見の溜め方と根治の判断」5）。人・AIが足すのは下の ANNOTATIONS
（ふつうの言葉の1文）だけで、機械の一覧とは欄を分ける。

作り方:
  1. 書き手: common/sec_data/*.py の AST から書き込み（open(..., "w"/"a")・write_text・to_csv・os.replace の先・
     requests.post/urlopen）を探し、パスの式（os.path.join・f文字列・モジュールの定数・self の属性・
     返り値が1つの関数・import した定数）を解決してパスの型（{T}=銘柄 等）にする。
     common/sec_data/ の外の .py が common/sec_data/ に書く場所も同じ方法で探す（止める条件の数）
  2. 読み手（保存済みファイル）:
     - Python: AST の読み込み（open(...)〈"r"・省略〉・json.load・read_text・pd.read_json/read_csv）のパスを解決して照合
     - 全種類（.py・.js・.html・.yml）: パスの型のファイル名の部分から作った正規表現の grep（コメント・docstring を除く）
     - 経由: 上で読み手になった Python の関数を import して呼ぶ箇所（1段まで）
  3. 読み手（その場で計算）: 書き手の関数に渡す値を作る関数（`x = f(...)` → `save(…, x)` の f）を import して呼ぶ箇所
  4. 画面: docs/ の .html・.js が直接読むもの（直接）と、Python の読み手（とそれを import する本番のファイル、2段まで）が
     docs/ に書くファイルを読む docs/ のページ（経由、推定）
  5. 定義: docs/architecture/new_data_platform/FIELD_DEFINITIONS.md の表の行のうち、ファイル名の型を含む行の ID
  6. 規則: RULES の検出式（定義の書き方の型。式は人が決め、場所は grep で出す）に当たる場所。2ファイル以上なら二重
  7. 本番外のコード: NON_PRODUCTION_DIRS の .py の行数と、ワークフロー・ほかのコード・テストからの参照

使い方:
  python scripts/inventory/build_output_inventory.py           # docs/architecture/output_inventory/sec_layer.json と sec_layer.md を書く
  python scripts/inventory/build_output_inventory.py --check   # 書かずに、今のファイルと同じかだけを見る（違えば終了コード1）

CHECK としてはまだ動かさない（ゲートに入れない。段階3で、消した後に入れる）。
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import posixpath
import re
import subprocess
import sys
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LAYER = "common/sec_data"
OUT_DIR = "docs/architecture/output_inventory"
OUT_JSON = f"{OUT_DIR}/sec_layer.json"
OUT_MD = f"{OUT_DIR}/sec_layer.md"
FIELD_DEFINITIONS = "docs/architecture/new_data_platform/FIELD_DEFINITIONS.md"
STOP_RATIO = 0.10
VIA_DEPTH = 3   # 読み手の関数を呼ぶ箇所をたどる段数

# 投資判断に効く読み手（パスの前方一致）。読み手にこれらが入れば「効く」とする（機械の判定、対応は人が決めた表）
DECISION_PREFIXES = [
    ("src/value/tanuki_valuation/", "理論株価・判定（TANUKI VALUATION）"),
    ("src/value/tanuki_score/", "判定（TANUKI SCORE）"),
    ("src/value/hypecore/", "HypeCore（TANUKI のtiming・判定の入力）"),
    ("src/tail/", "保有銘柄の監視（TANUKI TAIL）"),
]

# 本番外のコード（[[OUTPUT-INVENTORY-CHECK-1]]の2026-10-19追記）
NON_PRODUCTION_DIRS = ["scripts/analysis", "browser_checks", "discover/stonks-silo"]

# 規則の検出式。式（定義の書き方の型）は人が決め、当たる場所は grep で出す
RULES = [
    {
        "id": "q4_implied",
        "name": "Q4の逆算（年次−Q1〜Q3）",
        "patterns": [r"\b\w*q4\w*\s*=\s*\w*(annual|fy)\w*\s*-\s*", r"def\s+_?\w*q4_implied\w*\s*\("],
    },
    {
        "id": "latest_filed",
        "name": "同じ期の値が複数あるときの選び方（提出日が最新のもの）",
        "patterns": [r"max\([^)]*key\s*=\s*lambda[^)]*[\"']filed[\"']", r"sorted\([^)]*[\"']filed[\"'][^)]*reverse\s*=\s*True",
                     r"\[[\"']filed[\"']\]\s*>=?\s*\w+(\[[\"']filed[\"']\]|_filed)", r"def\s+select_latest_filed\s*\("],
    },
    {
        "id": "staleness",
        "name": "データの古さ・遅れの判定",
        "patterns": [r"def\s+_?\w*(stale|lag_days|freshness)\w*\s*\(", r"function\s+\w*(stale|Stale|lag|Lag|fresh|Fresh)\w*\s*\("],
        "context": r"10-Q|10-K|[\"']filed[\"']|SEC_LAG",   # SECの提出の古さに限る（ファイルにこの語があるもの）
    },
    {
        "id": "ttm_sum",
        "name": "TTM（直近4四半期の合計）の作り方",
        "patterns": [r"def\s+_?\w*(calc|calculate|compute|build)_ttm\w*\s*\(", r"rolling\(\s*4\s*\)\.sum\(", r"function\s+\w*(ttm|Ttm|TTM)\w*\s*\("],
        "context": r"sec_data|10-Q|quarterly",
    },
    {
        "id": "quarter_label",
        "name": "期の名前（四半期のラベル）の付け方",
        "patterns": [r"def\s+_?\w*quarter_label\w*\s*\(", r"function\s+\w*(quarterLabel|QuarterLabel|periodLabel)\w*\s*\("],
    },
    {
        "id": "fiscal_year",
        "name": "会計年度・会計年度末の決め方",
        "patterns": [r"def\s+_?\w*(determine_fiscal_year|detect_fiscal_\w+|get_fiscal_year_end|fiscal_year_of)\w*\s*\("],
    },
]

# FINDINGS 2026-10-15 の読み手0件の6種（F3）。場所は grep で出す
F3_NAMES = ["spac_shell_detection_log", "share_unit_fix_log", "low_base_effect", "fields_missing", "maturity_monitor", "market_data_violations_log"]

# 人・AIの注記（ふつうの言葉の1文）。機械の一覧とは別の欄。キーはパスの型
ANNOTATIONS: Dict[str, str] = {
    "common/sec_data/data/{T}/company_facts.json": "SECから取った、その銘柄の全XBRLの申告の生データ",
    "common/sec_data/data/{T}/submissions.json": "SECから取った、その銘柄の提出書類の一覧（提出日・対象期末）",
    "common/sec_data/data/{T}/submissions_legacy_{CIK}.json": "社名・CIKが変わった銘柄の、旧CIKの提出書類の一覧",
    "common/sec_data/data/{T}/company_facts_legacy_{CIK}.json": "社名・CIKが変わった銘柄の、旧CIKのXBRLの生データ",
    "common/sec_data/data/{T}/annual_{YYYY}.json": "生データから1年分ずつ取り出した年次の財務（損益・貸借・CF）",
    "common/sec_data/data/{T}/quarterly_{Q}.json": "生データから1四半期分ずつ取り出した四半期の財務",
    "common/sec_data/normalized/{T}_quarterly_normalized.json": "四半期の財務を項目ごとの時系列に並べ直し、Q4の逆算を足したもの",
    "common/sec_data/ttm/{T}_ttm_series.json": "直近4四半期の合計（TTM）の時系列",
    "common/sec_data/store_v2/{T}.json": "Layer3（項目ごとの四半期・年次の値と提出日）をファイルに保存したもの",
    "common/sec_data/data/_freshness.json": "銘柄ごとのSECデータの新しさ（REFRESHED・SEC_LAG・FAILED等）",
    "common/sec_data/data/_cik_cache.json": "銘柄とCIKの対応の控え",
    "common/sec_data/dimension_aggregate_registry.json": "セグメント等の内訳タグを合計して使う銘柄・項目の登録簿",
    "docs/common/sec_data/rotce/{T}.json": "銘柄ごとのROTCE（有形自己資本利益率）の計算結果",
    "docs/common/sec_data/rotce/_summary.json": "ROTCEの全銘柄のまとめ",
    "${RUNNER_TEMP}/sec_update_result.json": "SEC_Data_Updateの1回の実行の結果（成功・失敗の銘柄）。後の段の通知が読む",
    "common/sec_data/data/{T}/fy_collision_log.json": "同じ会計年度に2つの年次が当たった記録（解析の内部の記録）",
    "common/sec_data/data/{T}/fy_tag_mismatch_log.json": "申告の会計年度のタグと期末日が合わない記録（解析の内部の記録）",
    "common/sec_data/data/{T}/fye_boundary_collision_log.json": "会計年度末の境目で期がぶつかった記録（解析の内部の記録）",
    "common/sec_data/data/{T}/share_unit_fix_log.json": "株式数の単位（千株・株）の取り違えを直した記録",
    "common/sec_data/data/{T}/spac_shell_detection_log.json": "SPAC（上場のための空の会社）の時期の値を外した記録",
    "common/sec_data/data/{T}/bs_identity_violations_log.json": "貸借対照表の資産＝負債＋資本が合わない期の記録",
    "${GITHUB_STEP_SUMMARY}": "GitHub Actionsの実行の画面に出す要約（人が見る）",
    "Discord（DISCORD_WEB_HOOK）": "SECデータの監査の結果のDiscordへの通知",
    "config/cik_lookup.csv": "登録銘柄の一覧（CIK・状態・各システムに載せるかの印）。新規登録の検証で状態を書き換える",
}

# 段階2の判断（残す／消す）。Koichi さんの判断を写す欄で、機械の一覧とは別。書いていないものは DECISION_DEFAULT
DECISION_DEFAULT = "残す（2026-10-20）"
DECISIONS: Dict[str, str] = {
    # 段階2で消すと決めたが、変更の前の grep で読み手・言及が見つかり、チャット側が改めて判断したもの（2026-10-20）
    "common/sec_data/data/{T}/submissions_legacy_{CIK}.json":
        "残す（2026-10-20。fetcher.py の _fetch_legacy_submissions が旧CIKの取得の24時間キャッシュと、取得に失敗したときの代わりとして読む）",
    "scripts/analysis/market_pulse_redesign.py":
        "残す（2026-10-20。Market Pulse の作り直しの検討中。src/market/market_pulse/stage_conclusions.py が同じ値を使う）",
}

# 段階2で消したもの（2026-10-20）。一覧からは消え、ここに記録だけ残す
DELETED: List[Tuple[str, str]] = [
    ("common/sec_data/data/{T}/share_unit_fix_log.json", "書き手 parser.py:SECParser._save_share_unit_fix_log と呼び出し、既存15ファイル。単位を直す処理は残す"),
    ("common/sec_data/data/{T}/spac_shell_detection_log.json", "書き手 parser.py:SECParser._save_spac_shell_detection_log と呼び出し、既存98ファイル。SPACの時期の値を外す処理は残す"),
    ("common/sec_data/store_v2/{T}.json", "layer3_builder.py の save_ticker_store・build_and_save・手動のCLI・STORE_V2_DIR、.gitignore の行。その場で計算する読み手は変えない"),
    ("scripts/analysis の14ファイル", "macro_alfred_lib・macro_claims_slot_rows_repair・macro_events_known_at_repair・macro_events_revision_repair・macro_events_slot_rows_repair・"
     "macro_liquidity_m2_yoy_repair・macro_liquidity_row_date_repair・macro_liquidity_weekly_before_after・macro_m4_hollow_rally・macro_m4_m2_percentile・"
     "macro_m4_score_vs_recession・macro_m4_thresholds・macro_m5_threshold_simulation・macro_ticker_history_check（.py）"),
    ("scripts/analysis/futures_roll_suspects.py と src/market/market_pulse/backfill_futures_roll.py",
     "組で削除（2026-10-20、保留の決着）。一過性の修復の道具で、互いの参照のほかに読み手0件。付けた印（roll_suspect 等）のデータは残る"),
    ("scripts/analysis/macro_cfnai_ma3_rows_repair.py",
     "削除（2026-10-20、保留の決着）。修復は 70793c2185（2026-10-03）で実行済み。browser_checks/check_macro_pulse.py の N-07 の説明の文から名前を消した"),
]

# 規則の二重のうち既知のもの（CHECK-INV-3 で WARN。SEC層のファイルを含むものでこれ以外が二重になったら NG）
KNOWN_DUPLICATE_RULES = ["q4_implied", "latest_filed", "staleness", "ttm_sum", "fiscal_year"]

PLACEHOLDER_BY_NAME = [
    (re.compile(r"(^|_)(ticker|tickers|tk|sym|symbol)($|_)|^t$|^T$", re.I), "{T}"),
    (re.compile(r"(^|_)(year|yr|fy)($|_)|^y$", re.I), "{YYYY}"),
    (re.compile(r"(^|_)(quarter|qtr|period)($|_)|^q$", re.I), "{Q}"),
    (re.compile(r"cik", re.I), "{CIK}"),
]

CODE_EXT = (".py", ".js", ".html", ".yml", ".yaml")


# ---------------------------------------------------------------- 共通

def git_ls_files(*args: str) -> List[str]:
    out = subprocess.run(["git", "ls-files", *args], cwd=REPO, capture_output=True, text=True, encoding="utf-8", check=True).stdout
    return [line for line in out.splitlines() if line]


def read_text(rel: str) -> str:
    try:
        with open(os.path.join(REPO, rel), encoding="utf-8") as f:
            return f.read()
    except (OSError, UnicodeDecodeError):
        return ""


def is_test(rel: str) -> bool:
    base = posixpath.basename(rel)
    return rel.startswith("tests/") or "/tests/" in rel or base.startswith("test_") or base.endswith("_test.py")


def reader_type(rel: str) -> str:
    if is_test(rel):
        return "テスト"
    if rel.startswith(".github/workflows/"):
        return "ワークフロー"
    if rel.endswith((".js", ".html")):
        return "JS・HTML"
    return "Python"


def code_files() -> List[str]:
    files = []
    for rel in git_ls_files():
        if not rel.endswith(CODE_EXT):
            continue
        if rel.startswith(("venv/", "docs/architecture/")) or "/node_modules/" in rel:
            continue
        if rel.endswith((".yml", ".yaml")) and not rel.startswith(".github/workflows/"):
            continue
        if rel == "scripts/inventory/build_output_inventory.py":
            continue
        files.append(rel)
    return sorted(files)


def module_rel_to_dotted(rel: str) -> str:
    return rel[:-3].replace("/", ".") if rel.endswith(".py") else rel


# ---------------------------------------------------------------- AST: モジュールの情報とパスの解決

class Module:
    def __init__(self, rel: str, tree: ast.Module, src: str):
        self.rel = rel
        self.tree = tree
        self.src = src
        self.consts: Dict[str, List[ast.AST]] = defaultdict(list)
        self.funcs: Dict[str, ast.AST] = {}
        self.classes: Dict[str, ast.ClassDef] = {}
        self.from_imports: Dict[str, Tuple[str, str]] = {}   # local -> (module rel, name)
        self.mod_imports: Dict[str, str] = {}                # local -> module rel
        self.parents: Dict[ast.AST, ast.AST] = {}
        self.skip_lines: Set[int] = set()                    # docstring・コメントの行
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                self.parents[child] = node
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        self.consts[tgt.id].append(node.value)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
                self.consts[node.target.id].append(node.value)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.funcs[node.name] = node
            elif isinstance(node, ast.ClassDef):
                self.classes[node.name] = node
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                base = resolve_import_module(rel, node.module, node.level)
                for a in node.names:
                    local = a.asname or a.name
                    sub = f"{base}/{a.name}.py" if base else None
                    if sub and sub in ALL_PY:
                        self.mod_imports[local] = sub
                    elif base and f"{base}.py" in ALL_PY:
                        self.from_imports[local] = (f"{base}.py", a.name)
                    elif base and f"{base}/__init__.py" in ALL_PY:
                        self.from_imports[local] = (f"{base}/__init__.py", a.name)
            elif isinstance(node, ast.Import):
                for a in node.names:
                    cand = a.name.replace(".", "/") + ".py"
                    if cand in ALL_PY:
                        self.mod_imports[a.asname or a.name] = cand
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                body = getattr(node, "body", [])
                if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant) \
                        and isinstance(body[0].value.value, str):
                    self.skip_lines.update(range(body[0].lineno, (body[0].end_lineno or body[0].lineno) + 1))
        for i, line in enumerate(src.splitlines(), 1):
            if line.lstrip().startswith("#"):
                self.skip_lines.add(i)

    def enclosing_func(self, node: ast.AST) -> Optional[ast.AST]:
        cur = self.parents.get(node)
        while cur is not None:
            if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return cur
            cur = self.parents.get(cur)
        return None

    def enclosing_class(self, node: ast.AST) -> Optional[ast.ClassDef]:
        cur = self.parents.get(node)
        while cur is not None:
            if isinstance(cur, ast.ClassDef):
                return cur
            cur = self.parents.get(cur)
        return None

    def qualname(self, node: ast.AST) -> str:
        names = []
        cur = node if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) else self.enclosing_func(node)
        while cur is not None:
            if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(cur.name)
            cur = self.parents.get(cur)
        return ".".join(reversed(names)) if names else "<module>"


ALL_PY: Set[str] = set()
MODULES: Dict[str, Module] = {}
CALLERS: Dict[Tuple[str, str], List[Tuple[str, int]]] = defaultdict(list)   # (モジュール, 関数の qualname) -> 呼び出し箇所
DEF_INDEX: Dict[str, List[Tuple[str, str]]] = defaultdict(list)            # 関数名 -> [(モジュール, qualname)]
CALL_FUNC: Dict[Tuple[str, int], str] = {}                                  # (モジュール, 行) -> 呼び出し元の関数の qualname
ALT = " || "   # 引数を呼び出し元から解決したときの、複数のパスの区切り


def resolve_import_module(rel: str, module: Optional[str], level: int) -> Optional[str]:
    if level:
        base = posixpath.dirname(rel)
        for _ in range(level - 1):
            base = posixpath.dirname(base)
        return posixpath.join(base, module.replace(".", "/")) if module else base
    return module.replace(".", "/") if module else None


def load_module(rel: str) -> Optional[Module]:
    if rel in MODULES:
        return MODULES[rel]
    src = read_text(rel)
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    MODULES[rel] = Module(rel, tree, src)
    return MODULES[rel]


UNRESOLVED = "<?>"


def placeholder_for(expr: ast.AST) -> str:
    name = None
    cur = expr
    while True:
        if isinstance(cur, ast.Call):
            cur = cur.func.value if isinstance(cur.func, ast.Attribute) else (cur.args[0] if cur.args else None)
        elif isinstance(cur, ast.Attribute):
            name = name or cur.attr
            cur = cur.value
        elif isinstance(cur, ast.Subscript):
            sl = cur.slice
            if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
                name = sl.value
            cur = cur.value
        elif isinstance(cur, ast.Name):
            name = cur.id if name is None or name in ("upper", "lower", "strip") else name
            break
        else:
            break
    if name in (None, "upper", "lower", "strip") and isinstance(cur, ast.Name):
        name = cur.id
    if not name:
        return "{x}"
    for pat, ph in PLACEHOLDER_BY_NAME:
        if pat.search(name):
            return ph
    return "{" + name + "}"


def resolve(expr: ast.AST, mod: Module, use_node: ast.AST, depth: int = 0) -> str:
    """パスの式を、パスの型の文字列にする。解決できない部分は <?> を含む。"""
    if depth > 12 or expr is None:
        return UNRESOLVED
    r = lambda e: resolve(e, mod, use_node, depth + 1)  # noqa: E731
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return expr.value
    if isinstance(expr, ast.JoinedStr):
        parts = []
        for v in expr.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                inner = r(v.value)
                parts.append(inner if UNRESOLVED not in inner and "/" in inner else placeholder_for(v.value))
        return "".join(parts)
    if isinstance(expr, ast.Name):
        if expr.id == "__file__":
            return mod.rel
        func = mod.enclosing_func(use_node)
        # 入れ子の関数の中で代入も引数も無い名前は、外側の関数で探す（クロージャ）
        while func is not None and not any(
                (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == expr.id for t in n.targets))
                for n in ast.walk(func)) and not any(a.arg == expr.id for a in func.args.args + func.args.kwonlyargs):
            outer = mod.enclosing_func(func)
            if outer is None:
                break
            use_node, func = func, outer
        # 関数の中の代入（使う行より前で最も近いもの）
        if func is not None:
            best = None
            for n in ast.walk(func):
                if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == expr.id for t in n.targets):
                    if n.lineno < getattr(use_node, "lineno", 10 ** 9) and (best is None or n.lineno > best.lineno):
                        best = n
                elif isinstance(n, (ast.For, ast.comprehension)) and isinstance(n.target, ast.Name) and n.target.id == expr.id:
                    return placeholder_for(expr)
            if best is not None:
                return resolve(best.value, mod, best, depth + 1)
            args = func.args
            for a, d in zip(args.args[len(args.args) - len(args.defaults):], args.defaults):
                if a.arg == expr.id and not (isinstance(d, ast.Constant) and d.value is None):
                    return r(d)
            if any(a.arg == expr.id for a in args.args + args.kwonlyargs):
                if any(p.search(expr.id) for p, _ in PLACEHOLDER_BY_NAME):
                    return placeholder_for(expr)
                return resolve_param_from_callers(func, expr.id, mod, depth)
        if expr.id in mod.consts:
            cands = [r(v) for v in mod.consts[expr.id]]
            good = [c for c in cands if UNRESOLVED not in c]
            return good[-1] if good else cands[-1]
        if expr.id in mod.from_imports:
            src_rel, name = mod.from_imports[expr.id]
            other = load_module(src_rel)
            if other is not None and name in other.consts:
                return resolve(other.consts[name][-1], other, other.consts[name][-1], depth + 1)
        return placeholder_for(expr) if any(p.search(expr.id) for p, _ in PLACEHOLDER_BY_NAME) else UNRESOLVED
    if isinstance(expr, ast.Attribute) and expr.attr == "parent":
        inner = r(expr.value)
        return UNRESOLVED if UNRESOLVED in inner else posixpath.dirname(inner)
    if isinstance(expr, ast.Subscript) and isinstance(expr.value, ast.Attribute) and expr.value.attr == "parents"             and isinstance(expr.slice, ast.Constant) and isinstance(expr.slice.value, int):
        inner = r(expr.value.value)
        if UNRESOLVED in inner:
            return UNRESOLVED
        for _ in range(expr.slice.value + 1):
            inner = posixpath.dirname(inner)
        return inner
    if isinstance(expr, ast.Attribute):
        if isinstance(expr.value, ast.Name) and expr.value.id == "self":
            cls = mod.enclosing_class(use_node)
            if cls is not None:
                cands = []
                for n in ast.walk(cls):
                    if isinstance(n, ast.Assign):
                        for t in n.targets:
                            if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id == "self" and t.attr == expr.attr:
                                cands.append(resolve(n.value, mod, n, depth + 1))
                good = [c for c in cands if UNRESOLVED not in c]
                if good:
                    return good[-1]
            return UNRESOLVED
        if isinstance(expr.value, ast.Name) and expr.value.id in mod.mod_imports:
            other = load_module(mod.mod_imports[expr.value.id])
            if other is not None and expr.attr in other.consts:
                return resolve(other.consts[expr.attr][-1], other, other.consts[expr.attr][-1], depth + 1)
        return UNRESOLVED
    if isinstance(expr, ast.BinOp):
        if isinstance(expr.op, ast.Div):
            return posixpath.join(r(expr.left), r(expr.right))
        if isinstance(expr.op, ast.Add):
            return r(expr.left) + r(expr.right)
        return UNRESOLVED
    if isinstance(expr, ast.BoolOp) and isinstance(expr.op, ast.Or):
        cands = [r(v) for v in expr.values]
        good = [c for c in cands if UNRESOLVED not in c]
        return good[0] if good else cands[-1]
    if isinstance(expr, ast.IfExp):
        a, b = r(expr.body), r(expr.orelse)
        return a if UNRESOLVED not in a else b
    if isinstance(expr, ast.Subscript):
        if isinstance(expr.value, ast.Attribute) and expr.value.attr == "environ":
            sl = expr.slice
            if isinstance(sl, ast.Constant):
                return "${" + str(sl.value) + "}"
        return placeholder_for(expr)
    if isinstance(expr, ast.Call):
        f = expr.func
        fname = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
        fbase = f.value.id if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) else ""
        if fname == "join" and (fbase in ("path", "os", "posixpath") or isinstance(f.value, ast.Attribute)):
            parts = [r(a) for a in expr.args]
            return posixpath.join(*parts) if parts else UNRESOLVED
        if fname in ("dirname",):
            inner = r(expr.args[0]) if expr.args else UNRESOLVED
            return UNRESOLVED if inner == UNRESOLVED else posixpath.dirname(inner)
        if fname in ("abspath", "realpath", "normpath", "expanduser", "str", "Path", "resolve", "fspath"):
            if expr.args:
                return r(expr.args[0])
            if isinstance(f, ast.Attribute):
                return r(f.value)
            return UNRESOLVED
        if fname == "get" and isinstance(f, ast.Attribute) and isinstance(f.value, ast.Attribute) and f.value.attr == "environ":
            if expr.args and isinstance(expr.args[0], ast.Constant):
                return "${" + str(expr.args[0].value) + "}"
        if fname == "getenv" and expr.args and isinstance(expr.args[0], ast.Constant):
            return "${" + str(expr.args[0].value) + "}"
        if fname == "format" and isinstance(f, ast.Attribute):
            return re.sub(r"\{[^}]*\}", "{x}", r(f.value))
        if fname in ("upper", "lower", "strip") and isinstance(f, ast.Attribute):
            return r(f.value)
        # 返り値が1つのモジュールの関数（default_path() 等）
        target = None
        if isinstance(f, ast.Name) and f.id in mod.funcs:
            target = (mod, mod.funcs[f.id])
        elif isinstance(f, ast.Name) and f.id in mod.from_imports:
            src_rel, name = mod.from_imports[f.id]
            other = load_module(src_rel)
            if other is not None and name in other.funcs:
                target = (other, other.funcs[name])
        if target is not None:
            m2, fn = target
            rets = [n for n in ast.walk(fn) if isinstance(n, ast.Return) and n.value is not None]
            vals = {resolve(n.value, m2, n, depth + 1) for n in rets}
            vals.discard(UNRESOLVED)
            # 環境変数だけで上書きする返り値（${X}）は、ほかに決まったパスがあれば除く
            if len(vals) > 1:
                vals = {v for v in vals if not re.fullmatch(r"\$\{[A-Z_]+\}", v)} or vals
            if len(vals) == 1:
                return vals.pop()
        return UNRESOLVED
    return UNRESOLVED


def resolve_param_from_callers(func: ast.AST, pname: str, mod: Module, depth: int) -> str:
    """引数のパスを、呼び出し元の引数の式から解決する（呼び出し元ごとに違えば ALT で並べる）。"""
    if depth > 8:
        return UNRESOLVED
    args = func.args
    names = [a.arg for a in args.args]
    is_method = bool(names) and names[0] in ("self", "cls")
    idx = names.index(pname) if pname in names else None
    if idx is not None and is_method:
        idx -= 1
    vals: List[str] = []
    for (c_rel, c_line) in CALLERS.get((mod.rel, mod.qualname(func)), []):
        cm = MODULES.get(c_rel)
        if cm is None:
            continue
        for call in (n for n in ast.walk(cm.tree) if isinstance(n, ast.Call) and n.lineno == c_line):
            fn = call.func.attr if isinstance(call.func, ast.Attribute) else (call.func.id if isinstance(call.func, ast.Name) else "")
            if fn != func.name and not (isinstance(call.func, ast.Name) and cm.from_imports.get(call.func.id, ("", ""))[1] == func.name):
                continue
            arg = next((k.value for k in call.keywords if k.arg == pname), None)
            if arg is None and idx is not None and idx < len(call.args) and not any(isinstance(a, ast.Starred) for a in call.args[:idx + 1]):
                arg = call.args[idx]
            if arg is None:
                continue
            v = resolve(arg, cm, call, depth + 2)
            vals.extend(v.split(ALT))
    good = sorted({normalize_path(v) for v in vals if UNRESOLVED not in v})
    if good and len(good) == len({v for v in vals}):
        return ALT.join(good)
    if good:
        return ALT.join(good + [UNRESOLVED])
    return UNRESOLVED


def normalize_path(p: str) -> str:
    if ALT in p:
        return ALT.join(normalize_path(x) for x in p.split(ALT))
    if p.startswith("${"):
        return p
    p = p.replace("\\", "/")
    # リポジトリの根だけが決まらないパス（<?>/common/... 等）は、根からのパスとして扱う
    m = re.match(r"^(?:<\?>/)+((?:common|docs|config|src|data|scripts|discover)/.*)$", p)
    if m:
        p = m.group(1)
    if p.startswith(REPO.replace("\\", "/")):
        p = p[len(REPO) + 1:]
    p = posixpath.normpath(p) if p else p
    return "" if p == "." else p.lstrip("./") if p.startswith("./") else p


# ---------------------------------------------------------------- 書き込み・読み込みの場所

WRITE_MODES = re.compile(r"[wax]")


def _mode_of(call: ast.Call, pos: int) -> Optional[str]:
    if len(call.args) > pos and isinstance(call.args[pos], ast.Constant) and isinstance(call.args[pos].value, str):
        return call.args[pos].value
    for kw in call.keywords:
        if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
            return str(kw.value.value)
    return None


def io_sites(mod: Module) -> Tuple[List[dict], List[dict]]:
    """(書き込み, 読み込み) の場所。各 {line, func, path, kind}"""
    writes, reads = [], []
    for node in ast.walk(mod.tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        fname = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
        site = {"line": node.lineno, "func": mod.qualname(node)}
        if fname == "open" and (isinstance(f, ast.Name) or (isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in ("io", "codecs"))):
            if not node.args:
                continue
            mode = _mode_of(node, 1) or "r"
            path = normalize_path(resolve(node.args[0], mod, node))
            (writes if WRITE_MODES.search(mode) else reads).append({**site, "path": path, "kind": "ファイル"})
        elif fname == "open" and isinstance(f, ast.Attribute):     # Path(...).open("w")
            mode = _mode_of(node, 0) or "r"
            path = normalize_path(resolve(f.value, mod, node))
            (writes if WRITE_MODES.search(mode) else reads).append({**site, "path": path, "kind": "ファイル"})
        elif fname in ("write_text", "write_bytes") and isinstance(f, ast.Attribute):
            writes.append({**site, "path": normalize_path(resolve(f.value, mod, node)), "kind": "ファイル"})
        elif fname in ("read_text", "read_bytes") and isinstance(f, ast.Attribute):
            reads.append({**site, "path": normalize_path(resolve(f.value, mod, node)), "kind": "ファイル"})
        elif fname in ("to_csv", "to_json", "to_parquet") and node.args:
            writes.append({**site, "path": normalize_path(resolve(node.args[0], mod, node)), "kind": "ファイル"})
        elif fname in ("read_json", "read_csv", "read_parquet") and node.args:
            reads.append({**site, "path": normalize_path(resolve(node.args[0], mod, node)), "kind": "ファイル"})
        elif fname in ("replace", "move", "copy", "copyfile", "copy2") and isinstance(f, ast.Attribute) \
                and isinstance(f.value, ast.Name) and f.value.id in ("os", "shutil") and len(node.args) >= 2:
            writes.append({**site, "path": normalize_path(resolve(node.args[1], mod, node)), "kind": "ファイル"})
        elif (fname == "post" and isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in ("requests", "session", "s")) \
                or fname == "urlopen":
            env = sorted(set(re.findall(r"[\"']((?:DISCORD|WEBHOOK)[A-Z_]*)[\"']", mod.src)))
            if env:
                writes.append({**site, "path": "Discord（" + "・".join(env) + "）", "kind": "通知"})
    def split(sites: List[dict]) -> List[dict]:
        out = []
        for s in sites:
            for p in s["path"].split(ALT):
                out.append({**s, "path": p, "from_callers": ALT in s["path"]})
        return out
    writes, reads = split(writes), split(reads)
    for w in writes:
        if w["path"] == "${GITHUB_STEP_SUMMARY}":
            w["kind"] = "Step Summary"
    return writes, reads


# ---------------------------------------------------------------- パスの型 → 正規表現・grep の式

PH_RE = re.compile(r"\{[^}]*\}")


def template_regex(tmpl: str) -> re.Pattern:
    out, pos = "", 0
    for m in PH_RE.finditer(tmpl):
        out += re.escape(tmpl[pos:m.start()])
        ph = m.group(0)
        out += r"\d{4}" if ph == "{YYYY}" else r"[^/]+"
        pos = m.end()
    out += re.escape(tmpl[pos:])
    return re.compile("^" + out + "$")


GENERIC_BASES = {"_summary", "summary", "index", "latest", "data", "result", "results", "", "state"}


def needles_for(tmpl: str) -> Tuple[List[re.Pattern], Optional[str]]:
    """(ファイル名の grep の式, 一緒に必要なフォルダの語)。"""
    base = posixpath.basename(tmpl)
    stem = re.sub(r"\.(json|csv|txt|md)$", "", base)
    lits = [p for p in PH_RE.split(stem)]
    lit_core = "".join(lits).strip("_")
    ext = base[len(stem):]
    pats = []
    if len(PH_RE.sub("", stem)) > 0:
        body = r"[^\"'\s]{0,40}?".join(re.escape(p) for p in lits if p)
        pats.append(re.compile(body + re.escape(ext)))
        first = next((p for p in lits if p), "")
        if len(first) >= 6:
            pats.append(re.compile(r"[\"'`]" + re.escape(first) + r"[\"'`]"))
    need_dir = None
    if lit_core in GENERIC_BASES or len(lit_core) < 6:
        parts = [p for p in posixpath.dirname(tmpl).split("/") if p and not PH_RE.search(p)]
        need_dir = parts[-1] if parts else None
        if not pats:
            pats.append(re.compile(re.escape(ext) if ext else r"$^"))
    return pats, need_dir


# ---------------------------------------------------------------- 呼び出し（import を解決した呼び出し先）

def calls_in(mod: Module) -> List[Tuple[int, str, str]]:
    """(行, 呼び出し先のモジュール rel, 関数名) の一覧。"""
    out = []
    for node in ast.walk(mod.tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Name):
            if f.id in mod.from_imports:
                src_rel, name = mod.from_imports[f.id]
                out.append((node.lineno, src_rel, name))
            elif f.id in mod.funcs:
                out.append((node.lineno, mod.rel, f.id))
        elif isinstance(f, ast.Attribute):
            if isinstance(f.value, ast.Name) and f.value.id in mod.mod_imports:
                out.append((node.lineno, mod.mod_imports[f.value.id], f.attr))
            elif isinstance(f.value, ast.Name) and f.value.id in mod.from_imports:   # クラスの静的な呼び出し等
                src_rel, name = mod.from_imports[f.value.id]
                out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
            elif isinstance(f.value, ast.Call) and isinstance(f.value.func, ast.Name) and f.value.func.id in mod.from_imports:
                src_rel, name = mod.from_imports[f.value.func.id]               # SECReader().get_x()
                out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
            elif isinstance(f.value, ast.Name):
                # 変数に入れたインスタンス（r = SECReader(); r.get_x()）
                func = mod.enclosing_func(node)
                scope = func if func is not None else mod.tree
                for n in ast.walk(scope):
                    if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == f.value.id for t in n.targets) \
                            and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name) and n.value.func.id in mod.from_imports:
                        src_rel, name = mod.from_imports[n.value.func.id]
                        out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
                        break
                    if isinstance(n, ast.Assign) and any(isinstance(t, ast.Attribute) and t.attr == f.value.id for t in n.targets):
                        pass
            elif isinstance(f.value, ast.Attribute) and isinstance(f.value.value, ast.Name) and f.value.value.id == "self":
                # self.reader.get_x(): クラスの中で self.reader = SECReader() としているもの
                cls = mod.enclosing_class(node)
                if cls is not None:
                    for n in ast.walk(cls):
                        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Attribute) and t.attr == f.value.attr for t in n.targets) \
                                and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name) and n.value.func.id in mod.from_imports:
                            src_rel, name = mod.from_imports[n.value.func.id]
                            out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
                            break
            if isinstance(f.value, ast.Name) and f.value.id in ("self", "cls"):
                cls = mod.enclosing_class(node)
                if cls is not None and any(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == f.attr for n in cls.body):
                    out.append((node.lineno, mod.rel, f"{cls.name}.{f.attr}"))
                    continue
            # ほかで解決できないメソッドの呼び出し: リポジトリで1か所だけに定義された名前（8文字以上）ならそれとみなす
            if not any(ln == node.lineno and nm.split(".")[-1] == f.attr for ln, _r, nm in out):
                defs = DEF_INDEX.get(f.attr, [])
                if len(defs) > 1:   # 属性の呼び出しなので、メソッドの定義が1つならそれ
                    defs = [d for d in defs if "." in d[1]]
                if len(defs) == 1 and len(f.attr) >= 8:
                    out.append((node.lineno, defs[0][0], defs[0][1]))
    return out


# ---------------------------------------------------------------- 本体

def build() -> dict:
    for g in (ALL_PY, MODULES, CALLERS, DEF_INDEX, CALL_FUNC):   # 2回目の呼び出しで積み増さない
        g.clear()
    files = code_files()
    ALL_PY.update(f for f in git_ls_files("*.py"))
    py_files = [f for f in files if f.endswith(".py")]
    texts = {f: read_text(f) for f in files}
    for f in py_files:
        load_module(f)

    layer_py = [f for f in py_files if f.startswith(LAYER + "/") and not is_test(f)]
    for rel in py_files:
        mod = MODULES.get(rel)
        if mod is not None and not is_test(rel):
            for n in ast.walk(mod.tree):
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    DEF_INDEX[n.name].append((rel, mod.qualname(n)))
    for rel in py_files:
        mod = MODULES.get(rel)
        if mod is not None:
            for line, src_rel, name in calls_in(mod):
                CALLERS[(src_rel, name)].append((rel, line))
                call = next((n for n in ast.walk(mod.tree) if isinstance(n, ast.Call) and n.lineno == line), None)
                CALL_FUNC[(rel, line)] = mod.qualname(call) if call is not None else "<module>"

    # 1. 書き手
    outputs: Dict[str, dict] = {}
    unresolved_writes = []
    for rel in layer_py:
        mod = MODULES.get(rel)
        if mod is None:
            continue
        writes, _ = io_sites(mod)
        for w in writes:
            if UNRESOLVED in w["path"]:
                unresolved_writes.append({"file": rel, "line": w["line"], "func": w["func"], "path": w["path"]})
                continue
            o = outputs.setdefault(w["path"], {"path": w["path"], "kind": w["kind"], "writers": [], "writers_outside_layer": []})
            o["writers"].append({"file": rel, "func": w["func"], "line": w["line"]})
    # common/sec_data/ の外の書き手（止める条件）
    outside_writes = []
    for rel in py_files:
        if rel.startswith(LAYER + "/") or is_test(rel):
            continue
        mod = MODULES.get(rel)
        if mod is None or "sec_data" not in mod.src:
            continue
        writes, _ = io_sites(mod)
        for w in writes:
            if w["path"].startswith(LAYER + "/"):
                outside_writes.append({"file": rel, "line": w["line"], "func": w["func"], "path": w["path"]})
                for key, o in outputs.items():
                    if template_regex(key).match(w["path"]) or key == w["path"]:
                        o["writers_outside_layer"].append({"file": rel, "func": w["func"], "line": w["line"]})

    tracked = git_ls_files()
    workflows = [f for f in tracked if f.startswith(".github/workflows/") and f.endswith((".yml", ".yaml"))]

    # 書き手の関数の範囲（自分の読み書きを読み手から除く）
    def func_range(rel: str, qual: str) -> Tuple[int, int]:
        mod = MODULES[rel]
        for n in ast.walk(mod.tree):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and mod.qualname(n) == qual:
                return n.lineno, n.end_lineno or n.lineno
        return 0, 0

    # Python の読み込みの場所（全ファイル）
    py_reads: Dict[str, List[dict]] = {}
    for rel in py_files:
        mod = MODULES.get(rel)
        if mod is None:
            continue
        _, reads = io_sites(mod)
        py_reads[rel] = reads
    reads_into_layer = [(rel, r) for rel, rs in py_reads.items() for r in rs if r["path"].startswith(LAYER) or UNRESOLVED in r["path"]]

    # 起動元（ワークフロー）: 書き手のファイル、または書き手の関数を呼ぶファイルを参照するワークフロー
    def workflows_running(rel: str) -> List[str]:
        dotted = module_rel_to_dotted(rel)
        return sorted(w for w in workflows if rel in texts.get(w, "") or dotted in texts.get(w, ""))

    callers_index = CALLERS

    def importers_of(rel: str) -> List[str]:
        out = set()
        for other in py_files:
            mod = MODULES.get(other)
            if mod is None or other == rel:
                continue
            if rel in mod.mod_imports.values() or any(v[0] == rel for v in mod.from_imports.values()):
                out.add(other)
        return sorted(out)

    importers_cache: Dict[str, List[str]] = {}

    def importers(rel: str) -> List[str]:
        if rel not in importers_cache:
            importers_cache[rel] = importers_of(rel)
        return importers_cache[rel]

    # docs/ に書く Python のファイル → docs/ の出力の型
    docs_writes_cache: Dict[str, List[str]] = {}

    def docs_writes(rel: str) -> List[str]:
        if rel not in docs_writes_cache:
            mod = MODULES.get(rel)
            ws = io_sites(mod)[0] if mod is not None else []
            docs_writes_cache[rel] = sorted({w["path"] for w in ws if w["path"].startswith("docs/") and UNRESOLVED not in w["path"]})
        return docs_writes_cache[rel]

    pages = [f for f in tracked if f.startswith("docs/") and f.endswith((".html", ".js")) and not f.startswith("docs/architecture/")]

    def pages_reading(tmpl: str) -> List[str]:
        pats, need_dir = needles_for(tmpl)
        hits = []
        for p in pages:
            t = texts.get(p) or read_text(p)
            texts[p] = t
            if any((need_dir is None or need_dir in line) and any(rx.search(line) for rx in pats) for line in t.splitlines()):
                hits.append(p)
        return hits

    fd_text = read_text(FIELD_DEFINITIONS)
    fd_rows = [line for line in fd_text.splitlines() if re.match(r"^\|\s*AS-IS-\d+", line)]

    for key, o in sorted(outputs.items()):
        o["writers"].sort(key=lambda w: (w["file"], w["line"]))
        # 起動元: 書き手の関数から呼び出し元を5段までたどり、そのファイルを参照するワークフロー
        launched: Set[str] = set()
        frontier_f = [(w["file"], w["func"]) for w in o["writers"]]
        seen_f: Set[Tuple[str, str]] = set()
        for _ in range(6):
            nxt_f = []
            for (fr, fq) in frontier_f:
                if (fr, fq) in seen_f or is_test(fr):
                    continue
                seen_f.add((fr, fq))
                if fq == "<module>":
                    continue
                for (c_rel, c_line) in callers_index.get((fr, fq), []):
                    nxt_f.append((c_rel, CALL_FUNC.get((c_rel, c_line), "<module>")))
            frontier_f = nxt_f
        launched.update(wf for (fr, fq) in seen_f if not is_test(fr) for wf in workflows_running(fr))
        o["launched_by"] = sorted(launched)
        if o["kind"] != "ファイル" or key.startswith("${"):
            o.update({"files_tracked": None, "top_keys": [], "readers": [], "readers_compute": [], "screens": [], "definitions": []})
            if key.startswith("${") and key != "${GITHUB_STEP_SUMMARY}":
                o["kind"] = "一時ファイル"
                o["files_tracked"] = None
            else:
                o["readers"] = [{"file": "（人）", "line": 0, "type": "人", "how": "画面・通知を見る", "via": ""}]
                o["decision_effect"] = []
                continue
        else:
            rx = template_regex(key)
            matched = sorted(f for f in tracked if rx.match(f))
            o["files_tracked"] = len(matched)
            keys: Dict[str, int] = {}
            for f in ([matched[0], matched[len(matched) // 2], matched[-1]] if matched else []):
                try:
                    with open(os.path.join(REPO, f), encoding="utf-8") as fh:
                        d = json.load(fh)
                except (OSError, ValueError):
                    continue
                if isinstance(d, dict):
                    for k in d:
                        keys[k] = keys.get(k, 0) + 1
            o["top_keys_sampled_files"] = min(3, len(matched))
            o["top_keys"] = [{"key": k} for k in sorted(keys)]

        # 2. 読み手（保存済みファイル）
        pats, need_dir = needles_for(key)
        own = {(w["file"], w["func"]) for w in o["writers"]}
        own_ranges = [(w["file"], *func_range(w["file"], w["func"])) for w in o["writers"] if w["func"] != "<module>"]
        readers: Dict[Tuple[str, int], dict] = {}
        tmpl_rx = template_regex(key) if not key.startswith("${") else None

        def in_own(rel: str, line: int) -> bool:
            return any(rel == f and a <= line <= b for f, a, b in own_ranges)

        for rel, rs in py_reads.items():
            for r_ in rs:
                p = r_["path"]
                if (tmpl_rx is not None and tmpl_rx.match(p)) or p == key:
                    if not in_own(rel, r_["line"]):
                        readers[(rel, r_["line"])] = {"file": rel, "line": r_["line"], "func": r_["func"], "how": "保存済みファイル", "via": "AST"}
        for rel in files:
            t = texts.get(rel, "")
            if not t or (need_dir and need_dir not in t):
                continue
            mod = MODULES.get(rel) if rel.endswith(".py") else None
            for i, line in enumerate(t.splitlines(), 1):
                if mod is not None and i in mod.skip_lines:
                    continue
                s = line.strip()
                if s.startswith(("//", "<!--", "#")) and not rel.endswith(".py"):
                    continue
                if (need_dir is None or need_dir in line) and any(rx.search(line) for rx in pats):
                    if in_own(rel, i):
                        continue
                    func = ""
                    if mod is not None:
                        node = next((n for n in ast.walk(mod.tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                                     and n.lineno <= i <= (n.end_lineno or n.lineno) and not any(
                                         isinstance(c, (ast.FunctionDef, ast.AsyncFunctionDef)) and c.lineno <= i <= (c.end_lineno or c.lineno)
                                         for c in ast.iter_child_nodes(n))), None)
                        func = mod.qualname(node) if node is not None else "<module>"
                    readers.setdefault((rel, i), {"file": rel, "line": i, "func": func, "how": "保存済みファイル", "via": "grep"})
        # 経由（3段まで）: 読み手の関数を呼ぶ箇所、それを呼ぶ箇所…
        frontier_r = [(r_["file"], r_["func"], f"{r_['file']}::{r_['func']}") for r_ in readers.values()
                      if r_["file"].endswith(".py") and r_["func"] and r_["func"] != "<module>"]
        seen_r: Set[Tuple[str, str]] = set()
        for _ in range(VIA_DEPTH):
            nxt_r = []
            for (fr, fq, origin) in frontier_r:
                if (fr, fq) in seen_r:
                    continue
                seen_r.add((fr, fq))
                for (c_rel, c_line) in callers_index.get((fr, fq), []):
                    if in_own(c_rel, c_line):
                        continue
                    cq = CALL_FUNC.get((c_rel, c_line), "<module>")
                    readers.setdefault((c_rel, c_line), {"file": c_rel, "line": c_line, "func": cq, "how": "保存済みファイル", "via": origin})
                    if cq != "<module>":
                        nxt_r.append((c_rel, cq, origin))
            frontier_r = nxt_r
        # 3. その場で計算: 書き手の関数に渡す値を作る関数
        producers: Set[Tuple[str, str]] = set()
        for w in o["writers"]:
            wname = w["func"].split(".")[-1]
            for (c_rel, c_line) in callers_index.get((w["file"], wname), []):
                mod = MODULES[c_rel]
                call = next((n for n in ast.walk(mod.tree) if isinstance(n, ast.Call) and n.lineno == c_line), None)
                func = mod.enclosing_func(call) if call is not None else None
                if call is None or func is None:
                    continue
                argnames = {a.id for a in call.args if isinstance(a, ast.Name)}
                for n in ast.walk(func):
                    if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and any(isinstance(t, ast.Name) and t.id in argnames for t in n.targets):
                        for (ln, s_rel, name) in calls_in(mod):
                            if ln == n.lineno and s_rel.startswith(LAYER + "/"):
                                producers.add((s_rel, name))
        compute = {}
        for (p_rel, p_name) in sorted(producers):
            for (c_rel, c_line) in callers_index.get((p_rel, p_name), []):
                mod = MODULES[c_rel]
                call = next((n for n in ast.walk(mod.tree) if isinstance(n, ast.Call) and n.lineno == c_line), None)
                caller_func = mod.qualname(call) if call is not None else ""
                if (c_rel, caller_func) in own or any(c_rel == w["file"] for w in o["writers"]) and c_rel.startswith(LAYER):
                    # 書き手と同じファイルの中で書き手のために作る呼び出しは除く
                    if any(c_rel == w["file"] for w in o["writers"]):
                        continue
                compute[(c_rel, c_line)] = {"file": c_rel, "line": c_line, "func": caller_func, "how": "その場で計算", "via": f"{p_rel}::{p_name}"}
        # 書き手のパイプライン（common/sec_data の書き手の起動元）の呼び出しは「書く側」なので除く
        writer_callers = {c for w in o["writers"] for (c, _l) in callers_index.get((w["file"], w["func"].split(".")[-1]), [])}
        compute = {k: v for k, v in compute.items() if v["file"] not in writer_callers}

        def finish(lst: Iterable[dict]) -> List[dict]:
            out = []
            for r_ in sorted(lst, key=lambda x: (x["file"], x["line"])):
                r_["type"] = reader_type(r_["file"])
                out.append(r_)
            return out

        o["readers"] = finish(readers.values())
        o["readers_compute"] = finish(compute.values())
        o["producers"] = [f"{a}::{b}" for a, b in sorted(producers)]

        # 4. 画面
        screens = {p: "直接" for p in pages_reading(key)} if not key.startswith("${") else {}
        # 経由: 読み手（呼び出しを3段までたどった箇所を含む）の Python のファイルが docs/ に書くファイルを読むページ
        for rel in sorted({r_["file"] for r_ in o["readers"] + o["readers_compute"] if r_["type"] == "Python"}):
            for dw in docs_writes(rel):
                for p in pages_reading(dw):
                    if p.endswith(".html"):
                        screens.setdefault(p, f"経由（{rel} → {dw}）")
        o["screens"] = [{"page": p, "how": h} for p, h in sorted(screens.items())]

        # 5. 定義
        ids = []
        for row in fd_rows:
            if (need_dir is None or need_dir in row) and any(rx.search(row) for rx in pats):
                m = re.match(r"^\|\s*(AS-IS-\d+)", row)
                if m:
                    ids.append(m.group(1))
        o["definitions"] = sorted(set(ids))

        # 最上位のキーの読み手（読み手のファイルに、キーが文字列か属性で出てくる数）
        reader_files = sorted({r_["file"] for r_ in o["readers"] + o["readers_compute"]})
        for k in o["top_keys"]:
            krx = re.compile(r"[\"'`]" + re.escape(k["key"]) + r"[\"'`]|\." + re.escape(k["key"]) + r"\b")
            k["reader_files"] = sum(1 for f in reader_files if krx.search(texts.get(f, "")))

        # 投資判断に効くか（読み手のパスの前方一致）
        eff = []
        for prefix, label in DECISION_PREFIXES:
            if any(r_["file"].startswith(prefix) and r_["type"] != "テスト" for r_ in o["readers"] + o["readers_compute"]):
                eff.append(label)
        o["decision_effect"] = eff

    # 読み手の数と並びの区分
    for o in outputs.values():
        prod = {r_["file"] for r_ in o.get("readers", []) if r_["type"] not in ("テスト",)}
        tests = {r_["file"] for r_ in o.get("readers", []) if r_["type"] == "テスト"}
        comp = {r_["file"] for r_ in o.get("readers_compute", []) if r_["type"] != "テスト"}
        o["reader_count"] = len(prod)
        o["reader_count_test"] = len(tests)
        o["reader_count_compute"] = len(comp)
        if o["kind"] in ("Step Summary", "通知"):
            o["group"] = "人が見る"
        elif not prod and tests:
            o["group"] = "テストだけ"
        elif not prod:
            o["group"] = "読み手0件"
        elif len(prod) == 1:
            o["group"] = "1件"
        else:
            o["group"] = "2件以上"
        o["note"] = ANNOTATIONS.get(o["path"], "")
        o["decision"] = DECISIONS.get(o["path"], DECISION_DEFAULT)

    # 6. 規則
    rules_out = []
    for rule in RULES:
        sites = []
        for rel in files:
            if is_test(rel) or not rel.endswith((".py", ".js", ".html")):
                continue
            if rule.get("context") and not re.search(rule["context"], texts.get(rel, "")):
                continue
            mod = MODULES.get(rel) if rel.endswith(".py") else None
            for i, line in enumerate(texts.get(rel, "").splitlines(), 1):
                if mod is not None and i in mod.skip_lines:
                    continue
                if any(re.search(p, line) for p in rule["patterns"]):
                    sites.append({"file": rel, "line": i, "text": line.strip()[:120]})
        files_hit = sorted({s["file"] for s in sites})
        rules_out.append({"id": rule["id"], "name": rule["name"], "patterns": rule["patterns"], "sites": sites,
                          "files": files_hit, "in_layer": any(f.startswith(LAYER + "/") for f in files_hit),
                          "duplicated": len(files_hit) >= 2})

    # 7. 本番外のコード
    nonprod = []
    prod_code = [f for f in files if f.endswith(".py") and not is_test(f)]
    test_code = [f for f in files if is_test(f)]
    basename_count: Dict[str, int] = defaultdict(int)
    for f in ALL_PY:
        basename_count[posixpath.basename(f)] += 1

    def code_lines(f: str) -> List[str]:
        """コメント・docstring を除いた行。"""
        mod = MODULES.get(f) if f.endswith(".py") else None
        out = []
        for i, line in enumerate(texts.get(f, "").splitlines(), 1):
            if mod is not None and i in mod.skip_lines:
                continue
            if f.endswith((".yml", ".yaml")) and line.lstrip().startswith("#"):
                continue
            out.append(line)
        return out
    for d in NON_PRODUCTION_DIRS:
        for rel in sorted(git_ls_files(f"{d}/*.py", f"{d}/**/*.py")):
            if is_test(rel) or "/tests/" in rel:
                continue
            stem = posixpath.splitext(posixpath.basename(rel))[0]
            base = posixpath.basename(rel)
            # 参照の式: リポジトリからのパス・モジュール名・親フォルダ付きのファイル名。ファイル名だけはリポジトリで1つのときに限る
            alts = [re.escape(rel), re.escape(module_rel_to_dotted(rel)), re.escape(posixpath.join(posixpath.basename(posixpath.dirname(rel)), base))]
            if basename_count[base] == 1:
                alts.append(r"(?<![\w-])" + re.escape(base))
            name_rx = re.compile("|".join(alts))
            same_dir_import = re.compile(r"^\s*(from|import)\s+" + re.escape(stem) + r"\b")
            # フォルダを sys.path・working-directory に足して名前だけで読む使い方: フォルダの語があり、ファイル名・import・python <ファイル名> があるもの
            dir_token = posixpath.basename(d)
            loose_rx = re.compile(r"[\"'/]" + re.escape(base) + r"[\"']|python3?\s+" + re.escape(base) + r"\b|^\s*(from|import)\s+" + re.escape(stem) + r"\b")
            wf_dir_rx = re.compile(r"working-directory:\s*" + re.escape(posixpath.dirname(rel)) + r"\s*$", re.M)

            def refs(pool: List[str]) -> List[str]:
                out = []
                for f in pool:
                    if f == rel:
                        continue
                    lines = code_lines(f)
                    if any(name_rx.search(line) for line in lines):
                        out.append(f)
                        continue
                    mod = MODULES.get(f) if f.endswith(".py") else None
                    if mod is not None and (rel in mod.mod_imports.values() or any(v[0] == rel for v in mod.from_imports.values())):
                        out.append(f)
                    elif posixpath.dirname(f) == posixpath.dirname(rel) and any(same_dir_import.search(line) for line in lines):
                        out.append(f)
                    elif f.endswith((".yml", ".yaml")):
                        if wf_dir_rx.search(texts.get(f, "")) and any(re.search(r"python3?\s+" + re.escape(base) + r"\b", line) for line in lines):
                            out.append(f)
                    elif dir_token in texts.get(f, "") and any(loose_rx.search(line) for line in lines):
                        out.append(f)
                return sorted(set(out))
            nonprod.append({
                "dir": d, "file": rel, "decision": DECISIONS.get(rel, DECISION_DEFAULT), "lines": texts.get(rel, read_text(rel)).count("\n"),
                "workflows": refs(workflows), "code": [f for f in refs(prod_code) if not f.startswith(d + "/")],
                "same_dir": [f for f in refs(prod_code) if f.startswith(d + "/")], "tests": refs(test_code),
            })

    # F3 の6種の場所
    f3 = []
    for name in F3_NAMES:
        hits = []
        for rel in files:
            if is_test(rel) or not rel.endswith((".py", ".js", ".html")):
                continue
            mod = MODULES.get(rel) if rel.endswith(".py") else None
            for i, line in enumerate(texts.get(rel, "").splitlines(), 1):
                if (mod is None or i not in mod.skip_lines) and name in line:
                    hits.append({"file": rel, "line": i})
        f3.append({"name": name, "in_layer": any(h["file"].startswith(LAYER + "/") for h in hits),
                   "output": next((k for k in outputs if name in k), None), "sites": hits})

    # 止める条件
    n_out = len(outputs)
    # 読み方を決めきれない読み込み: パスに <?> が残り、パスに sec_data を含むか、SEC層のモジュールの中のもの（テストを除く）
    undetermined_reads = [{"file": rel, "line": r_["line"], "func": r_["func"], "path": r_["path"]} for rel, r_ in reads_into_layer
                          if UNRESOLVED in r_["path"] and ("sec_data" in r_["path"] or rel.startswith(LAYER + "/")) and not is_test(rel)]
    resolved_reads = [1 for rel, r_ in reads_into_layer if UNRESOLVED not in r_["path"] and not is_test(rel)]
    outputs_with_outside_writer = [k for k, o in outputs.items() if o["writers_outside_layer"]]
    stop = {
        "outputs": n_out,
        "outputs_with_writer_outside_layer": outputs_with_outside_writer,
        "unresolved_write_sites": unresolved_writes,
        "python_read_sites_into_layer_resolved": len(resolved_reads),
        "python_read_sites_unresolved_in_files_mentioning_sec_data": undetermined_reads,
        "ratio_writer": round((len(outputs_with_outside_writer) + len(unresolved_writes)) / max(1, n_out + len(unresolved_writes)), 3),
        "ratio_reader": round(len(undetermined_reads) / max(1, len(undetermined_reads) + len(resolved_reads)), 3),
        "threshold": STOP_RATIO,
    }
    stop["hit"] = stop["ratio_writer"] > STOP_RATIO or stop["ratio_reader"] > STOP_RATIO

    return {
        "generated_by": "scripts/inventory/build_output_inventory.py",
        "layer": LAYER,
        "stage": "段階1（一覧の下書き。CHECK としてはまだ動かさない）",
        "outputs": [outputs[k] for k in sorted(outputs, key=lambda k: (["読み手0件", "テストだけ", "1件", "2件以上", "人が見る"].index(outputs[k]["group"]), k))],
        "rules": rules_out,
        "non_production_code": nonprod,
        "f3_zero_reader": f3,
        "deleted": [{"what": a, "detail": b} for a, b in DELETED],
        "stop_condition": stop,
    }


# ---------------------------------------------------------------- CHECK（report_consistency_check.py の CHECK-INV-1〜4 が使う）

def check_inventory(saved: dict, now: dict) -> Tuple[List[str], List[str]]:
    """保存した一覧（sec_layer.json）と、今のリポジトリから作った一覧を比べる。(NG, WARN) の文の一覧を返す。
    銘柄ごとではなく全体の判定。"""
    ng: List[str] = []
    warn: List[str] = []
    saved_out = {o["path"]: o for o in saved.get("outputs", [])}
    now_out = {o["path"]: o for o in now.get("outputs", [])}
    # CHECK-INV-1: 一覧に無い出力
    for path in sorted(set(now_out) - set(saved_out)):
        w = "、".join(f"{x['file']}:{x['func']}" for x in now_out[path].get("writers", []))
        ng.append(f"  [NG-INV-1 一覧に無い出力] {path}（書き手 {w}）。{OUT_JSON} に入れる（スクリプトで作り直す）か、出力をやめる")
    # CHECK-INV-2: 一覧で読み手とされているファイルが読んでいない／読み手0件の出力
    for path in sorted(set(saved_out) & set(now_out)):
        before = {r["file"] for r in saved_out[path].get("readers", []) + saved_out[path].get("readers_compute", [])}
        after = {r["file"] for r in now_out[path].get("readers", []) + now_out[path].get("readers_compute", [])}
        for f in sorted(before - after):
            ng.append(f"  [NG-INV-2 読み手が読んでいない] {path} の読み手 {f} が、今はこの出力を読んでいない。一覧を作り直すか、読み手を確かめる")
    for path, o in sorted(now_out.items()):
        if o.get("group") == "読み手0件":
            ng.append(f"  [NG-INV-2 読み手0件の出力] {path}。読み手を作るか、出力をやめる（[[OUTPUT-INVENTORY-CHECK-1]]）")
    # CHECK-INV-3: 規則の二重（SEC層のファイルを含むもの）。既知は WARN、それ以外は NG
    for r in now.get("rules", []):
        if not (r.get("duplicated") and r.get("in_layer")):
            continue
        where = "、".join(r["files"])
        if r["id"] in KNOWN_DUPLICATE_RULES:
            warn.append(f"  [WARN-INV-3 規則の二重（既知）] {r['name']}: {len(r['files'])}ファイル（{where}）")
        else:
            ng.append(f"  [NG-INV-3 規則の二重（新規）] {r['name']}: {len(r['files'])}ファイル（{where}）。直し方はチャット側が決める")
    # CHECK-INV-4: 「何か」の注記が無い出力
    for path, o in sorted(now_out.items()):
        if not o.get("note"):
            warn.append(f"  [WARN-INV-4 注記なし] {path}。scripts/inventory/build_output_inventory.py の ANNOTATIONS に1文を足す")
    return ng, warn


_RUN_CHECK_CACHE: List[Tuple[List[str], List[str]]] = []


def run_check() -> Tuple[List[str], List[str]]:
    """今のリポジトリで一覧を作り、保存した一覧と比べる。保存した一覧が無ければ NG。
    一覧を作るのに時間がかかるため、同じプロセスでは1回だけ作る（report_consistency_check.py を何度呼んでも同じ）。"""
    if not _RUN_CHECK_CACHE:
        _RUN_CHECK_CACHE.append(_run_check())
    return _RUN_CHECK_CACHE[0]


def _run_check() -> Tuple[List[str], List[str]]:
    try:
        with open(os.path.join(REPO, OUT_JSON), encoding="utf-8") as f:
            saved = json.load(f)
    except (OSError, ValueError) as e:
        return [f"  [NG-INV-1 一覧が読めない] {OUT_JSON}: {e}"], []
    return check_inventory(saved, build())


# ---------------------------------------------------------------- Markdown（Koichi さんが読む表）

def md_escape(s: str) -> str:
    return s.replace("|", "\\|")


def render_md(inv: dict) -> str:
    L = []
    outs = inv["outputs"]
    files = [o for o in outs if o["group"] != "人が見る"]
    L.append("# SECデータ層の出力の一覧（段階1の下書き）")
    L.append("")
    L.append("[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。"
             "機械が読む形は`sec_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。")
    L.append("")
    L.append("- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数")
    L.append("- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）")
    L.append("- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`があれば「効く」")
    L.append("- 残す／消す: 段階2の判断（2026-10-20、チャット側の案をKoichiさんが了承）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す")
    L.append("")
    st = inv["stop_condition"]
    n_zero = sum(1 for o in files if o["group"] == "読み手0件")
    n_test = sum(1 for o in files if o["group"] == "テストだけ")
    n_dup = sum(1 for r in inv["rules"] if r["duplicated"])
    n_dup_layer = sum(1 for r in inv["rules"] if r["duplicated"] and r["in_layer"])
    L.append(f"**件数**: 出力 {len(outs)}（ファイル {len(files)}・人が見るもの {len(outs) - len(files)}）／読み手0件 {n_zero}／テストだけ {n_test}／"
             f"1件 {sum(1 for o in files if o['group'] == '1件')}／2件以上 {sum(1 for o in files if o['group'] == '2件以上')}／規則の二重 {n_dup}（検出式 {len(inv['rules'])}種のうち。SEC層のファイルを含むもの {n_dup_layer}）")
    L.append("")
    L.append(f"**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない {len(st['outputs_with_writer_outside_layer'])}＋{len(st['unresolved_write_sites'])}件（{st['ratio_writer']:.1%}）、"
             f"読み方を決めきれない読み込み {len(st['python_read_sites_unresolved_in_files_mentioning_sec_data'])}件／解決できた読み込み {st['python_read_sites_into_layer_resolved']}件（{st['ratio_reader']:.1%}）→ "
             + ("**当たる**" if st["hit"] else "当たらない"))
    L.append("")
    L.append("## 消したもの（段階2、2026-10-20）")
    L.append("")
    for x in inv["deleted"]:
        L.append(f"- `{x['what']}`: {x['detail']}")
    L.append("")
    L.append("## 出力（読み手0件 → テストだけ → 1件 → 2件以上）")
    L.append("")
    L.append("| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for o in files:
        writers = "<br>".join(f"`{w['file'].replace(LAYER + '/', '')}:{w['func']}`" for w in o["writers"])
        if o.get("writers_outside_layer"):
            writers += "<br>外: " + "<br>".join(f"`{w['file']}:{w['func']}`" for w in o["writers_outside_layer"])
        screens = "<br>".join(f"{s['page'].replace('docs/', '')}（{'直接' if s['how'] == '直接' else '経由'}）" for s in o["screens"]) or "—"
        rc = f"{o['reader_count']}"
        if o["reader_count_test"]:
            rc += f"・テスト{o['reader_count_test']}"
        if o["reader_count_compute"]:
            rc += f"（{o['reader_count_compute']}）"
        eff = "効く: " + "・".join(o["decision_effect"]) if o["decision_effect"] else "—"
        defs = "・".join(o["definitions"]) or "—"
        files_n = f"<br>（{o['files_tracked']}ファイル）" if o.get("files_tracked") is not None else ""
        L.append(f"| {o['group']} | `{md_escape(o['path'])}`{files_n} | {md_escape(o['note'])} | {writers} | {rc} | {screens} | {md_escape(eff)} | {defs} | {md_escape(o['decision'])} |")
    L.append("")
    L.append("### 人が見るもの（Step Summary・Discord）")
    L.append("")
    L.append("| 出力 | 何か | 書き手 | 起動元のワークフロー |")
    L.append("|---|---|---|---|")
    for o in outs:
        if o["group"] != "人が見る":
            continue
        writers = "<br>".join(f"`{w['file'].replace(LAYER + '/', '')}:{w['func']}`" for w in o["writers"])
        L.append(f"| `{md_escape(o['path'])}` | {md_escape(o['note'])} | {writers} | {'<br>'.join(o['launched_by']) or '—'} |")
    L.append("")
    L.append("## 読み手の内訳")
    L.append("")
    for o in files:
        L.append(f"### `{o['path']}`（{o['group']}）")
        if o["launched_by"]:
            L.append(f"- 起動元: {'・'.join(o['launched_by'])}")
        else:
            L.append("- 起動元: ワークフローから書き手を呼ぶ参照が見つからない")
        if o["top_keys"]:
            ks = "・".join(f"`{k['key']}`({k['reader_files']})" for k in o["top_keys"][:40])
            more = f" ほか{len(o['top_keys']) - 40}" if len(o["top_keys"]) > 40 else ""
            L.append(f"- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: {ks}{more}")
        groups: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
        for r in o["readers"] + o["readers_compute"]:
            groups[(r["type"], r["how"], r["file"])].append(r)
        order = {"Python": 0, "JS・HTML": 1, "ワークフロー": 2, "テスト": 3}
        for (typ, how, f), rs in sorted(groups.items(), key=lambda kv: (order.get(kv[0][0], 9), kv[0][1], kv[0][2])):
            lines = "・".join(str(r["line"]) for r in rs[:12]) + (f" ほか{len(rs) - 12}" if len(rs) > 12 else "")
            vias = sorted({r["via"] for r in rs if r["via"] not in ("AST", "grep")})
            via = f"（経由 {'、'.join('`' + v + '`' for v in vias[:3])}{' ほか' if len(vias) > 3 else ''}）" if vias else ""
            L.append(f"- {typ}・{how}: `{f}` 行 {lines}{via}")
        for s in o["screens"]:
            if s["how"] != "直接":
                L.append(f"- 画面（{s['how']}）: `{s['page']}`")
        if not o["readers"] and not o["readers_compute"]:
            L.append("- 読み手なし")
        L.append("")
    L.append("## FINDINGS 2026-10-15 の読み手0件の6種（F3）")
    L.append("")
    L.append("| 名前 | SEC層か | この一覧の出力 | 書かれている場所（grep） |")
    L.append("|---|---|---|---|")
    for f in inv["f3_zero_reader"]:
        sites = "<br>".join(f"`{s['file']}:{s['line']}`" for s in f["sites"][:6]) + (f"<br>ほか{len(f['sites']) - 6}" if len(f["sites"]) > 6 else "")
        L.append(f"| `{f['name']}` | {'はい' if f['in_layer'] else 'いいえ（後の段階）'} | {('`' + f['output'] + '`') if f['output'] else '—'} | {sites or '—'} |")
    L.append("")
    L.append("## 規則が2か所以上にあるもの")
    L.append("")
    L.append("検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。SEC層のファイルを含むものだけでなく全部を示す。")
    L.append("")
    L.append("| 規則 | ファイル数 | 場所 | SEC層を含む |")
    L.append("|---|---|---|---|")
    for r in inv["rules"]:
        if not r["duplicated"]:
            continue
        sites = "<br>".join(f"`{s['file']}:{s['line']}`" for s in r["sites"])
        L.append(f"| {r['name']} | {len(r['files'])} | {sites} | {'はい' if r['in_layer'] else 'いいえ'} |")
    single = [r for r in inv["rules"] if not r["duplicated"]]
    if single:
        L.append("")
        L.append("1か所だけのもの: " + "・".join(f"{r['name']}（{len(r['files'])}）" for r in single))
    L.append("")
    L.append("## 本番外のコード（scripts/analysis・browser_checks・discover/stonks-silo）")
    L.append("")
    L.append("参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト")
    L.append("")
    for d in NON_PRODUCTION_DIRS:
        rows = [x for x in inv["non_production_code"] if x["dir"] == d]
        total = sum(x["lines"] for x in rows)
        L.append(f"### `{d}`（{len(rows)}ファイル・{total:,}行）")
        L.append("")
        L.append("| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |")
        L.append("|---|---|---|---|---|---|---|")
        for x in rows:
            fmt = lambda xs: "<br>".join(f"`{v}`" for v in xs) or "—"  # noqa: E731
            L.append(f"| `{x['file'].replace(d + '/', '')}` | {x['lines']:,} | {fmt(x['workflows'])} | {fmt(x['code'])} | {fmt(x['same_dir'])} | {fmt(x['tests'])} | {md_escape(x['decision'])} |")
        L.append("")
    L.append("## 止める条件の内訳")
    L.append("")
    for k in ("outputs_with_writer_outside_layer", "unresolved_write_sites", "python_read_sites_unresolved_in_files_mentioning_sec_data"):
        items = st[k]
        L.append(f"- `{k}`: {len(items)}件")
        for it in items[:30]:
            L.append(f"  - {('`' + it + '`') if isinstance(it, str) else '`' + it['file'] + ':' + str(it['line']) + '` ' + it.get('func', '') + ' → `' + it['path'] + '`'}")
    L.append("")
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="書かずに、今のファイルと同じかだけを見る")
    args = ap.parse_args()
    inv = build()
    js = json.dumps(inv, ensure_ascii=False, indent=1) + "\n"
    md = render_md(inv)
    if args.check:
        same = read_text(OUT_JSON) == js and read_text(OUT_MD) == md
        print("一覧は最新" if same else "一覧が古い（スクリプトを実行して作り直す）")
        return 0 if same else 1
    os.makedirs(os.path.join(REPO, OUT_DIR), exist_ok=True)
    with open(os.path.join(REPO, OUT_JSON), "w", encoding="utf-8", newline="\n") as f:
        f.write(js)
    with open(os.path.join(REPO, OUT_MD), "w", encoding="utf-8", newline="\n") as f:
        f.write(md)
    st = inv["stop_condition"]
    print(f"出力 {len(inv['outputs'])}、止める条件 {'当たる' if st['hit'] else '当たらない'}（書き手 {st['ratio_writer']:.1%}・読み方 {st['ratio_reader']:.1%}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
