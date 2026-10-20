"""
common/sec_data/fact_selection.py
fact競合解決の共通プリミティブ（EPS-ANALYZER-NORMALIZE-SCOPE-1）

同一期間（同一end_date、または同一(start, end)）に複数のXBRL factが競合する
場合（株式分割による比較年度再掲・10-K/A訂正申告等、SEC-TAG-FICO-CPRT-1と
同型のパターン）、「filed日が最新のものを優先する」という単一規則を
common/sec_data/quarterly.py（`_process_entries`・`_select_best_filing`）と
src/value/adjusted_eps_analyzer/extract_key_facts.py（SPLIT-AUTO-CHECK-1）が
それぞれ独立に実装していたのを、この最小プリミティブに切り出した。

parser.py本体（本人データ優先・fy一致・期間近似等を含む多段規則）は対象外。
本関数はあくまで「同一期間の複数候補からfiled日最新の1件を選ぶ」という
末端の判定ロジックのみを担う。
"""
from typing import Any, Sequence


def select_latest_filed(candidates: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """同一期間内の複数候補factから、filed日が最新のものを返す。

    Args:
        candidates: 各要素が"filed"キー（文字列、ISO8601日付相当）を持つdictの列。
                     空でないことを呼び出し元が保証すること。

    Returns:
        "filed"が最大（辞書順で最新）の要素。同着の場合はcandidates内で
        最初に出現した要素を返す（Pythonのmax()は最初の最大値を保持する）。

    Raises:
        ValueError: candidatesが空の場合。
    """
    if not candidates:
        raise ValueError("select_latest_filed: candidates must not be empty")
    return max(candidates, key=lambda c: c.get("filed", ""))


# ---------------------------------------------------------------------------
# 最初の申告と、ある時点の値（[[LAYER3-FIRST-FILED-DATE-1]]、2026-10-21）
#   first_filed: その期間の値が最初に申告された書類の提出日（候補のタグ全体で最小の filed）
#   first_val:   採用したタグで、その期間に最初に申告された値
#   _asof:       採用したタグで、その期間の値の申告の履歴 [[提出日, 値], ...]（提出日の昇順）。
#                ある時点で使えた値（value_asof()）を出すためのメモリの中だけのキーで、保存の前に消す（strip_asof()）
#   filed・val は今の意味（最新の申告）のまま。派生の値（YTD からの差し引き・Q4 の逆算・合算）は、
#   first_filed を元の値の first_filed の最大、first_val・_asof を元の値から同じ計算で作る（derive()）
# ---------------------------------------------------------------------------

FIRST_KEYS = ("first_filed", "first_val")
ASOF_KEY = "_asof"
DERIVED_KEYS = FIRST_KEYS + (ASOF_KEY,)
# YTD の差し引きで、累計の始まり（まだ何も足していない）を表す。提出日の空文字はどの日付より前
ZERO_TERM: dict[str, Any] = {"first_filed": "", "first_val": 0.0, ASOF_KEY: [["", 0.0]]}


def select_first_filed(candidates: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """同一期間内の複数候補factから、filed日が最も古いものを返す（同着は先に出現したもの）。"""
    if not candidates:
        raise ValueError("select_first_filed: candidates must not be empty")
    return min(candidates, key=lambda c: c.get("filed", ""))


def asof_history(candidates: Sequence[dict[str, Any]]) -> list[list[Any]]:
    """同一期間内の候補factから、申告の履歴 [[提出日, 値], ...]（同じ提出日は後に出現したもの）。"""
    by_filed: dict[str, Any] = {}
    for c in candidates:
        by_filed[c.get("filed", "")] = c.get("val")
    return [[f, v] for f, v in sorted(by_filed.items()) if v is not None]


def value_asof(entry: dict[str, Any] | None, as_of: str) -> Any:
    """entry の、提出日が as_of 以前の申告のうち最も新しい値（無ければ None）。"""
    hist = (entry or {}).get(ASOF_KEY)
    if not hist:
        return None
    vals = [v for f, v in hist if f <= as_of]
    return vals[-1] if vals else None


def combine_first(terms: Sequence[tuple[float, dict[str, Any] | None]]) -> dict[str, Any]:
    """派生の値の最初の申告。terms は (係数, 元のエントリ) の列で、値 = Σ 係数 × 元の値 の派生に使う。

    元の値のどれかに first_filed・first_val が無ければ空の辞書（最初の申告は分からない）。"""
    if not terms:
        return {}
    filed: list[str] = []
    total = 0.0
    for coef, e in terms:
        if not e or e.get("first_val") is None or e.get("first_filed") is None:
            return {}
        filed.append(e["first_filed"])
        total += coef * e["first_val"]
    if not max(filed):
        return {}
    return {"first_filed": max(filed), "first_val": total}


def _combine_asof(terms: Sequence[tuple[float, dict[str, Any] | None]]) -> list[list[Any]]:
    """派生の値の申告の履歴。元の値の提出日の和集合の各日に、その日に使えた値どうしで同じ計算をする。"""
    if not terms or any(not e or not e.get(ASOF_KEY) for _, e in terms):
        return []
    dates = sorted({f for _, e in terms for f, _v in e[ASOF_KEY]})
    out: list[list[Any]] = []
    for d in dates:
        vals = [value_asof(e, d) for _, e in terms]
        if any(v is None for v in vals):
            continue
        out.append([d, sum(coef * v for (coef, _), v in zip(terms, vals))])
    return out


def derive(terms: Sequence[tuple[float, dict[str, Any] | None]]) -> dict[str, Any]:
    """派生の値（Σ 係数 × 元の値）の first_filed・first_val・_asof。分からないキーは入れない。"""
    out = dict(combine_first(terms))
    hist = _combine_asof(terms)
    if hist:
        out[ASOF_KEY] = hist
    return out


def abs_term(e: dict[str, Any] | None) -> dict[str, Any] | None:
    """符号をそろえる（abs）値の first_val・_asof も abs にした写し（売上原価・設備投資の符号）。"""
    if not e:
        return e
    out = dict(e)
    if out.get("first_val") is not None:
        out["first_val"] = abs(out["first_val"])
    if out.get(ASOF_KEY):
        out[ASOF_KEY] = [[f, abs(v)] for f, v in out[ASOF_KEY]]
    return out


def set_derived(entry: dict[str, Any], derived: dict[str, Any]) -> dict[str, Any]:
    """entry の first_filed・first_val・_asof を derived で置き換える（元の値から写したものを残さない）。"""
    for k in DERIVED_KEYS:
        entry.pop(k, None)
    entry.update(derived)
    return entry


def strip_asof(entries: Sequence[dict[str, Any]]) -> None:
    """保存・公開の前に、メモリの中だけのキー _asof を消す。"""
    for e in entries:
        e.pop(ASOF_KEY, None)
