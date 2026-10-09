"""
tests/test_hypecore_realstrong_dual_impl.py

[[HYPECORE-REALSTRONG-DUAL-IMPL-1]]の回帰テスト。

修正前は detect_substage()（サーバー側）の real_strong 判定
（(rev_yoy>15 and eps_surprise>-5) or (eps_surprise>0 and rev_yoy>0) or
(rev_yoy>30 and eps_surprise>-30)）と、detail.html/index.htmlのgetRec()
（クライアント側）の独自簡略版（rev_yoy>30 AND eps_surprise>0のみ）が
別々の条件・閾値で実装されており、同一銘柄・同一月でもサーバー側
substageとクライアント側の推奨表示が矛盾しうる不整合があった。

修正後は共通関数 compute_real_strong() をサーバー側で1箇所のみ実装し、
その判定結果を{ticker}_poc.jsonの`real_strong`フィールドとして出力、
detail.html/index.htmlはその値をそのまま使う（独自の再計算を廃止）。

HTML側のJSロジックは直接実行できない（本リポジトリにJSテストランナーが
ない）ため、ソースパターンの検証（独自の閾値付き再実装が消え、
`d.real_strong`を直接参照するようになっていること）で確認する。

実行方法:
    python -m pytest tests/test_hypecore_realstrong_dual_impl.py -v
"""

import os
import sys

import pandas as pd
import pytest

_HYPECORE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "src", "value", "hypecore")
)
if _HYPECORE_DIR not in sys.path:
    sys.path.insert(0, _HYPECORE_DIR)

from hypecore import compute_real_strong, detect_substage  # noqa: E402

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_DETAIL_HTML = os.path.join(_REPO_ROOT, "docs", "value-monitor", "hypecore", "detail.html")
_INDEX_HTML = os.path.join(_REPO_ROOT, "docs", "value-monitor", "hypecore", "index.html")


def _rel_id(path):
    """テストIDをリポジトリのルートからの相対パスにする（絶対パスだと置き場所ごとにIDが変わる）"""
    return os.path.relpath(path, _REPO_ROOT).replace(os.sep, "/")


def _row(rev_yoy=None, eps_surprise=None, **extra):
    data = {"rev_yoy": rev_yoy, "eps_surprise": eps_surprise}
    data.update(extra)
    return pd.Series(data)


class TestComputeRealStrongMatchesServerConditions:
    """compute_real_strong()（サーバー側の唯一の実装）自体の判定を検証"""

    def test_standard_condition_rev_over_15_no_large_eps_miss(self):
        assert compute_real_strong(_row(rev_yoy=20, eps_surprise=-3)) is True

    def test_eps_positive_surprise_with_positive_revenue(self):
        assert compute_real_strong(_row(rev_yoy=5, eps_surprise=1)) is True

    def test_high_growth_condition_rev_over_30_moderate_eps_miss(self):
        assert compute_real_strong(_row(rev_yoy=35, eps_surprise=-20)) is True

    def test_weak_case_fails_all_conditions(self):
        assert compute_real_strong(_row(rev_yoy=10, eps_surprise=-10)) is False

    def test_client_side_old_bug_would_have_disagreed_here(self):
        """修正前のクライアント側簡略版(rev_yoy>30 AND eps_surprise>0のみ)は
        rev_yoy=20・eps_surprise=-3ではFalseだが、サーバー側の正しい判定は
        Trueになる（条件A該当）。この食い違いが本バグの実害そのもの"""
        row = _row(rev_yoy=20, eps_surprise=-3)
        server_side = compute_real_strong(row)
        old_client_side_bug = bool(
            (row["rev_yoy"] is not None and row["rev_yoy"] > 30) and
            (row["eps_surprise"] is not None and row["eps_surprise"] > 0)
        )
        assert server_side is True
        assert old_client_side_bug is False
        assert server_side != old_client_side_bug  # 不整合の実例


class TestDetectSubstageUsesSharedRealStrong:
    def test_bottoming_detected_when_real_strong_true(self):
        """detect_substage()の底打ち兆候判定がcompute_real_strong()経由の
        real_strongを正しく使っていること（既存挙動の非回帰確認）"""
        row = _row(
            rev_yoy=20, eps_surprise=-3,
            ma200_dev_local=10, from_peak=-20, rsi=50, price_mom3m=0,
            ma200_mom=-2, price_iv_ratio=1.0, forward_pe=20,
        )
        result = detect_substage(row, stage=4, stage_months=3)
        assert result["label"] == "底打ち兆候"


class TestFrontendUsesServerComputedField:
    """detail.html/index.htmlが独自のreal_strong再計算をやめ、
    JSON出力のd.real_strongをそのまま使っていることをソース上で確認する。"""

    @pytest.mark.parametrize("path", [_DETAIL_HTML, _INDEX_HTML], ids=_rel_id)
    def test_old_client_side_reimplementation_is_gone(self, path):
        with open(path, encoding="utf-8") as f:
            content = f.read()
        # 修正前のバグパターン: rev_yoy>30 && eps_surprise>0 のみの独自再実装
        assert "d.rev_yoy!=null&&d.rev_yoy>30" not in content
        assert "d.eps_surprise!=null&&d.eps_surprise>0" not in content

    @pytest.mark.parametrize("path", [_DETAIL_HTML, _INDEX_HTML], ids=_rel_id)
    def test_uses_server_computed_real_strong_field(self, path):
        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert "const real_strong=d.real_strong" in content
