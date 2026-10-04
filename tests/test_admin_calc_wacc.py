"""
tests/test_admin_calc_wacc.py

2026-10-04: admin.htmlのcalcWacc()（βの編集欄の横に出すWACCのプレビュー）が、
Rf（小数）とβ×ERP（%）を単位をそろえずに足していた（β=1.0で5.74%）。
RF・RM_RFの定数とcalcWacc()の式をadmin.htmlから取り出してPythonで評価する。

実行方法:
    python -m pytest tests/test_admin_calc_wacc.py -v
"""

import os
import re

import pytest

_ADMIN = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "docs", "value-monitor", "admin.html"))


def _calc_wacc():
    src = open(_ADMIN, encoding="utf-8").read()
    rf = float(re.search(r"const RF = ([0-9.]+);", src).group(1))
    rm_rf = float(re.search(r"const RM_RF = ([0-9.]+);", src).group(1))
    body = re.search(r"function calcWacc\(beta\) \{(.*?)\n\}", src, re.S).group(1)
    expr = re.search(r"return (.*)\.toFixed\(2\) \+ '%';", body).group(1)
    return lambda beta: round(eval(expr, {}, {"RF": rf, "RM_RF": rm_rf, "beta": beta}), 2)


@pytest.mark.parametrize("beta, expected", [(1.0, 10.00), (1.5, 12.85), (0.6, 7.72)])
def test_calc_wacc_percent(beta, expected):
    # wacc.pyのcalculate_wacc()と同じ Rf + β×(Rm−Rf)（Rf 4.3%・Rm 10%）を%で表示する
    assert _calc_wacc()(beta) == pytest.approx(expected)
