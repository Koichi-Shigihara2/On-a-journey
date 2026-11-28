"""Market Pulse: AIの入力の文の「前日比出来高比」は、出来高が無い・0以下の指数（取得時に出来高が入っていない ^NYA 等）では出さない
（N/A も 0.00 も書かない）。出来高が入れば文に出る（2026-11-28）"""
import importlib.util
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)


def _mod():
    mp_dir = os.path.join(REPO, "src", "market", "market_pulse")
    if mp_dir not in sys.path:
        sys.path.insert(0, mp_dir)           # collect_and_send は同じフォルダの sector_rotation 等を import する
    path = os.path.join(mp_dir, "collect_and_send.py")
    spec = importlib.util.spec_from_file_location("collect_and_send_vol_test", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_volume_ratio_message_is_omitted_when_volume_is_missing_or_zero():
    m = _mod()
    assert m._volume_ratio_msg(0, 5000) == "" and m._volume_ratio_msg(5000, 0) == ""
    assert m._volume_ratio_msg(None, 5000) == "" and m._volume_ratio_msg(5000, None) == ""
    assert m._volume_ratio_msg(0, 0) == ""


def test_volume_ratio_message_returns_when_volume_is_filled():
    m = _mod()
    assert m._volume_ratio_msg(5400, 5000) == " | 前日比出来高比:1.08"


def test_format_line_and_nya_line_use_the_same_helper():
    src = open(os.path.join(REPO, "src", "market", "market_pulse", "collect_and_send.py"), encoding="utf-8").read()
    assert src.count("_volume_ratio_msg(") >= 3 and '"N/A"' not in src.split("def _volume_ratio_msg")[1].split("def format_line")[0]
    assert "vol_ratio_str" not in src
