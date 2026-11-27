"""[[MARKETPULSE-REVIEW-EPIC-1]] 順5（2-c）: 「買い」の主は BUY チェックリスト、自前の指数のシグナル・Tech Pulse は参考。
画面の文言は出力の rule_text（定数から作る）を読む。閾値を変えたら文言も変わることを確かめる"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "market" / "market_pulse"))
import collect_and_send as cs


def test_rule_texts_come_from_the_constants(monkeypatch):
    assert str(cs.BUY_TRIGGER_ENTER) in cs.BUY_RULE_TEXT and str(cs.BUY_TRIGGER_EXIT) in cs.BUY_RULE_TEXT
    assert str(cs.SENTIMENT_BUY_MAX) in cs.SENTIMENT_SIGNAL_RULE_TEXT and "参考" in cs.SENTIMENT_SIGNAL_RULE_TEXT
    assert "参考" in cs.TP_SIGNAL_RULE_TEXT and str(cs.TP_REBOUND_FG_MAX) in cs.TP_SIGNAL_RULE_TEXT
    assert "主" in cs.BUY_RULE_TEXT


def test_outputs_carry_rule_text():
    c = cs.calc_buy_checklist(20.0, False, False, 3.5, 3.9, False)
    assert c["rule_text"] == cs.BUY_RULE_TEXT and c["extreme"] is False
    assert cs.calc_buy_checklist(10.0, False, False, 3.5, 3.9, False)["extreme"] is True
    sg = cs.sentiment_signal(15.0, [("2026-11-01", 50.0)], "2026-10-30")
    assert sg["signal"] == "BUY" and sg["rule_text"] == cs.SENTIMENT_SIGNAL_RULE_TEXT
    assert cs.sentiment_signal(21.0, [], "2026-10-30")["signal"] == "HOLD"


def test_tp_signal_thresholds_unchanged():
    assert cs._get_tp_signal(11, 1.1, 29) == "ハイテク先行反発シグナル"
    assert cs._get_tp_signal(11, 1.1, 30) == ""
    assert cs._get_tp_signal(-11, -1.1, 50) == "ハイテク先行下落注意"


def test_screen_puts_buy_checklist_first_and_labels_the_others_reference():
    h = (ROOT / "docs" / "market-monitor" / "market-pulse" / "index.html").read_text(encoding="utf-8")
    i_buy, i_sig, i_tp = h.index('id="buyChecklistSec"'), h.index('id="signalBadge"'), h.index('id="tpSignalNote"')
    assert i_buy < i_sig < i_tp
    assert "買いの判断（主）" in h and "参考: センチメントのシグナル" in h and "参考: Tech Pulse のシグナル" in h
    assert "bc.rule_text" in h and "sig.rule_text" in h and "tpd?.rule_text" in h
