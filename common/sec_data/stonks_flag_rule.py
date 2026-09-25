"""
stonks_siloフラグの機械判定（[[FLAG-THRESHOLD-DESIGN-1]]案C、2026-09-25採用）

基準: TTM営業利益 < 0 または TTM売上 = 0（プレレベニュー）→ stonks_silo=true

判定値はcommon/sec_data/ttm/{TICKER}_ttm_series.jsonの最新TTM（series[0]）を使う。
report_consistency_check.py（CHECK-51）とregistration_validator.py（P7、新規
登録時）の両方から本モジュールのjudge_stonks_silo()を呼ぶ（判定ロジックを
1か所に集約）。

案A（TTM純利益<0 or TTM FCF<0 or 売上0）・案B（直近5年の年次純利益N年以上
赤字）との比較（2026-09-25、全102銘柄）で案Cを採用した理由:
  - 純利益は営業外損益で符号が逆転する銘柄が多い（ESTC・IOT・SITM・ONDS・
    LYFT・LITE）。営業利益ならその歪みを受けない
  - FCFは大型設備投資で一時的にマイナスになる黒字企業（AMZN・COHR）を
    誤って拾う
  - 現行フラグとの不一致が最少（案C 6件、案A 9件、案B N=3 9件）で、直近
    8四半期の判定反転も少ない
適用外・判定不能の扱い:
  - config/stonks_flag_rule.jsonのexcluded（SOFI: 金融機関）は判定しない
  - TTMの営業利益・売上が取れない銘柄は判定不能（expected=None）とし、
    警告の対象外にする（現行フラグを維持）。既知の理由はknown_undeterminable
    に注記する（ASTS・XOM・SN）
  - TTM売上がNone/0でも、年次売上に実績がある銘柄は「データ欠損」であり
    プレレベニューとは扱わない（SN型）。年次売上も全年None/0の場合のみ
    プレレベニュー（APGE型）としてtrue
"""

import glob
import json
import os
from typing import Any, Dict, Optional

_REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
_TTM_DIR = os.path.join(_REPO_ROOT, "common", "sec_data", "ttm")
_DATA_DIR = os.path.join(_REPO_ROOT, "common", "sec_data", "data")
_CONFIG_PATH = os.path.join(_REPO_ROOT, "config", "stonks_flag_rule.json")


def resolve_stonks_flag_rule_config_path() -> str:
    """config/stonks_flag_rule.jsonの絶対パス（report_consistency_check.py
    CHECK-34の_CONFIG_LOADER_REGISTRYから存在確認される）"""
    return _CONFIG_PATH


def load_rule_config(path: Optional[str] = None) -> Dict[str, Any]:
    with open(path or resolve_stonks_flag_rule_config_path(), encoding="utf-8") as f:
        cfg = json.load(f)
    return {
        "excluded": cfg.get("excluded") or {},
        "known_undeterminable": cfg.get("known_undeterminable") or {},
    }


def _latest_ttm(ticker: str, ttm_dir: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(ttm_dir, f"{ticker}_ttm_series.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            series = json.load(f).get("series") or []
    except Exception:
        return None
    return series[0] if series else None


def _is_pre_revenue(ticker: str, data_dir: str) -> bool:
    """年次売上が全年None/0ならプレレベニュー（年次ファイル自体がなければFalse）"""
    files = sorted(glob.glob(os.path.join(data_dir, ticker, "annual_*.json")))
    if not files:
        return False
    for p in files:
        try:
            with open(p, encoding="utf-8") as f:
                rev = (json.load(f).get("pl") or {}).get("revenue")
        except Exception:
            return False
        if rev not in (None, 0):
            return False
    return True


def judge_stonks_silo(ticker: str, ttm_dir: str = _TTM_DIR, data_dir: str = _DATA_DIR,
                      config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """案Cでstonks_siloの期待値を判定する。

    Returns:
        {"ticker", "expected": True/False/None,
         "state": "judged" | "excluded" | "undeterminable",
         "reason": str, "ttm_end": str|None,
         "operating_income": float|None, "revenue": float|None}
    """
    cfg = config if config is not None else load_rule_config()
    t = ticker.upper()
    res: Dict[str, Any] = {"ticker": t, "expected": None, "state": "undeterminable",
                           "reason": "", "ttm_end": None, "operating_income": None, "revenue": None}
    if t in cfg["excluded"]:
        res.update(state="excluded", reason=cfg["excluded"][t])
        return res

    ttm = _latest_ttm(t, ttm_dir)
    note = cfg["known_undeterminable"].get(t, "")
    if ttm is None:
        res["reason"] = note or "TTM系列なし"
        return res
    flow = ttm.get("flow") or {}
    oi = (flow.get("operating_income") or {}).get("val")
    rev = (flow.get("revenue") or {}).get("val")
    res.update(ttm_end=ttm.get("ttm_end"), operating_income=oi, revenue=rev)

    if rev in (None, 0):
        if _is_pre_revenue(t, data_dir):
            res.update(expected=True, state="judged", reason="TTM売上=0（プレレベニュー）")
        else:
            res["reason"] = note or "TTM売上欠損（年次売上には実績あり）"
        return res
    if oi is None:
        res["reason"] = note or "TTM営業利益欠損"
        return res
    res.update(expected=oi < 0, state="judged",
               reason=f"TTM営業利益{'<' if oi < 0 else '>='}0")
    return res
