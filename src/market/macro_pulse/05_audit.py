#!/usr/bin/env python3
"""
MACRO PULSE — 軽量整合性チェック（MACRO-NFP-1 再発防止）
========================================================
05_events.csv に対して以下2点を検出する:
  CHECK-1: 同一indicator×同一actual値の重複行（WARN）
           （refresh_monthly_indicators()のスロット判定漏れによる二重書き込みの疑い。
            IC4WSA等の移動平均系指標は正常でも同値が並ぶことがあるため要目視確認）
  CHECK-2: NFPの「水準」残存兆候（NG）
           （前月比変換されず旧PAYEMS水準のまま格納されている場合の検出）

使用方法:
  python src/market/macro_pulse/05_audit.py
  python src/market/macro_pulse/05_audit.py --fail-on-ng
"""
import os, sys, argparse
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import importlib.util, pathlib

_main_path = pathlib.Path(__file__).parent / "05_main.py"
_spec = importlib.util.spec_from_file_location("main05_audit", _main_path)
_m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_m)

load_events            = _m.load_events
load_schedule          = _m.load_schedule
INDICATOR_CONFIG       = _m.INDICATOR_CONFIG
_MONTHLY_REFRESH_SET   = _m._MONTHLY_REFRESH_SET
# [[MACRO-THRESHOLD-INCONSISTENCY-1]]対応（2026-09-10）: 本関数の実体は
# 05_main.py::dedupe_new_rows()（データ書き込み側の重複判定）でも
# 使われるため05_main.py側へ移動・単一化した。ここでは移動先を再利用する
# （二重定義による将来の判定ドリフトを防ぐ）。
_duplicate_risk_indicators = _m._duplicate_risk_indicators

# CHECK-2: NFPが「水準」のまま格納されている兆候を判定するための閾値。
# 直近実績が全てこのレンジに収まり、かつ単調非減少（一度も減少しない）場合、
# 月次雇用者数の絶対水準（現在158,000〜165,000千人程度）がそのまま
# 格納されている可能性が高いと判定する（前月比なら通常プラスマイナス双方に振れる）
_NFP_LEVEL_BAND = (100_000, 999_999)
_NFP_LEVEL_LOOKBACK = 6


def _parse_date(s: str):
    try:
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def check_duplicate_events(events: pd.DataFrame, schedule: pd.DataFrame) -> tuple[list, list]:
    """
    CHECK-1: 同一indicator×同一actual値×release_date近接の重複行を検出する。

    IC4WSA（4週MA）等は正常運用でも「隣接週で同値が続く」ことがあり、
    値の一致だけでは重複バグと区別できないため WARN 扱いとする
    （NG=機械的に確定した不整合、WARN=人間の確認が必要な疑いの区別を踏襲）。
    """
    ng: list = []
    warn: list = []
    if events.empty:
        return ng, warn

    risk_inds = _duplicate_risk_indicators(schedule)

    for ind_name, grp in events.groupby("indicator"):
        if ind_name not in risk_inds:
            continue
        # refresh_monthly_indicators()の窓判定(obs_date±14日)を踏まえ、
        # 05_main.py dedupe_new_rows()と同じ基準(lag+14日)で比較する
        lag = INDICATOR_CONFIG.get(ind_name, {}).get("obs_to_release_lag", 35) + 14
        rows = []
        for _, r in grp.iterrows():
            d = _parse_date(r.get("release_date", ""))
            try:
                v = float(r.get("actual", ""))
            except (ValueError, TypeError):
                continue
            if d is None:
                continue
            rows.append((d, v, r["event_id"]))
        rows.sort(key=lambda x: x[0])

        for i in range(1, len(rows)):
            d0, v0, eid0 = rows[i - 1]
            d1, v1, eid1 = rows[i]
            if abs(v0 - v1) < 1e-6 and abs((d1 - d0).days) <= lag:
                warn.append(
                    f"  ⚠️  CHECK-1 重複疑い: {ind_name} {eid0}({v0}) と {eid1}({v1}) "
                    f"が同一値・{(d1 - d0).days}日差（lag={lag}日以内、要目視確認）"
                )
    return ng, warn


def check_nfp_level_residue(events: pd.DataFrame) -> tuple[list, list]:
    """
    CHECK-2: NFPが前月比ではなく水準のまま格納されている兆候を検出する。

    「単調非減少」では②の重複書き込みバグ由来の順序の乱れで見逃すことがあるため、
    直近実績の値のばらつき（レンジ／平均比）が極端に小さいことを判定に使う。
    前月比（本来のNFP）は月によって符号・大きさが大きく振れるのに対し、
    雇用者数の水準はほぼ横ばいで推移するため、レンジが平均の数%未満に収まる。
    """
    ng: list = []
    warn: list = []
    nfp = events[events["indicator"] == "NFP"].copy()
    if nfp.empty:
        return ng, warn

    nfp["_date"] = nfp["release_date"].apply(_parse_date)
    nfp = nfp.dropna(subset=["_date"]).sort_values("_date")

    vals = []
    for _, r in nfp.tail(_NFP_LEVEL_LOOKBACK).iterrows():
        try:
            vals.append(float(r["actual"]))
        except (ValueError, TypeError):
            continue

    if len(vals) < _NFP_LEVEL_LOOKBACK:
        return ng, warn

    in_band = all(_NFP_LEVEL_BAND[0] <= abs(v) <= _NFP_LEVEL_BAND[1] for v in vals)
    mean_abs = sum(abs(v) for v in vals) / len(vals)
    val_range = max(vals) - min(vals)
    tight_cluster = mean_abs > 0 and (val_range / mean_abs) < 0.05

    if in_band and tight_cluster:
        ng.append(
            f"  ❌ CHECK-2 NFP水準残存疑い: 直近{_NFP_LEVEL_LOOKBACK}件が"
            f"{_NFP_LEVEL_BAND}レンジ内かつレンジ幅が平均の{val_range / mean_abs:.1%}に収束"
            f"（前月比なら通常プラスマイナス双方に大きく振れる）: {vals}"
        )
    return ng, warn


def run_checks(quiet: bool = False) -> tuple[int, int]:
    """整合性チェックを実行し (ng_count, warn_count) を返す。"""
    events = load_events()
    schedule = load_schedule()
    if not quiet:
        print(f"=== MACRO PULSE 整合性チェック ({len(events)} イベント) ===\n")

    total_ng: list = []
    total_warn: list = []

    ng, warn = check_duplicate_events(events, schedule)
    total_ng.extend(ng)
    total_warn.extend(warn)

    ng, warn = check_nfp_level_residue(events)
    total_ng.extend(ng)
    total_warn.extend(warn)

    if not total_ng and not total_warn:
        if not quiet:
            print("✅ NG=0 / 警告=0\n")
    else:
        for item in total_ng:
            print(item)
        for item in total_warn:
            print(item)
        print()

    if not quiet:
        print("─" * 50)
        print(f"合計: NG={len(total_ng)} 件 / 警告={len(total_warn)} 件")

    return len(total_ng), len(total_warn)


def parse_args():
    parser = argparse.ArgumentParser(description="MACRO PULSE 整合性チェック")
    parser.add_argument(
        "--fail-on-ng",
        action="store_true",
        help="NG件数 > 0 のとき sys.exit(1) で終了する（省略時は常にexit(0)）",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="見出し・サマリ行を表示しない（NG/WARNのみ出力）"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ng_count, warn_count = run_checks(quiet=args.quiet)

    print(f"\n{'=' * 50}")
    print(f"結果: NG={ng_count}件 / WARN={warn_count}件")

    if args.fail_on_ng and ng_count > 0:
        print("❌ ゲート失敗: NGが存在するためexit(1)で終了します")
        sys.exit(1)
