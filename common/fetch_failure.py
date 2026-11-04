"""取得の失敗の重大度の表と、失敗が続いた日数の記録（[[CHECK-REFERENCE-DEFINITION-1]]、2026-11-03）。

System Health の取得の失敗を数える欄（[L] MacroData の系列・[M] SecFresh の FETCH_FAILED の銘柄・
[O] Intraday の欠けた銘柄）は、重大度をこの表（judge）だけで決める:

  🔴 その取得元の全件が失敗
  🔴 同じ項目（系列・銘柄）の失敗が CONTINUED_DAYS 日（定時の実行 CONTINUED_DAYS 回）以上続く
  ⚠️ 一部の失敗で1日目
  ✅ 失敗なし

続いた日数は、書き手が残す記録 failed_since（その項目の失敗が続いている最初の実行の時刻、ISO 8601）から数える
（failed_days。暦の日付で数え、同じ日の2回目の実行は1日目のまま）。書き手は carry_failed_since で前回の記録から
引き継ぐ（[L] は common/macro_data/fetcher.py の macro_data_violations_log.json、[M] は common/sec_data/freshness.py の
_freshness.json。[O] は日ごとのファイルが残るので、前の取引日のファイルの欠けから数える）。
2026-10-25〜10-30 に FRED の全25系列の取得の失敗が6日続いても ⚠️ どまりだった（FINDINGS 2026-10-30）ための表。

取得元が取れず、前に取れた結果（予備のキャッシュ）で動いている項目は、別の行で決める（judge_stale、[[EXTERNAL-SOURCE-CHANGE-WATCH-1]]、
2026-11-04）: キャッシュが古い（各項目の上限の日数を超える等）なら ⚠️。予備で動いているので🔴にはしない。
System Health の [P] ExternalChange（外の変化・ばらつきの検知、最初はMACRO PULSEのFOMCの予定のキャッシュ）が使う。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Dict, Mapping, Optional, Tuple

CONTINUED_DAYS = 2
JST = timezone(timedelta(hours=9))   # 日数は日本時間の暦の日付で数える（System Health は JST 08:30、記録は JST・UTC が混ざる）

SEVERITY_TABLE = (
    (2, "全件が失敗"),
    (2, f"同じ項目の失敗が{CONTINUED_DAYS}日以上続く"),
    (1, "一部の失敗で1日目"),
    (0, "失敗なし"),
)


def _as_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(JST).date() if v.tzinfo else v.date()
    if isinstance(v, date):
        return v
    try:
        return _as_date(datetime.fromisoformat(str(v).replace("Z", "+00:00")))
    except ValueError:
        try:
            return date.fromisoformat(str(v)[:10])
        except ValueError:
            return None


def carry_failed_since(prev_entry: Optional[Mapping], prev_failed: bool, failed: bool, now_iso: str) -> Optional[str]:
    """今回の記録に書く failed_since。今回が失敗でなければ None（キーを書かない）。
    前回も失敗で、前回の記録に failed_since があれば引き継ぐ。無ければ（記録を足す前の失敗を含む）今回の時刻。"""
    if not failed:
        return None
    if prev_failed and prev_entry and prev_entry.get("failed_since"):
        return prev_entry["failed_since"]
    return now_iso


def failed_days(failed_since, now) -> int:
    """failed_since の日から now の日まで（両端を含む）の暦の日数。記録が無ければ1（1日目）。"""
    a, b = _as_date(failed_since), _as_date(now)
    if a is None or b is None:
        return 1
    return max(1, (b - a).days + 1)


def judge(n_total: int, failed: Mapping[str, int]) -> Tuple[int, str]:
    """(重大度 0〜2, 当てはまった表の行)。failed は 失敗した項目 → 続いた日数。"""
    if not failed:
        return 0, SEVERITY_TABLE[3][1]
    if n_total and len(failed) >= n_total:
        return 2, SEVERITY_TABLE[0][1]
    if max(failed.values()) >= CONTINUED_DAYS:
        return 2, SEVERITY_TABLE[1][1]
    return 1, SEVERITY_TABLE[2][1]


STALE_CACHE_ROW = (1, "予備のキャッシュが古い（取得元が取れず、前に取れた結果で動いている）")


def judge_stale(reasons) -> Tuple[int, str]:
    """予備のキャッシュの古さの理由（空なら古くない）から (重大度, 当てはまった表の行)"""
    return STALE_CACHE_ROW if reasons else (0, SEVERITY_TABLE[3][1])


ICON = {0: "✅", 1: "⚠️ ", 2: "🔴"}


def describe(n_total: int, failed: Mapping[str, int], unit: str) -> str:
    """通知の1行に出す文（切らない）: 件数・先頭の項目の名前を全部の形で・ほかの件数・続いた項目。
    例: 取得失敗 25/25系列（BAMLH0A0HYM2ほか24件）・全件が失敗"""
    if not failed:
        return f"全{n_total}{unit}取得成功"
    names = sorted(failed)
    head = names[0] + (f"ほか{len(names) - 1}件" if len(names) > 1 else "")
    sev, row = judge(n_total, failed)
    cont = sorted(k for k, d in failed.items() if d >= CONTINUED_DAYS)
    tail = ""
    if cont:
        tail = f"・{len(cont)}{unit}が{CONTINUED_DAYS}日以上（最長{max(failed.values())}日、{cont[0]}" + \
               (f"ほか{len(cont) - 1}件" if len(cont) > 1 else "") + "）"
    elif sev == 2:
        tail = f"・{row}"
    if sev == 2 and cont and len(failed) >= n_total:
        tail = f"・全件{tail}"
    return f"取得失敗 {len(failed)}/{n_total}{unit}（{head}）{tail}"


def days_by_item(entries: Mapping[str, Mapping], now, is_failed) -> Dict[str, int]:
    """記録（項目 → エントリ）のうち失敗した項目の、続いた日数。"""
    return {k: failed_days(e.get("failed_since"), now) for k, e in entries.items() if is_failed(e)}
