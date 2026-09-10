"""
common/sec_data/normalizer.py
責務: Raw TableのYTD値を単一四半期値に差分変換する
出力: normalized/{ticker}_quarterly_normalized.json
"""

import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta

from .contracts import validate_fields
from .q4_implied import build_q4_implied_entries

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(__file__)
NORMALIZED_DIR = os.path.join(BASE_DIR, "normalized")


def normalize(ticker: str, raw: dict) -> dict:
    """
    YTD値 → Q単独値への差分変換。

    - is_ytd=True のエントリを対象に、前四半期YTD値との差分を計算。
    - is_ytd=False（SA）はそのまま通す。
    - is_annual=True のエントリも変換せずそのまま保持。
    GrossProfit が欠損している場合、Revenue - _COGS で逆算を試みる。
    """
    ticker = ticker.upper()
    fields_raw = raw.get("fields", {})
    fields_norm: dict[str, list] = {}

    for field_name, entries in fields_raw.items():
        if field_name == "_COGS":
            # _COGSもYTD変換を適用してからQ4 implied計算に備える
            fields_norm[field_name] = _normalize_field(entries)
            continue
        fields_norm[field_name] = _normalize_field(entries)

    # Q4 implied計算（フロー系フィールドのみ）
    # FY年次値 - (Q1+Q2+Q3) = Q4 implied
    # Revenue・_COGSを最初に処理してGrossProfit逆算に備える
    # ※ RPO はストック値（期末残高）のため Q4 implied 対象外
    Q4_IMPLIED_FIELDS = (
        "Revenue", "_COGS",
        "OCF", "ICF", "CFF", "CapEx",
        "RD", "SM", "SBC", "DA",
        "NetIncome", "OperatingIncome",
        "GrossProfit",
    )

    # 欠落四半期逆算（中間YTD累計から。例: 上場直後で起点Q1が未申告のケース）
    # Q4逆算より先に行うことで、これにより復元されたQ1等を使って
    # 後続のQ4逆算（Q1+Q2+Q3が揃って初めて動作）も正しく機能するようにする。
    for field_name in Q4_IMPLIED_FIELDS:
        src = fields_norm.get(field_name, [])
        if not src:
            continue
        missing_list = _build_missing_quarter_implied_entries(src)
        if missing_list:
            existing_ends = {e["end"] for e in src if not e.get("is_annual")}
            added = [e for e in missing_list if e["end"] not in existing_ends]
            if added:
                fields_norm[field_name] = sorted(src + added, key=lambda x: x["end"])
                logger.debug("[%s] %s 欠落四半期逆算: %d件", ticker, field_name, len(added))

    for field_name in Q4_IMPLIED_FIELDS:
        src = fields_norm.get(field_name, [])
        if not src:
            continue
        annual_entries = [e for e in src if e.get("is_annual")]
        quarterly_entries = [e for e in src if not e.get("is_annual") and not e.get("is_ytd")]
        q4_list = build_q4_implied_entries(annual_entries, quarterly_entries, field_name)
        if q4_list:
            existing_ends = {e["end"] for e in src if not e.get("is_annual")}
            added = [e for e in q4_list if e["end"] not in existing_ends]
            if added:
                fields_norm[field_name] = sorted(src + added, key=lambda x: x["end"])
                logger.debug("[%s] %s Q4 implied added: %d entries", ticker, field_name, len(added))

    # GrossProfit逆算（欠損補完）
    fields_norm = _calc_gross_profit(fields_norm)

    # 内部フィールドは出力から除外
    fields_norm.pop("_COGS", None)

    # 逐次差分・欠落四半期逆算のいずれでも解決できなかった未解決YTD累計は、
    # 単独四半期として扱うと二重計上・誤ラベルの原因になるため出力から除外する
    # （BUG-CON-YTD-1: ttm_calculator等の下流はis_annualのみで四半期判定するため
    #  is_ytd=Trueの残骸を残すと誤って1四半期分として計上されてしまう）
    for field_name, field_entries in fields_norm.items():
        cleaned = [
            e for e in field_entries
            if e.get("is_annual") or not e.get("is_ytd")
        ]
        if len(cleaned) != len(field_entries):
            dropped = len(field_entries) - len(cleaned)
            logger.debug("[%s] %s 未解決YTD %d件を除外", ticker, field_name, dropped)
            fields_norm[field_name] = cleaned

    # CapEx符号正規化（CAPEX-SIGN-UNNORMALIZED-1）: SEC XBRLのCapExは発行体に
    # よって正負どちらの符号でも報告されるため、最終出力直前の1箇所で
    # abs()を適用する。Q4逆算（上記）より後に行うことで、直接取得エントリ・
    # Q4逆算エントリの両方を区別なく正しくカバーする。
    for entry in fields_norm.get("CapEx", []):
        if entry.get("val") is not None:
            entry["val"] = abs(entry["val"])

    logger.info("[%s] normalization done", ticker)
    return {
        "ticker": ticker,
        "generated_at": datetime.now().isoformat(),
        "fields": fields_norm,
    }


def _normalize_field(entries: list) -> list:
    """1フィールドのエントリを正規化する"""
    if not entries:
        return []

    annual = [e for e in entries if e.get("is_annual")]
    quarterly = [e for e in entries if not e.get("is_annual")]

    sa_entries = [e for e in quarterly if not e.get("is_ytd")]
    ytd_entries = [e for e in quarterly if e.get("is_ytd")]

    if not ytd_entries:
        return sorted(annual + sa_entries, key=lambda x: x["end"])

    # YTDチェーンの起点となるstart（FY開始日）を特定
    ytd_starts = {e["start"] for e in ytd_entries}

    # Q1 SA など start=FY_start のエントリもYTDチェーンに含める
    chain_entries = [e for e in quarterly if e["start"] in ytd_starts]
    passthrough_entries = [e for e in quarterly if e["start"] not in ytd_starts]

    # FY開始日でグルーピング
    by_fy_start: dict[str, list] = defaultdict(list)
    for e in chain_entries:
        by_fy_start[e["start"]].append(e)

    converted: list = []
    unresolved: list = []
    for fy_start, fy_entries in by_fy_start.items():
        sorted_entries = sorted(fy_entries, key=lambda x: x["end"])
        c, u = _ytd_to_quarterly(sorted_entries)
        converted.extend(c)
        unresolved.extend(u)

    # 同一end日付に複数候補（生SA=passthrough / YTD逐次差分=converted）が
    # 存在する場合、passthrough を優先して1件に絞る。
    # 多くの企業は同一四半期を「単独3ヶ月」と「累計6/9ヶ月」の両方でXBRLタグ付け
    # するため（例: AAPL）、両方を素通しすると同一四半期が2件生成され
    # 二重計上になる（BUG-DUP-QUARTER-1）。生SAタグの方が変換を経ない
    # 一次情報のため優先する。
    by_end: dict[str, list] = defaultdict(list)
    for e in passthrough_entries:
        by_end[e["end"]].append(("passthrough", e))
    for e in converted:
        by_end[e["end"]].append(("converted", e))

    all_quarterly: list = []
    for end_date, candidates in by_end.items():
        raw_sa = [e for kind, e in candidates if kind == "passthrough"]
        pool = raw_sa if raw_sa else [e for kind, e in candidates if kind == "converted"]
        all_quarterly.append(max(pool, key=lambda x: x.get("filed", "")))
    all_quarterly.extend(unresolved)

    all_quarterly.sort(key=lambda x: x["end"])
    return sorted(annual + all_quarterly, key=lambda x: x["end"])


def _ytd_to_quarterly(fy_entries: list) -> tuple[list, list]:
    """
    YTDエントリのリストを受け取り、差分変換した単一四半期エントリを返す。

    fy_entries: 同一FY内のエントリをend_date昇順でソート済み。
    Q1（年度最初）がSAの場合は差分なしでそのまま使用し、以降を逐次差分変換する。
    Q1がSAとして存在せず、チェーン先頭が複数四半期分のYTD（例: 起点Q1が
    未申告で最初に現れるのが9ヶ月累計等）の場合、その値自体は「1四半期分」
    としては信頼できない（BUG-CON-YTD-1）ため unresolved に分離するが、
    後続エントリとの差分計算の起点（prev_ytd）としてはそのまま使用する
    （2つの累計値の差分はどちらも同じ起点からの累計である限り、起点の
    絶対値が未確定でも常に正しい単独四半期値になるため）。

    [[NORMALIZER-YTD-METADATA-STALE-1]]対応: Q2以降のYTD差分変換エントリは
    valのみYTD差分値に更新し、start/period_daysが変換前のYTD期間（FY開始日
    起点）のまま残っていた。前四半期end翌日〜当四半期endの単四半期期間に
    再計算する（起点は「前四半期endの翌日」——SECの実際の本人申告SA
    エントリの慣例〈例: AAPL Revenue実データで確認: Q1 end=2025-12-27の
    翌日、Q2 start=2025-12-28〉と一致させるため、prev_end自体ではなく
    +1日した日付を採用する）。

    戻り値: (converted, unresolved)
    """
    converted: list = []
    unresolved: list = []
    prev_ytd: float = 0
    prev_ytd_known = False
    prev_end: str | None = None

    for entry in fy_entries:
        new_entry = dict(entry)

        if not entry.get("is_ytd"):
            # SA entry（Q1など）: そのまま使用し、累積YTDに加算
            standalone = entry["val"]
            prev_ytd += standalone
            prev_ytd_known = True
            prev_end = entry["end"]
            new_entry["val"] = standalone
            converted.append(new_entry)
            continue

        if not prev_ytd_known:
            # チェーン先頭が既にYTD＝起点(Q1相当)が未申告。
            # この値自体は単独四半期としては使えないため unresolved に回すが、
            # 以降の差分計算の起点としては採用する。
            unresolved.append(new_entry)
            prev_ytd = entry["val"]
            prev_ytd_known = True
            prev_end = entry["end"]
            continue

        # YTD entry: 前回YTDとの差分
        standalone = entry["val"] - prev_ytd
        # 異常フラグ: 前QのYTDが正値だったのに累積が減少した場合（決算期変更等）
        if prev_ytd > 0 and entry["val"] < prev_ytd:
            new_entry["anomaly"] = True
            logger.warning(
                "YTD reversal for end=%s fp=%s val=%s prev_ytd=%s",
                entry.get("end"), entry.get("fp"), entry["val"], prev_ytd,
            )

        # start/period_daysを変換後の単四半期期間（前回end翌日〜当期end）に
        # 再計算する。prev_endがパース不能等で使えない場合は元のstart/
        # period_days（YTD期間のまま）にフォールバックする（安全側）。
        new_start = None
        period_days = entry.get("period_days")
        if prev_end:
            try:
                new_start_date = date.fromisoformat(prev_end) + timedelta(days=1)
                new_start = new_start_date.isoformat()
                period_days = (date.fromisoformat(entry["end"]) - new_start_date).days
            except (ValueError, TypeError):
                new_start = None
                period_days = entry.get("period_days")

        prev_ytd = entry["val"]  # YTDは全Q累積なのでprevを上書き
        prev_end = entry["end"]
        new_entry["is_ytd"] = False
        new_entry["val"] = standalone
        if new_start:
            new_entry["start"] = new_start
        if period_days is not None:
            new_entry["period_days"] = period_days
        converted.append(new_entry)

    return converted, unresolved


# Q4 implied生成本体はcommon/sec_data/q4_implied.py::build_q4_implied_entries()
# に集約済み（[[Q4-IMPLIED-CALC-TRIPLICATION-1]]対応、移行実装計画フェーズB）。


def _build_missing_quarter_implied_entries(entries: list) -> list:
    """
    複数四半期分のYTD累計（is_ytd=True のまま未解決で残っているエントリ、
    または年次実績）から、既知の単独四半期を差し引いて、残る1四半期分の
    値を逆算する。q4_implied.py::build_q4_implied_entries（FY - Q1〜Q3 = Q4）を一般化し、
    9ヶ月累計等の中間累計からも欠落四半期（例: 上場直後のQ1）を
    復元できるようにする（BUG-CON-YTD-1対応）。

    対象スパン内にちょうど1四半期分の欠落がある場合のみ逆算する
    （複数四半期欠落・重複計上の疑いがある場合は対象外とし何もしない）。
    """
    from datetime import date as _date, timedelta as _timedelta

    cumulative_candidates = [
        e for e in entries
        if (e.get("is_annual") or e.get("is_ytd")) and e.get("val") is not None
        and e.get("start") and e.get("end")
    ]
    known_quarters = [
        e for e in entries
        if not e.get("is_annual") and not e.get("is_ytd") and e.get("val") is not None
    ]

    result: list = []
    for cand in cumulative_candidates:
        c_start, c_end, c_val = cand["start"], cand["end"], cand["val"]
        try:
            span_days = (_date.fromisoformat(c_end) - _date.fromisoformat(c_start)).days
        except (ValueError, TypeError):
            continue

        # 単一四半期未満のスパンは対象外（2四半期分=約150日以上のみ）
        if span_days < 150:
            continue
        n_quarters = round(span_days / 91)
        if n_quarters < 2:
            continue

        contained = [
            q for q in known_quarters
            if q.get("start", "") >= c_start and q.get("end", "") <= c_end
        ]
        if len(contained) != n_quarters - 1:
            continue  # ちょうど1四半期分の欠落でなければ対象外（安全側に倒す）

        contained_sorted = sorted(contained, key=lambda x: x["start"])
        try:
            covered_span = sum(
                (_date.fromisoformat(q["end"]) - _date.fromisoformat(q["start"])).days
                for q in contained_sorted
            )
        except (ValueError, TypeError):
            continue
        # 既知四半期の合計期間 + 欠落想定1四半期(約91日) がスパン全体と
        # ほぼ一致することを確認（重複・飛び地の混入を防ぐ簡易チェック）
        if abs(covered_span + 91 - span_days) > 20:
            continue

        first_known_start = contained_sorted[0]["start"]
        last_known_end = contained_sorted[-1]["end"]
        if first_known_start > c_start:
            # 欠落四半期は先頭（既知四半期群より前）。期間の非重複を保つため、
            # 終了日は次の既知四半期の開始日の前日とする（BUG-CON-YTD-3対応）。
            # ここを first_known_start のまま（1日ずれ）にすると、
            # q4_implied.py::build_q4_implied_entries が同じ四半期を end=12/31 として
            # 別途算出した際に end 不一致で重複排除できず、二重計上を生む。
            try:
                missing_end = (
                    _date.fromisoformat(first_known_start) - _timedelta(days=1)
                ).isoformat()
            except (ValueError, TypeError):
                continue
            missing_start = c_start
        elif last_known_end < c_end:
            # 欠落四半期は末尾（既知四半期群より後）。開始日は直前の既知四半期の
            # 終了日の翌日とする（同様に非重複を保つ）。
            try:
                missing_start = (
                    _date.fromisoformat(last_known_end) + _timedelta(days=1)
                ).isoformat()
            except (ValueError, TypeError):
                continue
            missing_end = c_end
        else:
            continue  # 欠落位置が中間（飛び地）等で特定できないため対象外

        missing_val = c_val - sum(q["val"] for q in contained_sorted)

        try:
            period_days = (_date.fromisoformat(missing_end) - _date.fromisoformat(missing_start)).days
        except (ValueError, TypeError):
            period_days = 90

        result.append({
            "end":         missing_end,
            "start":       missing_start,
            "val":         missing_val,
            "fp":          "implied",
            "fy":          cand.get("fy"),
            "form":        cand.get("form", ""),
            "filed":       cand.get("filed", ""),
            "accn":        cand.get("accn", ""),
            "period_days": period_days,
            "is_ytd":      False,
            "is_annual":   False,
            "is_implied":  True,
        })

    # 複数の累計候補（例: 6ヶ月YTDと9ヶ月YTDの両方）が同一の欠落四半期を
    # 独立に逆算することがあるため、(start, end) 単位で重複排除する
    # （BUG-CON-YTD-2: 重複したまま返すとcheck_revenue_quality等の四半期合計で
    #  二重計上になる）。値が食い違う場合は不整合として両方除外する。
    by_period: dict[tuple, list] = defaultdict(list)
    for e in result:
        by_period[(e["start"], e["end"])].append(e)

    deduped: list = []
    for period, candidates in by_period.items():
        vals = {round(c["val"], 2) for c in candidates}
        if len(vals) == 1:
            deduped.append(candidates[0])
        else:
            logger.warning(
                "missing quarter implied value mismatch for %s: %s",
                period, [c["val"] for c in candidates],
            )

    return deduped


def _index_quarterly_by_end(entries: list) -> dict:
    """
    非annualの四半期エントリをend日付でインデックス化する（value ではなく
    エントリ全体を保持）。

    同一end日付に複数候補が存在する場合（単独四半期値=is_ytd Falseと、
    まだ最終クリーンアップ前で残っている未解決の累積値=is_ytd Trueが
    共存するケース）、単独四半期値を優先する。単独四半期値がない場合のみ
    累積値にフォールバックし、同種同士では最新filed優先とする。
    """
    by_end: dict[str, list] = defaultdict(list)
    for e in entries:
        if e.get("is_annual"):
            continue
        by_end[e["end"]].append(e)

    result: dict[str, dict] = {}
    for end_date, candidates in by_end.items():
        sa = [c for c in candidates if not c.get("is_ytd")]
        pool = sa if sa else candidates
        result[end_date] = max(pool, key=lambda x: x.get("filed", ""))
    return result


def _calc_gross_profit(fields: dict) -> dict:
    """
    GrossProfit が欠損している四半期を Revenue - _COGS で逆算する。

    _COGS XBRL概念: CostOfRevenue（quarterly.pyで取得済み）
    """
    gp_entries = fields.get("GrossProfit", [])
    rev_entries = fields.get("Revenue", [])
    cogs_entries = fields.get("_COGS", [])

    if not cogs_entries or not rev_entries:
        return fields

    # 既存GrossProfit の end_date セット
    gp_ends = {e["end"] for e in gp_entries if not e.get("is_annual")}

    # Revenue と COGS を end_date でインデックス化
    # （BUG-CON-YTD-3対応: 最終クリーンアップ前は同一end日付に単独四半期値と
    #  未解決の累積値〈is_ytd=True〉が共存し得るため、単独四半期値を優先する。
    #  end日付のみで無条件に上書きすると累積値が誤って採用され、GrossProfitが
    #  実際の四半期値の数倍に膨らむ）
    rev_by_end = _index_quarterly_by_end(rev_entries)
    cogs_by_end = _index_quarterly_by_end(cogs_entries)

    backfilled: list = []
    for end_date, rev_entry in rev_by_end.items():
        if end_date in gp_ends:
            continue
        cogs_entry = cogs_by_end.get(end_date)
        if cogs_entry is None or rev_entry.get("val") is None or cogs_entry.get("val") is None:
            continue
        # Revenue と COGS は符号が正なので単純差分
        gp_val = rev_entry["val"] - abs(cogs_entry["val"])
        backfilled.append({
            **{k: rev_entry[k] for k in ("end", "start", "fp", "fy", "form", "filed",
                                          "period_days", "is_ytd", "is_annual")
               if k in rev_entry},
            "val": gp_val,
            "accn": rev_entry.get("accn", ""),
            "backfilled": True,
        })
        logger.debug("GrossProfit backfilled for end=%s: %s", end_date, gp_val)

    if backfilled:
        merged = sorted(
            gp_entries + backfilled,
            key=lambda x: x["end"],
        )
        fields = dict(fields)
        fields["GrossProfit"] = merged

    return fields


def save_normalized(ticker: str, normalized: dict) -> str:
    """normalized dataをJSONファイルに保存し、パスを返す

    QUALITY-GATES-EPIC-1 Phase 3a: json.dump()直前に規約B（標準エントリ形状）を
    検証する。検証結果のオブジェクトは破棄し、保存対象データ自体は変更しない。
    違反時はContractViolation（ValueErrorのサブクラス）を送出し、呼び出し元
    （update.py）の既存のper-ticker try/except Exceptionで捕捉・スキップされる。
    """
    validate_fields(normalized.get("fields", {}))
    os.makedirs(NORMALIZED_DIR, exist_ok=True)
    path = os.path.join(NORMALIZED_DIR, f"{ticker.upper()}_quarterly_normalized.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)
    logger.info("[%s] normalized saved → %s", ticker, path)
    return path
