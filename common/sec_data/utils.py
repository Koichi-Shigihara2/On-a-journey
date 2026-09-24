"""
共通ユーティリティ関数
"""
import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple


def compute_snapshot_hash(data: Dict[str, Any]) -> str:
    """fixed_registry.jsonのsnapshot_hash算出方式（[[SEC-DATA-REDESIGN-
    OPERATIONAL-POLICY-1]]）。annual_{year}.jsonの内容全体を
    sort_keys=Trueで正規化してからSHA-256を取る。fixed_registry.json
    生成時（Stage 1一括登録）・report_consistency_check.py CHECK-31
    （不一致検知）の両方から参照する単一実装（重複実装を避ける）。

    ticker/period/formのような不変メタ情報も含めてハッシュ化対象とする
    （これらは対象パスごとに恒久固定のため除外の必要がなく、除外ロジックを
    足すことでの実装ミスの余地を避けるため）。`parsed_at`のような揮発性
    タイムスタンプはannual_{year}.jsonファイル自体には含まれない
    （save_parsed_data()が年次ファイルへ書き込まないため）ことを実測確認
    済みで、再パース実行のたびの偽陽性は発生しない。
    """
    canonical = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# parser.py::_apply_fixed_registry_freeze()がfields_snapshotの各フィールドを
# 探索するカテゴリ順（同メソッドとCHECK-31で共有する単一定義）。
FIXED_REGISTRY_CATEGORIES = ("bs", "pl", "cf", "shares", "other")


def compute_fields_snapshot_hash(data: Dict[str, Any], fields: Sequence[str]) -> str:
    """fixed_registry.jsonの`fields_snapshot_hash`算出方式
    （[[CHECK31-WHOLE-FILE-HASH-VS-DIFF-FREEZE-1]]）。

    凍結機構（parser.py::_apply_fixed_registry_freeze()）が強制復元する
    範囲＝fields_snapshotの各フィールドについて、最初に見つかったカテゴリ・
    値・`{category}_provenance`の該当エントリのみをハッシュ化する。
    凍結機構は差分適用方式でfields_snapshot外の新規フィールドを通す設計の
    ため、ファイル全体のハッシュ（compute_snapshot_hash）で比較すると
    parser.pyへフィールドを追加しただけで凍結年度が全てNG化していた。
    フィールドが見つからない場合は`category=None`として含める（欠落も
    ハッシュ不一致として検知される）。
    """
    picked = {}
    for field in sorted(set(fields)):
        rec = {"category": None, "value": None, "provenance": None}
        for category in FIXED_REGISTRY_CATEGORIES:
            cat = data.get(category) or {}
            if field in cat:
                rec = {
                    "category": category,
                    "value": cat[field],
                    "provenance": (data.get(f"{category}_provenance") or {}).get(field),
                }
                break
        picked[field] = rec
    canonical = json.dumps(picked, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def quarters_in_trailing_window(
    quarters: Sequence[Tuple[str, Any]],
    fy_end: str,
    window_days: int = 370,
) -> list:
    """
    会計年度end日を起点に、trailing window_days日以内の四半期エントリの値を抽出する。

    暦年ラベル（end[:4]）での四半期グルーピングは非12月決算企業（KLAC/LRCX等）で
    誤判定を起こす（CHECK-QREV-FYE-1・DILUTION-FYE-1で同型バグとして独立に
    発見・修正されていた。ARCH-DATA-1残課題①として本関数へ一本化）。

    Args:
        quarters: (end日文字列, 値) のタプルのリスト（四半期エントリのみ・is_annual除外済み前提）
        fy_end: 年次end日文字列（ISO形式 "YYYY-MM-DD"）
        window_days: 窓の日数（デフォルト370日）

    Returns:
        list: window_start(=fy_end - window_days) < end <= fy_end を満たすエントリの値
              （入力の並び順を保持。ちょうど4件揃っているかの判定・合計/中央値等の
              集計方法は呼び出し元の責務とする——用途によって要件が異なるため）
    """
    fy_end_dt = date.fromisoformat(fy_end)
    window_start = fy_end_dt - timedelta(days=window_days)
    return [
        v for e, v in quarters
        if window_start < date.fromisoformat(e) <= fy_end_dt
    ]


def detect_fiscal_end_month(us_gaap: dict, candidate_keys: Sequence[str]) -> int:
    """
    10-K（form完全一致・fp=="FY"）エントリのend月から、会計年度末月を検出する
    （最頻値を返す、デフォルト12）。

    ARCH-DATA-1ステージ2: parser.py::_detect_fiscal_end_month（10-K完全一致・
    10-K/A除外）とextract_key_facts.py::determine_fiscal_year_end（10-K/A含む
    startswith判定）の2系統に分かれていた検出ロジックをここに統一する。
    10-K/Aを含めると最頻値が変わり年度バケツ計算全体に波及する回帰が
    RCATで確認されているため（ARCH-DATA-1ステージ1実装時の検証）、
    10-K完全一致（10-K/A除外）をより保守的な基準として正本にする。

    candidate_keysは'us-gaap:'プレフィックス付き・なしのいずれでも受け付ける
    （呼び出し元ごとにタグ名の記法が異なるため）。

    [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]対応（案①）: form=="10-K"・
    fp=="FY"の条件だけでは、毎年の10-Kに再掲載される「Selected Quarterly
    Financial Data（未監査）」注記等の四半期比較値も同じform/fp属性を持つため
    集計に混入し、四半期エントリの反復再掲載が多い銘柄（ELF等）で真の年次月を
    量的に上回り最頻値が反転しうる。`_collect_own_data_annual()`・
    `detect_fiscal_anchor_date()`と同一の340-380日必須フィルタを追加し、
    真に年次（duration ~365日）のエントリのみを集計対象とする。

    Args:
        us_gaap: SEC XBRLデータ（facts["us-gaap"]相当の辞書）
        candidate_keys: 優先順位順のXBRLタグ名リスト（net_income+revenue等）

    Returns:
        int: 会計年度末の月（1-12、検出不可の場合はデフォルト12）
    """
    month_counts: Dict[int, int] = {}
    for raw_key in candidate_keys:
        xbrl_key = raw_key[8:] if raw_key.startswith("us-gaap:") else raw_key
        if xbrl_key not in us_gaap:
            continue
        for entry in us_gaap[xbrl_key].get("units", {}).get("USD", []):
            if entry.get("form") == "10-K" and entry.get("fp") == "FY":
                start = entry.get("start", "")
                end = entry.get("end", "")
                if not start or not end or len(start) < 10 or len(end) < 10:
                    continue
                try:
                    start_dt = datetime.strptime(start, "%Y-%m-%d")
                    end_dt = datetime.strptime(end, "%Y-%m-%d")
                except ValueError:
                    continue
                days = (end_dt - start_dt).days
                if not (340 <= days <= 380):
                    continue
                m = end_dt.month
                month_counts[m] = month_counts.get(m, 0) + 1
        if month_counts:
            break
    return max(month_counts, key=month_counts.get) if month_counts else 12


def _day_of_year(month: int, day: int) -> int:
    """(月,日)を基準うるう年(2000年)上の年内通算日(1-366)に変換する。
    2/29も基準年が閏年のため有効な日付として扱える"""
    return date(2000, month, day).timetuple().tm_yday


def _cluster_fiscal_anchor_candidates(
    day_counts: Dict[Tuple[int, int], int],
    merge_threshold_days: int = 7,
) -> list:
    """
    (月,日)の出現回数辞書を、年境界（12/31→1/1）をまたぐ循環距離で
    クラスタリングする（ARCH-DATA-1ステージ2: JNJ/TDY型対応）。

    52/53週企業は決算日が年によって12月末〜1月頭を往復するため、
    完全一致の(月,日)最頻値だけを見ると「12/28,12/29,12/30,12/31」
    （Dec側）と「1/1,1/2,1/3」（Jan側）に票が分散し、たまたま
    Jan側の特定の1日（例: 1/1固定）が単独最多になることがある。
    これをdetermine_fiscal_year()のアンカーに使うと、年境界を挟む
    候補年度の距離計算が「Dec31基準」と「Jan1基準」で逆転し、
    企業自身のfyタグと矛盾する年度判定になる（JNJ/TDYで実データ確認）。

    通算日(1-366)でソートし、隣接する点同士の循環距離が
    merge_threshold_days以内なら同一クラスタとして併合する
    （貪欲法。年境界をまたぐ併合も許可する）。

    Returns:
        list[list[tuple[int,int,int,int]]]: クラスタのリスト。
        各クラスタは (day_of_year, month, day, count) のリスト。
        入力が空の場合は空リストを返す。
    """
    points = []
    for (m, d), cnt in day_counts.items():
        points.append((_day_of_year(m, d), m, d, cnt))
    if not points:
        return []
    points.sort()
    n = len(points)
    if n == 1:
        return [points]

    gaps = []
    for i in range(n):
        cur = points[i][0]
        nxt = points[(i + 1) % n][0]
        gap = (nxt - cur) if i < n - 1 else (nxt + 366 - cur)
        gaps.append(gap)

    cut_positions = {i for i in range(n) if gaps[i] > merge_threshold_days}
    if not cut_positions:
        return [points]  # 全点が1つの循環クラスタにまとまる

    start = (min(cut_positions) + 1) % n
    rotated = points[start:] + points[:start]
    clusters = []
    current = [rotated[0]]
    for k in range(1, n):
        prev_original_index = (start + k - 1) % n
        if prev_original_index in cut_positions:
            clusters.append(current)
            current = []
        current.append(rotated[k])
    clusters.append(current)
    return clusters


def _collect_anchor_day_counts(us_gaap: dict, candidate_keys: Sequence[str]) -> Dict[Tuple[int, int], int]:
    """
    本人10-K annualエントリ（form in ("10-K","10-K/A")・fp=="FY"・
    340〜380日の真の年次期間）のend日から、(月,日)ごとの出現回数を集計する。

    detect_fiscal_anchor_date()・detect_fiscal_anchor_clusters()の共通部分
    （ARCH-DATA-1ステージ2 [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②で
    2関数に分岐する前の走査ロジックをここへ集約）。
    """
    day_counts: Dict[Tuple[int, int], int] = {}
    for raw_key in candidate_keys:
        xbrl_key = raw_key[8:] if raw_key.startswith("us-gaap:") else raw_key
        if xbrl_key not in us_gaap:
            continue
        for unit_type in ("USD", "shares", "USD/shares"):
            for entry in us_gaap[xbrl_key].get("units", {}).get(unit_type, []):
                if entry.get("form") not in ("10-K", "10-K/A") or entry.get("fp") != "FY":
                    continue
                start = entry.get("start", "")
                end = entry.get("end", "")
                if not start or not end or len(start) < 10 or len(end) < 10:
                    continue
                try:
                    start_dt = datetime.strptime(start, "%Y-%m-%d")
                    end_dt = datetime.strptime(end, "%Y-%m-%d")
                except ValueError:
                    continue
                days = (end_dt - start_dt).days
                if not (340 <= days <= 380):
                    continue
                key = (end_dt.month, end_dt.day)
                day_counts[key] = day_counts.get(key, 0) + 1
        if day_counts:
            break
    return day_counts


def _cluster_anchor_point(cluster: list) -> Tuple[int, int]:
    """クラスタ内エントリの加重中央値（実在しない場合はクラスタ内最頻値）をアンカー日とする"""
    weighted_points = []
    for doy, m, d, cnt in cluster:
        weighted_points.extend([(doy, m, d)] * cnt)
    weighted_points.sort()
    n = len(weighted_points)
    mid = n // 2
    if n % 2 == 1:
        _, month, day = weighted_points[mid]
    else:
        lower = weighted_points[mid - 1]
        upper = weighted_points[mid]
        if lower == upper:
            _, month, day = lower
        else:
            # 中央値がクラスタ内に実在しない(月,日)になる → クラスタ内最頻値を採用
            best_point = max(cluster, key=lambda p: p[3])
            month, day = best_point[1], best_point[2]
    return (month, day)


def detect_fiscal_anchor_date(us_gaap: dict, candidate_keys: Sequence[str]) -> Optional[Tuple[int, int]]:
    """
    本人10-K annualエントリ（form in ("10-K","10-K/A")・fp=="FY"・
    340〜380日の真の年次期間）のend日から、決算アンカー日（月+日）を
    検出する（ARCH-DATA-1ステージ2: アンカー日ウィンドウ方式の入力）。

    detect_fiscal_end_month()と同じcandidate_keys・us_gaapの走査パターンを
    共有する（片方のタグでエントリが見つかった時点で走査を打ち切る）。

    JNJ/TDY型対応（2026-07-17検証で発見・循環クラスタリング方式に変更）:
    単純な(月,日)完全一致の最頻値では、年境界をまたぐ企業のend日が
    Dec側とJan側に分散し、企業のfyタグと矛盾する年度判定を引き起こす。
    _cluster_fiscal_anchor_candidates()で年境界を考慮した循環クラスタに
    まとめた上で、最大クラスタ（合計出現数が最多）を採用し、その中央値
    （中央値がクラスタ内に実在しない場合はクラスタ内最頻値）をアンカー日とする。

    Args:
        us_gaap: SEC XBRLデータ（facts["us-gaap"]相当の辞書）
        candidate_keys: 優先順位順のXBRLタグ名リスト（net_income+revenue等）

    Returns:
        (anchor_month, anchor_day): 検出したアンカー日。
        該当エントリが1件もない場合はNone（呼び出し元は
        determine_fiscal_year()に anchor_month/anchor_day=None を渡し、
        従来の月のみ比較にフォールバックさせる）
    """
    day_counts = _collect_anchor_day_counts(us_gaap, candidate_keys)
    clusters = _cluster_fiscal_anchor_candidates(day_counts)
    if not clusters:
        return None

    best_cluster = max(clusters, key=lambda c: sum(p[3] for p in c))
    return _cluster_anchor_point(best_cluster)


def detect_fiscal_anchor_clusters(
    us_gaap: dict, candidate_keys: Sequence[str], min_support: int = 2
) -> List[Tuple[int, int]]:
    """
    [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②: detect_fiscal_anchor_date()と
    同一の(月,日)クラスタリング結果のうち、主anchor（最大クラスタ）以外で
    合計得票がmin_support以上の「有意な」クラスタのアンカー日を、era別決算期
    変更の追加候補（determine_fiscal_year()のextra_anchors引数）として返す。

    単一クラスタしか存在しない銘柄（実測105銘柄中100銘柄）は必ず空リストを
    返す。これによりdetermine_fiscal_year()に渡るextra_anchorsが空リストと
    なり、既存の計算結果と数学的に完全同一になることを設計上保証する
    （新規候補を追加するだけの加算的操作とし、既存の正しい判定を上書きする
    経路を作らない）。

    Args:
        us_gaap: SEC XBRLデータ（facts["us-gaap"]相当の辞書）
        candidate_keys: 優先順位順のXBRLタグ名リスト（net_income+revenue等）
        min_support: 追加候補として採用する最小クラスタ内合計得票数

    Returns:
        list[Tuple[int, int]]: 主クラスタ以外の(anchor_month, anchor_day)候補。
        単一クラスタの場合、または他クラスタがいずれもmin_support未満の場合は空リスト
    """
    day_counts = _collect_anchor_day_counts(us_gaap, candidate_keys)
    clusters = _cluster_fiscal_anchor_candidates(day_counts)
    if len(clusters) <= 1:
        return []

    best_cluster = max(clusters, key=lambda c: sum(p[3] for p in c))
    extra: List[Tuple[int, int]] = []
    for cluster in clusters:
        if cluster is best_cluster:
            continue
        support = sum(p[3] for p in cluster)
        if support >= min_support:
            extra.append(_cluster_anchor_point(cluster))
    return extra


def determine_fiscal_year(end_date, fiscal_end_month: int,
                           anchor_month: Optional[int] = None,
                           anchor_day: Optional[int] = None,
                           extra_anchors: Optional[Sequence[Tuple[int, int]]] = None) -> int:
    """
    期末日と会計年度末月から、その期間が属する会計年度を返す。

    ARCH-DATA-1ステージ2: anchor_month/anchor_day（detect_fiscal_anchor_date()
    で検出した決算アンカー日）が指定された場合、end_date.yearを中心に
    [year-1, year, year+1] の3候補年度それぞれでアンカー日を組み立て、
    end_dateとの日数差が最小の候補年度を採用する「アンカー日ウィンドウ方式」
    で判定する。旧来の「month > fiscal_end_month」片方向月比較は、
    12月決算企業でmonth>12が恒常的にFalseとなり年またぎ補正が原理的に
    機能しない欠陥と、52/53週企業の決算日前後変動・月境界またぎを
    扱えない欠陥の両方を持っていた（FY52WEEK-BUCKET-MISPLACE-1で判明）。

    [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②: extra_anchors（detect_fiscal_
    anchor_clusters()が返す、主anchor以外の有意なクラスタのアンカー日）が
    指定された場合、主anchor（anchor_month/anchor_day）とextra_anchorsの
    全候補から、同じ365日ウィンドウ探索でグローバルに最小距離の候補年度を
    採用する（実在する決算期変更を、era（年代）ごとに正しいanchorへ機械的に
    振り分ける）。extra_anchors省略時・空リスト時は主anchor単独のウィンドウ
    探索のみとなり、本引数追加前と数学的に完全同一の計算になる。

    anchor_month/anchor_day省略時、または最小日数差が60日を超える場合
    （未知の異常パターンへの安全弁）は、従来の月のみ比較にフォールバックし
    WARNログを出力する（silent failさせない）。

    Args:
        end_date: datetime.date または datetime.datetime
        fiscal_end_month: 会計年度末の月（1-12）
        anchor_month: 決算アンカー日の月（省略時は月のみ比較にフォールバック）
        anchor_day: 決算アンカー日の日（省略時は月のみ比較にフォールバック）
        extra_anchors: 主anchor以外に探索するアンカー日候補のリスト
                       （[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②）

    Returns:
        int: 会計年度（西暦4桁）

    Examples:
        AAPL (fiscal_end_month=9):  end_date=2024-12-28 → 2025
        MSFT (fiscal_end_month=6):  end_date=2024-09-30 → 2025
        NVDA (fiscal_end_month=1):  end_date=2025-04-27 → 2026
        Dec  (fiscal_end_month=12): end_date=2025-03-31 → 2025
        CDNS (fiscal_end_month=12, anchor=(12,31)): end_date=2015-01-03 → 2014
    """
    def _month_only_fallback() -> int:
        if end_date.month > fiscal_end_month:
            return end_date.year + 1
        return end_date.year

    if anchor_month is None or anchor_day is None:
        return _month_only_fallback()

    end_date_only = end_date.date() if isinstance(end_date, datetime) else end_date

    anchors = [(anchor_month, anchor_day)]
    if extra_anchors:
        anchors.extend(extra_anchors)

    best_year = None
    best_diff = None
    for a_month, a_day in anchors:
        for candidate_year in (end_date_only.year - 1, end_date_only.year, end_date_only.year + 1):
            try:
                candidate = date(candidate_year, a_month, a_day)
            except ValueError:
                if (a_month, a_day) == (2, 29):
                    candidate = date(candidate_year, 2, 28)  # 非うるう年フォールバック
                else:
                    continue
            diff = abs((end_date_only - candidate).days)
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_year = candidate_year

    if best_year is not None and best_diff <= 60:
        return best_year

    print(f"[WARN] determine_fiscal_year: アンカー日ウィンドウ判定の最小日数差が"
          f"60日を超えたため月のみ比較にフォールバックします "
          f"(end_date={end_date_only}, fiscal_end_month={fiscal_end_month}, "
          f"anchor=({anchor_month},{anchor_day}), best_diff={best_diff})")
    return _month_only_fallback()
