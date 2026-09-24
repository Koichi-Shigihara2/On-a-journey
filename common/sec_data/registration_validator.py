#!/usr/bin/env python3
"""
登録パイプライン健全性チェッカー
-----------------------------------------------------------------------
Usage:
    python common/sec_data/registration_validator.py          # 全銘柄
    python common/sec_data/registration_validator.py AAPL     # 単体
    python common/sec_data/registration_validator.py --summary # サマリーのみ
    python common/sec_data/registration_validator.py AAPL --promote active
        # 単体ティッカーのNG=0を確認し、cik_lookup.csvのstatusを
        # provisioning等から指定値（active/candidate）へ昇格する
        # （[[REGISTER-FLOW-REDESIGN-1]]方針2、2026-09-03新設）。
        # --promoteは単一ティッカー指定時のみ使用可。NG判定は当該
        # ティッカーに関するNGのみに絞り込む（P3等の全体チェックが
        # 無関係な既存銘柄のNGを含みうるため、他銘柄のNGでは昇格を
        # ブロックしない設計）。NG>0の場合は昇格せずexit 1。

チェック項目:
    P1. 7ステップ登録完全性 (SEC/Beta/Valuation/HypeCore/Discover/Monitor)
    P2. データ品質・鮮度 (latest_revenue TTM乖離、旧XBRL形式、TTM陳腐化)
    P3. segment_config 整合性 (fiscal_year鮮度、weight合計、growth率)
    P4. Config 孤立エントリ (discover_config/cik_lookup の非監視残存)
    P5. 自動更新ワークフロー カバレッジ
    P6. CIK断絶候補検知 (新規登録銘柄の法人再編疑い)
"""
import csv
import json
import os
import re
import sys
from datetime import date, datetime
from typing import Optional

# ── パス解決 ──────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT   = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", ".."))
sys.path.insert(0, _REPO_ROOT)

from common.sec_data import tickers

SEC_DATA_DIR = os.path.join(_REPO_ROOT, "common", "sec_data", "data")
TTM_DIR      = os.path.join(_REPO_ROOT, "common", "sec_data", "ttm")
TANUKI_DIR   = os.path.join(_REPO_ROOT, "docs", "value-monitor", "tanuki_valuation", "data")
HYPECORE_DIR = os.path.join(_REPO_ROOT, "docs", "value-monitor", "hypecore", "data")
EPS_DIR      = os.path.join(_REPO_ROOT, "docs", "value-monitor", "adjusted_eps_analyzer", "data")
BETA_CFG     = os.path.join(_REPO_ROOT, "config", "beta_config.json")
SEG_CFG      = os.path.join(_REPO_ROOT, "config", "segment_config.json")
DISCOVER_CFG = os.path.join(_REPO_ROOT, "config", "discover_config.json")
CIK_CSV      = os.path.join(_REPO_ROOT, "config", "cik_lookup.csv")
MONITOR_YAML = os.path.join(_REPO_ROOT, "config", "monitor_tickers.yaml")
CIK_HISTORY_JSON = os.path.join(_REPO_ROOT, "common", "sec_data", "cik_history.json")

TODAY = date.today()

# CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1: 確認済み・接続しない方針の構造的境界銘柄
# （スピンオフ・カーブアウト型、破産再生型）。旧CIKへの接続対象ではないため、
# cik_history.json 未登録でも P6 の再フラグ対象から除外する。
CIK_DISCONTINUITY_CONFIRMED_STRUCTURAL = {
    "CEG", "LITE", "ABBV", "GEV", "SN", "CON", "VST",
}

# 汎用検知ロジック（CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1で検証済み: 既知9銘柄
# Recall100%/Precision65%）: 最古年度 >= 2010 かつ 判定年度revenue >= $500M
CIK_DISCONTINUITY_BOUNDARY_YEAR_MIN = 2010
CIK_DISCONTINUITY_REVENUE_MIN = 500_000_000

# ── ヘルパー ──────────────────────────────────────────────────────────

def _load_monitor_tickers() -> list[str]:
    with open(MONITOR_YAML, encoding="utf-8") as f:
        return sorted([l.strip().lstrip("- ") for l in f if l.strip().startswith("- ")])


def _load_json(path: str) -> Optional[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _annual_files(ticker: str) -> list[str]:
    d = os.path.join(SEC_DATA_DIR, ticker)
    if not os.path.isdir(d):
        return []
    return sorted([f for f in os.listdir(d) if re.match(r"annual_\d{4}\.json$", f)])


def _max_annual_year(ticker: str) -> Optional[int]:
    files = _annual_files(ticker)
    if not files:
        return None
    return max(int(f[7:11]) for f in files)


def _ttm_end(ticker: str) -> Optional[str]:
    path = os.path.join(TTM_DIR, f"{ticker}_ttm_series.json")
    d = _load_json(path)
    if d and d.get("series"):
        return d["series"][0].get("ttm_end")
    return None


def _ttm_revenue(ticker: str) -> Optional[float]:
    path = os.path.join(TTM_DIR, f"{ticker}_ttm_series.json")
    d = _load_json(path)
    if not d or not d.get("series"):
        return None
    # [[REGISTRATION-VALIDATOR-TTM-REVENUE-KEY-STALE-1]]: flowキーはttm_calculator.py
    # フェーズC移行（2026-07-25）以降snake_case。旧"Revenue"のままだったため常にNoneで
    # P2-Aが無言でスキップされていた（tests/test_ttm_flow_key_names.pyで旧キーを検出）
    rv = d["series"][0].get("flow", {}).get("revenue")
    if isinstance(rv, dict):
        return rv.get("val")
    return rv


def _latest_json(ticker: str) -> Optional[dict]:
    return _load_json(os.path.join(TANUKI_DIR, ticker, "latest.json"))


def _fy_year(fy_str: str) -> Optional[int]:
    """'FY2025' or '2025' -> 2025"""
    if not fy_str:
        return None
    m = re.search(r"(20\d{2})", str(fy_str))
    return int(m.group(1)) if m else None


# ── Issue コレクター ───────────────────────────────────────────────────

class Issues:
    def __init__(self) -> None:
        self._items: list[tuple[str, str, str]] = []  # (sev, category, msg)

    def ng(self, cat: str, msg: str) -> None:
        self._items.append(("NG", cat, msg))

    def warn(self, cat: str, msg: str) -> None:
        self._items.append(("WARN", cat, msg))

    def info(self, cat: str, msg: str) -> None:
        self._items.append(("INFO", cat, msg))

    def count_ng(self) -> int:
        return sum(1 for s, _, _ in self._items if s == "NG")

    def count_warn(self) -> int:
        return sum(1 for s, _, _ in self._items if s == "WARN")

    def all(self) -> list[tuple[str, str, str]]:
        return list(self._items)


# ═══════════════════════════════════════════════════════════════════════
# P1: 7ステップ登録完全性チェック
# ═══════════════════════════════════════════════════════════════════════

def check_p1_registration_completeness(ticker: str, issues: Issues,
                                        beta_overrides: dict,
                                        discover_tickers: set,
                                        monitor_set: set,
                                        eps_disabled: set = None) -> None:
    """7ステップ各段のデータ存在確認"""

    # Step 1: SEC annual data
    annual = _annual_files(ticker)
    if not annual:
        issues.ng("P1-Step1-SEC", f"{ticker}: SEC annual data なし (common/sec_data/data/{ticker}/)")
    elif len(annual) < 3:
        issues.warn("P1-Step1-SEC", f"{ticker}: SEC annual data が {len(annual)} 件のみ (<3件、上場直後?)")

    # Step 2: Beta config
    if ticker not in beta_overrides:
        issues.warn("P1-Step2-Beta", f"{ticker}: beta_config.json に override なし (raw yfinance 値使用中)")

    # Step 3: Valuation (latest.json)
    lj_path = os.path.join(TANUKI_DIR, ticker, "latest.json")
    if not os.path.exists(lj_path):
        issues.ng("P1-Step3-Valuation", f"{ticker}: latest.json 未生成 (pipeline 未実行?)")

    # Step 4: Audit – runtime チェックのため skip（別途 audit.py で実行）

    # Step 5: HypeCore poc.json
    poc_path = os.path.join(HYPECORE_DIR, f"{ticker}_poc.json")
    if not os.path.exists(poc_path):
        issues.warn("P1-Step5-HypeCore", f"{ticker}: _poc.json 未生成 (hypecore 未実行?)")

    # Step 5.5: EPS analyzer (cik_lookup.csv の eps=false 銘柄はスキップ)
    if not (eps_disabled and ticker in eps_disabled):
        eps_path = os.path.join(EPS_DIR, ticker)
        if not os.path.exists(eps_path):
            issues.warn("P1-Step5b-EPS", f"{ticker}: EPS analyzer データなし")

    # Step 6: discover_config
    if ticker not in discover_tickers:
        issues.ng("P1-Step6-Discover", f"{ticker}: discover_config.json に未登録")

    # Step 7: monitor_tickers.yaml
    if ticker not in monitor_set:
        issues.ng("P1-Step7-Monitor", f"{ticker}: monitor_tickers.yaml に未登録 (自動更新対象外)")


# ═══════════════════════════════════════════════════════════════════════
# P2: データ品質・鮮度チェック
# ═══════════════════════════════════════════════════════════════════════

def check_p2_data_quality(ticker: str, issues: Issues) -> None:

    lj = _latest_json(ticker)
    comps = (lj or {}).get("components", {})

    # ── A: latest_revenue TTM 乖離 ───────────────────────────────────
    annual_rev = comps.get("latest_revenue") or 0
    ttm_rev = _ttm_revenue(ticker)
    sector = comps.get("sector", "")

    if annual_rev and ttm_rev:
        ratio = ttm_rev / annual_rev
        # 金融セクターは revenue 定義が異なるため特別扱い
        fin_sectors = {"Financial Services", "Financial", "Banks", "Insurance"}
        is_financial = any(fs.lower() in (sector or "").lower() for fs in fin_sectors)

        if ratio >= 3.0:
            sev = "WARN" if is_financial else "NG"
            issues.ng("P2-A-RevTTM", f"{ticker}: latest_revenue=${annual_rev/1e9:.2f}B <<"
                      f" TTM=${ttm_rev/1e9:.2f}B (ratio={ratio:.1f}x) "
                      f"{'[金融セクター: revenue定義差の可能性]' if is_financial else '[SEC parserバグ疑い]'}")
        elif ratio >= 1.5:
            issues.warn("P2-A-RevTTM", f"{ticker}: latest_revenue(annual)=${annual_rev/1e9:.2f}B"
                        f" < TTM=${ttm_rev/1e9:.2f}B (ratio={ratio:.1f}x) "
                        f"[高成長: TTM反映推奨]")

    # ── B: 旧XBRL形式 (net_income=None in recent years 2022+) ────────
    annual_files = _annual_files(ticker)
    recent_none_years = []
    for fn in annual_files:
        yr = int(fn[7:11])
        if yr < 2022:
            continue  # 旧データは許容
        d = _load_json(os.path.join(SEC_DATA_DIR, ticker, fn))
        if d and d.get("pl", {}).get("net_income") is None and d.get("pl", {}).get("revenue"):
            recent_none_years.append(yr)
    if recent_none_years:
        issues.warn("P2-B-XBRL", f"{ticker}: net_income=None in recent years={recent_none_years}"
                    " (旧XBRL形式 → ROE計算でEPSフォールバック使用中)")

    # ── C: 年次データ陳腐化 ──────────────────────────────────────────
    max_yr = _max_annual_year(ticker)
    if max_yr is not None:
        lag = TODAY.year - max_yr
        if lag >= 3:
            issues.ng("P2-C-AnnualStale", f"{ticker}: 最新年次={max_yr} ({lag}年前) SEC取得が止まっている")
        elif lag == 2:
            issues.warn("P2-C-AnnualStale", f"{ticker}: 最新年次={max_yr} ({lag}年前) 要確認")

    # ── D: TTM 陳腐化 ─────────────────────────────────────────────────
    ttm_end_str = _ttm_end(ticker)
    if ttm_end_str:
        try:
            ttm_end_d = date.fromisoformat(ttm_end_str[:10])
            days_old = (TODAY - ttm_end_d).days
            if days_old > 200:
                issues.warn("P2-D-TTMStale", f"{ticker}: ttm_end={ttm_end_str} ({days_old}日前)"
                            " TTM更新が6ヶ月以上停止")
        except Exception:
            pass

    # ── E: Valuation calculation_date 陳腐化 ──────────────────────────
    if lj:
        calc_date_str = (lj.get("calculation_date") or "")[:10]
        if calc_date_str:
            try:
                calc_d = date.fromisoformat(calc_date_str)
                days_old = (TODAY - calc_d).days
                if days_old > 30:
                    issues.warn("P2-E-ValStale", f"{ticker}: calculation_date={calc_date_str}"
                                f" ({days_old}日前) 再生成推奨")
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════
# P6: CIK断絶候補検知（新規登録銘柄向け）
# ═══════════════════════════════════════════════════════════════════════

def check_p6_cik_discontinuity_candidate(ticker: str, issues: Issues, cik_history: dict) -> None:
    """CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1で検証済みの汎用検知ロジックを
    新規登録銘柄に適用し、法人再編（持株会社化・スピンオフ・破産再生等）に
    よるCIK断絶の候補を機械的に検知する。

    既に確定登録済み（cik_history.json、同一事業継続型で旧CIKと接続済み）
    または確認済み・接続しない方針が確定している構造的境界銘柄
    （CIK_DISCONTINUITY_CONFIRMED_STRUCTURAL）は再フラグしない。

    検知条件: 最古年度 >= 2010 かつ 判定年度revenue >= $500M
    （既知9銘柄でRecall100%/Precision65%、残り35%は誤検知の可能性があるため
    一次情報〈SEC EDGAR企業検索〉での人手確認が必須。自動でcik_history.jsonに
    登録することはしない — 確定登録は引き続き人手判断を経ること）。
    """
    if ticker in cik_history or ticker in CIK_DISCONTINUITY_CONFIRMED_STRUCTURAL:
        return

    years_by_num: dict[int, dict] = {}
    for fn in _annual_files(ticker):
        d = _load_json(os.path.join(SEC_DATA_DIR, ticker, fn))
        if not d:
            continue
        m = re.match(r"annual_(\d{4})\.json$", fn)
        if not m:
            continue
        years_by_num[int(m.group(1))] = d

    if not years_by_num:
        return

    years = sorted(years_by_num.keys())
    earliest_year = years[0]
    earliest_d = years_by_num[earliest_year]
    boundary_pattern = (
        not earliest_d.get("pl", {}) and not earliest_d.get("cf", {}) and bool(earliest_d.get("bs", {}))
    )
    check_year = earliest_year + 1 if boundary_pattern else earliest_year
    check_d = years_by_num.get(check_year)
    rev = check_d.get("pl", {}).get("revenue") if check_d else None

    flagged = (earliest_year >= CIK_DISCONTINUITY_BOUNDARY_YEAR_MIN
               and rev is not None and rev >= CIK_DISCONTINUITY_REVENUE_MIN)
    if flagged:
        issues.warn(
            "P6-CIKDiscontinuity",
            f"{ticker}: CIK断絶候補・一次情報確認要 (最古年度={earliest_year}, "
            f"判定年度={check_year}, revenue=${(rev or 0)/1e6:,.0f}M) "
            "— SEC EDGARで法人再編歴（持株会社化/スピンオフ/破産再生等）を確認し、"
            "同一事業継続型ならcik_history.json、スピンオフ・破産再生型なら"
            "CIK_DISCONTINUITY_CONFIRMED_STRUCTURALへの追加を検討（要人手判断）"
        )


# ═══════════════════════════════════════════════════════════════════════
# P3: segment_config 整合性チェック
# ═══════════════════════════════════════════════════════════════════════

def check_p3_segment_config(issues: Issues) -> None:
    seg = _load_json(SEG_CFG) or {}
    for ticker, conf in seg.items():
        if ticker.startswith("_"):
            continue
        segs = conf.get("segments", {})
        fy_str = str(conf.get("fiscal_year", ""))
        fy_year = _fy_year(fy_str)

        # fiscal_year 鮮度
        if fy_year:
            lag = TODAY.year - fy_year
            if lag >= 2:
                issues.ng("P3-SegFY", f"{ticker}: segment_config fiscal_year={fy_str}"
                          f" ({lag}年前) 要更新 [成長率ウェイトが古い]")
            # lag==1 は現在 FY2025 が正常 → WARN なし
        elif segs and not all(s == "General" for s in segs):
            issues.warn("P3-SegFY", f"{ticker}: fiscal_year 未設定 (セグメントあり)")

        # weight 合計検証
        if segs:
            total_w = sum(float(v.get("weight", 0)) for v in segs.values())
            if abs(total_w - 1.0) > 0.05:
                issues.ng("P3-SegWeight", f"{ticker}: weight合計={total_w:.3f} (≠1.0)"
                          " [Segment_Weighted_Growth が不正になる]")

        # growth 率異常値
        for seg_name, sv in segs.items():
            g = sv.get("growth")
            if g is not None:
                if g < -0.5 or g > 3.0:
                    issues.warn("P3-SegGrowth", f"{ticker}/{seg_name}: growth={g:.1%}"
                                " (範囲外: -50%~+300%)")


# ═══════════════════════════════════════════════════════════════════════
# P4: Config 孤立エントリチェック
# ═══════════════════════════════════════════════════════════════════════

def check_p4_orphan_configs(issues: Issues, monitor_set: set) -> None:

    # discover_config の非監視残存
    disc = _load_json(DISCOVER_CFG) or {}
    disc_tickers = set(disc.get("tickers", {}).keys())
    for t in sorted(disc_tickers - monitor_set):
        issues.warn("P4-DiscoverOrphan", f"{t}: discover_config.json に残存するが monitor_tickers 未登録"
                    " [削除漏れ?]")

    # cik_lookup の非監視残存 (annual data あり)
    try:
        with open(CIK_CSV, encoding="utf-8") as f:
            cik_tickers = {row["ticker"].strip() for row in csv.DictReader(f)
                           if row.get("ticker", "").strip()}
    except Exception:
        cik_tickers = set()

    for t in sorted(cik_tickers - monitor_set - {"ticker", "TICKER"}):
        if t.startswith("_"):
            continue
        sec_dir = os.path.join(SEC_DATA_DIR, t)
        has_annual = os.path.isdir(sec_dir) and any(
            re.match(r"annual_\d{4}\.json$", f) for f in os.listdir(sec_dir)
        ) if os.path.isdir(sec_dir) else False
        if has_annual:
            issues.warn("P4-CIKOrphan", f"{t}: cik_lookup に登録済み+SEC data あり だが"
                        " monitor_tickers 未登録 [inactive?]")
        else:
            # SEC data なし = 登録途中の可能性
            company_facts = os.path.exists(os.path.join(sec_dir, "company_facts.json"))
            if company_facts or os.path.isdir(sec_dir):
                issues.warn("P4-CIKIncomplete", f"{t}: cik_lookup 登録済みだが annual data 未取得"
                            " (登録途中 or SEC fetch 失敗?)")

    # SEC data dir があるが monitor 未登録 かつ cik_lookup にも未登録の銘柄のみ報告
    # (cik_lookup 登録済みは P4-C / P4-CI で既に報告されるため除外)
    if os.path.isdir(SEC_DATA_DIR):
        for t in sorted(os.listdir(SEC_DATA_DIR)):
            if not os.path.isdir(os.path.join(SEC_DATA_DIR, t)):
                continue
            if t in monitor_set or t.startswith("--") or t.startswith("_"):
                continue
            if t in cik_tickers:
                continue  # P4-C or P4-CI で報告済み
            issues.warn("P4-SecDataOrphan", f"{t}: SEC data dir あり だが monitor・cik_lookup 未登録")


# ═══════════════════════════════════════════════════════════════════════
# P5: 自動更新ワークフロー カバレッジ
# ═══════════════════════════════════════════════════════════════════════

def check_p5_workflow_coverage(issues: Issues, monitor_set: set) -> None:
    """
    SEC_Data_Update は cik_lookup.csv から get_all() で ticker を取得する。
    monitor_tickers.yaml 未登録銘柄は TANUKI_VALUATION_Update の対象外になる。
    （pipeline.py は引数なしで tanuki data dir を走査するため実質カバー済みだが念のため確認）
    """
    # tanuki data に latest.json がある銘柄 vs monitor_tickers
    tanuki_tickers = set()
    if os.path.isdir(TANUKI_DIR):
        for t in os.listdir(TANUKI_DIR):
            if os.path.exists(os.path.join(TANUKI_DIR, t, "latest.json")):
                tanuki_tickers.add(t)

    uncovered = sorted(tanuki_tickers - monitor_set)
    if uncovered:
        issues.warn("P5-WorkflowGap", f"tanuki latest.json があるが monitor 未登録: {uncovered}"
                    " [自動更新ワークフローの対象外]")

    # TANUKI_VALUATION_Update が weekdays 実行か確認
    wf_path = os.path.join(_REPO_ROOT, ".github", "workflows", "TANUKI_VALUATION_Update.yml")
    if os.path.exists(wf_path):
        with open(wf_path, encoding="utf-8") as f:
            wf_content = f.read()
        if "weekday" not in wf_content and "1-5" not in wf_content and "schedule" not in wf_content:
            issues.warn("P5-WorkflowSchedule", "TANUKI_VALUATION_Update.yml に schedule 設定が見つからない")


# ═══════════════════════════════════════════════════════════════════════
# メイン
# ═══════════════════════════════════════════════════════════════════════

def run(target_tickers: Optional[list] = None, summary_only: bool = False) -> Issues:
    monitor_tickers = _load_monitor_tickers()
    monitor_set = set(monitor_tickers)

    beta = _load_json(BETA_CFG) or {}
    beta_overrides = beta.get("overrides", {})

    disc = _load_json(DISCOVER_CFG) or {}
    discover_tickers = set(disc.get("tickers", {}).keys())

    tickers_to_check = target_tickers if target_tickers else tickers.get_all_tickers()

    # cik_lookup.csv の eps=false 銘柄を収集（EPS analyzer 非対応銘柄の WARN 抑制用）
    eps_disabled: set = set()
    try:
        with open(CIK_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("eps", "true").strip().lower() == "false":
                    eps_disabled.add(row["ticker"].strip())
    except Exception:
        pass

    all_issues = Issues()
    cik_history = _load_json(CIK_HISTORY_JSON) or {}

    # ── P1 / P2 / P6: 銘柄ごとチェック ────────────────────────────────
    for t in tickers_to_check:
        check_p1_registration_completeness(t, all_issues, beta_overrides,
                                           discover_tickers, monitor_set,
                                           eps_disabled=eps_disabled)
        check_p2_data_quality(t, all_issues)
        check_p6_cik_discontinuity_candidate(t, all_issues, cik_history)

    # ── P3: segment_config（全銘柄対象）──────────────────────────────
    check_p3_segment_config(all_issues)

    # ── P4: Config孤立（全体チェック）────────────────────────────────
    check_p4_orphan_configs(all_issues, monitor_set)

    # ── P5: ワークフローカバレッジ ────────────────────────────────────
    check_p5_workflow_coverage(all_issues, monitor_set)

    # ── レポート出力 ──────────────────────────────────────────────────
    _print_report(all_issues, len(tickers_to_check), summary_only)

    return all_issues


def _print_report(issues: Issues, n_tickers: int, summary_only: bool) -> None:
    items = issues.all()
    ng_items   = [(c, m) for s, c, m in items if s == "NG"]
    warn_items = [(c, m) for s, c, m in items if s == "WARN"]

    print(f"╔══════════════════════════════════════════════════════════╗")
    print(f"║  登録パイプライン健全性チェック ({n_tickers} 銘柄)  {TODAY}  ║")
    print(f"╚══════════════════════════════════════════════════════════╝")
    print()

    # カテゴリ別グルーピング
    cats_ng: dict[str, list[str]] = {}
    cats_warn: dict[str, list[str]] = {}
    for cat, msg in ng_items:
        cats_ng.setdefault(cat, []).append(msg)
    for cat, msg in warn_items:
        cats_warn.setdefault(cat, []).append(msg)

    _CAT_LABELS = {
        "P1-Step1-SEC":      "P1-Step1  SEC data 未取得",
        "P1-Step2-Beta":     "P1-Step2  Beta 未設定",
        "P1-Step3-Valuation":"P1-Step3  Valuation 未生成",
        "P1-Step5-HypeCore": "P1-Step5  HypeCore 未実行",
        "P1-Step5b-EPS":     "P1-Step5b EPS Analyzer なし",
        "P1-Step6-Discover": "P1-Step6  Discover 未登録",
        "P1-Step7-Monitor":  "P1-Step7  monitor_tickers 未登録",
        "P2-A-RevTTM":       "P2-A  latest_revenue TTM乖離",
        "P2-B-XBRL":         "P2-B  旧XBRL (net_income=None)",
        "P2-C-AnnualStale":  "P2-C  年次データ陳腐化",
        "P2-D-TTMStale":     "P2-D  TTM陳腐化(>6ヶ月)",
        "P2-E-ValStale":     "P2-E  Valuation陳腐化(>30日)",
        "P3-SegFY":          "P3-FY segment fiscal_year鮮度",
        "P3-SegWeight":      "P3-W  segment weight合計不正",
        "P3-SegGrowth":      "P3-G  segment growth率異常",
        "P4-DiscoverOrphan": "P4-D  discover_config孤立エントリ",
        "P4-CIKOrphan":      "P4-C  cik_lookup孤立エントリ",
        "P4-CIKIncomplete":  "P4-CI cik_lookup登録途中",
        "P4-SecDataOrphan":  "P4-S  SEC data孤立ディレクトリ",
        "P5-WorkflowGap":    "P5    ワークフローカバレッジ外",
        "P5-WorkflowSchedule":"P5   ワークフロースケジュール",
        "P6-CIKDiscontinuity":"P6   CIK断絶候補(要一次情報確認)",
    }

    def _print_section(label: str, cats: dict[str, list[str]], icon: str) -> None:
        if not cats:
            return
        for cat, msgs in sorted(cats.items()):
            cat_label = _CAT_LABELS.get(cat, cat)
            print(f"{icon} [{cat_label}]  ({len(msgs)}件)")
            if not summary_only:
                for m in msgs:
                    print(f"     {m}")
            print()

    if ng_items:
        print(f"━━━━━ ❌ NG ({len(ng_items)}件) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        _print_section("", cats_ng, "❌")
    else:
        print("━━━━━ ❌ NG: 0件 ✅ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print()

    if warn_items:
        print(f"━━━━━ ⚠️  WARN ({len(warn_items)}件) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        _print_section("", cats_warn, "⚠️ ")
    else:
        print("━━━━━ ⚠️  WARN: 0件 ✅ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print()

    print("─" * 60)
    print(f"合計: NG={len(ng_items)} / WARN={len(warn_items)}  "
          f"(対象={n_tickers}銘柄  日付={TODAY})")


# ═══════════════════════════════════════════════════════════════════════
# --promote: provisioning状態からの昇格（[[REGISTER-FLOW-REDESIGN-1]]方針2）
# ═══════════════════════════════════════════════════════════════════════

_PROMOTABLE_STATUSES = {"active", "candidate"}


def ticker_ng_items(issues: Issues, ticker: str) -> list[tuple[str, str]]:
    """指定ティッカーに関するNG項目のみを抽出する。

    P3（segment_config整合性）・P4/P5相当は`target_tickers`フィルタに
    関わらずcik_lookup.csv/segment_config.json全体を毎回スキャンする
    設計（run()内のコメント参照）のため、`result.count_ng()`の総数には
    無関係な既存銘柄のNGが混入しうる。全NGメッセージが
    `f"{ticker}: ..."`形式で統一されていることを前提に、メッセージ
    先頭のプレフィックスで当該ティッカー分のみへ絞り込む
    （他銘柄の既存NGで昇格がブロックされないようにするための設計）。
    """
    prefix = f"{ticker}: "
    return [(cat, msg) for sev, cat, msg in issues.all() if sev == "NG" and msg.startswith(prefix)]


def promote_ticker_status(ticker: str, new_status: str) -> str:
    """cik_lookup.csvの該当ティッカーのstatus列のみを更新する。

    他の列・他の行は一切変更しない（テキストベースの部分編集、
    2026-09-02のJSON全体書き直し事故と同型のリスクを避ける設計方針を
    CSVでも踏襲）。対象行1行のみcsv.reader/csv.writerで読み書きし、
    他の行は元のテキストをそのまま保持する。

    Returns:
        昇格前のstatus値（ログ表示用）
    Raises:
        ValueError: tickerがcik_lookup.csvに見つからない場合
    """
    with open(CIK_CSV, encoding="utf-8", newline="") as f:
        lines = f.readlines()

    header_fields = next(csv.reader([lines[0]]))
    status_idx = header_fields.index("status")

    old_status = None
    for i in range(1, len(lines)):
        if not lines[i].strip():
            continue
        # ticker列は常に先頭フィールドでカンマ・引用符を含まないため、
        # 対象行の特定自体は単純split(",",1)で安全に行える
        # （値の書き換え自体はcsv.reader/writerで行い引用符ルールを保つ）。
        if lines[i].split(",", 1)[0] != ticker:
            continue
        fields = next(csv.reader([lines[i]]))
        old_status = fields[status_idx]
        fields[status_idx] = new_status
        import io
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\n").writerow(fields)
        lines[i] = buf.getvalue()
        break

    if old_status is None:
        raise ValueError(f"{ticker} が {CIK_CSV} に見つかりません")

    with open(CIK_CSV, "w", encoding="utf-8", newline="") as f:
        f.writelines(lines)

    return old_status


if __name__ == "__main__":
    raw_args = sys.argv[1:]

    promote_status = None
    if "--promote" in raw_args:
        idx = raw_args.index("--promote")
        if idx + 1 >= len(raw_args) or raw_args[idx + 1] not in _PROMOTABLE_STATUSES:
            print(f"--promote には {'/'.join(sorted(_PROMOTABLE_STATUSES))} のいずれかを指定してください")
            sys.exit(2)
        promote_status = raw_args[idx + 1]
        raw_args = raw_args[:idx] + raw_args[idx + 2:]

    args = [a for a in raw_args if not a.startswith("--")]
    summary_only = "--summary" in raw_args

    if promote_status is not None:
        if len(args) != 1:
            print("--promote は単一ティッカー指定時のみ使用できます（例: registration_validator.py AAPL --promote active）")
            sys.exit(2)
        ticker = args[0]
        result = run(target_tickers=args, summary_only=summary_only)
        ng_for_ticker = ticker_ng_items(result, ticker)
        if not ng_for_ticker:
            old_status = promote_ticker_status(ticker, promote_status)
            print(f"\n✅ {ticker}: NG=0（自ティッカー分）のためstatusを"
                  f"'{old_status}' → '{promote_status}' へ昇格しました")
            sys.exit(0)
        else:
            print(f"\n⏸️  {ticker}: 自ティッカー分のNGが{len(ng_for_ticker)}件残っているため"
                  f"昇格を見送りました（statusは変更していません）")
            for cat, msg in ng_for_ticker:
                print(f"     [{cat}] {msg}")
            sys.exit(1)
    elif args:
        run(target_tickers=args, summary_only=summary_only)
    else:
        result = run(summary_only=summary_only)
        sys.exit(1 if result.count_ng() > 0 else 0)
