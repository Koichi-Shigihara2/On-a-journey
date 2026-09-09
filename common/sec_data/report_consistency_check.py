#!/usr/bin/env python3
"""
report_consistency_check.py
全銘柄 report.txt の整合性一括チェック

検出項目:
  NG  1. FCF符号矛盾          FCF_History最新年マイナス & Matrix④ Key_Metric_Y 正値
  NG  2. DCF_Reliability欠落   FCF_Base行 or FCF_Conversion_Rate行あり & DCF_Reliability行なし
                              （Policy A: FCF_Base直接方式 / Policy B: FCF_Conversion_Rate方式、DCF-RELIABILITY-1）
  NG  3. LOW丸め未発動         DCF_Reliability=LOW & Classification が WATCH/SELL/PASS 以外
  NG  4. 割引率1段             Discount_Rate_Primary 行なし（旧WACC単独形式）
  NG  7. RPO条件違反           RPO_PV>0 & whitelist外 & RPO/Revenue<0.3
  NG  8. Matrix④高FCFラベル赤字 Matrix④ Label="高FCF" & 最新FCF実績マイナス
  NG  11. Revenue桁違い        annual_JSONの隣接年Revenue比が10倍超（誤XBRLタグ検出）
  WARN 5. NetDebt旧表示        Net_Debt行あり & ST_Invest 非ゼロ（latest.json）& 報告なし
  WARN 6. 負PER数値表示        Market_PER_GAAP が負数（N/M 未変換）
  WARN 9. セグメント設定陳腐化  segment_config fiscal_yearが2年以上前
  WARN 10. PS異常値            yfinance PSが自社計算値(price×shares/rev)の2.5倍超 or 0.4倍未満
  WARN 12. Cash-STI期ズレ      latest.jsonのCashが最新四半期値なのにST_Investが年次値のまま
  NG  13. RICE負値ラベルなし   rice.available=true かつ RICE<0 なのに Matrix Label に N/A/OCF赤字 なし
  NG  14. EPS>株価50%          EPS Analyzer直近Q adj_eps が株価の50%超（単位バグ検出）
  NG  15. EPS>株価             EPS Analyzer直近Q adj_eps が株価を上回る（単位バグ確実）
  WARN 16. TTM四半期不足       EPS Analyzer TTM計算に使用した四半期数が4未満
  NG  17. EPS全値$0.0          quarterly.json の全四半期 adj_eps=0.0（BUG-EPS-ZERO-1 回帰検知）
  WARN 18. G=15%デフォルト未調整 recommended_g あり & phase1_growth_auto_adjusted=False（DCF-DEFAULT-G-1 回帰）
  NG  19. SEC株数=0            quarterly.json に diluted_shares=0 の四半期（株数取得失敗）
  WARN 20. fcf_cagr floor張り付き growth.source=fcf_cagr かつ growth.rateがgrowth_floor(15%)に
                              完全一致（recommended_gの有無を問わず検知、GROWTH-FLOOR-VERDICT-1）
  WARN 21. Revenue段差型急変    直近6年の隣接年Revenue比が2.0倍以上/0.5倍以下（QUALITY-GATES-EPIC-1
                              Phase 2b-2、common.screening.dcf_validity_checker::check_c_data_jump()を
                              統合。NG-11との役割分担・NGではなくWARNとした理由は下記CHECK-21
                              実装箇所のコメント参照）
  WARN 22. fyキー競合          本人データ(reportDate==end_date)同士で同一fyタグに複数の異なる
                              真の期間が対応する矛盾（FY52WEEK-BUCKET-MISPLACE-1根本修正で新設。
                              CRM/FCX/CAKE/HON/COHR/AVAV/FICO/NVDAで実在確認済み。parser.pyの
                              tie-breakで自動解決済みのため非ブロッキング）
  WARN 23. fyタグ裏取り不一致  本人データ(is_own_data=True)自身の年度バケツキー
                              （determine_fiscal_year()の計算結果）と採用エントリの
                              生XBRL fyタグが食い違う（ARCH-DATA-1ステージ3で新設。
                              CHECK-22とは独立した別軸で「fyタグは単一だが値の年度バケツ配置
                              自体がfyタグと異なる」CDNS型を検知する。比較年度再掲エントリ
                              〈is_own_data=False〉はfyタグがfiling側の属性でしかなく
                              正常仕様のため対象外。自動修正なし）
  WARN 24. 決算期変更境界バケツ競合 決算期変更の境界年で、生fyタグ・end_dateの両方が
                              異なる2エントリ（本人データ側と非本人データ側）が同一年度
                              バケツ（computed_year）で競合する（FYE-CHANGE-BOUNDARY-
                              COLLISION-BLIND-1で新設。CHECK-22〈同一fyタグ前提〉・
                              CHECK-23〈勝者自身のfyタグとバケツの不一致、敗者側は対象外〉
                              のいずれとも異なる軸。RCAT（決算期を2回変更）で実在確認済み、
                              ELF/MSCI/NOWはクラスタリング候補ではあるが実際の競合なしと
                              確認済み。現状は_own_override_is_safe()の汎用accnベース判定
                              の副次効果で正しい値が採用されているため実害はなく、将来の
                              実装変更等で崩れうる潜在リスクの予防的可視化が目的。
                              自動修正なし）
  WARN 25. BS項目None         最新annual_YYYY.jsonのtotal_assets/total_liabilities/
                              stockholders_equity/current_assets/current_liabilities/
                              cash_and_equivalentsのいずれかがNone
                              （FY52WEEK-BS-NULL-SILENT-1 Phase A新設。全105銘柄実測で
                              None率がほぼ0-4%のフィールドに限定——ほぼ確実にデータ異常の
                              シグナル。従来はreader.py::get_net_cash()等で`or 0`により
                              静かに$0化され検知不能だった。short_term_investments/
                              long_term_debt/short_term_debt〈真のゼロとの判別困難〉・
                              rpo〈非SaaS銘柄はNoneが正常〉はPhase B/Cとして対象外）
  WARN 26. BS項目遷移(有値→None) short_term_investments/long_term_debt/
                              short_term_debt/rpo（WARN-25対象外の4フィールド）を
                              対象に、直近2年度分のannual_*.jsonを比較し、前年に
                              値があったフィールドが当年でNoneに遷移した場合に発火
                              （BS-FIELD-NONE-TRANSITION-DETECT-1新設）。period
                              （fyラベル）の年度差が厳密に1でない場合（決算期変更
                              等でfiles[-2]が真の「1年前」を表さない可能性がある
                              場合、FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1参照）・
                              annual_*.jsonが1年分のみ（新規登録銘柄）の場合は
                              判定不能として発火させない。事前調査（[[NVDA-STI-
                              TAG-UNIDENTIFIED-1]]調査時の体制確認）でFY52WEEK-
                              BS-NULL-SILENT-1「生涯フェードアウト」既確認済み
                              8件（APP/short_term_debt・BKNG/short_term_
                              investments・CPRT/long_term_debt・DOCN/short_term_
                              investments・ENTG/short_term_debt・KULR/short_term_
                              debt・MSCI/short_term_debt・SOUN/long_term_debt）が
                              実装直後に発火することが判明済みのため、
                              warn_acknowledged.jsonへ事前登録済み
  WARN 27. 近似値残差過大      parser.py::_apply_cross_filing_tags()が付与する
                              bs_provenance[field].is_approximated=Trueのエントリで
                              residual_pctが5%を超過（NVDA-STI-TAG-UNIDENTIFIED-1・
                              ANOMALY-PATTERN-CATALOG-1型C対応。cross_filing_tags
                              機構の将来の再利用先で、想定外に大きな乖離が
                              生じていないかの安全網。NVDA自身は+0.88%のため
                              通常は発火しない）
  WARN 28. 10-KT/10-QT除外    company_facts.jsonにform=10-KT/10-QTのaccnが存在する
                              のに、そのaccnがaccn_to_reportdate（submissions.json
                              由来）に未登録（[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]
                              新設。fetcher.py::_fetch_submissions_for_cikの
                              relevant_formsに10-KT/10-QTが含まれておらず、決算期
                              変更移行期報告書の本人データがis_own_data判定の対象外
                              になる構造的欠落を直接検知する。WARN-24〈決算期変更
                              境界バケツ競合〉はこの欠落が引き起こす症状〈バケツ
                              競合〉を検知するのに対し、本WARNは根本原因〈10-KT/
                              10-QT自体の除外〉を直接検知する別軸。RCATで実在確認
                              済み。自動修正なし、検知のみ）
  WARN 29. 会計恒等式不成立    Total_Assets=Total_Liabilities+Stockholders_Equity
                              が、NCI・一時的持分（MinorityInterest・
                              TemporaryEquityCarryingAmount系・
                              RedeemableNoncontrollingInterestEquity...
                              CarryingAmount系の許可リストのみ）を加算した拡張形
                              でも成立しない（[[CHECK29-ACCOUNTING-IDENTITY-
                              DETECTION-LAYER-1]]新設。①本体一致・②拡張形一致の
                              OR条件フォールバックはparser.py側で判定済みで、
                              いずれでも解消しないケースのみ発火する。実装前
                              シミュレーションで、無条件加算はKO/WMT/VZ等の
                              既存正常ケースで二重計上を起こすと判明したため、
                              「本体不一致の場合のみ拡張形を試す」設計とした。
                              105銘柄実測で156件中133件が拡張形で解消、残る
                              23件が本WARN対象（[[CHECK29-UNRESOLVED-23-MIXED-
                              CAUSES-1]]参照）。自動修正なし、検知のみ）
  NG   31. fixed_registry不整合 fixed_registry.json登録済みのticker×年度で、
                              annual_{year}.jsonの現在のsnapshot_hashが
                              registry記録時のsnapshot_hashと不一致
                              （[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]
                              新設。フィックス機構の二次防御〈CI検知〉。
                              一次防御〈parser.py::_apply_fixed_registry_
                              freeze()〉が正しく機能していればこのNGは
                              発生しないはずであり、不一致は「意図しない
                              書き換え」または「registry更新を伴わない
                              手動再フィックス漏れ」を意味するためNG化する
                              （WARN化すると「許容してよいWARN」として
                              放置されるリスクがあり、フィックスの
                              「以後変更されない」という保証自体が
                              骨抜きになるため）。report.txtの有無に
                              関わらず常に実行する（common/sec_data/側の
                              検証でありTANUKI VALUATION出力に依存しない）。
                              自動修正なし、検知のみ）

WARN台帳（QUALITY-GATES-EPIC-1 Phase 1・2026-07-12新設）:
  config/warn_acknowledged.json に (CHECK番号, ticker) の組み合わせを事前登録すると
  「確認済み」として通常表示される。未登録のWARNは実行時に [🆕未確認 WARN-N ...] と
  強調表示される（非ブロッキング動作は維持、NG化はしない）。
"""

import argparse
import os
import re
import json
import glob
import sys
from datetime import datetime
from typing import Optional

# ─── パス設定 ────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT    = os.path.normpath(os.path.join(SCRIPT_DIR, "../.."))
DATA_DIR     = os.path.join(REPO_ROOT, "docs/value-monitor/tanuki_valuation/data")
SEC_DATA_DIR = os.path.join(REPO_ROOT, "common/sec_data/data")
EPS_DATA_DIR = os.path.join(REPO_ROOT, "docs/value-monitor/adjusted_eps_analyzer/data")
RPO_CONFIG   = os.path.join(REPO_ROOT, "config/rpo_config.json")
SEG_CONFIG   = os.path.join(REPO_ROOT, "config/segment_config.json")
WARN_LEDGER  = os.path.join(REPO_ROOT, "config/warn_acknowledged.json")

# common.screening.dcf_validity_checker（CHECK-21用）をimportするためrepo_rootを
# sys.pathに追加する（registration_validator.pyと同一パターン）
sys.path.insert(0, REPO_ROOT)
from common.screening.dcf_validity_checker import check_c_data_jump  # noqa: E402
from common.sec_data import tickers as _tickers_mod  # noqa: E402
from common.sec_data.fetcher import load_submissions, load_company_facts  # noqa: E402
from common.sec_data.parser import _load_fixed_registry  # noqa: E402
from common.sec_data.utils import compute_snapshot_hash  # noqa: E402
from common.yfinance_utils import safe_yf_ticker  # noqa: E402

_SEG_CFG_CACHE: dict = {}

def _load_seg_config() -> dict:
    global _SEG_CFG_CACHE
    if not _SEG_CFG_CACHE:
        try:
            with open(SEG_CONFIG, encoding="utf-8") as f:
                _SEG_CFG_CACHE = json.load(f)
        except Exception:
            pass
    return _SEG_CFG_CACHE


# ─── ユーティリティ ──────────────────────────────────────────

def _load_rpo_whitelist() -> set:
    try:
        with open(RPO_CONFIG, encoding="utf-8") as f:
            cfg = json.load(f)
        return set(cfg.get("whitelist", {}).keys())
    except Exception:
        return set()


_WARN_CHECK_RE = re.compile(r'\[WARN-(\d+)')


def load_warn_ledger(path: str = WARN_LEDGER) -> set[tuple[str, str]]:
    """
    確認済みWARN台帳（QUALITY-GATES-EPIC-1 Phase 1）を読み込む。

    台帳ファイルが存在しない場合は空集合を返す（全WARNが「未確認」扱いになる）。
    キーは (CHECK番号, ticker) のタプル。exact な message文字列ではなく
    check番号単位で確認済みとするため、同じ銘柄・同じCHECKのWARNが
    数値だけ変わって再発しても「確認済み」のまま扱われる（意図的な粗さ）。
    """
    if not os.path.exists(path):
        return set()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return {
            (entry["check"], entry["ticker"])
            for entry in data.get("acknowledged", [])
        }
    except Exception:
        return set()


def _check_discover_config_sync() -> list[str]:
    """CHECK-32: config/discover_config.json・config/theme_config.jsonと
    docs/portfolio/data/側コピーの内容一致を検証する
    （[[DISCOVER-CONFIG-DUAL-MGMT-1]]の同期漏れ検知。書き手が複数
    存在するため`Discover_Config_Sync.yml`が自動同期する設計だが、
    同期漏れ・ワークフロー失敗をNGとして検出する。
    [[FCFCONFIG-MISSING-DETECTION-WEAK-1]]と同型のサイレント破損対策）。
    ティッカー非依存の単発チェックのため、check_ticker()内ではなく
    run_checks()から直接1回だけ呼ばれる。NGメッセージのリストを返す。
    """
    ng: list[str] = []
    pairs = [
        ("config/discover_config.json", "docs/portfolio/data/discover_config.json"),
        ("config/theme_config.json", "docs/portfolio/data/theme_config.json"),
    ]
    for src_rel, dst_rel in pairs:
        src_path = os.path.join(REPO_ROOT, src_rel)
        dst_path = os.path.join(REPO_ROOT, dst_rel)
        if not os.path.exists(src_path):
            ng.append(f"  [NG-32 discover_config同期不整合] {src_rel} が存在しない")
            continue
        if not os.path.exists(dst_path):
            ng.append(
                f"  [NG-32 discover_config同期不整合] {dst_rel} が存在しない "
                f"（{src_rel}からの同期未実施の可能性）"
            )
            continue
        try:
            with open(src_path, encoding="utf-8") as f:
                src_data = json.load(f)
            with open(dst_path, encoding="utf-8") as f:
                dst_data = json.load(f)
        except Exception as e:
            ng.append(f"  [NG-32 discover_config同期不整合] {src_rel}/{dst_rel} 読み込みエラー ({e})")
            continue
        # JSONパース後の内容で比較する（バイト単位比較はしない）。
        # Windows core.autocrlf=true環境ではgit checkout時にCRLF/LFが
        # 混在しうるが、git自体はこれを「変更なし」とみなす（コミット時に
        # 正規化される）ため、生バイト比較は改行コード差だけで誤検知
        # （false NG）を起こす（2026-08-15実測で確認済み）。
        if src_data != dst_data:
            ng.append(
                f"  [NG-32 discover_config同期不整合] {src_rel} と {dst_rel} の内容が"
                f"一致しない → Discover_Config_Sync.ymlの自動同期が未実行・失敗している"
                f"可能性（自動修正なし）"
            )
    return ng


# CHECK-34: config/設定ファイル読み込みの横断解決チェック用レジストリ
# （[[CONFIG-LOAD-SILENT-FALLBACK-1]]）。CHECK-32/33で確立した
# 「代理の検証（チェッカー独自のos.path.exists()）ではなく、本番コードの
# 解決ロジックそのものを呼び出して検証する」原則を、個別チェック関数を
# ファイル数分作るのではなく1つの汎用関数+データテーブルへ一般化した
# （CHECK-33のfcf_conversion_config.json専用実装はこのテーブルの1エントリ
# として統合済み、専用関数は廃止）。
# import_style:
#   "flat"    - sys.pathにmodule_dirを追加し、モジュール名だけでimportする
#               （src.value.tanuki_valuation配下は__init__.pyの.wacc import
#               失敗によりフルパッケージimportができないため、既存テストと
#               同じflat importパターンを使う）
#   "package" - REPO_ROOT起点のフルドット区切りパスでimportする
#               （src.value.adjusted_eps_analyzer配下は相対import
#               〈from .module import ...〉を使っているためflat importでは
#               動かず、パッケージとしてimportする必要がある）
_CONFIG_LOADER_REGISTRY = [
    {
        "label": "config/rpo_config.json",
        "import_style": "flat",
        "module_dir": os.path.join(REPO_ROOT, "src", "value", "tanuki_valuation", "calculator"),
        "module": "adjustments",
        "func": "resolve_rpo_config_path",
    },
    {
        "label": "config/beta_config.json",
        "import_style": "flat",
        "module_dir": os.path.join(REPO_ROOT, "src", "value", "tanuki_valuation"),
        "module": "data_fetcher",
        "func": "resolve_beta_config_path",
    },
    {
        "label": "config/fcf_conversion_config.json",
        "import_style": "flat",
        "module_dir": os.path.join(REPO_ROOT, "src", "value", "tanuki_valuation", "calculator"),
        "module": "adjustments",
        "func": "resolve_fcf_conversion_config_path",
    },
    {
        "label": "config/split_history.yaml",
        "import_style": "package",
        "module_dir": None,
        "module": "src.value.adjusted_eps_analyzer.pipeline",
        "func": "resolve_split_history_path",
    },
]


def _check_config_loaders_resolvable() -> list[str]:
    """CHECK-34: config/配下の設定ファイルが、各モジュールの実際の
    パス解決ロジックで解決できるかを_CONFIG_LOADER_REGISTRY駆動で
    横断検証する（[[CONFIG-LOAD-SILENT-FALLBACK-1]]）。

    ティッカー非依存の単発チェックのため、check_ticker()内ではなく
    run_checks()から直接1回だけ呼ばれる。NGメッセージのリストを返す。
    """
    import importlib

    ng: list[str] = []
    for entry in _CONFIG_LOADER_REGISTRY:
        label = entry["label"]
        try:
            if entry["import_style"] == "flat":
                if entry["module_dir"] not in sys.path:
                    sys.path.insert(0, entry["module_dir"])
                mod = importlib.import_module(entry["module"])
            else:
                mod = importlib.import_module(entry["module"])
            resolver = getattr(mod, entry["func"])
            resolved = resolver()
        except Exception as e:
            ng.append(
                f"  [NG-34 config読み込み解決失敗] {label}: "
                f"{entry['module']}.{entry['func']}()の呼び出しでエラー ({e})"
            )
            continue
        if resolved is None or not os.path.exists(resolved):
            ng.append(
                f"  [NG-34 config読み込み解決失敗] {label}: "
                f"{entry['module']}.{entry['func']}()がパスを解決できない "
                f"(resolved={resolved!r})。読み込み処理がサイレントに"
                f"フォールバック値を使用している可能性（自動修正なし）"
            )
    return ng


def _check_fixed_registry_integrity(ticker: str) -> list[str]:
    """CHECK-31: fixed_registry.json登録済みのticker×年度について、
    annual_{year}.jsonの現在のsnapshot_hashがregistry記録時のものと
    一致するかを検証する（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]の
    二次防御・CI検知）。NGメッセージのリストを返す（登録なし・全一致の
    場合は空リスト）。
    """
    registry = _load_fixed_registry().get(ticker, {})
    if not registry:
        return []

    ng: list[str] = []
    for year_str, entry in sorted(registry.items()):
        path = os.path.join(SEC_DATA_DIR, ticker, f"annual_{year_str}.json")
        expected_hash = entry.get("snapshot_hash")
        if not os.path.exists(path):
            ng.append(
                f"  [NG-31 fixed_registry不整合] {year_str}: fixed登録済みだが"
                f"annual_{year_str}.jsonが存在しない"
            )
            continue
        try:
            with open(path, encoding="utf-8") as f:
                current_data = json.load(f)
        except Exception as e:
            ng.append(
                f"  [NG-31 fixed_registry不整合] {year_str}: annual_{year_str}.json"
                f"読み込みエラー ({e})"
            )
            continue
        current_hash = compute_snapshot_hash(current_data)
        if current_hash != expected_hash:
            ng.append(
                f"  [NG-31 fixed_registry不整合] {year_str}: snapshot_hash不一致 "
                f"(registry={str(expected_hash)[:19]}..., current={current_hash[:19]}...) "
                f"→ fixed年度の値が意図せず変更された可能性（自動修正なし）"
            )
    return ng


def _get_annual_period_end(ticker: str, ann: dict) -> Optional[str]:
    """annual_YYYY.jsonの対象年度が表す実際の期末日（YYYY-MM-DD）を求める。

    annual_YYYY.jsonは`period`に年ラベル（例: 2025）しか保持しないため、
    決算期が12月以外の銘柄（COHR/KLAC等、2026-08-19時点で30銘柄確認済み）
    では年ラベルだけでyfinance側の対応する列（各列が期末日）を一意に
    特定できない（CHECK-35期ズレバグ、2026-08-19発見）。

    `bs_provenance`の本人データ（`is_own_data: True`）エントリのaccnを
    `load_submissions()`が返す`accn_to_reportdate`（submissions.json由来、
    SEC提出時点の報告期末日）で引く。全105銘柄で解決可能なことを確認済み。
    取得不可の場合はNoneを返す。
    """
    for field_prov in ann.get("bs_provenance", {}).values():
        if isinstance(field_prov, dict) and field_prov.get("is_own_data") and field_prov.get("accn"):
            accn_map = load_submissions(ticker)
            end = accn_map.get(field_prov["accn"])
            if end:
                return end
    return None


def _get_yf_financial_value(
    ticker: str, target_end: Optional[str], yf_row_label: str
) -> Optional[float]:
    """CHECK-35拡張（[[QUALITY-GATES-EPIC-1]]本線3、ゲート1第一歩、
    2026-08-19。2026-09-03、CHECK-41新設に合わせ`_get_yf_operating_
    income()`から汎用化——`yf_row_label`をパラメータ化し、
    `income_stmt`の任意の行を同じ照合ロジックで取得できるようにした。
    照合ロジック自体・関数のふるまいは変更していない）: yfinance
    income_stmtから、SECデータ側の対象年度と**期末日が一致する列**の
    `yf_row_label`行の値を取得する。

    **`yf_row_label`の選定（2026-09-03実測確認）**: `income_stmt`の
    行ラベルは想定と異なりうるため、実装前にAAPL/SITM/COHRの3銘柄で
    `pl.revenue`/`pl.net_income`とyfinance各候補行を突合して確認した。
    - revenue: `"Total Revenue"`（`"Operating Revenue"`も同値だが
      SEC側`revenue`タグの語感に近い前者を採用）— 3銘柄とも完全一致
    - net_income: `"Net Income"` — AAPL/SITMは候補4行（`Net Income`/
      `Net Income Common Stockholders`/`Net Income Including
      Noncontrolling Interests`/`Net Income From Continuing Operation
      Net Minority Interest`）が全て同値で判別不能だったが、COHRで
      `Net Income`=804,998,000のみがSEC`pl.net_income`
      （`NetIncomeLoss`タグ由来）と完全一致し、他3行はNCI・優先株
      配当等の調整後で乖離することを確認（`Net Income Common
      Stockholders`=769,896,000・`Net Income Including
      Noncontrolling Interests`=786,884,000）。`NetIncomeLoss`は
      NCI控除前・優先株調整前の値のため`"Net Income"`が正しい対応
    - operating_income: 従来通り`"Operating Income"`（変更なし）

    `common.yfinance_utils.safe_yf_ticker()`経由で呼び出す（リトライ・
    ログ出力の一元化を踏襲。既存のWARN-10/audit.pyのβ照合が使う
    `common.market_data.reader.get_attributes()`ローカルキャッシュには
    operating_income等の相当フィールドが存在しないため踏襲できず、単一
    ティッカーの直接取得に適したこちらの既存パターンを採用した。詳細は
    BACKLOG.md `[[QUALITY-GATES-EPIC-1]]`参照）。

    **期ズレバグの修正（2026-08-19）**: 当初`row.iloc[0]`（先頭列）を
    無条件に「直近確定年度」として使っていたが、決算期が12月以外の
    銘柄では、現在日時によってyfinance側の先頭列がSECデータの対象年度
    より1期先（予備的な値）になりうる（実例: COHR/KLACとも6月末決算で、
    2026-08-19時点でyfinance先頭列はFY2026・SECデータはFY2025を表して
    おり、KLACは誤って-11.4%の乖離ありと報告していたが正しくは0.0%
    だった）。位置（先頭列）ではなく期末日という本来照合すべき属性で
    列を選ぶよう修正した。

    **完全一致ではなく±10日の許容窓で照合する（2026-08-19、同日追加
    発見）**: 52/53週決算企業（JNJ等）はSEC側の実際の期末日が暦月末と
    一致しない（例: JNJ FY2025は`2025-12-28`）が、yfinanceは期末日を
    暦月末（`2025-12-31`）に正規化して格納するため、完全一致では
    「同じ会計年度なのに一致しない」偽陰性が発生する（JNJで実際に発生・
    2026-08-19発見）。52/53週のズレは最大でも1週間程度のため、±10日を
    許容窓とし、窓内で最も近い列を採用する。窓内に候補が無い場合は
    照合をスキップする（位置ベースの代理判定はしない）。

    **呼び出し頻度の制御は呼び出し元の責務**: CHECK-35
    （`_check_operating_income_reconstruction()`）はNone/derived判定
    銘柄のみ、CHECK-41（`_check_revenue_net_income_reconciliation()`）
    は全105銘柄を対象に呼ぶ。2026-09-03、`--include-yfinance-checks`
    フラグ新設によりこの関数の呼び出し自体を週次実行時のみに限定する
    設計へ変更（[[QUALITY-GATES-EPIC-1]]、詳細はCLI引数の説明参照）。
    取得失敗・データ不在・期間不一致時はNoneを返し、呼び出し元は照合を
    スキップする。
    """
    if target_end is None:
        return None
    try:
        target_date = datetime.strptime(target_end, "%Y-%m-%d").date()
    except ValueError:
        return None
    try:
        tk = safe_yf_ticker(ticker)
        if tk is None:
            return None
        fin = tk.income_stmt
        if fin is None or fin.empty or yf_row_label not in fin.index:
            return None
        row = fin.loc[yf_row_label].dropna()
        if len(row) == 0:
            return None
        best_col = None
        best_diff = None
        for col in row.index:
            try:
                col_date = col.to_pydatetime().date()
            except AttributeError:
                continue
            diff = abs((col_date - target_date).days)
            if diff <= 10 and (best_diff is None or diff < best_diff):
                best_col, best_diff = col, diff
        if best_col is None:
            return None
        return float(row[best_col])
    except Exception:
        return None


def _check_operating_income_reconstruction(
    ticker: str, include_yfinance: bool = False
) -> list[str]:
    """CHECK-35: operating_income（営業利益）が標準タグ`OperatingIncomeLoss`
    以外から再構成されている、または再構成にも失敗しNoneのままの銘柄を
    検知する（[[OPERATING-INCOME-EXTRACTION-GAP-1]]）。

    再構成の使用自体は正常動作（意図した設計）のためNGではなくWARN。
    開示打ち切りが新たに発生した銘柄・突き合わせ検証に失敗した銘柄を
    可視化する目的。

    **yfinance照合（2026-08-19拡張、[[QUALITY-GATES-EPIC-1]]本線3・
    ゲート1第一歩）**: None/derivedと判定された銘柄についてのみ、
    yfinance income_stmtのOperating Incomeを追加取得しWARN文言に含める。
    全105銘柄の実測分布（BACKLOG_DONE.md参照）では、標準タグ採用済みの
    「正常」銘柄でも会計年度ズレ・yfinance側の簡略化等によりp95で81%・
    最大342%の乖離が生じることを確認済みのため、**乖離率によるNG格上げは
    行わない**（Phase 2b-2の2.0倍/0.5倍閾値が19銘柄を誤検知した教訓を
    踏まえ、迷ったらWARNに留める判断）。あくまで検知精度向上（既存の
    None/derived判定に定量情報を添える）が目的。yfinance取得失敗時
    （データ不在・ネットワーク不調とも）は照合をスキップし、既存の
    WARN-35単体の挙動をそのまま維持する。

    **`include_yfinance`（2026-09-03、`--include-yfinance-checks`フラグ
    新設）**: `False`（デフォルト）の場合はyfinance呼び出し自体を行わない
    （`yf_str`は常に空文字）。None/derived判定ロジック自体はSECデータの
    週次更新（SEC_Data_Update.yml、日曜）に依存する一方、本チェックは
    従来毎日（TANUKI_VALUATION_Update.yml経由）実行され続けており、
    yfinance側だけ日次で呼んでも照合対象が変わらない無駄があったため
    是正した。詳細は[[QUALITY-GATES-EPIC-1]]参照。
    """
    warn: list[str] = []
    ticker_dir = os.path.join(SEC_DATA_DIR, ticker)
    if not os.path.exists(ticker_dir):
        return warn
    years = sorted(
        int(fn[7:11]) for fn in os.listdir(ticker_dir)
        if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
    )
    if not years:
        return warn
    latest_year = years[-1]
    path = os.path.join(ticker_dir, f"annual_{latest_year}.json")
    try:
        with open(path, encoding="utf-8") as f:
            ann = json.load(f)
    except Exception:
        return warn

    oi_val = ann.get("pl", {}).get("operating_income")
    oi_prov = ann.get("pl_provenance", {}).get("operating_income")
    target_end = _get_annual_period_end(ticker, ann)

    if oi_val is None:
        # 注: 妥当性ガード（net_income比較）で不採用になったケース（SOFI等）も
        # 含め、operating_income=Noneの理由はここでは区別できない
        # （parser.py::_backfill_operating_income()のprovenance仕様、
        # 詳細はそちらのコードコメント参照）。
        yf_oi = _get_yf_financial_value(ticker, target_end, "Operating Income") if include_yfinance else None
        yf_str = ""
        if yf_oi is not None and yf_oi != 0:
            yf_str = f" yfinance実測: {yf_oi:,.0f}（有意値あり、要確認）"
        warn.append(
            f"  [WARN-35 operating_income取得不可] FY{latest_year}: "
            f"標準タグ`OperatingIncomeLoss`・再構成（GP法/pretax調整法）の"
            f"いずれでも取得できず、または再構成が妥当性ガードで不採用。"
            f"moat_score/RICE-1のroic経路が参照不能"
            f"（既知の設計上の安全側フォールバックだが要注視）。{yf_str}"
        )
    elif oi_prov and oi_prov.get("derived"):
        source = oi_prov.get("source", "unknown")
        ratio = oi_prov.get("nonop_coverage_ratio")
        ratio_str = f" coverage_ratio={ratio:.2f}" if ratio is not None else ""
        yf_oi = _get_yf_financial_value(ticker, target_end, "Operating Income") if include_yfinance else None
        yf_str = ""
        if yf_oi is not None and yf_oi != 0:
            dev = (oi_val - yf_oi) / abs(yf_oi)
            yf_str = f" yfinance実測: {yf_oi:,.0f}（乖離{dev:+.1%}）"
        warn.append(
            f"  [WARN-35 operating_income再構成] FY{latest_year}: "
            f"source={source} value={oi_val:,.0f}{ratio_str}（標準タグ非報告、"
            f"[[OPERATING-INCOME-EXTRACTION-GAP-1]]の再構成ロジックで算出）。"
            f"{yf_str}"
        )
    return warn


# CHECK-35: ティッカー非依存の単発チェック用の基準件数。2026-08-16実装時点で
# 再構成・取得不可の対象は6銘柄（LLY/JNJ/XOM/KLAC/ASTS/COHR）。
# `OperatingIncomeLoss`の開示打ち切りは今後も発生しうる想定のため、ある程度の
# 増加は許容しつつ、急激な増加（開示慣行の構造変化等）だけを検知する目的で
# 現状件数の約2倍を基準値とする。
_OI_RECONSTRUCTION_BASELINE_COUNT = 12


def _check_operating_income_reconstruction_scope(tickers: list[str]) -> list[str]:
    """CHECK-35（集計）: 再構成・取得不可の対象銘柄数が基準値を超えていないか
    を確認する（[[OPERATING-INCOME-EXTRACTION-GAP-1]]）。個別銘柄の詳細は
    ticker単位のCHECK-35（`_check_operating_income_reconstruction`）を参照。

    tickers: 呼び出し元（main）が既にtanukiフラグ等で絞り込み済みの
    ティッカーリストをそのまま受け取る（FLAG-CONSUMER-AUDIT-2と同じ理由で
    os.listdir(SEC_DATA_DIR)によるルートディレクトリ直接スキャンを避ける）。
    """
    affected: list[str] = []
    for ticker in tickers:
        ticker_dir = os.path.join(SEC_DATA_DIR, ticker)
        if not os.path.isdir(ticker_dir):
            continue
        years = sorted(
            int(fn[7:11]) for fn in os.listdir(ticker_dir)
            if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
        )
        if not years:
            continue
        try:
            with open(os.path.join(ticker_dir, f"annual_{years[-1]}.json"), encoding="utf-8") as f:
                ann = json.load(f)
        except Exception:
            continue
        oi_val = ann.get("pl", {}).get("operating_income")
        oi_prov = ann.get("pl_provenance", {}).get("operating_income")
        if oi_val is None or (oi_prov and oi_prov.get("derived")):
            affected.append(ticker)

    if len(affected) > _OI_RECONSTRUCTION_BASELINE_COUNT:
        return [
            f"  [WARN-35 operating_income再構成対象の急増] "
            f"{len(affected)}銘柄（基準値{_OI_RECONSTRUCTION_BASELINE_COUNT}を超過）: "
            f"{', '.join(affected)} — `OperatingIncomeLoss`の開示打ち切りが"
            f"新たに複数銘柄で発生した可能性"
        ]
    return []


def _check_revenue_net_income_reconciliation(
    ticker: str, include_yfinance: bool = False
) -> list[str]:
    """CHECK-41（[[QUALITY-GATES-EPIC-1]]ゲート1拡張、2026-09-03新設）:
    `pl.revenue`（売上高）・`pl.net_income`（純利益）をyfinance
    income_stmtと突合する。

    CHECK-35（`_check_operating_income_reconstruction`）はNone/derived
    判定による対象限定（実測7銘柄前後）だったが、revenue/net_incomeは
    2026-09-03実測でNoneになる銘柄がほぼ皆無（revenue None 1件のみ、
    net_income Noneは0件）だったため、同じ限定方式では実効性が出ない。
    本チェックは**全105銘柄**を対象とし、値が存在する限り常にyfinance
    突合を試みる設計とした。

    **`yf_row_label`の選定根拠**は`_get_yf_financial_value()`の
    docstring参照（revenue→`"Total Revenue"`、net_income→
    `"Net Income"`、AAPL/SITM/COHR実測確認済み）。

    **乖離率は情報提供のみでNG格上げは行わない**（CHECK-35と同じ判断。
    revenue/net_incomeはoperating_incomeのような再構成〈GP法/pretax
    調整法〉を経ないため、当初はoperating_income〈p95=81%〉より狭い
    分布になると予想されたが、これは実装後の全105銘柄実測で検証し
    `[[QUALITY-GATES-EPIC-1]]`に記録する）。

    **`include_yfinance`（`--include-yfinance-checks`フラグ）が
    `False`（デフォルト）の場合は何もせず空リストを返す**——本チェック
    はyfinance突合そのものが目的のため、フラグ無効時に実行する意味が
    ない（CHECK-35のNone/derived判定部分のような「yfinance非依存の
    基本チェック」を持たない）。
    """
    warn: list[str] = []
    if not include_yfinance:
        return warn
    ticker_dir = os.path.join(SEC_DATA_DIR, ticker)
    if not os.path.exists(ticker_dir):
        return warn
    years = sorted(
        int(fn[7:11]) for fn in os.listdir(ticker_dir)
        if fn.startswith("annual_") and fn.endswith(".json") and fn[7:11].isdigit()
    )
    if not years:
        return warn
    latest_year = years[-1]
    path = os.path.join(ticker_dir, f"annual_{latest_year}.json")
    try:
        with open(path, encoding="utf-8") as f:
            ann = json.load(f)
    except Exception:
        return warn

    target_end = _get_annual_period_end(ticker, ann)
    sec_revenue = ann.get("pl", {}).get("revenue")
    sec_net_income = ann.get("pl", {}).get("net_income")

    if sec_revenue is not None:
        yf_revenue = _get_yf_financial_value(ticker, target_end, "Total Revenue")
        if yf_revenue is not None and yf_revenue != 0:
            dev = (sec_revenue - yf_revenue) / abs(yf_revenue)
            warn.append(
                f"  [WARN-41 revenue yfinance突合] FY{latest_year}: "
                f"SEC={sec_revenue:,.0f} yfinance={yf_revenue:,.0f}（乖離{dev:+.1%}）"
            )

    if sec_net_income is not None:
        yf_net_income = _get_yf_financial_value(ticker, target_end, "Net Income")
        if yf_net_income is not None and yf_net_income != 0:
            dev = (sec_net_income - yf_net_income) / abs(yf_net_income)
            warn.append(
                f"  [WARN-41 net_income yfinance突合] FY{latest_year}: "
                f"SEC={sec_net_income:,.0f} yfinance={yf_net_income:,.0f}（乖離{dev:+.1%}）"
            )

    return warn


def _check_moat_score_neutral_fallback(ticker: str, latest: dict) -> list[str]:
    """CHECK-36: moat_score=0.5が実測結果ではなく、最低2指標ルールによる
    中立フォールバック（有効指標<2のためのプレースホルダ）である銘柄を
    検知する（[[MOAT-SCORE-PARTIAL-NULL-1]]）。

    中立フォールバックの使用自体は正常動作（測定不能を誠実に示す設計）の
    ためNGではなくWARN。TANUKI SCORE等の下流分類がこの銘柄でプレースホルダ
    値に基づいて判定されている可能性を可視化する目的
    （実例: BKNGのTANUKI SCORE WATCH→BUYはこの中立フォールバックに由来、
    2026-08-16判明）。
    """
    warn: list[str] = []
    components = latest.get("components", {})
    source = components.get("moat_score_source")
    if source == "neutral_fallback":
        n_present = components.get("moat_score_n_present")
        warn.append(
            f"  [WARN-36 moat_score中立フォールバック] moat_score=0.5は実測では"
            f"なく最低2指標ルールによるプレースホルダ（有効指標n_present="
            f"{n_present}）。TANUKI SCORE等この値に依存する下流分類は測定不能な"
            f"モート強度に基づいている点に留意（自動修正なし）"
        )
    return warn


def _check_moat_score_validity(ticker: str, latest: dict) -> list[str]:
    """CHECK-42: moat_scoreがNone、または0〜1の範囲外の銘柄を検知する
    （[[CHECK-COVERAGE-1]]）。

    ※BACKLOG原案では「CHECK-20」を指定していたが、CHECK-20/CHECK-21は
    それぞれ別件（fcf_cagr floor値張り付き / Revenue段差型急変）で既に
    使用済みのため、本チェックはCHECK-42として新規採番する。

    calculate_moat_score()の戻り値は設計上0〜1に正規化される想定だが、
    上流の計算ロジック変更やデータ欠損によりNone・範囲外値が紛れ込む
    リグレッションを検知する防御的チェック（2026-09-04時点で全銘柄
    正常値のためWARN発火0件、将来の回帰防止が目的）。
    """
    warn: list[str] = []
    components = latest.get("components", {})
    moat_score = components.get("moat_score")
    if moat_score is None or not (0 <= moat_score <= 1):
        warn.append(
            f"  [WARN-42 moat_score異常] moat_score={moat_score!r} はNoneまたは"
            f"0〜1範囲外 → calculate_moat_score()の異常値混入の可能性"
        )
    return warn


def _check_dupont_null_validity(ticker: str, latest: dict) -> list[str]:
    """CHECK-43: dupontフィールドがNone（キー自体の欠落含む）なのに、
    最新annual_YYYY.jsonのstockholders_equityが正の値（負債超過ではない）
    の銘柄を検知する（[[CHECK-COVERAGE-2]]、[[CHECK-COVERAGE-1]]完了時に
    DuPont分解null検出分として切り出し）。

    pipeline.py::DuPont分解ロジック（`[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]`
    以降の「DuPont観測性統一」実装）は、計算不能な全ケース（negative_equity/
    revenue_too_small/total_assets_unavailable/equity_unavailable/
    ni_ttm_unavailable/revenue_unavailable/insufficient_data）で必ず
    `dupont`キーへ`{"excluded": True, "reason": "..."}`形式の理由付き辞書を
    設定する設計のため、latest.jsonの`dupont`が真にNone（キー自体が欠落）に
    なるのは、この一連のtry/except内で理由設定前に例外が発生し無言で
    握りつぶされた場合のみのはずである。負債超過（stockholders_equity<=0）
    の銘柄がこの経路でdupont=Noneになることは設計上想定されていないため、
    正の純資産を持つ銘柄でdupont=Noneが観測された場合はDuPont分解ロジック
    の回帰（例外の握りつぶし等）を示唆する（2026-09-06時点で全銘柄
    `{"excluded": ...}`形式の辞書かroe_decomposed算出済みのためWARN発火
    0件、将来の回帰防止が目的。CHECK-42と同型の防御的チェック）。
    """
    warn: list[str] = []
    dupont = latest.get("dupont")
    if dupont is not None:
        return warn
    bs, period = _read_latest_annual_bs(ticker)
    equity = bs.get("stockholders_equity")
    if equity is not None and equity > 0:
        warn.append(
            f"  [WARN-43 dupont異常] dupont=None（キー欠落）だがFY{period}"
            f" stockholders_equity={equity:,.0f}は正の値（負債超過ではない）"
            f" → DuPont分解ロジックの例外握りつぶし等による回帰の可能性"
        )
    return warn


# CHECK-36: ティッカー非依存の単発チェック用の基準件数。2026-08-16実装時点で
# 中立フォールバック対象は2銘柄（BKNG/CPRT）。今後の推移を見て閾値は調整する。
_MOAT_NEUTRAL_FALLBACK_BASELINE_COUNT = 4


def _check_moat_score_neutral_fallback_scope(tickers: list[str]) -> list[str]:
    """CHECK-36（集計）: moat_score中立フォールバック対象銘柄数が基準値を
    超えていないかを確認する（[[MOAT-SCORE-PARTIAL-NULL-1]]）。個別銘柄の
    詳細はticker単位のCHECK-36（`_check_moat_score_neutral_fallback`）を参照。
    """
    affected: list[str] = []
    for ticker in tickers:
        latest = _read_latest(ticker)
        if latest.get("components", {}).get("moat_score_source") == "neutral_fallback":
            affected.append(ticker)

    if len(affected) > _MOAT_NEUTRAL_FALLBACK_BASELINE_COUNT:
        return [
            f"  [WARN-36 moat_score中立フォールバック対象の急増] "
            f"{len(affected)}銘柄（基準値{_MOAT_NEUTRAL_FALLBACK_BASELINE_COUNT}を超過）: "
            f"{', '.join(affected)} — 有効指標<2の銘柄が新たに複数増加した可能性"
        ]
    return []


# CHECK-37: 直近提出から本日までの経過日数がこの窓（日）以内であれば
# 「SEC提出が直近に存在する」とみなす。四半期提出サイクル（約90日）＋
# 年次提出のばらつきを1周以上カバーする値として400日を採用。
_CHECK37_RECENT_FILING_WINDOW_DAYS = 400

# CHECK-37: 監視対象を全保有ポジションに拡大した方針変更日
# （`[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`、2026-08-19決定）。この日より
# 前に提出された決算はNG判定対象にしない（下記docstring「過渡期の扱い」
# 参照）。
_CHECK37_POLICY_CHANGE_DATE = "2026-08-19"


def _get_latest_10q_10k_filed_date(ticker: str) -> Optional[str]:
    """`company_facts.json`内の全us-gaapエントリのうち、formが
    10-Q/10-Q-A/10-K/10-K-Aのものに限定して最も新しい`filed`日付
    （YYYY-MM-DD文字列）を返す。データ不在・取得不可の場合はNone。
    """
    company_facts = load_company_facts(ticker)
    if not company_facts:
        return None
    latest_filed: Optional[str] = None
    try:
        for concept_data in company_facts.get("facts", {}).get("us-gaap", {}).values():
            for unit_data in concept_data.get("units", {}).values():
                for entry in unit_data:
                    if entry.get("form") not in ("10-Q", "10-Q/A", "10-K", "10-K/A"):
                        continue
                    filed = entry.get("filed")
                    if filed and (latest_filed is None or filed > latest_filed):
                        latest_filed = filed
    except Exception:
        return None
    return latest_filed


def _check_core_position_review_coverage() -> tuple[list[str], list[str]]:
    """CHECK-37: TAIL保有ポジション（2026-08-19以降は全保有ポジションが
    RSS監視・四半期レビュー自動生成の対象、`[[TAIL-COVERAGE-POLICY-
    UNDECIDED-1]]`で方針決定）について、直近にSEC提出（10-Q/10-K）が
    あるにもかかわらずレビューが1件も生成されていないものをNGとして
    検知する。

    **発見経緯（2026-08-19）**: 本チェックは狙って設計したものではない。
    `[[LAYER3-ANNUAL-CLASSIFICATION-DROPS-DATA-1]]`の範囲実測（Layer3の
    期間分類という無関係な調査）の副産物として、TAIL保有銘柄APGEが
    RSS監視パイプラインから漏れていた事実が偶然発覚した
    （`[[TAIL-SATELLITE-POSITION-MONITORING-GAP-1]]`）。「保有している
    のに監視されていない」状態を検出する仕組みがシステムのどこにも
    無かったため、同種の監視漏れが発生しても偶然の副産物でしか発見
    できない状態だった。これを塞ぐために本チェックを新設した。当初は
    core種別のみを対象としたが、2026-08-19②に監視対象そのものが全保有
    ポジションへ拡大されたため、本チェックの対象も合わせて拡大した
    （下記「対象銘柄の決定」参照）。

    **対象銘柄の決定（事例5の教訓を適用）**: `edgar_rss_monitor.
    get_monitored_tickers()`をそのまま呼び、監視対象の判定を本チェック
    側で再実装しない。判定ロジックを検査側で部分的に再現すると、本番の
    対象範囲の変更に検査側が追随できず、事例5と同型の誤検知を生む。
    `get_monitored_tickers()`が返さなかったポジション（現時点では
    通常存在しないが、thesisファイルのパース失敗等が起きた場合に
    現れうる）は、理由とともに戻り値のinfoに含め、呼び出し元が必ず
    表示する（沈黙除外を避けるための可視化。これが本チェックの再発防止
    の本体）。

    **「SEC提出が直近に存在する」の判定**: `company_facts.json`内の
    formが10-Q/10-Q-A/10-K/10-K-Aのエントリのうち最大`filed`日付を求め
    （`_get_latest_10q_10k_filed_date()`）、本日からの経過日数が
    `_CHECK37_RECENT_FILING_WINDOW_DAYS`（400日）以内であることとする。
    データ取得不可・提出が古い場合は判定対象外（NG化しない）。

    **過渡期の扱い（2026-08-19②追加）**: 監視対象拡大により新たに
    監視対象になったポジション（旧satellite）は、拡大前から蓄積して
    いた未レビュー決算の在庫を抱えている。これは`edgar_rss_monitor.py`
    の差分検知設計（初回実行はベースライン記録のみでキューに追加しない
    ため、既存の未レビュー在庫は自動的には解消しない、2026-08-19②の
    実地検証で確認）の帰結であり、次回以降の新規提出で自然に監視が
    始まる性質のものであって、CI基盤の障害ではない。これをNGとして
    出し続けるのは不適切な一方、baselineへ登録して恒久的に黙らせるのも
    不適切なため、**`latest_filed`が`_CHECK37_POLICY_CHANGE_DATE`
    （方針変更日、2026-08-19）以降の提出のみ**をNG判定対象とする。
    方針変更日より前に提出された既存の未レビュー在庫はNG化しない
    （`[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`に実測件数を記録済み）。

    **「レビューが1件も生成されていない」の判定**: `REVIEWS_DIR`配下の
    `{ticker}_*_review.json`実ファイルと、`review_queue.json`
    （`quarterly_review_generator.load_queue()`経由）の両方を見る。
    どちらか一方でも存在すれば「生成されている／生成待ち」とみなし
    NG化しない（片方だけを見ると、キュー投入済みだが未生成の正常な
    待ち状態を誤検知するため）。

    Returns:
        (ng_list, info_list) — infoにはget_monitored_tickers()から
        漏れたポジションの表示を含む（通常は空）。
    """
    from src.tail.edgar_rss_monitor import get_monitored_tickers, get_excluded_positions
    from src.tail.quarterly_review_generator import REVIEWS_DIR, load_queue

    ng: list[str] = []
    info: list[str] = []

    for ex_ticker, ex_type in get_excluded_positions():
        info.append(
            f"  [INFO-37] {ex_ticker}: type={ex_type} のため監視対象外"
            f"（get_monitored_tickers()から除外、thesisファイル異常等の"
            f"可能性、要確認）"
        )

    try:
        queue_tickers = {e.get("ticker") for e in load_queue().get("queue", [])}
    except Exception:
        queue_tickers = set()

    for ticker in get_monitored_tickers():
        latest_filed = _get_latest_10q_10k_filed_date(ticker)
        if latest_filed is None:
            continue
        try:
            filed_date = datetime.strptime(latest_filed, "%Y-%m-%d").date()
        except ValueError:
            continue
        days_since = (datetime.now().date() - filed_date).days
        if days_since > _CHECK37_RECENT_FILING_WINDOW_DAYS:
            continue
        if latest_filed < _CHECK37_POLICY_CHANGE_DATE:
            continue

        has_review_file = False
        if os.path.exists(REVIEWS_DIR):
            has_review_file = any(
                f.startswith(f"{ticker}_") and f.endswith("_review.json")
                for f in os.listdir(REVIEWS_DIR)
            )
        has_queue_entry = ticker in queue_tickers

        if not has_review_file and not has_queue_entry:
            ng.append(
                f"  [NG-37 保有ポジションの監視漏れ] {ticker}: 方針変更日"
                f"（{_CHECK37_POLICY_CHANGE_DATE}）以降・直近"
                f"{_CHECK37_RECENT_FILING_WINDOW_DAYS}日以内に10-Q/10-K提出あり"
                f"（最終filed={latest_filed}）だが、レビューが1件も生成されて"
                f"おらずキューにも投入されていない → RSS監視パイプラインから"
                f"の脱落の可能性（自動修正なし、要調査）"
            )

    return ng, info


# CHECK-38: baselineファイルの配置。fixed_registry.jsonと同じ
# `common/sec_data/`ではなく`config/`に置く（tail_kpi_map.json等、
# TAIL関連の手動設定ファイルは`config/`配下に統一する既存方針
# 〈TAILKPI-CONFIG-LOCATION-1〉に倣う）。
_TAIL_KPI_BASELINE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "config", "tail_kpi_fetch_baseline.json",
)


def _check_kpi_fetch_failures() -> tuple[list[str], list[str]]:
    """CHECK-38: TAIL保有ポジションの登録済みKPIのうち、`xbrl_segment_
    fetcher.py`が実際に値を取得できなかったもの（`{ticker}_layer2.json`
    の`missing_kpis`）を検知する。

    **発見経緯（2026-08-19⑤）**: `[[TAIL-KPI-PROPOSER-CORE-ONLY-
    GATE-1]]`のsatellite対応実装後、KPI提案・登録という入口は解消した
    ものの、値の取得という出口が構造的に（非セグメント指標の取得不可、
    `[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-GAP-1]]`）・個別のタグ
    精度により（`[[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]`）失敗して
    いることが判明した。`xbrl_segment_fetcher.py::fetch_ticker()`は
    `missing_kpis`があっても常に`True`を返し、CI（`TANUKI_TAIL_KPI_
    Update.yml`）は毎週GREENで完走し続けるため、この失敗は本チェック
    新設まで系統的に検知されていなかった（core 3銘柄で過去9四半期中
    最大7四半期、既存レビューに「KPI不足」が沈黙して混入していたことを
    実測確認済み）。

    **対象銘柄の決定（事例5の教訓）**: `edgar_rss_monitor.
    get_monitored_tickers()`をそのまま使う（CHECK-37と同じ入口）。

    **NG/WARNの設計**: 2026-08-19時点で39/52件が既に失敗しており、
    これを即座にNG化すると全チェックがブロックされる一方、黙らせる
    ことは本セッションで繰り返し否定してきたサイレント・フォール
    バックと同型になる。そのため:
    - 失敗が1件でもあればWARNとして出し、**件数を必ず表示する**
      （`{ticker}: KPI {missing}/{total}件が取得失敗`）
    - `config/tail_kpi_fetch_baseline.json`に記録済みの銘柄別
      missing_count（baselineは記録日時点の実測値で、**許容値では
      なく是正目標**——ファイル内`_meta`に明記）を超えて悪化した場合
      のみNGとする
    - baselineに存在しない銘柄（新規追加等）は悪化判定をスキップし
      WARNのみ（比較対象がないため）

    Returns:
        (ng_list, warn_list)
    """
    from src.tail.edgar_rss_monitor import get_monitored_tickers
    from src.tail.quarterly_review_generator import KPI_DIR

    ng: list[str] = []
    warn: list[str] = []

    try:
        with open(_TAIL_KPI_BASELINE_PATH, encoding="utf-8") as f:
            baseline = json.load(f)
    except Exception:
        baseline = {}

    for ticker in get_monitored_tickers():
        layer2_path = os.path.join(KPI_DIR, f"{ticker}_layer2.json")
        if not os.path.exists(layer2_path):
            continue
        try:
            with open(layer2_path, encoding="utf-8") as f:
                layer2 = json.load(f)
        except Exception:
            continue

        missing = layer2.get("missing_kpis", [])
        total = len(layer2.get("kpis", {}))
        if not missing:
            continue

        warn.append(
            f"  [WARN-38 KPI取得失敗] {ticker}: KPI {len(missing)}/{total}件が"
            f"取得失敗（{', '.join(missing)}）"
        )

        base_entry = baseline.get(ticker)
        if base_entry is None:
            continue
        base_count = base_entry.get("missing_count", 0)
        if len(missing) > base_count:
            ng.append(
                f"  [NG-38 KPI取得失敗の悪化] {ticker}: baseline"
                f"（{base_entry.get('recorded_at', '不明')}時点）{base_count}件"
                f" → 現在{len(missing)}件に悪化"
            )

    return ng, warn


def _check_kpi_rejected_proposals() -> tuple[list[str], list[str]]:
    """CHECK-39: `kpi_proposer.py`が登録前の実取得検証で却下したKPI
    （`kpi_proposals/{ticker}_proposal.json`の`rejected_kpis`）を検知
    する。

    **発見経緯（2026-08-19⑧）**: `[[TAIL-XBRL-MEMBER-VALIDATION-
    GAP-1]]`で「登録前に本番の取得経路で実際に値が取れるか試し、取れた
    ものだけ登録する」方式に切り替えた結果、satelliteの実取得成功KPIは
    0件→14件に改善した。しかし副作用として、**却下された22件（後に
    APGE分を含め26件と判明）が`rejected_kpis`という、CHECK-38の集計
    対象外の場所へ移動していた**。CHECK-38は`{ticker}_layer2.json`の
    `missing_kpis`（＝登録したが取れなかったKPI）だけを見ており、
    `rejected_kpis`（＝必要と判断されたが登録すらされなかったKPI）は
    可視化する仕組みが無かった。「失敗を検知する仕組みを改善した
    つもりが、失敗の置き場所を変えただけで検知範囲から外れていた」
    という、本セッションで繰り返し否定してきたサイレント・フォール
    バックと同型の問題を、今回は対応の副作用として自分たちで新たに
    作ってしまっていた（`CHAT_RULES.md`事例7参照）。

    **`missing_kpis`と`rejected_kpis`は意味が異なるため、CHECK-38とは
    別のCHECK番号・別のbaselineキー（`rejected_count`）で管理し、
    件数を混ぜない。**
    - `missing_kpis`（CHECK-38）＝登録したが値が取れなかった
    - `rejected_kpis`（CHECK-39、本関数）＝必要と判断されたが実取得
      検証で登録すらされなかった

    Returns:
        (ng_list, warn_list)
    """
    from src.tail.edgar_rss_monitor import get_monitored_tickers
    from src.tail.kpi_proposer import KPI_PROPOSALS_DIR

    ng: list[str] = []
    warn: list[str] = []

    try:
        with open(_TAIL_KPI_BASELINE_PATH, encoding="utf-8") as f:
            baseline = json.load(f)
    except Exception:
        baseline = {}

    for ticker in get_monitored_tickers():
        proposal_path = os.path.join(KPI_PROPOSALS_DIR, f"{ticker}_proposal.json")
        if not os.path.exists(proposal_path):
            continue
        try:
            with open(proposal_path, encoding="utf-8") as f:
                proposal = json.load(f)
        except Exception:
            continue

        rejected = proposal.get("rejected_kpis", [])
        if not rejected:
            continue

        names = [r.get("name", "?") for r in rejected]
        warn.append(
            f"  [WARN-39 KPI却下] {ticker}: {len(rejected)}件が登録前の"
            f"実取得検証で却下（{', '.join(names)}）"
        )

        base_entry = baseline.get(ticker)
        if base_entry is None:
            continue
        base_count = base_entry.get("rejected_count", 0)
        if len(rejected) > base_count:
            ng.append(
                f"  [NG-39 KPI却下の悪化] {ticker}: baseline"
                f"（{base_entry.get('recorded_at', '不明')}時点）{base_count}件"
                f" → 現在{len(rejected)}件に悪化"
            )

    return ng, warn


# CHECK-40: baselineファイルはCHECK-38/39と同じ`config/`配下に配置。
_DCF_VALIDATION_BASELINE_PATH = os.path.join(REPO_ROOT, "config", "dcf_validation_baseline.json")


def _check_dcf_validation_failures(tickers: list[str]) -> tuple[list[str], list[str]]:
    """CHECK-40: TANUKI VALUATIONの`validator.py::run_basic_checks()`が
    本番パイプライン実行時に既に算出している`{ticker}/latest.json`の
    `validation.overall`（PASS/WARN/FAIL）を検知する。

    **発見経緯（2026-08-20、`[[QUALITY-GATES-EPIC-1]]`ゲート3棚卸しの
    追加調査）**: `validator.py::run_basic_checks()`は`pt_shares_
    consistency`（P_t/shares再計算突合）・`dcf_components`（DCF構成
    要素の合計突合）・`formula_verification`（α公式の教科書的再計算
    突合）・`anomaly_detection`（異常値の性質検査）という、ゲート3
    （計算式検証）が求める検証をpipeline.py実行時に全銘柄で既に行って
    いた。しかし結果は`latest.json`の`validation`フィールドと
    `stock.html`の個別ページ表示にのみ残り、`report_consistency_
    check.py`・`audit.py`・pytestのいずれからも一切参照されておらず、
    FAILが出ていても個別ページを開かない限り誰も気づけない沈黙構造
    だった（CHECK-32〜36と同型のパターン）。本チェックは新規の検証
    ロジックを実装せず、既に生成済みの`validation`フィールドを読んで
    集約表示するだけに留める。

    **対象銘柄の決定（事例5の原則）**: 呼び出し元がその都度
    `common.sec_data.tickers.get_tanuki_tickers()`（本番の一覧取得
    関数）で構築した`tickers`をそのまま受け取る。本関数内で独自の
    銘柄一覧を再構築しない。

    **NG/WARNの設計（CHECK-38と同じ考え方）**:
    - `validation.overall`が`PASS`以外の銘柄が1件でもあればWARNとして
      出し、**件数・overall・不合格チェック名を必ず表示する**
    - `config/dcf_validation_baseline.json`に記録済みの銘柄別
      `fail_count`（不合格サブチェック数、baselineは記録日時点の実測値
      で**許容値ではなく是正目標**——同ファイル`_meta`に明記）を超えて
      悪化した場合のみNGとする
    - baselineに存在しない銘柄（新規追加等）は悪化判定をスキップし
      WARNのみ（比較対象がないため）

    Returns:
        (ng_list, warn_list)
    """
    ng: list[str] = []
    warn: list[str] = []

    try:
        with open(_DCF_VALIDATION_BASELINE_PATH, encoding="utf-8") as f:
            baseline = json.load(f)
    except Exception:
        baseline = {}

    for ticker in tickers:
        latest_path = os.path.join(DATA_DIR, ticker, "latest.json")
        if not os.path.exists(latest_path):
            continue
        try:
            with open(latest_path, encoding="utf-8") as f:
                latest = json.load(f)
        except Exception:
            continue

        validation = latest.get("validation")
        if not validation:
            continue

        overall = validation.get("overall", "PASS")
        checks = validation.get("checks", {})
        failed_names = [name for name, res in checks.items() if not res.get("pass", True)]
        if not failed_names:
            continue

        warn.append(
            f"  [WARN-40 DCF検証不合格] {ticker}: validation.overall="
            f"{overall}（不合格チェック: {', '.join(failed_names)}）"
        )

        base_entry = baseline.get(ticker)
        if base_entry is None:
            continue
        base_count = base_entry.get("fail_count", 0)
        if len(failed_names) > base_count:
            ng.append(
                f"  [NG-40 DCF検証不合格の悪化] {ticker}: baseline"
                f"（{base_entry.get('recorded_at', '不明')}時点）{base_count}件"
                f" → 現在{len(failed_names)}件に悪化（overall={overall}）"
            )

    return ng, warn


def annotate_warn(ticker: str, message: str, ledger: set[tuple[str, str]]) -> tuple[str, bool]:
    """
    WARNメッセージに台帳照合結果を反映する。

    Returns:
        (表示用メッセージ, is_new) — is_new=Trueは台帳未登録（未確認）WARN
    """
    m = _WARN_CHECK_RE.search(message)
    if not m:
        return message, True
    check_id = f"WARN-{m.group(1)}"
    if (check_id, ticker) in ledger:
        return message, False
    return message.replace("[WARN-", "[\U0001f195未確認 WARN-", 1), True


def _read_report(ticker: str):
    path = os.path.join(DATA_DIR, ticker, "report.txt")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return f.read()


def _read_latest(ticker: str) -> dict:
    path = os.path.join(DATA_DIR, ticker, "latest.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _read_fy_collision_log(ticker: str) -> list:
    """
    parser.py が本人データ(reportDate==end_date)同士のfyキー衝突を検知した
    際に書き出す common/sec_data/data/{ticker}/fy_collision_log.json を読む。
    CRM/FCX/CAKE/HON/COHR/AVAV/FICO/NVDA等で実在確認済み（filing代行者側の
    タグ付け起因と推測。原因追及は対象外、tie-break結果の継続監視が目的）。
    """
    path = os.path.join(SEC_DATA_DIR, ticker, "fy_collision_log.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("collisions", [])
    except Exception:
        return []


def _read_fy_tag_mismatch_log(ticker: str) -> list:
    """
    ARCH-DATA-1ステージ3（fyタグ裏取り）: parser.pyが年度バケツキー
    （determine_fiscal_year()の計算結果）と採用エントリの生XBRL fyタグの
    食い違いを検知した際に書き出す
    common/sec_data/data/{ticker}/fy_tag_mismatch_log.json を読む。
    CHECK-22（同一fyタグへの複数本人end_date競合）とは独立した別軸のチェックで、
    「fyタグは単一だが値の年度バケツ配置自体がfyタグと異なる」ケース
    （CDNS型）を対象とする。
    """
    path = os.path.join(SEC_DATA_DIR, ticker, "fy_tag_mismatch_log.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("mismatches", [])
    except Exception:
        return []


def _read_fye_boundary_collision_log(ticker: str) -> list:
    """
    FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1: parser.pyが決算期変更の境界年で
    「本人データ」と生fyタグが異なる別エントリ（end_dateも異なる）が同一年度
    バケツで競合したケースを検知した際に書き出す
    common/sec_data/data/{ticker}/fye_boundary_collision_log.json を読む。
    CHECK-22（同一fyタグへの複数本人end_date競合）・CHECK-23（勝者自身の
    fyタグとバケツの不一致）のいずれとも異なる軸で、競合する2エントリの
    生fyタグ・end_dateが両方とも異なるケース（RCAT型）を対象とする。
    """
    path = os.path.join(SEC_DATA_DIR, ticker, "fye_boundary_collision_log.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("collisions", [])
    except Exception:
        return []


def _read_bs_identity_violations_log(ticker: str) -> list:
    """
    [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]: parser.pyが会計恒等式
    Total_Assets = Total_Liabilities + Stockholders_Equity（+NCI+一時的
    持分）の検証結果を書き出す
    common/sec_data/data/{ticker}/bs_identity_violations_log.json を読む。
    本体一致で解消したケースはparser.py側で記録対象外済みのため、ここには
    ①拡張形（NCI・一時的持分の許可リスト加算）で解消したケースと
    ②いずれでも解消しないケースのみが含まれる。
    """
    path = os.path.join(SEC_DATA_DIR, ticker, "bs_identity_violations_log.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("violations", [])
    except Exception:
        return []


# FY52WEEK-BS-NULL-SILENT-1 Phase A対象フィールド。全105銘柄実測でNone率が
# ほぼ0-4%（CASH-TAG-MISSING-1系の既知欠落を除けばほぼ確実にデータ異常の
# シグナル）のBS項目に限定する。short_term_investments/long_term_debt/
# short_term_debt（真のゼロとの判別困難）・rpo（非SaaS銘柄はNoneが正常）は
# Phase B/Cとして対象外。
_BS_NULL_CHECK_FIELDS = [
    "total_assets", "stockholders_equity", "total_liabilities",
    "cash_and_equivalents", "current_assets", "current_liabilities",
]


def _read_latest_annual_bs(ticker: str) -> tuple[dict, str]:
    """最新のannual_YYYY.jsonのbs辞書とperiod文字列を返す。存在しなければ({}, "")"""
    files = sorted(glob.glob(os.path.join(SEC_DATA_DIR, ticker, "annual_*.json")))
    if not files:
        return {}, ""
    try:
        with open(files[-1], encoding="utf-8") as f:
            d = json.load(f)
        return d.get("bs", {}) or {}, str(d.get("period", ""))
    except Exception:
        return {}, ""


def _read_eps_quarterly(ticker: str) -> list:
    path = os.path.join(EPS_DATA_DIR, ticker, "quarterly.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, list) else d.get("quarters", [])
    except Exception:
        return []


# ─── パーサ ──────────────────────────────────────────────────

def _parse_report(text: str) -> dict:
    """report.txt から必要フィールドを抽出して dict で返す。"""
    lines = text.splitlines()

    result = {
        "classification": None,
        "dcf_reliability": None,
        "matrix_type": None,
        "key_metric_y": None,
        "label": None,
        "has_fcf_base": False,
        "has_fcf_conversion_rate": False,
        "discount_rate_primary_data": None,  # data行のみ（定義行は除外）
        "has_wacc_old": False,               # 旧 WACC: 単独行
        "has_net_debt_report": False,
        "has_st_invest_report": False,
        "per_gaap_value": None,
        "rpo_pv_value": None,
        "rpo_pv_line": None,
        "fcf_history": [],                   # [(year, neg_fcf, margin_or_None)]
        "_raw_lines": lines,                 # CHECK-9 用
    }

    in_fcf_section = False

    for line in lines:
        # FCF_History セクション
        if line.strip() == "FCF_History:":
            in_fcf_section = True
            continue
        if in_fcf_section:
            # 年次 FCF 行: "  2025: $-7.19B (FCF_Margin: -140.2%)"
            # または:      "  2023: $-0.27B"
            m = re.match(
                r'^\s{2}(\d{4}): \$([^\s(]+)'
                r'(?:\s+\(FCF_Margin:\s*([+-]?\d+\.?\d*)%\))?',
                line,
            )
            if m:
                year = int(m.group(1))
                fcf_str = m.group(2)   # e.g. "-7.19B" or "9.11B"
                neg_fcf = fcf_str.startswith("-")
                margin  = float(m.group(3)) if m.group(3) is not None else None
                result["fcf_history"].append((year, neg_fcf, margin))
                continue
            # 空行や次セクション開始でリセット
            if line.strip() == "" or (line and not line.startswith(" ")):
                in_fcf_section = False

        # Classification（最初のマッチのみ）
        if result["classification"] is None:
            m = re.match(r'^Classification:\s+(\S+)', line)
            if m:
                result["classification"] = m.group(1)

        # DCF_Reliability（データ行: "DCF_Reliability: LOW ⚠️..." or "...NORMAL"）
        # [[DCF-RELIABILITY-LABEL-MISMATCH-1]]対応（2026-08-30）: 表示語彙を
        # NORMALへ統一する前はFCF_Base方式のみHIGHと表示していたが、この
        # 正規表現自体はどちらの語彙でも影響を受けない（\wでそのまま捕捉）。
        if result["dcf_reliability"] is None:
            m = re.match(r'^DCF_Reliability:\s+(\w+)', line)
            if m:
                result["dcf_reliability"] = m.group(1)  # "LOW" or "NORMAL"

        # Matrix: （最初のマッチ）
        if result["matrix_type"] is None:
            m = re.match(r'^Matrix:\s+(.+)', line)
            if m:
                result["matrix_type"] = m.group(1).strip()

        # Key_Metric_Y: （最初のマッチ）
        if result["key_metric_y"] is None:
            m = re.match(r'^Key_Metric_Y:\s+(.+)', line)
            if m:
                result["key_metric_y"] = m.group(1).strip()

        # Label: （最初のマッチ）
        if result["label"] is None:
            m = re.match(r'^Label:\s+(.+)', line)
            if m:
                result["label"] = m.group(1).strip()

        # FCF_Base: 行の存在
        if not result["has_fcf_base"]:
            if re.match(r'^FCF_Base:', line):
                result["has_fcf_base"] = True

        # FCF_Conversion_Rate: 行の存在（DCF-RELIABILITY-1: Policy B対象銘柄の判定）
        if not result["has_fcf_conversion_rate"]:
            if re.match(r'^FCF_Conversion_Rate:', line):
                result["has_fcf_conversion_rate"] = True

        # Discount_Rate_Primary データ行（"10.00% (DCF discount rate used)"）
        # 定義行は "Discount_Rate_Primary: Actual discount rate..." → 除外
        if result["discount_rate_primary_data"] is None:
            m = re.match(r'^Discount_Rate_Primary:\s+([\d.]+)%', line)
            if m:
                result["discount_rate_primary_data"] = m.group(1)

        # 旧 WACC: 単独行（"WACC: 12.0%"）
        if not result["has_wacc_old"]:
            if re.match(r'^WACC:\s+[\d.]+%', line):
                result["has_wacc_old"] = True

        # Net_Debt 行（Financial_Health セクション内 "  Net_Debt: ..."）
        if not result["has_net_debt_report"]:
            if re.match(r'^\s+Net_Debt:\s+\$', line):
                result["has_net_debt_report"] = True

        # ST_Invest 表記（"ST_Invest:" を含む行）
        if not result["has_st_invest_report"]:
            if "ST_Invest:" in line:
                result["has_st_invest_report"] = True

        # Market_PER_GAAP（Financial_Health セクション内）
        if result["per_gaap_value"] is None:
            m = re.match(r'^\s+Market_PER_GAAP:\s+(.+)', line)
            if m:
                result["per_gaap_value"] = m.group(1).strip()

        # RPO_PV データ行（"RPO_PV: $NNN ..."）
        if result["rpo_pv_value"] is None:
            m = re.match(r'^RPO_PV:\s+\$([0-9,]+)', line)
            if m:
                try:
                    result["rpo_pv_value"] = float(m.group(1).replace(",", ""))
                    result["rpo_pv_line"]  = line.strip()
                except ValueError:
                    pass

    return result


# ─── チェック本体 ─────────────────────────────────────────────

def check_ticker(ticker: str, whitelist: set, include_yfinance: bool = False) -> tuple[list, list]:
    """
    Returns (issues_ng, issues_warn)
    各要素は表示用文字列。

    include_yfinance: `--include-yfinance-checks`フラグ（2026-09-03新設、
    デフォルトFalse）。TrueのときのみCHECK-35/41のyfinance突合部分を
    実行する。詳細はCLI引数の説明・[[QUALITY-GATES-EPIC-1]]参照。
    """
    ng: list[str]   = []
    warn: list[str] = []

    # CHECK-31: fixed_registry.json整合性検知。common/sec_data/側の検証
    # であり、TANUKI VALUATION出力（report.txt）の有無に依存しないため
    # report.txt存在チェックより前に実行する。
    ng.extend(_check_fixed_registry_integrity(ticker))

    # CHECK-35: operating_income再構成・取得不可の検知。CHECK-31と同様、
    # common/sec_data/側の検証でありreport.txtに依存しない。
    warn.extend(_check_operating_income_reconstruction(ticker, include_yfinance))

    # CHECK-41: revenue/net_incomeのyfinance突合（[[QUALITY-GATES-
    # EPIC-1]]ゲート1拡張、2026-09-03新設）。CHECK-35と同様、
    # common/sec_data/側の検証でありreport.txtに依存しない。
    warn.extend(_check_revenue_net_income_reconciliation(ticker, include_yfinance))

    text = _read_report(ticker)
    if text is None:
        return ng, warn

    latest  = _read_latest(ticker)
    warn.extend(_check_moat_score_neutral_fallback(ticker, latest))
    warn.extend(_check_moat_score_validity(ticker, latest))
    warn.extend(_check_dupont_null_validity(ticker, latest))
    parsed  = _parse_report(text)

    fcf_hist = parsed["fcf_history"]
    latest_entry = max(fcf_hist, key=lambda x: x[0]) if fcf_hist else None

    # CHECK-1: FCF符号矛盾
    mt = parsed["matrix_type"] or ""
    kmy = parsed["key_metric_y"] or ""
    if "④" in mt and "FCF_Margin" in kmy and latest_entry:
        m = re.search(r'FCF_Margin\s*=\s*([+-]?\d+\.?\d*)%', kmy)
        if m:
            key_margin = float(m.group(1))
            _, latest_neg, latest_margin = latest_entry
            # 最新FCFがマイナスなのに Key_Metric_Y が正値
            if latest_neg and key_margin > 0:
                ng.append(
                    f"  [NG-1 FCF符号矛盾] 最新FCH({latest_entry[0]})マイナス"
                    f" & Key_Metric_Y FCF_Margin={key_margin:+.1f}%"
                )
                ng.append(f"    → {kmy}")

    # CHECK-2: DCF_Reliability欠落
    if parsed["has_fcf_base"] and parsed["dcf_reliability"] is None:
        ng.append("  [NG-2 DCF_Reliability欠落] FCF_Base行あり & DCF_Reliability行なし")
    # DCF-RELIABILITY-1: FCF_Conversion_Rate方式（Policy B対象）でも同様に欠落を検出
    if parsed["has_fcf_conversion_rate"] and parsed["dcf_reliability"] is None:
        ng.append("  [NG-2 DCF_Reliability欠落] FCF_Conversion_Rate行あり & DCF_Reliability行なし")

    # CHECK-3: LOW丸め未発動
    rel = parsed["dcf_reliability"]
    cls = parsed["classification"]
    if rel == "LOW" and cls not in ("WATCH", "SELL", "PASS", None):
        ng.append(
            f"  [NG-3 LOW丸め未発動] DCF_Reliability=LOW & Classification={cls}"
        )

    # CHECK-4: 割引率1段
    if parsed["discount_rate_primary_data"] is None:
        if parsed["has_wacc_old"]:
            ng.append("  [NG-4 割引率1段] Discount_Rate_Primary行なし・旧WACC単独形式")
        else:
            ng.append("  [NG-4 割引率1段] Discount_Rate_Primary行が存在しない")

    # CHECK-5: NetDebt旧表示 (警告)
    if parsed["has_net_debt_report"] and not parsed["has_st_invest_report"]:
        fh = latest.get("financial_health", {}) or {}
        st_inv = fh.get("short_term_investments") or 0
        if st_inv and st_inv != 0.0:
            warn.append(
                f"  [WARN-5 NetDebt旧表示] Net_Debt行あり & ST_Invest非ゼロ({st_inv:,.0f})"
                " だが報告行なし"
            )

    # CHECK-6: 負PER数値表示 (警告)
    pv = parsed["per_gaap_value"] or ""
    if re.match(r'^-[\d.]+', pv):
        warn.append(f"  [WARN-6 負PER数値表示] Market_PER_GAAP: {pv}  (N/M 未変換)")

    # CHECK-7: RPO条件違反
    rpo_pv = parsed["rpo_pv_value"]
    if rpo_pv is not None and rpo_pv > 0 and ticker not in whitelist:
        comp    = latest.get("components", {}) or {}
        rpo_raw = comp.get("rpo") or 0
        rev_ttm = comp.get("latest_revenue") or 0
        if rev_ttm > 0 and rpo_raw > 0:
            ratio = rpo_raw / rev_ttm
            if ratio < 0.30:
                ng.append(
                    f"  [NG-7 RPO条件違反] RPO_PV={rpo_pv:,.0f} >0"
                    f" & whitelist外 & RPO/Rev={ratio:.2f}<0.30"
                )
                ng.append(f"    → {parsed['rpo_pv_line']}")

    # CHECK-8: Matrix④高FCFラベルだが実績赤字
    lbl = parsed["label"] or ""
    if "④" in mt and "高FCF" in lbl and latest_entry:
        _, latest_neg, _ = latest_entry
        if latest_neg:
            ng.append(
                f"  [NG-8 Matrix④高FCFラベル赤字]"
                f" Label={lbl!r} & 最新FCF({latest_entry[0]})実績マイナス"
            )

    # CHECK-9: セグメント設定鮮度 (警告)
    # segment_configのfiscal_yearが2年以上前の場合、陳腐化の可能性を警告
    seg_cfg = _load_seg_config().get(ticker, {})
    if seg_cfg.get("enabled") and seg_cfg.get("fiscal_year"):
        fy_str = seg_cfg["fiscal_year"]  # e.g. "FY2025"
        m_fy = re.match(r"FY(\d{4})", fy_str)
        if m_fy:
            fy_yr = int(m_fy.group(1))
            # report内のGenerated行から生成年を取得
            gen_yr = None
            for line in (parsed.get("_raw_lines") or []):
                mm = re.search(r"Generated: (\d{4})-", line)
                if mm:
                    gen_yr = int(mm.group(1))
                    break
            if gen_yr and (gen_yr - fy_yr) >= 2:
                warn.append(
                    f"  [WARN-9 セグメント設定陳腐化] segment_config fiscal_year={fy_str}"
                    f" (現在{gen_yr}年、{gen_yr - fy_yr}年前のデータ)"
                )

    # CHECK-10: PS異常値 (警告)
    # yfinance PSが自社計算値(price×shares/revenue)と大きく乖離する場合にWARN
    comp = latest.get("components", {}) or {}
    ps_yf   = comp.get("ps")
    price   = comp.get("current_price") or 0
    shares  = comp.get("diluted_shares") or 0
    rev     = comp.get("latest_revenue") or 0
    sector  = (comp.get("sector") or "").lower()
    is_fin  = "financial" in sector or "bank" in sector
    if ps_yf is not None and price and shares and rev and not is_fin:
        ps_calc = (price * shares) / rev
        if ps_calc > 0:
            ratio = ps_yf / ps_calc
            if ratio > 2.5 or ratio < 0.4:
                warn.append(
                    f"  [WARN-10 PS異常値] yfinance PS={ps_yf:.1f}x vs 自社計算={ps_calc:.1f}x"
                    f" (乖離{ratio:.1f}倍) → ステール値の可能性"
                )

    # CHECK-11: Revenue桁違い (NG)
    # BUG-REV-SPAC-1型の誤XBRLタグ検出。
    # 隣接年Revenue比が10倍超かつベース年 > $1M (スタートアップ微少値を除外) の場合はNG。
    # IONQ 2022: Revenuesタグが$1,235M(SPAC調達)を誤タグ → 正常年$11M との比 112倍
    sec_ticker_dir = os.path.join(SEC_DATA_DIR, ticker)
    if os.path.isdir(sec_ticker_dir):
        _annual_revs: dict[int, float] = {}
        for _fn in sorted(os.listdir(sec_ticker_dir)):
            if _fn.startswith("annual_") and _fn.endswith(".json") and _fn[7:11].isdigit():
                _yr = int(_fn[7:11])
                try:
                    with open(os.path.join(sec_ticker_dir, _fn), encoding="utf-8") as _f:
                        _d = json.load(_f)
                    _r = _d.get("pl", {}).get("revenue")
                    if _r is not None:
                        _annual_revs[_yr] = _r
                except Exception:
                    pass
        _yrs = sorted(_annual_revs.keys())
        for _i, _yr in enumerate(_yrs):
            _r = _annual_revs[_yr]
            if _r <= 1_000_000:
                continue  # スタートアップ微少値はスキップ
            # 孤立年チェック: 前後両年が存在し、どちらも当該年の5%未満 → 誤XBRLタグ疑い
            # (IONQ 2022: 前=$2.1M/後=$22M vs $1,235M → どちらも1.8%以下 → 異常)
            # (ASTS 2025: 後年データなし → 正常な高成長トレンドとして除外)
            _prev = _annual_revs.get(_yrs[_i - 1]) if _i > 0 else None
            _next = _annual_revs.get(_yrs[_i + 1]) if _i < len(_yrs) - 1 else None
            if _prev is None or _next is None:
                continue  # 両端年はスキップ（孤立か判定不能）
            if _prev <= 0 or _next <= 0:
                continue
            _threshold = _r * 0.05  # 前後が当該年の5%未満なら異常
            if _prev < _threshold and _next < _threshold:
                _ratio_prev = _r / _prev
                _ratio_next = _r / _next
                ng.append(
                    f"  [NG-11 Revenue孤立年] {_yr}=${_r/1e6:.1f}M"
                    f" (前年{_yrs[_i-1]}=${_prev/1e6:.1f}M: {_ratio_prev:.0f}x,"
                    f" 翌年{_yrs[_i+1]}=${_next/1e6:.1f}M: {_ratio_next:.0f}x)"
                    f" → XBRLタグ誤り疑い(TICKER_RESTRICTIONSで修正)"
                )

    # CHECK-21: Revenue段差型急変（QUALITY-GATES-EPIC-1 Phase 2b-2）
    # common.screening.dcf_validity_checker::check_c_data_jump()を統合。
    #
    # NG-11との役割分担（重複ではなく併存）:
    #   NG-11（孤立年検知）は「前後両年とも当該年の5%未満」の**スパイク型**
    #   （その年だけ突出し、前後は元の水準に戻る）のみを検知する。
    #   WARN-21（本チェック、段差型検知）は前後判定を要さず、隣接年比が
    #   2.0倍以上/0.5倍以下であれば検知するため、**ジャンプ後も高い水準が
    #   継続するステップ型**（FICO/CPRT/LITE型、SEC-TAG-FICO-CPRT-1参照）も
    #   捕捉できる。NG-11はこのステップ型を構造的に検知できなかった
    #   （次年が「当該年の5%未満」に該当せず孤立年条件が成立しないため）。
    #
    # 重要度はNGではなくWARNとする（2026-07-12・実装検証時の判断変更）:
    # 全105銘柄で試験実行した結果、19銘柄が新規に該当したが、うち複数
    # （NVDA: AI GPU需要による実際の売上急成長$26.9B→$130.5B、JOBY: プレ
    # コマーシャル航空機企業のほぼゼロからの売上立ち上がり等）は一次情報
    # （annual_YYYY.json）で確認した結果、タグ取得ミスではなく実際の事業
    # 成長・売上立ち上がりだった。dcf_validity_checker.pyの2.0倍/0.5倍閾値は
    # 元々「人間が目視で選別する前提のフラグ付けツール」として設計されており、
    # NG（ブロッキング）にするには誤検知率が高すぎると判断しWARNへ変更した。
    c_flag, c_jumps, _c_revs = check_c_data_jump(REPO_ROOT, ticker)
    if c_flag:
        for _jump in c_jumps:
            warn.append(
                f"  [WARN-21 Revenue段差型急変] {_jump}"
                f" → XBRLタグ誤り、または実際の急成長/急減の可能性（要確認）"
            )

    # CHECK-44・CHECK-45: 売上総利益・CapExの段差型急変
    # （[[DATA-JUMP-CHECK-GENERALIZE-1]]、2026-09-06）
    #
    # check_c_data_jump()をsection/fieldパラメータ化し、CHECK-21（Revenue）と
    # 同じ「隣接年比の段差型検知」を売上総利益（pl.gross_profit）・
    # CapEx（cf.capital_expenditure）にも展開した。純利益・SBCは対象外
    # （[[DATA-JUMP-CHECK-NETINCOME-SBC-1]]として別途切り出し。純利益は
    # 符号反転により比率が発散、SBCはゼロ近傍からの急増（最大2468倍）が
    # 頻発し、比率方式そのものが本質的に機能しないと実データで確認済み
    # のため）。
    #
    # 閾値は実測分布（tanuki=true全100銘柄・直近6年）を基準に、Revenue
    # と同様「NGにするには誤検知率が高すぎる」ため一貫してWARN止まりと
    # した：
    #   売上総利益: 上振れ5.0倍/下振れ0.2倍
    #   CapEx: 上振れ8.0倍/下振れ0.15倍（上振れの逆数0.125とは異なる、
    #     非対称な閾値。down_ratio引数で個別指定）
    # いずれも小規模銘柄（ASTS/RCAT/ONDS/KULR/BBAI等）でゼロ近傍から
    # 有意な水準への立ち上がり・巡航期入り前の設備投資急増が主な発火要因
    # であり、事業実態の反映であってXBRLタグ誤りとは限らない点はWARN-21
    # と同様（要確認レベルの可視化が目的、自動修正なし）。
    gp_flag, gp_jumps, _gp_vals = check_c_data_jump(
        REPO_ROOT, ticker, section="pl", field="gross_profit",
        jump_ratio=5.0, down_ratio=0.2,
    )
    if gp_flag:
        for _jump in gp_jumps:
            warn.append(
                f"  [WARN-44 売上総利益段差型急変] {_jump}"
                f" → XBRLタグ誤り、または実際の急成長/急減の可能性（要確認）"
            )

    capex_flag, capex_jumps, _capex_vals = check_c_data_jump(
        REPO_ROOT, ticker, section="cf", field="capital_expenditure",
        jump_ratio=8.0, down_ratio=0.15,
    )
    if capex_flag:
        for _jump in capex_jumps:
            warn.append(
                f"  [WARN-45 CapEx段差型急変] {_jump}"
                f" → XBRLタグ誤り、または実際の設備投資急増/急減の可能性（要確認）"
            )

    # CHECK-12: Cash-ST_Invest 期整合チェック（BUG-NETDEBT-5回帰検知）
    # Cashが最新四半期値に更新されているのにST_Investが年次のままなら期ズレ
    _ann_files_c12 = sorted(glob.glob(os.path.join(SEC_DATA_DIR, ticker, "annual_*.json")))
    _q_files_c12   = sorted(glob.glob(os.path.join(SEC_DATA_DIR, ticker, "quarterly_*.json")))
    if _ann_files_c12 and _q_files_c12:
        try:
            with open(_ann_files_c12[-1], encoding="utf-8") as _f12:
                _ann12 = json.load(_f12)
            with open(_q_files_c12[-1], encoding="utf-8") as _f12q:
                _q12   = json.load(_f12q)
            _ann_period12 = _ann12.get("period", "")
            _q_period12   = _q12.get("period", "")
            if _ann_period12 != _q_period12:
                _ann_bs12  = _ann12.get("bs", {})
                _q_bs12    = _q12.get("bs", {})
                _ann_cash12 = _ann_bs12.get("cash_and_equivalents") or 0
                _q_cash12   = _q_bs12.get("cash_and_equivalents") or 0
                _ann_sti12  = _ann_bs12.get("short_term_investments") or 0
                _q_sti12    = _q_bs12.get("short_term_investments") or 0
                # Cashが四半期値に更新済み かつ ST_Investが存在し値が変化する場合のみチェック
                if _q_cash12 != _ann_cash12 and _q_sti12 > 0 and _q_sti12 != _ann_sti12:
                    _fh12     = latest.get("financial_health", {})
                    _rep_cash = _fh12.get("cash_and_equivalents") or 0
                    _rep_sti  = _fh12.get("short_term_investments") or 0
                    # レポートCash≈四半期値 かつ レポートSTI≈年次値 かつ STI≠四半期値 → 期ズレ未修正
                    # （quarterly STI ≈ annual STI の偽陽性を除外: PLTR/QBTS など）
                    _cash_ok = abs(_rep_cash - _q_cash12) < max(1_000_000, _q_cash12 * 0.01)
                    _sti_stale = abs(_rep_sti - _ann_sti12) < max(1_000_000, _ann_sti12 * 0.01)
                    _sti_already_qtr = abs(_rep_sti - _q_sti12) < max(1_000_000, _q_sti12 * 0.01)
                    if _cash_ok and _sti_stale and not _sti_already_qtr:
                        warn.append(
                            f"  [WARN-12 Cash-STI期ズレ] Cash={_rep_cash/1e6:.0f}M({_q_period12})"
                            f" だがST_Invest={_rep_sti/1e6:.0f}M(年次{_ann_period12})のまま"
                            f" → 正={_q_sti12/1e6:.0f}M"
                        )
        except Exception:
            pass

    # CHECK-13: RICE負値ラベル確認（RICE-3 回帰検知）
    # rice.available=true かつ BASE RICE < 0 なら Matrix Label が "N/A" か "OCF赤字" を含むこと
    _rice_ld = latest.get("rice", {})
    if _rice_ld.get("available", False):
        _rice_base_val = (_rice_ld.get("base") or {}).get("rice")
        if _rice_base_val is not None and _rice_base_val < 0:
            _label_c13 = parsed.get("label", "") or ""
            if "N/A" not in _label_c13 and "OCF赤字" not in _label_c13:
                ng.append(
                    f"  [NG-13 RICE負値ラベルなし] BASE RICE={_rice_base_val:.3f} "
                    f"だが Label='{_label_c13}' に 'N/A (OCF赤字)' なし"
                )

    # CHECK-14/15: EPS異常値チェック（単位バグ・大型一時利益検出）
    # EPS Analyzer quarterly.json の直近Q adj_eps / gaap_eps を株価と比較する
    _price_c14 = None
    for _pline in parsed.get("_raw_lines", []):
        _pm = re.match(r'^Price:\s*\$([0-9,.]+)', _pline.strip())
        if _pm:
            try:
                _price_c14 = float(_pm.group(1).replace(",", ""))
            except Exception:
                pass
            break

    if _price_c14 and _price_c14 > 0:
        _eps_qs = _read_eps_quarterly(ticker)
        if _eps_qs:
            _latest_q = sorted(_eps_qs, key=lambda x: x.get("filing_date", ""))[-1]
            _latest_adj = abs(_latest_q.get("adjusted_eps", 0) or 0)
            _latest_gaap = abs(_latest_q.get("gaap_eps", 0) or 0)
            _max_eps = max(_latest_adj, _latest_gaap)
            if _max_eps > _price_c14:
                ng.append(
                    f"  [NG-15 EPS>株価] 直近Q adj_eps={_latest_adj:.2f} gaap_eps={_latest_gaap:.2f}"
                    f" > Price=${_price_c14:.2f}"
                    f" (filing:{_latest_q.get('filing_date','?')})"
                )
            elif _max_eps > _price_c14 * 0.5:
                ng.append(
                    f"  [NG-14 EPS>株価50%] 直近Q adj_eps={_latest_adj:.2f} gaap_eps={_latest_gaap:.2f}"
                    f" > Price*0.5=${_price_c14 * 0.5:.2f}"
                    f" (filing:{_latest_q.get('filing_date','?')})"
                )

            # CHECK-16: TTM計算に使われる四半期数チェック（4件未満は不完全なTTM）
            _recent = sorted(
                [q for q in _eps_qs if q.get("filing_date", "") >= "2023-01-01"],
                key=lambda x: x.get("filing_date", ""),
                reverse=True
            )
            if 0 < len(_recent) < 4:
                warn.append(
                    f"  [WARN-16 TTM四半期不足] EPS Analyzer TTM計算に{len(_recent)}四半期しかない（4必要）"
                )

    # CHECK-17: EPS全値$0.0（BUG-EPS-ZERO-1 回帰検知）
    # 直近3年の四半期で全てadj_eps=gaap_eps=0.0の場合、株式数取得失敗の可能性
    _eps_qs_c17 = _read_eps_quarterly(ticker)
    _recent_c17 = [q for q in _eps_qs_c17 if (q.get("filing_date") or "") >= "2022-01-01"]
    if len(_recent_c17) >= 2:
        _all_adj_zero = all(abs(q.get("adjusted_eps") or 0) < 1e-9 for q in _recent_c17)
        _all_gaap_zero = all(abs(q.get("gaap_eps") or 0) < 1e-9 for q in _recent_c17)
        if _all_adj_zero and _all_gaap_zero:
            ng.append(
                f"  [NG-17 EPS全値$0.0] 直近{len(_recent_c17)}四半期すべてadj_eps=gaap_eps=0.0"
                f" → 株式数取得失敗疑い(BUG-EPS-ZERO-1 回帰)"
            )

    # CHECK-18: G=15%デフォルト未調整（DCF-DEFAULT-G-1 回帰検知）
    # recommended_gがあるのにphase1_growth_auto_adjusted=Falseかつ成長率が15%のままならWARN
    _g_c18 = latest.get("growth") or {}
    _rate_c18 = _g_c18.get("rate")
    _source_c18 = _g_c18.get("source", "")
    _rec_g_c18 = latest.get("recommended_g")
    _auto_adj_c18 = latest.get("phase1_growth_auto_adjusted", False)
    if (
        _rate_c18 is not None
        and _rec_g_c18 is not None
        and not _auto_adj_c18
        and _source_c18 != "segment_weighted"  # segment_configによる意図的設定は除外
        and abs(_rate_c18 - 0.15) < 0.002      # 15%デフォルトのまま
        and abs(_rate_c18 - _rec_g_c18) > 0.05 # recommended_gと5%以上乖離
    ):
        warn.append(
            f"  [WARN-18 G=15%デフォルト未調整] growth.rate={_rate_c18:.1%}"
            f" & recommended_g={_rec_g_c18:.1%} だがauto_adjusted=False"
            f" → DCF-DEFAULT-G-1 回帰の可能性"
        )

    # CHECK-19: SEC株数=0（BUG-EPS-ZERO-1 回帰検知）
    # 直近3年の四半期でdiluted_shares=0かつnet_income非ゼロの場合はNG
    _eps_qs_c19 = _read_eps_quarterly(ticker)
    _recent_c19 = [q for q in _eps_qs_c19 if (q.get("filing_date") or "") >= "2022-01-01"]
    _zero_shares_c19 = [
        q for q in _recent_c19
        if (q.get("gaap_net_income") or 0) != 0 and (q.get("diluted_shares") or 0) == 0
    ]
    if _zero_shares_c19:
        _dates_c19 = [q.get("filing_date", "?") for q in _zero_shares_c19[:3]]
        ng.append(
            f"  [NG-19 SEC株数=0] {len(_zero_shares_c19)}四半期でdiluted_shares=0"
            f" (例: {', '.join(_dates_c19)})"
            f" → 株式数取得失敗(BUG-EPS-ZERO-1 回帰)"
        )

    # CHECK-20: fcf_cagr floor値張り付き（GROWTH-FLOOR-VERDICT-1）
    # growth.source=fcf_cagrのままgrowth.rateがgrowth_floor(15%)に完全一致している場合に検知。
    # CHECK-18はrecommended_gがNoneの場合に構造的に発火できないため、
    # recommended_gの有無を問わずgrowth_source/rateのみで判定するのが本チェックの目的
    # （MO/LOAR/XOM等、recommended_g算出不可でfloorに落ちるケースを補完的に捕捉する）
    _g_c20 = latest.get("growth") or {}
    _rate_c20 = _g_c20.get("rate")
    _source_c20 = _g_c20.get("source", "")
    if (
        _source_c20 == "fcf_cagr"
        and _rate_c20 is not None
        and abs(_rate_c20 - 0.15) < 0.002
    ):
        warn.append(
            f"  [WARN-20 fcf_cagr floor張り付き] growth.rate={_rate_c20:.1%}"
            f" (source=fcf_cagr) がgrowth_floor(15%)に完全一致"
            f" → 実績と無関係な下駄履き値の可能性(GROWTH-FLOOR-VERDICT-1)"
        )

    # CHECK-22: fyキー競合（FY52WEEK-BUCKET-MISPLACE-1根本修正で新設）
    # parser.pyが本人データ同士のfyタグ衝突を検知した場合に記録するログを監視する。
    # tie-breakで自動解決済みのため非ブロッキングWARNとする。CRM/FCX/CAKE/HON/COHR/
    # AVAV/FICO/NVDAで実在確認済み。新規銘柄で発生した場合は本チェックで検知される。
    _collisions_c22 = _read_fy_collision_log(ticker)
    if _collisions_c22:
        _fields_c22 = sorted({c.get("field", "?") for c in _collisions_c22})
        warn.append(
            f"  [WARN-22 fyキー競合] 本人データ同士で{len(_collisions_c22)}件"
            f" (対象フィールド: {', '.join(_fields_c22[:5])}{'...' if len(_fields_c22) > 5 else ''})"
            f" → tie-breakで自動解決済み。filing代行者側のタグ付け起因と推測"
            f"（原因追及は対象外）"
        )

    # CHECK-23: fyタグ裏取り不一致（ARCH-DATA-1ステージ3で新設）
    # parser.pyが年度バケツキーと採用エントリの生fyタグの食い違いを検知した場合に
    # 記録するログを監視する。CHECK-22（同一fyタグへの複数本人end_date競合）とは
    # 独立した別軸のチェック。fy_tag_mismatch_log.json自体がis_own_data=True
    # （本人データ自身のfyタグが実際に採用されてしまっているケース）のみを対象に
    # 絞り込み済み（is_own_data=Falseの比較年度再掲エントリは、fyタグが「その数値が
    # どの10-Kに載っていたか」というfiling側の属性でしかなく企業の申告ミスとは
    # 無関係な正常仕様のため、2026-07-17に検知対象から除外した。全105銘柄検証で
    # 除外前は4,434件・105銘柄というノイズになっていた）。自動修正は行わない。
    _mismatches_c23 = _read_fy_tag_mismatch_log(ticker)
    if _mismatches_c23:
        _fields_c23 = sorted({m.get("field", "?") for m in _mismatches_c23})
        warn.append(
            f"  [WARN-23 fyタグ裏取り不一致] {len(_mismatches_c23)}件"
            f" (対象フィールド: {', '.join(_fields_c23[:5])}{'...' if len(_fields_c23) > 5 else ''})"
            f" → 本人データ自身のfyタグが年度バケツと食い違う（裏取り検知、自動修正なし）"
        )

    # CHECK-24: 決算期変更境界の年度バケツ競合（FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1）
    # parser.pyが「本人データ」と生fyタグ・end_dateの両方が異なる別エントリが
    # 同一年度バケツ（computed_year）で競合したケースを検知した場合に記録する
    # ログを監視する。CHECK-22（同一fyタグ前提）・CHECK-23（勝者自身のfyタグと
    # バケツの不一致、敗者側は対象外）のいずれとも異なる軸で、「fyタグが元々
    # 異なる2エントリが同一バケツで競合する」ケース（RCAT型、決算期変更の
    # 境界年）を対象とする。現状は_own_override_is_safe()の汎用accnベース判定
    # の副次効果で正しい値が採用されているため実害はなく、将来の実装変更等で
    # 崩れうる潜在リスクの予防的可視化が目的。自動修正は行わない。
    _collisions_c24 = _read_fye_boundary_collision_log(ticker)
    if _collisions_c24:
        _fields_c24 = sorted({c.get("field", "?") for c in _collisions_c24})
        warn.append(
            f"  [WARN-24 決算期変更境界バケツ競合] {len(_collisions_c24)}件"
            f" (対象フィールド: {', '.join(_fields_c24[:5])}{'...' if len(_fields_c24) > 5 else ''})"
            f" → 決算期変更の境界年で生fyタグ・end_dateが異なる2エントリが同一"
            f"年度バケツで競合（現状は本人データ側が正しく採用済み、自動修正なし）"
        )

    # CHECK-25: BS項目None検知（FY52WEEK-BS-NULL-SILENT-1 Phase A）
    # total_assets/total_liabilities/stockholders_equity/current_assets/
    # current_liabilities/cash_and_equivalentsは全105銘柄実測でNone率が
    # ほぼ0-4%（ほぼ確実にデータ異常のシグナル）。従来はreader.py::
    # get_net_cash()等の計算経路で`or 0`により静かに$0化され検知不能
    # だった。最新年度annual_YYYY.jsonを直接参照し、対象フィールドの
    # 欠損を明示的に検知する（report.txt/latest.jsonの生成有無に依存
    # しない独立チェック）。short_term_investments/long_term_debt/
    # short_term_debt（真のゼロとの判別困難）・rpo（非SaaS銘柄はNoneが
    # 正常）はPhase B/Cとして対象外。
    _bs_c25, _period_c25 = _read_latest_annual_bs(ticker)
    if _bs_c25:
        _none_fields_c25 = [f for f in _BS_NULL_CHECK_FIELDS if _bs_c25.get(f) is None]
        if _none_fields_c25:
            warn.append(
                f"  [WARN-25 BS項目None] FY{_period_c25}: {', '.join(_none_fields_c25)}"
                f" が欠損 → 計算経路でNoneが暗黙に0化されている可能性"
                f"（FY52WEEK-BS-NULL-SILENT-1 Phase A、要確認）"
            )

    # CHECK-26: BS項目「前年値あり→当年None」遷移検知
    # （BS-FIELD-NONE-TRANSITION-DETECT-1）
    # WARN-25（ブランケット型、total_assets等6フィールド対象）とは独立した
    # 別軸のチェック。short_term_investments/long_term_debt/short_term_debt/
    # rpo（WARN-25がNone率過多〈35〜65%〉を理由に対象外とした4フィールド）は
    # 「Noneであること自体」の検知には向かないが、「前年は値があったのに
    # 当年からNoneになる」という**遷移**は正常な企業では発生しないため、
    # WARN-25とは別のブランケット型不採用理由（ノイズの多さ）が当てはまらない。
    #
    # 会計年度の連続性に関する注意（FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1関連）:
    # files[-2]/files[-1]の2ファイルを機械的に「1年前・当年」とみなさず、
    # 双方のperiod（fyラベル）の年度差が厳密に1であることを確認したうえで
    # のみ判定する。決算期変更の境界年（例: RCATは2019年・2024-2025年に
    # 決算期を変更済み、FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1参照）では
    # periodラベルが連続していても、files[-2]が真の「1年前」の期間を
    # 表さない場合があり得る（スタブ期間の混入等）。年度差が1でない場合は
    # 判定不能として発火させない（誤判定より見逃しを優先する設計）。
    # 新規登録銘柄（annual_*.jsonが1年分のみ）も同様に対象外。
    #
    # 事前調査（NVDA-STI-TAG-UNIDENTIFIED-1調査時の体制確認、BS-FIELD-NONE-
    # TRANSITION-DETECT-1）で、FY52WEEK-BS-NULL-SILENT-1「生涯フェードアウト」
    # 25件のうち8件（APP/BKNG/CPRT/DOCN/ENTG/KULR/MSCI/SOUN）が実装直後の
    # 直近2年度比較で発火することが判明済み。いずれも一次情報（10-K原本）で
    # 真の無借金/無投資継続と確認済みのため、warn_acknowledged.jsonへ事前
    # 登録し初回実行時のアラート疲れを回避する。
    _TRANSITION_CHECK_FIELDS = ["short_term_investments", "long_term_debt", "short_term_debt", "rpo"]
    _ann_files_c26 = sorted(glob.glob(os.path.join(SEC_DATA_DIR, ticker, "annual_*.json")))
    if len(_ann_files_c26) >= 2:
        try:
            with open(_ann_files_c26[-1], encoding="utf-8") as _f26_latest:
                _ann_latest26 = json.load(_f26_latest)
            with open(_ann_files_c26[-2], encoding="utf-8") as _f26_prior:
                _ann_prior26 = json.load(_f26_prior)
            _latest_period26 = _ann_latest26.get("period")
            _prior_period26 = _ann_prior26.get("period")
            try:
                _year_diff26 = int(_latest_period26) - int(_prior_period26)
            except (TypeError, ValueError):
                _year_diff26 = None
            if _year_diff26 == 1:
                _latest_bs26 = _ann_latest26.get("bs", {}) or {}
                _prior_bs26 = _ann_prior26.get("bs", {}) or {}
                _transitioned26 = [
                    f for f in _TRANSITION_CHECK_FIELDS
                    if _prior_bs26.get(f) is not None and _latest_bs26.get(f) is None
                ]
                if _transitioned26:
                    warn.append(
                        f"  [WARN-26 BS項目遷移(有値→None)] FY{_prior_period26}→FY{_latest_period26}: "
                        f"{', '.join(_transitioned26)} が前年値あり→当年Noneに遷移"
                        f"（タグ申告停止の可能性。生涯フェードアウト〈真のゼロ継続〉の"
                        f"場合はwarn_acknowledged.jsonへ登録すること）"
                    )
        except Exception:
            pass

    # CHECK-27: cross_filing_tags近似値の残差率閾値超過検知
    # （NVDA-STI-TAG-UNIDENTIFIED-1・ANOMALY-PATTERN-CATALOG-1型C対応）
    # parser.py::_apply_cross_filing_tags()が付与するbs_provenance[field].
    # is_approximated=Trueのエントリを対象に、residual_pctが閾値（5%）を
    # 超える場合のみ発火する。NVDA（+0.88%）等の既知の合算近似値は許容範囲内
    # のため通常は発火しない。将来同型（型C）を別銘柄に適用した際、想定外に
    # 大きな乖離が生じていないかの安全網。
    _RESIDUAL_PCT_THRESHOLD = 0.05
    _ann_files_c27 = sorted(glob.glob(os.path.join(SEC_DATA_DIR, ticker, "annual_*.json")))
    if _ann_files_c27:
        try:
            with open(_ann_files_c27[-1], encoding="utf-8") as _f27:
                _ann27 = json.load(_f27)
            _prov27 = _ann27.get("bs_provenance", {}) or {}
            _period27 = _ann27.get("period", "")
            for _field27, _fp27 in _prov27.items():
                if not isinstance(_fp27, dict) or not _fp27.get("is_approximated"):
                    continue
                _residual27 = _fp27.get("residual_pct")
                if _residual27 is not None and abs(_residual27) > _RESIDUAL_PCT_THRESHOLD:
                    warn.append(
                        f"  [WARN-27 近似値残差過大] FY{_period27} {_field27}: "
                        f"cross_filing_tags合算値の残差{_residual27*100:+.1f}%が"
                        f"閾値({_RESIDUAL_PCT_THRESHOLD*100:.0f}%)を超過 → 合算元タグ"
                        f"（{', '.join(_fp27.get('combined_tags', []))}）の妥当性を要確認"
                    )
        except Exception:
            pass

    # CHECK-28: 10-KT/10-QT除外検知（[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]）
    # fetcher.py::_fetch_submissions_for_cik()のrelevant_formsに10-KT・
    # 10-QT（決算期変更移行期報告書）が含まれておらず、該当formのaccnが
    # accn_to_reportdateに登録されないため、is_own_data判定が恒常的に
    # Falseになり本人データが年次バケツ争いで採用されない構造的欠落を
    # 直接検知する。WARN-24〈決算期変更境界バケツ競合〉はこの欠落が
    # 引き起こす症状〈バケツ競合〉を検知するのに対し、本WARNは根本原因
    # 〈10-KT/10-QT自体の除外〉を直接検知する別軸。自動修正なし、検知のみ。
    _cf_path_c28 = os.path.join(SEC_DATA_DIR, ticker, "company_facts.json")
    if os.path.exists(_cf_path_c28):
        try:
            with open(_cf_path_c28, encoding="utf-8") as _f28:
                _cf28 = json.load(_f28)
            _facts28 = _cf28.get("facts", {}).get("us-gaap", {})
            _transition_forms28 = {"10-KT", "10-QT"}
            _transition_accns28: dict = {}
            for _tagdata28 in _facts28.values():
                for _entries28 in _tagdata28.get("units", {}).values():
                    for _e28 in _entries28:
                        _accn28 = _e28.get("accn")
                        _form28 = _e28.get("form")
                        if _accn28 and _form28 in _transition_forms28:
                            _transition_accns28.setdefault(_accn28, (_form28, _e28.get("end")))
            if _transition_accns28:
                _accn_reportdate28 = load_submissions(ticker, data_dir=SEC_DATA_DIR)
                for _accn28, (_form28, _end28) in sorted(_transition_accns28.items()):
                    if _accn28 not in _accn_reportdate28:
                        warn.append(
                            f"  [WARN-28 10-KT/10-QT除外] accn={_accn28} form={_form28} "
                            f"end={_end28} → accn_to_reportdateに未登録のため本人データ"
                            f"判定の対象外（fetcher.py relevant_forms除外、"
                            f"[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]、自動修正なし）"
                        )
        except Exception:
            pass

    # CHECK-29: 会計恒等式Total_Assets=Total_Liabilities+Stockholders_
    # Equity（+NCI+一時的持分）検知（[[CHECK29-ACCOUNTING-IDENTITY-
    # DETECTION-LAYER-1]]）。parser.pyが①本体一致・②NCI/一時的持分を加算
    # した拡張形一致（許可リスト方式、OR条件フォールバック）のいずれでも
    # 解消しなかったケースを検知した際に書き出すログを監視する。①で解消
    # したケース・②拡張形で解消したケースはparser.py側で記録対象外済み
    # （ノイズ削減）のため、ここで発火するのはいずれでも未解消のケースのみ。
    # 自動修正は行わない。
    _violations_c29 = _read_bs_identity_violations_log(ticker)
    _unresolved_c29 = [v for v in _violations_c29 if not v.get("resolved_by_extension")]
    if _unresolved_c29:
        _periods_c29 = sorted({str(v.get("period", "?")) for v in _unresolved_c29})
        warn.append(
            f"  [WARN-29 会計恒等式不成立] {len(_unresolved_c29)}件"
            f" (対象年度: {', '.join(_periods_c29[:5])}{'...' if len(_periods_c29) > 5 else ''})"
            f" → Total_Assets=Total_Liabilities+Stockholders_Equity"
            f"（NCI・一時的持分の許可リスト加算を含む拡張形でも）が成立しない"
            f"（自動修正なし、[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]参照）"
        )

    # CHECK-46: revenue − cost_of_revenue = gross_profit の算術的整合性
    # （[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]）。
    #
    # [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]で発見・是正した9銘柄
    # 15年度分（AMD/BSY/CRM/JNJ/KO/LRCX/MRVL/ONDS/RMBS）は、いずれも
    # revenue/cost_of_revenue/gross_profitが独立にaccn・期間を選定する
    # ため異なる会計年度のデータが混在する構造的欠陥であり、既存の常設
    # 監査では一件も検知できず個別調査でのみ発覚していた。CHAT_RULES.md
    # 「探索的スキャンツールと常設WARN条件の分離」の原則に従い、その
    # 探索手法（accn/期間の突き合わせ）はそのまま転用せず、常設WARN
    # としては3フィールドの算術的整合性を許容誤差込みで検証するだけの
    # 軽量な設計とする（自動修正なし、検知のみ）。
    #
    # 許容誤差0.1%は実データ校正（tanuki=true全105銘柄・annual_YYYY.json
    # 全年度、1034件のrevenue/cost_of_revenue/gross_profit三つ組を実測）
    # により決定した:
    #   - 1000件（96.7%）は完全一致（diff=0）
    #   - 非一致34件中、最小はCRM(FY2017)のdiff=$39,000（rev比0.0005%、
    #     丸め誤差の範囲内）で、次に小さいCRM(FY2018)のdiff=$60,510,000
    #     （rev比0.5741%）との間に約1000倍のギャップがある
    #   - 0.5741%以上の残り33件は、[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-
    #     GAP-MO-PM-SCCO-1]]で個別確認済みの物品税定義差（MO 13.5-24.8%・
    #     PM 63.2-64.4%）・D&A別建て業界慣行（SCCO 4.2-12.0%）という
    #     genuine定義差、または[[LITE-COGS-DA-TAG-UNMERGED-1]]の未解消
    #     タグ分離バグ（LITE 0.75-6.17%）のいずれかで、既に個別調査済み
    #     か調査対象として登録済みである
    # 0.0005%と0.5741%の間（約1000倍のギャップ）に閾値を置けば、実データ
    # 上は丸め誤差とそれ以外を完全に分離できるため、0.1%（0.001）を
    # 採用した。genuine定義差（MO/PM/SCCO）も含めて発火する設計とした
    # （WARNは「バグ確定」ではなく「要確認」のシグナルであり、
    # warn_acknowledged.jsonで確認済み銘柄として登録することで、既存の
    # WARN-21等と同じ運用に合流させる）。
    _GP_COGS_TOLERANCE_PCT = 0.001
    _ann_files_c46 = sorted(glob.glob(os.path.join(SEC_DATA_DIR, ticker, "annual_*.json")))
    _mismatches_c46: list[tuple[str, float]] = []
    for _path_c46 in _ann_files_c46:
        try:
            with open(_path_c46, encoding="utf-8") as _f46:
                _ann46 = json.load(_f46)
        except Exception:
            continue
        _pl46 = _ann46.get("pl", {}) or {}
        _rev46 = _pl46.get("revenue")
        _cogs46 = _pl46.get("cost_of_revenue")
        _gp46 = _pl46.get("gross_profit")
        if _rev46 is None or _cogs46 is None or _gp46 is None:
            continue
        # gross_profitがrevenue-cost_of_revenueの逆算値（derived）の場合、
        # 定義上diff=0で恒等的に成立するため比較対象から除外する
        # （[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]と同じ整理）
        _gp_prov46 = (_ann46.get("pl_provenance", {}) or {}).get("gross_profit", {}) or {}
        if _gp_prov46.get("derived"):
            continue
        _diff46 = (_rev46 - _cogs46) - _gp46
        if _diff46 == 0:
            continue
        _denom46 = abs(_rev46) if _rev46 else 0
        if _denom46 == 0:
            continue
        _rel46 = abs(_diff46) / _denom46
        if _rel46 > _GP_COGS_TOLERANCE_PCT:
            _period46 = _ann46.get("period", "?")
            _mismatches_c46.append((str(_period46), _rel46))
    if _mismatches_c46:
        _mismatches_c46.sort()
        _periods_str_c46 = ", ".join(
            f"{p}({r*100:.1f}%)" for p, r in _mismatches_c46[:5]
        )
        _more_c46 = "..." if len(_mismatches_c46) > 5 else ""
        warn.append(
            f"  [WARN-46 GP-COGS不整合] {len(_mismatches_c46)}件"
            f" (対象年度: {_periods_str_c46}{_more_c46})"
            f" → revenue−cost_of_revenue≠gross_profit（許容誤差{_GP_COGS_TOLERANCE_PCT*100:.1f}%超）"
            f"。accn/期間の取り違え等のタグ選定バグ、または業界特有の"
            f"non-GAAP調整（物品税・D&A別建て等）のgenuine定義差の可能性"
            f"（自動修正なし、[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]参照）"
        )

    return ng, warn


# ─── CLI ─────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="TANUKI VALUATION report.txt 整合性チェック"
    )
    parser.add_argument(
        "--fail-on-ng",
        action="store_true",
        help="NG件数 > 0 のとき sys.exit(1) で終了する（省略時は常にexit(0)）",
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="チェック対象銘柄（カンマ区切り可。例: NVDA または NVDA,AAPL）",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="PASS行を表示しない（NGとWARNのみ出力）",
    )
    parser.add_argument(
        "--include-yfinance-checks",
        action="store_true",
        help=(
            "CHECK-35/41のyfinance突合部分（operating_income/revenue/"
            "net_income）を実行する（省略時デフォルトFalse＝yfinance呼び出し"
            "をスキップ）。SECデータは週1回（SEC_Data_Update.yml）しか"
            "更新されないため、このフラグはSEC_Data_Update.yml側でのみ"
            "指定する想定。他のワークフロー（TANUKI_VALUATION_Update.yml等）"
            "の日次実行では指定しない（2026-09-03新設、"
            "[[QUALITY-GATES-EPIC-1]]）"
        ),
    )
    return parser.parse_args()


# ─── メイン ──────────────────────────────────────────────────

def run_checks(args=None) -> tuple[int, int]:
    """整合性チェックを実行し (ng_count, warn_count) を返す。"""
    whitelist = _load_rpo_whitelist()
    warn_ledger = load_warn_ledger()
    quiet = getattr(args, "quiet", False)
    include_yfinance = getattr(args, "include_yfinance_checks", False)
    ticker_filter = None
    if args and args.ticker:
        ticker_filter = {t.strip().upper() for t in args.ticker.split(",")}

    # FLAG-CONSUMER-AUDIT-2: 以前はos.listdir(DATA_DIR)でtanukiフラグを見ず
    # ディレクトリ実在＋report.txt存在だけでスキャン対象を決めており、
    # tanuki=false化済みだがreport.txtが残存する銘柄（ZS・RKLB等）が
    # スキャン対象に混入していた（ZS-TICKERS-LEAK-1参照）。
    # tickers.get_tanuki_tickers()との積集合に限定する。
    all_tickers = sorted([
        t for t in _tickers_mod.get_tanuki_tickers()
        if os.path.exists(os.path.join(DATA_DIR, t, "report.txt"))
    ])

    if ticker_filter:
        tickers = [t for t in all_tickers if t in ticker_filter]
    else:
        tickers = all_tickers

    if not quiet:
        print(f"=== TANUKI VALUATION report.txt 整合性チェック ({len(tickers)} 銘柄) ===\n")

    total_ng        = 0
    total_warn      = 0
    total_warn_new  = 0
    flagged: list[tuple[str, list, list]] = []

    for ticker in tickers:
        ng, warn = check_ticker(ticker, whitelist, include_yfinance)
        annotated_warn = []
        for w in warn:
            msg, is_new = annotate_warn(ticker, w, warn_ledger)
            annotated_warn.append(msg)
            if is_new:
                total_warn_new += 1
        if ng or annotated_warn:
            flagged.append((ticker, ng, annotated_warn))
            total_ng   += len(ng)
            total_warn += len(annotated_warn)

    # CHECK-32: ティッカー非依存の単発チェック（discover_config/theme_config同期）。
    # --tickerフィルタの有無に関わらず常時実行する（ticker単位のデータとは
    # 無関係な、config/とdocs/の同期状態そのものを検証するため）。
    discover_sync_ng = _check_discover_config_sync()
    if discover_sync_ng:
        flagged.append(("[GLOBAL]", discover_sync_ng, []))
        total_ng += len(discover_sync_ng)

    # CHECK-34: ティッカー非依存の単発チェック（config/設定ファイル読み込み
    # の横断解決チェック、_CONFIG_LOADER_REGISTRY駆動）。
    # CHECK-32と同様、--tickerフィルタの有無に関わらず常時実行する。
    config_loader_ng = _check_config_loaders_resolvable()
    if config_loader_ng:
        flagged.append(("[GLOBAL]", config_loader_ng, []))
        total_ng += len(config_loader_ng)

    # CHECK-35: ティッカー非依存の単発チェック（operating_income再構成
    # 対象銘柄数の急増検知、[[OPERATING-INCOME-EXTRACTION-GAP-1]]）。
    # CHECK-32/34と同様、--tickerフィルタの有無に関わらず常時実行する
    # （all_tickers＝tanukiフラグで絞り込み済みの全銘柄を使う。--ticker
    # フィルタ適用後のtickersではなく、常に全銘柄を対象にする）。
    oi_scope_warn = _check_operating_income_reconstruction_scope(all_tickers)
    if oi_scope_warn:
        flagged.append(("[GLOBAL]", [], oi_scope_warn))
        total_warn += len(oi_scope_warn)

    # CHECK-36: ティッカー非依存の単発チェック（moat_score中立フォールバック
    # 対象銘柄数の急増検知、[[MOAT-SCORE-PARTIAL-NULL-1]]）。
    moat_scope_warn = _check_moat_score_neutral_fallback_scope(all_tickers)
    if moat_scope_warn:
        flagged.append(("[GLOBAL]", [], moat_scope_warn))
        total_warn += len(moat_scope_warn)

    # CHECK-37: ティッカー非依存の単発チェック（TAIL保有ポジションの監視
    # 漏れ検知、[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]）。tanuki銘柄の
    # 絞り込みとは無関係にTAILのポジション一覧（get_monitored_tickers()、
    # 2026-08-19②以降は全保有ポジション）を対象とするため、all_tickers
    # ではなく専用関数を呼ぶ。
    core_review_ng, core_review_info = _check_core_position_review_coverage()
    if core_review_info:
        flagged.append(("[GLOBAL]", [], core_review_info))
    if core_review_ng:
        flagged.append(("[GLOBAL]", core_review_ng, []))
        total_ng += len(core_review_ng)

    # CHECK-38: ティッカー非依存の単発チェック（TAIL登録KPIの取得失敗
    # 検知、[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-GAP-1]]）。
    # CHECK-37と同じくget_monitored_tickers()を対象とする。
    kpi_fetch_ng, kpi_fetch_warn = _check_kpi_fetch_failures()
    if kpi_fetch_warn:
        flagged.append(("[GLOBAL]", [], kpi_fetch_warn))
        total_warn += len(kpi_fetch_warn)
    if kpi_fetch_ng:
        flagged.append(("[GLOBAL]", kpi_fetch_ng, []))
        total_ng += len(kpi_fetch_ng)

    # CHECK-39: ティッカー非依存の単発チェック（登録前の実取得検証で
    # 却下されたKPIの検知、[[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]）。
    # CHECK-38のmissing_kpis（登録したが取れなかった）とは意味が
    # 異なるため別集計にする（rejected_kpis＝登録すらされなかった）。
    kpi_reject_ng, kpi_reject_warn = _check_kpi_rejected_proposals()
    if kpi_reject_warn:
        flagged.append(("[GLOBAL]", [], kpi_reject_warn))
        total_warn += len(kpi_reject_warn)
    if kpi_reject_ng:
        flagged.append(("[GLOBAL]", kpi_reject_ng, []))
        total_ng += len(kpi_reject_ng)

    # CHECK-40: ティッカー非依存の単発チェック（validator.pyが本番算出
    # 済みのDCF検証結果`validation.overall`の未接続を解消、
    # [[QUALITY-GATES-EPIC-1]]ゲート3棚卸しの追加調査）。CHECK-35/36と
    # 同様、--tickerフィルタの有無に関わらず常に全銘柄（all_tickers）を
    # 対象にする。
    dcf_validation_ng, dcf_validation_warn = _check_dcf_validation_failures(all_tickers)
    if dcf_validation_warn:
        flagged.append(("[GLOBAL]", [], dcf_validation_warn))
        total_warn += len(dcf_validation_warn)
    if dcf_validation_ng:
        flagged.append(("[GLOBAL]", dcf_validation_ng, []))
        total_ng += len(dcf_validation_ng)

    if not flagged:
        if not quiet:
            print("✅ 全銘柄整合 — NG=0 / 警告=0\n")
    else:
        for ticker, ng, warn in flagged:
            icon = "❌" if ng else "⚠️"
            print(f"{icon} {ticker}")
            for item in ng:
                print(item)
            for item in warn:
                print(item)
            print()

    if not quiet:
        print("─" * 50)
        total_warn_ack = total_warn - total_warn_new
        print(
            f"合計: NG={total_ng} 件 / 警告={total_warn} 件"
            f"（確認済み{total_warn_ack} / \U0001f195未確認{total_warn_new}）"
            f"  (対象 {len(tickers)} 銘柄)"
        )
        if total_ng == 0:
            print("✅ NG=0 全銘柄整合")
        if total_warn_new > 0:
            print(f"⚠️  未確認WARNが{total_warn_new}件あります。台帳（{WARN_LEDGER}）で確認要否を判断してください。")

    return total_ng, total_warn


if __name__ == "__main__":
    args = parse_args()
    ng_count, warn_count = run_checks(args)

    print(f"\n{'='*50}")
    print(f"結果: NG={ng_count}件 / WARN={warn_count}件")

    if args.fail_on_ng and ng_count > 0:
        print("❌ ゲート失敗: NGが存在するためexit(1)で終了します")
        sys.exit(1)
    else:
        print("✅ ゲート通過")
        sys.exit(0)
