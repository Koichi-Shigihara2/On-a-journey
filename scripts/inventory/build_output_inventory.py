"""出力の一覧（書き手・読み手・画面・定義・規則）を AST と grep で機械的に作る（[[OUTPUT-INVENTORY-CHECK-1]] 段階1、2026-10-20）。

対象（段階1）: common/sec_data/ の .py が書く出力（ファイル・Step Summary・Discord）と、ファイルの最上位のキー。
一覧は AI が書き起こさない（CHAT_RULES.md「発見の溜め方と根治の判断」5）。人・AIが足すのは下の ANNOTATIONS
（ふつうの言葉の1文）だけで、機械の一覧とは欄を分ける。

作り方:
  1. 書き手: common/sec_data/*.py の AST から書き込み（open(..., "w"/"a")・write_text・to_csv・os.replace の先・
     requests.post/urlopen）を探し、パスの式（os.path.join・f文字列・モジュールの定数・self の属性・
     返り値が1つの関数・import した定数）を解決してパスの型（{T}=銘柄 等）にする。
     common/sec_data/ の外の .py が common/sec_data/ に書く場所も同じ方法で探す（止める条件の数）
  2. 読み手（保存済みファイル）:
     - Python: AST の読み込み（open(...)〈"r"・省略〉・json.load・read_text・pd.read_json/read_csv）のパスを解決して照合
     - 全種類（.py・.js・.html・.yml）: パスの型のファイル名の部分から作った正規表現の grep（コメント・docstring を除く）
     - 経由: 上で読み手になった Python の関数を import して呼ぶ箇所（1段まで）
  3. 読み手（その場で計算）: 書き手の関数に渡す値を作る関数（`x = f(...)` → `save(…, x)` の f）を import して呼ぶ箇所
  4. 画面: docs/ の .html・.js が直接読むもの（直接）と、Python の読み手（とそれを import する本番のファイル、2段まで）が
     docs/ に書くファイルを読む docs/ のページ（経由、推定）
  5. 定義: docs/architecture/new_data_platform/FIELD_DEFINITIONS.md の表の行のうち、ファイル名の型を含む行の ID
  6. 規則: RULES の検出式（定義の書き方の型。式は人が決め、場所は grep で出す）に当たる場所。2ファイル以上なら二重
  7. 本番外のコード: NON_PRODUCTION_DIRS の .py の行数と、ワークフロー・ほかのコード・テストからの参照

使い方:
  python scripts/inventory/build_output_inventory.py                 # SEC層: docs/architecture/output_inventory/sec_layer.json と sec_layer.md を書く
  python scripts/inventory/build_output_inventory.py --layer tanuki  # TANUKI: tanuki_layer.json と tanuki_layer.md を書く
  python scripts/inventory/build_output_inventory.py --check   # 書かずに、今のファイルと同じかだけを見る（違えば終了コード1）

CHECK としてはまだ動かさない（ゲートに入れない。段階3で、消した後に入れる）。
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import posixpath
import re
import subprocess
import sys
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LAYER = "common/sec_data"
LAYER_DIRS: Tuple[str, ...] = ("common/sec_data",)
OUT_DIR = "docs/architecture/output_inventory"
OUT_JSON = f"{OUT_DIR}/sec_layer.json"
OUT_MD = f"{OUT_DIR}/sec_layer.md"
FIELD_DEFINITIONS = "docs/architecture/new_data_platform/FIELD_DEFINITIONS.md"
STOP_RATIO = 0.10
VIA_DEPTH = 3   # 読み手の関数を呼ぶ箇所をたどる段数

# 投資判断に効く読み手（パスの前方一致）。読み手にこれらが入れば「効く」とする（機械の判定、対応は人が決めた表）
DECISION_PREFIXES = [
    ("src/value/tanuki_valuation/", "理論株価・判定（TANUKI VALUATION）"),
    ("src/value/tanuki_score/", "判定（TANUKI SCORE）"),
    ("src/value/hypecore/", "HypeCore（TANUKI のtiming・判定の入力）"),
    ("src/tail/", "保有銘柄の監視（TANUKI TAIL）"),
]

# 本番外のコード（[[OUTPUT-INVENTORY-CHECK-1]]の2026-10-19追記）
NON_PRODUCTION_DIRS = ["scripts/analysis", "browser_checks", "discover/stonks-silo"]

# 規則の検出式。式（定義の書き方の型）は人が決め、当たる場所は grep で出す
RULES = [
    {
        "id": "q4_implied",
        "name": "Q4の逆算（年次−Q1〜Q3）",
        "patterns": [r"\b\w*q4\w*\s*=\s*\w*(annual|fy)\w*\s*-\s*", r"def\s+_?\w*q4_implied\w*\s*\("],
    },
    {
        "id": "latest_filed",
        "name": "同じ期の値が複数あるときの選び方（提出日が最新のもの）",
        "patterns": [r"max\([^)]*key\s*=\s*lambda[^)]*[\"']filed[\"']", r"sorted\([^)]*[\"']filed[\"'][^)]*reverse\s*=\s*True",
                     r"\[[\"']filed[\"']\]\s*>=?\s*\w+(\[[\"']filed[\"']\]|_filed)", r"def\s+select_latest_filed\s*\("],
    },
    {
        "id": "staleness",
        "name": "データの古さ・遅れの判定",
        "patterns": [r"def\s+_?\w*(stale|lag_days|freshness)\w*\s*\(", r"function\s+\w*(stale|Stale|lag|Lag|fresh|Fresh)\w*\s*\("],
        "context": r"10-Q|10-K|[\"']filed[\"']|SEC_LAG",   # SECの提出の古さに限る（ファイルにこの語があるもの）
    },
    {
        "id": "ttm_sum",
        "name": "TTM（直近4四半期の合計）の作り方",
        "patterns": [r"def\s+_?\w*(calc|calculate|compute|build)_ttm\w*\s*\(", r"rolling\(\s*4\s*\)\.sum\(", r"function\s+\w*(ttm|Ttm|TTM)\w*\s*\("],
        "context": r"sec_data|10-Q|quarterly",
    },
    {
        "id": "quarter_label",
        "name": "期の名前（四半期のラベル）の付け方",
        "patterns": [r"def\s+_?\w*quarter_label\w*\s*\(", r"function\s+\w*(quarterLabel|QuarterLabel|periodLabel)\w*\s*\("],
    },
    {
        "id": "fiscal_year",
        "name": "会計年度・会計年度末の決め方",
        "patterns": [r"def\s+_?\w*(determine_fiscal_year|detect_fiscal_\w+|get_fiscal_year_end|fiscal_year_of)\w*\s*\("],
    },
]

# FINDINGS 2026-10-15 の読み手0件の6種（F3）。場所は grep で出す
F3_NAMES = ["spac_shell_detection_log", "share_unit_fix_log", "low_base_effect", "fields_missing", "maturity_monitor", "market_data_violations_log"]

# 人・AIの注記（ふつうの言葉の1文）。機械の一覧とは別の欄。キーはパスの型
ANNOTATIONS: Dict[str, str] = {
    "common/sec_data/data/{T}/company_facts.json": "SECから取った、その銘柄の全XBRLの申告の生データ",
    "common/sec_data/data/{T}/submissions.json": "SECから取った、その銘柄の提出書類の一覧（提出日・対象期末）",
    "common/sec_data/data/{T}/submissions_legacy_{CIK}.json": "社名・CIKが変わった銘柄の、旧CIKの提出書類の一覧",
    "common/sec_data/data/{T}/company_facts_legacy_{CIK}.json": "社名・CIKが変わった銘柄の、旧CIKのXBRLの生データ",
    "common/sec_data/data/{T}/annual_{YYYY}.json": "生データから1年分ずつ取り出した年次の財務（損益・貸借・CF）",
    "common/sec_data/data/{T}/quarterly_{Q}.json": "生データから1四半期分ずつ取り出した四半期の財務",
    "common/sec_data/normalized/{T}_quarterly_normalized.json": "四半期の財務を項目ごとの時系列に並べ直し、Q4の逆算を足したもの",
    "common/sec_data/ttm/{T}_ttm_series.json": "直近4四半期の合計（TTM）の時系列",
    "common/sec_data/store_v2/{T}.json": "Layer3（項目ごとの四半期・年次の値と提出日）をファイルに保存したもの",
    "common/sec_data/data/_freshness.json": "銘柄ごとのSECデータの新しさ（REFRESHED・SEC_LAG・FAILED等）",
    "common/sec_data/data/_cik_cache.json": "銘柄とCIKの対応の控え",
    "common/sec_data/dimension_aggregate_registry.json": "セグメント等の内訳タグを合計して使う銘柄・項目の登録簿",
    "docs/common/sec_data/rotce/{T}.json": "銘柄ごとのROTCE（有形自己資本利益率）の計算結果",
    "docs/common/sec_data/rotce/_summary.json": "ROTCEの全銘柄のまとめ",
    "${RUNNER_TEMP}/sec_update_result.json": "SEC_Data_Updateの1回の実行の結果（成功・失敗の銘柄）。後の段の通知が読む",
    "common/sec_data/data/{T}/fy_collision_log.json": "同じ会計年度に2つの年次が当たった記録（解析の内部の記録）",
    "common/sec_data/data/{T}/fy_tag_mismatch_log.json": "申告の会計年度のタグと期末日が合わない記録（解析の内部の記録）",
    "common/sec_data/data/{T}/fye_boundary_collision_log.json": "会計年度末の境目で期がぶつかった記録（解析の内部の記録）",
    "common/sec_data/data/{T}/share_unit_fix_log.json": "株式数の単位（千株・株）の取り違えを直した記録",
    "common/sec_data/data/{T}/spac_shell_detection_log.json": "SPAC（上場のための空の会社）の時期の値を外した記録",
    "common/sec_data/data/{T}/bs_identity_violations_log.json": "貸借対照表の資産＝負債＋資本が合わない期の記録",
    "${GITHUB_STEP_SUMMARY}": "GitHub Actionsの実行の画面に出す要約（人が見る）",
    "Discord（DISCORD_WEB_HOOK）": "SECデータの監査の結果のDiscordへの通知",
    "config/cik_lookup.csv": "登録銘柄の一覧（CIK・状態・各システムに載せるかの印）。新規登録の検証で状態を書き換える",
}

# 段階2の判断（残す／消す）。Koichi さんの判断を写す欄で、機械の一覧とは別。書いていないものは DECISION_DEFAULT
DECISION_DEFAULT = "残す（2026-10-20）"
DECISIONS: Dict[str, str] = {
    # 段階2で消すと決めたが、変更の前の grep で読み手・言及が見つかり、チャット側が改めて判断したもの（2026-10-20）
    "common/sec_data/data/{T}/submissions_legacy_{CIK}.json":
        "残す（2026-10-20。fetcher.py の _fetch_legacy_submissions が旧CIKの取得の24時間キャッシュと、取得に失敗したときの代わりとして読む）",
    "scripts/analysis/market_pulse_redesign.py":
        "残す（2026-10-20。Market Pulse の作り直しの検討中。src/market/market_pulse/stage_conclusions.py が同じ値を使う）",
}

# 段階2で消したもの（2026-10-20）。一覧からは消え、ここに記録だけ残す
DELETED: List[Tuple[str, str]] = [
    ("common/sec_data/data/{T}/share_unit_fix_log.json", "書き手 parser.py:SECParser._save_share_unit_fix_log と呼び出し、既存15ファイル。単位を直す処理は残す"),
    ("common/sec_data/data/{T}/spac_shell_detection_log.json", "書き手 parser.py:SECParser._save_spac_shell_detection_log と呼び出し、既存98ファイル。SPACの時期の値を外す処理は残す"),
    ("common/sec_data/store_v2/{T}.json", "layer3_builder.py の save_ticker_store・build_and_save・手動のCLI・STORE_V2_DIR、.gitignore の行。その場で計算する読み手は変えない"),
    ("scripts/analysis の14ファイル", "macro_alfred_lib・macro_claims_slot_rows_repair・macro_events_known_at_repair・macro_events_revision_repair・macro_events_slot_rows_repair・"
     "macro_liquidity_m2_yoy_repair・macro_liquidity_row_date_repair・macro_liquidity_weekly_before_after・macro_m4_hollow_rally・macro_m4_m2_percentile・"
     "macro_m4_score_vs_recession・macro_m4_thresholds・macro_m5_threshold_simulation・macro_ticker_history_check（.py）"),
    ("scripts/analysis/futures_roll_suspects.py と src/market/market_pulse/backfill_futures_roll.py",
     "組で削除（2026-10-20、保留の決着）。一過性の修復の道具で、互いの参照のほかに読み手0件。付けた印（roll_suspect 等）のデータは残る"),
    ("scripts/analysis/macro_cfnai_ma3_rows_repair.py",
     "削除（2026-10-20、保留の決着）。修復は 70793c2185（2026-10-03）で実行済み。browser_checks/check_macro_pulse.py の N-07 の説明の文から名前を消した"),
]

# 規則の二重のうち既知のもの（CHECK-INV-3 で WARN。SEC層のファイルを含むものでこれ以外が二重になったら NG）
KNOWN_DUPLICATE_RULES = ["q4_implied", "latest_filed", "staleness", "ttm_sum", "fiscal_year"]

OUTSIDE_HINT = "sec_data"                    # 外の書き手を探す .py（この語を含むものだけ見る）
OUTSIDE_PREFIXES: Tuple[str, ...] = ("common/sec_data/",)   # 外の書き手として数える書き先
READ_PREFIXES: Tuple[str, ...] = ("common/sec_data",)       # 止める条件で数える読み込みの先
READ_HINT = "sec_data"                       # 決めきれない読み込みとして数えるパスの語
NAME_SHORT = "SEC層"
TITLE = "SECデータ層の出力の一覧（段階1の下書き）"
DECISION_NOTE = "段階2の判断（2026-10-20、チャット側の案をKoichiさんが了承）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す"
NON_PRODUCTION_FILES: List[str] = []         # 本番外のコードの候補（フォルダ単位でなく、ファイル単位で示すもの）
SCREEN_PAGES: Tuple[str, ...] = ()           # 画面の項目の表を作るページのフォルダ（TANUKI）
SCREEN_SECTION_DECISIONS: List[Tuple[str, str, str, str]] = []   # (ページ, 区画の始まりの行の式, 次の区画の行の式, 判断)
UNREAD_KEY_NOTES: Dict[str, str] = {}      # 誰も読まないキーを残した理由（段階2の判断）
FULL_SCAN = False      # キーを抜き取りでなく全ファイルで集める（HypeCore の段階から。TANUKI の段階2で抜き取りが見落としを生んだ）
SCREEN_SECTIONS = False    # 画面の区画（HTML の <!-- 見出し --> ごと）の一覧を作る
HTML_COPIES: List[Tuple[str, List[str]]] = []   # 本番外の HTML の複製の候補と、比べる docs/ のページ
PAGE_CHECKS: List[str] = []    # 範囲外の確認をするページ（何のページか・書き手・読み手・ナビ）
WORKFLOW_SUMMARY = False   # 段階のファイルを動かすワークフローが自分で書く Step Summary と、それが読むキーの有無
SECTION_DECISIONS: Dict[str, str] = {}   # 画面の区画の判断（ページ → 判断。そのページの区画すべてに当てる）
NONPROD_SCAN = False   # 名前を挙げた候補が無くても、段階のフォルダの .py のうちどこからも呼ばれないものを本番外のコードの候補にする（EPS の段階から）

_SEC = dict(LAYER=LAYER, LAYER_DIRS=LAYER_DIRS, OUT_JSON=OUT_JSON, OUT_MD=OUT_MD, DECISION_PREFIXES=DECISION_PREFIXES,
            NON_PRODUCTION_DIRS=NON_PRODUCTION_DIRS, RULES=RULES, ANNOTATIONS=ANNOTATIONS, DECISION_DEFAULT=DECISION_DEFAULT,
            DECISIONS=DECISIONS, DELETED=DELETED, OUTSIDE_HINT=OUTSIDE_HINT, OUTSIDE_PREFIXES=OUTSIDE_PREFIXES,
            READ_PREFIXES=READ_PREFIXES, READ_HINT=READ_HINT, NAME_SHORT=NAME_SHORT, TITLE=TITLE, DECISION_NOTE=DECISION_NOTE,
            NON_PRODUCTION_FILES=NON_PRODUCTION_FILES, SCREEN_PAGES=SCREEN_PAGES, KNOWN_DUPLICATE_RULES=KNOWN_DUPLICATE_RULES,
            SCREEN_SECTION_DECISIONS=SCREEN_SECTION_DECISIONS, UNREAD_KEY_NOTES=UNREAD_KEY_NOTES,
            FULL_SCAN=FULL_SCAN, SCREEN_SECTIONS=SCREEN_SECTIONS, HTML_COPIES=HTML_COPIES, PAGE_CHECKS=PAGE_CHECKS,
            WORKFLOW_SUMMARY=WORKFLOW_SUMMARY, SECTION_DECISIONS=SECTION_DECISIONS, NONPROD_SCAN=NONPROD_SCAN)
# ---- TANUKI VALUATION・TANUKI Score（段階1、2026-10-20）
_TANUKI = dict(_SEC)
_TANUKI.update(
    LAYER="src/value/tanuki_valuation・src/value/tanuki_score",
    LAYER_DIRS=("src/value/tanuki_valuation", "src/value/tanuki_score"),
    OUT_JSON=f"{OUT_DIR}/tanuki_layer.json", OUT_MD=f"{OUT_DIR}/tanuki_layer.md",
    # TANUKI の段階では、自分のフォルダ（src/value/tanuki_valuation/）の読み手は「効く」に数えない（通知の控え等も効くになるため）。
    # 画面と、ほかのシステム（TANUKI SCORE・HypeCore・TAIL）が読むものを「効く」とする
    DECISION_PREFIXES=[x for x in DECISION_PREFIXES if x[0] != "src/value/tanuki_valuation/"] + [
        ("docs/value-monitor/tanuki_valuation/", "TANUKI VALUATIONの画面"),
        ("docs/value-monitor/tanuki_score/", "TANUKI SCOREの画面"),
    ],
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=["src/value/tanuki_valuation/diag_iv_trace.py", "src/value/tanuki_valuation/growth_model_audit.py",
                          "src/value/tanuki_valuation/remove_legacy_tanuki_score.py", "src/value/tanuki_valuation/pipeline_log.txt"],
    ANNOTATIONS={
        "docs/value-monitor/tanuki_valuation/data/{T}/latest.json": "銘柄ごとの最新の評価の全体（理論株価・判定・DCF・RICE・各指標）。個別ページが読む",
        "docs/value-monitor/tanuki_valuation/data/{T}/history.json": "銘柄ごとの理論株価・上振れ余地・成長率などの推移（実行ごとに1行）",
        "docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json": "銘柄ごとの、実行の日時ごとの評価の全体のスナップショット",
        "docs/value-monitor/tanuki_valuation/data/{T}/score_history.json": "銘柄ごとのTANUKI SCORE（funda等）・判定時の株価と、その後30・60・90日の値動きの推移",
        "docs/value-monitor/tanuki_valuation/data/{T}/report.txt": "銘柄ごとの評価の計算の説明（人が読む文の形式のレポート）",
        "docs/value-monitor/tanuki_valuation/data/tickers.json": "一覧のページ用の、全銘柄の理論株価・判定の要約",
        "docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json": "銘柄ごとのHypeCoreのライフサイクル・ステージ・フェーズの推移（TANUKIの実行のたびに1行）",
        "docs/value-monitor/tanuki_valuation/data/.watcher_state.json": "score_watcherが前回の判定を覚えておく控え（判定の変化の通知に使う）",
        "config/beta_config.json": "銘柄ごとのβ（市場との連動の強さ）の上書きの設定。WACCの計算に使う",
        "${GITHUB_STEP_SUMMARY}": "GitHub Actionsの実行の画面に出す要約（人が見る）",
        "Discord（DISCORD_WEB_HOOK）": "βの更新・判定の変化のDiscordへの通知",
    },
    DECISION_DEFAULT="残す（2026-10-20）",
    DECISIONS={},
    DELETED=[
        ("src/value/tanuki_valuation の diag_iv_trace.py・growth_model_audit.py・remove_legacy_tanuki_score.py・pipeline_log.txt",
         "削除（2026-10-20、本番外。参照は自分自身のほか0件、計407行と UTF-16 のログ）"),
        ("docs/value-monitor/tanuki_score/daily_pick.json・history.json",
         "削除（2026-10-20。2026-06-27 から更新が無く、書き手・読み手0件。画面は integrated-dashboard を読む）"),
        ("誰も読まないキー14個（latest.json 等への書き込み）",
         "書き込みをやめた（2026-10-20）: avg_rev_growth_lagged・assumed_realization_years・intrinsic_value_pt・is_provisional・"
         "moat_gross_margin_norm・moat_roic_norm・moat_fcf_margin_norm・forward_earnings_yield・shares_yr_now_label・shares_yr_3ago_label・"
         "upside_pct・validated_at・hype_phase_label・hype_substage_label。98銘柄の latest.json の iv・判定・RICE・TANUKI Score は前後で同じ"),
        ("今日の特選銘柄（src/value/tanuki_score/daily_pick.py・docs/integrated-dashboard/daily_pick.json・history.json・"
         "tanuki_score/index.html の区画・TANUKI_Score_Update.yml の段）",
         "削除（2026-10-20、Koichiさんの判断で機能をまるごと。history.json の読み手は daily_pick.py 自身だけだった。"
         "src/value/tanuki_score/ と docs/integrated-dashboard/ はフォルダごと無くなった。判定の実績表は score_history.json を読み、影響なし）"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-20）。画面の区画はKoichiさんの判断で「今日の特選銘柄」だけ消し、ほかは残す。出力・キー・本番外のコードはチャット側の判断。"
                  "スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="tanuki", OUTSIDE_PREFIXES=("docs/value-monitor/tanuki_valuation/", "docs/value-monitor/tanuki_score/", "src/value/tanuki_valuation/", "src/value/tanuki_score/"),
    READ_PREFIXES=("docs/value-monitor/tanuki_valuation", "docs/value-monitor/tanuki_score", "src/value/tanuki_valuation", "src/value/tanuki_score"),
    READ_HINT="tanuki", NAME_SHORT="TANUKI",
    KNOWN_DUPLICATE_RULES=["quarter_label", "valuation_threshold", "growth_bounds", "wacc_beta", "fcf_fallback"], TITLE="TANUKI VALUATION・TANUKI Scoreの出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/value-monitor/tanuki_valuation", "docs/value-monitor/tanuki_score"),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={
        **{k: "残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）"
           for k in ("applicable", "current_subgroup", "deviation_from_current", "invalid_reason", "realized_ratio",
                     "reclassify_recommended", "recommended_subgroup", "software_system_reclassification", "v0_adjusted")},
        "g_fundamental": "残す: report_consistency_check.py が名前の組（_REC_G_CANDIDATE_KEYS）で読む（一覧の読みの形では拾えない）",
        "formula_verification": "残す: validator の検証の結果（validation.checks。overall の判定に入る）",
        "pt_shares_consistency": "残す: validator の検証の結果（validation.checks。overall の判定に入る）",
        **{k: "残す: テストが参照する" for k in ("dupont_bs_period", "max_eps", "max_eps_ttm_end", "segment_quarter", "segment_xbrl_status", "v0_note")},
        # 2026-10-21 の本番の実行（819d51bc53）で latest.json・tickers.json からは消えた。過去の history/ のスナップショットと
        # score_history.json の古いエントリには残り続けるため、一覧の「誰も読まないキー」からは消えない（FINDINGS.md 2026-10-21）
        **{k: "書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る"
           for k in ("avg_rev_growth_lagged", "assumed_realization_years", "intrinsic_value_pt", "is_provisional",
                     "moat_gross_margin_norm", "moat_roic_norm", "moat_fcf_margin_norm", "forward_earnings_yield",
                     "shares_yr_now_label", "shares_yr_3ago_label", "upside_pct", "validated_at",
                     "hype_phase_label", "hype_substage_label")},
        "fallback": "残す: 名前が一般的で、grep で読み手を決めきれない",
        "labels": "残す: 名前が一般的で、grep で読み手を決めきれない",
    },
    RULES=RULES + [
        {"id": "valuation_threshold", "name": "理論株価の割安・割高の判定のしきい値（upsideの比較）",
         "patterns": [r"\bupside\w*\s*(?:>=|>|<=|<)\s*-?\d", r"\bupside\w*\s*(?:>=|>|<=|<)\s*[A-Z_]{3,}"]},
        {"id": "growth_bounds", "name": "成長率の上限・下限",
         "patterns": [r"(?i)^\s*(?:self\.)?_?(?:growth|g|cagr)_(?:cap|floor|max|min|upper|lower)(?![a-z])\w*\s*=(?!=)",
                      r"(?i)^\s*(?:self\.)?_?(?:cap|floor|max|min)_(?:growth|g|cagr)(?![a-z])\w*\s*=(?!=)"]},
        {"id": "wacc_beta", "name": "β・WACCの作り方",
         "patterns": [r"def\s+_?\w*(?:calculate_wacc|calc_wacc|cost_of_equity|capped_beta|determine_beta|default_beta)\w*\s*\(",
                      r"function\s+\w*(?:[Ww]acc|WACC|[Bb]eta)\w*\s*\("]},
        {"id": "fcf_fallback", "name": "FCFの基準の選び方・推定のフォールバック",
         "patterns": [r"def\s+_?\w*(?:fcf_base|select_fcf_source|estimate_fcf|fcf_fallback|fcf_2yr_avg|fcf_5yr_avg)\w*\s*\("]},
    ],
)
# ---- HypeCore（段階1、2026-10-21）
_HYPECORE = dict(_SEC)
_HYPECORE.update(
    LAYER="src/value/hypecore",
    LAYER_DIRS=("src/value/hypecore",),
    OUT_JSON=f"{OUT_DIR}/hypecore_layer.json", OUT_MD=f"{OUT_DIR}/hypecore_layer.md",
    # 自分のフォルダの読み手は「効く」に数えない。TANUKI（timing・判定の入力）と画面を「効く」とする
    DECISION_PREFIXES=[x for x in DECISION_PREFIXES if x[0] != "src/value/hypecore/"] + [
        ("docs/value-monitor/hypecore/", "HypeCoreの画面"),
        ("docs/value-monitor/tanuki_valuation/", "TANUKI VALUATIONの画面"),
    ],
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],
    ANNOTATIONS={
        "src/value/hypecore/data/{T}_poc.json": "銘柄ごとの月次の期待・実体の指標とステージ判定の作業用の出力（.gitignore。docs/ に写す元）",
        "docs/value-monitor/hypecore/data/{T}_poc.json": "銘柄ごとの月次の期待・実体の指標とステージ・小区分の判定（src/value/hypecore/data/ から写したもの。画面とTANUKIのtimingが読む）",
        "docs/value-monitor/hypecore/data/tickers.json": "一覧のページ用の、HypeCoreの対象銘柄の名前の一覧（tickers・count・updated_at）。"
                                                        "読み手は docs/common/ticker-links.js・docs/index.html・docs/value-monitor/hypecore/index.html の3つ"
                                                        "（2026-10-21 に読み手をパスの全体で照合するよう直し、同じ名前の tanuki_valuation/data/tickers.json の読み手が混ざらなくなった）",
    },
    DECISION_DEFAULT="残す（2026-10-21）",
    DECISIONS={},
    DELETED=[
        ("src/value/hypecore/hypecore.html",
         "削除（2026-10-21、チャット側の判断）。2026-05-17 に poc_chart.html から改名した古い単ページ版（436行）。.gitignore の作業用の src/value/hypecore/data/ を読む手元のプレビューで、"
         "docs/ の画面とは detail.html と19%・index.html と8%の行だけが同じ。参照は pipeline.py の docstring（移植元が消えたことを書いた）だけ"),
        ("HypeCore_Update.yml の Summary の段の LC・Rec の列",
         "削除（2026-10-21、チャット側の判断）。lifecycle_label・recommendation はどの poc.json にも無く、常に「--」だった（FINDINGS 2026-10-21）"),
        ("docs/value-monitor/extreme-fear/index.html（Extreme Fear のページ、394行）と、ナビの項目・HOME のカード・site-header.js の 'ef'・site-theme.css の --tool-ef と body[data-tool=\"ef\"]",
         "削除（2026-10-21、Koichiさんの判断: 使っていない）。このページだけが読む出力・キーは無い（market_data.json・TANUKI の tickers.json・latest.json はほかの読み手がある）。"
         "scripts/audit_site_links.py でリンク切れ0件"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-21）。画面の区画はKoichiさんの判断で detail.html の7区画をすべて残す。Extreme Fear のページはKoichiさんの判断で消した（範囲外の確認の結果）。"
                  "出力・本番外のコード・Step Summary の列はチャット側の判断。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="hypecore", OUTSIDE_PREFIXES=("docs/value-monitor/hypecore/", "src/value/hypecore/"),
    READ_PREFIXES=("docs/value-monitor/hypecore", "src/value/hypecore"),
    READ_HINT="hypecore", NAME_SHORT="HypeCore",
    KNOWN_DUPLICATE_RULES=["ttm_sum"], TITLE="HypeCoreの出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/value-monitor/hypecore",),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={},
    RULES=_TANUKI["RULES"],
    FULL_SCAN=True, SCREEN_SECTIONS=True,
    HTML_COPIES=[],   # 段階1で比べた src/value/hypecore/hypecore.html は段階2で削除
    PAGE_CHECKS=[],   # 段階1で確かめた docs/value-monitor/extreme-fear/ は段階2で削除
    WORKFLOW_SUMMARY=True,
    SECTION_DECISIONS={"docs/value-monitor/hypecore/detail.html": "残す（2026-10-21、Koichiさんの判断）"},
)
# ---- EPS（adjusted_eps_analyzer、段階1、2026-10-21）
_EPS = dict(_SEC)
_EPS.update(
    LAYER="src/value/adjusted_eps_analyzer",
    LAYER_DIRS=("src/value/adjusted_eps_analyzer",),
    OUT_JSON=f"{OUT_DIR}/eps_layer.json", OUT_MD=f"{OUT_DIR}/eps_layer.md",
    # 自分のフォルダの読み手は「効く」に数えない。TANUKI（理論株価・判定）と画面を「効く」とする
    DECISION_PREFIXES=DECISION_PREFIXES + [
        ("docs/value-monitor/adjusted_eps_analyzer/", "EPSの画面"),
        ("docs/value-monitor/tanuki_valuation/", "TANUKI VALUATIONの画面"),
    ],
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],
    NONPROD_SCAN=True,
    ANNOTATIONS={
        "docs/value-monitor/adjusted_eps_analyzer/data/summary.json":
            "全銘柄の一覧（銘柄ごとの最新期のGAAP EPS・調整後EPS・差・前年比・健全性）と実行の結果（run_status）。EPSの画面の一覧・TANUKI VALUATIONの調整後EPSが読む",
        "docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json":
            "銘柄ごとの四半期の調整後EPS（調整項目・税の調整・AIの分析・特記）と経営者実行力の指標（execution_metrics）。EPSの個別ページ・TANUKI VALUATIONの調整後PERが読む",
        "docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json":
            "銘柄ごとの年次の調整後EPS（年ごとの調整項目）。TANUKI VALUATIONのFCFの外れ値の判定が、一過性の費用の突合に読む（calculator/adjustments.py）",
        "docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json":
            "銘柄ごとの直近4四半期の合計（TTM）の純利益・調整後純利益・EPSの時系列。EPSの個別ページが読む",
        "${GITHUB_STEP_SUMMARY}": "GitHub Actionsの実行の画面に出す要約（成功・失敗の件数と失敗した銘柄、人が見る）",
    },
    DECISION_DEFAULT="残す（2026-10-21）",
    DECISIONS={},
    DELETED=[
        ("docs/value-monitor/adjusted_eps_analyzer/admin.html（EPSの管理、596行）・docs/value-monitor/admin.html（VALUE MONITOR · Admin、3,650行）と、"
         "サブナビの「管理」「管理（VALUE MONITOR ADMIN）」のリンク・tests/test_admin_calc_wacc.py",
         "削除（2026-10-21、Koichiさんの判断）。GitHubのAPIでリポジトリのファイルを直接読み書きする管理のページ。操作はClaude Codeが代わりにする"
         "（CLAUDE_CODE_START.md「管理の操作の手順」）。消したファイル（kpi_config.py・fcf_conversion_config.json）を読む2か所（FINDINGS 2026-10-21）もページと一緒に無くなった。"
         "scripts/audit_site_links.py でリンク切れ・孤立ページ0件"),
        ("config/workflow_dependencies.json の label・accepts_tickers・input_param・bulk_update_order・bulk_update_phases・new_ticker_order",
         "削除（2026-10-21）。読み手は管理のページの一括更新だけだった。depends_on・outputs（生成）とキー・yml は残す"),
        ("誰も読まないキー11個（quarterly.json・ttm.json への書き込み）",
         "書き込みをやめた（2026-10-21、チャット側の判断）: " + "・".join(('adjusted_income', 'condition1', 'detected_items', 'adjustment_amount', 'dta_detected', 'maturity_monitor', 'sbc_per_share', 'sector_threshold', 'sector_exclusions', 'split_adjusted', 'xbrl_eps')) + "。"
         "全95銘柄の EPS の出力は、消したキーのほかは前後で同じ。TANUKI の理論株価・判定も前後で同じ（EPS の annual.json を FCF の外れ値の判定が読むため確かめた）"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-21）。画面の区画はKoichiさんの判断で stock.html・一覧の表を残し、管理のページ2つを消した。"
                  "誰も読まないキーはチャット側の判断（TANUKI と同じ手順で、どこにも使われていないものだけ書くのをやめた）。"
                  "スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="adjusted_eps", OUTSIDE_PREFIXES=("docs/value-monitor/adjusted_eps_analyzer/", "src/value/adjusted_eps_analyzer/"),
    READ_PREFIXES=("docs/value-monitor/adjusted_eps_analyzer", "src/value/adjusted_eps_analyzer"),
    READ_HINT="adjusted_eps", NAME_SHORT="EPS",
    KNOWN_DUPLICATE_RULES=["q4_implied", "latest_filed", "ttm_sum"], TITLE="EPS（adjusted_eps_analyzer）の出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/value-monitor/adjusted_eps_analyzer",),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={
        **{k: "残す: テストが参照する" for k in ("fair_value_auto_detect", "finished_at", "share_structure_mismatch")},
        # 今の出力のファイル（commit 済み）には、次の本番の実行（月曜の連鎖）まで残る
        **{k: "書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える" for k in ('adjusted_income', 'condition1', 'detected_items', 'adjustment_amount', 'dta_detected', 'maturity_monitor', 'sbc_per_share', 'sector_threshold', 'sector_exclusions', 'split_adjusted', 'xbrl_eps')},
    },
    RULES=_TANUKI["RULES"],
    FULL_SCAN=True, SCREEN_SECTIONS=True,
    HTML_COPIES=[],
    PAGE_CHECKS=[],   # 段階1で確かめた docs/value-monitor/admin.html は段階2で削除
    WORKFLOW_SUMMARY=True,
    SECTION_DECISIONS={
        "docs/value-monitor/adjusted_eps_analyzer/stock.html": "残す（2026-10-21、Koichiさんの判断）",
        "docs/value-monitor/adjusted_eps_analyzer/index.html": "残す（2026-10-21、Koichiさんの判断: 一覧の表）",
        "docs/value-monitor/adjusted_eps_analyzer/tickers.html": "残す（2026-10-21）",
    },
)
LAYERS: Dict[str, dict] = {"sec": _SEC, "tanuki": _TANUKI, "hypecore": _HYPECORE, "eps": _EPS}


def set_layer(name: str) -> None:
    """段階の設定を選ぶ（モジュールの変数を差し替える）。"""
    globals().update(LAYERS[name])


def in_layer(path: str) -> bool:
    return any(path.startswith(d + "/") for d in LAYER_DIRS)


def strip_layer(path: str) -> str:
    for d in LAYER_DIRS:
        if path.startswith(d + "/"):
            return path[len(d) + 1:]
    return path


PLACEHOLDER_BY_NAME = [
    (re.compile(r"(^|_)(ticker|tickers|tk|sym|symbol)($|_)|^t$|^T$", re.I), "{T}"),
    (re.compile(r"(^|_)(year|yr|fy)($|_)|^y$", re.I), "{YYYY}"),
    (re.compile(r"(^|_)(quarter|qtr|period)($|_)|^q$", re.I), "{Q}"),
    (re.compile(r"cik", re.I), "{CIK}"),
]

CODE_EXT = (".py", ".js", ".html", ".yml", ".yaml")


# ---------------------------------------------------------------- 共通

def git_ls_files(*args: str) -> List[str]:
    out = subprocess.run(["git", "ls-files", *args], cwd=REPO, capture_output=True, text=True, encoding="utf-8", check=True).stdout
    return [line for line in out.splitlines() if line]


def read_text(rel: str) -> str:
    try:
        with open(os.path.join(REPO, rel), "rb") as f:
            raw = f.read()
    except OSError:
        return ""
    for enc in (("utf-16",) if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else ("utf-8",)):
        try:
            return raw.decode(enc).replace("\r\n", "\n").replace("\r", "\n")
        except UnicodeDecodeError:
            return ""
    return ""


def is_test(rel: str) -> bool:
    base = posixpath.basename(rel)
    return rel.startswith("tests/") or "/tests/" in rel or base.startswith("test_") or base.endswith("_test.py")


def reader_type(rel: str) -> str:
    if is_test(rel):
        return "テスト"
    if rel.startswith(".github/workflows/"):
        return "ワークフロー"
    if rel.endswith((".js", ".html")):
        return "JS・HTML"
    return "Python"


def code_files() -> List[str]:
    files = []
    for rel in git_ls_files():
        if not rel.endswith(CODE_EXT):
            continue
        if rel.startswith(("venv/", "docs/architecture/")) or "/node_modules/" in rel:
            continue
        if rel.endswith((".yml", ".yaml")) and not rel.startswith(".github/workflows/"):
            continue
        if rel == "scripts/inventory/build_output_inventory.py":
            continue
        files.append(rel)
    return sorted(files)


def module_rel_to_dotted(rel: str) -> str:
    return rel[:-3].replace("/", ".") if rel.endswith(".py") else rel


# ---------------------------------------------------------------- AST: モジュールの情報とパスの解決

class Module:
    def __init__(self, rel: str, tree: ast.Module, src: str):
        self.rel = rel
        self.tree = tree
        self.src = src
        self.consts: Dict[str, List[ast.AST]] = defaultdict(list)
        self.funcs: Dict[str, ast.AST] = {}
        self.classes: Dict[str, ast.ClassDef] = {}
        self.from_imports: Dict[str, Tuple[str, str]] = {}   # local -> (module rel, name)
        self.mod_imports: Dict[str, str] = {}                # local -> module rel
        self.parents: Dict[ast.AST, ast.AST] = {}
        self.skip_lines: Set[int] = set()                    # docstring・コメントの行
        self.calls_by_line: Dict[int, List[ast.Call]] = defaultdict(list)   # 行 -> 呼び出しの式（ast.walk の順）
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                self.parents[child] = node
            if isinstance(node, ast.Call):
                self.calls_by_line[node.lineno].append(node)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        self.consts[tgt.id].append(node.value)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
                self.consts[node.target.id].append(node.value)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.funcs[node.name] = node
            elif isinstance(node, ast.ClassDef):
                self.classes[node.name] = node
            elif isinstance(node, ast.If) and "__main__" in ast.unparse(node.test):
                # if __name__ == "__main__": の中の代入もモジュールの名前として扱う（関数の中は除く。HypeCore の段階から）
                stack = list(node.body)
                while stack:
                    n = stack.pop()
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                        continue
                    if isinstance(n, ast.Assign):
                        for tgt in n.targets:
                            if isinstance(tgt, ast.Name):
                                self.consts[tgt.id].append(n.value)
                    stack.extend(ast.iter_child_nodes(n))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                base = resolve_import_module(rel, node.module, node.level)
                for a in node.names:
                    local = a.asname or a.name
                    sub = f"{base}/{a.name}.py" if base else None
                    if sub and sub in ALL_PY:
                        self.mod_imports[local] = sub
                    elif base and f"{base}.py" in ALL_PY:
                        self.from_imports[local] = (f"{base}.py", a.name)
                    elif base and f"{base}/__init__.py" in ALL_PY:
                        self.from_imports[local] = (f"{base}/__init__.py", a.name)
            elif isinstance(node, ast.Import):
                for a in node.names:
                    cand = a.name.replace(".", "/") + ".py"
                    if cand in ALL_PY:
                        self.mod_imports[a.asname or a.name] = cand
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                body = getattr(node, "body", [])
                if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant) \
                        and isinstance(body[0].value.value, str):
                    self.skip_lines.update(range(body[0].lineno, (body[0].end_lineno or body[0].lineno) + 1))
        for i, line in enumerate(src.splitlines(), 1):
            if line.lstrip().startswith("#"):
                self.skip_lines.add(i)

    def enclosing_func(self, node: ast.AST) -> Optional[ast.AST]:
        cur = self.parents.get(node)
        while cur is not None:
            if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return cur
            cur = self.parents.get(cur)
        return None

    def enclosing_class(self, node: ast.AST) -> Optional[ast.ClassDef]:
        cur = self.parents.get(node)
        while cur is not None:
            if isinstance(cur, ast.ClassDef):
                return cur
            cur = self.parents.get(cur)
        return None

    def qualname(self, node: ast.AST) -> str:
        names = []
        cur = node if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) else self.enclosing_func(node)
        while cur is not None:
            if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(cur.name)
            cur = self.parents.get(cur)
        return ".".join(reversed(names)) if names else "<module>"


ALL_PY: Set[str] = set()
MODULES: Dict[str, Module] = {}
CALLERS: Dict[Tuple[str, str], List[Tuple[str, int]]] = defaultdict(list)   # (モジュール, 関数の qualname) -> 呼び出し箇所
DEF_INDEX: Dict[str, List[Tuple[str, str]]] = defaultdict(list)            # 関数名 -> [(モジュール, qualname)]
CALL_FUNC: Dict[Tuple[str, int], str] = {}                                  # (モジュール, 行) -> 呼び出し元の関数の qualname
ALT = " || "   # 引数を呼び出し元から解決したときの、複数のパスの区切り


def resolve_import_module(rel: str, module: Optional[str], level: int) -> Optional[str]:
    if level:
        base = posixpath.dirname(rel)
        for _ in range(level - 1):
            base = posixpath.dirname(base)
        return posixpath.join(base, module.replace(".", "/")) if module else base
    if not module:
        return None
    path = module.replace(".", "/")
    # sys.path に自分のフォルダを足して読む形（tanuki_valuation の from core_calculator import … 等）は、同じフォルダから探す
    if not any(c in ALL_PY for c in (f"{path}.py", f"{path}/__init__.py")) and not any(p.startswith(path + "/") for p in ALL_PY):
        local = posixpath.join(posixpath.dirname(rel), path)
        if any(c in ALL_PY for c in (f"{local}.py", f"{local}/__init__.py")) or any(p.startswith(local + "/") for p in ALL_PY):
            return local
    return path


def load_module(rel: str) -> Optional[Module]:
    if rel in MODULES:
        return MODULES[rel]
    src = read_text(rel)
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    MODULES[rel] = Module(rel, tree, src)
    return MODULES[rel]


UNRESOLVED = "<?>"


def placeholder_for(expr: ast.AST) -> str:
    name = None
    cur = expr
    while True:
        if isinstance(cur, ast.Call):
            cur = cur.func.value if isinstance(cur.func, ast.Attribute) else (cur.args[0] if cur.args else None)
        elif isinstance(cur, ast.Attribute):
            name = name or cur.attr
            cur = cur.value
        elif isinstance(cur, ast.Subscript):
            sl = cur.slice
            if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
                name = sl.value
            cur = cur.value
        elif isinstance(cur, ast.Name):
            name = cur.id if name is None or name in ("upper", "lower", "strip") else name
            break
        else:
            break
    if name in (None, "upper", "lower", "strip") and isinstance(cur, ast.Name):
        name = cur.id
    if not name:
        return "{x}"
    for pat, ph in PLACEHOLDER_BY_NAME:
        if pat.search(name):
            return ph
    return "{" + name + "}"


def _func_index(mod: Module, func: ast.AST) -> Tuple[Dict[str, List[ast.Assign]], Set[str]]:
    """関数の中の (名前 → 代入〈ast.walk の順〉, ループの変数の名前)。resolve() が名前ごとに関数を歩き直さないよう、関数ごとに1回だけ作る。"""
    cache = mod.__dict__.setdefault("_func_index", {})
    key = id(func)
    if key not in cache:
        assigns: Dict[str, List[ast.Assign]] = defaultdict(list)
        loops: Set[str] = set()
        for n in ast.walk(func):
            if isinstance(n, ast.Assign):
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        assigns[t.id].append(n)
            elif isinstance(n, (ast.For, ast.comprehension)) and isinstance(n.target, ast.Name):
                loops.add(n.target.id)
        cache[key] = (assigns, loops)
    return cache[key]


def _self_assigns(mod: Module, cls: ast.ClassDef) -> Dict[str, List[ast.Assign]]:
    """クラスの中の self.名前 = … の代入（名前 → 代入〈ast.walk の順〉）。クラスごとに1回だけ作る。"""
    cache = mod.__dict__.setdefault("_self_assigns", {})
    if id(cls) not in cache:
        d: Dict[str, List[ast.Assign]] = defaultdict(list)
        for n in ast.walk(cls):
            if isinstance(n, ast.Assign):
                for t in n.targets:
                    if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id == "self":
                        d[t.attr].append(n)
        cache[id(cls)] = d
    return cache[id(cls)]


def resolve(expr: ast.AST, mod: Module, use_node: ast.AST, depth: int = 0) -> str:
    """パスの式を、パスの型の文字列にする。解決できない部分は <?> を含む。"""
    if depth > 30 or expr is None:
        return UNRESOLVED
    r = lambda e: resolve(e, mod, use_node, depth + 1)  # noqa: E731
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return expr.value
    if isinstance(expr, ast.JoinedStr):
        parts = []
        for v in expr.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                inner = r(v.value)
                parts.append(inner if UNRESOLVED not in inner and "/" in inner else placeholder_for(v.value))
        return "".join(parts)
    if isinstance(expr, ast.Name):
        if expr.id == "__file__":
            return mod.rel
        func = mod.enclosing_func(use_node)
        # 入れ子の関数の中で代入も引数も無い名前は、外側の関数で探す（クロージャ）
        while func is not None and expr.id not in _func_index(mod, func)[0]                 and not any(a.arg == expr.id for a in func.args.args + func.args.kwonlyargs):
            outer = mod.enclosing_func(func)
            if outer is None:
                break
            use_node, func = func, outer
        # 関数の中の代入（使う行より前で最も近いもの）
        if func is not None:
            best = None
            assigns, loops = _func_index(mod, func)
            if expr.id in loops:
                return placeholder_for(expr)
            for n in assigns.get(expr.id, []):
                if n.lineno < getattr(use_node, "lineno", 10 ** 9) and (best is None or n.lineno > best.lineno):
                    best = n
            if best is not None:
                return resolve(best.value, mod, best, depth + 1)
            args = func.args
            for a, d in zip(args.args[len(args.args) - len(args.defaults):], args.defaults):
                # None・空の文字列の既定値は「渡されなければ使わない」の印なので、呼び出し元の引数から解決する
                if a.arg == expr.id and not (isinstance(d, ast.Constant) and d.value in (None, "")):
                    return r(d)
            if any(a.arg == expr.id for a in args.args + args.kwonlyargs):
                if any(p.search(expr.id) for p, _ in PLACEHOLDER_BY_NAME):
                    return placeholder_for(expr)
                return resolve_param_from_callers(func, expr.id, mod, depth)
        if expr.id in mod.consts:
            cands = [r(v) for v in mod.consts[expr.id]]
            good = [c for c in cands if UNRESOLVED not in c]
            return good[-1] if good else cands[-1]
        if expr.id in mod.from_imports:
            src_rel, name = mod.from_imports[expr.id]
            other = load_module(src_rel)
            if other is not None and name in other.consts:
                return resolve(other.consts[name][-1], other, other.consts[name][-1], depth + 1)
        return placeholder_for(expr) if any(p.search(expr.id) for p, _ in PLACEHOLDER_BY_NAME) else UNRESOLVED
    if isinstance(expr, ast.Attribute) and expr.attr == "parent":
        inner = r(expr.value)
        return UNRESOLVED if UNRESOLVED in inner else posixpath.dirname(inner)
    if isinstance(expr, ast.Subscript) and isinstance(expr.value, ast.Attribute) and expr.value.attr == "parents"             and isinstance(expr.slice, ast.Constant) and isinstance(expr.slice.value, int):
        inner = r(expr.value.value)
        if UNRESOLVED in inner:
            return UNRESOLVED
        for _ in range(expr.slice.value + 1):
            inner = posixpath.dirname(inner)
        return inner
    if isinstance(expr, ast.Attribute):
        if isinstance(expr.value, ast.Name) and expr.value.id == "self":
            cls = mod.enclosing_class(use_node)
            if cls is not None:
                cands = [resolve(n.value, mod, n, depth + 1) for n in _self_assigns(mod, cls).get(expr.attr, [])]
                good = [c for c in cands if UNRESOLVED not in c]
                if good:
                    return good[-1]
            return UNRESOLVED
        if isinstance(expr.value, ast.Name) and expr.value.id in mod.mod_imports:
            other = load_module(mod.mod_imports[expr.value.id])
            if other is not None and expr.attr in other.consts:
                return resolve(other.consts[expr.attr][-1], other, other.consts[expr.attr][-1], depth + 1)
        return UNRESOLVED
    if isinstance(expr, ast.BinOp):
        if isinstance(expr.op, ast.Div):
            return posixpath.join(r(expr.left), r(expr.right))
        if isinstance(expr.op, ast.Add):
            return r(expr.left) + r(expr.right)
        return UNRESOLVED
    if isinstance(expr, ast.BoolOp) and isinstance(expr.op, ast.Or):
        cands = [r(v) for v in expr.values]
        good = [c for c in cands if UNRESOLVED not in c]
        return good[0] if good else cands[-1]
    if isinstance(expr, ast.IfExp):
        a, b = r(expr.body), r(expr.orelse)
        return a if UNRESOLVED not in a else b
    if isinstance(expr, ast.Subscript):
        if isinstance(expr.value, ast.Attribute) and expr.value.attr == "environ":
            sl = expr.slice
            if isinstance(sl, ast.Constant):
                return "${" + str(sl.value) + "}"
        return placeholder_for(expr)
    if isinstance(expr, ast.Call):
        f = expr.func
        fname = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
        fbase = f.value.id if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) else ""
        if fname == "join" and (fbase in ("path", "os", "posixpath") or isinstance(f.value, ast.Attribute)):
            parts = [r(a) for a in expr.args]
            return posixpath.join(*parts) if parts else UNRESOLVED
        if fname in ("dirname",):
            inner = r(expr.args[0]) if expr.args else UNRESOLVED
            return UNRESOLVED if inner == UNRESOLVED else posixpath.dirname(inner)
        if fname in ("abspath", "realpath", "normpath", "expanduser", "str", "Path", "resolve", "fspath"):
            if expr.args:
                return r(expr.args[0])
            if isinstance(f, ast.Attribute):
                return r(f.value)
            return UNRESOLVED
        if fname == "get" and isinstance(f, ast.Attribute) and isinstance(f.value, ast.Attribute) and f.value.attr == "environ":
            if expr.args and isinstance(expr.args[0], ast.Constant):
                return "${" + str(expr.args[0].value) + "}"
        if fname == "getenv" and expr.args and isinstance(expr.args[0], ast.Constant):
            return "${" + str(expr.args[0].value) + "}"
        if fname == "format" and isinstance(f, ast.Attribute):
            return re.sub(r"\{[^}]*\}", "{x}", r(f.value))
        if fname in ("upper", "lower", "strip") and isinstance(f, ast.Attribute):
            return r(f.value)
        # 返り値が1つのモジュールの関数（default_path() 等）
        target = None
        if isinstance(f, ast.Name) and f.id in mod.funcs:
            target = (mod, mod.funcs[f.id])
        elif isinstance(f, ast.Name) and f.id in mod.from_imports:
            src_rel, name = mod.from_imports[f.id]
            other = load_module(src_rel)
            if other is not None and name in other.funcs:
                target = (other, other.funcs[name])
        if target is not None:
            m2, fn = target
            rets = [n for n in ast.walk(fn) if isinstance(n, ast.Return) and n.value is not None]
            vals = {resolve(n.value, m2, n, depth + 1) for n in rets}
            vals.discard(UNRESOLVED)
            # 環境変数だけで上書きする返り値（${X}）は、ほかに決まったパスがあれば除く
            if len(vals) > 1:
                vals = {v for v in vals if not re.fullmatch(r"\$\{[A-Z_]+\}", v)} or vals
            if len(vals) == 1:
                return vals.pop()
        return UNRESOLVED
    return UNRESOLVED


def resolve_param_from_callers(func: ast.AST, pname: str, mod: Module, depth: int) -> str:
    """引数のパスを、呼び出し元の引数の式から解決する（呼び出し元ごとに違えば ALT で並べる）。"""
    if depth > 8:
        return UNRESOLVED
    args = func.args
    names = [a.arg for a in args.args]
    is_method = bool(names) and names[0] in ("self", "cls")
    idx = names.index(pname) if pname in names else None
    if idx is not None and is_method:
        idx -= 1
    vals: List[str] = []
    # __init__ の引数は、クラスを呼ぶ箇所（インスタンスを作る箇所）の引数から解決する
    cls = mod.enclosing_class(func) if func.name == "__init__" else None
    names_ok = {func.name} if cls is None else {cls.name}
    keys = [(mod.rel, mod.qualname(func))] + ([(mod.rel, cls.name)] if cls is not None else [])
    for (c_rel, c_line) in [c for k in keys for c in CALLERS.get(k, [])]:
        cm = MODULES.get(c_rel)
        if cm is None:
            continue
        for call in cm.calls_by_line.get(c_line, []):
            fn = call.func.attr if isinstance(call.func, ast.Attribute) else (call.func.id if isinstance(call.func, ast.Name) else "")
            call_args = call.args
            if fn == "submit" and call.args and isinstance(call.args[0], ast.Name) and (
                    call.args[0].id in names_ok or cm.from_imports.get(call.args[0].id, ("", ""))[1] in names_ok):
                fn, call_args = call.args[0].id, call.args[1:]   # executor.submit(関数, 引数…)
                names_ok = names_ok | {fn}
            if fn not in names_ok and not (isinstance(call.func, ast.Name) and cm.from_imports.get(call.func.id, ("", ""))[1] in names_ok):
                continue
            arg = next((k.value for k in call.keywords if k.arg == pname), None)
            if arg is None and idx is not None and idx < len(call_args) and not any(isinstance(a, ast.Starred) for a in call_args[:idx + 1]):
                arg = call_args[idx]
            if arg is None:
                continue
            v = resolve(arg, cm, call, depth + 2)
            vals.extend(v.split(ALT))
    good = sorted({normalize_path(v) for v in vals if UNRESOLVED not in v})
    if good and len(good) == len({v for v in vals}):
        return ALT.join(good)
    if good:
        return ALT.join(good + [UNRESOLVED])
    return UNRESOLVED


def normalize_path(p: str) -> str:
    if ALT in p:
        return ALT.join(normalize_path(x) for x in p.split(ALT))
    if p.startswith("${"):
        return p
    p = p.replace("\\", "/")
    # GitHub Actions のリポジトリの根（${GITHUB_WORKSPACE}）は、根からのパスとして扱う
    if p.startswith("${GITHUB_WORKSPACE}/"):
        p = p[len("${GITHUB_WORKSPACE}/"):]
    # リポジトリの根だけが決まらないパス（<?>/common/... 等）は、根からのパスとして扱う
    m = re.match(r"^(?:<\?>/)+((?:common|docs|config|src|data|scripts|discover)/.*)$", p)
    if m:
        p = m.group(1)
    if p.startswith(REPO.replace("\\", "/")):
        p = p[len(REPO) + 1:]
    p = posixpath.normpath(p) if p else p
    return "" if p == "." else p.lstrip("./") if p.startswith("./") else p


# ---------------------------------------------------------------- 書き込み・読み込みの場所

WRITE_MODES = re.compile(r"[wax]")


def _mode_of(call: ast.Call, pos: int) -> Optional[str]:
    if len(call.args) > pos and isinstance(call.args[pos], ast.Constant) and isinstance(call.args[pos].value, str):
        return call.args[pos].value
    for kw in call.keywords:
        if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
            return str(kw.value.value)
    return None


def io_sites(mod: Module) -> Tuple[List[dict], List[dict]]:
    """(書き込み, 読み込み) の場所。各 {line, func, path, kind}。段階に関係しないので、モジュールごとに1回だけ作る（呼び出し側は変えない）"""
    cached = mod.__dict__.get("_io_sites")
    if cached is None:
        cached = mod._io_sites = _io_sites(mod)
    return cached


def _io_sites(mod: Module) -> Tuple[List[dict], List[dict]]:
    writes, reads = [], []
    for node in ast.walk(mod.tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        fname = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else "")
        site = {"line": node.lineno, "func": mod.qualname(node)}
        if fname == "open" and (isinstance(f, ast.Name) or (isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in ("io", "codecs"))):
            if not node.args:
                continue
            mode = _mode_of(node, 1) or "r"
            path = normalize_path(resolve(node.args[0], mod, node))
            (writes if WRITE_MODES.search(mode) else reads).append({**site, "path": path, "kind": "ファイル"})
        elif fname == "open" and isinstance(f, ast.Attribute):     # Path(...).open("w")
            mode = _mode_of(node, 0) or "r"
            path = normalize_path(resolve(f.value, mod, node))
            (writes if WRITE_MODES.search(mode) else reads).append({**site, "path": path, "kind": "ファイル"})
        elif fname in ("write_text", "write_bytes") and isinstance(f, ast.Attribute):
            writes.append({**site, "path": normalize_path(resolve(f.value, mod, node)), "kind": "ファイル"})
        elif fname in ("read_text", "read_bytes") and isinstance(f, ast.Attribute):
            reads.append({**site, "path": normalize_path(resolve(f.value, mod, node)), "kind": "ファイル"})
        elif fname in ("to_csv", "to_json", "to_parquet") and node.args:
            writes.append({**site, "path": normalize_path(resolve(node.args[0], mod, node)), "kind": "ファイル"})
        elif fname in ("read_json", "read_csv", "read_parquet") and node.args:
            reads.append({**site, "path": normalize_path(resolve(node.args[0], mod, node)), "kind": "ファイル"})
        elif fname in ("replace", "move", "copy", "copyfile", "copy2") and isinstance(f, ast.Attribute) \
                and isinstance(f.value, ast.Name) and f.value.id in ("os", "shutil") and len(node.args) >= 2:
            writes.append({**site, "path": normalize_path(resolve(node.args[1], mod, node)), "kind": "ファイル"})
        elif (fname == "post" and isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id in ("requests", "session", "s")) \
                or fname == "urlopen":
            env = sorted(set(re.findall(r"[\"']((?:DISCORD|WEBHOOK)[A-Z_]*)[\"']", mod.src)))
            if env:
                writes.append({**site, "path": "Discord（" + "・".join(env) + "）", "kind": "通知"})
    def split(sites: List[dict]) -> List[dict]:
        out = []
        for s in sites:
            for p in s["path"].split(ALT):
                out.append({**s, "path": p, "from_callers": ALT in s["path"]})
        return out
    writes, reads = split(writes), split(reads)
    for w in writes:
        if w["path"] == "${GITHUB_STEP_SUMMARY}":
            w["kind"] = "Step Summary"
    return writes, reads


# ---------------------------------------------------------------- パスの型 → 正規表現・grep の式

PH_RE = re.compile(r"\{[^}]*\}")


def template_regex(tmpl: str) -> re.Pattern:
    out, pos = "", 0
    for m in PH_RE.finditer(tmpl):
        out += re.escape(tmpl[pos:m.start()])
        ph = m.group(0)
        out += r"\d{4}" if ph == "{YYYY}" else (r"[A-Z0-9][A-Z0-9.=^-]*" if ph == "{T}" else r"[^/]+")
        pos = m.end()
    out += re.escape(tmpl[pos:])
    return re.compile("^" + out + "$")


AMBIGUOUS_BASES: Dict[str, bool] = {}   # ファイル名（型）→ リポジトリの中で別のフォルダにも同じ名前があるか（build() が埋める）
GENERIC_BASES = {"_summary", "summary", "index", "latest", "data", "result", "results", "", "state"}


def tail_match(tmpl: str, path: str) -> bool:
    """根だけが決まらないパス（<?>/value-monitor/... 等）を、出力のパスの型の末尾（2段以上）で照合する。"""
    m = re.match(r"^(?:<\?>/)+([^<]+)$", path)
    if not m or m.group(1).count("/") < 2:
        return False
    # パスの中のプレースホルダーは見本の値に置き換えてから照合する
    tail = m.group(1).replace("{T}", "AAA").replace("{YYYY}", "2020")
    tail = PH_RE.sub("x", tail)
    parts = tmpl.split("/")
    return any(template_regex("/".join(parts[i:])).match(tail) for i in range(1, len(parts)))


def needles_for(tmpl: str) -> Tuple[List[re.Pattern], Optional[str]]:
    """(ファイル名の grep の式, 一緒に必要なフォルダの語)。"""
    base = posixpath.basename(tmpl)
    stem = re.sub(r"\.(json|csv|txt|md)$", "", base)
    lits = [p for p in PH_RE.split(stem)]
    lit_core = "".join(lits).strip("_")
    ext = base[len(stem):]
    pats = []
    if len(PH_RE.sub("", stem)) > 0:
        # プレースホルダーの所は、前後・途中のどこでも任意の文字（annual_{YYYY}.json の末尾の年も）
        body = r"[^\"'\s]{0,40}?".join(re.escape(p) for p in lits)
        pats.append(re.compile(body + re.escape(ext)))
        first = next((p for p in lits if p), "")
        if len(first) >= 6:
            pats.append(re.compile(r"[\"'`]" + re.escape(first) + r"[\"'`]"))
    need_dir = None
    if lit_core in GENERIC_BASES or len(lit_core) < 6 or AMBIGUOUS_BASES.get(base, False):
        parts = [p for p in posixpath.dirname(tmpl).split("/") if p and not PH_RE.search(p)]
        need_dir = parts[-1] if parts else None
        if not pats:
            pats.append(re.compile(re.escape(ext) if ext else r"$^"))
    return pats, need_dir


# ---------------------------------------------------------------- 読み込みの行のパスの解決（2026-10-21、同じ名前の別のフォルダのファイルを分ける）
# ファイル名の grep で当たった行は、その行の文字列を、組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と
# 定数）までたどってパスにし、出力のパスの型と全体で照合する。フォルダを決めきれないものは「読み方を決めきれない」に入れる

SITE_ROOT = "/On-a-journey"   # GitHub Pages の根（docs/）
TOP_DIRS: Set[str] = set()    # リポジトリの最上位のフォルダ（prepare() が埋める）
_SAMPLES = ("AAA", "2020", "x")
_ROOT = "\x00"                # JS の根の決まらない値（<?> の ? がクエリの区切りと紛れないよう、最後に <?> にする）
_PATH_CALLS = {"join", "Path", "abspath", "realpath", "normpath", "expanduser", "fspath", "str"}
_JS_STR_RE = re.compile(r"""(['"`])((?:\\.|(?!\1)[^\\])*)\1""")
_JS_LEFT_RE = re.compile(r"""(?:(['"`])(?:\\.|(?!\1)[^\\])*\1|[\w.$]+)\s*\+\s*$""")
_JS_RIGHT_RE = re.compile(r"""^\s*\+\s*(?:(['"`])(?:\\.|(?!\1)[^\\])*\1|[\w.$]+(?:\(\))?)""")
_JS_TICKER_RE = re.compile(r"ticker$|(^|\.)_?(tk|t|sym|symbol)$", re.I)
_YML_TOKEN_RE = re.compile(r"[\w./{}$*-]+")
_MENTIONS: Dict[Tuple[str, int], List[Tuple[str, str]]] = {}   # (ファイル, 行) → [(文字列, 解決したパス)]
_CODE_LINES: Dict[str, List[str]] = {}   # ファイル → コメント・docstring を除いた行（本番外のコードの参照の照合）
_CODE_TEXT: Dict[str, str] = {}          # ファイル → その行をつないだ文字列
WORKFLOW_COPIES: List[Tuple[str, str]] = []   # ワークフローの cp・rsync の (写し先, 元)。写しを読む行は元の出力の読み手（prepare() が埋める）
_COPY_RE = re.compile(r"^\s*(?:rsync|cp)\s+(?:-[-\w]+\s+)*([\w./-]+)\s+([\w./-]+)\s*$")


def _workflow_copies(texts: Dict[str, str]) -> List[Tuple[str, str]]:
    out = []
    for rel, t in texts.items():
        if rel.startswith(".github/workflows/"):
            for line in t.splitlines():
                m = _COPY_RE.match(line)
                if m and "/" in m.group(1) and "/" in m.group(2):
                    out.append((m.group(2), m.group(1)))
    return out


def _sample(p: str, s_: str) -> str:
    return PH_RE.sub(lambda m: "2020" if m.group(0) == "{YYYY}" else ("AAA" if m.group(0) == "{T}" else s_), p)


def path_matches(tmpl: str, p: str) -> bool:
    """解決したパス（プレースホルダーを含んでよい）が、出力のパスの型に当たるか。"""
    if p == tmpl:
        return True
    rx = template_regex(tmpl)
    return any(rx.match(_sample(p, x)) for x in _SAMPLES) or tail_match(tmpl, p)


def _name_segments(tmpl: str) -> int:
    """ファイルの名前として比べる末尾の段数。ファイル名がプレースホルダーだけ（{T}.json 等）なら、文字のある親のフォルダまで含める。"""
    parts = tmpl.split("/")
    for k in range(1, len(parts) + 1):
        if re.sub(r"\.\w+$", "", PH_RE.sub("", "/".join(parts[-k:]))).strip("/"):
            return k
    return len(parts)


def classify_mention(tmpl: str, p: str) -> str:
    """reader（この出力）・other（同じ名前の別のファイル）・undetermined（同じ名前で、フォルダを決めきれない）・none（この名前ではない）。"""
    alts = [x.strip() for x in p.split(ALT) if x.strip()] if p else []
    if len(alts) > 1:
        got = [classify_mention(tmpl, x) for x in alts]
        return next((c for c in ("reader", "undetermined", "other") if c in got), "none")
    p = alts[0] if alts else ""
    if p.startswith("${GITHUB_WORKSPACE}/"):   # ワークフローのリポジトリの根
        p = p[len("${GITHUB_WORKSPACE}/"):]
    if p.startswith("/"):   # 根が絶対パス（テストが渡す /repo 等）: リポジトリの最上位のフォルダの所から
        segs = p.split("/")
        i = next((j for j, x in enumerate(segs) if x in TOP_DIRS), None)
        p = "/".join(segs[i:]) if i is not None else UNRESOLVED + p
    if not p or re.match(r"^[a-z0-9]+:/", p):   # 外部の URL
        return "none"
    if path_matches(tmpl, p):
        return "reader"
    for dst, src in WORKFLOW_COPIES:   # ワークフローが写したファイル（docs/common/sec_data/normalized/ 等）
        if (dst.endswith("/") and p.startswith(dst) and path_matches(tmpl, src + p[len(dst):])) or (p == dst and path_matches(tmpl, src)):
            return "reader"
    k = _name_segments(tmpl)
    name = "/".join(p.split("/")[-k:])
    nrx = template_regex("/".join(tmpl.split("/")[-k:]))
    if UNRESOLVED in name or name.count("/") < k - 1 or not any(nrx.match(_sample(name, x)) for x in _SAMPLES):
        return "none"
    head = posixpath.dirname(p)
    if UNRESOLVED in head:
        tail = p.rsplit(UNRESOLVED, 1)[1].lstrip("/")
        parts = tmpl.split("/")
        if "/" in tail and not any(template_regex("/".join(parts[i:])).match(_sample(tail, x))
                                   for i in range(len(parts)) for x in _SAMPLES):
            return "other"
        return "undetermined"
    return "other" if head.split("/")[0] in TOP_DIRS else "undetermined"


def _py_str_nodes(mod: Module) -> Dict[int, List[ast.AST]]:
    cached = mod.__dict__.get("_str_nodes")
    if cached is None:
        cached = defaultdict(list)
        for n in ast.walk(mod.tree):
            if isinstance(n, ast.JoinedStr) or (isinstance(n, ast.Constant) and isinstance(n.value, str)
                                                and not isinstance(mod.parents.get(n), ast.JoinedStr)):
                for ln in range(n.lineno, (n.end_lineno or n.lineno) + 1):
                    cached[ln].append(n)
        mod._str_nodes = cached
    return cached


_NAME_CHECKS = {"endswith", "startswith", "replace", "removesuffix", "removeprefix", "split", "rsplit", "strip", "rstrip", "lstrip",
                "find", "rfind", "index", "count", "get", "pop", "fnmatch"}


def _py_climb(mod: Module, n: ast.AST) -> Tuple[ast.AST, str]:
    """パスを組み立てる式の外側まで上がる。(式, 使い方)。使い方は path・glob・name（ファイル名を比べるだけ。読み込みではない）。"""
    cur = n
    while True:
        par = mod.parents.get(cur)
        if isinstance(par, ast.BinOp) and isinstance(par.op, (ast.Add, ast.Div)):
            cur = par
        elif isinstance(par, ast.Call) and cur in par.args and (
                (isinstance(par.func, ast.Name) and par.func.id in _PATH_CALLS)
                or (isinstance(par.func, ast.Attribute) and par.func.attr in _PATH_CALLS)):
            cur = par
        elif isinstance(par, ast.Call) and cur in par.args and isinstance(par.func, ast.Attribute) and par.func.attr in ("glob", "rglob"):
            return par, "glob"
        elif isinstance(par, ast.Call) and isinstance(par.func, ast.Attribute) and par.func.attr in _NAME_CHECKS and (
                par.func.attr not in ("get", "pop") or (par.args and par.args[0] is cur)):
            return cur, "name"
        elif isinstance(par, ast.Compare) or (isinstance(par, ast.Dict) and cur in par.keys):
            return cur, "name"
        else:
            return cur, "path"


def _py_resolve_use(mod: Module, n: ast.AST, depth: int = 0) -> List[str]:
    cur, how = _py_climb(mod, n)
    if how == "name":
        return []
    if how == "glob":
        f = cur.func
        base = resolve(f.value, mod, cur) if f.value is not None else UNRESOLVED
        pat = resolve(cur.args[0], mod, cur)
        if f.attr == "glob" and isinstance(f.value, ast.Name) and f.value.id == "glob":   # glob.glob(パス)
            return [normalize_path(pat.replace("*", "{x}"))]
        return [normalize_path(posixpath.join(base, pat).replace("*", "{x}"))]
    got = normalize_path(resolve(cur, mod, cur)).replace("*", "{x}")
    # ファイル名だけの定数（FRESHNESS_FILENAME = "_freshness.json" 等）は、その名前を使う場所でパスにする
    par = mod.parents.get(cur)
    if depth == 0 and "/" not in got and isinstance(par, (ast.Assign, ast.AnnAssign)):
        tgts = par.targets if isinstance(par, ast.Assign) else [par.target]
        names = {t.id for t in tgts if isinstance(t, ast.Name)}
        uses = [u for u in ast.walk(mod.tree) if isinstance(u, ast.Name) and u.id in names and isinstance(u.ctx, ast.Load)]
        if uses:
            return [x for u in uses for x in _py_resolve_use(mod, u, depth + 1)]
    return [got]


def _py_mentions(mod: Module, line: int) -> List[Tuple[str, str]]:
    out = []
    for n in _py_str_nodes(mod).get(line, []):
        text = n.value if isinstance(n, ast.Constant) else "".join(
            str(v.value) if isinstance(v, ast.Constant) else "{}" for v in n.values)
        paths = _py_resolve_use(mod, n)
        out.append((text, ALT.join(paths) if paths else ""))
    return out


def _js_consts(rel: str, text: str) -> Dict[str, str]:
    d = _JS_CONSTS.get(rel)
    if d is None:
        d = {}
        for m in re.finditer(r"(?:const|let|var)\s+(\w+)\s*=\s*([^;\n]+)", text):
            d.setdefault(m.group(1), m.group(2).strip())
        _JS_CONSTS[rel] = d
    return d


_JS_CONSTS: Dict[str, Dict[str, str]] = {}


def _js_value(expr: str, consts: Dict[str, str], depth: int = 0) -> str:
    """JS の + でつないだ式（文字列・定数・変数）を、パスの文字列にする。根の決まらない名前は _ROOT、途中の変数はプレースホルダー。"""
    out, i, first = "", 0, True
    while i < len(expr):
        c = expr[i]
        if c.isspace() or c == "+":
            i += 1
            continue
        if c in "'\"`":
            j = i + 1
            while j < len(expr) and expr[j] != c:
                j += 2 if expr[j] == "\\" else 1
            lit = expr[i + 1:j]
            if c == "`":
                lit = re.sub(r"\$\{([^}]*)\}", lambda m: _js_name(m.group(1).strip(), consts, depth, False), lit)
            out += lit
            i = j + 1
        else:
            m = re.match(r"[\w.$]+(?:\([^)]*\))?", expr[i:])
            if not m:
                return out + _ROOT
            out += _js_name(m.group(0), consts, depth, first)
            i += m.end()
        first = False
    return out


def _js_name(name: str, consts: Dict[str, str], depth: int, first: bool) -> str:
    if name in consts and depth < 6:
        v = _js_value(consts[name], consts, depth + 1)
        if _ROOT not in v:
            return v
    if _JS_TICKER_RE.search(name):
        return "{T}"
    return _ROOT if first else "{x}"


def _js_mentions(rel: str, text: str, line: str) -> List[Tuple[str, str]]:
    consts = _js_consts(rel, text)
    api = "api.github.com/repos/" in text
    out = []
    for m in _JS_STR_RE.finditer(line):
        a, b = m.start(), m.end()
        while True:
            lm = _JS_LEFT_RE.search(line[:a])
            if not lm:
                break
            a = lm.start()
        while True:
            rm = _JS_RIGHT_RE.match(line[b:])
            if not rm:
                break
            b += rm.end()
        p = _js_value(line[a:b], consts)
        if m.group(1) == "`":
            p = re.sub(r"\$\{[^}]*\}", "{x}", p)
        p = p.split("?")[0].split("#")[0]
        if re.match(r"^(https?:)?//", p):
            g = re.search(r"api\.github\.com/repos/[^/]+/[^/]+/contents/(.+)$", p)
            p = g.group(1) if g else ""
        elif p.startswith(SITE_ROOT + "/"):
            p = "docs/" + p[len(SITE_ROOT) + 1:]
        elif p.startswith(_ROOT) or p.startswith("/"):
            p = UNRESOLVED + "/" + p.lstrip(_ROOT).lstrip("/")
        elif rel.endswith(".js"):
            p = UNRESOLVED + "/" + p   # .js の相対パスは、読み込むページのフォルダからになる
        elif not (api and p.split("/")[0] in TOP_DIRS):
            p = posixpath.join(posixpath.dirname(rel), p)
        p = p.replace(_ROOT, UNRESOLVED)
        out.append((m.group(2), normalize_path(p) if p else ""))
    return out


def _yml_mentions(rel: str, text: str, line: str) -> List[Tuple[str, str]]:
    """ワークフローの行の、パスらしい語（作業フォルダはリポジトリの根）。同じファイルの代入（Python の name = "…"・シェルの name=…）は
    {name}・$name・${name} に入れる。/ の無い名前（commit のメッセージ・比べ）は読み込みとしない。"""
    consts = _JS_CONSTS.get(rel)
    if consts is None:
        consts = {}
        for m in re.finditer(r"^\s*(?:export\s+)?(\w+)\s*=\s*[\"']?([\w./-]+/[\w./-]*)[\"']?\s*$", text, re.M):
            consts.setdefault(m.group(1), m.group(2))
        _JS_CONSTS[rel] = consts
    out = []
    for m in _YML_TOKEN_RE.finditer(line):
        tok = re.sub(r"\$\{\{[^}]*\}\}", "{x}", m.group(0))
        tok = re.sub(r"\$?\{(\w+)\}|\$(\w+)", lambda g: consts.get(g.group(1) or g.group(2), "{x}"), tok).replace("*", "{x}")
        out.append((m.group(0), normalize_path(tok) if "/" in tok else ""))
    return out


def line_mentions(rel: str, line_no: int, line: str, text: str) -> List[Tuple[str, str]]:
    """その行の文字列ごとの (文字列, 解決したパス)。.py は AST、.js・.html は + と定数、ワークフローは根からのパス。"""
    key = (rel, line_no)
    if key not in _MENTIONS:
        if rel.endswith(".py"):
            mod = MODULES.get(rel)
            got = _py_mentions(mod, line_no) if mod is not None else [(line, UNRESOLVED)]
        elif rel.endswith((".yml", ".yaml")):
            got = _yml_mentions(rel, text, line)
        else:
            got = _js_mentions(rel, text, line)
        _MENTIONS[key] = got
    return _MENTIONS[key]


def mention_class(tmpl: str, pats: List[re.Pattern], rel: str, line_no: int, line: str, text: str) -> str:
    """その行がこの出力を読むか: reader・undetermined・no（別のファイル・ファイル名でない文字列）。"""
    got = [classify_mention(tmpl, path) for lit, path in line_mentions(rel, line_no, line, text)
           if any(rx.search(lit) or rx.search('"' + lit + '"') for rx in pats)]
    if "reader" in got:
        return "reader"
    return "undetermined" if "undetermined" in got else "no"


# ---------------------------------------------------------------- 呼び出し（import を解決した呼び出し先）

def calls_in(mod: Module) -> List[Tuple[int, str, str]]:
    """(行, 呼び出し先のモジュール rel, 関数名) の一覧（モジュールごとに1回だけ作る）。"""
    cached = getattr(mod, "_calls_cache", None)
    if cached is None:
        cached = mod._calls_cache = _calls_in(mod)
    return cached


def _scope_instances(mod: Module, scope: ast.AST) -> Dict[str, Tuple[str, str]]:
    """スコープの中で「変数 = import したクラス(...)」と代入した最初のもの（ast.walk の順）。スコープごとに1回だけ作る。"""
    cache = mod.__dict__.setdefault("_scope_cache", {})
    key = id(scope)
    if key not in cache:
        m: Dict[str, Tuple[str, str]] = {}
        for n in ast.walk(scope):
            if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name) \
                    and n.value.func.id in mod.from_imports:
                for tg in n.targets:
                    if isinstance(tg, ast.Name) and tg.id not in m:
                        m[tg.id] = mod.from_imports[n.value.func.id]
        cache[key] = m
    return cache[key]


def _calls_in(mod: Module) -> List[Tuple[int, str, str]]:
    out = []
    for node in ast.walk(mod.tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        # executor.submit(関数, 引数…): 渡した関数の呼び出しとみなす（EPS の段階から。引数は1つずらして解決する）
        if isinstance(f, ast.Attribute) and f.attr == "submit" and node.args and isinstance(node.args[0], ast.Name):
            fn = node.args[0].id
            if fn in mod.funcs:
                out.append((node.lineno, mod.rel, fn))
            elif fn in mod.from_imports:
                out.append((node.lineno, *mod.from_imports[fn]))
        if isinstance(f, ast.Name):
            if f.id in mod.from_imports:
                src_rel, name = mod.from_imports[f.id]
                out.append((node.lineno, src_rel, name))
            elif f.id in mod.funcs:
                out.append((node.lineno, mod.rel, f.id))
            elif f.id in mod.classes:   # 同じモジュールのクラスの呼び出し（インスタンスを作る）
                out.append((node.lineno, mod.rel, f.id))
        elif isinstance(f, ast.Attribute):
            if isinstance(f.value, ast.Name) and f.value.id in mod.mod_imports:
                out.append((node.lineno, mod.mod_imports[f.value.id], f.attr))
            elif isinstance(f.value, ast.Name) and f.value.id in mod.from_imports:   # クラスの静的な呼び出し等
                src_rel, name = mod.from_imports[f.value.id]
                out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
            elif isinstance(f.value, ast.Call) and isinstance(f.value.func, ast.Name) and f.value.func.id in mod.from_imports:
                src_rel, name = mod.from_imports[f.value.func.id]               # SECReader().get_x()
                out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
            elif isinstance(f.value, ast.Name):
                # 変数に入れたインスタンス（r = SECReader(); r.get_x()）
                func = mod.enclosing_func(node)
                scope = func if func is not None else mod.tree
                inst = _scope_instances(mod, scope).get(f.value.id)
                if inst is not None:
                    src_rel, name = inst
                    out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
            elif isinstance(f.value, ast.Attribute) and isinstance(f.value.value, ast.Name) and f.value.value.id == "self":
                # self.reader.get_x(): クラスの中で self.reader = SECReader() としているもの
                cls = mod.enclosing_class(node)
                if cls is not None:
                    for n in ast.walk(cls):
                        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Attribute) and t.attr == f.value.attr for t in n.targets) \
                                and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name) and n.value.func.id in mod.from_imports:
                            src_rel, name = mod.from_imports[n.value.func.id]
                            out.append((node.lineno, src_rel, f"{name}.{f.attr}"))
                            break
            if isinstance(f.value, ast.Name) and f.value.id in ("self", "cls"):
                cls = mod.enclosing_class(node)
                if cls is not None and any(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == f.attr for n in cls.body):
                    out.append((node.lineno, mod.rel, f"{cls.name}.{f.attr}"))
                    continue
            # ほかで解決できないメソッドの呼び出し: リポジトリで1か所だけに定義された名前（8文字以上）ならそれとみなす
            if not any(ln == node.lineno and nm.split(".")[-1] == f.attr for ln, _r, nm in out):
                defs = DEF_INDEX.get(f.attr, [])
                if len(defs) > 1:   # 属性の呼び出しなので、メソッドの定義が1つならそれ
                    defs = [d for d in defs if "." in d[1]]
                if len(defs) == 1 and len(f.attr) >= 8:
                    out.append((node.lineno, defs[0][0], defs[0][1]))
    return out


# ---------------------------------------------------------------- 本体

_PREPARED: Dict[str, object] = {}


def prepare(force: bool = False) -> Tuple[List[str], List[str], Dict[str, str]]:
    """リポジトリのコードを読んで、パースと呼び出しの索引を作る。段階に関係しないので、同じプロセスでは1回だけ作る
    （report_consistency_check.py が SEC 層と TANUKI を続けて作るとき）。force で作り直す。"""
    if _PREPARED and not force:
        return _PREPARED["files"], _PREPARED["py_files"], _PREPARED["texts"]
    for g in (ALL_PY, MODULES, CALLERS, DEF_INDEX, CALL_FUNC, TOP_DIRS, _MENTIONS, _JS_CONSTS, WORKFLOW_COPIES, _CODE_LINES, _CODE_TEXT):   # 作り直すときに積み増さない
        g.clear()
    files = code_files()
    TOP_DIRS.update(f.split("/")[0] for f in git_ls_files() if "/" in f)
    ALL_PY.update(f for f in git_ls_files("*.py"))
    py_files = [f for f in files if f.endswith(".py")]
    texts = {f: read_text(f) for f in files}
    WORKFLOW_COPIES.extend(_workflow_copies(texts))
    for f in py_files:
        load_module(f)
    for rel in py_files:
        mod = MODULES.get(rel)
        if mod is not None and not is_test(rel):
            for n in ast.walk(mod.tree):
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    DEF_INDEX[n.name].append((rel, mod.qualname(n)))
    for rel in py_files:
        mod = MODULES.get(rel)
        if mod is not None:
            for line, src_rel, name in calls_in(mod):
                CALLERS[(src_rel, name)].append((rel, line))
                call = next(iter(mod.calls_by_line.get(line, [])), None)
                CALL_FUNC[(rel, line)] = mod.qualname(call) if call is not None else "<module>"
    _PREPARED.update(files=files, py_files=py_files, texts=texts)
    return files, py_files, texts


def build() -> dict:
    files, py_files, texts = prepare()
    texts = dict(texts)   # 段階ごとに足すページの文字列で、共有の辞書を汚さない
    layer_py = [f for f in py_files if in_layer(f) and not is_test(f)]

    # 1. 書き手
    outputs: Dict[str, dict] = {}
    unresolved_writes = []
    for rel in layer_py:
        mod = MODULES.get(rel)
        if mod is None:
            continue
        writes, _ = io_sites(mod)
        for w in writes:
            # パスがプレースホルダーだけ（{path} 等、呼び出し元の変数そのまま）のものも決めきれない書き込みとする
            if UNRESOLVED in w["path"] or (w["kind"] == "ファイル" and not w["path"].startswith("${") and not PH_RE.sub("", w["path"]).strip("/.")):
                unresolved_writes.append({"file": rel, "line": w["line"], "func": w["func"], "path": w["path"]})
                continue
            o = outputs.setdefault(w["path"], {"path": w["path"], "kind": w["kind"], "writers": [], "writers_outside_layer": []})
            o["writers"].append({"file": rel, "func": w["func"], "line": w["line"]})
    # common/sec_data/ の外の書き手（止める条件）
    outside_writes = []
    for rel in py_files:
        if in_layer(rel) or is_test(rel):
            continue
        mod = MODULES.get(rel)
        if mod is None or OUTSIDE_HINT not in mod.src:
            continue
        writes, _ = io_sites(mod)
        for w in writes:
            if w["path"].startswith(OUTSIDE_PREFIXES):
                outside_writes.append({"file": rel, "line": w["line"], "func": w["func"], "path": w["path"]})
                for key, o in outputs.items():
                    if template_regex(key).match(w["path"]) or key == w["path"]:
                        o["writers_outside_layer"].append({"file": rel, "func": w["func"], "line": w["line"]})

    tracked = git_ls_files()
    workflows = [f for f in tracked if f.startswith(".github/workflows/") and f.endswith((".yml", ".yaml"))]
    # 同じファイル名（型）が、この出力のフォルダの外にもあるか（history.json 等。読み手をパスの全体で照合するため）
    AMBIGUOUS_BASES.clear()
    for key in outputs:
        if key.startswith("${"):
            continue
        base = posixpath.basename(key)
        brx = template_regex(base)
        drx = template_regex(posixpath.dirname(key)) if posixpath.dirname(key) else None
        others = {posixpath.dirname(f) for f in tracked if brx.match(posixpath.basename(f))
                  and not (drx is not None and drx.match(posixpath.dirname(f)))}
        AMBIGUOUS_BASES[base] = AMBIGUOUS_BASES.get(base, False) or bool(others)

    # 書き手の関数の範囲（自分の読み書きを読み手から除く）
    def func_range(rel: str, qual: str) -> Tuple[int, int]:
        mod = MODULES[rel]
        for n in ast.walk(mod.tree):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and mod.qualname(n) == qual:
                return n.lineno, n.end_lineno or n.lineno
        return 0, 0

    # Python の読み込みの場所（全ファイル）
    py_reads: Dict[str, List[dict]] = {}
    for rel in py_files:
        mod = MODULES.get(rel)
        if mod is None:
            continue
        _, reads = io_sites(mod)
        py_reads[rel] = reads
    reads_into_layer = [(rel, r) for rel, rs in py_reads.items() for r in rs if r["path"].startswith(READ_PREFIXES) or UNRESOLVED in r["path"]]
    # ファイル名の grep で当たり、行のパスを解決した場所（読み手・別のファイル・決めきれない）と、決めきれないもの（止める条件）
    mention_sites: Set[Tuple[str, int]] = set()
    undetermined_mentions: Dict[Tuple[str, int], dict] = {}

    # 起動元（ワークフロー）: 書き手のファイル、または書き手の関数を呼ぶファイルを参照するワークフロー
    def workflows_running(rel: str) -> List[str]:
        dotted = module_rel_to_dotted(rel)
        return sorted(w for w in workflows if rel in texts.get(w, "") or dotted in texts.get(w, ""))

    callers_index = CALLERS

    def importers_of(rel: str) -> List[str]:
        out = set()
        for other in py_files:
            mod = MODULES.get(other)
            if mod is None or other == rel:
                continue
            if rel in mod.mod_imports.values() or any(v[0] == rel for v in mod.from_imports.values()):
                out.add(other)
        return sorted(out)

    importers_cache: Dict[str, List[str]] = {}

    def importers(rel: str) -> List[str]:
        if rel not in importers_cache:
            importers_cache[rel] = importers_of(rel)
        return importers_cache[rel]

    # docs/ に書く Python のファイル → docs/ の出力の型
    docs_writes_cache: Dict[str, List[str]] = {}

    def docs_writes(rel: str) -> List[str]:
        if rel not in docs_writes_cache:
            mod = MODULES.get(rel)
            ws = io_sites(mod)[0] if mod is not None else []
            docs_writes_cache[rel] = sorted({w["path"] for w in ws if w["path"].startswith("docs/") and UNRESOLVED not in w["path"]})
        return docs_writes_cache[rel]

    pages = [f for f in tracked if f.startswith("docs/") and f.endswith((".html", ".js")) and not f.startswith("docs/architecture/")]

    def pages_reading(tmpl: str) -> List[str]:
        pats, need_dir = needles_for(tmpl)
        only_ext = not PH_RE.sub("", re.sub(r"\.\w+$", "", posixpath.basename(tmpl)))
        hits = []
        for p in pages:
            t = texts.get(p) or read_text(p)
            texts[p] = t
            if only_ext and need_dir and need_dir not in t:
                continue
            if not any(rx.search(t) for rx in pats):   # 行で当たるなら全体でも当たる（全体で当たらないファイルを先に飛ばす）
                continue
            if any(any(rx.search(line) for rx in pats) and mention_class(tmpl, pats, p, i, line, t) == "reader"
                   for i, line in enumerate(t.splitlines(), 1)):
                hits.append(p)
        return hits

    fd_text = read_text(FIELD_DEFINITIONS)
    fd_rows = [line for line in fd_text.splitlines() if re.match(r"^\|\s*AS-IS-\d+", line)]

    for key, o in sorted(outputs.items()):
        o["writers"].sort(key=lambda w: (w["file"], w["line"]))
        # 起動元: 書き手の関数から呼び出し元を5段までたどり、そのファイルを参照するワークフロー
        launched: Set[str] = set()
        frontier_f = [(w["file"], w["func"]) for w in o["writers"]]
        seen_f: Set[Tuple[str, str]] = set()
        for _ in range(6):
            nxt_f = []
            for (fr, fq) in frontier_f:
                if (fr, fq) in seen_f or is_test(fr):
                    continue
                seen_f.add((fr, fq))
                if fq == "<module>":
                    continue
                for (c_rel, c_line) in callers_index.get((fr, fq), []):
                    nxt_f.append((c_rel, CALL_FUNC.get((c_rel, c_line), "<module>")))
            frontier_f = nxt_f
        launched.update(wf for (fr, fq) in seen_f if not is_test(fr) for wf in workflows_running(fr))
        o["launched_by"] = sorted(launched)
        if o["kind"] != "ファイル" or key.startswith("${"):
            o.update({"files_tracked": None, "top_keys": [], "readers": [], "readers_compute": [], "screens": [], "definitions": []})
            if key.startswith("${") and key != "${GITHUB_STEP_SUMMARY}":
                o["kind"] = "一時ファイル"
                o["files_tracked"] = None
            else:
                o["readers"] = [{"file": "（人）", "line": 0, "type": "人", "how": "画面・通知を見る", "via": ""}]
                o["decision_effect"] = []
                continue
        else:
            rx = template_regex(key)
            matched = sorted(f for f in tracked if rx.match(f))
            o["files_tracked"] = len(matched)
            keys: Dict[str, int] = {}
            for f in (matched if FULL_SCAN else [matched[0], matched[len(matched) // 2], matched[-1]] if matched else []):
                try:
                    with open(os.path.join(REPO, f), encoding="utf-8") as fh:
                        d = json.load(fh)
                except (OSError, ValueError):
                    continue
                if isinstance(d, dict):
                    for k in d:
                        keys[k] = keys.get(k, 0) + 1
            o["top_keys_sampled_files"] = len(matched) if FULL_SCAN else min(3, len(matched))
            o["top_keys"] = [{"key": k} for k in sorted(keys)]

        # 2. 読み手（保存済みファイル）
        pats, need_dir = needles_for(key)
        own = {(w["file"], w["func"]) for w in o["writers"]}
        own_ranges = [(w["file"], *func_range(w["file"], w["func"])) for w in o["writers"] if w["func"] != "<module>"]
        readers: Dict[Tuple[str, int], dict] = {}
        tmpl_rx = template_regex(key) if not key.startswith("${") else None

        def in_own(rel: str, line: int) -> bool:
            return any(rel == f and a <= line <= b for f, a, b in own_ranges)

        for rel, rs in py_reads.items():
            for r_ in rs:
                p = r_["path"]
                if (tmpl_rx is not None and path_matches(key, p)) or p == key:
                    if not in_own(rel, r_["line"]):
                        readers[(rel, r_["line"])] = {"file": rel, "line": r_["line"], "func": r_["func"], "how": "保存済みファイル", "via": "AST"}
        # ファイル名の grep で当たった行は、行のパスを解決して出力のパスの型と全体で照合する（同じ名前の別のフォルダのファイルを除く）。
        # ファイル名がプレースホルダーだけの型（{T}.json 等）は、フォルダの語のあるファイルだけを見る
        only_ext = not PH_RE.sub("", re.sub(r"\.\w+$", "", posixpath.basename(key)))
        for rel in files:
            t = texts.get(rel, "")
            if not t or (only_ext and need_dir and need_dir not in t) or not any(rx.search(t) for rx in pats):
                continue
            mod = MODULES.get(rel) if rel.endswith(".py") else None
            for i, line in enumerate(t.splitlines(), 1):
                if mod is not None and i in mod.skip_lines:
                    continue
                s = line.strip()
                if s.startswith(("//", "<!--", "#", "*", "/*")) and not rel.endswith(".py"):
                    continue
                if any(rx.search(line) for rx in pats):
                    if in_own(rel, i):
                        continue
                    mc = mention_class(key, pats, rel, i, line, t)
                    if mc == "undetermined" and not is_test(rel):
                        undetermined_mentions.setdefault((rel, i), {"file": rel, "line": i, "output": key, "text": s[:120]})
                    if mc != "no":
                        mention_sites.add((rel, i))
                    if mc != "reader":
                        continue
                    func = ""
                    if mod is not None:
                        node = next((n for n in ast.walk(mod.tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                                     and n.lineno <= i <= (n.end_lineno or n.lineno) and not any(
                                         isinstance(c, (ast.FunctionDef, ast.AsyncFunctionDef)) and c.lineno <= i <= (c.end_lineno or c.lineno)
                                         for c in ast.iter_child_nodes(n))), None)
                        func = mod.qualname(node) if node is not None else "<module>"
                    readers.setdefault((rel, i), {"file": rel, "line": i, "func": func, "how": "保存済みファイル", "via": "grep"})
        # 経由（3段まで）: 読み手の関数を呼ぶ箇所、それを呼ぶ箇所…
        frontier_r = [(r_["file"], r_["func"], f"{r_['file']}::{r_['func']}") for r_ in readers.values()
                      if r_["file"].endswith(".py") and r_["func"] and r_["func"] != "<module>"]
        seen_r: Set[Tuple[str, str]] = set()
        for _ in range(VIA_DEPTH):
            nxt_r = []
            for (fr, fq, origin) in frontier_r:
                if (fr, fq) in seen_r:
                    continue
                seen_r.add((fr, fq))
                for (c_rel, c_line) in callers_index.get((fr, fq), []):
                    if in_own(c_rel, c_line):
                        continue
                    cq = CALL_FUNC.get((c_rel, c_line), "<module>")
                    readers.setdefault((c_rel, c_line), {"file": c_rel, "line": c_line, "func": cq, "how": "保存済みファイル", "via": origin})
                    if cq != "<module>":
                        nxt_r.append((c_rel, cq, origin))
            frontier_r = nxt_r
        # 3. その場で計算: 書き手の関数に渡す値を作る関数
        producers: Set[Tuple[str, str]] = set()
        for w in o["writers"]:
            wname = w["func"].split(".")[-1]
            for (c_rel, c_line) in callers_index.get((w["file"], wname), []):
                mod = MODULES[c_rel]
                call = next(iter(mod.calls_by_line.get(c_line, [])), None)
                func = mod.enclosing_func(call) if call is not None else None
                if call is None or func is None:
                    continue
                argnames = {a.id for a in call.args if isinstance(a, ast.Name)}
                for n in ast.walk(func):
                    if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and any(isinstance(t, ast.Name) and t.id in argnames for t in n.targets):
                        for (ln, s_rel, name) in calls_in(mod):
                            if ln == n.lineno and in_layer(s_rel):
                                producers.add((s_rel, name))
        compute = {}
        for (p_rel, p_name) in sorted(producers):
            for (c_rel, c_line) in callers_index.get((p_rel, p_name), []):
                mod = MODULES[c_rel]
                call = next(iter(mod.calls_by_line.get(c_line, [])), None)
                caller_func = mod.qualname(call) if call is not None else ""
                if (c_rel, caller_func) in own or any(c_rel == w["file"] for w in o["writers"]) and in_layer(c_rel):
                    # 書き手と同じファイルの中で書き手のために作る呼び出しは除く
                    if any(c_rel == w["file"] for w in o["writers"]):
                        continue
                compute[(c_rel, c_line)] = {"file": c_rel, "line": c_line, "func": caller_func, "how": "その場で計算", "via": f"{p_rel}::{p_name}"}
        # 書き手のパイプライン（common/sec_data の書き手の起動元）の呼び出しは「書く側」なので除く
        writer_callers = {c for w in o["writers"] for (c, _l) in callers_index.get((w["file"], w["func"].split(".")[-1]), [])}
        compute = {k: v for k, v in compute.items() if v["file"] not in writer_callers}

        def finish(lst: Iterable[dict]) -> List[dict]:
            out = []
            for r_ in sorted(lst, key=lambda x: (x["file"], x["line"])):
                r_["type"] = reader_type(r_["file"])
                out.append(r_)
            return out

        o["readers"] = finish(readers.values())
        o["readers_compute"] = finish(compute.values())
        o["producers"] = [f"{a}::{b}" for a, b in sorted(producers)]

        # 4. 画面
        screens = {p: "直接" for p in pages_reading(key)} if not key.startswith("${") else {}
        # 経由: 読み手（呼び出しを3段までたどった箇所を含む）の Python のファイルが docs/ に書くファイルを読むページ
        for rel in sorted({r_["file"] for r_ in o["readers"] + o["readers_compute"] if r_["type"] == "Python"}):
            for dw in docs_writes(rel):
                for p in pages_reading(dw):
                    if p.endswith(".html"):
                        screens.setdefault(p, f"経由（{rel} → {dw}）")
        o["screens"] = [{"page": p, "how": h} for p, h in sorted(screens.items())]

        # 5. 定義
        ids = []
        for row in fd_rows:
            if (need_dir is None or need_dir in row) and any(rx.search(row) for rx in pats):
                m = re.match(r"^\|\s*(AS-IS-\d+)", row)
                if m:
                    ids.append(m.group(1))
        o["definitions"] = sorted(set(ids))

        # 最上位のキーの読み手（読み手のファイルに、キーが文字列か属性で出てくる数）
        reader_files = sorted({r_["file"] for r_ in o["readers"] + o["readers_compute"]})
        for k in o["top_keys"]:
            krx = re.compile(r"[\"'`]" + re.escape(k["key"]) + r"[\"'`]|\." + re.escape(k["key"]) + r"\b")
            k["reader_files"] = sum(1 for f in reader_files if krx.search(texts.get(f, "")))

        # 投資判断に効くか（読み手のパスの前方一致）
        eff = []
        for prefix, label in DECISION_PREFIXES:
            if any(r_["file"].startswith(prefix) and r_["type"] != "テスト" for r_ in o["readers"] + o["readers_compute"]):
                eff.append(label)
        o["decision_effect"] = eff

    # 読み手の数と並びの区分
    for o in outputs.values():
        prod = {r_["file"] for r_ in o.get("readers", []) if r_["type"] not in ("テスト",)}
        tests = {r_["file"] for r_ in o.get("readers", []) if r_["type"] == "テスト"}
        comp = {r_["file"] for r_ in o.get("readers_compute", []) if r_["type"] != "テスト"}
        o["reader_count"] = len(prod)
        o["reader_count_test"] = len(tests)
        o["reader_count_compute"] = len(comp)
        if o["kind"] in ("Step Summary", "通知"):
            o["group"] = "人が見る"
        elif not prod and tests:
            o["group"] = "テストだけ"
        elif not prod:
            o["group"] = "読み手0件"
        elif len(prod) == 1:
            o["group"] = "1件"
        else:
            o["group"] = "2件以上"
        o["note"] = ANNOTATIONS.get(o["path"], "")
        o["decision"] = DECISIONS.get(o["path"], DECISION_DEFAULT)

    # 6. 規則
    rules_out = []
    for rule in RULES:
        sites = []
        for rel in files:
            if is_test(rel) or not rel.endswith((".py", ".js", ".html")):
                continue
            if rule.get("context") and not re.search(rule["context"], texts.get(rel, "")):
                continue
            if not any(re.search(p, texts.get(rel, ""), re.M) for p in rule["patterns"]):   # 行で当たるなら全体でも当たる（^・$ は行ごと）
                continue
            mod = MODULES.get(rel) if rel.endswith(".py") else None
            for i, line in enumerate(texts.get(rel, "").splitlines(), 1):
                if mod is not None and i in mod.skip_lines:
                    continue
                if any(re.search(p, line) for p in rule["patterns"]):
                    sites.append({"file": rel, "line": i, "text": line.strip()[:120]})
        files_hit = sorted({s["file"] for s in sites})
        rules_out.append({"id": rule["id"], "name": rule["name"], "patterns": rule["patterns"], "sites": sites,
                          "files": files_hit, "in_layer": any(in_layer(f) for f in files_hit),
                          "duplicated": len(files_hit) >= 2})

    # 7. 本番外のコード
    nonprod = []
    prod_code = [f for f in files if f.endswith(".py") and not is_test(f)]
    test_code = [f for f in files if is_test(f)]
    basename_count: Dict[str, int] = defaultdict(int)
    for f in ALL_PY:
        basename_count[posixpath.basename(f)] += 1

    def code_text(f: str) -> str:
        if f not in _CODE_TEXT:
            _CODE_TEXT[f] = "\n".join(code_lines(f))
        return _CODE_TEXT[f]

    def code_lines(f: str) -> List[str]:
        """コメント・docstring を除いた行（ファイルごとに1回だけ作る）。"""
        if f not in _CODE_LINES:
            _CODE_LINES[f] = _code_lines(f)
        return _CODE_LINES[f]

    def _code_lines(f: str) -> List[str]:
        mod = MODULES.get(f) if f.endswith(".py") else None
        out = []
        for i, line in enumerate(texts.get(f, "").splitlines(), 1):
            if mod is not None and i in mod.skip_lines:
                continue
            if f.endswith((".yml", ".yaml")) and line.lstrip().startswith("#"):
                continue
            out.append(line)
        return out
    pairs = [(d, rel) for d in NON_PRODUCTION_DIRS for rel in sorted(git_ls_files(f"{d}/*.py", f"{d}/**/*.py"))]
    if NON_PRODUCTION_FILES or NONPROD_SCAN:
        # 段階のフォルダの .py をすべて見て、名前を挙げたものと、どこからも呼ばれないものを候補にする
        layer_files = git_ls_files(*[f"{d}/*.py" for d in LAYER_DIRS], *[f"{d}/**/*.py" for d in LAYER_DIRS])
        pairs += [(posixpath.dirname(rel), rel) for rel in sorted(set(NON_PRODUCTION_FILES) | set(layer_files))]
    for d, rel in pairs:
        if is_test(rel) or "/tests/" in rel:
            continue
        stem = posixpath.splitext(posixpath.basename(rel))[0]
        base = posixpath.basename(rel)
        # 参照の式: リポジトリからのパス・モジュール名・親フォルダ付きのファイル名。ファイル名だけはリポジトリで1つのときに限る
        alts = [re.escape(rel), re.escape(module_rel_to_dotted(rel)), re.escape(posixpath.join(posixpath.basename(posixpath.dirname(rel)), base))]
        if basename_count[base] == 1:
            alts.append(r"(?<![\w-])" + re.escape(base))
        name_rx = re.compile("|".join(alts))
        is_py = rel.endswith(".py")
        same_dir_import = re.compile(r"^\s*(from|import)\s+" + re.escape(stem) + r"\b" if is_py else r"(?!x)x")
        # フォルダを sys.path・working-directory に足して名前だけで読む使い方: フォルダの語があり、ファイル名・import・python <ファイル名> があるもの
        dir_token = posixpath.basename(d)
        loose_rx = re.compile(r"[\"'/]" + re.escape(base) + r"[\"']|python3?\s+" + re.escape(base) + r"\b"
                              + (r"|^\s*(from|import)\s+" + re.escape(stem) + r"\b" if is_py else ""))
        loose_rx_m = re.compile(loose_rx.pattern, re.M)   # 行で当たるなら、行をつないだ文字列でも当たる（先に飛ばすだけ）
        wf_dir_rx = re.compile(r"working-directory:\s*" + re.escape(posixpath.dirname(rel)) + r"\s*$", re.M)

        def refs(pool: List[str]) -> List[str]:
            out = []
            for f in pool:
                if f == rel:
                    continue
                lines = code_lines(f)
                if name_rx.search(code_text(f)):
                    out.append(f)
                    continue
                mod = MODULES.get(f) if f.endswith(".py") else None
                if mod is not None and (rel in mod.mod_imports.values() or any(v[0] == rel for v in mod.from_imports.values())):
                    out.append(f)
                elif posixpath.dirname(f) == posixpath.dirname(rel) and any(same_dir_import.search(line) for line in lines):
                    out.append(f)
                elif f.endswith((".yml", ".yaml")):
                    if wf_dir_rx.search(texts.get(f, "")) and any(re.search(r"python3?\s+" + re.escape(base) + r"\b", line) for line in lines):
                        out.append(f)
                elif dir_token in texts.get(f, "") and loose_rx_m.search(code_text(f)) and any(loose_rx.search(line) for line in lines):
                    out.append(f)
            return sorted(set(out))
        nonprod.append({
            "dir": d, "file": rel, "decision": DECISIONS.get(rel, DECISION_DEFAULT), "lines": texts.get(rel, read_text(rel)).count("\n"),
            "workflows": refs(workflows), "code": [f for f in refs(prod_code) if not f.startswith(d + "/")],
            "same_dir": [f for f in refs(prod_code) if f.startswith(d + "/")], "tests": refs(test_code),
        })
    nonprod_scanned = [dict(x) for x in nonprod] if NONPROD_SCAN else []
    if NON_PRODUCTION_FILES or NONPROD_SCAN:
        nonprod = [x for x in nonprod if x["file"] in NON_PRODUCTION_FILES
                   or (not x["workflows"] and not x["code"] and not x["same_dir"] and not x["file"].endswith("__init__.py"))]
        for x in nonprod:
            x["found_by"] = "名前を挙げた候補" if x["file"] in NON_PRODUCTION_FILES else "どこからも呼ばれない"

    # F3 の6種の場所
    f3 = []
    for name in F3_NAMES:
        hits = []
        for rel in files:
            if is_test(rel) or not rel.endswith((".py", ".js", ".html")):
                continue
            if name not in texts.get(rel, ""):
                continue
            mod = MODULES.get(rel) if rel.endswith(".py") else None
            for i, line in enumerate(texts.get(rel, "").splitlines(), 1):
                if (mod is None or i not in mod.skip_lines) and name in line:
                    hits.append({"file": rel, "line": i})
        f3.append({"name": name, "in_layer": any(in_layer(h["file"]) for h in hits),
                   "output": next((k for k in outputs if name in k), None), "sites": hits})

    # 止める条件
    n_out = len(outputs)
    # 読み方を決めきれない読み込み: パスに <?> が残り、パスに sec_data を含むか、SEC層のモジュールの中のもの（テストを除く）
    # ただし、根だけが決まらず末尾で出力に照合できたもの、ファイル名が決まっていてこの段階のどの出力とも違うものは除く
    # 出力のファイル名の型。ファイル名が全部プレースホルダー（{T}.json 等）なら親のフォルダ名と組で比べる
    def _name_part(k: str) -> str:
        base = posixpath.basename(k)
        if not re.sub(r"\.\w+$", "", PH_RE.sub("", base)):
            return "/".join(k.split("/")[-2:])
        return base
    out_names = [(_name_part(k).count("/"), template_regex(_name_part(k))) for k in outputs if not k.startswith("${")]

    def other_file(path: str) -> bool:
        base = posixpath.basename(path)
        if not base or UNRESOLVED in base:
            return False
        for n_slash, rx in out_names:
            part = "/".join(path.split("/")[-(n_slash + 1):])
            if UNRESOLVED not in part and rx.match(part):
                return False
        return True

    def tail_resolved(path: str) -> bool:
        return any(tail_match(k, path) for k in outputs if not k.startswith("${"))
    undetermined_reads = [{"file": rel, "line": r_["line"], "func": r_["func"], "path": r_["path"]} for rel, r_ in reads_into_layer
                          if UNRESOLVED in r_["path"] and (READ_HINT in r_["path"] or in_layer(rel)) and not is_test(rel)
                          and not tail_resolved(r_["path"]) and not other_file(r_["path"])]
    resolved_reads = [1 for rel, r_ in reads_into_layer if (UNRESOLVED not in r_["path"] or tail_resolved(r_["path"])) and not is_test(rel)]
    # ファイル名の grep で当たった行（テストを除く）: 決めきれないものは、上の決めきれない読み込みと同じ数え方で比に入れる
    und_mentions = sorted(undetermined_mentions.values(), key=lambda x: (x["file"], x["line"]))
    und_keys = {(x["file"], x["line"]) for x in und_mentions}
    n_mention_resolved = sum(1 for k in mention_sites if k not in und_keys and not is_test(k[0]))
    outputs_with_outside_writer = [k for k, o in outputs.items() if o["writers_outside_layer"]]
    stop = {
        "outputs": n_out,
        "outputs_with_writer_outside_layer": outputs_with_outside_writer,
        "unresolved_write_sites": unresolved_writes,
        "python_read_sites_into_layer_resolved": len(resolved_reads),
        "python_read_sites_unresolved_in_files_mentioning_sec_data": undetermined_reads,
        "name_hit_sites_resolved": n_mention_resolved,
        "name_hit_sites_undetermined": und_mentions,
        "ratio_writer": round((len(outputs_with_outside_writer) + len(unresolved_writes)) / max(1, n_out + len(unresolved_writes)), 3),
        "ratio_reader": round((len(undetermined_reads) + len(und_mentions))
                              / max(1, len(undetermined_reads) + len(resolved_reads) + len(und_mentions) + n_mention_resolved), 3),
        "threshold": STOP_RATIO,
    }
    stop["hit"] = stop["ratio_writer"] > STOP_RATIO or stop["ratio_reader"] > STOP_RATIO

    result = {
        "generated_by": "scripts/inventory/build_output_inventory.py",
        "layer": LAYER,
        "stage": "段階1（一覧の下書き。CHECK としてはまだ動かさない）",
        "outputs": [outputs[k] for k in sorted(outputs, key=lambda k: (["読み手0件", "テストだけ", "1件", "2件以上", "人が見る"].index(outputs[k]["group"]), k))],
        "rules": rules_out,
        "non_production_code": nonprod,
        **({"non_production_scanned": nonprod_scanned} if NONPROD_SCAN else {}),
        "f3_zero_reader": f3,
        "deleted": [{"what": a, "detail": b} for a, b in DELETED],
        "stop_condition": stop,
    }
    if SCREEN_PAGES:
        writer_files = {w["file"] for o in outputs.values() for w in o["writers"]}
        result["screen"] = screen_items(outputs, texts, tracked, writer_files)
        if SCREEN_SECTIONS:
            result["screen"]["sections"] = screen_sections(result["screen"], texts)
    if HTML_COPIES:
        result["html_copies"] = html_copies(texts)
    if PAGE_CHECKS:
        result["page_checks"] = page_checks(tracked, texts, py_files)
    if WORKFLOW_SUMMARY:
        result["workflow_outputs"] = workflow_outputs(outputs, tracked, texts, layer_py)
    return result


# ---------------------------------------------------------------- 画面の区画・複製・範囲外のページ・ワークフローの出力（HypeCore の段階から）

COMMENT_RE = re.compile(r"^\s*<!--\s*(.+?)\s*-->\s*$")
ID_RE = re.compile(r"\bid=[\"']([\w-]+)[\"']")
JS_ID_RE = re.compile(r"getElementById\(\s*['\"]([\w-]+)['\"]\s*\)|querySelector(?:All)?\(\s*['\"]#([\w-]+)['\"]")
JS_FUNC_RE = re.compile(r"^\s*(?:async\s+)?function\s+(\w+)\s*\(|^\s*(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s*)?(?:\([^)]*\)|\w+)\s*=>")
NEAR_LINES = 15


def _html_sections(raw: List[str]) -> Tuple[List[dict], Dict[str, int]]:
    """<body> の中（<script> の外）を <!-- 見出し --> の行で区切った区画と、id → 区画の番号。"""
    sections = [{"title": "（ページの先頭）", "start": 1, "ids": []}]
    id_sec: Dict[str, int] = {}
    in_script = False
    for n, line in enumerate(raw, 1):
        st = line.strip()
        if re.search(r"<script\b", st):
            in_script = "</script>" not in st
            continue
        if in_script:
            in_script = "</script>" not in st
            continue
        m = COMMENT_RE.match(line)
        if m:
            sections.append({"title": m.group(1).strip(), "start": n, "ids": []})
            continue
        for i in ID_RE.findall(line):
            sections[-1]["ids"].append(i)
            id_sec.setdefault(i, len(sections) - 1)
    return sections, id_sec


def _js_functions(raw: List[str]) -> List[Tuple[str, int, int]]:
    """JS の関数の (名前, 始まりの行, 終わりの行)。終わりは波かっこの数で決める（文字列の中の波かっこは数えてしまう。下書き）。"""
    out = []
    for n, line in enumerate(raw, 1):
        m = JS_FUNC_RE.match(line)
        if not m:
            continue
        name = m.group(1) or m.group(2)
        depth, started, end = 0, False, n
        for k in range(n - 1, len(raw)):
            depth += raw[k].count("{") - raw[k].count("}")
            started = started or "{" in raw[k]
            if started and depth <= 0:
                end = k + 1
                break
        out.append((name, n, end))
    return out


def screen_sections(sc: dict, texts: Dict[str, str]) -> List[dict]:
    """画面の区画ごとに、画面が読むキーと書き手の有無。キーを読む JS の行から、同じ関数の中で近くに出てくる要素の id
    （getElementById・querySelector('#…')）を探し、その id がある HTML の区画に入れる。関数の中に id が無ければ、その関数を
    呼ぶ行で同じことをする（2段まで）。決められないものは「（区画を決められない）」。"""
    out: List[dict] = []
    for pg in sc["pages"]:
        if not pg.endswith(".html"):
            continue
        raw = (texts.get(pg) or read_text(pg)).splitlines()
        sections, id_sec = _html_sections(raw)
        funcs = _js_functions(raw)
        id_lines = [(n, a or b) for n, line in enumerate(raw, 1) for a, b in JS_ID_RE.findall(line)]

        def enclosing(line: int) -> Optional[Tuple[str, int, int]]:
            c = [f for f in funcs if f[1] <= line <= f[2]]
            return min(c, key=lambda f: f[2] - f[1]) if c else None

        def ids_for(line: int, depth: int = 0) -> List[str]:
            f = enclosing(line)
            lo, hi = (f[1], f[2]) if f else (1, len(raw))
            inside = [(n, i) for n, i in id_lines if lo <= n <= hi]
            after = [(n, i) for n, i in inside if line <= n <= line + NEAR_LINES]
            before = [(n, i) for n, i in inside if line - NEAR_LINES <= n < line]
            if after:
                return [after[0][1]]
            if before:
                return [before[-1][1]]
            if inside:
                return [min(inside, key=lambda x: abs(x[0] - line))[1]]
            if f and depth < 2:
                call_rx = re.compile(r"(?<![\w.])" + re.escape(f[0]) + r"\s*\(")
                got: List[str] = []
                for n, l in enumerate(raw, 1):
                    if not (f[1] <= n <= f[2]) and call_rx.search(l) and not JS_FUNC_RE.match(l):
                        got += ids_for(n, depth + 1)
                return sorted(set(got))
            return []

        by_sec: Dict[str, Dict[str, dict]] = defaultdict(dict)
        for it in [i for i in sc["items"] if i["page"] == pg]:
            for ln in it["lines"]:
                ids = ids_for(ln)
                titles = sorted({sections[id_sec[i]]["title"] if i in id_sec else f"（JSで作る要素 #{i}）" for i in ids}) or ["（区画を決められない）"]
                for t in titles:
                    e = by_sec[t].setdefault(it["key"], {"key": it["key"], "status": it["status"], "lines": []})
                    e["lines"].append(ln)
        order = {s_["title"]: k for k, s_ in enumerate(sections)}
        for t in sorted(by_sec, key=lambda t: (order.get(t, 999), t)):
            out.append({"page": pg, "section": t, "keys": [by_sec[t][k] for k in sorted(by_sec[t])]})
    return out


def _last_commit_date(rel: str) -> str:
    try:
        return subprocess.run(["git", "log", "-1", "--format=%ad", "--date=short", "--", rel], cwd=REPO,
                              capture_output=True, text=True, encoding="utf-8").stdout.strip()
    except OSError:
        return ""


def html_copies(texts: Dict[str, str]) -> List[dict]:
    """本番外の HTML の複製の候補と、docs/ のページの近さ（difflib の一致率）・最後の commit の日。"""
    import difflib
    out = []
    for src, targets in HTML_COPIES:
        st = texts.get(src) or read_text(src)
        rows = []
        for t in targets:
            tt = texts.get(t) or read_text(t)
            a = [x.strip() for x in st.splitlines() if x.strip()]
            b = [x.strip() for x in tt.splitlines() if x.strip()]
            sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
            same = sum(m.size for m in sm.get_matching_blocks())
            rows.append({"page": t, "lines": tt.count("\n"), "same_lines": same, "share_of_copy": round(same / max(1, len(a)), 3),
                         "last_commit": _last_commit_date(t)})
        out.append({"file": src, "lines": st.count("\n"), "last_commit": _last_commit_date(src), "pages": rows})
    return out


def page_checks(tracked: List[str], texts: Dict[str, str], py_files: List[str]) -> List[dict]:
    """範囲外のページの確認: タイトル・読む JSON とその書き手（.py に JSON のファイル名とフォルダの語があるもの）・
    ページへのリンク（docs/ の HTML・JS にフォルダのパスが出てくるもの）・最後の commit の日。"""
    out = []
    for pg in PAGE_CHECKS:
        t = texts.get(pg) or read_text(pg)
        title = (re.search(r"<title>(.*?)</title>", t) or [None, ""])[1]
        consts = dict(re.findall(r"(?:const|let|var)\s+(\w+)\s*=\s*(?:\w+\s*=>\s*)?['\"`]([^'\"`]+\.json)['\"`]", t))
        lits = sorted(set(consts.values()) | set(re.findall(r"fetch\(\s*['\"`]([^'\"`]+\.json)", t)))
        reads = []
        for lit in lits:
            tail = posixpath.normpath(posixpath.join(posixpath.dirname(pg), lit.split("?")[0]))
            tmpl = re.sub(r"\$\{\w+\}", "{T}", tail)
            rx = template_regex(tmpl)
            files = [f for f in tracked if rx.match(f)]
            writers = sorted({f"{rel}:{w['func']}" for rel in py_files if not is_test(rel) and MODULES.get(rel) is not None
                              for w in io_sites(MODULES[rel])[0] if rx.match(w["path"]) or w["path"] == tmpl or tail_match(tmpl, w["path"])})
            reads.append({"literal": lit, "path": tmpl, "files": len(files), "writers": writers})
        page_dir = posixpath.dirname(pg).replace("docs/", "")
        links = sorted(f for f in tracked if f.endswith((".html", ".js")) and f != pg and not f.startswith("docs/architecture/")
                       and page_dir in (texts.get(f) or read_text(f)))
        item = {"page": pg, "title": title, "reads": reads, "linked_from": links, "last_commit": _last_commit_date(pg)}
        if "/contents/${" in t:
            item.update(_page_api_io(t, tracked, py_files))
        out.append(item)
    return out


def _page_api_io(t: str, tracked: List[str], py_files: List[str]) -> dict:
    """GitHub の API（contents）でリポジトリのファイルを読み書きするページ: パスはリポジトリの根から。
    読み: fetchFile(…)。書き: commitFile(…)・{ path: …, content: … }（複数ファイルの1commit）。ほかに起動するワークフロー。"""
    consts = dict(re.findall(r"(?:const|let|var)\s+(\w+)\s*=\s*['\"`]([^'\"`]+)['\"`]", t))

    def path_of(arg: str) -> Optional[str]:
        arg = arg.strip()
        if arg in consts:
            arg = consts[arg]
        elif arg[:1] in "'\"`":
            arg = arg.strip("'\"`")
            arg = re.sub(r"\$\{(\w+)\}", lambda m: consts.get(m.group(1), "{T}" if _JS_TICKER_RE.search(m.group(1)) else "{x}"), arg)
        else:
            return None
        return arg if arg.split("/")[0] in TOP_DIRS else None

    def sites(rx: str) -> Dict[str, List[int]]:
        got: Dict[str, List[int]] = defaultdict(list)
        for n, line in enumerate(t.splitlines(), 1):
            if re.search(r"\bfunction\s+(fetchFile|commitFile)\b", line):   # 関数の定義の行
                continue
            for m in re.finditer(rx, line):
                pth = path_of(m.group(1))
                if pth:
                    got[pth].append(n)
        return got

    arg = r"(\w+|'[^']*'|\"[^\"]*\"|`[^`]*`)"
    reads = sites(r"fetchFile\(\s*" + arg)
    writes = sites(r"commitFile\(\s*" + arg)
    for k, v in sites(r"\{\s*path:\s*" + arg + r"\s*,\s*content:").items():
        writes[k].extend(v)
    # 関数の中で const path = '…' を決めて、contents の API を直接呼ぶもの（GET は読み、method: 'PUT' は書き）
    tl = t.splitlines()
    for n, line in enumerate(tl, 1):
        m = re.search(r"const\s+path\s*=\s*['\"`]([^'\"`]+)['\"`]", line)
        if not m or m.group(1).split("/")[0] not in TOP_DIRS:
            continue
        end = next((k for k in range(n, min(len(tl), n + 80)) if re.match(r"^(async\s+)?function\b", tl[k])), min(len(tl), n + 80))
        for k in range(n, end):
            if "/contents/${path}" in tl[k]:
                put = any("method: 'PUT'" in x or 'method: "PUT"' in x for x in tl[k:k + 4])
                (writes if put else reads)[m.group(1)].append(k + 1)

    def row(pth: str, lines: List[int]) -> dict:
        rx = template_regex(pth)
        files = [f for f in tracked if rx.match(f)]
        writers = sorted({f"{rel}:{w['func']}" for rel in py_files if not is_test(rel) and MODULES.get(rel) is not None
                          for w in io_sites(MODULES[rel])[0] if rx.match(w["path"]) or w["path"] == pth or tail_match(pth, w["path"])})
        return {"path": pth, "lines": sorted(set(lines)), "files": len(files), "py_writers": writers}

    wfs = sorted(set(re.findall(r"dispatchWorkflow\(\s*['\"]([\w.-]+\.ya?ml)['\"]", t)))
    return {"api_reads": [row(k, v) for k, v in sorted(reads.items())],
            "api_writes": [row(k, v) for k, v in sorted(writes.items())],
            "dispatches": wfs}


def workflow_outputs(outputs: Dict[str, dict], tracked: List[str], texts: Dict[str, str], layer_py: List[str]) -> List[dict]:
    """段階の .py を動かすワークフローが、自分の中で書く Step Summary（.py の外）と、それが .get() で読むキーが段階の JSON にあるか。"""
    keys: Set[str] = set()
    for k, o in outputs.items():
        if o["kind"] != "ファイル" or not k.endswith(".json"):
            continue
        rx = template_regex(k)
        for f in tracked:
            if rx.match(f):
                try:
                    with open(os.path.join(REPO, f), encoding="utf-8") as fh:
                        json_keys(json.load(fh), out=keys)
                except (OSError, ValueError):
                    pass
    out = []
    for wf in sorted(f for f in tracked if f.startswith(".github/workflows/") and f.endswith((".yml", ".yaml"))):
        t = texts.get(wf) or read_text(wf)
        if "GITHUB_STEP_SUMMARY" not in t or not any(rel in t for rel in layer_py):
            continue
        got = sorted(k for k in set(re.findall(r"\.get\(\s*['\"](\w+)['\"]", t)) if not k.isupper())
        lines = [n for n, line in enumerate(t.splitlines(), 1) if "GITHUB_STEP_SUMMARY" in line]
        out.append({"workflow": wf, "lines": lines, "keys": [{"key": k, "in_json": k in keys} for k in got]})
    return out


# ---------------------------------------------------------------- 画面の項目（TANUKI）

SCREEN_SAMPLE = 12   # 画面の項目の表で、キーを集める JSON のファイルの数（型ごと）
JP_RE = re.compile(r"[぀-ヿ一-鿿]")
SNAKE_RE = re.compile(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)+$")
# プロパティの読み: 式の後ろの .key / ?.key（直前が識別子・閉じ括弧のときだけ。'.css_class' 等の文字列を除く）
PROP_RE = re.compile(r"(?<=[\w\)\]])\??\.([A-Za-z_][A-Za-z0-9_]*)")
SUBS_RE = re.compile(r"\[\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]\s*\]")


def json_keys(obj, depth: int = 0, max_depth: int = 8, out: Optional[Set[str]] = None) -> Set[str]:
    """JSON の dict のキーを入れ子まで（max_depth 段まで）集める。リストの中の dict も見る。"""
    out = set() if out is None else out
    if depth > max_depth:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str):
                out.add(k)
            json_keys(v, depth + 1, max_depth, out)
    elif isinstance(obj, list):
        for v in (obj[:50] + obj[-50:] if len(obj) > 100 else obj):
            json_keys(v, depth + 1, max_depth, out)
    return out


def sample_files(files: List[str], n: int = SCREEN_SAMPLE) -> List[str]:
    """200ファイルまでは全部、それより多ければ等間隔に n 個（銘柄ごとにキーが違うため、銘柄の数の分は全部見る）。"""
    if len(files) <= 200 or FULL_SCAN:
        return files
    return [files[round(i * (len(files) - 1) / (n - 1))] for i in range(n)]


def page_code_lines(rel: str, text: str) -> List[Tuple[int, str]]:
    """JS の行（.js は全部、.html は <script> の中だけ）からコメントの行を除いた (行, 文字列)。"""
    out, in_script = [], rel.endswith(".js")
    for i, line in enumerate(text.splitlines(), 1):
        s = line.strip()
        if rel.endswith(".html"):
            if re.search(r"<script\b(?![^>]*\bsrc=)", s):
                in_script = True
                s = s.split(">", 1)[-1] if ">" in s else ""
            if "</script>" in s:
                in_script = False
                continue
            if not in_script:
                continue
        if s.startswith(("//", "<!--", "*", "/*")):
            continue
        out.append((i, line))
    return out


def nearest_label(lines: List[str], idx: int) -> str:
    """その行と前の3行から、日本語を含む見出し・文字列（推定の表示位置）を1つ取る。"""
    for j in range(idx, max(-1, idx - 4), -1):
        line = lines[j]
        cands = re.findall(r">([^<>]*?)<", line) + re.findall(r"['\"`]([^'\"`]{1,60})['\"`]", line)
        for c in cands:
            c = re.sub(r"\$\{[^}]*\}", "", c).strip()
            if JP_RE.search(c):
                return c[:40]
    return ""


def screen_items(outputs: Dict[str, dict], texts: Dict[str, str], tracked: List[str], writer_files: Set[str]) -> dict:
    """画面（SCREEN_PAGES の HTML・JS）が読むキーと、書き手の有無。書き手があるのに誰も読まないキー。"""
    pages = sorted(f for f in tracked if f.startswith(tuple(d + "/" for d in SCREEN_PAGES))
                   and f.endswith((".html", ".js")) and "/data/" not in f)
    # 書き手のある JSON のキー（この段階の docs/ の出力。各型から3ファイルまで）
    written: Dict[str, Set[str]] = defaultdict(set)
    written_parents: Dict[str, Set[str]] = defaultdict(set)

    def _parents(obj, parent: str = "", depth: int = 0) -> None:
        if depth > 8:
            return
        if isinstance(obj, dict):
            for kk, vv in obj.items():
                if isinstance(kk, str):
                    written_parents[kk].add(parent)
                _parents(vv, kk if isinstance(kk, str) else parent, depth + 1)
        elif isinstance(obj, list):
            for vv in (obj[:50] + obj[-50:] if len(obj) > 100 else obj):
                _parents(vv, parent, depth + 1)
    for k, o in outputs.items():
        if o["kind"] != "ファイル" or not k.endswith(".json") or not k.startswith("docs/"):
            continue
        rx = template_regex(k)
        matched = sorted(f for f in tracked if rx.match(f))
        for f in sample_files(matched):
            try:
                with open(os.path.join(REPO, f), encoding="utf-8") as fh:
                    d = json.load(fh)
            except (OSError, ValueError):
                continue
            for key in json_keys(d):
                written[key].add(k)
            _parents(d)
    docs_json = [f for f in tracked if f.startswith("docs/") and f.endswith(".json")]
    items = []
    for pg in pages:
        text = texts.get(pg) or read_text(pg)
        raw = text.splitlines()
        # ページが読む JSON（.json の文字列の末尾で、docs/ の JSON と照合。各3ファイルまで）のキー
        fetched: Dict[str, Set[str]] = defaultdict(set)
        for lit in sorted(set(re.findall(r"['\"`]([^'\"`\s]*?\.json)(?:\$\{[^}]*\}|\?[^'\"`\s]*)?['\"`]", text))):
            tail = lit.split("}")[-1].lstrip("/").lstrip("./")
            if lit.split("}")[-1].startswith("."):
                # ファイル名がプレースホルダーだけ（rotce/${ticker}.json 等）: 末尾のフォルダからの型で照合する
                tmpl = re.sub(r"^(\.\.?/)+", "", re.sub(r"\$\{[^}]*\}", "{x}", lit))
                if "/" not in tmpl:
                    continue
                trx = re.compile(r"(^|/)" + re.escape(tmpl).replace(r"\{x\}", "[^/]+") + "$")
                cands = sorted(f for f in docs_json if trx.search(f))
            elif not tail:
                continue
            else:
                cands = sorted(f for f in docs_json if f.endswith(tail if tail.startswith(("_", "-")) else "/" + tail) or f == tail)
            for f in sample_files(cands):
                try:
                    with open(os.path.join(REPO, f), encoding="utf-8") as fh:
                        d = json.load(fh)
                except (OSError, ValueError):
                    continue
                for key in json_keys(d):
                    fetched[key].add(f)
        # ページの中で作る値: オブジェクトのキー（key: ・{ key, } の省略形）、宣言・代入（const key = ・obj.key = ）
        local_keys = set(re.findall(r"[{,]\s*([A-Za-z_][A-Za-z0-9_]*)\s*:(?!:)", text))
        local_keys |= set(re.findall(r"\b(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=", text))
        local_keys |= set(re.findall(r"\.([A-Za-z_][A-Za-z0-9_]*)\s*=(?!=)", text))
        local_keys |= set(re.findall(r"[{,]\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?=[,}])", text))
        seen: Dict[str, dict] = {}
        for i, line in page_code_lines(pg, text):
            names = [m.group(1) for m in PROP_RE.finditer(line)] + [m.group(1) for m in SUBS_RE.finditer(line)]
            for nm in names:
                if nm not in written and nm not in fetched and not SNAKE_RE.match(nm):
                    continue
                if nm in written:
                    status = "この段階の書き手あり"
                elif nm in fetched:
                    status = "ほかの書き手あり"
                elif nm in local_keys:
                    status = "画面の中で作る値"
                else:
                    status = "書き手なし"
                it = seen.setdefault(nm, {"page": pg, "key": nm, "has_writer": nm in written or nm in fetched, "status": status,
                                          "sources": sorted(written.get(nm, [])) or sorted(fetched.get(nm, []))[:3],
                                          "label": "", "lines": []})
                it["lines"].append(i)
                if not it["label"]:
                    it["label"] = nearest_label(raw, i - 1)
        # 区画ごとの判断: キーを読む行がすべてその区画の中にあれば、その区画の判断
        sections = []
        for spg, start_rx, end_rx, decision in SCREEN_SECTION_DECISIONS:
            if spg != pg:
                continue
            start = next((n for n, ln in enumerate(raw, 1) if re.search(start_rx, ln)), None)
            if start is None:
                continue
            end = next((n for n, ln in enumerate(raw, 1) if n > start and re.search(end_rx, ln)), len(raw) + 1)
            sections.append((start, end, decision))
        for k in sorted(seen):
            it = seen[k]
            it["decision"] = next((d for a, b, d in sections if all(a <= x < b for x in it["lines"])), DECISION_DEFAULT)
            items.append(it)
    # 書き手があるのに誰も読まないキー: テストを除くコード（.py・.js・.html。書き手のファイルを含む）に読みの形で出てこず、
    # 画面がそのキーの親を列挙（Object.keys/entries/values・for in）してもいないもの
    readers_pool = [f for f in texts if f.endswith((".py", ".js", ".html")) and not is_test(f)]
    big = "\n".join(texts[f] for f in readers_pool)
    read_names = set(re.findall(r"(?<=[\w\)\]])\??\.([A-Za-z_]\w*)\b", big))
    read_names |= set(re.findall(r"\[\s*['\"]([A-Za-z_]\w*)['\"]\s*\](?!\s*=[^=])", big))
    read_names |= set(re.findall(r"\.get\(\s*['\"]([A-Za-z_]\w*)['\"]", big))
    page_text = "\n".join(texts.get(pg) or read_text(pg) for pg in pages)
    parents = written_parents
    unread = []
    for key in sorted(written):
        # 銘柄名・日付などの値のキー（小文字を含まないもの）は除く
        if len(key) < 3 or not re.match(r"[a-z_]\w*$", key):
            continue
        if key in read_names:
            continue
        loop_par = [par for par in sorted(parents.get(key, set())) if par and re.search(
            r"Object\.(?:keys|entries|values)\(\s*[\w.?\[\]'\"]*\b" + re.escape(par) + r"\b|for\s*\(\s*(?:const|let|var)\s+\w+\s+in\s+[\w.?]*\b" + re.escape(par) + r"\b", page_text)]
        if loop_par:
            continue
        unread.append({"key": key, "sources": sorted(written[key]), "parents": sorted(p for p in parents.get(key, set()) if p),
                       "note": UNREAD_KEY_NOTES.get(key, "")})
    return {"pages": pages, "items": items, "unread_keys": unread, "written_key_count": len(written)}


# ---------------------------------------------------------------- CHECK（report_consistency_check.py の CHECK-INV-1〜4 が使う）

def check_inventory(saved: dict, now: dict, known_rules: Optional[List[str]] = None,
                    out_json: Optional[str] = None, tag: str = "") -> Tuple[List[str], List[str]]:
    """保存した一覧（sec_layer.json・tanuki_layer.json）と、今のリポジトリから作った一覧を比べる。(NG, WARN) の文の一覧を返す。
    銘柄ごとではなく全体の判定。known_rules・out_json を省くと、今の段階の設定を使う。"""
    known_rules = KNOWN_DUPLICATE_RULES if known_rules is None else known_rules
    out_json = OUT_JSON if out_json is None else out_json
    tg = f"{tag} " if tag else ""
    ng: List[str] = []
    warn: List[str] = []
    saved_out = {o["path"]: o for o in saved.get("outputs", [])}
    now_out = {o["path"]: o for o in now.get("outputs", [])}
    # CHECK-INV-1: 一覧に無い出力
    for path in sorted(set(now_out) - set(saved_out)):
        w = "、".join(f"{x['file']}:{x['func']}" for x in now_out[path].get("writers", []))
        ng.append(f"  [NG-INV-1 {tg}一覧に無い出力] {path}（書き手 {w}）。{out_json} に入れる（スクリプトで作り直す）か、出力をやめる")
    # CHECK-INV-2: 一覧で読み手とされているファイルが読んでいない／読み手0件の出力
    for path in sorted(set(saved_out) & set(now_out)):
        before = {r["file"] for r in saved_out[path].get("readers", []) + saved_out[path].get("readers_compute", [])}
        after = {r["file"] for r in now_out[path].get("readers", []) + now_out[path].get("readers_compute", [])}
        for f in sorted(before - after):
            ng.append(f"  [NG-INV-2 {tg}読み手が読んでいない] {path} の読み手 {f} が、今はこの出力を読んでいない。一覧を作り直すか、読み手を確かめる")
    for path, o in sorted(now_out.items()):
        if o.get("group") == "読み手0件":
            ng.append(f"  [NG-INV-2 {tg}読み手0件の出力] {path}。読み手を作るか、出力をやめる（[[OUTPUT-INVENTORY-CHECK-1]]）")
    # CHECK-INV-3: 規則の二重（その段階のファイルを含むもの）。既知は WARN、それ以外は NG
    for r in now.get("rules", []):
        if not (r.get("duplicated") and r.get("in_layer")):
            continue
        where = "、".join(r["files"])
        if r["id"] in known_rules:
            warn.append(f"  [WARN-INV-3 {tg}規則の二重（既知）] {r['name']}: {len(r['files'])}ファイル（{where}）")
        else:
            ng.append(f"  [NG-INV-3 {tg}規則の二重（新規）] {r['name']}: {len(r['files'])}ファイル（{where}）。直し方はチャット側が決める")
    # CHECK-INV-4: 「何か」の注記が無い出力
    for path, o in sorted(now_out.items()):
        if not o.get("note"):
            warn.append(f"  [WARN-INV-4 {tg}注記なし] {path}。scripts/inventory/build_output_inventory.py の ANNOTATIONS に1文を足す")
    # 画面の項目（TANUKI）: 画面が読むのに書き手の無いキーが新しくできたら NG、書き手があるのに誰も読まないキーが新しくできたら WARN
    # INV-5・6 は抜き取った JSON の中身で結果が変わる（条件のときだけ書くキーが全銘柄で無くなると、コードを変えなくても出る）。
    # 本番のワークフロー（GITHUB_ACTIONS）で公開を止めないよう、INV-5 は本番では WARN、手元のゲートでは NG のまま
    inv5_warn = os.environ.get("GITHUB_ACTIONS") == "true"
    if now.get("screen") is not None:
        s_saved = saved.get("screen") or {}
        known_nw = {(i["page"], i["key"]) for i in s_saved.get("items", []) if i.get("status") == "書き手なし"}
        for i in now["screen"].get("items", []):
            if i.get("status") == "書き手なし" and (i["page"], i["key"]) not in known_nw:
                msg = (f"{tg}画面のキーに書き手なし] {i['page']} の {i['key']}（行 {'・'.join(str(x) for x in i['lines'][:4])}）。"
                       "書き手を作るか、表示をやめる")
                if inv5_warn:
                    warn.append(f"  [WARN-INV-5 {msg}（本番のワークフローでは WARN。手元のゲートでは NG）")
                else:
                    ng.append(f"  [NG-INV-5 {msg}")
        known_ur = {u["key"] for u in s_saved.get("unread_keys", [])}
        for u in now["screen"].get("unread_keys", []):
            if u["key"] not in known_ur:
                warn.append(f"  [WARN-INV-6 {tg}誰も読まないキー] {u['key']}（{'、'.join(u['sources'][:2])}）。読み手を作るか、書くのをやめる")
    return ng, warn


CHECK_LAYERS = ("sec", "tanuki", "hypecore")   # CHECK-INV を当てる段階（段階3が済んだもの）
_RUN_CHECK_CACHE: Dict[str, Tuple[List[str], List[str]]] = {}


def run_check(layers: Tuple[str, ...] = CHECK_LAYERS) -> Tuple[List[str], List[str]]:
    """今のリポジトリで各段階の一覧を作り、保存した一覧と比べる。保存した一覧が無ければ NG。
    一覧を作るのに時間がかかるため、同じプロセスでは段階ごとに1回だけ作る（report_consistency_check.py を何度呼んでも同じ）。"""
    ng: List[str] = []
    warn: List[str] = []
    for layer in layers:
        if layer not in _RUN_CHECK_CACHE:
            _RUN_CHECK_CACHE[layer] = _run_check(layer)
        ng += _RUN_CHECK_CACHE[layer][0]
        warn += _RUN_CHECK_CACHE[layer][1]
    return ng, warn


def _run_check(layer: str) -> Tuple[List[str], List[str]]:
    set_layer(layer)
    tag = NAME_SHORT
    try:
        with open(os.path.join(REPO, OUT_JSON), encoding="utf-8") as f:
            saved = json.load(f)
    except (OSError, ValueError) as e:
        return [f"  [NG-INV-1 {tag} 一覧が読めない] {OUT_JSON}: {e}"], []
    return check_inventory(saved, build(), tag=tag)


# ---------------------------------------------------------------- Markdown（Koichi さんが読む表）

def md_escape(s: str) -> str:
    return s.replace("|", "\\|")


def render_md(inv: dict) -> str:
    L = []
    outs = inv["outputs"]
    files = [o for o in outs if o["group"] != "人が見る"]
    L.append(f"# {TITLE}")
    L.append("")
    L.append("[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。"
             f"機械が読む形は`{posixpath.basename(OUT_JSON)}`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。")
    L.append("")
    L.append("- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数")
    L.append("- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）")
    L.append("- 投資判断: 読み手（その場で計算を含む、テストを除く）に" + "・".join(f"`{p}`" for p, _ in DECISION_PREFIXES) + "があれば「効く」")
    L.append(f"- 残す／消す: {DECISION_NOTE}")
    L.append("")
    st = inv["stop_condition"]
    n_zero = sum(1 for o in files if o["group"] == "読み手0件")
    n_test = sum(1 for o in files if o["group"] == "テストだけ")
    n_dup = sum(1 for r in inv["rules"] if r["duplicated"])
    n_dup_layer = sum(1 for r in inv["rules"] if r["duplicated"] and r["in_layer"])
    L.append(f"**件数**: 出力 {len(outs)}（ファイル {len(files)}・人が見るもの {len(outs) - len(files)}）／読み手0件 {n_zero}／テストだけ {n_test}／"
             f"1件 {sum(1 for o in files if o['group'] == '1件')}／2件以上 {sum(1 for o in files if o['group'] == '2件以上')}／規則の二重 {n_dup}（検出式 {len(inv['rules'])}種のうち。{NAME_SHORT}のファイルを含むもの {n_dup_layer}）")
    if inv.get("screen"):
        sc = inv["screen"]
        by = defaultdict(int)
        for it in sc["items"]:
            by[it["status"]] += 1
        L.append("")
        L.append(f"**画面の項目**: 画面が読むキー {len(sc['items'])}（この段階の書き手あり {by['この段階の書き手あり']}・ほかの書き手あり {by['ほかの書き手あり']}・"
                 f"画面の中で作る値 {by['画面の中で作る値']}・**書き手なし {by['書き手なし']}**）／書き手があるのに誰も読まないキー **{len(sc['unread_keys'])}**")
    L.append("")
    L.append(f"**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない {len(st['outputs_with_writer_outside_layer'])}＋{len(st['unresolved_write_sites'])}件（{st['ratio_writer']:.1%}）、"
             f"読み方を決めきれない読み込み {len(st['python_read_sites_unresolved_in_files_mentioning_sec_data'])}件＋ファイル名で当たった行 {len(st.get('name_hit_sites_undetermined', []))}件"
             f"／解決できた読み込み {st['python_read_sites_into_layer_resolved']}件＋ファイル名で当たった行 {st.get('name_hit_sites_resolved', 0)}件（{st['ratio_reader']:.1%}）→ "
             + ("**当たる**" if st["hit"] else "当たらない"))
    L.append("")
    L.append("読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・"
             "ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。"
             "ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする")
    if st.get("name_hit_sites_undetermined"):
        L.append("")
        L.append("フォルダを決めきれない行（ファイル名で当たった行）:")
        for x in st["name_hit_sites_undetermined"]:
            L.append(f"- `{x['file']}` 行 {x['line']}（`{x['output']}`）: `{md_escape(x['text'])}`")
    L.append("")
    if inv["deleted"]:
        L.append("## 消したもの（段階2、" + ("2026-10-21" if NAME_SHORT in ("HypeCore", "EPS") else "2026-10-20") + "）")
        L.append("")
        for x in inv["deleted"]:
            L.append(f"- `{x['what']}`: {x['detail']}")
        L.append("")
    L.append("## 出力（読み手0件 → テストだけ → 1件 → 2件以上）")
    L.append("")
    L.append("| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    for o in files:
        writers = "<br>".join(f"`{strip_layer(w['file'])}:{w['func']}`" for w in o["writers"])
        if o.get("writers_outside_layer"):
            writers += "<br>外: " + "<br>".join(f"`{w['file']}:{w['func']}`" for w in o["writers_outside_layer"])
        screens = "<br>".join(f"{s['page'].replace('docs/', '')}（{'直接' if s['how'] == '直接' else '経由'}）" for s in o["screens"]) or "—"
        rc = f"{o['reader_count']}"
        if o["reader_count_test"]:
            rc += f"・テスト{o['reader_count_test']}"
        if o["reader_count_compute"]:
            rc += f"（{o['reader_count_compute']}）"
        eff = "効く: " + "・".join(o["decision_effect"]) if o["decision_effect"] else "—"
        defs = "・".join(o["definitions"]) or "—"
        files_n = f"<br>（{o['files_tracked']}ファイル）" if o.get("files_tracked") is not None else ""
        L.append(f"| {o['group']} | `{md_escape(o['path'])}`{files_n} | {md_escape(o['note'])} | {writers} | {rc} | {screens} | {md_escape(eff)} | {defs} | {md_escape(o['decision'])} |")
    L.append("")
    L.append("### 人が見るもの（Step Summary・Discord）")
    L.append("")
    L.append("| 出力 | 何か | 書き手 | 起動元のワークフロー |")
    L.append("|---|---|---|---|")
    for o in outs:
        if o["group"] != "人が見る":
            continue
        writers = "<br>".join(f"`{strip_layer(w['file'])}:{w['func']}`" for w in o["writers"])
        L.append(f"| `{md_escape(o['path'])}` | {md_escape(o['note'])} | {writers} | {'<br>'.join(o['launched_by']) or '—'} |")
    L.append("")
    L.append("## 読み手の内訳")
    L.append("")
    for o in files:
        L.append(f"### `{o['path']}`（{o['group']}）")
        if o["launched_by"]:
            L.append(f"- 起動元: {'・'.join(o['launched_by'])}")
        else:
            L.append("- 起動元: ワークフローから書き手を呼ぶ参照が見つからない")
        if o["top_keys"]:
            ks = "・".join(f"`{k['key']}`({k['reader_files']})" for k in o["top_keys"][:40])
            more = f" ほか{len(o['top_keys']) - 40}" if len(o["top_keys"]) > 40 else ""
            L.append(f"- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: {ks}{more}")
        groups: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
        for r in o["readers"] + o["readers_compute"]:
            groups[(r["type"], r["how"], r["file"])].append(r)
        order = {"Python": 0, "JS・HTML": 1, "ワークフロー": 2, "テスト": 3}
        for (typ, how, f), rs in sorted(groups.items(), key=lambda kv: (order.get(kv[0][0], 9), kv[0][1], kv[0][2])):
            lines = "・".join(str(r["line"]) for r in rs[:12]) + (f" ほか{len(rs) - 12}" if len(rs) > 12 else "")
            vias = sorted({r["via"] for r in rs if r["via"] not in ("AST", "grep")})
            via = f"（経由 {'、'.join('`' + v + '`' for v in vias[:3])}{' ほか' if len(vias) > 3 else ''}）" if vias else ""
            L.append(f"- {typ}・{how}: `{f}` 行 {lines}{via}")
        for s in o["screens"]:
            if s["how"] != "直接":
                L.append(f"- 画面（{s['how']}）: `{s['page']}`")
        if not o["readers"] and not o["readers_compute"]:
            L.append("- 読み手なし")
        L.append("")
    if inv.get("screen"):
        sc = inv["screen"]
        L.append("## 画面の項目")
        L.append("")
        scope = "全ファイル" if FULL_SCAN else f"型ごとに等間隔で{SCREEN_SAMPLE}ファイル"
        L.append("画面（" + "・".join(f"`{p}`" for p in sc["pages"]) + "）のJSが読むキー（`.key`・`?.key`・`['key']`。"
                 "この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（"
                 f"{scope}、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。"
                 "画面の言葉は、その行と前の3行の日本語の文字列（推定）。")
        L.append("")
        L.append("### 書き手なし（画面が読むのに、どのJSONにも無いキー）")
        L.append("")
        for it in [i for i in sc["items"] if i["status"] == "書き手なし"]:
            L.append(f"- `{it['key']}`（`{it['page']}` 行 {'・'.join(str(x) for x in it['lines'][:6])}）{(': ' + md_escape(it['label'])) if it['label'] else ''}")
        L.append("")
        L.append("### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）")
        L.append("")
        if not sc["unread_keys"]:
            L.append("なし")
        for u in sc["unread_keys"]:
            L.append(f"- `{u['key']}`（{'、'.join('`' + s + '`' for s in u['sources'][:2])}）{(': ' + md_escape(u['note'])) if u.get('note') else ''}")
        L.append("")
        for pg in sc["pages"]:
            rows = [i for i in sc["items"] if i["page"] == pg]
            L.append(f"### `{pg}`（{len(rows)}項目）")
            L.append("")
            L.append("| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |")
            L.append("|---|---|---|---|---|")
            for it in rows:
                src = "<br>".join(f"`{s}`" for s in it["sources"][:2])
                L.append(f"| {md_escape(it['label']) or '—'} | `{it['key']}` | {it['status']}{('<br>' + src) if src else ''} | {'・'.join(str(x) for x in it['lines'][:4])} | {md_escape(it.get('decision', ''))} |")
            L.append("")
    if inv.get("screen", {}).get("sections") is not None:
        L.append("## 画面の区画")
        L.append("")
        L.append("HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後"
                 f"{NEAR_LINES}行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた"
                 "（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。")
        L.append("")
        for pg in sorted({x["page"] for x in inv["screen"]["sections"]}):
            L.append(f"### `{pg}`")
            L.append("")
            L.append("| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |")
            L.append("|---|---|---|")
            for x in [x for x in inv["screen"]["sections"] if x["page"] == pg]:
                ks = "<br>".join(f"`{k['key']}`（{k['status']}）" for k in x["keys"])
                L.append(f"| {md_escape(x['section'])} | {ks} | {md_escape(SECTION_DECISIONS.get(pg, ''))} |")
            L.append("")
    if inv.get("workflow_outputs"):
        L.append("## ワークフローが自分で書く出力（.pyの外）")
        L.append("")
        L.append("段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。")
        L.append("")
        for w in inv["workflow_outputs"]:
            L.append(f"- `{w['workflow']}`（行 {'・'.join(str(x) for x in w['lines'][:6])}）: "
                     + "・".join(f"`{k['key']}`（{'ある' if k['in_json'] else '**無い**'}）" for k in w["keys"]))
        L.append("")
    if inv.get("html_copies"):
        L.append("## 本番外のHTMLの複製の候補")
        L.append("")
        L.append("| ファイル | 行数 | 最後のcommit | 比べたページ（行数・複製の行のうち同じ行がページにある割合〈空行を除き、前後の空白を無視、difflibの順序つき一致〉・最後のcommit） |")
        L.append("|---|---|---|---|")
        for h in inv["html_copies"]:
            pages = "<br>".join(f"`{p['page']}`（{p['lines']}行・{p['same_lines']}行で{p['share_of_copy']:.0%}・{p['last_commit']}）" for p in h["pages"])
            L.append(f"| `{h['file']}` | {h['lines']} | {h['last_commit']} | {pages} |")
        L.append("")
    if inv.get("page_checks"):
        L.append("## 範囲外の確認（ページ）")
        L.append("")
        for c in inv["page_checks"]:
            L.append(f"### `{c['page']}`（{md_escape(c['title'])}、最後のcommit {c['last_commit']}）")
            L.append("")
            for r in c["reads"]:
                if "api_reads" in c and r["literal"].split("/")[0] in TOP_DIRS:
                    continue   # 下の GitHub の API の読みに出す
                w = "・".join(f"`{x}`" for x in r["writers"]) or "見つからない"
                L.append(f"- 読むJSON `{r['literal']}` → `{r['path']}`（{r['files']}ファイル）。書き手（.pyの書き込みの場所で照合）: {w}")
            if "api_reads" in c:
                L.append("- GitHub の API（contents）でリポジトリのファイルを読み書きするページ（パスはリポジトリの根から。0ファイルは、リポジトリに無いファイル）")
                for label, key in (("読む（fetchFile）", "api_reads"), ("書く（commitFile・複数ファイルの1commit）", "api_writes")):
                    L.append(f"  - {label}:")
                    for r in c[key]:
                        w = "・".join(f"`{x}`" for x in r["py_writers"]) or "なし"
                        L.append(f"    - `{r['path']}`（{r['files']}ファイル、行 {'・'.join(map(str, r['lines']))}）。.pyの書き手: {w}")
                L.append("  - 起動するワークフロー: " + ("・".join(f"`{x}`" for x in c["dispatches"]) or "なし"))
            L.append("- リンク・参照しているファイル: " + ("・".join(f"`{x}`" for x in c["linked_from"]) or "なし"))
            L.append("")
    L.append("## FINDINGS 2026-10-15 の読み手0件の6種（F3）")
    L.append("")
    L.append(f"| 名前 | {NAME_SHORT}か | この一覧の出力 | 書かれている場所（grep） |")
    L.append("|---|---|---|---|")
    for f in inv["f3_zero_reader"]:
        sites = "<br>".join(f"`{s['file']}:{s['line']}`" for s in f["sites"][:6]) + (f"<br>ほか{len(f['sites']) - 6}" if len(f["sites"]) > 6 else "")
        L.append(f"| `{f['name']}` | {'はい' if f['in_layer'] else 'いいえ（後の段階）'} | {('`' + f['output'] + '`') if f['output'] else '—'} | {sites or '—'} |")
    L.append("")
    L.append("## 規則が2か所以上にあるもの")
    L.append("")
    L.append(f"検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。{NAME_SHORT}のファイルを含むものだけでなく全部を示す。")
    L.append("")
    L.append(f"| 規則 | ファイル数 | 場所 | {NAME_SHORT}を含む |")
    L.append("|---|---|---|---|")
    for r in inv["rules"]:
        if not r["duplicated"]:
            continue
        sites = "<br>".join(f"`{s['file']}:{s['line']}`" for s in r["sites"])
        L.append(f"| {r['name']} | {len(r['files'])} | {sites} | {'はい' if r['in_layer'] else 'いいえ'} |")
    single = [r for r in inv["rules"] if not r["duplicated"]]
    if single:
        L.append("")
        L.append("1か所だけのもの: " + "・".join(f"{r['name']}（{len(r['files'])}）" for r in single))
    L.append("")
    np_dirs = NON_PRODUCTION_DIRS or sorted({x["dir"] for x in inv["non_production_code"]})
    L.append("## 本番外のコード（" + "・".join(np_dirs or LAYER_DIRS) + "）")
    L.append("")
    L.append("参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト")
    L.append("")
    if NONPROD_SCAN and not NON_PRODUCTION_FILES:
        L.append("候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）")
        L.append("")
        if not inv["non_production_code"]:
            rows = [x for x in inv.get("non_production_scanned", []) if not x["file"].endswith("__init__.py")]
            L.append(f"候補は0件。見たファイル（{len(rows)}ファイル・{sum(x['lines'] for x in rows):,}行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:")
            L.append("")
            L.append("| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |")
            L.append("|---|---|---|---|---|---|")
            for x in rows:
                fmt = lambda xs: "<br>".join(f"`{v}`" for v in xs[:4]) + (f"<br>ほか{len(xs) - 4}" if len(xs) > 4 else "") or "—"  # noqa: E731
                L.append(f"| `{posixpath.basename(x['file'])}` | {x['lines']:,} | {fmt(x['workflows'])} | {fmt(x['code'])} | {fmt(x['same_dir'])} | {len(x['tests'])}件 |")
            L.append("")
    if NON_PRODUCTION_FILES:
        L.append("候補: 名前を挙げたもの（" + "・".join(f"`{posixpath.basename(f)}`" for f in NON_PRODUCTION_FILES) + "）と、段階のフォルダの.pyのうちワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの")
        L.append("")
    for d in np_dirs:
        rows = [x for x in inv["non_production_code"] if x["dir"] == d]
        total = sum(x["lines"] for x in rows)
        L.append(f"### `{d}`（{len(rows)}ファイル・{total:,}行）")
        L.append("")
        L.append("| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |")
        L.append("|---|---|---|---|---|---|---|")
        for x in rows:
            fmt = lambda xs: "<br>".join(f"`{v}`" for v in xs) or "—"  # noqa: E731
            L.append(f"| `{x['file'].replace(d + '/', '')}` | {x['lines']:,} | {fmt(x['workflows'])} | {fmt(x['code'])} | {fmt(x['same_dir'])} | {fmt(x['tests'])} | {md_escape(x['decision'])} |")
        L.append("")
    L.append("## 止める条件の内訳")
    L.append("")
    for k in ("outputs_with_writer_outside_layer", "unresolved_write_sites", "python_read_sites_unresolved_in_files_mentioning_sec_data"):
        items = st[k]
        L.append(f"- `{k}`: {len(items)}件")
        for it in items[:30]:
            L.append(f"  - {('`' + it + '`') if isinstance(it, str) else '`' + it['file'] + ':' + str(it['line']) + '` ' + it.get('func', '') + ' → `' + it['path'] + '`'}")
    L.append("")
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="書かずに、今のファイルと同じかだけを見る")
    ap.add_argument("--layer", choices=sorted(LAYERS), default="sec", help="段階（sec: SECデータ層、tanuki: TANUKI VALUATION・TANUKI Score、hypecore: HypeCore、eps: adjusted_eps_analyzer）")
    args = ap.parse_args()
    set_layer(args.layer)
    inv = build()
    js = json.dumps(inv, ensure_ascii=False, indent=1) + "\n"
    md = render_md(inv)
    if args.check:
        same = read_text(OUT_JSON) == js and read_text(OUT_MD) == md
        print("一覧は最新" if same else "一覧が古い（スクリプトを実行して作り直す）")
        return 0 if same else 1
    os.makedirs(os.path.join(REPO, OUT_DIR), exist_ok=True)
    with open(os.path.join(REPO, OUT_JSON), "w", encoding="utf-8", newline="\n") as f:
        f.write(js)
    with open(os.path.join(REPO, OUT_MD), "w", encoding="utf-8", newline="\n") as f:
        f.write(md)
    st = inv["stop_condition"]
    print(f"出力 {len(inv['outputs'])}、止める条件 {'当たる' if st['hit'] else '当たらない'}（書き手 {st['ratio_writer']:.1%}・読み方 {st['ratio_reader']:.1%}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
