"""出力の一覧の段階の設定（層ごと）: 対象のフォルダ・注記・残す／消すの判断・消したもの・規則の検出式・既知の二重など。

[[OUTPUT-INVENTORY-CHECK-1]]。解析は inventory_analysis.py、一覧の書き出しは inventory_render.py、入口は build_output_inventory.py
（R-A の終わりの見直し3、2026-10-23に1つのファイルから分けた）。段階を足すときは、ここに設定を足して LAYERS に入れる。
"""
from __future__ import annotations

from typing import Dict, List, Tuple

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
# discover/stonks-silo は 2026-10-21 に src/value/stonks_silo/ へ移した（STONKS SILO の段階の一覧で扱う）
NON_PRODUCTION_DIRS = ["scripts/analysis", "browser_checks"]

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
HTML_COPIES: List[Tuple[str, List[str]]] = []   # 本番外の HTML の複製の候補と、比べる docs/ のページ
PAGE_CHECKS: List[str] = []    # 範囲外の確認をするページ（何のページか・書き手・読み手・ナビ）
SECTION_DECISIONS: Dict[str, str] = {}   # 画面の区画の判断（ページ → 判断。そのページの区画すべてに当てる）
IO_HELPER_BY_CALLER = False   # パスを引数で受けて読み書きする汎用の関数（_load_json・_atomic_write_json 等）を読み手・書き手にせず、その出力のパスを渡す呼び出し箇所をそうする（市場データ層の段階から）
# Pythonの書き手の無いファイルも出力に加えるフォルダ（Portfolio の段階から）。そのフォルダの git にあるファイルのうち、どの出力の型にも当たらないものを
# 出力にし、書き手はワークフローの cp（あれば）、無ければ「書き手なし（人の手で書く）」とする
EXTRA_OUTPUT_DIRS: Tuple[str, ...] = ()
# 本番外のコードの候補ごとの注記（実行の記録〈commit〉等、ファイル → 1文）。空でなければ表に欄を足す（MACRO PULSE の段階から）
NONPROD_NOTES: Dict[str, str] = {}
# 段階ごとの印（解決の厳しさ・キーの全ファイルの走査・画面の区画・ワークフローの Step Summary・本番外のコードの走査・CSV の列・
# getElementById を包んだ関数）は、R-A の終わりの見直し（2026-10-23）で全段階に同じ一番厳しい側の設定にした（印は無い）。
# 画面の無い層だけ、JSON のキー（入れ子まで）のうち誰も読まないものを出す（画面のある層は画面の項目の表で出す）。
# IO_HELPER_BY_CALLER だけは段階ごと（市場データ層・マクロデータ層）: SEC層に広げると self._load_json(path) 形の呼び出しを
# 読み手にできず、SECReader 経由の本当の読み手が消えるため

_SEC = dict(LAYER=LAYER, LAYER_DIRS=LAYER_DIRS, OUT_JSON=OUT_JSON, OUT_MD=OUT_MD, DECISION_PREFIXES=DECISION_PREFIXES,
            NON_PRODUCTION_DIRS=NON_PRODUCTION_DIRS, RULES=RULES, ANNOTATIONS=ANNOTATIONS, DECISION_DEFAULT=DECISION_DEFAULT,
            DECISIONS=DECISIONS, DELETED=DELETED, OUTSIDE_HINT=OUTSIDE_HINT, OUTSIDE_PREFIXES=OUTSIDE_PREFIXES,
            READ_PREFIXES=READ_PREFIXES, READ_HINT=READ_HINT, NAME_SHORT=NAME_SHORT, TITLE=TITLE, DECISION_NOTE=DECISION_NOTE,
            NON_PRODUCTION_FILES=NON_PRODUCTION_FILES, SCREEN_PAGES=SCREEN_PAGES, KNOWN_DUPLICATE_RULES=KNOWN_DUPLICATE_RULES,
            SCREEN_SECTION_DECISIONS=SCREEN_SECTION_DECISIONS, UNREAD_KEY_NOTES=UNREAD_KEY_NOTES,
HTML_COPIES=HTML_COPIES, PAGE_CHECKS=PAGE_CHECKS,
SECTION_DECISIONS=SECTION_DECISIONS, IO_HELPER_BY_CALLER=IO_HELPER_BY_CALLER, EXTRA_OUTPUT_DIRS=EXTRA_OUTPUT_DIRS,NONPROD_NOTES=NONPROD_NOTES)
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

    HTML_COPIES=[],   # 段階1で比べた src/value/hypecore/hypecore.html は段階2で削除
    PAGE_CHECKS=[],   # 段階1で確かめた docs/value-monitor/extreme-fear/ は段階2で削除

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

    HTML_COPIES=[],
    PAGE_CHECKS=[],   # 段階1で確かめた docs/value-monitor/admin.html は段階2で削除

    SECTION_DECISIONS={
        "docs/value-monitor/adjusted_eps_analyzer/stock.html": "残す（2026-10-21、Koichiさんの判断）",
        "docs/value-monitor/adjusted_eps_analyzer/index.html": "残す（2026-10-21、Koichiさんの判断: 一覧の表）",
        "docs/value-monitor/adjusted_eps_analyzer/tickers.html": "残す（2026-10-21）",
    },
)
# ---- STONKS SILO（src/value/stonks_silo、段階1、2026-10-21。段階2で discover/stonks-silo/src から移した）
_STONKS = dict(_SEC)
_STONKS.update(
    LAYER="src/value/stonks_silo",
    LAYER_DIRS=("src/value/stonks_silo",),
    OUT_JSON=f"{OUT_DIR}/stonks_layer.json", OUT_MD=f"{OUT_DIR}/stonks_layer.md",
    # 自分のフォルダの読み手は「効く」に数えない。TANUKI（理論株価・判定）と画面を「効く」とする
    DECISION_PREFIXES=DECISION_PREFIXES + [
        ("docs/value-monitor/stonks-silo/", "STONKS SILOの画面"),
        ("docs/value-monitor/tanuki_valuation/", "TANUKI VALUATIONの画面"),
    ],
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],

    ANNOTATIONS={
        "docs/value-monitor/stonks-silo/data/results.json":
            "赤字・小型の成長株の全銘柄の評価（生存能力〈runway〉・黒字化の道筋・拡大再生産・バリュエーション・総合の判定と点数）と、"
            "四半期・年次の財務の推移（financial_vectors）。STONKS SILOの画面・TANUKI VALUATIONの赤字銘柄の評価とTANUKI Score・rcc・System Healthが読む",
    },
    DECISION_DEFAULT="残す（2026-10-21）",
    DECISIONS={},
    DELETED=[
        ("discover/stonks-silo/src/ の6ファイル",
         "src/value/stonks_silo/ へ移した（2026-10-21、チャット側の判断）。Discover は削除済みのサブシステムで、フォルダの名前と中身が合わなかった（型E）。"
         "根からの深さは同じ3段で、根を階層の数で求めるコードは変えていない。出力 results.json と画面の場所は変えない。"
         "画面の見出しの「Discover · 10x Candidates」も「Value Monitor · Early-Stage Growth」に直した"),
        ("誰も読まないキー10個（results.json への書き込み）",
         "書き込みをやめた（2026-10-21、チャット側の判断）: " + "・".join(('angle', 'composite', 'population_size', 'fields_available', 'fields_missing', 'heatmap_fields', 'financial_vectors_note', 'gross_margin_note', 'ticker_count', 'unit_economics_label')) + "（data_quality はこの3つの入れ物ごと）。"
         "合成ベクトル・角度の計算と使われなくなった定数（COMPOSITE_FIELDS・HEATMAP_FIELDS・VECTOR_FIELDS の weight）も消した。"
         "全24銘柄の results.json は消したキーと時刻のほか前後で同じ、overall_verdict・overall_score も同じ"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-21）。画面の区画はKoichiさんの判断ですべて残す。フォルダの付け替えと誰も読まないキーはチャット側の判断。"
                  "スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="stonks", OUTSIDE_PREFIXES=("docs/value-monitor/stonks-silo/", "src/value/stonks_silo/"),
    READ_PREFIXES=("docs/value-monitor/stonks-silo", "src/value/stonks_silo"),
    READ_HINT="stonks", NAME_SHORT="STONKS",
    KNOWN_DUPLICATE_RULES=["staleness"], TITLE="STONKS SILO（src/value/stonks_silo）の出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/value-monitor/stonks-silo",),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={
        "gross_margin_trend": "残す: 書き手の中の計算（Unit Economics の点数の GM トレンド）に使う",
        "latest_year": "残す: テストが参照する（DeficitQuality・RunwayAnalysis の引数）",
        # 今の出力のファイル（commit 済み）には、次の本番の実行まで残る
        **{k: "書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える" for k in ('angle', 'composite', 'population_size', 'fields_available', 'fields_missing', 'heatmap_fields', 'financial_vectors_note', 'gross_margin_note', 'ticker_count', 'unit_economics_label')},
    },
    RULES=_TANUKI["RULES"],

    HTML_COPIES=[],
    PAGE_CHECKS=[],

    SECTION_DECISIONS={
        "docs/value-monitor/stonks-silo/index.html":
            "残す（2026-10-21、Koichiさんの判断: 一覧の表・買い／回避シグナル・黒字化への道のり・拡大再生産・①〜③・推移のグラフ・財務トレンド・使い方のすべて）",
    },
)
# ---- 市場データ層（common/market_data、段階1、2026-10-22）。SEC層と同じく画面を持たない共有のデータ層
_MARKET = dict(_SEC)
_MARKET.update(
    LAYER="common/market_data",
    LAYER_DIRS=("common/market_data",),
    OUT_JSON=f"{OUT_DIR}/market_data_layer.json", OUT_MD=f"{OUT_DIR}/market_data_layer.md",
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],

    ANNOTATIONS={
        "common/market_data/daily/{T}.json":
            "銘柄・指数・先物・ETFごとの日次の株価（始値・高値・安値・終値・出来高）。暫定の足・Yahooに足の無い日の印を含む。TANUKI・HypeCore・STONKS・Market Pulse・Score Verifier等が読む",
        "common/market_data/attributes/{T}.json":
            "銘柄ごとの週次の属性（yfinanceの.infoのスナップショット: β・時価総額・PER・セクター・アナリストの目標株価・次の決算のEPSの予想等）",
        "common/market_data/analyst_history/{T}.json":
            "銘柄ごとのアナリストのイベント（格付け・目標株価の変更）、EPSの実績と予想、推奨の内訳の推移。"
            "3系統とも追記型の履歴（Yahooが返すのは直近の分だけで、過去の分はこのファイルにしか残らない）",
        "common/market_data/_sp500_constituents_cache.json":
            "S&P500の構成銘柄の一覧の控え（正。calendar_universe.sp500_constituents が7日以内なら取り直さずに使う）。"
            "市場データ層の取得の銘柄の範囲（fetcher）・reader・Market Pulseのブレッス（写しを data/sp500_tickers.json に書く）が使う",
        "docs/market-monitor/market-pulse/data/nyse_holidays.json":
            "前年〜翌年のNYSEの休場日（土日以外、XNYS）。Market Pulse（collect_and_send.py）が毎晩 calendar_universe.write_nyse_holidays で書き、"
            "画面（market-pulse/index.html）が休場の行に使う（2026-10-23に書き手を calendar_universe.py へ移した）",
        "docs/market-monitor/market-pulse/data/sp500_tickers.json":
            "S&P500の構成銘柄の写し（正は common/market_data/_sp500_constituents_cache.json）。Market Pulseの breadth_calculator.py が "
            "calendar_universe.write_sp500_copy で書き、browser_checks/market_pulse_elements.py が読む（2026-10-23に書き手を calendar_universe.py へ移した）",
        "/tmp/daily_status.json": "Market_Data_Daily_Updateの1回の日次取得の結果（status）。同じワークフローの後の段が読む",
    },
    DECISION_DEFAULT="残す（2026-10-22）",
    DECISIONS={},
    DELETED=[
        ("common/market_data/{T}/market_data_violations_log.json（596ファイル）と書き手（fetcher.py の _write_violations_section・_violations_log_path）",
         "削除（2026-10-22、Koichiさんの判断）。読み手0件（FINDINGS 2026-10-15 の F3）。保存前の検証は残し、警告は各レコードの _validation_warnings と標準出力に出す。"
         "ワークフロー2本の git add の行と .gitattributes の行も消した"),
        ("common/market_data/_daily_close_retry_log.json と fetch_daily_prices の追記の処理",
         "削除（2026-10-22、Koichiさんの判断）。読み手0件（書き手が追記のために読み直すだけ）。終値の無い足の取り直し（_retry_missing_closes）と、"
         "Yahooの作り直しの時間帯の判定は残す"),
        ("attributes の next_quarter_eps_date の書き込み",
         "書き込みをやめた（2026-10-22）。読み手0件（TANUKI が読むのは next_quarter_eps_estimate だけ）。前後で daily・attributes・analyst_history は"
         "消したキーのほか同じ（yfinance の応答を記録して再生、11銘柄）、TANUKI の96銘柄の理論株価・判定と HypeCore の98銘柄の stage は前後で同じ"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-22、Koichiさんの判断）。読み手0件の2つ（検証の結果のログ・取り直しの記録）と誰も読まないキー1つを消し、"
                  "analyst_history の8キーは溜まる履歴のため残す。_validation_warnings は品質の印として残す（読み手は後の画面の整理で決める）。"
                  "スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="market_data", OUTSIDE_PREFIXES=("common/market_data/",),
    READ_PREFIXES=("common/market_data",),
    READ_HINT="market_data", NAME_SHORT="市場データ層",
    KNOWN_DUPLICATE_RULES=[], TITLE="市場データ層（common/market_data）の出力の一覧（段階1の下書き）",
    UNREAD_KEY_NOTES={
        **{k: "残す: 溜まる履歴（analyst_history は追記型で、Yahooが返すのは直近の分だけ。過去の分は取り直せない）"
           for k in ("current_price_target", "eps_actual", "eps_difference", "eps_estimate",
                     "price_target_action", "prior_price_target", "strong_buy", "strong_sell")},
        "_validation_warnings": "残す: 保存前の検証の品質の印（読み手は後の画面の整理で決める）",
        "next_quarter_eps_date": "書き込みをやめた（2026-10-22）。今の attributes のファイルからは次の本番の実行（Market Data Weekly）で消える",
    },
    RULES=[
        # 2026-10-23（[[MARKET-CALENDAR-UNIVERSE-UNIFY-1]]）: 3つとも「決める所」に当てる式にした。以前の名前の式は、取引日・暫定の足を
        # 使うだけの関数（fetcher の find_missing_trading_days・rcc の _check_*・Market Pulse の項目の印付け等）も数えていた。
        # 本番外のコード（browser_checks・scripts/analysis）は画面の期待値を独立に作るため数えない
        {"id": "trading_day", "name": "取引日（市場の営業日）の決め方（取引所の暦を直接読む所・手で書いた営業日の計算）",
         "patterns": [r"^\s*(?:import|from)\s+pandas_market_calendars\b", r"def\s+_?\w*(?:business_days?|business_day_add)\w*\s*\(",
                      r"function\s+\w*(?:[Tt]radingDays?|[Bb]usinessDays?|[Mm]arketOpen)\w*\s*\("],
         "exclude": r"^(?:browser_checks|scripts/analysis)/"},
        {"id": "provisional_bar", "name": "暫定の足の判定（日足の確定時刻・暫定かどうか・暫定の種類を決める関数）",
         "patterns": [r"def\s+_?(?:bar_final_at|is_provisional_\w+|is_flat_\w+_bar|\w*provisional_kind\w*)\s*\(",
                      r"function\s+\w*[Pp]rovisional\w*\s*\("],
         "exclude": r"^(?:browser_checks|scripts/analysis)/"},
        {"id": "sp500_constituents", "name": "S&P500の構成銘柄の一覧の取り方（取得元を直接読む所）",
         "patterns": [r"List_of_S%26P_500_companies", r"s-and-p-500-companies"],
         "exclude": r"^(?:browser_checks|scripts/analysis)/"},
    ],

    IO_HELPER_BY_CALLER=True,
)
# ---- TANUKI TAIL（src/tail・docs/portfolio/tail、段階1、2026-10-22）
_TAIL = dict(_SEC)
_TAIL.update(
    LAYER="src/tail",
    LAYER_DIRS=("src/tail",),
    OUT_JSON=f"{OUT_DIR}/tail_layer.json", OUT_MD=f"{OUT_DIR}/tail_layer.md",
    # 自分のフォルダの読み手は「効く」に数えない。TANUKI（理論株価・判定）と画面を「効く」とする
    DECISION_PREFIXES=[x for x in DECISION_PREFIXES if x[0] != "src/tail/"] + [
        ("docs/portfolio/tail/", "TANUKI TAILの画面（保有銘柄の監視）"),
        ("docs/value-monitor/tanuki_valuation/", "TANUKI VALUATIONの画面"),
    ],
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],

    ANNOTATIONS={
        "docs/portfolio/tail/data/positions_index.json":
            "TAILの保有・監視銘柄の一覧（positions）。画面の入力（Position Write）から書き、RSS Monitor・SEC Ctrl・Items・Market Pulse等が監視の対象に読む",
        "docs/portfolio/tail/data/positions/{T}_thesis.json":
            "銘柄ごとの投資の仮説（エントリーの理由・条件・撤退の目安・監視するKPI）。画面の入力から書く（人の入力）",
        "docs/portfolio/tail/data/journal.json": "売買・仮説の見直しの記録（判断の日誌）。画面の入力から書く（人の入力）",
        "/tmp/tail_commit_message.txt": "TANUKI_TAIL_Position_Writeの1回の書き込みのcommitの件名（同じワークフローの後の段が git commit -F で読む）",
        "docs/portfolio/tail/data/rss_state.json": "SEC EDGARのRSSで見た銘柄ごとの最後の提出・確認の日時（新規提出・提出の遅れの判定に使う）。",
        "docs/portfolio/tail/data/review_queue.json":
            "四半期のレビューを作る待ち行列（新規提出で積み、レビューを作ったら completed・古い期は superseded）",
        "docs/portfolio/tail/data/reviews/{T}_{Q}_review.json":
            "銘柄・四半期ごとのレビュー。**AIの出力（Grok）**: stage1（健康度・推奨・理由等）・stage2（シナリオ〈bull/base/bear〉・KPIの予想等）・call2。"
            "grok_call1_success・grok_call2_success・stage2_json_valid はAIの呼び出しの結果の印、kpi_snapshot・layer3_snapshot・macro_snapshot は作った時点の入力の控え。"
            "レビューは対象の期にかかわらず作った時点の最新の財務で書かれる（[[TAIL-REVIEW-PERIOD-FILTER-1]]。06-06の一括生成の分は[[TAIL-REVIEW-BACKFILL-DATA-PERIOD-1]]、統合済み）",
        "docs/portfolio/scenario/{T}/{sc_name}.json":
            "銘柄ごとのシナリオ（bull/base/bear）のDCF。tail_dcf_bridge.py がレビューの stage2.scenarios（**AIの出力**）とLayer2のKPIから作る",
        "docs/portfolio/tail/data/prediction_history.json": "レビューの予想（AIの出力のKPIの予想）と実績の突合の履歴（予想の当たり外れ）",
        "docs/portfolio/tail/data/kpi/{T}_layer2.json":
            "銘柄ごとのXBRLのセグメント・KPIの四半期の値（Layer2）。TANUKI VALUATIONのセグメント成長率（segment_growth_xbrl.py）も読む。",
        "docs/portfolio/tail/data/kpi/{T}_layer3.json": "銘柄ごとの、決算の本文から取ったKPI（Layer3。XBRLに無い数値）。",
        "docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json":
            "銘柄ごとのKPIの提案。**AIの出力（Grok）**: proposed_kpis・rejected_kpis（却下はXBRLとの照合の結果）。"
            "kpi_proposer.py は手で動かす（CLAUDE_CODE_START.md「TANUKI TAIL 銘柄追加手順」。ワークフローは無い）",
        "config/tail_kpi_map.json": "銘柄ごとに監視するKPIとXBRLのタグの対応。kpi_proposer.py（手で動かす）が提案のうち照合を通ったものを書く",
        "docs/portfolio/tail/data/ctrl/{T}/latest.json":
            "銘柄ごとの最新の10-QのItem 4（内部統制）の抜粋と判定。**AIの出力（Grok）**: item4_excerpt_ja（抜粋の和訳）。",
        "docs/portfolio/tail/data/ctrl/{T}/{Q}.json":
            "銘柄・四半期ごとのItem 4の抜粋（latest.jsonと同じ中身の期ごとの控え）。**AIの出力**: item4_excerpt_ja。"
            "この型は{item_key}の型（mda・risk_factors・legal_proceedings）にも当たり、読み手が混ざる",
        "docs/portfolio/tail/data/ctrl/{T}/index.json": "銘柄ごとにItem 4を取った四半期の一覧（quarters）。画面の index.html が最新の四半期を選ぶのに読む",
        "docs/portfolio/tail/data/{item_key}/{T}/latest.json":
            "銘柄ごとの最新のMD&A・リスク要因・訴訟（item_key = mda・risk_factors・legal_proceedings）の抜粋。"
            "**AIの出力（Grok）**: excerpt_ja（和訳）。segment_outlook（MD&Aのセグメントの見通し）は2026-10-22に生成を止め、過去のファイルにだけ残る。型は ctrl/ にも当たる。",
        "docs/portfolio/tail/data/{item_key}/{T}/{Q}.json":
            "銘柄・期ごとのMD&A・リスク要因・訴訟の抜粋の控え。**AIの出力**: excerpt_ja（segment_outlook は2026-10-22に生成を止めた）。型は ctrl/ にも当たる",
        "docs/portfolio/tail/data/{item_key}/{T}/index.json":
            "銘柄・項目ごとに取った期の一覧（periods）。型は ctrl/{T}/index.json にも当たり、読み手の1件（sec_ctrl_fetcher.py）は ctrl の分",
        "Discord（DISCORD_WEBHOOK）": "新規提出・提出の遅れ（法定の期限超え）のDiscordへの通知",
    },
    DECISION_DEFAULT="残す（2026-10-22）",
    DECISIONS={},
    DELETED=[
        ("docs/portfolio/tail/data/satellite_alerts.json",
         "削除（2026-10-22、Koichiさんの判断）。書き手（satellite_monitor.py、2026-09-23に削除）も読み手も無いファイルだけが残っていた（FINDINGS 2026-10-22）"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-22、Koichiさんの判断）。satellite_alerts.json だけを消し、ほかの出力はすべて残す（ctrl/{T}/index.json は画面の index.html が読む）。"
                  "誰も読まないキー30もすべて残す（作った時点の記録で作り直せない）。画面に出していないレビューのキーは IDEAS_AND_WATCH.md の [[UI-STORY-REDESIGN-1]] に記録。"
                  "スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="tail", OUTSIDE_PREFIXES=("docs/portfolio/tail/", "src/tail/"),
    READ_PREFIXES=("docs/portfolio/tail", "src/tail"),
    READ_HINT="tail", NAME_SHORT="TAIL",
    KNOWN_DUPLICATE_RULES=["latest_filed", "quarter_label", "fiscal_year"], TITLE="TANUKI TAIL（src/tail）の出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/portfolio/tail",),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={
        "business_model": "残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "competitive_advantage": "残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "management": "残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "recommendation_reason": "残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "segment_outlook": "生成を止めた（2026-10-22、Koichiさんの判断）。過去の mda のファイルにだけ作った時点の記録として残る（TANUKI の report.txt にも出ていなかった、FINDINGS 2026-10-22）",
        "base_revenue": "残す: シナリオのDCFの入力（レビューの stage2.scenarios〈AIの出力〉から作った時点の値）",
        "fcf_adjustment": "残す: シナリオのDCFの入力（レビューの stage2.scenarios〈AIの出力〉から作った時点の値）",
        "rejection_reason": "残す: KPIの提案を却下した理由（XBRLとの照合の結果）。作った時点の記録",
        "macro_score_at_action": "残す: 人の入力（売買・見直しの判断をした時点のMACROの点数）。作り直せない",
        "thesis_version": "残す: 作った時点の仮説の版（人の入力の版。後から仮説が変わっても、どの版で作ったかを残す）",
        "kpi_snapshot": "残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）",
        "layer3_snapshot": "残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）",
        "macro_snapshot": "残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）",
        "filed_date": "残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）",
        "data_version": "残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）",
        "queued_at": "残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録",
        "completed_at": "残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録",
        "superseded_at": "残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録",
        "superseded_reason": "残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録",
        "review_path": "残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録",
        "grok_call1_success": "残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "grok_call2_success": "残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "stage2_json_valid": "残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "layer1_complete": "残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "layer3_complete": "残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "layer2_missing_kpis": "残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "transcript_available": "残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）",
        "changed": "残す: 取得の印（前回の取得から本文が変わったか）。取得した時点の記録",
        "_updated_at": "残す: 予想と実績の突合を最後に更新した時点の記録",
        "last_checked": "残す: RSSで銘柄を最後に確かめた時点の記録（提出の遅れの判定の状態）",
    },
    RULES=_TANUKI["RULES"] + [
        # 宛先・再試行は common/external_api.py の1か所（[[EXTERNAL-API-CLIENT-UNIFY-1]]、2026-10-24）。各ファイルの call_grok は
        # プロンプト・モデル・温度を組む呼び出し側の関数なので当てない
        {"id": "grok_call", "name": "AI（Grok）の呼び方（APIの宛先・再試行）",
         "patterns": [r"^\s*GROK_URL\s*=", r"[\"']https://api\.x\.ai/v1/chat/completions[\"']"]},
    ],

    HTML_COPIES=[],
    PAGE_CHECKS=[],

    SECTION_DECISIONS={},

)
# ---- Portfolio（src/portfolio・docs/portfolio/index.html・docs/portfolio/data、段階1、2026-10-22）
_PORTFOLIO = dict(_SEC)
_PORTFOLIO.update(
    LAYER="src/portfolio",
    LAYER_DIRS=("src/portfolio",),
    OUT_JSON=f"{OUT_DIR}/portfolio_layer.json", OUT_MD=f"{OUT_DIR}/portfolio_layer.md",
    DECISION_PREFIXES=DECISION_PREFIXES + [
        ("docs/portfolio/index.html", "Portfolioの画面"),
        ("docs/portfolio/tail/", "TANUKI TAILの画面（保有銘柄の監視）"),
    ],
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],

    ANNOTATIONS={
        "docs/portfolio/data/portfolio.json":
            "証券会社ごとの保有（銘柄・株数・取得単価・現金）。**コードの書き手は無い（人の入力）**: 管理のページ（2026-10-21に削除）の代わりに、"
            "CLAUDE_CODE_START.md「管理の操作の手順」の「ポートフォリオを変えて」でClaude Codeが直接編集する。最後の更新は2026-06-03。"
            "Portfolioの画面・snapshot.py・TAIL（取得単価・画面）・System Health（保有の無い監視銘柄）・Market Pulse（監視銘柄）が読む",
        "docs/portfolio/data/history.json": "日ごとの資産のスナップショット（snapshot.py が portfolio.json と株価・為替から作って追記）。Portfolioの画面の推移が読む",
    },
    DECISION_DEFAULT="残す（2026-10-23）",
    DECISIONS={},
    DELETED=[
        ("docs/portfolio/data/theme_config.json",
         "削除（2026-10-23、Koichiさんの判断）。元の config/theme_config.json も消し、Discover_Config_Sync.yml の写しと rcc の同期の確認（CHECK-32）の組も外した。"
         "docs/ の写しを読むのは CHECK-32 だけで、画面は読んでいなかった（画面の color・label は画面の中の PHASE_CFG の値）"),
        ("docs/portfolio/data/discover_config.json（と元の config/discover_config.json・Discover_Config_Sync.yml・rcc の CHECK-32）",
         "削除（2026-10-23、Koichiさんの判断、[[HOLDINGS-SINGLE-SOURCE-1]]）。保有の正を portfolio.json にし、画面は区分「保有中」を読まなくなった。"
         "区分の監視中・休眠とメモを読むコードは登録の検証（register_ticker.py の Step 6・registration_validator.py の P1-Step6・P4）のほかに無かった。"
         "休眠（BBAI）は cik_lookup.csv の status の dormant に移した"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-23、Koichiさんの判断）。theme_config.json（config/・docs/）を消し、discover_config.json の各銘柄の themes を消した。"
                  "history.json・portfolio.json・discover_config.json は残す（discover_config.json は後の [[HOLDINGS-SINGLE-SOURCE-1]] で削除）。history.json の合計9キーは日ごとの記録で作り直せないため残す（下の注記）。"
                  "スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="portfolio", OUTSIDE_PREFIXES=("docs/portfolio/data/", "src/portfolio/"),
    READ_PREFIXES=("docs/portfolio/data", "src/portfolio"),
    READ_HINT="portfolio", NAME_SHORT="Portfolio",
    KNOWN_DUPLICATE_RULES=[], TITLE="Portfolio（src/portfolio・docs/portfolio）の出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/portfolio/index.html",),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={k: "残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない"
                      for k in ("cash_usd", "cost_usd", "equity_usd", "total_cash_jpy", "total_cash_usd", "total_equity_jpy",
                                "total_equity_usd", "total_pnl_jpy", "total_pnl_pct")},
    RULES=_TANUKI["RULES"],

    HTML_COPIES=[],
    PAGE_CHECKS=[],

    SECTION_DECISIONS={},
    EXTRA_OUTPUT_DIRS=("docs/portfolio/data",),

)
# ---- MACRO PULSE（段階1、2026-10-23）。共有のデータ層 common/macro_data（画面なし）と、src/market/macro_pulse・docs/market-monitor/macro-pulse の2つ
_MACRO_DECISION_PREFIXES = DECISION_PREFIXES + [
    ("src/market/macro_pulse/", "MACRO PULSE（景気後退リスクの点数・局面）"),
    ("docs/market-monitor/macro-pulse/index.html", "MACRO PULSEの画面"),
]
_MACRO_RULES = [
    {"id": "recession_score", "name": "景気後退リスクの点数（8指標の段階の点の合計）の計算",
     "patterns": [r"def\s+_?\w*(?:compute|calc)\w*score\w*\s*\(", r"function\s+(?:compute|calc)\w*Score\w*\s*\("],
     "context": r"05_events|events\.csv"},
    {"id": "step_signal", "name": "指標ごとの段階の信号（しきい値で点を付ける）",
     "patterns": [r"def\s+_?step_(?:signal|score)\s*\(", r"function\s+stepSignal\s*\("]},
    # 関数の名前（fred_latest 等）では当てない: reader.py 経由に切り替えた後も名前が残るため。APIを直接呼ぶ所だけ
    {"id": "fred_api", "name": "FREDのAPIを直接呼ぶ所（系列の値・公表日）",
     "patterns": [r"\bFred\(\s*api_key", r"[\"']https://api\.stlouisfed\.org/fred/"]},
    {"id": "grok_endpoint", "name": "AI（Grok）のAPIの宛先の直書き",
     "patterns": [r"[\"']https://api\.x\.ai/v1/chat/completions[\"']"]},
    {"id": "atomic_write_json", "name": "JSONの書き込み（一時ファイル→置き換え）の実装",
     "patterns": [r"def\s+_?atomic_write\w*\s*\("]},
]
_MACRO_DATA = dict(_SEC)
_MACRO_DATA.update(
    LAYER="common/macro_data",
    LAYER_DIRS=("common/macro_data",),
    OUT_JSON=f"{OUT_DIR}/macro_data_layer.json", OUT_MD=f"{OUT_DIR}/macro_data_layer.md",
    DECISION_PREFIXES=_MACRO_DECISION_PREFIXES,
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],

    ANNOTATIONS={
        "common/macro_data/series/{series_id}.json":
            "FREDの系列ごとの観測値（records）。Macro_Data_Update（毎日10:00 UTC、直近400日）が取り直す。"
            "MACRO PULSE（05_main.py）・Market Pulse（collect_and_send.py）が reader.py 経由で読む",
        "common/macro_data/macro_data_violations_log.json":
            "系列ごとの保存前の検証の結果（警告と取得の成否 fetch_status）。System Health の [L]（check_l_macro_data）が取得の失敗を見るのに読む",
        "common/macro_data/series_meta.json":
            "系列ごとのメタ情報（fred_release_id・obs_to_release_lag・category・note）。**コードの書き手は無い（人の手で書く静的なファイル）**。"
            "fetcher.py が取得する系列の一覧として読む",
        "${GITHUB_STEP_SUMMARY}": "Macro_Data_Update の取得の成功・失敗の件数と、失敗した系列と理由の表",
    },
    DECISION_DEFAULT="残す（2026-10-23）",
    DECISIONS={},
    DELETED=[
        ("common/macro_data/migrate_bamlh0a0hym2_history.py",
         "削除（2026-10-23、Koichiさんの判断）。05_events.csv の HY Spread の行を series/BAMLH0A0HYM2.json へ移す一度きりの移行で、2026-08-12（2ff481b3ad）に適用済み"),
        ("series_meta.json の consumers・input_id、series/BAMLH0A0HYM2.json の fallback_used",
         "削除（2026-10-23）。誰も読まない。consumers は reader.py に切り替える前の取り方の古い説明（読み手は一覧が出す）、input_id は INPUT_DATA_AS_IS.md の表から作り直せる参照の番号、"
         "fallback_used は消した移行のスクリプトが移した6,947件に書いた値（すべて False）"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-23、Koichiさんの判断）。移行のスクリプトと誰も読まないキー3つを消し、出力はすべて残す"
                  "（macro_data_violations_log.json は System Health の [L] が読む）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="macro_data", OUTSIDE_PREFIXES=("common/macro_data/",),
    READ_PREFIXES=("common/macro_data",),
    READ_HINT="macro_data", NAME_SHORT="マクロデータ層",
    KNOWN_DUPLICATE_RULES=["atomic_write_json"], TITLE="マクロデータ層（common/macro_data）の出力の一覧（段階1の下書き）",
    UNREAD_KEY_NOTES={
        "source_detail": "残す: 値の出所の記録（取得した系列。BAMLH0A0HYM2 の古い6,947件は 05_events.csv から移した印 migrated_from）。"
                         "読んでいたのは2026-10-23に消した移行のスクリプト（二重に動かさない判定）だけ",
    },
    RULES=_TANUKI["RULES"] + _MACRO_RULES,

    # _atomic_write_json(path)・_load_json(path) で読み書きする（市場データ層と同じ作り）。series_meta.json は書き手の無い静的なファイル
    IO_HELPER_BY_CALLER=True,EXTRA_OUTPUT_DIRS=("common/macro_data",),
)
_MACRO_PULSE = dict(_SEC)
_MACRO_PULSE.update(
    LAYER="src/market/macro_pulse",
    LAYER_DIRS=("src/market/macro_pulse",),
    OUT_JSON=f"{OUT_DIR}/macro_pulse_layer.json", OUT_MD=f"{OUT_DIR}/macro_pulse_layer.md",
    DECISION_PREFIXES=_MACRO_DECISION_PREFIXES,
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=["src/market/macro_pulse/05_audit.py"],

    NONPROD_NOTES={
        "src/market/macro_pulse/05_audit.py":
            "手で動かす05_events.csvの監査（重複の行等、読むだけで書かない）。ワークフローからは呼ばれず、テストが重複の判定を使う。"
            "重複の判定の本体は2026-09-10に05_main.pyへ移した（f38aae4303）",
    },
    ANNOTATIONS={
        "docs/market-monitor/macro-pulse/data/05_events.csv":
            "指標の発表ごとの行（実績・予想・サプライズ・その時点のFF金利・10Y-2Y・HY・VIX・S&P500とt+1/5/10/20の終値）。景気後退リスクの点数の元。"
            "過去の行は手で動かすスクリプトと修復のスクリプト（4つとも削除済み）も書いた",
        "docs/market-monitor/macro-pulse/data/05_fed_context.csv":
            "FRBの政策の局面とFF金利・1年金利・利下げの織り込み。**AIの出力（Grok）**: regime・dominant_concern・dominant_label・ai_reason"
            "（FOMCの声明から。Grokが使えないときは数値から決め〈_fallback_regime〉、regime_source に出典を書く）",
        "docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv":
            "指標の発表の予定（週1回 update_schedule が作り直す）。手で入れる指標は actual 列に人が入力する",
        "docs/market-monitor/macro-pulse/data/05_liquidity.csv":
            "流動性の指標の日次の行（M2・FRBのバランスシート・TGA・RRP・ネットの流動性・ステルス流動性の印・S&P500 等）。画面の流動性モニターと、TAILのレビュー（quarterly_review_generator.py）が読む",
        "docs/market-monitor/macro-pulse/data/05_meta.json": "流動性の更新の時刻（generated_at）。画面の最終更新の表示",
        "docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv":
            "週次の点数・局面と解説。**AIの出力（Grok）**: summary・factor_analysis・watchpoints・indicator_comments（model は使ったモデル）。"
            "score・phase・score_change_1w・score_change_1m・indicator_deltas・surprise_alerts はPythonの計算。TAILのレビューも読む",
        "Discord（DISCORD_WEB_HOOK）": "手で入れる指標・実績の入っていない指標のリマインドと、FOMCの局面・週次のAIの解説の通知",
    },
    DECISION_DEFAULT="残す（2026-10-23）",
    DECISIONS={},
    DELETED=[
        ("scripts/analysis/macro_events_sp500_asof_repair.py・src/market/macro_pulse/05_backfill_nfp_mom.py（と控えの 05_events.csv.bak_{datetime}）",
         "削除（2026-10-23、Koichiさんの判断）。1回だけの修復・変換で適用済み（sp500_t0_asof は2026-10-04のM-3、NFPの前月比は2026-07-08の671c5cc0d6）。参照するテストも消した"),
        ("src/market/macro_pulse/05_import_history.py",
         "削除（2026-10-23、Koichiさんの判断）。CLAUDE_CODE_START.md・MACRO_PULSE_LOGIC_INVENTORY.md 等に履歴の作り直しに使う手順が無い（あるのは過去の実行・復旧の記録だけ）"),
        ("05_events.csv の analysis・ret_t1・ret_t5・ret_t10・ret_t20・surprise_pct の列と書き込み",
         "削除（2026-10-23、Koichiさんの判断）。誰も読まない（画面の COL の対応表にだけあった行も消した）。analysis は全行が空、ret_t* は sp500_t0・sp500_tN から、"
         "surprise_pct は surprise・consensus から作り直せる。05_main.py の --fill-returns・--recalc・新しい行の書き込みもやめた。"
         "点数・局面・比較バーの過去の点数・週次の解説の入力・recalc と fill-returns の結果は前後で同じ（消した列のほか）、check_macro_pulse.py は前後とも一致67・不一致5・判定不能1"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-23、Koichiさんの判断）。手で動かすスクリプト3つと修復のスクリプト1つ、05_events.csv の誰も読まない6列を消した。"
                  "zq_price・zq_ticker（cuts_implied を作った時点の入力の記録）と 05_audit.py は残す（下の注記）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="macro", OUTSIDE_PREFIXES=("docs/market-monitor/macro-pulse/", "src/market/macro_pulse/"),
    READ_PREFIXES=("docs/market-monitor/macro-pulse", "src/market/macro_pulse"),
    READ_HINT="macro", NAME_SHORT="MACRO PULSE",
    KNOWN_DUPLICATE_RULES=["recession_score", "step_signal"],
    TITLE="MACRO PULSE（src/market/macro_pulse・docs/market-monitor/macro-pulse）の出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/market-monitor/macro-pulse",),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={k: "残す: cuts_implied を作った時点の入力の記録（1年の金利の取り方と値。後から取り方が変わっても、その行が何を見たかを残す）"
                      for k in ("zq_price", "zq_ticker")},
    RULES=_TANUKI["RULES"] + _MARKET["RULES"][:1] + _MACRO_RULES,

    HTML_COPIES=[],
    PAGE_CHECKS=[],

    SECTION_DECISIONS={},

)
# ---- Market Pulse（段階1、2026-10-23）。src/market/market_pulse と docs/market-monitor/market-pulse
_MARKET_PULSE = dict(_SEC)
_MARKET_PULSE.update(
    LAYER="src/market/market_pulse",
    LAYER_DIRS=("src/market/market_pulse",),
    OUT_JSON=f"{OUT_DIR}/market_pulse_layer.json", OUT_MD=f"{OUT_DIR}/market_pulse_layer.md",
    DECISION_PREFIXES=DECISION_PREFIXES + [
        ("src/market/market_pulse/", "Market Pulse（市況の判定・8段階の結論・天気）"),
        ("docs/market-monitor/market-pulse/index.html", "Market Pulseの画面"),
    ],
    NON_PRODUCTION_DIRS=[],
    NON_PRODUCTION_FILES=[],

    ANNOTATIONS={
        "docs/market-monitor/market-pulse/data/market_data.json":
            "1日1エントリの市況（指標・センチメント・F&G・Tech Pulse・資金の流れ・クレジット・8段階の結論・天気・data_quality 等）。画面が読む。"
            "**AIの出力（Grok）**: summary・comments_history の summary・haiku（judgment は summary から取り出す）、headlines の items の title_ja・market と translation・relevance。"
            "2026-09-26からの再設計（MARKET_PULSE_REDESIGN.md、実装A〜C）で足したキー: stage_conclusions・weather・data_quality・ai_facts・hindenburg・"
            "sector_rotation・semis_m7・commodity_fx・watch_list・headlines・calendar・futures",
        "docs/market-monitor/market-pulse/data/market_data.csv":
            "market_data.json の指標を平らにした1日1行（指標_value 等）。**AIの出力（Grok）**: summary 列（judgment 列は summary から取り出す）",
        "docs/market-monitor/market-pulse/data/breadth_data.json":
            "S&P500の構成銘柄の騰落・新高値・新安値・50日線の上の割合（breadth_calculator.py）。collect_and_send.py が読んで market_data.json に写す",
        "src/market/market_pulse/02_market_rss.txt":
            "市況のニュースを取るRSSの一覧（Google Newsの検索7本）。**コードの書き手は無い（人の手で書く静的なファイル）**。collect_and_send.py の get_market_news が読む",
        "メール（GMAIL_PASSWORD・GMAIL_USER）": "その日の市況の判定・AIの見解のGmailへの送信（自分あて）",
    },
    DECISION_DEFAULT="残す（2026-10-23）",
    DECISIONS={},
    DELETED=[
        ("src/market/market_pulse/backfill_implb.py・backfill_implc.py・backfill_stage_conclusions.py・backfill_tech_pulse.py",
         "削除（2026-10-23、Koichiさんの判断）。手で動かす一過性の付け足し・補完で適用済み（implb は eb6cf8d98d〈2026-10-01〉、implc は 01634073a9・747936b56c〈2026-10-09〉、"
         "stage_conclusions は 59406c6131・14b03af90f〈2026-09-30〉、tech_pulse は 140ce1877c〈2026-05-03〉・c50a9f3ded〈2026-05-21〉）。"
         "ワークフロー・ほかのコード・テストからの参照は無かった。過去の記録（BACKLOG_DONE.md・MARKET_PULSE_REDESIGN.md の経緯）は残す"),
    ],
    DECISION_NOTE="段階2の判断（2026-10-23、Koichiさんの判断）。手で動かすスクリプト4つを消し、出力はすべて残す。sp500_tickers.json・nyse_holidays.json は"
                  "市場データ層・MACRO PULSE との二重で、R-A の終わりの見直し（論点⑤）で扱う。誰も読まないキー48は残す（下の注記）。"
                  "スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す",
    OUTSIDE_HINT="market-pulse", OUTSIDE_PREFIXES=("docs/market-monitor/market-pulse/", "src/market/market_pulse/"),
    READ_PREFIXES=("docs/market-monitor/market-pulse", "src/market/market_pulse"),
    READ_HINT="market-pulse", NAME_SHORT="Market Pulse",
    KNOWN_DUPLICATE_RULES=[],
    TITLE="Market Pulse（src/market/market_pulse・docs/market-monitor/market-pulse）の出力の一覧（段階1の下書き）",
    SCREEN_PAGES=("docs/market-monitor/market-pulse",),
    SCREEN_SECTION_DECISIONS=[],
    UNREAD_KEY_NOTES={
        **{k: "残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力"
           for k in (
                 "ad_ratio", "ai_facts", "base_n", "breadth_date", "conditions", "distribution", "divergence_vs_sp", "earnings_tickers",
                 "etf", "etf_change_percent", "hy_bond", "ig_bond", "ivw_minus_ive_pt", "mcclellan_negative", "median_pct", "momentum_lag_weeks",
                 "new_highs", "new_lows", "nh_le_2x_nl", "nh_nl", "nh_nl_both_2_2pct", "one_month_ago", "one_week_ago", "prev_close",
                 "prev_date", "ratio_weeks", "raw_change", "raw_change_percent", "risk_avg_pct", "rsp_spy_divergence", "safe_avg_pct", "sentiment_label",
                 "sentiment_score", "sox_date", "sp500_ma200", "sp500_ma_dev", "sp500_uptrend_50d", "stage1_vix", "stage3", "stage4",
                 "stocks_ahead_of_base_date", "threshold_pt", "ultra_short", "vix_level", "vxn_available", "week", "zscore",
           )},
        **{k: "残す: その日の見出しの「市況と関係あり」の判定（Grok）の件数の記録。読んでいたのは2026-10-23に消した backfill_implc.py（結果の表示）だけ"
           for k in ("true", "false")},
        "backfilled_at": "残す: 過去の行の記録（2026-10-09 に最新のエントリの見出しを取り直した印。書き手の backfill_implc.py は2026-10-23に削除し、新しい行には付かない）",
    },
    RULES=_TANUKI["RULES"] + _MARKET["RULES"] + _MACRO_RULES + [
        {"id": "market_holidays", "name": "休場日（祝日）の一覧の作り方",
         "patterns": [r"def\s+_?\w*holidays\s*\("], "exclude": r"^(?:browser_checks|scripts/analysis)/"},
    ],

    HTML_COPIES=[],
    PAGE_CHECKS=[],

    SECTION_DECISIONS={},
    # 02_market_rss.txt（人の手で書くRSSの一覧、.py でない入力）も出力に加え、読み手を出す
EXTRA_OUTPUT_DIRS=("src/market/market_pulse",),
)
LAYERS: Dict[str, dict] = {"sec": _SEC, "tanuki": _TANUKI, "hypecore": _HYPECORE, "eps": _EPS, "stonks": _STONKS, "market_data": _MARKET,
                           "tail": _TAIL, "portfolio": _PORTFOLIO, "macro_data": _MACRO_DATA, "macro_pulse": _MACRO_PULSE,
                           "market_pulse": _MARKET_PULSE}


# CHECK-INV を当てる段階（段階3が済んだもの）。どの段階も同じ設定なので、呼び出しの索引は1回だけ作る
CHECK_LAYERS = ("sec", "tanuki", "hypecore", "eps", "stonks", "market_data", "macro_data", "macro_pulse", "tail", "market_pulse", "portfolio")
