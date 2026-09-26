# SYSTEM MAP — On-a-journey

最終更新: 2026-08-15（新DB構築プロジェクトのフィールド単位の完了確認・
`05_import_history.py`の復旧を反映。`FIELD_DEFINITIONS.md`499項目
単位での新DB参照切替状況を集計した結果、yfinance/FRED由来18項目
〈TANUKI VALUATIONの`data_fetcher.py`・Discoverの`collect.py`・
Market Pulseの`collect_and_send.py`/`breadth_calculator.py`・MACRO
PULSEの`05_main.py`〉が全件`common/market_data/`・`common/macro_
data/`へ切替済みと確認（消費者ファイル単位・重複計算パターン単位に
続く3つ目の粒度での完了確認）。また`05_import_history.py`（一過性の
一括過去投入ツール）が`05_main.py`の`get_fred()`削除〈2026-08-12〉に
巻き込まれ3日間実行不能だった状態を発見・復旧し、`common.macro_data.
reader`経由の設計へ統合した（`[[MACRODATA-IMPORT-HISTORY-
CONFIG-DRIFT-1]]`）。詳細はBACKLOG_DONE.md「2026-08-15（完了）」参照）

最終更新: 2026-08-13（重複計算パターン4件の解消を反映。STONKS SILO
（`discover/stonks-silo/src/pipeline.py`）はTANUKI VALUATIONと同じ
`SECReader.get_net_cash()`を直接参照するよう切替済み（独自の
`cash - yfinance totalDebt`算出は廃止、`[[NETCASH-DUAL-CALC-1]]`）。
HypeCore・TANUKI VALUATION・STONKS SILOのRule of 40系フィールドは
NAMING_CONVENTIONS.md規則2に従い`rule40_yoy_netmargin`（HypeCore、
旧`rule40`）・`rule40_cagr3y_opmargin`（STONKS SILO、旧`rule_of_40`）へ
改名し定義の違いを明示（`[[RULE40-DEFINITION-MISMATCH-1]]`）。STONKS
SILOの`net_income_fy`（単年度）・EPS Analyzerの`net_income_ttm`
（TTM）も同規則で期間ラベルを明示（統一はしない方針、`[[NETINCOME-
DUAL-PIPELINE-1]]`）。`common/market_data/`・`common/macro_data/`は
未追跡だった`collect_asset_flow()`のSHV等6資産・`backfill_tech_
pulse.py`のVXNCLSも切替完了し、本線タスクは完全に完了。詳細は
BACKLOG_DONE.md「2026-08-13（完了）」参照）

最終更新: 2026-08-12（common/macro_data/セクションへ`migrate_
bamlh0a0hym2_history.py`（BAMLH0A0HYM2の例外的履歴移行スクリプト）を
追記。FRED側が2026年4月から同系列の提供範囲を直近3年に制限したため、
`common/macro_data/`の通常のfetcher/reader運用とは別系統の一度限りの
例外専用スクリプトとして新設、旧`05_events.csv`から`2023-08-14`より前の
6,947件を移行済み（`[[MACRODATA-BAMLH0A0HYM2-HISTORY-EXCEPTION-1]]`参照）。

最終更新: 2026-08-12（common/macro_data/セクションを本番消費者切替完了
（**完成**）に更新。`05_main.py`（9関数・約20箇所）・`collect_and_send.py`
（3関数）の全FRED直接呼び出しを`common.macro_data.reader`経由に切替、
`get_fred()`削除・重複3系列解消を反映。単一最新値では機能を維持できない
5箇所（NFP前月比等）は`reader.get_series()`使用である旨も明記。
`fred_release_dates()`のみ別API表面のため対象外・維持である旨を明記。
詳細はBACKLOG.md`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照）

最終更新: 2026-08-12（common/macro_data/セクションを定期取得ワークフロー
稼働開始に更新。`.github/workflows/Macro_Data_Update.yml`（毎日
UTC10:00・workflow_dispatch対応）新設と`series/`への初回実データ投入
（25系列中24系列成功）を反映。`fetcher.py`の`if __name__ ==
"__main__":`ブロック（CLIエントリポイント）についても追記。日次cronが
毎回全期間履歴を再取得する設計上の課題（`[[MACRODATA-FULL-HISTORY-
DAILY-REFETCH-1]]`）も明記。詳細はBACKLOG.md`[[MACRODATA-LAYER-
CONSTRUCTION-1]]`参照）

最終更新: 2026-08-12（新規セクション「common/macro_data/（FRED統合層）」
を追加。`fetcher.py`（`fetch_series`/`update_series`/`fetch_all_series`、
fredapiクライアントのモジュールレベル一元化・リトライ3回＋指数
バックオフ・保存前検証2項目）・`reader.py`（`get_latest`/`get_series`/
`get_value_as_of`）のAPI構成・保存構造（`series/{SERIES_ID}.json`・
`series_meta.json`・`macro_data_violations_log.json`）を記載。本番消費者
（`05_main.py`・`collect_and_send.py`）は未切替である旨も明記。詳細は
BACKLOG.md`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照）

最終更新: 2026-08-12（common/market_data/セクションを診断ツール2
（`audit.py`・`score_verifier.py`）・周辺ツール2（`extract_key_facts.py`・
`backfill_tech_pulse.py`）切替完了に更新。**本番消費者8＋診断ツール2＋
周辺ツール2の全12ファイルが完了**。reader.pyのAPI一覧に
`get_price_on_or_after`・`get_price_series_as_of`を追加（10種→12種）。
「変更時の影響範囲チェックリスト」の該当2行も「8/8切替済み」から
「全12ファイル切替済み」表記に更新。詳細はBACKLOG.md
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・BACKLOG_DONE.md「2026-08-12
（完了）」参照）

最終更新: 2026-08-11（common/market_data/セクションを本番消費者
**8ファイル8/8切替完了**に更新。pipeline.py・collect.py・
collect_and_send.py・breadth_calculator.py・hypecore.py（前提作業3件
込み）の切替完了を反映し、reader.pyのAPI一覧に`get_earnings_history`・
`get_recommendations_history`を追加（8種→10種）。auto_adjust差分の
訂正経緯（バグ登録→事実確認→「移行に伴う意図せぬ改善」への訂正・
クローズ）も追記。「変更時の影響範囲チェックリスト」の2行も
切替済み分/3ファイルの表記を8/8完了に更新。詳細はBACKLOG.md
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・BACKLOG_DONE.md
「2026-08-11（完了）」参照）

最終更新: 2026-08-11（新規セクション「common/market_data/（yfinance統合層）」
を追加。fetcher.py/reader.pyのAPI構成・保存構造・レイヤー横断再計算禁止
方針を記載し、本番消費者切替の進捗（8ファイル中3/8完了:
beta_fetcher.py・data_fetcher.py・valuation_fetcher.py）を反映。
「変更時の影響範囲チェックリスト」にcommon/market_data/fetcher.py・
reader.pyの2行を追加。詳細はBACKLOG.md`[[MARKETDATA-LAYER-
CONSTRUCTION-1]]`参照）

最終更新: 2026-08-07（common/sec_data統合フェーズD実質完了に伴う陳腐化
是正。layer3_builder.py項の「フェーズD Step2-1」追記に続けて、
②STONKS SILO・③TANUKI TAIL・④HypeCoreのLayer3切替完了、⑤stock.html
＋診断・補助スクリプト7件（Step2-5）実質完了（切替対象がほぼ存在せず）、
`normalized/`残存消費者が`fetcher.py`〈STONKS SILO〉・
`dcf_validity_checker.py::check_c_data_jump()`・stock.htmlの3系統に
確定・恒久化、フェーズE〈`normalized/`完全廃止〉着手不可、という
最終状況を追記。あわせて2026-07-30時点の記述「`quality_checker.py`/
STONKS SILO`financial_trend_calculator.py`が`normalized/`を直接消費」
が陳腐化していた点（`quality_checker.py`はimportゼロの死蔵コードと
判明、`financial_trend_calculator.py`はLayer3へ切替済み）を訂正）

最終更新: 2026-08-02（2026-07-30の4コミット分の陳腐化を追加是正。
data_fetcher.py::TTMReader項に[[TTM-PASCALCASE-KEY-STALE-1]]
（audit.py::audit_ticker()・build_rice_annual_shape()のPascalCase→
snake_case取り残しでRICEスコア3日間全停止・94銘柄FCFソース誤後退、
コミットa7b840c32）を追記。layer3_builder.py項にLAYER3-SGA-Q4-MISSING-1
（selling_general_and_administrativeのQ4恒常欠落42銘柄171四半期を解消、
同コミット）を追記。同項に[[DOCS-SECDATA-NORMALIZED-DIR-STALE-1]]
（`docs/common/sec_data/normalized/`が2.2〜2.3ヶ月55銘柄分陳腐化、
quarterly_review_generator.py/tail_dcf_bridge.pyのredirect＋
SEC_Data_Update.ymlへのrsync同期ステップ新設で解消、コミット5ee157c6b）を
追記。新規`segment_fetcher.py`項を追加し[[SEGMENT-FETCHER-DUPLICATE-
ORPHAN-1]]（src/value/tanuki_valuation側の重複ファイル削除・
common/sec_data側へ一本化、コミット0e60ee255）を記載。STONKS SILO節に
STONKS-SILO-COGS-DEAD-FALLBACK-1（discover/stonks-silo/src/fetcher.py::
_normalize_record()のcost_of_revenue代替キー参照デッドコード削除・
falsy-zeroバグ副次修正、コミット84385c271）を追記。newfield_q4_cutoff_
check.pyの新設日表記を2026-08-01→2026-07-30〈同コミットa7b840c32〉に訂正）

最終更新: 2026-08-02（2026-08-01〜02セッションの陳腐化是正。parser.py項に
[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]（`_backfill_total_
liabilities_via_identity()`）・[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]
案b（`_align_cost_of_revenue_to_revenue_period()`）・[[LAYER3-GROSSPROFIT-
BACKFILL-PROD-UNREACHED-1]]①（`_backfill_gross_profit_from_revenue_
cogs()`）・[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1・段階2
（`_resolve_bs_entity_mixing()`、新設`spac_shell_detection_log.json`）を
追記。utils.py項に`detect_fiscal_anchor_clusters()`（ELF-FISCAL-END-
MONTH-MISDETECTION-1案②）を追記。fetcher.py項に[[FETCHER-10KT-10QT-
FORM-EXCLUSION-1]]案③（report_consistency_check.pyのCHECK-28/WARN-28）を
追記。新規スクリプト`newfield_q4_cutoff_check.py`を独立ツールとして追記。
いずれも2026-08-01実装済みだが本ファイル未反映だった陳腐化箇所）

最終更新: 2026-07-30（common/sec_data統合フェーズA〜D準備セッション。
【SECデータ取得層】ツリーに`layer3_builder.py`（Layer3、
`config/sec_concept_definitions.json`ベースの統一snake_caseインメモリ
ストア、`build_ticker_store()`）を新規追記し、`ttm_calculator.py`の
入力元がフェーズC対応で旧`normalize()`戻り値からLayer3ストアに切替済み
であることを反映。現状「新旧2スキーマ併存」ではなく実態は
Layer3（snake_case・インメモリ）・`data/annual_*.json`等（parser.py、
snake_case・ファイル）・`normalized/`（normalizer.py、PascalCase・
quality_checker.py/STONKS SILO financial_trend_calculator.pyが直接消費）
の**3スキーマ併存**であることを明記（詳細は`docs/architecture/
new_data_platform/SEC_EDGAR_LAYER_DESIGN.md`「3スキーマ併存の実態」・
移行手順は同ディレクトリ`MIGRATION_CHECKLIST.md`参照）。
`tag_definitions.py`の項に、`config/sec_concept_definitions.json`
（Layer3側の独立した候補タグリスト）との乖離リスク（[[JNJ-RD-TAG-
PRIORITY-1]]・[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]で発見）を追記。
フェーズD（本体consumer切替）は対象優先順位確定済みで着手可能な段階
〈詳細はCLAUDE_CODE_START.md・PROJECT_STATUS.md参照〉）

最終更新: 2026-07-23（新一次データベース構築プロジェクトの存在を追記。
現状の一次データ取得（`common/sec_data/`のSEC EDGAR・yfinance11ファイル/
FRED2サブシステムの分散状態）を、TO-BE設計に基づく統合層へ再構築する
プロジェクトが2026-07-22〜23の設計フェーズを経て起票された（**実装は
まだ未着手**）。進捗は`PROJECT_STATUS.md`（リポジトリルート）で管理する。
仕様書一式は`docs/architecture/new_data_platform/`配下:
`INPUT_DATA_TOBE.md`（新DB設計仕様書）・`INPUT_DATA_AS_IS.md`（移行対象
棚卸し）・`FIELD_DEFINITIONS.md`（導出データ499項目要件定義書）・
`TO_BE_FINAL_LIST.md`（導出データ最終仕様）・`NAMING_CONVENTIONS.md`
（命名規約）・`CONCEPT_PARAMETER_VARIATIONS.md`（要注意パラメータ
バリエーション一覧）。経緯記録は同ディレクトリ`archive/`配下
（`RETROSPECTIVE_2026-07-22.md`・`OUTPUT_ITEMS_INVENTORY.md`・
`TO_BE.md`・`DERIVED_DATA_SUBCATEGORIES.md`）。本プロジェクトに関わる
実装・仕様書更新を依頼する際は、完了報告に`PROJECT_STATUS.md`の該当
ステータス更新有無を含めること（`CHAT_RULES.md`参照）。）

最終更新: 2026-07-12（TTM-QUARTERS-CHECK-1完了に伴いdata_fetcher.py::TTMReader
の説明を更新（_select_fcf_source()導入・quarters_used>=4フィルタ反映）、
report_consistency_check.pyのWARN確認済み台帳（config/warn_acknowledged.json・
QUALITY-GATES-EPIC-1 Phase 1）を追記、変更時の影響範囲チェックリストに
data_fetcher.py行を追加）

最終更新: 2026-07-10（銘柄振り分けの正本（cik_lookup.csv）セクション新設、
システム一覧テーブルの出力先パス誤記5件を修正、STONKS SILOの解像度向上、
AutoTrade運用実体・OpenD前提を追記、report.txt統合レポートの存在・構成・
パース注意点を追記、common/screening/配下2スクリプト新設反映）

---

## システム一覧と責任範囲

| システム | 主な責任 | 出力先 |
|---|---|---|
| TANUKI VALUATION | DCF理論株価・RICE投資効率 | docs/value-monitor/tanuki_valuation/ |
| HypeCore | 期待プレミアム・フェーズ判定 | docs/value-monitor/hypecore/ |
| TANUKI SCORE | 多銘柄比較・最終投資判断 | docs/value-monitor/tanuki_score/（daily pick機能の出力は docs/integrated-dashboard/、下記参照） |
| STONKS SILO | 赤字企業の投資適合性評価（詳細は下記セクション参照） | docs/value-monitor/stonks-silo/ |
| EPS ANALYZER | GAAP/Non-GAAP乖離・割安発掘 | docs/value-monitor/adjusted_eps_analyzer/ |
| MACRO PULSE | マクロ環境・景気後退リスク | docs/market-monitor/macro-pulse/ |
| Market Pulse | 市場センチメント・資金フロー | docs/market-monitor/market-pulse/ |
| Extreme Fear | 買付支援・TANUKI TOP10・投入額シミュレーター（Market Pulse配下） | docs/value-monitor/extreme-fear/ |
| PORTFOLIO | 保有ポートフォリオ管理 | docs/portfolio/ |
| AutoTrade | F&G×TQQQ自動売買 | C:\Users\shigi\AutoTrade\fg_level2\（リポジトリ外、運用中の実体。下記「AutoTrade/OpenD運用前提」参照） |

（2026-07-10全件棚卸しで、上記5システムの出力先パス記載がリポジトリ実態と
乖離していたことが判明したため修正: STONKS SILO/EPS ANALYZER/MACRO PULSE/
Market Pulse/PORTFOLIO。特にPORTFOLIOの旧記載`docs/management/portfolio/`は
該当ディレクトリ自体が存在しなかった）

---

## 銘柄振り分けの正本（cik_lookup.csv）（2026-07-10追記）

`config/cik_lookup.csv`（106銘柄）が、どの銘柄をどのシステムで評価するかを
決める**入力側の正本**。カラム構成:

| カラム | 役割 |
|---|---|
| ticker / cik / name | 銘柄コード・CIK・会社名 |
| eps_sector | EPS ANALYZERのセクター分類上書き（`sector_classifier_v2.py`が参照。多くの銘柄で空欄） |
| stonks_silo / tanuki / eps / hypecore | 各システムの対象可否フラグ（後述） |
| status | active / candidate / retired。新規登録手順Step 0.5で記録（詳細はCLAUDE_CODE_START.md参照） |
| registered_date / registration_source / registration_note | 登録日・登録経緯（moomoo_screening/manual_thesis等）・登録理由の要約 |

**銘柄リスト統一アクセサ（`common/sec_data/tickers.py`、2026-07-12 ZS-TICKERS-LEAK-1で新設・
FLAG-CONSUMER-AUDIT-2/3で全面適用完了）:**
`get_active_tickers(flag)`（フラグ='true'かつstatus≠'retired'の銘柄を返す。'candidate'は
含める）を核に、`get_tanuki_tickers()`/`get_stonks_silo_tickers()`/`get_eps_tickers()`/
`get_hypecore_tickers()`の4便利関数を提供する。以下の全システムがこれ経由で
「フラグ='true'銘柄の一括取得」を行う——**cik_lookup.csvの独自読み込み・
`os.listdir()`直接スキャン等の独立経路は原則存在しない**：

**フラグと各システムの対象銘柄取得経路（実装確認済み・2026-07-12時点）:**
- `tanuki=true` → `src/value/tanuki_valuation/pipeline.py`（`_load_tickers_from_csv()`が
  `get_tanuki_tickers()`経由で取得）。CLI引数でticker明示指定時も`_filter_tanuki_tickers()`で
  同フラグを検証し範囲外を除外
- `stonks_silo=true` → `discover/stonks-silo/src/pipeline.py`（`stonks_tickers()`が
  `get_stonks_silo_tickers()`経由）。CLI引数明示指定時も`_filter_stonks_silo_tickers()`で検証
- `hypecore=true` → `src/value/hypecore/hypecore.py --all`（`get_hypecore_tickers()`直接）。
  `--batch`/単体指定時も`_filter_hypecore_tickers()`で検証（2026-07-12まではノーガードだった、
  FLAG-CONSUMER-AUDIT-3参照）
- `eps=true` → `src/value/adjusted_eps_analyzer/pipeline.py`の`run()`（引数なし＝通常の
  バッチ実行）が`get_eps_tickers()`を**直接使用**（旧SYSTEM_MAP記載の「使われない」は
  誤りだったため訂正）。`--ticker`明示指定時も`_filter_eps_tickers()`で検証（2026-07-12まで
  はmonitor_tickers.yaml突合＜非ブロッキング警告のみ＞しかなく、フラグ検証自体がなかった）。
  `registration_validator.py`でもEPS未対応銘柄のWARN抑制に使用
- `common/sec_data/report_consistency_check.py`のスキャン対象も`get_tanuki_tickers()`と
  report.txt存在確認の積集合（旧`os.listdir(DATA_DIR)`直接スキャンから2026-07-12に変更）
- `src/value/tanuki_valuation/score_verifier.py`の`--ticker`省略時の全銘柄スキャンも
  `get_tanuki_tickers()`に限定（2026-07-12まではディレクトリ実在のみで判定していた）
- フラグに依らない共通upstream: `common/sec_data/config.py::get_all()`がcik_lookup.csv**全106銘柄**を返し、`common/sec_data/update.py`（SEC生データ取得）はこれを既定の対象とする。個別システムのフラグはこのSECデータ取得より下流の各パイプラインで参照される

**`config/monitor_tickers.yaml`（99銘柄）との関係:**
cik_lookup.csvとは独立した別ファイルで、以下の実際の用途を持つ:
- `src/value/adjusted_eps_analyzer/pipeline.py`の`run()`は`--ticker`明示指定時のみ、
  指定銘柄がmonitor_tickers.yamlに存在するかを**非ブロッキング警告**としてチェックする
  （未登録でも処理は続行。eps=trueフラグの検証＝ブロッキングとは別軸）
- `common/sec_data/registration_validator.py`の既定スキャン範囲（`target_tickers`未指定時）
- `docs/value-monitor/adjusted_eps_analyzer/admin/`のUIから直接編集可能（EPS ANALYZER運用者向けの手動キュレーションリスト）

cik_lookup.csvとの同期は自動化されておらず、新規銘柄登録手順Step 7
（CLAUDE_CODE_START.md）で手動追加する運用。`registration_validator.py`の
P4-CIKOrphanチェックが乖離をWARN（NGではない）として検出するが、
WARNはコミットをブロックしないため見落とされやすい。

**2026-07-10棚卸しで判明した実際の差分（cik_lookup.csv 106件 − monitor_tickers.yaml 99件 = 7件）:**
- `RMBS`/`ENTG`/`TER`/`KLAC`/`LRCX`（5件・2026-07-09に「半導体関連・手動一括登録」で登録、
  いずれも`tanuki=true`/`eps=true`/`hypecore=true`）: Step 7が未実施のまま残っている
  同期漏れと判定（本来monitor_tickers.yamlに存在すべき）
- `APGE`（1件・2026-07-02登録、`eps=true`/`hypecore=true`、status=candidate）: 他のcandidate銘柄
  （WST/CON/SN）はmonitor_tickers.yamlに存在するため、status=candidateであること自体は
  除外理由にならない。こちらも同期漏れの可能性が高い
- `BX`（1件）: `stonks_silo`/`tanuki`/`eps`/`hypecore`が全てfalseのため、monitor_tickers.yaml
  非掲載は正当（[[CIK-ORPHAN-FLAGS-1]]参照）
- 対応要否（monitor_tickers.yamlへの6件追加）は本調査のスコープ外のため、
  別途BACKLOG化を検討する

---

## 統合レポート（report.txt）— 銘柄ごとのAI向け横断出力（2026-07-10追記）

各TICKERフォルダ `docs/value-monitor/tanuki_valuation/data/{TICKER}/` には、
`latest.json`/`history.json`/`score_history.json` と並んで `report.txt`
（統合レポート・AIプロンプト用プレーンテキスト）が生成されている。
TANUKI VALUATION・HypeCore・STONKS SILO・RISK EVENTS等、複数システムの
出力を1ファイルに集約した横断ビューであり、**複数銘柄のスクリーニング・
比較作業に着手する前に、個別JSONを組み合わせる前段としてまずこのファイルの
有無を確認すること**（2026-07-10のサテライト投資候補スクリーニングで
この存在に気づかず大きく遠回りした教訓）。

**主要セクション構成:**
- `[1] TANUKI SCORE`: Classification（BUY/WATCH/HOLD/TRIM/GROWTH_PREMIUM/SELL/PASS）・Funda_Score・Timing_Score構成要素
- `[2] MATRIX POSITION`: Matrix種別（①投資効率系〜④キャッシュ創出力系）・Quadrant・Key_Metric_Y
- `[3] TANUKI VALUATION`: Current_Price・Intrinsic_Value_BASE・BEAR/BASE/BULLシナリオ・DCF_Reliability・**Growth_Rate_Rec（乖離⚠️警告付き。この項目は[3]内にあり[4]ではない）**
- `[4] 成長率根拠`: Phase1成長率（DCF適用値）・推奨成長率/元成長率/最終推奨値・判定（PLAUSIBLE/REVIEW/AGGRESSIVE）
- `[5] RICE METRICS` / `[6] EPS ANALYZER`
- `[7] HYPECORE`: Current_Phase・Phase_History（直近6ヶ月）
- `[8] STONKS SILO`: Short_Interest・Runway_Months・Breakeven_Estimate等（TANUKI VALUATION対象銘柄でもセクション自体は常に出力され、非該当項目はN/A表示になるだけ）
- `[9] RISK EVENTS`: Grok web検索由来のリスクイベント（高/中/低の重要度付き。カタリスト等アップサイド事象は含まれない、[[REPORT-CATALYST-1]]参照）

**パース時の注意点（2026-07-10のスクリーニング作業で発生した3件のバグより）:**
- `Growth_Rate_Rec`（乖離⚠️付き）はセクション`[3]`内にあり、セクション`[4]`の
  「推奨成長率」「元成長率」「最終推奨値」とは別の場所にある。`[4]`のみを
  見て「見つからない」と誤判定しないこと
- `Intrinsic_Value_BASE`・シナリオIVはマイナス値（例: `$-1.34`）を取り得る
  （LYFT等、DCF評価がマイナスになる赤字銘柄で発生）
- シナリオ行 `"IV=$X, Deviation=Y%"` のIV捕捉を貪欲マッチにすると
  末尾のカンマまで飲み込みDeviationが取得できなくなる（非貪欲マッチが必要）
- 上記3件の教訓を反映済みの正式パーサー: `common/screening/report_txt_parser.py`

**report.txt生成対象外の銘柄（cik_lookup.csvでtanuki=false）の注意点:**
tanuki=falseに変更された後も、変更前に生成された`report.txt`/`latest.json`が
自動削除されずファイルシステム上に残存することがある（例: RKLB/ZSは
2026-07-02にtanuki=falseへ変更されたが、2026-06-26/27生成のreport.txtが
現存する）。`generated_at`/`calculation_date`が他銘柄より古い場合は
tanuki=false化前の旧データである可能性を疑い、参考値扱いとすること。

**DCF前提・ROIC妥当性の機械チェック:** `common/screening/dcf_validity_checker.py`
（成長率floor値張り付き・SECデータ異常ジャンプ・投下資本の妥当性・
HypeCore遷移確率サンプル数の4観点を機械判定。詳細はスクリプト内docstring参照）

**report_consistency_check.pyのWARN確認済み台帳（QUALITY-GATES-EPIC-1 Phase 1・
2026-07-12新設）:** `config/warn_acknowledged.json`に`(CHECK番号, ticker)`の
組み合わせを事前登録すると「確認済み」として通常表示される。未登録のWARNは
実行時に`[🆕未確認 WARN-N ...]`と強調表示される（既存の非ブロッキング動作は
維持、NG化はしない）。台帳読み込み・照合ロジックは`load_warn_ledger()`/
`annotate_warn()`（同スクリプト内）、単体テストは`tests/test_report_consistency_check.py`。

---

## STONKS SILO（詳細・2026-07-10追記）

赤字企業（cik_lookup.csv `stonks_silo=true`、2026-07-10時点25銘柄）を対象に、
黒字化までの投資適合性を評価する。

**`stonks_silo=false`の一般方針（2026-09-03追記、[[REGISTER-FLOW-
REDESIGN-1]]方針4対応）**: `cik_lookup.csv`の`stonks_silo=false`は
2026-09-03時点78銘柄と大多数を占めるが、これは個別銘柄ごとの判断では
なく**「大型株・黒字企業全般はSTONKS SILO評価対象外」という設計方針**
そのものである（STONKS SILOは上記の通り赤字企業の黒字化評価に特化した
システムであり、`stonks_silo=true`側〈25銘柄〉が例外的に選定された赤字
企業リスト）。そのため`cik_lookup.csv`の`exclusion_reason`列には、この
一般方針に該当する78銘柄分は記入しない（個別の除外理由が存在する
RKLB/ZS〈成長株のためSTONKS SILO評価が適切、tanuki側は対象外〉のような
ケースのみ記入する設計）。

**パイプライン:** `discover/stonks-silo/src/pipeline.py`（analyzer.py/fetcher.py/
financial_trend_calculator.py/valuation_fetcher.pyで構成）→
`docs/value-monitor/stonks-silo/data/results.json`

**追記（STONKS-SILO-COGS-DEAD-FALLBACK-1 2026-07-30実装完了、コミット
84385c271）**: `fetcher.py::_normalize_record()`のgross_profit補完で、
`cost_of_goods_sold`・`cost_of_goods_and_services_sold`の2キーはannual_
YYYY.jsonのpl辞書に実在せず常にNoneを返すデッドコードだったため削除し
`cost_of_revenue`のみ参照に単純化（登録時コミットのメッセージ確認により、
意図的な将来予約ではなくparser.pyの複数候補タグ→単一キー統合設計を誤認
した憶測に基づくデッドコードと確認）。副次的発見として、旧`or`チェーンは
`cost_of_revenue=0`（RXRX 2021年で実在）の場合に次の候補キーへ誤って
フォールスルーしcostがNone扱いになるfalsy-zeroバグを内包していたが、
今回の単純化で同時に解消。全25 stonks_silo銘柄のStonksAnalyzer.analyze()
出力を変更前後で比較し差分ゼロを確認済み。

**主要フィールド構成（`results.json.tickers.{TICKER}`）:**
- `deficit_quality`: revenue_growth_pct・cagr_3yr・rnd_ratio・sm_ratio・gross_margin・
  verdict（BUY/WATCH等）・score・sbc_adjusted_fcf・sbc_ratio・dilution_risk
- `runway`: cash・monthly_burn・**runway_months**・verdict（SAFE等）・score
- `profitability_path`: core_profit/ocf_annualの年次推移・ocf_yoy_change・ocf_acceleration
- `overall_score` / `overall_verdict` / `summary` / `records` / `valuation` / `financial_vectors`

**TANUKI VALUATIONとの依存関係（重要・従来SYSTEM_MAPに未記載）:**
`src/value/tanuki_valuation/pipeline.py`は`docs/value-monitor/stonks-silo/data/results.json`を
**直接読み取り**、以下2箇所で使用する:
1. Matrix③（成長性系）のX軸「Runway(years)」表示（`stonks_data.get("runway", {}).get("runway_months")`）
2. Runway 12ヶ月未満のペナルティ判定（資金枯渇リスクをDCF評価に反映）

STONKS SILO非対象銘柄（stonks_silo=false）でRunway相当の情報が必要な場合は、
pipeline.py内で`computed_runway_months`（Cash / 月次FCF Burn）を独自にフォールバック計算する。
→ 変更時の影響範囲チェックリストに関わる: `discover/stonks-silo/src/pipeline.py`の
runway計算ロジックを変更する場合、TANUKI VALUATION側のMatrix③・Runwayペナルティへの
影響も確認すること。

---

## TANUKI SCORE daily pick（docs/integrated-dashboard/）（2026-07-10追記）

`src/value/tanuki_score/daily_pick.py`の出力先は`docs/value-monitor/tanuki_score/`配下ではなく
リポジトリ直下の`docs/integrated-dashboard/`（`daily_pick.json`・`history.json`）。
`docs/value-monitor/tanuki_score/index.html`がfetchして表示する（CHAT_RULES.mdの
「銘柄スクリーニング着手前の確認事項」で言及される「Extreme Fear TOP10」の実体データ）。

---

## ワークフロー依存関係定義（config/workflow_dependencies.json）（2026-07-10追記、2026-08-22更新）

GitHub Actions各ワークフロー（SEC_Data_Update → HypeCore_Update / Adjusted_EPS_Update /
Stonks_Silo_Update → TANUKI_VALUATION_Update、Market_Data_Daily_Update → TANUKI_VALUATION_Update）の
依存関係グラフを定義するJSON。`docs/value-monitor/admin.html`の「実行」タブが読み取り、
一括更新ボタンの実行順序制御に使用する。ワークフローを新設・依存関係変更した場合は
このファイルへの追記が必要（admin.html側の実行UIに反映されないと手動個別実行が必要になる）。

**（2026-08-22追記）** 上記の論理的依存関係は、以前は本JSON（admin.html手動実行用の
メタデータ）にのみ定義され、実際のGitHub Actions自動トリガーには反映されていなかった
（[[WORKFLOW-SEC-TANUKI-GAP-1]]・[[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]）。
`HypeCore_Update.yml`・`Adjusted_Eps_Analyzer_update.yml`・`Stonks_Silo_Update.yml`・
`TANUKI_VALUATION_Update.yml`の`on.workflow_run`トリガーとして実装し、実際のCI構成にも
反映した（`TANUKI_Score_Update.yml`が先行して使っていたworkflow_run+conclusionチェック
パターンに倣った。旧来の独立cronは低頻度の安全網フォールバックとしてのみ一部残存）。
`Market_Data_Daily_Update`は本JSONに存在しなかった新規ノードとして追加登録した
（`TANUKI_VALUATION_Update`のcurrent_price鮮度に必要な依存）。本JSON自体と実際の
`.github/workflows/*.yml`の`on.workflow_run.workflows`設定は別々のファイルで手動同期される
（本JSONを変更しても自動的にYAML側へは反映されない）ため、依存関係を変更する際は
両方を更新すること。

---

## AutoTrade/OpenD運用前提（2026-07-10追記）

**運用中の実体はリポジトリ外:** `C:\Users\shigi\AutoTrade\fg_level2\`が実際に稼働している
F&G Level2×TQQQ自動売買システム（Windowsタスクスケジューラから`trader.py --entry`/
`--monitor`を日次実行、moomoo OpenD経由で発注）。signal.json/state.json/trade_log.jsonlが
日次で更新される。

**リポジトリ内の陳腐化した初期複製は削除済み（2026-09-16）:** `src/subport/fg_level2/`は
2026-05-03の開発初期に作成された同名モジュール一式（trader.py/signal.py/config.json等）
だったが、`register_tasks.ps1`の`$RepoRoot`設定にも関わらず本番運用（外部
`C:\Users\shigi\AutoTrade\fg_level2\`）から実際には一切参照されていないことを
実装レベル（外部`trader.py`のCONFIG_PATH実装・Windowsタスクスケジューラの実登録
タスク3件の実行パス）で確認した上で削除した（`[[STALE-SUBPORT-CLEANUP-1]]`、
詳細はBACKLOG_DONE.md「2026-09-16（完了）」参照）。

**OpenD常時起動という運用前提:** AutoTrade運用のため、moomoo OpenD（ローカルゲートウェイ）は
既に常時起動している（2026-07-10 DESIGN-16調査時に確認）。この前提により、Moomoo API Skillや
Moomoo Skills Hub等、OpenD経由のローカル連携機能が「導入すれば追加のローカル常駐プロセスなしで
利用可能」という状態にある。ただしPC自体の停止・再起動時はOpenD接続も途切れるため、
GitHub Actions（クラウド・端末状態非依存）と同等の可用性は持たない。

---

## 共通フロントエンド部品（docs/common/）

| ファイル | 役割 | 適用範囲 |
|---|---|---|
| site-nav.js | `.nav-links`/`[data-site-nav]`をナビゲーションリンク行に置換。`body[data-tool]`でアクティブリンクをハイライト | 全ページ |
| site-header.js | `header a.logo`をロゴ画像・タイトルドット・タイトル・サブタイトルの統一DOMに置換。`body[data-tool]`からタイトル/サブタイトル/アクセント色を自動解決（EPIC-HEADER-1 2026-06-21新設） | TANUKI VALUATION・TANUKI SCORE・EPS ANALYZER・HOME・Extreme Fear（EXTREME-FEAR-1 2026-07-01追加） |
| site-theme.css | 配色トークン（`--tool-*`）・タイポ・ナビ/ヘッダー共通CSSを`!important`で上書き | 全ページ |
| glossary.json + info-tooltip.js | `<span data-info="key">`を自動検出しホバー/タップで用語説明をポップアップ表示（EPIC-LEGEND-1） | 該当箇所のみ |

---

## `config/`と`docs/`の配置原則（GitHub Pages配信制約、2026-08-15追記）

**GitHub Pagesの公開ソースは`kaihatsu`ブランチの`docs/`配下であり、
`config/`配下はHTTP経由で一切fetchできない。** 2026-08-15、実際に
稼働中の公開URLへ直接アクセスして実測済み:

```
GET https://koichi-shigihara2.github.io/On-a-journey/config/portfolio.json          → 404
GET https://koichi-shigihara2.github.io/On-a-journey/portfolio/data/portfolio.json  → 200
```

したがって**フロントエンド（ブラウザのJS）が`fetch()`で読むJSONは
必ず`docs/`配下に置く**。`config/`へ移動してはならない。`config/`は
Pythonバックエンド専用ディレクトリとして扱う。

`admin.html`等が使うGitHub Contents API（`https://api.github.com/
repos/.../contents/{path}`）はパスが`config/`でも`src/`でも`docs/`でも
区別なくアクセス可能（Pages非公開の`config/fcf_conversion_config.json`
を実際に読み書きしている実績あり）。ただし
`Authorization: token`ヘッダーにPAT（個人アクセストークン）が必須で、
未認証アクセスは60req/hour/IPと極めて低いレート制限のため、**不特定
多数が閲覧する表示ページの代替経路にはならない**（admin.html等、単一の
管理者が使う管理画面限定で成立する経路）。

**再発防止の記録（過去に同一の取り違えが3回、独立に発生）**:

| ファイル | 経緯 |
|---|---|
| `portfolio.json` | 2026-05-23 21:37 `config/`のみに作成→66分後の22:43、`4ff1f1992`「portfolio.jsonをdocs/以下に配置・fetchパス修正」でdocs/コピーを追加 |
| `discover_config.json` | 2026-05-23 `config/`に作成（`ee1b6ddad`）→同日中に`7875c6be1`「discover_config配置・認証DOM修正」でdocs/コピーを追加 |
| `theme_config.json` | 2026-05-31 `config/`のみに作成→4日後の06-04、`2e99c8844`「docs/ 全HTMLリンク整合性修正」で「GitHub Pagesから参照可能に」とコミットメッセージに明記した上でdocs/コピーを追加 |

新規に手動設定ファイルを追加する際、フロントエンドから直接fetchする
想定であれば最初から`docs/`配下に置くこと。詳細な調査経緯は
`BACKLOG.md`の`[[PORTFOLIO-CONFIG-DUP-1]]`・`[[DISCOVER-CONFIG-
DUAL-MGMT-1]]`参照。

### `config/`↔`docs/`重複ファイルの解消パターン（2件の実例、2026-08-15）

`config/`と`docs/`の両方に同名ファイルが存在する重複は、**「Pythonバック
エンドが読むか否か」で解消の方向が逆になる**。次に同種の判断をする際は
以下の基準に従うこと（毎回投資調査から出発しない）:

| 消費者の実態 | 唯一の正 | 解消方法 | 実例 |
|---|---|---|---|
| Pythonバックエンドの読み手がゼロ（フロントエンドのみが`fetch()`で読む） | `docs/`側 | `config/`側を削除し`docs/`側に一本化。admin.html等の書き込み経路もdocs/側へ変更 | `[[PORTFOLIO-CONFIG-DUP-1]]`（`config/portfolio.json`を削除、`docs/portfolio/data/portfolio.json`に統一） |
| Pythonバックエンドの読み手が存在する（パイプライン本体の入力等） | `config/`側 | `config/`側は削除できない（削除すると本番パイプラインが壊れる）。`docs/`側は表示専用の自動追従コピーとして残し、`config/`への変更をトリガーに`docs/`へ自動同期するGitHub Actionsワークフローを新設する（書き手が複数存在する場合、書き手ごとに同期処理を分散実装せず1箇所に集約する） | `[[DISCOVER-CONFIG-DUAL-MGMT-1]]`（`config/discover_config.json`は`common/sec_data/registration_validator.py`が読むため削除不可。`Discover_Config_Sync.yml`新設で`docs/portfolio/data/`側を自動追従させる） |

**判断の初手は必ず「Pythonバックエンドの読み手を`grep -rn`で網羅的に
洗い出す」こと。** `[[DISCOVER-CONFIG-DUAL-MGMT-1]]`は当初「読み手ゼロ」
という誤った前提でPORTFOLIO-CONFIG-DUP-1と同じ解消方法（`config/`側
削除）を実装しようとし、実装直前の調査で`collect.py`という致命的な
読み手を発見して停止した経緯がある（詳細はBACKLOG_DONE.md参照）。

### config/読み込み失敗の横断検知（CHECK-32〜34の一般化原則、2026-08-16追記）

`report_consistency_check.py`のCHECK-32（`_check_discover_config_sync()`）・
CHECK-33（fcf_conversion_config.json専用、CHECK-34へ統合済み・廃止）・
CHECK-34（`_check_config_loaders_resolvable()`）は、いずれも同一原則
「**チェッカー独自の代理判定（`os.path.exists()`・バイト比較等）ではなく、
本番コードが実際に使う解決/比較ロジックそのものを呼び出して検証する**」
に基づく。

CHECK-34では、この原則を**複数ファイルへ横展開する際の設計パターン**を
確立した:

- 設定ファイルの読み込みロジックは`config/`配下のファイルごとに別々の
  Pythonモジュールへ分散しており、共通ローダーは存在しない（TANUKI
  VALUATION側3モジュール・EPS Analyzer側1モジュールに分散）。共通化は
  大規模リファクタリングになるため、既存の読み込み関数から**パス解決
  部分だけ**を`resolve_<name>_path()`として個別に切り出す方が既存動作を
  壊すリスクが低い
- チェッカー側は個別チェック関数をファイル数分作らず、
  `_CONFIG_LOADER_REGISTRY`（表示名・sys.path追加先・モジュール名・
  解決関数名のテーブル）を1つ持ち、汎用チェック関数`_check_config_
  loaders_resolvable()`が1つでテーブルを走査する。新しいファイルを
  対象に追加する際は、対象モジュールに`resolve_*_path()`を1つ追加し、
  レジストリに1エントリ追記するだけで済む（個別チェック関数の増殖を
  防ぐ）
- import方式は対象モジュールのパッケージ構造に依存する。`src.value.
  tanuki_valuation`配下は`__init__.py`が`.wacc`importで失敗しフル
  パッケージimportができないため、`sys.path`にモジュールのディレクト
  リを追加してモジュール名だけでimportする`"flat"`方式を使う（既存
  テスト`tests/test_divergence_sign_guard.py`等と同じパターン）。
  `src.value.adjusted_eps_analyzer`配下は相対import（`from .module
  import ...`）を使っているため`"package"`方式（`REPO_ROOT`起点の
  フルドット区切りパスでimport）が必要
- `[[CONFIG-LOAD-SILENT-FALLBACK-1]]`では7件中4件（悪質度の高い
  「完全サイレント」3件＋統合対象1件）を実装、残り3件は着手条件なしの
  ままBACKLOGに残した（詳細はBACKLOG_DONE.md参照）。追加実装時はこの
  レジストリパターンを踏襲すること

---

### DCF検証結果（validator.py）のCHECK-40接続（2026-08-20⑤追記）

TANUKI VALUATIONの`src/value/tanuki_valuation/validator.py::
run_basic_checks()`は、`pipeline.py`実行時に全銘柄で4つの決定論的
チェック（`pt_shares_consistency`・`dcf_components`・
`formula_verification`・`anomaly_detection`）を既に実行しており、
`[[QUALITY-GATES-EPIC-1]]`ゲート3（計算式検証）が求める「ゴールデン
テスト」「性質テスト」に近い内容を本番パイプライン内で担っている。
結果は`{ticker}/latest.json`の`validation`フィールドに保存され、
`stock.html:838-840`が`validation.overall`を個別ページに表示するが、
**`report_consistency_check.py`・`audit.py`・pytestのいずれからも
参照されておらず**、FAILがあっても個別ページを開かない限り誰も
気づけない沈黙構造だった（CHECK-32〜34と同型のパターン、上記参照）。

CHECK-40（`_check_dcf_validation_failures()`）は、この沈黙を解消
するため**新規の検証ロジックを実装せず、既に生成済みの`validation`
フィールドを読んで集約表示するだけ**に留めた設計とした
（CHECK-32〜34の「本番コードが実際に使う解決ロジックそのものを呼ぶ」
原則の変形——本ケースでは「本番コードが既に算出済みの結果をそのまま
読む」）。対象銘柄は呼び出し元の`all_tickers`
（`get_tanuki_tickers()`ベース）をそのまま受け取り、CHECK内で銘柄
一覧を再構築しない（事例5の原則）。

`config/dcf_validation_baseline.json`にCHECK-38と同じ設計
（baseline＝許容値ではなく是正目標、超過時のみNG）でbaselineを
記録。2026-08-20⑤実測ではPASS67/WARN32/FAIL1（100銘柄）。**WARN32件は
全件が`formula_verification`に集中しており、`validator.py`の
`alpha_cap`固定値バグ（`[[VALIDATOR-ALPHA-CAP-STALE-1]]`）による
偽陽性と判明**（本番`core_calculator.py`はセクター別alpha_capを
`maturity_config.json`から読むが、`validator.py`は1.0固定のまま
追従していなかった）。FAIL1件（LYFT）は`anomaly_detection`が
FCF恒久マイナス銘柄の負の理論株価を正しく検知したものでバグでは
ない。

**教訓**: ゲート3は「未実装」ではなく「実行結果を捨てていた」状態
だった。新規の検証機構を作る前に、既存パイプラインが既に算出して
いる値がどこかに埋もれていないかを確認する価値があるパターンとして
記録する。

**解決ロジックの一本化パターン（2026-08-20⑥追記、`[[VALIDATOR-ALPHA-
CAP-STALE-1]]`・`[[TEST-STALE-IV-1]]`修正）**: 上記WARN32件の原因は
`validator.py::_extract_params()`が`alpha_cap`を常に`1.0`固定で
読んでいたことだった（本番`core_calculator.py`はセクター別alpha_cap
を`maturity_config.json`から解決している）。修正では**validator.py
側に本番の判定ロジック〈mega_tech優先→業種別→セクター別→デフォルト〉
を書き写さなかった**——それをやると今回と同じ乖離が将来また起きる
（本番側の優先順位が変わった際にvalidator.py側だけ取り残される）。
代わりに`core_calculator.py`側の既存インライン実装を
`resolve_alpha_cap(ticker, sector, industry, default_alpha_cap)`と
いうモジュール関数へ切り出し、`validator.py`がそれをimportして使う
形にした（本番と検証が常に同一のロジックを参照する設計）。

同じパターンを`test_iv_formula.py`（IV per share再計算式の
pytest回帰テスト、ALPHA-REDESIGN-1後の式変更に長期間追従できていな
かった）にも適用した。`validator.py::pt_shares_consistency`が使う
再計算式を`recalc_ivps_from_components(v0, rpo_pv, growth_option_pv,
diluted_shares, net_cash_per_share)`という関数へ切り出し、
`test_iv_formula.py`はこの関数をimportして使う（自前の式実装を
削除）。

**一般原則として記録**: 「同じ解決ロジック・計算式を2箇所以上に
独立実装しない」（`[[QUALITY-GATES-EPIC-1]]`ゲート3の核心原則）を
満たす具体的な手段は、**規約をdocstring等の文書で伝えるのではなく、
本番コード側にある既存の実装を関数として切り出し、検証・テスト側は
それをimportして使う**こと。切り出し可能かどうかは、対象ロジックが
`self`（インスタンス状態）に依存しているかで判断する——今回はいずれも
純粋なパラメータ→戻り値の関数に切り出せたため、クラスメソッドの
外へ出す形を取った。pipeline.pyが既に`core_calculator.py`・
`validator.py`を同一ディレクトリのflatインポート
（`from core_calculator import ...`）で読んでいる前提があるため、
`validator.py`から`core_calculator.py`をimportしても既存のimport
方式と矛盾しない（循環importの有無は事前に確認すること）。

---

### operating_income（営業利益）の再構成方式とprovenance（2026-08-16追記）

`common/sec_data/parser.py`は`OperatingIncomeLoss`タグが欠落する年度
（未報告、または過去に報告していたが開示を打ち切った銘柄。LLY/XOM/
JNJ/KLAC/ASTS/COHRで確認、[[OPERATING-INCOME-EXTRACTION-GAP-1]]）に
ついて、`SECParser._backfill_operating_income()`が2段階で再構成する
（`_backfill_gross_profit_from_revenue_cogs()`と同型の「差分適用・
欠損の穴埋めのみ」設計、標準タグ取得済みの年度は変更しない）。

**優先順位（2026-08-19、案Dで改訂）**:
1. **GP法**: `gross_profit - research_and_development -
   (selling_general_and_administrative または selling_and_marketing)`。
   統合SGA報告企業はSGAを、S&M別建て報告企業（SOFI等）はS&Mを使う。
   **算出可能なら常にこちらを採用する**（下記「フォールバック向きの
   反転」参照）
2. **pretax調整法**: GP法が構造的に算出不可能な年度（XOM・ASTS等、
   COGS区分が存在しない業態、gross_profit/R&D/SGAのいずれかが欠落）
   専用のフォールバック。pretax incomeから非事業性項目
   （`NonoperatingIncomeExpense`集計タグ優先、無ければ`InterestExpense`
   〈加算〉・`InvestmentIncomeInterest`〈減算〉・
   `OtherNonoperatingIncomeExpense`〈減算〉を個別合算）を控除

**フォールバック向きの反転（2026-08-19、案A）**: 当初は「両手法の
乖離のうち非事業性項目が50%以上を説明できればGP法、未満ならpretax
調整法」という設計だったが、yfinance実測（CHECK-35第一歩、期末日を
正しく一致させた比較）で以下が判明し反転した:
- GP法が算出可能だった4銘柄（LLY/JNJ/KLAC/COHR）**全てでyfinanceと
  誤差0.0%**（完全一致）
- 旧設計でフォールバックが実際に発動した2銘柄（LLY・COHR）は、
  pretax採用値がyfinanceに対しそれぞれ-11.4%・-82.4%も乖離
- COHR自身の過去3年（`OperatingIncomeLoss`標準タグがまだ開示されて
  いたFY2022〜2024）でバックテストしても、GP法が3年連続で旧pretax
  方式を上回った（GP法誤差: 0.0%/+320.9%/+28.1%、旧pretax法誤差:
  -32.0%/-857.9%/-253.6%）

以降、両手法が利用可能な場合は**常にGP法を採用**する
（`source="reconstructed_gp"`）。旧来の「coverage_ratio 50%閾値」は
GP法の採否には使わず、**pretax調整法がどの程度信頼できたかを示す
事後診断指標**として`nonop_coverage_ratio`にそのまま記録する（役割の
転換。高い=pretaxもGP法に近かった、低い=pretaxは大きく外れていた）。

**GP法入力の整合性ガード（2026-08-19、案D）**: GP法優先化の直後、
VRT FY2018で`revenue=0`（取得失敗）にもかかわらず`gross_profit=
-$28.65億`という定義上（`gross_profit = revenue - COGS`）成立しない
組み合わせから`operating_income=-$42.87億`という明らかに誤った値が
生成される事故が発生した。この防止策として、GP法を計算する**前**に
入力（`revenue`と`gross_profit`の関係）が内部整合しているかを確認する
ガードを追加した:
- `revenue`が`None`または`0`なのに`gross_profit`が非ゼロ → 不採用
- `|gross_profit| > |revenue|`（COGSが負値であることを意味する）→
  不採用
不採用の場合、`gp_val`を`None`に落として後続のGP法計算を自然に
スキップさせ、pretax調整法へフォールバックする（詳細は下記
「GP法入力整合性ガードの設計判断」参照）。

**妥当性ガード（pretax調整法専用）**: pretax調整法は「受取利息等は
非事業性」という仮定に基づくが、銀行/フィンテック企業（SOFI）では
受取利息が本業収益そのものであり成立しない。`reconstructed_pretax`の
結果が`net_income`を下回る場合は不採用とする（GP法はこの仮定に依存
しないため対象外。ただしGP法自体の入力整合性は上記の別ガードで扱う）。

**provenance**: 採用した年度は`pl_provenance.operating_income`に
`derived: true`・`source`（`NAMING_CONVENTIONS.md`規則4準拠）・
`nonop_coverage_ratio`等を記録する。**既存の`_record()`ヘルパーが
`val is None`の場合provenance自体をスキップする仕様のため、妥当性
ガードで不採用になった年度の理由（`rejected_reason`）は最終的な
annual_YYYY.jsonには残らない**（`operating_income: None`という結果
のみが残る、他の全欠損フィールドと同じ扱い）。

**fixed_registry.jsonとの関係**: `_apply_fixed_registry_freeze()`は
`fields_snapshot`記載フィールドのみを復元する差分適用方式のため、
新規フィールド追加（今回のoperating_income）はフィックス年度でも
素通しされる（既存のgross_profit逆算バックフィルと同じ挙動）。この
結果、フィックス年度の`snapshot_hash`（CHECK-31）が変わることは
**設計上想定された挙動**であり、フィックス済みフィールドの値自体が
変わったわけではない（追加のみであることをdiffで確認した上で、
`snapshot_hash`を再計算して`fixed_registry.json`側を更新すればよい）。

**検知（CHECK-35）**: `report_consistency_check.py`が個別銘柄の
再構成使用・取得不可、および全体件数が基準値（12件、2026-08-16実装
時点の実績6件の約2倍）を超えた場合の急増をWARN（NGではない。再構成の
使用自体は正常動作のため）で検知する。

**yfinance照合の追加（2026-08-19、`[[QUALITY-GATES-EPIC-1]]`本線3・
ゲート1「複数ソース自動照合」の第一歩）**: None/derivedと判定された
銘柄についてのみ（全105銘柄ではない）、`common.yfinance_utils.
safe_yf_ticker()`経由でyfinance income_stmtのOperating Incomeを追加
取得し、WARN文言に実測値・乖離率を含めるよう拡張した。既存のWARN-10
（PS比率）・audit.pyのβ照合が使う`common.market_data.reader.
get_attributes()`ローカルキャッシュにはoperating_income相当の
フィールドが存在しないため踏襲できず、単一ティッカーの直接取得に
適したこちらの既存パターンを採用した。

**乖離率によるNG格上げは行わない設計判断**: 全105銘柄の実測（BACKLOG_
DONE.md「2026-08-19（完了）」参照）で、標準タグ採用済みの「正常」銘柄
でもyfinance側との乖離が中央値0.2%である一方でp95=81%・最大342%（AVAV）
まで裾が広いことを確認した。この分布では乖離率ベースの閾値は
Phase 2b-2（2.0倍/0.5倍で19銘柄誤検知）と同型の誤検知リスクを抱える
ため、乖離率は情報提供のみに留めWARN据え置きとした（「迷ったらWARNに
留める」判断）。SOFI・CWANはyfinance側にOperating Incomeの行自体が
存在せず照合はスキップされる（金融/フィンテック企業の簡略化された
損益計算書に由来すると推測）。

**（2026-08-19訂正）** 初回実装時点ではCOHRの乖離が-89.6%
（reconstructed_pretax $94.2M vs yfinance $901.5M）と大きく、再構成値
自体の妥当性に疑問符が残るとされていたが、これは**yfinance側の期ズレ
バグ**（`row.iloc[0]`が決算期12月以外の銘柄でSECデータより1期先の
予備的な値を指していた）が原因だった。期末日を正しく一致させて
再照合した結果、COHRの真の乖離は-82.4%（yfinance $534.9M）であり、
これは上記「フォールバック向きの反転」（案A）でGP法を採用した結果、
**0.0%（完全一致）に解消**した。詳細は`CHAT_RULES.md`「外部データとの
照合では位置ではなく期間・キーの一致を確認する」の教訓参照。

**GP法入力整合性ガードの設計判断（2026-08-19、案D）——net_income比較
ではなく入力の内部整合性を確認する理由**: 案A実装直後、VRT FY2018の
異常値対策として、当初「GP法採用値がnet_incomeを下回るなら不採用」
という案（net_income比較、既存のpretax専用ガードと同型）が検討された
が、**採用しなかった**。理由はHON FY2011の実データ:
- `revenue`/`gross_profit`の関係は正常（比率21.8%、他年度〈22-34%〉と
  同水準）——GP法の**入力**は健全
- しかし`net_income($2,067M) > GP法値($775M)`——net_incomeとの比較では
  「異常」と誤判定されうる
- これは非事業性の利得が大きい年の**正当な結果**であり、除外すべきでは
  ない（「営業利益は通常net_incomeを下回らない」は普遍的に真ではなく、
  非事業性損益が大きい年は正当に逆転しうる）

net_income比較は「GP法の値そのものの正しさ」ではなく「別の量との
相対関係」を見る**代理判定**であり、正当なケースを誤って除外する
リスクがある。代わりに、GP法の**入力そのもの**（`revenue`と
`gross_profit`の関係が定義上成立しているか）を確認する設計とした
（「前提が壊れているときは計算しない」という本セッション一貫の原則の
適用）。

**整合性条件の実測根拠**: `revenue`が有効な全年度（n=1096、105銘柄）で
`ratio=|gross_profit|/|revenue|`は中央値0.543・p99=0.939に収まり、
1.0を超える例は実質存在しない（早期段階の極小額銘柄RCAT 2件
〈2012・2018、ともに標準タグ採用済みでGP法自体は不使用のため実害
なし〉を除く）。`revenue=0`または未取得なのに`gross_profit`が非ゼロと
いう組み合わせは、GP法が実際に計算されうる年度としてはVRT FY2018のみ
該当した（詳細は`[[VRT-REVENUE-2018-MISSING-1]]`参照）。

**HONの11年度変化が示すGP法の構造的弱点**: HON（2008-2017の一部・
2021、11年度）は整合性ガードを通過し正当にGP法へ切り替わったが、
HON FY2022-2025（近年`OperatingIncomeLoss`標準タグの開示を再開した
年度、真値が既知）でGP法をバックテストした結果、**-11.9%〜-38.3%の
系統的な過小評価**を確認した。原因はHONのタグ調査で判明した
`RestructuringCharges`（FY2011で$743M）等、GP法の式
（Revenue-COGS-R&D-SGA）に含まれない別建ての営業費用項目の存在。
COHRのFY2023（GP法でも誤差+320.9%）と同型の限界であり、GP法は
「入力が整合していれば常に正確」ではなく「pretax法より大抵は近いが
完璧ではない」という位置づけにとどまる。RestructuringCosts等を
GP法の控除項目に追加する拡張は本反転のスコープ外（タグ候補拡充の
一環として別途検討）。

**下流への波及経路は2系統ある（2026-08-16追記、実測で判明）**:

1. **`moat_score`経由**: `roic_wacc_ratio`（`_calc_roic_wacc_ratio()`）
   →`moat_score`→`moat_phase1_years`（高成長期間の年数）→
   `calculate_two_stage_dcf()`のメインIV計算に直結
2. **`g_fundamental`経由（`recommended_g`への波及、AS-IS-042/
   FIELD_DEFINITIONS.md参照）**: `operating_income`→
   `_calc_g_fundamental()`（RR×ROICファンダメンタル成長率）→
   `growth_sanity.recommended_g`（3yr CAGR・5yr CAGR・業界ベンチマーク・
   RR×ROICの中央値）→`segment_configured=False`銘柄ではDCF成長率
   そのものを上書き

**②の経路は当初の影響分析（moat_scoreのみ）に含まれていなかったが、
実測ではIV変化の主因になっているケースがある**。`recommended_g`は
少数（2〜4件）の候補の中央値であるため、**候補が1件増減するだけで
中央値が大きく動きうる**（候補数が少ないほど1候補あたりの影響が
大きい、という中央値計算一般の性質）。2026-08-16実測:

| 銘柄 | g_fundamental（新規算出） | recommended_g変化 | IV変化 |
|---|---|---|---|
| XOM | +0.81% | 6.78%→0.81% | -18.7% |
| LLY | +9.46% | 21.58%→15.52% | -10.9% |
| JNJ | -2.43%（負値のため候補から除外、`recommended_g`不変） | 変化なし | -0.6%（moat_score経由のみ） |
| KLAC | -0.69%（同上、候補から除外） | 変化なし | +21.4%（moat_score経由のみ） |

`operating_income`のような一見ローカルな入力修正であっても、複数の
独立した下流消費者（本ケースでは`moat_score`・`g_fundamental`の2系統）
に同時に波及しうる点は、今後同種の根本原因修正を行う際の一般的な
教訓として留意すること（Step 0で消費者を網羅的に洗い出す際、直接の
引数だけでなく`None`ガードの有無まで確認する必要がある）。

---

### moat_score（Moat Score）の欠損処理方式（2026-08-16追記）

`src/value/tanuki_valuation/calculator/adjustments.py::calculate_moat_score()`
は`gross_margin_3yr_avg`・`roic`・`fcf_margin_3yr_avg`の3指標（重み
0.40/0.40/0.20）から`moat_score`（0〜1）と`phase1_years`（高成長期間
3〜10年、メインIVのDCF計算に直結）を算出する（[[MOAT-SCORE-
PARTIAL-NULL-1]]）。

**roicのNone原因別の扱い**: `_calc_roic_wacc_ratio()`（`pipeline.py`）は
`(値 | None, 理由コード)`のタプルを返す。理由コードにより
`calculate_moat_score()`側の扱いが変わる:
- `"reported_negative_oi"`（`operating_income`は取得できた上で
  nopat<=0、真の赤字）→ `roic_norm=0.0`で**算入**（弱いモートという
  実態を正しく反映）
- `"roic_diverged_over10"`（ROIC>=1000%、測定アーティファクトの疑い）
  → `roic_norm=1.0`（上限クランプ）で**算入**（2026-08-16時点で該当
  0件、実データ未検証）
- それ以外（`missing_cash_data`/`negative_invested_capital`/
  `no_operating_income`等、測定不能）→ **除外**（残り指標の重みを
  再正規化）

`gross_margin_3yr_avg`・`fcf_margin_3yr_avg`が欠損（None）の場合も
同様に除外・再正規化する。

**最低2指標ルール**: 有効指標（算入されるgm/roic/fcf）が2未満の場合、
`moat_score=0.5`（既存の全欠損時デフォルトと同じ値）にフォールバックし、
個別の指標norm値もNoneを返す。「薄い根拠から確信ありげな出力を出さない」
という原則を高スコア側・低スコア側の両方に対称的に適用する設計。

**RICE-1(`vc_factor`)への影響なし**: `_calc_roic_wacc_ratio()`の
戻り値をタプル化したが、RICE-1消費側（`financials["roic_wacc_ratio"]`
経由）は値のNone判定のみで動作するため、理由コード追加による既存挙動
への影響はない。

**消費者**: `core_calculator.py::calculate_pt()`（DCF計算、`phase1_years`
経由でメインIVに直結）・`stock.html`（`moat_phase1_years`表示）・
`index.html`（`moat_score`列のソート・`#avg-moat`全銘柄平均表示、
2026-08-16のStep1消費者確認で新規発見。いずれも表示専用でDCF計算への
フィードバックはない）。

**provenance（`moat_score_source`、2026-08-16追記）**: `moat_score=0.5`
は「実測の結果0.5」と「最低2指標ルールによる中立フォールバック
（プレースホルダ）」のいずれかでありうる。`MoatScoreResult.source`
（`"measured"`/`"neutral_fallback"`）・`.n_present`（有効指標数）を
`components.moat_score_source`/`components.moat_score_n_present`として
出力し区別する（`NAMING_CONVENTIONS.md`規則4、`operating_income_source`
と同型）。`index.html`の`#avg-moat`は`neutral_fallback`銘柄を平均計算
から除外する（プレースホルダを実測平均に混ぜないため）。検知は
CHECK-36（`report_consistency_check.py`、銘柄単位WARN＋対象数急増の
集計WARN）。

**BKNGの恒久的な注意事項**: BKNGは自己資本マイナス
（`negative_invested_capital`）でROIC測定不能・粗利率も欠損しており、
有効指標はFCFマージンのみ（`n_present=1`）のため`moat_score=0.5`
（`neutral_fallback`）が採用され続けている。2026-08-16時点でTANUKI
SCOREが`BUY`と判定されているが、**これは測定されたモート強度に基づく
ものではない**（BACKLOG_DONE.md「2026-08-16（完了）」
`[[MOAT-SCORE-PARTIAL-NULL-1]]`参照）。自己資本が大幅マイナスの状態
（大規模自社株買いの継続等）が解消されない限り、この状態は継続する
見込み。`moat_score`を人為的に調整することはしない
（測定不能を推測で埋めることは本項目で解消した`(値 or 0.0)`型の問題の
再発となるため）。

---

## データフロー（上流→下流）
【SECデータ取得層】
SEC EDGAR
└─ common/sec_data/update.py
├─ fetcher.py  # SECFetcher::fetch_company_facts()/fetch_submissions()。SEC EDGAR
│    Company Facts/Submissions APIへの生取得（update.py Step1が最初に呼ぶ）。
│    **追記（CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1 2026-07-20新設）**: 企業再編で
│    CIKが不連続になった銘柄（買収・スピンオフ・LBO等でSECが新CIKを発番し、
│    旧CIK側のcompany_facts.json取得が新CIK単独では最古年度が欠落する）向けに、
│    `common/sec_data/cik_history.json`（`{TICKER: {legacy_ciks: [...],
│    transition_note: "..."}}`）を`_load_cik_history()`で読み込み、
│    `_fetch_legacy_company_facts()`/`_fetch_legacy_submissions()`で旧CIKの
│    データも取得した上で`_merge_us_gaap_facts()`（静的メソッド）が
│    us-gaapタグ単位でマージする。`fetch_company_facts()`/`fetch_submissions()`
│    は保存前に本マージを実行。現在登録銘柄: MRVL/GOOGL/AVGO/DELL
│    （DELLはFY2013-2016非公開期間のフィリングギャップの注記あり）。
│    `registration_validator.py::check_p6_cik_discontinuity_candidate()`
│    （P6-CIKDiscontinuity、`CIK_DISCONTINUITY_CONFIRMED_STRUCTURAL`＝
│    {CEG,LITE,ABBV,GEV,SN,CON,VST}・境界年2010以降・売上5億ドル以上を
│    ヒューリスティックとする）が新規登録時にCIK不連続候補をWARN検知する。
│    **追記（FETCHER-10KT-10QT-FORM-EXCLUSION-1案③ 2026-08-01実装完了、
│    コミット1fd44fc0a）**: fetcher.py本体は無変更（案①のrelevant_forms
│    追加+バケツ再設計はコスト過大と判明し見送り確定）。代わりに
│    `report_consistency_check.py`にCHECK-28/WARN-28を新設し、
│    company_facts.json上のform=10-KT/10-QTのaccnが`accn_to_reportdate`に
│    未登録の場合を検知する（検知のみ、自動修正なし）。全105銘柄実行で
│    RCATにWARN-28が2件発火（10-KT accn`0001641172-25-001892`・10-QT accn
│    `0001554795-19-000269`〈2019年、RCAT第1回目の決算期変更に伴う移行期
│    四半期報告書〉）、他104銘柄で誤検知なし。単体テストは
│    `tests/test_report_consistency_check.py`。詳細はBACKLOG_DONE.md
│    [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]参照。
├─ quarterly.py      # 四半期データ取得・正規化
├─ normalizer.py     # フィールド正規化
├─ layer3_builder.py # Layer3（common/sec_data統合、config/sec_concept_definitions.json
│    〈全32フィールドの候補タグ・分類定義〉に基づきcompany_facts.json生データから
│    直接、統一snake_caseの銘柄別ストア（インメモリのみ、ファイル永続化なし）を
│    構築する`build_ticker_store()`が本体。update.py Step5で呼ばれ、戻り値は
│    そのままttm_calculator.py::calc_ttm_series()へ渡される（フェーズC対応、
│    2026-07-24〜。旧経路はnormalize()の戻り値を渡していた）。
│    Q4逆算（q4_implied.py::Q4_IMPLIED_FIELDS）・欠落四半期逆算
│    （MISSING_QUARTER_IMPLIED_FIELDS）・GrossProfitバックフィル
│    （`_backfill_gross_profit()`、インメモリのみで本番の`data/annual_*.json`
│    には反映されない＝[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]）・
│    ticker_overrides機構（`action: "exclude"`/`"override_concept"`、
│    `cross_filing_tags`によるNVDA向け複数タグ合算）を持つ。
│    `_get_concept_units()`は2026-07-30、[[LAYER3-COGS-ASTS-LRCX-
│    RECOVERABLE-FOLLOWUP-1]]対応で「名前空間:タグ名」形式（コロン区切り）の
│    企業固有拡張タグ参照に対応（現状利用箇所はゼロ、将来の再利用向けに保持）。
│    **追記（[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]① 2026-08-01
│    実装完了）**: 上記「インメモリのみで本番に反映されない」ギャップは、
│    parser.py側に独立の`_backfill_gross_profit_from_revenue_cogs()`を
│    新設し本番`data/annual_YYYY.json`への反映経路を追加したことで解消（34
│    銘柄342件是正）。本ファイルの`_backfill_gross_profit()`自体は無変更の
│    ままインメモリ専用として残置。詳細はparser.pyの項・BACKLOG_DONE.md参照。
│    **追記（LAYER3-SGA-Q4-MISSING-1 2026-07-30実装完了、コミットa7b840c32）**:
│    `selling_general_and_administrative`をQ4_IMPLIED_FIELDS・
│    MISSING_QUARTER_IMPLIED_FIELDS双方に追加し、42銘柄・171四半期のQ4
│    恒常欠落を解消。同コミットでLAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1
│    （Layer2新規6フィールドの投資調査）も完了し、他4フィールド
│    （short_term_investments/total_liabilities/eps_basic/eps_diluted）は
│    性質上Q4逆算・カットオフチェックの対象外と判定した（SGA/cost_of_revenue
│    向けの常設チェックは`newfield_q4_cutoff_check.py`、詳細は同ファイルの
│    項参照）。
│    **重要（3スキーマ併存の実態、詳細は`docs/architecture/new_data_platform/
│    SEC_EDGAR_LAYER_DESIGN.md`参照）**: 2026-07-30時点で
│    ①Layer3（本ファイル、snake_case・インメモリ）②`data/annual_*.json`等
│    （parser.py、snake_case・ファイル永続化）③`normalized/`
│    （normalizer.py、**PascalCase**・`quality_checker.py`/STONKS SILO
│    `financial_trend_calculator.py`が直接消費）の3スキーマが並行して
│    存在する。`config/sec_concept_definitions.json`は`common/sec_data/
│    tag_definitions.py::TAG_CANDIDATES`と一部フィールド（例:
│    research_and_development、[[JNJ-RD-TAG-PRIORITY-1]]で発見）で
│    独立した候補タグリストを保持しており、片方のみ修正すると優先順位が
│    3スキーマ間で乖離する（[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]参照）。
│    フェーズD（本体consumer切替、対象優先順位: ①TANUKI VALUATION本体
│    ②STONKS SILO ③TANUKI TAIL ④HypeCore ⑤stock.html）着手前提条件は
│    `SEC_EDGAR_LAYER_DESIGN.md`「フェーズD」節・`MIGRATION_CHECKLIST.md`参照。
│    **追記（フェーズD Step2-1 2026-08-06実装完了）**: ①TANUKI
│    VALUATION本体（`pipeline.py`）のLayer3切替が完了し、`normalized/`を
│    直接参照する箇所はゼロになった（希薄化率・TTM信頼性判定・LTDebt
│    フォールバック・`_estimate_ttm_operating_income()`・
│    `_calc_moat_inputs()`の6箇所を`layer3_builder.py::get_field_
│    entries()`経由に統一）。事前に`[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]`
│    （本節391行目で言及した3スキーマ間乖離）・`[[LAYER3-ANNUAL-
│    MISCLASSIFICATION-BBAI-1]]`を先行修正済み。
│    詳細はBACKLOG_DONE.md「2026-08-06（完了）」
│    [[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP2-1]]参照。
│    **追記（フェーズD 2026-08-07実質完了・最終状況）**: ②STONKS SILO
│    〈`financial_trend_calculator.py`〉・③TANUKI TAIL
│    〈`quarterly_review_generator.py`・`tail_dcf_bridge.py`〉・
│    ④HypeCore〈`hypecore.py`〉のLayer3切替が完了（それぞれ
│    `[[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP2-2]]`〜`[[SEC-EDGAR-LAYER-
│    DESIGN-PHASE-D-STEP2-4]]`参照）。⑤stock.html＋診断・補助
│    スクリプト7件（Step2-5）は投資調査の結果、Layer3切替の実質対象が
│    `dcf_validity_checker.py::check_c_data_jump()`のみと判明し実装
│    不要（実質完了扱い）。本節384行目の「`quality_checker.py`/STONKS
│    SILO`financial_trend_calculator.py`が直接消費」という記述は
│    **陳腐化**（`quality_checker.py`はrepo全体でimportゼロの死蔵
│    コード`[[QUALITY-CHECKER-CLEANUP-1]]`と判明、
│    `financial_trend_calculator.py`はStep2-2でLayer3へ切替済み）。
│    **`normalized/`の残存消費者は以下3系統に確定・恒久化**（いずれも
│    設計判断により現状維持を選択、着手見送り確定）:
│    - `discover/stonks-silo/src/fetcher.py`（`data/annual_*.json`
│      直読み、`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`
│      案2採用）
│    - `common/screening/dcf_validity_checker.py::check_c_data_jump()`
│      （同上、`report_consistency_check.py`WARN-21として本番稼働中）
│    - `docs/value-monitor/tanuki_valuation/stock.html`（ブラウザJSが
│      `normalized/`を直接fetch、`[[STOCKHTML-LAYER3-PUBLISH-
│      PIPELINE-MISSING-1]]`によりLayer3公開パイプライン未整備のため
│      着手不可）
│    **フェーズE（`normalized/`完全廃止）は上記3系統が存続する限り
│    着手不可**と判定（詳細はBACKLOG.md`[[SECDATA-STORAGE-
│    FRAGMENTATION-1]]`参照）。
│    **罠防止（[[SCHEMA-NORMALIZED-ISSUES-1]]⑤、2026-09-23）**:
│    `normalized/{TICKER}_quarterly_normalized.json`はファイル名に
│    "quarterly"と明記されているが、実際は`is_annual: true`エントリ
│    としてannualデータも同一ファイル内に混在保持している
│    （`quarterly.py::build_raw_table()`が四半期・年次両方を同一
│    fieldsに格納するため）。ファイル名から「四半期データのみ」と
│    誤推測しないこと。
│    **追記（DOCS-SECDATA-NORMALIZED-DIR-STALE-1 2026-07-30実装完了、
│    コミット5ee157c6b）**: GitHub Pages公開フロントエンド（stock.html）向け
│    公開コピー`docs/common/sec_data/normalized/`が2026-05-23以降同期
│    されず、本家`common/sec_data/normalized/`（③、上記）から約2.2〜2.3
│    ヶ月・55銘柄分乖離していた。選択肢A+B併用で解消: (A)
│    `src/tail/quarterly_review_generator.py`・`src/tail/tail_dcf_bridge.py`
│    のCOMMON_NORMALIZED_DIR定数を本家`common/sec_data/normalized/`へ
│    redirect（TANUKI TAIL側は公開コピーではなく本家を直接参照するよう変更）、
│    (B)`.github/workflows/SEC_Data_Update.yml`に本家→公開コピーへの
│    `rsync --delete`同期ステップを新設（`.gitattributes`に
│    `merge=ours`設定も追加）。初回手動同期で105銘柄を本家と完全一致させた。
│    TANUKI TAIL 10ポジションでの新旧比較: ADBE/APGEはファイル不在→実データ
│    復帰、NVDAは参照四半期が更新されoperating_margin等が変化（残り7銘柄は
│    変化なし）。詳細はBACKLOG_DONE.md
│    [[DOCS-SECDATA-NORMALIZED-DIR-STALE-1]]参照。
├─ ttm_calculator.py # TTM系列計算。本番経路は calc_ttm_series()/save_ttm_series()
│    のみ（update.pyが呼ぶ）。**フェーズC対応（2026-07-24〜）**: 入力元は
│    旧`normalize()`の戻り値から`layer3_builder.py::build_ticker_store()`の
│    戻り値（Layer3、インメモリ）に切替済み。フィールド抽出は
│    `layer3_builder.py::get_field_entries()`経由。FLOW_FIELDS（4Q合算）のみを処理し、STOCK_FIELDS/
│    SHARES_FIELDS（Cash/STDebt/LTDebt/DeferredRevenue/Equity/Assets/
│    SharesBasic/SharesDiluted）は処理しない。
│    **追記（TTM-STOCK-FIELDS-DEAD-1 2026-07-18・対応方針a完了）**:
│    STOCK_FIELDS/SHARES_FIELDSを実際に処理していたcalc_ttm()/save_ttm()
│    （{ticker}_ttm.json生成）は2026-05-07のcalc_ttm_series()追加以降
│    用途を失った到達不能コードだったため削除した（calc_ttm()専用の補助関数
│    `_make_q4_implied_output()`・`_latest_end()`・`_calc_burn_rate()`、
│    および呼び出し元ゼロの孤立関数`_calc_q4_implied()`も連動して削除。
│    `_build_q4_quarterly_entries()`はcalc_ttm_series()が使うため維持）。
│    STOCK_FIELDS/SHARES_FIELDS定数自体は、下記のvalidate_field_classification()
│    契約チェックを維持するため削除せず残置（EXCLUDED_FIELDSへ統合すると
│    「意図的除外」の意味が変質するため）。CurrentAssets/CurrentLiabilities
│    を含む8メンバー全て、本番のcalc_ttm_series()経由の_ttm_series.jsonには
│    引き続き反映されない（LTDebt/SharesBasic/SharesDilutedのみ別実装で
│    個別生存）。孤立データファイルcommon/sec_data/ttm/NVDA_ttm.json
│    （2026-05-11の2分間の誤呼び出しの残存物）も削除済み。
│    EXCLUDED_FIELDS（_COGS・RPO）新設・FIELD_CONCEPTS全キーの分類網羅性を
│    モジュールロード時に検証する契約チェック（contracts.py::
│    validate_field_classification()）も同時に追加済み。
├─ parser.py         # XBRL解析
│    **追記（CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1 2026-07-20新設）**:
│    `common/sec_data/fact_overrides.json`（`{TICKER: {YEAR: {reason, fields:
│    {field: {value, ...}}}}}`）による年度・フィールド単位の個別上書き機構を
│    追加。本人データ優先ロジック自体は変更せず、`_apply_fact_overrides()`が
│    `save_parsed_data()`内でannual/quarterly保存直前の最終ステージとして
│    特定ticker+year+fieldのみ明示的に差し替える（GOOGL FY2012/2013の非継続
│    事業区分変更に伴う遡及修正値など、本人データ優先では当初申告値のまま
│    残ってしまうケース向け）。ファイル不在時は空dictで無効化。
│    **追記（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]] 2026-08-05新設）**:
│    `common/sec_data/fixed_registry.json`（`{TICKER: {YEAR: {fixed_at,
│    fixed_by, verified_against, fields_snapshot, snapshot_hash}}}`）による
│    「フィックス」機構を追加。`fact_overrides.json`（値の個別上書き）とは
│    役割が異なり、`_apply_fixed_registry_freeze()`が`_apply_fact_
│    overrides()`・各種逆算バックフィルより後・`result["annual"]`組み立て
│    直後に、`fields_snapshot`記録済みフィールドを既存`annual_{year}.json`
│    の値へ強制復元する（差分適用方式、新規フィールドのみ通常抽出を通す）。
│    2026-08-05時点でStage1（26銘柄・372エントリ、`fixed_by:
│    checkgate_pass`）・Stage2（12銘柄・17エントリ）・Stage3a
│    （MO/PM/LLY 3銘柄・31エントリ）・Stage3b（SCCO/RDW/ASTS
│    3銘柄・12エントリ）まで累計**44銘柄・432銘柄×年度
│    エントリ**登録済み（Stage2以降は`fixed_by: manual_verification`）。
│    CI側は`report_consistency_check.py`のCHECK-31/WARN-31がsnapshot_hash
│    不一致をNG検知。quarterly/TTM側（layer3_builder.py）は対象外
│    （[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]と同じ独立パイプライン構造の
│    ため）。**新規登録時は必ずannual_{year}.json実ファイルでfields_
│    snapshot対象フィールドの現存を確認すること**
│    （CLAUDE_CODE_START.md該当節・BACKLOG_DONE.md「2026-08-05（完了）」
│    Stage2/Stage3エントリ参照。BACKLOG_DONE.mdの過去記述と実データが
│    後続タスクにより乖離しうるため）。
│    **追記（[[SECDATA-STORAGE-FRAGMENTATION-1]] 2026-08-05実装完了、
│    新DB構築プロジェクト フェーズ1 Step1本線）**: `data/quarterly_
│    {FYQ}.json`のpl/cf/shares区分が従来XBRL申告のYTD累積値のまま
│    保存されており（約65〜66%のエントリが該当）、annual側とは異なり
│    正規化されていなかった問題を修正。`quarterly.py::
│    _classify_period()`・`normalizer.py::_ytd_to_quarterly()`
│    （normalized/側で実績のあるロジック）を再利用する統一アルゴリズム
│    （SA〈単一四半期〉候補優先→YTD差分計算フォールバック→加重平均
│    フィールドは差分計算対象外でnull許容）を`parse_company_facts()`の
│    四半期抽出ループ（`_extract_values_merged()`・
│    `_extract_single_key()`）に実装。annual側ロジックは無変更
│    （1,441ファイル横断比較で差分0件）。RCAT 2016Q3の1ファイルのみ
│    四半期キー自体が新抽出結果から消滅し旧ファイルが未上書きのまま
│    残存（[[RCAT-2016Q3-ORPHANED-QUARTERLY-FILE-1]]）。併せて
│    `common/sec_data/raw/`（`quarterly.py`書き込み専用・実消費者ゼロの
│    デッドコード）を削除済み（Step1で全消費者洗い出し済み）。
├─ tag_definitions.py  # XBRLタグ候補の共通定義（TAG_CANDIDATES。quarterly.py・parser.py
│    双方が参照。9概念のみ統合済み、LTDebt/SM/DA/RPO/Revenueは意図的に未統合。
│    LLY-CAPEX-STALE-1 Phase 2a 2026-07-12新設）
│    **注意（[[JNJ-RD-TAG-PRIORITY-1]] 2026-07-30）**: `config/
│    sec_concept_definitions.json`（Layer3、layer3_builder.pyが参照）は
│    本ファイルとは独立した候補タグリストを保持する。両タグが並存報告
│    される銘柄（研究開発費のResearchAndDevelopmentExpense vs
│    ExcludingAcquiredInProcessCost等）で優先順位を修正する際は、本ファイル
│    だけでなくLayer3側の候補順序も同時に確認すること
│    （[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]は本ファイルのみ修正し
│    Layer3側が未修正のまま残っている既知の実例）。
├─ contracts.py  # 正規化契約の型（QUALITY-GATES-EPIC-1 Gate2 Phase 3a 2026-07-13新設）
│    FinancialEntry/EntryProvenance/FCFSeries。quarterly.py::save_raw_table()・
│    normalizer.py::save_normalized()がjson.dump()直前にFinancialEntryで
│    エントリ形状を検証（違反時ContractViolation、既存try/exceptで捕捉）。
│    **追記（GATE2-PHASE3B-1② 2026-07-17）**: validate_field_classification()
│    新設（規約C）。field_concepts辞書と分類セット（frozenset）を引数で
│    受け取る汎用設計とし、quarterly.py/ttm_calculator.pyのどちらも
│    importしない（contracts.pyは既にquarterly.pyからimportされているため、
│    逆方向の依存を追加すると循環importになる。呼び出し元のttm_calculator.py
│    側でFIELD_CONCEPTS・FLOW_FIELDS等の具体的な値を渡す設計で回避）。
│    **追記（GATE2-PHASE3B-1③-a 2026-07-17）**: GrowthVerdict(str, Enum)
│    新設（規約D）。src/value/tanuki_valuation/growth_sanity.py::
│    check_growth_sanity()のverdict（PLAUSIBLE/REVIEW/AGGRESSIVE/
│    FLOOR_HIT_REVIEW）を型で表現する。Python 3.11+のEnum仕様変更
│    （str,Enum継承でも__str__がデフォルトでクラス名付き表記
│    "GrowthVerdict.PLAUSIBLE"を返す）に対応するため__str__をoverrideし
│    self.valueを返すようにしている。enum.StrEnum（3.11+限定）は
│    pyproject.tomlのrequires-python=">=3.10"と不整合のため不採用。
│    data_fetcher.py同様、growth_sanity.py側もsys.path解決を自前で
│    行いcommon.sec_data.contractsをimportする（growth_sanity.pyは
│    src/value/tanuki_valuation/配下でパッケージ化されておらず
│    bareモジュールとしてimportされるため）。
│    **追記（GATE2-PHASE3B-1③-b 2026-07-18）**: Classification(str, Enum)
│    新設（規約D）。pipeline.py::classify()のscore（BUY/WATCH/HOLD/TRIM/
│    GROWTH_PREMIUM/SELL/PASS）を型で表現する。GrowthVerdict同様
│    __str__をoverride。json.dumps()はstr継承のisinstance高速パスにより
│    __str__override前でも素の文字列でシリアライズされることを確認済み
│    （f-string/str()のプロトコルとは別経路のため無関係）。影響が及ぶのは
│    pipeline.py内でf-string補間される箇所のみで、特にreport.txt生成の
│    f"Classification: {score}"（1221行目）はreport_consistency_check.py
│    のNG-3（LOW丸め未発動）がこの行をregexで再パースして比較するため
│    最重要（__str__override漏れがあるとNG-3が全銘柄で誤発火する）。
│    daily_pick.py（ELIGIBLE_CATEGORIES/CATEGORY_PRIORITY）・フロントエンド
│    （tanuki_score/index.htmlのCAT_META/CAT_COLOR辞書等）はJSON経由で
│    文字列を受け取るのみのため無改修で動作。
│    quarterly.py::_select_best_candidate()のフォールバック採用時・
│    ticker_restrictionsオーバーライド採用時に_provenance.source_tagを付与。
│    FCFSeriesはdata_fetcher.py::TTMReader.get_fcf_series()内でのみ使用し
│    （JSONシリアライズ不可のため.as_list()で境界越えさせる）、fcf_listの
│    新しい順規約をconstruction時に検証する。parser.py・ttm_calculator.py・
│    reader.py::get_fcf_list()は未対応（Phase 3bで判断、[[GATE2-PHASE3B-1]]・
│    [[GATE2-READER-FCFLIST-1]]参照）
│
│    **注意（2026-07-16）**: parser.py側にも別設計の独自provenance機構が
│    追加された（ARCH-DATA-1ステージ1、`{bs,pl,cf,shares,other}_provenance`）。
│    これはannual_{YEAR}.jsonの各フィールド単位でaccn/filed/is_own_data
│    （reportDate==end_date本人データ判定の結果）を記録するもので、
│    上記contracts.py（EntryProvenance.source_tag/duration_days、
│    quarterly.py/normalizer.py向け・エントリ単位）とは設計思想・
│    対象データ・粒度が異なる別の仕組みである。同じ「_provenance」という
│    語で2つの異なる機構を指すため、混同しないよう注意すること。
│    **追記（2026-07-17・ARCH-DATA-1ステージ3）**: 同provenanceに`fy_tag`
│    （採用エントリの生XBRL fyタグ値）を追加。年度バケツキー（determine_
│    fiscal_year()計算結果）と食い違う場合、`_extract_values_merged`/
│    `_extract_values_best_candidate`が`common/sec_data/data/{ticker}/
│    fy_tag_mismatch_log.json`に記録する（`is_own_data=True`限定。
│    比較年度再掲エントリ〈is_own_data=False〉のfyタグ不一致は正常仕様
│    のため対象外——全105銘柄検証で対象外にする前は4,434件・105銘柄という
│    ノイズになることが判明したための設計）。report_consistency_check.py
│    のCHECK-23/WARN-23が読み取り、既存のCHECK-22（fy_collision_log.json、
│    同一fyタグへの複数本人end_date競合）とは独立した別軸のチェックとして動作する。
│    **追記（FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1 2026-07-19）**: CHECK-22
│    （同一fyタグ前提）・CHECK-23（勝者自身のfyタグとバケツの不一致、敗者側は
│    対象外）のいずれにも該当しない第3の軸として、CHECK-24/WARN-24を新設。
│    決算期変更の境界年で、生fyタグ・end_dateの両方が異なる2エントリ（本人
│    データ側と非本人データ側）が同一年度バケツ（computed_year）で競合する
│    ケース（RCAT型、決算期を2回変更）を検知する。`_own_override_is_safe()`
│    自体は変更せず、`_extract_values_best_candidate`/`_extract_values_merged`
│    内の呼び出し前後で新設した`_is_boundary_collision()`（純関数）が判定する。
│    「生fyタグ・end_dateが異なる」だけでは不十分（ADSK/AVAV/CRM/CAKE等の
│    固定決算日企業で「同一(月,日)・隣接暦年」の組み合わせ＝fyタグが実際の
│    期間より1年ずれるWARN-23既知パターンが頻出し、実装中に7銘柄で誤発火する
│    ことが判明した）ため、`_fiscal_anchors_far_apart()`（(月,日)の循環距離が
│    30日超かを判定。`fye_change_candidate_scan.py`のMIN_CLUSTER_DISTANCE_DAYS
│    と同じ閾値）を追加の必須条件とし、決算日そのものが動いたケースのみに
│    限定した。検知結果は`common/sec_data/data/{ticker}/fye_boundary_
│    collision_log.json`に記録（fy_collision_log.json等と同一パターン、
│    0件でも毎回書き込み）。全100銘柄再生成でRCAT/LITE/WSTの3銘柄が該当
│    （事前調査時点の想定はRCATのみだったが、LITE/WSTは単発の孤立した
│    比較年度エントリで`override_applied=true`＝実害なしと確認済み。詳細は
│    BACKLOG_DONE.md [[FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1]]参照）。
│    併せて、候補抽出用の統計的シグナル（`_cluster_fiscal_anchor_candidates()`
│    再利用、support≧2クラスタ・循環距離30日超）を`common/sec_data/
│    fye_change_candidate_scan.py`として独立ツール化した。WARN-24本体の
│    常設発火条件としては採用していない（誤検知率が高く、過去4候補中
│    RCAT以外は実害なしと判明したため）。新規銘柄登録時・定期監査時の
│    手動実行による候補洗い出し補助という位置づけに限定する。
│    **追記（FY-COLLISION-LOG-NONDETERMINISTIC-1 2026-07-20）**: CHECK-22
│    （`fy_collision_log.json`）は`_extract_values_best_candidate()`が
│    フィールドの候補XBRLタグごとに`_collect_own_data()`を独立呼び出しする
│    設計のため、複数の候補タグが同一(fy, end_dates)衝突を独立に検出すると
│    内容が完全同一のエントリが候補タグ数だけ重複する決定的なバグがあった
│    （AVAV/CAKE/COHR/CRM/FCX/FICO/HONで実在確認済み、実行のたびに増殖
│    するのではなく毎回同じ件数だけ重複）。値の採用ロジックには影響しない
│    ため、根本原因（候補タグループ）には手を入れず、
│    `_save_fy_collision_log()`側で(field, fy, end_dates, resolution)を
│    キーに重複排除するガードを追加する対症療法で対応した。詳細は
│    BACKLOG_DONE.md [[FY-COLLISION-LOG-NONDETERMINISTIC-1]]参照。
│    **追記（2026-07-18・ARCH-DATA-1残課題④）**: `_collect_own_data_annual()`は
│    start_date必須フィルタを持つためBS項目（instant fact）を常に除外していた
│    （本人データ判定の対象外）。`_collect_own_data_instant()`を新設し
│    （start_date/期間長フィルタなし版）、`INSTANT_FACT_FIELDS`（BS9項目+rpo）を
│    `_collect_own_data()`ディスパッチャで振り分け。`_own_override_is_safe()`に
│    `is_instant`引数を追加し、「同一end_date→上書き安全」ショートカットを
│    instant factでは無効化（duration factでは同一end_dateの2候補タグが
│    同一概念の別名表記である前提が成立するが、instant factのBS項目は
│    同一年度なら別概念のタグでもend_dateが機械的に一致するため。VZの
│    short_term_debtがShortTermBorrowings＝短期借入金でLongTermDebtCurrent
│    ＝長期債務流動化分を誤って上書きする回帰を実装中に検出・修正）。
│    **追記（TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1 2026-08-01実装完了、
│    コミットee46018b2）**: `_backfill_total_liabilities_via_identity()`新設。
│    `XBRL_MAPPING["total_liabilities"]`の2番目の候補タグ
│    `LiabilitiesAndStockholdersEquity`（定義上total_assetsと数学的に一致する
│    誤った代替タグ）が誤採用された年度をtotal_liabilities==total_assetsという
│    数学的シグネチャで検知し、貸借対照表恒等式（total_assets-
│    stockholders_equity）で逆算した値に置き換える。105銘柄中278件（銘柄年度、
│    AMZN/GOOGL/MSFT/NVDA等の大型株含む）を是正、全105銘柄フローズン入力比較で
│    対象外無変化を確認済み。
│    **追記（PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1案b 2026-08-01実装完了、
│    コミットb756021f6・ゲート追加9616e8058）**: `_align_cost_of_revenue_to_
│    revenue_period()`新設。revenueとcost_of_revenueが異なるaccnから独立
│    採用されている年度についてのみ、revenueと同一accn・同一期間を持つ
│    cost_of_revenue候補タグが存在すればそちらを優先採用する（欠損穴埋め型の
│    ゲート条件）。検証時の実データ確認で発見した「数学的矛盾がない場合は適用
│    しない」ガードを追加（巻き添え防止）。LRCX(2010)のみ是正、CRM(2013)・
│    JNJ(2017)・MRVL(2017)・ONDS(2017)は本案単独では未解決のまま残存
│    （案a・案cはゲート条件込みの再設計が必要、詳細はBACKLOG.md参照）。
│    **追記（LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1① 2026-08-01実装
│    完了、コミットdc0507c27）**: `_backfill_gross_profit_from_revenue_cogs()`
│    新設。標準タグからgross_profitが取得できない年度のみ、
│    revenue-cost_of_revenueで逆算した値を本番data/annual_YYYY.jsonへ
│    書き戻す（欠損の穴埋めのみ、既存の正しい値は上書きしない）。34銘柄342件を
│    是正。従来layer3_builder.py::_backfill_gross_profit()はインメモリのみで
│    本番ファイルに反映されない設計だった（同ファイル該当箇所参照）が、本対応で
│    parser.py側から独立に本番ファイルへの反映経路を新設した。
│    **追記（SPAC-SHELL-BS-ENTITY-MIXING-1段階1・段階2 2026-08-01実装完了、
│    コミット段階1 4f9588fb1・段階2 1f6e95d92）**: `_resolve_bs_entity_
│    mixing()`新設。SPAC合併等により同一年度のBS(instant fact)フィールドが
│    異なる法的実体（accn）から混在採用されているケースを是正する（実例:
│    BBAI/RDW/RKLB/SOFI/VRT/ONDS/KULR(2016)、段階2追加によりSPIR(2020)も対象）。
│    「①複数accnが混在」「②本人データaccnが単一に定まる」「③数学的矛盾が
│    確認できる、または③'アンカー候補accnのreportDateが法人名変更履歴
│    〈former_names〉のいずれかの区間内にある（段階2、SPAC合併疑いの機械的
│    検知）」「④アンカー統一で実際に矛盾が解消する」の4条件をすべて満たす
│    年度のみ、本人データaccnをアンカーとして採用し他accn由来の値をNone化する
│    （減算的設計、既存の正しい値を上書きする経路を持たない）。105銘柄・87件の
│    複数accn混在ケースへのシミュレーションで矛盾のない56件（41銘柄）・
│    KULR(2019)に無影響を確認済み。段階2で発火した年度の詳細は新設
│    common/sec_data/data/{ticker}/spac_shell_detection_log.json
│    （_save_spac_shell_detection_log()が保存、0件でも毎回書き込みの化石
│    ファイル対策パターン）に記録。詳細はBACKLOG_DONE.md
│    [[SPAC-SHELL-BS-ENTITY-MIXING-1]]参照。
├─ utils.py  # determine_fiscal_year() — 年度判定共通関数（ARCH-DATA-1-FY 2026-06-25）。
│    ARCH-DATA-1ステージ2（2026-07-17）でアンカー日ウィンドウ方式に刷新:
│    end_date.month > fiscal_end_monthの片方向月比較を廃し、
│    detect_fiscal_end_month()（会計年度末月検出。parser.py・
│    extract_key_facts.py双方が参照する統一関数）・detect_fiscal_anchor_date()
│    （決算アンカー日〈月+日〉検出。年境界〈Dec31/Jan1〉をまたぐ(月,日)分布を
│    _cluster_fiscal_anchor_candidates()で循環クラスタリングしてから中央値/
│    最頻値を採用——JNJ/TDY型の52/53週企業で完全一致最頻値だと企業自身の
│    fyタグと矛盾する誤判定が起きることが実データ検証で判明したための設計）
│    を新設し、determine_fiscal_year()はend_date.yearを中心とした
│    [year-1,year,year+1]の3候補年度とアンカー日との日数差で判定する方式に
│    変更。最小日数差60日超はWARN+月のみ比較にフォールバック（安全弁）。
│    呼び出し元8箇所（parser.py 4箇所・extract_key_facts.py 4箇所）に
│    anchor_month/anchor_day引数を追加。parser.py::_own_override_is_safe()の
│    条件2（12月決算企業で機能しない欠陥があった月のみ比較の事前フィルタ）も
│    同時に統一版determine_fiscal_year()呼び出しへ置き換えた
│    quarters_in_trailing_window()も同ファイルに追加（ARCH-DATA-1残課題① 2026-07-15新設）
│    ——会計年度end日起点のtrailing 370日窓で四半期エントリを抽出する共有関数。
│    quarterly.py::check_revenue_quality()・src/value/tanuki_valuation/pipeline.py
│    （DILUTION-FYE-1、希薄化率の分割検知）双方が参照し、暦年グルーピングの
│    重複実装（CHECK-QREV-FYE-1型バグの再発）を解消した
│    **追記（ELF-FISCAL-END-MONTH-MISDETECTION-1案② 2026-08-01新設、
│    コミット499b4d478）**: detect_fiscal_anchor_clusters(us_gaap,
│    candidate_keys, min_support=2)新設。detect_fiscal_anchor_date()と同一の
│    (月,日)クラスタリング結果のうち、主anchor（最大クラスタ）以外で合計得票が
│    min_support以上の「有意な」クラスタのアンカー日を、era別決算期変更の
│    追加候補（determine_fiscal_year()のextra_anchors引数）として返す。単一
│    クラスタしか存在しない銘柄（実測105銘柄中100銘柄）は必ず空リストを返す
│    設計（加算的操作のみ、既存の正しい判定を上書きする経路を作らない）。
│    ELFの2015-2018年度データ是正に使用。詳細はBACKLOG_DONE.md
│    [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]参照
├─ revenue_tag_conflict_check.py  # revenue系タグ競合検知（ARCH-DATA-1残課題③
│    2026-07-15新設）。company_facts.jsonを再読込し、MERGE_ALL_TAGS_FIELDS対象
│    （revenue/selling_and_marketing/depreciation_and_amortization）の候補タグ間で
│    同一年度の値が閾値以上乖離していないかWARN検知する。parser.py本体は無変更、
│    SECParserの既存メソッドを再利用（新規の候補タグ一覧は作らない）。
│    update.py Step1完了直後（check_revenue_quality()の直後、4c.相当）に配線。
│    自動修正は行わない（人間がTICKER_RESTRICTIONS登録可否を判断する既存フロー
│    に委ねる）。詳細はBACKLOG.md [[REVENUE-TAG-CONFLICT-SCAN-1]]参照
├─ fact_selection.py  # fact競合解決の共通プリミティブ（EPS-ANALYZER-NORMALIZE-
│    SCOPE-1 2026-07-20新設）。`select_latest_filed(candidates)`——同一期間
│    （同一end_date等）に複数XBRL factが競合する場合に「filed日が最新のものを
│    優先する」という単一規則のみを担う末端プリミティブ。quarterly.py
│    （`_process_entries`/`_select_best_filing`）とsrc/value/adjusted_eps_analyzer/
│    extract_key_facts.py（SPLIT-AUTO-CHECK-1）がそれぞれ独立実装していた
│    同一ロジックをここに集約。parser.py本体（本人データ優先・fy一致等の
│    多段規則）は対象外
├─ newfield_q4_cutoff_check.py  # LAYER3-TTM-REGRESSION-NEWFIELD-BLINDSPOT-1
│    対応の常設チェックツール（2026-07-30新設、コミットa7b840c32）。現行の
│    TTM回帰比較（旧ttm/データのキーを起点に新旧を突合する設計）は、旧
│    パイプラインに存在しなかった新規フィールドを検証対象外にしてしまう
│    構造的欠陥がある。selling_general_and_administrative・cost_of_revenue
│    の2フィールドについて、①Q4欠落チェック（年次エントリのFY窓内に
│    Q1〜Q3が揃っているのにQ4が存在しないケースを検出、LAYER3-SGA-Q4-
│    MISSING-1型の再発検知）・②カットオフチェック（非12月決算企業の単
│    四半期エントリのperiod_daysがlayer3_builder.py::_is_plausible_
│    standalone_quarter()の妥当範囲〈75〜100日〉に収まっているか確認）を
│    行う。対象2フィールドに限定する理由はq4_implied.pyのモジュール
│    docstring参照
└─ segment_fetcher.py  # セグメント別売上・KPI取得（SEC EDGARセグメント
     報告、ASC280）。コンテキスト境界跨ぎ誤マッチ防止・金融業向けタグ2件を
     保持する。**追記（SEGMENT-FETCHER-DUPLICATE-ORPHAN-1 2026-07-30実装
     完了、コミット0e60ee255）**: `src/value/tanuki_valuation/
     segment_fetcher.py`（機能的下位互換の重複ファイル）を削除し本ファイルへ
     一本化。両ファイルとも他モジュールからimportされておらず影響範囲は
     ゼロだった。削除前に旧ファイル側のみに存在したXBRL値スケール
     （decimals=-6）に関する補足コメントは本ファイルへ移植済み。セグメント
     データ手動取得スクリプト自体は、銘柄新規登録が原則Claude Code経由と
     なったため現状は使用しない（将来的な再検討の余地は残す）。詳細は
     BACKLOG_DONE.md [[SEGMENT-FETCHER-DUPLICATE-ORPHAN-1]]参照

【EPS ANALYZER 独自抽出パイプライン（common/sec_data/とは完全に独立・2026-07-12訂正）】
`src/value/adjusted_eps_analyzer/extract_key_facts.py`はSEC Company Facts APIを
都度ライブ取得する**独自の抽出パイプライン**であり、上記`common/sec_data/`配下の
quarterly.py・parser.py・tag_definitions.pyは一切importしていない（importは
`common.sec_data.utils`の`determine_fiscal_year`・`detect_fiscal_end_month`・
`detect_fiscal_anchor_date`の3関数のみ、ARCH-DATA-1ステージ2 2026-07-17時点）。
自前で持っていた会計年度末月検出ロジック（旧`determine_fiscal_year_end()`、
10-K/Aを含むstartswith判定でparser.py側とは基準が異なっていた）は削除し、
統一関数を参照するよう変更済み。ローカルraw JSONキャッシュも
持たない。Phase 2a（タグフォールバック選定ロジック統一）の対象範囲外であり、
恩恵を受けていない点に注意（旧SYSTEM_MAP記載が`common/sec_data/`ツリーの一部
であるかのような誤解を招く配置だったため2026-07-12訂正）。
- `extract_quarterly_facts()`: 株数4段フォールバック（quarterly.json生成。
  ①EarningsPerShareDilutedからのEPS逆算 ②Basic株数代用 ③隣接する実四半期
  からの引き継ぎ〈`_neighbor_quarter_diluted_shares()`、ASTS-SHARES-
  OSCILLATION-1 2026-07-13新設〉④yfinance現在株数代入〈全期間タグ欠落
  銘柄・Visa等限定〉。③新設前は②で埋まらない四半期に無条件で④が適用され、
  スクリプト実行時点の現在株数が過去の四半期に逆行伝播していた
  〈ASTS/AVAV/RCAT/CART/CEG/BROS/GEV/XOM/CONの9銘柄で実害確認〉）
- 同一期間に複数fact（原初filed値と後年10-Kの比較年度再掲値）が競合する場合、
  「filed日が最新のものを優先」に統一済み（SPLIT-AUTO-CHECK-1 2026-07-12完了。
  以前はQ1〜Q3が末尾勝ち・Q4が先頭勝ちで不整合、NVDA等の分割前後で分割前株数が
  残存する実害があった。SEC自体にfactが1件も存在しない期間＜分割直後〜翌年
  10-K再掲まで＞は原理的に是正不能な残存ギャップあり、詳細はSPLIT-REALTIME-GAP-1
  参照）
  **追記（SPLIT-REALTIME-GAP-1 2026-07-20完了）**: 上記の「再掲機会が一度も
  ない先頭ブロックが恒久固着する」ギャップに対し、`pipeline.py:140-205`の
  `load_split_history()`/`apply_split_adjustments()`（`config/split_history.yaml`
  への個別登録＋post-split四半期平均の1.5倍を閾値とした遡及補正、NOWで
  既に実績あり）を流用して対応。NVDA単独の想定から、全101銘柄の横断スキャン
  でAVGO/CPRT/WMT/LRCX/CELHの5銘柄にも同型ギャップを確認し登録を拡大。
  KLAC（2026-06-12分割済みだがpost-split四半期データ未到達）は事前登録のみ
  （`post_split_shares`空リストで安全にno-op）。RCATは分割自体が存在せず
  （2019年以降無分割、SEC XBRL確認済み）、往復変動の正体は
  `_neighbor_quarter_diluted_shares()`（[[ASTS-SHARES-OSCILLATION-1]]）が
  RCAT自身の四半期単位XBRL開示欠落を埋めていただけと判明したため対象外。
  KULR/SPIRのリバース分割（1-for-8）の同型ギャップは
  [[SPLIT-REALTIME-GAP-REVERSE-1]]（2026-09-25完了）で実在を確認し、
  判定を方向非依存化（common/sec_data/split_adjust.py、EPS ANALYZERと
  TANUKIの3年希薄化率で共通）してKULR・SPIR・HON（1-for-2）・BKNGを登録。
  詳細はBACKLOG_DONE.md [[SPLIT-REALTIME-GAP-1]]・
  [[SPLIT-REALTIME-GAP-REVERSE-1]]参照
     ↓ TTMデータ（JSON）
【バリュエーション計算層】
├─ common/sec_data/reader.py::SECReader.get_net_cash()  # BS項目（Cash/ST_Invest/
│    LTDebt/STDebt）の同一時点原則統合＋Insurance/Fintechセクターガード適用。
│    data_fetcher.pyから呼ばれ、calculator/adjustments.py::calculate_bs_adjustment()
│    経由でvaluation["bs_adjustment"]として保存される（ARCH-DATA-1残課題①
│    2026-07-15でnet_debt_periodフィールドを追加、pipeline.py側の重複実装を解消）
│    **追記（GATE2-PHASE3B-1① 2026-07-17）**: モジュールレベル汎用アクセサ
│    get_quarterly_series(normalized, field_name)（is_annual・is_ytd両方を
│    除外した四半期エントリをend日昇順で返す）・get_latest_quarterly(normalized,
│    field_name)（その最新1件、空ならNone）を新設。戻り値は素の辞書のまま
│    （dataclass化は見送り）。get_rpo_context()内の既存_q_sorted()
│    （is_annualのみ除外・is_ytdは除外していなかった）をget_quarterly_series()
│    に置き換え（is_ytd除外を追加する意図的な挙動修正、現行データでは無害と
│    実データ検証済み）。従来は`financial_trend_calculator.py`（STONKS SILO）・
│    `quarterly_review_generator.py`/`tail_dcf_bridge.py`（TAIL）・
│    `hypecore.py`（HypeCore）が同種ロジック（`_latest_q()`・`_lq()`等）を
│    それぞれ独立再実装していたが、本アクセサ経由に統一した（呼び出し箇所計
│    15箇所は関数シグネチャ不変のため無改修。financial_trend_calculator.py
│    固有のQ4逆算ロジック`_build_q4_implied()`・hypecore.py固有のpandas変換
│    ロジックはそれぞれのファイル側にローカル残置し、reader.py側にはpandas
│    依存を持ち込まない設計を維持）。詳細は[[GATE2-PHASE3B-1]]参照
│    **追記（FY52WEEK-BS-NULL-SILENT-1 Phase A 2026-07-18）**:
│    `cash_and_equivalents`が全105銘柄実測でNone率0-4%（ほぼ確実にデータ
│    異常のシグナル）と判明したため、`or 0`による暗黙のゼロ化を廃止。
│    annual・四半期のいずれからも取得できなかった場合`available=False`・
│    新規`cash_missing`フラグをTrueにし、`calculate_bs_adjustment()`側の
│    既存フォールバック（`available=False`→`net_cash_per_share=0.0`）で
│    BS補正自体を安全にスキップする設計とした。`short_term_investments`/
│    `long_term_debt`/`short_term_debt`は「真のゼロ」との判別困難のため
│    対象外（Phase B/C、従来通り`or 0`を維持）。`pipeline.py::
│    _calc_g_fundamental()`（成長率g候補）・`_calc_roic_wacc_ratio()`
│    （RICEのVC_Factor）でも同フィールドに同種の除外パターンを追加。
│    `report_consistency_check.py`にCHECK-25/WARN-25を新設し、最新
│    annual_YYYY.jsonの対象6フィールド（total_assets/total_liabilities/
│    stockholders_equity/current_assets/current_liabilities/
│    cash_and_equivalents）のNoneを独立検知する。詳細はBACKLOG.md
│    [[FY52WEEK-BS-NULL-SILENT-1]]参照
│    **追記（NVDA-STI-TAG-UNIDENTIFIED-1 2026-07-19）**: `common/sec_data/
│    quarterly.py::TICKER_RESTRICTIONS`のticker別オーバーライドに、
│    `sti_concept`/`ltdebt_concept`/`revenue_concept`（単一タグへの差替え、
│    同一filing内・fy/fpタグベースの標準抽出の延長）と並ぶ新エントリ種別
│    `cross_filing_tags`を追加（KLAC/TER/V/SOFIは前者、NVDAは後者。
│    [[ANOMALY-PATTERN-CATALOG-1]]型C「資産クラス変化・当年度未タグ化型」
│    向け）。`sti_concept`等が「1フィールド=1タグの差替え」なのに対し、
│    `cross_filing_tags`は「指定end_date・指定form制限で複数XBRLタグを
│    直接検索し合算する」ため、`parser.py::_find_entry_by_end_date()`/
│    `_apply_cross_filing_tags()`という別関数群で実装されている。既存の
│    `_collect_own_data_annual/_instant`が持つ`form in (10-K, 10-K/A)`
│    フィルタ・accn_reportdate自己一致チェック（他銘柄の「比較年度再掲」
│    誤混入を防ぐ主要な防波堤）はグローバルには一切変更せず、
│    `cross_filing_tags`に明示登録されたticker×period×fieldの組み合わせ
│    にのみ迂回を適用する設計（`_parse_raw_data()`の標準抽出ループ後に
│    後付け上書きする形で配線、既存抽出ロジック自体は無改修）。合算値が
│    近似値の場合（NVDAのannual FY2026: +0.88%残差）は`bs_provenance
│    [field].is_approximated`/`residual_pct`に記録し、reader.py::
│    get_net_cash()→adjustments.py::BSAdjustmentResult→pipeline.py::
│    financial_health（`sti_approximated`/`sti_residual_pct`）を経由して
│    report.txtのST_Invest行に残差率を注記する。ただしBUG-NETDEBT-4
│    「同一時点原則」により、最新四半期にCash/LTDebtが揃っている場合は
│    四半期側の値が優先されるため、annual側の近似値フラグが実際に
│    report.txtへ表示されるのは四半期データが annual より古い/欠落して
│    いる期間に限られる（NVDA自身は現在2027Q1四半期側の正規合算値が
│    優先され、近似値表示は発生していない）。詳細はBACKLOG_DONE.md
│    [[NVDA-STI-TAG-UNIDENTIFIED-1]]参照
│    **追記（BS-FIELD-NONE-TRANSITION-DETECT-1 2026-07-19）**:
│    `report_consistency_check.py`にCHECK-26/WARN-26を新設。WARN-25が
│    「None率が高すぎる（35〜65%）」ことを理由に対象外とした
│    `short_term_investments`/`long_term_debt`/`short_term_debt`/`rpo`の
│    4フィールドについて、「Noneであること自体」ではなく「前年は値が
│    あったのに当年からNoneになる**遷移**」を検知する（正常な企業では
│    発生しないため、WARN-25のブランケット型不採用理由〈ノイズの多さ〉が
│    当てはまらない）。直近2年度分のannual_*.jsonのperiod（fyラベル）の
│    年度差が厳密に1であることを確認したうえでのみ判定し（決算期変更で
│    files[-2]が真の「1年前」を表さない可能性がある場合、RCATの
│    long_term_debt遷移年〈2024〉が[[FYE-CHANGE-BOUNDARY-COLLISION-
│    BLIND-1]]の決算期変更境界〈2024-2025年〉と一致する実例あり、判定不能
│    として発火させない）、annual_*.jsonが1年分のみの新規登録銘柄も対象外
│    とする。実装直後にFY52WEEK-BS-NULL-SILENT-1で確認済みの「生涯
│    フェードアウト」8件（APP/BKNG/CPRT/DOCN/ENTG/KULR/MSCI/SOUN）が
│    発火することが事前調査済みのため、`config/warn_acknowledged.json`へ
│    事前登録済み。詳細はBACKLOG_DONE.md
│    [[BS-FIELD-NONE-TRANSITION-DETECT-1]]参照
│    **追記（FY52WEEK-BS-FADEOUT-FALLBACK-1 2026-07-19）**: 「生涯
│    フェードアウト」（過去に明示的$0申告があるが最新年度はNone）銘柄向けに
│    `short_term_investments`/`long_term_debt`/`short_term_debt`の履歴
│    フォールバックを追加。新設`_lookup_last_confirmed_zero_year()`が
│    `get_annual_range(ticker, years=100)`を最新年度を除き降順走査し、
│    最初に見つかった非None値が**厳密に0**の場合のみその年度を返す
│    （非0ならNoneを返し即座にフォールバック不成立とする）。年数閾値・
│    M&A等イベント判別の専用機構は設けず、この「直近既知値が0か否か」
│    という条件のみで判定する設計（CSGP/KULR/RCATの3件は最後の$0の後に
│    非0の実額が再出現する複雑パターンのため、この条件で自然に除外
│    される。ハードコードされた銘柄リストは一切使用しない）。該当時は
│    `{field}_estimated_zero`/`{field}_last_confirmed_zero_year`を
│    戻り値に追加し、BUG-NETDEBT-3（正規化データからのLTDebt補完）・
│    BUG-NETDEBT-4（同一時点原則の四半期上書き）のいずれかで実データが
│    見つかった場合はフラグを解除する（四半期上書き時は該当サブ
│    フィールド自体に実データがある場合のみ解除、四半期側も同フィールドを
│    欠く場合はannualベースの推定ゼロ注記を維持）。adjustments.py::
│    BSAdjustmentResult・pipeline.py::financial_health経由でreport.txtに
│    「推定ゼロ（最終確認: FY20XX）」注記として表示、および従来combined
│    のみだった`total_debt`に加え`long_term_debt`/`short_term_debt`を
│    financial_healthへ個別公開。100銘柄実測で対象22件中19件が発火
│    （残3件はBUG-NETDEBT-3/4のより新しいデータへの正当な迂回）。
│    詳細はBACKLOG_DONE.md [[FY52WEEK-BS-FADEOUT-FALLBACK-1]]、除外3件は
│    BACKLOG.md [[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]参照
├─ data_fetcher.py::TTMReader  # common/sec_data/ttm/{TICKER}_ttm_series.jsonを
│    読み込み、_select_fcf_source()経由でSEC 10-Kベースのfcf_5yr_avg/fcf_listと
│    比較のうえ採用可否を決定する（TTM-QUARTERS-CHECK-1 2026-07-12完了:
│    OCF・CapEx双方のquarters_used>=4フィルタを追加し不完全TTM値を除外。
│    TTM点数が年次実績より少ない場合は年次を優先する_select_fcf_source()
│    ヘルパーも新設。詳細はBACKLOG_DONE.md参照）
│    **追記（TTM-PASCALCASE-KEY-STALE-1 2026-07-30実装完了、コミット
│    a7b840c32）**: `get_fcf_series()`/`get_periods()`（本ファイル）・
│    `build_rice_annual_shape()`（同ファイル、RICE用annual_data形状への
│    変換）・`common/sec_data/audit.py::audit_ticker()`が、フェーズC移行
│    （2026-07-25、ttm_calculator.py snake_case化）後もttm_series.jsonの
│    flowキーを旧PascalCase（"OCF"/"CapEx"/"Revenue"/"NetIncome"等）のまま
│    参照し続けていたため、2026-07-26〜29の約3日間、監視100銘柄全件で
│    RICEスコアが完全停止し94銘柄でFCFソースが誤ってannual_fallbackへ後退
│    していた。両ファイルのキー参照をsnake_caseへ修正し、全100銘柄再生成で
│    RICE 62銘柄・FCFソース94銘柄が正常化（IVが変化した40銘柄のうち
│    MSCI/LITE/ENTGは一次データで裏取りしFCF_Base選択ロジックの妥当な挙動と
│    確認済み）。詳細はBACKLOG_DONE.md [[TTM-PASCALCASE-KEY-STALE-1]]参照。
├─ core_calculator.py    # DCF・理論株価
├─ calculator/rice.py    # RICE投資効率
├─ calculator/dcf.py     # DCFエンジン
├─ calculator/adjustments.py  # alpha計算（参考値保持）・Moat Score計算（ALPHA-REDESIGN-1 2026-06-25）・
│   calculate_bs_adjustment()（reader.py::get_net_cash()の戻り値をBSAdjustmentResultへ変換）
│   注記: ALPHA-REDESIGN-1によりDCF_v0へのalpha乗算を廃止。
│        競争優位はMoat Score（粗利率・ROIC超過幅・FCFマージン）によるPhase1期間自動計算で表現。
└─ growth_sanity.py      # 成長率サニティチェック
↑ HypeCoreフェーズを参照
     ↓ latest.json（銘柄ごと）
pipeline.py              # 全銘柄を統合・TANUKI SCORE算出
├─ _load_extra_data()内のfinancial_health（report.txt表示＋TANUKI SCORE判定用）は
│    valuation["bs_adjustment"]を再利用する形に統一（ARCH-DATA-1残課題① 2026-07-15。
│    従来はpipeline.py独自にcommon/sec_data配下の生JSONを再読込しBS同一時点原則を
│    別実装していたため、reader.py::get_net_cash()側のみが適用するセクターガードが
│    反映されずV〈Visa〉で表示乖離が生じていた。二重読み込み自体も解消）
├─ hypecore_history/{TICKER}.json生成
│   （docs/value-monitor/hypecore/data/{TICKER}_poc.json を参照 →
│    docs/value-monitor/tanuki_valuation/data/hypecore_history/ に出力）
├─ stock.html（個別銘柄ページ）
└─ tanuki_score結果 → Discord通知（ACTION-10）
【独立データ取得層（他システムへの依存なし）】
Market Pulse  ← yfinance / CNN F&G / FREDデータ
　　TAKE PROFIT / BUY チェックリスト（F&G×200日MA×HYスプレッド×ヒンデンブルグ簡易版、
　　MP-LOGIC-1/2 2026-06-24実装）を market_data.json に出力
MACRO PULSE   ← FREDデータ / FRBステートメント
　　整合性チェック: src/market/macro_pulse/05_audit.py（05_events.csvの重複行検出・
　　NFP水準残存兆候の検出、report_consistency_check.py相当の軽量版。MACRO-NFP-1 2026-07-07新設）
　　過去データ補正: src/market/macro_pulse/05_backfill_nfp_mom.py（05_events.csv内の
　　既存NFP行を水準→前月比に一括変換する一回限りのバックフィルスクリプト。
　　MACRO-NFP-HIST-1 2026-07-08新設、実行済み）
　　RECESSION RISK SCORE（景気後退リスク複合スコア）: index.htmlの`computeCurrentScore()`
　　（JS）が算出するが、2026-08-22以降は`05_main.py::_compute_current_score()`が
　　AIウィークリーコメンタリー生成時（週1回、`05_weekly_analysis.csv`へ保存）に
　　サーバー側で算出した値を`WEEKLY_SNAPSHOT`グローバル変数へ読み込み、これを正として
　　返す（ブラウザ側ライブ再計算は未読込時のフォールバックのみ）。これにより「景気後退
　　リスク複合スコア」ゲージ・スコア推移チャートの「本日」データ点・比較バー・AIウィーク
　　リーコメンタリーが全て同一値を参照する（[[MACRO-PULSE-3M-FORECAST-SNAPSHOT-
　　MISMATCH-1]]）。8指標ごとのsignals（現在地カード・アラート判定）はこの統合対象外で
　　引き続きブラウザ側ライブ計算のまま。
## Market Pulse・MACRO PULSE 画面要素→導出関数→生データソース 依存関係マップ（2026-08-26新設）

`CHAT_RULES.md`事例13（層単位横串検証よりフロントエンド起点の縦割り検証を
優先する）を実践する上で、「画面ごとに検証で通過した依存関係を記録し、
次の画面検証時に再調査を省略する」ための土台として新設。各画面要素を
(a)表示コンポーネント（HTMLファイル・JS関数）→(b)導出関数（Pythonモジュール・
関数）→(c)生データソース（ファイル・カラム名）の順にたどる。次回以降、
これらの依存先を再検証する際は、まずこのマップで「既に検証済みか」を
確認してから着手すること。

### ① MACRO PULSEゲージ（`#pg-score-num`、RECESSION RISK SCORE現在値）
- **(a)** `docs/market-monitor/macro-pulse/index.html`
  `renderPhaseGauge()`（1180行目）が`el('pg-score-num')`（603行目）へ描画。
  `computeCurrentScore()`（JS、1087行目）がライブ計算のステップ関数を
  提供するが、`WEEKLY_SNAPSHOT`（771行目、AI週次解説生成時にサーバー側で
  算出された値を読み込むグローバル変数、2237-2244行目）が非nullの間は
  そちらを正として表示（1175行目）——ライブ計算はWEEKLY_SNAPSHOT未読込時の
  フォールバックのみ（`[[MACRO-PULSE-3M-FORECAST-SNAPSHOT-MISMATCH-1]]`
  で統合済み）
- **(b)** `src/market/macro_pulse/05_main.py::_compute_current_score()`
  （1414行目）——`computeCurrentScore()`とスコア計算式を1対1で同期させた
  Python版。8指標（yc/hy/philly/cfnai/claims/cbcc2/cbcc/sahm）の加重平均。
  週次AI解説生成時（`--weekly-analysis`）にのみ実行され、結果が
  `05_weekly_analysis.csv`へ保存される
- **(c)** `docs/market-monitor/macro-pulse/data/05_events.csv`の`indicator`/
  `actual`/`release_date`列（8指標分のみ、`indicator_keys`で列挙）。
  `regime`/`ff_rate`/`yc_10y2y`/`hy_spread`/`vix`/`cuts_implied`列は
  スコア計算に一切使われない（下記STEP 3-2参照）

### ② MACRO PULSE AIウィークリーコメンタリー
- **(a)** `docs/market-monitor/macro-pulse/index.html`内、週次解説表示部
  （`WEEKLY_SNAPSHOT`経由で①のゲージと同一値を共有）
- **(b)** `05_main.py::generate_weekly_analysis_with_grok()`（1556行目）。
  xAI Grok APIへ①のスコア・直近1週間の指標発表・FED政策局面等を
  プロンプトとして渡し生成。`regime`/`ff_current`/`cuts_implied`
  （プロンプト文言、1601-1603行目）は`fed_context`辞書（1727-1732行目、
  `05_fed_context.csv`の最終行）から取得——**events.csv側の同名列とは
  別ソース**（下記STEP 3-2参照）
- **(c)** `05_events.csv`（8指標のactual値・直近イベント）＋
  `05_fed_context.csv`（`regime`/`ff_current`/`cuts_implied`等のFED
  政策局面）。出力先: `05_weekly_analysis.csv`

### ③ MACRO PULSEスコア推移チャート・tooltip
- **(a)** `renderScoreHistory()`（1660行目）。ECharts。tooltip
  formatter（1729-1741行目）が「本日=実測値」「過去日=lerp補間表示」の
  算出方式注記を表示（`[[RECESSION-SCORE-TRIPLE-CALC-1]]`③対応、
  `[[MACRO-COMPUTE-DUP-1]]`で意図的設計と確定済み）
- **(b)** 本日1点は`computeCurrentScore()`（①と同一のステップ関数）、
  過去日は`computeScoreAsOf()`（JS、lerp補間、`c3eb81572`で導入）
- **(c)** `05_events.csv`の8指標（①と同一）

### ④ Hindenburg omen関連表示
- **(a)** `docs/market-monitor/market-pulse/index.html`
  `renderTakeProfit()`（948行目）・`renderBuyChecklist()`（997行目）が
  `CHECK_KEYS.hindenburg`（979行目）経由でチェック結果を表示
- **(b)** `src/market/market_pulse/collect_and_send.py::
  calc_hindenburg_active()`（1307行目）。新高値・新安値がともに
  `total_stocks×2.2%`を超えるかを判定（`[[MARKETPULSE-MINOR-
  INCONSISTENCIES-1]]`①で固定値500→実測`total_stocks`へ修正済み）。
  結果は`calc_take_profit_checklist()`（1329行目）・
  `calc_buy_checklist()`（1393行目）へ渡される
- **(c)** `docs/market-monitor/market-pulse/data/breadth_data.json`の
  `new_highs_52w`/`new_lows_52w`/`total_stocks`（⑦のbreadth_summaryと
  同一の生成元）

### ⑤ Hollow Rally関連表示
- **(a)** `docs/market-monitor/macro-pulse/index.html`内インライン
  （2590-2607行目、専用関数化されていない）。`#liqGrid`直上に
  `.hollow-rally-badge`を挿入（2609-2616行目）
- **(b)** 導出ロジック自体がフロントエンドJS内に直書き（S&P500の
  5営業日リターン>+1.0% かつ FRB純流動性の前回行比<-0.5%）。バックエンド
  側の対応する導出関数は存在せず、`05_main.py::get_sp500()`（858行目、
  FRED "SP500"系列・失敗時stooq.comフォールバック）と
  `update_liquidity_csv()`（1977行目）が生データを`05_liquidity.csv`へ
  書き込むところまでを担当
- **(c)** `docs/market-monitor/macro-pulse/data/05_liquidity.csv`の
  `sp500`列（`[[HOLLOW-RALLY-DEAD-1]]`案Xで新設・過去1309/1311行
  バックフィル済み）・`net_liquidity`列

### ⑥ Fear & Greed関連表示
- **(a)** `docs/market-monitor/market-pulse/index.html`
  `renderGauge()`（696-777行目、F&Gゲージ本体）・`renderTimeline()`
  （582行目、`d.fear_greed`参照）・`renderTakeProfit()`/
  `renderBuyChecklist()`（F&G≥75/≤25判定）
- **(b)** `collect_and_send.py::fetch_cnn_fear_greed()`（1629行目）。
  `fear_greed`パッケージの`fg.fetch()`（生API直接呼び出し、`get()`は
  使わない——`[[FEARGREED-DUPKEY-BUG-1]]`で`previous_close`誤値を修正
  済み）。**Market Pulseがこのシステム内で唯一のCNN F&G取得経路**——
  TANUKI VALUATION（`pipeline.py:390`）・TANUKI SCORE
  （`daily_pick.py:330`）はいずれも独自取得せず`market_data.json`の
  `fear_greed.score`を読むのみであることを確認済み（重複実装なし）
- **(c)** CNN Fear & Greed Index API（`fear_greed`パッケージ経由）。
  出力先: `docs/market-monitor/market-pulse/data/market_data.json`の
  `fear_greed`フィールド

### ⑦ breadth_summary関連表示
- **(a)** `renderGauge()`内（748-758行目、`const b=s.breadth`）。
  ADV/DEC比・NH-NL差・McClellan Oscillator・pct_above_50ma等の
  警戒バッジを表示
- **(b)** `breadth_calculator.py::compute_breadth()`（141行目）が
  S&P500構成銘柄の日次価格からadvances/declines/new_highs_52w/
  new_lows_52w/total_stocks等を算出し`breadth_data.json`へ保存。
  `collect_and_send.py::calc_sentiment_score()`内で`breadth_summary`
  辞書（325-348行目）を構築し`market_data.json`の`sentiment.breadth`
  へ格納（`[[MARKETPULSE-MINOR-INCONSISTENCIES-1]]`④で欠落5フィールド
  追加済み）
- **(c)** `docs/market-monitor/market-pulse/data/sp500_tickers.json`
  （銘柄リスト、`get_sp500_tickers()`で取得・`common.market_data`経由
  への統合対象外と明記〈39行目コメント〉）＋各銘柄の日次価格
  （`common.market_data.reader`経由）→`breadth_data.json`

### Market Pulse 全画面要素（MP-01〜MP-29、2026-09-26追加・指示書⑲）
上記①〜⑦のうちMarket Pulseに関わる④⑥⑦に加え、Market Pulse画面
（`docs/market-monitor/market-pulse/index.html`）の表示要素を全て洗い出した
（表示コンポーネント単位で29要素。1要素に複数の数値を含むものは(a)欄に列挙）。
(a)の行番号はindex.html、(b)の行番号は`src/market/market_pulse/collect_and_send.py`
（`bc`=`breadth_calculator.py`）。生データは原則`docs/market-monitor/market-pulse/
data/market_data.json`の最新エントリ（以下`L`）に集約され、フロントはそれを読むだけ。
【共有】は他画面・他システムも同じ値を読む要素（MACRO PULSEは`market_data.json`を
読まないため、MACRO PULSEとの共有要素はない。④⑥⑦は上記参照）。

| ID | (a) 表示コンポーネント | (b) 導出関数 | (c) 生データソース |
|---|---|---|---|
| MP-01 | 更新日時 `#lastUpdated`（`init()` 1174） | `save_data_to_json_and_csv()` 1451（実行時刻） | `L.date` |
| MP-02 | センチメントスコア `#gaugeScore`・針（`renderGauge()` 696） | `compute_sentiment()` 191（8指標の加重平均） | `L.sentiment.score` ← daily/（^VIX・^VIX9D・^GSPC・HYG・LQD・IVW・IVE）＋`breadth_data.json` |
| MP-03 | センチメントラベル `#gaugeLabel` | `compute_sentiment()`（20/35/50/65/80区切り） | `L.sentiment.label` |
| MP-04 | 前回比 `#gaugeDelta` | フロント計算（最新−1つ前のスコア） | `market_data.json`直近2エントリ |
| MP-05 | シグナルバッジ `#signalBadge`（BUY/TAKE PROFIT/HOLD） | フロント計算（`L.sentiment.signal`が無い場合の簡易判定、716行） | 表示期間内のスコア系列 |
| MP-06 | スコア構成指標バー `#subScores`（8本・重み・ツールチップ） | `compute_sentiment()`の`sub_scores` | `L.sentiment.sub_scores` |
| MP-07 | VIX短期vs中期 `#vix9dRow` | `get_realtime_data()` 755（VIX9D/VIX比・順鞘判定） | daily/の^VIX9D・^VIX |
| MP-08 | 市場の広がり `#breadthSummary`（ADV/DEC・AD5d・NH/NL・>50MA/>200MA・EW乖離・McClellan・警戒バッジ） | `bc::compute_breadth()` 141 → `compute_sentiment()`の`breadth_summary` | daily/のS&P500構成銘柄・RSP・SPY → `breadth_data.json`（=⑦） |
| MP-09 | センチメント推移チャート `#oscChart`（8系列切替・既定Score/F&G） | フロント描画のみ | `market_data.json`過去エントリ |
| MP-10 | センチメント予測ミニゲージ `#miniGaugesSection`（明日/5日後/20日後） | フロント計算（`renderMiniGauges()` 896、同ゾーン時の平均S&P500リターン×2） | `market_data.json`全エントリのスコア・S&P500 |
| MP-11 | CNN F&Gゲージ `#fgGaugeScore`/`#fgGaugeLbl`（`renderTechPulse()` 808） | `fetch_cnn_fear_greed()` 1633（=⑥） | CNN API → `L.fear_greed`【共有: TANUKI VALUATION `pipeline.py`・TANUKI SCORE（`daily_pick.py`・画面）・Extreme Fear画面】 |
| MP-12 | Tech Pulseゲージ `#tpGaugeScore`/`#tpGaugeLbl` | `calc_tech_pulse_score()` 728（QQQ vs MA125・VXN vs MA50・QQQ vs SPY 20日の90日パーセンタイル平均） | daily/のQQQ・SPY、FRED VXNCLS（`common/macro_data`） |
| MP-13 | 乖離 `#tpDivVal`・シグナル `#tpDivBadge` | `main`内（TP−CNN F&G）・`_get_tp_signal()` 708 | `L.tech_pulse.divergence` |
| MP-14 | 乖離Zスコア `#tpDivZscore`/`#tpCZscore` | `_calc_divergence_zscore()` 696（過去90件） | `market_data.json`過去エントリの乖離 |
| MP-15 | VXN `#tpCVXN` | `fetch_vxn_from_fred()` 464 | FRED VXNCLS |
| MP-16 | QQQ vs SPY 20日 `#tpCQQQ` | `fetch_qqq_tech_data()` 425 | daily/のQQQ・SPY |
| MP-17 | CNN F&G vs Tech Pulse推移 `#tpChart` | フロント描画のみ | `market_data.json`過去30日 |
| MP-18 | TAKE PROFITチェックリスト（F&G≥75で発動） | `calc_take_profit_checklist()` 1333（MA200・HYスプレッド・Hindenburg=④） | daily/の^GSPC、FRED BAMLH0A0HYM2、`breadth_data.json` |
| MP-19 | BUYチェックリスト（F&G≤25で発動） | `calc_buy_checklist()` 1397（同上） | 同上 |
| MP-20 | 資金フロー 今日のタイル（7資産） | `collect_asset_flow()` 1256・`fetch_fred_short_bond()` 1214 | daily/のSHV・GLD・TLT・LQD・HYG・SPY、FRED DGS3MO |
| MP-21 | 資金フロー 直近7日グリッド（休場行つき） | フロント描画のみ | `market_data.json`過去7エントリの`asset_flow` |
| MP-22 | 資金フロー 5日平均判定 | フロント計算（`renderAssetFlow()` 1101、リスク資産>+0.3%／安全資産>+0.05%） | 同上（過去5エントリ） |
| MP-23 | 市場フェーズピル `#phasePills` | `extract_judgment()` 1207（AI文から「判定：晴れ/曇り/嵐」を抽出） | `market_data.json`表示期間の`judgment` |
| MP-24 | 分析履歴タイムライン `#timeline`（日付・判定・スコア・F&G・VIX・TECH・乖離） | 上記各要素 | `market_data.json`表示期間 |
| MP-25 | 指標6カード `#metricsRow`（S&P500・NASDAQ・10Y・USD/JPY・WTI・GOLD） | `get_realtime_data()` 755（直近2終値の前日比）・`_fill_fallbacks()` 623 | daily/の^GSPC・^IXIC・^TNX・JPY=X・CL=F・GC=F【共有: USD/JPYは`src/portfolio/snapshot.py`も読む】 |
| MP-26 | 推移チャート `#mainChart`（VIX・S&P500等6系列） | フロント描画のみ | `market_data.json`表示期間 |
| MP-27 | 詳細カード クレジット3判定・Risk-Offスコア・確信度 | `save_data_to_json_and_csv()` 1451（S&P500<−1%・TLT/SPY・HYG−LQDの3判定） | `L.credit` ← indicators・asset_flow |
| MP-28 | 詳細カード AI分析本文・俳句・過去の分析 | `analyse_market()` 994（xAI Grok） | `L.summary`・`L.comments_history` |
| MP-29 | 計算式モーダル `#infoModal`（重み表・ゾーン表） | 静的HTML（導出なし） | なし（index.html直書き） |

**更新タイミング（2026-09-26確認）**: `Market_Pulse_Update.yml`（cron `35 21 * * 1-5`）は
`breadth_calculator.py`→`collect_and_send.py`を実行し、daily/は
`Market_Data_Daily_Update.yml`（cron `25 21 * * 1-5`）が書く。**両者はworkflow_runで
連鎖しておらず、cronの10分差だけに依存している**。GitHub側の遅延で両者とも
約2時間遅れてほぼ同時に起動するため、Market Pulseのcheckoutが日次データのpushより
先になる日がある（詳細は`MARKET_PULSE_LOGIC_INVENTORY.md`・BACKLOG参照）。
2026-08-26の横断点検（BACKLOG_DONE.md）で「ワークフロー間タイミング競合のリスクは
構造上存在しない」としたのは、2026-08-11にdaily/経由へ切り替えた後も
単一ワークフロー内で完結している前提で判断したもので、この前提は成り立っていない。
**2026-09-26に対応**（指示書⑳ STEP C）: Market_Pulse_Update.ymlはMarket Data Daily Updateの完了
（workflow_run）を起点にし、独立cronは金曜22:50 UTCのフォールバックにした。起動時にdaily/の最新日付と
期待する終値日を比べ、`market_data.json`のエントリの`data_freshness`に記録する。

### 作成中に見つけた注記事項（新規BACKLOG登録は不要と判断）
- Fear & Greedは上記の通りMarket Pulseへの一本化を確認済み（重複なし）。
  TANUKI VALUATION/TANUKI SCOREはいずれも`market_data.json`経由の
  参照のみで、独自にCNN APIを叩く経路は存在しない
- Hindenburg omenの判定ロジックは`collect_and_send.py`に1箇所のみ存在し、
  他システムでの重複実装は確認されなかった
- RECESSION RISK SCOREのJS/Python二重実装（`computeCurrentScore()`と
  `_compute_current_score()`）は既知・意図的設計として
  `[[MACRO-COMPUTE-DUP-1]]`で確定済みのため再登録しない

### 実ブラウザ確認スクリプト（2026-08-26新設）
上記7要素それぞれについて、生データから独立に計算した期待値と実ブラウザ
描画値を突き合わせる再利用可能なPlaywrightスクリプトを`browser_checks/
check_dependency_map.py`に整備した（詳細は同ディレクトリのREADME.md
参照）。CI組み込みはスコープ外・手動実行のみ。2026-08-26初回実行では
7要素全て一致、consoleエラー0件を確認済み。2026-09-26に上記MP-01〜MP-29と、導出層・データ層の独立再計算
（D-01〜D-05、`browser_checks/market_pulse_elements.py`）、要素ごとのデータ基準日の分類を追加した。次回以降、このマップの
依存先を変更した際は本スクリプトで再確認すること。

PORTFOLIO     ← 手動入力 / 証券会社API
TANUKI TAIL（docs/portfolio/tail/）← EDGAR RSS / Grok（KPI提案・四半期レビュー生成）
　　内部統制評価: src/tail/sec_ctrl_fetcher.py → docs/portfolio/tail/data/ctrl/{TICKER}/{QUARTER}.json + latest.json
　　（SEC-CTRL-1 2026-06-24実装、週次自動更新）
　　モーダル構成: coreとsatelliteで同等の5タブ（テーゼ/最新レビュー/KPIトレンド/DCFシナリオ/内部統制）
　　（TAIL-SAT-CORE-1 2026-06-26でcore同等化。AI視点タブはTAIL-UX-1 2026-07-05でdetail.htmlに
　　一本化・モーダルから削除）
　　分離ページ: decision_log.html（TAIL-LAYOUT-1 2026-06-24新設）
　　詳細ページ: detail.html（TAIL-PAGE-1 2026-06-27新設、モーダル廃止・全情報1ページ表示）
　　内部統制データ: data/ctrl/{TICKER}/{QUARTER}.json + latest.json + index.json
　　（TAIL-CTRL-TRANS-1 2026-06-27、期別保存・Grok日本語翻訳・履歴セレクター対応）
　　書き込み系（ポジション登録・ジャーナル記録・KPI確定）は
　　tail/index.html → GitHub Actions workflow_dispatch
　　（.github/workflows/TANUKI_TAIL_Position_Write.yml → src/tail/workflow_write.py）
　　経由でリポジトリにコミット（TAIL-SEC-1 2026-06-21、旧:ブラウザから直接GitHub API書き込み）

　　**RSS監視・四半期レビュー自動生成の対象範囲（確定、2026-08-19②
　　決定）**: RSS監視（`edgar_rss_monitor.py`の`--ticker`未指定時
　　デフォルト、`get_monitored_tickers()`）・四半期レビュー自動生成
　　（`quarterly_review_generator.py`）とも、**全保有ポジション（10銘柄）
　　が対象**（`[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`で方針決定・完了）。
　　core/satelliteの区別はthesis内のポジション重み付けとして残るが、
　　**監視対象の決定には使わない**（保有している以上、決算は見る）。
　　`get_core_tickers()`（core限定）は他用途向けに関数として存置して
　　いるが、監視対象の決定には使用しない。

　　旧方針（core限定維持、方針(b)）は一度採用されたが、判断の前提
　　（対象を1銘柄と誤認）が誤っていたため差し戻された経緯がある
　　（`CHAT_RULES.md`事例6参照）。satelliteでもLayer3のSEC EDGAR
　　データ自体は取得・保持されている（例: APGEの`operating_income`/
　　`stock_based_compensation`/`net_income`/`shares_diluted`は実データ
　　あり、`revenue`のみ無収益バイオのため0件）。

　　`edgar_rss_monitor.py`の差分検知設計（初回実行はベースライン記録の
　　みでキューに追加しない）により、対象拡大前から蓄積していた
　　未レビュー決算の在庫（旧satellite 6銘柄分、実測件数は
　　`[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`参照）は自動的には解消されず、
　　次回以降の新規提出から順次監視が始まる。2026-08-19②の実地検証で
　　ADBE（`latest.json`あり）・APGE（`latest.json`なし、tanuki=false）
　　それぞれ実際にレビュー生成〜DCFシナリオ生成まで確認済み（ADBEは
　　シナリオ生成成功、APGEは`latest.json`不在により`[SKIP]`で正常に
　　スキップ、クラッシュなし）。この検証中に、レビュー生成プロンプトが
　　satelliteスキーマ（`strategy_name`/`entry_condition`/
　　`exit_condition`/`holding_period`）を読まずcoreスキーマ
　　（`thesis`/`entry_story`/`exit_guide`）のみを読んでいたため
　　satelliteの投資テーゼが常に「未設定」としてGrokに渡っていた実バグ
　　を発見・修正した（`_thesis_narrative_fields()`、
　　`quarterly_review_generator.py`）。

　　除外は`edgar_rss_monitor.py`・`tail_dcf_bridge.py::
　　generate_scenario_files()`双方が`[SKIP]`ログで明示する（沈黙除外の
　　廃止、2026-08-19実装）。監視対象の拡大が正しく機能しているか
　　（＝保有ポジションが何らかの理由で監視から脱落していないか）は
　　`report_consistency_check.py`のCHECK-37が継続的に検出する
　　（対象を`get_monitored_tickers()`に合わせて更新済み。ただし方針
　　変更日〈2026-08-19〉より前に提出された決算は、対象拡大前からの
　　既知の在庫として一律NG化しない）。

　　**`satellite_monitor.py`との役割分担（2026-08-19③調査・記録）**:
　　**2026-09-23削除済み**——`satellite_monitor.py`・
　　`.github/workflows/TANUKI_TAIL_Satellite_Monitor.yml`は
　　Koichiさんの承認済み判断（機能自体が「認識されていない・不要」と
　　判明）により完全削除した（`[[TAIL-SATELLITE-MONITOR-CORE-
　　APPLICABILITY-1]]`参照、BACKLOG_DONE.md）。以下は削除前の設計・
　　調査記録として歴史的に保持する。
　　`satellite_monitor.py`は`edgar_rss_monitor.py`/
　　`quarterly_review_generator.py`とは独立した別システムで、
　　`positions_index.json`の`type=="satellite"`を**直接**フィルタする
　　（`get_monitored_tickers()`/`get_core_tickers()`は使わない、独自の
　　ハードコード選定）。4条件を監視しDiscord通知＋
　　`journal.json`のwatchlistエントリとして記録する:
　　①価格変動±20%（エントリー価格比）、②エグジット条件の数値目標
　　到達（`exit_condition`から正規表現で数値抽出）、③Grok Web検索に
　　よるテーゼ否定ニュース検知（直近2週間）、④決算接近（`rss_state.
　　json`の`last_filed`＋90日サイクルから推定した次回決算日の2週間前
　　に警告）。`.github/workflows/TANUKI_TAIL_Satellite_Monitor.yml`で
　　平日2回（JST 08:00・17:00）実行——`TANUKI_TAIL_RSS_Monitor.yml`
　　（平日1回、JST 17:00）より高頻度。

　　**④決算接近と、RSS監視によるレビュー生成の関係**: 重複ではなく
　　役割が異なる。④は`rss_state.json`の`last_filed`から次回決算日を
　　**予測**し、決算**前**に「エグジット条件を再確認してください」と
　　促す予防的アラート。RSS監視〈`edgar_rss_monitor.py`〉→四半期レビュー
　　〈`quarterly_review_generator.py`〉は実際の決算提出を検知した
　　**後**に、その内容を踏まえたAI評価（health_score・
　　recommendation）を生成する事後分析。タイミング（前／後）と内容
　　（予告のみ／テーゼへの影響評価）の両方が異なり補完関係にある。

　　ただし④は`rss_state.json`にそのポジションのエントリが存在しないと
　　`_check_earnings_approach()`が即座に`(False, None, None)`を返し
　　機能しない（345-349行目）。**satelliteは2026-08-19の監視対象拡大
　　より前、`rss_state.json`に一度もエントリが存在しなかった**ため、
　　④「決算接近」アラートはsatelliteについて実質的に機能していな
　　かった（`edgar_rss_monitor.py`がsatelliteに対して一度も実行されて
　　いなかったための帰結、`[[TAIL-SATELLITE-POSITION-MONITORING-
　　GAP-1]]`と同根）。2026-08-19③時点でsatellite 7銘柄全てに
　　`rss_state.json`のエントリが存在するため、今後は④も機能する。

　　**core側の非対称（2026-08-19③発見）**: `satellite_monitor.py`は
　　`type=="satellite"`のみを対象とし、core（PLTR/SOFI/TSLA）は
　　`_load_satellite_positions()`のフィルタで**除外される**。
　　`.github/workflows/`にcore向けの同等システム（価格変動・
　　エグジット条件数値・テーゼ否定ニュースの監視）は存在しない
　　（全ワークフロー〈`TANUKI_TAIL_KPI_Update`・`TANUKI_TAIL_Position_
　　Write`・`TANUKI_TAIL_RSS_Monitor`・`TANUKI_TAIL_SEC_Ctrl`・
　　`TANUKI_TAIL_Satellite_Monitor`〉を確認、該当なし）。**したがって
　　core 3銘柄は、価格変動・エグジット条件充足・テーゼ否定ニュースの
　　継続監視を一切受けていない**——四半期ごとのAI評価（filing検知
　　トリガー）のみ。これは`[[TAIL-SATELLITE-POSITION-MONITORING-
　　GAP-1]]`・`[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`で発見・是正した
　　「satelliteがレビュー生成から漏れていた」問題と**逆方向の同型の
　　非対称**（今度はcoreが高頻度アラートから漏れている）。事実の記録
　　のみ、対応要否は別途判断。

　　**KPI取得失敗の検知（CHECK-38、2026-08-19⑥新設）**: TAIL登録KPI
　　（`config/tail_kpi_map.json`）の値取得が`xbrl_segment_fetcher.py`
　　で失敗すると`docs/portfolio/tail/data/kpi/{ticker}_layer2.json`の
　　`missing_kpis`に記録されるが、これを検知する仕組みがCHECK-38
　　新設まで存在しなかった（`xbrl_segment_fetcher.py::fetch_ticker()`
　　は部分・全件失敗でも常に成功扱いを返し、CIは毎週GREENで完走して
　　いた——`[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-GAP-1]]`実測で
　　発見、core 3銘柄の過去レビュー27件中15件〈56%〉に沈黙してKPI不足が
　　混入していた）。

　　CHECK-38は`get_monitored_tickers()`を対象に`missing_kpis`が空でない
　　銘柄をWARNとして件数付きで表示し（`{ticker}: KPI {n}/{total}件が
　　取得失敗`）、`config/tail_kpi_fetch_baseline.json`に記録した銘柄別
　　missing_count（2026-08-19時点の実測値）を**超えて悪化した場合の
　　みNG**とする。

　　**baselineは許容値ではなく是正目標である**（`tail_kpi_fetch_
　　baseline.json`の`_meta`にも同じ文言を明記）。2026-08-19時点で
　　39/52件のKPIが既に失敗しており、これを即座にNG化すると全チェック
　　がブロックされる一方、黙らせることは`[[TAIL-SATELLITE-POSITION-
　　MONITORING-GAP-1]]`以降このセッションで繰り返し否定してきた
　　サイレント・フォールバックと同型になるため、「悪化のみを機械的に
　　検知し、現状の失敗件数自体は都度WARNで可視化し続ける」設計とした。
　　対応が進んだ場合はbaselineを引き下げて更新すること（引き上げる
　　場合は新たな悪化を許容したことになるため理由を`_meta`に明記する）。
　　**2026-08-19⑦、satellite分でこの運用を実際に一度行った**——下記
　　「KPI登録は実取得検証を通過したものだけ」の対応によりsatelliteの
　　取得失敗が0件になったため、baselineを39件→7件（satellite分は
　　全て0に、core分はKPI未変更のため据え置き）へ引き下げた
　　（`config/tail_kpi_fetch_baseline.json`の`_meta.update_reason`に
　　理由を記録）。

　　**KPI登録は「実取得検証を通過したものだけ」を原則とする
　　（2026-08-19⑦、`[[TAIL-XBRL-MEMBER-VALIDATION-GAP-1]]`）**:
　　`kpi_proposer.py`がGrokに提案させたKPI（`xbrl_tag`/
　　`xbrl_dimension`/`xbrl_member`）を、実際にXBRLへ存在するかの
　　個別照合（タグ一覧・member一覧の提示、2026-08-19⑥実装）だけでは
　　不十分だった——タグ・memberが個別に実在してもその組み合わせの
　　ファクトが存在するとは限らず、実測ではdimension/member一致率が
　　100%に改善しても実際のKPI値取得は0件のままだった。そのため
　　**`config/tail_kpi_map.json`へ登録する前に、本番の取得経路
　　（`xbrl_segment_fetcher.py::parse_and_extract()`をそのまま呼ぶ）
　　で直近1四半期分のXBRLに対して実際に値が取れるか検証し、取れた
　　KPIだけを登録する**方式（`validate_kpis_fetchable()`）に切り替
　　えた。値が取れなかったKPIは登録せず、却下理由（tag不在／member
　　不在／組み合わせ不在／非セグメント指標の構造的制約／検証不能）を
　　必ず表示し、`kpi_proposals/{ticker}_proposal.json`の
　　`rejected_kpis`にも記録する（黙って捨てない）。全KPIが却下された
　　銘柄は`tail_kpi_map.json`に`[]`を明示的に書き込み「未処理」と
　　「0件」を区別する。実測では satellite 7銘柄で提案41件中14件が
　　検証を通過・登録され、本番`xbrl_segment_fetcher.py`実行で14件
　　全てが実際に値取得成功（登録前はsatellite 0件だった）。事例5の
　　原則（部分の代理判定ではなく本番の入口から出口まで通す）をKPI
　　登録という別の場面に適用した実例。

　　**却下KPIの可視化（CHECK-39、2026-08-19⑧新設）**: 実取得検証方式
　　への切り替えは副作用を伴った——却下されたKPIが`kpi_proposals/
　　{ticker}_proposal.json`の`rejected_kpis`という、CHECK-38の集計
　　対象外の場所へ移動し、「失敗を検知する仕組みを改善したつもりが、
　　失敗の置き場所を変えただけで検知範囲から外れていた」という状態に
　　なっていた（`CHAT_RULES.md`事例7参照。今回は対応の副作用として
　　自分たちで作ってしまった点も含めて記録）。CHECK-39を新設し、
　　`rejected_kpis`（＝必要と判断されたが登録すらされなかったKPI）を
　　CHECK-38の`missing_kpis`（＝登録したが値が取れなかったKPI）とは
　　別に集計・別のbaselineキー（`rejected_count`）で管理する。

　　**Layer3経由取得の自動振替（2026-08-19⑨、`[[TAIL-XBRL-SEGMENT-
　　FETCHER-NONDIMENSIONED-GAP-1]]`）**: `xbrl_segment_fetcher.py`は
　　セグメント区分（`explicitMember`）の無い会社全体の事実を構造的に
　　取得できない設計上の制約を持つ。この制約自体は解消していないが、
　　却下されたKPIの大半（実測: satellite全体で却下26件中24件）が
　　SEC EDGAR Layer3統合スキーマ（`common/sec_data/layer3_builder.py`、
　　`config/sec_concept_definitions.json`）に既に存在するデータで
　　あることが判明したため、**取得元を機械的に振り分ける経路**を
　　追加した。

　　`config/tail_kpi_map.json`のスキーマに`"source": "layer3"`＋
　　`"layer3_field"`（直接参照）／`"layer3_formula"`（`"a/b"`形式の
　　除算のみ）を追加（既存エントリは`source`キー無し＝従来通りXBRL
　　直接取得のまま後方互換）。`xbrl_segment_fetcher.py::
　　fetch_layer3_kpis()`が`build_ticker_store()`/`get_quarterly_
　　series()`から値を取得し、既存の`{ticker}_layer2.json`と同じ
　　スキーマへ書き込む（消費側は無変更）。`layer3_formula`使用時、
　　**分母が0またはNoneの四半期はその四半期のエントリ自体を作らず
　　スキップする**（falsy-zeroを作らない設計）。

　　**振り分けの判断主体は取得側の機械的照合**（`kpi_proposer.py::
　　route_rejected_to_layer3()`）——`config/sec_concept_definitions.
　　json`の`fields[*].candidates`（Layer3ビルダー自身が使う唯一の正の
　　タグ一覧、新たな独自エイリアス表は作らない）から`xbrl_tag`の
　　ローカル名を逆引きするだけで、**Grokに「これはLayer3から取れる」
　　と判断させていない**（タグ名をGrokに生成させて失敗したのと同型の
　　リスクを避けるため）。名前が一致するだけでは登録せず、
　　`build_ticker_store()`を実際に呼んで対象ティッカーにそのLayer3
　　フィールドの実データが存在するか（`get_latest_quarterly()`が
　　非Noneを返すか）も確認してから登録する（事例5の原則）。

　　実測結果: satelliteの実取得成功KPIは14件→38件、core+satellite
　　総合計は26件→50件に増加。1件（NVDAの`gross_profit`）を実際の
　　決算生タグと手動突合し完全一致を確認済み。

　　**dimensionガード（2026-08-20②、`[[TAIL-LAYER3-ROUTING-DIMENSION-
　　BLIND-1]]`）**: `route_rejected_to_layer3()`の機械的照合は当初
　　`xbrl_tag`のローカル名だけで判定しており、元の提案が持つ
　　`xbrl_dimension`（セグメント/製品/地域別等の区分指標として
　　提案されたか）を確認していなかった。Layer3の32フィールドは
　　いずれも会社全体（非ディメンション）の集計値のため、
　　`xbrl_dimension`が設定された却下KPIをそのまま振り替えると、
　　**KPI名は区分別指標を名乗ったまま実データは会社全体の値に
　　すり替わる**。実例2件（APP「継続営業利益」→会社全体`net_income`、
　　CELH「機能性エナジードリンク売上」→会社全体`revenue`、いずれも
　　値の取得自体は成功していたため気づかれずtail_kpi_map.jsonへ
　　登録されていた）を発見し、`xbrl_dimension`が非空のKPIはLayer3
　　振替の対象外とするガードを追加（対象外にした場合は
　　`[Layer3振替対象外]`のログを必ず出力し沈黙除外にしない）。
　　既存2件は是正: APPは重複が無かったためKPI名を実態（`純利益
　　（会社全体）`）に合わせて改名、CELHは既存の「売上収益」
　　（同一`revenue`フィールド参照）と完全重複していたため削除した。
　　いずれも元々のセグメント/製品別データを求める分析上のニーズは
　　解決していないため、`rejected_kpis`へ記録を戻している
　　（`config/tail_kpi_fetch_baseline.json`のAPP/CELH `rejected_count`
　　を0→1へ更新）。

---

## common/market_data/（yfinance統合層、2026-08-11追記）
`common/sec_data/`と同型のfetcher.py/reader.py分離構成でyfinance依存を
一元化する新層（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`）。レイヤー横断の
再計算は禁止（`reader.get_ma_deviation()`のみ`daily/`層内での計算を許可
する例外）。

- `common/market_data/fetcher.py`: ネットワーク取得＋検証＋原子的書き込み。
  `fetch_daily_prices()`（daily/層、`Market_Data_Daily_Update.yml`平日
  実行）・`fetch_weekly_attributes()`（attributes/層、`Market_Data_Weekly_
  Update.yml`週次実行）・`fetch_analyst_events()`（analyst_history/層、
  upgrades_downgrades・earnings_history・recommendations_historyの3系統）・
  `backfill_daily_prices(symbols, period="1y", start=None)`（ma200等の
  移動平均が自然蓄積で使えるようになるまでの立ち上げ用バックフィル、
  定期cronには組み込まない一過性ツール、CLIの`--backfill`/`--start`
  フラグ経由で手動実行。`start=`指定時は`period`より優先、hypecore.py
  切替の前提作業で`start="2021-01-01"`の5.5年分バックフィルに使用）。
- `common/market_data/reader.py`: 読み取り専用API。
  `get_latest_price()`・`get_price_series()`・`get_price_series_as_of(as_of_date)`
  （`get_price_series()`の「終点＝daily/の最新保存日」を任意の過去日に
  一般化した版、共通実装`_price_series_ending_at()`を両者で共有。
  `backfill_tech_pulse.py`切替の前提作業として2026-08-12新設）・
  `get_price_on_or_after(date)`（`score_verifier.py`切替の前提作業として
  2026-08-12新設）・`get_ma_deviation(window)`（移動平均乖離率%を返す、
  生の移動平均価格そのものは保存しない設計）・`get_attributes()`・
  `get_calendar()`・`get_analyst_events()`・`get_earnings_history()`・
  `get_recommendations_history(latest_only)`・`get_index_series()`・
  `get_sp500_constituents_prices()`の12種。
- 保存構造は3層独立（`daily/{SYMBOL}.json`・`attributes/{SYMBOL}.json`・
  `analyst_history/{SYMBOL}.json`）＋`{SYMBOL}/market_data_violations_log.json`
  （fy_collision_log.json型、セクション独立のread-modify-write）。
- current_priceは常に`daily/`層（前日終値、`get_latest_price()["close"]`）
  から取得する方針で統一。`attributes/`層は`.info`スナップショット
  （market_cap/beta/sector/PER/PEG/PS/EV_EBITDA/dividend_yield/
  アナリスト目標株価/revenue_growth/earnings_growth/gross_margins/
  recommendation_mean/short_pct_float/short_ratio/average_volume等）
  専任で、`previousClose`等の価格系フィールドは意図的に含めない。
- 消費者切替（**本番8＋診断ツール2＋周辺ツール2の全12ファイル完了**、
  2026-08-12時点）:
  本番消費者8ファイル: `beta_fetcher.py`・`data_fetcher.py`（TANUKI
  VALUATION本体、DCF計算直結の本丸）・
  `discover/stonks-silo/src/valuation_fetcher.py`（STONKS SILO）・
  `pipeline.py`（`.calendar`のみ）・`collect.py`（Discover）・
  `collect_and_send.py`（Market Pulse）・`breadth_calculator.py`・
  `hypecore.py`（daily/attributes/analyst_historyの3層すべてが混在する
  最複雑の消費者、前提作業3件〈daily/バックフィル拡張・attributes/7
  フィールド追加・analyst_history/2系統追加〉込み）。診断ツール2
  ファイル: `audit.py`（β乖離監査・カナダ企業判定）・`score_verifier.py`
  （判定実績の事後検証、`get_price_on_or_after()`新設が前提作業）。
  周辺ツール2ファイル: `extract_key_facts.py`（株式数フォールバック④）・
  `backfill_tech_pulse.py`（QQQ/SPY取得、`get_price_series_as_of()`
  新設が前提作業）。詳細はBACKLOG.md`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・
  BACKLOG_DONE.md「2026-08-12（完了）」参照。

  切替過程で`daily/`層が`auto_adjust=False`（未調整終値）で保存されて
  いることを発見し、当初「バグ」として登録したが、事実確認調査の結果
  「テクニカル指標としては未調整終値の方が元々正しく、旧実装
  〈調整済み終値使用〉の方が不適切だった」と判明し訂正・クローズ済み
  （`[[MARKETDATA-DAILY-UNADJUSTED-PRICE-DIVIDEND-DRIFT-1]]`、対応不要
  で確定。教訓は`CHAT_RULES.md`「新旧の値が食い違う場合...」参照）。

---

## common/macro_data/（FRED統合層、2026-08-12追記）
`common/market_data/`と同型のfetcher.py/reader.py分離構成でFRED依存を
一元化する新層（`[[MACRODATA-LAYER-CONSTRUCTION-1]]`）。

**【状態（2026-08-12時点）】完成**: `fetcher.py`/`reader.py`本体・
`.github/workflows/Macro_Data_Update.yml`（定期取得ワークフロー）・
本番消費者2ファイル（`05_main.py`・`collect_and_send.py`）の切替が
全て完了。`series/`へ初回実データ投入済み（25系列中24系列成功）。
過去データ一括投入（フェーズ2）は、24系列中23系列が初回投入時点で
FRED公式の全期間履歴を既に取得済みと判明し追加作業不要、残る
`BAMLH0A0HYM2`（FRED側の2026年4月からの提供範囲制限で再取得不可能）
のみ例外的移行（`migrate_bamlh0a0hym2_history.py`）を実施済み。
FRED分の過去データ移管は実質完了、SEC EDGAR・yfinance分は次段階で
扱う（詳細はPROJECT_STATUS.md「フェーズ2」参照）。

- `common/macro_data/fetcher.py`: ネットワーク取得＋検証＋原子的書き込み。
  `fetch_series(series_id, start=None)`（FRED系列取得の唯一の外部
  アクセス関数、リトライ3回＋指数バックオフ。`05_main.py::
  fetch_event_row()`のリトライ実装パターンを踏襲）・
  `update_series(series_id, start=None)`（`fetch_series()`結果を
  既存ストアへ日付単位でupsert、保存前検証を実行）・
  `fetch_all_series(series_ids=None)`（`series_meta.json`の全系列
  または指定系列へ`update_series()`を順次実行するバッチ関数）。
  `if __name__ == "__main__":`ブロック（`python common/macro_data/
  fetcher.py [series_ids]`、`common/market_data/fetcher.py`と同じ
  CLIパターン）から`.github/workflows/Macro_Data_Update.yml`が呼び出す。
  `start`未指定時は常に全期間履歴を取得する設計であり、日次cronが
  毎回全期間を再取得する非効率が判明済み（`[[MACRODATA-FULL-HISTORY-
  DAILY-REFETCH-1]]`、対応未定）。fredapiクライアントはモジュール
  レベルで1つだけ生成し使い回す（切替前の各ファイルが呼び出しの都度
  `Fred()`を個別生成していた設計は踏襲しない）。`FRED_API_KEY`環境
  変数名・クライアント初期化方法（`Fred(api_key=...)`）は切替前の
  `05_main.py`・`collect_and_send.py`側の実装を実コード確認の上で
  踏襲。
- `common/macro_data/reader.py`: 読み取り専用API。
  `get_latest(series_id)`・`get_series(series_id, start=None,
  end=None)`（期間内の全エントリを観測日昇順で返す、観測日を含む
  ため消費側が期間の連続性を自前で検証できる）・
  `get_value_as_of(series_id, date, lookback_days=45)`（指定日以前の
  直近観測値をルックバック窓内で探索、`market_data/reader.py::
  get_price_on_or_after()`系のAPIパターンを踏襲しつつ探索方向は
  「date以前」）の3種。reader.py内で外部API呼び出しは一切行わない。
- 保存構造: `series/{SERIES_ID}.json`（系列単位の時系列ストア、
  観測日昇順のレコードリスト。各エントリは`value`/`as_of`/
  `fetched_at`〈ISO8601・JST〉/`source`〈`"FRED"`固定〉/
  `source_detail`）・`series_meta.json`（25系列の`fred_release_id`/
  `obs_to_release_lag`/`category`/`consumers`、静的ファイル）・
  `macro_data_violations_log.json`（単一の共有ログファイル、系列IDを
  キーに0件でも毎回書き込む。`market_data_violations_log.json`は
  銘柄ごとに別ファイルだが、macro_dataは単一ファイル内をセクション
  分割する点が異なる）。
- 保存前検証: ①同一`as_of`の重複（今回取得バッチ内）②直前値からの
  変化が1桁（10倍または1/10）以上か、の2項目（`EXTRACTION_DESIGN_
  PRINCIPLES.md`原則3対応）。
- `series_meta.json`は25系列全件を機械的に走査して生成
  （`INDICATOR_CONFIG`12系列＋流動性カード/FOMC/Market Pulse用13系列）。
  `INDICATOR_CONFIG`にメタが存在しない系列は`fred_release_id`/
  `obs_to_release_lag`をnullとし`note`フィールドに明記。`consumers`
  には実際の参照関数名を記録（重複取得3系列`BAMLH0A0HYM2`/`T10Y2Y`/
  `VIXCLS`は複数の`consumers`を持つ）。
- 新規テスト`tests/test_macro_data_fetcher.py`・`tests/test_macro_data_
  reader.py`（計43件）。fredapi呼び出しは全てモック化、実通信なし。
- **本番消費者切替（2026-08-12完了）**: `05_main.py`（`get_ff_current`/
  `get_implied_cuts`/`get_financial_context`/`get_sp500`/
  `fetch_event_row`/`refresh_monthly_indicators`/`update_liquidity_csv`/
  `update_fed_context`/`_load_sp500_cache`の計9関数、約20箇所）・
  `collect_and_send.py`（`fetch_vxn_from_fred`/`fetch_hy_spread_from_
  fred`/`fetch_fred_short_bond`の3関数）の全FRED直接呼び出しを
  `common.macro_data.reader`経由に切替済み（`get_fred()`は削除）。
  重複3系列（`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`、`[[MACRODATA-AS-IS-
  DUPLICATION-UNDERCOUNT-1]]`参照）は`reader.get_latest()`への集約で
  解消。NFP前月比・VXN MA50・HYスプレッド90日min/max・DGS3MO前日比・
  S&P500複数日履歴の5箇所は単一最新値では機能を維持できないため
  `reader.get_series()`（期間指定）を使用。`fred_release_dates()`
  （FRED Release Calendar REST APIへの直接`requests.get()`、
  observation値ではなく発表日カレンダーを扱う別API表面）のみ対象外・
  維持。詳細はBACKLOG.md`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照。
- `common/macro_data/migrate_bamlh0a0hym2_history.py`（例外的履歴移行、
  2026-08-12実装・実行）: `fetcher.py`・`reader.py`とは別系統の、一度
  限りの例外専用スクリプト。FRED側（データ提供元ICE Data Indices）が
  2026年4月から`BAMLH0A0HYM2`の提供範囲を直近3年に制限したため
  （`fredapi::get_series_info()`のnotes欄で確認）、通常のfetcher.py
  再取得では対応不能となった過去分を、削除予定の旧`docs/market-monitor/
  macro-pulse/data/05_events.csv`（`indicator == "HY Spread"`行）から
  一度だけ移行する。`fetcher.py`の`_atomic_write_json`・`_load_json`・
  `_validate_incoming_batch`・`_write_violations_section`を再利用。
  移行レコードの`source_detail`にはライブ取得分（`"series=
  BAMLH0A0HYM2"`）と区別可能な`migrated_from=05_events.csv`マーカーを
  付与。二重実行防止ガード（既存レコードに移行マーカーが検出された
  場合`--force`なしでは中断）・`--dry-run`オプションを実装。実行結果:
  `2023-08-14`より前の6,947件を追加投入（785件→7,732件）、既存レコード
  は無変化。スクリプト自体は監査証跡として恒久残置（使い捨てではない）。
  詳細はBACKLOG_DONE.md`[[MACRODATA-BAMLH0A0HYM2-HISTORY-EXCEPTION-1]]`
  参照。

---

## 既知の技術的負債（2026-09-16新設）

- `src/value/tanuki_valuation/__init__.py`は`.wacc`/`.growth`等8モジュール全てで
  相対importのパスが誤っており（実体は`calculator/`配下）、標準的なパッケージ
  import（`import src.value.tanuki_valuation`等）は常に失敗する。本番運用・
  既存テストスイートともスクリプト直接実行で`__init__.py`を経由しないため
  実害は確認されていないが、将来パッケージimport経路が使われる場合は
  ImportErrorとして顕在化するリスクがある（詳細はBACKLOG_DONE.md
  `[[TANUKI-VALUATION-INIT-RELATIVE-IMPORT-BROKEN-1]]`参照）。

---

## 変更時の影響範囲チェックリスト

| 変更ファイル | 必要な追加作業 |
|---|---|
| quarterly.py / normalizer.py / ttm_calculator.py | 全銘柄TTM再生成（update.py）→ audit.py |
| parser.py | 影響銘柄のupdate.py → audit.py。`XBRL_MAPPING`の`short_term_investments`/`long_term_debt`/`short_term_debt`/`rpo`候補タグリストは[[FY52WEEK-BS-NULL-SILENT-1]] Phase B Stage1（2026-07-19）で拡充済み（各フィールドの追加タグはコード内コメント参照）。**候補タグを追加する際は`quarterly.py`の`TICKER_RESTRICTIONS`（`ltdebt_concept`等の銘柄別override、SOFI-DATA-1）と衝突しないか個別確認すること**（SOFIは流動/非流動を分けず合算タグ`DebtLongtermAndShorttermCombinedAmount`を`long_term_debt`に固定済みのため、`short_term_debt`側に同種の合算タグを追加すると二重計上になる） |
| tag_definitions.py（TAG_CANDIDATES） | quarterly.py/parser.py双方に波及するため、変更前後で全銘柄のbuild_raw_table/_extract_values出力を比較し影響銘柄を特定（同日生成のcompany_facts.jsonで新旧比較すること。generated_atタイムスタンプ差だけで見かけ上の差分が出るため単純な過去ファイル比較は不可。raw/は2026-08-05にデッドコード除去のため廃止済み、normalized/の`generated_at`フィールドで同様の注意が必要）→ 影響銘柄のみupdate.py → audit.py |
| contracts.py（FinancialEntry必須キー変更等） | quarterly.py::save_raw_table()・normalizer.py::save_normalized()の検証が全銘柄で走るため、変更後は全105銘柄のupdate.pyを実行しContractViolationが新規発生しないか確認 → report_consistency_check.py |
| data_fetcher.py（TTMReader・_select_fcf_source） | 全銘柄fcf_list_raw/fcf_5yr_avgに影響するため全銘柄pipeline.py再実行 → report_consistency_check.py |
| common/market_data/fetcher.py（daily/attributes/analyst_history各層のスキーマ・取得ロジック） | スキーマ変更時は影響銘柄でfetch_daily_prices/fetch_weekly_attributes/fetch_analyst_eventsを再実行しJSON構造を更新 → 本番消費者8ファイル（beta_fetcher.py/data_fetcher.py/valuation_fetcher.py/pipeline.py/collect.py/collect_and_send.py/breadth_calculator.py/hypecore.py）のpipeline.py再実行で反映確認。診断ツール2（audit.py/score_verifier.py）・周辺ツール2（extract_key_facts.py/backfill_tech_pulse.py）を含め全12ファイル切替済み |
| common/market_data/reader.py（get_latest_price/get_attributes/get_ma_deviation/get_earnings_history/get_recommendations_history/get_price_on_or_after/get_price_series_as_of等の読み取りAPI） | 戻り値の意味・キーを変更する場合は本番消費者8ファイル＋診断ツール2ファイル（score_verifier.py・audit.py）＋周辺ツール2ファイル（extract_key_facts.py・backfill_tech_pulse.py）の全12ファイルへ影響するため、変更前に全消費者のgrep洗い出し必須。get_ma_deviation()のwindow引数の意味変更はma200代数逆算（data_fetcher.py）に直結するため特に注意 |
| extract_key_facts.py | EPS quarterly.json 再生成 → report_consistency_check.py（CHECK-17/19確認）|
| core_calculator.py / calculator/dcf.py | 影響銘柄のpipeline.py再実行 |
| calculator/adjustments.py | 影響銘柄のpipeline.py再実行（FCF外れ値・estimate_fcf等）。`check_software_system_reclassification()`（FCF-CONVRATE-DESIGN-LIMIT-1、2026-07-14追加）はconfig書き換えを行わない純関数で、`determine_fcf_base()`と同じ「pipeline.py実行のたびに実績データから再判定」パターンを踏襲している。今後この種の自己補正ロジックを追加する際も同パターンを踏襲すること |
| config/fcf_conversion_config.json（`estimate_fcf_from_eps()`が参照。ticker_overrides / sector_conversion_rates。2026-08-15、`src/value/tanuki_valuation/`から移動） | 影響銘柄のpipeline.py再実行（EPS推定FCFのconversion_rate変更時）。sector_conversion_ratesのキーはDamodaran業種カテゴリの省略形（下記beta_config.json/SECTOR_TO_DAMODARANと同一タクソノミー）だが、114分類中10分類（`Software_System_Mature`/`_SaaS`分割後）しかカバーしておらず、該当なしの銘柄は一律default(0.70)になる点に注意（[[FCF-CONVRATE-DESIGN-LIMIT-1]]参照。SECTOR-FCF-RATE-BROKEN-1で2026-07-14完了、Software_Systemグループ分割も2026-07-14完了）。`Software_System`（未分割・0.80）はIOT/QBTS/RBRK/S/SOUN等の判定保留銘柄向けに残置している |
| `FCF_CYCLICAL_VOLATILITY_TICKERS`（FCF-CONVRATE②、TRUST-SUMMARY-EPIC-1、2026-07-18新設。業界サイクルにより固定転換率が実態を表現できないと個別原因分析で確定した銘柄の手動リスト。閾値〈cv・divergence_ratio〉による自動判定はLLYがDOCNを両軸で上回るなど数学的に分離不可能と判明済みのため不採用） | `stock.html`（バナー表示）と`pipeline.py`（report.txtのdivergence_ratio表示）の**2箇所に同一のSetが独立定義**されており、共通configファイル経由ではない。3件目以降を追加する場合は個別原因分析（業界サイクル起因かどうかの確認）を経た上で**両ファイルに同期して追記**すること（片方のみの追記だとバナー・report.txtの表示が食い違う）。Classification判定ロジックには一切参照されない表示専用の定数（`FCF_LOW_RELIABILITY_SECTORS`と並ぶ第2の個別ティッカーベース信頼性フラグ機構。既存のFCF_LOW_RELIABILITY_SECTORSは業種ベース・stock.html単独定義で、pipeline.py側の対応はない） |
| `GROWTH_STRUCTURAL_MISMATCH_TICKERS`（GROWTH-STRUCTURAL-MISMATCH-CANDIDATES-1、TRUST-SUMMARY-EPIC-1段階1可視化、2026-07-20新設。ハイパーグロース事業と成熟業種平均〈Damodaran業種分類〉との構造的ミスマッチが原因分析で確定した14銘柄〈AMD/NVDA/ONDS/ASTS/BKNG/BROS/ELF/KULR/LLY/TER/XOM/ALAB/IONQ/RCAT〉の手動リスト。`FCF_CYCLICAL_VOLATILITY_TICKERS`と同型の設計） | `stock.html`（`#growth-sanity-container`内のsanityHTMLバナー）と`pipeline.py`（report.txtの[4. 成長率根拠]セクション、signals/warnings表示直後）の**2箇所に同一のSetが独立定義**されており、共通configファイル経由ではない。TERのみ業界平均比ではなく自社実績比での警告のため注記文言を専用に分岐させている。追加・削除時は両ファイルへの同期を忘れないこと。Classification判定ロジックには一切参照されない表示専用の定数 |
| config/beta_config.json（`overrides[ticker].sector`。`data_fetcher.py::_load_beta_config()`が正しいパスで読み込む共通ローダーで、`core_calculator.py::estimate_fcf_from_eps()`〈FCF転換率〉と`pipeline.py::_load_beta_sector()`〈growth_sanity向け〉の両方から参照される） | 影響銘柄のpipeline.py再実行。`sector`値は`growth_sanity.py::SECTOR_TO_DAMODARAN`の「beta_config.json形式」ブロック（例: `Semiconductor`/`Software_Internet`/`Aerospace_Defense`/`Software_System_Mature`/`Software_System_SaaS`）のキー形式で統一すること |
| src/value/tanuki_valuation/beta_fetcher.py（β自動取得＋`classify_software_system_subgroup()`〈FCF-CONVRATE-DESIGN-LIMIT-1、2026-07-14追加〉） | β変更時: 影響銘柄のpipeline.py再実行。`--classify-software-system`は新規銘柄登録時（sector="Software_System"の未分類銘柄）に前受収益/売上高比率（company_facts.jsonから算出、閾値0.40）でMature/SaaSを暫定分類する。CLAUDE_CODE_START.mdのStep 2.5として登録手順に組み込み済み |
| growth_sanity.py::TICKER_INDUSTRY_OVERRIDES / SECTOR_TO_DAMODARAN（ticker→Damodaran業種名の直接上書き辞書・sector省略キー→正式業種名の変換表。`beta_config.json`の`sector`より優先順位が高い） | 影響銘柄のpipeline.py再実行。妥当性検証はDamodaran公式データセット`docs/value-monitor/tanuki_valuation/common/damodaran_cache/indname.xls`（企業別48,157社の実分類データ、`Exchange:Ticker`列を主要取引所限定でticker照合すると該当企業の実際のIndustry Groupが直接引ける）と突き合わせて行う |
| config/adjustment_items.json（一過性費用・調整項目のXBRLタグ定義。EPS Analyzerとcalculator/adjustments.pyのFCF外れ値判定`TRANSIENT_CATEGORIES`が共に参照） | EPS Analyzer全銘柄再実行 → 影響銘柄のtanuki pipeline.py再実行（fcf_outlier判定への波及）|
| calculator/rice.py | 影響銘柄のpipeline.py再実行 |
| config/maturity_config.json | 影響銘柄のpipeline.py再実行（alpha上限・WACC・成熟度設定変更時）|
| growth_sanity.py（Damodaran業種別成長率ベンチマーク。fundgrEB.xls〈ROC/再投資率/期待EBIT成長率〉を`docs/value-monitor/tanuki_valuation/common/damodaran_cache/`から読み込み） | HypeCoreデータ確認 → 影響銘柄のpipeline.py再実行 |
| hypecore結果（hypecore_results.json） | growth_sanity経由でDCF成長率が変わるため影響銘柄のpipeline.py再実行 |
| hypecore_results（poc.json）更新時 | 影響銘柄のpipeline.py再実行（hypecore_history/{TICKER}.jsonが更新される） |
| pipeline.py | audit.py → 全銘柄pipeline.py再実行 |
| discover/stonks-silo/src/pipeline.py（runway計算ロジック） | TANUKI VALUATION側のMatrix③・Runwayペナルティも影響を受けるため、影響銘柄のtanuki pipeline.py再実行を確認 |
| Market Pulse / MACRO PULSE 各スクリプト | 独立しているため他システムへの影響なし |

---

## SYSTEM_MAP更新タイミング

以下の変化が生じたときのみ更新する（毎作業ごとの更新は不要）：
- 新しいシステムを追加したとき
- システム間に新しいデータの依存関係が生まれたとき
- 主要ファイルの役割・配置が変わったとき

**2026-07-10の教訓:** 上記は「変化が生じたとき」の更新基準だが、今回のように
既存の構造（銘柄振り分けの正本・STONKS SILOとTANUKI VALUATIONの依存関係・
出力先パスの誤記等）が長期間未記載/誤記のまま気づかれないケースもある。
CHAT_RULES.mdの「一日の作業終了時のブラッシュアップ」にSYSTEM_MAP.md点検を
定例項目として追加した（詳細はCHAT_RULES.md参照）。
