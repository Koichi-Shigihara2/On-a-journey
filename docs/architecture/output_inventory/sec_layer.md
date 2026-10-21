# SECデータ層の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`sec_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-20、チャット側の案をKoichiさんが了承）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 21（ファイル 19・人が見るもの 2）／読み手0件 0／テストだけ 0／1件 6／2件以上 13／規則の二重 6（検出式 6種のうち。SEC層のファイルを含むもの 5）

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 3件／解決できた読み込み 58件（4.9%）→ 当たらない

## 消したもの（段階2、2026-10-20）

- `common/sec_data/data/{T}/share_unit_fix_log.json`: 書き手 parser.py:SECParser._save_share_unit_fix_log と呼び出し、既存15ファイル。単位を直す処理は残す
- `common/sec_data/data/{T}/spac_shell_detection_log.json`: 書き手 parser.py:SECParser._save_spac_shell_detection_log と呼び出し、既存98ファイル。SPACの時期の値を外す処理は残す
- `common/sec_data/store_v2/{T}.json`: layer3_builder.py の save_ticker_store・build_and_save・手動のCLI・STORE_V2_DIR、.gitignore の行。その場で計算する読み手は変えない
- `scripts/analysis の14ファイル`: macro_alfred_lib・macro_claims_slot_rows_repair・macro_events_known_at_repair・macro_events_revision_repair・macro_events_slot_rows_repair・macro_liquidity_m2_yoy_repair・macro_liquidity_row_date_repair・macro_liquidity_weekly_before_after・macro_m4_hollow_rally・macro_m4_m2_percentile・macro_m4_score_vs_recession・macro_m4_thresholds・macro_m5_threshold_simulation・macro_ticker_history_check（.py）
- `scripts/analysis/futures_roll_suspects.py と src/market/market_pulse/backfill_futures_roll.py`: 組で削除（2026-10-20、保留の決着）。一過性の修復の道具で、互いの参照のほかに読み手0件。付けた印（roll_suspect 等）のデータは残る
- `scripts/analysis/macro_cfnai_ma3_rows_repair.py`: 削除（2026-10-20、保留の決着）。修復は 70793c2185（2026-10-03）で実行済み。browser_checks/check_macro_pulse.py の N-07 の説明の文から名前を消した

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `common/sec_data/data/{T}/bs_identity_violations_log.json`<br>（98ファイル） | 貸借対照表の資産＝負債＋資本が合わない期の記録 | `parser.py:SECParser._save_bs_identity_violations_log` | 1・テスト2 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`<br>（4ファイル） | 社名・CIKが変わった銘柄の、旧CIKのXBRLの生データ | `fetcher.py:SECFetcher._fetch_legacy_company_facts` | 1 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/fy_collision_log.json`<br>（98ファイル） | 同じ会計年度に2つの年次が当たった記録（解析の内部の記録） | `parser.py:SECParser._save_fy_collision_log` | 1・テスト4 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/fy_tag_mismatch_log.json`<br>（98ファイル） | 申告の会計年度のタグと期末日が合わない記録（解析の内部の記録） | `parser.py:SECParser._save_fy_tag_mismatch_log` | 1・テスト3 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/fye_boundary_collision_log.json`<br>（98ファイル） | 会計年度末の境目で期がぶつかった記録（解析の内部の記録） | `parser.py:SECParser._save_fye_boundary_collision_log` | 1・テスト3 | — | — | — | 残す（2026-10-20） |
| 1件 | `docs/common/sec_data/rotce/_summary.json`<br>（1ファイル） | ROTCEの全銘柄のまとめ | `rotce.py:build_all` | 1 | value-monitor/tanuki_score/index.html（直接） | — | — | 残す（2026-10-20） |
| 2件以上 | `${RUNNER_TEMP}/sec_update_result.json` | SEC_Data_Updateの1回の実行の結果（成功・失敗の銘柄）。後の段の通知が読む | `update.py:_finalize` | 2・テスト1（2） | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/_cik_cache.json`<br>（0ファイル） | 銘柄とCIKの対応の控え | `fetcher.py:SECFetcher._save_cik_cache` | 2 | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/_freshness.json`<br>（1ファイル） | 銘柄ごとのSECデータの新しさ（REFRESHED・SEC_LAG・FAILED等） | `freshness.py:write` | 4・テスト5 | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/annual_{YYYY}.json`<br>（1392ファイル） | 生データから1年分ずつ取り出した年次の財務（損益・貸借・CF） | `parser.py:SECParser.save_parsed_data` | 13・テスト20 | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（経由）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/company_facts.json`<br>（98ファイル） | SECから取った、その銘柄の全XBRLの申告の生データ | `fetcher.py:SECFetcher.fetch_company_facts` | 24・テスト18 | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（経由）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL） | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/quarterly_{Q}.json`<br>（3425ファイル） | 生データから1四半期分ずつ取り出した四半期の財務 | `parser.py:SECParser.save_parsed_data` | 7・テスト10 | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（経由）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/submissions.json`<br>（98ファイル） | SECから取った、その銘柄の提出書類の一覧（提出日・対象期末） | `fetcher.py:SECFetcher.fetch_submissions` | 5・テスト10 | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/submissions_legacy_{CIK}.json`<br>（4ファイル） | 社名・CIKが変わった銘柄の、旧CIKの提出書類の一覧 | `fetcher.py:SECFetcher._fetch_legacy_submissions` | 2 | — | — | — | 残す（2026-10-20。fetcher.py の _fetch_legacy_submissions が旧CIKの取得の24時間キャッシュと、取得に失敗したときの代わりとして読む） |
| 2件以上 | `common/sec_data/dimension_aggregate_registry.json`<br>（1ファイル） | セグメント等の内訳タグを合計して使う銘柄・項目の登録簿 | `dimension_aggregate_fetcher.py:_save_dimension_aggregate_registry` | 2（1） | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/normalized/{T}_quarterly_normalized.json`<br>（98ファイル） | 四半期の財務を項目ごとの時系列に並べ直し、Q4の逆算を足したもの | `normalizer.py:save_normalized` | 9・テスト15 | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（経由）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-071・AS-IS-093・AS-IS-096 | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/ttm/{T}_ttm_series.json`<br>（98ファイル） | 直近4四半期の合計（TTM）の時系列 | `ttm_calculator.py:save_ttm_series` | 9・テスト13（2） | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（経由）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-046 | 残す（2026-10-20） |
| 2件以上 | `config/cik_lookup.csv`<br>（1ファイル） | 登録銘柄の一覧（CIK・状態・各システムに載せるかの印）。新規登録の検証で状態を書き換える | `registration_validator.py:promote_ticker_status` | 33・テスト25 | common/ticker-links.js（直接）<br>index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（直接）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL） | AS-IS-273 | 残す（2026-10-20） |
| 2件以上 | `docs/common/sec_data/rotce/{T}.json`<br>（98ファイル） | 銘柄ごとのROTCE（有形自己資本利益率）の計算結果 | `rotce.py:build_all` | 4・テスト3 | value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/tanuki_score/index.html（直接） | — | — | 残す（2026-10-20） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `${GITHUB_STEP_SUMMARY}` | GitHub Actionsの実行の画面に出す要約（人が見る） | `audit.py:main`<br>`rollback_failed.py:main`<br>`update.py:_finalize` | .github/workflows/SEC_Data_Audit.yml<br>.github/workflows/SEC_Data_Update.yml |
| `Discord（DISCORD_WEB_HOOK）` | SECデータの監査の結果のDiscordへの通知 | `audit.py:post_discord` | .github/workflows/SEC_Data_Audit.yml<br>.github/workflows/SEC_Data_Update.yml<br>.github/workflows/System_Health.yml |

## 読み手の内訳

### `common/sec_data/data/{T}/bs_identity_violations_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `ticker`(3)・`violations`(1)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2414・2418・3251・3455・3701（経由 `common/sec_data/report_consistency_check.py::_read_bs_identity_violations_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_bs_identity_violations_log`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_bs_identity_violations_log`）

### `common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `cik`(1)・`entityName`(0)・`facts`(1)
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 246・437（経由 `common/sec_data/fetcher.py::SECFetcher._read_json_or_none`）

### `common/sec_data/data/{T}/fy_collision_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `collisions`(3)・`ticker`(4)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2354・2358・3055・3455・3701（経由 `common/sec_data/report_consistency_check.py::_read_fy_collision_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_fy_collision_log`）
- テスト・保存済みファイル: `tests/test_fy_collision_log_dedup.py` 行 44・62・78・86
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_fy_collision_log`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 326

### `common/sec_data/data/{T}/fy_tag_mismatch_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `mismatches`(2)・`ticker`(4)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2374・2378・3074・3455・3701（経由 `common/sec_data/report_consistency_check.py::_read_fy_tag_mismatch_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_fy_tag_mismatch_log`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_fy_tag_mismatch_log`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 283・304

### `common/sec_data/data/{T}/fye_boundary_collision_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `collisions`(2)・`ticker`(4)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2394・2398・3092・3455・3701（経由 `common/sec_data/report_consistency_check.py::_read_fye_boundary_collision_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_fye_boundary_collision_log`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_fye_boundary_collision_log`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 370・389・416

### `docs/common/sec_data/rotce/_summary.json`（1件）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `generated_at`(1)・`reference_only`(0)・`thresholds`(0)・`tickers`(1)
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 241

### `${RUNNER_TEMP}/sec_update_result.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- Python・その場で計算: `common/sec_data/fetcher.py` 行 603（経由 `common/sec_data/fetcher.py::SECFetcher`）
- Python・その場で計算: `common/sec_data/freshness.py` 行 103・128（経由 `common/sec_data/freshness.py::utc_now_iso`）
- Python・保存済みファイル: `common/sec_data/rollback_failed.py` 行 30
- Python・保存済みファイル: `common/sec_data/update_notify.py` 行 38・65・82・95（経由 `common/sec_data/update_notify.py::load_result`）
- テスト・その場で計算: `tests/test_sec_freshness.py` 行 84（経由 `common/sec_data/fetcher.py::SECFetcher`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 86・254・327・334・341・348・355（経由 `common/sec_data/update_notify.py::build_message`）

### `common/sec_data/data/_cik_cache.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 84・85・103（経由 `common/sec_data/fetcher.py::SECFetcher._load_cik_cache`）
- ワークフロー・保存済みファイル: `.github/workflows/SEC_Data_Update.yml` 行 108

### `common/sec_data/data/_freshness.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `counts`(3)・`generated_at`(4)・`tickers`(6)
- Python・保存済みファイル: `common/sec_data/freshness.py` 行 19・115
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1793・3623・3701（経由 `common/sec_data/freshness.py::load`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 338・355・428・432（経由 `common/sec_data/update.py::_finalize`）
- Python・保存済みファイル: `common/system_health.py` 行 876・956・1051・1110（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 351・375・386・391・397・417・418・423・428・429・434・435 ほか8（経由 `common/sec_data/freshness.py::load`、`tests/test_sec_freshness.py::_freshness_doc`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 99・205・224・238・253・262・263・264・268・269・270・278 ほか4（経由 `common/sec_data/freshness.py::load`、`common/sec_data/update.py::_finalize`、`tests/test_sec_update_ticker_failure.py::_read`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/sec_data/freshness.py::load`）

### `common/sec_data/data/{T}/annual_{YYYY}.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `bs`(12)・`bs_provenance`(7)・`bs_tag_sources`(1)・`cf`(9)・`cf_provenance`(4)・`form`(8)・`other`(2)・`other_provenance`(0)・`period`(13)・`pl`(17)・`pl_provenance`(4)・`shares`(3)・`shares_provenance`(0)・`ticker`(16)
- Python・保存済みファイル: `common/macro_data/migrate_bamlh0a0hym2_history.py` 行 103・209（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 75・201・222・368・369・408（経由 `common/screening/dcf_validity_checker.py::_get_annual_years`）
- Python・保存済みファイル: `common/sec_data/parser.py` 行 505・811・4001・4057（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- Python・保存済みファイル: `common/sec_data/reader.py` 行 134・158・159・184・208・222・234・243・274・296・322・338 ほか15（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_annual_range`、`common/sec_data/reader.py::SECReader.get_data_summary`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 484・656・663・730・811・818・1172・2613・2617・2623・2646・2782 ほか6（経由 `common/screening/dcf_validity_checker.py::_get_annual_years`、`common/sec_data/reader.py::SECReader.get_annual_range`、`common/sec_data/report_consistency_check.py::_check_fixed_registry_integrity` ほか）
- Python・保存済みファイル: `common/sec_data/roic.py` 行 77・85
- Python・保存済みファイル: `discover/stonks-silo/src/financial_as_of.py` 行 54・87（経由 `discover/stonks-silo/src/financial_as_of.py::scoring_fy_end`）
- Python・保存済みファイル: `discover/stonks-silo/src/pipeline.py` 行 121・275（経由 `common/sec_data/reader.py::SECReader.get_annual_range`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/execution_metrics.py` 行 78・85・123（経由 `common/sec_data/roic.py::calc_roic_wacc_ratio`、`src/value/adjusted_eps_analyzer/execution_metrics.py::calc_roic_trend`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 829（経由 `src/value/adjusted_eps_analyzer/execution_metrics.py::calc_roic_trend`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 456・1023（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 596・599・603・625・628・632・636・689・895（経由 `common/sec_data/reader.py::SECReader.get_annual_range`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 427・445・451・454・490・991・1004・1005・1047・1069・1115・2836 ほか9（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_annual_range`、`common/sec_data/roic.py::calc_roic_wacc_ratio` ほか）
- テスト・保存済みファイル: `tests/test_bs_field_cross_year_misselect.py` 行 304・314（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- テスト・保存済みファイル: `tests/test_check31_fields_snapshot_hash.py` 行 63・67・75・82・96・105（経由 `common/sec_data/report_consistency_check.py::_check_fixed_registry_integrity`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 40・89（経由 `common/sec_data/report_consistency_check.py::_check_operating_income_reconstruction_scope`、`common/sec_data/report_consistency_check.py::_check_revenue_net_income_reconciliation`）
- テスト・保存済みファイル: `tests/test_conditional_tags_and_stale_latest.py` 行 105（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `common/sec_data/reader.py::SECReader.get_annual_range`）
- テスト・保存済みファイル: `tests/test_dcf_validity_checker.py` 行 77・86・95・107・118・128・133・143・155・165（経由 `common/screening/dcf_validity_checker.py::_get_annual_years`）
- テスト・保存済みファイル: `tests/test_execution_metrics.py` 行 84・98・102・139（経由 `src/value/adjusted_eps_analyzer/execution_metrics.py::calc_roic_trend`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 123・133（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 461・500・522・2005・2066・2079・2100・2119・2146・2162・2175・2315 ほか23（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`、`common/sec_data/reader.py::SECReader.get_annual_range`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_pl_field_cross_accn_alignment.py` 行 414・431（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- テスト・保存済みファイル: `tests/test_reader_fcf_list_with_dates.py` 行 22・36・37・44（経由 `common/sec_data/reader.py::SECReader.get_annual_range`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_operating_income_reconstruction_scope`）
- テスト・保存済みファイル: `tests/test_roic_shared_module.py` 行 40・50・56・65・76・88・97・107・118・129・148・149 ほか2（経由 `common/sec_data/roic.py::calc_roic_wacc_ratio`）
- テスト・保存済みファイル: `tests/test_runway_cash_unify.py` 行 149・158・169・170・172（経由 `common/sec_data/reader.py::SECReader.get_annual_range`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_stonks_silo_financial_as_of.py` 行 68・72・73・75（経由 `discover/stonks-silo/src/financial_as_of.py::scoring_fy_end`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_annual_revenues`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/sec_data/data/{T}/company_facts.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `cik`(3)・`entityName`(2)・`facts`(17)
- Python・保存済みファイル: `common/sec_data/audit.py` 行 157・160・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 509・513・516
- Python・保存済みファイル: `common/sec_data/freshness.py` 行 23
- Python・保存済みファイル: `common/sec_data/fye_change_candidate_scan.py` 行 100・151・168（経由 `common/sec_data/fetcher.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/layer3_builder.py` 行 207・211・214・1262（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/newfield_q4_cutoff_check.py` 行 133・178（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/parser.py` 行 482・485・489・4057・4067（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- Python・保存済みファイル: `common/sec_data/quarterly.py` 行 164
- Python・保存済みファイル: `common/sec_data/reader.py` 行 55・814・837（経由 `common/sec_data/layer3_builder.py::load_company_facts`、`common/sec_data/reader.py::SECReader.get_data_summary`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 423・521・692・706・708（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1432・1438・1551・1554・1802・1926・2022・2632・2634・3215・3218・3455 ほか3（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/report_consistency_check.py::_check_annual_da_partial_concept`、`common/sec_data/report_consistency_check.py::_check_sec_freshness` ほか）
- Python・保存済みファイル: `common/sec_data/revenue_tag_conflict_check.py` 行 91・93・176・188（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/revenue_tag_conflict_check.py::check_revenue_tag_conflict`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 415・421・674・706（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 53・95・118・173・175・183・185・191・193・230・241・323 ほか3（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/layer3_builder.py::load_company_facts`、`common/sec_data/parser.py::SECParser.parse_company_facts` ほか）
- Python・保存済みファイル: `common/system_health.py` 行 86・788・1038・1049・1110（経由 `common/sec_data/audit.py::audit_ticker`、`common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- Python・保存済みファイル: `discover/stonks-silo/src/financial_trend_calculator.py` 行 466・468（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `discover/stonks-silo/src/pipeline.py` 行 147・150・275（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/tail/kpi_proposer.py` 行 212・573・743・801・842（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 203・1574（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 137・207（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/tail/xbrl_segment_fetcher.py` 行 427・557（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 261・263・515・1060・1267（経由 `common/sec_data/layer3_builder.py::load_company_facts`、`src/value/hypecore/hypecore.py::fetch_quarterly_fundamentals`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/segment_growth_xbrl.py` 行 218・262・313・372（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 402・1178・2967・3209・3441・3581・3617（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_bs_field_cross_year_misselect.py` 行 304・314（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::check_ticker`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 185・190・195（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_conditional_tags_and_stale_latest.py` 行 50・69・80・86・87・89・94・101・102・105・116・118 ほか1（経由 `common/sec_data/layer3_builder.py::load_company_facts`、`common/sec_data/parser.py::SECParser.parse_company_facts`、`tests/test_conditional_tags_and_stale_latest.py::_real_store`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 251・291・299・357・412・464・473（経由 `common/sec_data/layer3_builder.py::load_company_facts`、`src/value/hypecore/hypecore.py::fetch_quarterly_fundamentals`）
- テスト・保存済みファイル: `tests/test_layer3_formula_yoy.py` 行 44・67・85・114・129・147（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2625・2631（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- テスト・保存済みファイル: `tests/test_pl_field_cross_accn_alignment.py` 行 414・431（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::check_ticker`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 720・734・749・764・789・808・1546・1555・1559・1563・1567・1571 ほか6（経由 `common/sec_data/report_consistency_check.py::_check_annual_da_partial_concept`、`tests/test_report_consistency_check.py::TestCheck28TransitionFormExclusion._write_company_facts`、`tests/test_report_consistency_check.py::TestCheck55AnnualDaPartialConcept._setup`）
- テスト・保存済みファイル: `tests/test_rotce.py` 行 64・70・81・94・96・101・110・119・130・138・145・153 ほか20（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 121・122・149・153・161・168・174・179・184・186・195・196 ほか20（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/registration_validator.py::check_p4_orphan_configs`、`common/sec_data/report_consistency_check.py::_check_sec_freshness` ほか）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 99・224・234・247・251・262・263・264・268・269・270・274 ほか2（経由 `common/sec_data/report_consistency_check.py::_check_sec_freshness`、`common/sec_data/update.py::process_ticker`、`tests/test_sec_update_ticker_failure.py::_read`）
- テスト・保存済みファイル: `tests/test_share_count_outlier_check.py` 行 30・39・40・45・46・51・53・60・62・68・69・73 ほか1（経由 `common/sec_data/report_consistency_check.py::_check_share_count_outliers`、`tests/test_share_count_outlier_check.py::_write`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/kpi_proposer.py → docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/_summary.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/sec_data/data/{T}/quarterly_{Q}.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `bs`(5)・`bs_tag_sources`(0)・`cf`(6)・`data_quality`(2)・`form`(3)・`other`(1)・`period`(8)・`pl`(7)・`shares`(2)・`ticker`(9)
- Python・保存済みファイル: `common/macro_data/migrate_bamlh0a0hym2_history.py` 行 103・209（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `common/sec_data/reader.py` 行 134・159・184・207・208・222・234・243・274・296・322・338 ほか15（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_data_summary`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1172・2646・3455（経由 `common/sec_data/reader.py::SECReader.get_net_cash`）
- Python・保存済みファイル: `discover/stonks-silo/src/pipeline.py` 行 121・275（経由 `common/sec_data/reader.py::SECReader.get_net_cash`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 456・1023（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 596・599・603・625・628・632・636・689・895（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_net_cash`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 427・454・490・991・1047・1069・1115・3098・3861（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_net_cash`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `common/sec_data/reader.py::SECReader.get_net_cash`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 123・133（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 461・500・522・2005・2066・2079・2100・2119・2146・2162・2175・2315 ほか22（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_net_cash`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_reader_fcf_list_with_dates.py` 行 22・37・44（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_runway_cash_unify.py` 行 149・158・169・170・172（経由 `common/sec_data/reader.py::SECReader.get_net_cash`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/sec_data/data/{T}/submissions.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `accn_to_filingdate`(3)・`accn_to_reportdate`(5)・`cik`(3)・`former_names`(2)・`legacy_ciks_merged`(2)・`ticker`(10)
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 529・533・536・551・555・558
- Python・保存済みファイル: `common/sec_data/parser.py` 行 498・503・4057・4067（経由 `common/sec_data/fetcher.py::load_former_names`、`common/sec_data/fetcher.py::load_submissions`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 517・670・823・2617・2623・3231・3455・3701（経由 `common/sec_data/fetcher.py::load_submissions`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 183（経由 `common/sec_data/fetcher.py::load_submissions`）
- Python・保存済みファイル: `discover/stonks-silo/src/financial_as_of.py` 行 64・87（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_bs_field_cross_year_misselect.py` 行 304・314（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 40・89（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_conditional_tags_and_stale_latest.py` 行 105（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2631（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_pl_field_cross_accn_alignment.py` 行 414・431（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_preflight_check.py` 行 187・193・200・213
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 726・737・752・767・793・811（経由 `tests/test_report_consistency_check.py::TestCheck28TransitionFormExclusion._write_submissions`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 263・271・280・330・336
- テスト・保存済みファイル: `tests/test_stonks_silo_financial_as_of.py` 行 66・68・72・73・75（経由 `common/sec_data/fetcher.py::load_submissions`）

### `common/sec_data/data/{T}/submissions_legacy_{CIK}.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `accn_to_filingdate`(2)・`accn_to_reportdate`(1)・`cik`(1)・`ticker`(2)
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 186・239・246・488・606（経由 `common/sec_data/fetcher.py::SECFetcher._read_json_or_none`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 91（経由 `common/sec_data/fetcher.py::SECFetcher._read_json_or_none`）

### `common/sec_data/dimension_aggregate_registry.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `0000813672-26-000016|2025-01-01~2025-12-31`(0)・`0000896878-25-000035|2024-08-01~2025-07-31`(0)・`0001193125-09-239249|2008-09-30`(0)・`0001193125-21-060650|2019-12-31`(0)・`0001213900-20-008037|2019-12-31`(0)・`0001341766-26-000024|2025-12-31`(0)・`0001579091-24-000027|2023-12-31`(0)・`0001579091-25-000015|2024-12-31`(0)・`0001579091-26-000018|2025-12-31`(0)
- Python・その場で計算: `common/sec_data/parser.py` 行 3726・3939（経由 `common/sec_data/dimension_aggregate_fetcher.py::load_dimension_aggregate_registry`）
- Python・保存済みファイル: `common/sec_data/dimension_aggregate_fetcher.py` 行 73・95・407・433・439（経由 `common/sec_data/dimension_aggregate_fetcher.py::load_dimension_aggregate_registry`、`common/sec_data/dimension_aggregate_fetcher.py::main`）
- Python・保存済みファイル: `common/sec_data/parser.py` 行 505・771・823・3726・3840・3939（経由 `common/sec_data/dimension_aggregate_fetcher.py::load_dimension_aggregate_registry`）

### `common/sec_data/normalized/{T}_quarterly_normalized.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `fields`(13)・`generated_at`(2)・`ticker`(15)
- Python・保存済みファイル: `common/macro_data/migrate_bamlh0a0hym2_history.py` 行 103・209（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `common/sec_data/reader.py` 行 134・159・184・208・222・234・243・274・296・322・338・343 ほか16（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`、`common/sec_data/reader.py::SECReader.get_rpo_series`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 929・932・1172・1742・1744・2631・2646・3455・3609・3701（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`、`common/sec_data/report_consistency_check.py::_check_restated_annual`、`common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- Python・保存済みファイル: `common/sec_data/rollback_failed.py` 行 45・57・95・112（経由 `common/sec_data/rollback_failed.py::ticker_paths`）
- Python・保存済みファイル: `discover/stonks-silo/src/pipeline.py` 行 121・275（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 456・1023（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 596・599・603・625・628・632・636・689・895（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 427・454・490・1047・1115・3861（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`、`common/sec_data/reader.py::SECReader.get_rpo_context`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 336
- テスト・その場で計算: `tests/test_normalizer.py` 行 204（経由 `common/sec_data/normalizer.py::normalize`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_restated_annual`）
- テスト・保存済みファイル: `tests/test_check47_contiguous_window.py` 行 44・49・55・59・63（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`、`tests/test_check47_contiguous_window.py::_run`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23・27・28・41（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- テスト・保存済みファイル: `tests/test_first_filed.py` 行 149・164・165・172・173（経由 `common/sec_data/report_consistency_check.py::_check_restated_annual`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 123・133（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2005・2066・2079・2100・2119・2146・2162・2175・3305（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- テスト・保存済みファイル: `tests/test_reader_fcf_list_with_dates.py` 行 22・37・44（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_restated_annual`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 1154・1178・1188・1192・1202・1207・1218・1224・1235・1241・1251・1257 ほか8（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`、`tests/test_report_consistency_check.py::TestCheck47TtmParserLayer3Reconciliation._write_normalized`）
- テスト・保存済みファイル: `tests/test_runway_cash_unify.py` 行 149・158・169・170・172（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 224・236・248・260・262・263・264・268・269・270・277・278 ほか1（経由 `common/sec_data/rollback_failed.py::ticker_paths`、`tests/test_sec_update_ticker_failure.py::_read`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`

### `common/sec_data/ttm/{T}_ttm_series.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `generated_at`(2)・`n_periods`(0)・`series`(11)・`ticker`(18)
- Python・その場で計算: `common/sec_data/reader.py` 行 56（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- Python・その場で計算: `discover/stonks-silo/src/financial_as_of.py` 行 40（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 81・86・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 82・103・104・111・202・230・247・295・341・356・397・480 ほか10（経由 `common/sec_data/registration_validator.py::_load_json`、`common/sec_data/registration_validator.py::_ttm_end`、`common/sec_data/stonks_flag_rule.py::_latest_ttm`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 930・937・1867・2631・3455・3560・3701（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`、`common/sec_data/stonks_flag_rule.py::_latest_ttm`）
- Python・保存済みファイル: `common/sec_data/rollback_failed.py` 行 46・57・95・112（経由 `common/sec_data/rollback_failed.py::ticker_paths`）
- Python・保存済みファイル: `common/sec_data/stonks_flag_rule.py` 行 56・60・101（経由 `common/sec_data/stonks_flag_rule.py::_latest_ttm`）
- Python・保存済みファイル: `common/system_health.py` 行 86・788・1038・1049（経由 `common/sec_data/audit.py::audit_ticker`、`common/sec_data/registration_validator.py::_load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 244
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 490・991・1069・1306・1310・3093・3108・3370・3375・3861（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._save_result`）
- ワークフロー・保存済みファイル: `.github/workflows/SEC_Data_Update.yml` 行 110
- テスト・その場で計算: `tests/test_ttm_anchor_reach.py` 行 47・60・67・77・90（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- テスト・その場で計算: `tests/test_ttm_calculator.py` 行 95・121（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_check47_contiguous_window.py` 行 45・49・55・59・63（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`、`tests/test_check47_contiguous_window.py::_run`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 461・500・522・2315・2332・2375・2400・2429・2452・2464・2477・2488 ほか24（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`、`tests/test_pipeline_logic.py::_write_dupont_ttm`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82・161・168（経由 `common/sec_data/registration_validator.py::_load_json`、`common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 1162・1184・1188・1198・1202・1213・1218・1230・1235・1247・1251・1263 ほか8（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`、`tests/test_report_consistency_check.py::TestCheck47TtmParserLayer3Reconciliation._write_ttm`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 224・237・249・260・262・263・264・268・269・270・277・278 ほか1（経由 `common/sec_data/rollback_failed.py::ticker_paths`、`tests/test_sec_update_ticker_failure.py::_read`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_stonks_flag_rule.py` 行 38・48・49・55・60・65・71・76・82・105・111・120 ほか1（経由 `common/sec_data/stonks_flag_rule.py::_latest_ttm`、`tests/test_stonks_flag_rule.py::_write`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::_load_json`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._save_result`）
- テスト・保存済みファイル: `tests/test_ttm_flow_key_names.py` 行 39・62
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `config/cik_lookup.csv`（2件以上）
- 起動元: .github/workflows/Discover_Config_Sync.yml
- Python・保存済みファイル: `browser_checks/check_signal_consistency_section.py` 行 65（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `browser_checks/check_valuation_chart_basis.py` 行 184（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/registration/preflight_check.py` 行 163・166・212・223（経由 `common/registration/preflight_check.py::run_preflight_check`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 100・117・259・373・375・386・418・456・467（経由 `common/registration/preflight_check.py::run_preflight_check`、`common/registration/register_ticker.py::_load_cik_row`、`common/registration/register_ticker.py::register_one` ほか）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 393・398・448（経由 `common/screening/dcf_validity_checker.py::main`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/screening/report_txt_parser.py` 行 264・270・305（経由 `common/screening/report_txt_parser.py::main`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/config.py` 行 17・22・43・86（経由 `common/sec_data/config.py::_load_from_csv`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 474（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/fye_change_candidate_scan.py` 行 134・136・168（経由 `common/sec_data/fye_change_candidate_scan.py::main`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/newfield_q4_cutoff_check.py` 行 165・167・217（経由 `common/sec_data/newfield_q4_cutoff_check.py::main`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 55・405・486・491・500・521・692・706・708（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`、`common/sec_data/registration_validator.py::run`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1863・3414・3415・3560・3651・3701（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/revenue_tag_conflict_check.py` 行 165・167・188（経由 `common/sec_data/revenue_tag_conflict_check.py::main`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 705（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/tickers.py` 行 13・19・25・38・58・72・100・108・113・118・123・166（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 422（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/system_health.py` 行 293・294・370・756・788・1045・1049・1110（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `discover/stonks-silo/src/pipeline.py` 行 46・62・96・101・275（経由 `common/sec_data/tickers.py::_load`、`discover/stonks-silo/src/pipeline.py::_filter_stonks_silo_tickers`、`discover/stonks-silo/src/pipeline.py::run`）
- Python・保存済みファイル: `src/tail/kpi_proposer.py` 行 739・842（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 426・478（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/sec_items_fetcher.py` 行 670・702（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 326（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/text_kpi_extractor.py` 行 581・701（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/extract_key_facts.py` 行 50・161・190・480・1262（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 394・397・402・643・649・881・971・988・993・999・1068・1069 ほか1（経由 `common/sec_data/tickers.py::_load`、`src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`、`src/value/adjusted_eps_analyzer/pipeline.py::_filter_eps_tickers` ほか）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 1155・1236・1260・1262（経由 `src/value/hypecore/hypecore.py::_filter_hypecore_tickers`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 926
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 412・414・427・528・537・538・567・572・3840・3861（経由 `common/sec_data/tickers.py::_load`、`src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher.get_financials`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._filter_tanuki_tickers` ほか）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 112（経由 `common/sec_data/tickers.py::_load`）
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 15
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/admin.html` 行 280・393・526・530・577
- JS・HTML・保存済みファイル: `docs/value-monitor/admin.html` 行 935・1469
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_CIK_Lookup.yml` 行 42・76・134・154
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher.get_financials`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py` 行 103・123・143・163（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py` 行 106・128・151・174（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`）
- テスト・保存済みファイル: `tests/test_eps_cik_history_merge.py` 行 70・84・92（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`）
- テスト・保存済みファイル: `tests/test_eps_run_status.py` 行 95・115・125・133・151・160・174（経由 `src/value/adjusted_eps_analyzer/pipeline.py::run`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_net_income_selection.py` 行 107・154（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_share_fallback.py` 行 88・122・149・190（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_split.py` 行 100・124・149・176（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::load_cik_map`）
- テスト・保存済みファイル: `tests/test_flag_consumer_audit_3.py` 行 42・50・59・77・87（経由 `src/value/adjusted_eps_analyzer/pipeline.py::_filter_eps_tickers`、`src/value/hypecore/hypecore.py::_filter_hypecore_tickers`）
- テスト・保存済みファイル: `tests/test_iv_formula.py` 行 42（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_no_direct_ticker_access.py` 行 121・244・250（経由 `tests/test_no_direct_ticker_access.py::_find_cik_lookup_direct_parsers`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 3940・3945・3951・3956（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._filter_tanuki_tickers`）
- テスト・保存済みファイル: `tests/test_preflight_check.py` 行 179・192・205・218・229（経由 `common/registration/preflight_check.py::run_preflight_check`）
- テスト・保存済みファイル: `tests/test_register_ticker_preflight.py` 行 62・84・100・118（経由 `common/registration/register_ticker.py::register_one`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82・148（経由 `common/registration/register_ticker.py::register_one`、`common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_registration_validator.py` 行 65・72・96・110（経由 `tests/test_registration_validator.py::TestPromoteTickerStatus._write_csv`）
- テスト・保存済みファイル: `tests/test_score_verifier.py` 行 47・70（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_sec_data_config.py` 行 28・61・66・69・74・80・86・95・98（経由 `common/sec_data/tickers.py::_load`、`tests/test_sec_data_config.py::_write_csv`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_stonks_flag_rule.py` 行 105・111（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_stonks_silo_pipeline.py` 行 105・113・120・128（経由 `discover/stonks-silo/src/pipeline.py::_filter_stonks_silo_tickers`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_tickers.py` 行 25・50・55・60・64・70・74・78・82・85・86・89 ほか37（経由 `common/sec_data/tickers.py::_load`、`tests/test_tickers.py::_write_csv`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/tail/tail_dcf_bridge.py → docs/portfolio/scenario/{T}/{sc_name}.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/kpi_proposer.py → docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/_summary.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/common/sec_data/rotce/{T}.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `current`(3)・`generated_at`(2)・`history`(3)・`notes`(3)・`percentile`(3)・`reference_only`(1)・`tags`(1)・`ticker`(7)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1831・3635・3701（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 639
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 756
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 241
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）
- テスト・保存済みファイル: `tests/test_rotce.py` 行 345・350・365（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | SEC層か | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | `discover/stonks-silo/src/financial_trend_calculator.py:448` |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22`<br>`src/value/adjusted_eps_analyzer/pipeline.py:814` |
| `market_data_violations_log` | いいえ（後の段階） | — | `common/market_data/fetcher.py:315` |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。SEC層のファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | SEC層を含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:773`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:916`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:944` | はい |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:655` | はい |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1633`<br>`common/sec_data/report_consistency_check.py:1774`<br>`common/system_health.py:955`<br>`discover/stonks-silo/src/financial_as_of.py:70`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192` | はい |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:405`<br>`src/value/hypecore/hypecore.py:284` | はい |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | はい |

## 本番外のコード（scripts/analysis・browser_checks・discover/stonks-silo）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

### `scripts/analysis`（3ファイル・711行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `daily_intraday_rows_scan.py` | 123 | — | `common/market_data/fetcher.py` | — | — | 残す（2026-10-20） |
| `macro_events_sp500_asof_repair.py` | 110 | — | — | — | `tests/test_macro_pulse_accuracy.py` | 残す（2026-10-20） |
| `market_pulse_redesign.py` | 478 | — | — | — | — | 残す（2026-10-20。Market Pulse の作り直しの検討中。src/market/market_pulse/stage_conclusions.py が同じ値を使う） |

### `browser_checks`（6ファイル・3,208行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `check_dependency_map.py` | 458 | — | — | — | — | 残す（2026-10-20） |
| `check_macro_pulse.py` | 1,273 | — | — | — | — | 残す（2026-10-20） |
| `check_signal_consistency_section.py` | 206 | — | — | — | — | 残す（2026-10-20） |
| `check_tanuki_score_undetermined.py` | 86 | — | — | — | — | 残す（2026-10-20） |
| `check_valuation_chart_basis.py` | 213 | — | — | — | — | 残す（2026-10-20） |
| `market_pulse_elements.py` | 972 | — | — | `browser_checks/check_dependency_map.py` | — | 残す（2026-10-20） |

### `discover/stonks-silo`（6ファイル・2,620行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `src/analyzer.py` | 1,351 | — | — | `discover/stonks-silo/src/pipeline.py` | `tests/test_pipeline_logic.py`<br>`tests/test_runway_cash_unify.py`<br>`tests/test_stonks_silo_breakeven_unify.py`<br>`tests/test_stonks_silo_path_score.py`<br>`tests/test_stonks_silo_pipeline.py`<br>`tests/test_stonks_silo_yoy_period_validation.py` | 残す（2026-10-20） |
| `src/fetcher.py` | 298 | — | — | `discover/stonks-silo/src/analyzer.py`<br>`discover/stonks-silo/src/pipeline.py` | — | 残す（2026-10-20） |
| `src/financial_as_of.py` | 104 | — | — | `discover/stonks-silo/src/pipeline.py` | `tests/test_conditional_tags_and_stale_latest.py`<br>`tests/test_stonks_silo_financial_as_of.py` | 残す（2026-10-20） |
| `src/financial_trend_calculator.py` | 471 | — | — | `discover/stonks-silo/src/pipeline.py` | `tests/test_conditional_tags_and_stale_latest.py`<br>`tests/test_gate2_phase3b1_reader_integration.py`<br>`tests/test_stonks_silo_yoy_period_validation.py` | 残す（2026-10-20） |
| `src/pipeline.py` | 275 | `.github/workflows/Stonks_Silo_Update.yml` | — | — | `tests/test_pipeline_logic.py`<br>`tests/test_stonks_silo_financial_as_of.py`<br>`tests/test_stonks_silo_pipeline.py` | 残す（2026-10-20） |
| `src/valuation_fetcher.py` | 121 | — | — | `discover/stonks-silo/src/pipeline.py` | `tests/test_valuation_fetcher_market_data_switch.py` | 残す（2026-10-20） |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 3件
  - `common/sec_data/reader.py:776` SECReader._load_json → `<?>`
  - `common/sec_data/report_consistency_check.py:309` _check_discover_config_sync → `<?>`
  - `common/sec_data/report_consistency_check.py:311` _check_discover_config_sync → `<?>`
