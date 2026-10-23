# SECデータ層の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`sec_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-20、チャット側の案をKoichiさんが了承）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 21（ファイル 19・人が見るもの 2）／読み手0件 0／テストだけ 0／1件 7／2件以上 12／規則の二重 6（検出式 6種のうち。SEC層のファイルを含むもの 5）

**誰も読まないキー**: **129**（この段階のJSONのキー 12527 のうち。下の「誰も読まないキー」）

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 1件／解決できた読み込み 59件＋ファイル名で当たった行 74件（0.7%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

フォルダを決めきれない行（ファイル名で当たった行）:
- `.github/workflows/SEC_Data_Update.yml` 行 111（`common/sec_data/data/{T}/annual_{YYYY}.json`）: `annual_count=$(ls -1 $dir/annual_*.json 2>/dev/null \| wc -l)`

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
| 1件 | `common/sec_data/data/_cik_cache.json`<br>（0ファイル） | 銘柄とCIKの対応の控え | `fetcher.py:SECFetcher._save_cik_cache` | 1 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/bs_identity_violations_log.json`<br>（98ファイル） | 貸借対照表の資産＝負債＋資本が合わない期の記録 | `parser.py:SECParser._save_bs_identity_violations_log` | 1・テスト2 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`<br>（4ファイル） | 社名・CIKが変わった銘柄の、旧CIKのXBRLの生データ | `fetcher.py:SECFetcher._fetch_legacy_company_facts` | 1 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/fy_collision_log.json`<br>（98ファイル） | 同じ会計年度に2つの年次が当たった記録（解析の内部の記録） | `parser.py:SECParser._save_fy_collision_log` | 1・テスト2 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/fy_tag_mismatch_log.json`<br>（98ファイル） | 申告の会計年度のタグと期末日が合わない記録（解析の内部の記録） | `parser.py:SECParser._save_fy_tag_mismatch_log` | 1・テスト2 | — | — | — | 残す（2026-10-20） |
| 1件 | `common/sec_data/data/{T}/fye_boundary_collision_log.json`<br>（98ファイル） | 会計年度末の境目で期がぶつかった記録（解析の内部の記録） | `parser.py:SECParser._save_fye_boundary_collision_log` | 1・テスト2 | — | — | — | 残す（2026-10-20） |
| 1件 | `docs/common/sec_data/rotce/_summary.json`<br>（1ファイル） | ROTCEの全銘柄のまとめ | `rotce.py:build_all` | 1 | value-monitor/tanuki_score/index.html（直接） | — | — | 残す（2026-10-20） |
| 2件以上 | `${RUNNER_TEMP}/sec_update_result.json` | SEC_Data_Updateの1回の実行の結果（成功・失敗の銘柄）。後の段の通知が読む | `update.py:_finalize` | 2（2） | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/_freshness.json`<br>（1ファイル） | 銘柄ごとのSECデータの新しさ（REFRESHED・SEC_LAG・FAILED等） | `freshness.py:write` | 3・テスト5 | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/annual_{YYYY}.json`<br>（1392ファイル） | 生データから1年分ずつ取り出した年次の財務（損益・貸借・CF） | `parser.py:SECParser.save_parsed_data` | 17・テスト21 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-019・AS-IS-023・AS-IS-027・AS-IS-045・AS-IS-047 | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/company_facts.json`<br>（98ファイル） | SECから取った、その銘柄の全XBRLの申告の生データ | `fetcher.py:SECFetcher.fetch_company_facts` | 22・テスト15 | index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL） | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/quarterly_{Q}.json`<br>（3425ファイル） | 生データから1四半期分ずつ取り出した四半期の財務 | `parser.py:SECParser.save_parsed_data` | 6・テスト10 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-046・AS-IS-071・AS-IS-093・AS-IS-096 | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/submissions.json`<br>（98ファイル） | SECから取った、その銘柄の提出書類の一覧（提出日・対象期末） | `fetcher.py:SECFetcher.fetch_submissions` | 6・テスト7 | value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/data/{T}/submissions_legacy_{CIK}.json`<br>（4ファイル） | 社名・CIKが変わった銘柄の、旧CIKの提出書類の一覧 | `fetcher.py:SECFetcher._fetch_legacy_submissions` | 2 | — | — | — | 残す（2026-10-20。fetcher.py の _fetch_legacy_submissions が旧CIKの取得の24時間キャッシュと、取得に失敗したときの代わりとして読む） |
| 2件以上 | `common/sec_data/dimension_aggregate_registry.json`<br>（1ファイル） | セグメント等の内訳タグを合計して使う銘柄・項目の登録簿 | `dimension_aggregate_fetcher.py:_save_dimension_aggregate_registry` | 2（1） | — | — | — | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/normalized/{T}_quarterly_normalized.json`<br>（98ファイル） | 四半期の財務を項目ごとの時系列に並べ直し、Q4の逆算を足したもの | `normalizer.py:save_normalized` | 8・テスト12 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-071・AS-IS-093・AS-IS-096 | 残す（2026-10-20） |
| 2件以上 | `common/sec_data/ttm/{T}_ttm_series.json`<br>（98ファイル） | 直近4四半期の合計（TTM）の時系列 | `ttm_calculator.py:save_ttm_series` | 9・テスト10（2） | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-046 | 残す（2026-10-20） |
| 2件以上 | `config/cik_lookup.csv`<br>（1ファイル） | 登録銘柄の一覧（CIK・状態・各システムに載せるかの印）。新規登録の検証で状態を書き換える | `registration_validator.py:promote_ticker_status` | 28・テスト16 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・保有銘柄の監視（TANUKI TAIL） | AS-IS-264・AS-IS-273 | 残す（2026-10-20） |
| 2件以上 | `docs/common/sec_data/rotce/{T}.json`<br>（98ファイル） | 銘柄ごとのROTCE（有形自己資本利益率）の計算結果 | `rotce.py:build_all` | 3・テスト3 | value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/tanuki_score/index.html（経由） | — | — | 残す（2026-10-20） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `${GITHUB_STEP_SUMMARY}` | GitHub Actionsの実行の画面に出す要約（人が見る） | `audit.py:main`<br>`rollback_failed.py:main`<br>`update.py:_finalize` | .github/workflows/SEC_Data_Audit.yml<br>.github/workflows/SEC_Data_Update.yml |
| `Discord（DISCORD_WEB_HOOK）` | SECデータの監査の結果のDiscordへの通知 | `audit.py:post_discord` | .github/workflows/SEC_Data_Audit.yml<br>.github/workflows/SEC_Data_Update.yml<br>.github/workflows/System_Health.yml |

## 読み手の内訳

### `common/sec_data/data/_cik_cache.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 83・84・99（経由 `common/sec_data/fetcher.py::SECFetcher._load_cik_cache`）

### `common/sec_data/data/{T}/bs_identity_violations_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `ticker`(3)・`violations`(1)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2393・2397・3230・3434・3681（経由 `common/sec_data/report_consistency_check.py::_read_bs_identity_violations_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_bs_identity_violations_log`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_bs_identity_violations_log`）

### `common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `cik`(1)・`entityName`(0)・`facts`(1)
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 240・429（経由 `common/sec_data/fetcher.py::SECFetcher._read_json_or_none`）

### `common/sec_data/data/{T}/fy_collision_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `collisions`(1)・`ticker`(3)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2333・2337・3034・3434・3681（経由 `common/sec_data/report_consistency_check.py::_read_fy_collision_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_fy_collision_log`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_fy_collision_log`）

### `common/sec_data/data/{T}/fy_tag_mismatch_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `mismatches`(1)・`ticker`(3)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2353・2357・3053・3434・3681（経由 `common/sec_data/report_consistency_check.py::_read_fy_tag_mismatch_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_fy_tag_mismatch_log`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_fy_tag_mismatch_log`）

### `common/sec_data/data/{T}/fye_boundary_collision_log.json`（1件）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `collisions`(1)・`ticker`(3)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2373・2377・3071・3434・3681（経由 `common/sec_data/report_consistency_check.py::_read_fye_boundary_collision_log`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_fye_boundary_collision_log`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_fye_boundary_collision_log`）

### `docs/common/sec_data/rotce/_summary.json`（1件）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `generated_at`(1)・`reference_only`(0)・`thresholds`(0)・`tickers`(1)
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 241

### `${RUNNER_TEMP}/sec_update_result.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- Python・その場で計算: `common/sec_data/fetcher.py` 行 595（経由 `common/sec_data/fetcher.py::SECFetcher`）
- Python・その場で計算: `common/sec_data/freshness.py` 行 103・128（経由 `common/sec_data/freshness.py::utc_now_iso`）
- Python・保存済みファイル: `common/sec_data/rollback_failed.py` 行 30
- Python・保存済みファイル: `common/sec_data/update_notify.py` 行 38・82・95（経由 `common/sec_data/update_notify.py::load_result`）
- テスト・その場で計算: `tests/test_sec_freshness.py` 行 83（経由 `common/sec_data/fetcher.py::SECFetcher`）

### `common/sec_data/data/_freshness.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `counts`(3)・`generated_at`(4)・`tickers`(5)
- Python・保存済みファイル: `common/sec_data/freshness.py` 行 19・115
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1746・3603・3681（経由 `common/sec_data/freshness.py::load`）
- Python・保存済みファイル: `common/system_health.py` 行 876・956・1051・1110（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 385・396・417・422・428・434・440・443・450・453・472・487 ほか2（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 238・253・270・300・306・312・317（経由 `common/sec_data/freshness.py::load`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/sec_data/freshness.py::load`）

### `common/sec_data/data/{T}/annual_{YYYY}.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `bs`(15)・`bs_provenance`(7)・`bs_tag_sources`(1)・`cf`(12)・`cf_provenance`(4)・`data_quality`(3)・`form`(8)・`other`(2)・`other_provenance`(0)・`period`(13)・`pl`(23)・`pl_provenance`(4)・`shares`(3)・`shares_provenance`(0)・`ticker`(21)
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 66・79・80・166・203・226・254・364・367・368・369・370 ほか2（経由 `common/screening/dcf_validity_checker.py::_load_annual`、`common/screening/dcf_validity_checker.py::_load_json`）
- Python・保存済みファイル: `common/sec_data/parser.py` 行 505・811・3995・4001・4057（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- Python・保存済みファイル: `common/sec_data/reader.py` 行 133・134・159・184・208・222・234・243・274・296・322・338 ほか12（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 335・499（経由 `common/sec_data/stonks_flag_rule.py::_is_pre_revenue`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 419・436・613・615・687・768・770・1027・1117・1124・1285・1339 ほか23（経由 `common/screening/dcf_validity_checker.py::_load_annual`、`common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/report_consistency_check.py::_check_annual_da_partial_concept` ほか）
- Python・保存済みファイル: `common/sec_data/roic.py` 行 85
- Python・保存済みファイル: `common/sec_data/stonks_flag_rule.py` 行 69・112（経由 `common/sec_data/stonks_flag_rule.py::_is_pre_revenue`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/execution_metrics.py` 行 85・123（経由 `common/sec_data/roic.py::calc_roic_wacc_ratio`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 819（経由 `common/sec_data/roic.py::calc_roic_wacc_ratio`）
- Python・保存済みファイル: `src/value/stonks_silo/analyzer.py` 行 1297（経由 `src/value/stonks_silo/fetcher.py::load_annual_data`）
- Python・保存済みファイル: `src/value/stonks_silo/fetcher.py` 行 56・282（経由 `src/value/stonks_silo/fetcher.py::load_annual_data`）
- Python・保存済みファイル: `src/value/stonks_silo/financial_as_of.py` 行 52・54・87（経由 `src/value/stonks_silo/financial_as_of.py::scoring_fy_end`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 114・121・175・256（経由 `common/sec_data/reader.py::SECReader._load_json`、`src/value/stonks_silo/fetcher.py::load_annual_data`、`src/value/stonks_silo/financial_as_of.py::scoring_fy_end`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/adjustments.py` 行 1299
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 324・456・1023（経由 `common/sec_data/reader.py::SECReader._load_json`、`src/value/tanuki_valuation/calculator/adjustments.py::capitalize_rd`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 596・599・603・625・628・632・636・689・895（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 426・444・450・453・489・990・1003・1004・1046・1068・1114・2839 ほか7（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/roic.py::calc_roic_wacc_ratio`、`src/value/tanuki_valuation/calculator/adjustments.py::capitalize_rd` ほか）
- テスト・保存済みファイル: `tests/test_bs_field_cross_year_misselect.py` 行 304・314（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- テスト・保存済みファイル: `tests/test_check31_fields_snapshot_hash.py` 行 63・67・75・82・96・105（経由 `common/sec_data/report_consistency_check.py::_check_fixed_registry_integrity`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 40・89（経由 `common/sec_data/report_consistency_check.py::_check_operating_income_reconstruction_scope`、`common/sec_data/report_consistency_check.py::_check_revenue_net_income_reconciliation`）
- テスト・保存済みファイル: `tests/test_conditional_tags_and_stale_latest.py` 行 105（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23・27・28・41（経由 `src/value/tanuki_valuation/calculator/adjustments.py::capitalize_rd`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `src/value/tanuki_valuation/calculator/adjustments.py::capitalize_rd`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_dcf_validity_checker.py` 行 46・64・77・86・95・107・118・128・133・143・155・165（経由 `common/screening/dcf_validity_checker.py::_load_annual`、`common/screening/dcf_validity_checker.py::_load_json`）
- テスト・保存済みファイル: `tests/test_execution_metrics.py` 行 78・84・96・98・102・139（経由 `common/sec_data/roic.py::calc_roic_wacc_ratio`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 123・133（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 450・461・479・489・500・513・522・2005・2066・2079・2100・2119 ほか31（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`、`common/sec_data/reader.py::SECReader._load_json`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data` ほか）
- テスト・保存済みファイル: `tests/test_pl_field_cross_accn_alignment.py` 行 414・431（経由 `common/sec_data/parser.py::SECParser._apply_fixed_registry_freeze`）
- テスト・保存済みファイル: `tests/test_reader_fcf_list_with_dates.py` 行 22・37・44（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_operating_income_reconstruction_scope`）
- テスト・保存済みファイル: `tests/test_roic_shared_module.py` 行 40・50・56・65・76・88・97・107・118・129・148・149 ほか2（経由 `common/sec_data/roic.py::calc_roic_wacc_ratio`）
- テスト・保存済みファイル: `tests/test_runway_cash_unify.py` 行 149・158・169・170・172（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/report_consistency_check.py::_read_latest_annual_bs`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 235・250・252・265
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `src/value/tanuki_valuation/calculator/adjustments.py::capitalize_rd`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_stonks_flag_rule.py` 行 49・55・60・65・71・76・82・105・111・120・128（経由 `common/sec_data/stonks_flag_rule.py::_is_pre_revenue`）
- テスト・保存済みファイル: `tests/test_stonks_silo_financial_as_of.py` 行 68・72・73・75（経由 `src/value/stonks_silo/financial_as_of.py::scoring_fy_end`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/sec_data/data/{T}/company_facts.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `cik`(3)・`entityName`(2)・`facts`(14)
- Python・保存済みファイル: `common/sec_data/audit.py` 行 157・160・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 501・505
- Python・保存済みファイル: `common/sec_data/fye_change_candidate_scan.py` 行 100・151・168（経由 `common/sec_data/fetcher.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/layer3_builder.py` 行 207・211・1262（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/newfield_q4_cutoff_check.py` 行 133・178（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/parser.py` 行 482・489・4057・4067（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- Python・保存済みファイル: `common/sec_data/reader.py` 行 55（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 410・505・674・688・690（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1384・1390・1503・1506・1879・2001・2611・2613・3194・3197・3434・3488 ほか1（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/report_consistency_check.py::_check_annual_da_partial_concept`、`common/sec_data/report_consistency_check.py::_check_share_count_outliers` ほか）
- Python・保存済みファイル: `common/sec_data/revenue_tag_conflict_check.py` 行 91・176・188（経由 `common/sec_data/fetcher.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 415・421・674・706（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 53・118・173・183・191・230・241・323・425・428（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/layer3_builder.py::load_company_facts`、`common/sec_data/parser.py::SECParser.parse_company_facts`）
- Python・保存済みファイル: `common/system_health.py` 行 86・788・1038・1049・1110（経由 `common/sec_data/audit.py::audit_ticker`、`common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- Python・保存済みファイル: `src/tail/kpi_proposer.py` 行 190・551・721・779・820（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 172・1543（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 137・207（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/tail/xbrl_segment_fetcher.py` 行 411・541（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 261・515（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/value/stonks_silo/financial_trend_calculator.py` 行 388（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 147・150・212・256（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/segment_growth_xbrl.py` 行 218・262・313・372（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 401・1177・2966・3208・3440・3580・3616（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_bs_field_cross_year_misselect.py` 行 304・314（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::check_ticker`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 185・190・195（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_conditional_tags_and_stale_latest.py` 行 50・69・80・86・89・94・101・105・116・165（経由 `common/sec_data/layer3_builder.py::load_company_facts`、`common/sec_data/parser.py::SECParser.parse_company_facts`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2625・2631（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- テスト・保存済みファイル: `tests/test_pl_field_cross_accn_alignment.py` 行 414・431（経由 `common/sec_data/parser.py::SECParser.parse_company_facts`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::check_ticker`）
- テスト・保存済みファイル: `tests/test_rotce.py` 行 64・70・81・94・96・101・110・119・130・138・145・153 ほか20（経由 `common/sec_data/layer3_builder.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 120・148・160・173・185・198・212・224・240・252・264・321 ほか2（経由 `common/sec_data/fetcher.py::load_company_facts`、`common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 99・234・247・251・262・268・274・278（経由 `common/sec_data/fetcher.py::load_company_facts`）
- テスト・保存済みファイル: `tests/test_share_count_outlier_check.py` 行 40・46・53・62・69・74（経由 `common/sec_data/report_consistency_check.py::_check_share_count_outliers`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/portfolio/index.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/kpi_proposer.py → docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/_summary.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/sec_data/data/{T}/quarterly_{Q}.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `bs`(5)・`bs_tag_sources`(0)・`cf`(6)・`cf_provenance`(1)・`data_quality`(2)・`form`(3)・`other`(1)・`period`(8)・`pl`(8)・`shares`(2)・`ticker`(10)
- Python・保存済みファイル: `common/sec_data/reader.py` 行 134・159・183・184・208・222・234・243・274・296・322・338 ほか12（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_quarterly`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1124・2870・3434・3681（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/report_consistency_check.py::check_ticker`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 121（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 456・1023（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 596・599・603・625・628・632・636・689・895（経由 `common/sec_data/reader.py::SECReader._load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 426・453・1046・1114（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::check_ticker`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 123・133（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2005・2066・2079・2100・2119・2146・2162・2175・2998・3002・3019・3023（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_reader_fcf_list_with_dates.py` 行 22・37・44（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::check_ticker`）
- テスト・保存済みファイル: `tests/test_runway_cash_unify.py` 行 149・158（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `common/sec_data/reader.py::SECReader._load_json`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/sec_data/data/{T}/submissions.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `accn_to_filingdate`(2)・`accn_to_reportdate`(3)・`cik`(2)・`former_names`(1)・`legacy_ciks_merged`(1)・`ticker`(8)
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 521・525・543・547
- Python・保存済みファイル: `common/sec_data/parser.py` 行 498・503・4057・4067（経由 `common/sec_data/fetcher.py::load_former_names`、`common/sec_data/fetcher.py::load_submissions`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 469・622・775・2596・2602・3210・3434・3681（経由 `common/sec_data/fetcher.py::load_submissions`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 183（経由 `common/sec_data/fetcher.py::load_submissions`）
- Python・保存済みファイル: `src/value/stonks_silo/financial_as_of.py` 行 64・87（経由 `common/sec_data/fetcher.py::load_submissions`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 175（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_bs_field_cross_year_misselect.py` 行 304・314（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 40・89（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_conditional_tags_and_stale_latest.py` 行 105（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2631（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_pl_field_cross_accn_alignment.py` 行 414・431（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/fetcher.py::load_submissions`）
- テスト・保存済みファイル: `tests/test_stonks_silo_financial_as_of.py` 行 68・72・73・75（経由 `common/sec_data/fetcher.py::load_submissions`）
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `common/sec_data/data/{T}/submissions_legacy_{CIK}.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `accn_to_filingdate`(2)・`accn_to_reportdate`(1)・`cik`(1)・`ticker`(2)
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 181・233・240・480・598（経由 `common/sec_data/fetcher.py::SECFetcher._read_json_or_none`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 91（経由 `common/sec_data/fetcher.py::SECFetcher._read_json_or_none`）

### `common/sec_data/dimension_aggregate_registry.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `0000813672-26-000016|2025-01-01~2025-12-31`(0)・`0000896878-25-000035|2024-08-01~2025-07-31`(0)・`0001193125-09-239249|2008-09-30`(0)・`0001193125-21-060650|2019-12-31`(0)・`0001213900-20-008037|2019-12-31`(0)・`0001341766-26-000024|2025-12-31`(0)・`0001579091-24-000027|2023-12-31`(0)・`0001579091-25-000015|2024-12-31`(0)・`0001579091-26-000018|2025-12-31`(0)
- Python・その場で計算: `common/sec_data/parser.py` 行 3726・3939（経由 `common/sec_data/dimension_aggregate_fetcher.py::load_dimension_aggregate_registry`）
- Python・保存済みファイル: `common/sec_data/dimension_aggregate_fetcher.py` 行 73・92・402・434（経由 `common/sec_data/dimension_aggregate_fetcher.py::load_dimension_aggregate_registry`）
- Python・保存済みファイル: `common/sec_data/parser.py` 行 505・771・823・3726・3840・3939（経由 `common/sec_data/dimension_aggregate_fetcher.py::load_dimension_aggregate_registry`）

### `common/sec_data/normalized/{T}_quarterly_normalized.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `fields`(10)・`generated_at`(2)・`ticker`(13)
- Python・保存済みファイル: `common/sec_data/reader.py` 行 134・159・184・208・222・234・243・274・296・322・338・343 ほか16（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`、`common/sec_data/reader.py::SECReader.get_rpo_series`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 881・884・1124・2610・2625・3434・3681（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`、`common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- Python・保存済みファイル: `common/sec_data/rollback_failed.py` 行 45・57・95・112（経由 `common/sec_data/rollback_failed.py::ticker_paths`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 121・256（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 456・1023（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 596・599・603・625・628・632・636・689・895（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 426・453・489・1046・1114・3863（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`、`common/sec_data/reader.py::SECReader.get_rpo_context`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 336
- テスト・その場で計算: `tests/test_normalizer.py` 行 204（経由 `common/sec_data/normalizer.py::normalize`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_check47_contiguous_window.py` 行 49・55・59・63（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23・27・28・41（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 123・133（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2005・2066・2079・2100・2119・2146・2162・2175・3305（経由 `common/sec_data/reader.py::SECReader._load_json`、`common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- テスト・保存済みファイル: `tests/test_reader_fcf_list_with_dates.py` 行 22・37・44（経由 `common/sec_data/reader.py::SECReader._load_json`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_runway_cash_unify.py` 行 149・158・169・170・172（経由 `common/sec_data/reader.py::SECReader.get_lt_debt_from_normalized`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 236・248・260・263・277・282（経由 `common/sec_data/rollback_failed.py::ticker_paths`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `common/sec_data/reader.py::SECReader.get_rpo_context`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`

### `common/sec_data/ttm/{T}_ttm_series.json`（2件以上）
- 起動元: .github/workflows/SEC_Data_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `generated_at`(2)・`n_periods`(0)・`series`(10)・`ticker`(15)
- Python・その場で計算: `common/sec_data/reader.py` 行 56（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- Python・その場で計算: `src/value/stonks_silo/financial_as_of.py` 行 40（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 81・86・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 81・102・103・110・196・224・241・289・335・350・467・483 ほか7（経由 `common/sec_data/registration_validator.py::_load_json`、`common/sec_data/registration_validator.py::_ttm_end`、`common/sec_data/stonks_flag_rule.py::_latest_ttm`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 882・889・1820・2610・3434・3540・3681（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`、`common/sec_data/stonks_flag_rule.py::_latest_ttm`）
- Python・保存済みファイル: `common/sec_data/rollback_failed.py` 行 46・57・95・112（経由 `common/sec_data/rollback_failed.py::ticker_paths`）
- Python・保存済みファイル: `common/sec_data/stonks_flag_rule.py` 行 56・60・101（経由 `common/sec_data/stonks_flag_rule.py::_latest_ttm`）
- Python・保存済みファイル: `common/system_health.py` 行 86・1038（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 244
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 489・990・1068・1305・1309・3092・3107・3369・3374・3863（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._save_result`）
- ワークフロー・保存済みファイル: `.github/workflows/SEC_Data_Update.yml` 行 112
- テスト・その場で計算: `tests/test_ttm_anchor_reach.py` 行 47・60・67・77・90（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- テスト・その場で計算: `tests/test_ttm_calculator.py` 行 95・121（経由 `common/sec_data/ttm_calculator.py::calc_ttm_series`）
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_check47_contiguous_window.py` 行 49・55・59・63（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 461・500・522・2315・2332・2375・2400・2429・2452・2464・2477・2488 ほか24（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`、`tests/test_pipeline_logic.py::_write_dupont_ttm`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82・161（経由 `common/sec_data/report_consistency_check.py::_check_ttm_parser_layer3_reconciliation`）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 237・249・260・264・269・277・282（経由 `common/sec_data/rollback_failed.py::ticker_paths`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_stonks_flag_rule.py` 行 49・55・60・65・71・76・82・105・111・120・128（経由 `common/sec_data/stonks_flag_rule.py::_latest_ttm`）
- テスト・保存済みファイル: `tests/test_ttm_flow_key_names.py` 行 39
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `config/cik_lookup.csv`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- Python・保存済みファイル: `browser_checks/check_signal_consistency_section.py` 行 65（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `browser_checks/check_valuation_chart_basis.py` 行 184（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/registration/preflight_check.py` 行 158・207（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 99・115・353・366・435・446（経由 `common/registration/register_ticker.py::_load_cik_row`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 393（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/screening/report_txt_parser.py` 行 264（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/config.py` 行 17・22・43・86（経由 `common/sec_data/config.py::_load_from_csv`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 466（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/fye_change_candidate_scan.py` 行 134（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/newfield_q4_cutoff_check.py` 行 165（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 54・392・470・475・484・505・674・688・690（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`、`common/sec_data/registration_validator.py::run`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1816・3393・3394・3540・3631・3681（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/revenue_tag_conflict_check.py` 行 165（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 705（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/tickers.py` 行 13・19・25・38・58・72・102・110・115・120・125・168（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 422（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `common/system_health.py` 行 293・294・370・756・788・1045・1049・1110（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`、`common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/kpi_proposer.py` 行 717・820（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 407・459（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/sec_items_fetcher.py` 行 636・668（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 326（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/tail/text_kpi_extractor.py` 行 559・679（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/extract_key_facts.py` 行 50・155・427（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 391・394・645・961・978・983・989・1058・1059・1079（経由 `common/sec_data/tickers.py::_load`、`src/value/adjusted_eps_analyzer/pipeline.py::load_cik_data`、`src/value/adjusted_eps_analyzer/pipeline.py::run`）
- Python・保存済みファイル: `src/value/stonks_silo/pipeline.py` 行 46（経由 `common/sec_data/tickers.py::_load`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 926
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 411・413・426・527・536・537・566・3842・3863（経由 `common/sec_data/tickers.py::_load`、`src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher.get_financials`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_tickers_from_csv`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 112（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher.get_financials`）
- テスト・保存済みファイル: `tests/test_eps_get_cik_registry_only.py` 行 35・36・47（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_eps_run_status.py` 行 95・115・125・133・151・160・174（経由 `src/value/adjusted_eps_analyzer/pipeline.py::run`）
- テスト・保存済みファイル: `tests/test_flag_consumer_audit_3.py` 行 59・87（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_iv_formula.py` 行 42（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 3940・3945・3951・3956（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_preflight_check.py` 行 179・192・205・218・229（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_register_ticker_preflight.py` 行 61・83・99・117（経由 `common/registration/register_ticker.py::_load_cik_row`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82・148（経由 `common/registration/register_ticker.py::_load_cik_row`、`common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_score_verifier.py` 行 47・70（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_sec_data_config.py` 行 66・74・86・95・98（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 472（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_stonks_flag_rule.py` 行 105・111（経由 `common/sec_data/tickers.py::_load`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/tail/sec_items_fetcher.py → docs/portfolio/tail/data/{item_key}/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/kpi_proposer.py → docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/_summary.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/stonks_silo/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/common/sec_data/rotce/{T}.json`（2件以上）
- 起動元: .github/workflows/Market_Data_Daily_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `company_reported`(2)・`current`(3)・`generated_at`(1)・`history`(3)・`notes`(3)・`percentile`(3)・`reference_only`(1)・`tags`(1)・`ticker`(6)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1784・3615・3681（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 635・639
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 756
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）
- テスト・保存済みファイル: `tests/test_rotce.py` 行 345・350・365（経由 `common/sec_data/rotce.py::unregistered_latest_quarters`）
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/_summary.json））: `docs/value-monitor/tanuki_score/index.html`

## 誰も読まないキー

この段階の出力のJSON（全ファイル、入れ子8段まで）のキーのうち、テストを除くコード（.py・.js・.html。書き手のファイルを含む）に読みの形（`.key`・`['key']`〈代入でない〉・`.get('key')`）で出てこないもの。銘柄名・日付など小文字を含まないキーは除く。Pythonで`for k in d`・`d.items()`のように回して読むものは拾えないので、消す前にキーの名前でgrepする

- `accn_aligned`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `accn_aligned_anchor`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `acquisition`（`common/sec_data/data/{T}/company_facts.json`）
- `acre`（`common/sec_data/data/{T}/company_facts.json`）
- `aligned_to_sibling_accn`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `annual_fill_max_days`（`docs/common/sec_data/rotce/_summary.json`）
- `appeal`（`common/sec_data/data/{T}/company_facts.json`）
- `bs_tag_sources`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`）
- `business`（`common/sec_data/data/{T}/company_facts.json`）
- `business_combination`（`common/sec_data/data/{T}/company_facts.json`）
- `business_unit`（`common/sec_data/data/{T}/company_facts.json`）
- `businesscombinations`（`common/sec_data/data/{T}/company_facts.json`）
- `case_filed`（`common/sec_data/data/{T}/company_facts.json`）
- `charge`（`common/sec_data/data/{T}/company_facts.json`）
- `claim`（`common/sec_data/data/{T}/company_facts.json`）
- `claimant`（`common/sec_data/data/{T}/company_facts.json`）
- `classAction`（`common/sec_data/data/{T}/company_facts.json`）
- `class_action_lawsuit`（`common/sec_data/data/{T}/company_facts.json`）
- `companies`（`common/sec_data/data/{T}/company_facts.json`）
- `complaints`（`common/sec_data/data/{T}/company_facts.json`）
- `computed_year`（`common/sec_data/data/{T}/fy_tag_mismatch_log.json`）
- `cross_check`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `current_employees`（`common/sec_data/data/{T}/company_facts.json`）
- `current_litigation_m`（`common/sec_data/data/{T}/company_facts.json`）
- `defendant`（`common/sec_data/data/{T}/company_facts.json`）
- `derivative`（`common/sec_data/data/{T}/company_facts.json`）
- `derivativeInstrument`（`common/sec_data/data/{T}/company_facts.json`）
- `diff_base`（`common/sec_data/data/{T}/bs_identity_violations_log.json`）
- `diff_base_pct`（`common/sec_data/data/{T}/bs_identity_violations_log.json`）
- `diff_extended`（`common/sec_data/data/{T}/bs_identity_violations_log.json`）
- `dimension_aggregate_applied`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `ecd`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `employee`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `entity`（`common/sec_data/data/{T}/company_facts.json`）
- `eps_basic`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`、`common/sec_data/ttm/{T}_ttm_series.json`）
- `extra_components`（`common/sec_data/data/{T}/bs_identity_violations_log.json`）
- `facility`（`common/sec_data/data/{T}/company_facts.json`）
- `fcf_method`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`）
- `ffd`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `financing_cash_flow`（`common/sec_data/ttm/{T}_ttm_series.json`）
- `first_quarter`（`docs/common/sec_data/rotce/{T}.json`）
- `former_employee`（`common/sec_data/data/{T}/company_facts.json`）
- `frame`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `gap_amount`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`）
- `goodwill`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`、`docs/common/sec_data/rotce/_summary.json`）
- `gp_anchor_realigned`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `gp_anchor_realigned_tag`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `gp_estimate`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `gp_rejected_reason`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `individuals`（`common/sec_data/data/{T}/company_facts.json`）
- `instrument`（`common/sec_data/data/{T}/company_facts.json`）
- `insuranceCompany`（`common/sec_data/data/{T}/company_facts.json`）
- `insurance_company`（`common/sec_data/data/{T}/company_facts.json`）
- `insurance_reserves`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`）
- `interest_expense`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `interest_income`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `invest`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `investing_cash_flow`（`common/sec_data/ttm/{T}_ttm_series.json`）
- `investment`（`common/sec_data/data/{T}/company_facts.json`）
- `judicialCase`（`common/sec_data/data/{T}/company_facts.json`）
- `last_quarter`（`docs/common/sec_data/rotce/{T}.json`）
- `legacy_ciks_merged`（`common/sec_data/data/{T}/submissions.json`）
- `lender`（`common/sec_data/data/{T}/company_facts.json`）
- `min_quarters`（`docs/common/sec_data/rotce/_summary.json`）
- `mmbtu`（`common/sec_data/data/{T}/company_facts.json`）
- `n_periods`（`common/sec_data/ttm/{T}_ttm_series.json`）
- `nonop_items`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `nonop_net`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `numberOfCompany`（`common/sec_data/data/{T}/company_facts.json`）
- `numberOfSegment`（`common/sec_data/data/{T}/company_facts.json`）
- `operating_segment`（`common/sec_data/data/{T}/company_facts.json`）
- `operating_segments`（`common/sec_data/data/{T}/company_facts.json`）
- `other_nonop_income`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `other_provenance`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `other_side`（`common/sec_data/data/{T}/fye_boundary_collision_log.json`）
- `override_applied`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/fye_boundary_collision_log.json`）
- `override_reason`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `own_data_side`（`common/sec_data/data/{T}/fye_boundary_collision_log.json`）
- `patent`（`common/sec_data/data/{T}/company_facts.json`）
- `percentage`（`common/sec_data/data/{T}/company_facts.json`）
- `plaintiff`（`common/sec_data/data/{T}/company_facts.json`）
- `plantiff`（`common/sec_data/data/{T}/company_facts.json`）
- `position`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `pretax_raw`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `project`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `ptbv_pctl_max`（`docs/common/sec_data/rotce/_summary.json`）
- `ptbv_pctl_min`（`docs/common/sec_data/rotce/_summary.json`）
- `pure`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `reference_only`（`docs/common/sec_data/rotce/_summary.json`、`docs/common/sec_data/rotce/{T}.json`）
- `registration_note`（`config/cik_lookup.csv`）
- `reportableSegment`（`common/sec_data/data/{T}/company_facts.json`）
- `reportable_segment`（`common/sec_data/data/{T}/company_facts.json`）
- `reportable_segments`（`common/sec_data/data/{T}/company_facts.json`）
- `reportable_unit`（`common/sec_data/data/{T}/company_facts.json`）
- `reportingUnit`（`common/sec_data/data/{T}/company_facts.json`）
- `reporting_Unit`（`common/sec_data/data/{T}/company_facts.json`）
- `reporting_unit`（`common/sec_data/data/{T}/company_facts.json`、`common/sec_data/data/{T}/company_facts_legacy_{CIK}.json`）
- `restaurant`（`common/sec_data/data/{T}/company_facts.json`）
- `restricted_cash`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`）
- `restructuring_charges_deducted`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `retiree`（`common/sec_data/data/{T}/company_facts.json`）
- `revenue_tag_realigned`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `revenue_tag_realigned_to`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `rich`（`docs/common/sec_data/rotce/_summary.json`）
- `rotce_pctl_max`（`docs/common/sec_data/rotce/_summary.json`）
- `rotce_pctl_min`（`docs/common/sec_data/rotce/_summary.json`）
- `rotce_series`（`docs/common/sec_data/rotce/{T}.json`）
- `rotce_ttm_tce_avg`（`docs/common/sec_data/rotce/{T}.json`）
- `securities`（`common/sec_data/data/{T}/company_facts.json`）
- `security`（`common/sec_data/data/{T}/company_facts.json`）
- `segement`（`common/sec_data/data/{T}/company_facts.json`）
- `sga_captured`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`）
- `sga_total`（`common/sec_data/data/{T}/annual_{YYYY}.json`、`common/sec_data/data/{T}/quarterly_{Q}.json`）
- `shares_provenance`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `shares_source`（`docs/common/sec_data/rotce/{T}.json`）
- `source_is_own_data`（`common/sec_data/data/{T}/annual_{YYYY}.json`）
- `sqft`（`common/sec_data/data/{T}/company_facts.json`）
- `sqm`（`common/sec_data/data/{T}/company_facts.json`）
- `srt`（`common/sec_data/data/{T}/company_facts.json`）
- `store`（`common/sec_data/data/{T}/company_facts.json`）
- `stores`（`common/sec_data/data/{T}/company_facts.json`）
- `subsidiaries`（`common/sec_data/data/{T}/company_facts.json`）
- `survivor`（`common/sec_data/data/{T}/company_facts.json`）
- `tradingDay`（`common/sec_data/data/{T}/company_facts.json`）
- `trading_day`（`common/sec_data/data/{T}/company_facts.json`）
- `upscale`（`common/sec_data/data/{T}/company_facts.json`）
- `verification_command`（`common/sec_data/dimension_aggregate_registry.json`）
- `vote`（`common/sec_data/data/{T}/company_facts.json`）
- `votePerShare`（`common/sec_data/data/{T}/company_facts.json`）

## ワークフローが自分で書く出力（.pyの外）

段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。

- `.github/workflows/Market_Data_Daily_Update.yml`（行 120・121・122・123・125）: 
- `.github/workflows/SEC_Data_Update.yml`（行 104・105・106・107・113）: 
- `.github/workflows/Stonks_Silo_Update.yml`（行 75・76・77・78・87）: `overall_score`（**無い**）・`overall_verdict`（**無い**）・`tickers`（ある）
- `.github/workflows/TANUKI_VALUATION_Update.yml`（行 104・105・106・107・112）: `intrinsic_value_per_share`（**無い**）・`overall`（**無い**）・`validation`（**無い**）

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | SEC層か | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。SEC層のファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | SEC層を含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:720`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:863`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:891` | はい |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:219`<br>`src/tail/edgar_rss_monitor.py:419`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:602` | はい |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1585`<br>`common/sec_data/report_consistency_check.py:1727`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | はい |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:284` | はい |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:375`<br>`src/tail/xbrl_segment_fetcher.py:180`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:137` | はい |

## 本番外のコード（scripts/analysis・browser_checks）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

### `scripts/analysis`（1ファイル・478行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `market_pulse_redesign.py` | 478 | — | — | — | — | 残す（2026-10-20。Market Pulse の作り直しの検討中。src/market/market_pulse/stage_conclusions.py が同じ値を使う） |

### `browser_checks`（5ファイル・2,236行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `check_dependency_map.py` | 458 | — | — | — | — | 残す（2026-10-20） |
| `check_macro_pulse.py` | 1,273 | — | — | — | — | 残す（2026-10-20） |
| `check_signal_consistency_section.py` | 206 | — | — | — | — | 残す（2026-10-20） |
| `check_tanuki_score_undetermined.py` | 86 | — | — | — | — | 残す（2026-10-20） |
| `check_valuation_chart_basis.py` | 213 | — | — | — | — | 残す（2026-10-20） |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
