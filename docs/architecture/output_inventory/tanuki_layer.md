# TANUKI VALUATION・TANUKI Scoreの出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`tanuki_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`docs/value-monitor/tanuki_valuation/`・`docs/value-monitor/tanuki_score/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-20）。画面の区画はKoichiさんの判断で「今日の特選銘柄」だけ消し、ほかは残す。出力・キー・本番外のコードはチャット側の判断。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 11（ファイル 9・人が見るもの 2）／読み手0件 0／テストだけ 0／1件 4／2件以上 5／規則の二重 9（検出式 10種のうち。TANUKIのファイルを含むもの 4）

**画面の項目**: 画面が読むキー 379（この段階の書き手あり 328・ほかの書き手あり 40・画面の中で作る値 8・**書き手なし 3**）／書き手があるのに誰も読まないキー **50**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 2件＋ファイル名で当たった行 0件／解決できた読み込み 31件＋ファイル名で当たった行 67件（2.0%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

## 消したもの（段階2、2026-10-20）

- `src/value/tanuki_valuation の diag_iv_trace.py・growth_model_audit.py・remove_legacy_tanuki_score.py・pipeline_log.txt`: 削除（2026-10-20、本番外。参照は自分自身のほか0件、計407行と UTF-16 のログ）
- `docs/value-monitor/tanuki_score/daily_pick.json・history.json`: 削除（2026-10-20。2026-06-27 から更新が無く、書き手・読み手0件。画面は integrated-dashboard を読む）
- `誰も読まないキー14個（latest.json 等への書き込み）`: 書き込みをやめた（2026-10-20）: avg_rev_growth_lagged・assumed_realization_years・intrinsic_value_pt・is_provisional・moat_gross_margin_norm・moat_roic_norm・moat_fcf_margin_norm・forward_earnings_yield・shares_yr_now_label・shares_yr_3ago_label・upside_pct・validated_at・hype_phase_label・hype_substage_label。98銘柄の latest.json の iv・判定・RICE・TANUKI Score は前後で同じ
- `今日の特選銘柄（src/value/tanuki_score/daily_pick.py・docs/integrated-dashboard/daily_pick.json・history.json・tanuki_score/index.html の区画・TANUKI_Score_Update.yml の段）`: 削除（2026-10-20、Koichiさんの判断で機能をまるごと。history.json の読み手は daily_pick.py 自身だけだった。src/value/tanuki_score/ と docs/integrated-dashboard/ はフォルダごと無くなった。判定の実績表は score_history.json を読み、影響なし）

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>（1ファイル） | score_watcherが前回の判定を覚えておく控え（判定の変化の通知に使う） | `score_watcher.py:save_state` | 1 | — | — | — | 残す（2026-10-20） |
| 1件 | `docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>（99ファイル） | 銘柄ごとのHypeCoreのライフサイクル・ステージ・フェーズの推移（TANUKIの実行のたびに1行） | `pipeline.py:TanukiValuationPipeline._save_hypecore_history` | 1 | value-monitor/tanuki_valuation/stock.html（直接） | 効く: TANUKI VALUATIONの画面 | — | 残す（2026-10-20） |
| 1件 | `docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>（96ファイル） | 銘柄ごとの理論株価・上振れ余地・成長率などの推移（実行ごとに1行） | `pipeline.py:TanukiValuationPipeline._save_result` | 1 | value-monitor/tanuki_valuation/stock.html（直接） | 効く: TANUKI VALUATIONの画面 | — | 残す（2026-10-20） |
| 1件 | `docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>（28889ファイル） | 銘柄ごとの、実行の日時ごとの評価の全体のスナップショット | `pipeline.py:TanukiValuationPipeline._save_result` | 1 | index.html（経由）<br>portfolio/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: HypeCore（TANUKI のtiming・判定の入力） | AS-IS-047・AS-IS-116・AS-IS-258・AS-IS-297・AS-IS-298・AS-IS-299・AS-IS-438・AS-IS-439・AS-IS-506 | 残す（2026-10-20） |
| 2件以上 | `config/beta_config.json`<br>（1ファイル） | 銘柄ごとのβ（市場との連動の強さ）の上書きの設定。WACCの計算に使う | `beta_fetcher.py:save_config` | 9・テスト9 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | — | AS-IS-013 | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/tickers.json`<br>（1ファイル） | 一覧のページ用の、全銘柄の理論株価・判定の要約 | `pipeline.py:TanukiValuationPipeline._save_tickers_index` | 9・テスト4 | common/ticker-links.js（直接）<br>portfolio/index.html（直接）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: TANUKI VALUATIONの画面・TANUKI SCOREの画面 | — | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/latest.json`<br>（96ファイル） | 銘柄ごとの最新の評価の全体（理論株価・判定・DCF・RICE・各指標）。個別ページが読む | `pipeline.py:TanukiValuationPipeline._save_result`<br>`pipeline.py:TanukiValuationPipeline._save_result` | 27・テスト18 | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（直接）<br>portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/hypecore/detail.html（直接）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（直接）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-056・AS-IS-071 | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/report.txt`<br>（96ファイル） | 銘柄ごとの評価の計算の説明（人が読む文の形式のレポート） | `pipeline.py:TanukiValuationPipeline._save_report` | 3・テスト3 | value-monitor/tanuki_valuation/stock.html（直接） | 効く: TANUKI VALUATIONの画面 | AS-IS-045・AS-IS-056・AS-IS-057・AS-IS-058・AS-IS-060 | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`<br>（98ファイル） | 銘柄ごとのTANUKI SCORE（funda等）・判定時の株価と、その後30・60・90日の値動きの推移 | `pipeline.py:TanukiValuationPipeline._save_result`<br>`score_verifier.py:verify_ticker` | 2・テスト2 | value-monitor/tanuki_score/index.html（直接） | 効く: TANUKI SCOREの画面 | — | 残す（2026-10-20） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `${GITHUB_STEP_SUMMARY}` | GitHub Actionsの実行の画面に出す要約（人が見る） | `beta_fetcher.py:main`<br>`score_watcher.py:main` | .github/workflows/Beta_Config_Update.yml<br>.github/workflows/TANUKI_VALUATION_Update.yml |
| `Discord（DISCORD_WEB_HOOK）` | βの更新・判定の変化のDiscordへの通知 | `beta_fetcher.py:post_discord`<br>`score_watcher.py:post_discord` | .github/workflows/Beta_Config_Update.yml<br>.github/workflows/TANUKI_VALUATION_Update.yml |

## 読み手の内訳

### `docs/value-monitor/tanuki_valuation/data/.watcher_state.json`（1件）
- 起動元: .github/workflows/TANUKI_VALUATION_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `AAPL`(0)・`ABBV`(0)・`ADBE`(0)・`ADSK`(0)・`ALAB`(0)・`AMAT`(0)・`AMD`(0)・`AMZN`(0)・`APP`(0)・`ASTS`(0)・`AVAV`(0)・`BBAI`(0)・`BKNG`(0)・`BROS`(0)・`BSY`(0)・`CAKE`(0)・`CART`(0)・`CAT`(0)・`CDNS`(0)・`CEG`(0)・`CELH`(0)・`CIX`(0)・`COHR`(0)・`CPRT`(0)・`CRM`(0)・`CRWV`(0)・`CSGP`(0)・`DDOG`(0)・`DELL`(0)・`DOCN`(0)・`ELF`(0)・`ENTG`(0)・`ESTC`(0)・`FCX`(0)・`FICO`(0)・`FLYW`(0)・`FROG`(0)・`FRSH`(0)・`GEV`(0)・`GOOGL`(0) ほか58
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 38・59・271・314（経由 `src/value/tanuki_valuation/score_watcher.py::load_state`）

### `docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`（1件）
- 起動元: .github/workflows/TANUKI_VALUATION_Update.yml
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 531

### `docs/value-monitor/tanuki_valuation/data/{T}/history.json`（1件）
- 起動元: .github/workflows/TANUKI_VALUATION_Update.yml
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 761

### `docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`（1件）
- 起動元: .github/workflows/TANUKI_VALUATION_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `alpha`(0)・`alpha_was_capped`(0)・`approx_value`(0)・`beta`(0)・`bs_adjustment`(0)・`calculation_date`(1)・`calculation_steps`(0)・`components`(0)・`dcf_components`(0)・`dcf_type`(0)・`fcf_base`(0)・`fcf_estimation`(0)・`fcf_outlier`(0)・`fcf_source`(0)・`fcf_ttm_end`(0)・`fcf_ttm_periods`(0)・`formula`(0)・`future_values`(0)・`growth`(0)・`growth_options`(0)・`growth_scenarios`(0)・`implied_irr`(0)・`intrinsic_value_beta`(0)・`intrinsic_value_per_share`(1)・`intrinsic_value_pt`(0)・`intrinsic_value_rf`(0)・`intrinsic_value_rf_rate`(0)・`invalid`(0)・`invalid_reason`(0)・`kpi_data`(0)・`latest_period_missing`(0)・`m_total`(0)・`maturity_profile`(0)・`method_change`(1)・`phase1_growth`(0)・`rd_capitalization`(0)・`required_growth_rf_base`(0)・`required_growth_rf_live`(0)・`return_metrics`(0)・`rice`(0) ほか18
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 336・534・1076・1283（経由 `src/value/hypecore/hypecore.py::fetch_tanuki_iv`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/{T}_poc.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `config/beta_config.json`（2件以上）
- 起動元: .github/workflows/Beta_Config_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_comment`(0)・`_damodaran_ref_2025`(0)・`_update_note`(0)・`_updated_at`(1)・`overrides`(8)
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 100
- Python・保存済みファイル: `common/sec_data/audit.py` 行 33・310・390・468（経由 `common/sec_data/audit.py::audit_beta_drift`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 52・81・103・110・196・224・241・289・350・467・483・496 ほか5（経由 `common/sec_data/registration_validator.py::_load_json`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 311
- Python・保存済みファイル: `common/system_health.py` 行 205・296・1045・1110（経由 `common/system_health.py::check_h_config`）
- Python・保存済みファイル: `src/value/tanuki_valuation/beta_fetcher.py` 行 40・71・155・288・319（経由 `src/value/tanuki_valuation/beta_fetcher.py::load_config`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 290・1026（経由 `src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 483・489・503・558（経由 `src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 454・490・1021・1062・1130・1338・1870・1871・3828・3830・3886（経由 `src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_beta_sector`）
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 87・95・99・104・111（経由 `common/sec_data/audit.py::audit_beta_drift`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23（経由 `src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`）
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96・102・108（経由 `common/sec_data/audit.py::audit_beta_drift`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 211・233・253・266・285・289・294・299・304・309・637・657 ほか22（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 472（経由 `common/system_health.py::check_h_config`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/system_health.py::check_h_config`）
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

### `docs/value-monitor/tanuki_valuation/data/tickers.json`（2件以上）
- 起動元: .github/workflows/TANUKI_VALUATION_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `count`(3)・`tickers`(11)・`updated_at`(0)
- Python・保存済みファイル: `common/sec_data/audit.py` 行 56・59・359・468（経由 `common/sec_data/audit.py::get_registered_tickers`）
- Python・保存済みファイル: `common/system_health.py` 行 159・1032・1041・1110（経由 `common/sec_data/audit.py::get_registered_tickers`、`common/system_health.py::check_d_actions`）
- Python・保存済みファイル: `src/value/tanuki_valuation/beta_fetcher.py` 行 83・86・279・319（経由 `src/value/tanuki_valuation/beta_fetcher.py::get_registered_tickers`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 49・52・264・314（経由 `src/value/tanuki_valuation/score_watcher.py::get_tickers`）
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 28
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 271
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 417
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/index.html` 行 535
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 2720
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96・102・108（経由 `common/sec_data/audit.py::get_registered_tickers`）
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 215
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 472（経由 `common/sec_data/audit.py::get_registered_tickers`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/sec_data/audit.py::get_registered_tickers`）

### `docs/value-monitor/tanuki_valuation/data/{T}/latest.json`（2件以上）
- 起動元: .github/workflows/TANUKI_VALUATION_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `alpha`(4)・`alpha_was_capped`(2)・`breakeven_estimate`(2)・`breakeven_reason`(2)・`bs_adjustment`(5)・`calculation_date`(8)・`components`(24)・`computed_runway_months`(3)・`dcf_components`(7)・`dcf_type`(4)・`dilution_comment`(2)・`dilution_severity`(2)・`dupont`(5)・`erp`(2)・`fcf_base`(6)・`fcf_estimation`(4)・`fcf_history`(6)・`fcf_margin_bear_mult_applied`(1)・`fcf_margin_note`(1)・`fcf_outlier`(5)・`fcf_source`(0)・`fcf_ttm_end`(2)・`fcf_ttm_periods`(0)・`fcfe_valuation`(1)・`financial_health`(7)・`formula`(2)・`funda_score`(7)・`future_values`(4)・`growth`(12)・`growth_options`(4)・`growth_sanity`(6)・`growth_scenarios`(6)・`intrinsic_value_beta`(3)・`intrinsic_value_per_share`(15)・`intrinsic_value_rf`(3)・`intrinsic_value_rf_rate`(3)・`latest_period_missing`(5)・`matrix`(4)・`maturity_profile`(2)・`method_change`(4) ほか39
- Python・保存済みファイル: `browser_checks/check_signal_consistency_section.py` 行 66・175・206（経由 `browser_checks/check_signal_consistency_section.py::pick_sparse_ticker`）
- Python・保存済みファイル: `browser_checks/check_tanuki_score_undetermined.py` 行 39・86（経由 `browser_checks/check_tanuki_score_undetermined.py::main`）
- Python・保存済みファイル: `browser_checks/check_valuation_chart_basis.py` 行 106・107・184・197・213（経由 `browser_checks/check_valuation_chart_basis.py::check_ticker`、`browser_checks/check_valuation_chart_basis.py::main`）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 66・80・166・203・226・254・361・364・367・368・369・370 ほか2（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/screening/dcf_validity_checker.py::check_ticker`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 127・138・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 81・103・110・168・196・224・241・289・350・442・467・483 ほか8（経由 `common/sec_data/registration_validator.py::_latest_json`、`common/sec_data/registration_validator.py::_load_json`、`common/sec_data/registration_validator.py::check_p1_registration_completeness` ほか）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1661・1665・1902・2306・2310・2372・2375・2672・2869・2898・2909・3487 ほか4（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/sec_data/report_consistency_check.py::_check_dcf_validation_failures`、`common/sec_data/report_consistency_check.py::_check_latest_period_missing` ほか）
- Python・保存済みファイル: `common/system_health.py` 行 86・132・137・1038・1040・1110（経由 `common/sec_data/audit.py::audit_ticker`、`common/system_health.py::check_c_latest`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 1520・1547・1904・1906（経由 `src/market/market_pulse/market_calendar.py::earnings_events`、`src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/market/market_pulse/market_calendar.py` 行 111・114・200（経由 `src/market/market_pulse/market_calendar.py::earnings_events`）
- Python・保存済みファイル: `src/market/market_pulse/sector_rotation.py` 行 231・265（経由 `src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 146・148・150・221・223・1542・1543・1744・1756・1780（経由 `src/tail/quarterly_review_generator.py::load_layer1_financials`、`src/tail/quarterly_review_generator.py::load_tanuki_valuation`、`src/tail/tail_dcf_bridge.py::_load_latest_valuation`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 73・76・159・161・194・207・337・349（経由 `src/tail/tail_dcf_bridge.py::_load_latest_valuation`、`src/tail/tail_dcf_bridge.py::_load_layer1_financials`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 348・350・534・1076・1283（経由 `src/value/hypecore/hypecore.py::fetch_tanuki_iv`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 517・3871・3886（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._save_tickers_index`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 72・75・106・277・314（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 335
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 363
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 1004
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/index.html` 行 251
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 643
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/detail.html` 行 408・652
- JS・HTML・保存済みファイル: `docs/value-monitor/stonks-silo/index.html` 行 446・619
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 426
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/index.html` 行 550
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 751・2726
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_VALUATION_Update.yml` 行 108
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 127（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- テスト・保存済みファイル: `tests/test_dcf_validity_checker.py` 行 46・64・77・86・95・107・118・128・133・143・155・165（経由 `common/screening/dcf_validity_checker.py::_load_json`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_iv_formula.py` 行 47・50・57（経由 `tests/test_iv_formula.py::_load`）
- テスト・保存済みファイル: `tests/test_latest_period_guard.py` 行 107（経由 `common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_market_pulse_implc.py` 行 373
- テスト・保存済みファイル: `tests/test_market_pulse_sector_rotation.py` 行 162
- テスト・保存済みファイル: `tests/test_method_change.py` 行 78・88（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 95・106・113・122・130・138・151・160（経由 `tests/test_output_inventory_check.py::_tanuki_inventory`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2867・2877・2902・2909・3037・3041
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 472（経由 `common/system_health.py::check_c_latest`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 93・96
- テスト・保存済みファイル: `tests/test_stock_html_waterfall_rm.py` 行 62
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/system_health.py::check_c_latest`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_tanuki_valuation`）
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/index.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/hypecore/hypecore.py → docs/value-monitor/hypecore/data/tickers.json））: `docs/value-monitor/hypecore/index.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/report.txt`（2件以上）
- 起動元: .github/workflows/TANUKI_VALUATION_Update.yml
- Python・保存済みファイル: `common/screening/report_txt_parser.py` 行 245・248・265・275・280・305（経由 `common/screening/report_txt_parser.py::_all_tickers_with_report`、`common/screening/report_txt_parser.py::parse_ticker_report`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2364・2367・2668・3451・3487・3747（経由 `common/sec_data/report_consistency_check.py::_read_report`、`common/sec_data/report_consistency_check.py::run_checks`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 2844
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::run_checks`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::run_checks`）
- テスト・保存済みファイル: `tests/test_report_txt_parser.py` 行 25・35・36・37・43・49・50（経由 `common/screening/report_txt_parser.py::_all_tickers_with_report`、`tests/test_report_txt_parser.py::_make_report`）

### `docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`（2件以上）
- 起動元: .github/workflows/Score_Verifier.yml・.github/workflows/TANUKI_VALUATION_Update.yml
- Python・保存済みファイル: `common/system_health.py` 行 101・106・1039・1110（経由 `common/system_health.py::check_b_score_history`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 710
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 472（経由 `common/system_health.py::check_b_score_history`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/system_health.py::check_b_score_history`）

## 画面の項目

画面（`docs/value-monitor/tanuki_score/index.html`・`docs/value-monitor/tanuki_valuation/index.html`・`docs/value-monitor/tanuki_valuation/stock.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）

- `market_data`（`docs/value-monitor/tanuki_score/index.html` 行 809）: が
- `basis_mix`（`docs/value-monitor/tanuki_valuation/stock.html` 行 1093・1093・1096）: FCFは古い期のTTMで評価しているため、DCF信頼性をLOWにして判定をWAT
- `rice_na_reason`（`docs/value-monitor/tanuki_valuation/stock.html` 行 1419・1419・1426・1426・1433・1433）

### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `alpha_clipped`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `alpha_raw`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `applicable`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `approx_value`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `assumed_realization_years`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `avg_rev_growth_lagged`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `base_cagr`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `calculation_steps`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `config_growth`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `constraints`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `current_subgroup`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `deviation_from_current`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `dupont_bs_period`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `fallback`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: 名前が一般的で、grep で読み手を決めきれない
- `fcf_calc_method`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `fcf_list`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `fiscal_year_end`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `formula_verification`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`、`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: validator の検証の結果（validation.checks。overall の判定に入る）
- `g_fundamental`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: report_consistency_check.py が名前の組（_REC_G_CANDIDATE_KEYS）で読む（一覧の読みの形では拾えない）
- `implied_irr`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `input`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `intrinsic_value_pt`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `invalid_reason`（`docs/value-monitor/tanuki_valuation/data/{T}/history.json`、`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `is_provisional`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `kpi_data`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `labels`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`、`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: 名前が一般的で、grep で読み手を決めきれない
- `m_total`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `max_eps`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `max_eps_ttm_end`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `moat_fcf_margin_norm`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `moat_gross_margin_norm`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `moat_roic_norm`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `pt_shares_consistency`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`、`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: validator の検証の結果（validation.checks。overall の判定に入る）
- `realized_ratio`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `reclassify_recommended`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `recommended_subgroup`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `segment_quarter`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `segment_xbrl_status`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `software_system_reclassification`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `step1`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `step2`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `step3`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `step4`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `step5`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `target_irr_deviation`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `total_revenue`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）
- `upside_pct`（`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `v0_adjusted`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `v0_note`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`、`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `validated_at`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る

### `docs/value-monitor/tanuki_score/index.html`（94項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `all` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 422・707 | 残す（2026-10-20） |
| — | `asset_turnover` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 899・1169 | 残す（2026-10-20） |
| — | `available` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 482 | 残す（2026-10-20） |
| — | `base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 480 | 残す（2026-10-20） |
| — | `base_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 462 | 残す（2026-10-20） |
| — | `buyback_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 506 | 残す（2026-10-20） |
| — | `components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 463・464・483・484 | 残す（2026-10-20） |
| VIX指数 | `credit` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 398 | 残す（2026-10-20） |
| VIX指数 | `date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 399・400・782・783 | 残す（2026-10-20） |
| 銘柄 | `desc` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 600・622 | 残す（2026-10-20） |
| — | `detected` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 500 | 残す（2026-10-20） |
| — | `dilution_3yr_annual_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 496 | 残す（2026-10-20） |
| — | `dividend_yield` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 503 | 残す（2026-10-20） |
| DuPont信頼性LOW(${s.dupont?.reliability_rea | `dupont` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 494・842・846・846 | 残す（2026-10-20） |
| — | `excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 846 | 残す（2026-10-20） |
| △床 | `fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 338・339・1047・1052 | 残す（2026-10-20） |
| — | `fcf_base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 462 | 残す（2026-10-20） |
| — | `fcf_floor_applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 464 | 残す（2026-10-20） |
| — | `fcf_history` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 499 | 残す（2026-10-20） |
| — | `fcf_outlier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 500 | 残す（2026-10-20） |
| — | `fear_greed` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 396 | 残す（2026-10-20） |
| — | `fields` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 502 | 残す（2026-10-20） |
| 判定なし（比較できる四半期が | `filled_components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 997 | 残す（2026-10-20） |
| 判定なし（比較できる四半期が | `filled_from_annual` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 992・997・1016 | 残す（2026-10-20） |
| — | `financial_health` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 496・506・507 | 残す（2026-10-20） |
| — | `financial_leverage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 900 | 残す（2026-10-20） |
| — | `funda_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 474 | 残す（2026-10-20） |
| 更新: （common/sec_data/rotce.py） | `generated_at` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 1039 | 残す（2026-10-20） |
| — | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 495 | 残す（2026-10-20） |
| VIX指数 | `indicators` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 397 | 残す（2026-10-20） |
| 景気敏感() | `industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 498・1112・1124 | 残す（2026-10-20） |
| — | `invalid` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 733 | 残す（2026-10-20） |
| \n目安: ${sig ? sig.label : r.signal_reaso | `label` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 343・545・597・619 | 残す（2026-10-20） |
| — | `latest_period_missing` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 502・502 | 残す（2026-10-20） |
| 銘柄 | `length` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 393・457・502・559 | 残す（2026-10-20） |
| が | `market_data` | 書き手なし | 809 | 残す（2026-10-20） |
| — | `matrix` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 478・539 | 残す（2026-10-20） |
| — | `max` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 969・970・1327 | 残す（2026-10-20） |
| — | `max_eps_per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 487 | 残す（2026-10-20） |
| — | `max_eps_reliability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 488 | 残す（2026-10-20） |
| — | `min` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 969・970・1322・1327 | 残す（2026-10-20） |
| — | `monthly` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 457・457 | 残す（2026-10-20） |
| 判定なし（比較できる四半期が | `n_quarters` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 995 | 残す（2026-10-20） |
| — | `net_current_assets_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 505 | 残す（2026-10-20） |
| — | `net_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 507 | 残す（2026-10-20） |
| — | `net_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 898・1170 | 残す（2026-10-20） |
| — | `no_data` | 画面の中で作る値 | 1009 | 残す（2026-10-20） |
| FCFは年次の値で評価（判定は通常どおり） | `note` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 565 | 残す（2026-10-20） |
| — | `payout_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 504 | 残す（2026-10-20） |
| — | `peg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 485・551・552・552 | 残す（2026-10-20） |
| 低PER(x) | `per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 351・351・484・550 | 残す（2026-10-20） |
| — | `per_is_forward` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 491 | 残す（2026-10-20） |
| — | `price_at_judgment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 790・790 | 残す（2026-10-20） |
| — | `ps` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 486・555・556・557 | 残す（2026-10-20） |
| — | `ptbv` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 962・962・963・963 | 残す（2026-10-20） |
| 判定なし（比較できる四半期が | `ptbv_pctl` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 996・996 | 残す（2026-10-20） |
| \n直近の四半期: | `quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 998 | 残す（2026-10-20） |
| — | `qx` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 541・542・543 | 残す（2026-10-20） |
| — | `qy` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 541・542・543 | 残す（2026-10-20） |
| — | `rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 495 | 残す（2026-10-20） |
| — | `reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1008 | 残す（2026-10-20） |
| — | `reliability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 882・1131 | 残す（2026-10-20） |
| 一過性要因の可能性あり | `reliability_reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 884・1137 | 残す（2026-10-20） |
| — | `return_30d` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 737・737・737・791 | 残す（2026-10-20） |
| — | `return_60d` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 738・738・738・792 | 残す（2026-10-20） |
| — | `return_90d` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 739・740・740・793 | 残す（2026-10-20） |
| — | `rev_yoy` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 458 | 残す（2026-10-20） |
| — | `rice` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 480・480・482・497 | 残す（2026-10-20） |
| VIX指数 | `risk_off_score` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 398 | 残す（2026-10-20） |
| DuPont ROEと実績ROEの乖離が | `roe` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 841・891・891・893 | 残す（2026-10-20） |
| — | `roe_10yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 483 | 残す（2026-10-20） |
| DuPont ROEと実績ROEの乖離が | `roe_decomposed` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 876・877・881・891 | 残す（2026-10-20） |
| — | `roic_wacc_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 497 | 残す（2026-10-20） |
| — | `rotce` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 962・963・990・991 | 残す（2026-10-20） |
| 判定なし（比較できる四半期が | `rotce_pctl` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 996 | 残す（2026-10-20） |
| — | `rule40` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 347・347・347・347 | 残す（2026-10-20） |
| — | `rule40_yoy_netmargin` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 459 | 残す（2026-10-20） |
| — | `score` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json`<br>`docs/value-monitor/stonks-silo/data/results.json` | 396 | 残す（2026-10-20） |
| — | `sell_reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 477 | 残す（2026-10-20） |
| P/TBV（対数） | `signal` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 988・1013・1014・1015 | 残す（2026-10-20） |
| \n目安: ${sig ? sig.label : r.signal_reaso | `signal_reason` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 994・1015 | 残す（2026-10-20） |
| — | `stage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 354・460・1092・1092 | 残す（2026-10-20） |
| — | `stage_label` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 469 | 残す（2026-10-20） |
| — | `structural_deficit` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 463 | 残す（2026-10-20） |
| — | `substage_label` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 468 | 残す（2026-10-20） |
| FCFは年次の値で評価（判定は通常どおり） | `substituted_by_annual` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 565 | 残す（2026-10-20） |
| — | `tanuki_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 476・732・782・789 | 残す（2026-10-20） |
| 銘柄なし | `ticker` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 342・342・442・588 | 残す（2026-10-20） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 418・442・447・959 | 残す（2026-10-20） |
| — | `timing_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 475 | 残す（2026-10-20） |
| — | `upside_percent` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 461 | 残す（2026-10-20） |
| VIX指数 | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397・430・438 | 残す（2026-10-20） |
| — | `year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1046・1046 | 残す（2026-10-20） |
| — | `yoy_growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 465 | 残す（2026-10-20） |

### `docs/value-monitor/tanuki_valuation/index.html`（36項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 394・397・403・404 | 残す（2026-10-20） |
| — | `calculation_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 417・569 | 残す（2026-10-20） |
| — | `cf_conversion` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 416 | 残す（2026-10-20） |
| (値 or 0.0) | `components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 393・398・401・413 | 残す（2026-10-20） |
| — | `count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 419 | 残す（2026-10-20） |
| — | `current_price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 393 | 残す（2026-10-20） |
| — | `date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 498・498 | 残す（2026-10-20） |
| — | `fcf_base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 418 | 残す（2026-10-20） |
| — | `fcf_base_method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 418 | 残す（2026-10-20） |
| — | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 494・494 | 残す（2026-10-20） |
| — | `growth_options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 419 | 残す（2026-10-20） |
| — | `growth_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 397 | 残す（2026-10-20） |
| — | `growth_scenarios` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397・420 | 残す（2026-10-20） |
| — | `intrinsic_value_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 395 | 残す（2026-10-20） |
| — | `intrinsic_value_per_share` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 394・394・558 | 残す（2026-10-20） |
| — | `ivps_beta` | 画面の中で作る値 | 488・488 | 残す（2026-10-20） |
| — | `ma200` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 413 | 残す（2026-10-20） |
| — | `ma200_dev_md` | 画面の中で作る値 | 472・472・473・473 | 残す（2026-10-20） |
| — | `method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 418 | 残す（2026-10-20） |
| (値 or 0.0) | `moat_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 398・579 | 残す（2026-10-20） |
| (値 or 0.0) | `moat_score_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 401・578 | 残す（2026-10-20） |
| — | `price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 486 | 残す（2026-10-20） |
| — | `primary` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397・420 | 残す（2026-10-20） |
| — | `q` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 415・496・496・496 | 残す（2026-10-20） |
| — | `rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397 | 残す（2026-10-20） |
| — | `rice` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 403・403・404・415 | 残す（2026-10-20） |
| — | `rice_per` | 画面の中で作る値 | 476・476・476 | 残す（2026-10-20） |
| — | `rice_per_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 404 | 残す（2026-10-20） |
| — | `scenario_valuations` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 394・397 | 残す（2026-10-20） |
| — | `source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 420 | 残す（2026-10-20） |
| — | `ticker` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 423・444・445・466 | 残す（2026-10-20） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 538 | 残す（2026-10-20） |
| — | `title` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 585 | 残す（2026-10-20） |
| — | `type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 521 | 残す（2026-10-20） |
| — | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 402 | 残す（2026-10-20） |
| — | `wacc` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 402・495・495 | 残す（2026-10-20） |

### `docs/value-monitor/tanuki_valuation/stock.html`（249項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `CapEx` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 389・389・391・395 | 残す（2026-10-20） |
| — | `DA` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 403 | 残す（2026-10-20） |
| — | `OCF` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 388・391・395 | 残す（2026-10-20） |
| — | `Revenue` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 395・399・403 | 残す（2026-10-20） |
| — | `SBC` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 399 | 残す（2026-10-20） |
| — | `action` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 850 | 残す（2026-10-20） |
| — | `adj_net_income` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 836 | 残す（2026-10-20） |
| — | `ai_concerns` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 879 | 残す（2026-10-20） |
| — | `alpha` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 802・2384 | 残す（2026-10-20） |
| — | `alpha_uncapped` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 804 | 残す（2026-10-20） |
| — | `alpha_was_capped` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 803 | 残す（2026-10-20） |
| — | `amount` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1774 | 残す（2026-10-20） |
| — | `analyst_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1015 | 残す（2026-10-20） |
| — | `analyst_rec_key` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1016 | 残す（2026-10-20） |
| — | `analyst_target_high` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1014 | 残す（2026-10-20） |
| — | `analyst_target_low` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1013 | 残す（2026-10-20） |
| — | `analyst_target_mean` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1012 | 残す（2026-10-20） |
| — | `analyst_target_median` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1011 | 残す（2026-10-20） |
| — | `analyst_vs_iv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1017 | 残す（2026-10-20） |
| — | `applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 833・861・925・933 | 残す（2026-10-20） |
| 資産回転率（Asset Turnover） | `asset_turnover` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1652 | 残す（2026-10-20） |
| — | `available` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 870・2747 | 残す（2026-10-20） |
| — | `base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 574・871・1282・1282 | 残す（2026-10-20） |
| — | `base_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・817 | 残す（2026-10-20） |
| （Ke ・高成長年 ＝ 表の中央） | `base_wacc` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1157 | 残す（2026-10-20） |
| （Ke ・高成長年 ＝ 表の中央） | `base_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1157 | 残す（2026-10-20） |
| FCFは古い期のTTMで評価しているため、DCF信頼性をLOWにして判定をWAT | `basis_mix` | 書き手なし | 1093・1093・1096 | 残す（2026-10-20） |
| — | `bear` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 872・1283・1283 | 残す（2026-10-20） |
| Bear乗数 | `bear_multiplier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1688 | 残す（2026-10-20） |
| — | `beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 971 | 残す（2026-10-20） |
| — | `bs_adjustment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 858・2386 | 残す（2026-10-20） |
| — | `bull` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 873・1281・1281 | 残す（2026-10-20） |
| Bull乗数 | `bull_multiplier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1689 | 残す（2026-10-20） |
| — | `calculation_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 797 | 残す（2026-10-20） |
| — | `cash` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 862 | 残す（2026-10-20） |
| — | `cash_and_equivalents` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1593 | 残す（2026-10-20） |
| — | `category` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1773 | 残す（2026-10-20） |
| 参考: 実績CAGR等 | `clipped` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1390・1700 | 残す（2026-10-20） |
| 最新年度 | `components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 795・804・809・810 | 残す（2026-10-20） |
| — | `conversion_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 837・934 | 残す（2026-10-20） |
| — | `count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 826 | 残す（2026-10-20） |
| — | `current_price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 795・2514 | 残す（2026-10-20） |
| — | `cv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 821 | 残す（2026-10-20） |
| — | `damodaran_industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2237・2237 | 残す（2026-10-20） |
| — | `damodaran_year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2238・2238 | 残す（2026-10-20） |
| — | `date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 547・2512 | 残す（2026-10-20） |
| Koichi式 v6.2 | `dcf_components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・808・2381 | 残す（2026-10-20） |
| — | `dcf_type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 807 | 残す（2026-10-20） |
| — | `deficit_quality` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2759 | 残す（2026-10-20） |
| — | `delay_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1993 | 残す（2026-10-20） |
| — | `detected` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 847 | 残す（2026-10-20） |
| 最新年度 | `diluted_shares` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 892・999・2385 | 残す（2026-10-20） |
| — | `dilution_3yr_annual_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1573 | 残す（2026-10-20） |
| — | `dilution_comment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1576 | 残す（2026-10-20） |
| — | `dilution_severity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1575 | 残す（2026-10-20） |
| — | `divergence_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 838・926 | 残す（2026-10-20） |
| — | `divergence_warning` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 839 | 残す（2026-10-20） |
| — | `dupont` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1618 | 残す（2026-10-20） |
| — | `end` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 346・346・358・361 | 残す（2026-10-20） |
| — | `erp` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 659 | 残す（2026-10-20） |
| — | `error` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2733・2824 | 残す（2026-10-20） |
| — | `estimated_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・835 | 残す（2026-10-20） |
| — | `estimated_revenue` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1368 | 残す（2026-10-20） |
| — | `excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1623 | 残す（2026-10-20） |
| — | `expectation_score` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 590・593 | 残す（2026-10-20） |
| — | `expected_return_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1322 | 残す（2026-10-20） |
| — | `fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2136・2146・2160・2160 | 残す（2026-10-20） |
| — | `fcf_2yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 819・819 | 残す（2026-10-20） |
| — | `fcf_5yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 818・818 | 残す（2026-10-20） |
| — | `fcf_base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・815 | 残す（2026-10-20） |
| — | `fcf_base_method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 816 | 残す（2026-10-20） |
| — | `fcf_base_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 817・2773 | 残す（2026-10-20） |
| — | `fcf_estimation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・832・2771 | 残す（2026-10-20） |
| FCF CAGR 上限 | `fcf_growth_cap` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1687 | 残す（2026-10-20） |
| FCF CAGR 下限 | `fcf_growth_floor` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1686 | 残す（2026-10-20） |
| — | `fcf_history` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2134 | 残す（2026-10-20） |
| — | `fcf_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1993・2771 | 残す（2026-10-20） |
| — | `fcf_outlier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 829 | 残す（2026-10-20） |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `fcf_ttm_end` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2191 | 残す（2026-10-20） |
| — | `fcf_value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 855 | 残す（2026-10-20） |
| — | `fields` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 340・1086 | 残す（2026-10-20） |
| — | `financial_health` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1501・1569 | 残す（2026-10-20） |
| 財務レバレッジ（Financial Leverage） | `financial_leverage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1653 | 残す（2026-10-20） |
| — | `fiscal_year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 854・866 | 残す（2026-10-20） |
| Koichi式 v6.2 | `formula` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 798・1068・1068 | 残す（2026-10-20） |
| — | `found` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 851 | 残す（2026-10-20） |
| — | `from_peak` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 557 | 残す（2026-10-20） |
| 5年後 | `future_value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1325・1338 | 残す（2026-10-20） |
| — | `future_values` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 988 | 残す（2026-10-20） |
| 年 @ | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1370・1388・1514・1514 | 残す（2026-10-20） |
| 上限 | `growth_cap` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1703・1703 | 残す（2026-10-20） |
| 上限 | `growth_floor` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1703 | 残す（2026-10-20） |
| — | `growth_options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 824・2383 | 残す（2026-10-20） |
| — | `growth_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1281・1282・1283・1418 | 残す（2026-10-20） |
| — | `growth_sanity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 990 | 残す（2026-10-20） |
| — | `growth_scenarios` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 980 | 残す（2026-10-20） |
| — | `growth_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 977 | 残す（2026-10-20） |
| — | `high_growth_rate_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 887 | 残す（2026-10-20） |
| — | `high_growth_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 889 | 残す（2026-10-20） |
| — | `industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 894 | 残す（2026-10-20） |
| — | `insider_buy_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1027 | 残す（2026-10-20） |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `insider_latest_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1030・2191 | 残す（2026-10-20） |
| — | `insider_net_direction` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1029 | 残す（2026-10-20） |
| — | `insider_sell_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1028 | 残す（2026-10-20） |
| — | `intrinsic_value_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 791 | 残す（2026-10-20） |
| — | `intrinsic_value_per_share` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 788・1281・1282・1283 | 残す（2026-10-20） |
| — | `intrinsic_value_rf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 792 | 残す（2026-10-20） |
| — | `intrinsic_value_rf_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2262・2264 | 残す（2026-10-20） |
| — | `invalid` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 2509 | 残す（2026-10-20） |
| — | `is_annual` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 345 | 残す（2026-10-20） |
| — | `item_name` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1773 | 残す（2026-10-20） |
| — | `items` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 853 | 残す（2026-10-20） |
| ERP（株式リスクプレミアム、現在値） | `label` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 385・686・1078・1310 | 残す（2026-10-20） |
| — | `latest_period_missing` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1086・1086・1088・1089 | 残す（2026-10-20） |
| — | `latest_revenue` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 997・1362・2774 | 残す（2026-10-20） |
| 直近件（新しい順） | `length` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 356・535・562・586 | 残す（2026-10-20） |
| — | `lifecycle` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 548 | 残す（2026-10-20） |
| — | `long_term_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 864 | 残す（2026-10-20） |
| — | `ma200` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 2515 | 残す（2026-10-20） |
| — | `ma200_dev` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 556 | 残す（2026-10-20） |
| — | `ma_addback_detected_but_not_applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 846 | 残す（2026-10-20） |
| — | `ma_addback_excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 845 | 残す（2026-10-20） |
| Rm基準（βなし %） | `market_return` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 973・1131・1503・1959 | 残す（2026-10-20） |
| — | `matrix` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 978 | 残す（2026-10-20） |
| — | `maturity_profile` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 885・889 | 残す（2026-10-20） |
| — | `max` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1453・1454・2139・2149 | 残す（2026-10-20） |
| — | `method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 816・1792 | 残す（2026-10-20） |
| — | `min` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1453・1454・1454・1585 | 残す（2026-10-20） |
| Phase1: 年（Moat Score由来${d.components?.mo | `moat_phase1_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1948 | 残す（2026-10-20） |
| Phase1: 年（Moat Score由来${d.components?.mo | `moat_score_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1948 | 残す（2026-10-20） |
| — | `model` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 881 | 残す（2026-10-20） |
| 表示期間: history記録開始以降（' + pivPoints[0].mon | `month` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 672・702・702・709 | 残す（2026-10-20） |
| — | `monthly` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 585・666 | 残す（2026-10-20） |
| — | `name` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1374・1989 | 残す（2026-10-20） |
| — | `net_cash` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 859 | 残す（2026-10-20） |
| — | `net_cash_per_share` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 860・2386 | 残す（2026-10-20） |
| — | `net_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1501・1571 | 残す（2026-10-20） |
| 純利益率（Net Margin） | `net_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1651 | 残す（2026-10-20） |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `next_earnings_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1068・1068・1574・2191 | 残す（2026-10-20） |
| 減収（構造的変化・継続）: | `ni_ocf_divergence` | 画面の中で作る値 | 1118 | 残す（2026-10-20） |
| 。直近の4四半期が組み替えの前後のベースで混ざるため、DCF信頼性をLOWにして | `note` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 840・849・874・1088 | 残す（2026-10-20） |
| 営業利益率 | `op_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1933 | 残す（2026-10-20） |
| 未実現の成長機会を「仮説セグメント」として定量化。TAM × 侵入率で期待売上、 | `options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1986 | 残す（2026-10-20） |
| — | `overall` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 880 | 残す（2026-10-20） |
| D&A比率 | `paper_bgcolor` | 画面の中で作る値 | 432・461・488 | 残す（2026-10-20） |
| — | `penetration` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1993 | 残す（2026-10-20） |
| — | `per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 993 | 残す（2026-10-20） |
| — | `per_adjusted` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 994 | 残す（2026-10-20） |
| — | `phase` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 550 | 残す（2026-10-20） |
| 年 @ | `phase1` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 889・1884・1884 | 残す（2026-10-20） |
| — | `phase1_growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2236 | 残す（2026-10-20） |
| — | `phase1_growth_auto_adjusted` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2210 | 残す（2026-10-20） |
| 年 @ | `phase2` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1890・1890 | 残す（2026-10-20） |
| D&A比率 | `plot_bgcolor` | 画面の中で作る値 | 433・462・489 | 残す（2026-10-20） |
| — | `price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 552 | 残す（2026-10-20） |
| price/IV 推移（表示期間中） | `price_iv_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 553・672・675・675 | 残す（2026-10-20） |
| — | `primary` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 981・982 | 残す（2026-10-20） |
| — | `probability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1993 | 残す（2026-10-20） |
| — | `pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1990 | 残す（2026-10-20） |
| — | `pv_fcf_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2426 | 残す（2026-10-20） |
| — | `pv_high` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 809 | 残す（2026-10-20） |
| — | `pv_phase1` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 811 | 残す（2026-10-20） |
| — | `pv_phase1_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2417 | 残す（2026-10-20） |
| — | `pv_phase2` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 812 | 残す（2026-10-20） |
| — | `pv_phase2_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2417 | 残す（2026-10-20） |
| — | `pv_terminal` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 810 | 残す（2026-10-20） |
| — | `pv_tv_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2417・2426 | 残す（2026-10-20） |
| — | `rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 982・1306・1311・1514 | 残す（2026-10-20） |
| — | `rate_is_sector_default` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 933 | 残す（2026-10-20） |
| — | `ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 820・821 | 残す（2026-10-20） |
| — | `raw_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 834 | 残す（2026-10-20） |
| 売上(推定=最新Revenue×weight) \| 構成比 \| 成長率シナリオ（ | `raw_weighted_growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1386・1387・1702・1703 | 残す（2026-10-20） |
| 算出不能（reason: ${dp.reason \|\| | `reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1632・1632 | 残す（2026-10-20） |
| — | `recommendation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 551 | 残す（2026-10-20） |
| ターミナル成長率 | `recommended_g` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1389・1697・1698・2211 | 残す（2026-10-20） |
| — | `recommended_g_median` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2212 | 残す（2026-10-20） |
| — | `required_growth_rf_base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2295 | 残す（2026-10-20） |
| — | `required_growth_rf_live` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2296 | 残す（2026-10-20） |
| — | `return_metrics` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 989 | 残す（2026-10-20） |
| — | `rev_yoy` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 554 | 残す（2026-10-20） |
| 減収（構造的変化・継続）: | `revenue_decline` | 画面の中で作る値 | 1118 | 残す（2026-10-20） |
| — | `revenue_growth_pct` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2759 | 残す（2026-10-20） |
| — | `revenue_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 996 | 残す（2026-10-20） |
| 最新年度 | `revenue_ttm_end` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 998 | 残す（2026-10-20） |
| RICE_adj: R&D資本化想定（CapExのみ投資強度）参考値 | `rice` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 574・574・869・1419 | 残す（2026-10-20） |
| — | `rice_adj` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1420・1420・1420・1427 | 残す（2026-10-20） |
| — | `rice_na_reason` | 書き手なし | 1419・1419・1426・1426 | 残す（2026-10-20） |
| — | `rice_per_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1421・1421・1428・1428 | 残す（2026-10-20） |
| — | `risk_free_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 972・2262・2299 | 残す（2026-10-20） |
| 固定値 | `risk_free_rate_live` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 691・2297 | 残す（2026-10-20） |
| 固定値 | `risk_free_rate_live_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 689・690・2265・2266 | 残す（2026-10-20） |
| — | `risk_free_rate_live_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 688・689・2265 | 残す（2026-10-20） |
| セクター除外 | `roe_10yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2751 | 残す（2026-10-20） |
| ROE（分解値） | `roe_decomposed` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1641・1641・1641・1654 | 残す（2026-10-20） |
| — | `roe_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 891 | 残す（2026-10-20） |
| — | `rpo` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 966 | 残す（2026-10-20） |
| 非連続RPO | `rpo_adjustment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1932・1933 | 残す（2026-10-20） |
| 非連続RPO | `rpo_incremental` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1932 | 残す（2026-10-20） |
| — | `rpo_pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 967・2382 | 残す（2026-10-20） |
| — | `rule` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 848 | 残す（2026-10-20） |
| — | `rule40` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 555 | 残す（2026-10-20） |
| Q異常値 | `runway` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2756 | 残す（2026-10-20） |
| Q異常値 | `runway_months` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2756 | 残す（2026-10-20） |
| — | `sbc_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1572 | 残す（2026-10-20） |
| — | `scenario_bear` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 2516 | 残す（2026-10-20） |
| — | `scenario_bull` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 2517 | 残す（2026-10-20） |
| — | `scenario_valuations` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 987・1497 | 残す（2026-10-20） |
| — | `score_comment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1078・1078 | 残す（2026-10-20） |
| — | `sector` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 893 | 残す（2026-10-20） |
| — | `segment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 985 | 残す（2026-10-20） |
| DCF採用値（クリップ後）: % | `segment_growth_quarter` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1394・1394 | 残す（2026-10-20） |
| 売上(推定=最新Revenue×weight) \| 構成比 \| 成長率シナリオ（ | `segment_growth_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1385 | 残す（2026-10-20） |
| — | `segments` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1360 | 残す（2026-10-20） |
| — | `sensitivity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 975 | 残す（2026-10-20） |
| →年 | `shares_yr_3ago` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1605 | 残す（2026-10-20） |
| →年 | `shares_yr_now` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1605 | 残す（2026-10-20） |
| — | `short_term_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 865 | 残す（2026-10-20） |
| — | `short_term_investments` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 863 | 残す（2026-10-20） |
| — | `signals` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2205 | 残す（2026-10-20） |
| — | `source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 981 | 残す（2026-10-20） |
| — | `stage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 549・588・711・715 | 残す（2026-10-20） |
| — | `stage_label` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 589・711・722 | 残す（2026-10-20） |
| — | `substituted_by_annual` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1089 | 残す（2026-10-20） |
| — | `tam` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1993 | 残す（2026-10-20） |
| 判定不能（株価なし） | `tanuki_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1050・1050・1078 | 残す（2026-10-20） |
| — | `terminal_growth_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 890 | 残す（2026-10-20） |
| — | `ticker` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2623・2637・2647・2665 | 残す（2026-10-20） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 2723・2732 | 残す（2026-10-20） |
| (' + count + '銘柄) | `title` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 2818 | 残す（2026-10-20） |
| — | `total_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1593 | 残す（2026-10-20） |
| — | `total_pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 825・2383 | 残す（2026-10-20） |
| — | `total_transient_amount` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 852 | 残す（2026-10-20） |
| — | `transient_evidence` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 851・852・853 | 残す（2026-10-20） |
| — | `type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2637・2671 | 残す（2026-10-20） |
| — | `upside_percent` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 575・796・2740 | 残す（2026-10-20） |
| — | `upside_percent_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 794 | 残す（2026-10-20） |
| — | `upside_percent_rf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 793 | 残す（2026-10-20） |
| Koichi式 v6.2 | `v0` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・1866・1905・2196 | 残す（2026-10-20） |
| Koichi式 v6.2 | `v0_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・2381 | 残す（2026-10-20） |
| — | `val` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 362 | 残す（2026-10-20） |
| — | `validation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 878 | 残す（2026-10-20） |
| 、固定値 | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 970・2267・2738 | 残す（2026-10-20） |
| — | `verdict` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2204・2234 | 残す（2026-10-20） |
| Rm基準（βなし %） | `wacc` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 969・1131・1503・1959 | 残す（2026-10-20） |
| — | `wacc_values` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 976 | 残す（2026-10-20） |
| — | `warnings` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 875・2207 | 残す（2026-10-20） |
| セグメント別売上構成（推定） | `weight` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1367・1388 | 残す（2026-10-20） |
| — | `year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2154・2164・2164 | 残す（2026-10-20） |
| 年 @ | `years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 889・1884・1890 | 残す（2026-10-20） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/value-monitor/tanuki_score/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `all`（ほかの書き手あり）<br>`asset_turnover`（この段階の書き手あり）<br>`available`（この段階の書き手あり）<br>`base`（この段階の書き手あり）<br>`base_fcf`（この段階の書き手あり）<br>`buyback_ttm`（この段階の書き手あり）<br>`components`（この段階の書き手あり）<br>`credit`（ほかの書き手あり）<br>`date`（この段階の書き手あり）<br>`desc`（ほかの書き手あり）<br>`detected`（この段階の書き手あり）<br>`dilution_3yr_annual_pct`（この段階の書き手あり）<br>`dividend_yield`（この段階の書き手あり）<br>`dupont`（この段階の書き手あり）<br>`excluded`（この段階の書き手あり）<br>`fcf`（この段階の書き手あり）<br>`fcf_base`（この段階の書き手あり）<br>`fcf_floor_applied`（この段階の書き手あり）<br>`fcf_history`（この段階の書き手あり）<br>`fcf_outlier`（この段階の書き手あり）<br>`fear_greed`（ほかの書き手あり）<br>`fields`（この段階の書き手あり）<br>`filled_components`（ほかの書き手あり）<br>`filled_from_annual`（ほかの書き手あり）<br>`financial_health`（この段階の書き手あり）<br>`financial_leverage`（この段階の書き手あり）<br>`funda_score`（この段階の書き手あり）<br>`generated_at`（ほかの書き手あり）<br>`growth`（この段階の書き手あり）<br>`indicators`（ほかの書き手あり）<br>`industry`（この段階の書き手あり）<br>`label`（この段階の書き手あり）<br>`latest_period_missing`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`matrix`（この段階の書き手あり）<br>`max`（この段階の書き手あり）<br>`max_eps_per`（この段階の書き手あり）<br>`max_eps_reliability`（この段階の書き手あり）<br>`min`（この段階の書き手あり）<br>`monthly`（ほかの書き手あり）<br>`n_quarters`（ほかの書き手あり）<br>`net_current_assets_ratio`（この段階の書き手あり）<br>`net_debt`（この段階の書き手あり）<br>`net_margin`（この段階の書き手あり）<br>`no_data`（画面の中で作る値）<br>`note`（この段階の書き手あり）<br>`payout_ratio`（この段階の書き手あり）<br>`peg`（この段階の書き手あり）<br>`per`（この段階の書き手あり）<br>`per_is_forward`（この段階の書き手あり）<br>`ps`（この段階の書き手あり）<br>`ptbv`（ほかの書き手あり）<br>`ptbv_pctl`（ほかの書き手あり）<br>`quarter_end`（ほかの書き手あり）<br>`qx`（この段階の書き手あり）<br>`qy`（この段階の書き手あり）<br>`rate`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`reliability`（この段階の書き手あり）<br>`reliability_reason`（この段階の書き手あり）<br>`rev_yoy`（この段階の書き手あり）<br>`rice`（この段階の書き手あり）<br>`risk_off_score`（ほかの書き手あり）<br>`roe`（この段階の書き手あり）<br>`roe_10yr_avg`（この段階の書き手あり）<br>`roe_decomposed`（この段階の書き手あり）<br>`roic_wacc_ratio`（この段階の書き手あり）<br>`rotce`（ほかの書き手あり）<br>`rotce_pctl`（ほかの書き手あり）<br>`rule40`（この段階の書き手あり）<br>`rule40_yoy_netmargin`（ほかの書き手あり）<br>`score`（ほかの書き手あり）<br>`sell_reason`（この段階の書き手あり）<br>`signal`（ほかの書き手あり）<br>`signal_reason`（ほかの書き手あり）<br>`stage`（この段階の書き手あり）<br>`stage_label`（ほかの書き手あり）<br>`structural_deficit`（この段階の書き手あり）<br>`substage_label`（ほかの書き手あり）<br>`substituted_by_annual`（この段階の書き手あり）<br>`tanuki_score`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり）<br>`timing_score`（この段階の書き手あり）<br>`upside_percent`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`year`（この段階の書き手あり）<br>`yoy_growth`（この段階の書き手あり） |  |
| ④ 判定実績バックテスト | `date`（この段階の書き手あり）<br>`invalid`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`market_data`（書き手なし）<br>`price_at_judgment`（この段階の書き手あり）<br>`return_30d`（この段階の書き手あり）<br>`return_60d`（この段階の書き手あり）<br>`return_90d`（この段階の書き手あり）<br>`tanuki_score`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり） |  |
| （JSで作る要素 #cat-board-body） | `all`（ほかの書き手あり） |  |
| （区画を決められない） | `desc`（ほかの書き手あり）<br>`fcf`（この段階の書き手あり）<br>`filled_from_annual`（ほかの書き手あり）<br>`label`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`per`（この段階の書き手あり）<br>`signal`（ほかの書き手あり）<br>`signal_reason`（ほかの書き手あり）<br>`ticker`（この段階の書き手あり） |  |

### `docs/value-monitor/tanuki_valuation/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `base`（この段階の書き手あり）<br>`calculation_date`（この段階の書き手あり）<br>`cf_conversion`（この段階の書き手あり）<br>`components`（この段階の書き手あり）<br>`count`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`fcf_base`（この段階の書き手あり）<br>`fcf_base_method`（この段階の書き手あり）<br>`growth`（この段階の書き手あり）<br>`growth_options`（この段階の書き手あり）<br>`growth_rate`（この段階の書き手あり）<br>`growth_scenarios`（この段階の書き手あり）<br>`intrinsic_value_beta`（この段階の書き手あり）<br>`intrinsic_value_per_share`（この段階の書き手あり）<br>`ivps_beta`（画面の中で作る値）<br>`ma200`（この段階の書き手あり）<br>`ma200_dev_md`（画面の中で作る値）<br>`method`（この段階の書き手あり）<br>`moat_score`（この段階の書き手あり）<br>`moat_score_source`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`primary`（この段階の書き手あり）<br>`q`（この段階の書き手あり）<br>`rate`（この段階の書き手あり）<br>`rice`（この段階の書き手あり）<br>`rice_per`（画面の中で作る値）<br>`rice_per_ratio`（この段階の書き手あり）<br>`scenario_valuations`（この段階の書き手あり）<br>`source`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり）<br>`title`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`wacc`（この段階の書き手あり） |  |

### `docs/value-monitor/tanuki_valuation/stock.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `action`（この段階の書き手あり）<br>`adj_net_income`（この段階の書き手あり）<br>`ai_concerns`（この段階の書き手あり）<br>`alpha`（この段階の書き手あり）<br>`alpha_uncapped`（この段階の書き手あり）<br>`alpha_was_capped`（この段階の書き手あり）<br>`analyst_count`（この段階の書き手あり）<br>`analyst_rec_key`（この段階の書き手あり）<br>`analyst_target_high`（この段階の書き手あり）<br>`analyst_target_low`（この段階の書き手あり）<br>`analyst_target_mean`（この段階の書き手あり）<br>`analyst_target_median`（この段階の書き手あり）<br>`analyst_vs_iv`（この段階の書き手あり）<br>`applied`（この段階の書き手あり）<br>`available`（この段階の書き手あり）<br>`base`（この段階の書き手あり）<br>`base_fcf`（この段階の書き手あり）<br>`base_wacc`（この段階の書き手あり）<br>`base_years`（この段階の書き手あり）<br>`basis_mix`（書き手なし）<br>`bear`（この段階の書き手あり）<br>`beta`（この段階の書き手あり）<br>`bs_adjustment`（この段階の書き手あり）<br>`bull`（この段階の書き手あり）<br>`calculation_date`（この段階の書き手あり）<br>`cash`（この段階の書き手あり）<br>`cash_and_equivalents`（この段階の書き手あり）<br>`clipped`（この段階の書き手あり）<br>`components`（この段階の書き手あり）<br>`conversion_rate`（この段階の書き手あり）<br>`count`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`cv`（この段階の書き手あり）<br>`dcf_components`（この段階の書き手あり）<br>`dcf_type`（この段階の書き手あり）<br>`detected`（この段階の書き手あり）<br>`diluted_shares`（この段階の書き手あり）<br>`dilution_3yr_annual_pct`（この段階の書き手あり）<br>`dilution_comment`（この段階の書き手あり）<br>`dilution_severity`（この段階の書き手あり）<br>`divergence_ratio`（この段階の書き手あり）<br>`divergence_warning`（この段階の書き手あり）<br>`dupont`（この段階の書き手あり）<br>`end`（この段階の書き手あり）<br>`estimated_fcf`（この段階の書き手あり）<br>`estimated_revenue`（この段階の書き手あり）<br>`excluded`（この段階の書き手あり）<br>`expected_return_pct`（この段階の書き手あり）<br>`fcf_2yr_avg`（この段階の書き手あり）<br>`fcf_5yr_avg`（この段階の書き手あり）<br>`fcf_base`（この段階の書き手あり）<br>`fcf_base_method`（この段階の書き手あり）<br>`fcf_base_used`（この段階の書き手あり）<br>`fcf_estimation`（この段階の書き手あり）<br>`fcf_outlier`（この段階の書き手あり）<br>`fcf_value`（この段階の書き手あり）<br>`fields`（この段階の書き手あり）<br>`financial_health`（この段階の書き手あり）<br>`fiscal_year`（この段階の書き手あり）<br>`formula`（この段階の書き手あり）<br>`found`（この段階の書き手あり）<br>`future_value`（この段階の書き手あり）<br>`future_values`（この段階の書き手あり）<br>`growth`（この段階の書き手あり）<br>`growth_options`（この段階の書き手あり）<br>`growth_rate`（この段階の書き手あり）<br>`growth_sanity`（この段階の書き手あり）<br>`growth_scenarios`（この段階の書き手あり）<br>`growth_years`（この段階の書き手あり）<br>`high_growth_rate_used`（この段階の書き手あり）<br>`high_growth_years`（この段階の書き手あり）<br>`industry`（この段階の書き手あり）<br>`insider_buy_count`（この段階の書き手あり）<br>`insider_latest_date`（この段階の書き手あり）<br>`insider_net_direction`（この段階の書き手あり）<br>`insider_sell_count`（この段階の書き手あり）<br>`intrinsic_value_beta`（この段階の書き手あり）<br>`intrinsic_value_per_share`（この段階の書き手あり）<br>`intrinsic_value_rf`（この段階の書き手あり）<br>`is_annual`（ほかの書き手あり）<br>`items`（この段階の書き手あり）<br>`label`（この段階の書き手あり）<br>`latest_period_missing`（この段階の書き手あり）<br>`latest_revenue`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`long_term_debt`（この段階の書き手あり）<br>`ma_addback_detected_but_not_applied`（この段階の書き手あり）<br>`ma_addback_excluded`（この段階の書き手あり）<br>`market_return`（この段階の書き手あり）<br>`matrix`（この段階の書き手あり）<br>`maturity_profile`（この段階の書き手あり）<br>`max`（この段階の書き手あり）<br>`method`（この段階の書き手あり）<br>`min`（この段階の書き手あり）<br>`model`（この段階の書き手あり）<br>`name`（この段階の書き手あり）<br>`net_cash`（この段階の書き手あり）<br>`net_cash_per_share`（この段階の書き手あり）<br>`net_debt`（この段階の書き手あり）<br>`net_margin`（この段階の書き手あり）<br>`next_earnings_date`（この段階の書き手あり）<br>`ni_ocf_divergence`（画面の中で作る値）<br>`note`（この段階の書き手あり）<br>`overall`（この段階の書き手あり）<br>`per`（この段階の書き手あり）<br>`per_adjusted`（この段階の書き手あり）<br>`phase1`（この段階の書き手あり）<br>`primary`（この段階の書き手あり）<br>`pv_high`（この段階の書き手あり）<br>`pv_phase1`（この段階の書き手あり）<br>`pv_phase2`（この段階の書き手あり）<br>`pv_terminal`（この段階の書き手あり）<br>`rate`（この段階の書き手あり）<br>`rate_is_sector_default`（この段階の書き手あり）<br>`ratio`（この段階の書き手あり）<br>`raw_fcf`（この段階の書き手あり）<br>`raw_weighted_growth`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`recommended_g`（この段階の書き手あり）<br>`return_metrics`（この段階の書き手あり）<br>`revenue_decline`（画面の中で作る値）<br>`revenue_ttm`（この段階の書き手あり）<br>`revenue_ttm_end`（この段階の書き手あり）<br>`rice`（この段階の書き手あり）<br>`rice_adj`（この段階の書き手あり）<br>`rice_na_reason`（書き手なし）<br>`rice_per_ratio`（この段階の書き手あり）<br>`risk_free_rate`（この段階の書き手あり）<br>`roe_used`（この段階の書き手あり）<br>`rpo`（この段階の書き手あり）<br>`rpo_pv`（この段階の書き手あり）<br>`rule`（この段階の書き手あり）<br>`sbc_ttm`（この段階の書き手あり）<br>`scenario_valuations`（この段階の書き手あり）<br>`score_comment`（この段階の書き手あり）<br>`sector`（この段階の書き手あり）<br>`segment`（この段階の書き手あり）<br>`segment_growth_quarter`（この段階の書き手あり）<br>`segment_growth_source`（この段階の書き手あり）<br>`segments`（この段階の書き手あり）<br>`sensitivity`（この段階の書き手あり）<br>`shares_yr_3ago`（この段階の書き手あり）<br>`shares_yr_now`（この段階の書き手あり）<br>`short_term_debt`（この段階の書き手あり）<br>`short_term_investments`（この段階の書き手あり）<br>`source`（この段階の書き手あり）<br>`substituted_by_annual`（この段階の書き手あり）<br>`tanuki_score`（この段階の書き手あり）<br>`terminal_growth_used`（この段階の書き手あり）<br>`total_debt`（この段階の書き手あり）<br>`total_pv`（この段階の書き手あり）<br>`total_transient_amount`（この段階の書き手あり）<br>`transient_evidence`（この段階の書き手あり）<br>`upside_percent`（この段階の書き手あり）<br>`upside_percent_beta`（この段階の書き手あり）<br>`upside_percent_rf`（この段階の書き手あり）<br>`v0`（この段階の書き手あり）<br>`v0_rm`（この段階の書き手あり）<br>`val`（ほかの書き手あり）<br>`validation`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`wacc`（この段階の書き手あり）<br>`wacc_values`（この段階の書き手あり）<br>`warnings`（この段階の書き手あり）<br>`weight`（この段階の書き手あり）<br>`years`（この段階の書き手あり） |  |
| （JSで作る要素 #all-matrices-grid） | `available`（この段階の書き手あり）<br>`base`（この段階の書き手あり）<br>`components`（この段階の書き手あり）<br>`deficit_quality`（ほかの書き手あり）<br>`error`（ほかの書き手あり）<br>`fcf_base_used`（この段階の書き手あり）<br>`fcf_estimation`（この段階の書き手あり）<br>`fcf_margin`（この段階の書き手あり）<br>`latest_revenue`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`max`（この段階の書き手あり）<br>`min`（この段階の書き手あり）<br>`note`（この段階の書き手あり）<br>`revenue_growth_pct`（ほかの書き手あり）<br>`rice`（この段階の書き手あり）<br>`roe_10yr_avg`（この段階の書き手あり）<br>`runway`（ほかの書き手あり）<br>`runway_months`（ほかの書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり）<br>`title`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`upside_percent`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| （JSで作る要素 #cf-margin-chart） | `paper_bgcolor`（画面の中で作る値）<br>`plot_bgcolor`（画面の中で作る値） |  |
| （JSで作る要素 #cf-quality-chart） | `paper_bgcolor`（画面の中で作る値）<br>`plot_bgcolor`（画面の中で作る値） |  |
| （JSで作る要素 #cf-waterfall-chart） | `base_fcf`（この段階の書き手あり）<br>`estimated_fcf`（この段階の書き手あり）<br>`fcf_base`（この段階の書き手あり）<br>`fcf_estimation`（この段階の書き手あり） |  |
| （JSで作る要素 #cf-waterfall-section） | `CapEx`（ほかの書き手あり）<br>`DA`（ほかの書き手あり）<br>`OCF`（ほかの書き手あり）<br>`Revenue`（ほかの書き手あり）<br>`SBC`（ほかの書き手あり）<br>`label`（この段階の書き手あり） |  |
| （JSで作る要素 #growth-sanity-container） | `amount`（この段階の書き手あり）<br>`asset_turnover`（この段階の書き手あり）<br>`bear_multiplier`（この段階の書き手あり）<br>`bull_multiplier`（この段階の書き手あり）<br>`category`（この段階の書き手あり）<br>`clipped`（この段階の書き手あり）<br>`components`（この段階の書き手あり）<br>`damodaran_industry`（この段階の書き手あり）<br>`damodaran_year`（この段階の書き手あり）<br>`delay_years`（この段階の書き手あり）<br>`fcf`（この段階の書き手あり）<br>`fcf_growth_cap`（この段階の書き手あり）<br>`fcf_growth_floor`（この段階の書き手あり）<br>`fcf_history`（この段階の書き手あり）<br>`fcf_margin`（この段階の書き手あり）<br>`fcf_ttm_end`（この段階の書き手あり）<br>`financial_leverage`（この段階の書き手あり）<br>`growth`（この段階の書き手あり）<br>`growth_cap`（この段階の書き手あり）<br>`growth_floor`（この段階の書き手あり）<br>`insider_latest_date`（この段階の書き手あり）<br>`item_name`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`market_return`（この段階の書き手あり）<br>`max`（この段階の書き手あり）<br>`method`（この段階の書き手あり）<br>`moat_phase1_years`（この段階の書き手あり）<br>`moat_score_source`（この段階の書き手あり）<br>`name`（この段階の書き手あり）<br>`next_earnings_date`（この段階の書き手あり）<br>`note`（この段階の書き手あり）<br>`op_margin`（この段階の書き手あり）<br>`options`（この段階の書き手あり）<br>`penetration`（この段階の書き手あり）<br>`phase1`（この段階の書き手あり）<br>`phase1_growth`（この段階の書き手あり）<br>`phase1_growth_auto_adjusted`（この段階の書き手あり）<br>`phase2`（この段階の書き手あり）<br>`probability`（この段階の書き手あり）<br>`pv`（この段階の書き手あり）<br>`raw_weighted_growth`（この段階の書き手あり）<br>`recommended_g`（この段階の書き手あり）<br>`recommended_g_median`（この段階の書き手あり）<br>`roe_decomposed`（この段階の書き手あり）<br>`rpo_adjustment`（この段階の書き手あり）<br>`rpo_incremental`（この段階の書き手あり）<br>`signals`（この段階の書き手あり）<br>`tam`（この段階の書き手あり）<br>`v0`（この段階の書き手あり）<br>`verdict`（この段階の書き手あり）<br>`wacc`（この段階の書き手あり）<br>`warnings`（この段階の書き手あり）<br>`year`（この段階の書き手あり）<br>`years`（この段階の書き手あり） |  |
| （JSで作る要素 #history-chart-section） | `current_price`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`intrinsic_value_per_share`（この段階の書き手あり）<br>`invalid`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`ma200`（この段階の書き手あり）<br>`scenario_bear`（この段階の書き手あり）<br>`scenario_bull`（この段階の書き手あり） |  |
| （JSで作る要素 #hype-history-body） | `date`（この段階の書き手あり）<br>`from_peak`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`lifecycle`（この段階の書き手あり）<br>`ma200_dev`（この段階の書き手あり）<br>`phase`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`price_iv_ratio`（この段階の書き手あり）<br>`recommendation`（この段階の書き手あり）<br>`rev_yoy`（この段階の書き手あり）<br>`rule40`（この段階の書き手あり）<br>`stage`（この段階の書き手あり） |  |
| （JSで作る要素 #hype-history-section） | `length`（ほかの書き手あり） |  |
| （JSで作る要素 #hype-signal-row） | `base`（この段階の書き手あり）<br>`expectation_score`（ほかの書き手あり）<br>`length`（ほかの書き手あり）<br>`monthly`（ほかの書き手あり）<br>`rice`（この段階の書き手あり）<br>`stage`（この段階の書き手あり）<br>`stage_label`（ほかの書き手あり）<br>`upside_percent`（この段階の書き手あり） |  |
| （JSで作る要素 #layer2AdjustedIvps） | `alpha`（この段階の書き手あり）<br>`bs_adjustment`（この段階の書き手あり）<br>`components`（この段階の書き手あり）<br>`dcf_components`（この段階の書き手あり）<br>`diluted_shares`（この段階の書き手あり）<br>`growth_options`（この段階の書き手あり）<br>`net_cash_per_share`（この段階の書き手あり）<br>`rpo_pv`（この段階の書き手あり）<br>`total_pv`（この段階の書き手あり）<br>`v0`（この段階の書き手あり）<br>`v0_rm`（この段階の書き手あり） |  |
| （JSで作る要素 #rfMarketBlock） | `required_growth_rf_base`（この段階の書き手あり）<br>`required_growth_rf_live`（この段階の書き手あり）<br>`risk_free_rate`（この段階の書き手あり）<br>`risk_free_rate_live`（この段階の書き手あり）<br>`risk_free_rate_live_date`（この段階の書き手あり）<br>`wacc`（この段階の書き手あり） |  |
| （JSで作る要素 #rfValueBlock） | `intrinsic_value_rf_rate`（この段階の書き手あり）<br>`market_return`（この段階の書き手あり）<br>`risk_free_rate`（この段階の書き手あり）<br>`risk_free_rate_live_date`（この段階の書き手あり）<br>`risk_free_rate_live_source`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`wacc`（この段階の書き手あり） |  |
| （JSで作る要素 #signal-consistency-body） | `erp`（この段階の書き手あり）<br>`label`（この段階の書き手あり）<br>`length`（ほかの書き手あり）<br>`month`（ほかの書き手あり）<br>`monthly`（ほかの書き手あり）<br>`price_iv_ratio`（この段階の書き手あり）<br>`risk_free_rate_live`（この段階の書き手あり）<br>`risk_free_rate_live_date`（この段階の書き手あり）<br>`risk_free_rate_live_source`（この段階の書き手あり）<br>`stage`（この段階の書き手あり）<br>`stage_label`（ほかの書き手あり） |  |
| （区画を決められない） | `CapEx`（ほかの書き手あり）<br>`OCF`（ほかの書き手あり）<br>`pv_fcf_rm`（この段階の書き手あり）<br>`pv_phase1_rm`（この段階の書き手あり）<br>`pv_phase2_rm`（この段階の書き手あり）<br>`pv_tv_rm`（この段階の書き手あり） |  |

## ワークフローが自分で書く出力（.pyの外）

段階の.pyを動かすワークフローの中で、`$GITHUB_STEP_SUMMARY`に書く段。`.get()`で読むキーが、この段階のJSON（全ファイル）にあるか。

- `.github/workflows/TANUKI_VALUATION_Update.yml`（行 104・105・106・107・112）: `intrinsic_value_per_share`（ある）・`overall`（ある）・`validation`（ある）

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | TANUKIか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1066`<br>`src/value/hypecore/hypecore.py:1111` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。TANUKIのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | TANUKIを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:720`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:863`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:891` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:873`<br>`common/sec_data/layer3_builder.py:1111`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:219`<br>`src/tail/edgar_rss_monitor.py:419`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:602` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1587`<br>`common/sec_data/report_consistency_check.py:1780`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:285` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:375`<br>`src/tail/xbrl_segment_fetcher.py:180`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | はい |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:137` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:650`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1140`<br>`docs/value-monitor/tanuki_valuation/stock.html:1141`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:891`<br>`src/value/tanuki_valuation/pipeline.py:911`<br>`src/value/tanuki_valuation/pipeline.py:913`<br>`src/value/tanuki_valuation/pipeline.py:982`<br>`src/value/tanuki_valuation/pipeline.py:983`<br>`src/value/tanuki_valuation/pipeline.py:984`<br>`src/value/tanuki_valuation/pipeline.py:985`<br>`src/value/tanuki_valuation/pipeline.py:1370`<br>`src/value/tanuki_valuation/pipeline.py:1397`<br>`src/value/tanuki_valuation/pipeline.py:1402`<br>`src/value/tanuki_valuation/pipeline.py:1448`<br>`src/value/tanuki_valuation/pipeline.py:1753`<br>`src/value/tanuki_valuation/pipeline.py:1931` | はい |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1127` | はい |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1111` | はい |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（src/value/tanuki_valuation）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 名前を挙げたもの（`diag_iv_trace.py`・`growth_model_audit.py`・`remove_legacy_tanuki_score.py`・`pipeline_log.txt`）と、段階のフォルダの.pyのうちワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの

### `src/value/tanuki_valuation`（4ファイル・0行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `diag_iv_trace.py` | 0 | — | — | — | — | 残す（2026-10-20） |
| `growth_model_audit.py` | 0 | — | — | — | — | 残す（2026-10-20） |
| `pipeline_log.txt` | 0 | — | — | — | — | 残す（2026-10-20） |
| `remove_legacy_tanuki_score.py` | 0 | — | — | — | — | 残す（2026-10-20） |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 2件
  - `src/value/tanuki_valuation/data_fetcher.py:255` TTMReader._load → `<?>`
  - `src/value/tanuki_valuation/data_fetcher.py:506` _load_beta_config → `<?>`
