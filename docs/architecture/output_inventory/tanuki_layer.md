# TANUKI VALUATION・TANUKI Scoreの出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`tanuki_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`docs/value-monitor/tanuki_valuation/`・`docs/value-monitor/tanuki_score/`があれば「効く」
- 残す／消す: 空欄（Koichiさんが決める）

**件数**: 出力 13（ファイル 11・人が見るもの 2）／読み手0件 0／テストだけ 0／1件 2／2件以上 9／規則の二重 10（検出式 10種のうち。TANUKIのファイルを含むもの 5）

**画面の項目**: 画面が読むキー 369（この段階の書き手あり 313・ほかの書き手あり 35・画面の中で作る値 6・**書き手なし 15**）／書き手があるのに誰も読まないキー **53**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋1件（7.1%）、読み方を決めきれない読み込み 2件／解決できた読み込み 35件（5.4%）→ 当たらない

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>（1ファイル） | score_watcherが前回の判定を覚えておく控え（判定の変化の通知に使う） | `score_watcher.py:save_state` | 1 | — | — | — |  |
| 1件 | `docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>（99ファイル） | 銘柄ごとのHypeCoreのライフサイクル・ステージ・フェーズの推移（TANUKIの実行のたびに1行） | `pipeline.py:TanukiValuationPipeline._save_hypecore_history` | 1 | value-monitor/tanuki_valuation/stock.html（直接） | 効く: TANUKI VALUATIONの画面 | — |  |
| 2件以上 | `config/beta_config.json`<br>（1ファイル） | 銘柄ごとのβ（市場との連動の強さ）の上書きの設定。WACCの計算に使う | `beta_fetcher.py:save_config` | 12・テスト13 | index.html（経由）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（直接）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: TANUKI VALUATIONの画面 | AS-IS-013 |  |
| 2件以上 | `docs/integrated-dashboard/daily_pick.json`<br>（1ファイル） | TANUKI SCOREの今日の1銘柄（daily pick）と選んだ理由 | `daily_pick.py:main` | 3 | market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 判定（TANUKI SCORE）・TANUKI SCOREの画面 | AS-IS-285・AS-IS-286・AS-IS-287 |  |
| 2件以上 | `docs/integrated-dashboard/history.json`<br>（1ファイル） | daily pickの過去30日分の選出の履歴 | `daily_pick.py:save_history` | 24・テスト12 | index.html（経由）<br>market-monitor/market-pulse/index.html（直接）<br>portfolio/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（経由）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 判定（TANUKI SCORE）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-258・AS-IS-297・AS-IS-298・AS-IS-299・AS-IS-506 |  |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/tickers.json`<br>（1ファイル） | 一覧のページ用の、全銘柄の理論株価・判定の要約 | `pipeline.py:TanukiValuationPipeline._save_tickers_index` | 49・テスト30 | common/ticker-links.js（直接）<br>index.html（直接）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（直接）<br>value-monitor/admin.html（直接）<br>value-monitor/extreme-fear/index.html（直接）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（直接）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 判定（TANUKI SCORE）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-082 |  |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>（96ファイル） | 銘柄ごとの理論株価・上振れ余地・成長率などの推移（実行ごとに1行） | `pipeline.py:TanukiValuationPipeline._save_result` | 23・テスト10 | market-monitor/market-pulse/index.html（直接）<br>portfolio/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 判定（TANUKI SCORE）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-258・AS-IS-297・AS-IS-298・AS-IS-299・AS-IS-506 |  |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>（28217ファイル） | 銘柄ごとの、実行の日時ごとの評価の全体のスナップショット | `pipeline.py:TanukiValuationPipeline._save_result` | 22・テスト12 | market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 判定（TANUKI SCORE）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-047・AS-IS-116・AS-IS-258・AS-IS-297・AS-IS-298・AS-IS-299・AS-IS-438・AS-IS-439・AS-IS-506 |  |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/latest.json`<br>（96ファイル） | 銘柄ごとの最新の評価の全体（理論株価・判定・DCF・RICE・各指標）。個別ページが読む | `pipeline.py:TanukiValuationPipeline._save_result`<br>`pipeline.py:TanukiValuationPipeline._save_result` | 29・テスト19 | market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/extreme-fear/index.html（直接）<br>value-monitor/hypecore/detail.html（直接）<br>value-monitor/stonks-silo/index.html（直接）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 判定（TANUKI SCORE）・HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-056・AS-IS-071 |  |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/report.txt`<br>（96ファイル） | 銘柄ごとの評価の計算の説明（人が読む文の形式のレポート） | `pipeline.py:TanukiValuationPipeline._save_report` | 6・テスト6 | market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 判定（TANUKI SCORE）・TANUKI VALUATIONの画面 | AS-IS-045・AS-IS-056・AS-IS-057・AS-IS-058・AS-IS-060 |  |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`<br>（98ファイル） | 銘柄ごとのTANUKI SCORE（funda等）・判定時の株価と、その後30・60・90日の値動きの推移 | `pipeline.py:TanukiValuationPipeline._save_result`<br>`score_verifier.py:verify_ticker` | 3・テスト3 | value-monitor/tanuki_score/index.html（直接） | 効く: TANUKI SCOREの画面 | — |  |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `${GITHUB_STEP_SUMMARY}` | GitHub Actionsの実行の画面に出す要約（人が見る） | `beta_fetcher.py:main`<br>`score_watcher.py:main` | .github/workflows/Beta_Config_Update.yml |
| `Discord（DISCORD_WEB_HOOK）` | βの更新・判定の変化のDiscordへの通知 | `beta_fetcher.py:post_discord`<br>`score_watcher.py:post_discord` | .github/workflows/Beta_Config_Update.yml |

## 読み手の内訳

### `docs/value-monitor/tanuki_valuation/data/.watcher_state.json`（1件）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `AAPL`(0)・`ABBV`(0)・`ADBE`(0)・`ADSK`(0)・`ALAB`(0)・`AMAT`(0)・`AMD`(0)・`AMZN`(0)・`APP`(0)・`ASTS`(0)・`AVAV`(0)・`BBAI`(0)・`BKNG`(0)・`BROS`(0)・`BSY`(0)・`CAKE`(0)・`CART`(0)・`CAT`(0)・`CDNS`(0)・`CEG`(0)・`CELH`(0)・`CIX`(0)・`COHR`(0)・`CPRT`(0)・`CRM`(0)・`CRWV`(0)・`CSGP`(0)・`DDOG`(0)・`DELL`(0)・`DOCN`(0)・`ELF`(0)・`ENTG`(0)・`ESTC`(0)・`FCX`(0)・`FICO`(0)・`FLYW`(0)・`FROG`(0)・`FRSH`(0)・`GEV`(0)・`GOOGL`(0) ほか58
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 38・59・271・314（経由 `src/value/tanuki_valuation/score_watcher.py::load_state`）

### `docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`（1件）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 531

### `config/beta_config.json`（2件以上）
- 起動元: .github/workflows/Beta_Config_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `_comment`(0)・`_damodaran_ref_2025`(1)・`_update_note`(0)・`_updated_at`(1)・`overrides`(9)
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 101
- Python・保存済みファイル: `common/sec_data/audit.py` 行 33・310・390・468（経由 `common/sec_data/audit.py::audit_beta_drift`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 52・82・104・111・167・202・230・247・295・356・397・480 ほか10（経由 `common/sec_data/registration_validator.py::_load_json`、`common/sec_data/registration_validator.py::check_p1_registration_completeness`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 356
- Python・保存済みファイル: `common/system_health.py` 行 205・296・788・1045・1049・1110（経由 `common/sec_data/registration_validator.py::_load_json`、`common/system_health.py::check_h_config`）
- Python・保存済みファイル: `src/value/tanuki_valuation/beta_fetcher.py` 行 40・71・155・288・319（経由 `src/value/tanuki_valuation/beta_fetcher.py::load_config`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 483・489・503・509・558・561・874・1153（経由 `src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher._determine_beta`、`src/value/tanuki_valuation/data_fetcher.py::_load_beta_config`、`src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`）
- Python・保存済みファイル: `src/value/tanuki_valuation/growth_sanity.py` 行 217
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 427・491・1007・1330・1862・1863・3814・3816・3872（経由 `src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher._determine_beta`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_beta_sector`）
- JS・HTML・保存済みファイル: `docs/value-monitor/admin.html` 行 936・1997・2002
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/index.html` 行 269
- ワークフロー・保存済みファイル: `.github/workflows/Beta_Config_Update.yml` 行 88・92
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 52・87・95・99・104・111（経由 `common/sec_data/audit.py::audit_beta_drift`）
- テスト・保存済みファイル: `tests/test_beta_fetcher.py` 行 36・80・89・98・105・111・121・129・173・188・201・211 ほか2（経由 `src/value/tanuki_valuation/beta_fetcher.py::load_config`）
- テスト・保存済みファイル: `tests/test_data_fetcher_market_data_switch.py` 行 68・76・83・91・112・121・129・137・165・177・184・191（経由 `src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher._determine_beta`）
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96・102・108（経由 `common/sec_data/audit.py::audit_beta_drift`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 211・233・253・266・285・289・294・299・304・309・637・657 ほか22（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 168（経由 `common/sec_data/registration_validator.py::_load_json`）
- テスト・保存済みファイル: `tests/test_registration_validator.py` 行 33
- テスト・保存済みファイル: `tests/test_rice_report_txt_none_display.py` 行 39・51・63・86・102（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_h_config`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::_load_json`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_h_config`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/integrated-dashboard/daily_pick.json`（2件以上）
- 起動元: .github/workflows/TANUKI_Score_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `category`(2)・`company`(2)・`funda_score`(2)・`generated_at`(2)・`report`(2)・`selection_reason`(2)・`tanuki`(1)・`ticker`(2)・`timing_score`(2)
- Python・保存済みファイル: `src/value/tanuki_score/daily_pick.py` 行 26
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 695
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_Score_Update.yml` 行 60
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/integrated-dashboard/history.json`（2件以上）
- 起動元: .github/workflows/TANUKI_Score_Update.yml
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 922・1247・1273（経由 `browser_checks/check_macro_pulse.py::compare`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 644
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 15
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 57・324・513・692・706・708（経由 `common/sec_data/registration_validator.py::check_p6_cik_discontinuity_candidate`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 412・493・516・518・519・520・674・676・680・706・714（経由 `common/sec_data/rotce.py::build_all`、`common/sec_data/rotce.py::compute_ticker`）
- Python・保存済みファイル: `common/system_health.py` 行 101・1039・1110（経由 `common/system_health.py::check_b_score_history`）
- Python・保存済みファイル: `src/portfolio/snapshot.py` 行 119・142（経由 `src/portfolio/snapshot.py::main`）
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 31
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 61
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/extract_key_facts.py` 行 258・484・1262・1285（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 643（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 318・518・1060・1267（経由 `src/value/hypecore/hypecore.py::fetch_tanuki_iv`）
- Python・保存済みファイル: `src/value/tanuki_score/daily_pick.py` 行 27・42・48・52・56・59・66・72・79・90・94・259 ほか9（経由 `src/value/tanuki_score/daily_pick.py::load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 491・988・1216・1248・3872（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._save_result`）
- Python・保存済みファイル: `src/value/tanuki_valuation/remove_legacy_tanuki_score.py` 行 52・56・75（経由 `src/value/tanuki_valuation/remove_legacy_tanuki_score.py::main`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 57・119・124（経由 `src/value/tanuki_valuation/score_verifier.py::verify_ticker`）
- JS・HTML・保存済みファイル: `docs/market-monitor/market-pulse/index.html` 行 517
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 217
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 916
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 800
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 761・766
- ワークフロー・保存済みファイル: `.github/workflows/Score_Verifier.yml` 行 45
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_Score_Update.yml` 行 61・62
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` 行 132
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 222・232（経由 `src/value/tanuki_score/daily_pick.py::load_json`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py` 行 103・123・143・163（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py` 行 106・128・151・174（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_eps_cik_history_merge.py` 行 70・84・92（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_net_income_selection.py` 行 107・154（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_share_fallback.py` 行 88・122・149・190（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_split.py` 行 100・124・149・176（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_rotce.py` 行 64・70・71・81・82・94・96・101・110・111・119・120 ほか47（経由 `common/sec_data/rotce.py::compute_ticker`）
- テスト・保存済みファイル: `tests/test_score_verifier.py` 行 29・41・42・56（経由 `tests/test_score_verifier.py::_make_score_history`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_b_score_history`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_b_score_history`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._save_result`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/index.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/admin.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`

### `docs/value-monitor/tanuki_valuation/data/tickers.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `count`(9)・`tickers`(48)・`updated_at`(3)
- Python・保存済みファイル: `browser_checks/check_valuation_chart_basis.py` 行 171・213（経由 `browser_checks/check_valuation_chart_basis.py::main`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 47・866・946
- Python・保存済みファイル: `common/market_data/fetcher.py` 行 1502・1528・1529・1559・1568・1580・1581・1645（経由 `common/market_data/fetcher.py::get_monitor_tickers`、`common/market_data/fetcher.py::get_sp500_constituents`）
- Python・保存済みファイル: `common/market_data/reader.py` 行 388（経由 `common/market_data/fetcher.py::get_sp500_constituents`）
- Python・保存済みファイル: `common/registration/preflight_check.py` 行 207・223（経由 `common/registration/preflight_check.py::main`）
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 268・274・422・445・456・467（経由 `common/registration/register_ticker.py::main`、`common/registration/register_ticker.py::step6_discover_register`）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 398・448（経由 `common/screening/dcf_validity_checker.py::main`）
- Python・保存済みファイル: `common/screening/report_txt_parser.py` 行 270・305（経由 `common/screening/report_txt_parser.py::main`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 56・59・359・361・468（経由 `common/sec_data/audit.py::get_registered_tickers`、`common/sec_data/audit.py::main`）
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 124・164・299・488・606（経由 `common/sec_data/fetcher.py::SECFetcher.get_cik`）
- Python・保存済みファイル: `common/sec_data/freshness.py` 行 130・132・133・134・136・187・192
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 398・484・521・692・706・708（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`、`common/sec_data/registration_validator.py::run`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 215・1116・1686・1743・2590・3400・3534・3554・3632（経由 `common/sec_data/report_consistency_check.py::_check_sec_freshness`、`common/sec_data/report_consistency_check.py::_load_stonks_results`、`scripts/gen_update_schedule.py::curated_input_problems`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 695・706・707・709・714（経由 `common/sec_data/rotce.py::build_all`、`common/sec_data/rotce.py::main`）
- Python・保存済みファイル: `common/sec_data/update.py` 行 55・91・118・173・175・338・353・407・417・428・432（経由 `common/sec_data/fetcher.py::SECFetcher.get_cik`、`common/sec_data/freshness.py::write`、`common/sec_data/update.py::_finalize` ほか）
- Python・保存済みファイル: `common/system_health.py` 行 159・188・355・404・788・884・956・1032・1041・1042・1046・1049 ほか2（経由 `common/sec_data/audit.py::get_registered_tickers`、`common/sec_data/registration_validator.py::check_p4_orphan_configs`、`common/system_health.py::_check_i` ほか）
- Python・保存済みファイル: `discover/stonks-silo/src/pipeline.py` 行 196・229・275（経由 `discover/stonks-silo/src/pipeline.py::run`）
- Python・保存済みファイル: `scripts/analysis/market_pulse_redesign.py` 行 60・62・233・478（経由 `scripts/analysis/market_pulse_redesign.py::load_inputs`）
- Python・保存済みファイル: `scripts/gen_update_schedule.py` 行 217・218・242・253・269（経由 `scripts/gen_update_schedule.py::curated_input_problems`）
- Python・保存済みファイル: `src/market/market_pulse/backfill_implb.py` 行 49・63（経由 `src/market/market_pulse/sector_rotation.py::watch_list`）
- Python・保存済みファイル: `src/market/market_pulse/backfill_implc.py` 行 39・62（経由 `src/market/market_pulse/collect_and_send.py::compute_implc`）
- Python・保存済みファイル: `src/market/market_pulse/breadth_calculator.py` 行 34・81・82・127・135・136・507（経由 `src/market/market_pulse/breadth_calculator.py::get_sp500_tickers`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 1559・1583・1967・1969（経由 `src/market/market_pulse/collect_and_send.py::compute_implc`、`src/market/market_pulse/sector_rotation.py::watch_list`）
- Python・保存済みファイル: `src/market/market_pulse/sector_rotation.py` 行 281
- Python・保存済みファイル: `src/tail/edgar_rss_monitor.py` 行 245・601・638（経由 `src/tail/edgar_rss_monitor.py::get_cik_map`）
- Python・保存済みファイル: `src/tail/xbrl_segment_fetcher.py` 行 72・569・746・758（経由 `src/tail/xbrl_segment_fetcher.py::get_cik`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/company_metadata.py` 行 38
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/extract_key_facts.py` 行 198・480・1262・1285（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 509・594・643・649・655・926・975・1055・1059・1066・1078・1089（経由 `src/value/adjusted_eps_analyzer/company_metadata.py::get_company_metadata`、`src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`、`src/value/adjusted_eps_analyzer/pipeline.py::_write_step_summary` ほか）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 1198・1202・1210・1291（経由 `src/value/hypecore/hypecore.py::_save_tickers_index`）
- Python・保存済みファイル: `src/value/tanuki_score/daily_pick.py` 行 42・48・49・52・56・59・66・69・72・79・90・94 ほか10（経由 `src/value/tanuki_score/daily_pick.py::load_eps_summary`、`src/value/tanuki_score/daily_pick.py::load_json`、`src/value/tanuki_score/daily_pick.py::load_tickers`）
- Python・保存済みファイル: `src/value/tanuki_valuation/beta_fetcher.py` 行 83・86・279・319（経由 `src/value/tanuki_valuation/beta_fetcher.py::get_registered_tickers`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/fcfe.py` 行 67・72・79・80（経由 `src/value/tanuki_valuation/calculator/fcfe.py::_load_financial_institution_config`）
- Python・保存済みファイル: `src/value/tanuki_valuation/growth_model_audit.py` 行 39・152（経由 `src/value/tanuki_valuation/growth_model_audit.py::audit_growth_models`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 439・491・593・811・830・992・1072・1149・1330・1537・1622・2959 ほか4（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._inject_ttm_for_general_segment` ほか）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 49・52・264・314（経由 `src/value/tanuki_valuation/score_watcher.py::get_tickers`）
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 16・18・28・34
- JS・HTML・保存済みファイル: `docs/index.html` 行 246
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 274
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/admin.html` 行 335
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/tickers.html` 行 245
- JS・HTML・保存済みファイル: `docs/value-monitor/admin.html` 行 210・2788・3092・3105
- JS・HTML・保存済みファイル: `docs/value-monitor/extreme-fear/index.html` 行 113・364
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/index.html` 行 214
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 442
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/index.html` 行 535
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 2715
- ワークフロー・保存済みファイル: `.github/workflows/Stonks_Silo_Update.yml` 行 88
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_CIK_Lookup.yml` 行 91
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_sec_freshness`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 222・232（経由 `src/value/tanuki_score/daily_pick.py::load_json`）
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96・102・108（経由 `common/sec_data/audit.py::main`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py` 行 103・123・143・163（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py` 行 106・128・151・174（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`）
- テスト・保存済みファイル: `tests/test_eps_cik_history_merge.py` 行 70・84・92（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`）
- テスト・保存済みファイル: `tests/test_eps_run_status.py` 行 45・95・115・123・125・132・133・135・144・151・159・160 ほか18（経由 `common/system_health.py::_check_i`、`src/value/adjusted_eps_analyzer/pipeline.py::generate_summary`、`src/value/adjusted_eps_analyzer/pipeline.py::run` ほか）
- テスト・保存済みファイル: `tests/test_eps_upc_prereorg_filter.py` 行 177・178（経由 `src/value/adjusted_eps_analyzer/pipeline.py::generate_summary`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_net_income_selection.py` 行 107・154（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_share_fallback.py` 行 88・122・149・190（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_split.py` 行 100・124・149・176（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_cik`）
- テスト・保存済みファイル: `tests/test_fcfe.py` 行 34・39・44・49・55
- テスト・保存済みファイル: `tests/test_latest_period_guard.py` 行 68・72・80・83・90・91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`）
- テスト・保存済みファイル: `tests/test_market_data_reader.py` 行 452・457・473（経由 `common/market_data/fetcher.py::get_sp500_constituents`）
- テスト・保存済みファイル: `tests/test_market_pulse_sector_rotation.py` 行 117・168（経由 `src/market/market_pulse/sector_rotation.py::watch_list`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 165・211・233・253・266・285・289・294・299・304・309・328 ほか83（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map` ほか）
- テスト・保存済みファイル: `tests/test_register_ticker_preflight.py` 行 62・84・100・118（経由 `common/registration/register_ticker.py::step6_discover_register`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82・148（経由 `common/registration/register_ticker.py::step6_discover_register`、`common/sec_data/report_consistency_check.py::_check_sec_freshness`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 943・952・961・970・980・988・996・1004（経由 `common/sec_data/report_consistency_check.py::_load_stonks_results`）
- テスト・保存済みファイル: `tests/test_rice_report_txt_none_display.py` 行 39・51・63・86・102（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 121・122・279・292・307・335・343・353・357・359・360・365 ほか20（経由 `common/sec_data/audit.py::get_registered_tickers`、`common/sec_data/fetcher.py::SECFetcher.get_cik`、`common/sec_data/freshness.py::step_summary_markdown` ほか）
- テスト・保存済みファイル: `tests/test_sec_update_ticker_failure.py` 行 99・111・123・131・180・191・294・299・300・306・312・317（経由 `common/sec_data/freshness.py::write`、`common/sec_data/report_consistency_check.py::_check_sec_freshness`、`common/sec_data/update.py::_finalize` ほか）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_stonks_silo_financial_as_of.py` 行 140・177
- テスト・保存済みファイル: `tests/test_stonks_silo_pipeline.py` 行 203・205・212・222・223・289・300・301・302
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::check_p4_orphan_configs`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/sec_data/audit.py::get_registered_tickers`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._compute_tanuki_score`）
- テスト・保存済みファイル: `tests/test_update_schedule.py` 行 30・31・50・52・64・73・88・89・90（経由 `scripts/gen_update_schedule.py::curated_input_problems`）
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/tail/edgar_rss_monitor.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/edgar_rss_monitor.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（discover/stonks-silo/src/pipeline.py → docs/value-monitor/stonks-silo/data/results.json））: `docs/value-monitor/stonks-silo/index.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/history.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 922・1247・1273（経由 `browser_checks/check_macro_pulse.py::compare`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 644
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 15
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 57・324・513・692・706・708（経由 `common/sec_data/registration_validator.py::check_p6_cik_discontinuity_candidate`）
- Python・保存済みファイル: `common/sec_data/rotce.py` 行 412・493・516・518・519・520・674・676・680・706・714（経由 `common/sec_data/rotce.py::build_all`、`common/sec_data/rotce.py::compute_ticker`）
- Python・保存済みファイル: `common/system_health.py` 行 101・1039・1110（経由 `common/system_health.py::check_b_score_history`）
- Python・保存済みファイル: `src/portfolio/snapshot.py` 行 119・142（経由 `src/portfolio/snapshot.py::main`）
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 31
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 61
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/extract_key_facts.py` 行 258・484・1262・1285（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 643（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 318・518・1060・1267（経由 `src/value/hypecore/hypecore.py::fetch_tanuki_iv`）
- Python・保存済みファイル: `src/value/tanuki_score/daily_pick.py` 行 27
- Python・保存済みファイル: `src/value/tanuki_valuation/remove_legacy_tanuki_score.py` 行 52・56・75（経由 `src/value/tanuki_valuation/remove_legacy_tanuki_score.py::main`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 57・119・124（経由 `src/value/tanuki_valuation/score_verifier.py::verify_ticker`）
- JS・HTML・保存済みファイル: `docs/market-monitor/market-pulse/index.html` 行 517
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 217
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 916
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 800
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 761・766
- ワークフロー・保存済みファイル: `.github/workflows/Score_Verifier.yml` 行 45
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_Score_Update.yml` 行 61・62
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` 行 132
- テスト・保存済みファイル: `tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py` 行 103・123・143・163（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py` 行 106・128・151・174（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_eps_cik_history_merge.py` 行 70・84・92（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_net_income_selection.py` 行 107・154（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_share_fallback.py` 行 88・122・149・190（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_split.py` 行 100・124・149・176（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_rotce.py` 行 64・70・71・81・82・94・96・101・110・111・119・120 ほか47（経由 `common/sec_data/rotce.py::compute_ticker`）
- テスト・保存済みファイル: `tests/test_score_verifier.py` 行 29・41・42・56（経由 `tests/test_score_verifier.py::_make_score_history`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_b_score_history`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_b_score_history`）
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（common/sec_data/rotce.py → docs/common/sec_data/rotce/{T}.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `alpha`(2)・`alpha_was_capped`(2)・`bs_adjustment`(2)・`calculation_date`(3)・`components`(7)・`dcf_components`(1)・`dcf_type`(1)・`fcf_base`(3)・`fcf_estimation`(2)・`fcf_outlier`(2)・`fcf_source`(0)・`fcf_ttm_end`(1)・`fcf_ttm_periods`(0)・`formula`(1)・`future_values`(2)・`growth`(5)・`growth_options`(1)・`growth_scenarios`(1)・`intrinsic_value_beta`(1)・`intrinsic_value_per_share`(6)・`intrinsic_value_pt`(0)・`intrinsic_value_rf`(1)・`intrinsic_value_rf_rate`(1)・`latest_period_missing`(2)・`maturity_profile`(1)・`phase1_growth`(1)・`rd_capitalization`(1)・`required_growth_rf_base`(1)・`required_growth_rf_live`(1)・`return_metrics`(1)・`rice`(3)・`rice_data_source`(0)・`risk_free_rate_live`(1)・`risk_free_rate_live_date`(1)・`risk_free_rate_live_source`(1)・`rpo_adjustment`(2)・`scenario_valuations`(2)・`sensitivity`(1)・`software_system_provisional`(0)・`software_system_reclassification`(0) ほか8
- Python・保存済みファイル: `common/market_data/fetcher.py` 行 1465・1660（経由 `common/market_data/fetcher.py::fetch_analyst_events`）
- Python・保存済みファイル: `common/market_data/reader.py` 行 311・332・356
- Python・保存済みファイル: `common/sec_data/fetcher.py` 行 15
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 57・324・513・692・706・708（経由 `common/sec_data/registration_validator.py::check_p6_cik_discontinuity_candidate`）
- Python・保存済みファイル: `common/system_health.py` 行 101・1039・1110（経由 `common/system_health.py::check_b_score_history`）
- Python・保存済みファイル: `src/portfolio/snapshot.py` 行 119・142（経由 `src/portfolio/snapshot.py::main`）
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 31
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 61
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/extract_key_facts.py` 行 258・484・1262・1285（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 643（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 320・393・430・489・518・521・1060・1267（経由 `common/market_data/reader.py::get_analyst_events`、`common/market_data/reader.py::get_earnings_history`、`common/market_data/reader.py::get_recommendations_history` ほか）
- Python・保存済みファイル: `src/value/tanuki_score/daily_pick.py` 行 27
- Python・保存済みファイル: `src/value/tanuki_valuation/remove_legacy_tanuki_score.py` 行 52・56・75（経由 `src/value/tanuki_valuation/remove_legacy_tanuki_score.py::main`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 57・119・124（経由 `src/value/tanuki_valuation/score_verifier.py::verify_ticker`）
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 217
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 916
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 800
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 531・702・704・761・763・766
- ワークフロー・保存済みファイル: `.github/workflows/Market_Data_Weekly_Update.yml` 行 71
- ワークフロー・保存済みファイル: `.github/workflows/Score_Verifier.yml` 行 45
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_Score_Update.yml` 行 61・62
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` 行 132
- テスト・保存済みファイル: `tests/test_eps_analyzer_q2q3_zero_quarterly_presence_guard.py` 行 103・123・143・163（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_eps_analyzer_q4_zero_quarterly_presence_guard.py` 行 106・128・151・174（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_eps_cik_history_merge.py` 行 70・84・92（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_net_income_selection.py` 行 107・154（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_share_fallback.py` 行 88・122・149・190（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_extract_key_facts_split.py` 行 100・124・149・176（経由 `src/value/adjusted_eps_analyzer/extract_key_facts.py::fetch_company_facts_with_legacy`）
- テスト・保存済みファイル: `tests/test_hypecore_market_data_switch.py` 行 156・171・182・199・207・212（経由 `common/market_data/reader.py::get_analyst_events`）
- テスト・保存済みファイル: `tests/test_market_data_fetcher.py` 行 871・873・887・889・896・898・917・919・928・929・931・938 ほか10（経由 `common/market_data/fetcher.py::fetch_analyst_events`）
- テスト・保存済みファイル: `tests/test_market_data_reader.py` 行 330・334・338・343・347・354・358・367・371・378・387・389 ほか10（経由 `common/market_data/reader.py::get_analyst_events`、`common/market_data/reader.py::get_earnings_history`、`common/market_data/reader.py::get_recommendations_history`）
- テスト・保存済みファイル: `tests/test_score_verifier.py` 行 29・41・42・56（経由 `tests/test_score_verifier.py::_make_score_history`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_b_score_history`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_b_score_history`）
- 画面（経由（src/portfolio/snapshot.py → docs/portfolio/data/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/latest.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `alpha`(5)・`alpha_was_capped`(2)・`breakeven_reason`(1)・`bs_adjustment`(6)・`calculation_date`(7)・`components`(27)・`dcf_components`(6)・`dcf_type`(3)・`dilution_comment`(1)・`dilution_severity`(1)・`dupont`(4)・`erp`(1)・`fcf_base`(6)・`fcf_estimation`(4)・`fcf_history`(5)・`fcf_margin_bear_mult_applied`(0)・`fcf_margin_note`(0)・`fcf_outlier`(4)・`fcf_source`(0)・`fcf_ttm_end`(1)・`fcf_ttm_periods`(0)・`financial_health`(7)・`formula`(2)・`forward_earnings_yield`(0)・`funda_score`(7)・`future_values`(4)・`growth`(12)・`growth_options`(5)・`growth_sanity`(6)・`growth_scenarios`(5)・`intrinsic_value_beta`(2)・`intrinsic_value_per_share`(16)・`intrinsic_value_pt`(1)・`intrinsic_value_rf`(2)・`intrinsic_value_rf_rate`(2)・`latest_period_missing`(4)・`matrix`(3)・`maturity_profile`(1)・`net_current_assets_ratio`(1)・`next_earnings_date`(6) ほか34
- Python・保存済みファイル: `browser_checks/check_tanuki_score_undetermined.py` 行 39・57・86（経由 `browser_checks/check_tanuki_score_undetermined.py::main`）
- Python・保存済みファイル: `browser_checks/check_valuation_chart_basis.py` 行 106・107・184・197・213（経由 `browser_checks/check_valuation_chart_basis.py::check_ticker`、`browser_checks/check_valuation_chart_basis.py::main`）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 66・80・166・203・226・254・361・364・367・368・369・370 ほか2（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/screening/dcf_validity_checker.py::check_ticker`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 138・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 82・104・111・202・230・247・295・356・397・480・483・499 ほか7（経由 `common/sec_data/registration_validator.py::_load_json`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1709・1841・2223・2288・2585・2782・2811・2822・3400・3449・3494・3542 ほか1（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/sec_data/report_consistency_check.py::_check_dcf_validation_failures`、`common/sec_data/report_consistency_check.py::_check_latest_period_missing` ほか）
- Python・保存済みファイル: `common/system_health.py` 行 86・137・788・1038・1040・1049・1110（経由 `common/sec_data/audit.py::audit_ticker`、`common/sec_data/registration_validator.py::_load_json`、`common/system_health.py::check_c_latest`）
- Python・保存済みファイル: `src/market/market_pulse/backfill_implb.py` 行 49・63（経由 `src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/market/market_pulse/backfill_implc.py` 行 39（経由 `src/market/market_pulse/market_calendar.py::earnings_events`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 1559・1586・1967・1969（経由 `src/market/market_pulse/market_calendar.py::earnings_events`、`src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/market/market_pulse/market_calendar.py` 行 104・107・192（経由 `src/market/market_pulse/market_calendar.py::earnings_events`）
- Python・保存済みファイル: `src/market/market_pulse/sector_rotation.py` 行 234・268（経由 `src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 181・254・1573・1574・1775・1811（経由 `src/tail/quarterly_review_generator.py::load_layer1_financials`、`src/tail/quarterly_review_generator.py::load_tanuki_valuation`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 76・161・194・207・337・349（経由 `src/tail/tail_dcf_bridge.py::_load_latest_valuation`、`src/tail/tail_dcf_bridge.py::_load_layer1_financials`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 334・518・1060・1267（経由 `src/value/hypecore/hypecore.py::fetch_tanuki_iv`）
- Python・保存済みファイル: `src/value/tanuki_score/daily_pick.py` 行 42・48・52・56・59・66・72・79・90・94・259・260 ほか8（経由 `src/value/tanuki_score/daily_pick.py::load_json`）
- Python・保存済みファイル: `src/value/tanuki_valuation/diag_iv_trace.py` 行 21・54・180（経由 `src/value/tanuki_valuation/diag_iv_trace.py::load`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 75・106・277・314（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 289・338・363
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 893・1004・1388
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/index.html` 行 251
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 643
- JS・HTML・保存済みファイル: `docs/value-monitor/extreme-fear/index.html` 行 114
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/detail.html` 行 408・652
- JS・HTML・保存済みファイル: `docs/value-monitor/stonks-silo/index.html` 行 446・619
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 451
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/index.html` 行 550
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 751・2721
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_VALUATION_Update.yml` 行 106
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 128・222・232（経由 `src/value/tanuki_score/daily_pick.py::load_json`、`src/value/tanuki_valuation/score_watcher.py::load_latest`）
- テスト・保存済みファイル: `tests/test_dcf_validity_checker.py` 行 46・64・77・86・95・107・118・128・133・143・155・165（経由 `common/screening/dcf_validity_checker.py::_load_json`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 251・291・299・357・412（経由 `src/tail/quarterly_review_generator.py::load_layer1_financials`、`src/tail/tail_dcf_bridge.py::_load_layer1_financials`）
- テスト・保存済みファイル: `tests/test_iv_formula.py` 行 50・57（経由 `tests/test_iv_formula.py::_load`）
- テスト・保存済みファイル: `tests/test_latest_period_guard.py` 行 107（経由 `common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_market_pulse_implc.py` 行 374・381（経由 `src/market/market_pulse/market_calendar.py::earnings_events`）
- テスト・保存済みファイル: `tests/test_market_pulse_sector_rotation.py` 行 117・168（経由 `src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- テスト・保存済みファイル: `tests/test_method_change.py` 行 78・88（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2867・2877・2902・2909・3037・3041
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82・168（経由 `common/sec_data/registration_validator.py::_load_json`、`common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_c_latest`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 96
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::_load_json`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_c_latest`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_tanuki_valuation`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_unchanged.py` 行 148
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/portfolio/index.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/report.txt`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 341・425・456・467（経由 `common/registration/register_ticker.py::step7_5_consistency_check`）
- Python・保存済みファイル: `common/screening/report_txt_parser.py` 行 245・248・265・269・270・275・280・299・305（経由 `common/screening/report_txt_parser.py::_all_tickers_with_report`、`common/screening/report_txt_parser.py::main`、`common/screening/report_txt_parser.py::parse_ticker_report`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2277・2280・2581・3293・3364・3382・3392・3400・3631・3632（経由 `common/sec_data/report_consistency_check.py::_read_report`、`common/sec_data/report_consistency_check.py::parse_args`、`common/sec_data/report_consistency_check.py::run_checks`）
- Python・保存済みファイル: `src/value/tanuki_score/daily_pick.py` 行 551・588（経由 `src/value/tanuki_score/daily_pick.py::main`）
- Python・保存済みファイル: `src/value/tanuki_valuation/diag_iv_trace.py` 行 14・24・27・54・111・180（経由 `src/value/tanuki_valuation/diag_iv_trace.py::load`、`src/value/tanuki_valuation/diag_iv_trace.py::run`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 2839
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 82・89（経由 `common/sec_data/report_consistency_check.py::run_checks`）
- テスト・保存済みファイル: `tests/test_collect_and_send_market_data_switch.py` 行 360
- テスト・保存済みファイル: `tests/test_register_ticker_preflight.py` 行 62・84・100・118（経由 `common/registration/register_ticker.py::step7_5_consistency_check`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 54・64・71・77・82・104・113・118・125・132・148（経由 `common/registration/register_ticker.py::step7_5_consistency_check`、`common/sec_data/report_consistency_check.py::run_checks`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 123・127・139・150・174・178・190・203・227・231・243・263 ほか42（経由 `tests/test_report_consistency_check.py::TestCheck23FyTagMismatch._make_ticker_dir`、`tests/test_report_consistency_check.py::TestCheck24FyeBoundaryCollision._make_ticker_dir`、`tests/test_report_consistency_check.py::TestCheck26BsFieldNoneTransition._make_ticker_dir` ほか）
- テスト・保存済みファイル: `tests/test_report_txt_parser.py` 行 25・35・36・37・43・49・50（経由 `common/screening/report_txt_parser.py::_all_tickers_with_report`、`tests/test_report_txt_parser.py::_make_report`）
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_score/daily_pick.py → docs/integrated-dashboard/daily_pick.json））: `docs/value-monitor/tanuki_score/index.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`（2件以上）
- 起動元: .github/workflows/Score_Verifier.yml
- Python・保存済みファイル: `common/system_health.py` 行 101・106・1039・1110（経由 `common/system_health.py::check_b_score_history`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 800
- ワークフロー・保存済みファイル: `.github/workflows/Score_Verifier.yml` 行 45
- テスト・保存済みファイル: `tests/test_score_verifier.py` 行 29・41・42・56（経由 `tests/test_score_verifier.py::_make_score_history`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_b_score_history`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_b_score_history`）

## 画面の項目

画面（`docs/value-monitor/tanuki_score/index.html`・`docs/value-monitor/tanuki_valuation/index.html`・`docs/value-monitor/tanuki_valuation/stock.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（型ごとに等間隔で12ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）

- `market_data`（`docs/value-monitor/tanuki_score/index.html` 行 899）: が
- `no_data`（`docs/value-monitor/tanuki_score/index.html` 行 1099）
- `risk_off_score`（`docs/value-monitor/tanuki_score/index.html` 行 423）: VIX指数
- `rule40_yoy_netmargin`（`docs/value-monitor/tanuki_score/index.html` 行 484）
- `stage_label`（`docs/value-monitor/tanuki_score/index.html` 行 494）
- `substage_label`（`docs/value-monitor/tanuki_score/index.html` 行 493）
- `substituted_by_annual`（`docs/value-monitor/tanuki_score/index.html` 行 590）: FCFは年次の値で評価（判定は通常どおり）
- `ma200_dev_md`（`docs/value-monitor/tanuki_valuation/index.html` 行 472・472・473・473）
- `expectation_score`（`docs/value-monitor/tanuki_valuation/stock.html` 行 590・593）
- `is_annual`（`docs/value-monitor/tanuki_valuation/stock.html` 行 345）
- `pv_phase1_rm`（`docs/value-monitor/tanuki_valuation/stock.html` 行 2412）
- `pv_phase2_rm`（`docs/value-monitor/tanuki_valuation/stock.html` 行 2412）
- `rice_na_reason`（`docs/value-monitor/tanuki_valuation/stock.html` 行 1414・1414・1421・1421・1428・1428）
- `stage_label`（`docs/value-monitor/tanuki_valuation/stock.html` 行 589・711・722）
- `substituted_by_annual`（`docs/value-monitor/tanuki_valuation/stock.html` 行 1089）

### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

`ai_assessment`・`all_categories`・`applicable`・`assumed_realization_years`・`avg_rev_growth_lagged`・`current_subgroup`・`deviation_from_current`・`dupont_bs_period`・`expectation`・`fcf_conversion_rate`・`fcf_estimated`・`fcf_margin_bear_mult_applied`・`fcf_margin_bear_multiplier`・`fcf_margin_note`・`fcf_outlier_excl_avg`・`formula_verification`・`forward_earnings_yield`・`fundamental`・`g_fundamental`・`growth_model_reason`・`growth_source`・`hype_label`・`hype_phase_label`・`hype_stage`・`hype_substage_label`・`invalid_reason`・`is_provisional`・`max_eps`・`max_eps_ttm_end`・`moat_fcf_margin_norm`・`moat_gross_margin_norm`・`moat_roic_norm`・`news`・`ni_ttm`・`phase1_growth_original`・`pre_rounding_comment`・`pre_rounding_score`・`pt_shares_consistency`・`rd_capitalization`・`realized_ratio`・`reclassify_recommended`・`recommended_subgroup`・`rice_score`・`rounded_by_policy`・`segment_ttm_applied`・`selection_reason_note`・`shares_yr_3ago_label`・`shares_yr_now_label`・`software_system_reclassification`・`upside_pct`・`v0_adjusted`・`v0_note`・`validated_at`

### `docs/value-monitor/tanuki_score/index.html`（93項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `asset_turnover` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 989・1259 |  |
| — | `available` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 507 |  |
| — | `base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 505 |  |
| — | `base_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 487 |  |
| — | `buyback_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 531 |  |
| — | `category` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 723 |  |
| — | `company` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 743 |  |
| — | `components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 488・489・508・509 |  |
| VIX指数 | `credit` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 423 |  |
| VIX指数 | `date` | この段階の書き手あり<br>`docs/integrated-dashboard/history.json`<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 424・425・872・873 |  |
| — | `detected` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 525 |  |
| — | `dilution_3yr_annual_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 521 |  |
| — | `dividend_yield` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 528 |  |
| DuPont信頼性LOW(${s.dupont?.reliability_rea | `dupont` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 519・932・936・936 |  |
| — | `excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 936 |  |
| △床 | `fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 363・364・1137・1142 |  |
| — | `fcf_base` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 487 |  |
| — | `fcf_floor_applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 489 |  |
| — | `fcf_history` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 524 |  |
| — | `fcf_outlier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 525 |  |
| — | `fear_greed` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 421 |  |
| — | `fields` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 527 |  |
| 判定なし（比較できる四半期が | `filled_components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1087 |  |
| 判定なし（比較できる四半期が | `filled_from_annual` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1082・1087・1106 |  |
| — | `financial_health` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 521・531・532 |  |
| — | `financial_leverage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 990 |  |
| 選出理由: | `funda_score` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 499・747 |  |
| 更新: （common/sec_data/rotce.py） | `generated_at` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json` | 712・1129 |  |
| — | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 520 |  |
| VIX指数 | `indicators` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 422 |  |
| 景気敏感() | `industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 523・1202・1214 |  |
| — | `invalid` | この段階の書き手あり<br>`docs/integrated-dashboard/history.json` | 823 |  |
| 選出理由: | `label` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 368・570・622・644 |  |
| — | `latest_period_missing` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 527・527 |  |
| 銘柄 | `length` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 418・482・527・584 |  |
| が | `market_data` | 書き手なし | 899 |  |
| — | `matrix` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 503・564 |  |
| — | `max_eps_per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 512 |  |
| — | `max_eps_reliability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 513 |  |
| 判定なし（比較できる四半期が | `n_quarters` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1085 |  |
| — | `net_current_assets_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 530 |  |
| — | `net_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 532 |  |
| — | `net_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 988・1260 |  |
| — | `no_data` | 書き手なし | 1099 |  |
| FCFは年次の値で評価（判定は通常どおり） | `note` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 590 |  |
| — | `payout_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 529 |  |
| — | `peg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 510・576・577・577 |  |
| 低PER(x) | `per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 376・376・509・575 |  |
| — | `per_is_forward` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 516 |  |
| — | `price_at_judgment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 880・880 |  |
| — | `ps` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 511・580・581・582 |  |
| — | `ptbv` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1052・1052・1053・1053 |  |
| 判定なし（比較できる四半期が | `ptbv_pctl` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1086・1086 |  |
| \n直近の四半期: | `quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1088 |  |
| — | `qx` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 566・567・568 |  |
| — | `qy` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 566・567・568 |  |
| — | `rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 520 |  |
| — | `reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1098 |  |
| — | `reliability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 972・1221 |  |
| 一過性要因の可能性あり | `reliability_reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 974・1227 |  |
| — | `report` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json` | 725 |  |
| — | `return_30d` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 827・827・827・881 |  |
| — | `return_60d` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 828・828・828・882 |  |
| — | `return_90d` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 829・830・830・883 |  |
| — | `rev_yoy` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 483 |  |
| — | `rice` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 505・505・507・522 |  |
| VIX指数 | `risk_off_score` | 書き手なし | 423 |  |
| — | `roe_10yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508 |  |
| DuPont ROEと実績ROEの乖離が | `roe_decomposed` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 966・967・971・981 |  |
| — | `roic_wacc_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 522 |  |
| — | `rotce` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1052・1053・1080・1081 |  |
| 判定なし（比較できる四半期が | `rotce_pctl` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1086 |  |
| — | `rule40` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 372・372・372・372 |  |
| — | `rule40_yoy_netmargin` | 書き手なし | 484 |  |
| — | `score` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json`<br>`docs/value-monitor/stonks-silo/data/results.json` | 421 |  |
| 選出理由: | `selection_reason` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 745 |  |
| — | `sell_reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 502 |  |
| P/TBV（対数） | `signal` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1078・1103・1104・1105 |  |
| \n目安: ${sig ? sig.label : r.signal_reaso | `signal_reason` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 1084・1105 |  |
| — | `stage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 379・485・1182・1182 |  |
| — | `stage_label` | 書き手なし | 494 |  |
| — | `structural_deficit` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 488 |  |
| — | `substage_label` | 書き手なし | 493 |  |
| FCFは年次の値で評価（判定は通常どおり） | `substituted_by_annual` | 書き手なし | 590 |  |
| — | `tanuki_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 501・822・872・879 |  |
| 銘柄なし | `ticker` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 367・367・467・613 |  |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 443・467・472・1049 |  |
| 銘柄なし | `timing` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json` | 370・370・615・637 |  |
| 選出理由: | `timing_score` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 500・747 |  |
| — | `upside_percent` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json` | 486 |  |
| VIX指数 | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 422・455・463 |  |
| — | `year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1136・1136 |  |
| — | `yoy_growth` | ほかの書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 490 |  |

### `docs/value-monitor/tanuki_valuation/index.html`（35項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 394・397・403・404 |  |
| — | `calculation_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 417・569 |  |
| — | `cf_conversion` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 416 |  |
| (値 or 0.0) | `components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 393・398・401・413 |  |
| — | `count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 419 |  |
| — | `current_price` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 393 |  |
| — | `date` | この段階の書き手あり<br>`docs/integrated-dashboard/history.json`<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 498・498 |  |
| — | `fcf_base` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 418 |  |
| — | `fcf_base_method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 418 |  |
| — | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 494・494 |  |
| — | `growth_options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 419 |  |
| — | `growth_rate` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 397 |  |
| — | `growth_scenarios` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397・420 |  |
| — | `intrinsic_value_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 395 |  |
| — | `intrinsic_value_per_share` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 394・394・558 |  |
| — | `ivps_beta` | 画面の中で作る値 | 488・488 |  |
| — | `ma200` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 413 |  |
| — | `ma200_dev_md` | 書き手なし | 472・472・473・473 |  |
| — | `method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 418 |  |
| (値 or 0.0) | `moat_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 398・579 |  |
| (値 or 0.0) | `moat_score_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 401・578 |  |
| — | `price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 486 |  |
| — | `primary` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397・420 |  |
| — | `q` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 415・496・496・496 |  |
| — | `rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397 |  |
| — | `rice` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 403・403・404・415 |  |
| — | `rice_per` | 画面の中で作る値 | 476・476・476 |  |
| — | `rice_per_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 404 |  |
| — | `scenario_valuations` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 394・397 |  |
| — | `source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 420 |  |
| — | `ticker` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 423・444・445・466 |  |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 538 |  |
| — | `type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 521 |  |
| — | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 402 |  |
| — | `wacc` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 402・495・495 |  |

### `docs/value-monitor/tanuki_valuation/stock.html`（241項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `CapEx` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 389・389・391・395 |  |
| — | `OCF` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 388・391・395 |  |
| — | `Revenue` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 395・399・403 |  |
| — | `action` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 850 |  |
| — | `adj_net_income` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 836 |  |
| — | `ai_concerns` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 879 |  |
| — | `alpha` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 802・2379 |  |
| — | `alpha_uncapped` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 804 |  |
| — | `alpha_was_capped` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 803 |  |
| — | `amount` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1769 |  |
| — | `analyst_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1015 |  |
| — | `analyst_rec_key` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1016 |  |
| — | `analyst_target_high` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1014 |  |
| — | `analyst_target_low` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1013 |  |
| — | `analyst_target_mean` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1012 |  |
| — | `analyst_target_median` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1011 |  |
| — | `analyst_vs_iv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1017 |  |
| — | `applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 833・861・925・933 |  |
| 資産回転率（Asset Turnover） | `asset_turnover` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1647 |  |
| — | `available` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 870・2742 |  |
| — | `base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 574・871・1277・1277 |  |
| — | `base_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・817 |  |
| （Ke ・高成長年 ＝ 表の中央） | `base_wacc` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1152 |  |
| （Ke ・高成長年 ＝ 表の中央） | `base_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1152 |  |
| — | `bear` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 872・1278・1278 |  |
| Bear乗数 | `bear_multiplier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1683 |  |
| — | `beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 971 |  |
| — | `bs_adjustment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 858・2381 |  |
| — | `bull` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 873・1276・1276 |  |
| Bull乗数 | `bull_multiplier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1684 |  |
| — | `calculation_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 797 |  |
| — | `cash` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 862 |  |
| — | `cash_and_equivalents` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1588 |  |
| — | `category` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 1768 |  |
| 参考: 実績CAGR等 | `clipped` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/SOUN/latest.json` | 1385・1695 |  |
| 最新年度 | `components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 795・804・809・810 |  |
| — | `conversion_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 837・934 |  |
| — | `count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 826 |  |
| — | `current_price` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 795・2509 |  |
| — | `cv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 821 |  |
| — | `damodaran_industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2232・2232 |  |
| — | `damodaran_year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2233・2233 |  |
| — | `date` | この段階の書き手あり<br>`docs/integrated-dashboard/history.json`<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 547・2507 |  |
| Koichi式 v6.2 | `dcf_components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・808・2376 |  |
| — | `dcf_type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 807 |  |
| — | `deficit_quality` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2754 |  |
| — | `delay_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1988 |  |
| — | `detected` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 847 |  |
| 最新年度 | `diluted_shares` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 892・999・2380 |  |
| — | `dilution_3yr_annual_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1568 |  |
| — | `dilution_comment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1571 |  |
| — | `dilution_severity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1570 |  |
| — | `divergence_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 838・926 |  |
| — | `divergence_warning` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 839 |  |
| — | `dupont` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1613 |  |
| — | `end` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 346・346・358・361 |  |
| — | `erp` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 659 |  |
| — | `error` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2728・2819 |  |
| — | `estimated_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・835 |  |
| — | `estimated_revenue` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1363 |  |
| — | `excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1618 |  |
| — | `expectation_score` | 書き手なし | 590・593 |  |
| — | `expected_return_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1317 |  |
| — | `fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2131・2141・2155・2155 |  |
| — | `fcf_2yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 819・819 |  |
| — | `fcf_5yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 818・818 |  |
| — | `fcf_base` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 508・815 |  |
| — | `fcf_base_method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 816 |  |
| — | `fcf_base_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 817・2768 |  |
| — | `fcf_estimation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・832・2766 |  |
| FCF CAGR 上限 | `fcf_growth_cap` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1682 |  |
| FCF CAGR 下限 | `fcf_growth_floor` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1681 |  |
| — | `fcf_history` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2129 |  |
| — | `fcf_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1988・2766 |  |
| — | `fcf_outlier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 829 |  |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `fcf_ttm_end` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2186 |  |
| — | `fcf_value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 855 |  |
| — | `fields` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 340・1086 |  |
| — | `financial_health` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1496・1564 |  |
| 財務レバレッジ（Financial Leverage） | `financial_leverage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1648 |  |
| — | `fiscal_year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 854・866 |  |
| Koichi式 v6.2 | `formula` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 798・1068・1068 |  |
| — | `found` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 851 |  |
| — | `from_peak` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 557 |  |
| 5年後 | `future_value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1320・1333 |  |
| — | `future_values` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 988 |  |
| 年 @ | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1365・1383・1509・1509 |  |
| 上限 | `growth_cap` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/SOUN/latest.json` | 1698・1698 |  |
| 上限 | `growth_floor` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/SOUN/latest.json` | 1698 |  |
| — | `growth_options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 824・2378 |  |
| — | `growth_rate` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 1276・1277・1278・1413 |  |
| — | `growth_sanity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 990 |  |
| — | `growth_scenarios` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 980 |  |
| — | `growth_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 977 |  |
| — | `high_growth_rate_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 887 |  |
| — | `high_growth_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 889 |  |
| — | `industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 894 |  |
| — | `insider_buy_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1027 |  |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `insider_latest_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1030・2186 |  |
| — | `insider_net_direction` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1029 |  |
| — | `insider_sell_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1028 |  |
| — | `intrinsic_value_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 791 |  |
| — | `intrinsic_value_per_share` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 788・1276・1277・1278 |  |
| — | `intrinsic_value_rf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 792 |  |
| — | `intrinsic_value_rf_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2257・2259 |  |
| — | `invalid` | この段階の書き手あり<br>`docs/integrated-dashboard/history.json` | 2504 |  |
| — | `is_annual` | 書き手なし | 345 |  |
| — | `item_name` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1768 |  |
| — | `items` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 853 |  |
| ERP（株式リスクプレミアム、現在値） | `label` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 385・686・1078・1305 |  |
| — | `latest_period_missing` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1086・1086・1088・1089 |  |
| — | `latest_revenue` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 997・1357・2769 |  |
| 直近件（新しい順） | `length` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 356・535・562・586 |  |
| — | `lifecycle` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 548 |  |
| — | `long_term_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 864 |  |
| — | `ma200` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 2510 |  |
| — | `ma200_dev` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 556 |  |
| — | `ma_addback_detected_but_not_applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 846 |  |
| — | `ma_addback_excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 845 |  |
| Rm基準（βなし %） | `market_return` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 973・1126・1498・1954 |  |
| — | `matrix` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 978 |  |
| — | `maturity_profile` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 885・889 |  |
| — | `method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 816・1787 |  |
| Phase1: 年（Moat Score由来${d.components?.mo | `moat_phase1_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1943 |  |
| Phase1: 年（Moat Score由来${d.components?.mo | `moat_score_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1943 |  |
| — | `model` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 881 |  |
| — | `name` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1369・1984 |  |
| — | `net_cash` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 859 |  |
| — | `net_cash_per_share` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 860・2381 |  |
| — | `net_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1496・1566 |  |
| 純利益率（Net Margin） | `net_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1646 |  |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `next_earnings_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1068・1068・1569・2186 |  |
| 減収（構造的変化・継続）: | `ni_ocf_divergence` | 画面の中で作る値 | 1113 |  |
| (' + count + '銘柄) | `note` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 840・849・874・1088 |  |
| 営業利益率 | `op_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1928 |  |
| 未実現の成長機会を「仮説セグメント」として定量化。TAM × 侵入率で期待売上、 | `options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1981 |  |
| — | `overall` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 880 |  |
| D&A比率 | `paper_bgcolor` | 画面の中で作る値 | 432・461・488 |  |
| — | `penetration` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1988 |  |
| — | `per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 993 |  |
| — | `per_adjusted` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 994 |  |
| — | `phase` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 550 |  |
| 年 @ | `phase1` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 889・1879・1879 |  |
| — | `phase1_growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2231 |  |
| — | `phase1_growth_auto_adjusted` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2205 |  |
| 年 @ | `phase2` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1885・1885 |  |
| D&A比率 | `plot_bgcolor` | 画面の中で作る値 | 433・462・489 |  |
| — | `price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 552 |  |
| price/IV 推移（表示期間中） | `price_iv_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 553・672・675・675 |  |
| — | `primary` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 981・982 |  |
| — | `probability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1988 |  |
| — | `pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1985 |  |
| — | `pv_fcf_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2421 |  |
| — | `pv_high` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 809 |  |
| — | `pv_phase1` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 811 |  |
| — | `pv_phase1_rm` | 書き手なし | 2412 |  |
| — | `pv_phase2` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 812 |  |
| — | `pv_phase2_rm` | 書き手なし | 2412 |  |
| — | `pv_terminal` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 810 |  |
| — | `pv_tv_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2412・2421 |  |
| — | `rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 982・1301・1306・1509 |  |
| — | `rate_is_sector_default` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 933 |  |
| — | `ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 820・821 |  |
| — | `raw_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 834 |  |
| 売上(推定=最新Revenue×weight) \| 構成比 \| 成長率シナリオ（ | `raw_weighted_growth` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/SOUN/latest.json` | 1381・1382・1697・1698 |  |
| 算出不能（reason: ${dp.reason \|\| | `reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1627・1627 |  |
| — | `recommendation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 551 |  |
| ターミナル成長率 | `recommended_g` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1384・1692・1693・2206 |  |
| — | `recommended_g_median` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2207 |  |
| — | `required_growth_rf_base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2290 |  |
| — | `required_growth_rf_live` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2291 |  |
| — | `return_metrics` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 989 |  |
| — | `rev_yoy` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 554 |  |
| 減収（構造的変化・継続）: | `revenue_decline` | 画面の中で作る値 | 1113 |  |
| — | `revenue_growth_pct` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2754 |  |
| — | `revenue_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 996 |  |
| 最新年度 | `revenue_ttm_end` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 998 |  |
| RICE_adj: R&D資本化想定（CapExのみ投資強度）参考値 | `rice` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 574・574・869・1414 |  |
| — | `rice_adj` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1415・1415・1415・1422 |  |
| — | `rice_na_reason` | 書き手なし | 1414・1414・1421・1421 |  |
| — | `rice_per_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1416・1416・1423・1423 |  |
| — | `risk_free_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 972・2257・2294 |  |
| 固定値 | `risk_free_rate_live` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 691・2292 |  |
| 固定値 | `risk_free_rate_live_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 689・690・2260・2261 |  |
| — | `risk_free_rate_live_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 688・689・2260 |  |
| セクター除外 | `roe_10yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2746 |  |
| ROE（分解値） | `roe_decomposed` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1636・1636・1636・1649 |  |
| — | `roe_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 891 |  |
| — | `rpo` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 966 |  |
| 非連続RPO | `rpo_adjustment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1927・1928 |  |
| 非連続RPO | `rpo_incremental` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1927 |  |
| — | `rpo_pv` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 967・2377 |  |
| — | `rule` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 848 |  |
| — | `rule40` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 555 |  |
| Q異常値 | `runway` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2751 |  |
| Q異常値 | `runway_months` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2751 |  |
| — | `sbc_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1567 |  |
| — | `scenario_bear` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 2511 |  |
| — | `scenario_bull` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 2512 |  |
| — | `scenario_valuations` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 987・1492 |  |
| — | `score_comment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1078・1078 |  |
| — | `sector` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 893 |  |
| — | `segment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 985 |  |
| DCF採用値（クリップ後）: % | `segment_growth_quarter` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/SOUN/latest.json` | 1389・1389 |  |
| 売上(推定=最新Revenue×weight) \| 構成比 \| 成長率シナリオ（ | `segment_growth_source` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/SOUN/latest.json` | 1380 |  |
| — | `segments` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1355 |  |
| — | `sensitivity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 975 |  |
| →年 | `shares_yr_3ago` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1600 |  |
| →年 | `shares_yr_now` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1600 |  |
| — | `short_term_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 865 |  |
| — | `short_term_investments` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 863 |  |
| — | `signals` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2200 |  |
| — | `source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 981 |  |
| — | `stage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 549・588・711・715 |  |
| — | `stage_label` | 書き手なし | 589・711・722 |  |
| — | `substituted_by_annual` | 書き手なし | 1089 |  |
| — | `tam` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1988 |  |
| 判定不能（株価なし） | `tanuki_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1050・1050・1078 |  |
| — | `terminal_growth_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 890 |  |
| — | `ticker` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/integrated-dashboard/history.json` | 2618・2632・2642・2660 |  |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 2718・2727 |  |
| — | `total_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1588 |  |
| — | `total_pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 825・2378 |  |
| — | `total_transient_amount` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 852 |  |
| — | `transient_evidence` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 851・852・853 |  |
| — | `type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 2632・2666 |  |
| — | `upside_percent` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json` | 575・796・2735 |  |
| — | `upside_percent_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 794 |  |
| — | `upside_percent_rf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 793 |  |
| Koichi式 v6.2 | `v0` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・1861・1900・2191 |  |
| Koichi式 v6.2 | `v0_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・2376 |  |
| — | `val` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 362 |  |
| — | `validation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 878 |  |
| 、固定値 | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 970・2262・2733 |  |
| — | `verdict` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2199・2229 |  |
| Rm基準（βなし %） | `wacc` | この段階の書き手あり<br>`docs/integrated-dashboard/daily_pick.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 969・1126・1498・1954 |  |
| — | `wacc_values` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 976 |  |
| — | `warnings` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 875・2202 |  |
| セグメント別売上構成（推定） | `weight` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1362・1383 |  |
| — | `year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2149・2159・2159 |  |
| 年 @ | `years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 889・1879・1885 |  |

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | TANUKIか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | `discover/stonks-silo/src/financial_trend_calculator.py:448` |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22`<br>`src/value/adjusted_eps_analyzer/pipeline.py:814` |
| `market_data_violations_log` | いいえ（後の段階） | — | `common/market_data/fetcher.py:315` |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。TANUKIのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | TANUKIを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:102`<br>`common/sec_data/q4_implied.py:176`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:773`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:916`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:944` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:805`<br>`common/sec_data/layer3_builder.py:1041`<br>`common/sec_data/normalizer.py:169`<br>`common/sec_data/normalizer.py:419`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:655` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1632`<br>`common/sec_data/report_consistency_check.py:1719`<br>`common/system_health.py:955`<br>`discover/stonks-silo/src/financial_as_of.py:70`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:405`<br>`src/value/hypecore/hypecore.py:284` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | はい |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 6 | `docs/portfolio/index.html:673`<br>`docs/value-monitor/extreme-fear/index.html:143`<br>`docs/value-monitor/extreme-fear/index.html:175`<br>`docs/value-monitor/extreme-fear/index.html:184`<br>`docs/value-monitor/tanuki_score/index.html:596`<br>`docs/value-monitor/tanuki_score/index.html:1184`<br>`docs/value-monitor/tanuki_score/index.html:1413`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:623`<br>`src/value/tanuki_valuation/pipeline.py:625`<br>`src/value/tanuki_valuation/pipeline.py:627`<br>`src/value/tanuki_valuation/pipeline.py:866`<br>`src/value/tanuki_valuation/pipeline.py:879`<br>`src/value/tanuki_valuation/pipeline.py:899`<br>`src/value/tanuki_valuation/pipeline.py:901`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:971`<br>`src/value/tanuki_valuation/pipeline.py:1362`<br>`src/value/tanuki_valuation/pipeline.py:1389`<br>`src/value/tanuki_valuation/pipeline.py:1394`<br>`src/value/tanuki_valuation/pipeline.py:1440`<br>`src/value/tanuki_valuation/pipeline.py:1745`<br>`src/value/tanuki_valuation/pipeline.py:1923` | はい |
| 成長率の上限・下限 | 5 | `scripts/analysis/macro_events_sp500_asof_repair.py:32`<br>`src/market/macro_pulse/05_main.py:204`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:121`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:122`<br>`src/value/tanuki_valuation/growth_model_audit.py:62`<br>`src/value/tanuki_valuation/growth_model_audit.py:122`<br>`src/value/tanuki_valuation/growth_sanity.py:582` | はい |
| β・WACCの作り方 | 4 | `docs/value-monitor/admin.html:1236`<br>`docs/value-monitor/admin.html:1838`<br>`docs/value-monitor/admin.html:1860`<br>`docs/value-monitor/admin.html:1898`<br>`docs/value-monitor/admin.html:1915`<br>`docs/value-monitor/admin.html:1919`<br>`docs/value-monitor/admin.html:1972`<br>`src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | はい |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:185`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | はい |

## 本番外のコード（src/value/tanuki_valuation）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 名前を挙げたもの（`diag_iv_trace.py`・`growth_model_audit.py`・`remove_legacy_tanuki_score.py`・`pipeline_log.txt`）と、段階のフォルダの.pyのうちワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの

### `src/value/tanuki_valuation`（4ファイル・1,485行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 残す／消す／移す |
|---|---|---|---|---|---|---|
| `diag_iv_trace.py` | 180 | — | — | — | — |  |
| `growth_model_audit.py` | 152 | — | — | — | — |  |
| `pipeline_log.txt` | 1,078 | — | — | — | — |  |
| `remove_legacy_tanuki_score.py` | 75 | — | — | — | — |  |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 1件
  - `src/value/tanuki_valuation/remove_legacy_tanuki_score.py:45` clean_history_file → `{path}`
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 2件
  - `src/value/tanuki_valuation/data_fetcher.py:255` TTMReader._load → `<?>`
  - `src/value/tanuki_valuation/data_fetcher.py:506` _load_beta_config → `<?>`
