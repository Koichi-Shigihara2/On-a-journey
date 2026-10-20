# TANUKI VALUATION・TANUKI Scoreの出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`tanuki_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`docs/value-monitor/tanuki_valuation/`・`docs/value-monitor/tanuki_score/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-20）。画面の区画はKoichiさんの判断で「今日の特選銘柄」だけ消し、ほかは残す。出力・キー・本番外のコードはチャット側の判断。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 11（ファイル 9・人が見るもの 2）／読み手0件 0／テストだけ 0／1件 2／2件以上 7／規則の二重 9（検出式 10種のうち。TANUKIのファイルを含むもの 4）

**画面の項目**: 画面が読むキー 372（この段階の書き手あり 320・ほかの書き手あり 42・画面の中で作る値 8・**書き手なし 2**）／書き手があるのに誰も読まないキー **29**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 2件／解決できた読み込み 31件（6.1%）→ 当たらない

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
| 2件以上 | `config/beta_config.json`<br>（1ファイル） | 銘柄ごとのβ（市場との連動の強さ）の上書きの設定。WACCの計算に使う | `beta_fetcher.py:save_config` | 12・テスト13 | index.html（経由）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/admin.html（直接）<br>value-monitor/extreme-fear/index.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: TANUKI VALUATIONの画面 | AS-IS-013 | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/tickers.json`<br>（1ファイル） | 一覧のページ用の、全銘柄の理論株価・判定の要約 | `pipeline.py:TanukiValuationPipeline._save_tickers_index` | 19・テスト10 | common/ticker-links.js（直接）<br>index.html（直接）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（直接）<br>value-monitor/extreme-fear/index.html（直接）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/hypecore/index.html（直接）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: TANUKI VALUATIONの画面・TANUKI SCOREの画面 | — | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>（96ファイル） | 銘柄ごとの理論株価・上振れ余地・成長率などの推移（実行ごとに1行） | `pipeline.py:TanukiValuationPipeline._save_result` | 11 | portfolio/index.html（直接）<br>portfolio/tail/index.html（直接）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: 保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | — | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>（28313ファイル） | 銘柄ごとの、実行の日時ごとの評価の全体のスナップショット | `pipeline.py:TanukiValuationPipeline._save_result` | 20・テスト12 | portfolio/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/admin.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/adjusted_eps_analyzer/tickers.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-047・AS-IS-116・AS-IS-258・AS-IS-297・AS-IS-298・AS-IS-299・AS-IS-438・AS-IS-439・AS-IS-506 | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/latest.json`<br>（96ファイル） | 銘柄ごとの最新の評価の全体（理論株価・判定・DCF・RICE・各指標）。個別ページが読む | `pipeline.py:TanukiValuationPipeline._save_result`<br>`pipeline.py:TanukiValuationPipeline._save_result` | 27・テスト20 | market-monitor/market-pulse/index.html（経由）<br>portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/extreme-fear/index.html（直接）<br>value-monitor/hypecore/detail.html（直接）<br>value-monitor/stonks-silo/index.html（直接）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（直接）<br>value-monitor/tanuki_valuation/stock.html（直接） | 効く: HypeCore（TANUKI のtiming・判定の入力）・保有銘柄の監視（TANUKI TAIL）・TANUKI VALUATIONの画面・TANUKI SCOREの画面 | AS-IS-056・AS-IS-071 | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/report.txt`<br>（96ファイル） | 銘柄ごとの評価の計算の説明（人が読む文の形式のレポート） | `pipeline.py:TanukiValuationPipeline._save_report` | 4・テスト6 | value-monitor/tanuki_valuation/stock.html（直接） | 効く: TANUKI VALUATIONの画面 | AS-IS-045・AS-IS-056・AS-IS-057・AS-IS-058・AS-IS-060 | 残す（2026-10-20） |
| 2件以上 | `docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`<br>（98ファイル） | 銘柄ごとのTANUKI SCORE（funda等）・判定時の株価と、その後30・60・90日の値動きの推移 | `pipeline.py:TanukiValuationPipeline._save_result`<br>`score_verifier.py:verify_ticker` | 3・テスト3 | value-monitor/tanuki_score/index.html（直接） | 効く: TANUKI SCOREの画面 | — | 残す（2026-10-20） |

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
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 357
- Python・保存済みファイル: `common/system_health.py` 行 205・296・788・1045・1049・1110（経由 `common/sec_data/registration_validator.py::_load_json`、`common/system_health.py::check_h_config`）
- Python・保存済みファイル: `src/value/tanuki_valuation/beta_fetcher.py` 行 40・71・155・288・319（経由 `src/value/tanuki_valuation/beta_fetcher.py::load_config`）
- Python・保存済みファイル: `src/value/tanuki_valuation/data_fetcher.py` 行 483・489・503・509・558・561・874・1153（経由 `src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher._determine_beta`、`src/value/tanuki_valuation/data_fetcher.py::_load_beta_config`、`src/value/tanuki_valuation/data_fetcher.py::resolve_beta_config_path`）
- Python・保存済みファイル: `src/value/tanuki_valuation/growth_sanity.py` 行 209
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 427・490・1006・1323・1855・1856・3803・3805・3861（経由 `src/value/tanuki_valuation/data_fetcher.py::TanukiDataFetcher._determine_beta`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_beta_sector`）
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
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/extreme-fear/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/hypecore/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/value-monitor/tanuki_valuation/data/tickers.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `count`(6)・`tickers`(22)・`updated_at`(1)
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 47
- Python・保存済みファイル: `common/market_data/fetcher.py` 行 1502・1581・1645（経由 `common/market_data/fetcher.py::get_monitor_tickers`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 59・359・468（経由 `common/sec_data/audit.py::get_registered_tickers`）
- Python・保存済みファイル: `common/system_health.py` 行 159・188・355・404・1032・1041・1042・1046・1110（経由 `common/sec_data/audit.py::get_registered_tickers`、`common/system_health.py::_check_i`、`common/system_health.py::check_d_actions` ほか）
- Python・保存済みファイル: `scripts/analysis/market_pulse_redesign.py` 行 60・233・478（経由 `scripts/analysis/market_pulse_redesign.py::load_inputs`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/company_metadata.py` 行 38
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 655（経由 `src/value/adjusted_eps_analyzer/company_metadata.py::get_company_metadata`）
- Python・保存済みファイル: `src/value/tanuki_valuation/beta_fetcher.py` 行 86・279・319（経由 `src/value/tanuki_valuation/beta_fetcher.py::get_registered_tickers`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 439・490・592・810・991・1069・1144・1323・1530・1615・2952・3529 ほか1（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._inject_ttm_for_general_segment`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 52・264・314（経由 `src/value/tanuki_valuation/score_watcher.py::get_tickers`）
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 16・18・28・34
- JS・HTML・保存済みファイル: `docs/index.html` 行 246
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/tickers.html` 行 245
- JS・HTML・保存済みファイル: `docs/value-monitor/extreme-fear/index.html` 行 113
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/index.html` 行 214
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 417
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/index.html` 行 535
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 2715
- ワークフロー・保存済みファイル: `.github/workflows/Stonks_Silo_Update.yml` 行 88
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96・102・108（経由 `common/sec_data/audit.py::get_registered_tickers`）
- テスト・保存済みファイル: `tests/test_eps_run_status.py` 行 161・220・232・242・248・255・264（経由 `common/system_health.py::_check_i`）
- テスト・保存済みファイル: `tests/test_latest_period_guard.py` 行 68・72・80・83・90・91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 165・211・233・253・266・285・289・294・299・304・309・328 ほか83（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`、`tests/test_pipeline_logic.py::_write_stonks_json`）
- テスト・保存済みファイル: `tests/test_rice_report_txt_none_display.py` 行 39・51・63・86・102（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/sec_data/audit.py::get_registered_tickers`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/sec_data/audit.py::get_registered_tickers`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 181・193・194・216・220（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/history.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/history.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 57
- Python・保存済みファイル: `src/portfolio/snapshot.py` 行 119・142（経由 `src/portfolio/snapshot.py::main`）
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 31
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 57・119・124（経由 `src/value/tanuki_valuation/score_verifier.py::verify_ticker`）
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 217
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 916
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 710
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 761
- ワークフロー・保存済みファイル: `.github/workflows/Score_Verifier.yml` 行 45
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_Score_Update.yml` 行 46
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` 行 132

### `docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `alpha`(1)・`alpha_was_capped`(1)・`bs_adjustment`(1)・`calculation_date`(3)・`components`(6)・`dcf_components`(1)・`dcf_type`(1)・`fcf_base`(2)・`fcf_estimation`(1)・`fcf_outlier`(2)・`fcf_source`(0)・`fcf_ttm_end`(1)・`fcf_ttm_periods`(0)・`formula`(1)・`future_values`(2)・`growth`(4)・`growth_options`(1)・`growth_scenarios`(1)・`intrinsic_value_beta`(1)・`intrinsic_value_per_share`(5)・`intrinsic_value_pt`(0)・`intrinsic_value_rf`(1)・`intrinsic_value_rf_rate`(1)・`latest_period_missing`(2)・`maturity_profile`(1)・`phase1_growth`(1)・`rd_capitalization`(0)・`required_growth_rf_base`(1)・`required_growth_rf_live`(1)・`return_metrics`(1)・`rice`(2)・`rice_data_source`(0)・`risk_free_rate_live`(1)・`risk_free_rate_live_date`(1)・`risk_free_rate_live_source`(1)・`rpo_adjustment`(1)・`scenario_valuations`(2)・`sensitivity`(1)・`software_system_provisional`(0)・`software_system_reclassification`(0) ほか8
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
- Python・保存済みファイル: `src/value/tanuki_valuation/score_verifier.py` 行 57・119・124（経由 `src/value/tanuki_valuation/score_verifier.py::verify_ticker`）
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 217
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 916
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 710
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 531・702・704・761・763・766
- ワークフロー・保存済みファイル: `.github/workflows/Market_Data_Weekly_Update.yml` 行 71
- ワークフロー・保存済みファイル: `.github/workflows/Score_Verifier.yml` 行 45
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_Score_Update.yml` 行 46
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
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/admin.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/summary.json））: `docs/value-monitor/adjusted_eps_analyzer/tickers.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/latest.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `alpha`(3)・`alpha_was_capped`(1)・`breakeven_reason`(1)・`bs_adjustment`(4)・`calculation_date`(7)・`components`(24)・`dcf_components`(5)・`dcf_type`(2)・`dilution_comment`(1)・`dilution_severity`(1)・`dupont`(4)・`erp`(1)・`fcf_base`(5)・`fcf_estimation`(3)・`fcf_history`(5)・`fcf_margin_bear_mult_applied`(0)・`fcf_margin_note`(0)・`fcf_outlier`(4)・`fcf_source`(0)・`fcf_ttm_end`(1)・`fcf_ttm_periods`(0)・`financial_health`(7)・`formula`(2)・`funda_score`(6)・`future_values`(4)・`growth`(11)・`growth_options`(4)・`growth_sanity`(6)・`growth_scenarios`(5)・`intrinsic_value_beta`(2)・`intrinsic_value_per_share`(13)・`intrinsic_value_rf`(2)・`intrinsic_value_rf_rate`(2)・`latest_period_missing`(4)・`matrix`(3)・`maturity_profile`(1)・`net_current_assets_ratio`(1)・`next_earnings_date`(6)・`phase1_growth`(2)・`phase1_growth_auto_adjusted`(3) ほか32
- Python・保存済みファイル: `browser_checks/check_tanuki_score_undetermined.py` 行 39・57・86（経由 `browser_checks/check_tanuki_score_undetermined.py::main`）
- Python・保存済みファイル: `browser_checks/check_valuation_chart_basis.py` 行 106・107・184・197・213（経由 `browser_checks/check_valuation_chart_basis.py::check_ticker`、`browser_checks/check_valuation_chart_basis.py::main`）
- Python・保存済みファイル: `common/screening/dcf_validity_checker.py` 行 66・80・166・203・226・254・361・364・367・368・369・370 ほか2（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/screening/dcf_validity_checker.py::check_ticker`）
- Python・保存済みファイル: `common/sec_data/audit.py` 行 138・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/registration_validator.py` 行 82・104・111・202・230・247・295・356・397・480・483・499 ほか7（経由 `common/sec_data/registration_validator.py::_load_json`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1710・1896・2278・2343・2640・2837・2866・2877・3455・3504・3549・3597 ほか1（経由 `common/screening/dcf_validity_checker.py::_load_json`、`common/sec_data/report_consistency_check.py::_check_dcf_validation_failures`、`common/sec_data/report_consistency_check.py::_check_latest_period_missing` ほか）
- Python・保存済みファイル: `common/system_health.py` 行 86・137・788・1038・1040・1049・1110（経由 `common/sec_data/audit.py::audit_ticker`、`common/sec_data/registration_validator.py::_load_json`、`common/system_health.py::check_c_latest`）
- Python・保存済みファイル: `src/market/market_pulse/backfill_implb.py` 行 49・63（経由 `src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/market/market_pulse/backfill_implc.py` 行 39（経由 `src/market/market_pulse/market_calendar.py::earnings_events`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 1559・1586・1967・1969（経由 `src/market/market_pulse/market_calendar.py::earnings_events`、`src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/market/market_pulse/market_calendar.py` 行 104・107・192（経由 `src/market/market_pulse/market_calendar.py::earnings_events`）
- Python・保存済みファイル: `src/market/market_pulse/sector_rotation.py` 行 234・268（経由 `src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 181・254・1573・1574・1775・1811（経由 `src/tail/quarterly_review_generator.py::load_layer1_financials`、`src/tail/quarterly_review_generator.py::load_tanuki_valuation`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 76・161・194・207・337・349（経由 `src/tail/tail_dcf_bridge.py::_load_latest_valuation`、`src/tail/tail_dcf_bridge.py::_load_layer1_financials`）
- Python・保存済みファイル: `src/value/hypecore/hypecore.py` 行 334・518・1060・1267（経由 `src/value/hypecore/hypecore.py::fetch_tanuki_iv`）
- Python・保存済みファイル: `src/value/tanuki_valuation/score_watcher.py` 行 75・106・277・314（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 289・338・363
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 893・1004・1388
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/index.html` 行 251
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 643
- JS・HTML・保存済みファイル: `docs/value-monitor/extreme-fear/index.html` 行 114
- JS・HTML・保存済みファイル: `docs/value-monitor/hypecore/detail.html` 行 408・652
- JS・HTML・保存済みファイル: `docs/value-monitor/stonks-silo/index.html` 行 446・619
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 426
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/index.html` 行 550
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 751・2721
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_VALUATION_Update.yml` 行 106
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_close_missing_price.py` 行 127（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- テスト・保存済みファイル: `tests/test_dcf_validity_checker.py` 行 46・64・77・86・95・107・118・128・133・143・155・165（経由 `common/screening/dcf_validity_checker.py::_load_json`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_gate2_phase3b1_reader_integration.py` 行 251・291・299・357・412（経由 `src/tail/quarterly_review_generator.py::load_layer1_financials`、`src/tail/tail_dcf_bridge.py::_load_layer1_financials`）
- テスト・保存済みファイル: `tests/test_iv_formula.py` 行 50・57（経由 `tests/test_iv_formula.py::_load`）
- テスト・保存済みファイル: `tests/test_latest_period_guard.py` 行 107（経由 `common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_market_pulse_implc.py` 行 374・381（経由 `src/market/market_pulse/market_calendar.py::earnings_events`）
- テスト・保存済みファイル: `tests/test_market_pulse_sector_rotation.py` 行 117・168（経由 `src/market/market_pulse/sector_rotation.py::_tanuki_score`）
- テスト・保存済みファイル: `tests/test_method_change.py` 行 78・88（経由 `src/value/tanuki_valuation/score_watcher.py::load_latest`）
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 88・99・106・115・123・131・144・153（経由 `tests/test_output_inventory_check.py::_tanuki_inventory`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 2867・2877・2902・2909・3037・3041
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82・168（経由 `common/sec_data/registration_validator.py::_load_json`、`common/sec_data/report_consistency_check.py::_check_latest_period_missing`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_c_latest`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 96
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/sec_data/registration_validator.py::_load_json`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_c_latest`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_tanuki_valuation`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_unchanged.py` 行 148
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`

### `docs/value-monitor/tanuki_valuation/data/{T}/report.txt`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- Python・保存済みファイル: `common/registration/register_ticker.py` 行 341・425・456・467（経由 `common/registration/register_ticker.py::step7_5_consistency_check`）
- Python・保存済みファイル: `common/screening/report_txt_parser.py` 行 245・248・265・269・270・275・280・299・305（経由 `common/screening/report_txt_parser.py::_all_tickers_with_report`、`common/screening/report_txt_parser.py::main`、`common/screening/report_txt_parser.py::parse_ticker_report`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2332・2335・2636・3348・3419・3437・3447・3455・3700・3701（経由 `common/sec_data/report_consistency_check.py::_read_report`、`common/sec_data/report_consistency_check.py::parse_args`、`common/sec_data/report_consistency_check.py::run_checks`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_valuation/stock.html` 行 2839
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 82・89（経由 `common/sec_data/report_consistency_check.py::run_checks`）
- テスト・保存済みファイル: `tests/test_collect_and_send_market_data_switch.py` 行 360
- テスト・保存済みファイル: `tests/test_register_ticker_preflight.py` 行 62・84・100・118（経由 `common/registration/register_ticker.py::step7_5_consistency_check`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 54・64・71・77・82・104・113・118・125・132・148（経由 `common/registration/register_ticker.py::step7_5_consistency_check`、`common/sec_data/report_consistency_check.py::run_checks`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 123・127・139・150・174・178・190・203・227・231・243・263 ほか42（経由 `tests/test_report_consistency_check.py::TestCheck23FyTagMismatch._make_ticker_dir`、`tests/test_report_consistency_check.py::TestCheck24FyeBoundaryCollision._make_ticker_dir`、`tests/test_report_consistency_check.py::TestCheck26BsFieldNoneTransition._make_ticker_dir` ほか）
- テスト・保存済みファイル: `tests/test_report_txt_parser.py` 行 25・35・36・37・43・49・50（経由 `common/screening/report_txt_parser.py::_all_tickers_with_report`、`tests/test_report_txt_parser.py::_make_report`）

### `docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`（2件以上）
- 起動元: .github/workflows/Score_Verifier.yml
- Python・保存済みファイル: `common/system_health.py` 行 101・106・1039・1110（経由 `common/system_health.py::check_b_score_history`）
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 710
- ワークフロー・保存済みファイル: `.github/workflows/Score_Verifier.yml` 行 45
- テスト・保存済みファイル: `tests/test_score_verifier.py` 行 29・41・42・56（経由 `tests/test_score_verifier.py::_make_score_history`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_b_score_history`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_b_score_history`）

## 画面の項目

画面（`docs/value-monitor/tanuki_score/index.html`・`docs/value-monitor/tanuki_valuation/index.html`・`docs/value-monitor/tanuki_valuation/stock.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（型ごとに等間隔で12ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）

- `market_data`（`docs/value-monitor/tanuki_score/index.html` 行 809）: が
- `rice_na_reason`（`docs/value-monitor/tanuki_valuation/stock.html` 行 1414・1414・1421・1421・1428・1428）

### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `applicable`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `assumed_realization_years`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `avg_rev_growth_lagged`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `current_subgroup`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `deviation_from_current`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `dupont_bs_period`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `fallback`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: 名前が一般的で、grep で読み手を決めきれない
- `formula_verification`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`、`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: validator の検証の結果（validation.checks。overall の判定に入る）
- `g_fundamental`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: report_consistency_check.py が名前の組（_REC_G_CANDIDATE_KEYS）で読む（一覧の読みの形では拾えない）
- `intrinsic_value_pt`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `invalid_reason`（`docs/value-monitor/tanuki_valuation/data/{T}/history.json`、`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `is_provisional`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `labels`（`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: 名前が一般的で、grep で読み手を決めきれない
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
- `upside_pct`（`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る
- `v0_adjusted`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 残す: 書き手が grep で見つからない（今の pipeline は書かず、過去の history/ のスナップショットにだけ残る）
- `v0_note`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`、`docs/value-monitor/tanuki_valuation/data/{T}/latest.json`）: 残す: テストが参照する
- `validated_at`（`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`）: 書き込みをやめた（2026-10-20）。latest.json からは 2026-10-21 の実行で消えた。過去の history/ のスナップショット・score_history.json の古いエントリには残る

### `docs/value-monitor/tanuki_score/index.html`（92項目）

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
| — | `fields` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 502 | 残す（2026-10-20） |
| 判定なし（比較できる四半期が | `filled_components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 997 | 残す（2026-10-20） |
| 判定なし（比較できる四半期が | `filled_from_annual` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json` | 992・997・1016 | 残す（2026-10-20） |
| — | `financial_health` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 496・506・507 | 残す（2026-10-20） |
| — | `financial_leverage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 900 | 残す（2026-10-20） |
| — | `funda_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 474 | 残す（2026-10-20） |
| 更新: （common/sec_data/rotce.py） | `generated_at` | ほかの書き手あり<br>`docs/common/sec_data/rotce/_summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 1039 | 残す（2026-10-20） |
| — | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 495 | 残す（2026-10-20） |
| VIX指数 | `indicators` | ほかの書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 397 | 残す（2026-10-20） |
| 景気敏感() | `industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 498・1112・1124 | 残す（2026-10-20） |
| — | `invalid` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 733 | 残す（2026-10-20） |
| \n目安: ${sig ? sig.label : r.signal_reaso | `label` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 343・545・597・619 | 残す（2026-10-20） |
| — | `latest_period_missing` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 502・502 | 残す（2026-10-20） |
| 銘柄 | `length` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 393・457・502・559 | 残す（2026-10-20） |
| が | `market_data` | 書き手なし | 809 | 残す（2026-10-20） |
| — | `matrix` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 478・539 | 残す（2026-10-20） |
| — | `max_eps_per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 487 | 残す（2026-10-20） |
| — | `max_eps_reliability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 488 | 残す（2026-10-20） |
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
| FCFは年次の値で評価（判定は通常どおり） | `substituted_by_annual` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 565 | 残す（2026-10-20） |
| — | `tanuki_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 476・732・782・789 | 残す（2026-10-20） |
| 銘柄なし | `ticker` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 342・342・442・588 | 残す（2026-10-20） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 418・442・447・959 | 残す（2026-10-20） |
| — | `timing_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 475 | 残す（2026-10-20） |
| — | `upside_percent` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 461 | 残す（2026-10-20） |
| VIX指数 | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 397・430・438 | 残す（2026-10-20） |
| — | `year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1046・1046 | 残す（2026-10-20） |
| — | `yoy_growth` | ほかの書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 465 | 残す（2026-10-20） |

### `docs/value-monitor/tanuki_valuation/index.html`（35項目）

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
| — | `type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 521 | 残す（2026-10-20） |
| — | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 402 | 残す（2026-10-20） |
| — | `wacc` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 402・495・495 | 残す（2026-10-20） |

### `docs/value-monitor/tanuki_valuation/stock.html`（245項目）

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
| — | `alpha` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 802・2379 | 残す（2026-10-20） |
| — | `alpha_uncapped` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 804 | 残す（2026-10-20） |
| — | `alpha_was_capped` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 803 | 残す（2026-10-20） |
| — | `amount` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1769 | 残す（2026-10-20） |
| — | `analyst_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1015 | 残す（2026-10-20） |
| — | `analyst_rec_key` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1016 | 残す（2026-10-20） |
| — | `analyst_target_high` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1014 | 残す（2026-10-20） |
| — | `analyst_target_low` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1013 | 残す（2026-10-20） |
| — | `analyst_target_mean` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1012 | 残す（2026-10-20） |
| — | `analyst_target_median` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1011 | 残す（2026-10-20） |
| — | `analyst_vs_iv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1017 | 残す（2026-10-20） |
| — | `applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 833・861・925・933 | 残す（2026-10-20） |
| 資産回転率（Asset Turnover） | `asset_turnover` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1647 | 残す（2026-10-20） |
| — | `available` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 870・2742 | 残す（2026-10-20） |
| — | `base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 574・871・1277・1277 | 残す（2026-10-20） |
| — | `base_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・817 | 残す（2026-10-20） |
| （Ke ・高成長年 ＝ 表の中央） | `base_wacc` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1152 | 残す（2026-10-20） |
| （Ke ・高成長年 ＝ 表の中央） | `base_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1152 | 残す（2026-10-20） |
| — | `bear` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 872・1278・1278 | 残す（2026-10-20） |
| Bear乗数 | `bear_multiplier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1683 | 残す（2026-10-20） |
| — | `beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 971 | 残す（2026-10-20） |
| — | `bs_adjustment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 858・2381 | 残す（2026-10-20） |
| — | `bull` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 873・1276・1276 | 残す（2026-10-20） |
| Bull乗数 | `bull_multiplier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1684 | 残す（2026-10-20） |
| — | `calculation_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 797 | 残す（2026-10-20） |
| — | `cash` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 862 | 残す（2026-10-20） |
| — | `cash_and_equivalents` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1588 | 残す（2026-10-20） |
| — | `category` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1768 | 残す（2026-10-20） |
| 参考: 実績CAGR等 | `clipped` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1385・1695 | 残す（2026-10-20） |
| 最新年度 | `components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 795・804・809・810 | 残す（2026-10-20） |
| — | `conversion_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 837・934 | 残す（2026-10-20） |
| — | `count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 826 | 残す（2026-10-20） |
| — | `current_price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 795・2509 | 残す（2026-10-20） |
| — | `cv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 821 | 残す（2026-10-20） |
| — | `damodaran_industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2232・2232 | 残す（2026-10-20） |
| — | `damodaran_year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2233・2233 | 残す（2026-10-20） |
| — | `date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 547・2507 | 残す（2026-10-20） |
| Koichi式 v6.2 | `dcf_components` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・808・2376 | 残す（2026-10-20） |
| — | `dcf_type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 807 | 残す（2026-10-20） |
| — | `deficit_quality` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2754 | 残す（2026-10-20） |
| — | `delay_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1988 | 残す（2026-10-20） |
| — | `detected` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 847 | 残す（2026-10-20） |
| 最新年度 | `diluted_shares` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 892・999・2380 | 残す（2026-10-20） |
| — | `dilution_3yr_annual_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1568 | 残す（2026-10-20） |
| — | `dilution_comment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1571 | 残す（2026-10-20） |
| — | `dilution_severity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1570 | 残す（2026-10-20） |
| — | `divergence_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 838・926 | 残す（2026-10-20） |
| — | `divergence_warning` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 839 | 残す（2026-10-20） |
| — | `dupont` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1613 | 残す（2026-10-20） |
| — | `end` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 346・346・358・361 | 残す（2026-10-20） |
| — | `erp` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 659 | 残す（2026-10-20） |
| — | `error` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2728・2819 | 残す（2026-10-20） |
| — | `estimated_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・835 | 残す（2026-10-20） |
| — | `estimated_revenue` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1363 | 残す（2026-10-20） |
| — | `excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1618 | 残す（2026-10-20） |
| — | `expectation_score` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 590・593 | 残す（2026-10-20） |
| — | `expected_return_pct` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1317 | 残す（2026-10-20） |
| — | `fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2131・2141・2155・2155 | 残す（2026-10-20） |
| — | `fcf_2yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 819・819 | 残す（2026-10-20） |
| — | `fcf_5yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 818・818 | 残す（2026-10-20） |
| — | `fcf_base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・815 | 残す（2026-10-20） |
| — | `fcf_base_method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 816 | 残す（2026-10-20） |
| — | `fcf_base_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 817・2768 | 残す（2026-10-20） |
| — | `fcf_estimation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 508・832・2766 | 残す（2026-10-20） |
| FCF CAGR 上限 | `fcf_growth_cap` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1682 | 残す（2026-10-20） |
| FCF CAGR 下限 | `fcf_growth_floor` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1681 | 残す（2026-10-20） |
| — | `fcf_history` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2129 | 残す（2026-10-20） |
| — | `fcf_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1988・2766 | 残す（2026-10-20） |
| — | `fcf_outlier` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 829 | 残す（2026-10-20） |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `fcf_ttm_end` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2186 | 残す（2026-10-20） |
| — | `fcf_value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 855 | 残す（2026-10-20） |
| — | `fields` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 340・1086 | 残す（2026-10-20） |
| — | `financial_health` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1496・1564 | 残す（2026-10-20） |
| 財務レバレッジ（Financial Leverage） | `financial_leverage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1648 | 残す（2026-10-20） |
| — | `fiscal_year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 854・866 | 残す（2026-10-20） |
| Koichi式 v6.2 | `formula` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 798・1068・1068 | 残す（2026-10-20） |
| — | `found` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 851 | 残す（2026-10-20） |
| — | `from_peak` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 557 | 残す（2026-10-20） |
| 5年後 | `future_value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1320・1333 | 残す（2026-10-20） |
| — | `future_values` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 988 | 残す（2026-10-20） |
| 年 @ | `growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1365・1383・1509・1509 | 残す（2026-10-20） |
| 上限 | `growth_cap` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1698・1698 | 残す（2026-10-20） |
| 上限 | `growth_floor` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1698 | 残す（2026-10-20） |
| — | `growth_options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 824・2378 | 残す（2026-10-20） |
| — | `growth_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 1276・1277・1278・1413 | 残す（2026-10-20） |
| — | `growth_sanity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 990 | 残す（2026-10-20） |
| — | `growth_scenarios` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 980 | 残す（2026-10-20） |
| — | `growth_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 977 | 残す（2026-10-20） |
| — | `high_growth_rate_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 887 | 残す（2026-10-20） |
| — | `high_growth_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 889 | 残す（2026-10-20） |
| — | `industry` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 894 | 残す（2026-10-20） |
| — | `insider_buy_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1027 | 残す（2026-10-20） |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `insider_latest_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1030・2186 | 残す（2026-10-20） |
| — | `insider_net_direction` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1029 | 残す（2026-10-20） |
| — | `insider_sell_count` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1028 | 残す（2026-10-20） |
| — | `intrinsic_value_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 791 | 残す（2026-10-20） |
| — | `intrinsic_value_per_share` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 788・1276・1277・1278 | 残す（2026-10-20） |
| — | `intrinsic_value_rf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 792 | 残す（2026-10-20） |
| — | `intrinsic_value_rf_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2257・2259 | 残す（2026-10-20） |
| — | `invalid` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/score_history.json` | 2504 | 残す（2026-10-20） |
| — | `is_annual` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 345 | 残す（2026-10-20） |
| — | `item_name` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1768 | 残す（2026-10-20） |
| — | `items` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 853 | 残す（2026-10-20） |
| ERP（株式リスクプレミアム、現在値） | `label` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 385・686・1078・1305 | 残す（2026-10-20） |
| — | `latest_period_missing` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1086・1086・1088・1089 | 残す（2026-10-20） |
| — | `latest_revenue` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 997・1357・2769 | 残す（2026-10-20） |
| 直近件（新しい順） | `length` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 356・535・562・586 | 残す（2026-10-20） |
| — | `lifecycle` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 548 | 残す（2026-10-20） |
| — | `long_term_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 864 | 残す（2026-10-20） |
| — | `ma200` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 2510 | 残す（2026-10-20） |
| — | `ma200_dev` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 556 | 残す（2026-10-20） |
| — | `ma_addback_detected_but_not_applied` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 846 | 残す（2026-10-20） |
| — | `ma_addback_excluded` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 845 | 残す（2026-10-20） |
| Rm基準（βなし %） | `market_return` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 973・1126・1498・1954 | 残す（2026-10-20） |
| — | `matrix` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 978 | 残す（2026-10-20） |
| — | `maturity_profile` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 885・889 | 残す（2026-10-20） |
| — | `method` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 816・1787 | 残す（2026-10-20） |
| Phase1: 年（Moat Score由来${d.components?.mo | `moat_phase1_years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1943 | 残す（2026-10-20） |
| Phase1: 年（Moat Score由来${d.components?.mo | `moat_score_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1943 | 残す（2026-10-20） |
| — | `model` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 881 | 残す（2026-10-20） |
| 表示期間: history記録開始以降（' + pivPoints[0].mon | `month` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 672・702・702・709 | 残す（2026-10-20） |
| — | `monthly` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 585・666 | 残す（2026-10-20） |
| — | `name` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1369・1984 | 残す（2026-10-20） |
| — | `net_cash` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 859 | 残す（2026-10-20） |
| — | `net_cash_per_share` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 860・2381 | 残す（2026-10-20） |
| — | `net_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1496・1566 | 残す（2026-10-20） |
| 純利益率（Net Margin） | `net_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1646 | 残す（2026-10-20） |
| 財務基準  ｜ 生成  ｜ 次回決算  ｜ インサイダー | `next_earnings_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1068・1068・1569・2186 | 残す（2026-10-20） |
| 減収（構造的変化・継続）: | `ni_ocf_divergence` | 画面の中で作る値 | 1113 | 残す（2026-10-20） |
| (' + count + '銘柄) | `note` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 840・849・874・1088 | 残す（2026-10-20） |
| 営業利益率 | `op_margin` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1928 | 残す（2026-10-20） |
| 未実現の成長機会を「仮説セグメント」として定量化。TAM × 侵入率で期待売上、 | `options` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1981 | 残す（2026-10-20） |
| — | `overall` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 880 | 残す（2026-10-20） |
| D&A比率 | `paper_bgcolor` | 画面の中で作る値 | 432・461・488 | 残す（2026-10-20） |
| — | `penetration` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1988 | 残す（2026-10-20） |
| — | `per` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 993 | 残す（2026-10-20） |
| — | `per_adjusted` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 994 | 残す（2026-10-20） |
| — | `phase` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 550 | 残す（2026-10-20） |
| 年 @ | `phase1` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 889・1879・1879 | 残す（2026-10-20） |
| — | `phase1_growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2231 | 残す（2026-10-20） |
| — | `phase1_growth_auto_adjusted` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2205 | 残す（2026-10-20） |
| 年 @ | `phase2` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1885・1885 | 残す（2026-10-20） |
| D&A比率 | `plot_bgcolor` | 画面の中で作る値 | 433・462・489 | 残す（2026-10-20） |
| — | `price` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 552 | 残す（2026-10-20） |
| price/IV 推移（表示期間中） | `price_iv_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 553・672・675・675 | 残す（2026-10-20） |
| — | `primary` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 981・982 | 残す（2026-10-20） |
| — | `probability` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1988 | 残す（2026-10-20） |
| — | `pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1985 | 残す（2026-10-20） |
| — | `pv_fcf_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2421 | 残す（2026-10-20） |
| — | `pv_high` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 809 | 残す（2026-10-20） |
| — | `pv_phase1` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 811 | 残す（2026-10-20） |
| — | `pv_phase1_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2412 | 残す（2026-10-20） |
| — | `pv_phase2` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 812 | 残す（2026-10-20） |
| — | `pv_phase2_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2412 | 残す（2026-10-20） |
| — | `pv_terminal` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 810 | 残す（2026-10-20） |
| — | `pv_tv_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2412・2421 | 残す（2026-10-20） |
| — | `rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 982・1301・1306・1509 | 残す（2026-10-20） |
| — | `rate_is_sector_default` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json` | 933 | 残す（2026-10-20） |
| — | `ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 820・821 | 残す（2026-10-20） |
| — | `raw_fcf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 834 | 残す（2026-10-20） |
| 売上(推定=最新Revenue×weight) \| 構成比 \| 成長率シナリオ（ | `raw_weighted_growth` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1381・1382・1697・1698 | 残す（2026-10-20） |
| 算出不能（reason: ${dp.reason \|\| | `reason` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1627・1627 | 残す（2026-10-20） |
| — | `recommendation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 551 | 残す（2026-10-20） |
| ターミナル成長率 | `recommended_g` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1384・1692・1693・2206 | 残す（2026-10-20） |
| — | `recommended_g_median` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2207 | 残す（2026-10-20） |
| — | `required_growth_rf_base` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2290 | 残す（2026-10-20） |
| — | `required_growth_rf_live` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2291 | 残す（2026-10-20） |
| — | `return_metrics` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 989 | 残す（2026-10-20） |
| — | `rev_yoy` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 554 | 残す（2026-10-20） |
| 減収（構造的変化・継続）: | `revenue_decline` | 画面の中で作る値 | 1113 | 残す（2026-10-20） |
| — | `revenue_growth_pct` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2754 | 残す（2026-10-20） |
| — | `revenue_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 996 | 残す（2026-10-20） |
| 最新年度 | `revenue_ttm_end` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 998 | 残す（2026-10-20） |
| RICE_adj: R&D資本化想定（CapExのみ投資強度）参考値 | `rice` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 574・574・869・1414 | 残す（2026-10-20） |
| — | `rice_adj` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1415・1415・1415・1422 | 残す（2026-10-20） |
| — | `rice_na_reason` | 書き手なし | 1414・1414・1421・1421 | 残す（2026-10-20） |
| — | `rice_per_ratio` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1416・1416・1423・1423 | 残す（2026-10-20） |
| — | `risk_free_rate` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 972・2257・2294 | 残す（2026-10-20） |
| 固定値 | `risk_free_rate_live` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 691・2292 | 残す（2026-10-20） |
| 固定値 | `risk_free_rate_live_date` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 689・690・2260・2261 | 残す（2026-10-20） |
| — | `risk_free_rate_live_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 688・689・2260 | 残す（2026-10-20） |
| セクター除外 | `roe_10yr_avg` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2746 | 残す（2026-10-20） |
| ROE（分解値） | `roe_decomposed` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1636・1636・1636・1649 | 残す（2026-10-20） |
| — | `roe_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 891 | 残す（2026-10-20） |
| — | `rpo` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 966 | 残す（2026-10-20） |
| 非連続RPO | `rpo_adjustment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1927・1928 | 残す（2026-10-20） |
| 非連続RPO | `rpo_incremental` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1927 | 残す（2026-10-20） |
| — | `rpo_pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 967・2377 | 残す（2026-10-20） |
| — | `rule` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 848 | 残す（2026-10-20） |
| — | `rule40` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 555 | 残す（2026-10-20） |
| Q異常値 | `runway` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2751 | 残す（2026-10-20） |
| Q異常値 | `runway_months` | ほかの書き手あり<br>`docs/value-monitor/stonks-silo/data/results.json` | 2751 | 残す（2026-10-20） |
| — | `sbc_ttm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1567 | 残す（2026-10-20） |
| — | `scenario_bear` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 2511 | 残す（2026-10-20） |
| — | `scenario_bull` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 2512 | 残す（2026-10-20） |
| — | `scenario_valuations` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 987・1492 | 残す（2026-10-20） |
| — | `score_comment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1078・1078 | 残す（2026-10-20） |
| — | `sector` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 893 | 残す（2026-10-20） |
| — | `segment` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 985 | 残す（2026-10-20） |
| DCF採用値（クリップ後）: % | `segment_growth_quarter` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1389・1389 | 残す（2026-10-20） |
| 売上(推定=最新Revenue×weight) \| 構成比 \| 成長率シナリオ（ | `segment_growth_source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1380 | 残す（2026-10-20） |
| — | `segments` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1355 | 残す（2026-10-20） |
| — | `sensitivity` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 975 | 残す（2026-10-20） |
| →年 | `shares_yr_3ago` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1600 | 残す（2026-10-20） |
| →年 | `shares_yr_now` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1600 | 残す（2026-10-20） |
| — | `short_term_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 865 | 残す（2026-10-20） |
| — | `short_term_investments` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 863 | 残す（2026-10-20） |
| — | `signals` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2200 | 残す（2026-10-20） |
| — | `source` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 981 | 残す（2026-10-20） |
| — | `stage` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json` | 549・588・711・715 | 残す（2026-10-20） |
| — | `stage_label` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 589・711・722 | 残す（2026-10-20） |
| — | `substituted_by_annual` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1089 | 残す（2026-10-20） |
| — | `tam` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1988 | 残す（2026-10-20） |
| 判定不能（株価なし） | `tanuki_score` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1050・1050・1078 | 残す（2026-10-20） |
| — | `terminal_growth_used` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 890 | 残す（2026-10-20） |
| — | `ticker` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2618・2632・2642・2660 | 残す（2026-10-20） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 2718・2727 | 残す（2026-10-20） |
| — | `total_debt` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1588 | 残す（2026-10-20） |
| — | `total_pv` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 825・2378 | 残す（2026-10-20） |
| — | `total_transient_amount` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 852 | 残す（2026-10-20） |
| — | `transient_evidence` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 851・852・853 | 残す（2026-10-20） |
| — | `type` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2632・2666 | 残す（2026-10-20） |
| — | `upside_percent` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/.watcher_state.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/history.json` | 575・796・2735 | 残す（2026-10-20） |
| — | `upside_percent_beta` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 794 | 残す（2026-10-20） |
| — | `upside_percent_rf` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 793 | 残す（2026-10-20） |
| Koichi式 v6.2 | `v0` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・1861・1900・2191 | 残す（2026-10-20） |
| Koichi式 v6.2 | `v0_rm` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 801・2376 | 残す（2026-10-20） |
| — | `val` | ほかの書き手あり<br>`docs/common/sec_data/normalized/AAPL_quarterly_normalized.json`<br>`docs/common/sec_data/normalized/ABBV_quarterly_normalized.json` | 362 | 残す（2026-10-20） |
| — | `validation` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 878 | 残す（2026-10-20） |
| 、固定値 | `value` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 970・2262・2733 | 残す（2026-10-20） |
| — | `verdict` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2199・2229 | 残す（2026-10-20） |
| Rm基準（βなし %） | `wacc` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 969・1126・1498・1954 | 残す（2026-10-20） |
| — | `wacc_values` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 976 | 残す（2026-10-20） |
| — | `warnings` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 875・2202 | 残す（2026-10-20） |
| セグメント別売上構成（推定） | `weight` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 1362・1383 | 残す（2026-10-20） |
| — | `year` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 2149・2159・2159 | 残す（2026-10-20） |
| 年 @ | `years` | この段階の書き手あり<br>`docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json`<br>`docs/value-monitor/tanuki_valuation/data/{T}/latest.json` | 889・1879・1885 | 残す（2026-10-20） |

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
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:773`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:916`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:944` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:655` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1633`<br>`common/sec_data/report_consistency_check.py:1774`<br>`common/system_health.py:955`<br>`discover/stonks-silo/src/financial_as_of.py:70`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:405`<br>`src/value/hypecore/hypecore.py:284` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | はい |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 6 | `docs/portfolio/index.html:673`<br>`docs/value-monitor/extreme-fear/index.html:143`<br>`docs/value-monitor/extreme-fear/index.html:175`<br>`docs/value-monitor/extreme-fear/index.html:184`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:865`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:898`<br>`src/value/tanuki_valuation/pipeline.py:900`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:1355`<br>`src/value/tanuki_valuation/pipeline.py:1382`<br>`src/value/tanuki_valuation/pipeline.py:1387`<br>`src/value/tanuki_valuation/pipeline.py:1433`<br>`src/value/tanuki_valuation/pipeline.py:1738`<br>`src/value/tanuki_valuation/pipeline.py:1916` | はい |
| β・WACCの作り方 | 4 | `docs/value-monitor/admin.html:1236`<br>`docs/value-monitor/admin.html:1838`<br>`docs/value-monitor/admin.html:1860`<br>`docs/value-monitor/admin.html:1898`<br>`docs/value-monitor/admin.html:1915`<br>`docs/value-monitor/admin.html:1919`<br>`docs/value-monitor/admin.html:1972`<br>`src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | はい |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | はい |

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
