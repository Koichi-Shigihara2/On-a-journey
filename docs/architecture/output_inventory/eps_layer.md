# EPS（adjusted_eps_analyzer）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`eps_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`docs/value-monitor/adjusted_eps_analyzer/`・`docs/value-monitor/tanuki_valuation/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-21）。画面の区画はKoichiさんの判断で stock.html・一覧の表を残し、管理のページ2つを消した。誰も読まないキーはチャット側の判断（TANUKI と同じ手順で、どこにも使われていないものだけ書くのをやめた）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 5（ファイル 4・人が見るもの 1）／読み手0件 0／テストだけ 0／1件 1／2件以上 3／規則の二重 9（検出式 10種のうち。EPSのファイルを含むもの 3）

**画面の項目**: 画面が読むキー 107（この段階の書き手あり 62・ほかの書き手あり 41・画面の中で作る値 1・**書き手なし 3**）／書き手があるのに誰も読まないキー **14**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 0件／解決できた読み込み 11件＋ファイル名で当たった行 18件（0.0%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

## 消したもの（段階2、2026-10-21）

- `docs/value-monitor/adjusted_eps_analyzer/admin.html（EPSの管理、596行）・docs/value-monitor/admin.html（VALUE MONITOR · Admin、3,650行）と、サブナビの「管理」「管理（VALUE MONITOR ADMIN）」のリンク・tests/test_admin_calc_wacc.py`: 削除（2026-10-21、Koichiさんの判断）。GitHubのAPIでリポジトリのファイルを直接読み書きする管理のページ。操作はClaude Codeが代わりにする（CLAUDE_CODE_START.md「管理の操作の手順」）。消したファイル（kpi_config.py・fcf_conversion_config.json）を読む2か所（FINDINGS 2026-10-21）もページと一緒に無くなった。scripts/audit_site_links.py でリンク切れ・孤立ページ0件
- `config/workflow_dependencies.json の label・accepts_tickers・input_param・bulk_update_order・bulk_update_phases・new_ticker_order`: 削除（2026-10-21）。読み手は管理のページの一括更新だけだった。depends_on・outputs（生成）とキー・yml は残す
- `誰も読まないキー11個（quarterly.json・ttm.json への書き込み）`: 書き込みをやめた（2026-10-21、チャット側の判断）: adjusted_income・condition1・detected_items・adjustment_amount・dta_detected・maturity_monitor・sbc_per_share・sector_threshold・sector_exclusions・split_adjusted・xbrl_eps。全95銘柄の EPS の出力は、消したキーのほかは前後で同じ。TANUKI の理論株価・判定も前後で同じ（EPS の annual.json を FCF の外れ値の判定が読むため確かめた）

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json`<br>（95ファイル） | 銘柄ごとの直近4四半期の合計（TTM）の純利益・調整後純利益・EPSの時系列。EPSの個別ページが読む | `pipeline.py:process_one_ticker` | 1 | value-monitor/adjusted_eps_analyzer/stock.html（直接） | 効く: EPSの画面 | — | 残す（2026-10-21） |
| 2件以上 | `docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>（1ファイル） | 全銘柄の一覧（銘柄ごとの最新期のGAAP EPS・調整後EPS・差・前年比・健全性）と実行の結果（run_status）。EPSの画面の一覧・TANUKI VALUATIONの調整後EPSが読む | `pipeline.py:run` | 8・テスト7 | common/ticker-links.js（直接）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/adjusted_eps_analyzer/tickers.html（直接）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・EPSの画面 | — | 残す（2026-10-21） |
| 2件以上 | `docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>（95ファイル） | 銘柄ごとの年次の調整後EPS（年ごとの調整項目）。TANUKI VALUATIONのFCFの外れ値の判定が、一過性の費用の突合に読む（calculator/adjustments.py） | `pipeline.py:process_one_ticker` | 3・テスト4 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION） | AS-IS-020・AS-IS-021 | 残す（2026-10-21） |
| 2件以上 | `docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`<br>（95ファイル） | 銘柄ごとの四半期の調整後EPS（調整項目・税の調整・AIの分析・特記）と経営者実行力の指標（execution_metrics）。EPSの個別ページ・TANUKI VALUATIONの調整後PERが読む | `pipeline.py:process_one_ticker` | 7・テスト10 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（直接）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（直接）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・EPSの画面 | AS-IS-181・AS-IS-265 | 残す（2026-10-21） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `${GITHUB_STEP_SUMMARY}` | GitHub Actionsの実行の画面に出す要約（成功・失敗の件数と失敗した銘柄、人が見る） | `pipeline.py:_write_step_summary` | .github/workflows/Adjusted_Eps_Analyzer_update.yml |

## 読み手の内訳

### `docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json`（1件）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `last_updated`(1)・`ticker`(1)・`ttm`(1)
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 633
- テスト・その場で計算: `tests/test_split_history_adjustments.py` 行 41・52（経由 `src/value/adjusted_eps_analyzer/pipeline.py::load_split_history`）

### `docs/value-monitor/adjusted_eps_analyzer/data/summary.json`（2件以上）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `last_updated`(5)・`run_status`(3)・`tickers`(12)
- Python・保存済みファイル: `common/system_health.py` 行 204・350・404・1046・1110（経由 `common/system_health.py::_check_i`）
- Python・保存済みファイル: `src/value/adjusted_eps_analyzer/pipeline.py` 行 503・891・895
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 490・585・590・810・991・1069・1144・1323・1530・2952（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 31
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/index.html` 行 240
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 595
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/tickers.html` 行 244
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 238
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_eps_run_status.py` 行 191・206・220・232・242・248・255・264（経由 `common/system_health.py::_check_i`、`src/value/adjusted_eps_analyzer/pipeline.py::generate_summary`）
- テスト・保存済みファイル: `tests/test_eps_upc_prereorg_filter.py` 行 177（経由 `src/value/adjusted_eps_analyzer/pipeline.py::generate_summary`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 211・233・253・266・285・289・294・299・304・309・332・348 ほか64（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::_check_i`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_eps_map`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::_check_i`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/adjusted_eps_analyzer/pipeline.py → docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`（2件以上）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `last_updated`(0)・`ticker`(4)・`years`(5)
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/adjustments.py` 行 972・976・1073（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 246・1023（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 454・1047・1115（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- テスト・その場で計算: `tests/test_split_history_adjustments.py` 行 41・52（経由 `src/value/adjusted_eps_analyzer/pipeline.py::load_split_history`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 3203・3212
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `src/value/tanuki_valuation/calculator/adjustments.py::_load_eps_annual`）
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

### `docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`（2件以上）
- 起動元: .github/workflows/Adjusted_Eps_Analyzer_update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `execution_metrics`(1)・`last_updated`(2)・`quarters`(7)・`ticker`(12)
- Python・保存済みファイル: `common/sec_data/audit.py` 行 126・130・191・369・468（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1340・1388・2476・2480・2979・3011・3045・3482・3690・3734（経由 `common/sec_data/report_consistency_check.py::_read_eps_quarterly`）
- Python・保存済みファイル: `common/system_health.py` 行 86・1038（経由 `common/sec_data/audit.py::audit_ticker`）
- Python・保存済みファイル: `src/value/tanuki_valuation/core_calculator.py` 行 933・984・988・1023（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 454・490・991・1047・1069・1115・1323・1540・1544・2611・2614・3293 ほか2（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- JS・HTML・保存済みファイル: `docs/value-monitor/adjusted_eps_analyzer/stock.html` 行 617
- JS・HTML・保存済みファイル: `docs/value-monitor/stonks-silo/index.html` 行 654
- テスト・その場で計算: `tests/test_split_history_adjustments.py` 行 41・52（経由 `src/value/adjusted_eps_analyzer/pipeline.py::load_split_history`）
- テスト・保存済みファイル: `tests/test_audit_market_data_switch.py` 行 66・71・78（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_read_eps_quarterly`）
- テスト・保存済みファイル: `tests/test_core_calculator_rf_reference.py` 行 23・27・28・41（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- テスト・保存済みファイル: `tests/test_core_calculator_v0_note.py` 行 47・54・63・72（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- テスト・保存済みファイル: `tests/test_dcf_phase1_taper.py` 行 91（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`）
- テスト・保存済みファイル: `tests/test_discord_user_agent.py` 行 96（経由 `common/sec_data/audit.py::audit_ticker`）
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 211・233・253・266・285・289・294・299・304・309・461・500 ほか48（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._generate_report`、`src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_read_eps_quarterly`）
- テスト・保存済みファイル: `tests/test_sensitivity_tapering.py` 行 60・65（経由 `src/value/tanuki_valuation/core_calculator.py::_calc_adjusted_per`）
- テスト・保存済みファイル: `tests/test_split_adjust.py` 行 137・140・148（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_extra_data`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

## 画面の項目

画面（`docs/value-monitor/adjusted_eps_analyzer/index.html`・`docs/value-monitor/adjusted_eps_analyzer/stock.html`・`docs/value-monitor/adjusted_eps_analyzer/tickers.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）

- `book_append_sheet`（`docs/value-monitor/adjusted_eps_analyzer/index.html` 行 469）
- `book_new`（`docs/value-monitor/adjusted_eps_analyzer/index.html` 行 467）
- `table_to_sheet`（`docs/value-monitor/adjusted_eps_analyzer/index.html` 行 468）

### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `adjusted_income`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `adjustment_amount`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `condition1`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `detected_items`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `dta_detected`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `fair_value_auto_detect`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 残す: テストが参照する
- `finished_at`（`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`）: 残す: テストが参照する
- `maturity_monitor`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `sbc_per_share`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `sector_exclusions`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `sector_threshold`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `share_structure_mismatch`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 残す: テストが参照する
- `split_adjusted`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える
- `xbrl_eps`（`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json`）: 書き込みをやめた（2026-10-21）。今の出力のファイルからは次の本番の実行で消える

### `docs/value-monitor/adjusted_eps_analyzer/index.html`（19項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `adjusted_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 395 | 残す（2026-10-21） |
| — | `book_append_sheet` | 書き手なし | 469 | 残す（2026-10-21） |
| — | `book_new` | 書き手なし | 467 | 残す（2026-10-21） |
| — | `company_name` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 328・392 | 残す（2026-10-21） |
| 👁 過小評価候補 — Non-GAAP調整で実力が隠れている可能性 | `deviation_rate` | 画面の中で作る値 | 214・255・271・284 | 残す（2026-10-21） |
| 過大調整 | `eps_diff` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 370・396・396 | 残す（2026-10-21） |
| 差分 | `eps_ratio` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 212・261・261・265 | 残す（2026-10-21） |
| — | `gaap_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 394 | 残す（2026-10-21） |
| — | `gaap_to_adj_positive` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 265・320・368 | 残す（2026-10-21） |
| 過大調整 | `health` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 369・401・401 | 残す（2026-10-21） |
| データが見つかりません | `last_updated` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 244 | 残す（2026-10-21） |
| — | `latest_filing_date` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 193・383・383・384 | 残す（2026-10-21） |
| — | `latest_fiscal_quarter` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 199 | 残す（2026-10-21） |
| — | `table_to_sheet` | 書き手なし | 468 | 残す（2026-10-21） |
| — | `tanuki` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 249 | 残す（2026-10-21） |
| ⚠️ 過大評価注意 — 調整でGAAPより悪く見せている可能性 | `ticker` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 251・286・287・299 | 残す（2026-10-21） |
| データが見つかりません | `tickers` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 243 | 残す（2026-10-21） |
| — | `upside_percent` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 254 | 残す（2026-10-21） |
| YoY成長 | `yoy_growth` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 213・337・337・387 | 残す（2026-10-21） |

### `docs/value-monitor/adjusted_eps_analyzer/stock.html`（87項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `adjusted_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 1091・1092・1092・1092 | 残す（2026-10-21） |
| — | `adjusted_net_income` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1321 | 残す（2026-10-21） |
| 項目 | `adjustments` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1319・1438・1438・1443 | 残す（2026-10-21） |
| — | `ai_analysis` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1013・1445・1445・1445 | 残す（2026-10-21） |
| 調整なし | `comment` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1198 | 残す（2026-10-21） |
| — | `company_reported` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 812 | 残す（2026-10-21） |
| — | `components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 960 | 残す（2026-10-21） |
| — | `computed` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 820 | 残す（2026-10-21） |
| 引用ソース: | `confidence` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1202・1202・1448・1448 | 残す（2026-10-21） |
| — | `current` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 762 | 残す（2026-10-21） |
| >出典: ${co.source \|\| | `definition` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 835 | 残す（2026-10-21） |
| — | `diff` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 821 | 残す（2026-10-21） |
| — | `diluted_shares_used` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1322 | 残す（2026-10-21） |
| — | `end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 843・867 | 残す（2026-10-21） |
| — | `eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json` | 1242 | 残す（2026-10-21） |
| — | `eps_diff_pct` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1110 | 残す（2026-10-21） |
| — | `eps_discrepancy` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1105 | 残す（2026-10-21） |
| — | `excluded_from_percentile` | ほかの書き手あり<br>`docs/common/sec_data/rotce/APP.json`<br>`docs/common/sec_data/rotce/ASTS.json` | 857 | 残す（2026-10-21） |
| — | `execution_metrics` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 669 | 残す（2026-10-21） |
| — | `extracted_from` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1459 | 残す（2026-10-21） |
| — | `filing_date` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 983・1034・1066・1075 | 残す（2026-10-21） |
| ・ | `filled_components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 928 | 残す（2026-10-21） |
| — | `filled_from_annual` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 854・927 | 残す（2026-10-21） |
| — | `fiscal_year` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1222・1223 | 残す（2026-10-21） |
| XBRL計算EPS: | `gaap_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 1113・1124・1137・1163 | 残す（2026-10-21） |
| — | `gaap_net_income` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1318 | 残す（2026-10-21） |
| — | `guidance` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 822 | 残す（2026-10-21） |
| 調整なし | `health` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1195 | 残す（2026-10-21） |
| 長期目標 <span style= | `high` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 829 | 残す（2026-10-21） |
| — | `history` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 765 | 残す（2026-10-21） |
| — | `item` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1209・1447・1447 | 残す（2026-10-21） |
| — | `item_id` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1339・1447 | 残す（2026-10-21） |
| — | `item_name` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1339・1447・1456 | 残す（2026-10-21） |
| — | `label` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 908 | 残す（2026-10-21） |
| （登録済みの最新 ${co.last_registered_quarter_en | `last_registered_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 825 | 残す（2026-10-21） |
| — | `last_updated` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 653 | 残す（2026-10-21） |
| >直近の四半期末（）の会社発表の値: 未登録 | `latest_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 824 | 残す（2026-10-21） |
| — | `latest_registered` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 823 | 残す（2026-10-21） |
| — | `long_term_target` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 822 | 残す（2026-10-21） |
| 長期目標 <span style= | `low` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 829 | 残す（2026-10-21） |
| 算出不可 | `missing_components` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ADSK.json` | 771・771・772 | 残す（2026-10-21） |
| 。無形資産を0と仮定した期は除外 | `n_excluded_assumed_zero` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 808・808 | 残す（2026-10-21） |
| 比較できる四半期が期（8期以上で判定） | `n_quarters` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 801・807 | 残す（2026-10-21） |
| — | `net_adjustment_total` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1320 | 残す（2026-10-21） |
| — | `net_amount` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1337・1457 | 残す（2026-10-21） |
| 純利益（普通株主帰属のタグなし） | `net_income_source` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 787 | 残す（2026-10-21） |
| — | `next_earnings_date` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 951 | 残す（2026-10-21） |
| ・ | `notes` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 774 | 残す（2026-10-21） |
| — | `official_eps` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1109 | 残す（2026-10-21） |
| — | `per` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 961 | 残す（2026-10-21） |
| — | `per_adjusted` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 962 | 残す（2026-10-21） |
| — | `percentile` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 762 | 残す（2026-10-21） |
| ガイダンス（）<span style= | `period` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json` | 828・1022・1239・1514 | 残す（2026-10-21） |
| 期末: | `period_end` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 720・720・1226・1226 | 残す（2026-10-21） |
| 末と末の平均 | `prev_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 790 | 残す（2026-10-21） |
| 。参考（TTM）（自社比の目安はこちらで比較） | `price` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 794 | 残す（2026-10-21） |
| 。参考（TTM）（自社比の目安はこちらで比較） | `price_date` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 794 | 残す（2026-10-21） |
| — | `prior_4q_avg_yoy` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 691 | 残す（2026-10-21） |
| 。参考（TTM）（自社比の目安はこちらで比較） | `ptbv` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ADBE.json` | 766・769・792・792 | 残す（2026-10-21） |
| — | `quarter` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1222・1223 | 残す（2026-10-21） |
| （直近の四半期: ） | `quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 777・777・789・790 | 残す（2026-10-21） |
| — | `quarters` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 625・626・626・655 | 残す（2026-10-21） |
| 算出不可 | `reason` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 687・702・704・717 | 残す（2026-10-21） |
| — | `recent_4q_avg_yoy` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 691 | 残す（2026-10-21） |
| — | `revenue_growth_acceleration` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 684 | 残す（2026-10-21） |
| 直近16四半期分のデータ不足 | `roic_trend` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 698 | 残す（2026-10-21） |
| — | `roic_wacc_ratio` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 704 | 残す（2026-10-21） |
| 純利益（普通株主帰属のタグなし） | `rotce` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 766・769・782・785 | 残す（2026-10-21） |
| 末と末の平均 | `rotce_basis` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 790 | 残す（2026-10-21） |
| 。参考（TTM）（自社比の目安はこちらで比較） | `rotce_ttm` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 791・791 | 残す（2026-10-21） |
| 直近4年中3年以上で算出不能（営業赤字等） | `sbc_ratio` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 714 | 残す（2026-10-21） |
| 。参考（TTM）（自社比の目安はこちらで比較） | `shares_quarter_end` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ALAB.json` | 794 | 残す（2026-10-21） |
| — | `signal` | ほかの書き手あり<br>`docs/common/sec_data/rotce/AAPL.json`<br>`docs/common/sec_data/rotce/ABBV.json` | 797・798 | 残す（2026-10-21） |
| — | `snippet` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1210 | 残す（2026-10-21） |
| >出典: ${co.source \|\| | `source` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json`<br>`docs/portfolio/tail/data/legal_proceedings/ADBE/latest.json` | 835 | 残す（2026-10-21） |
| 引用ソース: | `sources` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1199・1199・1201・1445 | 残す（2026-10-21） |
| — | `special_flags` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 627・627・1104・1104 | 残す（2026-10-21） |
| — | `special_notes` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1105・1105・1144・1144 | 残す（2026-10-21） |
| — | `tanuki` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 641 | 残す（2026-10-21） |
| — | `ticker` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json` | 597 | 残す（2026-10-21） |
| — | `tickers` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/summary.json` | 597 | 残す（2026-10-21） |
| — | `ttm` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/ttm.json` | 1020・1020・1021・1505 | 残す（2026-10-21） |
| — | `upc_prereorg_zero_profit` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 1144・1424 | 残す（2026-10-21） |
| ガイダンス（）<span style= | `value` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 588・609・687・688 | 残す（2026-10-21） |
| — | `values` | ほかの書き手あり<br>`docs/common/sec_data/rotce/SOFI.json` | 817・867 | 残す（2026-10-21） |
| （ROIC、WACC=10%基準） | `year` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 707 | 残す（2026-10-21） |
| — | `years` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/annual.json`<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 701 | 残す（2026-10-21） |

### `docs/value-monitor/adjusted_eps_analyzer/tickers.html`（1項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `value` | この段階の書き手あり<br>`docs/value-monitor/adjusted_eps_analyzer/data/{T}/quarterly.json` | 315 | 残す（2026-10-21） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/value-monitor/adjusted_eps_analyzer/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `company_name`（この段階の書き手あり）<br>`deviation_rate`（画面の中で作る値）<br>`eps_ratio`（この段階の書き手あり）<br>`gaap_to_adj_positive`（この段階の書き手あり）<br>`last_updated`（この段階の書き手あり）<br>`tanuki`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり）<br>`upside_percent`（ほかの書き手あり）<br>`yoy_growth`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断: 一覧の表） |
| （JSで作る要素 #main-table） | `adjusted_eps`（この段階の書き手あり）<br>`book_append_sheet`（書き手なし）<br>`book_new`（書き手なし）<br>`company_name`（この段階の書き手あり）<br>`deviation_rate`（画面の中で作る値）<br>`eps_diff`（この段階の書き手あり）<br>`eps_ratio`（この段階の書き手あり）<br>`gaap_eps`（この段階の書き手あり）<br>`gaap_to_adj_positive`（この段階の書き手あり）<br>`health`（この段階の書き手あり）<br>`latest_filing_date`（この段階の書き手あり）<br>`latest_fiscal_quarter`（この段階の書き手あり）<br>`table_to_sheet`（書き手なし）<br>`ticker`（この段階の書き手あり）<br>`yoy_growth`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断: 一覧の表） |

### `docs/value-monitor/adjusted_eps_analyzer/stock.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| ヘッダー | `value`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| エラー | `execution_metrics`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| メインコンテンツ | `last_updated`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 銘柄ヘッダー | `next_earnings_date`（ほかの書き手あり）<br>`tanuki`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`tickers`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| メトリクスカード | `adjusted_eps`（この段階の書き手あり）<br>`eps_diff_pct`（この段階の書き手あり）<br>`eps_discrepancy`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`gaap_eps`（この段階の書き手あり）<br>`official_eps`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり）<br>`special_notes`（この段階の書き手あり）<br>`upc_prereorg_zero_profit`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 経営者実行力評価（参考指標、[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]④、2026-09-23） | `period_end`（この段階の書き手あり）<br>`prior_4q_avg_yoy`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`recent_4q_avg_yoy`（この段階の書き手あり）<br>`revenue_growth_acceleration`（この段階の書き手あり）<br>`roic_trend`（この段階の書き手あり）<br>`roic_wacc_ratio`（この段階の書き手あり）<br>`sbc_ratio`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`year`（この段階の書き手あり）<br>`years`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| ROTCE・P/TBV（[[ROTCE-PTBV-1]]、2026-10-10、参考表示） | `current`（ほかの書き手あり）<br>`history`（ほかの書き手あり）<br>`missing_components`（ほかの書き手あり）<br>`net_income_source`（ほかの書き手あり）<br>`notes`（ほかの書き手あり）<br>`percentile`（ほかの書き手あり）<br>`prev_quarter_end`（ほかの書き手あり）<br>`price`（ほかの書き手あり）<br>`price_date`（ほかの書き手あり）<br>`ptbv`（ほかの書き手あり）<br>`quarter_end`（ほかの書き手あり）<br>`reason`（この段階の書き手あり）<br>`rotce`（ほかの書き手あり）<br>`rotce_basis`（ほかの書き手あり）<br>`rotce_ttm`（ほかの書き手あり）<br>`shares_quarter_end`（ほかの書き手あり）<br>`signal`（ほかの書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 会社発表のROTCE（[[ROTCE-QUARTERLY-ANNUALIZED-1]]、config/company_reported_metrics.jsonに登録した銘柄だけ） | `company_reported`（ほかの書き手あり）<br>`computed`（ほかの書き手あり）<br>`definition`（ほかの書き手あり）<br>`diff`（ほかの書き手あり）<br>`end`（ほかの書き手あり）<br>`excluded_from_percentile`（ほかの書き手あり）<br>`filled_components`（ほかの書き手あり）<br>`filled_from_annual`（ほかの書き手あり）<br>`guidance`（ほかの書き手あり）<br>`high`（ほかの書き手あり）<br>`label`（ほかの書き手あり）<br>`last_registered_quarter_end`（ほかの書き手あり）<br>`latest_quarter_end`（ほかの書き手あり）<br>`latest_registered`（ほかの書き手あり）<br>`long_term_target`（ほかの書き手あり）<br>`low`（ほかの書き手あり）<br>`n_excluded_assumed_zero`（ほかの書き手あり）<br>`n_quarters`（ほかの書き手あり）<br>`period`（この段階の書き手あり）<br>`ptbv`（ほかの書き手あり）<br>`quarter_end`（ほかの書き手あり）<br>`reason`（この段階の書き手あり）<br>`rotce`（ほかの書き手あり）<br>`signal`（ほかの書き手あり）<br>`source`（ほかの書き手あり）<br>`value`（この段階の書き手あり）<br>`values`（ほかの書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| PER比較（Addition G） | `components`（ほかの書き手あり）<br>`per`（ほかの書き手あり）<br>`per_adjusted`（ほかの書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 期間ピッカー | `ai_analysis`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`period`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり）<br>`ttm`（この段階の書き手あり）<br>`value`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| チャート | `adjusted_eps`（この段階の書き手あり）<br>`eps`（この段階の書き手あり）<br>`fiscal_year`（この段階の書き手あり）<br>`gaap_eps`（この段階の書き手あり）<br>`period`（この段階の書き手あり）<br>`period_end`（この段階の書き手あり）<br>`quarter`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| AI分析 | `comment`（この段階の書き手あり）<br>`confidence`（この段階の書き手あり）<br>`health`（この段階の書き手あり）<br>`item`（この段階の書き手あり）<br>`snippet`（この段階の書き手あり）<br>`sources`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| ウォーターフォール図 | `adjusted_net_income`（この段階の書き手あり）<br>`adjustments`（この段階の書き手あり）<br>`diluted_shares_used`（この段階の書き手あり）<br>`gaap_net_income`（この段階の書き手あり）<br>`item_id`（この段階の書き手あり）<br>`item_name`（この段階の書き手あり）<br>`net_adjustment_total`（この段階の書き手あり）<br>`net_amount`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |
| 調整内訳 | `adjustments`（この段階の書き手あり）<br>`ai_analysis`（この段階の書き手あり）<br>`confidence`（この段階の書き手あり）<br>`extracted_from`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`item`（この段階の書き手あり）<br>`item_id`（この段階の書き手あり）<br>`item_name`（この段階の書き手あり）<br>`net_amount`（この段階の書き手あり）<br>`period`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`sources`（この段階の書き手あり）<br>`special_flags`（この段階の書き手あり）<br>`special_notes`（この段階の書き手あり）<br>`ttm`（この段階の書き手あり）<br>`upc_prereorg_zero_profit`（この段階の書き手あり） | 残す（2026-10-21、Koichiさんの判断） |

### `docs/value-monitor/adjusted_eps_analyzer/tickers.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| DataTables CSS | `value`（この段階の書き手あり） | 残す（2026-10-21） |

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | EPSか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | はい | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。EPSのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | EPSを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:723`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:866`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:894` | はい |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:605` | はい |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1634`<br>`common/sec_data/report_consistency_check.py:1775`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:284` | はい |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:650`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:865`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:898`<br>`src/value/tanuki_valuation/pipeline.py:900`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:1355`<br>`src/value/tanuki_valuation/pipeline.py:1382`<br>`src/value/tanuki_valuation/pipeline.py:1387`<br>`src/value/tanuki_valuation/pipeline.py:1433`<br>`src/value/tanuki_valuation/pipeline.py:1738`<br>`src/value/tanuki_valuation/pipeline.py:1916` | いいえ |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | いいえ |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（src/value/adjusted_eps_analyzer）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（11ファイル・3,536行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `adjustment_detector.py` | 165 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `ai_analyzer.py` | 164 | — | `common/sec_data/report_consistency_check.py` | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `company_metadata.py` | 43 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `eps_calculator.py` | 73 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `execution_metrics.py` | 125 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 1件 |
| `extract_key_facts.py` | 1,235 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 9件 |
| `fair_value_detector.py` | 403 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 1件 |
| `maturity_monitor.py` | 84 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |
| `pipeline.py` | 1,079 | `.github/workflows/Adjusted_Eps_Analyzer_update.yml` | `common/registration/register_ticker.py`<br>`common/sec_data/report_consistency_check.py` | — | 11件 |
| `sector_classifier_v2.py` | 107 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 1件 |
| `tax_adjuster.py` | 58 | — | — | `src/value/adjusted_eps_analyzer/pipeline.py` | 0件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
