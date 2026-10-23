# TANUKI TAIL（src/tail）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`tail_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`docs/portfolio/tail/`・`docs/value-monitor/tanuki_valuation/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-22、Koichiさんの判断）。satellite_alerts.json だけを消し、ほかの出力はすべて残す（ctrl/{T}/index.json は画面の index.html が読む）。誰も読まないキー30もすべて残す（作った時点の記録で作り直せない）。画面に出していないレビューのキーは IDEAS_AND_WATCH.md の [[UI-STORY-REDESIGN-1]] に記録。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 20（ファイル 19・人が見るもの 1）／読み手0件 0／テストだけ 0／1件 4／2件以上 15／規則の二重 10（検出式 11種のうち。TAILのファイルを含むもの 4）

**画面の項目**: 画面が読むキー 180（この段階の書き手あり 163・ほかの書き手あり 12・画面の中で作る値 5・**書き手なし 0**）／書き手があるのに誰も読まないキー **31**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 0件／解決できた読み込み 35件＋ファイル名で当たった行 62件（0.0%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

## 消したもの（段階2、2026-10-22）

- `docs/portfolio/tail/data/satellite_alerts.json`: 削除（2026-10-22、Koichiさんの判断）。書き手（satellite_monitor.py、2026-09-23に削除）も読み手も無いファイルだけが残っていた（FINDINGS 2026-10-22）

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `/tmp/tail_commit_message.txt`<br>（0ファイル） | TANUKI_TAIL_Position_Writeの1回の書き込みのcommitの件名（同じワークフローの後の段が git commit -F で読む） | `workflow_write.py:main` | 1 | — | — | — | 残す（2026-10-22） |
| 1件 | `docs/portfolio/tail/data/ctrl/{T}/index.json`<br>（9ファイル） | 銘柄ごとにItem 4を取った四半期の一覧（quarters）。画面の index.html が最新の四半期を選ぶのに読む | `sec_ctrl_fetcher.py:main` | 1・テスト1 | portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-402 | 残す（2026-10-22） |
| 1件 | `docs/portfolio/tail/data/kpi/{T}_layer3.json`<br>（3ファイル） | 銘柄ごとの、決算の本文から取ったKPI（Layer3。XBRLに無い数値）。SECへの取得のUser-Agentは直書き（FINDINGS 2026-10-22、型B。直さない） | `text_kpi_extractor.py:extract_layer3` | 1・テスト1 | portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由） | — | — | 残す（2026-10-22） |
| 1件 | `docs/portfolio/tail/data/rss_state.json`<br>（1ファイル） | SEC EDGARのRSSで見た銘柄ごとの最後の提出・確認の日時（新規提出・提出の遅れの判定に使う）。SECへの取得のUser-Agentは直書き（FINDINGS 2026-10-22、型B。直さない） | `edgar_rss_monitor.py:save_state` | 1 | portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由） | — | AS-IS-403・AS-IS-404・AS-IS-405 | 残す（2026-10-22） |
| 2件以上 | `config/tail_kpi_map.json`<br>（1ファイル） | 銘柄ごとに監視するKPIとXBRLのタグの対応。kpi_proposer.py（手で動かす）が提案のうち照合を通ったものを書く | `kpi_proposer.py:update_tail_kpi_map` | 5・テスト1 | portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由） | — | AS-IS-437・AS-IS-438・AS-IS-439・AS-IS-440・AS-IS-441 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/scenario/{T}/{sc_name}.json`<br>（27ファイル） | 銘柄ごとのシナリオ（bull/base/bear）のDCF。tail_dcf_bridge.py がレビューの stage2.scenarios（**AIの出力**）とLayer2のKPIから作る | `tail_dcf_bridge.py:generate_scenario_files` | 2 | portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-446 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>（9ファイル） | 銘柄ごとの最新の10-QのItem 4（内部統制）の抜粋と判定。**AIの出力（Grok）**: item4_excerpt_ja（抜粋の和訳）。SECへの取得のUser-Agentは直書き（FINDINGS 2026-10-22、型B。直さない） | `sec_ctrl_fetcher.py:main` | 3・テスト2 | portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | — | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/ctrl/{T}/{Q}.json`<br>（36ファイル） | 銘柄・四半期ごとのItem 4の抜粋（latest.jsonと同じ中身の期ごとの控え）。**AIの出力**: item4_excerpt_ja。この型は{item_key}の型（mda・risk_factors・legal_proceedings）にも当たり、読み手が混ざる | `sec_ctrl_fetcher.py:main` | 3・テスト3 | portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-402 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/journal.json`<br>（1ファイル） | 売買・仮説の見直しの記録（判断の日誌）。画面の入力から書く（人の入力） | `workflow_write.py:register_journal` | 4・テスト1 | portfolio/tail/decision_log.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-412・AS-IS-470 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>（9ファイル） | 銘柄ごとのXBRLのセグメント・KPIの四半期の値（Layer2）。TANUKI VALUATIONのセグメント成長率（segment_growth_xbrl.py）も読む。SECへの取得のUser-Agentは直書き（FINDINGS 2026-10-22、型B。直さない） | `xbrl_segment_fetcher.py:_write_layer2_output` | 8・テスト4 | portfolio/index.html（経由）<br>portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・TANUKI TAILの画面（保有銘柄の監視） | — | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>（9ファイル） | 銘柄ごとのKPIの提案。**AIの出力（Grok）**: proposed_kpis・rejected_kpis（却下はXBRLとの照合の結果）。kpi_proposer.py は手で動かす（CLAUDE_CODE_START.md「TANUKI TAIL 銘柄追加手順」。ワークフローは無い） | `kpi_proposer.py:propose_kpis`<br>`kpi_proposer.py:propose_kpis`<br>`kpi_proposer.py:propose_kpis` | 3・テスト2 | portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | — | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/positions/{T}_thesis.json`<br>（9ファイル） | 銘柄ごとの投資の仮説（エントリーの理由・条件・撤退の目安・監視するKPI）。画面の入力から書く（人の入力） | `workflow_write.py:register_position`<br>`workflow_write.py:confirm_kpis` | 4・テスト2 | portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（経由） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-455 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/positions_index.json`<br>（1ファイル） | TAILの保有・監視銘柄の一覧（positions）。画面の入力（Position Write）から書き、RSS Monitor・SEC Ctrl・Items・Market Pulse等が監視の対象に読む | `workflow_write.py:register_position` | 11・テスト4 | common/ticker-links.js（直接）<br>market-monitor/market-pulse/index.html（経由）<br>portfolio/tail/decision_log.html（経由）<br>portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接）<br>value-monitor/tanuki_score/index.html（経由） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-469 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/prediction_history.json`<br>（1ファイル） | レビューの予想（AIの出力のKPIの予想）と実績の突合の履歴（予想の当たり外れ） | `prediction_tracker.py:main` | 4・テスト1 | portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-506 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/review_queue.json`<br>（1ファイル） | 四半期のレビューを作る待ち行列（新規提出で積み、レビューを作ったら completed・古い期は superseded） | `edgar_rss_monitor.py:save_queue`<br>`quarterly_review_generator.py:save_queue` | 9・テスト3（1） | portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-394・AS-IS-406・AS-IS-407・AS-IS-408・AS-IS-409・AS-IS-410・AS-IS-481 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`<br>（43ファイル） | 銘柄・四半期ごとのレビュー。**AIの出力（Grok）**: stage1（健康度・推奨・理由等）・stage2（シナリオ〈bull/base/bear〉・KPIの予想等）・call2。grok_call1_success・grok_call2_success・stage2_json_valid はAIの呼び出しの結果の印、kpi_snapshot・layer3_snapshot・macro_snapshot は作った時点の入力の控え。レビューは対象の期にかかわらず作った時点の最新の財務で書かれる（[[TAIL-REVIEW-PERIOD-FILTER-1]]。06-06の一括生成の分は[[TAIL-REVIEW-BACKFILL-DATA-PERIOD-1]]、統合済み） | `quarterly_review_generator.py:generate_review` | 3・テスト1 | portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由） | — | — | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/{item_key}/{T}/index.json`<br>（36ファイル） | 銘柄・項目ごとに取った期の一覧（periods）。型は ctrl/{T}/index.json にも当たり、読み手の1件（sec_ctrl_fetcher.py）は ctrl の分 | `sec_items_fetcher.py:_save_result` | 2・テスト2 | portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | — | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/{item_key}/{T}/latest.json`<br>（36ファイル） | 銘柄ごとの最新のMD&A・リスク要因・訴訟（item_key = mda・risk_factors・legal_proceedings）の抜粋。**AIの出力（Grok）**: excerpt_ja（和訳）。segment_outlook（MD&Aのセグメントの見通し）は2026-10-22に生成を止め、過去のファイルにだけ残る。型は ctrl/ にも当たる。SECへの取得のUser-Agentは直書き（FINDINGS 2026-10-22、型B。直さない） | `sec_items_fetcher.py:_save_result` | 4・テスト4 | portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-056・AS-IS-071 | 残す（2026-10-22） |
| 2件以上 | `docs/portfolio/tail/data/{item_key}/{T}/{Q}.json`<br>（174ファイル） | 銘柄・期ごとのMD&A・リスク要因・訴訟の抜粋の控え。**AIの出力**: excerpt_ja（segment_outlook は2026-10-22に生成を止めた）。型は ctrl/ にも当たる | `sec_items_fetcher.py:_save_result` | 5・テスト6 | portfolio/tail/detail.html（直接）<br>portfolio/tail/index.html（直接） | 効く: TANUKI TAILの画面（保有銘柄の監視） | AS-IS-013・AS-IS-019・AS-IS-046・AS-IS-047・AS-IS-056・AS-IS-071・AS-IS-300・AS-IS-334・AS-IS-388・AS-IS-390・AS-IS-391・AS-IS-481 | 残す（2026-10-22） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `Discord（DISCORD_WEBHOOK）` | 新規提出・提出の遅れ（法定の期限超え）のDiscordへの通知 | `edgar_rss_monitor.py:send_discord` | .github/workflows/TANUKI_TAIL_RSS_Monitor.yml |

## 読み手の内訳

### `/tmp/tail_commit_message.txt`（1件）
- 起動元: .github/workflows/TANUKI_TAIL_Position_Write.yml
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_Position_Write.yml` 行 52

### `docs/portfolio/tail/data/ctrl/{T}/index.json`（1件）
- 起動元: .github/workflows/TANUKI_TAIL_SEC_Ctrl.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `quarters`(1)
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 1380
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 486

### `docs/portfolio/tail/data/kpi/{T}_layer3.json`（1件）
- 起動元: .github/workflows/TANUKI_TAIL_RSS_Monitor.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `fetched_at`(0)・`kpis`(1)・`layer3_complete`(0)・`not_found`(0)・`quarter`(2)・`ticker`(2)
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 169・172・1571・1775・1811（経由 `src/tail/quarterly_review_generator.py::load_layer3_kpi`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_layer3_kpi`）
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/index.html`

### `docs/portfolio/tail/data/rss_state.json`（1件）
- 起動元: .github/workflows/TANUKI_TAIL_RSS_Monitor.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `ADBE`(0)・`APP`(0)・`CELH`(0)・`CRWV`(0)・`NVDA`(0)・`PLTR`(0)・`SOFI`(0)・`SOUN`(0)・`TSLA`(0)
- Python・保存済みファイル: `src/tail/edgar_rss_monitor.py` 行 41・361・606・638（経由 `src/tail/edgar_rss_monitor.py::load_state`）
- 画面（経由（src/tail/edgar_rss_monitor.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/edgar_rss_monitor.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/index.html`

### `config/tail_kpi_map.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `ADBE`(0)・`APP`(0)・`CELH`(0)・`CRWV`(1)・`NVDA`(0)・`PLTR`(1)・`SOFI`(1)・`SOUN`(1)・`TSLA`(1)
- Python・保存済みファイル: `src/tail/kpi_proposer.py` 行 65・147・387・740・769・842・854（経由 `src/tail/kpi_proposer.py::load_kpi_map`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 62・610・661・672・1075・1648・1775（経由 `src/tail/quarterly_review_generator.py::_load_tail_kpi_map`）
- Python・保存済みファイル: `src/tail/xbrl_segment_fetcher.py` 行 48・722・758（経由 `src/tail/xbrl_segment_fetcher.py::main`）
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_KPI_Update.yml` 行 49
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` 行 130
- テスト・保存済みファイル: `tests/test_xbrl_segment_fetcher_unit_label.py` 行 88・90・97・109・132（経由 `tests/test_xbrl_segment_fetcher_unit_label.py::TestTailKpiMapConfigConsistency._load_config`）
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/kpi_proposer.py → docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json））: `docs/portfolio/tail/index.html`

### `docs/portfolio/scenario/{T}/{sc_name}.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_RSS_Monitor.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `assumptions`(1)・`base_intrinsic_value`(2)・`current_price`(2)・`future_values`(2)・`generated_at`(2)・`kpi_current`(2)・`kpi_forecasts`(2)・`kpi_format`(2)・`kpi_layer1_keys`(2)・`quarter`(2)・`scenario`(1)・`ticker`(2)
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 352
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 1025

### `docs/portfolio/tail/data/ctrl/{T}/latest.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_SEC_Ctrl.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `effective`(2)・`fetched_at`(2)・`filing_date`(3)・`item4_excerpt`(2)・`item4_excerpt_ja`(2)・`material_weaknesses`(2)・`quarter`(2)・`report_date`(1)・`significant_deficiencies`(2)・`ticker`(4)
- Python・保存済みファイル: `common/system_health.py` 行 223・1043・1110（経由 `common/system_health.py::check_f_tail`）
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 301・303・327（経由 `src/tail/sec_ctrl_fetcher.py::_stored_latest`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 893・1388
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_f_tail`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_f_tail`）

### `docs/portfolio/tail/data/ctrl/{T}/{Q}.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_SEC_Ctrl.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `effective`(2)・`fetched_at`(2)・`filing_date`(3)・`item4_excerpt`(2)・`item4_excerpt_ja`(2)・`material_weaknesses`(2)・`quarter`(2)・`quarters`(2)・`report_date`(1)・`significant_deficiencies`(2)・`ticker`(4)
- Python・保存済みファイル: `common/system_health.py` 行 223・1043・1110（経由 `common/system_health.py::check_f_tail`）
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 301・303・327（経由 `src/tail/sec_ctrl_fetcher.py::_stored_latest`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 893・1380・1387・1388・2414
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 486
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_f_tail`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_f_tail`）

### `docs/portfolio/tail/data/journal.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_Position_Write.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `entries`(4)
- Python・保存済みファイル: `src/tail/workflow_write.py` 行 33
- JS・HTML・保存済みファイル: `docs/portfolio/tail/decision_log.html` 行 150
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 925
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_Position_Write.yml` 行 48
- テスト・保存済みファイル: `tests/test_tail_workflow_write.py` 行 28
- 画面（経由（src/tail/workflow_write.py → docs/portfolio/tail/data/positions/{T}_thesis.json））: `docs/portfolio/tail/detail.html`

### `docs/portfolio/tail/data/kpi/{T}_layer2.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_KPI_Update.yml・.github/workflows/TANUKI_TAIL_RSS_Monitor.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `fetched_at`(1)・`kpis`(8)・`layer2_complete`(1)・`missing_kpis`(2)・`ticker`(11)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2116・2120・3525・3702（経由 `common/sec_data/report_consistency_check.py::_check_kpi_fetch_failures`）
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 47・50・93・164・253（経由 `src/tail/prediction_tracker.py::_load_layer2_actuals`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 160・164・1570・1775・1787・1811（経由 `src/tail/quarterly_review_generator.py::load_layer2_kpi`、`src/tail/tail_dcf_bridge.py::_load_layer2_kpi`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 105・108・206・337・349（経由 `src/tail/tail_dcf_bridge.py::_load_layer2_kpi`）
- Python・保存済みファイル: `src/value/tanuki_valuation/calculator/segment_growth_xbrl.py` 行 203・207・255・339・364（経由 `src/value/tanuki_valuation/calculator/segment_growth_xbrl.py::_load_layer2_kpi_series`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 3321（経由 `src/value/tanuki_valuation/calculator/segment_growth_xbrl.py::_load_layer2_kpi_series`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 321
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 1356
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_kpi_fetch_failures`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_kpi_fetch_failures`）
- テスト・保存済みファイル: `tests/test_segment_growth_xbrl.py` 行 44・114・138・159・198・219・224・284・287・297（経由 `tests/test_segment_growth_xbrl.py::_write_layer2`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_layer2_kpi`）
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_score/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `generated_at`(1)・`proposed_kpis`(2)・`rejected_kpis`(1)・`thesis_version`(0)・`ticker`(5)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2191・2195・3537・3702（経由 `common/sec_data/report_consistency_check.py::_check_kpi_rejected_proposals`）
- Python・保存済みファイル: `src/tail/text_kpi_extractor.py` 行 113・117・586・701・713（経由 `src/tail/text_kpi_extractor.py::load_proposal`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 2585
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_kpi_rejected_proposals`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_kpi_rejected_proposals`）

### `docs/portfolio/tail/data/positions/{T}_thesis.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_Position_Write.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `created_at`(0)・`entry_condition`(1)・`entry_date`(2)・`entry_price`(3)・`entry_story`(2)・`exit_condition`(1)・`exit_guide`(3)・`holding_period`(0)・`kpis`(4)・`status`(3)・`strategy_name`(1)・`thesis`(3)・`ticker`(6)・`type`(2)・`updated_at`(1)・`version`(2)
- Python・保存済みファイル: `src/tail/kpi_proposer.py` 行 136・140・727・842・854（経由 `src/tail/kpi_proposer.py::load_thesis`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 151・155・1569・1775・1787・1811（経由 `src/tail/quarterly_review_generator.py::load_thesis`、`src/tail/tail_dcf_bridge.py::_load_thesis`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 97・100・205・337・349（経由 `src/tail/tail_dcf_bridge.py::_load_thesis`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 299
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_thesis`）
- テスト・保存済みファイル: `tests/test_tail_workflow_write.py` 行 219
- 画面（経由（src/tail/kpi_proposer.py → docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json））: `docs/portfolio/tail/index.html`

### `docs/portfolio/tail/data/positions_index.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_Position_Write.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `positions`(10)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2010・2022・2115・2190・3515・3525・3537・3702（経由 `src/tail/edgar_rss_monitor.py::get_excluded_positions`、`src/tail/edgar_rss_monitor.py::get_monitored_tickers`）
- Python・保存済みファイル: `scripts/analysis/market_pulse_redesign.py` 行 399・478（経由 `src/tail/edgar_rss_monitor.py::get_monitored_tickers`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 1559・1583・1969（経由 `src/tail/edgar_rss_monitor.py::get_monitored_tickers`）
- Python・保存済みファイル: `src/market/market_pulse/sector_rotation.py` 行 225・261（経由 `src/tail/edgar_rss_monitor.py::get_monitored_tickers`）
- Python・保存済みファイル: `src/tail/edgar_rss_monitor.py` 行 39・261・295・332・338・589・590・638（経由 `src/tail/edgar_rss_monitor.py::get_excluded_positions`、`src/tail/edgar_rss_monitor.py::get_monitored_tickers`）
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 37・397・416・478（経由 `src/tail/sec_ctrl_fetcher.py::load_tail_tickers`）
- Python・保存済みファイル: `src/tail/workflow_write.py` 行 32
- JS・HTML・保存済みファイル: `docs/common/ticker-links.js` 行 37
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 377
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 865
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_Position_Write.yml` 行 47
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `src/tail/edgar_rss_monitor.py::get_monitored_tickers`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `src/tail/edgar_rss_monitor.py::get_monitored_tickers`）
- テスト・保存済みファイル: `tests/test_tail_sec_ctrl_fetcher.py` 行 26・74
- テスト・保存済みファイル: `tests/test_tail_workflow_write.py` 行 27
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/tail/workflow_write.py → docs/portfolio/tail/data/journal.json））: `docs/portfolio/tail/decision_log.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/value-monitor/tanuki_score/index.html`

### `docs/portfolio/tail/data/prediction_history.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_RSS_Monitor.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `ADBE`(0)・`APP`(1)・`CELH`(1)・`CRWV`(0)・`NVDA`(0)・`PLTR`(1)・`SOFI`(1)・`SOUN`(1)・`TSLA`(1)・`_updated_at`(1)
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 31・173
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 61・961・1600・1775・1811（経由 `src/tail/quarterly_review_generator.py::_load_past_predictions`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 916
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` 行 132
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::_load_past_predictions`）
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`

### `docs/portfolio/tail/data/review_queue.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_RSS_Monitor.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `queue`(8)
- Python・その場で計算: `common/sec_data/report_consistency_check.py` 行 2018（経由 `src/tail/quarterly_review_generator.py::load_queue`）
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 2018・3515・3702（経由 `src/tail/quarterly_review_generator.py::load_queue`）
- Python・保存済みファイル: `src/tail/edgar_rss_monitor.py` 行 42・374・607・638（経由 `src/tail/edgar_rss_monitor.py::load_queue`）
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 73・92・164（経由 `src/tail/review_status.py::load_queue`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 57・1535・1746・1787・1811（経由 `src/tail/quarterly_review_generator.py::load_queue`、`src/tail/review_status.py::load_queue`）
- Python・保存済みファイル: `src/tail/review_status.py` 行 27・49
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 87・200・337（経由 `src/tail/review_status.py::load_queue`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 305
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 953
- ワークフロー・保存済みファイル: `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` 行 53・71・90・127
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `src/tail/quarterly_review_generator.py::load_queue`）
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `src/tail/quarterly_review_generator.py::load_queue`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58・82・85・92・93・103・105・114・118（経由 `src/tail/quarterly_review_generator.py::load_queue`、`src/tail/review_status.py::load_queue`）

### `docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_RSS_Monitor.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `call2`(1)・`data_version`(1)・`filed_date`(1)・`generated_at`(2)・`grok_call1_success`(1)・`grok_call2_success`(1)・`kpi_snapshot`(1)・`layer1_complete`(1)・`layer2_complete`(1)・`layer2_missing_kpis`(1)・`layer3_snapshot`(1)・`macro_snapshot`(1)・`quarter`(4)・`stage1`(1)・`stage2`(3)・`stage2_json_valid`(1)・`thesis_version`(1)・`ticker`(4)・`transcript_available`(1)
- Python・保存済みファイル: `src/tail/prediction_tracker.py` 行 74・92・164・253（経由 `src/tail/prediction_tracker.py::_load_reviews_for_ticker`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 1787・1811（経由 `src/tail/tail_dcf_bridge.py::_load_latest_review`）
- Python・保存済みファイル: `src/tail/tail_dcf_bridge.py` 行 89・91・200・337・349（経由 `src/tail/tail_dcf_bridge.py::_load_latest_review`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58・82・85・92・93・103・105・118（経由 `src/tail/prediction_tracker.py::_load_reviews_for_ticker`、`src/tail/tail_dcf_bridge.py::_load_latest_review`）
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/prediction_tracker.py → docs/portfolio/tail/data/prediction_history.json））: `docs/portfolio/tail/index.html`

### `docs/portfolio/tail/data/{item_key}/{T}/index.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_SEC_Items.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `periods`(1)・`quarters`(2)
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 445・453・478（経由 `src/tail/sec_ctrl_fetcher.py::main`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 1380
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 486
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 188・196・197

### `docs/portfolio/tail/data/{item_key}/{T}/latest.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_SEC_Items.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `accession`(2)・`changed`(1)・`effective`(3)・`excerpt`(2)・`excerpt_ja`(2)・`fetched_at`(2)・`filing_date`(5)・`item4_excerpt`(2)・`item4_excerpt_ja`(2)・`item_key`(1)・`material_weaknesses`(3)・`period`(2)・`quarter`(3)・`report_date`(2)・`segment_outlook`(1)・`significant_deficiencies`(3)・`source`(3)・`ticker`(7)
- Python・保存済みファイル: `common/system_health.py` 行 223・1043・1110（経由 `common/system_health.py::check_f_tail`）
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 301・303・327・433・444・478（経由 `src/tail/sec_ctrl_fetcher.py::_stored_latest`、`src/tail/sec_ctrl_fetcher.py::main`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 338
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 893・1388
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 162
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_f_tail`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_f_tail`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 186・202・225・226

### `docs/portfolio/tail/data/{item_key}/{T}/{Q}.json`（2件以上）
- 起動元: .github/workflows/TANUKI_TAIL_SEC_Items.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `accession`(4)・`changed`(2)・`effective`(3)・`excerpt`(4)・`excerpt_ja`(4)・`fetched_at`(3)・`filing_date`(7)・`item4_excerpt`(2)・`item4_excerpt_ja`(2)・`item_key`(3)・`material_weaknesses`(3)・`period`(4)・`periods`(2)・`quarter`(4)・`quarters`(2)・`report_date`(4)・`segment_outlook`(1)・`significant_deficiencies`(3)・`source`(4)・`ticker`(9)
- Python・保存済みファイル: `common/system_health.py` 行 223・1043・1110（経由 `common/system_health.py::check_f_tail`）
- Python・保存済みファイル: `src/tail/sec_ctrl_fetcher.py` 行 301・303・327・433・443・444・445・453・478（経由 `src/tail/sec_ctrl_fetcher.py::_stored_latest`、`src/tail/sec_ctrl_fetcher.py::main`）
- Python・保存済みファイル: `src/tail/sec_items_fetcher.py` 行 366・368・376・412・479・610・617（経由 `src/tail/sec_items_fetcher.py::_stored_period`）
- JS・HTML・保存済みファイル: `docs/portfolio/tail/detail.html` 行 338
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 893・1380・1387・1388・2414
- テスト・保存済みファイル: `tests/test_output_inventory_check.py` 行 486
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 162
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_f_tail`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_f_tail`）
- テスト・保存済みファイル: `tests/test_tail_sec_items_fetcher.py` 行 184・186・188・197・202・226
- テスト・保存済みファイル: `tests/test_tail_sec_items_unchanged.py` 行 89・91

## 画面の項目

画面（`docs/portfolio/tail/decision_log.html`・`docs/portfolio/tail/detail.html`・`docs/portfolio/tail/index.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）


### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `_updated_at`（`docs/portfolio/tail/data/prediction_history.json`）: 残す: 予想と実績の突合を最後に更新した時点の記録
- `base_revenue`（`docs/portfolio/scenario/{T}/{sc_name}.json`）: 残す: シナリオのDCFの入力（レビューの stage2.scenarios〈AIの出力〉から作った時点の値）
- `business_model`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `changed`（`docs/portfolio/tail/data/{item_key}/{T}/latest.json`、`docs/portfolio/tail/data/{item_key}/{T}/{Q}.json`）: 残す: 取得の印（前回の取得から本文が変わったか）。取得した時点の記録
- `competitive_advantage`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `completed_at`（`docs/portfolio/tail/data/review_queue.json`）: 残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録
- `data_version`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）
- `fcf_adjustment`（`docs/portfolio/scenario/{T}/{sc_name}.json`）: 残す: シナリオのDCFの入力（レビューの stage2.scenarios〈AIの出力〉から作った時点の値）
- `filed_date`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）
- `grok_call1_success`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `grok_call2_success`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `kpi_snapshot`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）
- `last_checked`（`docs/portfolio/tail/data/rss_state.json`）: 残す: RSSで銘柄を最後に確かめた時点の記録（提出の遅れの判定の状態）
- `layer1_complete`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `layer2_missing_kpis`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `layer3_complete`（`docs/portfolio/tail/data/kpi/{T}_layer3.json`）: 残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `layer3_snapshot`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）
- `macro_score_at_action`（`docs/portfolio/tail/data/journal.json`）: 残す: 人の入力（売買・見直しの判断をした時点のMACROの点数）。作り直せない
- `macro_snapshot`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: レビューを作った時点の入力の控え（後から入力が変わっても、そのレビューが何を見たかを残す）
- `management`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `queued_at`（`docs/portfolio/tail/data/review_queue.json`）: 残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録
- `quote`（`docs/portfolio/tail/data/{item_key}/{T}/latest.json`、`docs/portfolio/tail/data/{item_key}/{T}/{Q}.json`）
- `recommendation_reason`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: AIの出力（Grok）。作った時点の記録で、同じものは作り直せない。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `rejection_reason`（`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`）: 残す: KPIの提案を却下した理由（XBRLとの照合の結果）。作った時点の記録
- `review_path`（`docs/portfolio/tail/data/review_queue.json`）: 残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録
- `segment_outlook`（`docs/portfolio/tail/data/{item_key}/{T}/latest.json`、`docs/portfolio/tail/data/{item_key}/{T}/{Q}.json`）: 生成を止めた（2026-10-22、Koichiさんの判断）。過去の mda のファイルにだけ作った時点の記録として残る（TANUKI の report.txt にも出ていなかった、FINDINGS 2026-10-22）
- `stage2_json_valid`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）
- `superseded_at`（`docs/portfolio/tail/data/review_queue.json`）: 残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録
- `superseded_reason`（`docs/portfolio/tail/data/review_queue.json`）: 残す: 待ち行列の状態（いつ積み・終え・置き換えたか）。作った時点の記録
- `thesis_version`（`docs/portfolio/tail/data/journal.json`、`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`）: 残す: 作った時点の仮説の版（人の入力の版。後から仮説が変わっても、どの版で作ったかを残す）
- `transcript_available`（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）: 残す: 品質の印（AIの呼び出し・入力のそろい方の結果）。作った時点の記録。画面には出していない（[[UI-STORY-REDESIGN-1]]）

### `docs/portfolio/tail/decision_log.html`（10項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `entries` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 154 | 残す（2026-10-22） |
| — | `health_score_at_action` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 192・192 | 残す（2026-10-22） |
| — | `price` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 190・190 | 残す（2026-10-22） |
| — | `reason` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 204 | 残す（2026-10-22） |
| — | `shares` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 191・191 | 残す（2026-10-22） |
| — | `tags` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 193 | 残す（2026-10-22） |
| — | `ticker` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json` | 163・174・200 | 残す（2026-10-22） |
| — | `timestamp` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 188 | 残す（2026-10-22） |
| — | `type` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 175・187・201 | 残す（2026-10-22） |
| — | `value` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>`docs/portfolio/tail/data/kpi/{T}_layer3.json` | 117・171・172 | 残す（2026-10-22） |

### `docs/portfolio/tail/detail.html`（72項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| DCFシナリオ | `base` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 612・621・630・783 | 残す（2026-10-22） |
| 現在 | `base_intrinsic_value` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 637 | 残す（2026-10-22） |
| DCFシナリオ | `bear` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 612・621・630・783 | 残す（2026-10-22） |
| DCFシナリオ | `bull` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 612・621・630・783 | 残す（2026-10-22） |
| レビューデータなし | `call2` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 717 | 残す（2026-10-22） |
| 歴史的類比: ' + esc(ha.company) + ' | `company` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 738・740 | 残す（2026-10-22） |
| — | `components` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 368 | 残す（2026-10-22） |
| ⚠ 楽観バイアス警告 | `concerns` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 571 | 残す（2026-10-22） |
| — | `current_price` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 370・448・631 | 残す（2026-10-22） |
| — | `data` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json` | 523・529 | 残す（2026-10-22） |
| 有効 (Effective) | `effective` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 876・877 | 残す（2026-10-22） |
| — | `entry_condition` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 501・502 | 残す（2026-10-22） |
| 登録日: ' + esc(th.entry_date \|\| '–') + ' | `entry_date` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 506 | 残す（2026-10-22） |
| テーゼデータなし | `entry_price` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 484・484 | 残す（2026-10-22） |
| — | `entry_story` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 495・496 | 残す（2026-10-22） |
| エントリーストーリー進捗 | `entry_story_progress` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 746・746 | 残す（2026-10-22） |
| — | `excerpt` | この段階の書き手あり<br>`docs/portfolio/tail/data/{item_key}/{T}/latest.json`<br>`docs/portfolio/tail/data/{item_key}/{T}/{Q}.json` | 911 | 残す（2026-10-22） |
| — | `excerpt_ja` | この段階の書き手あり<br>`docs/portfolio/tail/data/{item_key}/{T}/latest.json`<br>`docs/portfolio/tail/data/{item_key}/{T}/{Q}.json` | 910 | 残す（2026-10-22） |
| — | `exit_condition` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 499・500 | 残す（2026-10-22） |
| — | `exit_distance` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 579 | 残す（2026-10-22） |
| 遠 | `exit_distance_reason` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 583 | 残す（2026-10-22） |
| — | `exit_guide` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 493・494 | 残す（2026-10-22） |
| / 提出日: | `filing_date` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 901・904・904 | 残す（2026-10-22） |
| マクロ環境の示唆 | `five_perspectives` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 751 | 残す（2026-10-22） |
| 現在 | `future_values` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 638 | 残す（2026-10-22） |
| — | `generated_at` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json` | 556・556 | 残す（2026-10-22） |
| — | `health_score` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 419・451・551・593 | 残す（2026-10-22） |
| — | `historical_analogy` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 737 | 残す（2026-10-22） |
| 示唆: | `implication` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 743 | 残す（2026-10-22） |
| — | `intrinsic_value` | 画面の中で作る値 | 447 | 残す（2026-10-22） |
| — | `intrinsic_value_per_share` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 371 | 残す（2026-10-22） |
| — | `key_assumptions` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 692 | 残す（2026-10-22） |
| — | `kpi_current` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 788 | 残す（2026-10-22） |
| 1年後 | `kpi_forecasts` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 799・841・842 | 残す（2026-10-22） |
| — | `kpi_format` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 790 | 残す（2026-10-22） |
| — | `kpi_layer1_keys` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 789 | 残す（2026-10-22） |
| KPIデータなし | `kpis` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>`docs/portfolio/tail/data/kpi/{T}_layer3.json` | 521 | 残す（2026-10-22） |
| — | `label` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 657・685・761・871 | 残す（2026-10-22） |
| マクロ環境の示唆 | `macro_implications` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 748・748 | 残す（2026-10-22） |
| 世の中の注目ポイント | `market_attention` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 747・747 | 残す（2026-10-22） |
| — | `material_weaknesses` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 883 | 残す（2026-10-22） |
| — | `next_kpis` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 585 | 残す（2026-10-22） |
| テーゼへの問いかけ | `next_review_focus` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 729 | 残す（2026-10-22） |
| — | `operating_margin_terminal` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 689 | 残す（2026-10-22） |
| ⚠ 楽観バイアス警告 | `optimism_bias_warning` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 567・568 | 残す（2026-10-22） |
| その後: | `outcome` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 742 | 残す（2026-10-22） |
| — | `period` | この段階の書き手あり<br>`docs/portfolio/tail/data/{item_key}/{T}/latest.json`<br>`docs/portfolio/tail/data/{item_key}/{T}/{Q}.json` | 900 | 残す（2026-10-22） |
| — | `positions` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions_index.json` | 381 | 残す（2026-10-22） |
| ⚠ 楽観バイアス警告 | `positives` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 570 | 残す（2026-10-22） |
| — | `quarter` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json` | 313・316・531・531 | 残す（2026-10-22） |
| — | `queue` | この段階の書き手あり<br>`docs/portfolio/tail/data/review_queue.json` | 309 | 残す（2026-10-22） |
| — | `recommendation` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 420・452・552・942 | 残す（2026-10-22） |
| — | `revenue_growth_y1` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 686 | 残す（2026-10-22） |
| — | `revenue_growth_y2` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 687 | 残す（2026-10-22） |
| — | `revenue_growth_y3` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 688 | 残す（2026-10-22） |
| — | `risk_factors` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 693 | 残す（2026-10-22） |
| DCFシナリオ | `scenarios` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 358・612・612・612 | 残す（2026-10-22） |
| — | `significant_deficiencies` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 891 | 残す（2026-10-22） |
| 類似点: | `similarity` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 741 | 残す（2026-10-22） |
| — | `source` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 903・903 | 残す（2026-10-22） |
| — | `stage1` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 418・450・550・593 | 残す（2026-10-22） |
| — | `stage2` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 619・792 | 残す（2026-10-22） |
| ステータス: ' + esc(th.status \|\| '–') + ' | `status` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json`<br>`docs/portfolio/tail/data/review_queue.json` | 310・345・358・388 | 残す（2026-10-22） |
| — | `strategy_name` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 498 | 残す（2026-10-22） |
| — | `summary` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`<br>`docs/portfolio/tail/data/{item_key}/{T}/latest.json` | 564・565・944・945 | 残す（2026-10-22） |
| — | `thesis` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 302・414・479・492 | 残す（2026-10-22） |
| — | `thesis_questions` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 724 | 残す（2026-10-22） |
| — | `ticker` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json` | 310・313・391・391 | 残す（2026-10-22） |
| テーゼデータなし | `type` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 390・390・390・415 | 残す（2026-10-22） |
| — | `unit` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>`docs/portfolio/tail/data/kpi/{T}_layer3.json` | 534 | 残す（2026-10-22） |
| — | `upside_percent` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 372・446 | 残す（2026-10-22） |
| — | `value` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>`docs/portfolio/tail/data/kpi/{T}_layer3.json` | 262・345・345・358 | 残す（2026-10-22） |

### `docs/portfolio/tail/index.html`（98項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| 売上成長率 Y1/Y2/Y3 | `Y1_growth` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2174 | 残す（2026-10-22） |
| 売上成長率 Y1/Y2/Y3 | `Y2_growth` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2174 | 残す（2026-10-22） |
| 売上成長率 Y1/Y2/Y3 | `Y3_growth` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2174 | 残す（2026-10-22） |
| — | `accuracy` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1801 | 残す（2026-10-22） |
| — | `actual` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1808 | 残す（2026-10-22） |
| 売上成長率 Y1/Y2/Y3 | `assumptions` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2173・2178・2182・2186 | 残す（2026-10-22） |
| 自動取得可 | `auto_fetchable` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2616 | 残す（2026-10-22） |
| — | `avg_cost` | ほかの書き手あり<br>`docs/portfolio/data/portfolio.json` | 991 | 残す（2026-10-22） |
| 1年後 | `base` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1149・1152・1160・1477 | 残す（2026-10-22） |
| 現在 | `base_intrinsic_value` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2110・2117 | 残す（2026-10-22） |
| 1年後 | `bear` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1160・1477・1478・2089 | 残す（2026-10-22） |
| — | `brokers` | ほかの書き手あり<br>`docs/portfolio/data/portfolio.json` | 987 | 残す（2026-10-22） |
| 1年後 | `bull` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1160・1477・1478・2089 | 残す（2026-10-22） |
| — | `call2` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1671 | 残す（2026-10-22） |
| — | `components` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 1010 | 残す（2026-10-22） |
| — | `concerns` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1620・1622・1722 | 残す（2026-10-22） |
| DCFデータなし | `current_price` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 1012・1093・1094・1148 | 残す（2026-10-22） |
| KPIデータなし | `data` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json` | 1439・2006・2016 | 残す（2026-10-22） |
| — | `description` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2623 | 残す（2026-10-22） |
| — | `deviation_pct` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1800 | 残す（2026-10-22） |
| 有効 | `effective` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 1119・1121・1515・1515 | 残す（2026-10-22） |
| — | `entries` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 929・988 | 残す（2026-10-22） |
| — | `entry_condition` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2456・2533 | 残す（2026-10-22） |
| ⚠ KPI未設定 | `entry_date` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 1211・1254・1301・1697 | 残す（2026-10-22） |
| — | `entry_price` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 1092・1092・1212・1255 | 残す（2026-10-22） |
| 投資テーゼ | `entry_story` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 1695・2515 | 残す（2026-10-22） |
| AI視点 | `exit_condition` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 1258・1258・1679・2457 | 残す（2026-10-22） |
| ⚠ 楽観バイアス警告 | `exit_distance` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1132・1654・1729 | 残す（2026-10-22） |
| エグジット条件との距離感 | `exit_distance_reason` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1133・1655・1767 | 残す（2026-10-22） |
| AI視点 | `exit_guide` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 1679・1696・2516 | 残す（2026-10-22） |
| EX閾値: | `exit_threshold` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2626 | 残す（2026-10-22） |
| >取得日時: | `fetched_at` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 2405 | 残す（2026-10-22） |
| ' + esc(d.quarter \|\| '') + ' / 提出日: ' +  | `filing_date` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 1520・2342 | 残す（2026-10-22） |
| レビュー予測 → | `forecast_target` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1815 | 残す（2026-10-22） |
| 1年後 | `future_values` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 1152・1159・1484・2118 | 残す（2026-10-22） |
| — | `generated_at` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json` | 1719・1719 | 残す（2026-10-22） |
| 指定された四半期のレビューが見つかりません。 | `health_score` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1079・1223・1223・1312 | 残す（2026-10-22） |
| — | `health_score_at_action` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 1886・1886 | 残す（2026-10-22） |
| — | `holding_period` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2458・2535 | 残す（2026-10-22） |
| — | `intrinsic_value` | 画面の中で作る値 | 1541・1541 | 残す（2026-10-22） |
| — | `intrinsic_value_per_share` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 1013 | 残す（2026-10-22） |
| ▶ Item 4 原文（English）を表示 | `item4_excerpt` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 2393・2400 | 残す（2026-10-22） |
| 有効 | `item4_excerpt_ja` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 1517・1522・1522・2385 | 残す（2026-10-22） |
| シナリオ根拠</div><div style= | `key_assumptions` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 2207・2308 | 残す（2026-10-22） |
| — | `kpi_current` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2224 | 残す（2026-10-22） |
| — | `kpi_forecast_available` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1785 | 残す（2026-10-22） |
| 1年後 | `kpi_forecasts` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 2223・2262・2263 | 残す（2026-10-22） |
| — | `kpi_format` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2226 | 残す（2026-10-22） |
| — | `kpi_layer1_keys` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2225 | 残す（2026-10-22） |
| KPIデータ読込中... | `kpis` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>`docs/portfolio/tail/data/kpi/{T}_layer3.json` | 1206・1206・1434・2004 | 残す（2026-10-22） |
| 前提 | `label` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 2192・2296 | 残す（2026-10-22） |
| — | `matchable` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1785 | 残す（2026-10-22） |
| — | `material_weaknesses` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 2348・2348・2349 | 残す（2026-10-22） |
| — | `name` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2620 | 残す（2026-10-22） |
| — | `new_thesis` | 画面の中で作る値 | 1987 | 残す（2026-10-22） |
| 他' + conMore + '件 | `next_kpis` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1629・1732 | 残す（2026-10-22） |
| — | `old_thesis` | 画面の中で作る値 | 1986 | 残す（2026-10-22） |
| 最終営業利益率 | `operating_margin` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2186 | 残す（2026-10-22） |
| ⚠ 楽観バイアス警告 | `optimism_bias_warning` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1109・1633・1634・1634 | 残す（2026-10-22） |
| — | `positions` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions_index.json` | 869・988 | 残す（2026-10-22） |
| — | `positives` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1619・1621・1721 | 残す（2026-10-22） |
| — | `predicted` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1807 | 残す（2026-10-22） |
| — | `predictions` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1794 | 残す（2026-10-22） |
| — | `price` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 1884・1884・1977 | 残す（2026-10-22） |
| — | `proposed_kpis` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json` | 2608・2671 | 残す（2026-10-22） |
| — | `q` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 1604・1611 | 残す（2026-10-22） |
| KPIデータなし | `quarter` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json` | 963・963・966・973 | 残す（2026-10-22） |
| 読み込み中… | `quarters` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/index.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 1385・1385・1385・2428 | 残す（2026-10-22） |
| — | `queue` | この段階の書き手あり<br>`docs/portfolio/tail/data/review_queue.json` | 957 | 残す（2026-10-22） |
| — | `rationale` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 2199・2302 | 残す（2026-10-22） |
| — | `reason` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 1898 | 残す（2026-10-22） |
| — | `recommendation` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1080・1224・1313・1548 | 残す（2026-10-22） |
| 関連: | `related_exit_condition` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2627 | 残す（2026-10-22） |
| レビュー予測 → | `review_quarter` | この段階の書き手あり<br>`docs/portfolio/tail/data/prediction_history.json` | 1815 | 残す（2026-10-22） |
| — | `revised_item` | 画面の中で作る値 | 1985 | 残す（2026-10-22） |
| — | `risk_factors` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 2208・2309 | 残す（2026-10-22） |
| — | `scenario` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/prediction_history.json` | 1785 | 残す（2026-10-22） |
| レビューデータなし | `scenarios` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 2086 | 残す（2026-10-22） |
| — | `shares` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 991・992・992・996 | 残す（2026-10-22） |
| — | `significant_deficiencies` | この段階の書き手あり<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json`<br>`docs/portfolio/tail/data/ctrl/{T}/{Q}.json` | 2370・2370・2371 | 残す（2026-10-22） |
| ソース: | `source` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2628 | 残す（2026-10-22） |
| 指定された四半期のレビューが見つかりません。 | `stage1` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 1076・1222・1311・1546 | 残す（2026-10-22） |
| レビューデータなし | `stage2` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 2085 | 残す（2026-10-22） |
| workflow_dispatch失敗 ( | `status` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json`<br>`docs/portfolio/tail/data/review_queue.json` | 884・888・957・1044 | 残す（2026-10-22） |
| — | `strategy_name` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 1275・1297・1570・2455 | 残す（2026-10-22） |
| — | `summary` | この段階の書き手あり<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`<br>`docs/portfolio/tail/data/{item_key}/{T}/latest.json` | 1170・1640・1759 | 残す（2026-10-22） |
| — | `tags` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 1887 | 残す（2026-10-22） |
| — | `tanuki_score` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 1015 | 残す（2026-10-22） |
| ターミナル成長率 | `terminal_growth` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json` | 2182 | 残す（2026-10-22） |
| 投資テーゼ | `thesis` | この段階の書き手あり<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 1297・1570・1694・2514 | 残す（2026-10-22） |
| 最終ログ: | `ticker` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json`<br>`docs/portfolio/tail/data/ctrl/{T}/latest.json` | 877・877・885・889 | 残す（2026-10-22） |
| ログ未登録 | `timestamp` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json` | 936・946・1882 | 残す（2026-10-22） |
| 最終ログ: | `type` | この段階の書き手あり<br>`docs/portfolio/tail/data/journal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 937・1045・1056・1056 | 残す（2026-10-22） |
| — | `unit` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>`docs/portfolio/tail/data/kpi/{T}_layer3.json` | 1444・2019 | 残す（2026-10-22） |
| — | `upside_percent` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 1014・1100・1101・1216 | 残す（2026-10-22） |
| 📝 判断ログ登録 — | `value` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi/{T}_layer2.json`<br>`docs/portfolio/tail/data/kpi/{T}_layer3.json` | 773・834・835・854 | 残す（2026-10-22） |
| 警戒: | `warning_threshold` | この段階の書き手あり<br>`docs/portfolio/tail/data/kpi_proposals/{T}_proposal.json`<br>`docs/portfolio/tail/data/positions/{T}_thesis.json` | 2625 | 残す（2026-10-22） |
| 加重成長率（使用値） | `weighted_growth` | この段階の書き手あり<br>`docs/portfolio/scenario/{T}/{sc_name}.json` | 2178 | 残す（2026-10-22） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/portfolio/tail/decision_log.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| 共通テーマ。ページ内の<style>より前に読み、ページ側の :root の色を残す（NAV-LINK-INTEGRITY-1） | `entries`（この段階の書き手あり）<br>`health_score_at_action`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`shares`（この段階の書き手あり）<br>`tags`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`timestamp`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |

### `docs/portfolio/tail/detail.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| 認証画面 | `value`（この段階の書き手あり） |  |
| アプリシェル（認証後に表示） | `base`（この段階の書き手あり）<br>`bear`（この段階の書き手あり）<br>`bull`（この段階の書き手あり）<br>`call2`（この段階の書き手あり）<br>`company`（この段階の書き手あり）<br>`components`（ほかの書き手あり）<br>`concerns`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`data`（この段階の書き手あり）<br>`effective`（この段階の書き手あり）<br>`entry_condition`（この段階の書き手あり）<br>`entry_date`（この段階の書き手あり）<br>`entry_price`（この段階の書き手あり）<br>`entry_story`（この段階の書き手あり）<br>`entry_story_progress`（この段階の書き手あり）<br>`excerpt`（この段階の書き手あり）<br>`excerpt_ja`（この段階の書き手あり）<br>`exit_condition`（この段階の書き手あり）<br>`exit_distance`（この段階の書き手あり）<br>`exit_distance_reason`（この段階の書き手あり）<br>`exit_guide`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`five_perspectives`（この段階の書き手あり）<br>`generated_at`（この段階の書き手あり）<br>`health_score`（この段階の書き手あり）<br>`historical_analogy`（この段階の書き手あり）<br>`implication`（この段階の書き手あり）<br>`intrinsic_value`（画面の中で作る値）<br>`intrinsic_value_per_share`（ほかの書き手あり）<br>`key_assumptions`（この段階の書き手あり）<br>`kpi_current`（この段階の書き手あり）<br>`kpi_forecasts`（この段階の書き手あり）<br>`kpi_format`（この段階の書き手あり）<br>`kpi_layer1_keys`（この段階の書き手あり）<br>`kpis`（この段階の書き手あり）<br>`label`（ほかの書き手あり）<br>`macro_implications`（この段階の書き手あり）<br>`market_attention`（この段階の書き手あり）<br>`material_weaknesses`（この段階の書き手あり）<br>`next_kpis`（この段階の書き手あり）<br>`next_review_focus`（この段階の書き手あり）<br>`operating_margin_terminal`（この段階の書き手あり）<br>`optimism_bias_warning`（この段階の書き手あり）<br>`outcome`（この段階の書き手あり）<br>`period`（この段階の書き手あり）<br>`positions`（この段階の書き手あり）<br>`positives`（この段階の書き手あり）<br>`quarter`（この段階の書き手あり）<br>`queue`（この段階の書き手あり）<br>`recommendation`（この段階の書き手あり）<br>`revenue_growth_y1`（この段階の書き手あり）<br>`revenue_growth_y2`（この段階の書き手あり）<br>`revenue_growth_y3`（この段階の書き手あり）<br>`risk_factors`（この段階の書き手あり）<br>`scenarios`（この段階の書き手あり）<br>`significant_deficiencies`（この段階の書き手あり）<br>`similarity`（この段階の書き手あり）<br>`source`（この段階の書き手あり）<br>`stage1`（この段階の書き手あり）<br>`stage2`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`strategy_name`（この段階の書き手あり）<br>`summary`（この段階の書き手あり）<br>`thesis`（この段階の書き手あり）<br>`thesis_questions`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`unit`（この段階の書き手あり）<br>`upside_percent`（ほかの書き手あり）<br>`value`（この段階の書き手あり） |  |
| （区画を決められない） | `base_intrinsic_value`（この段階の書き手あり）<br>`future_values`（この段階の書き手あり）<br>`kpi_forecasts`（この段階の書き手あり） |  |

### `docs/portfolio/tail/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| ── Auth screen ── | `base`（この段階の書き手あり）<br>`bear`（この段階の書き手あり）<br>`bull`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`effective`（この段階の書き手あり）<br>`entry_price`（この段階の書き手あり）<br>`exit_distance`（この段階の書き手あり）<br>`exit_distance_reason`（この段階の書き手あり）<br>`future_values`（この段階の書き手あり）<br>`health_score`（この段階の書き手あり）<br>`optimism_bias_warning`（この段階の書き手あり）<br>`positions`（この段階の書き手あり）<br>`recommendation`（この段階の書き手あり）<br>`stage1`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`summary`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`upside_percent`（ほかの書き手あり）<br>`value`（この段階の書き手あり） |  |
| ── Decision Log link ── | `entries`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`timestamp`（この段階の書き手あり）<br>`type`（この段階の書き手あり） |  |
| ── Usage flow guide ── | `entry_condition`（この段階の書き手あり）<br>`entry_date`（この段階の書き手あり）<br>`entry_price`（この段階の書き手あり）<br>`entry_story`（この段階の書き手あり）<br>`exit_condition`（この段階の書き手あり）<br>`exit_guide`（この段階の書き手あり）<br>`holding_period`（この段階の書き手あり）<br>`positions`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`strategy_name`（この段階の書き手あり）<br>`thesis`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`timestamp`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| ── Detail Modal ─────────────────────────────────── | `Y1_growth`（この段階の書き手あり）<br>`Y2_growth`（この段階の書き手あり）<br>`Y3_growth`（この段階の書き手あり）<br>`accuracy`（この段階の書き手あり）<br>`actual`（この段階の書き手あり）<br>`assumptions`（この段階の書き手あり）<br>`base`（この段階の書き手あり）<br>`base_intrinsic_value`（この段階の書き手あり）<br>`bear`（この段階の書き手あり）<br>`bull`（この段階の書き手あり）<br>`call2`（この段階の書き手あり）<br>`concerns`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`data`（この段階の書き手あり）<br>`deviation_pct`（この段階の書き手あり）<br>`effective`（この段階の書き手あり）<br>`entry_date`（この段階の書き手あり）<br>`entry_price`（この段階の書き手あり）<br>`entry_story`（この段階の書き手あり）<br>`exit_condition`（この段階の書き手あり）<br>`exit_distance`（この段階の書き手あり）<br>`exit_distance_reason`（この段階の書き手あり）<br>`exit_guide`（この段階の書き手あり）<br>`fetched_at`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`forecast_target`（この段階の書き手あり）<br>`generated_at`（この段階の書き手あり）<br>`health_score`（この段階の書き手あり）<br>`intrinsic_value`（画面の中で作る値）<br>`item4_excerpt`（この段階の書き手あり）<br>`item4_excerpt_ja`（この段階の書き手あり）<br>`key_assumptions`（この段階の書き手あり）<br>`kpi_current`（この段階の書き手あり）<br>`kpi_forecast_available`（この段階の書き手あり）<br>`kpi_forecasts`（この段階の書き手あり）<br>`kpi_format`（この段階の書き手あり）<br>`kpi_layer1_keys`（この段階の書き手あり）<br>`kpis`（この段階の書き手あり）<br>`label`（ほかの書き手あり）<br>`matchable`（この段階の書き手あり）<br>`material_weaknesses`（この段階の書き手あり）<br>`next_kpis`（この段階の書き手あり）<br>`operating_margin`（この段階の書き手あり）<br>`optimism_bias_warning`（この段階の書き手あり）<br>`positives`（この段階の書き手あり）<br>`predicted`（この段階の書き手あり）<br>`predictions`（この段階の書き手あり）<br>`q`（ほかの書き手あり）<br>`quarter`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり）<br>`rationale`（この段階の書き手あり）<br>`recommendation`（この段階の書き手あり）<br>`review_quarter`（この段階の書き手あり）<br>`risk_factors`（この段階の書き手あり）<br>`scenario`（この段階の書き手あり）<br>`scenarios`（この段階の書き手あり）<br>`significant_deficiencies`（この段階の書き手あり）<br>`stage1`（この段階の書き手あり）<br>`stage2`（この段階の書き手あり）<br>`strategy_name`（この段階の書き手あり）<br>`summary`（この段階の書き手あり）<br>`terminal_growth`（この段階の書き手あり）<br>`thesis`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`unit`（この段階の書き手あり）<br>`upside_percent`（ほかの書き手あり）<br>`value`（この段階の書き手あり）<br>`weighted_growth`（この段階の書き手あり） |  |
| ── Journal Modal ───────────────────────────────── | `value`（この段階の書き手あり） |  |
| entry / exit 追加項目 | `value`（この段階の書き手あり） |  |
| thesis_revision 追加項目 | `health_score`（この段階の書き手あり）<br>`new_thesis`（画面の中で作る値）<br>`old_thesis`（画面の中で作る値）<br>`price`（この段階の書き手あり）<br>`revised_item`（画面の中で作る値）<br>`shares`（この段階の書き手あり）<br>`stage1`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| ── KPI Setup Modal ──────────────────────────────── | `auto_fetchable`（この段階の書き手あり）<br>`description`（この段階の書き手あり）<br>`exit_threshold`（この段階の書き手あり）<br>`name`（この段階の書き手あり）<br>`proposed_kpis`（この段階の書き手あり）<br>`related_exit_condition`（この段階の書き手あり）<br>`source`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`warning_threshold`（この段階の書き手あり） |  |
| （JSで作る要素 #core-tbody） | `entry_date`（この段階の書き手あり）<br>`entry_price`（この段階の書き手あり）<br>`health_score`（この段階の書き手あり）<br>`kpis`（この段階の書き手あり）<br>`recommendation`（この段階の書き手あり）<br>`stage1`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`upside_percent`（ほかの書き手あり） |  |
| （JSで作る要素 #db-ctrl-zone） | `effective`（この段階の書き手あり）<br>`filing_date`（この段階の書き手あり）<br>`item4_excerpt_ja`（この段階の書き手あり）<br>`quarter`（この段階の書き手あり）<br>`quarters`（この段階の書き手あり） |  |
| （JSで作る要素 #db-dcf-zone） | `base`（この段階の書き手あり）<br>`bear`（この段階の書き手あり）<br>`bull`（この段階の書き手あり）<br>`current_price`（この段階の書き手あり）<br>`future_values`（この段階の書き手あり） |  |
| （JSで作る要素 #db-kpi-zone） | `data`（この段階の書き手あり）<br>`kpis`（この段階の書き手あり）<br>`quarter`（この段階の書き手あり）<br>`unit`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| （JSで作る要素 #jnl-filter-ticker） | `ticker`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| （JSで作る要素 #jnl-filter-type） | `health_score_at_action`（この段階の書き手あり）<br>`price`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`shares`（この段階の書き手あり）<br>`tags`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`timestamp`（この段階の書き手あり）<br>`type`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| （JSで作る要素 #kpi-add-desc） | `value`（この段階の書き手あり） |  |
| （JSで作る要素 #kpi-add-exit） | `value`（この段階の書き手あり） |  |
| （JSで作る要素 #kpi-add-name） | `proposed_kpis`（この段階の書き手あり） |  |
| （JSで作る要素 #kpi-add-src） | `value`（この段階の書き手あり） |  |
| （JSで作る要素 #kpi-add-warn） | `value`（この段階の書き手あり） |  |
| （JSで作る要素 #sat-tbody） | `current_price`（この段階の書き手あり）<br>`entry_date`（この段階の書き手あり）<br>`entry_price`（この段階の書き手あり）<br>`exit_condition`（この段階の書き手あり）<br>`strategy_name`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり） |  |
| （区画を決められない） | `avg_cost`（ほかの書き手あり）<br>`base_intrinsic_value`（この段階の書き手あり）<br>`brokers`（ほかの書き手あり）<br>`components`（ほかの書き手あり）<br>`current_price`（この段階の書き手あり）<br>`entries`（この段階の書き手あり）<br>`entry_condition`（この段階の書き手あり）<br>`entry_date`（この段階の書き手あり）<br>`entry_price`（この段階の書き手あり）<br>`exit_condition`（この段階の書き手あり）<br>`future_values`（この段階の書き手あり）<br>`holding_period`（この段階の書き手あり）<br>`intrinsic_value_per_share`（ほかの書き手あり）<br>`kpi_forecasts`（この段階の書き手あり）<br>`positions`（この段階の書き手あり）<br>`quarter`（この段階の書き手あり）<br>`queue`（この段階の書き手あり）<br>`shares`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`strategy_name`（この段階の書き手あり）<br>`tanuki_score`（ほかの書き手あり）<br>`ticker`（この段階の書き手あり）<br>`upside_percent`（ほかの書き手あり） |  |

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | TAILか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。TAILのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | TAILを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:723`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:866`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:894` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:605` | はい |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1634`<br>`common/sec_data/report_consistency_check.py:1775`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:284` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | はい |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | はい |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:673`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:865`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:898`<br>`src/value/tanuki_valuation/pipeline.py:900`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:1355`<br>`src/value/tanuki_valuation/pipeline.py:1382`<br>`src/value/tanuki_valuation/pipeline.py:1387`<br>`src/value/tanuki_valuation/pipeline.py:1433`<br>`src/value/tanuki_valuation/pipeline.py:1738`<br>`src/value/tanuki_valuation/pipeline.py:1916` | いいえ |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | いいえ |
| AI（Grok）の呼び方（APIの宛先・モデル・再試行） | 6 | `src/market/market_pulse/news_headlines.py:41`<br>`src/tail/kpi_proposer.py:71`<br>`src/tail/kpi_proposer.py:90`<br>`src/tail/quarterly_review_generator.py:78`<br>`src/tail/quarterly_review_generator.py:89`<br>`src/tail/sec_ctrl_fetcher.py:50`<br>`src/tail/text_kpi_extractor.py:47`<br>`src/tail/text_kpi_extractor.py:68`<br>`src/value/tanuki_valuation/calculator/fcf_outlier_ai.py:35`<br>`src/value/tanuki_valuation/calculator/fcf_outlier_ai.py:56` | はい |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（src/tail）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（12ファイル・6,912行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `edgar_rss_monitor.py` | 638 | `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` | `common/sec_data/report_consistency_check.py`<br>`scripts/analysis/market_pulse_redesign.py`<br>`src/market/market_pulse/sector_rotation.py` | — | 2件 |
| `kpi_proposer.py` | 854 | — | `common/sec_data/report_consistency_check.py` | — | 0件 |
| `prediction_tracker.py` | 285 | `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` | — | — | 1件 |
| `quarterly_review_generator.py` | 1,811 | `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` | `common/sec_data/report_consistency_check.py` | — | 3件 |
| `review_status.py` | 71 | — | — | `src/tail/prediction_tracker.py`<br>`src/tail/quarterly_review_generator.py`<br>`src/tail/tail_dcf_bridge.py` | 1件 |
| `sec_ctrl_fetcher.py` | 478 | `.github/workflows/TANUKI_TAIL_SEC_Ctrl.yml` | — | `src/tail/sec_items_fetcher.py` | 1件 |
| `sec_items_fetcher.py` | 677 | `.github/workflows/TANUKI_TAIL_SEC_Items.yml` | — | — | 3件 |
| `tail_dcf_bridge.py` | 349 | `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` | — | `src/tail/quarterly_review_generator.py` | 3件 |
| `text_kpi_extractor.py` | 713 | `.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` | — | — | 1件 |
| `thesis_utils.py` | 48 | — | — | `src/tail/kpi_proposer.py`<br>`src/tail/quarterly_review_generator.py` | 0件 |
| `workflow_write.py` | 230 | `.github/workflows/TANUKI_TAIL_Position_Write.yml` | — | — | 1件 |
| `xbrl_segment_fetcher.py` | 758 | `.github/workflows/TANUKI_TAIL_KPI_Update.yml`<br>`.github/workflows/TANUKI_TAIL_RSS_Monitor.yml` | `common/sec_data/dimension_aggregate_fetcher.py` | `src/tail/kpi_proposer.py` | 3件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
