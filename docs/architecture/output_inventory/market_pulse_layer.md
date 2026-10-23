# Market Pulse（src/market/market_pulse・docs/market-monitor/market-pulse）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`market_pulse_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`src/market/market_pulse/`・`docs/market-monitor/market-pulse/index.html`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-23、Koichiさんの判断）。手で動かすスクリプト4つを消し、出力はすべて残す。sp500_tickers.json・nyse_holidays.json は市場データ層・MACRO PULSE との二重で、R-A の終わりの見直し（論点⑤）で扱う。誰も読まないキー48は残す（下の注記）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 7（ファイル 6・人が見るもの 1）／読み手0件 0／テストだけ 0／1件 2／2件以上 4／規則の二重 18（検出式 19種のうち。Market Pulseのファイルを含むもの 6）

**画面の項目**: 画面が読むキー 152（この段階の書き手あり 151・ほかの書き手あり 0・画面の中で作る値 0・**書き手なし 1**）／書き手があるのに誰も読まないキー **50**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 2件／解決できた読み込み 22件＋ファイル名で当たった行 18件（4.8%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

フォルダを決めきれない行（ファイル名で当たった行）:
- `scripts/analysis/market_pulse_redesign.py` 行 58（`docs/market-monitor/market-pulse/data/market_data.json`）: `md = json.loads(subprocess.run(["git", "-C", REPO, "show", f"{DATA_REV}:docs/market-monitor/market-pulse/data/market_dat`
- `scripts/analysis/market_pulse_redesign.py` 行 60（`docs/market-monitor/market-pulse/data/sp500_tickers.json`）: `sp500 = json.loads(subprocess.run(["git", "-C", REPO, "show", f"{DATA_REV}:docs/market-monitor/market-pulse/data/sp500_t`

## 消したもの（段階2、2026-10-23）

- `src/market/market_pulse/backfill_implb.py・backfill_implc.py・backfill_stage_conclusions.py・backfill_tech_pulse.py`: 削除（2026-10-23、Koichiさんの判断）。手で動かす一過性の付け足し・補完で適用済み（implb は eb6cf8d98d〈2026-10-01〉、implc は 01634073a9・747936b56c〈2026-10-09〉、stage_conclusions は 59406c6131・14b03af90f〈2026-09-30〉、tech_pulse は 140ce1877c〈2026-05-03〉・c50a9f3ded〈2026-05-21〉）。ワークフロー・ほかのコード・テストからの参照は無かった。過去の記録（BACKLOG_DONE.md・MARKET_PULSE_REDESIGN.md の経緯）は残す

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `docs/market-monitor/market-pulse/data/market_data.csv`<br>（1ファイル） | market_data.json の指標を平らにした1日1行（指標_value 等）。**AIの出力（Grok）**: summary 列（judgment 列は summary から取り出す） | `collect_and_send.py:save_data_to_json_and_csv`<br>`collect_and_send.py:save_data_to_json_and_csv` | 1 | market-monitor/market-pulse/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由） | 効く: Market Pulse（市況の判定・8段階の結論・天気） | AS-IS-386 | 残す（2026-10-23） |
| 1件 | `src/market/market_pulse/02_market_rss.txt`<br>（1ファイル） | 市況のニュースを取るRSSの一覧（Google Newsの検索7本）。**コードの書き手は無い（人の手で書く静的なファイル）**。collect_and_send.py の get_market_news が読む |  | 1 | market-monitor/market-pulse/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由） | 効く: Market Pulse（市況の判定・8段階の結論・天気） | — | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/market-pulse/data/breadth_data.json`<br>（1ファイル） | S&P500の構成銘柄の騰落・新高値・新安値・50日線の上の割合（breadth_calculator.py）。collect_and_send.py が読んで market_data.json に写す | `breadth_calculator.py:save_breadth` | 4 | market-monitor/market-pulse/index.html（経由）<br>value-monitor/tanuki_score/index.html（経由） | 効く: Market Pulse（市況の判定・8段階の結論・天気） | AS-IS-334 | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/market-pulse/data/market_data.json`<br>（1ファイル） | 1日1エントリの市況（指標・センチメント・F&G・Tech Pulse・資金の流れ・クレジット・8段階の結論・天気・data_quality 等）。画面が読む。**AIの出力（Grok）**: summary・comments_history の summary・haiku（judgment は summary から取り出す）、headlines の items の title_ja・market と translation・relevance。2026-09-26からの再設計（MARKET_PULSE_REDESIGN.md、実装A〜C）で足したキー: stage_conclusions・weather・data_quality・ai_facts・hindenburg・sector_rotation・semis_m7・commodity_fx・watch_list・headlines・calendar・futures | `collect_and_send.py:save_data_to_json_and_csv` | 8・テスト3 | market-monitor/market-pulse/index.html（直接）<br>portfolio/index.html（経由）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/index.html（経由）<br>value-monitor/adjusted_eps_analyzer/stock.html（経由）<br>value-monitor/hypecore/detail.html（経由）<br>value-monitor/stonks-silo/index.html（経由）<br>value-monitor/tanuki_score/index.html（直接）<br>value-monitor/tanuki_valuation/index.html（経由）<br>value-monitor/tanuki_valuation/stock.html（経由） | 効く: 理論株価・判定（TANUKI VALUATION）・Market Pulse（市況の判定・8段階の結論・天気）・Market Pulseの画面 | AS-IS-300・AS-IS-388・AS-IS-390 | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/market-pulse/data/nyse_holidays.json`<br>（1ファイル） | 前年〜翌年のNYSEの休場日（pandas_market_calendars から毎回書く）。画面の休場の行の表示に使う。**休場日の情報は市場データ層（fetcher.py・daily_guard.py が pandas_market_calendars を直接）と MACRO PULSE（05_main.py の us_holidays、手で書いた祝日の規則）にも別にある** | `collect_and_send.py:write_nyse_holidays` | 3 | market-monitor/market-pulse/index.html（直接） | 効く: Market Pulseの画面 | — | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/market-pulse/data/sp500_tickers.json`<br>（1ファイル） | S&P500の構成銘柄の一覧の控え（7日以内なら取り直さない）。**市場データ層の common/market_data/_sp500_constituents_cache.json と同じ情報の別の写し**（取り方も同じ。2026-10-23 は503銘柄で一致。fetcher.py の get_sp500_constituents は意図的な独立の実装と書く） | `breadth_calculator.py:get_sp500_tickers` | 3 | — | 効く: Market Pulse（市況の判定・8段階の結論・天気） | — | 残す（2026-10-23） |

### 人が見るもの（Step Summary・Discord・メール）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `メール（GMAIL_PASSWORD・GMAIL_USER）` | その日の市況の判定・AIの見解のGmailへの送信（自分あて） | `collect_and_send.py:send_email` | .github/workflows/Market_Pulse_Update.yml |

## 読み手の内訳

### `docs/market-monitor/market-pulse/data/market_data.csv`（1件）
- 起動元: .github/workflows/Market_Pulse_Update.yml
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 27
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/value-monitor/tanuki_score/index.html`

### `src/market/market_pulse/02_market_rss.txt`（1件）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 19・1067
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/value-monitor/tanuki_score/index.html`

### `docs/market-monitor/market-pulse/data/breadth_data.json`（2件以上）
- 起動元: .github/workflows/Market_Pulse_Update.yml
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 71・175・366・409・458（経由 `browser_checks/check_dependency_map.py::expected_hindenburg_active`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 44
- Python・保存済みファイル: `src/market/market_pulse/breadth_calculator.py` 行 33
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 28・221・368・1877・1936（経由 `src/market/market_pulse/collect_and_send.py::_load_latest_breadth`）
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/value-monitor/tanuki_score/index.html`

### `docs/market-monitor/market-pulse/data/market_data.json`（2件以上）
- 起動元: .github/workflows/Market_Pulse_Update.yml
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 68・146・157・177・331・353・366・399・409・412・448・458（経由 `browser_checks/check_dependency_map.py::expected_breadth_summary`、`browser_checks/check_dependency_map.py::expected_fear_greed_score`、`browser_checks/check_dependency_map.py::expected_hindenburg_active` ほか）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 41・153・337・810・914（経由 `browser_checks/market_pulse_elements.py::load_entries`）
- Python・保存済みファイル: `common/system_health.py` 行 158・1041・1110（経由 `common/system_health.py::check_d_actions`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 26・552・581・600・668・1883・1893・1896・1905・1958（経由 `src/market/market_pulse/collect_and_send.py::_load_div_history`、`src/market/market_pulse/collect_and_send.py::_load_prev_tech_pulse_score`、`src/market/market_pulse/collect_and_send.py::_load_recent_entries` ほか）
- Python・保存済みファイル: `src/portfolio/snapshot.py` 行 34・35・142（経由 `src/portfolio/snapshot.py::main`）
- Python・保存済みファイル: `src/value/tanuki_valuation/pipeline.py` 行 490・604・608・853・1144（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_live_fg`）
- JS・HTML・保存済みファイル: `docs/market-monitor/market-pulse/index.html` 行 1393
- JS・HTML・保存済みファイル: `docs/value-monitor/tanuki_score/index.html` 行 239
- テスト・保存済みファイル: `tests/test_pipeline_logic.py` 行 332・348・364・396・421・1579・1607・1677・1696・1723・1742・1762 ほか6（経由 `src/value/tanuki_valuation/pipeline.py::TanukiValuationPipeline._load_live_fg`）
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 473（経由 `common/system_health.py::check_d_actions`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 290・298・430・443（経由 `common/system_health.py::check_d_actions`）
- 画面（経由（src/portfolio/snapshot.py → docs/portfolio/data/history.json））: `docs/portfolio/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/portfolio/tail/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/adjusted_eps_analyzer/stock.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/hypecore/detail.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/{T}/latest.json））: `docs/value-monitor/stonks-silo/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/tickers.json））: `docs/value-monitor/tanuki_valuation/index.html`
- 画面（経由（src/value/tanuki_valuation/pipeline.py → docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json））: `docs/value-monitor/tanuki_valuation/stock.html`

### `docs/market-monitor/market-pulse/data/nyse_holidays.json`（2件以上）
- 起動元: .github/workflows/Market_Pulse_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `from`(1)・`holidays`(2)・`source`(2)・`to`(1)
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 399・409（経由 `browser_checks/market_pulse_elements.py::expected_holiday_rows`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 218・221・539（経由 `browser_checks/market_pulse_elements.py::expected_holiday_rows`）
- JS・HTML・保存済みファイル: `docs/market-monitor/market-pulse/index.html` 行 1392

### `docs/market-monitor/market-pulse/data/sp500_tickers.json`（2件以上）
- 起動元: .github/workflows/Market_Pulse_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `count`(1)・`fetched_at`(1)・`tickers`(2)
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 412・448・458（経由 `browser_checks/market_pulse_elements.py::classify_base_dates`、`browser_checks/market_pulse_elements.py::run_derivation_checks`）
- Python・保存済みファイル: `browser_checks/market_pulse_elements.py` 行 47・864・944
- Python・保存済みファイル: `src/market/market_pulse/breadth_calculator.py` 行 34

## 画面の項目

画面（`docs/market-monitor/market-pulse/index.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）

- `m7_missing`（`docs/market-monitor/market-pulse/index.html` 行 1043・1043）: 判定: SOXはS&amp;P500との差±1.0pt、M7は±0.5ptで「け

### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `ad_ratio`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `ai_facts`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `backfilled_at`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 過去の行の記録（2026-10-09 に最新のエントリの見出しを取り直した印。書き手の backfill_implc.py は2026-10-23に削除し、新しい行には付かない）
- `base_n`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `breadth_date`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `conditions`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `distribution`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `divergence_vs_sp`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `earnings_tickers`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `etf`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `etf_change_percent`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `false`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: その日の見出しの「市況と関係あり」の判定（Grok）の件数の記録。読んでいたのは2026-10-23に消した backfill_implc.py（結果の表示）だけ
- `hy_bond`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `ig_bond`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `ivw_minus_ive_pt`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `mcclellan_negative`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `median_pct`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `momentum_lag_weeks`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `new_highs`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `new_lows`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `nh_le_2x_nl`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `nh_nl`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `nh_nl_both_2_2pct`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `one_month_ago`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `one_week_ago`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `prev_close`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `prev_date`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `ratio_weeks`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `raw_change`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `raw_change_percent`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `risk_avg_pct`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `rsp_spy_divergence`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `safe_avg_pct`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `sentiment_label`（`docs/market-monitor/market-pulse/data/market_data.csv`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `sentiment_score`（`docs/market-monitor/market-pulse/data/market_data.csv`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `sox_date`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `sp500_ma200`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `sp500_ma_dev`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `sp500_uptrend_50d`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `stage1_vix`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `stage3`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `stage4`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `stocks_ahead_of_base_date`（`docs/market-monitor/market-pulse/data/breadth_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `threshold_pt`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `true`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: その日の見出しの「市況と関係あり」の判定（Grok）の件数の記録。読んでいたのは2026-10-23に消した backfill_implc.py（結果の表示）だけ
- `ultra_short`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `vix_level`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `vxn_available`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `week`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力
- `zscore`（`docs/market-monitor/market-pulse/data/market_data.json`）: 残す: 段階の結論の根拠の値とその日に取った値で、過去の日の分は取り直せない。Market Pulse の再設計・画面のストーリー化（[[UI-STORY-REDESIGN-1]]）の入力

### `docs/market-monitor/market-pulse/index.html`（152項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| データ取得不可 | `above` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1266・1267・1271・1271 | 残す（2026-10-23） |
| データ取得不可 | `active` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1289・1290・1292 | 残す（2026-10-23） |
| VIX指数 | `ad_line` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637 | 残す（2026-10-23） |
| VIX指数 | `ad_ratio_5d` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・862・862・884 | 残す（2026-10-23） |
| VIX短期vs中期 | `advances` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 859・859・859・884 | 残す（2026-10-23） |
| シグナル | `as_of` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 998・1025・1130・1132 | 残す（2026-10-23） |
| 今日 | `asset_flow` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1316・1356・1369・1377 | 残す（2026-10-23） |
| 最新値 | `bar_time_utc` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1096 | 残す（2026-10-23） |
| 件数不足 | `base_up_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995・995 | 残す（2026-10-23） |
| VIX指数 | `breadth` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・637・637・637 | 残す（2026-10-23） |
| — | `buy_checklist` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1215 | 残す（2026-10-23） |
| 銘柄 | `calculated` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057・1057 | 残す（2026-10-23） |
| — | `calendar` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1100 | 残す（2026-10-23） |
| 米10年債 | `change` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 717・717 | 残す（2026-10-23） |
| 米10年債 | `change_bp` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 717 | 残す（2026-10-23） |
| セクター | `change_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1025・1025・1025・1025 | 残す（2026-10-23） |
| 米10年債 | `change_percent` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 718・1152・1152 | 残す（2026-10-23） |
| 期待する終値日（実行時点で取引が終わった直近のNYSE取引日）: | `checked_at` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1132・1132 | 残す（2026-10-23） |
| S&P500の出来高÷前営業日の出来高と、前日比 | `chg_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 840・840・840 | 残す（2026-10-23） |
| — | `comments_history` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 802 | 残す（2026-10-23） |
| — | `commodity_fx` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1038 | 残す（2026-10-23） |
| — | `components` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 955・957・1166・1216 | 残す（2026-10-23） |
| VIX指数 | `contango` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 849 | 残す（2026-10-23） |
| 最新値 | `contract` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1096 | 残す（2026-10-23） |
| 米10年債 | `contract_roll` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 721 | 残す（2026-10-23） |
| 株 | `credit` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 769・770・771・772 | 残す（2026-10-23） |
| データ取得不可 | `current` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1278・1279・1280 | 残す（2026-10-23） |
| ）`:"最新"} | `data_freshness` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1131 | 残す（2026-10-23） |
| — | `data_quality` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1111・1119・1126・1406 | 残す（2026-10-23） |
| }時点の値） | `date` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.csv` | 676・699・726・756 | 残す（2026-10-23） |
| 該当なし | `date_et` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1103 | 残す（2026-10-23） |
| 該当なし | `date_local` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1103 | 残す（2026-10-23） |
| VIX短期vs中期 | `declines` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 859・884 | 残す（2026-10-23） |
| スコア構成指標 | `desc` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 843・1352 | 残す（2026-10-23） |
| スコア構成指標 | `detail` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 843・1204 | 残す（2026-10-23） |
| 指標 | `diff_percent` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1153・1153 | 残す（2026-10-23） |
| 件数不足 | `diff_pt` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995 | 残す（2026-10-23） |
| 日付 | `end_et` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1104 | 残す（2026-10-23） |
| 記録なし | `events` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1102 | 残す（2026-10-23） |
| セクター | `excluded` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1025・1025 | 残す（2026-10-23） |
| 今日の結論 | `expected_close_date` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1114・1114・1132・1406 | 残す（2026-10-23） |
| すべて最新 | `expected_provisional_elements` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1129 | 残す（2026-10-23） |
| f+(v.failed_reasons?.[f]==='stale'?'（更新停 | `failed` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1064・1064 | 残す（2026-10-23） |
| f+(v.failed_reasons?.[f]==='stale'?'（更新停 | `failed_reasons` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1064 | 残す（2026-10-23） |
| VIX指数 | `fear_greed` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・693・921・932 | 残す（2026-10-23） |
| x.title_ja)?'日本語の見出しはAI（Grok）による訳で、下の小さな | `fetched_at` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json`<br>`docs/market-monitor/market-pulse/data/sp500_tickers.json` | 1078・1086・1096・1104 | 残す（2026-10-23） |
| — | `fg_score` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1166・1216 | 残す（2026-10-23） |
| 記録なし | `flowing` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1056 | 残す（2026-10-23） |
| — | `futures` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1094 | 残す（2026-10-23） |
| VIX指数 | `gold` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 629・629・629 | 残す（2026-10-23） |
| 件数不足 | `guide` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995 | 残す（2026-10-23） |
| AIの見解 | `haiku` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 798・798・1142・1142 | 残す（2026-10-23） |
| 段階1の区分×VIX水準 | `headline_signal` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 987・990 | 残す（2026-10-23） |
| — | `headlines` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1070 | 残す（2026-10-23） |
| 銘柄 | `held` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057 | 残す（2026-10-23） |
| シグナル | `history_from` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 998 | 残す（2026-10-23） |
| — | `holidays` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/nyse_holidays.json` | 1392 | 残す（2026-10-23） |
| 件数不足 | `horizon` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995・995 | 残す（2026-10-23） |
| 銘柄 | `hype_phase` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057・1057 | 残す（2026-10-23） |
| VIX指数 | `indicators` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・637・694・715 | 残す（2026-10-23） |
| 現在: % \| 90日最高値: ${c.max_90d?? | `is_contracting` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1282 | 残す（2026-10-23） |
| 同限月 | `is_fallback` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 725・1344・1369 | 残す（2026-10-23） |
| 取得できず | `items` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1072・1078・1087・1096 | 残す（2026-10-23） |
| 不明 | `judgment` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.csv`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 647・1395・1395 | 残す（2026-10-23） |
| 米10年債 | `key` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 715・717・718・741 | 残す（2026-10-23） |
| 該当なし | `kind` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1103 | 残す（2026-10-23） |
| 不明 | `label` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 647・677・741・761 | 残す（2026-10-23） |
| 一部前営業日 | `line` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 793・793・1113・1115 | 残す（2026-10-23） |
| f+(v.failed_reasons?.[f]==='stale'?'（更新停 | `link` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1066 | 残す（2026-10-23） |
| 記録なし | `m7_members` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1041・1041・1043・1043 | 残す（2026-10-23） |
| 判定: SOXはS&amp;P500との差±1.0pt、M7は±0.5ptで「け | `m7_missing` | 書き手なし | 1043・1043 | 残す（2026-10-23） |
| 均等加重 | `m7_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 736・736・736・1042 | 残す（2026-10-23） |
| 前日比 | `m7_vs_sp500_pt` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1042 | 残す（2026-10-23） |
| — | `market` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1089・1090 | 残す（2026-10-23） |
| データ取得不可 | `max_90d` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1279・1279・1280 | 残す（2026-10-23） |
| VIX指数 | `mcclellan_oscillator` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・866・866 | 残す（2026-10-23） |
| 件数不足 | `mean_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995 | 残す（2026-10-23） |
| 限月乗り換え日 | `method` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 722・723 | 残す（2026-10-23） |
| 記録なし | `momentum` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1024・1024・1025・1027 | 残す（2026-10-23） |
| 件数不足 | `n` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995 | 残す（2026-10-23） |
| セクター | `name` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1025・1046・1096・1096 | 残す（2026-10-23） |
| 市場の広がり | `new_highs_52w` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 884 | 残す（2026-10-23） |
| 市場の広がり | `new_lows_52w` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 884 | 残す（2026-10-23） |
| VIX指数 | `nh_nl_diff` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・863・863・884 | 残す（2026-10-23） |
| 取得できず: ${escHtml(dq.unavailable.join( | `old_elements` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1121・1121・1130 | 残す（2026-10-23） |
| — | `old_stages` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1111 | 残す（2026-10-23） |
| — | `outcomes` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 993 | 残す（2026-10-23） |
| — | `passed` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1195 | 残す（2026-10-23） |
| 市場の広がり | `pct_above_200ma` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 884・884 | 残す（2026-10-23） |
| 市場の広がり | `pct_above_50ma` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 868・868・884・884 | 残す（2026-10-23） |
| ❌ 警戒 | `point` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1198・1198・1299 | 残す（2026-10-23） |
| }時点の値） | `provisional` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 727・730 | 残す（2026-10-23） |
| 取得できず: ${escHtml(dq.unavailable.join( | `provisional_elements` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1121・1130 | 残す（2026-10-23） |
| 暫定（） | `provisional_kind` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 730・730・731・731 | 残す（2026-10-23） |
| 取得できず（取得時刻から時間以内の見出しが無い） | `published_utc` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1075・1087 | 残す（2026-10-23） |
| 取得できず（取得時刻から時間以内の見出しが無い） | `publisher` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1075・1075 | 残す（2026-10-23） |
| — | `qqq_vs_spy_20d` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 957 | 残す（2026-10-23） |
| セクター | `quadrant` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1025・1025・1025・1057 | 残す（2026-10-23） |
| — | `rating` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 932 | 残す（2026-10-23） |
| セクター | `ratio` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1025・1027 | 残す（2026-10-23） |
| スコア構成指標 | `raw` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 843 | 残す（2026-10-23） |
| 以降の直近20営業日）`:""} | `reason` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1014・1112 | 残す（2026-10-23） |
| 最新値 | `region` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1096・1096・1103 | 残す（2026-10-23） |
| — | `relevance` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1089 | 残す（2026-10-23） |
| — | `risk_off_score` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 770・772 | 残す（2026-10-23） |
| 同限月 | `roll_suspect` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 723 | 残す（2026-10-23） |
| 銘柄 | `rows` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057 | 残す（2026-10-23） |
| 市場の広がり | `rsp_spy_divergence_20d_avg` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/breadth_data.json`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 864・864・884・884 | 残す（2026-10-23） |
| 今日の結論 | `rule` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1114 | 残す（2026-10-23） |
| VIX指数 | `score` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・637・691・693 | 残す（2026-10-23） |
| 銘柄 | `sector_etf` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057・1057 | 残す（2026-10-23） |
| 銘柄 | `sector_name` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057 | 残す（2026-10-23） |
| — | `sector_rotation` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1022 | 残す（2026-10-23） |
| 記録なし | `sectors` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1024 | 残す（2026-10-23） |
| — | `semis_m7` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 734・1038 | 残す（2026-10-23） |
| VIX指数 | `sentiment` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・637・637・637 | 残す（2026-10-23） |
| 件数不足 | `signal` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995・1006・1013・1013 | 残す（2026-10-23） |
| 記録なし | `signals` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 986・992 | 残す（2026-10-23） |
| 傾き: 上向き | `slope_up` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1268・1271 | 残す（2026-10-23） |
| 取得できず（取得時刻から時間以内の見出しが無い） | `source` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json`<br>`docs/market-monitor/market-pulse/data/nyse_holidays.json` | 1075 | 残す（2026-10-23） |
| 前日比 | `sox_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 735・735・1042 | 残す（2026-10-23） |
| — | `sox_value` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 735・735 | 残す（2026-10-23） |
| 前日比 | `sox_vs_sp500_pt` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1042 | 残す（2026-10-23） |
| 前日比 | `sp500_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1042 | 残す（2026-10-23） |
| — | `stage_conclusions` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 792・985・1111・1136 | 残す（2026-10-23） |
| 日付 | `start_et` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1104 | 残す（2026-10-23） |
| 最新値 | `status` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1089・1096・1120・1121 | 残す（2026-10-23） |
| — | `sub_scores` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 832 | 残す（2026-10-23） |
| AIの見解 | `summary` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.csv`<br>`docs/market-monitor/market-pulse/data/market_data.json` | 798・804・809・1142 | 残す（2026-10-23） |
| 値 | `symbol` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1046 | 残す（2026-10-23） |
| — | `tags` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1136 | 残す（2026-10-23） |
| 銘柄 | `tail` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057 | 残す（2026-10-23） |
| — | `take_profit_checklist` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1165 | 残す（2026-10-23） |
| 銘柄 | `tanuki_score` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057・1057 | 残す（2026-10-23） |
| VIX指数 | `tech_pulse` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 695・920・970・1166 | 残す（2026-10-23） |
| 銘柄 | `ticker` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1057・1057・1341・1352 | 残す（2026-10-23） |
| 該当なし | `time_et` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1103・1103 | 残す（2026-10-23） |
| 該当なし | `time_jst` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1103・1103 | 残す（2026-10-23） |
| f+(v.failed_reasons?.[f]==='stale'?'（更新停 | `title` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1067・1067・1103 | 残す（2026-10-23） |
| f+(v.failed_reasons?.[f]==='stale'?'（更新停 | `title_ja` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1067・1067・1078 | 残す（2026-10-23） |
| 同限月 | `to` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json`<br>`docs/market-monitor/market-pulse/data/nyse_holidays.json` | 723 | 残す（2026-10-23） |
| セクター | `trail` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1027・1027・1027 | 残す（2026-10-23） |
| 取得できず: ${escHtml(dq.unavailable.join( | `unavailable` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1121・1121 | 残す（2026-10-23） |
| 件数不足 | `up_pct` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995 | 残す（2026-10-23） |
| VIX指数 | `value` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 637・637・706・706 | 残す（2026-10-23） |
| S&P500の出来高÷前営業日の出来高と、前日比 | `vol_ratio` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 840 | 残す（2026-10-23） |
| — | `vxn_latest` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 955 | 残す（2026-10-23） |
| （計算日: —） | `watch_list` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1054 | 残す（2026-10-23） |
| 不明 | `weather` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 647・647・1112・1114 | 残す（2026-10-23） |
| スコア構成指標 | `weight` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 843・843 | 残す（2026-10-23） |
| 以降の直近20営業日）`:""} | `window_start` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 1014・1014 | 残す（2026-10-23） |
| 件数不足 | `z` | この段階の書き手あり<br>`docs/market-monitor/market-pulse/data/market_data.json` | 995 | 残す（2026-10-23） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/market-monitor/market-pulse/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `as_of`（この段階の書き手あり）<br>`change`（この段階の書き手あり）<br>`change_bp`（この段階の書き手あり）<br>`change_percent`（この段階の書き手あり）<br>`checked_at`（この段階の書き手あり）<br>`comments_history`（この段階の書き手あり）<br>`contract_roll`（この段階の書き手あり）<br>`credit`（この段階の書き手あり）<br>`data_freshness`（この段階の書き手あり）<br>`data_quality`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`expected_close_date`（この段階の書き手あり）<br>`expected_provisional_elements`（この段階の書き手あり）<br>`failed`（この段階の書き手あり）<br>`failed_reasons`（この段階の書き手あり）<br>`fetched_at`（この段階の書き手あり）<br>`gold`（この段階の書き手あり）<br>`haiku`（この段階の書き手あり）<br>`headlines`（この段階の書き手あり）<br>`holidays`（この段階の書き手あり）<br>`indicators`（この段階の書き手あり）<br>`is_fallback`（この段階の書き手あり）<br>`items`（この段階の書き手あり）<br>`judgment`（この段階の書き手あり）<br>`key`（この段階の書き手あり）<br>`m7_pct`（この段階の書き手あり）<br>`market`（この段階の書き手あり）<br>`method`（この段階の書き手あり）<br>`name`（この段階の書き手あり）<br>`old_elements`（この段階の書き手あり）<br>`provisional`（この段階の書き手あり）<br>`provisional_elements`（この段階の書き手あり）<br>`provisional_kind`（この段階の書き手あり）<br>`published_utc`（この段階の書き手あり）<br>`publisher`（この段階の書き手あり）<br>`relevance`（この段階の書き手あり）<br>`risk_off_score`（この段階の書き手あり）<br>`roll_suspect`（この段階の書き手あり）<br>`semis_m7`（この段階の書き手あり）<br>`sentiment`（この段階の書き手あり）<br>`source`（この段階の書き手あり）<br>`sox_pct`（この段階の書き手あり）<br>`sox_value`（この段階の書き手あり）<br>`stage_conclusions`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`summary`（この段階の書き手あり）<br>`tags`（この段階の書き手あり）<br>`title_ja`（この段階の書き手あり）<br>`to`（この段階の書き手あり）<br>`unavailable`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| ① メーター | `score`（この段階の書き手あり）<br>`sentiment`（この段階の書き手あり） |  |
| ① メーター2つ横並び | `components`（この段階の書き手あり）<br>`fear_greed`（この段階の書き手あり）<br>`qqq_vs_spy_20d`（この段階の書き手あり）<br>`rating`（この段階の書き手あり）<br>`score`（この段階の書き手あり）<br>`tech_pulse`（この段階の書き手あり）<br>`vxn_latest`（この段階の書き手あり） |  |
| ② 関連指標バー | `ad_ratio_5d`（この段階の書き手あり）<br>`advances`（この段階の書き手あり）<br>`breadth`（この段階の書き手あり）<br>`chg_pct`（この段階の書き手あり）<br>`contango`（この段階の書き手あり）<br>`declines`（この段階の書き手あり）<br>`desc`（この段階の書き手あり）<br>`detail`（この段階の書き手あり）<br>`indicators`（この段階の書き手あり）<br>`mcclellan_oscillator`（この段階の書き手あり）<br>`new_highs_52w`（この段階の書き手あり）<br>`new_lows_52w`（この段階の書き手あり）<br>`nh_nl_diff`（この段階の書き手あり）<br>`pct_above_200ma`（この段階の書き手あり）<br>`pct_above_50ma`（この段階の書き手あり）<br>`raw`（この段階の書き手あり）<br>`rsp_spy_divergence_20d_avg`（この段階の書き手あり）<br>`score`（この段階の書き手あり）<br>`sentiment`（この段階の書き手あり）<br>`sub_scores`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`vol_ratio`（この段階の書き手あり）<br>`weight`（この段階の書き手あり） |  |
| ③ 折れ線グラフ | `above`（この段階の書き手あり）<br>`active`（この段階の書き手あり）<br>`ad_line`（この段階の書き手あり）<br>`ad_ratio_5d`（この段階の書き手あり）<br>`as_of`（この段階の書き手あり）<br>`asset_flow`（この段階の書き手あり）<br>`bar_time_utc`（この段階の書き手あり）<br>`base_up_pct`（この段階の書き手あり）<br>`breadth`（この段階の書き手あり）<br>`buy_checklist`（この段階の書き手あり）<br>`calculated`（この段階の書き手あり）<br>`calendar`（この段階の書き手あり）<br>`change_pct`（この段階の書き手あり）<br>`change_percent`（この段階の書き手あり）<br>`commodity_fx`（この段階の書き手あり）<br>`components`（この段階の書き手あり）<br>`contract`（この段階の書き手あり）<br>`credit`（この段階の書き手あり）<br>`current`（この段階の書き手あり）<br>`data_quality`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`date_et`（この段階の書き手あり）<br>`date_local`（この段階の書き手あり）<br>`desc`（この段階の書き手あり）<br>`detail`（この段階の書き手あり）<br>`diff_percent`（この段階の書き手あり）<br>`diff_pt`（この段階の書き手あり）<br>`end_et`（この段階の書き手あり）<br>`events`（この段階の書き手あり）<br>`excluded`（この段階の書き手あり）<br>`expected_close_date`（この段階の書き手あり）<br>`failed`（この段階の書き手あり）<br>`failed_reasons`（この段階の書き手あり）<br>`fear_greed`（この段階の書き手あり）<br>`fetched_at`（この段階の書き手あり）<br>`fg_score`（この段階の書き手あり）<br>`flowing`（この段階の書き手あり）<br>`futures`（この段階の書き手あり）<br>`guide`（この段階の書き手あり）<br>`haiku`（この段階の書き手あり）<br>`headline_signal`（この段階の書き手あり）<br>`held`（この段階の書き手あり）<br>`history_from`（この段階の書き手あり）<br>`horizon`（この段階の書き手あり）<br>`hype_phase`（この段階の書き手あり）<br>`indicators`（この段階の書き手あり）<br>`is_contracting`（この段階の書き手あり）<br>`is_fallback`（この段階の書き手あり）<br>`items`（この段階の書き手あり）<br>`judgment`（この段階の書き手あり）<br>`key`（この段階の書き手あり）<br>`kind`（この段階の書き手あり）<br>`label`（この段階の書き手あり）<br>`line`（この段階の書き手あり）<br>`m7_members`（この段階の書き手あり）<br>`m7_missing`（書き手なし）<br>`m7_pct`（この段階の書き手あり）<br>`m7_vs_sp500_pt`（この段階の書き手あり）<br>`max_90d`（この段階の書き手あり）<br>`mcclellan_oscillator`（この段階の書き手あり）<br>`mean_pct`（この段階の書き手あり）<br>`momentum`（この段階の書き手あり）<br>`n`（この段階の書き手あり）<br>`name`（この段階の書き手あり）<br>`nh_nl_diff`（この段階の書き手あり）<br>`old_stages`（この段階の書き手あり）<br>`outcomes`（この段階の書き手あり）<br>`passed`（この段階の書き手あり）<br>`point`（この段階の書き手あり）<br>`provisional_kind`（この段階の書き手あり）<br>`quadrant`（この段階の書き手あり）<br>`ratio`（この段階の書き手あり）<br>`reason`（この段階の書き手あり）<br>`region`（この段階の書き手あり）<br>`risk_off_score`（この段階の書き手あり）<br>`rows`（この段階の書き手あり）<br>`rule`（この段階の書き手あり）<br>`score`（この段階の書き手あり）<br>`sector_etf`（この段階の書き手あり）<br>`sector_name`（この段階の書き手あり）<br>`sector_rotation`（この段階の書き手あり）<br>`sectors`（この段階の書き手あり）<br>`semis_m7`（この段階の書き手あり）<br>`sentiment`（この段階の書き手あり）<br>`signal`（この段階の書き手あり）<br>`signals`（この段階の書き手あり）<br>`slope_up`（この段階の書き手あり）<br>`sox_pct`（この段階の書き手あり）<br>`sox_vs_sp500_pt`（この段階の書き手あり）<br>`sp500_pct`（この段階の書き手あり）<br>`stage_conclusions`（この段階の書き手あり）<br>`start_et`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`summary`（この段階の書き手あり）<br>`symbol`（この段階の書き手あり）<br>`tail`（この段階の書き手あり）<br>`take_profit_checklist`（この段階の書き手あり）<br>`tanuki_score`（この段階の書き手あり）<br>`tech_pulse`（この段階の書き手あり）<br>`ticker`（この段階の書き手あり）<br>`time_et`（この段階の書き手あり）<br>`time_jst`（この段階の書き手あり）<br>`title`（この段階の書き手あり）<br>`trail`（この段階の書き手あり）<br>`up_pct`（この段階の書き手あり）<br>`value`（この段階の書き手あり）<br>`watch_list`（この段階の書き手あり）<br>`weather`（この段階の書き手あり）<br>`window_start`（この段階の書き手あり）<br>`z`（この段階の書き手あり） |  |
| （区画を決められない） | `fetched_at`（この段階の書き手あり）<br>`items`（この段階の書き手あり）<br>`link`（この段階の書き手あり）<br>`provisional`（この段階の書き手あり）<br>`provisional_kind`（この段階の書き手あり）<br>`published_utc`（この段階の書き手あり）<br>`title`（この段階の書き手あり）<br>`title_ja`（この段階の書き手あり） |  |

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | Market Pulseか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。Market Pulseのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | Market Pulseを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:723`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:866`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:894` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:229`<br>`src/tail/edgar_rss_monitor.py:433`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:605` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1634`<br>`common/sec_data/report_consistency_check.py:1775`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:284` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:389`<br>`src/tail/xbrl_segment_fetcher.py:196`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:143` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:673`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:622`<br>`src/value/tanuki_valuation/pipeline.py:624`<br>`src/value/tanuki_valuation/pipeline.py:626`<br>`src/value/tanuki_valuation/pipeline.py:865`<br>`src/value/tanuki_valuation/pipeline.py:878`<br>`src/value/tanuki_valuation/pipeline.py:898`<br>`src/value/tanuki_valuation/pipeline.py:900`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:970`<br>`src/value/tanuki_valuation/pipeline.py:1355`<br>`src/value/tanuki_valuation/pipeline.py:1382`<br>`src/value/tanuki_valuation/pipeline.py:1387`<br>`src/value/tanuki_valuation/pipeline.py:1433`<br>`src/value/tanuki_valuation/pipeline.py:1738`<br>`src/value/tanuki_valuation/pipeline.py:1916` | いいえ |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1121` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1105` | いいえ |
| 取引日（市場の営業日）の決め方 | 5 | `common/market_data/fetcher.py:226`<br>`common/market_data/fetcher.py:722`<br>`common/market_data/fetcher.py:1011`<br>`common/market_data/fetcher.py:1032`<br>`common/market_data/fetcher.py:1042`<br>`common/market_data/reader.py:104`<br>`common/sec_data/report_consistency_check.py:1604`<br>`src/market/macro_pulse/05_main.py:442`<br>`src/market/macro_pulse/05_main.py:456`<br>`src/market/market_pulse/breadth_calculator.py:342` | はい |
| 暫定の足（取引時間中・取得の途中の行）の判定 | 4 | `common/market_data/fetcher.py:403`<br>`common/market_data/fetcher.py:411`<br>`common/market_data/fetcher.py:450`<br>`common/market_data/fetcher.py:572`<br>`common/market_data/fetcher.py:583`<br>`common/market_data/fetcher.py:734`<br>`common/market_data/fetcher.py:746`<br>`common/sec_data/report_consistency_check.py:1634`<br>`src/market/market_pulse/collect_and_send.py:781`<br>`src/market/market_pulse/collect_and_send.py:795`<br>`src/market/market_pulse/futures_snapshot.py:38` | はい |
| S&P500の構成銘柄の一覧の取り方 | 3 | `common/market_data/fetcher.py:1443`<br>`common/market_data/reader.py:377`<br>`src/market/market_pulse/breadth_calculator.py:69` | はい |
| 景気後退リスクの点数（8指標の段階の点の合計）の計算 | 2 | `docs/market-monitor/macro-pulse/index.html:1443`<br>`docs/market-monitor/macro-pulse/index.html:2389`<br>`src/market/macro_pulse/05_main.py:1580`<br>`src/market/macro_pulse/05_main.py:1700` | いいえ |
| 指標ごとの段階の信号（しきい値で点を付ける） | 2 | `docs/market-monitor/macro-pulse/index.html:1410`<br>`src/market/macro_pulse/05_main.py:1563`<br>`src/market/macro_pulse/05_main.py:1576` | いいえ |
| FREDのAPIを直接呼ぶ所（系列の値・公表日） | 3 | `common/macro_data/fetcher.py:69`<br>`src/market/macro_pulse/05_main.py:527`<br>`src/market/market_pulse/market_calendar.py:63` | はい |
| AI（Grok）のAPIの宛先の直書き | 10 | `src/market/macro_pulse/05_main.py:1339`<br>`src/market/macro_pulse/05_main.py:1832`<br>`src/market/market_pulse/collect_and_send.py:1147`<br>`src/market/market_pulse/news_headlines.py:41`<br>`src/tail/kpi_proposer.py:71`<br>`src/tail/quarterly_review_generator.py:78`<br>`src/tail/sec_ctrl_fetcher.py:50`<br>`src/tail/text_kpi_extractor.py:47`<br>`src/value/adjusted_eps_analyzer/ai_analyzer.py:74`<br>`src/value/tanuki_valuation/calculator/fcf_outlier_ai.py:35`<br>`src/value/tanuki_valuation/validator.py:37` | はい |
| JSONの書き込み（一時ファイル→置き換え）の実装 | 2 | `common/macro_data/fetcher.py:79`<br>`common/market_data/fetcher.py:180` | いいえ |
| 休場日（祝日）の一覧の作り方 | 2 | `src/market/macro_pulse/05_main.py:409`<br>`src/market/market_pulse/collect_and_send.py:1638` | はい |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（src/market/market_pulse）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（7ファイル・3,795行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `breadth_calculator.py` | 517 | `.github/workflows/Market_Pulse_Update.yml` | — | `src/market/market_pulse/collect_and_send.py` | 2件 |
| `collect_and_send.py` | 1,988 | `.github/workflows/Market_Pulse_Update.yml` | — | — | 4件 |
| `futures_snapshot.py` | 69 | — | — | `src/market/market_pulse/collect_and_send.py` | 1件 |
| `market_calendar.py` | 204 | — | — | `src/market/market_pulse/collect_and_send.py` | 1件 |
| `news_headlines.py` | 274 | — | `browser_checks/market_pulse_elements.py` | `src/market/market_pulse/collect_and_send.py` | 1件 |
| `sector_rotation.py` | 289 | — | — | `src/market/market_pulse/collect_and_send.py`<br>`src/market/market_pulse/stage_conclusions.py` | 2件 |
| `stage_conclusions.py` | 454 | — | — | `src/market/market_pulse/collect_and_send.py` | 4件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
