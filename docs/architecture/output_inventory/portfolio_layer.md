# Portfolio（src/portfolio・docs/portfolio）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`portfolio_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`docs/portfolio/index.html`・`docs/portfolio/tail/`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-23、Koichiさんの判断）。theme_config.json（config/・docs/）を消し、discover_config.json の各銘柄の themes を消した。history.json・portfolio.json・discover_config.json は残す（discover_config.json は後の [[HOLDINGS-SINGLE-SOURCE-1]] で削除）。history.json の合計9キーは日ごとの記録で作り直せないため残す（下の注記）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 2（ファイル 2・人が見るもの 0）／読み手0件 0／テストだけ 0／1件 1／2件以上 1／規則の二重 9（検出式 10種のうち。Portfolioのファイルを含むもの 0）

**画面の項目**: 画面が読むキー 31（この段階の書き手あり 14・ほかの書き手あり 17・画面の中で作る値 0・**書き手なし 0**）／書き手があるのに誰も読まないキー **9**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 1件／解決できた読み込み 6件＋ファイル名で当たった行 8件（6.7%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

フォルダを決めきれない行（ファイル名で当たった行）:
- `scripts/analysis/market_pulse_redesign.py` 行 393（`docs/portfolio/data/portfolio.json`）: `pf = json.loads(subprocess.run(["git", "-C", REPO, "show", f"{DATA_REV}:docs/portfolio/data/portfolio.json"],`

## 消したもの（段階2、2026-10-23）

- `docs/portfolio/data/theme_config.json`: 削除（2026-10-23、Koichiさんの判断）。元の config/theme_config.json も消し、Discover_Config_Sync.yml の写しと rcc の同期の確認（CHECK-32）の組も外した。docs/ の写しを読むのは CHECK-32 だけで、画面は読んでいなかった（画面の color・label は画面の中の PHASE_CFG の値）
- `docs/portfolio/data/discover_config.json（と元の config/discover_config.json・Discover_Config_Sync.yml・rcc の CHECK-32）`: 削除（2026-10-23、Koichiさんの判断、[[HOLDINGS-SINGLE-SOURCE-1]]）。保有の正を portfolio.json にし、画面は区分「保有中」を読まなくなった。区分の監視中・休眠とメモを読むコードは登録の検証（register_ticker.py の Step 6・registration_validator.py の P1-Step6・P4）のほかに無かった。休眠（BBAI）は cik_lookup.csv の status の dormant に移した

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 1件 | `docs/portfolio/data/history.json`<br>（1ファイル） | 日ごとの資産のスナップショット（snapshot.py が portfolio.json と株価・為替から作って追記）。Portfolioの画面の推移が読む | `snapshot.py:main` | 1 | portfolio/index.html（直接） | 効く: Portfolioの画面 | — | 残す（2026-10-23） |
| 2件以上 | `docs/portfolio/data/portfolio.json`<br>（1ファイル） | 証券会社ごとの保有（銘柄・株数・取得単価・現金）。**コードの書き手は無い（人の入力）**: 管理のページ（2026-10-21に削除）の代わりに、CLAUDE_CODE_START.md「管理の操作の手順」の「ポートフォリオを変えて」でClaude Codeが直接編集する。最後の更新は2026-06-03。Portfolioの画面・snapshot.py・TAIL（取得単価・画面）・System Health（保有の無い監視銘柄）・Market Pulse（監視銘柄）が読む |  | 8・テスト8 | market-monitor/market-pulse/index.html（経由）<br>portfolio/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（直接）<br>value-monitor/tanuki_score/index.html（経由） | 効く: 保有銘柄の監視（TANUKI TAIL）・Portfolioの画面・TANUKI TAILの画面（保有銘柄の監視） | AS-IS-264・AS-IS-391・AS-IS-393・AS-IS-469 | 残す（2026-10-23） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|

## 読み手の内訳

### `docs/portfolio/data/history.json`（1件）
- 起動元: .github/workflows/TANUKI_Score_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `snapshots`(1)
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 215

### `docs/portfolio/data/portfolio.json`（2件以上）
- 起動元: ワークフローから書き手を呼ぶ参照が見つからない
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `brokers`(9)・`last_updated`(2)
- Python・保存済みファイル: `common/sec_data/report_consistency_check.py` 行 1907・1909・3496・3681（経由 `common/sec_data/report_consistency_check.py::_check_holdings_vs_theses`）
- Python・保存済みファイル: `common/system_health.py` 行 52・729・776・1049・1110（経由 `common/system_health.py::_portfolio_tickers`）
- Python・保存済みファイル: `src/market/market_pulse/collect_and_send.py` 行 1520・1544・1904・1906（経由 `src/market/market_pulse/sector_rotation.py::watch_tickers`）
- Python・保存済みファイル: `src/market/market_pulse/sector_rotation.py` 行 215・258（経由 `src/market/market_pulse/sector_rotation.py::watch_tickers`）
- Python・保存済みファイル: `src/portfolio/snapshot.py` 行 25・30・142（経由 `src/portfolio/snapshot.py::main`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 61・271・1551・1744・1780（経由 `src/tail/quarterly_review_generator.py::get_avg_cost`）
- JS・HTML・保存済みファイル: `docs/portfolio/index.html` 行 212
- JS・HTML・保存済みファイル: `docs/portfolio/tail/index.html` 行 981
- テスト・保存済みファイル: `tests/test_check41_registration_ng.py` 行 89（経由 `common/sec_data/report_consistency_check.py::_check_holdings_vs_theses`）
- テスト・保存済みファイル: `tests/test_market_pulse_sector_rotation.py` 行 109
- テスト・保存済みファイル: `tests/test_registration_consistency_gate.py` 行 64・71・77・82（経由 `common/sec_data/report_consistency_check.py::_check_holdings_vs_theses`）
- テスト・保存済みファイル: `tests/test_report_consistency_check.py` 行 1625
- テスト・保存済みファイル: `tests/test_sec_freshness.py` 行 472（経由 `common/system_health.py::_portfolio_tickers`）
- テスト・保存済みファイル: `tests/test_system_health_ticker_audit.py` 行 71・80・88・96・106・114・120・133・141・154・165・174 ほか4（経由 `common/system_health.py::_portfolio_tickers`）
- テスト・保存済みファイル: `tests/test_system_health_workflow_monitor.py` 行 289・297・429・442（経由 `common/system_health.py::_portfolio_tickers`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::get_avg_cost`）
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/market-monitor/market-pulse/index.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/market/market_pulse/collect_and_send.py → docs/market-monitor/market-pulse/data/market_data.json））: `docs/value-monitor/tanuki_score/index.html`

## 画面の項目

画面（`docs/portfolio/index.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）


### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `cash_usd`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `cost_usd`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `equity_usd`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `total_cash_jpy`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `total_cash_usd`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `total_equity_jpy`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `total_equity_usd`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `total_pnl_jpy`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない
- `total_pnl_pct`（`docs/portfolio/data/history.json`）: 残す: 日ごとの資産の合計の記録（snapshot.py がその日の株価・為替で作る）。過去の日の値は作り直せない

### `docs/portfolio/index.html`（31項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `avg_cost` | この段階の書き手あり<br>`docs/portfolio/data/portfolio.json` | 309・405 | 残す（2026-10-23） |
| — | `base` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 366 | 残す（2026-10-23） |
| — | `brokers` | この段階の書き手あり<br>`docs/portfolio/data/history.json`<br>`docs/portfolio/data/portfolio.json` | 293 | 残す（2026-10-23） |
| — | `cash` | この段階の書き手あり<br>`docs/portfolio/data/portfolio.json` | 302・411・412・477 | 残す（2026-10-23） |
| — | `components` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 365・407 | 残す（2026-10-23） |
| — | `count` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json` | 725 | 残す（2026-10-23） |
| — | `current_price` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 365・407 | 残す（2026-10-23） |
| — | `date` | この段階の書き手あり<br>`docs/portfolio/data/history.json` | 779 | 残す（2026-10-23） |
| — | `funda_score` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 387・566 | 残す（2026-10-23） |
| — | `intrinsic_value_per_share` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 366・367 | 残す（2026-10-23） |
| 陶酔期 | `label` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 505・585・594・594 | 残す（2026-10-23） |
| — | `last_updated` | この段階の書き手あり<br>`docs/portfolio/data/portfolio.json` | 290・290 | 残す（2026-10-23） |
| — | `memo` | この段階の書き手あり<br>`docs/portfolio/data/portfolio.json` | 314・314・506 | 残す（2026-10-23） |
| — | `monthly` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 364・364・560・560 | 残す（2026-10-23） |
| — | `name` | ほかの書き手あり<br>`docs/portfolio/tail/data/mda/ADBE/latest.json`<br>`docs/portfolio/tail/data/mda/CELH/latest.json` | 412・474 | 残す（2026-10-23） |
| — | `positions` | この段階の書き手あり<br>`docs/portfolio/data/history.json`<br>`docs/portfolio/data/portfolio.json` | 306・405・406 | 残す（2026-10-23） |
| — | `scenario_valuations` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 366 | 残す（2026-10-23） |
| — | `shares` | この段階の書き手あり<br>`docs/portfolio/data/history.json`<br>`docs/portfolio/data/portfolio.json` | 308・405・408 | 残す（2026-10-23） |
| — | `snapshots` | この段階の書き手あり<br>`docs/portfolio/data/history.json` | 280・770 | 残す（2026-10-23） |
| — | `stage_label` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 369・561・593 | 残す（2026-10-23） |
| — | `substage_label` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/AAPL_poc.json`<br>`docs/value-monitor/hypecore/data/ABBV_poc.json` | 370 | 残す（2026-10-23） |
| — | `tanuki_score` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 386・558 | 残す（2026-10-23） |
| 仕込みゾーン候補（TANUKI SCORE BUY判定 × 早期フェーズ・未保有 | `ticker` | ほかの書き手あり<br>`docs/portfolio/tail/data/ctrl/ADBE/latest.json`<br>`docs/portfolio/tail/data/ctrl/APP/latest.json` | 493・493・648・649 | 残す（2026-10-23） |
| — | `tickers` | ほかの書き手あり<br>`docs/value-monitor/hypecore/data/tickers.json`<br>`docs/value-monitor/tanuki_valuation/data/tickers.json` | 277 | 残す（2026-10-23） |
| — | `timing_score` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 388・567 | 残す（2026-10-23） |
| — | `total_assets_jpy` | この段階の書き手あり<br>`docs/portfolio/data/history.json` | 781 | 残す（2026-10-23） |
| — | `total_assets_usd` | この段階の書き手あり<br>`docs/portfolio/data/history.json` | 780 | 残す（2026-10-23） |
| — | `total_pnl_usd` | この段階の書き手あり<br>`docs/portfolio/data/history.json` | 782 | 残す（2026-10-23） |
| — | `upside_percent` | ほかの書き手あり<br>`docs/value-monitor/tanuki_valuation/data/AAPL/latest.json`<br>`docs/value-monitor/tanuki_valuation/data/ABBV/latest.json` | 368・565 | 残す（2026-10-23） |
| — | `usdjpy` | この段階の書き手あり<br>`docs/portfolio/data/history.json` | 281・281 | 残す（2026-10-23） |
| — | `value` | この段階の書き手あり<br>`docs/portfolio/data/history.json` | 184・275・277・278 | 残す（2026-10-23） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/portfolio/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| ── Auth screen ── | `value`（この段階の書き手あり） |  |
| ── Main content ── | `avg_cost`（この段階の書き手あり）<br>`base`（ほかの書き手あり）<br>`brokers`（この段階の書き手あり）<br>`cash`（この段階の書き手あり）<br>`components`（ほかの書き手あり）<br>`count`（ほかの書き手あり）<br>`current_price`（ほかの書き手あり）<br>`funda_score`（ほかの書き手あり）<br>`intrinsic_value_per_share`（ほかの書き手あり）<br>`label`（ほかの書き手あり）<br>`last_updated`（この段階の書き手あり）<br>`memo`（この段階の書き手あり）<br>`monthly`（ほかの書き手あり）<br>`name`（ほかの書き手あり）<br>`positions`（この段階の書き手あり）<br>`scenario_valuations`（ほかの書き手あり）<br>`shares`（この段階の書き手あり）<br>`snapshots`（この段階の書き手あり）<br>`stage_label`（ほかの書き手あり）<br>`substage_label`（ほかの書き手あり）<br>`tanuki_score`（ほかの書き手あり）<br>`ticker`（ほかの書き手あり）<br>`tickers`（ほかの書き手あり）<br>`timing_score`（ほかの書き手あり）<br>`upside_percent`（ほかの書き手あり）<br>`usdjpy`（この段階の書き手あり）<br>`value`（この段階の書き手あり） |  |
| （JSで作る要素 #history-plotly） | `date`（この段階の書き手あり）<br>`snapshots`（この段階の書き手あり）<br>`total_assets_jpy`（この段階の書き手あり）<br>`total_assets_usd`（この段階の書き手あり）<br>`total_pnl_usd`（この段階の書き手あり） |  |

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | Portfolioか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。Portfolioのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | Portfolioを含む |
|---|---|---|---|
| Q4の逆算（年次−Q1〜Q3） | 3 | `common/sec_data/parser.py:2312`<br>`common/sec_data/q4_implied.py:126`<br>`common/sec_data/q4_implied.py:137`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:720`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:863`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:891` | いいえ |
| 同じ期の値が複数あるときの選び方（提出日が最新のもの） | 7 | `common/sec_data/fact_selection.py:19`<br>`common/sec_data/fact_selection.py:35`<br>`common/sec_data/layer3_builder.py:817`<br>`common/sec_data/layer3_builder.py:1055`<br>`common/sec_data/normalizer.py:171`<br>`common/sec_data/normalizer.py:434`<br>`common/sec_data/rotce.py:345`<br>`common/sec_data/share_unit_fix.py:104`<br>`src/tail/edgar_rss_monitor.py:219`<br>`src/tail/edgar_rss_monitor.py:419`<br>`src/value/adjusted_eps_analyzer/extract_key_facts.py:602` | いいえ |
| データの古さ・遅れの判定 | 7 | `common/sec_data/freshness.py:83`<br>`common/sec_data/parser.py:3035`<br>`common/sec_data/report_consistency_check.py:1585`<br>`common/sec_data/report_consistency_check.py:1727`<br>`common/system_health.py:955`<br>`docs/portfolio/tail/index.html:942`<br>`docs/value-monitor/adjusted_eps_analyzer/index.html:192`<br>`src/value/stonks_silo/financial_as_of.py:70` | いいえ |
| TTM（直近4四半期の合計）の作り方 | 3 | `common/sec_data/ttm_calculator.py:254`<br>`src/value/adjusted_eps_analyzer/pipeline.py:402`<br>`src/value/hypecore/hypecore.py:284` | いいえ |
| 期の名前（四半期のラベル）の付け方 | 3 | `src/tail/edgar_rss_monitor.py:375`<br>`src/tail/xbrl_segment_fetcher.py:180`<br>`src/value/tanuki_valuation/calculator/segment_growth_xbrl.py:144` | いいえ |
| 会計年度・会計年度末の決め方 | 3 | `common/sec_data/parser.py:451`<br>`common/sec_data/parser.py:460`<br>`common/sec_data/utils.py:93`<br>`common/sec_data/utils.py:274`<br>`common/sec_data/utils.py:309`<br>`common/sec_data/utils.py:349`<br>`src/tail/edgar_rss_monitor.py:137` | いいえ |
| 理論株価の割安・割高の判定のしきい値（upsideの比較） | 5 | `docs/portfolio/index.html:650`<br>`docs/value-monitor/tanuki_score/index.html:571`<br>`docs/value-monitor/tanuki_score/index.html:1094`<br>`docs/value-monitor/tanuki_score/index.html:1323`<br>`docs/value-monitor/tanuki_valuation/index.html:468`<br>`docs/value-monitor/tanuki_valuation/stock.html:1135`<br>`docs/value-monitor/tanuki_valuation/stock.html:1136`<br>`src/value/tanuki_valuation/pipeline.py:621`<br>`src/value/tanuki_valuation/pipeline.py:623`<br>`src/value/tanuki_valuation/pipeline.py:625`<br>`src/value/tanuki_valuation/pipeline.py:864`<br>`src/value/tanuki_valuation/pipeline.py:877`<br>`src/value/tanuki_valuation/pipeline.py:897`<br>`src/value/tanuki_valuation/pipeline.py:899`<br>`src/value/tanuki_valuation/pipeline.py:966`<br>`src/value/tanuki_valuation/pipeline.py:967`<br>`src/value/tanuki_valuation/pipeline.py:968`<br>`src/value/tanuki_valuation/pipeline.py:969`<br>`src/value/tanuki_valuation/pipeline.py:1354`<br>`src/value/tanuki_valuation/pipeline.py:1381`<br>`src/value/tanuki_valuation/pipeline.py:1386`<br>`src/value/tanuki_valuation/pipeline.py:1432`<br>`src/value/tanuki_valuation/pipeline.py:1737`<br>`src/value/tanuki_valuation/pipeline.py:1915` | いいえ |
| β・WACCの作り方 | 3 | `src/value/tanuki_valuation/beta_fetcher.py:110`<br>`src/value/tanuki_valuation/calculator/wacc.py:47`<br>`src/value/tanuki_valuation/calculator/wacc.py:54`<br>`src/value/tanuki_valuation/data_fetcher.py:1115` | いいえ |
| FCFの基準の選び方・推定のフォールバック | 3 | `common/sec_data/reader.py:220`<br>`src/value/tanuki_valuation/calculator/adjustments.py:184`<br>`src/value/tanuki_valuation/data_fetcher.py:114`<br>`src/value/tanuki_valuation/data_fetcher.py:1099` | いいえ |

1か所だけのもの: 成長率の上限・下限（1）

## 本番外のコード（src/portfolio）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 段階のフォルダの.pyのうち、ワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの（名前を挙げた候補は無い）

候補は0件。見たファイル（1ファイル・142行）はどれも、ワークフロー・ほかのコード・同じフォルダのどれかから呼ばれる:

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト |
|---|---|---|---|---|---|
| `snapshot.py` | 142 | `.github/workflows/TANUKI_Score_Update.yml` | — | — | 0件 |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
