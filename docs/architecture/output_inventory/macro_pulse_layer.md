# MACRO PULSE（src/market/macro_pulse・docs/market-monitor/macro-pulse）の出力の一覧（段階1の下書き）

[[OUTPUT-INVENTORY-CHECK-1]]の段階1。`scripts/inventory/build_output_inventory.py`がASTとgrepで作った一覧で、手で直さない（作り直すときはスクリプトを実行する）。機械が読む形は`macro_pulse_layer.json`。「何か」の欄だけは人・AIの注記（スクリプトの`ANNOTATIONS`）で、空なら書き手の関数名を見る。

- 読み手の数: その出力のファイルを読む本番のコードのファイルの数（テストを除く。ワークフロー・JS・HTMLを含む）。「その場で計算」は、ファイルを読まずに同じ中身を関数で作り直す読み手のファイルの数
- 画面: 直接（docs/のページがそのファイルを読む）と経由（読み手のPythonのファイル〈呼び出しを3段までたどった箇所を含む〉がdocs/に書くファイルを読むページ。推定。経由の元は「読み手の内訳」）
- 投資判断: 読み手（その場で計算を含む、テストを除く）に`src/value/tanuki_valuation/`・`src/value/tanuki_score/`・`src/value/hypecore/`・`src/tail/`・`src/market/macro_pulse/`・`docs/market-monitor/macro-pulse/index.html`があれば「効く」
- 残す／消す: 段階2の判断（2026-10-23、Koichiさんの判断）。手で動かすスクリプト3つと修復のスクリプト1つ、05_events.csv の誰も読まない6列を消した。zq_price・zq_ticker（cuts_implied を作った時点の入力の記録）と 05_audit.py は残す（下の注記）。スクリプトの`DECISIONS`に写す。消したものは下の「消したもの」に記録だけ残す

**件数**: 出力 7（ファイル 6・人が見るもの 1）／読み手0件 0／テストだけ 0／1件 0／2件以上 6／規則の二重 12（検出式 16種のうち。MACRO PULSEのファイルを含むもの 2）

**画面の項目**: 画面が読むキー 48（この段階の書き手あり 48・ほかの書き手あり 0・画面の中で作る値 0・**書き手なし 0**）／書き手があるのに誰も読まないキー **2**

**止める条件**（1割）: 書き手が外にもある・書き手のパスを決めきれない 0＋0件（0.0%）、読み方を決めきれない読み込み 0件＋ファイル名で当たった行 0件／解決できた読み込み 24件＋ファイル名で当たった行 22件（0.0%）→ 当たらない

読み手の照合（2026-10-21から）: ファイル名で当たった行は、その行のパスを組み立て（os.path.join・f文字列・Path の /・モジュールの定数・JS の + と定数・ワークフローの代入）までたどって解決し、出力のパスの型と全体で照合する。同じ名前の別のフォルダのファイルを読む行・ファイル名を比べるだけの行は読み手にしない。ワークフローの cp・rsync で写したファイルを読む行は、元の出力の読み手とする

## 消したもの（段階2、2026-10-23）

- `scripts/analysis/macro_events_sp500_asof_repair.py・src/market/macro_pulse/05_backfill_nfp_mom.py（と控えの 05_events.csv.bak_{datetime}）`: 削除（2026-10-23、Koichiさんの判断）。1回だけの修復・変換で適用済み（sp500_t0_asof は2026-10-04のM-3、NFPの前月比は2026-07-08の671c5cc0d6）。参照するテストも消した
- `src/market/macro_pulse/05_import_history.py`: 削除（2026-10-23、Koichiさんの判断）。CLAUDE_CODE_START.md・MACRO_PULSE_LOGIC_INVENTORY.md 等に履歴の作り直しに使う手順が無い（あるのは過去の実行・復旧の記録だけ）
- `05_events.csv の analysis・ret_t1・ret_t5・ret_t10・ret_t20・surprise_pct の列と書き込み`: 削除（2026-10-23、Koichiさんの判断）。誰も読まない（画面の COL の対応表にだけあった行も消した）。analysis は全行が空、ret_t* は sp500_t0・sp500_tN から、surprise_pct は surprise・consensus から作り直せる。05_main.py の --fill-returns・--recalc・新しい行の書き込みもやめた。点数・局面・比較バーの過去の点数・週次の解説の入力・recalc と fill-returns の結果は前後で同じ（消した列のほか）、check_macro_pulse.py は前後とも一致67・不一致5・判定不能1

## 出力（読み手0件 → テストだけ → 1件 → 2件以上）

| 区分 | 出力（パスの型） | 何か | 書き手 | 読み手の数（その場で計算） | 画面 | 投資判断 | 定義 | 残す／消す |
|---|---|---|---|---|---|---|---|---|
| 2件以上 | `docs/market-monitor/macro-pulse/data/05_events.csv`<br>（1ファイル） | 指標の発表ごとの行（実績・予想・サプライズ・その時点のFF金利・10Y-2Y・HY・VIX・S&P500とt+1/5/10/20の終値）。景気後退リスクの点数の元。過去の行は手で動かすスクリプトと修復のスクリプト（4つとも削除済み）も書いた | `05_main.py:save_events` | 3・テスト1 | market-monitor/macro-pulse/index.html（直接） | 効く: MACRO PULSE（景気後退リスクの点数・局面）・MACRO PULSEの画面 | — | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/macro-pulse/data/05_fed_context.csv`<br>（1ファイル） | FRBの政策の局面とFF金利・1年金利・利下げの織り込み。**AIの出力（Grok）**: regime・dominant_concern・dominant_label・ai_reason（FOMCの声明から。Grokが使えないときは数値から決め〈_fallback_regime〉、regime_source に出典を書く） | `05_main.py:update_fed_context` | 3・テスト1 | market-monitor/macro-pulse/index.html（直接） | 効く: MACRO PULSE（景気後退リスクの点数・局面）・MACRO PULSEの画面 | AS-IS-205 | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv`<br>（1ファイル） | 指標の発表の予定（週1回 update_schedule が作り直す）。手で入れる指標は actual 列に人が入力する | `05_main.py:ensure_schedule_csv`<br>`05_main.py:update_schedule` | 3 | market-monitor/macro-pulse/index.html（直接） | 効く: MACRO PULSE（景気後退リスクの点数・局面）・MACRO PULSEの画面 | AS-IS-240・AS-IS-242 | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/macro-pulse/data/05_liquidity.csv`<br>（1ファイル） | 流動性の指標の日次の行（M2・FRBのバランスシート・TGA・RRP・ネットの流動性・ステルス流動性の印・S&P500 等）。画面の流動性モニターと、TAILのレビュー（quarterly_review_generator.py）が読む | `05_main.py:update_liquidity_csv` | 5・テスト1 | market-monitor/macro-pulse/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由） | 効く: 保有銘柄の監視（TANUKI TAIL）・MACRO PULSE（景気後退リスクの点数・局面）・MACRO PULSEの画面 | AS-IS-204 | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/macro-pulse/data/05_meta.json`<br>（1ファイル） | 流動性の更新の時刻（generated_at）。画面の最終更新の表示 | `05_main.py:update_liquidity_csv` | 2 | market-monitor/macro-pulse/index.html（直接） | 効く: MACRO PULSEの画面 | AS-IS-196 | 残す（2026-10-23） |
| 2件以上 | `docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv`<br>（1ファイル） | 週次の点数・局面と解説。**AIの出力（Grok）**: summary・factor_analysis・watchpoints・indicator_comments（model は使ったモデル）。score・phase・score_change_1w・score_change_1m・indicator_deltas・surprise_alerts はPythonの計算。TAILのレビューも読む | `05_main.py:save_weekly_analysis` | 5・テスト2 | market-monitor/macro-pulse/index.html（直接）<br>portfolio/tail/detail.html（経由）<br>portfolio/tail/index.html（経由） | 効く: 保有銘柄の監視（TANUKI TAIL）・MACRO PULSE（景気後退リスクの点数・局面）・MACRO PULSEの画面 | — | 残す（2026-10-23） |

### 人が見るもの（Step Summary・Discord）

| 出力 | 何か | 書き手 | 起動元のワークフロー |
|---|---|---|---|
| `Discord（DISCORD_WEB_HOOK）` | 手で入れる指標・実績の入っていない指標のリマインドと、FOMCの局面・週次のAIの解説の通知 | `05_main.py:send_discord` | .github/workflows/MACRO_PULSE_Update.yml |

## 読み手の内訳

### `docs/market-monitor/macro-pulse/data/05_events.csv`（2件以上）
- 起動元: .github/workflows/MACRO_PULSE_Update.yml
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 234・250・252・254・255・256（経由 `browser_checks/check_macro_pulse.py::read_csv`）
- Python・保存済みファイル: `src/market/macro_pulse/05_main.py` 行 50・522・844・1061・1789・2473・2480・2489・2503・2617・2626（経由 `src/market/macro_pulse/05_main.py::load_events`）
- JS・HTML・保存済みファイル: `docs/market-monitor/macro-pulse/index.html` 行 743
- テスト・保存済みファイル: `tests/test_macro_pulse_accuracy.py` 行 263・399（経由 `src/market/macro_pulse/05_main.py::load_events`）

### `docs/market-monitor/macro-pulse/data/05_fed_context.csv`（2件以上）
- 起動元: .github/workflows/MACRO_PULSE_Update.yml
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 234・250・252・254・255・256（経由 `browser_checks/check_macro_pulse.py::read_csv`）
- Python・保存済みファイル: `src/market/macro_pulse/05_main.py` 行 52・777・1834・2480・2506・2617・2626（経由 `src/market/macro_pulse/05_main.py::get_financial_context`、`src/market/macro_pulse/05_main.py::run_weekly_analysis`）
- JS・HTML・保存済みファイル: `docs/market-monitor/macro-pulse/index.html` 行 744・3131
- テスト・保存済みファイル: `tests/test_macro_pulse_accuracy.py` 行 263・399（経由 `src/market/macro_pulse/05_main.py::run_weekly_analysis`）

### `docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv`（2件以上）
- 起動元: .github/workflows/MACRO_PULSE_Update.yml
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 234・250・252・254・255・256（経由 `browser_checks/check_macro_pulse.py::read_csv`）
- Python・保存済みファイル: `src/market/macro_pulse/05_main.py` 行 51・508・623・646・2472・2476・2491・2617・2626（経由 `src/market/macro_pulse/05_main.py::load_schedule`）
- JS・HTML・保存済みファイル: `docs/market-monitor/macro-pulse/index.html` 行 745

### `docs/market-monitor/macro-pulse/data/05_liquidity.csv`（2件以上）
- 起動元: .github/workflows/MACRO_PULSE_Update.yml
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 62・123・308・409・458（経由 `browser_checks/check_dependency_map.py::expected_hollow_rally_trigger`）
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 234・250・252・254・255・256（経由 `browser_checks/check_macro_pulse.py::read_csv`）
- Python・保存済みファイル: `src/market/macro_pulse/05_main.py` 行 54
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 237・256・1541・1744・1780（経由 `src/tail/quarterly_review_generator.py::load_macro_context`）
- JS・HTML・保存済みファイル: `docs/market-monitor/macro-pulse/index.html` 行 2768
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_macro_context`）
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/index.html`

### `docs/market-monitor/macro-pulse/data/05_meta.json`（2件以上）
- 起動元: .github/workflows/MACRO_PULSE_Update.yml
- 最上位のキー（括弧はキーの名前が出てくる読み手のファイルの数）: `generated_at`(2)
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 257
- JS・HTML・保存済みファイル: `docs/market-monitor/macro-pulse/index.html` 行 2769

### `docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv`（2件以上）
- 起動元: .github/workflows/MACRO_PULSE_Update.yml
- Python・保存済みファイル: `browser_checks/check_dependency_map.py` 行 65・102・246・409・458（経由 `browser_checks/check_dependency_map.py::expected_weekly_score`）
- Python・保存済みファイル: `browser_checks/check_macro_pulse.py` 行 234・250・252・254・255・256（経由 `browser_checks/check_macro_pulse.py::read_csv`）
- Python・保存済みファイル: `src/market/macro_pulse/05_main.py` 行 53・1379・1804・1847・2480・2617（経由 `src/market/macro_pulse/05_main.py::load_weekly_analysis`）
- Python・保存済みファイル: `src/tail/quarterly_review_generator.py` 行 236・242・1541・1744・1780（経由 `src/tail/quarterly_review_generator.py::load_macro_context`）
- JS・HTML・保存済みファイル: `docs/market-monitor/macro-pulse/index.html` 行 746
- テスト・保存済みファイル: `tests/test_macro_pulse_accuracy.py` 行 263・399（経由 `src/market/macro_pulse/05_main.py::load_weekly_analysis`）
- テスト・保存済みファイル: `tests/test_tail_review_latest.py` 行 58（経由 `src/tail/quarterly_review_generator.py::load_macro_context`）
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/detail.html`
- 画面（経由（src/tail/quarterly_review_generator.py → docs/portfolio/tail/data/review_queue.json））: `docs/portfolio/tail/index.html`

## 画面の項目

画面（`docs/market-monitor/macro-pulse/index.html`）のJSが読むキー（`.key`・`?.key`・`['key']`。この段階のJSONか、ページが読むJSONにあるキー、またはsnake_caseのもの）。書き手は、この段階の出力のJSON（全ファイル、入れ子8段まで）と、ページが読む.jsonの文字列で照合したdocs/のJSONのキーで判定する。画面の言葉は、その行と前の3行の日本語の文字列（推定）。

### 書き手なし（画面が読むのに、どのJSONにも無いキー）


### 書き手があるのに誰も読まないキー（この段階のJSONのキーで、書き手のファイルとテストを除くコードに読みの形で出てこない）

- `zq_price`（`docs/market-monitor/macro-pulse/data/05_fed_context.csv`）: 残す: cuts_implied を作った時点の入力の記録（1年の金利の取り方と値。後から取り方が変わっても、その行が何を見たかを残す）
- `zq_ticker`（`docs/market-monitor/macro-pulse/data/05_fed_context.csv`）: 残す: cuts_implied を作った時点の入力の記録（1年の金利の取り方と値。後から取り方が変わっても、その行が何を見たかを残す）

### `docs/market-monitor/macro-pulse/index.html`（48項目）

| 項目（画面の言葉、推定） | 元のキー | 書き手 | 行 | 投資判断に使っているか（残す／消す） |
|---|---|---|---|---|
| — | `actual` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv` | 864・884・916・2213 | 残す（2026-10-23） |
| — | `ai_reason` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1078 | 残す（2026-10-23） |
| — | `analysis_date` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2591・2591・2604・2642 | 残す（2026-10-23） |
| — | `consensus` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv` | 865・866・2337・2337 | 残す（2026-10-23） |
| — | `cuts_implied` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1076 | 残す（2026-10-23） |
| 現在 | `date` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 887・958・1109・1163 | 残す（2026-10-23） |
| — | `dominant_concern` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1077 | 残す（2026-10-23） |
| — | `dominant_label` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1077 | 残す（2026-10-23） |
| — | `easing_check` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 3081 | 残す（2026-10-23） |
| — | `factor_analysis` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2646 | 残す（2026-10-23） |
| — | `fed_balance` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2828・2828 | 残す（2026-10-23） |
| — | `ff_current` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1074 | 残す（2026-10-23） |
| — | `fomc_date` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1080 | 残す（2026-10-23） |
| — | `generated_at` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_meta.json` | 3168・3168 | 残す（2026-10-23） |
| 前週比 | `h41_date` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2948・2948・2966・2966 | 残す（2026-10-23） |
| — | `hy_spread` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2829・2829・2844・2845 | 残す（2026-10-23） |
| 指標名 | `indicator` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv` | 832・886・1163・1774 | 残す（2026-10-23） |
| — | `indicator_comments` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2648 | 残す（2026-10-23） |
| 指標名:値:週差:月差;... | `indicator_deltas` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2665 | 残す（2026-10-23） |
| — | `known_at` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv` | 891・891 | 残す（2026-10-23） |
| — | `m2_yoy_pct` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2840・2840・2840 | 残す（2026-10-23） |
| — | `m2_yoy_pctile` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2841・2841・2841 | 残す（2026-10-23） |
| — | `model` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2753 | 残す（2026-10-23） |
| — | `net_liq_decline_weeks` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 3029・3029 | 残す（2026-10-23） |
| — | `net_liq_wow_pct` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2964・2964・2964 | 残す（2026-10-23） |
| — | `net_liquidity` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2827・2827 | 残す（2026-10-23） |
| — | `phase` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2604・2644 | 残す（2026-10-23） |
| — | `record_date` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1016・1016 | 残す（2026-10-23） |
| LAYER 1 — FRB政策意図 | `regime` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1073・3095・3137 | 残す（2026-10-23） |
| — | `regime_source` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1079 | 残す（2026-10-23） |
| 発表予定日 | `release_date` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv` | 2310・2314 | 残す（2026-10-23） |
| — | `revised_actual` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv` | 898 | 残す（2026-10-23） |
| — | `revised_at` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv` | 899・899 | 残す（2026-10-23） |
| 先行1ヶ月 | `score` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 1415・1416・1419・1518 | 残す（2026-10-23） |
| — | `score_change_1m` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2650 | 残す（2026-10-23） |
| — | `score_change_1w` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2649 | 残す（2026-10-23） |
| — | `sp500_5d_pct` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 2963・2963・2963 | 残す（2026-10-23） |
| — | `sp500_t0_asof` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv` | 1118 | 残す（2026-10-23） |
| データなし | `status` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_indicator_schedule.csv` | 3161・3166 | 残す（2026-10-23） |
| — | `stealth_absorb_weeks` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 3028・3028 | 残す（2026-10-23） |
| — | `stealth_alert` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 3030 | 残す（2026-10-23） |
| — | `stealth_signal` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_liquidity.csv` | 3011 | 残す（2026-10-23） |
| — | `summary` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2645 | 残す（2026-10-23） |
| — | `surprise` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv` | 867 | 残す（2026-10-23） |
| — | `surprise_alerts` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2608 | 残す（2026-10-23） |
| — | `updated_at` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_events.csv`<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 893・1109・2753 | 残す（2026-10-23） |
| — | `watchpoints` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_weekly_analysis.csv` | 2647 | 残す（2026-10-23） |
| — | `zq_rate` | この段階の書き手あり<br>`docs/market-monitor/macro-pulse/data/05_fed_context.csv` | 1075 | 残す（2026-10-23） |

## 画面の区画

HTMLの`<!-- 見出し -->`の行で区切った区画ごとに、画面が読むキー。キーを読むJSの行から、同じ関数の中で近く（前後15行）に出てくる要素のid（`getElementById`・`querySelector('#…')`）を探し、そのidがある区画に入れた（関数の中に無ければ、その関数を呼ぶ行で探す。2段まで。推定）。「投資判断に使っているか」はKoichiさんが決める欄（空）。

### `docs/market-monitor/macro-pulse/index.html`

| 区画 | 元のキー（書き手の有無） | 投資判断に使っているか |
|---|---|---|
| （ページの先頭） | `date`（この段階の書き手あり）<br>`status`（この段階の書き手あり） |  |
| Notices | `actual`（この段階の書き手あり）<br>`consensus`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり）<br>`known_at`（この段階の書き手あり）<br>`revised_actual`（この段階の書き手あり）<br>`revised_at`（この段階の書き手あり）<br>`score`（この段階の書き手あり）<br>`surprise`（この段階の書き手あり）<br>`updated_at`（この段階の書き手あり） |  |
| FED Regime Bar | `ai_reason`（この段階の書き手あり）<br>`cuts_implied`（この段階の書き手あり）<br>`dominant_concern`（この段階の書き手あり）<br>`dominant_label`（この段階の書き手あり）<br>`ff_current`（この段階の書き手あり）<br>`fomc_date`（この段階の書き手あり）<br>`regime`（この段階の書き手あり）<br>`regime_source`（この段階の書き手あり）<br>`zq_rate`（この段階の書き手あり） |  |
| Ticker | `actual`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり）<br>`score`（この段階の書き手あり）<br>`sp500_t0_asof`（この段階の書き手あり）<br>`updated_at`（この段階の書き手あり） |  |
| 💧 流動性モニター | `actual`（この段階の書き手あり）<br>`consensus`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`easing_check`（この段階の書き手あり）<br>`fed_balance`（この段階の書き手あり）<br>`generated_at`（この段階の書き手あり）<br>`h41_date`（この段階の書き手あり）<br>`hy_spread`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり）<br>`known_at`（この段階の書き手あり）<br>`m2_yoy_pct`（この段階の書き手あり）<br>`m2_yoy_pctile`（この段階の書き手あり）<br>`net_liq_decline_weeks`（この段階の書き手あり）<br>`net_liq_wow_pct`（この段階の書き手あり）<br>`net_liquidity`（この段階の書き手あり）<br>`record_date`（この段階の書き手あり）<br>`regime`（この段階の書き手あり）<br>`revised_actual`（この段階の書き手あり）<br>`revised_at`（この段階の書き手あり）<br>`sp500_5d_pct`（この段階の書き手あり）<br>`status`（この段階の書き手あり）<br>`stealth_absorb_weeks`（この段階の書き手あり）<br>`stealth_alert`（この段階の書き手あり）<br>`stealth_signal`（この段階の書き手あり）<br>`surprise`（この段階の書き手あり）<br>`updated_at`（この段階の書き手あり） |  |
| ① 景気フェーズ判定 | `actual`（この段階の書き手あり）<br>`score`（この段階の書き手あり） |  |
| スコア比較バー | `score`（この段階の書き手あり） |  |
| DESIGN-13: マクロサプライズバナー（週次更新） | `actual`（この段階の書き手あり）<br>`analysis_date`（この段階の書き手あり）<br>`consensus`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`factor_analysis`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり）<br>`indicator_comments`（この段階の書き手あり）<br>`indicator_deltas`（この段階の書き手あり）<br>`known_at`（この段階の書き手あり）<br>`model`（この段階の書き手あり）<br>`phase`（この段階の書き手あり）<br>`revised_actual`（この段階の書き手あり）<br>`revised_at`（この段階の書き手あり）<br>`score`（この段階の書き手あり）<br>`score_change_1m`（この段階の書き手あり）<br>`score_change_1w`（この段階の書き手あり）<br>`summary`（この段階の書き手あり）<br>`surprise`（この段階の書き手あり）<br>`surprise_alerts`（この段階の書き手あり）<br>`updated_at`（この段階の書き手あり）<br>`watchpoints`（この段階の書き手あり） |  |
| AI週次解説（①の直後） | `analysis_date`（この段階の書き手あり） |  |
| ② 指標ヘルスバー | `actual`（この段階の書き手あり）<br>`score`（この段階の書き手あり） |  |
| score-history chart | `date`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり）<br>`score`（この段階の書き手あり） |  |
| ③→④ 過去の後退局面との類似度 | `date`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり）<br>`score`（この段階の書き手あり） |  |
| ⑤ 直近の動き | `actual`（この段階の書き手あり）<br>`date`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり） |  |
| ⑥ 今後のスケジュール | `consensus`（この段階の書き手あり）<br>`indicator`（この段階の書き手あり）<br>`release_date`（この段階の書き手あり）<br>`score`（この段階の書き手あり） |  |
| （JSで作る要素 #stealthLayer1） | `regime`（この段階の書き手あり） |  |

## FINDINGS 2026-10-15 の読み手0件の6種（F3）

| 名前 | MACRO PULSEか | この一覧の出力 | 書かれている場所（grep） |
|---|---|---|---|
| `spac_shell_detection_log` | いいえ（後の段階） | — | — |
| `share_unit_fix_log` | いいえ（後の段階） | — | — |
| `low_base_effect` | いいえ（後の段階） | — | `src/value/hypecore/hypecore.py:1050`<br>`src/value/hypecore/hypecore.py:1095` |
| `fields_missing` | いいえ（後の段階） | — | — |
| `maturity_monitor` | いいえ（後の段階） | — | `src/value/adjusted_eps_analyzer/pipeline.py:22` |
| `market_data_violations_log` | いいえ（後の段階） | — | — |

## 規則が2か所以上にあるもの

検出式（定義の書き方の型）は人が決め、場所はgrepで出した。同じ式に当たるファイルが2つ以上なら二重。MACRO PULSEのファイルを含むものだけでなく全部を示す。

| 規則 | ファイル数 | 場所 | MACRO PULSEを含む |
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
| 景気後退リスクの点数（8指標の段階の点の合計）の計算 | 2 | `docs/market-monitor/macro-pulse/index.html:1443`<br>`docs/market-monitor/macro-pulse/index.html:2389`<br>`src/market/macro_pulse/05_main.py:1496`<br>`src/market/macro_pulse/05_main.py:1616` | はい |
| 指標ごとの段階の信号（しきい値で点を付ける） | 2 | `docs/market-monitor/macro-pulse/index.html:1410`<br>`src/market/macro_pulse/05_main.py:1479`<br>`src/market/macro_pulse/05_main.py:1492` | はい |
| JSONの書き込み（一時ファイル→置き換え）の実装 | 2 | `common/macro_data/fetcher.py:71`<br>`common/market_data/fetcher.py:175` | いいえ |

1か所だけのもの: 成長率の上限・下限（1）・取引日（市場の営業日）の決め方（取引所の暦を直接読む所・手で書いた営業日の計算）（1）・FREDのAPIを直接呼ぶ所（系列の値・公表日）（1）・AI（Grok）のAPIの宛先の直書き（1）

## 本番外のコード（src/market/macro_pulse）

参照: ワークフロー（ファイル名・モジュール名が出てくるもの）／ほかのコード（importか、ファイル名・モジュール名の文字列。同じフォルダは別欄）／テスト

候補: 名前を挙げたもの（`05_audit.py`）と、段階のフォルダの.pyのうちワークフロー・ほかのコード・同じフォルダのどれからも呼ばれないもの

### `src/market/macro_pulse`（1ファイル・197行）

| ファイル | 行数 | ワークフロー | ほかのコード | 同じフォルダ | テスト | 実行の記録・注記 | 残す／消す／移す |
|---|---|---|---|---|---|---|---|
| `05_audit.py` | 197 | — | — | — | `tests/test_macro_pulse_logic.py` | 手で動かす05_events.csvの監査（重複の行等、読むだけで書かない）。ワークフローからは呼ばれず、テストが重複の判定を使う。重複の判定の本体は2026-09-10に05_main.pyへ移した（f38aae4303） | 残す（2026-10-23） |

## 止める条件の内訳

- `outputs_with_writer_outside_layer`: 0件
- `unresolved_write_sites`: 0件
- `python_read_sites_unresolved_in_files_mentioning_sec_data`: 0件
