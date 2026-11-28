# 二重実装の規則 14件・Python の増加の内訳・古い経路を消す順番（2026-11-28、読み取りだけ）

HEAD `4f9723d2a9`。コードは変えていない。数え方は `PROJECT_STATUS.md` の「2026-11-28 終業の数」と同じ（11-03 の `920e5cf8e1` との差）。
「正にする案」「規模」「投資判断に効くか」は読み取りからの**案**で、決めるのは Koichi さん。確かめていないものは「未確認」と書いた。

## 1. 二重実装の規則 14件

数えた14件＝出力の一覧の CHECK-INV-3 の規則の種類 12（同じ種類が層ごとに複数行出るので WARN は 20 行）＋ SYSTEM_MAP の MAC-11・MAC-20（【二重】）。
規模: 小＝1か所の付け替え（〜1日）／中＝試算・scope・印が要る（計算方法の変更の手順）／大＝全銘柄の試算が要る。

| # | 規則 | 二重になっている場所（ファイル・関数） | 正にする案 | 当たる BACKLOG | 規模 | 投資判断の数字に |
|---|---|---|---|---|---|---|
| 1 | Q4 の逆算（年次−Q1〜Q3） | `common/sec_data/parser.py:2325`／`common/sec_data/q4_implied.py:191`（`implied_q4_value`）／`src/value/adjusted_eps_analyzer/extract_key_facts.py:720,863,891` | `q4_implied.py`（R-C で決めた1か所） | parser 側は PARSER-LAYER3-SINGLE-SOURCE-1（段2a）。EPS 側は**なし** | 中 | 効く（EPS・TTM→理論株価） |
| 2 | 同じ期の値が複数あるときの選び方（提出日が最新） | `fact_selection.py:19`（`select_latest_filed`）／`bs_lines.py:292,443`／`cf_lines.py:461,471`／`layer3_builder.py:1031,1254`／`normalizer.py:168,431`／`share_unit_fix.py:104`／`split_adjust.py:156`／`src/tail/edgar_rss_monitor.py`／`extract_key_facts.py`／`tools/sim/parser_l3/val_guard_proto.py:138`（削除済み） | `fact_selection.py`。ただし `share_unit_fix`（accn で同点を決める）・`split_adjust` は目的が違い**別規則かもしれない**（未確認） | PARSER-LAYER3（parser 側のみ）。他は**なし** | 中 | 効く |
| 3 | データの古さ・遅れの判定 | `common/sec_data/freshness.py:83`（`lag_days`）／`parser.py:3048`（`_freshness`）／`report_consistency_check.py:1776`／`common/system_health.py:975`／`src/value/stonks_silo/financial_as_of.py:70`（`is_stale`）／画面 JS 2（TAIL `_updateReviewStaleness`・EPS `stalePeriod`） | `freshness.py` の `lag_days` を呼ぶ形に。画面は出力に印を持たせて読むだけ | **なし** | 小〜中 | 効かない（表示・警告） |
| 4 | TTM（直近4四半期の合計） | `common/sec_data/ttm_calculator.py:242`（`calc_ttm_series`）／`src/value/adjusted_eps_analyzer/pipeline.py:402`（`calculate_ttm`）／`src/value/hypecore/hypecore.py:293`（`rolling(4).sum()`） | `ttm_calculator.py` | **なし** | 中 | 効く（HypeCore stage・EPS） |
| 5 | 会計年度・会計年度末 | `parser.py:455,464`／`utils.py:112,293,368`（`determine_fiscal_year`）／`src/tail/edgar_rss_monitor.py:137` | `utils.py` | PARSER-LAYER3（parser 側）。TAIL 側は**なし** | 小 | 効く（期の割り当て） |
| 6 | 期の名前（四半期ラベル） | `edgar_rss_monitor.py:375`／`src/tail/xbrl_segment_fetcher.py:180`／`tanuki_valuation/calculator/segment_growth_xbrl.py:144` | 共通の1関数を新設して3か所が呼ぶ | **なし** | 小 | TAIL は表示のみ。TANUKI のセグメント成長は効く可能性（未確認） |
| 7 | 理論株価の割安・割高のしきい値（upside） | `tanuki_valuation/pipeline.py:590-594`（30/10/0）／`docs/value-monitor/tanuki_score`・`tanuki_valuation/index.html`・`stock.html`・`docs/portfolio/index.html`（JS の写し） | pipeline が段の名前を出力に書き、画面は読むだけ | **なし**（pipeline のコメントに ARCH-SCORE-SYNC-1 が出るが BACKLOG には無い） | 中 | 効く（判定・BUY のゲート） |
| 8 | β・WACC | `beta_fetcher.py:110`（`calc_capped_beta`）／`calculator/wacc.py:47,54`／`data_fetcher.py:1194`（`_determine_beta`） | `wacc.py` と `beta_fetcher.py` を入口にし `data_fetcher` の判断を移す | **なし** | 中 | 効く（IV） |
| 9 | FCF の基準の選び方・フォールバック | `common/sec_data/reader.py:295`（`get_fcf_5yr_avg`）／`calculator/adjustments.py:199`（`determine_fcf_base`）／`data_fetcher.py:126,1178` | FCF-DEFINITION で作った `common/sec_data/fcf.py` に「基準の選び方」まで寄せる | 定義は済（FCF-DEFINITION-SINGLE-SOURCE-1）。**選び方は BACKLOG なし** | 中 | 効く（IV） |
| 10 | JSON の書き込み（一時ファイル→置き換え） | `common/macro_data/fetcher.py:71`／`common/market_data/fetcher.py:175`（`_atomic_write_json`） | 共通の1関数（新設案）。ほかの `os.replace` 3か所も同時に点検 | **なし** | 小 | 効かない |
| 11 | 景気後退リスクの点数 | `src/market/macro_pulse/05_main.py:1588`（`_compute_current_score`）／`docs/market-monitor/macro-pulse/index.html:1494`（`computeCurrentScore`） | Python の出力を正に。画面は計算せず読む | MACROPULSE-RECESSION-SCORE-STEPS-1・MARKETPULSE-REVIEW-EPIC-1（型P） | 中 | 表示・局面の読みに効く（売買の数字ではない） |
| 12 | 指標ごとの段階の信号 | `05_main.py:1567`（`_step_score`）／画面 `index.html:1461`（`stepSignal`）・`SIGNAL_STEPS`（写し） | `common/step_rules.py`（11-27 に作った部品）の境目を正に。画面の写しは廃止 | 同上。写しの一致は `tests/test_macro_pulse_accuracy.py` が検査している | 中 | 同上 |
| 13 | MAC-11（10Y-2Y の −0.2/0.5） | 画面 `idxLatestVal` と L2（SYSTEM_MAP に「意図的」と注記） | 閾値を設定1か所に | MARKETPULSE-REVIEW-EPIC-1 | 小 | 効かない |
| 14 | MAC-20（LAYER 1 = `fed.regime` の別の読み方） | `updateStealthLayer1()` と MAC-04 | 1つの読み方に | FED-POLICY-PAGE-1 に含められる（未確認） | 小 | 効かない |

R-E1・R-E2・R-C との対応:
- **R-E1（市場の暦・銘柄の集合）・R-E2（外部 API の呼び方）は14件に残っていない**（検出式は0件。`mcal` 直接 import・`requests.get` は決める所だけ。`requests.post` は Discord への通知2か所〈05_main.py:620・edgar_rss_monitor.py:390〉で、外部データの取得ではない）。
- **R-C の続き**＝1（Q4）・2（提出日）・4（TTM）・5（会計年度）。R-C は first_filed・first_val・Q4 の逆算を「年次と同じ時点」にしたところまでで、**parser・EPS ANALYZER・HypeCore の独自の実装は残っている**。
- 画面の写し＝7・11・12・13・14（計算の二重ではなく、Python と JS の二重）。

## 2. 11-03 から今日までの Python（本番側）+7,616行・+34ファイルの内訳

Python 全体の差は追加 85・削除 3・変更 95 ファイル（うち本番側の追加は約36、削除は `stonks_flag_rule.py` の1）。**この数え方は 11-03 の 71,323行 → 78,939行（+7,616）に一致**。

| 区分 | 行 | 主なファイル |
|---|---|---|
| A 一本化の新経路（SEC Layer3） | +1,646 | `cf_lines` 577・`bs_lines` 472・`layer3_builder` +266・`quarterly_bs` 184・`val_guard` 131・`retention` |
| B 定義の共通部品 | +1,968 | `profitability` 670・`multiples` 470・`sector_master` 363・`shares_base` 128・`step_rules` 120・`dilution` 109・`stonks_target` 108 |
| C 定義v2の並行計算 | +361 | `stage_v2`（旧 stage と並行して持つ設計） |
| D 守り・監視 | +1,420 | `split_reconcile` 264・`system_health` +213・`source_watch` 176・`earnings_pending` 148・`fomc_calendar` 130・`futures_roll` 127・`estimates_history` 114・`value_freshness` 102・`external_watch` 76 ほか |
| E 機能 | +223 | `reverse_growth` 81・`handset_growth` 78・`guidance_gap` 64 |
| F 試算・移行の道具 | +886 | `tools/sim` 722（`sim.py` 260・`val_guard_proto` 172・`parser_vs_l3` 139 ほか）・`scripts/migrate_*` 164 |
| G 一覧の道具 | +218 | `scripts/inventory` |
| H その他 | +894 | `market_pulse` +181・`macro_data` +130・`tanuki_valuation` +158・`market_data/fetcher` +134 ほか |

減ったのは `src/value/stonks_silo/analyzer.py` −173・`stonks_flag_rule.py` −122（削除）・`sector_classifier_v2` −53 など（数ファイルの減りの合計で −400 弱。+7,616 は差し引きの値）。
大づかみには、**「新しい一本化の経路」と「定義の共通部品」の A＋B＋C（約4,000行）が、古い側を消さないまま足された分**。D は新機能ではなく守り。F はまるごと本番の外。

### 2-1 新しい経路を足したのに古い経路が残っているもの（消せる候補）

| 古い側 | 新しい側 | 今も呼ばれているか | 消す条件 |
|---|---|---|---|
| `parser.py` の年次・四半期の独自選択（4,142行のうち該当部分の行数は**未測定**） | `cf_lines`・`bs_lines`・`quarterly_bs`（`quarterly_bs` は parser からしか呼ばれない） | 呼ばれている（本番） | 段2a の公開（12-01）＋確認。**触らない（`parser-layer3-stage2a`）** |
| `normalizer.py`（506行。`normalize` は `update.py` が呼ぶ。`_ytd_to_quarterly` は parser が import） | `layer3_builder.py`（提出日の選び方が `normalizer.py:168,431` と同形） | 呼ばれている | 段2a の後。Layer2 の読み手が残るかを調べてから |
| `extract_key_facts.py`（EPS。独自の Q4・提出日。`get_diluted_shares_from_facts` 42行は**どこからも呼ばれない**） | `q4_implied.py`・`fact_selection.py` | 本体は呼ばれている | 規則1・2 の実装と一緒 |
| `tools/sim/parser_l3/val_guard_proto.py`（172行。削除済み） | `common/sec_data/val_guard.py`（131行、本番に入り済み） | 呼ばれていない | **今すぐ消せる**（試作） |
| `tools/sim/parser_l3/parser_overlay.py`・`parser_redirect.py`・`parser_vs_l3.py`（計197行） | 段2a の試算の道具 | `parser_vs_l3` 以外は参照なし | 段2a の公開・確認の後 |
| `scripts/migrate_market_data_latest.py`（87行）・`migrate_market_data_comments_history.py`（77行） | 1回限りの移行 | latest は参照なし／comments_history は参照元あり（未確認） | 移行が済んでいるか確認してから |
| `common/market_data/calendar_universe.py::us_federal_business_days_add`（11行） | R-E1 の整理後の `nyse_trading_days_add` ほか | 本番からは呼ばれない（テストだけ） | 小 |
| 旧 stage（HypeCore の定義v1） | `stage_v2`（361行） | 両方呼ばれる（並行は設計） | STAGE-V2-ROLLOUT-1 で切り替えた後 |
| 業種の別の対応表 | `sector_master` | `sector_classifier_v2` は −53 行済み。ほかに残る表があるかは**未確認** | 点検が先 |

### 2-2 呼び出し元のない関数・ファイル（デッドコード候補）

非テストのコードからの参照が名前の出現数で0〜1のもの。**文字列・`getattr`・表で呼ぶものは拾えず、偽陽性がある**（特に `system_health.py` の `check_*`。`check_j_workflow_runs` はテストが4回呼ぶ）。消す前に1つずつ grep で確かめる。

- 関数・クラス 22件・210行: `src/tail/prediction_tracker.py::format_for_prompt` 51行、`extract_key_facts.py::get_diluted_shares_from_facts` 42行、`common/yfinance_utils.py::safe_yf_history` 18行（テスト4）、`src/value/stonks_silo/fetcher.py::available_tickers`、`common/sec_data/tickers.py::get_tickers_by_flag` 13行、`common/sec_data/fetcher.py::needs_quarterly_update`・`update_quarterly_run_state`、`quarterly_review_generator.py::_lookup_layer2_value`・`_calc_yoy_text_stage2`、`core_calculator.py::create_calculator`、`src/subport/day_trade/backtest.py::fetch_spy_qqq`、`browser_checks/market_pulse_elements.py::fmt_md・signed`、`collect_and_send.py::calc_hindenburg_active`（テスト5）、`value_freshness.py::stale_names`・`step_rules.py::check_table`（新規。テストだけ）、`system_health.py::check_m/n/o/p…`（偽陽性の疑いが強い）。
- ファイル 9件: `common/sec_data/newfield_q4_cutoff_check.py`（217行、一度きりの検査）、`scripts/migrate_market_data_latest.py`、`tools/sim` 4ファイル（273行）、`browser_checks` 2ファイル（手で動かす検査。`NON_PRODUCTION_DIRS` なので対象外にしてよい）、`src/subport/fg_level2_extended.py`（390行、AutoTrade の別系統。**本件の対象外**として触らない）。
- 合計の見込み: **本番側で消せる見込みは確認後に最大 約700行（デッドコード関数 約200＋ `newfield_q4_cutoff_check` 217＋ tools/sim・migrate 約400 を重複を除いて）**。ただし +7,616 のうち大きいのは使われている新経路なので、デッドコードの削除だけでは戻らない。

## 3. 二重を0にし、古い経路を消す順番の案

前提（動かさない）: 段2a（`parser-layer3-stage2a`）は12-01の公開まで触らない。手順55a〜c（11-29・11-30 の確認）と MARKETPULSE-REVIEW-EPIC-1 の移設（55c の後）は先に済ませる。**計算方法の変更が重なると手順55・scope の確認が混ざるので、段2a の公開・確認が済むまで計算方法を変える段（2・3 の中・中以上）には入らない。**

| 段 | いつ | 内容 | 二重の規則 | Python の行 | 読み手なし |
|---|---|---|---|---|---|
| 0 | 11-29〜11-30 | 手順55a・b・c の確認だけ（変更なし） | 14 | 変わらず | 0 |
| 1 | 12-01 | 段2a の公開（別ブランチの分が入る） | 14のまま（parser 側の場所が減るだけで、他の場所が残るため種は動かない） | **増える**（ブランチの分。未測定） | 0（SEC の INV-6b 16件はこの後に仕分け） |
| 2 | 段2a の確認後（目安12-08） | 消せる確実な古い側: `val_guard_proto`・`parser_*`・migrate の済んだもの・デッドコードのうち確認済み | 14 | **−400〜−700** | 0 |
| 3 | 同じ頃 | 小さい種を先に: 10（JSON 書き込み）・6（期の名前）・5（会計年度の TAIL 側と parser 側）・13・14 | **14→9** | 小（−100 前後。共通部品の分が増える） | 0 |
| 4 | 12月中旬 | R-C の続き（計算方法の変更の手順。試算・scope・印）: 2（提出日の選び方、normalizer を fact_selection へ）→1（EPS と parser の Q4）→4（TTM）→3 | **9→5** | −（`normalizer`・`extract_key_facts` の重複分。増減は試算で出る） | 0 |
| 5 | MARKETPULSE-REVIEW-EPIC-1 の移設後 | 画面の写し: 7・11・12（Python の出力を画面が読む形に） | **5→2** | JS の行が減る（Python は変わらずか微増） | 0 |
| 6 | 最後 | 8（β・WACC）・9（FCF の選び方）。IV に直接効くので全銘柄の試算が要る | **2→0** | 小 | 0 |

読み方: 「二重の規則を0」＝出力の一覧の検出式が1か所だけを指す状態。JS の写しの廃止は画面の挙動に関わるので、段5 は Playwright で前後を見る。**段2・3 は「消す・寄せる」だけなので、依頼として受けても終業の数は増えない（Python は減る）。段1 だけは入る行が増える。**

## 4. 未確認・限界

- 規則2 の `share_unit_fix`・`split_adjust` は別の規則の可能性（検出式が広い）。
- 規則3 の rcc・system_health・parser の「独自」は、`freshness.py` を呼んでいるのか独自なのかを個別に確かめていない。
- デッドコードの走査は名前の出現数で、動的な呼び出しは拾えない。`fg_level2_extended.py` は別系統で対象外。
- 行数の見込みは「増減の向き」を示すもので、段4 以降は試算の後に決まる。

## 5. 2026-11-28 追記（`[[DUPLICATE-RULES-ZERO-EPIC-1]]` の開始。読み取りの確認と、段2・3 の一部）

### 5-1 規則2・3 は別の規則か（読み取り）

**規則2（同じ期の値が複数あるときの選び方）: 一部が同じ規則。数は14のまま、範囲を狭める。**
検出の9か所（`val_guard_proto` の削除後）を1つずつ読んだ。
- **同じ規則（真の二重）**: `normalizer.py:168,431`（`_normalize_field`・`_index_quarterly_by_end` の中）と `layer3_builder.py:1031,1254`。関数名（`_ytd_to_quarterly`・`_index_quarterly_by_end`）も中身も写しで、Layer3 の側だけが後で伸びている（`restating` の引数）。**一本化の案**: `normalizer.py` の3関数（`_normalize_field`・`_ytd_to_quarterly`・`_index_quarterly_by_end`）を `layer3_builder.py` の版を呼ぶ形にして消す。parser が `normalizer._ytd_to_quarterly` を import しているので、`restating` なしの呼び方で値が同じかを全銘柄で試算する必要がある＝**段4**（計算方法の変更の手順）。
- **別の規則（目的が違う）**: `bs_lines.py:292`（直前の10-Kの申告を選ぶ）・`bs_lines.py:443`（最新の申告の1件）・`cf_lines.py:461,471`（CapEx の内訳が落ちた申告を避ける守り）・`share_unit_fix.py:104`（同点を accn で決める、単位の誤りの直し）・`split_adjust.py:156`（株数の出どころごとの最新値）。ここは申告や出どころを選ぶもので、「同じ期の事実の中の最新」ではない。
- **すでに1か所**: `extract_key_facts.py`（EPS）は `fact_selection.select_latest_filed` を呼んでいる。TAIL の `edgar_rss_monitor.py` は「前回より新しい提出か」の比較で、選び方ではない。
- 結論: 真の二重は `normalizer.py` ↔ `layer3_builder.py` の1組。検出式が広く9か所を拾っている（数え方の見直しは、この EPIC の間はゲートの変更になるため行わない）。

**規則3（データの古さ・遅れの判定）: 検出の7か所のうち4か所は別の規則か呼び出し。真の二重は1組。数は14のまま、範囲を7→3に狭める。**
- **呼び出し（二重でない）**: `report_consistency_check.py:1776`（CHECK-60）と `common/system_health.py` の [M] は、どちらも `common/sec_data/freshness.py`（`lag_days`・`SEC_LAG_WARN_DAYS`）を import して使う。
- **別の規則（検出の誤り）**: `parser.py:3048 _freshness`（複数の候補タグから最新の年・四半期のものを選ぶ並べ替えで、古さの判定ではない）・`docs/portfolio/tail/index.html` の `_updateReviewStaleness`（TAIL のレビューのログが何日前かで、SEC のデータではない）。
- **真の二重**: `src/value/stonks_silo/financial_as_of.py::is_stale`（四半期末から `STALE_DAYS_AFTER_QUARTER_END` 日を超えたら古い）と、`docs/value-monitor/adjusted_eps_analyzer/index.html::stalePeriod`（最新の提出日から10-Q 145日・10-K 190日を超えたら古い、JS）。どちらも「新しい四半期が出ていないか」を、**基準の日と日数を変えて**2か所で判定している。**一本化の案**: EPS ANALYZER の出力に Python 側で判定した `stale` を書き、画面は読むだけにする（STONKS は今の `is_stale` を残すか、同じ関数に寄せるかは基準の日の違い〈四半期末か提出日か〉を決めてから）。投資判断の数字には効かない（表示）。段3 の中で規模は小〜中だが、**EPS ANALYZER の画面に触るので、この EPIC の「値が変わらない」の条件を画面の前後で確かめてから**。

### 5-2 段3 の小さい種（今日の判定）

条件＝①計算の値が変わらない ②`parser-layer3-stage2a`（変更ファイルは `quarterly_bs.py`・`tests/test_quarterly_bs.py` と記録の文書だけ）と重ならない ③MACRO PULSE に触らない。

| # | 内容 | 判定 |
|---|---|---|
| 6 | 期の名前（四半期ラベル）の3実装 | **済（今日）**。`common/sec_data/utils.py::quarter_label(period_end, strict=False)` の1か所に。TAIL の2つは既定（読めなければ入力を返す）、TANUKI の `segment_growth_xbrl` は `strict=True`（読めなければ例外）で従来の動きのまま。1990〜2034 の日付 32,872 件＋日時形式＋不正な入力4件で、3つの旧実装と新しい関数の出力が同じ（差0）。3ファイルの定義を消して import に。二重の規則 14→13 |
| 5 | 会計年度・会計年度末 | **一本化する物が無い**。`parser.py` の `_detect_fiscal_end_month`・`_detect_fiscal_anchor_date` は `utils.py` を呼ぶ薄いラッパー（コメントに明記）。`edgar_rss_monitor.get_fiscal_year_end` は SEC の submissions API の `fiscalYearEnd`（MMDD）を取る関数で、XBRL の事実から推定する `utils.py` とは別。検出式の誤りに近い。**数え方から外すか（14→12）は Koichi さんの判断**（今日は外していない） |
| 10 | JSON の書き込み（`_atomic_write_json`） | **見送り**: 片方が `common/macro_data/fetcher.py`（MACRO PULSE の入力を書く層）なので、「MACRO PULSE に触らない」に当たる。手順55c の後に（`market_data/fetcher.py` との2か所） |
| 13・14 | MAC-11・MAC-20 | **見送り**（MACRO PULSE の画面。手順55c の後。依頼の指示どおり） |
| 1・2・4 | Q4・提出日・TTM | **対象外**（値が変わりうる＝段4） |
| 7・11・12 | 画面の写し | **対象外**（段5。MARKETPULSE-REVIEW-EPIC-1 の移設後） |
| 8・9 | β・WACC・FCF の選び方 | **対象外**（段6。IV に効く） |

つまり、今日の条件を満たす一本化は **6 の1件だけ**。

### 5-3 デッドコードの候補（確認済みの一覧。**消すのは段2、今日は一覧だけ**）

確かめた方法: 候補の名前を `git grep -w`（ドキュメントと一覧を除く全ファイル）で数え、テストからの参照・非テストからの参照・動的な読み（`getattr`・`globals()`・`importlib`）・ワークフローからの起動を見た。`common/system_health.py` は `getattr`・`globals()`・`importlib` を使わず、`System_Health.yml` は `python common/system_health.py` で起動する（`check_*` の呼び出しは表でなく関数の直呼びで、下のラッパーは呼ばれない）。

**A. 呼ばれていない（非テスト0・テスト0）。本当に消せる（段2）**
- `browser_checks/market_pulse_elements.py::fmt_md`・`signed`（手で動かす検査の中の使われない部品）
- `common/sec_data/fetcher.py::needs_quarterly_update`・`update_quarterly_run_state`
- `common/sec_data/tickers.py::get_tickers_by_flag`
- `common/system_health.py::check_m_sec_freshness`・`check_n_headlines`・`check_o_intraday`・`check_p_external_change`（3行のラッパー。本体の `_check_m` などは使われている）
- `src/subport/day_trade/backtest.py::fetch_spy_qqq`
- `src/tail/prediction_tracker.py::format_for_prompt`（51行）
- `src/tail/quarterly_review_generator.py::_calc_yoy_text_stage2`・`_lookup_layer2_value`
- `src/value/adjusted_eps_analyzer/extract_key_facts.py::get_diluted_shares_from_facts`（42行）
- `src/value/stonks_silo/fetcher.py::available_tickers`
- `src/value/tanuki_valuation/core_calculator.py::create_calculator`

**B. テストだけが呼ぶ（消すなら関数とテストを一緒に）**
- `common/system_health.py::check_j_workflow_runs`（テスト4）・`common/yfinance_utils.py::safe_yf_history`（テスト4）・`src/market/market_pulse/collect_and_send.py::calc_hindenburg_active`（テスト5）・`common/market_data/calendar_universe.py::us_federal_business_days_add`（テスト3）・`common/step_rules.py::check_table`（新しい。テスト1）・`common/value_freshness.py::stale_names`（新しい。テスト1）

**C. ファイル**
- `common/sec_data/newfield_q4_cutoff_check.py`（217行。`python -m` で手で動かす一度きりの検査。参照なし）→ 消してよい候補
- `scripts/migrate_market_data_latest.py`・`scripts/migrate_market_data_comments_history.py`（移行の道具。`tests/test_market_pulse_latest.py`・`tests/test_market_data_comments_history.py` が読む。`collect_and_send.py` のコメントも触れる）→ **移行が済んだかを確かめてから、テストと一緒に**
- `tools/sim/parser_l3/parser_overlay.py`・`parser_redirect.py`・`parser_vs_l3.py`（`quarterly_bs.py` の説明文が `parser_vs_l3.py` に触れるだけ）→ 段2a の確認の後
- `tools/sim/regen_derived.py`・`revert_noise.py`・`sim.py`（手で動かす試算の道具。計算方法の変更の手順で使う）→ **残す**
- `src/subport/fg_level2_extended.py`（390行）→ 別系統（AutoTrade）。**この EPIC の対象外**
- `browser_checks/check_tanuki_score_undetermined.py`・`check_valuation_chart_basis.py` → 手で動かす検査（`NON_PRODUCTION_DIRS`）。対象外

A は22件のうち関数16件・約150行（`signed`・`fmt_md` を含む）。B は6件。

### 5-4 今日の終業の数の変化

| 数 | 変更前（`4f9723d2a9`） | 変更後 |
|---|---|---|
| 二重実装の規則 | 14 | **13**（6 を一本化） |
| Python の行数 | 78,939行・179ファイル | **78,764行・178ファイル**（−175行・−1ファイル。`val_guard_proto.py` の削除 −172 と、`quarter_label` の集約・ほかの差し引き −3） |
| ワークフローの本数 | 21 | 21 |
| 読み手の決まっていない出力 | 0 | 0 |
