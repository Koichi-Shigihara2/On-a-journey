# On-a-journey — 改善バックログ（全システム）

最終更新: 2026-08-12（`[[MACRODATA-LAYER-CONSTRUCTION-1]]`: 本番消費者
2ファイル（`05_main.py`・`collect_and_send.py`）を`common.macro_data.
reader`経由へ全面切替し**完成**。MIGRATION_CHECKLIST.md Step1〜3に
従い実施。Step1（洗い出し）で想定リストになかった2箇所
（`refresh_monthly_indicators()`・`update_fed_context()`）と、grepパターン
（`Fred(`/`fred_latest(`）では検出できなかった`_load_sp500_cache()`の
`fred.get_series()`直接呼び出しを追加発見。重複3系列
（`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`）は`reader.get_latest()`への集約で
解消。単一最新値だけでは機能を維持できない5箇所（NFP前月比・VXN
MA50・HYスプレッド90日min/max・DGS3MO前日比・S&P500複数日履歴）は
`reader.get_series()`（期間指定）を使用（依頼の`get_latest()`一本化
方針から実装上必要な範囲でのみ逸脱、詳細を明記）。Step2（値突合）で
18項目全て完全一致（差分0件）を実測確認。Step3（grep最終確認）で
`Fred(`・`fred_latest(`等の直接呼び出しが両ファイルとも0件であることを
確認。リトライ・指数バックオフロジックも削除（`fred_release_dates()`
は別API表面のため対象外・維持）。`tests/test_macro_pulse_logic.py`の
関連7件をmonkeypatch方式に更新。pytest全体771 passed / 2 known-failed。
PROJECT_STATUS.md・SYSTEM_MAP.mdも同時更新）

最終更新: 2026-08-12（`[[MACRODATA-LAYER-CONSTRUCTION-1]]`:
`.github/workflows/Macro_Data_Update.yml`（毎日UTC10:00・
workflow_dispatch対応）を新設し定期取得ワークフローが稼働開始。
GitHub Actions側のworkflow_dispatchを直接トリガーする手段がセッション
環境になかったため、同一エントリポイントをローカルで実FRED_API_KEY
実行し代替検証。25系列中24系列成功（更新レコード合計94,909件）、
`FTSD`のみFRED API上に系列が実在せず失敗（`[[MACRODATA-FTSD-SERIES-
ID-INVALID-1]]`新規登録）。`macro_data_violations_log.json`の警告
255件は全て実在する経済事象・近ゼロ交差によるorder-of-magnitude
jump検知でありデータ品質問題なしと判断。副次発見として日次cronが
毎回全期間履歴を再取得する非効率な設計も判明
（`[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]`新規登録）。05_main.py・
collect_and_send.pyは今回も変更していない。PROJECT_STATUS.md・
SYSTEM_MAP.mdも同時更新）

最終更新: 2026-08-12（`[[MACRODATA-LAYER-CONSTRUCTION-1]]`:
`common/macro_data/fetcher.py`/`reader.py`本体を実装（**構築中**）。
`fetch_series`/`update_series`/`fetch_all_series`（fredapiクライアント
のモジュールレベル一元化・リトライ3回＋指数バックオフ・保存前検証2項目
＋`macro_data_violations_log.json`）・`get_latest`/`get_series`/
`get_value_as_of`・25系列分の`series_meta.json`を実装。新規テスト
`tests/test_macro_data_fetcher.py`・`tests/test_macro_data_reader.py`
（計43件）を追加、pytest全体771 passed / 2 known-failedで回帰なしを
確認。今回のスコープは新規モジュール構築のみで、`05_main.py`・
`collect_and_send.py`側の本番消費者切替（重複3系列解消含む）・GitHub
Actionsワークフロー新設・過去データ一括投入（フェーズ2）はいずれも
今回変更していない（次段階）。PROJECT_STATUS.md・SYSTEM_MAP.mdも
同時更新。詳細は`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照）

最終更新: 2026-08-12（セッション終了時ブラッシュアップ。遡って
`[[MACRODATA-LAYER-CONSTRUCTION-1]]`（`common/macro_data/`新設、
FRED統合層）をマスター追跡エントリとして正式登録（`[[MARKETDATA-
LAYER-CONSTRUCTION-1]]`と同型。投資調査サマリー・新規発見4件への
リンク・次セッション着手順序を記載）。前回投資調査で言及していたが
未登録のまま参照していたことが本ブラッシュアップで判明したための
訂正登録。あわせて`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`エントリ内の
「次セッションでの着手順序」を`[[MACRODATA-LAYER-CONSTRUCTION-1]]`
参照に更新。実装コード変更なし）

最終更新: 2026-08-12（`common/macro_data/`新設事前調査（FRED消費者洗い出し、
`MIGRATION_CHECKLIST.md`Step1相当）で発見した新規4件を登録（記録のみ、
実装なし）。優先度：中に`[[MACRODATA-AS-IS-DUPLICATION-UNDERCOUNT-1]]`
（`INPUT_DATA_TOBE.md`記載の`BAMLH0A0HYM2`重複取得「3箇所」は実際には
`get_financial_context()`を含む4箇所、`T10Y2Y`・`VIXCLS`にも同型の
未記載重複あり）・`[[MACRODATA-SCHEDULED-SILENT-GAP-CSCICP-USALOL-1]]`
（現行`INDICATOR_CONFIG`から削除済みの2系列の残存`scheduled`行が
サイレントにactual欠落を起こす疑い、実データ確認が着手条件）、
優先度：低に`[[MACRODATA-FTSD-MISSING-FROM-INVENTORY-1]]`（`FTSD`が
24系列台帳に未掲載）・`[[MACRODATA-IMPORT-HISTORY-CONFIG-DRIFT-1]]`
（`05_import_history.py`の独自`FRED_INDICATORS`辞書が現行
`INDICATOR_CONFIG`と2系列分乖離）を登録。実装コード変更なし）

最終更新: 2026-08-12（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`着手順序6-2:
`backfill_tech_pulse.py`（QQQ/SPY取得）切替が**完了**。前提作業として
`reader.py`へ`get_price_series_as_of(symbol, as_of_date, days)`を新規
追加（任意過去基準日起点のトレイリングウィンドウ取得、共通実装
`_price_series_ending_at()`へ`get_price_series()`ともリファクタ）した
上で本体切替。「実行時点で1回だけ取得し全エントリで使い回す」旧設計
思想は維持。51件のmissingエントリ全件で`--dry-run`実行・旧実装との
`tp_score`/`tp_label`突合を実施し、`_tp_label()`バケット判定のクロス
0件を確認。pytest全体は728 passed（既知失敗2件はTEST-STALE-IV-1、
無関係）。**これにより着手順序6は周辺ツール2/2（全数完了）、着手順序
4〜6（本番消費者8＋診断ツール2＋周辺ツール2の全12ファイル）が完了し、
`common/market_data/`構築プロジェクト自体が完了**。詳細・検証結果は
BACKLOG_DONE.md「2026-08-12（完了）」`[[MARKETDATA-LAYER-CONSTRUCTION-1]]
着手順序6-2`参照（コミット`4a864bc1c`）

最終更新: 2026-08-12（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`着手順序6-1:
`extract_key_facts.py`（株式数フォールバック④）切替が**完了**。
yfinance直接呼び出し（`.info.get('sharesOutstanding') or
.get('impliedSharesOutstanding')`）を`reader.get_attributes()`経由
（`shares_outstanding`優先→`implied_shares_outstanding`フォールバック、
既存優先順位パターン維持）に置換。V（該当フォールバックの実例銘柄）で
切替後コードを実際に発動させ、切替前のキャッシュ値と完全一致を確認。
EPS Analyzer対象101銘柄をキャッシュ済みquarterly.jsonで走査した結果、
fallback④該当はV 1件のみと判明（他100銘柄は影響なし）。pytest全体は
721 passed（既知失敗2件はTEST-STALE-IV-1、無関係）。これで着手順序6は
周辺ツール2ファイル中1ファイル完了（1/2）、残るは`backfill_tech_pulse.py`
（`reader.py`への新規API追加が前提と判明済み）のみ。詳細・検証結果は
BACKLOG_DONE.md「2026-08-12（完了）」`[[MARKETDATA-LAYER-CONSTRUCTION-1]]
着手順序6-1`参照（コミット`212454681`）

最終更新: 2026-08-12（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`着手順序5-2:
`score_verifier.py`切替が**完了**。前提作業として`reader.py`へ
`get_price_on_or_after(symbol, date)`を新規追加（date以降5日ウィンドウで
先頭値を採用、旧`fetch_price_after()`と同じクエリ形状をdaily/層に対して
再現）。`fetch_price_after()`をyfinance直接呼び出しから同API経由に置換。
`Score_Verifier.yml`の依存インストールも`pip install -r requirements.txt`
へ更新。ライブA/Bテスト（RKLB/ZS各3サンプル）で旧実装との価格・日付特定
ロジック完全一致を確認、実データ全102銘柄でscore_verifier.py実行が
例外なく完走することを確認済み。これで診断ツール2/2（`audit.py`・
`score_verifier.py`）とも切替完了、本番消費者8＋診断ツール2の
全10ファイルが切替完了。残るは着手順序6（周辺ツール2ファイル）のみ。
詳細・検証結果はBACKLOG_DONE.md「2026-08-12（完了）」
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序5-2`参照（コミット
`2668f3aaf`・`bc0f6fb24`）

最終更新: 2026-08-11（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`着手順序5-1:
`audit.py`（β乖離監査・カナダ企業判定）切替完了。`attributes/`へ
`country`フィールドを新規追加し、両判定ともyfinance直接呼び出しから
`reader.get_attributes()`経由に切替（設計確定事項6の方針通り）。
`SEC_Data_Audit.yml`の依存インストールも`pip install -r requirements.txt`
へ更新。副次発見: 旧`SEC_Data_Audit.yml`はyfinance未導入のためカナダ判定が
本番自動実行で常に無音スキップされ事実上死んでいたが、今回の切替で
実際に機能するようになった。残る5-2`score_verifier.py`は、`reader.py`へ
任意過去日点参照API（`get_price_on_or_after`相当）の新規追加が前提と
判明し次点保留。詳細はBACKLOG_DONE.md「2026-08-11（完了）」着手順序5-1・
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`エントリ参照）

最終更新: 2026-08-11（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`: `fetcher.py`・
`reader.py`新設と定期実行ワークフロー2件（Daily/Weekly Update）を実装、
続けて本番消費者8ファイル（`beta_fetcher.py`・`data_fetcher.py`・
`valuation_fetcher.py`・`pipeline.py`・`collect.py`・`collect_and_send.py`・
`breadth_calculator.py`・`hypecore.py`）**全数の切替が完了**。うち
`hypecore.py`は前提作業3件（daily/バックフィルを`start="2021-01-01"`へ
拡張・attributes/へ7フィールド追加・analyst_history/へearnings_history・
recommendations_historyの2系統追加）を要する最複雑の消費者だった。
一連の作業で発見した副次課題を`[[MARKETDATA-CWAN-FROZEN-DATA-
SUSPECT-1]]`・`[[MARKETDATA-SP500-SCRAPE-INVALID-TICKERS-1]]`・
`[[MARKETDATA-VIX9D-DATA-GAP-1]]`・`[[STONKS-SILO-CLI-TICKERS-
SHADOW-1]]`として登録。誤って「バグ」登録した`[[MARKETDATA-DAILY-
UNADJUSTED-PRICE-DIVIDEND-DRIFT-1]]`（daily/層のauto_adjust=False
〈未調整終値〉と旧実装のauto_adjust=True〈調整済み終値〉の乖離）は、
事実確認調査の結果「旧実装の調整済み終値使用の方が技術指標としては
元々不適切だった」と判明したため訂正・クローズ（対応不要で確定）。
次は着手順序5（診断ツール2ファイル`score_verifier.py`・`audit.py`の
切替）。詳細はBACKLOG_DONE.md「2026-08-11（完了）」および
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`エントリ参照）

最終更新: 2026-08-08（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`の未決定
事項9件を全件最終確定。1.営業日連続性保証〈pandas_market_calendars
新規依存採用〉2.fetched_at付与 3.書き込みアトミック化〈tempfile→
os.replace()〉4.層またぎ再計算禁止〈reader.pyドキュメント明記〉
5.保存前検証〈時価総額乖離許容率=相対2%または絶対$1,000,000の
大きい方、52週高安は日次バッチ時点のみ、失敗時は警告フラグ付き保存、
market_data_violations_log.json〉6.audit.pyとの役割分担〈内部整合性
ゲートvs外部妥当性監視、両方維持〉7.twoHundredDayAverageは非保存・
アナリスト目標株価コンセンサスはattributes/へ 8.workflow_run連鎖
トリガー採用 9.NETCASH-DUAL-CALC-1とは独立並行進行、の内容で確定。
着手順序を「fetcher.py→reader.py→本番消費者→周辺ツール」の4段階に
更新（設計判断ステップを解消により削除）。実装コード変更・データ
再生成なし（BACKLOG登録のみ）。

最終更新: 2026-08-07（セッション終了時ブラッシュアップ。「次セッション
での着手順序」欄を全面再構成し、1〜5を`[[MARKETDATA-LAYER-
CONSTRUCTION-1]]`の未決定事項9件確認→`fetcher.py`→`reader.py`→本番
消費者→周辺ツールの具体的5段階に、6を本線外課題群（優先度中の
AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1・優先度低のLayer3関連10件）に
整理。`common/macro_data/`着手はmarket_data完了後の継続タスクとして
注記。実装コード変更なし）

最終更新: 2026-08-07（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`の「未決定
事項」に、`EXTRACTION_DESIGN_PRINCIPLES.md`3原則照合投資調査（チャット
記録）の結果である追加6項目（営業日連続性保証・`fetched_at`付与・
書き込みアトミック化・層またぎ再計算の禁止・保存前恒等式検証＋
`market_data_violations_log.json`・`audit.py`との役割分担）を追記。
着手順序を「6項目の設計確定」を最初のステップとして追加した5段階に
更新。実装コード変更・データ再生成なし（BACKLOG登録のみ）。

最終更新: 2026-08-07（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`を新規登録
（本来は事前調査着手時点で登録すべきだったが未登録のまま3回の投資
調査を実施していたため遡って正式登録）。3回の投資調査サマリー
（12ファイル使用実態・3区分分類・`fetcher.py`/`reader.py`実装設計）
と、設計確定事項（保存構造・API設計、`.info`取得方式案C採用、
TANUKI VALUATION/STONKS SILOの株価を取引時間中リアルタイムから前日
終値ベースへ仕様変更、`audit.py`の`reader.py`経由切替）を記録。
未決定事項3件（`twoHundredDayAverage`等の格納先・`workflow_run`連鎖
トリガーの適用範囲・`[[NETCASH-DUAL-CALC-1]]`との関係整理）と着手
順序（`fetcher.py`→`reader.py`→本番消費者→周辺ツール）を明記。
実装コード変更・データ再生成なし（BACKLOG登録のみ）。

最終更新: 2026-08-07（`common/market_data/`新設事前調査で発見した
`[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`（優先度：低）を登録。
`INPUT_DATA_AS_IS.md` 1-B節の「11ファイル」調査が`src/`配下のみを
対象としており、`common/sec_data/audit.py`（β乖離監査、
`SEC_Data_Audit.yml`経由で本番稼働中）を見落としていたと判明。
`INPUT_DATA_AS_IS.md`本体は別途12ファイルへ訂正。実装コード変更・
データ再生成なし（BACKLOG登録＋ドキュメント訂正のみ）。

最終更新: 2026-08-07（フェーズE（`normalized/`廃止）の着手不可判定を
`[[SECDATA-STORAGE-FRAGMENTATION-1]]`（マスター追跡エントリ）に反映。
フェーズD最終状況：Step2-1〜2-4（①〜④主要4消費者パイプライン）完了、
Step2-5（⑤stock.html＋診断・補助スクリプト7件）は切替対象ほぼ存在
せず実質完了、保留中だった2判断（`fetcher.py`選択思想・stock.html
公開パイプライン）はいずれも現状維持・着手見送りで確定。
`fetcher.py`・`dcf_validity_checker.py`・stock.htmlが`normalized/`
またはparser.py系データへの依存を意図的に継続する恒久的例外として
残るため、フェーズE（`normalized/`完全廃止）は着手不可と判定し、
`normalized/`はこの3系統向けに存続する設計とすることを記録。
CLAUDE_CODE_START.mdのフェーズD進捗欄も、フェーズD実質完了・次の
優先タスクは新DB構築プロジェクトの他フェーズ（`common/market_data/`・
`common/macro_data/`新設）への移行検討である旨に更新。実装コード
変更・データ再生成なし（BACKLOG登録のみ）。

最終更新: 2026-08-07（stock.htmlのLayer3切替着手要否投資調査結果を
反映。`[[STOCKHTML-LAYER3-PUBLISH-PIPELINE-MISSING-1]]`（優先度：低、
Layer3ストア公開パイプライン未整備が着手ブロッカー、技術コストは
低いが現状実害ゼロのため見送り）・`[[STOCKHTML-YTD-FILTER-BUG-
SUSPECT-1]]`（優先度：低、JS側`getQ()`のis_ytd未除外は構造的リスク
だが105銘柄×5フィールド全数実測で現状未発現と確認）を新規登録。
`[[SCHEMA-NORMALIZED-ISSUES-1]]`⑥DAフォールバック欠如に、今回の
実測データ（105銘柄中30銘柄・約29%でDAフィールド空、MSFT/TSLA/
GOOGL/AVGO等主要銘柄含む）を追記。実装コード変更・データ再生成なし
（BACKLOG登録のみ）。

最終更新: 2026-08-07（`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-
MISMATCH-1]]`の対応方針を確定。案2（Layer3切替を見送り、`fetcher.py`・
`dcf_validity_checker.py::check_c_data_jump()`とも`data/annual_*.json`
直読みを継続）を採用し、優先度を高→低に格下げ（対応不要、記録のみ・
恒久的な例外扱い、BACKLOG.md「優先度：低」セクションへ移動）。採用
理由：Layer3の「filed日最新優先」は修正再表示の正誤を区別できない
不確実な方式である一方、parser.pyの「own-year優先」は実績ある一貫
した基準のため、正確性の確実性を犠牲にしてまで統一する理由がない。
本決定により前提条件が消滅した`[[LAYER3-STONKS-SPAC-EARLY-YEAR-
GAP-1]]`をクローズしBACKLOG_DONE.mdへ移動。CLAUDE_CODE_START.mdの
フェーズD進捗欄も本決定を反映するよう更新。実装コード変更・データ
再生成なし（BACKLOG登録のみ）。

最終更新: 2026-08-07（フェーズD Step2-4（HypeCore）実装完了。
`hypecore.py::fetch_quarterly_fundamentals()`のnormalized/参照を
SEC EDGAR Layer3（`layer3_builder.py::build_ticker_store()`/
`get_quarterly_series()`）経由に切替。104銘柄全数比較で事前調査の
予測（差分銘柄数6/104：ASTS/CEG/CWAN/DDOG/RCAT/BROS）と完全一致、
ASTS/RCAT/DDOGの`determine_stage()`再確認でもステージ判定差分ゼロを
再確認。report_consistency_check.py NG=0・WARN=78件（不変）、pytest
505 passed/2 known failed（既知のみ）。完了記録は`[[SEC-EDGAR-LAYER-
DESIGN-PHASE-D-STEP2-4]]`としてBACKLOG_DONE.mdへ記録。
`[[HYPECORE-SUBSTAGE-LAYER3-UNVERIFIED-1]]`（優先度：低）も登録。
「次セッションでの着手順序」欄を更新：①フェーズD Step2-5（⑤stock.html
フロントエンド＋診断・補助スクリプト7件、主要4消費者パイプライン
完了に伴う残る最後のフェーズD対象）②`[[LAYER3-FETCHER-SELECTION-
PHILOSOPHY-MISMATCH-1]]`（Step2-2で保留中のSTONKS SILO fetcher.py
設計判断、並行して選択可能）③フェーズE（`normalized/`廃止、Step2-5
完了後）の3項目を明記。

最終更新: 2026-08-07（フェーズD Step2-4（HypeCore）事前調査（読み取り
専用）を反映。`hypecore.py::fetch_quarterly_fundamentals()`は
`reader.py`共通アクセサのみでnormalized/を参照（独自インライン実装
なし）、対象母集団は104銘柄（`hypecore=true`、ほぼ全銘柄ユニバース）
と確認。104銘柄全数のLayer3事前差分シミュレーションでRevenue 2/104
（ASTS/RCAT、既知パターン）・NetIncome 4/104（CEG/CWAN/DDOG/BROS、
うちDDOGはLayer3側が異常な30日フラグメントを正しく除外する改善を
実測）・OCF 0/104差分ゼロを確認。差分がpoc.json表示（2024年以降）に
及ぶASTS/RCAT/DDOGの3銘柄について`determine_stage()`を実際に実行し
ステージ判定への影響ゼロ（32ヶ月×3銘柄すべて一致）を確認。substage
（別ロジック、rev_yoy/eps_surprise直接参照）は範囲外として
`[[HYPECORE-SUBSTAGE-LAYER3-UNVERIFIED-1]]`（優先度：低）で記録。
実装コード変更・データ再生成なし（BACKLOG登録のみ）。

最終更新: 2026-08-07（フェーズD Step2-3（TANUKI TAIL）実装完了。
`quarterly_review_generator.py`・`tail_dcf_bridge.py`の
normalized/参照をSEC EDGAR Layer3（`layer3_builder.py::
build_ticker_store()`/`get_quarterly_series()`/`get_latest_
quarterly()`）経由に個別切替。10銘柄全数比較で差分ゼロ、
report_consistency_check.py NG=0・WARN=78件（不変）、pytest 505
passed/2 known failed（既知のみ）。完了記録は`[[SEC-EDGAR-LAYER-
DESIGN-PHASE-D-STEP2-3]]`としてBACKLOG_DONE.mdへ記録。「次セッション
での着手順序」欄をフェーズD Step2-4（④HypeCore）に更新。STONKS SILO
の`fetcher.py`（年次データ）切替は`[[LAYER3-FETCHER-SELECTION-
PHILOSOPHY-MISMATCH-1]]`の設計判断待ちのまま並行して選択肢として
残置（Step2-4着手前に対応してもよい）。

最終更新: 2026-08-07（フェーズD Step2-3（TANUKI TAIL）着手前の使用実態
調査（読み取り専用）を反映。`quarterly_review_generator.py`・
`tail_dcf_bridge.py`はいずれも`reader.py`共通アクセサ（独自インライン
実装なし）のみでnormalized/を参照しており、対象母集団は105銘柄でも
STONKS SILOの25銘柄でもなく、実データが存在する3銘柄（PLTR/SOFI/
TSLA、ポジション登録10銘柄中）と訂正。10銘柄×5フィールド全数の
Layer3事前差分シミュレーションで差分ゼロを確認した一方、
`[[LAYER3-SHARESDILUTED-TAG-GAP-1]]`の対応がpipeline.py限定実装で
あり本2ファイルの`get_latest_quarterly()`直接呼び出しには及ばないため
`[[TAIL-SHARESDILUTED-Q4-TIMING-RISK-1]]`（優先度：低、現状実害なし）
を新規登録。TANUKI TAIL独自の「Layer3」用語（AI KPI抽出、SEC EDGAR
Layer3とは別概念）との衝突に注意する旨も記録。実装コード変更・データ
再生成なし（BACKLOG登録のみ）。

最終更新: 2026-08-07（フェーズD Step2-2（STONKS SILO）実装完了。
`financial_trend_calculator.py`のnormalized/参照をLayer3
（`layer3_builder.py::get_field_entries()`）経由に切替（`fetcher.py`は
`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`の対応方針決定待ちで
現状維持、`analyzer.py`は変更不要と確認済み）。25銘柄全数比較で25/25
銘柄に差分が生じたが、パーセンタイル母集団の連鎖効果・GrossProfit
バックフィル改善・AVAV/ESTCのYoY計算停止解消（good side effect、
normalized/側に`q4_implied.py`集約以前の旧世代`fp:"implied"`ラベルが
残存していたことが原因と判明）・RCATの既知パターンのみで、いずれも
許容範囲・改善方向と確認。`SUB_FIELDS`（SM/SBC）が`compute_vectors()`
から現状呼び出されていない未使用の定数と判明したため
`[[FINTREND-SM-JOBY-NONE-1]]`に補足を追記。完了記録は
`[[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP2-2]]`としてBACKLOG_DONE.mdへ
記録。report_consistency_check.py NG=0・WARN=78件（不変）、pytest 505
passed/2 known failed（既知のみ）。「次セッションでの着手順序」欄を
更新。

最終更新: 2026-08-07（フェーズD Step2-2（STONKS SILO）着手前の使用実態
調査（読み取り専用）を反映。`financial_trend_calculator.py`・
`fetcher.py`・`analyzer.py`の実装・25銘柄（`stonks_silo=true`）全数の
Layer3事前差分シミュレーション結果から、新規課題4件を登録:
`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`（優先度：高。
fetcher.pyの年次データ選択思想がLayer3〈filed日最新優先〉とparser.py
〈own-year優先〉で異なり、単純差替えでPL/CF系年次データのほぼ全セルが
変わることをAVAV実測で確認。フェーズD Step2-2完了の前提条件）・
`[[LAYER3-STONKS-SPAC-EARLY-YEAR-GAP-1]]`（優先度：中、①の対応方針
決定後に再評価）・`[[FETCHER-PY-BS-FIELDS-DEAD-KEYS-1]]`（優先度：低、
Layer3移行と無関係の既存デッドコード）・`[[FINTREND-SM-JOBY-NONE-1]]`
（優先度：低、`[[SCHEMA-NORMALIZED-ISSUES-1]]`②の既知帰結）。調査時、
依頼文の「105銘柄」前提が誤りで実際の消費範囲は25銘柄（`stonks_silo=
true`）のみと訂正した。実装コード変更・データ再生成なし（BACKLOG登録
のみ）。コミット・push未実施（ユーザー確認待ち）。

最終更新: 2026-08-06（Layer3統一方針への文書横断整合性確認・修正。
`SEC_EDGAR_LAYER_DESIGN.md`フェーズD（Layer3統合）と
`[[SECDATA-STORAGE-FRAGMENTATION-1]]`が5消費者の移行先（Layer3 vs
data/）で1ヶ月弱食い違ったまま併存していた問題を発見し、本エントリの
対応方針・残タスクをフェーズD方向に統一。分岐の経緯を記録として追記。
`INPUT_DATA_TOBE.md`2-A章の保持構造案に、実際の統合先が`store_v2/`
（Layer3）である旨の位置づけ注記を追加。`FIELD_DEFINITIONS.md`に
TANUKI TAIL側「Layer3」（AI KPI抽出）との用語衝突を記録。以前チャット
上言及のみで未登録だった`[[PARSER-MERGED-TAG-MIXING-RISK-1]]`
（`parser.py::_extract_values_merged()`のタグ混入リスク疑い）を正式
登録（優先度：低）。`CHAT_RULES.md`に再発防止のためのルール3件
（文書横断整合性チェック・根拠のない懸念提示の禁止・確定済み方針の
独自変更禁止）を新設。本セッション追加分が旧セッション（2026-08-05）
の日付表記をそのまま引き継いでいた5箇所を2026-08-06へ訂正。
`PROJECT_STATUS.md`の残タスク記述も同様にLayer3方向へ修正。「次
セッションでの着手順序」欄を更新。実装コード変更・データ再生成なし
（ドキュメントのみ）。機能コミット・BACKLOG更新コミットとも同一
コミットで実施、push済み。

最終更新: 2026-08-05（新DB構築プロジェクト フェーズ1 Step1、
`[[SECDATA-STORAGE-FRAGMENTATION-1]]`対応の一環として`data/
quarterly_{FYQ}.json`のpl/cf/shares区分のYTD→単一四半期(SA)修正を
実装。normalized/→data/統合の事前調査4段階を通じて、`quarterly_*.json`
がXBRL申告のYTD累積値のまま保存されていた（約65〜66%のエントリが
該当）ことが判明し、`quarterly.py::_classify_period()`・
`normalizer.py::_ytd_to_quarterly()`（normalized/側で実績のある
ロジック）を再利用する統一アルゴリズムを`parser.py::
parse_company_facts()`に実装した。手作業シミュレーションが
`parser.py`本体のタグ選定ロジックを正確に再現できず誤った結果を
出したため、実際の`parser.py`関数への直接実装＋メモリ上比較方式に
切り替えて検証し、その後の実書き込み・全105銘柄再パース結果と完全
一致することを確認。annual側は無変化（1,441ファイル横断比較で差分
0件）、report_consistency_check.py NG=0・WARN=78件（不変）、
pytest 497 passed/2 known failed確認。残タスクは新設アクセサ実装・
5本番消費者のnormalized/→data/切り替え。詳細はBACKLOG_DONE.md
「2026-08-05（完了）」参照。コミット・push未実施（ユーザー確認待ち）。

最終更新: 2026-08-05（`[[SCHEMA-NORMALIZED-ISSUES-1]]`④SharesBasic概念
不一致の実害調査完了（読み取り専用）。normalized/側の「SharesBasic」
フィールドはリポジトリ全体で消費者ゼロの死んだフィールドと確認、
data/側の`shares_basic`は`reader.py::get_diluted_shares()`の
異常値フォールバックとしてのみ限定的に参照されると判明。結論として
④自体による実害はなし・normalized/→data/統合の障害にはならないと
確定し、`[[SCHEMA-NORMALIZED-ISSUES-1]]`④の優先度を中→低に格下げ。
副次発見として、フォールバックが現在実際に発火しているONDS・LOARの
2銘柄で、shares_basic自体も同じ桁の異常値を持ち救済不能な疑いが
判明したため`[[ONDS-LOAR-SHARES-SCALE-SUSPECT-1]]`として新規登録
（記録のみ、実装なし）。

最終更新: 2026-08-05（新DB構築プロジェクト フェーズ1 Step1: SEC EDGAR
統合、`[[SECDATA-STORAGE-FRAGMENTATION-1]]`対応の一環として
`common/sec_data/raw/`を削除した。全消費者洗い出し（Step1調査）で
`raw/{TICKER}_quarterly_raw.json`が`quarterly.py`の書き込み専用出力
であり、リポジトリ全体で読み込み側が一切存在しない実質デッドコードと
確認済み。削除前の最終確認（Step0）で`.github/workflows/
SEC_Data_Update.yml`に想定外の参照（`git add common/sec_data/raw/ ||
true`）を新規発見し、これも含めて削除する方針をユーザーに確認の上で
実施。`quarterly.py`の書込処理削除・既存105ファイル（約16MB）削除・
`update.py`のimport整理・`SEC_Data_Update.yml`の該当行削除・
`contracts.py`/`audit.py`のコメント整理を実施。全105銘柄フローズン
検証（`build_raw_table()`+`normalize()`再実行）でnormalized/出力に
`generated_at`タイムスタンプ以外の実質的な差分がないことを確認（検証用の
タイムスタンプ差分は復元しコミット対象から除外）。
`report_consistency_check.py --fail-on-ng`でNG=0（WARN=78件、既存と
不変）、pytest 497 passed/2 known failed（既知）を確認。残タスクは
normalized/→data/統合のみ（別途設計セッション）。「次セッションでの
着手順序」欄を更新。コミット・push未実施（ユーザー確認待ち）。

最終更新: 2026-08-05（[[AVGO-2015-DATA-THIN-1]]原因調査完了（読み取り
専用）。SEC EDGAR一次情報の決算期比較（現行CIK・真の前身候補CIK
1441634はいずれも10月末〜11月初決算、`cik_history.json`登録済みの
旧CIK 1054374は12月31日決算）により、AVGOの旧CIK登録が無関係な買収先
企業（Broadcom Corporation、2016年にAvago Technologies社に買収され
Broadcomへ社名変更）を指している疑いが判明。2006-2014年の「AVGO」
年次データがAvago自身ではなくBroadcom Corpの実績を表している可能性が
あり、2015年欠落はこの誤りの副産物と確認。真の前身企業CIK 1441634
「Avago Technologies LTD」は2016-02-08にForm 15-12B提出で消滅して
おり`cik_history.json`に未登録のまま。実害は現時点でゼロと確認済み
（growth_sanity・roe_10yr_avgとも窓が届く範囲外、fixed_registry.json
はAVGO 2016/2017のみ登録済みで無関係）。原因確定により
`[[AVGO-2015-DATA-THIN-1]]`をクローズし、`[[AVGO-CIK-HISTORY-WRONG-
LEGACY-CIK-1]]`として新規登録（優先度：高、着手条件: 新DB構築フェーズ
1完了後または実害発生時まで保留）。対応方針3案（旧CIK差し替え・
現状維持＋警告・2006-2014年データ削除）を記録、実装は未実施。「次
セッションでの着手順序」欄を更新。コミット・push未実施
（ユーザー確認待ち）。

最終更新: 2026-08-05（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]
Stage 3b実装完了。SCCO(2010-2019)のgross_profit、RDW(2020)・
ASTS(2020)のBS恒等式修正後の値（total_assets/total_liabilities/
stockholders_equity）を`fixed_by: manual_verification`で
`fixed_registry.json`へ登録した（3銘柄・計12エントリ）。

**登録前確認（SCCO）**: annual_2010.json〜annual_2019.jsonを実測し、
gross_profit値・revenue-cost_of_revenue逆算差分が前回のStage 3調査
時点から不変であることを確認。BACKLOG.md（未完了側）をgrepしたが
SCCOのgross_profit・当該年度に関するOPEN課題は見つからなかった
（唯一の関連言及は既にクローズ済みエントリ内の過去スナップショット
記述）。**新たな発見**: SCCO(2010)は`is_own_data=False`（同一accnの
2011年10-K比較列由来）であり、以前の報告「2010-2019は全年度is_own_
data=True」は不正確だったと判明。ただし`derived`キーはなし（直接タグ
値）であり、genuine定義差の対象母集団として妥当と判断し登録対象に
含めた。

**fields_snapshot特定（RDW/ASTS）**: 依頼は「一時的持分に対応する
実際のフィールド名」の特定を求めていたが、実ファイル確認の結果、
`bs_identity_violations_log.json`のextra_components（一時的持分の値）
は検証専用ロジックがraw XBRLから都度算出するのみで、annual_{year}.json
への書き戻しは一切行われない設計と確認した。したがってfields_snapshot
はStage 2のHEI/LRCX/TSLA/XOMと同じ`total_assets`/`total_liabilities`/
`stockholders_equity`の3項目とした。

**検証結果**: 全105銘柄フローズン再パースで新規12件を含め無変化
（`bs_identity_violations_log.json`10銘柄分の既知の非決定的キー順序
差分のみ発生、復元しコミット対象から除外）。CHECK-31試験発火:
SCCO(2015)を意図的に改変→NG-31検知→復元後NG=0に復帰を確認。
`report_consistency_check.py --fail-on-ng`でNG=0（WARN=78件、既存と
不変）。pytest 497 passed/2 known failed（既知）を確認。「次セッション
での着手順序」欄を更新。コミット・push未実施（ユーザー確認待ち）。

最終更新: 2026-08-05（ASTS(2020) BS恒等式残差$150,596,928を解消。
Step 0でannual_2020.json実測により残差額が前回報告時点から不変と確認
した上で着手。Step 1の全105銘柄机上シミュレーションで、RDWと同じ
「フォールバック機構への追加」案を試したところ、ASTSでは既存の
MinorityInterestのcross-accn一致（$2,490,000）に本タグが後乗せされ
diff=-$2,490,000という不正確な合算（許容誤差内で見かけ上resolvedに
なるだけ）が生じることが判明。一方`_BS_IDENTITY_ALLOWLIST`への無条件
追加案はown-accn一次パスのみでdiff=0の厳密一致となることを確認し、
**RDWとは異なり主許可リストへの無条件追加を採用**
（`TemporaryEquityValueExcludingAdditionalPaidInCapital`は簿価系の
測定基準であり、RDW型のRedemptionValue〈測定基準が異なる開示専用
タグ〉とは性質が異なるため）。全105銘柄シミュレーションで他に影響
したのはFRSH(2020)のみ（解消済み年度に名目値$0.0001が追加一致するのみ、
実質影響なしと確認）。全105銘柄フローズン再パースでASTS/FRSH以外に
差分なし、`report_consistency_check.py`でASTS(2020)分のWARN-29解消・
全体NG=0（ASTS(2019)は別問題として存続）、pytest 497 passed/2 known
failed（既知）を確認。
`[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]`の「②許可リスト拡張で対応
可能」2件（RDW/ASTS）が両方解消。ASTS(2020)のTA/TL/SE等は今後
fixed_registry.json Stage 3登録候補になりうる旨を`[[SEC-DATA-REDESIGN-
OPERATIONAL-POLICY-1]]`へ申し送り（今回は未登録）。「次セッションでの
着手順序」欄を更新。コミット・push未実施（ユーザー確認待ち）。

最終更新: 2026-08-05（RDW(2020) BS恒等式残差$120,314,578を解消。
`_BS_IDENTITY_FALLBACK_ONLY_TAG`を複数タグ対応（`_BS_IDENTITY_
FALLBACK_ONLY_TAGS`）へ拡張し、`RedeemableNoncontrollingInterest
EquityCommonRedemptionValue`を追加（`_BS_IDENTITY_ALLOWLIST`への
無条件追加ではなく、HEI型と同じ安全側のフォールバック機構を採用）。
Step 1で全105銘柄・全既知違反年度の机上シミュレーションを実施し、
無条件追加案・フォールバック追加案の両方でRDW(2020)のみが解消し他104
銘柄・RDW自身の他年度（2019/2021含む）に影響がないことを確認してから
実装。全105銘柄フローズン再パースでRDW(2020)以外に差分なし、
`report_consistency_check.py`でRDW単体WARN=0・全体NG=0、pytest
497 passed/2 known failed（既知）を確認。
[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]の「②許可リスト拡張で対応可能」
はASTS(2020)のみ残存。RDW(2020)のTA/TL/SE等は今後fixed_registry.json
Stage 3登録候補になりうる旨を申し送り（今回は未登録）。「次セッション
での着手順序」欄を更新。コミット・push未実施（ユーザー確認待ち）。

最終更新: 2026-08-05（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]
Stage 3a実装完了。Stage 3準備調査（BACKLOG_DONE.md各エントリの訂正・
新規登録2件）で特定した対象年度に基づき、MO(2016-2025)・PM(2016-2017)の
gross_profit、LLY(2007-2025)のcapital_expenditure・free_cash_flow・
fcf_method・finance_lease_payments_appliedの計3銘柄・31銘柄×年度
エントリを`fixed_by: manual_verification`で`fixed_registry.json`へ
登録した。

MO/PMは10-K原本のExciseAndSalesTaxesタグ突合によるgenuine業界定義差
確認（Stage 2以前のBACKLOG_DONE.md記載）＋Stage 3調査での対象年度実測を
根拠とする。PMは従来「10年連続」との誤認があったが2016-2017の2年度のみが
対象と訂正済み。LLYはタグフォールバック選定ロジック転換
（コミット`14862976f`）のgit diff直接確認を根拠とし、従来「2023-2025のみ」
という想定を2007-2025全19年度に訂正済み。SCCO(2010-2019)は今回のStage 3a
の対象外（別途対応）。

**検証結果**: 全105銘柄フローズン再パースで新規31件を含め無変化
（`bs_identity_violations_log.json`10銘柄分の既知の非決定的キー順序
差分〈[[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]〉のみ発生、復元し
コミット対象から除外）。CHECK-31試験発火: LLY(2023)を意図的に改変→NG-31
検知→復元後NG=0に復帰を確認。`report_consistency_check.py --fail-on-ng`
でNG=0（WARN=79件、既存と同水準）。pytest 497 passed/2 known failed
（既知の[[TEST-STALE-IV-1]] MSFT/NVDA、新規回帰なし）。

**残タスク**: SCCO(2010-2019)のfixed_registry登録（今回スコープ外）・
RDW(2020)の許可リスト拡張実装・MRVL/AVGO/DELL旧CIK分の個別確認・
AVGO(2015)原因調査・BBAI/RKLB/SOFI/VRT/ONDSグループの検討。「次セッション
での着手順序」欄を更新。コミット未実施（ユーザー確認待ち）。

最終更新: 2026-08-05（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]
Stage 2実装完了。taxonomy属性①〜⑧該当58銘柄のうち、過去の個別バグ
調査（BACKLOG_DONE.md）とSEC EDGAR一次情報照合（companyconcept API
直接照合）の両方で正しさが確定済みの12銘柄・17銘柄×年度エントリを
`fixed_by: manual_verification`で`fixed_registry.json`へ登録した:
HEI(2020)/LRCX(2012)/TSLA(2018)/XOM(2023)（会計恒等式TA=TL+SE全タグ
網羅確認）、AVGO(2016 revenue・net_income／2017 revenue・
operating_income、SEC EDGAR companyconcept API accn
0001730168-18-000084と完全一致確認）、RCAT(2024
stock_based_compensation)、ELF(2015/2016、10-K原本Selected Financial
Data表と一致確認)、FICO(2019/2020)・CPRT(2019/2020)・LITE(2019)の
revenue（365日正規年次値へのフローズン入力比較検証）、GOOGL(2012/2013、
revenue/operating_income/research_and_development/selling_and_
marketing/gross_profit、SEC EDGAR accn 0001288776-15-000008と完全
一致確認。selling_and_marketingはGOOGL固有のMarketingAndAdvertising
Expenseタグ規約との整合性も確認）、SPIR(2025 net_income・
operating_cash_flow、10-K MD&A一次情報と一致確認)。

**登録前検証で2件を対象外に確定（判定: 対応不要・既に是正済みのため
凍結対象なし）**: VRT(2016)/net_income・SPIR(2020)/long_term_debtは、
候補リスト作成時点のBACKLOG_DONE.md記述（前者は
`[[PERIOD-LENGTH-VALIDATION-GAP-1]]`実装時点、後者は
`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`登録時点の記述）を根拠にしていたが、
本Stage 2実装の登録前検証でannual_{year}.jsonの実ファイルを直接確認した
ところ、**現在は対象フィールド自体が存在しない**と判明した:
- **VRT(2016)/net_income**: annual_2016.jsonの`pl`セクションが完全に空
  （`{}`）。VRT(2016)はSPACシェル期（Vertivと合併前のGS Acquisition
  Holdings Corp）で、PERIOD-LENGTH-VALIDATION-GAP-1完了時点では値が
  存在した可能性が高いが、後続の`[[SPAC-STUB-PERIOD-VERIFICATION-1]]`
  調査で「stockholders_equity/operating_cash_flowのみ残存」と再確定され、
  その過程でPL系フィールドがNone化・削除されたとみられる（両タスクの
  完了日はいずれも2026-08-02で近接しており、実行順序の記録は本セッション
  時点では追跡できていない）。
- **SPIR(2020)/long_term_debt**: annual_2020.jsonの`bs`セクションに
  キー自体が存在しない。`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`段階2
  （2026-08-01）で該当値$26,645,000が誤った値としてNone化済みであり、
  そもそも凍結すべき正しい値が存在しない。

いずれも**Stage 3（保留・要追加調査）ではなく、この時点で「対応不要」
として判定完了**する。もし将来これらのフィールドに値が再度現れた場合
（例: 候補タグ拡充によるVRT(2016)のPL復旧、RDW(2020)と同型の許可リスト
拡張がSPIR(2020)にも適用された場合等）、その時点で改めてfixed_registry
登録の要否を検討する。

**教訓**: BACKLOG_DONE.mdの記述はその投稿時点のスナップショットであり、
同一領域で後続の別タスク（本件はいずれも同日2026-08-01/02の別エントリ）
が実行されると記述と実データが乖離しうる。fixed_registry登録のような
「値そのものを対象にした」作業では、BACKLOG_DONE.mdの記述を根拠として
そのまま信用せず、登録直前に必ず実ファイル（annual_{year}.json）で
対象フィールドの現存を確認する工程を欠かせない。一般化の要否（
`MIGRATION_CHECKLIST.md`または`EXTRACTION_DESIGN_PRINCIPLES.md`への
反映）は次回セッション終了時のブラッシュアップで検討する（今回は
記録のみ）。

**検証結果**: 全105銘柄フローズン再パースで新規21件を含め無変化
（`bs_identity_violations_log.json`9銘柄分の非決定的キー順序差分〈既知の
[[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]〉のみ発生、復元しコミット
対象から除外）。CHECK-31試験発火: RCAT(2024)を意図的に改変→NG-31検知
→復元後NG=0に復帰を確認。`report_consistency_check.py --fail-on-ng`で
NG=0（WARN=79件、既存と同水準）。pytest 497 passed/2 known failed
（既知の[[TEST-STALE-IV-1]] MSFT/NVDA、新規回帰なし）。

**残タスク: Stage 3**（対象年度・フィールドの追加特定が必要な保留分）:
RDW(2020)〈[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]で別途未解決の残差
$120,314,578が判明、要RedeemableNoncontrollingInterestEquityCommon
RedemptionValue加算〉、MO/PM/SCCO〈gross_profit genuine定義差は確定済み
だが対象年度リストの明示が必要〉、MRVL/AVGO/DELL旧CIK拡張分（MRVL
2007-2018・AVGO 2006-2014・DELL 2007-2013、フィールド別の詳細特定が
必要）、LLY（capital_expenditureの正確な対象年度特定）、BBAI/RKLB/SOFI/
VRT/ONDS（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1で解消済みだが
None化されたBSフィールド名の特定が必要）。「次セッションでの着手順序」欄
を更新。コミット未実施（ユーザー確認待ち）。

最終更新: 2026-08-05（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]
Stage 1実装完了。fixed_registry.jsonフィックス機構を実データで実測・
実登録した。taxonomy属性①〜⑧非該当銘柄を実測した結果47銘柄（属性
該当58銘柄、前回見積「約55銘柄」から上方修正）、既存チェックゲート
（registration_validator.py・report_consistency_check.py CHECK-1〜29・
revenue_tag_conflict_check.py）全通過の絞り込みを経て**Stage 1最終候補
26銘柄・372銘柄×年度エントリ**を確定・実登録した（revenue_tag_
conflict_check.pyのD&A/S&M系警告は`SEC_DATA_BUG_TAXONOMY.md` #19の
既知誤検知パターンと判断し除外対象から除外、TDYは真のrevenue系競合
`[[REVENUE-TAG-PRIORITY-FRAGILE-1]]`のためStage1から除外）。
`parser.py`（`_apply_fixed_registry_freeze()`、差分適用方式）・
`utils.py`（`compute_snapshot_hash()`）・`report_consistency_check.py`
（CHECK-31/WARN-31、NG化）を実装し、全105銘柄再パースでフィックス対象
372エントリ含め無変化を確認、CHECK-31の発火・復元も確認、pytest
497 passed/2 known failed（既知）、NG=0を確認。機能コミット
`7d7c63faf`。pushは保留、コミットのみ。Stage 2〜3（属性該当58銘柄の
段階的フィックス）が残タスク。「次セッションでの着手順序」欄を更新）

最終更新: 2026-08-03（[[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-
MISSELECT-1]]の全105銘柄横断スキャン結果を反映（チャット記録、読み取り
のみ）。不一致は1249年度中13件（1.04%）のみと判明。真のバグは
CRM(2011)・VRT(2017)の2件、CWAN(2023)は構造的に必然（cross-accn、
値は正しい可能性が高い）、9件（ELF/LITE/QBTS/TSLA/BKNG2009/HON/
V2008/DOCN/LYFT）はメタデータのみ不一致で実害なし、CAKE(2009)・
LITE(2014)は別種の異常（total_liabilities欠損、本バグとは無関係）。
根本原因を`_collect_own_data_instant()`のフィールド独立抽出設計と
`_resolve_bs_entity_mixing()`の「本人データaccnが単一」前提の限界と
特定（EXTRACTION_DESIGN_PRINCIPLES.md原則2の新実例）。実害確認:
CRM(2011)は現在の10年ROEトレイリング窓外のため実害なし、VRT(2017)は
窓内のため現在進行形の実害可能性あり（winsorize仕様により影響は限定的
と推測、実測は未実施）。対応方針2案（VRT型・CRM型）を記録し、VRT型
から着手する方針を確定。登録・更新のみ、実装は未着手。

最終更新: 2026-08-03（[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]残り13件の
個別調査結果を反映（チャット記録、読み取りのみ）。①genuine（対応不要）
2件（BKNG2011/2012、Redeemable NCIがFairValue基準のみで簿価タグが
存在しないため既存の許可リスト設計方針と整合的に対応不要と確定）・
②許可リスト拡張で対応可能2件（ASTS2020のTemporaryEquityValue
ExcludingAdditionalPaidInCapital加算・RDW2020のRedeemableNoncontrolling
InterestEquityCommonRedemptionValue、HEI型フォールバックと同型の別タグ名
パターン）・③要さらなる確認7件（PLTR2019・CART2023-2025・V2008・
CELH2025・ASTS2019）に分類。調査の過程でCRM(2011)・VRT(2017)の2件が
CHECK29の対象外（NCI/一時的持分タグ不足ではなく、stockholders_equity
抽出自体が別年度・別filingの無関係な値を誤って採用している独立した
parser.pyバグ）と判明し、[[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-
MISSELECT-1]]（優先度：高、新規）として分離登録。「次セッションでの
着手順序」欄を更新。登録・更新のみ、実装は未着手。

最終更新: 2026-08-03（BACKLOG.md統合作業。同一種類の作業をまとめられる
3グループを統合（実装・修正は行わず記録整理のみ）。①normalized/スキーマ
問題群6件（SCHEMA-STDEBT-COVERAGE-GAP-1・SCHEMA-SM-SGA-CONFLATION-1・
SCHEMA-LTDEBT-DOUBLECOUNT-RISK-1・SCHEMA-SHARESBASIC-CONCEPT-MISMATCH-1・
SCHEMA-NORMALIZED-ANNUAL-NAMING-MISMATCH-1・SCHEMA-DA-FALLBACK-MISSING-1）
を`[[SCHEMA-NORMALIZED-ISSUES-1]]`へ統合。②不要ファイル判定待ち4件
（PHASE1-SCAN-CLEANUP-1・BACKFILL-HISTORY-CLEANUP-1・QUALITY-CHECKER-
CLEANUP-1・REPORT-TXT-PARSER-CLEANUP-1）を`[[DEAD-CODE-AUDIT-BATCH-1]]`
へ統合（STALE-SUBPORT-CLEANUP-1は判定基準がリポジトリ外の別システム
〈AutoTrade〉の参照確認を要し、他4件の「grep確認→未使用なら削除」という
共通基準と異なるため統合対象から除外し、既存エントリのまま残置）。
③銘柄リスト重複読み込み3件（SYSHEALTH-CIK-DEDUP-1・TAIL-CIK-LOOKUP-
DEDUP-1・TICKER-SOURCE-CONFIG-DUP-1）を`[[TICKER-LOADING-
UNIFICATION-1]]`へ統合。いずれも旧ID参照を「(旧XXX)」形式で各箇条書きに
残置し、`SEC_EDGAR_LAYER_DESIGN.md`・`layer3_builder.py`等の外部からの
旧ID言及の追跡可能性を維持。DESIGN-8の既知のID重複（8-3/8-4が同一
`[DESIGN-8]`タグを共有）も確認し、`[DESIGN-8-3]`/`[DESIGN-8-4]`へ表記
訂正（他ファイルからの`[[DESIGN-8]]`参照が皆無であることを確認済みの
ため実質的な参照断絶リスクなし）。BACKLOG_DONE.mdの8-1/8-2/8-5/8-6
（完了済み）も同型のID共有パターンを持つが、今回のスコープ外のため
未対応のまま。コミットのみ、pushは保留。

最終更新: 2026-08-03（[[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]実装
完了。CHECK29の本人データ〈own-accn〉限定照合に、cross-accnフォール
バック（同一end_dateの他filingへ探索範囲を拡張）を実装。M&A・組織再編
直後、一時的持分が当該年度自身の10-Kには存在せず後続filingの比較列
としてのみ開示されるケース（COHR2022/2023・CRWV2024・VRT2018）を解消。
実装過程の最初の版で、既存の正しい解消結果を壊す回帰5件（SOUN2021・
PM2010/2011・TSLA2020/2021・HEI2014・FCX2015、いずれも「別タグ族での
同額重複計上」または「own-accnのみで既に完成していた解への不要な追加」
が原因）を検知し、コミット前に復元。2段階ガード
（①ベースゲート: own-accnのみで恒等式が厳密に一致する場合はフォール
バック自体をスキップ、②重複値ガード: 候補値が既にmatched済みの値と
同額の場合は不採用）を再設計し、全105銘柄・全156エントリの網羅的な
before/after比較で回帰5件の再発防止・意図した4件の解消・TSLA(2016)の
精度改善（1件、既存判定は不変）を確認してからコミット。WARN-29は
17件/11銘柄→13件/9銘柄に減少。annual_YYYY.json等の実データは無変更
（検知専用ロジックのため）。pytest 497 passed/2 known failed（既知・
無関係）。機能コミット`b63be0026`・データ再生成コミット`05e7f853d`。
[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]の残件を15件→13件に更新。
「次セッションでの着手順序」欄を更新。pushは保留、コミットのみ。

最終更新: 2026-08-03（[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]の長期的構造
対応（パイプライン統合）の設計調査結果を反映（チャット記録、読み取り
のみ）。当初「parser.py⇔layer3_builder.pyの2パイプライン問題」という
認識を訂正し、`update.py`内で3つの独立生成パス（①parser.py→
annual_*.json、②quarterly.py→normalizer.py→normalized/*.json、
③layer3_builder.py→ttm_calculator.py→ttm/*.json）が並存する構造であり、
SEC_EDGAR_LAYER_DESIGN.mdが既に「3スキーマ併存」として認識済みの既知
課題の一部だったと確定。重複ロジック棚卸しの結果、parser.py側の安全
ロジックの大半（BS系バックフィル等）はTTM出力対象外フィールドで移植
不要と判明し、真に問題になりうるスコープは「FLOW型フィールドの本人
データ優先判定」のみに縮小。統合案A（完全統合・新DB構築フェーズ相当）・
案B（部分統合・個別バグ修正1〜2件相当）・案C（運用チェック継続）を
再評価し、実害ゼロ確定・スコープ限定・新DB構築フェーズD以降の射程・
既存の個別重複許容先例を根拠に案C（運用チェック継続、統合作業は
着手しない）を推奨として確定。着手条件を「案Bのトリガー（TTM anchor
範囲内×FLOW型フィールドの実害発生）」「案A/Bのトリガー（新DB構築
フェーズD着手時）」の2条件保留に更新。登録・更新のみ、実装は未着手。

最終更新: 2026-08-02（セッション終了処理。BACKLOG.md/BACKLOG_DONE.mdの
クロスリファレンス整合性を確認（本セッションでクローズした7件
〈[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]・[[GOOGL-FACT-
OVERRIDE-SEQUENCING-BUG-1]]・[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]・
[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]・[[TTM-CALC-QUARTER-CONTIGUITY-
UNCHECKED-1]]・[[KULR-CAPEX-TTM-STUB-ENTRY-CONTAMINATION-1]]〉が
BACKLOG.mdに残存していないこと、双方向の[[...]]参照が機能していることを
確認済み。DESIGN-8・UI-DISCOVER-1のID重複は本セッション以前からの既知の
構造的経緯のため今回は対応せず記録のみ）。作業ツリークリーン確認済み。
「次セッションでの着手順序」欄を最終整理（[[TTM-DATA-DRIFT-BEHIND-
PIPELINE-1]]を①に、以降指定順で再構成、[[XBRL-UNIT-SCALE-MISMATCH-
DETECTION-1]]はリストから除外）。PROJECT_STATUS.mdのcommon/sec_data/
統合フェーズ1備考欄・更新日も、セッション最終盤の構造的発見
（layer3_builder.pyとparser.pyの独立パイプライン問題）を反映して更新。
登録・整理のみで実装は未着手）。

最終更新: 2026-08-02（[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]の影響実測
結果を反映（チャット記録、読み取りのみ）。7件の既知修正
（[[PERIOD-LENGTH-VALIDATION-GAP-1]]・[[SPAC-SHELL-BS-ENTITY-
MIXING-1]]・[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]・
[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]・[[GOOGL-FACT-OVERRIDE-
SEQUENCING-BUG-1]]・[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]・
[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]）について、TANUKI VALUATION・
STONKS SILOいずれも現在進行形の実害はゼロと確定。BS項目・shares系は
TTM出力（FLOW_FIELDS）に構造的に含まれず消費経路もannual_*.json直接
参照のため無関係、その他は対象年度が現在のTTM anchor範囲（2021〜2022年
始まり）の外のため無関係、STONKS SILOはTTM/layer3を一切参照しない独立
パイプラインのため無関係、と確認。ただし2つの独立パイプラインが同期
しない構造的脆弱性自体は温存されているため、優先度を「高」→「中」に
引き下げつつエントリは残置。短期的運用対応・長期的構造対応の2案を記録。
「次セッションでの着手順序」欄を更新。登録・更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]の内容を確定
（チャット記録、読み取りのみ）。GitHub Actions APIでワークフロー実行
履歴を確認した結果、`SEC Data Update`ワークフロー自体は正常稼働中
（毎週日曜success、無効化なし）で、単なる週次発火タイミングの問題と
判明。一方、より深刻な構造的発見: `common/sec_data/ttm/`を生成する
`layer3_builder.py`は`parser.py`（annual_YYYY.json生成）とは完全に
独立した別実装のパイプラインであり、`fact_overrides.json`も読み込まず
`_resolve_bs_entity_mixing()`等annual側の主要ロジックも実装されていない
ことを確認。結果、本セッションの修正（[[PERIOD-LENGTH-VALIDATION-
GAP-1]]・[[SPAC-SHELL-BS-ENTITY-MIXING-1]]・[[TOTAL-LIABILITIES-
FALLBACK-TAG-DESIGN-FLAW-1]]・[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]・
[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]・[[COHR-SHARES-DILUTED-UNIT-
SCALE-BUG-1]]・[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]）はワークフローが
正常実行されてもTTM系列には反映されない（唯一の例外は
[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]、ttm_calculator.py自体への
実装のため次回実行で全105銘柄に自動反映）。対応方針を確定: まずTANUKI
VALUATION・STONKS SILOの実消費への影響を実測確認してから、大規模な
layer3_builder.pyへの個別移植の要否・優先度を判断する。「次セッション
での着手順序」欄を更新。登録のみ、実装は未着手）。

最終更新: 2026-08-02（[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]を新規登録
（優先度：高）。[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]実装検証時に
発見した「common/sec_data/ttm/配下が2026-07-26生成のまま、以降の
layer3_builder.py/q4_implied.py〈2026-07-30〉・parser.py〈2026-08-02〉
側のパイプライン修正に追従せず陳腐化している」という事象について、
既存BACKLOG.md/BACKLOG_DONE.mdに独立登録がないことをgrepで確認した
うえで新規登録した。PEP実測（SG&A約9.5%差）・`.github/workflows/
SEC_Data_Update.yml`（毎週日曜自動実行の既存ワークフローの存在、なぜ
2026-07-26以降ttm/が更新されていないかは未確認）を記載。対応方針は
未定、まず陳腐化の実際の範囲（何銘柄・何フィールド）とワークフロー
実行履歴の確認調査が必要。「次セッションでの着手順序」欄を更新。登録
のみ、実装は未着手）。

最終更新: 2026-08-02（[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]実装
完了。`ttm_calculator.py`に`_last4_is_contiguous()`（合計スパン305-425日・
隣接ギャップ±10日）を新設し`calc_ttm_series()`のlast4選定直後に挿入
（機能コミット`e2892a91f`）。実装中に、eps_basic/eps_dilutedを対象外
とする除外を実装し忘れ99/105銘柄が誤って影響を受ける不具合を発見し
即座に是正（`CONTIGUITY_CHECK_EXEMPT_FIELDS`追加）。該当18銘柄のTTM
系列を再生成（データコミット`426b4fa2f`、対象外の87銘柄は無変化を
確認の上で意図的に据え置き）。`pipeline.py`試験実行によりRCAT・KULRの
IV影響を実測しΔIV=$0を確定（`FCF外れ値`検知時の代替推定オーバーライドが
fcf_avgの値によらず同一の理論株価を出力するため、当初見立てていた
「約20%改善がIVに反映される」は誤りと訂正）。FROGはTTM系列を維持し
投資適格性の結論に変化なし。report_consistency_check.py NG=0・
pytest 519 passed/2 known failedを確認。[[TTM-CALC-QUARTER-CONTIGUITY-
UNCHECKED-1]]・[[KULR-CAPEX-TTM-STUB-ENTRY-CONTAMINATION-1]]をいずれも
BACKLOG_DONE.mdへ移動。「次セッションでの着手順序」欄を更新。pushは
保留、コミットのみ）。

最終更新: 2026-08-02（[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]の実装前
最終シミュレーション完了（チャット記録、読み取り・オフラインシミュレー
ションのみ）。重要な設計上の発見: 「不完全な四半期の代わりに古い代替
4四半期を探索する」設計は、KULRで試験実装した結果、正規四半期を巻き
添えで飛ばし約6ヶ月古いデータを現在時点のものとして無自覚に混入させる
危険な挙動が判明したため不採用とし、単純に`quarters_used=0`とする保守的
設計を採用確定。該当18銘柄中FCF自体に変化が生じるのはRCAT・KULR・FROGの
3銘柄のみ、他87銘柄・PEP等の正当ケースで新規誤検知なしを最終確認。
重要な訂正: KULR・RCATともにTTM系列の完全点数不足で年次実績への完全
フォールバックが発生することが判明し、KULRのfcf_avgは約20%改善
（前回試算2.7%より大幅に大きい）・RCATは約59%改善（既報告の34〜55%
過小評価の枠組みより大きい変化、ただしΔIV=$0の結論は別経路のオーバー
ライドにより引き続き有効と推定）。実装設計を確定（calc_ttm_series()内
last4選定直後に連続性チェックを挿入、既存のTTM-QUARTERS-CHECK-1と自然に
統合）。[[KULR-CAPEX-TTM-STUB-ENTRY-CONTAMINATION-1]]は本実装で自動解消
される旨を追記。「次セッションでの着手順序」欄を更新。登録・更新のみ、
実装は未着手）。

最終更新: 2026-08-02（[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]の105
銘柄横断スキャン完了（チャット記録、読み取りのみ）。eps_basic/eps_diluted
は既知・実害なしの仕様のため対象外と確定。除外後18銘柄が該当し、①RCAT型
（標準タグの一時的空白、既報告・ΔIV=$0で確定済み）②タグ切り替え・段階的
移行型（5銘柄、FCF中核フィールド非該当）③本人データ側の異常エントリ型
（新規発見、KULR/FROG）の3タイプに分類。KULRは開始日欠落の異常エントリ
（$2,000,000）がcapital_expenditureに混入し、2024年度で約10.8倍・2023
年度で約30%過大、fcf_avgが約2.7%変化する現在進行形の実害を確認し
[[KULR-CAPEX-TTM-STUB-ENTRY-CONTAMINATION-1]]として新規登録（優先度：
高）。FROGは同型だが影響僅少。対応方針の実現可能性を確認: 「合計スパン
305〜425日」「隣接四半期間ギャップ±10日以内」の統一チェックで3タイプ
全てを検知できることを確認、個々の四半期長は判定基準に含めない（PEP等
の正当な決算暦特性を誤検知しないため）。実装前には全母集団シミュレー
ションが必要。「次セッションでの着手順序」欄を更新。登録・更新のみ、
実装は未着手）。

最終更新: 2026-08-02（[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]実装完了。
`fact_overrides.json`にCOHR(2009-2011)のshares_diluted/shares_basic
（単位スケール補正、値は事前確定済み）を追加（機能コミット`82e25d92d`）。
GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1で確立済みの`_apply_fact_overrides()`
がそのまま機能し、コード変更は不要と確認。COHR再生成（データコミット
`3896f7393`）でshares_diluted/basicを3年度とも是正、net_income/eps系は
無変化、NI≈EPS×Sharesの恒等式が1%未満の誤差で成立するようになったことを
確認（修正前は約1000倍の乖離）。105銘柄フローズン入力比較でCOHR以外は
0件差分、report_consistency_check.py NG=0・WARN 81件（変化なし）、
pytest 519 passed/2 known failed（既知・無関係）を確認。TANUKI VALUATION
（get_diluted_shares()は直近1年度のみ参照）・STONKS SILO（COHRは
stonks_silo=falseで追跡対象外）いずれも影響なしと確定。同エントリを
BACKLOG_DONE.mdへ移動。「次セッションでの着手順序」欄を更新。pushは
保留、コミットのみ）。

最終更新: 2026-08-02（[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]の実装前
最終確認（チャット記録、読み取り・オフラインシミュレーションのみ）完了。
既存の恒等式ベース安全網（`_backfill_total_liabilities_via_identity()`・
[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]）との相互作用リスクは
なし（BS項目とshares項目でフィールド集合が重ならず構造的に相互作用
経路が存在しない）と確認、前回懸念したWMT(2014)型のすり抜けもガード
条件により正しく除外されることを確認。ガード適用後の全母集団シミュ
レーションで該当・変化するのはCOHR(2010)のshares_diluted/basicの2
フィールドのみと最終確定。実装方針を確定: [[COHR-SHARES-DILUTED-
UNIT-SCALE-BUG-1]]の`fact_overrides.json`個別上書き（3年度とも1回で
解決）を実装対象とし、tie-break変更（ソースコード変更）は当面見送る
（2010年度1件しか解決せずfact_overrides側で重複解決される・現時点で
COHR以外に該当する実ケースがゼロと確定しコストに見合う価値が現状ない
ため。ガード条件の設計自体は破棄せず将来の予防的対応として保留）。
着手条件を更新。登録・更新のみ、実装は未着手）。

最終更新: 2026-08-02（セッション終了処理。BACKLOG.md/BACKLOG_DONE.mdの
クロスリファレンス整合性を確認（本セッションでクローズした5件
〈[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]・[[GOOGL-FACT-
OVERRIDE-SEQUENCING-BUG-1]]・[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]〉が
BACKLOG.mdに残存していないこと、双方向の[[...]]参照が機能していることを
確認済み。DESIGN-8・UI-DISCOVER-1のID重複はいずれも本セッション以前から
存在する既知の構造的経緯（DESIGN-8はサブタスク8-1〜8-6の共有ベースID、
UI-DISCOVER-1は同一IDの別タスクへの再利用）であり、本セッションの作業
とは無関係のため今回は対応せず記録のみ）。作業ツリークリーン確認済み。
「次セッションでの着手順序」欄を最終整理（[[XBRL-UNIT-SCALE-MISMATCH-
DETECTION-1]]を①に、[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]は密結合の
ため①内に統合表示）。PROJECT_STATUS.mdのcommon/sec_data/統合フェーズ1
備考欄・更新日も本セッション後半の完了項目を反映して更新。登録・整理の
みで実装は未着手）。

最終更新: 2026-08-02（[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]の全母集団
シミュレーション（チャット記録、読み取り・オフラインシミュレーションの
み）の結果、当初提起した「tie-break条件を新しいfiling優先に単純変更
する」という方針は不採用と確定。31銘柄・124件で値が変化し、確実な改善は
COHRの2件のみで、残り122件は改悪（VZ(2008)純利益が黒字$6,428M→赤字
-$2,193Mに反転等）・改悪疑い（SOUN/KULRのSPAC実体混同、HON/FCX/HEIの
restatement・株式分割調整）が大半。WMT(2014)では既存の恒等式ベース
安全網が偶発的にすり抜け[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-
FLAW-1]]型のバグを別経路で復活させかねない相互作用リスクも発見。
「同符号かつ比が10のべき乗値」というガード条件を適用すると124件中
COHRの2件のみが該当することを確認し、ガード条件付き介入として
[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]に統合・実装方針を追記。
[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]は「解消・統合」として
BACKLOG_DONE.mdへ移動。「次セッションでの着手順序」欄を更新。登録・
統合のみ、実装は未着手）。

最終更新: 2026-08-02（[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]の対応方針を
確定（チャット記録、読み取り・オフラインシミュレーションのみ）。COHR個別の
是正は`fact_overrides.json`個別上書き（GOOGLと同型）に確定、値は2009年度
60,164,000/59,334,000・2010年度61,504,000/60,304,000・2011年度
63,612,000/62,211,000（shares_diluted/basic）。「後続filing優先」への
一般設計変更は不採用（2011年度は本人データ優先ロジックが他銘柄で正しく
機能しており巻き添えリスク大、2009年度はそもそも後続filingに正しい値が
存在せず解決しない）。調査過程で2件を新規分離登録: 本人データ不在時に
複数比較年度再掲が競合すると最も古いfilingが勝つ未文書化tie-break欠陥
[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]（優先度：中〜高、COHR2010で実証）、
同一タグ・同一期間の値が複数filing間で10のべき乗単位（1000倍等）で乖離
する場合を検知する汎用チェック提案[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]
（優先度：中、105銘柄試験適用で18銘柄・126件を検出）。「次セッションでの
着手順序」欄を更新。登録・更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]実装完了。
影響範囲確認（チャット記録、読み取り・オフラインシミュレーションのみ）で
fact_overrides.json対象がGOOGL(2012/2013)限定であること、逆算バックフィル
との重複入力がrevenueのみ（→gross_profit逆算にのみ影響）であることを確認
した上で、案A（`_apply_fact_overrides()`を全逆算バックフィルより前に移動）
を採用して実装（機能コミット`ba8628198`）。extracted[field]["annual"]
[year]構造への書き込みに作り直し、`_parse_raw_data()`内で抽出直後・
`_backfill_total_liabilities_via_identity()`/`_backfill_gross_profit_
from_revenue_cogs()`より前で実行するよう変更。GOOGL再生成（データコミット
`dd6fba1a1`）でgross_profitを是正（2012: $32,999M→$28,863M、2013:
$37,832M→$33,526M）。105銘柄フローズン入力比較でGOOGL(2012/2013)以外は
0件差分・全19年次/51四半期も他の変化なしを確認。report_consistency_
check.py NG=0・WARN 81件（変化なし）、pytest 519 passed/2 known failed
（MSFT/NVDA、既知・無関係）を確認。TANUKI VALUATIONはgross_profitを参照
しておらずSTONKS SILOの追跡対象にもGOOGLは含まれないため影響なしと確定。
同エントリをBACKLOG_DONE.mdへ移動。「次セッションでの着手順序」欄を更新。
pushは保留、コミットのみ）。

最終更新: 2026-08-02（[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]
の残る3種（GP≠Revenue−COGS・OI>GP・NI≠EPS×Shares）の分類調査結果
（チャット記録、読み取りのみ）を反映し、同エントリを「分類調査完了・
後継タスクへ引き継ぎ」としてBACKLOG_DONE.mdへ移動しクローズ。GOOGL
(2012/2013)のGP≠Revenue−COGSは`fact_overrides.json`によるrevenue手動
補正が`_backfill_gross_profit_from_revenue_cogs()`より後段で実行される
シーケンシングバグと確定し[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]
（優先度：中〜高）として新規登録。LMT(18/19年度)のOI>GPは同一accn・
非derivedの安定パターンから①genuine（設計スコープ外、対応不要）と確定。
COHR(2009-2011)のNI≠EPS×Sharesは、COHR自身のFY2011 10-Kが
shares_dilutedを実際の1/1000でタグ付けしていた本人データ側の単位
スケール申告誤りと確定し[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]
（優先度：中）として新規登録。「次セッションでの着手順序」欄を更新。
登録・クローズのみ、実装は未着手）。

最終更新: 2026-08-02（[[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]を
新規登録（優先度：低）。CHECK29のHEI・ONDS実装検証時、PM銘柄の
`bs_identity_violations_log.json`でキー順序のみが実行のたびに非決定的に
変化する現象を発見（Python `frozenset`のハッシュランダム化が原因と推定、
値・resolved状態は完全に同一で実害なし）。登録のみ、実装は未着手）。

最終更新: 2026-08-02（[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]のHEI・
ONDS型を実装完了。CHECK29の許可リストに`TemporaryEquityRedemption
Value`（CarryingAmount系タグ不在時のフォールバック限定）・
`RedeemableNoncontrollingInterestEquityCarryingAmount`のSUPERSEDES
ルールを追加（機能コミット`a910afef2`）。全105銘柄で再検証し156件中
133件→139件が解消（HEI×5・ONDS×1）、副次的にFCX(2013)も改善、他99
銘柄・既存133件・COHR型2件・残り15件のresolved状態は維持を確認。
annual_YYYY.json等は無変更、pytest 497 passed/2 known failed、
WARN 83→81件（-2）を確認。「次セッションでの着手順序」欄を更新。
BACKLOG更新コミットは機能コミットとは別）。

最終更新: 2026-08-02（[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]の個別調査
（COHR・HEI・ONDS優先、チャット記録、読み取りのみ）完了。3件とも
①genuineと確定（②タグ選定バグに分類されるものはなし）。COHR(2022/2023)
はCHECK29の「本人データ限定」照合という設計方針そのものが原因で検知
不可能な構造的限界と判明し[[CHECK29-COHR-CROSS-ACCN-TEMPORARY-
EQUITY-1]]として別スコープで新規登録（優先度：中）。HEI(2009-2013)は
TemporaryEquityRedemptionValueをCarryingAmount系タグ不在時のフォール
バックとして許可リストに追加すれば対応可能と判明。ONDS(2023)は
CHECK29自体のSUPERSEDESルール不備（自己申告）と判明、既存ルールと
同型の拡張で対応可能。残る20件（PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/
VRT/RDW）は未着手のまま。「次セッションでの着手順序」欄を更新。調査・
登録のみ、実装は未着手）。

最終更新: 2026-08-02（[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]
実装完了。会計恒等式TA=TL+SE(+NCI+一時的持分)検証をOR条件フォールバック
方式（許可リスト方式のタグ選定）でparser.py・report_consistency_
check.py（CHECK-29/WARN-29）に実装（機能コミット`bd91000f0`）。全105銘柄
で検証し156件中133件が拡張形で解消・23件が未解消（事前シミュレーションと
完全一致）、既存1,085件への新規誤検知なし、annual_YYYY.json等の既存
データ値は無変更（新規bs_identity_violations_log.json 105件のみ追加）、
pytest 497 passed/2 known failed、WARN 70→83件（純増13件、全てWARN-29）を
確認。同エントリをBACKLOG_DONE.mdへ移動し、[[CHECK29-UNRESOLVED-23-MIXED-
CAUSES-1]]（着手条件充足）・[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-
MISSING-1]]（TA=TL+SE分の対応完了を反映、残る3種の分類調査は未着手のため
存置）を更新。「次セッションでの着手順序」欄を更新。BACKLOG更新コミットは
機能コミットとは別）。

最終更新: 2026-08-02（[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]
実装前シミュレーション完了（チャット記録、読み取り・オフライン試算のみ）。
当初想定した「TA=TL+SE+NCI+TemporaryEquityへの拡張」を無条件適用する
設計は、既存の正しい1,085件のうち33件（VZ最大-$56.6B・WMT・KO・AVGO・
LLY・AMD・ASTS・BROS・CAKE）で新規誤検知を生む重大な危険があると実証。
「TA=TL+SEが不一致の場合のみNCI・一時的持分を試すOR条件フォールバック
方式」・許可リスト方式のタグ選定に設計を確定。この設計で156件中133件
（85.3%）が解消見込みと判明し、残る23件を
[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]として新規登録（優先度：中）。
検知専用ログフォーマット・report_consistency_check.py側の実装方法・
実装コストの再評価も反映。「次セッションでの着手順序」欄を更新。設計
確定・登録のみ、実装は未着手）。

最終更新: 2026-08-02（[[HEI-LRCX-TA-TLSE-UNEXPLAINED-RESIDUAL-1]]根本原因
調査完了（チャット記録、読み取りのみ）。登録時は「バグ・未特定の会計
恒等式不整合」としたが、対象accn・end_dateの全XBRLタグを機械的に網羅する
手法で再調査した結果、HEI(2020)は`TemporaryEquityCarryingAmountIncluding
PortionAttributableToNoncontrollingInterests`（前回未チェックの別名
タグ）、LRCX(2012)は`TemporaryEquityCarryingAmountAttributableToParent`
（候補には含めていたが確認スクリプトの表示件数制限で該当年度分を見落とし）
で、いずれもTA=TL+SE+NCI+TemporaryEquityが完全一致することを確認。
「誤登録・訂正のうえクローズ（原因は①genuine、探索範囲不足による誤判定
だった）」としてBACKLOG_DONE.mdへ移動。追加でTSLA・XOMもサンプル確認し
完全一致を確認したことで、累計10銘柄が例外なく①genuineに分類され、
[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]のTA=TL+SE違反156件は
ほぼ全件が①genuineへ収束する見込みが高いと判明。同エントリへ追加調査
結果・確定対応方針を追記。「次セッションでの着手順序」欄を更新。訂正・
クローズ・更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]
のTA=TL+SE違反156件・分類調査完了（チャット記録、読み取りのみ）。持続性
区分（単年度28銘柄・2年度5銘柄・3年度以上17銘柄）を確定。8銘柄のサンプル
確認で6銘柄（FCX/BROS/RKLB/GTLB/COHR/ONDS）が①genuine（NCI・一時的持分の
未捕捉、設計スコープ外）と確定、156件の過半数が①に該当する見込みと判明。
HEI・LRCXの2銘柄はNCI等を含めても解消しない未特定の不整合と判明し
[[HEI-LRCX-TA-TLSE-UNEXPLAINED-RESIDUAL-1]]として新規登録（優先度：
中〜高）。恒等式検証の対応方針を「TA==TL+SE+NCI+一時的持分」の拡張形で
確定。「次セッションでの着手順序」欄を更新。分類調査・登録のみ、実装は
未着手）。

最終更新: 2026-08-02（`docs/architecture/new_data_platform/
EXTRACTION_DESIGN_PRINCIPLES.md`を新規作成。common/sec_data/抽出
アーキテクチャの俯瞰的脆弱性分析で判明した5バグの教訓（期間の妥当性・
フィールド間整合性・会計恒等式の3原則）を、これから新設する
`common/market_data/`・`common/macro_data/`向けに一般化。
`MIGRATION_CHECKLIST.md`と同型の位置づけの独立文書。
[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]・[[CHECK29-
ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]から本文書への参照を追記。
CHAT_RULES.mdの「新DB構築プロジェクトの進捗管理」節・PROJECT_STATUS.md
のcommon/market_data/・common/macro_data/行にも参照を追記。作成・登録
のみ、実装は未着手）。

最終更新: 2026-08-02（common/sec_data/抽出アーキテクチャの俯瞰的脆弱性
分析完了（チャット記録、読み取りのみ）。本セッションで発見した5バグ
（[[PERIOD-LENGTH-VALIDATION-GAP-1]]・[[TOTAL-LIABILITIES-FALLBACK-TAG-
DESIGN-FLAW-1]]・[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]・[[SPAC-SHELL-
BS-ENTITY-MIXING-1]]・[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]）が
「候補プールから単純な新しさ基準で1つを確定し、他フィールド・他期間・
会計上の制約とは一切照合しない」という共通の設計的欠陥に帰着すると判明。
105銘柄への機械的予備スキャンでTA≠TL+SE違反156件（50銘柄）・GP≠Revenue−
COGS違反43件（9銘柄、GOOGL(2012/2013)は新規発見）・OI>GP違反22件（LMT
単独、新規発見）・NI≠EPS×Shares違反67件（31銘柄、COHRに単位スケール
バグの疑い）を確認。[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]
（優先度：高、分類調査未着手）・[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-
LAYER-1]]（優先度：高、横断検証レイヤー新設提案）を新規登録。「次
セッションでの着手順序」欄を更新。登録のみ、実装は未着手）。

最終更新: 2026-08-02（[[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-
UNCHECKED-1]]根本原因調査完了（チャット記録、読み取りのみ）。当初の懸念
（継続/非継続タグの取り扱いミス）ではなく、`ttm_calculator.py::
calc_ttm_series()`が採用四半期の日付連続性を検証しない一般的な設計欠陥が
根本原因と判明。RCATでは標準タグの空白（継続/非継続分割開示と決算期変更が
重なった約11ヶ月間）により、2023年7〜10月・10月〜2024年1月の四半期が
`ttm_end=2025-03-31`・`2026-03-31`の両方に重複使用され、現在の
fcf_5yr_avg（-40,185,008.5）・fcf_2yr_avg（-50,540,837.0）が正しい値
（試算：約-53,985,212・約-78,141,244）より34〜55%過小評価と確定。ただし
IVへの影響は現時点でΔIV=$0（revenue floor＋EPSベース推定オーバーライド
が吸収、将来業績改善時に顕在化しうる潜在リスクの留保付き）。他銘柄
（HON/AVAV/TER）への現時点の実害なしと確認。優先度を「高→中」に訂正し、
根本原因（ticker非依存の一般的欠陥）を
[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]として新規登録（優先度：
中〜高）。「次セッションでの着手順序」欄を更新。登録・更新のみ、実装は
未着手）。

最終更新: 2026-08-02（[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]パターンB実装前
シミュレーション完了（チャット記録、読み取り・オフライン試算のみ）。RCATの
本番FCF計算がreader.py::get_fcf_5yr_avg()（年次ファイルベース）を使わず、
data_fetcher.py::_select_fcf_source()がTTM系列
（common/sec_data/ttm/RCAT_ttm_series.json）を優先採用する設計と判明。
TTM系列は四半期10-Qの集計であり年次10-Kの継続/非継続事業分割タグ問題の
影響を受けず既に完全な値を持つため、年次パーサー側のパターンB実装では
RCATのfcf_base_used・DCF・tanuki_score・Classificationは一切変化しない
（ΔIV=$0と試算確認）ことが判明し、優先度を「高→低」に訂正。副次的に
発見したTTM系列生成ロジック側の継続/非継続タグ扱い未検証の問題を
[[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]として新規登録
（優先度：高）。「次セッションでの着手順序」欄を更新。訂正・登録のみ、
実装は未着手）。

最終更新: 2026-08-02（[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-
GAP-1]]TANUKI VALUATION実害確認調査完了（チャット記録、読み取りのみ）。
25銘柄中24銘柄（AAPL/MSFT/TSLA/XOM/CAT/ABBV等を含む）は該当年度がすべて
現在の直近5年窓（2021-2026年）の外にあり実害なしと確定、優先度を「高→
中」に訂正。RCAT単独については、前回（[[FETCHER-10KT-10QT-FORM-
EXCLUSION-1]]）の「成長率決定には影響しない」という限定的確認だけでの
「実害なし」結論を訂正し、`reader.py::get_fcf_5yr_avg()`が実質2021-2023年
の3年平均になっておりDCFのFCFベース値計算に構造的な実害があることを確認、
[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]として新規登録（優先度：高）。「次
セッションでの着手順序」欄を更新。登録・訂正のみ、実装は未着手）。

最終更新: 2026-08-02（[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]根本原因
調査完了（チャット記録、読み取りのみ）。RCATのoperating_cash_flow欠落は
標準タグ`NetCashProvidedByUsedInOperatingActivities`がFY2024フィリングから
継続/非継続事業の分割タグに置き換わったことが原因と確定。105銘柄横断
スキャンで25銘柄該当（AAPL/MSFT/TSLA/XOM/CAT/ABBV等の主力銘柄を含む）する
候補タグ設計欠陥と判明し、`operating_cash_flow`はTANUKI VALUATIONのDCF/
FCF計算に直結するため実害の可能性が高いと判断。「原因確定・スコープ拡大・
統合」としてBACKLOG_DONE.mdへ移動し、
[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]として新規登録
（優先度：高）。「次セッションでの着手順序」欄を更新。登録のみ、実装は
未着手）。

最終更新: 2026-08-02（[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]案③実装完了。
`report_consistency_check.py`にCHECK-28（WARN-28）を新規追加（コード
`1fd44fc0a`）し、company_facts.json上のform=10-KT/10-QTのaccnが
`accn_to_reportdate`に未登録の場合を検知（検知のみ、自動修正なし）。全105
銘柄実行でRCATにWARN-28が2件発火（10-KT・**新規発見**の10-QT
〈2019年、RCAT第1回目の決算期変更由来〉）、他104銘柄で誤検知なし、WARN数
68→70件、NG=0維持。pytest 519 passed/2 known failed。データファイルは
無変更（検知のみ）。案1（relevant_forms追加+バケツ再設計）は見送り確定の
まま、BACKLOG_DONE.mdへ全文移動。「次セッションでの着手順序」欄を更新。
pushは保留、コミットのみ）。

最終更新: 2026-08-02（[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]対応方針を
確定（チャット記録、読み取りのみ）。案1（relevant_forms追加+バケツ再設計）
は見送り。RCAT own-data 10-K・10-KTがSEC自身により両方ともfy=2024と
タグ付けされており真正のバケツキー衝突が発生すること、旧12ヶ月データの
再配置先がないこと、複数消費者の改修が必要になることを確認し、コストが
当初想定より高いと判明。実害は確認済みでゼロ・対象は105銘柄中RCAT1銘柄
限定のため、案3（`report_consistency_check.py`への新規WARN追加のみ）を
採用方針として確定。トリガー条件（RCAT再変更または他銘柄での実害確認）
発生時に案1を再検討する旨を着手条件に明記。副産物として発見した
STONKS SILOのfpラベル脆弱性（fetcher.py側とは独立）を
[[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]として新規登録（優先度：
低〜中）。登録・更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案b実装
完了。`_align_cost_of_revenue_to_revenue_period()`を新規追加し、
revenue・cost_of_revenueが異なるaccnから独立採用され、かつ数学的矛盾
（revenue−cost_of_revenue≠gross_profit）が現に存在する年度についてのみ、
revenueと同一accn・同一期間の候補で矛盾が厳密に解消する場合に限り置換
（コード`b756021f6`＋安全性修正`9616e8058`・データ`7c94c6f95`）。
**実装検証時に重大な副作用を発見**（初回実装が矛盾のない年度＝GOOGL
(2008)/HON(2008)/SCCO(2009/2010)まで誤って書き換える巻き添え、
gross_profit未確定〈derived前〉年度との比較が原因）し、ゲート条件強化で
是正。最終的に対象はLRCX(2010)の1件のみ、全105銘柄フローズン入力比較で
無変化を確認、report_consistency_check.py NG=0（WARN=68件）、pytest
513 passed/2 known failed。CRM(2013)・JNJ(2017)・MRVL(2017)・ONDS(2017)
は案b単独では未解決のまま残存（案aの対応が必要な可能性）。エントリは
全件解決していないためBACKLOG.mdに残置し、実装結果・残存部分を明記。
「次セッションでの着手順序」欄を更新。pushは保留、コミットのみ）。

最終更新: 2026-08-02（[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]対応方針を
全面改訂（チャット記録、全母集団シミュレーション結果）。案a（候補タグ追加）
・案c（2タグ合算）とも単純適用は既存の正しい値を壊す重大な副作用を確認
（案aはLLY/FCX/CAT/ABBV等で新規劣化10件、案cはENTG/TERで2倍計上・CAT等
6件で破壊）、ゲート条件込みの再設計が必要として保留。案d（revenue側優先
順位変更）は105銘柄202件スキャンで大半がgenuine定義差と判明し不採用確定。
案b（同一accn優先）を採用方針とし、CRM型検知のため期間一致までの精密化
が必要と明記。着手条件に「ゲート条件を伴わない実装は行わないこと」を追加。
登録・更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]内容確定
（チャット記録、読み取りのみ）。残り6銘柄（AMD/BSY/KO/LRCX/ONDS/RMBS）を
個別調査した結果、全6銘柄が②タグ選定バグと確定（①genuine定義差は0件）。
確定9銘柄（AMD/BSY/CRM/JNJ/KO/LRCX/MRVL/ONDS/RMBS）の根本原因を4サブ
パターン（(a)候補タグ完全欠落・(b)クロスaccn/期間不整合・(c)複数タグの
合算漏れ・(d)同一filing内での類似タグ誤選択）に整理。net_income/
operating_income等主要フィールドへの波及なしと確認し、当初想定した重い
「同一期間強制」設計変更は不要と判明、軽量な個別候補タグ拡張（案a・c）
優先の対応方針に更新。着手条件（6銘柄個別確認）を充足済みとして削除。
登録・更新のみ、実装は未着手）。

最終更新: 2026-08-02（セッション終了処理。「次セッションでの着手順序」欄を
最終整理（①PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1 ②FETCHER-10KT-10QT-
FORM-EXCLUSION-1 ③RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1 ④LITE-COGS-
DA-TAG-UNMERGED-1 ⑤HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1
⑥ELF-ROE10YR-RECALC-PENDING-1 ⑦REPORT-CONSISTENCY-GROSSPROFIT-COGS-
CHECK-MISSING-1 ⑧STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1〈クローズ
済み、コード整理のみ将来検討〉の8件）。2026-08-01〜02セッション全体
（gross_profit調査発端の一連の作業）の完了・クローズ・新規登録サマリを
記録。BACKLOG整合性チェック実施、クロスリファレンス双方向・重複なしを
確認。クローズ・更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-
SCCO-1]]個別調査完了（チャット記録、読み取りのみ）。MO・PM・SCCOの3銘柄
を「①genuine定義差、確定・対応不要」としてクローズしBACKLOG_DONE.mdへ
移動（PM/MOはExciseAndSalesTaxesタグ、SCCOはDepreciationDepletionAnd
Amortizationタグが検出diffと完全一致することを10-K原本相当の生データ
突合で確認）。CRM/JNJ/MRVLで確定した「revenue/cost_of_revenue/gross_
profitが異なるaccn・会計年度から独立採用される」設計欠陥を
[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]として新規登録（優先度：中〜
高、[[SPAC-SHELL-BS-ENTITY-MIXING-1]]と同種のフィールド間整合性問題、
残り6銘柄は要個別確認）。LITEのCOGS由来償却費タグ未合算を
[[LITE-COGS-DA-TAG-UNMERGED-1]]として新規登録（優先度：低〜中）。
「次セッションでの着手順序」欄を更新。登録・クローズのみ、実装は未着手）。

最終更新: 2026-08-02（[[SPAC-STUB-PERIOD-VERIFICATION-1]]個別調査完了
（チャット記録、読み取りのみ）。11銘柄・12ティッカー年度すべてで現状の
処理が妥当と確認。SPAC系6銘柄（ASTS/IONQ/JOBY/RKLB/SOFI/SPIR）はBSが
SPAC本体の自己データ（Nasdaq上場要件由来の$5,000,00X型自己資本）、
PL/CFは後年filingの正しい12ヶ月比較列と確認（340-380日フィルタが単純に
None化するだけでなく、正しい代替値を自動的に拾い上げていたことが判明）。
SOUN(2020)・APGE(2022)・NOW(2010/2011)も現状妥当と確認（NOW(2010)の
BS一部欠落は原因未特定だが実害軽微につき注記のみ）。RCAT(2012)は
own-dataで充実、D&A「1日間」エントリはval=0のXBRLタグ付けミスと特定し
実害ゼロと確認。VRT(2016)は当初の記載理由（Emersonスピンオフ）が
事実誤認と判明し、実際はSPAC〈GS Acquisition Holdings Corp〉自身の
設立初年度スタブと訂正（データ自体は正確、実害はほぼゼロ）。
「解消（実害なし、現状の処理は妥当）」としてBACKLOG_DONE.mdへ移動。
「次セッションでの着手順序」欄を更新。クローズ・訂正のみ、実装は未着手）。

最終更新: 2026-08-02（[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]TANUKI
VALUATION/STONKS SILO実害確認調査結果を反映（チャット記録、読み取りのみ）。
TANUKI VALUATIONは実害なし（RCATのgrowth rateはsegment_weighted手動設定が
優先されannual_YYYY.jsonの年度系列・fcf_listを一切参照しないため）。
STONKS SILOは一時的な実害を確認（financial_trend_calculator.py::
_calc_yoy_change()のfpラベル完全一致照合が、8ヶ月しか離れていない新旧
"Q4"を誤って比較しchange_pct=-152.2%という歪んだシグナルを生成していた
可能性、実際に関数実行し数値確認済み）が、データ蓄積により現在は自然解消
済みと確認。優先度を「高→中」に訂正。副産物として発見した
[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（RCATのoperating_cash_flow
完全欠落、10-KT除外バグとは別原因の疑い）を新規登録（優先度：中）。
「次セッションでの着手順序」欄を更新。登録・更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]根本原因
調査完了（チャット記録、読み取りのみ）。3段階目の決算期変更は存在せず、
直近10-Kの12月/4月両クラスタ同時出現はSEC開示規則（Regulation S-X
Article 3-06等）による比較列表示の正常な挙動、era別anchor不一致も対称
探索設計により無害と確認。「解消（実害なし、当初の懸念は誤りだったと
確認）」としてBACKLOG_DONE.mdへ移動。調査中に発見した別種の実害
（RCATの決算期変更移行期スタブ8ヶ月分〈2024-05-01〜2024-12-31〉が
annual_YYYY.jsonから完全欠落。根本原因はfetcher.pyのrelevant_formsに
10-KT・10-QTが含まれずis_own_data判定が恒常的にFalseになるticker非依存の
設計欠落）を[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]として新規登録
（優先度：高）。「次セッションでの着手順序」欄を更新。クローズ・新規登録
のみ、実装は未着手）。

最終更新: 2026-08-02（[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]
実装完了。`SECParser._backfill_total_liabilities_via_identity()`を新規
追加し、貸借対照表恒等式逆算（total_assets − stockholders_equity）で
278件のtotal_liabilitiesをバックフィル（コード`ee46018b2`・データ
`11d75b2c0`）。278件全件で完全一致（許容誤差なし）、全105銘柄フローズン
入力比較で対象278件以外に変化なし、NVDA(2015)/RCAT(2023)で代替候補タグ
値ではなく逆算値が採用されていることを確認。derived provenanceに加え
逆算元データの本人データ有無を示すsource_is_own_dataを新設。
report_consistency_check.py NG=0（WARN=68件、変化なし）、pytest 504
passed/2 known failed（既知）。TANUKI VALUATION（growth.py・DCF/EV計算）
への影響なしを再確認。BACKLOG_DONE.mdへ全文移動。「次セッションでの
着手順序」欄を更新。pushは保留、コミットのみ）。

最終更新: 2026-08-02（[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]の
設計調査＋全母集団シミュレーション結果を反映（チャット記録、読み取り・
オフラインシミュレーションのみ）。278件の内訳をパターンA(恒常的欠如)
14銘柄238件・パターンB(過渡期欠如)8銘柄40件に確定。候補タグのフォール
スルー案は271件で代替候補が存在せずNone化にしかならないため不採用とし、
貸借対照表恒等式逆算（total_assets − stockholders_equity）によるバック
フィルを採用方針として確定（278件全件で計算可能・100%是正可能、
[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]①と同型パターン）。
安全性（既存の正しい値への影響ゼロ・負の自己資本10件でも意味を持つ）を
確認。実装時の留意点として、本人データでない基礎値に依存する11件を
明記。登録のみ、実装は未着手）。

最終更新: 2026-08-02（[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]（KULR2019
単独）の根本原因調査完了（チャット記録、読み取りのみ）。原因を
`XBRL_MAPPING["total_liabilities"]`の2番目のフォールバック候補
`LiabilitiesAndStockholdersEquity`（定義上`Assets`と数学的に一致する
誤った代替タグ）と確定。105銘柄への予備スキャンでAMZN/GOOGL/MSFT/NVDA等
大型株を含む278件（銘柄年度）に及ぶ横断的な設計欠陥と判明したため、
[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]をクローズしBACKLOG_DONE.md
「2026-08-02（完了）」へ移動、[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-
FLAW-1]]として新規登録（優先度：高）。downstream影響調査により
Net_Debt/Total_Debt算出への直接汚染はないことを確認済み（`pipeline.py`
内の診断WARN専用の消費のみ）。「次セッションでの着手順序」欄を更新
（①TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1を筆頭に追加）。登録のみ、
実装は未着手）。

最終更新: 2026-08-02（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2実装完了・
BACKLOG_DONE.mdへ移動（段階1・段階2いずれも完了）。`fetcher.py`で
formerNames（法人名変更履歴）を既存レスポンスから追加取得・保存（新規API
コールなし）、`_resolve_bs_entity_mixing()`にformerNames区間一致による
新トリガー条件③'を追加（コード`1f6e95d92`・データ`43470bccf`）。SPIR(2020)
のlong_term_debtをformerNames一致（triggered_by="former_names_window"）で
新規検知・None化。BBAI/RDW/RKLB/SOFI/VRTは③'でも重複検知されるが結果不変
（冪等性を確認）。全105銘柄フローズン入力比較でSPIR以外に変化なし（RKLBの
2025年再法人化という「単純な改名」ケースでの誤検知なしを含む）。
`spac_shell_detection_log.json`を全105銘柄で新規生成。pytest 473 passed/
2 known failed、report_consistency_check.py NG=0（WARN=68件、変化なし）。
残り99銘柄のformerNamesは通常の週次自動更新で自然にバックフィルされる
設計（特別な一括再取得は未実施）。pushは保留、コミットのみ）。

最終更新: 2026-08-02（セッション終了処理。[[STONKS-SILO-FETCHER-
GROSSPROFIT-BACKFILL-DUP-1]]をクローズしBACKLOG_DONE.mdへ移動（実害解消済み
〈STONKS SILO対象25銘柄で発火条件0件を確認〉、fetcher.py側のコード自体は
デッドコードとして残存・削除ではない旨を明記。コード整理はcommon/sec_data
統合フェーズ1到達時に別途検討）。「次セッションでの着手順序」欄を最終整理
（①SPAC-SHELL-BS-ENTITY-MIXING-1段階2 ②BS-ENTITY-MIXING-UNEXPLAINED-
ONDS-KULR-1 ③RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1 ④SPAC-STUB-PERIOD-
VERIFICATION-1 ⑤GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1
⑥HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1 ⑦ELF-ROE10YR-RECALC-
PENDING-1 ⑧REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1の8件）。
2026-08-01〜02セッションで完了6件・新規登録5件・訂正1件のサマリを記録）。

最終更新: 2026-08-02（[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]①
（本番書き戻し）実装完了・BACKLOG_DONE.mdへ移動。`SECParser._backfill_
gross_profit_from_revenue_cogs()`を新規追加し、標準タグから取得できない
gross_profitをrevenue-cost_of_revenue逆算値で埋め、`pl_provenance.
gross_profit.derived=True`を付与（コード`dc0507c27`・データ`65ddd0d6b`）。
Case A対象34銘柄342件で完全一致を確認、Case B残存49件・他71銘柄は無変化。
STONKS SILO fetcher.pyの重複自己修復ロジック（[[STONKS-SILO-FETCHER-
GROSSPROFIT-BACKFILL-DUP-1]]）はSTONKS SILO対象25銘柄全体で発火条件が
0件になり実質デッドコード化したことを確認（同エントリのクローズ判断材料）。
TANUKI VALUATIONはannual_YYYY.jsonのgross_profitを一切参照しないため
IV・Classificationへの影響はゼロと確定。②（突合検算）は[[GROSSPROFIT-
COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]へ引き継ぎ済み。pushは保留、
コミットのみ）。

最終更新: 2026-08-02（[[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]新規登録
（優先度：低。HON(2009)のgross_profit乖離が[[PERIOD-LENGTH-VALIDATION-
GAP-1]]是正後も残存、他8銘柄は全解消したのに対し既知パターンと異なる原因の
疑い）＋[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]の対象を
MO/PM/SCCOの3銘柄から14銘柄（AMD/BSY/CRM/JNJ/KO/LITE/LRCX/MO/MRVL/ONDS/
PM/RMBS/SCCO）へ拡大訂正。再スキャンでMO/SCCOが各10年連続の持続的乖離、
LITE(9年)・CRM(7年)という当初未記載の大規模クラスタが判明したことを反映。
登録・訂正のみ、実装は未着手）。

最終更新: 2026-08-02（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1実装完了。
`SECParser._resolve_bs_entity_mixing()`を新規追加し、「①複数accn混在・
②本人データaccnが単一に定まる・③現に数学的矛盾が確認できる・④アンカー
統一で実際に矛盾が解消する」の4条件を満たす年度に限定して単一accn強制を
適用（コード`80e51d2c2`・データ`c5e588474`）。条件④はKULR(2019)型の
巻き添えNone化を防ぐため実装中に追加。BBAI(2020)/RDW(2020)/RKLB(2020)/
SOFI(2020)/VRT(2019)/ONDS(2017)/KULR(2016)の7銘柄7年度で数学的矛盾を解消し、
全105銘柄フローズン入力比較で対象7件以外（矛盾のない56件・KULR(2019)・
SPIR(2020)含む）に変化がないことを確認。pytest 461 passed/2 known failed、
report_consistency_check.py NG=0（WARN=68件、変化なし）。優先度を高→中に
訂正（残る段階2はSPIR型の事前検知という予防的対応のため）。pushは保留、
コミットのみ）。

最終更新: 2026-08-02（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]対応方針設計調査結果
を反映。案A（単一accn強制）単独は105銘柄・87件シミュレーションで正常系56件
（41銘柄）を新たにNone化する副作用が判明し不採用と確定。段階1（複数accn混在
かつ数学的矛盾が既に確認されている場合のみ単一accn強制、新規データ取得
不要・副作用ゼロ）と段階2（SPIR型の事故的正しさを事前検知するSPAC合併疑い
機械的検知＝案B、submissions.jsonへのformerNames取得拡張が前提）の二段構成に
整理した。[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]をKULR(2019)単独の
課題に再定義（ONDS(2017)・KULR(2016)は段階1で副次的に解消見込みのため対象
除外。KULR(2019)のみcurrent_liabilities/total_liabilitiesが既に同一accn
〈entity混在ではない〉から採用されているにも関わらず矛盾しており、同一
filing内でのcandidate tag誤選択が原因と確定）。登録・訂正のみ、実装は
未着手）。

最終更新: 2026-08-02（[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]の優先度を
低〜中→中に訂正。ONDS(2017)・KULR(2016)・KULR(2019)の3件とも数学的矛盾＝
実害が確定済みであり「原因未特定」は優先度を下げる理由にならないこと、また
原因が[[SPAC-SHELL-BS-ENTITY-MIXING-1]]と異なりSPAC文脈に限定されない汎用的な
抽出ロジックの欠陥である可能性があり105銘柄全体への影響範囲が未確認である点を
理由とする。更新のみ、実装は未着手）。

最終更新: 2026-08-02（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]対象銘柄にSPIR(2020)を
明示追加（同一パターンだが数学的矛盾は未顕在化の"事故的な正しさ"。対応方針の
設計・検証範囲にBBAI/RDW/RKLB/SOFI/VRTと並べて含める）。[[BS-ENTITY-MIXING-
UNEXPLAINED-ONDS-KULR-1]]を新規登録（優先度：低〜中。ONDS(2017)・KULR(2016)・
KULR(2019)でSPACシェル型と一致しないBS混在＋数学的矛盾を確認。ONDS/KULR2016は
total_assets側の値がcurrent_assetsより著しく過小、KULR2019はtotal_liabilities
とcurrent_liabilitiesの食い違いで、いずれもSPAC実体混在とは異なりtotal_assets/
total_liabilities集計タグ自体の誤選択が疑われる。原因未特定・登録のみ、
実装は未着手）。

最終更新: 2026-08-01（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]新規登録（優先度：高、
登録・調査のみ実装は未着手）＋[[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]訂正
（ELF/KULR除外・BBAI/RDWのPL/CF系は既にNone化済みと確認しクローズ扱いへ）。
[[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]個別調査で、BBAI/RDW 2020のBS系フィールドが
合併前SPACシェルと合併後本体の異なる法的実体から混在採用され、数学的に矛盾
する値（current_assets>total_assets等）が本番稼働中であることが判明。全105
銘柄横断スキャンでRKLB(2020)・SOFI(2020)・VRT(2019)にも同型の数学的矛盾を
確認、SPIR(2020)は同一パターンだが偶然矛盾していない状態を確認。ONDS(2017)・
KULR(2016/2019)は類似症状だがSPACパターンと一致せず別原因の可能性ありとして
対応方針検討の対象外に区分。BS系は期間長フィルタの対象外のため
[[PERIOD-LENGTH-VALIDATION-GAP-1]]では検知不可能だった独立した欠陥系統）。

最終更新: 2026-08-01（[[ELF-ROE10YR-RECALC-PENDING-1]]新規登録、登録のみで
TANUKI VALUATION側のコミット・反映は未実施。[[ELF-FISCAL-END-MONTH-
MISDETECTION-1]]完了時の試験実行で、ELF 2015-2018年度データ是正に伴い
ROE_avg(10yr)が7.0%→9.6%・Alpha_Premiumが0.29→0.40へ変化することを確認
〈TANUKI SCORE分類・Matrix Quadrant/Labelは不変〉。バグではなく是正済み
データに基づく期待された再計算結果のため、通常の定期更新サイクルでの
反映を待つ方針で優先度：中で登録）。

最終更新: 2026-08-01（[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②実装完了・
BACKLOG_DONE.mdへ移動。`detect_fiscal_anchor_clusters()`を新規追加し、
`determine_fiscal_year()`にextra_anchors引数を追加、SECParserの5つの
呼び出し箇所全てに配線した（コード`7c44ac266`）。全105銘柄でbucketing
比較を行い、変化があったのはELFのみ（RCAT/AVGO/MSCI/NOWは複数クラスタ
検出も実害ゼロ、単一クラスタの100銘柄は完全不変）を確認。前回除外していた
ELFのannual_2014-2019.jsonをフローズン入力で再生成し除外を解除（データ
`6d9c18b2f`）。2015-2018は真の暦年値に復旧、2014・2019（移行期）はPL/CF
系フィールドをNone化（BS項目は維持）。pytest 453 passed/2 known failed、
report_consistency_check.py NG=0（WARN=68件、変化なし）。TANUKI VALUATION
試験実行でIV/DCF/Growth_Rate（5年FCF窓）は無変化を確認したが、ROE_10yr_avg
（7.0%→9.6%）は変化することを検知（10年窓は2017-2019年度を含むため。
TANUKI SCORE分類は不変、この再生成自体は未実施・未コミット）。
ユーザー指示によりpushは保留、コミットのみ）。

最終更新: 2026-08-01（[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]新規登録、
登録のみで実装・調査は未着手。[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案②
シミュレーションの過程で、RCATの`detect_fiscal_anchor_date()`クラスタ分析が
直近10-K〈filed 2026-03-19〉を12月31日・4月30日の両クラスタに同時投票させて
いることを発見。RCATは既に決算期を2回変更済みとBACKLOG_DONE.mdに記載済みだが、
今回の重複は3段階目の移行が進行中の可能性を示唆する。現時点でbucketingへの
実害はゼロ〈月のみ比較フォールバックによる「事故的な正しさ」〉だが、将来の
データ追加で均衡が崩れるリスクがあるため優先度：中で登録）。

最終更新: 2026-07-31（[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]案①実装完了。
`detect_fiscal_end_month()`に`detect_fiscal_anchor_date()`と同一の340-380日
必須フィルタを追加し、四半期注記再掲載による得票汚染を除去（コミット
`96c42d8f0`）。全105銘柄で判定結果を新旧比較した結果、変化した銘柄は0件
（ELF/RCAT/AVGO含む全銘柄で不変）。事前見立て通り、この修正単独では
ELF（3月18票 vs 12月11票のまま）・RCAT（12月/(4,30)の食い違いのまま）・
AVGO（12月のままで真のFYE 10月末と不一致）いずれの誤判定も解消せず、
era別対応（案②）が根治に必須であることを実証的に確定した。ELFの
annual_2015〜2019.json 5ファイルは引き続き除外を維持。pytest 447 passed/
2 known failed、report_consistency_check.py NG=0（WARN=68件、変化なし）を
確認。[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]の「対応方針」を案①完了・
案②着手待ちに更新）

最終更新: 2026-07-31（[[FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1]]完了総括の
記録是正＋[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]統合タスク化、報告・登録のみ
実装は未着手）。[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]調査の過程で、
BACKLOG_DONE.mdの[[FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1]]完了総括
（「RCAT型決算期変更検知は解消済み」）が、実際の解決範囲（WARN-24による
検知・ログ記録層のみ、`_own_override_is_safe()`は無改修）より広いラベルで
表現されており、`_detect_fiscal_end_month()`/`_detect_fiscal_anchor_date()`
自体のera別対応（1銘柄が単一のfiscal_end_month/anchorしか持てないアーキ
テクチャ上の限界）は一貫して未着手のまま残っていたことが判明。BACKLOG_DONE.md
のARCH-DATA-1クローズ根拠・冒頭changelog（本ファイル114行目付近）・
「次セッションでの着手順序」欄の2026-07-17〜18付けブロックに訂正注記を追加。
[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]を、ELF単独ではなくRCAT
（2026-07-17から未着手のまま持ち越し）・AVGO（[[PERIOD-LENGTH-VALIDATION-
GAP-1]]で背景要因として既発見）を含む統合タスクとして再定義（優先度：高、
IDは変更せず内容を拡張）。

最終更新: 2026-07-31（[[PERIOD-LENGTH-VALIDATION-GAP-1]]実装完了。
`_extract_single_key()`（gross_profit等9フィールド）・`_extract_values_merged()`
（revenue/S&M/D&A）双方に340-380日の期間長フィルタを追加し、全105銘柄の
annual_YYYY.jsonをフローズン入力で再生成（コード`e3723b3eb`・データ
`d6d404016`）。実際に値が変化したのは28銘柄・194フィールドエントリで、AVGO
revenue 2016/2017の是正値($13,240M/$17,636M)は10-K原本と完全一致。
pytest 446 passed/2 known failed（既知のみ）、report_consistency_check.py
NG=0（WARN 71→68件に減少、新規WARNなし）を確認。STONKS SILOの自己修復
ロジック・TANUKI VALUATIONの直近5年窓への影響も個別確認済み（RCAT 2024の
stock_based_compensationのみ現役銘柄で該当、軽微な是正）。検証過程でELF
固有の別バグ（fiscal_end_month自動検出誤り）を発見しELF分5ファイルは
本コミットから除外、[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]として新規登録。
[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]の着手条件は充足（対応方針
決定・実装は別タスク）。詳細はBACKLOG_DONE.md「2026-07-31（完了）」参照）

最終更新: 2026-07-31（[[PERIOD-LENGTH-VALIDATION-GAP-1]]の追加シミュレーション
（`MERGE_ALL_TAGS_FIELDS`側revenue/selling_and_marketing/depreciation_and_
amortizationの3フィールド）結果を反映（登録・確認のみ、実装・データ再生成は
未実施）。OK約3,487件・b:改善13件（AVGO revenue 2016/2017の是正後値$13,240M/
$17,636Mが10-K原本値と完全一致）・c:新規欠損化12件を確認。対応スコープを
`_extract_single_key()`経由9フィールドに加えこの3フィールドにも拡大し、
tie-breakを候補単一時も含めた無条件340-380日フィルタへ変更する方針を確定。
新規発見のVRT 2016(revenue)・RCAT 2012(depreciation_and_amortization)を
[[SPAC-STUB-PERIOD-VERIFICATION-1]]に追加（9銘柄→11銘柄）。また
2026-07-12完了済み[[SEC-TAG-FICO-CPRT-1]]のFICO/CPRT/LITEについて、無条件
フィルタ適用後もregressionが発生しないことを実コード・実データで個別確認済み
（FICO全18年度・CPRT全17年度・LITE全13年度、合計48年度すべて340-380日の
範囲内で維持）。

最終更新: 2026-07-31（[[PERIOD-LENGTH-VALIDATION-GAP-1]]の全母集団オフライン
シミュレーション結果を反映（登録・訂正のみ、実装・データ再生成は未実施）。
105銘柄×9フィールドで実コード（`_detect_fiscal_end_month()`・
`_detect_fiscal_anchor_date()`・`determine_fiscal_year()`）を読み取り専用で
実行し、現状OK約9,700件・b:改善53件・c:新規欠損化138件を確認。対応方針
（`_extract_single_key()`への340-380日フィルタ追加）の安全性（既存の正しい
約9,700件には影響しない設計）を確認し、同エントリの「対応方針」を確定扱いに
更新。新規発見のMRVL(gross_profit)・COHR/INTU(cost_of_revenue、INTUは
12年連続)を影響範囲に追加。
[[SPAC-STUB-PERIOD-VERIFICATION-1]]からRCAT 2024(stock_based_compensation)
を訂正削除（「決算期変更に伴う正当なスタブ期」との推定が誤りと判明、実際は
正しい年次代替値が存在する[[PERIOD-LENGTH-VALIDATION-GAP-1]]側のb:改善
ケースだったため、対象9銘柄に変更）。

最終更新: 2026-07-31（[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]の調査から
派生した横断調査（登録のみ、実装は未着手）。新規登録6件:
[[PERIOD-LENGTH-VALIDATION-GAP-1]]〈優先度：高。parser.pyのFLOW型フィールド抽出
（`_extract_values_best_candidate()`→`_extract_single_key()`経路）に期間長検証が
構造的に欠落しており、AVGO revenue/net_income/operating_income(2016/2017)・
gross_profit9銘柄(TDY/AVGO/CPRT/ABBV/CAT/FICO/HEI/HON/KLAC)で四半期値が年次値
として誤採用されていたことを確認。2026-07-12 [[SEC-TAG-FICO-CPRT-1]]の対症療法
（revenue等3フィールド限定のtie-break追加）では根本原因が未解消だったことも確定〉・
[[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]〈優先度：高、要個別調査。BBAI/RDW/ELF/KULRで
同一年度内にフィールドごと異なる期間長が混在、predecessor/successor期間混在の疑い〉・
[[SPAC-STUB-PERIOD-VERIFICATION-1]]〈優先度：中。ASTS/IONQ/JOBY/RKLB/SOFI/SOUN/
SPIR/APGE/NOW/RCATの非365日期間データは正当なスタブ期の可能性が高く要個別確認〉・
[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]〈優先度：低〜中。MO/PM/SCCO
の年次同士の乖離は会計上の定義差の疑い〉・
[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]〈優先度：低。gross_profit
逆算ロジックの3箇所重複〉・
[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]〈優先度：低〜中。
gross_profit/cost_of_revenue整合性の常設監査項目が存在しない〉。
既存[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]の着手条件に
[[PERIOD-LENGTH-VALIDATION-GAP-1]]解消を前提として追記。次回セッション筆頭候補は
[[PERIOD-LENGTH-VALIDATION-GAP-1]]）

最終更新: 2026-07-30（common/sec_data統合フェーズA〜D準備セッション。
[[TTM-PASCALCASE-KEY-STALE-1]]〈Phase C移行によるPascalCase→snake_case
キー不一致バグ、RICEスコア100/100銘柄・FCFフォールバック94/100銘柄への
本番影響を修正〉・[[LAYER3-SGA-Q4-MISSING-1]]〈SGA/cost_of_revenueのQ4
逆算・欠落四半期逆算スコープ漏れ、42銘柄・171四半期影響を修正、
newfield_q4_cutoff_check.py新設〉・[[LAYER3-TTM-REGRESSION-NEWFIELD-
BLINDSPOT-1]]〈TTM回帰比較スクリプトの新規フィールド検証漏れ〉・
[[DOCS-SECDATA-NORMALIZED-DIR-STALE-1]]〈TANUKI TAIL/stock.htmlが参照する
docs/common/sec_data/normalized/の2ヶ月超陳腐化、週次自動同期を追加〉・
[[SEGMENT-FETCHER-DUPLICATE-ORPHAN-1]]〈segment_fetcher.py重複統合〉・
[[LAYER3-COGS-ASTS-LRCX-RECOVERABLE-FOLLOWUP-1]]〈ASTS/LRCXのcost_of_
revenue欠落を一次情報で個別裏取りし両銘柄とも回収不可能と確定、
副産物として`layer3_builder.py::_get_concept_units()`に名前空間対応
コードを追加〉・[[STONKS-SILO-COGS-DEAD-FALLBACK-1]]〈デッドな代替キー
参照削除、副次的にfalsy-zeroバグ(RXRX)も解消〉・[[JNJ-RD-TAG-PRIORITY-1]]
〈research_and_development候補タグ優先順位誤りをSEC EDGAR 10-K原本裏取り
の上で修正、adjustments.py R&D資本化調整の不適用という現在進行形の実害を
解消〉を完了。新規登録・未着手:
[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]・
[[LAYER3-COGS-STRUCTURAL-GAP-16TICKERS-1]]・
[[LAYER3-VISA-EPS-TAG-MISSING-1]]・[[LAYER3-GA-STANDALONE-TAG-UNMAPPED-1]]・
[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]〈JNJ-RD-TAG-PRIORITY-1と同一の誤りが
config/sec_concept_definitions.json側に残存〉。詳細は各エントリ・
BACKLOG_DONE.md参照。CHAT_RULES.mdへ運用原則2件を追記
〈「バグが0にならなければ次に進まない」・「新セッション開始時は渡された
資料を全文確認する」〉）

最終更新: 2026-07-23（AS-IS/TO-BE設計セッション〈RETROSPECTIVE_2026-07-22.md・
FIELD_DEFINITIONS.md全10フェーズ・CONCEPT_PARAMETER_VARIATIONS.md・
INPUT_DATA_AS_IS.md/TOBE.md〉で発見された未対応事象を一括起票。優先度高
11件〈net_cash/net_income二重計算・stock.html CapEx符号バグ・MACRO PULSE
truthy判定バグ・RECESSION RISK SCORE閾値不一致・Hollow Rally恒久不発火・
Portfolio二重保持・risk_free_rateハードコード・moat_score部分欠損・FCF
CAGR経過年数未補正・Bear/Bull符号反転〉・中19件・低9件、計39件を新規
登録。既存BACKLOG.mdとの重複は確認済みで該当なし。詳細は各エントリの
「発見」欄の根拠ドキュメント参照）

最終更新: 2026-07-22（[[FCF-DIVERGENCE-SIGN-GUARD-1]]実装完了。
divergence_ratio（estimated_fcf/raw_fcf）が符号・境界を無視することで
生じる乖離検知漏れを2段階で解消：第1段階はraw_fcf>0×estimated_fcf<0
の符号反転ガード（コミット`f6201ae04a4e242bbda2014b0f71ca2ef42911b6`）、
第2段階はraw_fcf<=0×estimated_fcf>0の対称ケース（コミット
`99014218b676fa4e36e4babefaf9ce407cac8ba4`）。いずれも既存の閾値判定
（>=2.0/>=5.0）とは独立に無条件で警告を生成する設計とし、回帰テスト
計6件・全100銘柄フローズン入力比較で既存データへの影響なしを確認済み。
FCF-CONVRATE①③（sector未収録銘柄・Damodaran NIベース設計の構造的
脆弱性）を調査し、対象53銘柄中49銘柄でPolicy Bの強制丸めが支配的で
TANUKI SCORE Classificationには無関係と判明したため、根本修正は
見送り現状維持と決定。ARCH-DATA-1をゼロベース棚卸しし、SEC正規化
3段階設計は既に全完了済み（RCAT型決算期変更検知も引き継ぎ先で解消済み）
であることを再確認するとともに、BACKLOG_DONE.md内でStage1/2/3の
完了記録が本体エントリと重複していた問題を解消（コミット
`0316b90f2badd5797a9b3409e0880dd7d98da9fc`）。CHAT_RULES.mdへ教訓3件を
追記: 独立ガード追加時の全象限（符号・境界の組み合わせ）事前洗い出し、
新規発見事象はBACKLOG.md起票を実装依頼に先行させる運用徹底、入力精度
向上に着手する前に下流の丸め・ゲート条件（Policy A/B等）への影響を
安価に確認する。）

最終更新: 2026-07-20（同日2回目: BACKLOG.md/BACKLOG_DONE.md整合性修正。
ARCH-DATA-1・FY52WEEK-BS-NULL-SILENT-1（+統合済みのFY52WEEK-BS-
INSTANT-FACT-1）の2件をクローズしBACKLOG_DONE.mdへ完全移動——ARCH-DATA-1は
3段階設計+残課題④まで全完了、唯一残っていたRCAT型決算期変更検知は
[[FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1]]へ引き継がれ完了済みと確認
（※2026-07-31追記: この完了はWARN-24による検知・ログ記録層のみを指す。
`_detect_fiscal_end_month()`等の抽出ロジック自体のera別対応は含まれておらず、
[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]調査で未解消と再確認した）。
FY52WEEK-BS-NULL-SILENT-1はPhase A・Phase B Stage1-3・Phase C全完了
（Stage2=FY52WEEK-BS-STI-OVERRIDE-DESIGN-1・Stage3=FY52WEEK-BS-
FADEOUT-FALLBACK-1、いずれも完了・分離先タスクへ相互参照済み）。
ANOMALY-PATTERN-CATALOG-1の型C実例（NVDA）・FCF-CONVRATE-DESIGN-LIMIT-1
の発見した別問題（FRSH validator誤FAIL）を、それぞれ対応完了タスクへの
参照に更新。PREVENT-5・TICKER-AUDIT-1にQUALITY-GATES-EPIC-1への統合
マッピング済みである旨を追記（二重実装防止）。DESIGN-15の着手条件
「ARCH-DATA-1完了」を充足済みとして反映。詳細はBACKLOG_DONE.md
「[ARCH-DATA-1]」「[FY52WEEK-BS-NULL-SILENT-1]」参照）

最終更新: 2026-07-13（同日2回目: セッション終了時ブラッシュアップ。
Phase 3a完了後に追加で完了した4件——TICKER-DIRECT-ACCESS-GUARD-1
（FLAG-CONSUMER-AUDIT-2/3再発防止CIガード新設・全リポジトリスキャンで
発見したtail_dcf_bridge.pyのtanukiフラグ検証漏れを同日中に修正）・
ASTS-SHARES-OSCILLATION-1（diluted_shares_used往復変動の恒久修正。
影響範囲が調査時点推定の3銘柄から新旧比較でCART/CEG/BROS/GEV/XOM/CONを
加えた9銘柄に拡大、副次発見のBROS Up-C組織再編前四半期をEPS-UPC-
PREREORG-1として分離登録）・WARN12-COHR-ONDS-1（根本原因がfact競合型
バグではなくSEC自動更新とTANUKI VALUATION再生成の生成順序のズレ〈約20
時間の陳腐化窓〉と判明、構造的ギャップをWORKFLOW-SEC-TANUKI-GAP-1として
新規登録）・HYPECORE-DASHBOARD-COUNT-BUG-1（index.htmlのticker数表示
修正、他8箇所の横展開確認で同型バグなしと確認）——を反映。前回
ブラッシュアップの教訓（「次セッションでの着手順序」欄の陳腐化）を踏まえ、
同日中の完了分もその場で同欄に反映し、次回候補をFLAG-THRESHOLD-DESIGN-1
筆頭に更新した。他の確認項目（BACKLOG_DONE.md記録の正確性・git status
のクリーン状態）はいずれも問題なし）

最終更新: 2026-07-13（セッション終了時ブラッシュアップ。ARCH-DATA-1の棚卸し
調査でQUALITY-GATES-EPIC-1のゲート1/ゲート2への統合マッピングを確認し、
Phase 3前提整理として[[ARCH-DATA-1-PREP-1]]（TAG-DEFS-UNIFY-1クローズ・
SOFI-DATA-1のLTDebt恒久修正〈2026-06-24の手動パッチが自動再生成で巻き戻って
いたことを発見・ticker_restrictionsによる恒久修正に切替〉・audit.py UP-C
検知・バグA/Bスコープ判断〈同日中に既に別コミットで解消済みと判明〉）を完了。
続けてPhase 3a（Gate2本体第一段階: `common/sec_data/contracts.py`新設。
FinancialEntry/EntryProvenance/FCFSeriesで規約A・B・③を型化し、
quarterly.py/normalizer.py/data_fetcher.pyのjson.dump()直前・fcf_list生成
箇所に検証を配線）を完了。全105銘柄で新旧比較（git stash、ネットワーク
未使用）し値の差分0件・TTMReader系メソッドの新旧比較も差分0件を確認。
pytest 302 passed/2 known failed。Phase 3b（独立実装4ファイルのreader.py
統合・規約C/Dの型化）・GATE2-READER-FCFLIST-1（reader.py::get_fcf_list()の
順序規約が未検証のまま残存）を新規登録。セッション終了時ブラッシュアップで
「次セッションでの着手順序」欄が2026-07-11以降更新されていなかった陳腐化を
発見し2026-07-12・07-13分を追記、SYSTEM_MAP.mdにcontracts.pyの記載漏れを
発見し追記、SOFI-DATA-1の旧完了エントリに巻き戻り発見の相互参照を追記。
他の確認項目（BACKLOG_DONE.md記録の正確性・git statusのクリーン状態）は
いずれも問題なし）

最終更新: 2026-07-12（同日13回目: セッション終了時ブラッシュアップ。
BACKLOG.md内QUALITY-GATES-EPIC-1エントリの陳腐化した中間ポインタ
（「次はPhase 2」、Phase 2a〜2b-3完了後も残置）を削除。
CLAUDE_CODE_START.mdに2件追記——①`if __name__ == "__main__":`ブロックを
持つスクリプト変更時はpytestに加え実機直接実行を必須化
（HYPECORE-SAVE-INDEX-NAMEERROR-1の教訓）、②cik_lookup.csvの4フラグを
参照するスクリプトの必須パターン（全銘柄一括取得は統一アクセサ経由、
CLI引数明示指定時も同フラグで検証、FLAG-CONSUMER-AUDIT-2/3の教訓）。
SYSTEM_MAP.mdの「銘柄振り分けの正本」セクションが本日の統一アクセサ導入・
CLI引数フラグ検証追加前の記述のまま陳腐化していたため全面更新
（`eps=true`が「バッチ実行に使われない」という誤記述を含む）。加えて
`extract_key_facts.py`が`common/sec_data/`ツリーの一部であるかのような
誤解を招く配置を訂正し、独立パイプラインである旨とfact選定ロジック統一
（SPLIT-AUTO-CHECK-1）を明記。他の確認項目（BACKLOG_DONE.md記録の正確性・
git statusのクリーン状態）はいずれも問題なし）

最終更新: 2026-07-12（同日12回目: HYPECORE-SAVE-INDEX-NAMEERROR-1を緊急対応
（優先度：高）で完了。`src/value/hypecore/hypecore.py::_save_tickers_index()`
の関数定義位置を`if __name__ == "__main__":`ブロックより前へ移動し、
2026-07-09 21:54以降3日間続いていたNameErrorを解消。GitHub Actionsの
"Run HypeCore Pipeline"失敗により"Commit and push"ステップがスキップされ、
週次自動更新が沈黙的に空振りしていた本番障害を解消。実機実行
（`python hypecore.py PLTR`）でNameErrorが発生せず終了コード0・
tickers.json自己再生成（updated_at最新化・103銘柄一致維持）を確認。
副次発見のdocs/index.html側の形式不一致バグを
[[HYPECORE-DASHBOARD-COUNT-BUG-1]]として新規登録。詳細はBACKLOG_DONE.md参照）

最終更新: 2026-07-12（同日11回目: HYPECORE-ZS-EPS-STALE-1完了。
実装前提の再確認で、RKLBはhypecore=true（前回セッションでの調査ミスにより
hypecore=falseと誤登録していた）と判明したためRKLB分は対応不要と判断・
訂正。ZSはeps=falseを確認済みのため`docs/value-monitor/adjusted_eps_analyzer/data/ZS/`
のみ削除。実機検証で発見した`hypecore.py::_save_tickers_index()`の既存
NameErrorバグ（`__main__`ブロック内の呼び出しが関数定義より前にあるため
常に失敗）を[[HYPECORE-SAVE-INDEX-NAMEERROR-1]]として新規登録。詳細は
BACKLOG_DONE.md参照）

最終更新: 2026-07-12（同日10回目: FLAG-CONSUMER-AUDIT-3・STALE-REPORT-CLEANUP-1
完了。hypecore.py --batch/単体指定・catalyst.py --ticker・
adjusted_eps_analyzer/pipeline.py --ticker の3箇所で、FLAG-CONSUMER-AUDIT-2と
同型（CLI引数明示指定時のフラグ検証バイパス）の構造的ギャップを発見・修正
（_filter_hypecore_tickers()・_filter_eps_tickers()を新設）。
dcf_validity_checker.pyの同型ギャップは読み取り専用診断ツールのため意図的に
未修正と判断。RKLB・ZS双方のTANUKI VALUATION残存ファイル（report.txt・
latest.json・history.json・history/）を削除（score_history.jsonは過去実績
データとして保持）。副次発見をHYPECORE-ZS-EPS-STALE-1として新規登録。
詳細はBACKLOG_DONE.md参照）

最終更新: 2026-07-12（同日9回目: QUALITY-GATES-EPIC-1 Phase 2b-3完了。
[[SPLIT-AUTO-CHECK-1]]の実害確認調査で根本原因がsplit_history.yaml未登録では
なくEPS Analyzer独自パイプライン`extract_key_facts.py`のfact選定ロジック不整合
（SEC-TAG-FICO-CPRT-1と同型のfact競合パターン）と判明し、選定ロジックを
「filed日最新優先」に統一する根本修正を実施。全105銘柄で新旧比較し11銘柄の
株数系列異常是正を確認。残存する構造的ギャップを[[SPLIT-REALTIME-GAP-1]]、
副次発見のASTS異常変動を[[ASTS-SHARES-OSCILLATION-1]]として新規登録。
詳細はBACKLOG_DONE.md参照）

最終更新: 2026-07-12（同日6回目: FLAG-CONSUMER-AUDIT-2完了。
report_consistency_check.py::run_checks()・stonks-silo/pipeline.py::run()・
score_verifier.pyの残る3消費者に統一アクセサ（tickers.get_tanuki_tickers()/
get_stonks_silo_tickers()）ベースのフラグ検証を適用し、ZS-TICKERS-LEAK-1で
発見した構造的ギャップを解消。横展開未確認事項をFLAG-CONSUMER-AUDIT-3として
新規登録。詳細はBACKLOG_DONE.md参照）

最終更新: 2026-07-12（同日5回目: QUALITY-GATES-EPIC-1のゲート1を
「取得時データ検証（検知のみ）」から「複数ソース自動照合・自動補正」に
設計修正。単一ソース依存の思考停止だったと認識し、検知止まりではなく
自動補正までをスコープに含めるようPhase 2の説明も更新。ゲート2・3は
「検知」ではなく「予防設計」（型による構造的な間違え防止）であることを
明示する注記を追加。詳細は本セクション末尾の「追記（2026-07-12 同日5回目）」参照）

最終更新: 2026-07-12（同日4回目: QUALITY-GATES-EPIC-1のPhase 1
（全テスト実行化・WARN確認済み台帳導入）が完了。CLAUDE_CODE_START.mdのStep 2を
test_pipeline_logic.py単体からtests/全体実行に変更、config/warn_acknowledged.json
新設・report_consistency_check.pyにannotate_warn()/load_warn_ledger()追加。
次はPhase 2（ゲート1: 取得時データ検証）。詳細は本セクション末尾の
「追記（2026-07-12 同日4回目）」参照）

最終更新: 2026-07-12（同日3回目: QUALITY-GATES-EPIC-1（バグ根絶に向けた
5段階品質ゲート導入）を優先度：最高で新規登録し、既存タスク（ARCH-DATA-1・
REGISTER-FLOW-REDESIGN-1・PREFLIGHT-CHECK-1・PREVENT-5・TICKER-AUDIT-1・
LLY-CAPEX-STALE-1等）をゲート0〜4配下の統合マッピングとして整理。
TEST-IV-FORMULA-ALPHA-1とTEST-STALE-IV-1の重複登録を発見しTEST-STALE-IV-1に
統合（優先度は低→中に格上げ）、TEST-IV-FORMULA-ALPHA-1は削除。
詳細は本セクション末尾の「追記（2026-07-12 同日3回目）」参照）

最終更新: 2026-07-11（セッション最終ブラッシュアップ: PREVENT-5・TICKER-AUDIT-1・
TICKER-SOURCE-UNIFY-1・REGISTER-FLOW-REDESIGN-1・PREFLIGHT-CHECK-1
（いずれも優先度：中）が「## 優先度：低」セクション配下に誤配置されていた
構造的不整合を修正し「## 優先度：中」セクション末尾へ移動。全55項目のID・
本文を保持したまま再配置したことを検証済み。銘柄リスト参照の一元化調査を
実施しTICKER-SOURCE-UNIFY-1を新規登録。registration_validator.py・
adjusted_eps_analyzer/pipeline.pyのmonitor_tickers.yaml誤参照2件を確定、
common/sec_data/tickers.pyが既存の未活用統一ユーティリティであることを特定。
REGISTER-FLOW-REDESIGN-1にP1/P4の同時導入経緯（git履歴確認）・
system_health.py日次アラート見落としを追記、TICKER-AUDIT-1・
CIK-ORPHAN-FLAGS-1・PREFLIGHT-CHECK-1に相互参照追記）

追記（2026-07-11 同日中）: TICKER-SOURCE-UNIFY-1の対応方針1・2
（adjusted_eps_analyzer/pipeline.py・registration_validator.pyの
monitor_tickers.yaml誤参照2件）をコミット`ba2cfef42`で修正・完了。
対応方針3（他呼び出し箇所のtickers.py経由統一）は未着手のため
エントリはBACKLOG.mdに残置。REGISTER-FLOW-REDESIGN-1の対応方針1も
同一修正のため完了注記を追記、CIK-ORPHAN-FLAGS-1に本修正で新規検出
されるようになったBXの追記を反映。

追記（2026-07-11 同日3回目）: TICKER-SOURCE-UNIFY-1の対応方針3
（tanuki_valuation/pipeline.py・stonks-silo/pipeline.py・
common/screening配下2スクリプトの計4ファイル）をコミット`b41b447d6`で
完了。横断調査でhypecore.pyが既に移行済みと判明したため訂正を反映
（「1箇所のみ採用」の記述を「2箇所」に修正）。対応方針1・2・3すべて完了・
残作業なしとなったが、エントリの完全クローズはKoichiさんの判断待ちのため
保留。新規発見のcommon/sec_data/config.py重複ユーティリティ問題を
TICKER-SOURCE-CONFIG-DUP-1として新規登録。

追記（2026-07-11 同日4回目）: TICKER-SOURCE-UNIFY-1は対応方針1・2・3すべて
完了・残作業なしとなったため、エントリ全文（対応方針1・2・3の完了注記・
検証結果セクションを含む）をBACKLOG.mdからBACKLOG_DONE.mdへ完全移動した。
移動に伴い、BACKLOG.md内で本エントリを参照していた他エントリ
（CIK-ORPHAN-FLAGS-1・TICKER-AUDIT-1・REGISTER-FLOW-REDESIGN-1・
PREFLIGHT-CHECK-1・TICKER-SOURCE-CONFIG-DUP-1）のリンク表記を
「[[TICKER-SOURCE-UNIFY-1]]（完了・BACKLOG_DONE.md参照）」に更新し、
リンク切れの体裁を解消した。

追記（2026-07-11 同日5回目）: BX（Blackstone Inc.）の登録抹消（コミット
`8dde36fdc`、cik_lookup.csv 1行＋関連SECデータ73件削除）をBACKLOGに反映。
[[CIK-ORPHAN-FLAGS-1]]のBX該当箇所を解消済みに更新（ENBは未解消のまま残置）、
REGISTER-FLOW-REDESIGN-1の分類記載のBXを取り消し線で解消済み表示に更新、
BACKLOG_DONE.md内のEPS-BX-1に対象消滅の追記、BX完全削除自体を新規
BACKLOG_DONE.mdエントリとして記録。TANUKI-FIN-2（JPM・GS対象）にBXの
記載はなく対応不要と確認済み。

追記（2026-07-11 同日6回目）: GROWTH-FLOOR-VERDICT-1（コミット`8df1f1172`）が
完了したため、エントリ全文（実装着手前調査・実装完了・検証結果を含む）を
BACKLOG.mdからBACKLOG_DONE.mdへ完全移動した。同じ2026-07-10格上げ組の
[[DCF-REL-SYNC-1]]に状況更新（GROWTH-FLOOR-VERDICT-1完了・本タスクは未着手のまま
残置）を追記。

追記（2026-07-11 同日7回目・セッション最終ブラッシュアップ）: [[DCF-REL-SYNC-1]]
実装検討を進め、以下を実施：①Policy Bの`transient_found`/`action`取り違えバグを
発見・分離し[[TANUKI-POLICYB-FIX-1]]として先行修正・完了（コミット`327982770`）、
②`FCFOutlierResult`に`deviation_pct`フィールドを追加しreport.txt表示に反映
（コミット`b5c91180d`。当初追加した200%安全弁は対象母集団0件と判明し削除・
シンプル化）、③調査過程で新規発見した[[FCF-OUTLIER-QUAL-1]]（一過性費用の
説明妥当性の定性評価・優先度未定）・[[SECTOR-FCF-RATE-BROKEN-1]]（FCF実力推定の
sector取得経路破損・優先度中）を新規登録。DCF-REL-SYNC-1本体は
「Policy Bのexcluded分岐の扱い」「Policy A未カバー範囲（ENTG/RMBS等）への対応」
の2点が未決着のまま次回セッション持ち越し。

追記（2026-07-11 同日8回目）: DCF-REL-SYNC-1「Policy A未カバー範囲」の調査を
進め、ENTG/RMBSはEPS Analyzerデータ未生成によるstale状態（再生成のみで解消）と
判明する一方、真に構造的な未カバー範囲（BKNG: BUY・乖離36%未説明、RBRK: 241%
乖離）を新規発見し[[POLICYB-GATE-FIX-1]]として分離・修正・完了（コミット未反映の
場合はBACKLOG_DONE.md参照）。修正過程で「floor_applied>0でもfcf_estimation.applied
=Trueなら実際のDCFはconversion-rate推定値を使う」という別の回帰リスク
（BROS/CEG/SOFI/SPIR型）も発見し同時に修正済み。全銘柄再生成・pytest 131件・
report_consistency_check NG=0を確認済み。横断調査で新たに
[[GROWTH-SANITY-CLASS-SYNC-1]]（growth_sanity.verdictとClassification未連動、
MO/LOAR/XOMのFLOOR_HIT_REVIEW）を優先度：高で新規登録。

追記（2026-07-11 同日9回目）: DCF-REL-SYNC-1の未決着点①（Policy Bの`excluded`分岐の
扱い）を再調査した結果、POLICYB-GATE-FIX-1でPolicy Bの呼び出しゲートが変わったことで
`excluded`分岐が副次的に到達可能になっていたと判明（AMZN/COHRの2銘柄で実際に機能）。
当初確定していた「デッドコードとして簡略化」の方針は撤回し現状維持に訂正した上で、
DCF-REL-SYNC-1本体を**完全クローズ**しBACKLOG.mdからBACKLOG_DONE.mdへ全文移動した
（未決着点①②とも解消済みのため）。移動に伴い、BACKLOG.md内で本エントリを参照していた
他エントリ（GROWTH-SANITY-CLASS-SYNC-1・FCF-OUTLIER-QUAL-1・SECTOR-FCF-RATE-BROKEN-1）
のリンク表記を「[[DCF-REL-SYNC-1]]（完了・BACKLOG_DONE.md参照）」に更新した。

追記（2026-07-11 同日10回目・セッション最終）: POLICYB-GATE-FIX-1の3コミットを
push（コンフリクトなし）。[[GROWTH-SANITY-CLASS-SYNC-1]]実装前調査中に
`calculate_fcf_cagr()`のCAGR計算式符号反転バグを発見し[[GROWTH-CAGR-SIGN-1]]
として分離・修正・コミット（`b09757ee5`/`41c95bf3d`）。MO/XOMのIV急変動を
一次データで追跡した結果、TTM系列構築時の四半期完全性チェック不足
（全105銘柄中94銘柄でfcf_list_rawへ不完全TTM値が混入）を発見し
[[TTM-QUARTERS-CHECK-1]]として優先度：高で新規登録。GROWTH-CAGR-SIGN-1の
全銘柄再生成は同タスクの対応方針確定まで保留。
完了済み項目は BACKLOG_DONE.md にアーカイブ

追記（2026-07-12）: [[TTM-QUARTERS-CHECK-1]]（案1・quarters_used>=4フィルタ）と
[[GROWTH-CAGR-SIGN-1]]（保留中だった全銘柄再生成）を完了し、両エントリを
BACKLOG_DONE.mdへ移動した。実装過程でCRWV/CONの計算失敗（TTM点数が年次実績
より少ないのに優先され`min_fcf_years`未満でエラー）を自己誘発・同一タスク内で
修正（`_select_fcf_source()`新設）。105銘柄フルバッチ再生成完了（成功100/
失敗0）。Classification変化14銘柄・fcf_outlier.detected変化13銘柄・
growth_sanity.verdict変化5銘柄（詳細はBACKLOG_DONE.md参照）。
[[GROWTH-SANITY-CLASS-SYNC-1]]にMOのfloor_hit再発の状況更新を追記。
副産物として発見した[[LLY-CAPEX-STALE-1]]（LLY CapEx四半期取得バグ）・
[[TEST-IV-FORMULA-ALPHA-1]]（test_iv_formula.pyのALPHA-REDESIGN-1後未更新、
MSFT/NVDA既存2件失敗）を優先度：中で新規登録。

追記（2026-07-12 同日2回目）: [[GROWTH-SANITY-CLASS-SYNC-1]]の設計を再検討し、
「verdictをClassificationに丸めて反映する」単発対応は不採用と判断。
信頼性が崩れうる段階を段階0（データ完全性）・段階1（成長率算出）・
段階2（FCF/DCF計算）の3段階に整理した上で、各段階の「信頼できない」事象を
可視化前に「解消可能（バグ）」と「構造的に解消不能」へ切り分ける方針を
新たに追加し、[[TRUST-SUMMARY-EPIC-1]]として優先度：高で新規登録した
（実装は未着手、次回セッションで設計方針を固めてから着手）。
[[GROWTH-SANITY-CLASS-SYNC-1]]は本EPICの段階1担当として位置づけを更新。

追記（2026-07-12 同日3回目）: セッション振り返り議論で、過去1ヶ月の主要バグが
共通して「発見手段が別作業中の偶然」に依存し機械的ゲートが存在しないことが
根本原因と判明したため、[[QUALITY-GATES-EPIC-1]]（バグ根絶に向けた5段階品質
ゲート：ゲート0登録適格性・ゲート1取得時データ検証・ゲート2正規化契約・
ゲート3計算式検証・ゲート4出力整合＋回帰）を優先度：最高で新規登録した。
[[ARCH-DATA-1]]・[[REGISTER-FLOW-REDESIGN-1]]・[[PREFLIGHT-CHECK-1]]・
[[PREVENT-5]]・[[TICKER-AUDIT-1]]・[[LLY-CAPEX-STALE-1]]等の既存タスクを
ゲート0〜4配下の統合マッピングとして整理し、[[TRUST-SUMMARY-EPIC-1]]は
本EPIC完了後の再評価対象（Phase 5）と位置づけた。

同日中に[[TEST-IV-FORMULA-ALPHA-1]]（本日新規登録）と[[TEST-STALE-IV-1]]
（2026-07-02発見・先行登録済み）が同一バグの重複登録であることが判明したため、
先行するTEST-STALE-IV-1を正式エントリとして残し優先度を低→中に格上げ、
TEST-IV-FORMULA-ALPHA-1は削除した。

追記（2026-07-12 同日4回目）: [[QUALITY-GATES-EPIC-1]]のPhase 1
（即時・低コスト施策）が完了した。CLAUDE_CODE_START.mdのStep 2・
「よく使うコマンド」内pytest実行の2箇所をtest_pipeline_logic.py単体から
tests/全体実行に変更（既知例外[[TEST-STALE-IV-1]]のMSFT/NVDAを明記、
全体実行で新規失敗なしを確認：204 passed/2 known failed）。
report_consistency_check.pyにWARN確認済み台帳機能を追加し、
`config/warn_acknowledged.json`に既知WARN3件（ELF WARN-10、MO/XOM WARN-20）を
事前登録。未登録WARNは`[🆕未確認 WARN-N ...]`と強調表示されるようになった
（既存の非ブロッキング動作は維持）。単体テスト10件追加、全件パス。
次はPhase 2（ゲート1）に進む。

追記（2026-07-12 同日5回目）: セッション振り返りの議論で、[[QUALITY-GATES-EPIC-1]]
ゲート1の設計思想に問題があったことが判明した。「外部データは不確実だから
検知しかできない」という前提は、SEC EDGARという単一ソースへの依存を
無自覚に前提していた思考停止であり、独立した複数ソース（yfinance等）と
機械的に突合すれば、多くのケースは「検知して人間に投げる」のではなく
「取り込む前に自動で弾く・補正する」構造にできると整理し直した。ゲート1を
「取得時データ検証（検知のみ）」から「複数ソース自動照合・自動補正」に
名称・内容とも修正し、タグ取得ミス（KLAC/FICO/CPRT型）・同一値の使い回し
（LLY型CapEx欠損）・株式分割見逃しの3パターンを自動補正対象として明記。
Phase 2の説明も「取得時検証6項目の実装」から「複数ソース自動照合・
自動補正の実装（検知止まりではなく自動補正までをスコープに含める）」に
更新した。

あわせて、ゲート2（正規化契約）・ゲート3（計算式検証）は外部データの
不確実性への対処ではなく自分たちのコード内の規約違反を対象とする
「予防設計」（型による構造的な間違え防止）であり、実行時の検知を行う
ゲート1とは性質が異なることを明示する注記を追加した。

---

## 📌 このバックログの読み方（2026-06-19 統合で追加）

前回までのバックログは個別バグ・個別画面の課題を1件1項目で並列管理しており、
94件超まで肥大化していた。分析の結果、その多くが少数の**横断的パターン**に
起因することが判明したため、今回以下の方針で再構成した。

1. **「個別画面の表示崩れ」90件以上 → 6つの横断課題（EPIC）に統合**
   個別チケットは各EPICの「対象一覧」に格下げし、EPIC単位で一括対応する。
   1件ずつ直すと作業コストが線形に積み上がるが、共通コンポーネント化すれば
   1回の実装で全画面に波及する。
2. **「個別バグ」は引き続き個別管理**（データ不整合・計算ロジック誤り等、
   汎用化できない性質のもの）
3. **アーキテクチャ課題（ARCH-DATA-1 / BUG-SCORE-SYNC-1根本解決）を「高」に格上げ**
   個別バグの多くがこの2つに起因しており、先送りするほど利息が複利で積み上がる
   技術的負債である。詳細は下部「開発方針メモ」参照。

---

## 優先度：最高（構造的負債・着手で多くの後続課題が消える）

### [QUALITY-GATES-EPIC-1] バグ根絶に向けた5段階品質ゲートの導入
**優先度:** 最高
**分類:** アーキテクチャ / 品質管理 / マスタープランEPIC
**登録日:** 2026-07-12
**発見:** セッション振り返り議論（バックログ増加原因の分析）

#### 背景
過去1ヶ月の主要バグ（KLAC/FICO/CPRT/LLY/NOW/TTM系・CAGR符号反転等）を
横断すると、共通して「発見手段が別作業中の偶然」に依存しており、
機械的に検知・ブロックするゲートが存在しないことが根本原因と判明した。
1件の修正が新たな発見を誘発する連鎖（TTM-QUARTERS-CHECK-1で3件誘発、
DCF-REL-SYNC-1で複数件誘発等）もこの構造に起因する。

対症療法（発見された個別バグを都度直す）を続ける限りバグ発見ペースは
落ちないため、データの上流（取得）→中流（正規化）→計算（式の正しさ）→
下流（出力整合）の各段階に機械的なゲートを設け、多くのバグをそもそも
本番に到達させない構造に転換する。

#### ゲート構成
**ゲート0（登録適格性）**: 米国上場・10-K/10-Q提出企業のみを機械判定で
通す。submissions APIで提出フォーム種別（20-F/40-F即NG）・国・上場年数・
収益タグ存在を確認。除外時は`exclusion_reason`をcik_lookup.csvに必須記録し
「フラグfalse＝経緯不明」を根絶する。

**ゲート1（複数ソース自動照合・自動補正）**: 単一ソース（SEC EDGAR）の値を
無条件に正としない。「外部データは不確実だから検知しかできない」という
前提は、SEC EDGARという単一ソースへの依存を無自覚に前提していた思考停止
であり、独立した複数ソースと機械的に突合すれば、多くのケースは「検知して
人間に投げる」のではなく「取り込む前に自動で弾く・補正する」構造にできる：
- **タグ取得ミス（KLAC/FICO/CPRT型）**: yfinance数値・前年/前々年からの
  連続性・同業他社水準と突合し、乖離が閾値超過なら該当タグ値を不採用とし、
  暫定値または前期繰越等の安全な代替に自動フォールバックする
  （人間確認待ちにしない）
- **同一値の使い回し（LLY型CapEx欠損）**: 「同じ値がN期連続」という
  時系列パターン自体を機械検知し、取得失敗とみなして別経路
  （yfinance cashflow等）へ自動フォールバックする
- **株式分割見逃し**: yfinance splits履歴という独立ソースと突合し、
  SEC側の対応漏れがあっても自動補正する
- **主要数値の前年比急変**: 上記の自動照合で説明できない急変のみ、
  従来通り理由確認までブロックする（機械的に解消できないものだけを
  人間の確認対象に絞り込む）
- 決算期末日の同一as-of日整合チェック・10-K/A等訂正データの優先取得・
  四半期充足数チェック（TTM-QUARTERS-CHECK-1の考え方をFCF以外の全系列に
  一般化）は既存方針を維持

##### 付録: 元[PREVENT-5]（2026-09-05統合、要約なしの全文転記）
下記は独立BACKLOGエントリだった`[PREVENT-5] 定期横断調査スクリプトの
整備`の全文をそのまま転記したもの（[[QUALITY-GATES-EPIC-1]]統合前の
「ゲート1（PREVENT-5）」というマッピング記載はあったが具体的提案内容
（cross_check.pyのチェック項目案等）自体は転記されていなかったため、
2026-09-05に本節へ追記した）。見出しレベルのみ本エントリの階層に
合わせて`####`→`######`に2段階下げてあり（本付録自体の見出しが
`#####`のため）、文言・内容は無変更。

**優先度:** 中
**分類:** 再発防止 / 品質管理

**QUALITY-GATES-EPIC-1への統合について**: 本タスクは
[[QUALITY-GATES-EPIC-1]]のゲート1（PREVENT-5）/ゲート4
（TICKER-AUDIT-1）に統合マッピング済み。個別に着手する前に
QUALITY-GATES-EPIC-1のPhase 3/4の進行状況を確認し、二重実装を
避けること。

###### 背景
今回のような横断調査を毎回手動で実施するのはコストが高い。
system_health.pyでカバーできない観点（表示ロジック・用語統一・
フィールド定義整合性等）については、定期的な手動調査が必要。

###### 優先度見直し理由（2026-07-09）
現状、データ形起因バグの発見手段が「新規銘柄登録・決算更新時に
たまたま発火する」という受動的トリガーに限られている。
2026-07-09のASTS/LLYの期末日不整合バグ（バグB）は、
XBRL-TAG-KLAC-1-FOLLOWUP検証がなければ発見されず、既存102銘柄の
IVが過大評価されたまま放置されていた可能性がある。
ARCH-DATA-1の着手条件（次にデータ形起因バグが発生した時点で着手）は
維持するが、そのバグを発見する手段自体を能動化する必要があるため、
横断監査スクリプト整備の優先度を引き上げる。

###### 対応内容
以下を整備する：
- 横断調査用のチェックスクリプト（cross_check.py）を新規作成
  - cik_lookup.csv vs 全configの整合性
  - glossary.jsonのdata-info属性カバレッジ
  - console.log残存チェック
  - フィールド名の表記ゆれ検出
- 月次メンテナンスタスクとしてCLAUDE_CODE_START.mdに追記

**ゲート2（正規化契約）・ゲート3（計算式検証）の位置づけ**: ゲート2・3は
外部データの不確実性への対処ではなく、自分たちのコード内の規約違反
（GROWTH-CAGR-SIGN-1のfcf_list並び順取り違え等）を対象とする「**予防設計**」
である。docstringの規約ではなく型（dataclass等）で構造的に間違えられなく
することが目的であり、実行時の検知ではなくコード自体が誤りを許容しない
設計を指す。ゲート1（実行時の外部データ検証・自動補正）とは性質が異なる。

**ゲート2（正規化契約）**: 全計算ロジックは正規化済みJSONのみを読む。
各フィールドに出所・充足度メタデータを必須付与。規約（fcf_listの並び順等）は
docstringではなく型（dataclass等）でコード化し、構造的に間違えられなくする。

**ゲート3（計算式検証）**: 全計算式に「ゴールデンテスト（教科書的定義との
手計算突合）」と「性質テスト（単調性等の性質検証）」を1式1件以上必須にする。
同一概念の計算が2箇所以上に重複実装される状態自体をNG検知する。

**ゲート4（出力整合＋回帰）**: report_consistency_checkのWARN放置を廃止し
「確認済み」台帳管理に変更（未確認WARNは次回実行でNG化）。CI相当の実行対象を
全テストファイルにする。フルバッチ再生成時のClassification差分レポートを
標準出力にする。

##### 付録: 元[TICKER-AUDIT-1]（2026-09-05統合、要約なしの全文転記）
下記は独立BACKLOGエントリだった`[TICKER-AUDIT-1] 銘柄棚卸しスクリプト`の
全文をそのまま転記したもの（[[QUALITY-GATES-EPIC-1]]統合前の
「ゲート4（TICKER-AUDIT-1）」というマッピング記載はあったが、P4-
CIKOrphan WARN見落とし経緯・monitor_tickers.yaml同期漏れの過去
インシデント等の具体的内容自体は転記されていなかったため、2026-09-05に
本節へ追記した）。見出しレベルのみ本エントリの階層に合わせて
`####`→`######`に2段階下げてあり（本付録自体の見出しが`#####`のため）、
文言・内容は無変更。

**優先度:** 中
**分類:** 再発防止 / 品質管理
**登録日:** 2026-07-02

**QUALITY-GATES-EPIC-1への統合について**: 本タスクは
[[QUALITY-GATES-EPIC-1]]のゲート1（PREVENT-5）/ゲート4
（TICKER-AUDIT-1）に統合マッピング済み。個別に着手する前に
QUALITY-GATES-EPIC-1のPhase 3/4の進行状況を確認し、二重実装を
避けること。

###### 背景
テスト目的等での銘柄追加が本番パイプラインに紛れ込み、野放図に増加する問題への対処。
cik_lookup.csvへのstatus/registration_source/registration_note列追加を前提に、
定期的な棚卸しを自動化する。

###### 前提条件
cik_lookup.csvへのstatus/registered_date/registration_source/registration_note列追加、
完了済み（commit 337bf3d29。既存97銘柄はstatus=active/registration_source=unknownで
バックフィル済み。CLAUDE_CODE_START.mdのStep 0.5として新規登録手順にも組み込み済み）

###### 想定機能
① status=test かつ registered_date が一定期間（閾値は要検討、例：30日）より古い銘柄を
   「見直し候補」として一覧化
② registration_source=moomoo_screening等の検証由来かつポジションなしの銘柄を抽出
③ system_health.pyの拡張として実装するか、独立スクリプトにするか要検討
④ 判断（retired化等）は自動化せず、候補出しまでに留める。最終判断はKoichi自身が行う。
⑤ `registration_validator.py`のP4-CIKOrphanチェック相当（全フラグfalseかつ
   status=activeの孤立エントリ検出）を定期棚卸しレポートに明示的に集約する
   （下記「P4-CIKOrphan WARN見落とし問題」参照）
⑥ monitor_tickers.yamlとcik_lookup.csvの件数差・銘柄差分の検出も棚卸し対象条件に含める
   （下記「monitor_tickers.yaml同期漏れ」参照）

###### P4-CIKOrphan WARN見落とし問題（2026-07-10発見・2026-07-11追記）
`registration_validator.py`のP4-CIKOrphanチェックは、全フラグfalseかつactiveな
孤立エントリ（BX・ENB）を以前から検出していたが、WARNは非ブロッキングのため
運用上見落とされていた。[[CIK-ORPHAN-FLAGS-1]]（2026-07-10登録）は実質この
見落としの再発見だった。TICKER-AUDIT-1実装時は、WARNレベルの検出結果であっても
定期的に人の目に触れる仕組み（棚卸しレポートへの明示的な集約等）にすること。

###### monitor_tickers.yaml同期漏れ（2026-07-10発見・2026-07-11修正済み）
SYSTEM_MAP.md実態調査（2026-07-10）で、cik_lookup.csv（正本・106件）に対し
monitor_tickers.yaml（99件）が6件未反映（RMBS/ENTG/TER/KLAC/LRCX/APGE、
いずれもStep 7の同期漏れ）だったことが判明し、2026-07-11に手動追加で修正済み
（BXのみ全フラグfalseで除外が正当なため対象外のまま）。cik_lookup.csvと
monitor_tickers.yamlの同期は自動化されておらず、新規登録手順Step 7の手動実施のみに
依存しているため、TICKER-AUDIT-1実装時は両ファイルの差分検出も棚卸し対象に含めること。

###### 銘柄リスト正本参照の一元化との関係（2026-07-11追記・同日完了済み）
本タスクが検出すべき「同期漏れ」の根本原因は、[[TICKER-SOURCE-UNIFY-1]]
（完了・BACKLOG_DONE.md参照）で確定した「本来cik_lookup.csvを参照すべき箇所が
monitor_tickers.yamlを参照している」同型バグ（registration_validator.py・
adjusted_eps_analyzer/pipeline.py）にある。TICKER-AUDIT-1は「症状の棚卸し」、
TICKER-SOURCE-UNIFY-1は「原因の是正」という役割分担だったが、
TICKER-SOURCE-UNIFY-1は対応方針1・2・3すべて2026-07-11中に完了したため、
本タスクが検出すべき同期漏れ自体の新規発生は構造的に減っている。

###### 着手条件
当面は運用でカバー可能。銘柄数がさらに増えた場合に着手。

##### ゲート4実装（2026-09-05）
上記想定機能①〜⑥を`common/system_health.py::check_k_ticker_audit()`
として実装した（既存の`check_X_name() -> tuple[str, bool, str]`パターン
に準拠、`main()`の`check_a`〜`check_j`と並列に呼び出しK列として集約）。

- **①**: status=candidate（現行cik_lookup.csvに`status=test`は存在せず、
  candidateが唯一のtest相当値。想定機能①原案の「test相当」を実データに
  合わせて修正）かつ登録から30日超の銘柄を一覧化
- **②**: registration_source=technical_screening（想定機能②原案の
  「moomoo_screening」は現行データに存在しない値のため、実際の列挙値
  technical_screeningに修正）かつ`docs/portfolio/data/portfolio.json`に
  保有記載のない銘柄を抽出
- **③**: `common/system_health.py`への追加関数として実装（決定済み事項の実装）
- **④**: 判断は自動化せず、レポート出力のみ（status変更・retired化等は
  一切行わない）。テストで書き込み副作用がないことも確認済み
- **⑤**: `registration_validator.py::check_p4_orphan_configs()`を再利用し、
  戻り値からカテゴリ`P4-CIKOrphan`の警告のみを抽出して他の想定機能と
  同じ目立つ形式（`⑤P4-CIKOrphan`プレフィックス）で出力（独自の
  再実装はせず、既存の唯一の実装を再利用）
- **⑥**: `monitor_tickers.yaml`と`cik_lookup.csv`の銘柄集合差分
  （どちらか片方にのみ存在する銘柄）を検出

`common/sec_data/tickers.py`に新規関数`get_all_rows()`
（cik_lookup.csvの全行を辞書リストで返す）を追加し、①②の実装で
status/registered_date/registration_source列を横断参照する際も
cik_lookup.csvを独自にcsv.DictReaderで再パースしない設計とした
（[[TICKER-LOADING-UNIFICATION-1]]の方針を踏襲）。

##### 検証結果（2026-09-05）
- 現行103銘柄で実行し、想定通りWST/APGE/CON/SNの4銘柄が①②双方で
  検知され（いずれも2026-07-02登録のtechnical_screening・candidate・
  無保有）、⑤⑥は該当なしと確認（`python common/system_health.py`の
  実行結果で確認）
- ⑤⑥については意図的にテスト用の孤立エントリ・同期漏れをmonkeypatchで
  模した16ケースの単体テスト（`tests/test_system_health_ticker_audit.py`）
  で正しく検知することを確認（本番データは変更していない）
- 既存check_a〜jへの影響がないことを既存テスト
  （`tests/test_system_health_workflow_monitor.py`）全件パスで確認
- `python -m pytest -q`: 1118件全パス（新規19件含む）
- `python common/sec_data/audit.py`: 既存警告9件のみ（変化なし）
- `python common/sec_data/report_consistency_check.py --fail-on-ng`:
  NG=0 / WARN=93件（ベースラインと同数）

#### 既存タスクの位置づけ（統合マッピング）
以下は個別タスクとして独立進行させず、本EPIC配下のPhase実装時に吸収する：
- ゲート0: REGISTER-FLOW-REDESIGN-1の対応方針2〜4（完了・
  BACKLOG_DONE.md参照。2026-09-06クローズ）、
  [[PREFLIGHT-CHECK-1]]（完了・BACKLOG_DONE.md参照。2026-09-05、
  `register_ticker.py::register_one()`への組み込みで実装完了）
- ゲート1: [[ARCH-DATA-1]]のaudit.py拡張項目、PREVENT-5（2026-09-05に
  独立エントリを削除し、上記「ゲート構成」内の付録として全文統合済み）。
  [[LLY-CAPEX-STALE-1]]（完了・BACKLOG_DONE.md参照）はPhase 2aで
  「フォールバック選定ロジックの最新end日優先化」として一般化実装済み
- ゲート2: [[ARCH-DATA-1]]本体（正規化レイヤー強化）
- ゲート3: 新規（計算式ゴールデンテスト整備は現状ほぼ手つかず）
- ゲート4: TICKER-AUDIT-1のWARN集約構想（2026-09-05に独立エントリを
  削除し、上記「ゲート構成」内の付録として全文統合済み）。**2026-09-05、
  想定機能①〜⑥を`system_health.py::check_k_ticker_audit()`として実装
  完了**（詳細は付録「ゲート4実装（2026-09-05）」参照）
- 上記いずれでも解消しない構造的限界の可視化は[[TRUST-SUMMARY-EPIC-1]]に
  引き続き委ねる（本EPIC完了後に再評価）

#### 着手順序（Phase）
1. **Phase 1（即時・低コスト）**: BACKLOG重複統合
   （[[TEST-IV-FORMULA-ALPHA-1]]・[[TEST-STALE-IV-1]]の統合）、
   CLAUDE_CODE_START.md Step 2を全テストファイル実行に変更、
   WARN台帳方式の導入
2. **Phase 2（ゲート1）**: 複数ソース自動照合・自動補正の実装（過去の
   KLAC/FICO/CPRT/LLY型バグを発生前に自動無害化することが目標。検知止まり
   ではなく自動補正までをスコープに含める）。投資対効果最大と判断
   （過去1ヶ月の主要バグの大半がここで止まっていたはず）
3. **Phase 3（ゲート0＋2）**: 登録適格性の機械化・正規化契約の整備
4. **Phase 4（ゲート3）**: 全計算式のゴールデンテスト・性質テスト整備
5. **Phase 5**: [[TRUST-SUMMARY-EPIC-1]]（可視化）を、上記ゲートで拾いきれない
   構造的限界に対象を絞って再評価

**Phase 1完了（2026-07-12）**:
- CLAUDE_CODE_START.mdのStep 2・「よく使うコマンド」内pytest実行の2箇所を
  test_pipeline_logic.py単体からtests/全体実行に変更。既知例外
  （[[TEST-STALE-IV-1]]のMSFT/NVDA）を明記。
- 全テスト実行結果: 204 passed / 2 known failed（新規失敗なし）
- `config/warn_acknowledged.json`を新設し、report_consistency_check.pyに
  `load_warn_ledger()`/`annotate_warn()`を追加。未登録WARNは
  `[🆕未確認 WARN-N ...]`と強調表示、既存の非ブロッキング動作は維持。
  既知3件（ELF WARN-10、MO/XOM WARN-20）を確認済みとして事前登録。
- 単体テスト10件追加（tests/test_report_consistency_check.py）、全件パス
- 検証: pytest 204 passed/2 known failed、report_consistency_check.py NG=0/
  警告3件（確認済み3・未確認0）

**ゲート1設計修正（2026-07-12 同日5回目）**: 「取得時データ検証（検知のみ）」
から「複数ソース自動照合・自動補正」に設計を修正した。詳細は上記
「ゲート構成」の該当箇所を参照。

**Phase 2a完了（2026-07-12 同日6回目）**: Step 0調査（同日中）で、
LLY型（タグ切替見逃し）とKLAC型（期間分類ミスによるノイズタグ混入）が
「候補タグ群の中で最初に条件を満たしたものを採用して打ち切る」という
同一のフォールバック選定ロジックに起因すると判明したため、選定ロジックを
「最小件数を満たす候補の中から最新end日が最も新しいものを採用する」方式へ
転換した（[[LLY-CAPEX-STALE-1]]（完了・BACKLOG_DONE.md参照）として着手）。
- `common/sec_data/tag_definitions.py`を新設し、quarterly.py（四半期/TTM側）と
  parser.py（年次側）で独立管理されていたタグ候補リストのうち、優先順位・
  候補集合が完全一致または一方が他方の厳密な上位集合になっている9概念
  （CapEx・FinanceLeasePmts・SBC・GrossProfit・NetIncome・Cash・RD・Buyback・OCF）
  を統合。LTDebt・SM・DA・RPO・Revenueは優先順位・候補集合が構造的に異なり
  （既存の修正済みバグ・設計判断と衝突するリスクがあるため）意図的に統合対象外とし、
  [[TAG-DEFS-UNIFY-1]]として別タスクに切り出した。
- `quarterly.py::_select_best_candidate()`・`parser.py::_extract_values_best_candidate()`
  を新設し、候補タグを全て評価した上で最小件数を満たすものの中から最新end日優先で
  採用する方式に変更（parser.py側はuse_max/merge_all_tags使用フィールド
  ＜Revenue・selling_and_marketing・depreciation_and_amortization・SharesBasic/Diluted等＞
  は従来ロジックを完全に維持し、変更対象外とした）。
- 影響範囲確認: 同日生成のcompany_facts.jsonを用いて新旧ロジックを直接比較した結果
  （raw/*.jsonの生成日時差による見かけ上の差分を排除するため、旧コードをgit stash
  で一時退避し同一日に再生成して比較）、105銘柄中**LLY（CapEx: 4件→19件、
  最新end日2022-09-30→2026-03-31）とWMT（SBC: 0件→6件、副次的に発見。WMTは
  ShareBasedCompensationタグを一度も申告せずAllocatedShareBasedCompensationExpense
  のみで申告しており、旧ロジックの厳密な四半期件数改善要求により従来フォールバックが
  発動しなかった）の2銘柄のみ**に影響が限定されることを確認済み。他103銘柄は無変化。
- 検証: `update.py LLY WMT`→`audit.py`（LLY正常、WMT既存の軽微なWARN「OCF一部None」
  のみ・私の変更とは無関係）→`pipeline.py --skip-risk LLY WMT`（2/2成功）→
  `report_consistency_check.py`（NG=0、既存WARN3件のみ・LLY/WMTともにWARN対象外）→
  pytest 214 passed/2 known failed（新規8件追加、既存2件のみ既知失敗）。
- 単体テスト`tests/test_tag_fallback_selection.py`を新規追加（8件、全件パス）。

**Phase 2b-1完了（2026-07-12 同日7回目）**: ゲート1「同一値使い回し」パターンの
一般化として、TTM鮮度チェックを新設した（段差型検知の統合・株式分割自動照合は
別依頼、Phase 2b-2以降）。
- `src/value/tanuki_valuation/data_fetcher.py`に`_quarters_fresh()`・
  `_freshest_end()`を新設。`_quarters_complete()`（quarters_used>=4の件数チェック）
  とは別の姉妹関数とし、既存関数自体は変更していない。
- 閾値根拠: 105銘柄のTTM系列でttm_end(最新)と実行日の差を実測した結果、
  正常銘柄は44〜113日に収まっていた（四半期決算の通常の報告ラグ）ため、
  その3倍弱にあたる270日を閾値に設定（詳細はコード内コメント参照）。
- **実装前の分布確認で重大な設計変更が必要と判明**: TTM系列は各エントリが
  約1年間隔で過去5年分の実データとして保存される設計のため、依頼文通り
  「件数チェックと並列に各エントリごとに鮮度を適用」すると、series[0]
  （最新）以外の全エントリが構造的に365日超で陳腐化扱いされ、全105銘柄で
  `get_fcf_series()`が1点以下（=None、年次フォールバックに全件落ちる）になる
  ことが判明した。ユーザー確認の上、**鮮度チェックはシリーズ中の最新エントリ
  （`_freshest_end()`でソート順に依存せず判定）のみに適用し、最新エントリが
  陳腐化していればシリーズ全体を不採用とする**方式に設計変更した（過去年度の
  正規の履歴エントリは引き続き信頼する）。
- `TanukiDataFetcher.get_financials()`内の3呼び出し元
  （`TTMReader.get_fcf_series()`・`get_periods()`・`build_rice_annual_shape()`）
  すべてに同一の判定を適用。
- 影響範囲確認: 同日生成データで新旧を直接比較した結果、**105銘柄中0銘柄が
  影響**（現時点で全銘柄のfreshest_endが270日以内に収まっているため）。
  データ再生成（update.py/pipeline.py）は不要と判断し実施していない。
  LLY CapEx（Phase 2a修正後）が引き続き正しく採用されることも個別確認済み。
- 検証: `report_consistency_check.py`（NG=0、既存WARN3件のみ）→
  pytest 223 passed/2 known failed（新規5件`tests/test_pipeline_logic.py`に
  追加、既存の`TestTTMReaderQuartersCompleteness`等8件はdate.today()依存の
  陳腐化を防ぐため絶対日付から相対日付ベースに書き換え）。

**Phase 2b-2完了（2026-07-12 同日8回目）**: 段差型の前年比急変検知として、
既存の`common/screening/dcf_validity_checker.py::check_c_data_jump()`
（手動実行専用スクリプト内、Revenue専用・直近6年の隣接年比2.0倍以上/0.5倍以下）を
`report_consistency_check.py`へ統合した（関数自体は改変せず、呼び出し元を
追加するのみ）。株式分割自動照合は引き続き未着手（[[SPLIT-AUTO-CHECK-1]]
（完了・BACKLOG_DONE.md参照）参照）。
- `common/sec_data/report_consistency_check.py`が`common.screening.dcf_validity_checker`を
  importするため、`registration_validator.py`と同一の`sys.path.insert(0, REPO_ROOT)`
  パターンを追加。
- NG-11（孤立年検知：前後両年とも閾値未満のスパイク型）との役割分担を明記:
  新設チェックは前後判定を要さず、ジャンプ後も高い水準が継続する**段差型**
  （FICO/CPRT/LITE型）を検知する。両者は検知パターンが異なるため併存させた。
- **重要度をNGからWARNへ変更（依頼書の想定から判断変更）**: 全105銘柄で試験実行した
  結果、19銘柄（ALAB・ASTS・AVAV・BBAI・CELH・CRWV・IONQ・JOBY・KULR・LITE・NVDA・
  ONDS・QBTS・RCAT・RDW・RKLB・RXRX・S・TDY）が新規該当。うちNVDA（AI GPU需要による
  実際の売上急成長$26.9B→$130.5B）・JOBY（プレコマーシャル航空機企業のほぼゼロからの
  売上立ち上がり）等を一次情報（annual_YYYY.json）で確認した結果、いずれもタグ取得
  ミスではなく実際の事業成長だった。check_c_data_jump()の2.0倍/0.5倍閾値は元々
  「人間が目視で選別する前提のフラグ付けツール」向けの設計でNG（ブロッキング）には
  誤検知率が高すぎると判断し、ユーザー確認の上でWARN-21として実装した。
- 19銘柄全件を一次情報で個別確認し（IPO直後の急成長・AI需要急増・M&A〈TDY=FLIR
  Systems買収・AVAV=BlueHalo買収〉・プレコマーシャル企業の立ち上がり・LITE=既知の
  会計年度末変更、いずれもタグ誤りなし）、`config/warn_acknowledged.json`に
  確認済みとして登録済み。
- 検証: `report_consistency_check.py --fail-on-ng`でNG=0・exit 0（警告38件、
  確認済み38・未確認0）を確認。pytest 226 passed/2 known failed
  （新規`tests/test_report_consistency_check.py`に3件追加）。

**Phase 2b-3完了（2026-07-12 同日9回目）**: [[SPLIT-AUTO-CHECK-1]]の実害確認調査で、
根本原因はsplit_history.yaml未登録ではなく、EPS Analyzer独自の抽出パイプライン
`extract_key_facts.py`のfact選定ロジック不整合（Q1〜Q3は末尾勝ち・Q4は先頭勝ちで
filed日を見ていなかった。SEC-TAG-FICO-CPRT-1と同型のfact競合パターン）と判明。
`extract_key_facts.py`のみを対象にfiled日最新優先へ統一する根本修正を実施し、
split_history.yamlへの個別登録（対症療法）は行わなかった。全105銘柄で新旧比較し、
NVDA・AVGO・TSLA・LRCX・CPRT・CELH・KULR・RCAT・SPIR・WMT・SCCO（新規発見）の
株数系列異常が是正されたことを確認。ただしfact自体が1件も存在しない期間
（分割直後〜翌年10-K再掲まで）は原理的に是正不能な残存ギャップがあり、
[[SPLIT-REALTIME-GAP-1]]として切り出した。詳細はBACKLOG_DONE.md参照。

次はPhase 3（ゲート0＋2: 登録適格性の機械化・正規化契約の整備）。

**Phase 3前提整理完了（2026-07-13）**: Gate2本体（正規化契約の構造化）着手前の
小粒4項目（[[ARCH-DATA-1]]棚卸しで発見・[[ARCH-DATA-1-PREP-1]]として実施、
完了・BACKLOG_DONE.md参照）を完了した。TAG-DEFS-UNIFY-1のクローズ判断・
normalized JSON不足フィールド補完（SOFI-DATA-1のLTDebt恒久修正）・
audit.py UP-C検知・バグA/Bのスコープ判断（既に解消済みと判明）のいずれも
Gate2の設計自体を左右する新規発見はなく、Phase 3設計セッションの着手条件が
整った。Gate2本体（型によるフィールド規約のコード化・出所/充足度メタデータ
付与）は未着手のまま。

**Phase 3a完了（2026-07-13）**: Gate2本体の第一段階として、JSON on-disk形式を
変えずに読み込み直後・書き込み直前でバリデーションする薄い型層
`common/sec_data/contracts.py` を新設した。対象は規約A（fcf_listの新しい順規約）・
規約B（quarterly.py標準エントリ形状）・規約③（出所・充足度メタデータ）の3点。
規約C（フィールド分類の二重管理）・規約D（enum風文字列の型化）と、4ファイル
（financial_trend_calculator.py・quarterly_review_generator.py・tail_dcf_bridge.py・
hypecore.py）のreader.py経由統合はPhase 3bとして分離登録（[[GATE2-PHASE3B-1]]参照）。

- `FinancialEntry`/`EntryProvenance`: quarterly.py::save_raw_table()・
  normalizer.py::save_normalized()のjson.dump()直前に検証を追加（検証のみ、
  保存対象データ自体は変更しないためJSON on-disk形式は不変）。あわせて
  `_select_best_candidate()`のフォールバック採用時・SOFI等のticker_restrictions
  オーバーライド採用時に`_provenance.source_tag`を付与するよう配線し、
  「なぜこの値が採用されたか」を出力JSONから追跡可能にした（全105銘柄で
  114件のフィールド×銘柄組み合わせに付与されることを確認）。SOFI-DATA-1の
  ような手動パッチが自動再生成で静かに巻き戻る事態の再発防止に直結する。
- `FCFSeries`: data_fetcher.py::TTMReader.get_fcf_series()内でのみ使用し、
  新しい順（降順）規約をconstruction時に検証する。JSONシリアライズ不可能な
  ため呼び出し境界で`.as_list()`により素のlist[float]へ変換して返す（戻り値の
  型・下流5+消費者への影響は変えない設計判断）。
- 検証: 全105銘柄の既存company_facts.jsonを用い、新旧コード（git stash）で
  build_raw_table()/normalize()の出力を`_provenance`除外で直接比較した結果、
  **値の差分は0件**（純粋な追加的変更であることを確認）。pytest 302 passed/
  2 known failed（既存4テストファイルのうち、正規表現importの都合で
  test_normalizer.pyのimport文をパッケージ経由に変更、_select_best_candidate()の
  戻り値がタプル化したことに伴いtest_tag_fallback_selection.pyの3箇所を
  タプルアンパックに変更、test_pipeline_logic.pyの1テストを新しい順序規約に
  合わせて仕様変更〈混在順序のTTM seriesを与えた場合、旧実装は誤った順序の
  まま`get_fcf_series()`が返していたが、新実装は安全側でNoneを返すよう変更〉）。
  新規`tests/test_contracts.py`を追加（26件、GROWTH-CAGR-SIGN-1相当の順序違反を
  意図的に発生させ`ContractViolation`が送出されることを確認するテストを含む）。
  `report_consistency_check.py` NG=0。

**規約A（fcf_list順序）の限界（次回セッションへの申し送り）**: `FCFSeries`は
「construction時に渡されたデータの順序」を検証するものであり、GROWTH-CAGR-SIGN-1の
実際のバグ（`calculate_fcf_cagr()`内部で`start_value`/`end_value`という変数への
割り当てを取り違えた、渡されたデータ自体は正しい順序だった）を直接再現・検知する
ものではない。`.newest`/`.oldest`という named accessor の提供によって「正しい
使い方を選びやすくする」ことが主眼であり、growth.py側がこれらのaccessorを実際に
採用するかはGate3（計算式検証）の範疇として別途判断が必要（今回は未着手）。

**規約A（reader.py::get_fcf_list()）の未カバー範囲**: 年次ベースのfcf_list
（`reader.py::get_fcf_list()`が生成、TTM系列が使えない銘柄のフォールバック経路）は
生成時点で既に日付情報が失われているため、`FCFSeries`の順序検証を今回は
適用できていない。reader.pyはPhase 3aの対象ファイル外（当初スコープの
normalizer.py/quarterly.py/data_fetcher.pyに含まれない）のため、対応要否は
次回セッションで判断する（[[GATE2-READER-FCFLIST-1]]として新規登録）。

#### 着手条件（2026-08-19再訂正）
なし。Phase 1・Phase 2a・Phase 2b-1・Phase 2b-2・Phase 2b-3・Phase 3前提整理・
Phase 3aは完了。Phase 3b（[[GATE2-PHASE3B-1]]、4ファイル統合・規約C/D）も
2026-07-17〜18に①②③-a③-b全項目完了し、BACKLOG_DONE.mdへ全文移動済み。
**Phase 4（ゲート3）の優先順位は2026-08-19に再訂正**——本線3の
`operating_income`第一歩完了後の実測（`[[LAYER3-ANNUAL-CLASSIFICATION-
DROPS-DATA-1]]`・`[[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]`の発見）を
踏まえ、Phase 4棚卸しはLayer3範囲実測の次点に位置づけ直した。優先順位の
全体像・判断基準（真値一致ではなく思想↔式↔データの整合性で評価する）は
`CHAT_RULES.md`「本線の定義」の「次の本線候補」参照（重複記載を避けここ
では詳細を繰り返さない）。
（2026-07-22訂正）。

**上記の個々のPhase完了記録は事実（コミットは存在する）だが、「これにより
Phase 3までの目的が達成された」という従来の含意は誤りだったと2026-08-18の
実コード確認で判明したため、着手条件を以下の通り訂正する。**

**未着手として残るのはPhase 4・5だけではない**:
- **ゲート0（登録適格性の機械化）は2026-08-18時点で未着手だったが、
  2026-09-03にREGISTER-FLOW-REDESIGN-1（完了・BACKLOG_DONE.md参照）
  方針1〜5が全件完了・実質
  達成した（exclusion_reason列・provisioning状態・オーケストレーション
  スクリプト等）**。「Phase 3（ゲート0＋2）完了」と記載していたが、
  実際に完了したのはゲート2（`contracts.py`、Phase 3a/3b）のみで、
  ゲート0側は登録時点での機械的ブロック（submissions API照会等）が
  上記5方針の対応範囲外のため依然未着手のまま。詳細は下記「ゲート0の
  実装状況（2026-08-18訂正、2026-09-03追記）」参照
- **ゲート1（Phase 2a〜2b-3）は「複数ソース自動照合・自動補正」という
  設計変更（2026-07-12同日5回目）の適用範囲が限定的なまま**。詳細は下記
  「ゲート1の実装状況（2026-08-18訂正）」参照。次に着手すべきは
  Phase 4（ゲート3）ではなく、**Phase 2（ゲート1）の適用範囲拡大**と
  判断する（Phase 4は「予防設計」であり、`[[OPERATING-INCOME-
  EXTRACTION-GAP-1]]`（2026-08-16）が実害を出したデータ取得層とは
  性質が異なる層のため）
- Phase 4（ゲート3: 全計算式のゴールデンテスト・性質テスト整備、現状ほぼ
  手つかず）・Phase 5（[[TRUST-SUMMARY-EPIC-1]]の再評価）が未着手である
  こと自体は従来の記載通り

ARCH-DATA-1のスコープ拡張（2026-07-16、年次データ正規化3段階設計）
とは対象領域が重複しないため、並行して進めて支障ない。

#### ゲート1の実装状況（2026-08-18訂正）
ゲート1は2026-07-12同日5回目の追記で「取得時データ検証（検知のみ）」から
「複数ソース自動照合・自動補正」へ設計変更された。Phase 2a〜2b-3の完了
記録（上記）はそれぞれ事実だが、この設計変更の核心である**外部ソース
（yfinance）との突合**は、Phase 2a〜2b-3のいずれにも含まれていなかった
ことが2026-08-18の実コード確認で判明した：

| Phase | 実装内容 | 外部ソース突合 |
|---|---|---|
| 2a | SEC内部の候補タグ群から「最新end日優先」で選ぶロジック改善（`tag_definitions.py`） | なし |
| 2b-1 | TTM系列の鮮度チェック（内部の日付比較） | なし |
| 2b-2 | 前年比2.0倍/0.5倍の統計的閾値検知（WARN-21） | なし |
| 2b-3（[[SPLIT-AUTO-CHECK-1]]） | EPS Analyzer独自パイプラインのfact選定ロジック不整合の修正（BACKLOG_DONE.mdの完了記録で確認） | なし（yfinance splits履歴との突合は未実施） |

Phase 2a〜2b-3で変更対象だったファイル（`tag_definitions.py`・
`data_fetcher.py`・`quarterly.py`・`parser.py`・`extract_key_facts.py`）
に`yfinance`の参照は存在しない（2026-08-18確認）。

**ただし「外部ソース突合の仕組み自体が存在しない」わけではない**（当初
そう誤認しかけたが訂正）。`common/sec_data/`配下の診断・監査層には既に
以下が稼働している：
- `report_consistency_check.py` WARN-10: yfinanceのPS比率と自社計算のPS
  比率を突合（2.5倍超/0.4倍未満で警告）。**Gate1設計変更（2026-07-12
  同日5回目）より前から存在する既存機能**であり、Phase 2a〜2b-3の成果物
  ではない
- `audit.py`: `beta_config`とyfinance実測βの乖離チェック
- `registration_validator.py`: P1-Step2-Betaで「raw yfinance値使用中」を
  WARN表示

**したがってゲート1の未達は「仕組みの不在」ではなく「適用範囲の限定」**:
外部照合が適用されているのはPS比率・βという派生的・参考的な指標のみで、
**営業利益・売上・純利益といった損益計算書の中核項目には適用されて
いない**。`[[OPERATING-INCOME-EXTRACTION-GAP-1]]`（2026-08-16）で6銘柄の
営業利益がゼロ扱いされていた問題は、この適用範囲の偏りが直接の原因。
yfinanceはこれら4銘柄（LLY/JNJ/XOM/KLAC）の営業利益を問題なく返すことを
実測確認済み（2026-08-18、`yfinance.Ticker(t).income_stmt`で全件取得成功）。

**着手コストへの影響**: ゲート1の残作業は新規機構の開発ではなく、**既存
のWARN-10と同型のパターンを主要財務数値へ横展開する作業**である。した
がって当初見積もりより着手コストは小さくなる可能性が高い。主な作業は
実装そのものではなく、フィールドごとの閾値の実データ検証（Phase 2b-2で
NG想定がWARNへ格下げになった経緯を踏まえ、誤検知率の確認が必要）。

**本線3・ゲート1第一歩完了（2026-08-19）**: `operating_income`単体を
対象にyfinance自動照合を実装した（既存CHECK-35の拡張、新規CHECK番号
なし）。

- 実装: `common/sec_data/report_consistency_check.py::
  _check_operating_income_reconstruction()`にNone/derived判定済みの
  銘柄のみを対象とした`_get_yf_operating_income()`を追加し、WARN文言に
  yfinance実測値・乖離率を含めるよう拡張。`common.yfinance_utils.
  safe_yf_ticker()`（既存の安全呼び出しラッパー）経由で取得し、
  取得失敗時は照合をスキップする
- 対象範囲の限定によりレート制限リスクを抑制: 全105銘柄ではなく、
  CHECK-35が既にNone/derivedと判定した銘柄（2026-08-19時点で7銘柄
  ——LLY/JNJ/XOM/KLAC/ASTS/COHR/SOFI）のみyfinance呼び出しが発生する
  設計
- **既存パターンからの逸脱とその理由**: WARN-10（PS比率）・audit.pyの
  β照合が使う`common.market_data.reader.get_attributes()`ローカル
  キャッシュにはoperating_income相当のフィールドが存在せず踏襲不能
  だったため、単一ティッカーの直接取得に適した別の既存パターン
  （`common.yfinance_utils.safe_yf_ticker()`）を採用した
- **乖離率によるNG格上げは行わない**: 全105銘柄の乖離率分布を実測した
  結果、標準タグ採用済みの「正常」銘柄でも中央値0.2%な一方でp95=81%・
  最大342%（AVAV）まで裾が広く、Phase 2b-2と同型の誤検知リスクがある
  ため、乖離率は情報提供のみに留めWARNのまま据え置いた
- **新たな発見（当初）**: 再構成6銘柄のうちASTS・JNJはyfinanceとの
  乖離0.0%で再構成の正しさを裏付けたが、COHRは-89.6%
  （reconstructed_pretax $94.2M vs yfinance $901.5M）と大きく乖離して
  おり、再構成値自体の妥当性に疑問符が残ることが判明した
- 検証: 6対象銘柄全件でWARN文言に乖離率が正しく出ることを確認。
  残り99銘柄で意図しないWARN新規発火なし（トリガー条件自体は変更して
  いないため設計上当然）。pytest 781 passed/2 known-failed（既存の
  MSFT/NVDA）、`report_consistency_check.py --fail-on-ng` NG=0/
  WARN=88件、`audit.py` exit 0

**期ズレバグの発見・フォールバック向き反転（2026-08-19、同日追加）**:
実装翌日の検証で、上記COHR -89.6%という数値自体が`_get_yf_operating_
income()`の期ズレバグ（`row.iloc[0]`が決算期12月以外の銘柄でSECデータ
より1期先の予備的な値を指す）に起因すると判明し、期末日ベースの照合
（±10日許容窓）に修正した。正しく照合した結果、COHRの真の乖離は
-82.4%であり、これは「2手法不一致時にpretax法へフォールバックする」
という旧設計自体が誤りだったことを示していた（GP法が算出可能な4銘柄
全てでyfinanceと0.0%完全一致、フォールバック発動2銘柄はpretax採用値が
-11.4%/-82.4%も乖離）。フォールバック向きを反転（案A、GP法優先）し、
VRT FY2018の異常値対策として入力整合性ガード（案D）を追加実装した。
詳細・全検証結果は`[[OPERATING-INCOME-EXTRACTION-GAP-1]]`
（BACKLOG_DONE.md「2026-08-19（完了）」参照）。

#### ゲート0の実装状況（2026-08-18訂正、2026-09-03追記）
- `exclusion_reason`は`src/value/tanuki_valuation/calculator/
  adjustments.py`（3件）・`pipeline.py`（1件）に存在するが、これは
  RPO/Revenue比率調整で「なぜ調整を適用しなかったか」を記録するDCF計算
  内部のフィールドであり、**ゲート0が求める`cik_lookup.csv`の銘柄登録
  除外理由列とは無関係の同名フィールド**
- ~~`cik_lookup.csv`向けの`exclusion_reason`列は存在しない
  （2026-08-18確認）~~ **2026-09-03解消**: REGISTER-FLOW-REDESIGN-1
  （完了・BACKLOG_DONE.md参照）方針4実装（コミット`f0c4b18e09`）により
  `cik_lookup.csv`
  に`exclusion_reason`列を新設し、RKLB/ZS/SN/APGEの4銘柄に除外理由を
  記入した。実装過程でENB（カナダのIFRS/40-F提出企業、`[[CIK-ORPHAN-
  FLAGS-1]]`参照）の孤立登録も解消（登録抹消、コミット`62aa662102`）
- `audit.py`のカナダ企業（IFRS/40-F）検知は登録済み銘柄への事後WARNで
  あり、ゲート0が求める「登録時点でのsubmissions API照会による機械的
  ブロック」ではない（この項目は未着手のまま）
- ~~`[[REGISTER-FLOW-REDESIGN-1]]`（ゲート0のマッピング元）の対応方針
  5件のうち完了は方針1・4の2件のみで、方針2・3・5は引き続き未着手~~
  **2026-09-03解消**: 同日中に方針2（`status=provisioning`、
  `_INVALID_STATUSES`拡張＋`registration_validator.py --promote`昇格
  ロジック）・方針3（`common/registration/register_ticker.py`
  オーケストレーションスクリプト新設）を実装し、方針5（単一エントリ
  ポイント化）も方針3自体が実質達成（運用規律による担保、技術的な
  強制はなし）。**5件の対応方針は全件完了または実質達成**となった
  （詳細は`[[REGISTER-FLOW-REDESIGN-1]]`「方針2・3実装（2026-09-03）」
  参照）。~~ただし「登録時点でのsubmissions API照会による機械的
  ブロック」（上記`audit.py`の限界の裏返し）自体は方針5件の対応範囲
  外のため、ゲート0が完全に満たされたとまでは言えない~~
  **2026-09-05解消**: `[[PREFLIGHT-CHECK-1]]`（想定機能①〜④）を
  `common/registration/preflight_check.py`として実装し、
  `register_ticker.py::register_one()`のStep 0.5直後・Step 1実行前に
  組み込んだ。①上場後3年未満（submissions API照会）・②直近提出書類が
  10-K/10-Q以外・③収益系XBRLタグ不在、をいずれも登録時点で機械的に
  検知・警告表示する（自動停止はしない設計）。これにより「登録時点での
  submissions API照会による機械的ブロック（＝警告）」という、ゲート0の
  最後の未達成部分が実装された。詳細は`[[PREFLIGHT-CHECK-1]]`
  （完了・BACKLOG_DONE.md参照）

#### CHECK-32〜36の位置づけ（2026-08-18追記）
2026-08-15〜16に新設したCHECK-32〜36が、本EPICの5ゲート構造の**どこにも
正式にマッピングされていない**ことを記録する。

| CHECK | 内容 | 対応ゲート |
|---|---|---|
| CHECK-32 | discover_config/theme_config同期チェック | ゲート4寄りだが元々のスコープ外（config二重管理検知） |
| CHECK-33 | fcf_conversion_config.json専用実装（→CHECK-34へ統合） | 同上 |
| CHECK-34 | config読み込み失敗の横断検知（レジストリテーブル方式） | 当初の5ゲートに定義されていない新種 |
| CHECK-35 | operating_income再構成使用・取得不可の検知（WARN） | 2026-08-18時点ではゲート1に最も近いが検知止まり（自動照合・自動補正ではない） |
| CHECK-36 | moat_score中立フォールバックの検知（WARN） | ゲート4寄り（出口側の検知） |

実態は「`report_consistency_check.py`のWARN機構を土台に、その都度発見した
個別の脆弱箇所を検知専用で塞いだパッチ群」であり、**いずれも「検知して
WARN表示する」段階に留まり、自動補正まで到達しているものは1つもない**
——これはEPIC自身が2026-07-12に「検知止まりでは不十分」と自己批判した、
まさにその手前の水準である。本EPICを「部分的に実施していた」とは言えない。
今後、これらをゲート構造に正式に位置づけるか独立した仕組みとして扱うかは
未決（着手時に判断する）。

**（2026-08-19追記）** CHECK-35はその後、本線3の第一歩として
`operating_income`のyfinance自動照合（実測値・乖離率のWARN文言への
追記）を獲得し、上表時点（自動照合ゼロ）から前進した——ただし自動
「補正」（値の書き換え）までは依然到達していない点は変わらない。
CHECK-32〜34・CHECK-36は本追記時点でも検知止まりのまま。詳細は
`[[OPERATING-INCOME-EXTRACTION-GAP-1]]`（BACKLOG_DONE.md
「2026-08-19（完了）」）参照。

#### Phase 4（ゲート3）対象棚卸し（2026-08-20④、調査のみ・実装せず）

**Step 0: 前提の再検証**

Step 0-1（原文引用）: ゲート3は上記「#### ゲート構成」に以下の通り
定義されている（原文）:

> **ゲート3（計算式検証）**: 全計算式に「ゴールデンテスト（教科書的
> 定義との手計算突合）」と「性質テスト（単調性等の性質検証）」を
> 1式1件以上必須にする。同一概念の計算が2箇所以上に重複実装される
> 状態自体をNG検知する。

Step 0-2（ゲート0〜2の実測確認、既存記述の再検証）: 本エントリ内の
「ゲート0の実装状況（2026-08-18訂正）」「ゲート1の実装状況
（2026-08-18訂正）」節は実コード確認済みで、`cik_lookup.csv`に
`exclusion_reason`列が無いこと（ゲート0未着手）・`tag_definitions.py`
等5ファイルに`yfinance`参照が無いこと（ゲート1がPS比率・β・
operating_income以外の中核PL項目に未適用）は今回grepで再確認し、
記述と実態は一致していた。ゲート2（`contracts.py`378行・
`test_contracts.py`）も実在を確認済み。**この3ゲートについては
記述と実態の食い違いは見つからなかった**。

**「該当ファイル0件」の精査（重要な補足）**: 今回の調査依頼文にある
「現時点で該当ファイルは0件」は、**Gate3として明示的に設計・命名
された専用ファイルという意味では正しい**が、以下2点の精査により
補足が必要と判明した:

1. `tests/test_iv_formula.py`（73行）は、IV per share算出式
   （`v0_rm×(1+alpha)+rpo_pv+go_pv`を`diluted_shares`で割り
   `net_cash_per_share`を足す）を、既に計算済みの構成要素から
   再計算し、保存済み`intrinsic_value_per_share`と1セント未満の
   誤差で一致するかを検証する。これはゲート3が求める「ゴールデン
   テスト」に近い性質を持つが、対象は`MSFT/NVDA/CELH/PLTR/TSLA`の
   **5銘柄ハードコード**のみで、「教科書的定義との手計算突合」
   というより「本番が使う中間値の再結合が自己無矛盾か」の確認に
   近く、性質テスト（単調性等）は含まない。全計算式を対象とする
   ゲート3の要求水準には遠く及ばないが、「0件」ではなく**「1件、
   ただし狭い範囲・自己整合性検証のみ」**が正確な現状
2. `src/value/tanuki_valuation/validator.py::run_basic_checks()`
   （238-372行目）は、**pytestテストではなく本番パイプライン内の
   実行時検証**として、4つの決定論的チェックを既に持っている:
   `pt_shares_consistency`（test_iv_formula.pyとほぼ同じ式の再計算・
   誤差1%未満）・`dcf_components`（2段階/3段階DCFの構成要素合計と
   V0の突合）・`formula_verification`（α = ROE×0.6/Rm×0.7を独立
   再計算し実際の値と突合、誤差0.01未満——これは「教科書的定義
   （α公式）との手計算突合」そのもの）・`anomaly_detection`
   （乖離率>1000%・株式数<100万・IV>$50000・IV<=0の性質的異常値
   検出、ゲート3の「性質テスト」に相当）。`pipeline.py`が全銘柄
   再生成時に呼び出し、結果は`validation`フィールドとして
   `latest.json`に保存され、`stock.html:838-840`が
   `validation.overall`をユーザーに表示する

**したがって「Gate3が対象とする種類の検証機構」自体は、TANUKI
VALUATIONのDCF計算について既に相当程度存在する。** ただし
**`report_consistency_check.py`・`audit.py`・pytestのいずれも
`validation.overall`を一切参照していない**（grep実測: 3ファイルとも
0件）ことを確認した。`validation.overall`が`FAIL`の銘柄があっても、
個別の`stock.html`を開かない限り誰も気づけない——これは本Epicが
ゲート1で問題視した「検知はあるが集約されず沈黙する」構造
（CHECK-32〜36の位置づけと同型）が、実はTANUKI VALUATIONの中核DCF
検証そのものにも既に存在していたことを意味する。

この精査結果は「棚卸しを続けるかどうかを左右する食い違い」
（Step 0-3）には該当しないと判断した——ゲート3が「ほぼ手つかず」
という大枠の記述は維持されるが（全計算式を体系的にカバーする設計は
存在しない）、**ゼロから作るのではなく既存資産（validator.py・
test_iv_formula.py）の接続・拡張を優先すべき**という、棚卸しの結論に
直結する重要な発見のため、Step 1以降にそのまま反映する。

**Step 1: 計算思想の洗い出し**

`NAMING_CONVENTIONS.md`・`FIELD_DEFINITIONS.md`・`SYSTEM_MAP.md`・
`CLAUDE_CODE_START.md`から、計算に関する設計思想・原則を抽出した。

| # | 思想（原文引用・要約） | 実装箇所 | 機械検査の有無 |
|---|---|---|---|
| 1 | 規則1: データソースが異なる場合は接尾辞で区別（`_sec`/`_yf`/`_fred`/`_local`） | `net_cash_sec`等、一部フィールドのみ | **なし**（新規フィールド追加時のチェックリスト運用のみ、機械化されていない） |
| 2 | 規則2: 期間・時間軸を明示（`_yoy`/`_ttm`/`_fy`/`_cagrNy`） | `rule40_yoy_netmargin`/`rule40_cagr3y_opmargin`等 | **なし** |
| 3 | 規則3: 変数名は実際の計算内容と一致させる（誤称禁止） | HypeCore`op_margin`→`net_margin`改名（2026-08-13、`[[RULE40-DEFINITION-MISMATCH-1]]`）等、個別修正実績あり | **なし**（自然言語理解が必要なため一般的な機械検査は困難、後述） |
| 4 | 規則4: 生データ直接転記フィールドはprovenance明示必須 | `contracts.py::FinancialEntry/EntryProvenance`（Gate2 Phase3a、`json.dump()`直前で検証） | **あり**（`test_contracts.py`、pytest） |
| 5 | 規則5: 「唯一の正」が定まった概念はパススルー先も同名を使う | `upside_percent`（AS-IS-006、「統一クラスタの唯一の正」と明記） | **なし** |
| 6 | ゲート2/3位置づけ: 「規約はdocstringでなく型で構造的に間違えられなくする」 | `FCFSeries`（新しい順規約をconstruction時に検証） | **あり**（Gate2 Phase3aの範囲のみ。`growth.py`が`.newest`/`.oldest`アクセサを実際に採用しているかは**エピック自身が「Gate3の範疇、今回は未着手」と明記**——2131-2141行目参照） |
| 7 | 「データの信頼性を判定結果に混ぜず明確にフラグを立てる」（`FIELD_DEFINITIONS.md`1080-1082行目） | `dcf_reliability`/`validation.overall`等の各種`_reliability`/`_verdict`フィールド分離設計 | **一部あり**（フラグ自体は分離されているが、フラグ間の整合〈次項〉は未検査） |
| 8 | 「本番コードが実際に使う解決ロジックそのものを呼び出す」（`CLAUDE_CODE_START.md`確立した設計原則、CHECK-32〜34共通） | 各種CHECK関数の設計方針 | **設計方針として運用中**（Gate3実装時もこの原則に従うべき） |

**Step 1-3（暗黙の前提の発掘）**: 上記に加え、コードには存在するが
規則として文書化されていない前提を発見した。

- **`validator.py::run_basic_checks()`の4チェックそのもの**が、
  文書化された「ゲート3」の定義に先行して2026年前半（v6.2の履歴が
  示す通りEpic登録より前）から本番稼働していた。ゲート3という
  概念が2026-07-12に言語化される前に、既にその実体の一部が
  別の目的（Grok API検証の代替）で実装されていた——つまり
  「思想が後から明文化された」のではなく「実装が先にあり、後から
  策定された思想がそれを追認できていない」逆方向のギャップ
- **`_provenance`という同一の語で2つの異なる機構が併存**
  （`SYSTEM_MAP.md`1018-1025行目）: `contracts.py::EntryProvenance`
  （エントリ単位、quarterly.py/normalizer.py向け）と、
  `parser.py`の独自provenance（`{bs,pl,cf,shares,other}_provenance`、
  フィールド単位）は設計思想・対象データ・粒度が異なる別物だが、
  同名のため混同しうる。規則3（誤称）そのものではないが同種の
  「命名と実体の不一致」リスクであり、機械検査なし
- **falsy-zeroパターン**（`CLAUDE_CODE_START.md`
  `[[FALSY-ZERO-PATTERN-SWEEP-1]]`）: 「0値と未取得(None)を区別する」
  という前提は、複数の個別バグ修正（本セッション以前だけで5例）を
  経て初めて認識された暗黙の前提であり、いまだ横断的なルール文書化
  もなければ機械検査もない（優先度中、着手条件なしのまま滞留）
- **FIELD_DEFINITIONS.mdが記録した既知の未解決乖離が複数存在**
  （詳細はStep 2参照）。これらは「暗黙の前提」というより「文書化は
  されたが機械検査に繋がっていない既知のバグ予備軍」であり、
  ゲート3が本来検知すべき状態そのものが既に実例として存在している

**Step 2: 既存の検査層とのギャップ**

検査されている思想（表内「機械検査の有無」参照）: 規則4（provenance、
Gate2/`test_contracts.py`）のみが明確に機械検査下にある。

検査されていない思想のうち、**機械的に検査可能なもの**:
- 規則2（期間接尾辞の命名規約）: 新規フィールド追加時に
  `_yoy`/`_ttm`/`_fy`/`_cagrNy`のいずれかを含むかを正規表現で検査
  可能（ただし「命名規約違反」の検知であり「計算式の正しさ」の
  検証ではない点に注意）
- 規則6（Gate2アクセサの未使用確認、`growth.py`）: `.newest`/
  `.oldest`アクセサの呼び出し有無をAST解析で機械検査可能
- **`validation.overall`の未接続**（Step 0-2で発見）: 既に生成されて
  いる値を読むだけなので機械検査は容易（新規ロジック不要）
- **`FIELD_DEFINITIONS.md`が記録した閾値二重管理の実例群**
  （下記）のうち、Python側だけで完結するもの（例:
  STONKS SILO AS-IS-141の`_deficit_verdict()`閾値〈65/35〉と
  `pillarColor()`閾値〈70/45〉——両方ともコード内の定数なので
  数値抽出・突合が可能）

検査されていない思想のうち、**機械的に検査困難/不可能なもの**:
- **規則3（誤称禁止）そのもの**: 変数名が示す「意味」と計算式が
  示す「意味」の一致は自然言語理解を要し、汎用的な機械検査は
  基本的に不可能。限定的な対処として、既知のキーワード（地域名・
  セグメント名等）との突合による部分的検査は可能——これは
  `[[TAIL-LAYER3-ROUTING-DIMENSION-BLIND-1]]`の追加調査
  （2026-08-20③、TANUKI TAILのKPI名照合）で実際に採用した手法だが、
  「語彙リストの網羅性に依存し新種の誤称は検知できない」という
  限界がある（同エントリのStep 3参照）。TANUKI VALUATION側の
  規則3違反を横断的に機械検査する仕組みは現状なく、実装するとしても
  同様の限界を抱える
- **規則1・規則5**: 「データソースの系統」「唯一の正の参照元」は
  コード内の関数呼び出し関係の意味論的な理解を要し、AST解析だけでは
  「正しいソースを参照しているか」までは判定できない（呼び出しの
  有無は検査できても、その呼び出しが概念的に正しいかは別問題）
- **JS側（フロントエンド）にハードコードされた閾値との突合**
  （AS-IS-214/232のMACRO PULSEフェーズ境界「25」vs実閾値「30」等）:
  Python側のCHECK機構はPythonコード・JSONデータのみを対象としており、
  `.html`内の埋め込みJS定数を機械的に抽出・突合する仕組みは存在
  しない。正規表現による簡易抽出は技術的に可能だが、対象範囲の
  特定（どの定数がどの計算式に対応するかの対応付け）は人手の判断を
  要するため、完全自動化は困難と判断する（**判断がつかない**——
  対応付けの機械化コストを見積もるには個別ケースの詳細調査が必要）

**FIELD_DEFINITIONS.mdに記録済みの未解決の閾値/定義乖離（実例、
現在いずれも無検査）**:

| 対象 | 内容 | 出典 |
|---|---|---|
| STONKS SILO DeficitQuality | `_deficit_verdict()`閾値(65/35) vs `pillarColor()`閾値(70/45)が不一致 | FIELD_DEFINITIONS.md AS-IS-141、1117行目 |
| MACRO PULSE RECESSION RISK SCORE | ゲージバー・チャート背景・AI解説プロンプトの3箇所で境界「25」使用、実閾値は「30」（AS-IS-213）。AIが誤った境界を前提に解説文を生成しうる | 同AS-IS-214/232、1390/1408/1507行目「最重要」 |
| MACRO PULSEスコア計算 | 現在時点用（ステップ関数）と過去日付用（lerp補間）で異なる閾値・カーブの2実装が併存。意図的設計だが表示上どこにも明記なし | 同1506行目 |
| 10Y-2Yスプレッド判定 | 用途により3種類の閾値セットが存在、うち1種類のみ異なる | 同1511行目 |
| HypeCore substage | 「別々に実装され、条件・閾値が異なる」推定表示との矛盾 | 同637-649行目 |
| DCF_Reliability Policy A/B | 「非LOW」表示語彙が不一致 | 同1158行目 |

これら6件は**Gate3が実装されていれば検知できたはずの、実在する
現行の乖離**であり、「機械検査可能」列に該当するものから優先着手
候補とする根拠になる。

**Step 3: ゲート3の範囲の提案（優先順位付き、実装しない）**

1. **【最優先・最小コスト】`validation.overall`の
   `report_consistency_check.py`への接続**: 既存CHECK-35と同型の
   新規CHECK（例CHECK-40）で、全銘柄`latest.json`の
   `validation.overall`が`FAIL`/`WARN`の場合にWARN表示するだけ。
   新規計算ロジックは不要、既存の`validate_calculation()`出力を
   読むのみ。ゲート3が既に持つ検証能力を「検知して終わり」ではなく
   「集約して可視化する」段階へ進める、投資対効果最大の一歩
2. **【低コスト】`test_iv_formula.py`の対象を全銘柄へ拡張**:
   現在5銘柄ハードコードの`TICKERS`定数を
   `common/sec_data/tickers.py::get_tanuki_tickers()`経由の動的
   リストに変更するだけ。既存パターンの横展開
3. **【中コスト】`growth.py`のFCFSeriesアクセサ採用確認**
   （Gate2が自ら積み残した宿題、`[[GATE2-...]]`関連）: AST解析で
   `.newest`/`.oldest`呼び出しの有無を検査。GROWTH-CAGR-SIGN-1
   という実害バグへの直接対応であり優先度は中
4. **【中コスト・要個別判断】FIELD_DEFINITIONS.md記録済み6件の
   個別修正**: 新規の検査機構を作るより先に、既に発見済みの実例
   （特にMACRO PULSEフェーズ境界の3箇所不一致、AI生成コンテンツに
   影響するため実害が大きい）を直接修正する方が費用対効果が高い
   可能性がある。ゲート3という「仕組み」の整備よりも「実例の解消」
   を先にすべきかはユーザー判断
5. **【低優先・高コスト】JS埋め込み閾値とPython計算式の横断突合
   機構の新設**: 技術的難度が高く（対応付けの自動化が困難）、
   4の個別対応で十分な可能性がある。全部作る前提としない
6. **【機械化困難につき対象外】規則3（誤称禁止）の汎用機械検査**:
   自然言語理解を要するため一般解は無い。個別ケースでの名称照合
   （TAILで採用した手法と同型）は可能だが網羅性に限界があるため、
   横断的な仕組みとしての新設は見送りが妥当

**着手条件（2026-08-20再訂正）**: 上記1（`validation.overall`の
`report_consistency_check.py`への接続）は2026-08-20⑤に実装完了。
上記2（`test_iv_formula.py`の対象拡大）は2026-08-20⑥に
`[[TEST-STALE-IV-1]]`修正の一環として完了（5→100銘柄）。3〜6は
未実装のまま。着手要否・優先順位はユーザー判断とする。

#### Phase 4 追加調査（2026-08-20⑤）: validation.overallの実測＋検知への接続

上記「Step 0-2」で「`validation.overall`が集約されず沈黙している」
と発見した状態について、まず実測してから検知機構（CHECK-40）に
接続した。

**Step 1: 全銘柄実測**（`common.sec_data.tickers.get_tanuki_tickers()`
＝本番の銘柄一覧関数、100銘柄全件に`validation`フィールド・
`latest.json`とも存在）:

| overall | 件数 |
|---|---|
| PASS | 67 |
| WARN | 32 |
| FAIL | 1 |

**FAIL銘柄**: `LYFT`（1件のみ）。**WARN銘柄**（32件）:
`AMAT/BSY/CAKE/KO/LLY/LMT/VRT/ADBE/BKNG/AVGO/ADSK/CDNS/INTU/HON/
SCCO/V/CPRT/MSCI/FICO/ABBV/CAT/GEV/JNJ/MO/PEP/VZ/WMT/WST/CON/TER/
KLAC/LRCX`。

**項目別内訳**: WARN32件は**全件が`formula_verification`**
（α公式の再計算突合）で不合格。FAIL1件（LYFT）は`anomaly_detection`
（性質検査）。他の2項目（`pt_shares_consistency`・`dcf_components`）
は100銘柄全件が合格——同じ項目に32件集中しているのは個別銘柄の
問題ではなく式・思想側の問題という仮説を裏付ける（下記Step 2参照）。

**ポートフォリオ保有銘柄・BUY判定銘柄との照合**（`docs/portfolio/
data/portfolio.json`・`tanuki_score`フィールド）: 保有9銘柄
（ADBE/APP/CELH/CRWV/NVDA/PLTR/SOFI/SOUN/TSLA）のうち**ADBEがWARN**。
BUY判定4銘柄（BKNG/META/NVDA/TASK）のうち**BKNGがWARN**。FAIL銘柄
LYFTはBUY・保有いずれにも該当せず`tanuki_score=WATCH`。

**Step 2: FAILの中身の確認（原因判定）**

*LYFT（FAIL、`anomaly_detection`）*: `pt_shares_consistency`・
`dcf_components`・`formula_verification`は全て合格（α=0.000で
cap判定自体も一致）。`anomaly_detection`のみ「理論株価$-1.34が
ゼロ以下（FCF恒久マイナス銘柄）」で不合格。**検証側・計算側いずれの
バグでもない**——LYFTはFCFが恒久的にマイナスの銘柄であり、DCF
モデルが構造的にそのような銘柄に対して負の理論株価を算出しうる
ことを、`anomaly_detection`が設計通り正しく検知したもの。

*WARN32件（`formula_verification`、BKNG/ADBE/AMAT/KOで数値確認）*:
`validator.py::_extract_params()`（49行目）が`alpha_cap`を**常に
`1.0`固定**で返しているのに対し、本番の`core_calculator.py`
（503-516行目）は`maturity_config.json`の`_alpha_caps`
（セクター別、0.4〜1.0）・`_industry_alpha_caps`（業種別上書き）を
参照してセクターごとに異なる上限を適用している。実測: BKNG
（Consumer Cyclical想定）の実際のcapは0.6（stored alpha=0.6）・
ADBE/AMAT（Technology）は0.8（stored alpha=0.8）・KO（Consumer
Defensive想定）は0.6（stored alpha=0.6）——いずれも`_alpha_caps`の
該当セクター値と完全一致し、本番の計算自体は正しいことを確認した。
一方`validator.py`は`alpha_cap=1.0`との一致だけを見るため、
セクターcapが1.0未満の全銘柄で機械的にWARNになる。**これは
計算側ではなく検証側（`validator.py`）のバグ**であり、
`[[VALIDATOR-ALPHA-CAP-STALE-1]]`として新規登録した（修正は
別途、本調査では実施していない）。

**Step 3: 検知への接続（実装）**

`common/sec_data/report_consistency_check.py`にCHECK-40
（`_check_dcf_validation_failures()`）を新設し、`main()`の
CHECK-38/39と同じ位置に配線した。設計はCHECK-38を踏襲:
- 対象銘柄は呼び出し元が`all_tickers`（`get_tanuki_tickers()`ベース、
  既存の変数をそのまま渡す。事例5の原則、銘柄一覧の再実装なし）
- `validation.overall`が`PASS`以外の銘柄ごとに、不合格チェック数
  （`fail_count`）・`overall`・不合格チェック名をWARN表示
- `config/dcf_validation_baseline.json`（新規）に33銘柄の現状
  （fail_count・overall・failed_checks）を記録。`_meta`に
  「許容値ではなく是正目標」である旨、および32件がValidator側の
  既知バグ（`[[VALIDATOR-ALPHA-CAP-STALE-1]]`）由来の偽陽性である
  旨を明記
- baselineを超えて悪化した場合のみNG（CHECK-38と同一ロジック）

実行確認: `report_consistency_check.py --fail-on-ng`でWARN-40が
33件（32件`formula_verification`＋LYFT1件`anomaly_detection`）
表示され、baselineと一致するためNG=0を確認。

**Step 3-4: `test_iv_formula.py`との重複判定**

`test_iv_formula.py`（5銘柄限定）と`validator.py::pt_shares_
consistency`は同じ概念（P_t/shares整合性）を検証しているが、
**「重複」ではなく「乖離（片方が古い式のまま）」だった**:

- `validator.py`（正）: `total_v0 = v0 + rpo_pv + growth_option_pv`
  （**alphaを乗算しない**、コード内コメント「ALPHA-REDESIGN-1:
  core_calculator.pyはP_t算出にalphaを乗算しない」と明記）
- `test_iv_formula.py`（誤）: `iv_pt = v0_rm * (1.0 + alpha) + rpo_pv
  + go_pv`（**alphaを乗算している**、ALPHA-REDESIGN-1前の古い式の
  まま）

NVDAの実データで再現: `v0_rm=$15700.65B, alpha=1.0(capped),
rpo_pv=$0, go_pv=$1.86B, diluted_shares=24.221B, net_cash_ps=$3.065`。
`validator.py`式で計算すると`$651.37`（保存値と一致・pass）。
`test_iv_formula.py`式で計算すると`$1299.59`（alphaを二重に乗算する
ため約2倍）——これは既知の`test_iv_formula.py`失敗
（`pytest`常時2件failed）の失敗メッセージ
（`recalculated=$1299.5915, stored=$651.3666`）と完全一致し、
根本原因を確定した。**統合ではなく修正が必要な案件**と判断した
（**訂正、2026-08-20⑥**: 当時「正式なBACKLOGエントリが一度も
作られていなかった」と判断して`[[TEST-STALE-IV-1]]`を新規登録した
が、これは誤りで、2026-07-02登録の既存`[[TEST-STALE-IV-1]]`エントリ
を見落とした重複登録だった。同日中の後続の修正作業（⑥）で発覚し、
既存エントリへ統合した。詳細は同エントリ参照）。

**（2026-08-20追記）両バグとも修正完了。** `[[VALIDATOR-ALPHA-CAP-
STALE-1]]`はcore_calculator.py側のalpha_cap解決ロジックを
`resolve_alpha_cap()`として切り出しvalidator.pyがimportする設計で
解消（全100銘柄再検証でWARN32→0）。`[[TEST-STALE-IV-1]]`は
`validator.py::recalc_ivps_from_components()`という共有関数へ式を
統合し、`test_iv_formula.py`がそれをimportする設計で解消（pytest
既知failed2件が解消、対象銘柄も5→100へ拡張）。詳細は各エントリ参照。

#### ゲート1拡張: 売上高・純利益のyfinance突合＋週次化（2026-09-03）

`operating_income`単体だった2026-08-19のゲート1第一歩を、損益計算書の
残る中核2項目（売上高・純利益）へ横展開した。併せて、既存の
operating_income側yfinance突合が「SECデータは週1回（SEC_Data_
Update.yml、日曜）しか更新されないのに、yfinance突合自体は毎日
（TANUKI_VALUATION_Update.yml経由）実行され続けている」という無駄を
抱えていたことが判明したため、これも同時に是正した。

**STEP1（実装前の実データ確認）**: yfinance `income_stmt`の行ラベルは
想定と異なりうるため、AAPL/SITM/COHRの3銘柄で`pl.revenue`/
`pl.net_income`とyfinance各候補行を実測突合した。
- revenue: `"Total Revenue"`（3銘柄とも完全一致）
- net_income: `"Net Income"`——AAPL/SITMでは`Net Income`/`Net Income
  Common Stockholders`/`Net Income Including Noncontrolling
  Interests`/`Net Income From Continuing Operation Net Minority
  Interest`の4候補行が全て同値で判別不能だったが、COHRで`Net
  Income`=804,998,000のみがSEC`pl.net_income`（`NetIncomeLoss`タグ
  由来）と完全一致し、他3行はNCI・優先株配当等の調整後で乖離する
  ことを確認（`Net Income Common Stockholders`=769,896,000・`Net
  Income Including Noncontrolling Interests`=786,884,000）

**STEP2（関数の一般化）**: `_get_yf_operating_income(ticker,
target_end)`を`_get_yf_financial_value(ticker, target_end,
yf_row_label)`へリファクタリング。期末日±10日許容窓によるマッチング
ロジック・マッチ失敗時のNone返却は変更なし。

**STEP3（CHECK-41新設）**: `_check_revenue_net_income_reconciliation()`
を新設。CHECK-35（`_check_operating_income_reconstruction`）は
None/derived判定による対象限定（実測7銘柄前後）だが、revenue/
net_incomeは実測でNoneになる銘柄がほぼ皆無（**現存99銘柄**中、
revenue None 1件のみ・net_income Noneは0件）だったため、同じ限定
方式では実効性が出ないと判断し**全99銘柄**を対象にする設計とした
（依頼書は「全105銘柄」と記載していたが、実際に`get_tanuki_
tickers()`が返す対象は本タスク時点で99銘柄——AVGO除外
〈`[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`、2026-09-02〉等の累積
減少によるものと推測され、依頼書の想定銘柄数自体が既に陳腐化して
いた）。乖離率はCHECK-35と同様に情報提供のみでNG格上げは行わない。

**全99銘柄の乖離率分布実測結果（当初の予想通り、operating_incomeより
大幅に狭い分布）**:

| 項目 | n（マッチ数） | p50 | p95 | max |
|---|---|---|---|---|
| revenue | 98/99 | 0.0% | 0.0% | 15.6%（MO） |
| net_income | 98/99 | 0.0% | 0.0% | 88.4%（FCX） |

（参考: operating_income側は2026-08-19実測でp95=81%・max=342%
〈AVAV〉、再構成を経ない分だけ分布が大幅に狭いという依頼書の仮説が
実測で裏付けられた）

- revenue/net_incomeともp50=p95=0.0%——大多数の銘柄で完全一致する
  一方、少数の外れ値は実在する会計上の差異であり実装上の不具合ではない
  ことを個別確認した:
  - MO（revenue 15.6%）: SEC=$23,279M vs yfinance=$20,139M。yfinance
    側が物品税（excise tax）控除後の純額表示になっていると推測される
    表示方針の違い（net_incomeは6,947Mで完全一致のため、単純な
    マッチング誤りではない）
  - FCX（net_income 88.4%）: SEC=$4,152M vs yfinance=$2,204M。FCXは
    インドネシアPT-FI鉱山等でNCI（非支配持分）比率が高い企業であり、
    `"Net Income"`行がyfinance側でNCI控除後の値を返しているケースが
    あることを示唆（STEP1でCOHRとは逆方向の乖離——`"Net Income"`が
    企業ごとに必ずしも同じ意味の値を返すとは限らない可能性が残る。
    ただしCOHR/MO/FCXいずれも実在する会計上の差異であり、乖離率の
    NG格上げを避けた本設計判断の妥当性を裏付ける実例）
  - 未マッチ1銘柄（CWAN）: `yfinance.Ticker("CWAN").income_stmt`が
    空DataFrameを返す（既存`[[MARKETDATA-CWAN-FROZEN-DATA-SUSPECT-1]]`
    と同型のyfinanceデータ欠落、CHECK-41側の実装不具合ではない）

**閾値設定の要否（依頼書の判断事項）**: 上記分布・原因確認の結果、
NG格上げは見送りWARNのまま据え置く方針を維持する。p95=0.0%という
分布はNG化の実益（誤検知リスクなし）を示唆する一方、MO/FCXのような
「実在する会計上の差異」を持つ銘柄が今後も一定数存在しうるため、
閾値を設けても「何%を境にするか」の恣意性が残る。当面はWARN据え置き
で運用し、実際にWARN-41が問題の早期発見に寄与した実例が蓄積された
時点で改めて判断する。

**STEP4（週次化）**: `report_consistency_check.py`にCLIフラグ
`--include-yfinance-checks`（デフォルトFalse）を新設し、
`_get_yf_financial_value()`呼び出し（operating_income/revenue/
net_income の3種）をこのフラグでゲートした。フラグFalse時は
None/derived判定自体（CHECK-35のyfinance非依存部分）は従来通り毎日
実行を維持し、yfinance呼び出し部分のみをスキップする。
`.github/workflows/SEC_Data_Update.yml`（週1回、日曜）の
`Consistency Check Gate`ステップのみフラグを追加し、他3ワークフロー
（TANUKI_VALUATION_Update.yml・Adjusted_Eps_Analyzer_update.yml・
Stonks_Silo_Update.yml、いずれも日次実行）は変更していない。

**検証**: フラグなしでAAPL/SITM/COHRの3銘柄を実行し、yfinance呼び出し
0回・所要0.85秒を確認（`safe_yf_ticker`呼び出し回数を実測カウント）。
フラグありで同3銘柄を実行し、7回のyfinance呼び出し（revenue/
net_income各1回×3銘柄＋COHRのoperating_income再構成分1回）・
WARN-41が正しく発火することを確認。全99銘柄では`--fail-on-ng`が
フラグなし/ありともNG=0（WARN=93件→289件、CHECK-41追加分+196件が
内訳と一致）。pytest 1009 passed。YAML構文エラーなし
（`PyYAML`で`schedule.cron`同様に`Consistency Check Gate`ステップの
`run`を確認）。audit.py実行、既存の🟡警告9銘柄（本タスクと無関係の
None値系警告）以外に新規の異常なし。

**未実施（次回サイクル待ち）**: SEC_Data_Update.ymlの次回日曜実行
（workflow_dispatchでの早期確認も可）で、実際にyfinance突合が発火し
他3ワークフローの日次実行では発火しないことの実地確認は本タスクでは
未実施。次回日曜サイクル後に確認すること。

実装コミット: `297ba95523`（関数リファクタリング＋CHECK-41新設）・
`6fa8c3905f`（週次化: フラグ新設・ワークフロー変更）。

**週次化の実地確認完了（2026-09-07）**: 2026-09-06（日）12:00 UTC
（23:29 JST push）の`SEC_Data_Update.yml`定期実行（run #104、
`https://github.com/Koichi-Shigihara2/On-a-journey/actions/runs/
34039199103`、コミット`16a3cd2e89`）を実地確認した。

1. **CHECK-41（yfinance突合）の発火確認**: `Consistency Check Gate`
   ステップのログで、対象銘柄のほぼ全てにrevenue/net_income両方の
   WARN-41が実測値付きで出力されていることを確認した。ログ該当箇所
   （原文引用）:
   ```
   ⚠️ AAPL
   [🆕未確認 WARN-41 revenue yfinance突合] FY2025: SEC=416,161,000,000 yfinance=416,161,000,000（乖離+0.0%）
   [🆕未確認 WARN-41 net_income yfinance突合] FY2025: SEC=112,010,000,000 yfinance=112,010,000,000（乖離+0.0%）
   ```
   ゲート全体の最終行:
   ```
   結果: NG=0件 / WARN=317件
   ✅ ゲート通過
   ```

2. **他3ワークフローで発火しないことの確認**: 直近実行——
   `Stonks_Silo_Update.yml` run #155（2026-09-06、コミット
   `16a3cd2e89`、`Consistency Check Gate`4秒）・
   `Adjusted_Eps_Analyzer_update.yml` run #88（2026-09-07、
   コミット`07c594f...`、同ステップ6秒）——のログをいずれも全文
   確認したが、WARN-41は1件も出現しない（他のWARN種別
   〈WARN-21/22/23/26/29/35/36/44/45等〉は同一銘柄で通常通り出現
   しており、ステップ自体は正常実行されている）。`HypeCore_Update.yml`
   は`report_consistency_check.py`自体を呼び出していないため対象外
   （コード上再確認済み）。3ワークフローとも`--include-yfinance-checks`
   フラグ無しでの呼び出しであることをYAML上再確認済み。

3. **乖離分布の変化確認**: 2026-09-03時点の実測（p50=p95=0.0%、
   max: revenue 15.6%〈MO〉・net_income 88.4%〈FCX〉）と比較し、
   最大値を作る2銘柄はいずれも**完全に同一の乖離率で不変**
   （MO revenue+15.6%・FCX net_income+88.4%、いずれも既知の会計上の
   差異——注記済み）。新たに1%未満の小さな乖離が数件確認できた
   （XOM revenue+2.6%・RXRX revenue+0.6%・SCCO net_income+0.3%・
   ONDS net_income-0.2%等）が、いずれもp95=0.0%の分布を動かす水準では
   なく、閾値設定判断（WARN据え置き方針）に変更を要する材料はない。
   なお今回のWARN総数317件は09-03時点の119件から増加しているが、
   これはCHECK-41自体の悪化ではなく、本セッションで新設した
   WARN-44/45（段差型急変検知の売上総利益・CapExへの展開、
   `[[DATA-JUMP-CHECK-GENERALIZE-1]]`）等、無関係な新規チェック項目が
   同じゲートに合流したことによる増分である。

以上により、ゲート1拡張（CHECK-41・週次化）は設計通り本番稼働して
いることを実地確認済み。追加対応は不要。

---

## 優先度：高（早急に対応）

### [MACRODATA-FULL-HISTORY-DAILY-REFETCH-1] fetch_series()/fetch_all_series()がstart未指定時に常に全期間履歴を再取得する設計になっている（日次cronが非効率）
**優先度:** 低〜中（実害は限定的〈FRED APIへの負荷・実行時間増加のみ、
upsert設計のため正確性への影響はない〉が、`common/market_data/`が
確立した「日次は直近のみ・全期間取得は一過性の別関数」という設計
パターンから逸脱している）
**分類:** 設計改善 / 効率性
**登録日:** 2026-08-12
**発見:** `common/macro_data/`定期取得ワークフロー新設・動作確認
（`fetch_all_series()`の実FRED_API_KEYによるローカル実行、チャット
記録、2026-08-12）

#### 内容
`common/macro_data/fetcher.py::fetch_series(series_id, start=None)`は
`start`未指定時、`observation_start`パラメータをFRED APIへ渡さない
ため、系列の提供開始日（系列によっては1940〜1970年代）からの**全期間
履歴**を毎回取得する。`.github/workflows/Macro_Data_Update.yml`
（毎日UTC10:00実行、`python common/macro_data/fetcher.py`を`start`
指定なしで呼び出す）はこの関数を経由するため、**日次cronが実行の
たびに25系列全件・合計約9.5万レコード（初回実測、18MB）を毎回
再取得する**設計になっている。

これは`common/market_data/fetcher.py`が確立した設計パターン
（`fetch_daily_prices()`＝日次cronは直近数日分のみ取得・
`backfill_daily_prices(period/start)`＝全期間取得は定期cronに
組み込まない一過性の別関数）から逸脱している。`update_series()`は
日付単位のupsertのため正確性への実害はないが、FRED APIへの負荷・
GitHub Actions実行時間・git差分サイズが日次cronとしては不必要に
大きい。

#### 対応方針（未定）
- `fetcher.py`のCLIへ`--start`引数を追加し、`Macro_Data_Update.yml`の
  日次cron呼び出し側は直近数日〜数週間分のみを指定する
  （`common/market_data/`の`fetch_daily_prices()`相当の設計に揃える）
- 初回の全期間投入は今回実施済みのため、以降は「直近分のみ日次取得・
  全期間再取得が必要な場合のみ手動で`--start`省略実行」という運用に
  切替える

#### 着手条件
なし。次回`common/macro_data/`関連作業時に対応要否を判断する。

---

### [MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1] 系列単位の取得失敗がviolations_log.jsonで「正常」と区別できない
**優先度:** 中（実害は現時点でFTSD1件のみ確認済みだが、今後同様の
失敗〈系列ID変更・FRED側仕様変更等〉が起きても気づけない構造的リスク）
**分類:** 設計上のギャップ / 可視性欠如
**登録日:** 2026-08-15
**発見:** `common/macro_data/`更新実行実績・データ鮮度の確認調査
（チャット記録、2026-08-15）
**統合について（2026-09-05）**: `MACRODATA-FTSD-SERIES-ID-INVALID-1`
（FRED系列コード「FTSD」がFRED API上に実在しない具体事例）を本エントリへ
統合した。両者は「一般的な欠陥（取得失敗が可視化されない設計）」と
「その欠陥が実際に表面化した具体例（FTSD系列コード誤り）」という直接の
親子関係にあるため、可視性欠如を扱う本エントリを主エントリとして残し、
FTSDケースの内容は要約せず全文そのまま下記「具体事例（FTSDケース）」に
保持する。`MACRODATA-FTSD-SERIES-ID-INVALID-1`はBACKLOG.mdから削除済み。
`[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]`は別論点のため統合対象外。

#### 内容
`fetch_series()`は失敗時に例外を投げずNoneを返す設計（print()ログの
みでリポジトリには残らない）。`update_series()`はNone時に
`{"updated": 0, "warnings": []}`を返し`violations_log.json`へ書き込む
が、この構造は正常に0件警告だった健全な系列と区別がつかない。
FTSDエントリ（`{"checked_at": ..., "warnings": []}`）が実例（詳細は
下記「具体事例（FTSDケース）」参照）。`series/{ID}.json`ファイルが
存在しないことに能動的に気づかない限り、取得失敗を発見できない。

加えて、`fetch_all_series()`のforループには系列単位のtry/exceptが
なく、予期しない例外（ディスクエラー等）が発生した場合、その系列
以降の全系列が未処理のままバッチ全体が中断する構造的リスクも
あわせて確認された（今回の3日間の実行では未発生）。

#### 対応方針（未定）
- `violations_log.json`に「fetch自体の成否」を示すフィールド
  （例: `fetch_status: "success"/"failed"/"skipped"`）を追加する
- `fetch_all_series()`のforループに系列単位のtry/exceptを追加し、
  1系列の失敗が他系列の処理を止めないようにする
- 週次等の定期監視（`audit.py`型の診断ツール）で
  `series_meta.json`の全系列と`series/`ディレクトリの実ファイルを
  突合し、欠落を検知する仕組みを追加する

#### 着手条件
なし。対応方針の具体化から。

#### 具体事例（FTSDケース、統合元[MACRODATA-FTSD-SERIES-ID-INVALID-1]）
以下は独立BACKLOGエントリだった`[MACRODATA-FTSD-SERIES-ID-INVALID-1]
FRED系列コード「FTSD」がFRED API上に実在しない（05_main.pyのWTREGEN
フォールバックが機能しない可能性）`の全文をそのまま転記したもの。

**優先度:** 中で据え置き（2026-08-13事実確認の結果、実害は極めて稀と
確認できたため引き上げ不要と判断。ただし機能しないフォールバックを
放置すべきではないため記録は残す。詳細は下記「追記」参照）
**分類:** バグ疑い / データソース側の系列コード誤り
**登録日:** 2026-08-12
**更新日:** 2026-08-13（事実確認調査完了。原因特定・実害実績確認・
修正案提示。詳細は下記「追記」参照。実装コード変更なし）
**発見:** `common/macro_data/`定期取得ワークフロー新設・動作確認
（`fetch_all_series()`の実FRED_API_KEYによるローカル実行、チャット
記録、2026-08-12）

##### 内容
`common/macro_data/fetcher.py::fetch_series("FTSD")`を実行したところ、
fredapi経由・`curl`による直接FRED REST API呼び出し
（`https://api.stlouisfed.org/fred/series?series_id=FTSD&api_key=...`）
の両方で`{"error_code":400,"error_message":"Bad Request.  The series
does not exist."}`が返り、**`FTSD`はFRED上に実在しない系列コードで
あることを確認した**（ネットワーク一時障害やfredapiライブラリ側の
問題ではなく、系列コード自体が無効）。

`05_main.py::update_liquidity_csv()`は`WTREGEN`（TGA残高）取得失敗時に
`FTSD`へフォールバックする実装になっている（1959-1960行:
`if tga_val is None: tga_val, _ = fred_latest(fred, "FTSD",
target_date, lookback=21)`）が、このフォールバックが実際に発動しても
`FTSD`自体が無効な系列コードのため取得は失敗し、TGA値は結局取得
できないままになると推定される。`FTSD`は
`[[MACRODATA-FTSD-MISSING-FROM-INVENTORY-1]]`（`INPUT_DATA_TOBE.md`の
24系列台帳への追加漏れ、`INPUT-A-049`として2026-08-12に対応済み）で
台帳に追加した系列だが、台帳追加時点では実際にFRED上に存在するかの
検証は行っていなかった。

##### 追記（2026-08-13、事実確認調査完了、記録のみ・実装なし）

**原因特定**: `FTSD`は2026-05-08のコミット`8561125f3`（`Co-Authored-By:
Claude Sonnet 4.6`、NET LIQUIDITY計算のためWTREGEN/RRPONTSYD取得を
追加した際にフォールバック先として導入）で、実在確認なしに導入されて
いたことをコミット履歴で確認した。コミットメッセージ・コードいずれにも
典拠の記載はない。

**正しい代替系列の特定**: FRED検索API（`search_text=treasury general
account`）で調査した結果、`WDTGAL`（Liabilities and Capital: Deposits
with F.R. Banks, Other Than Reserve Balances: U.S. Treasury, General
Account: **Wednesday Level**）が有力な代替候補と判明。`WTREGEN`
（**Week Average**）と同一カテゴリ・同一期間（2002-12-18〜2026-08-05、
現在も更新中）・同一単位（Millions USD、週次）でありながら集計方法が
異なる（週平均 vs 水曜時点値）ため、名目だけの重複ではなく実務上意味の
あるフォールバックになる。なお同種の旧系列`WLTGAL`・`LDGUST`は2018年に
DISCONTINUEDと確認済みのため候補外。`search_text=FTSD`はFRED検索でも
0件ヒットで、類似候補すら存在しない。

**実害実績の確認**: `docs/market-monitor/macro-pulse/data/
05_liquidity.csv`（2023-01-01〜現在、1302日分）を全件確認した結果、
`tga`列が空欄なのは**2026-05-31の1件のみ**（前日5/30・翌日6/1は正常
取得）。この日、`WTREGEN`取得失敗→`FTSD`へフォールバック→`FTSD`も
無効のため結局取得失敗、という実際の発火・失敗の実績を確認した。
同日`net_liquidity`列（`=(WALCL−TGA−RRP)/1,000,000`）も算出不能で
空欄。`stealth_signal`列はその日も"neutral"のまま記録が継続しており
致命的な誤判定はないが、NET LIQUIDITY系列に1日分の欠測点が生じていた。
WTREGEN自体の失敗頻度は1302日中1日（約0.08%）で極めて低頻度。

**新アーキテクチャ（`[[MACRODATA-LAYER-CONSTRUCTION-1]]`切替後）での
位置づけ**: 現行`fred_latest()`は`reader.get_latest()`を呼ぶのみで、
旧実装が持っていた`target_date`基準・`lookback`日数ウィンドウの制約が
廃止されている。ローカルの`series/WTREGEN.json`に一度でも値が書き込ま
れていれば、当日の取得が一時的に失敗しても直近キャッシュ値をそのまま
返すため、フォールバック発動条件（`reader.get_latest("WTREGEN")`が
Noneを返す）は「`WTREGEN`系列ファイル自体が存在しない／空」という、
旧実装よりさらに稀なケースに限定される。初回投入時点でWTREGENは25系列
中の成功24系列に含まれており、現状ローカルにデータが存在するため、
現行アーキテクチャ下ではこのフォールバックが発動する可能性はさらに
低下していると評価できる。ただし「発動条件が稀になったこと」と
「発動時に機能するか」は別問題であり、後者（`FTSD`が無効）は現在も
未解消。

**修正案（未実装、次回対応時の実装指針）**:
1. `05_main.py::update_liquidity_csv()`のフォールバック先を
   `"FTSD"`→`"WDTGAL"`に変更
2. `common/macro_data/series_meta.json`に`WDTGAL`エントリを新規追加
   （`category: "liquidity"`、`consumers: ["05_main.py::
   update_liquidity_csv (WTREGENフォールバック候補)"]`）。追加すれば
   `fetch_all_series()`が自動的にバッチ取得対象に含める
3. `INPUT_DATA_TOBE.md`/`INPUT_DATA_AS_IS.md`の`FTSD`
   （`INPUT-A-049`）記載を`WDTGAL`に置き換えるか、無効系列だった旨の
   注記を追加

**優先度判断**: 実害頻度は旧実装でも0.08%、新実装ではさらに稀と推定
されるため、優先度「中」からの引き上げは不要と判断し据え置く。一方で
「一度も機能しないフォールバックが存在し続ける」こと自体は望ましくない
ため、記録は残す。

##### 着手条件
次回の低優先度課題群まとめ対応時（`[[MACRODATA-AS-IS-DUPLICATION-
UNDERCOUNT-1]]`・`[[MACRODATA-SCHEDULED-SILENT-GAP-CSCICP-USALOL-1]]`・
`[[MACRODATA-IMPORT-HISTORY-CONFIG-DRIFT-1]]`・
`[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]`等と合わせて着手検討）。
上記「修正案」を踏まえ、対応方針は事実上確定済み。

---

### [MARKETDATA-SP500-SCRAPE-INVALID-TICKERS-1] get_sp500_constituents()のWikipediaスクレイピングに不正銘柄コード（FDXF/HONA/Q）が混入
**優先度:** 低（実害は限定的。該当銘柄の日次データ取得が無駄になる程度で、
`validate_price_record()`等の検証機構は正常に機能し保存自体は成立する）
**分類:** バグ疑い / データソース側スクレイピングの解析精度
**登録日:** 2026-08-11
**発見:** `common/market_data/`日次価格層バックフィル全母集団実行時
（チャット記録、2026-08-11。前回2026-08-10の同種バックフィル検証時にも
同一事象を確認済みだが、当時は未登録のまま作業ツリーごと消失していた
ため今回改めて登録する）

#### 内容
`fetcher.py::get_sp500_constituents()`（Wikipedia
`List_of_S%26P_500_companies`テーブルを`pandas.read_html()`でスクレイピング）
の取得結果に、正規のS&P500構成銘柄コード（`FDX`・`HON`）とは**別に**
`FDXF`・`HONA`という不正な銘柄コードが混入していることを確認した
（`common/market_data/_sp500_constituents_cache.json`実測: `FDX`/`FDXF`・
`HON`/`HONA`がいずれも別エントリとして両方存在、総数503件）。加えて
`Q`という短い銘柄コードも含まれており、こちらは実在ティッカーの
可能性もあるが要確認。実際にこの3銘柄（`FDXF`・`HONA`・`Q`）へ
`backfill_daily_prices()`を実行したところ、正規のS&P500構成銘柄と異なり
それぞれ52日・39日・197日分という不完全な履歴しか取得できず、実在しない
または別の性質を持つ銘柄コードである可能性が高い。

推定原因: Wikipediaのテーブル構造変化・脚注マーカーの混入・列ズレ等に
よる`pandas.read_html()`側のパース精度問題（未特定）。

#### 対応方針（未定）
- `get_sp500_constituents()`のWikipediaスクレイピング結果を手動で
  Wikipedia実ページと突合し、`FDXF`/`HONA`/`Q`がどの行・どのセルから
  混入しているか特定する
- 原因判明後、パースロジックに除外フィルタ（例: 既知の正規ティッカーと
  重複するprefixを持つ不審なコードを除外する、またはテーブル列数の
  妥当性チェックを追加する）を実装する
- 対症療法として不正コードのブロックリストを設ける案もあるが、
  Wikipedia側の構造変化のたびに再発しうるため根本原因の特定を優先する

#### 着手条件
なし。優先度低のため`common/market_data/`本線完了後の低優先度タスクと
して余裕があるときに着手する（2026-08-13更新: 登録時点の「着手順序4の
残作業と並行」という前提は、`common/market_data/`本番消費者切替が全数
完了したことで消滅した。バグ自体〈不正銘柄コードの混入〉は未解消の
まま有効）。

---

### [MARKETDATA-VIX9D-DATA-GAP-1] ^VIX9Dのyfinanceデータに約1ヶ月の欠落期間（2026-07-17〜08-10）が存在する
**優先度:** 中（`collect_and_send.py`のsentiment_score算出
〈vix_level補正〉・VIX9D対VIX比較の直接入力のため、`[[MARKETDATA-
CWAN-FROZEN-DATA-SUSPECT-1]]`より実害の潜在範囲が広い。ただし現時点の
実測ではsentiment_scoreへの影響はゼロと確認済み、詳細は「内容」参照）
**分類:** データ品質疑い / 外部データソース側の異常疑い
**登録日:** 2026-08-11
**発見:** `[[MARKETDATA-LAYER-CONSTRUCTION-1]]`着手順序4-6
（`collect_and_send.py`切替）のStep3検証中（チャット記録、2026-08-11）

#### 内容
`common/market_data/daily/^VIX9D.json`（`period="1y"`バックフィル済み）
を確認したところ、2026-07-17を最後に次のレコードが2026-08-10まで
存在しない（約1ヶ月分の欠落）。**yfinance側の問題であることを実測で
確認済み**: `yf.Ticker("^VIX9D").history(period="1mo")`を実行しても
Yahoo Finance自体が2026-07-13〜07-17の5件しか返さず、`period="5d"`では
2026-08-10の1件のみしか返らない。取得コード側の不具合ではなくデータ
ソース側の欠落（`[[MP-IRX-FRED-1]]`の教訓通り、一次情報〈yfinance直接
呼び出し〉で切り分け済み）。

実害範囲: `collect_and_send.py::get_realtime_data()`のVIX9D関連2出力
（メイン指標表示・VIX9D対VIX比較）と`compute_sentiment()`の
`vix_level`サブスコア補正（VIX9D逆転時-0.05）が、直近2営業日の実データ
不足により中立デフォルト（None・補正なし）にフォールバックする。
2026-08-11時点の実測比較では、`compute_sentiment()`の`vix_level`
サブスコアは切替前後（yfinance直接呼び出し版・market_data版）で完全
一致しており、両者ともVIX9D欠落の影響を同一に受けている（本切替による
新規劣化ではない）ことを確認済み。

#### 対応方針（未定）
- 単発の事象か、繰り返し発生するかを次回日次バッチ実行時に再確認する
- yfinance側でVIX9Dのデータ提供が回復するかを継続監視する
- 実害が拡大した場合（sentiment_scoreの結果自体に有意な影響が出る場合）
  のみ優先度を上げ、代替データソース（CBOE公式等）への切替を検討する

#### 着手条件
なし。優先度中だが実害は現時点でゼロと確認済みのため、次回同種事象の
再確認時に着手判断する。

---

（[[STONKS-SILO-CLI-TICKERS-SHADOW-1]]は2026-08-27実装完了・全25銘柄
再生成完了、BACKLOG_DONE.md「2026-08-27（完了）」参照）

---

（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]は2026-09-10、「真の残タスク
2件」のうち`[[SECDATA-LEGACY-CIK-GRANULARITY-1]]`が2026-09-09に根拠薄弱
判定でクローズ済み・もう1件`[[SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-
CONSIDERATION-1]]`は既に本ファイルへ独立登録済みと判明し、本エントリ
固有の残タスクが消滅したためクローズ、BACKLOG_DONE.md「2026-09-10
（完了）」参照）

---

### [XBRL-UNIT-SCALE-MISMATCH-DETECTION-1] 同一タグ・同一期間の値が複数filing間で10のべき乗単位で乖離する場合を検知する汎用チェックの新設提案
**優先度:** 中
**分類:** アーキテクチャ改善 / 新規検知チェック提案
**登録日:** 2026-08-02
**発見:** [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]調査から派生（chat記録）

#### 内容
同一XBRLタグ・同一unit・同一(start,end)期間について、異なるaccn
（filing）間で値の比率が10のべき乗値（1000倍・1,000,000倍等、±2%許容）
に近い場合、SEC提出時のスケール指定漏れ（"in thousands"欠落等）という
業界共通のXBRLタグ付けミスの可能性が高いことが判明した。素朴な閾値
（比≥100）のみでは72銘柄がヒットしノイズが大きすぎたが、比≒10の
べき乗という条件を追加することで、SPAC逆合併の会計主体入替
（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]と同系統、正当な処理）等のノイズを
排除し、18銘柄・126件まで収束することを確認した。

該当銘柄・件数: COHR(26)・KO(20)・NVDA(20)・CPRT(18)・TER(12)・ONDS(5)・
FCX(4)・HEI(4)・MO(4)・ADSK(2)・CELH(2)・TSLA(2)・ZS(2)・ASTS(1)・
IONQ(1)・MSCI(1)・SOUN(1)・ZETA(1)。対象フィールドはWeightedAverageNumber
OfSharesOutstandingBasic/Diluted(計98)が過半だが、CommonStockShares
Outstanding・EPS系・Depreciation系・NetIncomeLoss系・LongTermDebt・
DebtCurrent・Liabilities・OperatingIncomeLoss・AmortizationOfIntangible
Assets等、幅広いフィールドに及ぶ。

#### 影響
COHR以外の該当銘柄は未トリアージ。検知＝即実害ではなく、本人データ優先
ロジックにより既に正しい値が採用されているケース（実害なし、COHR自身の
FY2019/2020 Q3・FY2023/2024 D&A等で確認済み）もあれば、実際に格納値が
誤っているケース（COHR 2009-2011のshares系）もある、個別トリアージが
必要な問題。

#### 対応方針（登録時点）
未定。既存WARN群（WARN-24等）と同型の「検知のみ・自動修正なし」枠組みで
新設（WARN-30候補）することを推奨する。ただし既存WARN群がextracted
（抽出済み）データを対象にするのに対し、本チェックはcompany_facts.json
の生タグレベル（抽出前）を横断的に見る必要があり、`_parse_raw_data()`
または`report_consistency_check.py`への新規ロジック層追加という設計に
なる。検知後は126件を個別トリアージし、実害あり/なしを分類する運用が
必要。

#### 実装方針追記（2026-08-02、[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]
全母集団シミュレーション結果を統合、チャット記録・読み取り・オフライン
シミュレーションのみ）
[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]として個別登録していた
「`_extract_single_key()`のtie-break条件を新しいfiling優先に変更する」
という対応方針を、本エントリに統合する（詳細は同エントリのBACKLOG_DONE.md
移動後の記録を参照）。

全母集団シミュレーションの結果、tie-break条件を単純に「新しいfiling優先」
へ変更する広範な設計変更は不採用と確定した。31銘柄・124件で値が変化し、
確実な改善はCOHRの2件（shares_diluted/basic）のみで、残り122件は改悪
（VZ(2008)純利益が黒字$6,428M→赤字-$2,193Mに反転等）・改悪疑い
（SOUN/KULRのSPAC実体混同、HON/FCX/HEIのrestatement・株式分割調整）・
判断不能な乖離が大半だった。また、WMT(2014)でtotal_assetsが微小変動した
結果、`_backfill_total_liabilities_via_identity()`の安全網（TL==TAの
場合のみ発動）が完全一致条件を偶然すり抜け、[[TOTAL-LIABILITIES-
FALLBACK-TAG-DESIGN-FLAW-1]]と同型のバグを別経路で復活させかねない
という重大な相互作用リスクも判明した。

**実装方式を確定**: 「同符号 かつ 比が10のべき乗値（±2%許容、n≥2）」
という本エントリのガード条件に該当する場合のみ、tie-breakをより新しい
filing優先に切り替える設計とする。この条件で124件をフィルタしたところ、
COHRの2件（shares_diluted/basic）のみが該当し、他122件は自動的に
除外されることを確認済み。

実装前に2点の追加確認が必須:
(a) 既存の恒等式ベース安全網（`_backfill_total_liabilities_via_
    identity()`等）との相互作用を個別に再検証する（WMT(2014)のような
    偶発的なすり抜けがないか）
(b) ガード適用後も105銘柄で改めて全母集団シミュレーションを行い、
    新規の意図しない変化がゼロであることを確認する

対象は当面COHRの2件（shares_diluted/basic、2009-2011年度）に限定される
見込み。`fact_overrides.json`での個別対応（[[COHR-SHARES-DILUTED-
UNIT-SCALE-BUG-1]]で確定済み）と、tie-break側の恒久対応のどちらを
採るか、または両方必要かは実装時に判断する。

#### 実装前最終確認結果・実装方針確定（2026-08-02、チャット記録、読み取り・
オフラインシミュレーションのみ）
実装前の2点の追加確認（前項）を完了した。

(a) 既存の恒等式ベース安全網との相互作用リスクはなし。
`_backfill_total_liabilities_via_identity()`・[[CHECK29-ACCOUNTING-
IDENTITY-DETECTION-LAYER-1]]はいずれもBS項目（total_assets/total_
liabilities/stockholders_equity/NCI/一時的持分）のみを対象とする一方、
ガード条件付き介入が実際に触れるのはshares項目の2フィールドのみで、
両者が扱うフィールド集合に重なりがなく構造的に相互作用の経路が存在し
ないことを確認した。前回懸念したWMT(2014)型のすり抜けは、ガード条件
（比が10のべき乗、最低100倍）により正しく除外されることを確認した
（WMTの乖離比≒1.001はガード条件を満たさないため対象外）。

(b) ガード適用後の全母集団シミュレーションで、該当・変化するのはCOHRの
2010年度shares_diluted/basicの2フィールドのみと最終確定した。他104
銘柄・COHRの他年度（2009・2011年度含む）は完全に無変化、新規の意図
しない波及も確認されなかった。

**実装方針を確定**: [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]の
`fact_overrides.json`個別上書き（2009-2011年度、3年度とも1回で解決）を
実装対象とし、tie-break変更（ソースコード変更）は当面見送る。理由:
(a)tie-break変更は2010年度1件しか解決せず、その1件もfact_overrides側
で重複解決される（tie-break変更単独では2009・2011年度は解決しない:
2009年度は後続filingに正しい値自体が存在せず、2011年度は本人データ
優先ロジックにより保護されているため）、(b)現時点でCOHR以外に該当する
実ケースがゼロと確定しており、ソースコード変更のコストに見合う実利用
価値が現状ない。ガード条件の設計自体は妥当性・安全性が確認済みのため
破棄せず、将来「本人データ優先ロジックでは救えず、かつ個別override
登録が非現実的な規模の」新規ケースが発見された時点で再検討する。

#### 着手条件
[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]のfact_overrides実装で事実上
完結、tie-break変更部分は将来の予防的対応として保留。優先度中（業界
共通のミスパターンとして汎用的価値が高いが、即座の実害は限定的
〈COHR以外は未確認〉のため）。

---

### [TTM-DATA-DRIFT-BEHIND-PIPELINE-1] common/sec_data/ttm/配下のTTM系列ファイルが2026-07-26生成のまま、以降のパイプライン修正に追従しておらず陳腐化している可能性
**優先度:** 中（登録時「高」から引き下げ、影響実測の結果、現在進行形の
実害はゼロと確定したため。構造的リスクは残存）
**分類:** データ品質 / パイプライン出力の陳腐化
**登録日:** 2026-08-02
**発見:** [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]実装検証時（チャット記録）

#### 内容
`common/sec_data/ttm/`配下の全105銘柄のTTM系列ファイル
（`{ticker}_ttm_series.json`）が、`git log`確認で2026-07-26生成のまま
であることが判明した。一方、TTM系列の入力元となる抽出パイプライン
（`common/sec_data/layer3_builder.py`・`common/sec_data/q4_implied.py`）
は2026-07-30に、`common/sec_data/parser.py`は本セッション中の
2026-08-02に、それぞれ別コミットで修正されている。実際にPEP銘柄で
検証したところ、現行パイプライン（2026-08-02時点）で再生成すると
`selling_general_and_administrative`が$34,501,000,000→$37,791,000,000
（約9.5%）変化することを確認済み（`[[TTM-CALC-QUARTER-CONTIGUITY-
UNCHECKED-1]]`実装作業の副産物として発見。この差分は今回実装した連続性
チェックとは無関係で、単純にttm/ファイルが2026-07-26時点のパイプライン
出力のまま更新されていないことに起因すると特定済み）。

`.github/workflows/SEC_Data_Update.yml`を確認したところ、毎週日曜
12:00 UTC（cron: `0 12 * * 0`）に`update.py`を実行し
`common/sec_data/ttm/`を含む全出力を自動再生成・commit・pushする
ワークフローが既に存在する。**このワークフローが正常に稼働していれば
陳腐化は本来自然解消されるはずであり、なぜ2026-07-26以降ttm/が
更新されていないのか（ワークフロー自体の失敗・無効化・直近未実行等）
が未確認の論点として残る。**

#### 影響
未確定。PEP1銘柄のSG&Aで約9.5%の差分を確認したのみで、105銘柄全体で
どのフィールド・どの銘柄にどの程度の乖離があるかは未調査。TTM系列は
TANUKI VALUATIONのFCFベースDCF計算・STONKS SILOのrunway計算に直結する
ため、陳腐化の程度次第では現在進行形のIV算出精度への実害がありうる。
`[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]`実装時は対象18銘柄のみを
最新化し、残り87銘柄は意図的に未対応のまま据え置いている。

#### 対応方針（登録時点）
未定。実装は行わず、まず以下の調査が必要:
- `.github/workflows/SEC_Data_Update.yml`のGitHub Actions実行履歴を
  確認し、2026-07-26以降に正常実行されているか・失敗しているか・
  無効化されていないかを特定する
- 陳腐化の実際の範囲（全105銘柄中何銘柄・どのフィールドで実質的な差分が
  生じるか）を、現行パイプラインでの全銘柄再生成とフローズン入力比較で
  定量化する
- 通常の週次自動更新サイクルで自然解消される見込みか（ワークフローが
  正常なら次回日曜実行で解消するはず）を確認する
- 上記調査の結果次第で、手動での全105銘柄再生成が必要か、ワークフロー
  側の修正が必要かを判断する

#### 根本原因調査結果（2026-08-02、チャット記録、読み取りのみ・重大な
構造的発見）
GitHub Actions APIで`SEC Data Update`ワークフローの実行履歴を確認した
結果、**ワークフロー自体は正常稼働中**と判明した（毎週日曜、直近9回超
すべて`schedule`トリガーで`success`、無効化もされていない。`git log`上の
`ttm/`最終更新コミット`340b8b8ae`〈author=`github-actions[bot]`〉が
2026-07-26の実行と完全に一致）。調査時点（2026-08-02 12:32〜12:36 UTC、
本日も日曜）では本日分の実行が未発火だったが、前週の実行もcron時刻
（12:00 UTC）から49分遅れて開始しており、GitHub自身が公式に案内する
「12:00〜15:00 UTC帯はscheduleトリガーの遅延が起きやすい」時間帯と
一致するため、**単なる未発火（これから発火する見込み）であり失敗では
ない可能性が高い**。default_branch=`kaihatsu`とワークフローの
checkout先も一致しており、本セッションのコード変更後も
`common.sec_data.update`のimportエラーなし・ゲート
（`report_consistency_check.py`）もNG=0を確認済みで、本セッションの
変更との衝突の兆候はない。

**真の問題（当初想定より深刻）**: `common/sec_data/ttm/`を生成する
`layer3_builder.py`（＋`quarterly.py`・`fact_selection.py`・
`q4_implied.py`）は、`parser.py`（annual_YYYY.json生成）とは**完全に
独立した別実装のパイプライン**であることを確認した。`layer3_builder.py`
は`parser.py`のクラス・関数を一切importせず、`fact_overrides.json`も
読み込まない。`parser.py`側の`_resolve_bs_entity_mixing()`・
`_backfill_total_liabilities_via_identity()`・
`_align_cost_of_revenue_to_revenue_period()`に相当する処理も存在しない。

結果、本セッションで実装した以下の修正は、**ワークフローが正常実行
されてもTTM系列には反映されない**（唯一の例外は
[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]。これは`ttm_calculator.py`
自体への実装のため次回実行で全105銘柄に自動反映される）:
- [[PERIOD-LENGTH-VALIDATION-GAP-1]]（28銘柄）
- [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1・2（7銘柄+SPIR）
- [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]（22銘柄278件、
  AMZN/GOOGL/MSFT/NVDA/AMD/WMT等の大型株含む）
- [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案b（LRCX）
- [[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]（GOOGL、`fact_overrides.json`
  自体が未読込のため）
- [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]（COHR、同上）
- [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]（ELF）

なお`layer3_builder.py`側は`gross_profit`逆算のみ独自に別実装済みで
（既存の別系統バグ追跡ID`[[LAYER3-GROSSPROFIT-BACKFILL-MISSING-1]]`、
annual側の`[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]`とは別系統
であることを確認済み）、全ての annual側修正が未移植というわけではない。

**結論**: 「ワークフローを動かせば陳腐化が解消する」という単純な話では
なく、今回実装した連続性チェック以外のannual側の修正は、たとえ
ワークフローが毎週正常に動いても恒久的にTTM側へは反映されない
（別途`layer3_builder.py`側への個別移植が必要）という、より根深い
構造的問題であることが判明した。

**対応方針の選択肢**:
1. 現状維持（cron待ち）: [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]の
   みが次回実行で全105銘柄に自動反映される。他は反映されないまま。
2. 手動トリガー（`workflow_dispatch`）: pushを伴うため明示的承認が必要。
3. `layer3_builder.py`側への個別移植: 範囲が大きく複数タスクへの分割が
   必要。
4. 影響の実測確認を先行: TANUKI VALUATION・STONKS SILOがTTM経由で
   未移植の修正対象フィールド・銘柄をどの程度消費しているか確認し、
   実害の大きさに応じて3の優先度を判断する。

#### 対応方針（前回時点）
④（影響の実測確認を先行）から着手する。範囲の大きい③（個別移植）に
いきなり着手する前に、実装前に実害を確認するという原則に基づき、実際に
どれだけの影響があるかをまず確認する。

#### 影響実測結果（2026-08-02、チャット記録、読み取りのみ）
7件の既知修正について、TANUKI VALUATION・STONKS SILOいずれも**現在
進行形の実害は確認されなかった**。

- [[SPAC-SHELL-BS-ENTITY-MIXING-1]]・[[TOTAL-LIABILITIES-FALLBACK-
  TAG-DESIGN-FLAW-1]]（AMZN/GOOGL/MSFT/NVDA/AMD/WMT等22銘柄278件）・
  [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]: 対象フィールド（BS項目・
  shares系）が構造的にTTM出力（`FLOW_FIELDS`17種のみ）に一切含まれない
  カテゴリであり、消費経路（`get_net_cash()`・`get_diluted_shares()`）も
  `annual_*.json`を直接参照するため無関係と確定。
- [[PERIOD-LENGTH-VALIDATION-GAP-1]]（28銘柄）・[[PL-FIELD-CROSS-ACCN-
  PERIOD-MISMATCH-1]]（LRCX）・[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]・
  [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]: 対象年度が現在のTTM系列
  anchor範囲（実測で2021〜2022年始まり）の外にあるため無関係。唯一の
  例外RCAT(2024年度)のstock_based_compensationも、FCF計算式
  （`_calc_fcf()`）に直接使われず、現状RCATのRICEスコア自体が「年次
  データ不足で計算不可」のため現時点で出力に無影響。
- STONKS SILOは独立した第3のパイプライン（`load_annual_data()`経由で
  `annual_*.json`を直接読み込み）であり、コード全体を検索してもTTM/
  layer3経由の参照が一切存在せず、実害はゼロと確定。

**重要な留保**: これは「今回はたまたま対象年度がTTM窓の外だった」結果
であり、2つの独立パイプラインが同期しない設計上の脆弱性自体は温存されて
いる。将来のannual側修正が、対象年度が現在のTTM窓内である場合には同様の
未反映リスクが顕在化しうる。

#### 対応方針
現在進行形の実害がゼロと確定したため、優先度を「高」から「中」に
引き下げる。ただし構造的脆弱性は残存するため、以下のいずれかの対応を
将来検討する:
- 短期的な運用対応: annual側で新規修正を行う際は、対象年度がTTM系列の
  anchor範囲内かどうかを都度確認し、範囲内の場合はlayer3_builder.py側
  への個別移植も検討するというチェック項目を、今後の実装依頼テンプレート
  に追加する
- 長期的な構造対応: layer3_builder.pyとparser.pyの重複ロジック
  （gross_profit逆算等）を統合する、またはannual側の修正結果をTTM側が
  参照する設計に変更する等、パイプライン統合自体の検討（大規模な設計
  変更のため別途独立検討が必要）

#### 長期的構造対応の検討結果（2026-08-03、チャット記録、読み取りのみ）
上記「長期的な構造対応」（パイプライン統合）の実現可能性を設計調査した。

**認識の訂正**: 当初「parser.py⇔layer3_builder.pyの2パイプライン問題」
としていたが、実際は`update.py`内で3つの独立生成パスが並存する構造
であり（①`parser.py`→`annual_*.json`、②`quarterly.py`→`normalizer.py`
→`normalized/*.json`、③`layer3_builder.py`→`ttm_calculator.py`→
`ttm/*.json`）、`SEC_EDGAR_LAYER_DESIGN.md`が既に「3スキーマ併存」として
認識済みの既知課題の一部だったと判明した。

**重複ロジックの棚卸し結果**: parser.py側の安全ロジック（本人データ優先・
BS系バックフィル・cost_of_revenue期間整合）の大半はTTM出力対象フィールド
（FLOW_FIELDS 17種）に該当しないBS/shares系であり、構造的に「移植する
意味自体がない」。真に問題になりうるスコープは「FLOW型フィールドに
関わる本人データ優先判定」のみという、当初想定より狭い範囲であることが
判明した。

**経緯の確認**: `layer3_builder.py`初出は2026-07-24（既存コード非改変
方針で新規構築）、parser.py側の安全ロジック追加は2026-08-01（本
セッション）。設計時点で後からparser.py側にこれらのロジックが追加
されることは想定されておらず、意図的な除外ではなく単純な時間差による
取り残されと確定した。

**選択肢の再評価**:
- 案A（完全統合）: layer3_builder.pyがparser.pyの共通ロジックを
  import・再利用する設計。新DB構築フェーズ相当の規模
- 案B（部分統合）: FLOW_FIELDS関連の本人データ優先ロジックのみ
  `fact_selection.py`へ追加。個別バグ修正1〜2件相当の規模
- 案C（運用チェック継続）: CHAT_RULES.md追記済みの現状（parser.py修正
  依頼作成時のTTM同期確認チェック）を維持

**推奨・対応方針**: 現時点は案C（運用チェック継続）を維持し、統合作業
（案A/B）には着手しない。根拠:
(a) 実害が実測でゼロと確定済み
(b) 真に問題になるスコープは当初想定より狭い（FLOW型フィールドの
    本人データ優先判定のみ）
(c) 3スキーマ併存自体は新DB構築プロジェクトのフェーズD（consumer切替）
    以降で本格的に扱われる射程の既存の中長期課題であり、前倒しの
    必然性が薄い
(d) gross_profit逆算のように既に個別重複が許容されている先例
    （[[LAYER3-GROSSPROFIT-BACKFILL-MISSING-1]]系）がある

#### 着手条件
以下いずれかのトリガー条件が発生するまで保留:
1. 今後の運用チェックでTTM anchor範囲内×FLOW型フィールドの修正が発生し
   実害が確認された場合 → 案B（部分統合）を個別タスクとして起票
2. 新DB構築プロジェクトのフェーズDに進む際、3スキーマ併存全体の解消を
   検討するタイミングで本件も合わせて設計する

---

### [SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1] BBAI/RKLB/SOFI/VRT/ONDSグループの「維持フィールド」の凍結検討
**優先度:** 低
**分類:** データ品質 / 将来検討事項
**登録日:** 2026-08-05
**発見:** [[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]] Stage 3準備調査（チャット記録）

#### 内容
[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1でBS項目をNone化・修正した
BBAI(2020)・RDW(2020)・RKLB(2020)・SOFI(2020)・VRT(2019)・ONDS(2017)の
6件は、None化されたフィールド自体（current_assets/current_liabilities/
long_term_debt/short_term_debt等）に「凍結すべき正しい値」が存在しない
ため、現行のfixed_registry.jsonスキーマでは登録不可と確定済み
（Stage 3調査、BACKLOG_DONE.md「2026-08-05（完了）」Stage 2エントリ
参照）。

一方、各銘柄でNone化されず**維持**されたフィールド（例: BBAIの
total_assets/stockholders_equity/total_liabilities/cash_and_equivalents）
は、`_resolve_bs_entity_mixing()`の数学的整合性チェック
（current_assets<=total_assets等）を通過済みであり、「誤った値をNone化
した」修正の裏返しとして「正しいと確認済みの値」というカテゴリに
位置づけられる可能性がある。

#### 影響
未確定。仮に凍結対象とする場合、Stage 1/2とは異なる「除外的検証
（誤りが混入していないことの消去法的確認）」という性質を持つため、
Stage 1/2の「積極的な値の検証」基準にそのまま当てはめてよいか設計判断が
必要。

#### 対応方針
未定。次回以降、余力があれば検討する将来課題。

#### 着手条件
なし（Stage 2/3の主要スコープ外、優先度低のため急ぎ着手しない）。

---

（[[SECDATA-COMPANYFACTS-OVERLOOKED-1]]は登録翌日の2026-07-24に
実質解消済みだったと判明、2026-08-16に移設漏れを訂正しBACKLOG_DONE.md
「2026-08-16（完了）」へ移設。参照）

---

（[[LIQUIDITY-CSV-FIRST-ROW-UNBOUNDLOCALERROR-1]]は2026-08-27実装完了、
BACKLOG_DONE.md「2026-08-27（完了）」参照）

---

（[[PORTFOLIO-CONFIG-DUP-1]]は2026-08-15実装完了、BACKLOG_DONE.md
「2026-08-15（完了）」参照）

---

（[[TAILKPI-CONFIG-LOCATION-1]]・[[FCFCONFIG-LOCATION-1]]は2026-08-15
実装完了、BACKLOG_DONE.md「2026-08-15（完了）」参照）

（[[FCFCONFIG-MISSING-DETECTION-WEAK-1]]は2026-08-15実装完了、
BACKLOG_DONE.md「2026-08-15（完了）」参照）

---

（[[EPSANALYZER-ADMIN-ORPHAN-PAGE-1]]は2026-08-15実装完了、
BACKLOG_DONE.md「2026-08-15（完了）」参照）

---

### [CONFIG-LOAD-SILENT-FALLBACK-1] config/設定ファイル読み込み失敗時のサイレントフォールバックが複数箇所に存在（残り3件）
**優先度:** 低
**分類:** データ品質 / 監視・検知
**登録日:** 2026-08-15
**発見:** `[[FCFCONFIG-MISSING-DETECTION-WEAK-1]]`実装中の観察事項

#### 内容（部分対応済み、2026-08-16）
当初7ファイルを対象に登録したが、悪質度「完全サイレント（ログ出力
なし）」の4件（`rpo_config.json`・`beta_config.json`・
`split_history.yaml`・統合対象の`fcf_conversion_config.json`）は
CHECK-34として実装完了（詳細はBACKLOG_DONE.md参照）。

**残り3件は未実装のまま本項目に残す**:

| ファイル | ローダー | ファイル不存在時のログ | フォールバック値の性質 |
|---|---|---|---|
| `config/prompts.yaml` | `ai_analyzer.py::load_prompt()` | `Warning:`あり | もっともらしい偽装データ（`DEFAULT_PROMPT`定数） |
| `config/maturity_config.json` | `maturity_config.py::_ensure_loaded()` | `[ERROR]`あり | デフォルトプロファイルのみ（銘柄別設定は全て失われる） |
| `config/segment_config.json`・`growth_options_config.json` | `segment_config.py::_load_json()` | `[ERROR]`あり | 空辞書 |

いずれも既に`[ERROR]`/`Warning`ログが出ており相対的に緊急性が低い。
特に`maturity_config.json`・`segment_config.json`はWACC/DCF計算コアに
直結し、変更時は全銘柄再生成による影響確認が必須になるため対応
コストが高い。

#### 対応方針
CHECK-34で確立したレジストリテーブル方式（`SYSTEM_MAP.md`
「config/読み込み失敗の横断検知」参照）を踏襲する。対象モジュールに
`resolve_*_path()`を切り出し、`report_consistency_check.py`の
`_CONFIG_LOADER_REGISTRY`に1エントリずつ追記すれば、汎用チェック関数
`_check_config_loaders_resolvable()`がそのまま対応する（新規CHECK
番号の採番は不要、CHECK-34のテーブルにエントリを追加するのみ）。

#### 着手条件
なし

---

（[[RISK-FREE-RATE-HARDCODE-1]]は2026-08-16調査完了、優先度「高」は
実態と乖離していたと判明し「低」へ訂正の上、現状維持〈対応不要〉で
BACKLOG_DONE.mdへ移設。詳細はBACKLOG_DONE.md「2026-08-16（完了）」
参照）

---

（[[OPERATING-INCOME-EXTRACTION-GAP-1]]は2026-08-16実装完了、
BACKLOG_DONE.md「2026-08-16（完了）」参照）

---

（[[MOAT-SCORE-PARTIAL-NULL-1]]は2026-08-16実装完了、
BACKLOG_DONE.md「2026-08-16（完了）」参照）

---

### [SCENARIO-BEARBULL-SIGN-FLIP-1] Bear/Bull成長率の符号が負の基準成長率で意図と逆転
**優先度:** 中（2026-08-16、高→中へ訂正。理由は下記「優先度訂正の経緯」参照）
**分類:** バグ / 設計上の欠陥 / TANUKI VALUATION
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ6（AS-IS-015、依頼文名指し）

#### 背景
`calculate_scenario_valuations()`（`calculator/scenarios.py:64-66`）は
`bear_rate=base_growth_rate×0.7`・`bull_rate=base_growth_rate×1.2`という
単純な乗算で構成される。`base_growth_rate`が正の値である前提では
「Bearは基準より控えめ・Bullは基準より強気」という意図通りに機能するが、
`base_growth_rate`が負の場合（例: -10%）はBear(-7%、実際は緩やかな下落=
楽観的)・Bull(-12%、実際は急な下落=悲観的)とラベルと実態が完全に逆転する。
現状は`growth_floor=0.15`による下限クリップ等により本番データでは顕在化
していないが、ロジック自体の欠陥は残る。同一の乗算ロジックは
`calculator/growth.py::get_scenario_growth_rates()`・`segment_config.py::
calculate_scenario_growth()`という2つの未使用デッドコードにも重複実装
されている。

#### 対応方針
`base_growth_rate`が負の場合の乗数を反転させる（例: Bearは`×1.3`、Bullは
`×0.8`とし、下落幅がBear>Bullになるよう補正する）等の修正方針を設計して
から実装する。使われていないデッドコード2箇所の削除も合わせて検討する。

#### 優先度訂正の経緯（2026-08-16）
案2 Step C（BACKLOG優先度中以上の棚卸し）で、本文の「#### 背景」欄に
「`growth_floor=0.15`による下限クリップ等により本番データでは顕在化
していない」と明記されているにもかかわらず優先度が「高」のまま維持
されている自己矛盾を発見した。`calculator/growth.py:142`の
`max(growth_floor, min(growth_cap, raw_cagr))`で下限クリップ（0.15）
が実在することをコードで確認済みで、本文の主張自体は正確。さらに
同一ロジックの重複実装2箇所は本文で明示的に「未使用デッドコード」と
されている。ロジック自体の欠陥という事実認識は変えないが、優先度
表記のみを「中」へ訂正する。

#### 着手条件
なし

---

## 優先度：未定（要判断）

### [FCF-CONVRATE-LOWER-DIVERGENCE-1] dr<1側29銘柄の構造的ミスマッチをFCF-CONVRATE②可視化に統合
**優先度:** 未定
**分類:** データ品質 / TANUKI VALUATION / FCF-CONVRATE②派生
**登録日:** 2026-07-20
**発見:** [[TRUST-SUMMARY-EPIC-1]]段階2再調査・divergence_warning閾値検証

#### 背景
`divergence_warning`はdr>=2.0のみ検知し、dr<1（過小推定）側は一切検知
しない非対称設計になっている。既存の2.0/5.0という閾値はgit調査の結果、
明確な根拠のない経験的な割り切り値と確認済み（導入コミット`3c12dd1b1`
2026-04-19「Add files via upload」、根拠記載なし）。

dr<1の銘柄はtanuki=true・fcf_estimation.applied=Trueの59銘柄中
**29銘柄（49%）**と多数存在する。サンプル5銘柄（LYFT/PAYS/FLYW/CSGP/
ZETA）を10-K等の一次情報で確認した結果、いずれも①raw_fcfの一過性な
水増しでも③Adj_NI側の異常な過小評価（バグ）でもなく、
**②conversion_rateの構造的ミスマッチ**（FCF-CONVRATE②と同型の性質）
と判明した。内訳は以下の通り異なるメカニズムに分解される：
- **決済/フロート型（LYFT/PAYS/FLYW）**: 事業モデル特有の運転資本
  タイミング（保険準備金・顧客資金float等）がOCFを体系的に押し上げる、
  SITM/LITEの「サイクル変動」とは別種の固定比率限界
- **意図的な成長投資による一時圧縮（CSGP）**: AMZN/LLY等で既に確認済みの
  「戦略的投資による収益圧縮」と同型パターン（CoStarはHomes.com投資を
  2026年に$3億削減予定と表明済みで、時間経過で正常化する見込み）
- **SBC比重の高い高成長企業のD&A非加算（ZETA）**:
  [[FCF-CONVRATE-DESIGN-LIMIT-1]]既知の残課題（Mature/SaaS判定精度
  約78%問題）と関連する可能性

#### 対応方針（未確定）
Policy B判定に新たな下限閾値を追加する方向（＝異常検知として扱う）
ではなく、既存のFCF-CONVRATE②可視化パターン（現在
`FCF_CYCLICAL_VOLATILITY_TICKERS`＝LITE・SITMのみ）を拡張し、構造的
限界として透明化する方向を推奨する。ただし残り29銘柄中、サンプル5件を
除く24銘柄は未確認のままであり、同様の一次情報確認が必要。

#### 着手条件
なし（残り24銘柄の一次情報確認を先行させることを推奨）

---

### [GROK-MODEL-PRICE-1] Grok呼び出しモデルの実価格確認
**優先度:** 低（xAI Console確認待ち）
**分類:** コスト管理 / 全体
**登録日:** 2026-07-05
**更新日:** 2026-09-10

#### 問題
daily_pick.py等で使用中の `grok-3-mini`/`grok-3`/`grok-2-1212` が
xAI現行価格表（docs.x.ai/developers/models）に存在せず、レガシーエイリアスとして
`grok-4.3`（$1.25/M入力・$2.50/M出力）へ自動ルーティングされ、想定（旧grok-3-mini想定
$0.30/M入力・$0.50/M出力）の4倍以上の価格で課金されている可能性がある
（UI-DISCOVER-1事前調査時に発見）。

#### 2026-09-10（1回目）対応済み: レガシーエイリアスのモデル名是正
呼び出し箇所9ファイル全てのモデル名を`grok-4.3`へ明示的に変更し、実際に
応答するモデルとコード上の表記を一致させた。全呼び出し箇所を実API経由で
動作確認済み。reasoning_tokens起因のJSON打ち切りリスクも実データ検証の
結果、対応不要と判断（既存の全呼び出し箇所が"JSON only"指示を持っており、
実測で打ち切りが発生しないことを確認）。

#### 2026-09-10（2回目）xAI Console実データ確認 → 想定と異なる結果が判明
Koichiさんがxai Consoleで直近7日間の支出を確認したところ、$1.65のうち
**$1.33（80%）が「grok-4.20-0309-reasoning」**というモデル名に帰属して
おり、1回目調査が想定していた`grok-3-mini`→`grok-4.3`の自動ルーティング
（残り$0.32）はむしろ少数派だった。追加調査の結果を以下に記録する。

**①grok-4.20-0309-reasoningの呼び出し元特定（推測ではなくコード確認）**
`grep -rln "grok-4.20-0309-reasoning"` で該当するのは
`src/value/adjusted_eps_analyzer/ai_analyzer.py`の1箇所のみ
（`XAI_MODEL = os.environ.get("XAI_MODEL", "grok-4.20-0309-reasoning")`、
環境変数未設定時のデフォルト値）。実API検証の結果、このモデル名は
grok-3系のような自動ルーティングを受けず、指定通り
`grok-4.20-0309-reasoning`として応答することを確認した（`grok-4.3`への
リダイレクトではなく、独立した現行の有効なモデル）。呼び出し元は
`pipeline.py`が`get_eps_tickers()`（99銘柄）をループし、各銘柄の
最新四半期についてのみ`analyze_adjustments()`を呼ぶ構造（過去四半期は
対象外、調整項目が0件のティッカーはAPI呼び出し自体をスキップ）。

**②9/6・9/7に集中した理由: `Adjusted_Eps_Analyzer_update.yml`が週1回のはず
が実質週2回発火する構造的バグ（新規発見）**
GitHub Actions実行履歴（API直接確認、`gh`コマンド不使用）で以下を確認:
```
2026-09-06T14:27:03Z  SEC Data Update          schedule   success
2026-09-06T14:29:19Z  Adjusted_EPS_Data_Update  workflow_run  success  (SEC Data Update完了の2分後)
2026-09-07T09:14:55Z  Adjusted_EPS_Data_Update  schedule      success  (月曜フォールバックcron)
```
同型パターンが直近3週連続（08-23/24・08-30/31・09-06/07）で発生。原因は
`.github/workflows/Adjusted_Eps_Analyzer_update.yml`（2026-08-22、
`[[WORKFLOW-SEC-TANUKI-GAP-1]]`対応コミット`ca925ffa27`で導入）の
`on:`設定:
```yaml
on:
  workflow_run: {workflows: ["SEC Data Update"], types: [completed]}
  schedule: [{cron: '10 4 * * 1'}]  # コメント上は「安全網」の想定
jobs:
  update:
    if: github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success'
```
コメントでは月曜cronを「workflow_run連鎖が発火しなかった場合の安全網」と
説明しているが、`if`条件はworkflow_runイベント自体にしか効かず、
**scheduleイベント側は「workflow_runが既にその週成功済みか」を一切
判定できない**ため、workflow_run連鎖が成功した週でも月曜cronは無条件に
追加実行される。結果として、99銘柄分のGrok呼び出し（調整項目ありの
銘柄のみ、内容は前日と実質同一）が週2回発生している。
実行履歴で唯一の例外は08-23（SEC Data Update失敗→workflow_run側は
`skipped`）→08-24（cronのみ成功）で、この週だけは設計意図通りの
「真の安全網」として機能していた。つまり本バグは「SEC Data Updateが
失敗した週は正しく安全網として機能するが、成功した週は毎回無条件に
二重実行される」という構造。本件は新規`[[WORKFLOW-FALLBACK-CRON-
DUPLICATE-1]]`として別途登録した（同型の`workflow_run`+週次フォール
バックcronパターンは`TANUKI_VALUATION_Update.yml`・`TANUKI_Score_
Update.yml`にも存在するため、横展開調査が必要）。

**③grok-4.3の「X searches」とrisk_fetcher/Discover削除の関連（一部訂正）**
risk_fetcher.py・Discoverサブシステム（2026-09-01削除）のGrok呼び出し
コードを削除前のgit履歴で直接確認した結果、**いずれも`search_parameters`
（xAI Live Search機能の明示的な有効化パラメータ）を指定していなかった**
ことが判明した。実は両方とも2026-05-23の別コミット
（`899858a5db`・`445a78331f`、「Grok API呼び出し方式を修正」）で
`search_parameters: {"mode": "on"}`が既に削除済みだった（当時は
エラー解消目的の修正で、コスト目的ではない）。実API検証でも、
`search_parameters`なしでの「web検索して」という指示プロンプトは
`num_sources_used: 0`（実際には検索を行わない）ことを確認した。
よって**「X searches」課金の直接origin ではない**というのが訂正点。

ただし、risk_fetcher.py・Discoverはいずれも高頻度・高銘柄数の
legacy-aliasモデル呼び出し元だった（Discoverは`Discover_Update.yml`で
**毎日**実行、多数ティッカーのニュース分類・カタリスト抽出を実施）。
これらの削除により`grok-4.3`（旧エイリアス経由）への総呼び出し回数が
劇的に減少したことが、2026-09-01を境に日次支出がほぼゼロに落ちた
直接の原因と考えられる（「X searches」という個別費目自体の消滅では
なく、grok-4.3の呼び出し総量そのものが激減したため）。
`search_parameters`を現在も明示的に使っている呼び出し元は
`src/tail/satellite_monitor.py`（`TANUKI_TAIL_Satellite_Monitor.yml`、
平日2回/日）のみで、これが引き続き「X searches」費目の実際の発生源と
みられる（削除前後で継続稼働しており、9/1以降の残存$0.32/7日の
出どころとして矛盾しない）。

**④現時点でGrokを呼び出している箇所の完全な一覧**

| ファイル | モデル | トリガー | search_parameters |
|---|---|---|---|
| `src/market/macro_pulse/05_main.py`（2箇所） | grok-4.3 | `MACRO_PULSE_Update.yml`（毎日+週次） | なし |
| `src/market/market_pulse/collect_and_send.py` | grok-4.3 | `Market_Pulse_Update.yml` | なし |
| `src/tail/kpi_proposer.py` | grok-4.3 | 該当workflowなし（手動実行想定） | なし |
| `src/tail/quarterly_review_generator.py` | grok-4.3 | `TANUKI_TAIL_RSS_Monitor.yml`（平日毎日） | なし |
| `src/tail/satellite_monitor.py` | grok-4.3 | `TANUKI_TAIL_Satellite_Monitor.yml`（平日2回/日） | **あり**（"X searches"の実源） |
| `src/tail/sec_ctrl_fetcher.py` | grok-4.3 | `TANUKI_TAIL_SEC_Ctrl.yml`（毎週月曜） | なし |
| `src/tail/text_kpi_extractor.py` | grok-4.3 | `TANUKI_TAIL_RSS_Monitor.yml`（平日毎日） | なし |
| `src/value/tanuki_score/daily_pick.py` | grok-4.3 | `TANUKI_Score_Update.yml`（workflow_run連鎖+週末独立cron） | なし |
| `src/value/tanuki_valuation/validator.py` | grok-4.3 | `TANUKI_VALUATION_Update.yml`（workflow_run連鎖・4系統いずれか完了で発火+金曜安全網cron） | なし |
| `src/value/adjusted_eps_analyzer/ai_analyzer.py` | **grok-4.20-0309-reasoning**（grok-4.3とは別モデル、非エイリアス） | `Adjusted_Eps_Analyzer_update.yml`（99銘柄ループ、**週2回発火バグあり**） | なし |

#### 残課題（Koichiさん本人のみ対応可能）
xAI Console（https://console.x.ai/ 等の管理画面）で実際の請求明細・
モデル別単価を確認すること。本セッションはKoichiさんのアカウントに
アクセスできないため対象外。`grok-4.3`は$1.25/M入力・$2.50/M出力
（旧`grok-3-mini`想定単価の4倍以上）。`grok-4.20-0309-reasoning`の
単価はxAI価格表に個別記載がなく未確認（reasoning系モデルは一般に
プレミアム価格帯のため、Console上の実額での確認が必要）。
`[[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]`の対応（週2回発火の解消）に
より`ai_analyzer.py`分のコストは理論上半減が見込まれる。

---

### [WORKFLOW-FALLBACK-CRON-DUPLICATE-1] workflow_run連鎖の週次フォールバックcronが「安全網」ではなく毎週無条件に二重実行している
**優先度:** 中（実コスト・API呼び出し重複の実害が実行履歴で確認済み）
**分類:** インフラ / GitHub Actions / コスト管理
**登録日:** 2026-09-10
**発見:** `[[GROK-MODEL-PRICE-1]]`のgrok-4.20-0309-reasoning呼び出し元調査
（xAI Console実データで9/6・9/7に支出が集中していた理由の特定）

#### 内容
`[[WORKFLOW-SEC-TANUKI-GAP-1]]`対応（2026-08-22、commit `ca925ffa27`）で
`Adjusted_Eps_Analyzer_update.yml`・`TANUKI_VALUATION_Update.yml`・
`TANUKI_Score_Update.yml`の3ワークフローに「`workflow_run`連鎖 +
週1回の低頻度フォールバックcron（`workflow_run`が発火しなかった場合の
安全網、という触れ込み）」パターンが導入された。しかし実際のjob-level
`if`条件（例: `if: github.event_name != 'workflow_run' ||
github.event.workflow_run.conclusion == 'success'`）は「scheduleイベント
実行時にworkflow_runが同じ週に既に成功しているか」を判定する手段を
持たない構造的な設計であり、GitHub Actions単体でもこの種の週内条件は
表現できない。結果として、workflow_run連鎖が正常に成功した週でも、
フォールバックcronは無条件に追加実行される。

#### 実測（Adjusted_Eps_Analyzer_update.yml、GitHub Actions API直接確認）
```
2026-08-23  SEC Data Update失敗 → workflow_run側 skipped（正しい安全網動作）
2026-08-24  schedule成功                            （唯一の正常フォールバック）
2026-08-30  workflow_run成功
2026-08-31  schedule成功                            （＝無条件の重複実行）
2026-09-06  workflow_run成功
2026-09-07  schedule成功                            （＝無条件の重複実行）
```
直近3週のうち2週（08-30/31・09-06/07）で無条件重複を確認。`pipeline.py`
に冪等性ガード（同一データでの再実行をスキップする仕組み）が存在しない
ため、99銘柄分の`ai_analyzer.py`（Grok）呼び出しが実質週2回発生して
いる。同型パターンの`TANUKI_VALUATION_Update.yml`
（`workflow_run`が4系統いずれかの完了で発火する設計のため、週内の
重複発火頻度はさらに高い可能性）・`TANUKI_Score_Update.yml`は本調査の
スコープ外（`Adjusted_Eps_Analyzer_update.yml`のみ実行履歴で確認）の
ため、横展開調査が必要。

#### 対応方針（未定・要判断）
以下のいずれかの設計変更が必要:
1. フォールバックcronを削除し、`workflow_run`連鎖の失敗を別の手段
   （Slack/Discord通知等）で検知する運用に切り替える
2. フォールバックcron実行時に、直近成果物のタイムスタンプ等から
   「今週既に更新済みか」を判定し、既に最新なら早期returnする
   冪等性ガードをpipeline.py側に追加する
3. 各パイプライン側に「入力データが前回実行から変化していなければ
   Grok呼び出し自体をスキップする」キャッシュ機構を設ける

いずれも設計判断を伴うため対応方針は保留。`[[WORKFLOW-SEC-TANUKI-
GAP-1]]`のクローズ時（2026-08-31）は「workflow_run連鎖が発火すること」
のみを実地確認しており、フォールバックcronとの重複可能性は検証範囲外
だった。

#### 着手条件
なし（Koichiさんの判断待ち。`TANUKI_VALUATION_Update.yml`・
`TANUKI_Score_Update.yml`への同型バグの横展開調査も含めて次回対応）

---

### [FCF-OUTLIER-QUAL-1] 一過性費用の説明妥当性に関する定性評価の導入
**優先度:** 未定（要判断）
**分類:** データ品質 / TANUKI VALUATION / AI活用
**登録日:** 2026-07-11
**発見:** [[DCF-REL-SYNC-1]]（完了・BACKLOG_DONE.md参照）実装検討時

#### 背景
`fcf_outlier.action`（`excluded`/`flagged`）は、一過性費用の金額が
FCF乖離の一定割合（20%等）を占めるかという**金額比率のみ**で機械的に
判定されており、その一過性費用の内容自体が乖離を実質的に説明しているか
（例: 事業の一時的な問題か、構造的な問題の兆候か）は評価されていない。
`transient_evidence.items`には一過性費用の個別項目（内容の記述含む）が
既に格納されているため、これをAI（`quarterly_review_generator.py`等の
既存の定性評価の仕組みを参考に）に評価させ、action判定に反映する、
または別フィールドとして表示する設計が考えられる。

#### 対応方針（未確定・検討課題）
- 案A: action判定自体にAI定性評価を組み込む（判定ロジックの複雑化に注意）
- 案B: 定性評価は別フィールド（例: `transient_evidence.ai_assessment`）として
  追加し、report.txtに参考情報として表示するのみでaction判定は変えない
  （既存の機械的判定はそのまま維持し、人間の最終判断材料を増やす方向）
- 案Bの方がリスクが低く、[[DCF-REL-SYNC-1]]（完了・BACKLOG_DONE.md参照）のスコープとも独立して着手しやすい

#### 着手条件
なし（設計判断が必要なため、次回セッションで方針確定してから着手）

---

### [MA-INTEGRATION-TAG-GAP-1] adjustment_items.jsonのma_integration項目がXBRLタグ不足、TRANSIENT_CATEGORIESからも除外され実キャッシュM&A費用を検出漏れ
**優先度:** 未定（全銘柄への影響範囲を網羅調査した上で確定）
**分類:** データ品質 / 一過性費用検出
**登録日:** 2026-07-14
**発見:** [[TRANSIENT-EXPENSE-COVERAGE-1]]（完了・BACKLOG_DONE.md参照）（AVAV/RDW調査）の副次発見

#### 内容
AVAV・RDWの一過性費用検出漏れを10-K原文で調査した結果、両社とも
`config/adjustment_items.json`のma_integration項目（買収・統合関連
カテゴリ）が実際に両社が使用するXBRLタグを拾えていないことが判明した：

1. **タグ不足**: ma_integration項目のxbrl_tagsが
   `us-gaap:BusinessCombinationIntegrationCosts`のみで、以下が未登録：
   - `us-gaap:BusinessCombinationAcquisitionRelatedCosts`
     （AVAV FY2026: $48.17M、RDW FY2025: $21.24M で実際に使用）
   - `us-gaap:BusinessCombinationIntegrationRelatedCosts`
     （RDW FY2025: $1.14M、登録タグと"Related"の有無のみ相違）

2. **カテゴリ設計の問題**: `calculator/adjustments.py:772-776`の
   `TRANSIENT_CATEGORIES = {"リストラ・事業再編関連", "在庫・サプライチェーン
   関連", "金融関連"}`から「買収・統合関連」カテゴリ自体が丸ごと除外されて
   いる。これはのれん減損・無形資産償却等の非現金項目を除外する意図の
   設計だが、ma_integration（実キャッシュ支出項目）も同一カテゴリに
   含まれているため巻き込まれて除外されている

AVAV・RDW自体は悪化の主因が別（運転資本変動）だったため実害は軽微
だったが、M&A取引費用そのものが悪化の主因になる別の銘柄では実害が
大きくなる可能性がある。

#### 調査要望（着手時）
全105銘柄でM&A関連XBRLタグ（`BusinessCombinationAcquisitionRelatedCosts`等、
未登録タグ候補を含む）の申告有無・金額を洗い出し、上記2つの穴
（タグ不足・カテゴリ除外設計）の影響を受けている銘柄が他にないか
網羅的に確認すること。

#### 状況追記（2026-07-20・全105銘柄タグ影響網羅調査完了）
機械スキャンの結果、未登録2タグを過去に一度でも申告した銘柄は34件、
うち現在（EPS Analyzer最新年度）も申告中は**17件**（過去申告のみで
現在は無関係な17件を除外済み）。

現在も申告中の17件を、CWAN-SNPS-MA-DISTORTION-1控除メカニズムへの
影響で3グループに分類した：
- **グループA（メカニズム未到達・無関係、6件）**: AVAV/BBAI/COHR/RDW
  （`fcf_outlier.action=="excluded"`によりガードAで早期リターンし
  控除ロジック自体に到達しない）・CART（該当額$0）・VZ（別要因で
  `applied=False`）
- **グループB（タグ追加で改善方向、dr>1が是正される、4件）**: SNPS
  （1.34→1.18）・SITM（2.41→2.14）・NOW（1.54→1.50）・AVGO
  （1.15→1.14、ほぼ無視できる）
- **グループC（タグ追加で悪化方向、dr<1が更に悪化する、4件）**: CSGP
  （0.52→0.28、明確に悪化）・ZETA（0.66→0.53）・AMD（0.52→0.51、
  無視できる）・HQY（0.52→0.51、無視できる）

**単純なタグ追加は改善（グループB）と悪化（グループC）がほぼ相殺し、
net効果は中立に近いと判明**。控除メカニズムはdr>1（過大推定の是正）を
前提に設計されており、dr<1の銘柄に同じ控除を機械的に適用する妥当性は
別途検証が必要（詳細は[[FCF-EST-DIRECTION-GUARD-1]]参照）。タグ追加
単独での対応は非推奨、方向性ガード実装を優先すべきと判断する。

`TRANSIENT_CATEGORIES`からの「買収・統合関連」除外設計（カテゴリ除外
問題）についても検証した。技術的にはitem_id単位の分離が
`adjustment_items.json`のスキーマ変更なしで実現可能と確認したが、
現在flagged状態の対象銘柄は全て上方乖離（FCF-OUTLIER-1ルールにより
一過性費用の有無に関わらずaction=flaggedのまま変化しない設計）のため、
**現在の母集団では実害ゼロ**と確認した。優先度は低のまま据え置く。

#### 状況追記（2026-07-20・[[FCF-EST-DIRECTION-GUARD-1]]完了により
グループC懸念が構造的に解消）
方向性ガード実装（コミット`3b413b849`）により、控除前Adj_NIベースの
`pre_deduction_dr<=1.0`の銘柄には控除自体が適用されなくなった。これにより
上記グループC（タグ追加で悪化方向、CSGP/ZETA/AMD/HQY）の懸念は構造的に
解消されている見込みである——タグ追加後に新たに`pre_deduction_dr<=1`と
判定される銘柄は、ガードにより自動的に控除対象から除外されるため、
「タグ追加で改善と悪化が相殺する」問題自体が発生しないはずである。
ただし本追記はガードの設計上の帰結からの推論であり、実際にタグを
追加した場合の正式な全母集団再検証は次回タグ追加検討時に行うこと。

#### 状況追記（2026-07-20・正式再検証の結果、上記「構造的解消」は
一部誤りと判明）
実関数呼び出しによる正式な全母集団再検証の結果、上記の推論は
**AMD・HQYの2銘柄では正しかったが、CSGP・ZETAの2銘柄では成立しない**
ことが判明した。CSGP・ZETAは現在のpre_dr（0.96・0.95）が1.0のすぐ下に
あり、タグ追加によるAdj_NI増分（新タグの税引後net_amount）だけで
pre_drが1.0を超えてしまう「境界の跳ね返り」が実データで確認された
（CSGP: dr 0.96→0.45、ZETA: dr 0.95→0.63、いずれも悪化）。ガードは
「pre_drの計算に用いるadj_net_income_orig自体が変化する」ケースまでは
防げないため、タグ追加時にAdj_NI自体を押し上げる副作用がある限り、
境界近傍銘柄の跳ね返りリスクは残る。

**副次発見・別件として対応済み**: 上記の精査過程で、`ma_addback`計算が
税引前`amount`を合算していたのに対し、`adjusted_net_income`自体は
EPS Analyzer側で税引後`net_amount`ベースで構築されているという基準
不一致（CWAN-SNPS-MA-DISTORTION-1実装当初からの系統的な過剰是正バグ、
本件の境界跳ね返りリスクとは独立の問題）を発見し、`net_amount`基準へ
統一する修正を実施済み（コミット`3d434c29d`）。ガード許可中51銘柄中
25銘柄で実質的な変化（全件est_fcf増加方向、悪化ゼロ）。IV変化幅
+0.01%（GOOGL）〜+17.11%（CWAN、想定通り0.88倍→1.15倍相当に是正）。
Classification変化は0件。この修正はpre_deduction_dr・ガード判定自体には
影響しないため、上記の境界跳ね返りリスクの解消には寄与しない（別軸の
独立した問題であることを確認済み）。

**本タスク本体（未登録タグ追加の要否）は引き続き判断保留**。境界跳ね返り
リスクへの対応方針として、CSGP・ZETAを固定除外リストで弾く案は
CHAT_RULES.md「銘柄固定のハードコード禁止」原則（機械的に判定可能な
条件がないか5回以上検討する）に反するため不採用とする。次回検討時は
①根本原因（pre_dr=1.0近傍の境界脆弱性）そのものへの対応、②境界近傍銘柄
（pre_dr 0.9〜1.1、現在17銘柄該当: INTU/ADBE/NET/CPRT/META/CSGP/ESTC/
MSFT/ZETA/ADSK/SCCO/FROG/PM/WST/RMBS/ENTG/KLAC）を機械的に検知し
タグ追加前に個別確認する運用、のいずれかを検討する。

#### 長期停滞の原因分析・新しい設計角度の提案（2026-09-10、67件総点検の
一環。実装はせず分析・提案の記録のみ）
本エントリは2026-07-14登録以来2ヶ月近く「未定」のまま進んでいない。
停滞の原因は単純な見落としではなく、①タグ追加単独では改善
（グループB）と悪化（グループC）がほぼ相殺し正味の実利が薄いこと、
②`[[FCF-EST-DIRECTION-GUARD-1]]`実装後もCSGP/ZETAで「境界の跳ね返り」
（タグ追加による`pre_deduction_dr`のわずかな変化が0.96→0.45のような
大きなIV変化を引き起こす）という副作用が残ることの2点が、実質的な
着手障壁になっている、という**本物の設計上の難しさ**であることを
確認した。

**根本原因の再確認**（`calculator/adjustments.py:1768-1781`実コード）:
現行実装は`pre_deduction_dr > 1.0`という**二値（binary）ゲート**で
`ma_addback`（控除額）を「全額適用」か「全く適用しない」かの二択で
切り替える設計になっている。この二値性自体が「境界のすぐ近くにいる
銘柄では、入力のごく小さな変化（タグ追加によるAdj_NIの微増）が
出力（控除額）を0→満額へ不連続にジャンプさせる」という構造的な
脆さの直接の原因である。CSGP/ZETAの「跳ね返り」は、この意味で
偶然ではなく二値ゲート設計の必然的な帰結だった。

**新しい設計角度の提案（未実装・次回検討材料）**: 二値ゲートを
「`pre_deduction_dr`が1.0を超えた度合いに応じて控除額を連続的に
スケールする」設計に変更する案（ヒステリシス/ランプ関数方式）を
検討する価値がある。例:
```
band = 0.10  # 境界帯の幅（要検討・実データで校正）
deduction_fraction = clamp((pre_deduction_dr - 1.0) / band, 0.0, 1.0)
ma_addback_applied = ma_addback * deduction_fraction
```
この設計であれば、CSGP（0.96→タグ追加後1.02程度と推定）のような
境界をわずかに超えるケースは控除額のごく一部（例: 上記bandなら
約20%）のみが適用され、dr 0.96→0.45のような急変ではなく緩やかな
変化になる。一方、明確にdr>1.0+band（過大推定が確定的）な銘柄は
従来通り全額控除が適用され、既存の判定結果（COHR等）は変えない設計
にできる見込み。band幅の校正・全105銘柄での境界通過ケースの有無の
確認・`[[FCF-EST-DIRECTION-GUARD-1]]`との相互作用再検証が必要なため、
実装前に必ず全母集団シミュレーションを行うこと。

**位置づけ**: これは「本タスクを止めている根本原因への対応」（上記
選択肢①）の具体案の一つであり、選択肢②（境界近傍17銘柄の運用監視）を
置き換えるものではなく併用しうる。二値ゲート自体の設計変更はコア
バリュエーション計算に触れるため、着手する場合はKoichiさんの承認を
得た上で、全105銘柄再生成・IV変化幅の実測を伴う独立タスクとして扱う
こと（本項目では分析・提案のみに留め、コード変更は行っていない）。

#### 着手条件
なし（上記の新しい設計角度を含め、着手要否・優先順位はKoichiさんの
判断を仰いでから）

---

### [FCF-CONVRATE-DESIGN-LIMIT-1] SECTOR-FCF-RATE-BROKEN-1修正後もLITEの業種カテゴリ欠落・固定比率設計の限界が残存
**優先度:** 未定（2026-07-14 6/8カテゴリのキー名不一致修正・
Software_Systemグループ分割は完了。残るIOT等判定保留5銘柄・暫定判定
精度の2課題は再評価待ち。LITE/SITMカテゴリ欠落・固定比率設計の限界・
EBIT(1-t)→純利益変換ロジック不在の3課題は2026-07-15
TRUST-SUMMARY-EPIC-1へ統合済み）
**分類:** DCF信頼性判定ロジック / データ推定
**登録日:** 2026-07-14
**発見:** [[FCF-EPS-CONVRATE-SECTOR-1]]（完了・BACKLOG_DONE.md参照）（LITE/SITM）調査時

#### 内容
SECTOR-FCF-RATE-BROKEN-1（sector取得経路のバグ）を修正しても、
以下2点はLITE/SITMのconversion_rate精度改善には不十分であることが判明：

1. **LITEに対応するfcf_conversion_config.jsonの業種カテゴリが存在しない**
   （Software_Internet/AdTech_Internet/Semiconductor/Cloud_Services/
   EV_Automotive/Fintech/Consumer_Beverage/Space_Defenseの8分類中、
   光学部品・通信機器ハードウェア製造業に該当するものがなく、
   sector修正後もdefaultに留まる可能性が高い）

2. **固定比率という設計自体がサイクル変動の大きい銘柄を表現できない**
   （SITMの実質転換率は年により0.065倍〜3.65倍と振れており、
   どの固定値を設定しても大幅乖離は解消しない構造的限界）

代替経路として`growth_sanity.py`のdamodaran_industry判定
（Damodaranデータ＋銘柄別手動マッピング辞書）も確認したが、
LITE・SITMともに未登録（damodaran_industry=None）でこちらも
追加整備が必要。

#### 追加調査で判明した問題（2026-07-14 調査・キー名不一致）
上記1の再評価のため`estimate_fcf_from_eps()`の基準（純利益ベースか
EBIT(1-t)ベースか）を確認する調査を実施した際、当初想定と異なる
より根の深い問題が判明した：**現行8カテゴリのキー名
（EV_Automotive/Fintech/Consumer_Beverage/Space_Defense/
AdTech_Internet/Cloud_Services）が、実際に`config/beta_config.json`
の`overrides.<TICKER>.sector`へ書き込まれるDamodaran taxonomy準拠の
表記（Auto_Truck/Financial_NonBank/Beverage_Soft/Aerospace_Defense等）
と文字列不一致だったため、Software_Internet（3銘柄）・Semiconductor
（7銘柄）を除く6カテゴリは該当銘柄0件＝事実上デッドコードだった**
（tanuki=true全100銘柄で実測。TSLA/LMT/KO/PEP/SOFI/V/MSCI等、本来
非defaultレートが適用されるべきだった銘柄が軒並みdefault(0.70)に
落ちていた）。ダモドラン公式データセット（oifcff.xls、94業種）との
突合では、現行8カテゴリは全て単一のダモドラン業種への1:1対応で
あり、複数業種の平均・統合ではないことも確認した。

#### 対応内容（2026-07-14 実装完了・キー名リネームのみ）
`fcf_conversion_config.json`の`sector_conversion_rates`・
`_sector_rationale`のキー名を、`growth_sanity.py::SECTOR_TO_DAMODARAN`
（既存の正式なDamodaran業種マッピング辞書）を用いて実際の
beta_config.json sector表記に一致させてリネームした（**転換率の数値は
一切変更していない**）：
- `EV_Automotive` → `Auto_Truck`（0.65のまま）
- `Space_Defense` → `Aerospace_Defense`（0.55のまま）
- `Consumer_Beverage` → `Beverage_Soft`（0.75のまま）
- `Fintech` → `Financial_NonBank`（0.50のまま）
- `AdTech_Internet` → `Advertising`（0.88のまま。該当銘柄0件のため
  SECTOR_TO_DAMODARANの逆引きで本来の表記に合わせた）
- `Cloud_Services` → `Software_System`（0.80のまま。同様に逆引き。
  結果としてADBE/CRM/NOW/PLTR/INTU/DDOG等23銘柄の広範な
  エンタープライズソフトウェア群が対象になった。この23銘柄は
  Azure型クラウドインフラ企業を想定した`Software_System`の
  _sector_rationale説明文とは事業特性が幅広く異なる可能性があり、
  レート0.80の妥当性自体は未検証のまま——次項の残課題参照）
- `Software_Internet`・`Semiconductor`は元々一致していたため変更なし

全105銘柄（tanuki=true 100銘柄）で新旧比較を実施し、38銘柄で
新たにdefault以外のconversion_rateが適用されるようになったことを
確認（TSLA→0.65、LMT/AVAV/HEI/HWM/LOAR/RDW→0.55、KO/PEP/CELH→0.75、
SOFI/V/MSCI/FLYW/PAYS→0.50、ADBE/ADSK/APP/BSY/CDNS/CRM/CWAN/DDOG/
ESTC/FICO/FROG/FRSH/GTLB/INTU/IOT/NOW/PLTR/QBTS/RBRK/S/SNPS/SOUN/
ZETA→0.80の23銘柄）。Software_Internet・Semiconductor該当銘柄
（10銘柄）およびticker_overrides銘柄（AMZN/GOOGL/MSFT/META/MRVL/CEG）
の計15銘柄と、元々どの分類にも該当しない残り47銘柄には差分が
発生していないことを確認した。pytest: 309 passed / 2 failed
（MSFT/NVDA、[[TEST-STALE-IV-1]]の既知バグで本修正とは無関係）。

**注意**: 今回の修正は`fcf_conversion_config.json`のキー名変更のみ。
影響を受ける38銘柄の`latest.json`/`report.txt`（conversion_rateが
反映されたIV）は未再生成であり、次回`pipeline.py`実行まで
生成済みデータとコードの間に不整合が残る（コミット・本番反映方針は
別途判断が必要）。

#### Software_Systemグループ分割 実装完了（2026-07-14）
残課題4（旧: `Software_System`統合カテゴリのAzure型想定レート0.80が
エンタープライズソフトウェア全般23銘柄に妥当か未検証）に対応するため、
23銘柄の実績データ（生FCF/調整済み純利益比率、直近5年）を検証した結果、
成熟ライセンス/プラットフォーム型（グループA、平均比率≈1.00）と
サブスクリプション型SaaS（グループB、平均比率≈1.61、NOWの一時的DTA
歪み除外後）の二極化を確認（相関検証・前受収益比率での分離可能性も
調査済み。分離精度は約78%）。

**実装内容:**
1. `fcf_conversion_config.json`に`Software_System_Mature`(1.00)・
   `Software_System_SaaS`(1.61)を新設。`Software_System`(0.80)キー自体は
   判定保留銘柄向けに残置（削除していない）
2. `config/beta_config.json`の18銘柄のsectorを更新:
   - グループA→`Software_System_Mature`: SNPS/ZETA/FICO/CDNS/APP/ADSK/
     PLTR/ADBE/INTU
   - グループB→`Software_System_SaaS`: BSY/DDOG/CWAN/CRM/FRSH/GTLB/
     FROG/NOW/ESTC
   - IOT（サンプル1件のみ）・QBTS/RBRK/S/SOUN（常時赤字で判定不能）・
     MSFT（ticker_override適用のため無関係）は`Software_System`のまま
     据え置き
3. `growth_sanity.py::SECTOR_TO_DAMODARAN`に新sector値2件を追加
   （両方とも既存と同じ"Software (System & Application)"を指す）。
   sectorリネームがgrowth_sanity側の成長率ベンチマーク参照
   （damodaran_industry）を壊さないための必須の付随対応
4. `calculator/adjustments.py::check_software_system_reclassification()`
   を新設。determine_fcf_base()と同じ設計思想（config書き換えなし、
   pipeline.py実行のたびに実績から純関数として再判定）で、実測FCF/
   調整済み純利益比率が現在のサブグループのレートから30%以上乖離、
   かつもう一方のレートの方が近い場合にこの実行に限り推奨レートへ
   差し替え、report.txtに見直し推奨を表示する
5. `beta_fetcher.py::classify_software_system_subgroup()`を新設
   （`--classify-software-system`オプション）。新規銘柄でsectorが
   `Software_System`（未分類）の場合、前受収益（Deferred Revenue/
   Contract Liability）/売上高比率（company_facts.jsonから算出）で
   0.40を閾値にMature/SaaSを暫定分類する。0.30〜0.50の境界近傍は
   report.txtに要確認フラグを表示
6. `CLAUDE_CODE_START.md`の新規銘柄登録手順にStep 2.5として追記

**検証:**
- 全105銘柄（tanuki=true 100銘柄）で新旧比較を実施し、意図した18銘柄
  （グループA9銘柄→1.00、グループB9銘柄→1.61）のみが変化し、
  IOT/QBTS/RBRK/S/SOUN/MSFTを含む残り82銘柄には差分がないことを確認
- pytest: 309 passed / 2 failed（MSFT/NVDA、[[TEST-STALE-IV-1]]の
  既知バグで本修正とは無関係）
- 影響18銘柄の`pipeline.py`再実行（`--skip-risk`）: 成功18/失敗0
- `report_consistency_check.py`: NG=0、警告36件は全て既存確認済み
  （新規0件）
- 自己補正チェック（`check_software_system_reclassification`）を
  18銘柄全件で検証し、いずれも見直し推奨が発火しないことを確認
  （実績データから直接分類した銘柄なので当然の結果。初期実装では
  現分類との乖離幅のみで判定しもう一方のレートとの近さを見ていなかった
  ため、ESTC（実測比率2.21、SaaS想定1.61からの乖離+37%）で
  「より遠いはずのMature(1.00)へ切替」という誤判定を検出・修正済み）

#### 発見した別問題（未対応・要別途調査）
本タスクの影響18銘柄再生成時、`FRSH`の内部検証（`validation.overall`）が
FAILになった。原因調査の結果、**本修正とは無関係の既存バグ**と判明:
`validator.py::run_basic_checks`の`pt_shares_consistency`チェックが
再計算する理論株価（例: FRSH $127.83、ADBE $1153.85）と、最終的に
`latest.json`へ保存される`intrinsic_value_per_share`（FRSH $41.47、
ADBE $639.89）が大きく乖離しており（ADBEは本修正前のHEAD時点データでも
乖離80.40%で再現、pass=False・overall=WARN）、pipeline.py内で検証時点の
IVスナップショットと最終保存IVの間に何らかの後段調整（alphaテーパリング等の
候補）が挟まっていると推測される。乖離幅が閾値（±1000%）を超えたのは
FRSHが初めてで、`report_consistency_check.py`のNG判定には含まれず
実害はNG=0のまま維持されているが、`validation.overall`の信頼性に
関わる別バグとして新規登録が必要（本タスクでは未調査・未修正）。
**2026-07-15完了の[[VALIDATOR-IVPS-MISMATCH-1]]（BACKLOG_DONE.md参照）
で解消済み**。

#### 残課題（クローズしない）
4. **IOT・QBTS/RBRK/S/SOUNの判定保留**: IOTはDR/Rev比率0.50で境界上
   （サンプル不足）、QBTS/RBRK/S/SOUNは観測期間中一貫して調整済み
   純利益が赤字のためMature/SaaS判定の前提が成立しない。いずれも
   `Software_System`(0.80)のまま据え置き
5. 前受収益比率による新規銘柄暫定判定の分離精度は約78%（実績検証済み
   18銘柄ベース）。ADSK/BSY/CWAN型（DR/Rev比率と実際のFCF転換挙動が
   逆相関する例外）が一定数存在するため、暫定判定はあくまで初期値
   であり、`check_software_system_reclassification()`による実績ベース
   の自動見直しに委ねる設計

**移設注記（2026-07-15）**: 上記のうち残課題2（固定比率という設計自体が
サイクル変動の大きい銘柄を表現できない構造的限界）・残課題3（EBIT(1-t)
ベース→純利益ベースの変換ロジックが存在しない）は[[TRUST-SUMMARY-EPIC-1]]
へ統合済み（詳細は同エントリ参照）。

残課題1（LITE/SITM型カテゴリ欠如）は調査の結果、SITMは既に解決済み
（beta_config.jsonのsector確定済み）、LITE単体の追加対応は残課題③と
同根の問題（EBIT(1-t)→純利益変換ロジック不在）のため見送り。加えて
調査中にLITE以外44銘柄（うち33銘柄がfcf_estimation.applied=Trueで
default(0.70)使用中）のsector未収録という同型の広範なギャップを新規発見し、
TRUST-SUMMARY-EPIC-1へ統合済み（詳細は同エントリ参照）。

このエントリには残課題4（IOT等判定保留）・残課題5（暫定判定精度78%）のみ
残置する。

#### 着手条件
**キー名不一致修正・Software_Systemグループ分割は完了済み（2026-07-14）**。
上記残課題4・5はいずれも着手条件なし（次回セッションで優先度・方針を
判断してから着手）。

---

## 優先度：中（こなれてきたら対応）

### [LAYER3-ANNUAL-CLASSIFICATION-DROPS-DATA-1] Layer3の年次期間分類が実在するデータを取りこぼしている（年次側は調査完了・実質解消／四半期側は限定的な残課題あり）
**優先度:** 低（2026-08-19②の範囲実測により、当初の仮説は大半が誤りと
判明。年次側は確認された未解決の欠陥が0件、四半期側も確認された欠陥は
4件でいずれもTAIL非消費フィールド）
**分類:** 調査完了 / データ品質 / SEC EDGAR / Layer3統合スキーマ
**登録日:** 2026-08-19
**更新日:** 2026-08-19③（`CELH stock_based_compensation`のgap=1を
個別調査。原因未特定のまま記録、TAIL実消費への影響なしを確認）
（2026-08-19②：全105銘柄相当・32フィールドの範囲実測を実施。
年次側の仮説はほぼ全否定、四半期側も実測。詳細は本文参照）
**発見:** `[[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]`実測調査（Step 3、
Layer3側の営業利益再構成の実態確認）

#### 内容（登録時点、2026-08-19①）
`layer3_builder.py::build_ticker_store()`を全tanuki銘柄（100件）で
実行した結果、JNJ・KLAC・LLY・XOMの4銘柄で`operating_income`（年次）の
エントリが0件だった。JNJの`company_facts.json`には`OperatingIncomeLoss`
のFY 10-Kエントリが12件存在することを確認し、「Layer3独自の期間分類が
実在するデータを拾えていない可能性」という仮説のもとで登録した。

#### 年次側 範囲実測結果（2026-08-19②）——仮説は大半が否定された
100銘柄×32フィールド＝3,200行を本番の`_classify_period()`・
`build_ticker_store()`をimportして実測（自前の判定ロジック再実装なし）。

| 状態 | 件数 |
|---|---|
| (a) 元データなし | 1,820 |
| (b) 正常 | 13 |
| (b) 部分取りこぼし | 1,335 |
| (c) 完全取りこぼし | 32 |

- (c) 32件のうち**30件は`_ANNUAL_YEARS=6`保持窓で完全に説明可能**
  （タグ報告終了が6年より前）。JNJ・KLAC・LLY・XOMの`operating_income`
  もこれに該当——**「期間分類のバグ」ではなく「6年保持窓の意図した
  挙動＋タグ報告打ち切りが古い」の組み合わせ**だった（JNJの最新
  `OperatingIncomeLoss`エントリ〈`end=2014-12-28`〉を1条件ずつ評価し、
  `_classify_period()`自体は`is_annual=True`と正しく分類、
  `_process_entries()`の`end>=cutoff_a`保持窓チェックのみで除外される
  ことを実際に確認済み）
- 残り2件（`APP capital_expenditure`・`MSFT depreciation_and_
  amortization`）は当初「6年窓内に生データがあるのにLayer3が0件＝真の
  異常」と報告したが、**これも誤りだった**。`config/sec_concept_
  definitions.json`の`ticker_overrides`にAPP/MSFTそれぞれ`action:
  "exclude"`の設定が存在し、**意図的な除外設定**だった（旧
  `quarterly.py::TICKER_RESTRICTIONS`からの移行済み設定）。当初の実測
  スクリプトが`ticker_overrides`を参照していなかったための誤検知
- (b)部分取りこぼしのうち保持窓で説明できない14件は**全てBBAI**
  （11フィールドで一律`raw_within_6yr=11→l3_count=7`）だったため、
  当初「銘柄固有の構造的異常、原因未特定」と報告したが、**これも誤り**
  だった。`[[LAYER3-ANNUAL-MISCLASSIFICATION-BBAI-1]]`
  （2026-08-06完了・BACKLOG_DONE.md参照）が`_reclassify_misannotated_
  fy_entries()`をBBAI限定（`_ANNUAL_MISCLASSIFICATION_FIX_TICKERS =
  frozenset({"BBAI"})`）で既に実装済みであり、中間期のYTD比較開示が
  誤ってis_annual=Trueに混入する既知パターンを正しく除外した**結果**
  だった。当初の実測スクリプトはこの後処理を呼び出しておらず
  `_classify_period()`単体の結果と比較したための誤検知

**訂正の結論**: 年次側は`ticker_overrides`未考慮・既存修正の後処理
未再現という**2つの計測方法の誤り**により偽陽性を報告していた。
これらを補正した結果、**年次側で確認された未解決の取りこぼしは0件**。
`SEC_EDGAR_LAYER_DESIGN.md`のフォールバック不在に関する記載なしという
指摘自体は事実として残るが、期間分類ロジックそのものに未知の欠陥は
確認されなかった。

#### 四半期側 範囲実測結果（2026-08-19②、新規）
TAILの実消費経路（`get_quarterly_series`/`get_latest_quarterly`）は
四半期データを読むため、年次側だけでの「実害ゼロ」は消費経路を測らず
安全宣言することになるとの指摘を受け、四半期側も同一100銘柄×32
フィールドで実測した（`_QUARTERLY_YEARS=5`、末尾のend日ベースで
Layer3側の最終エントリ集合と比較）。

| 状態 | 件数 |
|---|---|
| (a) 元データなし | 881 |
| (b) 正常 | 2,189 |
| (b) 部分取りこぼし | 41 |
| (c) 完全取りこぼし | 89 |

- (c) 89件のうち83件は5年保持窓で説明可能。残り6件のうち2件
  （`APP capital_expenditure`・`MSFT depreciation_and_amortization`）
  は年次側と同じ`ticker_overrides`除外設定で説明できる。
  **残る4件が現時点で唯一の確認された欠陥**:
  `ALAB buyback`・`CRWV buyback`・`CWAN buyback`・
  `FICO finance_lease_payments`（5年窓内に生データがあるのにLayer3が
  0件、`ticker_overrides`にも該当なし、原因未特定）
- 「Layer3の実エントリ末尾end日が、5年窓内の生データend日集合と
  一致しない」という粗い指標（`genuine_gap`）では442件が該当したが、
  **この数値は信頼性が低いと判断し、確定した欠陥件数としては扱わない**。
  理由: 同じ粗い指標で年次側は当初32件・14件（BBAI）を「真の異常」と
  誤検知しており、四半期側でも`_merge_candidate_entries()`の候補間
  優先度マージ・`_is_plausible_standalone_quarter`等のプルーニングを
  本実測では再現できていないため、同型の誤検知が442件の大半を占めて
  いる可能性が高い。442件を欠陥として扱う前に、これらの内部ロジックを
  踏まえた再測定が必要（本項目のスコープ外、着手時に再実施すること）

#### 実害判定（TAIL消費経路そのものを実測、2026-08-19②）
TAILが実際に消費する5フィールド（revenue/operating_income/
stock_based_compensation/net_income/shares_diluted）×保有10銘柄
（ADBE/APGE/APP/CELH/CRWV/NVDA/PLTR/SOFI/SOUN/TSLA）の50セルを
四半期データで個別確認した。

- **APGEは`get_tanuki_tickers()`に含まれておらず**（tanuki=false）、
  当初の100銘柄スキャンから漏れていた。`build_ticker_store('APGE')`を
  個別実行し確認
- 欠落2件: `APGE revenue`（0件）・`SOFI operating_income`（0件）。
  **いずれも(a)元データなし**——APGEはcompany_facts.jsonにrevenue系
  タグが1件も存在しない（臨床段階バイオ、実際に無収益と推測）。SOFIが
  `OperatingIncomeLoss`を報告しないことは`[[OPERATING-INCOME-
  EXTRACTION-GAP-1]]`で既に確認済みの事実と整合。**取りこぼし(c)では
  ない**
- `CELH stock_based_compensation`のgap=1は2026-08-19③で追加調査した。
  欠落しているend日は**`2021-09-30`（1件のみ）**——CELHの生データでは
  この期がQ3 2021のYTD開示（10-Q、`is_ytd=True`）だが、`_ytd_to_
  quarterly()`が単一四半期額へ差分するために必要なQ1/Q2 2021の同一
  YTDチェーン先行エントリが、`_QUARTERLY_YEARS=5`の保持窓カットオフ
  （実測時点基準で`cutoff_q≈2021-08-20`）の外側にあるため存在しない。
  結果としてこのYTDエントリは差分元を持たない「孤立エントリ」となり、
  `_ytd_to_quarterly()`内で`unresolved`リストへ追加される経路を通る
  （`_normalize_field_entries()`が`all_quarterly.extend(unresolved)`で
  再結合する箇所まではコード追跡で確認）。**ただし、この`unresolved`
  エントリが最終的に`build_ticker_store()`の出力へ現れない具体的な
  分岐点までは特定できなかった**——推測で断定せず「原因未特定」として
  記録する。実害は`get_latest_quarterly(store, 'stock_based_
  compensation')`で個別確認済みで、TAILが実際に消費する最新エントリは
  `end=2026-03-31, val=7,626,000`であり、2021-09-30の欠落はTAILの
  `sbc_quarterly`出力に**影響しない**（優先度低のまま据え置き、対応
  不要）
- 残り48セルは全て正常一致

**消費側の欠損時挙動（i/ii/iii判定）**: `tail_dcf_bridge.py`・
`quarterly_review_generator.py`とも
```python
if rev and oi and rev.get("val"):
    result["operating_margin"] = round(oi["val"] / rev["val"], 4)
if sbc:
    result["sbc_quarterly"] = sbc["val"]
if ni and sd and sd.get("val"):
    result["eps_diluted"] = round(ni["val"] / sd["val"], 4)
```
**(ii) 欠損として明示的に除外される**（対応するresultキー自体が
出力されない、クラッシュなし）。**(iii)の0/既定値への暗黙置換は
確認されなかった**。ただし`rev.get("val")`・`sd.get("val")`は
truthy評価されており、収益・希薄化株式数が正当に`0`となる四半期が
将来発生した場合、そこだけ`operating_margin`/`eps_diluted`が欠損扱い
される軽微なfalsy-zeroリスクが理論上残る（実データでは現状該当なし、
優先度低）。

#### parser.py↔Layer3の2経路乖離（2026-08-19②、事実の登録のみ）
`[[OPERATING-INCOME-EXTRACTION-GAP-1]]`本線1でparser.py側に
`operating_income`のGP法/pretax法再構成を実装したが、`layer3_builder.py`
側には同等のフォールバックがない。同一`company_facts.json`から同一概念
を取る2経路のうち、片方だけ再構成されている非対称な状態。

実数（100銘柄）: **Layer3年次`operating_income`が0件の4銘柄
（JNJ・KLAC・LLY・XOM）は、4/4ともparser.py側の`annual_YYYY.json`には
`operating_income`が入っている**（JNJ: $25.596B `reconstructed_gp`・
KLAC: $5.014B `reconstructed_gp`・LLY: $29.696B `reconstructed_gp`・
XOM: $41.871B `reconstructed_pretax`）。実装（Layer3側への再構成移植）
は別途判断、本項目では事実の登録のみ。

#### 対応方針
- 四半期側の4件（ALAB/CRWV/CWAN buyback、FICO finance_lease_payments）
  は原因未特定のまま優先度低で保留可（TAIL非消費フィールドのため実害
  なし）
- 442件の粗いgenuine_gap指標は、`_merge_candidate_entries()`内部の
  候補優先度マージ・プルーニングロジックを実測に組み込んだ上での
  再測定が必要（未着手）
- parser.py↔Layer3の2経路乖離への対応要否は別途判断

#### 関連
- `[[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]`（本問題の発見元、
  GP法/pretax法フォールバック不在の課題とは別種）
- `[[OPERATING-INCOME-EXTRACTION-GAP-1]]`（BACKLOG_DONE.md、parser.py側
  の再構成実装元。Layer3側との2経路乖離の一方の当事者）
- `[[LAYER3-ANNUAL-MISCLASSIFICATION-BBAI-1]]`（BACKLOG_DONE.md、
  2026-08-19②の実測でBBAIパターンの真因と判明した既存完了項目）
- `[[LAYER3-FALLBACK-STALE-TAG-PRIORITY-1]]`（BACKLOG_DONE.md、Layer3の
  別の既知問題〈古いタグ優先バグ〉。本問題とは異なる原因）

#### 着手条件
なし（優先度低のため急ぎ不要）

---


---


### [TAIL-LAYER3-FORMULA-YOY-UNSUPPORTED-1] layer3_formulaが除算のみ対応で、YoY成長率など系列比較を表現できない
**優先度:** 低（着手条件なし。現状ブロックしているのはPLTR「希薄化後
EPS成長率」1件のみで、実害は限定的）
**分類:** 機能不足 / TAIL自動化パイプライン
**登録日:** 2026-08-20
**発見:** `[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-GAP-1]]`Step 4
（core 3銘柄へのLayer3適用）でPLTR「希薄化後EPS成長率」をLayer3経由
へ振り替えられるか判定中に発見。

#### 内容
`src/tail/xbrl_segment_fetcher.py::fetch_layer3_kpis()`の
`layer3_formula`は`"field_a/field_b"`形式の**同一四半期内の2フィールド
の除算のみ**対応する（439-454行目）。「希薄化後EPS成長率」のような
「同一フィールドの前年同期比（YoY）」を表現する構文が存在しない
（`eps_diluted`という1フィールドの時系列上で`(今期値-前年同期値)/
前年同期値`を計算する必要があり、除算専用の現行パーサーでは表現
不可能）。

`layer3_field`を`eps_diluted`に設定して直接値を渡す代替も検討したが、
それは希薄化後EPSの**水準**であって**成長率**ではなく、KPIの
`warning_threshold`（成長率ベースの閾値）と意味が合わなくなるため
採用しなかった。

#### 着手条件
着手条件なし。同型の「系列に対する前年同期比・前期比」を必要とする
KPIが他にも将来登録される可能性があるため、個別対応ではなく
`layer3_formula`のミニ構文自体を拡張する（例:
`"yoy(eps_diluted)"`のような関数呼び出し記法）方が汎用的だが、
現時点で対象は1件のみのため優先度は低いまま。対象KPIが増えた場合に
再評価すること。

---

### [TAIL-SATELLITE-MONITOR-CORE-APPLICABILITY-1] satellite_monitor.pyの4条件をcore 3銘柄へ適用できるかの技術調査（調査のみ、実装なし）
**優先度:** 低（現状維持でも実害はない。core側の高頻度監視が無いことは
事実だが、四半期レビューによる評価は別途機能している）
**分類:** 運用ギャップ / TAIL自動化パイプライン / 技術調査
**登録日:** 2026-08-19④
**発見:** `[[TAIL-SATELLITE-POSITION-MONITORING-GAP-1]]`・
`[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`のSYSTEM_MAP.md記録時に発見した
「core 3銘柄はsatellite_monitor.pyの対象外＝価格変動・エグジット条件・
テーゼ否定ニュースの継続監視を一切受けていない」という非対称の技術的
検証

#### 内容
`satellite_monitor.py`は`positions_index.json`の`type=="satellite"`を
直接フィルタする独立システムで、4条件（①価格変動±20%、②エグジット
条件の数値目標到達、③Grokによるテーゼ否定ニュース検知、④決算接近）を
Discord通知する。core（PLTR/SOFI/TSLA）は対象外であり、同等の監視を
提供する別システムは存在しない（`.github/workflows/`の全TAIL関連
ワークフローを確認、該当なし）。4条件それぞれについて、core 3銘柄の
実際のthesisデータに対して技術的に適用可能かを実測で判定した。

#### 判定結果（4条件それぞれ）

**①価格変動±20%: そのまま適用可**
core 3銘柄はいずれも`entry_price: null`（`thesis.json`）だが、
`monitor_ticker()`は`pos.get("entry_price") or avg_costs.get(ticker)`
という、`portfolio.json`の加重平均取得単価へのフォールバックを既に
実装している。このフォールバック自体はcore/satelliteのスキーマに
依存しないため、そのまま機能する（同様のフォールバックは
`quarterly_review_generator.py`でも既に使われている既存パターン）。

**②エグジット条件の数値目標到達: 適用不可（そのままでは）**
`_extract_numeric_exit()`（正規表現による「$XXX到達」「X倍」等の短い
定型文からの数値抽出）を、core 3銘柄の実際の`exit_guide`テキストに
対して実行した結果、**3銘柄とも`None`（抽出不可）を実測確認**。
core 3銘柄の`exit_guide`は数千文字規模の構造化された長文エッセイ
（複数の「壊れる条件」シナリオ・監視指標を段落形式で記述）であり、
satelliteの短い`exit_condition`（例:「割安感がなくなって利が乗ったら
売り切る」）とは形式が根本的に異なる。

加えて、`monitor_ticker()`自体（376行目）が`pos.get("exit_condition",
"（条件未設定）")`という**satellite専用のフィールド名**を直接読んで
おり、修正なしにcoreへ適用すると`exit_cond`が常に「（条件未設定）」に
なる——これは本セッションで3回目に発見した同型のスキーマ不整合
（`quarterly_review_generator.py`・`kpi_proposer.py`ではsatellite側が
「未設定」になっていたが、今度は逆方向にcore側が「未設定」になる）。

代替案（実装はしない）: (i) Grokによる定性判定（`exit_guide`の長文を
渡し、現在の決算・株価状況が「壊れる条件」に近づいているかをAIに判断
させる、正規表現による数値抽出とは別の方式）、(ii) 将来的な数値目標
フィールドの新設（thesisに`exit_price_target`等を追加）。

**③テーゼ否定ニュース検知: 要改修**
`_call_grok_news()`自体はstrategy_name・exit_condition文字列を
プロンプトへ埋め込むだけの汎用的な実装で、機構的にはcoreのテキストを
渡しても動作する。ただし②と同じ理由（`monitor_ticker()`が
`exit_condition`という satellite専用フィールドを直接読む）で、
修正なしでは「（条件未設定）」がプロンプトに渡ってしまう。今回新設した
`thesis_narrative_fields()`（`src/tail/thesis_utils.py`）を
`satellite_monitor.py`側でも使うよう改修すれば、`exit_guide`を正しく
読めるようになり解消可能。`strategy_name`はcoreに存在しないフィールド
のため空文字のままになるが、プロンプト自体は成立する。

**④決算接近: そのまま適用可**
`rss_state.json`にcore 3銘柄のエントリが実際に存在することを確認
（PLTR/SOFI/TSLAとも`last_filed`等が記録済み）。`_check_earnings_
approach()`を実際に呼び出し、3銘柄とも次回決算予想日を正しく計算
できることを実測確認（例: PLTR推定2026-11-02・74日後、現時点では
2週間以内の閾値に達していないため`triggered=False`）。

#### 通知頻度への影響
現状`satellite_monitor.py`は平日2回（JST 08:00・17:00）×satellite
7銘柄＝1日あたり最大14回の`monitor_ticker()`呼び出し。core 3銘柄を
追加した場合、1日あたり最大20回（+43%、週あたり+30回）に増加する。
Grok Web検索（条件③）・Discord通知の呼び出し回数もこれに比例して
増加する。

#### 対応方針
未定（本項目では技術調査の記録のみ、実装しない）。着手する場合は
最低限②③の改修（`thesis_narrative_fields()`の`satellite_monitor.py`
への導入、②は代替案の選定）が前提となる。

#### 関連
- `[[TAIL-SATELLITE-POSITION-MONITORING-GAP-1]]`・
  `[[TAIL-COVERAGE-POLICY-UNDECIDED-1]]`（BACKLOG_DONE.md、逆方向の
  同型の非対称〈satelliteがレビュー生成から漏れていた〉を発見・是正
  した項目）
- `SYSTEM_MAP.md`「TANUKI TAIL」節（本調査の要点を記録済み）

#### 着手条件
ユーザーに、core 3銘柄へsatellite_monitor.pyの監視を広げるべきかの
投資方針判断を確認してから着手すること。

---


### [STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1] STONKS SILOのYoY計算がfpラベルの完全一致のみで照合し期間長の妥当性チェックを持たない
**優先度:** 低〜中
**分類:** データ品質 / 設計改善
**登録日:** 2026-08-02
**発見:** [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]対応方針確定調査（チャット記録）

#### 内容
STONKS SILOのYoY計算（`financial_trend_calculator.py::_calc_yoy_change()`）
がfpラベル（Q1〜Q4）の完全一致のみでYoY照合しており、期間長の妥当性
チェックを持たない。RCATの決算期変更移行期（2024年）で、8ヶ月しか離れて
いない新旧"Q4"を誤って比較し歪んだYoYシグナル（`change_pct=-152.2%`）を
生成していた実例が確認済み（現在はデータ蓄積により自然解消済み）。
[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]の対応方針（fetcher.py側）とは
完全に独立した問題であり、fetcher.py側の対応の有無に関わらず必要な改善
（STONKS SILOのYoY計算は`quarterly_normalized.json`という別パイプライン
を参照するため）。

#### 影響
現時点で実害はない（自然解消済み）。決算期変更を経た他銘柄でも将来
同様の問題が起こりうる。

#### 対応方針
未定。「同fp・かつ期間長〜365日程度（許容誤差込み）」という追加の妥当性
チェックを`_calc_yoy_change()`に導入することを検討する。決算期変更を経た
他銘柄でも将来同様の問題が起こりうるため、ticker非依存の一般的な改善
として設計する。

#### 着手条件
なし。優先度低〜中（現状は自然解消済みで緊急性なし、将来の決算期変更
銘柄への予防的対応）。

---

### [BETA-FALLBACK-DESIGN-GAPS-1] β取得の3経路重複・0/負値無条件フォールバック・許容範囲の2基準並存
**優先度:** 中
**分類:** 設計不整合 / TANUKI VALUATION
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ6（AS-IS-013）・`OUTPUT_ITEMS_INVENTORY.md`「β（3経路：日次/月次/監査トリガー時）」

#### 内容
`beta_fetcher.py::calc_capped_beta()`は生βが`None`かどうかのみをチェックし、
`0`や負値であっても`max(0.3,min(2.5,raw_beta))`で無条件に0.3へフロアされ
`beta_config.json`に書き込まれる。警告フラグは一切付与されない。さらに
実行時の`data_fetcher.py::_determine_beta()`は`beta_config.json`のoverride
を最優先かつ無条件に採用するため、`_determine_beta()`自身が持つ「yfinance
直接値は0.1〜3.0の範囲内のみ採用」という健全性チェックが一切適用されない。
加えて`beta_fetcher.py`の許容範囲（0.3〜2.5）と`_determine_beta()`の
直接値許容範囲（0.1〜3.0）が異なる2つの基準として並存している。β取得
自体も日次/月次/監査トリガー時の3経路が重複している。

#### 対応方針
①生βが0/負値だった場合に警告フラグを付与する②`beta_config.json`の
overrideにも健全性チェックを適用する③許容範囲の基準を1つに統一する
④3経路の重複を整理する、の優先順位を検討してから着手する。

#### 着手条件
なし

---

（[[DISCOVER-CONFIG-DUAL-MGMT-1]]は2026-08-15実装完了、BACKLOG_DONE.md
「2026-08-15（完了）」参照）

---

### [SCHEMA-NORMALIZED-ISSUES-1] normalized/スキーマ関連の構造的ギャップまとめ（STDebtタグ網羅性劣化・SM/SGA概念混同・LTDebt優先順序逆転・SharesBasic概念不一致・ファイル名annualデータ混在・DAフォールバック欠如）
**優先度:** 中〜高（内訳: 中〜高1件・低5件、個別優先度は各項目参照。
2026-08-15、①②を実害調査完了により中〜高/中→低へ引き下げ）
**分類:** データ品質 / normalized/スキーマ（common/sec_data統合スキーマ設計関連）
**登録日:** 2026-07-23〜2026-07-24（統合日: 2026-08-03）
**発見:** data/quarterly⇔normalizedフィールド網羅性比較調査・Layer2設計調査

#### 内容
`normalized/`（`quarterly.py::FIELD_CONCEPTS`由来）と`data/quarterly`
（`parser.py::XBRL_MAPPING`由来）の間で、タグ網羅性・優先順序・概念
定義が系統的に食い違っている個別事象をまとめる。いずれも
`docs/architecture/new_data_platform/SEC_EDGAR_LAYER_DESIGN.md`
（367-378行目）でLayer2/Layer3統合スキーマ設計時の解消対象として
参照されている。

① **STDebtタグ網羅性劣化**（旧SCHEMA-STDEBT-COVERAGE-GAP-1、優先度
中〜高→**実害調査完了により低（2026-08-15）**）: 短期有利子負債
（STDebt/short_term_debt）のタグ網羅性が、normalized/側
（`quarterly.py::FIELD_CONCEPTS`、単一タグ`ShortTermBorrowings`のみ・
フォールバックなし）でdata/quarterly側（`parser.py::XBRL_MAPPING`、
9タグ候補＋フォールバック）に対し著しく劣化している。10銘柄実データ
確認でAAPL 33/51件・XOM 51/51件・V 30/51件がnormalized側で**0件**と
いう深刻な乖離を示した（逆にCAT等はdata/quarterly側が0件でnormalized
側に値がある逆転ケースもあり）。

**実害調査結果（2026-08-13〜15、読み取り専用調査・チャット記録）**:
消費箇所の洗い出しを完了。TANUKI VALUATION本体の主要消費経路
（`SECReader.get_net_cash()`）は`data/annual_*.json`（parser.py層、
9タグ候補版）を参照しており、normalized/の劣化版STDebtは経由しない。
normalized/を恒久的に使い続けると決定済みの3系統（フェーズE恒久的
例外、`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照）についても個別に
確認した:
- `fetcher.py`（STONKS SILO）: `_BS_FIELDS`に`short_term_debt`単体
  フィールドは存在せず（`total_debt`のみ）、対象外
- `dcf_validity_checker.py`（診断ツール）: `common/sec_data/data/
  {TICKER}/annual_{year}.json`（parser.py層）を参照しており、
  normalized/は経由しない
- `stock.html`（TANUKI VALUATION frontend）: normalized/を直接
  fetchする唯一の系統だが、取得フィールドはCF滝グラフ用の
  `OCF`/`CapEx`/`Revenue`/`SBC`/`DA`の5つに限定され`STDebt`は対象外。
  ページ内に表示される`bsAdj.short_term_debt`は`get_net_cash()`が
  計算済みの値（parser.py層由来）のパススルー表示であり、normalized/
  の直接読み取りではない

**結論: normalized/のSTDebtタグ網羅性劣化による実害は現時点で確認
できない**（リポジトリ全体を通じてnormalized/側の劣化版STDebtを
実際に読む消費者がゼロ）。スキーマ自体の欠陥（`ShortTermBorrowings`
単一タグ・フォールバックなし）は現存するため、`common/sec_data`統合
スキーマ設計時の一括解消対象としては残す。着手条件: なし（優先度を
中〜高→低に格下げ、統合作業と同時対応で可）。

② **SM/SGA概念混同**（旧SCHEMA-SM-SGA-CONFLATION-1、優先度中→
**実害調査完了により低（2026-08-15）**）: data/quarterlyは
`selling_and_marketing`（純S&M費用）と
`selling_general_and_administrative`（SGA総額）を別フィールドとして
両方保持するが、normalized/は`SM`という単一フィールドしか持たず、
S&M単体タグが取得できない銘柄（JOBY/NVDA/CIX/ELF/KO等、
quarterly.py:236-243に明記）では`_FIELD_FALLBACKS["SM"]`経由でSGA
総額へ静かにフォールバックする。同じ`SM`値が銘柄によって「純S&M」と
「SGA総額」という異なる意味を持ちうるが、フィールド名からは判別
できない。

**実害調査結果（2026-08-13〜15、読み取り専用調査・チャット記録）**:
TANUKI VALUATION本体の`_estimate_ttm_operating_income()`
（`pipeline.py`、フェーズD Step2-1でLayer3化済み）は
`get_field_entries(store, "selling_and_marketing")`でLayer3の
`selling_and_marketing`フィールド（data/quarterly側と同型の分離
フィールド）を参照しており、normalized/の混同版`SM`は経由しない。
①と同じ3系統（fetcher.py・dcf_validity_checker.py・stock.html）も
個別に確認した:
- `fetcher.py`: `_PL_FIELDS`に`selling_and_marketing`（parser.py層の
  分離フィールド）を参照するが、normalized/の`SM`ではない
- `dcf_validity_checker.py`: SM/SGA系フィールドへの参照なし
- `stock.html`: CF滝グラフの取得フィールド（OCF/CapEx/Revenue/SBC/
  DA）に`SM`は含まれず対象外

**結論: normalized/のSM/SGA概念混同による実害は現時点で確認できない**
（リポジトリ全体を通じてnormalized/側の混同版`SM`を実際に読む消費者が
ゼロ）。スキーマ自体の欠陥は現存するため、統合スキーマ設計時の一括
解消対象としては残す。着手条件: なし（優先度を中→低に格下げ、統合
作業と同時対応で可）。

③ **LTDebt優先順序逆転**（旧SCHEMA-LTDEBT-DOUBLECOUNT-RISK-1、優先度
中〜高）: 長期有利子負債（LTDebt/long_term_debt）のprimaryタグ優先
順序が`quarterly.py::FIELD_CONCEPTS`と`parser.py::XBRL_MAPPING`の間で
逆転している。`parser.py`側は`LongTermDebtNoncurrent`を`LongTermDebt`
より優先しており、これは「`LongTermDebtCurrent`との二重計上防止」
という明示的な設計意図（BUG-NETDEBT-2対応コメントあり）に基づく。一方
`quarterly.py`側（`normalized/`生成元）は`LongTermDebt`を先に試す設定
になっており、この配慮が反映されていない。【2026-07-24検証結果】
reader.py::get_net_cash()の実装を確認した結果、二重計上が発生しうる
のは「annual側long_term_debtが0/欠損 かつ normalizedフォールバックが
非ゼロ」の場合に限られる。105銘柄全数で確認したところ該当銘柄は0件
であり、現行データでは実害が確認されなかった。ただし構造的リスクは
残るため、Layer2統合時にparser.py側の優先順序（二重計上防止済み）へ
統一することで解消する方針とする。着手条件: なし。

④ **SharesBasic概念不一致**（旧SCHEMA-SHARESBASIC-CONCEPT-MISMATCH-1、
優先度中→**実害調査完了により低**）: SharesBasic（発行済株式数関連）の
primaryタグが、2システム間で単なる順序差ではなく**意味的に異なる財務
概念**を指している。`quarterly.py`側は`CommonStockSharesOutstanding`
（貸借対照表項目・期末時点の発行済株式数）をprimaryとするのに対し、
`parser.py`側は`WeightedAverageNumberOfSharesOutstandingBasic`
（損益計算書項目・期中加重平均株式数）をprimaryとする。

**実害調査結果（2026-08-05、チャット記録・読み取りのみ）**: 消費箇所の
洗い出しを完了。**normalized/側の「SharesBasic」フィールドは、
`quarterly.py`（定義側）以外に参照するコードがリポジトリ全体でゼロ件**
（既存5本番消費者はいずれも未参照の死んだフィールドと確認）。data/側の
`shares_basic`は`reader.py::get_diluted_shares()`が
`shares_diluted<1,000,000`時のフォールバックとしてのみ参照（呼び出し元:
`data_fetcher.py`、TANUKI VALUATION）。15銘柄サンプルでnormalized側
SharesBasicが5/15銘柄（BKNG/WMT/JNJ/PEP/VZ）で0件という新たな網羅性
ギャップも確認。**結論: normalized/⇔data/間の概念不一致自体による実害は
なし**（normalized/側に消費者がいないため、normalized/→data/統合の
障害にはならない）。副次発見（ONDS/LOARのshares_basic単位スケール
異常疑い）は`[[ONDS-LOAR-SHARES-SCALE-SUSPECT-1]]`として別途登録。
着手条件: なし（優先度を中→低に格下げ、統合作業と同時対応で可）。

⑤ **ファイル名とannualデータ混在**（旧SCHEMA-NORMALIZED-ANNUAL-NAMING-
MISMATCH-1、優先度低）: `normalized/{TICKER}_quarterly_normalized.json`
はファイル名に"quarterly"と明記されているが、実際は`is_annual: true`
エントリとしてannualデータも同一ファイル内に混在保持している
（`quarterly.py::build_raw_table()`がcompany_factsから四半期・年次
両方を同一fieldsに格納するため）。現状の実害は確認されていないが、
統合スキーマ設計時にファイル名から内容を誤推測する混乱要因になり
うる。着手条件: なし。

⑥ **DAフォールバック欠如**（旧SCHEMA-DA-FALLBACK-MISSING-1、優先度
低）: `quarterly.py::FIELD_CONCEPTS`のDA（減価償却）概念はフォール
バック候補が一切設定されておらず（単一タグ
`DepreciationDepletionAndAmortization`のみ）、このタグを報告しない
銘柄（LMT等、`DepreciationAndAmortization`のみ報告）で`normalized/`側
のDAフィールドが完全に空（0エントリ）になる。①と同型のフォール
バック欠如パターン。実質的な計算消費箇所（成長率推計、
pipeline.py:2807）は`normalized/`ではなくannual/quarterly側
（parser.py由来、フォールバック4候補あり）を参照しているため、現状の
計算結果への実害はない。`normalized/`のDAはstock.htmlの単純表示にしか
使われていないため影響は限定的。着手条件: なし。

**定量実測結果（2026-08-07、`[[STOCKHTML-LAYER3-PUBLISH-PIPELINE-
MISSING-1]]`着手要否投資調査の一環）**: `normalized/`105銘柄全数を
実測した結果、**30銘柄（約29%）でDAフィールドが完全に空（0件）**と
確認。MSFT・TSLA・GOOGL・AVGOを含む主要銘柄も対象:
`ABBV, ADSK, AMD, APGE, AVGO, BBAI, CEG, COHR, CON, ENB, ESTC, GEV,
GOOGL, INTU, IONQ, KULR, LITE, LMT, LOAR, LRCX, MRVL, MSFT, RKLB,
TASK, TDY, TSLA, VZ, V, WMT, ZETA`。いずれもstock.htmlのCF滝グラフ
「SBC・D&A比率」チャートでD&A系列が欠落する（表示のみへの影響、
TANUKI VALUATION計算結果への実害は上記の通りなし）。

**調査依頼文の前提訂正（2026-08-15）**: ①②の実害調査を進める過程で、
依頼文が前提としていた「fetcher.py・dcf_validity_checker.pyは
normalized/直読み継続で確定済み」という記述を実コードで確認したところ
**誤りと判明した**。両ファイルとも実際には`common/sec_data/data/
{TICKER}/annual_*.json`（parser.py層）を参照しており、normalized/を
直接fetchするのは`[[SECDATA-STORAGE-FRAGMENTATION-1]]`が記す
フェーズE恒久的例外3系統のうち**stock.htmlのみ**（`CLAUDE_CODE_
START.md`自体の記述「`fetcher.py`・`dcf_validity_checker.py`
（`data/annual_*.json`依存継続）・stock.html（`normalized/`直接依存
継続）」が正しかった）。この訂正を経た上で3系統×2フィールド
（STDebt・SM）の実消費有無を個別に確認し、上記①②の結論に至った。

#### 対応方針
①〜⑥のいずれも、`common/sec_data`統合スキーマ（Layer2/Layer3）設計時に
`parser.py`側の定義（フォールバック網羅性・優先順序とも既に安全性検証
済み）へ統一することで一括解消する見込み。個別の緊急対応は不要。

#### 着手条件
①〜⑥いずれも個別の着手条件なし（優先度に応じて統合作業と同時対応で
可。①②は2026-08-15、実害調査完了〈実消費者ゼロ確認〉により
「common/sec_data統合スキーマ設計の確定後」という着手条件を撤廃し
優先度低へ格下げ済み）。

---

### [SEC-SUBMISSIONS-DUAL-FETCH-1] SEC EDGAR submissions APIがfetcher.pyとedgar_rss_monitor.pyで独立に重複取得されている
**優先度:** 低〜中
**分類:** 技術的負債 / API呼び出し重複
**登録日:** 2026-07-23
**発見:** annual/segment/filing_text AS-IS構造調査（フェーズ1）④

#### 内容
SEC EDGAR submissions API（`data.sec.gov/submissions/CIK{cik}.json`）
が、`common/sec_data/fetcher.py::fetch_submissions()`（週次、全filings
一括、`submissions.json`へキャッシュ）と`src/tail/
edgar_rss_monitor.py::get_filing_period()`（平日毎日、特定accnのみ
live fetch・キャッシュなし）の2箇所で独立に叩かれている。
EPS Analyzerの独立SEC取得（前回調査⑤(A)②-3で確認済み、別課題）と
同型のパターン。

#### 影響
API呼び出しの無駄な重複。実害としては、新規提出直後は週次キャッシュ
に未反映なため、TAIL側がlive fetchで補っているという設計上の理由が
ある（鮮度ギャップの解消目的）。単純な参照統合は鮮度要件を壊す
リスクがある。

#### 対応方針
未定。edgar_rss_monitor.py側をsubmissions.json参照＋未ヒット時のみ
live fetchにフォールバックする設計への変更が有力候補だが、鮮度
ギャップの設計対応が別途必要。

#### 着手条件
なし

---

### [NAMING-CONVENTIONS-APPLY-1] NAMING_CONVENTIONS.md規則1〜5の実装への適用
**優先度:** 中
**分類:** リファクタリング / 命名規則
**登録日:** 2026-07-23
**発見:** `NAMING_CONVENTIONS.md`

#### 内容
`NAMING_CONVENTIONS.md`が策定した5つの命名規則（データソース接尾辞・
期間接尾辞・誤称禁止・provenance明示・唯一の正の参照元明示）は、策定の
みで実装（既存フィールドのリネーム）には未反映。個別の適用例
（[[NETCASH-DUAL-CALC-1]]の`net_cash_sec`化、[[RULE40-DEFINITION-
MISMATCH-1]]の期間接尾辞化等）は該当タスク側で扱うが、命名規則全体の
チェックリスト運用（新規フィールド追加時の適用）自体は独立したタスクと
して管理する。

**2026-08-15追記（実装結果との食い違い）**: `[[NETCASH-DUAL-CALC-1]]`の
実際の実装（2026-08-13完了）は、想定していた規則1の接尾辞化
（`net_cash`→`net_cash_sec`）を行わず、**フィールド名`net_cash`を維持
したまま算出元のみ`SECReader.get_net_cash()`へ統一**した（STONKS
SILOの独自算出`cash − yfinance totalDebt`を廃止）。これは規則1の趣旨
（データソースが異なる場合に接尾辞で識別できるようにする）に照らすと
矛盾ではない解釈も成り立つ：統一後はTANUKI VALUATION・STONKS SILOとも
同一のデータソース（`SECReader.get_net_cash()`）を参照するようになった
ため、「データソースが異なる場合」という規則1の適用前提自体が消滅し、
接尾辞による識別の必要性がなくなったとも言える。一方`[[RULE40-
DEFINITION-MISMATCH-1]]`の期間接尾辞化（`rule40_yoy_netmargin`・
`rule40_cagr3y_opmargin`）は想定通り規則2に従って実装済み。個別適用例
の記載は「命名規則の適用＝機械的な接尾辞付与」ではなく「適用要否は
統一後のデータソース同一性を踏まえて都度判断する」という運用実態に
即した表現に将来更新することが望ましい（本エントリの対応方針自体
〈チェックリスト運用〉には影響しないため、記録として付記するのみ）。

#### 対応方針
新規フィールド追加時に`NAMING_CONVENTIONS.md`の適用チェックリストを
参照する運用をCLAUDE_CODE_START.md等に明記する。既存フィールドの一括
リネームは影響範囲が大きいため、個別タスク（上記関連タスク）の実装時に
順次適用する。

#### 着手条件
なし

---

### [SENS-MATRIX-DUAL-IMPL-1] stock.html独自5×5感応度マトリクスとbackend 3×3の並存（一部対応済み）
**優先度:** 中
**分類:** 設計不整合 / TANUKI VALUATION
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ6（AS-IS-066/AS-IS-014）・`OUTPUT_ITEMS_INVENTORY.md`横断的発見事項1
**一部対応:** 2026-08-30（フェーズ1バッチA cluster 3）

#### 内容
バックエンドのAS-IS-014（`sensitivity.matrix`、3×3、DCFタイプにより
two_stage/three_stageを切替）とは別に、stock.html上に完全に独立した
クライアント側5×5感応度マトリクス（`calcSensIV()`）が同一ページに並存
する。`calcSensIV()`は常に2段階DCFのみで再計算するため、three_stage DCFや
tapering DCFを採用している銘柄では、同じページ内の2つの「感応度分析」
セクションが異なる計算式で異なる数値を表示する。

#### 2026-08-30時点の対応状況
死コード（`const alpha = d.alpha ?? 1.0;`、宣言されているが式中で未使用）は
`stock.html`の`calcSensIV()`から削除済み。ただし本項目の核心である
「バックエンドAS-IS-014（3×3）とクライアント側5×5マトリクスの計算式
二重実装」自体は未解消のまま残っている。

#### 対応方針（未解消部分）
バックエンドのAS-IS-014をそのまま表示する設計に統一するか、クライアント側
5×5マトリクスをDCFタイプ切替に対応させるかの設計判断がKoichiさんに必要。
判断待ちのため未着手。

#### 着手条件
Koichiさんによる設計方針決定（バックエンド表示統一 or クライアント側
DCFタイプ対応拡張）

---

### [RICE-ADJ-ASYMMETRIC-ZERO-1] RICEのrice_adjのみ0フロアガードがある非対称設計
**優先度:** 中
**分類:** バグ / TANUKI VALUATION
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ6（AS-IS-027）

#### 内容
`calculate_rice()`（`rice.py:440`）で`rice_adj_val = (...) if cf_adj > 0
and wacc > 0 else 0.0`と明示的にゼロフロアされるのに対し、直上の
`rice_val`（`rice.py:438`）には`cf`（本来のCF値）が負であっても同様の
ガードがなく、そのまま計算される。CF（投資再生産効率）が構造的に負値を
取りうる銘柄では、`rice`は符号が反転した値をそのまま返すのに`rice_adj`
だけが0にフォールバックするという不整合が生じる。

#### 対応方針
`rice_val`にも`cf`が負の場合の同等のガードを追加するか、両者とも
ガードなしにするかを設計判断してから実装する。

#### 着手条件
なし

#### 再検証記録（2026-08-30、フェーズ1バッチB）
`rice.py:438-440`で現在も再現することを実コードで確認した（差異なし）。
「rice_valに0フロアを追加する」か「rice_adjのガードを外す」かで
画面に表示される数値そのものが変わる製品判断であり、技術的な実装の
巧拙ではなくどちらの表示仕様が正しいかというKoichiさんの判断が必要な
ため、本ラウンドでは実装せず現状維持とした。

---


---

### [EPS-LITE-ANNUAL-AS-QUARTERLY-1] LITE FY2026第4四半期のEPSが通期実績の単一四半期誤抽出で異常値化
**優先度:** 中
**分類:** データ品質 / EPS ANALYZER
**登録日:** 2026-09-05
**発見:** [[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]対応中、EPS閾値校正のための
実データ分布調査（チャット記録）

#### 内容
LITE（Lumentum）のEPS Analyzer quarterly.json、FY2026Q4（filing_date=
2026-06-27、form=10-K）のadjusted_eps=-95.004（gaap_eps=-96.001、
gaap_net_income=-$7,161,700,000）が、直前3四半期（+0.93/+1.62/+2.12）
から見て明らかに桁違いの異常値となっている。

原因を確認したところ、当該レコードのrevenue=$3,014,000,000は
Q3実績（$808.4M）・Q2実績（$665.5M）と比較して明らかにFY2026の
**通期revenue**の規模であり、LITEの10-K（annual filing）における
通期実績（Q4単体ではなくFY全体）がそのまま単一四半期のレコードとして
誤抽出されていると判明。net_income=-$7.16Bも同様に通期の値と推測される
（真のQ4単体revenue・net_incomeは、通期実績からQ1〜Q3累計を差し引く
標準的な手法で導出する必要があるが未実施）。

会計上・タグ付け自体に誤りがあるわけではなく（company_facts.jsonの値
自体は正しい通期実績）、EPS Analyzer側の四半期抽出ロジック
（`extract_key_facts.py`）が10-Kのfiscal Q4を「通期実績 − Q1〜Q3累計」
で導出していない、あるいはこのケースで導出に失敗しフォールバックして
いる可能性が高い。

#### 影響
[[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]対応の過程でTANUKI VALUATION側に
EPS絶対値の異常値除外ガード（EPS_MAGNITUDE_CAP=30）を新設したため、
黒字化予測への実害は解消済み（LITEは正しくACHIEVEDと判定される）。
ただしEPS Analyzer自体の表示（stock.html・quarterly.json）には
-95.004という誤った値がそのまま残っており、それを直接参照する他の
指標（YoY成長率等）に波及している可能性がある。

#### 対応方針（未確認・要調査）
`extract_key_facts.py`のLITE（および他社の10-K）向け四半期導出ロジックを
確認し、通期実績を単一四半期として誤って採用しているケースがLITE以外にも
ないか横展開調査した上で、Q4=通期−Q1〜Q3累計の正しい導出に修正する。

#### 着手条件
なし

---

### [EPS-AI-ANALYSIS-LATEST-ONLY-1] EPS Analyzer ai_analysisが最新四半期のみ・過去四半期に遡及されない
**優先度:** 中
**分類:** 機能ギャップ / EPS Analyzer
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ8（AS-IS-270/271）

#### 内容
`pipeline.py`は`quarterly_results[0]`（最新のみ）に対して
`analyze_adjustments()`を呼ぶため、過去四半期の調整項目についてはAIに
よる健全性評価（health/comment/sources）が生成されない。

#### 対応方針
過去四半期についても遡及的にAI分析を生成するか、意図的な設計（コスト
抑制目的）であることを明示するかを判断する。

#### 着手条件
なし

---

### [FIVE-CATEGORY-RECLASSIFY-1] 5分類レベルの再判定（AS-IS-437〜441・404・057/058/060）
**優先度:** 中
**分類:** ドキュメント整合性 / 分類見直し
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ9・フェーズ10

#### 内容
①AS-IS-437〜441（TANUKI TAIL `tail_kpi_map.json`関連5項目）は「手動入力
データ」（AS-IS-425〜436と同一のAI下書き＋人手承認ワークフロー）に酷似
しているが、ステップ7の一次分類時点で「導出データ」側に区分された。
②AS-IS-404（`last_filed`）はフェーズ1で定義した「システム設定データ
（監視状態管理系）」と同種の性質だが「その他」に取り残されている。
③AS-IS-057/058/060（Reverse DCF比較表のメタ情報行「場所」「用途」
「ガード」）は実データ値ではなく「実装差異の比較分析」自体がAS-IS番号を
持ってしまっている。いずれも`DERIVED_DATA_SUBCATEGORIES.md`の8分類内の
再配置ではなく、より上位の5分類（一次データ／手動入力データ／移送
データ／システム設定データ／導出データ）自体の再判定が必要。

#### 対応方針
①②は5分類を手動入力データ・システム設定データへ変更するか判断する。
③はカタログから除外する（メタ情報であり出力データではないため）か、
現状維持するかを判断する。`TO_BE_FINAL_LIST.md`・
`DERIVED_DATA_SUBCATEGORIES.md`・`FIELD_DEFINITIONS.md`への反映が
必要になる。

#### 着手条件
なし

---

### [SP500-GSPC-MULTI-FETCH-1] S&P500/^GSPCの複数取得経路（クロスシステム+Market Pulse内4重取得、外部APIコストの実害は解消済み）
**優先度:** 低（2026-08-13、中から引き下げ。理由は下記「2026-08-13更新」参照）
**分類:** 効率化 / 重複取得 / MACRO PULSE / Market Pulse
**登録日:** 2026-07-23
**更新日:** 2026-08-13（重複計算パターン棚卸し調査〈チャット記録〉で
実コードを再確認。`collect_and_send.py`の`^GSPC`参照は現在
`_get_sp500_ma_deviation()`内3箇所（`_md_get_ma_deviation()`×2・
`_md_get_price_series()`×1）＋`fetch_recent_records()`呼び出し3箇所
（主要9銘柄ブロック・NYSE Composite divergence用・大型対小型比用）の
計6箇所に整理されているが、いずれも`common.market_data.reader`経由の
ローカルファイル読み取りに統一済み（`[[MARKETDATA-LAYER-
CONSTRUCTION-1]]`実装済み）。**外部API直接呼び出しは0件になっており、
当初の実害（外部APIコストの無駄な重複）は既に消滅している。**
一方、対応方針が挙げていた「Market Pulse内部の複数箇所を1回の取得
結果を使い回す設計に統合する」というコードレベルの集約リファクタリング
自体は未実施のまま（呼び出し箇所は依然として独立）。実装しても実害
削減効果はほぼゼロ（ローカルファイル読み取りの重複コストは無視できる
水準）で、コード整理としての価値のみのため優先度を「中」から「低」へ
引き下げる）
**発見:** `FIELD_DEFINITIONS.md`フェーズ1・フェーズ10（AS-IS-190/312）・`CONCEPT_PARAMETER_VARIATIONS.md`軸2概念5

#### 内容
MACRO PULSE（FRED `SP500`優先→stooqフォールバック）とMarket Pulse
（yfinance `history()`）が完全に独立した経路でS&P500を取得しているのに
加え、Market Pulse単体のスクリプト内だけで`^GSPC`が少なくとも4箇所
独立にyfinance取得されている（主要9銘柄ブロック・NYSE Composite
divergence用・大型対小型比用・`sentiment.sub_scores.sp500_ma_dev`及び
両checklistのMA200判定用）。いずれもキャッシュ・再利用されていない。
（登録時点＝yfinance直接呼び出し時代の記述。2026-08-13時点の実コード
状況は上記「更新日」参照）

#### 対応方針
Market Pulse内部の複数箇所はまず1回の取得結果を使い回す設計に統合する
（低コストで対応可能。ただし2026-08-13時点で外部APIコストの実害は
解消済みのため、着手はコード整理目的の優先度低タスクとして扱う）。
MACRO PULSE・Market Pulse間の統合は`INPUT_DATA_TOBE.md`のyfinance
統合層設計に委ねる（`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`で実施済み）。

#### 着手条件
なし

---

### [MARKETPULSE-MINOR-INCONSISTENCIES-1] Market Pulseの軽微な構造的不整合まとめ（①②③④⑤は2026-08-26対応完了・⑥は再確認済み・休眠状態のため優先度低で据え置き）
**優先度:** 中→低（①②③④⑤は2026-08-26完了。⑥は再確認の結果、現状ライブ
データに実害なし〈休眠状態〉と判明したため優先度を低へ引き下げ）
**分類:** データ品質 / Market Pulse
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ10

#### 内容
①Hindenburg Omen判定が実測`total_stocks`（503件）ではなく固定値500を
ハードコード。②`credit.stock`（^GSPC指数）と`credit.bond`の株式側判定
（SPY ETF）が同一の`credit`ブロック内で異なる原資産を参照。
`credit.credit`（HYG/LQD各ETFのchange_percent差分）とAS-IS-328（HYG対
LQD比、比率そのものの変化）も独立した別計算経路。③`market_data.csv`は
`CSV_COLUMNS`に列挙されていないフィールド（NASDAQ本体・全volume_ratio・
tech_pulse/asset_flow/credit/両checklist/fear_greed/comments_history）を
無条件に欠落させる。④`sentiment.breadth`は`breadth_data.json`の単純な
パススルーだが`unchanged`/`ad_ratio_1d`/`total_stocks`/`rsp_return_1d`/
`spy_return_1d`がパススルー対象から漏れている。⑤CNN（`fear_greed`
パッケージ）とfeargreedchart.comという2つの異なるF&G情報源が一部の
フォールバック・後方互換コードで区別なく代替される。⑥`backfill_tech_
pulse.py`のTech Pulseスコア計算式（固定レンジ加算方式）が現行
`collect_and_send.py`（90日パーセンタイル方式）と全く異なり、過去の
バックフィル値と最近の値は単純比較できない。

#### 対応方針
①実測`total_stocks`を使うよう修正②原資産を統一するか意図的差異である
旨を明示③CSV出力対象フィールドを見直すか、CSVとJSONの非互換性を明示
④パススルー対象フィールドを追加⑤2つのF&G情報源を明確に区別する
⑥バックフィル済みデータの再計算要否を判断する。優先順位を付けて
順次対応する。

#### 2026-08-26 再検証（実コード・実データで6件全て再確認、対応方針の実施はまだ）
BACKLOG記載の前提を着手時に再検証する原則に従い、`collect_and_send.py`
（実装は`src/market/market_pulse/collect_and_send.py`、登録時から行番号は
シフトしているが該当箇所は現存）を1件ずつ実コード確認した。**6件とも
未解消のまま現存**、優先度「中」は妥当と判断した。

- **①Hindenburg固定値500**: `collect_and_send.py:1659`
  `hindenburg_active = bool(nh >= 500 * 0.022 and nl >= 500 * 0.022)`が
  現存。一方`breadth_data.json`最新エントリには`"total_stocks": 501`が
  既に存在しており（`_load_latest_breadth()`で読み込み済み）、実測値を
  使わず固定値500のままなことを確認
- **②credit原資産不整合**: `collect_and_send.py:1436`の`credit_stock`は
  `structured_data.get("S&P500")`（`^GSPC`指数）を参照する一方、
  `collect_and_send.py:1444`の`credit_bond`判定は
  `asset_flow_data.get("equity")`（SPY ETF、コメント「SPY(equity)下落」）
  を参照しており、同一`credit`ブロック内で異なる原資産が現存
- **③CSV列欠落**: `collect_and_send.py:1535`の
  `csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction='ignore')`が
  `row`のうち`CSV_COLUMNS`外のキーを無条件に無視する構造は不変。`row`は
  `structured_data`＋`sentiment_score`/`label`/`summary`のみから構築され、
  `tech_pulse_data`/`asset_flow_data`/`credit_data`/両checklist/
  `fear_greed_data`/`comments_history`はCSV化のロジック自体に一切
  含まれておらず欠落が現存（JSON`market_data.json`側には保存されている）
- **④breadthパススルー漏れ**: `collect_and_send.py:323-337`の
  `breadth_summary`は`breadth`（`breadth_data.json`最新エントリ、実際に
  `unchanged`/`ad_ratio_1d`/`total_stocks`/`rsp_return_1d`/
  `spy_return_1d`を含むことを実データで確認済み）から特定フィールドのみ
  ホワイトリスト方式でコピーしており、この5フィールドは現在も
  コピー対象に含まれず欠落が現存
- **✅⑤F&G情報源混同**（2026-08-26③修正完了、下記参照）: 旧実装は
  `_load_div_history()`のフォールバック（`divergence.value`未記録の
  旧エントリ向け）が`tech_pulse.components.fg_score`（feargreedchart.com
  由来）を参照しており、docstringの「CNN F&Gベースで一貫性が保たれる」
  という主張と矛盾していた。`(entry.get("fear_greed") or
  {}).get("score")`（CNN）を参照するよう修正済み
- **⑥Tech Pulseバックフィル式相違**: `backfill_tech_pulse.py:121-130`
  の`_calc_score()`（docstring「固定範囲方式 Tech Pulse スコア
  （バックフィル専用）」、加算方式）と`collect_and_send.py:711-729`の
  `calc_tech_pulse_score()`（`scipy.stats.percentileofscore`による
  90日パーセンタイル方式）が別計算式のまま現存を確認。同スクリプトは
  GitHub Actionsワークフローに未登録（`grep -rln backfill_tech_pulse
  .github/workflows/`で0件）で手動実行専用のため、既にバックフィル
  済みの過去データのみに影響が限定される（新規の日次汚染は発生しない）

**横断点検（Market Pulseのcron依存関係）**: `Market_Pulse_Update.yml`は
単一cron（`35 21 * * 1-5`、月〜金）・単一ジョブ内で
`breadth_calculator.py`→`collect_and_send.py`を順次実行する構造のため、
TANUKI VALUATIONで past発見された`[[TANUKI-VALUATION-PRICE-SCHEDULE-
LAG-1]]`のようなワークフロー間タイミング競合のリスクは構造上存在しない
ことを確認した（他ワークフローからの`workflow_run`依存もなし、単独で
完結）。他に構造的な新規問題は発見しなかった。

#### 2026-08-26② ⑤F&G情報源混同の追加調査（調査のみ・実装なし。
前回判定の訂正を含む）

MACRO-TRUTHY-ZERO-BUG-1対応完了を受け、優先順位に従い次に着手した
本項目の詳細調査で、**前回（2026-08-26①）の実害判定に誤りがあった
ことが判明したため訂正する**。

**発生メカニズムの再確認**（実コード引用、行番号は現状に一致）:
- 「今日」の値: `collect_and_send.py:1621-1628`
  `fg_cnn_score = (fear_greed_data or {}).get("score")` →
  `div_value = round(float(tp_score) - float(fg_cnn_score), 1) if
  fg_cnn_score is not None else None`（CNN由来）
- 過去データ再構築のフォールバック: `collect_and_send.py:667-675`
  `divergence.value`が欠損している場合、
  `tech_pulse.components.fg_score`（`fg_score_tech`、
  feargreedchart.com由来）で代替計算する
- **由来の経緯（`git log`で確認）**: 2026-06-14に2コミットが同日連続で
  入っている。`886654a97`（14:05、当時は「今日の値」もfeargreedchart.com
  由来だったため両者をfeargreedchart.comに揃えた）→`39be125a6`
  （15:55、90分後。「今日の値」をCNN由来へ切替、docstringも
  「CNN F&Gベースで一貫性が保たれる」と書き換えたが、**フォールバック
  側の参照先修正が漏れた**）。設計上の不整合自体はこの時点から現存

**実害の再定量化（訂正）**: `_load_div_history()`のロジックを実データ
（`market_data.json`全131件）に対して直接シミュレートした結果:
- 90日ウィンドウ内の`div_hist`（実際にZスコア計算に使われる系列）:
  長さ80件、**全80件が`divergence.value`（CNN由来）から取得**、
  feargreedchart.comフォールバックからの取得は**0件**
- `divergence.value`欠損の10件（2026-05-28〜06-07）は、前回「フォール
  バックが発火している」と誤認したが、実際には`tech_pulse`ブロック
  自体（`score`含む）が丸ごと`null`（Tech Pulse機能導入以前の旧
  スキーマ時代のエントリ）であり、フォールバック条件`tp_s is not
  None and fg_s is not None`を満たさず**完全にスキップされている**
  （Zスコア計算に一切寄与しない）
- 全131件の履歴を通じて、このフォールバックが実際に発火した
  （＝`divergence.value`欠損かつ`tp_s`・`fg_s`とも取得できた）事例は
  **ゼロ件**
- **前回判定「潜在バグではなく現在進行形で発火中」は誤りであり撤回する。
  正しくは「コード上の設計不整合は現存するが、現在の実データでは
  一度も発火したことがない（潜在的リスクに留まる）」**

**潜在リスクとしての性質**: `calc_tech_pulse_score()`はCNNの成否と
無関係にscipy/QQQ/VXNデータのみから算出されるため、理論上は「CNN取得
が失敗しTech Pulse計算だけ成功する日」が発生すればフォールバックが
発火しうる。ただし実データ131件中この組み合わせは一度も発生していない
ことを確認済み。加えて、`divergence.value`欠損10件は2026-09-05頃には
90日ウィンドウから自然に外れるため、現行データに起因する発火条件は
今後さらに希少化する（コード自体の設計不整合は残存するため優先度は
維持するが、緊急性は低い）。

**下流への伝播**: 現状ゼロ（`div_hist`が100%CNN一貫のため、
`div_zscore`・`tp_signal`とも汚染なし）。

**修正方式の選択肢（実装はまだしない）**:
- 案A（推奨）: フォールバックの参照先を`components.fg_score`
  （feargreedchart.com）から`(entry.get("fear_greed") or
  {}).get("score")`（CNN）へ差し替える。docstringが既に主張している
  内容を実際に真にする1行修正。実装規模: 小（1行＋docstring調整＋
  テスト1件追加）
- 案B: フォールバック自体を廃止し、`divergence.value`欠損日は単純に
  スキップする。CNN取得失敗日は案Aでも`fear_greed.score`がNoneのため
  結局値が取れず、実質的な結果は案Aとほぼ同じ。実装規模: 小
  （コード削減のみ）
- 案C: 欠損日を前日値でforward-fillする。データ完全性は上がるが
  Zスコアの分散を人為的に縮小させる副作用があり非推奨。実装規模: 中

**横断確認**: `collect_and_send.py`全体を"CNN"/"feargreedchart"で
確認した結果、同種の「複数情報源の無区別混在」はこの1箇所のみ。
隣接する既知課題（②credit.stock/bondの原資産不一致）は既存カタログ
済みで別種の問題であり、新規の類似箇所は発見しなかった。

#### 2026-08-26③ ⑤F&G情報源混同の実装完了（案A採用）

**修正内容**（`src/market/market_pulse/collect_and_send.py:667-679`）:
`_load_div_history()`のフォールバックの参照先を
`(entry.get("tech_pulse") or {}).get("components", {}).get("fg_score")`
（feargreedchart.com由来）から`(entry.get("fear_greed") or
{}).get("score")`（CNN由来、当日の`div_value`算出と同一ソース）へ
差し替え。docstringも「どちらの経路もCNN F&Gベースで一貫性が保たれる」
へ修正し、実態と一致させた。

**追加テスト**（`tests/test_collect_and_send_market_data_switch.py::
TestLoadDivHistory`、4件）:
- `test_uses_stored_divergence_value_when_present`: `divergence.value`
  存在時はそのまま使う（既存挙動の回帰確認）
- `test_fallback_uses_cnn_score_not_feargreedchart_score`:
  `divergence.value`欠損時、CNN score=30・feargreedchart.com
  score=57という意図的に大きく異なる値を与え、結果が
  `72-30=42.0`（CNN）であり`72-57=15.0`（feargreedchart.com）では
  ないことを検証。**修正前のコードに対して実行すると`[15.0]`を返し
  失敗することを確認済み**（`git stash`で一時的に修正前へ戻して
  実行、regression testとして機能することを確認した上で修正を復元）
- `test_entry_missing_both_scores_is_skipped`: `tech_pulse`ブロック
  自体が丸ごと`null`のエントリ（旧スキーマ）は完全にスキップされる
  ことを検証（2026-08-26②で確認した実データの挙動を固定化）
- `test_entry_outside_window_excluded`: window日数外のエントリが
  除外されることを検証

**影響確認**: 修正後、本番`market_data.json`（131件）に対して
`_load_div_history(window=90)`を実行した結果、`div_hist`は修正前と
同じ**80件**（全件CNN由来の`divergence.value`から取得、フォール
バック発火は0件）で完全に一致することを確認した。2026-08-26②で
確認済みの通り、現行データではフォールバックが一度も発火していない
ため、この修正による既存の表示・スコアへの影響はない（想定通り）。

**検証ゲート結果**（全て通過）:
- `pytest tests/`: **909 passed, 0 failed**（新規4件含む、既存905件
  無変化）
- `python common/sec_data/audit.py`: 🟢正常95銘柄/🟡警告5銘柄
  （既存WARN、Market Pulse非対象で無変化）
- `python common/sec_data/report_consistency_check.py --fail-on-ng`:
  NG=0件/WARN=96件（既存WARN、Market Pulse非対象で無変化）
- Market Pulse専用の検証スクリプトは存在しない（`find`で確認済み、
  MACRO PULSEの`05_audit.py`に相当するものはなし）

#### 元の着手条件（2026-08-26①登録時点のもの、下記②で①③④実装完了）
①②③④⑥は引き続き未着手（優先順位は次回Koichiさんと相談）

---

#### 2026-08-26② ①③④の実装完了・②の再確認結果（実装待機）

`[[MACRO-TRUTHY-ZERO-BUG-1]]`・`[[HOLLOW-RALLY-DEAD-1]]`対応完了を受け、
優先度順に①③④⑤の残り4件（⑤は既に完了済み）のうち①③④に着手した。
②は再確認の結果、原資産の選び方に設計判断が絡むため実装せず報告に
とどめる（Koichiさんの指示通り）。

##### ①Hindenburg固定値500の修正（実装完了）
`collect_and_send.py:1665`付近の`hindenburg_active = bool(nh >= 500 *
0.022 and nl >= 500 * 0.022)`を、`breadth_data.json`の実測
`total_stocks`を参照する形に修正した。テスト容易化のため、`__main__`
ブロック内にインラインで書かれていた計算ロジックを`calc_hindenburg_
active(breadth)`という独立関数へ切り出した（既存の`_get_tp_signal()`
等と同じ設計パターン）。

**実データでの出力差分確認**: `breadth_data.json`（93件）に対し修正前後
のロジックを実行して突合した結果、**4件で判定が変化**（いずれも
`nl=11`ちょうどの境界事例、`total_stocks=503`のため新閾値
11.066>11となり、旧ロジックの誤った「シグナル発生」判定〈True〉から
正しい「シグナルなし」〈False〉へ訂正された）: 2026-04-02・
2026-04-23・2026-07-20・2026-07-22。**旧ロジックはTAKE PROFIT
チェックリストのヒンデンブルグ・オーメン項目でこれら4日間、誤って
「シグナル発生」を計上していたことが判明した**（実害の確定）。

##### ③CSV書き出しフィールド欠落の修正（実装完了）
`CSV_COLUMNS`と実際の`structured_data`の全キー・サブキーを実データ
（`market_data.json`最新エントリ）で突合し、欠落フィールドを網羅的に
特定した。当初のBACKLOG記載「NASDAQ本体・全volume_ratio」に加え、
再確認で`NYSE Composite_divergence_vs_sp`（既存コードで計算済みだが
CSV_COLUMNS未登録）も同型の欠落として新たに発見し、合わせて追加した:
`NASDAQ_value`/`_change`/`_change_percent`/`_volume_ratio`、
`VIX指数_volume_ratio`、`米10年債_volume_ratio`、`ドル円_volume_ratio`、
`S&P500_volume_ratio`、`WTI原油_volume_ratio`、`金（GOLD）_volume_
ratio`、`HYG（ハイイールド債ETF）_volume_ratio`、`LQD（投資適格債
ETF）_volume_ratio`、`NYSE Composite_divergence_vs_sp`（計13列追加）。

**tech_pulse/asset_flow/credit/両checklist/fear_greed/comments_
historyは今回のスコープから意図的に除外した**: これらはネスト
した辞書・リスト構造（例: `checks`が複数チェック項目のリスト、
`comments_history`が無制限に伸びる履歴リスト）であり、フラットな
CSV列として機械的に単純追加できる性質のものではなく、どのサブ
フィールドをどう平坦化するかという設計判断が別途必要なため
（②と同種の「単純な修正で済まない」ケースと判断し、対応不要——
JSON側〈`market_data.json`〉に完全な形で既に保存されているため、CSVは
元々フラットな時系列比較用の補助出力という位置づけで割り切ることも
妥当と考える）。

**実データでの動作確認**: 実際に`save_data_to_json_and_csv()`を
一時ディレクトリに対して実行し、新規13列が正しく書き込まれること、
既存列（`VIX指数_value`等）に変化がないこと、および**既存の
`market_data.csv`（旧ヘッダー）から新ヘッダーへの自動マイグレーション
機構（`CSVヘッダー整合チェック`）が、既存行のデータを保持したまま
新列を空欄で追加することを実際に確認した**（本番`market_data.csv`は
次回実行時に自動的に安全移行される）。

##### ④breadth_summaryホワイトリスト欠落の修正（実装完了）
`compute_sentiment()`内の`breadth_summary`辞書に、パススルー対象から
漏れていた5フィールド（`unchanged`/`ad_ratio_1d`/`total_stocks`/
`rsp_return_1d`/`spy_return_1d`）を追加した。実データを模したダミー
`breadth`辞書で`compute_sentiment()`を実行し、5フィールド全てが
正しく`breadth_summary`へ反映されること、既存フィールドに変化が
ないことを確認した。

##### テスト追加（`tests/test_collect_and_send_market_data_switch.py`、8件）
- `TestCalcHindenburgActive`（4件）: `None`/空dict入力・`total_stocks`
  実測値使用時の境界ケース再現（`nl=11`かつ`total_stocks=503`で
  不発火）・実際に発火するケース・`total_stocks`欠損時の500
  フォールバックを検証
- `TestBreadthSummaryFields`（2件）: 5フィールドのパススルー確認・
  `breadth=None`時の既存挙動（`breadth_summary=None`）の非破壊確認
- `TestSaveDataCsvFields`（2件）: NASDAQ・volume_ratio系フィールドの
  CSV書き込み確認・旧ヘッダーCSVからの無損失マイグレーション確認

8件中7件は修正前コード（`git stash`で一時的に戻して実行）に対して
実際に失敗する（`KeyError`等）ことを確認済み（残り1件
`test_none_breadth_yields_none_summary`は変更のない既存挙動の確認
のため、修正前後どちらでもPASSして正しい）。

##### 検証ゲート結果（全て通過）
- `pytest tests/`: **920 passed, 0 failed**（新規8件含む、既存912件
  無変化）
- `python common/sec_data/audit.py`: 🟢正常95銘柄/🟡警告5銘柄
  （既存WARN、Market Pulse非対象で無変化）
- `python common/sec_data/report_consistency_check.py --fail-on-ng`:
  NG=0件/WARN=96件（既存WARN、Market Pulse非対象で無変化）
- Market Pulse専用の検証スクリプトは存在しない（前回確認済み）

##### ②credit原資産不一致の再確認結果（実装は行わず報告のみ）

**正確な使用箇所の再確認**:
- `credit_stock`（`collect_and_send.py:1471,1475`）:
  `structured_data["S&P500"]["change_percent"]`——`get_realtime_data()`
  の`main_tickers`ループで`^GSPC`（S&P500**指数**そのもの）から取得
- `credit_bond`（同1479-1487）:
  `asset_flow_data["equity"]["change_pct"]`——**別関数**
  `collect_asset_flow()`（SHV/DGS3MO/GLD/TLT/LQD/HYG/SPYの7資産クラス
  を「安全→リスク」順で並べた「資産クラス間資金フロービジュアライザー」
  専用のデータ収集）で`SPY`（S&P500**ETF**）から取得。TLT（債券）との
  「質への逃避」比較に使われる

**問題の性質の切り分け**: 「同一credit カテゴリ内での無区別な混在」
というより、**目的の異なる2つの独立した計算経路**であることを確認
した。`credit_stock`は「市場全体の今日の方向性」を測る単純な指標
（指数の正確な値が適切）、`credit_bond`は「資金が株式から債券へ
逃避しているか」という**資金フロー**概念（実際に売買可能なETF＝
SPY/TLTの比較が必須、指数自体には資金は流出入しない）。この観点では
両者とも個別に見れば合理的な設計選択でありうる。

ただし実データで検証した結果、**単なる理論上の懸念ではなく、実際に
無視できない乖離が周期的に発生していることを確認した**:
- 直近90日の日次サンプルで`|^GSPC変化率 - SPY変化率|`の中央値は
  0.029pt・平均0.068ptと小さいが、**最大0.673pt**（2026-06-27〜29の
  3日間連続）を観測した
- この3日間はSPYの`change_pct=-0.723%`に対し`^GSPC`の`change_percent`
  は`-0.05%`とほぼ横ばい——SPYの四半期分配落ち（ex-dividend、SPYは
  四半期分配のETFで分配日に価格が理論分配額だけ機械的に下落するが
  指数自体はこの影響を受けない）が原因と推定される
- 今回の3日間はTLT側の条件（`>0.3%`）が`0.011%`で不成立だったため
  `credit_bond`の最終判定自体は変わらなかったが、**この乖離幅
  （0.673pt）は`credit_bond`が使う閾値`-0.5%`より大きく、TLT側条件が
  同時に成立する別の局面では、原資産の選択（^GSPC vs SPY）が
  `credit_bond`の最終判定を実際に左右しうる**ことを確認した

**修正の選択肢**:
- 案a: `credit_stock`もSPY（`asset_flow_data.equity`）に統一する。
  長所: `credit_data`内で完全に同一instrument系列に統一され概念的に
  一貫。短所: `asset_flow_data`は`collect_asset_flow()`という別関数の
  取得失敗に連動して`credit_stock`まで巻き込まれ判定不能になる
  （現状は`structured_data`の`S&P500`から独立して取得できるため
  この結合リスクがない）。四半期分配落ちの影響を毎回受ける。
  実装規模: 小
- 案b: `credit_bond`も`^GSPC`（`structured_data.S&P500`）に統一する。
  長所: 同上の一貫性。短所: 「資金の質への逃避」という資金フロー
  概念上、指数自体には資金流出入という概念が存在しないため、TLTとの
  比較対象として指数を使う設計的な妥当性が薄れる（`collect_asset_
  flow()`の設計意図・7資産クラスの並びとも整合しなくなる）。
  実装規模: 小
- 案c（暫定推奨、判断根拠は要相談）: 現状維持（意図的差異として
  容認）とし、コード上のコメントで設計意図（`credit_stock`=指数の
  正確な日次方向性、`credit_bond`=資金フロー概念で実際の売買可能
  ファンドが必須）を明記する。長所: 各指標の目的に応じた合理的な
  instrument選択を維持でき、独立取得による耐障害性も保たれる。
  短所: 四半期分配落ち時の乖離（最大0.673pt確認済み）が将来
  `credit_bond`の判定を左右する潜在リスクは残る（発生頻度は
  四半期に数日程度と推定、影響はTLT側条件が同時境界に来た場合のみ）。
  実装規模: 極小（コメント追記のみ）

②についてはKoichiさんのご判断を仰いでから着手する。

##### 2026-08-26③ ②の対応完了（案c採用、コメント追記のみ）
Koichiさんの判断により**案c（現状維持・意図的差異として容認）**を採用。
`collect_and_send.py`の`credit_stock`・`credit_bond`それぞれの計算箇所に
以下をコードコメントとして明記した（ロジック自体は一切変更していない）:
- なぜ異なるinstrument（`credit_stock`=^GSPC指数、`credit_bond`=SPY ETF）
  を使うのか（前者は市場全体の日次方向性を測る単純指標のため指数が適切、
  後者は資金フロー概念のため実際に売買可能なファンドが必須）
- 統一案（②案a/b）をそれぞれ見送った理由（案a: 結合による耐障害性
  低下、案b: 資金フロー概念との不整合）
- 残存リスク（SPYの四半期分配落ち時に^GSPCとSPYで最大0.673pt〈実測
  済み〉の乖離が生じ、`credit_bond`の閾値`-0.5%`を上回るためTLT側
  条件が同時に境界へ来る局面で判定を左右しうること）

**検証**: コメント追記のみでロジック変更がないことを`git diff`で
追加・削除行が全てコメント行（`#`始まり）または空行のみであることを
確認した上で、`pytest tests/`: **920 passed, 0 failed**（変化なし、
想定通り）を確認した。

##### 2026-08-26④ ⑥の再確認結果（調査のみ・実装なし、優先度低のまま据え置き）

`[[MACRO-TRUTHY-ZERO-BUG-1]]`・`[[HOLLOW-RALLY-DEAD-1]]`・②③④の対応
完了を受け、最後に残った⑥（`backfill_tech_pulse.py`のTech Pulseスコア
計算式が現行`collect_and_send.py`と異なる件）を再確認した。

**ワークフロー登録予定の有無**: `.github/workflows/`配下に
`backfill_tech_pulse.py`への参照は**0件**（`grep -rln`で確認）。
`SYSTEM_MAP.md`/`PROJECT_STATUS.md`/`INPUT_DATA_TOBE.md`の本スクリプト
言及箇所は全て2026-08-12〜13の「新DB構築プロジェクト」（yfinance/FRED
統合層への配線切替）に関するもののみで、将来ワークフロー化する計画を
示唆する記述は見当たらなかった。git履歴上、直近の変更は2026-08-13
（配線切替のみ）、スコア計算式自体の最終変更は2026-05-21（3ヶ月以上
前）。新DB構築プロジェクト自体は2026-08-13付けで「本線タスク完了」
としてクローズ済み。今後もワークフロー未登録・手動専用のままである
可能性が高いと判断する。

**新たな発見（実データ確認）**: `market_data.json`を確認したところ、
`tech_pulse`が入っている80件は全て2026-06-08〜2026-08-26の連続した
期間で、いずれも`divergence.value`が同時に存在する（＝ライブ収集＝
現行パーセンタイル方式で書かれたエントリの特徴、⑤調査で確認済みの
パターン）と一致していた。**現在のライブデータには、旧・固定レンジ
方式（バックフィル）で書かれたスコアは1件も存在しない**——
`tech_pulse`が空欄のまま残っている51件（2026-04-04〜06-07）は、
バックフィルが一度も実行されないまま放置されている状態である。

**結論**: 「新旧の値が混在して単純比較できない」という実害は、
**バックフィルが実際に実行されるまでは発生しない**（現状は完全に
休眠状態）。実害が顕在化するのは、将来誰かが`backfill_tech_pulse.py`
を実行してこの51件の空欄を埋めた場合のみ。

**実装規模感の見積もり**: 式を揃えるには`calc_tech_pulse_score()`
（90日パーセンタイル方式）が必要とする「対象日を含む直近90日分の
`qqq_vs_ma125`/`vxn_vs_ma50`/`qqq_vs_spy_20d`のローリング履歴」を、
バックフィル対象の各日について新たに構築する必要がある（現状は対象日
1点のみ計算）。既存の`_qqq_components()`/`_vxn_components()`はそのまま
再利用可能、`collect_and_send.py`から`calc_tech_pulse_score`を追加
importするだけで済むが、90日分×対象日数のループが増える。
**実装規模: 中規模**（既存ヘルパーの再利用は可能だが、ローリング履歴
構築ロジックの新規実装が必要、目安30〜50行程度）。追加の考慮点として、
既にバックフィル済みのエントリと新規ライブ収集エントリを区別する
マーカーが現状存在しないため、「過去にバックフィルされた分を新方式で
再計算する」対応は別途識別方法の検討が必要（ただし上記の通り現状は
該当エントリが0件のため、今この時点では再計算対象自体が存在しない）。

**Koichiさんの判断（2026-08-26）**: 上記結論に同意、優先度「低」のまま
据え置き。今回は実装しない。

#### 着手条件
①②③④⑤は完了。⑥は引き続き未着手のまま、優先度低で据え置き
（休眠状態のため緊急性なし。着手判断は次回相談）。

---

### [JNJ-XOM-PM-FLOOR-RISK-1] JNJ・XOM・PM・CONはrecommended_g候補が最低ラインでMO型floor転落の潜在リスクあり
**優先度:** 中
**分類:** データ品質 / TANUKI VALUATION / 監視対象
**登録日:** 2026-07-19
**発見:** [[GROWTH-SANITY-CLASS-SYNC-1]]（完了・BACKLOG_DONE.md参照）floor適用対象
範囲確認調査

#### 内容
`segment_config.json`未登録37銘柄のうち、`recommended_g`算出候補
（rev_cagr_3yr/5yr・g_fundamental・industry_benchmark）が**ちょうど
2件**（1件失えば`recommended_g`算出不能＝MO型のfloor転落リスク）の
銘柄が6件存在する: JNJ・PM・XOM・GEV・FLYW・PAYS。うちJNJ・PM・XOMは
成熟ディフェンシブ株（GEV/FLYW/PAYSは高成長株で候補不足が偶発的、
リスクの性質が異なる）。CONも`rev_cagr_5yr`算出不能（2024年スピンオフで
データ不足）な近接リスク銘柄。

実際のFCF生成長率（`calculate_fcf_cagr()`のraw_cagr、floor適用前）を
実データで確認したところ、**JNJ（+3.1%）・XOM（-31.5%）はMO
（+2.7%）と同等またはそれ以上にfloor(15%)との乖離が大きい**
（JNJ: floor gap +11.9pt、XOM: floor gap +46.5pt、MOの+12.3ptと同等〜
それ以上）。特にXOMはFCFが実際に大きく減少している局面（raw=-31.5%）で、
もしfloorが発動すれば「FCFが3割減っている最中の企業に年15%成長を
仮定する」という最も危険なミスマッチになる。

#### 対応方針（未確定）
即座の対応は不要（現時点でrecommended_g算出候補2件を維持できており
floorには落ちていない）。ただし次回以降の決算更新でJNJ/PM/XOM/CONの
`rev_cagr_3yr`/`5yr`のいずれかがマイナスへ転じ候補が1件以下になった
場合、MOと同型の`TICKER_INDUSTRY_OVERRIDES`追加＋
[[GROWTH-SANITY-CLASS-SYNC-1]]で実装した案B'（floor到達中かつ
industry_g単独1件の場合のみ候補数閾値を緩和）が既存の実装パターンとして
そのまま適用できる見込み。

#### 着手条件
候補件数が実際に2件を下回った場合（`report_consistency_check.py`等での
継続監視、またはgrowth_sanity再確認時に検知）

#### 定点確認（2026-09-04）
対象7銘柄（JNJ・PM・XOM・CON・GEV・FLYW・PAYS）の`growth_sanity`実データを
確認したところ、全銘柄で`floor_hit=False`。`recommended_g`算出候補数
（rev_cagr_3yr/5yr・g_fundamental・industry_benchmarkのうち非None数）は
2026-07-19登録時点（各2件）から増加していた: JNJ 4件・PM 3件・XOM 4件・
CON 2件・GEV 2件・FLYW 3件・PAYS 3件。データ蓄積によりCAGR系候補
（rev_cagr_3yr/5yr）が順次算出可能になったことによるもので、リスクは
軽減方向にある。着手条件（候補1件以下への転落）は引き続き未発生のため
対応不要、監視継続とする。

---

### [EPS-UPC-PREREORG-1] Up-C構造・組織再編前四半期のAdjusted EPS計算への算入方針
**優先度:** 中
**分類:** データ品質 / EPS ANALYZER / 設計方針
**登録日:** 2026-07-13
**発見:** [[ASTS-SHARES-OSCILLATION-1]]恒久修正時、BROS 2021-03-31の
復活データ妥当性確認調査

#### 背景
BROS（Dutch Bros、Up-C構造でIPO・組織再編は2021年9月）の2021-03-31
（Q1 2021、組織再編前）を一次情報（SEC EDGARライブ）で確認したところ、
以下の特異なパターンが判明した：
- **revenue**: $98,785,000（実在・妥当。FY2021通期$497,876,000から逆算した
  四半期進捗とも整合）
- **net_income**: 正確に`0`（端数なし）
- **調整項目**: `ShareBasedCompensation: $14,650,000`が存在

売上$98.8Mの実在企業がドル単位まで正確にゼロ利益、というのは通常の事業活動
としては不自然であり、**Up-C構造特有の会計処理の産物**と推測される：
組織再編（IPO）前は、SEC登録主体（PubCo）が事業会社（OpCo）の経済的持分を
まだ保有していないため、OpCoの実際の売上・損益とは無関係に、PubCo単体の
帰属純利益が形式的に$0となる。

[[ASTS-SHARES-OSCILLATION-1]]の恒久修正（隣接四半期からの株数引き継ぎ）に
より、この四半期の希薄化後株数が新たに埋まるようになった結果、
`adjusted_net_income = gaap_net_income(0) + 税引後SBC加算 ≈ プラスの値`と
なり、**PubCoの帰属利益が実質ゼロだった四半期に対して、見かけ上プラスの
Adjusted EPSが新たに算出される**ようになった。株数の引き継ぎ自体（実データに
基づく合理的な近似）は妥当だが、この四半期をAdjusted EPS計算にそのまま
含めてよいかは、今回の株数バグ修正とは別軸の設計論点である。

#### 対応方針（未確定・次回セッションで判断）
- Up-C組織再編前かつnet_income=0（または売上に対して不自然に小さい）四半期を
  機械的に検知し、Adjusted EPS計算から除外する、または「参考値」として
  別枠表示するかを検討する
- 「net_income=0だが売上は実在」というパターン自体を検知条件として使えるか
  （閾値・誤検知率を含めて設計）
- 他のUp-C構造銘柄（CART等、組織再編を経て上場した銘柄）で同型のケースが
  ないか横展開確認する（[[ASTS-SHARES-OSCILLATION-1]]の新旧比較でCARTも
  値変化が確認されている。組織再編前四半期を含むかどうかの個別確認が必要）

#### 着手条件
なし（次回セッションで検知方法・対応方針を設計してから着手）

---

### [SPLIT-REALTIME-GAP-REVERSE-1] KULR/SPIRのリバース分割で同型の恒久固着ギャップ有無が未確認
**優先度:** 低
**分類:** データ品質 / EPS ANALYZER
**登録日:** 2026-07-20
**発見:** [[SPLIT-REALTIME-GAP-1]]（完了・BACKLOG_DONE.md参照）実装時

#### 背景
SPLIT-REALTIME-GAP-1の実装前調査で行った全101銘柄横断スキャンは、フォワード
分割（`diluted_shares_used`が数倍に「ジャンプ」するパターン、比率>1のみ）を
検知対象としていたため、リバース分割（比率<1、株数が「減る」パターン）を
見落としていた。

BACKLOG_DONE.md「Phase 2b-3完了（2026-07-12）」の記述で、KULR・SPIRの2銘柄が
当時から`extract_key_facts.py`のfact選定ロジック修正の対象銘柄として言及
されていたことを再確認し、yfinanceでKULR（2025-06-23、1-for-8）・SPIR
（2023-08-31、1-for-8）のリバース分割が実在することを確認した。

ローカルキャッシュ（`docs/value-monitor/adjusted_eps_analyzer/data/{KULR,SPIR}/
quarterly.json`）を見ると、いずれも「高い値が数四半期続いた後、低い値へ
ジャンプし、以後低い値が続く」というNVDA型と鏡写しのパターンが見られる
（KULR: 2022-06-30〜2024-03-31が約104M〜142M→2024-06-30以降は約22.7M〜46.2M。
SPIR: 2022-03-31〜2022-06-30が約139M→2022-09-30以降は約17.5M〜33.3M）。
いずれも実際のリバース分割日より1年程度早いタイミングでジャンプしており、
SPLIT-REALTIME-GAP-1のNVDA等と同型の「翌年以降の10-Q再掲で先に是正された
四半期」＋「再掲機会がなく古い側の値が残存」という構造が疑われるが、
一次情報（SEC 10-Q/8-K）での確認・`apply_split_adjustments()`が
リバース比率（ratio<1）を正しく扱えるかのコード確認はいずれも未実施。

SCCO（yfinanceに2024年以降ほぼ毎四半期`~1.005-1.01`という極小の「分割様」
記録があるが、ローカルキャッシュのdiluted_shares_used系列はほぼ横ばい
〜緩やかな増加のみで明確なジャンプ/ドロップなし）は、特別配当等に伴う
yfinance側のデータ仕様上のノイズであり実分割ではないと判断、対象外。

#### 対応方針（未確定）
- KULR/SPIRそれぞれのSEC 10-Q/8-K一次情報でリバース分割日・比率を確認する
- `apply_split_adjustments()`の閾値計算（`pre_split_threshold = post_split_avg
  / ratio × 1.5`）がratio<1（リバース分割）でも意図通り機能するか
  （現状の実装はratio>1のフォワード分割のみで検証されている）をコードで確認する
- 実装するか否か・優先度はKoichiさんの次回判断待ち

#### 着手条件
なし（次回セッションで判断）

---

### [DATA-JUMP-CHECK-NETINCOME-SBC-1] 純利益・SBCの段差型急変検知（比率方式以外の代替方式検討）
**優先度:** 低（着手急がず）
**分類:** アーキテクチャ / 品質管理
**登録日:** 2026-09-06
**発見:** [[DATA-JUMP-CHECK-GENERALIZE-1]]実装時の実データ比率分布確認

#### 背景
[[DATA-JUMP-CHECK-GENERALIZE-1]]で`check_c_data_jump()`（YoY比率が閾値以上/
以下で発火する段差型検知）を売上総利益・CapExへ展開する際、当初は純利益・
SBCも対象候補としていたが、実データで比率分布を確認した結果、比率方式が
本質的に機能しないことが判明したため、この2フィールドは今回のスコープから
除外した：

- **純利益（pl.net_income）**: tanuki=true全100銘柄・直近6年のYoY比率477件中
  53件が負値（黒字↔赤字の符号反転）。符号反転を跨ぐ比率は数学的に意味を
  持たない（例: LITE 2025→2026: $25.9M→$-69.35億、比率-267.76倍という値
  自体が「267倍悪化」を意味しない）。閾値方式で符号反転を捕捉しようとすると
  「負の比率は全て閾値以下」という粗い判定にしかならず、実質的に「符号が
  変わったかどうか」の二値判定と変わらない
- **SBC（cf.stock_based_compensation）**: ゼロ近傍の小額から上場後の本格的な
  株式報酬制度導入で急増するケースが頻発し、実測でZETA（2020→2021、
  $105K→$259.16M、倍率2468.18倍）のような正当な急増が比率の上限を
  無意味化する。SBCはスタートアップ〜上場直後の企業で「ほぼゼロから
  始まり数年で定常化する」という成長曲線自体がありふれているため、
  段差型検知が想定する「タグ切替による不連続 vs 正当な急変」の区別が
  比率方式では原理的に困難

#### 対応方針（未確定・次回セッション以降で判断）
比率方式（YoY倍率）以外のアプローチを検討する必要がある。候補（いずれも
未検証、次回セッションで実データを見ながら判断）：
- 純利益: 符号反転自体を検知する二値チェック（「前年黒字→当年赤字」等の
  遷移を、[[BS-FIELD-NONE-TRANSITION-DETECT-1]]（WARN-26、有値→None遷移
  検知）と同型の「状態遷移検知」として設計する案
- SBC: 絶対額ベースの閾値（例: 直近年のSBCが売上の一定比率を超えて
  急増した場合のみ検知）、またはゼロ近傍を除外した上での比率方式再検討
- いずれも「NGにするには誤検知率が高すぎる」というWARN-21/44/45と同じ
  教訓が当てはまる可能性が高く、実装する場合もWARNレベルに留める前提で
  設計すること

#### 着手条件
なし（優先度含め次回以降のセッションで判断。急ぎではない）

---

### [FLAG-THRESHOLD-DESIGN-1] tanuki/stonks_silo等4フラグの判定基準ロジック導入（第二段階）
**優先度:** 未定
**分類:** アーキテクチャ / 銘柄登録フロー
**登録日:** 2026-07-12
**発見:** [[ZS-TICKERS-LEAK-1]]（完了・本ファイル上部参照）の消費者統一（第一段階）に伴う調査時

#### 背景
一連の調査（フラグ判定ロジック確認・基準設計材料収集）の結果、`cik_lookup.csv`の
4フラグ（tanuki/stonks_silo/eps/hypecore）は**完全手動設定**であり、財務指標等に
基づく自動判定は一切存在しないことが確認された。暗黙の基準（赤字→stonks_silo=true）は
概ね成立するが、以下の逸脱事例が判明している：

- **ESTC・LITE**: 黒字転換後もstonks_silo=trueのまま残留（直近1年基準では不一致）
- **GTLB**: 直近5年間**一度も黒字化していない**にもかかわらずstonks_silo=false
  （既知8件の逸脱事例には含まれていなかった新規発見。明示的な除外理由の記録もなし）
- **APGE**: 売上ゼロのプレレベニュー企業。「赤字」という1軸だけでは判定基準として
  不十分であることを示す事例（「売上の有無」も判定軸に含める必要）

#### 対応方針（未確定・基準案をKoichiさんに複数提示して確認後に実装）
- 直近5年中N年以上赤字継続→stonks_silo=true、といった機械判定可能な基準の導入
  （Nの値・判定ウィンドウは要確認）
- プレレベニュー企業・金融機関・IFRS企業・特殊株式構造企業等、「赤字」以外の
  軸での評価枠組み非適合パターンの明文化
- GTLB・ESTC・LITEの現行フラグ設定の是正（基準確定後に個別対応）
- フラグ再判定のトリガー（新規登録時のみか、定期棚卸しか）の設計
- tanuki=trueとstonks_silo=trueの併用は意図的な設計（SYSTEM_MAP.mdに明記の
  TANUKI VALUATION↔STONKS SILO runway参照依存）であり、「赤字企業は両方trueに
  する」という運用を基準に組み込む余地がある

#### 議論の要旨（2026-07-14追記・タスクの本質的なゴールの整理）
本日の一連の調査（DCF計算可否ロジック確認・Policy A/B判定ロジックの網羅調査・
AVAV/RDW/LITE/SITM個別調査）を経て、本タスクの本質的なゴールは
「stonks_silo判定基準を確定させること」単体ではなく、**「TANUKI VALUATION・
STONKS SILOのどちらの評価軸でも適切に評価しにくい銘柄をどう判定・振り分ける
か」**という、より広い問いであると整理された。

根拠：
- GTLB/ESTC/LITEの逸脱事例は、既存のDCF_Reliability判定（Policy A/B）を
  そのまま機械基準に転用しても再現できないことが判明した（[[POLICY-AB-
  TREND-BLIND-1]]で確認したPolicy Bの設計限界が一因）
- APGE（プレレベニュー）のような「赤字」以外の軸で評価不適合になる
  パターンが存在し、stonks_silo単独の基準では収まらない
- LITE/SITMの調査で判明したFCF推定ロジックの限界（[[SECTOR-FCF-RATE-
  BROKEN-1]]・[[FCF-CONVRATE-DESIGN-LIMIT-1]]）も、根本的には「TANUKI
  VALUATIONのDCFフレームワークが不得意とする事業特性の銘柄をどう扱うか」
  という同根の問題

**注意：上記は議論の要旨・方向性の整理であり、基準案の具体的な数値
（N年赤字継続のN等）やフラグ再判定の実装方針は本日時点でも未確定のまま。**
上記「対応方針（未確定）」の内容と矛盾するものではなく、着手時に
判断材料として踏まえるべき文脈を追記したもの。

#### 着手条件
なし（基準案の確認・確定後に着手）

---

### [STALE-SUBPORT-CLEANUP-1] src/subport/fg_level2/ 陳腐化複製の整理
**優先度:** 低〜中
**分類:** 保守性 / リポジトリ整理
**登録日:** 2026-07-11
**発見:** SYSTEM_MAP.md実態調査（2026-07-10）でAutoTrade運用実体を確認した際

#### 問題
AutoTrade（F&G Level2×TQQQ自動売買）の運用実体はリポジトリ外
`C:\Users\shigi\AutoTrade\fg_level2\`にあり、Windowsタスクスケジューラから
`trader.py --entry`/`--monitor`を日次実行している（signal.json/state.json/
trade_log.jsonlが実際に日次更新される）。一方、リポジトリ内
`src/subport/fg_level2/`は2026-05-03の開発初期に作成された同名モジュール一式
（trader.py/signal.py/config.json等）だが、2026-05-03以降git上で更新がなく、
内容が本番運用側と既に乖離している。`register_tasks.ps1`が`$RepoRoot`をこの
リポジトリパスに設定しているにも関わらず、実際には使われていない（詳細は
SYSTEM_MAP.md「AutoTrade/OpenD運用前提」参照）。

#### 対応方針
即削除はリスクがあるため、以下いずれかを判断する：
- A案: `src/subport/fg_level2/`が本番運用（リポジトリ外）から一切参照されて
  いないことを確認した上で削除する。削除の場合、他モジュールからの
  import参照がないことを`grep -rn "subport.fg_level2\|subport/fg_level2"`等で
  確認してから行うこと
- B案: 削除せず、README等を追加して「これは非稼働の旧複製であり、
  正は`C:\Users\shigi\AutoTrade\fg_level2\`である」と明示する

#### 影響
実害は薄い（本番運用に影響しない陳腐化コードの残存）が、将来このモジュールを
誤って参照・変更するリスクがあるため記録する。

---

### [TAIL-SEC-ITEMS-1] TANUKI TAIL SEC項目の保存拡張（Item 1/1A/3/7）
**優先度:** 中
**分類:** 機能追加 / TANUKI TAIL
**登録日:** 2026-06-27

#### 問題
現在はItem 4（内部統制）のみ保存・表示している。
以下の項目もItem 4と同様にEDGARから取得・保存・表示したい：
- Item 1: Business（事業概要）
- Item 1A: Risk Factors（リスク要因）
- Item 3: Legal Proceedings（法的手続き）
- Item 7: Management's Discussion and Analysis（MD&A）

#### 対応方針
sec_ctrl_fetcher.pyを拡張するか別スクリプトを作成するか設計判断が必要。
TAIL-CTRL-TRANS-1（2026-06-27完了）の構造を踏襲する。

---

### [EPS-1] アナリスト予想EPS四半期値の取得
- 現状: Next_Quarter_EPSはN/A（Alpha Vantage無料枠の制約）
- 問題: 四半期サプライズ率が計算できない
- 改善: 有料API検討 or yfinance の quarterly_earnings 活用

### [TANUKI-ROE-2] デュポン分解 業種平均比較・潜在ROE試算
**優先度:** 低
**状態:** 部分完了（2026-06-26）
- ✅ stock.htmlにDUPONT ANALYSISパネルを追加（4カード：純利益率・資産回転率・財務レバレッジ・ROE）
- [ ] 業種平均との比較表示（Damodaranにデータなし・データソース確保が必要）
- [ ] 潜在ROE試算（業種平均データ確保後に実装）

### [TANUKI-FIN-2] 金融機関銘柄（JPM・GS・SOFI）へのエクイティDCF並行評価対応
**優先度:** 低（設計相談は完了・実装未着手）
**分類:** 設計課題 / TANUKI VALUATION
**登録日:** 2026-07-06
**ステータス:** 保留（着手時期未定、AI側の調子が良いタイミングで着手予定）
**統合について（2026-09-05）**: TANUKI-FIN-1（金融機関向け
バリュエーション対応・DDM等、より一般的な構想）を本エントリへ統合した。
本エントリ（JPM/GS/SOFI向けエクイティDCF）の方が設計相談完了・具体化
された実行可能な設計のため主エントリとして残し、TANUKI-FIN-1の内容は
要約せず全文そのまま下記「背景（統合元TANUKI-FIN-1より）」に保持する。
TANUKI-FIN-1はBACKLOG.mdから削除済み。

#### 背景（統合元TANUKI-FIN-1より）
以下は独立BACKLOGエントリだった`[TANUKI-FIN-1] 金融機関向け
バリュエーション対応（DDM等）`の全文をそのまま転記したもの。

**優先度:** 中
**分類:** 設計課題 / TANUKI VALUATION

##### 背景
金融機関（銀行・保険・証券等）はFCFの概念がなじまず、TANUKI VALUATIONへの
登録が困難。一方で保有銘柄・ウォッチ銘柄に金融株が含まれるケースがある。

##### 対応方針（案）
- DDM（配当割引モデル）を新たなバリュエーション手法として導入
- TANUKI VALUATIONと横並びで主要データ（PER/PBR/ROE/配当利回り等）を保持できる
  金融株専用セクションまたは別フレームワークの設計
- 無理にFCFベースDCFに当てはめることを廃止

#### 背景
金融機関（JPM・GS・SOFI）は負債が事業構造そのものの一部であるため、通常の
FCFF（企業DCF）が適合しにくい。業界標準としてFCFEベースのエクイティDCFが
適合する。Vは決済ネットワーク型で通常のFCFF DCFが適合するため対象外。

**SOFI追加の経緯（2026-07-19）**: SOFI-DATA-1（銀行免許取得後にLongTermDebt
系タグの申告を停止し合算タグへ移行した問題のLTDebt恒久修正）、および
[[FY52WEEK-BS-STI-OVERRIDE-DESIGN-1]]のSOFI個別調査（流動/非流動を区分
しない銀行持株会社特有の非分類BS構造であることが判明）を通じて、SOFIも
JPM・GS同様「負債・投資有価証券が事業構造そのものの一部」という金融機関
特有の性質を持つことが確認された。これを契機に、対象銘柄にSOFIを追加する
意向が確定した。

#### 対応方針
- 案（A）: 既存のFCFF（企業DCF）は維持したまま、対象銘柄（JPM・GS・SOFI）
  について追加でFCFE（エクイティDCF）評価を並行実施し、latest.json/
  report.txt上で両方式の結果を比較できるようにする機能拡張とする
  （既存FCFFを置き換える「切り替え」ではなく「並行評価・比較」）
- 判定方式: SIC code等による自動判定ではなく、対象ティッカー
  （JPM・GS・SOFI）を設定ファイルに明記する方式を採用（対象が少数のため
  過剰実装を避ける）
- 今後対象銘柄が増える場合、自動判定ロジックへの切り替えを再検討する

#### 関連
- 元TANUKI-FIN-1（金融機関向けバリュエーション対応・DDM等、上記
  「背景（統合元TANUKI-FIN-1より）」参照）とは対象アプローチが異なる
  （DDMではなくFCFEエクイティDCF、対象は少数ティッカーのハードコード方式）。
  着手時にどちらの方式を採用するか、あるいは併存させるかを判断する。
- [[FY52WEEK-BS-STI-OVERRIDE-DESIGN-1]]（SOFIのshort_term_investments
  override設計）は、既存FCFFのNet Debt計算に引き続き使われるフィールドの
  ため、本エントリの着手を待たず独立に進めてよい（FCFE並行評価の実装
  時期に関わらず、既存FCFFの正確性向上に直接寄与するため）。
  **2026-07-19実装完了（`sti_concept=OtherInvestments`、BACKLOG_DONE.md
  参照）。既存FCFFのNet Debt計算にSOFIの正しいshort_term_investments値が
  反映される状態になった**。

---

### [SCREENING-SIGNAL-INTEGRATION-EPIC-1] スクリーニング判断への既存シグナル統合3件（元EPS-ANALYZER-INTEGRATE-1/RICE-INTEGRATE-1/ANALYST-VS-IV-INTEGRATE-1）
**優先度:** 中
**分類:** 機能統合 / TANUKI VALUATION / EPS ANALYZER
**登録日:** 各サブ項目とも2026-07-10。統合日: 2026-09-05
**発見:** 2026-09-05のBACKLOG横断整理

#### 統合の経緯
EPS-ANALYZER-INTEGRATE-1・RICE-INTEGRATE-1・ANALYST-VS-IV-INTEGRATE-1の
3件は、いずれも「report.txt/latest.jsonに既に出力されているが
スクリーニング判定（`common/screening/dcf_validity_checker.py`等）に
組み込まれていないシグナルを統合する」という共通テーマを持つため、
2026-09-05に1つのエピックへ統合した。元の3件はBACKLOG.mdから削除し、
内容は要約せず全文そのまま以下の①〜③に保持する。

#### ① 元[EPS-ANALYZER-INTEGRATE-1] スクリーニング判断へのEPS Analyzer統合
**優先度:** 中
**分類:** 機能統合 / EPS ANALYZER
**登録日:** 2026-07-10

##### 背景
EPS Analyzer（GAAP/Non-GAAP乖離・割安発掘）は独立したシステムとして
存在するが、TANUKI SCOREベースのスクリーニング判断に統合されていない。
report.txt [6]セクションにAdjustment_Delta・PER_Comparison
（Market_PER_GAAP vs Adjusted_EPS_PER）が既に出力されているが、
これを比較・除外判定に使う仕組みがない。

##### 実装方針
1. `common/screening/dcf_validity_checker.py`（2026-07-10格納済み）に、
   EPS Analyzerの PER_Comparison（Delta: GAAP PERとAdjusted EPS PERの差）を
   追加チェック項目として組み込む
2. Deltaが一定閾値以上（要検討：例えば±10x以上）の銘柄をフラグし、
   「SBCや一時費用の影響で見かけの割安度が歪んでいる可能性」として
   出力に含める
3. 既存のTANUKI乖離率と、Adjusted EPSベースのPERから逆算した
   簡易的な参考株価を並記できるか検討する

#### ② 元[RICE-INTEGRATE-1] スクリーニング判断へのRICE指標統合
**優先度:** 中
**分類:** 機能統合 / TANUKI VALUATION
**登録日:** 2026-07-10

##### 背景
RICE（投資効率指標、Matrix①投資効率系の軸）が計算可能な銘柄
（今回確認では55銘柄）について、スクリーニング時にRICE値を
参照していない。RICE>=3.0（高効率）/1.0-3.0（中効率）/<1.0（低効率）
という既存の閾値定義があるにもかかわらず未活用。

##### 実装方針
1. `common/screening/dcf_validity_checker.py`または
   `common/screening/report_txt_parser.py`の出力に、
   RICE値とその閾値区分（高/中/低効率）を追加フィールドとして含める
2. TANUKI SCORE=BUY かつ RICE<1.0（低効率）の銘柄を「割安だが
   再投資効率が低い」候補として別途フラグする運用を検討する
3. RICE Available=falseの銘柄（Revenue/CapExデータ不足）は
   従来通りMatrix②〜④で評価する

##### 関連（2026-07-10追記、2026-09-05更新）
[[FUTURE-FEATURE-IDEAS-CATALOG-1]]（旧MULTI-1、2026-09-05に同カタログへ
統合済み）のマルチバリュエーション表示とRICE指標の活用目的が部分重複する。
役割分担としては、本タスク（旧RICE-INTEGRATE-1）は**スクリーニング判定への
組み込み**（機械的なフラグ付け・除外候補の抽出）が主眼、旧MULTI-1は
**画面表示**（DCF/PEG/EV/Sales/RICE/HypeCoreの並列スコアカード表示）が
主眼という違いがある。将来的にどちらかへ統合するか、双方独立で進めるかは
どちらかの着手時に判断する。

#### ③ 元[ANALYST-VS-IV-INTEGRATE-1] アナリストコンセンサスとの突合せ
**優先度:** 中
**分類:** 機能統合 / TANUKI VALUATION
**登録日:** 2026-07-10

##### 背景
report.txtに「Analyst_Consensus ... vs IV: +151.4%」のような、
アナリスト目標株価とTANUKI理論株価の乖離が既に出力されているが、
スクリーニング判断に系統的に組み込まれていない。TANUKIは
「市場心理から独立した本源的価値」を意図的に狙っているため、
アナリストコンセンサス（市場心理側の代表値）との大幅な乖離は、
どちらの前提がズレているかを問い直す材料になる。

##### 実装方針
1. `common/screening/dcf_validity_checker.py`に、TANUKI IVとアナリスト
   目標株価中央値の乖離幅を計算するチェックを追加する（vs IVの値を
   そのまま利用可）
2. 乖離が大きい銘柄（例：50pt以上）を「TANUKIとアナリストの意見が
   大きく割れている銘柄」としてフラグし、どちらの前提に無理があるか
   個別確認を促す出力にする
3. 乖離の方向性（TANUKIが強気/弱気どちらに倒れているか）も記録する

#### 着手条件
なし（①〜③いずれも個別の着手条件なし。共通基盤である
`common/screening/dcf_validity_checker.py`への統合実装として
まとめて着手するか、個別に着手するかは着手時に判断する）

---

### [ANOMALY-PATTERN-CATALOG-1] 異常データパターンのカタログ化・新規登録時照合の仕組み
**優先度:** 中
**分類:** アーキテクチャ / データ品質ゲート / 銘柄登録フロー
**登録日:** 2026-07-19
**発見:** [[FY52WEEK-BS-STI-OVERRIDE-DESIGN-1]]（完了・BACKLOG_DONE.md参照。
KLAC/TER/V/SOFI）の対応中の議論

#### 背景
XBRLタグ選定等で銘柄固有の異常が見つかるたび、都度ゼロから個別調査
する非効率を解消するため、異常パターンを「型」として整理・蓄積し、
新規銘柄登録時（または既存銘柄で新たな異常が疑われた時）に既知の
型と照合してから対応判断する運用を導入する。

**2026-07-15の関連決定との関係（重要・要参照）**：
PREFLIGHT-CHECK-1で一度、ARCH-DATA-1と共有する汎用パターン判定
カタログ構想を検討したが、「登録前段階の統計的推測（SIC・社名等）
だけでは、実際にどのタグが正しいかの確定判定はできない」との理由で
見送られた経緯がある（本エントリ直前の「設計メモ追記（2026-07-15・
ARCH-DATA-1残課題③調査結果を反映）」参照）。本タスクはこれを覆すもの
ではなく、**照合のタイミングを「登録前」ではなく「登録後・実データ
取得後」に置く**点で異なる。2026-07-15の決定が「登録前の統計的推測は
時期尚早」としつつ、revenue系タグ競合を「Step1完了後の実データ検知」
（`revenue_tag_conflict_check.py`）へ統合する方針を既に採用していた
ことと同じ考え方を、short_term_investments等の他フィールドにも
一般化するのが本タスクの位置づけ。

#### 初期カタログ（今回確定した型）
**型A：候補集合＋freshness収束型**
- 症状: 正しいXBRLタグが他銘柄でも広く使われる汎用タグで、上限
  チェック等の値ベース検証だけでは誤タグの混入を検知できない
- 根本原因: 汎用タグ自体は銘柄非依存で存在するが、どのタグが
  「その銘柄にとって正しいか」は銘柄固有の申告慣行に依存する
- 対応方法: TICKER_RESTRICTIONSにticker別の候補タグ（単一または
  複数）を登録し、既存の`_extract_values_best_candidate()`の
  freshnessスコアで自動選定させる。グローバル候補リストへは
  追加しない
- 実例: KLAC/TER/V/SOFI（short_term_investments、2026-07-19実装）

**型B：非分類BS・近似値許容型（予約・実例なし）**
- 症状: BS構造自体が流動/非流動を区分しない等、単一タグでは
  真の値を表現できない
- 根本原因: 会計上の科目構造そのものの制約
- 対応方法: 近似値を採用しつつreport.txt/latest.json上で残差・
  不確実性を明示する
- 実例: なし（SOFIで型B該当を想定したが、調査の結果OtherInvestments
  タグで完全一致し型Aに収束したため、2026-07-19時点で実例なし。
  将来型B該当銘柄が見つかった場合の受け皿として型定義のみ残す）

**型C：資産クラス変化・当年度未タグ化型**
- 症状: BS計上額の構成が、単発の企業イベント（保有先の非上場
  投資先が新規上場する等）により当年度から質的に変化する。
  従来使用していた候補タグの申告自体も同時に停止する。新たに
  混入した資産クラスは、その変化が発生した当年度の10-K自体では
  対応するXBRL概念が明確にタグ付けされておらず、後続の四半期
  報告（10-Q）の比較年度開示で初めて該当タグが登場することがある
- 根本原因: (a) 一時的・単発的な企業イベント（投資先のIPO等に
  よる会計分類の切替）、(b) filer側のXBRLタグ付けが、その年の
  10-K提出時点では新資産クラスに対応する概念を採用しておらず、
  翌四半期以降に整備される、という2つの要因が重なったもの。
  型A（銘柄固有の恒常的な申告慣行の違い）・型B（BS構造自体の
  恒常的制約）と異なり、恒常的な性質ではなく一過性の移行期
  特有の欠損である可能性が高い
- 対応方法: 単一タグでの完全解消は不可能なことが多い。選択肢は
  以下3つ（優先順位はケースバイケースで判断）：
  ① 複数タグの合算による近似値を採用し、型Bと同様に残差を明示する
  ② 翌年度の10-K提出後、filer側のタグ付けが整備され単一/合算
     タグで正確に捕捉できるようになったか再確認する（型Aへ
     収束する可能性がある）
  ③ 当面はNoneのまま許容し、Net Debt計算等の下流への影響を
     個別確認する
- 実例: NVDA（short_term_investments、2026-07-19発見・2026-07-20
  対応方針①〈候補タグ合算の近似値〉採用・cross_filing_tags機構で
  実装完了。詳細はBACKLOG_DONE.md「NVDA-STI-TAG-UNIDENTIFIED-1」参照）

**型D：次元分解開示専用型（company_facts一括APIが構造的に返せない）
（2026-09-10追加）**
- 症状: 正しい値が10-K原文には確実に存在し、金額まで厳密に特定できて
  いるにもかかわらず、SECのcompanyfacts一括API（`company_facts.json`）
  を全namespace・全accn・全期間で横断検索しても**該当する値が一件も
  存在しない**
- 根本原因: XBRLの基底タグ（例:
  `TemporaryEquityCarryingAmountAttributableToParent`）が
  `StatementClassOfStockAxis`等のディメンションで次元分解された文脈
  でのみ開示され、非次元（デフォルトコンテキスト）版の事実が提出企業
  側でそもそも作成されていない。加えて発行体固有の名前空間
  （例: `cart:SeriesARedeemableConvertiblePreferredStockMember`）による
  開示や、初期XBRL移行期（2008〜2010年頃、義務化直後）の未タグ付けも
  同根の構造的欠落として現れる。SECのcompanyfacts一括APIは次元付き
  （dimensionally-qualified）事実を返さない設計上の制約を持つため、
  `parser.py`が参照する`us_gaap`辞書には対象タグの値自体が現れない
- 型A・型B・型Cとの違い: 型A（候補タグは存在するが優先順位の問題）・
  型C（一時的にタグが未整備なだけで翌四半期以降に登場しうる）とは
  異なり、型Dは**該当する非次元タグの事実自体が構造的に存在しない**
  （翌四半期を待っても解消しない）。型Bとも異なり、BS構造自体の科目
  制約ではなく、XBRL開示方式（次元分解 or 発行体固有名前空間）に
  起因する
- 対応方法（2026-09-10実装完了。BS恒等式チェック・PLフロー項目
  〈cost_of_revenue〉の両方に適用済み）:
  `common/sec_data/dimension_aggregate_fetcher.py`を新設した。
  当初検討していた(a) XBRLインスタンス文書を直接パースする経路を
  採用（(b)の「10-K原文の実測値を`fact_overrides.json`へハードコード」
  案は不採用のまま——(a)は個別filingの生XBRLタグから機械的に抽出・
  合算するため「タグ由来のない生の数値の直接注入」には当たらず、
  設計哲学と衝突しない）。当初instant（BS項目）のみ対応していたが、
  duration契約（PLフロー項目）にも対応するよう拡張し、`_bs_identity_
  extra_components()`（BS恒等式チェックのextra_components）・
  `_apply_dimension_aggregate_field_overrides()`（annual[field][year]
  の直接補完、cost_of_revenue向けに新設）の2経路から`dimension_
  aggregate_registry.json`（事前検証済みの結果をキャッシュ、
  パイプライン実行時のライブ取得はしない設計）を参照する形で統合済み。
  当初「cost_of_revenue等DCF計算に直接使われるフィールドへの適用は
  実害リスクが格段に大きい」として別エントリ（`[[LAYER3-COGS-
  DIMENSION-RECOVERY-CDNS-INTU-1]]`）に切り出していたが、実装時の
  消費経路の全数調査で**TANUKI VALUATIONのDCF計算は実際にはこの
  フィールドを一切消費していない**（Moat Score/TTMはLayer3という
  別パイプライン経由でありCDNS/INTUのLayer3側は元々空、`SECReader`に
  対応アクセサも存在しない）と判明し、想定していたリスクは実在しな
  かった。詳細はBACKLOG_DONE.md「2026-09-10（完了）」
  `[[LAYER3-COGS-DIMENSION-RECOVERY-CDNS-INTU-1]]`参照
- 実例（BS恒等式チェック用、5件）: PLTR(2019、$2,127,231,000)・
  CART(2023-2025、Series A優先株式、年度ごとに$177M〜$195M）・
  V(2008、$1,136,000,000)・CELH(2025、$1,759,975,000)・
  ASTS(2019、$218,519,748)。いずれも`[[CHECK29-UNRESOLVED-23-MIXED-
  CAUSES-1]]`の個別調査（2026-09-09）で10-K原文により金額まで厳密に
  特定済みだった値を、2026-09-10に`dimension_aggregate_fetcher.py`で
  機械的に再現・検証し**全件解消済み**（BKNG2011/2012は公正価値基準
  タグのみのgenuineな対応不能ケースのため型D対象外のまま。詳細は
  BACKLOG_DONE.md「2026-09-09⑬」追記・「2026-09-10（完了）」参照）
- 実例（PLフロー項目用、2件）: CDNS(FY2025、cost_of_revenue
  $722,249,000、`srt:ProductOrServiceAxis`でProduct/Service区分）・
  INTU(FY2025、$3,692,000,000、同軸）。10-K原文の「Total costs and
  expenses」から明示開示項目の合計を差し引いた残差と厳密一致する
  独立検算で裏付け済み。詳細はBACKLOG_DONE.md「2026-09-10（完了）」
  `[[LAYER3-COGS-DIMENSION-RECOVERY-CDNS-INTU-1]]`参照

#### 対応方針（設計・未着手）
- REGISTER-FLOW-REDESIGN-1・PREFLIGHT-CHECK-1（完了・BACKLOG_DONE.md
  参照。2026-09-05実装。ただし実装場所は`common/registration/
  preflight_check.py`として独立しており、本タスクとコードを共有する
  形にはなっていない）と統合的に設計する（別々に実装しない）
- 照合タイミングはStep1（SECデータ取得）完了後とし、
  `revenue_tag_conflict_check.py`（ARCH-DATA-1残課題③で実装済み）と
  同様の位置に配線することを想定
- REGISTER-FLOW-REDESIGN-1の対応方針2（status=provisioning導入、
  未着手）と組み合わせ、カタログ照合を通過するまでactiveへ
  昇格しない設計との統合要否を検討する
- 自動適用は既知パターンと完全一致した場合のみとし、非該当の
  場合は個別調査へ回す（自動停止はしない、判断材料の提示に留める
  というPREFLIGHT-CHECK-1の原則を踏襲）

#### 着手条件
なし（ただし新規銘柄登録はいつでも発生しうるため、着手を
先延ばしにする前提にはしないこと）

---

### [MACRODATA-SCHEDULED-SILENT-GAP-CSCICP-USALOL-1] CSCICP03USM665S・USALOLITONOSTSAMが現行INDICATOR_CONFIGから削除済みにも関わらず、05_indicator_schedule.csvの既存scheduled行がfred_id空文字列のまま処理され、actualが埋まらない静かなデータ欠落を起こす可能性
**優先度:** 低〜中（実害の有無・範囲が未確認）
**分類:** バグ疑い（サイレント欠落）
**登録日:** 2026-08-12
**発見:** `common/macro_data/`新設事前調査・FRED消費者洗い出し
（チャット記録、2026-08-12）

#### 内容
`CSCICP03USM665S`（CB Consumer Confidence）・`USALOLITONOSTSAM`
（Conference Board LEI）は`05_import_history.py`固有の旧
`FRED_INDICATORS`辞書にのみ存在し、現行`05_main.py`の
`INDICATOR_CONFIG`（12系列）には**含まれていない**（実コード確認済み）。
にもかかわらず、`docs/market-monitor/macro-pulse/data/
05_indicator_schedule.csv`には両指標の`scheduled`行が現存する
（実データ確認済み、例:
`Conference Board LEI,2026-06-08,USALOLITONOSTSAM,FRED,,,scheduled`）。

`fred_release_dates()`（458-498行）は`INDICATOR_CONFIG.items()`のみを
走査するため、この2系列の**新規**`scheduled`行が今後生成されることは
ない。しかし**既存の残存`scheduled`行**は`main()`の`for sched in
scheduled:`ループ（2196-2216行）で処理対象になり、`fetch_event_row()`
内で`cfg = INDICATOR_CONFIG.get(indicator, {})`が空dictを返すため
`fred_id`が空文字列となり、`if fred and fred_id and actual_val is
None:`（921行）の条件が成立せずFRED取得がスキップされる。**例外は
発生しないが、`actual`欄が空欄のまま行だけが`05_events.csv`に
生成される**静かな欠落が起こりうる。

#### 対応方針（未定・実データ確認が必要）
- `05_events.csv`・`05_indicator_schedule.csv`の実データを確認し、
  この2指標の`scheduled`行が既に処理済み（過去日、`actual`空欄のまま
  残存）か、まだ未来日で残存しているかを確認する
- 既に空欄行が生成されている場合は実害の範囲（何件か）を確認する
- 対応要否の判断: ①`05_indicator_schedule.csv`から該当2系列の
  残存`scheduled`行を削除する、②`INDICATOR_CONFIG`に復活させる
  （意図的に除外されたのか要確認）、のいずれかを実データ確認後に判断

#### 着手条件
実際に`05_events.csv`等でこの欠落が発生しているか（scheduled行の
残存有無）を実データで確認してから対応要否を判断する。

---

## 優先度：低（アイデア段階）

### [STOCKHTML-LAYER3-PUBLISH-PIPELINE-MISSING-1] stock.htmlのLayer3切替は新規公開パイプライン構築が前提だが、現時点で着手しない
**優先度:** 低（対応不要、記録のみ）
**分類:** アーキテクチャ上のブロッカー / 着手見送り
**登録日:** 2026-08-07
**発見:** フェーズD Step2-5事前調査・着手要否投資調査（チャット記録、2026-08-07）

#### 内容
stock.htmlは`normalized/`を直接fetchしており、Layer3切替には
Layer3ストアをJSON化して`docs/`配下に公開する新規パイプライン
（GitHub Actions拡張）が必要。技術コストは低い（既存`SEC_Data_
Update.yml`への追加ステップとして実装可能、ファイルサイズ2〜4MB
程度、計算コストも軽微）が、以下の理由で着手を見送る：
- 現状に緊急性のある実害はゼロ（is_ytdフィルタ漏れは実データで
  未発現、DAフィールド欠如29%も表示のみへの影響でフォールバックも
  堅牢）
- CF滝グラフはページ最下部の補助的機能、2.5ヶ月間安定稼働
- 着手すれば`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`と
  同じ「filed日最新優先 vs own-year優先」の検証課題が再発する
  可能性がある（未検証）

#### 着手条件
なし。DAフィールド欠如が実際にユーザー影響を持つと判明した場合、
またはstock.htmlの利用実態が変化した場合に再検討。

---

### [LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1] STONKS SILO fetcher.py・dcf_validity_checker.py（check_c_data_jump）の年次データがparser.py（own-year優先）を直接参照している一方、Layer3（filed日最新優先）とは選択思想が異なる
**優先度:** 低（対応不要、記録のみ。案2〈現状維持〉採用により
2026-08-07に高→低へ格下げ）
**分類:** 設計判断確定（恒久的な例外扱い）
**登録日:** 2026-08-07
**更新日:** 2026-08-07（対応方針確定。優先度を高→低に格下げ）
**発見:** フェーズD Step2-2事前調査（チャット記録、2026-08-07）

#### 内容
Layer3の年次エントリ選択（`_process_entries()`→`select_latest_filed()`）
は「同一end日付の全候補中、filed日最新」を機械的に採用する設計。
parser.py（`fetcher.py`・`dcf_validity_checker.py::check_c_data_
jump()`が直読みする`annual_*.json`の生成元）は`is_own_data`判定・
`_resolve_bs_entity_mixing()`等で「当年自身の10-K（own-year）」を
優先する設計。両者は元々別目的（Layer3のこの設計は四半期のYTD
チェーン解決・10-K/A訂正取り込みのため）で作られており、PL/CF系
フィールド（10-Kで2〜3年比較列を持つ）でほぼ全銘柄・全年度異なる
値を拾う。AVAV FY2022 revenueで実測検証済み（現状値445,732,000＝
own-year10-K、Layer3選択値＝2年後の10-Kの比較列が同一期間を
再採録したもの）。BS（残高、通常2年比較）で差分ゼロなのはこの説明と
整合。

フェーズD Step2-5事前調査（2026-08-07）で、`dcf_validity_checker.py::
check_c_data_jump()`（`report_consistency_check.py`のWARN-21として
本番稼働中）も同一のデータソース・同一の参照パターンで、この課題を
同様に引き継ぐことを確認した。

#### 対応方針（確定・2026-08-07）
**案2を採用**：Layer3切替を見送り、現状維持。`fetcher.py`・
`dcf_validity_checker.py`（`check_c_data_jump()`）は`data/
annual_*.json`（parser.py経由）の直読みを継続する。

**採用理由**：Layer3の「filed日最新優先」は、修正再表示（10-K/A等）を
正しく反映するケースと、タグ定義変更等で不正確な値を拾うケースを
区別できない不確実な方式である一方、parser.pyの「own-year優先」は
一貫した基準を持つ実績のある方式。正確性の確実性を犠牲にしてまで
統一する理由がない。

3スキーマ並存のうち、この2ファイル分（`fetcher.py`・
`dcf_validity_checker.py`の該当関数）は恒久的な例外として残る。

#### 着手条件
なし。将来、修正再表示の理由自動判定の仕組み（フェーズF/G
「filing_text吸収」関連）が実現した場合に再検討する。

---

### [TAIL-SHARESDILUTED-Q4-TIMING-RISK-1] TANUKI TAILのeps_diluted計算が、レビュー生成タイミングによってはCommonStockSharesOutstanding（期末発行済株式数）由来のSharesDilutedを拾う構造的リスクを持つ
**優先度:** 低（現時点で10銘柄全数、最新四半期はWeightedAverage側が
採用されており実害なし）
**分類:** 潜在リスク
**登録日:** 2026-08-07
**発見:** フェーズD Step2-3事前調査（チャット記録、2026-08-07）

#### 内容
`[[LAYER3-SHARESDILUTED-TAG-GAP-1]]`の対応（source_tagフィルタで
CommonStockSharesOutstanding由来を除外）はpipeline.pyの希薄化率
計算箇所に限定実装されており、共通アクセサ（reader.py/
layer3_builder.py）自体には手を入れていない。TANUKI TAILの
`quarterly_review_generator.py`・`tail_dcf_bridge.py`は
`get_latest_quarterly()`を直接呼ぶため、この既存フィルタの恩恵を
受けない。直近四半期がQ4に当たるタイミング（WeightedAverage系タグが
四半期報告されない期）でレビューが生成された場合、eps_diluted計算が
期末発行済株式数ベースの値を使ってしまう可能性がある。

#### 着手条件
なし。実際にQ4タイミングでの計算誤りが発生した時点、または
`[[LAYER3-SHARESDILUTED-TAG-GAP-1]]`の対応をpipeline.py外にも展開する
判断がされた時点で再検討。

---

### [FETCHER-PY-BS-FIELDS-DEAD-KEYS-1] fetcher.pyの_BS_FIELDSでtotal_debt・shares_outstanding・shares_dilutedがannual_*.jsonに実在しないキーを参照しており常にNone
**優先度:** 低（analyzer.pyがこの4項目を参照しないため現状無害）
**分類:** バグ（死んだコード）
**登録日:** 2026-08-07
**発見:** フェーズD Step2-2事前調査（チャット記録、2026-08-07）

#### 内容
実際は`long_term_debt`/`short_term_debt`が別名、shares系は`shares`
セクション別置き。Layer3移行とは無関係の独立した既存バグ。

#### 着手条件
なし。実害が発生した時点で対応。

---

### [PARSER-MERGED-TAG-MIXING-RISK-1] parser.py::_extract_values_merged()が、Layer3が[[LAYER3-FALLBACK-STALE-TAG-PRIORITY-1]]で廃棄した危険パターン（複数タグの生エントリを先に混ぜてからYTD変換）と同型の構造を持つ疑い
**優先度:** 低（Layer3統一方針確定により、data/系統の重要度自体が
低下したため、中→低に格下げ）
**分類:** バグ疑い / 構造的リスク
**登録日:** 2026-08-06
**発見:** `SEC_EDGAR_LAYER_DESIGN.md`との整合性確認調査（チャット記録、
2026-08-06）

#### 内容
`layer3_builder.py::_merge_candidate_entries()`は、候補タグごとに
独立して`_process_entries()`→`_normalize_field_entries()`（YTD→単四半期
変換を含む）を完了させてから、正規化済み系列同士をend_date単位で
マージする設計になっている。これは当初の実装（生エントリを先に
end_date単位でマージしてからYTD→単四半期変換する順序）が、異なる
タグ由来のエントリが同一end_dateで競合した際にFYチェーン判定を
破壊し、YTD差分計算が中間四半期を1つ読み飛ばして2四半期分を1四半期
として誤算出するバグを引き起こした（CPRT・PEP等6銘柄・20エントリで
実データ確認、[[LAYER3-FALLBACK-STALE-TAG-PRIORITY-1]]）ことを踏まえた
意図的な設計変更。

一方、`common/sec_data/parser.py::_extract_values_merged()`
（merge_all_tags対象フィールド向け、`SECDATA-STORAGE-FRAGMENTATION-1`
2026-08-05実装のSA/YTD統一アルゴリズム）は、全キー（＝複数タグ）を
早期終了せずループし、四半期の生候補`(fy, fp, start, end, val)`を
タグ区別のないまま単一の`quarterly_candidates`リストへ蓄積してから、
`_resolve_quarterly_values()`でまとめて解決する構造になっている。これは
Layer3が明示的に廃棄した「生エントリを先に混ぜてから変換」という
旧パターンと同型であり、複数タグが競合する銘柄・フィールドで同種の
誤算出が発生する構造的リスクを持つ疑いがある。

なお、単一タグのみを扱う`_extract_values_best_candidate()`経路は
タグ混入の余地がないため対象外。939b8f57fコミット時の検証（全105銘柄
再パース結果が独自シミュレーションと完全一致）は旧parser.py実装との
内部整合性確認であり、Layer3側の値との突合ではないため、本リスクを
検出できるものではない。実データでの影響有無は未検証。

#### 着手条件
merge_all_tags対象フィールド一覧の洗い出し・実データでの影響有無検証
から。ただしdata/系統の位置づけがLayer3統一に伴い補助的になったため、
緊急性は低い。

### [LAYER3-SM-SGA-SEPARATION-NONE-FALLOUT-1] Layer3のSM/SGA概念分離に伴うNone化2件の統合（元LAYER3-ROIC-WACC-NONE-4TICKERS-1/FINTREND-SM-JOBY-NONE-1）
**優先度:** 低（意図的な仕様、既知の`[[SCHEMA-NORMALIZED-ISSUES-1]]`②
SM/SGA概念混同問題の帰結）
**分類:** 仕様変更（改善）/ ユーザー影響あり
**登録日:** 各サブ項目とも2026-08-06・2026-08-07。統合日: 2026-09-05
**発見:** 2026-09-05のBACKLOG横断整理

#### 統合の経緯
LAYER3-ROIC-WACC-NONE-4TICKERS-1・FINTREND-SM-JOBY-NONE-1は、いずれも
`[[SCHEMA-NORMALIZED-ISSUES-1]]`②のSM/SGA概念分離の帰結として、
Layer3切替後は正しい挙動としてNoneを返すようになったという同一の
根本原因を持つため、2026-09-05に1エントリへ統合した。元の2件は
BACKLOG.mdから削除し、内容は要約せず全文そのまま以下の①②に保持する。

#### ① 元[LAYER3-ROIC-WACC-NONE-4TICKERS-1] COHR/LLY/JNJ/KLACのROIC-WACC比率・Moat ROICが、Layer3切替に伴いNone表示になった
**優先度:** 低（意図的な仕様、既知のSM/SGA概念混同問題の帰結）
**分類:** 仕様変更（改善）/ ユーザー影響あり
**登録日:** 2026-08-06
**発見:** フェーズD Step2-1実装時（チャット記録、2026-08-06）

##### 内容
normalized/時代は間違った値（SGA総額を誤混入）でROIC-WACC比率を
計算していたが、Layer3切替後はselling_and_marketingが正しく分離
されたため、3フィールド共通end日のintersectionが0件となりNoneを
返すようになった。`[[SCHEMA-NORMALIZED-ISSUES-1]]`②SM/SGA概念混同
問題の根本解消（別タスク）まで、この4銘柄はROIC-WACC比率非表示の
まま。

##### 着手条件
SM/SGA概念混同問題の解消時に再検討。

#### ② 元[FINTREND-SM-JOBY-NONE-1] financial_trend_calculator.pyのSMフィールドがJOBYでNone化する（Layer3切替時）
**優先度:** 低（既知の`[[SCHEMA-NORMALIZED-ISSUES-1]]`②の帰結、
`[[LAYER3-ROIC-WACC-NONE-4TICKERS-1]]`と同型・同じ判断基準を適用）
**分類:** 仕様変更（改善）
**登録日:** 2026-08-07
**発見:** フェーズD Step2-2事前調査（チャット記録、2026-08-07）

##### 内容
normalized側はSGA総額へのフォールバック値を保持していたがLayer3側は
`selling_and_marketing`のみを候補としNoneを返す。正しい方の挙動として
受け入れる。

**補足（2026-08-07、実装時に判明）**: `financial_trend_calculator.py`の
`compute_vectors()`は`VECTOR_FIELDS`（Revenue/GrossProfit/OperatingIncome/
RD/NetIncome/OCF/CapExの7項目）のみを処理しており、`SUB_FIELDS`
（SM・SBC）は定義されているだけで`compute_vectors()`から一切呼び出され
ていない未使用の定数と判明した。そのため本項目のJOBY None化は
`_get_quarterly_entries()`単体の挙動としては真だが、**現状の
`results.json`出力（`financial_vectors`）には実影響がゼロ**である。
将来`SUB_FIELDS`が実際に配線された場合に初めて表面化する。

##### 着手条件
SM/SGA概念混同問題（`[[SCHEMA-NORMALIZED-ISSUES-1]]`②）の根本解消時に
再検討。

#### 着手条件（統合後、両サブ項目共通）
`[[SCHEMA-NORMALIZED-ISSUES-1]]`②のSM/SGA概念混同問題の根本解消時に
再検討。①②とも個別の着手条件は上記のとおり同一のため、統合先の
本条件に一本化する。

### [DEFICIT-SCORE-CEILING-95-1] STONKS SILO DEFICIT分類、赤字企業の実質上限95点
**優先度:** 低
**分類:** 設計上の制約 / STONKS SILO
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ8（セッション終了時ブラッシュアップで39件起票から漏れていたものを追加起票）

#### 内容
`analyzer.py::_deficit_verdict()`の黒字状況(10pt)項目は、`is_profitable`が
真の場合のみ10ptを付与し、赤字企業（`net_income is not None`のみが条件の
`elif`分岐）は最大5ptしか取れない（`analyzer.py:453-457`）。売上成長40pt＋
投資姿勢30pt＋粗利率20pt＋黒字状況10pt＝100点満点の設計だが、STONKS SILOの
対象銘柄は原則として赤字企業（プレレベニュー/赤字拡大企業の投資適合性
評価が目的）であるため、実質的な達成可能上限は95点になる。

#### 実データでの影響確認（本追加起票時に実施）
`docs/value-monitor/stonks-silo/data/results.json`（現行25銘柄）を確認した
ところ、**QBTS が実際に95.0点（現行データの最高スコア）に到達しており、
本問題が理論上の懸念ではなく現行データで実際に発生していることを確認**
した。ただし`verdict`判定の閾値（`GOOD_DEFICIT>=65`／`WATCH>=35`／
`BAD_DEFICIT`未満）はいずれも95点を大きく下回るため、**この5pt差が
verdict分類（GOOD_DEFICIT/WATCH/BAD_DEFICIT）を変えることはない**。
実害は「なぜ赤字企業は100点に到達できないのか」という数値スケールの
解釈上の疑問に留まり、投資判断そのものへの影響は現行データでは
確認されなかった。以上を踏まえ優先度は低のまま登録する。

#### 対応方針
黒字企業（`is_profitable=True`）は既に`verdict="PROFITABLE"`として
score非依存の別カテゴリに分岐するため、スコア自体を100点満点で比較する
必要があるのは実質的に赤字企業同士のみである。「赤字企業内での相対
比較」であることを踏まえ、100点満点表記を95点満点表記に変更するか、
現状維持のまま「黒字状況」項目の配点自体を見直すかを判断する。

#### 着手条件
なし

---

### [TAILKPI-FIELD-VALIDATION-GAP-1] TANUKI TAIL KPI提案確定時の個別フィールド妥当性検証未実装
**優先度:** 低〜未定
**分類:** データ品質 / TANUKI TAIL
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ2（セッション終了時ブラッシュアップで39件起票から漏れていたものを追加起票）

#### 内容
TANUKI TAILのKPI提案確定フロー（AS-IS-425〜436、`kpi_proposer.py`のGrok
提案を人間が画面で確認・編集して確定する）において、`workflow_write.py:
149-152`は`kpis`が空リストでないことのみをチェックしており、個別
フィールド（`warning_threshold`の数値妥当性、`xbrl_tag`の形式等）の
検証ロジックは確認できなかった。Discoverのconfig系（[[DISCOVER-CONFIG-
DUAL-MGMT-1]]、バリデーション0件）ほど深刻ではない（コンテナレベルの
非空チェックは存在する）が、個別フィールドの誤入力を防ぐ仕組みがない
点は同種のリスクである。

#### 対応方針
`warning_threshold`が数値であること・`xbrl_tag`が既知のタグ命名規則に
従っていること等、個別フィールドレベルのバリデーションを
`workflow_write.py`に追加することを検討する。

#### 着手条件
なし

---

### [TANUKI-VALUATION-MISC-GAPS-1] TANUKI VALUATIONの軽微な構造的ギャップまとめ（PERフォールバック欠如・EV/EBITDA負値格納・net_debt符号エイリアス・v0_adjusted死フィールド・Runway cash算出相違・mature_profit S&M欠落・根拠不明な定数・セグメントKPIテーブル機能撤去済み）
**優先度:** 低
**分類:** データ品質 / TANUKI VALUATION
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ3・4・6

#### 内容
①PS/PEG/EV-EBITDAはTANUKIデータ欠落時にHypeCore自身の`poc.json`へ
フォールバックするが、PERだけはTANUKIの`comps.per`のみを参照しフォール
バックしない（`detail.html:550`）。②`hypecore.py:130`は負のEV/EBITDAを
そのまま格納する設計（現状UIガードで実害なしだが将来別画面追加時に
誤表示リスク）。③`net_debt = -net_cash`という単純な符号反転が「正=
ネットキャッシュ」「正=純負債」という2つの概念を並存させており、
横断参照コードで符号取り違えリスクがある。④`v0_adjusted`（AS-IS-008）は
`v0_adjusted = v0`という代入のみで実質的な死フィールド（コメント自体が
後方互換用と明記）。⑤Runway概念がTANUKI（`SECReader.get_net_cash()`
経由、セクターガードあり）とSTONKS SILO（`annual_{yr}.json`の`bs`単純
合算のみ）でcash算出経路が異なる。⑥`research_and_development`・
`selling_and_marketing`がSEC非開示の場合`or 0`で「支出ゼロ」として
足し戻され、`mature_profit`が実態より低く算出される。⑦`growth_floor
(15%)`・`growth_cap(50%)`・`market_return(10%)`の根拠がコード内に一切
記載されていない。⑧（2026-08-26発見、2026-08-27機能撤去済み）
`kpi_fetcher.py::build_kpi_data()`（セグメント別KPIデータを構築、
stock.htmlの`renderSegmentKpiTable()`が`d.kpi_data`として参照する
想定）が本番パイプラインから呼び出されておらず「セグメントKPI
テーブル」が恒久的に非表示だった問題。2026-08-27に配線を追加した
ところ（コミット`0450abe77`）、配線後も全銘柄で`kpi_data=None`のまま
であることが判明し（`[[KPI-FETCHER-SEGMENT-SOURCE-ORPHANED-1]]`調査）、
さらにKoichiさんとの対話で**機能の前提自体が誤りだった**ことが判明した
——当初「KPI＝XBRLの正式な会計セグメントデータ」を前提に設計されて
いたが、本来のKPIイメージ（例: SOFIの総会員数・クロスバイ率・NIM等）は
決算資料の文章に企業ごと個別の形式で開示される経営指標であり、XBRLの
会計セグメントとは全く別物だった。Koichiさんの判断により、この機能は
一から作り直す前提で誤った実装（残骸）を撤去した（`kpi_fetcher.py`・
`kpi_config.py`・`common/sec_data/segment_fetcher.py`削除、
`pipeline.py`の配線削除、`stock.html`の表示コード削除。新機能の着手は
見送り中、詳細は`[[KPI-FETCHER-SEGMENT-SOURCE-ORPHANED-1]]`
〈BACKLOG_DONE.md〉・`[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-
IDEA-1]]`参照）。

#### 対応方針
①〜⑦は影響が限定的なため、他の関連タスク（[[RISK-FREE-RATE-
HARDCODE-1]]等）着手時に合わせて解消することを推奨する。⑧は誤った
前提の実装を撤去済み。再設計する場合は`[[SEGMENT-KPI-NARRATIVE-
EXTRACTION-FUTURE-IDEA-1]]`を起点に、着手タイミングをKoichiさんが
判断する。

#### 着手条件
なし

---

### [SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1] 決算資料の文章から企業固有の経営指標をAIが抽出する新機能（構想メモ、着手見送り中）
**優先度:** 保留 / 将来検討（今すぐ着手する項目ではない）
**分類:** 新機能構想 / TANUKI VALUATION
**登録日:** 2026-08-27
**発見:** `[[TANUKI-VALUATION-MISC-GAPS-1]]`⑧・`[[KPI-FETCHER-SEGMENT-
SOURCE-ORPHANED-1]]`対応中、Koichiさんとの対話で「セグメントKPI
テーブル」機能が誤った前提で設計されていたと判明した際の議論から

#### 内容（再定義後の構想）
旧実装（`kpi_fetcher.py`・`kpi_config.py`・`common/sec_data/
segment_fetcher.py`、2026-08-27に撤去済み）は「KPI＝XBRLの正式な
会計セグメントデータ（構造化された財務諸表タクソノミ上のセグメント別
売上・利益）」を前提に設計されていた。しかしKoichiさんが提示した
実例（SOFIの「総会員数」「クロスバイ率」「ローン実行額」「純金利
マージン」「調整後EBITDAマージン」等）により、本来求められるKPIは
**決算プレスリリース・株主レター・MD&A等の文章の中に企業ごとに
個別の形式で開示される経営指標**であり、XBRLの正式な会計セグメント
データとは全く別物であることが分かった。

再定義後の構想: 決算資料の文章をAIが読み解き、企業固有の経営指標
（例のような数値）を抽出・時系列で記録する機能。旧実装（XBRL会計
セグメントデータ前提）とは前提・実装方式とも別物であり、ゼロから
設計し直す必要がある。

#### 対応方針（未定・構想メモのみ）
- どの銘柄から対象にするか（SOFI等、既にKPI開示が定型化している企業
  から着手する等）
- 決算資料の取得元（プレスリリースPDF・株主レター・10-K MD&A等の
  どれを一次ソースとするか）
- AI抽出の方式（毎四半期の決算発表後にGrok/Claude等へ抽出依頼する
  運用フローの設計）
- 抽出結果の検証方法（誤抽出・単位間違い等をどう検知するか）

いずれも設計判断・投資対効果の検討が必要であり、今回は構想の記録
のみに留める。

#### 着手条件
なし（Koichiさんが着手タイミングを判断する。現時点では見送り中）

---

（[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]は2026-08-27実装完了(PascalCase→
snake_caseキー修正、全3実消費箇所で正常化を確認)、BACKLOG_DONE.md
「2026-08-27（完了）」参照）

---

（[[KPI-FETCHER-SEGMENT-SOURCE-ORPHANED-1]]は2026-08-27、誤った前提
〈XBRL会計セグメントデータ〉で設計された機能と判明したため残骸を撤去、
BACKLOG_DONE.md「2026-08-27（完了）」参照）

---

### [MACRO-THRESHOLD-INCONSISTENCY-1] MACRO PULSEの閾値不一致・重複判定の軽微な構造的リスク（YC閾値3セット・dedupe_new_rows()のCFNAI/Sahm無条件適用）
**優先度:** 低
**分類:** データ品質 / MACRO PULSE
**登録日:** 2026-07-23
**発見:** `FIELD_DEFINITIONS.md`フェーズ10

#### 内容
①10Y-2Yスプレッドの判定閾値が用途によって3セット存在する（ティッカー
表示のINVERTED/FLAT/NORMAL: -0.2/0.5、RECESSION RISK SCOREのステップ
関数: -0.5/0/0.5の4段階、LAYER2健全性バーのbull/bear: 0.5/-0.2）。②
`dedupe_new_rows()`の重複判定は`obs_to_release_lag`に基づく日数窓＋
完全一致する`actual`値のみで行われ、指標ごとの「値の反復が正常で
ありうるか」を区別する例外リストがない。Sahm Ruleが複数月連続で0.00に
近い値を取る、CFNAI MA3が安定期に近似値を繰り返す、といった正当な
ケースを重複と誤判定して新規データ行を捨てるリスクが構造的に残る。

#### 対応方針
①閾値セットを統一するか、用途別に異なる理由を明示する②指標別の
「反復許容」例外リストを`dedupe_new_rows()`に追加する。

#### 着手条件
なし

---

### [MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1] 景気サイクルフェーズ複合スコアで、CFNAI・Building Permitsに鮮度注記が欠けている
**優先度:** 中（複合スコアの22%〈CFNAI 12%＋Building Permits 10%〉が
実測約7週間遅れのデータに基づくが、閲覧者がこれに気づく手段がない）
**分類:** UI/UX / データ鮮度の開示不足
**登録日:** 2026-08-15
**発見:** MACRO PULSEにおける遅延系列の扱い確認調査（チャット記録、
2026-08-15）

#### 内容
`docs/market-monitor/macro-pulse/index.html`の「景気サイクルフェーズ」
複合スコア（`computeCurrentScore()`、合計100%）で、Michigan Sentiment
（8%）のみ「※FREDは1ヶ月遅延公開のため、最新発表値と異なる場合が
あります」という鮮度注記があるが、より大きなウェイトを持つCFNAI
（12%）・Building Permits（10%）には注記がない。

FRED自身の再公開遅延（大学等の発表からFRED反映まで）が実測で約
7週間（49日）規模であることを、Michigan Consumer Sentiment
2026年6月分の実データ（大学発表2026-06-12、FRED反映検知
2026-07-31）で具体的に確認済み。`obs_to_release_lag`設定
（大学の速報発表タイミングのみを表す、10日）とは別に、この
FRED再公開遅延が上乗せされる構造。

技術的制約: `idxLatestAsOf()`（901-912行目）が`.actual`（値）のみを
返し観測日情報を構造的に破棄する実装のため、現状は各指標の
ツールチップに観測日自体を表示する手段がない。

#### 対応方針の選択肢（未実装）
1. CFNAI・Building Permitsへの鮮度注記追加（低コスト、Michigan
   Sentimentと同様のdesc文言追加のみ）
2. `idxLatestAsOf()`が観測日も返すよう拡張し、各指標のツールチップに
   「観測日: YYYY-MM-DD」を表示できるようにする（中コスト、構造変更）
3. AI週次レポートのプロンプト（`05_main.py` 1626-1631行目）へ各指標の
   観測月を付記し、AI生成コメントが遅延データを「直近」と誤って
   記述しないようにする（低〜中コスト）

#### 着手条件
なし。優先度に応じて対応方針を選択の上、実装する。

---


### [TTM-SBC-QUARTERS-GAP-1] build_rice_annual_shape()のSBCがquarters完全性チェック対象外
**優先度:** 低〜未定
**分類:** データ品質 / SECデータ取得層
**登録日:** 2026-07-18
**発見:** TRUST-SUMMARY-EPIC-1ステップ1棚卸し調査時の副次発見

#### 内容
`data_fetcher.py::build_rice_annual_shape()`は、OCF/CapEx/Revenue/
NetIncomeの4フィールドについて`_quarters_complete()`（quarters_used≥4）で
完全性を判定してから出力対象に含めるが、同じ辞書内に含まれる`SBC`
（stock_based_compensation）はこの完全性チェックの対象外のまま無条件で
出力される（298行目付近、`_quarters_complete()`呼び出し引数にSBCが
含まれていない）。RD/SMがrice.py側で意図的にNone許容（0扱い・警告ログ
のみ）とされているのとは異なり、SBCについては「意図的な許容」なのか
「チェック漏れ」なのか、現時点では未確認。

#### 対応方針（未定）
`build_rice_annual_shape()`のSBC出力が実際にquarters_used<4の不完全な
値を含むケースがあるか実データで確認し、意図的な設計か単純な漏れかを
切り分けてから対応要否を判断する。

#### 着手条件
なし

---

### [FUTURE-FEATURE-IDEAS-CATALOG-1] 将来構想6件の統合カタログ（元UX-FLOW-1/MULTI-1/ARCH-1/EVAL-2/DESIGN-8-3/DESIGN-8-4）
**優先度:** 低（いずれも構想段階・実装未着手のアイデアメモ）
**分類:** 将来構想 / 複数画面・複数サブシステム横断
**登録日:** 各サブ項目の元登録日は各①〜⑥の記載を参照。統合日: 2026-09-05
**発見:** 2026-09-05のBACKLOG横断整理

#### 統合の経緯
UX-FLOW-1・MULTI-1・ARCH-1・EVAL-2・DESIGN-8-3・DESIGN-8-4の6件は、いずれも
将来的な機能拡張・設計改善のアイデアメモであり、現時点で個別に着手予定は
ないため、2026-09-05に1つのカタログエントリへ統合した。元の6件は
BACKLOG.mdから削除し、内容は要約せず全文そのまま以下の①〜⑥に保持する。
DESIGN-8-3・DESIGN-8-4については、統合時点で判明している注意点を各項目
末尾に「注記（2026-09-05追記）」として追記した（元の構想自体は変更・
削除していない）。

#### ① 元[UX-FLOW-1] On a Journey標準利用フローの設計
**優先度:** 低（思想設計タスク、実装ではなく方針検討から開始）
**分類:** 設計課題 / 全画面横断

##### 内容
画面間を行き来する非線形な利用が前提だが、緩やかな標準利用フロー
（例: stock.htmlで個別検証→TANUKI SCOREで横断相対判断、等）を
今後設計したい。

##### 格上げ検討理由（2026-07-01）
EXTREME-FEAR-1対応時、買い候補TOP10機能（TANUKI score×乖離率×funda×phaseベースの
銘柄選定）のナビ登録先を検討した際、本来TANUKI SCOREの役割に近い機能をMarket Pulse
配下に置く形で暫定決着した。これは各画面の役割定義はあるものの、画面間の回遊動線・
機能配置の指針が未設計であることに起因する。今後複数システムの性質を跨ぐ機能が増える
たびに同種の判断コストが発生するため、次セッション以降の設計着手候補として優先的に
検討する。

#### ② 元[MULTI-1] マルチバリュエーション表示
- 現状: DCF一本槍
- 改善: DCF / PEG / EV/Sales / RICE / HypeCoreを並列スコアカード表示
- GPT提案: 2026-05-30
- 関連（2026-07-10追記、2026-09-05更新）: [[SCREENING-SIGNAL-
  INTEGRATION-EPIC-1]]（旧RICE-INTEGRATE-1、2026-09-05に同エピックへ
  統合済み）とRICE指標の活用目的が部分重複。本タスク（MULTI-1）は
  画面表示（並列スコアカード）が主眼、旧RICE-INTEGRATE-1はスクリーニング
  判定への組み込みが主眼という役割分担。どちらかの着手時に統合要否を
  判断する。

#### ③ 元[ARCH-1] ボトルネック企業プレミアム
- 現状: 未実装
- 内容: NVDA・ASML等の独占的ポジションを持つ企業への追加プレミアム
- 設計: 手動フラグ（bottleneck: true）+ Moat Scoreへの上乗せ or Phase1延長の形
- 注記: ALPHA-REDESIGN-1（2026-06-25）でalphaが廃止されたため、
  α加算方式は使用不可。設計を再検討する必要あり。
- 記録日: 2026-04-12

#### ④ 元[EVAL-2] 期待値エンジン（仮称）
- 現状: 構想中
- 内容: 各サブポート戦略の期待値を統合管理するエンジン

#### ⑤ 元[DESIGN-8-3] 8-3 ワンクリック銘柄登録〜更新
- 概要: Discover画面から「➕ 登録」ボタンで
  CIK取得→β/セグメント/Damodaran業種AI提案→承認→一括更新
  を一気通貫で実行
- 実装難易度: 高
- **注記（2026-09-05追記）**: 「Discover画面」は2026-09-01に
  [[DISCOVER-SUBSYSTEM-REMOVAL-1]]で削除済みのため、実装時はUI設置
  場所の再設計が必要。

#### ⑥ 元[DESIGN-8-4] 8-4 指数採用候補銘柄の発掘（設計見直し済み・実装保留）
- 概要: S&P MidCap 400 → S&P 500 昇格候補を定期サーチ
  GS・バンカメ等が発表する昇格候補レポートをGrok Web検索で収集
  機械的条件判定（yfinance）ではなくアナリストレポートベースの設計
- 実装方針: Grokのweb検索で「S&P 500 addition candidates」を定期検索
  週次でDiscover候補セクションに表示
- 実装難易度: 中
- 状態: 実装保留（着手時期未定）
- **注記（2026-09-05追記）**: 実装方針の「Grok Web検索」は、
  risk_fetcher/Discover撤去（2026-09-01〜02）で確立した「根拠不明の
  生成をそのまま採用しない」方針と抵触するため、実装時は代替手法
  （yfinance機械的判定等）を検討すること。

#### 着手条件
なし（いずれも構想段階、個別項目ごとに着手可否を判断する）

---

### [RPO-ADMIN-1] rpo_config.jsonがadmin.htmlで編集できない
**優先度:** 低
**分類:** 管理UI漏れ / admin.html
**発見:** 2026-06-26横断調査

#### 問題
rpo_config.json（RPOプレミアムのホワイトリスト管理）は
report_consistency_check.py L42で参照されているが、
admin.htmlにUI編集機能が存在しない。
RPOプレミアムを付与・変更する際は手動JSONファイル編集が必要。

**再確認（追記、2026-08-15、フェーズ3未登録11件調査）**: 編集UI欠如を
再確認済み（`INPUT-C-004`）。加えて`rpo_config.json`は`_meta`相当の
メタ情報（更新者・更新日時）も持たないことが判明した。管理UI追加時は
`_meta`付与（`NAMING_CONVENTIONS.md`規則8参照）も併せて検討対象とする。

#### 対応方針
admin.htmlにrpo_config.jsonの編集UIセクションを追加する。追加時は
`_meta`フィールドの付与も併せて検討する（2026-08-15追記）。

---


### [SEC-XBRL-MISSING-START-ENTRY-1] raw XBRLにstart日付が欠落した変則的なエントリが含まれる
**優先度:** 低
**分類:** データ品質 / SEC提出データ異常
**登録日:** 2026-07-24
**発見:** LAYER3-ANNUAL-QUARTERLY-COLLISION-1根本修正後の
105銘柄回帰レポート（4回目）

#### 内容
AVAV（accession 0001104659-26-078906、2026-06-29提出の新しい
10-K）等で、raw XBRLに`start`日付が欠落した変則的なエントリが
含まれており、shares_dilutedで比較対象normalized/生成後に到着した
新規提出によるデータドリフトとして3件（AVAV/ELF/ESTC）の差異と
して検出された。

#### 影響
shares_dilutedはNO_CANDIDATE_MERGE_FIELDS（今回の変更対象外パス）
を通るため、今回の修正とは無関係。影響範囲・実害は未調査。

#### 対応方針
未定。start欠落エントリの扱い（除外するか、end日付のみで妥当性
判定するか）の検討が必要。

#### 着手条件
なし

---

---

---

---

### [LAYER3-GA-STANDALONE-TAG-UNMAPPED-1] GeneralAndAdministrativeExpense（Selling抜きG&A単体タグ）がLayer2のどのフィールドにもマッピングされていない
**優先度:** 低〜中（2026-07-30投資調査により中→低〜中に修正。理由は下記対応方針参照）
**分類:** データ品質 / タグ網羅性
**登録日:** 2026-07-24
**発見:** SM/SGA分離258件全数検証

#### 内容
GeneralAndAdministrativeExpense（Selling抜きのG&A単体タグ）が、
Layer2の32フィールドのいずれにもマッピングされていない。

【2026-07-30投資調査で規模を再確認】当初「少なくとも6銘柄」としていた
規模認識は過小評価だった。全105銘柄スキャンの結果、
GeneralAndAdministrativeExpenseタグを報告している銘柄は56銘柄に及ぶ。
ただし影響度で3分類できる:
- **実害なし（4銘柄: AAPL/AMAT/CELH/TER）**: SGA総額
  （selling_general_and_administrative）が別途取得済みのため対応不要
- **部分的ギャップ（47銘柄）**: selling_and_marketingは機能するが、
  selling_general_and_administrativeのみ空
- **完全なギャップ（5〜6銘柄: APGE/ASTS/CON/ENB/RXRX）**: SM・SGA両方が
  完全に空。当初報告の6銘柄のうちCAKE/CPRTは、実際にはAdvertisingExpense
  （SM候補タグ）を報告しており部分的ギャップ側に該当することが判明
  （ただし四半期粒度では取り込まれないため実質SM空という当初の観測自体は
  誤りではない）。ENBは今回の調査で新たに完全なギャップ銘柄として発見

#### 影響
5〜6銘柄（APGE/ASTS/CON/ENB/RXRX、CAKE/CPRTは部分的ギャップ）でSM・SGA
両フィールドが完全に空になる。GeneralAndAdministrativeExpenseタグ自体の
存在という意味では56銘柄規模。

#### 対応方針
- **選択肢B（既存selling_general_and_administrativeへのフォールバック
  候補化）は非推奨**: [[SCHEMA-NORMALIZED-ISSUES-1]]（旧SCHEMA-SM-SGA-
  CONFLATION-1）と同型の概念混在リスクを再導入する。G&A単体とSGA総額は金額の性質が異なり（SGA総額は
  Selling費用を含むため同規模の企業でもG&A単体より必然的に大きい）、
  同一フィールドに混在させると時系列比較・銘柄間比較の両方で不整合が
  生じる
- **選択肢A（新規フィールド化、例: general_and_administrative_expense）
  を推奨するが、優先度は低〜中に留める**: selling_general_and_
  administrative自体が現状TANUKI VALUATION計算に一切消費されておらず
  （report_consistency_check.pyのSGA整合性チェック用途のみ）、新規
  フィールド追加の実利は当面限定的なため
- 技術的には、GeneralAndAdministrativeExpenseはFLOW系（duration型）
  のため、Q4_IMPLIED_FIELDS・MISSING_QUARTER_IMPLIED_FIELDS・
  newfield_q4_cutoff_check.pyの対象に加えることは可能
  （selling_general_and_administrativeと同型の扱いができる）

#### 着手条件
SGA（selling_general_and_administrative）・SM（selling_and_marketing）
のいずれかが新機能（投資強度分析の精緻化等）で実消費される計画が
立った時点、またはreport_consistency_check.pyのSGA整合性チェックを
強化するタイミングで、選択肢A（新規フィールド化）を再検討する。

---

## システム全体バックログ（TANUKI VALUATION以外）

### 【Stonks Silo】
- 現状: 26銘柄・results.json更新済み

### 【Moomoo API】
- [ ] β自動計算（SPY日次リターンからbeta_config.jsonを自動更新）
- [ ] advance/decline比率収集（MACRO PULSE向け）
      ※ Market Pulse向けの二極化検知はRSP/SPY乖離・A-Dライン・マクラレンオシレーターで
        2026-06-22に実装済み（[[MP-BREADTH-2]]、BACKLOG_DONE.md参照）。本項目はMACRO PULSE向けの残タスク。
- [ ] CANSLIM候補スクリーニングリスト（US株対象）
- [ ] 資金フロー（大口/小口）表示
- [ ] 決算ウォッチ用プレ/アフターマーケットデータ

【Moomoo API Skill 移行】※2026-06-07以降着手
- 背景: moomoo証券が2026年4月にリリースしたClaude Code向けSkillパック
  自然言語指示で発注・バックテスト・戦略変更が可能
  現在の手製trader.pyと基本アーキテクチャ（ローカルPC+OpenD）は同じ
- 前提: signal.jsonの蓄積データ（2026-04-04〜）でバックテストを実施してから移行判断
- 手順:
  ① Claude CodeにMoomoo API Skillをインストール・動作確認
  ② 蓄積済みsignal.jsonデータ（約62件）でF&G Level2×TQQQ戦略をバックテスト
  ③ 結果が良好なら手製trader.pyをAPI Skillに移行
- 懸念: OpenDのローカルPC起動が前提だが、KoichiさんのAutoTrade運用のためOpenDは
  既に常時起動しており、この制約は実質的に解消済み（2026-07-10確認）。
  ただしPC自体の停止・再起動（ハードウェア障害・停電・OS更新等）が発生した場合は
  連携も止まるため、「運用上の恒常的な制約」ではなく「稀な障害シナリオ」として
  引き続き留意する
- 参考: https://www.moomoo.com/ja/community/feed/moomoo-api-skills-now-unlocked-ai-becomes-a-24-7-116413328916486

【SCREEN-2STAGE-1】二段階スクリーニング運用（構想）
- 背景: moomoo AIによる価格・出来高ベースのテクニカルスクリーニング
  （移動平均線順序・52週高値位置・RSI等）は取得できるが、
  ミネルヴィニ条件に必要なRS Rating・EPS/売上成長率の加速は
  moomoo単体では評価できないことが判明（2026-07-02）
- 想定運用: ①moomooで価格モメンタム側を粗くスクリーニング
  →②On-a-Journey登録後、既存の四半期系列データ
  （TANUKI VALUATIONのnormalized/series_q等）を使い、
  EPS/売上成長率が直近四半期で加速しているかを事後確認する
- 目的: 「それっぽい」スクリーニングから、ミネルヴィニ条件に
  近い精度への引き上げ
- 実装イメージ: 既存の四半期系列から成長率加速判定を行う
  軽量スクリプト（新規 or 既存パイプラインへの追加関数）。
  詳細設計は未着手
- 優先度: 低（構想段階、着手時期未定）

### 【TANUKI TAIL】
- 残タスク: データパス統一（優先度低）
- ~~EWM楽観バイアス係数~~ → TAIL-EWM-1としてB案（現状維持）でクローズ済み（2026-06-26）

### 【情報収集支援システム】
- ~~カタリスト×割安検知（価格下落+空売り比率+カタリスト接近）~~ → CATALYST-1として実装完了（2026-06-25）
- [ ] テック/市場ブレークスルーニュース分類
- [ ] NEWS_API_KEY + Grok使用、yfinance/FMP連携

### 【Market Pulse】
- [ ] 予測バックテスト表示

---

## 設計相談メモ（未着手）

### [HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1] HypeCore/期待値評価フレームワーク構想の統合（元DESIGN-2/4/5/6/14/15）
**優先度:** 低（いずれも技術的な実装課題ではなく、Koichiさんの投資方針・
製品方向性の判断待ちの構想メモ）
**分類:** 設計相談メモ / HypeCore / 期待値評価フレームワーク
**登録日:** 各サブ項目の元登録日は不明（「## 設計相談メモ（未着手）」節に
日付なしで記録されていたもの）。統合日: 2026-09-05
**発見:** 2026-09-05のBACKLOG横断整理

#### 統合の経緯
DESIGN-2・DESIGN-4・DESIGN-5・DESIGN-6・DESIGN-14・DESIGN-15の6件は、
いずれも「HypeCore/期待値評価フレームワーク」という一つの構想の異なる
側面（マクロ層・期待込み価値・期待の可視化・経営者評価・非線形成長
検知・期待と理論価格の整理）であり、相互に連動している（DESIGN-4は
原文に「DESIGN-2・DESIGN-5と連動」と明記）。技術的な実装課題ではなく、
いずれもKoichiさんの投資方針・製品方向性の判断待ちの構想メモである
ため、2026-09-05に1エントリへ統合した。

元の6件（DESIGN-2/4/5/6/14/15というID）はBACKLOG.mdから削除し（統合後
エントリへの一本化のためBACKLOG_DONE.mdへの移設ではなく削除）、内容は
要約せず全文そのまま以下の①〜⑥に保持する。**唯一の変更点**: 元
DESIGN-15内部の見出し（目的/設計方針/レイヤー構造/着手条件/理論的背景/
実装難易度）は、本エントリの見出し階層に正しくネストさせるため
`####`→`#####`へ1段階分だけ下げた（文言・内容は一切変更していない）。

#### ① 元[DESIGN-2] マクロによる銘柄フェーズ変化の認識
- 概要: マクロ環境（金利・流動性・センチメント）の変化が
  銘柄固有の品質変化なしにHypeCoreフェーズを変動させることを認める
- 設計: 2層構造
  Layer1（マクロ環境層）: Risk-On/Neutral/Risk-Off
  Layer2（銘柄固有層）: 現行HypeCore Phase1〜4
  最終フェーズ = 銘柄フェーズ × マクロ補正
- 連携: TANUKIの高成長期間・成長率への反映も将来検討
- 実装難易度: 高

#### ② 元[DESIGN-4] 期待込みの価値計算
- 概要: TANUKI（本源的価値）+ Moat Premium（競争優位・Moat Score連動Phase1で表現）
  + マクロ補正 = 期待込みの価値（フロアまたは最高値の目安）
- 注記: ALPHA-REDESIGN-1（2026-06-25）でHypeCore αは廃止。
  Moat Scoreが競争優位の定量化を担う設計に変更済み。
- 連携: DESIGN-2・DESIGN-5と連動
- 実装難易度: 高

#### ③ 元[DESIGN-5] 期待の要素と構造の可視化
- 概要: 株価に織り込まれた「期待」を分解して可視化する
  TAM期待・シェア期待・利益率期待・時間軸期待・流動性期待
- 現状: 逆DCF（必要成長率）は実装済み → 拡張
- アイデア未固まり。設計を深める必要あり
- 実装難易度: 高

#### ④ 元[DESIGN-6] 経営者の実行力評価
- 概要: 目標の難易度 × ビート度合いで経営者を定量評価
- 指標候補:
  ガイダンス達成率（過去8四半期の実績/予想）
  売上成長の加速度
  ROICの改善トレンド
  SBC比率（希薄化の質）
- データ: EPS Analyzerで近似可能
- 実装難易度: 中

#### ⑤ 元[DESIGN-14] 非線形的成長の検知スコア
- 概要: 構造変化×経営者実行力×業界変曲点の3要素で
  企業が非線形的成長を起こしそうかをスコア化
  非線形成長スコア = 構造変化(40%) × 実行力(30%) × 変曲点(30%)
- 各要素の計算:
  構造変化: RPO急増・粗利率急改善・Grok分析
  経営者実行力: ガイダンス達成率・ROIC改善・成長加速度
  業界変曲点: RPO/売上比率・競合動向・Grok分析
- TANUKIとの連携:
  スコア高→成長期間延長・逓減傾きを緩やかに設定
- 実装難易度: 高

#### ⑥ 元[DESIGN-15] 期待と理論価格の関係の整理（前提課題）

##### 目的
「なぜ今この乖離率なのか」を銘柄をまたいで同じフレームで説明できるようにする。
数値的精密さよりも説明の一貫性と納得感が目的。

##### 設計方針
理論価格（資産＋FCF割引現在価値）と市場価格の差分（乖離率）の時系列を蓄積し、
主要イベントとオーバーレイすることで、人間が目視で因果を判断できる材料を提供する。

##### レイヤー構造（APTベース）
乖離率の発生要因を以下の4レイヤーで説明する：
- L1 マクロ：金利・景気サイクルへの感応度（全銘柄共通）
- L2 マーケット：相場を動かすテーマへの感応度（全銘柄共通、やや業種差あり）
- L3 業種／業態：業種ナラティブの盛衰（例：SaaSの死、AI半導体相場）への感応度（業種単位）
- L4 銘柄固有：個社のナラティブ・期待（HypeCoreが担う領域）

各レイヤーは正にも負にも働く調整要因であり、独立ではなく相互に影響し合った結果が乖離率として現れる。

##### 着手条件
- 過去理論価格の時系列蓄積には財務データのpoint-in-time管理が必要
- 精度を妥協した実装は行わない
- ARCH-DATA-1（SECデータ正規化レイヤー強化）が実質的に完了してから着手する
  → ✅ 2026-07-20充足（ARCH-DATA-1完了・BACKLOG_DONE.md参照）。ただし
    着手自体は他の優先度判断とは独立に次回セッションで判断
- それまでは理論価格スナップショットの定期保存（将来の時系列構築のための仕込み）のみ検討する

##### 理論的背景
- APT（裁定価格理論、Ross 1976）を骨格として採用
- 因子はFama-Frenchから借用せず、ONAJURNEY独自指標で構成する
  （RECESSION RISK SCORE・Market Pulseセンチメント・セクター騰落・HypeCoreスコア）

##### 実装難易度
高

#### [[STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1]]との関係整理（重複領域確認、2026-09-05）
本エントリと`[[STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1]]`（HypeCoreを
「IVと市場価格の乖離分析・予測」に特化させる新機能epic）は、いずれも
「HypeCore側でIV/市場価格の乖離を扱う」という点で重複領域を持つ:
- **⑥（元DESIGN-15）とのL4「銘柄固有：個社のナラティブ・期待
  （HypeCoreが担う領域）」**は、STOCKHTML側の「DCF理論価格とERPの方向性
  食い違いから乖離要因候補を提示する」という着想と同じ問題意識
  （なぜ理論価格と市場価格が乖離するか）を扱っている。⑥のAPT型4層
  （マクロ/マーケット/業種/銘柄固有）構造は、STOCKHTML側の乖離要因候補
  （Beta・RISK EVENTS・HypePhase等）をより体系的な骨格に位置づけ直す
  ヒントになりうる
- **①（元DESIGN-2）のマクロ層（Risk-On/Neutral/Risk-Off）**は、STOCKHTML
  側が今後扱う行動経済学的要素（アンカリング・モメンタム等、Timing側の
  「勢いの持続性・剥落」）とは異なる粒度（マクロ環境全体 vs 個別銘柄の
  価格パターン）だが、両者とも「HypeCoreフェーズ判定に外部要因を
  組み込む」という方向性は共通する
- 一方、②③④⑤（期待込みの価値計算・期待の可視化・経営者実行力評価・
  非線形成長検知）はSTOCKHTML側のスコープ（Timing側＝IVと市場価格の
  関係分析）とは異なり、TANUKI VALUATION側（Funda側＝IVそのものの
  算出・成長率設定）に近い内容であり、重複はない
- 統合はせず別エントリのまま維持する（本エントリはKoichiさんの方針
  判断待ちの構想メモ群、STOCKHTML側は既に設計・実装フェーズに進んで
  いるエントリのため、状態管理が異なる）。STOCKHTML側の詳細設計に
  着手する際は、本エントリ（特に①⑥）を参照検討することを推奨する

#### 着手条件
なし（Koichiさんとの投資方針・製品方向性の確認待ち。個別サブ項目
（①〜⑥）ごとに着手可否を判断する）

---

（[[REPORT-TXT-CAPM-IV-MISSING-1]]は2026-08-27実装完了（8フィールド
全対応）、BACKLOG_DONE.md「2026-08-27（完了）」参照）

---

### [STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1] HypeCoreを「IVと市場価格の乖離分析・予測」に特化させる新機能epic（シグナル整合性チェック＋行動経済学的要素、GROWTH-1除去分の受け皿）
**優先度:** 中（投資判断の質向上に資するが、緊急性はない）
**分類:** 新機能 / TANUKI VALUATION / HypeCore / フロントエンド
**登録日:** 2026-08-23
**追記日:** 2026-08-26（`[[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]`STEP A完了を
受け、コンセプトを拡張。行動経済学的要素の追加・GROWTH-1除去分の
受け皿という位置づけを明確化）
**発見:** チャット側Claudeとの検討で設計方針が固まった（Koichiさん発案）

#### 元の内容（2026-08-23登録時点のもの）
DCFの理論価格とERP（市場織り込み型の期待水準指標）の方向性が食い違う
場合、それはDCFが拾いきれていない要素（当該銘柄のBeta〈市場リスク〉、
個別リスクイベント〈訴訟・規制調査等〉、HypePhaseが示す成長持続性への
市場の懐疑）が存在することを意味しうる、という考察に基づき、これを
個別銘柄ごとの一過性コメントではなく全ティッカー共通で再利用できる
仕組みとして、stock.html（個別銘柄ページ）に新設のセクション
「シグナル整合性チェック」を追加する。

新設セクションの目的: DCF乖離率・ERP・アナリスト目標株価・HypePhase
を横断的に照合し、これらが同じ方向を示さない場合に、既存データ
（Beta、RISK EVENTSの重要度、Growth_Rate_Recとの乖離フラグ等）から
導ける乖離要因の候補を自動的に表示する。

#### 拡張コンセプト（2026-08-26追記）

`[[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]`のSTEP Aで、GROWTH-1
（HypePhaseに応じてDCF成長率＝Funda側の加重比率を変える仕組み）を
Funda側から削除し固定50:50へ復元した（2026-08-26完了）。この削除に
伴い、Koichiさんの最終判断として、HypePhaseが本来捉えようとしていた
「勢いの持続性・剥落」という発想自体は無駄にせず、Timing側（HypeCore）
の機能として正式に引き取ることになった。

これにより本エントリのスコープを、単発の「シグナル整合性チェック
セクション追加」から、**HypeCoreを「本源価値(IV)と市場価格の関係の
分析・将来予測」に特化させる新機能epic**へ拡張する。含む要素:

1. **既存のシグナル整合性チェック構想**（上記「元の内容」）: DCF乖離率・
   ERP・アナリスト目標・HypePhase・RISK EVENTSを横断照合し、乖離要因
   候補を提示する
2. **行動経済学的要素（新規・未実装）**: アンカリング・過剰反応・
   モメンタムの持続と反転パターン等を新たに組み込み、GROWTH-1が担って
   いた「勢いの持続性・剥落」という発想を、Funda側（DCF成長率）では
   なくTiming側（HypeCore）の機能として再現する。具体的な指標設計
   （例: どのタイミングでモメンタム持続 vs 反転を判定するか、何を
   もって「アンカリング」と識別するか）は未確定——次回Koichiさんとの
   確認事項

**epicの目的の再定義**: 「IVを算出すること」（Funda側の責務、
DCF計算そのもの）ではなく、「既に算出済みの（センチメントから独立
した）IVと、実際の市場価格がなぜ・どう乖離しているか、今後どう推移
しうるかを分析・予測すること」（Timing側の責務）に特化させる。これは
本システムの設計思想（`Funda_Score`/`Timing_Score`分離、割引率側は
Rm=10%固定・Beta非考慮でセンチメントを意図的に排除）と、GROWTH-1
除去後により明確に整合する。

#### `[[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]`との関係整理（重複登録
防止のため明記）
- `[[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]`: **Funda側**（DCF成長率
  計算）からHypePhase依存を削除する課題。STEP Aで完了済み（固定50:50
  へ復元）
- 本エントリ（`[[STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1]]`）:
  **Timing側**（HypeCore）でHypePhaseが示す情報を活用する課題。
  GROWTH-1除去で「行き場を失った」勢い・剥落という発想の受け皿として、
  既存のシグナル整合性チェック構想と統合する
- 両者は「HypePhaseという同じ入力データを、Funda側から削除しTiming側
  で正式に扱う」という表裏の関係にあり、重複ではなく相互補完。前者は
  実装完了、後者はこれから設計・実装するため、別エントリとして残す
  （1エントリに統合すると「完了」と「未着手」が混在し状態管理が煩雑に
  なるため）

#### `[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]`との関係整理（2026-09-05
追記、重複登録防止のため明記）
`[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]`（旧DESIGN-2/4/5/6/14/15を
統合した設計相談メモ）と本エントリは、いずれも「HypeCore側でIV/市場
価格の乖離を扱う」という点で重複領域を持つ:
- 同エントリ⑥（旧DESIGN-15、期待と理論価格の関係の整理）のL4
  「銘柄固有：個社のナラティブ・期待（HypeCoreが担う領域）」は、
  本エントリの「DCF理論価格とERPの方向性食い違いから乖離要因候補を
  提示する」という着想と同じ問題意識を扱っている。同エントリ⑥のAPT型
  4層（マクロ/マーケット/業種/銘柄固有）構造は、本エントリの乖離要因
  候補（Beta・RISK EVENTS・HypePhase等）を体系的な骨格に位置づけ直す
  ヒントになりうる
- 同エントリ①（旧DESIGN-2、マクロによる銘柄フェーズ変化の認識）の
  マクロ層は、本エントリが今後扱う行動経済学的要素とは粒度が異なる
  （マクロ環境全体 vs 個別銘柄の価格パターン）が、「HypeCoreフェーズ
  判定に外部要因を組み込む」方向性は共通する
- 統合はせず別エントリのまま維持する（`[[HYPECORE-EXPECTATION-
  FRAMEWORK-EPIC-1]]`はKoichiさんの方針判断待ちの構想メモ群、本エントリ
  は既に設計・実装フェーズに進んでいるため状態管理が異なる）。本エントリ
  の詳細設計に着手する際は、同エントリ（特に①⑥）を参照検討すること
  を推奨する

#### 実コード確認結果（2026-08-23登録前に実施した最小限の事前調査）
各データソースの現状把握:
- Beta: `latest.json`の`wacc.beta`（`stock.html:916`で`const beta =
  wacc.beta || 1.0;`として既に読み込み・表示中）
- ERP: `pipeline.py:978`で`latest_data["erp"]`として保存されているが、
  **stock.htmlは現状これを一切参照していない**（`stock.html`全体を
  grepしてもERP関連の変数・表示は0件。report.txt[7]HYPECOREセクション
  にのみ表示されている）
- アナリスト目標株価: `latest.json`の`components.analyst_target_median`
  等（`stock.html:950`で既に読み込み・表示中）
- HypePhase: stock.htmlは`docs/value-monitor/hypecore/data/
  {ticker}_poc.json`を`stock.html:659`で既に直接fetchしている
- RISK EVENTS: `latest.json`の`risk_events`（`type`/`summary`/`impact`
  の3フィールド構成、`stock.html:1332`で`const evs = d.risk_events;`
  として既に読み込み・表示中）
- 上記よりERP以外は既にstock.html側で取得済みのデータであり、新
  セクションは主にERPの新規配線＋既存データの横断ロジック追加で実現
  できる可能性が高いが、確定的な判断は次回セッションでの詳細調査が
  必要
- `[[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]`（Layer1成長率のHypePhase
  減速適用有無）の結論は、この新セクションが提示する「乖離要因候補」
  の一つ（HypePhaseが示す成長持続性への市場の懐疑、という論点）として
  扱う。~~同課題の結論が出るまで本セクションの詳細設計は確定しない~~
  → **2026-08-26、同課題はSTEP Aで解消済み（HypePhase加重をFunda側
  から削除・固定50:50へ復元）。本エントリの詳細設計に着手可能な状態と
  なった**

#### 対応方針（未定・要調査、2026-08-26拡張版）
- stock.htmlの現在の生成・データ取得構造を上記より詳しく確認し、新
  セクションが既存データの組み合わせだけで実現できるか、追加のデータ
  生成（バックエンド側の新規計算）が必要かを判断する
- ポートフォリオ・TAILウォッチリスト全体で、このセクションが実際に
  何か表示する対象となる銘柄数（Beta高×RISK EVENTS高重要度×DCF乖離
  大×ERP中立以下、に該当する銘柄数）を概算し、実装の投資対効果を
  判断する材料とする
- **（2026-08-26追加）行動経済学的要素の具体的な設計案をKoichiさんと
  検討する**: どの指標（例: 価格モメンタムの持続日数、出来高急増後の
  反転パターン、アナリスト目標修正のアンカリング効果等）を採用するか、
  既存のHypeCore Phase1-4判定ロジック（`hypecore.py`、価格とMA200の
  位置関係・モメンタム・RSI・出来高ベース）とどう統合するか、GROWTH-1
  時代のような「実証データなしの理論的仮説のみでの導入」を繰り返さない
  ためのバックテスト・検証方法を含めて設計する
- HypeCoreの現在の実装構造（`hypecore.py`のPhase判定ロジック、
  `docs/value-monitor/hypecore/data/{ticker}_poc.json`のデータ構造）と
  stock.html/report.txtとのデータ連携経路を、詳細設計着手前に確認する

#### 着手条件
具体的な行動経済学的要素の設計内容をKoichiさんと確定してから着手する。
今回は登録内容の拡張（コンセプト整理）のみで実装しない。

---

## システム設計の基本思想（2026-05-31）

### On-a-journeyの本質的な目的

このシステムは「情報表示ツール」ではなく
「投資仮説の構築・検証を支援するツール」である。

長期投資家の本質的な行動サイクル：
  仮説を立てる
  → ポジションを取る（仮説への賭け）
  → 仮説を検証し続ける
  → 仮説が崩れたら撤退・正しければ保有継続

各システムの位置づけ：
  TANUKI VALUATION：
    「この企業は本質的にXXXドルの価値がある」
    という仮説を数値化するツール
  HypeCore：
    「今市場はどの程度の期待を織り込んでいるか」
    という仮説を検証するツール
  MACRO PULSE・Market Pulse：
    「仮説が成立する外部環境か」を確認するツール
  EPS Analyzer：
    「企業が仮説通りに実行しているか」を
    四半期ごとに検証するツール
  Discover：
    「次の有望な仮説候補を発掘する」ツール

この思想に基づき、全ての新機能開発において
「仮説の構築・検証にどう貢献するか」を
設計判断の基準とする。

---

## 開発方針メモ（2026-06-19 統合時に追加）

### 今回の統合で見えた3つの教訓

**1. 表示系の個別バグは「症状」であり「病気」ではない**
凡例不足・ヘッダー不統一・列はみ出しの3カテゴリで合計36件、
全アクティブ課題の4割近くを占めていた。これらは個別に直すと
1件あたり小さな工数でも、画面数×指標数で掛け算的に増え続ける。
共通コンポーネント化（EPIC-LEGEND-1/EPIC-HEADER-1/EPIC-LAYOUT-1）に
先行投資すれば、今後の新機能開発でも「説明を書き忘れる」「ヘッダーが
バラバラになる」という再発自体を構造的に防げる。

**2. アーキテクチャ課題は「優先度：中」に埋もれると複利で効いてくる**
ARCH-DATA-1とBUG-SCORE-SYNC-1（→ARCH-SCORE-SYNC-1に改名）は
どちらも「個別バグ修正の繰り返しコスト」を生み出す根本原因であるにも
関わらず、これまで「個別バグの掃討が落ち着いてから」という消極的な
着手条件で塩漬けにされていた。直近1ヶ月の修正ログを読むと、
両者に起因するバグ修正だけで全体の半分近くを占めている。
今回「高」に格上げし、次の同種バグが出た時点で着手する条件に変更した。

**3. 「N/A」「–」の表示規約が画面ごとにバラバラ**
TSCORE-DISP-1/2、SILO-DISP-1/2、MP-DISP-4は同じ症状（空値の意味不明）。
EPIC化はしなかったが、どこかのタイミングで「空値表示規約」を
一度ドキュメント化し、site-nav.js的な共通JSに寄せることを推奨する。

**4. 個別タスク中に発見した構造的問題はその場でBACKLOG化する**
2026-06-21のセッションで、PORT-LOGIC-1実装中にARCH-PORTFOLIO-DUP-1を、
MACRO-BUG-1修正中にMACRO-COMPUTE-DUP-1を、それぞれ作業の副産物として
発見しBACKLOG登録した。個別タスクの調査・実装過程で見つかった「これは
ARCH-SCORE-SYNC-1と同種の問題では」という気づきを記憶やメモに留めず、
気づいた時点でBACKLOG.mdに登録することを標準動作とする。

### 次セッションでの着手順序（提案）

**（2026-08-08更新〈`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`未決定事項
9件の最終確定に伴う再更新〉。以下が最新の優先順位。旧「本線
（2026-08-05更新）」以下は`common/sec_data`統合フェーズD着手前
〈normalized/→data/統合案〉時点の古い計画のため陳腐化・参照時は
本節を優先すること）**

**1. `fetcher.py`新設**（`.history()`・`.download()`呼び出しの一元化、
`pandas_market_calendars`依存追加含む。`[[MARKETDATA-LAYER-
CONSTRUCTION-1]]`の未決定事項9件は2026-08-08に全件確定済みのため、
設計判断ステップは不要）

**2. `reader.py`新設**（API群の実装: `get_latest_price`・
`get_price_series`・`get_ma_deviation`・`get_attributes`・
`get_analyst_events`・`get_calendar`・`get_index_series`・
`get_sp500_constituents_prices`）

**3. 本番消費者8ファイル＋診断ツール2ファイルの段階的切替**
（`pipeline.py`・`data_fetcher.py`・`beta_fetcher.py`・`hypecore.py`・
`valuation_fetcher.py`・`collect_and_send.py`・
`breadth_calculator.py`・`collect.py`＋`score_verifier.py`・
`audit.py`。TANUKI VALUATION本体から、フェーズDと同様の優先順位を
検討）

**4. 周辺ツール2ファイルの切替**（`backfill_tech_pulse.py`・
`extract_key_facts.py`）

（上記1〜4完了後、新DB構築プロジェクト フェーズ1の残りコンポーネント
として`common/macro_data/`新設〈FRED統合層、`INPUT-A-024〜047`対応、
`INPUT_DATA_TOBE.md` 2-C参照、investigate未着手〉に着手する。着手前に
`docs/architecture/new_data_platform/EXTRACTION_DESIGN_PRINCIPLES.md`
を必ず確認すること）

**5. （本線外）本セッション・前セッションで蓄積した課題群**:

- **優先度中**: `[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`対応（AVGOの
  旧CIK登録が無関係な買収先企業Broadcom Corporationを指している疑い。
  対応方針3案〈旧CIK差し替え・現状維持＋警告・2006-2014年データ
  削除〉を検討・実装。着手条件: 新DB構築フェーズ1〈SEC EDGAR統合〉
  実質完了により充足済み）
- **優先度低**（Layer3関連課題群、一覧化。いずれも着手条件「なし」
  または実害発生時まで保留）:
  - `[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`（fetcher.py・
    dcf_validity_checker.pyの年次データ選択思想不一致、案2で決着済み・
    恒久的例外）
  - `[[STOCKHTML-LAYER3-PUBLISH-PIPELINE-MISSING-1]]`（stock.htmlの
    Layer3切替、公開パイプライン未整備のため着手見送り）
  - `[[STOCKHTML-YTD-FILTER-BUG-SUSPECT-1]]`（stock.html JS側の
    is_ytd未除外、実データでは未発現）
  - `[[HYPECORE-SUBSTAGE-LAYER3-UNVERIFIED-1]]`（detect_substage()の
    Layer3切替影響が未検証）
  - `[[TAIL-SHARESDILUTED-Q4-TIMING-RISK-1]]`（TANUKI TAILの
    eps_diluted計算、Q4タイミング依存の構造的リスク）
  - `[[FETCHER-PY-BS-FIELDS-DEAD-KEYS-1]]`（fetcher.pyの_BS_FIELDS
    デッドコード、Layer3移行とは無関係の既存バグ）
  - `[[PARSER-MERGED-TAG-MIXING-RISK-1]]`（parser.py::
    _extract_values_merged()のタグ混入リスク疑い）
  - `[[LAYER3-SNPS-STALE-TAG-PRIORITY-1]]`（SNPS FY2022 Revenue、
    Layer3候補タグ優先順位が修正再表示を拾えない構造的リスク）
  - `[[LAYER3-SM-SGA-SEPARATION-NONE-FALLOUT-1]]`（2026-09-05に旧
    FINTREND-SM-JOBY-NONE-1〈financial_trend_calculator.pyのSM
    フィールドJOBY None化〉と旧LAYER3-ROIC-WACC-NONE-4TICKERS-1
    〈COHR/LLY/JNJ/KLACのROIC-WACC比率None化〉を統合。いずれもSM/SGA
    概念分離の帰結という同一の根本原因）

---

**旧・本線（2026-08-05更新、新DB構築プロジェクト フェーズ1 Step1: SEC EDGAR統合、CHAT_RULES.md「本線逸脱防止」参照。陳腐化・参照不要）:**
0-A. ~~`[[SECDATA-STORAGE-FRAGMENTATION-1]]` Step1: 全消費者洗い出し~~
     ✅ 2026-08-05完了（raw/normalized/ttm/data/company_facts.json・
     EPS Analyzer/TANUKI TAIL独自経路の全消費者を実ファイルで確認）
0-B. ~~raw/削除（デッドコード除去）~~ ✅ 2026-08-05完了（詳細後述）
0-C. ~~`data/quarterly_{FYQ}.json` pl/cf/shares区分のYTD→単一四半期(SA)修正~~
     ✅ 2026-08-05完了（事前調査でpl/cf/shares区分が従来XBRL申告のYTD
     累積値のまま保存されていたと判明〈約65〜66%のエントリが該当〉。
     quarterly.py::_classify_period()・normalizer.py::_ytd_to_quarterly()
     を再利用する統一アルゴリズムをparser.py::parse_company_facts()に
     実装し、全105銘柄を実再パース。annual側は無変化（1,441ファイル
     横断比較で差分0件）、report_consistency_check.py NG=0・WARN=78件
     （不変）、pytest 497 passed/2 known failed確認。RCAT 2016Q3の
     SBC1件のみ四半期キー自体が消滅しファイル未上書きという別要因の
     残存を発見・`[[RCAT-2016Q3-ORPHANED-QUARTERLY-FILE-1]]`として
     記録。詳細はBACKLOG_DONE.md参照）
1. **新設アクセサの実装**: `reader.py::get_quarterly_series()`/
   `get_latest_quarterly()`相当のdata/quarterly_*.json版（フィールド
   単位の時系列抽出関数）。前回調査で未着手と判明済み
2. **5本番消費者のnormalized/→data/切り替え**: financial_trend_
   calculator.py・quarterly_review_generator.py・tail_dcf_bridge.py・
   hypecore.py・pipeline.py内5用途。フィールド名変換
   （PascalCase→snake_case）・`[[SCHEMA-NORMALIZED-ISSUES-1]]`①〜⑥の
   残り論点（②SM/SGA概念混同の設計判断・⑤ファイル名混在等）の解消方法
   確定を含む。詳細な論点整理は別途設計セッションで実施
3. **normalized/廃止**: 上記1・2完了後、全消費者がdata/へ移行済みと
   確認した上で実施（raw/削除と同じ手順: 全消費者洗い出し→Step0最終
   確認→削除）

**本線外・優先度中（2026-08-05更新、CHAT_RULES.md「本線逸脱防止」参照）:**
4. `[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`対応（旧CIK登録が無関係な
   買収先企業Broadcom Corpを指している疑い。対応方針3案〈差し替え/
   現状維持+警告/削除〉を検討・実装。着手条件成立まで保留）
   + MRVL/AVGO/DELL旧CIK拡張分の年度×フィールド単位の個別確認
   （AVGO分は上記の対応方針確定後に着手する方が効率的。MRVL/DELL分は
   先行して着手可）
   + `[[SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1]]`の検討
   ※ SCCO(2010-2019)のfixed_registry.json登録は2026-08-05
   Stage 3bで完了済み（gross_profit、10エントリ）。以前の依頼文に
   残タスクとして記載されていたが、現状確認の結果既に完了と判明。

**本線外・優先度低（2026-08-05更新）:**
5. `[[ONDS-LOAR-SHARES-SCALE-SUSPECT-1]]`（shares_basic単位スケール
   異常の疑い、記録のみ）
   + `[[RCAT-2016Q3-ORPHANED-QUARTERLY-FILE-1]]`（RCAT 2016Q3の
   quarterly_*.jsonが新ロジックで未上書きのまま残存、記録のみ）

**バグ修正（優先）:**
1. ~~ALPHA-REDESIGN-2: stock.htmlのα乗算残存・説明文修正~~ ✅ 2026-06-26完了
2. ~~STAGE0-STOCK-1: stock.htmlでstage=0が非表示~~ ✅ 2026-06-26完了
3. ~~HYPE-INF-1: poc.jsonにInf値混入（ASTS/JOBY）~~ ✅ 2026-06-26完了

**データ補完（コマンド実行のみ）:**
4. ~~SEC-CTRL-2: tailの内部統制データ8銘柄一括生成~~ ✅ 2026-06-26完了
5. ~~CATALYST-DATA-1: catalyst.py --allで全94銘柄初回投入~~ ✅ 2026-06-26完了

**機能・設定修正:**
6. ~~HYPE-FLAG-1: CSGP/ZSのcik_lookupフラグ設定~~ ✅ 2026-06-26完了
7. ~~HYPE-ENB-1: ENBのhypecore=false修正~~ ✅ 2026-06-26完了
8. ~~DISCOVER-THEMES-1: macro_themes_history.json初回生成~~ ✅ 2026-06-26完了

**本日追加完了（未予定だったが実施）:**
9. ~~TAIL-SAT-CORE-1: satelliteモーダルをcore同等6タブ構成に変更~~ ✅ 2026-06-26完了

※ 2026-06-26完了: EVAL-3・TANUKI-ENB-1・SILO-UX-1・MP-ASSETFLOW-UI-1・
  TANUKI-ROE-2（部分）・SS-1（クローズ）
※ 2026-06-26横断調査・バグ調査実施: PREVENT-1〜5・各バグ・設定不整合をBACKLOG登録済み
※ 2026-06-27完了: CN-ENB-1・RKLB-CLEANUP-1・PICK-DUP-1・TTM-NULL-1・STONKS-DIV-1（ガード確認+テスト追加）・PREVENT-1・PREVENT-2・PREVENT-3・SEC-CTRL-1（パス変更・Grok翻訳・マイグレーション）
※ 2026-07-01完了: STOCK-GLOSSARY-1・PREVENT-4・QBITconfig孤立エントリ削除・DUPONT-COLOR-1・EPS-BX-1・EXTREME-FEAR-1
※ 2026-07-03完了: ARCH-DATA-1-YTD（AMZN固有の追加回帰バグA・B発見・修正含め全101銘柄ロールアウト完了。副産物としてTEST-STALE-IV-1を登録）
※ 2026-07-08完了: MACRO-NFP-HIST-1（NFP過去履歴370件を水準→前月比に一括変換）・
  TTM-NULL-1（calc_ttm_series()内の見落とし箇所を追加修正、2026-06-27対応時の残存分）・
  STONKS-DIV-1（再調査の結果ガード済みと再確認、L625の回帰テスト追加）・
  BACKLOG-DEDUP-CHECK-1（BACKLOG.md/BACKLOG_DONE.md間ID重複の全数チェック、削除対象なしと判定）
※ 2026-07-09完了: TAIL-DCF-TABIDX-1（DCFタブindex不一致修正）・
  新規銘柄5件登録（RMBS/ENTG/TER/KLAC/LRCX）・PARSER-ENTG-COMPYEAR-1・
  XBRL-TAG-KLAC-1・CHECK-QREV-FYE-1（パーサーバグ3件根本修正、
  副産物としてXBRL-TAG-KLAC-1-FOLLOWUPを登録）
※ 2026-07-10: サテライト投資候補91銘柄への前提妥当性チェック展開に伴い、
  新規バグ・課題16件超を登録（GROWTH-SOURCE-LABEL-1・SEC-TAG-FICO-CPRT-1・
  STALE-REPORT-CLEANUP-1・CIK-ORPHAN-FLAGS-1・DESIGN-16等）。精査の結果
  GROWTH-FLOOR-VERDICT-1・DCF-REL-SYNC-1を「中」→「高」へ格上げ、
  ARCH-DATA-1-CONSOLIDATE-1を完了クローズ、RICE-INTEGRATE-1/MULTI-1に
  相互参照を追記。common/screening/にdcf_validity_checker.py・
  report_txt_parser.pyを正式格納。CHAT_RULES.mdに確認プロセス適用範囲・
  銘柄スクリーニング標準フローを追記。この結果、次に着手可能な
  優先度：高の項目はGROWTH-FLOOR-VERDICT-1・DCF-REL-SYNC-1・ARCH-DATA-1
  （着手条件成立済み・ただし難易度高）の3件（BACKTEST-SCORE-1は
  着手条件未達のため2026年10月以降まで対象外）。
※ 2026-07-11: SYSTEM_MAP.md全体像の実態調査を実施し出力先パス誤記5件を修正、
  銘柄振り分けの正本（cik_lookup.csv）セクションを新設。monitor_tickers.yaml
  同期漏れ6件（APGE/RMBS/ENTG/TER/KLAC/LRCX）を修正し、同6銘柄のEPS Analyzer
  データ生成（Step 5b）も実施。新規銘柄登録プロセスの構造診断を行い
  REGISTER-FLOW-REDESIGN-1を登録、続く銘柄リスト参照の横断調査で
  TICKER-SOURCE-UNIFY-1（根本課題）を登録。セッション終了時ブラッシュアップで
  PREVENT-5・TICKER-AUDIT-1・TICKER-SOURCE-UNIFY-1・REGISTER-FLOW-REDESIGN-1・
  PREFLIGHT-CHECK-1（いずれも優先度：中）の「## 優先度：低」への誤配置を
  「## 優先度：中」へ修正。
  **次セッションの筆頭候補は[[TICKER-SOURCE-UNIFY-1]]**（既存関数
  `common/sec_data/tickers.py`を呼ぶだけで直せる低コスト・低リスク対応。
  確定済みバグ2件: `registration_validator.py`のP1デフォルトスキャン・
  `adjusted_eps_analyzer/pipeline.py::run()`）。着手後、余力があれば
  [[REGISTER-FLOW-REDESIGN-1]]の残り対応方針（status列拡張・
  オーケストレーション化等、コスト高）に進む。

※ 2026-07-12: TICKER-SOURCE-UNIFY-1対応方針1〜3を完了しBACKLOG_DONE.mdへ
  全文移動。QUALITY-GATES-EPIC-1（バグ根絶に向けた5段階品質ゲート）を
  優先度：最高で新規登録し、Phase 1（BACKLOG重複統合・pytest全体実行化・
  WARN台帳導入）・Phase 2a（タグフォールバック選定ロジック統一、
  common/sec_data/tag_definitions.py新設）・Phase 2b-1（TTM鮮度チェック）・
  Phase 2b-2（段差型急変検知統合）・Phase 2b-3（EPS Analyzer fact選定ロジック
  統一）を同日中に完了。副産物としてSEC-TAG-FICO-CPRT-1・LLY-CAPEX-STALE-1・
  GROWTH-CAGR-SIGN-1（CAGR計算式符号反転バグ）・TTM-QUARTERS-CHECK-1・
  SPLIT-AUTO-CHECK-1等の個別バグを多数発見・修正。HYPECORE-SAVE-INDEX-
  NAMEERROR-1（3日間沈黙していた本番障害）を緊急対応で完了。詳細は
  BACKLOG_DONE.md「2026-07-12（完了）」セクション参照。
※ 2026-07-13: ARCH-DATA-1の棚卸し調査を実施し、QUALITY-GATES-EPIC-1の
  ゲート1/ゲート2への統合マッピングを確認。Phase 3前提整理として
  [[ARCH-DATA-1-PREP-1]]（TAG-DEFS-UNIFY-1クローズ・SOFI-DATA-1のLTDebt
  恒久修正〈2026-06-24の手動パッチが自動再生成で巻き戻っていたことを発見〉・
  audit.py UP-C検知・バグA/Bスコープ判断〈既に解消済みと判明〉）を完了。
  続けてPhase 3a（Gate2本体第一段階: `common/sec_data/contracts.py`新設。
  FinancialEntry/EntryProvenance/FCFSeriesで規約A・B・③を型化し、
  quarterly.py/normalizer.py/data_fetcher.pyに検証を配線）を完了。
  全105銘柄で新旧比較し値の差分0件を確認済み。詳細はBACKLOG_DONE.md
  「2026-07-13（完了）」セクション参照。
  ~~次セッションの候補: ①ASTS-SHARES-OSCILLATION-1 ②WARN12-COHR-ONDS-1・
  HYPECORE-DASHBOARD-COUNT-BUG-1 ③FLAG-THRESHOLD-DESIGN-1
  ④GATE2-PHASE3B-1~~ → ①②は同日中に完了（下記追記参照）。

追記（2026-07-13 同日2回目・セッション終了時ブラッシュアップ）:
上記候補のうち①[[ASTS-SHARES-OSCILLATION-1]]・②[[WARN12-COHR-ONDS-1]]・
[[HYPECORE-DASHBOARD-COUNT-BUG-1]]を全て完了。加えて予定外だった
[[TICKER-DIRECT-ACCESS-GUARD-1]]（FLAG-CONSUMER-AUDIT-2/3の再発防止CI
ガード新設、`tests/test_no_direct_ticker_access.py`）も完了し、同ガードで
発見した`tail_dcf_bridge.py`のtanukiフラグ検証漏れを修正した。

- ASTS-SHARES-OSCILLATION-1: 調査時点の推定（ASTS/AVAV/RCATの3銘柄）から
  恒久修正の全105銘柄新旧比較で影響範囲がCART/CEG/BROS/GEV/XOM/CONを
  加えた**9銘柄に拡大**。副次発見のBROS 2021-03-31（Up-C組織再編前
  四半期）の妥当性を一次情報で確認し[[EPS-UPC-PREREORG-1]]として分離登録
- WARN12-COHR-ONDS-1: 根本原因はfact競合型バグではなく、**SEC自動更新
  （日曜21:00 JST）とTANUKI VALUATION再生成（平日23:05 JST）の生成順序の
  ズレ**（約20時間の陳腐化窓）と判明。pipeline.py再実行のみで解消し、
  この構造的ギャップ自体を[[WORKFLOW-SEC-TANUKI-GAP-1]]として新規登録
- TICKER-DIRECT-ACCESS-GUARD-1: 全リポジトリスキャンでcik_lookup.csv直接
  パース12ファイル・ルートディレクトリlistdir直接スキャン5ファイルを検出・
  許可リスト化。うち3件の既存直し漏れ（`phase1_scan.py`・
  `backfill_history.py`は一回限りスクリプトの疑い、`tail_dcf_bridge.py`は
  同日中に修正）を発見し、後者2件は[[PHASE1-SCAN-CLEANUP-1]]・
  [[BACKFILL-HISTORY-CLEANUP-1]]として登録。副次発見の軽微な重複実装2件
  （[[SYSHEALTH-CIK-DEDUP-1]]・[[TAIL-CIK-LOOKUP-DEDUP-1]]）も登録

**次セッションの候補（優先順位の所感）**:
① [[FLAG-THRESHOLD-DESIGN-1]]（優先度：未定・4フラグの機械判定基準を
Koichiさんに複数案提示して確定させる設計セッション。他タスクの前提に
なりうるため筆頭候補）に進む。
② 余力があれば[[GATE2-PHASE3B-1]]（優先度：中・独立実装4ファイルの
reader.py統合＋規約C/Dの型化、規模見積もりから）・
[[GATE2-READER-FCFLIST-1]]（優先度：中・reader.py::get_fcf_list()の
順序規約未検証）・[[EPS-UPC-PREREORG-1]]（優先度：中・Up-C組織再編前
四半期のAdjusted EPS算入方針）・[[WORKFLOW-SEC-TANUKI-GAP-1]]
（優先度：中・SEC更新とTANUKI VALUATION更新のworkflow連携）のいずれかに
着手する。
③ 優先度：低の軽量クリーンアップ4件（[[PHASE1-SCAN-CLEANUP-1]]・
[[BACKFILL-HISTORY-CLEANUP-1]]・[[SYSHEALTH-CIK-DEDUP-1]]・
[[TAIL-CIK-LOOKUP-DEDUP-1]]、いずれも陳腐化確認・重複実装解消の軽微作業）
は手が空いた時に片付ける。
~~[[SPLIT-REALTIME-GAP-1]]~~ ✅ 2026-07-20完了（NVDA+新規発見AVGO/CPRT/
WMT/LRCX/CELH/TSLA〈8銘柄〉・KLAC事前登録、RCAT除外。詳細はBACKLOG_DONE.md参照）。
（[[DATA-JUMP-CHECK-GENERALIZE-1]]は2026-09-06実装完了・売上総利益/
CapExへ展開済み、BACKLOG_DONE.md参照。純利益/SBC向けの代替方式検討は
[[DATA-JUMP-CHECK-NETINCOME-SBC-1]]として分離登録）

（ARCH-SCORE-SYNC-1は2026-06-20、TAIL-SEC-1/EPIC-LEGEND-1は2026-06-21、
EPIC-HEADER-1は2026-06-21、EPIC-LAYOUT-1グループA/グループBは2026-06-22、
EPIC-LAYOUT-1グループC（SILO-DISP-3）・MP-GAUGE-NEEDLE-1・MACRO-DISP-2は
2026-06-23に完了。BACKLOG_DONE.md参照）

追記（2026-07-14）: FLAG-THRESHOLD-DESIGN-1の検討過程で
[[POLICY-AB-TREND-BLIND-1]]（優先度：高、8銘柄影響）を新規発見。
フラグ判定基準の設計はPolicy A/Bの結果を前提にできないため、
本バグの修正をFLAG-THRESHOLD-DESIGN-1より先行して対応する方針とした。
副次発見として[[FCF-EPS-CONVRATE-SECTOR-1]]・[[TRANSIENT-EXPENSE-COVERAGE-1]]
（いずれも優先度：未定）も新規登録。

追記（2026-07-14 同日2回目）: POLICY-AB-TREND-BLIND-1の網羅調査完了後、
ラベル（DCF_Reliability表示）の実利用価値は限定的（DCF数値自体には
影響せず、外部AI評価時の見え方緩和が主目的）と整理されたため、
優先度を高→低に変更。修正方針〈直近2年連続黒字を主基準に上方乖離を
LOW対象から除外〉は確定済みのまま保留し、後日必ず着手する。
代わりにFCF数値自体に影響しうる[[FCF-EPS-CONVRATE-SECTOR-1]]・
[[TRANSIENT-EXPENSE-COVERAGE-1]]を優先度：高に格上げし、次の着手対象とする。

追記（2026-07-14 同日3回目）: TRANSIENT-EXPENSE-COVERAGE-1のAVAV/RDW調査
完了。両銘柄とも一過性費用の検出漏れ（M&A取引費用タグ不足）は実在するが、
悪化の主因は別（運転資本変動）と10-K原文で確認したため、この2銘柄に
関しては現状のFCF数値・DCF計算は正しいと判断しクローズ・BACKLOG_DONE.mdへ
移動。副次発見のタグ・カテゴリ設計の穴を[[MA-INTEGRATION-TAG-GAP-1]]として
新規登録（優先度は全銘柄への影響範囲調査後に確定）。

追記（2026-07-14 同日4回目）: FCF-EPS-CONVRATE-SECTOR-1（LITE/SITM）の
調査完了。独立バグではなく既存[[SECTOR-FCF-RATE-BROKEN-1]]の実害具体例と
判明したためクローズ・BACKLOG_DONE.mdへ移動。同バグの優先度を中→高に
格上げ（LITE/SITMでの実害確認による）。副次発見の2課題（LITEの業種
カテゴリ欠落・固定比率設計の限界）を[[FCF-CONVRATE-DESIGN-LIMIT-1]]として
分離登録（着手条件: SECTOR-FCF-RATE-BROKEN-1完了後）。

追記（2026-07-14 セッション終了時ブラッシュアップ）: 本日1〜4回目の
変更を踏まえ、次セッションの筆頭候補を更新する。

**次セッションの筆頭候補は[[SECTOR-FCF-RATE-BROKEN-1]]**（本日 中→高に
格上げ・LITE/SITMでの実害を実データで確認済み・対応方針①②が既に整理
済みで着手条件もなし）。案①（`core_calculator.py:244`のsector変数差し替え、
低コスト）から着手し、効果範囲（Financial Services判定改善のみか）を
確認した上で案②（`damodaran_industry`連携、本格対応）の要否を判断する
のが妥当。

[[POLICY-AB-TREND-BLIND-1]]は優先度：高→低に変更済みだが、修正方針
〈直近2年連続黒字を主基準に上方乖離をLOW対象から除外〉は確定済みのまま
のため、余力があれば並行着手も可能（他タスクをブロックしない独立作業）。

[[FLAG-THRESHOLD-DESIGN-1]]は本日の議論でゴールを再整理した
（エントリ本文の「議論の要旨」追記参照）ものの、基準案の具体的な数値は
未確定のまま。[[POLICY-AB-TREND-BLIND-1]]の実装（優先度は下がったが
未着手）がstonks_silo判定基準の材料に影響するため、着手順序としては
SECTOR-FCF-RATE-BROKEN-1・POLICY-AB-TREND-BLIND-1の後が妥当。

新規登録の[[MA-INTEGRATION-TAG-GAP-1]]・[[FCF-CONVRATE-DESIGN-LIMIT-1]]
はいずれも優先度：未定（前者は全銘柄影響調査後、後者はSECTOR-FCF-RATE-
BROKEN-1完了後に再評価）のため、今回の筆頭候補には含めない。

追記（2026-07-14 実装完了）: [[SECTOR-FCF-RATE-BROKEN-1]]を実装・完了し
BACKLOG_DONE.mdへ全文移動した。①`core_calculator.py`のbeta_config.json
読み込みパス誤りを`data_fetcher.py::_load_beta_config()`呼び出しに統一、
②Damodaran公式データセット`indname.xls`への直接照合でtanuki=true97銘柄
（100銘柄中、CIX/MO/PMの3銘柄は対応キー不存在のため対応する省略キーが
なく据え置き）に`beta_config.json`の`sector`を新規付与、③既存
`TICKER_INDUSTRY_OVERRIDES`のうちテストデータと判明した8件
（HON/TDY/KULR/META/AMZN/NET/CIX/BKNG）+ CRWV（既存値がindname.xlsと
不一致と判明）をindname.xls実態値に修正。全105銘柄再生成・
report_consistency_check NG=0・pytest 309 passed（既知の2件除く）を確認済み。
副次課題[[FCF-CONVRATE-DESIGN-LIMIT-1]]の着手条件（本タスク完了）が
成立したため、次回セッションで再評価可能な状態になった。
これでSECTOR-FCF-RATE-BROKEN-1発の一連の調査・実装
（FCF-EPS-CONVRATE-SECTOR-1・MA-INTEGRATION-TAG-GAP-1・
FCF-CONVRATE-DESIGN-LIMIT-1・POLICY-AB-TREND-BLIND-1を含む）が一区切り。
次セッションの筆頭候補は[[FCF-CONVRATE-DESIGN-LIMIT-1]]（着手条件成立・
残存する8/114カバレッジ不足への対応方針検討）または
[[POLICY-AB-TREND-BLIND-1]]（修正方針確定済みで着手可能・優先度は低だが
軽量な独立作業）のいずれか。

追記（2026-07-14 セッション終了時ブラッシュアップ・2回目）:
[[SECTOR-FCF-RATE-BROKEN-1]]をコミット`3df6f4da2`（core_calculator.pyの
beta_config.json読み込みパス誤り修正＋indname.xls直接照合による
全銘柄sector一括付与＋TICKER_INDUSTRY_OVERRIDESテストデータ8件修正）・
`9e03134ad`（全105銘柄再生成）で完了・push済みであることを最終確認。
CIX/MO/PMの3銘柄は対応するDamodaran分類（Office Equipment & Services /
Tobacco）に対応する省略キーがSECTOR_TO_DAMODARANに存在しないため
sector未設定のまま残存するが、いずれも`fcf_conversion_config.json`の
8分類に該当しないため実害はない（default 0.70のまま、回帰でもない）。

次セッションの筆頭候補：
① [[FCF-CONVRATE-DESIGN-LIMIT-1]]（着手条件〈SECTOR-FCF-RATE-BROKEN-1完了〉
成立済み。LITEの`fcf_estimation.sector`が`Telecom_Equipment`に正しく設定
されたが該当カテゴリがないため`conversion_rate`はdefault(0.70)のまま
残存することを実データで確認済み。fcf_conversion_config.jsonのカテゴリ
拡張方針を検討）
② [[POLICY-AB-TREND-BLIND-1]]（優先度：低・修正方針〈直近2年連続黒字を
主基準に上方乖離をLOW対象から除外〉確定済みのまま。他タスクをブロック
しない軽量な独立作業のため余力があれば並行着手も可）

追記（2026-07-14 [[FCF-CONVRATE-DESIGN-LIMIT-1]] キー名不一致修正）:
上記①の着手として、まず`estimate_fcf_from_eps()`のconversion_rate基準
（純利益ベースであることを実装確認・確定）とダモドラン公式データ
（oifcff.xls、94業種）との整合性を調査したところ、想定していた
「LITEに対応するカテゴリが1つ足りない」以上に根が深い問題を発見した：
`fcf_conversion_config.json`の8カテゴリキー名が`config/beta_config.json`
のsector表記（Damodaran taxonomy準拠の略称）と文字列不一致で、
Software_Internet・Semiconductor以外の6カテゴリが該当銘柄0件＝
事実上デッドコード化していた。この6カテゴリのキー名リネーム
（数値は無変更）を実装し、38銘柄（TSLA/LMT他Aerospace_Defense6銘柄/
KO・PEP・CELH/SOFI・V・MSCI・FLYW・PAYS/ADBE・CRM・NOW・PLTR等
Software_System 23銘柄）で新たに非defaultレートが適用されるように
なったことを新旧比較で確認、pytest回帰なし（既知のMSFT/NVDA 2件除く）。
**ただし本修正はconfig側のキー名変更のみで、影響を受ける38銘柄の
`latest.json`/`report.txt`（本番データ）は未再生成のまま**——次回
再生成の要否・タイミングをKoichiさんと要確認。

次セッションの筆頭候補：
① 上記38銘柄の`pipeline.py`再実行・`report_consistency_check.py`
   NG=0確認・本番データコミットの要否判断（未実施のまま残っている）
② [[FCF-CONVRATE-DESIGN-LIMIT-1]]の残課題3点（LITE/SITM型のカテゴリ
   自体の欠如、固定比率設計の限界、EBIT(1-t)→純利益変換ロジック不在）
   ——優先度・対応方針は未定のまま
③ `Software_System`にリネームしたことで新規に対象となった23銘柄への
   レート0.80の妥当性検証（Azure型インフラ企業を想定した設計だが
   対象は汎用エンタープライズソフトウェア全般に拡大したため）

追記（2026-07-14 [[FCF-CONVRATE-DESIGN-LIMIT-1]] Software_Systemグループ分割実装完了）:
上記③の検証として23銘柄（IOT・QBTS/RBRK/S/SOUN除く18銘柄）の直近5年
実績（生FCF/調整済み純利益比率）を検証し、成熟ライセンス型（グループA、
平均≈1.00）とSaaS型（グループB、平均≈1.61）の二極化を確認。
`Software_System_Mature`/`Software_System_SaaS`の2カテゴリへ分割し、
18銘柄のsectorを実績ベースで確定した。新規銘柄向けには前受収益比率
（DR/Rev、閾値0.40）による暫定判定ロジック（`beta_fetcher.py
--classify-software-system`）と、実績蓄積後にpipeline.py実行のたびに
純関数として再判定する自己補正ロジック（`check_software_system_
reclassification()`、config書き換えなし・determine_fcf_base()と同設計
思想）を実装。全105銘柄で新旧比較・pytest・report_consistency_check
（NG=0）を確認済み。

このタスクの過程で[[VALIDATOR-IVPS-MISMATCH-1]]（validator.pyの
pt_shares_consistencyチェックが検証時と最終保存時で異なるIVを比較して
いる疑い、本タスクとは無関係の既存バグ）を新規発見・登録した。

次セッションの筆頭候補：
① [[VALIDATOR-IVPS-MISMATCH-1]]の影響範囲調査（WARN/FAILになっている
   銘柄の全件洗い出し）——DCF_Reliability表示の信頼性に関わるため
② [[FCF-CONVRATE-DESIGN-LIMIT-1]]残課題1〜3（LITE/SITM型のカテゴリ
   自体の欠如、固定比率設計の限界、EBIT(1-t)→純利益変換ロジック不在）
   ——優先度・対応方針は依然未定
③ 前受収益比率による暫定判定ロジックの分離精度（約78%）を踏まえ、
   新規銘柄登録が発生した際に実際にIOT型（境界近傍）のケースが
   出た場合の運用確認（テストケースがまだ実データで発生していない）

追記（本日セッション終了時ブラッシュアップ）: VALIDATOR-IVPS-MISMATCH-1
（主因: validator.pyがALPHA-REDESIGN-1のalpha非乗算式に未追随、
副因: _save_resultでのvalidation再実行漏れ）を対応①②で修正・完了
（コミット03b855b54・3d0f1de43・26328aab5）。全100銘柄で新旧比較し
pt_shares_consistency pass 36→100/100、overall PASS 34→69・WARN
64→30・FAIL 2→1を確認。report_consistency_check NG=0・pytest
309 passed（既知2件除く）。

派生課題2件を登録・優先度確定（コミットa6555e3b0）:
- [[REPORT-ALPHA-STALE-1]]（優先度：中〜高・pipeline.py:1478-1510の
  report.txt REPORT-6ブロックが廃止済みalpha乗算式のまま、DCF構成要素の
  自己完結性が崩れている実害あり・未着手）
- [[ALPHA-CAP-HARDCODE-1]]（優先度：低・実害なしと確認済み・
  validator.pyのformula_verification誤警告のみ）

次セッションの筆頭候補：
① [[REPORT-ALPHA-STALE-1]]（実害あり・優先度中〜高・未着手）
② [[FCF-CONVRATE-DESIGN-LIMIT-1]] 残課題1〜3（持ち越し中）
③ [[POLICY-AB-TREND-BLIND-1]]（優先度低・軽量な独立作業）
④ [[ALPHA-CAP-HARDCODE-1]]（優先度低・手が空いた時に）

追記（2026-07-15 [[REPORT-ALPHA-STALE-1]]完了）: 事前調査（読み取り専用）で
`pipeline.py:1478-1510`（REPORT-6ブロック）に加え、同ファイル内の
Definition固定テキスト2箇所（`[3.TANUKI VALUATION]`セクションの
`P_t = DCF_v0 × (1+Alpha) + ...`説明文、`[7.HYPECORE]`セクションの
`Alpha: ... added to IV`説明文）にも同型の陳腐化を追加発見したため、
これら3箇所を一括してスコープに含めて実装・完了した（コミット
`581a93d28`コード修正・`59ae5b6c6`全100銘柄再生成）。ADBE/NVDAで
Equity_Value ÷ Shares_Used = Intrinsic_Valueの式が成立することを手計算で
確認済み。report_consistency_check NG=0・pytest 309 passed（既知2件除く）。
横展開調査でscenarios.py/sensitivity.py/adjustments.py/validator.py/
stock.htmlはいずれも実害なしと確認済み（詳細はBACKLOG_DONE.md
「2026-07-15（完了）」セクション参照）。

これにより次セッションの筆頭候補を更新する：
① [[FCF-CONVRATE-DESIGN-LIMIT-1]] 残課題1〜3（LITE/SITM型のカテゴリ
   自体の欠如、固定比率設計の限界、EBIT(1-t)→純利益変換ロジック不在。
   着手条件成立済み・持ち越し中）
② [[POLICY-AB-TREND-BLIND-1]]（優先度：低・修正方針〈直近2年連続黒字を
   主基準に上方乖離をLOW対象から除外〉確定済み。他タスクをブロック
   しない軽量な独立作業）
③ [[ALPHA-CAP-HARDCODE-1]]（優先度：低・実害なしと確認済み・
   validator.pyのformula_verification誤警告のみ・手が空いた時に）

追記（2026-07-15 [[FCF-CONVRATE-DESIGN-LIMIT-1]]残課題2・3を
[[TRUST-SUMMARY-EPIC-1]]へ統合）: 残課題2（固定比率設計がサイクル変動
銘柄を表現できない構造的限界）・残課題3（EBIT(1-t)ベース→純利益ベース
変換ロジック不在）をTRUST-SUMMARY-EPIC-1のFCF/DCF信頼性層スコープへ
統合し、FCF-CONVRATE-DESIGN-LIMIT-1エントリからは削除（移設注記を追記）。
FCF-CONVRATE-DESIGN-LIMIT-1には残課題1（LITE/SITM型カテゴリ欠如。
LITE/SITM型カテゴリ追加の調査を別途依頼済み・報告待ち）・残課題4
（IOT等判定保留）・残課題5（暫定判定精度78%）のみ残置。

これにより次セッションの筆頭候補を更新する：
① [[FCF-CONVRATE-DESIGN-LIMIT-1]]残課題①（LITE/SITM型カテゴリ追加、
   調査依頼発行済み・報告待ち）
② [[TRUST-SUMMARY-EPIC-1]]（②③統合後の設計検討・優先度：高で未着手のまま）
③ [[POLICY-AB-TREND-BLIND-1]]（優先度：低・軽量な独立作業）
④ [[ALPHA-CAP-HARDCODE-1]]（優先度：低）

追記（2026-07-15 [[FCF-CONVRATE-DESIGN-LIMIT-1]]残課題①をTRUST-SUMMARY-EPIC-1へ統合）:
残課題①（LITE/SITM型カテゴリ欠如）の調査完了。SITMは既に解決済み
（beta_config.jsonのsector確定済み）、LITE単体の追加対応は残課題③と
同根の問題（EBIT(1-t)→純利益変換ロジック不在）のため見送りと判断。
加えて調査中にLITE以外44銘柄（うち33銘柄がfcf_estimation.applied=Trueで
default(0.70)使用中、乖離大: LITE 4.33倍・SPIR 8.65倍・LLY 1.92倍等）の
sector未収録という同型の広範なギャップを新規発見したため、個別対応では
なくTRUST-SUMMARY-EPIC-1のFCF/DCF信頼性層スコープへ統合。
FCF-CONVRATE-DESIGN-LIMIT-1エントリからは残課題①を削除し移設注記を追記、
残課題4（IOT等判定保留）・残課題5（暫定判定精度78%）のみ残置。

これにより次セッションの筆頭候補を更新する：
① [[TRUST-SUMMARY-EPIC-1]]（優先度：高・②③・残課題①統合後の
   設計検討、未着手）
② [[POLICY-AB-TREND-BLIND-1]]（優先度：低・軽量な独立作業）
③ [[ALPHA-CAP-HARDCODE-1]]（優先度：低）
④ [[FCF-CONVRATE-DESIGN-LIMIT-1]]（残課題4・5のみ残置・優先度未定
   のまま待機）

追記（2026-07-15 [[ARCH-DATA-1]]残課題①完了）: 計算層への重複実装
一本化（暦年グルーピング・BS項目同一時点原則）を完了（コミット
`4e4629a3b`・`60d44b2d8`）。調査の過程でV（Visa）の表示乖離（約$1.56B）・
SOUN（LTDebt誤除外）の2件の実害を発見・是正した（いずれもIntrinsic_Value・
TANUKI SCORE分類には影響なし）。残課題②（EPS Analyzer経路のスコープ判断）を
[[EPS-ANALYZER-NORMALIZE-SCOPE-1]]として分離登録。残課題③（パターン判定
ロジックの実装）は依然未着手。

これにより次セッションの筆頭候補を更新する：
① [[ARCH-DATA-1]]残課題③（パターン判定ロジックの実装、PREFLIGHT-CHECK-1と
   共有設計・今回洗い出したパターン一覧を材料に設計）
② [[EPS-ANALYZER-NORMALIZE-SCOPE-1]]（優先度未定・スコープ判断待ち）
③ [[TRUST-SUMMARY-EPIC-1]]（段階0の可視化検討はARCH-DATA-1①③の進捗を
   踏まえて再開）
④ [[POLICY-AB-TREND-BLIND-1]]（優先度：低・軽量な独立作業）

追記（2026-07-15 [[ARCH-DATA-1]]残課題③完了・revenue系タグ競合検知）:
当初想定していたPREFLIGHT-CHECK-1と共有する汎用パターン判定カタログ構想は
精度未検証のリスクが高いと判明したため見送り、revenue系タグ競合の実データ
検知（`common/sec_data/revenue_tag_conflict_check.py`新設、`update.py`の
Step1完了直後に配線）に最小スコープを絞って実装した（コミット
`f05cae0ba`）。SOFI・IONQの既知ケースを正しく再現することを確認し、全100
銘柄実行でPM・AVGO・DELL等の新規候補タグ競合を発見（詳細・対応要否は
[[REVENUE-TAG-CONFLICT-SCAN-1]]に分離登録）。副次的に未使用の
`quality_checker.py`を発見し[[QUALITY-CHECKER-CLEANUP-1]]として登録。
PREFLIGHT-CHECK-1エントリにも見送りの経緯を追記済み。

これにより次セッションの筆頭候補を更新する：
① [[TRUST-SUMMARY-EPIC-1]]（段階0の可視化検討を再開。ARCH-DATA-1残課題
   ①③が完了しFCF/DCF信頼性層〈段階2〉統合も済んだため、段階0側の
   前提が揃った状態）
② [[EPS-ANALYZER-NORMALIZE-SCOPE-1]]（優先度未定・スコープ判断待ち）
③ [[POLICY-AB-TREND-BLIND-1]]（優先度：低・軽量な独立作業）
④ [[QUALITY-CHECKER-CLEANUP-1]]（優先度：低・未使用コードの削除要否判断）

追記（2026-07-15 [[FY52WEEK-BUCKET-MISPLACE-1]]新規登録・実装は見送り）:
[[REVENUE-TAG-CONFLICT-SCAN-1]]で新規発見したAVGO/DELL/CAKE/ELFの
revenueタグ競合について、根本原因（52/53週会計年度企業で
`determine_fiscal_year()`の月判定により真の年次値が隣接年度バケツへ
系統的に押し出される）を特定した。duration filterによる最小修正
（誤った値→値なしの明示）を試行したが、①複数年度にわたる系統的な
ズレのため一部年度は隣接年度の値へのラベル誤りのまま横滑りするだけで
値なしにならない、②DELLの真のFY2019値自体がduration filterとは無関係な
別種のend_yearバケツ衝突により消失したまま、という2点から目的を
達成できないと判明。実装は復元・未コミットのまま
[[FY52WEEK-BUCKET-MISPLACE-1]]として根本修正の設計待ちで新規登録した
（growth_sanity実害はCAGR参照窓の外のため現時点でなしと確認済み）。
併せてTDY・ASTSの一次情報確認結果を[[REVENUE-TAG-PRIORITY-FRAGILE-1]]
として新規登録した。

これにより次セッションの筆頭候補を更新する：
① [[FY52WEEK-BUCKET-MISPLACE-1]]（優先度：高・根本修正の設計要）
② [[TRUST-SUMMARY-EPIC-1]]
③ [[EPS-ANALYZER-NORMALIZE-SCOPE-1]]
④ [[REVENUE-TAG-PRIORITY-FRAGILE-1]]
⑤ [[POLICY-AB-TREND-BLIND-1]]
⑥ [[QUALITY-CHECKER-CLEANUP-1]]

追記（2026-07-15 [[FY52WEEK-BUCKET-MISPLACE-1]]実装完了）:
submissions API（`reportDate==end_date`本人データ判定）による根本修正を
実装・全106銘柄のデータ再生成・検証まで完了し、BACKLOG_DONE.mdへ全文
移動した（コミット`b93daff80`〜`cd43b03cf`の5件、push済み）。当初10銘柄
に加え、実装過程で新規発見したfyタグ衝突8銘柄（CRM/FCX/WMT等）も解消。

実装過程で新規に2件（[[FY52WEEK-BS-INSTANT-FACT-1]]・
[[FY52WEEK-BS-NULL-SILENT-1]]、いずれも優先度：高・着手条件なし）と、
別原因のデータ欠損1件（[[MRVL-2019-2020-NULL-1]]、優先度：中〜低・
原因調査は別途依頼要）を新規登録した。

これにより次セッションの筆頭候補を更新する：
① [[FY52WEEK-BS-INSTANT-FACT-1]]（優先度：高・着手条件なし・instant
   fact〈BS項目〉向けの本人データ判定を今回のPL/CF項目向け実装と
   同型で再設計。対応方針は本文に明記済みで着手しやすい）
② [[FY52WEEK-BS-NULL-SILENT-1]]（優先度：高・着手条件なし・①と対になる
   構造的リスク。`or 0`パターンのNone検知＋明示的警告化。①の修正後も
   別原因のNone化全般に効くため、①と独立に着手可能）
③ [[TRUST-SUMMARY-EPIC-1]]（優先度：高・要設計・実装未着手の大規模EPIC。
   ①②はこのEPICが対象とする構造的リスクの具体事例のため、①②を先に
   片付けてから再開するのが妥当）
④ [[EPS-ANALYZER-NORMALIZE-SCOPE-1]]（優先度：未定だがスコープ判断のみの
   軽量タスク・着手条件は「次回セッションで方針判断してから」）
⑤ [[REVENUE-TAG-PRIORITY-FRAGILE-1]]（優先度：中〜低）
⑥ [[MRVL-2019-2020-NULL-1]]（優先度：中〜低・原因調査は別途依頼要）
⑦ [[QUALITY-CHECKER-CLEANUP-1]]（優先度：低）
⑧ [[POLICY-AB-TREND-BLIND-1]]（優先度：低・修正方針確定済みの軽量独立
   作業・他タスクをブロックしないため手が空いた時でも可）

追記（2026-07-16 [[FY52WEEK-BS-INSTANT-FACT-1]]事前調査②完了・ARCH-DATA-1へ統合）:
[[FY52WEEK-BS-INSTANT-FACT-1]]の安全弁ロジック設計調査（2回目）で、
`_own_override_is_safe`の安全弁条件2が12月決算企業で機能しない欠陥を
実データ（CDNS FY2015のtotal_assets/revenueがFY2014の値のまま誤って
保持されている実例）で確認した。個別パッチではなく、年次データ正規化を
「値の確定→決算アンカー日ベースの年度ラベル計算→XBRLタグとの突き合わせ
検証」の3段階で再設計する方針が固まったため、[[FY52WEEK-BS-INSTANT-FACT-1]]
は個別タスクとしてはクローズし[[ARCH-DATA-1]]へ統合、同時に[[ARCH-DATA-1]]
の優先度を「高」→「最高」に格上げした。副次発見として
[[CASH-TAG-MISSING-1]]（優先度：中、CAT/CPRT/ELF/GEV/HEIのcash_and_equivalents
欠落、52/53週バグとは無関係なタグ定義漏れ）を新規登録した。

これにより次セッションの筆頭候補を更新する：
①~~[[ARCH-DATA-1]]（3段階設計の実装着手・優先度：最高）~~
   ✅ ステージ1（値の確定）のみ2026-07-16完了。ステージ2・3は未着手
   （下記追記参照）
② [[QUALITY-GATES-EPIC-1]] Phase 3b（①と並行可）
③ [[FY52WEEK-BS-NULL-SILENT-1]]（①と独立着手可）
④ [[TRUST-SUMMARY-EPIC-1]]（①進捗待ちで据え置き）
⑤ [[POLICY-AB-TREND-BLIND-1]]（優先度：低・軽量な独立作業）
⑥ [[CASH-TAG-MISSING-1]]（優先度：中・新規）

追記（2026-07-16 [[ARCH-DATA-1]]ステージ1「値の確定」完了）:
10-K/A候補プール化・filed日タイブレーク・出所メタデータサイドカーを
実装・全105銘柄再生成完了（コミット`4587ee09e`・`ba9927676`）。事前
検証の185件・18銘柄と完全一致、pytest・report_consistency_check.py
とも変更前と同一水準を確認。DOCN/LYFT/QBTS/SPIRの個別確認では
TANUKI SCORE分類はいずれも不変（SPIRのみIntrinsic_Value_BASEが
+7.6%変化）。詳細は[[ARCH-DATA-1]]「残課題④」参照。

これにより次セッションの筆頭候補を更新する：
① [[ARCH-DATA-1]]ステージ2（年度ラベル計算のアンカー日ウィンドウ化）
② [[QUALITY-GATES-EPIC-1]] Phase 3b
③ [[FY52WEEK-BS-NULL-SILENT-1]]

追記（2026-07-17〜18 [[ARCH-DATA-1]]ステージ2・3完了／[[GATE2-PHASE3B-1]]
①②③-a③-b全完了）: 上記①②の両方が完了した。

[[ARCH-DATA-1]]はステージ2（アンカー日ウィンドウ方式。実装中にJNJ/TDY型
〈決算日が年境界12/31〜1/1を往復する52/53週企業〉で企業自身のfyタグと
矛盾する誤判定を新規発見し、循環クラスタリング方式（`_cluster_fiscal_
anchor_candidates()`）へ設計変更して解消）・ステージ3（fyタグ裏取り、
WARN-23新設。初版は`is_own_data`不問で4,434件・105銘柄という実用不能な
ノイズになったため`is_own_data=True`限定に設計変更）を完了し、
「値の確定→年度ラベル計算→裏取り」の3段階設計が全完了した（詳細は
[[ARCH-DATA-1]]「ステージ2完了」「ステージ3完了」参照）。

[[GATE2-PHASE3B-1]]（=[[QUALITY-GATES-EPIC-1]] Phase 3b）は①（4ファイル
のreader.py統合）・②（規約C、STOCK_FIELDS分類の網羅性契約）・③-a（規約D、
`GrowthVerdict`のEnum化）・③-b（規約D、`Classification`のEnum化）の
全項目を完了し、BACKLOG_DONE.mdへ全文移動した。②の実装検証で
STOCK_FIELDS/SHARES_FIELDS分類が構造的に本番未到達（`calc_ttm()`が
2026-05-07以降到達不能）という構造的問題を新規発見し[[TTM-STOCK-FIELDS-
DEAD-1]]として分離登録。③-bの事前調査でreport_txt_parser.pyの孤立モジュール
化・history.jsonのレガシーフィールド残存も新規発見し、それぞれ
[[REPORT-TXT-PARSER-CLEANUP-1]]・[[HISTORY-JSON-LEGACY-TANUKI-SCORE-1]]
として登録した（いずれも優先度：低）。

これにより次セッションの筆頭候補を更新する：
① [[ARCH-DATA-1]]残課題④（BS項目〈instant fact〉が本人データ判定の
   対象外のまま。CDNS型の実害〈修正済み〉と同根の未解消リスク）
② RCAT型決算期変更検知（企業が実際に決算期を変更したケースと単なる
   52/53週の測定誤差との区別。ARCH-DATA-1ステージ2のスコープ外として
   引き続き未着手）
   ※2026-07-31追記: この行の内容は現在も有効（未着手のまま）。
   [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]として2026-07-31に統合的に
   再登録した（ELF/RCAT/AVGOを対象とする統合タスク）。
③ [[FY52WEEK-BS-NULL-SILENT-1]]（優先度：高・着手条件なし。
   ①と独立に着手可能）
④ [[TRUST-SUMMARY-EPIC-1]]（優先度：高・要設計・実装未着手の大規模EPIC）
⑤ ~~WARN-23残り8銘柄（ADSK/AVAV/COHR/CRM/FCX/FICO/HON/WMT）の一次情報検証~~
   ✅ 2026-07-18完了。全10銘柄・12件でXBRL fyタグ側の誤りと確認、
   実害なし。詳細はARCH-DATA-1「WARN-23残り8銘柄の一次情報検証完了」参照
⑥ ~~[[TTM-STOCK-FIELDS-DEAD-1]]（方針判断のみの軽量タスク）~~
   ✅ 2026-07-18完了。対応方針(a)デッドコード削除を実施、
   BACKLOG_DONE.mdへ全文移動

追記（2026-07-21 FCF-CONVRATE①③調査完了・[[FCF-DIVERGENCE-SIGN-GUARD-1]]新規登録）:
これにより次セッションの筆頭候補を更新する：
① ~~[[FCF-DIVERGENCE-SIGN-GUARD-1]]（優先度：高・新規・実装コスト低）~~
   ✅ 2026-07-22完了（両方向の符号不一致を検知するよう2段階で実装。
   第1段階: raw_fcf>0×estimated_fcf<0の符号反転ガード
   〈f6201ae04a4e242bbda2014b0f71ca2ef42911b6〉。
   第2段階: raw_fcf<=0×estimated_fcf>0の対称ケース
   〈99014218b676fa4e36e4babefaf9ce407cac8ba4〉。
   回帰テスト計6件・100銘柄フローズン比較で影響なしを確認済み。
   詳細はBACKLOG_DONE.md参照）
② TRUST-SUMMARY-EPIC-1の①③②対応（2026-07-22調査で「②と同型に統合」
   という前回方針を撤回、3件に分けて設計し直す）:
   - ①: FCFEstimationResultに`rate_is_sector_default`等のフラグを追加し、
     機械的に検知・表示する設計（固定リスト不要）
   - ③: ticker単位の表示ではなく、fcf_conversion_config.json側に
     セクターカテゴリ単位の開発者向けメタ情報として持たせる設計
     （画面表示は変更しない）
   - ②表示不一致バグ（新規発見）: report.txtはfcf_estimation.applied=True
     前提でネストされているが、stock.htmlはticker集合の所属のみで判定し
     applied状態を見ないため、LITE等でapplied=Falseの間、両者の表示が
     食い違う。独立バグとして修正要（report.txt側の条件に合わせる方向を推奨）
   いずれも実装未着手

追記（2026-07-31 [[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]派生調査・
新規6件登録、登録のみで実装は未着手）:
これにより次セッションの筆頭候補を更新する：
① [[PERIOD-LENGTH-VALIDATION-GAP-1]]（優先度：高・新規。parser.pyのFLOW型
フィールド抽出〈`_extract_values_best_candidate()`→`_extract_single_key()`
経路〉に期間長検証が構造的に欠落しており、AVGO revenue/net_income/
operating_income(2016/2017)・gross_profit9銘柄で四半期値が年次値として
誤採用されていたことを確認済み。対応方針確定前に105銘柄×全FLOW型フィールドの
オフラインシミュレーションが必要）
② [[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]（優先度：高・要個別調査。BBAI/RDW/ELF/
KULRの10-K原本確認。①のシミュレーション精度に影響するため①と並行、または
①着手前に着手が望ましい）
③ 余力があれば[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]
（優先度：低〜中・MO/PM/SCCOの10-K原本確認）
④ [[SPAC-STUB-PERIOD-VERIFICATION-1]]・[[REPORT-CONSISTENCY-GROSSPROFIT-
COGS-CHECK-MISSING-1]]は①②の対応確定後（着手条件未達）。
[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]はcommon/sec_data統合
フェーズ進捗待ち（着手条件未達）。
[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]（本体）は①の解消が前提
のため、それまで着手保留。

追記（2026-07-31 [[PERIOD-LENGTH-VALIDATION-GAP-1]]実装完了）:
① ~~[[PERIOD-LENGTH-VALIDATION-GAP-1]]（優先度：高・全母集団シミュレーション
   〈9フィールド+revenue/S&M/D&A3フィールド〉→parser.py実装
   〈`_extract_single_key()`・`_extract_values_merged()`両方に340-380日
   フィルタ追加〉→全105銘柄フローズン入力再生成・検証まで完了）~~
   ✅ 2026-07-31完了（コード`e3723b3eb`・データ`d6d404016`。pytest 446 passed
   /2 known failed、report_consistency_check.py NG=0〈WARN 71→68件に減少〉。
   検証中にELF固有の別バグ（fiscal_end_month誤検出）を発見し
   [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]として新規登録、ELF分5ファイルは
   本コミットから除外。詳細はBACKLOG_DONE.md「2026-07-31（完了）」参照）。
これにより次セッションの筆頭候補を更新する：
~~① [[ELF-FISCAL-END-MONTH-MISDETECTION-1]]（優先度：高・新規。ELFの
   fiscal_end_month自動検出誤り〈3月と誤検出、実際は当該期間12月決算〉に
   よる年度ラベル一括ズレ。10-K原本で決算期変更の実態を確認してから設計）~~
   ✅ 2026-08-01完了（案②era別anchor対応を実装。コード`7c44ac266`・データ
   `6d9c18b2f`。全105銘柄でbucketing比較しELFのみ変化、RCAT/AVGO/MSCI/NOW
   は複数クラスタ検出も実害ゼロを確認。ELFのannual_2015-2018が真の暦年値に
   復旧、2014・2019（移行期）はPL/CF系フィールドをNone化。pytest 453
   passed/2 known failed、report_consistency_check.py NG=0〈WARN=68件、
   変化なし〉。詳細はBACKLOG_DONE.md「2026-08-01（完了）」参照。RCATの
   直近10-K重複投票は[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]として別途
   新規登録済み〈優先度：中、10-K原本確認は未着手〉。pushは保留、コミットのみ）
② [[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]（優先度：高・要個別調査。BBAI/RDW/ELF/
   KULRの10-K原本確認。ELFは①の案②実装で決算期変更自体は解消済みのため、
   本項目はELF 2015年の89日/333日フィールド分裂という別種の問題として
   引き続き要確認）
③ 余力があれば[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]
   （優先度：低〜中・MO/PM/SCCOの10-K原本確認）
④ [[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]（本体、着手条件は充足済み）
   の対応方針（①本番書き戻し／②突合検算ロジック追加）決定は、②の10-K確認
   結果を踏まえてから着手するのが望ましい。
⑤ [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]（優先度：中・新規。RCATの
   直近10-Kが12月31日・4月30日の両クラスタに同時投票、3段階目の決算期変更が
   進行中の可能性。10-K原本での個別確認が未着手）

追記（2026-08-02 [[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]個別調査〜
[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1完了、複数セッションにわたる進捗を
まとめて反映）:
~~② [[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]~~ ✅ 2026-08-01完了（BBAI/RDW/ELF/
   KULRを10-K原本で個別確認。ELF(2015)・KULR(2015)は対象外〈既に正しく
   処理済み〉と判明し除外、BBAI(2020)・RDW(2020)のPL/CF系は既に安全側に
   None化済みで追加対応不要と確認しクローズ扱いへ訂正。調査過程で新たに
   判明したBS系の実害を[[SPAC-SHELL-BS-ENTITY-MIXING-1]]として分離登録。
   詳細はBACKLOG_DONE.md「2026-08-01（完了）」参照）
~~[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1~~ ✅ 2026-08-02完了（コード
   `80e51d2c2`・データ`c5e588474`。BBAI(2020)/RDW(2020)/RKLB(2020)/
   SOFI(2020)/VRT(2019)/ONDS(2017)/KULR(2016)の7銘柄7年度で数学的矛盾を
   解消、全105銘柄フローズン入力比較で対象7件以外〈矛盾のない56件・
   KULR(2019)・SPIR(2020)含む〉に変化なしを確認。pytest 461 passed/
   2 known failed、report_consistency_check.py NG=0〈WARN=68件、変化
   なし〉。詳細は本ファイル該当エントリ参照）

これにより次セッションの筆頭候補を更新する：
① [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2（優先度：中・新規着手候補。
   SPIR(2020)型の"事故的な正しさ"を事前検知するSPAC合併疑いの機械的検知
   〈案B〉。submissions.jsonへのformerNames〈法人名変更履歴〉取得・保存
   拡張がデータ取得層の前提条件として必要）
② [[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]（優先度：中。KULR(2019)
   単独の課題に再定義済み。current_liabilities/total_liabilitiesが既に
   同一accnから採用されているにも関わらず矛盾しており、同一filing内での
   candidate tag誤選択が原因と確定。タグそのものを10-K原本と突合する
   個別調査が未着手）
③ [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]（優先度：中。10-K原本での
   個別確認が未着手、①②と独立に着手可能）
④ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。ELF是正済みデータに伴う
   ROE_avg(10yr)の再計算未反映。TANUKI VALUATION通常の定期更新サイクルで
   自然解消見込みのため、優先度高の項目ではない）
⑤ 余力があれば[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]
   （優先度：低〜中・MO/PM/SCCOの10-K原本確認）
⑥ [[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]（本体、着手条件は
   充足済み）の対応方針（①本番書き戻し／②突合検算ロジック追加）決定は、
   ⑤の10-K確認結果を踏まえてから着手するのが望ましい。

追記（2026-08-02 [[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]①本番書き戻し
実装完了）:
~~⑥ [[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]~~ ✅ 2026-08-02完了
（①本番書き戻しを実装。コード`dc0507c27`・データ`65ddd0d6b`。標準タグから
gross_profitが取得できない年度のみrevenue-cost_of_revenue逆算値で埋め、
`pl_provenance.gross_profit.derived=True`を付与。Case A対象34銘柄342件で
完全一致・Case B残存49件は無変化を確認。STONKS SILO fetcher.pyの重複自己
修復ロジックが実質デッドコード化したことを確認〈[[STONKS-SILO-FETCHER-
GROSSPROFIT-BACKFILL-DUP-1]]のクローズ判断材料〉。TANUKI VALUATIONへの
影響はゼロと確定。pytest 467 passed/2 known failed、report_consistency_
check.py NG=0〈WARN=68件、変化なし〉。②突合検算は[[GROSSPROFIT-COGS-
ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]へ引き継ぎ。詳細はBACKLOG_DONE.md
「2026-08-02（完了）」参照。副産物として[[HON-GROSSPROFIT-2009-RESIDUAL-
DISCREPANCY-1]]を新規登録・[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-
MO-PM-SCCO-1]]の対象を14銘柄へ拡大）
これにより次セッションの筆頭候補を更新する：
① [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2（優先度：中。SPIR(2020)型の
   事前検知、submissions.jsonへのformerNames取得拡張が前提）
② [[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]（優先度：中。KULR(2019)
   単独、candidate tag誤選択の10-K原本突合が未着手）
③ [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]（優先度：中。10-K原本での
   個別確認が未着手、①②と独立に着手可能）
④ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期
   更新サイクルで自然解消見込み）
⑤ [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは10年連続、LITE/CRMは新規大規模
   クラスタ。10-K原本確認が未着手）
⑥ 余力があれば[[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：
   低・HON(2009)単独の10-K原本確認）

追記（2026-08-02 セッション終了処理。[[STONKS-SILO-FETCHER-GROSSPROFIT-
BACKFILL-DUP-1]]クローズ）:
~~[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]~~ ✅ 2026-08-02
クローズ（[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]①実装により、
STONKS SILO対象25銘柄全体で発火条件〈gross_profit=None かつ revenue/
cost_of_revenue両方present〉が0件になったことを確認。実害解消済みだが
fetcher.py側のコード自体は残存〈デッドコード化、削除ではない〉。コード
整理はcommon/sec_data統合フェーズ1到達時に別途検討。詳細はBACKLOG_DONE.md
「2026-08-02（完了）」参照）

**2026-08-01〜02セッションの完了サマリ**: [[PERIOD-LENGTH-VALIDATION-
GAP-1]]・[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]・[[SPAC-STUB-PERIOD-
FIELD-SPLIT-1]]・[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1・[[LAYER3-
GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]①・[[STONKS-SILO-FETCHER-
GROSSPROFIT-BACKFILL-DUP-1]]の6件完了。新規登録:
[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]・[[ELF-ROE10YR-RECALC-
PENDING-1]]・[[SPAC-SHELL-BS-ENTITY-MIXING-1]]（段階2残存）・
[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]（KULR2019単独に再定義）・
[[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]。訂正:
[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（3銘柄→14銘柄へ
対象拡大）。

**次セッションでの着手順序（2026-08-02時点、優先度順に整理・最終版）**:
① [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2（優先度：中・実害が現在進行形
   だった段階1は完了済み。SPIR(2020)型の"事故的な正しさ"を事前検知する
   SPAC合併疑いの機械的検知〈案B〉。submissions.jsonへのformerNames
   〈法人名変更履歴〉取得・保存拡張がデータ取得層の前提条件として必要）
② [[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]（優先度：中・KULR(2019)
   単独の課題に再定義済み。current_liabilities/total_liabilitiesが既に
   同一accnから採用されているにも関わらず矛盾しており、同一filing内での
   candidate tag誤選択が原因と確定。タグそのものを10-K原本と突合する
   個別調査が未着手）
③ [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]（優先度：中・RCATの直近10-Kが
   12月31日・4月30日の両クラスタに同時投票、3段階目の決算期変更が進行中の
   可能性。10-K原本での個別確認が未着手、①②と独立に着手可能）
④ [[SPAC-STUB-PERIOD-VERIFICATION-1]]（優先度：中・SPAC合併前・IPO前と
   見られる正当な非365日期間データ11銘柄の個別確認、10-K原本での裏取り
   未実施）
⑤ [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは各10年連続、LITE(9年)/CRM(7年)は
   新規発見の大規模クラスタ。10-K原本確認が未着手）
⑥ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑦ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・ELF是正済みデータに伴う
   ROE_avg(10yr)の再計算未反映。TANUKI VALUATION通常の定期更新サイクルで
   自然解消見込みのため、単独での緊急着手は不要。次回定期更新後に反映
   確認・クローズ）
⑧ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・gross_profit/cost_of_revenue整合性の常設監査項目が存在しない。
   ⑤⑥の10-K確認が概ね収束してから、再発防止のための常設WARN項目化を
   検討するのが望ましい）

追記（2026-08-02 [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2実装完了）:
~~① [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2~~ ✅ 2026-08-02完了（コード
   `1f6e95d92`・データ`43470bccf`。fetcher.pyで既存レスポンスから
   formerNamesを追加取得〈新規APIコールなし〉、`_resolve_bs_entity_
   mixing()`にformerNames区間一致による新トリガー条件③'を追加。
   SPIR(2020)のlong_term_debtを新規検知・None化
   〈triggered_by="former_names_window"〉。BBAI/RDW/RKLB/SOFI/VRTは
   ③'でも重複検知されるが結果不変（冪等性を確認）。全105銘柄フローズン
   入力比較でSPIR以外に変化なし（RKLBの2025年再法人化という「単純な
   改名」ケースでの誤検知なしを含む）。spac_shell_detection_log.jsonを
   全105銘柄で新規生成。pytest 473 passed/2 known failed、
   report_consistency_check.py NG=0〈WARN=68件、変化なし〉。
   [[SPAC-SHELL-BS-ENTITY-MIXING-1]]は段階1・段階2ともに完了し
   BACKLOG_DONE.md「2026-08-02（完了）」へ全文移動。残り99銘柄の
   formerNamesは通常の週次自動更新で自然にバックフィルされる設計
   〈特別な一括再取得は未実施〉）
これにより次セッションの筆頭候補を更新する：
① [[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]（優先度：中・KULR(2019)
   単独、candidate tag誤選択の10-K原本突合が未着手）
② [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]（優先度：中・10-K原本での
   個別確認が未着手、①と独立に着手可能）
③ [[SPAC-STUB-PERIOD-VERIFICATION-1]]（優先度：中・SPAC合併前・IPO前と
   見られる正当な非365日期間データ11銘柄の個別確認、10-K原本での裏取り
   未実施）
④ [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは各10年連続、LITE(9年)/CRM(7年)は
   新規発見の大規模クラスタ。10-K原本確認が未着手）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・④の10-K確認が概ね収束してから常設WARN項目化を検討）

追記（2026-08-02 [[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]根本原因調査
完了）:
~~① [[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]~~ ✅ 原因確定・
   [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]へ統合しクローズ
   （BACKLOG_DONE.md「2026-08-02（完了）」へ移動）。KULR(2019)の矛盾は
   `XBRL_MAPPING["total_liabilities"]`の2番目のフォールバック候補
   `LiabilitiesAndStockholdersEquity`（定義上`Assets`と一致する誤った
   代替タグ）が原因と確定。予備スキャンで105銘柄中278件（AMZN/GOOGL/
   MSFT/NVDA等含む）に及ぶ横断的な設計欠陥と判明したため、KULR単独対応
   ではなく新規タスクへ統合。
これにより次セッションの筆頭候補を更新する（優先度順）：
① [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]（優先度：高・新規。
   278件（銘柄年度）・AMZN/GOOGL/MSFT/NVDA等の大型株を含む候補タグ設計
   欠陥。対応方針（案A/案B）の設計調査・全母集団シミュレーションが未着手）
② [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]（優先度：中・10-K原本での
   個別確認が未着手、①と独立に着手可能）
③ [[SPAC-STUB-PERIOD-VERIFICATION-1]]（優先度：中・SPAC合併前・IPO前と
   見られる正当な非365日期間データ11銘柄の個別確認、10-K原本での裏取り
   未実施）
④ [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは各10年連続、LITE(9年)/CRM(7年)は
   新規発見の大規模クラスタ。10-K原本確認が未着手）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・④の10-K確認が概ね収束してから常設WARN項目化を検討）

追記（2026-08-02 [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]実装完了）:
~~① [[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]~~ ✅ 2026-08-02完了
   （コード`ee46018b2`・データ`11d75b2c0`。貸借対照表恒等式逆算
   〈total_assets − stockholders_equity〉によるtotal_liabilitiesバック
   フィルを実装。278件全件で完全一致（許容誤差なし）を確認、全105銘柄
   フローズン入力比較で対象278件以外に変化がないことを確認。
   report_consistency_check.py NG=0（WARN=68件、変化なし）、pytest 504
   passed/2 known failed（既知のMSFT/NVDA）。BACKLOG_DONE.md
   「2026-08-02（完了）」へ全文移動）。
これにより次セッションの筆頭候補を更新する（優先度順）：
① [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]（優先度：中・10-K原本での
   個別確認が未着手、他項目と独立に着手可能）
② [[SPAC-STUB-PERIOD-VERIFICATION-1]]（優先度：中・SPAC合併前・IPO前と
   見られる正当な非365日期間データ11銘柄の個別確認、10-K原本での裏取り
   未実施）
③ [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは各10年連続、LITE(9年)/CRM(7年)は
   新規発見の大規模クラスタ。10-K原本確認が未着手）
④ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑤ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑥ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・③の10-K確認が概ね収束してから常設WARN項目化を検討）

追記（2026-08-02 [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]根本原因調査完了）:
~~① [[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]~~ ✅ 解消（実害なし、
   当初の懸念は誤りだったと確認）。3段階目の決算期変更は存在せず、直近
   10-Kの2クラスタ同時出現はSEC開示規則（Regulation S-X Article 3-06等）
   による比較列表示の正常な挙動と確認。era別anchor不一致も対称探索設計
   により計算結果に無害と確認。BACKLOG_DONE.md「2026-08-02（完了）」へ
   全文移動。調査から派生した実害（8ヶ月移行期データ完全欠落）は
   [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]として独立登録（優先度：高）。
これにより次セッションの筆頭候補を更新する（優先度順）：
① [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]（優先度：高・新規。fetcher.pyの
   relevant_formsに10-KT・10-QTが含まれず、RCATの8ヶ月移行期データが
   annual_YYYY.jsonから完全欠落。対応方針確定にはまずTANUKI VALUATION
   計算経路への実害有無の確認が必要）
② [[SPAC-STUB-PERIOD-VERIFICATION-1]]（優先度：中・SPAC合併前・IPO前と
   見られる正当な非365日期間データ11銘柄の個別確認、10-K原本での裏取り
   未実施）
③ [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは各10年連続、LITE(9年)/CRM(7年)は
   新規発見の大規模クラスタ。10-K原本確認が未着手）
④ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑤ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑥ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・③の10-K確認が概ね収束してから常設WARN項目化を検討）

追記（2026-08-02 [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]実害確認調査完了）:
①の優先度を「高→中」に訂正（TANUKI VALUATIONは実害なし・STONKS SILOの
一時的実害はデータ蓄積により自然解消済みと確認。詳細はBACKLOG.md該当項目
参照）。副産物として[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]を新規
登録（優先度：中）。これにより次セッションの筆頭候補を更新する：
① [[SPAC-STUB-PERIOD-VERIFICATION-1]]（優先度：中・SPAC合併前・IPO前と
   見られる正当な非365日期間データ11銘柄の個別確認、10-K原本での裏取り
   未実施）
② [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは各10年連続、LITE(9年)/CRM(7年)は
   新規発見の大規模クラスタ。10-K原本確認が未着手）
③ [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]（優先度：中・現在進行形の実害は
   解消済み、将来同型の決算期変更を行う他銘柄が現れた場合の再発リスクとして
   監視対象。対応方針〈案1〜3〉未確定）
④ [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（優先度：中・新規。RCATの
   operating_cash_flow欠落、継続/非継続事業タグ分割が原因の疑い。現時点で
   直接的な計算実害は未確認だが将来のOCF黒字転換時にfcf_list/DCFへ影響
   するリスク）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・②の10-K確認が概ね収束してから常設WARN項目化を検討）

追記（2026-08-02 [[SPAC-STUB-PERIOD-VERIFICATION-1]]個別確認完了）:
~~① [[SPAC-STUB-PERIOD-VERIFICATION-1]]~~ ✅ 解消（実害なし、現状の処理は
   妥当）。11銘柄・12ティッカー年度すべてで追加対応不要と確認。SPAC系
   6銘柄（ASTS/IONQ/JOBY/RKLB/SOFI/SPIR）はBSがSPAC本体の自己データ、
   PL/CFは後年filingの正しい12ヶ月比較列と確認。VRT(2016)は記載理由を
   訂正（Emersonスピンオフではなく、SPAC〈GS Acquisition Holdings
   Corp〉自身の設立初年度スタブと判明）。RCAT(2012)はown-dataで充実、
   D&A「1日間」エントリはval=0のタグ付けミスで実害ゼロと確認。
   BACKLOG_DONE.md「2026-08-02（完了）」へ全文移動。
これにより次セッションの筆頭候補を更新する：
① [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]（優先度：
   低〜中・対象14銘柄49件。MO/SCCOは各10年連続、LITE(9年)/CRM(7年)は
   新規発見の大規模クラスタ。10-K原本確認が未着手）
② [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]（優先度：中・現在進行形の実害は
   解消済み、将来同型の決算期変更を行う他銘柄が現れた場合の再発リスクとして
   監視対象。対応方針〈案1〜3〉未確定）
③ [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（優先度：中・RCATの
   operating_cash_flow欠落、継続/非継続事業タグ分割が原因の疑い。現時点で
   直接的な計算実害は未確認だが将来のOCF黒字転換時にfcf_list/DCFへ影響
   するリスク）
④ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑤ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑥ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・①の10-K確認が概ね収束してから常設WARN項目化を検討）

追記（2026-08-02 [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]
個別調査完了）:
~~① [[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]~~ ✅ 一部
   解消（MO/PM/SCCOの3銘柄は①genuine定義差、確定・対応不要としてクローズ。
   PM/MOはExciseAndSalesTaxesタグが検出diffと完全一致〈物品税込み収益vs
   税抜きベースのgross profitという業界標準〉、SCCOはDepreciationDepletion
   AndAmortizationタグが検出diffと完全一致〈D&A別建て表示という鉱業界
   標準〉。BACKLOG_DONE.md「2026-08-02（完了）」へ全文移動）。残り11銘柄
   は[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高、CRM/JNJ/
   MRVL確定3件＋AMD/BSY/KO/LRCX/ONDS/RMBS要確認6件）・
   [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中、LITE1件）へ新規分離
   登録。
これにより次セッションの筆頭候補を更新する：
① [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高・新規。
   revenue/cost_of_revenue/gross_profitが異なるaccn・会計年度から独立
   採用される設計欠陥。CRM/JNJ/MRVLで確定、残り6銘柄は要個別確認）
② [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]（優先度：中・現在進行形の実害は
   解消済み、将来同型の決算期変更を行う他銘柄が現れた場合の再発リスクとして
   監視対象。対応方針〈案1〜3〉未確定）
③ [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（優先度：中・RCATの
   operating_cash_flow欠落、継続/非継続事業タグ分割が原因の疑い。現時点で
   直接的な計算実害は未確認だが将来のOCF黒字転換時にfcf_list/DCFへ影響
   するリスク）
④ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中・新規。LITEのcost_of_
   revenueがCOGS由来償却費タグを未合算、タグ拡張で解消可能）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い。10-K原本確認が未着手）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・①の6銘柄確認が概ね収束してから常設WARN項目化を検討）

追記（2026-08-02 セッション終了処理、次セッションでの着手順序を最終整理）:
**2026-08-01〜02セッション全体のサマリ**: gross_profit調査（[[LAYER3-
GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]）を発端に、[[PERIOD-LENGTH-
VALIDATION-GAP-1]]・[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]・[[SPAC-
SHELL-BS-ENTITY-MIXING-1]]段階1/2・[[TOTAL-LIABILITIES-FALLBACK-TAG-
DESIGN-FLAW-1]]・[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]
（一部）・[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]・[[SPAC-STUB-PERIOD-
VERIFICATION-1]]・[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]の
完了・クローズが連鎖的に波及した。新規登録は
[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]（後に統合クローズ）・
[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]・[[FETCHER-10KT-10QT-
FORM-EXCLUSION-1]]・[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]・
[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]・[[LITE-COGS-DA-TAG-
UNMERGED-1]]。詳細はBACKLOG_DONE.md「2026-08-01/02（完了）」参照。

**次セッションでの着手順序（2026-08-02時点、最終版）**:
① [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高・CRM/JNJ/
   MRVLで確定、残り6銘柄〈AMD/BSY/KO/LRCX/ONDS/RMBS〉の個別確認→横断的
   設計変更の検討へ）
② [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]（優先度：中・現在進行形の実害は
   解消済み、将来同型の決算期変更を行う他銘柄が現れた場合の再発リスクとして
   監視対象）
③ [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（優先度：中・RCATの
   operating_cash_flow欠落、継続/非継続事業タグ分割が原因の疑い）
④ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中・LITEのcost_of_revenue
   がCOGS由来償却費タグを未合算、タグ拡張で解消可能）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・①の6銘柄確認が概ね収束してから常設WARN項目化を検討）
⑧ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低・
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理
   自体は将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案b実装完了）:
①の一部（案b）を実装完了。`_align_cost_of_revenue_to_revenue_period()`を
新規追加し、revenue・cost_of_revenueが異なるaccnから独立採用され、かつ
`revenue − cost_of_revenue ≠ gross_profit`という数学的矛盾が現に存在する
年度についてのみ、revenueと同一accn・同一期間のcost_of_revenue候補で
矛盾が厳密に解消する場合に限り置換する設計（コード`b756021f6`＋安全性
修正`9616e8058`・データ`7c94c6f95`）。**実装時の検証で発見した重大な
副作用**（初回実装が矛盾のない年度＝GOOGL(2008)/HON(2008)/SCCO(2009/2010)
まで誤って書き換える巻き添え）を、gross_profitがNone〈導出前〉の年度を
比較不能として除外するゲート条件の追加で是正した。結果、対象はLRCX(2010)
の1件のみとなり、それ以外は全105銘柄フローズン入力比較で無変化と確認。
report_consistency_check.py NG=0（WARN=68件）、pytest 513 passed/2 known
failed。CRM(2013)・JNJ(2017)・MRVL(2017)・ONDS(2017)は案b単独では未解決
のまま残存（案aの対応が必要な可能性）。エントリ自体は全件解決していない
ためBACKLOG.mdに残置し、実装結果・残存部分を本文に明記した
（BACKLOG_DONE.mdへの完全移動はしない）。
これにより次セッションの筆頭候補を更新する:
① [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]（優先度：中・現在進行形の実害は
   解消済み、将来同型の決算期変更を行う他銘柄が現れた場合の再発リスクとして
   監視対象）
② [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（優先度：中・RCATの
   operating_cash_flow欠落、継続/非継続事業タグ分割が原因の疑い）
③ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中・LITEのcost_of_revenue
   がCOGS由来償却費タグを未合算、タグ拡張で解消可能）
④ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]残存分（優先度：中〜高・案a
   〈候補タグ拡張、AMD/KO/JNJ/MRVL等〉・案c〈2タグ合算、RMBS〉・案d
   〈BSY個別対応〉、いずれもゲート条件込みの再設計が必要）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・④の確認が概ね収束してから常設WARN項目化を検討）
⑧ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低・
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理
   自体は将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]対応方針確定）:
①の対応方針を確定。案1（relevant_forms追加+バケツ再設計）は見送り
（RCAT own-data 10-K・10-KTが両方ともSEC自身によりfy=2024とタグ付け
されており真正のバケツキー衝突が発生すること、複数消費者の改修が必要な
ことを確認しコストが当初想定より高いと判明）。案3（`report_consistency_
check.py`への新規WARN追加のみ）を採用方針として確定、実装は未着手。
トリガー条件（RCAT再変更または他銘柄での実害確認）発生時に案1を再検討。
副産物として[[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]を新規登録
（優先度：低〜中、fetcher.py側とは独立の別タスク）。
これにより次セッションの筆頭候補を更新する:
① [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（優先度：中・RCATの
   operating_cash_flow欠落、継続/非継続事業タグ分割が原因の疑い）
② [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中・LITEのcost_of_revenue
   がCOGS由来償却費タグを未合算、タグ拡張で解消可能）
③ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]残存分（優先度：中〜高・案a
   〈候補タグ拡張、AMD/KO/JNJ/MRVL等〉・案c〈2タグ合算、RMBS〉・案d
   〈BSY個別対応〉、いずれもゲート条件込みの再設計が必要）
④ [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]案3実装（優先度：中・
   report_consistency_check.pyへの新規WARN追加、既存WARN-24との役割
   分担を明記した設計が確定済み）
⑤ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中・新規。
   _calc_yoy_change()への期間長妥当性チェック追加、緊急性なし）
⑥ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い）
⑦ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑧ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・③の確認が概ね収束してから常設WARN項目化を検討）
⑨ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低・
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理
   自体は将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]案③実装完了）:
~~④ [[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]案3実装~~ ✅ 2026-08-02完了
   （コード`1fd44fc0a`。`report_consistency_check.py`にCHECK-28
   〈WARN-28〉を新規追加、company_facts.json上のform=10-KT/10-QTのaccnが
   `accn_to_reportdate`に未登録の場合を検知〈検知のみ、自動修正なし〉。
   全105銘柄実行でRCATにWARN-28が2件発火（10-KT accn
   `0001641172-25-001892`・**新規発見**の10-QT accn
   `0001554795-19-000269`〈2019年、RCAT第1回目の決算期変更に伴う移行期
   四半期報告書〉）、他104銘柄で誤検知なし、WARN数68→70件（+2）、NG=0
   維持。pytest 519 passed/2 known failed。データファイルは無変更（検知
   のみ）。BACKLOG_DONE.md「2026-08-02（完了）」へ全文移動）。
これにより次セッションの筆頭候補を更新する:
① [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（優先度：中・RCATの
   operating_cash_flow欠落、継続/非継続事業タグ分割が原因の疑い）
② [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中・LITEのcost_of_revenue
   がCOGS由来償却費タグを未合算、タグ拡張で解消可能）
③ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]残存分（優先度：中〜高・案a
   〈候補タグ拡張、AMD/KO/JNJ/MRVL等〉・案c〈2タグ合算、RMBS〉・案d
   〈BSY個別対応〉、いずれもゲート条件込みの再設計が必要）
④ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中・
   _calc_yoy_change()への期間長妥当性チェック追加、緊急性なし）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・③の確認が概ね収束してから常設WARN項目化を検討）
⑧ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低・
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理
   自体は将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]根本原因調査
完了）:
~~① [[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]~~ ✅ 原因確定・
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]へスコープ拡大・
   統合しクローズ（BACKLOG_DONE.md「2026-08-02（完了）」へ移動）。RCATの
   OCF欠落は標準タグ`NetCashProvidedByUsedInOperatingActivities`が
   FY2024フィリングから継続/非継続事業の分割タグに置き換わったことが原因
   と確定。105銘柄横断スキャンで**25銘柄該当**（AAPL/MSFT/TSLA/XOM/CAT/
   ABBV等の主力銘柄を含む）する候補タグ設計欠陥と判明し、`operating_
   cash_flow`はTANUKI VALUATIONのDCF/FCF計算に直結するため実害の可能性が
   高いと判断、新規タスクへ統合（優先度：高）。
これにより次セッションの筆頭候補を更新する:
① [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：高・
   新規。25銘柄該当、まずTANUKI VALUATION計算経路への実害有無の確認が
   必要）
② [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中・LITEのcost_of_revenue
   がCOGS由来償却費タグを未合算、タグ拡張で解消可能）
③ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]残存分（優先度：中〜高・案a
   〈候補タグ拡張、AMD/KO/JNJ/MRVL等〉・案c〈2タグ合算、RMBS〉・案d
   〈BSY個別対応〉、いずれもゲート条件込みの再設計が必要）
④ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中・
   _calc_yoy_change()への期間長妥当性チェック追加、緊急性なし）
⑤ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い）
⑥ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑦ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・③の確認が概ね収束してから常設WARN項目化を検討）
⑧ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低・
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理
   自体は将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]
実害確認調査完了）:
①の優先度を「高→中」に訂正（25銘柄中24銘柄〈AAPL/MSFT/TSLA/XOM/CAT/ABBV
等を含む〉は該当年度がすべて現在の直近5年窓〈2021-2026年〉の外にあり実害
なしと確定。RCAT単独の実害は[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]として新規
独立登録〈優先度：高〉。RCATの`get_fcf_5yr_avg()`が実質2021-2023年の3年
平均になっており、真により大きな悪化を示す2024/2025年〈特に-$89.1M〉が
欠落していることを確認。前回〈[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]〉の
「成長率決定には影響しない」という限定的確認だけでの「実害なし」結論を
訂正）。
これにより次セッションの筆頭候補を更新する:
① [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：高・新規。RCATのDCF FCF
   ベース値計算に構造的な実害。[[OPERATING-CASH-FLOW-CONTINUING-
   DISCONTINUED-GAP-1]]のRCAT分〈パターンB〉解決が前提）
② [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中・LITEのcost_of_revenue
   がCOGS由来償却費タグを未合算、タグ拡張で解消可能）
③ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]残存分（優先度：中〜高・案a
   〈候補タグ拡張、AMD/KO/JNJ/MRVL等〉・案c〈2タグ合算、RMBS〉・案d
   〈BSY個別対応〉、いずれもゲート条件込みの再設計が必要）
④ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中・
   RCAT以外の24銘柄は現在進行形の実害なし、過去年度のデータ品質向上として
   引き続き価値あり）
⑤ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中・
   _calc_yoy_change()への期間長妥当性チェック追加、緊急性なし）
⑥ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低・HON(2009)
   単独、既知パターンと異なる原因の疑い）
⑦ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中・TANUKI VALUATION通常の
   定期更新サイクルで自然解消見込み。次回定期更新後に反映確認・クローズ）
⑧ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中・③の確認が概ね収束してから常設WARN項目化を検討）
⑨ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低・
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理
   自体は将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 セッション終了処理、優先度順に並び順を最終整理）:
**2026-08-01〜02セッション全体のサマリ（gross_profit調査発端から2日間に
わたり波及した一連のデータ品質是正作業）**: [[PERIOD-LENGTH-VALIDATION-
GAP-1]]・[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]・[[SPAC-STUB-PERIOD-
FIELD-SPLIT-1]]・[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階1/2・[[TOTAL-
LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]・[[GROSSPROFIT-COGS-ANNUAL-
DEFINITION-GAP-MO-PM-SCCO-1]]（一部）・[[RCAT-TRIPLE-FISCAL-CHANGE-
SUSPECTED-1]]・[[SPAC-STUB-PERIOD-VERIFICATION-1]]・[[STONKS-SILO-FETCHER-
GROSSPROFIT-BACKFILL-DUP-1]]・[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]・
[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]（[[OPERATING-CASH-FLOW-
CONTINUING-DISCONTINUED-GAP-1]]へ統合）・[[PL-FIELD-CROSS-ACCN-PERIOD-
MISMATCH-1]]案bが完了。次回最優先タスクは
[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（現在進行形のDCF計算実害）。
**次セッションでの着手順序（2026-08-02時点、最終版）**:
① [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：高。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分
   〈パターンB〉解決が前提）
② [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCATパターンBの対応がRCAT-FCF-5YR-AVG-ACTUAL-3YR-1
   の前提）
③ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
④ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑤ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑥ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑦ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑧ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑨ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理は
   将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]パターンB実装前
シミュレーション完了）:
①の優先度を「高→低」に訂正。RCATの本番FCF計算がreader.py::
get_fcf_5yr_avg()（年次ファイルベース）を使わず、data_fetcher.py::
_select_fcf_source()がTTM系列（common/sec_data/ttm/RCAT_ttm_series.json）
を優先採用する設計と判明。TTM系列は四半期10-Qの集計であり年次10-Kの
継続/非継続事業分割タグ問題の影響を受けず既に完全な値を持つため、
[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分（パターン
B）を年次パーサー側のみに実装してもRCATのfcf_base_used・DCF・
tanuki_score・Classificationは一切変化しない（ΔIV=$0と試算確認）。
副次的に発見したTTM系列生成ロジック側の継続/非継続タグ扱い未検証の問題を
[[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]として新規登録
（優先度：高。RCATの本番IV計算経路に直結するため、こちらを優先）。
これにより次セッションでの着手順序を更新する:
① [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：高・
   新規。TTM系列生成ロジックの継続/非継続タグ扱いが未検証、RCATの本番
   IV計算に直結する可能性）
② [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
③ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
④ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：高→低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される。単独での緊急対応は不要）
⑤ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑥ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑦ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑧ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑨ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑩ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理は
   将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]
根本原因調査完了）:
①の優先度を「高→中」に訂正。当初の懸念（継続/非継続タグの取り扱いミス）
ではなく、`ttm_calculator.py::calc_ttm_series()`が採用四半期の日付連続性
を検証しない一般的な設計欠陥が根本原因と判明。RCATでは標準タグの空白
（継続/非継続分割開示と決算期変更が重なった約11ヶ月間）により、2023年
7〜10月・10月〜2024年1月の四半期が`ttm_end=2025-03-31`・`2026-03-31`の
両方に重複使用され、現在のfcf_5yr_avg（-40,185,008.5）・fcf_2yr_avg
（-50,540,837.0）が正しい値（試算：約-53,985,212・約-78,141,244）より
34〜55%過小評価と確定。IVへの影響は現時点でΔIV=$0（revenue floor＋EPS
ベース推定オーバーライドが吸収。将来業績改善時に顕在化しうる潜在リスクの
留保付き）。他銘柄（HON/AVAV/TER）への現時点の実害なしと確認。根本原因
（ticker非依存の一般的欠陥）を[[TTM-CALC-QUARTER-CONTIGUITY-
UNCHECKED-1]]として新規登録（優先度：中〜高）。
これにより次セッションでの着手順序を更新する:
① [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高・新規。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   まず105銘柄横断スキャンから着手）
② [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
③ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：
   高→中。現時点のIV実害はゼロ、恒久対応は①側で行う）
④ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑤ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：高→低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される。単独での緊急対応は不要）
⑥ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑦ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑧ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑨ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑩ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑪ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、fetcher.py側の重複ロジックのコード整理は
   将来のcommon/sec_data統合フェーズ1到達時に検討）

追記（2026-08-02 セッション終了処理、優先度順に並び順を最終整理）:
本セッション（RCAT-FCF-5YR-AVG-ACTUAL-3YR-1パターンB実装前シミュレーション
→RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1根本原因調査、いずれも
読み取り専用の調査・BACKLOG登録のみ、実装なし）の結果を反映し、
**次セッションでの着手順序（2026-08-02時点、最終版）**を確定する:
① [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
② [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は①側で行う）
③ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
④ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑤ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑥ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑦ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑧ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑨ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑩ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑪ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 common/sec_data/抽出アーキテクチャの俯瞰的脆弱性分析
完了）:
本セッションで発見した5バグが共通の設計的欠陥（候補プールから新しさ
基準のみで値を確定し、他フィールド・他期間・会計恒等式と照合しない）に
帰着すると判明。105銘柄への機械的予備スキャンでTA≠TL+SE違反156件
（50銘柄）・GP≠Revenue−COGS違反43件（9銘柄、GOOGL(2012/2013)は新規
発見）・OI>GP違反22件（LMT単独、新規発見）・NI≠EPS×Shares違反67件
（31銘柄、COHRに単位スケールバグの疑い）を確認（詳細調査は未実施、件数
把握のみ）。[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]・
[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]を優先度：高で新規登録。
これにより次セッションでの着手順序を更新する:
① [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高・新規。
   予備スキャンで見つかった4種の違反〈TA≠TL+SE 156件・GP≠Revenue−COGS
   43件・OI>GP 22件・NI≠EPS×Shares 67件〉の分類調査〈bug/genuine差/
   対応不要〉が未着手）
② [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]（優先度：高・新規。
   横断的な会計恒等式検証レイヤーの新設提案。①の分類調査結果を踏まえて
   から実装に進むのが望ましい）
③ [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
④ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は③側で行う）
⑤ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑥ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑦ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑧ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑨ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑩ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑪ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑫ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑬ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]の
TA=TL+SE違反156件・分類調査完了）:
持続性区分（単年度28銘柄・2年度5銘柄・3年度以上17銘柄）を確定。8銘柄の
サンプル確認で6銘柄（FCX/BROS/RKLB/GTLB/COHR/ONDS）が①genuine（NCI・
一時的持分の未捕捉、設計スコープ外）と確定し、156件の過半数が①に該当する
見込みと判明。HEI・LRCXの2銘柄はNCI等を含めても解消しない未特定の不整合
（同一filing・同一accn内での恒等式不成立）と判明し
[[HEI-LRCX-TA-TLSE-UNEXPLAINED-RESIDUAL-1]]として新規登録（優先度：
中〜高）。恒等式検証の対応方針を「TA==TL+SE+NCI+一時的持分」の拡張形で
確定（許容誤差を広げるだけの対応はHEI・LRCX型の真の異常を隠蔽するため
不採用）。GP≠Revenue−COGS・OI>GP・NI≠EPS×Shares の3種の分類調査は未着手。
これにより次セッションでの着手順序を更新する:
① [[HEI-LRCX-TA-TLSE-UNEXPLAINED-RESIDUAL-1]]（優先度：中〜高・新規。
   NCI等を含めても解消しない同一filing内の恒等式不成立、原因未特定）
② [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]（優先度：高。
   横断的な会計恒等式検証レイヤーの新設提案。TA=TL+SE違反の分類調査結果
   〈①の設計方針〉を踏まえて実装可能な段階）
③ [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高。
   TA=TL+SE違反の分類調査は完了、残る3種〈GP≠Revenue−COGS 43件・
   OI>GP 22件・NI≠EPS×Shares 67件〉の分類調査が未着手）
④ [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
⑤ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は④側で行う）
⑥ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑦ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑧ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑨ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑩ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑪ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑫ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑬ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑭ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 [[HEI-LRCX-TA-TLSE-UNEXPLAINED-RESIDUAL-1]]根本原因調査
完了）:
①の判定は誤りと判明。対象accn・end_dateの全XBRLタグを機械的に網羅する
手法で再調査した結果、HEI・LRCXともNCI・一時的持分タグ（前回見落とし
分）を含めればTA=TL+SEが完全一致することを確認。「誤登録・訂正のうえ
クローズ（原因は①genuine、探索範囲不足による誤判定だった）」として
BACKLOG_DONE.mdへ移動。追加でTSLA・XOMもサンプル確認し完全一致を確認、
累計10銘柄が例外なく①genuineに分類されたことで、TA=TL+SE違反156件は
ほぼ全件が①genuineへ収束する見込みが高いと判明（詳細は
[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]参照）。
これにより次セッションでの着手順序を更新する:
① [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]（優先度：高。
   横断的な会計恒等式検証レイヤーの新設提案。TA=TL+SE違反はほぼ全件
   ①genuineへ収束する見込みが確認され、「TA==TL+SE+NCI+一時的持分」の
   拡張形での実装可否判断が可能な段階）
② [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高。
   TA=TL+SE違反の分類調査は完了、残る3種〈GP≠Revenue−COGS 43件・
   OI>GP 22件・NI≠EPS×Shares 67件〉の分類調査が未着手）
③ [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
④ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は③側で行う）
⑤ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑥ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑦ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑧ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑨ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑩ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑪ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑫ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑬ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]実装前
シミュレーション完了）:
無条件でNCI・一時的持分を加算する設計は、既存の正しい1,085件のうち33件
（VZ最大-$56.6B・WMT・KO・AVGO・LLY・AMD・ASTS・BROS・CAKE）で新規誤検知
を生む重大な危険があると実証。「TA=TL+SEが不一致の場合のみ拡張形を試す
OR条件フォールバック方式」・許可リスト方式のタグ選定に設計を確定し、
156件中133件（85.3%）が解消見込みと判明。残る23件を
[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]として新規登録（優先度：中）。
これにより次セッションでの着手順序を更新する:
① [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]（優先度：高。設計
   確定済み、実装に着手可能な段階。OR条件フォールバック方式・許可リスト
   方式のタグ選定・検知専用ログ・CHECK-29実装方法まで確定済み）
② [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高。
   TA=TL+SE違反の分類調査は完了、残る3種〈GP≠Revenue−COGS 43件・
   OI>GP 22件・NI≠EPS×Shares 67件〉の分類調査が未着手）
③ [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
④ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は③側で行う）
⑤ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑥ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑦ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑧ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑨ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中・新規。着手条件:
   ①CHECK29本体の実装後）
⑩ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑪ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑫ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑬ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑭ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 [[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]実装
完了）:
会計恒等式TA=TL+SE(+NCI+一時的持分)検証をOR条件フォールバック方式・
許可リスト方式のタグ選定でparser.py（`_check_bs_identity_violations()`・
`bs_identity_violations_log.json`新設）・report_consistency_check.py
（CHECK-29/WARN-29）に実装（機能コミット`bd91000f0`）。全105銘柄で
オフライン再パースし156件中133件が拡張形で解消・23件が未解消となる
ことを事前シミュレーションと完全一致で確認。既存1,085件（正常ケース）
への新規誤検知なし（VZ/WMT/KO/AVGO/LLY/AMD/ASTS/BROS/CAKE個別確認済み）、
annual_YYYY.json等の既存データ値は無変更、pytest 497 passed/2 known
failed、WARN 70→83件（純増13件、全てWARN-29）を確認。同エントリを
「実装完了」としてBACKLOG_DONE.mdへ移動し、
[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（着手条件充足、実測23件を
WARN-29で確認）を更新、[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-
MISSING-1]]にTA=TL+SE分の対応完了を反映（残る3種の分類調査が未着手の
ため本体はクローズせず存置）。
これにより次セッションでの着手順序を更新する:
① [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高。
   TA=TL+SE分は対応完了、残る3種〈GP≠Revenue−COGS 43件・OI>GP 22件・
   NI≠EPS×Shares 67件〉の分類調査が未着手）
② [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
③ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は②側で行う）
④ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑤ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。着手条件充足済み。
   COHR・ONDS等の個別調査から着手可能）
⑥ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑦ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑧ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑨ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑩ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑪ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑫ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑬ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]個別調査完了
〈COHR・HEI・ONDS優先〉）:
3件とも①genuineと確定（②タグ選定バグに分類されるものはなし）。
COHR(2022/2023)はCHECK29の「本人データ限定」照合という設計方針そのもの
が原因で検知不可能な構造的限界と判明し
[[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]として別スコープで新規
登録（優先度：中）。HEI(2009-2013)はTemporaryEquityRedemptionValueを
CarryingAmount系タグ不在時のフォールバックとして許可リストに追加すれば
対応可能と判明。ONDS(2023)はCHECK29自体のSUPERSEDESルール不備
（RedeemableNoncontrollingInterestEquityCarryingAmount存在時に
...PreferredCarryingAmountを除外するルールの欠如、自己申告）と判明、
既存ルールと同型の拡張で対応可能。残る20件
（PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW）は未着手のまま。
これにより次セッションでの着手順序を更新する:
① [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高。
   TA=TL+SE分は対応完了、残る3種〈GP≠Revenue−COGS 43件・OI>GP 22件・
   NI≠EPS×Shares 67件〉の分類調査が未着手）
② [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
③ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は②側で行う）
④ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑤ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。COHR・HEI・
   ONDS（8件）は原因確定済み・許可リスト拡張の実装待ち。残る15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉は個別調査未着手）
⑥ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中・新規。
   CHECK29の本人データ限定照合の設計方針拡張検討、該当は現時点でCOHR
   2件のみ）
⑦ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑧ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑨ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑩ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑪ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑫ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑬ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑭ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]のHEI・ONDS型
実装完了）:
CHECK29の許可リストに`TemporaryEquityRedemptionValue`（CarryingAmount系
タグ不在時のフォールバック限定）・`RedeemableNoncontrollingInterest
EquityCarryingAmount`のSUPERSEDESルールを追加（機能コミット
`a910afef2`）。全105銘柄で再検証し156件中133件→139件が解消（HEI×5・
ONDS×1）、副次的にFCX(2013)も改善。他99銘柄・既存133件・COHR型2件・
残り15件のresolved状態は維持を確認。WARN 83→81件（-2）。
これにより次セッションでの着手順序を更新する:
① [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高。
   TA=TL+SE分は対応完了、残る3種〈GP≠Revenue−COGS 43件・OI>GP 22件・
   NI≠EPS×Shares 67件〉の分類調査が未着手）
② [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
③ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は②側で行う）
④ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑤ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中。CHECK29の
   本人データ限定照合の設計方針拡張検討、該当は現時点でCOHR2件のみ）
⑥ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残る15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉が個別調査未着手。
   HEI・ONDSは実装完了・COHRは⑤で別扱い）
⑦ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑧ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑨ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑩ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑪ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑫ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑬ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑭ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）

追記（2026-08-02 セッション終了処理、優先度順に並び順を最終整理）:
本セッション後半（[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]
実装〈会計恒等式TA=TL+SE検証レイヤー新設、機能コミット`bd91000f0`〉→
HEI・ONDS型許可リスト拡張〈機能コミット`a910afef2`〉、いずれも全105銘柄
検証・pytest/WARN数確認済み）の結果を反映し、
**次セッションでの着手順序（2026-08-02時点、最終版）**を確定する:
① [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]（優先度：高。
   TA=TL+SE以外の残る3種の分類調査が未着手: GP≠Revenue−COGS新規2件・
   OI>GP〈LMT〉・NI≠EPS×Shares〈COHR単位スケール疑い〉）
② [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
③ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中。CHECK29の
   own-accn限定照合という設計方針そのものの緩和検討、該当は現時点で
   COHR2件のみ）
④ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残り15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉が個別調査未着手。
   HEI・ONDSは実装完了・COHRは③で別扱い）
⑤ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は②側で行う）
⑥ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑦ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑧ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑨ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑩ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑪ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑫ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑬ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑭ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）
⑮ [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]（優先度：低。
   bs_identity_violations_log.jsonのキー順序非決定性、実害なし）

追記（2026-08-02 [[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]の
残る3種の分類調査が完了し同エントリをクローズしたことを反映し、
次セッションでの着手順序を更新する:
**次セッションでの着手順序（2026-08-02時点、最終版）**:
① [[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]（優先度：中〜高。GOOGL
   (2012/2013)のgross_profitがfact_overrides.json適用順序バグで誤り、
   対応方針未定・他フィールド・他銘柄への波及有無も未調査）
② [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
③ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
④ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中。CHECK29の
   own-accn限定照合という設計方針そのものの緩和検討、該当は現時点で
   COHR2件のみ）
⑤ [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]（優先度：中。COHR自身の
   FY2011 10-Kのshares_diluted単位スケール申告誤り、汎用の桁違い
   検知チェック新設も検討。105銘柄横断スキャン未着手）
⑥ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残り15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉が個別調査未着手。
   HEI・ONDSは実装完了・COHRは④で別扱い）
⑦ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は②側で行う）
⑧ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑨ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑩ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑪ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑫ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑬ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑭ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑮ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）
⑯ [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]（優先度：低。
   bs_identity_violations_log.jsonのキー順序非決定性、実害なし）

追記（2026-08-02 [[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]実装完了を反映し、
次セッションでの着手順序を更新する）:
**次セッションでの着手順序（2026-08-02時点、最終版）**:
① [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
② [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
③ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中。CHECK29の
   own-accn限定照合という設計方針そのものの緩和検討、該当は現時点で
   COHR2件のみ）
④ [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]（優先度：中。COHR自身の
   FY2011 10-Kのshares_diluted単位スケール申告誤り、汎用の桁違い
   検知チェック新設も検討。105銘柄横断スキャン未着手）
⑤ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残り15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉が個別調査未着手。
   HEI・ONDSは実装完了・COHRは③で別扱い）
⑥ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は①側で行う）
⑦ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑧ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑨ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑩ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑪ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑫ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑬ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑭ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）
⑮ [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]（優先度：低。
   bs_identity_violations_log.jsonのキー順序非決定性、実害なし）

追記（2026-08-02 [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]の対応方針確定・
[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]・[[XBRL-UNIT-SCALE-MISMATCH-
DETECTION-1]]新規登録を反映し、次セッションでの着手順序を更新する）:
**次セッションでの着手順序（2026-08-02時点、最終版）**:
① [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
② [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
③ [[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]（優先度：中〜高。本人データ
   不在時に複数比較年度再掲が競合すると最も古いfilingが勝つ未文書化
   tie-break欠陥、COHR(2010)で実証。105銘柄全体での該当範囲・実装前
   全母集団シミュレーションが未着手）
④ [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]（優先度：中。対応方針確定済み
   〈fact_overrides.json個別上書き、値も確定〉、実装のみ残存）
⑤ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中。CHECK29の
   own-accn限定照合という設計方針そのものの緩和検討、該当は現時点で
   COHR2件のみ）
⑥ [[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]（優先度：中。同一タグ・同一
   期間の値が複数filing間で10のべき乗単位で乖離する場合の汎用検知チェック
   新設提案、105銘柄試験適用で18銘柄・126件を検出済み・個別トリアージ未着手）
⑦ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残り15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉が個別調査未着手。
   HEI・ONDSは実装完了・COHRは⑤で別扱い）
⑧ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は①側で行う）
⑨ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑩ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑪ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑫ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑬ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑭ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑮ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑯ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）
⑰ [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]（優先度：低。
   bs_identity_violations_log.jsonのキー順序非決定性、実害なし）

追記（2026-08-02 [[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]の全母集団
シミュレーション結果を反映し、次セッションでの着手順序を更新する。
tie-break条件の広範な見直しは不採用と確定・[[XBRL-UNIT-SCALE-
MISMATCH-DETECTION-1]]へガード条件付き介入として統合したため、
③の位置から除去し繰り上げる）:
**次セッションでの着手順序（2026-08-02時点、最終版）**:
① [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
② [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
③ [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]（優先度：中。対応方針確定済み
   〈fact_overrides.json個別上書き、値も確定〉、実装のみ残存）
④ [[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]（優先度：中。実装方式確定済み
   〈同符号かつ比が10のべき乗値のガード条件でtie-break新filing優先へ
   切り替え、対象は当面COHR2件〉。実装前に(a)既存の恒等式ベース安全網
   との相互作用再検証、(b)ガード適用後の全母集団再シミュレーションが
   必須）
⑤ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中。CHECK29の
   own-accn限定照合という設計方針そのものの緩和検討、該当は現時点で
   COHR2件のみ）
⑥ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残り15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉が個別調査未着手。
   HEI・ONDSは実装完了・COHRは⑤で別扱い）
⑦ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は①側で行う）
⑧ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑨ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑩ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑪ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑫ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑬ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑭ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑮ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）
⑯ [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]（優先度：低。
   bs_identity_violations_log.jsonのキー順序非決定性、実害なし）

追記（2026-08-02 セッション終了処理、次セッションでの着手順序を最終整理）:
[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]（fact_overrides.json個別対応）
と[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]（tie-break恒久対応）は
対象・実装タイミングが密結合のため①に統合表示する。
**次セッションでの着手順序（2026-08-02時点、セッション終了時最終版）**:
① [[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]（優先度：中。実装前最終確認
   完了・fact_overrides.json個別上書き〈2009-2011年度、値も確定済み〉の
   実装のみ残存。[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]のtie-break
   変更部分は、実装しても2010年度1件しか解決せずfact_overrides側で
   重複解決される・現時点で他に該当実ケースがゼロと確定したため、
   当面見送り〈将来の予防的対応として保留〉）
② [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]（優先度：中〜高。
   calc_ttm_series()の日付連続性チェック欠如、ticker非依存の一般的欠陥。
   105銘柄横断スキャンが未着手）
③ [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]（優先度：中。CHECK29の
   own-accn限定照合という設計方針そのものの緩和検討、該当は現時点で
   COHR2件のみ）
④ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残り15件
   〈PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW〉が個別調査未着手。
   HEI・ONDSは実装完了・COHRは③で別扱い）
⑤ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   現時点のIV実害はゼロ、恒久対応は②側で行う）
⑥ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑦ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑧ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑨ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑩ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑪ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑫ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑬ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑭ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）
⑮ [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]（優先度：低。
   bs_identity_violations_log.jsonのキー順序非決定性、実害なし）

追記（2026-08-02 セッション終了処理、次セッションでの着手順序を最終整理）:
**次セッションでの着手順序（2026-08-03時点、最終版）**:
① [[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]（優先度：中。layer3_builder.pyと
   parser.pyが同期しない構造的脆弱性は残存するが、既知7件の修正への
   現在進行形の実害はゼロと確定済み）
~~② [[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]~~ ✅ 2026-08-03完了
   （2段階ガード〈own-accnのみで厳密一致する場合はフォールバック自体を
   スキップするベースゲート＋重複値ガード〉を実装。COHR(2022/2023)・
   CRWV(2024)・VRT(2018)の4件を解消、実装過程で発見した回帰5件
   〈SOUN2021・PM2010/2011・TSLA2020/2021・HEI2014・FCX2015〉は
   ガードにより再発防止済みで検証済み。詳細はBACKLOG_DONE.md参照）
~~② [[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]]~~ ✅ 2026-08-30完了
   （CRM型・VRT型を別々に修正、105銘柄シミュレーションで副次発見の
   AVAV(2020, rpo)も解消。詳細はBACKLOG_DONE.md参照）
③ [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]（優先度：中。残り13件の
   個別調査完了・①genuine2件〈BKNG2011/2012〉・②許可リスト拡張可能
   2件〈ASTS2020・RDW2020〉・③要さらなる確認7件〈PLTR/CART×3/V/CELH/
   ASTS2019〉に分類済み。CRM/VRT(2017)は②〈PARSER-STOCKHOLDERS-
   EQUITY-CROSS-YEAR-MISSELECT-1〉へ分離。次のアクションは②の許可
   リスト拡張実装、または③の個別方針確定）
④ [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]（優先度：中。
   [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]実装完了によりRCAT分の
   根本原因は解消済みの可能性が高いが、本エントリ自体のクローズ判断は
   別途確認が必要なため未着手のまま残置）
⑤ [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]（優先度：中。
   24銘柄分は実害なし・RCAT分〈パターンB〉も年次パーサーのみでは
   IVへの実効果なしと判明）
⑥ [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]（優先度：中〜高。残存: 案a
   〈候補タグ拡張再設計〉・案c〈2タグ合算再設計〉・CRM/JNJ/MRVL/ONDS型の
   未解決分）
⑦ [[LITE-COGS-DA-TAG-UNMERGED-1]]（優先度：低〜中）
⑧ [[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]（優先度：低〜中）
⑨ [[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]（優先度：低。着手条件:
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]のRCAT分実装と
   同時に副次的効果として解消される見込み）
⑩ [[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]（優先度：低）
⑪ [[ELF-ROE10YR-RECALC-PENDING-1]]（優先度：中。TANUKI VALUATION定期更新
   で自然解消見込み）
⑫ [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]（優先度：
   低〜中）
⑬ [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]（優先度：低。
   クローズ済み〈実害解消済み〉、デッドコード整理は将来検討）
⑭ [[BS-IDENTITY-LOG-NONDETERMINISTIC-KEY-ORDER-1]]（優先度：低。
   bs_identity_violations_log.jsonのキー順序非決定性、実害なし）

**次セッションでの着手順序（2026-08-05時点、最終版）**:
① [[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]（優先度：高。Stage 1
   〈taxonomy属性①〜⑧非該当26銘柄・372エントリ〉は実装完了・push済み
   〈機能コミット`7c15b2a75`、詳細はBACKLOG_DONE.md参照〉。次はStage 2
   〈属性該当58銘柄のうちBACKLOG_DONE.mdで解消済み確認済みの年度、
   `fixed_by: manual_verification`で登録〉の対象リスト生成に着手する）
② 以下、2026-08-03時点リストから変更なし（上記①〜⑭を参照）:
   [[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]・[[PARSER-STOCKHOLDERS-EQUITY-
   CROSS-YEAR-MISSELECT-1]]・[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]・
   [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]・
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]・
   [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]・
   [[LITE-COGS-DA-TAG-UNMERGED-1]]・[[STONKS-SILO-FP-LABEL-PERIOD-
   VALIDATION-1]]・[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]・[[HON-GROSSPROFIT-
   2009-RESIDUAL-DISCREPANCY-1]]・[[ELF-ROE10YR-RECALC-PENDING-1]]・
   [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]・
   [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]・[[BS-IDENTITY-
   LOG-NONDETERMINISTIC-KEY-ORDER-1]]

**次セッションでの着手順序（2026-08-06時点、最終版）**:
上記①[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]はStage 2〜3b（Stage
2・Stage 3準備・Stage 3a・RDW/ASTS BS恒等式修正・Stage 3bまで全て
実装完了、詳細はBACKLOG_DONE.md「2026-08-05（完了）」参照）につき
本リストから除外。Layer3統一方針の確定（2026-08-06投資調査）を受け、
以下を最優先とする:
~~1. `SEC_EDGAR_LAYER_DESIGN.md`フェーズD Step1: アクセサのラッパー化~~
   ✅ 2026-08-06完了（`layer3_builder.py::get_quarterly_series()`/
   `get_latest_quarterly()`を新設。`get_field_entries()`をそのまま
   呼ぶ薄いラッパー、シグネチャは`(store, field_name)`で既存
   `get_field_entries()`に統一。10フィールドのPascalCase→snake_case
   対応表を確定〈Revenue→revenue・OperatingIncome→operating_income・
   GrossProfit→gross_profit・RD→research_and_development・
   NetIncome→net_income・OCF→operating_cash_flow・
   CapEx→capital_expenditure・SM→selling_and_marketing・
   SBC→stock_based_compensation・SharesDiluted→shares_diluted〉。
   `get_lt_debt_from_normalized()`相当のLayer3版は見送り（BUG-
   NETDEBT-3のdata/annual側フォールバックであり、フェーズD Step2で
   TANUKI VALUATION本体を切り替える際に改めて要否判断する）。実データ
   （AAPL/CPRT/PEP/RCAT/CEG）でnormalized/経由の値と突合、AAPLは
   10/10フィールド完全一致・他4銘柄もselling_and_marketing以外は
   完全一致（乖離2件はいずれも既知課題`[[SCHEMA-NORMALIZED-ISSUES-1]]`
   ②・`[[LAYER3-GA-STANDALONE-TAG-UNMAPPED-1]]`由来と特定、新規bugでは
   ない）。既存消費者は無変更、pytest 505 passed/2 known failed（既知の
   `[[TEST-STALE-IV-1]]`のみ、新規8件追加分すべてpass）。詳細は
   BACKLOG_DONE.md参照
~~2. フェーズD Step2-1: TANUKI VALUATION本体切替（reader.py・
   pipeline.py）~~ ✅ 2026-08-06完了（事前バグ修正2件
   〈`[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]`・`[[LAYER3-ANNUAL-
   MISCLASSIFICATION-BBAI-1]]`〉→pipeline.py 6箇所をget_field_entries()
   経由に切替→100銘柄全数回帰確認、の順で実施。`get_lt_debt_v2`
   （`get_long_term_debt_latest()`）新規実装、Layer3優先方式を採用
   （RCAT/SPIR/CPRTのSEC EDGAR照合結果に基づく）。全数回帰で
   `roic_wacc_ratio`/`moat_roic`が4銘柄（COHR/LLY/JNJ/KLAC）で値→
   Noneに変化したが、SM/SGA概念混同問題の既知の帰結と特定し、
   ユーザー判断で現状維持を採用（`[[LAYER3-ROIC-WACC-NONE-
   4TICKERS-1]]`参照）。pytest 505 passed/2 known failed（既知）、
   report_consistency_check.py NG=0・WARN=78件（既存と不変）。詳細は
   BACKLOG_DONE.md参照
3. **次はフェーズD Step2-2**: STONKS SILO切替
   （financial_trend_calculator.py・fetcher.py・analyzer.py）
4. フェーズD Step2-3: TANUKI TAIL切替
   （quarterly_review_generator.py・tail_dcf_bridge.py）
5. フェーズD Step2-4: HypeCore切替
6. フェーズD Step2-5: stock.htmlフロントエンド＋診断・補助スクリプト
   7件切替
7. フェーズE: `normalized/`廃止
8. （本線外・優先度中）[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]対応
9. （本線外・優先度低）[[ONDS-LOAR-SHARES-SCALE-SUSPECT-1]]・
   [[RCAT-2016Q3-ORPHANED-QUARTERLY-FILE-1]]・
   [[PARSER-MERGED-TAG-MIXING-RISK-1]]・[[LAYER3-ANNUAL-
   MISCLASSIFICATION-NOW-RMBS-1]]・[[LAYER3-ANNUAL-MISCLASSIFICATION-
   MINOR-5TICKERS-1]]・[[LAYER3-SNPS-STALE-TAG-PRIORITY-1]]・
   [[LAYER3-SM-SGA-SEPARATION-NONE-FALLOUT-1]]（2026-09-05に旧
   LAYER3-ROIC-WACC-NONE-4TICKERS-1・旧FINTREND-SM-JOBY-NONE-1を統合）
10. 以下、2026-08-03時点リストから変更なし（上記の旧①〜⑭のうち
   Stage系を除く未完了分）: [[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]・
   [[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]]・
   [[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]・
   [[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]・
   [[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]・
   [[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]・
   [[LITE-COGS-DA-TAG-UNMERGED-1]]・[[STONKS-SILO-FP-LABEL-PERIOD-
   VALIDATION-1]]・[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]・[[HON-GROSSPROFIT-
   2009-RESIDUAL-DISCREPANCY-1]]・[[ELF-ROE10YR-RECALC-PENDING-1]]・
   [[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]・
   [[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]・[[BS-IDENTITY-
   LOG-NONDETERMINISTIC-KEY-ORDER-1]]

---

## セッション終了時ブラッシュアップ（2026-07-19）

**2026-07-18の完了内容**: [[ARCH-DATA-1]]残課題④・[[FYE-CHANGE-BOUNDARY-
COLLISION-BLIND-1]]新規登録・[[FY52WEEK-BS-NULL-SILENT-1]]Phase A完了・
WARN-23全10銘柄検証・[[TTM-STOCK-FIELDS-DEAD-1]]完了・
[[TRUST-SUMMARY-EPIC-1]]棚卸し再検証・[[FCF-ESTIMATE-SKIP-STABLE-1]]完了・
[[FCF-CONVRATE②]]可視化実装完了・[[SKIP-RISK-EVENTS-WIPE-1]]新規登録
（詳細はBACKLOG_DONE.md該当エントリ参照）。

**2026-07-19の完了内容**:
- [[FY52WEEK-BS-NULL-SILENT-1]] Phase B Stage1完了（BS4フィールド
  〈short_term_investments/long_term_debt/short_term_debt/rpo〉の
  absent銘柄179件を一次情報〈SEC EDGAR 10-K原本〉で個別確認し、①候補
  タグ欠落・②生涯フェードアウト・③真の構造的ゼロの3類型に分解。
  安全に解消できる57件（41銘柄）を`parser.py`のXBRL_MAPPINGへ標準タグ
  追加。BACKLOG_DONE.mdへ記録済み〈本体エントリはStage2/3が残るため
  BACKLOG.mdに残置〉。残るStage2/3を[[FY52WEEK-BS-STI-OVERRIDE-
  DESIGN-1]]・[[FY52WEEK-BS-FADEOUT-FALLBACK-1]]として新規登録
- [[GROWTH-SANITY-CLASS-SYNC-1]]完了（MOのfloor〈15%〉問題を解消。
  `growth_sanity.py::TICKER_INDUSTRY_OVERRIDES`にMO: "Tobacco"追加＋
  floor到達中かつindustry_g単独1件候補の場合のみ閾値を2件→1件へ緩和
  する限定的な条件緩和〈案B'〉を実装。事前に全37銘柄でシミュレーション
  し、LOARへの副作用がないことを確認してから実装〈この手法をCHAT_RULES.md
  「候補修正の全母集団シミュレーション」として明文化〉。MO以外への影響
  ゼロを全100銘柄比較で確認。BACKLOG_DONE.mdへ全文移動済み）。残る論点
  5件を新規登録: [[GROWTH-VERDICT-SEQUENCING-BUG-1]]・
  [[WST-SECTOR-MISCLASSIFICATION-1]]・[[JNJ-XOM-PM-FLOOR-RISK-1]]・
  [[GROWTH-STRUCTURAL-MISMATCH-CANDIDATES-1]]・
  [[JOBY-STATIC-GROWTH-HARDCODE-1]]
- [[GROWTH-VERDICT-SEQUENCING-BUG-1]]完了（growth_sanityのverdict/
  warnings・TANUKI SCOREのGROWTH_PREMIUM判定が、DCF再計算前の初期計算値
  を検証し続けるシーケンシングバグを根本修正。再計算〈条件付き発火〉
  成功後に`check_growth_sanity()`を採用値`recommended_g`で再実行し
  `verdict`/`warnings`/`signals`/`phase1_growth`/`floor_hit`を更新する
  方式で実装。全母集団シミュレーションで事前確認してから61銘柄を実データ
  再生成し、改善17件〈VZ: AGGRESSIVE→PLAUSIBLE含む〉・悪化3件
  〈ALAB/IONQ/RCAT、PLAUSIBLE→REVIEW。ハイパーグロース×成熟業種平均の
  構造的ミスマッチの正しい表面化と個別調査で確認済み・想定内〉・
  変化なし8件〈ASTS/BKNG/BROS/ELF/KULR/LLY/TER/XOM〉という結果を得た。
  TANUKI SCOREはCONのみGROWTH_PREMIUM→TRIMへ是正。
  `report_consistency_check.py` NG=0・pytest 377 passed（既知の
  MSFT/NVDA 2件除く）を確認。悪化3銘柄の個別調査で判明した副次発見
  〈RCATのsector誤分類、Electronics_General設定だがyfinance実態は
  Aerospace & Defense〉を[[RCAT-SECTOR-MISCLASSIFICATION-1]]として
  新規登録。BACKLOG_DONE.mdへ全文移動済み）
- [[ANOMALY-PATTERN-CATALOG-1]]新規登録（型A「候補集合＋freshness
  収束型」を確定〈実例: KLAC/TER/V/SOFI〉、型B「非分類BS・近似値
  許容型」は予約のみで実例なし。REGISTER-FLOW-REDESIGN-1・
  PREFLIGHT-CHECK-1と統合的に設計する方針。CLAUDE_CODE_START.md
  Step 0.5付近に実装までの暫定注意書きも追加）
- [[NVDA-STI-TAG-UNIDENTIFIED-1]]個別調査完了（$12.4B差額の正体は
  FY2026〈2026-01-25期〉に新規上場した投資先の株式評価額。従来型A
  〈ticker_restrictions単一タグ適用〉で解決できると見込んでいたが、
  債券タグ＋株式タグを合算しても実額と0.9%乖離し、かつ株式タグは
  当該10-K本体では未申告〈後続10-Qの比較開示でのみ登場〉と判明。
  型Aにも型Bにも該当しない新パターン「型C: 資産クラス変化・
  当年度未タグ化型」として整理したが、対応方針〈①近似値許容
  ②翌年度10-K待ち③当面None許容〉は未確定のまま次回持ち越し）
- [[BS-FIELD-NONE-TRANSITION-DETECT-1]]新規登録（NVDA調査の過程で、
  XBRLタグ申告停止による完全欠損を検知する仕組みがBS項目に一切
  存在せず、実例6件〈SOFI-DATA-1・AVGO型14銘柄・LLY-CAPEX-STALE-1・
  CASH-TAG-MISSING-1〈未解決〉・KLAC/TER/V・NVDA〉すべてが偶然発見
  だったと判明。「前年有値→当年None」遷移を検知するWARN-26案として
  登録。[[ANOMALY-PATTERN-CATALOG-1]]の予防側・[[FY52WEEK-BS-NULL-
  SILENT-1]] Phase B/Cと補完関係）

次セッションの筆頭候補（優先順・各項目の優先度欄を確認の上で確定）：
~~① [[GROWTH-VERDICT-SEQUENCING-BUG-1]]~~ ✅ 2026-07-19完了。
   growth_sanityのverdict/warnings・TANUKI SCORE判定を採用値ベースに
   根本修正、61銘柄再生成（改善17件・悪化3件・変化なし8件）。
   詳細はBACKLOG_DONE.md参照
~~① [[FY52WEEK-BS-STI-OVERRIDE-DESIGN-1]]~~ ✅ 2026-07-19完了
   （KLAC/TER/V/SOFIの4銘柄）。NVDAのみ[[NVDA-STI-TAG-UNIDENTIFIED-1]]
   として分離継続。詳細はBACKLOG_DONE.md参照
~~① [[NVDA-STI-TAG-UNIDENTIFIED-1]]~~ ✅ 2026-07-19完了（`cross_filing_tags`
   機構を新設し実装。$12.4B差額の正体〈FY2026新規上場投資先の株式評価額〉
   を、候補タグ合算の近似値〈実額比残差+0.9%〉で解消）。詳細は
   BACKLOG_DONE.md参照
~~② [[BS-FIELD-NONE-TRANSITION-DETECT-1]]~~ ✅ 2026-07-19完了
   （`report_consistency_check.py`へWARN-26新設、既知8件を
   `warn_acknowledged.json`へ事前登録。新規3件〈LLY/SCCO/SPIR〉は
   [[BS-FIELD-NEWLY-MISSING-2026-1]]として分離登録）。詳細は
   BACKLOG_DONE.md参照
~~③ [[FYE-CHANGE-BOUNDARY-COLLISION-BLIND-1]]~~ ✅ 2026-07-19完了
   （WARN-24新設・`fye_change_candidate_scan.py`クラスタリングツール化。
   新規2件〈LITE/WST〉は[[FYE-BOUNDARY-COLLISION-UNCONFIRMED-1]]として
   分離登録）。詳細はBACKLOG_DONE.md参照
~~④ [[SKIP-RISK-EVENTS-WIPE-1]]~~ ✅ 2026-07-19完了（`_pre_existing_risk_events`
   スナップショットパターンで解消、単一コミット）。詳細はBACKLOG_DONE.md参照
~~⑤ [[WST-SECTOR-MISCLASSIFICATION-1]]~~・~~⑥ [[RCAT-SECTOR-MISCLASSIFICATION-1]]~~
   ✅ 2026-07-19完了（2件一括対応、`beta_config.json`のsector値修正のみ）。
   詳細はBACKLOG_DONE.md参照
~~⑦ [[FY52WEEK-BS-FADEOUT-FALLBACK-1]]~~ ✅ 2026-07-19完了（22銘柄）。
   除外3件（CSGP/KULR/RCAT）は[[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]
   として分離継続。詳細はBACKLOG_DONE.md参照
~~⑧ [[SPLIT-REALTIME-GAP-1]]~~ ✅ 2026-07-20完了（NVDA+新規発見AVGO/CPRT/
   WMT/LRCX/CELH/TSLA〈8銘柄〉・KLAC事前登録、RCAT除外）。詳細はBACKLOG_DONE.md参照
~~⑨ [[GROWTH-STRUCTURAL-MISMATCH-CANDIDATES-1]]~~ ✅ 2026-07-20完了
   （HON: segment_config.json修正でAGGRESSIVE→PLAUSIBLE。残る14銘柄は
   FCF-CONVRATE②型の可視化注記を実装）。詳細はBACKLOG_DONE.md参照
   ~~[[FY-COLLISION-LOG-NONDETERMINISTIC-1]]~~ ✅ 2026-07-20完了（対象7銘柄
   AVAV/CAKE/COHR/CRM/FCX/FICO/HON、詳細はBACKLOG_DONE.md参照）
   ~~[[MRVL-2019-2020-NULL-1]]~~ ✅ 2026-07-20完了（実害なし・構造的境界特性と
   判明。詳細はBACKLOG_DONE.md参照。副次発見はCIK-DISCONTINUITY-OLDEST-YEAR-GAP-1
   として分離登録）
   ~~[[EPS-ANALYZER-NORMALIZE-SCOPE-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md
   参照（net_income共通化過程で41銘柄規模の潜在バグを発見・是正）
   ~~[[KO-SPIR-CF-CAUSE-UNCONFIRMED-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md参照
   ~~[[JOBY-STATIC-GROWTH-HARDCODE-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md参照
   ~~[[CWAN-SNPS-MA-DISTORTION-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md参照
   （対応方針を生FCF平均調整から買収・統合関連加算控除へ転換、47銘柄に一般適用）
   ~~[[CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1]]~~ ✅ 2026-07-20完了。詳細は
   BACKLOG_DONE.md参照（複数CIK統合実装・汎用検知ロジックの登録フロー組み込み
   まで完了）
   ~~[[FCF-EST-DIRECTION-GUARD-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md
   参照（ENTGのSELL→WATCH是正含む21銘柄のIV精度改善）
   ~~[[FCF-EST-NET-BASIS-FIX-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md参照
   （ma_addback計算をnet_amount基準に統一、25銘柄のIV精度改善）
   ~~[[AMZN-DIVERGENCE-HIGH-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md
   参照（原因確定・対応不要、副次発見はAMZN-CONVRATE-OVERRIDE-REVIEW-1として
   分離登録）
   ~~[[FCF-EST-NOTE-DISPLAY-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md参照
   ~~[[FCF-OUTLIER-PREROUNDING-LOSS-1]]~~ ✅ 2026-07-20完了。詳細はBACKLOG_DONE.md参照

**着手条件未達のため次回候補から除外**: [[JNJ-XOM-PM-FLOOR-RISK-1]]
（優先度：中だが着手条件は「候補件数が実際に2件を下回った場合」。
現時点では監視対象として登録のみ、着手不可）

---

**2026-07-23時点の申し送り（AS-IS/TO-BE設計セッション終了時）**:
本日〜翌朝（2026-07-22〜23）のセッションで、全サブシステム監査・
AS-IS/TO-BE設計・499項目の完全定義（`FIELD_DEFINITIONS.md`）・一次データ層
のAS-IS/TO-BE設計（`INPUT_DATA_AS_IS.md`/`INPUT_DATA_TOBE.md`）を実施し、
発見事象**39件を本日新規登録**した（優先度高11件・中19件・低9件、上記
「次セッションの筆頭候補」欄より上に記載の各エントリ参照）。

**次セッションで着手可能な状態にあるもの、いずれも未着手**:
- 本日登録した39件（優先度高11件を筆頭候補とする。特に
  [[NETCASH-DUAL-CALC-1]]・[[NETINCOME-DUAL-PIPELINE-1]]は`TO_BE.md`⑫⑭群
  で統一定義まで確定済みのため実装コストが低い）
- `TO_BE_FINAL_LIST.md`・`TO_BE.md`（①〜⑯群）で確定した統一定義・重複解消・
  `NAMING_CONVENTIONS.md`の命名規則は、いずれもまだ実際のソースコードに
  反映されていない。本日の作業は一貫して「実装は行っていない、定義・
  分類の記録のみ」という範囲宣言のもとで進めたため、対応の実装着手は
  本セッション終了時点で全て次回以降に持ち越しとなっている
- [[FIVE-CATEGORY-RECLASSIFY-1]]（AS-IS-437〜441・404・057/058/060の
  5分類再判定）も同様に未着手

次セッションの筆頭候補は、上記39件のうち優先度高から、実データでの
実害が確認済みかつ統一定義が既に確定している[[NETCASH-DUAL-CALC-1]]・
[[NETINCOME-DUAL-PIPELINE-1]]を推奨する（着手条件の確認は各エントリ参照）。

---

追記（2026-07-24 [[CAPEX-SIGN-UNNORMALIZED-1]]・[[RICE-TTM-CAPEX-SUM-SIGN-1]]完了）:
~~[[CAPEX-SIGN-UNNORMALIZED-1]]~~・~~[[RICE-TTM-CAPEX-SUM-SIGN-1]]~~
✅ 2026-07-24完了（normalizer.py・ttm_calculator.py・STONKS SILO
fetcher.pyの3箇所にCapEx符号正規化を実装。影響銘柄5件
〈ALAB/APGE/INTU/KULR/ONDS〉のnormalized/・ttm/・TANUKI VALUATION
出力を再生成。詳細はBACKLOG_DONE.md「2026-07-24（完了）」参照）。

この完了により、[[SECDATA-STORAGE-FRAGMENTATION-1]]（優先度：中、
common/sec_data統合フェーズ1）の着手条件「[[CAPEX-SIGN-UNNORMALIZED-1]]
の対応方針確定」が満たされ、着手可能な状態になった（同タスクの
「対応方針」「着手条件」欄に反映済み）。ただし上記[[NETCASH-DUAL-CALC-1]]・
[[NETINCOME-DUAL-PIPELINE-1]]（優先度：高）を差し置く優先度ではないため、
次セッションの筆頭候補自体は変更しない。
