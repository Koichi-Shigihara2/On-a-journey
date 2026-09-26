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

## 優先度：中（こなれてきたら対応）

---



（[[DISCOVER-CONFIG-DUAL-MGMT-1]]は2026-08-15実装完了、BACKLOG_DONE.md
「2026-08-15（完了）」参照）

---

（[[JNJ-XOM-PM-FLOOR-RISK-1]]は2026-09-26、report_consistency_check.pyのCHECK-52〈候補1件以下〉・CHECK-53〈floor発動×raw FCF CAGR負〉による自動検知に置き換えてクローズ、BACKLOG_DONE.md「2026-09-26（完了）」参照）

---

（[[TAIL-SEC-ITEMS-1]]は2026-09-13、TANUKI TAIL全10銘柄への展開完了
（うちAPGEはrisk_factors/mdaの2項目のみ既知のギャップあり、
[[TAIL-SEC-ITEMS-APGE-WHITESPACE-1]]として別途新規登録）によりクローズ、
BACKLOG_DONE.md「2026-09-13（完了）」参照。TAIL-SEC-ITEMS-APGE-
WHITESPACE-1自体も2026-09-16に方針Y実装で解消済み、
BACKLOG_DONE.md「2026-09-16（完了）」参照）

---

（[[MACRODATA-SCHEDULED-SILENT-GAP-CSCICP-USALOL-1]]は2026-09-13、
`05_indicator_schedule.csv`から該当7行を削除し実装完了、
BACKLOG_DONE.md「2026-09-13（完了）」参照）

---

### [TTM-DATA-DRIFT-BEHIND-PIPELINE-1] common/sec_data/ttm/配下のTTM系列ファイルが2026-07-26生成のまま、以降のパイプライン修正に追従しておらず陳腐化している可能性
**優先度:** 中（登録時「高」から引き下げ、影響実測の結果、現在進行形の
実害はゼロと確定したため。構造的リスクは残存）
**分類:** データ品質 / パイプライン出力の陳腐化
**登録日:** 2026-08-02
**発見:** [[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]実装検証時（チャット記録）

#### 内容
（2026-09-19注記: 本文中の「105銘柄」は登録当時〈2026-08-02〉の
ticker宇宙の件数。その後のAVGO/CWAN/ENB除外により現行ticker宇宙は
102銘柄。過去記録のため本文自体は書き換えない。）

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

#### 再確認結果（2026-09-12、チャット記録、読み取りのみ・据え置き継続）
直近1ヶ月の追加修正（CRM(2018) GP-COGS不整合修正
[[CRM-REVENUE-COGS-TAG-COVERAGE-GAP-1]]含む）を踏まえ、鮮度・実害の
再確認を実施した。結論として「据え置き継続」（クローズしない）。

**STEP1: 鮮度・cron健全性の再確認**
`common/sec_data/ttm/`の最終コミットは2026-09-06であり、陳腐化はして
いない。GitHub Actions API（`gh`未導入のため`curl`直接照会、
workflow ID 258451437「SEC Data Update」）でワークフロー実行履歴を
確認したところ、2026-08-16・2026-08-23の2回連続でscheduleトリガーの
実行が失敗していたことが判明した。ただしこれは既に根本原因が診断済み
（依存パッケージインストール工程の欠落）で、BACKLOG_DONE.mdの
[[DATA-FRESHNESS-MONITORING-FUTURE-IDEA-1]]（2026-08-30完了）で
是正済みの事象であり、同タスクでは再発検知のための監視機構
（`common/system_health.py`の`check_j_workflow_runs()`、通称
「Check J」）も新設されている。本セッションで`python3
common/system_health.py`をローカル実行し確認した結果、`[J]
CronRuns: ✅ 17件監視 / すべて正常`であり、直近2回（08-30・09-06）は
成功していることを確認した。cron自体は現在健全。

**STEP2: 直近1ヶ月の修正のLayer3/TTM側への反映状況の再確認**
`layer3_builder.py`を確認したところ、parser.py側の
[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]案a〜e（`_align_revenue_*`
`_align_cost_of_revenue_*`・`dimension_aggregate`等の関数群）および
本日実施したCRM修正（`_REVENUE_ALIGNMENT_CANDIDATES`・
`_COST_OF_REVENUE_ALIGNMENT_CANDIDATES`への`SalesRevenueServicesNet`・
`CostOfServices`追加）は、依然としてLayer3側へ移植されていないことを
確認した。Layer3独自の候補タグ設定`config/sec_concept_definitions.json`
を直接確認したところ、`revenue`候補リストには`SalesRevenueServicesNet`
が同様に欠落している（parser.py側が今回修正した欠落と同一）一方、
`cost_of_revenue`候補リストには`CostOfServices`が既に含まれており
欠落はなかった。なお[[LAYER3-OI-RECONSTRUCTION-FALLBACK-GAP-1]]は
Layer3側へも移植済み（コミット`b9f781c7f3`）であることを確認しており、
移植の要否判断・実施は案件ごとに一貫していない運用実態が改めて
確認された。

**新規発見（構造的リスクの実例化）**: APP（保有銘柄）のFY2023年次
revenueを直接比較したところ、parser.py側
（`common/sec_data/data/APP/annual_2023.json`）は$3,283,087,000、
Layer3側（`build_ticker_store("APP")`→`get_field_entries(store,
"revenue")`をannual・end=2023-12-31でフィルタ）は$1,841,762,000と、
**約14.4億ドルの乖離**を確認した（FY2024も$4,709,248,000 vs
$3,224,058,000で同様に乖離、FY2025は$5,480,717,000で完全一致）。
原因を`company_facts.json`の生XBRLデータで確認したところ、SECの
`Revenues`タグ自体が同一期間（FY2023）に対し複数の異なる値を報告して
いた（FY2023本体・FY2024の10-Kでの比較列では$3,283,087,000、FY2025の
10-Kでの比較列では$1,841,762,000〈遡及修正後とみられる〉）。
parser.py側とLayer3側でどちらの値を採用するかのtie-breakロジックが
異なるため、同一の生データから異なる値を選択してしまっていることが
直接確認された。これは本日のCRMタグ網羅漏れとは異なる、
「企業が過去実績を遡及修正した場合、独立した2パイプラインが
クロスチェックなしに異なる値を選択しうる」という、本チケット登録時
から理論上の懸念とされていた構造的リスクの、初めての具体的・定量的な
実例確認である。

**STEP3: 保有9銘柄の現在のTTMローリング窓における実害再確認**
`common/sec_data/reader.py::SECReader.get_rpo_context()`と同じ手法
（`common/sec_data/normalized/{ticker}_quarterly_normalized.json`から
直近4四半期を合算する`rev_ttm = sum(e["val"] for e in rev_all[-4:])`
方式）をparser.py側の独立再計算値とみなし、`common/sec_data/ttm/
{ticker}_ttm_series.json`の最新値（`series[0]["flow"][field]["val"]`）
と、保有9銘柄（ADBE/APP/CELH/CRWV/NVDA/PLTR/SOFI/SOUN/TSLA）×7指標
（Revenue/NetIncome/GrossProfit/OCF/SBC/CapEx/OperatingIncome）の
全63通りで突合した。結果、Revenue/NetIncome/GrossProfit/OCF/SBC/CapExの
6指標は9銘柄全てで完全一致（乖離ゼロ）。唯一SOFIのOperatingIncomeのみ
normalized/側が`None`（parser.py側`common/sec_data/data/SOFI/
quarterly_*.json`の`operating_income`も直近四半期で同じく`None`である
ことを直接確認済み）に対しLayer3/TTM側は$1,572,033,000と、Layer3側が
より完全なデータ（自己のGP逆算バックフィル）を持っているという無害な
差異であり、実害ではない。

**総括・対応方針**: 現在のTTMローリング窓において保有銘柄への実害は
今回も確認されなかった（STEP3）ため、優先度「中」は維持する。一方で
APP実例により、構造的リスク自体は理論上の懸念ではなく実例のある
現実のリスクであることが再確認された（STEP2）ため、クローズはせず
「据え置き継続」とする。cron健全性は現状問題なし（STEP1）。今後の
自動検知については、STEP3で用いた「parser.py側の独立再計算値と
Layer3/TTM側の値を突合する」手法を`report_consistency_check.py`への
新規WARNチェックとして恒久化する（本チケットとは別コミットで対応、
下記③参照）。

#### 再確認結果（2026-09-19、チャット記録、読み取りのみ・据え置き継続）
STEP1（鮮度・cron健全性）: `common/sec_data/ttm/`の直近更新はコミット
`1be11d715`（2026-09-14 00:09 JST、`github-actions[bot]`の週次自動更新）＋
`c79df1cd5`（2026-09-17 21:14 JST、`[[FCF-CONVRATE-LOWER-DIVERGENCE-1]]`
に伴うLYFT/PAYS/FLYW3銘柄のみの手動再生成）。`gh`未導入のため
`SEC_Data_Update`ワークフロー（cron `0 12 * * 0`＝毎週日曜21:00 JST）の
実行成否は自動コミット`Update SEC Data - YYYY-MM-DD`の履歴突合で代替確認
し、2026-08-16・08-23の既知障害（2026-08-30是正済み）以降は毎週正常に
コミットが発生していることを確認した。`layer3_builder.py`/
`ttm_calculator.py`/`parser.py`への直近の変更は2026-09-17の1件（LYFT
CapExタグ追加）のみで、同日中に対象3銘柄が手動再生成済みであることも
確認した。

STEP2（全銘柄再生成との突合）: 現行ticker宇宙（`config.get_all()`＝102
銘柄、`ttm/`ファイル数と完全一致）全件について、`layer3_builder.
build_ticker_store()`＋`ttm_calculator.calc_ttm_series()`を一時
ディレクトリ（scratchpad配下、調査後削除済み）へ再生成し、FLOW_FIELDS
17種＋FCFを既存`ttm/`と`ttm_end`単位（anchor不一致0件）で突合した。
**実質差分（|Δ|>1%）は102銘柄×18フィールド中0件**。唯一検出された
1件（COHR・eps_diluted・ttm_end=2025-06-30）は5.5e-17 vs 2.08e-17という
浮動小数点誤差ノイズ（両者とも実質ゼロ）であり実害なしと判定。
FCF/runway系フィールドへの影響もゼロ。登録時（2026-08-02、PEP銘柄
SG&A約9.5%乖離を確認）や前回再確認時（2026-09-12、APP FY2023 revenue
約14.4億ドル乖離を確認）とは異なり、今回は現行パイプライン出力と
`ttm/`が完全に同期していることを確認した（cronが直近まで正常稼働し、
唯一のコード変更〈LYFT CapEx〉も即日手動反映されたため）。

STEP3（「layer3_builderがparser.pyと独立実装」診断の再検証）: 現行
コードでも診断は成立することを確認した。`layer3_builder.py`は
`parser.py`を一切import せず（grep 0件）、`fact_overrides.json`参照も
`parser.py`側のみ（`layer3_builder.py`は0件）。parser.py側の安全ロジック
3種（`_resolve_bs_entity_mixing`・`_backfill_total_liabilities_via_
identity`・`_align_cost_of_revenue_to_revenue_period`）も
`layer3_builder.py`には引き続き存在しない（一方`quarterly.py::
_classify_period`・`fact_selection.py::select_latest_filed`はドキュメント化
済みの意図的共有のまま）。2026-09-12発見のCRM関連ギャップ
（`config/sec_concept_definitions.json`のrevenue候補に
`SalesRevenueServicesNet`が依然欠落、`CostOfServices`は既存のまま）も
未変化で残存している。

**総括・対応方針**: 実質差分0件（COHRの1件は測定誤差）・cron健全・
保有銘柄含む全銘柄で実害なしという結果は、前回（2026-09-12）の
「据え置き継続」判断を追認するもの。案C（運用チェック継続、統合作業には
着手しない）を維持し、優先度「中」も変更しない。着手条件（下記）も
変更なし。

#### 着手条件
以下いずれかのトリガー条件が発生するまで保留:
1. 今後の運用チェックでTTM anchor範囲内×FLOW型フィールドの修正が発生し
   実害が確認された場合 → 案B（部分統合）を個別タスクとして起票
2. 新DB構築プロジェクトのフェーズDに進む際、3スキーマ併存全体の解消を
   検討するタイミングで本件も合わせて設計する
3. `report_consistency_check.py`に新設したparser.py⇔Layer3/TTM突合
   WARNが実際に発火した場合（99銘柄いずれかで乖離検知）→ 発火した
   銘柄・フィールドを起点に実害確認・案B着手要否を判断する

#### 優先度変更（2026-09-19、中→低）
2026-08-02の初回検証・2026-09-19の再検証と2回連続で実質差0件が確認され、
着手条件も上記3トリガーのいずれかが発生するまで完全に保留（トリガー
制）となっている。能動的な着手見込みがない待機状態であるため、
優先度を「中」から「低」へ変更する。

#### 再確認（2026-09-24、クローズ見送り）
陳腐化クローズを検討したが、前提の一部が不一致のため見送った:
- 鮮度: `common/sec_data/ttm/*_ttm_series.json`全102ファイルの
  `generated_at`が2026-09-24（同日のSEC Data Update〈bot、`569acf66bf`〉後）
  であることを確認（条件一致）
- **CHECK-47（parser⇔Layer3 TTM乖離）が3件発火**（着手条件3に該当）:
  - BKNG `stock_based_compensation`（乖離2.2%、🆕未確認）
  - FCX `net_income`（乖離39.9%、🆕未確認）
  - RCAT `stock_based_compensation`（乖離8.2%、`config/warn_acknowledged.json`で
    確認済み: normalized側のSBC四半期欠落によるCHECK-47突合方式由来の
    見かけ上の乖離、実害なし）
  BKNG・FCXの2件は未確認のため、着手条件3に従い発火銘柄・フィールドを
  起点とした実害確認が次のステップとなる（本日は記録のみ）

#### 着手条件3の実害確認と対応（2026-09-24、指示書⑥・同補足）
2026-09-24に発火したWARN-47 3件を調査した:
- BKNG SBC（2.2%）・RCAT SBC（8.2%）: normalized/側の末尾4件が連続した4四半期でない
  （10-K由来のQ4欠落）ことによるCHECK-47の誤検知で実害なし。
  [[CHECK47-NONCONTIGUOUS-WINDOW-1]]で連続性チェックを追加し解消
- FCX net_income（39.9%）: 実害あり。3系統とも非支配持分込みの`ProfitLoss`を
  親会社帰属タグより優先していた（FY2025 連結4,152M vs 親会社帰属2,204M）。
  [[NET-INCOME-NCI-PARENT-ATTRIBUTION-1]]で3系統共通の候補定義に一本化し解消
- 横断確認で見つかったLayer3の上書き設定の写しの陳腐化（CPRT/CEG/JOBYのTTM
  gross_profit None等）は[[TICKER-OVERRIDES-SINGLE-SOURCE-1]]で解消
CIと同条件の再生成後、WARN-47は0件。本エントリの構造的リスク（layer3_builder.pyが
parser.pyと独立実装であること）自体は残るため、エントリは引き続きトリガー制で保留する。

#### missing>0（4四半期未満）のTTMの消費側の扱い（2026-09-24、指示書⑦、調査のみ・実害なし）
`common/sec_data/ttm/*_ttm_series.json`の消費者は、TANUKI VALUATION
（`data_fetcher.py`・`pipeline.py`）と検証系（`audit.py`・`registration_validator.py`・
`report_consistency_check.py`）のみ。HypeCore・TAIL（tail_dcf_bridge等）はLayer3の
四半期系列を直接読み自前で集計、EPS Analyzerは独自のTTM計算、STONKS SILOは
ttm/を参照しない。
- `data_fetcher.py`: FCF系列（`TTMReader._filtered_fcf`）はOCF・CapExのいずれかが
  quarters_used<4の期間を除外、RICE用（`build_rice_annual_shape`）はOCF・CapEx・
  revenue・net_incomeのいずれかが不完全な行を除外（SBCのみ部分値をNone化して行は維持）
  → (a) 除外
- `pipeline.py`のDuPont分解（net_income・revenue・buyback）とセグメントのTTM売上
  フォールバック（revenue）: `series[0]`（最新アンカー）のみ参照し、quarters_usedを
  見ない → 最新アンカーについては(b)、それ以前のアンカーは(c) 参照しない
- 実測（本番データ）: missing>0のエントリは全銘柄で1,390件、うち1,182件（85%）が
  最古アンカー（index 4、四半期粒度データ取得開始前の境界期間）。(b)経路
  （最新アンカーのnet_income・revenue・buyback）に該当するのはbuybackの5件のみ
  （CPRT 1,414M〈1四半期〉・S 49.2M〈2四半期〉・JOBY 85K〈3四半期〉・CIX 0・KULR 0）。
  net_income・revenueの該当は0件
- buybackの消費者は`financial_health.buyback_ttm`→TANUKI SCORE画面のキャッシュトラップ
  判定のみで、`buybackTtm === 0`（還元ゼロか）の真偽しか使わない。部分合計>0の
  CPRT・S・JOBYは「自社株買いあり」で正しい。CIXは配当利回り3.2%のため判定に
  影響せず、KULRは部分合計0を除外（null）しても判定ロジックがnullを還元なしと
  同じに扱うため結果は同じ → 判定への実害なし
- BROS・CEG（[[NET-INCOME-NCI-PARENT-ATTRIBUTION-1]]でスタブ期間を除外した結果、
  最古アンカー2022-06-30のnet_incomeがquarters_used=3の部分TTMになる）: 最古アンカーは
  pipeline.pyが参照せず、data_fetcher.pyのRICE変換はnet_income不完全の行を除外する
  （修正前もval=Noneで除外されていた）ため、成長率・YoY・CAGR・RICEへの混入はない
- LYFTのTTM capital_expenditure・FCF=None（[[TICKER-OVERRIDES-SINGLE-SOURCE-1]]、
  上書き先タグが10-K年次のみの申告）: FCFは`_select_fcf_source()`で年次実績へ、RICEは
  `build_rice_annual_shape()`が空になり`get_annual_range()`の年次実績へフォールバック
  （latest.jsonの`fcf_source`・`rice_data_source`はいずれも`annual_fallback`）。
  IV（64.87）・RICE（base rice 1.709）・tanuki_score（HOLD）は算出されており、
  欠落する指標はない

---

## 優先度：低（アイデア段階）

（[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]は数値KPI抽出構想
→「MD&A原文のセグメント別成長見通し定性要約」（10銘柄パイロット、
2026-09-18完了）→ MD&A原文に将来向き定量ガイダンスが存在しないと
判明したため方針転換し、XBRLセグメント売上からの決定論的算出（案①）
へ最終確定・第1陣7銘柄実装完了（2026-09-18）。拡張候補3件は
[[SEGMENT-XBRL-GROWTH-EXPANSION-CANDIDATES-1]]（IDEAS_AND_WATCH.md へ移動）参照。
BACKLOG_DONE.md「2026-09-18（完了）」参照）

---

（[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]は2026-08-27実装完了(PascalCase→
snake_caseキー修正、全3実消費箇所で正常化を確認)、BACKLOG_DONE.md
「2026-08-27（完了）」参照）

---

（[[KPI-FETCHER-SEGMENT-SOURCE-ORPHANED-1]]は2026-08-27、誤った前提
〈XBRL会計セグメントデータ〉で設計された機能と判明したため残骸を撤去、
BACKLOG_DONE.md「2026-08-27（完了）」参照）

---

（[[PARSER-MERGED-PARTIAL-CONCEPT-TAG-1]]は2026-09-26、年次D&Aの確認後にreport_consistency_check.pyのCHECK-55による自動検知に置き換えてクローズ、BACKLOG_DONE.md「2026-09-26（完了）」参照）

---

### [SPLIT-HISTORY-REGISTRATION-GAP-DETECT-1] split_history.yamlへの株式分割の登録漏れを検知する仕組みがない
**優先度:** 低
**分類:** データ品質ゲート / EPS ANALYZER・TANUKI VALUATION
**登録日:** 2026-09-25
**発見:** [[SPLIT-REALTIME-GAP-REVERSE-1]]の対象洗い出し（2026-09-25）

#### 内容
株式分割の遡及補正（EPS ANALYZERの`apply_split_adjustments()`、TANUKIの
3年希薄化率）は`config/split_history.yaml`に登録された分割にしか効かないが、
登録は手作業で、漏れを検知する仕組みがない。実例: BKNGの25-for-1
（2026-04-06、yfinanceのsplitsには記録あり）が2026-09-25まで未登録だった。
また、登録されていない過去の分割（NVDA 2021年4:1・TSLA 2020年5:1など）で
四半期株数の基準が混在している（既存のヒューリスティック検知で吸収されて
いる範囲のみ）。

#### 対応方針（未確定）
- yfinanceのsplitsとsplit_history.yamlを突き合わせ、未登録の分割（比率が
  1から十分離れているもの）をWARNで出すチェックを、report_consistency_check.py
  等に追加する
- 除外が必要なもの: 分社化に伴う株価調整のノイズ（SCCO〈~1.005〉・HON
  〈1.032・1.061・0.9535〉）、評価対象期間より前のシェル会社時代の分割
  （RCAT 2014・2016・2019）。HONの2026-06-29はyfinanceでは0.9535と記録
  されているが、実際は1-for-2の株式併合（8-Kで確認）であり、yfinanceの
  比率をそのまま登録値に使えない点にも注意
- 登録時は8-K（Item 3.03/5.03・8.01）で分割日・比率・accnを確認する運用を
  維持する

#### 着手条件
なし

#### 2026-09-26 検知を実装、未登録の分割29件を確認（指示書⑰ STEP 3、停止条件に該当）
`report_consistency_check.py`にCHECK-54（WARN-54 split_history未登録、NG化しない）を追加した。
yfinanceのsplitsのうち、比率が0.9〜1.1の範囲外で、銘柄のローカルデータ（SEC年次・EPS四半期の最古日）
以降にあり、`split_history.yaml`に同一銘柄・日付±10日の登録がないものを1分割1件でWARNにする。
`--include-yfinance-checks`指定時のみ実行（CHECK-41と同じ扱い。SEC_Data_Updateで実行される）。
yfinanceの取得に失敗した銘柄はINFO-54としてスキップを出すだけ。対象はtanukiまたはepsがtrueの全銘柄。

除外: RCATの2014-07-23・2016-11-22・2019-08-01（シェル会社時代）は`warn_acknowledged.json`に
分割日を`match`にして登録した（RCATの他の分割は発火する）。SCCO・HONの分社化調整（1.032・1.061・
0.9535等）は0.9〜1.1の範囲内で発火しないため、台帳には登録していない。

**未登録の分割（2026-09-26、yfinance比率そのまま。`split_history.yaml`には登録していない）**:

| 銘柄 | yfinanceの日付 | yfinanceの比率 |
|---|---|---|
| AAPL | 2014-06-09 | 7 |
| AAPL | 2020-08-31 | 4 |
| AMZN | 2022-06-06 | 20 |
| COHR | 2011-06-27 | 2 |
| CPRT | 2012-03-29 | 2 |
| CPRT | 2017-04-11 | 2 |
| CPRT | 2022-11-04 | 2 |
| CRM | 2013-04-18 | 4 |
| CSGP | 2021-06-28 | 10 |
| DELL | 2018-12-28 | 1.806 |
| DELL | 2021-11-02 | 1.973 |
| FCX | 2011-02-02 | 2 |
| GOOGL | 2014-04-03 | 1.998 |
| GOOGL | 2022-07-18 | 20 |
| HEI | 2010-04-27 | 1.25 |
| HEI | 2011-04-26 | 1.25 |
| HEI | 2012-04-25 | 1.25 |
| HEI | 2013-10-23 | 1.25 |
| HEI | 2017-04-19 | 1.25 |
| HEI | 2018-01-18 | 1.25 |
| HEI | 2018-06-28 | 1.25 |
| KO | 2012-08-13 | 2 |
| NVDA | 2007-09-11 | 1.5 |
| NVDA | 2021-07-20 | 4 |
| SCCO | 2006-10-03 | 2 |
| SCCO | 2008-07-10 | 3 |
| TSLA | 2020-08-31 | 5 |
| V | 2015-03-19 | 4 |
| WST | 2013-09-27 | 2 |

- 指示書で例示されたNVDA 2021（4:1）・TSLA 2020（5:1）は上表に含まれる。HON 2026-06-29（1-for-2）は
  2026-09-25に登録済みのため発火しない
- 登録すると`apply_split_adjustments()`・TANUKIの3年希薄化率が動き出すため（CHAT_RULES事例21）、
  登録は別の指示書で、8-Kでの日付・比率確認と既存結果への影響確認を行ってから実施する
- 回帰テスト8件追加（stashで実装を外すと7件失敗、戻すと全件成功。失敗しなかった1件は台帳matchの
  確認で、STEP 2で追加済みの機能）

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

（[[REPORT-TXT-CAPM-IV-MISSING-1]]は2026-08-27実装完了（8フィールド
全対応）、BACKLOG_DONE.md「2026-08-27（完了）」参照）

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
  - ~~`[[PARSER-MERGED-TAG-MIXING-RISK-1]]`~~（2026-09-25クローズ: 仮説の型は
    実データで0件。別原因の部分概念タグ混入を`[[PARSER-MERGED-PARTIAL-CONCEPT-
    TAG-1]]`〈優先度低〉として登録、BACKLOG_DONE.md参照）
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
[[DATA-JUMP-CHECK-NETINCOME-SBC-1]]として分離登録。2026-09-26にIDEAS_AND_WATCH.md へ移動）

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
   ~~[[PARSER-MERGED-TAG-MIXING-RISK-1]]~~（2026-09-25クローズ、後続は
   [[PARSER-MERGED-PARTIAL-CONCEPT-TAG-1]]）・[[LAYER3-ANNUAL-
   MISCLASSIFICATION-NOW-RMBS-1]]・[[LAYER3-ANNUAL-MISCLASSIFICATION-
   MINOR-5TICKERS-1]]・[[LAYER3-SNPS-STALE-TAG-PRIORITY-1]]・
   ~~[[LAYER3-MOAT-ROIC-4TICKERS-NONE-1]]~~（2026-09-24陳腐化クローズ。
   4銘柄とも年次operating_incomeが存在しroic.pyは年次OIを優先するため
   TTM推定フォールバックは発火しておらず、ROIC-WACC比率・Moat Scoreは
   実測値で算出済み。BACKLOG_DONE.md「2026-09-24（完了）」参照）
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
