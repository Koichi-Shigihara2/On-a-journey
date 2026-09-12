# PROJECT_STATUS.md — 新一次データベース構築プロジェクト進捗

作成日: 2026-07-23

---

**セッションサマリー（本ファイルは新DB構築プロジェクト専用の進捗記録だが、
専用のCHANGELOG.mdが存在しないため`CHAT_RULES.md`の規定によりセッション
終了時ブラッシュアップのサマリーもここに記録する。新DB構築プロジェクト
自体とは無関係な話題であることに留意）**

- **2026-08-26**: Market Pulse/MACRO PULSE既知バグ6項目の稼働状況を
  実コード・実データで再検証し、全項目を解消。`[[MACRO-TRUTHY-ZERO-
  BUG-1]]`（履歴バックフィルのtruthy判定ゼロ値欠落、ゼロ金利期間
  10,570行のff_rate等を復元）・`[[HOLLOW-RALLY-DEAD-1]]`（sp500列
  欠落による恒久不発火、案Xで解消・過去1309行バックフィル）・
  `[[MARKETPULSE-MINOR-INCONSISTENCIES-1]]`①③④⑤（Hindenburg固定値・
  CSV列欠落・breadthパススルー漏れ・F&G情報源混同を解消、②は設計意図を
  コメント明記、⑥は休眠状態確認の上優先度低で据え置き）・
  `[[FEARGREED-DUPKEY-BUG-1]]`（previous_closeを`fetch()`経由の真の値へ
  修正）・`[[RECESSION-SCORE-TRIPLE-CALC-1]]`③（ステップ関数/lerp補間の
  併存を画面上へ開示、実ブラウザ〈Playwright〉で表示確認済み）。加えて
  `[[LAYER1-GROWTH-HYPEPHASE-DECAY-GAP-1]]`（GROWTH-1のHypePhase加重を
  DCF成長率計算から削除、固定50:50へ復元・7銘柄再生成）を実装し、
  受け皿となる新機能epic`[[STOCKHTML-SIGNAL-CONSISTENCY-SECTION-1]]`を
  登録。`CHAT_RULES.md`へ教訓2件（事例11: フォールバック発火件数の代理
  指標混同／事例12: 再検証時もBACKLOG_DONE.mdの毎回grepが必要）を追加。
  セッション終了時ブラッシュアップで、上記7項目に加え過去完了分
  `[[MACRO-PULSE-ZONE-25-STALE-1]]`（2026-08-21完了）・`[[MACRO-PULSE-
  3M-FORECAST-SNAPSHOT-MISMATCH-1]]`（2026-08-22完了）の移設漏れも発見し
  合わせてBACKLOG_DONE.mdへ移設、`CLAUDE_CODE_START.md`の完了済み4項目
  （MP-BIZDAY-1/ARCH-DATA-1/TSCORE-TRAP-1/SEC-CTRL-1）の陳腐化記載を
  削除した。詳細はBACKLOG_DONE.md「2026-08-26（完了）」「2026-08-22
  （完了）」「2026-08-21（完了）」参照。新DB構築プロジェクトのコード・
  データには変更なし。

- **2026-08-26②**（同日、上記に続く3件の指示書を順次実施）:
  1. **層単位/フロントエンド単位の方法論導入**: `CHAT_RULES.md`へ
     事例13（層単位横串検証よりフロントエンド起点の縦割り検証を優先する
     教訓）を追加。`SYSTEM_MAP.md`へMarket Pulse・MACRO PULSEの
     「画面要素→導出関数→生データソース」依存関係マップ（7要素）を
     新設（コミット`ee6adccbf`・`071d7704d`）
  2. **実地確認2件**: `[[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]`は
     `Market_Data_Daily_Update`→`TANUKI_VALUATION_Update`の
     `workflow_run`連鎖を2営業日連続（16分ラグ）で確認・current_price
     鮮度も実測確認しBACKLOG_DONE.mdへクローズ。`[[WORKFLOW-SEC-
     TANUKI-GAP-1]]`は下流チェーン（HypeCore/EPS→TANUKI VALUATION）は
     確認できたが`SEC_Data_Update`起点のチェーン自体は唯一の発火機会
     （2026-08-23）で証拠見つからず未確定のまま現状維持。
     `[[Q4-IMPLIED-CALC-TRIPLICATION-1]]`は実装（2026-07-24完了済み）を
     再確認しBACKLOG_DONE.mdへクローズ移設（コミット`fbc8c6f95`・
     `c40c0124d`）
  3. **CAPM-IV調査（実装なし）・Playwright体系的拡張**:
     `[[REPORT-TXT-CAPM-IV-MISSING-1]]`の対応要否・範囲を調査し
     （実装せず報告のみ）、同種の見落とし7件を追加発見。副次発見の
     `kpi_data`未配線（セグメントKPIテーブル恒久非表示）を
     `[[TANUKI-VALUATION-MISC-GAPS-1]]`へ⑧として追記。
     `browser_checks/check_dependency_map.py`を新設し、SYSTEM_MAP.mdの
     依存関係マップ7要素を実ブラウザで初回確認（全一致、consoleエラー
     0件）（コミット`040a2142e`・`74f836586`）
  4. **セッション終了時ブラッシュアップ**: `CLAUDE_CODE_START.md`の
     陳腐化記載2件（SEC/TANUKI VALUATION生成順序ズレ節の「対応未実装」・
     低優先度課題リスト中の`[[Q4-IMPLIED-CALC-TRIPLICATION-1]]`）を
     現状に合わせて訂正
  詳細はBACKLOG_DONE.md「2026-08-26（完了）」参照。新DB構築プロジェクトの
  コード・データには変更なし。

- **2026-08-27**（指示書5件を順次実施、**実害のあった重大インシデント
  〈STONKS SILO 45日間本番停止〉の復旧を含む**）:
  1. **バックログ再分析＋精度改善2件**: `[[LIQUIDITY-CSV-FIRST-ROW-
     UNBOUNDLOCALERROR-1]]`（`update_liquidity_csv()`が空CSV初回実行時
     UnboundLocalError、`prev_rrp`等のNone初期化漏れ）を修正
     （コミット`823ad7404`）。`[[STONKS-SILO-PRICE-SCHEDULE-LAG-
     SUSPECT-1]]`（cron実行順序ラグの疑い）を調査する過程で、
     **`results.json`が2026-08-13から更新されていないことに気づき、
     STONKS SILOが2026-07-13以降45日間・約30回連続でGitHub Actions
     自動更新に失敗し続けていた実障害を新規発見**（真因は既存登録
     `[[STONKS-SILO-CLI-TICKERS-SHADOW-1]]`、`pipeline.py`の
     `__main__`ブロックが`tickers`変数でモジュール参照を上書きする
     衝突。2026-08-11登録時点で「cronは無事のはず」という未検証の
     想定を優先度「中」のまま16日間放置していたことが実害拡大の
     一因、`CHAT_RULES.md`事例14として教訓化）
  2. **STONKS SILO 45日間停止の緊急復旧**（Koichiさん承認済み）:
     `pipeline.py`の変数名を`cli_tickers`へリネーム、CLI引数あり/なし
     両経路の回帰テスト追加、全25銘柄を実際に再生成し45日ぶりの
     正常完走を確認（判定・スコアは全銘柄不変、価格のみ市場変動を
     反映）（コミット`ff59e7b13`・`c649741ca`）
  3. **report.txt網羅性拡充8件＋セグメントKPIテーブル配線修正**:
     CAPM-IV・DuPont・sensitivity・maturity_profile・return_metrics・
     validation・alpha_was_capped・fcf_ttm_endをreport.txtへ追加
     （各フィールドの性質に応じた粒度で、`[[REPORT-TXT-CAPM-IV-
     MISSING-1]]`8件全対応、コミット`772ff9b17`）。`pipeline.py`から
     `kpi_fetcher.build_kpi_data()`を呼び出す配線を追加したところ
     （コミット`0450abe77`）、配線後も`kpi_data`が全銘柄で`None`の
     ままと判明し、依存データソース2種（`annual_*.json["segments"]`・
     `segment_config.py::SEGMENT_OVERRIDES`）が両方とも陳腐化して
     いることを新規発見（`[[KPI-FETCHER-SEGMENT-SOURCE-ORPHANED-1]]`）。
     さらにDuPont分解が`ttm_series.json`のキーをPascalCaseで参照して
     おり全104銘柄で恒久的に未発火だったことも新規発見
     （`[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]`）
  4. **セグメントKPIテーブル機能の残骸撤去**: `[[KPI-FETCHER-SEGMENT-
     SOURCE-ORPHANED-1]]`の対応方針をKoichiさんと検討する対話の中で、
     **機能の設計前提自体が根本的に誤っていた**ことが判明した——
     当初「KPI＝XBRLの正式な会計セグメントデータ」を前提に設計されて
     いたが、Koichiさんが提示した実例（SOFIの総会員数・クロスバイ率・
     NIM等）により、本来のKPIは決算資料の文章に企業ごと個別の形式で
     開示される経営指標であり全く別物と判明。Koichiさんの判断
     （一から作り直してよいが新機能の着手は見送り、今回は誤った前提の
     既存実装の撤去のみ）に従い、`kpi_fetcher.py`・`kpi_config.py`・
     `common/sec_data/segment_fetcher.py`・`pipeline.py`の配線・
     `stock.html`の表示コード（関数・専用CSS計約300行）を撤去。
     削除前に`src/value/tanuki_valuation/segment_config.py`
     （Pythonモジュール）が削除対象と紛らわしいが実際は現役の中核
     モジュールであることを確認し誤削除を回避（コミット`0091fa09c`）。
     再設計の構想は`[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-
     IDEA-1]]`に記録（着手は見送り中）
  5. **DuPont分解のキー不一致修正**: `ttm_calculator.py::FLOW_FIELDS`
     はsnake_caseと再確認した上で`pipeline.py`側5箇所をsnake_caseへ
     修正、bare exceptにログ追加。**既存の回帰テスト5クラスのモック
     ヘルパーが、当時のバグと同じPascalCaseで自己整合的にモックして
     おり、本番不具合を長期間検知できていなかったことを発見**、
     ヘルパーをsnake_caseへ是正（`CHAT_RULES.md`事例15として教訓化、
     Koichiさんからの提案）。APP/ASTS/NVDAで実データ検証しDuPont値が
     正しく算出されることを確認（コミット`5ce4a592c7`）
  詳細はBACKLOG_DONE.md「2026-08-27（完了）」参照。新DB構築プロジェクトの
  コード・データには変更なし。全銘柄への反映は次回の通常パイプライン
  実行サイクルに委ねる（今回はAPP/ASTS/NVDA・STONKS SILO全25銘柄のみ
  再生成）。

- **2026-08-30**（🟢有効判定済み項目の診断結論再検証・棚卸しの再実行
  〈全19ラウンド〉完了、Koichiさん承認の複数タスクを順次実施）:
  1. **診断結論再検証・全19ラウンド完了**: 事例17（CHAT_RULES.md）に
     基づき、BACKLOG.md全168件（フェーズ1バッチA完了8件除く160件相当）
     について「診断結論そのものの妥当性」を2段階（表層的事実確認＋
     診断結論の検証）で全数再検証し**162/162件（100%）完了**。2件が
     診断結論自体の陳腐化と判明しBACKLOG_DONE.mdへ移設
     （`[[MP-TOOLTIP-1]]`・`[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`）。
  2. **SEC_Data_Update障害調査＋修正**: `SEC_Data_Update`ワークフローが
     2026-08-16・08-23の2回連続でConsistency Check Gateに失敗し、
     約3週間分（2026-08-09以降）のSEC EDGARデータ更新が本番に反映
     されないまま放置されていたことを発見。原因はCIの依存パッケージ
     インストール不足（`pandas`/`yfinance`欠落）と特定し
     `.github/workflows/SEC_Data_Update.yml`を修正、`update.py`を
     ローカル実行して105/105銘柄のデータを復旧した。
  3. **コード場所ベースのクラスタリング再分析**: 旧来のタイトル・
     カテゴリ類似ベースクラスタ（バッチA、的中率半数未満）を、実際の
     コード配置基準で再分析し確度：高/中/低を明示して
     `BACKLOG_PRIORITY_ROADMAP.md`5B節に記録した。
  4. **データ鮮度監視の実装**: `[[DATA-FRESHNESS-MONITORING-FUTURE-
     IDEA-1]]`をKoichiさん判断で優先度引き上げの上実装完了。
     `common/system_health.py`に`check_j_workflow_runs()`を新設し、
     GitHub Actions REST APIでcron定義済み全ワークフローの実行状況を
     監視する（新規外部サービス連携なし）。
  5. **フェーズ1バッチB（残り6件）**: `[[FCF-CAGR-YEARS-MISMATCH-1]]`・
     `[[EPS-DISCREPANCY-FLAG-OVERLOAD-1]]`・
     `[[HYPECORE-REALSTRONG-DUAL-IMPL-1]]`・`[[TVGROWTH-EXPLICIT-
     DEFAULT-AMBIGUOUS-1]]`・`[[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-
     MISSELECT-1]]`の5件を実装完了（**フェーズ1は10件中9件完了**）。
     `[[PARSER-STOCKHOLDERS-EQUITY-CROSS-YEAR-MISSELECT-1]]`は
     CRM型・VRT型を別ロジックで修正し、105銘柄シミュレーションで
     副次発見したAVAV(2020, rpo)の同型バグも解消（AVGO/ENTGへの
     意図しない副作用を検知・是正した経緯もコミットメッセージに記録）。
     残る`[[RICE-ADJ-ASYMMETRIC-ZERO-1]]`1件のみ、表示数値そのものを
     左右する製品判断でKoichiさんの設計判断待ちのため据え置き。
  詳細はBACKLOG_DONE.md「2026-08-30（完了）」参照。新DB構築プロジェクトの
  コード・データには変更なし。現存BACKLOG.md総数はブラッシュアップ後の
  機械カウントで**156件**。

- **2026-09-01〜09-02**（指示書4件を順次実施、全てpush済み）:
  1. **`[[DISCOVER-SUBSYSTEM-REMOVAL-1]]`**: Discoverサブシステム
     （ニュース収集・カタリスト発掘・株価インパクト予測）本体を削除。
     一次情報でなくGrok生成コンテンツへの依存を減らす方針の一環
     （risk_fetcher.py撤去と同系統）。`config/discover_config.json`は
     `registration_validator.py`・`docs/portfolio/index.html`が参照する
     共有のティッカー区分設定ファイルと確認し削除対象外に明示的に除外。
     関連する既存タスク7件（`CATALYST-DEDUP-1`等）もあわせてクローズ。
  2. **`[[RISK-EVENTS-REMOVAL-1]]`**: `risk_fetcher.py`（Grok web検索に
     よる簡易リスクイベント取得）を撤去。IV・DCF・TANUKI SCORE等の
     計算系のいずれにも使われていない表示専用機能だったため。全100銘柄
     フルパイプライン再生成でrisk_events削除以外の実質差分0件を確認。
  3. **`[[MARKET-PULSE-LOCAL-DUAL-EXEC-1]]`**: Market Pulseの実際の
     更新元がKoichiさんのローカル環境で稼働するWindowsタスク
     スケジューラ（2026-05-06作成、リポジトリ管理外、FG_Level2向けの
     目的で二重実行）だったと判明・削除。GitHub Actions単独化のため
     `Market_Pulse_Update.yml`のcronをUTC21:25へ変更したところ、
     `Market_Data_Daily_Update.yml`（同じくUTC21:25）と完全に同時刻に
     なりpush競合リスクを一度作り込んでしまったことに気づき、
     UTC21:35（2026-08-29時点の10分差設計）へ即座に是正した。
  4. **`[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`**: AVGOの旧CIK登録が
     無関係な買収先企業（Broadcom Corporation）のデータを指す誤統合が
     発覚。データ境界の是正ではなくAVGO自体をOn-a-journey管理対象から
     除外する方針で解決（ポートフォリオ・TANUKI TAILいずれにも保有・
     監視登録されていないことを確認済み）。既存の「銘柄削除時の必須
     手順」に載っていない設定・データファイルが9件見つかり、Step 0
     （削除前の全参照洗い出し）新設を含め手順書自体を恒久拡充した。

  **副次発見**: `[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]`の本文が
  「残タスク（Stage 2〜3、未着手）」のまま2026-08-05時点から更新されて
  おらず、実際にはStage 2・3・3a・3bまで全て実装完了していたという
  陳腐化をAVGO対応の過程で発見・訂正。真の残タスクの精査中、
  「MRVL/DELL旧CIK拡張データの粒度確認」が正式ID未採番のまま宙に
  浮いていたことも発見し、`[[SECDATA-LEGACY-CIK-GRANULARITY-1]]`として
  新規登録（優先度低〜未定）。

  詳細はBACKLOG_DONE.md「2026-09-01（完了）」〜「2026-09-02②（完了）」
  参照。新DB構築プロジェクトのコード・データには変更なし。現存
  BACKLOG.md総数は機械カウントで**148件**。

- **2026-09-03**（長時間セッション、9項目を順次実施・全てpush済み）:
  1. **`[[MARKET-PULSE-LOCAL-DUAL-EXEC-1]]`実地確認**: cron変更後
     2営業日分（09-02完了JST08:31・09-03完了JST08:30）を確認したところ
     想定JST6:35頃に対し約1時間55分の遅延が継続。2日分のみでは恒常的
     傾向か一時的かの判断材料として不十分なため判断は保留し、次回確認を
     1週間程度後に設定
  2. **`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・`[[MACRODATA-LAYER-
     CONSTRUCTION-1]]`クローズ**: 本文で「完了」明記済みなのに優先度
     「高」のアクティブなエントリとして残置されていた2件をBACKLOG_
     DONE.mdへ移設。依頼文の「参照先が壊れている」という前提を検証した
     結果、実際は既に完了・正しく存在していたと判明し前提を是正
  3. **`[[TRUST-SUMMARY-EPIC-1]]`クローズ**: データ信頼性3段階（入力
     完全性・成長率算出・FCF/DCF計算）を貫通する可視化EPIC。
     FCF-CONVRATE①（rate_is_sector_defaultフラグ）②（report.txt/
     stock.html表示不一致修正）③（Damodaran比較メタ情報追加）を実装完了
     し、判断保留中だった4件の解消も確認した上でEPIC自体をクローズ
  4. **`[[QUALITY-GATES-EPIC-1]]`ゲート1拡張**: yfinance自動照合を
     `operating_income`単体から売上高・純利益へ横展開（CHECK-41新設、
     全99銘柄実測p50=p95=0.0%）。`--include-yfinance-checks`フラグを
     新設し、SECデータ更新頻度（週1回）に合わせて`SEC_Data_Update.yml`
     のみでyfinance突合を実行するよう週次化
  5. **ENB登録抹消・`exclusion_reason`列追加**: `[[QUALITY-GATES-
     EPIC-1]]`ゲート0対応・`[[REGISTER-FLOW-REDESIGN-1]]`方針4。
     ENB（カナダのIFRS/40-F提出企業、SEC annual data 0件のまま孤立
     登録）をBXと同じA案（登録抹消）で解消。`cik_lookup.csv`に
     `exclusion_reason`列を新設しRKLB/ZS/SN/APGEの4銘柄に除外理由を
     記入
  6. **`[[REGISTER-FLOW-REDESIGN-1]]`方針2・3**: `cik_lookup.csv`の
     status列に`provisioning`（登録処理中）を追加し4大パイプラインの
     バッチ対象から除外、`registration_validator.py --promote`でNG=0
     確認後に昇格する仕組みを新設。`common/registration/register_
     ticker.py`（新規銘柄登録オーケストレーションスクリプト）を新設し
     Step 1〜8を自動連続実行、Step 2.5・3.5はClaude Codeの10-K確認を
     前提に一時停止する設計とした。実装過程で既存の安全弁
     （ZS-TICKERS-LEAK-1由来のCLI引数フィルタ）との衝突を実地検証
     （HIMS、一時ブランチ）で発見・`get_registrable_tickers()`新設で
     解消。対応方針5件が全件完了・実質達成となった

  項目1〜3は前セッションからの続き（詳細はBACKLOG_DONE.md「2026-09-02
  ④（完了）」以前参照）。項目4〜6は本セッションで新規に着手・完了
  （詳細はBACKLOG_DONE.md「2026-09-03（完了）」・BACKLOG.md各エントリ
  参照）。新DB構築プロジェクトのコード・データには変更なし。現存
  BACKLOG.md総数は機械カウントで**151件**。

- **2026-09-04〜09-05**（非常に長時間のセッション、実装・修正20件超・
  BACKLOG統合9クラスタ・些末項目クローズ2件・ゲート実装2件、
  全てpush済み）:
  1. **個別バグ・データ品質是正8件**: `[[STONKS-PILLAR-THRESHOLD-
     MISMATCH-1]]`（`dq.score`専用の`deficitColor()`新設でpillarColor
     閾値不一致を解消、IOT/JOBYの表示色を実測確認）・
     `[[RPO-REVTTM-GATE-SKIP-1]]`（懸念シナリオが構造上発生し得ないと
     判明・クローズ）・`[[JNJ-XOM-PM-FLOOR-RISK-1]]`（定点確認で
     `floor_hit=False`継続を確認・監視継続）・`[[ENTG-TER-SEGMENT-1]]`
     （10-K一次情報でsegment_config.jsonへセグメント別成長率を追加）・
     `[[EPS-LOAR-1]]`（IPO前株式数構造の別物四半期を株式数基準で除外）・
     `[[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]`（黒字化年予測手法を
     TANUKI VALUATION・STONKS SILO間で統一）・`[[LAYER3-COGS-
     STRUCTURAL-GAP-16TICKERS-1]]`（残10〈実11〉銘柄の一次情報裏取り、
     新規2件を追加登録）・`[[FYE-BOUNDARY-COLLISION-UNCONFIRMED-1]]`
     （LITE/WSTを一次情報で確認・確定しクローズ）
  2. **`[[QUALITY-GATES-EPIC-1]]`ゲート実装2件**: ゲート4（旧
     TICKER-AUDIT-1）を`system_health.py::check_k_ticker_audit()`として
     実装（見直し候補・検証由来無保有・P4-CIKOrphan集約・
     monitor_tickers.yaml同期漏れの4検知）。ゲート0（`[[PREFLIGHT-
     CHECK-1]]`）を`common/registration/preflight_check.py`として実装し
     `register_ticker.py`のStep 0.5直後に組み込み（上場後3年未満・
     直近提出書式が10-K/10-Q以外・収益系XBRLタグ不在の3検知、
     フラグが立っても自動停止しない設計）。いずれもBACKLOG_DONE.mdへ
     完了移設済み
  3. **`[[TICKER-LOADING-UNIFICATION-1]]`実装**: 銘柄リスト読み込みの
     重複実装3箇所（`system_health.py`・`src/tail/`3ファイル・
     `common/sec_data/config.py`）を`tickers.py`経由に統一。`get_cik()`・
     `get_all_rows()`・`get_registrable_tickers(flag=None)`を新設
  4. **小規模技術的負債13件（2バッチ）・些末項目2件クローズ**: 1バッチ目
     8件（`[[CLAUDE-CODE-START-FY-DESC-FIX-1]]`等）・2バッチ目5件
     （`[[MOAT-CATALOG-DUP-1]]`・`[[CHECK-COVERAGE-1]]`〈CHECK-42として
     実装〉等）・`[[TAIL-DETAIL-1]]`/`[[SILO-LEGEND-1]]`を対応不要として
     クローズ
  5. **BACKLOG統合9クラスタ（21件→8エントリ）**: DESIGN-2/4/5/6/14/15
     （6件）→`[[HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1]]`、PREVENT-5・
     TICKER-AUDIT-1（2件）→`[[QUALITY-GATES-EPIC-1]]`付録へ全文統合、
     UX-FLOW-1/MULTI-1/ARCH-1/EVAL-2/DESIGN-8-3/DESIGN-8-4（6件）→
     `[[FUTURE-FEATURE-IDEAS-CATALOG-1]]`、TANUKI-FIN-1・TANUKI-FIN-2
     （2件）→`[[TANUKI-FIN-2]]`に一本化、EPS-ANALYZER-INTEGRATE-1/
     RICE-INTEGRATE-1/ANALYST-VS-IV-INTEGRATE-1（3件）→
     `[[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]`、LAYER3-ROIC-WACC-
     NONE-4TICKERS-1・FINTREND-SM-JOBY-NONE-1（2件）→
     `[[LAYER3-SM-SGA-SEPARATION-NONE-FALLOUT-1]]`、MACRODATA-FTSD-
     SERIES-ID-INVALID-1・MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1
     （2件）→後者に一本化。いずれも要約せず全文転記し、検証スクリプトで
     元エントリの内容が一言一句欠落なく含まれていることを確認済み

  **Market Pulseのローカル/GitHub Actions二重実行**
  （`[[MARKET-PULSE-LOCAL-DUAL-EXEC-1]]`）は実地確認を継続中（1週間分の
  傾向確認が完了していないため判断は引き続き保留）。詳細はBACKLOG_
  DONE.md「2026-09-04（完了）」「2026-09-05①〜⑤（完了）」・
  BACKLOG.md各エントリ参照。新DB構築プロジェクトのコード・データには
  変更なし。現存BACKLOG.md総数は機械カウントで**107件**（統合・クローズ
  により151件から大幅減）。

- **2026-09-06〜09-07**（非常に長時間のセッション、実装・修正10件・
  BACKLOG.md全97件棚卸し・CWAN登録抹消、全てpush済み）:
  1. **個別実装・修正10件**: `[[STONKS-SILO-PRICE-SCHEDULE-LAG-
     SUSPECT-1]]`・`[[REGISTER-FLOW-REDESIGN-1]]`クローズ、
     `[[TOOLTIP-INDEX-1]]`（`index.html`へ`info-tooltip.js`適用）・
     `[[LAYER3-COGS-CANDIDATE-TAG-EXPANSION-1]]`（JOBY/CEG/CPRT限定で
     `cost_of_revenue`候補タグ拡張）・`[[CHECK-COVERAGE-2]]`（DuPont
     分解null銘柄検出consistency check）・`[[THESIS-FIELD-1]]`（NVDA
     `thesis.json`の`entry_price`欠損修正）・`[[DISCOVER-RESIDUAL-
     LINKS-1]]`（Discoverリンク残骸除去）・`[[SN-TANUKI-DELAY-1]]`
     （SNの`tanuki=false`解除でTANUKI VALUATION対象拡大）・
     `[[DATA-JUMP-CHECK-GENERALIZE-1]]`（段差型急変検知を売上総利益・
     CapExへ展開、純利益・SBCは`[[DATA-JUMP-CHECK-NETINCOME-SBC-1]]`へ
     切り出し）・`[[STONKS-FINANCIAL-VECTORS-RELATIVE-1]]`
     （`financial_vectors`が相対順位である旨を明示）を実施
  2. **BACKLOG.md全97件棚卸し**（「表層的事実確認＋診断結論の検証」の
     2段階で全件を本文精読＋実コード照合）: Round1（34件検証・🟢有効
     30件・クローズ2件〈`[[TAIL-KPI-PROPOSER-CORE-ONLY-GATE-1]]`・
     `[[SECDATA-STORAGE-FRAGMENTATION-1]]`〉・疑義2件報告）に続き
     残り63件を中断せず連続実施、97件全件完了。最終結果は🟢有効93件・
     クローズ2件・⚠️疑義2件〈`[[MARKETDATA-CWAN-FROZEN-DATA-
     SUSPECT-1]]`・`[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]`（後者は
     `[[LAYER3-ANNUAL-CLASSIFICATION-DROPS-DATA-1]]`の既知パターンの
     新事例と確認、記載維持）〉・軽微な更新余地2件〈`[[ANOMALY-
     PATTERN-CATALOG-1]]`・`[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-
     CHECK-MISSING-1]]`、いずれも報告のみ〉
  3. **CWAN登録抹消**: 上記棚卸しで疑義2件のうち`[[MARKETDATA-CWAN-
     FROZEN-DATA-SUSPECT-1]]`（66日間の価格凍結データがTANUKI
     VALUATION最新出力へ実際に混入）を最優先個別調査した結果、
     **CWAN（Clearwater Analytics）が2026-06-25にPermira・Warburg
     Pincus主導で1株$24.55の非公開化買収を受けNYSE上場廃止**していた
     ことが根本原因と判明（SEC EDGAR`former_names`・yfinance
     delisted検知・凍結価格が買収対価と一致等、複数独立ソースで
     裏付け。データパイプライン自体は正常挙動だったことも確定）。
     AVGO・ENBに続く3件目の同型ケースとして「銘柄削除時の必須手順」に
     従いtanuki/stonks_silo/eps/hypecore全4パイプラインから登録抹消・
     全銘柄再生成し、`[[MARKETDATA-CWAN-FROZEN-DATA-SUSPECT-1]]`を
     クローズ

  詳細はBACKLOG_DONE.md「2026-09-06（完了）」・BACKLOG.md各エントリ
  参照。新DB構築プロジェクトのコード・データには変更なし。現存
  BACKLOG.md総数は機械カウントで**94件**。

- **2026-09-08〜09-09**（非常に長時間のセッション、根拠薄弱項目の
  系統的クローズ〈計17件〉・保有銘柄関連の実バグ修正・複数の根治的
  対応を実施、全てpush済み）:
  1. **根拠薄弱項目クローズ17件**（BACKLOG.md全86件へ判定基準を機械的
     適用したバッチ12件〈`[[JOBY-BLADE-ACQUISITION-IMPACT-SCOPE-1]]`・
     `[[HYPECORE-SUBSTAGE-LAYER3-UNVERIFIED-1]]`・`[[LAYER3-SNPS-
     STALE-TAG-PRIORITY-1]]`・`[[LAYER3-UNEXPLAINED-SINGLE-TICKER-
     DIFFS-1]]`・`[[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]`・
     `[[VRT-REVENUE-2018-MISSING-1]]`・`[[RCAT-2016Q3-ORPHANED-
     QUARTERLY-FILE-1]]`・`[[LAYER3-RPO-CANDIDATE-ORDER-1]]`・
     `[[SECDATA-LEGACY-CIK-GRANULARITY-1]]`・`[[AMZN-CONVRATE-
     OVERRIDE-REVIEW-1]]`・`[[LITE-COGS-DA-TAG-UNMERGED-1]]`・
     `[[LAYER3-VISA-EPS-TAG-MISSING-1]]`〉＋既存の安全策・後続実装で
     無効化された5件〈`[[STOCKHTML-YTD-FILTER-BUG-SUSPECT-1]]`・
     `[[SEC-BKNG-SHARES-ANOMALY-1]]`・`[[LAYER3-CROSS-TAG-YEARLY-
     QUARTERLY-GENERAL-RISK-1]]`・`[[OPERATING-CASH-FLOW-CONTINUING-
     DISCONTINUED-GAP-1]]`・`[[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]`〉）
  2. **`[[TAIL-THESIS-KPIS-EMPTY-ADBE-APGE-1]]`**（保有銘柄ADBE含む）:
     thesis.jsonのkpis未登録による監視KPI実績セクション消失バグを発見。
     対症療法（手作業コピー）を撤回し`_get_effective_thesis_kpis()`
     による自動フォールバックへ根治的修正、satellite全7銘柄へ波及。
     直近4四半期の推移表示も追加
  3. **`[[POLICY-AB-TREND-BLIND-1]]`**: Policy Bの上方乖離トレンド好転
     検知不能バグを修正。49銘柄でDCF_Reliability LOW→NORMAL（保有銘柄
     4つ含む）、うち45銘柄でClassificationもWATCHから変化
  4. **`[[FALSY-ZERO-PATTERN-SWEEP-1]]`・`[[MACRO-STYLE-FCF-ZERO-
     TRUTHY-EXCLUDE-1]]`**: falsy-zeroパターン横断調査、新規発見2件
     含む4箇所を修正
  5. **`[[REVENUE-TAG-PRIORITY-FRAGILE-1]]`**: revenue/cost_of_revenue
     タグ選択の根治的対応（四半期整合性tie-break新設）、TDY
     FY2013-2015のrevenue誤取得を修正
  6. **`[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`**: 案a/c/d実装で
     対象9銘柄・全15年度分（LRCX/AMD/KO/JNJ/RMBS/BSY/CRM/ONDS/MRVL）を
     完全解消。MRVL(2017)の前回判断（対応困難）をrevenue restatement
     見落としと訂正
  7. **`[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]`**:
     CHECK-46新設（revenue−cost_of_revenue=gross_profit整合性検証）。
     実データ校正（105銘柄・1034件）で許容誤差0.1%を確定、CRM(2018)の
     新規未調査乖離を発見（次セッション候補）
  8. **`[[DEAD-CODE-AUDIT-BATCH-1]]`**: 3件削除（phase1_scan.py・
     backfill_history.py・quality_checker.py）、report_txt_parser.py
     はCLI運用手順の現役利用と判明し現状維持に訂正
  9. **`[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]`**: 当初23件を全件
     最終トリアージ完了。PLTR(2019)含む残存7件は構造的制約で対応不可と
     確定
  10. **`[[ONDS-LOAR-SHARES-SCALE-SUSPECT-1]]`**クローズ（安全策
      〈yf_implied優先〉により実害ゼロと確認）

  依頼書は「SN-TANUKI-DELAY-1・STONKS-SILO-PRICE-SCHEDULE-LAG-
  SUSPECT-1・CWAN登録抹消」も本日の対応として例示していたが、git log
  照合の結果これらは2026-09-06〜09-07セッション（上記）で既に完了済み
  だったと確認し、本サマリーからは除外した（詳細は`CLAUDE_CODE_
  START.md`該当ブロック参照）。

  詳細はBACKLOG_DONE.md「2026-09-08」「2026-09-08②」「2026-09-09」〜
  「2026-09-09⑬」（完了）各節・`CLAUDE_CODE_START.md`該当ブロック
  参照。新DB構築プロジェクトのコード・データには変更なし。現存
  BACKLOG.md総数は機械カウントで**67件**（94件から27件減）。

- **2026-09-10**（非常に長時間のセッション、10件クローズ・1件新規
  登録、全てpush済み）:
  1. `[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]`クローズ（既知の
     クロスファイルID重複2件のうち1件を解消）
  2. **PLTR(2019)・CHECK29系のBS恒等式「対応不可」7件を新規機構
     `dimension_aggregate_fetcher.py`（生XBRLインスタンス直接パース）
     で全件解消**（`[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]`、
     `[[ANOMALY-PATTERN-CATALOG-1]]`型D新規カタログ化）
  3. `[[LAYER3-COGS-DIMENSION-RECOVERY-CDNS-INTU-1]]`: 上記②の機構を
     duration factへ拡張しCDNS/INTUのcost_of_revenueを回収。過大な
     事前警戒（DCF影響を未検証のまま記載）・NG=2報告の説明不足を
     自ら訂正した経緯を含め正直に記録（詳細は`CLAUDE_CODE_START.md`
     該当ブロック参照）
  4. `[[KPI-UNIT-HARDCODE-USD-1]]`: PLTR等の比率KPI誤表示（unit
     ハードコード）を名前ベース判定で修正、detail.htmlの副次バグも
     解消
  5. `[[GROK-MODEL-PRICE-1]]`（未クローズ、xAI Console確認待ちで
     BACKLOG.mdに残置）: レガシーエイリアス是正後、Koichiさんの
     xAI Console実データ確認で支出の80%が想定外の
     「grok-4.20-0309-reasoning」だったと判明し追加調査。呼び出し元
     特定・risk_fetcher/Discover仮説の訂正・新規バグ発見（下記6.）
  6. **新規発見・実装完了** `[[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]`:
     週次フォールバックcronが`workflow_run`連鎖成功後も無条件に追加
     実行される構造的バグを実行履歴の実測で発見、冪等性ガードで解消
     （Adjusted_EPS・TANUKI_Score。TANUKI_VALUATIONは実害ゼロと確認
     し対応不要）
  7. `[[MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1]]`: 景気サイクル
     フェーズ複合スコアの観測日開示を`idxLatestAsOf()`拡張で根治的に
     解消、AI週次レポートプロンプトにも観測日を付記
  8. `[[MACRO-THRESHOLD-INCONSISTENCY-1]]`: `dedupe_new_rows()`の
     実害（Sahm Rule 930ヶ月中155件の反復値誤除外）を実データ・
     再現実験で確認し、監査側の既存ロジックとの統合で根治的修正。
     対応範囲は当初想定2指標から10指標へ拡大。YC閾値3セットは意図的
     設計と判断し統一せず理由を明記、新規`[[MACRO-TOOLTIP-THRESH-
     LABEL-MISMATCH-1]]`を発見・登録
  9. `[[TTM-SBC-QUARTERS-GAP-1]]`: GEV/HWM/TDYのRICE計算Q値過大評価
     （SBC部分四半期合計の誤用）を実測確認し修正
  10. `[[NORMALIZER-YTD-METADATA-STALE-1]]`: layer3_builder.py側の
      既存正実装を参照移植して解消、移植過程でlayer3_builder.py自身の
      オフバイワンバグも副次発見・修正
  11. `[[BACKTEST-SCORE-1]]`実装（TANUKI SCORE初の答え合わせ、着手
      条件充足を実データで確認）・`[[BS-FIELD-NEWLY-MISSING-2026-1]]`
      （LLY/SCCO/SPIR全3件が生涯フェードアウトと確定）・
      `[[MA-INTEGRATION-TAG-GAP-1]]`（長期停滞原因分析・新設計角度
      提案、実装は未着手）

  詳細はBACKLOG_DONE.md「2026-09-10（完了）」節・`CLAUDE_CODE_
  START.md`該当ブロック参照。新DB構築プロジェクトのコード・データには
  変更なし。現存BACKLOG.md総数は機械カウントで**60件**（67件から
  クローズ8件・新規登録1件で7件減）。

- **2026-09-12**（2026-09-10 23:03〈コミット`d6793202d1`〉以降の3件の
  指示書を実施、全てpush済み）:
  1. `[[MACRO-TOOLTIP-THRESH-LABEL-MISMATCH-1]]`実装（コミット
     `43bf5e03a1`）: RECESSION RISK SCORE 5指標のツールチップ表示閾値と
     実スコア計算ステップ関数の境界値の食い違いを修正。Michigan
     Consumer Sentimentは構造的に到達不可能な「BULL」ラベルを
     「NEUTRAL」へ変更
  2. `[[TAIL-SHARESDILUTED-Q4-TIMING-RISK-1]]`実装（コミット
     `03435b035d`）: shares_diluted取得のsource_tagフィルタを
     pipeline.py限定実装から共通アクセサ（layer3_builder.py）へ統合し、
     TANUKI TAILのQ4タイミング依存リスクを根治的に解消
  3. `[[MA-INTEGRATION-TAG-GAP-1]]`実装・本番反映（コミット
     `a9f507b3e9`・`a632b09745`）: 買収統合費用控除を二値ゲートから
     連続スケーリング（MA_ADDBACK_RAMP_BAND=0.20）へ変更し境界近傍銘柄
     （CSGP/ZETA等）の単発急変リスクを解消、未登録2タグも追加。**実装が
     承認取得前に一時未コミットのまま留め置かれる経緯があり、指摘を
     受けて是正**（詳細は`CLAUDE_CODE_START.md`該当ブロックに正直に
     記録）

  詳細はBACKLOG_DONE.md該当3節・`CLAUDE_CODE_START.md`該当ブロック
  参照。新DB構築プロジェクトのコード・データには変更なし。現存
  BACKLOG.md総数は機械カウントで**57件**（60件から3件減）。

- **2026-09-12②**（同日、上記に続く6件の指示書を順次実施、全て
  push済み）:
  1. `[[CRM-REVENUE-COGS-TAG-COVERAGE-GAP-1]]`実装・本番反映（コミット
     `eafa361844`・`4f7608d291`）: CRM(2018)のGP-COGS不整合の根本原因
     （CRM自身のFY2018申告タグ`SalesRevenueServicesNet`・
     `CostOfServices`が`parser.py`の候補リストに未登録で本人データ
     ではなくFY2019比較列を誤採用）を特定・修正。tanuki=true全99銘柄
     スキャンでCRMのみが対象と確認
  2. `[[EPS-LITE-ANNUAL-AS-QUARTERLY-1]]`実装・本番反映（コミット
     `aba7fcfcc5`・`4e90d7c77d`）: LITEのadjusted_eps異常値を調査した
     結果、登録時の当初仮説（net_income側の問題）は誤りで、EPS
     Analyzerの「Q4に年次調整項目タグを追加」ループが四半期申告皆無の
     タグを誤ってQ4値に採用する構造的バグ（74銘柄に波及、47銘柄で
     実値変化）と判明。承認を得て修正範囲を全調整項目タグへ拡大
  3. `[[SCENARIO-BEARBULL-SIGN-FLIP-1]]`実装・本番反映（コミット
     `acab509d1a`・`1a72a10cd6`）: `calculate_scenario_valuations()`の
     base_growth_rate負値時の符号逆転バグを修正、デッドコード2箇所も
     削除。あわせて調査中に発見した`tanuki_valuation/__init__.py`の
     相対import破損（実害なし・直接スクリプト実行で回避済み）を
     `[[TANUKI-VALUATION-INIT-RELATIVE-IMPORT-BROKEN-1]]`として記録のみ
     で新規登録（コミット`99a2fc6bd2`）
  4. `[[BETA-FALLBACK-DESIGN-GAPS-1]]`実装（コミット`6cfb0af3e7`）:
     β取得の3経路重複・0/負値無条件フォールバック等4項目を読み取り
     専用調査し全て実害ゼロと確認、案A（`calc_capped_beta()`への
     WARNログ追加のみ）でクローズ
  5. `[[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]`実装・本番反映
     （コミット`d179b5f0f0`・`8803455c56`）: STONKS SILOのYoY計算が
     fpラベル完全一致のみで照合し期間長の妥当性チェックを持たない
     バグを調査。登録時の「自然解消済み」という前提は誤りで、9銘柄
     （保有銘柄CRWV含む）で現在進行形の実例を確認し、期間長
     (330-400日)検証を追加して是正
  6. `[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]`据え置き更新・CHECK-47新設
     （コミット`db2b590319`・`2d069c3f34`）: 鮮度・実害を再確認し
     クローズはせず据え置き継続。APP(保有銘柄)FY2023年次revenueで
     parser.py側とLayer3側が約14.4億ドル乖離する実例（SEC遡及修正時の
     tie-break不一致）を新規発見し、構造的リスクが実例のある現実の
     リスクだと確認。この調査手法を`report_consistency_check.py`へ
     CHECK-47として恒常化、全99銘柄初回実行でWARN1件（RCAT、原因は
     normalized/側の四半期欠落、本番出力への実害なし）を検知

  詳細はBACKLOG_DONE.md「2026-09-12②〜⑥（完了）」該当5節・
  `CLAUDE_CODE_START.md`該当ブロック参照。新DB構築プロジェクトの
  コード・データには変更なし。現存BACKLOG.md総数は機械カウントで
  **54件**（57件からクローズ5件・新規登録1件で3件減）。

---

更新日: 2026-08-15（**フェーズ3「導出データ層の管理方法検討」完了**。
分類C14件のうち、登録済み5件（`INPUT-C-006/007`〈`[[DISCOVER-CONFIG-
DUAL-MGMT-1]]`〉・`INPUT-C-008`〈`[[PORTFOLIO-CONFIG-DUP-1]]`〉・
`INPUT-C-009`〈`[[TAILKPI-CONFIG-LOCATION-1]]`〉・`INPUT-C-010`
〈`[[FCFCONFIG-LOCATION-1]]`〉）は実装完了、残り9件（`INPUT-C-001〜005`・
`011〜014`）は調査の上「現状維持が妥当」と判断し完了とした（判断根拠は
`INPUT_DATA_TOBE.md`分類C表の各行注記参照）。`_meta`スキーマの標準化
方針は`NAMING_CONVENTIONS.md`規則8に新規策定（既存ファイルへの遡及
適用はしない）。調査過程で発見した死蔵ページ1件を`[[EPSANALYZER-
ADMIN-ORPHAN-PAGE-1]]`として新規登録、既存`[[RPO-ADMIN-1]]`に
`_meta`欠如の追記を実施。**これにより新DB構築プロジェクトのフェーズ
1〜3が全て完了した**。次の本線は未定（次セッション開始時に
BACKLOG.mdの優先順位に従い判断すること。CHAT_RULES.md「本線の定義」
節も参照し、必要なら更新する）。詳細はBACKLOG_DONE.md「2026-08-15
（完了）」参照）
更新日: 2026-08-15（フェーズ3のSEC EDGAR由来4件（AS-IS-129・266・273・
395）の検証を完了。いずれも意図的にLayer3化対象外と確定済みで、
`FIELD_DEFINITIONS.md`の記載も現状と一致していることを確認した
（AS-IS-129は`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`に
よりSTONKS SILO `fetcher.py`が現状維持確定、AS-IS-266・273はEPS
Analyzerの独立ライブ取得設計、AS-IS-395はTANUKI TAILのリアルタイム
監視専用ファイルのためフェーズD対象外）。AS-IS-129のみ、フェーズD
対象外である旨の軽量注記を追加。**これにより、新DB構築プロジェクトは
消費者ファイル単位・重複計算パターン単位・フィールド単位（yfinance/
FRED由来18件＋SEC EDGAR由来4件）の全ての粒度で完了確認が完了した**。
詳細はBACKLOG.md`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照。実装
コード変更なし）
更新日: 2026-08-15（フェーズ3「`FIELD_DEFINITIONS.md`499項目の新DB
参照への切替方針」の投資調査完了を反映。499項目中、yfinance/FRED由来
の一次データ18件が全件`common/market_data/`・`common/macro_data/`へ
切替済みであることを実コードベースで確認した（`FIELD_DEFINITIONS.md`
側の記載は2026-07-22時点のまま更新されていなかったため、該当行へ
切替済み注記を追加）。SEC EDGAR由来4件はsec_data側フェーズDでの対応
状況が本調査では未検証のまま残る。これにより、新DB構築プロジェクトの
sec_data/market_data/macro_data本線タスクは、消費者ファイル単位に
加えフィールド単位でも実質的な完了を確認できた。詳細はBACKLOG.md
`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照。実装コード変更なし）
更新日: 2026-08-13（セッション終了時ブラッシュアップ。フェーズ1
`common/market_data/`・`common/macro_data/`両行に、未追跡のまま残って
いた切替2件の完了を追記：`collect_and_send.py::collect_asset_flow()`の
SHV等6資産切替（`[[MARKETDATA-COLLECT-ASSET-FLOW-UNTRACKED-1]]`）・
`backfill_tech_pulse.py`のVXNCLS切替（`[[MACRODATA-BACKFILL-TECH-
PULSE-VXNCLS-UNTRACKED-1]]`）。これにより両コンポーネントの本線タスクは
完全に完了。また重複計算パターン4件（`[[NETCASH-DUAL-CALC-1]]`・
`[[NETINCOME-DUAL-PIPELINE-1]]`・`[[RULE40-DEFINITION-MISMATCH-1]]`・
`[[FRED-HYSPREAD-TRIPLE-FETCH-1]]`）を解消し、新DB構築プロジェクトの
sec_data/market_data/macro_data本線は全て完了。残るのは`[[ERP-DUAL-
CALC-1]]`等の本線外・低優先度課題群とフェーズ3（499項目切替）のみ。
詳細はBACKLOG_DONE.md「2026-08-13（完了）」参照。実装コード変更なし）
更新日: 2026-08-12（ブラッシュアップで発見したフェーズ3セクションと
BACKLOG.mdとの矛盾を訂正。フェーズ3表の2行が「未着手（フェーズ1・2
完了後に着手）」のまま残っており、BACKLOG.md側の「フェーズ3着手済み
（分類C3件登録完了）」という記載と食い違っていた。`FIELD_DEFINITIONS.md`
499項目の新DB参照への切替方針の行へフェーズ3投資調査の結果
（`common/sec_data/`13箇所参照済み・`common/market_data/`/`common/
macro_data/`は0件、次のアクションは対象件数を数える調査から）を反映。
分類C14件の管理方法検討の行を「着手済み」に更新し、登録済み3件
（`[[PORTFOLIO-CONFIG-DUP-1]]`・`[[TAILKPI-CONFIG-LOCATION-1]]`・
`[[FCFCONFIG-LOCATION-1]]`）・残り11件の内訳を明記。見出しにも
「2026-08-12着手」を追加（フェーズ1・2見出しと同形式）。実装コード
変更・BACKLOG.md側の変更なし）
更新日: 2026-08-12（`[[PHASE2-YFINANCE-REFETCH-DESIGN-1]]`（yfinance
過去データ移管の投資調査）が完了。旧保存先（hypecore・
`market_data.json`・`breadth_data.json`）の派生指標の元となる価格・
出来高の生データが、`common/market_data/daily/`へ既に2021-01-04〜の
深さで保存済みであり、必要な期間（2021年〜／2026年4月〜）を十分に
カバーしていることを確認。**yfinanceはフェーズ2「過去データ移管」の
対象外と確定、移行作業不要**。これによりフェーズ2の4データソース
（SEC EDGAR・yfinance・FRED・取得前提条件）全てに結論が確定し
**フェーズ2が実質完了**。フェーズ2表・冒頭サマリー段落を更新。
次の優先タスクをフェーズ3検討・本線外課題群の対応要否判断に更新。
実装コード変更・データ変更なし）
更新日: 2026-08-12（`[[PHASE2-SECDATA-FULL-DEPTH-VERIFICATION-1]]`
（SEC EDGAR全105銘柄の履歴深度精査）が完了。`company_facts.json`と
`annual_*.json`の最古日付を機械的に突合した結果、104銘柄で有意な
深度差なし（-1〜+1年のノイズ帯）を確認し、**SEC EDGARはフェーズ2
「過去データ移管」の対象外と確定**。異常ケースとしてENB（Enbridge）の
`annual_*.json`/`quarterly_*.json`が1件も生成されていないことを発見し
`[[SECDATA-ENB-NORMALIZATION-MISSING-1]]`として新規登録（原因調査は
次回以降）。フェーズ2表SEC EDGAR行を更新、フェーズ2の残タスクは
`[[PHASE2-YFINANCE-REFETCH-DESIGN-1]]`1件のみに整理。実装コード変更・
データ変更なし）
更新日: 2026-08-12（セッション終了時ブラッシュアップで発見した本ファイル
内部の矛盾を訂正。フェーズ2表FRED行（449行目付近）が「実施済み」と
記載する一方、直後の説明文が方針決定コミット時点の「実装は次段階に
分離」のまま`BAMLH0A0HYM2`実装完了後も更新されていなかった。実装完了
の事実を反映し、SEC EDGAR・yfinance分の残タスクを新規登録した
BACKLOG.md`[[PHASE2-SECDATA-FULL-DEPTH-VERIFICATION-1]]`・
`[[PHASE2-YFINANCE-REFETCH-DESIGN-1]]`への参照も追加。詳細は
BACKLOG_DONE.md`[[BACKLOG-STALE-NEXTSTEPS-BLOCK-MACRODATA-2]]`参照。
実装コード変更なし）
更新日: 2026-08-12（`[[MACRODATA-BAMLH0A0HYM2-HISTORY-EXCEPTION-1]]`の
実装・実行が完了。`common/macro_data/migrate_bamlh0a0hym2_history.py`
（一度限りの例外的移行専用スクリプト、`common/macro_data/`配下に監査
証跡として恒久残置）を新規実装し、旧`05_events.csv`のHY Spread行
（`indicator == "HY Spread"`）から`2023-08-14`より前の6,947件を
`common/macro_data/series/BAMLH0A0HYM2.json`へ追加投入（移行前785件→
移行後7,732件）。`2023-08-14`以降の既存レコードはサンプル5件の値
突合で無変化を確認、`as_of`の重複0件・昇順整列済みを確認、保存前検証
（`fetcher.py::_validate_incoming_batch`再利用）で警告0件、二重実行
防止ガードの動作も確認。pytest 793 passed / 2 known-failed
（`[[TEST-STALE-IV-1]]`）で回帰なし。フェーズ2表FRED行の備考を
「実施予定」→「実施済み」に更新。`[[MACRODATA-BAMLH0A0HYM2-HISTORY-
EXCEPTION-1]]`をBACKLOG.mdからBACKLOG_DONE.mdへ移動。詳細は
BACKLOG_DONE.md「2026-08-12（完了）」参照）
更新日: 2026-08-12（フェーズ2「過去データ移管」の移行方針を確定・記録。
原則は再取得・再導出、データ提供元が恒久的にAPI提供範囲を制限しており
再取得が技術的に不可能な場合に限り旧保存先からの例外的移行を許容する
方針とした。文書ベース調査では移管の定義・完了判定基準がいずれも
文書上未定義と判明し、続く実データ検証で「移管が必要なのはSEC EDGAR
のみ」という当初仮説を反証（yfinance旧保存先の一部・FRED旧`05_events.
csv`にも数ヶ月〜数十年分の時系列が存在）。common/macro_data/の履歴深度
実測でBAMLH0A0HYM2のみ著しく浅い（785件、2023-08-14〜）ことを発見し、
原因調査の結果FRED側が2026年4月から提供範囲を直近3年に制限したことが
根本原因と判明（取得側のバグではないことをコード読解・違反ログ実測で
確認済み）。全25系列への横展開調査で同種の新規制限に該当するのは
BAMLH0A0HYM2のみと確認。SEC EDGAR（3銘柄サンプル比較、有意差なし）・
yfinance（一部旧保存先が数ヶ月〜32ヶ月分の時系列を保持）・FRED・
取得前提条件（対象外）の現状評価をフェーズ2表の備考欄へ反映。
`[[MACRODATA-BAMLH0A0HYM2-HISTORY-EXCEPTION-1]]`をBACKLOG.mdへ新規
登録（未着手、実装は次段階）。実装（実際のデータ投入等）は行っておらず
方針決定・記録のみ。詳細はBACKLOG_DONE.md`[[PHASE2-MIGRATION-POLICY-
DECIDED-1]]`・BACKLOG.md`[[MACRODATA-BAMLH0A0HYM2-HISTORY-
EXCEPTION-1]]`参照）
更新日: 2026-08-12（`common/macro_data/`の本番消費者2ファイル
（`05_main.py`・`collect_and_send.py`）を`common.macro_data.reader`経由へ
全面切替し**完成**。状態を「構築中」から「完成」に更新（`common/
market_data/`が全消費者切替完了時に「完成」とした前例に倣う）。重複3系列
（`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`）は`reader.get_latest()`への集約で
解消。単一最新値だけでは機能を維持できない5箇所（NFP前月比・VXN
MA50・HYスプレッド90日min/max・DGS3MO前日比・S&P500複数日履歴）は
`reader.get_series()`（期間指定）を使用。切替前後で18項目の値突合を
実施し全項目完全一致（差分0件）を確認。`Fred(`・`fred_latest(`等の
直接呼び出しをgrep最終確認で0件に。リトライ・指数バックオフロジックも
削除。新規テスト計50件を追加・更新、pytest全体771 passed / 2
known-failedで回帰なしを確認。フェーズ1表該当行・冒頭サマリー段落を
更新。詳細はBACKLOG.md`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照）
更新日: 2026-08-12（`common/macro_data/`の定期取得ワークフロー
`.github/workflows/Macro_Data_Update.yml`（毎日UTC10:00・
workflow_dispatch対応）を新設し稼働開始。既存の`MACRO_PULSE_
Update.yml`（最速`3 13 * * *`＝UTC13:03）・`Market_Pulse_Update.yml`
（`35 21 * * 1-5`）より確実に先行する時刻に設定。GitHub Actions側の
workflow_dispatchをこのセッション環境から直接トリガーする手段（`gh`
CLI・トークン）がなかったため、同一エントリポイントをローカル環境の
実`FRED_API_KEY`で実行し代替検証。25系列中24系列成功・`series/`へ
初回実データ投入（約9.5万レコード）、`FTSD`のみFRED上に系列が実在せず
失敗と判明（`[[MACRODATA-FTSD-SERIES-ID-INVALID-1]]`新規登録）。
`macro_data_violations_log.json`の警告255件はサンプル確認の結果
実在する経済事象由来と判断（データ品質問題なし）。副次発見として
日次cronが毎回全期間履歴を再取得する非効率設計も判明
（`[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]`新規登録）。
`05_main.py`・`collect_and_send.py`は今回も変更していない。フェーズ1表
該当行・冒頭サマリー段落を更新。詳細はBACKLOG.md`[[MACRODATA-LAYER-
CONSTRUCTION-1]]`参照）
更新日: 2026-08-12（`common/macro_data/`（FRED統合層）の`fetcher.py`/
`reader.py`本体を実装。状態を「設計確定（実装未着手）」から「構築中」に
更新。新規モジュール構築のみが今回のスコープであり、`05_main.py`・
`collect_and_send.py`側の本番消費者切替（重複3系列解消含む）・GitHub
Actionsワークフロー新設・過去データ一括投入（フェーズ2）はいずれも
今回変更していない（次段階）。新規テスト`tests/test_macro_data_
fetcher.py`・`tests/test_macro_data_reader.py`（計43件）を追加、
pytest全体で既知失敗2件〈TEST-STALE-IV-1〉以外の回帰なしを確認。
フェーズ1表該当行・冒頭サマリー段落を更新。詳細はBACKLOG.md
`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照）
更新日: 2026-08-12（`common/macro_data/`（FRED統合層）の実装設計を確定。
状態を「投資調査完了（実装未着手）」から「設計確定（実装未着手）」に
更新。保存形式JSON確定（`common/market_data/`と統一）・
`series_meta.json`新設・`fetcher.py`/`reader.py`のAPI・重複3系列
（`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`）の`reader.py`一本化方針を確定し、
`INPUT_DATA_TOBE.md`/`INPUT_DATA_AS_IS.md`へ反映。`FTSD`を
`INPUT-A-049`として両ファイルへ追加し分類A件数48件→49件・合計
65件→66件に更新。機械的網羅性証明の再実行で`INPUT-A-048`の
`INPUT_DATA_AS_IS.md`側反映漏れ〈2026-07-24時点の既存の乖離〉も発見・
解消し66件・差分0件を確認。「一次データ層の総数」表・フェーズ1表
該当行を更新。`[[MACRODATA-FTSD-MISSING-FROM-INVENTORY-1]]`は本対応で
解消しBACKLOG_DONE.mdへ移動。詳細はBACKLOG.md`[[MACRODATA-LAYER-
CONSTRUCTION-1]]`参照。実装コード変更・データ再生成なし）
更新日: 2026-08-12（`common/market_data/`（yfinance統合層）が診断ツール
2ファイル（`audit.py`・`score_verifier.py`）・周辺ツール2ファイル
（`extract_key_facts.py`・`backfill_tech_pulse.py`）の切替完了により
**全12ファイル（本番消費者8＋診断ツール2＋周辺ツール2）が完了**。状態を
「構築中」から「完成」に更新。続けてフェーズ1の次コンポーネント
`common/macro_data/`（FRED統合層）の新設事前調査（FRED消費者洗い出し、
`MIGRATION_CHECKLIST.md`Step1相当）を実施し、状態を「未着手」から
「投資調査完了（実装未着手）」に更新。フェーズ1表該当2行・冒頭サマリー
段落を更新。詳細はBACKLOG.md`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・
`[[MACRODATA-LAYER-CONSTRUCTION-1]]`・BACKLOG_DONE.md「2026-08-12
（完了）」参照。実装コード変更なし）
更新日: 2026-08-11（`common/market_data/`本番消費者8ファイル**8/8切替
完了**を反映。3/8時点から追加で`pipeline.py`（.calendar）・`collect.py`
（Discover）・`collect_and_send.py`（Market Pulse）・
`breadth_calculator.py`・`hypecore.py`（daily/attributes/analyst_history
の3層混在、前提作業3件込みで最複雑）が完了し全数切替が完了。フェーズ1表
該当行・冒頭サマリー段落を更新。詳細はBACKLOG.md`[[MARKETDATA-LAYER-
CONSTRUCTION-1]]`・BACKLOG_DONE.md「2026-08-11（完了）」参照。実装
コード変更なし）
更新日: 2026-08-11（`common/market_data/`を「未着手（投資調査・設計確定済み）」
から「構築中」に更新。`fetcher.py`・`reader.py`・Daily/Weekly Update
workflows実装完了、本番消費者8ファイル中3/8〈`beta_fetcher.py`・
`data_fetcher.py`・`valuation_fetcher.py`〉切替完了を反映。フェーズ1表
該当行を更新。詳細はBACKLOG.md`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・
BACKLOG_DONE.md「2026-08-11（完了）」参照。実装コード変更なし）
更新日: 2026-08-07（`common/market_data/`の状態表記を「未着手」から
「未着手（投資調査・設計確定済み）」に更新。`[[MARKETDATA-LAYER-
CONSTRUCTION-1]]`の3原則照合完了を反映し、フェーズ1表・冒頭サマリー
段落を修正。あわせてフェーズ2表のyfinance既存データ行「実測11
ファイル」を「実測12ファイル」に訂正（`[[MARKETDATA-AS-IS-AUDIT-
PY-OMITTED-1]]`根拠、他ドキュメントには反映済みだったがPROJECT_
STATUS.mdへの反映漏れを本更新で解消）。実装コード変更なし）
更新日: 2026-08-07（フェーズD Step2-2〜2-5実質完了を反映し、
`common/sec_data/`統合の状態を「構築中」→「完成（実質完了）」に更新。
Step2-2（②STONKS SILO）・Step2-3（③TANUKI TAIL）・Step2-4
（④HypeCore）の実装完了、Step2-5（⑤stock.html＋診断・補助スクリプト
7件）は投資調査の結果ほぼ切替対象なしと判明し実質完了。並行して
保留中だった2判断（`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-
MISMATCH-1]]`・`[[STOCKHTML-LAYER3-PUBLISH-PIPELINE-MISSING-1]]`）は
いずれも現状維持・着手見送りで確定し、`normalized/`が`fetcher.py`・
`dcf_validity_checker.py::check_c_data_jump()`・stock.htmlの3系統
向けに恒久的に存続する設計とした。フェーズE（`normalized/`完全廃止）は
この3系統が残る限り着手不可と判定。次の優先タスクを`common/
market_data/`・`common/macro_data/`新設への着手検討に更新。詳細は
BACKLOG.md`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照）

更新日: 2026-08-06（フェーズD Step1〈アクセサのラッパー化〉・Step2-1
〈TANUKI VALUATION本体切替〉完了を反映。事前バグ修正2件
〈`[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]`・`[[LAYER3-ANNUAL-
MISCLASSIFICATION-BBAI-1]]`〉→`pipeline.py`6箇所切替→100銘柄全数
回帰確認の順で実施。本書下部の残タスク欄を更新。詳細はBACKLOG_DONE.md
「2026-08-06（完了）」参照）

更新日: 2026-08-06（`SECDATA-STORAGE-FRAGMENTATION-1`と`SEC_EDGAR_LAYER_
DESIGN.md`フェーズDが5消費者の移行先（Layer3 vs data/）で1ヶ月弱
食い違ったまま併存していた問題を投資調査で発見・修正。本書下部の
common/sec_data統合フェーズ1備考欄「残タスク」を、`SEC_EDGAR_LAYER_
DESIGN.md`フェーズD（Layer3統合）方向へ統一した。詳細は`BACKLOG.md`
`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照）

更新日: 2026-08-05（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]] Stage 1
完了。`common/sec_data/`一次データ取得層の「フィックス」機構
（`fixed_registry.json`、検証済み銘柄×年度を以後の抽出ロジック変更の
対象外とする仕組み）の運用方針確定・スキーマ設計・実装・検証まで
完了した。taxonomy属性①〜⑧（SPAC上場・決算期変更・M&A直後・非継続
事業・IPO前・業界特有会計慣行・標準タグ外れ・原因不明）非該当26銘柄・
372銘柄×年度エントリを`fixed_by: checkgate_pass`で登録、`parser.py`
（`_apply_fixed_registry_freeze()`、差分適用方式）・`report_
consistency_check.py`（CHECK-31/WARN-31、NG化）を実装。全105銘柄再
パースでフィックス対象含め全出力が無変化であることを確認済み。
機能コミット`7c15b2a75`・BACKLOG更新コミット`ae88715c5`（push済み）。
残タスク: taxonomy属性該当58銘柄のStage 2〜3（段階的フィックス拡大）。
詳細はBACKLOG_DONE.md「2026-08-05（完了）」・BACKLOG.md該当項目参照）

更新日: 2026-08-02（セッション終了処理。common/sec_data/統合フェーズ1の
備考欄に、セッション最終盤で発見した最重要事項を反映: TTM系列生成
パイプライン`layer3_builder.py`が`parser.py`（annual_YYYY.json生成）
とは完全に独立した別実装であり、annual側の修正（`[[PERIOD-LENGTH-
VALIDATION-GAP-1]]`・`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`・
`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`等7件）が構造的に
TTM側へ反映されないという設計上の発見（`[[TTM-DATA-DRIFT-BEHIND-
PIPELINE-1]]`）を登録。影響実測の結果、TANUKI VALUATION・STONKS SILO
への現在進行形の実害はゼロと確定し優先度を高→中に引き下げたが、構造的
脆弱性自体は温存されている旨を申し送り事項として明記。「次セッション
での着手順序」欄を最終整理。詳細はBACKLOG.md/BACKLOG_DONE.md該当項目
参照）

更新日: 2026-08-02（セッション終了処理。common/sec_data/統合フェーズ1の
備考欄を更新。`[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]`が
提起した4種の恒等式違反すべての分類調査が完了し同エントリをクローズ
（TA=TL+SE分は先行実装済み、GP≠Revenue−COGS/OI>GP/NI≠EPS×Sharesの
残る3種も本セッションで分類調査完了）。GOOGL(2012/2013)のGP≠Revenue−
COGSは`[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]`として実装完了（案A
採用、`_apply_fact_overrides()`実行順序修正、機能コミット`ba8628198`・
データコミット`dd6fba1a1`）。COHR(2009-2011)のNI≠EPS×Sharesは
`[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]`として対応方針確定（
`fact_overrides.json`個別上書き、実装は次回）。同調査から派生した
`[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]`（未文書化tie-break欠陥）は
全母集団シミュレーションで広範な設計変更は危険と判明したため不採用とし
`[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]`へガード条件付き介入として
統合。「次セッションでの着手順序」欄を最終整理。詳細はBACKLOG.md/
BACKLOG_DONE.md該当項目参照）

更新日: 2026-08-02（セッション終了処理。common/sec_data/抽出アーキテク
チャの俯瞰的脆弱性分析から`docs/architecture/new_data_platform/
EXTRACTION_DESIGN_PRINCIPLES.md`（新規データ層向け抽出設計原則）を新設。
`[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]`（会計恒等式TA=TL+SE
の横断検証レイヤー）を実装完了し、続くHEI・ONDS型許可リスト拡張
（[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]）と合わせてTA=TL+SE違反156件
中139件（89.1%）を解消。残る17件はCOHR2件
（[[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]）・その他15件
（[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]）に分けて継続調査。

更新日: 2026-08-02（セッション終了処理。`[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]`
パターンB実装前シミュレーション→`[[RCAT-TTM-SERIES-CONTINUING-
DISCONTINUED-UNCHECKED-1]]`根本原因調査を実施（いずれも読み取り専用の
調査・BACKLOG登録のみ、実装なし）。

**確定した内容**: RCATの本番FCF計算は`reader.py::get_fcf_5yr_avg()`
（年次ファイルベース）ではなく`data_fetcher.py::_select_fcf_source()`が
優先するTTM系列（`common/sec_data/ttm/RCAT_ttm_series.json`）経由である
ことが判明。年次パーサー側の継続/非継続事業分割タグ問題
（`[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]`）を年次パーサー
のみに実装してもRCATのIV・Classificationは変化しない（ΔIV=$0）ため、
`[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]`の優先度を「高→低」に訂正。

根本原因調査の結果、より深刻で現在進行形の実害を持つ別バグを発見:
`ttm_calculator.py::calc_ttm_series()`が採用四半期の日付連続性を検証せず
「アンカー日以前の直近4件」を単純採用する設計欠陥（ticker非依存の一般的
欠陥）により、RCATでは2023年7〜10月・10月〜2024年1月の四半期が
`ttm_end=2025-03-31`・`2026-03-31`の両方に重複使用され、現在の
`fcf_5yr_avg`（-40,185,008.5）・`fcf_2yr_avg`（-50,540,837.0）が正しい値
（試算：約-53,985,212・約-78,141,244）より34〜55%過小評価と確定。ただし
IVへの影響は現時点でΔIV=$0（revenue floor＋EPSベース推定オーバーライド
が吸収、将来業績改善時に顕在化しうる潜在リスクの留保付き）。他銘柄
（HON/AVAV/TER）への現時点の実害なしと確認。

根本原因（ticker非依存の一般的欠陥）を`[[TTM-CALC-QUARTER-CONTIGUITY-
UNCHECKED-1]]`として新規登録（優先度：中〜高）。

**次回最優先タスク**: `[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]`
〈優先度：中〜高、calc_ttm_series()の日付連続性チェック欠如。まず105銘柄
全体での該当有無の横断スキャンから着手〉。

詳細はBACKLOG.md/BACKLOG_DONE.md該当項目参照）

更新日: 2026-08-02（セッション終了処理。2026-08-01〜02の2日間にわたり
gross_profit調査を発端に波及した一連のデータ品質是正作業の最終サマリを
反映。

**完了・クローズ項目（全16件）**: `[[PERIOD-LENGTH-VALIDATION-GAP-1]]`・
`[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]`・`[[SPAC-STUB-PERIOD-FIELD-
SPLIT-1]]`・`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`段階1・段階2・`[[LAYER3-
GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]`①・`[[STONKS-SILO-FETCHER-
GROSSPROFIT-BACKFILL-DUP-1]]`・`[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-
KULR-1]]`（`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`へ統合）・
`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`（278件是正実装完了）・
`[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]`（解消・実害なし）・
`[[SPAC-STUB-PERIOD-VERIFICATION-1]]`（解消・11銘柄すべて妥当と確認）・
`[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]`一部（MO/PM/
SCCOをgenuine定義差と確定）・`[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`
案b（LRCX(2010)是正実装完了、CRM/JNJ/MRVL/ONDS型は残存）・
`[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]`（案③WARN-28実装完了）・
`[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]`（`[[OPERATING-CASH-FLOW-
CONTINUING-DISCONTINUED-GAP-1]]`へスコープ拡大・統合）。

**次回最優先タスク**: `[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]`〈優先度：高、
現在進行形のDCF計算実害。RCATの`get_fcf_5yr_avg()`が実質2021-2023年の
3年平均になっており、真により大きな悪化を示す2024/2025年〈特に-$89.1M〉
が欠落。`[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]`のRCAT分
〈パターンB、継続+非継続の合算〉解決が前提〉。残る25銘柄中24銘柄は該当
年度が現在の直近5年窓の外にあり実害なしと確定済み。

詳細はBACKLOG.md/BACKLOG_DONE.md該当項目参照）

更新日: 2026-08-02（セッション終了処理。2026-08-01〜02セッション全体
〈gross_profit調査発端の一連の作業〉のサマリを反映。

**完了・クローズ項目**: `[[PERIOD-LENGTH-VALIDATION-GAP-1]]`・
`[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]`・`[[SPAC-STUB-PERIOD-
FIELD-SPLIT-1]]`・`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`段階1・段階2
（完了）・`[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]`①・
`[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]`（実害解消済み、
コード整理は将来検討）・`[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]`
（`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`へ統合）・
`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`（貸借対照表恒等式
逆算によるtotal_liabilitiesバックフィルを実装、278件是正・全105銘柄
フローズン入力比較で対象外無変化を確認）・`[[RCAT-TRIPLE-FISCAL-
CHANGE-SUSPECTED-1]]`（解消・実害なし、3段階目の決算期変更は存在せず）・
`[[SPAC-STUB-PERIOD-VERIFICATION-1]]`（解消・11銘柄すべて現状の処理が
妥当と確認）・`[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]`
一部（MO/PM/SCCOの3銘柄をgenuine定義差と確定・クローズ）。

**新規発見・残存タスク（次セッションでの着手順序、優先度順）**:
①`[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`〈中〜高、revenue/cost_of_
revenue/gross_profitが異なるaccn・会計年度から独立採用される設計欠陥。
CRM/JNJ/MRVLで確定、残り6銘柄〈AMD/BSY/KO/LRCX/ONDS/RMBS〉は要個別確認〉
②`[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]`〈中、fetcher.pyのrelevant_
formsに10-KT・10-QTが含まれず本人データが採用されない。現在進行形の実害は
解消済みだが将来の再発リスクとして監視対象〉③`[[RCAT-OCF-CONTINUING-
DISCONTINUED-SPLIT-1]]`〈中、RCATのoperating_cash_flow欠落〉
④`[[LITE-COGS-DA-TAG-UNMERGED-1]]`〈低〜中、LITEのCOGS由来償却費タグ
未合算〉⑤`[[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]`〈低〉
⑥`[[ELF-ROE10YR-RECALC-PENDING-1]]`〈中、TANUKI VALUATION定期更新で
自然解消見込み〉⑦`[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-
MISSING-1]]`〈低〜中〉⑧`[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-
DUP-1]]〈低、クローズ済み・コード整理のみ将来検討〉。

詳細はBACKLOG.md/BACKLOG_DONE.md該当項目参照）

更新日: 2026-08-02（セッション後半。`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`
段階2〈formerNames区間一致によるSPAC合併疑いの機械的検知〉が完了し、
同エントリはBACKLOG_DONE.mdへ全文移動（段階1・段階2とも完了）。副産物
として新規登録した`[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]`
〈KULR(2019)単独〉は、根本原因調査（読み取りのみ）で
`XBRL_MAPPING["total_liabilities"]`の2番目のフォールバック候補
`LiabilitiesAndStockholdersEquity`〈定義上`total_assets`と数学的に一致する
誤った代替タグ〉が原因と確定。予備スキャンで105銘柄中278件（銘柄年度、
AMZN・GOOGL・MSFT・NVDA等の大型株を含む）に及ぶ横断的な候補タグ設計欠陥と
判明したため、KULR単独対応は不要と判断してクローズし、規模の大きい横断課題
`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`〈優先度：高、対応方針
未定・実装未着手〉として新規登録・統合した。downstream影響調査により
Net_Debt/Total_Debt算出への直接汚染はないことを確認済み（診断WARN
メッセージでの消費のみ）。詳細はBACKLOG.md/BACKLOG_DONE.md該当項目参照）

更新日: 2026-08-02（2026-08-01〜02セッションで`common/sec_data/`統合
フェーズ1の一次データ抽出品質に関わる残課題6件が解消。①`[[PERIOD-
LENGTH-VALIDATION-GAP-1]]`〈parser.pyのFLOW型フィールド抽出に期間長検証
340-380日を追加、9銘柄のgross_profit等の四半期→年次誤採用を是正〉、
②`[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]`〈era別fiscal_end_month/
anchor対応、ELFの2015-2018年度データを是正〉、③`[[SPAC-STUB-PERIOD-
FIELD-SPLIT-1]]`〈BBAI/RDW/ELF/KULRのSPAC・predecessor/successor期間
混在を個別調査、対応不要と確認〉、④`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`
段階1〈BS instant factの法的実体混在によるcurrent_assets>total_assets
等の数学的矛盾を7銘柄7年度で解消。段階2〈SPAC合併疑いの機械的検知〉は
未着手で残存〉、⑤`[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]`①
〈gross_profitのrevenue-cost_of_revenue逆算フォールバックを本番
annual_YYYY.jsonへ実装、34銘柄342件を書き戻し〉、⑥`[[STONKS-SILO-
FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]`〈⑤の効果でSTONKS SILO側の重複
補完ロジックが実質デッドコード化、クローズ〉が完了。新規発見の残存事項:
`[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]`〈KULR(2019)、同一filing内
でのcandidate tag誤選択、entity混在ではない別原因と確定〉・
`[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]`〈14銘柄49件へ
対象拡大、会計上の定義差または未解消バグの疑い〉・
`[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]`〈RCAT直近10-Kの決算期変更
再発疑い〉・`[[ELF-ROE10YR-RECALC-PENDING-1]]`〈TANUKI VALUATION定期
更新待ち〉。詳細はBACKLOG.md/BACKLOG_DONE.md該当項目参照）

更新日（2026-07-24分、履歴として保持）: 一次データベース設計の投資調査で
判明した3件の実態を反映。①INPUT-A-016〈セグメント別売上・KPI〉を正式
ASC280セグメントから`tail_kpi_map.json`ベースの銘柄固有カスタムKPI
〈フェーズ1統合スコープ外〉に訂正、②Adjusted EPS算出専用の税務・一過性
項目タグ群52種を`INPUT-A-048`として新規追加、③`common/sec_data/data/
{TICKER}/company_facts.json`〈SEC EDGAR company_facts API生レスポンス
全量、既存〉がLayer1（無加工アーカイブ）の要件を既に満たしていることが
判明し、新規構築不要と判明。分類A件数を47件→48件に更新。詳細は
`INPUT_DATA_TOBE.md`該当箇所・BACKLOG.md
`[[SECDATA-COMPANYFACTS-OVERLOOKED-1]]`参照）
位置づけ: 「新一次データベース構築プロジェクト」（2段階プロジェクトの
第1段階＝一次データ層の構築・過去データ移管、第2段階＝導出データ層
〈`FIELD_DEFINITIONS.md`499項目〉の管理方法検討）の進捗を追跡する。
仕様書本体は`docs/architecture/new_data_platform/`を参照。

2026-07-24〜2026-08-07にかけて`common/sec_data/` 統合（フェーズ1の
一部）を実施し、**2026-08-07に実質完了**（`normalized/`は3系統向けの
恒久的な設計上の例外を除き全消費者がLayer3へ切替済み、詳細は下記
表・`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照）。
`common/market_data/`（yfinance統合層）は2026-08-12時点で**完成**
（`fetcher.py`/`reader.py`実装・定期実行ワークフロー2件・本番消費者8＋
診断ツール2＋周辺ツール2の**全12ファイル切替完了**、詳細は下記表・
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`参照）。続けて`common/macro_data/`
（FRED統合層）も新設事前調査（FRED消費者洗い出し、`MIGRATION_
CHECKLIST.md`Step1相当）・実装設計確定・`fetcher.py`/`reader.py`本体
実装・定期取得ワークフロー`Macro_Data_Update.yml`新設に続けて、本番
消費者2ファイル（`05_main.py`・`collect_and_send.py`）を`reader`経由へ
全面切替し**完成**（重複3系列〈`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`〉
解消・値突合18項目完全一致・grep最終確認で直接呼び出し0件、詳細は
下記表・`[[MACRODATA-LAYER-CONSTRUCTION-1]]`参照）。続けてフェーズ2
（過去データ一括投入）の4データソース（SEC EDGAR・yfinance・FRED・
取得前提条件）全てに投資調査を実施し、2026-08-12に**フェーズ2が実質
完了**（SEC EDGAR・yfinance・取得前提条件はフェーズ2対象外と確定、
FREDは`BAMLH0A0HYM2`1系列のみ例外的移行を実施済み・他23系列は対象外、
詳細は下記フェーズ2表参照）。**次の優先タスクはフェーズ3（導出データ層
の管理方法検討）への着手検討**、または本線外の低優先度課題群
（`[[MACRODATA-FTSD-SERIES-ID-INVALID-1]]`・`[[SECDATA-ENB-
NORMALIZATION-MISSING-1]]`等）の対応要否判断。

## 一次データ層の総数（`INPUT_DATA_TOBE.md`3分類、2026-08-12時点）

| 分類 | 件数 | ID範囲 | フェーズ1・2のスコープ内か |
|---|---|---|---|
| A. 一次データ本体 | 49件 | `INPUT-A-001`〜`049` | **対象**（一次データ層構築の主対象） |
| B. 取得前提条件 | 3件 | `INPUT-B-001`〜`003` | **対象**（SEC EDGAR取得〈`INPUT-B-002`/`003`〉・全体の対象銘柄決定〈`INPUT-B-001`〉の前提として、分類Aの取得と一体で構築する） |
| C. 導出データの入力 | 14件 | `INPUT-C-001`〜`014` | **対象外**（一次データそのものではなく`FIELD_DEFINITIONS.md`導出データ側の入力のため、フェーズ3〈導出データ層の管理方法検討〉で扱う） |
| **合計** | **66件** | — | — |

---

## フェーズ1: 一次データ層の構築（分類A48件＋分類B3件が対象）

| コンポーネント | 状態（未着手/構築中/完成） | 備考 |
|---|---|---|
| `common/sec_data/` 統合（raw/normalized/ttm統合含む、`INPUT-A-001〜018`対応） | 完成（実質完了。着手日2026-07-24、フェーズD実質完了日2026-08-07。`fetcher.py`・`dcf_validity_checker.py::check_c_data_jump()`・stock.htmlの3系統向けに`normalized/`が恒久的に存続する設計上の例外あり、詳細は下記備考） | `INPUT_DATA_TOBE.md` 2-A参照。統合スコープに`raw/`・`normalized/`・`ttm/`の3系統を含む旨を明記済み。`SEC_EDGAR_LAYER_DESIGN.md`のフェーズA〜C（Layer3スキーマ構築・`layer3_builder.py`実装・`ttm_calculator.py`snake_case統一）が実装済み。2026-07-29、フェーズC移行時の消費者横展開漏れ（`data_fetcher.py`・`audit.py`が旧PascalCaseキー参照のまま取り残されRICEスコア全銘柄停止）を`[[TTM-PASCALCASE-KEY-STALE-1]]`として修正完了（コミット`a7b840c32fde3b6619707f7a7c588baeaed12fd1`、`BACKLOG_DONE.md`参照）。**2026-08-01〜02、一次データ抽出品質の残課題6件が完了**（`[[PERIOD-LENGTH-VALIDATION-GAP-1]]`・`[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]`・`[[SPAC-STUB-PERIOD-FIELD-SPLIT-1]]`・`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`段階1・`[[LAYER3-GROSSPROFIT-BACKFILL-PROD-UNREACHED-1]]`①・`[[STONKS-SILO-FETCHER-GROSSPROFIT-BACKFILL-DUP-1]]`、詳細はBACKLOG_DONE.md「2026-08-01/02（完了）」参照）。**セッション後半、`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`段階2〈formerNames区間一致によるSPAC合併疑いの機械的検知〉も完了**し同エントリは段階1・段階2とも完了としてBACKLOG_DONE.mdへ全文移動済み。副産物`[[BS-ENTITY-MIXING-UNEXPLAINED-ONDS-KULR-1]]`〈KULR(2019)個別〉は根本原因調査でXBRL_MAPPING候補タグ設計欠陥と確定・105銘柄中278件へ及ぶ横断課題と判明したためクローズし`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`へ統合。**同エントリは実装完了**（貸借対照表恒等式逆算〈total_assets−stockholders_equity〉によるtotal_liabilitiesバックフィルを実装、278件全件で完全一致を確認、全105銘柄フローズン入力比較で対象外無変化を確認。BACKLOG_DONE.mdへ移動済み）。**`[[RCAT-TRIPLE-FISCAL-CHANGE-SUSPECTED-1]]`は解消**（3段階目の決算期変更は存在せず、実害なしと確認）。**`[[SPAC-STUB-PERIOD-VERIFICATION-1]]`は解消**（11銘柄すべて現状の処理が妥当と確認）。**`[[GROSSPROFIT-COGS-ANNUAL-DEFINITION-GAP-MO-PM-SCCO-1]]`はMO/PM/SCCOの3銘柄をgenuine定義差と確定しクローズ**（残り11銘柄は`[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`・`[[LITE-COGS-DA-TAG-UNMERGED-1]]`へ分離登録）。**`[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`は案b（同一accn＋期間一致優先）を実装完了**（LRCX(2010)のcost_of_revenueを是正、全105銘柄フローズン入力比較で対象外無変化を確認。CRM(2013)・JNJ(2017)・MRVL(2017)・ONDS(2017)は案b単独では未解決のまま残存、案a・案cはゲート条件込みの再設計が必要）。**`[[FETCHER-10KT-10QT-FORM-EXCLUSION-1]]`は案③（`report_consistency_check.py`へのWARN-28追加）を実装完了**しBACKLOG_DONE.mdへ移動（案①のrelevant_forms修正はコスト過大と判明し見送り確定）。**`[[RCAT-OCF-CONTINUING-DISCONTINUED-SPLIT-1]]`は根本原因調査の結果、105銘柄中25銘柄へ及ぶ横断課題と判明したためクローズし`[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]`へ統合**（実害確認調査で25銘柄中24銘柄は現在の直近5年窓の外にあり実害なしと確定、優先度を高→中に訂正）。**RCAT単独については`[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]`として新規登録**（優先度：高、`get_fcf_5yr_avg()`が実質3年平均になっておりDCFのFCFベース値計算に現在進行形の実害）。**2026-08-02後半、`[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]`パターンB実装前シミュレーションを実施した結果、RCATの本番FCF計算はTTM系列（`common/sec_data/ttm/RCAT_ttm_series.json`）経由でありreader.py::get_fcf_5yr_avg()（年次ファイルベース）は使われていないと判明**（年次パーサー側パターンB実装ではΔIV=$0、優先度を高→低に訂正）。**続く`[[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]`根本原因調査で、当初懸念とは異なりticker非依存の一般的欠陥（`ttm_calculator.py::calc_ttm_series()`が採用四半期の日付連続性を検証しない）を発見。RCATでは2023年7〜10月・10月〜2024年1月の四半期がttm_end=2025-03-31・2026-03-31の両方に重複使用され、fcf_5yr_avg・fcf_2yr_avgが正しい値より34〜55%過小評価と確定したが、IVへの影響は現時点でΔIV=$0（revenue floor＋EPSベース推定オーバーライドが吸収、将来業績改善時に顕在化しうる潜在リスクの留保付き）。根本原因を`[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]`として新規登録（優先度：中〜高）。**2026-08-02セッション後半、common/sec_data/抽出アーキテクチャの俯瞰的
脆弱性分析を実施し、本セッションで発見した5バグ（[[PERIOD-LENGTH-
VALIDATION-GAP-1]]・[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]・
[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]・[[SPAC-SHELL-BS-ENTITY-
MIXING-1]]・[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]）が共通の設計的
欠陥に帰着すると判明**。この教訓を新規データ層向けに一般化した
`docs/architecture/new_data_platform/EXTRACTION_DESIGN_PRINCIPLES.md`
（`common/market_data/`・`common/macro_data/`着手前に確認すべき3原則・
チェックリスト、`MIGRATION_CHECKLIST.md`と同型の独立文書）を新設した。
**`[[CHECK29-ACCOUNTING-IDENTITY-DETECTION-LAYER-1]]`（会計恒等式
Total_Assets=Total_Liabilities+Stockholders_Equityの横断検証レイヤー）
を実装完了**（機能コミット`bd91000f0`）。OR条件フォールバック方式
（①本体一致→②不一致時のみNCI・一時的持分の許可リストを加算した拡張形）
で実装し、全105銘柄検証でTA=TL+SE違反156件中133件を解消。続けて
[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]のHEI・ONDS型（許可リスト拡張、
機能コミット`a910afef2`）を実装し**156件中139件（89.1%）が解消**。
残る17件のうちCOHR2件は[[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]
（CHECK29のown-accn限定照合という設計方針の緩和検討）へ切り出し、
残り15件（PLTR/CART/CRWV/BKNG/V/CRM/CELH/ASTS/VRT/RDW）は個別調査未着手。
**2026-08-02セッション後半（続き）、`[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]`が提起した4種のPL/BS恒等式違反のうち残る3種（GP≠Revenue−COGS・OI>GP・NI≠EPS×Shares）の分類調査が完了**（TA=TL+SE分は上記の通り実装済み）。GOOGL(2012/2013)のGP≠Revenue−COGSは`fact_overrides.json`によるrevenue手動補正が`_backfill_gross_profit_from_revenue_cogs()`より後段で適用されるシーケンシングバグと確定・**`[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]`として実装完了**（案A採用、`_apply_fact_overrides()`を全逆算バックフィルより前に移動、機能コミット`ba8628198`・データコミット`dd6fba1a1`、GOOGL(2012/2013)のgross_profitを是正、105銘柄フローズン入力比較で対象外無変化を確認）。LMT(18/19年度)のOI>GPは①genuine（設計スコープ外）と確定・対応不要。COHR(2009-2011)のNI≠EPS×SharesはCOHR自身のFY2011 10-Kのshares_diluted/basic単位スケール申告誤り（1/1000）と確定し**`[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]`として新規登録・対応方針確定**（`fact_overrides.json`個別上書き、値も確定済み、実装は次回）。同エントリ調査から派生した`[[FIFO-TIEBREAK-OLDEST-FILING-WINS-1]]`（本人データ不在時の未文書化tie-break欠陥）は全母集団シミュレーションの結果、広範な設計変更は危険（31銘柄・124件変化、確実な改善はCOHRの2件のみ）と判明したため不採用とし、**ガード条件付き介入として`[[XBRL-UNIT-SCALE-MISMATCH-DETECTION-1]]`へ統合・実装方式確定**（同符号かつ比が10のべき乗値の場合のみ新filing優先へ切り替え）。`[[ACCOUNTING-IDENTITY-VALIDATION-LAYER-MISSING-1]]`本体は4種すべて分類調査完了としてBACKLOG_DONE.mdへ移動。

**2026-08-02セッション最終盤、`[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]`実装完了に伴う検証過程で、common/sec_data/統合フェーズ1の設計上の重大な発見があった**: TTM系列（`common/sec_data/ttm/`）を生成する`layer3_builder.py`（＋`quarterly.py`・`fact_selection.py`・`q4_implied.py`）は、annual_YYYY.jsonを生成する`parser.py`とは**完全に独立した別実装のパイプライン**であり、`parser.py`のクラス・関数を一切importせず`fact_overrides.json`も読み込まず、`_resolve_bs_entity_mixing()`等の主要ロジックも実装されていないことを確認した。結果、本セッションのannual側修正の大部分
（`[[PERIOD-LENGTH-VALIDATION-GAP-1]]`・`[[SPAC-SHELL-BS-ENTITY-MIXING-1]]`・`[[TOTAL-LIABILITIES-FALLBACK-TAG-DESIGN-FLAW-1]]`・`[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`・`[[GOOGL-FACT-OVERRIDE-SEQUENCING-BUG-1]]`・`[[COHR-SHARES-DILUTED-UNIT-SCALE-BUG-1]]`・`[[ELF-FISCAL-END-MONTH-MISDETECTION-1]]`）は、`.github/workflows/SEC_Data_Update.yml`（毎週日曜自動実行、正常稼働中と確認済み）が何度実行されてもTTM系列には反映されないという構造的問題であると判明し、`[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]`として新規登録した（唯一の例外は`[[TTM-CALC-QUARTER-CONTIGUITY-UNCHECKED-1]]`自体、`ttm_calculator.py`への直接実装のため自動反映される）。続く影響実測調査で、TANUKI VALUATION・STONKS SILOいずれも上記7件への現在進行形の実害はゼロと確定した（BS項目・shares系はTTM出力＝`FLOW_FIELDS`17種に構造的に含まれず消費経路も`annual_*.json`を直接参照、その他は対象年度が現在のTTM anchor範囲外、STONKS SILOはTTM/layer3を一切参照しない独立パイプラインと確認）ため優先度を高→中に引き下げたが、**2つの独立パイプラインが同期しない構造的脆弱性自体は温存されている**ため、将来の新規annual側修正では都度TTM側への影響確認が必要である旨を申し送る。

残課題（優先度順、2026-08-02セッション終了時点）: `[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]`〈中、構造的脆弱性は残存・既知7件への実害はゼロ確定〉・`[[CHECK29-COHR-CROSS-ACCN-TEMPORARY-EQUITY-1]]`〈中〉・`[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]`〈中、残り15件〉・`[[RCAT-TTM-SERIES-CONTINUING-DISCONTINUED-UNCHECKED-1]]`〈中〉・`[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]`〈中〉・`[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`残存分〈中〜高〉・`[[LITE-COGS-DA-TAG-UNMERGED-1]]`〈低〜中〉・`[[STONKS-SILO-FP-LABEL-PERIOD-VALIDATION-1]]`〈低〜中〉・`[[RCAT-FCF-5YR-AVG-ACTUAL-3YR-1]]`〈低、副次的解消見込み〉・`[[HON-GROSSPROFIT-2009-RESIDUAL-DISCREPANCY-1]]`〈低〉・`[[ELF-ROE10YR-RECALC-PENDING-1]]`〈中、定期更新で自然解消見込み〉・`[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]`〈低〜中〉等、`BACKLOG.md`該当項目を参照。**2026-08-05、`[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]` Stage 1完了**（`fixed_registry.json`フィックス機構の運用方針確定・スキーマ設計・実装・検証。taxonomy属性①〜⑧非該当26銘柄・372銘柄×年度エントリを`fixed_by: checkgate_pass`で登録、`parser.py`/`report_consistency_check.py`〈CHECK-31/WARN-31〉実装、全105銘柄再パースで無変化を確認。詳細はBACKLOG_DONE.md「2026-08-05（完了）」参照）。**Stage 2完了**（taxonomy属性該当58銘柄のうち、過去の個別バグ調査・SEC EDGAR一次情報照合で正しさが確定済みの12銘柄・17銘柄×年度エントリ〈HEI(2020)/LRCX(2012)/TSLA(2018)/XOM(2023)/AVGO(2016,2017)/RCAT(2024)/ELF(2015,2016)/FICO(2019,2020)/CPRT(2019,2020)/LITE(2019)/GOOGL(2012,2013)/SPIR(2025)〉を`fixed_by: manual_verification`で登録。候補のうちVRT(2016)/net_income・SPIR(2020)/long_term_debtは登録前検証でannual_{year}.jsonに対象フィールドが実在しない〈後続の別修正でNone化済み〉ことが判明し登録対象外とした。全105銘柄フローズン再パースで無変化、CHECK-31/WARN-31の発火・復元を実測確認、pytest 497 passed/2 known failed（既知）を確認。**Stage 3準備調査完了**（RDW(2020)残差未解消・PM対象年度2016-2017への訂正・SCCO(2010-2019)確認・MRVL(2019)意図せぬ解消発見・AVGO(2015)データ薄さ発見・LLY対象年度2007-2025への訂正・BBAI/RKLB/SOFI/VRT/ONDSの凍結可能フィールドなしを確認。BACKLOG_DONE.md「2026-08-05（完了）」内の各エントリ訂正・BACKLOG.md新規エントリ2件`[[AVGO-2015-DATA-THIN-1]]`/`[[SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1]]`を参照）。**Stage 3a完了**（MO(2016-2025)/PM(2016-2017)のgross_profit・LLY(2007-2025)のcapital_expenditure/free_cash_flow/fcf_method/finance_lease_payments_appliedの計31銘柄×年度エントリを`fixed_by: manual_verification`で登録。全105銘柄フローズン再パースで無変化、CHECK-31/WARN-31の発火・復元を実測確認、pytest 497 passed/2 known failed（既知）を確認）。**RDW(2020)/ASTS(2020) BS恒等式修正完了**（`_BS_IDENTITY_FALLBACK_ONLY_TAGS`拡張・`_BS_IDENTITY_ALLOWLIST`拡張、コミット`1db003c0d`・`9618b6754`。全母集団シミュレーションでタグの測定基準に応じ設計判断を使い分け）。**Stage 3b完了**（SCCO(2010-2019)のgross_profit・RDW(2020)/ASTS(2020)のtotal_assets/total_liabilities/stockholders_equityの計3銘柄・12銘柄×年度エントリを`fixed_by: manual_verification`で登録。全105銘柄フローズン再パースで無変化、CHECK-31/WARN-31の発火・復元を実測確認、pytest 497 passed/2 known failed（既知）を確認）。**`[[AVGO-2015-DATA-THIN-1]]`原因調査完了**（原因確定・`[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`へ統合、AVGO旧CIK登録が無関係な買収先企業Broadcom Corpを指している疑いが判明・実害ゼロ確認済み・着手条件成立まで保留）。**新DB構築プロジェクト フェーズ1 Step1（SEC EDGAR統合、`[[SECDATA-STORAGE-FRAGMENTATION-1]]`）着手・raw/削除完了**（6ファイル系統・EPS Analyzer/TANUKI TAIL独自アクセス経路の全消費者を実ファイルで洗い出し、実消費者ゼロと確認した`raw/`〈`quarterly.py`書込処理・既存105ファイル約16MB・`SEC_Data_Update.yml`該当行〉を削除。全105銘柄フローズン検証でnormalized/への影響ゼロ、NG=0・WARN不変、pytest 497 passed/2 known failed確認）。**`data/quarterly_{FYQ}.json` pl/cf/shares区分のYTD→単一四半期(SA)修正完了**（2026-08-05。normalized/→data/統合の事前調査で、`quarterly_*.json`のpl/cf/shares区分が従来XBRL申告のYTD累積値をそのまま保存しており〈約65〜66%のエントリが該当〉、`INPUT_DATA_TOBE.md`が想定する「正規化済み」を実データで満たしていないことが判明。`quarterly.py::_classify_period()`・`normalizer.py::_ytd_to_quarterly()`〈normalized/側で実績のあるロジック〉を再利用する統一アルゴリズム〈SA〈単一四半期〉候補優先、なければYTD差分計算にフォールバック、加重平均フィールド〈shares_diluted等〉は差分計算対象外〉を`parser.py::parse_company_facts()`の四半期抽出ループに実装。メモリ上シミュレーションと実書き込み結果が完全一致することを確認した上で全105銘柄を実再パース、annual側は1,441ファイル横断比較で差分0件、report_consistency_check.py NG=0・WARN=78件〈不変〉、pytest 497 passed/2 known failed確認。詳細はBACKLOG_DONE.md参照）。**フェーズD Step1（アクセサのラッパー化）・Step2-1（TANUKI VALUATION本体切替）完了**（2026-08-06。`layer3_builder.py::get_quarterly_series()`/`get_latest_quarterly()`新設に続き、`pipeline.py`6箇所〈希薄化率・TTM信頼性判定・LTDebtフォールバック・`_estimate_ttm_operating_income()`・`_calc_moat_inputs()`〉を`get_field_entries()`経由に切替。事前バグ修正2件〈`[[LAYER3-CONFIG-RD-TAG-PRIORITY-1]]`・`[[LAYER3-ANNUAL-MISCLASSIFICATION-BBAI-1]]`〉・新設`get_long_term_debt_latest()`（Layer3優先方式）を含む。100銘柄全数回帰確認、pytest 505 passed/2 known failed（既知）、report_consistency_check.py NG=0・WARN=78件（既存と不変）。詳細はBACKLOG_DONE.md「2026-08-06（完了）」`[[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP1]]`・`[[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP2-1]]`参照）。**フェーズD Step2-2〜2-5、実質完了**（2026-08-07）。Step2-2
（②STONKS SILO`financial_trend_calculator.py`のみ）・Step2-3
（③TANUKI TAIL`quarterly_review_generator.py`・`tail_dcf_bridge.py`）・
Step2-4（④HypeCore`hypecore.py`）を実装完了（各回とも全数比較で
差分ゼロまたは既知パターンのみと確認、詳細はBACKLOG_DONE.md
`[[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP2-2]]`〜`[[SEC-EDGAR-LAYER-
DESIGN-PHASE-D-STEP2-4]]`参照）。Step2-5（⑤stock.htmlフロントエンド
＋診断・補助スクリプト7件）は投資調査の結果、Layer3切替の実質対象が
`dcf_validity_checker.py::check_c_data_jump()`のみと判明し実装不要
（実質完了扱い）。並行して保留中だった2判断が確定：
`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`（STONKS SILO
`fetcher.py`・`dcf_validity_checker.py::check_c_data_jump()`の年次
データ選択思想の不一致）は案2（Layer3切替を見送り現状維持）を採用、
`[[STOCKHTML-LAYER3-PUBLISH-PIPELINE-MISSING-1]]`（stock.htmlは
Layer3公開パイプライン未整備のため切替不可）も着手見送りで確定。
**フェーズE（`normalized/`完全廃止）は上記2系統＋stock.htmlが恒久的
例外として残るため着手不可**と判定（詳細はBACKLOG.md
`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照）。
**次の優先タスク**: `common/sec_data/`統合が実質完了したため、
新DB構築プロジェクト フェーズ1の次コンポーネント（`common/
market_data/`・`common/macro_data/`新設）への着手を検討する
（着手前に`EXTRACTION_DESIGN_PRINCIPLES.md`の3原則を確認、下記
参照）。本線外の残タスク（MRVL/AVGO/DELL旧CIK分の年度×フィールド
単位の個別確認・BBAI/RKLB/SOFI/VRT/ONDSの維持フィールド凍結検討・
`[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`対応）は優先度中〜低のまま
BACKLOG.mdに残置 |
| `common/market_data/` 新設（yfinance統合層、`INPUT-A-019〜023`対応） | **完成**（`fetcher.py`・`reader.py`・Daily/Weekly Update workflows完成、本番消費者8＋診断ツール2＋周辺ツール2の**全12ファイル切替完了**、2026-08-12。**2026-08-13追記**: 未追跡だった`collect_and_send.py::collect_asset_flow()`のSHV等6資産切替も完了、`common/market_data/`本線タスクは完全に完了） | `INPUT_DATA_TOBE.md` 2-B参照。日次/週次属性/イベント履歴の3層分離設計。`fetcher.py`（`fetch_daily_prices`/`fetch_weekly_attributes`/`fetch_analyst_events`/`backfill_daily_prices`、`start=`パラメータ対応済み）・`reader.py`（`get_earnings_history`/`get_recommendations_history`/`get_price_on_or_after`/`get_price_series_as_of`含む12種の読み取りAPI）を実装、`Market_Data_Daily_Update.yml`/`Market_Data_Weekly_Update.yml`をworkflow_dispatchで実行確認済み。本番消費者8ファイル（`beta_fetcher.py`・`data_fetcher.py`〈TANUKI VALUATION本体、DCF計算直結〉・`valuation_fetcher.py`〈STONKS SILO〉・`pipeline.py`〈`.calendar`〉・`collect.py`〈Discover〉・`collect_and_send.py`〈Market Pulse〉・`breadth_calculator.py`・`hypecore.py`〈daily/attributes/analyst_historyの3層混在、前提作業3件込みで最複雑〉）・診断ツール2ファイル（`audit.py`・`score_verifier.py`）・周辺ツール2ファイル（`extract_key_facts.py`・`backfill_tech_pulse.py`）が全て完了、実データ全数比較で回帰なしを確認。2026-08-13、`collect_and_send.py::collect_asset_flow()`が未追跡のまま`yfinance`直接呼び出しを残していたことが判明し（`[[MARKETDATA-COLLECT-ASSET-FLOW-UNTRACKED-1]]`）、SHV等6資産を`reader`経由へ切替・全数一致・overall_score不変を確認して解消（BACKLOG_DONE.md参照）。詳細は`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・BACKLOG_DONE.md「2026-08-12（完了）」「2026-08-13（完了）」参照 |
| `common/macro_data/` 新設（FRED統合層、`INPUT-A-024〜047`・`049`対応） | **完成**（`fetcher.py`/`reader.py`実装・定期取得ワークフロー・本番消費者2ファイル全数切替が完了、2026-08-12。**2026-08-13追記**: 未追跡だった`backfill_tech_pulse.py`のVXNCLS切替も完了、`common/macro_data/`本線タスクは完全に完了） | `INPUT_DATA_TOBE.md` 2-C参照。系列単位の時系列ストア設計。**実装サマリー**: `fetcher.py::fetch_series/update_series/fetch_all_series`（fredapiクライアントをモジュールレベルで1つだけ生成、リトライ3回＋指数バックオフ、保存前検証2項目＋`macro_data_violations_log.json`）・`reader.py::get_latest/get_series/get_value_as_of`・`series_meta.json`（25系列）・`.github/workflows/Macro_Data_Update.yml`（毎日UTC10:00・workflow_dispatch対応）を実装。**本番消費者2ファイル（`05_main.py`・`collect_and_send.py`）を`reader`経由へ全面切替**（`Fred(`/`fred_latest(`等の直接呼び出しをgrep最終確認で0件に、リトライ・指数バックオフも削除）。重複3系列（`BAMLH0A0HYM2`・`T10Y2Y`・`VIXCLS`）は`reader.get_latest()`への集約で解消。単一最新値だけでは機能を維持できない5箇所（NFP前月比・VXN MA50・HYスプレッド90日min/max・DGS3MO前日比・S&P500複数日履歴）は`reader.get_series()`（期間指定）を使用。切替前後で18項目の値突合を実施し全項目完全一致（差分0件）。`FTSD`はFRED上に系列が実在せず取得失敗（`[[MACRODATA-FTSD-SERIES-ID-INVALID-1]]`、旧実装でも同様の失敗だったため回帰ではない）。新規テスト計50件（`tests/test_macro_data_fetcher.py`・`tests/test_macro_data_reader.py`・更新済み`tests/test_macro_pulse_logic.py`）、pytest全体771 passed / 2 known-failedで回帰なしを確認。副次発見: 日次cronが全期間履歴を再取得する非効率設計（`[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]`、対応未定）。過去データ一括投入（フェーズ2）は別段階。2026-08-13、`backfill_tech_pulse.py`が未追跡のまま`VXNCLS`をFRED直接呼び出ししていたことが判明し（`[[MACRODATA-BACKFILL-TECH-PULSE-VXNCLS-UNTRACKED-1]]`）、`reader`経由へ切替・回帰なしを確認して解消。同時に重複計算パターン調査で`[[FRED-HYSPREAD-TRIPLE-FETCH-1]]`（HYスプレッド3重取得）も実装は既に別プロジェクトで解消済みと判明しクローズ（BACKLOG_DONE.md参照）。詳細は`[[MACRODATA-LAYER-CONSTRUCTION-1]]`・BACKLOG_DONE.md「2026-08-12（完了）」「2026-08-13（完了）」参照 |
| 取得前提条件の一元管理（`INPUT-B-001〜003`） | 未着手 | `INPUT_DATA_TOBE.md`分類B参照。監視銘柄マスタ・CIKマッピングの管理方法は分類Aの取得と一体で設計する |
| provenanceメタデータ標準化 | 未着手 | `INPUT_DATA_TOBE.md` 2-D参照（`as_of`/`fetched_at`/`source`/`source_detail`/`fallback_used`） |
| fetcher/reader分離アクセス制御 | 未着手 | `INPUT_DATA_TOBE.md` 3-B参照 |

## フェーズ2: 過去データ移管（分類A48件＋分類B3件が対象、**2026-08-12実質完了**）

**フェーズ2は4データソース全てに結論が確定し実質完了した**（SEC EDGAR・
yfinance・取得前提条件はフェーズ2対象外と確定、FREDは`BAMLH0A0HYM2`
1系列のみ例外的移行を実施済み・他23系列は対象外）。詳細は下記表・
BACKLOG_DONE.md該当項目参照。

| データソース | 状態 | 対象範囲 |
|---|---|---|
| SEC EDGAR既存データ（`INPUT-A-001〜018`） | **対象外と確定** | `INPUT_DATA_AS_IS.md` 1-A・2-A参照（実測7経路）。**現状評価（2026-08-12投資調査、全105銘柄精査完了）**: **フェーズ2対象外と確定**（全105銘柄精査完了、詳細は`[[PHASE2-SECDATA-FULL-DEPTH-VERIFICATION-1]]`参照）。`company_facts.json`と`annual_*.json`の履歴深度を全105銘柄で機械的に突合した結果、104銘柄で有意な差は確認できず（-1〜+1年のノイズ帯に収束）、`common/sec_data/`のfetcherは既にSEC EDGAR APIから取得可能な範囲の履歴を再取得できていると判断。ただしENBの正規化データ欠落を別問題として発見、`[[SECDATA-ENB-NORMALIZATION-MISSING-1]]`参照 |
| yfinance既存データ（`INPUT-A-019〜023`） | **対象外と確定** | `INPUT_DATA_AS_IS.md` 1-B・2-B参照（実測12ファイル。`common/sec_data/audit.py`見落としを2026-08-07訂正、`[[MARKETDATA-AS-IS-AUDIT-PY-OMITTED-1]]`参照）。**現状評価（2026-08-12投資調査完了）**: **フェーズ2対象外と確定（移行作業不要）**。旧保存先（hypecore・`market_data.json`・`breadth_data.json`）の派生指標の元となる生データ（価格・出来高）は、`common/market_data/daily/`へ既に2021-01-04〜の深さで保存済みであり、必要な期間（hypecoreは2021年〜、`market_data.json`/`breadth_data.json`は2026年4月〜）を十分にカバーしていることを確認。派生指標は既存データから再計算可能なため移行不要と判断。詳細は`[[PHASE2-YFINANCE-REFETCH-DESIGN-1]]`参照 |
| FRED既存データ（`INPUT-A-024〜047`） | **`BAMLH0A0HYM2`のみ例外的移行済み、他23系列は対象外** | `INPUT_DATA_AS_IS.md` 1-C・2-C参照（実測2サブシステム）。**現状評価（2026-08-12投資調査、`BAMLH0A0HYM2`分は同日実施済み）**: 全24系列を調査した結果、`BAMLH0A0HYM2`のみが2026年4月からのFRED側新規提供制限（直近3年のみ）に該当し、再取得不可能と確認。下記「移行方針」の「例外」規定を適用し、`common/macro_data/migrate_bamlh0a0hym2_history.py`を実装・実行、旧`05_events.csv`（1996-12-31〜2023-08-11分、6,947件）からの一度限りの移行を**実施済み**（`2023-08-14`以降の既存785件は無変化を確認、移行後合計7,732件。詳細は`[[MACRODATA-BAMLH0A0HYM2-HISTORY-EXCEPTION-1]]`参照）。他23系列は初回投入時点で既にFRED公式の全期間履歴を取得済みのため追加の再取得作業は不要と確認済み |
| 取得前提条件（`INPUT-B-001〜003`） | **対象外と確定** | `INPUT_DATA_AS_IS.md` 1-D・1-E参照（`monitor_tickers.yaml`・`cik_lookup.csv`／`cik_lookup_result.json`はいずれも現状`config/`配下に存在確認済み）。**現状評価（2026-08-12投資調査）**: 対象外と判断（ファイルサイズが小さく、移管・再取得いずれも不要） |

**移行方針（2026-08-12確定）**:
- **原則**: 過去データは「再取得」（取得元APIから履歴込みで取得し直す）
  または「再導出」（既存の一次データから計算し直す）を基本とする。
  旧保存先のファイルをそのままコピーする形の「移管」は原則行わない
- **例外**: データ提供元が恒久的にAPI経由の提供範囲を制限しており、
  再取得が技術的に不可能な場合に限り、削除予定の旧保存先データから
  一度限りの例外的移行を行う。この場合、移行したレコードには
  provenanceの`source_detail`に例外的移行である旨を明記し、ライブ
  取得分と区別できるようにする（適用例: `[[MACRODATA-BAMLH0A0HYM2-
  HISTORY-EXCEPTION-1]]`）
- **前提**: 旧保存先データは、フェーズ2完了後に不要となったものから
  順次削除する方針のため、削除前にこの判断を完了させる必要がある

詳細な調査経緯（文書ベース調査での仮説提示、実データ検証による反証、
FRED全25系列の`observation_start`横断調査等）はBACKLOG_DONE.md
`[[PHASE2-MIGRATION-POLICY-DECIDED-1]]`参照。`BAMLH0A0HYM2`の例外的
移行は実装・実行済み（詳細はBACKLOG_DONE.md`[[MACRODATA-BAMLH0A0HYM2-
HISTORY-EXCEPTION-1]]`参照）。SEC EDGAR・yfinance分の結論確定は
それぞれBACKLOG_DONE.md`[[PHASE2-SECDATA-FULL-DEPTH-VERIFICATION-1]]`・
`[[PHASE2-YFINANCE-REFETCH-DESIGN-1]]`参照（いずれも完了、フェーズ2
対象外と確定）。

**分類Cはフェーズ1・2の対象外**: `config/segment_config.json`等14件
（`INPUT-C-001〜014`）は一次データそのものではなく`FIELD_DEFINITIONS.md`
導出データ側（392件）が消費する入力であるため、一次データ層の構築・
移管スコープには含めない。Portfolio二重保持（`INPUT-C-008`）・
`config/`外配置（`INPUT-C-009`/`010`）等の是正要否は、フェーズ3
（導出データ層の管理方法検討）で扱う。

## フェーズ3: 導出データ層の管理方法検討（分類C14件を含む、**2026-08-12着手・2026-08-15完了**）

| 項目 | 状態 |
|---|---|
| `FIELD_DEFINITIONS.md` 499項目の新DB参照への切替方針 | **調査完了（2026-08-15）**。499項目中、一次データ29件をyfinance/FRED/SEC EDGAR/システム内部に分類した結果、yfinance/FRED由来18件（AS-IS-032・190・192・194・197・199・200・210・211・212・262・312・320・321・322・325・352・362）は**全件`common/market_data/`・`common/macro_data/`への切替が実コードで確認済み**（`data_fetcher.py`・`05_main.py`・`collect.py`・`collect_and_send.py`・`breadth_calculator.py`いずれも直接呼び出し0件）。`FIELD_DEFINITIONS.md`側の「データ取得元」列記載（2026-07-22時点のまま）が現状と乖離していたため、該当18件に切替済み注記を追記した。**SEC EDGAR由来4件（AS-IS-129・266・273・395）は全件検証完了（2026-08-15）**。いずれも意図的にLayer3化対象外と確定済みで、記載も現状と一致（AS-IS-129のみ、フェーズD対象外である旨の軽量注記を追加。AS-IS-129はSTONKS SILOの`fetcher.py`選択思想の違いにより`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`で現状維持確定、AS-IS-266・273はEPS Analyzerの独立ライブ取得設計によりフェーズD対象外、AS-IS-395はTANUKI TAILの`sec_ctrl_fetcher.py`〈リアルタイム監視専用、フェーズD対象の`quarterly_review_generator.py`等とは別ファイル〉）。残り477件は新DB化対象外（システム内部・手動入力・導出データの非カタログ入力等） |
| 分類C14件（`INPUT-C-001〜014`）の管理方法検討（`config/`外配置2件の是正、Portfolio二重保持の是正等） | **完了（2026-08-15）**。**対応済み5件**: `[[TAILKPI-CONFIG-LOCATION-1]]`（`INPUT-C-009`、`config/tail_kpi_map.json`へ移動完了・コミット`80890c711`）・`[[FCFCONFIG-LOCATION-1]]`（`INPUT-C-010`、`config/fcf_conversion_config.json`へ移動完了・コミット`493e8843a`）・`[[PORTFOLIO-CONFIG-DUP-1]]`（`INPUT-C-008`、`config/portfolio.json`を廃止し`docs/portfolio/data/portfolio.json`へ一本化・コミット`e97741f54`/`eaf3016cb`）・`[[DISCOVER-CONFIG-DUAL-MGMT-1]]`（`INPUT-C-006`/`007`、`config/discover_config.json`をPythonバックエンド〈`collect.py`・`registration_validator.py`〉の入力として維持しつつ`docs/portfolio/data/`側を`Discover_Config_Sync.yml`〈新設〉で自動追従させる非対称設計に確定・実装完了、同期漏れ検知として`report_consistency_check.py`にCHECK-32新設・コミット`20a173a76`/`80bcf5f57`）。**調査完了・現状維持が妥当と判断した9件**: `INPUT-C-001〜005`（`_meta`統一済み3件＋admin.html編集UIあり2件、実害なし）・`INPUT-C-011〜014`（低頻度・専門知識を要する編集はGitHub直接編集の方が適切、または既存の死蔵ページ問題〈`[[EPSANALYZER-ADMIN-ORPHAN-PAGE-1]]`〉に集約）。個別の判断根拠は`INPUT_DATA_TOBE.md`分類C表の各行注記を参照。`rpo_config.json`（`INPUT-C-004`）の編集UI欠如は既存`[[RPO-ADMIN-1]]`で捕捉済み（重複登録せず追記のみ）。`_meta`スキーマの標準化方針は`NAMING_CONVENTIONS.md`規則8に記録（既存ファイルへの遡及適用はしない）。フェーズ3はこれで完了 |

---

## 関連BACKLOG項目

`NETCASH-DUAL-CALC-1`・`NETINCOME-DUAL-PIPELINE-1`・
`SECDATA-STORAGE-FRAGMENTATION-1`・`FRED-HYSPREAD-TRIPLE-FETCH-1`・
`SP500-GSPC-MULTI-FETCH-1`・`PORTFOLIO-CONFIG-DUP-1`・
`BETA-FALLBACK-DESIGN-GAPS-1`等、2026-07-22〜23に起票した39件のうち、
新DB構築（本プロジェクト）によって構造的に自動解消されるものと、新DB
構築後も個別対応が必要なもの（例: [[SCENARIO-BEARBULL-SIGN-FLIP-1]]の
ような計算ロジック自体の欠陥は、一次データ層の統合だけでは解消しない）
を区別する必要がある。この整理は本プロジェクトの各コンポーネント着手時に
行う。

## 更新ルール

本ファイルの各ステータスは、該当コンポーネントの実装依頼が完了した都度、
その完了報告に含まれる更新内容を反映して更新する（`CHAT_RULES.md`
「新DB構築プロジェクトの進捗管理」参照）。「未着手」→「構築中」→
「完成」の3段階で管理し、「構築中」に遷移した場合は着手日を、
「完成」に遷移した場合は完了日とコミットハッシュを備考欄に追記する。

分類A/B/Cの件数・IDリストに追加・削除が生じた場合は、本ファイル冒頭の
「一次データ層の総数」表も同時に更新する（`CHAT_RULES.md`
「一次データ層の件数管理」参照）。
