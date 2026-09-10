# Claude Code 作業開始テンプレート

最終更新: 2026-09-10（**セッション終了時ブラッシュアップ・2026-09-10
セッションサマリー**。既知の安全策で無効化された案件の体系的クローズに
始まり、CDNS/INTUの次元分解値回収機構、KPI unit表示バグ、Grokコスト
調査（新規バグ発見含む）、MACRO PULSEの複数の根治的修正（観測日開示・
重複判定統合）、RICE計算のSBC部分合計誤用修正等、非常に多数の実装・
調査を実施した（全てpush済み）:

1. `[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]`クローズ: 前回
   （09-08〜09-09）ブラッシュアップで「既知の2件のID重複」の一つと
   していた本エントリの残存BACKLOG.mdアクティブヘッダーを再確認した
   ところ、「真の残タスク2件」として記載されていた内容が実際には
   既に消滅していることが判明し、クローズした（クロスファイル重複は
   これでCONFIG-LOAD-SILENT-FALLBACK-1のみに減少）

2. **PLTR(2019)・CHECK29系のBS恒等式「対応不可」7件を型D対応の
   新機構で全件解消**（`[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]`）:
   SECの`data.sec.gov/api/xbrl/companyfacts`バルクAPIが構造的に返せ
   ない「次元分解開示専用・非次元版が一件も存在しない」ファクト
   （`[[ANOMALY-PATTERN-CATALOG-1]]`型Dとして新規カタログ化）に対応
   するため、生XBRLインスタンス文書を直接パースし次元別ファクトを
   機械合算する新モジュール`dimension_aggregate_fetcher.py`を新設。
   PLTR(2019)・CART(2023-2025、3件)・V(2008)・CELH・ASTS(2019)の
   BS恒等式チェックを全件解消（BKNGのみ真に開示自体が存在せず対応
   不可と確定）。CELH実データで単一軸コンテキストと二重軸コンテキスト
   （Statement of Stockholders Equity再掲）の重複計上リスクを発見し
   単一軸限定フィルタで対処、ASTS(2019)では既存クロスaccnフォール
   バックとの二重計上を早期return方式の優先順位付けで解消

3. `[[LAYER3-COGS-DIMENSION-RECOVERY-CDNS-INTU-1]]`: 上記②の機構を
   duration fact（P&L・cost_of_revenue）にも拡張し、CDNS/INTUの
   cost_of_revenue（standard tagだが次元限定開示のため消失していた）
   を回収。**正直な経緯として記録**: 登録時「cost_of_revenueは
   DCF/FCF計算に直接影響しうるため慎重な検証が必要」と過大な警戒を
   実際の消費先確認前に書いたが、後の確認でTANUKI VALUATIONの
   DCF/Moat Scoreは別系統のLayer3パイプライン（本フィールドは
   元々空）を参照しており実害なしと判明、これを報告時に訂正した。
   また実装完了直後、コミット前の`git status`未コミット差分を指して
   「NG=2」と報告したが、これは自分のローカル未コミット状態のみを
   反映したもので、Koichiさんの独立確認（クリーンな状態）と食い違い、
   「正直に状況を教えてください」とのご指摘を受けた。再確認の結果、
   説明不足だったことを認め、実際に何がコミット・pushされているかを
   正確に再報告した。最終的に一次情報citation・`--expect`ライブ再取得
   検証・CHECK-46整合確認・全105銘柄シミュレーション・DCF/FCF実消費
   確認を経て、Koichiさんの承認を得てコミット・push（fixed_registry.
   json snapshot_hash再計算込み）した

4. `[[KPI-UNIT-HARDCODE-USD-1]]`: `xbrl_segment_fetcher.py`が全KPIの
   unitを`"USD"`に固定していたため、PLTR等の比率系KPI（貢献利益率等）
   がツールチップで金額表記になっていた表示バグを修正。値そのものの
   型ではなくKPI名のキーワード（「率」「マージン」等）で判定する
   `_infer_kpi_unit()`を新設し、`docs/portfolio/tail/index.html`・
   `detail.html`双方のフォーマッタに`unit==='ratio'`分岐を追加。
   検証過程で`detail.html`のformatVal()が元々ratio表示に一切対応して
   いなかった副産物バグも発見・修正

5. `[[GROK-MODEL-PRICE-1]]`（未クローズ、xAI Console確認待ちのまま
   BACKLOG.mdに残置）: 1回目調査でレガシーエイリアス
   （grok-3-mini/grok-3/grok-2-1212）が実際には`grok-4.3`へ自動
   ルーティングされていることを実測確認し、全9ファイル・10箇所の
   モデル名表記を是正（実API呼び出しで動作確認）。grok-2-1212は
   完全に廃止済み（HTTP 400）だったことも判明。その後Koichiさんが
   xAI Console実データを確認したところ、直近7日支出$1.65の**80%
   （$1.33）が想定外の「grok-4.20-0309-reasoning」**に帰属している
   ことが判明し、2回目の追加調査を実施: 呼び出し元を
   `ai_analyzer.py`（adjusted_eps_analyzer）1箇所と特定し、9/6・9/7
   への支出集中の原因が新規発見バグ`[[WORKFLOW-FALLBACK-CRON-
   DUPLICATE-1]]`（下記6.）であることを実行履歴の実測で突き止めた。
   また当初の想定（risk_fetcher.py/Discover削除がX searches費目の
   直接原因）を訂正: 両者は実は`search_parameters`を指定しておらず
   （2026-05-23に別コミットで既に削除済み）、X searches課金の直接
   originではなかった（ただし高頻度呼び出し元だったため削除は
   grok-4.3総呼び出し量の激減に寄与したと判断）

6. **新規発見・実装完了** `[[WORKFLOW-FALLBACK-CRON-DUPLICATE-1]]`:
   上記5.の調査から、`Adjusted_Eps_Analyzer_update.yml`が
   `workflow_run`連鎖成功後も週次フォールバックcronが無条件に追加
   実行され、99銘柄分のGrok呼び出しが実質週2回発生していたことを
   GitHub Actions実行履歴（API直接確認）で実測。冪等性ガード
   （`summary.json`のlast_updatedが24時間以内なら早期return、
   `--force`で手動re-run時はバイパス）を実装し解消。同型パターンを
   `TANUKI_VALUATION_Update.yml`・`TANUKI_Score_Update.yml`にも
   確認したが、前者は`validate_calculation()`のGrok呼び出しが本番
   エントリポイントで`use_ai_validation=False`ハードコードのため
   実害ゼロと判明し対応不要、後者（daily_pick.py）には「1日1回」の
   設計意図に合わせた日付ベースの冪等性ガードを追加した

7. `[[MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1]]`: 景気サイクルフェーズ
   複合スコアの22%（CFNAI・Building Permits）が実測約7週間遅れの
   データに基づくが閲覧者が気づく手段がなかった問題を、個別注記の
   追加ではなく`idxLatestAsOf()`拡張（`.actual`のみでなくエントリ
   全体`{dateMs,actual,updatedMs}`を返す）で根治的に解消。数値のみ
   必要な既存呼び出し元（ticker widget・LAYER2ヘルスバー・L3類似度
   計算）は薄いラッパー`idxLatestVal()`へ個別移行し破壊的変更を回避。
   8指標全てのツールチップに「観測日」行を追加し、Michigan
   Sentimentの個別注記文言はこの汎用機構に統合し廃止。AI週次レポート
   プロンプト（05_main.py）にも各指標の観測日を付記（既存の`info
   ['date']`を1行追記するだけで対応完了）

8. `[[MACRO-THRESHOLD-INCONSISTENCY-1]]`: ②`dedupe_new_rows()`の
   重複判定を実データで検証したところ、Sahm Rule（930ヶ月中155件・
   約17%）が直前月と完全一致する値を取ることを確認、現行コードへ
   模擬データを実際に投入し正当な新規行が誤除外されることを再現した
   （本番でまだ実害が顕在化していないのは導入後の実データに偶然
   反復値が発生していないためで「次に発生すれば確実に起きる既知
   バグ」と判断し先行修正）。修正は静的な例外リストではなく、
   `05_audit.py::check_duplicate_events()`に既に存在していた
   `_duplicate_risk_indicators(schedule)`（監査側では既に正しく
   機能していた判定ロジック）を`05_main.py`へ移動して両者で共有する
   形に統合し、[[MACRO-THRESHOLD-INCONSISTENCY-1]]が指す「監査側と
   書き込み側の不一致」自体を解消。**対応範囲は当初想定の2指標
   （Sahm Rule・CFNAI）から、実際のschedule.csv適用結果に基づき
   10指標へ拡大**（HY Spread 651件・Yield Curve 1086件等、同種の
   正当な反復パターンを実データで確認済み）。①YC閾値3セットは
   ticker/L2（粗いラベル用途）とRECESSION RISK SCORE（加重平均への
   入力用途、4段階）の相違を意図的な設計と判断し統一せず、判断根拠を
   コードコメントに明記。あわせてYC自身のツールチップ表示バグ
   （実際の閾値-0.5%ではなく-0.2%と誤表示）を修正し、調査中に発見した
   同型パターン（HY Spread等5指標）は新規`[[MACRO-TOOLTIP-THRESH-
   LABEL-MISMATCH-1]]`として別途登録（未対応、次セッション候補）

9. `[[TTM-SBC-QUARTERS-GAP-1]]`: `build_rice_annual_shape()`でSBCが
   quarters完全性チェック対象外だった件を実データ調査したところ、
   102銘柄相当374行中40行でSBCのquarters_used<4、うち8行
   （GEV/HWM/TDY）はSBCが非Noneの部分四半期合計として完全な年間値
   であるかのようにrice.py::_calc_q()（Q=OCF÷(純利益+SBC)）へ渡って
   いたことを実測確認した（Q値がGEVで+3.71%等、過大に算出）。
   `_quarters_complete()`にSBCを追加し行全体を除外する案（依頼文の
   想定）ではなく、SBC自体をNone化しrice.py既存のNone許容
   フォールバック（RD/SMと同型）に委ねる、より対象を絞った修正を採用
   （40行中32行はvalが元々Noneで無害だったため、行全体除外だと
   その32行の他フィールドまで不必要に失うと判断）

10. `[[NORMALIZER-YTD-METADATA-STALE-1]]`: `normalizer.py::
    _ytd_to_quarterly()`のQ2以降エントリでstart/period_daysが変換前
    のYTD期間のまま残るバグを、`layer3_builder.py`側に既に存在した
    「正しい実装」（2026-07-24新規構築時から同種の再計算込みで実装
    済み）を参照し移植して解消。parser.py側のperiod_days参照11箇所は
    全て無関係と確認済み。**移植過程で移植元のlayer3_builder.py自身
    にもオフバイワンバグ**（start起点が前四半期end日そのものになって
    おり真の慣例より1日短い、AAPL実データのQ2 CapEx start/period_days
    で発見）を発見し両モジュールとも修正。全103銘柄・18,114変換
    エントリの新旧ロジック突合でval不一致0件を確認、normalized/
    102ファイルを再生成

11. `[[BACKTEST-SCORE-1]]`: TANUKI SCORE分類別の勝率・平均リターン
    バックテストの着手条件（90日リターンのサンプル数）が実データで
    充足していることを確認し実装。既存の部分実装
    `renderScoreVerify()`を拡張（重複実装を避ける）し90日指標・
    低サンプル数フラグを追加。実データ・既知の集計値との突合検証、
    claude-in-chromeでのブラウザ実地検証（ライブDOM値と独立Python
    集計の完全一致確認）を実施

12. `[[BS-FIELD-NEWLY-MISSING-2026-1]]`: LLY/SCCO/SPIRのBSフィールド
    None遷移を一次情報（10-K）で個別調査し、全3件が生涯フェードアウト
    （真のゼロ継続）と確定、抽出バグではないことを確認して
    `warn_acknowledged.json`へ登録

13. `[[MA-INTEGRATION-TAG-GAP-1]]`: 長期停滞していた本エントリの
    停滞原因を分析し、新たな設計角度を提案（実装は未着手、次セッション
    以降の判断待ち）

**次セッションの着手候補**:
- `[[MACRO-TOOLTIP-THRESH-LABEL-MISMATCH-1]]`（本日新規登録。
  RECESSION RISK SCOREのHY Spread・Philadelphia Fed Manufacturing・
  CFNAI MA3・Initial Claims 4W MA・Michigan Consumer Sentimentで、
  ツールチップ表示閾値と実際のスコア計算ステップ関数の境界値が
  食い違っている）
- CRM(2018)のGP-COGS不整合（CHECK-46実装過程の実データ校正で発見、
  前回09-08〜09-09セッションから継続未着手）
- `[[TAIL-SEC-ITEMS-1]]`（機能追加要望、Koichiさん判断待ちで保留中）
- `[[MA-INTEGRATION-TAG-GAP-1]]`（本日新設計角度を提案済み、実装は
  いずれ向き合う必要あり）
- `[[GROK-MODEL-PRICE-1]]`のxAI Console側実請求確認（Koichiさん本人
  のアカウントアクセスが必要、本セッションでは対象外のまま）

**セッション終了時ブラッシュアップの検証結果**:
- BACKLOG.md/BACKLOG_DONE.md移設漏れ: 本セッションでクローズした
  8件（LAYER3-COGS-DIMENSION-RECOVERY-CDNS-INTU-1・BACKTEST-SCORE-1・
  BS-FIELD-NEWLY-MISSING-2026-1・KPI-UNIT-HARDCODE-USD-1・
  NORMALIZER-YTD-METADATA-STALE-1・WORKFLOW-FALLBACK-CRON-
  DUPLICATE-1・MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1・
  MACRO-THRESHOLD-INCONSISTENCY-1・TTM-SBC-QUARTERS-GAP-1・
  SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1、計10件）全件が
  `### ✅ [ID]`パターンでBACKLOG_DONE.mdに存在し、BACKLOG.md側に
  アクティブヘッダーとして残存していないことを機械的に確認
  （移設漏れ0件）。GROK-MODEL-PRICE-1（xAI Console確認待ち・意図的
  に保留）・MACRO-TOOLTIP-THRESH-LABEL-MISMATCH-1（新規登録・未着手）
  はBACKLOG.mdにアクティブヘッダーとして意図通り残存
- ID重複チェック: BACKLOG.mdとBACKLOG_DONE.md間でヘッダーIDが重複
  するものは、既知の1件（`[[CONFIG-LOAD-SILENT-FALLBACK-1]]`、
  段階的完了・部分対応の意図的な分割）以外に新規重複なしを確認
  （`[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]`は本セッション1.で
  クローズしたため既知2件から1件に減少）。BACKLOG_DONE.md内部での
  同一ID複数エントリ（MARKETDATA-LAYER-CONSTRUCTION-1・DESIGN-8等）
  は多段階プロジェクトの進捗記録として妥当なパターンと確認、新規の
  誤登録ではない
- git status: クリーン（未コミット変更・未追跡ファイルなし）。
  作業中にorigin/kaihatsuへ自動データ更新コミットが複数回入ったため、
  都度fetch→マージ（コンフリクトなし）で追従した
- BACKLOG.mdアクティブ件数: 機械カウントで**60件**（前回09-08〜
  09-09時点の67件から、本セッションでクローズした8件・新規登録した
  1件〈MACRO-TOOLTIP-THRESH-LABEL-MISMATCH-1〉により7件減）

詳細は各BACKLOGエントリ・BACKLOG_DONE.md「2026-09-10（完了）」節・
PROJECT_STATUS.md参照。

---

最終更新: 2026-09-09（**セッション終了時ブラッシュアップ・2026-09-08〜
09-09セッションサマリー**。前回2026-09-07サマリー〈下記ブロック、
コミット`5769026dc9`〉作成後の09-07 20:32に`[[QUALITY-GATES-EPIC-1]]`
ゲート1週次化の実地確認（CHECK-41本番稼働確認・追加対応不要と判断、
コミット`a21f5506b7`）を1件挟んでいるが、本ブロックでは重複記録しない。
非常に長時間のセッションで、根拠薄弱項目の体系的クローズ（計17件）・
保有銘柄関連の実バグ修正（KPI表示・DCF_Reliability誤判定）・複数の
根治的対応（falsy-zero横断調査・revenue/cost_of_revenueタグ選択の
根本修正・BS恒等式検証の全件トリアージ・自動検知チェックの新設）を
実施した（全てpush済み）:

1. **根拠薄弱項目の系統的クローズ（計17件、BACKLOG.md総数94→67件の
   一部）**:
   - 個別クローズ1件: `[[JOBY-BLADE-ACQUISITION-IMPACT-SCOPE-1]]`
     （JOBYの2025年Blade買収影響範囲確認、調査見送り・対応不要と判断）
   - 根拠薄弱バッチ2〜4（計11件、BACKLOG.md全86件へ判定基準を機械的
     適用）: `[[HYPECORE-SUBSTAGE-LAYER3-UNVERIFIED-1]]`・
     `[[LAYER3-SNPS-STALE-TAG-PRIORITY-1]]`・`[[LAYER3-UNEXPLAINED-
     SINGLE-TICKER-DIFFS-1]]`・`[[HON-GROSSPROFIT-2009-RESIDUAL-
     DISCREPANCY-1]]`（以上4件）・`[[VRT-REVENUE-2018-MISSING-1]]`・
     `[[RCAT-2016Q3-ORPHANED-QUARTERLY-FILE-1]]`・`[[LAYER3-RPO-
     CANDIDATE-ORDER-1]]`（以上3件）・`[[SECDATA-LEGACY-CIK-
     GRANULARITY-1]]`・`[[AMZN-CONVRATE-OVERRIDE-REVIEW-1]]`・
     `[[LITE-COGS-DA-TAG-UNMERGED-1]]`・`[[LAYER3-VISA-EPS-TAG-
     MISSING-1]]`（以上4件）。いずれも非保有銘柄・推測段階の懸念
   - 既存の安全策・後続実装により無効化された5件: `[[STOCKHTML-
     YTD-FILTER-BUG-SUSPECT-1]]`・`[[SEC-BKNG-SHARES-ANOMALY-1]]`・
     `[[LAYER3-CROSS-TAG-YEARLY-QUARTERLY-GENERAL-RISK-1]]`・
     `[[OPERATING-CASH-FLOW-CONTINUING-DISCONTINUED-GAP-1]]`・
     `[[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]`（登録時の懸念リスクが
     別の安全策・フォールバック機構により既に実害ゼロになっている
     パターンを横断調査して発見）
   - `[[ONDS-LOAR-SHARES-SCALE-SUSPECT-1]]`個別クローズ（上記とは別
     コミット。TANUKI VALUATIONがyf_implied優先で保護されていることを
     確認）

2. `[[TAIL-THESIS-KPIS-EMPTY-ADBE-APGE-1]]`（保有銘柄ADBE含む）:
   ADBE/APGEのthesis.jsonでkpisフィールドが未登録のため「## 監視KPI
   実績」セクション自体がレビュー文書から丸ごと消えていた表示バグを
   発見。第1段階の対症療法（thesis.jsonへの手作業コピー）は新規銘柄
   登録のたびに再発する構造的問題を放置すると判断し撤回、
   `_get_effective_thesis_kpis()`新設によるtail_kpi_map.jsonへの
   自動フォールバックという根治的修正に変更した。satellite残5銘柄
   （APP/CELH/CRWV/NVDA/SOUN）にも同型ギャップが波及することを確認し
   全7銘柄で解消。あわせて監視KPI実績表に直近4四半期の推移表示
   （`_build_kpi_trend_suffix()`）を追加

3. `[[POLICY-AB-TREND-BLIND-1]]`: `_calc_dcf_reliability_policy_b()`が
   上方乖離を一過性費用要因と誤って扱えず恒常的にLOW判定し続けていた
   トレンド好転検知不能バグを修正（直近2年連続黒字を主基準に追加）。
   全銘柄再生成で49銘柄がDCF_Reliability: LOW→NORMALに変化、うち45
   銘柄でClassificationもWATCHから変化。保有銘柄4つ（ADBE/CELH/PLTR/
   TSLA等）を含む。SOFI/XOM（下方乖離・継続赤字の正当な懸念）は
   引き続きLOWのまま

4. `[[FALSY-ZERO-PATTERN-SWEEP-1]]`・`[[MACRO-STYLE-FCF-ZERO-TRUTHY-
   EXCLUDE-1]]`: falsy-zeroパターンの横断調査を実施し新規発見2件を
   含む4箇所を修正、いずれも実装完了としてクローズ

5. `[[REVENUE-TAG-PRIORITY-FRAGILE-1]]`: revenue/cost_of_revenueタグ
   選択の根治的対応として四半期整合性tie-breakを新設。TDY
   FY2013-2015のrevenue誤取得を修正・クローズ

6. `[[PL-FIELD-CROSS-ACCN-PERIOD-MISMATCH-1]]`: 案a（候補タグ拡張＋
   gross_profitアンカー）・案c（2タグ合算バックフィル）・案d（BSY型
   revenue個別対応）を実装、残存4銘柄（MRVL/ONDS/RMBS(2019)/CRM）の
   追加調査対応で当初対象9銘柄・全15年度分（LRCX/AMD/KO/JNJ/RMBS/
   BSY/CRM/ONDS/MRVL）を完全解消。前回セッションで「これ以上の機械的
   対応が困難」と報告していたMRVL(2017)の判断を、revenue自体の
   restatement見落としが原因だったと訂正

7. `[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-CHECK-MISSING-1]]`:
   report_consistency_check.pyにCHECK-46（revenue−cost_of_revenue=
   gross_profitの算術整合性検証、WARN-46）を新設。実データ校正
   （tanuki=true全105銘柄・1034件のrevenue/cost_of_revenue/
   gross_profit三つ組）でCRM(FY2017、0.0005%)とCRM(FY2018、0.5741%)の
   間に約1000倍のギャップを発見し許容誤差0.1%を確定。この校正過程で
   CRM(FY2018)の新規未調査乖離を発見した（下記「次セッションの着手
   候補」参照）。2026-09-06〜09-07棚卸しで「軽微な更新余地」と
   報告していた本エントリを、今日の教訓を踏まえ自動検知チェックへ
   転換した形

8. `[[DEAD-CODE-AUDIT-BATCH-1]]`: 4件を一括判断。phase1_scan.py・
   backfill_history.py・quality_checker.pyは全リポジトリ参照ゼロを
   再確認し削除。report_txt_parser.pyは登録時「孤立モジュール」と
   判断していたが、CHAT_RULES.md標準フロー手順②としてCLI直接実行
   される現役の運用手順と判明（import文でのgrepでは検知できないCLI
   実行パターンの見落とし）、現状維持に訂正

9. `[[CHECK29-UNRESOLVED-23-MIXED-CAUSES-1]]`: 当初23件を全件最終
   トリアージ完了。PLTR(2019、BS恒等式diff_base_pct=133.45%・
   $2,127,231,000)を10-K原文（R2.htm）で個別調査し、乖離の正体を
   Temporary Equity区分の転換優先株式2値と特定したが、この2値は
   company_facts.jsonに次元付き開示のみで非次元版が一件も存在しない
   構造的制約（CDNS/INTU等で既に確立済みの既知パターン）により解決
   不可能と確定。残存6件（CART/V/CELH/ASTS2019等）もあわせて個別調査
   し、23件中7件は現行データソース・アーキテクチャの構造的制約で対応
   不可と確定（うち一部はconfig/warn_acknowledged.jsonへ登録し
   「未確認」表示を解消）

**次セッションの着手候補**:
- `[[MA-INTEGRATION-TAG-GAP-1]]`（ADBE含む境界近傍17銘柄、設計課題
  として保留中、いずれ向き合う必要あり）
- CRM(2018)のGP-COGS不整合（CHECK-46実装過程の実データ校正で新規
  発見、未確認のまま`config/warn_acknowledged.json`にも意図的に
  未登録）
- `[[SEC-SUBMISSIONS-DUAL-FETCH-1]]`（技術的負債、優先度低〜中）
- `Market_Pulse_Update.yml`観察の最終確認・`SEC_Data_Update.yml`次回
  サイクル確認（いずれも継続観察中、本セッションでは新規の実地確認
  なし）

**依頼書の前提の訂正**: 本ブラッシュアップの依頼書は「ONDS-LOAR-
SHARES-SCALE-SUSPECT-1・SN-TANUKI-DELAY-1・STONKS-SILO-PRICE-SCHEDULE-
LAG-SUSPECT-1・CWAN登録抹消等」を本日の対応項目として例示していたが、
git log（コミット日時ベース、本セッション範囲は09-08 17:52〜09-09
22:01と確認）を照合した結果、`[[SN-TANUKI-DELAY-1]]`・
`[[STONKS-SILO-PRICE-SCHEDULE-LAG-SUSPECT-1]]`・CWAN登録抹消は
いずれも2026-09-06〜09-07セッション（下記ブロック参照）で既に完了
済みの過去の作業であり、本セッションでの対応はなかったことを確認
した（`[[ONDS-LOAR-SHARES-SCALE-SUSPECT-1]]`のみ本セッションで実施、
上記1.参照）。依頼書側の記憶違いと判断し、本ブロックには実際の作業
内容のみを記録する。

**セッション終了時ブラッシュアップの検証結果**:
- BACKLOG.md/BACKLOG_DONE.md移設漏れ: 本セッションでクローズした27件
  全件が`### ✅ [ID]`パターンでBACKLOG_DONE.mdに存在し、BACKLOG.md側に
  アクティブヘッダーとして残存していないことを機械的に確認（漏れ0件）
- ID重複チェック: BACKLOG.mdとBACKLOG_DONE.md間でヘッダーIDが重複する
  ものは、既知の2件（`[[CONFIG-LOAD-SILENT-FALLBACK-1]]`・
  `[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]`、いずれも段階的完了・
  部分対応の意図的な分割）以外に新規重複なしを確認
- git status: クリーン（未コミット変更・未追跡ファイルなし）。
  origin/kaihatsuに1件のみ後行（自動マクロデータ更新）していたため
  fast-forward pullで追従。作業ディレクトリに`common/sec_data/data/
  --HELP`という空の不審ディレクトリ（09-09 14:56作成、gitは空
  ディレクトリを追跡しないため`git status`には現れない）を発見・
  削除した（コード変更ではないためコミットは不要）
- BACKLOG.mdアクティブ件数: 機械カウントで**67件**（前回09-06〜09-07
  時点の94件から27件減、本セッションのクローズ件数と一致）

詳細は各BACKLOGエントリ・BACKLOG_DONE.md「2026-09-08」「2026-09-08②」
「2026-09-09」〜「2026-09-09⑬」（完了）各節・PROJECT_STATUS.md参照。

---

最終更新: 2026-09-07（**セッション終了時ブラッシュアップ・2026-09-06〜
09-07セッションサマリー**。前回2026-09-05サマリー〈下記ブロック〉の
続き。実装・修正10件、BACKLOG.md全97件棚卸し（陳腐化2件クローズ・
疑義2件報告）、その副産物としてのCWAN登録抹消（AVGO・ENBに続く3件目の
同型ケース）を実施した（全てpush済み）:

1. `[[STONKS-SILO-PRICE-SCHEDULE-LAG-SUSPECT-1]]`クローズ
   （コミット`eae85f83d3`）
2. `[[REGISTER-FLOW-REDESIGN-1]]`クローズ（コミット`3cdea86b59`。
   TICKER-AUDIT-1・PREFLIGHT-CHECK-1とも解消済みとなり前提が変わった
   ため、前回セッションの申し送り通りクローズ可否を判断）
3. `[[TOOLTIP-INDEX-1]]`実装（コミット`545718d1bf`）:
   `tanuki_valuation/index.html`に`info-tooltip.js`を適用
4. `[[LAYER3-COGS-CANDIDATE-TAG-EXPANSION-1]]`実装（コミット
   `125262c4b8`）: `cost_of_revenue`候補タグをJOBY/CEG/CPRT限定で拡張
5. `[[CHECK-COVERAGE-2]]`実装（コミット`d5f015129e`）: DuPont分解の
   null銘柄検出consistency checkを追加
6. `[[THESIS-FIELD-1]]`修正（コミット`7c963420f7`）: NVDA
   `thesis.json`の`entry_price`欠損を解消・スキーマ記載を実態に修正
7. `[[DISCOVER-RESIDUAL-LINKS-1]]`修正（コミット`691ccc3c09`）:
   `index.html`・`site-header.js`に残るDiscoverリンク残骸を除去
8. `[[SN-TANUKI-DELAY-1]]`実装（コミット`2058326dd6`）: SNの
   `tanuki=false`を解除しTANUKI VALUATIONを有効化（対象銘柄拡大）
9. `[[DATA-JUMP-CHECK-GENERALIZE-1]]`実装（コミット`e27c4d413c`）:
   段差型急変検知（WARN-21相当）を売上総利益・CapExの2項目へ展開
   （WARN-44/45新設）。純利益・SBCは別方式の検討課題として
   `[[DATA-JUMP-CHECK-NETINCOME-SBC-1]]`へ切り出し
10. `[[STONKS-FINANCIAL-VECTORS-RELATIVE-1]]`対応（コミット
    `163449555a`）: `financial_vectors`のpercentile/angle/lengthが
    相対順位であることを`results.json`に明示

**BACKLOG.md全97件棚卸し**（依頼書に基づき「表層的事実確認＋診断結論の
検証」の2段階で全件を本文精読＋実コード照合）:
- Round1（コミット`94294fad7f`）: 34件検証・🟢有効30件・クローズ2件
  （`[[TAIL-KPI-PROPOSER-CORE-ONLY-GATE-1]]`・
  `[[SECDATA-STORAGE-FRAGMENTATION-1]]`）・疑義2件報告
- 残り63件を中断せず連続実施し全97件完了。最終結果:
  🟢有効93件・クローズ2件（上記Round1分）・⚠️疑義2件・軽微な更新余地2件
  - 疑義2件（内容は変更せず報告のみ）: `[[MARKETDATA-CWAN-FROZEN-
    DATA-SUSPECT-1]]`（→下記の通り最優先で個別調査・登録抹消まで
    実施）、`[[TTM-DATA-DRIFT-BEHIND-PIPELINE-1]]`（`[[LAYER3-COGS-
    CANDIDATE-TAG-EXPANSION-1]]`実装により発生した新事例だが、
    `[[LAYER3-ANNUAL-CLASSIFICATION-DROPS-DATA-1]]`が既に文書化済みの
    既知パターンと確認、記載はそのまま維持）
  - 軽微な更新余地2件（報告のみ、本セッションでは未対応）:
    `[[ANOMALY-PATTERN-CATALOG-1]]`（本日のJOBY/CEG/CPRT実例が
    型Aカタログに未反映）・`[[REPORT-CONSISTENCY-GROSSPROFIT-COGS-
    CHECK-MISSING-1]]`（本日追加のWARN-44/45は本エントリが求める
    revenue−COGS=gross_profitの算術整合性チェックとは別種で代替に
    ならない旨を確認）

**`[[MARKETDATA-CWAN-FROZEN-DATA-SUSPECT-1]]`個別調査・CWAN登録抹消**
（コミット`ba6c1fc8d7`「CWAN削除本体」・`0ae69e966f`「BACKLOG更新」）:
97件棚卸しで66日間の価格凍結データがTANUKI VALUATION最新出力へ実際に
混入していることが判明し優先度を引き上げ、個別調査した結果、
**CWAN（Clearwater Analytics）が2026-06-25にPermira・Warburg
Pincus主導で1株$24.55の非公開化買収を受けNYSE上場廃止**していたと
判明（SEC EDGAR `submissions.json`の`former_names`・yfinance
delisted検知・凍結価格が買収対価と一致等、複数独立ソースで裏付け）。
登録時点の診断「取得側の技術的異常」は誤りで、データパイプライン
自体は正常挙動（存在しない株価を取得できないのは当然の挙動）だった
ことを確定。AVGO・ENBに続く3件目の同型ケースとして「銘柄削除時の
必須手順」に従いtanuki/stonks_silo/eps/hypecore全パイプラインから
登録抹消（Step 0再洗い出しで新規3箇所を追加発見）、4パイプライン
全銘柄再生成、`[[MARKETDATA-CWAN-FROZEN-DATA-SUSPECT-1]]`をクローズ。
検証: `audit.py`/`report_consistency_check.py --fail-on-ng`/
`system_health.py`いずれもNG=0・CWAN起因の新規WARN無し、
`pytest tests/ -q`: 1139 passed。

詳細は各BACKLOGエントリ・BACKLOG_DONE.md「2026-09-06（完了）」・
PROJECT_STATUS.md参照。

**次セッションの着手候補**:
- `Market_Pulse_Update.yml`の実地確認（1週間分の観察がまもなく完了、
  最終確認）
- `SEC_Data_Update.yml`（週次yfinance突合、次回日曜サイクルでの実地
  確認）
- `[[DATA-JUMP-CHECK-NETINCOME-SBC-1]]`（純利益・SBC向け代替方式の
  検討、優先度低）
- `[[ANOMALY-PATTERN-CATALOG-1]]`への本日実例（JOBY/CEG/CPRT）反映
- `[[JOBY-BLADE-ACQUISITION-IMPACT-SCOPE-1]]`（COGS以外への波及確認、
  未着手）

---

最終更新: 2026-09-05（**セッション終了時ブラッシュアップ・2026-09-04〜
09-05セッションサマリー**。前回2026-09-03サマリー〈下記ブロック〉の
続き。実装・修正20件超、BACKLOG統合9クラスタ（21件→8エントリ）、
些末項目クローズ2件、ゲート実装2件を実施した（全てpush済み）:

**前回（2026-09-03）サマリーの要点再掲**（実際の完了日は2026-09-01〜02、
詳細は下記2026-09-03ブロック参照。日付の食い違いに注意——本ブロック
作成時に再確認した結果、実質的には09-01〜09-02の作業だったと判明）:
1. `risk_fetcher.py`・Discoverサブシステム撤去、`AVGO`/`ENB`管理対象
   除外（2026-09-01〜09-02完了）
2. `[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・`[[MACRODATA-LAYER-
   CONSTRUCTION-1]]`クローズ（コミット`d6f854634c`、2026-09-02完了）
3. `[[TRUST-SUMMARY-EPIC-1]]`クローズ（コミット`547736e376`・
   `39e9eee08f`・`7167df09ae`、2026-09-02完了。FCF-CONVRATE①②③実装完了）
4. `[[QUALITY-GATES-EPIC-1]]`ゲート1拡張（コミット`297ba95523`等、
   revenue/純利益へのyfinance自動照合横展開・CHECK-41新設・週次化）
5. ENB登録抹消・`exclusion_reason`列追加、`[[REGISTER-FLOW-REDESIGN-1]]`
   方針2・3（`register_ticker.py`新設）

**Market Pulseのローカル/GitHub Actions二重実行**
（`[[MARKET-PULSE-LOCAL-DUAL-EXEC-1]]`）: cron調整後の実地確認を継続中。
2026-09-02・09-03の2営業日確認時点では約1時間55分の遅延が継続しており
判断保留だった。**実地確認は引き続き継続観察中**（解消をまだ確認できて
いない、下記「次セッションの着手候補」参照）。

**本セッション（2026-09-04〜09-05）で新規に実施した内容**:

6. 個別バグ・データ品質是正8件:
   - `[[STONKS-PILLAR-THRESHOLD-MISMATCH-1]]`（コミット`67117ae1e7`）:
     `dq.score`専用の`deficitColor()`新設で`pillarColor`閾値不一致を
     解消（IOT/JOBYの表示色を実測確認）
   - `[[RPO-REVTTM-GATE-SKIP-1]]`（コミット`9b01402ec2`）: 懸念シナリオ
     が`get_rpo_context()`の構造上発生し得ないと判明・クローズ＋防御的
     コメント追加
   - `[[JNJ-XOM-PM-FLOOR-RISK-1]]`（コミット`11ad98f00e`）: 定点確認
     結果を追記、対象7銘柄で`floor_hit=False`継続を確認・**監視継続**
   - `[[ENTG-TER-SEGMENT-1]]`（コミット`8a33444325`・`cec6710ef8`・
     `b88cafe402`）: 10-K一次情報に基づき`segment_config.json`へ
     セグメント別成長率を追加、TANUKI VALUATION再生成
   - `[[EPS-LOAR-1]]`（コミット`ea44bfb081`）: IPO前株式数構造の別物
     四半期を株式数基準で除外
   - `[[BREAKEVEN-FORECAST-METHOD-MISMATCH-1]]`（コミット
     `f9c22d08ac`・`eea2b3a151`）: 黒字化年予測手法をTANUKI VALUATION・
     STONKS SILO間で統一（常時直近4点OLS回帰）
   - `[[LAYER3-COGS-STRUCTURAL-GAP-16TICKERS-1]]`（コミット
     `feac5bf0d4`）: 残10（実11）銘柄の一次情報裏取り調査を実施、
     新規2件（`[[LAYER3-COGS-CANDIDATE-TAG-EXPANSION-1]]`・
     `[[JOBY-BLADE-ACQUISITION-IMPACT-SCOPE-1]]`）を登録
   - `[[FYE-BOUNDARY-COLLISION-UNCONFIRMED-1]]`（コミット
     `b5a72aafe8`）: LITE/WSTを一次情報で確認、判定1で確定しクローズ

7. `[[QUALITY-GATES-EPIC-1]]`のゲート実装2件:
   - ゲート4（旧TICKER-AUDIT-1、コミット`21242ac3c7`）:
     `system_health.py::check_k_ticker_audit()`を新設し、①見直し候補
     （status=candidate&gt;30日）②検証由来・無保有③P4-CIKOrphan集約
     ④`monitor_tickers.yaml`同期漏れ検知を実装
   - ゲート0（`[[PREFLIGHT-CHECK-1]]`、コミット`acc4740837`）:
     `common/registration/preflight_check.py`を新設し、①上場後3年未満
     ②直近提出書式が10-K/10-Q以外③収益系XBRLタグ不在を`register_
     ticker.py`のStep 0.5直後で自動検知（自動停止はしない）。実データ
     検証でSN（2023年当時20-F提出企業）が登録日以降に10-K/10-Q提出企業
     へ既に移行済みと判明し、②ではなく①が発火することを確認・記録
   - いずれもBACKLOG_DONE.mdへ完了移設済み（`[[TICKER-AUDIT-1]]`は
     ゲート4付録へ全文統合、`[[PREFLIGHT-CHECK-1]]`は通常の完了移設）

8. `[[TICKER-LOADING-UNIFICATION-1]]`実装（コミット`aa8104a6c3`）:
   銘柄リスト読み込みの重複実装3箇所（①`system_health.py`
   ②`src/tail/kpi_proposer.py`・`sec_ctrl_fetcher.py`・
   `text_kpi_extractor.py`③`common/sec_data/config.py`）を`tickers.py`
   経由に統一。`tickers.py`に`get_cik()`・`get_all_rows()`・
   `get_registrable_tickers(flag=None)`を新設

9. 小規模技術的負債13件（2バッチ、コミット`58805c18b9`〜`847080a3b9`・
   `189024383e`〜`a6634ba1da`）:
   - 1バッチ目8件: `[[CLAUDE-CODE-START-FY-DESC-FIX-1]]`・
     `[[CHECK-FORMAT-1]]`・`[[ADMIN-LOG-1]]`・`[[PICK-FIELD-1]]`・
     `[[HISTORY-JSON-LEGACY-TANUKI-SCORE-1]]`・`[[TOBE-SEGMENTS-
     RESIDUAL-WORDING-1]]`・`[[BS-IDENTITY-LOG-NONDETERMINISTIC-
     KEY-ORDER-1]]`・`[[TTM-FLOW-FIELDS-FROZENSET-NONDETERMINISTIC-1]]`
   - 2バッチ目5件: `[[MOAT-CATALOG-DUP-1]]`・`[[EPS-267-MIXED-
     PASSTHROUGH-1]]`・`[[CHECK-COVERAGE-1]]`（CHECK-42として実装、
     原案のCHECK-20は既に別件で使用済みのため採番変更）・
     `[[STALE-CHECK-1-IMPL]]`・`[[ERP-DUAL-CALC-1]]`
   - 些末項目2件クローズ（コミット`8d2b0ba228`）: `[[TAIL-DETAIL-1]]`・
     `[[SILO-LEGEND-1]]`を対応不要としてクローズ

10. BACKLOG統合9クラスタ（21件→8エントリ、コミット`15e7c48ae2`・
    `7dda887292`・`f444437820`・`6508d6722a`・`290043ebc1`）:
    - DESIGN-2/4/5/6/14/15（6件）→`[[HYPECORE-EXPECTATION-
      FRAMEWORK-EPIC-1]]`
    - PREVENT-5・TICKER-AUDIT-1（2件）→`[[QUALITY-GATES-EPIC-1]]`
      付録（ゲート1・ゲート4）へ全文統合
    - UX-FLOW-1/MULTI-1/ARCH-1/EVAL-2/DESIGN-8-3/DESIGN-8-4（6件）→
      `[[FUTURE-FEATURE-IDEAS-CATALOG-1]]`
    - TANUKI-FIN-1・TANUKI-FIN-2（2件）→`[[TANUKI-FIN-2]]`に一本化
    - EPS-ANALYZER-INTEGRATE-1/RICE-INTEGRATE-1/ANALYST-VS-IV-
      INTEGRATE-1（3件）→`[[SCREENING-SIGNAL-INTEGRATION-EPIC-1]]`
    - LAYER3-ROIC-WACC-NONE-4TICKERS-1・FINTREND-SM-JOBY-NONE-1（2件）
      →`[[LAYER3-SM-SGA-SEPARATION-NONE-FALLOUT-1]]`
    - MACRODATA-FTSD-SERIES-ID-INVALID-1・MACRODATA-FETCH-FAILURE-
      VISIBILITY-GAP-1（2件）→後者`[[MACRODATA-FETCH-FAILURE-
      VISIBILITY-GAP-1]]`に一本化
    いずれも要約せず全文をそのまま転記し、検証スクリプトで元エントリの
    内容が一言一句欠落なく新エントリ内に含まれていることを機械的に
    確認済み。統合に伴う相互参照の更新漏れ（ダングリング参照）も
    その都度検知・修正した

詳細は各BACKLOGエントリ・BACKLOG_DONE.md「2026-09-04（完了）」
「2026-09-05①〜⑤（完了）」・PROJECT_STATUS.md参照。

**次セッションの着手候補**:
- `Market_Pulse_Update.yml`の実地確認継続（1週間の傾向確認、土曜分は
  クリア済み）
- `SEC_Data_Update.yml`（週次yfinance突合、次回日曜サイクルでの実地
  確認）
- `[[REGISTER-FLOW-REDESIGN-1]]`のクローズ可否判断（TICKER-AUDIT-1・
  PREFLIGHT-CHECK-1とも解消済みのため前提が変わった）
- `[[QUALITY-GATES-EPIC-1]]`の残り（ゲート0オーケストレーション
  磨き込み、Phase 4/ゲート3は優先度低のまま）
- BS項目整合性系4件・MACRO PULSE系2件（統合せず個別対応が必要と
  判断済み）

---

最終更新: 2026-09-03（**セッション終了時ブラッシュアップ・本日
セッションサマリー**。9項目を順次実施した（全てpush済み）:

1. `[[RISK-EVENTS-REMOVAL-1]]`・`[[DISCOVER-SUBSYSTEM-REMOVAL-1]]`:
   前セッションからの続き（2026-09-01〜09-02にpush済み）。詳細は
   下記の2026-09-02付ブロック参照
2. `[[MARKET-PULSE-LOCAL-DUAL-EXEC-1]]`の実地確認（コミット
   `1f92b9015e`）: cron変更後2営業日分（09-02完了JST08:31・09-03完了
   JST08:30）を確認したところ、想定JST6:35頃に対し約1時間55分の遅延が
   継続していた。2日分のみでは恒常的な傾向か一時的かの判断材料として
   不十分なため、**判断は保留**とし次回確認を1週間程度後に設定
   （申し送り、下記「次セッションの着手候補」参照）
3. `[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`: 前セッションで管理対象
   除外により解決済み（詳細は下記の2026-09-02付ブロック参照）
4. `[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・`[[MACRODATA-LAYER-
   CONSTRUCTION-1]]`クローズ（コミット`d6f854634c`）: 本文で「完了」
   「完成」と明記済みなのに優先度「高」のアクティブなエントリとして
   残置されていた2件をBACKLOG_DONE.mdへ移設。依頼文の「参照先が壊れて
   いる」という前提を検証した結果、実際は既に完了・正しく存在して
   いたと判明し前提を是正した上でクローズ
5. `[[TRUST-SUMMARY-EPIC-1]]`クローズ（コミット`547736e376`・
   `39e9eee08f`・`7167df09ae`、2026-09-02完了）: データ信頼性3段階
   （入力完全性・成長率算出・FCF/DCF計算）を貫通する可視化EPIC。
   FCF-CONVRATE①（rate_is_sector_defaultフラグ）②（report.txt/
   stock.html表示不一致修正）③（fcf_conversion_config.jsonへ
   Damodaran比較メタ情報追加）を実装完了。判断保留中だった4件
   （GROWTH-SANITY-CLASS-SYNC-1・FY52WEEK-BS-NULL-SILENT-1・
   MRVL-2019-2020-NULL-1・EPS-ANALYZER-NORMALIZE-SCOPE-1）が別セッション
   で既に解消済みと確認した上でEPIC自体をクローズ
6. `[[QUALITY-GATES-EPIC-1]]`ゲート1拡張（コミット`297ba95523`・
   `6fa8c3905f`・`8aefa4ef2c`）: yfinance自動照合を`operating_income`
   単体から売上高・純利益へ横展開（CHECK-41新設）。全99銘柄実測で
   p50=p95=0.0%（operating_income側p95=81%より大幅に狭い分布、依頼書の
   仮説を裏付け）。SECデータは週1回更新なのにyfinance突合が毎日実行
   されていた無駄を`--include-yfinance-checks`フラグで是正し
   `SEC_Data_Update.yml`（週1回）のみに限定
7. ENB登録抹消・`exclusion_reason`列追加（コミット`62aa662102`・
   `f0c4b18e09`・`c55505dae9`）: `[[QUALITY-GATES-EPIC-1]]`ゲート0対応・
   `[[REGISTER-FLOW-REDESIGN-1]]`方針4。ENB（カナダのIFRS/40-F提出
   企業、SEC annual data 0件のまま孤立登録）をBXと同じA案（登録抹消）
   で解消。`cik_lookup.csv`に`exclusion_reason`列を新設しRKLB/ZS/SN/
   APGEの4銘柄に除外理由を記入。`stonks_silo=false`78銘柄は個別判断
   ではなく設計方針そのものと確認し`SYSTEM_MAP.md`に一般方針として
   文書化
8. `[[REGISTER-FLOW-REDESIGN-1]]`方針2・3（コミット`8bfab35919`・
   `fc17699fd6`・`0b6bc3f211`・`488dd94640`）: `cik_lookup.csv`の
   status列に`provisioning`（登録処理中）を追加し4大パイプラインの
   バッチ対象から除外、`registration_validator.py --promote`でNG=0
   確認後に昇格する仕組みを新設。`common/registration/register_
   ticker.py`（新規銘柄登録オーケストレーションスクリプト）を新設し
   Step 1〜8を自動連続実行、Step 2.5・3.5はClaude Codeの10-K確認を
   前提に一時停止する設計とした。実装過程で`_INVALID_STATUSES`変更が
   既存の安全弁（ZS-TICKERS-LEAK-1由来のCLI引数フィルタ）と衝突し
   オーケストレータ自身がprovisioning中ティッカーを処理できなくなる
   自己矛盾を実地検証（HIMS、一時ブランチ）で発見・`get_registrable_
   tickers()`新設で解消。`[[REGISTER-FLOW-REDESIGN-1]]`対応方針5件が
   全件完了・実質達成となった

詳細は各BACKLOGエントリ・BACKLOG_DONE.md「2026-09-03（完了）」・
PROJECT_STATUS.md参照。**次セッションの着手候補**:
- `Market_Pulse_Update.yml`の実地確認継続（1週間程度分の遅延傾向蓄積・
  土曜朝の金曜分データ反映確認）— 引き続き最優先の申し送り事項
- `[[QUALITY-GATES-EPIC-1]]`ゲート0残り（`common/sec_data/update.py`が
  statusを見ずSEC取得自体はprovisioning中でも行われる既知のギャップは
  実害小のため対象外のまま。「登録時点でのsubmissions API照会による
  機械的ブロック」自体は[[REGISTER-FLOW-REDESIGN-1]]の対応方針5件の
  範囲外で未着手）
- `[[SECDATA-LEGACY-CIK-GRANULARITY-1]]`（MRVL/DELL旧CIK拡張データの
  粒度確認、優先度低〜未定、着手条件なし）
- `[[SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1]]`（優先度低、
  意図的据え置き中）
- `tests/test_flag_consumer_audit_3.py`のENB言及docstring
  （`test_production_enb_excluded`、ENBが実際には未登録〈削除済み〉に
  変わったための軽微な記述陳腐化。テスト自体は引き続き正しく通過して
  おり機能への影響はない、着手要否は未定）

最終更新: 2026-09-02（**セッション終了時ブラッシュアップ・
2026-09-01〜09-02セッションサマリー**。指示書4件を順次実施した
（全てpush済み）:

1. `[[DISCOVER-SUBSYSTEM-REMOVAL-1]]`: Discoverサブシステム
   （ニュース収集・カタリスト発掘・株価インパクト予測）本体を削除
   （`src/discover/`3ファイル・`docs/discover/`一式・関連ワークフロー
   2件・関連テスト）。一次情報でなくGrok生成コンテンツへの依存を
   減らす方針の一環（risk_fetcher.py撤去と同系統）。
   `config/discover_config.json`はDiscoverサブシステム固有ではなく
   `registration_validator.py`・`docs/portfolio/index.html`が参照する
   共有のティッカー区分設定ファイルと確認し削除対象外に明示的に除外。
   関連する既存タスク7件（`CATALYST-DEDUP-1`・`DISCOVER-UTCJST-
   DATE-MISMATCH-1`・`DISCOVER-IMPACT-PRED-GAPS-1`・`REPORT-
   CATALYST-1`・`UI-DISCOVER-1`・`DISCOVER-PRECISION-GAPS-1`・
   `DESIGN-16`）もあわせてクローズ。実装中に発見した`tests/test_flag_
   consumer_audit_3.py`のcatalyst.py依存テスト1件・SYSTEM_MAP.mdの
   追加陳腐化2箇所は承認を得た上で対応
2. `[[RISK-EVENTS-REMOVAL-1]]`: `risk_fetcher.py`（Grok web検索による
   簡易リスクイベント取得）を撤去。一次情報でなくGrok応答をそのまま
   採用しており信ぴょう性が低く、IV・DCF・TANUKI SCORE等の計算系の
   いずれにも使われていない表示専用機能だったため。`pipeline.py`の
   配線一式・`stock.html`のカード表示・`report_txt_parser.py`の
   パーサーを削除し、全100銘柄フルパイプライン再生成でrisk_events
   削除以外の実質差分0件を確認
3. `[[MARKET-PULSE-LOCAL-DUAL-EXEC-1]]`: 「朝7時頃に最新データを見たい」
   という要望の調査中、Market Pulseの実際の更新元がGitHub Actions側の
   cronではなく、Koichiさんのローカル環境で稼働するWindowsタスク
   スケジューラ「MarketPulse_Update」（2026-05-06作成、リポジトリ
   管理外）であることが判明。同スクリプトはFG_Level2（現在稼働して
   いないシステム売買検討）向けの目的で二重実行されていた。ローカル
   タスクはKoichiさんが別途削除し、GitHub Actions側`Market_Pulse_
   Update.yml`のcronをUTC21:25へ変更して単独化。**その直後、
   `Market_Data_Daily_Update.yml`（同じくUTC21:25）と完全に同時刻に
   なり、両ワークフローとも末尾でgit push処理を持つためpush競合
   リスクを一度作り込んでしまったことに気づき、UTC21:35（2026-08-29
   時点の10分差設計）へ即座に是正した**
4. `[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`: AVGOの旧CIK登録
   （`legacy_ciks=["1054374"]`）が無関係な買収先企業（Broadcom
   Corporation）のデータを指しており、真の前身企業（Avago
   Technologies LTD, CIK 1441634）と決算期が不一致という誤統合が
   発覚。Koichiさんの判断により、データ境界の是正（CIK差し替え等）
   ではなくAVGO自体をOn-a-journey管理対象から除外する方針で解決した。
   ポートフォリオ・TANUKI TAILいずれにも保有・監視登録されていない
   ことを確認済み。既存の「銘柄削除時の必須手順」を実際に適用する
   過程で、手順に載っていない設定ファイル・データファイルが9件
   （`maturity_config.py`・`growth_sanity.py`・`common/market_data/`
   配下等）見つかり、Step 0（削除前の全参照洗い出し）新設を含め
   手順書自体を恒久拡充した

**副次発見**: `[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]`の本文が
「残タスク（Stage 2〜3、未着手）」のまま2026-08-05時点から更新されて
おらず、実際にはStage 2・3・3a・3bまで全て実装完了していたという
陳腐化をAVGO対応の過程で発見・訂正した。真の残タスクの精査中、
「MRVL/DELL旧CIK拡張データの粒度確認」が正式ID未採番のまま宙に浮いて
いたことも発見し、`[[SECDATA-LEGACY-CIK-GRANULARITY-1]]`として新規
登録（優先度低〜未定）。

詳細はBACKLOG_DONE.md「2026-09-01（完了）」〜「2026-09-02②
（完了）」・PROJECT_STATUS.md参照。**次の本線は未定**。次セッションの
着手候補:
- `Market_Pulse_Update.yml`の次回平日サイクルでの実地確認（cron変更後
  の完了時刻がJST7:00前か、次の土曜朝に金曜分データが反映されているか）
- `[[SECDATA-LEGACY-CIK-GRANULARITY-1]]`（MRVL/DELL旧CIK拡張データの
  粒度確認、優先度低〜未定、着手条件なし）
- `[[SPAC-SHELL-MAINTAINED-FIELDS-FREEZE-CONSIDERATION-1]]`（優先度低、
  意図的据え置き中）
- `[[QUALITY-GATES-EPIC-1]]`（唯一の最高優先度エピック、Phase 4実装の
  着手要否が複数セッションにわたり判断待ちのまま）

最終更新: 2026-08-27（**セッション終了時ブラッシュアップ・本日
セッションサマリー**。指示書5件を順次実施した（全てpush済み）:

1. バックログ再分析＋精度改善2件（`[[LIQUIDITY-CSV-FIRST-ROW-
   UNBOUNDLOCALERROR-1]]`修正）— この過程でSTONKS SILOの
   `results.json`が2026-08-13から更新されていないことに気づき、
   **STONKS SILOが2026-07-13以降45日間・約30回連続でGitHub Actions
   自動更新に失敗し続けていた実障害**（`[[STONKS-SILO-CLI-TICKERS-
   SHADOW-1]]`、pipeline.pyの変数名衝突）を新規発見
2. STONKS SILO 45日間停止の緊急復旧（Koichiさん承認済み）: 修正・
   全25銘柄再生成、45日ぶりの正常完走を確認
3. report.txt網羅性拡充8件（CAPM-IV/DuPont/sensitivity/
   maturity_profile/return_metrics/validation/alpha_was_capped/
   fcf_ttm_end）＋セグメントKPIテーブル配線修正 — 配線修正の過程で
   `[[KPI-FETCHER-SEGMENT-SOURCE-ORPHANED-1]]`・`[[DUPONT-TTM-
   FIELD-CASE-MISMATCH-1]]`（DuPont分解が全104銘柄で恒久的に
   未発火）を新規発見
4. セグメントKPIテーブル機能の残骸撤去: Koichiさんとの対話で
   「KPI＝XBRL会計セグメントデータ」という設計前提自体が誤りだったと
   判明（本来のKPIは決算資料の文章に開示される企業固有の経営指標）。
   一から作り直す前提で既存実装（`kpi_fetcher.py`・`kpi_config.py`・
   `common/sec_data/segment_fetcher.py`・stock.html表示コード）を
   撤去。新機能の着手は見送り、構想は`[[SEGMENT-KPI-NARRATIVE-
   EXTRACTION-FUTURE-IDEA-1]]`に記録
5. DuPont分解のPascalCase/snake_caseキー不一致修正: 既存回帰テストの
   モックデータが同じ誤りを自己整合的に再現していたため本番不具合を
   検知できていなかったと判明（`CHAT_RULES.md`事例15、Koichiさんの
   提案で追加）

本日の特徴（記録として重要）: 当日の緊急復旧タスク（2）は、想定外の
実害（45日間の本番停止）を無断で拡大せず一旦報告してからKoichiさんの
承認を得て対応した事例。タスク3〜5は「配線・呼び出しは実装したが、
呼び出し先の内部データソースが別の理由で陳腐化していて結局動かない」
という構造が連続して見つかった（`[[KPI-FETCHER-SEGMENT-SOURCE-
ORPHANED-1]]`・`[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]`）。

詳細はPROJECT_STATUS.md「2026-08-27」・BACKLOG_DONE.md「2026-08-27
（完了）」参照。**次の本線は未定**。次セッションの着手候補:
- `[[DUPONT-TTM-FIELD-CASE-MISMATCH-1]]`修正の全銘柄反映確認
  （tanuki_score DuPontパネル・one-time-gain-trap検知の実発火確認）
- `[[STONKS-SILO-PRICE-SCHEDULE-LAG-SUSPECT-1]]`の再検証（cron正常化
  により実データ検証が可能になった、次回平日の自動実行後に着手）
- `[[WORKFLOW-SEC-TANUKI-GAP-1]]`のSEC_Data_Update起点チェーン再確認
  （2026-08-30サイクル後に着手可能）
- `[[SEGMENT-KPI-NARRATIVE-EXTRACTION-FUTURE-IDEA-1]]`（優先度保留、
  Koichiさんの着手判断待ち）

最終更新: 2026-08-26（**セッション終了時ブラッシュアップ・本日
セッションサマリー**。当日午前のMarket Pulse/MACRO PULSE既知バグ6項目
再検証に続き、以下4件の指示書を順次実施した:
1. 層単位/フロントエンド単位の方法論導入（`CHAT_RULES.md`事例13、
   `SYSTEM_MAP.md`依存関係マップ新設）
2. 実地確認2件: `[[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]`クローズ、
   `[[WORKFLOW-SEC-TANUKI-GAP-1]]`は下流チェーンのみ確認・SEC起点
   チェーンは未確定のため現状維持、`[[Q4-IMPLIED-CALC-TRIPLICATION-1]]`
   クローズ
3. `[[REPORT-TXT-CAPM-IV-MISSING-1]]`対応要否調査（実装なし）・
   Playwright実ブラウザ確認の体系的拡張（`browser_checks/`新設）
4. 本ブラッシュアップ（本節末尾に記載の陳腐化記載2件を訂正）

詳細はPROJECT_STATUS.md「2026-08-26②」・BACKLOG_DONE.md「2026-08-26
（完了）」参照。**次の本線は未定**。次セッションの着手候補:
`[[WORKFLOW-SEC-TANUKI-GAP-1]]`の再確認は2026-08-30（次回日曜の
`SEC_Data_Update`サイクル後）以降まで着手不可。それ以外は
`[[REPORT-TXT-CAPM-IV-MISSING-1]]`の対応要否判断（Koichiさん判断待ち）・
BACKLOG.md「残タスク」節の低優先度課題群から選択。

最終更新: 2026-08-22（**セッション終了時ブラッシュアップ・本日
セッションサマリー**。5件を実施した:

1. `[[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]`: GP法への
   `RestructuringCharges`控除実装（コミット`2eda3df90`）＋対象10銘柄の
   本番データ再生成（`610c18c71`）。Layer3側の同型ギャップは
   `[[LAYER3-OI-RECONSTRUCTION-FALLBACK-GAP-1]]`へ切り出し
2. `[[QUALITY-GATES-EPIC-1]]` Phase 4（ゲート3）対象棚卸しを本線として
   実施・完了（`bca010a89`）。対象計算式12式のテストカバレッジ状況を
   確認。詳細は`CHAT_RULES.md`「本線の定義」節参照。次の本線は未定
3. `[[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]`（PORTFOLIOページの
   時価が1営業日遅れる問題を新規発見・登録）・`[[WORKFLOW-SEC-TANUKI-
   GAP-1]]`（既存、SEC_Data_Update連携欠如）を、`workflow_run`連鎖化で
   まとめて構造的に解消（`ca925ffa2`）。ただし実際のGitHub Actions発火
   確認は次回発火サイクル待ちのため「実装完了・実地確認待ち」のまま
   BACKLOG.mdに残置（BACKLOG_DONE.mdへの移設は未実施、意図的）。
   副次発見の`[[STONKS-SILO-PRICE-SCHEDULE-LAG-SUSPECT-1]]`も登録
4. BACKLOG.md重複登録の訂正: `[[RECESSION-SCORE-TRIPLE-CALC-1]]`
   （2026-07-23登録）と`[[MACRO-PULSE-ZONE-25-STALE-1]]`（2026-08-21
   登録・実装）が対応方針①②で内容重複していたことが判明
   （`CHAT_RULES.md`事例10として教訓化）。`5be83e2f3`で訂正、
   `RECESSION-SCORE-TRIPLE-CALC-1`は①②解消済み・③のみ残存の状態へ
   優先度を高→中に見直し
5. `[[MACRO-PULSE-3M-FORECAST-SNAPSHOT-MISMATCH-1]]`: 「3ヶ月先の
   マクロ予測スコア」（ブラウザ側ライブ再計算）と「AIウィークリー
   コメンタリー」（週1回のサーバー側スナップショット）のスコア不一致を
   新規発見・登録の上、同日中にパイプライン統合で解消（`1d68c7342`）。
   副次発見として`[[MACRO-PULSE-ZONE-25-STALE-1]]`の見落とし6箇所目
   （スコア解説ツールチップの残存「25」）も同コミットで修正

**次の本線は未定**（`CHAT_RULES.md`「本線の定義」節参照。2026-08-16
時点の本ファイル内の旧「次セッションの着手候補」リストは6日分の
セッションを経て陳腐化しているため、次セッション開始時は同節と
BACKLOG.mdの優先順位から改めて判断すること）。

詳細は各BACKLOGエントリ参照（1・4・5は✅付きでBACKLOG.mdに残置
——既存の同型9件と同じく、まだ開いている兄弟課題〈[[LAYER3-OI-
RECONSTRUCTION-FALLBACK-GAP-1]]・[[RECESSION-SCORE-TRIPLE-CALC-1]]③〉
への相互参照が密なためBACKLOG_DONE.mdへは移設していない。2は
`CHAT_RULES.md`「本線の定義」節にのみ記録、3は実地確認待ちのため
未✅）。

最終更新: 2026-08-21（**架空日付の訂正**。2026-08-20の長時間セッション
中、Cowork側の指示文に実在しない日付（`2026-08-21`〜`2026-08-24`）が
書かれ、Claude Code側もシステム日付を確認せずそれをBACKLOG.md・
SYSTEM_MAP.md・config/*.json・コードコメントへ反映していたことが判明。
実際には全て`2026-08-20`の同一日の作業だった（システム日付・
git commit日時とも2026-08-20 20:01〜21:46 JSTの範囲に収まる）。
架空日付を含んでいた箇所を`2026-08-20`（同一日内の複数作業は
`2026-08-20②`のような連番表記）へ訂正した。**git履歴は書き換えていない**
——force pushや`--amend`は使わず、現在のファイル内容のみを訂正する
新規コミットで対応した。コミットメッセージ本文には元々日付を書いて
いなかったため訂正対象はない一方、**過去4コミット
（`f6f2358d0`・`f801fe792`・`26902b6e7`・`dbcaae0be`）が生成した
当時のBACKLOG.md・SYSTEM_MAP.md等のファイル内容（各コミットの
スナップショット）には、架空日付を含んだままの記述が残っている**
（`git show <commit>:BACKLOG.md`等で参照した場合）。現在のHEADの
ファイル内容は訂正済みだが、過去のコミット時点のスナップショットは
訂正されない（履歴を書き換えていないため）。今後、特定コミット時点の
ファイル内容を参照する際は、実際の作業日時はgit commit日時
（`git log --format="%ci"`で確認可能、いずれも2026-08-20
20:01〜21:46 JST）を正とすること。詳細は`CHAT_RULES.md`
「BACKLOG記載の前提は着手時に再検証する」事例8・
`[[QUALITY-GATES-EPIC-1]]`関連の各BACKLOGエントリ参照。実装コード
変更なし、記録の訂正のみ）

最終更新: 2026-08-20（**`[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-
GAP-1]]`Step 4: coreへのLayer3適用＋全10銘柄レビュー再生成**、
コミット`2378f5e86`。前日実装したsatellite向けLayer3機械的照合
（`route_rejected_to_layer3()`）と同型の照合をcore 3銘柄
（PLTR/SOFI/TSLA）の既存`missing_kpis`7件にも適用した。

**実装**: `kpi_proposer.py::propose_kpis()`は再実行せず（Grokに
既存core設定を作り直させると精度を壊すリスクがあるため）、
`tail_kpi_map.json`の既存登録エントリの`revenue_tag`を`sec_concept_
definitions.json`のcandidatesと直接照合し3件を`source: "layer3"`へ
書き換え: PLTR株式報酬費用（`layer3_field`）・SOFI GAAP純利益
（`layer3_field`）・PLTR営業利益率（`layer3_formula: "operating_
income/revenue"`、除算のみ対応のformulaで表現可能と判定）。
`xbrl_segment_fetcher.py`本番実行でmissing_kpis実測: PLTR 3→1、
SOFI 4→3、TSLA 0→0（core計7→4）。PLTR株式報酬費用$201,592,000を
生タグと突合し完全一致を確認。

**意図的に未対応とした4件のうち2件が新規BACKLOG登録につながった**:
PLTR希薄化後EPS成長率はYoY系列比較が必要だが現行`layer3_formula`は
除算のみ対応で表現不能→`[[TAIL-LAYER3-FORMULA-YOY-UNSUPPORTED-1]]`
新規登録。SOFI Technology Platform売上成長率は`revenue_tag`が文字列上
Layer3の会社全体`revenue`フィールドと一致するが、`dimension`がセグ
メント指標であるため意図的に除外——この過程で`route_rejected_to_
layer3()`が`dimension`を確認せずタグ名だけで照合するため、satellite側
に同型の誤同定（APP「継続営業利益」・CELH「機能性エナジードリンク
売上」がいずれも会社全体データにすり替わっている）が既に2件実在する
ことを発見し`[[TAIL-LAYER3-ROUTING-DIMENSION-BLIND-1]]`として新規
登録した（本タスクでは回避したが、satellite側の既存2件は未修正のまま
残っている）。

**全10銘柄（ADBE/APGE/APP/CELH/CRWV/NVDA/PLTR/SOFI/SOUN/TSLA）の
2026Q2レビューを再生成**。NVDAで先行確認し「KPIデータが一切ない」旨の
文言が完全に消えたことを確認した上で残り9銘柄を実施。**recommendation
が変わったのはAPGEのみ（EXIT→WATCH）**——旧レビューはKPIデータの
完全欠如そのものをEXIT判定の直接根拠にしていた
（`optimism_bias_warning: "データゼロ状態での保有継続は根拠なき楽観
バイアスの典型例"`）が、新レビューは現金残高QoQ+243%・営業CF QoQ改善
11.4%等の実データに基づきWATCHへ変化した。他9銘柄はrecommendation
不変だが、satellite側は旧`summary`が全て「KPIデータが一切ない/提示
されていない」で始まっていたのに対し、新`summary`は実データに基づく
具体的な懸念・肯定材料の記述に全面的に置き換わった。TSLAは今回KPI
変更がなくscoreも42点で完全に不変——対照確認として機能している。
詳細な変化一覧はBACKLOG.md`[[TAIL-XBRL-SEGMENT-FETCHER-NONDIMENSIONED-
GAP-1]]`「全10銘柄レビュー再生成」参照。

検証: pytest 781 passed/2 known-failed（`[[TEST-STALE-IV-1]]`既知
例外、回帰なし）、`audit.py` NG=0（既存🟡警告5銘柄は今回変更対象外）、
`report_consistency_check.py --fail-on-ng` NG=0（WARN-38がPLTR/SOFIの
新しいmissing_kpis件数に正しく追従していることを確認）。

最終更新: 2026-08-19（**セッション終了時ブラッシュアップ・
`986230663`以降のセッションサマリー**。BACKLOG棚卸し・
`[[QUALITY-GATES-EPIC-1]]`本線化判断・本線3（ゲート1適用範囲拡大）の
第一歩を実施した。詳細は各BACKLOG項目・BACKLOG_DONE.md
「2026-08-19（完了）」参照）。

**BACKLOG棚卸しと本線判断**:
- `[[RISK-FREE-RATE-HARDCODE-1]]`優先度高→低に訂正・現状維持でクローズ
  （`b999b95b5`）
- `CHAT_RULES.md`「BACKLOG記載の前提は着手時に再検証する」に事例4を
  追加（`41220a326`）: `[[QUALITY-GATES-EPIC-1]]`の「Phase完了」記録が
  実質を伴っていなかった型の教訓（事例1〜3の「記述の誤り」とは別種）
- 優先度中以上78件のスクリーニング・グループA 8件の深掘り
  （`fe41b888a`、前回セッションからの継続）
- `[[QUALITY-GATES-EPIC-1]]`のPhase完了記述を実コード確認に基づき訂正
  （`5edae53aa`）: ゲート0（登録適格性の機械化）が未着手のまま、
  ゲート1（外部ソース自動照合）も損益計算書の中核項目には未適用
  だったことが判明
- 本線3を設定（`7ccc4f8c7`）: `[[QUALITY-GATES-EPIC-1]]`ゲート1の
  適用範囲拡大（`operating_income`単体から着手）

**本線3の第一歩とその連鎖**:
- CHECK-35へyfinance照合を追加（`78ce4e314`/`457ba1986`）
- 実装翌日、`row.iloc[0]`を無条件に「直近確定年度」とする期ズレバグを
  自己発見・修正（期末日ベースの照合＋52/53週決算企業向け±10日許容窓、
  `4eeaf6d70`/`3387a030b`）——KLAC/COHRで誤った乖離率を報告していた
- 期ズレ修正後の正しい実測により、フォールバック向き（GP法↔pretax
  調整法の優先順位）が逆だったと判明。案A（GP法優先への反転）を実装
  （`eae181b42`）
- 反転直後の全銘柄再生成でVRT FY2018が`revenue=0`×`gross_profit`巨額
  負値という内部矛盾入力から誤った値（-$42.87億）を生成する事故を検知。
  net_income比較という代理判定ではなく、GP法の入力そのもの
  （revenue↔gross_profitの整合性）を確認する案Dを追加実装
  （同`eae181b42`、全銘柄再生成`d7cf74e74`、LLY/COHR再計算
  `233405547`、記録`c8c40cdec`）
- タグ候補の実測調査（GP法675件・pretax法979件のticker-yearで
  バックテスト）: GP法は`RestructuringCharges`のみが一貫して改善に
  寄与（該当行の61%で改善）、他の候補・pretax法側の候補拡充は改善より
  悪化する行の方が多く見送りと判断（`eda43b046`）

**新規登録**:
- `[[VRT-REVENUE-2018-MISSING-1]]`（VRT FY2018のrevenue取得失敗、
  優先度低・登録のみ）
- `[[OI-RECONSTRUCTION-MISSING-OPEX-LINES-1]]`（営業利益再構成が別建て
  営業費用を見落とす問題、優先度中、`4fc5b24ec`/`eda43b046`）
- `[[LAYER3-ANNUAL-CLASSIFICATION-DROPS-DATA-1]]`（Layer3の年次期間
  分類が実在データを取りこぼす問題、原因未特定、優先度中〜高、
  `eda43b046`）

**この日の特徴（記録として重要）**:
- 本セッションで発見した欠陥6件（期ズレバグ・フォールバック向き誤り・
  VRT入力破損・GP法の構造的弱点・pretax法候補拡充の逆効果・Layer3の
  データ取りこぼし）は、**いずれも別作業中の偶然または自己検証による
  発見**であり、`[[QUALITY-GATES-EPIC-1]]`が根絶しようとしている
  「発見手段が偶然に依存する」構造が現在も続いていることの実例
- ただし今回は**ゲート1（yfinance照合）が実装初日に本線1（操作利益
  抽出根本修正）の設計判断の誤りを検出**しており、ゲートの有効性が
  実証された初のケースでもある
- **実測により「実装しない」と判断したものが複数ある**（pretax法の
  タグ拡充4候補・GP法の候補のうち4/5・Layer3への一律フォールバック
  追加）。実測せず直感で実装していれば、精度を下げる変更を入れていた
  可能性が高い（特にpretax法は4候補全てで中央値誤差が悪化した）

最終更新: 2026-08-16（**セッション終了時ブラッシュアップ（3回目）・
`986230663`以降のセッションサマリー**。BACKLOG棚卸し〈案1→案2〉、
本線1`[[OPERATING-INCOME-EXTRACTION-GAP-1]]`、本線2
`[[MOAT-SCORE-PARTIAL-NULL-1]]`を実施した。詳細はBACKLOG_DONE.md
「2026-08-16（完了）」参照。

**BACKLOG棚卸し（案1→案2）**:
- `[[RISK-FREE-RATE-HARDCODE-1]]`優先度高→低に訂正・現状維持でクローズ
  `b999b95b5`
- CHAT_RULES.md「BACKLOG記載の前提は着手時に再検証する」新設
  `1ad7977f0`
- 優先度「中」以上78件をスクリーニングし「要深掘り」14件のうち
  グループA 8件を深掘り調査。優先度引き下げ2件・完了移設1件
  （`[[SECDATA-COMPANYFACTS-OVERLOOKED-1]]`は登録翌日に実質解消済み
  だった移設漏れと判明）・根拠強化1件・検証記録4件を実施
  `fe41b888a`
- **未着手**: グループB（`[[QUALITY-GATES-EPIC-1]]`・
  `[[TRUST-SUMMARY-EPIC-1]]`・`[[BACKTEST-SCORE-1]]`のエピック3件）は
  「記述の誤り」ではなく「着手要否の判断」が必要な性質のため保留

**本線1: `[[OPERATING-INCOME-EXTRACTION-GAP-1]]`**（登録`16a4b3a6e`、
本線設定`06333eab2`、実装`86d5011a9`/`01e417d75`/`bbc23ff1e`、
記録`962471031`）:
- `common/sec_data/parser.py`の`operating_income`抽出が単一タグ
  `OperatingIncomeLoss`依存でフォールバック不能だった問題を解消。
  GP法（`gross_profit-R&D-SGA/SM`）・pretax調整法
  （`pretax-非事業性項目`）で再構成し、**両手法の突き合わせ検証**
  （乖離の50%以上が非事業性項目で説明できること）を通ったものだけ
  採用する設計とした（pretaxをそのまま使わない）。COHRは5.7倍の乖離で
  GP法の値を棄却し保守的なpretax調整法にフォールバック
- **妥当性ガード**を追加: `reconstructed_pretax < net_income`なら
  不採用（SOFI等、受取利息が本業収益である金融/フィンテック企業で
  「受取利息は非事業性」という仮定が崩れるため。検証中に発見）
- CHECK-35新設（WARN、再構成使用・取得不可を検知）、
  `operating_income_source` provenance追加（`NAMING_CONVENTIONS.md`
  規則4準拠）
- fixed_registry.json 23エントリのsnapshot_hashを機械的に全数照合
  （259フィールド、不一致0件）の上で更新

**本線2: `[[MOAT-SCORE-PARTIAL-NULL-1]]`**（実装
`16da15c99`/`e077b99f0`/`f6f3c4f0f`、provenance追加
`7ab8672db`/`5b115fd3e`/`787c9adbf`）:
- `calculate_moat_score()`の`(値 or 0.0)`パターンを解消。roicが
  Noneの原因別に扱いを変える（真の赤字`reported_negative_oi`は
  `roic_norm=0.0`で算入、`roic_diverged_over10`は`roic_norm=1.0`で
  算入〈該当0件・未検証〉、それ以外の測定不能は除外・重み再正規化）
- **最低2指標ルール**: 有効指標2未満なら`moat_score=0.5`
  （中立フォールバック）。「薄い根拠から確信ありげな出力を出さない」を
  高低スコア双方に対称適用
- `moat_score_source`（`measured`/`neutral_fallback`）・`n_present`
  provenance追加、CHECK-36新設（WARN）。BKNG・CPRTが中立フォールバック
  対象（BACKLOG_DONE.md・SYSTEM_MAP.mdに恒久注意事項として記録）

**IV変化（実測、いずれもポートフォリオ非保有銘柄）**: KLAC +21.4%・
XOM -18.7%・LLY -10.9%（本線1由来）、BKNG +32.4%・CPRT +22.6%・
V +16.2%（本線2由来）。TANUKI SCORE分類が変わったのはBKNG
（WATCH→BUY）のみ——ただしこれは測定されたモート強度ではなく中立
フォールバック（プレースホルダ0.5）に基づく判定である点に注意
（moat_scoreは人為的に調整していない）。

**当初想定になかった波及経路（教訓）**: 本線1では`g_fundamental`→
`recommended_g`経路がIV変化の主因だった（当初の消費者分析は
`moat_score`経由のみを想定していた）。本線2ではStep1の消費者確認で
`index.html`の`#avg-moat`（全銘柄平均表示）を新規発見。以後、消費者
確認では**直接の引数だけでなく`None`ガードの有無まで確認する**必要が
あるという教訓を`SYSTEM_MAP.md`に記録済み。

**コミットメッセージの引用符事故（2回発生）**: シングルクォート化後も
メッセージ中の`''`（連続シングルクォート）がbashのクォート解釈で
脱落する事故が2026-08-16に2回発生（本線1・本線2の実装コミットで
各1回）。いずれも`git log -1 --format=%B`確認でpush前に発見し
`--amend`で修正。CHAT_RULES.mdに追記済み。

**次の本線は未定**（`CHAT_RULES.md`「本線の定義」参照）。**次セッションの
着手候補**:
1. `[[FALSY-ZERO-PATTERN-SWEEP-1]]`（優先度中、横断調査。falsy-zero
   パターンが本セッションだけで5例確認されており、個別対応では
   追いつかない規模になっている）
2. `[[CONFIG-LOAD-SILENT-FALLBACK-1]]`残り3件（優先度低、着手条件なし。
   `maturity_config.json`/`segment_config.json`はWACC/DCF計算コアに
   直結し全銘柄再生成の検証コストが高いため後回しでよい）
3. `[[MACRO-STYLE-FCF-ZERO-TRUTHY-EXCLUDE-1]]`（優先度低、現状実害
   ゼロの潜伏バグ、着手条件なし）
4. `[[MACRO-TRUTHY-ZERO-BUG-1]]`・`[[RECESSION-SCORE-TRIPLE-CALC-1]]`・
   `[[HOLLOW-RALLY-DEAD-1]]`（案2 Step Cで優先度維持と判定済みの既存
   課題群、着手条件なし。`[[FCF-CAGR-YEARS-MISMATCH-1]]`は2026-08-30
   実装完了・BACKLOG_DONE.md移設済みのためリストから削除）
5. **グループB（エピック3件、`[[QUALITY-GATES-EPIC-1]]`・
   `[[TRUST-SUMMARY-EPIC-1]]`・`[[BACKTEST-SCORE-1]]`）は「記述の誤り」
   ではなく「未着手の大型設計項目」であり、次の着手先として選ぶ前に
   まず「着手要否の判断」自体を行う必要がある**（詳細調査ではなく、
   着手する価値があるかどうかの意思決定が先）

最終更新: 2026-08-16（**セッション終了時ブラッシュアップ・本日
セッションサマリー**。フェーズ3完了後の残作業として`[[FCFCONFIG-
MISSING-DETECTION-WEAK-1]]`・`[[EPSANALYZER-ADMIN-ORPHAN-PAGE-1]]`・
`[[CONFIG-LOAD-SILENT-FALLBACK-1]]`（着手条件なしの残2件）を実施した
（`79d623ceb`以降の作業。フェーズ3完了記録自体は`79d623ceb`で完了
済み、本エントリでは重複記録しない）。

**実装完了（コミットハッシュ付き）**:
- `[[FCFCONFIG-MISSING-DETECTION-WEAK-1]]`（CHECK-33新設）
  `027c6868c`/`73bff7fbc`（後にCHECK-34へ統合、下記参照）
- `[[EPSANALYZER-ADMIN-ORPHAN-PAGE-1]]`（死蔵ページ削除）
  `9e2e0fd9d`/`806502eb6`
- `[[CONFIG-LOAD-SILENT-FALLBACK-1]]`登録`bf080bb35`、部分実装
  （CHECK-34、7件中4件）`3241a360e`/`dea525ea3`

**CHAT_RULES.mdへの運用ルール追加2件**:
- git add・破壊的git操作の事故防止 `12e3c9679`
- ローカル環境固有の検証ノイズ `f8272834c`

**確立した設計原則**: サイレント破損の検知は「チェッカー独自の代理
判定ではなく、本番コードが実際に使う解決ロジックそのものを呼び出す」
（CHECK-32〜34共通、`SYSTEM_MAP.md`に記録済み）。横展開はレジストリ
テーブル方式を採用し、個別チェック関数を対象数分作らず、
`resolve_*_path()`の切り出し＋`_CONFIG_LOADER_REGISTRY`への1エントリ
追記で済む構造にした。

**スコープが登録前確認で変わった事例**: `rpo_config.json`単体の課題
として登録する予定だったが、登録前の横断確認で同型パターンが7ファイル
に及ぶと判明し、`[[CONFIG-LOAD-SILENT-FALLBACK-1]]`として横断項目に
変更した（個別登録していれば残り6ファイルは追跡されないまま残って
いた）。

**環境要因による検証ノイズ2件**: `core.autocrlf`によるCRLF誤検知、
`ALPHA_VANTAGE_API_KEY`未設定による`EPS_DISCREPANCY`欠落。いずれも
原因特定の上で`CHAT_RULES.md`に記録済み（EPS Analyzer側の生成データは
環境要因による差分を含むためコミット対象から除外し、コード変更のみ
コミットした）。

**次セッションの着手候補**: `[[CONFIG-LOAD-SILENT-FALLBACK-1]]`
残り3件（`prompts.yaml`・`maturity_config.json`・`segment_config.json`/
`growth_options_config.json`、着手条件なし）。ただし`maturity_
config.json`/`segment_config.json`はWACC/DCF計算コアに直結し全銘柄
再生成の検証コストが高いため、優先度は低いまま。それ以外は
`[[RISK-FREE-RATE-HARDCODE-1]]`等、既存の本線外・優先度高課題群も
候補（詳細はBACKLOG.md参照）。

詳細はBACKLOG_DONE.md「2026-08-16（完了）」参照。

最終更新: 2026-08-15（**セッション終了時ブラッシュアップ・本日
セッションサマリー**。フェーズ3合同設計調査（Bグループ・Aグループ・
未登録11件調査）を通じて実施した以下を記録する。

**実装完了（コミットハッシュ付き）**:
- `[[TAILKPI-CONFIG-LOCATION-1]]`（`config/tail_kpi_map.json`へ移動）
  `80890c711`
- `[[FCFCONFIG-LOCATION-1]]`（`config/fcf_conversion_config.json`へ
  移動）`493e8843a`（+設定ファイル不在時のWARN追加`7e69f8025`）
- `[[PORTFOLIO-CONFIG-DUP-1]]`（`config/portfolio.json`廃止・`docs/`
  一本化）`e97741f54`/`eaf3016cb`
- `[[DISCOVER-CONFIG-DUAL-MGMT-1]]`（`Discover_Config_Sync.yml`新設・
  `report_consistency_check.py`CHECK-32新設）`20a173a76`/`80bcf5f57`
- フェーズ3完了記録`27b38e953`、CHAT_RULES.md追記`d69a8e879`

**確定した設計判断**:
- `config/`配下はGitHub Pages非公開（実測404）。過去3回同一の取り違え
  （`portfolio.json`・`discover_config.json`・`theme_config.json`）が
  独立に発生していた事実と併せてSYSTEM_MAP.mdに恒久記録
- `config/`↔`docs/`重複ファイルの解消は、Pythonバックエンドの読み手の
  有無で方向が逆になる（読み手ゼロ→`docs/`側に一本化、読み手あり→
  `config/`側を正としつつ`docs/`側を自動追従させる）。この判断基準を
  SYSTEM_MAP.mdに明文化
- `_meta`スキーマ標準（`NAMING_CONVENTIONS.md`規則8）。既存3件
  （`segment_config`/`growth_options_config`/`maturity_config`）が
  既に統一済みのスキーマを標準化、既存ファイルへの遡及適用はしない
- 新DB構築プロジェクト フェーズ1〜3完了、次の本線は未定

**実装直前に停止した前提の誤り3件（本セッションで再発防止価値が
最も高い部分）**:
- 架空の追記の指摘（`docs/quality/quality_checker.html`→
  `docs/quality-monitor/quality_checker.html`と2回にわたり存在しない
  パスが提示された。`common/sec_data/quality_checker.py`という無関係な
  既存BACKLOG項目`[[QUALITY-CHECKER-CLEANUP-1]]`との取り違えが発生源と
  推測される）
- 結論が似た別調査の同一視（`[[SCHEMA-NORMALIZED-ISSUES-1]]`6290行目の
  「調査依頼文の前提訂正」は、本セッションのフロントエンド28ファイル
  横断点検とは対象範囲が異なる別調査だったが、結論文が酷似していたため
  同一視されかけた）
- 削除不可のファイルを削除しようとした（`config/discover_config.json`は
  `src/discover/collect.py`〈Discoverパイプライン本体〉・
  `common/sec_data/registration_validator.py`の入力であり、
  `[[PORTFOLIO-CONFIG-DUP-1]]`と同型の「`docs/`側へ一本化・`config/`側
  廃止」という解決法は実装直前の`grep -rn`調査で致命的だと判明し停止）

**発見したサイレント破損経路2件**:
- `adjustments.py::estimate_fcf_from_eps()`が設定ファイル不在時に例外を
  投げずraw_fcfへフォールバックする経路（標準出力へのWARN追加で
  緩和、恒久対策は`[[FCFCONFIG-MISSING-DETECTION-WEAK-1]]`として
  記録のみ）
- `report_consistency_check.py`CHECK-32の初期実装がバイト単位比較で
  誤検知していた（Windows`core.autocrlf=true`環境での`git checkout`
  後のCRLF/LF差、gitからは「変更なし」判定される差異。JSON意味比較に
  修正）

**セッション終了時レビューで発見・修正した文書間の矛盾1件**: 本ファイル
自身に「`CHAT_RULES.md`「本線の定義」節の更新要否も、次の本線が定まった
時点で判断すること」という記述が残っていたが、同節は既に
`27b38e953`で更新済みだったため、この矛盾を訂正した。

詳細はBACKLOG_DONE.md「2026-08-15（完了）」参照）

最終更新: 2026-08-15（**新DB構築プロジェクト フェーズ3「導出データ層の
管理方法検討」完了、これによりフェーズ1〜3が全て完了**。分類C14件の
うち登録済み5件（`[[DISCOVER-CONFIG-DUAL-MGMT-1]]`〈`INPUT-C-006/007`〉・
`[[PORTFOLIO-CONFIG-DUP-1]]`〈`INPUT-C-008`〉・`[[TAILKPI-CONFIG-
LOCATION-1]]`〈`INPUT-C-009`〉・`[[FCFCONFIG-LOCATION-1]]`
〈`INPUT-C-010`〉）は実装完了、残り9件（`INPUT-C-001〜005`・
`011〜014`）は調査の上「現状維持が妥当」と判断し完了とした（判断根拠は
`INPUT_DATA_TOBE.md`分類C表の各行注記参照）。`_meta`スキーマ標準化
方針を`NAMING_CONVENTIONS.md`規則8に新規策定（既存ファイルへの遡及
適用はしない）。調査過程で発見した死蔵ページ1件を`[[EPSANALYZER-
ADMIN-ORPHAN-PAGE-1]]`として新規登録、既存`[[RPO-ADMIN-1]]`に
`_meta`欠如の追記を実施。**次の本線は未定**（本ファイル「毎回の
作業開始時に必ず実行すること」節の「次セッションの一次データ層
プロジェクト着手順序」は、フェーズ1〜3完了によりその役目を終えた。
次セッション開始時は、残存する本線外・低優先度課題群〈下記既存の
1・2・3〉への対応、または新たな本線をBACKLOG.mdの優先順位・
PROJECT_STATUS.mdを踏まえて判断することから始める。`CHAT_RULES.md`
「本線の定義」節は**既に更新済み**（旧本線＝フェーズ1を取り消し線化・
「次の本線は未定」を記録、コミット`27b38e953`）。次の本線が定まった
時点で、同節に新本線を追記すること。
詳細はPROJECT_STATUS.md冒頭・BACKLOG_DONE.md「2026-08-15（完了）」
参照。実装コード変更なし）

最終更新: 2026-08-15（セッション終了時ブラッシュアップ。2026-08-15の
セッションで完了した以下を反映：①BACKLOG項目の棚卸しクローズ・優先度
引き下げ複数件（`[[MACRODATA-AS-IS-DUPLICATION-UNDERCOUNT-1]]`クローズ・
`[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`/`[[MARKETDATA-SP500-SCRAPE-
INVALID-TICKERS-1]]`/`[[NAMING-CONVENTIONS-APPLY-1]]`着手条件更新・
`[[SCHEMA-NORMALIZED-ISSUES-1]]`①②/`[[LAYER3-RPO-CANDIDATE-ORDER-1]]`
優先度引き下げ）②新規発見2件のBACKLOG登録：`[[MACRODATA-FETCH-
FAILURE-VISIBILITY-GAP-1]]`（macro_data取得失敗の可視化設計欠如）・
`[[MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1]]`（景気サイクルフェーズ
複合スコアの鮮度注記欠如）③`[[MACRODATA-IMPORT-HISTORY-CONFIG-
DRIFT-1]]`対応完了：`05_import_history.py`を`common.macro_data.
reader`経由に作り直し（案B）。当初「設定乖離」という問題設定だったが
実装過程で「実行不能〈AttributeError〉」というより深刻な実態が判明・
復旧した④`FIELD_DEFINITIONS.md`499項目の新DB参照切替状況を集計する
投資調査を実施し、yfinance/FRED由来18項目が全件切替済みと確認（フィー
ルド単位でも本線タスクの完了を確認）。SEC EDGAR由来4件（AS-IS-129・
266・273・395）はsec_data側フェーズDでの対応状況が未検証のまま残る。
CHAT_RULES.mdへ「本番消費者リスト外の間接依存が切替に巻き込まれて
破損する」パターンを`[[MACRODATA-IMPORT-HISTORY-CONFIG-DRIFT-1]]`の
事例として追記。詳細はBACKLOG_DONE.md「2026-08-15（完了）」参照。
実装コード変更あり（`05_import_history.py`のみ、検証済み・pytest回帰
なし）)

最終更新: 2026-08-14（セッション終了時ブラッシュアップ。2026-08-13の
セッションで完了した以下を反映：①`common/market_data/`の未追跡だった
`collect_and_send.py::collect_asset_flow()`のSHV等6資産切替
（`[[MARKETDATA-COLLECT-ASSET-FLOW-UNTRACKED-1]]`）②`common/macro_data/`
の未追跡だった`backfill_tech_pulse.py`のVXNCLS切替
（`[[MACRODATA-BACKFILL-TECH-PULSE-VXNCLS-UNTRACKED-1]]`）③重複計算
パターン4件の解消：`[[NETCASH-DUAL-CALC-1]]`・`[[NETINCOME-DUAL-
PIPELINE-1]]`・`[[RULE40-DEFINITION-MISMATCH-1]]`（NAMING_CONVENTIONS.md
規則2適用、TANUKI VALUATIONのsell_funda判定波及も全数検証済み）・
`[[FRED-HYSPREAD-TRIPLE-FETCH-1]]`（未着手のまま放置されていたが実装は
既に別プロジェクトで解消済みと判明しクローズ）④`[[SP500-GSPC-MULTI-
FETCH-1]]`の優先度中→低引き下げ。これにより新DB構築プロジェクトの
sec_data/market_data/macro_data本線タスクは完了、残るのは本線外・低
優先度課題群のみとなった。CHAT_RULES.mdへ「『完了』報告済み事項の
定期再点検」ルールを新規追加。詳細はBACKLOG_DONE.md「2026-08-13
（完了）」参照。実装コード変更なし）

最終更新: 2026-08-13（「次セッションの一次データ層プロジェクト着手順序」節が
陳腐化していたのを是正。`[[MACRODATA-LAYER-CONSTRUCTION-1]]`は前回の
本ファイル更新（直後の2026-08-12エントリ「次のアクションは実装設計に
変更」）の後、同日中に実装が完了していた：`fetcher.py`/`reader.py`実装・
`.github/workflows/Macro_Data_Update.yml`新設（定期取得ワークフロー稼働
開始）・本番消費者2ファイル（`05_main.py`・`collect_and_send.py`）の
`common.macro_data.reader`経由への全面切替・重複3系列（`BAMLH0A0HYM2`・
`T10Y2Y`・`VIXCLS`）の`reader.get_latest()`一本化・`BAMLH0A0HYM2`の
例外的履歴移行まで完了し、切替前後18項目の値突合で完全一致を確認済み
（BACKLOG.md`[[MACRODATA-LAYER-CONSTRUCTION-1]]`の見出しも「完成（本番
消費者切替完了）」）。「次のアクションは実装設計」という本ファイルの
記述が実態と食い違ったまま残っていたため、下記「次セッションの一次
データ層プロジェクト着手順序」節を実態に合わせて訂正し、次のアクションを
残存する本線外の低〜中優先度課題群に更新した。訂正自体はユーザーからの
陳腐化発見依頼による。詳細はBACKLOG.md`[[MACRODATA-LAYER-
CONSTRUCTION-1]]`参照。実装コード変更なし）

最終更新: 2026-08-12（セッション終了時ブラッシュアップ。「次セッションの
一次データ層プロジェクト着手順序」節を、`common/market_data/`の残り
4ファイル完了を反映して更新：着手順序5-1`audit.py`・5-2`score_verifier.py`
（診断ツール2/2）・6-1`extract_key_facts.py`・6-2`backfill_tech_pulse.py`
（周辺ツール2/2）が全て完了し、**本番消費者8＋診断ツール2＋周辺ツール2の
全12ファイルが完了、`common/market_data/`構築プロジェクト自体が完了**。
続けて`common/macro_data/`（FRED統合層）の新設事前調査（FRED消費者
洗い出し、`MIGRATION_CHECKLIST.md`Step1相当）を実施し、ドキュメント
不備2件・サイレント欠落疑い1件・設定陳腐化1件の新規4件をBACKLOG登録
（記録のみ、実装未着手）。次のアクションは`[[MACRODATA-LAYER-
CONSTRUCTION-1]]`の実装設計（重複解消3系列＋`fetcher.py`/`reader.py`
設計）に変更。詳細はBACKLOG.md`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・
BACKLOG_DONE.md「2026-08-12（完了）」参照。実装コード変更なし）

最終更新: 2026-08-11（セッション終了時ブラッシュアップ。「次セッションの
一次データ層プロジェクト着手順序」節を、本番消費者8ファイル**8/8切替
完了**（`pipeline.py`・`collect.py`・`collect_and_send.py`・
`breadth_calculator.py`・`hypecore.py`〈前提作業3件込み〉が今回完了）を
反映して更新。次のアクションは着手順序5（診断ツール2ファイル
`score_verifier.py`・`audit.py`）に変更。切替過程で発見した
`auto_adjust`差分は「バグ」として誤登録した後、事実確認調査で
「旧実装の調整済み終値使用の方が技術指標としては不適切だった」と判明し
訂正・クローズ済み（教訓を`CHAT_RULES.md`へ新規ルール化）。詳細は
BACKLOG.md`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・BACKLOG_DONE.md
「2026-08-11（完了）」参照。実装コード変更なし）

最終更新: 2026-08-11（「次セッションの一次データ層プロジェクト着手順序」節の
`hypecore.py`切替順序を訂正。読み取り専用事前調査（チャット記録、
2026-08-11）でdaily/バックフィル期間拡張・attributes/未収録7フィールド・
analyst_history/未収録2系統という前提作業が判明し、BACKLOG.md
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`側で着手順序を4-4→4-8〈残り消費者の
最後〉へ変更済みだったが、本ファイルへの反映が漏れていたため今回訂正。
次のアクションは着手順序4-4〜4-7〈`pipeline.py`/`collect.py`/
`collect_and_send.py`/`breadth_calculator.py`の軽量消費者〉に更新。
実装コード変更なし）

最終更新: 2026-08-11（「次セッションの一次データ層プロジェクト着手順序」節が
2026-08-07時点の記述のまま陳腐化していたのを是正。PROJECT_STATUS.md・
SYSTEM_MAP.md・BACKLOG.mdは2026-08-11時点の状態〈`common/market_data/`
未決定事項9件の最終設計判断・`fetcher.py`/`reader.py`新設・Daily/Weekly
Update workflows実装・本番消費者8ファイル中3/8切替完了〉へ既に更新済み
だったが、本ファイルへの反映が漏れていたため今回訂正。訂正自体は
ユーザーからの陳腐化発見依頼による。詳細はBACKLOG.md
`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・BACKLOG_DONE.md「2026-08-11
（完了）」参照。実装コード変更なし）

## 毎回の作業開始時に必ず実行すること

> ⚠️ **一時的な運用注意（2026-08-16追記、Koichiさんが実施確認後に本ブロックごと削除してよい）**
> `docs/value-monitor/admin.html`（`[[PORTFOLIO-CONFIG-DUP-1]]`、コミット
> `e97741f54`/`eaf3016cb`）の保存先が`config/portfolio.json`→
> `docs/portfolio/data/portfolio.json`（v2）へ変更された。同様に
> `docs/discover/admin.html`（`[[DISCOVER-CONFIG-DUAL-MGMT-1]]`、コミット
> `20a173a76`/`80bcf5f57`）も保存フロー（同期ワークフロー・検証ロジック）が
> 変更されている。**ブラウザに旧版admin.htmlがキャッシュされたまま保存操作を
> 行うと、value-monitor/admin.htmlは旧パス（`config/portfolio.json`）に
> ファイルが再作成され、表示側（`docs/portfolio/`）に反映されない。**
> admin.html自体は「保存成功」と表示するため、この不一致には気づけない。
> **次回admin.html使用前にハードリロード（Ctrl+Shift+R）を必須で実施する
> こと。** value-monitor/admin.htmlは画面上部に保存先バージョン表記
> （「保存先: docs/portfolio/data/portfolio.json（v2、2026-08-15〜。
> config/への保存は廃止）」）があり、新版が読み込まれているかここで確認
> できる。

### Step 0: ローカルリポジトリの最新化（最優先）
GitHub Actions が前回セッション後にデータを自動更新している可能性があるため、
作業開始前に必ずローカルを最新化する。

```bash
cd C:\Users\shigi\Documents\On-a-journey-git
git pull --rebase origin kaihatsu
```

コンフリクトが発生した場合：
- 自動生成データファイル（.gitattributes の merge=ours 対象）→ ローカル版が自動採用される
- 手書きファイル（.py / .md / config/*.json 等）→ 内容を確認してから解決する

### Step 1: 現状確認
以下のファイルを読んでください：
- SYSTEM_MAP.md（システム間の依存関係・変更影響範囲を把握）
- BACKLOG.md
- PROJECT_STATUS.md（新一次データベース構築プロジェクトの進捗確認。
  フェーズ1〜3のいずれかが「構築中」以上になっている場合、そのプロジェクトが
  進行中であることを認識した上で作業する。全て「未着手」の場合は
  プロジェクト自体は設計完了・実装未着手の段階であるため、通常の
  BACKLOG.md起点の作業を優先してよい。2026-08-07時点で`common/sec_data`
  統合のフェーズD〈本体consumer切替、対象優先順位: ①TANUKI VALUATION
  本体②STONKS SILO③TANUKI TAIL④HypeCore⑤stock.html〉は**実質完了**。
  Step2-1〜2-4（①〜④の主要4消費者パイプライン）は完了、Step2-5
  （⑤stock.html＋診断・補助スクリプト7件）は2026-08-07投資調査の結果
  「9系統中Layer3切替の実質対象は1系統のみ、他8系統は死蔵コード・
  書き込み専用・既にLayer3経由・アーキテクチャ上切替不可のいずれか」
  と判明し実装不要（実質完了扱い）。保留中だった2判断
  （`[[LAYER3-FETCHER-SELECTION-PHILOSOPHY-MISMATCH-1]]`のSTONKS
  SILO`fetcher.py`選択思想・`[[STOCKHTML-LAYER3-PUBLISH-PIPELINE-
  MISSING-1]]`のstock.html公開パイプライン）はいずれも**現状維持
  （着手見送り）で確定**。

  **フェーズE（`normalized/`廃止）は着手不可**：`fetcher.py`・
  `dcf_validity_checker.py`（`data/annual_*.json`依存継続）・
  stock.html（`normalized/`直接依存継続）が意図的な恒久的例外として
  残るため、`normalized/`は完全廃止できず、この3系統向けに存続する
  設計とする（詳細は`[[SECDATA-STORAGE-FRAGMENTATION-1]]`参照）。

  **次セッションの一次データ層プロジェクト着手順序（2026-08-15訂正）**：
  `common/sec_data`統合（フェーズD）・`common/market_data/`（yfinance
  統合層）・`common/macro_data/`（FRED統合層）とも**主要切替は完了**。
  2026-08-15、`FIELD_DEFINITIONS.md`499項目単位での新DB参照切替状況を
  集計する投資調査を実施し、yfinance/FRED由来18項目が全件切替済みと
  実コードで確認した（詳細はBACKLOG.md`[[MARKETDATA-LAYER-
  CONSTRUCTION-1]]`・`[[MACRODATA-LAYER-CONSTRUCTION-1]]`の
  2026-08-15付注記参照）。**新DB構築プロジェクトは消費者ファイル単位・
  重複計算パターン単位・フィールド単位の3つの粒度全てで本線タスクの
  完了を確認できた**。`[[MACRODATA-IMPORT-HISTORY-CONFIG-DRIFT-1]]`
  対応（`05_import_history.py`を`common.macro_data.reader`経由に
  作り直し）も完了し、当初「設定乖離」という軽微な問題設定だったが
  投資調査で「実行不能」というより深刻な実態が判明・復旧した。
  次のアクションは以下の本線外・低優先度課題群のみ:
  1. （本線外・低優先度）これまでの本線外課題群: `[[ERP-DUAL-CALC-1]]`・
     `[[Q4-IMPLIED-CALC-TRIPLICATION-1]]`・`[[MOAT-CATALOG-DUP-1]]`・
     `[[SEC-SUBMISSIONS-DUAL-FETCH-1]]`・`[[SP500-GSPC-MULTI-
     FETCH-1]]`（重複計算パターン棚卸しで残った5件、外部APIコストの
     実害解消を受け優先度中→低に引き下げ済み）・`[[MARKETDATA-CWAN-
     FROZEN-DATA-SUSPECT-1]]`・`[[MARKETDATA-SP500-SCRAPE-INVALID-
     TICKERS-1]]`・`[[MARKETDATA-VIX9D-DATA-GAP-1]]`・`[[STONKS-SILO-
     CLI-TICKERS-SHADOW-1]]`・`[[MACRODATA-SCHEDULED-SILENT-GAP-
     CSCICP-USALOL-1]]`・`[[MACRODATA-FULL-HISTORY-DAILY-REFETCH-1]]`・
     `[[MACRODATA-FTSD-SERIES-ID-INVALID-1]]`・`[[LAYER3-RPO-
     CANDIDATE-ORDER-1]]`・`[[SCHEMA-NORMALIZED-ISSUES-1]]`①②
     （判断保留中の既存課題群、いずれも実害調査済みで優先度低のまま
     対応方針未確定。**2026-08-26追記**: このうち`[[Q4-IMPLIED-CALC-
     TRIPLICATION-1]]`は解消済み（実装は2026-07-24完了、記録の
     クローズ処理漏れを2026-08-26に是正）と判明したためBACKLOG_DONE.md
     へ移設済み。残りの項目は本リストの記載通り未対応のまま）
  2. （本線外・新規）2026-08-15セッションで新規発見した2件、対応方針
     未定: `[[MACRODATA-FETCH-FAILURE-VISIBILITY-GAP-1]]`（macro_data
     系列単位の取得失敗がviolations_log.jsonで「正常」と区別できない
     設計上のギャップ）・`[[MACRO-PULSE-STALENESS-DISCLOSURE-GAP-1]]`
     （景気サイクルフェーズ複合スコアで、CFNAI・Building Permitsに
     鮮度注記が欠けている）
  3. sec_data側フェーズDでのSEC EDGAR由来4件（AS-IS-129・266・273・
     395）の対応状況確認: `FIELD_DEFINITIONS.md`499項目調査
     （2026-08-15）ではyfinance/FRED由来18件のみを検証対象とし、
     SEC EDGAR由来4件はsec_data側フェーズDでの新DB参照切替状況を
     未検証のまま残した。次回、`common/sec_data/reader`経由への
     切替状況を個別に確認する調査から着手する
  4. 新DB構築プロジェクトの完全な区切り: sec_data/market_data/
     macro_dataの新設・切替という**本線タスクは完了**。以降は本線外・
     低優先度課題（上記1・2）とsec_data側の残検証（上記3）の順次対応
     のみで、新規の大規模構築フェーズは予定されていない

  切替過程で発見した`daily/`層の`auto_adjust=False`（未調整終値）と
  旧実装`auto_adjust=True`（調整済み終値）の乖離は、当初「バグ」として
  登録したが、事実確認調査の結果「旧実装の調整済み終値使用の方が
  テクニカル指標としては元々不適切だった」と判明し訂正・クローズ済み
  （`[[MARKETDATA-DAILY-UNADJUSTED-PRICE-DIVIDEND-DRIFT-1]]`、対応不要
  で確定）。この教訓は`CHAT_RULES.md`「新旧の値が食い違う場合、新側を
  疑う前に『どちらが目的に対して正しいか』を確認する」として新規
  ルール化済み。2026-08-13セッションではこれに続く教訓として「『完了』
  報告済み事項の定期再点検」（`[[FRED-HYSPREAD-TRIPLE-FETCH-1]]`が
  実は既に解消済みなのに未クローズのまま残存していた等3パターン）も
  `CHAT_RULES.md`へ新規ルール化した。2026-08-15セッションでは、
  「本番消費者リスト外の間接依存（動的import経由）が切替に巻き込まれて
  破損する」パターン（`05_import_history.py`が`05_main.py`の`get_fred`
  削除に巻き込まれ3日間気づかれず実行不能だった事例）を4件目の実例
  として同ルールへ追記した。

  `common/sec_data`統合の詳細はBACKLOG.md`[[SECDATA-STORAGE-
  FRAGMENTATION-1]]`（マスター追跡エントリ、最終状況を記載）・
  BACKLOG_DONE.md`[[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP2-4]]`（④）・
  `[[SEC-EDGAR-LAYER-DESIGN-PHASE-D-STEP2-3]]`（③）・`[[SEC-EDGAR-
  LAYER-DESIGN-PHASE-D-STEP2-2]]`（②）・`[[SEC-EDGAR-LAYER-DESIGN-
  PHASE-D-STEP2-1]]`（①TANUKI VALUATION本体）参照。`common/market_data/`
  の詳細はBACKLOG.md`[[MARKETDATA-LAYER-CONSTRUCTION-1]]`・
  `common/macro_data/`の詳細はBACKLOG.md`[[MACRODATA-LAYER-
  CONSTRUCTION-1]]`・重複計算パターン4件の詳細はBACKLOG_DONE.md
  「2026-08-13（完了）」参照
- src/value/tanuki_valuation/pipeline.py（直近の変更を把握）

### Step 2: テスト実行
cd C:\Users\shigi\Documents\On-a-journey-git
python -m pytest tests/ -v
全件パスを確認してから作業を開始する。
失敗があれば先に修正する。
既知の例外: tests/test_iv_formula.py の MSFT/NVDA 2件は既存バグ
（[[TEST-STALE-IV-1]]、ALPHA-REDESIGN-1後にテスト式が未更新）として
認識済み。この2件以外の失敗が出た場合のみ先に修正する
（QUALITY-GATES-EPIC-1 Phase 1で2026-07-12にtest_pipeline_logic.py単体実行
からtests/全体実行へ変更。ALPHA-REDESIGN-1後の回帰がtest_pipeline_logic.py
実行のみでは2週間見逃されていた教訓による）。

### Step 3: 作業内容の確認
BACKLOGから以下の優先順位で作業項目を選定：
1. 優先度：高 かつ 着手条件が満たされているもの
2. 難易度が低いものを優先
3. 着手条件が未達のものはスキップ

### Step 3.5: 既存実装の確認と設置場所の妥当性検証
実装前に必ず以下を確認する：

**① 既存の類似機能を検索**
```bash
grep -rn "[機能キーワード]" docs/ src/ --include="*.html" --include="*.py"
```
既存実装がある場合は新規実装ではなく改善・移動を検討する。

**② 設置先ファイルの利用目的との整合性を確認**
実装しようとしている機能が、設置先ファイルの本来の目的と一致しているか確認する。
- そのファイルは何のためのファイルか？
- 追加しようとしている機能はその目的の範囲内か？
- 目的が異なる場合は正しいファイルを探すか、新規ファイルを作成する

例：ポートフォリオ管理機能 → TANUKI VALUATION画面ではなくPORTFOLIO画面へ

誤配置の実例（2026-06-17修正済み）:
  誤: TANUKI SCOREの売買判定履歴 → stock.html（TANUKI VALUATION）に実装
  正: 売買判定 → TANUKI SCOREの責任範囲
     HYPECOREフェーズ履歴 → TANUKI VALUATIONの文脈に合致

### Step 4: 作業前の宣言
「〇〇（BACKLOG項目名）を実装します。
 変更するファイルは△△のみです。」
と宣言してから作業を開始する。

---

## 作業ルール

### ファイル変更の原則
- 指示されたファイルのみを変更する
- 変更範囲を事前に明示する
- 既存の動作を壊さない

### 調査・診断タスクでの書き込み系コマンド実行の注意（2026-07-11追加）
調査・診断のみが目的のタスク（登録プロセス診断・データ形式確認等）で、
挙動確認のために`update.py`等の書き込み系・データ生成系コマンドを
実行する必要がある場合：
- 対象ティッカーを最小限に限定する（全銘柄実行しない）
- 実行前に`git status`で作業ツリーがクリーンであることを確認する
- 実行後は必ず`git diff`で意図しない本番データの書き換えが
  発生していないか確認し、調査目的以外の変更は復元してから
  コミットする（2026-07-10の教訓: `update.py ENB`実行で
  SECデータ3ファイルが意図せず書き換わり、コミット前に復元した事例）

#### 調査中に発見した別バグの実装は別途依頼を待つ（2026-07-11追加）
調査・診断タスクの実行中に、依頼スコープ外の別バグを発見した場合、
その修正実装・pipeline.py等の再実行・latest.json等の本番データ再生成は
行わない。発見内容を報告に含めるにとどめる。

「バグを見つけたのでついでに直した」は、たとえ修正内容が正しくても、
影響範囲（分類変更銘柄数等）の事前確認・承認プロセスを飛ばすことになり
禁止とする。修正は完了報告後、チャット側Claudeが作成する別依頼文で
改めて着手する（2026-07-11実例: DCF-REL-SYNC-1の実装前調査中にPolicy Bの
別バグを発見しその場で実装・30銘柄再生成まで行ってしまい、後日
TANUKI-POLICYB-FIX-1として正式分離・再コミットした事例）。

### 履歴JSONへの記録ルール
- 記録キーは必ず日付ベース（YYYY-MM-DD）で重複排除する
  （タイムスタンプ全体を記録する場合でも重複排除キーはdate[:10]を使う）
- 実装参照: score_history.json（pipeline.py:566）・hypecore_history/（同パターン）

### 新規銘柄属性を追加した場合の必須対応

バックエンド（pipeline.py・data_fetcher.py等）に新しい銘柄属性・設定項目を追加した場合、
以下を必ずセットで実施する：

**① フロントエンドへの登録機能追加**
- admin.html（または該当する管理画面）に
  新属性の入力・編集UIを追加する
- 既存銘柄への一括適用手段も合わせて用意する

**② 銘柄登録手順への追記**
- CLAUDE_CODE_START.md の「新規銘柄登録時の必須手順」に
  新属性の設定ステップを追加する
- 設定漏れ時の影響（フォールバック値・デフォルト動作）も明記する

**③ ユーザー向け数値・バッジを追加した場合は glossary.json に説明を追加する**（EPIC-LEGEND-1）
- 新しいスコア・バッジ・色分け・「–」表示等、ユーザーが意味を読み取れない可能性のある
  表示を追加した場合は `docs/common/glossary.json` に用語キー→説明文のエントリを追加し、
  該当箇所のHTMLに `<span data-info="key">` を付与する（`docs/common/info-tooltip.js` が
  自動でホバー/タップ可能なツールチップに変換する）
- 銘柄ごとに異なる動的な説明文（理由付きの警告等）には `data-info-text="説明文"` を使う
  （glossary.jsonの静的辞書を経由しない。既存要素への後付け属性設定もJSから可能）
- 既存の用語と意味が同じ場合は新規エントリを作らず既存キーを再利用する
  （例: 「DCF_Reliability=LOW」の説明はTANUKI VALUATION/TANUKI SCORE両方で共通利用）

例：
  discover_config.json への登録 → Step 6 として追加（2026-06-03）
  HypeCore 実行 → Step 5 として追加（2026-06-03）

### 銘柄フラグ（tanuki/stonks_silo/eps/hypecore）を参照するスクリプトの必須パターン（FLAG-CONSUMER-AUDIT-2/3の教訓）
cik_lookup.csvの4フラグのいずれかで対象銘柄を絞り込むスクリプトを新規作成・変更する場合：
- 全銘柄バッチ処理は必ず `common/sec_data/tickers.py` の `get_active_tickers(flag)` 系関数
  （`get_tanuki_tickers()` / `get_stonks_silo_tickers()` / `get_eps_tickers()` /
  `get_hypecore_tickers()`）経由で取得する。cik_lookup.csvを独自に読み込む・
  `os.listdir()` でディレクトリを直接スキャンする等の独立した経路を新たに作らない
- **CLI引数でticker明示指定を受け付ける場合、その引数も同じフラグで検証し、
  範囲外のtickerは警告した上で除外する（無条件実行しない）**: 「全銘柄バッチ
  実行パスだけ正しくフィルタされ、CLI引数で個別ticker指定した場合はフラグ検証
  が一切ない」という構造的ギャップが、`tanuki_valuation/pipeline.py`・
  `stonks-silo/pipeline.py`・`report_consistency_check.py`・`score_verifier.py`・
  `hypecore.py`・`catalyst.py`・`adjusted_eps_analyzer/pipeline.py`の計7箇所で
  独立に発生していた（FLAG-CONSUMER-AUDIT-2/3、2026-07-12）。新規スクリプトは
  最初から`_filter_<flag>_tickers(target, allowed_tickers)`パターン
  （範囲外を警告・除外して返す）を実装すること。

**CIによる機械的検知（TICKER-DIRECT-ACCESS-GUARD-1、2026-07-13新設）:**
上記の「cik_lookup.csvを独自に読み込む・`os.listdir()`でディレクトリを
直接スキャンする」の禁止事項は、`tests/test_no_direct_ticker_access.py`が
AST解析で機械的に検知する（pytest実行時に自動でチェックされる）。
新規スクリプトがcik_lookup.csvを`csv.DictReader`で直接パースする、または
SEC/TANUKI VALUATION/HypeCore/EPS ANALYZERのルートデータディレクトリを
`os.listdir()`で直接スキャンすると、このテストが失敗する。
- 銘柄フラグに基づくバッチ処理対象リストの構築が目的ならテスト失敗を
  「直せ」のサインとして受け取り、`tickers.py`経由に修正する
- 単一ティッカーのCIK参照等、正当な理由がある場合のみ、同ファイル内の
  許可リスト（`_CIK_LOOKUP_DIRECT_PARSE_ALLOWED` /
  `_ROOT_DIR_LISTDIR_ALLOWED`）に追加する（機械的検知を逃れる目的での
  安易な追加は禁止。追加時は必ずコメントで用途を明記する）
- 本ガードは「CLI引数のフラグ検証漏れ」（上記`_filter_<flag>_tickers`
  パターンの欠如）そのものは検知できない（あくまでアクセス経路の独立実装を
  検知するもの）。CLI引数フラグ検証の実装は引き続き上記の手動チェックリストに
  従うこと

### コミットルール
git add [変更ファイル]
git commit -m "feat/fix/docs: 変更内容の説明"
git pull --rebase origin kaihatsu
git push origin kaihatsu

# ※ パイプラインコード変更後の再生成コミット時は、pushの前に手動でも確認可能：
# python common/sec_data/report_consistency_check.py --fail-on-ng
# （GitHub Actionsの自動ゲートで止まるが、ローカルで先に確認したい場合）
- `git push --force` は絶対に使わない
- results.json を含むコミットは必ず rebase してから push
- **バグ修正時の再生成順序を守る（巻き戻り防止）**:
  「コード修正 → コミット＆push → 全銘柄再生成 → 再生成結果をコミット＆push」の順で行う。
  「再生成が先・コード修正pushが後」にすると、Actions自動再生成が修正前コードで走り
  修正前レポートが本番に残存する（2026-06-11事例: 再生成02:32 → 修正push04:49）。

### テストルール
- 実装後に必ず pytest を実行する
- 新機能には必ずテストを追加する
- テスト失敗のままコミットしない
- **`if __name__ == "__main__":` ブロックを持つスクリプトを変更した場合は
  pytestだけでなく実機で直接実行して確認する（HYPECORE-SAVE-INDEX-NAMEERROR-1
  の教訓）**: pytestはモジュールをimportするだけで`__main__`ブロック自体は
  実行しないため、このブロック内の関数呼び出し順・NameError等はpytestでは
  検出できない。実例: `_save_tickers_index()`の呼び出しが関数定義より前の
  行にあるバグが、pytest 124件パス・report_consistency_check NG=0の確認を
  経てコミットされたが、実際に`python hypecore.py`を実行すると毎回NameError
  でクラッシュしており、GitHub Actionsの自動実行が3日間沈黙的に失敗し続けた
  （後続の commit/push ステップが `continue-on-error` なしでスキップされる
  ため気づかれなかった）。

### フロントエンドのエラー耐性ルール（MP-RENDERALL-CRASH-1の教訓・必須）

画面内の複数セクションを描画する一括処理（renderAll()的な関数）を
実装・変更する際は、1つのセクションの描画失敗が他のセクションの描画を
道連れにしない構造にする：

- 個々の描画関数（renderXxx()）呼び出しはtry-catchで包むか、
  config配列化してforEach+try-catchでループ処理する
- 例外発生時はconsole.errorで関数名付きログを出力する（無言で
  握りつぶさない）
- オブジェクトの外側（`if(!obj)`等）をnullガードしても、内部の
  個々のフィールド（`obj.value`等）が独立してnullになり得る場合は
  別途ガードが必要。「外側を確認した＝中身も安全」と類推しない。
  フィールドアクセスごとに`!=null`/`?.`の有無を確認する
- 新しいnull/undefined値を生む可能性のある変更（データクレンジング・
  フォールバック処理の追加等）を行った場合、その値を消費する側の
  全箇所（renderXxx関数群）にガード漏れがないか横展開で確認する

### フロントエンドのデータ表示不具合の調査順序

データがN/A・空白・読込中のまま表示される場合、以下の順序で調査する：

**① まずfetchパスを確認（最優先）**
データが存在するのに表示されない場合は、
fetchパスが正しいかを最初に確認する。

```bash
# detail.htmlからの相対パスを計算
python3 -c "
import os
base = 'docs/[HTMLファイルのディレクトリ]'
target = 'docs/[JSONファイルのパス]'
print('正しい相対パス:', os.path.relpath(target, base))
"
# HTMLのfetchパスと一致しているか確認
grep -n "fetch" docs/[対象HTMLファイル]
```

**② 次にデータの存在確認**
fetchパスが正しい場合に限り、JSONファイルの
フィールド名・値を確認する。

**③ 最後にロジック確認**
データが存在してパスも正しい場合に
計算ロジック・フィルタ条件を確認する。

※ データ側の調査を先にするとfetchパスの問題を見落とす。

**④ 「データソース側の問題（外部要因）」と結論づける前の確認
（MP-IRX-FRED-1の教訓・必須）**

特定のティッカー・指標だけ繰り返し取得失敗する場合、安易に
「Yahoo Finance側にデータがない」等の外部要因と結論づけない。
取得コード（yfinance等）が失敗していることと、データソース自体に
データが存在しないことは別問題であり、混同すると誤った対応（様子見・
放置）につながる。

判断前に以下を確認する：
1. データソースの公式サイト・一次情報で、実際にそのデータが存在するか
   直接確認する（例: Yahoo Financeの該当銘柄ページを直接見る）
2. 同じ取得ループ内の他の銘柄・指標と比較し、処理の違い（個別分岐の
   有無）を確認する
3. GitHub Actions実行ログ（取得できる範囲で）を確認し、実際に
   何が返ってきているか（空応答・エラー・タイムアウト等）を確認する
4. レート制限・IPブロック等、実行環境（GitHub Actions等）特有の問題の
   可能性を検討する（同一コード・同一ティッカーでもクラウド環境からは
   失敗し、別環境からは成功することがある）

データソース自体に確かにデータがあるにも関わらず取得が失敗する場合は、
「外部要因だから仕方ない」ではなく、取得経路の変更（代替API・リトライ・
レート制限対策等）を検討する。

**⑤ 「値が変化しない」症状はfetch失敗だけが原因とは限らない（MACRO-NFP-1の教訓）**

同一値が連続する場合、fetch失敗・staleキャッシュだけでなく、格納している値の
意味論自体が想定と異なっている（水準 vs 差分、絶対値 vs 増減率等）ケースも疑う。
表示側の丸め処理（例：`/1000`表示）が微小な変動を吸収して「同一値」に見せている
可能性も確認する。

（2026-07-07事例: MACRO PULSEのNFPがFRED PAYEMSの雇用者数**水準**をそのまま
格納しており、本来の「前月比新規雇用者数」になっていなかった。fetchは正常に
成功していたため、①〜④の調査だけでは「正常」に見えてしまうパターンだった）

### パイプラインコード変更時の追加手順

以下のファイルを変更した場合は、コミット前に影響銘柄を特定して再生成する。

**対象ファイル（変更したら必ず監査を実行）：**
- `common/sec_data/quarterly.py`
- `common/sec_data/normalizer.py`
- `common/sec_data/ttm_calculator.py`
- `common/sec_data/parser.py`（[[SEC-DATA-REDESIGN-OPERATIONAL-POLICY-1]]
  Stage1で発見: `fixed_registry.json`に登録済みの銘柄×年度
  （2026-08-05時点Stage1〜3b累計44銘柄・432エントリ）は
  `_apply_fixed_registry_freeze()`により抽出ロジック変更の影響を受けない
  よう意図的に凍結されている。`parser.py`のロジックを変更しても対象
  銘柄×年度の`annual_{year}.json`が変化しないのは想定通りの挙動であり
  バグではない。フィックス済みデータの再検証・解除が必要な場合は
  BACKLOG.md該当項目を参照し個別に判断すること。
  **fixed_registry.jsonへ新規登録する際の必須手順（Stage 2/3の教訓、
  2026-08-05追加）**: 登録候補の根拠がBACKLOG_DONE.mdの過去の完了記録
  である場合、その記述を鵜呑みにせず、登録直前に必ず対象`annual_
  {year}.json`を実際に読んでfields_snapshot対象フィールドが現存するか
  確認すること。BACKLOG_DONE.mdは完了時点のスナップショットであり、
  同一領域で後続の別タスクが実行されると記述と実データが乖離しうる
  （実例: VRT(2016)・SPIR(2020)は後続タスクでフィールドがNone化されて
  いたにも関わらず記述は旧値のまま、MRVL(2019)は逆に「取得不能」と
  クローズされていたが後続タスクが意図せず解消していた）。未確認のまま
  登録すると、`_apply_fixed_registry_freeze()`が対象フィールドを
  見つけられずRuntimeErrorで全銘柄再パースを止める）
- `common/sec_data/tag_definitions.py`（[[JNJ-RD-TAG-PRIORITY-1]]で発見:
  `TAG_CANDIDATES`は`parser.py`〈merge型候補選択〉・`quarterly.py`〈primary
  +fallback-if-empty型候補選択〉の2つの異なる消費ロジックから参照される
  ため、候補タグの優先順位を変更する際は両消費者への影響を個別に確認する。
  また`config/sec_concept_definitions.json`〈Layer3、`layer3_builder.py`が
  参照〉に同名フィールドの独立した候補タグリストが存在する場合、
  こちらも同時に見直すべきか確認する）
- `config/sec_concept_definitions.json`（Layer3の候補タグ・フィールド定義。
  `common/sec_data/tag_definitions.py`と重複する候補タグリストを独立して
  保持しているため、片方のみ修正すると3スキーマ間でタグ優先順位が
  乖離する。詳細は`docs/architecture/new_data_platform/
  SEC_EDGAR_LAYER_DESIGN.md`「3スキーマ併存の実態」参照）
- `src/value/tanuki_valuation/calculator/rice.py`
- `src/value/tanuki_valuation/core_calculator.py`
- `src/value/tanuki_valuation/calculator/growth.py`（GROWTH-CAGR-SIGN-1で発見:
  成長率計算式の符号を誤ると全銘柄のIVに波及する）
- `src/value/tanuki_valuation/data_fetcher.py`（TTM-QUARTERS-CHECK-1で発見:
  TTMReaderのfcf_list_raw/fcf_5yr_avg構築ロジックは大半の銘柄の
  DCF計算に影響する）
- `src/value/tanuki_valuation/pipeline.py`の`_save_result()`（growth_sanity
  検証ロジック）・`_compute_tanuki_score()`（TANUKI SCORE分類ロジック）
  部分（GROWTH-VERDICT-SEQUENCING-BUG-1で発見: DCF計算式そのものを
  変更しなくても、growth_sanityのverdict/warnings・TANUKI SCOREの
  Classification〈GROWTH_PREMIUM等〉が対象となる場合、rice.py/
  core_calculator.py同様に影響銘柄のpipeline.py再実行が必須。
  対象は「Step 3: TTMデータ変更なし・影響銘柄のみ再実行」に分類）

**手順：**

```bash
# Step 1: データ品質監査（影響銘柄を特定）
python common/sec_data/audit.py

# Step 2: quarterly.py / normalizer.py / ttm_calculator.py を変更した場合
python common/sec_data/update.py [影響銘柄]

# Step 3: rice.py / core_calculator.py を変更した場合
#   → TTMデータ変更なし。影響銘柄のパイプラインのみ再実行
python src/value/tanuki_valuation/pipeline.py [影響銘柄]

# Step 4: 再監査で問題消滅を確認
python common/sec_data/audit.py
```

**影響銘柄の特定方法（rice.py変更時）：**
全銘柄 TTM を走査して変更前後の Q 値を比較するスクリプトを都度作成するか、
変更内容から論理的に対象銘柄を絞り込む（例：ni < 0 チェック追加 → 赤字年が含まれる銘柄）。

**全銘柄再生成後の必須検証：**
```bash
python common/sec_data/report_consistency_check.py
# NG=0 を確認してからコミットする
# WARN は内容確認の上、対処が必要なもののみ修正する
```

**年度キー・年度割り当てに関わる変更時の追加検証（PARSER-1の教訓・必須）：**

`parser.py` の年次辞書キーや年度割り当てロジックを変更した場合、
pytest と test_iv_formula が全通過しても**過去年度データの破壊を見逃す**。
（2026-06-13 事例: 年次キーを fy→end_date年 に変更した際、INTU FY2019 revenue が
10-K内Q1比較値 $1.16B で通年値 $6.78B を上書きする regression が発生したが、
式の整合テストでは検出できなかった）

そのため以下を必須とする:
```bash
# 変更前後で annual_YYYY.json の主要系列が変わった銘柄を全件抽出
#   対象フィールド: revenue / net_income / fcf / total_debt
#   特に non-December 決算企業（AAPL/MSFT/NVDA/CRM/ELF/HQY/COHR/INTU等）と
#   上場直後・SPAC銘柄は FY 10-K 内の他時点比較値が混入しやすい
```
- 差分が出た銘柄は 10-K 実績との一致を 3 銘柄以上スポットチェックする
- IV/FCF_Base/CAGR が動いた銘柄を一覧化する（直近5年系列が変われば波及する）
- **「テスト全通過」は年度割り当ての正しさを保証しない**ことを前提に、
  必ず before/after の全件差分で担保してからコミットする

**aggregate_annual を変更した場合の追加確認（ANNUAL-FY-1の教訓）：**

`aggregate_annual`（EPS Analyzer pipeline.py）のグループ化ロジックを変更した場合、
IV に直接影響する `estimate_fcf_from_eps` 経由の波及が起きる。
（2026-06-13 事例: filing_date[:4] → fiscal_year に変更した際、
NVDA +18% / MSFT -12% / AVAV +93% / IOT applied=False→True 等、20銘柄のIVが変化）

そのため以下を必須とする:
- `adjustments.py の estimate_fcf_from_eps` が参照する `annual.json years[0]` の
  adjusted_net_income が変わった銘柄を全件抽出する
- IOT等の applied=False→True の変化（赤字→黒字化）は特に要注意:
  本物の黒字化なら許容、ゼロ近傍アーティファクトならゲート閾値を見直す
- **年度判定は `common/sec_data/utils.py` の `determine_fiscal_year()` に統一済み**（ARCH-DATA-1-FY 2026-06-25完了）:
  直接呼び出すのは `parser.py`（年次10-Kエントリの分類、2026-09-06時点で4箇所）と
  `extract_key_facts.py`（四半期エントリの(fiscal_year, quarter)分類・Q4逆算時の年次
  エントリマッチング、4箇所）の2ファイルのみ。`aggregate_annual`
  （`adjusted_eps_analyzer/pipeline.py`）は本関数を呼ばず、extract_key_facts.pyが
  設定済みの`fiscal_year`フィールドで単純にグループ化するだけの間接消費箇所であり、
  独立した呼び出し箇所ではない。
  変更する際は `determine_fiscal_year()` 本体と、直接呼び出す2ファイルの
  呼び出し箇所で矛盾が生じないか確認すること（呼び出し箇所数は今後の実装追加で
  変動しうるため、着手時に `grep -n "determine_fiscal_year("` で現状を再確認する
  こと。[[CLAUDE-CODE-START-FY-DESC-FIX-1]]参照）。

**タイブレークロジック追加時の適用範囲限定（ARCH-DATA-1ステージ1の教訓・必須）：**

複数バージョン・複数ソースの競合を「新しい方を優先する」等のタイブレークで
解決するロジックを追加する際は、適用対象を無条件に広げず、そのタイブレークが
本来解決すべき状況（例: 訂正申告の関与）に限定する。通常の比較年度再掲・
多重掲載など、タイブレークの前提が成立しない別パターンまで巻き込むと、
数字の意味自体が異なる（discontinued operations区分変更等）ケースを
誤って「新しい方が正しい」と判定してしまうリスクがある。
（実例: ARCH-DATA-1ステージ1でfiled日タイブレークを無条件適用したところ
509件の差分が発生し、多くが誤りと判明。10-K/A関与時に限定して185件に
収束させた）

**既存関数のフィルタ条件拡張時の横展開確認（ARCH-DATA-1ステージ1の教訓・必須）：**

既存関数のフィルタ条件（例: form判定）を拡張する際は、同じフィルタパターンが
別の目的で流用されている箇所がないか確認する。
（実例: 10-K/A対応でform条件を拡張した際、`_detect_fiscal_end_month`
〈会計年度末月検出〉が同じ関数を流用しており、RCAT〈決算期変更銘柄〉で
無関係な回帰を引き起こしかけた。1つの拡張が意図しない副作用を生まないか、
フィルタの使用箇所を横断的に確認すること）

---

## 重要ルール

### AI APIキー管理ルール

- システム全体のAI APIはxAI（XAI_API_KEY）に統一されている
- 新規AI API呼び出しを実装する際は必ずGrok（`api.x.ai/v1/chat/completions`）を使用すること
- モデルはフォールバック方式：`["grok-3-mini", "grok-3", "grok-2-1212"]` の順で試行
- GeminiやOpenAI等の別APIを使用しているコードを発見した場合はGrokに移行すること

### 財務指標計算における期ズレ防止ルール（BUG-NETDEBT-5の教訓）

Net Debt 等、複数の BS 項目（Cash / ST_Invest / Debt）を組み合わせて計算する場合、
**すべての項目が同一決算期（同じ as-of 日）から取得されていること**を確認する。

- **NG例**: Cash は最新四半期（Q1 2026）・ST_Invest は年次（FY2025）から混在取得
- **OK例**: Cash・ST_Invest・Debt のすべてを同じ四半期 bs から取得

**normalized JSON にフィールドが存在しない項目は自動上書き経路から漏れやすい。**
BUG-NETDEBT-1 で Cash は自動更新されても、ShortTermInvestments がない場合は
ST_Invest が年次のまま取り残される（→ BUG-NETDEBT-5 で修正済み）。
新たに BS 項目を追加するときも同様のズレが起きないか確認すること。

### レポート定義の明示ルール（外部AIレビュー誤指摘の予防）

外部 AI が「計算がおかしい」と指摘するパターンのうち、設計仕様として明示化しておくもの：

- **FCF_History の CapEx 定義**: 素の OCF − PP&E 購入（Capitalized Software 除く・FinanceLease 除外）。
  R&D 資本化補正・maintenance CapEx 分離は FCF_Base にのみ反映し、FCF_History には乗せない。
  外部 AI が「CapEx が過少だ」と指摘してきた場合は FCF_Base との差分を確認してから判断する。
- **OperatingLeaseLiability は Total_Debt に含めない**: ASC 842 オペレーティングリースは
  利付き借入（金融負債）ではなく将来リース支払義務。IONQ の $30M 等は意図的に除外。
  EV 計算にリースを含める「アジャステッド EV」方式を採用する場合は設計変更として明示すること。
- **DCF構成要素は「上から足すと必ずIVになる」構造で表示する（REPORT-6拡張の教訓・必須）**:
  外部AIは report.txt のDCF項目を順に足してIVを逆算する。途中の段が非表示・不整合だと
  「IV再現不能」と誤指摘される。
  **注意（2026-07-15追記・[[REPORT-ALPHA-STALE-1]]）**: 下記の表示順序は2026-06-13
  当時（αがP_tに乗算されていた頃）の仕様であり、ALPHA-REDESIGN-1（2026-06-25、
  P_t算出からalpha乗算を廃止）後は**古いまま追随できていない**（`pipeline.py:1478-1510`
  が今も`DCF_v0_x_alpha = v0×(1+alpha)`を計算・表示しており、実際のIntrinsic_Valueとの
  自己矛盾が生じている。詳細はBACKLOG [[REPORT-ALPHA-STALE-1]]参照・未修正）。
  このルールを参照して新規表示ブロックを実装する際は、まず現在のcore_calculator.pyの
  実装（P_t = V₀ + RPO_PV + GrowthOption_PV、alpha非乗算）を確認してから合わせること。
  以下は2026-06-13時点の（現在は古い）記述:
  `DCF_FCF_PV → DCF_TV_PV → DCF_v0(=PV合計) → Alpha_Premium → DCF_v0_x_alpha(=v0×(1+α))
   → RPO_PV → Growth_Option_PV → Equity_Value(=上記−Net_Debt、優先株があれば控除行追加)
   → Shares_Used(source明記) → Intrinsic_Value`。
  DCF構成要素を追加・変更する際は test_iv_formula.py で「表示項目を積み上げてIVに一致」を
  必ず回帰テストすること（同テスト自体も[[TEST-STALE-IV-1]]によりALPHA-REDESIGN-1に
  未追随のまま、MSFT/NVDAで既知失敗している。「テスト失敗＝既知の無関係な問題」と
  安易に片付けず、テストが検出しようとしている対象が実際に直っているかを都度疑うこと）。
- **DCF_Reliability=LOW の判定仕様（Policy A・明文化済み）**:
  FCF実績マイナスで revenue_floor 適用時は DCF_Reliability=LOW とし、IVは参考値扱い。
  TANUKI SCORE 分類は BUY/TRIM/HOLD/WATCH → **WATCH に丸める**（SELL/PASS は維持）。
  乖離率は表示するが分類判定には使用しない。この仕様を変更する場合は CRWV/SOUN/RKLB/JOBY/CEG 等
  該当銘柄への影響を確認すること。
- **RICE 定義式は実装に一致させる**: 表示する定義式は `(G × VC_Factor × Q × CF) / WACC`。
  VC_Factor を式本体から省くと外部AIが「定義と計算値が2倍ずれる」と誤指摘する（注記バグ）。
- **FCF_Conversion_Rate は Adj_NI への変換率であり OCF→FCF 変換率ではない**: 高FCFマージン企業
  （ADBE/PLTR等）では実績FCFを下回るが、これは正常化前提による保守設計。定義文に明記する。
- **実績データに基づく分類・レートの自己補正は「config書き換えなしの純関数」パターンを踏襲する**
  （determine_fcf_base()のCV方式が原型。FCF-CONVRATE-DESIGN-LIMIT-1の
  `check_software_system_reclassification()`で2026-07-14に踏襲）。
  分類・パラメータが実績データの蓄積によって変わりうる場合、config/beta_config.json等の
  永続化ファイルをpipeline.py実行中に書き換えるのではなく、実行のたびに直近実績から
  純関数として再判定し、その実行内のみでレート等を差し替えてreport.txtに注記する設計とする。
  理由: ①pipeline.py実行ごとにconfigへのgit diffが発生しない、②config書き換えは
  beta_fetcher.py等の明示的な手動スクリプト経由のみという既存アーキテクチャ規約と整合する、
  ③バッチ実行時の並行書き込み・巻き戻りリスクを避けられる。
  永続的な分類変更が必要と判断した場合は、この自己補正結果を参考に人間が別途
  beta_fetcher.py等の登録スクリプトを実行して確定させる（自動では確定しない）。
- **IV割引率（Rm=10%/β=0）は市場リスクを意図的に除外した本源価値**: 高β銘柄では市場WACC比で
  IVが高めに出るが設計通り。市場リスク調整後の参照は WACC_CAPM_Reference でのIVを併用する旨を
  定義文に記載（外部AIは「高β銘柄でIV過大」を頻繁に誤指摘するため）。
- **ROE=N/A（負債超過）表示仕様**: 全年度で株主資本≤0の銘柄（PM/TSLA等）は ROE=0% ではなく
  ROE=N/A（負債超過）と表示する。`reader.py get_roe_avg_detail` が `(None, 0, False)` を返し、
  `pipeline.py` が `roe_years_used==0` をシグナルとして N/A 表示に切り替える仕様。
  `roe_avg or 0.0` のような OR 短絡評価は None を 0.0 に変換するため禁止（ROE-ZERO-1の教訓）。
- **industry_alpha_caps 方針**: セクター別 `_alpha_caps` よりも業種別 `_industry_alpha_caps` を優先する。
  同一セクター内で業種差が大きい場合（例: Communication Services 内の Telecom Services）に使用。
  `core_calculator.py` でのチェック順は `_mega_tech_tickers` → `_industry_alpha_caps` → `_alpha_caps`。
  新銘柄でαが過大になる場合は業種名（yfinance `info.industry`）を確認してから設定すること。
- **FCF外れ値の除外方向性ルール（FCF-OUTLIER-1の教訓）**: 上方乖離（latest_fcf > 5yr_avg）の場合、
  一過性コスト（impairment等）が検出されても `transient_explains=False` とし FCF を除外しない。
  一過性コストは FCF を下げる方向に働くため、FCF が高い年に一過性コストがあれば
  「コストがなければさらに高かった」ことを意味し、除外の根拠にならない。
  除外が許可されるのは下方乖離（latest_fcf < 0 か latest_fcf < 5yr_avg）のみ。
- **比較表示する2つの倍率は必ず同一期ベースで計算する（EPS-PER-TTM-1の教訓）**:
  GAAP PER（yfinance trailingPE = TTM）と Adjusted_EPS_PER を並べる場合、
  後者も同じTTM（直近4四半期の adjusted_eps 合計）を分母にしなければ比較が無意味になる。
  年次FYを使うとGAAP（TTM）と期間が食い違い、成長株では ADJ > GAAP の逆転が常態化する
  （実例: NVDA 48.3x vs GAAP 31.4x → TTM統一後 30.3x に正常化）。
  新たにPER・EV・PS等の倍率を並列表示する場合は、両者の分母期間が同一か必ず確認すること。
  片方が TTM なら他方も TTM、片方が NTM（Forward）なら他方も NTM に揃える。

### 外部AIレビューの活用と品質還元ループ

大きな修正後は `report.txt` を外部 AI（Grok / Claude 等）に敬対的レビューさせることで
ロジックバグを早期に発見できる。

**レビューの仕分けルール（指摘 → 分類 → 対応）：**
1. **本物のバグ**: 再現可能な計算誤り → 修正 → `report_consistency_check.py` に検出項目を追加して恒久化
2. **設計仕様**: 意図的なモデル選択（FCF_Base方式・OperatingLease除外等）→ 定義文を明記して予防
3. **外部データ差**: AI の学習データと XBRL 値の差 → 一次ソース（SEC EDGAR）で照合して判定

**サンプル選定のコツ:**
- 主力9銘柄だけでなく、消費セクター・金融・赤字初期（IONQ/JOBY等）をセクター横断でかけると
  属性固有のバグ（SPAC誤タグ・金融収益混入・CAGR過大等）が出やすい。
- 成熟・ディフェンシブセクター（通信: VZ/T、公益: CEG/VST、タバコ: PM/MO等）を含めると
  ROE=N/A（負債超過）/ alpha上限抵触 / FCF外れ値 等の設計端ケースを検出しやすい。

### SEC EDGAR一次情報検証の標準手順（WARN-23等のfyタグ裏取り確認・2026-07-18追加）

「企業自身がその期間を何と自称しているか」を一次情報で確認する必要がある場合
（WARN-23のfyタグ裏取り不一致確認、RCAT型決算期変更の実在確認等）、以下の手順が
実データで検証済み：

1. **WebFetchはSEC EDGARに対して403で失敗する**（`data.sec.gov`・
   `www.sec.gov/cgi-bin/browse-edgar`とも、User-Agent要件のため）。
   代わりにBashツールで`curl`を直接使う：
   ```bash
   curl -s -A "Koichi Personal Investment Tools koichi@example.com" \
     "https://data.sec.gov/submissions/CIK{10桁ゼロ埋めCIK}.json" -o out.json
   ```
2. 提出履歴（`filings.recent`、直近1000件超は`filings.files`に別ファイル分割）
   から対象accession numberの`form`/`primaryDocument`を特定する
3. 実際の10-K本文を取得: `https://www.sec.gov/Archives/edgar/data/{CIKの先頭ゼロなし}/
   {accessionのハイフンなし}/{primaryDocument}`
4. カバーページの"For the fiscal year ended [date]"表記、および本文中の
   "fiscal 20XX"自己言及（企業自身の用語定義文があれば最優先）で判定する。
   `dei:DocumentFiscalYearFocus`タグも参考になるが、**このタグ自体が
   filer側のテンプレート更新漏れで誤っている実例を複数確認済み**
   （FCX/HON: カバーページ本文と同一文書内で直接矛盾）。カバーページ本文の
   方が信頼度が高いため、両者が食い違う場合は本文を優先する

**Bashツールのパス変換に関する既知の注意点**: Git Bash（MSYS）は
コマンドライン**引数**として渡されたPOSIX形式パス（`/c/Users/...`）を
Windows形式に自動変換するが、Pythonスクリプト内の文字列リテラルに
埋め込まれたパスは変換されない。`python3 -c "open('/c/Users/...')"`は
`FileNotFoundError`になるため、スクリプト内で直接パスを扱う場合は
Windows形式（`r"C:\Users\..."`)を使うこと。

### 自動生成データファイルのgit管理ルール

- `docs/` 以下の自動生成JSON/CSVは `.gitattributes` で `merge=ours` 設定済み
- `git pull --rebase` でコンフリクトが発生した場合、対象データファイルは自動でローカル版が採用される
- 新たに自動生成データファイルを追加した場合は `.gitattributes` にも追記すること
  （対象: `docs/market-monitor/`, `docs/portfolio/tail/data/`, `docs/value-monitor/tanuki_valuation/data/`）
- **`git checkout --theirs` をデータファイルに使用してはならない**
  （JSONが古いリモート版で上書きされデータが消失する）

### 生成パイプラインをバイパスした手動データパッチの禁止（SOFI-DATA-1の教訓・2026-07-13追加）

`common/sec_data/normalized/` 等の生成物JSONに対し、生成元コード
（quarterly.py/parser.py等）を経由しない手動編集（直接JSONファイルへの
値の追記・上書き）を行ってはならない。次回の `update.py` 実行（GitHub Actions
週次自動更新を含む）で標準パイプラインが再生成する際、手動編集分は
参照されず静かに上書き・消失する。

2026-06-24のSOFI LTDebt修正がこのパターンで実害化した実例:
「`quarterly.py` フェッチスクリプトがフォールバック未対応のため手動パッチ」
という形で13件のデータを直接JSON追記したが、直後の自動更新で全て巻き戻り、
2026-07-13時点でLTDebtが3年近くstale化したまま気づかれずにいた
（Net_Debtが実際はnet cashであるにも関わらずnet debtとして表示され続けた）。

タグ切替・フォールバック未対応等でデータが取得できない場合は、必ず
生成元コード側（`TICKER_RESTRICTIONS` へのticker限定オーバーライド追加等、
`quarterly.py` のSOFI `ltdebt_concept`/`revenue_concept` 参照）に恒久修正を
実装すること。「今回だけ手で直す」対応は次回の自動再生成で必ず巻き戻る
前提で臨む。

### SEC自動更新とTANUKI VALUATION自動更新の生成順序ズレ（WARN12-COHR-ONDS-1の教訓・2026-07-13追加、2026-08-26状況更新）

**2026-08-26追記**: 2026-08-22の`workflow_run`連鎖化対応
（[[WORKFLOW-SEC-TANUKI-GAP-1]]・[[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]）
により、下記の独立cron自体は既に廃止済み。`TANUKI_VALUATION_Update`は
`Market_Data_Daily_Update`/`HypeCore_Update`/`Adjusted_EPS_Data_Update`/
`Stonks_Silo_Update`いずれかの完了で`workflow_run`起動する設計へ変更
されており、この経路は2026-08-24〜26の実地確認で実際に機能している
ことを確認済み（`[[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]`は
BACKLOG_DONE.mdへクローズ済み）。ただし`SEC_Data_Update`→
`HypeCore_Update`等3ワークフローの連鎖自体は2026-08-26時点でまだ
実地確認できておらず（`[[WORKFLOW-SEC-TANUKI-GAP-1]]`は未クローズ、
次回2026-08-30サイクルで再確認予定）、以下の「毎週26時間のズレ」という
記述は当時（2026-07-13時点）の独立cron運用を前提としたものであり、
現在の実態とは異なる可能性がある点に注意。`WARN-12`等の期ズレ症状が
出た場合の切り分け手順自体（`calculation_date`とコミット日時の比較）は
引き続き有効。

`SEC_Data_Update.yml`（毎週**日曜**12:00 UTC=JST21:00）と
`TANUKI_VALUATION_Update.yml`（**平日**月〜金のみJST23:05）は独立した
cronスケジュールで動作しており、`config/workflow_dependencies.json`が
定義する論理的依存関係（TANUKI_VALUATION_UpdateはSEC_Data_Updateに依存）は
実際のGitHub Actionsトリガーとしては実装されていない
（[[WORKFLOW-SEC-TANUKI-GAP-1]]参照、2026-07-13登録時点の記述。
2026-08-22以降の状況は上記追記参照）。

このため**日曜のSEC自動更新後〜月曜23:05の次回TANUKI VALUATION自動更新
までの約26時間、SECデータは最新だがlatest.json/report.txtは陳腐化した
まま**という状態が毎週構造的に発生しうる（2026-07-13時点の記述）。
`report_consistency_check.py`の`WARN-12`（Cash-STI期ズレ）等がこの
時間帯に新規発生した場合、まずfact競合等のコードバグを疑う前に、
`latest.json`の`calculation_date`と対象銘柄の
`common/sec_data/data/{TICKER}/quarterly_*.json`のコミット日時を
比較し、**単なる生成順序のズレでないか**を確認すること（該当すれば
`pipeline.py [TICKER]`の再実行のみで解消する。実例:
2026-07-12にCOHR/ONDSで発生、コード修正不要だった）。

### 表示期間フィルタのルール

- HTMLの日付フィルタ（`getDate()-N`）は指標の更新頻度に合わせて設定すること
- 月次指標を含むセクションは最低90日以上を確保すること（14日では月次指標が表示されない）

---

## BACKLOG優先順位の目安

（2026-08-26ブラッシュアップ時点で全件再確認: MP-BIZDAY-1・ARCH-DATA-1・
TSCORE-TRAP-1・SEC-CTRL-1の4件はいずれも2026-06-24〜07-19の間に完了済み
（BACKLOG_DONE.md参照）だったにもかかわらず本欄が未更新のまま残存して
いたため削除した。本欄は個々のタスク完了報告のたびに都度更新する
運用が原則であり〈CHAT_RULES.md「次セッション着手順序」欄の都度更新
参照〉、この一括削除は都度更新の代替にはしない）

（2026-08-30追記: BACKLOG.md未完了項目181件全件の陳腐化・ニーズ・課題
認識検証＋開発合理性による分類・重要性判断・着手手順の提案が完了し、
`BACKLOG_PRIORITY_ROADMAP.md`に記録した。次の作業候補はまず同ファイルの
「フェーズ1: 最優先グループ」10件を参照すること。本欄・BACKLOG.md本体の
記載が優先し、`BACKLOG_PRIORITY_ROADMAP.md`はフェーズ1消化時・大きな
BACKLOG変動時に更新する）

### 順次着手（優先度中・難易度中〜高）
- TANUKI-FIN-1: 金融株DDM対応

### 着手条件あり
- DESIGN-15: 期待と理論価格の整理（DESIGN-4・5の設計確定後）
- Moomoo API Skill移行（signal.jsonバックテスト実施後）
- Moomoo API系4件（クォータ回復後）

---

## 新規銘柄登録時の必須手順

**2026-09-03改訂（[[REGISTER-FLOW-REDESIGN-1]]方針2・3）**: Step 1〜8は
`common/registration/register_ticker.py`が自動連続実行する。Step 0・0.5
（下記）を手動で終えた後、このオーケストレーションスクリプトを実行する
運用に切り替わった。Step 1〜8を個別に手動実行する旧手順は行わないこと
（[[REGISTER-FLOW-REDESIGN-1]]方針5、「手動一括登録」の抜け道を塞ぐ
構造的な理由による）。

```bash
# Step 0: カナダ企業チェック（登録前に必ず実行）
python -c "import yfinance as yf; t = yf.Ticker('[TICKER]'); print(t.info.get('country', 'N/A'))"
# 出力が "Canada" の場合は登録を中止する。
# カナダ企業はIFRS/40-Fのため TANUKI VALUATION・EPS Analyzerに非対応。

# Step 0.5: 登録メタデータの記録（必須・オーケストレーションスクリプト実行前に実施）
# cik_lookup.csv の新規行に以下を記録してから実行すること：
#   status: 常に provisioning で開始する（2026-09-03変更、[[REGISTER-
#     FLOW-REDESIGN-1]]方針2）。common/sec_data/tickers.pyの
#     _INVALID_STATUSESに含まれるため、Step 8でNG=0が確認され
#     オーケストレーションスクリプトが昇格させるまでは4大パイプライン
#     （TANUKI VALUATION/HypeCore/EPS Analyzer/STONKS SILO）いずれの
#     自動対象にも含まれない。「指示書内で明示された本来のstatus
#     （active/candidate）」は、後述する--target-statusへそのまま
#     引き継ぐため別途メモしておくこと。
#   tanuki/stonks_silo/eps/hypecore: 業態に応じた対象可否フラグ
#   cik: SEC EDGARのCIK（10桁ゼロパディング）
#   registered_date: 作業実行日（本日の日付、YYYY-MM-DD）
#   registration_source: 指示書内で明示されていればその値を使用。
#     不明な場合は manual_thesis をデフォルトとする。
#     （定型カテゴリ例: moomoo_screening / manual_thesis / catalyst_discovery /
#      satellite_watch / initial_setup / technical_screening / unknown）
#   registration_note: 指示書内の登録理由・経緯を1〜2文で要約して記録。
#     指示書に理由が書かれていない場合は、Claude Codeから
#     「登録理由が指示書に見当たりません。記録すべき経緯を教えてください」と
#     確認を求め、回答を得てから記録する。
#
# 【暫定注意（[[ANOMALY-PATTERN-CATALOG-1]]実装までの措置・2026-07-19追加）】
# 新規銘柄登録時は、BACKLOG.md [[ANOMALY-PATTERN-CATALOG-1]]記載の
# 既知パターン（型A：候補タグのfreshness収束問題等）に該当しないか
# 手動確認すること。特にBS項目（short_term_investments等）でNoneや
# 既存値との齟齬が疑われる場合は、TICKER_RESTRICTIONSの適用要否を
# 個別に検討すること。

# Step 1〜8: オーケストレーションスクリプトを実行
# --target-statusには、Step 0.5でメモした「指示書内で明示された本来の
# status」（active/candidate）をそのまま渡す。複数銘柄をまとめて登録する
# 場合もスペース区切りで並べれば1銘柄ずつフルにStep 1〜8を実行する。
python common/registration/register_ticker.py [TICKER] --target-status active
```

このスクリプトはStep 1（SEC取得）・Step 2（β取得）・Step 3（TANUKI
VALUATIONパイプライン実行）・Step 4（audit.py --check-beta）・Step 5
（HypeCore、hypecore=trueのみ）・Step 5b（EPS Analyzer、eps=trueのみ）・
Step 6（Discover登録）・Step 7（monitor_tickers.yaml追加）・Step 8
（registration_validator.pyでNG=0を確認し、NG=0ならstatusを
`--target-status`の値へ昇格）を自動連続実行する。各ステップはべき等
（再実行しても安全）なので、失敗・一時停止したら原因に対処した上で
**同じコマンドをそのまま再実行**すればよい（ロールバックは実装されて
いない設計）。

**Step 2.5・3.5は一時停止し、Claude Codeの判断を待つ**（risk_fetcher.py・
Discoverサブシステム撤去と同じ「根拠不明の生成をそのまま採用しない」
方針。スクリプトはこの2ステップを代行しない）：

- **Step 2.5**（sectorが"Software_System"の場合のみ発生）: 10-Kの
  前受収益（Deferred Revenue）関連の記述・事業内容をClaude Codeが確認し、
  `config/beta_config.json`の`overrides.{TICKER}.sector`を
  `Software_System_Mature`または`Software_System_SaaS`に設定してから
  再実行する（参考値として`beta_fetcher.py [TICKER]
  --classify-software-system --dry-run`でDR/Rev比率を確認できるが、
  最終判断は10-K原文の確認に基づくこと）
- **Step 3.5**（`tanuki=true`の銘柄は毎回発生。ASC 280正式セグメント数は
  XBRLタグから機械的に判定できないため）: Claude Codeが10-Kの
  "Segment Information"セクションを確認し、下記「新規銘柄のセグメント
  設定判断ルール」に従ってLLY型/LMT型を判定した上で、
  `common/sec_data/data/{TICKER}/segment_review.json`に
  `{"reviewed": true, "formal_segments": N, "result": "...",
  "note": "<10-Kの該当箇所の引用・確認内容>"}`を書き込んでから再実行する
  （LMT型の場合はあわせて`config/segment_config.json`も設定する）

**`common/sec_data/update.py`（Step 1本体）はstatusを見ない既知の
ギャップ**: `config.py::get_all()`という別経路でティッカー一覧を取得
するため、provisioning状態のティッカーもSEC取得自体は通常通り行われる
（SEC取得は計算・表示に影響しないため実害小、[[REGISTER-FLOW-
REDESIGN-1]]に記録済み）。

**Step 8.5: 対象システム横断チェック（必須・XBRL-TAG-KLAC-1-FOLLOWUP 2026-07-09新設）**

Step 1〜8はシステム個別の登録手順だが、「意図した通りに対象/対象外が
振り分けられているか」を横断で確認する項目がなかったため、Step 5
（HypeCore）の実行漏れが後から発覚する事例が発生した。オーケストレー
ション化により実施漏れ自体は起きにくくなったが、以下は引き続き確認する：

- [ ] TANUKI VALUATION（tanuki）/ HypeCore（hypecore）/
      EPS Analyzer（eps）/ STONKS SILO（stonks_silo）—
      cik_lookup.csvの4フラグが業態と整合しているか
      （例: 黒字大型株なのにstonks_silo=trueになっていないか、
      hypecore=trueなのに実データ`docs/value-monitor/hypecore/data/{TICKER}_poc.json`
      が生成されていないか）
- [ ] segment_config.json — ASC 280 formal segment数を確認し
      LMT型（2segment以上）なら登録済みか（Step 3.5参照）
- [ ] rpo_config.json — SaaS/クラウド業態ならwhitelist登録済みか
- [ ] TANUKI TAIL — 保有ポジションでない限り登録しない
      （誤操作防止の注意書き。thesis.json等を新規登録手順の
      一環として作成しないこと）
- [ ] monitor_tickers.yaml — cik_lookup.csvとの件数・銘柄突合を明示的に行う
      （2026-07-11当時は「複数銘柄の手動一括登録」でStep 7・Step 8
      〈[TICKER]指定での個別実行〉の両方が省略される事例が6件発生し、
      唯一のセーフティネットだった`registration_validator.py`の
      P4-CIKOrphan〈cik_lookup.csv全体を無条件スキャン、WARN止まりで
      非ブロッキング〉頼みで見落としやすかった。2026-09-03のオーケスト
      レーション化〈[[REGISTER-FLOW-REDESIGN-1]]方針3・5〉により
      Step 7・Step 8はスクリプトが常にセットで実行するため、この種の
      省略は構造的に起きにくくなった——ただし旧手順の個別コマンドを
      直接実行した場合は同じリスクが残るため、本チェックリスト自体は
      残す。[[TICKER-AUDIT-1]]・[[TICKER-SOURCE-UNIFY-1]]参照）
- [ ] EPS Analyzerデータ — eps=trueの銘柄でStep 5bの実施漏れがないか、
      `registration_validator.py`のP1-Step5b-EPS WARN「EPS Analyzer なし」が
      残っていないかを確認する（monitor_tickers.yamlへの登録だけでは
      既存データは自動生成されない）

**注意事項：**
- Step 2 を忘れると β=未設定のまま yfinance の raw 値が使われる
- 異常値が疑われる場合は `--dry-run` で差分確認してから適用
- LMT 等 Damodaran 手動設定銘柄は `beta_fetcher.py` の `DAMODARAN_OVERRIDES` に追加
- Step 5 HypeCore は yfinance 依存。KULR 等データ不足銘柄は失敗するが無視してよい
- Step 5b EPS Analyzer: 非US GAAP（IFRS系外国企業、例: ASML）は us-gaap タグが欠如しているため
  update.py で annual データ 0 件になる。その場合は cik_lookup.csv の eps 列を false に設定すること。
  XBRL形式が US GAAP でも NetIncomeLoss の四半期データが欠損している銘柄（BKNG, FCX 等）も
  同様に eps=false を設定する。
  ※ NetIncomeLoss が古いデータしか持たない場合は ProfitLoss タグへ自動フォールバックする（SCCO対応 2026-06-15）。
    タグ選択ロジック: 「最初に見つかったタグ」→「最新エントリが最も新しいタグを優先」（extract_key_facts.py）

**EPS Analyzer 設計ルール（2026-06-15 更新）：**
- **ProfitLoss フォールバック（extract_key_facts.py）**: NetIncomeLoss が 5年以上古い場合、ProfitLoss タグへ
  自動フォールバック。タグ選択は「最新エントリが最も新しいタグ優先」ロジック。対象: SCCO 等の再編企業
- **DTA 自動補正（pipeline.py `apply_dta_adjustments()`）**: 繰延税金資産（DTA）認識による adj_eps 異常高値を
  自動検出・補正。
  - Type-A: `pretax ≤ 0 かつ NI > 0`（損失→DTA還付で黒字化） ← LYFT Q4 2025 型
  - Type-B: `NI > pretax × 3`（黒字にDTAが上乗せ）
  - 補正値: `adjusted_net = pretax - median(正常四半期の税費用)`
- **split_history.yaml 管理**: 株式分割が確認された銘柄は `config/split_history.yaml` に追記する。
  現在登録済み: NOW（2025-12-18 5:1）。追記後は EPS Analyzer を単体実行して TTM adj_eps を確認すること。
- Step 6 の discover_config.json は **dict 形式**（キー=ticker）。list 形式のコードは誤り
- Step 7 の monitor_tickers.yaml は **単純リスト形式**（yaml.dump 使用不可 → コメントが消える）
- Step 8 の NG は必ず解消してからコミットする。主なNG要因:
  - `P2-A NG`: latest_revenue が TTM の 3 倍以上乖離 → SEC パーサーのタグ確認
  - `P1-Step3 NG`: latest.json 未生成 → pipeline.py を再実行
  - `P1-Step7 NG`: monitor_tickers 未登録 → Step 7 を再確認
- SaaS系銘柄でRPOプレミアムを適用する場合は `config/rpo_config.json` の
  whitelist に理由コメント付きで明示登録する（industry keyword 依存禁止）
  理由: keyword は将来銘柄追加時に意図しない適用の再発リスクあり（GOOGL等参照）

**新規銘柄のセグメント設定判断ルール（SEGMENT-1 全17銘柄完了の教訓）：**

segment_config.json の設定要否は **ASC 280 の formal operating segment 数** で判断する。
製品別・エンドマーケット別の売上開示（disaggregated revenue）は **formal segment ではない**。

| 判定 | 条件 | 設定 | 例 |
|------|------|------|-----|
| LLY型（設定不要） | formal segment が1つ | General 100%のまま | LLY/MRVL/BSY/ALAB/ELF |
| LMT型（設定対象） | formal segment が2つ以上 | 比率・成長率を設定 | LMT/AMAT/VRT/COHR/LITE |

設定する際の注意:
- セグメントの名称・比率は 10-K の **"Segment Information"（ASC 280）** セクションの数値を使う
- 製品別や顧客別の disaggregated revenue（ASC 606）は使わない（VST/CEGで架空セグメントを埋めた失敗参照）
- **growth rate の設定根拠を segment_config.json の comment フィールドに記録する**
  （例: "FY2024 YoY +13%、中期ガイダンス考慮で10%設定"）
- weighted_growth が recommended_g より大幅に高い場合はIV上昇（LMT +12%型）、
  低い場合はIV下落（COHR -57%型）。どちらも「正しい是正」だが before/after を記録すること
- COHR/LITEのような光通信デバイス・M&A統合後の銘柄は rec_g が急成長TTMを引いて過大になりやすい。
  設定後のIV下落が大きい（-50%超）場合でも、長期成長率として妥当なら正しい是正

---

## よく使うコマンド

### 単体テスト実行
python src/value/tanuki_valuation/pipeline.py NVDA

### 全銘柄再生成
python src/value/tanuki_valuation/pipeline.py

### pytest実行
python -m pytest tests/ -v

### GitHub Actions 確認
admin.html の「実行」タブ → 一括更新ボタンを使用

---

## TANUKI TAIL 銘柄追加手順

TANUKI TAIL（長期投資テーゼ管理）に新規銘柄を追加する場合、
以下の順序で実施すること。

```bash
# Step T1: TANUKI TAILページでテーゼ登録（UIで実施）
#   → docs/portfolio/tail/data/positions/{TICKER}_thesis.json が生成される

# Step T2: KPI提案生成（Grok）
python src/tail/kpi_proposer.py --ticker {TICKER}
# → docs/portfolio/tail/data/kpi_proposals/{ticker}_proposal.json 生成
# → tail_kpi_map.json に auto_fetchable=true 分が自動追記

# Step T3: TANUKI TAILページでKPI確定（UIで実施）
#   → thesis.json の kpis フィールドにKPIが保存される
#   → 「⚠ KPI未設定」バッジが消える

# Step T4: XBRL セグメントデータ取得（layer2）
python src/tail/xbrl_segment_fetcher.py --ticker {TICKER}
# → docs/portfolio/tail/data/kpi/{ticker}_layer2.json 生成

# Step T5: テキストKPI抽出（layer3）
python src/tail/text_kpi_extractor.py --ticker {TICKER}
# → docs/portfolio/tail/data/kpi/{ticker}_layer3.json 生成
# → auto_fetchable=false のKPIを10-Q MD&A + 8-K EX-99.1 から抽出

# Step T6: コミット
git add docs/portfolio/tail/data/kpi_proposals/ \
        config/tail_kpi_map.json \
        docs/portfolio/tail/data/kpi/
git commit -m "feat: TANUKI TAIL {TICKER} 銘柄追加 layer2/layer3 初期データ"
git pull --rebase origin kaihatsu
git push origin kaihatsu
```

**Step T6以降**: 次回RSS検知時（EDGAR 10-Q/10-K 提出）から四半期レビューが自動生成される。

**注意事項:**
- Step T2 は thesis.type="core" の銘柄のみ対象（satellite銘柄はスキップ）
- Step T5 が失敗した場合（EX-99.1未発見等）でも Step T6 に進んでよい
  → レビュー生成時に layer3 未取得KPIは「— 未取得」と表示される
- CIK が cik_lookup.csv にない場合、Step T2/T4/T5 前に追加すること:
  `echo "{TICKER},{CIK},{会社名},,,true,true,true" >> config/cik_lookup.csv`

---

## 銘柄削除時の必須手順

### 削除対象の判断基準
- 投資対象として見込みがなくなった銘柄
- 上場廃止・買収・合併により追跡不要になった銘柄
- リポジトリサイズ管理のため（目安：100銘柄を超えたら低優先銘柄を削除）

### 削除手順

> ⚠️ **本手順のファイルリストを過信しないこと（2026-09-02追記、
> `[[AVGO-CIK-HISTORY-WRONG-LEGACY-CIK-1]]`クローズ〈AVGO除外〉実施時の
> 教訓）**: AVGO削除を実際に適用した際、
> 下記Step 2・Step 3のリストに載っていない設定ファイル・データファイルが
> 9件（`maturity_config.py`・`growth_sanity.py`・
> `docs/common/company_names.json`・`common/sec_data/data/_cik_cache.json`
> ・`common/market_data/{daily,attributes,analyst_history}/[TICKER].json`
> ・`common/market_data/[TICKER]/`・
> `docs/common/sec_data/normalized/[TICKER]_quarterly_normalized.json`）
> 追加で見つかった。新しいデータレイヤー（`common/market_data/`等）が
> 追加されるたびに本リストが陳腐化する構造的リスクがあるため、**Step 1の
> 前に必ずStep 0（下記）を実施し、本リストを機械的になぞるだけで済ませ
> ないこと。**

```bash
# Step 0: 削除前の全参照洗い出し（本リストを過信せず必ず実施）
grep -rln "[TICKER]" . --include="*.py" --include="*.html" --include="*.js" \
  --include="*.yml" --include="*.yaml" --include="*.json" --include="*.csv" \
  --include="*.md" 2>/dev/null
# ヒットしたファイルを1つずつ確認し、以下に分類する:
#   a. このティッカー専用の生成データ（per-ticker JSON等）→ Step 3相当で削除
#   b. 設定ファイル内のティッカーキー付きエントリ（辞書/dict型）→ Step 2相当で削除
#   c. 過去の完了記録・アーカイブ文書（BACKLOG_DONE.md・CHAT_RULES.md・
#      PROJECT_STATUS.md・SYSTEM_MAP.md・docs/architecture/配下等の日付
#      ナラティブ）→ 過去の事実の記録のため編集しない
#   d. 外部実体を反映したキャッシュ（S&P500構成銘柄リスト等、当該銘柄が
#      実際に指数に含まれる場合）→ 我々の内部登録とは無関係のため編集しない
#   e. コメント中の例示ティッカー・使用例（docstring・placeholder等）→
#      任意（実害なし、着手者の判断で放置可）
#   f. テストコードが実データを読む/登録リストを検証する箇所 → 該当テスト
#      メソッドのみ削除するか、リストから対象ティッカーのみ除外する
#      （テストファイル全体・無関係な他ティッカー分は変更しないこと）

# Step 1: 削除対象を確認
grep [TICKER] config/cik_lookup.csv
grep [TICKER] config/discover_config.json

# Step 2: 設定ファイルから削除
# cik_lookup.csv から該当行を削除
grep -v "^[TICKER]," config/cik_lookup.csv > /tmp/cik_tmp.csv
mv /tmp/cik_tmp.csv config/cik_lookup.csv

# beta_config.json から削除
# 【重要】ensure_ascii=Falseを必ず指定すること（省略すると日本語コメント
# が\uXXXXエスケープに化ける事故が発生する、2026-09-02実例で発覚）
python3 -c "
import json
with open('config/beta_config.json', encoding='utf-8') as f:
    d = json.load(f)
d.get('overrides', {}).pop('[TICKER]', None)
with open('config/beta_config.json', 'w', encoding='utf-8', newline='') as f:
    json.dump(d, f, indent=2, ensure_ascii=False)
"

# discover_config.json から削除
# 2026-08-15: docs/側同期はDiscover_Config_Sync.ymlが自動実行
# （[[DISCOVER-CONFIG-DUAL-MGMT-1]]）
python3 -c "
import json
with open('config/discover_config.json', encoding='utf-8') as f:
    d = json.load(f)
d['tickers'] = {k: v for k, v in d['tickers'].items() if k != '[TICKER]'}
with open('config/discover_config.json', 'w', encoding='utf-8', newline='') as f:
    json.dump(d, f, ensure_ascii=False, indent=2)
"

# monitor_tickers.yaml から削除
python3 -c "
ticker = '[TICKER]'
with open('config/monitor_tickers.yaml', encoding='utf-8') as f:
    lines = f.readlines()
lines = [l for l in lines if l.strip() != f'- {ticker}']
with open('config/monitor_tickers.yaml', 'w', encoding='utf-8') as f:
    f.writelines(lines)
print(f'{ticker} を monitor_tickers.yaml から削除しました')
"

# 以下4件は「該当する場合のみ」削除（全銘柄が登録されているとは限らない、
# 各ファイルをgrepして存在確認してから対応すること）
# - config/segment_config.json のティッカーキーエントリ
# - config/split_history.yaml のティッカーキーエントリ（登録されている
#   場合、tests/test_split_history_adjustments.pyの登録銘柄タプル・
#   専用回帰テストメソッドも対応して削除すること）
# - common/sec_data/fixed_registry.json のティッカーキーエントリ
# - common/sec_data/cik_history.json のティッカーキーエントリ

# 【重要】上記の日本語コメント付きJSON/YAMLファイルは、python json.dump
# の全体再書き出しで既存のコンパクト配列表記（例: ["1234567"]）が
# 多行表記に意図せず再整形される事故が発生しうる（2026-09-02実例で発覚、
# common/sec_data/cik_history.jsonで無関係なDELLエントリの表記が変わって
# しまった）。git diffで対象ティッカーの削除以外に差分がないことを
# 必ず確認し、余分な差分が出た場合はEdit等によるテキストベースの
# 部分削除に切り替えること。

# 併せて確認・該当すれば削除する辞書エントリ（Pythonソース・小規模JSON）:
# - src/value/tanuki_valuation/maturity_config.py の _TICKER_TV_G 辞書
# - src/value/tanuki_valuation/growth_sanity.py の
#   TICKER_INDUSTRY_OVERRIDES 辞書
# - docs/common/company_names.json のティッカーキーエントリ
# - common/sec_data/data/_cik_cache.json のティッカーキーエントリ
#   （.gitignore対象のローカルキャッシュのため削除してもコミット不要、
#   ローカル環境の整合性のためだけに実施する）

# Step 3: データファイルを削除
# （common/sec_data/raw/は2026-08-05にデッドコード除去のため廃止済み
#   [[SECDATA-STORAGE-FRAGMENTATION-1]]、削除対象から除外）
rm -rf common/sec_data/data/[TICKER]
rm -f common/sec_data/normalized/[TICKER]_quarterly_normalized.json
rm -f common/sec_data/ttm/[TICKER]_ttm_series.json
rm -rf docs/value-monitor/tanuki_valuation/data/[TICKER]
rm -f docs/value-monitor/hypecore/data/[TICKER]_poc.json
rm -rf docs/value-monitor/adjusted_eps_analyzer/data/[TICKER]

# 2026-09-02追加: common/market_data/ レイヤー（[[MARKETDATA-LAYER-
# CONSTRUCTION-1]]で新設、既存手順の対象外だったため見落とされていた）
rm -f common/market_data/daily/[TICKER].json
rm -f common/market_data/attributes/[TICKER].json
rm -f common/market_data/analyst_history/[TICKER].json
rm -rf common/market_data/[TICKER]

# 2026-09-02追加: docs/側の正規化データミラー
rm -f docs/common/sec_data/normalized/[TICKER]_quarterly_normalized.json

# Step 4: 健全性チェック・登録系チェッカーで不整合がないことを確認
# （削除直後はdocs/value-monitor/tanuki_valuation/data/tickers.json等の
# 派生ファイルが未更新のため、当該ティッカーの「欠損」警告が一時的に出る
# ことがある。Step 5の全銘柄再生成後に再実行して解消することを確認する）
python common/system_health.py
python common/sec_data/audit.py
python common/sec_data/report_consistency_check.py --fail-on-ng

# Step 5: 全銘柄を再生成し、tickers.json等の派生ファイルを整合させる
cd src/value/tanuki_valuation && python pipeline.py && cd -

# Step 6: 削除したティッカーに依存するテストがないか確認
pytest tests/ -q

# Step 7: コミット
git add -A
git commit -m "chore: [TICKER] 銘柄削除"
git pull --rebase origin kaihatsu
git push origin kaihatsu
```

---

## 作業完了時のチェックリスト

- [ ] pytest 全件パス
- [ ] 単体テストで動作確認
- [ ] 全銘柄再生成で成功率確認
- [ ] **`python common/sec_data/report_consistency_check.py` を実行し NG=0 を確認**
  - 現行チェック項目（CHECK-1〜22）:
    CHECK-1:FCF符号矛盾 / CHECK-2:DCF_Reliability欠落 / CHECK-3:LOW丸め / CHECK-4:割引率2段 /
    CHECK-5:NetDebt旧表示 / CHECK-6:負PER / CHECK-7:RPO条件 / CHECK-8:Matrix④高FCFラベル赤字 /
    CHECK-9:セグメント鮮度 / CHECK-10:PS異常値 / CHECK-11:Revenue孤立年 / CHECK-12:Cash-STI期ズレ /
    CHECK-13:RICE負値ラベル / CHECK-14:EPS>株価50% / CHECK-15:EPS>株価 / CHECK-16:TTM四半期不足 /
    CHECK-17:EPS全値$0 / CHECK-18:G=15%未調整（recommended_g算出不可でfcf_cagr floor
    に落ちるMO型ケースは検知対象外。詳細はBACKLOG [GROWTH-FLOOR-VERDICT-1] 参照）/
    CHECK-19:SEC株数=0 / CHECK-20:fcf_cagr floor値張り付き（[GROWTH-FLOOR-VERDICT-1]） /
    CHECK-21:Revenue段差型急変（QUALITY-GATES-EPIC-1 Phase 2b-2） /
    CHECK-22:fyキー競合（[FY52WEEK-BUCKET-MISPLACE-1]根本修正で新設。
    `reportDate==end_date`本人データ同士でfyタグが競合した銘柄をWARNで検知）
  - **新種バグを修正したら同スクリプトに検出項目を追加して恒久化する**
- [ ] HTMLファイルを新規作成・移設・削除した場合は `python ~/check_links.py` でリンク切れ0件を確認
- [ ] **新規計算フィールドを追加した場合**: report_consistency_check.pyに対応CHECKを追加（追加できない場合はBACKLOGにCHECK-COVERAGE-Nとして登録）
- [ ] **新規フィールド・指標を追加した場合**: 同一指標を表示する全画面をgrepで確認し全画面への反映を確認してから完了宣言する
- [ ] **機能を廃止した場合**: 全HTML・全Pythonで残骸をgrepで確認する
- [ ] **既存の計算式・アルゴリズムを変更した場合（乗算項の廃止・係数変更等）**:
  その式を「本体（core_calculator.py等）とは独立に再実装・再表示・再検証している」
  箇所が他にないかgrepで横断確認する（変更後の式そのものではなく、変更前の
  定数・変数名でgrepすると見つけやすい）。対象になりやすいのは検証ロジック
  （validator.py等）・レポート生成のテキスト組み立て（pipeline.pyのreport.txt
  生成部等）・回帰テスト（tests/）の3種。ALPHA-REDESIGN-1（P_t算出からalpha
  乗算を廃止）はcore_calculator.py本体こそ正しく修正されたが、上記3種のうち
  検証ロジック（[[VALIDATOR-IVPS-MISMATCH-1]]）・レポート表示
  （[[REPORT-ALPHA-STALE-1]]）・回帰テスト（[[TEST-STALE-IV-1]]）の
  いずれにも追随できておらず、3週間近く個別のバグとして気づかれずに
  残っていた（2026-07-15判明）。
- [ ] **複数銘柄への適用が必要な処理**: 全対象銘柄への実行完了を確認する
- [ ] BACKLOG.mdから該当項目を削除し、BACKLOG_DONE.mdに完了記録を移動
- [ ] コミット・プッシュ完了

---

## 月次メンテナンスタスク（月初の作業開始時に実施）

### フロントエンド表示内容の最新性確認

以下を確認し、実態と乖離している箇所を修正する：

**① 各画面のタイトル・サブタイトル・説明文**
- 機能追加後に説明文が古いままになっていないか
- 廃止した機能の説明が残っていないか

**② ツールチップ・凡例・ラベル**
- スコアリング基準やフェーズ定義の変更がUIに反映されているか
- 単位・計算式の説明が実装と一致しているか

**③ CLAUDE_CODE_START.md 自体の内容**
- よく使うコマンドが現在の構成と一致しているか
- 登録銘柄数・ファイルパス等の記載が最新か
- 新規銘柄登録手順・削除手順のステップが実態と一致しているか
- 手順を実際に実施した際に漏れ・誤りがあれば即座に手順書を更新する
  （気づいた時点で更新・次回以降に先送りしない）
- BACKLOG優先順位の目安が BACKLOG.md の実態と一致しているか
  （完了済み項目が残っていないか）

**④ SYSTEM_MAP.md の更新確認**
以下のいずれかに該当する作業を行った場合は必ずSYSTEM_MAP.mdを更新する：
- 新規ファイル・モジュールを追加した
- 既存ファイルの役割・出力先が変わった
- システム間の依存関係が変わった
- 新規銘柄登録でパイプライン対象が増えた

月次メンテナンス時にも全体を通読して陳腐化がないか確認する。

**⑤ 横断整合性チェック（PREVENT-5）**
以下を実行して不整合を検出する：
- cik_lookup.csv vs 全config（segment/maturity/beta）の銘柄整合性確認
- glossary.jsonのdata-info属性カバレッジ確認（HTML未使用キーがないか）
- console.log残存チェック（本番コードに残っていないか）
- system_health.py の実行（全チェックがHEALTHYか確認）
整合性問題が見つかった場合はその場でBACKLOGに登録する。

確認後、修正があればコミット：
```bash
git add docs/
git commit -m "docs: 月次フロントエンド表示内容の最新化"
git pull --rebase origin kaihatsu
git push origin kaihatsu
```

---

## BACKLOG管理ルール

### BACKLOGファイルの場所
- アクティブな課題: BACKLOG.md（TANUKI VALUATION系+システム全体を統合）
- 完了済みアーカイブ: BACKLOG_DONE.md
- Step 1 で読むのは BACKLOG.md のみ。BACKLOG_DONE.md は
  過去の実装経緯を調べる必要があるときだけ参照する
- 編集前に必ず grep で行を特定してから変更する（行番号の直接指定は禁止）

### BACKLOG更新のタイミング
- タスク完了後、メモリではなくファイルに記録する
- 完了時の手順:
  ① BACKLOG.md から該当項目を削除
  ② BACKLOG_DONE.md の該当日付セクション（なければ新設・新しい日付が上）に
     `✅ [XX-N] タスク名（YYYY-MM-DD 完了）` として移動
  ③ 実装内容を箇条書きで3行以内に要約して残す
- 新規課題の追加は BACKLOG.md の該当優先度セクションへ

### コミットルール（BACKLOG更新時）
git add BACKLOG.md BACKLOG_DONE.md
git commit -m "docs: [タスクID] 完了済みに更新"
git pull --rebase origin kaihatsu
git push origin kaihatsu

---

## Market Pulse プロンプト修正時の注意

対象ファイル: src/market/market_pulse/collect_and_send.py

修正時に必ず確認すること：
- 出来高比はS&P500/NASDAQを個別表記（まとめ表現禁止）
- 債券バッジは「債券売り/債券買い」（「リスクオン/オフ」は禁止）
- HYG・LQD同時下落は「信用収縮」禁止→「金利上昇圧力」に限定
- 乖離Zスコアの符号：正=NASDAQ優位 / 負=S&P500優位

修正後は index.html のバッジ表示との整合性も確認すること。

---

## Market Pulse CSV列追加時の注意（MP-HISTORY-FIX / MP-PRED-FIX の教訓）

対象: collect_and_send.py に新しい指標列を追加するとき

### 必須確認手順

**列追加後は必ず以下を実行すること：**

```bash
# 1. ヘッダー列数と最新行の列数が一致しているか確認
python3 -c "
import csv
with open('docs/market-monitor/market-pulse/data/market_data.csv') as f:
    reader = csv.reader(f)
    header = next(reader)
    rows = list(reader)
    print(f'ヘッダー列数: {len(header)}')
    for row in rows[-3:]:
        print(f'データ列数: {len(row)}  (日付: {row[0]})')
"

# 2. 主要フィールドの値域チェック
python3 -c "
import json
with open('docs/market-monitor/market-pulse/data/market_data.json') as f:
    data = json.load(f)
for entry in data[-5:]:
    ind = entry.get('indicators', {})
    sp = (ind.get('S&P500') or {}).get('value', '?')
    score = (entry.get('sentiment') or {}).get('score', '?')
    print(f\"{entry['date'][:10]}: S&P500={sp}, score={score}\")
"
```

**正常値の目安：**
- `S&P500.value`: 3000〜15000 の範囲（0.08 等の小数は列ズレ）
- `sentiment.score`: 0〜100 の範囲（負値・100超は異常）
- `sentiment.label`: EXTREME FEAR / FEAR / CAUTION / NEUTRAL / GREED / EXTREME GREED のいずれか

### CSV列追加後の必須ゲートチェック

**列追加・collect_and_send.py 実行後に必ず実行すること：**

```python
python3 -c "
import csv
with open('docs/market-monitor/market-pulse/data/market_data.csv') as f:
    reader = csv.reader(f)
    header = next(reader)
    rows = list(reader)
    header_len = len(header)
    errors = []
    for i, row in enumerate(rows):
        if len(row) != header_len:
            errors.append(f'行{i+2}: {len(row)}列 (ヘッダー{header_len}列)')
    if errors:
        print('❌ 列数不一致:')
        for e in errors: print(' ', e)
    else:
        print(f'✅ 全{len(rows)}行 列数一致（{header_len}列）')
"
```

✅ が出ること。❌ が出た場合は以下の対処へ。

### 列ズレが発生した場合の対処
1. CSV の旧行（列追加前）と新ヘッダーの列数差を確認
2. ずれた列数分だけオフセットした正しいフィールドを特定
3. market_data.json の異常期間エントリを CSV 生データから再構築
4. index.html 側の計算ロジックに防衛チェックを追加

---

## サブシステム削除時の必須手順

`[[DISCOVER-SUBSYSTEM-REMOVAL-1]]`（2026-09-01実装）で、削除対象自身の
ディレクトリ（`docs/discover/`等）や個別ページの参照は洗い出せていた
一方、**サイトトップページ（`docs/index.html`）のカード導線**・
**`docs/common/site-header.js`のようなサイト全体で共有される共通
コンポーネント内の設定オブジェクト（`TOOL_META`のキー等）**の2箇所を
確認漏れし、5日後にKoichiさんの目視指摘で発覚した
（`[[DISCOVER-RESIDUAL-LINKS-1]]`、2026-09-06クローズ）。

原因は「削除対象の正式名称・パス（`discover`ディレクトリ名等）」だけを
手がかりに`grep`していたため機械的には見つかる状態だったにも
関わらず、削除作業のチェックリストに「サイトトップページ・共通ヘッダー
コンポーネントを確認する」という**能動的な確認項目**が存在せず、
洗い出し自体を実施していなかったこと。銘柄削除時の必須手順
（上記）がAVGO削除時に学んだのと同じ教訓——固定ファイルリストを
機械的になぞるだけでは新しいレイヤーの追加に追従できない——が、
サブシステム削除でも同様に成り立つ。

### 削除手順

```bash
# Step 0: 削除前の全参照洗い出し（サブシステムの正式名・短縮キー
# 両方で検索すること。例: Discoverなら"discover"だけでなく、
# 個別ページ内で使われる短縮タグ・クラス名も対象に含める）
grep -rln "[SUBSYSTEM_NAME]" . --include="*.py" --include="*.html" \
  --include="*.js" --include="*.yml" --include="*.yaml" --include="*.json" \
  --include="*.md" 2>/dev/null

# Step 1: 必ず個別に目視確認する箇所（銘柄削除時のcik_lookup.csvに相当、
# サブシステム削除では「サイト全体の導線・共通コンポーネント」が
# この位置づけになる）
# - docs/index.html（サイトトップページ）のカード（<a href="...">〜</a>
#   ブロック全体）・対応するCSS（.card-{subsystem}系）
# - docs/common/site-header.js のTOOL_META等、キー付き設定オブジェクト
# - docs/common/site-nav.js 等、他の共通ナビゲーションコンポーネント
#   （site-header.js以外にも存在する場合は同様に確認）
# - 他サブシステムのキャッチコピー・ソースパス名としての偶然の同名
#   一致（例: STONKS SILOの"Discover · 10x Candidates"というタグ、
#   discover/stonks-silo/というソースパス）は削除対象と誤認しないこと

# Step 2: リンク整合性チェックを必ず実行（下記セクション参照）
python ~/check_links.py
```

`docs/index.html`・共通ヘッダーコンポーネントは全ページから参照される
「削除対象自身のディレクトリ配下には存在しない」参照元のため、
Step 0のgrep結果に必ず含まれるにも関わらず見落としやすい。次にサブ
システムを削除する際は、このStep 1リストを機械的になぞるだけで済ませず、
Step 0のgrep結果を1件ずつ確認すること。

---

## リンク整合性チェック（HTMLファイルを新規作成・移設・削除した場合は必須）

```bash
python ~/check_links.py
```

リンク切れが0件であることを確認してからコミットすること。
スクリプトが存在しない場合は以下で再作成：

```python
# ~/check_links.py
import os, re
from pathlib import Path

DOCS_ROOT = Path("docs")
html_files = sorted(DOCS_ROOT.rglob("*.html"))

PATTERNS = [
    r'href=["\']([^"\'#?]+)["\']',
    r"fetch\(['\"]([^'\"?#]+)['\"]",
    r"src=['\"]([^'\"?#]+)['\"]",
]

errors = []

for html_path in html_files:
    base_dir = html_path.parent
    content = html_path.read_text(encoding="utf-8", errors="ignore")
    for pat in PATTERNS:
        for match in re.finditer(pat, content):
            raw = match.group(1).strip()
            if raw.startswith(("http", "//", "data:", "mailto:", "#", "javascript")) or not raw:
                continue
            if raw.startswith("/"):
                target = DOCS_ROOT / raw.lstrip("/")
            else:
                target = (base_dir / raw).resolve()
                try:
                    target.relative_to(Path("docs").resolve())
                except ValueError:
                    errors.append(f"[OUT-OF-DOCS] {html_path} → {raw}")
                    continue
            if not target.exists():
                errors.append(f"[DEAD] {html_path} → {raw}  (resolved: {target})")

print(f"=== チェック対象: {len(html_files)} ファイル ===\n")
if errors:
    for e in errors: print(e)
    print(f"\n合計 {len(errors)} 件のリンク切れ")
else:
    print("リンク切れなし ✅")
```

---

## 新規HTMLページ作成時の必須チェックリスト

### ① リンク切れチェック（HTMLファイル作成・移設・削除後は必須）

```bash
python ~/check_links.py
```

リンク切れ0件を確認してからコミットすること。

### ② site-nav.js への登録（新規ページ作成時は必須）

`docs/common/site-nav.js` の `ITEMS` 配列に新ページのエントリを追加：

```js
{ key: 'xxx', label: 'PAGE NAME', href: BASE + '/path/to/page/' }
```

新規HTMLの `<body>` タグに `data-tool="xxx"` を設定すること（key と完全一致）。
これを忘れるとナビが正しく生成されず、activeハイライトも当たらない。

**確認コマンド：**

```bash
grep -n "data-tool" docs/path/to/new/index.html
grep -n "key:.*'xxx'" docs/common/site-nav.js
```

### ③ ナビのactiveハイライト確認（新規ページ作成時は必須）

```bash
python -m http.server 8767 --directory docs
```

ブラウザで新規ページを開いてナビの該当項目がハイライトされていることを目視確認すること。

### ④ 共通デザイン適用（新規ページ作成時は必須）

`docs/common/site-header.js` の `TOOL_META` に新ページのツール定義を追加：

```js
xxx: { title: 'PAGE TITLE', subtitle: 'サブタイトル' }
```

`docs/common/site-theme.css` に `body[data-tool="xxx"] { --acc: #xxxxxx; }` を追加。

これを忘れると、ナビには登録されてもヘッダーが未適用のまま
（ロゴ・タイトル・アクセントカラーが統一されない）になる。

**確認コマンド：**

```bash
grep -n "xxx:" docs/common/site-header.js
grep -n "data-tool=\"xxx\"" docs/common/site-theme.css
```

（2026-07-01 EXTREME-FEAR-1対応時の実施パターンより追記）

---

## ファイル削除・上書き前の必須確認（重要）

### HTMLファイルを削除・新規作成・上書きする前に必ず実行すること

1. 削除・上書き対象ファイルの行数と主要セクションを確認

```bash
wc -l <対象ファイル>
grep -n "<section\|<div id\|<h2" <対象ファイル>
```

2. 「旧ページ」「不要」と判断する前に git log で履歴を確認

```bash
git log --oneline -- <対象ファイル>
```

3. 新規HTMLを作成する場合、同じ役割のページが既存していないか確認

```bash
find docs/ -name "*.html" | xargs grep -l "<キーワード>" 2>/dev/null
```

4. 上記確認結果をレポートしてから削除・作成を実行すること。
   **確認なしの削除・上書きは禁止。**
