# Claude Code 作業の手順・決まり

`CLAUDE_CODE_START.md` から手順・決まりの部分を移した（2026-11-06、使用量を減らす進め方の整備）。以前の「CLAUDE_CODE_START.md「◯◯」」という参照は、
**このファイルの同名の節**を指す。現在の状態と未済の確認手順は `CLAUDE_CODE_START.md`、日付つきの履歴は `docs/history/CLAUDE_CODE_START_HISTORY.md`。

**プロジェクトの運用**: On-a-journeyは長期投資の仮説構築・検証を支援する
システム群（TANUKI VALUATION／TANUKI TAIL／STONKS SILO／MACRO PULSE／
Market Pulse等）。毎セッション開始時は本ファイル（直近セッションの
サマリーを新しい順に蓄積）→`BACKLOG.md`（優先度別の未着手課題）の順で
読んで着手する。完了した課題は要約せず全文を`BACKLOG_DONE.md`へ移設し、
セッション終了時には毎回「ブラッシュアップ」（本ファイル・
`BACKLOG.md`/`BACKLOG_DONE.md`間の移設漏れ・ID重複チェック・
`PROJECT_STATUS.md`更新）を実施する。詳細な運用ルール・過去の失敗事例は
`CHAT_RULES.md`に蓄積されている。

**STEP 0（作業開始時の`git fetch`）の運用ルール（2026-10-03 チャット側の指示で例外を追加）**: kaihatsuに取り込まれていない差分が次だけで、
`git merge --ff-only`で追いつける場合は続行してよい。報告には件数とcommitの一覧（ハッシュ・件名）を書く。これ以外のcommitが1件でも含まれる場合は停止する。
1. botの自動データ更新（作者`github-actions[bot]`）
2. MACRO PULSE側セッションのcommit: メッセージに`M-<番号>`（例: `M-2`）を含むもの（実際の件名は「…（指示書M-2 STEP 4）」「…（M-2レビュー対応）」のように末尾に付く）
3. 件名が`Merge origin/kaihatsu into feature/macro-pulse-`で始まるマージcommitで、それが取り込むcommitがすべて1・2のどちらかか、既にkaihatsuにあるもの

## 毎回の作業開始時に必ず実行すること

- モデルは依頼文の見出しの【Opus】【Sonnet】に合わせて`/model`で切り替える（決まりは`CHAT_RULES.md`「モデルの使い分け」。付いていない依頼はOpus）


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
     未検証のまま残した。次回、`common/sec_data/reader.py`経由への
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
python -m pytest tests/ src/subport/day_trade/test_logic.py -v
全件パスを確認してから作業を開始する。
件数を記録するときは、実行したコマンドも書く（例:「`pytest tests/ src/subport/day_trade/test_logic.py` 2014件全パス」）。
2026-10-08: ゲートの対象に`src/subport/day_trade/test_logic.py`（22件）を加えた。`pytest tests/`（1992件）と対象を指定しない
ルートでの実行（2014件）の件数の違いが、テストの減少と取り違えられたため（tests/の件数は変わっていなかった）。
pytestの件数はテストの追加と、`tests/test_iv_formula.py`がティッカーの登録状況（`get_tanuki_tickers()`）でパラメータ化されているため変動する。
固定値として扱わない（例: 10-08の2014件→10-09の2028件は、TAILのテスト17件の追加とCON・SN・WSTの登録解除による3件の減少）。
失敗があれば先に修正する。
既知の例外は無い（以前の例外だった tests/test_iv_formula.py の MSFT/NVDA 2件〈[[TEST-STALE-IV-1]]、
ALPHA-REDESIGN-1後にテスト式が未更新〉は2026-08-20に解消済み。2026-10-09に記述を更新）。
1件でも失敗したら先に修正する
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
- 新規フィールド追加時は`NAMING_CONVENTIONS.md`を参照する。適用要否は統一後のデータソース同一性を踏まえ都度判断する（既存フィールドの一括リネームは行わない、2026-09-19追加・[[NAMING-CONVENTIONS-APPLY-1]]完了・BACKLOG_DONE.md参照）

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

**① 設定の手順の追加**
- 「管理の操作の手順」の表に、新属性の設定ファイルと編集の手順を足す
  （管理のページは2026-10-21に削除。編集の画面は作らない）
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
- 実装後に必ず pytest を実行する（`python -m pytest tests/ src/subport/day_trade/test_logic.py`、Step 2と同じ対象。件数を記録するときは実行したコマンドも書く）
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

### 外部API（Grok・SEC・FRED）の呼び出しのルール（2026-10-24、[[EXTERNAL-API-CLIENT-UNIFY-1]]）

- 外部APIの通信は `common/external_api.py` の1か所を通す。宛先・タイムアウト・再試行（429・5xx・通信の失敗は2秒→5秒待って計3回）・失敗の形（`ExternalApiError`）・ログはここで決まる。プロンプト・モデル・温度・応答の読み方は呼び出し側に書く
  - Grok: `grok_request(payload, label)`（応答のJSON）・`grok_text(messages, model, label, **引数)`（答えの文字）。トークンの使用量はここでログに出る。Grokが使えないときに画面・データに出す文は `GROK_UNAVAILABLE_TEXT` の1つ
  - SEC: `sec_get(url, label)`。User-Agent は `On-a-journey <SEC_USER_AGENT_EMAIL>`、毎秒10件の制限もここで守る。呼び出し側で User-Agent・sleep を書かない
  - FRED: `fred_get(path, params)`（REST）・`fred_client()`（fredapi の Fred）
- AI APIはxAI（`XAI_API_KEY`）に統一。モデルは grok-4.3（[[GROK-MODEL-PRICE-1]]）。EPSの調整項目だけ `XAI_MODEL`（既定 grok-4.20-0309-reasoning）
- GeminiやOpenAI等の別APIを使用しているコードを発見した場合はGrokに移行すること
- 一覧の規則（grok_endpoint・grok_call・fred_api）が `common/external_api.py` 以外に当たったら、CHECK-INV-3 で二重として出る

### SECの連絡先（SEC_USER_AGENT_EMAIL）と手元で動かすとき

- SECは連絡先のメールを User-Agent に求める。値は GitHub の secret `SEC_USER_AGENT_EMAIL` にだけ置き、コードに書かない（実在のアドレスをコードに直書きしない）
- SECを呼ぶワークフロー7本（SEC_Data_Update・Adjusted_Eps_Analyzer_update・TANUKI_TAIL_KPI_Update・TANUKI_TAIL_RSS_Monitor・TANUKI_TAIL_SEC_Ctrl・TANUKI_TAIL_SEC_Items・TANUKI_VALUATION_Update）は、先頭の `env:` で `SEC_USER_AGENT_EMAIL: ${{ secrets.SEC_USER_AGENT_EMAIL }}` を渡す。SECを呼ぶワークフローを足したら同じ行を足す
- 環境変数が無いと `sec_get` は `SystemExit` で止まる（架空のアドレスで続けない。`except Exception` では受けられない）
- 手元でSECを呼ぶスクリプト（`common/sec_data/update.py`・登録の事前確認・TAIL・EPS・TANUKIのインサイダー取引）を動かすときは、その回だけ環境変数を付ける:
  ```bash
  SEC_USER_AGENT_EMAIL='<連絡先のメール>' python common/sec_data/update.py APP
  ```
  シェルの設定ファイル・リポジトリの中のファイルには書かない。pytest は `tests/conftest.py` が試験用の値（`tests@example.invalid`）を入れ、本物のSECは呼ばない

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

### SEC自動更新とTANUKI VALUATION自動更新の生成順序ズレ（WARN12-COHR-ONDS-1の教訓・2026-07-13追加、2026-09-30更新）

**起動時刻・連鎖は `docs/architecture/UPDATE_SCHEDULE.md` を参照**（SEC Data Updateの完了でHypeCore・Adjusted EPS・
Stonks Siloが動き、その完了でTANUKI VALUATIONが動く）。2026-07-13時点の独立cron運用で起きていた「日曜のSEC自動更新から
次のTANUKI VALUATIONまで約26時間、latest.json/report.txtが陳腐化する」構造は、2026-08-22の連鎖化で解消した
（経緯は同文書の変更履歴、[[WORKFLOW-SEC-TANUKI-GAP-1]]・[[TANUKI-VALUATION-PRICE-SCHEDULE-LAG-1]]）。

`report_consistency_check.py`の`WARN-12`（Cash-STI期ズレ）等が新規発生した場合は、まずfact競合等のコードバグを疑う前に、
`latest.json`の`calculation_date`と対象銘柄の`common/sec_data/data/{TICKER}/quarterly_*.json`のコミット日時を比較し、
**単なる生成順序のズレでないか**を確認すること（連鎖が失敗・スキップした週に起こりうる。該当すれば`pipeline.py [TICKER]`の
再実行のみで解消する。実例: 2026-07-12にCOHR/ONDSで発生、コード修正不要だった）。

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
`BACKLOG_PRIORITY_ROADMAP.md`に記録した。**2026-09-23時点で181件は12件へ
大幅減少しており、同ファイルは陳腐化しているため参照しないこと**（大幅な
BACKLOG変動があったが同ファイルは未更新のまま）。

2026-09-23セッション終了時ブラッシュアップで11件クローズ
（CART-QUARTERLY-REVENUE-EXTRACTION-GAP-1・SCHEMA-NORMALIZED-ISSUES-1・
HYPECORE-EXPECTATION-FRAMEWORK-EPIC-1・LRCX-CAT-DELL-STI-TAG-CANDIDATE-
GAP-1・TAIL-LAYER3-FORMULA-YOY-UNSUPPORTED-1・TAIL-SATELLITE-MONITOR-
CORE-APPLICABILITY-1・CON-SM-MISSING-RICE-OVERSTATEMENT-1・
ANOMALY-PATTERN-CATALOG-1・XBRL-UNIT-SCALE-MISMATCH-DETECTION-1・
MARKETPULSE-MINOR-INCONSISTENCIES-1・LAYER3-SM-SGA-SEPARATION-NONE-
FALLOUT-1）・新規1件登録（LAYER3-MOAT-ROIC-4TICKERS-NONE-1）を経て、
BACKLOG.mdのアクティブ項目は12件（`grep -c "^### \["`で確認）。下記
「順次着手」「着手条件あり」を全面更新した）

### 順次着手（技術判断のみで着手可能、優先順）
- ~~LAYER3-MOAT-ROIC-4TICKERS-NONE-1~~（2026-09-24陳腐化クローズ済み。
  年次OIが4銘柄とも存在し実測値で算出済みと判明、BACKLOG_DONE.md参照）
- ~~DERIVED-DATA-SUBCATEGORIES-CROSSTAB-STALE-1~~（2026-09-24完了、
  BACKLOG_DONE.md参照）
- SEGMENT-XBRL-GROWTH-EXPANSION-CANDIDATES-1（IDEAS_AND_WATCH.md へ移動）の②のみ（優先度低。
  segment_xbrl拡張候補3件のうち「company_facts自動列挙可否」の調査部分
  だけは、着手前の価値検証として独立して実施可能。①③本体はKoichiさん
  判断待ち）

### 着手条件あり（Koichiさんの判断・外部トリガー待ち）
- ~~FLAG-THRESHOLD-DESIGN-1~~（2026-09-25完了。stonks_siloに案C〈TTM営業利益<0
  または TTM売上=0〉を採用しCHECK-51・登録フローP7で機械判定、フラグ6件是正。
  BACKLOG_DONE.md参照）
- ~~BBAI-RDW-RUNWAY-VERIFICATION-1~~（2026-09-25完了。一次情報で両銘柄とも
  実態SAFEと確認し、Runway cashを四半期優先＋ST投資込みの共通関数に統一。
  BACKLOG_DONE.md参照）
- SEGMENT-XBRL-GROWTH-EXPANSION-CANDIDATES-1（IDEAS_AND_WATCH.md へ移動）（①③本体）: Koichiさんが
  優先順位・着手タイミングを判断
- ~~HYPECORE-POC-SYNTHESIS-FIELDS-NOT-IN-REPORT-1~~（2026-09-24、親エピック
  完了・report.txt消費側に必要なしと確認しクローズ）
- FUTURE-FEATURE-IDEAS-CATALOG-1（IDEAS_AND_WATCH.md へ移動）: 構想段階の8件、個別項目ごとに
  Koichiさんが着手可否を判断
- ~~UNCONFIRMED-RISK-INVESTIGATION-CATALOG-1~~（2026-09-25解体: ①は仮説の型が
  実在せずクローズ、②はSPLIT-REALTIME-GAP-REVERSE-1として完了、③は
  DATA-JUMP-CHECK-NETINCOME-SBC-1として単独項目に戻した（IDEAS_AND_WATCH.md へ移動）。新規登録:
  PARSER-MERGED-PARTIAL-CONCEPT-TAG-1・SPLIT-HISTORY-REGISTRATION-GAP-DETECT-1。
  以下は旧記載）: ①〜③いずれも実データ未確認の
  推測段階、次回セッション以降でKoichiさんが判断
- JNJ-XOM-PM-FLOOR-RISK-1・TTM-DATA-DRIFT-BEHIND-PIPELINE-1・
  LAYER3-GA-STANDALONE-TAG-UNMAPPED-1（IDEAS_AND_WATCH.md へ移動）: いずれも特定トリガー条件（候補件数低下・実害顕在化・
  新機能での実消費計画等）が発生するまで監視・保留（詳細は各エントリ
  「着手条件」参照。LAYER3-ANNUAL-CLASSIFICATION-DROPS-DATA-1は2026-09-24
  クローズ。TTM-DATA-DRIFTは2026-09-24にCHECK-47発火で着手条件3が成立）

---

## 計算方法の変更の手順（2026-11-02、`[[METHOD-CHANGE-PUBLISH-TIMING-1]]`、実装 `2b42795069`）

計算方法（理論株価・判定・funda・stage・Layer3・TTMの作り方）を変えるcommitは、次の順でする（チャット側の決定、2026-11-02）。

1. **全銘柄のシミュレーション（株価・市場データを固定した前後）とscope**: 前のコードで手元のTANUKI VALUATION（Grok・SECのインサイダー取引は呼ばない）・HypeCore・Stonks・ROTCEのうち変更が届くものを回し
   `python scripts/method_change_scope.py snapshot <前>`、新しいコードで同じように回し`snapshot <後>`、
   `python scripts/method_change_scope.py diff --id <BACKLOGの項目ID> --before <前> --after <後> --write`で`config/method_change.json`に
   `{"id", "committed_at", "scope": [{"ticker", "fields", "before", "after"}]}`を足す。**scopeは手で書かない**。公開のデータは`git checkout`で戻す。
   同じ項目で2回目の変更を出すときは、IDの末尾に番号を足す（例: `FCF-DEFINITION-SINGLE-SOURCE-1-2`）
2. **公開の時点を1つに**: 影響する派生ファイル（`common/sec_data/ttm/`・`normalized/`等、日曜のSEC_Data_Updateが作り直すもの）を、
   計算方法の変更のcommitと同時に作り直してcommitする。「その場で作る」経路（Layer3を`build_ticker_store()`で毎回作る読み手）と
   「日曜に作り直す」経路（TTM・normalizedを読む読み手）が、次の定時実行で同時に出る（CHAT_RULES.md 事例39・「計算方法の変更の公開の手順」）
3. **読み合い**: 理論株価が動く変更を出すときは、pushの後に`gh workflow run`で TANUKI VALUATION → HypeCore → TANUKI VALUATION を1回まわす
   （`gh workflow run TANUKI_VALUATION_Update.yml --ref kaihatsu`→終わったら`HypeCore_Update.yml`→終わったら`TANUKI_VALUATION_Update.yml`。
   HypeCoreのstageは前回のTANUKIの理論株価を読み、TANUKIのtimingはHypeCoreのstageを読むため。ワークフローの連鎖の常設の変更はしない）
4. **印**: 各出力（TANUKIのlatest.json・history/・history.json・score_history.json・hypecore_history/、HypeCoreの{T}_poc.json、Stonksのresults.json、ROTCE）は、
   前回の出力の`method_change_applied`と比べ、新しいIDを初めて読んだ実行でだけscopeの銘柄に`method_change`を付ける（日付では決めない）。
   Discord（`score_watcher.py`）は、印のある銘柄の変化のうちscopeの項目に当たるものだけを「🛠 計算方法の変更による変化」に入れ、
   「計算方法の分（scopeのbefore→after）」と「残り（相場等）」に分けて出す。古い形`{"id", "date"}`の件（〜2026-11-01）は記録として残す（その日の全銘柄）
5. 確認: 最初の実行の後に、🛠に入った銘柄がscopeの判定・funda・乖離率・stageの変わる銘柄と一致すること、次の実行で印が付かないこと

## 管理の操作の手順（2026-10-21、管理のページの削除の後）

2026-10-21に管理のページ2つ（`docs/value-monitor/admin.html`・`docs/value-monitor/adjusted_eps_analyzer/admin.html`）を消した
（Koichiさんの判断、R-A EPSの段階2）。Koichiさんは「〇〇を登録して」「〇〇のβを変えて」と頼むだけにし、Claude Codeが下の手順でする。

- 設定ファイルを直接編集する操作も、ゲート3種（pytest・audit.py・report_consistency_check.py --fail-on-ng）を通してからcommit・pushする
- ワークフローの起動は`gh workflow run <yml> --ref kaihatsu -f <入力>=<値>`。結果は`gh run list --workflow <yml> -L 1`・`gh run watch <id>`で確かめる。
  入力の名前はワークフローごとに違う（EPSだけ`ticker`、ほかは`tickers`。UPDATE_SCHEDULE.mdの1章「手動の入力」）
- 設定を変えた後は、その設定を読むワークフローを起動して結果を確かめる（多くはTANUKI_VALUATION_Update.yml）

| 頼まれること | 前のページの操作 | Claude Codeがすること |
|---|---|---|
| 〇〇を登録して | 銘柄管理「新規銘柄登録」・EPSの管理「追加」 | 「新規銘柄登録時の必須手順」（Step 0・0.5の後に`common/registration/register_ticker.py`）。CIKはSECの`company_tickers.json`で確かめて`config/cik_lookup.csv`の新しい行に書く |
| 〇〇を監視から外して | 銘柄管理「選択銘柄を削除」 | `config/monitor_tickers.yaml`からその行を消す（単純リスト。yaml.dumpは使わず、コメントを残してテキストで消す）。前のページもこのファイルから消すだけで、`cik_lookup.csv`は変えなかった |
| 〇〇の対象のシステム・EPSセクターを変えて | 銘柄管理の行の編集・EPSの管理「EPSセクター手動設定」 | `config/cik_lookup.csv`のその行の列（`tanuki`・`eps`・`hypecore`・`stonks_silo`）を直接編集する。EPSの区分は業種のマスター（`config/sector_master.json`の`eps_category`）で決まり、手で変えない（`eps_sector`列は2026-11-11に廃止〈[[SECTOR-MASTER-ADOPT-1]] 段4〉。例外が要るときは理由つきで`config/sector_exceptions.json`へ） |
| 〇〇のβを変えて | β設定タブ | `config/beta_config.json`の`overrides.{T}`（`beta`・`sector`・`industry`・`reason`）を直接編集する（`config/BETA_POLICY.md`の手順）。月次の自動の取得は`Beta_Config_Update.yml`（`beta_fetcher.py`） |
| 〇〇のセグメント・成熟プロファイル・成長オプションを変えて | 銘柄設定タブ | `config/segment_config.json`・`config/maturity_config.json`・`config/growth_options_config.json`を直接編集する（セグメントは下の「新規銘柄のセグメント設定判断ルール」に従い10-Kから。根拠を`comment`に書く） |
| Phase2成長率のセクター別の上限を変えて | 銘柄設定タブ「上限テーブル」 | `config/maturity_config.json`のセクター別の上限の表を直接編集する |
| RPOのホワイトリストを変えて | RPO設定タブ | `config/rpo_config.json`のwhitelistを理由付きで直接編集する |
| 〇〇を休眠にして・休眠から戻して | Discover設定タブ（2026-10-23まで`config/discover_config.json`の区分） | `config/cik_lookup.csv`のその行の`status`を`dormant`（休眠）・`active`にする。`dormant`は人が見る印で、各システムには通したまま（外すのは`retired`・`provisioning`だけ）。`discover_config.json`は2026-10-23に削除した（[[HOLDINGS-SINGLE-SOURCE-1]]。保有はportfolio.json、メモは置き場が無い） |
| ポートフォリオを変えて | ポートフォリオタブ「保存」 | `docs/portfolio/data/portfolio.json`を直接編集する |
| 〇〇を更新して（全銘柄） | 実行タブ・一括実行 | `gh workflow run SEC_Data_Update.yml --ref kaihatsu`。完了で連鎖（HypeCore・EPS・Stonks → TANUKI VALUATION → TANUKI Score）が動く |
| 〇〇を更新して（一部の銘柄） | 銘柄管理「SECデータ取得」「選択銘柄を更新実行」・実行タブ・EPSの管理「選択した銘柄を更新」 | 必要なワークフローだけを銘柄を指定して起動する（例: `gh workflow run TANUKI_VALUATION_Update.yml --ref kaihatsu -f tickers=NVDA`）。SEC_Data_Updateを銘柄を指定して起動しても、連鎖の下流は全銘柄で動く |
| MACRO PULSE・Market Pulseを動かして | 実行タブ | `gh workflow run MACRO_PULSE_Update.yml --ref kaihatsu -f mode=<モード>`・`gh workflow run Market_Pulse_Update.yml --ref kaihatsu` |
| 状態を見せて | ステータスタブ | `docs/value-monitor/tanuki_valuation/data/tickers.json`・`{T}/latest.json`を読んで報告する |

**コードを書き換える操作**（2026-10-21、チャット側の判断で手順にした）:

- **〇〇のDamodaranの業種を変えて**（前のページのβ設定タブ）: `src/value/tanuki_valuation/growth_sanity.py`の`TICKER_INDUSTRY_OVERRIDES`の
  その銘柄の行を書き換える（無ければ足す、外すなら行を消す。行末のコメントに元の分類と理由）。業種名は`_load_damodaran()`が読む
  Damodaranの表（`fundgrEB.xls`の「Industry Averages」の Industry Name）にある名前と一字一句同じにする（無い名前は黙って無視され、
  ベンチマークが無くなる）。ゲート3種を通してcommitし、TANUKI_VALUATION_Update.ymlを起動して結果を確かめる
- **ポートフォリオのパスワードを変えて**: パスワードそのものはチャット・commit・ログに入れない。
  1. Koichiさんが手元の自分のターミナル（Claude Codeの`!`ではなく）で、ハッシュだけを作る:
     `python -c "import getpass,hashlib;print(hashlib.sha256(getpass.getpass('新しいパスワード: ').encode('utf-8')).hexdigest())"`
     （入力は画面に出ず、シェルの履歴にも残らない。画面のJSと同じく、UTF-8の文字列のSHA-256を小文字の16進で出す）
  2. Koichiさんは出た64文字のハッシュだけをClaude Codeに渡す
  3. Claude Codeは`docs/portfolio/index.html`の`const PW_HASH = "…";`の値をそのハッシュに書き換え（64文字の小文字の16進かを確かめる）、
     ゲート3種を通してcommitする。GitHub Pagesへの反映は数分かかる
  - 注記（2026-10-21、チャット側の確認。今は記録だけ）: このパスワードは画面を隠すだけで、保有銘柄の`docs/portfolio/data/portfolio.json`は
    公開のリポジトリにあり、誰でも読める

**ページと一緒に無くなった操作**: FCF転換率タブ（`config/fcf_conversion_config.json`は192a45116bで削除済み）、
登録の中の`kpi_config.py`への追記（0091fa09cbで削除済み）、銘柄設定タブ「AI案を生成」（`TANUKI_Segment_AI.yml`、Grokでセグメントを作る。
セグメントは10-Kから決める手順〈Step 3.5〉と合わない）、CIKの自動取得（`TANUKI_CIK_Lookup.yml`。CIKは登録の手順の中で確かめる）。
この2本のワークフローと出力の`config/cik_lookup_result.json`も2026-10-21に削除した（チャット側の判断。`TANUKI_CIK_Lookup.yml`は`cik_lookup.csv`の書き手の二重で、
statusの列の無い行を書いていた）

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

このスクリプトはStep 1（SEC取得）・Step 1.5（日足の過去分の取り込み。2026-10-26追加、[[MARKETDATA-DAILY-HISTORY-POLICY-1]]。
決まり「日足は2021-01-01か上場日の遅い方から」〈`docs/architecture/UPDATE_SCHEDULE.md`の「日足の履歴の取り方」〉により、
`python common/market_data/fetcher.py [TICKER] --backfill --start 2021-01-01`をその銘柄だけに実行する。非ブロッキングで、失敗したら登録の後に同じコマンドで取り直す。
銘柄を省くと全銘柄の過去分を取り直すので、手で実行するときも必ず銘柄を指定する）・Step 1.6（属性〈業種〉の1銘柄の取得と業種のマスター`config/sector_master.json`への追加。2026-11-05追加、[[HYPECORE-STAGE-TIMESCALE-1]] 段B-1。`python common/market_data/fetcher.py [TICKER] --layer attributes`→`python common/sector_master.py --ticker [TICKER]`、非ブロッキング）・Step 1.7（収益の段階の追加。2026-11-05追加、段B-2。`python common/profitability.py --ticker [TICKER]`、非ブロッキング）・Step 1.8（倍率の履歴の追加。2026-11-05追加、段B-4。`python common/multiples.py --ticker [TICKER]`、非ブロッキング）・Step 1.9（定義v2のstageの並行計算の追加。2026-11-06追加。`python common/stage_v2.py --ticker [TICKER]`、非ブロッキング）・Step 2（β取得）・Step 3（TANUKI
VALUATIONパイプライン実行）・Step 4（audit.py --check-beta）・Step 5
（HypeCore、hypecore=trueのみ）・Step 5b（EPS Analyzer、eps=trueのみ）・
Step 7（monitor_tickers.yaml追加。Step 6〈Discover登録〉は2026-10-23に削除）・Step 7.5
（report_consistency_check.pyを`--ticker <T> --include-provisioning
--include-yfinance-checks --fail-on-ng`で実行、NGまたは対象0件なら昇格を止める。
2026-09-24追加、[[REGISTRATION-VALIDATOR-P2A-PERIOD-MISMATCH-1]]）・Step 8
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
  `common/sec_data/data/{TICKER}/segment_review.json`（登録のときに作る。まだ無い銘柄もある）に
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
- Step 6（discover_config.json）は2026-10-23に削除した（[[HOLDINGS-SINGLE-SOURCE-1]]）
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
python -m pytest tests/ src/subport/day_trade/test_logic.py -v

### GitHub Actions 確認
`gh workflow run <yml> --ref kaihatsu`・`gh run list --workflow <yml> -L 1`（管理のページは2026-10-21に削除。「管理の操作の手順」）

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

# （discover_config.json は2026-10-23に削除した。[[HOLDINGS-SINGLE-SOURCE-1]]）

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
pytest tests/ src/subport/day_trade/test_logic.py -q

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
ディレクトリ（`docs/discover/`（`ec4590fd6e`〈2026-09-01〉で削除）等）や個別ページの参照は洗い出せていた
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
#   src/value/stonks_silo/というソースパス）は削除対象と誤認しないこと

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
python scripts/audit_site_links.py            # リンク切れ・孤立ページがあれば終了コード1
python scripts/audit_site_links.py --verbose  # 検証できなかったテンプレートのリンクも表示
```

リンク切れ0件・孤立ページ0件を確認してからコミットすること（2026-10-17 [[NAV-LINK-INTEGRITY-1]]で新設。
`site-nav.js`のITEMS・`ticker-links.js`の各ページへのリンク、`/On-a-journey/`の絶対パス、`${...}`のテンプレートのリンクのパス部分も検証する。
意図的にどこからもリンクしないページは、同スクリプトの`ORPHAN_ALLOWED`に理由付きで登録する）。
pytestの`tests/test_audit_site_links.py`も同じ判定をする（CIへの組込みは[[NAV-LINK-AUDIT-CI-1]]、未着手）。
以下の`~/check_links.py`は旧手順（fetchのパスも見るが、孤立ページ・ITEMSは見ない）。
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
python scripts/audit_site_links.py
```

リンク切れ0件・孤立ページ0件を確認してからコミットすること（上の「リンク整合性チェック」）。

### ② site-nav.js への登録（新規ページ作成時は必須）

`docs/common/site-nav.js` の `ITEMS` 配列に新ページのエントリを追加：

```js
{ key: 'xxx', label: 'PAGE NAME', href: BASE + '/path/to/page/' }
```

新規HTMLの `<body>` タグに `data-tool="xxx"` を設定すること（key と完全一致）。
これを忘れるとナビが正しく生成されず、activeハイライトも当たらない。

**確認コマンド：**

```bash
grep -n "data-tool" docs/{新しいページ}/index.html
grep -n "key:.*'xxx'" docs/common/site-nav.js
```

### ③ ナビのactiveハイライト確認（新規ページ作成時は必須）

```bash
python -m http.server 8767 --directory docs
```

ブラウザで新規ページを開いてナビの該当項目がハイライトされていることを目視確認すること。

**ローカルサーバーの止め方（CHAT_RULES.md 事例42、2026-10-17）:**
- バックグラウンドで起動したサーバーは、起動時にPIDを記録しておく（例: `python -m http.server 8767 --directory docs & echo $! > <スクラッチパッド>/server.pid`。Claude Codeのバックグラウンドのタスクとして起動した場合はそのタスクID）
- 止めるときは、記録したPID（`kill <PID>`・`taskkill /PID <PID> /F`）か、Claude Codeのタスク停止機能だけを使う
- イメージ名の一括指定（`taskkill /IM python.exe`・`pkill python`・`killall python`等）は禁止。無関係のPythonのプロセスまで止める

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

### ⑤ ヘッダー・ナビ・サブナビの置き方（新規ページ作成時は必須、2026-10-17 [[NAV-LINK-INTEGRITY-1]]）

- **ヘッダーは`position:fixed`にせず、`position:sticky; top:0`にする**。fixedにして本文の`padding-top`で高さを補う形は、
  ナビの項目数・画面幅でヘッダーの高さが変わると本文が隠れる（EPS stock.htmlで銘柄名の行が隠れていた。A-6）。`height`の決め打ちもしない
- **`site-theme.css`は全ページで読み込む**（ページ内の`<style>`より前に読めば、ページ側の`:root`の色が残る）
- **ナビ・サブナビ・銘柄横断リンクのCSS（`.site-nav`・`.nav-links`・`[data-sub-nav]`・`[data-ticker-links]`）をページ内に書かない**。
  正本は`site-theme.css`の1か所（TAILのページ内の定義が二重になり、ロゴが押し縮められて折り返していた）
- グローバルナビ（ツール間）は、ヘッダーの中に空の`<nav class="nav-links"></nav>`か`<nav data-site-nav></nav>`を置くだけにする。
  項目を書かない（正本は`site-nav.js`のITEMSだけ）
- ヘッダーを`innerHTML`等で後から描画するページは、描画の後に`if (window.SiteNav) SiteNav.apply();`を呼ぶ（TANUKI stock.htmlの`applySharedNav()`）
- ツール内のサブページ（一覧・詳細・管理画面等）への導線は、ヘッダーの直後に`<nav data-sub-nav>`で置く（`site-nav.js`の置換の対象外）。
  現在のページの項目に`class="active"`。どのページも必ずどこかのサブナビから辿れるようにする（孤立ページにしない）

### ⑥ 個別銘柄ページの銘柄横断リンク（`?ticker=`を受けるページを作る場合は必須）

- `<div data-ticker-links></div>`を置き、`docs/common/ticker-links.js`を読み込む。URLの`?ticker=X`で「X TANUKI / EPS / HYPE / TAIL」を描画する
- 銘柄を後から決める・ページ内で切り替えるページは、描画の後に`TickerLinks.render(X)`を呼び、`history.replaceState`でURLの`?ticker=`も合わせる（EPS stock.html）
- 新しいツールの個別銘柄ページを足すときは、`ticker-links.js`の`TOOLS`に、ページのパスと、フロントエンドが読める登録銘柄リスト（`docs/`配下のJSON）を足す。
  `config/cik_lookup.csv`はGitHub Pagesから読めないため使わない

### ⑦ HOMEカード（`site-nav.js`のITEMSに足した場合は必須）

- `docs/index.html`のカードをITEMSと同じ順に足す（HOMEを除く。2026-10-17時点10枚）
- カードの色は`site-theme.css`の`--tool-*`（`body[data-tool]`と同じ色）を使い、`docs/index.html`に`.card-xxx{--acc-c:var(--tool-xxx)}`・`.card-xxx .card-title{color:var(--tool-xxx)}`を足す

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
