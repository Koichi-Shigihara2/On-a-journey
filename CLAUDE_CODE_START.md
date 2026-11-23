# Claude Code 作業開始テンプレート

> 2026-11-06 から細くした。**今の状態・未済の確認手順・決まりへの参照だけ**を置く（行数の上限は `tests/test_claude_code_start_thin.py`）。
> 日付つきの記録・件数の推移・済んだ手順は `docs/history/CLAUDE_CODE_START_HISTORY.md`。終業のまとめは `PROJECT_STATUS.md`。

## 今の状態（2026-11-12 終業）

- BACKLOG.md のアクティブは **24件**（`^### [` の機械カウント）。rcc NG 0・WARN 223、pytest 2960件通過・失敗2（kaihatsu。ruffの無い環境の1件と、SITMの既知の例外 `test_stonks_flag_rule`）・skip 6。コマンド: `env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider --deselect tests/test_common_path_guard.py::test_no_open_without_encoding_ruff_plw1514`（`PYTHONUTF8=1`。ruffの無い環境ではその1件を除く）＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`
- **未公開のコードの置き場所は5つ**（Layer3の定義のコードをpushするとそのまま公開になる。**PARSER-LAYER3 段1は2026-11-16に公開済み**〈TANUKI `43832337705`→HypeCore `43834462211`→TANUKI `43834776503`、1回目の印はscopeの17銘柄だけ・3回目0件・CAKE 50.00・KO 24.29が試算と一致。確認は手順34〉。残りの公開の順は CONTINUITY → DILUTION → DEFICIT → MULTIPLES → TANUKI-SHARES、各々の前に前の公開の上にrebaseして試算し直す）: (2) TANUKI-CALC-CONTINUITY-1はブランチ`tanuki-calc-continuity-1`（originにも控えあり〈7b8a432d2b〉）、(3) DILUTION-DEFINITION-UNIFY-1は**2026-11-18に公開済み**（ID `-1-1`、scope 90銘柄。確認は手順36）、(4) DEFICIT-QUALITY-ADOPT-1は**2026-11-19に公開済み**（method_change登録なし。確認は手順37）。(5) MULTIPLES-ADOPT-1は**2026-11-20に公開済み**（確認は手順39。旧キー読みは外し済み。以下は公開前の記録: ブランチ`multiples-adopt-1`〈`2741efd6e7`、kaihatsu `7a6a5acd2b`から。公開の手順は`MULTIPLES_ADOPT_STEP0.md`末尾。公開の前に`python common/multiples.py --all`と`python common/sec_data/rotce.py`）。(6) TANUKI-SHARES-SINGLE-SOURCE-1はブランチ`tanuki-shares-single-source-1`（`8cdf37598c`、`multiples-adopt-1`の上。公開の手順は`TANUKI_SHARES_STEP0.md`末尾。公開の前に`python common/multiples.py --all`）。(2)・(5)・(6)はkaihatsuには無く、originに控えがある。6つを公開の順に重ねた予行（衝突・各段の試算・公開の日の段取りの案）は`docs/architecture/PUBLISH_REHEARSAL_2026-11.md`
- 次の作業の順と注記は `PROJECT_STATUS.md` の「次の作業」が正
- 依頼文の見出しの【Opus】【Sonnet】に合わせて `/model` で切り替える（`CHAT_RULES.md`「モデルの使い分け」。付いていない依頼はOpus）
- 報告は20行以内（結論・変わった数・判定の変化・決めてほしいこと）。詳しい表はファイルに置き、場所だけ書く（`CHAT_RULES.md`「報告の形」）
- 範囲外の発見は直さず、BACKLOGにも入れず、`FINDINGS.md` に1行で記録する（`CHAT_RULES.md`「発見の溜め方と根治の判断」・事例41）。
  GitHubのscheduleは数時間遅れることがあるため、実行の有無は `gh run list` で先に確かめる

## 次回の確認手順（未済のものだけ。日時〈JST〉の順。番号は前のまま〈BACKLOGが番号を参照するため〉）

済んだ手順・日付つきの確認の記録は `docs/history/CLAUDE_CODE_START_HISTORY.md`。各項目は「日時｜期限｜確認の手順」。

29. **定義v2の後の段階（S3・S4）の12か月の並びの評価**（`[[STAGE-V2-ROLLOUT-1]]` ②、2026-11-06 記録開始 `common/stage_v2.py`）
   - 日時: 2027-11ごろ（並行計算の記録が12か月たまった後）
   - 期限: 2027-12-31
   - 確認の手順: `docs/common/stage_v2/{T}.json`の2026-11以降の月末の`stage`ごとに、その後12か月のS&P500に対する超過リターン（中央値）を出し、S3・S4が前の段階（S1・S0・消化）より下に来るかを見る（段Cは2022-01〜でS3・S4が0付近、2024-12〜で−0.214・−0.184）。件数も並べる。切り替えの条件にはしない（決定 2026-11-06）
   - 結果は`[[STAGE-V2-ROLLOUT-1]]`の②に書き、§0でS3・S4を売りの判断に使うかの材料にする

9. **TANUKI TAILのRSS Monitorの残り**（`[[TAIL-RSS-FALSE-NEW-FILING-1]]`・`[[TAIL-RSS-SECRET-NAME-1]]`、どちらも移設済み。残りは「RSS 取得エラー」の回だけ。ほかの経路は2026-10-19・10-23に確認済み〈BACKLOG_DONE.mdの「確認」の欄〉）
   - 日時: 平日の実行（cronは08:00 UTC＝17:00 JST。scheduleは数時間遅れるため`gh run list --workflow TANUKI_TAIL_RSS_Monitor.yml`で先に確かめる）の後
   - 期限: 起きた回（10-16・10-22の実行はRSS 取得エラー0件で、この経路は通っていない）
   - 確認の手順: 「RSS 取得エラー」が出た回のログで、その銘柄に「★ 新規提出」が出ず「新規提出の判定をしない」が出ている。確かめたら、BACKLOG_DONE.mdの2件の「確認」の欄に書き足す

33. **TANUKI-SHARES-SINGLE-SOURCE-1 の原本の確認（SECの取得）の残り**（TSLAの412Mは2026-11-23にチャット側で確認済み＝2025 CEO報酬の未確定の株423,743,904株・決定2のまま〈`TANUKI_SHARES_STEP0.md`末尾〉。公開の前提から外した）
   - 日時: 環境変数に`SEC_USER_AGENT_EMAIL`（定時と同じ連絡先。値は会話・文書・commitに書かない）を置いた回
   - 期限: なし（公開の後でよい）
   - 確認の手順: ASTS・PLTR・SOUNの表紙の発行済（クラスごと）、IONQ（yfinanceの implied 464.7 が表紙432.5より+7.5%の理由）。読み取りだけ・10件/秒以下。結果は`TANUKI_SHARES_STEP0.md`末尾に書く

## 決まりへの参照（本文は `CLAUDE_CODE_PROCEDURES.md` の同名の節）

| 知りたいこと | 節 |
|---|---|
| 作業の始め方（pull・現状確認・テスト・宣言）、運用の基本、STEP 0 の運用ルール | 「毎回の作業開始時に必ず実行すること」・冒頭の運用の基本 |
| ファイル変更の原則・調査での書き込みの注意・履歴JSONへの記録 | 「作業ルール」 |
| 計算方法（理論株価・判定・funda・stage・Layer3・TTM）を変えるとき（scope・派生ファイル・読み合い・印） | 「計算方法の変更の手順」 |
| 試算の共通の道具（前後の全銘柄比較・scope・変化の表） | `tools/sim/README.md` |
| 新規銘柄の登録・削除・TAILの銘柄追加 | 「新規銘柄登録時の必須手順」・「銘柄削除時の必須手順」・「TANUKI TAIL 銘柄追加手順」 |
| 管理の操作（ポートフォリオ・設定の編集をClaude Codeが代行） | 「管理の操作の手順」 |
| 作業完了時・月次・BACKLOG管理 | 「作業完了時のチェックリスト」・「月次メンテナンスタスク」・「BACKLOG管理ルール」 |
| HTMLを新規作成・移設・削除したとき | 「リンク整合性チェック」 |
| 外部AIレビューの誤指摘の予防・フロントエンドの調査順序 | 「重要ルール」の各節 |
| 決まり全般・過去の失敗事例 | `CHAT_RULES.md` |
