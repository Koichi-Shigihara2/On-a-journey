# Claude Code 作業開始テンプレート

> 2026-11-06 から細くした。**今の状態・未済の確認手順・決まりへの参照だけ**を置く（行数の上限は `tests/test_claude_code_start_thin.py`）。
> 日付つきの記録・件数の推移・済んだ手順は `docs/history/CLAUDE_CODE_START_HISTORY.md`。終業のまとめは `PROJECT_STATUS.md`。

## 今の状態（2026-11-11 終業）

- BACKLOG.md のアクティブは **13件**（`^### [` の機械カウント）。rcc NG 0・WARN 217、pytest 2948件通過・失敗1（SITMの既知の例外 `test_stonks_flag_rule`）・skip 6。コマンド: `env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider --deselect tests/test_common_path_guard.py::test_no_open_without_encoding_ruff_plw1514`（`PYTHONUTF8=1`。ruffの無い環境ではその1件を除く）＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`
- **未公開のコードの置き場所**: PARSER-LAYER3 段1は`0544696ddc`・`1ebce36a25`でrevert済み（公開の時にrevertの戻し）、TANUKI-CALC-CONTINUITY-1はブランチ`tanuki-calc-continuity-1`（kaihatsuには無い。originにも控えあり〈7b8a432d2b〉）。Layer3の定義のコードをpushするとそのまま公開になる
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

15. **TAILのレビューを対象の期のデータで作る**（`[[TAIL-REVIEW-PERIOD-FILTER-1]]`、2026-10-30 実装 `f9a7563010`・移設済み）
   - 日時: 保有銘柄の次のTANUKI TAILのレビューの生成（Q3の10-Qの後、11月上旬の見込み。`gh run list --workflow TANUKI_TAIL_RSS_Monitor.yml`で先に確かめる）の後
   - 期限: 2026-11-30
   - 確認の手順: 新しいレビュー（`docs/portfolio/tail/data/reviews/{T}_{Q}_review.json`）に`data_as_of`（キューのfiled）が入り、`backfilled`がfalse、`kpi_snapshot`・`layer3_snapshot`の期が対象の期まで（対象より後の期が無い）、SECの財務の値が対象の期の申告日以前の値（Layer3の`first_val`）であること。確かめたらBACKLOG_DONE.mdの`[[TAIL-REVIEW-PERIOD-FILTER-1]]`の「確認」に書き足す

30. **SECTOR-MASTER-ADOPT-1 の印が次の定時の実行で付かないこと**（`[[SECTOR-MASTER-ADOPT-1]]` 段1〜4、段4の公開は2026-11-11 手動のEPS ANALYZER run `42941456669`→TANUKI `42942898607`）
   - 日時: 2026-11-12（木）06:30 JST ごろのTANUKI VALUATIONの定時の実行（`gh run list --workflow TANUKI_VALUATION_Update.yml`で先に確かめる）と、2026-11-15（日）のSEC_Data_Updateの連鎖のEPS ANALYZER・TANUKI
   - 期限: 2026-11-16
   - 確認の手順: 各銘柄の`latest.json`の`method_change`が`SECTOR-MASTER-ADOPT-1-1`〜`-4`で付いていない（`method_change_applied`に4つとも入っている）こと。EPSの`quarterly.json`の`sector`が95銘柄とも付いたままであること
   - 付かなければ、BACKLOG.mdの`[[SECTOR-MASTER-ADOPT-1]]`をBACKLOG_DONE.mdへ移設する（残る段は無い。アクティブ13→12）。付けば原因を報告する
31. **PARSER-LAYER3-SINGLE-SOURCE-1 段0（`-0`）の次の定時の実行の確認**（公開は2026-11-11 手動: TANUKI `42960814475`→HypeCore `42962427012`→TANUKI `42962625921`。1回目の印はscopeの9銘柄だけ・3回目は0件を確認済み）
   - 日時: 2026-11-15（日）のSEC_Data_Update（ttm・normalizedを日曜に作り直す）と、次のMarket Data Weekly（`common/multiples.py --all`が倍率の履歴を作り直す）の後
   - 確認の手順: ①日曜の作り直しで`common/sec_data/ttm/`の19銘柄のD&A・`normalized/`の年次11年が、コミットした値のまま（新しい四半期の分だけ増える）②`docs/common/multiples/`は12か月より前の月だけが変わり（64銘柄、`net_income_5y_*`・`pe_5y_avg_earnings`）、直近12か月は変わらない ③`latest.json`の`method_change`が付かない
   - 期限: 2026-11-16。次の段は`PARSER_LAYER3_STAGE0.md`の§7（段1の前に、動く銘柄の理由を原本の申告で確かめた一覧）

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
