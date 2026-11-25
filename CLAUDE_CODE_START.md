# Claude Code 作業開始テンプレート

> 2026-11-06 から細くした。**今の状態・未済の確認手順・決まりへの参照だけ**を置く（行数の上限は `tests/test_claude_code_start_thin.py`）。
> 日付つきの記録・件数の推移・済んだ手順は `docs/history/CLAUDE_CODE_START_HISTORY.md`。終業のまとめは `PROJECT_STATUS.md`。

## 今の状態（2026-11-12 終業）

- BACKLOG.md のアクティブは **22件**（`^### [` の機械カウント）。rcc NG 0・WARN 223、pytest 2960件通過・失敗2（kaihatsu。ruffの無い環境の1件と、SITMの既知の例外 `test_stonks_flag_rule`）・skip 6。コマンド: `env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider --deselect tests/test_common_path_guard.py::test_no_open_without_encoding_ruff_plw1514`（`PYTHONUTF8=1`。ruffの無い環境ではその1件を除く）＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`
- **未公開のコードの置き場所は無い**（Layer3の定義の列〈PARSER段1・CONTINUITY・DILUTION・DEFICIT・MULTIPLES・TANUKI-SHARES〉は2026-11-23に全部公開済み。記録は`BACKLOG_DONE.md`・各STEP 0・`docs/architecture/PUBLISH_REHEARSAL_2026-11.md`）。PARSER-LAYER3は段2以降が残る
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

41. **UNIVERSE-REBALANCE-2026Q4-1 の削除（14銘柄）の後、定時の実行で14銘柄が戻ってこないこと**（抹消は2026-11-23。`cik_lookup.csv`の`status=retired`・専用のデータ・設定を削除。市場データはS&P500の6銘柄〈FCX・MO・PEP・PM・VZ・XOM〉だけ残す）
   - 日時: 2026-11-24（火）06:30 JST ごろのTANUKI・Stonksの定時の実行の後（**11-24分は合格・確認済み**: 14銘柄は4つの一覧・専用のデータ・画面のデータに戻らず、市場データはS&P500の6銘柄だけ、HypeCoreのstageは削除の前後で84銘柄差0）。**残り: 日曜の連鎖（11-29）の後に1回**
   - 期限: 2026-11-25
   - 確認の手順: ①`tanuki_valuation/data/tickers.json`・`hypecore/data/tickers.json`・`stonks-silo/data/results.json`〈17銘柄〉・`adjusted_eps_analyzer/data/summary.json`に14銘柄が無い ②`docs/value-monitor/tanuki_valuation/data/{T}/`・`common/sec_data/data/{T}/`・`docs/common/multiples/{T}.json`が再び作られていない ③`common/market_data/`のFCX・MO・PEP・PM・VZ・XOM以外（KULR・QBTS・SPIR・BBAI・RDW・ONDS・RXRX・SCCO）が再び作られていない（`monitor_tickers.yaml`から外した） ④画面（tanuki_score・stonks-silo・hypecore・screening）に14銘柄が出ない。戻ってきたら止めて報告する

43. **POINT-IN-TIME-HISTORY-INTEGRITY-1 (4)（倍率の履歴の株数をas-ofに）の公開の後の確認**（公開は2026-11-25 kaihatsuへ`pit-4`を統合 `182a82a42d`・データ `5cde1fe3f3`。method_changeは登録しない〈理論株価・判定・stageは動かない〉）
   - 日時: 次のMarket Data Weekly（土曜）の定時の実行の後（2026-11-28 または 11-29）
   - 期限: 2026-12-01
   - 確認の手順: ①`docs/common/multiples/{T}.json`の直近12か月（2025-11以降）の`shares`が、公開の直後の値から動かない（動くのは新しい申告・日足の分だけ）②`current`の行・`docs/common/stage_v2/`の直近12か月のラベルが変わらない③NVDAの2021-01〜の株数が25.2B前後・CRWVの2025-11〜2026-02が497.886M（丸めた498Mに戻らない）。合格したら`[[POINT-IN-TIME-HISTORY-INTEGRITY-1]]`をBACKLOG_DONE.mdへ移設する（(1)〜(4)が全部済む）

44. **EARNINGS-REFLECTION-LAG-1 案B（決算は反映前の印）の、日曜の取り込みの後の確認**（公開は2026-11-25 手動: TANUKI `45512330488`・`45515069025`。表示だけ・method_changeなし。理論株価・判定の変化0〔82銘柄〕。印が付いたのは2026-11-25の時点でNVDA〔11-18発表・Layer3は2026-07-26まで〕とCEG〔11-09発表・SEC_LAG〕）
   - 日時: 2026-11-30（月）06:30 JST ごろのTANUKI VALUATIONの定時の実行の後（11-29の日曜の`SEC_Data_Update`の後）
   - 期限: 2026-12-01
   - 確認の手順: ①日曜の取り込みで10-Qが入った銘柄（`_freshness.json`のREFRESHED）の`latest.json`の`earnings_pending.status`が`none`に戻り、`report.txt`の`Earnings_Pending`行・stock.htmlの見出し・TANUKI SCOREの行から消えている ②まだ提出の無い銘柄（NVDAが提出前なら）は印が残る ③新しい印が決算前の銘柄に付いていない（`earnings_date`が米国東部の今日以前。予定日が取れない銘柄は`no_date`で、report.txtに「付けられません」） ④理論株価・判定が前日と同じ（印のせいで動かない）。合格したら`[[EARNINGS-REFLECTION-LAG-1]]`をBACKLOG_DONE.mdへ移設する

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
