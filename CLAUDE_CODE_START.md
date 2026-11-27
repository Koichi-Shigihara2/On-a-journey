# Claude Code 作業開始テンプレート

> 2026-11-06 から細くした。**今の状態・未済の確認手順・決まりへの参照だけ**を置く（行数の上限は `tests/test_claude_code_start_thin.py`）。
> 日付つきの記録・件数の推移・済んだ手順は `docs/history/CLAUDE_CODE_START_HISTORY.md`。終業のまとめは `PROJECT_STATUS.md`。

## 今の状態（2026-11-12 終業）

- BACKLOG.md のアクティブは **23件**（`^### [` の機械カウント）。rcc NG 0・WARN 223、pytest 2960件通過・失敗2（kaihatsu。ruffの無い環境の1件と、SITMの既知の例外 `test_stonks_flag_rule`）・skip 6。コマンド: `env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider --deselect tests/test_common_path_guard.py::test_no_open_without_encoding_ruff_plw1514`（`PYTHONUTF8=1`。ruffの無い環境ではその1件を除く）＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`
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
   - 確認の手順: ①日曜の取り込みで10-Qが入った銘柄（`_freshness.json`のREFRESHED）の`latest.json`の`earnings_pending.status`が`none`に戻り、`report.txt`の`Earnings_Pending`行・stock.htmlの見出し・TANUKI SCOREの行から消えている ②まだ提出の無い銘柄（NVDAが提出前なら）は印が残る ③新しい印が決算前の銘柄に付いていない（`earnings_date`が米国東部の今日以前。予定日が取れない銘柄は`no_date`で、report.txtに「付けられません」） ④理論株価・判定が前日と同じ（印のせいで動かない）⑤**取りこぼしが0**: 日曜（11-29）の Market Data Weekly の後、`attributes/{T}.json` の `calendar.last_earnings_date` が全82銘柄に入り（取れない銘柄は一覧にして報告）、`calendar.earnings_date`（予定日）が次の四半期へ進んだ銘柄（2026-11-25 の時点で AMAT・WMT）で、10-Qが未取り込みなら印が付いている（`earnings_pending.status=pending`）。2026-11-25 の修正（`771872fa0f`）で AMAT〈11-12 発表〉・WMT〈11-19 発表〉・DELL〈11-24 発表〉に印が付く見込み。合格したら`[[EARNINGS-REFLECTION-LAG-1]]`をBACKLOG_DONE.mdへ移設する

45. **TANUKI-GUIDANCE-REVISION-BLIND-1 案1（成長率と予想の差）の、次の Weekly の定時の後の確認**（公開は2026-11-25 手動: Market Data Weekly `45523880067`・TANUKI `45525203351`。表示だけ・method_changeなし。理論株価・判定の変化0〔82銘柄〕。81/82で値が入り、CIXだけ空欄）
   - 日時: 2026-11-30（月）06:30 JST ごろのTANUKI VALUATIONの定時の実行の後（11-29 日曜の Market Data Weekly の後）
   - 期限: 2026-12-01
   - 確認の手順: ①`attributes/{T}.json` の `estimates.fetched_at` が日曜の定時の時刻に更新されている（監視銘柄84のうち取れない銘柄は一覧にして報告。CIXが取れない理由を確認）②`latest.json` の `guidance_gap.status` が82銘柄で `ok` か `no_estimate`（理由つき）、`report.txt` に `Guidance_Gap` 行がある ③TANUKI SCOREの「成長率−予想(pt)」列で並べ替えられ、stock.htmlに行が出る ④理論株価・判定が前日と同じ。合格したら`[[TANUKI-GUIDANCE-REVISION-BLIND-1]]`をBACKLOG_DONE.mdへ移設する

47. **STONKS-DEFICIT-REALITY-EPIC-1 ①（資金の持ちの流出をTTMに）の、定時の後の確認**（公開は2026-11-26 手動: kaihatsu `f7b9d04284`・Stonks `45832636938`・TANUKI `45832652097`→HypeCore `45834738589`→TANUKI `45835195932`。ID `STONKS-DEFICIT-REALITY-EPIC-1-1`、scope 17銘柄。理論株価・判定・funda・stageの変化0〔82銘柄〕）
   - 日時: **2026-11-28（土）06:30 JST ごろ**のTANUKI・Stonksの定時の実行の後（1回目の🛠は公開の手動の連鎖で付いた。この回が2回目。当初は11-27朝だったが、11-26 は米国の感謝祭でNYSE休場のため Market Data Daily がガードで何もせず連鎖が動かない〈11-26 10:03・10:52 UTCの実行は cancelled・下流 skipped を確認〉。11-27 は米国の短縮日で、その分の Daily が 11-28 朝に動く）
   - 期限: 2026-11-30（②の公開はこの確認の後。11-28 朝に合格したら 11-28 中に公開できる）
   - 確認の手順: ①`latest.json`と`stonks-silo/data/results.json`に`method_change`の印が付かない（`method_change_applied`に`STONKS-DEFICIT-REALITY-EPIC-1-1`）②runwayが公開の直後の値と同じ（SOUN 19.27か月・IONQ 17.47か月〔総合 PROMISING 70.7〕・ASTS 35.34・AVAV 155.48・CRWV 2.46・JOBY 33.62・RKLB 169.81、TANUKIの`computed_runway_months`はCOHR 15.82・AMZN 29.23）③SOFIの`computed_runway_basis`が`financial_excluded`のまま（runwayなし・fundaは変わらない）④RCAT・CPRTが`annual_fallback`のまま ⑤理論株価・判定が前日と同じ。合格したら`[[STONKS-DEFICIT-REALITY-EPIC-1]]`の①を済にする（②③④は残す）

48. **MARKETPULSE-REVIEW-EPIC-1 順1（型L: 日付・鮮度）の、次の定時の後の確認**（実装は2026-11-27 kaihatsu。method_changeなし・判定に効かない）
   - 日時: 2026-11-28（土）06:30 JST ごろの Market Pulse の実行の後（11-27 の米国は短縮日で、その分のDailyが11-28朝に動く）。MACRO PULSE の `05_liquidity.csv` の新しい列は次の MACRO PULSE の実行の後
   - 期限: 2026-12-01
   - 確認の手順: ①`market_data.json` の最新エントリの `data_quality` に `expected_by_kind`（us_close・jpx・fred_daily）があり、`as_of` に「日経平均」「HYスプレッド」が入っている。`freshness` は新しくない要素だけ（日経が期待する日以降なら空）②`buy_checklist.checks.hy_spread.as_of` に日付がある ③画面（Market Pulse）のカードに日付、BUYチェックリストのHYスプレッドに日付が出る ④`daily/^N225.json` に 11-25（と 11-26）の行が入っている（`daily_guard` の取り直し。11-26 の感謝祭の分は 11-27 の実行で入る見込み）⑤`05_liquidity.csv` の最新の行に `sp500_asof`・`hy_spread_asof` が入り、System Health の [Q] が ✅（古い値なら⚠️の理由が日付つきで出る）⑥理論株価・判定・stageが前日と同じ ⑦**日本の行の取り直しだけの回**（`daily_guard` の `mode=jpx_only`。実行の Summary に `mode=jpx_only`・コミットの件名に「日本の行の取り直しだけ: ^N225」）が出ていれば、その回は**取り消し（cancelled）で終わり、直後に Market Pulse・Stonks Silo・TANUKI が起動していない**（`gh run list`で確認。実装の commit は `771196c82d`）。1日の jpx_only の回は、^N225の終値がYahooに入る21:30 UTC以降のcron〈21:47・22:17・22:47・23:17・01:47・02:17〉で、入った時点で止まる（最大6回。休場日は外部の起動が無くcronだけ）

49. **MARKETPULSE-REVIEW-EPIC-1 順2（型E: 説明と計算のずれ）の、次の定時の後の確認**（実装は2026-11-27 kaihatsu。表示とAIの入力だけ・理論株価・判定に効かない）
   - 日時: 2026-11-28（土）06:30 JST ごろの Market Pulse の実行の後。MACRO PULSE の週次AI解説は次の週次の実行の後
   - 期限: 2026-12-01
   - 確認の手順: ①`market_data.json` の最新エントリの `ai_facts` に「センチメント指数の内訳（寄与ptの合計＝指数−50）」があり、合計が`sentiment.score − 50`と±0.1で合う ②`stage_conclusions` の段階5の文言に「SPYより相対的に強い（Strong）」と「業種の実際の騰落: 1日…・5日…」、段階7が「SPYより相対的に強いセクター（Strong・Improving）にいる監視銘柄」、`sector_rotation.sectors.*.change_5d_pct` がある ③AIの見解（`summary`）が寄与0・「使った値=false」の指標を理由にしていない ④画面: 段階8の題名が「過去の似た日、その後どうなったか」、セクター表に1日・5日の列、BUYチェックリスト②の文言が「90日の最大値より0.30%以上低いか」⑤`05_weekly_analysis.csv` の次の行の `factor_analysis` が、寄与0の指標（点数が1週前と同じ）を理由にしていない（実行ログに「寄与0の指標を理由に挙げた」の警告が出ていれば言い直しの結果も見る）

50. **MARKETPULSE-REVIEW-EPIC-1 順3（型S: market_data.json の分割）の、明朝の定時の後の確認と、旧キーの読みを外す**（実装は2026-11-27 kaihatsu。`comments_history` を持たない形。Market Pulse の判定・出力は変わらない）
   - 日時: 2026-11-28（土）06:30 JST ごろの Market Pulse の実行の後
   - 期限: 2026-12-01
   - 確認の手順: ①`market_data.json` の最新のエントリに `comments_history` が無く、ほかのキー（`summary`・`stage_conclusions`・`data_quality` 等）は前日と同じ形で、ファイルが 7.3MB 前後（1日約50KB増）②画面（Market Pulse）の「過去のAIコメント」が最新のエントリで直近11件ぶん出る（前のエントリの `summary` から作る）③`scripts/migrate_market_data_comments_history.py --check` が「旧キーあり 0・食い違い 0」。合格したら、移行のスクリプト（`scripts/migrate_market_data_comments_history.py` と `tests/test_market_data_comments_history.py` の移行の検査）は役目を終えるので削除してよい（`COMMENTS_HISTORY_FROM` は残す。旧エントリの表示を変えないため）。⑤**latest.json と trail**: `docs/market-monitor/market-pulse/data/latest.json` が最新のエントリと同じ（`market_data.json` の最後のエントリと等しい）で、最新以外のエントリの `sector_rotation.sectors.*.trail` が増えていない（新しい回で前の最新の trail が消えている）。TANUKI SCORE の画面が表示され「最終更新」が最新の回の時刻、`snapshot.py`（TANUKI_Score_Update）が成功。System Health [Q] が latest.json を読んで✅。**旧キーの読みは画面に入れていない**（同じ push で旧キーを消したため。書き手のないキーを画面が読むと出力の一覧の検査 INV-5 が NG になる）

51. **MARKETPULSE-REVIEW-EPIC-1 順4（型P: 段・閾値の切り替わり）の、明朝の定時の後の確認**（実装は2026-11-27 kaihatsu。理論株価・判定に効かない。TANUKI・HypeCore は局面・ゾーン・象限・発動を読まない）
   - 日時: 2026-11-28（土）06:30 JST ごろの Market Pulse の実行の後。MACRO PULSE の週次（スコア・局面）は次の週次の実行の後
   - 期限: 2026-12-01
   - 確認の手順: ①`market_data.json` の最新のエントリに `basis.step_rules`（`STEP-RULES-1`）があり、`buy_checklist` に `trigger_held` がある。前日のF&Gが25以下で発動し当日26〜30なら `triggered=true`・`trigger_held=true`（31以上なら外れる）②`sector_rotation.sectors.*.quadrant` が前日の象限と、境目（100）を0.5以内で越えた分だけ違わない（前日 Strong で RS-Ratio が99.5〜100なら Strong のまま）③画面（Market Pulse）の時系列に、`basis` が入った最初の日（11-28）の縦線「判定の基準を変更」④MACRO: 次の週次の行に `basis=STEP-RULES-1`、`score` は小数の点数を丸めた整数、`phase` は前の週の phase から2pt越えたときだけ変わる（前の週が拡張で今週スコア30・31なら拡張のまま、32以上で踊り場）。画面のスコア推移に縦線 ⑤`05_main.py` の `SIGNAL_STEPS` と画面の写しが一致（テストが検査）⑥理論株価・判定が前日と同じ

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
