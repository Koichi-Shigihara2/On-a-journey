# Claude Code 作業開始テンプレート

> 2026-11-06 から細くした。**今の状態・未済の確認手順・決まりへの参照だけ**を置く（行数の上限は `tests/test_claude_code_start_thin.py`）。
> 日付つきの記録・件数の推移・済んだ手順は `docs/history/CLAUDE_CODE_START_HISTORY.md`。終業のまとめは `PROJECT_STATUS.md`。

## 今の状態（2026-11-28）

- BACKLOG.md のアクティブは **26件**（`^### [` の機械カウント）。rcc NG 0・WARN 179〔11-27 終業の実測。START の前の数字 181 は誤り〕、pytest 3164件通過・失敗0・skip 3（kaihatsu）。コマンド: `env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider`（`PYTHONUTF8=1`。約5〜7分。長いので分けるときは`-k`で絞る）＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`
- **未公開のコードの置き場所**: ブランチ `parser-layer3-stage2a`（`37d45f7310`、PARSER-LAYER3 段2a＋2b、公開は 12-01〈火〉。手順は `docs/architecture/PARSER_LAYER3_STAGE2_STEP0.md` §2c）の1つだけ（Layer3の定義の列〈PARSER段1・CONTINUITY・DILUTION・DEFICIT・MULTIPLES・TANUKI-SHARES〉は2026-11-23に全部公開済み。記録は`BACKLOG_DONE.md`・各STEP 0・`docs/architecture/PUBLISH_REHEARSAL_2026-11.md`）。PARSER-LAYER3は段3（年次）以降が残る
- 2026-11-28: 手順47〜54のうち47・48・50・52〜54は合格〔49・51は MACRO の週次待ち〕。STONKS-DEFICIT-REALITY-EPIC-1 ② を公開（`3bc782822d`、手順55）
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

55a. **11-29（日）Market Data Weekly の後の確認**（EPS予想の改定の履歴・sector_master の写しの廃止・[SKIP] の記録。実測の値は 2026-11-28 に測った値）
   - 日時: 2026-11-29（日）の Market Data Weekly（13:20 UTC）の後。`gh run list --workflow "Market Data Weekly Update"` で成功を先に確かめる
   - 期限: 2026-11-30（55b の前に済ませる）
   - ①**履歴が積まれた**（[[EPS-REVISION-OUTLOOK-MARK-1]]）: `common/market_data/estimates_history/{T}.json` の `records` が2行〔11-28・11-29〕の銘柄が83（CIX だけ無い）。合格: 83銘柄・CIX 以外の欠け 0
   - ②**属性に新しい項目**: `attributes/{T}.json` の `estimates` に `eps_trend`・`period_ends`・`fiscal` がある。合格: 83銘柄（11-28 の実測 83）。RKLB の 0q だけ値が無いのは同じ（11-28 の実測）
   - ③**Weekly の commit に全部入った**: 同じ commit に `estimates_history/`・`daily/`・`attributes/` が入る（`git add` の行が単独であること）。合格: `git show --stat <Weekly の commit>` に3つとも出る
   - ④**sector_master の写し**: `config/sector_master.json` に `rules`・銘柄ごとの `attributes_fetched_at` が無い。合格: grep で 0件・`tickers` 84（11-28 の実測 84）
   - ⑤**`split_reconcile`**: Weekly のログの「走査 N銘柄、当たり M銘柄」と `[SKIP]`。合格: 当たり 0・[SKIP] 0（11-28 の本番データでの走査の実測 0・0）。`_split_reconcile_skips.json` の `skips` が `{}`
55b. **11-30（月）朝の定時の後の確認**（STONKS-DEFICIT-REALITY-EPIC-1 ② の2回目ほか。公開は 2026-11-28 手動: kaihatsu `3bc782822d`・Stonks `46092665726`・TANUKI `46092663177`→HypeCore `46093926925`→TANUKI `46094103217`。ID `STONKS-DEFICIT-REALITY-EPIC-1-2`、scope 1銘柄〔SOUN〕）
   - 日時: 2026-11-30（月）06:30 JST ごろの定時の実行の後（11-29 の Weekly が `profitability.json` を作り直した後）
   - 期限: 2026-12-01
   - ①**印が付かない**: `stonks-silo/data/results.json` の `method_change` が無く、`method_change_applied` に `STONKS-DEFICIT-REALITY-EPIC-1-2`。合格: 印 0件
   - ②**SOUN・AVAV**: SOUN の黒字化の予測が `TOO_FAR`・`op_breakeven_year` 2030・総合 10x_CANDIDATE 78.2、AVAV が `NO_TREND`（公開の直後〔11-28〕の実測）。合格: 4つとも同じ
   - ③**`profitability.json`**: `noncash_adjustment` が applied 12・candidate 2〔IONQ・SITM〕・none 70、SOUN `loss` 14.99四半期・R² 0.964、RKLB 12.21・S 14.70・RCAT R² 0.001・AVAV R² 0.749（11-28 の実測）。合格: 同じ値（日曜の再計算で動く分は0.1以内）
   - ④**理論株価・funda・stage**: `latest.json` の `intrinsic_value_per_share`・`funda_score`・HypeCore の stage が前日と同じ。合格: 変化 0件（82銘柄。`tanuki_score` が変わるのは `timing_score` の動きだけ。11-28 の実測は理論株価・funda の変化 0）。`docs/common/stage_v2/SOUN.json` の最新の月も変わらない
   - ⑤**`expected_by_kind` の廃止**: 11-29・11-30 の Market Pulse の新しいエントリに `data_quality.expected_by_kind` が無い。合格: 0件。`freshness` の各要素に `expected` が出ている
   - ⑥**System Health**: [Q]（値の日付・鮮度）・[D]（MACRO PULSE の鮮度の項。`05_fed_context.csv` の最後の commit から7日超で ⚠️。11-28 の実測は6日）・[R]（日足の検証の印）が出る。合格: [R] の直近90日の「それ以外」が 1行/1銘柄（WBD。11-28 の実測）・新しい印 0・新しい [SKIP] 0
   - ⑦**Daily と下流**: Market Data Daily と下流4つ（Market Pulse・Stonks Silo・TANUKI・Portfolio）が成功。合格: すべて success（jpx_only の回は Daily が cancelled・下流が skipped で正）
   - ⑧**本番の STONKS 画面**（「③ 黒字化パス · 詳細」）: SOUN は「営業利益(TTM) 報告値 −$69.3M → 調整後 −$221.0M」「評価益 +$151.7M を除外」、AVAV は「−$252.6M → −$101.3M」「減損 −$151.3M を除外」、ASTS は行なし、「runwayの基準」の行（SOUN は TTM 〜2026-09-30、RCAT は年次・営業CFの直近4四半期が揃わない）、右端が切れない。合格: 値が試算（11-28）と同じ
   - ⑨**TANUKI の `report.txt`**: SOUN に `Breakeven_Noncash`・`Breakeven_OpIncome_TTM`・`Runway_Basis`（期末つき）。合格: ある。上限が効いた銘柄に `Moat_Phase1_Years: phase1 年数 N → 7（上限）`（11-28 の実測 17銘柄）
   - ⑩**STONKS の出力の4キー**: `results.json` に `core_profit`・`ocf_acceleration`・`ocf_yoy_change`・`gross_margin_trend` が無い（次の STONKS の実行の後）。合格: 0件
   - 合格したら `[[STONKS-DEFICIT-REALITY-EPIC-1]]` の②を済にする（③④は残す）
55c. **MACRO PULSE の週次の実行の後の確認**（旧 手順49⑤・51④）と **MARKETPULSE-REVIEW-EPIC-1 の移設の判断**
   - 日時: 次の MACRO PULSE の週次（土曜 22:11 UTC の weekly-analysis。11-28 の週次は 11-28 22:11 UTC）の実行の後
   - 期限: 2026-12-01
   - ①**EPIC 順2（旧 49⑤）**: `05_weekly_analysis.csv` の次の行の `factor_analysis` が、寄与0の指標（点数が1週前と同じ）を理由にしていない。合格: 寄与0の指標を理由に挙げた行 0件（実行ログの「寄与0の指標を理由に挙げた」の警告 0）
   - ②**EPIC 順4（旧 51④）**: 次の週次の行に `basis=STEP-RULES-1`、`score` は小数の点数を丸めた整数、`phase` は前の週から2pt越えたときだけ変わる（前の週が拡張で今週スコア30・31なら拡張のまま、32以上で踊り場）。画面のスコア推移に縦線。合格: `basis` 列が `STEP-RULES-1`・整数の `score`・局面の変化が2ptの幅どおり
   - ③**移設の判断**: ①②が合格し、手順48〜54 の残りは 2026-11-28 に合格済み（記録は `docs/history/CLAUDE_CODE_START_HISTORY.md`）。合格: ①②とも合格なら `[[MARKETPULSE-REVIEW-EPIC-1]]` を BACKLOG_DONE.md へ移設（移設しない間は BACKLOG に残す）
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
