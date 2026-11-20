# Claude Code 作業開始テンプレート

> 2026-11-06 から細くした。**今の状態・未済の確認手順・決まりへの参照だけ**を置く（行数の上限は `tests/test_claude_code_start_thin.py`）。
> 日付つきの記録・件数の推移・済んだ手順は `docs/history/CLAUDE_CODE_START_HISTORY.md`。終業のまとめは `PROJECT_STATUS.md`。

## 今の状態（2026-11-12 終業）

- BACKLOG.md のアクティブは **14件**（`^### [` の機械カウント）。rcc NG 0・WARN 223、pytest 2960件通過・失敗2（kaihatsu。ruffの無い環境の1件と、SITMの既知の例外 `test_stonks_flag_rule`）・skip 6。コマンド: `env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider --deselect tests/test_common_path_guard.py::test_no_open_without_encoding_ruff_plw1514`（`PYTHONUTF8=1`。ruffの無い環境ではその1件を除く）＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`
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

33. **TANUKI-SHARES-SINGLE-SOURCE-1 の原本の確認（SECの取得）**（`[[TANUKI-SHARES-SINGLE-SOURCE-1]]`、`SEC_USER_AGENT_EMAIL`が環境変数に無く未実施）
   - 日時: 環境変数に`SEC_USER_AGENT_EMAIL`（定時と同じ連絡先。値は会話・文書・commitに書かない）を置いた回
   - 期限: 本件の公開の前
   - 確認の手順: ASTS・PLTR・SOUNの表紙の発行済（クラスごと）、TSLAの10-Qの本文（表紙3,953M と加重平均の差412Mの内訳）、IONQ（yfinanceの implied 464.7 が表紙432.5より+7.5%の理由）。読み取りだけ・10件/秒以下。結果は`TANUKI_SHARES_STEP0.md`末尾に書く。TSLAの未確定の株は確かめるまで分母に入れない（決定2）

34. **PARSER-LAYER3-SINGLE-SOURCE-1 段1（`-1`）の次の定時の実行の確認**（公開は2026-11-16 手動: TANUKI `43832337705`→HypeCore `43834462211`→TANUKI `43834776503`。1回目の印はscopeの17銘柄だけ・3回目は0件を確認済み）
   - 日時: 2026-11-22（日）のSEC_Data_Update（四半期BSの置き換え`quarterly_bs.py`が`parse_and_save`で動く）の連鎖の後。**11-17朝の分は合格済み**（TANUKI 06:49 JSTの定時、96銘柄とも印0件。チャット側で確認、確認の手順の①）
   - 期限: 2026-11-23
   - 確認の手順: ①`latest.json`の`method_change`が付かない（`method_change_applied`に`-1`が入っている）②日曜の作り直しで`common/sec_data/data/{T}/quarterly_*.json`の`bs`が、Layer3のvalのままで（新しい四半期の分だけ増える）CAKE・KOの理論株価（50.00・24.29）が動かない ③`tests/test_bs_lines.py::test_saved_records_crwv`の失敗はFINDINGS 2026-11-16（本件とは無関係）

35. **TANUKI-CALC-CONTINUITY-1（`-1-1`）の次の定時の実行の確認**（公開は2026-11-17 手動: TANUKI `44056048730`→HypeCore `44058384410`→TANUKI `44058703497`。1回目の印はscopeの16銘柄だけ・3回目は0件を確認済み。CAKE 41.25・DOCN 56.03〈HOLDのまま〉が試算と一致）
   - 日時: 2026-11-18（水）06:30 JST ごろのTANUKI VALUATIONの定時の実行の後（11-22の日曜のSEC_Data_Updateの後の分は手順34と同じ回で見る）。**11-18朝の分は合格済み**（TANUKI 06:48 JSTの定時、96銘柄とも method_change 0件・`method_change_applied`に`TANUKI-CALC-CONTINUITY-1-1`。残りは11-22の分）
   - 期限: 2026-11-23
   - 確認の手順: ①`latest.json`の`method_change`が付かない（`method_change_applied`に`TANUKI-CALC-CONTINUITY-1-1`が入っている）②日曜の作り直し後も`ttm/`の9ファイル（APP・ASTS・BBAI・BSY・DELL・FRSH・HWM・ONDS・RCAT）と、CAKE・KO・DOCNの理論株価が動かない（動くのは新しい四半期の分だけ）。済んだら`[[TANUKI-CALC-CONTINUITY-1]]`をBACKLOG_DONEへ移設する

36. **DILUTION-DEFINITION-UNIFY-1（`-1-1`）の次の定時の実行の確認**（公開は2026-11-18 手動: Stonks `44195625531`・TANUKI `44195630510`〈Stonksの完了で`44196135444`も自動で起動〉→HypeCore `44199705052`→TANUKI `44200008042`。1回目の印はscopeの90銘柄だけ・2回目以降0件、SOUN funda 35→46・SPIR 0→6〈予行の43・1とはQ3の10-Qで窓の終点が進んだ分の違い、原因は確定〉、STONKS `results.json`は`sbc_burden` 24件・`dilution_risk` 0件を確認済み）
   - 日時: 2026-11-19（木）06:30 JST ごろのStonks→TANUKIの定時の実行の後。**11-19朝の分は合格済み**（Stonks `44401956898`・TANUKI `44402530106`〈11-18 21:29Z・21:32Z〉、96銘柄とも印0件・`method_change_applied`に`DILUTION-DEFINITION-UNIFY-1-1`、`results.json`は`sbc_burden` 24件・`dilution_risk` 0件、画面はSBC負担のラベルがDOMにあり旧ラベルは無い・JSエラー0〈折りたたみの中のため可視の目視は手元の確認まで〉）。残りは11-22の日曜のSEC_Data_Updateの後の分（手順34と同じ回で見る）
   - 期限: 2026-11-24
   - 確認の手順: ①全銘柄の`latest.json`の`method_change`が付かない（`method_change_applied`に`DILUTION-DEFINITION-UNIFY-1-1`）②`results.json`の`sbc_burden`のまま（旧キーが戻らない）③画面（stock.html・tanuki_score・stonks-silo）で希薄化の欄・SBC負担のラベルが出る。済んだら`[[DILUTION-DEFINITION-UNIFY-1]]`をBACKLOG_DONEへ移設する

37. **DEFICIT-QUALITY-ADOPT-1 の次の定時の実行の確認**（公開は2026-11-19 手動。method_changeには登録しない〈表示だけ〉。公開直後の確認は合格済み: 対象24銘柄・`op_breakeven_*`・`structural_deficit_kind` 18銘柄・印0件。**2026-11-20朝（06:30 JSTの定時の後）の分は合格**: results.json 24銘柄・`gaap_breakeven_*`無し・latest.jsonに`breakeven_estimate`/`breakeven_reason`/`method_change`無し・`structural_deficit_kind` 18銘柄・SOUN 2026 PREDICTED・CRWV NO_TREND。画面の目視は未〈コードは`op_*`を先に読む〉。SOUNの2025Q4+42.6Mは条件付対価の公正価値の変動が原因に見える→FINDINGS 11-20）
   - 日時: 2026-11-20（金）06:30 JST ごろのStonks→TANUKIの定時の実行の後
   - 期限: 2026-11-25
   - 確認の手順: ①`results.json`が24銘柄のまま・`gaap_breakeven_*`が戻らない②`latest.json`に`breakeven_estimate`・`breakeven_reason`が戻らず、`method_change`が付かない③画面（stonks-silo・tanuki_score）の黒字化・構造的赤字の型の表示。SOUN（保有）は2026 PREDICTED〈R² 0.696、TTM 2026-09-30で成立。予行は「不明」だった〉、CRWV（保有）はNO_TREND。SITMの対象からの出入りは12月末以降（持続の規則）。済んだら`[[DEFICIT-QUALITY-ADOPT-1]]`をBACKLOG_DONEへ移設する
39. **MULTIPLES-ADOPT-1 の2回目の確認**（旧キー読みは2026-11-20に外し済み〈`d0aafa58f4`、rcc NG 0〉。次の定時の実行）
   - 日時: 2026-11-21（土）06:30 JSTの定時の後
   - 期限: 2026-11-25
   - 確認の手順: ①`latest.json`の`components`が新キーだけ・`market_cap_source`が95`sec`＋V`yf_fallback`のまま ②STONKSの`results.json`の時価総額・EVが部品から（Vに「（yfinance値）」の印）③公開の連鎖の目視〈TANUKI SCORE・stock.html・HypeCore detailのPER・PSR〉は**2026-11-20に済**（Playwright、手元。stock.html: SOUN〈GAAP PER「—」＝TTM赤字でforwardに落とさない・PSR 5.9x・時価総額$1.32B〉・NVDA〈PER 89.9x・PSR 11.7x・$3.55T〉・V〈PER 23.2x・PSR 17.3x・時価総額「yfinance値」の印〉、HypeCore detail SOUN〈PER「—」・PS 5.9x〉、TANUKI SCOREのPER 3列は96銘柄に出る〈GAAP空18＝TTM赤字〉。**ただしTANUKI SCOREはそのままでは構文エラーで描画されない**〈DEFICIT-QUALITY-ADOPT-1の337行目、FINDINGS 11-20。MULTIPLES-ADOPT-1とは無関係〉ので、確認は337行目を直した手元のコピーで行った。写真は作業用の場所 `C:\Users\shigi\AppData\Local\Temp\claude\096d85a5248d2f43\1b9d3f4c-902d-4179-aece-06680ae7a890\scratchpad\shots\`） ④日曜のMarket Data Weekly（`multiples.py --all`・`rotce.py`）の後にROTCEのP/TBVのパーセンタイル（ALAB 64→73・CELH 29→35）が反映。済んだら`[[MULTIPLES-ADOPT-1]]`をBACKLOG_DONEへ移設する。F（DCFの調整後PERとの差）の反転10銘柄は`ADSK・AVAV・BSY・CELH・FROG・LOAR・NET・RBRK・S`が真→偽・`PAYS`が偽→真（予行とはGTLB・ZETAが外れBSY・LOARが入った＝新しい四半期とGAAP PERがyfinanceのtrailingPEからSECのTTMに変わったため）

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
