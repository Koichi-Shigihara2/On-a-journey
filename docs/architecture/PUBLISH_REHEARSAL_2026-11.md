# 公開の予行（未公開の6つを手元で重ねた結果、2026-11-13）

手元の作業用ブランチ `publish-rehearsal`（kaihatsu `bcf4742573` から。**pushしていない・消した**）に、公開の順で重ねて、各段でゲートの pytest と `tools/sim`（TANUKI→HypeCore→TANUKI、96銘柄）を回した。各ブランチの単独の試算は、kaihatsu の今の HEAD に**そのブランチだけ**を入れた別の作業用ブランチ（worktree）で回し、重ねた結果と比べた。表（全銘柄）は `PUBLISH_REHEARSAL_2026-11_table.csv`、案の差分は `PUBLISH_REHEARSAL_2026-11_gap_pct.patch`。

## 結論

- 順どおりに重ねて**テキストの衝突は生成物だけ**（出力の一覧・倍率の部品・`profitability.json`）。作り直せば解ける。コード・設定の衝突は0（`pipeline.py`・`stock.html`・`sec_concept_definitions.json` は自動で重なり、全段でテストが通った）
- **理論株価の変化は、単独の変化の掛け算とぴったり一致**（全96銘柄で差 0.000pp）。掛け合わせにならない銘柄は、理論株価・判定・funda・stage では**無い**。CAKE は CONTINUITY −22.6%・SHARES −3.3% で、積 −25.2% = 全部の後の −25.2%（和 −25.9% ではない）。KO・HWM・APP も同様。STEP 0 で気にしていた「CAKE ほか」は、**単独の試算が前の公開の上の値でなかった**ための差で、今の HEAD の上では解消
- 全部の後、今の公開の値に対して: 理論株価が変わる 86 銘柄・5%超 13（ALAB・ASTS・CAKE・CART・DOCN・FICO・KO・LITE・LOAR・PAYS・PLTR・SOUN・TSLA）・判定が変わる **3（CPRT・DOCN・RMBS）**・funda 点が変わる 10（保有はSOUN 35→43）・stage 0
- 保有9銘柄の判定は全段で変化0。理論株価（全部の後）: TSLA +11.5・SOUN +11.3・PLTR −6.4・SOFI −4.8・APP −1.1・CELH −0.9・NVDA −0.2・ADBE −0.1・CRWV +0.1

## 1. 重ねた順と各段（pytest・衝突・前の段からの変化）

「前の段から」は前の段の出力との比。「単独」は kaihatsu の今の HEAD にそのブランチだけを入れた試算の kaihatsu との比。

| 段 | 入れたもの | 衝突 | pytest | 前の段から（理論株価 / 5%超 / 判定 / funda / scope） | 単独と同じか |
|---|---|---|---|---|---|
| 0 | 基準（kaihatsu `bcf4742573`） | — | 2960通過・失敗1 | （今の公開の `latest.json` と理論株価は一致。判定は CAKE・MO・WMT が基準の試算と公開で違う＝相場による。表では「判定（今の公開）」と「全部の後」を並べた） | — |
| 1 | PARSER段1（`0544696ddc`→`1ebce36a25` を revert の revert） | なし | 2969通過・失敗1 | 1銘柄（APP −0.06%）/ 0 / 0 / 0 / scope 3 | 同じ（最初の段） |
| 2 | tanuki-calc-continuity-1（2 commit） | なし（`pipeline.py`・`stock.html`・`sec_concept_definitions.json` は自動で重なる） | 2976通過・失敗1 | 11 / 4（CAKE −22.6・DOCN −13.1・LOAR −10.8・KO +7.5）/ **DOCN HOLD→TRIM** / 0 / 16 | 同じ（単独 11・4・DOCN） |
| 3 | dilution-unify-1（4 commit） | 出力の一覧（tanuki）のみ→作り直し | 2993通過・失敗1 | 0 / 0 / 0 / **funda 10銘柄**（ALAB 75→100・AVAV 85→95・CART 55→80・IONQ 100→93・QBTS 10→0・RBRK 65→90・RDW 75→65・RXRX 60→55・SOUN 35→43・SPIR 0→1）/ 90 | 同じ |
| 4 | deficit-quality-adopt-1（4 commit） | 出力の一覧（sec・tanuki・stonks）のみ→作り直し。`python common/profitability.py --all` で `profitability.json` を作り直した（1ファイル） | **2996通過・失敗0**（既知の `test_stonks_flag_rule` は本段で通る） | 0 / 0 / 0 / 0 / 94 | 同じ |
| 5 | multiples-adopt-1（2 commit） | 出力の一覧（eps・stonks・tanuki）のみ→作り直し。`multiples.py --all`・`rotce.py` | 3006通過・失敗0 | 0 / 0 / 0 / 0 / 96 | 同じ |
| 6 | tanuki-shares-single-source-1（1 commit）＋案（下の2） | 出力の一覧・倍率の部品8銘柄（AAPL・ABBV・ADSK・CDNS・ELF・HWM・MO・WMT）→作り直し | 3018通過・失敗0 | **86 / 9（ASTS +30.4・TSLA +11.5・SOUN +11.3・LITE −10.8・PAYS −9.2・FICO −7.0・PLTR −6.4・CART −5.9・ALAB −5.4）/ CPRT HOLD→TRIM・RMBS WATCH→HOLD** / 0 / 96 | 同じ（単独の `multiples+shares` も同数） |

stage（HypeCore）は全段で0。STONKS の出力（`results.json`）は `tools/sim` の chain に入らないため、本予行では比べていない（DEFICIT の公開の確認手順5で見る）。

### 衝突の解き方（案）

衝突は全て生成物。手順は共通:
1. `git checkout --ours <生成物>`（または取り込み側）で一度解く
2. 作り直す: `python scripts/inventory/build_output_inventory.py --layer {sec,tanuki,stonks,eps,hypecore}`、倍率の部品は `python common/multiples.py --all`（`rotce.py` も）、`profitability.json` は `python common/profitability.py --all`、Layer3の派生は `python tools/sim/regen_derived.py` → `python tools/sim/revert_noise.py common/sec_data/ttm common/sec_data/normalized`（実質の変更の残りは段1が4ファイル・段2が9ファイル）
3. commit して次の段へ

公開の日にコードの衝突が出る可能性が高いのは `pipeline.py`（CONTINUITY・DILUTION・SHARES が触る）と `stock.html`。今回は自動で重なったが、kaihatsu が進めば変わる。出たら、希薄化の箇所は DILUTION の側（既存の手順）、`Shares_Basis` の行は SHARES の側を残す。

## 2. `outstanding_gap_pct` の1か所への寄せ（案、`PUBLISH_REHEARSAL_2026-11_gap_pct.patch`）

DILUTION（`financial_health.outstanding_gap_pct`＝期末の発行済÷加重平均−1）と SHARES（`components.outstanding_gap_pct`＝表紙÷採った分母−1）に同じ名前の印が2つあった。案:
- 式は `common/shares_base.py::gap_pct` の1か所。`common/dilution.py::share_count_change` もその関数を呼ぶ（引数と戻りは今のまま。`test_dilution.py` はそのまま通る）
- キーは `components.outstanding_gap_pct` の1か所。`pipeline.py` は `financial_health` に書かない（出力の一覧を作り直し）。`stock.html` の希薄化の欄の注記も `components` を読む（表示は「発行済株数との差 +11.5%（制限付株式・未確定の株・転換前後など。分母には入れない）」）
- テスト: `tests/test_shares_base.py::test_outstanding_gap_pct_has_one_formula_and_one_key`（式の重複・fhへの書き込み・画面の旧キー読みが無い）
- この案を入れた段6の試算・pytest（3018通過・失敗0）は上の表のとおり。理論株価・判定への影響は0（表示のキーだけ）
- 公開の手順: SHARES の公開の前に、この差分を本ブランチ（`tanuki-shares-single-source-1`）へ当てる（`git am`）。DILUTION の公開が先なので、その後に当てれば衝突しない

## 3. 全部を重ねた後の、今の公開の値との比（銘柄ごと・どのブランチで動いたか）

全銘柄は CSV（87行）。ここは動きが大きい・判定が動く・保有の分。単位は理論株価の変化%（前の段との差。空は0.05%以下）。

| 銘柄 | 全体 | PARSER | CONTINUITY | DILUTION | DEFICIT | MULTIPLES | SHARES | 判定 今の公開→全部の後 | 備考 |
|---|---|---|---|---|---|---|---|---|---|
| CAKE | −25.2 | | −22.6 | | | | −3.3 | HOLD→TRIM | 判定は基準の試算ですでにTRIM（公開との差は相場）。掛け算 |
| ASTS | +30.4 | | | | | | +30.4 | | 種類株。表紙が取れず補正なし。SEC待ち |
| LOAR | −12.8 | | −10.8 | | | | −2.2 | | |
| DOCN | −10.9 | | −13.1 | | | | +2.5 | HOLD→TRIM | **判定が動くのは CONTINUITY**（SHARES は逆向きに戻す分） |
| TSLA（保有） | +11.5 | | | | | | +11.5 | | 未確定の株は入れない。印 |
| SOUN（保有） | +11.3 | | | | | | +11.3 | | funda 35→43（DILUTION） |
| LITE | −10.8 | | | | | | −10.8 | | 希薄化証券 |
| PAYS | −9.2 | | | | | | −9.2 | | |
| KO | +7.2 | | +7.5 | | | | −0.3 | | |
| FICO | −7.0 | | | | | | −7.0 | | 年度平均 |
| PLTR（保有） | −6.4 | | | | | | −6.4 | | |
| CART | −5.9 | | | | | | −5.9 | | |
| ALAB | −6.1 | | −0.7 | | | | −5.4 | | 判定は変わらない（funda 75→100 は DILUTION） |
| SOFI（保有） | −4.8 | | | | | | −4.8 | | |
| APP（保有） | −1.1 | −0.06 | | | | | −1.0 | | |
| CPRT | −3.2 | | | | | | −3.2 | HOLD→TRIM | SHARES（境目、年度平均） |
| RMBS | −1.6 | | | | | | −1.6 | WATCH→HOLD | SHARES（境目） |

どのブランチの分かは CSV の列。**判定を動かすのは CONTINUITY（DOCN）と SHARES（CPRT・RMBS）だけ**。funda 点を動かすのは DILUTION だけ（10銘柄）。DEFICIT・MULTIPLES は理論株価・判定・funda・stage を動かさない（DEFICIT は STONKS の対象・`breakeven_*`、MULTIPLES は PER・時価総額の出どころ＝表示）。

## 4. 公開の日の段取り（案）

前提:
- 公開 = kaihatsu への push（Layer3 はその場で計算されるため、PARSER段1・CONTINUITY は push でそのまま公開）。連鎖は TANUKI VALUATION → HypeCore → TANUKI（3つ目は手で起動しない）。各件で `sim.py scope --write`（ID）と、**重ね直しのたびに前の公開の上でこの予行の試算をやり直す**（TANUKI の理論株価は単独の積で決まるので、各段の変化は表の列のとおりになる）
- 日曜は SEC_Data_Update（`ttm/`・`normalized/` の作り直し）と Market Data Weekly（`multiples.py --all`・`profitability.json`）が走る。**日曜を挟まないように並べ、手順30〜32（2026-11-15の確認、期限11-16）が済んでから始める**
- 1日に出す件数は、理論株価・判定を動かす件（PARSER段1・CONTINUITY・SHARES）は**1日1件**、動かさない件（DILUTION＝funda、DEFICIT＝STONKS、MULTIPLES＝表示）は**同じ日に2件まで**。🛠の通知が混ざらず、原因の切り分けがつくため

| 日 | 件 | 確認の手順（詳細は各STEP 0の末尾） |
|---|---|---|
| 11-16（月）以降の最初の平日 | PARSER段1（revert戻し＋`regen_derived`・一覧・`scope`ID） | 手順31の確認の後。1回目の印はscopeの3銘柄だけ・2回目0件。APP −0.06%。`PARSER_LAYER3_STAGE1.md` §4 |
| 翌営業日 | TANUKI-CALC-CONTINUITY-1（rebase・`regen_derived`＋`revert_noise`・ID `TANUKI-CALC-CONTINUITY-1-1`） | scope 16銘柄（CAKE・DOCN・LOAR・KO・…）。DOCN HOLD→TRIM。`TANUKI_CALC_CONTINUITY_STEP0.md` §公開の手順 |
| その次 | DILUTION-DEFINITION-UNIFY-1（ID `DILUTION-DEFINITION-UNIFY-1-0`）＋DEFICIT-QUALITY-ADOPT-1（`profitability.py --all` を公開の前に） | funda 10銘柄（SOUN 35→43）が🛠で出る・理論株価は動かない。STONKSの`results.json`24銘柄・`op_breakeven_*`・CHECK-51。順は DILUTION → DEFICIT（Stonks Silo Update の鎖は DILUTION・DEFICIT とも通る） |
| その次 | MULTIPLES-ADOPT-1（`multiples.py --all`・`rotce.py`・一覧） | 理論株価・判定0。`latest.json` に `per_gaap_ttm_sec`・`per_forward_yf`・`market_cap_source`。画面5か所の目視。旧キー読みは次の連鎖の後に外す（手順は`MULTIPLES_ADOPT_STEP0.md`の7） |
| その次（週のうち、日曜を挟まない日） | TANUKI-SHARES-SINGLE-SOURCE-1（上の2の案を当てる・`multiples.py --all`・ID `…-1-1` の scope・`committed_at` を作り直す） | 理論株価86銘柄（TSLA +11.5・SOUN +11.3・ASTS +30.4 …）・判定 CPRT・RMBS（DOCN は CONTINUITY）。`_shares_source` が `sec_*`（Vだけ`yf_implied_fallback`）。TSLAの`Shares_OutstandingGap`。SECの原本（手順33）は公開の前に |

- 月曜の朝に出さない件: TANUKI VALUATION の定時（06:30 JST）の直前は避け、定時の後か、手動の連鎖で出す（朝の定時が公開の連鎖と重なると、3つ目の実行の印の確認が混ざる）
- 各件の公開の直後に、前の件の「2回目は印0件」を確認してから次へ進む。1件でも不合格なら、次の件は出さず、その件を revert する（Layer3の件は revert の次の日曜まで派生が戻らないので、`regen_derived` でも戻す）
- BACKLOG: 各件の公開が済んだら BACKLOG_DONE へ移設（SHARES が最後。公開後は旧キー読みの除去と、`CLAUDE_CODE_START.md` の置き場所の削除）

## 5. この予行で確かめられていないこと

- STONKS の出力（`results.json`・対象24銘柄）と、画面の目視（stonks-silo・tanuki_score・stock.html の見え方）。`tools/sim` は TANUKI と HypeCore の出力だけ
- SEC の原本（ASTS・PLTR・SOUN の表紙・TSLA の10-Q本文・IONQ）。`SEC_USER_AGENT_EMAIL` が無く未実施（手順33）
- 前の公開（PARSER段1 など）で kaihatsu が動いた後の再試算。今回は kaihatsu `bcf4742573` の上
- `method_change.json` は今のブランチに SHARES の ID（`…-1-1`、scope 96）だけが入っている。PARSER段1・CONTINUITY・DILUTION の ID は公開の時に作る（予行では登録していない）。SHARES の `committed_at` と scope は公開の前に作り直す
