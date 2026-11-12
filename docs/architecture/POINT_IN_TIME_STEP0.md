# POINT-IN-TIME-HISTORY-INTEGRITY-1 STEP 0（2026-11-12、読み取りと手元の試算のみ）

コードのcommit・push・公開は無し。道具はscratchpadの`pit/`（`d1.py`日足の突き合わせ・`d2.py`Yahooの分割履歴・`d3.py`株数の跳び・`d5.py`Q4のfirst_filed・`sim.py`試算）。Yahooの値は`yfinance`（auto_adjust=False）で2026-11-12に取った。

## 結論

| 件 | 結論 | 公開の値への影響 |
|---|---|---|
| (1) 日足の分割の調整 | 日足とYahooの食い違いは**2銘柄だけ**（APH・CTVA。どちらも`split_history.yaml`に無く、TANUKIの登録銘柄でもない）。`_daily_rejected_overwrites.json`は1件（KVUEの出来高、分割とは無関係） | 理論株価・判定は動かない。Market Pulseの移動平均のブレッドスだけに効く |
| (2) HON | 株数の0.5は正しい（Layer3の株数が2026Q1→Q2で0.499倍）。食い違いは「Yahooの株価の調整は併合×分社の純0.9535」と「`split_history`は株数の比0.5だけ」という**種類の混在**。倍率の履歴は分割前の月を台帳が判定不能にしている | 今の公開の値は変わらない |
| (3) Q4のfirst_filed | 逆算Q4 5,340件のうち**197件・34銘柄**で、Q4の`first_filed`が年次の`first_filed`より後。全件`q4_basis=latest`の印つき。原因は「Q1〜Q3のどれかの単独の値が、年次の10-Kの後の書類で初めて出る」こと。**案Bの試算で倍率・定義v2・収益の段階は1つも動かない** | なし |

## (1) 日足の分割の調整

- 全597ファイル・853,300行を、Yahooの終値（2020-12-28〜）と比較。閾値2%
- 食い違い: **APH**（1,423行、比2.0。2026-09-03の2:1分割）・**CTVA**（1,442行、比6.665。2026-10-01の分社。日足が10-14で止まっている→FINDINGS）
- 日足の中の跳び（前日比＞1.45か＜0.69）は43銘柄に出るが、ほとんどは実際の値動き（ASTS・SOUN・KULR等）。分割はAPH 2026-09-03（0.5127）とCTVA 2026-10-01（0.1619）。NFLX 10:1・CRWD・CVNA・DD・BKNG等ほかの分割は食い違い0＝調整済み
- なぜ`_daily_rejected_overwrites.json`に出ないか: 毎日の取得は直近（90日まで）の行しか取り直さないので、古い行は分割の後も取り直されず、拒否の記録にならない。全期間の突き合わせでしか見つからない
- `_daily_rejected_overwrites.json`の分類: 1件 = KVUE 2026-10-30の出来高（66,595,400→66,377,525、Yahooの事後の改定）。分割の調整は0件
- Yahooの分割履歴（2020年以降、597銘柄）: 登録済みで比が一致18、**HONだけ比が違う**（Yahoo 0.9535・split_history 0.5）、**未登録99件**（`d2.json`）。比が整数・半整数でないもの（BDX・DD・FTV・GE・MMM・SCCO・AVB・CMCSA・FDX・SPGI・DELL・CTVA等）は分社・特別配当の調整で、株数ではなく株価の調整
- 影響: `sector_master.registered_tickers`（98銘柄）にAPH・CTVAは無く、倍率・TANUKI・HypeCoreの入力に入らない。効くのはMarket Pulseのブレッドス（`breadth_calculator`、日足の移動平均）。APHは200日線が生で131.0・調整後75.5（終値79.3）、CTVAは50日線が生67.4・調整後12.4（終値13.4）で、どちらも「線の下」に誤って数えられる
- 案: 日足の調整を`split_history`に依らず**Yahooとの比**で決める処理を足す（HONの比が違う問題を避ける）。全銘柄の過去の行をYahooの終値と比べ、比が一定（2%超の行が全期間の8割以上で、比が一つに揃う）なら、`fetcher.correct_daily_rows(reason="split_adjust", note="ratio=…")`で境の日より前の行を割り戻す。書き手は`correct_daily_rows`だけ（`write_daily_rows`が唯一の書き手の決まりを守る）。1回の走査は597銘柄の`yf.download`で手元で数分（週1回のMarket Data Weeklyか、分割の検知〈CHECK-54〉の後）

## (2) HONの併合と分社

| 見るもの | 事実 |
|---|---|
| 日足 | 2026-06-26の終値は今のYahooで243.53、その時点の記録（history.json）は228.57 → 比1.0655。併合後2026-06-29は227.80。Yahooの分割履歴の比は0.9535（併合×分社の純の値）。生の株価は併合の前後でほぼ連続 |
| 株数 | Layer3の希薄化株数 2026Q1→Q2 が0.499倍（319.2M）。split_historyの0.5と一致。株数は正しい |
| 倍率の履歴 | 2021-01〜2026-05は`split_price_mismatch`で判定不能（`config/split_price_ledger.json`のmismatch 1件がHON）。2026-06以降は計算されている |
| 基準が混ざる所 | 2026-07〜09は株価・株数が併合後（時価総額67〜77B）、TTMの収益・純利益は分社の前の連結のまま（売上39.0B・純利益8.2B、PER 8.2〜9.4）。2026-10は10-Qの組み替えの後（売上33.6B・純利益7.05B）。`basis_mix`は`mixed=true`・`recast_filed=2026-10-22`で検知済み |

- 結論: `split_history.yaml`は「株数の比」と「株価の調整の比」を1つの`ratio`に入れているため、分社つきの併合で食い違う。HONは株数0.5が正しく、**株価の調整の比は別（0.9535）**。台帳は株価の側で分割前の月を判定不能にしているので、今の公開の値は正しい
- 案: `split_history.yaml`に`kind`（`split`・`reverse_split`・`spin_off`・`reverse_split_with_spin_off`）と`price_factor`（株価の系列に掛ける比。省略時は1/ratio）を足す。株数は`ratio`、株価の期待値は`price_factor`。HONは`kind: reverse_split_with_spin_off, ratio: 0.5, price_factor: 0.9535`。**この変更だけでは倍率の値は変わらない**（台帳がmismatch→okになり分割前の月が計算できるようになるが、その月の財務は分社の前の連結で基準が違うまま）。分割前の月を出すには「分社の前の連結」と「継続事業」の基準を分ける別の設計が要る（この件の範囲外）
- 同じ型の探索（Yahooの分割履歴 × split_history × Layer3の株数の跳び）:
  - 登録済みの分割で比が食い違う: HONのみ
  - 未登録でYahooが非整数の調整を持つ分社・特別配当の型: DD（2025-11-03 2.39・2026-06-24 0.3333）、BDX（2022-04・2026-02）、GE（2021-08 1:8逆分割・2023・2024）、FTV、MMM、AVB、CMCSA、FDX、SPGI、DELL、CTVA等。ただし日足がYahooと食い違うのは**CTVAだけ**（ほかは調整済み）
  - Layer3の株数が登録なしの分割で跳ぶ銘柄: 登録済み以外では、分割に対応する跳びは見つからなかった（跳びはIPO直後と登録済みの分割に限る）
  - 株数の単位が壊れて見える期（CIX 2026Q3・LOAR 2024Q2・FCX 2010・KO 2018Q1）は分割とは別→FINDINGS

## (3) Q4のfirst_filedが後の書類の日付になる件

- 数: 逆算Q4（`is_implied`）5,340件のうち、年次の`first_filed`が無い93件を除き、Q4の`first_filed`＞年次の`first_filed`が**197件（34銘柄）**。全件`q4_basis=latest`。期末の年: 2020 31・2021 101・2022 39・2023 13・2024 11・2025 2
- 銘柄: IPO・SPAC直後が大半（FROG・FRSH・GTLB・PLTR・QBTS各11、CART・FLYW・S 各10、BROS 9、RDW・RKLB・SPIR・ZETA・TASK 各8…）。IPO後でないものは1〜4件（HON RD 2022Q4、KO・PM・TDY・CIX・GOOGL・VRT・KULR・ONDS・ESTC・AVAV・CELH・SOFI）
- 原因: `implied_q4_value()`は、Q1〜Q3の値が年次の`first_filed`までに無いとき`q4_basis=latest`（最新どうしの差）に落ち、`first_filed`は`combine_first`で部品の`first_filed`の**最大**になる。IPO年は前年のQ1〜Q3が上場後の最初の10-Qの比較欄で初めて出る。IPO後でない例（KOのD&A 2024Q4 → 2025-10-23＝翌年Q3の10-Q）も、前年同期の単独の値が翌年の10-Qの比較欄にしか出ないため。「10-Qが無いQ4」ではなく「Q1〜Q3の単独の値が年次の後の書類にしか無い」
- 「正しい時点」: Q4の値が実際に組めた最初の日＝部品の`first_filed`の最大（今の定義）。年次の10-Kの日にするとQ4をその日から使えるが、組むのに後の書類の値を使うので先読みになる
- 案: **A**＝今のまま（`first_filed`は部品の最大、`q4_basis=latest`の印を読み手が見られる）。**B**＝年次の`first_filed`に揃える（先読み、試算用）。**C**＝年次の10-Kの前の10-Qの累計（YTD）の差から単独のQを作る（別の設計）。推奨はA
- 効く読み手: `common/multiples.py`・`common/profitability.py`（→`stage_v2.py`）・`src/tail/quarterly_review_generator.py`の3つ（grep）。`tanuki_valuation/pipeline.py`の`first_val`は組み替えの注記の表示だけで、理論株価・判定に効かない。TTM・normalizedは`val`
- 案Bの試算（`sim.py`、`build_ticker_store`の1,038エントリ〈全項目〉の`first_filed`を年次に揃えた）: 倍率の履歴（98銘柄×70か月）・定義v2の並行計算・収益の段階（98銘柄）とも**差0**。影響する月は、IPO直後（株数・株価・TTMが元々判定不能）と、4四半期のTTMの窓に入らない項目（SBC・D&A・RD等）だけ

## 読み手

| 読むもの | 読み手 |
|---|---|
| `daily/` | `common/multiples.py`・`stage_v2.py`・`rotce.py`・`tanuki_valuation/`の`pipeline.py`・`data_fetcher.py`・`score_verifier.py`・`beta_fetcher.py`・`hypecore/hypecore.py`・`stonks_silo/valuation_fetcher.py`・`adjusted_eps_analyzer/extract_key_facts.py`・Market Pulse（`breadth_calculator`・`sector_rotation`・`stage_conclusions`）・画面（tanuki_valuation・tanuki_scoreのindex.html） |
| `split_history.yaml` | `common/sec_data/split_adjust.py`・`multiples.py`・`rotce.py`・`report_consistency_check.py`・`adjusted_eps_analyzer/ai_analyzer.py`・`adjusted_eps_analyzer/pipeline.py`・`tanuki_valuation/pipeline.py` |
| `first_filed`・`first_val` | `multiples.py`・`profitability.py`・`quarterly_review_generator.py`（TAIL）・`stage_v2.py`（`multiples.py`の出力経由）・`tanuki_valuation/pipeline.py`（表示のみ） |

## 試算の範囲

- 基準（今のコード）と案B（Q4のfirst_filedを年次に揃える）で、`multiples.build`・`stage_v2.build`・`profitability.build`を全98銘柄で回した（出力はscratchpadだけ。Grok・SEC・Discordは呼ばない）。差は0
- 日足の案（APH・CTVAの割り戻し）は、この2銘柄が登録銘柄でないため、倍率・定義v2・収益の段階・HypeCore・TANUKIの入力に入らない（コードの確認）。**TANUKI→HypeCore→TANUKIの連鎖は回していない**（入力が変わらないため）。HONの`kind`・`price_factor`は値を変えない設計なので、これも連鎖は回していない
- 注: 基準の出力は公開の`docs/common/multiples/`と64銘柄で違う。今日の日付・株価で作り直したための差で、案とは無関係（案Bと基準は同じ条件で比べた）

## PARSER-LAYER3 段1・CONTINUITYとの重なり（どちらも未公開）

| 変更したい所 | PARSER-LAYER3 段1（revert済み: `0544696ddc`・`1ebce36a25`） | CONTINUITY（ブランチ`tanuki-calc-continuity-1`） |
|---|---|---|
| (1) 日足の割り戻し（新しい処理） | 重ならない | 重ならない |
| (2) `split_history`の`kind`・`price_factor` | 重ならない | `tanuki_valuation/pipeline.py`を触るが別の箇所（逓減モデル）。`split_adjust.py`はどちらも触らない |
| (3) 案Aの注記 | `layer3_builder.py`（同じ`first_filed`の周辺）を触る | `q4_implied.py`を触る（`q4_inputs`が`implied_q4_value`の分岐を複製しているので、Q4の`first_filed`の規則を変えるなら同期が要る） |

- 公開の順の案: ①(1)の日足は独立、いつでも（Market Pulseの表示が直る。理論株価・判定は動かない） ②PARSER-LAYER3 段1とCONTINUITYの公開を先に ③(2)・(3)は②の後。(3)は案Aならコード変更なし（読み手の注記だけ） ④(2)の`kind`は、定義v2の切り替え（MULTIPLES-ADOPT-1・STAGE-V2-ROLLOUT-1）の前に入れる

## 決定（2026-11-12、チャット側）と実装

- (a) **週1回**: `common/market_data/split_reconcile.py --apply` をMarket Data Weeklyの段に足した（属性・アナリストの取得の後、倍率の作り直しの前）。UPDATE_SCHEDULE.mdに反映
- (b) **台帳だけ**: `config/split_price_ledger.json` のHONの行に `price_factor: 0.9535` と理由を記録。`split_history.yaml` は変えない。台帳は機械で作り直すため、`multiples.carry_annotations` が前の台帳の同じ（銘柄・分割の日）の行の `price_factor`・`price_factor_reason` を引き継ぐ。mismatchの扱いは今のまま（分割前の月は判定不能）
- (c) **案A**: Q4の `first_filed` は部品の最大のまま・コード変更なし。`q4_basis=latest` の印が読み手に見える

### (1) の実装の結果（手元の全銘柄）
- 走査580銘柄（指数・先物・為替を除く）、当たりは **APH（比2、境2026-09-03、1,423/1,472行）とCTVA（比6.665、境2026-10-01、1,442/1,452行）の2銘柄だけ**。出来高は、APH・CTVAとも保存した出来高が調整前で、Yahooの出来高は比を掛けた値なので、出来高にも比を掛ける
- CTVAの日足が10-14で止まっている件: 割り戻しとは関係しない。CTVAは分社の後に S&P 500 の控え（`_sp500_constituents_cache.json`）から外れ、毎日の取得の対象でなくなったため。Yahooは11-11まで返す。割り戻しの走査は `daily/` にあるファイル全部を見るので、CTVAも対象になる。直さない（FINDINGSの行に原因を追記）

## 決めてほしいこと（STEP 0の時点。上の決定で解決済み）

- (a) (1)の処理を週1回のMarket Data Weeklyに入れるか、分割の検知（CHECK-54）の後に呼ぶか
- (b) (2)でHONの株価の調整の比を`split_history`に持たせるか、台帳だけに持つか
- (c) (3)は案A（コード変更なし）でよいか
