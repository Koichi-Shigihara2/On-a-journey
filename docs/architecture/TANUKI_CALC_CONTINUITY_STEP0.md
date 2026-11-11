# TANUKI-CALC-CONTINUITY-1 STEP 0（読み取りと手元の試算のみ。実装・公開なし）

2026-11-12。範囲は BACKLOG [[TANUKI-CALC-CONTINUITY-1]] の(1)〜(3)。先に [[GROWTH-SANITY-CONTINUITY-1]]（BACKLOG_DONE）の考え方（境目で方法を切り替えず、入力が少し動けば値も少し動く）と連続性のテスト（`tests/test_growth_sanity_continuity.py`、1刻みあたりの動きが10倍以下）を読んだ。
試算は `tools/sim`（TANUKI→HypeCore→TANUKI、前＝今のHEAD `f35ef8ec90`）。コードの変更はcommitしていない（作業ツリーで試算し、終わりに戻した）。
表: `TANUKI_CALC_CONTINUITY_STEP0_boundaries.csv`（境目の一覧 172行）・`_near.csv`（境目の近くの銘柄）・`_movers.csv`（動く銘柄・必須6銘柄・保有9銘柄と原本での理由）・`_q4neg.csv`（負の逆算Q4の17か所）。

## 1. 境目の洗い出し（grep）
15ファイル（adjustments・growth_sanity・wacc・growth・scenarios・dcf・fcfe・rice・sensitivity・future_values・core_calculator・pipeline・maturity_config・q4_implied・layer3_builder）の `if/elif` で数値や定数と比べている172行を機械で拾い、分類した（`_boundaries.csv`）。拾えるのは1行の比較だけで、複数行の条件（taper）と変数名の閾値は別の正規表現で足した。

| 分類 | 行数 | 例 |
|---|---|---|
| 保護（0以下・分母） | 88 | `if wacc <= 0` など。値の切り替えではない |
| 判定・スコア・表示の帯 | 38 | upside・F&G・希薄化・PEG・PS・ERP の段。設計上の段 |
| データ品質の保護 | 12 | 持ち株数の異常・分割検知・YTD組み替え |
| 表示のみ | 10 | 推奨成長率の signals／warnings の倍率の段、ラベル |
| 連続（clamp） | 2 | 上限・下限 |
| **方法の切り替え（値が飛ぶ）** | **11** | 下表 |
| 要確認（読むだけ） | 11 | RICE の `INTENSITY_MIN`・`CF_POINT_MAX`・`Q_MAX`、`pipeline` の黒字化・RF の古さ等。近傍の数は未計測 |

**方法の切り替え11か所**と、境目の±10%にいる銘柄数（公開中の latest.json の値。`_near.csv`）:
| # | 場所 | 切り替え | ±10%の銘柄 | 今回 |
|---|---|---|---|---|
| B1 | `determine_fcf_base` CV 0.5 | 5年平均 ↔ 直近2年平均 | **9**（CAKE・CIX・DOCN・FCX・HON・KO・LOAR・TER・TSLA。CV 0.45〜0.55） | (1)で連続化 |
| B1' | `analyze_fcf_outlier` の閾値（CV≤0.5で20%、超で60%） | 外れ値の検出幅 | 9（同じCV） | (1)に含める（同じ重み） |
| B2 | 直近2年平均が5年平均の15%未満 | 5年平均に戻す | 0 | 入れない |
| B3 | FCFのCAGR −5% | 直近1年／5年平均（回復） | 2（ABBV −5.1%・LITE −5.0%） | 入れない |
| B4 | 外れ値の乖離が閾値超 | 注記（除外されると基準が残り年の平均に） | 4（BKNG・BSY・CAT・VZ。乖離21%前後） | 入れない（注記だけ） |
| B5 | FCFマージン悪化 2.0pt | BEARの乗数 1.0→最新/3年平均 | 4（CAT・CIX・FLYW・KLAC） | **試算したが入れない**（§2.4） |
| B6 | R&D資本化 R&D/売上 5% | 適用なし ↔ 調整額を全額 | 1（TDY 5.19%） | 入れない |
| B7 | 直近が赤字・2年平均≤0 | 5年平均 | 符号の境目 | 入れない |
| B8 | taper を s>0 のときだけ（(3)③） | s=0で逓減なし ↔ s=0+で終点=推奨成長率 | s=0 の全銘柄でBULLが飛ぶ | (3)で連続化 |
| B9 | 逆算のQ4が負なら欠け（5項目だけ） | 欠け ↔ 負の値 | 既存9項目で17か所 | (2)で9項目に |
| — | `growth_blend_s`（逓減モデルとの混合） | 連続化済み | 途中の銘柄3 | 済 |

## 2. 案と全銘柄の試算
### 2.1 (1) CVでの切り替え（B1・B1'）
両案とも `base = (1−w)·5年平均 + w·2年平均`、`w = smoothstep((CV−下端)/(上端−下端))`、外れ値の閾値も同じ w で 20%→60%。CV の特殊ケース（赤字・15%未満・CAGR−5%・データ不足の経路）は触らない。
| 案 | 傾斜の幅 | 理論株価が変わる銘柄 | 判定 | 5%超 |
|---|---|---|---|---|
| **A（推奨）** | CV 0.40〜0.60 | 11（SCCO・SNPSは0.04%） | 0 | CAKE −22.6%・DOCN −13.1%・LOAR −10.8%・KO +7.5% |
| B | CV 0.45〜0.55 | 6 | 0 | CAKE −20.5%・DOCN −10.2%・LOAR −7.5%・KO +6.3% |
- 動く量が大きいのは、今の値が境目の上下で5年平均か2年平均のどちらかに全量寄っているため（CAKE: 5年平均125.0M と 2年平均189.4M の差が51%）。傾斜をなだらかにすると、境目の両側の銘柄が中間に寄る。Bでも5%超が4銘柄あり、傾斜の幅は動く銘柄の数だけを変える（11か6）。
- Aを勧める理由: CVは四半期ごとに0.1程度動く（HQYは0.48→0.58）。傾斜がBの幅（0.1）だと1四半期で端から端まで動く。Aは1刻みあたりの動きが小さい。
- 動く銘柄の原本の確認は `_movers.csv`。入力のFCF（OCF−capex）は10-Kの申告と一致（CAKE・KO・TER・HWM・HEI・HQY）、DOCN・LOARはcf_linesのcapex（ソフト開発費を含む）の定義どおり。

### 2.2 (2) 非負の項目の逆算Q4（B9）
`config/sec_concept_definitions.json` の9項目（buyback・SBC・R&D・販売費・SG&A・売上・売上原価・粗利・D&A）に `"non_negative": true` を足した（コードの変更なし。`q4_implied.non_negative_fields()` が読む）。全銘柄の Layer3 で負のQ4は **17か所**（`_q4neg.csv`、BACKLOGの19と数え方が違う）、TTMの窓内は5か所（APP販売費・FICO自社株買い・FRSH SBC・KULR粗利・TASK自社株買い）。
- **理論株価・判定・保有・stage・STONKS・調整後EPSの変化は0**（(2)だけの試算も0）。ttm の実質の変更は11ファイル（APP・ASTS・BBAI・BSY・DELL・FICO・FRSH・HWM・ONDS・TASK の ttm と RCAT の normalized）。
- 表示に出る変化: FICO の `buyback_ttm` 3,046M→欠け、TASK `buyback_ttm` 1,000→欠け、**FRSH の SBC 117.9M→欠けで RICE 0.112→0.175・max_eps 1.168→0.746（信頼度 HIGH→MED）**、HypeCoreの ASTS は売上のQ4（−1,000ドル）が欠けて rev_yoy 等が34か月ぶん動く（stage は不変）。
- **原本で確かめた結果、規則に直すべき点が2つある（決めてほしい）**:
  1. **粗利は実際に負になる**（KULR: 10-K FY2025 粗利771K＜10-Q 9か月1,549K＝Q4が−778K。原価が売上を超える）。粗利は非負の項目から外すのが正しい（9→8項目）。
  2. **千ドル単位の丸めの負**（TASK −1,000、FICO −1,000、ASTS −1,000／−2,000。10-K年間27,782K＜10-Q 9か月27,783K など）で、FICOの自社株買い3.05Bのような大きなTTMが丸ごと欠ける。許容幅（`|Q4| ≤ 年間の0.01%` など）を足し、丸めの範囲なら逆算の値のまま残す案。0で埋めるのではなく逆算の値（−1,000ドル）を残すので決定(c)に反しない。
  - 残りの負（APP販売費は2026年の10-Qで組み替え済みのQ1 59.4M〈2025年の10-Qは183.0M〉と10-K年間203.7Mの基準の混在、FRSH SBCは同じ9か月の値が2つの10-Qで同一なのに10-K年間が小さい申告の食い違い、DELL・HWMの2020〜22年は再表示）は欠けが正しい。

### 2.3 (3) check_growth_sanity の残り
- ①表示名「中央値モデル」→「重み付き平均モデル」: `pipeline.py` 4か所（`中央値ベース`・`中央値を自動適用`・`中央値モデル`・内訳の `中央値ベース:`）と `stock.html` 2か所。テスト `tests/test_pipeline_logic.py:667` の期待文字列も直す。キー名（`recommended_g_median`・`growth_model: "median"`）は残す（読み手が多い）。
- ②CHECK-52（`report_consistency_check.py` 1220行）: 「recommended_gが算出できずfcf_cagr floor(15%)に転落するおそれ」→「推奨成長率は候補が少なく、不足分を max(過去のFCF成長率, 0) で埋めた値（候補の重みの合計が2に届かない）」。`warn_acknowledged.json` の `match="候補数=N件"` は残す。
- ③taper: `tapering_g_end` を `s=0` でも `rec`（推奨成長率）で渡す（業種がNoneのときも `tapering_g_end_continuous` が `DECAY_END_DEFAULT` を使う）。BULLが推奨成長率より高いときだけ逓減が効くので、`s=0`→`0+` が連続になる。
- **理論株価・判定・保有の変化は0**（BASEは推奨成長率＝逓減の終点で逓減しない）。BULLの理論株価は52銘柄で下がる（ADSK 525.6→474.4〈−9.7%〉・FICO 1,879→1,623〈−13.6%〉など）。表示・BULLだけ。

### 2.4 1.で見つけた境目のうち足したもの・足さなかったもの
- B1'（外れ値の閾値）は(1)と同じ w で連続化した（CIX の外れ値の注記が「要確認→なし」に変わるなど）。
- **B5（FCFマージン悪化のBEAR乗数）は試算して入れない**。`gap/2.0` の傾斜で連続化すると理論株価は0銘柄だが、BEARの再計算（`pipeline.py` 1205行）が**推奨成長率の上書きを引き継がない**ため、経路に入る銘柄が増えるとBEARが不整合になる（CAT: BEAR 成長率 7.2%→13.2%・理論株価 285→401、CIX: BEAR 2.9%→9.1% で BASE 4.2%より高い）。既存の食い違いなので FINDINGS に登録し、直してから連続化する。
- B2〜B4・B6・B7は近い銘柄が0〜4で、直しても値が動かない（B4は注記だけ）か再設計になるため入れない。

### 2.5 全部入り（(1)案A＋(2)＋(3)）の結果
| 見るもの | 結果 |
|---|---|
| 理論株価 | 変化 **11**（上の表）。5%超は CAKE −22.6%・DOCN −13.1%・LOAR −10.8%・KO +7.5% の4銘柄。残り85銘柄は ±0.0% |
| 判定 | 変化 **0** |
| 保有9銘柄 | ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA すべて ±0.0%、判定の変化 0 |
| HypeCore stage | 変化 **0**（96銘柄の stage 列が一致）。12銘柄は最新月の `price_iv_ratio` だけ動く。ASTS は(2)の売上欠けで fundamental_score 等が34か月ぶん動く（stage は不変） |
| STONKS | 変化 **0**（出力一致） |
| 調整後EPS | 変化 **0**（285ファイル一致） |
| scope（公開するとき） | 19銘柄（理論株価・判定等が変わる13＋表示が変わる6）。`sim.py scope`、書き込みは公開のとき |
| 必須の6銘柄 | HQY −1.4%・DOCN −13.1%・TASK／FRSH／APP／KULR は ±0.0%（FRSH は RICE・max_eps が動く）。理由は `_movers.csv` |

## 3. 公開の順 — PARSER-LAYER3 段1（未公開）の上に重ねる必要はある
段1の差分（revert `0544696ddc`・`1ebce36a25` を戻した状態）を作業ツリーに当て、段1の置き換え685セルと派生の作り直しも含めて、(1)案A＋(2)＋(3)を重ねて試算した（`stk`）。
| | 段1だけ | 今回だけ | 重ねた結果 | 掛け合わせの見込み |
|---|---|---|---|---|
| **CAKE** | +29.1%（38.75→50.00） | −22.6%（→29.99） | **+6.5%（→41.25）** | 0.0%（食い違う） |
| KO | −3.2% | +7.5% | +4.3% | +4.0% |
| HWM | −1.1% | −3.1% | −4.2% | −4.2% |
| APP | −0.1% | ±0.0% | −0.1% | −0.1% |
- 重なるのは CAKE・KO・HWM・APP の4銘柄。CAKEだけ食い違うのは、段1が純負債（加算）、今回がFCF（倍率）を動かし、掛け算にならないため。判定の変化は重ねても0、保有はAPP −0.1% のまま、stage の変化も0。
- **結論: 先に出した方の上でもう一方を試算し直す**（`sim.py chain` で1回、約30分）。(a)の一覧は後に出す方でCAKE・KOが変わる。順は段1が先を勧める（段1の入力の修正は事実の訂正で、今回は方法の選び方の変更。CAKEの「今回だけ」−22.6%は段1の後の基準では−17.5%〈50.00→41.25〉になる）。順を逆にすると段1のCAKEは+37.5%〈29.99→41.25〉。
- 段1を先に公開しない（手順31の確認を待つ）場合でも、今回の公開前の試算し直しは上の `stk` の手順で足りる。

## 4. 決めてほしいこと
1. (1) の傾斜の幅: **A（CV 0.40〜0.60、11銘柄）を推奨**／B（0.45〜0.55、6銘柄）。
2. (2) の規則: **粗利を非負の項目から外す**（9→8項目）と**千ドルの丸めの許容幅**（FICOの自社株買い3.05B・TASK・ASTS）を入れるか。入れない場合は FICO の buyback_ttm が欠けになる。
3. 公開の順: 段1が先か、今回が先か（上のとおり、後に出す方は試算し直し）。

## 5. 範囲外の発見（FINDINGS.md）
- BEARの再計算が推奨成長率の上書きを引き継がない（B5）。
- 逆算Q4の非負の規則の項目選び: 粗利は実際に負になる／千ドルの丸め。

## 6. 実装済み（ブランチ・未公開）— 2026-11-12
ローカルブランチ `tanuki-calc-continuity-1`（`f35ef8ec90` から、commit `1a6f23f12c`）。kaihatsu・originには**入れていない**（このdocsだけ kaihatsu）。決定（§4）: (1)は案A（CV 0.40〜0.60、B1'も同じ重み）・(2)は8項目（粗利を除く）・(3)はSTEP 0のとおり・B5は入れない。
- 実装: `adjustments.py`（`CV_RAMP_LO/HI`・`cv_weight`・`determine_fcf_base`の最後を傾斜に・外れ値の閾値を同じ重みで）・`sec_concept_definitions.json`（8項目に`non_negative`）・`pipeline.py`（表示名・taperを s=0 でも終点=推奨成長率）・`stock.html`・`report_consistency_check.py`（CHECK-52の文言）・`q4_implied.py`（コメント）。既存テスト3か所を直した（`test_layer3_capital_allocation`の非負の項目・負のQ4の例を粗利に、`test_first_filed`の負のQ4の例を粗利に、`test_pipeline_logic`の表示名）。
- 連続性のテスト `tests/test_tanuki_calc_continuity.py`（6件。B1・B1'・B8・B9、`test_growth_sanity_continuity.py`と同じ物差し〈1刻みあたりの動きの上限を理論上の上限の10倍〉）。今の規則に戻すと5件が失敗する（B1×3・B8の配線・B9）。
- 試算（STEP 0と同じ chain。前＝`f35ef8ec90`）: **STEP 0の全部入りと一致**。理論株価の変化11・判定0・保有±0.0%・stage 0・STONKS 0・調整後EPS 0・scope 19銘柄。理論株価・判定は全96銘柄で STEP 0 と同じ値、latest.json 全体の差は ASTS だけ（粗利を非負から外したため）。ttm/・normalized/ の実質の変更は11ファイル（STEP 0と同じ。作り直した派生ファイルは戻した）。
- **丸めの許容幅は、この時点では実装していなかった（6.1で案1を実装）**。申告の `decimals` は `company_facts.json` に入っておらず（SECのcompanyfacts APIが返さない）、Layer3にも無い。決定（decimals が無いときは案を出して止める）に従い、今は千ドル単位の丸めの負も欠けになる。
  | 銘柄 | 厳密（今の実装） | 丸めを許したときの値 |
  |---|---|---|
  | FICO | `buyback_ttm` 欠け（TTM 2026-09-30） | 3,045,991,000（旧と同じ） |
  | TASK | `buyback_ttm` 欠け | 1,000（旧と同じ） |
  | ASTS | 売上のTTM（2023-06-30の点）6,561,000が欠け、HypeCoreの rev_yoy 等が34か月ぶん動く（−82.7→−88.1。stage不変） | 旧と同じ |
  理論株価・判定はどちらでも同じ（FICO 1,370.64・TASK 16.18・ASTS 3.60）。
  - **案**（実装しない）: (案1・推奨) 申告の粒度を値から決める。年次とQ1〜Q3の値がすべて1,000の倍数なら粒度1,000とし、`|逆算Q4| < 粒度`は丸めとみなして逆算の値（−1,000ドル）を残す（0で埋めない。decimals=−3 と同じ意味で、手元のデータだけで決まる）。TASK・FICO・ASTSの値は全て1,000の倍数。(案2) XBRLインスタンスから decimals を取る（SECの取得が要り、FCF段5の枠）。(案3) `|Q4| ≤ 年間の0.01%`（粒度の根拠がなく恣意的）。
- ゲート: `env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider --deselect tests/test_common_path_guard.py::test_no_open_without_encoding_ruff_plw1514`（`PYTHONUTF8=1`、ブランチ上）。2,954件が通り、失敗は SITM の1件（既知の例外）。rcc は NG 0・WARN 215、audit は通過。

### 公開の手順（段1の公開の後）
1. 段1の公開（手順31の確認の後）が済んでから、ブランチを `kaihatsu` に rebase（`git rebase kaihatsu tanuki-calc-continuity-1`）。衝突しやすいのは `pipeline.py`・`config/sec_concept_definitions.json`・`stock.html`（段1も触る）
2. **段1の上で試算し直す**（`sim.py chain`、前＝段1の公開後のHEAD。§3のとおりCAKE・KO・HWM・APPは掛け合わせにならない）。派生ファイルは `tools/sim/regen_derived.py` → `tools/sim/revert_noise.py` で作り直す
3. 決定(a)の一覧（5%超・判定が変わる・保有・HQY/DOCN/TASK/FRSH/APP/KULR・CAKE/KO）を段1の上の値で作り直し、原本の申告で理由を確かめる
4. 出力の一覧（INV-5）・派生ファイルをcommit→`sim.py scope --write`（ID `TANUKI-CALC-CONTINUITY-1-1`）→TANUKI→HypeCore→TANUKI の順で公開
5. （丸めの許容幅は 6.1 のとおりブランチに入れた）

### 6.1 丸めの許容幅（案1）を足した — 2026-11-12（ブランチ commit `7b8a432d2b`）
- 規則（`q4_implied.rounding_grain`）: 逆算に実際に使う年次・Q1〜Q3の値がすべて1,000の倍数なら粒度1,000。負のQ4が非負の項目で `|Q4| ≤ 粒度` なら、逆算の値のまま残す（0で埋めない。印 `q4_rounded: 1000`）。倍数でない値が1つでもあれば粒度なし。
- **決定の文言（`|Q4| < 粒度`）から1か所変えた: `≤`**。TASK・FICO・ASTSの負のQ4はちょうど −1,000ドル（10-K年間27,782K＜10-Q 9か月27,783K）で、`<` では残らず「旧と同じ」にならない。丸めた年間と丸めた累計の差は最大で粒度1つぶん（各±0.5）なので `≤` が丸めの範囲。粒度を超える負（−2,000ドル）は欠け。
- テスト（`tests/test_tanuki_calc_continuity.py`）: 丸めの負は残る（TASK型 −1,000）／粒度を超える負は欠け（−2,000）／1,000の倍数でない値があれば −1 でも欠け。
- 試算（STEP 0と同じ chain。前＝`f35ef8ec90`）: 理論株価・判定は全96銘柄で STEP 0 の全部入りと一致（11銘柄・判定0・保有±0.0%）、HypeCore stage 0、STONKS 0、調整後EPS 0、scope 16銘柄（19→16。FICO・TASK・ASTSの3銘柄の表示が旧に戻った）。ttm/・normalized/ の実質の変更は9ファイル（11→9。FICO・TASKが旧と同じ）。
- **FICO・TASK・ASTSは旧と同じ**: TASKは latest.json 全体が旧と一致。ASTSも latest.json・HypeCoreの出力（34か月ぶんの rev_yoy 等）が旧と一致。FICOは `buyback_ttm` 3,045,991,000 が旧のままで、latest.json の差は BULL の理論株価（(3)のtaper）だけ。
- ゲート（ブランチ上）: 同じコマンドで2,955件が通り、失敗は SITM の1件（既知の例外）。rcc は NG 0・WARN 215、audit は通過。
- 公開の手順（上）の5は不要になった（許容幅は同じブランチに入っている）。
