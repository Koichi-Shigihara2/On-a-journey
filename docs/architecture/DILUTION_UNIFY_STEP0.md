# DILUTION-DEFINITION-UNIFY-1 STEP 0（2026-11-12、読み取りと手元の試算のみ）

コードのcommit・push・公開は無し。表は `DILUTION_UNIFY_STEP0_table.csv`（全98銘柄）。道具はscratchpadの`dil/`（`a1.py`＝TANUKIの年次の再現と四半期の株数・`a3.py`＝表・`hook_dil.py`＝試算フック・`sim/`＝`tools/sim`の出力）。

## 0. ゲート

`PYTHONUTF8=1 env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider` → **2960件通過・失敗2・skip 6**。失敗2は既知の`test_stonks_flag_rule`（SITM）と、この環境にruffが無い`test_no_open_without_encoding_ruff_plw1514`。ほかに失敗なし（前の依頼で出た`test_update_schedule`は直し済みで通る）。

## 結論

| 見るもの | 結果 |
|---|---|
| 希薄化の定義 | 「希薄化」という名前で動いているのは**2つ**（TANUKIの年次の株数の3年の変化・STONKSのSBC÷売上）。定義v2の「増資÷時価総額」はコードに無い（設計書の記述と、Layer3の5項目の部品だけ） |
| 概念 | 3つは別の概念。**株数の変化**（TANUKI）・**SBC負担**（STONKS）・**増資による資金調達**（定義v2）。1つに寄せるのは株数の変化の部品だけで、あとは名前を分けて残す |
| TANUKIの値のずれ | 98銘柄のうち、四半期で測った株数の変化と2pt以上ずれるのが**25銘柄**（ほかに単位異常のCIX 1）（残り59は一致、欠け・比較不能13、単位異常1）。原因は「年次の窓が古い」と「年次の加重平均と四半期の系列の差」 |
| 試算（全銘柄、TANUKI→HypeCore→TANUKI） | 理論株価の変化 **0**・判定の変化 **0**・保有9銘柄の変化 **0**・HypeCoreのstageは時刻以外同じ。動くのは希薄化率の表示（85銘柄の値、うち2pt以上26）・重大度の表示（18銘柄）・fundaの点（5銘柄: IONQ・QBTS・RDW・RXRX・ZETA） |

## 1. 希薄化の定義の洗い出し（grepと読み取り）

| 名前 | 書き手 | 定義 | 読み手 | 何に効くか |
|---|---|---|---|---|
| `financial_health.dilution_3yr_annual_pct` | `tanuki_valuation/pipeline.py` `_load_extra_data`（L3118〜3230付近） | Layer3の`shares_diluted`の**年次（FY）**の最新と3つ前の比の年率。登録済みの分割は`split_history.yaml`で調整し、さらに前年比2.5倍超・四半期中央値との乖離で分割を判別するヒューリスティックが残る。SECの株数とyfinanceの株数が10倍以上離れると欠け | pipeline L919（funda点: >20%で−15・>40%で−25）、report（Dilution_3yr_Annual・重大度）、`dilution_severity`・`dilution_comment`、画面`stock.html`、`tanuki_score/index.html`（成長の罠の判定: >5%） | **funda点→スコア・判定**、表示 |
| `dilution_risk`（STONKS） | `stonks_silo/analyzer.py` L296〜302 | **SBC÷売上**（15%以上HIGH・8%以上MEDIUM） | STONKSの画面（`stonks-silo/index.html`「希薄化リスク」） | 表示のみ。株数とは別の概念 |
| 増資÷時価総額（定義v2） | コードに無い。`HYPECORE_STAGE_DEFINITION_V2.md`の③の裏づけの記述と、Layer3の5項目（`equity_issuance_proceeds`・`stock_issued_acquisitions`・`option_exercise`ほか、`config/sec_concept_definitions.json`・`q4_implied.py`）だけ | 増資の収入 ÷ 時価総額（設計） | 今の読み手なし（`stage_v2.py`・`multiples.py`・`profitability.py`は読まない） | 影響なし |
| `max_eps_shares`・`diluted_shares` | `pipeline.select_max_eps_shares`・`data_fetcher` | EPSの分母・1株価値の分母（今の完全希薄化後株数） | 理論株価 | **希薄化の「率」ではなく株数そのもの**。この件の範囲外（`EPS-SHARES-BASIS-MISMATCH-1`で別に決着） |
| `yf_outstanding_post_dilution`（`data_fetcher.py` L1228） | `data_fetcher.py` | yfinanceの発行済株数がSECの10倍以上のとき採用する株数の出所の名前 | 株数の選択 | 名前に「dilution」を含むだけ。別件 |

ほかのgrep結果（`common/sec_data/utils.py`のコメント等）は希薄化の定義ではない。HypeCore・stage・調整後EPS・倍率・収益の段階は希薄化率を読まない。

## 2. TANUKIの希薄化率と実際の株数の変化（全銘柄、`DILUTION_UNIFY_STEP0_table.csv`）

比べた列:
- **TANUKI今** = latest.jsonの`dilution_3yr_annual_pct`。**年次の再現** = Layer3の年次の末2点（分割は`split_history.yaml`で調整）。全98銘柄でTANUKI今と一致（ヒューリスティックの分割判別は今の公開に効いていない）
- **年次と同じ時点の四半期** = 年次の末の日付に最も近い四半期の加重平均の希薄化後株数で測った3年の年率（系列だけを変えた比較）
- **直近四半期÷12四半期前** = 案の定義。加重平均の希薄化後株数、分割は同じ基準で調整
- **dei** = 各社の10-K・10-Qの表紙の発行済株数（`EntityCommonStockSharesOutstanding`）の約3年の年率（独立した確認）

ずれの分類（TANUKI今と案の差が2pt以上、25銘柄＋CIX）:

| 分類 | 銘柄 | 意味 |
|---|---|---|
| 窓が古い＋年次と四半期の系列差（11） | BBAI・BROS・CRM・IONQ・JOBY・KULR・LITE・ONDS・RDW・RXRX・SITM | 年次の末（多くは2025-12-31）から直近の四半期（2026-06〜09）の間に株が増えた。さらに年次の加重平均と四半期の系列が違う |
| 窓が古い（6） | ADBE・COHR・FROG・SNPS・SPIR・ZETA | 年次の末が最大約12か月前（FYが10〜11月末のSNPS・ADBE等）。系列は一致 |
| 年次の加重平均と四半期の系列の差（6） | CART・CEG・DOCN・LYFT・QBTS・SOFI | 同じ時点でも年次の値と四半期の値が合わない（転換社債のif-converted・黒字化・買収の対価の株が期の途中で入る） |
| その他（2） | CAKE・FRSH | 差2.5〜3.5pt。年次の末付近の四半期の揺れ（原因は未特定） |
| 単位異常（1） | CIX | 2026Q3の株数が前期の0.001倍（FINDINGS 2026-11-12） |
| 分割の調整 | **該当なし** | 登録済みの分割14銘柄は、3年の窓で`split_history`が効いていて、TANUKI今と案のずれの原因になっていない（`split_adjusted`の印が付く銘柄も0） |
| 自社株買いとの相殺 | 区別できない | 希薄化後の加重平均は発行と買い戻しの差し引き後の株数。総発行を分けて測るには増資の収入・自社株買いの別の部品が要る（案の「増資」の部品） |
| 欠け・比較不能（13） | ASTS・BKNG・CRWV・LOAR・RKLB・V・XOM・ZS（TANUKI今が欠け。年次4点未満か10倍の乖離）、ALAB・GEV・GOOGL・RBRK・RCAT（四半期13点なし） | 案は四半期13点を条件にするので、年次4点未満で欠けていた銘柄が出る可能性がある（今回の試算は元が値を出した銘柄だけを置き換えたため未確認） |

### TSLA・LITE・CEGを原本の申告（`company_facts.json` = SECの申告のXBRL）で確かめた

| 銘柄 | 申告の事実 | 原因 |
|---|---|---|
| **TSLA** | 表紙の発行済株数 3,225M（2025-07-17）→ 3,326M（2025-10-16）→ 3,752M（2026-01-23）→ 3,950M（2026-07-16）＝**+22.6%**。希薄化後の加重平均は FY2024 3,498M → FY2025 3,528M → 2026Q3 3,541M（+1.2%）。基本の加重平均 3,197M → 3,240M | 発行済株数が増えた一方、加重平均（基本・希薄化後とも）はほぼ横ばい。増えた分は未確定の株（制限付株式の新規発行）とみられ、確定するまで加重平均に入らない（内訳は10-Kの本文で未確認、数値からの推定）。TANUKIの+0.51%は申告の加重平均として正しいが、「実際の株数」とは別の量。案でも+0.70% |
| **LITE** | 表紙 69.9M（2025-08-12）→ 71.4M（2026-01）→ 77.8M（2026-04-30）→ 89.7M（2026-08-14）→ 90.7M（2026-10-28）＝+30%。希薄化後の加重平均 FY2025 69.6M、**FY2026（2026-06-27）74.6M**、2026Q1（2026-09-26）**101.7M**。期末の発行済 88.6M（2026-06-27） | 年次の窓が古い（FY2026の年間平均74.6Mは年の前半が70M台で、年末に転換社債が株になった後の水準が反映されていない）＋2026Q1の希薄化後はif-convertedの転換株を含む（基本 92.5M との差）。案は+14.3%/年 |
| **CEG** | 表紙 312.3M（2025-10-31）→ **362.0M（2026-02-01）**→ 354.3M（2026-07-31）＝+13%。希薄化後の加重平均 FY2025（2025-12-31）314M → 2026Q1 **354M** → 2026Q2 357M | 年次の末（2025-12-31）の直後に約50Mの株が発行された（買収〈Calpine〉の対価の株とみられる。本文は未確認、BACKLOG登録時の記述と整合）。年次の窓が古く、TANUKIは**−1.54%**（買い戻しの減少）を示すが、直近の四半期では+13%。案は+3.15%/年（3年の年率だと買収の分が薄まる）。deiの3年の年率は+3.28% |

結論: TANUKIの値は「年次の最新と3つ前」という窓のため、**年次の末より後に起きた株数の増加**（LITEの転換・CEGの買収・IONQ・RDWの増資）を、公開の間ずっと拾わない。年次の末から直近の四半期までは最大約12か月ある。

## 3. 案

### 3-1. 「希薄化」を1つの部品に寄せる

概念ごとに名前を分け、**株数の変化だけを1つの部品**にする。

| 部品（案） | 定義 | 使う所 |
|---|---|---|
| **`share_count_change`**（新設、`common/`の共通の部品） | 直近の四半期の加重平均の希薄化後株数 ÷ 12四半期前の年率（3年）と、8四半期前の累計（2年）。分割は`split_history.yaml`（株数の比）で調整＝POINT-IN-TIMEの決定と同じ基準（株価の調整の比は台帳の`price_factor`で別）。13点が無ければ欠け（年次へ落とさない） | TANUKIの`dilution_3yr_annual_pct`（キー名は変えず、定義だけ差し替える）、成長の罠の判定、funda点、report・画面 |
| **`sbc_burden`**（STONKSの`dilution_risk`の改名） | SBC÷売上（今のまま）。名前を「SBC負担」にして、株数の希薄化と別の概念にする。閾値（15%・8%）は変えない | STONKSの画面（ラベルの変更だけ） |
| **`equity_raise_ratio`**（定義v2用、まだ作らない） | 増資の収入 ÷ 時価総額。Layer3の5項目が部品。stageの裏づけの材料の段階で作る | 定義v2（今は読み手なし） |
| 補助の印（案） | `outstanding_gap_pct` = 表紙の発行済株数 ÷ 直近四半期の加重平均 − 1。10%を超えたら表示だけの印（TSLA +11.6%〈制限付株〉・LITE −10.9%〈転換前後の差〉）。funda点には使わない | 画面の注記 |

- 分割の調整: `share_count_change`は`adjust_share_points`（`split_history.yaml`）だけ。年次のヒューリスティック（前年比2.5倍超の判別）は未登録の分割の安全網として、四半期の列でも残すかは決めてほしい（今の公開では効いていない）
- 窓の取り方: 3年の年率だけだと、LITEの+30%/2年のような最近の急増が薄まる（CEG 3.15%/年）。表示は3年の年率と2年の累計の両方を出し、**funda点は今のしきい値（>20%・>40%）のまま3年の年率で判定**する（点の意味を変えない）。2年の累計を点に使うかは別の判断

### 3-2. 影響する読み手（案を入れたとき）

`pipeline.py`の`_load_extra_data`と、それを読む report・funda点・`dilution_severity`・`dilution_comment`・`stock.html`・`tanuki_score/index.html`。HypeCore・stage・STONKS（改名のラベル以外）・調整後EPS・倍率・収益の段階は読まない。

## 4. 全銘柄の試算（`tools/sim`、TANUKI→HypeCore→TANUKI）

案（3-1）の`share_count_change`を、TANUKIの希薄化率にフック（`hook_dil.py`）で当てた。元の計算が値を出した銘柄だけ置き換えた（取れる範囲は変えない）。前後とも同じ道具・同じ日付。

| 見るもの | 結果 |
|---|---|
| 理論株価 | 変化 **0**（96銘柄） |
| 判定 | 変化 **0**（`tanuki_score`のラベル） |
| 保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA） | 理論株価・判定・funda点とも変化なし。希薄化率: ADBE −3.21→−5.55、APP −2.73→−4.01、CELH 3.95→3.88、NVDA −0.74→−1.17、PLTR 7.52→7.48、SOFI 11.59→13.99、SOUN 37.56→39.38（どちらも>20%で−15のまま）、TSLA 0.51→0.70、CRWV 欠けのまま |
| HypeCore（stage） | 時刻（`generated_at`）以外同じ |
| 希薄化率の表示値 | 85銘柄で値が変わる（2pt以上は26銘柄）。表は`DILUTION_UNIFY_STEP0_table.csv` |
| 重大度の表示（`dilution_severity`） | 18銘柄: BBAI severe→critical、CART severe→critical、KULR severe→critical、RDW high→critical、LITE low→high、SITM medium→high、SNPS low→medium、DOCN low→medium、CEG positive→medium、LYFT medium→low、NET low→medium、CAKE positive→low、CDNS positive→low、FCX positive→low、BSY low→positive、FRSH low→positive、HQY low→positive、VZ low→positive |
| funda点（5銘柄） | IONQ 100→85、QBTS 10→0、RDW 75→65、RXRX 60→50、ZETA 90→75。**判定のラベルは全て変わらない**（IONQ WATCH・QBTS PASS・RDW WATCH・RXRX WATCH・ZETA HOLD）。しきい値（>20%・>40%）をまたぐため。方向は全て下がる（原本で株数が実際に増えている銘柄: IONQ deiの3年+28%、RDW +57%、QBTS +55%。RXRX・ZETAはdeiの3年が出ない） |
| 成長の罠（>5%、画面のJS） | 7銘柄で入れ替わる: DDOG 4.8→5.9・DOCN 1.5→6.0・LITE 3.0→14.3・PAYS 4.1→6.0・SITM 3.3→11.3・SNPS 1.9→7.3（入る）、LYFT 5.6→2.0（外れる）。JSの判定なので`tools/sim`には出ない（数だけ） |
| 調整後EPS・STONKS | 希薄化率を読まないので変化なし（コードの確認。STONKSはラベルの改名だけ） |
| scope（`sim.py scope --id DILUTION-DEFINITION-UNIFY-1-0`） | 85銘柄（うち判定・funda・理論株価・stageが変わるのは5銘柄: IONQ・QBTS・RDW・RXRX・ZETA）。書いていない（STEP 0） |

### 5%超・判定変化・保有の一覧（決定(a)の形）

理論株価が5%を超えて動く銘柄はゼロ、判定が変わる銘柄もゼロ、保有銘柄の動きもゼロ。上の「funda点が動く5銘柄」と「重大度の表示が変わる18銘柄」を、原本（SECの申告のXBRL、`company_facts.json`）で次のとおり確かめた。

| 銘柄 | TANUKI今 → 案（年率%） | 原本の確認（deiの表紙の株数、3年の年率／2年の累計） | 理由 |
|---|---|---|---|
| IONQ | 12.3 → 26.4 | +28.3% / +99.9% | 年次の末（2025-12-31）より後の増資・転換。窓が古い |
| RDW | 23.6 → 57.9 | +56.9% / +276% | 同上。買収と増資 |
| QBTS | 39.0 → 43.5 | +55.1% / +134% | 年次と四半期の系列差。もともと>20%（−15）で、案で>40%になり−25（funda 10→0） |
| RXRX・ZETA | 36.6 → 46.3、16.7 → 23.6 | deiが取れない（表紙の株数が10-Kだけ）ため、四半期の加重平均で確認 | 窓が古い |
| LITE | 3.0 → 14.3 | +10.6% / +32.0% | 転換社債の株（if-converted）＋年次の窓が古い（上の表） |
| CEG | −1.5 → +3.2 | +3.3% / +13.3% | Calpineの買収の対価の株（2026Q1） |
| SNPS・SITM・DOCN | 1.9→7.3、3.3→11.3、1.5→6.0 | +8.0%、+10.4%、+11.1%（3年） | 年次の末より後に株数が増えている（原因の内訳は未確認） |
| TSLA（保有） | 0.51 → 0.70 | +7.5% / +23.1% | 制限付株が加重平均に入らない（上の表）。案でも「実際の発行済株数」は表さない→補助の印`outstanding_gap_pct`で示す |

## 5. PARSER段1（revertの戻し）・CONTINUITY（ブランチ）との掛け合わせ

- 希薄化率は`shares_diluted`だけを読む。PARSER段1の`val_guard`（`SPLIT_FIELDS`に`shares_diluted`を含む＝分割の項目として守りの対象から外す）と、CONTINUITYの変更（`q4_implied.py`の非負の規則・`pipeline.py`の成長モデルの文言と`s=0`の扱い・`adjustments.py`）は、どちらも`shares_diluted`の値と`_load_extra_data`の希薄化の箇所を変えない。**掛け合わせにならない**（部品が重ならない）
- 動く銘柄の重なり: PARSER段1の動く銘柄（`PARSER_LAYER3_STAGE1_movers.csv` 22銘柄）と案の動く銘柄（funda点5・重大度18）の積集合は CAKE・HQY・VZ（重大度の表示のみ。PARSER段1は借入金・純資産の定義で理論株価が動く）。CONTINUITYの動く銘柄（29銘柄）との積集合は CAKE・DOCN・FRSH・HQY・KULR（同上）。**5銘柄とも、案の側は表示（重大度のラベル）だけが動き、理論株価・判定は動かない**ので、足し合わせた動きは各々の動きの和（掛け算にならない）
- funda点が動く5銘柄（IONQ・QBTS・RDW・RXRX・ZETA）は、PARSER段1・CONTINUITYの動く銘柄にいない

## 6. 公開の順の案と決めてほしいこと

- 公開の順: PARSER段1 → CONTINUITY → 本件。理由は、本件は表示の値とfunda点だけを動かし、前の2件の「理論株価が動く銘柄の一覧」と重ならないため、順番を入れ替えても結果は同じ。ただし`pipeline.py`を3件とも触るので、rebaseの手間を避けるなら最後
- 公開は`method_change.json`の新しい形（`id`・`committed_at`・`scope`）で、scopeは85銘柄（判定・funda・理論株価・stageが変わるのは5銘柄）。通知（🛠）は「計算方法の分」を5銘柄に出すのが妥当（理論株価が動く銘柄はゼロ）
- 決めてほしいこと:
  1. 部品の定義: **加重平均の希薄化後株数・12四半期前との比**でよいか（deiの表紙の株数は、TSLAの制限付株のように未確定の株を含むため補助の印に留める案）
  2. funda点の判定を3年の年率のままにするか、最近の急増（LITE・CEG）を拾う2年の累計も使うか
  3. STONKSの`dilution_risk`を`sbc_burden`に改名してよいか（画面のラベルと、`stonks_silo/analyzer.py`・`index.html`・保存済みデータのキー）
  4. 未登録の分割の安全網（前年比2.5倍超のヒューリスティック）を四半期の列にも残すか


---

## 実装済み（ブランチ `dilution-unify-1`（cbd4d05783）・未公開、2026-11-12）

kaihatsuには入っていない（`origin/dilution-unify-1`に控えを置いた。PRは作らない）。決定（1〜4）のとおり実装した。

### 訂正（STEP 0の試算の誤り）

STEP 0の「直近四半期÷12四半期前」は、Layer3の四半期の株数が**Q1〜Q3だけでQ4が無い**ため、12個前の点が約4年前になっていた（3年で割っていたので、値が過大）。実装では**日付で3年前（1,096日±50日）の四半期**を探し、年率は実際の日数で割る（`common/dilution.py`）。直した後の値（`share_count_change`、93銘柄に値が出る）:

| 銘柄 | TANUKI今 | STEP 0の値（誤） | 実装の値 |
|---|---|---|---|
| TSLA | 0.51 | 0.70 | **0.46** |
| LITE | 2.98 | 14.30 | **15.15** |
| CEG | −1.54 | 3.15 | **3.47** |
| IONQ | 12.34 | 26.44 | **25.37** |
| RDW | 23.59 | 57.95 | **56.84** |
| CART | 40.19 | 50.93 | **37.21** |
| SOUN | 37.56 | 39.38 | **25.71** |

STEP 0の結論（定義が3つで別の概念・原因の分類・案）は変わらない。変わったのは試算の数字（下の表が正）。

### 実装の中身

| 場所 | 内容 |
|---|---|
| `common/dilution.py`（新設） | `share_count_change`（加重平均の希薄化後株数、直近の四半期÷3年前の四半期の年率。分割は`split_history.yaml`で調整。3年前の点が無ければ欠け〈年次に落とさない〉。未登録の分割の疑い〈隣の四半期との比が2.5倍超・0.4倍未満〉は**割り戻さず`split_suspect`の印**。発行済株数との差は`outstanding_gap_pct`〈表示だけ〉）と `dilution_penalty`（`PENALTY_MODE`: `"step"`＝今の段〈>20%で−15・>40%で−25〉、`"slope"`＝20〜40%で0→−25の直線。**既定は`"step"`**） |
| `tanuki_valuation/pipeline.py` | `_load_extra_data`の希薄化率を差し替え（キー`dilution_3yr_annual_pct`はそのまま。`shares_yr_now`・`shares_yr_3ago`は直近の四半期と3年前の四半期の株数、新キー`dilution_basis`・`dilution_window`・`split_suspect`〈疑いのときだけ〉・`outstanding_gap_pct`）。funda点の減点は`dilution_penalty`の1か所。`split_suspect`の銘柄には減点を使わない。`split_adjusted`・`split_factor`は廃止（年次のヒューリスティックが無くなったため） |
| STONKS | `dilution_risk` → `sbc_burden`（`analyzer.py`のdataclass・計算・出力、`stonks-silo/index.html`の表示〈ラベル「SBC負担」・旧キーも読む〉、`SYSTEM_MAP.md`、出力の一覧〈`stonks_layer`〉）。書き手: `analyzer.py`のみ。読み手: `stonks-silo/index.html`のみ。出力: `docs/value-monitor/stonks-silo/data/results.json`（次のSTONKS実行で新しいキーになる。24件）。`equity_raise_ratio`は作っていない |
| 画面 | `tanuki_valuation/stock.html`（株数の期間の注記・疑いの注記・発行済株数との差の注記）、`tanuki_score/index.html`（`split_suspect`の銘柄は成長の罠の数に入れない） |
| 出力の一覧 | `tanuki_layer`・`stonks_layer`を作り直し（`scripts/inventory/build_output_inventory.py --layer tanuki|stonks`） |
| テスト | `tests/test_dilution.py` 18件（定義・分割の調整・未登録の分割の印・**3年の境目で値が飛ばない**〈3年前の点が無ければ欠け・窓が進んでも同じ値・Q4無しの系列〉・**傾斜が20%・40%で連続**・段は今の規則）、`tests/test_split_adjust.py`のKULR型を四半期の定義に |

### 試算（`tools/sim`、TANUKI→HypeCore→TANUKI、基準はkaihatsuの今のコードを別のworktreeで回した値）

| 見るもの | 段（`step`、既定） | 傾斜（`slope`） |
|---|---|---|
| 理論株価の変化 | **0**（96銘柄） | **0** |
| 判定の変化 | **0** | **0** |
| 保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA） | 理論株価・判定・funda点とも変化なし（希薄化率: ADBE −3.21→−4.89、APP −2.73→−2.11、CELH 3.95→2.42、NVDA −0.74→−0.96、PLTR 7.52→3.44、SOFI 11.59→12.59、SOUN 37.56→25.71、TSLA 0.51→0.46、CRWV 欠けのまま） | **SOUNだけfunda 35→43**（段では35のまま。25.71%は20〜40%の間で傾斜は−7点）。判定は変わらずWATCH |
| HypeCore（stage） | 時刻以外同じ（96ファイル） | 同じ |
| 希薄化率の表示値 | 91銘柄で値が変わる（2pt以上は32銘柄）。欠け: CIX・RCAT（値が出なくなる）、新たに出る: BKNG・LOAR | 同じ |
| 重大度の表示 | 26銘柄で変わる | 同じ |
| funda点が動く銘柄 | ALAB 75→100、CART 55→80、RBRK 65→90（**3つとも`split_suspect`で減点を使わなくなったため**。IPO直後の株数の跳びで、実際の希薄化か分割かをここで決めない）、IONQ 100→85、QBTS 10→0、RDW 75→65（株数が実際に増えている）、AVAV・RXRX・SOUN・SPIR は変わらない | ALAB・CART・RBRK・QBTS・RDW は段と同じ。IONQ 100→93、AVAV 85→95、RXRX 60→55、SOUN 35→43、SPIR 0→1 |
| scope（`sim.py scope --id DILUTION-DEFINITION-UNIFY-1-0`） | 90銘柄（判定・funda・乖離率・理論株価・stageが変わるのは6銘柄: ALAB・CART・IONQ・QBTS・RBRK・RDW） | 90銘柄（同10銘柄: ALAB・AVAV・CART・IONQ・QBTS・RBRK・RDW・RXRX・SOUN・SPIR） |
| 画面の成長の罠（>5%）の入れ替わり | 入る: **LITE**（2.98→15.15）・**PAYS**（4.06→5.85）・**SITM**（3.28→11.76）・**SNPS**（1.92→7.47）。外れる: **FLYW**（5.78→1.11）・**LYFT**（5.59→−0.93）・**PLTR**（7.52→3.44）・**RCAT**（12.38→欠け）。`split_suspect`のALAB・CART・RBRK・LOAR・CIXは数えない | 同じ |
| 調整後EPS・STONKS | 希薄化率を読まないので変化なし（STONKSは`sbc_burden`への改名だけで値は同じ） | 同じ |

- 段と傾斜の差は**funda点だけ**で、判定のラベルは両方とも変わらない。傾斜は20%・40%の境目で点が飛ばない代わりに、20%を少し超えた銘柄の減点が段（−15）より小さい（SOUN 25.71%は段−15・傾斜−7）。採否はチャット側
- 理論株価が5%を超えて動く銘柄は0なので、決定(a)の「理由を原本で確かめた一覧」は、funda点が動く銘柄（上の行）と`split_suspect`の5銘柄（ALAB・CART・CIX・LOAR・RBRK）。`split_suspect`は上場前後の株数の跳び（ALAB 2024-06・CART 2024-03・RBRK 2024-07・LOAR 2024-06）かCIXの単位異常で、分割として登録するか、このまま減点に使わないかは決めてほしい

### PARSER段1・CONTINUITYとの関係

希薄化率は`shares_diluted`の四半期だけを読む。PARSER段1の`val_guard`（`SPLIT_FIELDS`に`shares_diluted`）も、CONTINUITYの`q4_implied.py`・`pipeline.py`の成長モデルの変更も、この箇所を変えないので、重ねても掛け合わせにならない。ただし`pipeline.py`を3件とも触るので、順番に`rebase`して試算し直す。

### 公開の手順（PARSER段1 → CONTINUITY → 本件）

1. PARSER段1・CONTINUITYがkaihatsuに入って公開された後、`git checkout dilution-unify-1 && git rebase kaihatsu`（`pipeline.py`が衝突したら、希薄化の箇所は本件の側を取る）
2. 試算し直す（`tools/sim/sim.py chain base`〈kaihatsuのworktree〉・`chain step`・`chain slope --hook 〈PENALTY_MODE="slope"のhook〉`→`compare`）。理論株価が5%を超えて動く銘柄・判定が変わる銘柄・保有を原本で確かめて一覧にする（決定(a)）。減点の方式が「傾斜」に決まったら`common/dilution.py`の`PENALTY_MODE`を`"slope"`に変える（テストは両方通る）
3. ゲート: `PYTHONUTF8=1 env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider`＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`。出力の一覧を作り直す（`build_output_inventory.py --layer tanuki|stonks`）
4. `config/method_change.json`に`DILUTION-DEFINITION-UNIFY-1-0`を足し、`tools/sim/sim.py scope --id DILUTION-DEFINITION-UNIFY-1-0 base step --write`でscopeを作る（手で書かない）
5. kaihatsuに入れてpush（コードのpushがそのまま公開になるので、ここで手順を止めない）。公開: Stonks Silo Update（キー名の変更）→ TANUKI VALUATION → HypeCore → TANUKI（公開の鎖のとおり。3つ目は手で起動しない）
6. 確認: 1回目の🛠がscopeの銘柄だけに付くこと、2回目は0件、`results.json`の`sbc_burden`、画面（stock.html・tanuki_score・stonks-silo）


---

## 決定の反映（2026-11-12、ブランチ `dilution-unify-1` 〈470d521ec1〉・未公開）

決定: (1) 減点は**傾斜だけ**（段の分岐は消した）。(2) `split_suspect`の5銘柄は`split_history.yaml`に**登録しない**。原本の申告で跳びを分類し、上場時の転換には規則を足す。

### 5銘柄の跳びの分類（原本の申告 = `company_facts.json`のXBRL・`submissions.json`）

| 銘柄 | 申告の事実（加重平均の希薄化後株数） | 分類 |
|---|---|---|
| ALAB | 2023Q1〜Q3 35.8〜37.5M（初めての10-Q〈2024-05-08提出〉の比較欄）→ 2024Q1 52.5M → **2024Q2 155.2M**（2024-08-07提出） | **上場時の転換**（2024-03のIPOで優先株が普通株に転換） |
| CART | 2023Q2 72.3M → 2023Q3 95.8M → FY2023 130.6M → **2024Q1 302.1M** | **上場時の転換**（2023-09のIPO） |
| RBRK | 2023Q1〜 59.9〜61.0M → 2024Q1（04-30）63.8M → **2024Q2（07-31）179.9M** | **上場時の転換**（2024-04のIPO、会計年度は1月末） |
| LOAR | 2023Q1〜2024Q1 **204**（Loar Holdings, LLCの持分のプレースホルダー）→ **2024Q2 89.2M** | **上場時の転換**（`former_names`: Loar Holdings, LLC → Loar Holdings Inc. 2024-04-17） |
| CIX | 2026Q1 12.323M → 2026Q2 **12,329**・2026Q3 **12,337**（申告の値が千株単位のまま） | **単位の誤り**（前の期と同じ会社の同じ概念で、桁が1,000違う） |

### 規則（`common/dilution.py`）

- 上場の時点は**SECの申告から機械的に決める**: `first_report_end(ticker)` = `submissions.json`の`accn_to_filingdate`で最も古い提出の期末（`accn_to_reportdate`）。全98銘柄に履歴がある（古い銘柄は1994年から）。銘柄名は手で書かない
- 3年前の点が**上場の前**（最初の定期報告の期末以前）なら、**上場後の最初の四半期**を基準にする。上場後の基準から直近までが**4四半期以上**（270日以上）のときだけ。年率は実際の日数。4四半期未満は欠け。上場が3年より前の銘柄の単なる欠けは、古い基準を使わず欠けのまま
- 単位の誤り（CIX）は印（`split_suspect`）のまま、減点・成長の罠に使わない。ほかの4銘柄は上場の前後の跳びが窓に入らなくなり、**規則のあとに`split_suspect`が残るのはCIXだけ**
- 上場後の基準になった銘柄（`dilution_basis = quarterly_weighted_since_listing`）: ALAB（2024-06-30 → 2026-09-30、+7.9%/年）・CART（2024-03-31 → 2026-09-30、−7.7%）・RBRK（2024-07-31 → 2026-07-31、+7.1%）・LOAR（2024-06-30 → 2026-09-30、+3.2%）・GEV（2024-04-01 → 2026-09-30、−0.9%）・**CRWV**（2025-06-30 → 2026-06-30、+13.2%。保有。これまで欠け）
- 値が出ないまま残る銘柄: ASTS・RCAT・V・XOM（3年前の点がデータに無い）、CIX（単位の誤り）

### 試算（`tools/sim`、傾斜、基準はkaihatsuの今のコード）

| 見るもの | 結果 |
|---|---|
| 理論株価・判定 | 変化 **0**（96銘柄） |
| stage（HypeCore） | 時刻以外同じ（96ファイル） |
| 保有9銘柄 | 理論株価・判定・funda点とも変化なし。**CRWVは希薄化率が欠け→+13.2%/年**（20%以下なので減点0、fundaは45のまま、成長の罠の数には入る）。SOUNだけfunda 35→43（希薄化率 37.56→25.71%。傾斜の減点が−7点で、段の−15より小さいため）。希薄化率: ADBE −3.21→−4.89、APP −2.73→−2.11、CELH 3.95→2.42、NVDA −0.74→−0.96、PLTR 7.52→3.44、SOFI 11.59→12.59、TSLA 0.51→0.46 |
| scope | 90銘柄（希薄化率の値か重大度の表示が変わる）。うち判定・funda・乖離率・理論株価・stageが変わるのは10銘柄: ALAB・AVAV・CART・IONQ・QBTS・RBRK・RDW・RXRX・SOUN・SPIR |

ALAB・CART・RBRKのfunda点（前＝今の公開、後＝本ブランチ。判定のラベルは変わらない）:

| 銘柄 | 希薄化率（前→後） | 前の減点 | funda点 前→後 | 判定 |
|---|---|---|---|---|
| ALAB | 73.85 → 7.88 | −25 | 75 → **100** | HOLD → HOLD |
| CART | 40.19 → −7.65 | −25 | 55 → **80** | BUY → BUY |
| RBRK | 48.84 → 7.05 | −25 | 65 → **90** | TRIM → TRIM |

前の値は、年次の末2点（IPOの前の少数の普通株 → 転換の後）の比で、IPO時の転換を「3年で数倍の希薄化」と読んでいた。上場後の最初の四半期を基準にすると、実際の希薄化は小さい。

ほかのfunda点の動き（傾斜）: IONQ 100→93、QBTS 10→0、RDW 75→65、RXRX 60→55、AVAV 85→95、SPIR 0→1。

画面の成長の罠（>5%）の入れ替わり（銘柄名）: 入る = **CRWV**（13.15%）・**LITE**（15.15%）・**PAYS**（5.85%）・**SITM**（11.76%）・**SNPS**（7.47%）。外れる = **CART**（−7.65%）・**FLYW**（1.11%）・**LYFT**（−0.93%）・**PLTR**（3.44%）・**RCAT**（欠け）。

### テスト（`tests/test_dilution.py` 17件ほか）

上場前の基準点は使わず上場後の最初の四半期を基準にする・上場後4四半期未満は欠け（182日で欠け、365日で値）・上場が古く3年前の点が無いのは欠けのまま・3年前が上場の後ならふつうの3年・単位の誤りは印のまま・`first_report_end`が最も古い申告の期末を読む・傾斜は20%・40%で連続して単調・段の分岐が無い。`tests/test_pipeline_logic.py`の減点（25% → −6点）を傾斜に直した。

公開の手順（上の「実装済み」の節）の2にある `PENALTY_MODE` の切り替えは不要（傾斜だけになった）。そのほかの手順は変わらない。
