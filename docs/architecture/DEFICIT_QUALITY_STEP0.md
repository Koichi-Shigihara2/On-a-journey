# DEFICIT-QUALITY-ADOPT-1 STEP 0（2026-11-12、読み取りと手元の試算のみ）

コードのcommit・push・公開は無し。道具はscratchpadの`dq/`（`t1.py`＝98銘柄の突き合わせ表・`hook_dq.py`＝試算フック・`sim/`＝`tools/sim`の出力）。基準はkaihatsuの今のコード（公開）。

## 結論

| 見るもの | 結果 |
|---|---|
| 「赤字か」の判定 | 場所は4つ（STONKSの対象のルール・STONKSの赤字の質・TANUKIの構造的赤字〈FCF〉・収益の段階の部品）。**営業利益の判定は、STONKSの対象のルールと部品がすでに同じ式（TTMの営業利益<0）**で、食い違うのは手で書いた`cik_lookup.csv`のフラグだけ（SITM・RXRX） |
| 4つの値で割れる銘柄 | **19銘柄**（営業利益・純利益・OCF・構造的赤字〈FCF〉の結論が揃わない）。原因は6つの型に分かれる（下の表）。概念が違うので、揃えるのは会計の赤字（営業利益）だけ。キャッシュ（OCF）の赤字・FCFの構造的赤字は別の名前で残す |
| 黒字化の予測 | **3つの方式**（STONKS＝年次の純利益率・年次のOCF率の回帰、TANUKI＝調整後EPSの直近4四半期の回帰、部品＝TTMの営業利益率の直近8四半期の回帰・R²≥0.5）。STONKS 24銘柄のうち**14銘柄**で、STONKSと部品の結論（達成済み／推定年／不明）が違う。TANUKIの調整後EPSの予測は理論株価・判定に効かない（表示だけ） |
| 試算（全銘柄、TANUKI→HypeCore→TANUKI） | **読み手を部品に寄せるだけなら、理論株価・判定・保有・stageは変わらない**（TANUKIの理論株価に効く赤字の判定は、すでに部品と同じ`is_structural_deficit`の1か所）。SITMをSTONKSの対象から外しても全銘柄で変化0 |
| AMZN・CEG・COHR型を構造的赤字から外す案 | **外せない**。収益の段階が「黒字」の銘柄を下限・Policy Aから外す試算で、CEGの理論株価 28.95→**−237.37**、AMZN 111.28→**−7.92**（負）、COHR WATCH→TRIM。下限（売上×8%）が負のFCFを支えているため。外す場合は別の基準（利益ベースのFCF等）が要る |
| QBTS・SOUNの決定との矛盾 | **矛盾しない**（下） |

## 1. 定義の洗い出し

### 1-1. 「赤字か」の判定

| # | 判定 | 値・期間・閾値 | 書き手 | 読み手 | 何に効くか |
|---|---|---|---|---|---|
| A | 収益の段階（部品） | TTMの営業利益>0で黒字、<0で赤字、回帰で2年以内に0なら黒字化目前。区分なし・金融は別 | `common/profitability.py::compute` → `docs/common/profitability.json` | なし（定義v2の並行計算は`common/stage_v2.py`が自分の部品を使う。`profitability.json`を読む実行コードは無い） | 今は何にも効かない |
| B | STONKSの対象 | TTM営業利益<0 または TTM売上=0（プレレベニュー）。SOFI（金融）は適用外 | `common/sec_data/stonks_flag_rule.py::judge_stonks_silo`（rcc CHECK-51・登録の検証P7が呼ぶ）。実際のフラグは手で書く`config/cik_lookup.csv`の`stonks_silo` | `src/value/stonks_silo/pipeline.py`（`get_stonks_silo_tickers`）、TANUKIが`results.json`を読む（下の4か所） | **STONKSに載る24銘柄**、TANUKIの`short_target`表示 |
| C | STONKSの赤字の質 | 直近年度。純利益>0なら黒字、それ以外は点数（成長40・R&D+販売費30・粗利率20・黒字10）で65以上 良い赤字・35以上 要観察・未満 悪い赤字 | `stonks_silo/analyzer.py::_analyze_deficit_quality` → 点数の関数は部品の`deficit_quality_verdict` | STONKSの画面、TANUKIが`revenue_growth_pct`を読む（funda点の売上成長・report） | STONKSの画面の判定、TANUKIの**funda点**（売上成長の加点） |
| D | TANUKIの構造的赤字（FCF） | FCFの5点平均・2点平均がともに0以下 | `calculator/adjustments.py::is_structural_deficit`（定義は1か所。部品が呼ぶ） | `core_calculator.py`（売上×8%の下限・`fcf_floor_used_in_base`）、`pipeline.py`のPolicy A（DCF信頼性LOW→WATCH）、`components.structural_deficit`（画面・`tanuki_score/index.html`） | **理論株価・判定**（18銘柄に下限、Policy A） |
| E | STONKSのRunway | 現金÷月間の消費（年次のOCF・設備投資） | `analyzer.py::_analyze_runway` | STONKSの画面、TANUKIの**funda点**（12か月未満で−30）・report | TANUKIのfunda点 |
| F | TANUKIのマトリクス③「深い赤字」 | RICEのQの異常値 | `pipeline.py` | report・画面のマトリクス | 表示 |

### 1-2. 黒字化の予測

| # | 方式 | 値 | 書き手 | 読み手 | 何に効くか |
|---|---|---|---|---|---|
| 1 | STONKSの「純利益黒字化」 | 年次の純利益率の直近4年の回帰（`_gaap_margin_breakeven`）。直近年度の純利益>0なら達成済み | `analyzer.py::_breakeven_estimate` → `results.json`の`profitability_path.gaap_breakeven_*` | STONKSの画面、STONKSの`overall_score`（黒字化パス30%）・summary | STONKSの画面の点数・文言 |
| 2 | STONKSの「営業CF黒字化」 | 年次のOCF率の直近4年の回帰 | 同上（`ocf_breakeven_*`） | 同上 | 同上（キャッシュの軸） |
| 3 | TANUKIの「黒字化四半期」 | 調整後EPS（非GAAP）の直近4四半期の回帰（`compute_eps_breakeven`） | `tanuki_valuation/pipeline.py` → `latest.json`の`breakeven_estimate`・`breakeven_reason` | report.txt（Breakeven_Estimate）、STONKS画面が`tk.breakeven_estimate`を読む | **表示だけ**（`core_calculator`・HypeCore・`tanuki_score`は読まない。grepで確認） |
| 4 | 部品 | TTMの営業利益率の直近8四半期の回帰、最新の実際の率から2年以内に0、R²≥0.5（`MIN_R2`） | `common/profitability.py::breakeven` → `profitability.json` | なし | 今は何にも効かない |

## 2. 結論が割れる19銘柄（営業利益・純利益・OCF・構造的赤字〈FCF〉）

`t1.py`の突き合わせ（2026-11-12、TTM）。列は 営業利益<0／純利益≦0／OCF≦0／構造的赤字 の順（1＝赤字）。金額は百万ドル。

| 型 | 銘柄（営業利益／純利益／OCF） | どの値の違いで割れるか |
|---|---|---|
| ① 営業赤字・純損失だがOCFは黒字（8） | AVAV（−253／−203／+59）・CRWV（−231／−1,928／+6,911）・FROG（−49／−21／+215）・GTLB（−90／−53／+223）・NET（−385／−239／+682）・RBRK（−282／−254／+337）・S（−324／−340／+57）・ZS（−133／−63／+1,130） | 株式報酬・減価償却・繰延収益でOCFが営業利益より大きい。会計の赤字とキャッシュの赤字が違う |
| ② 営業赤字だが純利益は黒字（4） | ESTC（−48／+376）・LYFT（−162／+2,810）・IONQ（−1,270／+255、OCFも負）・ONDS（−244／+167、OCFも負） | 営業外・税の項目（評価の戻し・税の評価引当の戻し・公正価値の変動のたぐい）で純利益が営業利益と逆になる。内訳は申告の数値の比較までで、個別の要因は未確認 |
| ③ 営業黒字だが純損失（2） | LITE（+878／−6,579）・ZETA（+25／−2） | LITEは営業外の巨額の損失（転換社債の公正価値等とみられる）、ZETAは境目 |
| ④ 黒字だがFCFだけ構造的赤字（2） | AMZN（+104,366／+134,779／+168,300）・CEG（+4,596／+3,465／+4,206） | 設備投資（FCF＝OCF−設備投資）。OCFも黒字 |
| ⑤ 営業・純利益は黒字、OCFが負（2） | COHR（+1,116／+963／−59）・SOFI（金融、+1,760／+738／−10,209） | COHRは運転資本・設備投資、SOFIは貸出の実行（金融はキャッシュの定義が別） |
| ⑥ 全部が赤字（1） | RXRX（業種のマスターで「区分なし」のバイオ。−495／−476／−311） | 割れていない。業種のマスターで「区分なし」のため収益の段階だけ`unclassified` |

SITM（TTM 2026-09-30で営業利益が黒字に転じた）は割れ方が1つ増える（フラグ`true`・収益の段階`profit`）。

### 部品の判定との照合

- 営業利益の赤字：部品の収益の段階（赤字・黒字化目前）と`cik_lookup.csv`のフラグを比べると、**食い違いは2銘柄**。SITM（フラグtrue・部品は黒字。手で書いたフラグが古い）・RXRX（フラグtrue・部品は区分なし。営業利益はTTMで−495M）
- 構造的赤字（FCF）：部品の`structural_deficit`（19銘柄）とTANUKIの`components.structural_deficit`（18銘柄）の違いは**RKLBだけ**（部品は真、TANUKIは偽）。部品は`is_structural_deficit`を呼ぶだけで定義は1つだが、入力のFCFの点（部品＝TTMの毎年同じ期末の5点・Layer3の`first_val`、TANUKI＝`fcf_5yr_avg`・`fcf_2yr_avg`）が違う
- 赤字の質の点数：STONKS 24銘柄のうち**5銘柄で点数が違う**（CRWV 30→10・IONQ 83→75・JOBY 43→35・LYFT 38→30・SOUN 83→75。判定ラベルは同じ）。原因は**粗利率の入力だけ**: 部品は`gross_profit`のフィールドしか見ず、5銘柄ではLayer3に粗利が無いため「粗利率計算不可」になる。STONKSは売上−売上原価から粗利率を導出している（`gross_margin_derived`）。部品に同じ導出を足せば揃う

## 3. 案

### 3-1. 読み手を部品に寄せる

| 概念 | 名前（案） | 定義 | 読み手 |
|---|---|---|---|
| 会計の赤字（営業利益） | 収益の段階（`revenue_stage`） | TTMの営業利益。部品の`compute`の1か所 | STONKSの対象、TANUKIの黒字化の表示、rcc CHECK-51、登録の検証P7 |
| キャッシュの赤字（FCF） | 構造的赤字（`structural_deficit`、FCF） | 今の`is_structural_deficit`のまま（TANUKIの下限・Policy Aはそのまま） | TANUKIの理論株価・判定 |
| キャッシュの赤字（OCF）の黒字化 | `ocf_breakeven`（STONKSに残す） | 年次のOCF率の回帰（キャッシュの軸なので部品の営業利益率とは別の概念）。部品へ寄せるなら、TTMのOCF率の回帰に直す別の判断 | STONKSの画面 |
| 調整後EPSの黒字化 | `adj_eps_breakeven`（TANUKIに名前を分けて残す、または廃止） | 非GAAPのEPS。表示だけなので、廃止か、名前を「調整後EPSの黒字化」に分けて残す | report.txt（画面は部品の黒字化を表示） |

- **対象の選び方（STONKS）**：`stonks_silo`フラグを、手で書く値から**部品の収益の段階（赤字・黒字化目前＝TTMの営業利益<0）**に置き換える。金融（SOFI）の適用外は今の`excluded`のまま。区分なしの業種（RXRX＝バイオ）は、TTMの営業利益<0なら対象に残す（STONKSのルールは業種の区分に依らない）。**入る銘柄は無し、外れる銘柄は SITM だけ**（TTM 2026-09-30で黒字に転じた）。ほかのフラグ`true`の22銘柄は収益の段階と一致
- **QBTS・SOUNの決定との関係**：決定は「OCFが5年・2年平均とも構造的に負＝会計処理の問題ではなく実際にキャッシュを消費している」で、TANUKIの**FCFの構造的赤字（下限・Policy A）を残す**根拠だった（BACKLOG_DONE.mdのFCF-DEFINITION-SINGLE-SOURCE-1段1b）。STONKSの対象の選び方には使っていない。QBTS・SOUNは収益の段階が赤字（TTMの営業利益<0）で、案の対象の選び方でもSTONKSに残る。**矛盾しない**
- **赤字の質の入力を1つにする**：部品に粗利の導出（売上−売上原価）を足し、STONKSの画面が部品を呼ぶ。入力は部品（Layer3の年次、その時点で申告済みの値）。STONKSの売上の外れ値のサニタイズ（`revenue_sanitized`）との整合は寄せる段で決める
- **TANUKIのRunway・売上成長（funda点）**：STONKSの出力を読む今の形のまま（Runwayは寄せない）

### 3-2. 黒字化の予測を1つの規則にそろえる

**会計の黒字化は部品の1つ**（TTMの営業利益率の直近8四半期の回帰・最新の実際の率から2年以内・R²≥0.5）に統一し、STONKSの「純利益黒字化」とTANUKIの調整後EPSの黒字化をこれに置き換える。「黒字化の見込みの規則」との関係：規則そのものが部品の規則（`MIN_R2`で当てはまりが悪い銘柄は「見込み不明」にする）。STONKSの純利益は年次4点で、1回の一過性の損益が直線の傾きを大きく動かす。部品は四半期のTTM8点で、最新の実際の率から出発するので、一過性の影響が小さい。

STONKS 24銘柄で、現在のSTONKSの純利益黒字化と部品の結論を比べた（達成済み／推定年／不明）。**10銘柄が同じ、14銘柄が変わる**:

| 銘柄 | 今のSTONKS | 部品 | 参考（TANUKIの調整後EPS） |
|---|---|---|---|
| AVAV | 2027 | 不明 | 達成済み |
| CRWV（保有） | 2026 | 不明（点が7点でそろわない） | 不明 |
| ESTC | 達成済み | 2027 | 達成済み |
| JOBY | 不明 | 2026 | 不明 |
| LYFT | 達成済み | 不明 | 達成済み |
| NET | 2026 | 不明 | 達成済み |
| ONDS | 不明 | 2027 | 不明 |
| RBRK | 不明 | 2026 | 達成済み |
| RKLB | 2028 | 2029 | 不明 |
| S | 2028 | 2030 | 達成済み |
| SITM | 不明 | 達成済み | 達成済み |
| SOUN（保有） | 2026 | 不明（R² 0.39） | 不明 |
| SPIR | 達成済み | 不明 | 2027 |
| ZS | 2027 | 不明 | 不明 |

- TANUKIの調整後EPSと部品の結論は、損益が赤字の銘柄で19銘柄違う（AVAV・BBAI・DOCN・ESTC・FROG・GTLB・IONQ・JNJ・JOBY・KULR・LYFT・NET・ONDS・QBTS・RBRK・RDW・RKLB・S・SPIR）。調整後EPSは非GAAP（SBC等を除く）なので、GAAPの赤字の銘柄が「達成済み」になる
- **保有のSOUN・CRWV**：どちらも画面の黒字化の年が「2026」から「見込み不明」に変わる。SOUNの原本（Layer3 = SECのXBRL）：営業利益は2024年−341.4M、2025年−23.3M（2025Q3 −115.9M、**Q4 +42.6M**、2026Q1 −22.7M、Q2 −43.3M）。STONKSの年次の純利益（2024 −350.7M → 2025 −14.0M）は2025Q4の一過性の黒字で大きく改善したように見え、直線を延ばすと2026年に0に届く。四半期のTTMの営業利益率は、2024Q4の−403%から2026Q2の−69%の間で上下し（8点、R² 0.393）、2025Q4の一過性の改善の後は悪化している。部品の「見込み不明」のほうが原本に合う。CRWVは営業利益が2025Q3 +51.9M、Q4 −89.6M、2026Q1 −144.0M、Q2 −49.0Mで、純利益は−1,167M（2025）。上場が2025年でTTMの点が7点しかなく、部品は欠け

### 3-3. 公開すると変わるもの（案）

- 画面：STONKSの黒字化の年（14銘柄）・STONKSの対象（SITMが外れる）・STONKSの赤字の質の点数（5銘柄）・TANUKIのreportのBreakeven_Estimate・STONKS画面がTANUKIから読む黒字化
- 理論株価・判定・stage：変わらない（次の試算）

## 4. 全銘柄の試算（`tools/sim`、TANUKI→HypeCore→TANUKI）

STONKSの出力はyfinanceの株価の取得（`fetch_valuation`）を含むため回していない。TANUKIが読む`results.json`の部分（対象の銘柄・Runway・売上成長）の変化を、フックで再現した。

| 試算 | 内容 | 結果 |
|---|---|---|
| 基準 | kaihatsuの今のコード | — |
| V2（STONKSの対象） | SITMを`results.json`から外す（対象の選び方の案） | 理論株価・判定・funda点とも**変化0**（SITMのRunwayは`None`〈キャッシュフロー黒字〉で、TANUKIのfallbackも発動しない） |
| V3（構造的赤字の再定義） | 収益の段階が「黒字」の銘柄（AMZN・CEG・COHR）を下限・Policy Aから外す | 理論株価が変わる2（**CEG 28.95→−237.37、AMZN 111.28→−7.92**）、判定が変わる1（COHR WATCH→TRIM）、funda点が3銘柄で下がる。**不採用の根拠** |
| 読み手を部品に寄せる（黒字化の表示・赤字の質の入力・対象の選び方） | TANUKIの理論株価・判定・funda点に効く入力（構造的赤字・Runway・売上成長）を変えない | 理論株価・判定・保有9銘柄・stageとも変化0（コードの確認とV2） |

保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA）は全試算で理論株価・判定・funda点とも変化なし。SOUNは画面の黒字化の年だけが変わる（上記）。HypeCoreのstageは時刻以外同じ。

## 5. 未公開の3つ（PARSER段1・CONTINUITY・DILUTION）との重なり

| 触る場所（本件の案） | PARSER段1 | CONTINUITY | DILUTION |
|---|---|---|---|
| `common/profitability.py`（粗利の導出・STONKSから呼ぶ） | `cf_lines.py`・`bs_lines.py`・`layer3_builder.py`の値の定義を変える（部品が読むLayer3の値が動く＝収益の段階・構造的赤字の入力が動く可能性） | `q4_implied.py`（Q4の逆算の非負の規則）がLayer3の四半期を変える（部品のTTMが動く） | なし |
| `stonks_silo/analyzer.py`（赤字の質・黒字化） | なし | なし | **重なる**（`dilution_risk`→`sbc_burden`の改名） |
| `stonks_silo/pipeline.py`・`stonks_flag_rule.py`・`cik_lookup.csv` | なし | なし | なし |
| `docs/value-monitor/stonks-silo/index.html`（表示） | なし | なし | **重なる**（希薄化→SBC負担のラベル） |
| `tanuki_valuation/pipeline.py`（黒字化の表示） | 触る（借入金・純資産の定義） | 触る（成長モデルの文言） | 触る（希薄化率） |
| `adjustments.py`（構造的赤字の定義を変える場合） | なし | **重なる**（`adjust_fcf`〈連続性〉） | なし |

- 公開の順の案: **PARSER段1 → CONTINUITY → DILUTION → 本件**。理由は、(1) 部品が読むLayer3の値は段1とCONTINUITYで動くので、本件の試算は両方の公開の後に作り直す必要がある（収益の段階の境目の銘柄が動く可能性）、(2) 本件のSTONKS側の変更はDILUTIONの改名と同じファイルなので、DILUTIONの後にrebaseするほうが衝突が少ない、(3) 本件はTANUKIの理論株価・判定を動かさないので、最後でよい。構造的赤字（`adjustments.py`）は触らない（V3の不採用）ので、CONTINUITYとの衝突は避けられる

## 6. 決めてほしいこと

1. **黒字化の予測を部品の1つ（TTMの営業利益率・R²≥0.5）にそろえてよいか**。STONKSの純利益の年次の回帰と、TANUKIの調整後EPSの回帰を置き換える。保有のSOUN・CRWVの黒字化の年は「2026」から「見込み不明」になる。STONKSの`ocf_breakeven`（キャッシュの軸）は残す案
2. **TANUKIの調整後EPSの黒字化**（表示だけ）を廃止するか、`adj_eps_breakeven`として名前を分けて残すか
3. **STONKSの対象を部品の収益の段階にする**とき、境目の銘柄の出入りを防ぐために確認を挟むか（例: 4四半期続けて黒字になるまで外さない。SITMは1四半期だけ黒字）。区分なし（RXRX）は営業利益<0なら残す案
4. **AMZN・CEG型**：V3で下限を外すと理論株価が負になる。これらを構造的赤字・下限・Policy Aに残す（今のまま・理論株価は参考値）でよいか。別の基準（利益ベースのFCF等）を設計するなら別の件


---

## 実装済み（ブランチ `deficit-quality-adopt-1`・未公開、2026-11-12）

kaihatsuには入っていない（`origin/deficit-quality-adopt-1`に控え。PRは作らない）。コードは`f3b306ec9c`、作り直したデータ（`docs/common/profitability.json`）は別のcommit`0ef1c29e29`。決定（1〜5）のとおり実装した。

### 実装の中身

| 決定 | 実装 |
|---|---|
| 1. 黒字化の予測は部品の1つ | 会計の黒字化は`common/profitability.py::breakeven`（TTMの営業利益率の直近8四半期の回帰・R²≥0.5）の**1か所**。読み手は`breakeven_view(entry)`（`load_entry(ticker)`）を呼ぶだけ。STONKSの年次の純利益率の回帰（`_gaap_margin_breakeven`）と出力`gaap_breakeven_*`は廃止し、`op_breakeven_year`・`op_breakeven_reason`（部品の理由コード: ACHIEVED・PREDICTED・TOO_FAR・NO_TREND・NO_DATA・LOW_FIT・UNCLASSIFIED・NO_ENTRY）に。`ocf_breakeven_*`は年次のOCF率の回帰のまま、**キャッシュの軸**として別の名前で残した（画面の文言も「営業CF黒字化予測(キャッシュ)」と「営業黒字化予測」に分けた） |
| 2. TANUKIの`compute_eps_breakeven`は廃止 | `compute_eps_breakeven`・`EPS_*`定数・`latest.json`の`breakeven_estimate`・`breakeven_reason`を削除。report.txtの`Breakeven_Estimate:`行（`report_txt_parser`が読む行名は同じ）は部品の値。STONKSの画面の「黒字転換目算」は`tk.breakeven_*`（TANUKI）ではなくSTONKS自身の`profitability_path.op_breakeven_*`を読む |
| 3. STONKSの対象は部品の収益の段階＋持続の規則 | 新設`common/stonks_target.py`。対象 = 部品が出す**月末ごとのTTM営業利益の符号**（`operating_state_history`、直近4か月末）を、**stage_v2の持続の条件（`persist`、2か月の月末で続けて同じになったときだけ変える）**に通した最後の状態が「赤字」の銘柄。profitabilityには持続の規則が無いので、stage_v2の`persist`をそのまま呼ぶ（新しい閾値は作っていない。テストで確認）。例外は`config/stonks_target.json`の`excluded`（SOFI・金融）と`pinned`（ASTS・TTMの営業利益が取れない既知の銘柄）だけで、どちらも理由つき。`cik_lookup.csv`の`stonks_silo`列を削除し、読み手（`tickers.get_stonks_silo_tickers`・`get_active_tickers("stonks_silo")`・`get_registrable_tickers`・`get_tickers_by_flag`）は`stonks_target.targets()`に切り替え。旧`stonks_flag_rule.py`・`config/stonks_flag_rule.json`・`tests/test_stonks_flag_rule.py`・rccのCHECK-51の旧照合（フラグ対判定）・登録の検証P7の旧照合を削除（**これで、SITMの既知の失敗〈CHAT_RULES 事例18のゲートの例外〉は無くなる**）。CHECK-51は「`results.json`の銘柄と今の判定のずれ」に置き換え。月末の履歴が1銘柄も無いときは空の対象で走らせず止める |
| 4. 構造的赤字は今のまま、型④を表示で区別 | 下限・Policy Aは変更なし。`components.structural_deficit_kind`（`capex_profit`＝収益の段階が黒字でFCFだけ構造的赤字: AMZN・CEG・COHR、`cash_loss`＝営業利益も赤字: 15銘柄）を`latest.json`に足し、report.txtは「ℹ️ FCFが負（設備投資）: TTMの営業利益は黒字ですが…」、`tanuki_score`の画面は「✗設備」（titleに説明）。FINDINGSに「AMZN・CEG型の理論株価は、成長の設備投資を分けない下限で支えられている（参考値）」を1行 |
| 5. 粗利率の導出 | 部品に`gross_ratio`（粗利のフィールドが無い年は売上−売上原価）。STONKSの赤字の質の点数との差は24銘柄で**0**（CRWV・IONQ・JOBY・LYFT・SOUNの5銘柄がそろった） |

### 試算（`tools/sim`、TANUKI→HypeCore→TANUKI、基準はkaihatsuの今のコード）

| 見るもの | 結果 |
|---|---|
| 理論株価・判定・funda点・upside・timing | 変化 **0**（96銘柄） |
| 保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA） | 理論株価・判定とも変化 0（SOUNはWATCH、CRWVはWATCHのまま） |
| stage（HypeCore） | 時刻以外同じ（96ファイル） |
| `latest.json`の変化 | `breakeven_estimate`・`breakeven_reason`が全銘柄から消える（94銘柄）、`components.structural_deficit_kind`が18銘柄に付く（`capex_profit` = AMZN・CEG・COHR、`cash_loss` = ASTS・AVAV・BBAI・CRWV・IONQ・JOBY・KULR・ONDS・QBTS・RCAT・RDW・RXRX・SOFI・SOUN・SPIR）。値は変わらない |
| STONKSの対象 | 今日は**入る銘柄も外れる銘柄も無い**（24銘柄のまま）。SITMは月末ごとの符号が7〜10月の4回とも赤字で（黒字に転じたTTM 2026-09-30は11-04の申告で、次の月末は11-30）、**持続の規則で、早くて12-31の月末（11-30・12-31の2回続けて黒字）の後に外れる**。RXRX（区分なし）は営業利益<0なので対象のまま |
| STONKSの黒字化の表示（`op_breakeven`） | 24銘柄のうち**14銘柄が変わる**（今→新）: AVAV 2027→不明、CRWV（保有）2026→不明、ESTC 達成済み→2027、JOBY 不明→2026、LYFT 達成済み→不明、NET 2026→不明、ONDS 不明→2027、RBRK 不明→2026、RKLB 2028→2029、S 2028→2030、SITM 不明→達成済み、SOUN（保有）2026→不明、SPIR 達成済み→不明、ZS 2027→不明。残り10銘柄は同じ。`ocf_breakeven`は24銘柄とも同じ |
| STONKSの赤字の質の点数 | 24銘柄とも今の公開と同じ（`dq_old`と一致）、部品とも一致 |
| scope | `sim.py scope`は94銘柄（キーの出入り）、判定・funda・理論株価・stageが変わる銘柄は**なし**。値が変わらない表示だけの変更なので、`config/method_change.json`に登録するかは公開のときに決める（登録しない案: 🛠の通知を出さない） |

### 未公開の3つとの衝突

`git merge-tree`で`dilution-unify-1`との統合を試した。**文字として衝突するのは出力の一覧4ファイルだけ**（`output_inventory/tanuki_layer`・`stonks_layer`の`.json`・`.md`。`build_output_inventory.py --layer tanuki|stonks`で作り直せば消える）。同じファイルを触る箇所は別の場所で自動で統合できる:

| ファイル | 本件 | DILUTION |
|---|---|---|
| `src/value/stonks_silo/analyzer.py` | 黒字化（`_breakeven_estimate`・`ProfitabilityPath`のop_*・summaryの文言） | `dilution_risk`→`sbc_burden` |
| `docs/value-monitor/stonks-silo/index.html` | 黒字化の列・文言・黒字転換目算 | 希薄化リスク→SBC負担のラベル |
| `src/value/tanuki_valuation/pipeline.py` | `compute_eps_breakeven`の削除・report・`structural_deficit_kind` | 希薄化率の計算 |
| `docs/value-monitor/tanuki_score/index.html` | 構造的赤字の表示（型④） | 成長の罠（`split_suspect`） |

統合した状態（出力の一覧を作り直し）で、本件・DILUTIONの関連テスト428件が通った。PARSER段1・CONTINUITYとは`pipeline.py`・`stock.html`・`adjustments.py`・`layer3_builder.py`で触る場所が違い、本件は構造的赤字の定義（`adjustments.py`）を触らない。

### 公開の手順（PARSER段1 → CONTINUITY → DILUTION → 本件）

1. 3つの公開が終わった後、`git checkout deficit-quality-adopt-1 && git rebase kaihatsu`（出力の一覧が衝突したら`build_output_inventory.py --layer sec|tanuki|stonks|eps`で作り直す。`docs/common/profitability.json`がボットの更新と衝突したら取り直す）
2. `python common/profitability.py --all`で`profitability.json`を作り直す（**公開の前に必ず**。`operating_state_history`が無いとSTONKSの対象が空になるので、`stonks_target.targets()`は止まる）。試算し直す（`tools/sim/sim.py chain base`〈kaihatsuのworktree〉・`chain after`・`compare`・`scope`）。理論株価・判定・保有が0であることを確かめる
3. ゲート: `PYTHONUTF8=1 env -u DISCORD_WEB_HOOK python -m pytest tests/ src/subport/day_trade/test_logic.py -q -p no:cacheprovider`＋`common/sec_data/audit.py`＋`report_consistency_check.py --fail-on-ng`
4. kaihatsuに入れてpush（`profitability.json`の作り直しも同じpush）。公開: Market Data Weekly（`profitability.json`の定時の作り直しと同じ）→ Stonks Silo Update（`op_breakeven_*`・対象）→ TANUKI VALUATION → HypeCore → TANUKI（`breakeven_*`が消え、`structural_deficit_kind`が付く）
5. 確認: STONKSの`results.json`が24銘柄、`op_breakeven_*`がある、`gaap_breakeven_*`が無い、TANUKIの`latest.json`に`breakeven_estimate`が無い、画面（stonks-silo・tanuki_score）、CHECK-51のずれが無い。SITMの対象からの出入りは12月末以降（持続の規則）に確かめる


---

## 手書きの例外を規則に（2026-11-12 追記、ブランチ `deficit-quality-adopt-1` `d4f824cf0e`・データ `c9aadc065d`）

上の「実装の中身」3の`config/stonks_target.json`（`pinned`＝ASTS・`excluded`＝SOFI）は**やめた**（ファイル削除）。STONKSの対象は規則だけで決まる（`common/stonks_target.py`）:

| 場合 | 扱い |
|---|---|
| 金融・資産の重い業種 | `common/sector_master.py::is_financial_ticker`（業種のマスターの判定）で対象外（SOFI） |
| TTM営業利益が取れない**月** | 直前の状態を保つ（持続の条件の「判定不能は直前を保つ」）。印 `held` |
| 履歴の**最初から**取れない銘柄 | 取れる最新の年次の営業利益の符号を使う。印 `annual` |
| どちらも無い銘柄 | 対象外。印 `no_data` |

- 月末ごとの履歴の行は `[月末, 状態, 根拠]`（根拠 = `ttm`・`annual`・`none`）。印は`stonks_target.marks()`・`explain()`・登録の検証P7（情報）に出る
- **規則のあとの対象は今の24銘柄と同じ**（入る銘柄・外れる銘柄とも無し）。ASTSは手書きの固定をやめても入る（Layer3でTTMの営業利益が取れるようになっていて、赤字と判定される。印なし）。SOFIは金融で除かれる。全98銘柄で印が付くのは**XOM（`no_data`、営業利益が取れない大型の黒字株で、元から対象外）の1銘柄だけ**
- テスト: 金融は除く／TTMが欠けても出入りしない（`held`）／最初から取れない銘柄は年次の符号（`annual`）／どちらも無ければ対象外（`no_data`）／手書きの例外が残っていない（`config/stonks_target.json`が無い）

### 公開の手順への追記

- `config/method_change.json`には**登録しない**（理論株価・判定・funda点が変わらない表示だけの変更。`sim.py scope`は`latest.json`のキーの出入りで94銘柄になるが、🛠の通知は出さない）
- 公開の順の手順（上）の2の`python common/profitability.py --all`は、月末の履歴の行が3列になった今の形で作り直す。**これを公開の前にしないと、STONKSの対象が空になる前に`stonks_target.targets()`が止まる**（空の対象で走らせない）
