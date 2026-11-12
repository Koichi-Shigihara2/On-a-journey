# TANUKI-SHARES-SINGLE-SOURCE-1 STEP 0（2026-11-13、読み取りと手元の試算のみ）

コードのcommit・push・公開は無し。全96銘柄の表は `TANUKI_SHARES_STEP0_table.csv`。道具はscratchpadの`ma/`（`sh1.py`〜`sh3.py`＝株数の突き合わせ・分類、`hook_shares.py`＝案Aの試算フック、`hook_sharesB.py`＝案Bの試算フック、`cmp3.py`＝前後の比較）。基準はkaihatsuの今のコード（`907ffa0ab5`の上、公開）。原本は、各社の10-Q・10-Kの**XBRLの値**（`company_facts.json`の`WeightedAverageNumberOfDilutedSharesOutstanding`・`…BasicSharesOutstanding`・`CommonStockSharesOutstanding`〈貸借対照表の期末の発行済〉・`dei:EntityCommonStockSharesOutstanding`〈表紙〉。accn付き）で確かめた。種類株の表紙（TSLA以外のASTS・PLTR・SOUNの次元つきの値）は、`sec.gov/Archives`の取得に連絡先つきのUser-Agentが要る（このセッションでは403）ため**原本の表紙は未確認**と書いた。

## 結論

| 見るもの | 結果 |
|---|---|
| 今の株数の出所 | `components.diluted_shares`＝**96/96がyfinanceの`impliedSharesOutstanding`**（`_shares_source=yf_implied`）。SECの値（最新年度の加重平均の希薄化後）は、yfが無い時だけの代わり |
| 名前と中身 | 「完全希薄化後」と呼んでいるが、中身は**最新の表紙の発行済株数（基本、全クラス）**にほぼ等しい（表紙のある73銘柄のうち63銘柄が0.5%以内、71銘柄が3%以内）。オプション・RSU・転換社債の希薄化は入らない。名前と中身がずれている（型E） |
| 何に効くか | 理論株価（1株あたり）・ネットキャッシュ／株・FCFE・Valuation Gap・validator・report・画面の時価総額と株数表示。funda・timing・stageには直接は効かない（理論株価の乖離率経由でtimingと判定に効く） |
| SECの加重平均（直近四半期、倍率の部品〈`shares_by_end`〉・DILUTIONの`share_count_change`と同じ基準）との差 | 3%超は**23銘柄**。原因は5種類（下）: 希薄化証券15・期中の発行／買戻し・時点の違い8・未確定の株1・種類株1（うち原本の表紙で確かめた銘柄と未確認の銘柄を表に分けた） |
| 試算（案A: 部品の株数、96銘柄、TANUKI→HypeCore→TANUKI） | 理論株価が変わる**92銘柄**・5%超**12銘柄**・判定が変わる**2銘柄**（CPRT HOLD→TRIM、RMBS WATCH→HOLD）・timing点2・**funda点0**・**stage0**・保有9銘柄の判定は全て変化0（理論株価はSOUN +11.3%・TSLA +11.6%・PLTR −6.4%・SOFI −4.4%・ほか−1.7%以内）・scope 95銘柄（理論株価が0.05%超動く銘柄が印の対象） |
| 案の勧め | **案A（部品の株数＝加重平均の希薄化後）を標準にし、DILUTIONの補助の印`outstanding_gap_pct`で「期末の発行済との差」を表示する**。理由は§4。案B（期末の発行済＋希薄化の増分）は試算したが、TSLAで二重に数える等の穴があり勧めない |

## 1. diluted_shares の書き手・読み手

### 書き手

| 場所 | 内容 |
|---|---|
| `common/market_data/fetcher.py` L1221 | `attributes/{T}.json`の`implied_shares_outstanding`＝yfinanceの`impliedSharesOutstanding`。同じく`shares_outstanding`＝`sharesOutstanding`（L1220付近）。週1回の取得 |
| `common/sec_data/reader.py::get_diluted_shares` L270 | 最新**年度**の`shares_diluted`（加重平均の希薄化後。100万未満なら`shares_basic`）。四半期ではない |
| `tanuki_valuation/data_fetcher.py::_determine_diluted_shares` L1205 | ①`yf_implied`＞100,000ならそれ（`yf_implied`）②無ければyfの`outstanding`とSEC年度値で: 比5倍超→`yf_outstanding_post_dilution`、0.2倍未満→SEC、それ以外は大きい方（`max_sec`／`max_yf`）③片方だけ ④無し→0。**実際は96/96が①** |
| `core_calculator.py` L912・L925 | `components.diluted_shares`・`_shares_source`に保存 |

### 読み手（`components.diluted_shares`）

| 読み手 | 用途 | 理論株価・判定への効き |
|---|---|---|
| `core_calculator.py` L178〜702、`calculator/{scenarios,sensitivity,adjustments,fcfe}.py` | 1株あたりの理論株価＝`PT÷diluted_shares＋ネットキャッシュ／株`（シナリオ・感度・RPO・FCFE含む） | **理論株価そのもの**（IV∝1÷株数） |
| `pipeline.py` L481 | 100,000以下ならその銘柄をスキップ | 有無 |
| `pipeline.py` L721・L2035・L2173・L3328 | reverse DCF（Valuation Gap）・ネットキャッシュ総額・FI（FCFE）の分母 | 理論株価・report |
| `pipeline.py` L2371 | 「期中に大規模な株式発行／買戻し」の注記（yf対直近annualの乖離5%超） | 表示のみ |
| `pipeline.py` L3217・L3284 | 希薄化率の計算のスキップ判定・流動資産の比（時価総額＝株数×株価） | 注記 |
| `pipeline.py` L1414〜1428（`select_max_eps_shares`） | 最大EPSの分母のフォールバック（TTMの加重平均が無い時の今の株数） | 表示のみ（最大EPS） |
| `validator.py` L101〜447 | 理論株価の再現・株数の異常検知 | 検証 |
| `stock.html` L892・L1002・L2388 | 時価総額（MULTIPLES-ADOPT-1で部品の値へ）・ネットキャッシュ／株・シナリオの再計算 | 画面 |
| `common/sec_data/report_consistency_check.py` L2797 | CHECK-10（PSの乖離）の分母 | 検査 |
| `common/sec_data/audit.py` L139 | EPS ANALYZERの株数との差の検査 | 検査 |

STONKS・HypeCore・ROTCEは`diluted_shares`を読まない（時価総額・株数は部品へ移行中）。EPS ANALYZERの`diluted_shares`は別（四半期のSECの値）。

## 2. 全銘柄の突き合わせと原因の分類（`TANUKI_SHARES_STEP0_table.csv`）

比べる値: **Y**＝TANUKIの`diluted_shares`（yfinance implied）、**Wd**＝Layer3の直近の加重平均の希薄化後株数（部品の`current`の株数、分割換算）、**Wb**＝同じ四半期の加重平均の基本株数、**BS**＝貸借対照表の期末の発行済株数、**表紙**＝`dei`の最新の表紙の株数。

| 分類 | 銘柄数 | 判定のしかた |
|---|---|---|
| 一致（Y対Wdが3%以内） | 72 | |
| 比較不能（株数欠け） | 3 | V（Layer3に株数なし）・RKLB・ZS（TANUKI対象外でYなし） |
| 希薄化証券（Y＜Wd。Wdにオプション・RSU・転換社債が入る） | 15 | Y≒BS≒表紙で、Wd−Wbが差の大部分 |
| 期中の発行・買戻し（加重平均と期末の違い） | 2 | JOBY・RXRX。期末の発行済が期中の加重平均より大きい |
| 時点の違い（Layer3が最新四半期を未収載） | 3 | ONDS・RCAT・SOUN。Layer3は2026-06-30まで（Q3の10-Qは11月に提出） |
| 未確定の株（TSLA型） | 1 | TSLA |
| 種類株 | 1 | ASTS |
| 要原本 | 1 | IONQ |

### 原本で確かめた一覧（5%超12銘柄＋判定が変わる2銘柄＋保有）

数値は百万株。accnは貸借対照表・損益計算書の値を取った10-Q。

| 銘柄 | Y→Wd（IV） | 原本の値 | 原因 |
|---|---|---|---|
| **TSLA（保有）** | 3,952.8→3,541.0（+11.6%） | 2026-09-30: 期末の発行済 3,953.0（前期末 3,949.0＝ほぼ増えていない）、加重平均 基本 3,240.0・希薄化後 3,541.0（accn 0001628280-26-066775）。表紙 3,952.8（2026-10-15） | **未確定の株**。期末の発行済と基本の加重平均の差が713M（22%）あるのに期中の発行はほぼ無い＝発行済に入っていて基本のEPSの分母に入らない株（制限付株式の未確定分）。希薄化後に301Mが入る |
| **SOUN（保有）** | 488.4→438.6（+11.3%） | Layer3は2026-06-30（Q2）: Wd 438.6・Wb 430.5・反希薄化 13.1（accn 0001840856-26-000022）。**Q3の10-Qは2026-11-09提出**（`submissions.json`）でLayer3は未取り込み。yfの発行済は455.9、implied 488.4（差+7%） | **時点の違い**（Layer3が1四半期遅れ。次の日曜のSEC_Data_Updateで追いつく）。yfの implied と発行済の差7%は種類株（A・B）の可能性があるが、表紙の原本は**未確認** |
| **PLTR（保有）** | 2,409.5→2,573.9（−6.4%） | 2026-09-30: BS 2,409.2、加重平均 基本 2,405.7・希薄化後 2,573.9（accn 0001321655-26-000048）。反希薄化は小さい | **希薄化証券**。Y＝BS（基本の発行済）。希薄化後−基本＝168M（オプション・RSU）がWdに入る。Yは希薄化証券を数えない |
| **SOFI（保有）** | 1,297.8→1,357.8（−4.4%） | 2026-09-30: BS 1,296.6、表紙 1,297.8（2026-10-30）、加重平均 基本 1,292.5・希薄化後 1,357.8（accn 0001818874-26-000070）。反希薄化の記載は2024年の81M（転換社債） | **希薄化証券**（転換社債のif-converted等で+65M） |
| ASTS | 390.1→299.1（+30.4%） | Layer3 Q2 2026-06-30: 基本＝希薄化後＝299.1（赤字のため。accn 0001193125-26-342550）。`CommonStockSharesOutstanding`・表紙が`company_facts`に無い（クラスごとの次元つき）。yfの発行済 300.7、implied 390.1 | **種類株**。yfの発行済（クラスAだけ）≒Wd、implied（全クラス）が+89M。クラスB・Cの換算の扱いは**原本の表紙で未確認** |
| IONQ | 464.7→400.9（+15.9%） | 2026-09-30: BS 408.9（前期末 381.0）、加重平均 基本 388.6・希薄化後 400.9。表紙 432.5（**2026-10-28**、accn 0001193125-26-449829）。yfの implied 464.7（表紙の+7.5%） | **期中・期末後の発行**（期末408.9→表紙432.5が+5.8%）。Yが表紙よりさらに7.5%大きい理由は**未確認**（yfの時点か計算） |
| ONDS | 582.3→503.6（+15.6%） | Layer3は2026-06-30: BS 529.8（前期末 469.1）、加重平均 基本 500.7・希薄化後 503.6。表紙 570.6（2026-08-11） | 期中の発行が大きい＋Layer3がQ3未収載 |
| RXRX | 623.4→543.0（+14.8%） | 2026-09-30: BS 561.1（前期末 534.1）、加重平均 543.0。表紙 622.8（2026-10-26） | **期末後の発行**（期末561.1→表紙622.8が+11%）。Yは表紙と一致 |
| RCAT | 152.7→136.9（+11.6%） | Layer3は2026-06-30: BS 152.7（前期末 121.8、+25%）、加重平均 136.9。表紙 152.7（2026-08-04） | 期中の大きな発行＋Layer3がQ3未収載 |
| LITE | 90.7→101.7（−10.8%） | 2026-09-26: BS 90.6、表紙 90.7、加重平均 基本 92.5・希薄化後 101.7（accn 0001628280-26-069615） | **希薄化証券**（転換社債のif-converted +9.2M）。基本の加重平均が期末の発行済より大きい＝自社株買い／転換の途中 |
| PAYS | 56.9→62.0（−8.3%） | 2026-06-30: BS 56.4、表紙 56.5、加重平均 基本 55.9・希薄化後 62.0（accn 0001683168-26-006021） | 希薄化証券（+6.1M）。Layer3がQ3未収載 |
| FICO | 21.4→23.0（−7.0%） | 年度（2026-09-30）の加重平均 基本 22.7・希薄化後 23.0＝**年度の平均**（Q4の株数が無い）、BS 21.4、表紙 21.4 | **年度の平均**（5銘柄: AAPL・CPRT・FICO・INTU・ZSでQ4の株数が無く年度平均を使う）。自社株買いが進む銘柄では期末より大きい |
| ALAB | 174.8→184.1（−5.0%） | 2026-09-30: BS 174.8、表紙 174.8、加重平均 基本 174.1・希薄化後 184.1 | 希薄化証券（+10M） |
| **判定が変わる** CPRT | 926.3→956.9（−3.2%） | 年度（2026-07-31）の加重平均 基本 936.3・希薄化後 956.9（年度平均）、BS 926.3 | 年度の平均＋希薄化証券。HOLD→TRIMは境目（乖離率の−3%の動き） |
| **判定が変わる** RMBS | 107.1→108.8（−1.6%） | 2026-09-30: BS 107.1、加重平均 基本 107.6・希薄化後 108.8 | 希薄化証券（+1.2M）。WATCH→HOLDは境目 |

残り（5%以下・判定の変化なし）の分類は表のCSVの`cls2`列。

## 3. 何が正しい「1株あたりの分母」か

理論株価は「今の株主の持ち分の価値」を「今の持ち分の数」で割る。分母の候補:

| 候補 | 何を数えるか | 長所 | 短所 |
|---|---|---|---|
| Y yfinance implied（今） | 最新の表紙の発行済（基本、全クラス） | 最新。種類株も全クラスを数える（GOOGL・DELL・META・HEI・TASKでyの発行済より大きい） | **希薄化証券を数えない**（PLTR・SOFI・LITE等で理論株価が5〜11%高く出る）。未確定の株を数える（TSLA）。外部の計算で、期末・時点・クラスが銘柄ごとに違う。部品・DILUTIONと別の基準 |
| 案A 部品の株数（加重平均の希薄化後、直近の四半期、分割換算） | 期間の平均の、希薄化証券込みの株数 | SEC由来で時点が揃う（`first_filed`）。**倍率の部品・DILUTIONの`share_count_change`と同じ関数**（`shares_by_end`）。TSLAの未確定の株は入らず、転換・RSUは入る | 期中に大きく発行／買戻しした銘柄は遅れる（RXRX・IONQ・ONDS・RCATで理論株価が11〜16%高く出る。**発行する銘柄を過大評価する向き**）。年度の平均を使う5銘柄がある（Q4の株数が無い） |
| 案B 期末（表紙・BS）の発行済＋（希薄化後−基本の増分） | 現在の持ち分＋希薄化証券 | 発行済に最も近い。RXRX型を直す | **TSLAで二重に数える**（未確定の株713Mが発行済に入っているうえ、希薄化の増分301Mも足す＝4,254M）。表紙が複数クラスの合計かクラスAだけかが銘柄で違う（LLYで表紙940.7に対しWd 893.4）。種類株（ASTS・PLTR・SOUN）は表紙の次元つきの値が取れず代わりが要る |

**勧め: 案A**。理由:
1. 1つの部品に揃えられる。倍率の部品の時価総額（MULTIPLES-ADOPT-1）・DILUTIONの株数の変化と**同じ株数**になり、同じ画面に違う株数が並ばない
2. 分母の定義が「希薄化後・加重平均」なので、希薄化証券（PLTR・SOFI・LITE）とTSLAの未確定の株の扱いがEPSと同じ基準で決まる
3. 弱点（期中・期末後の発行）は、DILUTIONで決めた**補助の印`outstanding_gap_pct`**（表紙÷加重平均−1が10%超で表示）が既に出す設計なので、同じ印を理論株価の画面にも出して、遅れの大きい銘柄（RXRX・IONQ・ONDS・RCAT・JOBY）に「株数が最新でない」と示す
4. 将来、期末の発行済を使いたければ、部品に`outstanding_diluted`（案B）を別の列として足す。案Bを標準にするのは、未確定の株と種類株の扱いを先に決めてから

**Vのように欠ける銘柄**: 部品の株数が無い（V）・100,000以下の場合は、今のyfinanceのimplied（種類株の全クラスを数える）に**印つきで**戻す（`_shares_source=yf_fallback`）。MULTIPLES-ADOPT-1の時価総額の代わりと同じ形。Vは理論株価が2つの株数の基準で動くため、画面に`yf値`の注記を出す。Layer3の取り込みを直す案（生のXBRLの次元つきの値を換算率で換算）は別の設計。

**年度の平均を使う5銘柄**（AAPL・CPRT・FICO・INTU・ZS）: Q4の株数が無く、部品は年度の加重平均になる。四半期の値（Q4）が取れれば直す、取れない間は`outstanding_gap_pct`の印を出す。

## 4. 全銘柄の試算（`tools/sim`、TANUKI→HypeCore→TANUKI、96銘柄）

前＝kaihatsu。後＝`diluted_shares`を部品の株数（案A）／期末の発行済＋希薄化の増分（案B）に差し替え（`_shares_source`を変えて、Vは今のまま）。

| 見るもの | 案A | 案B |
|---|---|---|
| 理論株価が変わる銘柄 | 92 | 77 |
| 5%超 | 12（ASTS +30.4%・IONQ +15.9%・ONDS +15.6%・RXRX +14.8%・TSLA +11.6%・RCAT +11.6%・SOUN +11.3%・LITE −10.8%・PAYS −8.3%・FICO −7.0%・PLTR −6.4%・ALAB −5.0%） | 10（ASTS +30.4%・SOUN +11.3%・LITE −9.2%・PAYS −9.1%・CAKE −8.6%・TSLA −7.1%・PLTR −6.5%・CART −5.8%・LLY −5.5%・ALAB −5.4%） |
| 判定が変わる | 2（CPRT HOLD→TRIM −3.2%・RMBS WATCH→HOLD −1.6%） | 1（RMBS） |
| funda点 | 変化0 | 変化0 |
| timing点 | 2（JOBY 25→35・RMBS 35→25） | 1（RMBS） |
| HypeCoreのstage（月次の履歴ごと） | 変化0 | 変化0 |
| 保有9銘柄の判定 | 変化0（ADBE −1.6%・APP −1.0%・CELH −1.7%・CRWV +0.1%・NVDA −0.6%・PLTR −6.4%・SOFI −4.4%・SOUN +11.3%・TSLA +11.6%） | 変化0（TSLA −7.1%〈二重に数える〉・SOUN +11.3%・PLTR −6.5%・SOFI −4.8%） |
| STONKS | 点数・判定は`diluted_shares`を読まない（時価総額は部品・yfのまま）。変化0 | 同じ |
| `sim.py scope`（理論株価0.05%超の変化） | 95銘柄中**95**（全員の理論株価が動く。株数の定義が全銘柄に効く＝method_changeの印は全銘柄に付く） | 80 |

案Aで動く理論株価の方向: 発行済が加重平均より大きい銘柄（TSLA・SOUN・RXRX・ONDS・RCAT・IONQ・ASTS）は**上がる**（分母が小さくなる）、希薄化証券が大きい銘柄（PLTR・SOFI・LITE・PAYS・ALAB）は**下がる**（分母が大きくなる）。

## 5. 未公開の5つとの重なりと公開の順の案

| 未公開 | 重なる箇所 | 影響 |
|---|---|---|
| PARSER-LAYER3 段1（revert済み） | `layer3_builder.py`・`bs_lines.py`（借入金・純資産・負債合計） | 株数（`shares_diluted`）は`SPLIT_FIELDS`の扱い。株数の値は変わらない見込みだが、段1の再検証で確かめる |
| TANUKI-CALC-CONTINUITY-1 | `q4_implied.py`（負のQ4）、`pipeline.py` | 株数のQ4の逆算（`_QUARTERLY_NO_DIFF_FIELDS`）に当たらない。重ならない |
| DILUTION-DEFINITION-UNIFY-1 | `common/dilution.py`（`share_count_change`、`outstanding_gap_pct`）、`pipeline.py`の`_load_extra_data`の希薄化率、`stock.html` | **同じ株数**（`shares_by_end`）の基準。`outstanding_gap_pct`が本件の補助の印になる。`pipeline.py` L3217（`_yf_shares`）はDILUTIONが書き換える行 |
| DEFICIT-QUALITY-ADOPT-1 | `pipeline.py`・`tanuki_score/index.html`・STONKS | 株数と重ならない |
| MULTIPLES-ADOPT-1 | `data_fetcher.py`（PER・PSR・時価総額）、`core_calculator.py`（`diluted_shares`の出力の周辺）、`stock.html`の時価総額 | 本件は同じ`data_fetcher.py`の`_determine_diluted_shares`と`core_calculator.py`の`diluted_shares`の行を変える。時価総額の`market_cap_sec`（`株価×部品の株数`）と**同じ株数**になる |

**公開の順の案**: PARSER段1 → CONTINUITY → DILUTION → DEFICIT → MULTIPLES → **本件（最後）**。理由: ①本件は理論株価を全銘柄で動かす計算方法の変更で、他の5つの上に積んでから試算し直すのが安全 ②DILUTIONの株数の基準とMULTIPLESの時価総額が先に揃うので、本件は「分母を同じ部品に付け替える」だけになる ③method_changeの印は全銘柄に付く（scope 95）ので、他の公開と同じ日に重ねない

## 6. 決めてほしいこと

1. 分母は**案A（部品の株数＝加重平均の希薄化後）**でよいか。案B（期末の発行済＋希薄化の増分）にするなら、未確定の株（TSLA）と種類株の扱いを先に決める必要がある
2. 期中・期末後に大きく発行した銘柄（RXRX・IONQ・ONDS・RCAT・JOBY）の遅れは、補助の印`outstanding_gap_pct`の表示だけで許容してよいか（理論株価は+11〜16%高めに出る）
3. 株数が欠ける銘柄（V）は、yfinanceのimpliedに印つきで戻す案でよいか
4. 種類株（ASTS・PLTR・SOUN）の表紙の原本を取る許可（`sec.gov/Archives`の取得に連絡先つきのUser-Agentが要る。環境変数`SEC_USER_AGENT_EMAIL`が手元に無い）。取れれば§2の「未確認」を確かめる
5. 年度の平均を使う5銘柄（AAPL・CPRT・FICO・INTU・ZS）のQ4の株数を、実装の前に直すか（QUARTERLY-BASIS-MIX-1型）、印だけで許容するか
