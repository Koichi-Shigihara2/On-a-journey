# PARSER-LAYER3-SINGLE-SOURCE-1 段1の前の守り（valの検査）＋段1（四半期の貸借対照表）のSTEP 0

2026-11-11。読み取りと手元の試算だけ（実装・公開なし）。試作は `tools/sim/parser_l3/val_guard_proto.py`（common には入れていない）。
表: `PARSER_LAYER3_VAL_GUARD_hits.csv`（守りの当たり全件）・`PARSER_LAYER3_STAGE1_cells.csv`（段1で変わる527セル）・`PARSER_LAYER3_STAGE1_movers.csv`（動く銘柄・必須の6銘柄・保有9銘柄と原本の申告での理由）・`PARSER_LAYER3_STAGE1_longtermdebt.csv`（長期借入金のタグと申告の行の差）・`PARSER_LAYER3_STAGE1_equity_nci.csv`（非支配持分を含む純資産の全期）。

## 1. valの守り（FINDINGS 2026-11-11のCOHR・APPを1つの規則に）

Layer3の `val` は「その期を最後に載せた申告の値」。最新の申告に取得元の誤りや内訳の欠けがあると、そのまま入る。`val` と `first_val` が違う期は3,583（全92,493期の3.9%）。ほとんどは本物の再表示・組み替えなので、「違う」だけでは悪化と言えない。見分ける3つの規則を全銘柄・全期で当てた。

| 規則 | 条件 | 当たり | 目で確かめた結果 |
|---|---|---|---|
| (i) 単位の誤り | val÷first_val が10の累乗（誤差0.3%）。持ち株数・1株の値は分割で10倍・100倍が正しく起きる（KLAC・LRCX・CSGP・NVDA、すべて val が正）ので |k|≥3 だけ。同じ期末で5項目以上が first_val から20%超動く期は期全体の組み替え（SPAC合併）として対象外。どちらが誤りかは隣の期（同じ種類で前後2期ずつの中央値）に近い方を正とする | 7件: val が誤り3（COHR 2023年次D&A 681,687,000,000＝1,000倍、HWM 2022・2023の1株配当 40000・50000＝100万倍）、first_val が誤り4（COHR 2020Q1のEPS 70・60、ASTS・IONQのSPAC合併前の殻の会社） | **val が誤り3件はすべて真の誤り。first_val が誤りの4件は触らない**（隣の期が val に近い）。誤検知0 |
| (ii) 内訳の行が減った | cf_lines の capex（有形固定資産＋ソフト開発費）で、最新の申告の内訳の行が、同じ期を載せた前の申告より少ない → 内訳の揃った申告のうち最新を採る | 16期・4銘柄（APP 7・IONQ 5・DELL 2・ONDS 2） | APP 2021・2022は原本で確認（2023-02の10-Kに ppe＋ソフト4.067M・5.949M があり、parser 5.457M・6.611M と同額）。残る14期は、最新の申告が行を落とした理由が組み替え（別の行に移した）か欠けかを原本で読む必要がある（値が変わるのは内訳が減った分だけ。DELL 2,796→2,812M は16M） |
| (iii) 隣の期から外れる | val が隣の期の中央値から 10^0.5（約3.2倍）以上外れ、first_val は 10^0.3（約2倍）以内。持ち株数・1株は対象外 | 15件 | **真の誤りは1件だけ**（MO 2024-06-30の無形資産: 2025-07-30の10-Qが日付2024-06-30の値を3.6Bと報告。2024-07-31の10-Qは13.048B。first_valが正）。BSY 2020Q4の自社株買いは要確認（2023年2月の10-Kだけ2020年が0、Q4逆算が−72M）。**ほか13件は本物の再表示・訂正（CELH 3・SITM・META・PLTR・ADSK・RCAT・TSLA・AVAV）かQ4・単独四半期の逆算の基準混在（HWM・SOFI）かSPAC合併後の組み替え（ASTS）で、val が正** |

**提案:**
- **(i)と(ii)を採る**。当たったときは first_val（(ii)は内訳の揃った申告の値）を採り、エントリに `val_guard`（理由: `pow10`・`components_dropped:software` など）と `val_latest`（置き換え前の val）を残す。銘柄の名前は書かない。(i)は `build_ticker_store` の最後（`strip_asof` の前）の1か所、(ii)は `cf_lines.period_values`（同じ期を載せた申告の中で内訳の種類がいちばん多い最新の申告を採る）
- **(iii)は値を置き換えない**。15件中1件しか真の誤りでなく、置き換えると誤検知13件の再表示が元に戻る。実際に試すと、HypeCoreの過去の月が3銘柄で動いた（AVAV の2026-04は stage 2→4、CELH 11か月・SITM 19か月の fundamental_score）。印だけ（`val_suspect`）にして一覧に出す案。MOのような「申告が日付を取り違えた事実」は、同じ期末日の別の申告と比べても再表示と見分けられず、機械では決まらない（FINDINGSに登録）
- (i)と(ii)だけの守りを入れた試算（`tools/sim/sim.py chain`、ID `s1_guard2`）: 理論株価の変化0・判定の変化0・HypeCoreの出力は過去の月まで含め生成時刻以外の差0。値が変わるセルは (i) 3セル・(ii) 16期

## 2. 段1（四半期の貸借対照表をLayer3の val から作る）の試算（守り(i)(ii)を入れた後の値）

parserの四半期 `bs` の値のうち、Layer3の val と違う**527セル（50銘柄）**を val で置き換える。parserにだけある値（44）と、Layer3にだけある値（1,651、のれん・優先株の0の補い等）はそのまま（0で埋めない、決定(c)）。

| 項目 | 置き換えるセル | | 項目 | 置き換えるセル |
|---|---|---|---|---|
| total_liabilities | 311 | | long_term_debt | 28 |
| stockholders_equity | 94 | | cash_and_equivalents | 15 |
| short_term_debt | 40 | | current_liabilities | 14 |
| total_assets | 9 | | current_assets | 5 |
| short_term_investments | 4 | | goodwill・minority_interest | 3・3 |

そのうち164セルは「parser＝最初の申告の値、Layer3 val＝その後の再表示」（改訂の違い）、363セルは定義の違い。total_liabilities の311は、parserが資産−純資産で非支配持分・一時的持分を負債に混ぜていたもの（段0で Layer3 を直した結果）。

| 見るもの | 結果 |
|---|---|
| 理論株価 | 5%超 **1銘柄**（CAKE +29.1%）、5%以下の変化 3銘柄（KO −3.2%・HWM −1.1%・ADSK −0.6%）、残り92銘柄は ±0.0% |
| 判定 | 変化 **0** |
| 保有9銘柄 | **全て ±0.0%・判定の変化0**（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA） |
| 必須の6銘柄 | SOFI・TSLA・ABBV・DDOG・HQY は ±0.0%（段1では値が変わるセルが無い。以前の試算で動いたのは年次・CF・0の補いなど後の段の置き換え）、CAKE は下のとおり |
| HypeCore stage | 変化 **0**（96銘柄の出力が過去の月まで含め生成時刻以外一致。`monthly` の `price_iv_ratio` だけ4銘柄で動く） |
| STONKS | 変化 **0**。STONKS が読むのは年次と `get_net_cash` の現金（runway）で、4銘柄は STONKS の対象でなく、4銘柄とも現金は変わらない |
| 調整後EPS | 変化 **0**（貸借対照表を読まない） |
| `get_net_cash`（TANUKI・STONKS共通） | 98銘柄中 4銘柄だけ変わる（ADSK・CAKE・HWM・KO。借入金のみ、現金は不変） |

### 動く4銘柄の理由（原本の申告で確認。全件は `PARSER_LAYER3_STAGE1_movers.csv`）
| 銘柄 | 原本の申告 | 結論 |
|---|---|---|
| CAKE +29.1%（38.75→50.00） | 2026-11-02の10-Q（0001104659-26-121460）の貸借対照表 2026-09-29: 長期借入金は流動分563.7M（`LongTermDebtCurrent`）の行だけで、非流動の行は無い（前年度末の2025-12-30には非流動561.3M・流動68.8M）。parserは前年度末の非流動561.3Mを今期にも残して流動分と**二重に数えていた**（純現金 −875.7M）。Layer3は流動分だけ（−314.4M） | **Layer3が正**。STEP 0で予想した「Q3に長期→短期の動き」は、満期が1年以内になった転換社債の組み替え |
| KO −3.2% | 2026-10-28の10-Q 2026-10-02: 1年内返済の長期債務4.923B（`LongTermDebtAndCapitalLeaseObligationsCurrent`）。parserの短期借入1.495Bは2025-12-31のコマーシャルペーパー（その後の期にタグが無い古い値）。純現金 −24.4B→−27.9B | **Layer3が正** |
| HWM −1.1% | 2026-10-29の10-Q 2026-09-30: 短期借入390M（`ShortTermBorrowings`）と1年内返済1M（`LongTermDebtCurrent`）の2行。parserは1Mだけ、Layer3は390Mだけ | Layer3が近いが**両方とも1行落とす**（正しい合計は391M） |
| ADSK −0.6% | 2026-08-28の10-Q 2026-07-31: `NotesPayableCurrent` 499M と `ShortTermBorrowings` 994M の2行。parserは499M、Layer3は994M | **両方とも1行落とす**（正しい流動の負債は1,493M） |

→ **発見**: 流動の借入金は申告ごとに複数の行（短期借入・1年内返済・転換社債・コマーシャルペーパー）に分かれ、Layer3の `short_term_debt` は候補から最初の1つだけを採る。parserも別の1つを採る。段1の公開の前に、`short_term_debt` を「集計のタグ（`DebtCurrent`）があればそれ、無ければ重ならない流動の行の合計」に直す必要がある（ADSK・HWMが正しい値になる）。

## 3. 段1で判断すると表にした候補タグの扱い（案）
| 候補 | 調べた結果 | 案 |
|---|---|---|
| `long_term_debt` の `LongTermDebt` | 252期・23銘柄で採用。申告の貸借対照表の行（bs_lines）と比べられる期は175期で、**70期・10銘柄で違う**（`PARSER_LAYER3_STAGE1_longtermdebt.csv`）。そのうち23期（ADSK 9・ZS 4・RXRX 4・ELF 3・RCAT 2・TSLA 1）は差が `short_term_debt` とほぼ同額＝1年内返済を含む合計と二重に数えている。ほかは額面と帳簿価額の差（ADSK 2021: 1,650M対1,637M）。**STEP 0の予想だったADBE 2022は二重でない**（`LongTermDebt` 3,629Mは非流動分で、流動分の `DebtCurrent` 500Mは別。合計4,129M＝申告の帳簿価額）。内訳のタグが無く機械で決まるのは252期中0期 | 申告の貸借対照表の行（bs_lines）の非流動の行があればそれを正にし、無いときだけタグ。違う期は `capex_basis` のように印を残す |
| `short_term_debt`（上の発見） | 4銘柄のうち2銘柄（ADSK・HWM）で、どちらの側も流動の借入金の1行を落とす | 集計のタグ（`DebtCurrent`）→無ければ重ならない流動の行の合計 |
| `stockholders_equity` の `...IncludingPortionAttributableToNoncontrollingInterest` | 202期・16銘柄。最新の期に効くのは ASTS（非支配持分が純資産の21%）と VZ（1.2%）だけ。ほかは過去の期か0 | タグが非支配持分込みで `minority_interest` の値があるときは、その額を引いた親会社の純資産にする。値が無いときは今のまま印（`equity_incl_nci`）。表: `PARSER_LAYER3_STAGE1_equity_nci.csv` |
| rpo の `ContractWithCustomerLiability`・`DeferredRevenue` | 貸借対照表の項目でなく、段1では触らない。348+315+64+2期（段0の表）で受注残でなく前受金が入る | rpoの段で試算してから決める（候補から外すと、これらを採る銘柄〈最大38〉のrpoが空になり、TANUKIの `adjust_rpo` が動かなくなる銘柄が出る） |

## 4. 段1の公開に進む前に
1. `short_term_debt` の定義の直し（上）と、直した後のADSK・HWM・KO・CAKEの再試算
2. (i)(ii)の守りの実装（`val_guard`・`val_latest` の印、`cf_lines` の内訳の規則）と、(ii)の14期の原本確認
3. 公開は計算方法の変更の手順（ID `PARSER-LAYER3-SINGLE-SOURCE-1-1`）。理論株価が5%を超えて動くのは CAKE の1銘柄（原本で確認済み、Layer3が正）
