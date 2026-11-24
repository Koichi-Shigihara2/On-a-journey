# SEC-RETAG-RECONCILE-1 STEP 0（読み取りと手元の試算のみ・2026-11-24）

対象: [[SEC-RETAG-RECONCILE-1]]。コード・データは変えていない。材料は `common/sec_data/data/{T}/company_facts.json`（原本の申告の写し）・`ttm/`・`normalized/`・`quarterly_*.json` と、TANUKIの今朝の出力。
削除した14銘柄（KULR・SPIR・MOほか）は対象外（BACKLOGの注のとおり。残る該当は APP・ENTG・FROG・LITE と COHR・DOCN）。

## 1. 最初に: APPのttm D&A 289.9M→133.2M は何に効くか

**原因はタグの漏れではなく、事業の売却（Apps）による組み替えの境目。**

| TTMの終わり | 4四半期（M） | 合計 |
|---|---|---|
| 2025-09-30 | Q4'24 約127.8（Apps込み・旧基準）＋Q1'25 79.9（旧基準）＋Q2 47.1＋Q3 35.1 | 289.9 |
| 2026-09-30 | Q4'25 32.7（年次−9か月の逆算）＋Q1 33.7＋Q2 32.6＋Q3 34.2 | 133.2 |

- 今のTTM 133.2Mは4四半期とも継続事業の基準（32〜34M）でそろっていて、**値は正しい**。落ちて見えるのは前の点（〜2025-09-30）が旧基準を含むから（QUARTERLY-BASIS-MIXの型。D&Aは`basis_mix`の対象外）
- D&Aの読み手と効き方（全部読んだ）:
  - 理論株価・判定: **効かない**。TTMのD&Aは`fcf_estimation`の「FCF内訳」の表示だけ（DCFに使わない）。成長率の検算`calc_fundamental_growth`は最新の**年次**（FY2025: D&A 194.8M・CapEx 0）を使い、再投資がマイナスで`g_fundamental`＝−6.2%。重みは`_weight_ramp`で負なら0なので、D&Aがどう直っても推奨成長率に入らない（APPは変わらず BUY・IV 1,384ドル・funda 100）
  - stage（HypeCore）: **効かない**（`src/value/hypecore`にD&Aの読み手なし）
  - TAIL: **効かない**（`src/tail`にD&Aの読み手なし）
  - STONKS: 取得項目に`depreciation_and_amortization`があるが使う所なし（`stonks_silo`の分析は読まない）
  - q4_implied: Q4の逆算の対象（FY2025の年次 194.8M − 9か月＝32.7M。年次が旧基準を含むなら過大の疑い。**要確認**）
- 結論: APPは直す対象ではない。再タグの突き合わせの対象から外す。FINDINGSの「APPは売却の可能性」を「売却の組み替え」に確定

## 2. 残る該当を型ごとに分類

| 型 | 銘柄・項目 | 原因 | 値に効くか |
|---|---|---|---|
| A 再タグ（候補のタグが最新の申告で欠け、別のタグに付け替わった） | COHR D&A（無形資産の償却 `AmortizationOfIntangibleAssets`→`OtherAmortizationOfDeferredCharges`） | 候補に新しいタグが無い | 表示のみ（ttm D&A −69.5M） |
| B 分類の漏れ（bs_linesの役割） | DOCN 短期投資（`MarketableSecurities`・Currentなし。2026-09-30 178.6M） | `short_term_investments`の分類が`MarketableSecuritiesCurrent`等だけ | **IVに効く**（下の3） |
| C 合計のタグが部分の値 | ENTG D&A（`DepreciationDepletionAndAmortization`の中身が減価償却だけ。FY2025は205.3M、無形資産の償却 `AmortizationOfIntangibleAssets` 184.4Mが別。FY2023は合算の387.2M） | 合計のタグが優先され、部分の足し算（`PARTIAL_SUM_FIELDS`）が働かない | 表示のみ。TTM 156.1Mは実際（約340M）の半分以下。IV・判定は0（成長率は年次、負→重み0。ENTGの`g_fundamental`は要確認） |
| D 年次と9か月の基準差でQ4の逆算が負 | FRSH SBC（FY2025 146.8M < 9か月 149.5M → Q4逆算 −2.65M → 最新のTTMが欠け） | 年次と四半期の基準がずれた（再申告・組み替え） | 表示（SBCの最新のTTMが欠け）。`q4_implied_negative`の印が出る型 |
| E 実際の減少（直さない） | FROG D&A 0.80倍（無形資産の償却が3.0Mへ）・TASK SBC 0.62倍（SBCが実際に減った）・APP（上） | 値は正しい | なし |
| F 原因が未特定 | LITE SBC（最新のTTMだけ欠ける。四半期4点（45.4・41.6・40.8・132.5M）とも値あり。最新Q 132.5Mは直近の3倍） | 守り・外れ値の除外の疑い。**再タグではない** | 表示のみ。STEP 1で追う |

追加の発見（再タグの型ではない・別件）:
- **`quarterly_*.json`のD&Aが部分タグの値になっている四半期**（TTMは正しい）: GOOGL 2026Q2/Q3＝367・375M（実際は約8.7B）・DELL 100M（実際は約750M）・HQY 0.5M（実際は約42M）・FROG 0.8・1.1M・ADSK 13M・INTU 47M。parser側（`quarterly_*.json`）と Layer3（TTM）の食い違いで、**PARSER-LAYER3-SINGLE-SOURCE-1の残り**。`quarterly_*.json`のD&Aを読む所は今回の調査で valuation・HypeCore・TAIL に見つからず、値への影響は0（FINDINGSに1行）
- APPのCapExが0（OCF 4.9B）。CapExの取りこぼしの疑い（成長率の検算では結果が変わらないが、FCFの内訳表示に出る）。**要確認**（FINDINGS）

## 3. DOCNの短期投資（値に効く唯一の件）

- 原本: 2026-09-30の`MarketableSecurities`＝`AvailableForSaleSecuritiesDebtSecurities`＝178,645,000（2025-12-31は0）。今の`bs_adjustment.short_term_investments`は0（`sti_estimated_zero`・`sti_quarterly_missing`）
- 1株あたり: 純現金 −4.62ドル/株（−531.7M ÷ 約115.1M株）→ +178.6M で **約+1.55ドル/株**
- 理論株価 57.44 → **約58.99（+2.7%）**。upside −27.3% → **約−25.3%**。判定の境（−25%）まで約0.3ポイント。判定は動かない見込みだが、境に近い
- 同じ分類の漏れの全銘柄の走査（company_facts × 最新四半期のbs `short_term_investments`、82銘柄）: **値に効くのはDOCNだけ**。ほかは AFSの注記の合計（CAT 3,994M・AMZN・NOW・LYFT・IONQ・KO・JNJ等＝現金同等物を含む合計で、バランスシートの行ではない）か、バランスシートの行が既に取れている銘柄。**小さい要確認**: CDNS 80.7M・DELL 82M・TER 95.7M（バランスシートの行かは`bs_lines`のroleで見ないと決められない）

## 4. 案

**案1: Layer3の候補選びの1か所に「再タグの突き合わせ」**（BACKLOGのとおり）
- 最新の申告で候補のタグが欠けた項目について、同じ申告の前年同期の比較値が前の申告の候補のタグの同じ期と0.5%以内で一致する別のタグを別名に採る（印`retag_tie_out`）。合わないものは採らない
- 効く型: A（COHR）。C（ENTG）は「合計のタグの中身が部分」なので、突き合わせではなく **「合計のタグと部分の足し算の値が合わないとき（FY2023は合算・FY2024から部分）は足し算を優先」の別の規則**が要る。分けて扱うこと
- D（FRSH）は再タグではない。`q4_implied_negative`の守りで既に出る。直すなら年次の基準（組み替え後の年次との突き合わせ）で別件

**案2: bs_linesの分類の漏れ**
- `short_term_investments`に`MarketableSecurities`（Current無し）を足す。`AvailableForSaleSecuritiesDebtSecurities`は注記の合計の会社（CAT・NVDA・KLAC・SOFI等）があるので、バランスシートの行（role）に限る（DOCNは`MarketableSecurities`だけで足りる。AFSを足さなくても直る）
- 全銘柄で動くのはDOCNだけの見込み（走査）。CDNS・DELL・TERはroleを見て決める

**全銘柄の試算（STEP 1の入口）**: 未実施（実装が要るため）。手順は`tools/sim/sim.py chain before/after`→`compare`→`scope`。見込みは、理論株価・判定の変化＝DOCNだけ（+2.7%、判定は動かない見込み）、保有銘柄の変化＝0（DOCNは保有なし。保有のAPPは直す対象外）、HypeCore stage＝0（D&A・短期投資とも読み手なし。要試算で確認）、scope＝DOCN（IV）＋表示だけのCOHR・ENTG・FRSH・LITE

## 5. 決めてほしいこと

1. **案1を再タグ（A）だけに絞る**か、C（ENTG型の合計のタグが部分）も同じ公開に入れるか。入れるなら規則が別になる
2. **案2はDOCNの`MarketableSecurities`の1語だけ**足す（推奨）か、AFSも足してroleで絞るか
3. LITE（F）は再タグの件から外し、別の件として追うか
4. APP（E）は「売却の組み替え・値は正しい」で確定してこの件から外す（推奨）
5. `quarterly_*.json`のD&Aの食い違い（GOOGL・DELL・HQYほか）をFINDINGSに1行置く（PARSER-LAYER3の残り）
