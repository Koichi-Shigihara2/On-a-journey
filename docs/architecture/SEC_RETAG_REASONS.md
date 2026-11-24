# SEC-RETAG-RECONCILE-1 実装の試算の結果（2026-11-24・ID `SEC-RETAG-RECONCILE-1-1`）

STEP 0（`SEC_RETAG_STEP0.md`）の決定に沿った実装。試算は `tools/sim/sim.py chain before/after`（前＝HEADのworktree、後＝この変更と派生ファイルの作り直し）。Grok・SEC・Discordは呼んでいない。

## 実装（銘柄名は手で書いていない）

| 何を | どこで | 規則 |
|---|---|---|
| 再タグの突き合わせ（COHR型） | `layer3_builder.retag_alias_rows`（`_merge_candidate_entries`の1か所） | 会社の最新の申告3本のうち、候補のタグの最後の申告より後のもの（A）で、候補のタグに値が無いとき、Aの別のタグuの前年同期の比較値が、前の申告の候補のタグの同じ期の値と0.5%以内で一致し（一致1つ以上・食い違い0・uの名前が項目の目印に当たる・uが1つに決まる）、uのAの値のうち候補のタグに値が無い期を採る。印 `retag_alias`・`source_tag="…@retag:u"`。対象はD&A・SBC・CapEx。営業CFは継続事業の組み替えがあるので自動の別名にしない |
| 合計のタグが部分（ENTG型） | `layer3_builder._add_partial_sum_series` | 合計のタグ（`DepreciationDepletionAndAmortization`等）の値が、減価償却・無形資産の償却のどちらか1つに0.5%以内で一致し、両方がある期は和を採る（印 `total_tag_was_partial`）。合計が本物の期（部分のどちらとも一致しない）は合計のまま。部分の1つしか無い期は値を変えず印 `partial_concept_only` |
| `quarterly_*.json`のD&A | `quarterly_bs.replace_record`（`REPLACED_FIELDS`） | parserの四半期のcfのD&Aを、同じ期のLayer3の値で置き換える（bsと同じ規則。ほかのcf・PL・株数は置き換えない） |
| 短期投資の分類 | `bs_lines.RULES` | `MarketableSecurities`（Current無し）を足した。`AvailableForSaleSecuritiesDebtSecurities`は足していない（下の走査） |
| 保存済みの申告の埋め戻し | `bs_lines.backfill_lines_from_company_facts`（`python common/sec_data/bs_lines.py --backfill`） | 分類に足した行（`MarketableSecurities`だけ）を、companyfactsの同じ申告・同じ時点の値で足す。SECは呼ばない |

## AFSを足さなかった理由（決定2）

全銘柄の保存済みの貸借対照表の行（`bs_lines/*.json`の`concepts`＝表の全部の行）を走査し、短期投資に近い名前で分類に当たらない行を探した。流動資産の取りこぼしが実際にあったのは **`MarketableSecurities`（DOCN・KO・ESTC）だけ**。`AvailableForSaleSecuritiesDebtSecurities`が表の行になっている銘柄は無く、注記の合計（CAT・AMZN・NOW等）にしか出ないので、足さない。

## 動く銘柄と理由（決定(a)）

**scope（`config/method_change.json`、5銘柄）**

| 銘柄 | 何が動くか | 理由 |
|---|---|---|
| DOCN | 理論株価 57.44→58.99（**+2.7%**）、判定HOLD→HOLDのまま | 短期投資178.6M（`MarketableSecurities`、2026-09-30）が分類に無く0だった。+1.55ドル/株 |
| COHR | `fcf_estimation.da` 476.8M→546.2M（表示のみ） | 最新の10-Qで無形資産の償却が`AmortizationOfIntangibleAssets`→`OtherAmortizationOfDeferredCharges`に付け替わった（前年同期の比較値が一致）。最新四半期 77.7M→147.1M |
| ENTG | `fcf_estimation.da` 156.1M→340.4M（表示のみ） | 合計のタグが減価償却だけの値。無形資産の償却（約184M/年）が別の行 |
| MSCI | `fcf_estimation.da` 26.1M→197.9M（表示のみ） | 同上（合計のタグ＝減価償却だけ26M、無形資産の償却が別） |
| FLYW | `fcf_estimation.da` 28.0M→29.7M（表示のみ） | 最新の10-QでD&Aが`OtherDepreciationAndAmortization`に付け替わった（前年同期の比較値が一致）。最新四半期 7.4M |

**scopeに入らない変化（出力に出ない）**

| 何 | 件数 | 理由 |
|---|---|---|
| `quarterly_*.json`のD&A | 704四半期（60銘柄） | parser側の部分タグをLayer3の値で置き換えた（GOOGL 375M→約8.7B、DELL 100M→約750M、HQY 0.5M→約42M、FROG 1.1M→4.0M ほか）。減ったのは17四半期（ALAB・CELH・ELF・GTLB・HON・HWM・RCAT・RMBS。Layer3が組み替え後・別のタグの値を持つ期。ALABは1.4M→1.39Mのような丸めの差）。読み手は valuation・HypeCore・TAIL に無く出力は変わらない |
| TTM（過去の点） | CRWV 2025-09-30 1,998→1,999M、JOBY 2025-09-30 36.1→38.6M | 同じ規則。最新の点には届かない |
| bs_lines | DOCN 1行・ESTC 8行・KO 23行 | `MarketableSecurities`の埋め戻し。ESTC・KOは会社のデータ（companyfacts）に値があり、Layer3の短期投資は変わらない |

## 試算の結果

- 理論株価が変わるのは **DOCNだけ（+2.7%）**。5%を超える銘柄なし
- 判定が変わる銘柄 0。保有9銘柄（ADBE・APP・CELH・CRWV・NVDA・PLTR・SOFI・SOUN・TSLA）の理論株価・判定の変化 0
- HypeCoreのstage: 82銘柄すべて変化 0
- 止める条件（保有の判定が変わる／5%超）には当たらない

## 決定外の確認

- **APPのCapEx 0**: 2026年9か月の投資CF全体が25.4M（`PaymentsForProceedsFromOtherInvestingActivities`）で、営業CF 4.9Bの約0.6%。CapExの専用のタグは2024年（`PaymentsToAcquirePropertyPlantAndEquipment` 4.8M）が最後で、2025年は無形資産の取得（`PaymentsToAcquireIntangibleAssets` 28.3M）。実際に小さく、タグも「その他の投資」で、CapExとは決められない → 入れない（FCFへの効きは最大でも約0.6%）
- **LITE**: 外した（FINDINGS）
