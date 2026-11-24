# コードに埋まった14銘柄の例外の一覧（2026-11-24・一覧だけ。消すのは後）

対象: KULR・QBTS・SPIR・BBAI・RDW・ONDS・RXRX・FCX・SCCO・XOM・PEP・PM・MO・VZ（[[UNIVERSE-REBALANCE-2026Q4-1]]で抹消）。
検索: `git grep -nIE`（語の境界）で `*.py *.js *.html *.yml *.yaml`、除外は tests・docs/architecture・.sim_out・docs/*/data。**コードの行はほぼ銘柄別の分岐・辞書・集合の中身、コメントの行は銘柄名を例に挙げた説明**（機械の分け方で、境目の行は誤りうる）。

**合計 172 行 = コード 58 行 ＋ コメント 114 行（32 ファイル）**。tests・設定・データは別（テスト10件は11-23に直し済み）。

## 1. コードの行（銘柄別の分岐・辞書。消す候補）

| ファイル:行 | 銘柄 | 内容（先頭110字） |
|---|---|---|
| `browser_checks/check_valuation_chart_basis.py:23` | RXRX | `python browser_checks\\check_valuation_chart_basis.py RXRX NVDA ALAB  # 指定銘柄` |
| `common/screening/dcf_validity_checker.py:8` | MO | `2026-07-10のサテライト投資候補10銘柄の手作業チェック（MO/FICOで発見した` |
| `common/screening/dcf_validity_checker.py:45` | MO | `python common/screening/dcf_validity_checker.py AAPL FICO MO --output out.json` |
| `common/sec_data/cf_lines.py:32` | VZ | `- VZ・LOARの quarterly.py の capex_concept は残す（やめると2021年より前の年次・四半期のCapExが消える。cf_linesは` |
| `common/sec_data/fetcher.py:38` | XOM | `（[[SEC-XOM-CIK-HOLDING-REORG-1]]、2026-10-12）。"""` |
| `common/sec_data/fetcher.py:51` | XOM | `（[[SEC-XOM-CIK-HOLDING-REORG-1]]、2026-10-12にSECFetcherの静的メソッドから移した）。` |
| `common/sec_data/layer3_builder.py:146` | BBAI・SPIR | `_ANNUAL_MISCLASSIFICATION_FIX_TICKERS = frozenset({"BBAI", "DELL", "SPIR"})` |
| `common/sec_data/layer3_builder.py:705` | ONDS | `を持つ下位タグより先に採用されてしまう問題があった（ONDS` |
| `common/sec_data/layer3_builder.py:1757` | SPIR | `（RCAT/SPIR/CPRT）全てでLayer3側の値がより正確・現行であることを` |
| `common/sec_data/layer3_builder.py:1764` | SPIR | `- SPIR: 同一accn・同一end日で、normalized/はLongTermDebt` |
| `common/sec_data/parser.py:900` | BBAI・ONDS・RDW | `採用されているケースを是正する（実例: BBAI/RDW/RKLB/SOFI/VRT/ONDS/` |
| `common/sec_data/parser.py:901` | KULR | `KULR(2016)、段階2追加によりSPIR(2020)も対象）。` |
| `common/sec_data/parser.py:924` | KULR | `条件④なしでは、KULR(2019)のshort_term_debt（矛盾とは無関係な別` |
| `common/sec_data/parser.py:1156` | BBAI・RDW | `Noneの年度、例: ELF 2014/2019・BBAI/RDW/RKLB/SOFI/VRT 2020/2019）は` |
| `common/sec_data/parser.py:1294` | XOM | `算出不可能な年度（gross_profit/R&D/SGAのいずれかが欠落。XOM・` |
| `common/sec_data/parser.py:1685` | SCCO | `比較を避けることで、GOOGL(2008)/HON(2008)/SCCO(2009/2010)等` |
| `common/sec_data/parser.py:1695` | KULR | `現状を維持する（KULR(2019)型の巻き添え防止と同じ設計、` |
| `common/sec_data/parser.py:2031` | ONDS | `- ONDS(2017)実例: gross_profitは本人データ（accn内に` |
| `common/sec_data/parser.py:2350` | FCX | `同一fyキーに複数の本人end_dateが競合した場合（CRM/FCX/CAKE/HON/COHR/AVAV/` |
| `common/sec_data/parser.py:3456` | FCX | `本人データ同士のfyキー衝突（CRM/FCX/CAKE/HON/COHR/AVAV/FICO/NVDA等で` |
| `common/sec_data/parser.py:3464` | FCX | `（AVAV/CAKE/COHR/CRM/FCX/FICO/HONで実在確認済み。決定的なバグであり、` |
| `common/sec_data/parser.py:3704` | FCX | `を防ぐ。実例: FCX(2015)の'RedeemableNoncontrollingInterest` |
| `common/sec_data/quarterly.py:95` | MO・ONDS | `"CRWV/DELL/LRCX/LYFT/MO/ONDS）も同タグを別の意味で申告している"` |
| `common/sec_data/quarterly.py:161` | FCX | `"Operatingは他銘柄（AMZN/CAKE/CAT/FCX/KO/LMT/META/RCAT等）"` |
| `common/sec_data/quarterly.py:264` | BBAI・RDW | `"[[NVDA-STI-TAG-UNIDENTIFIED-1]]・[[BBAI-RDW-RUNWAY-VERIFICATION-1]]参照。",` |
| `common/sec_data/quarterly.py:287` | VZ | `"VZ": {"capex_concept": "PaymentsToAcquireOtherProductiveAssets"},` |
| `common/sec_data/reader.py:49` | ONDS | `最新年度の売上を分母にしており、急成長企業では実態より大幅に高くなっていた（ONDS: 年次` |
| `common/sec_data/reader.py:74` | RDW | `増資（RDW: FY2025 cash $94.5M → 2026Q2 $557.0M）や、現金から短期投資` |
| `common/sec_data/reader.py:75` | BBAI | `への振替（BBAI: 2026Q2 cash $36.3M + 流動AFS $282.9M）を反映するため。` |
| `common/sec_data/reader.py:453` | KULR | `KULR/short_term_debt・RCAT/long_term_debt型: 直近の既知値が非ゼロの` |
| `common/sec_data/reader.py:547` | BBAI・RDW | `するためのフラグ（[[BBAI-RDW-` |
| `common/sec_data/report_consistency_check.py:42` | FCX | `CRM/FCX/CAKE/HON/COHR/AVAV/FICO/NVDAで実在確認済み。parser.pyの` |
| `common/sec_data/report_consistency_check.py:1040` | BBAI | `BBAI・RDWでSAFE(>=24ヶ月)/DANGER(<12ヶ月)の判定逆転（最大7.9倍）を` |
| `common/sec_data/report_consistency_check.py:1042` | BBAI・RDW | `[[BBAI-RDW-RUNWAY-VERIFICATION-1]]（2026-09-25）で一次情報により` |
| `common/sec_data/report_consistency_check.py:1078` | BBAI・RDW | `f"（[[BBAI-RDW-RUNWAY-VERIFICATION-1]]参照）"` |
| `common/sec_data/report_consistency_check.py:1089` | BBAI・RDW | `missing=True、0として計算）銘柄を検知する（[[BBAI-RDW-RUNWAY-` |
| `common/sec_data/report_consistency_check.py:1195` | PM・XOM | `背景: [[JNJ-XOM-PM-FLOOR-RISK-1]]。候補が足りずrecommended_gが算出できない` |
| `common/sec_data/report_consistency_check.py:1211` | PM・XOM | `f"不足分を max(過去のFCF成長率, 0) で埋めた値になる（JNJ-XOM-PM-FLOOR-RISK-1、GROWTH-SANITY-CONTINUITY-1）"` |
| `common/sec_data/report_consistency_check.py:1219` | PM・XOM | `背景: [[JNJ-XOM-PM-FLOOR-RISK-1]]。FCFが減っている企業に年15%成長を` |
| `common/sec_data/report_consistency_check.py:1244` | PM・XOM | `f"{raw:+.1%} → FCFが減っている企業に高成長を仮定している（JNJ-XOM-PM-FLOOR-RISK-1）"` |
| `common/sec_data/report_consistency_check.py:2361` | FCX | `CRM/FCX/CAKE/HON/COHR/AVAV/FICO/NVDA等で実在確認済み（filing代行者側の` |
| `common/sec_data/split_adjust.py:28` | KULR | `reverseで調整済みの値まで二重補正していた（KULR 2024-06-30: 22.68M→2.84M）。` |
| `common/sec_data/split_adjust.py:113` | ONDS | `同じ期末日の次の候補を試す（ONDS 2025-12-31: 年次の加重平均221,769→発行済380,763,481）。` |
| `docs/value-monitor/tanuki_valuation/stock.html:947` | KULR・ONDS | `'AMD', 'NVDA', 'ONDS', 'ASTS', 'BKNG', 'BROS', 'ELF', 'KULR',` |
| `docs/value-monitor/tanuki_valuation/stock.html:948` | XOM | `'LLY', 'TER', 'XOM', 'ALAB', 'IONQ', 'RCAT',` |
| `docs/value-monitor/tanuki_valuation/stock.html:962` | SPIR | `SPIR: {` |
| `scripts/inventory/inventory_layers.py:652` | BBAI | `"休眠（BBAI）は cik_lookup.csv の status の dormant に移した"),` |
| `src/value/adjusted_eps_analyzer/extract_key_facts.py:184` | XOM | `（[[SEC-XOM-CIK-HOLDING-REORG-1]]、2026-10-12）。` |
| `src/value/adjusted_eps_analyzer/extract_key_facts.py:192` | XOM | `（2026-10-12の試算: XOM・GOOGL・DELLの既存の四半期は変化0件。現CIKを先にすると、MRVLの2020-05-02の` |
| `src/value/stonks_silo/pipeline.py:10` | BBAI | `python src/value/stonks_silo/pipeline.py SOUN BBAI  # 指定ティッカーのみ` |
| `src/value/tanuki_valuation/pipeline.py:85` | KULR・ONDS | `"AMD", "NVDA", "ONDS", "ASTS", "BKNG", "BROS", "ELF", "KULR",` |
| `src/value/tanuki_valuation/pipeline.py:86` | XOM | `"LLY", "TER", "XOM", "ALAB", "IONQ", "RCAT",` |
| `src/value/tanuki_valuation/pipeline.py:106` | SPIR | `"SPIR": {` |
| `src/value/tanuki_valuation/pipeline.py:2104` | SPIR | `L.append("    [[KO-SPIR-CF-CAUSE-UNCONFIRMED-1]]参照。]")` |
| `src/value/tanuki_valuation/pipeline.py:2105` | SPIR | `elif ticker == "SPIR":` |
| `src/value/tanuki_valuation/pipeline.py:2110` | SPIR | `L.append("    [[KO-SPIR-CF-CAUSE-UNCONFIRMED-1]]参照。]")` |
| `src/value/tanuki_valuation/pipeline.py:3444` | SPIR | `SEC EDGAR照合で、両者が食い違う3銘柄〈RCAT/SPIR/CPRT〉全てで` |
| `src/value/tanuki_valuation/segment_config.py:215` | ONDS | `tickers = ["NVDA", "TSLA", "PLTR", "MSFT", "AMZN", "AMD", "SOFI", "RKLB", "APP", "CELH", "SOUN", "ONDS"]` |

## 2. コメントの行（ファイルごとの件数。説明の例であって動作に効かない）

| ファイル | 行数 |
|---|---|
| `common/sec_data/parser.py` | 35 |
| `common/sec_data/report_consistency_check.py` | 16 |
| `src/value/tanuki_valuation/pipeline.py` | 13 |
| `src/value/adjusted_eps_analyzer/extract_key_facts.py` | 8 |
| `common/sec_data/layer3_builder.py` | 7 |
| `common/sec_data/quarterly.py` | 6 |
| `common/sec_data/tag_definitions.py` | 3 |
| `src/value/stonks_silo/analyzer.py` | 3 |
| `common/sec_data/cf_lines.py` | 2 |
| `common/sec_data/reader.py` | 2 |
| `common/sec_data/ttm_calculator.py` | 2 |
| `src/value/tanuki_valuation/calculator/adjustments.py` | 2 |
| `common/profitability.py` | 1 |
| `common/sec_data/bs_lines.py` | 1 |
| `common/sec_data/conditional_tags.py` | 1 |
| `common/sec_data/q4_implied.py` | 1 |
| `common/sec_data/registration_validator.py` | 1 |
| `common/sec_data/rotce.py` | 1 |
| `common/sec_data/split_adjust.py` | 1 |
| `docs/value-monitor/adjusted_eps_analyzer/index.html` | 1 |
| `docs/value-monitor/stonks-silo/index.html` | 1 |
| `docs/value-monitor/tanuki_valuation/stock.html` | 1 |
| `src/value/stonks_silo/pipeline.py` | 1 |
| `src/value/tanuki_valuation/calculator/rice.py` | 1 |
| `src/value/tanuki_valuation/core_calculator.py` | 1 |
| `src/value/tanuki_valuation/data_fetcher.py` | 1 |
| `src/value/tanuki_valuation/growth_sanity.py` | 1 |

## 3. 銘柄ごとのコードの行数

BBAI 12・XOM 11・SPIR 9・ONDS 9・RDW 8・KULR 7・FCX 7・PM 4・MO 3・VZ 2・RXRX 1・SCCO 1
