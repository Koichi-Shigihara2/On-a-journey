# INV-6b: 読み手も注記も無いキーの仕分け案（2026-11-28、直していない）

`WARN-INV-6b`（2026-11-28 新設。読み手が無く、注記〈これから作る：項目名／残す理由〉も無いキーは、一覧に載っていても数える）が今引っかけるキー。
**基準**: A 予定された読み手がある → 「これから作る：項目名」と注記 ／ B 検証・監査のために残す意味がある → 残す理由を注記 ／ C どちらでもない → 書き出しごと消す ／ D 判断が要る（読み手の予定の有無を確かめてから A か C）。
**限界**: 案は、キー名・書き手のコード・文書の言及（BACKLOG・`docs/architecture/*.md`）から出した。個々のキーの読み手が本当に無いか（動的な読み）は、C の案を実行する前に1つずつ確かめる。


**合計 88 件**（層別: SEC 58・TANUKI 9・STONKS 6・HypeCore 1・市場データ 4・Market Pulse 7・common直下 3）。案の内訳: A 0・B 65・C 14・D 9。
## SEC（58件）

| キー | 書き出している場所 | 案 | 理由 |
|---|---|---|---|
| `accn_aligned` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `accn_aligned_anchor` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `aligned_to_sibling_accn` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `annual_fill_max_days` | docs/common/sec_data/rotce/_summary.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `bs_tag_sources` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `computed_year` | common/sec_data/data/{T}/fy_tag_mismatch_log.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `cross_check` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `diff_base` | common/sec_data/data/{T}/bs_identity_violations_log.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `diff_base_pct` | common/sec_data/data/{T}/bs_identity_violations_log.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `diff_extended` | common/sec_data/data/{T}/bs_identity_violations_log.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `dimension_aggregate_applied` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `eps_basic` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | D 要判断 | Layer3 の項目。`PARSER_LAYER3_*_STEP0.md` に出てくるので、段階〈段2b・段3 年次〉で読み手を作るか決める。決まるまで『これから作る：[[PARSER-LAYER3-SINGLE-SOURCE-1]] の段階』と注記する案 |
| `extra_components` | common/sec_data/data/{T}/bs_identity_violations_log.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `finance_lease_payments` | common/sec_data/ttm/{T}_ttm_series.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `financing_cash_flow` | common/sec_data/ttm/{T}_ttm_series.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `first_quarter` | docs/common/sec_data/rotce/{T}.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `gap_amount` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `goodwill` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | D 要判断 | Layer3 の項目。`PARSER_LAYER3_*_STEP0.md` に出てくるので、段階〈段2b・段3 年次〉で読み手を作るか決める。決まるまで『これから作る：[[PARSER-LAYER3-SINGLE-SOURCE-1]] の段階』と注記する案 |
| `gp_anchor_realigned` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `gp_anchor_realigned_tag` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `gp_estimate` | common/sec_data/data/{T}/annual_{YYYY}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `gp_rejected_reason` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `insurance_reserves` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | D 要判断 | Layer3 の項目。`PARSER_LAYER3_*_STEP0.md` に出てくるので、段階〈段2b・段3 年次〉で読み手を作るか決める。決まるまで『これから作る：[[PARSER-LAYER3-SINGLE-SOURCE-1]] の段階』と注記する案 |
| `interest_expense` | common/sec_data/data/{T}/annual_{YYYY}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `interest_income` | common/sec_data/data/{T}/annual_{YYYY}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `investing_cash_flow` | common/sec_data/ttm/{T}_ttm_series.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `lab` | common/sec_data/data/{T}/cf_lines/{accn}.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `last_quarter` | docs/common/sec_data/rotce/{T}.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `legacy_ciks_merged` | common/sec_data/data/{T}/submissions.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `method_change_applied` | docs/common/sec_data/rotce/_summary.json、docs/common/sec_data/rotce/{T}.json | B 残す | 計算方法の変更の『前回読んだ ID』の記録。次の実行が前回と比べて印を付けるために自分で読む（`common/method_change.py`。動的な読みで一覧が拾えない） |
| `min_quarters` | docs/common/sec_data/rotce/_summary.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `n_periods` | common/sec_data/ttm/{T}_ttm_series.json | D 要判断 | Layer3 の項目。`PARSER_LAYER3_*_STEP0.md` に出てくるので、段階〈段2b・段3 年次〉で読み手を作るか決める。決まるまで『これから作る：[[PARSER-LAYER3-SINGLE-SOURCE-1]] の段階』と注記する案 |
| `nonop_items` | common/sec_data/data/{T}/annual_{YYYY}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `nonop_net` | common/sec_data/data/{T}/annual_{YYYY}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `other_nonop_income` | common/sec_data/data/{T}/annual_{YYYY}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `other_side` | common/sec_data/data/{T}/fye_boundary_collision_log.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `override_applied` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/fye_boundary_collision_log.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `override_reason` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `own_data_side` | common/sec_data/data/{T}/fye_boundary_collision_log.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `pretax_raw` | common/sec_data/data/{T}/annual_{YYYY}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `ptbv_pctl_max` | docs/common/sec_data/rotce/_summary.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `ptbv_pctl_min` | docs/common/sec_data/rotce/_summary.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `reference_only` | docs/common/sec_data/rotce/_summary.json、docs/common/sec_data/rotce/{T}.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `restricted_cash` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | D 要判断 | Layer3 の項目。`PARSER_LAYER3_*_STEP0.md` に出てくるので、段階〈段2b・段3 年次〉で読み手を作るか決める。決まるまで『これから作る：[[PARSER-LAYER3-SINGLE-SOURCE-1]] の段階』と注記する案 |
| `restructuring_charges_deducted` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `revenue_tag_realigned` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `revenue_tag_realigned_to` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `rich` | docs/common/sec_data/rotce/_summary.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `role` | common/sec_data/data/{T}/bs_lines/{accn}.json、common/sec_data/data/{T}/cf_lines/{accn}.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `rotce_pctl_max` | docs/common/sec_data/rotce/_summary.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `rotce_pctl_min` | docs/common/sec_data/rotce/_summary.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `rotce_series` | docs/common/sec_data/rotce/{T}.json | B 残す | ROTCE の P/TBV のパーセンタイルの根拠・基準の記録（`percentile` の `signal` を読む側は読み、根拠のキーは検証用） |
| `sga_captured` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `sga_total` | common/sec_data/data/{T}/annual_{YYYY}.json、common/sec_data/data/{T}/quarterly_{Q}.json | C 消す候補 | 読み手の予定が文書・BACKLOG・コードに無い。取り込みはしているが使っていない項目。書き出しごと消す案（PARSER-LAYER3 の段3〈年次〉で一緒に） |
| `source_files` | common/sec_data/data/{T}/bs_lines/{accn}.json、common/sec_data/data/{T}/cf_lines/{accn}.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |
| `source_is_own_data` | common/sec_data/data/{T}/annual_{YYYY}.json | B 残す | 年次・四半期の取り込みの補正・突き合わせの印（どの補正を当てたかの根拠。`*_log.json` と合わせて人が確かめる） |
| `tce_first_val` | docs/common/sec_data/rotce/{T}.json | B 残す | ROTCE の P/TBV の履歴の分母に使った『その期の最初の申告日までの TCE』（MULTIPLES-ADOPT-1 段4）。`ptbv`（読まれる値）の根拠で、再申告で `tce`（最新の値）と食い違う行が 1,226 行のうち 69 行ある。SOFI の ROTCE・P/TBV の安い・高いの判定（`percentile`）の時点のずれを後から確かめる監査の記録 |
| `verification_command` | common/sec_data/dimension_aggregate_registry.json | B 残す | 取り込みの監査ログ・出所の記録（`*_log.json`・`bs_lines`・`cf_lines`・`submissions`）。人が確かめる |

## TANUKI（9件）

| キー | 書き出している場所 | 案 | 理由 |
|---|---|---|---|
| `age_days` | docs/value-monitor/tanuki_valuation/data/{T}/latest.json | B 残す | 手書きの端末の成長率（handset_growth）の鮮度の日数。`status`（stale 判定）の根拠の値 |
| `announced_period_end_est` | docs/value-monitor/tanuki_valuation/data/{T}/latest.json | B 残す | 決算は反映前の印（earnings_pending）の、発表された期の期末の推定。`status` の根拠 |
| `calculation_steps` | docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json | B 残す | 履歴ファイルだけにある計算過程の記録（latest からは外している）。過去の計算を人が確かめる |
| `dilution_basis` | docs/value-monitor/tanuki_valuation/data/{T}/latest.json | B 残す | 希薄化の率をどの期間〈3年／上場来〉で取ったかの根拠。後から基準が変わっても、その行が何を見たかを残す |
| `earnings_source` | docs/value-monitor/tanuki_valuation/data/{T}/latest.json | B 残す | 決算日の出所（yfinance／カレンダー）。`status` の根拠 |
| `estimates_fetched_at` | docs/value-monitor/tanuki_valuation/data/{T}/latest.json | B 残す | 成長率と予想の差（guidance_gap）の予想を取った時刻。鮮度の確認用 |
| `fiscal_year_end` | docs/value-monitor/tanuki_valuation/data/{T}/history/{filename_safe}.json | B 残す | 履歴ファイルの会計年度末。過去の行の前提を残す |
| `method_change_applied` | docs/value-monitor/tanuki_valuation/data/hypecore_history/{T}.json、docs/value-monitor/tanuki_valuation/data/{T}/history.json | B 残す | 計算方法の変更の『前回読んだ ID』の記録。次の実行が前回と比べて印を付けるために自分で読む（`common/method_change.py`。動的な読みで一覧が拾えない） |
| `rec_evidence_weight` | docs/value-monitor/tanuki_valuation/data/{T}/latest.json | B 残す | 成長率の健全性（growth_sanity）の根拠の重み。判定の根拠を後から確かめる |

## STONKS（6件）

| キー | 書き出している場所 | 案 | 理由 |
|---|---|---|---|
| `core_profit` | docs/value-monitor/stonks-silo/data/results.json | D 要判断 | STONKS の黒字化パスの旧項目。画面・要約文が読んでいるか未確認。読み手が無ければ消す候補（C） |
| `fair_value_change` | docs/value-monitor/stonks-silo/data/results.json | B 残す | 黒字化の予測から除いた分の種類別の内訳（`by_kind` のキー名）。画面・report.txt は `exclusions`（文）を読む。内訳は検証用の記録 |
| `impairment` | docs/value-monitor/stonks-silo/data/results.json | B 残す | 同上（減損） |
| `method_change_applied` | docs/value-monitor/stonks-silo/data/results.json | B 残す | 計算方法の変更の『前回読んだ ID』の記録。次の実行が前回と比べて印を付けるために自分で読む（`common/method_change.py`。動的な読みで一覧が拾えない） |
| `ocf_acceleration` | docs/value-monitor/stonks-silo/data/results.json | D 要判断 | 同上（OCFトレンドの加速度）。画面の『OCFトレンド（金額）』は別のキーを読む。読み手が無ければ消す候補（C） |
| `ocf_yoy_change` | docs/value-monitor/stonks-silo/data/results.json | D 要判断 | 同上（OCFの前年比）。同上 |

## HypeCore（1件）

| キー | 書き出している場所 | 案 | 理由 |
|---|---|---|---|
| `method_change_applied` | docs/value-monitor/hypecore/data/{T}_poc.json | B 残す | 計算方法の変更の『前回読んだ ID』の記録。次の実行が前回と比べて印を付けるために自分で読む（`common/method_change.py`。動的な読みで一覧が拾えない） |

## 市場データ（4件）

| キー | 書き出している場所 | 案 | 理由 |
|---|---|---|---|
| `_contract_roll` | common/market_data/daily/{T}.json | B 残す | 先物の限月の切り替えの印（足の訂正の台帳）。読み手は fetcher 自身の上書きの規則 |
| `prev` | common/market_data/daily/{T}.json | D 要判断 | 日足の訂正の台帳の『直前の値』。台帳の読み手は人（検証）。注記してよいか書き手のコードで確認 |
| `price_to_sales` | common/market_data/attributes/{T}.json、common/market_data/valuation_history/{T}.json | B 残す | 予想PERなどと同じく、月ごとに溜める評価倍率の履歴（valuation_history）。後から作り直せない |
| `reason_label` | common/market_data/daily/{T}.json | B 残す | 足の訂正の理由の表示名（人が読む台帳） |

## Market Pulse（7件）

| キー | 書き出している場所 | 案 | 理由 |
|---|---|---|---|
| `first_seen` | docs/market-monitor/market-pulse/data/headlines/{date_et}.json | B 残す | 見出しを最初に取った時刻（見出しの積み足しの記録） |
| `fred_daily` | docs/market-monitor/market-pulse/data/latest.json、docs/market-monitor/market-pulse/data/market_data.json | C 消す候補 | 廃止した `data_quality.expected_by_kind`（11-28）の下位のキー。書き手は無く、過去のエントリに残るだけ。注記ではなく『廃止、過去のエントリに残るだけ』と書いて数えない扱いにする |
| `interval` | docs/market-monitor/market-pulse/data/intraday/{date_et}.json | B 残す | 5分足の保存の間隔の記録（intraday の形の説明） |
| `jpx` | docs/market-monitor/market-pulse/data/latest.json、docs/market-monitor/market-pulse/data/market_data.json | C 消す候補 | 同上 |
| `new_by_date` | docs/market-monitor/market-pulse/data/headlines/{date_et}.json | B 残す | その回に新しく入った見出しの件数の日別の記録（積み足しの検証用） |
| `seen_at` | docs/market-monitor/market-pulse/data/headlines/{date_et}.json | B 残す | 見出しの題が変わった時刻の記録（積み足しの検証用） |
| `us_close` | docs/market-monitor/market-pulse/data/latest.json、docs/market-monitor/market-pulse/data/market_data.json | C 消す候補 | 同上 |

## common直下（3件）

| キー | 書き出している場所 | 案 | 理由 |
|---|---|---|---|
| `announced_period_end_est` | earnings_pending | B 残す | 決算は反映前の印（earnings_pending）の、発表された期の期末の推定。`status` の根拠 |
| `fair_value_change` | docs/common/profitability.json | B 残す | 黒字化の予測から除いた分の種類別の内訳（`by_kind` のキー名）。画面・report.txt は `exclusions`（文）を読む。内訳は検証用の記録 |
| `impairment` | docs/common/profitability.json | B 残す | 同上（減損） |

