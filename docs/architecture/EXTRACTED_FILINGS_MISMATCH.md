# EXTRACTED-FILINGS-MISMATCH-1 — `data/extracted_filings/` の取り違えの調べ（2026-11-20、読み取りのみ）

コード・データは変更していない。確かめ方は「各ファイルの表紙の CIK（先頭400行の10桁の数字）が、置き場所の銘柄の `config/cik_lookup.csv` の CIK と一致するか」。

## 1. 結果（26ファイル・13銘柄）

| 銘柄（置き場所） | ファイル | 中身の会社 | 中身の CIK | cik_lookup の CIK（今） | 判定 |
|---|---|---|---|---|---|
| **APP**（保有） | `APP_10-K_2026-02-26.txt`・`APP_10-Q_2025-11-06.txt` | Alta Equipment Group Inc.（名前空間`altg:`） | 0001759824 | 0001751008（AppLovin） | **取り違え** |
| **SOUN**（保有） | `SOUN_10-K_2026-02-26.txt`・`SOUN_10-Q_2025-11-05.txt` | Payoneer Global Inc.（`payo:`） | 0001845815 | 0001840856（SoundHound AI） | **取り違え** |
| **ONDS** | `ONDS_10-K_2026-03-31.txt`・`ONDS_10-Q_2025-11-04.txt` | Purple Innovation, Inc.（10-Qは Purple LLC の記述） | 表紙に10桁のCIK無し（会社名で判定。cik_lookup の当時の CIK 0001643953 に対応） | 0001646188（Ondas） | **取り違え** |
| FIG | 2ファイル | Figma, Inc. | 0001579878 | cik_lookup に FIG 無し（今は未収載） | 取り違えではない（書き手が FIG の誤った CIK 0001716310 を 0001579878 に手で直して取得していた） |
| AMD・AMZN・CELH・MSFT・NVDA・PLTR・RKLB・SOFI・TSLA | 各2ファイル（18） | 一致 | 一致 | 一致 | OK |

取り違え **6ファイル・3銘柄**（保有は APP・SOUN）。ファイルを作ったのは1回（2026-04-01、commit `8fb70d9cdf`）で、以後 `data/extracted_filings/` は一度も更新されていない（`git log` は1件のみ）。

## 2. 書き手と原因

- 書き手: `src/value/inflection_point_tracer/sec_extractor.py`（`8fb70d9cdf`で追加、2026-05-24 `fad930a35e` で inflection_point_tracer ごと削除済み）。`config/cik_lookup.csv` の `cik` 列を**そのまま**使い、`https://data.sec.gov/submissions/CIK{cik}.json` から最新の10-K・10-Qを取り、`{ticker}_{form}_{提出日}.txt` として保存する。**表紙（会社名・CIK）と銘柄の照合は無い**
- 原因: **cik_lookup.csv に当時入っていた CIK が3銘柄で別の会社のものだった**（書き手の検索の誤りではなく、入力の誤り）。

  | 銘柄 | 当時の誤った CIK | 入れた commit | 正しい CIK | 直した commit |
  |---|---|---|---|---|
  | APP | 0001759824（Alta Equipment） | `9b77e51c81`（2026-03-20 Add CIK for APP、人）・`cf191d5549`（03-23） | 0001751008 | `892ac2b662`（2026-04-18 bot） |
  | SOUN | 0001845815（Payoneer） | `046b0461f0`（2026-03-29 Update cik_lookup.csv） | 0001840856 | `81504d4608`（04-18 bot） |
  | ONDS | 0001643953（Purple Innovation） | 2026-03〜04の cik_lookup.csv | 0001646188 | `fbe9d4093d`（04-18 bot） |

- SOUN と Payoneer の近さ: 名前は無関係。CIK は 1840856 と 1845815（差 約5,000）で、どちらも2021年の SPAC 合併の会社（登録の順が近い）。手で CIK を探した時に近い番号を取った可能性があるが、**取り違えの経緯は commit からは断定できない**（`9b77e51c81` は人の commit、3/29の更新も人）。
- 2026-04-18 に bot（ワークフローの CIK 登録）が正しい CIK を入れて以後は、cik_lookup は正しい。しかし抽出ファイルはその前の4/1に作られ、**作り直されなかった**。
- 現在の cik_lookup.csv の全98銘柄について、`common/sec_data/data/{T}/company_facts.json` の `cik` と照合した結果、**不一致0**（SEC層の今のデータは取り違えていない）。

## 3. 読み手（どの出力に入ったか）

- **今の読み手は0件**。リポジトリ内（コード・ワークフロー・設定・画面・docs）で `extracted_filings` を参照するのは `FINDINGS.md` だけ。TAIL の SEC 項目・Grok・report.txt・画面は、このフォルダを読まない（TAIL は `sec_items_fetcher` 等が別に取得する）
- 履歴上の読み手: `src/value/inflection_point_tracer/gemini_analyzer.py` の1か所だけで、`../../../data/extracted_filings/PLTR/PLTR_10-K_2026-02-17.txt` を**PLTR 1ファイルに固定**して読んでいた（PLTR は一致するファイル）。2026-05-24 に削除済み
- 取り違えた CIK（0001845815・0001759824・0001643953）が履歴上に現れるのは `config/cik_lookup.csv` と取り違えたファイル、FINDINGS だけ（`git log -S`、`data/extracted_filings`・FINDINGS・filelist.txt を除く。0001643953 は検索が終わらず未確認）。画面・TAIL のデータ（`docs/portfolio/tail/data`）に別の会社の名前（Payoneer・Alta Equipment・Purple Innovation）は無い
- **結論: 取り違えたファイルはどの出力にも入っていない（保有銘柄の TAIL のレビューにも入っていない）。影響は、人またはAIがこのファイルを原本として読む場合だけ**（実際に2026-11-20のSOUN 2025Q4の確認で、このファイルが Payoneer の10-Kだったことに気づいた）。cik_lookup が誤っていた 3/20〜4/18 の間に cik_lookup を読む他の取得（SEC層・TAIL）が誤った会社を取っていたかは、当時のデータの履歴を追っていない（未確認）。今のデータは上のとおり一致

## 4. 直し方の案（実装は許可されていない）

1. **フォルダごと削除する**（推奨）。読み手0件・書き手は削除済み・作り直しの手順なし・原本は SEC から再取得できる。誤りの原本が残るのは害（調査中に実際に踏んだ）。削除の前に `docs/architecture` の出力一覧（`CHECK-INV`）の読み手0件の確認と、`filelist.txt` が無いことを確かめる
2. 残す場合: 取り違えた3銘柄6ファイルだけ、正しい CIK で取り直す。取得は `external_api`（`sec_get`）を通す。**書き手の守り**: 保存の前に、取得した書類の表紙の CIK（`submissions` の `cik` と書類の `dei:EntityCentralIndexKey`）が `cik_lookup` の CIK と一致しなければ保存しない（銘柄名・CIK は設定から読み、手で書かない）
3. 根治（型B・同じ規則の二重実装の予防）: 「`cik_lookup.csv` に CIK を入れる時に、SEC の `submissions` の `name`・`tickers` と照合する」検査を、登録の手順（`register_ticker.py`・CIK 登録のワークフロー）に入れる。今の CIK は全98銘柄で company_facts と一致しているため、検査は予防のみ

## 5. 決めてほしいこと

- 案1（削除）か案2（取り直し＋守り）か
- 案3（CIK 登録時の照合）を別件にするか
