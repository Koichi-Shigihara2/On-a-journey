# src/subport/fg_level2/ について

**このディレクトリは2026-05-03の開発初期に作成された複製であり、現在は非稼働です。**

本番運用の正本はリポジトリ外の `C:\Users\shigi\AutoTrade\fg_level2\` にあります。
Windowsタスクスケジューラから同ディレクトリの `trader.py --entry` / `--monitor` が
日次実行されており、`signal.json` / `state.json` / `trade_log.jsonl` が実際に
更新されているのはそちらです（詳細は `SYSTEM_MAP.md`「AutoTrade/OpenD運用前提」
参照）。

## config.json について（2026-09-16 再確認）

登録時点（`[[STALE-SUBPORT-CLEANUP-1]]`）では「外部運用がこのディレクトリの
config.json を参照している可能性がある」ため削除を見送っていました。しかし
2026-09-16の再確認で、外部運用側 `trader.py` の実装（`CONFIG_PATH = SCRIPT_DIR /
"config.json"`）を直接確認したところ、外部運用は自分自身のディレクトリ
（`C:\Users\shigi\AutoTrade\fg_level2\config.json`）内のconfig.jsonのみを
読み込んでおり、**リポジトリ内のこのファイルは参照していない**ことが判明しました。
両ファイルの内容を実際に比較したところ、キー構成・値とも完全に別物（外部運用側は
`market_data_path`絶対パス参照を持つフラットなスキーマなど、このリポジトリ内版
とは別物）であることも確認済みです。

なお`docs/architecture/new_data_platform/archive/OUTPUT_ITEMS_INVENTORY.md`
（AS-IS-386）には「外部AutoTrade fg_level2がsrc/subport/fg_level2/config.jsonを
参照」という記載が残っていますが、これは開発初期時点の記述であり、現在の運用実態
とは異なっていると考えられます（`archive/`配下の過去スナップショットのため本
README側では訂正の記録のみ残し、当該ドキュメント自体は変更していません）。

## このディレクトリを削除しない理由

上記の通りconfig.json単体の外部参照リスクは実際には確認されませんでしたが、本
ディレクトリは削除せず現状維持とします（2026-09-16、`[[STALE-SUBPORT-CLEANUP-1]]`
対応）。理由:
- 開発初期の実装リファレンスとして参照価値が残っている可能性がある
- 削除はリポジトリ上取り消しが困難な操作であり、削除自体の要否判断は今回の
  棚卸しタスク（ドキュメント整理）のスコープを超えるため、Koichiさんの判断を
  別途仰ぐこととした

このディレクトリ内のファイルを参照・編集する際は、本番運用の正本ではない
（あくまで陳腐化した初期複製である）ことを踏まえること。
