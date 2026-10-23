"""出力の一覧（書き手・読み手・画面・定義・規則）を AST と grep で機械的に作る（[[OUTPUT-INVENTORY-CHECK-1]] 段階1、2026-10-20）。

対象（段階1）: common/sec_data/ の .py が書く出力（ファイル・Step Summary・Discord）と、ファイルの最上位のキー。
一覧は AI が書き起こさない（CHAT_RULES.md「発見の溜め方と根治の判断」5）。人・AIが足すのは下の ANNOTATIONS
（ふつうの言葉の1文）だけで、機械の一覧とは欄を分ける。

作り方:
  1. 書き手: common/sec_data/*.py の AST から書き込み（open(..., "w"/"a")・write_text・to_csv・os.replace の先・
     requests.post/urlopen）を探し、パスの式（os.path.join・f文字列・モジュールの定数・self の属性・
     返り値が1つの関数・import した定数）を解決してパスの型（{T}=銘柄 等）にする。
     common/sec_data/ の外の .py が common/sec_data/ に書く場所も同じ方法で探す（止める条件の数）
  2. 読み手（保存済みファイル）:
     - Python: AST の読み込み（open(...)〈"r"・省略〉・json.load・read_text・pd.read_json/read_csv）のパスを解決して照合
     - 全種類（.py・.js・.html・.yml）: パスの型のファイル名の部分から作った正規表現の grep（コメント・docstring を除く）
     - 経由: 上で読み手になった Python の関数を import して呼ぶ箇所（1段まで）
  3. 読み手（その場で計算）: 書き手の関数に渡す値を作る関数（`x = f(...)` → `save(…, x)` の f）を import して呼ぶ箇所
  4. 画面: docs/ の .html・.js が直接読むもの（直接）と、Python の読み手（とそれを import する本番のファイル、2段まで）が
     docs/ に書くファイルを読む docs/ のページ（経由、推定）
  5. 定義: docs/architecture/new_data_platform/FIELD_DEFINITIONS.md の表の行のうち、ファイル名の型を含む行の ID
  6. 規則: RULES の検出式（定義の書き方の型。式は人が決め、場所は grep で出す）に当たる場所。2ファイル以上なら二重
  7. 本番外のコード: NON_PRODUCTION_DIRS の .py の行数と、ワークフロー・ほかのコード・テストからの参照

使い方:
  python scripts/inventory/build_output_inventory.py                 # SEC層: docs/architecture/output_inventory/sec_layer.json と sec_layer.md を書く
  python scripts/inventory/build_output_inventory.py --layer tanuki  # TANUKI: tanuki_layer.json と tanuki_layer.md を書く
  python scripts/inventory/build_output_inventory.py --layer market_data  # 市場データ層: market_data_layer.json と market_data_layer.md を書く
  python scripts/inventory/build_output_inventory.py --layer tail  # TANUKI TAIL: tail_layer.json と tail_layer.md を書く
  python scripts/inventory/build_output_inventory.py --layer portfolio  # Portfolio: portfolio_layer.json と portfolio_layer.md を書く
  python scripts/inventory/build_output_inventory.py --check   # 書かずに、今のファイルと同じかだけを見る（違えば終了コード1）

ファイルの分け方（R-A の終わりの見直し3、2026-10-23）: 段階の設定は inventory_layers.py、解析（AST・grep・パスの解決）と
一覧の組み立て・CHECK-INV は inventory_analysis.py、md の書き出しは inventory_render.py。このファイルは入口（コマンドと、
report_consistency_check.py が呼ぶ run_check）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import inventory_analysis as A
from inventory_analysis import *  # noqa: F401,F403  これまでどおり build_output_inventory.run_check() 等で呼べるように
from inventory_render import md_escape, render_md  # noqa: F401


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="書かずに、今のファイルと同じかだけを見る")
    ap.add_argument("--layer", choices=sorted(A.LAYERS), default="sec", help="段階（sec: SECデータ層、tanuki: TANUKI VALUATION・TANUKI Score、hypecore: HypeCore、eps: adjusted_eps_analyzer、stonks: STONKS SILO、market_data: 市場データ層、tail: TANUKI TAIL、portfolio: Portfolio、macro_data: マクロデータ層、macro_pulse: MACRO PULSE、market_pulse: Market Pulse）")
    args = ap.parse_args()
    A.set_layer(args.layer)
    inv = A.build()
    A.save_key_cache()
    js = json.dumps(inv, ensure_ascii=False, indent=1) + "\n"
    md = render_md(inv)
    if args.check:
        same = A.read_text(A.OUT_JSON) == js and A.read_text(A.OUT_MD) == md
        print("一覧は最新" if same else "一覧が古い（スクリプトを実行して作り直す）")
        return 0 if same else 1
    os.makedirs(os.path.join(A.REPO, A.OUT_DIR), exist_ok=True)
    with open(os.path.join(A.REPO, A.OUT_JSON), "w", encoding="utf-8", newline="\n") as f:
        f.write(js)
    with open(os.path.join(A.REPO, A.OUT_MD), "w", encoding="utf-8", newline="\n") as f:
        f.write(md)
    st = inv["stop_condition"]
    print(f"出力 {len(inv['outputs'])}、止める条件 {'当たる' if st['hit'] else '当たらない'}（書き手 {st['ratio_writer']:.1%}・読み方 {st['ratio_reader']:.1%}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
