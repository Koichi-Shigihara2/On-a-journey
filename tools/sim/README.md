# tools/sim — 試算の共通の道具

計算方法の変更の前後を、全銘柄で、公開のデータに触れずに比べる。使い方は `sim.py` の先頭の説明を参照。

標準の流れ（`CLAUDE_CODE_PROCEDURES.md`「計算方法の変更の手順」の1）:

```
python tools/sim/sim.py chain before --root <出力先>      # 前のコード（git stash で戻す）で TANUKI → HypeCore → TANUKI
python tools/sim/sim.py chain after  --root <出力先>      # 新しいコード
python tools/sim/sim.py compare before after --root <出力先>   # 変化の表（変化の大きい順・判定の変化は全件・保有銘柄は別）
python tools/sim/sim.py scope before after --id <ID> --root <出力先> --write   # config/method_change.json の scope
```

- Grok・SEC・Discord は呼ばない。出力は `--root`（既定 `.sim_out/`、gitignore）の下だけ
- 一時的な差し替え（確かめだけ）は `--hook FILE.py` の `apply(ctx)`（ctx は GS・MC・P・HC のモジュール）
- テスト: `tests/test_sim_tools.py`（作った値だけ。本物のパイプラインは回さない）
