# TANUKI VALUATION — ベータ（β）設定方針

> **配置先候補：** `src/value/tanuki_valuation/BETA_POLICY.md` または `docs/value-monitor/tanuki_valuation/BETA_POLICY.md`

---

## 基本思想：ボトムアップβとは

TANUKI VALUATIONで使用するβは、**ダモドラン（Damodaran）式ボトムアップβ**を採用している。

yfinanceが提供する個別株βは市場の投機・需給ノイズに歪みやすく、短期的な株価変動に引きずられる。これは「市場価格から独立した本源的価値の評価」というKoichi式DCFの目的と矛盾する。

そこで、**Damodaran 2025（NYU Stern）が公表する業種別unlevered β中央値をベースに、Koichiが企業特性を考慮して調整した値を `beta_config.json` に明示的に記載する**というアプローチを採用している。

> 「自動計算の仕組み」よりも「Koichiの判断を明示的にコードに刻む」ほうが、投資ツールとして誠実である。自動計算は客観的に見えるが、ピア選定という主観が隠れるだけである。

---

## βの優先順位（data_fetcher.py）

```
1. beta_config.json の overrides   ← Koichiが設定した意図的なβ（最優先）
2. yfinance β                       ← フォールバック（有効範囲 0.1〜3.0 のみ）
3. セクター別デフォルトβ            ← yfinanceも取れなかった場合
4. 全体デフォルト（1.0）
```

`beta_config.json` の `overrides` に記載があれば、yfinanceの値は使用されない。

---

## beta_config.json の構造

```json
{
  "_comment": "Damodaran 2025参照値ベース。未設定はyfinanceβにフォールバック",

  "_damodaran_ref_2025": {
    "Semiconductor":         0.93,
    "Software_Internet":     1.03,
    "AdTech_Internet":       1.15,
    "EV_Automotive":         1.25,
    "Fintech":               1.20,
    "Consumer_Beverage":     0.55,
    "Space_Defense":         0.85,
    "Cloud_Services":        1.10,
    "Healthcare_Support":    0.86
  },

  "overrides": {
    "NVDA": { "beta": 1.05, "industry": "Semiconductor",     "reason": "Semiconductor業種β基準、CUDA独占性考慮でやや低め" },
    "AMD":  { "beta": 1.10, "industry": "Semiconductor",     "reason": "Semiconductor業種β" },
    "PLTR": { "beta": 1.08, "industry": "Software_Internet", "reason": "Software業種β、政府契約で収益安定" },
    "MSFT": { "beta": 0.90, "industry": "Cloud_Services",    "reason": "Tech Services業種β、超安定" },
    "APP":  { "beta": 1.18, "industry": "AdTech_Internet",   "reason": "AdTech業種β、AXON成長フェーズ考慮" },
    "TSLA": { "beta": 1.25, "industry": "EV_Automotive",     "reason": "EV業種β、事業多角化途上" },
    "AMZN": { "beta": 1.10, "industry": "Cloud_Services",    "reason": "E-Commerce+Cloud複合、業種混合で判断" },
    "CELH": { "beta": 0.60, "industry": "Consumer_Beverage", "reason": "Consumer Staples業種β" },
    "SOFI": { "beta": 1.20, "industry": "Fintech",           "reason": "Fintech業種β" },
    "SOUN": { "beta": 1.08, "industry": "Software_Internet", "reason": "Software業種β、小型株プレミアム考慮" },
    "RKLB": { "beta": 0.85, "industry": "Space_Defense",     "reason": "Space業種β、RPOで収益安定" }
  }
}
```

---

## 業種別Damodaran参照値（2025年版）

| 業種キー | Unlevered β | 代表銘柄 |
|----------|------------|---------|
| Semiconductor | 0.93 | NVDA, AMD |
| Software_Internet | 1.03 | PLTR, SOUN |
| AdTech_Internet | 1.15 | APP |
| EV_Automotive | 1.25 | TSLA |
| Fintech | 1.20 | SOFI |
| Consumer_Beverage | 0.55 | CELH |
| Space_Defense | 0.85 | RKLB |
| Cloud_Services | 1.10 | MSFT, AMZN |
| Healthcare_Support | 0.86 | — |

データソース：[NYU Stern — Damodaran Online](https://pages.stern.nyu.edu/~adamodar/)（Beta by Sector、通常1〜2月更新）

---

## β値の調整ロジック

業種中央値をベースに、以下の観点でKoichiが調整する。調整理由は `reason` フィールドに明記する。

| 調整方向 | 根拠 |
|----------|------|
| **低め** | ボトルネック独占性が高い（CUDA等）、政府契約で収益安定、成熟した事業基盤 |
| **高め** | 小型株プレミアム、急成長フェーズ、事業多角化途上 |
| **複合業種** | 複数業種の加重平均的に判断（例：AMZN = EC + Cloud） |

---

## 新銘柄追加時の手順

1. [Damodaran最新版](https://pages.stern.nyu.edu/~adamodar/) で対象業種の **unlevered β中央値** を確認する
2. `_damodaran_ref_2025` にその業種が未登録なら追加する
3. `overrides` に銘柄別の最終β値・`industry`・`reason` を記載する
4. 編集はClaude Codeが直接する（管理のページは2026-10-21に削除。CLAUDE_CODE_START.md「管理の操作の手順」）

---

## 年次更新のタイミング

Damodaran教授は通常 **毎年1〜2月** にBeta by Sectorデータを更新する。  
年1回、`_damodaran_ref_2025` の値をサイトと照合し、必要に応じて更新する。  
キーの年号（例: `_damodaran_ref_2025` → `_damodaran_ref_2026`）も合わせて変更する。

---

## 不採用となった代替案

| 案 | 不採用理由 |
|----|-----------|
| yfinance個別株β | 投機・需給ノイズに歪む。短期市場連動性であり本源的価値評価と矛盾 |
| ピア中央値β（同業種yfinance銘柄の中央値） | 「yfinanceβの問題をyfinanceβの集合で解決しようとしている」循環論 |
| 業種ETFβ（SOXX等） | 構成銘柄・加重方式による差異があり透明性・再現性でDamodaran業種βに劣る |
| Blume平均回帰（×0.67+0.33） | 上記問題を緩和するが根本解決にならない |

---

## WACCへの組み込み方法

βは以下のように `core_calculator.py` でWACCに組み込まれる。

```
intrinsic_value_per_share  ← Rm（〜10%、β不使用）【主値、Koichi式の本命】
intrinsic_value_beta       ← β込みWACC（参考値、市場リスクプレミアム感度確認用）
intrinsic_value_rf         ← Rf（4.3%、理論的天井、参考値）
```

「β込みWACC」は市場価格との比較・感度分析用の参考値であり、Koichi式DCFの主値はβを使用しない `Rm` ベースである。詳細は `DISCOUNT_RATE_POLICY.md`（未作成）を参照。
