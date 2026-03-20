# adjustment_detector.py への追加パッチ

## 修正箇所: SECTOR_ITEM_ID_TO_NAME マッピング

以下の2エントリを追加してください。

### 現在のマッピング（引き継ぎ書より）

```python
SECTOR_ITEM_ID_TO_NAME = {
    "sbc":                   "株式報酬費用",
    "amortization_intangibles": "無形資産償却費",
    "inventory_writeoff":    "在庫評価損・減損",
    "ma_integration":        "M&A統合費用",
    "iprd_amortization":     "IPR&D償却",
    "depreciation":          "減価償却費",
    "crypto_fair_value":     "暗号資産公正価値変動損益",
}
```

### 追加する2エントリ

```python
SECTOR_ITEM_ID_TO_NAME = {
    "sbc":                          "株式報酬費用",
    "amortization_intangibles":     "無形資産償却費",
    "inventory_writeoff":           "在庫評価損・減損",
    "ma_integration":               "M&A統合費用",
    "iprd_amortization":            "IPR&D償却",
    "depreciation":                 "減価償却費",
    "crypto_fair_value":            "暗号資産公正価値変動損益",
    # ★★★ 以下2つを追加（フィンテック/銀行セクター用） ★★★
    "loan_fair_value":              "ローン公正価値評価損益",
    "loan_loss_provision_abnormal": "貸倒引当金繰入（異常変動）",
}
```

## なぜこれが必要か

sectors.yaml の exclusions に item_id: loan_fair_value 等を定義しても、
adjustment_detector.py の SECTOR_ITEM_ID_TO_NAME にエントリがないと
除外ロジックの名前マッチングが失敗し、除外が機能しない。

## adjustment_items.json への追加も必要

loan_fair_value と loan_loss_provision_abnormal に対応する
XBRL タグを adjustment_items.json の categories に追加する必要があります。

追加例（categories 配列の末尾に追加）:

```json
{
  "item_id": "loan_fair_value",
  "item_name": "ローン公正価値評価損益",
  "reason_default": "市場金利変動による一時的評価替え",
  "xbrl_tags": [
    "us-gaap:FairValueOptionChangesInFairValueGainLoss1",
    "us-gaap:GainLossOnSaleOfLoansAndLeases",
    "us-gaap:GainLossOnSalesOfLoansNet"
  ]
},
{
  "item_id": "loan_loss_provision_abnormal",
  "item_name": "貸倒引当金繰入（異常変動）",
  "reason_default": "マクロ変動による一時的な貸倒引当金の増加",
  "xbrl_tags": [
    "us-gaap:ProvisionForLoanAndLeaseLosses",
    "us-gaap:ProvisionForLoanLeaseAndOtherLosses",
    "us-gaap:ProvisionForDoubtfulAccounts"
  ]
}
```
