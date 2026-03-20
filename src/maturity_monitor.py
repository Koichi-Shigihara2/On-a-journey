"""
maturity_monitor.py (修正版 - VER2完全連携)
SBCの成熟度監視（SBC寄与率 + SBC/売上高 + セクター別閾値）
"""

from typing import Dict, List, Any

class MaturityMonitor:
    """SBC成熟度監視クラス（VER2のmaturity_watch専用）"""
    
    def __init__(self, config: Dict):
        # デフォルト（SaaS用）
        # sbc_to_revenue: 銀行はNetInterestIncome+NoninterestIncomeの合算が"revenue"
        self.thresholds = {
            "default":       {"sbc_contribution": 0.40, "sbc_to_revenue": 0.25},
            "saas":          {"sbc_contribution": 0.60, "sbc_to_revenue": 0.30},  # SaaSは緩め
            "manufacturing": {"sbc_contribution": 0.20, "sbc_to_revenue": 0.08},  # 製造/EVは厳しめ
            "banking":       {"sbc_contribution": 0.30, "sbc_to_revenue": 0.05},  # 銀行：SBC/純収益5%超でアラート
            "fintech":       {"sbc_contribution": 0.50, "sbc_to_revenue": 0.15},  # FinTechは中間
        }
        self.config = config
    
    def monitor(self, quarterly_results: List[Dict], sector: str = "default",
                latest_override: Dict = None) -> Dict:
        if not quarterly_results:
            return {"alert": None, "sbc_contribution": 0, "sbc_to_revenue": 0}
        
        # latest_override が渡されればそちらを優先（pipeline.py側でmax()取得済み）
        latest = latest_override if latest_override is not None else quarterly_results[-1]
        sbc_amount = 0
        revenue = latest.get('revenue', 0) or 0
        diluted_shares = latest.get('diluted_shares', 0) or 0
        
        # SBC金額取得
        for adj in latest.get('adjustments', []):
            if adj.get('item_id') == 'sbc':
                sbc_amount = adj.get('net_amount', 0)
                break
        
        print(f"  [MaturityMonitor] filing_date={latest.get('filing_date')} "
              f"sbc={sbc_amount:,.0f} revenue={revenue:,.0f} diluted_shares={diluted_shares:,.0f}")
        
        gaap_eps = latest.get('gaap_eps', 0)
        adjusted_eps = latest.get('adjusted_eps', 0)
        
        # === 正しい計算 ===
        # 1. SBC per share
        sbc_per_share = sbc_amount / diluted_shares if diluted_shares > 0 else 0
        
        # 2. SBC寄与率（SBCが調整差分の何%を占めるか）
        eps_difference = adjusted_eps - gaap_eps
        sbc_contribution = (sbc_per_share / eps_difference) if eps_difference > 0 else 0
        
        # 3. SBC/売上高
        sbc_to_revenue = sbc_amount / revenue if revenue > 0 else 0
        
        # セクター別閾値
        thresh = self.thresholds.get(sector.lower(), self.thresholds["default"])
        
        alert = None
        if sbc_contribution > thresh["sbc_contribution"]:
            alert = f"SBC寄与率が{thresh['sbc_contribution']:.0%}超（現在 {sbc_contribution:.1%}）"
        elif sbc_to_revenue > thresh["sbc_to_revenue"]:
            alert = f"SBC/売上高が{thresh['sbc_to_revenue']:.0%}超（現在 {sbc_to_revenue:.1%}）"
        
        return {
            "alert": alert,
            "sbc_contribution": round(sbc_contribution, 3),
            "sbc_to_revenue": round(sbc_to_revenue, 3),
            "sbc_per_share": round(sbc_per_share, 4),
            "sector_threshold": thresh
        }
