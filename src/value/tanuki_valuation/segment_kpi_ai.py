# src/value/tanuki_valuation/segment_kpi_ai.py
import os
from typing import Dict, List

class SegmentKPIAI:
    """AIによるブル/中立/ベア セグメント別KPI動的設定"""
    
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY") or os.getenv("XAI_API_KEY")
    
    def generate_scenarios(self, ticker: str, sec_text: str) -> Dict[str, List[Dict]]:
        """現在はダミーデータを返し、後続Phaseで本AI呼び出しに置き換え予定"""
        # 将来的にGemini/XAIで本実装（promptはconfig/prompts.yamlに移動）
        return {
            "bull": [
                {"revenue_growth": 0.35, "gross_margin": 0.45, "capex_ratio": 0.12, "working_capital_turnover": 8.5}
            ],
            "base": [
                {"revenue_growth": 0.22, "gross_margin": 0.38, "capex_ratio": 0.15, "working_capital_turnover": 7.2}
            ],
            "bear": [
                {"revenue_growth": 0.08, "gross_margin": 0.30, "capex_ratio": 0.18, "working_capital_turnover": 5.8}
            ]
        }
