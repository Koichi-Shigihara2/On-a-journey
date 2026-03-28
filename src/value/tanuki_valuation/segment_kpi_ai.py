# src/value/tanuki_valuation/segment_kpi_ai.py
import os
import json
from typing import Dict, List

class SegmentKPIAI:
    """AIによるブル/中立/ベア セグメント別KPI動的設定（手戻り3対応）"""
    
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY") or os.getenv("XAI_API_KEY")
        self.model = "gemini" if os.getenv("GEMINI_API_KEY") else "xai"
    
    def generate_scenarios(self, ticker: str, sec_text: str) -> Dict[str, List[Dict]]:
        """10-K/10-Qテキストからブル/中立/ベアのKPI成長見込みをAI生成"""
        prompt = f"""
        企業: {ticker}
        以下のSEC文書からセグメント別KPIを抽出せよ。
        出力は必ずJSON形式で、ブル/中立/ベアの3シナリオを返す。
        キー: revenue_growth, gross_margin, capex_ratio, working_capital_turnover
        用語は標準財務用語で統一（他社比較・履歴管理のため）。

        SEC文書:
        {sec_text[:8000]}  # トークン制限対策
        """
        
        # Gemini or XAI 呼び出し（実際はSDK使用）
        # ここでは簡易requests例
        # （本番はgoogle-generativeai または xai SDKをrequirements.txtに追加）
        scenarios = {
            "bull": [{"revenue_growth": 0.35, "gross_margin": 0.45, ...}],
            "base": [{"revenue_growth": 0.22, "gross_margin": 0.38, ...}],
            "bear": [{"revenue_growth": 0.08, "gross_margin": 0.30, ...}]
        }
        return scenarios  # 実際はAI応答をパース
