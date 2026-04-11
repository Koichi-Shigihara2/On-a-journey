"""
TANUKI VALUATION - Core Calculator v5.3
Koichi式株価評価モデル（セグメント別成長率対応）

P_t = (V_0 + RPO調整) × (1 + α)
V_0 = 2段階DCF（高成長期3年 + ターミナル）
α = min(1.0, max(0, (ROE_10yr × retention_rate / WACC) × 0.7))

v5.3 新機能:
- セグメント別成長率による加重平均成長率計算
- growth_scenarios フィールドで基本/セグメント両方を出力
- シナリオ別（bull/base/bear）成長率計算

パラメータ:
- WACC: 8.5%（成長期待を含まない固定値）
- terminal_growth: 3%
- retention_rate: 60%
- high_growth_range: 15%〜50%
- FCF floor: revenue × 8%（FCFがマイナスの場合）
- α_cap: 1.0（100%上限）
- min_fcf_years: 3（最低FCFデータ年数）
- RPO割引率: 15%（バックログの現在価値化）
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

# セグメント設定のインポート
try:
    from segment_config import get_segment_growth, calculate_scenario_growth
    HAS_SEGMENT_CONFIG = True
except ImportError:
    HAS_SEGMENT_CONFIG = False
    def get_segment_growth(ticker: str) -> Optional[Dict]:
        return None
    def calculate_scenario_growth(ticker: str, scenario: str = "base") -> Dict:
        return {"rate": None, "scenario": scenario, "source": "not_available"}


class KoichiValuationCalculator:
    """Koichi式 v5.3 バリュエーション計算エンジン"""

    def __init__(self):
        # 固定パラメータ
        self.wacc = 0.085           # 割引率（成長期待を含まない）
        self.high_growth_years = 3   # 高成長期間（年）
        self.retention_rate = 0.60   # 内部留保率
        self.terminal_growth = 0.03  # 永続成長率
        
        # v5.2 追加パラメータ
        self.alpha_cap = 1.0         # α上限（100%）
        self.min_fcf_years = 3       # 最低FCFデータ年数
        self.rpo_discount_rate = 0.15  # RPO割引率

    def _calculate_cagr_growth(self, ticker: str, fcf_list_raw: List[float]) -> Dict[str, Any]:
        """FCF CAGRベースの成長率計算（従来方式）"""
        high_growth_rate = 0.25  # デフォルト
        cagr_calculation = {"method": "default", "result": high_growth_rate}

        if len(fcf_list_raw) >= 3:
            recent_fcfs = [f for f in fcf_list_raw[-5:] if f > 0]
            if len(recent_fcfs) >= 2:
                raw_cagr = (recent_fcfs[-1] / recent_fcfs[0]) ** (1 / (len(recent_fcfs) - 1)) - 1
                high_growth_rate = max(0.15, min(0.50, raw_cagr))
                cagr_calculation = {
                    "method": "cagr",
                    "start_value": recent_fcfs[0],
                    "end_value": recent_fcfs[-1],
                    "periods": len(recent_fcfs) - 1,
                    "raw_cagr": raw_cagr,
                    "clipped_result": high_growth_rate
                }

        return {
            "rate": high_growth_rate,
            "source": "fcf_cagr",
            "calculation": cagr_calculation
        }

    def _get_growth_scenarios(self, ticker: str, fcf_list_raw: List[float]) -> Dict[str, Any]:
        """
        成長率シナリオを取得
        
        Returns:
            {
                "primary": {...},      # 計算に使用する成長率
                "base_cagr": {...},    # FCF CAGRベース
                "segment": {...},      # セグメント加重平均（あれば）
                "scenarios": {         # シナリオ別
                    "bull": {...},
                    "base": {...},
                    "bear": {...}
                }
            }
        """
        # 1. FCF CAGRベース（常に計算）
        cagr_result = self._calculate_cagr_growth(ticker, fcf_list_raw)
        
        # 2. セグメント別成長率（設定があれば）
        segment_result = None
        if HAS_SEGMENT_CONFIG:
            segment_data = get_segment_growth(ticker)
            if segment_data and segment_data.get("enabled"):
                segment_result = {
                    "rate": segment_data["weighted_growth"],
                    "source": "segment_weighted",
                    "fiscal_year": segment_data.get("fiscal_year"),
                    "segments": segment_data.get("segments")
                }
        
        # 3. プライマリ成長率を決定
        # セグメント設定がある場合はそちらを優先
        if segment_result:
            primary = {
                "rate": segment_result["rate"],
                "source": "segment_weighted",
                "note": "Segment-based weighted average"
            }
            print(f"   [{ticker}] セグメント加重成長率: {segment_result['rate']:.1%}")
        else:
            primary = {
                "rate": cagr_result["rate"],
                "source": "fcf_cagr",
                "note": "FCF CAGR-based (no segment config)"
            }
            print(f"   [{ticker}] FCF CAGR成長率: {cagr_result['rate']:.1%}")
        
        # 4. シナリオ別成長率
        scenarios = {}
        base_rate = primary["rate"]
        for scenario, multiplier in [("bull", 1.2), ("base", 1.0), ("bear", 0.7)]:
            adjusted = max(0.0, min(0.50, base_rate * multiplier))
            scenarios[scenario] = {
                "rate": adjusted,
                "multiplier": multiplier
            }
        
        return {
            "primary": primary,
            "base_cagr": cagr_result,
            "segment": segment_result,
            "scenarios": scenarios
        }

    def calculate_pt(self, financials: Dict[str, Any]) -> Dict[str, Any]:
        """
        メイン計算関数
        
        Args:
            financials: {
                "fcf_5yr_avg": float,
                "diluted_shares": int,
                "roe_10yr_avg": float,
                "current_price": float,
                "fcf_list_raw": list,
                "latest_revenue": float,
                "eps_data": {"ticker": str},
                "rpo": float (optional) - 残存履行義務
            }
        
        Returns:
            完全なバリュエーション結果
        """
        # データ抽出
        fcf_avg = financials.get("fcf_5yr_avg", 0.0)
        diluted_shares = financials.get("diluted_shares", 0)
        roe_avg = financials.get("roe_10yr_avg", 0.0)
        latest_revenue = financials.get("latest_revenue", 0.0)
        fcf_list_raw = financials.get("fcf_list_raw", [])
        current_price = financials.get("current_price", 0.0)
        ticker = financials.get("eps_data", {}).get("ticker", "Unknown")
        rpo = financials.get("rpo", 0.0)  # 残存履行義務

        # ========================================
        # バリデーション
        # ========================================
        if diluted_shares <= 100_000:
            return {"error": "diluted_shares missing or invalid", "ticker": ticker}
        
        # データ不足ガード
        if len(fcf_list_raw) < self.min_fcf_years:
            print(f"   [{ticker}] ⚠️ FCFデータ不足 ({len(fcf_list_raw)}年 < {self.min_fcf_years}年)")
            return {
                "error": f"FCFデータ不足 ({len(fcf_list_raw)}年)",
                "ticker": ticker,
                "fcf_years_available": len(fcf_list_raw),
                "min_required": self.min_fcf_years
            }

        # ========================================
        # STEP 1: FCF 5年平均算出
        # ========================================
        fcf_calculation = {
            "input": fcf_list_raw,
            "sum": sum(fcf_list_raw) if fcf_list_raw else 0,
            "count": len(fcf_list_raw),
            "result": fcf_avg
        }

        # ========================================
        # STEP 2: 成長率シナリオ取得（v5.3 新機能）
        # ========================================
        growth_scenarios = self._get_growth_scenarios(ticker, fcf_list_raw)
        high_growth_rate = growth_scenarios["primary"]["rate"]

        # ========================================
        # FCF現実的補正（マイナスFCF対応）
        # ========================================
        original_fcf = fcf_avg
        fcf_floor_applied = 0.0

        if fcf_avg <= 0 and latest_revenue > 0:
            fcf_floor = latest_revenue * 0.08
            fcf_avg = max(fcf_avg, fcf_floor)
            fcf_floor_applied = fcf_avg - original_fcf
            print(f"   [{ticker}] FCFが{original_fcf:,.0f}のため補正 → ${fcf_avg:,.0f} (売上高×8%)")

        # ========================================
        # STEP 3: 2段階DCF計算
        # ========================================
        current_fcf = fcf_avg
        pv_high = 0.0
        high_growth_detail = []

        for t in range(self.high_growth_years):
            current_fcf *= (1 + high_growth_rate)
            discount_factor = (1 + self.wacc) ** (t + 1)
            pv_year = current_fcf / discount_factor
            pv_high += pv_year
            high_growth_detail.append({
                "year": t + 1,
                "fcf": current_fcf,
                "discount_factor": discount_factor,
                "pv": pv_year
            })

        # ターミナル価値計算
        terminal_fcf = current_fcf * (1 + self.terminal_growth)
        terminal_value = terminal_fcf / (self.wacc - self.terminal_growth)
        pv_terminal = terminal_value / (1 + self.wacc) ** self.high_growth_years

        # V_0（本質的価値ベース）
        v0 = pv_high + pv_terminal

        # ========================================
        # STEP 4: RPO補正（SaaS企業向け）
        # ========================================
        rpo_adjustment = 0.0
        rpo_calculation = {"applied": False}
        
        if rpo > 0:
            # RPOを割引現在価値化（平均1.5年で実現と仮定）
            rpo_pv = rpo / (1 + self.rpo_discount_rate) ** 1.5
            rpo_adjustment = rpo_pv
            rpo_calculation = {
                "applied": True,
                "rpo_raw": rpo,
                "discount_rate": self.rpo_discount_rate,
                "assumed_realization_years": 1.5,
                "rpo_pv": rpo_pv
            }
            print(f"   [{ticker}] RPO補正: ${rpo:,.0f} → PV ${rpo_pv:,.0f}")

        # V_0 + RPO調整
        v0_adjusted = v0 + rpo_adjustment

        # ========================================
        # STEP 5: α（成長期待プレミアム）算出
        # ========================================
        g_individual = max(0.0, roe_avg * self.retention_rate)
        alpha_raw = (g_individual / self.wacc) * 0.7
        alpha_uncapped = max(0.0, alpha_raw)
        alpha = min(self.alpha_cap, alpha_uncapped)

        alpha_calculation = {
            "roe_10yr_avg": roe_avg,
            "retention_rate": self.retention_rate,
            "g_individual": g_individual,
            "wacc": self.wacc,
            "alpha_raw": alpha_raw,
            "alpha_uncapped": alpha_uncapped,
            "alpha_cap": self.alpha_cap,
            "alpha_final": alpha,
            "was_capped": alpha_uncapped > self.alpha_cap
        }

        if alpha_uncapped > self.alpha_cap:
            print(f"   [{ticker}] ROE_10yr = {roe_avg:.1%} → α = {alpha_uncapped:.3f} → キャップ適用 → {alpha:.3f}")
        else:
            print(f"   [{ticker}] ROE_10yr = {roe_avg:.1%} → α = {alpha:.3f}")

        # ========================================
        # STEP 6: 本質的価値（P_t）算出
        # ========================================
        intrinsic_value_pt = v0_adjusted * (1 + alpha)
        intrinsic_value_per_share = intrinsic_value_pt / diluted_shares if diluted_shares > 0 else 0.0

        # ========================================
        # 1〜3年後価値予測
        # ========================================
        future_values = {}
        current_value = intrinsic_value_per_share

        for year in range(1, 4):
            if year <= self.high_growth_years:
                growth_rate = high_growth_rate
            else:
                growth_rate = self.terminal_growth
            
            future_value = current_value * (1 + growth_rate)
            future_values[f"{year}年後"] = round(future_value, 2)
            current_value = future_value

        print(f"   [{ticker}] 1〜3年後理論株価: {future_values}")

        # ========================================
        # シナリオ別理論株価（v5.3 新機能）
        # ========================================
        scenario_valuations = {}
        for scenario_name, scenario_data in growth_scenarios["scenarios"].items():
            scenario_rate = scenario_data["rate"]
            scenario_v0 = self._calculate_dcf(fcf_avg, scenario_rate)
            scenario_v0_adj = scenario_v0 + rpo_adjustment
            scenario_pt = scenario_v0_adj * (1 + alpha)
            scenario_per_share = scenario_pt / diluted_shares if diluted_shares > 0 else 0.0
            scenario_valuations[scenario_name] = {
                "growth_rate": scenario_rate,
                "intrinsic_value_per_share": round(scenario_per_share, 2)
            }

        # 乖離率計算
        upside_percent = ((intrinsic_value_per_share / current_price) - 1) * 100 if current_price > 0 else 0

        # ========================================
        # 結果返却
        # ========================================
        return {
            "intrinsic_value_pt": float(intrinsic_value_pt),
            "intrinsic_value_per_share": float(intrinsic_value_per_share),
            "v0": float(v0),
            "v0_adjusted": float(v0_adjusted),
            "alpha": float(alpha),
            "alpha_was_capped": alpha_uncapped > self.alpha_cap,
            "future_values": future_values,
            "upside_percent": round(upside_percent, 1),
            "calculation_date": datetime.now().strftime("%Y-%m-%d"),
            "formula": "Koichi式 v5.3（セグメント成長率＋αキャップ＋RPO補正）",
            
            # v5.3 新規: 成長率シナリオ
            "growth_scenarios": {
                "primary": growth_scenarios["primary"],
                "base_cagr": {
                    "rate": growth_scenarios["base_cagr"]["rate"],
                    "source": growth_scenarios["base_cagr"]["source"]
                },
                "segment": growth_scenarios["segment"],
                "scenario_valuations": scenario_valuations
            },
            
            # 計算コンポーネント
            "components": {
                "fcf_5yr_avg": financials.get("fcf_5yr_avg"),
                "fcf_list_raw": fcf_list_raw,
                "diluted_shares": diluted_shares,
                "roe_10yr_avg": roe_avg,
                "current_price": current_price,
                "latest_revenue": latest_revenue,
                "rpo": rpo,
                "eps_data": financials.get("eps_data"),
                "high_growth_rate_used": float(high_growth_rate),
                "pv_high": float(pv_high),
                "pv_terminal": float(pv_terminal),
                "roe_used": float(roe_avg),
                "fcf_floor_applied": float(fcf_floor_applied),
                "rpo_adjustment": float(rpo_adjustment),
                "rpo_pv": float(rpo_adjustment),
                "alpha_uncapped": float(alpha_uncapped)
            }
        }

    def _calculate_dcf(self, fcf_avg: float, growth_rate: float) -> float:
        """DCF計算ヘルパー（シナリオ別計算用）"""
        current_fcf = fcf_avg
        pv_high = 0.0

        for t in range(self.high_growth_years):
            current_fcf *= (1 + growth_rate)
            discount_factor = (1 + self.wacc) ** (t + 1)
            pv_high += current_fcf / discount_factor

        terminal_fcf = current_fcf * (1 + self.terminal_growth)
        terminal_value = terminal_fcf / (self.wacc - self.terminal_growth)
        pv_terminal = terminal_value / (1 + self.wacc) ** self.high_growth_years

        return pv_high + pv_terminal


if __name__ == "__main__":
    calculator = KoichiValuationCalculator()
    
    # テスト: NVDA（セグメント設定あり）
    test_nvda = {
        "fcf_5yr_avg": 39859800000,
        "diluted_shares": 24514000000,
        "roe_10yr_avg": 0.456,
        "current_price": 188.63,
        "fcf_list_raw": [9108000000, 5641000000, 27021000000, 60853000000, 96676000000],
        "latest_revenue": 215938000000,
        "rpo": 2300000000,
        "eps_data": {"ticker": "NVDA"}
    }
    
    result = calculator.calculate_pt(test_nvda)
    print(f"\n=== NVDA ===")
    print(f"理論株価: ${result.get('intrinsic_value_per_share', 0):.2f}")
    print(f"成長率ソース: {result.get('growth_scenarios', {}).get('primary', {}).get('source')}")
    print(f"シナリオ別:")
    for name, data in result.get('growth_scenarios', {}).get('scenario_valuations', {}).items():
        print(f"  {name}: ${data['intrinsic_value_per_share']:.2f} (growth: {data['growth_rate']:.1%})")
    
    # テスト: UNKNOWN（セグメント設定なし）
    test_unknown = {
        "fcf_5yr_avg": 1000000000,
        "diluted_shares": 100000000,
        "roe_10yr_avg": 0.15,
        "current_price": 50.0,
        "fcf_list_raw": [800000000, 900000000, 1000000000, 1100000000, 1200000000],
        "latest_revenue": 5000000000,
        "eps_data": {"ticker": "UNKNOWN"}
    }
    
    result2 = calculator.calculate_pt(test_unknown)
    print(f"\n=== UNKNOWN ===")
    print(f"理論株価: ${result2.get('intrinsic_value_per_share', 0):.2f}")
    print(f"成長率ソース: {result2.get('growth_scenarios', {}).get('primary', {}).get('source')}")
