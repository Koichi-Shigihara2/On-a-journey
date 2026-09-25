"""
SEC データリーダー
各ツール（Adjusted EPS Analyzer, TANUKI VALUATION等）からのアクセス用インターフェース
"""

import json
import os
from typing import Optional, Dict, Any, List, Tuple

from .config import TICKERS, get_ticker_info


# =========================================
# normalized quarterly データ用汎用アクセサ
# GATE2-PHASE3B-1①: 4ファイル（financial_trend_calculator.py・
# quarterly_review_generator.py・tail_dcf_bridge.py・hypecore.py）が
# 独自実装していた四半期抽出ロジックの共通化窓口
# =========================================

def get_quarterly_series(normalized: dict, field_name: str) -> list:
    """is_annual・is_ytd両方を除外した四半期エントリをend日昇順で返す"""
    entries = normalized.get("fields", {}).get(field_name, [])
    quarterly = [
        e for e in entries
        if not e.get("is_annual") and not e.get("is_ytd")
    ]
    return sorted(quarterly, key=lambda x: x["end"])


def get_latest_quarterly(normalized: dict, field_name: str) -> Optional[dict]:
    """get_quarterly_seriesの最新1件（末尾）を返す。空ならNone"""
    series = get_quarterly_series(normalized, field_name)
    return series[-1] if series else None


# =========================================
# Runway算出用cash の共通窓口
# [[BBAI-RDW-RUNWAY-VERIFICATION-1]]: STONKS SILO（analyzer.py::
# _analyze_runway()）とTANUKI VALUATION（pipeline.py::
# computed_runway_months）が各自でcashを組み立てていたのを本関数へ集約
# =========================================

def get_runway_cash(net_cash_data: Optional[dict]) -> Optional[float]:
    """Runway算出用のcash（cash_and_equivalents + short_term_investments）を返す。

    SECReader.get_net_cash()の返却値をそのまま受け取り、その四半期優先
    ロジック（BUG-NETDEBT-4: 直近quarterly_*.jsonのBSが取れればそちらを
    優先）で確定したcash・short_term_investmentsを合算する。年次決算後の
    増資（RDW: FY2025 cash $94.5M → 2026Q2 $557.0M）や、現金から短期投資
    への振替（BBAI: 2026Q2 cash $36.3M + 流動AFS $282.9M）を反映するため。

    非流動AFS（満期1年超の投資有価証券）は含めない。保守側（過小評価）
    に倒れるだけで、SAFE/DANGERの判定逆転は起きないため（BBAIは非流動
    AFS $90.6Mを除いても流動分のみでSAFE）。

    get_net_cash()がcashを取得できなかった場合（cash_missing=True）や
    引数がNoneの場合はNoneを返し、呼び出し元で「データ不足」として扱う。
    """
    if not net_cash_data or net_cash_data.get("cash_missing"):
        return None
    return float(net_cash_data.get("cash") or 0.0) + float(
        net_cash_data.get("short_term_investments") or 0.0
    )


class SECReader:
    """SECデータ読み取りインターフェース"""
    
    def __init__(self, data_dir: str = None):
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    # =========================================
    # 年次データ取得
    # =========================================
    
    def get_annual(self, ticker: str, year: int) -> Optional[Dict[str, Any]]:
        """
        年次データ取得
        
        Args:
            ticker: ティッカーシンボル
            year: 年度（例: 2024）
        
        Returns:
            dict: {
                "ticker": "TSLA",
                "period": 2024,
                "form": "10-K",
                "bs": {"total_assets": ..., "stockholders_equity": ..., ...},
                "pl": {"revenue": ..., "net_income": ..., "eps_diluted": ...},
                "cf": {"operating_cash_flow": ..., "capital_expenditure": ..., "free_cash_flow": ...},
                "shares": {"shares_diluted": ..., "shares_basic": ...}
            }
        """
        ticker = ticker.upper()
        path = os.path.join(self.data_dir, ticker, f"annual_{year}.json")
        return self._load_json(path)
    
    def get_annual_range(self, ticker: str, years: int = 5) -> List[Dict[str, Any]]:
        """
        直近N年分の年次データ取得
        
        Args:
            ticker: ティッカーシンボル
            years: 取得年数（デフォルト5年）
        
        Returns:
            list: 年次データのリスト（新しい順）
        """
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        
        if not os.path.exists(ticker_dir):
            return []
        
        # 利用可能な年次ファイルを検索
        results = []
        files = sorted(os.listdir(ticker_dir), reverse=True)
        
        for f in files:
            if f.startswith("annual_") and f.endswith(".json"):
                data = self._load_json(os.path.join(ticker_dir, f))
                if data:
                    results.append(data)
                    if len(results) >= years:
                        break
        
        return results
    
    # =========================================
    # 四半期データ取得
    # =========================================
    
    def get_quarterly(self, ticker: str, quarter: str) -> Optional[Dict[str, Any]]:
        """
        四半期データ取得
        
        Args:
            ticker: ティッカーシンボル
            quarter: 四半期（例: "2024Q1"）
        
        Returns:
            dict: 四半期財務データ
        """
        ticker = ticker.upper()
        path = os.path.join(self.data_dir, ticker, f"quarterly_{quarter}.json")
        return self._load_json(path)
    
    def get_quarterly_range(self, ticker: str, quarters: int = 8) -> List[Dict[str, Any]]:
        """
        直近N四半期分のデータ取得
        
        Args:
            ticker: ティッカーシンボル
            quarters: 取得四半期数（デフォルト8）
        
        Returns:
            list: 四半期データのリスト（新しい順）
        """
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        
        if not os.path.exists(ticker_dir):
            return []
        
        results = []
        files = sorted(os.listdir(ticker_dir), reverse=True)
        
        for f in files:
            if f.startswith("quarterly_") and f.endswith(".json"):
                data = self._load_json(os.path.join(ticker_dir, f))
                if data:
                    results.append(data)
                    if len(results) >= quarters:
                        break
        
        return results
    
    # =========================================
    # TANUKI VALUATION用ヘルパー
    # =========================================
    
    def get_fcf_5yr_avg(self, ticker: str) -> float:
        """FCF 5年平均を取得"""
        annual_data = self.get_annual_range(ticker, 5)
        
        fcf_list = []
        for data in annual_data:
            fcf = data.get("cf", {}).get("free_cash_flow")
            if fcf is not None:
                fcf_list.append(fcf)
        
        return sum(fcf_list) / len(fcf_list) if fcf_list else 0.0
    
    def get_roe_avg(self, ticker: str, years: int = 10) -> float:
        """後方互換ラッパー: get_roe_avg_detail() の value のみを返す"""
        avg, _, _ = self.get_roe_avg_detail(ticker, years)
        return avg

    def get_roe_avg_detail(self, ticker: str, years: int = 10):
        """
        ROE平均を取得（純利益÷株主資本）
        損失年度を含む全期間を使用し、|ROE|>80%はwinsorize。
        Returns: (roe_avg: float, years_used: int, outlier_adjusted: bool)
        """
        annual_data = self.get_annual_range(ticker, years)

        roe_list = []
        outlier_adjusted = False
        for data in annual_data:
            net_income = data.get("pl", {}).get("net_income")
            equity = data.get("bs", {}).get("stockholders_equity")

            # net_income が None の場合、eps_diluted × shares_diluted で代替推計する
            # (古いSEC XBRLファイルで net_income タグが欠落している銘柄への対処)
            if net_income is None:
                eps_d = data.get("pl", {}).get("eps_diluted")
                sh_d  = (data.get("shares", {}) or {}).get("shares_diluted")
                if eps_d is not None and sh_d:
                    net_income = eps_d * sh_d

            if net_income is not None and equity and equity > 0:
                roe = net_income / equity
                if abs(roe) > 0.80:
                    roe = 0.80 if roe > 0 else -0.80
                    outlier_adjusted = True
                roe_list.append(roe)

        avg = sum(roe_list) / len(roe_list) if roe_list else None
        return avg, len(roe_list), outlier_adjusted
    
    def get_diluted_shares(self, ticker: str) -> int:
        """
        直近の希薄化後株式数を取得
        100万株未満の場合は異常値とみなし、shares_basicを試行
        """
        annual_data = self.get_annual_range(ticker, 1)
        
        if annual_data:
            shares_data = annual_data[0].get("shares", {})
            shares_diluted = shares_data.get("shares_diluted", 0)
            shares_basic = shares_data.get("shares_basic", 0)
            
            # 希薄化後株式数が100万以上なら使用
            if shares_diluted and shares_diluted >= 1_000_000:
                return int(shares_diluted)
            
            # 100万未満の場合、basicsを試行
            if shares_basic and shares_basic >= 1_000_000:
                return int(shares_basic)
            
            # それでも小さい場合、大きい方を返す（異常値の可能性あり）
            return int(max(shares_diluted or 0, shares_basic or 0))
        
        return 0
    
    def get_latest_revenue(self, ticker: str) -> float:
        """直近の売上高を取得"""
        annual_data = self.get_annual_range(ticker, 1)
        
        if annual_data:
            revenue = annual_data[0].get("pl", {}).get("revenue")
            if revenue:
                return float(revenue)
        
        return 0.0
    
    def get_fcf_list_with_dates(self, ticker: str, years: int = 5) -> Tuple[List[float], Optional[List[int]]]:
        """
        FCFリストと対応する会計年度（period）を取得（新しい順）。

        [[GATE2-READER-FCFLIST-1]]対応: get_fcf_list()は値だけを抽出して
        年度情報を切り捨てていたため、FCFSeriesによる順序検証を呼び出し元で
        後付けできなかった。get_annual_range()が返す各年次データは
        トップレベルに"period"（会計年度、int）を保持しているので、値と
        対にしたまま返す。

        Returns:
            (fcf_list, periods) のタプル。periodsはfcf_listと同じ長さ・
            同じ並び順で対応する（periods[i]がfcf_list[i]の会計年度）。
            いずれかの年度データに"period"が欠けている場合は、対応関係を
            保証できないためperiods=Noneを返す（呼び出し元は日付なし
            経路として扱う）。
        """
        annual_data = self.get_annual_range(ticker, years)
        # annual_data は新しい順（get_annual_range の仕様）なのでそのまま使用
        fcf_list: List[float] = []
        periods: List[Any] = []
        for data in annual_data:
            fcf = data.get("cf", {}).get("free_cash_flow")
            if fcf is not None:
                fcf_list.append(fcf)
                periods.append(data.get("period"))

        if any(p is None for p in periods):
            return fcf_list, None
        return fcf_list, periods

    def get_fcf_list(self, ticker: str, years: int = 5) -> List[float]:
        """FCFリストを取得（新しい順、fcf_list[0]が直近）"""
        fcf_list, _periods = self.get_fcf_list_with_dates(ticker, years)
        return fcf_list
    
    def get_rpo(self, ticker: str) -> float:
        """直近のRPO（残存履行義務）を取得 - SaaS企業向け"""
        annual_data = self.get_annual_range(ticker, 1)

        if annual_data:
            rpo = annual_data[0].get("other", {}).get("rpo")
            if rpo:
                return float(rpo)

        return 0.0

    def get_rpo_series(self, ticker: str, quarters: int = 8) -> list[dict]:
        """
        normalizedデータからRPO時系列を返す（直近quarters件、昇順）
        戻り値: [{"period": "2025-03", "rpo": 242800000000}, ...]  古→新順
        normalizedがない場合は[]を返す
        rpo_series[-1] が最新、rpo_series[-5] が約4四半期前
        """
        ticker = ticker.upper()
        normalized_dir = os.path.join(os.path.dirname(__file__), "normalized")
        path = os.path.join(normalized_dir, f"{ticker}_quarterly_normalized.json")

        normalized = self._load_json(path)
        if not normalized:
            return []

        rpo_entries = normalized.get("fields", {}).get("RPO", [])
        q_entries = [e for e in rpo_entries if not e.get("is_annual")]
        # 昇順ソート後、直近quarters件を取得（[-1]が最新になる）
        q_entries = sorted(q_entries, key=lambda x: x["end"])[-quarters:]

        return [
            {"period": e["end"][:7], "rpo": e["val"]}
            for e in q_entries
        ]

    def get_rpo_context(self, ticker: str) -> dict:
        """
        RPO補正に必要なコンテキストをnormalizedから計算して返す

        戻り値:
            rev_yoy    : Revenue前年比成長率（TTMベース, 8四半期以上で計算）
            rev_ttm    : TTM Revenue
            op_margin  : TTM営業利益率（OperatingIncome/Revenue）
            rpo_series : RPO時系列（昇順、get_rpo_series()と同一）
        """
        ticker = ticker.upper()
        normalized_dir = os.path.join(os.path.dirname(__file__), "normalized")
        path = os.path.join(normalized_dir, f"{ticker}_quarterly_normalized.json")

        normalized = self._load_json(path)
        if not normalized:
            return {"rev_yoy": None, "rev_ttm": None, "op_margin": None, "rpo_series": []}

        # TTM Revenue（直近4四半期合計）
        # 不変条件（[[RPO-REVTTM-GATE-SKIP-1]]、BACKLOG_DONE.md参照）: rev_yoyは
        # このif len(rev_all)>=4ブロックの内側かつlen(rev_all)>=8を追加条件として
        # 計算されるため、rev_yoyが非Noneならrev_ttmも必ず非Noneになる（逆は
        # 成立しない）。adjust_rpo()の比率安全弁（adjustments.py）はrev_ttmの
        # Noneチェックのみでゲートしているが、この依存関係によりrev_yoy取得済み・
        # rev_ttm未取得という組み合わせは構造上発生し得ない。将来rev_yoy算出の
        # 四半期数閾値（現在8）を変更する場合、rev_ttmの閾値（現在4）を下回る
        # 値にしない・または両者の依存関係を崩さないよう注意すること。
        rev_all = get_quarterly_series(normalized, "Revenue")
        rev_ttm: Optional[float] = None
        rev_yoy: Optional[float] = None
        if len(rev_all) >= 4:
            rev_ttm = sum(e["val"] for e in rev_all[-4:])
            if len(rev_all) >= 8:
                rev_yago_ttm = sum(e["val"] for e in rev_all[-8:-4])
                if rev_yago_ttm > 0:
                    rev_yoy = (rev_ttm - rev_yago_ttm) / rev_yago_ttm

        # TTM OperatingIncome → op_margin
        oi_all = get_quarterly_series(normalized, "OperatingIncome")
        op_margin: Optional[float] = None
        if len(oi_all) >= 4 and rev_ttm and rev_ttm > 0:
            oi_ttm = sum(e["val"] for e in oi_all[-4:])
            op_margin = oi_ttm / rev_ttm

        return {
            "rev_yoy":    rev_yoy,
            "rev_ttm":    rev_ttm,
            "op_margin":  op_margin,
            "rpo_series": self.get_rpo_series(ticker),
        }

    def get_lt_debt_from_normalized(self, ticker: str) -> float:
        """annual JSONでlong_term_debtが欠落した場合の補完値をnormalized quarterlyから取得。

        BUG-NETDEBT-2/3の共通修正ロジック。pipeline.pyの同名メソッドもここに委譲。
        """
        norm_path = os.path.join(os.path.dirname(self.data_dir), "normalized",
                                 f"{ticker}_quarterly_normalized.json")
        try:
            with open(norm_path, encoding="utf-8") as f:
                norm = json.load(f)
            lt_entries = [e for e in norm.get("fields", {}).get("LTDebt", []) if e.get("val") is not None]
            if lt_entries:
                return float(max(lt_entries, key=lambda e: e["end"])["val"])
        except Exception:
            pass
        return 0.0

    def _lookup_last_confirmed_zero_year(self, ticker: str, field: str) -> Optional[int]:
        """
        FY52WEEK-BS-FADEOUT-FALLBACK-1: 最新年度でfieldが完全欠損（None）の
        場合に、直近の過去年度から新しい順に走査し、最初に見つかった非None値
        が明示的に0であればその年度を返す（真のゼロとして推定してよい根拠）。

        最初に見つかった非None値が0でない場合（CSGP/short_term_investments・
        KULR/short_term_debt・RCAT/long_term_debt型: 直近の既知値が非ゼロの
        複雑パターン、[[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]参照）は
        Noneを返し、フォールバックを適用しない（誤って真のゼロと推定しない
        安全側の設計）。年数閾値は設けない（過去何年前の$0実績でも対象）。

        本メソッドは呼び出し元（get_net_cash）で「最新年度がNoneの場合のみ」
        呼ばれる前提。most_recent（annual_data[0]）自体はここでは除外せず、
        呼び出し元がget_annual_range()の2件目以降を渡す設計ではなく、本メソッド
        内でget_annual_range()を独自に取得し直し先頭（最新）をスキップする。
        """
        all_annual = self.get_annual_range(ticker, years=100)
        for entry in all_annual[1:]:
            val = entry.get("bs", {}).get(field)
            if val is None:
                continue
            if val == 0:
                try:
                    return int(entry.get("period"))
                except (TypeError, ValueError):
                    return None
            return None
        return None

    def get_net_cash(self, ticker: str, sector: Optional[str] = None, industry: str = "") -> dict:
        """
        ネットキャッシュ関連BSデータを取得 v8.1

        ネットキャッシュ = (現金 + 短期投資) - (長期有利子負債 + 短期有利子負債)
        プラス → 純キャッシュ（負債より現金が多い）
        マイナス → 純負債（負債が現金を上回る）

        v8.1追加: セクターガード
          - 保険 (Insurance): 有利子負債フィールドに保険準備金が混入するリスクあり
            → net_cash計算では負債側を0とし、現金のみで近似（保守的）
          - Fintech (Financial Services): DebtCurrent等に顧客預金が混入するリスク
            → long_term_debt のみを使用（short_term_debtを除外）

        Returns:
            {
                "cash":                   float
                "short_term_investments": float
                "long_term_debt":         float
                "short_term_debt":        float
                "net_cash":               float  ネットキャッシュ（符号付き）
                "fiscal_year":            int    取得会計年度
                "available":              bool   データ取得成功フラグ
                "sector_guard":           str    適用したセクターガード名（v8.1）
                "net_debt_period":        str    実際にBS項目を取得した時点のラベル
                                                  （"FYnnnn" | 四半期のperiod文字列 |
                                                  "Cash={四半期}/Debt=FYnnnn"）
                                                  ARCH-DATA-1残課題①でreport.txt表示
                                                  （pipeline.py）向けに追加
                "cash_missing":           bool   annual・四半期のいずれからも
                                                  cash_and_equivalentsを取得できな
                                                  かった場合True（FY52WEEK-BS-
                                                  NULL-SILENT-1 Phase A）。この場合
                                                  available=Falseも同時にFalseに
                                                  なり、net_cash自体は0円扱いのまま
                                                  算出されるが呼び出し元は
                                                  availableで信頼性を判定する前提
                "sti_approximated":       bool   short_term_investmentsがticker_
                                                  restrictionsのcross_filing_tags
                                                  経由の複数タグ合算近似値である
                                                  場合True（NVDA-STI-TAG-
                                                  UNIDENTIFIED-1）。四半期側の値に
                                                  切り替わった場合はFalseに戻る
                "sti_residual_pct":       float  近似値採用時の実額比残差率
                                                  （例: 0.0088 = +0.88%）。
                                                  sti_approximated=Falseの場合None
                "sti_estimated_zero":     bool   short_term_investmentsが最新年度
                                                  完全欠損かつ過去の直近既知値が
                                                  明示的0だったため、真のゼロと
                                                  推定した場合True（FY52WEEK-BS-
                                                  FADEOUT-FALLBACK-1）。四半期側の
                                                  値に切り替わった場合はFalseに戻る
                "sti_last_confirmed_zero_year": int  推定ゼロ採用時、最後に明示的
                                                  0が確認された年度。
                                                  sti_estimated_zero=Falseの場合None
                "ltdebt_estimated_zero":  bool   long_term_debt版（同上）
                "ltdebt_last_confirmed_zero_year": int  同上
                "stdebt_estimated_zero":  bool   short_term_debt版（同上）
                "stdebt_last_confirmed_zero_year": int  同上
            }
        """
        annual_data = self.get_annual_range(ticker, years=1)
        if not annual_data:
            return {
                "cash": 0.0, "short_term_investments": 0.0,
                "long_term_debt": 0.0, "short_term_debt": 0.0,
                "net_cash": 0.0, "fiscal_year": 0, "available": False,
                "sector_guard": "none", "net_debt_period": "",
                "cash_missing": False,
                "sti_approximated": False, "sti_residual_pct": None,
                "sti_estimated_zero": False, "sti_last_confirmed_zero_year": None,
                "ltdebt_estimated_zero": False, "ltdebt_last_confirmed_zero_year": None,
                "stdebt_estimated_zero": False, "stdebt_last_confirmed_zero_year": None,
            }

        latest = annual_data[0]
        bs = latest.get("bs", {})

        try:
            fy = int(latest.get("period", 0))
        except (ValueError, TypeError):
            fy = 0

        # FY52WEEK-BS-NULL-SILENT-1 Phase A: cash_and_equivalentsはNone率が
        # 全105銘柄実測でほぼ0-4%（CASH-TAG-MISSING-1系の既知欠落を除けば
        # ほぼ確実にデータ異常のシグナル）のため、`or 0`で暗黙にゼロ化せず
        # Noneを保持する（後段の四半期上書き・欠損判定で扱う）。
        # short_term_investments/long_term_debt/short_term_debtは「真のゼロ」
        # との判別が困難なため対象外（Phase B/C、従来通り`or 0`を維持）。
        cash    = bs.get("cash_and_equivalents")
        _sti_raw    = bs.get("short_term_investments")
        _ltdebt_raw = bs.get("long_term_debt")
        _stdebt_raw = bs.get("short_term_debt")
        st_inv  = _sti_raw or 0
        lt_debt = _ltdebt_raw or 0
        st_debt = _stdebt_raw or 0
        net_debt_period = f"FY{fy}"

        # NVDA-STI-TAG-UNIDENTIFIED-1: cross_filing_tags由来の近似値
        # （複数XBRLタグ合算・残差あり）かどうかをannual側のbs_provenanceから
        # 引き継ぐ。四半期側で上書きされた場合はFalse/Noneにリセットする
        # （四半期側は同一filing内合算のため近似ではない。下記参照）。
        _sti_prov = (latest.get("bs_provenance") or {}).get("short_term_investments") or {}
        sti_approximated = bool(_sti_prov.get("is_approximated"))
        sti_residual_pct = _sti_prov.get("residual_pct")

        # FY52WEEK-BS-FADEOUT-FALLBACK-1: 最新年度が完全欠損（None）の場合、
        # 過去の直近既知値が明示的0であればそれを真のゼロとして採用する
        # （年数閾値なし）。直近既知値が非ゼロの場合（CSGP/short_term_
        # investments・KULR/short_term_debt・RCAT/long_term_debt型）は
        # 適用しない（[[BS-FIELD-FADEOUT-NONZERO-LAST-VALUE-1]]参照）。
        sti_estimated_zero = False
        sti_last_confirmed_zero_year: Optional[int] = None
        ltdebt_estimated_zero = False
        ltdebt_last_confirmed_zero_year: Optional[int] = None
        stdebt_estimated_zero = False
        stdebt_last_confirmed_zero_year: Optional[int] = None
        if _sti_raw is None:
            _yr = self._lookup_last_confirmed_zero_year(ticker, "short_term_investments")
            if _yr is not None:
                sti_estimated_zero = True
                sti_last_confirmed_zero_year = _yr
        if _ltdebt_raw is None:
            _yr = self._lookup_last_confirmed_zero_year(ticker, "long_term_debt")
            if _yr is not None:
                ltdebt_estimated_zero = True
                ltdebt_last_confirmed_zero_year = _yr
        if _stdebt_raw is None:
            _yr = self._lookup_last_confirmed_zero_year(ticker, "short_term_debt")
            if _yr is not None:
                stdebt_estimated_zero = True
                stdebt_last_confirmed_zero_year = _yr

        # BUG-NETDEBT-3: annual JSONでlong_term_debtが欠落した場合にnormalized quarterlyで補完
        if lt_debt == 0:
            _lt_debt_normalized = self.get_lt_debt_from_normalized(ticker)
            if _lt_debt_normalized != 0:
                # 正規化四半期データから真の値が見つかった場合、推定ゼロの
                # 前提が崩れるためフラグを解除する
                lt_debt = _lt_debt_normalized
                ltdebt_estimated_zero = False
                ltdebt_last_confirmed_zero_year = None

        # BUG-NETDEBT-4: 同一時点原則（最新quarterly_*.jsonから全項目一括取得）
        # BUG-NETDEBT-1はCashのみ四半期上書きしていたが、負債が年次に留まる非対称性を解消。
        # 四半期にCash+負債が揃う場合は全項目を同一filingから参照する。
        ticker_dir = os.path.join(self.data_dir, ticker.upper())
        try:
            _q_files = sorted([
                f for f in os.listdir(ticker_dir)
                if f.startswith("quarterly_") and f.endswith(".json")
            ])
            if _q_files:
                with open(os.path.join(ticker_dir, _q_files[-1]), encoding="utf-8") as _qf:
                    _latest_q = json.load(_qf)
                _qbs = _latest_q.get("bs", {})
                _q_cash    = _qbs.get("cash_and_equivalents")
                _q_lt      = _qbs.get("long_term_debt")
                _q_st      = _qbs.get("short_term_debt")
                _q_sti_raw = _qbs.get("short_term_investments")
                _q_sti     = _q_sti_raw or 0
                _q_period  = _latest_q.get("period", "")
                if _q_cash is not None and _q_lt is not None:
                    # LTDebtが明示的に取得できる → 全項目を同一時点で統一（最優先）
                    # LTDebtがNone（パース失敗）の場合はフォールバック（負債は年次維持）
                    cash    = float(_q_cash)
                    lt_debt = float(_q_lt)
                    st_debt = float(_q_st or 0)
                    st_inv  = float(_q_sti)
                    net_debt_period = _q_period
                    # 四半期側のSTIに切り替わったため、annual側の近似値フラグは
                    # 引き継がない（同一filing内合算・cross_filing_tagsの
                    # quarterly側エントリは近似ではない正規の合算値のため）
                    sti_approximated = False
                    sti_residual_pct = None
                    # FY52WEEK-BS-FADEOUT-FALLBACK-1: ltdebtはこの分岐に入る
                    # 前提（_q_ltがNoneでない）が既に「四半期側の実データ」を
                    # 意味するため無条件で解除する。stdebt/stiは、四半期
                    # 自身が当該フィールドを持っている場合のみ解除する
                    # （四半期側もannual側と同じタグ欠損を抱えている場合
                    # 〈_q_st/_q_sti_rawがNone〉、annual側の推定ゼロ注記を
                    # 四半期側のor 0変換で誤って消さないため）。
                    ltdebt_estimated_zero = False
                    ltdebt_last_confirmed_zero_year = None
                    if _q_st is not None:
                        stdebt_estimated_zero = False
                        stdebt_last_confirmed_zero_year = None
                    if _q_sti_raw is not None:
                        sti_estimated_zero = False
                        sti_last_confirmed_zero_year = None
                elif _q_cash is not None:
                    # CashはあるがLTDebt未取得 → Cashのみ上書き（後方互換）
                    cash   = float(_q_cash)
                    if _q_sti > 0:
                        st_inv = float(_q_sti)
                        sti_approximated = False
                        sti_residual_pct = None
                        sti_estimated_zero = False
                        sti_last_confirmed_zero_year = None
                    net_debt_period = f"Cash={_q_period}/Debt=FY{fy}"
        except Exception:
            pass

        # FY52WEEK-BS-NULL-SILENT-1 Phase A: annual・四半期のいずれからも
        # cash_and_equivalentsを取得できなかった場合、DuPont分解（pipeline.py）
        # と同じ「ゼロ継続ではなく除外」方針に倣い、available=Falseを明示する。
        # calculate_bs_adjustment()がavailable=Falseの場合net_cash_per_share=0.0
        # にフォールバックする既存ロジックにより、BS補正自体が安全にスキップされる
        # （net_cash自体は0円扱いのまま算出するが、availableで信頼性を明示するため
        # 呼び出し元が誤って使うことはない）。
        cash_missing = cash is None
        if cash_missing:
            cash = 0.0

        # ── セクターガード（v8.1: industry優先判定）──
        # _is_insurance()と同じロジックをここで直接適用
        # （adjustments.pyのimportを避けるため複製）
        def _is_insurance_local(t: str, s: Optional[str], ind: str) -> bool:
            if ind:
                ind_lower = ind.lower()
                if "insurance" in ind_lower or "managed health" in ind_lower:
                    return True
            INSURANCE_TICKERS = {
                "UNH", "CVS", "CI", "HUM", "ELV", "CNC", "MOH",
                "MET", "PRU", "AFL", "ALL", "TRV", "CB", "HIG",
                "PGR", "AIZ", "CINF", "AIG", "L", "GL",
            }
            if t.upper() in INSURANCE_TICKERS:
                return True
            return s == "Insurance"

        sector_guard = "none"
        if _is_insurance_local(ticker, sector, industry):
            lt_debt = 0.0
            st_debt = 0.0
            sector_guard = "insurance_liabilities_excluded"
        elif sector == "Financial Services":
            st_debt = 0.0
            sector_guard = "fintech_st_debt_excluded"

        net_cash = (cash + st_inv) - (lt_debt + st_debt)
        available = (not cash_missing) and any([cash, st_inv, lt_debt, st_debt])

        return {
            "cash":                   float(cash),
            "short_term_investments": float(st_inv),
            "long_term_debt":         float(lt_debt),
            "short_term_debt":        float(st_debt),
            "net_cash":               float(net_cash),
            "fiscal_year":            fy,
            "available":              available,
            "sector_guard":           sector_guard,
            "net_debt_period":        net_debt_period,
            "cash_missing":           cash_missing,
            "sti_approximated":       sti_approximated,
            "sti_residual_pct":       sti_residual_pct,
            "sti_estimated_zero":     sti_estimated_zero,
            "sti_last_confirmed_zero_year": sti_last_confirmed_zero_year,
            "ltdebt_estimated_zero":  ltdebt_estimated_zero,
            "ltdebt_last_confirmed_zero_year": ltdebt_last_confirmed_zero_year,
            "stdebt_estimated_zero":  stdebt_estimated_zero,
            "stdebt_last_confirmed_zero_year": stdebt_last_confirmed_zero_year,
        }

    # =========================================
    # Adjusted EPS Analyzer用ヘルパー
    # =========================================
    
    def get_eps_diluted(self, ticker: str, quarter: str) -> Optional[float]:
        """四半期EPSを取得"""
        data = self.get_quarterly(ticker, quarter)
        if data:
            return data.get("pl", {}).get("eps_diluted")
        return None
    
    # =========================================
    # ユーティリティ
    # =========================================
    
    def _load_json(self, path: str) -> Optional[Dict[str, Any]]:
        """JSONファイル読み込み"""
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return None
    
    def get_available_tickers(self) -> List[str]:
        """データが存在するティッカー一覧"""
        if not os.path.exists(self.data_dir):
            return []
        
        tickers = []
        for name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, name)
            if os.path.isdir(path) and not name.startswith("_"):
                tickers.append(name)
        
        return sorted(tickers)
    
    def get_data_summary(self, ticker: str) -> Dict[str, Any]:
        """ティッカーのデータサマリー"""
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        
        if not os.path.exists(ticker_dir):
            return {"error": "データなし"}
        
        files = os.listdir(ticker_dir)
        annual_files = [f for f in files if f.startswith("annual_")]
        quarterly_files = [f for f in files if f.startswith("quarterly_")]
        
        info = get_ticker_info(ticker)
        
        return {
            "ticker": ticker,
            "name": info["name"],
            "status": info["status"],
            "annual_count": len(annual_files),
            "quarterly_count": len(quarterly_files),
            "has_company_facts": "company_facts.json" in files,
        }


# シングルトンインスタンス
_reader = None

def get_reader() -> SECReader:
    """グローバルリーダーインスタンス取得"""
    global _reader
    if _reader is None:
        _reader = SECReader()
    return _reader


if __name__ == "__main__":
    reader = SECReader()
    
    # テスト
    print("=== 利用可能ティッカー ===")
    print(reader.get_available_tickers())
    
    print("\n=== TSLA サマリー ===")
    print(reader.get_data_summary("TSLA"))
    
    print("\n=== TSLA FCF 5年平均 ===")
    print(f"${reader.get_fcf_5yr_avg('TSLA'):,.0f}")
    
    print("\n=== TSLA ROE平均 ===")
    print(f"{reader.get_roe_avg('TSLA'):.1%}")
