"""
TANUKI VALUATION - Data Fetcher v2.4
SEC EDGAR + yfinance ハイブリッド取得（マイクロキャップ対応 + β取得）

v2.4 変更点:
- beta_config.json によるKoichi意図βを最優先採用
  優先順位: beta_config.json override > yfinance β > セクターデフォルト
- β採用理由をログ出力・latest.jsonに記録
"""

import logging
import os
import sys
import json
from datetime import date
from statistics import mean
from typing import Dict, Any, Optional, Tuple

# SEC EDGAR - common/sec_data/reader.py
HAS_SEC = False
SECReader = None
repo_root: str | None = None

# QUALITY-GATES-EPIC-1 Phase 3a: common/sec_data/contracts.py（正規化契約の型）。
# reader.pyと同じsys.path解決に依存するためHAS_SECと同じtry/exceptブロックに同居させる。
FCFSeries = None
ContractViolation = None

try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))

    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    from common.sec_data.reader import SECReader
    from common.sec_data.contracts import FCFSeries, ContractViolation
    HAS_SEC = True
except Exception:
    pass

if not HAS_SEC:
    try:
        github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if github_workspace:
            repo_root = github_workspace
        if github_workspace and github_workspace not in sys.path:
            sys.path.insert(0, github_workspace)
        from common.sec_data.reader import SECReader
        from common.sec_data.contracts import FCFSeries, ContractViolation
        HAS_SEC = True
    except Exception:
        pass

# common/market_data - common.market_data.reader（BACKLOG [[MARKETDATA-
# LAYER-CONSTRUCTION-1]]着手順序4-2: 株価・β・PER等のyfinance直接呼び出し
# 〈.info単発〉をcommon.market_data.reader経由に切替。HAS_SECと同じ
# sys.path解決・二段構えtry/exceptパターンを踏襲）
HAS_MARKET_DATA = False
_md_get_latest_price = None
_md_get_attributes = None
_md_get_ma_deviation = None

try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))

    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    from common.market_data.reader import get_latest_price as _md_get_latest_price
    from common.market_data.reader import get_attributes as _md_get_attributes
    from common.market_data.reader import get_ma_deviation as _md_get_ma_deviation
    HAS_MARKET_DATA = True
except Exception:
    pass

if not HAS_MARKET_DATA:
    try:
        github_workspace = os.environ.get("GITHUB_WORKSPACE", "")
        if github_workspace:
            repo_root = github_workspace
        if github_workspace and github_workspace not in sys.path:
            sys.path.insert(0, github_workspace)
        from common.market_data.reader import get_latest_price as _md_get_latest_price
        from common.market_data.reader import get_attributes as _md_get_attributes
        from common.market_data.reader import get_ma_deviation as _md_get_ma_deviation
        HAS_MARKET_DATA = True
    except Exception:
        pass


# セクター別デフォルトβ
SECTOR_DEFAULT_BETA = {
    "Technology": 1.20,
    "Consumer Cyclical": 1.10,
    "Consumer Defensive": 0.80,
    "Communication Services": 1.00,
    "Healthcare": 0.90,
    "Financial Services": 1.30,
    "Industrials": 1.10,
    "Energy": 1.15,
    "Basic Materials": 1.10,
    "Real Estate": 0.90,
    "Utilities": 0.50,
    "default": 1.00
}


def _select_fcf_source(
    annual_fcf_list: list, ttm_fcf_series: list | None, min_years: int = 3
) -> tuple[list, bool]:
    """
    年次FCFリストとTTM FCF系列のどちらを採用するか決定する。

    TTM系列（四半期粒度・鮮度が高い）を常に優先する。ただし
    TTM-QUARTERS-CHECK-1の完全性フィルタ適用後にTTM系列の点数が
    min_years（core_calculator.min_fcf_yearsと同期。デフォルト3）未満に
    落ち込み、かつ年次実績の方が多い場合（CRWV/CON等、四半期粒度データの
    蓄積が浅い銘柄）に限り、より充実した年次実績を優先する。
    「TTM点数<年次点数なら常に年次優先」にすると、TTM点数が3〜4点で
    十分な大多数の銘柄（annual_fcf_list=5年がデフォルトのため）まで
    年次へ後退してしまうため、min_years判定を必須とする。

    Returns:
        (採用するFCFリスト, TTM系列を採用したか)
    """
    if not ttm_fcf_series:
        return annual_fcf_list, False
    if len(ttm_fcf_series) >= min_years:
        return ttm_fcf_series, True
    if len(annual_fcf_list) > len(ttm_fcf_series):
        return annual_fcf_list, False
    return ttm_fcf_series, True


def _select_fcf_dates(
    use_ttm: bool, annual_fcf_dates: list | None, ttm_fcf_dates: list | None
) -> list | None:
    """
    _select_fcf_source()が確定した採用経路（use_ttm）に対応する日付リストを
    返す薄いヘルパー（[[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]）。

    採用経路そのものの決定ロジック（min_years判定等）は_select_fcf_source()
    にのみ実装されており、本関数はその結果をそのまま使って対応する日付集合を
    選ぶだけ（ロジックの重複実装ではない）。_select_fcf_source()の既存の
    呼び出し元・テスト（2-tuple返却）への影響を避けるため、あえて別関数に
    分離した。
    """
    return ttm_fcf_dates if use_ttm else annual_fcf_dates


def _quarters_complete(flow: dict, *field_names: str, min_quarters: int = 4) -> bool:
    """
    指定フィールドすべてがquarters_used>=min_quartersを満たすか判定する。

    TTM-QUARTERS-CHECK-1対応: 四半期粒度のSECデータ取得が始まる前の境界期間
    （2022年Q1前後が大半）では、本来4四半期必要な集計が1〜3四半期分しか
    揃っていない不完全なTTM値がflow内に存在する。field_nameが未取得（キー自体が
    存在しない）場合もquarters_used=0扱いとして不完全と判定する。
    """
    return all(
        flow.get(name, {}).get("quarters_used", 0) >= min_quarters
        for name in field_names
    )


# TTM鮮度チェックの閾値（日数）。件数（quarters_used）が揃っていても、
# 最新end日がこれより古い期間は「同一値の使い回し」（LLY型・タグ切替見逃し等の
# 再発）を疑い除外する。
#
# 閾値根拠（2026-07-12実測）: 105銘柄のTTM系列でttm_end(最新)と実行日の差を
# 集計した結果、正常銘柄は44〜113日に収まっていた（四半期決算の通常の報告
# ラグ。10-Q提出期限は決算期末から最大45〜90日、大型株中心のためほぼ全銘柄が
# 45日区分）。この範囲の3倍弱（四半期3回分＝約270日）を閾値とすることで、
# 通常の報告ラグは正常範囲として許容しつつ、四半期を2回以上連続で取りこぼす
# ような真の陳腐化（LLY CapExの旧タグは発見時点で3年以上=1000日超陳腐化していた）
# のみを検知する。
_TTM_FRESHNESS_MAX_DAYS = 270


def _quarters_fresh(ttm_end: str | None, max_age_days: int = _TTM_FRESHNESS_MAX_DAYS) -> bool:
    """
    ttm_end（TTM期間の最新end日）が現在日からmax_age_days以内かを判定する。

    件数（quarters_used）だけを見る_quarters_complete()では、候補タグが
    サイレントに申告停止し古い値のまま四半期件数だけ満たしているケース
    （LLY型）を検知できない。ttm_endが不明・不正な日付の場合は判定不能として
    Falseを返す（保守的に「新鮮でない」扱いとし、完全性フィルタと同様に
    除外側に倒す）。
    """
    if not ttm_end:
        return False
    try:
        end_dt = date.fromisoformat(ttm_end)
    except ValueError:
        return False
    return (date.today() - end_dt).days <= max_age_days


def _freshest_end(series: list[dict]) -> str | None:
    """seriesの中で最もend日が新しいttm_endを返す（並び順に依存しない）。

    TTMReaderは通常降順（最新が先頭）でJSONを読み込むが、呼び出し側の
    順序を前提にせず安全側に倒すため、明示的にmax()で最新を求める。
    """
    ends = [s.get("ttm_end") for s in series if s.get("ttm_end")]
    return max(ends) if ends else None


class TTMReader:
    """TTM系列ファイル（{ticker}_ttm_series.json）の読み込み"""

    def __init__(self, ticker: str, repo_root_path: str | None):
        self.ticker = ticker.upper()
        self._series: list[dict] | None = None
        if repo_root_path:
            self._path: str | None = os.path.join(
                repo_root_path, "common", "sec_data", "ttm",
                f"{self.ticker}_ttm_series.json",
            )
        else:
            self._path = None
        self._load()

    def _load(self) -> None:
        if not self._path:
            logging.warning("[%s] TTMReader: repo_root未解決のためスキップ", self.ticker)
            return
        try:
            with open(self._path, encoding="utf-8") as f:
                data = json.load(f)
                self._series = data.get("series", [])
        except FileNotFoundError:
            self._series = None
            logging.warning("[%s] TTM series file not found: %s", self.ticker, self._path)

    def _filtered_fcf(self) -> tuple[list[float], list[str]] | tuple[None, None]:
        """
        フィルタ・鮮度チェックのみを行い、検証前の(vals, dates)を返す。
        get_fcf_series()/get_fcf_dates()共通のフィルタリング実装
        （[[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]、値と日付のペア整合性を
        1箇所でのみ組み立てることで両者のズレを構造的に防ぐ）。

        OCF・CapExいずれかがquarters_used<4（四半期集計が不完全）の期間は
        除外する（TTM-QUARTERS-CHECK-1）。FCF=OCF-CapExのため、片方でも
        不完全ならFCF自体の値も欠陥値になる。

        鮮度チェック（LLY-CAPEX-STALE-1型のタグ切替見逃し再発対策）は
        系列先頭（最新期間）のみに適用する。TTM系列は各エントリが約1年間隔で
        過去5年分の実データとして保存される設計のため、series[0]以外は
        正常な銘柄でも構造的に365日以上古い（過去年度の正規のTTMスナップショット）。
        全エントリに鮮度フィルタを適用すると正常な過去実績まで陳腐化扱いされ
        全銘柄でシリーズが1点以下に縮退してしまうため、「最新のはずのデータが
        実際には新しくない」ことだけを検知する目的でseries[0]限定とする。
        """
        if not self._series:
            return None, None
        if not _quarters_fresh(_freshest_end(self._series)):
            return None, None
        filtered = [
            s for s in self._series
            if s.get("flow", {}).get("FCF", {}).get("val") is not None
            and _quarters_complete(s.get("flow", {}), "operating_cash_flow", "capital_expenditure")
        ]
        if len(filtered) < 2:
            return None, None
        vals = [s["flow"]["FCF"]["val"] for s in filtered]
        dates = [s["ttm_end"] for s in filtered]
        return vals, dates

    def _validated_fcf_series(self) -> tuple[list[float], list[str]] | tuple[None, None]:
        """
        QUALITY-GATES-EPIC-1 Phase 3a: _filtered_fcf()の結果を
        common.sec_data.contracts の FCFSeries を経由させ、「新しい順
        （ttm_end降順）」規約を construction 時に検証する
        （GROWTH-CAGR-SIGN-1のような順序取り違えバグの再発防止）。

        get_fcf_series()/get_fcf_dates()の共通実装。検証を1箇所に
        集約することで、値だけ検証して日付は未検証のまま返す、といった
        ズレを防ぐ。
        """
        vals, dates = self._filtered_fcf()
        if vals is None:
            return None, None
        if FCFSeries is None:
            # contracts.py未import環境（HAS_SEC=False）向けフォールバック。
            # 通常の実行環境では発生しない。
            return vals, dates
        try:
            FCFSeries(vals, dates)
            return vals, dates
        except ContractViolation as e:
            logging.error("[%s] FCFSeries順序規約違反: %s", self.ticker, e)
            return None, None

    def get_fcf_series(self) -> list[float] | None:
        """
        FCF系列をfloatリストで返す（降順・最新が先頭）。2点未満・順序規約
        違反はNone。

        FCFSeries自体はJSONシリアライズ不可能なため、検証のみに使い
        素の list[float] に変換してから返す（戻り値の型・呼び出し元への
        影響は変えない）。
        """
        vals, _dates = self._validated_fcf_series()
        return vals

    def get_fcf_dates(self) -> list[str] | None:
        """
        get_fcf_series()が返すFCFリストに対応するttm_end日付リストを返す。

        [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]: growth.py側でFCF CAGR計算
        直前に順序を再検証するための日付供給元。get_fcf_series()と同一の
        フィルタ・鮮度チェック・順序検証を経由するため、両者が非Noneの
        場合は常に同じ長さ・同じ並び順で対応する。
        """
        _vals, dates = self._validated_fcf_series()
        return dates

    def get_ttm_end(self) -> str | None:
        if self._series:
            return self._series[0].get("ttm_end")
        return None

    def get_periods(self) -> int:
        """get_fcf_series()が実際に採用する点数と一致させる（表示用の点数が
        実データと乖離しないよう、同一の完全性・鮮度フィルタを適用する。
        鮮度チェックはget_fcf_series()と同様series[0]限定）"""
        if not self._series:
            return 0
        if not _quarters_fresh(_freshest_end(self._series)):
            return 0
        return sum(
            1 for s in self._series
            if s.get("flow", {}).get("FCF", {}).get("val") is not None
            and _quarters_complete(s.get("flow", {}), "operating_cash_flow", "capital_expenditure")
        )

    def get_series(self) -> list[dict] | None:
        return self._series


def build_rice_annual_shape(ttm_series: list[dict]) -> list[dict]:
    """
    TTM系列を rice.py が期待する annual_data 形式に変換するアダプター。
    rice.py は変更しない — このアダプターでインターフェースを吸収する。

    rice.py が使うフィールド:
      period                      ← "TTM@{ttm_end}" 形式（警告ログ用）
      cf.operating_cash_flow      ← operating_cash_flow
      cf.capital_expenditure      ← capital_expenditure
      pl.revenue                  ← revenue
      pl.net_income               ← net_income
      pl.research_and_development ← research_and_development（Noneの場合はNoneのまま渡す）
      pl.selling_and_marketing    ← selling_and_marketing（Noneの場合はNoneのまま渡す、rice側で or 0.0）
      data_quality                ← sga_gap_warning用（TTMパスでは空dict）

    operating_cash_flow/capital_expenditure/revenue/net_incomeのいずれかが
    quarters_used<4（四半期集計が不完全）の期間はresultから除外する
    （TTM-QUARTERS-CHECK-1）。research_and_development/selling_and_marketingは
    rice.py側で既にNone許容（0扱い・警告ログのみ）のためチェック対象外。

    stock_based_compensationは[[TTM-SBC-QUARTERS-GAP-1]]対応（2026-09-10）:
    quarters_used<4の期間が実データで105銘柄相当中40件（うち32件は
    val自体もNone、rice.py側の`sbc = ... or 0.0`により従来通り安全に
    SBC=0へフォールバックする）存在したが、残り8件（GEV/HWM/TDY）は
    val自体は非Noneの部分四半期合計（例: GEV quarters_used=1で
    54,000,000）であり、これが完全な年間SBCであるかのように
    _calc_q()のQ=OCF/(純利益+SBC)計算へ渡り、SBCを最大約13%
    過小評価する形で分母を歪めていた（実データで影響を実測・確認済み）。
    OCF/CapEx/Revenue/NetIncomeと同様に行全体を除外する対応（本関数の
    _quarters_complete()対象に追加）も検討したが、40件中32件（val=None
    のケース）はrice.py側で元々安全にフォールバックしており行全体を
    捨てる必要がないため、より対象を絞った修正として、
    stock_based_compensation自体のquarters_used<4の場合のみvalを
    Noneへ差し替える（research_and_development/selling_and_marketingの
    既存のNone許容パターンと同一の扱いに揃える）。これにより8件の
    誤った部分合計はNone化されrice.py側のor 0.0フォールバックで
    正しくSBC=0扱いとなり、残り32件の他フィールドが完全な行は
    引き続きRICE計算の対象として維持される。

    鮮度チェック（LLY-CAPEX-STALE-1型対策）はttm_series[0]（最新、降順ソート
    前提）のみに適用する。TTM系列は各エントリが約1年間隔で保存される設計の
    ため、series[0]以外は正常な銘柄でも構造的に365日以上古く、全エントリに
    適用すると正常な過去実績まで除外されてしまう（get_fcf_series()と同じ理由）。

    [[TTM-PASCALCASE-KEY-STALE-1]]対応（2026-07-29）: フィールド名は
    フェーズC移行（ttm_calculator.py snake_case化）後のLayer3命名
    （config/sec_concept_definitions.json）に統一する。旧PascalCase
    （"OCF"/"CapEx"/"Revenue"/"NetIncome"/"RD"/"SM"/"SBC"）のままだった
    ため2026-07-26のデータ再生成以降、全ての結果がフィルタで除外され
    本関数が常に空リストを返す状態になっていた（RICEスコア全銘柄停止）。
    """
    if ttm_series and not _quarters_fresh(_freshest_end(ttm_series)):
        return []
    result = []
    for s in ttm_series:
        flow = s.get("flow", {})
        if not _quarters_complete(flow, "operating_cash_flow", "capital_expenditure", "revenue", "net_income"):
            continue
        # [[TTM-SBC-QUARTERS-GAP-1]]対応: SBCのquarters_used<4は行全体を
        # 除外せず、SBC自体をNone化してresearch_and_development/
        # selling_and_marketingと同じNone許容パス（rice.py側でor 0.0）に
        # 委ねる（理由は本関数docstring参照）。
        sbc_field = flow.get("stock_based_compensation", {})
        sbc_val = sbc_field.get("val") if sbc_field.get("quarters_used", 0) >= 4 else None
        result.append({
            "period": f"TTM@{s.get('ttm_end', '?')}",
            "cf": {
                "operating_cash_flow":    flow.get("operating_cash_flow", {}).get("val"),
                "capital_expenditure":    flow.get("capital_expenditure", {}).get("val"),
                "stock_based_compensation": sbc_val,
            },
            "pl": {
                "revenue":                  flow.get("revenue", {}).get("val"),
                "net_income":               flow.get("net_income", {}).get("val"),
                "research_and_development": flow.get("research_and_development", {}).get("val"),
                "selling_and_marketing":    flow.get("selling_and_marketing", {}).get("val"),
            },
            "data_quality": {},
        })
    return result


def resolve_beta_config_path() -> Optional[str]:
    """beta_config.jsonのパス解決ロジック（report_consistency_check.pyの
    設定ファイル読み込み横断チェックと共用するため、2026-08-16に
    _load_beta_config()から切り出した。[[CONFIG-LOAD-SILENT-FALLBACK-1]]）。

    GitHub Actions 実行時は GITHUB_WORKSPACE から、
    ローカル実行時はファイルの相対パスで解決する。

    Returns:
        解決できたパス（存在確認済み）、解決できなければNone
    """
    search_paths = []

    # GitHub Actions 環境
    workspace = os.environ.get("GITHUB_WORKSPACE", "")
    if workspace:
        search_paths.append(os.path.join(workspace, "config", "beta_config.json"))

    # ローカル: このファイルから4階層上がるとリポジトリルート
    try:
        current = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(current)))
        search_paths.append(os.path.join(repo_root, "config", "beta_config.json"))
    except Exception:
        pass

    return next((p for p in search_paths if os.path.exists(p)), None)


def _load_beta_config() -> Dict[str, Any]:
    """
    config/beta_config.json を読み込む

    GitHub Actions 実行時は GITHUB_WORKSPACE から、
    ローカル実行時はファイルの相対パスで解決する。
    """
    path = resolve_beta_config_path()
    if path is not None:
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"   [WARN] beta_config.json 読み込みエラー: {e}")
            return {}

    return {}


class TanukiDataFetcher:
    """
    TANUKI VALUATION 用データフェッチャー v2.3
    
    データソース優先順位:
    1. 株式数: yfinance implied > max(SEC diluted, yfinance outstanding)
    2. FCF/Revenue/ROE/RPO: SEC XBRL (SECReaderのヘルパーメソッド使用)
    3. 株価/β: yfinance
    """
    
    def __init__(self):
        self.sec_reader = SECReader() if HAS_SEC else None
        # beta_config.json をロード（起動時1回のみ）
        _cfg = _load_beta_config()
        self._beta_overrides: Dict[str, Any] = _cfg.get("overrides", {})
        if self._beta_overrides:
            print(f"   [INFO] beta_config.json: {len(self._beta_overrides)}銘柄のβオーバーライドを読み込みました")
    
    def get_financials(self, ticker: str) -> Dict[str, Any]:
        """財務データ取得メイン関数"""
        print(f"\n   [{ticker}] データ取得開始")
        
        fcf_list = []
        annual_fcf_dates = None
        fcf_avg = 0.0
        sec_diluted = 0
        roe_avg = None
        roe_years_used = 0
        roe_outlier_adj = False
        revenue = 0.0
        rpo = 0.0
        net_cash_data = {"net_cash": 0.0, "available": False}  # BS評価補正用

        # TTM FCF/RICE 状態（TTMReader ブロックで更新）
        fcf_source = "annual_fallback"
        fcf_ttm_end: str | None = None
        fcf_ttm_periods = 0
        rice_annual_data = None
        rice_data_source = "annual_fallback"
        
        # ========================================
        # 1. SEC EDGAR
        # ========================================
        if self.sec_reader:
            try:
                fcf_avg = self.sec_reader.get_fcf_5yr_avg(ticker)
                print(f"   [{ticker}] SEC FCF 5yr avg: ${fcf_avg:,.0f}")
                
                fcf_list, annual_fcf_dates = self.sec_reader.get_fcf_list_with_dates(ticker, years=5)
                print(f"   [{ticker}] SEC FCF list: {len(fcf_list)}年分")
                
                # ファイナンスリース除外が適用されたか確認
                _annual = self.sec_reader.get_annual_range(ticker, 1)
                if _annual:
                    _fl_applied = _annual[0].get("cf", {}).get("finance_lease_payments_applied", False)
                    _fl_amt = _annual[0].get("cf", {}).get("finance_lease_payments", 0)
                    if _fl_applied:
                        print(f"   [{ticker}] ファイナンスリース除外: ${abs(_fl_amt):,.0f}をCapExから控除")
                
                sec_diluted = self.sec_reader.get_diluted_shares(ticker)
                if sec_diluted > 0:
                    print(f"   [{ticker}] SEC shares: {sec_diluted:,.0f}")
                
                roe_avg, roe_years_used, roe_outlier_adj = self.sec_reader.get_roe_avg_detail(ticker, years=10)
                _roe_str = f"{roe_avg:.1%}" if roe_avg is not None else "N/A (負債超過)"
                print(f"   [{ticker}] SEC ROE avg: {_roe_str} ({roe_years_used}yr)")
                
                revenue = self.sec_reader.get_latest_revenue(ticker)
                print(f"   [{ticker}] SEC revenue: ${revenue:,.0f}")
                
                rpo = self.sec_reader.get_rpo(ticker)
                if rpo > 0:
                    print(f"   [{ticker}] SEC RPO: ${rpo:,.0f}")

                # BS評価補正用（v8.1: sector確定後に呼ぶため、ここでは取得しない）

            except Exception as e:
                print(f"   [{ticker}] SEC取得エラー: {e}")

        # ========================================
        # TTM系列: FCFソース切り替え（年次→TTM）
        # ========================================
        ttm_reader = TTMReader(ticker, repo_root)
        fcf_series = ttm_reader.get_fcf_series()
        ttm_fcf_dates = ttm_reader.get_fcf_dates()
        fcf_list, _use_ttm_fcf = _select_fcf_source(fcf_list, fcf_series)
        fcf_dates = _select_fcf_dates(_use_ttm_fcf, annual_fcf_dates, ttm_fcf_dates)

        if _use_ttm_fcf:
            fcf_avg = float(mean(fcf_list))
            fcf_source = "ttm_series"
            fcf_ttm_end = ttm_reader.get_ttm_end()
            fcf_ttm_periods = ttm_reader.get_periods()
            print(f"   [{ticker}] TTM FCF series: {len(fcf_list)}点 end={fcf_ttm_end}")
        elif fcf_series:
            logging.warning(
                "[%s] TTM series has fewer complete points (%d) than annual FCF (%d); "
                "keeping annual FCF list", ticker, len(fcf_series), len(fcf_list)
            )
            print(f"   [{ticker}] TTM系列点数不足({len(fcf_series)}点<年次{len(fcf_list)}点) → 年次SEC実績を優先")
        else:
            logging.warning("[%s] TTM series unavailable, fallback to annual FCF", ticker)

        ttm_series_data = ttm_reader.get_series()
        rice_annual_data = build_rice_annual_shape(ttm_series_data) if ttm_series_data else []
        if rice_annual_data:
            rice_data_source = "ttm_series"
        else:
            # [[TTM-PASCALCASE-KEY-STALE-1]]対応: get_series()自体は非空でも
            # build_rice_annual_shape()の完全性フィルタで全件除外されうる
            # （field_name未取得＝quarters_used=0扱い）。get_fcf_series()/
            # _select_fcf_source()と対称に、フィルタ後の結果が空の場合のみ
            # annual実績へフォールバックする（フィルタ前のget_series()が
            # 非空というだけでrice_data_source="ttm_series"を確定させない）。
            rice_annual_data = self.sec_reader.get_annual_range(ticker, years=4) if self.sec_reader else None
            rice_data_source = "annual_fallback"

        # ========================================
        # 2. common/market_data（株式数、株価、β、セクター）
        # ========================================
        # [[MARKETDATA-LAYER-CONSTRUCTION-1]]着手順序4-2: yfinance直接呼び出し
        # （.info単発）をcommon.market_data.reader経由に切替。
        #   - current_price: reader.get_latest_price()["close"]（daily/層。
        #     旧来のcurrentPrice→regularMarketPrice→previousCloseフォール
        #     バックチェーンを置き換える——取引時間中リアルタイムから
        #     前日終値ベースへの仕様変更そのもの。previousCloseはattributes/
        #     に含まれない設計〈層またぎ再計算の禁止〉のためdaily/層に一本化）
        #   - beta以下: reader.get_attributes()（attributes/層、週次スナップ
        #     ショット）
        # reader側がNoneを返す場合（データ未生成銘柄）は、旧来の
        # 「yfinance完全失敗」except節と同じ中立デフォルトに倒す
        # （事前調査の結論・選択肢(A)を踏襲。daily/・attributes/は独立した
        # 更新頻度のため、価格取得失敗と属性取得失敗はそれぞれ独立に
        # 中立デフォルトへフォールバックする——旧コードの「tryブロック
        # 前半で部分成功した値は保持される」非atomicな挙動より一貫性が
        # 高くなる）。
        yf_implied = 0
        yf_outstanding = 0
        current_price = 0.0
        beta = None
        sector = "default"
        industry = ""       # v8.1: 保険判定精度向上のためindustryも取得
        per = None
        per_is_forward = False
        peg = None
        ps = None
        ev_ebitda = None
        ma200 = None
        forward_eps = None
        analyst_target_median = None
        analyst_target_mean = None
        analyst_target_low = None
        analyst_target_high = None
        analyst_count = None
        analyst_rec_key = ""
        dividend_yield = 0.0
        payout_ratio   = 0.0

        latest_price = _md_get_latest_price(ticker) if HAS_MARKET_DATA else None
        if latest_price is not None and latest_price.get("close") is not None:
            current_price = float(latest_price["close"])
            print(f"   [{ticker}] market_data price (daily/ {latest_price.get('date')}): ${current_price:.2f}")
        else:
            print(f"   [{ticker}] market_data daily/未取得（current_price=0.0で継続）")

        attrs = _md_get_attributes(ticker) if HAS_MARKET_DATA else None
        if attrs is not None:
            try:
                # 株式数
                yf_implied = attrs.get("implied_shares_outstanding") or 0
                if yf_implied > 0:
                    print(f"   [{ticker}] market_data implied shares: {yf_implied:,.0f}")

                yf_outstanding = attrs.get("shares_outstanding") or 0
                if yf_outstanding > 0:
                    print(f"   [{ticker}] market_data outstanding shares: {yf_outstanding:,.0f}")

                # β（ベータ）
                beta = attrs.get("beta")
                if beta is not None and beta > 0:
                    print(f"   [{ticker}] market_data beta: {beta:.2f}")

                # セクター・業種（v8.1: industryを追加取得）
                sector = attrs.get("sector") or "default"
                if sector and sector != "default":
                    print(f"   [{ticker}] market_data sector: {sector}")
                industry = attrs.get("industry") or ""
                if industry:
                    print(f"   [{ticker}] market_data industry: {industry}")

                # PER（株価収益率）
                _trailing_pe = attrs.get("trailing_pe")
                _forward_pe  = attrs.get("forward_pe")
                per = _trailing_pe or _forward_pe or None
                per_is_forward = (
                    (_trailing_pe is None or _trailing_pe <= 0)
                    and _forward_pe is not None and _forward_pe > 0
                )
                if per is not None and per > 0:
                    _pe_src = "Fwd" if per_is_forward else "Trailing"
                    print(f"   [{ticker}] market_data PER({_pe_src}): {per:.1f}")

                # PEG（成長調整PER）
                peg_raw = attrs.get("peg_ratio") or None
                if peg_raw is not None and peg_raw > 0:
                    peg = float(peg_raw)
                    print(f"   [{ticker}] market_data PEG: {peg:.2f}")

                # PS（株価売上高倍率）
                ps_raw = attrs.get("price_to_sales") or None
                if ps_raw is not None and ps_raw > 0:
                    ps = float(ps_raw)
                    print(f"   [{ticker}] market_data PS: {ps:.2f}")

                # EV/EBITDA
                ev_ebitda_raw = attrs.get("ev_to_ebitda") or None
                if ev_ebitda_raw is not None and ev_ebitda_raw > 0:
                    ev_ebitda = float(ev_ebitda_raw)
                    print(f"   [{ticker}] market_data EV/EBITDA: {ev_ebitda:.2f}")

                # 200日移動平均: reader.get_ma_deviation()（daily/由来、単一の
                # 正）を代数的に逆算してma200（価格）に戻す。get_ma_deviation()
                # がtwoHundredDayAverageを保存しない設計（BACKLOG確定事項7）
                # のため生のMA価格自体はどこにも保存されていないが、
                # pipeline.py側の既存計算式 ma200_dev=(current_price/ma200-1)
                # *100 をそのまま維持できるよう、同じ関係式を逆算してma200を
                # 復元する（current_priceはdaily/最新closeと同一日付・同一値
                # のため数学的に完全往復し、pipeline.py側の変更は不要になる。
                # ma200自体を独自ロジックで再計算しているわけではなく、
                # get_ma_deviation()の計算結果を形だけ元に戻しているだけ）。
                if HAS_MARKET_DATA:
                    ma200_dev = _md_get_ma_deviation(ticker, window=200)
                    if ma200_dev is not None and current_price > 0:
                        ma200 = current_price / (1 + ma200_dev / 100.0)
                        print(f"   [{ticker}] market_data 200MA(dev={ma200_dev:+.1f}%より逆算): ${ma200:.2f}")

                # Forward EPS（アナリスト予想EPS）
                forward_eps_raw = attrs.get("forward_eps")
                if forward_eps_raw is not None and isinstance(forward_eps_raw, (int, float)):
                    forward_eps = float(forward_eps_raw)
                    print(f"   [{ticker}] market_data forwardEps: ${forward_eps:.4f}")

                # 配当（ディビデンドトラップ判定用）
                dividend_yield = attrs.get("dividend_yield") or 0.0
                payout_ratio   = attrs.get("payout_ratio") or 0.0
                if dividend_yield > 0:
                    print(f"   [{ticker}] market_data dividend yield: {dividend_yield:.1%}, payout ratio: {payout_ratio:.1%}")

                # アナリスト目標株価
                _at_median = attrs.get("target_median_price")
                _at_mean   = attrs.get("target_mean_price")
                _at_low    = attrs.get("target_low_price")
                _at_high   = attrs.get("target_high_price")
                _at_count  = attrs.get("analyst_count")
                _at_rec    = attrs.get("analyst_recommendation_key") or ""
                if _at_median is not None and isinstance(_at_median, (int, float)) and _at_median > 0:
                    analyst_target_median = float(_at_median)
                    analyst_target_mean   = float(_at_mean)   if isinstance(_at_mean,  (int, float)) and _at_mean  > 0 else None
                    analyst_target_low    = float(_at_low)    if isinstance(_at_low,   (int, float)) and _at_low   > 0 else None
                    analyst_target_high   = float(_at_high)   if isinstance(_at_high,  (int, float)) and _at_high  > 0 else None
                    analyst_count         = int(_at_count)    if isinstance(_at_count, (int, float)) and _at_count > 0 else None
                    analyst_rec_key       = _at_rec.lower()
                    print(f"   [{ticker}] analyst target median: ${analyst_target_median:.2f} ({analyst_count} analysts)")

            except Exception as e:
                print(f"   [{ticker}] market_data attributes解析エラー: {e}")
                per = None
                per_is_forward = False
                peg = None
                ps = None
                ev_ebitda = None
                ma200 = None
                forward_eps = None
                analyst_target_median = None
                analyst_target_mean = None
                analyst_target_low = None
                analyst_target_high = None
                analyst_count = None
                analyst_rec_key = ""
                dividend_yield = 0.0
                payout_ratio   = 0.0
        else:
            print(f"   [{ticker}] market_data attributes/未取得（β・PER等は中立デフォルトで継続）")

        # ========================================
        # 3. β決定（beta_config.json > yfinance > セクターデフォルト）
        # ========================================
        final_beta, beta_source = self._determine_beta(ticker, beta, sector)
        
        # ========================================
        # 4. 株式数決定
        # ========================================
        final_shares, shares_source = self._determine_diluted_shares(
            ticker, yf_implied, yf_outstanding, sec_diluted
        )
        
        # ========================================
        # FCF 直近2年平均の計算（CV方式のベース判定用）
        # ========================================
        fcf_2yr_avg = self._calc_fcf_2yr_avg(fcf_list)
        if fcf_2yr_avg > 0:
            print(f"   [{ticker}] SEC FCF 2yr avg: ${fcf_2yr_avg:,.0f}")

        # ========================================
        # 5. BS評価補正データ取得（v8.1: sector確定後に呼ぶ）
        # ========================================
        if self.sec_reader and hasattr(self.sec_reader, 'get_net_cash'):
            try:
                net_cash_data = self.sec_reader.get_net_cash(ticker, sector=sector, industry=industry)
                guard = net_cash_data.get("sector_guard", "none")
                if guard != "none":
                    print(f"   [{ticker}] BS補正 セクターガード: {guard}")
            except Exception as e:
                print(f"   [{ticker}] get_net_cash エラー: {e}")
                net_cash_data = {"net_cash": 0.0, "available": False, "sector_guard": "none"}

        # ========================================
        # 最終サマリー
        # ========================================
        print(f"   [{ticker}] 最終結果:")
        print(f"       FCF 5yr Avg: ${fcf_avg:,.0f}")
        print(f"       FCF 2yr Avg: ${fcf_2yr_avg:,.0f}")
        print(f"       Diluted Shares: {final_shares:,.0f} ({shares_source})")
        print(f"       ROE avg: {f'{roe_avg:.1%}' if roe_avg is not None else 'N/A (負債超過)'}")
        print(f"       Current Price: ${current_price:.2f}")
        print(f"       Revenue: ${revenue:,.0f}")
        print(f"       Beta: {final_beta:.2f} ({beta_source})")
        if rpo > 0:
            print(f"       RPO: ${rpo:,.0f}")

        # ========================================
        # インサイダー取引データ取得（SEC EDGAR Form 4）
        # ========================================
        insider_buy_count: Optional[int] = None
        insider_sell_count: Optional[int] = None
        insider_net_direction: Optional[str] = None
        insider_latest_date: Optional[str] = None
        try:
            import csv as _csv
            _cik_csv = os.path.join(repo_root, "config", "cik_lookup.csv") if repo_root else ""
            if _cik_csv and os.path.exists(_cik_csv):
                _cik = None
                with open(_cik_csv, encoding="utf-8", newline="") as _f:
                    for _row in _csv.DictReader(_f):
                        if _row.get("ticker", "").upper() == ticker.upper():
                            _cik = _row.get("cik", "")
                            break
                if _cik:
                    _insider = self.fetch_insider_trades(ticker, _cik)
                    if _insider is not None:
                        insider_buy_count  = _insider.get("buy_count")
                        insider_sell_count = _insider.get("sell_count")
                        insider_net_direction = _insider.get("net_direction")
                        insider_latest_date   = _insider.get("latest_date")
        except Exception as _ie:
            print(f"   [{ticker}] insider fetch error: {_ie}")

        return {
            "fcf_5yr_avg": fcf_avg,
            "fcf_2yr_avg": fcf_2yr_avg,
            "fcf_list_raw": fcf_list,
            # [[GROWTH-FCFSERIES-ACCESSOR-ADOPT-1]]: fcf_list_rawと対応する日付
            # （TTM経路はttm_end文字列、年次経路は会計年度int、未取得時はNone）。
            # growth.py側でFCF CAGR算出直前の順序再検証にのみ使う。JSONへは
            # 保存しない（fcf_list_rawと違い本番latest.json出力の対象外）。
            "fcf_dates_raw": fcf_dates,
            "net_cash_data": net_cash_data,
            "diluted_shares": final_shares,
            "roe_10yr_avg": roe_avg,
            "roe_years_used": roe_years_used,
            "roe_outlier_adj": roe_outlier_adj,
            "current_price": current_price,
            "latest_revenue": revenue,
            "rpo": rpo,
            "beta": final_beta,
            "beta_yf_raw": float(beta) if (beta is not None and beta > 0) else None,
            "sector": sector,
            "industry": industry,
            "per": per,
            "per_is_forward": per_is_forward,
            "peg": peg,
            "ps": ps,
            "ev_ebitda": ev_ebitda,
            "ma200": ma200,
            "forward_eps": forward_eps,
            "analyst_target_median": analyst_target_median,
            "analyst_target_mean": analyst_target_mean,
            "analyst_target_low": analyst_target_low,
            "analyst_target_high": analyst_target_high,
            "analyst_count": analyst_count,
            "analyst_rec_key": analyst_rec_key,
            "dividend_yield": dividend_yield,
            "payout_ratio": payout_ratio,
            "insider_buy_count": insider_buy_count,
            "insider_sell_count": insider_sell_count,
            "insider_net_direction": insider_net_direction,
            "insider_latest_date": insider_latest_date,
            "eps_data": {"ticker": ticker},
            "_shares_source": shares_source,
            "_beta_source": beta_source,
            "fcf_source": fcf_source,
            "fcf_ttm_end": fcf_ttm_end,
            "fcf_ttm_periods": fcf_ttm_periods,
            "rice_annual_data": rice_annual_data,
            "rice_data_source": rice_data_source,
            "rice_sector": self._get_rice_sector(ticker, sector),
        }

    def fetch_insider_trades(self, ticker: str, cik: str, days: int = 90) -> Optional[Dict[str, Any]]:
        """SEC EDGAR Form 4 から直近N日のインサイダー取引(P=買い/S=売り)を集計"""
        import os as _os
        import time
        import xml.etree.ElementTree as ET
        from datetime import datetime, timedelta
        try:
            import requests as _req
        except ImportError:
            return None

        headers = {'User-Agent': 'tanuki-valuation research@example.com'}
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        cik_num = cik.lstrip('0') or '0'
        cik_padded = cik_num.zfill(10)

        try:
            r = _req.get(
                f'https://data.sec.gov/submissions/CIK{cik_padded}.json',
                headers=headers, timeout=10
            )
            if r.status_code != 200:
                return None
            sub_data = r.json()
        except Exception:
            return None

        filings = sub_data.get('filings', {}).get('recent', {})
        forms = filings.get('form', [])
        dates = filings.get('filingDate', [])
        accns  = filings.get('accessionNumber', [])
        docs   = filings.get('primaryDocument', [])

        form4_recent = [
            (d, a, doc) for f, d, a, doc in zip(forms, dates, accns, docs)
            if f == '4' and d >= cutoff
        ]

        buy_count = 0
        sell_count = 0
        latest_date: Optional[str] = None

        for filing_date, accn, primary_doc in form4_recent:
            accn_nd = accn.replace('-', '')
            # BUG-INSIDER-1: ファイル名は filer ごとに異なる（form4.xml固定は誤り）。
            # primaryDocument はXSLビューア用パス（xslF345X06/...）を指すことがあり、
            # そのままGETするとHTML整形版が返るため、basenameのみアクセッション直下で取得する
            # （実機検証済み: PLTR/NVDA/TSLA等で生XMLが正しく取得できることを確認）
            fname = _os.path.basename(primary_doc)
            xml_url = (
                f'https://www.sec.gov/Archives/edgar/data/{cik_num}/{accn_nd}/{fname}'
            )
            try:
                time.sleep(0.1)
                r = _req.get(xml_url, headers=headers, timeout=10)
                if r.status_code != 200:
                    continue
                root = ET.fromstring(r.content)
                for txn in root.findall('.//nonDerivativeTransaction'):
                    code_el = txn.find('.//transactionCode')
                    code = code_el.text.strip() if code_el is not None and code_el.text else ''
                    if code == 'P':
                        buy_count += 1
                    elif code == 'S':
                        sell_count += 1
                if latest_date is None or filing_date > latest_date:
                    latest_date = filing_date
            except Exception:
                continue

        total = buy_count + sell_count
        if total == 0:
            net_direction = "中立"
        elif buy_count > sell_count:
            net_direction = "Buy優勢"
        elif sell_count > buy_count:
            net_direction = "Sell優勢"
        else:
            net_direction = "中立"

        print(f"   [{ticker}] insider trades (90d): buy={buy_count}, sell={sell_count} → {net_direction}")
        return {
            "buy_count": buy_count,
            "sell_count": sell_count,
            "net_direction": net_direction,
            "latest_date": latest_date,
        }

    def _get_rice_sector(self, ticker: str, yf_sector: str) -> str:
        """RICE除外判定用セクター取得。beta_config.jsonのrice_sectorを優先する。"""
        override = self._beta_overrides.get(ticker, {}).get("rice_sector", "")
        if override:
            print(f"   [{ticker}] RICE sector override: {yf_sector} → {override}")
            return override
        return yf_sector
    
    def _calc_fcf_2yr_avg(self, fcf_list: list) -> float:
        """
        FCFリストから直近2年平均を計算

        fcf_listはget_fcf_list()の返却値（新しい順、インデックス0が最新）。
        最新2件はリストの先頭[:2]で取得する。
        2件未満の場合は0.0を返す。
        """
        if not fcf_list or len(fcf_list) < 2:
            return 0.0
        # fcf_listは新しい順（fcf_list[0]が直近）→ 先頭2件が直近2年
        recent_2 = fcf_list[:2]
        if all(v is not None for v in recent_2):
            return sum(recent_2) / 2
        return 0.0

    def _determine_beta(
        self,
        ticker: str,
        yf_beta: float,
        sector: str
    ) -> Tuple[float, str]:
        """
        β（ベータ）を決定

        優先順位:
        1. beta_config.json の overrides（Koichi意図β）
        2. yfinanceのβ（0.1〜3.0の範囲内）
        3. セクター別デフォルトβ
        4. 全体デフォルト（1.0）

        [[BETA-FALLBACK-DESIGN-GAPS-1]]（2026-09-12調査）: 優先順位1
        （beta_config.jsonのoverride）はbeta値の範囲チェックを一切行わず
        無条件採用するため、下記2.の0.1〜3.0範囲チェックは「overrideが
        存在しない銘柄」でしか実際には通過（到達）しない。2026-09-12
        時点でoverridesは監視対象101銘柄全てに存在しており、この状態が
        続く限り2.のチェックは新規登録直後（beta_fetcher.py初回実行前）
        の銘柄以外には事実上到達不能。削除すべきデッドコードではない
        （新規銘柄登録時のフォールバックとして必要）が、「常に有効な
        安全網」ではなく「overrides未登録時限定の安全網」であることに
        注意すること。
        """
        # ── 1. beta_config.json オーバーライド（最優先） ──
        override = self._beta_overrides.get(ticker)
        if override and isinstance(override.get("beta"), (int, float)):
            beta_val = float(override["beta"])
            reason   = override.get("reason", "")
            sector_label = override.get("sector", "")
            print(f"   [{ticker}] → beta_config.json採用: β={beta_val:.2f} ({sector_label})")
            if reason:
                print(f"   [{ticker}]   理由: {reason}")
            return beta_val, f"beta_config({sector_label})"

        # ── 2. yfinanceのβ（有効範囲: 0.1〜3.0） ──
        if yf_beta is not None and 0.1 <= yf_beta <= 3.0:
            return float(yf_beta), "yfinance"

        # ── 3. セクター別デフォルト ──
        sector_beta = SECTOR_DEFAULT_BETA.get(sector)
        if sector_beta:
            print(f"   [{ticker}] → セクターデフォルトβ採用: {sector} = {sector_beta}")
            return float(sector_beta), f"sector_{sector}"

        # ── 4. 全体デフォルト ──
        default_beta = SECTOR_DEFAULT_BETA["default"]
        print(f"   [{ticker}] → デフォルトβ採用: {default_beta}")
        return float(default_beta), "default"
    
    def _determine_diluted_shares(
        self, 
        ticker: str,
        yf_implied: int, 
        yf_outstanding: int, 
        sec_diluted: int
    ) -> Tuple[int, str]:
        """完全希薄化後株式数を決定"""
        MIN_SHARES = 100_000
        
        if yf_implied > MIN_SHARES:
            print(f"   [{ticker}] → yfinance implied採用（完全希薄化後）")
            return int(yf_implied), "yf_implied"
        
        has_sec = sec_diluted > MIN_SHARES
        has_yf = yf_outstanding > MIN_SHARES
        
        if has_sec and has_yf:
            ratio = yf_outstanding / sec_diluted
            
            if ratio > 5:
                print(f"   [{ticker}] ⚠️ 大規模増資検出: yf={yf_outstanding:,.0f} vs SEC={sec_diluted:,.0f} (×{ratio:.1f})")
                print(f"   [{ticker}] → yfinance outstanding採用（増資後の現在値）")
                return int(yf_outstanding), "yf_outstanding_post_dilution"
            elif ratio < 0.2:
                print(f"   [{ticker}] → SEC diluted採用")
                return int(sec_diluted), "sec_diluted"
            else:
                max_shares = max(sec_diluted, yf_outstanding)
                source = "max_sec" if sec_diluted >= yf_outstanding else "max_yf"
                print(f"   [{ticker}] → max採用: {max_shares:,.0f} ({source})")
                return int(max_shares), source
        
        if has_yf:
            print(f"   [{ticker}] → yfinance outstanding採用")
            return int(yf_outstanding), "yf_outstanding"
        
        if has_sec:
            print(f"   [{ticker}] → SEC diluted採用")
            return int(sec_diluted), "sec_diluted"
        
        print(f"   [{ticker}] ⚠️ 株式数取得不可")
        return 0, "none"
