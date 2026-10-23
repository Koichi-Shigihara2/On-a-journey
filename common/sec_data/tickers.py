"""
common/sec_data/tickers.py
責務: config/cik_lookup.csv から銘柄リストを取得する共通ユーティリティ
     各サブシステムの --all オプションはこのモジュールを使う
"""

import csv
import os

_DEFAULT_CSV = os.path.join(
    os.path.dirname(__file__),  # common/sec_data/
    "..", "..",                  # リポジトリルート
    "config", "cik_lookup.csv"
)


def _load(csv_path: str | None = None) -> list[dict]:
    path = csv_path or os.path.abspath(_DEFAULT_CSV)
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def get_all_tickers(csv_path: str | None = None) -> list[str]:
    """cik_lookup.csv の全銘柄を返す"""
    return [r["ticker"] for r in _load(csv_path)]


def get_all_rows(csv_path: str | None = None) -> list[dict]:
    """cik_lookup.csv の全行を辞書のリストで返す（status/registered_date/
    registration_source等、ticker以外の列も含む）。

    銘柄フラグ・statusによる絞り込みだけでは足りず、複数列を横断参照する
    必要がある消費者（例: system_health.py::check_k_ticker_audit()の
    棚卸しレポート）向けに新設した（[[QUALITY-GATES-EPIC-1]]ゲート4、
    2026-09-05）。cik_lookup.csvを独自にcsv.DictReaderで再パースする
    実装が新たに生まれることを防ぐため、既存の`_load()`をそのまま公開する。
    """
    return _load(csv_path)


def get_cik(ticker: str, csv_path: str | None = None) -> str | None:
    """指定ティッカーのCIKを10桁ゼロ埋め形式で返す（見つからなければNone）。

    SEC submissions API（`https://data.sec.gov/submissions/CIK{10桁}.json`）
    は10桁ゼロ埋めのCIKを要求する。cik_lookup.csvのcik列は基本10桁ゼロ埋めで
    登録されているが、CRWV等一部の行が未パディング（例:"1769628"）のまま
    登録されているため、読み込み側で常に`.zfill(10)`正規化して返す
    （[[TICKER-LOADING-UNIFICATION-1]]、旧`src/tail/kpi_proposer.py::
    load_cik()`の修正〈2026-08-19⑦、CRWV未パディング起因の404回帰〉と
    同じ正規化をここに集約）。

    `src/tail/kpi_proposer.py`・`sec_ctrl_fetcher.py`・
    `text_kpi_extractor.py`がそれぞれ独立実装していた同種の
    ticker→CIK単発検索を統合するための共有関数（ticker/CIKの
    大文字小文字・前後空白は無視する）。
    """
    ticker_upper = ticker.strip().upper()
    for r in _load(csv_path):
        if r.get("ticker", "").strip().upper() == ticker_upper:
            cik = r.get("cik", "").strip()
            return cik.zfill(10) if cik else None
    return None


def get_tickers_by_flag(flag: str, csv_path: str | None = None) -> list[str]:
    """
    指定フラグが 'true' の銘柄リストを返す（statusは見ない）。

    flag: 'hypecore' | 'tanuki' | 'eps' | 'stonks_silo'
    """
    return [
        r["ticker"] for r in _load(csv_path)
        if r.get(flag, "").strip().lower() == "true"
    ]


# statusのうち、フラグの値に関わらず対象外とすべき値。
# 'retired'（登録抹消済み）に加え、2026-09-03より'provisioning'
# （登録処理中・Step 8のNG=0確認前）も除外する（[[REGISTER-FLOW-
# REDESIGN-1]]方針2、common/registration/register_ticker.py参照）。
# 'dormant'（休眠: 人が見る印で、各パイプラインには通す。2026-10-23に discover_config.json の区分「休眠」から移した、
# [[HOLDINGS-SINGLE-SOURCE-1]]）も除外しない。
# 'candidate'（検証中だが各パイプラインには通す運用、WST/CON等）は
# 現状の既存動作を維持するため除外しない（statusによるパイプライン
# 対象外化は現状candidateには適用されていない。フラグ判定基準
# そのものの厳密化は別タスクとする）。
_INVALID_STATUSES = {"retired", "provisioning"}


def get_active_tickers(flag: str, csv_path: str | None = None) -> list[str]:
    """
    指定フラグが'true'、かつstatusが有効（'retired'でない）銘柄リストを返す。

    銘柄リストを組み立てる全ての消費者はcik_lookup.csv・config.get_all()を
    直接参照するのではなく、本関数を経由することで、フラグ判定ロジックを
    一元化する（tanuki=falseのZSがtickers.json等に混入し続けていた問題の
    再発防止。TICKER-SOURCE-UNIFY-1の延長）。

    flag: 'hypecore' | 'tanuki' | 'eps' | 'stonks_silo'
    """
    return [
        r["ticker"] for r in _load(csv_path)
        if r.get(flag, "").strip().lower() == "true"
        and r.get("status", "").strip().lower() not in _INVALID_STATUSES
    ]


def get_hypecore_tickers(csv_path: str | None = None) -> list[str]:
    """hypecore=true（かつstatus有効）の銘柄リストを返す"""
    return get_active_tickers("hypecore", csv_path)


def get_tanuki_tickers(csv_path: str | None = None) -> list[str]:
    """tanuki=true（かつstatus有効）の銘柄リストを返す"""
    return get_active_tickers("tanuki", csv_path)


def get_eps_tickers(csv_path: str | None = None) -> list[str]:
    """eps=true（かつstatus有効）の銘柄リストを返す"""
    return get_active_tickers("eps", csv_path)


def get_stonks_silo_tickers(csv_path: str | None = None) -> list[str]:
    """stonks_silo=true（かつstatus有効）の銘柄リストを返す"""
    return get_active_tickers("stonks_silo", csv_path)


def get_registrable_tickers(flag: str | None = None, csv_path: str | None = None) -> list[str]:
    """
    指定フラグが'true'、かつstatusが'retired'でない銘柄リストを返す
    （'provisioning'は除外しない、`get_active_tickers()`とはこの点のみ異なる）。

    **用途**: 各パイプラインの「CLI引数でticker明示指定時」の対象妥当性
    検証（ZS-TICKERS-LEAK-1由来の`_filter_*_tickers()`群）専用。
    新規銘柄登録オーケストレーション（[[REGISTER-FLOW-REDESIGN-1]]方針3、
    `common/registration/register_ticker.py`）が、status=provisioning
    のティッカーに対してStep 3（`pipeline.py TICKER`）・Step 5
    （`hypecore.py --batch TICKER`）・Step 5b（EPS Analyzer
    `--ticker TICKER`）を明示的に実行できる必要があるため新設した
    （2026-09-03）。

    **`get_active_tickers()`との使い分け**:
    - デフォルト・バッチ実行（`tickers=None`、`--all`等）の対象選定は
      引き続き`get_active_tickers()`（provisioning除外）を使う——
      registration_validator.pyのStep 8でNG=0が確認され`active`/
      `candidate`へ昇格するまでは、スケジュール実行等の自動対象には
      含めない
    - CLI引数でticker明示指定時の「範囲外ではないか」検証には本関数を
      使う——provisioningは「意図的な対象外」ではなく「登録処理中」
      であり、登録オーケストレーション自身が明示的に指定した場合は
      処理を許可すべきため。'retired'（登録抹消済み）は明示指定でも
      引き続き除外する（意図的な対象外のため、ZS-TICKERS-LEAK-1が
      防いだ種類のリークと同型のリスクを再導入しないため）

    flag: 'hypecore' | 'tanuki' | 'eps' | 'stonks_silo'。**None**の場合は
    フラグによる絞り込みを行わず、status='retired'以外の全銘柄を返す
    （`common/sec_data/config.py::get_all()`統合用、
    [[TICKER-LOADING-UNIFICATION-1]]、2026-09-05追加）。
    `common/sec_data/update.py`（SEC EDGAR生データ取得、Step 1）は
    tanuki/eps/hypecore/stonks_siloいずれのパイプラインフラグにも
    依存しない共有インフラ層であり、特定フラグで絞り込むと
    「そのフラグだけfalseの銘柄」がSEC生データ取得自体から漏れてしまう
    （2026-09-05時点の本番データではhypecore=trueが全銘柄で一致するため
    単一フラグ指定でも偶然結果が一致するが、将来hypecore=falseの銘柄が
    登録されると静かに破綻する設計のため、flag=Noneの専用モードを設けた）。
    """
    return [
        r["ticker"] for r in _load(csv_path)
        if (flag is None or r.get(flag, "").strip().lower() == "true")
        and r.get("status", "").strip().lower() != "retired"
    ]
