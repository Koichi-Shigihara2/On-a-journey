"""
SEC EDGAR データ取得モジュール
Company Facts API を使用してBS/PL/CFデータを取得・保存
"""

import requests
import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, Any

from .config import get_all, get_ticker_info

_CIK_HISTORY_PATH = os.path.join(os.path.dirname(__file__), "cik_history.json")


def _load_cik_history() -> dict:
    """cik_history.json（法人再編でCIKが切り替わった銘柄の旧CIK一覧）を読み込む。

    CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1: 同一事業継続型の法人再編
    （MRVL/GOOGL/AVGO/DELL）のみ、旧CIKのCompany Facts/submissionsを
    追加取得してマージする対象として登録される。ファイル不在時は空dict。
    """
    if os.path.exists(_CIK_HISTORY_PATH):
        try:
            with open(_CIK_HISTORY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


class SECFetcher:
    """SEC EDGAR Company Facts API クライアント"""

    BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
    CIK_LOOKUP_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

    # SEC APIは10リクエスト/秒制限
    RATE_LIMIT_DELAY = 0.15
    
    def __init__(self, data_dir: str = None):
        if data_dir:
            self.data_dir = data_dir
        else:
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        os.makedirs(self.data_dir, exist_ok=True)
        
        # CIKキャッシュ
        self.cik_cache_path = os.path.join(self.data_dir, "_cik_cache.json")
        self.cik_cache = self._load_cik_cache()
        
        # User-Agent必須（SEC要件）
        self.headers = {
            "User-Agent": "Koichi Personal Investment Tools koichi@example.com",
            "Accept": "application/json"
        }
        # SECへのリクエスト数（update.pyが実行の最後に表示する。[[SEC-FETCH-CACHE-MTIME-CI-1]]）
        self.request_count = 0

    def _get(self, url: str, timeout: int):
        self.request_count += 1
        return requests.get(url, headers=self.headers, timeout=timeout)

    def _load_cik_cache(self) -> dict:
        """CIKキャッシュ読み込み"""
        if os.path.exists(self.cik_cache_path):
            try:
                with open(self.cik_cache_path, "r") as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def _save_cik_cache(self):
        """CIKキャッシュ保存"""
        with open(self.cik_cache_path, "w") as f:
            json.dump(self.cik_cache, f, indent=2)
    
    def get_cik(self, ticker: str) -> Optional[str]:
        """ティッカーからCIKを取得（10桁ゼロパディング）"""
        ticker = ticker.upper()
        
        # キャッシュ確認
        if ticker in self.cik_cache:
            return self.cik_cache[ticker]
        
        # SEC ticker.json から取得
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            resp = self._get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for entry in data.values():
                    if entry.get("ticker", "").upper() == ticker:
                        cik = str(entry["cik_str"]).zfill(10)
                        self.cik_cache[ticker] = cik
                        self._save_cik_cache()
                        return cik
        except Exception as e:
            print(f"   [CIK ERROR] {ticker}: {e}")
        
        return None
    
    def fetch_company_facts(self, ticker: str, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """
        Company Facts API から全データ取得
        
        Returns:
            dict: SEC Company Facts 生データ
        """
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        
        raw_path = os.path.join(ticker_dir, "company_facts.json")
        
        # キャッシュ確認（24時間有効）
        if not force_refresh and os.path.exists(raw_path):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(raw_path))).total_seconds()
            if age < 86400:
                try:
                    with open(raw_path, "r", encoding="utf-8") as f:
                        print(f"   [{ticker}] キャッシュから読み込み")
                        return json.load(f)
                except:
                    pass
        
        # CIK取得
        cik = self.get_cik(ticker)
        if not cik:
            print(f"   [{ticker}] CIK取得失敗")
            return None
        
        # Company Facts API呼び出し
        url = f"{self.BASE_URL}/CIK{cik}.json"
        print(f"   [{ticker}] SEC API取得中... (CIK: {cik})")

        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            resp = self._get(url, timeout=30)

            if resp.status_code == 200:
                data = resp.json()

                # CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1: 法人再編で同一事業が
                # 旧CIKに継続していた銘柄は、旧CIKのus-gaap factsを追加取得して
                # マージする。マージ専用の優先順位ロジックは追加しない
                # （parser.pyの本人データ優先・タイブレークにそのまま委ねる）。
                legacy_ciks = _load_cik_history().get(ticker, {}).get("legacy_ciks", [])
                for legacy_cik in legacy_ciks:
                    legacy_data = self._fetch_legacy_company_facts(ticker, legacy_cik, force_refresh)
                    if legacy_data:
                        data = self._merge_us_gaap_facts(data, legacy_data)

                # 保存
                with open(raw_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                print(f"   [{ticker}] SEC API取得完了")
                return data
            else:
                print(f"   [{ticker}] SEC API エラー: {resp.status_code}")
                return None

        except Exception as e:
            print(f"   [{ticker}] SEC API 例外: {e}")
            return None

    def _fetch_legacy_company_facts(self, ticker: str, legacy_cik: str, force_refresh: bool) -> Optional[Dict[str, Any]]:
        """cik_history.json記載の旧CIKのCompany Factsを取得（24時間キャッシュ）。

        {ticker}/company_facts.json とは別に {ticker}/company_facts_legacy_{cik}.json
        として保存する（監査・再検証用に旧CIK単独のスナップショットを残す）。
        """
        legacy_cik = str(legacy_cik).zfill(10)
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        legacy_path = os.path.join(ticker_dir, f"company_facts_legacy_{legacy_cik}.json")

        if not force_refresh and os.path.exists(legacy_path):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(legacy_path))).total_seconds()
            if age < 86400:
                try:
                    with open(legacy_path, "r", encoding="utf-8") as f:
                        return json.load(f)
                except Exception:
                    pass

        url = f"{self.BASE_URL}/CIK{legacy_cik}.json"
        print(f"   [{ticker}] 旧CIK({legacy_cik})取得中...")
        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            resp = self._get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                with open(legacy_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                return data
            print(f"   [{ticker}] 旧CIK({legacy_cik}) SEC API エラー: {resp.status_code}")
        except Exception as e:
            print(f"   [{ticker}] 旧CIK({legacy_cik}) SEC API 例外: {e}")
        # 取得に失敗したら手元の旧CIKのファイルを使う（旧CIKは提出が止まっており内容は
        # 変わらない。旧CIK分の欠けたcompany_facts.jsonで上書きしないため）
        return self._read_json_or_none(legacy_path)

    @staticmethod
    def _read_json_or_none(path: str) -> Optional[Dict[str, Any]]:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"   手元のファイルを使う: {os.path.basename(path)}")
            return data
        except Exception:
            return None

    @staticmethod
    def _merge_us_gaap_facts(primary: Dict[str, Any], legacy: Dict[str, Any]) -> Dict[str, Any]:
        """primaryのfacts.us-gaapに、legacyのfacts.us-gaapエントリを統合する。

        同一conceptは units[unit_type] のentryリストをそのまま連結するのみで、
        マージ専用の優先順位判定・重複排除は行わない。年度ごとの採用値決定は
        既存のparser.py（本人データ優先・filed日時タイブレーク）にそのまま
        委ねる設計（CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1調査で確認済みの前提）。
        """
        merged = json.loads(json.dumps(primary))
        legacy_us_gaap = legacy.get("facts", {}).get("us-gaap", {})
        merged_us_gaap = merged.setdefault("facts", {}).setdefault("us-gaap", {})
        for concept, concept_data in legacy_us_gaap.items():
            if concept not in merged_us_gaap:
                merged_us_gaap[concept] = concept_data
                continue
            merged_units = merged_us_gaap[concept].setdefault("units", {})
            for unit_type, entries in concept_data.get("units", {}).items():
                merged_units.setdefault(unit_type, [])
                merged_units[unit_type].extend(entries)
        return merged
    
    def fetch_submissions(self, ticker: str, force_refresh: bool = False, save: bool = True,
                          return_payload: bool = False) -> Optional[Dict[str, Any]]:
        """
        submissions API（recent + 必要な過去分アーカイブ）から
        10-K/10-K/A/10-Q/10-Q/A の accession Number -> reportDate マッピングを取得・保存する。

        FY52WEEK-BUCKET-MISPLACE-1調査で判明した「fyタグはfiling単位の粗いラベルで
        比較年度再掲時に書き換わる」問題への対応。reportDate==end_dateが成立する
        factのみが「本人の当期データ」であることが全106銘柄・年次830件超・四半期859件で
        例外なく確認できている（parser.pyの本人データ判定ロジックが参照する）。

        [[SEC-FETCH-CACHE-MTIME-CI-1]]（2026-10-10）: 同じ書類の accn -> filingDate
        （accn_to_filingdate）も保存する（update.pyがcompany_factsを取り直すかの判定に使う）。
        save=Falseはファイルを書かない（update.py --dry-run）。return_payload=Trueは
        保存する内容（dict）全体を返す。
        取得が一部でも失敗した（現CIKのアーカイブ・旧CIKが取れず、手元のファイルでも
        補えない）ときは、手元にsubmissions.jsonがあれば上書きせずNoneを返す
        （旧CIK分・古い書類の欠けたマッピングで本人データ判定が変わらないようにするため）。
        手元に無い（新規登録）ときは、取れた分を従来どおり保存する。

        Returns:
            dict: {accn: reportDate} のマッピング（return_payload=Trueなら保存内容全体。取得失敗時はNone）
        """
        ticker = ticker.upper()
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)

        out_path = os.path.join(ticker_dir, "submissions.json")

        # キャッシュ確認（24時間有効。10-K/10-Q提出時のみ更新されれば十分なため
        # company_facts.jsonと同じ頻度でよい）
        if not force_refresh and os.path.exists(out_path):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(out_path))).total_seconds()
            if age < 86400:
                try:
                    with open(out_path, "r", encoding="utf-8") as f:
                        payload = json.load(f)
                    return payload if return_payload else payload.get("accn_to_reportdate", {})
                except Exception:
                    pass

        cik = self.get_cik(ticker)
        if not cik:
            print(f"   [{ticker}] submissions: CIK取得失敗")
            return None

        # [[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2: 現CIKのformerNames
        # （法人名変更履歴）を同一レスポンスから追加取得する。旧CIK
        # （legacy_ciks、CIK自体が切り替わったケース）側は対象外
        # （本機能はCIK不変のまま社名のみ変わったSPAC合併検知が目的のため）。
        former_names: list = []
        errors: list = []
        accn_to_filingdate: Dict[str, str] = {}
        accn_to_reportdate = self._fetch_submissions_for_cik(
            ticker, cik, former_names_out=former_names,
            filingdate_out=accn_to_filingdate, errors_out=errors)
        if accn_to_reportdate is None:
            return None

        # CIK-DISCONTINUITY-OLDEST-YEAR-GAP-1: 旧CIKのsubmissionsもマージする。
        # parser.pyの本人データ判定（accn_reportdate.get(accn)==end_date）は
        # このマッピングにaccnが含まれていないと機能しないため、company_facts.json
        # だけでなくsubmissions.json側も旧CIK分を統合する必要がある。
        legacy_ciks = _load_cik_history().get(ticker, {}).get("legacy_ciks", [])
        for legacy_cik in legacy_ciks:
            legacy = self._fetch_legacy_submissions(ticker, legacy_cik, force_refresh, save=save)
            if legacy is None:
                errors.append(f"legacy {legacy_cik}")
                continue
            legacy_rd, legacy_fd = legacy
            accn_to_reportdate.update(legacy_rd)
            accn_to_filingdate.update(legacy_fd)

        if errors and os.path.exists(out_path):
            print(f"   [{ticker}] submissions: 一部を取得できない（{', '.join(errors)}）→ 手元のsubmissions.jsonを残す")
            return None

        payload = {"ticker": ticker, "cik": cik, "legacy_ciks_merged": legacy_ciks,
                   "accn_to_reportdate": accn_to_reportdate,
                   "former_names": former_names,
                   "accn_to_filingdate": accn_to_filingdate}
        if save:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

        print(f"   [{ticker}] submissions取得完了 ({len(accn_to_reportdate)}件)")
        return payload if return_payload else accn_to_reportdate

    def _fetch_submissions_for_cik(self, ticker: str, cik: str,
                                    former_names_out: Optional[list] = None,
                                    filingdate_out: Optional[Dict[str, str]] = None,
                                    errors_out: Optional[list] = None) -> Optional[Dict[str, str]]:
        """指定CIKのsubmissions API（recent + 過去分アーカイブ）から
        {accn: reportDate} マッピングを取得する（キャッシュ・保存は行わない）。

        former_names_out: 指定した場合、メインレスポンス（recent側、archive側には
        含まれない）のformerNames配列（[{name, from, to}, ...]）を追記する
        （[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2: SPAC合併疑いの機械的検知に使用。
        レスポンス自体は既に全量取得済みのため追加APIコールは発生しない）。
        filingdate_out: 指定した場合、同じ書類の {accn: filingDate} を追記する。
        errors_out: 指定した場合、取得できなかったアーカイブの名前を追記する
        （アーカイブの失敗は従来どおり飛ばして続ける）。
        """
        relevant_forms = {"10-K", "10-K/A", "10-Q", "10-Q/A"}
        accn_to_reportdate: Dict[str, str] = {}

        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        try:
            time.sleep(self.RATE_LIMIT_DELAY)
            resp = self._get(url, timeout=30)
        except Exception as e:
            print(f"   [{ticker}] submissions({cik}) API 例外: {e}")
            return None

        if resp.status_code != 200:
            print(f"   [{ticker}] submissions({cik}) API エラー: {resp.status_code}")
            return None

        data = resp.json()
        if former_names_out is not None:
            former_names_out.extend(data.get("formerNames", []))
        self._extract_accn_reportdate(data.get("filings", {}).get("recent", {}), relevant_forms,
                                      accn_to_reportdate, filingdate_out)

        # 過去分アーカイブ（filings.recentは直近分のみのため、古いfilingは別JSONに分割）
        for finfo in data.get("filings", {}).get("files", []):
            fname = finfo.get("name")
            if not fname:
                continue
            archive_url = f"https://data.sec.gov/submissions/{fname}"
            try:
                time.sleep(self.RATE_LIMIT_DELAY)
                aresp = self._get(archive_url, timeout=30)
            except Exception as e:
                print(f"   [{ticker}] submissions({cik}) archive({fname}) 例外: {e}")
                if errors_out is not None:
                    errors_out.append(fname)
                continue
            if aresp.status_code != 200:
                print(f"   [{ticker}] submissions({cik}) archive({fname}) エラー: {aresp.status_code}")
                if errors_out is not None:
                    errors_out.append(fname)
                continue
            self._extract_accn_reportdate(aresp.json(), relevant_forms, accn_to_reportdate, filingdate_out)

        return accn_to_reportdate

    @staticmethod
    def _extract_accn_reportdate(recent_or_archive: dict, relevant_forms: set, out: Dict[str, str],
                                 filingdate_out: Optional[Dict[str, str]] = None) -> None:
        forms = recent_or_archive.get("form", [])
        accns = recent_or_archive.get("accessionNumber", [])
        report_dates = recent_or_archive.get("reportDate", [])
        for form, accn, rd in zip(forms, accns, report_dates):
            if form in relevant_forms and rd:
                out[accn] = rd
        if filingdate_out is not None:
            filing_dates = recent_or_archive.get("filingDate", [])
            for form, accn, fd in zip(forms, accns, filing_dates):
                if form in relevant_forms and fd:
                    filingdate_out[accn] = fd

    def _fetch_legacy_submissions(self, ticker: str, legacy_cik: str, force_refresh: bool,
                                  save: bool = True) -> Optional[tuple]:
        """cik_history.json記載の旧CIKのsubmissionsを取得（24時間キャッシュ）。

        {ticker}/submissions.json とは別に {ticker}/submissions_legacy_{cik}.json
        として保存する（監査・再検証用に旧CIK単独のスナップショットを残す）。
        取得に失敗したら手元の旧CIKのファイルを使う（旧CIKは提出が止まっており内容は変わらない）。

        Returns:
            ({accn: reportDate}, {accn: filingDate})。取得できず手元のファイルも無ければNone
        """
        legacy_cik = str(legacy_cik).zfill(10)
        ticker_dir = os.path.join(self.data_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        legacy_path = os.path.join(ticker_dir, f"submissions_legacy_{legacy_cik}.json")

        def _local():
            data = self._read_json_or_none(legacy_path)
            if data is None:
                return None
            return data.get("accn_to_reportdate", {}), data.get("accn_to_filingdate", {})

        if not force_refresh and os.path.exists(legacy_path):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(legacy_path))).total_seconds()
            if age < 86400:
                local = _local()
                if local is not None:
                    return local

        errors: list = []
        accn_to_filingdate: Dict[str, str] = {}
        accn_to_reportdate = self._fetch_submissions_for_cik(
            ticker, legacy_cik, filingdate_out=accn_to_filingdate, errors_out=errors)
        if accn_to_reportdate is None or errors:
            print(f"   [{ticker}] 旧CIK({legacy_cik}) submissionsを取得できない")
            return _local()

        if save:
            with open(legacy_path, "w", encoding="utf-8") as f:
                json.dump({"ticker": ticker, "cik": legacy_cik, "accn_to_reportdate": accn_to_reportdate,
                           "accn_to_filingdate": accn_to_filingdate},
                          f, ensure_ascii=False, indent=2)

        print(f"   [{ticker}] 旧CIK({legacy_cik}) submissions取得完了 ({len(accn_to_reportdate)}件)")
        return accn_to_reportdate, accn_to_filingdate

    def fetch_all(self, tickers: list = None, force_refresh: bool = False) -> Dict[str, bool]:
        """
        複数ティッカーの一括取得
        
        Returns:
            dict: {ticker: success_flag}
        """
        if tickers is None:
            tickers = get_all()
        
        results = {}
        total = len(tickers)
        
        print(f"\n{'='*60}")
        print(f"SEC EDGAR データ一括取得開始（{total}銘柄）")
        print(f"{'='*60}")
        
        for i, ticker in enumerate(tickers, 1):
            print(f"\n[{i}/{total}] {ticker}")
            info = get_ticker_info(ticker)
            print(f"   {info['name']} ({info['status']})")
            
            data = self.fetch_company_facts(ticker, force_refresh)
            results[ticker] = data is not None
        
        # サマリー
        success = sum(1 for v in results.values() if v)
        print(f"\n{'='*60}")
        print(f"取得完了: {success}/{total}")
        print(f"{'='*60}")
        
        return results


def load_company_facts(ticker: str, data_dir: str = None) -> Optional[Dict[str, Any]]:
    """
    {data_dir}/{ticker}/company_facts.json を読み込んで返す。

    ファイルが存在しない場合は None を返す（ネットワーク取得は行わない）。
    取得が必要な場合は SECFetcher.fetch_company_facts() を使うこと。
    """
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(__file__), "data")
    path = os.path.join(data_dir, ticker.upper(), "company_facts.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"   [{ticker}] company_facts.json 読み込みエラー: {e}")
        return None


def load_submissions(ticker: str, data_dir: str = None) -> Dict[str, str]:
    """
    {data_dir}/{ticker}/submissions.json を読み込んで accn_to_reportdate を返す。

    ファイルが存在しない場合は空dictを返す（ネットワーク取得は行わない）。
    取得が必要な場合は SECFetcher.fetch_submissions() を使うこと。
    """
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(__file__), "data")
    path = os.path.join(data_dir, ticker.upper(), "submissions.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("accn_to_reportdate", {})
    except Exception as e:
        print(f"   [{ticker}] submissions.json 読み込みエラー: {e}")
        return {}


def load_former_names(ticker: str, data_dir: str = None) -> list:
    """
    {data_dir}/{ticker}/submissions.json を読み込んでformer_names
    （[{name, from, to}, ...]）を返す（[[SPAC-SHELL-BS-ENTITY-MIXING-1]]段階2）。

    ファイルが存在しない場合・キー自体が存在しない場合（fetch_submissions()の
    former_names追加より前に取得されたsubmissions.json）は空リストを返す
    （ネットワーク取得は行わない）。
    """
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(__file__), "data")
    path = os.path.join(data_dir, ticker.upper(), "submissions.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f).get("former_names", [])
    except Exception as e:
        print(f"   [{ticker}] submissions.json 読み込みエラー: {e}")
        return []


def _get_latest_accn(company_facts: dict) -> Optional[str]:
    """company_facts 内で最も最近の accn（filingアクセッション番号）を返す"""
    latest_filed = ""
    latest_accn = None
    try:
        for concept_data in company_facts.get("facts", {}).get("us-gaap", {}).values():
            for unit_data in concept_data.get("units", {}).values():
                for entry in unit_data:
                    filed = entry.get("filed", "")
                    if filed > latest_filed:
                        latest_filed = filed
                        latest_accn = entry.get("accn")
    except Exception:
        pass
    return latest_accn


def needs_quarterly_update(ticker: str, company_facts: dict, run_state: dict) -> bool:
    """
    company_facts の最新 filing の accn を run_state と比較。
    差分がある（または未記録）場合のみ True を返す。
    """
    latest_accn = _get_latest_accn(company_facts)
    if latest_accn is None:
        return False
    saved_accn = run_state.get(ticker.upper(), {}).get("quarterly", {}).get("last_accn")
    return latest_accn != saved_accn


def update_quarterly_run_state(ticker: str, latest_accn: str, run_state: dict) -> dict:
    """run_state の quarterly セクションを更新して返す"""
    ticker = ticker.upper()
    updated = dict(run_state)
    updated.setdefault(ticker, {})["quarterly"] = {
        "last_fetched": datetime.now().strftime("%Y-%m-%d"),
        "last_accn": latest_accn,
    }
    return updated


if __name__ == "__main__":
    fetcher = SECFetcher()

    # テスト: 単一ティッカー
    data = fetcher.fetch_company_facts("TSLA")
    if data:
        print(f"\nTSLA facts keys: {list(data.get('facts', {}).keys())}")
