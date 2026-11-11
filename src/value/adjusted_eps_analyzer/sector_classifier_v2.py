"""
セクター分類モジュール（VER2）
- 区分は業種のマスター（config/sector_master.json の eps_category。industry主・sector、金融でない Financial Services は Technology）から決める
  （[[SECTOR-MASTER-ADOPT-1]] 段4、2026-11-11。以前は eps_sector の手設定 > SICコード > 会社名のキーワード > ティッカーのキーワード）
- セクター別デフォルト除外項目（config/sectors.yaml の exclusions）の取得
"""
import yaml
from typing import Optional, List, Dict


class SectorClassifierV2:
    """VER2基準に基づく業種分類（業種のマスターの写像）"""

    def __init__(self, config_path: str):
        """
        Args:
            config_path: sectors.yaml のパス（区分の name と、区分別の除外項目 exclusions だけを持つ）
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.sectors = yaml.safe_load(f)['sectors']

    def get_exclusions_for_sector(self, sector: str) -> List[Dict]:
        """セクターのデフォルト除外項目を取得"""
        for s in self.sectors:
            if s['name'] == sector:
                return s.get('exclusions', [])
        return []

    def get_maturity_watch_items(self, sector: str) -> List[str]:
        """成熟度監視対象の項目IDを取得（SBCなど）"""
        watch_items = []
        for s in self.sectors:
            if s['name'] == sector:
                for ex in s.get('exclusions', []):
                    if ex.get('maturity_watch', False):
                        watch_items.append(ex['item_id'])
        return watch_items

    def classify(self, ticker: str) -> Optional[str]:
        """業種のマスターの eps_category。マスターに無い銘柄（登録の直後）は、手元の属性・SICから同じ規則で作る。決まらなければ None"""
        if not ticker:
            return None
        from common import sector_master as sm
        entry = sm.master_entry(ticker)
        if entry is None:
            try:
                entry = sm.build_entry(ticker)
            except Exception:
                return None
        return entry.get("eps_category")

    def get_all_sectors(self) -> List[str]:
        """全セクター名のリストを取得（UI用）"""
        return [s['name'] for s in self.sectors]
