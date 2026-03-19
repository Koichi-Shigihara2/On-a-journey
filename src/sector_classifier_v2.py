"""
セクター分類モジュール（VER2）
- SICコードとキーワードによる業種判定
- セクター別デフォルト除外項目の取得
"""
import yaml
import os
import re
from typing import Optional, List, Dict

class SectorClassifierV2:
    """VER2基準に基づく業種分類（SICコード対応）"""
    
    def __init__(self, config_path: str):
        """
        Args:
            config_path: sectors.yaml のパス
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.sectors = yaml.safe_load(f)['sectors']
        
        # キーワード検索用に正規表現パターンを事前コンパイル
        self.patterns = {}
        for sector in self.sectors:
            keywords = '|'.join(re.escape(kw) for kw in sector['keywords'])
            self.patterns[sector['name']] = re.compile(keywords, re.IGNORECASE)
    
    def classify_by_sic(self, sic_code: str) -> Optional[str]:
        """SICコードで業種判定（最も正確）"""
        if not sic_code:
            return None
        
        for sector in self.sectors:
            if 'sic_codes' in sector and sic_code in sector['sic_codes']:
                return sector['name']
        return None
    
    def classify_by_keywords(self, text: str) -> Optional[str]:
        """キーワードで業種判定（フォールバック）"""
        if not text:
            return None
        
        for sector_name, pattern in self.patterns.items():
            if pattern.search(text):
                return sector_name
        return None
    
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
    
    def get_all_sectors(self) -> List[str]:
        """全セクター名のリストを取得（UI用）"""
        return [s['name'] for s in self.sectors]
