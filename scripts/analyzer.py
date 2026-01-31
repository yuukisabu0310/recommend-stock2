"""
財務データの正規化と分析ロジック
data/raw/*.jsonからデータを読み込み、名寄せを行って分析用データを作成
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import glob
import json
import re
import sys

# ルートディレクトリをパスに追加（constants.pyをインポートするため）
sys.path.insert(0, str(Path(__file__).parent.parent))

import constants


# ============================================
# マッピング辞書の定義（優先順位順）
# ============================================
MAPPING_DICT = {
    'Revenue': [
        'Total Revenue',  # 優先度1
        'Operating Revenue',  # 優先度2
        'Interest Income',  # 銀行業対応
        '売上高',
        '営業収益',
        'Revenue',
        'Net Sales',
    ],
    'OperatingIncome': [
        'Operating Income',  # 優先度1
        'Operating Profit',  # 優先度2
        'Pretax Income',  # 営業利益が取れない場合の予備
        '営業利益',
        'Operating Income/Loss',
    ],
    'NetIncome': [
        'Net Income Common Stockholders',  # 優先度1
        'Net Income',  # 優先度2
        'Net Income Including Noncontrolling Interests',
        '純利益',
        '当期純利益',
        'Net Income/Loss',
    ],
    'TotalAssets': [
        'Total Assets',
        '総資産',
        'Assets',
    ],
    'TotalDebt': [
        'Total Debt',  # 優先度1（Invested Capital計算用）
        'Long Term Debt + Short Term Debt',
        '有利子負債',
    ],
    'Equity': [
        'Stockholders Equity',  # Invested Capital計算用
        'Total Stockholders Equity',
        'Equity',
        '自己資本',
        '純資産',
        'Net Assets',
    ],
    'ShortTermDebt': [
        'Short Term Debt',
        'Current Debt',  # Invested Capital計算用
        '短期借入金',
    ],
    'LongTermDebt': [
        'Long Term Debt',  # Invested Capital計算用
        '長期借入金',
        'Non Current Debt',
    ],
    'Cash': [
        'Cash And Cash Equivalents',
        'Cash Cash Equivalents And Short Term Investments',
        'Cash And Short Term Investments',
        '現金及び預金',
        '現預金',
        'Cash',
    ],
}


# ============================================
# ログ設定
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================
# データ正規化クラス
# ============================================
class FinancialDataAnalyzer:
    """財務データの正規化と分析クラス"""
    
    def __init__(self, raw_data_dir: str = "data/raw", processed_data_dir: str = "data/processed"):
        """
        初期化
        
        Args:
            raw_data_dir: 生データディレクトリ
            processed_data_dir: 処理済みデータディレクトリ
        """
        self.raw_data_dir = Path(raw_data_dir)
        self.processed_data_dir = Path(processed_data_dir)
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
        # 税率（一律30%）
        self.tax_rate = 0.3
        
        logger.info(f"FinancialDataAnalyzer初期化完了")
    
    def _find_mapping_value(self, df: pd.DataFrame, mapping_keys: List[str], priority_order: bool = True) -> Optional[pd.Series]:
        """
        マッピング辞書に基づいて値を検索（優先順位対応）
        
        Args:
            df: DataFrame（インデックスに項目名が入っている）
            mapping_keys: 検索するキーのリスト（優先順位順）
            priority_order: Trueの場合、リストの順序を優先順位として扱う
            
        Returns:
            見つかったSeries（見つからない場合はNone）
        """
        index_col = df.index if isinstance(df.index, pd.Index) else None
        
        if index_col is None:
            return None
        
        # 優先順位に従って検索
        for key in mapping_keys:
            # 完全一致を試す（空白除去済み）
            key_normalized = key.strip().lower()
            for idx in index_col:
                if str(idx).strip().lower() == key_normalized:
                    return df.loc[idx]
            
            # 部分一致を試す（大文字小文字を区別しない、空白除去済み）
            matches = [idx for idx in index_col if key_normalized in str(idx).strip().lower()]
            if matches:
                # 最初に見つかったものを返す（優先順位が高い）
                return df.loc[matches[0]]
        
        return None
    
    def _extract_value(self, series: pd.Series, year_offset: int = 0) -> Optional[float]:
        """
        年度ごとの値を抽出（最新年度からyear_offset年目）
        
        Args:
            series: 年度ごとの値が入ったSeries
            year_offset: 何年目か（0=最新年度）
            
        Returns:
            値（見つからない場合はNone）
        """
        if series is None or series.empty:
            return None
        
        # NaNを除外してソート
        valid_values = series.dropna()
        if valid_values.empty:
            return None
        
        # 年度順にソート（新しい順）
        sorted_values = valid_values.sort_index(ascending=False)
        
        if len(sorted_values) > year_offset:
            value = sorted_values.iloc[year_offset]
            return float(value) if pd.notna(value) else None
        
        return None
    
    def _calculate_revenue_growth_rate(self, revenue_current: Optional[float], revenue_previous: Optional[float]) -> Optional[float]:
        """
        売上高成長率を計算
        
        Args:
            revenue_current: 直近年度の売上高
            revenue_previous: 前年度の売上高
            
        Returns:
            成長率（%）、計算できない場合はNone
        """
        if revenue_current is None or revenue_previous is None:
            return None
        
        if revenue_previous == 0:
            return None
        
        growth_rate = ((revenue_current - revenue_previous) / revenue_previous) * 100
        return growth_rate
    
    def _calculate_invested_capital(self, equity: Optional[float], total_debt: Optional[float],
                                   long_term_debt: Optional[float], short_term_debt: Optional[float]) -> Tuple[Optional[float], bool]:
        """
        Invested Capitalを計算
        
        Args:
            equity: 自己資本（Stockholders Equity）
            total_debt: 総負債（Total Debt）
            long_term_debt: 長期負債（Long Term Debt）
            short_term_debt: 短期負債（Current Debt）
            
        Returns:
            (Invested Capital, 代替フラグ) のタプル
            - Invested Capital: 計算できた場合の値、できない場合はNone
            - 代替フラグ: Total Assetsで代用した場合True
        """
        # Total Debtがあれば採用
        if total_debt is not None:
            debt_val = total_debt
        # なければ Long Term Debt + Current Debt
        elif long_term_debt is not None or short_term_debt is not None:
            debt_val = (long_term_debt or 0) + (short_term_debt or 0)
        else:
            debt_val = None
        
        # Stockholders Equityを加算
        if equity is not None and debt_val is not None:
            invested_capital = equity + debt_val
            return invested_capital, False
        
        # Invested Capitalが計算できない場合はNoneを返す（代替処理は呼び出し側で行う）
        return None, False
    
    def _calculate_roic(self, operating_income: Optional[float], invested_capital: Optional[float],
                       total_assets: Optional[float] = None) -> Tuple[Optional[float], bool]:
        """
        ROICを計算
        
        Args:
            operating_income: 営業利益
            invested_capital: 投下資本
            total_assets: 総資産（代替用）
            
        Returns:
            (ROIC（%）, 代替フラグ) のタプル
            - ROIC: 計算できた場合の値、できない場合はNone
            - 代替フラグ: Total Assetsで代用した場合True
        """
        if operating_income is None:
            return None, False
        
        # NOPAT = Operating Income * (1 - Tax Rate)
        nopat = operating_income * (1 - self.tax_rate)
        
        # Invested Capitalを使用（正の値の場合のみ）
        if invested_capital is not None and invested_capital > 0:
            roic = (nopat / invested_capital) * 100
            return roic, False
        
        # Invested Capitalが取れない、または負の値の場合はTotal Assetsで代用（正の値の場合のみ）
        if total_assets is not None and total_assets > 0:
            roic = (nopat / total_assets) * 100
            return roic, True
        
        return None, False
    
    def _check_other_liabilities_exist(self, bs_df: pd.DataFrame) -> bool:
        """
        BSデータ内に他の負債項目（買掛金など）が存在するかチェック
        
        Args:
            bs_df: BSデータのDataFrame
            
        Returns:
            他の負債項目が存在する場合True
        """
        if bs_df is None or bs_df.empty:
            return False
        
        # 負債関連のキーワード
        liability_keywords = [
            'Accounts Payable', '買掛金', 'Accounts Payable & Accrued Expense',
            'Current Liabilities', '流動負債', 'Total Liabilities', '総負債',
            'Other Current Liabilities', 'その他流動負債',
            'Accrued Expenses', '未払費用', 'Payables'
        ]
        
        index_col = bs_df.index if isinstance(bs_df.index, pd.Index) else None
        if index_col is None:
            return False
        
        # 負債関連の項目が存在するかチェック
        for keyword in liability_keywords:
            for idx in index_col:
                if keyword.lower() in str(idx).strip().lower():
                    # 値が存在するかチェック
                    series = bs_df.loc[idx]
                    if series is not None and not series.empty:
                        # NaN以外の値があるかチェック
                        if series.dropna().any():
                            return True
        
        return False
    
    def _calculate_data_quality_score(self, result: Dict) -> int:
        """
        データ品質スコアを計算（100点満点）
        
        Args:
            result: 正規化されたデータの辞書
            
        Returns:
            データ品質スコア（0-100点）
        """
        # missing_criticalなら0点
        if result.get('missing_critical', False):
            return 0
        
        # 無借金による項目不在の場合は100点
        if result.get('is_debt_free', False):
            # 必須項目が揃っているかチェック
            required_items = ['revenue', 'operating_income', 'net_income', 'total_assets', 'equity', 'roic']
            all_present = all(result.get(item) is not None for item in required_items)
            if all_present:
                return 100
        
        # roic_using_total_assets（代替使用）なら70点
        # ただし、無借金の場合は除外（既に上で処理済み）
        if result.get('roic_using_total_assets', False) and not result.get('is_debt_free', False):
            return 70
        
        # すべて揃っていれば100点
        # 必須項目がすべて揃っているかチェック
        required_items = ['revenue', 'operating_income', 'net_income', 'total_assets', 'equity', 'roic']
        all_present = all(result.get(item) is not None for item in required_items)
        
        if all_present:
            return 100
        
        # 一部欠損がある場合は80点
        critical_items = ['revenue', 'operating_income']
        critical_present = all(result.get(item) is not None for item in critical_items)
        
        if critical_present:
            return 80
        
        # それ以外は50点
        return 50
    
    def _calculate_roe(self, net_income: Optional[float], equity: Optional[float]) -> Optional[float]:
        """
        ROE（自己資本利益率）を計算
        
        Args:
            net_income: 純利益
            equity: 自己資本
            
        Returns:
            ROE（%）、計算できない場合はNone
        """
        if net_income is None or equity is None:
            return None
        if equity == 0:
            return None
        return (net_income / equity) * 100
    
    def _calculate_pbr(self, market_cap: Optional[float], equity: Optional[float]) -> Optional[float]:
        """
        PBR（株価純資産倍率）を計算
        
        Args:
            market_cap: 時価総額
            equity: 自己資本
            
        Returns:
            PBR、計算できない場合はNone
        """
        if market_cap is None or equity is None:
            return None
        if equity == 0:
            return None
        return market_cap / equity
    
    def _calculate_per(self, market_cap: Optional[float], net_income: Optional[float]) -> Optional[float]:
        """
        PER（株価収益率）を計算
        
        Args:
            market_cap: 時価総額
            net_income: 純利益
            
        Returns:
            PER、計算できない場合はNone
        """
        if market_cap is None or net_income is None:
            return None
        if net_income == 0:
            return None
        return market_cap / net_income
    
    def _calculate_equity_ratio(self, equity: Optional[float], total_assets: Optional[float]) -> Optional[float]:
        """
        自己資本比率を計算
        
        Args:
            equity: 自己資本
            total_assets: 総資産
            
        Returns:
            自己資本比率（%）、計算できない場合はNone
        """
        if equity is None or total_assets is None:
            return None
        if total_assets == 0:
            return None
        return (equity / total_assets) * 100
    
    def _calculate_growth_score(self, revenue_growth_rate: Optional[float]) -> float:
        """
        売上成長率スコアを計算（40点満点）
        
        Args:
            revenue_growth_rate: 売上成長率（%）
            
        Returns:
            スコア（0-40点）
        """
        if revenue_growth_rate is None:
            return 0.0
        
        # マイナス成長は大幅減点の対象（スコアは0点）
        if revenue_growth_rate < 0:
            return 0.0
        
        # 成長率に応じてスコアを計算（40点満点）
        # 20%以上で満点、0%で0点、線形補間
        score = min(40.0, max(0.0, revenue_growth_rate * 2.0))
        return score
    
    def _calculate_profit_score(self, roe: Optional[float]) -> float:
        """
        ROEスコアを計算（30点満点）
        
        Args:
            roe: ROE（%）
            
        Returns:
            スコア（0-30点）
        """
        if roe is None:
            return 0.0
        
        # ROEに応じてスコアを計算（30点満点）
        # 15%以上で満点、0%で0点、線形補間
        score = min(30.0, max(0.0, roe * 2.0))
        return score
    
    def _calculate_value_score(self, pbr: Optional[float], per: Optional[float]) -> float:
        """
        PBR/PERスコアを計算（20点満点）
        
        Args:
            pbr: PBR
            per: PER
            
        Returns:
            スコア（0-20点）
        """
        pbr_score = 0.0
        per_score = 0.0
        
        # PBRを数値型に変換
        if pbr is not None:
            try:
                pbr_float = float(pbr)
            except (ValueError, TypeError):
                pbr_float = None
        else:
            pbr_float = None
        
        # PERを数値型に変換
        if per is not None:
            try:
                per_float = float(per)
            except (ValueError, TypeError):
                per_float = None
        else:
            per_float = None
        
        # PBRスコア（10点満点）
        # PBRが1.0以下で満点、2.0以上で0点、線形補間
        if pbr_float is not None:
            if pbr_float <= 1.0:
                pbr_score = 10.0
            elif pbr_float >= 2.0:
                pbr_score = 0.0
            else:
                pbr_score = 10.0 * (2.0 - pbr_float) / 1.0
        
        # PERスコア（10点満点）
        # PERが10倍以下で満点、30倍以上で0点、線形補間
        if per_float is not None:
            if per_float <= 10.0:
                per_score = 10.0
            elif per_float >= 30.0:
                per_score = 0.0
            else:
                per_score = 10.0 * (30.0 - per_float) / 20.0
        
        # PBRとPERの平均を取る（両方ある場合）または片方のみ
        if pbr_float is not None and per_float is not None:
            return (pbr_score + per_score) / 2.0
        elif pbr_float is not None:
            return pbr_score
        elif per_float is not None:
            return per_score
        else:
            return 0.0
    
    def _calculate_safety_score(self, equity_ratio: Optional[float]) -> float:
        """
        自己資本比率スコアを計算（10点満点）
        
        Args:
            equity_ratio: 自己資本比率（%）
            
        Returns:
            スコア（0-10点）
        """
        if equity_ratio is None:
            return 0.0
        
        # 自己資本比率に応じてスコアを計算（10点満点）
        # 50%以上で満点、20%以下で0点、線形補間
        if equity_ratio >= 50.0:
            return 10.0
        elif equity_ratio <= 20.0:
            return 0.0
        else:
            return 10.0 * (equity_ratio - 20.0) / 30.0
    
    def _calculate_new_scoring(self, result: Dict) -> Dict:
        """
        新しいスコアリング方式でスコアを計算
        
        Args:
            result: 正規化されたデータの辞書
            
        Returns:
            スコア情報を追加した辞書
        """
        # 大幅減点フラグ
        penalty = 0.0
        if result.get('operating_income') is not None and result['operating_income'] < 0:
            penalty -= 40.0
        if result.get('revenue_growth_rate') is not None and result['revenue_growth_rate'] < 0:
            penalty -= 40.0
        
        # 各カテゴリのスコアを計算
        score_growth = self._calculate_growth_score(result.get('revenue_growth_rate'))
        score_profit = self._calculate_profit_score(result.get('roe'))
        score_value = self._calculate_value_score(result.get('pbr'), result.get('per'))
        score_safety = self._calculate_safety_score(result.get('equity_ratio'))
        
        # 合計スコア（100点満点 + 減点）
        total_score = score_growth + score_profit + score_value + score_safety + penalty
        
        # スコア情報を追加
        result['score_growth'] = score_growth
        result['score_profit'] = score_profit
        result['score_value'] = score_value
        result['score_safety'] = score_safety
        result['penalty'] = penalty
        result['total_score'] = max(0.0, total_score)  # マイナスにならないように
        
        return result
    
    def normalize_stock_data(self, ticker: str, pl_df: Optional[pd.DataFrame], 
                            bs_df: Optional[pd.DataFrame], info: Optional[Dict] = None) -> Dict:
        """
        単一銘柄のデータを正規化
        
        Args:
            ticker: 銘柄コード
            pl_df: PLデータのDataFrame
            bs_df: BSデータのDataFrame
            
        Returns:
            正規化されたデータの辞書
        """
        result = {
            'ticker': ticker,
            'revenue': None,
            'revenue_previous': None,
            'revenue_growth_rate': None,
            'operating_income': None,
            'net_income': None,
            'total_assets': None,
            'total_debt': None,
            'long_term_debt': None,
            'short_term_debt': None,
            'equity': None,
            'invested_capital': None,
            'roic': None,
            'roic_using_total_assets': False,  # Total Assetsで代用した場合True
            'debt_to_equity_ratio': None,  # 負債資本倍率
            'cash': None,  # 現預金
            'is_debt_free': False,  # 無借金と判断した場合True
            'debt_free_flag': False,  # total_debtが0またはNone（実質ゼロ）の場合True
            'net_cash_status': None,  # 実質無借金のラベル
            'missing_critical': False,
            'missing_items': [],
        }
        
        # PLデータから値を抽出
        if pl_df is not None and not pl_df.empty:
            # ticker列を除外して処理
            pl_work = pl_df.copy()
            if 'ticker' in pl_work.columns:
                pl_work = pl_work.drop(columns=['ticker'])
            
            # インデックスを設定（Unnamed: 0または最初の列）
            if 'Unnamed: 0' in pl_work.columns:
                pl_work = pl_work.set_index('Unnamed: 0')
            elif pl_work.index.name is None or pl_work.index.name == 'Unnamed: 0':
                # 既にインデックスが設定されている場合
                pass
            
            # Revenue（優先順位: Total Revenue > Operating Revenue）
            revenue_series = self._find_mapping_value(pl_work, MAPPING_DICT['Revenue'], priority_order=True)
            result['revenue'] = self._extract_value(revenue_series, year_offset=0)
            result['revenue_previous'] = self._extract_value(revenue_series, year_offset=1)
            result['revenue_growth_rate'] = self._calculate_revenue_growth_rate(
                result['revenue'], result['revenue_previous']
            )
            
            # Operating Income（優先順位: Operating Income > Operating Profit）
            operating_income_series = self._find_mapping_value(pl_work, MAPPING_DICT['OperatingIncome'], priority_order=True)
            result['operating_income'] = self._extract_value(operating_income_series, year_offset=0)
            
            # Net Income（優先順位: Net Income Common Stockholders > Net Income）
            net_income_series = self._find_mapping_value(pl_work, MAPPING_DICT['NetIncome'], priority_order=True)
            result['net_income'] = self._extract_value(net_income_series, year_offset=0)
            
            # 重要な項目が欠損しているかチェック
            if result['revenue'] is None:
                result['missing_items'].append('Revenue')
            if result['operating_income'] is None:
                result['missing_items'].append('OperatingIncome')
            
            if result['revenue'] is None or result['operating_income'] is None:
                result['missing_critical'] = True
        
        # BSデータから値を抽出
        if bs_df is not None and not bs_df.empty:
            # ticker列を除外して処理
            bs_work = bs_df.copy()
            if 'ticker' in bs_work.columns:
                bs_work = bs_work.drop(columns=['ticker'])
            
            # インデックスを設定
            if 'Unnamed: 0' in bs_work.columns:
                bs_work = bs_work.set_index('Unnamed: 0')
            
            # Total Assets
            total_assets_series = self._find_mapping_value(bs_work, MAPPING_DICT['TotalAssets'])
            result['total_assets'] = self._extract_value(total_assets_series, year_offset=0)
            
            # Equity（Stockholders Equity）
            equity_series = self._find_mapping_value(bs_work, MAPPING_DICT['Equity'])
            result['equity'] = self._extract_value(equity_series, year_offset=0)
            
            # Cash（現預金）
            cash_series = self._find_mapping_value(bs_work, MAPPING_DICT['Cash'])
            result['cash'] = self._extract_value(cash_series, year_offset=0)
            
            # Total Debt（優先順位: Total Debtがあれば採用）
            total_debt_series = self._find_mapping_value(bs_work, MAPPING_DICT['TotalDebt'], priority_order=True)
            if total_debt_series is not None:
                result['total_debt'] = self._extract_value(total_debt_series, year_offset=0)
            else:
                # Total Debtがない場合は Long Term Debt + Current Debt から計算
                long_term_debt_series = self._find_mapping_value(bs_work, MAPPING_DICT['LongTermDebt'])
                short_term_debt_series = self._find_mapping_value(bs_work, MAPPING_DICT['ShortTermDebt'])
                
                result['long_term_debt'] = self._extract_value(long_term_debt_series, year_offset=0)
                result['short_term_debt'] = self._extract_value(short_term_debt_series, year_offset=0)
                
                if result['long_term_debt'] is not None or result['short_term_debt'] is not None:
                    result['total_debt'] = (result['long_term_debt'] or 0) + (result['short_term_debt'] or 0)
                else:
                    # Total Debtが取得できない場合、無借金の可能性をチェック
                    if result['equity'] is not None and result['equity'] > 0:
                        # 他の負債項目（買掛金など）が存在するかチェック
                        if self._check_other_liabilities_exist(bs_work):
                            # 有利子負債は実質ゼロと判断
                            result['total_debt'] = 0.0
                            result['is_debt_free'] = True
                            logger.info(f"{ticker}: 無借金と判断（有利子負債=0）")
            
            if result['total_assets'] is None:
                result['missing_items'].append('TotalAssets')
        
        # Invested Capitalを計算
        invested_capital, _ = self._calculate_invested_capital(
            result['equity'],
            result['total_debt'],
            result['long_term_debt'],
            result['short_term_debt']
        )
        result['invested_capital'] = invested_capital
        
        # total_debtが0の場合は、invested_capital = equityとして再計算
        if result['total_debt'] == 0 and result['equity'] is not None:
            result['invested_capital'] = result['equity']
        
        # ROICを計算（Invested Capitalが取れない場合はTotal Assetsで代用）
        # ただし、total_debtが0の場合は代替フラグを立てない
        roic, using_total_assets = self._calculate_roic(
            result['operating_income'],
            result['invested_capital'],
            result['total_assets']
        )
        result['roic'] = roic
        # total_debtが0の場合は代替フラグを立てない（無借金による正常な状態）
        if result['total_debt'] == 0:
            result['roic_using_total_assets'] = False
        else:
            result['roic_using_total_assets'] = using_total_assets
        
        # 負債資本倍率を計算
        if result['equity'] is not None and result['equity'] != 0:
            if result['total_debt'] is not None:
                result['debt_to_equity_ratio'] = result['total_debt'] / result['equity']
            else:
                result['debt_to_equity_ratio'] = 0.0  # 無借金の場合
        else:
            result['debt_to_equity_ratio'] = None
        
        # debt_free_flagを設定（total_debtが0またはNoneの場合）
        if result['total_debt'] is None or result['total_debt'] == 0:
            result['debt_free_flag'] = True
        else:
            result['debt_free_flag'] = False
        
        # net_cash_statusを計算（有利子負債よりも現預金が多い場合）
        if result['total_debt'] is not None and result['cash'] is not None:
            if result['cash'] > result['total_debt']:
                result['net_cash_status'] = '実質無借金'
            else:
                result['net_cash_status'] = None
        elif result['debt_free_flag']:
            result['net_cash_status'] = '実質無借金'
        else:
            result['net_cash_status'] = None
        
        # infoから時価総額などを取得してROE、PBR、PER、自己資本比率を計算
        if info is not None:
            market_cap = info.get('marketCap')
            if market_cap is None:
                # currentPriceとsharesOutstandingから計算
                current_price = info.get('currentPrice')
                shares_outstanding = info.get('sharesOutstanding')
                if current_price is not None and shares_outstanding is not None:
                    market_cap = current_price * shares_outstanding
            
            # ROEを計算（infoに既にある場合はそれを使用、なければ計算）
            if 'returnOnEquity' in info and info['returnOnEquity'] is not None:
                result['roe'] = info['returnOnEquity'] * 100  # 小数から%に変換
            else:
                result['roe'] = self._calculate_roe(result['net_income'], result['equity'])
            
            # PBRを計算（infoに既にある場合はそれを使用、なければ計算）
            if 'priceToBook' in info and info['priceToBook'] is not None:
                try:
                    result['pbr'] = float(info['priceToBook'])
                except (ValueError, TypeError):
                    result['pbr'] = self._calculate_pbr(market_cap, result['equity'])
            else:
                result['pbr'] = self._calculate_pbr(market_cap, result['equity'])
            
            # PERを計算（infoに既にある場合はそれを使用、なければ計算）
            if 'trailingPE' in info and info['trailingPE'] is not None:
                try:
                    result['per'] = float(info['trailingPE'])
                except (ValueError, TypeError):
                    # forwardPEを試す
                    if 'forwardPE' in info and info['forwardPE'] is not None:
                        try:
                            result['per'] = float(info['forwardPE'])
                        except (ValueError, TypeError):
                            result['per'] = self._calculate_per(market_cap, result['net_income'])
                    else:
                        result['per'] = self._calculate_per(market_cap, result['net_income'])
            elif 'forwardPE' in info and info['forwardPE'] is not None:
                try:
                    result['per'] = float(info['forwardPE'])
                except (ValueError, TypeError):
                    result['per'] = self._calculate_per(market_cap, result['net_income'])
            else:
                result['per'] = self._calculate_per(market_cap, result['net_income'])
        else:
            # infoがない場合は計算のみ
            result['roe'] = self._calculate_roe(result['net_income'], result['equity'])
            result['pbr'] = None
            result['per'] = None
        
        # 自己資本比率を計算
        result['equity_ratio'] = self._calculate_equity_ratio(result['equity'], result['total_assets'])
        
        # 新しいスコアリング方式でスコアを計算
        result = self._calculate_new_scoring(result)
        
        return result
    
    def _json_to_dataframe(self, json_data: Dict) -> Optional[pd.DataFrame]:
        """
        JSON形式の財務データをDataFrameに変換
        
        Args:
            json_data: JSON形式の財務データ（financials, balance_sheet, cashflowなど）
            
        Returns:
            DataFrame（インデックスに項目名、列に日付）
        """
        if not json_data or not isinstance(json_data, dict):
            return None
        
        # JSONデータをDataFrameに変換
        # 構造: {"項目名": {"日付": 値, ...}, ...}
        rows = []
        for item_name, date_values in json_data.items():
            if isinstance(date_values, dict):
                row = {'item': item_name}
                row.update(date_values)
                rows.append(row)
        
        if not rows:
            return None
        
        df = pd.DataFrame(rows)
        if 'item' in df.columns:
            df = df.set_index('item')
        
        return df
    
    def load_raw_data(self) -> Dict[str, Dict]:
        """
        data/rawから生データを読み込み（JSONファイルから）
        
        Returns:
            銘柄ごとのデータ辞書 {ticker: {'pl': DataFrame, 'bs': DataFrame, ...}}
        """
        data_dict = {}
        
        # JSONファイルを検索（data/raw/*.json）
        json_files = list(self.raw_data_dir.glob("*.json"))
        
        # jpx_tse_info.csvは除外
        json_files = [f for f in json_files if f.name != 'jpx_tse_info.csv']
        
        logger.info(f"JSONファイル検索: {len(json_files)}件")
        
        for json_file in json_files:
            try:
                # ファイル名からtickerを抽出（例: 3038.json → 3038）
                ticker_match = re.search(r'(\d{4})', json_file.stem)
                if not ticker_match:
                    logger.warning(f"{json_file.name}: tickerを抽出できません")
                    continue
                
                ticker = ticker_match.group(1)
                
                # JSONファイルを読み込み
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # tickerがJSON内にもある場合は確認
                if 'ticker' in json_data:
                    json_ticker = str(json_data['ticker']).strip()
                    # コード整形（.0を除去して4桁に）
                    json_ticker_clean = re.sub(r'\.0$', '', json_ticker).zfill(4)
                    if json_ticker_clean != ticker:
                        logger.warning(f"{json_file.name}: ファイル名のticker({ticker})とJSON内のticker({json_ticker_clean})が不一致")
                
                # financials (PLデータ) をDataFrameに変換
                if 'financials' in json_data:
                    pl_df = self._json_to_dataframe(json_data['financials'])
                    if pl_df is not None and not pl_df.empty:
                        if ticker not in data_dict:
                            data_dict[ticker] = {}
                        data_dict[ticker]['pl'] = pl_df
                
                # balance_sheet (BSデータ) をDataFrameに変換
                if 'balance_sheet' in json_data:
                    bs_df = self._json_to_dataframe(json_data['balance_sheet'])
                    if bs_df is not None and not bs_df.empty:
                        if ticker not in data_dict:
                            data_dict[ticker] = {}
                        data_dict[ticker]['bs'] = bs_df
                
                # info (基本情報) を保存
                if 'info' in json_data:
                    if ticker not in data_dict:
                        data_dict[ticker] = {}
                    data_dict[ticker]['info'] = json_data['info']
                
                # cashflow (CFデータ) は必要に応じて使用（現在は未使用）
                # if 'cashflow' in json_data:
                #     cf_df = self._json_to_dataframe(json_data['cashflow'])
                #     if cf_df is not None and not cf_df.empty:
                #         if ticker not in data_dict:
                #             data_dict[ticker] = {}
                #         data_dict[ticker]['cf'] = cf_df
                
            except json.JSONDecodeError as e:
                logger.error(f"{json_file.name}のJSON解析エラー: {str(e)}")
            except Exception as e:
                logger.error(f"{json_file.name}の読み込みエラー: {str(e)}")
        
        logger.info(f"生データ読み込み完了: {len(data_dict)}銘柄")
        return data_dict
    
    def analyze_all(self) -> pd.DataFrame:
        """
        全銘柄のデータを正規化・分析
        
        Returns:
            分析結果のDataFrame
        """
        # 生データを読み込み
        raw_data = self.load_raw_data()
        
        if not raw_data:
            logger.warning("分析対象のデータがありません")
            return pd.DataFrame()
        
        # 各銘柄を正規化
        results = []
        for ticker, data in raw_data.items():
            pl_df = data.get('pl')
            bs_df = data.get('bs')
            info = data.get('info')
            
            normalized = self.normalize_stock_data(ticker, pl_df, bs_df, info)
            results.append(normalized)
            
            if normalized['missing_critical']:
                logger.warning(f"{ticker}: 重要なデータが欠損しています: {normalized['missing_items']}")
        
        # DataFrameに変換
        df = pd.DataFrame(results)
        
        logger.info(f"分析完了: {len(df)}銘柄")
        return df
    
    def save_results(self, df: pd.DataFrame, filename: str = "screened_results.csv") -> str:
        """
        分析結果を保存
        
        Args:
            df: 分析結果のDataFrame
            filename: 保存ファイル名
            
        Returns:
            保存されたファイルパス
        """
        if df.empty:
            logger.warning("保存するデータがありません")
            return ""
        
        filepath = self.processed_data_dir / filename
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"分析結果保存完了: {filepath}")
        
        return str(filepath)
    
    def generate_final_recommendations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        最終推奨リストを生成（新しいスコアリング方式）
        
        Args:
            df: 分析結果のDataFrame
            
        Returns:
            ランキング付きの推奨リストDataFrame
        """
        if df.empty:
            logger.warning("ランキング対象のデータがありません")
            return pd.DataFrame()
        
        df_work = df.copy()
        
        # total_scoreが既に計算されている場合はそのまま使用
        # ない場合は0点とする
        if 'total_score' not in df_work.columns:
            df_work['total_score'] = 0.0
        
        # ランキングを追加（total_scoreの降順）
        df_work = df_work.sort_values('total_score', ascending=False, na_position='last')
        df_work['rank'] = range(1, len(df_work) + 1)
        
        # 列の順序を整理（重要項目を前に）
        priority_columns = [
            'rank', 'ticker', 'total_score', 'data_quality_score', 'total_bonus_score',
            'debt_free_flag', 'net_cash_status', 'debt_to_equity_ratio',
            'revenue', 'revenue_growth_rate', 'operating_income', 'net_income',
            'roic', 'roic_using_total_assets', 'invested_capital',
            'total_assets', 'equity', 'total_debt', 'cash',
            'missing_critical', 'missing_items'
        ]
        
        # 存在する列のみを選択
        existing_priority = [col for col in priority_columns if col in df_work.columns]
        other_columns = [col for col in df_work.columns if col not in existing_priority]
        final_columns = existing_priority + other_columns
        
        df_ranked = df_work[final_columns].copy()
        
        logger.info(f"最終推奨リスト生成完了: {len(df_ranked)}銘柄")
        return df_ranked
    
    def save_final_recommendations(self, df: pd.DataFrame, filename: str = "final_recommendations.csv") -> str:
        """
        最終推奨リストを保存
        
        Args:
            df: ランキング付きの推奨リストDataFrame
            filename: 保存ファイル名
            
        Returns:
            保存されたファイルパス
        """
        if df.empty:
            logger.warning("保存するデータがありません")
            return ""
        
        filepath = self.processed_data_dir / filename
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"最終推奨リスト保存完了: {filepath}")
        
        return str(filepath)


# ============================================
# メイン関数
# ============================================
def main():
    """メイン関数"""
    print("=" * 60)
    print("財務データの正規化と分析")
    print("=" * 60)
    
    analyzer = FinancialDataAnalyzer()
    
    # 全銘柄を分析
    results_df = analyzer.analyze_all()
    
    if not results_df.empty:
        # 結果を保存
        filepath = analyzer.save_results(results_df)
        print(f"\n分析結果を保存しました: {filepath}")
        print(f"\n分析結果のサマリー:")
        print(f"  総銘柄数: {len(results_df)}")
        print(f"  重要データ欠損: {results_df['missing_critical'].sum()}銘柄")
        if 'debt_free_flag' in results_df.columns:
            print(f"  無借金銘柄: {results_df['debt_free_flag'].sum()}銘柄")
        if 'net_cash_status' in results_df.columns:
            net_cash_count = (results_df['net_cash_status'] == '実質無借金').sum()
            print(f"  実質無借金銘柄: {net_cash_count}銘柄")
        print(f"\n先頭5銘柄:")
        print(results_df.head().to_string())
        
        # 最終推奨リストを生成
        print("\n" + "=" * 60)
        print("最終推奨リストを生成中...")
        final_df = analyzer.generate_final_recommendations(results_df)
        
        if not final_df.empty:
            final_filepath = analyzer.save_final_recommendations(final_df)
            print(f"\n最終推奨リストを保存しました: {final_filepath}")
            print(f"\nランキングTOP5:")
            print(final_df.head().to_string())
    else:
        print("分析対象のデータがありません")


if __name__ == "__main__":
    main()
