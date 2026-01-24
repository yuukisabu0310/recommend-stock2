"""
財務データの正規化と分析ロジック
data/raw/*.csvからデータを読み込み、名寄せを行って分析用データを作成
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import glob

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
    
    def normalize_stock_data(self, ticker: str, pl_df: Optional[pd.DataFrame], 
                            bs_df: Optional[pd.DataFrame]) -> Dict:
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
        
        # データ品質スコアを計算
        result['data_quality_score'] = self._calculate_data_quality_score(result)
        
        return result
    
    def load_raw_data(self) -> Dict[str, Dict]:
        """
        data/rawから生データを読み込み
        
        Returns:
            銘柄ごとのデータ辞書 {ticker: {'pl': DataFrame, 'bs': DataFrame, ...}}
        """
        data_dict = {}
        
        # PLファイルを検索（通常のCSVとデバッグ用CSVの両方）
        pl_files = list(self.raw_data_dir.glob("*_PL.csv")) + list(self.raw_data_dir.glob("debug/*_PL.csv"))
        for pl_file in pl_files:
            try:
                pl_df = pd.read_csv(pl_file, encoding='utf-8-sig')
                
                # ticker列がある場合
                if 'ticker' in pl_df.columns:
                    for ticker in pl_df['ticker'].unique():
                        ticker_pl = pl_df[pl_df['ticker'] == ticker].copy()
                        if ticker not in data_dict:
                            data_dict[ticker] = {}
                        # 既存のデータがある場合は統合（デバッグデータを優先）
                        if 'pl' not in data_dict[ticker] or 'debug' in str(pl_file):
                            data_dict[ticker]['pl'] = ticker_pl
                else:
                    # ticker列がない場合、ファイル名からtickerを抽出
                    # 例: debug_7203_PL.csv → 7203
                    import re
                    match = re.search(r'(\d{4})', pl_file.stem)
                    if match:
                        ticker = match.group(1)
                        if ticker not in data_dict:
                            data_dict[ticker] = {}
                        data_dict[ticker]['pl'] = pl_df
                    else:
                        logger.warning(f"{pl_file.name}: ticker列もファイル名からtickerを抽出できません")
            except Exception as e:
                logger.error(f"{pl_file.name}の読み込みエラー: {str(e)}")
        
        # BSファイルを検索
        bs_files = list(self.raw_data_dir.glob("*_BS.csv")) + list(self.raw_data_dir.glob("debug/*_BS.csv"))
        for bs_file in bs_files:
            try:
                bs_df = pd.read_csv(bs_file, encoding='utf-8-sig')
                
                # ticker列がある場合
                if 'ticker' in bs_df.columns:
                    for ticker in bs_df['ticker'].unique():
                        ticker_bs = bs_df[bs_df['ticker'] == ticker].copy()
                        if ticker not in data_dict:
                            data_dict[ticker] = {}
                        if 'bs' not in data_dict[ticker] or 'debug' in str(bs_file):
                            data_dict[ticker]['bs'] = ticker_bs
                else:
                    # ticker列がない場合、ファイル名からtickerを抽出
                    import re
                    match = re.search(r'(\d{4})', bs_file.stem)
                    if match:
                        ticker = match.group(1)
                        if ticker not in data_dict:
                            data_dict[ticker] = {}
                        data_dict[ticker]['bs'] = bs_df
                    else:
                        logger.warning(f"{bs_file.name}: ticker列もファイル名からtickerを抽出できません")
            except Exception as e:
                logger.error(f"{bs_file.name}の読み込みエラー: {str(e)}")
        
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
            
            normalized = self.normalize_stock_data(ticker, pl_df, bs_df)
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
        最終推奨リストを生成（ボーナススコア付きランキング）
        
        Args:
            df: 分析結果のDataFrame
            
        Returns:
            ランキング付きの推奨リストDataFrame
        """
        if df.empty:
            logger.warning("ランキング対象のデータがありません")
            return pd.DataFrame()
        
        df_work = df.copy()
        
        # ボーナススコアを計算
        # debt_free_flagがTrueの場合にボーナスを加算
        bonus_score = 0
        if 'debt_free_flag' in df_work.columns:
            # 無借金フラグがTrueの場合、ボーナススコアを加算（例: 10点）
            df_work['debt_free_bonus'] = df_work['debt_free_flag'].apply(lambda x: 10 if x else 0)
            bonus_score = df_work['debt_free_bonus']
        else:
            df_work['debt_free_bonus'] = 0
        
        # net_cash_statusが「実質無借金」の場合もボーナス
        if 'net_cash_status' in df_work.columns:
            net_cash_bonus = df_work['net_cash_status'].apply(lambda x: 5 if x == '実質無借金' else 0)
            df_work['net_cash_bonus'] = net_cash_bonus
        else:
            df_work['net_cash_bonus'] = 0
        
        # 総合ボーナススコア
        df_work['total_bonus_score'] = df_work['debt_free_bonus'] + df_work['net_cash_bonus']
        
        # 総合スコア = データ品質スコア + ボーナススコア
        if 'data_quality_score' in df_work.columns:
            df_work['total_score'] = df_work['data_quality_score'] + df_work['total_bonus_score']
        else:
            df_work['total_score'] = df_work['total_bonus_score']
        
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
