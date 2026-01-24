"""
日本株財務データ取得モジュール
TradingView/yfinanceから財務データを取得し、data/rawに保存する
"""

import os
import time
import logging
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import yfinance as yf
from pathlib import Path

import constants
from ticker_list import get_processed_tickers as get_processed_tickers_from_files


# ============================================
# ログ設定
# ============================================
def setup_logger(config: Dict) -> logging.Logger:
    """ロガーを設定"""
    log_dir = Path(constants.LOG_CONFIG["log_dir"])
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_level = getattr(logging, config.get("general", {}).get("log_level", "INFO"))
    log_file = log_dir / constants.LOG_CONFIG["log_file_pattern"].format(
        date=datetime.now().strftime(constants.LOG_CONFIG["date_format"])
    )
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


# ============================================
# データ取得クラス
# ============================================
class StockDataFetcher:
    """日本株財務データ取得クラス"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        初期化
        
        Args:
            config_path: 設定ファイルのパス
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = setup_logger(self.config)
        self.data_dir = Path("data/raw")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定から取得
        self.years = self.config.get("data_acquisition", {}).get("years", 5)
        self.retry_count = self.config.get("data_acquisition", {}).get("retry_count", 3)
        self.retry_interval = self.config.get("data_acquisition", {}).get("retry_interval", 5)
        self.rate_limit_delay = self.config.get("data_acquisition", {}).get("rate_limit_delay", 1.0)
        self.batch_size = self.config.get("data_acquisition", {}).get("batch_size", 50)
        
        # レート制限対策：初回アクセス前の待機時間
        self.initial_delay = 5.0
        
        self.logger.info(f"StockDataFetcher初期化完了: 取得年数={self.years}年, バッチサイズ={self.batch_size}銘柄")
    
    def _convert_to_japanese_ticker(self, ticker: str) -> str:
        """
        日本株ティッカーに変換（4桁コードに.Tを付与）
        
        Args:
            ticker: 銘柄コード（4桁または既に.T付き）
            
        Returns:
            日本株ティッカー（例: 7203.T）
        """
        ticker = str(ticker).strip()
        if not ticker.endswith('.T'):
            # 4桁コードに変換（0埋め）
            ticker = ticker.zfill(4) + '.T'
        return ticker
    
    def _fetch_financial_data(self, ticker: str) -> Optional[yf.Ticker]:
        """
        財務データを取得（リトライ機能付き）
        
        Args:
            ticker: ティッカーシンボル
            
        Returns:
            yf.Tickerオブジェクト（失敗時はNone）
        """
        jp_ticker = self._convert_to_japanese_ticker(ticker)
        
        for attempt in range(self.retry_count):
            try:
                self.logger.debug(f"{jp_ticker} データ取得試行 {attempt + 1}/{self.retry_count}")
                stock = yf.Ticker(jp_ticker)
                
                # レート制限対策
                time.sleep(self.rate_limit_delay)
                
                return stock
                
            except Exception as e:
                error_msg = str(e)
                self.logger.warning(f"{jp_ticker} 取得失敗 (試行 {attempt + 1}/{self.retry_count}): {error_msg}")
                
                # レート制限エラー（429）の場合は待機時間を延長
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    wait_time = self.retry_interval * (attempt + 2)  # 段階的に待機時間を延長
                    self.logger.warning(f"レート制限検出。{wait_time}秒待機します...")
                    time.sleep(wait_time)
                elif attempt < self.retry_count - 1:
                    time.sleep(self.retry_interval)
                else:
                    self.logger.error(f"{jp_ticker} 取得最終失敗: {error_msg}")
                    return None
        
        return None
    
    def _extract_financials(self, stock: yf.Ticker, ticker: str) -> Dict:
        """
        財務データを抽出（PL, CF, BS）
        
        Args:
            stock: yf.Tickerオブジェクト
            ticker: ティッカーシンボル
            
        Returns:
            財務データの辞書
        """
        data = {
            'ticker': ticker,
            'pl': None,
            'cf': None,
            'bs': None,
            'missing_flags': [],
            'data_quality': {}
        }
        
        try:
            # 損益計算書（PL: Profit & Loss）
            try:
                pl = stock.financials
                if pl is not None and not pl.empty:
                    # 最新5年分に制限
                    if len(pl.columns) > self.years:
                        pl = pl.iloc[:, :self.years]
                    data['pl'] = pl
                    self.logger.debug(f"{ticker} PLデータ取得成功: {len(pl.columns)}年分")
                else:
                    data['missing_flags'].append('pl_empty')
                    self.logger.warning(f"{ticker} PLデータが空です")
            except Exception as e:
                data['missing_flags'].append('pl_error')
                self.logger.warning(f"{ticker} PLデータ取得エラー: {str(e)}")
            
            # キャッシュフロー（CF: Cash Flow）
            try:
                cf = stock.cashflow
                if cf is not None and not cf.empty:
                    if len(cf.columns) > self.years:
                        cf = cf.iloc[:, :self.years]
                    data['cf'] = cf
                    self.logger.debug(f"{ticker} CFデータ取得成功: {len(cf.columns)}年分")
                else:
                    data['missing_flags'].append('cf_empty')
                    self.logger.warning(f"{ticker} CFデータが空です")
            except Exception as e:
                data['missing_flags'].append('cf_error')
                self.logger.warning(f"{ticker} CFデータ取得エラー: {str(e)}")
            
            # 貸借対照表（BS: Balance Sheet）
            try:
                bs = stock.balance_sheet
                if bs is not None and not bs.empty:
                    if len(bs.columns) > self.years:
                        bs = bs.iloc[:, :self.years]
                    data['bs'] = bs
                    self.logger.debug(f"{ticker} BSデータ取得成功: {len(bs.columns)}年分")
                else:
                    data['missing_flags'].append('bs_empty')
                    self.logger.warning(f"{ticker} BSデータが空です")
            except Exception as e:
                data['missing_flags'].append('bs_error')
                self.logger.warning(f"{ticker} BSデータ取得エラー: {str(e)}")
            
            # データ品質チェック
            data['data_quality'] = self._check_data_quality(data)
            
        except Exception as e:
            self.logger.error(f"{ticker} 財務データ抽出エラー: {str(e)}")
            data['missing_flags'].append('extraction_error')
        
        return data
    
    def _check_data_quality(self, data: Dict) -> Dict:
        """
        データ品質をチェック（欠損値の検出）
        
        Args:
            data: 財務データの辞書
            
        Returns:
            データ品質情報の辞書
        """
        quality = {
            'pl_missing_rate': 0.0,
            'cf_missing_rate': 0.0,
            'bs_missing_rate': 0.0,
            'critical_missing': []
        }
        
        # PLデータの欠損率チェック
        if data['pl'] is not None and not data['pl'].empty:
            total_cells = data['pl'].size
            missing_cells = data['pl'].isna().sum().sum()
            quality['pl_missing_rate'] = missing_cells / total_cells if total_cells > 0 else 1.0
            
            # 重要な指標の欠損チェック
            important_items = ['Total Revenue', 'Operating Income', 'Net Income', 
                             '売上高', '営業利益', '純利益']
            for item in important_items:
                if item in data['pl'].index:
                    if data['pl'].loc[item].isna().all():
                        quality['critical_missing'].append(f'PL_{item}')
        else:
            quality['pl_missing_rate'] = 1.0
            quality['critical_missing'].append('PL_all')
        
        # CFデータの欠損率チェック
        if data['cf'] is not None and not data['cf'].empty:
            total_cells = data['cf'].size
            missing_cells = data['cf'].isna().sum().sum()
            quality['cf_missing_rate'] = missing_cells / total_cells if total_cells > 0 else 1.0
            
            # 重要な指標の欠損チェック
            important_items = ['Operating Cash Flow', 'Free Cash Flow',
                             '営業活動によるキャッシュフロー', 'フリーキャッシュフロー']
            for item in important_items:
                if item in data['cf'].index:
                    if data['cf'].loc[item].isna().all():
                        quality['critical_missing'].append(f'CF_{item}')
        else:
            quality['cf_missing_rate'] = 1.0
            quality['critical_missing'].append('CF_all')
        
        # BSデータの欠損率チェック
        if data['bs'] is not None and not data['bs'].empty:
            total_cells = data['bs'].size
            missing_cells = data['bs'].isna().sum().sum()
            quality['bs_missing_rate'] = missing_cells / total_cells if total_cells > 0 else 1.0
            
            # 重要な指標の欠損チェック
            important_items = ['Total Assets', 'Total Liab', 'Stockholders Equity',
                             '総資産', '総負債', '純資産']
            for item in important_items:
                if item in data['bs'].index:
                    if data['bs'].loc[item].isna().all():
                        quality['critical_missing'].append(f'BS_{item}')
        else:
            quality['bs_missing_rate'] = 1.0
            quality['critical_missing'].append('BS_all')
        
        # 欠損値がある場合はログに記録
        if quality['critical_missing']:
            self.logger.warning(
                f"{data['ticker']} 重要なデータ欠損: {', '.join(quality['critical_missing'])}"
            )
        
        return quality
    
    def get_processed_tickers(self) -> set:
        """
        既に処理済みの銘柄リストを取得
        
        Returns:
            処理済み銘柄コードのセット
        """
        return get_processed_tickers_from_files(str(self.data_dir))
    
    def filter_unprocessed_tickers(self, tickers: List[str]) -> List[str]:
        """
        未処理の銘柄のみをフィルタリング
        
        Args:
            tickers: 銘柄コードのリスト
            
        Returns:
            未処理銘柄コードのリスト
        """
        processed = self.get_processed_tickers()
        unprocessed = [ticker for ticker in tickers if ticker not in processed]
        
        if len(unprocessed) < len(tickers):
            skipped_count = len(tickers) - len(unprocessed)
            self.logger.info(f"処理済み銘柄をスキップ: {skipped_count}銘柄")
        
        return unprocessed
    
    def fetch_stock(self, ticker: str) -> Optional[Dict]:
        """
        単一銘柄の財務データを取得
        
        Args:
            ticker: 銘柄コード（4桁）
            
        Returns:
            財務データの辞書（失敗時はNone）
        """
        self.logger.info(f"銘柄データ取得開始: {ticker}")
        
        stock = self._fetch_financial_data(ticker)
        if stock is None:
            return None
        
        data = self._extract_financials(stock, ticker)
        
        # 欠損フラグがある場合はログに記録
        if data['missing_flags']:
            self.logger.warning(
                f"{ticker} 欠損フラグ: {', '.join(data['missing_flags'])}"
            )
        
        self.logger.info(
            f"{ticker} データ取得完了: "
            f"PL欠損率={data['data_quality']['pl_missing_rate']:.2%}, "
            f"CF欠損率={data['data_quality']['cf_missing_rate']:.2%}, "
            f"BS欠損率={data['data_quality']['bs_missing_rate']:.2%}"
        )
        
        return data
    
    def fetch_stocks(self, tickers: List[str], sector: Optional[str] = None, skip_processed: bool = True) -> List[Dict]:
        """
        複数銘柄の財務データを取得
        
        Args:
            tickers: 銘柄コードのリスト
            sector: 業種名（ファイル名に使用）
            skip_processed: 処理済み銘柄をスキップするか（デフォルト: True）
            
        Returns:
            財務データのリスト
        """
        # 処理済み銘柄をスキップ
        if skip_processed:
            tickers = self.filter_unprocessed_tickers(tickers)
            if not tickers:
                self.logger.info("すべての銘柄が既に処理済みです")
                return []
        
        self.logger.info(f"複数銘柄データ取得開始: {len(tickers)}銘柄")
        
        # バッチサイズを超える場合は警告
        if len(tickers) > self.batch_size:
            self.logger.warning(
                f"銘柄数({len(tickers)})がバッチサイズ({self.batch_size})を超えています。"
                f"最初の{self.batch_size}銘柄のみ処理します。"
            )
            tickers = tickers[:self.batch_size]
        
        # 初回アクセス前の待機（レート制限対策）
        if len(tickers) > 0:
            self.logger.info(f"レート制限対策のため{self.initial_delay}秒待機中...")
            time.sleep(self.initial_delay)
        
        results = []
        for i, ticker in enumerate(tickers, 1):
            self.logger.info(f"進捗: {i}/{len(tickers)} - {ticker}")
            data = self.fetch_stock(ticker)
            if data:
                results.append(data)
            
            # レート制限対策：最後の銘柄以外は追加待機
            if i < len(tickers):
                time.sleep(self.rate_limit_delay)
        
        self.logger.info(f"データ取得完了: {len(results)}/{len(tickers)}銘柄取得成功")
        
        return results
    
    def save_to_csv(self, data_list: List[Dict], sector: Optional[str] = None) -> str:
        """
        取得したデータをCSVに保存
        PL, CF, BSを別々のCSVファイルに保存
        
        Args:
            data_list: 財務データのリスト
            sector: 業種名（ファイル名に使用）
            
        Returns:
            保存されたファイルパス（メタデータファイル）
        """
        if not data_list:
            self.logger.warning("保存するデータがありません")
            return ""
        
        # ファイル名を決定
        if sector:
            base_name = sector
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"stocks_{timestamp}"
        
        # メタデータ（銘柄情報とデータ品質）を保存
        meta_records = []
        for data in data_list:
            meta_records.append({
                'ticker': data['ticker'],
                'missing_flags': ','.join(data['missing_flags']) if data['missing_flags'] else '',
                'pl_missing_rate': data['data_quality']['pl_missing_rate'],
                'cf_missing_rate': data['data_quality']['cf_missing_rate'],
                'bs_missing_rate': data['data_quality']['bs_missing_rate'],
                'critical_missing': ','.join(data['data_quality']['critical_missing']) if data['data_quality']['critical_missing'] else '',
            })
        
        meta_df = pd.DataFrame(meta_records)
        meta_filepath = self.data_dir / f"{base_name}_meta.csv"
        meta_df.to_csv(meta_filepath, index=False, encoding='utf-8-sig')
        self.logger.info(f"メタデータ保存完了: {meta_filepath}")
        
        # PLデータを保存
        pl_data = []
        for data in data_list:
            if data['pl'] is not None and not data['pl'].empty:
                pl_df = data['pl'].copy()
                pl_df.insert(0, 'ticker', data['ticker'])
                pl_data.append(pl_df)
        
        if pl_data:
            pl_combined = pd.concat(pl_data, ignore_index=False)
            pl_filepath = self.data_dir / f"{base_name}_PL.csv"
            pl_combined.to_csv(pl_filepath, encoding='utf-8-sig')
            self.logger.info(f"PLデータ保存完了: {pl_filepath}")
        
        # CFデータを保存
        cf_data = []
        for data in data_list:
            if data['cf'] is not None and not data['cf'].empty:
                cf_df = data['cf'].copy()
                cf_df.insert(0, 'ticker', data['ticker'])
                cf_data.append(cf_df)
        
        if cf_data:
            cf_combined = pd.concat(cf_data, ignore_index=False)
            cf_filepath = self.data_dir / f"{base_name}_CF.csv"
            cf_combined.to_csv(cf_filepath, encoding='utf-8-sig')
            self.logger.info(f"CFデータ保存完了: {cf_filepath}")
        
        # BSデータを保存
        bs_data = []
        for data in data_list:
            if data['bs'] is not None and not data['bs'].empty:
                bs_df = data['bs'].copy()
                bs_df.insert(0, 'ticker', data['ticker'])
                bs_data.append(bs_df)
        
        if bs_data:
            bs_combined = pd.concat(bs_data, ignore_index=False)
            bs_filepath = self.data_dir / f"{base_name}_BS.csv"
            bs_combined.to_csv(bs_filepath, encoding='utf-8-sig')
            self.logger.info(f"BSデータ保存完了: {bs_filepath}")
        
        self.logger.info(f"全データ保存完了: {len(data_list)}銘柄")
        
        return str(meta_filepath)
    
    def fetch_and_save(self, tickers: List[str], sector: Optional[str] = None, skip_processed: bool = True) -> str:
        """
        データ取得と保存を一括実行
        
        Args:
            tickers: 銘柄コードのリスト
            sector: 業種名（ファイル名に使用）
            skip_processed: 処理済み銘柄をスキップするか（デフォルト: True）
            
        Returns:
            保存されたファイルパス
        """
        data_list = self.fetch_stocks(tickers, sector, skip_processed=skip_processed)
        if not data_list:
            self.logger.info("保存するデータがありません")
            return ""
        return self.save_to_csv(data_list, sector)
    
    def fetch_incremental(self, all_tickers: List[str], sector: Optional[str] = None) -> str:
        """
        分割取得：未処理銘柄を優先して取得（バッチサイズ制限付き）
        
        Args:
            all_tickers: 全銘柄コードのリスト
            sector: 業種名（ファイル名に使用）
            
        Returns:
            保存されたファイルパス
        """
        # 処理済み銘柄を除外
        unprocessed = self.filter_unprocessed_tickers(all_tickers)
        
        if not unprocessed:
            self.logger.info("すべての銘柄が既に処理済みです")
            return ""
        
        # バッチサイズに制限
        batch = unprocessed[:self.batch_size]
        
        self.logger.info(
            f"分割取得開始: 全{len(all_tickers)}銘柄中、"
            f"未処理{len(unprocessed)}銘柄、"
            f"今回処理{len(batch)}銘柄"
        )
        
        return self.fetch_and_save(batch, sector, skip_processed=False)


# ============================================
# メイン関数（テスト用）
# ============================================
def main():
    """テスト実行用のメイン関数"""
    import sys
    
    # コマンドライン引数で動作モードを選択
    mode = sys.argv[1] if len(sys.argv) > 1 else "incremental"
    
    # テスト用の銘柄リスト（有名な日本株）
    test_tickers = [
        "7203",  # トヨタ自動車
        "6758",  # ソニーグループ
        "9984",  # ソフトバンクグループ
        "9434",  # ソフトバンク
        "6861",  # キーエンス
    ]
    
    # フェッチャーを初期化
    fetcher = StockDataFetcher()
    
    print("=" * 60)
    print("日本株財務データ取得テスト")
    print("=" * 60)
    
    if mode == "incremental":
        # 分割取得モード（推奨）
        print("モード: 分割取得（未処理銘柄を優先）")
        print(f"全銘柄数: {len(test_tickers)}")
        
        # 処理済み銘柄を確認
        processed = fetcher.get_processed_tickers()
        print(f"処理済み銘柄数: {len(processed)}")
        print(f"未処理銘柄数: {len(test_tickers) - len(processed)}")
        print()
        
        try:
            filepath = fetcher.fetch_incremental(test_tickers, sector="test")
            print()
            print("=" * 60)
            if filepath:
                print(f"データ取得完了！")
                print(f"保存先: {filepath}")
            else:
                print("すべての銘柄が既に処理済みです")
            print("=" * 60)
        except Exception as e:
            print(f"エラーが発生しました: {str(e)}")
            import traceback
            traceback.print_exc()
    
    elif mode == "direct":
        # 直接指定モード（処理済みも含めて取得）
        print("モード: 直接指定（処理済みも含む）")
        print(f"テスト銘柄数: {len(test_tickers)}")
        print(f"銘柄リスト: {', '.join(test_tickers)}")
        print()
        
        try:
            filepath = fetcher.fetch_and_save(test_tickers, sector="test", skip_processed=False)
            print()
            print("=" * 60)
            print(f"データ取得完了！")
            print(f"保存先: {filepath}")
            print("=" * 60)
        except Exception as e:
            print(f"エラーが発生しました: {str(e)}")
            import traceback
            traceback.print_exc()
    
    else:
        print(f"不明なモード: {mode}")
        print("使用可能なモード: incremental, direct")


if __name__ == "__main__":
    main()
