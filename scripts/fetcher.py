"""
日本株財務データ取得モジュール（インクリメンタル取得版）
jpx_tse_info.csvから全銘柄を読み込み、古い順に優先的に500件取得する
"""

import os
import time
import random
import logging
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import yfinance as yf
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import constants
from fetcher import StockDataFetcher as BaseStockDataFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IncrementalStockDataFetcher(BaseStockDataFetcher):
    """インクリメンタル取得対応のStockDataFetcher"""
    
    def __init__(self, config_path: str = "config.yaml", max_tickers_per_run: int = 500):
        """
        初期化
        
        Args:
            config_path: 設定ファイルのパス
            max_tickers_per_run: 1回の実行で取得する最大銘柄数（デフォルト: 500）
        """
        super().__init__(config_path)
        self.max_tickers_per_run = max_tickers_per_run
        self.today = datetime.now().date()
        
    def load_all_tickers_from_jpx(self, jpx_info_path: Optional[Path] = None) -> List[str]:
        """
        jpx_tse_info.csvから全銘柄を読み込む
        
        Args:
            jpx_info_path: jpx_tse_info.csvのパス（デフォルト: data/raw/jpx_tse_info.csv）
            
        Returns:
            銘柄コードのリスト（4桁文字列）
        """
        if jpx_info_path is None:
            jpx_info_path = Path("data/raw/jpx_tse_info.csv")
        
        if not jpx_info_path.exists():
            logger.warning(f"jpx_tse_info.csvが見つかりません: {jpx_info_path}")
            logger.info("既存のticker_list.pyから銘柄を読み込みます")
            from ticker_list import get_all_tickers
            return get_all_tickers()
        
        try:
            df = pd.read_csv(jpx_info_path, encoding='utf-8-sig')
            
            # ticker列を探す
            ticker_col = None
            for col in df.columns:
                if 'ticker' in col.lower() or 'コード' in col or 'code' in col.lower():
                    ticker_col = col
                    break
            
            if not ticker_col:
                logger.error("ticker列が見つかりません")
                from ticker_list import get_all_tickers
                return get_all_tickers()
            
            # 4桁の文字列に変換
            tickers = df[ticker_col].astype(str).str.strip().str.zfill(4).tolist()
            # 有効な4桁コードのみを抽出
            tickers = [t for t in tickers if len(t) == 4 and t.isdigit()]
            
            logger.info(f"jpx_tse_info.csvから{len(tickers)}銘柄を読み込みました")
            return tickers
            
        except Exception as e:
            logger.error(f"jpx_tse_info.csvの読み込みエラー: {str(e)}")
            logger.info("既存のticker_list.pyから銘柄を読み込みます")
            from ticker_list import get_all_tickers
            return get_all_tickers()
    
    def get_file_last_modified_date(self, ticker: str) -> Optional[datetime]:
        """
        銘柄のデータファイルの最終更新日時を取得
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            最終更新日時（ファイルが存在しない場合はNone）
        """
        # debugフォルダ内のファイルをチェック
        debug_dir = self.data_dir / "debug"
        debug_files = [
            debug_dir / f"debug_{ticker}_PL.csv",
            debug_dir / f"debug_{ticker}_CF.csv",
            debug_dir / f"debug_{ticker}_BS.csv",
        ]
        
        latest_mtime = None
        for file_path in debug_files:
            if file_path.exists():
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if latest_mtime is None or mtime > latest_mtime:
                    latest_mtime = mtime
        
        return latest_mtime
    
    def is_fetched_today(self, ticker: str) -> bool:
        """
        今日取得済みかどうかをチェック
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            今日取得済みの場合True
        """
        last_modified = self.get_file_last_modified_date(ticker)
        if last_modified is None:
            return False
        
        return last_modified.date() == self.today
    
    def select_tickers_by_priority(self, all_tickers: List[str]) -> List[str]:
        """
        優先順位に基づいて銘柄を選択（古い順、またはファイルが存在しない銘柄を優先）
        
        Args:
            all_tickers: 全銘柄コードのリスト
            
        Returns:
            選択された銘柄コードのリスト（最大max_tickers_per_run件）
        """
        logger.info(f"優先順位に基づいて銘柄を選択中（全{len(all_tickers)}銘柄）...")
        
        # 今日取得済みの銘柄を除外
        tickers_to_check = [t for t in all_tickers if not self.is_fetched_today(t)]
        logger.info(f"今日取得済みを除外: {len(tickers_to_check)}銘柄（除外: {len(all_tickers) - len(tickers_to_check)}銘柄）")
        
        # 各銘柄の最終更新日時を取得
        ticker_dates = []
        for ticker in tickers_to_check:
            last_modified = self.get_file_last_modified_date(ticker)
            # ファイルが存在しない場合は、非常に古い日付として扱う
            if last_modified is None:
                last_modified = datetime(2000, 1, 1)
            ticker_dates.append((ticker, last_modified))
        
        # 古い順にソート（ファイルが存在しない銘柄が最優先）
        ticker_dates.sort(key=lambda x: x[1])
        
        # 最大max_tickers_per_run件を選択
        selected_tickers = [ticker for ticker, _ in ticker_dates[:self.max_tickers_per_run]]
        
        logger.info(f"選択された銘柄数: {len(selected_tickers)}銘柄")
        if selected_tickers:
            oldest_date = ticker_dates[0][1]
            newest_date = ticker_dates[min(len(selected_tickers) - 1, len(ticker_dates) - 1)][1]
            logger.info(f"最古の更新日時: {oldest_date.strftime('%Y-%m-%d')}, 最新の更新日時: {newest_date.strftime('%Y-%m-%d')}")
        
        return selected_tickers
    
    def fetch_incremental(self, jpx_info_path: Optional[Path] = None) -> str:
        """
        インクリメンタル取得を実行
        
        Args:
            jpx_info_path: jpx_tse_info.csvのパス
            
        Returns:
            保存されたファイルパス
        """
        logger.info("=" * 60)
        logger.info("インクリメンタル取得開始")
        logger.info("=" * 60)
        
        # 全銘柄を読み込み
        all_tickers = self.load_all_tickers_from_jpx(jpx_info_path)
        logger.info(f"全銘柄数: {len(all_tickers)}銘柄")
        
        # 優先順位に基づいて銘柄を選択
        selected_tickers = self.select_tickers_by_priority(all_tickers)
        
        if not selected_tickers:
            logger.info("取得対象の銘柄がありません")
            return ""
        
        # 日付ベースのセクター名を使用
        sector_name = f"daily_{datetime.now().strftime('%Y%m%d')}"
        
        # データ取得と保存
        logger.info(f"データ取得開始: {len(selected_tickers)}銘柄")
        filepath = self.fetch_and_save(selected_tickers, sector=sector_name, skip_processed=False)
        
        if filepath:
            logger.info(f"データ取得完了: {filepath}")
        else:
            logger.info("データ取得完了（保存ファイルなし）")
        
        return filepath


def main():
    """メイン関数"""
    import sys
    
    # コマンドライン引数で最大銘柄数を指定可能
    max_tickers = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 500
    
    try:
        fetcher = IncrementalStockDataFetcher(max_tickers_per_run=max_tickers)
        filepath = fetcher.fetch_incremental()
        
        if filepath:
            print(f"\n✅ データ取得完了: {filepath}")
        else:
            print("\n✅ 取得対象の銘柄がありませんでした")
            
    except Exception as e:
        logger.error(f"エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
