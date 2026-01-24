"""
日本株財務データ取得モジュール（インクリメンタル取得版）
jpx_tse_info.csvから全銘柄を読み込み、JSON形式で個別保存する
"""

import os
import time
import random
import logging
import yaml
import json
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import yfinance as yf
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import constants

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IncrementalStockDataFetcher:
    """インクリメンタル取得対応のStockDataFetcher（JSON個別保存版）"""
    
    def __init__(self, config_path: str = "config.yaml", max_tickers_per_run: int = 500):
        """
        初期化
        
        Args:
            config_path: 設定ファイルのパス
            max_tickers_per_run: 1回の実行で取得する最大銘柄数（デフォルト: 500）
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.data_dir = Path("data/raw")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_tickers_per_run = max_tickers_per_run
        self.years = self.config.get("data_acquisition", {}).get("years", 5)
        
        # 404エラー連続カウンター
        self.consecutive_404_count = 0
        self.max_consecutive_404 = 5  # 5回連続で404が発生したら中断
        
        logger.info(f"IncrementalStockDataFetcher初期化完了: 最大取得数={max_tickers_per_run}銘柄")
    
    def load_all_tickers_from_jpx(self, jpx_info_path: Optional[Path] = None) -> List[str]:
        """
        jpx_tse_info.csvから全銘柄を読み込む（内国株式のみ）
        
        Args:
            jpx_info_path: jpx_tse_info.csvのパス（デフォルト: data/raw/jpx_tse_info.csv）
            
        Returns:
            銘柄コードのリスト（4桁文字列）
        """
        if jpx_info_path is None:
            jpx_info_path = Path("data/raw/jpx_tse_info.csv")
        
        if not jpx_info_path.exists():
            logger.error(f"jpx_tse_info.csvが見つかりません: {jpx_info_path}")
            return []
        
        try:
            df = pd.read_csv(jpx_info_path, encoding='utf-8-sig')
            
            # コード列と市場・商品区分列を探す
            code_col = None
            market_col = None
            for col in df.columns:
                if 'コード' in col:
                    code_col = col
                elif '市場・商品区分' in col:
                    market_col = col
            
            if not code_col:
                logger.error("コード列が見つかりません")
                return []
            
            # 内国株式のみをフィルタリング
            if market_col:
                df = df[df[market_col].astype(str).str.contains('内国株式', na=False)]
                logger.info(f"内国株式フィルタリング後: {len(df)}銘柄")
            
            # コード整形：astype(str)し、.str.replace('.0', '')とzfill(4)で正しい4桁コードに変換
            df['ticker'] = df[code_col].astype(str).str.replace('.0', '', regex=False).str.strip().str.zfill(4)
            
            # 有効な4桁コードのみを抽出（数字のみ）
            df = df[df['ticker'].str.len() == 4]
            df = df[df['ticker'].str.isdigit()]
            
            tickers = df['ticker'].tolist()
            
            logger.info(f"jpx_tse_info.csvから{len(tickers)}銘柄（内国株式のみ）を読み込みました")
            return tickers
            
        except Exception as e:
            logger.error(f"jpx_tse_info.csvの読み込みエラー: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_json_file_path(self, ticker: str) -> Path:
        """
        銘柄のJSONファイルパスを取得
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            JSONファイルのパス
        """
        return self.data_dir / f"{ticker}.json"
    
    def json_file_exists(self, ticker: str) -> bool:
        """
        JSONファイルが存在するかチェック
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            ファイルが存在する場合True
        """
        return self.get_json_file_path(ticker).exists()
    
    def get_json_file_mtime(self, ticker: str) -> Optional[datetime]:
        """
        JSONファイルの最終更新日時を取得
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            最終更新日時（ファイルが存在しない場合はNone）
        """
        json_path = self.get_json_file_path(ticker)
        if json_path.exists():
            return datetime.fromtimestamp(json_path.stat().st_mtime)
        return None
    
    def select_tickers_by_priority(self, all_tickers: List[str]) -> List[str]:
        """
        優先順位に基づいて銘柄を選択
        1. まだJSONファイルが存在しない銘柄を最優先
        2. 次に、ファイルの更新日時が最も古い銘柄を優先
        
        Args:
            all_tickers: 全銘柄コードのリスト
            
        Returns:
            選択された銘柄コードのリスト（最大max_tickers_per_run件）
        """
        logger.info(f"優先順位に基づいて銘柄を選択中（全{len(all_tickers)}銘柄）...")
        
        # 各銘柄の状態を取得
        ticker_status = []
        for ticker in all_tickers:
            json_exists = self.json_file_exists(ticker)
            mtime = self.get_json_file_mtime(ticker)
            
            # 優先度: ファイルが存在しない > 古いファイル
            if not json_exists:
                priority = (0, datetime(2000, 1, 1))  # 最優先
            else:
                priority = (1, mtime)  # 存在する場合は更新日時でソート
            
            ticker_status.append((ticker, priority, json_exists, mtime))
        
        # 優先度でソート（ファイルが存在しないもの → 古い順）
        ticker_status.sort(key=lambda x: (x[1][0], x[1][1]))
        
        # 最大max_tickers_per_run件を選択
        selected_tickers = [ticker for ticker, _, _, _ in ticker_status[:self.max_tickers_per_run]]
        
        # 統計情報をログ出力
        no_file_count = sum(1 for _, _, exists, _ in ticker_status[:self.max_tickers_per_run] if not exists)
        logger.info(f"選択された銘柄数: {len(selected_tickers)}銘柄（未取得: {no_file_count}銘柄, 更新対象: {len(selected_tickers) - no_file_count}銘柄）")
        
        if selected_tickers:
            oldest = ticker_status[0][3]
            if oldest:
                logger.info(f"最古の更新日時: {oldest.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return selected_tickers
    
    def _convert_to_japanese_ticker(self, ticker: str) -> str:
        """
        日本株ティッカーに変換（4桁コードに.Tを付与）
        
        Args:
            ticker: 銘柄コード（4桁）
            
        Returns:
            日本株ティッカー（例: 7203.T）
        """
        ticker = str(ticker).strip().zfill(4)
        if not ticker.endswith('.T'):
            ticker = ticker + '.T'
        return ticker
    
    def fetch_stock_data(self, ticker: str) -> Optional[Dict]:
        """
        1銘柄の財務データを取得
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            財務データの辞書（失敗時はNone）
        """
        jp_ticker = self._convert_to_japanese_ticker(ticker)
        
        try:
            logger.info(f"{ticker} ({jp_ticker}) データ取得中...")
            stock = yf.Ticker(jp_ticker)
            
            # 財務データを取得
            financials = stock.financials
            balance_sheet = stock.balance_sheet
            cashflow = stock.cashflow
            info = stock.info
            
            # データが空の場合は404とみなす
            if (financials is None or financials.empty) and \
               (balance_sheet is None or balance_sheet.empty) and \
               (cashflow is None or cashflow.empty):
                logger.warning(f"{ticker} データが空です（404の可能性）")
                self.consecutive_404_count += 1
                if self.consecutive_404_count >= self.max_consecutive_404:
                    logger.error(f"404エラーが{self.max_consecutive_404}回連続しました。安全のため中断します。")
                    return None
                return None
            
            # 404エラーカウンターをリセット
            self.consecutive_404_count = 0
            
            # データを辞書形式に変換（DataFrameはJSONシリアライズ可能な形式に変換）
            def df_to_dict(df):
                """DataFrameをJSONシリアライズ可能な辞書に変換"""
                if df is None or df.empty:
                    return {}
                # インデックス名を文字列に変換し、列名（日付）も文字列に変換
                result = {}
                for idx in df.index:
                    idx_str = str(idx)
                    result[idx_str] = {}
                    for col in df.columns:
                        col_str = str(col)
                        value = df.loc[idx, col]
                        # NaNやNoneをnullに変換
                        if pd.isna(value):
                            result[idx_str][col_str] = None
                        else:
                            # numpy型をPython型に変換
                            result[idx_str][col_str] = float(value) if pd.api.types.is_numeric_dtype(type(value)) else str(value)
                return result
            
            data = {
                'ticker': ticker,
                'japanese_ticker': jp_ticker,
                'fetch_date': datetime.now().isoformat(),
                'financials': df_to_dict(financials),
                'balance_sheet': df_to_dict(balance_sheet),
                'cashflow': df_to_dict(cashflow),
                'info': info if info else {},
            }
            
            logger.info(f"{ticker} データ取得完了")
            return data
            
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"{ticker} 取得失敗: {error_msg}")
            
            # 404エラーの可能性をチェック
            if "404" in error_msg or "Not Found" in error_msg:
                self.consecutive_404_count += 1
                if self.consecutive_404_count >= self.max_consecutive_404:
                    logger.error(f"404エラーが{self.max_consecutive_404}回連続しました。安全のため中断します。")
                    return None
            
            return None
    
    def save_stock_data(self, ticker: str, data: Dict) -> bool:
        """
        銘柄データをJSONファイルとして保存
        
        Args:
            ticker: 銘柄コード
            data: 財務データの辞書
            
        Returns:
            保存成功時True
        """
        json_path = self.get_json_file_path(ticker)
        
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"{ticker} データ保存完了: {json_path}")
            return True
            
        except Exception as e:
            logger.error(f"{ticker} データ保存失敗: {str(e)}")
            return False
    
    def load_stock_data(self, ticker: str) -> Optional[Dict]:
        """
        既存のJSONファイルから銘柄データを読み込む
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            財務データの辞書（ファイルが存在しない場合はNone）
        """
        json_path = self.get_json_file_path(ticker)
        
        if not json_path.exists():
            return None
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.warning(f"{ticker} データ読み込み失敗: {str(e)}")
            return None
    
    def fetch_incremental(self, jpx_info_path: Optional[Path] = None) -> int:
        """
        インクリメンタル取得を実行
        
        Args:
            jpx_info_path: jpx_tse_info.csvのパス
            
        Returns:
            取得した銘柄数
        """
        logger.info("=" * 60)
        logger.info("インクリメンタル取得開始")
        logger.info("=" * 60)
        
        # 全銘柄を読み込み
        all_tickers = self.load_all_tickers_from_jpx(jpx_info_path)
        if not all_tickers:
            logger.error("銘柄リストが空です")
            return 0
        
        logger.info(f"全銘柄数: {len(all_tickers)}銘柄")
        
        # 優先順位に基づいて銘柄を選択
        selected_tickers = self.select_tickers_by_priority(all_tickers)
        
        if not selected_tickers:
            logger.info("取得対象の銘柄がありません")
            return 0
        
        # データ取得と保存
        logger.info(f"データ取得開始: {len(selected_tickers)}銘柄")
        fetched_count = 0
        failed_count = 0
        
        for i, ticker in enumerate(selected_tickers, 1):
            # 404エラーが連続した場合は中断
            if self.consecutive_404_count >= self.max_consecutive_404:
                logger.warning(f"404エラーが連続したため、{i-1}/{len(selected_tickers)}銘柄で中断します")
                break
            
            logger.info(f"[{i}/{len(selected_tickers)}] {ticker} 処理中...")
            
            # データ取得
            data = self.fetch_stock_data(ticker)
            
            if data:
                # データ保存
                if self.save_stock_data(ticker, data):
                    fetched_count += 1
                else:
                    failed_count += 1
            else:
                failed_count += 1
            
            # 1銘柄ごとにランダムな待機時間（1秒程度）
            if i < len(selected_tickers):  # 最後の銘柄では待機しない
                sleep_time = random.uniform(0.8, 1.5)
                time.sleep(sleep_time)
        
        logger.info("=" * 60)
        logger.info(f"データ取得完了: 成功={fetched_count}銘柄, 失敗={failed_count}銘柄")
        logger.info("=" * 60)
        
        return fetched_count


def main():
    """メイン関数"""
    import sys
    
    # コマンドライン引数で最大銘柄数を指定可能
    max_tickers = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 500
    
    try:
        fetcher = IncrementalStockDataFetcher(max_tickers_per_run=max_tickers)
        fetched_count = fetcher.fetch_incremental()
        
        if fetched_count > 0:
            print(f"\n✅ データ取得完了: {fetched_count}銘柄")
        else:
            print("\n✅ 取得対象の銘柄がありませんでした")
            
    except KeyboardInterrupt:
        logger.warning("ユーザーによって中断されました")
        print("\n⚠️ 処理が中断されました")
    except Exception as e:
        logger.error(f"エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
