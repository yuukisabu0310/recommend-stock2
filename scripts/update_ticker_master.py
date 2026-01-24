"""
東証銘柄一覧を更新するスクリプト
東証公式の銘柄一覧Excelをダウンロードし、jpx_tse_info.csvを生成する
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional
import urllib.request
import tempfile
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 東証公式の銘柄一覧ExcelのURL
JPX_STOCK_LIST_URL = "https://www.jpx.co.jp/listing/stocks/list/res/jstock_j.xls"

# 除外する市場区分（REIT、ETF、インフラファンドなど）
EXCLUDED_MARKETS = [
    "REIT",
    "ETF・ETN",
    "インフラファンド",
    "プロダクト",
    "カントリーファンド",
    "ベンチャーファンド",
]

# 内国株として抽出する市場区分
INCLUDED_MARKETS = [
    "プライム",
    "スタンダード",
    "グロース",
    "マザーズ",
    "JASDAQ",
]


def download_jpx_excel(url: str, temp_dir: Optional[Path] = None) -> Path:
    """
    東証公式の銘柄一覧Excelをダウンロード
    
    Args:
        url: ExcelファイルのURL
        temp_dir: 一時保存ディレクトリ
        
    Returns:
        ダウンロードしたファイルのパス
    """
    if temp_dir is None:
        temp_dir = Path(tempfile.gettempdir())
    
    temp_file = temp_dir / "jpx_stock_list.xls"
    
    logger.info(f"Excelファイルをダウンロード中: {url}")
    try:
        urllib.request.urlretrieve(url, temp_file)
        logger.info(f"ダウンロード完了: {temp_file}")
        return temp_file
    except Exception as e:
        logger.error(f"ダウンロードエラー: {str(e)}")
        raise


def load_and_filter_stocks(excel_path: Path) -> pd.DataFrame:
    """
    Excelファイルを読み込み、内国株のみを抽出
    
    Args:
        excel_path: Excelファイルのパス
        
    Returns:
        フィルタリングされたDataFrame
    """
    logger.info(f"Excelファイルを読み込み中: {excel_path}")
    
    try:
        # Excelファイルを読み込み（最初のシートを読み込む）
        # .xlsファイルの場合はxlrd、.xlsxファイルの場合はopenpyxlを使用
        if excel_path.suffix.lower() == '.xls':
            try:
                df = pd.read_excel(excel_path, sheet_name=0, engine='xlrd')
            except Exception:
                # xlrdで読み込めない場合、openpyxlを試す
                logger.warning("xlrdで読み込めませんでした。openpyxlを試します...")
                df = pd.read_excel(excel_path, sheet_name=0, engine='openpyxl')
        else:
            df = pd.read_excel(excel_path, sheet_name=0, engine='openpyxl')
        logger.info(f"読み込み完了: {len(df)}行")
        
        # カラム名を確認（デバッグ用）
        logger.info(f"カラム名: {list(df.columns)}")
        
        # カラム名のマッピング（実際のExcelの構造に合わせて調整が必要な場合あり）
        # 一般的なカラム名を想定
        code_col = None
        name_col = None
        market_col = None
        sector_33_col = None
        sector_17_col = None
        
        for col in df.columns:
            col_str = str(col).strip()
            if 'コード' in col_str or 'code' in col_str.lower():
                code_col = col
            elif '銘柄名' in col_str or '名称' in col_str or 'name' in col_str.lower():
                name_col = col
            elif '市場区分' in col_str or 'market' in col_str.lower() or '区分' in col_str:
                market_col = col
            elif '33業種' in col_str or '33' in col_str:
                sector_33_col = col
            elif '17業種' in col_str or '17' in col_str:
                sector_17_col = col
        
        if not code_col or not name_col:
            # カラムが見つからない場合、最初の数行を表示してデバッグ
            logger.warning("カラムが見つかりません。先頭5行を表示します:")
            logger.warning(df.head())
            raise ValueError("必要なカラム（コード、銘柄名）が見つかりません")
        
        logger.info(f"コード列: {code_col}, 銘柄名列: {name_col}")
        logger.info(f"市場区分列: {market_col}, 33業種列: {sector_33_col}, 17業種列: {sector_17_col}")
        
        # 必要なカラムのみを抽出
        columns_to_keep = [code_col]
        if name_col:
            columns_to_keep.append(name_col)
        if market_col:
            columns_to_keep.append(market_col)
        if sector_33_col:
            columns_to_keep.append(sector_33_col)
        if sector_17_col:
            columns_to_keep.append(sector_17_col)
        
        df_filtered = df[columns_to_keep].copy()
        
        # 市場区分でフィルタリング（内国株のみ）
        if market_col:
            # 除外する市場区分を除外
            for excluded in EXCLUDED_MARKETS:
                df_filtered = df_filtered[~df_filtered[market_col].astype(str).str.contains(excluded, na=False)]
            
            # 含める市場区分のみを抽出（空の場合はすべて含める）
            if INCLUDED_MARKETS:
                mask = df_filtered[market_col].astype(str).str.contains('|'.join(INCLUDED_MARKETS), na=False)
                df_filtered = df_filtered[mask]
        
        logger.info(f"フィルタリング後: {len(df_filtered)}行")
        
        return df_filtered, code_col, name_col, market_col, sector_33_col, sector_17_col
        
    except Exception as e:
        logger.error(f"Excel読み込みエラー: {str(e)}")
        raise


def format_and_save(df: pd.DataFrame, code_col: str, name_col: str, 
                    market_col: Optional[str], sector_33_col: Optional[str], 
                    sector_17_col: Optional[str], output_path: Path) -> None:
    """
    データを整形してCSVに保存
    
    Args:
        df: DataFrame
        code_col: コード列名
        name_col: 銘柄名列名
        market_col: 市場区分列名
        sector_33_col: 33業種列名
        sector_17_col: 17業種列名
        output_path: 出力ファイルパス
    """
    logger.info("データを整形中...")
    
    # 新しいDataFrameを作成
    result_df = pd.DataFrame()
    
    # コードを4桁の文字列に変換
    result_df['ticker'] = df[code_col].astype(str).str.strip().str.zfill(4)
    
    # 銘柄名
    if name_col:
        result_df['name'] = df[name_col].astype(str).str.strip()
    else:
        result_df['name'] = ''
    
    # 市場区分
    if market_col:
        result_df['market'] = df[market_col].astype(str).str.strip()
    else:
        result_df['market'] = ''
    
    # 33業種区分
    if sector_33_col:
        result_df['sector_33'] = df[sector_33_col].astype(str).str.strip()
    else:
        result_df['sector_33'] = ''
    
    # 17業種区分
    if sector_17_col:
        result_df['sector_17'] = df[sector_17_col].astype(str).str.strip()
    else:
        result_df['sector_17'] = ''
    
    # 空の行や無効なコードを除外
    result_df = result_df[result_df['ticker'].str.len() == 4]
    result_df = result_df[result_df['ticker'].str.isdigit()]
    result_df = result_df[result_df['name'] != '']
    
    # 重複を削除（コードで）
    result_df = result_df.drop_duplicates(subset=['ticker'])
    
    # ソート（コード順）
    result_df = result_df.sort_values('ticker')
    
    logger.info(f"整形完了: {len(result_df)}銘柄")
    
    # CSVに保存
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"保存完了: {output_path}")
    
    # 統計情報を表示
    logger.info(f"市場区分別の銘柄数:")
    if 'market' in result_df.columns:
        print(result_df['market'].value_counts())
    
    logger.info(f"33業種別の銘柄数（上位10業種）:")
    if 'sector_33' in result_df.columns:
        print(result_df['sector_33'].value_counts().head(10))


def update_ticker_master(output_path: Optional[Path] = None) -> Path:
    """
    銘柄一覧を更新するメイン関数
    
    Args:
        output_path: 出力ファイルパス（デフォルト: data/raw/jpx_tse_info.csv）
        
    Returns:
        保存されたファイルのパス
    """
    if output_path is None:
        output_path = Path("data/raw/jpx_tse_info.csv")
    
    logger.info("=" * 60)
    logger.info("東証銘柄一覧更新スクリプト")
    logger.info("=" * 60)
    
    # 一時ファイルをダウンロード
    temp_file = None
    try:
        temp_file = download_jpx_excel(JPX_STOCK_LIST_URL)
        
        # Excelを読み込み、フィルタリング
        df, code_col, name_col, market_col, sector_33_col, sector_17_col = load_and_filter_stocks(temp_file)
        
        # データを整形して保存
        format_and_save(df, code_col, name_col, market_col, sector_33_col, sector_17_col, output_path)
        
        return output_path
        
    finally:
        # 一時ファイルを削除
        if temp_file and temp_file.exists():
            try:
                temp_file.unlink()
                logger.info(f"一時ファイルを削除: {temp_file}")
            except Exception as e:
                logger.warning(f"一時ファイルの削除に失敗: {str(e)}")


if __name__ == "__main__":
    try:
        output_file = update_ticker_master()
        print(f"\n✅ 銘柄一覧の更新が完了しました: {output_file}")
    except Exception as e:
        logger.error(f"エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)
