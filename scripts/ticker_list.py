"""
全銘柄リスト管理モジュール
東証プライム全銘柄のリストを管理し、未処理銘柄を追跡する
"""

from typing import List, Set
from pathlib import Path
import pandas as pd


# ============================================
# 全銘柄リスト（サンプル）
# 実際の運用では、東証プライム全銘柄リストを用意する
# ============================================
def get_all_tickers() -> List[str]:
    """
    全銘柄リストを取得
    
    Returns:
        銘柄コードのリスト（4桁文字列）
    """
    # TODO: 実際の運用では、東証プライム全銘柄リストを読み込む
    # 例: CSVファイルから読み込む、APIから取得するなど
    
    # サンプルとして主要な日本株をリスト化
    sample_tickers = [
        # テスト用
        "7203", "6758", "9984", "9434", "6861",
        # その他主要銘柄（実際には全銘柄が必要）
        "4503", "6098", "4063", "8031", "8306",
        "8058", "7267", "6954", "7974", "8411",
        "8766", "9022", "9104", "9202", "9301",
        "9432", "9433", "9501", "9502", "9503",
        "9613", "9684", "9697", "9719", "9735",
        "9983", "9989",
    ]
    
    return sample_tickers


def get_processed_tickers(data_dir: str = "data/raw") -> Set[str]:
    """
    既に処理済みの銘柄リストを取得
    
    Args:
        data_dir: データディレクトリのパス
        
    Returns:
        処理済み銘柄コードのセット
    """
    processed = set()
    data_path = Path(data_dir)
    
    if not data_path.exists():
        return processed
    
    # メタデータファイルから処理済み銘柄を抽出
    meta_files = list(data_path.glob("*_meta.csv"))
    
    for meta_file in meta_files:
        try:
            df = pd.read_csv(meta_file)
            if 'ticker' in df.columns:
                processed.update(df['ticker'].astype(str).tolist())
        except Exception as e:
            print(f"Warning: Failed to read {meta_file}: {e}")
    
    return processed


def get_remaining_tickers(batch_size: int = 50, data_dir: str = "data/raw") -> List[str]:
    """
    未処理の銘柄リストを取得（バッチサイズ分）
    
    Args:
        batch_size: 1回に処理する銘柄数
        data_dir: データディレクトリのパス
        
    Returns:
        未処理銘柄コードのリスト（最大batch_size個）
    """
    all_tickers = get_all_tickers()
    processed = get_processed_tickers(data_dir)
    
    remaining = [ticker for ticker in all_tickers if ticker not in processed]
    
    return remaining[:batch_size]


def save_ticker_list(tickers: List[str], filepath: str):
    """
    銘柄リストをファイルに保存
    
    Args:
        tickers: 銘柄コードのリスト
        filepath: 保存先ファイルパス
    """
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        for ticker in tickers:
            f.write(f"{ticker}\n")


def load_ticker_list(filepath: str) -> List[str]:
    """
    ファイルから銘柄リストを読み込み
    
    Args:
        filepath: ファイルパス
        
    Returns:
        銘柄コードのリスト
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return []


if __name__ == "__main__":
    # テスト実行
    print("全銘柄数:", len(get_all_tickers()))
    print("処理済み銘柄数:", len(get_processed_tickers()))
    print("未処理銘柄数:", len(get_remaining_tickers()))
    print("今日のバッチ:", get_remaining_tickers(batch_size=50))
