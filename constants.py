"""
日本株スクリーニングシステム用の定数定義
業種マッピング、日本株33業種分類、その他の定数を定義
"""

# ============================================
# 日本株33業種分類（東証業種分類）
# ============================================
JAPANESE_SECTORS_33 = [
    "水産・農林業",
    "鉱業",
    "建設業",
    "食料品",
    "繊維製品",
    "パルプ・紙",
    "化学",
    "医薬品",
    "石油・石炭製品",
    "ゴム製品",
    "ガラス・土石製品",
    "鉄鋼",
    "非鉄金属",
    "金属製品",
    "機械",
    "電気機器",
    "輸送用機器",
    "精密機器",
    "その他製品",
    "電気・ガス業",
    "陸運業",
    "海運業",
    "空運業",
    "倉庫・運輸関連業",
    "情報・通信業",
    "卸売業",
    "小売業",
    "銀行業",
    "証券、商品先物取引業",
    "保険業",
    "不動産業",
    "サービス業",
    "その他",
]

# ============================================
# 業種コードマッピング（内部管理用）
# ============================================
SECTOR_CODES = {
    "水産・農林業": "0100",
    "鉱業": "0200",
    "建設業": "0300",
    "食料品": "0500",
    "繊維製品": "0600",
    "パルプ・紙": "0700",
    "化学": "0800",
    "医薬品": "0900",
    "石油・石炭製品": "1000",
    "ゴム製品": "1100",
    "ガラス・土石製品": "1200",
    "鉄鋼": "1300",
    "非鉄金属": "1400",
    "金属製品": "1500",
    "機械": "1600",
    "電気機器": "1700",
    "輸送用機器": "1800",
    "精密機器": "1900",
    "その他製品": "2000",
    "電気・ガス業": "2100",
    "陸運業": "2200",
    "海運業": "2300",
    "空運業": "2400",
    "倉庫・運輸関連業": "2500",
    "情報・通信業": "2600",
    "卸売業": "2700",
    "小売業": "2800",
    "銀行業": "2900",
    "証券、商品先物取引業": "3000",
    "保険業": "3100",
    "不動産業": "3200",
    "サービス業": "3300",
    "その他": "9900",
}

# ============================================
# シクリカル業種（景気循環に敏感な業種）
# ============================================
CYCLICAL_SECTORS = [
    "鉄鋼",
    "非鉄金属",
    "化学",
    "石油・石炭製品",
    "パルプ・紙",
    "ガラス・土石製品",
    "機械",
    "輸送用機器",
]

# ============================================
# ディフェンシブ業種（景気に左右されにくい業種）
# ============================================
DEFENSIVE_SECTORS = [
    "食料品",
    "医薬品",
    "電気・ガス業",
    "銀行業",
    "保険業",
    "不動産業",
]

# ============================================
# 成長業種（高成長が期待される業種）
# ============================================
GROWTH_SECTORS = [
    "情報・通信業",
    "医薬品",
    "精密機器",
    "電気機器",
    "サービス業",
]

# ============================================
# 財務指標のカラム名マッピング
# ============================================
FINANCIAL_COLUMNS = {
    # 売上関連
    "revenue": ["売上高", "営業収益", "Revenue", "Net Sales"],
    "operating_revenue": ["営業収益", "Operating Revenue"],
    
    # 利益関連
    "operating_income": ["営業利益", "Operating Income", "営業損益"],
    "net_income": ["純利益", "当期純利益", "Net Income", "純損益"],
    "ebit": ["EBIT", "利払前税引前利益"],
    "ebitda": ["EBITDA", "利払前税引前償却前利益"],
    
    # EPS関連
    "eps": ["EPS", "1株当たり当期純利益", "Earnings Per Share"],
    
    # キャッシュフロー関連
    "operating_cf": ["営業CF", "営業活動によるキャッシュフロー", "Operating Cash Flow"],
    "free_cf": ["FCF", "フリーキャッシュフロー", "Free Cash Flow"],
    
    # バランスシート関連
    "total_assets": ["総資産", "Total Assets"],
    "total_liabilities": ["総負債", "Total Liabilities"],
    "interest_bearing_debt": ["有利子負債", "Interest Bearing Debt"],
    "equity": ["自己資本", "純資産", "Equity", "Net Assets"],
    "cash": ["現金及び預金", "現預金", "Cash and Cash Equivalents"],
    
    # 株価関連
    "market_cap": ["時価総額", "Market Capitalization"],
    "shares_outstanding": ["発行済み株式数", "Shares Outstanding"],
    "book_value": ["純資産", "Book Value"],
}

# ============================================
# 銘柄タイプ分類
# ============================================
STOCK_TYPES = {
    "本物成長株": {
        "description": "成長 + 質 + CF",
        "requirements": {
            "growth": True,
            "quality": True,
            "cash_flow": True,
        }
    },
    "クオリティグロース": {
        "description": "成長 + 高ROIC",
        "requirements": {
            "growth": True,
            "high_roic": True,
        }
    },
    "質の良い割安": {
        "description": "ROIC高・低評価",
        "requirements": {
            "high_roic": True,
            "low_valuation": True,
        }
    },
    "再生株": {
        "description": "改善途上",
        "requirements": {
            "improving": True,
        }
    },
    "シクリカル": {
        "description": "景気連動",
        "requirements": {
            "cyclical": True,
        }
    },
}

# ============================================
# ROIC計算用の定数
# ============================================
ROIC_CONFIG = {
    # 実効税率のデフォルト値（%）
    # 実際の税率が取得できない場合の代替値
    "default_tax_rate": 30.0,
    
    # ROIC計算の信頼度レベル
    "confidence_levels": {
        "high": "完全なデータで計算",
        "medium": "一部代替データで計算",
        "low": "簡易ROIC（ROE × 自己資本比率）",
    }
}

# ============================================
# データ取得関連の定数
# ============================================
DATA_ACQUISITION = {
    # デフォルトのバッチサイズ
    "default_batch_size": 50,
    
    # デフォルトの取得年数
    "default_years": 5,
    
    # キャッシュファイルの拡張子
    "cache_extension": ".csv",
    "sqlite_db": "data/cache.sqlite",
    
    # ファイル命名規則
    "raw_data_pattern": "data/raw/{sector}.csv",
    "processed_data_pattern": "data/processed/{sector}_processed.csv",
}

# ============================================
# ログ関連の定数
# ============================================
LOG_CONFIG = {
    "log_dir": "data/logs",
    "log_file_pattern": "stock_screening_{date}.log",
    "date_format": "%Y%m%d",
}

# ============================================
# ユーティリティ関数
# ============================================
def get_sector_code(sector_name: str) -> str:
    """
    業種名から業種コードを取得
    
    Args:
        sector_name: 業種名
        
    Returns:
        業種コード（見つからない場合は"9999"）
    """
    return SECTOR_CODES.get(sector_name, "9999")


def is_cyclical_sector(sector_name: str) -> bool:
    """
    業種がシクリカルかどうかを判定
    
    Args:
        sector_name: 業種名
        
    Returns:
        シクリカル業種の場合True
    """
    return sector_name in CYCLICAL_SECTORS


def is_defensive_sector(sector_name: str) -> bool:
    """
    業種がディフェンシブかどうかを判定
    
    Args:
        sector_name: 業種名
        
    Returns:
        ディフェンシブ業種の場合True
    """
    return sector_name in DEFENSIVE_SECTORS


def is_growth_sector(sector_name: str) -> bool:
    """
    業種が成長業種かどうかを判定
    
    Args:
        sector_name: 業種名
        
    Returns:
        成長業種の場合True
    """
    return sector_name in GROWTH_SECTORS


def get_all_sectors() -> list:
    """
    全業種リストを取得
    
    Returns:
        全業種名のリスト
    """
    return JAPANESE_SECTORS_33.copy()
