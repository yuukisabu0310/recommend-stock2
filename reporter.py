"""
GitHub Pages用のMarkdownレポート生成モジュール
final_recommendations.csvを読み込み、美しいMarkdown形式で出力
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReportGenerator:
    """レポート生成クラス"""
    
    def __init__(self, processed_data_dir: str = "data/processed", output_dir: str = "docs", 
                 raw_data_dir: str = "data/raw"):
        """
        初期化
        
        Args:
            processed_data_dir: 処理済みデータディレクトリ
            output_dir: 出力ディレクトリ（GitHub Pages用）
            raw_data_dir: 生データディレクトリ（銘柄名情報用）
        """
        self.processed_data_dir = Path(processed_data_dir)
        self.output_dir = Path(output_dir)
        self.raw_data_dir = Path(raw_data_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 銘柄名マッピングを読み込み
        self.company_names = self._load_company_names()
        
        logger.info(f"ReportGenerator初期化完了")
    
    def _convert_to_hundred_million(self, value: Optional[float]) -> Optional[float]:
        """
        値を億円単位に変換
        
        Args:
            value: 元の値
            
        Returns:
            億円単位の値（小数点第1位まで）
        """
        if value is None or pd.isna(value):
            return None
        return round(value / 100000000, 1)
    
    def _load_company_names(self) -> Dict[str, str]:
        """
        銘柄名情報を読み込む
        
        Returns:
            ticker -> company_name の辞書
        """
        company_names = {}
        
        # jpx_tse_info.csvを読み込み
        jpx_info_path = self.raw_data_dir / "jpx_tse_info.csv"
        if jpx_info_path.exists():
            try:
                jpx_df = pd.read_csv(jpx_info_path, encoding='utf-8-sig')
                # ticker列と銘柄名列を探す（列名は柔軟に対応）
                ticker_col = None
                name_col = None
                
                for col in jpx_df.columns:
                    col_lower = col.lower()
                    if 'ticker' in col_lower or 'コード' in col or 'code' in col_lower:
                        ticker_col = col
                    elif '名称' in col or 'name' in col_lower or 'company' in col_lower:
                        name_col = col
                
                if ticker_col and name_col:
                    for _, row in jpx_df.iterrows():
                        ticker = str(row[ticker_col]).strip()
                        name = str(row[name_col]).strip()
                        # tickerから.Tを除去して数値のみにする
                        ticker_clean = ticker.replace('.T', '').replace('T', '')
                        if ticker_clean and name:
                            company_names[ticker_clean] = name
                    logger.info(f"銘柄名情報を読み込みました: {len(company_names)}件")
            except Exception as e:
                logger.warning(f"銘柄名情報の読み込みに失敗: {str(e)}")
        else:
            logger.warning(f"銘柄名情報ファイルが見つかりません: {jpx_info_path}")
        
        return company_names
    
    def _get_company_name(self, ticker: str) -> str:
        """
        銘柄コードから銘柄名を取得
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            銘柄名（取得できない場合はコードを返す）
        """
        ticker_clean = str(ticker).replace('.T', '').replace('T', '').strip()
        return self.company_names.get(ticker_clean, ticker)
    
    def _format_percentage(self, value: Optional[float]) -> Optional[str]:
        """
        パーセンテージをフォーマット（小数点第1位まで）
        
        Args:
            value: パーセンテージ値
            
        Returns:
            フォーマットされた文字列（例: "12.3%"）
        """
        if value is None or pd.isna(value):
            return None
        return f"{value:.1f}%"
    
    def _format_growth_rate(self, value: Optional[float]) -> Optional[str]:
        """
        成長率をフォーマット（プラスの場合は+記号を追加、高い場合は太字）
        
        Args:
            value: 成長率（%）
            
        Returns:
            フォーマットされた文字列（例: "+10.5%", "**+15.2%**"）
        """
        if value is None or pd.isna(value):
            return None
        
        formatted = f"{value:+.1f}%"
        
        # 10%以上なら太字
        if value >= 10:
            return f"**{formatted}**"
        
        return formatted
    
    def _format_roic(self, value: Optional[float]) -> Optional[str]:
        """
        ROICをフォーマット（10%以上なら🔥アイコンを追加）
        
        Args:
            value: ROIC（%）
            
        Returns:
            フォーマットされた文字列（例: "12.3%", "15.5% 🔥"）
        """
        if value is None or pd.isna(value):
            return None
        
        formatted = f"{value:.1f}%"
        
        # 10%以上なら🔥アイコンを追加
        if value >= 10:
            return f"{formatted} 🔥"
        
        return formatted
    
    def _get_yahoo_finance_link(self, ticker: str) -> str:
        """
        Yahoo Financeへのリンクを生成
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            Markdownリンク形式の文字列
        """
        ticker_clean = str(ticker).replace('.T', '').replace('T', '').strip()
        url = f"https://finance.yahoo.co.jp/quote/{ticker_clean}.T"
        return f"[{ticker}]({url})"
    
    def _get_status_tags(self, row: pd.Series) -> List[str]:
        """
        ステータスタグを取得
        
        Args:
            row: DataFrameの行
            
        Returns:
            タグのリスト
        """
        tags = []
        
        # 無借金フラグ
        if row.get('debt_free_flag') == True or row.get('is_debt_free') == True:
            tags.append("💎無借金")
        
        # 実質無借金
        if row.get('net_cash_status') == '実質無借金':
            tags.append("💰キャッシュリッチ")
        
        # 高成長（売上成長率 > 10%）
        revenue_growth = row.get('revenue_growth_rate')
        if revenue_growth is not None and not pd.isna(revenue_growth) and revenue_growth > 10:
            tags.append("🚀高成長")
        
        return tags
    
    def _get_star_rank(self, score: Optional[float]) -> str:
        """
        スコアからSランク判定
        
        Args:
            score: 総合スコア
            
        Returns:
            ランク（S, A, B, C）
        """
        if score is None or pd.isna(score):
            return "C"
        
        if score >= 110:
            return "S"
        elif score >= 100:
            return "A"
        elif score >= 80:
            return "B"
        else:
            return "C"
    
    def generate_markdown(self, df: pd.DataFrame) -> str:
        """
        Markdownレポートを生成
        
        Args:
            df: final_recommendations.csvのDataFrame
            
        Returns:
            Markdown形式の文字列
        """
        if df.empty:
            return "# 推奨銘柄レポート\n\nデータがありません。\n"
        
        # 現在の日時
        now = datetime.now()
        update_time = now.strftime("%Y年%m月%d日 %H:%M")
        
        # Sランク銘柄数
        s_rank_count = len(df[df.get('total_score', 0) >= 110])
        
        # Header
        markdown = f"""# 📊 日本株 成長×割安スクリーニング結果

<div align="center">

![更新日時](https://img.shields.io/badge/更新日時-{update_time}-blue)
![注目銘柄数](https://img.shields.io/badge/今日の注目銘柄数-{s_rank_count}銘柄-brightgreen)

</div>

---

## 🏆 Top Picks (Sランク銘柄)

"""
        
        # Sランク銘柄（Score 110+）を抽出
        s_rank_df = df[df.get('total_score', 0) >= 110].copy()
        
        if not s_rank_df.empty:
            for idx, row in s_rank_df.iterrows():
                ticker = row.get('ticker', 'N/A')
                company_name = self._get_company_name(ticker)
                score = row.get('total_score', 0)
                roic = self._format_roic(row.get('roic'))
                growth_rate = self._format_growth_rate(row.get('revenue_growth_rate'))
                revenue = self._convert_to_hundred_million(row.get('revenue'))
                operating_income = self._convert_to_hundred_million(row.get('operating_income'))
                
                tags = self._get_status_tags(row)
                tag_str = " ".join(tags) if tags else ""
                
                # 銘柄名とコードを表示（銘柄名が取得できた場合のみ）
                if company_name != ticker:
                    title = f"{company_name} ({ticker})"
                else:
                    title = ticker
                
                # Yahoo Financeリンクを生成
                ticker_link = self._get_yahoo_finance_link(ticker)
                
                markdown += f"""### {title} {tag_str}

<div style="background-color: #f0f8ff; padding: 15px; border-radius: 8px; margin-bottom: 20px;">

**総合スコア**: {score:.0f}点 | **詳細**: {ticker_link}

**主要指標**:
- ROIC: {roic if roic else 'N/A'}
- 売上成長率: {growth_rate if growth_rate else 'N/A'}
- 売上高: {revenue:.1f}億円（直近年度）
- 営業利益: {operating_income:.1f}億円（直近年度）

"""
                
                # 追加情報
                if row.get('debt_to_equity_ratio') is not None:
                    debt_ratio = row.get('debt_to_equity_ratio')
                    markdown += f"- 負債資本倍率: {debt_ratio:.2f}\n"
                
                if row.get('cash') is not None:
                    cash = self._convert_to_hundred_million(row.get('cash'))
                    markdown += f"- 現預金: {cash:.1f}億円\n"
                
                markdown += "\n</div>\n\n"
        else:
            markdown += "Sランク銘柄はありません。\n\n"
        
        # Full Ranking Table
        markdown += """---

## 📈 Full Ranking (全銘柄比較)

<div style="overflow-x: auto;">

| Rank | 銘柄名 | Ticker | Score | ROIC | 成長率 | 財務ステータス | 売上高<br>(億円) | 営業利益<br>(億円) |
|:----:|:------:|:------:|:-----:|:----:|:------:|:--------------:|:----------------:|:-----------------:|
"""
        
        # テーブル行を生成
        for idx, row in df.iterrows():
            rank = row.get('rank', idx + 1)
            ticker = row.get('ticker', 'N/A')
            company_name = self._get_company_name(ticker)
            score = row.get('total_score', 0)
            roic = self._format_roic(row.get('roic'))
            growth_rate = self._format_growth_rate(row.get('revenue_growth_rate'))
            revenue = self._convert_to_hundred_million(row.get('revenue'))
            operating_income = self._convert_to_hundred_million(row.get('operating_income'))
            
            tags = self._get_status_tags(row)
            status_str = " ".join(tags) if tags else "-"
            
            # 値のフォーマット
            roic_str = roic if roic else "N/A"
            growth_str = growth_rate if growth_rate else "N/A"
            revenue_str = f"{revenue:.1f}" if revenue is not None else "N/A"
            op_income_str = f"{operating_income:.1f}" if operating_income is not None else "N/A"
            
            # Yahoo Financeリンクを生成
            ticker_link = self._get_yahoo_finance_link(ticker)
            
            markdown += f"| {rank} | {company_name} | {ticker_link} | {score:.0f} | {roic_str} | {growth_str} | {status_str} | {revenue_str} | {op_income_str} |\n"
        
        markdown += """
</div>

---

## 📝 凡例

- 💎無借金: 有利子負債がゼロの銘柄
- 💰キャッシュリッチ: 現預金が有利子負債を上回る銘柄（実質無借金）
- 🚀高成長: 売上成長率が10%を超える銘柄

## 📊 スコアリング

- **Sランク**: 110点以上（データ品質100点 + ボーナス10点以上）
- **Aランク**: 100-109点
- **Bランク**: 80-99点
- **Cランク**: 80点未満

---

*最終更新: {update_time}*

""".format(update_time=update_time)
        
        return markdown
    
    def generate_report(self, filename: str = "final_recommendations.csv") -> str:
        """
        レポートを生成して保存
        
        Args:
            filename: 入力CSVファイル名
            
        Returns:
            保存されたファイルパス
        """
        # CSVファイルを読み込み
        csv_path = self.processed_data_dir / filename
        if not csv_path.exists():
            logger.error(f"ファイルが見つかりません: {csv_path}")
            return ""
        
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            logger.info(f"データ読み込み完了: {len(df)}銘柄")
        except Exception as e:
            logger.error(f"CSV読み込みエラー: {str(e)}")
            return ""
        
        # Markdownを生成
        markdown = self.generate_markdown(df)
        
        # ファイルに保存
        output_path = self.output_dir / "index.md"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(markdown)
        
        logger.info(f"レポート保存完了: {output_path}")
        return str(output_path)


# ============================================
# メイン関数
# ============================================
def main():
    """メイン関数"""
    print("=" * 60)
    print("GitHub Pages用レポート生成")
    print("=" * 60)
    
    generator = ReportGenerator()
    
    # レポートを生成
    filepath = generator.generate_report()
    
    if filepath:
        print(f"\nレポートを生成しました: {filepath}")
    else:
        print("レポートの生成に失敗しました")


if __name__ == "__main__":
    main()
