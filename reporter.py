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
import re

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
        
        # セクター情報を読み込み
        self.sector_info = self._load_sector_info()
        
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
                
                # 「コード」と「銘柄名」カラムを直接使用
                if 'コード' not in jpx_df.columns or '銘柄名' not in jpx_df.columns:
                    logger.error("「コード」または「銘柄名」列が見つかりません")
                    return {}
                
                # 内国株式のみをフィルタリング
                if '市場・商品区分' in jpx_df.columns:
                    jpx_df = jpx_df[jpx_df['市場・商品区分'].astype(str).str.contains('内国株式', na=False)]
                
                for _, row in jpx_df.iterrows():
                    ticker = str(row['コード']).strip()
                    name = str(row['銘柄名']).strip()
                    # コード整形：.0$を正規表現で除去し、4桁の文字列（0埋め）に変換
                    ticker_clean = re.sub(r'\.0$', '', str(ticker)).strip()
                    # 4桁に整形
                    ticker_clean = ticker_clean.zfill(4)
                    if ticker_clean and name and ticker_clean.isdigit() and len(ticker_clean) == 4:
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
        # コード整形：.Tを除去し、.0$を正規表現で除去してから4桁に整形
        ticker_clean = str(ticker).replace('.T', '').replace('T', '').strip()
        # re.sub()で.0$を除去してからzfill(4)で4桁に整形
        ticker_clean = re.sub(r'\.0$', '', ticker_clean).strip()
        ticker_clean = ticker_clean.zfill(4)
        return self.company_names.get(ticker_clean, ticker)
    
    def _load_sector_info(self) -> Dict[str, str]:
        """
        セクター（33業種区分）情報を読み込む
        
        Returns:
            ticker -> sector_name の辞書
        """
        sector_info = {}
        
        # jpx_tse_info.csvを読み込み
        jpx_info_path = self.raw_data_dir / "jpx_tse_info.csv"
        if jpx_info_path.exists():
            try:
                jpx_df = pd.read_csv(jpx_info_path, encoding='utf-8-sig')
                
                # 「コード」と「33業種区分」カラムを直接使用
                if 'コード' not in jpx_df.columns or '33業種区分' not in jpx_df.columns:
                    logger.error("「コード」または「33業種区分」列が見つかりません")
                    return {}
                
                # 内国株式のみをフィルタリング
                if '市場・商品区分' in jpx_df.columns:
                    jpx_df = jpx_df[jpx_df['市場・商品区分'].astype(str).str.contains('内国株式', na=False)]
                
                for _, row in jpx_df.iterrows():
                    ticker = str(row['コード']).strip()
                    sector = str(row['33業種区分']).strip()
                    # コード整形：.0$を正規表現で除去し、4桁の文字列（0埋め）に変換
                    ticker_clean = re.sub(r'\.0$', '', str(ticker)).strip()
                    # 4桁に整形
                    ticker_clean = ticker_clean.zfill(4)
                    if ticker_clean and sector and sector != '-' and ticker_clean.isdigit() and len(ticker_clean) == 4:
                        sector_info[ticker_clean] = sector
                logger.info(f"セクター情報を読み込みました: {len(sector_info)}件")
            except Exception as e:
                logger.warning(f"セクター情報の読み込みに失敗: {str(e)}")
        else:
            logger.warning(f"セクター情報ファイルが見つかりません: {jpx_info_path}")
        
        return sector_info
    
    def _get_sector(self, ticker: str) -> Optional[str]:
        """
        銘柄コードからセクター（33業種区分）を取得
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            セクター名（取得できない場合はNone）
        """
        # コード整形：.Tを除去し、.0$を正規表現で除去してから4桁に整形
        ticker_clean = str(ticker).replace('.T', '').replace('T', '').strip()
        # re.sub()で.0$を除去してからzfill(4)で4桁に整形
        ticker_clean = re.sub(r'\.0$', '', ticker_clean).strip()
        ticker_clean = ticker_clean.zfill(4)
        return self.sector_info.get(ticker_clean)
    
    def _get_investment_badges(self, row: pd.Series) -> List[str]:
        """
        投資ポイントのバッジを生成（Shields.io形式）
        
        Args:
            row: DataFrameの行
            
        Returns:
            バッジのMarkdown文字列リスト
        """
        badges = []
        
        # ROICが高い場合（10%以上）
        roic = row.get('roic')
        if roic is not None and not pd.isna(roic) and roic >= 10:
            badges.append("![ROIC](https://img.shields.io/badge/効率-高ROIC-red)")
        
        # 売上成長が高い場合（10%以上）
        revenue_growth = row.get('revenue_growth_rate')
        if revenue_growth is not None and not pd.isna(revenue_growth) and revenue_growth >= 10:
            badges.append("![Growth](https://img.shields.io/badge/成長-加速-orange)")
        
        # 無借金
        if row.get('debt_free_flag') == True or row.get('is_debt_free') == True:
            badges.append("![Debt Free](https://img.shields.io/badge/財務-無借金-blue)")
        
        # キャッシュリッチ
        if row.get('net_cash_status') == '実質無借金':
            badges.append("![Cash Rich](https://img.shields.io/badge/財務-キャッシュリッチ-brightgreen)")
        
        return badges
    
    def _get_next_update_date(self) -> str:
        """
        次回のデータ更新予定日を計算（毎週土曜日）
        
        Returns:
            次回更新予定日の文字列
        """
        from datetime import datetime, timedelta
        
        today = datetime.now()
        # 今日が土曜日かどうか確認（weekday()で5が土曜日）
        days_until_saturday = (5 - today.weekday()) % 7
        if days_until_saturday == 0:
            # 今日が土曜日なら、来週の土曜日
            next_saturday = today + timedelta(days=7)
        else:
            # 今週の土曜日
            next_saturday = today + timedelta(days=days_until_saturday)
        
        return next_saturday.strftime("%Y年%m月%d日（%a）").replace('Sat', '土').replace('Sun', '日').replace('Mon', '月').replace('Tue', '火').replace('Wed', '水').replace('Thu', '木').replace('Fri', '金')
    
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
        Yahoo Financeへのリンクを生成（Markdown形式）
        
        Args:
            ticker: 銘柄コード
            
        Returns:
            Markdownリンク形式の文字列
        """
        # コード整形：.Tを除去し、.0$を正規表現で除去してから4桁に整形
        ticker_clean = str(ticker).replace('.T', '').replace('T', '').strip()
        # re.sub()で.0$を除去してからzfill(4)で4桁に整形
        ticker_clean = re.sub(r'\.0$', '', ticker_clean).strip()
        ticker_clean = ticker_clean.zfill(4)
        url = f"https://finance.yahoo.co.jp/quote/{ticker_clean}.T"
        return f"[📈 チャートを表示]({url})"
    
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
        
        # missing_criticalで分離
        # missing_criticalがTrueの銘柄を参考データとして分離
        if 'missing_critical' in df.columns:
            # ブール値または文字列の'True'/'False'に対応
            df['missing_critical'] = df['missing_critical'].astype(str).str.lower().isin(['true', '1', 'yes'])
            main_df = df[~df['missing_critical']].copy()
            reference_df = df[df['missing_critical']].copy()
        else:
            main_df = df.copy()
            reference_df = pd.DataFrame()
        
        # 現在の日時
        now = datetime.now()
        update_time = now.strftime("%Y年%m月%d日 %H:%M")
        next_update = self._get_next_update_date()
        
        # Sランク銘柄数（参考データを除く）
        s_rank_count = len(main_df[main_df.get('total_score', 0) >= 110])
        
        # Header
        markdown = f"""# 📊 日本株 成長×割安スクリーニング結果

<div align="center">

![更新日時](https://img.shields.io/badge/更新日時-{update_time}-blue)
![注目銘柄数](https://img.shields.io/badge/今日の注目銘柄数-{s_rank_count}銘柄-brightgreen)
![次回更新](https://img.shields.io/badge/次回更新-{next_update}-orange)

</div>

---

## 🏆 Top Picks (Sランク銘柄)

"""
        
        # Sランク銘柄（Score 110+）を抽出（参考データを除く）
        s_rank_df = main_df[main_df.get('total_score', 0) >= 110].copy()
        
        if not s_rank_df.empty:
            # テーブル形式でTop Picksを表示
            markdown += "\n\n<div style=\"overflow-x: auto;\">\n\n"
            markdown += "| 順位 | 銘柄名 | 業種 | スコア | ROIC | 成長率 | バッジ | リンク |\n"
            markdown += "|:----:|:------:|:----:|:-----:|:----:|:------:|:------:|:------:|\n"
            
            for idx, row in s_rank_df.iterrows():
                rank = row.get('rank', idx + 1)
                ticker = row.get('ticker', 'N/A')
                company_name = self._get_company_name(ticker)
                sector = self._get_sector(ticker)
                score = row.get('total_score', 0)
                roic = self._format_roic(row.get('roic'))
                growth_rate = self._format_growth_rate(row.get('revenue_growth_rate'))
                
                # 業種表示
                sector_display = sector if sector else "-"
                
                # バッジを取得（Shields.io形式）
                investment_badges = self._get_investment_badges(row)
                badges_str = " ".join(investment_badges) if investment_badges else "-"
                
                # Yahoo Financeリンク（Markdown形式）
                chart_link = self._get_yahoo_finance_link(ticker)
                
                # 値のフォーマット
                roic_str = roic if roic else "N/A"
                growth_str = growth_rate if growth_rate else "N/A"
                
                markdown += f"| {rank} | {company_name} | {sector_display} | {score:.0f} | {roic_str} | {growth_str} | {badges_str} | {chart_link} |\n"
            
            markdown += "\n</div>\n\n"
        else:
            markdown += "Sランク銘柄はありません。\n\n"
        
        # Full Ranking Table
        markdown += """---

## 📈 Full Ranking (全銘柄比較)\n\n"
        markdown += "<div style=\"overflow-x: auto;\">\n\n"
        markdown += "| Rank | 銘柄名 | Ticker | Score | ROIC | 成長率 | 財務ステータス | 売上高<br>(億円) | 営業利益<br>(億円) |\n"
        markdown += "|:----:|:------:|:------:|:-----:|:----:|:------:|:--------------:|:----------------:|:-----------------:|\n"
        
        # テーブル行を生成（参考データを除く）
        for idx, row in main_df.iterrows():
            rank = row.get('rank', idx + 1)
            ticker = row.get('ticker', 'N/A')
            company_name = self._get_company_name(ticker)
            sector = self._get_sector(ticker)
            score = row.get('total_score', 0)
            roic = self._format_roic(row.get('roic'))
            growth_rate = self._format_growth_rate(row.get('revenue_growth_rate'))
            revenue = self._convert_to_hundred_million(row.get('revenue'))
            operating_income = self._convert_to_hundred_million(row.get('operating_income'))
            
            tags = self._get_status_tags(row)
            status_str = " ".join(tags) if tags else "-"
            
            # セクター情報を追加
            company_display = f"{company_name} [{sector}]" if sector else company_name
            
            # 値のフォーマット
            roic_str = roic if roic else "N/A"
            growth_str = growth_rate if growth_rate else "N/A"
            revenue_str = f"{revenue:.1f}" if revenue is not None else "N/A"
            op_income_str = f"{operating_income:.1f}" if operating_income is not None else "N/A"
            
            # Yahoo Financeリンクを生成
            ticker_link = self._get_yahoo_finance_link(ticker)
            
            markdown += f"| {rank} | {company_display} | {ticker_link} | {score:.0f} | {roic_str} | {growth_str} | {status_str} | {revenue_str} | {op_income_str} |\n"
        
        markdown += "\n</div>\n\n"
        
        # 参考データセクション（missing_criticalがTrueの銘柄）
        if not reference_df.empty:
            markdown += "---\n\n"

## ⚠️ 参考データ（重要データ欠損あり）\n\n"
            markdown += "以下の銘柄は重要な財務データが欠損しているため、参考情報として表示しています。\n\n"
            markdown += "<div style=\"overflow-x: auto;\">\n\n"
            markdown += "| Rank | 銘柄名 | Ticker | Score | ROIC | 成長率 | 財務ステータス | 売上高<br>(億円) | 営業利益<br>(億円) | 欠損項目 |\n"
            markdown += "|:----:|:------:|:------:|:-----:|:----:|:------:|:--------------:|:----------------:|:-----------------:|:--------:|\n"
            
            # 参考データのテーブル行を生成
            for idx, row in reference_df.iterrows():
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
                
                # 欠損項目を取得
                missing_items = row.get('missing_items', '')
                if isinstance(missing_items, str):
                    if missing_items.startswith('[') and missing_items.endswith(']'):
                        # リスト形式の文字列をパース
                        import ast
                        try:
                            missing_list = ast.literal_eval(missing_items)
                            missing_str = ', '.join(missing_list) if missing_list else '-'
                        except:
                            missing_str = missing_items if missing_items else '-'
                    else:
                        missing_str = missing_items if missing_items else '-'
                else:
                    missing_str = '-'
                
                # 値のフォーマット
                roic_str = roic if roic else "N/A"
                growth_str = growth_rate if growth_rate else "N/A"
                revenue_str = f"{revenue:.1f}" if revenue is not None else "N/A"
                op_income_str = f"{operating_income:.1f}" if operating_income is not None else "N/A"
                
                # Yahoo Financeリンクを生成
                ticker_link = self._get_yahoo_finance_link(ticker)
                
                markdown += f"| {rank} | {company_name} | {ticker_link} | {score:.0f} | {roic_str} | {growth_str} | {status_str} | {revenue_str} | {op_income_str} | {missing_str} |\n"
            
            markdown += "\n</div>\n\n"
        
        markdown += """---

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
*次回更新予定: {next_update}*

""".format(update_time=update_time, next_update=next_update)
        
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
